use std::path::PathBuf;
use std::time::Duration;

use clap::{Args, Subcommand};
use colored::Colorize;
use comfy_table::{Cell, CellAlignment, Color};

use crate::cli::{output, GlobalOpts};
use crate::error::{KafkaError, KafkaResult};
use crate::kafka::admin::KafkaAdminClient;
use crate::kafka::reassign::{
    dedup_preserving_order, evaluate_partition_status, generate_balanced_plan,
    generate_rollback_plan, PartitionAssignment, ReassignmentPlan, ReassignmentStatus,
};
use crate::kafka::wire::alter_partition_reassignments;

/// Command line arguments for Kafka Cluster Rebalance & Partition Reassignment.
#[derive(Args, Debug, Clone)]
pub struct ReassignArgs {
    #[command(subcommand)]
    pub command: ReassignSubcommands,
}

/// Supported subcommands for `ferrika reassign`.
#[derive(Subcommand, Debug, Clone, PartialEq)]
pub enum ReassignSubcommands {
    /// Generate an optimal partition replica rebalance plan across target brokers
    Plan(ReassignPlanArgs),
    /// Execute or simulate partition reassignment from a plan JSON file
    Execute(ReassignExecuteArgs),
    /// Verify migration and in-sync replica status for an in-flight reassignment plan
    Verify(ReassignVerifyArgs),
    /// Generate a safe rollback plan capturing current live partition placements
    Rollback(ReassignRollbackArgs),
    /// Show the partition moves the cluster is running now
    Status,
}

/// Arguments for `ferrika reassign plan`.
#[derive(Args, Debug, Clone, PartialEq)]
pub struct ReassignPlanArgs {
    /// Target topic name(s) to rebalance (comma-separated or repeated)
    #[arg(short = 't', long, value_delimiter = ',', required = true)]
    pub topics: Vec<String>,

    /// Target broker ID list to distribute partitions across (e.g. '1,2,3,4')
    #[arg(short = 'b', long, value_delimiter = ',', required = true)]
    pub brokers: Vec<i32>,

    /// Desired replication factor (defaults to the topic's current one; with
    /// --partitions, to 2 or the broker count if smaller)
    #[arg(long)]
    pub rf: Option<usize>,

    /// Plan offline for this many partitions per topic, without reading the
    /// cluster. Otherwise topics and broker ids are checked against the cluster
    #[arg(short = 'p', long)]
    pub partitions: Option<usize>,

    /// Save generated reassignment plan to file (e.g. 'reassignment.json')
    #[arg(short = 'o', long)]
    pub output: Option<PathBuf>,

    /// Output plan directly in JSON format
    #[arg(long)]
    pub json: bool,
}

/// Arguments for `ferrika reassign execute`.
#[derive(Args, Debug, Clone, PartialEq)]
pub struct ReassignExecuteArgs {
    /// Path to the reassignment plan JSON file
    #[arg(short = 'i', long)]
    pub plan: PathBuf,

    /// Inter-broker replication throttle rate in bytes/sec (not supported yet; rejected if set)
    #[arg(long)]
    pub throttle_bytes: Option<u64>,

    /// Commit and execute the partition reassignment (defaults to safe dry-run preview)
    #[arg(short = 'e', long, default_value_t = false)]
    pub execute: bool,
}

/// Arguments for `ferrika reassign verify`.
#[derive(Args, Debug, Clone, PartialEq)]
pub struct ReassignVerifyArgs {
    /// Path to the reassignment plan JSON file to verify
    #[arg(short = 'i', long)]
    pub plan: PathBuf,
}

/// Arguments for `ferrika reassign rollback`.
#[derive(Args, Debug, Clone, PartialEq)]
pub struct ReassignRollbackArgs {
    /// Target topic name(s) to capture rollback state for
    #[arg(short = 't', long, value_delimiter = ',', required = true)]
    pub topics: Vec<String>,

    /// Path to write the rollback plan JSON file
    #[arg(short = 'o', long)]
    pub output: PathBuf,
}

/// Executes the requested `ferrika reassign` subcommand.
pub fn run(args: ReassignArgs, global: &GlobalOpts) -> KafkaResult<()> {
    match args.command {
        ReassignSubcommands::Plan(plan_args) => run_plan(plan_args, global),
        ReassignSubcommands::Execute(exec_args) => run_execute(exec_args, global),
        ReassignSubcommands::Verify(verify_args) => run_verify(verify_args, global),
        ReassignSubcommands::Rollback(rollback_args) => run_rollback(rollback_args, global),
        ReassignSubcommands::Status => run_status(global),
    }
}

fn run_status(global: &GlobalOpts) -> KafkaResult<()> {
    let moves = crate::kafka::wire::list_partition_reassignments(&global.to_kafka_config())?;
    if global.format == crate::cli::OutputFormat::Json {
        output::print_json(&moves)?;
        return Ok(());
    }
    if moves.is_empty() {
        println!("{} No partition moves in progress.", "✔".green().bold());
        return Ok(());
    }
    let ids = |v: &[i32]| v.iter().map(i32::to_string).collect::<Vec<_>>().join(", ");
    let mut table = output::table(vec!["TOPIC", "PARTITION", "REPLICAS", "ADDING", "REMOVING"]);
    for m in &moves {
        table.add_row(vec![
            Cell::new(&m.topic),
            Cell::new(m.partition).set_alignment(CellAlignment::Right),
            Cell::new(ids(&m.replicas)),
            Cell::new(ids(&m.adding)).fg(Color::Green),
            Cell::new(ids(&m.removing)).fg(Color::Yellow),
        ]);
    }
    println!("{table}");
    println!("{} partition move(s) in progress.", moves.len());
    Ok(())
}

/// Handles `ferrika reassign plan`
fn run_plan(mut args: ReassignPlanArgs, global: &GlobalOpts) -> KafkaResult<()> {
    // `-t a,a` would plan a partition twice; `-b 1,1` would repeat a replica.
    args.topics = dedup_preserving_order(&args.topics);
    args.brokers = dedup_preserving_order(&args.brokers);
    let mut all_partitions = Vec::new();
    // Each topic continues the leader rotation where the previous one ended.
    let mut placed = 0usize;

    if let Some(part_count) = args.partitions {
        // Offline planning: nothing to check the topics and brokers against.
        let rf = args.rf.unwrap_or_else(|| 2.min(args.brokers.len()));
        for topic in &args.topics {
            let topic_plan = generate_balanced_plan(topic, part_count, &args.brokers, rf, placed)?;
            placed += topic_plan.partitions.len();
            all_partitions.extend(topic_plan.partitions);
        }
    } else {
        let kafka_config = global.to_kafka_config();
        let admin = KafkaAdminClient::new(&kafka_config)?;
        let timeout = Some(Duration::from_secs(5));

        let known: Vec<i32> = admin.list_brokers(timeout)?.iter().map(|b| b.id).collect();
        let unknown: Vec<String> = args
            .brokers
            .iter()
            .filter(|id| !known.contains(id))
            .map(i32::to_string)
            .collect();
        if !unknown.is_empty() {
            return Err(KafkaError::config(format!(
                "Broker id(s) {} are not in the cluster (known: {})",
                unknown.join(", "),
                known
                    .iter()
                    .map(i32::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            )));
        }

        for topic in &args.topics {
            // A missing topic is an error rather than a made-up plan.
            let detail = admin.describe_topic(topic, timeout)?;
            let current_rf = detail.partitions.first().map_or(1, |p| p.replicas.len());
            let rf = args.rf.unwrap_or(current_rf);
            let topic_plan =
                generate_balanced_plan(topic, detail.partitions.len(), &args.brokers, rf, placed)?;
            placed += topic_plan.partitions.len();
            all_partitions.extend(topic_plan.partitions);
        }
    }

    let plan = ReassignmentPlan::new(all_partitions);

    if global.wants_json(args.json) {
        if let Some(ref path) = args.output {
            plan.save_to_file(path)?;
            eprintln!("Reassignment plan written to: {}", path.display());
        }
        println!("{}", plan.to_json_pretty()?);
        return Ok(());
    }

    render_plan_table(&plan, &args.brokers);

    if let Some(ref path) = args.output {
        plan.save_to_file(path)?;
        println!(
            "\n{} Reassignment plan successfully written to: {}",
            "✓".green().bold(),
            path.display().to_string().cyan().bold()
        );
    } else {
        println!(
            "\n{} To save this plan, re-run with '--output <file.json>' or '--json'.",
            "💡".yellow()
        );
    }

    Ok(())
}

/// Handles `ferrika reassign execute`
fn run_execute(args: ReassignExecuteArgs, global: &GlobalOpts) -> KafkaResult<()> {
    let plan = ReassignmentPlan::load_from_file(&args.plan)?;
    plan.validate()?;

    println!(
        "\n{} Loaded plan with {} partition assignments from {}",
        "📋".cyan(),
        plan.partitions.len().to_string().yellow().bold(),
        args.plan.display().to_string().bold()
    );

    if args.throttle_bytes.is_some() {
        return Err(KafkaError::config(
            "--throttle-bytes is not supported yet; re-run without it to submit an unthrottled reassignment",
        ));
    }

    if plan.partitions.iter().any(|p| p.log_dirs.is_some()) {
        println!(
            "{} Plan 'log_dirs' entries are ignored; replicas use the brokers' default log directories.",
            "⚠".yellow().bold()
        );
    }

    if !args.execute {
        println!(
            "\n{} {}",
            "💡 [SIMULATION MODE - DRY-RUN]".yellow().bold(),
            "No cluster partitions were modified. Re-run with '--execute' (-e) to apply this reassignment plan.".bright_white()
        );
        return Ok(());
    }

    let kafka_config = global.to_kafka_config();
    let pending = record_reassignment(global, &kafka_config, &plan.partitions);
    let results = alter_partition_reassignments(&kafka_config, &plan.partitions);
    let outcome = results
        .as_ref()
        .map_err(ToString::to_string)
        .and_then(
            |results| match results.iter().filter(|r| r.error.is_some()).count() {
                0 => Ok(()),
                n => Err(format!("{n} partition reassignment(s) were rejected")),
            },
        );
    crate::cli::record::finish(
        pending,
        &outcome,
        &format!(
            "Reassignment submitted for {} partition(s)",
            plan.partitions.len()
        ),
    );
    let results = results?;
    let failures: Vec<_> = results.iter().filter(|r| r.error.is_some()).collect();

    for failure in &failures {
        eprintln!(
            "{} {}-{}: {}",
            "✖".red().bold(),
            failure.topic,
            failure.partition,
            failure.error.as_deref().unwrap_or_default()
        );
    }

    let accepted = results.len() - failures.len();
    if accepted == 0 {
        return Err(KafkaError::admin(format!(
            "The controller rejected all {} partition reassignment(s)",
            results.len()
        )));
    }

    println!(
        "\n{} Controller accepted partition reassignment for {} of {} partitions",
        "✓".green().bold(),
        accepted.to_string().cyan().bold(),
        results.len()
    );
    if !failures.is_empty() {
        return Err(KafkaError::admin(format!(
            "{} partition reassignment(s) were rejected",
            failures.len()
        )));
    }
    println!(
        "{} Track progress by running: ferrika reassign verify -i {}",
        "ℹ".blue().bold(),
        args.plan.display()
    );

    Ok(())
}

/// Starts the history record of a reassignment: the partitions' replicas
/// now and in the plan.
fn record_reassignment(
    global: &GlobalOpts,
    config: &crate::kafka::config::KafkaClientConfig,
    partitions: &[crate::kafka::reassign::PartitionAssignment],
) -> Option<crate::cli::record::Pending> {
    use crate::model::history::{PartitionReplicas, Reversibility, State};
    let admin = KafkaAdminClient::new(config).ok();
    let wanted: Vec<(String, i32)> = partitions
        .iter()
        .map(|p| (p.topic.clone(), p.partition))
        .collect();
    let after = State::Replicas {
        partitions: partitions
            .iter()
            .map(|p| PartitionReplicas {
                topic: p.topic.clone(),
                partition: p.partition,
                replicas: p.replicas.clone(),
            })
            .collect(),
    };
    let captured = match &admin {
        Some(admin) => crate::ops::history::capture::replicas_of(admin, &wanted),
        None => Err(KafkaError::admin("no admin client")),
    }
    .map(|before| (vec![before], vec![after]));
    crate::cli::record::start(
        global,
        crate::cli::record::cluster_scope(global, admin.as_ref()),
        crate::cli::record::change_from_capture(Reversibility::Reversible, captured),
    )
}

/// Handles `ferrika reassign verify`
fn run_verify(args: ReassignVerifyArgs, global: &GlobalOpts) -> KafkaResult<()> {
    let plan = ReassignmentPlan::load_from_file(&args.plan)?;
    plan.validate()?;
    let kafka_config = global.to_kafka_config();

    println!(
        "\n{} Verifying reassignment progress for {} partition(s)...",
        "🔍".cyan(),
        plan.partitions.len().to_string().bold()
    );

    let mut table = output::table(vec![
        Cell::new("Topic").fg(Color::Cyan),
        Cell::new("Partition")
            .fg(Color::Cyan)
            .set_alignment(CellAlignment::Right),
        Cell::new("Target Replicas")
            .fg(Color::Cyan)
            .set_alignment(CellAlignment::Center),
        Cell::new("Status")
            .fg(Color::Cyan)
            .set_alignment(CellAlignment::Center),
    ]);

    let mut statuses = Vec::new();
    let admin = KafkaAdminClient::new(&kafka_config)?;

    for p in &plan.partitions {
        let detail = admin.describe_topic(&p.topic, Some(Duration::from_secs(5)))?;
        let status = detail
            .partitions
            .iter()
            .find(|part| part.partition_id == p.partition)
            .map_or(ReassignmentStatus::Failed, |part| {
                evaluate_partition_status(&part.replicas, &part.isr, &p.replicas)
            });

        statuses.push(status);

        let status_cell = match status {
            ReassignmentStatus::Completed => Cell::new("COMPLETED")
                .fg(Color::Green)
                .set_alignment(CellAlignment::Center),
            ReassignmentStatus::InProgress => Cell::new("IN_PROGRESS")
                .fg(Color::Yellow)
                .set_alignment(CellAlignment::Center),
            ReassignmentStatus::Failed => Cell::new("FAILED")
                .fg(Color::Red)
                .set_alignment(CellAlignment::Center),
        };

        table.add_row(vec![
            Cell::new(&p.topic),
            Cell::new(p.partition.to_string()).set_alignment(CellAlignment::Right),
            Cell::new(format!("{:?}", p.replicas)).set_alignment(CellAlignment::Center),
            status_cell,
        ]);
    }

    println!("{table}");

    match verify_outcome(&statuses) {
        Ok(()) => println!(
            "\n{} All partition reassignments completed successfully!",
            "✓".green().bold()
        ),
        Err(e) => {
            println!(
                "\n{} Some partitions are not done yet. Please retry in a few moments.",
                "⏳".yellow().bold()
            );
            return Err(e);
        }
    }

    Ok(())
}

/// Ok only when every partition is `Completed`; otherwise a check failure
/// naming how many are in progress or failed.
fn verify_outcome(statuses: &[ReassignmentStatus]) -> KafkaResult<()> {
    let count = |s: ReassignmentStatus| statuses.iter().filter(|x| **x == s).count();
    let in_progress = count(ReassignmentStatus::InProgress);
    let failed = count(ReassignmentStatus::Failed);
    if in_progress == 0 && failed == 0 {
        return Ok(());
    }
    Err(KafkaError::Check(format!(
        "Reassignment is not complete: {} of {} partition(s) in progress, {} failed",
        in_progress,
        statuses.len(),
        failed
    )))
}

/// Handles `ferrika reassign rollback`
fn run_rollback(args: ReassignRollbackArgs, global: &GlobalOpts) -> KafkaResult<()> {
    let kafka_config = global.to_kafka_config();
    let admin = KafkaAdminClient::new(&kafka_config)?;

    let mut current_assignments = Vec::new();
    // A partition being moved reports old and new replicas together; the
    // rollback must restore the old ones only.
    let ongoing = match crate::kafka::wire::list_partition_reassignments(&kafka_config) {
        Ok(moves) => moves,
        Err(e) => {
            eprintln!("warning: could not check ongoing reassignments ({e}); replicas are taken as listed");
            Vec::new()
        }
    };

    for topic in &dedup_preserving_order(&args.topics) {
        let detail = admin.describe_topic(topic, Some(Duration::from_secs(5)))?;
        for p in detail.partitions {
            let moving = ongoing
                .iter()
                .find(|m| m.topic == *topic && m.partition == p.partition_id);
            let replicas = crate::kafka::reassign::pre_move_replicas(&p.replicas, moving);
            current_assignments.push(PartitionAssignment::new(topic, p.partition_id, replicas));
        }
    }

    let rollback_plan = generate_rollback_plan(&current_assignments);
    rollback_plan.save_to_file(&args.output)?;

    println!(
        "\n{} Rollback plan capturing {} live partition placements saved to: {}",
        "✓".green().bold(),
        rollback_plan.partitions.len().to_string().cyan().bold(),
        args.output.display().to_string().bold()
    );

    Ok(())
}

/// Pretty prints the generated reassignment plan table
fn render_plan_table(plan: &ReassignmentPlan, target_brokers: &[i32]) {
    println!(
        "\n{} Target Brokers: {:?} | Planned Partitions: {}\n",
        "⚙️  Partition Reassignment Plan:".bold(),
        target_brokers,
        plan.partitions.len().to_string().cyan().bold()
    );

    let mut table = output::table(vec![
        Cell::new("Topic").fg(Color::Cyan),
        Cell::new("Partition")
            .fg(Color::Cyan)
            .set_alignment(CellAlignment::Right),
        Cell::new("Preferred Leader")
            .fg(Color::Cyan)
            .set_alignment(CellAlignment::Center),
        Cell::new("Planned Replica Set")
            .fg(Color::Cyan)
            .set_alignment(CellAlignment::Center),
    ]);

    for p in &plan.partitions {
        let leader = p.replicas.first().copied().unwrap_or(-1);
        table.add_row(vec![
            Cell::new(&p.topic),
            Cell::new(p.partition.to_string()).set_alignment(CellAlignment::Right),
            Cell::new(format!("Node {leader}"))
                .fg(Color::Green)
                .set_alignment(CellAlignment::Center),
            Cell::new(format!("{:?}", p.replicas)).set_alignment(CellAlignment::Center),
        ]);
    }

    println!("{table}");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verify_outcome_fails_unless_everything_completed() {
        use ReassignmentStatus::*;
        assert!(verify_outcome(&[Completed, Completed]).is_ok());
        assert!(matches!(
            verify_outcome(&[Completed, InProgress]),
            Err(KafkaError::Check(_))
        ));
        assert!(matches!(
            verify_outcome(&[Failed]),
            Err(KafkaError::Check(_))
        ));
    }

    #[test]
    fn test_clap_reassign_args_plan_parsing() {
        use clap::Parser;

        #[derive(Parser, Debug)]
        struct Wrapper {
            #[command(subcommand)]
            cmd: ReassignSubcommands,
        }

        let parsed = Wrapper::parse_from([
            "reassign",
            "plan",
            "-t",
            "orders-v1,orders-v2",
            "-b",
            "1,2,3",
            "--rf",
            "2",
            "-o",
            "test_plan.json",
        ]);

        match parsed.cmd {
            ReassignSubcommands::Plan(args) => {
                assert_eq!(args.topics, vec!["orders-v1", "orders-v2"]);
                assert_eq!(args.brokers, vec![1, 2, 3]);
                assert_eq!(args.rf, Some(2));
                assert_eq!(args.output, Some(PathBuf::from("test_plan.json")));
            }
            _ => panic!("Expected Plan subcommand"),
        }
    }

    #[test]
    fn test_clap_reassign_args_execute_parsing() {
        use clap::Parser;

        #[derive(Parser, Debug)]
        struct Wrapper {
            #[command(subcommand)]
            cmd: ReassignSubcommands,
        }

        let parsed = Wrapper::parse_from([
            "reassign",
            "execute",
            "-i",
            "plan.json",
            "--throttle-bytes",
            "10485760",
            "-e",
        ]);

        match parsed.cmd {
            ReassignSubcommands::Execute(args) => {
                assert_eq!(args.plan, PathBuf::from("plan.json"));
                assert_eq!(args.throttle_bytes, Some(10485760));
                assert!(args.execute);
            }
            _ => panic!("Expected Execute subcommand"),
        }
    }
}
