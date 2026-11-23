use std::path::PathBuf;
use std::time::Duration;

use clap::{Args, Subcommand};
use colored::Colorize;
use comfy_table::{Cell, CellAlignment, Color};

use crate::cli::{output, GlobalOpts, OutputFormat};
use crate::error::{KafkaError, KafkaResult};
use crate::kafka::admin::KafkaAdminClient;
use crate::kafka::reassign::{
    dedup_preserving_order, evaluate_partition_status, generate_balanced_plan,
    generate_minimal_move_plan, generate_rollback_plan, replicas_to_move, PartitionAssignment,
    ReassignmentPlan, ReassignmentStatus,
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

    /// Desired replication factor (defaults to each partition's current one,
    /// with --from-scratch the topic's first partition's; with --partitions,
    /// to 2 or the broker count if smaller)
    #[arg(long)]
    pub rf: Option<usize>,

    /// Plan offline for this many partitions per topic, without reading the
    /// cluster. Otherwise topics and broker ids are checked against the cluster
    #[arg(short = 'p', long)]
    pub partitions: Option<usize>,

    /// Lay every partition out afresh (leader rotation over the brokers)
    /// instead of keeping the replicas that are already well placed. Moves
    /// much more data; the offline plan (--partitions) always does this
    #[arg(long)]
    pub from_scratch: bool,

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
    // Online: replicas the plan copies to new brokers, current replicas it
    // keeps, and current replicas in all.
    let mut moved: Option<(usize, usize, usize)> = None;

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

        check_target_brokers(&admin.list_brokers(timeout)?, &args.brokers)?;

        crate::kafka::reassign::ensure_not_moving(&kafka_config, &args.topics)?;
        let mut current = Vec::new();
        for topic in &args.topics {
            // A missing topic is an error rather than a made-up plan.
            let detail = admin.describe_topic(topic, timeout)?;
            let current_rf = detail.partitions.first().map_or(1, |p| p.replicas.len());
            let rf = args.rf.unwrap_or(current_rf);
            current.extend(detail.partitions.iter().map(|p| {
                PartitionAssignment::new(topic.clone(), p.partition_id, p.replicas.clone())
            }));
            if args.from_scratch {
                let topic_plan = generate_balanced_plan(
                    topic,
                    detail.partitions.len(),
                    &args.brokers,
                    rf,
                    placed,
                )?;
                placed += topic_plan.partitions.len();
                all_partitions.extend(topic_plan.partitions);
            }
        }
        if !args.from_scratch {
            // All topics at once, so the brokers' shares count every topic.
            all_partitions =
                generate_minimal_move_plan(&current, &args.brokers, args.rf)?.partitions;
        }
        let planned = ReassignmentPlan::new(all_partitions.clone());
        let after: std::collections::HashMap<(&str, i32), &PartitionAssignment> = planned
            .partitions
            .iter()
            .map(|p| ((p.topic.as_str(), p.partition), p))
            .collect();
        let kept: usize = current
            .iter()
            .map(|before| {
                after
                    .get(&(before.topic.as_str(), before.partition))
                    .map_or(0, |after| {
                        before
                            .replicas
                            .iter()
                            .filter(|b| after.replicas.contains(b))
                            .count()
                    })
            })
            .sum();
        moved = Some((
            replicas_to_move(&current, &planned),
            kept,
            current.iter().map(|p| p.replicas.len()).sum::<usize>(),
        ));
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
    if let Some((moving, kept, existing)) = moved {
        println!(
            "\n{} replica(s) to copy onto other brokers; {kept} of {existing} current replica(s) stay where they are.",
            moving.to_string().yellow().bold(),
        );
    }

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
    let json = global.format == OutputFormat::Json;

    if !json {
        println!(
            "\n{} Loaded plan with {} partition assignments from {}",
            "📋".cyan(),
            plan.partitions.len().to_string().yellow().bold(),
            args.plan.display().to_string().bold()
        );
    }

    if args.throttle_bytes.is_some() {
        return Err(KafkaError::config(
            "--throttle-bytes is not supported yet; re-run without it to submit an unthrottled reassignment",
        ));
    }

    if plan.partitions.iter().any(|p| p.log_dirs.is_some()) {
        let note = format!(
            "{} Plan 'log_dirs' entries are ignored; replicas use the brokers' default log directories.",
            "⚠".yellow().bold()
        );
        if json {
            eprintln!("{note}");
        } else {
            println!("{note}");
        }
    }

    if !args.execute {
        if json {
            return output::print_json(&execute_json(&args.plan, plan.partitions.len(), None));
        }
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
    if json {
        let rejected: Vec<(String, i32, String)> = failures
            .iter()
            .map(|f| {
                (
                    f.topic.clone(),
                    f.partition,
                    f.error.clone().unwrap_or_default(),
                )
            })
            .collect();
        output::print_json(&execute_json(
            &args.plan,
            plan.partitions.len(),
            Some((accepted, &rejected)),
        ))?;
    }
    if accepted == 0 {
        return Err(KafkaError::admin(format!(
            "The controller rejected all {} partition reassignment(s)",
            results.len()
        )));
    }

    if !json {
        println!(
            "\n{} Controller accepted partition reassignment for {} of {} partitions",
            "✓".green().bold(),
            accepted.to_string().cyan().bold(),
            results.len()
        );
    }
    if !failures.is_empty() {
        return Err(KafkaError::admin(format!(
            "{} partition reassignment(s) were rejected",
            failures.len()
        )));
    }
    if json {
        return Ok(());
    }
    println!(
        "{} Track progress by running: ferrika reassign verify -i {}",
        "ℹ".blue().bold(),
        args.plan.display()
    );

    Ok(())
}

/// A partition the controller rejected: topic, partition and error.
type Rejected = (String, i32, String);

/// `reassign execute -f json`: the plan, and with `submitted` (accepted
/// count; rejected topic, partition and error) what the controller said.
/// Without `submitted` it was a dry run.
fn execute_json(
    plan: &std::path::Path,
    partitions: usize,
    submitted: Option<(usize, &[Rejected])>,
) -> serde_json::Value {
    let mut value = serde_json::json!({
        "plan": plan.display().to_string(),
        "partitions": partitions,
        "executed": submitted.is_some(),
    });
    if let Some((accepted, rejected)) = submitted {
        value["accepted"] = accepted.into();
        value["rejected"] = rejected
            .iter()
            .map(|(topic, partition, error)| {
                serde_json::json!({ "topic": topic, "partition": partition, "error": error })
            })
            .collect::<Vec<_>>()
            .into();
    }
    value
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
    let json = global.format == OutputFormat::Json;

    if !json {
        println!(
            "\n{} Verifying reassignment progress for {} partition(s)...",
            "🔍".cyan(),
            plan.partitions.len().to_string().bold()
        );
    }

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
    let mut rows = Vec::new();
    let admin = KafkaAdminClient::new(&kafka_config)?;
    // The moves the cluster is running: a partition that is neither at its
    // target nor being moved will not get there by waiting.
    let moving = crate::kafka::wire::list_partition_reassignments(&kafka_config).ok();
    let mut not_moving = 0usize;

    for p in &plan.partitions {
        let detail = admin.describe_topic(&p.topic, Some(Duration::from_secs(5)))?;
        let status = detail
            .partitions
            .iter()
            .find(|part| part.partition_id == p.partition)
            .map_or(ReassignmentStatus::Failed, |part| {
                evaluate_partition_status(&part.replicas, &part.isr, &p.replicas)
            });
        let is_moving = moving.as_ref().map(|moves| {
            moves
                .iter()
                .any(|m| m.topic == p.topic && m.partition == p.partition)
        });
        let stalled = stalled(status, is_moving);
        if stalled {
            not_moving += 1;
        }
        let status = if stalled {
            ReassignmentStatus::Failed
        } else {
            status
        };

        statuses.push(status);
        rows.push(serde_json::json!({
            "topic": p.topic,
            "partition": p.partition,
            "target_replicas": p.replicas,
            "status": verify_status_label(status, stalled),
        }));

        let status_cell = match status {
            ReassignmentStatus::Completed => Cell::new("COMPLETED")
                .fg(Color::Green)
                .set_alignment(CellAlignment::Center),
            ReassignmentStatus::InProgress => Cell::new("IN_PROGRESS")
                .fg(Color::Yellow)
                .set_alignment(CellAlignment::Center),
            ReassignmentStatus::Failed if stalled => Cell::new("NOT MOVING")
                .fg(Color::Red)
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

    if json {
        let outcome = verify_outcome(&statuses);
        output::print_json(&serde_json::json!({
            "plan": args.plan.display().to_string(),
            "complete": outcome.is_ok(),
            "not_moving": not_moving,
            "partitions": rows,
        }))?;
        return outcome;
    }

    println!("{table}");
    if not_moving > 0 {
        println!(
            "\n{} {not_moving} partition(s) are not at their target and not being moved \
             (the plan was not executed, was cancelled or was replaced): waiting will not \
             finish them. Run `ferrika reassign execute` with the plan again.",
            "✖".red()
        );
    }

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

/// The status `reassign verify` shows: `NOT_MOVING` for a stalled
/// partition, else the status itself.
fn verify_status_label(status: ReassignmentStatus, stalled: bool) -> String {
    if stalled {
        "NOT_MOVING".to_string()
    } else {
        status.to_string()
    }
}

/// A partition looks part-way (`InProgress`) but the cluster is not moving
/// it (`is_moving == Some(false)`): the move never started or was
/// cancelled. Unknown when the moves could not be listed.
fn stalled(status: ReassignmentStatus, is_moving: Option<bool>) -> bool {
    status == ReassignmentStatus::InProgress && is_moving == Some(false)
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

    if global.format == OutputFormat::Json {
        return output::print_json(&serde_json::json!({
            "output": args.output.display().to_string(),
            "partitions": rollback_plan.partitions.len(),
        }));
    }

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

/// Refuses target brokers the cluster does not know, or that are offline:
/// the broker list also names offline brokers that still host replicas,
/// and a plan placing replicas there cannot complete.
fn check_target_brokers(
    brokers: &[crate::model::cluster::BrokerInfo],
    targets: &[i32],
) -> Result<(), KafkaError> {
    let ids = |pick: &dyn Fn(i32) -> bool| -> Vec<String> {
        targets
            .iter()
            .filter(|id| pick(**id))
            .map(i32::to_string)
            .collect()
    };
    let unknown = ids(&|id| !brokers.iter().any(|b| b.id == id));
    if !unknown.is_empty() {
        return Err(KafkaError::config(format!(
            "Broker id(s) {} are not in the cluster (known: {})",
            unknown.join(", "),
            brokers
                .iter()
                .map(|b| b.id.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )));
    }
    let offline = ids(&|id| brokers.iter().any(|b| b.id == id && !b.is_online));
    if !offline.is_empty() {
        return Err(KafkaError::config(format!(
            "Broker id(s) {} are offline: a plan cannot place replicas on them. \
             Leave them out of -b, or start them first.",
            offline.join(", ")
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn execute_json_says_whether_it_ran_and_what_was_rejected() {
        let path = std::path::Path::new("plan.json");
        let dry = super::execute_json(path, 3, None);
        assert_eq!(dry["executed"], false);
        assert_eq!(dry["partitions"], 3);
        assert!(dry.get("accepted").is_none());
        let rejected = vec![("orders".to_string(), 1, "NOT_CONTROLLER".to_string())];
        let ran = super::execute_json(path, 3, Some((2, &rejected)));
        assert_eq!(ran["executed"], true);
        assert_eq!(ran["accepted"], 2);
        assert_eq!(ran["rejected"][0]["partition"], 1);
        assert_eq!(ran["rejected"][0]["error"], "NOT_CONTROLLER");
    }

    #[test]
    fn verify_labels_stalled_partitions() {
        use super::{verify_status_label, ReassignmentStatus::*};
        assert_eq!(verify_status_label(InProgress, true), "NOT_MOVING");
        assert_eq!(verify_status_label(InProgress, false), "IN_PROGRESS");
        assert_eq!(verify_status_label(Completed, false), "COMPLETED");
    }

    #[test]
    fn a_partition_nobody_moves_is_not_in_progress() {
        use super::{stalled, ReassignmentStatus::*};
        assert!(stalled(InProgress, Some(false)));
        assert!(!stalled(InProgress, Some(true)));
        // Moves could not be listed: keep the broker's word for it.
        assert!(!stalled(InProgress, None));
        assert!(!stalled(Completed, Some(false)));
    }

    #[test]
    fn plans_refuse_unknown_and_offline_target_brokers() {
        use super::check_target_brokers;
        let broker = |id: i32, is_online: bool| crate::model::cluster::BrokerInfo {
            id,
            host: format!("b{id}"),
            port: 9092,
            is_controller: false,
            partition_leaders: 0,
            replicas_hosted: 0,
            is_online,
            log_dir_bytes: None,
            log_dirs: Vec::new(),
        };
        let brokers = [broker(1, true), broker(2, true), broker(3, false)];
        assert!(check_target_brokers(&brokers, &[1, 2]).is_ok());
        let offline = check_target_brokers(&brokers, &[1, 2, 3])
            .unwrap_err()
            .to_string();
        assert!(offline.contains("3 are offline"), "{offline}");
        let unknown = check_target_brokers(&brokers, &[1, 9])
            .unwrap_err()
            .to_string();
        assert!(unknown.contains("9 are not in the cluster"), "{unknown}");
    }

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
