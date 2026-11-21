use clap::{Args, Subcommand};
use colored::Colorize;
use comfy_table::{Cell, CellAlignment, Color};
use serde_json::json;

use crate::cli::record::{self, Change};
use crate::cli::{output, print_connection_hint, GlobalOpts, OutputFormat};
use crate::error::{KafkaError, KafkaResult};
use crate::kafka::admin::KafkaAdminClient;
use crate::kafka::offset_reset::{
    check_group_idle, commit_offset_reset, prepare_offset_reset, OffsetResetPlan, ResetStrategy,
};
use crate::model::history::{PartitionOffset, Reversibility, State};
use crate::ops::history::capture;

/// Command line arguments for consumer group offset management.
#[derive(Args, Debug, Clone)]
pub struct OffsetArgs {
    #[command(subcommand)]
    pub command: OffsetSubcommands,
}

/// Supported subcommands for `ferrika offset`.
#[derive(Subcommand, Debug, Clone, PartialEq)]
pub enum OffsetSubcommands {
    /// Reset consumer group committed offsets (earliest, latest, specific, shift, or timestamp)
    Reset(OffsetResetArgs),
    /// Delete consumer groups and their committed offsets (only groups without members)
    DeleteGroup(DeleteGroupArgs),
    /// Delete a group's committed offsets for one topic, keeping the group
    /// (the group must not be consuming the topic)
    Delete(DeleteOffsetsArgs),
}

/// Arguments for `ferrika offset delete`.
#[derive(Args, Debug, Clone, PartialEq)]
pub struct DeleteOffsetsArgs {
    /// Consumer group ID
    pub group_id: String,

    /// Topic whose committed offsets are removed from the group
    #[arg(short = 't', long)]
    pub topic: String,

    /// Partitions to delete (all committed partitions of the topic if omitted)
    #[arg(short = 'p', long = "partition")]
    pub partitions: Vec<i32>,

    /// Skip the confirmation prompt
    #[arg(short = 'y', long)]
    pub yes: bool,
}

/// Arguments for `ferrika offset delete-group`.
#[derive(Args, Debug, Clone, PartialEq)]
pub struct DeleteGroupArgs {
    /// Consumer group IDs to delete
    #[arg(required = true)]
    pub groups: Vec<String>,

    /// Skip the confirmation prompt
    #[arg(short = 'y', long)]
    pub yes: bool,
}

/// Arguments for `ferrika offset reset`.
#[derive(Args, Debug, Clone, PartialEq)]
pub struct OffsetResetArgs {
    /// Consumer group ID to modify
    pub group_id: String,

    /// Target topic name to reset offsets for
    #[arg(short = 't', long)]
    pub topic: String,

    /// Specific partition to reset (resets all partitions if omitted)
    #[arg(short = 'p', long)]
    pub partition: Option<i32>,

    /// Reset offsets to the earliest available position (beginning)
    #[arg(long, conflicts_with_all = ["to_latest", "to_offset", "shift_by", "to_timestamp"])]
    pub to_earliest: bool,

    /// Reset offsets to the latest high watermark (skip all lag)
    #[arg(long, conflicts_with_all = ["to_earliest", "to_offset", "shift_by", "to_timestamp"])]
    pub to_latest: bool,

    /// Reset offsets to a specific absolute offset number
    #[arg(long, conflicts_with_all = ["to_earliest", "to_latest", "shift_by", "to_timestamp"])]
    pub to_offset: Option<i64>,

    /// Shift offsets by a relative delta (e.g. -100 to rewind, +50 to forward)
    #[arg(long, allow_hyphen_values = true, conflicts_with_all = ["to_earliest", "to_latest", "to_offset", "to_timestamp"])]
    pub shift_by: Option<i64>,

    /// Reset offsets to the position matching an epoch millisecond timestamp
    #[arg(long, conflicts_with_all = ["to_earliest", "to_latest", "to_offset", "shift_by"])]
    pub to_timestamp: Option<i64>,

    /// Actually commit changes to Kafka (defaults to false for safe dry-run preview)
    #[arg(long)]
    pub execute: bool,

    /// Explicit flag to simulate changes without committing
    #[arg(long)]
    pub dry_run: bool,
}

/// The smallest `--to-timestamp` taken (1973-03-03): any smaller value is
/// almost surely seconds, not milliseconds.
const MIN_TIMESTAMP_MS: i64 = 100_000_000_000;

impl OffsetResetArgs {
    /// Extracts the chosen ResetStrategy or returns an error if none was specified.
    pub fn strategy(&self) -> KafkaResult<ResetStrategy> {
        if self.to_earliest {
            Ok(ResetStrategy::Earliest)
        } else if self.to_latest {
            Ok(ResetStrategy::Latest)
        } else if let Some(off) = self.to_offset {
            Ok(ResetStrategy::Specific(off))
        } else if let Some(delta) = self.shift_by {
            Ok(ResetStrategy::Shift(delta))
        } else if let Some(ts) = self.to_timestamp {
            // Seconds (1700000000) read as milliseconds are January 1970, and
            // would rewind every partition to its oldest record.
            if ts < MIN_TIMESTAMP_MS {
                return Err(KafkaError::config(format!(
                    "--to-timestamp {ts} is before 1973: it takes epoch milliseconds \
                     (seconds × 1000, e.g. {})",
                    ts.saturating_mul(1000)
                )));
            }
            Ok(ResetStrategy::Timestamp(ts))
        } else {
            Err(KafkaError::config(
                "Please specify an offset reset strategy: --to-earliest, --to-latest, --to-offset <N>, --shift-by <N>, or --to-timestamp <TS>"
            ))
        }
    }
}

/// Main entry point for `ferrika offset`.
pub async fn run(args: OffsetArgs, global: &GlobalOpts) -> KafkaResult<()> {
    match args.command {
        OffsetSubcommands::Reset(reset_args) => handle_offset_reset(reset_args, global).await,
        OffsetSubcommands::DeleteGroup(delete_args) => {
            handle_delete_groups(delete_args, global).await
        }
        OffsetSubcommands::Delete(args) => handle_delete_offsets(args, global).await,
    }
}

async fn handle_delete_offsets(args: DeleteOffsetsArgs, global: &GlobalOpts) -> KafkaResult<()> {
    let scope = if args.partitions.is_empty() {
        "all partitions".to_string()
    } else {
        let list: Vec<String> = args.partitions.iter().map(i32::to_string).collect();
        format!("partition(s) {}", list.join(", "))
    };
    if !args.yes {
        confirm(
            &format!(
                "Delete the committed offsets of {} for {} ({scope})? [y/N]: ",
                args.group_id.red().bold(),
                args.topic.cyan()
            ),
            "Aborted.",
        )?;
    }
    let config = global.to_kafka_config();
    let captured = capture::group_committed(&config, &args.group_id, Some(&args.topic))
        .await
        .map(|state| {
            let before = only_partitions(state, &args.partitions);
            let after = cleared(&before);
            (vec![before], vec![after])
        });
    let pending = record::start(
        global,
        record::cluster_scope(global, None),
        record::change_from_capture(Reversibility::Reversible, captured),
    );
    let (group, topic, partitions) = (args.group_id.clone(), args.topic.clone(), args.partitions);
    let deleted = tokio::task::spawn_blocking(move || {
        crate::kafka::wire::delete_group_offsets(&config, &group, &topic, &partitions)
    })
    .await
    .map_err(|e| KafkaError::admin(format!("Offset deletion task failed: {e}")))
    .and_then(|r| r);
    record::finish(
        pending,
        &deleted,
        &format!(
            "Deleted the committed offsets of {} on {}",
            args.group_id, args.topic
        ),
    );
    let deleted = deleted?;

    if global.format == OutputFormat::Json {
        let out =
            json!({"group": args.group_id, "topic": args.topic, "deleted_partitions": deleted});
        output::print_json(&out)?;
    } else {
        let list: Vec<String> = deleted.iter().map(i32::to_string).collect();
        println!(
            "{} Deleted the committed offsets of '{}' for '{}' (partition(s) {})",
            "✔".green().bold(),
            args.group_id.cyan(),
            args.topic.cyan(),
            list.join(", ")
        );
    }
    Ok(())
}

async fn handle_delete_groups(args: DeleteGroupArgs, global: &GlobalOpts) -> KafkaResult<()> {
    if !args.yes {
        confirm(
            &format!(
                "Delete consumer group(s) {} and their committed offsets? [y/N]: ",
                args.groups.join(", ").red().bold()
            ),
            "Aborted group deletion.",
        )?;
    }

    let kafka_config = global.to_kafka_config();
    let admin = match KafkaAdminClient::new(&kafka_config) {
        Ok(admin) => admin,
        Err(err) => {
            print_connection_hint(&global.to_kafka_config(), &err);
            return Err(err);
        }
    };

    let scope = record::cluster_scope(global, Some(&admin));
    let mut results = Vec::with_capacity(args.groups.len());
    for (nth, group) in args.groups.iter().enumerate() {
        let captured = capture::group_committed(&kafka_config, group, None)
            .await
            .map(|before| {
                let after = cleared(&before);
                (vec![before], vec![after])
            });
        let pending = record::start_for(
            global,
            scope.clone(),
            record::change_from_capture(Reversibility::Partial, captured).limit(
                "undo commits the group's offsets again; its members and their metadata are not restored",
            ),
            group.clone(),
            nth,
        );
        let outcome = admin.delete_consumer_group(group, None).await;
        record::finish(pending, &outcome, &format!("Deleted group {group}"));
        results.push((group.clone(), outcome.err().map(|e| e.to_string())));
    }
    let failed = results.iter().filter(|(_, err)| err.is_some()).count();

    if global.format == OutputFormat::Json {
        let items: Vec<_> = results
            .iter()
            .map(|(group, err)| json!({"group": group, "deleted": err.is_none(), "error": err}))
            .collect();
        output::print_json(&items)?;
    } else {
        for (group, err) in &results {
            match err {
                None => println!("{} Deleted group '{}'", "✔".green().bold(), group.cyan()),
                Some(e) => eprintln!("{} {}: {e}", "✖".red().bold(), group.cyan()),
            }
        }
    }

    if failed > 0 {
        return Err(KafkaError::admin(format!(
            "{failed} of {} group(s) could not be deleted",
            results.len()
        )));
    }
    Ok(())
}

async fn handle_offset_reset(args: OffsetResetArgs, global: &GlobalOpts) -> KafkaResult<()> {
    let strategy = args.strategy()?;
    let kafka_config = global.to_kafka_config();

    let plans = match prepare_offset_reset(
        &kafka_config,
        &args.group_id,
        &args.topic,
        args.partition,
        &strategy,
    )
    .await
    {
        Ok(p) => p,
        Err(err) => {
            print_connection_hint(&global.to_kafka_config(), &err);
            return Err(err);
        }
    };

    let is_execute = args.execute && !args.dry_run;

    if is_execute {
        // Members' own commits would overwrite the reset (or the commit is
        // rejected), so refuse before touching anything.
        let admin = KafkaAdminClient::new(&kafka_config)?;
        let groups = admin.list_consumer_group_states(None)?;
        let state = groups
            .iter()
            .find(|(name, _, _)| *name == args.group_id)
            .map(|(_, state, members)| (state.as_str(), *members));
        check_group_idle(&args.group_id, state)?;
    }

    // JSON reports what happened, so with --execute it is printed after the
    // commit, saying whether it succeeded.
    let print_json = |executed: bool, error: Option<String>| -> KafkaResult<()> {
        let mut out = json!({
            "group_id": args.group_id,
            "topic": args.topic,
            "strategy": strategy.to_string(),
            "executed": executed,
            "plans": plans,
        });
        if let Some(error) = error {
            out["error"] = json!(error);
        }
        output::print_json(&out)
    };

    if global.format == OutputFormat::Table {
        render_plans_table(&args.group_id, &args.topic, &plans, is_execute);
    }

    if is_execute {
        let pending = record::start(
            global,
            record::cluster_scope(global, None),
            reset_change(&args.group_id, &plans),
        );
        let committed = commit_offset_reset(&kafka_config, &args.group_id, &plans);
        record::finish(
            pending,
            &committed,
            &format!(
                "Committed new offsets for {} on {}",
                args.group_id, args.topic
            ),
        );
        if let Err(err) = committed {
            if global.format == OutputFormat::Json {
                print_json(false, Some(err.to_string()))?;
            }
            return Err(err);
        }
        if global.format == OutputFormat::Json {
            print_json(true, None)?;
        } else {
            println!(
                "\n{} Successfully committed new offsets for group '{}' on topic '{}'.",
                "✔".green().bold(),
                args.group_id.yellow().bold(),
                args.topic.cyan().bold()
            );
        }
    } else if global.format == OutputFormat::Json {
        print_json(false, None)?;
    } else {
        println!(
            "\n{} {}",
            "💡 [DRY-RUN MODE]".yellow().bold(),
            "Offsets were NOT modified. Re-run with '--execute' to commit these changes to the cluster.".bright_white()
        );
    }

    Ok(())
}

/// The record of an offset reset.
fn reset_change(group: &str, plans: &[OffsetResetPlan]) -> Change {
    Change::offset_reset(group, plans)
}

/// `state` (group offsets) limited to `partitions`; all when empty.
fn only_partitions(state: State, partitions: &[i32]) -> State {
    match state {
        State::GroupOffsets { group, offsets } if !partitions.is_empty() => State::GroupOffsets {
            group,
            offsets: offsets
                .into_iter()
                .filter(|p| partitions.contains(&p.partition))
                .collect(),
        },
        other => other,
    }
}

/// The same partitions with no committed offset.
fn cleared(state: &State) -> State {
    match state {
        State::GroupOffsets { group, offsets } => State::GroupOffsets {
            group: group.clone(),
            offsets: offsets
                .iter()
                .map(|p| PartitionOffset {
                    offset: None,
                    ..p.clone()
                })
                .collect(),
        },
        other => other.clone(),
    }
}

/// Asks a yes/no question on stderr (stdout may carry JSON) and reads the
/// answer from stdin. Anything but `y`/`yes`, including end of input, is an
/// error so the command exits non-zero.
pub(crate) fn confirm(prompt: &str, aborted: &str) -> KafkaResult<()> {
    use std::io::Write as _;

    eprint!("{prompt}");
    std::io::stderr().flush().ok();
    let mut input = String::new();
    let read = std::io::stdin()
        .read_line(&mut input)
        .map_err(|e| KafkaError::io(format!("Cannot read confirmation: {e}")))?;
    if read == 0 || !is_yes(&input) {
        return Err(KafkaError::Check(aborted.to_string()));
    }
    Ok(())
}

/// Whether a confirmation answer accepts.
fn is_yes(input: &str) -> bool {
    matches!(input.trim().to_lowercase().as_str(), "y" | "yes")
}

fn render_plans_table(group_id: &str, topic: &str, plans: &[OffsetResetPlan], executed: bool) {
    let title = if executed {
        "Applying Consumer Offset Reset:".green().bold()
    } else {
        "Offset Reset Simulation Plan (Dry-Run):".yellow().bold()
    };

    println!(
        "\n{} Group: {} | Topic: {}\n",
        title,
        group_id.yellow().bold(),
        topic.cyan().bold()
    );

    let mut table = output::table(vec![
        Cell::new("PARTITION")
            .fg(Color::Cyan)
            .set_alignment(CellAlignment::Right),
        Cell::new("CURRENT OFFSET")
            .fg(Color::Cyan)
            .set_alignment(CellAlignment::Right),
        Cell::new("WATERMARKS [LOW..HIGH]")
            .fg(Color::Cyan)
            .set_alignment(CellAlignment::Center),
        Cell::new("TARGET OFFSET")
            .fg(Color::Cyan)
            .set_alignment(CellAlignment::Right),
        Cell::new("DELTA")
            .fg(Color::Cyan)
            .set_alignment(CellAlignment::Right),
    ]);

    for plan in plans {
        let current_str = if plan.current_offset < 0 {
            "<uncommitted>".to_string()
        } else {
            plan.current_offset.to_string()
        };

        let watermark_str = format!("{} .. {}", plan.low_watermark, plan.high_watermark);

        let delta_color = if plan.delta < 0 {
            Color::Yellow // Rewinding history
        } else if plan.delta > 0 {
            Color::Blue // Skipping forward
        } else {
            Color::DarkGrey // Unchanged
        };

        let delta_str = if plan.delta > 0 {
            format!("+{}", plan.delta)
        } else {
            plan.delta.to_string()
        };

        table.add_row(vec![
            Cell::new(plan.partition.to_string())
                .fg(Color::White)
                .set_alignment(CellAlignment::Right),
            Cell::new(current_str)
                .fg(Color::DarkGrey)
                .set_alignment(CellAlignment::Right),
            Cell::new(watermark_str)
                .fg(Color::Blue)
                .set_alignment(CellAlignment::Center),
            Cell::new(plan.target_offset.to_string())
                .fg(Color::Green)
                .set_alignment(CellAlignment::Right),
            Cell::new(delta_str)
                .fg(delta_color)
                .set_alignment(CellAlignment::Right),
        ]);
    }

    println!("{}", table);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_timestamp_in_seconds_is_refused() {
        let args = |ts| OffsetResetArgs {
            group_id: "g".into(),
            topic: "orders".into(),
            partition: None,
            to_earliest: false,
            to_latest: false,
            to_offset: None,
            shift_by: None,
            to_timestamp: Some(ts),
            execute: false,
            dry_run: false,
        };
        let err = args(1_700_000_000).strategy().unwrap_err().to_string();
        assert!(err.contains("1700000000000"), "{err}");
        assert!(matches!(
            args(1_700_000_000_000).strategy(),
            Ok(ResetStrategy::Timestamp(1_700_000_000_000))
        ));
    }

    #[test]
    fn reset_records_uncommitted_partitions_as_none() {
        let plan = |partition, current_offset, target_offset| OffsetResetPlan {
            group_id: "g".into(),
            topic: "orders".into(),
            partition,
            current_offset,
            low_watermark: 0,
            high_watermark: 100,
            target_offset,
            delta: target_offset - current_offset,
        };
        let change = reset_change("g", &[plan(0, 40, 0), plan(1, -1, 0)]);
        let offsets = |state: &State| match state {
            State::GroupOffsets { offsets, .. } => offsets.iter().map(|p| p.offset).collect(),
            _ => Vec::new(),
        };
        assert_eq!(offsets(&change.before[0]), vec![Some(40), None]);
        assert_eq!(offsets(&change.after[0]), vec![Some(0), Some(0)]);
        assert_eq!(offsets(&cleared(&change.before[0])), vec![None, None]);
        assert_eq!(
            offsets(&only_partitions(change.before[0].clone(), &[1])),
            vec![None]
        );
    }

    #[test]
    fn test_clap_offset_args_earliest() {
        use clap::Parser;

        #[derive(Parser, Debug)]
        struct TestCli {
            #[command(subcommand)]
            command: OffsetSubcommands,
        }

        let cli =
            TestCli::try_parse_from(["test", "reset", "my-group", "-t", "orders", "--to-earliest"])
                .unwrap();

        match cli.command {
            OffsetSubcommands::Reset(args) => {
                assert_eq!(args.group_id, "my-group");
                assert_eq!(args.topic, "orders");
                assert!(args.to_earliest);
                assert!(!args.execute);
                assert_eq!(args.strategy().unwrap(), ResetStrategy::Earliest);
            }
            other => panic!("expected reset, got {other:?}"),
        }
    }

    #[test]
    fn test_clap_offset_args_shift_and_execute() {
        use clap::Parser;

        #[derive(Parser, Debug)]
        struct TestCli {
            #[command(subcommand)]
            command: OffsetSubcommands,
        }

        let cli = TestCli::try_parse_from([
            "test",
            "reset",
            "billing-grp",
            "-t",
            "payments",
            "-p",
            "3",
            "--shift-by",
            "-500",
            "--execute",
        ])
        .unwrap();

        match cli.command {
            OffsetSubcommands::Reset(args) => {
                assert_eq!(args.group_id, "billing-grp");
                assert_eq!(args.topic, "payments");
                assert_eq!(args.partition, Some(3));
                assert_eq!(args.shift_by, Some(-500));
                assert!(args.execute);
                assert_eq!(args.strategy().unwrap(), ResetStrategy::Shift(-500));
            }
            other => panic!("expected reset, got {other:?}"),
        }
    }

    #[test]
    fn test_is_yes() {
        assert!(is_yes("y\n"));
        assert!(is_yes(" YES \r\n"));
        assert!(!is_yes(""));
        assert!(!is_yes("n\n"));
        assert!(!is_yes("yep"));
    }

    #[test]
    fn test_clap_delete_group_args() {
        use clap::Parser;

        #[derive(Parser)]
        struct TestCli {
            #[command(subcommand)]
            command: OffsetSubcommands,
        }
        let cli = TestCli::try_parse_from(["test", "delete-group", "g1", "g2", "-y"]).unwrap();
        match cli.command {
            OffsetSubcommands::DeleteGroup(args) => {
                assert_eq!(args.groups, vec!["g1".to_string(), "g2".to_string()]);
                assert!(args.yes);
            }
            other => panic!("expected delete-group, got {other:?}"),
        }
        assert!(TestCli::try_parse_from(["test", "delete-group"]).is_err());
    }
}
