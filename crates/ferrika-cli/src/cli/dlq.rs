use std::path::PathBuf;
use std::time::Duration;

use clap::{Args, Subcommand};
use comfy_table::{Cell, CellAlignment, Color};

use crate::cli::{output, GlobalOpts};
use crate::error::{KafkaError, KafkaResult};
use crate::kafka::bounded_read::{
    read_ranges, read_topic, topic_watermarks, BoundedReadOptions, PartitionRange, TopicMarks,
};
use crate::kafka::producer::{KafkaProducerClient, OwnedProducerRecord};
use crate::ops::dlq::{
    build_redrive_plan_from, DlqAnalysis, DlqMessage, RedrivePlanItem, RedriveStart,
};

#[derive(Debug, Args)]
#[command(
    name = "dlq",
    about = "Dead Letter Queue (DLQ) Manager: inspect error causes, redrive messages, and rescue poison pills"
)]
pub struct DlqArgs {
    #[command(subcommand)]
    pub command: DlqSubcommands,
}

#[derive(Debug, Subcommand)]
pub enum DlqSubcommands {
    /// Inspect failed messages in a DLQ topic, breakdown error categories, and view origin traces
    Inspect(InspectDlqArgs),

    /// Redrive/re-inject failed DLQ messages back to their origin topic (or target override)
    Redrive(RedriveDlqArgs),

    /// Export DLQ messages along with full error context and stacktraces to a JSON file
    Export(ExportDlqArgs),
}

#[derive(Debug, Args)]
pub struct InspectDlqArgs {
    /// Name of the DLQ topic (e.g., 'orders-v1-dlq')
    #[arg(value_name = "TOPIC")]
    pub topic: String,

    /// Specific partition to inspect (default: all partitions)
    #[arg(short, long)]
    pub partition: Option<i32>,

    /// Maximum number of DLQ messages to sample
    #[arg(short = 'n', long, default_value = "50")]
    pub limit: usize,

    /// Output full analysis and sampled messages in JSON format
    #[arg(long)]
    pub json: bool,

    /// Filter messages by error reason substring (e.g. 'timeout', 'deserializ')
    #[arg(long)]
    pub filter_error: Option<String>,
}

#[derive(Debug, Args)]
pub struct RedriveDlqArgs {
    /// Source DLQ topic name
    #[arg(value_name = "TOPIC")]
    pub topic: String,

    /// Override target destination topic (defaults to 'x-original-topic' or inferred topic name)
    #[arg(short = 't', long)]
    pub target_topic: Option<String>,

    /// Maximum number of messages to redrive
    #[arg(short = 'n', long)]
    pub limit: Option<usize>,

    /// Start reading the DLQ here instead of the beginning: `N` for every
    /// partition or `P:N` for one partition (repeatable). A live run prints the
    /// value to pass next time to continue where it stopped.
    #[arg(long = "from-offset", value_name = "[P:]N")]
    pub from_offset: Vec<String>,

    /// Consumer group on the DLQ that records progress: a run starts at the
    /// group's committed offsets and, with --execute, commits a record's
    /// offset only after it was delivered to its target, so a later run with
    /// the same group resumes there instead of sending the records again.
    /// Use a group of its own (no running consumers).
    #[arg(
        short = 'g',
        long,
        value_name = "GROUP",
        conflicts_with = "from_offset"
    )]
    pub group: Option<String>,

    /// Also redrive records that already carry the x-redriven-by marker
    /// (skipped by default so a record is not redriven twice)
    #[arg(long)]
    pub include_redriven: bool,

    /// Actually execute message injection to target topic (simulation dry-run by default)
    #[arg(long)]
    pub execute: bool,

    /// Output redrive summary in JSON format
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct ExportDlqArgs {
    /// Name of the DLQ topic to export
    #[arg(value_name = "TOPIC")]
    pub topic: String,

    /// Output path for exported JSON records
    #[arg(short = 'o', long, value_name = "FILE")]
    pub output: PathBuf,

    /// Maximum number of DLQ messages to export
    #[arg(short = 'n', long, default_value = "500")]
    pub limit: usize,
}

pub async fn handle_dlq(args: DlqArgs, global: &GlobalOpts) -> KafkaResult<()> {
    match args.command {
        DlqSubcommands::Inspect(inspect_args) => run_inspect(inspect_args, global).await,
        DlqSubcommands::Redrive(redrive_args) => run_redrive(redrive_args, global).await,
        DlqSubcommands::Export(export_args) => run_export(export_args, global).await,
    }
}

/// Samples up to `limit` DLQ messages of `topic` (or of one `partition`),
/// read from the start up to the high watermarks captured before reading.
/// Warns on stderr when the read is partial.
async fn sample_dlq_messages(
    topic: &str,
    partition: Option<i32>,
    limit: usize,
    global: &GlobalOpts,
) -> KafkaResult<Vec<DlqMessage>> {
    let config = global.to_kafka_config();
    let opts = BoundedReadOptions::with_max_records(limit);
    let read = read_topic(&config, topic, partition, &opts, |_| true).await?;
    if let Some(warning) = read.partial_warning(topic, false) {
        eprintln!("⚠ {warning}");
    }
    Ok(read
        .records
        .iter()
        .map(|msg| DlqMessage::from_consumed_message(msg, topic))
        .collect())
}

async fn run_inspect(args: InspectDlqArgs, global: &GlobalOpts) -> KafkaResult<()> {
    let mut messages = sample_dlq_messages(&args.topic, args.partition, args.limit, global).await?;

    let policy = crate::governance::policy::current();
    if let Some(ref filter) = args.filter_error {
        retain_error_matches(&mut messages, filter, policy.masking_enforced());
    }

    let analysis = DlqAnalysis::analyze_for_display(&messages, policy.masking_enforced());
    crate::cli::note_masking(policy);
    let messages = shown(messages, policy);

    if global.wants_json(args.json) {
        output::print_json(&serde_json::json!({
            "topic": args.topic,
            "analysis": analysis,
            "messages": messages,
        }))?;
        return Ok(());
    }

    println!("\n🔍 Ferrika Dead Letter Queue Diagnostics: {}", args.topic);
    println!("{}", "━".repeat(70));

    // Summary Table
    let mut summary_table = output::table(vec![
        Cell::new("Metric").fg(Color::Cyan),
        Cell::new("Value").fg(Color::Cyan),
    ]);

    summary_table.add_row(vec![
        Cell::new("DLQ Topic"),
        Cell::new(&args.topic).fg(Color::Yellow),
    ]);
    summary_table.add_row(vec![
        Cell::new("Sampled Failed Records"),
        Cell::new(analysis.total_messages.to_string()).fg(Color::Red),
    ]);
    summary_table.add_row(vec![
        Cell::new("Unique Failed Keys"),
        Cell::new(analysis.unique_keys.to_string()),
    ]);
    summary_table.add_row(vec![
        Cell::new("Total Failed Payload Bytes"),
        Cell::new(format!("{} bytes", analysis.total_bytes)),
    ]);

    // Headers name the source topic per record; the DLQ's own name is only a
    // guess for records without them, so prefer what the records say.
    let origin = match analysis.original_topics.as_slice() {
        [] => crate::ops::dlq::infer_original_topic_from_dlq_name(&args.topic).map_or_else(
            || "Unknown".to_string(),
            |t| format!("{t} (from the DLQ name)"),
        ),
        [(topic, _)] => topic.clone(),
        many => many
            .iter()
            .map(|(topic, n)| format!("{topic} ({n})"))
            .collect::<Vec<_>>()
            .join(", "),
    };
    summary_table.add_row(vec![
        Cell::new("Original Topic"),
        Cell::new(origin).fg(Color::Green),
    ]);

    println!("{summary_table}\n");

    // Breakdown by Error Category
    println!("💥 Error Root Cause Breakdown:");
    let mut error_table = output::fixed_table(vec![
        Cell::new("Root Cause Category").fg(Color::Cyan),
        Cell::new("Count")
            .fg(Color::Cyan)
            .set_alignment(CellAlignment::Right),
        Cell::new("Share %")
            .fg(Color::Cyan)
            .set_alignment(CellAlignment::Right),
    ]);

    for (cat, count) in &analysis.error_categories {
        let pct = if analysis.total_messages > 0 {
            (*count as f64 / analysis.total_messages as f64) * 100.0
        } else {
            0.0
        };
        error_table.add_row(vec![
            Cell::new(cat).fg(Color::Red),
            Cell::new(count.to_string()).set_alignment(CellAlignment::Right),
            Cell::new(format!("{pct:.1}%"))
                .set_alignment(CellAlignment::Right)
                .fg(Color::Yellow),
        ]);
    }
    println!("{error_table}\n");

    // Sampled Messages
    println!("📋 Recent Failed Messages Sample:");
    let mut msg_table = output::fixed_table(vec![
        Cell::new("DLQ [P:O]").fg(Color::Cyan),
        Cell::new("Orig [T:P:O]").fg(Color::Cyan),
        Cell::new("Retries").fg(Color::Cyan),
        Cell::new("Key").fg(Color::Cyan),
        Cell::new("Exception Cause").fg(Color::Cyan),
    ]);

    for m in messages.iter().take(15) {
        let orig_desc = match (&m.original_topic, m.original_partition, m.original_offset) {
            (Some(t), Some(p), Some(o)) => format!("{t} [{p}:{o}]"),
            (Some(t), _, _) => t.clone(),
            _ => "-".to_string(),
        };

        let truncated_err =
            crate::codec::pii::ellipsize(m.exception_message.as_deref().unwrap_or("-"), 50);

        msg_table.add_row(vec![
            Cell::new(format!("{}:{}", m.partition, m.offset)),
            Cell::new(orig_desc).fg(Color::Green),
            Cell::new(
                m.retry_count
                    .map_or_else(|| "-".to_string(), |n| n.to_string()),
            )
            .fg(Color::Yellow),
            Cell::new(m.key.as_deref().unwrap_or("-")),
            Cell::new(truncated_err).fg(Color::Red),
        ]);
    }
    println!("{msg_table}\n");

    Ok(())
}

/// Reads up to `limit` DLQ records for a redrive, each partition starting where
/// `start` says (the beginning by default).
async fn fetch_redrive_messages(
    topic: &str,
    limit: usize,
    start: &RedriveStart,
    global: &GlobalOpts,
) -> KafkaResult<Vec<DlqMessage>> {
    let config = global.to_kafka_config();
    let marks = topic_watermarks(&config, topic, None)?;
    let known = |p: &i32| marks.marks.contains_key(p) || marks.unavailable.contains(p);
    if let Some(missing) = start.partitions.keys().find(|p| !known(p)) {
        return Err(KafkaError::PartitionNotFound {
            topic: topic.to_string(),
            partition: *missing,
        });
    }
    // Up to the high watermarks captured now: records that land in the DLQ
    // while this runs (or that the redrive itself sends back) wait for the
    // next run.
    let ranges = redrive_ranges(&marks, start);
    let mut read = read_ranges(
        &config,
        &ranges,
        &BoundedReadOptions::with_max_records(limit),
        |_| true,
    )
    .await?;
    read.unavailable = marks.unavailable;
    if let Some(warning) = read.partial_warning(topic, false) {
        eprintln!("⚠ {warning}");
    }
    Ok(read
        .records
        .iter()
        .map(|msg| DlqMessage::from_consumed_message(msg, topic))
        .collect())
}

/// Per partition: from the `--from-offset` start (not below the low
/// watermark) up to the high watermark.
fn redrive_ranges(marks: &TopicMarks, start: &RedriveStart) -> Vec<PartitionRange> {
    marks
        .marks
        .iter()
        .map(|(&partition, &(low, high))| PartitionRange {
            topic: marks.topic.clone(),
            partition,
            start: start.offset_for(partition).map_or(low, |o| o.max(low)),
            end: high,
        })
        .filter(|r| r.start < r.end)
        .collect()
}

async fn run_redrive(args: RedriveDlqArgs, global: &GlobalOpts) -> KafkaResult<()> {
    if args.target_topic.as_deref() == Some(args.topic.as_str()) {
        return Err(KafkaError::config(format!(
            "Redrive target '{}' is the source DLQ itself; records would land back in it",
            args.topic
        )));
    }
    let limit = args.limit.unwrap_or(100);
    let start = match &args.group {
        Some(group) => group_start(&global.to_kafka_config(), group, &args.topic)?,
        None => RedriveStart::parse(&args.from_offset).map_err(KafkaError::Config)?,
    };
    let messages = fetch_redrive_messages(&args.topic, limit, &start, global).await?;
    let examined: Vec<(i32, i64)> = messages.iter().map(|m| (m.partition, m.offset)).collect();

    let is_simulation = !args.execute;
    // Without -t, a record with no origin header goes to a topic guessed
    // from the DLQ's name (`dlq-s3-sink` -> `s3-sink`): only on request.
    // (Records skipped as already redriven, or with no guess at all, are not
    // sent anyway.)
    let guessed = messages
        .iter()
        .filter(|m| {
            !crate::ops::dlq::names_original_topic(&m.raw_headers)
                && m.original_topic.is_some()
                && (args.include_redriven || !m.already_redriven())
        })
        .count();
    if !is_simulation && args.target_topic.is_none() && guessed > 0 {
        let target = crate::ops::dlq::infer_original_topic_from_dlq_name(&args.topic);
        return Err(KafkaError::config(format!(
            "{guessed} record(s) carry no original-topic header{}. Pass -t <topic> to choose \
             where they go.",
            target.map_or_else(String::new, |t| format!(
                "; their target '{t}' is only guessed from the DLQ's name"
            ))
        )));
    }
    let (plan_items, summary) = build_redrive_plan_from(
        Some(&args.topic),
        &messages,
        args.target_topic.as_deref(),
        is_simulation,
        !args.include_redriven,
    );
    if let Some(note) = rerun_note(is_simulation, args.group.as_deref()) {
        // On stderr: JSON on stdout stays parseable.
        eprintln!("{note}");
    }
    let commit = match (&args.group, is_simulation) {
        (Some(group), false) => Some(GroupCommit::new(global, group, &args.topic)?),
        _ => None,
    };

    if global.wants_json(args.json) {
        // JSON mode must still publish with --execute; it only changes the report.
        let outcome = if is_simulation {
            None
        } else {
            Some(publish_redrive(plan_items, &examined, commit.as_ref(), global).await?)
        };
        let mut json = serde_json::to_value(&summary).map_err(|e| {
            KafkaError::Serialization(format!("Failed to serialize redrive summary: {e}"))
        })?;
        if let (Some(obj), Some(outcome)) = (json.as_object_mut(), &outcome) {
            obj.insert("published".into(), outcome.published.into());
            obj.insert("failed".into(), outcome.failed.into());
            if args.group.is_none() {
                let next = start.resume_after(&examined, &outcome.failed_positions);
                obj.insert("next_from_offset".into(), next.to_args().into());
            }
        }
        if let (Some(obj), Some(group)) = (json.as_object_mut(), &args.group) {
            obj.insert("group".into(), group.clone().into());
        }
        output::print_json(&json)?;
        return outcome.map_or(Ok(()), RedriveOutcome::into_result);
    }

    println!("\n🔄 Ferrika DLQ Redrive Orchestrator: {}", args.topic);
    println!("{}", "━".repeat(70));

    if is_simulation {
        println!("⚠️  DRY-RUN SIMULATION MODE (No messages will be re-injected without --execute)");
    } else {
        println!("🚀 LIVE REDRIVE EXECUTION (Re-injecting failed messages to destination topics)");
    }

    let mut table = output::fixed_table(vec![
        Cell::new("Operation Attribute").fg(Color::Cyan),
        Cell::new("Details").fg(Color::Cyan),
    ]);

    table.add_row(vec![
        Cell::new("Source DLQ Topic"),
        Cell::new(&args.topic).fg(Color::Yellow),
    ]);
    if let Some(group) = &args.group {
        table.add_row(vec![
            Cell::new("Consumer Group (progress)"),
            Cell::new(group).fg(Color::Yellow),
        ]);
    }
    table.add_row(vec![
        Cell::new("Analyzed DLQ Records"),
        Cell::new(summary.total_examined.to_string()),
    ]);
    table.add_row(vec![
        Cell::new("Eligible for Redrive"),
        Cell::new(summary.planned_redrives.to_string()).fg(Color::Green),
    ]);
    table.add_row(vec![
        Cell::new("Skipped (Unresolved Destination)"),
        Cell::new(summary.skipped_unresolved_topic.to_string()).fg(Color::Red),
    ]);
    table.add_row(vec![
        Cell::new("Skipped (Already Redriven)"),
        Cell::new(summary.skipped_already_redriven.to_string()).fg(Color::Yellow),
    ]);
    table.add_row(vec![
        Cell::new("Execution Mode"),
        if is_simulation {
            Cell::new("Dry-Run Simulation").fg(Color::Yellow)
        } else {
            Cell::new("Live Production Execution").fg(Color::Green)
        },
    ]);

    println!("{table}\n");

    if !summary.target_topic_distribution.is_empty() {
        println!("🎯 Destination Topics Distribution:");
        let mut dist_table = output::fixed_table(vec![
            Cell::new("Target Topic").fg(Color::Cyan),
            Cell::new("Redrive Count")
                .fg(Color::Cyan)
                .set_alignment(CellAlignment::Right),
        ]);
        for (topic, count) in &summary.target_topic_distribution {
            dist_table.add_row(vec![
                Cell::new(topic).fg(Color::Green),
                Cell::new(count.to_string()).set_alignment(CellAlignment::Right),
            ]);
        }
        println!("{dist_table}\n");
    }

    if is_simulation {
        if guessed > 0 && args.target_topic.is_none() {
            println!(
                "💡 {guessed} record(s) carry no original-topic header, so their target is a guess. \
                 Choose it to perform the live redrive:\n   ferrika dlq redrive {} -t <topic> --execute\n",
                args.topic
            );
        } else {
            let group = args
                .group
                .as_ref()
                .map_or_else(String::new, |g| format!(" -g {g}"));
            println!("💡 Run with '--execute' to perform the live redrive:\n   ferrika dlq redrive {}{group} --execute\n", args.topic);
        }
        return Ok(());
    }

    let outcome = publish_redrive(plan_items, &examined, commit.as_ref(), global).await?;
    println!(
        "✅ Redrive Complete: {} messages published successfully, {} failed.",
        outcome.published, outcome.failed
    );
    if let Some(group) = &args.group {
        println!(
            "   Progress is committed to group '{group}': run the same command again to continue.\n"
        );
        return outcome.into_result();
    }
    let next = start.resume_after(&examined, &outcome.failed_positions);
    let next_args: Vec<String> = next
        .to_args()
        .iter()
        .map(|a| format!("--from-offset {a}"))
        .collect();
    if next_args.is_empty() {
        println!();
    } else {
        println!(
            "   Continue with: ferrika dlq redrive {} --execute {}\n",
            args.topic,
            next_args.join(" ")
        );
    }
    outcome.into_result()
}

/// Result of publishing a redrive plan.
struct RedriveOutcome {
    published: usize,
    failed: usize,
    first_error: Option<String>,
    /// DLQ `(partition, offset)` of every record that failed to publish
    failed_positions: Vec<(i32, i64)>,
}

impl RedriveOutcome {
    /// A partly failed redrive is an error, so scripts notice it.
    fn into_result(self) -> KafkaResult<()> {
        match self.first_error {
            Some(reason) if self.failed > 0 => Err(KafkaError::Check(format!(
                "{} of {} redrive messages failed to publish; first error: {reason}",
                self.failed,
                self.failed + self.published
            ))),
            _ => Ok(()),
        }
    }
}

/// The note for a live redrive without `-g`: nothing records how far it
/// got, so running it again sends the same records again.
fn rerun_note(is_simulation: bool, group: Option<&str>) -> Option<&'static str> {
    (!is_simulation && group.is_none()).then_some(
        "Note: without -g/--group no progress is recorded; running this redrive again \
         resends the same records. Pass -g <group> (or the --from-offset value printed \
         at the end) to continue where it stopped.",
    )
}

/// Where a redrive with `-g` starts: per partition of `topic`, the group's
/// committed offset; partitions without one start at the beginning.
fn group_start(
    config: &crate::kafka::config::KafkaClientConfig,
    group: &str,
    topic: &str,
) -> KafkaResult<RedriveStart> {
    let mut all = crate::kafka::wire::fetch_group_offsets(config, &[group.to_string()])?;
    let committed = all.remove(group).unwrap_or_default();
    Ok(start_from_committed(&committed, topic))
}

/// [`group_start`] from the group's committed offsets.
fn start_from_committed(
    committed: &std::collections::HashMap<(String, i32), i64>,
    topic: &str,
) -> RedriveStart {
    RedriveStart {
        default: None,
        partitions: committed
            .iter()
            .filter(|((t, _), offset)| t == topic && **offset >= 0)
            .map(|((_, partition), offset)| (*partition, *offset))
            .collect(),
    }
}

/// Commits the progress of `dlq redrive -g` to its consumer group on the
/// DLQ, without joining the group.
struct GroupCommit {
    consumer: crate::kafka::consumer::KafkaConsumerClient,
    group: String,
    topic: String,
}

impl GroupCommit {
    fn new(global: &GlobalOpts, group: &str, topic: &str) -> KafkaResult<Self> {
        let config = global.to_kafka_config().with_group_id(group);
        Ok(Self {
            consumer: crate::kafka::consumer::KafkaConsumerClient::new(&config)?,
            group: group.to_string(),
            topic: topic.to_string(),
        })
    }

    /// Commits `next` (partition -> next offset to read).
    fn commit(&self, next: &std::collections::BTreeMap<i32, i64>) -> KafkaResult<()> {
        if next.is_empty() {
            return Ok(());
        }
        let mut tpl = rdkafka::TopicPartitionList::new();
        for (&partition, &offset) in next {
            tpl.add_partition_offset(&self.topic, partition, rdkafka::Offset::Offset(offset))
                .map_err(|e| KafkaError::Offset(e.to_string()))?;
        }
        self.consumer
            .commit(&tpl, rdkafka::consumer::CommitMode::Sync)
            .map_err(|e| {
                KafkaError::Offset(format!(
                    "could not commit the redrive progress to group '{}': {e}; records \
                     delivered since the last commit are sent again by the next run",
                    self.group
                ))
            })
    }
}

/// Records sent between two progress commits of `dlq redrive -g`.
const COMMIT_EVERY: usize = 50;

/// What a redrive with a group commits as it goes: per partition, the
/// offset after the last record that is delivered (or needed no sending)
/// with every record before it, never past a record that failed (the
/// mirror's [`crate::ops::mirror::CommitTracker`] rule).
#[derive(Default)]
struct RedriveProgress {
    tracker: crate::ops::mirror::CommitTracker,
    /// Next offsets not committed yet.
    uncommitted: std::collections::BTreeMap<i32, i64>,
}

impl RedriveProgress {
    /// The record at `(partition, offset)` is finished: `sent` when it was
    /// handed to the producer, `done` when it was delivered or needed no
    /// sending.
    fn finished(&mut self, (partition, offset): (i32, i64), sent: bool, done: bool) {
        if sent {
            self.tracker.sent(partition, offset);
        }
        if let Some(last) = self.tracker.finished(partition, offset, done) {
            self.uncommitted.insert(partition, last + 1);
        }
    }
}

/// Publishes every planned message to its destination topic, in the order
/// the DLQ records were read (`examined`). With `commit`, the group's
/// offsets follow the deliveries (see [`RedriveProgress`]).
async fn publish_redrive(
    plan_items: Vec<RedrivePlanItem>,
    examined: &[(i32, i64)],
    commit: Option<&GroupCommit>,
    global: &GlobalOpts,
) -> KafkaResult<RedriveOutcome> {
    let config = global.to_kafka_config();
    let producer = KafkaProducerClient::new(&config)?;
    let mut outcome = RedriveOutcome {
        published: 0,
        failed: 0,
        first_error: None,
        failed_positions: Vec::new(),
    };
    let mut planned: std::collections::HashMap<(i32, i64), RedrivePlanItem> = plan_items
        .into_iter()
        .map(|item| ((item.source_partition, item.source_offset), item))
        .collect();
    let mut progress = RedriveProgress::default();
    let mut since_commit = 0usize;

    for &position in examined {
        // Skipped records (already redriven, no destination) need no
        // sending; they are finished as they are.
        let Some(item) = planned.remove(&position) else {
            progress.finished(position, false, true);
            continue;
        };
        let mut record = OwnedProducerRecord::new(&item.target_topic);
        record.key = item.key;
        record.payload = item.payload;
        for (hk, hv) in item.headers {
            record = record.with_header(hk, hv);
        }

        let delivered = match producer.send(&record, Some(Duration::from_secs(5))).await {
            Ok(_) => {
                outcome.published += 1;
                true
            }
            Err(e) => {
                outcome.failed += 1;
                outcome.first_error.get_or_insert_with(|| e.to_string());
                outcome.failed_positions.push(position);
                false
            }
        };
        progress.finished(position, true, delivered);
        since_commit += 1;
        if let Some(commit) = commit {
            if since_commit >= COMMIT_EVERY {
                // A failed commit is retried with the next one.
                match commit.commit(&progress.uncommitted) {
                    Ok(()) => progress.uncommitted.clear(),
                    Err(e) => tracing::warn!("{e}"),
                }
                since_commit = 0;
            }
        }
    }
    if let Some(commit) = commit {
        commit.commit(&progress.uncommitted)?;
    }
    Ok(outcome)
}

async fn run_export(args: ExportDlqArgs, global: &GlobalOpts) -> KafkaResult<()> {
    let messages = sample_dlq_messages(&args.topic, None, args.limit, global).await?;
    let policy = crate::governance::policy::current();
    crate::cli::note_masking(policy);
    let mask = policy.masking_enforced();
    let records: Vec<serde_json::Value> = messages
        .iter()
        .map(|message| export_record(message, mask))
        .collect();

    let json = serde_json::to_string_pretty(&records).map_err(|e| {
        KafkaError::Serialization(format!("Failed to serialize DLQ export records: {e}"))
    })?;

    if let Some(parent) = args.output.parent() {
        if !parent.as_os_str().is_empty() {
            let _ = std::fs::create_dir_all(parent);
        }
    }

    write_private(&args.output, json.as_bytes()).map_err(|e| {
        KafkaError::Serialization(format!(
            "Failed to write export file '{}': {e}",
            args.output.display()
        ))
    })?;
    crate::cli::export_finished(messages.len() as u64);

    println!(
        "✅ Exported {} DLQ records with failure diagnostics to '{}'\n",
        messages.len(),
        args.output.display()
    );

    Ok(())
}

/// One exported record: the message (masked while the policy enforces
/// masking) with its whole value, `payload` as text or `payload_base64` when
/// it is not UTF-8 (`payload` is then null, as for a record without a
/// value). Masked, a binary value becomes a "hidden" placeholder text. The
/// preview alone cut values at 120 characters.
fn export_record(message: &DlqMessage, mask: bool) -> serde_json::Value {
    use crate::codec::pii::{mask_bytes, MaskStrategy};
    let shown = if mask {
        message.masked()
    } else {
        message.clone()
    };
    let mut record = serde_json::to_value(&shown).unwrap_or_default();
    if let Some(obj) = record.as_object_mut() {
        let bytes = (!message.payload_is_null).then_some(message.raw_payload.as_slice());
        let (text, base64) = match bytes {
            None => (None, None),
            Some(bytes) if mask => (Some(mask_bytes(bytes, MaskStrategy::Partial)), None),
            Some(bytes) => match std::str::from_utf8(bytes) {
                Ok(text) => (Some(text.to_string()), None),
                Err(_) => (None, Some(crate::codec::base64::encode(bytes))),
            },
        };
        obj.insert("payload".into(), text.into());
        if let Some(b64) = base64 {
            obj.insert("payload_base64".into(), b64.into());
        }
    }
    record
}

/// Keeps the messages whose exception message contains `filter`
/// (case-insensitive). When masking is enforced the text searched is the
/// masked one, so the filter and the counts it changes cannot reveal what
/// masking hides.
fn retain_error_matches(messages: &mut Vec<DlqMessage>, filter: &str, mask: bool) {
    let filter_lower = filter.to_lowercase();
    messages.retain(|m| {
        m.exception_message.as_deref().is_some_and(|e| {
            let text = if mask {
                crate::codec::pii::mask_payload(e, crate::codec::pii::MaskStrategy::Partial)
            } else {
                e.to_string()
            };
            text.to_lowercase().contains(&filter_lower)
        })
    });
}

/// Writes an export file readable by its owner only (unix 0600).
fn write_private(path: &std::path::Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    crate::files::open_private_file(path, false)?.write_all(bytes)
}

/// The messages as `inspect` and `export` may show them: masked while the
/// company policy enforces masking, unchanged otherwise.
fn shown(
    messages: Vec<DlqMessage>,
    policy: &crate::governance::policy::EffectivePolicy,
) -> Vec<DlqMessage> {
    if policy.masking_enforced() {
        messages.iter().map(DlqMessage::masked).collect()
    } else {
        messages
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser)]
    struct Wrapper {
        #[command(subcommand)]
        cmd: DlqSubcommands,
    }

    #[test]
    fn redrive_group_starts_at_the_committed_offsets_of_its_topic() {
        let committed: std::collections::HashMap<(String, i32), i64> = [
            (("dlq".to_string(), 0), 7),
            (("dlq".to_string(), 2), 3),
            (("other".to_string(), 1), 9),
        ]
        .into_iter()
        .collect();
        let start = start_from_committed(&committed, "dlq");
        assert_eq!(start.default, None);
        assert_eq!(start.offset_for(0), Some(7));
        assert_eq!(start.offset_for(1), None);
        assert_eq!(start.offset_for(2), Some(3));
    }

    #[test]
    fn redrive_progress_commits_only_delivered_records() {
        let mut progress = RedriveProgress::default();
        // 0 skipped, 1 delivered: next read starts at 2.
        progress.finished((0, 0), false, true);
        progress.finished((0, 1), true, true);
        assert_eq!(progress.uncommitted.get(&0), Some(&2));
        // 2 fails: nothing from 2 on is committed, even after 3 is delivered.
        progress.finished((0, 2), true, false);
        progress.finished((0, 3), true, true);
        assert_eq!(progress.uncommitted.get(&0), Some(&2));
        // Partitions are kept apart.
        progress.finished((1, 5), true, true);
        assert_eq!(progress.uncommitted.get(&1), Some(&6));
    }

    #[test]
    fn a_live_redrive_without_a_group_warns_about_reruns() {
        assert!(rerun_note(false, None).is_some());
        assert!(rerun_note(false, Some("redrive")).is_none());
        assert!(rerun_note(true, None).is_none());
    }

    #[test]
    fn redrive_group_parses_and_excludes_from_offset() {
        let parsed = Wrapper::parse_from(["dlq", "redrive", "orders-dlq", "-g", "redrive-orders"]);
        let DlqSubcommands::Redrive(args) = parsed.cmd else {
            panic!("Expected Redrive subcommand");
        };
        assert_eq!(args.group.as_deref(), Some("redrive-orders"));
        assert!(Wrapper::try_parse_from([
            "dlq",
            "redrive",
            "orders-dlq",
            "-g",
            "g",
            "--from-offset",
            "3"
        ])
        .is_err());
    }

    #[test]
    fn redrive_reads_from_the_start_offset_up_to_the_high_watermark() {
        let marks = TopicMarks {
            topic: "dlq".into(),
            marks: [(0, (0, 10)), (1, (5, 8)), (2, (0, 4)), (3, (2, 2))]
                .into_iter()
                .collect(),
            unavailable: vec![],
        };
        let start = RedriveStart::parse(&["3".into(), "1:2".into(), "2:9".into()]).unwrap();
        let ranges: Vec<(i32, i64, i64)> = redrive_ranges(&marks, &start)
            .iter()
            .map(|r| (r.partition, r.start, r.end))
            .collect();
        // p0 from the default 3; p1 from its low watermark (2 is gone);
        // p2 starts past its end and p3 is empty, so neither is read.
        assert_eq!(ranges, vec![(0, 3, 10), (1, 5, 8)]);
        let from_beginning: Vec<i64> = redrive_ranges(&marks, &RedriveStart::default())
            .iter()
            .map(|r| r.start)
            .collect();
        assert_eq!(from_beginning, vec![0, 5, 0]);
    }

    #[test]
    fn test_clap_dlq_args_inspect_parsing() {
        let parsed = Wrapper::parse_from([
            "dlq",
            "inspect",
            "orders-v1-dlq",
            "-n",
            "100",
            "--filter-error",
            "timeout",
            "--json",
        ]);
        match parsed.cmd {
            DlqSubcommands::Inspect(args) => {
                assert_eq!(args.topic, "orders-v1-dlq");
                assert_eq!(args.limit, 100);
                assert_eq!(args.filter_error.as_deref(), Some("timeout"));
                assert!(args.json);
            }
            _ => panic!("Expected Inspect subcommand"),
        }
    }

    #[test]
    fn test_clap_dlq_args_redrive_and_export_parsing() {
        let parsed_redrive = Wrapper::parse_from([
            "dlq",
            "redrive",
            "orders-v1-dlq",
            "-t",
            "orders-v1",
            "--execute",
            "--from-offset",
            "100",
            "--from-offset",
            "2:350",
            "--include-redriven",
        ]);
        match parsed_redrive.cmd {
            DlqSubcommands::Redrive(args) => {
                assert_eq!(args.topic, "orders-v1-dlq");
                assert_eq!(args.target_topic.as_deref(), Some("orders-v1"));
                assert!(args.execute);
                assert_eq!(args.from_offset, vec!["100", "2:350"]);
                assert!(args.include_redriven);
            }
            _ => panic!("Expected Redrive subcommand"),
        }

        let parsed_export = Wrapper::parse_from([
            "dlq",
            "export",
            "payments-dlq",
            "-o",
            "target/dlq-dump.json",
            "-n",
            "250",
        ]);
        match parsed_export.cmd {
            DlqSubcommands::Export(args) => {
                assert_eq!(args.topic, "payments-dlq");
                assert_eq!(args.output, PathBuf::from("target/dlq-dump.json"));
                assert_eq!(args.limit, 250);
            }
            _ => panic!("Expected Export subcommand"),
        }
    }

    #[test]
    fn inspect_and_export_mask_pii_only_under_an_enforcing_policy() {
        use crate::governance::test_policy::{leaks, masking, none, PII};
        let msg = crate::kafka::consumer::ConsumedMessage {
            topic: "orders-dlq".into(),
            partition: 0,
            offset: 3,
            key: Some(PII.as_bytes().to_vec()),
            payload: Some(format!(r#"{{"note":"{PII}"}}"#).into_bytes()),
            timestamp_ms: None,
            headers: Vec::from([(
                "x-exception-message".to_string(),
                format!("bad contact {PII}").into_bytes(),
            )]),
        };
        let messages = vec![DlqMessage::from_consumed_message(&msg, "orders-dlq")];
        let as_json = |m: &[DlqMessage]| serde_json::to_string(m).unwrap();

        let plain = shown(messages.clone(), &none());
        assert!(leaks(&as_json(&plain)));

        let masked = shown(messages, &masking());
        let text = as_json(&masked);
        assert!(!leaks(&text), "{text}");
        assert!(masked[0].exception_message.is_some());
        assert!(masked[0]
            .payload_preview
            .as_deref()
            .unwrap()
            .contains("연락처"));
    }

    fn failed(exception: &str) -> DlqMessage {
        let consumed = crate::kafka::consumer::ConsumedMessage {
            topic: "t-dlq".into(),
            partition: 0,
            offset: 1,
            timestamp_ms: None,
            key: None,
            payload: Some(b"{}".to_vec()),
            headers: vec![(
                "exception-message".to_string(),
                exception.as_bytes().to_vec(),
            )],
        };
        DlqMessage::from_consumed_message(&consumed, "t-dlq")
    }

    fn with_payload(payload: Option<&[u8]>) -> DlqMessage {
        let consumed = crate::kafka::consumer::ConsumedMessage {
            topic: "t-dlq".into(),
            partition: 0,
            offset: 1,
            timestamp_ms: None,
            key: None,
            payload: payload.map(<[u8]>::to_vec),
            headers: Vec::new(),
        };
        DlqMessage::from_consumed_message(&consumed, "t-dlq")
    }

    #[test]
    fn export_keeps_the_whole_value() {
        let long = format!(
            "{{\"note\":\"{}\",\"email\":\"kim@example.com\"}}",
            "x".repeat(500)
        );
        let record = export_record(&with_payload(Some(long.as_bytes())), false);
        assert_eq!(record["payload"], long.as_str());
        assert!(record.get("payload_base64").is_none());

        let binary = [0xff, 0x00, 0xfe];
        let record = export_record(&with_payload(Some(&binary)), false);
        assert!(record["payload"].is_null());
        assert_eq!(record["payload_base64"], "/wD+");

        let record = export_record(&with_payload(None), false);
        assert!(record["payload"].is_null());
        assert!(record.get("payload_base64").is_none());
        let record = export_record(&with_payload(Some(b"")), false);
        assert_eq!(record["payload"], "");

        let record = export_record(&with_payload(Some(long.as_bytes())), true);
        let masked = record["payload"].as_str().unwrap();
        assert!(!masked.contains("kim@example.com"), "{masked}");
        assert!(masked.len() > 400, "the masked value is whole too");
    }

    #[test]
    fn error_filter_cannot_probe_masked_text() {
        let secret = "login failed for kim@example.com";
        let mut open = vec![failed(secret), failed("timeout")];
        retain_error_matches(&mut open, "kim@example.com", false);
        assert_eq!(open.len(), 1);

        let mut enforced = vec![failed(secret), failed("timeout")];
        retain_error_matches(&mut enforced, "kim@example.com", true);
        assert!(enforced.is_empty(), "masked text must not match the email");
        let mut enforced = vec![failed(secret), failed("timeout")];
        retain_error_matches(&mut enforced, "login failed", true);
        assert_eq!(enforced.len(), 1);
    }

    #[cfg(unix)]
    #[test]
    fn export_file_is_owner_only() {
        use std::os::unix::fs::PermissionsExt;
        let dir = std::env::current_dir()
            .unwrap()
            .join(format!("target/test-private-export-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("dlq-export.json");
        let _ = std::fs::remove_file(&path);
        write_private(&path, b"[]").unwrap();
        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
        // An existing world-readable file is narrowed on rewrite.
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        write_private(&path, b"[]").unwrap();
        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
