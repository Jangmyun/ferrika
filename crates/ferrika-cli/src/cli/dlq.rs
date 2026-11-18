use rdkafka::Offset;
use std::path::PathBuf;
use std::time::Duration;

use clap::{Args, Subcommand};
use comfy_table::{Cell, CellAlignment, Color};

use crate::cli::{output, GlobalOpts};
use crate::error::{KafkaError, KafkaResult};
use crate::kafka::bounded_read::{read_topic, BoundedReadOptions};
use crate::kafka::consumer::KafkaConsumerClient;
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

/// Fetches DLQ messages from the configured cluster.
async fn fetch_dlq_messages(
    topic: &str,
    limit: usize,
    global: &GlobalOpts,
) -> KafkaResult<Vec<DlqMessage>> {
    let mut messages = Vec::new();
    let config = global.to_kafka_config();
    let client = KafkaConsumerClient::scanner(&config)?;
    client.assign_topic(topic, Offset::Beginning)?;
    let timeout = Duration::from_millis(800);
    for _ in 0..limit {
        match tokio::time::timeout(timeout, client.recv()).await {
            Ok(Ok(msg)) => {
                messages.push(DlqMessage::from_consumed_message(&msg, topic));
            }
            Ok(Err(error)) => return Err(error),
            Err(_) => break,
        }
    }

    Ok(messages)
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
    let client = KafkaConsumerClient::scanner(&config)?;
    let partitions = client.topic_partitions(topic, None)?;
    if let Some(missing) = start.partitions.keys().find(|p| !partitions.contains(p)) {
        return Err(KafkaError::PartitionNotFound {
            topic: topic.to_string(),
            partition: *missing,
        });
    }
    let assignments: Vec<(&str, i32, Offset)> = partitions
        .iter()
        .map(|&p| {
            let offset = start
                .offset_for(p)
                .map_or(Offset::Beginning, Offset::Offset);
            (topic, p, offset)
        })
        .collect();
    client.assign_with_offsets(&assignments)?;

    let mut messages = Vec::new();
    let timeout = Duration::from_millis(800);
    for _ in 0..limit {
        match tokio::time::timeout(timeout, client.recv()).await {
            Ok(Ok(msg)) => messages.push(DlqMessage::from_consumed_message(&msg, topic)),
            Ok(Err(error)) => return Err(error),
            Err(_) => break,
        }
    }
    Ok(messages)
}

async fn run_redrive(args: RedriveDlqArgs, global: &GlobalOpts) -> KafkaResult<()> {
    if args.target_topic.as_deref() == Some(args.topic.as_str()) {
        return Err(KafkaError::config(format!(
            "Redrive target '{}' is the source DLQ itself; records would land back in it",
            args.topic
        )));
    }
    let limit = args.limit.unwrap_or(100);
    let start = RedriveStart::parse(&args.from_offset).map_err(KafkaError::Config)?;
    let messages = fetch_redrive_messages(&args.topic, limit, &start, global).await?;
    let examined: Vec<(i32, i64)> = messages.iter().map(|m| (m.partition, m.offset)).collect();

    let is_simulation = !args.execute;
    let (plan_items, summary) = build_redrive_plan_from(
        Some(&args.topic),
        &messages,
        args.target_topic.as_deref(),
        is_simulation,
        !args.include_redriven,
    );

    if global.wants_json(args.json) {
        // JSON mode must still publish with --execute; it only changes the report.
        let outcome = if is_simulation {
            None
        } else {
            Some(publish_redrive(plan_items, global).await?)
        };
        let mut json = serde_json::to_value(&summary).map_err(|e| {
            KafkaError::Serialization(format!("Failed to serialize redrive summary: {e}"))
        })?;
        if let (Some(obj), Some(outcome)) = (json.as_object_mut(), &outcome) {
            obj.insert("published".into(), outcome.published.into());
            obj.insert("failed".into(), outcome.failed.into());
            let next = start.resume_after(&examined, &outcome.failed_positions);
            obj.insert("next_from_offset".into(), next.to_args().into());
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
        println!("💡 Run with '--execute' to perform the live redrive:\n   ferrika dlq redrive {} --execute\n", args.topic);
        return Ok(());
    }

    let outcome = publish_redrive(plan_items, global).await?;
    println!(
        "✅ Redrive Complete: {} messages published successfully, {} failed.",
        outcome.published, outcome.failed
    );
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

/// Publishes every planned message to its destination topic.
async fn publish_redrive(
    plan_items: Vec<RedrivePlanItem>,
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

    for item in plan_items {
        let mut record = OwnedProducerRecord::new(&item.target_topic);
        record.key = item.key;
        record.payload = item.payload;
        for (hk, hv) in item.headers {
            record = record.with_header(hk, hv);
        }

        match producer.send(&record, Some(Duration::from_secs(5))).await {
            Ok(_) => outcome.published += 1,
            Err(e) => {
                outcome.failed += 1;
                outcome.first_error.get_or_insert_with(|| e.to_string());
                outcome
                    .failed_positions
                    .push((item.source_partition, item.source_offset));
            }
        }
    }
    Ok(outcome)
}

async fn run_export(args: ExportDlqArgs, global: &GlobalOpts) -> KafkaResult<()> {
    let messages = fetch_dlq_messages(&args.topic, args.limit, global).await?;
    let policy = crate::governance::policy::current();
    crate::cli::note_masking(policy);
    let messages = shown(messages, policy);

    let json = serde_json::to_string_pretty(&messages).map_err(|e| {
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
