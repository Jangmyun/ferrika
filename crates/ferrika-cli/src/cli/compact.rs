use std::path::PathBuf;

use clap::{Args, Subcommand};
use colored::Colorize;
use comfy_table::{Cell, CellAlignment, Color};

use crate::cli::{output, GlobalOpts};
use crate::codec::pii::{ellipsize, mask_bytes, mask_payload, MaskStrategy};
use crate::error::KafkaResult;
use crate::kafka::bounded_read::{read_topic, BoundedReadOptions};
use crate::kafka::config::{AutoOffsetReset, KafkaClientConfig};
use crate::kafka::consumer::ConsumedMessage;
use crate::ops::compact::{CompactedRecord, CompactedStateSnapshot, CompactionMetrics};

/// Command line arguments for Kafka Log Compaction Analysis and Snapshotting.
#[derive(Args, Debug, Clone)]
pub struct CompactArgs {
    #[command(subcommand)]
    pub command: CompactSubcommands,
}

/// Supported subcommands for `ferrika compact`.
#[derive(Subcommand, Debug, Clone, PartialEq)]
pub enum CompactSubcommands {
    /// Analyze topic log compaction efficiency, duplicate ratios, and tombstone counts
    Analyze(CompactAnalyzeArgs),
    /// Export latest deduplicated key-value snapshot to a file (.jsonl)
    Snapshot(CompactSnapshotArgs),
    /// Inspect active tombstone records (delete markers) in the topic
    Tombstones(CompactTombstonesArgs),
}

/// Arguments for `ferrika compact analyze`.
#[derive(Args, Debug, Clone, PartialEq)]
pub struct CompactAnalyzeArgs {
    /// Target topic name to analyze
    pub topic: String,

    /// Specific partition to evaluate (scans all partitions if omitted)
    #[arg(short = 'p', long)]
    pub partition: Option<i32>,

    /// Maximum message count to scan
    #[arg(short = 'n', long, default_value_t = 10000)]
    pub limit: usize,

    /// Read from the beginning of the topic (Default)
    #[arg(short = 'b', long, default_value_t = true)]
    pub from_beginning: bool,

    /// Output report in JSON format
    #[arg(long)]
    pub json: bool,
}

/// Arguments for `ferrika compact snapshot`.
#[derive(Args, Debug, Clone, PartialEq)]
pub struct CompactSnapshotArgs {
    /// Target topic name to snapshot
    pub topic: String,

    /// Output path for the deduplicated JSON Lines (.jsonl) snapshot
    #[arg(short = 'o', long, required = true)]
    pub output: PathBuf,

    /// Specific partition to evaluate (scans all partitions if omitted)
    #[arg(short = 'p', long)]
    pub partition: Option<i32>,

    /// Include tombstone records in snapshot
    #[arg(long, default_value_t = false)]
    pub include_tombstones: bool,

    /// Maximum message count to scan
    #[arg(short = 'n', long, default_value_t = 50000)]
    pub limit: usize,
}

/// Arguments for `ferrika compact tombstones`.
#[derive(Args, Debug, Clone, PartialEq)]
pub struct CompactTombstonesArgs {
    /// Target topic name to inspect
    pub topic: String,

    /// Specific partition to inspect
    #[arg(short = 'p', long)]
    pub partition: Option<i32>,

    /// Maximum tombstones to report
    #[arg(short = 'n', long, default_value_t = 100)]
    pub limit: usize,

    /// Output tombstones in JSON format
    #[arg(long)]
    pub json: bool,
}

/// Executes the requested `ferrika compact` subcommand.
pub async fn run(args: CompactArgs, global: &GlobalOpts) -> KafkaResult<()> {
    match args.command {
        CompactSubcommands::Analyze(analyze_args) => run_analyze(analyze_args, global).await,
        CompactSubcommands::Snapshot(snapshot_args) => run_snapshot(snapshot_args, global).await,
        CompactSubcommands::Tombstones(tombstones_args) => {
            run_tombstones(tombstones_args, global).await
        }
    }
}

/// What [`fetch_compacted_records`] keeps.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Scan {
    /// Every record, up to the limit; stopping at the limit leaves the
    /// analysis partial.
    All,
    /// Tombstones only, up to the limit (enough found is not partial).
    Tombstones,
}

/// Gathers compacted records of `topic` (or of one `partition`), read from
/// the start up to the high watermarks captured before reading, stopping
/// after `limit` kept records. Warns on stderr when the read is partial.
/// `mask`: build payload previews from PII-masked payloads (keys stay as they
/// are, since they are compared; see [`masked_key`]).
async fn fetch_compacted_records(
    topic: &str,
    partition: Option<i32>,
    limit: usize,
    scan: Scan,
    config: &KafkaClientConfig,
    mask: bool,
) -> KafkaResult<Vec<CompactedRecord>> {
    let opts = BoundedReadOptions::with_max_records(limit);
    let read = match scan {
        Scan::All => read_topic(config, topic, partition, &opts, |_| true).await?,
        Scan::Tombstones => {
            read_topic(config, topic, partition, &opts, |m| m.payload.is_none()).await?
        }
    };
    if let Some(warning) = read.partial_warning(topic, scan == Scan::All) {
        eprintln!("{} {warning}", "⚠".yellow());
    }
    Ok(read.records.iter().map(|m| to_record(m, mask)).collect())
}

/// One scanned record. With `mask`, the preview is cut from the masked
/// payload, so a cut cannot expose part of a value.
fn to_record(msg: &ConsumedMessage, mask: bool) -> CompactedRecord {
    let payload_preview = if mask {
        msg.payload
            .as_deref()
            .map(|p| ellipsize(&mask_bytes(p, MaskStrategy::Partial), 60))
    } else {
        msg.payload_str().map(|s| ellipsize(s, 60))
    };
    let record = CompactedRecord::new(
        msg.key
            .as_deref()
            .map(|k| String::from_utf8_lossy(k).into_owned())
            .unwrap_or_default(),
        msg.partition,
        msg.offset,
        msg.timestamp_ms.unwrap_or(0),
        msg.payload.is_none(),
        msg.payload.as_ref().map(|p| p.len()).unwrap_or(0),
        payload_preview,
    );
    // A null key and an empty-string key are different keys.
    if msg.key.is_none() {
        record.with_null_key()
    } else {
        record
    }
}

/// A record key as shown: masked when `mask` is set.
fn masked_key(key: &str, mask: bool) -> String {
    if mask {
        mask_payload(key, MaskStrategy::Partial)
    } else {
        key.to_string()
    }
}

/// The snapshot as written: keys masked while the policy enforces masking
/// (previews already are, see [`to_record`]).
fn shown_snapshot(mut snapshot: CompactedStateSnapshot, mask: bool) -> CompactedStateSnapshot {
    if mask {
        for entry in &mut snapshot.entries {
            entry.key = masked_key(&entry.key, true);
        }
    }
    snapshot
}

/// The tombstones as shown: keys masked while the policy enforces masking.
fn shown_tombstones(tombstones: Vec<&CompactedRecord>, mask: bool) -> Vec<CompactedRecord> {
    tombstones
        .into_iter()
        .map(|t| CompactedRecord {
            key: masked_key(&t.key, mask),
            ..t.clone()
        })
        .collect()
}

/// Handles `ferrika compact analyze`
async fn run_analyze(args: CompactAnalyzeArgs, global: &GlobalOpts) -> KafkaResult<()> {
    let mut kafka_config = global.to_kafka_config();
    if args.from_beginning {
        kafka_config = kafka_config.with_auto_offset_reset(AutoOffsetReset::Earliest);
    }
    kafka_config.group_id = Some(format!("ferrika-compact-analyze-{}", std::process::id()));
    // Only counts are shown, so nothing needs masking.
    let records = fetch_compacted_records(
        &args.topic,
        args.partition,
        args.limit,
        Scan::All,
        &kafka_config,
        false,
    )
    .await?;
    let metrics = CompactionMetrics::compute(&records);

    if global.wants_json(args.json) {
        output::print_json(&metrics)?;
        return Ok(());
    }

    println!(
        "\n{} {}",
        "🧹 Topic Log Compaction Analysis:".bold(),
        args.topic.cyan().bold()
    );
    println!("═════════════════════════════════════════════════════════════════════════════");

    let mut table = output::table(vec![
        Cell::new("Metric Attribute").fg(Color::Cyan),
        Cell::new("Observed Value")
            .fg(Color::Cyan)
            .set_alignment(CellAlignment::Right),
        Cell::new("Compaction Efficiency Impact").fg(Color::Cyan),
    ]);

    table.add_row(vec![
        Cell::new("Total Messages Scanned"),
        Cell::new(metrics.total_messages.to_string()).set_alignment(CellAlignment::Right),
        Cell::new("Total record log depth in scanned range"),
    ]);

    table.add_row(vec![
        Cell::new("Unique Keys"),
        Cell::new(metrics.unique_keys.to_string())
            .fg(Color::Green)
            .set_alignment(CellAlignment::Right),
        Cell::new("Active distinct keys retained after compaction"),
    ]);

    table.add_row(vec![
        Cell::new("Duplicate Key Revisions"),
        Cell::new(metrics.duplicate_messages.to_string())
            .fg(Color::Yellow)
            .set_alignment(CellAlignment::Right),
        Cell::new("Stale historical versions to be cleaned by broker"),
    ]);

    table.add_row(vec![
        Cell::new("Active Tombstones (Delete Markers)"),
        Cell::new(metrics.tombstone_count.to_string())
            .fg(Color::Red)
            .set_alignment(CellAlignment::Right),
        Cell::new("Null-payload records awaiting delete.retention.ms"),
    ]);

    let total_kb = crate::model::format::kib(metrics.total_payload_bytes as u64);
    let redundant_kb = crate::model::format::kib(metrics.redundant_payload_bytes as u64);

    table.add_row(vec![
        Cell::new("Total Payload Footprint"),
        Cell::new(format!("{total_kb:.2} KB")).set_alignment(CellAlignment::Right),
        Cell::new("Current raw uncompacted segment storage"),
    ]);

    table.add_row(vec![
        Cell::new("Estimated Redundant Bytes"),
        Cell::new(format!("{redundant_kb:.2} KB"))
            .fg(Color::Yellow)
            .set_alignment(CellAlignment::Right),
        Cell::new("Reclaimable disk space upon next cleaner execution"),
    ]);

    let dirty_color = if metrics.dirty_ratio_pct > 50.0 {
        Color::Red
    } else if metrics.dirty_ratio_pct > 20.0 {
        Color::Yellow
    } else {
        Color::Green
    };

    table.add_row(vec![
        Cell::new("Dirty Log Ratio"),
        Cell::new(format!("{:.1}%", metrics.dirty_ratio_pct))
            .fg(dirty_color)
            .set_alignment(CellAlignment::Right),
        Cell::new(if metrics.dirty_ratio_pct > 50.0 {
            "High dirty ratio; topic is primed for compaction cleaner trigger"
        } else {
            "Optimal dirty ratio within standard broker thresholds"
        }),
    ]);

    table.add_row(vec![
        Cell::new("Disk Space Savings Potential"),
        Cell::new(format!("{:.1}%", metrics.savings_pct))
            .fg(Color::Green)
            .set_alignment(CellAlignment::Right),
        Cell::new("Storage reduction after complete deduplication"),
    ]);

    println!("{table}\n");

    Ok(())
}

/// Handles `ferrika compact snapshot`
async fn run_snapshot(args: CompactSnapshotArgs, global: &GlobalOpts) -> KafkaResult<()> {
    let mut kafka_config = global.to_kafka_config();
    kafka_config = kafka_config.with_auto_offset_reset(AutoOffsetReset::Earliest);
    kafka_config.group_id = Some(format!("ferrika-compact-snapshot-{}", std::process::id()));
    let policy = crate::governance::policy::current();
    crate::cli::note_masking(policy);
    let mask = policy.masking_enforced();
    let records = fetch_compacted_records(
        &args.topic,
        args.partition,
        args.limit,
        Scan::All,
        &kafka_config,
        mask,
    )
    .await?;

    let snapshot = shown_snapshot(
        CompactedStateSnapshot::build(&args.topic, &records, args.include_tombstones),
        mask,
    );
    snapshot.save_jsonl(&args.output)?;
    crate::cli::export_finished(snapshot.entries.len() as u64);

    println!(
        "\n{} Compacted state snapshot with {} deduplicated keys saved to: {}",
        "✓".green().bold(),
        snapshot.entries.len().to_string().cyan().bold(),
        args.output.display().to_string().bold()
    );

    Ok(())
}

/// Handles `ferrika compact tombstones`
async fn run_tombstones(args: CompactTombstonesArgs, global: &GlobalOpts) -> KafkaResult<()> {
    let mut kafka_config = global.to_kafka_config();
    kafka_config = kafka_config.with_auto_offset_reset(AutoOffsetReset::Earliest);
    kafka_config.group_id = Some(format!("ferrika-compact-tombstones-{}", std::process::id()));
    let policy = crate::governance::policy::current();
    crate::cli::note_masking(policy);
    let mask = policy.masking_enforced();
    let records = fetch_compacted_records(
        &args.topic,
        args.partition,
        args.limit,
        Scan::Tombstones,
        &kafka_config,
        mask,
    )
    .await?;

    let tombstones = shown_tombstones(
        records
            .iter()
            .filter(|r| r.is_tombstone)
            .take(args.limit)
            .collect(),
        mask,
    );

    if global.wants_json(args.json) {
        output::print_json(&tombstones)?;
        return Ok(());
    }

    println!(
        "\n{} Active Tombstones for Topic: {}\n",
        "🪦".bold(),
        args.topic.cyan().bold()
    );

    if tombstones.is_empty() {
        println!(
            "{}",
            "No active tombstone records found in the scanned range.".green()
        );
        return Ok(());
    }

    let mut table = output::table(vec![
        Cell::new("Key").fg(Color::Cyan),
        Cell::new("Partition")
            .fg(Color::Cyan)
            .set_alignment(CellAlignment::Right),
        Cell::new("Offset")
            .fg(Color::Cyan)
            .set_alignment(CellAlignment::Right),
        Cell::new("Timestamp")
            .fg(Color::Cyan)
            .set_alignment(CellAlignment::Center),
        Cell::new("Marker Status")
            .fg(Color::Cyan)
            .set_alignment(CellAlignment::Center),
    ]);

    for t in &tombstones {
        table.add_row(vec![
            Cell::new(&t.key).fg(Color::Yellow),
            Cell::new(t.partition.to_string()).set_alignment(CellAlignment::Right),
            Cell::new(t.offset.to_string()).set_alignment(CellAlignment::Right),
            Cell::new(t.timestamp.to_string()).set_alignment(CellAlignment::Center),
            Cell::new("TOMBSTONE (NULL PAYLOAD)")
                .fg(Color::Red)
                .set_alignment(CellAlignment::Center),
        ]);
    }

    println!("{table}");
    println!(
        "\n{} Total active tombstones listed: {}",
        "✓".green().bold(),
        tombstones.len().to_string().cyan().bold()
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_clap_compact_args_analyze_parsing() {
        use clap::Parser;

        #[derive(Parser, Debug)]
        struct Wrapper {
            #[command(subcommand)]
            cmd: CompactSubcommands,
        }

        let parsed = Wrapper::parse_from([
            "compact",
            "analyze",
            "orders-v1",
            "-p",
            "0",
            "-n",
            "5000",
            "--json",
        ]);
        match parsed.cmd {
            CompactSubcommands::Analyze(args) => {
                assert_eq!(args.topic, "orders-v1");
                assert_eq!(args.partition, Some(0));
                assert_eq!(args.limit, 5000);
                assert!(args.json);
            }
            _ => panic!("Expected Analyze subcommand"),
        }
    }

    #[test]
    fn test_clap_compact_args_snapshot_and_tombstones_parsing() {
        use clap::Parser;

        #[derive(Parser, Debug)]
        struct Wrapper {
            #[command(subcommand)]
            cmd: CompactSubcommands,
        }

        let parsed_snapshot = Wrapper::parse_from([
            "compact",
            "snapshot",
            "user-state",
            "-o",
            "snapshot.jsonl",
            "--include-tombstones",
        ]);
        match parsed_snapshot.cmd {
            CompactSubcommands::Snapshot(args) => {
                assert_eq!(args.topic, "user-state");
                assert_eq!(args.output, PathBuf::from("snapshot.jsonl"));
                assert!(args.include_tombstones);
            }
            _ => panic!("Expected Snapshot subcommand"),
        }

        let parsed_tombstones =
            Wrapper::parse_from(["compact", "tombstones", "orders-v1", "-n", "50"]);
        match parsed_tombstones.cmd {
            CompactSubcommands::Tombstones(args) => {
                assert_eq!(args.topic, "orders-v1");
                assert_eq!(args.limit, 50);
            }
            _ => panic!("Expected Tombstones subcommand"),
        }
    }

    #[test]
    fn snapshot_and_tombstones_mask_pii_only_under_an_enforcing_policy() {
        use crate::governance::test_policy::{leaks, masking, none, PII};
        let msg = |offset: i64, payload: Option<Vec<u8>>| ConsumedMessage {
            topic: "users".into(),
            partition: 0,
            offset,
            key: Some(b"kim@example.com".to_vec()),
            payload,
            timestamp_ms: Some(1),
            headers: Vec::new(),
        };
        // The phone number sits past the 60-character cut, which must not
        // leave part of it visible either.
        let long = format!("{}{PII}", "x".repeat(50)).into_bytes();
        for (policy, expect_leak) in [(none(), true), (masking(), false)] {
            let mask = policy.masking_enforced();
            let live = vec![to_record(&msg(1, Some(PII.as_bytes().to_vec())), mask)];
            let snapshot =
                shown_snapshot(CompactedStateSnapshot::build("users", &live, true), mask);
            let text = serde_json::to_string(&snapshot).unwrap();
            assert_eq!(leaks(&text), expect_leak, "{text}");

            let cut = to_record(&msg(2, Some(long.clone())), mask);
            let preview = cut.payload_preview.unwrap_or_default();
            assert_eq!(preview.contains("010-1"), expect_leak, "{preview}");

            let dead = [to_record(&msg(3, None), mask)];
            let tombstones = shown_tombstones(dead.iter().collect(), mask);
            let text = serde_json::to_string(&tombstones).unwrap();
            assert_eq!(leaks(&text), expect_leak, "{text}");
        }
    }
}
