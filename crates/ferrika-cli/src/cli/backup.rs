use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::Path;
use std::time::{Duration, Instant};

use clap::Args;
use colored::Colorize;
use comfy_table::{Cell, Color};
use rdkafka::consumer::Consumer as _;
use rdkafka::Offset;
use serde_json::json;

use crate::cli::{output, print_connection_hint, GlobalOpts, OutputFormat};
use crate::error::{KafkaError, KafkaResult};
use crate::kafka::config::AutoOffsetReset;
use crate::kafka::consumer::{timestamp_start_offsets, KafkaConsumerClient, TimestampStop};
use crate::kafka::filter::MessageFilter;
use crate::kafka::producer::{
    DeliveryOutcome, DeliveryPipeline, KafkaProducerClient, OwnedProducerRecord,
};
use crate::ops::backup::{DumpRecord, DumpStats, RestoreStats};

/// Command line arguments for topic backup / export (`ferrika dump`).
#[derive(Args, Debug, Clone, PartialEq)]
pub struct DumpArgs {
    /// Topic name to dump
    pub topic: String,

    /// Destination file path (JSON Lines format, e.g. "backup.jsonl")
    #[arg(short = 'o', long)]
    pub output: String,

    /// Dump the retained records from the earliest offset and stop at the end.
    /// Without it, only records produced after the dump starts are written,
    /// until -n is reached or Ctrl+C
    #[arg(short = 'b', long)]
    pub from_beginning: bool,

    /// Specific partition to dump (dumps all partitions if omitted)
    #[arg(short = 'p', long)]
    pub partition: Option<i32>,

    /// Maximum number of messages to dump before exiting
    #[arg(short = 'n', long)]
    pub max_messages: Option<usize>,

    /// Filter payload with a regular expression (case-insensitive)
    #[arg(short = 'G', long = "grep")]
    pub grep: Option<String>,

    /// Start each partition at its first record on or after this epoch millisecond
    #[arg(long = "from-timestamp")]
    pub from_timestamp: Option<i64>,

    /// Stop each partition at its first record after this epoch millisecond
    #[arg(long = "to-timestamp")]
    pub to_timestamp: Option<i64>,

    /// With -b, give up after this many seconds without a record. Partitions
    /// that have not reached their end offsets by then fail the dump.
    #[arg(long = "idle-timeout", value_name = "SECS", default_value_t = DUMP_END_IDLE_TIMEOUT_SECS)]
    pub idle_timeout: u64,
}

/// Command line arguments for topic restore / import (`ferrika restore`).
#[derive(Args, Debug, Clone, PartialEq)]
pub struct RestoreArgs {
    /// Destination topic name to restore messages to
    pub topic: String,

    /// Source JSON Lines file path to read from
    #[arg(short = 'i', long)]
    pub input: String,

    /// Preserve original partition numbers recorded in the dump file
    #[arg(long)]
    pub preserve_partitions: bool,

    /// Maximum number of messages to restore before exiting
    #[arg(short = 'n', long)]
    pub max_messages: Option<usize>,

    /// Rate limit in messages per second
    #[arg(short = 'r', long)]
    pub rate_limit: Option<f64>,

    /// Restore the original message timestamps recorded in the dump file.
    /// Old timestamps may be deleted right away by the topic's retention.ms.
    #[arg(long)]
    pub preserve_timestamps: bool,

    /// Skip the first N records of the file (non-empty lines), e.g. to resume
    /// an interrupted restore at the position it printed
    #[arg(long, default_value_t = 0)]
    pub skip: u64,
}

/// Main entry point for `ferrika dump`.
pub async fn run_dump(args: DumpArgs, global: &GlobalOpts) -> KafkaResult<()> {
    let mut kafka_config = global.to_kafka_config();
    if args.from_beginning {
        kafka_config = kafka_config.with_auto_offset_reset(AutoOffsetReset::Earliest);
    }
    // A dump is a read-only export: partitions are assigned directly and no offsets are
    // committed, so it never creates a consumer group or moves an existing group's offsets.
    kafka_config.group_id = Some(format!("ferrika-dump-{}", std::process::id()));
    kafka_config = kafka_config.with_auto_commit(false, 5000);
    crate::kafka::consumer::limit_prefetch(&mut kafka_config);

    // Validate the expression before connecting or creating the output file.
    let mut filter =
        MessageFilter::new().with_timestamp_range(args.from_timestamp, args.to_timestamp);
    if let Some(ref g) = args.grep {
        filter = filter.with_grep(g)?;
    }

    // A read-only export with no group state: the consumer can shut down in
    // the background once the file is written.
    let consumer = match KafkaConsumerClient::new(&kafka_config) {
        Ok(c) => c.detached(),
        Err(err) => {
            print_connection_hint(&global.to_kafka_config(), &err);
            return Err(err);
        }
    };

    let partitions = match args.partition {
        Some(p) => vec![p],
        None => consumer.topic_partitions(&args.topic, None)?,
    };
    // --from-timestamp starts each partition at its first record at or after
    // the timestamp instead of scanning (or skipping) everything before it.
    let starts: Vec<(i32, Offset)> = match args.from_timestamp {
        Some(ts) => {
            let keys: Vec<(&str, i32)> = partitions
                .iter()
                .map(|&p| (args.topic.as_str(), p))
                .collect();
            let lookup = consumer.offsets_for_timestamp(&keys, ts, None)?;
            timestamp_start_offsets(&args.topic, &partitions, &lookup)
        }
        None if args.from_beginning => partitions.iter().map(|&p| (p, Offset::Beginning)).collect(),
        None => partitions.iter().map(|&p| (p, Offset::End)).collect(),
    };
    let assignments: Vec<(&str, i32, Offset)> = starts
        .iter()
        .map(|&(p, start)| (args.topic.as_str(), p, start))
        .collect();
    consumer.assign_with_offsets(&assignments)?;

    // --to-timestamp finishes a partition at its first record past it.
    let mut time_stop = TimestampStop::new(args.to_timestamp, &partitions);
    // With --from-beginning the dump stops once every partition reaches the end offset
    // it had when the dump started. Otherwise it streams new messages until -n or Ctrl+C.
    let mut end_offsets: HashMap<i32, i64> = HashMap::new();
    for &(p, start) in &starts {
        if !args.from_beginning {
            break;
        }
        let (low, high) = consumer.fetch_watermarks(&args.topic, p, None)?;
        let first = match start {
            Offset::Offset(o) => o,
            Offset::End => high,
            _ => low,
        };
        if high > first {
            end_offsets.insert(p, high);
        } else {
            time_stop.finish(p);
        }
    }
    let stop_at_end = args.from_beginning;
    let idle_timeout = Duration::from_secs(args.idle_timeout);
    // The next offset to read per partition, as far as this loop has seen.
    let mut next_offsets: HashMap<i32, i64> = HashMap::new();
    let mut stopped_idle = false;
    let mut interrupted = false;

    // Prepare output file
    let path = Path::new(&args.output);
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() && !parent.exists() {
            std::fs::create_dir_all(parent).map_err(|e| {
                KafkaError::File(format!(
                    "Failed to create output directory {}: {e}",
                    parent.display()
                ))
            })?;
        }
    }
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)
        .map_err(|e| KafkaError::File(format!("Failed to open output file: {e}")))?;
    let mut writer = BufWriter::new(file);

    if global.format != OutputFormat::Json {
        println!(
            "{} Starting topic dump: '{}' ➜ '{}'",
            "💾".cyan().bold(),
            args.topic.yellow().bold(),
            args.output.green().bold()
        );
        if args.from_beginning {
            println!(
                "  Offset Reset:       {}",
                "earliest (from beginning)".yellow()
            );
        } else {
            println!(
                "  Mode:               {}",
                "new records only, until -n or Ctrl+C (use -b to dump existing records)".yellow()
            );
        }
        if let Some(p) = args.partition {
            println!("  Partition:          {}", p.to_string().yellow());
        }
        if let Some(max) = args.max_messages {
            println!(
                "  Limit:              {} messages",
                max.to_string().yellow()
            );
        }
        println!();
    }

    let (mut stats, start_time) = DumpStats::new();
    let mut last_ticker = Instant::now();
    let ctrl_c = tokio::signal::ctrl_c();
    tokio::pin!(ctrl_c);

    loop {
        if let Some(max) = args.max_messages {
            if stats.records_dumped >= max as u64 {
                break;
            }
        }
        if (stop_at_end && end_offsets.is_empty()) || time_stop.all_done() {
            break;
        }

        tokio::select! {
            // Fallback for partitions whose last offsets are never delivered
            // (transaction markers, compacted-away records); checked below.
            _ = tokio::time::sleep(idle_timeout), if stop_at_end => {
                stopped_idle = true;
                break;
            }
            _ = &mut ctrl_c => {
                if global.format != OutputFormat::Json {
                    println!("\n{}", "Dump interrupted by user (Ctrl+C).".yellow());
                }
                interrupted = true;
                break;
            }
            msg_res = consumer.recv() => {
                match msg_res {
                    Ok(msg) => {
                        next_offsets.insert(msg.partition, msg.offset + 1);
                        if stop_at_end {
                            if let Some(&end) = end_offsets.get(&msg.partition) {
                                if msg.offset + 1 >= end {
                                    end_offsets.remove(&msg.partition);
                                }
                            }
                        }
                        if !time_stop.admit(msg.partition, msg.timestamp_ms) {
                            // Read past --to-timestamp: nothing more is needed from it.
                            end_offsets.remove(&msg.partition);
                            continue;
                        }
                        if !filter.matches(&msg) {
                            continue;
                        }

                        let dump_record = DumpRecord::from_message(&msg);
                        let json_line = serde_json::to_string(&dump_record)
                            .map_err(|e| KafkaError::Serialization(e.to_string()))?;

                        let line_bytes = json_line.len() + 1; // + newline
                        writeln!(writer, "{json_line}").map_err(|e| {
                            KafkaError::File(format!("Failed to write to '{}': {e}", args.output))
                        })?;

                        stats.records_dumped += 1;
                        stats.bytes_dumped += line_bytes as u64;

                        if global.format != OutputFormat::Json && last_ticker.elapsed() >= Duration::from_millis(500) {
                            let secs = start_time.elapsed().as_secs_f64();
                            let eps = if secs > 0.0 { stats.records_dumped as f64 / secs } else { 0.0 };
                            let mb = crate::model::format::mib(stats.bytes_dumped);
                            print!(
                                "\r{} Dumped: {} msgs ({:.1} EPS, {:.2} MB) | Elapsed: {:.1}s    ",
                                "💾".cyan(),
                                stats.records_dumped.to_string().green().bold(),
                                eps,
                                mb,
                                secs
                            );
                            let _ = std::io::stdout().flush();
                            last_ticker = Instant::now();
                        }
                    }
                    Err(err) => {
                        eprintln!("Consumer error during dump: {err}");
                    }
                }
            }
        }
    }

    writer
        .flush()
        .map_err(|e| KafkaError::File(format!("Failed to write to '{}': {e}", args.output)))?;
    crate::cli::export_finished(stats.records_dumped);

    // An idle stop is only complete when the remaining partitions' read
    // positions already reached their end offsets (their last offsets were
    // transaction markers or compacted away); otherwise the backup is cut
    // short. So is a dump to the end (`-b`) that Ctrl+C stopped early: a
    // script must not take the file for a complete backup.
    if must_check_completeness(stopped_idle, interrupted, !end_offsets.is_empty()) {
        let mut positions = next_offsets.clone();
        if let Ok(tpl) = consumer.inner().position() {
            for elem in tpl.elements_for_topic(&args.topic) {
                if let Offset::Offset(o) = elem.offset() {
                    let pos = positions.entry(elem.partition()).or_insert(o);
                    *pos = (*pos).max(o);
                }
            }
        }
        let incomplete = incomplete_partitions(&end_offsets, &positions);
        if !incomplete.is_empty() {
            if global.format != OutputFormat::Json {
                println!();
            }
            let why = if interrupted {
                "it was interrupted".to_string()
            } else {
                format!("no record arrived for {}s", args.idle_timeout)
            };
            eprintln!(
                "{} Dump incomplete: {why} before these partitions reached their end offsets:",
                "✖".red().bold(),
            );
            for (p, pos, end) in &incomplete {
                let pos =
                    pos.map_or_else(|| "nothing read".to_string(), |o| format!("at offset {o}"));
                eprintln!("  partition {p}: {pos}, end offset {end}");
            }
            return Err(KafkaError::Check(format!(
                "dump of '{}' to '{}' is incomplete: {} partition(s) did not reach their end offsets ({} records written); retry, or raise --idle-timeout",
                args.topic,
                args.output,
                incomplete.len(),
                stats.records_dumped
            )));
        }
    }

    let secs = start_time.elapsed().as_secs_f64();
    stats.elapsed_secs = secs;
    stats.average_eps = if secs > 0.0 {
        stats.records_dumped as f64 / secs
    } else {
        0.0
    };

    match global.format {
        OutputFormat::Json => {
            output::print_json(&json!({
                "topic": args.topic,
                "output_file": args.output,
                "records_dumped": stats.records_dumped,
                "bytes_dumped": stats.bytes_dumped,
                "elapsed_secs": stats.elapsed_secs,
                "average_eps": stats.average_eps,
            }))?;
        }
        OutputFormat::Table => {
            println!("\n");
            let mut table = output::table(vec![
                Cell::new("DUMP SUMMARY").fg(Color::Cyan),
                Cell::new("VALUE").fg(Color::Cyan),
            ]);

            table.add_row(vec![
                Cell::new("Source Topic").fg(Color::DarkGrey),
                Cell::new(&args.topic).fg(Color::Yellow),
            ]);
            table.add_row(vec![
                Cell::new("Destination File").fg(Color::DarkGrey),
                Cell::new(&args.output).fg(Color::Green),
            ]);
            table.add_row(vec![
                Cell::new("Total Records Dumped").fg(Color::DarkGrey),
                Cell::new(stats.records_dumped.to_string()).fg(Color::Cyan),
            ]);
            table.add_row(vec![
                Cell::new("Total File Size").fg(Color::DarkGrey),
                Cell::new(format!(
                    "{:.2} MB",
                    crate::model::format::mib(stats.bytes_dumped)
                ))
                .fg(Color::White),
            ]);
            table.add_row(vec![
                Cell::new("Elapsed Time").fg(Color::DarkGrey),
                Cell::new(format!("{:.2} s", stats.elapsed_secs)).fg(Color::Blue),
            ]);
            table.add_row(vec![
                Cell::new("Average Throughput").fg(Color::DarkGrey),
                Cell::new(format!("{:.1} records/sec", stats.average_eps)).fg(Color::Green),
            ]);

            println!("{}", table);
        }
    }

    Ok(())
}

/// The restore producer's config: idempotent, so a retried batch can
/// neither duplicate records nor land after the batches sent behind it, and
/// each partition keeps the backup's order. Idempotence needs `acks=all`
/// and at most 5 requests in flight per connection.
fn restore_config(
    mut config: crate::kafka::config::KafkaClientConfig,
) -> crate::kafka::config::KafkaClientConfig {
    let extra = &mut config.extra_properties;
    extra.insert("enable.idempotence".into(), "true".into());
    extra.insert("acks".into(), "all".into());
    extra.remove("request.required.acks");
    for key in ["max.in.flight.requests.per.connection", "max.in.flight"] {
        if let Some(v) = extra.get_mut(key) {
            if !matches!(v.trim().parse::<u32>(), Ok(n) if n <= 5) {
                *v = "5".into();
            }
        }
    }
    config
}

/// Main entry point for `ferrika restore`.
pub async fn run_restore(args: RestoreArgs, global: &GlobalOpts) -> KafkaResult<()> {
    let kafka_config = restore_config(global.to_kafka_config());
    let producer = match KafkaProducerClient::new(&kafka_config) {
        Ok(p) => p,
        Err(err) => {
            print_connection_hint(&global.to_kafka_config(), &err);
            return Err(err);
        }
    };

    let file = File::open(&args.input).map_err(|e| {
        KafkaError::File(format!("Failed to open backup file '{}': {e}", args.input))
    })?;
    let reader = BufReader::new(file);

    if global.format != OutputFormat::Json {
        println!(
            "{} Starting topic restore: '{}' ➜ '{}'",
            "📦".cyan().bold(),
            args.input.yellow().bold(),
            args.topic.green().bold()
        );
        if args.preserve_partitions {
            println!(
                "  Partition Mode:     {}",
                "Preserve original partitions".yellow()
            );
        }
        if let Some(r) = args.rate_limit {
            println!(
                "  Rate Limit:         {} records/sec",
                r.to_string().yellow()
            );
        }
        println!();
    }

    let (mut stats, start_time) = RestoreStats::new();
    let mut last_ticker = Instant::now();
    let rate = args.rate_limit.filter(|&r| r > 0.0);
    let mut pipeline = DeliveryPipeline::new(RESTORE_MAX_IN_FLIGHT);
    let mut submitted: u64 = 0;
    let mut failed: u64 = 0;
    // Non-empty lines consumed so far (including skipped ones); after the
    // in-flight deliveries are drained this is the `--skip` that resumes.
    let mut records_read: u64 = 0;
    let mut interrupted = false;
    let mut hit_limit = false;
    let ctrl_c = tokio::signal::ctrl_c();
    tokio::pin!(ctrl_c);

    for line_result in reader.lines() {
        if let Some(max) = args.max_messages {
            if submitted >= max as u64 {
                hit_limit = true;
                break;
            }
        }
        let stop = tokio::select! {
            biased;
            _ = &mut ctrl_c => true,
            _ = std::future::ready(()) => false,
        };
        if stop {
            interrupted = true;
            break;
        }

        let line = match line_result {
            Ok(l) => l,
            Err(e) => {
                eprintln!("Error reading input file: {e}");
                failed += 1;
                break;
            }
        };

        if line.trim().is_empty() {
            continue;
        }
        records_read += 1;
        if records_read <= args.skip {
            continue;
        }

        let record: DumpRecord = match serde_json::from_str(&line) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("Skipping invalid line: {e}");
                failed += 1;
                continue;
            }
        };

        let out_record = match build_restore_record(&record, &args) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("Skipping record at offset {}: {e}", record.offset);
                failed += 1;
                continue;
            }
        };

        if let Some(r) = rate {
            let due = start_time + Duration::from_secs_f64(submitted as f64 / r);
            let stop = tokio::select! {
                _ = tokio::time::sleep_until(due.into()) => false,
                _ = &mut ctrl_c => true,
            };
            if stop {
                // This record was not sent; a resume must start with it.
                records_read -= 1;
                interrupted = true;
                break;
            }
        }

        submitted += 1;
        match pipeline.submit(&producer, &out_record).await {
            Ok(Some(outcome)) => apply_restore_outcome(outcome, &mut stats, &mut failed),
            Ok(None) => {}
            Err(e) => {
                eprintln!("Failed to restore message: {e}");
                failed += 1;
            }
        }

        if global.format != OutputFormat::Json
            && last_ticker.elapsed() >= Duration::from_millis(500)
        {
            print_restore_progress(&stats, start_time);
            last_ticker = Instant::now();
        }
    }

    // Wait for every record already handed to the producer, also after Ctrl+C,
    // so the counts and the resume position below are exact.
    while let Some(outcome) = pipeline.next_outcome().await {
        apply_restore_outcome(outcome, &mut stats, &mut failed);
    }
    let next_skip = records_read.max(args.skip);

    let secs = start_time.elapsed().as_secs_f64();
    stats.elapsed_secs = secs;
    stats.average_eps = if secs > 0.0 {
        stats.records_restored as f64 / secs
    } else {
        0.0
    };

    match global.format {
        OutputFormat::Json => {
            output::print_json(&json!({
                "topic": args.topic,
                "input_file": args.input,
                "records_restored": stats.records_restored,
                "failed": failed,
                "bytes_restored": stats.bytes_restored,
                "interrupted": interrupted,
                "next_skip": next_skip,
                "elapsed_secs": stats.elapsed_secs,
                "average_eps": stats.average_eps,
            }))?;
        }
        OutputFormat::Table => {
            println!("\n");
            let mut table = output::table(vec![
                Cell::new("RESTORE SUMMARY").fg(Color::Cyan),
                Cell::new("VALUE").fg(Color::Cyan),
            ]);

            table.add_row(vec![
                Cell::new("Destination Topic").fg(Color::DarkGrey),
                Cell::new(&args.topic).fg(Color::Green),
            ]);
            table.add_row(vec![
                Cell::new("Source File").fg(Color::DarkGrey),
                Cell::new(&args.input).fg(Color::Yellow),
            ]);
            table.add_row(vec![
                Cell::new("Total Records Restored").fg(Color::DarkGrey),
                Cell::new(stats.records_restored.to_string()).fg(Color::Cyan),
            ]);
            table.add_row(vec![
                Cell::new("Total Data Transferred").fg(Color::DarkGrey),
                Cell::new(format!(
                    "{:.2} MB",
                    crate::model::format::mib(stats.bytes_restored)
                ))
                .fg(Color::White),
            ]);
            table.add_row(vec![
                Cell::new("Elapsed Time").fg(Color::DarkGrey),
                Cell::new(format!("{:.2} s", stats.elapsed_secs)).fg(Color::Blue),
            ]);
            table.add_row(vec![
                Cell::new("Average Throughput").fg(Color::DarkGrey),
                Cell::new(format!("{:.1} records/sec", stats.average_eps)).fg(Color::Green),
            ]);

            println!("{}", table);
            if interrupted || hit_limit {
                let why = if interrupted {
                    "Restore interrupted by user (Ctrl+C)"
                } else {
                    "Stopped at the --max-messages limit"
                };
                println!(
                    "{} after {} restored record(s); {} record(s) of the file processed.",
                    why.yellow(),
                    stats.records_restored,
                    next_skip
                );
                if !can_resume_with_skip(failed) {
                    // `--skip` would pass over the records that failed too.
                    println!(
                        "  {failed} record(s) failed before that point: resuming with --skip {next_skip} would skip them. Fix the cause, then restore those records again."
                    );
                } else {
                    println!(
                        "  Resume with: ferrika restore {} -i {} --skip {}",
                        args.topic, args.input, next_skip
                    );
                }
            }
        }
    }

    if failed > 0 {
        return Err(KafkaError::Check(format!(
            "{failed} record(s) could not be restored"
        )));
    }
    Ok(())
}

/// With --from-beginning, stop when no message arrives for this many seconds
/// (the default of `--idle-timeout`).
const DUMP_END_IDLE_TIMEOUT_SECS: u64 = 5;

/// The partitions still short of their end offset, sorted, with the next
/// offset read (`None`: nothing read) and the end offset.
fn incomplete_partitions(
    end_offsets: &HashMap<i32, i64>,
    positions: &HashMap<i32, i64>,
) -> Vec<(i32, Option<i64>, i64)> {
    let mut incomplete: Vec<(i32, Option<i64>, i64)> = end_offsets
        .iter()
        .filter_map(|(&p, &end)| {
            let pos = positions.get(&p).copied();
            match pos {
                Some(pos) if pos >= end => None,
                _ => Some((p, pos, end)),
            }
        })
        .collect();
    incomplete.sort_unstable();
    incomplete
}

/// Maximum number of unacknowledged records while restoring.
const RESTORE_MAX_IN_FLIGHT: usize = 10_000;

/// Converts a dump line back into a producer record, keeping null payloads (tombstones).
fn build_restore_record(
    record: &DumpRecord,
    args: &RestoreArgs,
) -> KafkaResult<OwnedProducerRecord> {
    let mut out_record = OwnedProducerRecord::new(&args.topic);
    if let Some(payload) = record.extract_payload_bytes()? {
        out_record = out_record.with_payload(payload);
    }
    if let Some(key) = record.extract_key_bytes()? {
        out_record = out_record.with_key(key);
    }
    if args.preserve_partitions {
        out_record = out_record.with_partition(record.partition);
    }
    if args.preserve_timestamps {
        if let Some(ts) = record.timestamp_ms {
            out_record = out_record.with_timestamp(ts);
        }
    }
    for (k, v) in record.extract_headers()? {
        out_record = out_record.with_header(k, v);
    }
    Ok(out_record)
}

fn apply_restore_outcome(outcome: DeliveryOutcome, stats: &mut RestoreStats, failed: &mut u64) {
    match outcome.result {
        Ok(_) => {
            stats.records_restored += 1;
            stats.bytes_restored += outcome.payload_bytes as u64;
        }
        Err(e) => {
            *failed += 1;
            if *failed <= 5 {
                eprintln!("Failed to restore message: {e}");
            }
        }
    }
}

fn print_restore_progress(stats: &RestoreStats, start_time: Instant) {
    let secs = start_time.elapsed().as_secs_f64();
    let eps = if secs > 0.0 {
        stats.records_restored as f64 / secs
    } else {
        0.0
    };
    let mb = crate::model::format::mib(stats.bytes_restored);
    print!(
        "\r{} Restored: {} msgs ({:.1} EPS, {:.2} MB) | Elapsed: {:.1}s    ",
        "📦".cyan(),
        stats.records_restored.to_string().green().bold(),
        eps,
        mb,
        secs
    );
    let _ = std::io::stdout().flush();
}

/// Whether a dump must prove it reached every partition's end before it
/// reports success: when it read to the end (`-b`, end offsets known) and
/// stopped on the idle timeout or on Ctrl+C rather than at those ends.
fn must_check_completeness(stopped_idle: bool, interrupted: bool, reads_to_end: bool) -> bool {
    reads_to_end && (stopped_idle || interrupted)
}

/// Whether `--skip N` (records read so far) is a safe resume point: not
/// when some of those records failed, as it would pass over them.
fn can_resume_with_skip(failed: u64) -> bool {
    failed == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interrupted_dumps_to_the_end_are_checked_and_failed_restores_not_resumed() {
        assert!(must_check_completeness(false, true, true));
        assert!(must_check_completeness(true, false, true));
        assert!(!must_check_completeness(false, false, true));
        // A dump without -b has no end to reach.
        assert!(!must_check_completeness(false, true, false));
        assert!(can_resume_with_skip(0));
        assert!(!can_resume_with_skip(2));
    }

    #[test]
    fn restore_producer_is_idempotent_and_keeps_order() {
        let config = crate::kafka::config::KafkaClientConfig::new("b:9092")
            .with_property("acks", "1")
            .with_property("max.in.flight.requests.per.connection", "100")
            .with_property("linger.ms", "5");
        let rd = restore_config(config).to_rdkafka_config().unwrap();
        assert_eq!(rd.get("enable.idempotence"), Some("true"));
        assert_eq!(rd.get("acks"), Some("all"));
        assert_eq!(rd.get("max.in.flight.requests.per.connection"), Some("5"));
        assert_eq!(rd.get("linger.ms"), Some("5"));
        // librdkafka accepts it as an idempotent producer.
        let rd = restore_config(crate::kafka::config::KafkaClientConfig::new("b:9092"))
            .to_rdkafka_config()
            .unwrap();
        assert!(rd.create::<rdkafka::producer::FutureProducer>().is_ok());
    }

    #[test]
    fn test_clap_dump_args_parsing() {
        use clap::Parser;

        #[derive(Parser, Debug)]
        struct TestCli {
            #[command(flatten)]
            args: DumpArgs,
        }

        let cli = TestCli::try_parse_from([
            "test",
            "orders-v1",
            "-o",
            "orders_backup.jsonl",
            "-b",
            "-p",
            "2",
            "-n",
            "1000",
            "-G",
            "PAID",
        ])
        .unwrap();

        assert_eq!(cli.args.topic, "orders-v1");
        assert_eq!(cli.args.output, "orders_backup.jsonl");
        assert!(cli.args.from_beginning);
        assert_eq!(cli.args.partition, Some(2));
        assert_eq!(cli.args.max_messages, Some(1000));
        assert_eq!(cli.args.grep.as_deref(), Some("PAID"));
        assert_eq!(cli.args.idle_timeout, 5);

        let cli =
            TestCli::try_parse_from(["test", "t", "-o", "f", "--idle-timeout", "30"]).unwrap();
        assert_eq!(cli.args.idle_timeout, 30);
    }

    #[test]
    fn test_incomplete_partitions() {
        let ends: HashMap<i32, i64> = [(0, 100), (1, 50), (2, 10)].into_iter().collect();
        // Partition 0 reached its end (trailing markers), 1 stopped short,
        // 2 was never read.
        let positions: HashMap<i32, i64> = [(0, 100), (1, 40)].into_iter().collect();
        assert_eq!(
            incomplete_partitions(&ends, &positions),
            vec![(1, Some(40), 50), (2, None, 10)]
        );
        let done: HashMap<i32, i64> = [(0, 101), (1, 50), (2, 10)].into_iter().collect();
        assert!(incomplete_partitions(&ends, &done).is_empty());
    }

    #[test]
    fn test_clap_restore_args_parsing() {
        use clap::Parser;

        #[derive(Parser, Debug)]
        struct TestCli {
            #[command(flatten)]
            args: RestoreArgs,
        }

        let cli = TestCli::try_parse_from([
            "test",
            "orders-staging",
            "-i",
            "orders_backup.jsonl",
            "--preserve-partitions",
            "-r",
            "250",
            "-n",
            "500",
            "--skip",
            "1200",
        ])
        .unwrap();

        assert_eq!(cli.args.skip, 1200);
        assert_eq!(cli.args.topic, "orders-staging");
        assert_eq!(cli.args.input, "orders_backup.jsonl");
        assert!(cli.args.preserve_partitions);
        assert_eq!(cli.args.rate_limit, Some(250.0));
        assert_eq!(cli.args.max_messages, Some(500));
    }
}
