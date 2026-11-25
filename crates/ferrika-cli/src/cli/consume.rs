use std::io::{BufWriter, Write};
use std::path::Path;
use std::time::Duration;

use clap::Args;
use colored::Colorize;

use crate::cli::{print_connection_hint, GlobalOpts, OutputFormat};
use crate::codec::pii::{mask_payload, MaskStrategy};
use crate::codec::schema::{format_hexdump, parse_confluent_wire_format};
use crate::kafka::config::AutoOffsetReset;
use crate::kafka::consumer::{
    timestamp_start_offsets, ConsumedMessage, KafkaConsumerClient, ParallelReader, PartitionRead,
    ReadEvent, TimestampStop,
};
use crate::kafka::consumer::{CommitMode, StartOffset};
use crate::kafka::filter::{FilterStats, MessageFilter};
use crate::ops::backup::jsonl_record;

/// Command line arguments for message consumption.
#[derive(Args, Debug, Clone, PartialEq, Eq)]
pub struct ConsumeArgs {
    /// Topic name to consume messages from
    pub topic: String,

    /// Consumer group identifier (defaults to a temporary group if omitted)
    #[arg(short, long)]
    pub group: Option<String>,

    /// Start consuming from the earliest available offset
    #[arg(short = 'b', long)]
    pub from_beginning: bool,

    /// Offset to start from. A negative value -N starts N messages before the end.
    /// Applies to --partition, or to every partition of the topic when omitted.
    #[arg(long, allow_hyphen_values = true)]
    pub offset: Option<i64>,

    /// Target partition to consume from (if omitted, subscribes to all partitions)
    #[arg(short, long)]
    pub partition: Option<i32>,

    /// Maximum number of messages to consume before exiting
    #[arg(short = 'n', long)]
    pub max_messages: Option<usize>,

    /// Maximum idle wait time in milliseconds before exiting
    #[arg(short = 't', long)]
    pub timeout: Option<u64>,

    /// Pretty-print JSON payloads
    #[arg(long)]
    pub pretty_json: bool,

    /// Show message headers
    #[arg(long)]
    pub show_headers: bool,

    /// Filter payload with a regular expression (case-insensitive by default)
    #[arg(short = 'G', long = "grep")]
    pub grep: Option<String>,

    /// Filter key by substring / pattern
    #[arg(short = 'k', long = "key-filter")]
    pub key_filter: Option<String>,

    /// Keep records matching an expression, e.g. '$.amount > 100 && $.status == "PAID"'.
    /// Fields: $.path (JSON value, decoded first with -D), key, partition,
    /// offset, timestamp, headers.<name>; operators == != > >= < <= =~
    /// contains exists && || !
    #[arg(short = 'W', long = "where", value_name = "EXPR")]
    pub where_expr: Option<String>,

    /// Keep records with a header: KEY:VALUE (value contains VALUE,
    /// case-insensitive unless --case-sensitive) or KEY=VALUE (value is
    /// exactly VALUE). Header names ignore case. Repeat it to require
    /// several; every one must match
    #[arg(long = "header-filter", value_name = "KEY:VALUE|KEY=VALUE")]
    pub header_filter: Vec<String>,

    /// Start each partition at its first record on or after this epoch
    /// millisecond (unless --offset is given), and keep only those records
    #[arg(long = "from-timestamp")]
    pub from_timestamp: Option<i64>,

    /// Keep records on or before this epoch millisecond; a partition stops at
    /// its first record after it, and consuming ends when all have stopped
    #[arg(long = "to-timestamp")]
    pub to_timestamp: Option<i64>,

    /// Perform case-sensitive matching for text filters
    #[arg(long)]
    pub case_sensitive: bool,

    /// Save matched messages to a file in JSON Lines (.jsonl) format
    #[arg(short = 's', long = "save")]
    pub save: Option<String>,

    /// Decode Confluent Schema Registry wire-format headers (Magic byte 0x00 + 4-byte Schema ID)
    #[arg(short = 'D', long = "decode-wire-format")]
    pub decode_wire_format: bool,

    /// Display payload as canonical 16-byte hexdump
    #[arg(long = "hex")]
    pub hex: bool,

    /// Schema Registry URL used with --decode-wire-format to decode Avro and
    /// Protobuf payloads (defaults to the profile's)
    #[arg(short = 'S', long = "schema-registry")]
    pub schema_registry: Option<String>,

    /// Mask card numbers, emails, SSNs, Korean IDs/phones and secrets in keys,
    /// payloads and headers (always on when a company policy enforces masking)
    #[arg(long = "mask-pii")]
    pub mask_pii: bool,
}

/// Executes the consumer CLI command.
pub async fn run(mut args: ConsumeArgs, global: &GlobalOpts) -> anyhow::Result<()> {
    let mut kafka_config = global.to_kafka_config();
    let policy = crate::governance::policy::current();
    let mask = args.mask_pii || policy.masking_enforced();
    // Under an enforced policy, filters see only the masked rendering: matching
    // raw values would let `--where value.rrn =~ "^9001"` recover masked data.
    let mask_filters = policy.masking_enforced();
    if mask && !args.mask_pii {
        eprintln!(
            "{}",
            format!("PII masking is enforced by {}.", policy.label()).bright_black()
        );
    }
    if mask && args.hex {
        // A hexdump would show the raw bytes.
        eprintln!(
            "{}",
            "--hex is ignored while PII masking is on.".bright_black()
        );
        args.hex = false;
    }

    if let Some(ref g) = args.group {
        kafka_config = kafka_config.with_group_id(g);
    }
    // Without an explicit group, partitions are assigned directly and nothing is committed,
    // so no consumer group is created and there is no group-join delay.
    let use_group = kafka_config.group_id.is_some();
    if use_group {
        // librdkafka would store (and auto-commit) the offset of every record it
        // hands over, including ones fetched after --max-messages stopped the
        // loop. Store only records this command actually processed.
        kafka_config = kafka_config.with_property("enable.auto.offset.store", "false");
    } else {
        kafka_config = kafka_config.with_auto_commit(false, 5000);
    }
    if args.from_beginning || args.offset.is_some() {
        kafka_config = kafka_config.with_auto_offset_reset(AutoOffsetReset::Earliest);
    }

    // With a registry, wire-format payloads are decoded (Avro/Protobuf to JSON).
    let mut decoder = if args.decode_wire_format {
        args.schema_registry
            .clone()
            .or_else(|| kafka_config.schema_registry_url.clone())
            .map(|url| {
                crate::kafka::schema::PayloadDecoder::new(Some(&url))
                    .with_timeout(global.http_timeout())
            })
    } else {
        None
    };

    // Build and validate the filter before opening a broker connection so an
    // invalid expression always produces its actionable validation error.
    let mut filter = MessageFilter::new()
        .with_case_sensitive(args.case_sensitive)
        .with_timestamp_range(args.from_timestamp, args.to_timestamp);

    // With -D, --grep and -k look at the decoded text (an Avro or Protobuf
    // value is binary before decoding), so they run after the decode.
    let mut text_filter = None;
    if args.decode_wire_format && (args.grep.is_some() || args.key_filter.is_some()) {
        let mut tf = MessageFilter::new().with_case_sensitive(args.case_sensitive);
        if let Some(ref g) = args.grep {
            tf = tf.with_grep(g)?;
        }
        if let Some(ref k) = args.key_filter {
            tf = tf.with_key(k);
        }
        text_filter = Some(tf);
    } else {
        if let Some(ref g) = args.grep {
            filter = filter.with_grep(g)?;
        }
        if let Some(ref k) = args.key_filter {
            filter = filter.with_key(k);
        }
    }
    for h in &args.header_filter {
        let header = crate::kafka::filter::parse_header_filter(h)
            .map_err(|e| anyhow::anyhow!("--header-filter: {e}"))?;
        filter = filter.with_header_filter(header);
    }
    let where_expr = match &args.where_expr {
        Some(text) => Some(
            crate::model::expr::Expr::parse(crate::model::expr::expression_text(text))
                .map_err(|e| anyhow::anyhow!("--where: {e}"))?,
        ),
        None => None,
    };

    let consumer = match KafkaConsumerClient::new(&kafka_config) {
        // Without a group nothing is left to commit or leave, so the client
        // can shut down in the background.
        Ok(client) if !use_group => client.detached(),
        Ok(client) => client,
        Err(err) => {
            print_connection_hint(&global.to_kafka_config(), &err);
            return Err(err.into());
        }
    };

    // Without a group, partitions are read by a ParallelReader, which filters
    // as it reads; with one, offsets are stored, so the group's consumer reads.
    let mut reader = None;
    // --to-timestamp finishes a partition at its first record past it; the
    // command ends once all are finished.
    let mut time_stop = TimestampStop::new(args.to_timestamp, &[]);
    // With a --to-timestamp already past, each partition's current end: the
    // command finishes a partition there even when no later record arrives
    // (it used to wait for one forever).
    let mut ends: std::collections::HashMap<i32, i64> = std::collections::HashMap::new();
    let timestamps = args.from_timestamp.is_some() || args.to_timestamp.is_some();
    if args.partition.is_some() || args.offset.is_some() || timestamps || !use_group {
        let existing = consumer.topic_partitions(&args.topic, None)?;
        let partitions = match args.partition {
            Some(p) => {
                crate::kafka::filter::ensure_partition(&args.topic, &existing, p)?;
                vec![p]
            }
            None => existing,
        };
        time_stop = TimestampStop::new(args.to_timestamp, &partitions);
        let starts: Vec<(i32, StartOffset)> = match (args.offset, args.from_timestamp) {
            // --from-timestamp seeks to the first record at or after it.
            (None, Some(ts)) => {
                let keys: Vec<(&str, i32)> = partitions
                    .iter()
                    .map(|&p| (args.topic.as_str(), p))
                    .collect();
                let lookup = time_lookup_offsets(consumer.offsets_at_time(&keys, ts, None)?)?;
                let starts = timestamp_start_offsets(&args.topic, &partitions, &lookup);
                // Nothing at or after --from-timestamp, and --to-timestamp
                // has passed: the window is empty.
                let window_closed = args
                    .to_timestamp
                    .is_some_and(|to| to < jiff::Timestamp::now().as_millisecond());
                for &(p, start) in &starts {
                    if window_closed && start == StartOffset::End {
                        time_stop.finish(p);
                    }
                }
                starts
            }
            (Some(off), None) if off >= 0 => {
                clamped_starts(&consumer, &args.topic, &partitions, off).await?
            }
            _ => {
                let start = start_offset(args.offset, args.from_beginning, use_group);
                partitions.iter().map(|&p| (p, start)).collect()
            }
        };
        if args
            .to_timestamp
            .is_some_and(|to| to < jiff::Timestamp::now().as_millisecond())
        {
            let keys: Vec<(&str, i32)> = partitions
                .iter()
                .map(|&p| (args.topic.as_str(), p))
                .collect();
            // A group starting at its committed offsets: where those are.
            let committed = match (
                &kafka_config.group_id,
                starts.iter().any(|(_, s)| *s == StartOffset::Committed),
            ) {
                (Some(group), true) => crate::kafka::wire::fetch_group_offsets(
                    &kafka_config,
                    std::slice::from_ref(group),
                )
                .ok()
                .and_then(|mut all| all.remove(group))
                .unwrap_or_default(),
                _ => Default::default(),
            };
            for ((_, p), (low, high)) in consumer.fetch_watermarks_batch_async(&keys, None).await? {
                let at_end = starts.iter().any(|&(sp, start)| {
                    sp == p
                        && match start {
                            StartOffset::Committed => match committed.get(&(args.topic.clone(), p))
                            {
                                Some(&c) => c >= high,
                                // No commit: the reset policy picks the start.
                                None => match kafka_config.auto_offset_reset {
                                    crate::kafka::config::AutoOffsetReset::Latest => true,
                                    _ => low >= high,
                                },
                            },
                            start => starts_at_end(start, low, high),
                        }
                });
                if at_end {
                    time_stop.finish(p);
                }
                ends.insert(p, high);
            }
            let unbounded: Vec<String> = partitions
                .iter()
                .filter(|p| !ends.contains_key(p))
                .map(i32::to_string)
                .collect();
            if !unbounded.is_empty() {
                eprintln!(
                    "{} Could not read the end of partition(s) {}; they finish only at a record past --to-timestamp.",
                    "!".yellow(),
                    unbounded.join(", ")
                );
            }
        }
        let assignments: Vec<(&str, i32, StartOffset)> = starts
            .iter()
            .map(|&(p, start)| (args.topic.as_str(), p, start))
            .collect();
        if use_group {
            consumer.assign_with_offsets(&assignments)?;
        } else {
            let reads: Vec<PartitionRead> = assignments
                .iter()
                .map(|&(topic, partition, start)| PartitionRead {
                    topic,
                    partition,
                    start,
                    end: ends.get(&partition).copied(),
                })
                .collect();
            let keep = filter.clone();
            // The first record past --to-timestamp of each partition is
            // handed over too, so the loop below sees the partition finish.
            let to = args.to_timestamp;
            let passed = std::sync::Mutex::new(std::collections::HashSet::new());
            reader = Some(ParallelReader::start(&kafka_config, &reads, move |msg| {
                if TimestampStop::is_past(to, msg.timestamp_ms) {
                    return passed
                        .lock()
                        .map_or(true, |mut passed| passed.insert(msg.partition));
                }
                if mask_filters {
                    keep.matches(&msg.masked())
                } else {
                    keep.matches(msg)
                }
            })?);
        }
    } else {
        consumer.subscribe(&[&args.topic])?;
    }

    // Optional file output stream
    let mut file_writer = if let Some(ref path_str) = args.save {
        let path = Path::new(path_str);
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() && !parent.exists() {
                std::fs::create_dir_all(parent)?;
            }
        }
        if let Some(notice) = append_notice(path) {
            eprintln!("{} {notice}", "ℹ".cyan());
        }
        // Record data: owner-only (0600) on unix, not the umask default.
        let file = crate::files::open_private_file(path, true)?;
        Some(BufWriter::with_capacity(OUTPUT_BUFFER_BYTES, file))
    } else {
        None
    };

    eprintln!(
        "{}",
        format!(
            "Subscribed to topic '{}'. Awaiting messages... (Ctrl+C to quit)",
            args.topic
        )
        .bright_black()
        .italic()
    );

    if filter.is_active() || text_filter.is_some() {
        eprintln!(
            "{}",
            "Active filter applied. Only matching messages will be shown."
                .yellow()
                .italic()
        );
    }

    let mut stats = FilterStats::default();
    // Offsets of processed records that could not be stored (`--group`).
    let mut store_failures = 0usize;
    let mut read_errors = 0usize;
    // The group permission hint is shown once, at the first refusal.
    let mut group_hint_shown = false;
    let idle_timeout = args.timeout.map(Duration::from_millis);
    // Buffer terminal output: println! issues one write per message, which dominates
    // the cost of consuming large topics. Buffers are flushed whenever the stream pauses.
    let mut out =
        BufWriter::with_capacity(OUTPUT_BUFFER_BYTES, DeliveredBytes::new(std::io::stdout()));
    // Which records have fully reached stdout: with `--group` only their
    // offsets are stored, so a reader that closes the pipe early (`| head`)
    // gets the rest again on the next run.
    let mut ledger = OutputLedger::default();
    // The error of a write to a stdout whose reader went away.
    let mut closed: Option<std::io::Error> = None;
    // `--format json` prints one JSON object per record (JSON Lines, the same
    // shape as --save); status lines go to stderr in every mode.
    let json_lines = global.format == OutputFormat::Json;
    let mut unflushed = false;
    let ctrl_c = tokio::signal::ctrl_c();
    tokio::pin!(ctrl_c);
    let mut idle_deadline = idle_timeout.map(|d| tokio::time::Instant::now() + d);

    loop {
        // Records whose output has reached stdout by now are done with.
        let done = ledger.settle(out.get_ref().delivered);
        if use_group {
            store_failures += store_done(&consumer, &args.topic, &done);
        }
        if let Some(max) = args.max_messages {
            if stats.matched >= max as u64 {
                break;
            }
        }
        if time_stop.all_done() {
            break;
        }

        tokio::select! {
            biased;
            _ = &mut ctrl_c => {
                keep_open_output(out.flush(), &mut closed)?;
                if closed.is_none() {
                    eprintln!("\n{}", "Consumer interrupted by user (Ctrl+C).".yellow());
                }
                break;
            }
            event = next_event(&consumer, reader.as_mut(), !ends.is_empty()) => {
                let Some(event) = event else {
                    // Every partition was read to its end.
                    break;
                };
                match event {
                    ReadEvent::Skipped(count) => {
                        idle_deadline = idle_timeout.map(|d| tokio::time::Instant::now() + d);
                        stats.record_skips(count);
                    }
                    ReadEvent::Record(msg) => {
                        idle_deadline = idle_timeout.map(|d| tokio::time::Instant::now() + d);
                        // Past --to-timestamp: not processed (nor stored).
                        if !time_stop.admit(msg.partition, msg.timestamp_ms) {
                            stats.record_skip();
                            continue;
                        }
                        if ends.get(&msg.partition).is_some_and(|&end| msg.offset + 1 >= end) {
                            time_stop.finish(msg.partition);
                        }
                        let masked = mask.then(|| msg.masked());
                        let filtered = match &masked {
                            Some(m) if mask_filters => m,
                            _ => &msg,
                        };
                        if !filter.matches(filtered) {
                            // Done with: stored once what came before reached stdout.
                            ledger.add(msg.partition, msg.offset, 0);
                            stats.record_skip();
                            continue;
                        }

                        // Decoded before --where so expressions see Avro and
                        // Protobuf records as JSON.
                        // (`decoder` exists only with -D; JSON lines carry the
                        // decoded record next to the raw one.)
                        let decode = !args.hex;
                        let decoded = match (&mut decoder, msg.payload.as_deref()) {
                            (Some(decoder), Some(payload)) if decode => {
                                decoder.decode(payload).await
                            }
                            _ => None,
                        };
                        // Schema-encoded keys are shown decoded too.
                        let decoded_key = match (&mut decoder, msg.key.as_deref()) {
                            (Some(decoder), Some(key)) if decode => decoder.decode(key).await,
                            _ => None,
                        };
                        let mask_decoded = |d: Option<crate::codec::schema::DecodedPayload>| {
                            if mask {
                                d.map(|d| crate::codec::schema::DecodedPayload {
                                    text: mask_payload(&d.text, MaskStrategy::Partial),
                                    ..d
                                })
                            } else {
                                d
                            }
                        };
                        // Masked before --where when the policy enforces it.
                        let (decoded, decoded_key) = if mask_filters {
                            (mask_decoded(decoded), mask_decoded(decoded_key))
                        } else {
                            (decoded, decoded_key)
                        };
                        if let Some(tf) = &text_filter {
                            if !tf.matches(&decoded_view(filtered, decoded.as_ref(), decoded_key.as_ref())) {
                                ledger.add(msg.partition, msg.offset, 0);
                                stats.record_skip();
                                continue;
                            }
                        }
                        if let Some(expr) = &where_expr {
                            if !crate::kafka::filter::expression_matches(expr, filtered, decoded.as_ref(), decoded_key.as_ref()) {
                                ledger.add(msg.partition, msg.offset, 0);
                                stats.record_skip();
                                continue;
                            }
                        }

                        stats.record_match();
                        let (decoded, decoded_key) = if mask_filters {
                            (decoded, decoded_key)
                        } else {
                            (mask_decoded(decoded), mask_decoded(decoded_key))
                        };
                        let msg = masked.unwrap_or(msg);

                        if let Some(ref mut writer) = file_writer {
                            writeln!(writer, "{}", jsonl_record(&msg))?;
                        }

                        let line = if json_lines {
                            let mut record = jsonl_record(&msg);
                            add_decoded(&mut record, decoded.as_ref(), decoded_key.as_ref());
                            record.to_string()
                        } else {
                            let shown;
                            let msg_ref = match decoded_key {
                                Some(k) => {
                                    shown = ConsumedMessage {
                                        key: Some(
                                            crate::codec::schema::key_text(&k.text).into_bytes(),
                                        ),
                                        ..msg.clone()
                                    };
                                    &shown
                                }
                                None => &msg,
                            };
                            format_message(
                                msg_ref,
                                args.pretty_json,
                                args.show_headers,
                                args.decode_wire_format,
                                args.hex,
                                decoded.as_ref(),
                            )
                        };
                        // The line and its newline.
                        ledger.add(msg.partition, msg.offset, line.len() as u64 + 1);
                        keep_open_output(writeln!(out, "{line}"), &mut closed)?;
                        if closed.is_some() {
                            break;
                        }
                        unflushed = true;
                    }
                    ReadEvent::Error(err) => {
                        eprintln!("{} Consumer error: {err}", "✖".red());
                        if !group_hint_shown {
                            if let Some(hint) = group_permission_hint(&err, kafka_config.group_id.as_deref()) {
                                eprintln!("  {} {hint}", "ℹ".cyan());
                                group_hint_shown = true;
                            }
                        }
                        read_errors += 1;
                    }
                }
            }
            _ = tokio::time::sleep(OUTPUT_FLUSH_INTERVAL), if unflushed => {
                keep_open_output(out.flush(), &mut closed)?;
                if closed.is_some() {
                    break;
                }
                if let Some(ref mut writer) = file_writer {
                    writer.flush()?;
                }
                unflushed = false;
            }
            _ = sleep_until_or_forever(idle_deadline) => {
                keep_open_output(out.flush(), &mut closed)?;
                if closed.is_some() {
                    break;
                }
                eprintln!(
                    "{}",
                    format!("Idle timeout of {} ms reached with no incoming messages.", args.timeout.unwrap_or(0))
                        .bright_black()
                );
                break;
            }
        }
    }

    if closed.is_none() {
        keep_open_output(out.flush(), &mut closed)?;
    }
    if let Some(ref mut writer) = file_writer {
        writer.flush()?;
    }
    // Records whose output reached stdout (all of them unless the pipe
    // closed) are done with; with a group their offsets are stored.
    let done = ledger.settle(out.get_ref().delivered);
    if use_group {
        store_failures += store_done(&consumer, &args.topic, &done);
    }
    if let Some(error) = closed {
        // The reader went away (`| head`): what it was given counts as read.
        // Bytes still buffered would never arrive; drop them unwritten.
        let (_, _unwritten) = out.into_parts();
        let written = ledger.written;
        return finish_closed_output(&consumer, use_group, written, store_failures, error);
    }
    // Every matched record went to the file or the (redirected) stdout.
    crate::cli::export_finished(stats.matched);
    let commit = if use_group {
        consumer.commit_stored(CommitMode::Sync)
    } else {
        Ok(())
    };
    if let Err(e) = &commit {
        eprintln!("{} Final offset commit failed: {e}", "✖".red());
    }

    eprintln!(
        "{} Consumed {} matched message(s) from topic '{}'.",
        "✔".green().bold(),
        stats.matched.to_string().cyan().bold(),
        args.topic.yellow().bold()
    );

    if filter.is_active() || text_filter.is_some() {
        eprintln!("{}", stats.summary_string().bright_black().italic());
    }

    if let Some(ref path_str) = args.save {
        eprintln!(
            "{} Saved matched records to '{}'",
            "💾".cyan(),
            path_str.green().bold()
        );
    }

    // The group's position did not (fully) move: say so in the exit code,
    // so a script does not take the run as done and skip the re-read.
    group_outcome(commit.is_ok(), store_failures)?;
    read_outcome(read_errors, stats.total_received)
}

/// Stores, for each partition, the offset after the last record done with
/// (`done`: partition to that record's offset). Returns how many failed.
fn store_done(
    consumer: &KafkaConsumerClient,
    topic: &str,
    done: &std::collections::BTreeMap<i32, i64>,
) -> usize {
    let mut failures = 0;
    for (&partition, &offset) in done {
        // librdkafka stores `offset + 1`, the next record to read.
        if let Err(e) = consumer.store_processed_offset(topic, partition, offset) {
            eprintln!("{} Could not store offset: {e}", "✖".red());
            failures += 1;
        }
    }
    failures
}

/// The end of a run whose stdout reader went away (`consume ... | head`):
/// commits the group's offsets of the records that reached stdout (stored
/// already), audits the outcome and returns the pipe error, which ends the
/// command with exit status 0 (see `cli::exit_with_error`).
fn finish_closed_output(
    consumer: &KafkaConsumerClient,
    use_group: bool,
    written: u64,
    store_failures: usize,
    error: std::io::Error,
) -> anyhow::Result<()> {
    crate::cli::export_finished(written);
    let outcome = if !use_group {
        format!("{written} record(s) written")
    } else {
        match consumer.commit_stored(CommitMode::Sync) {
            Ok(()) if store_failures == 0 => {
                format!("committed the offsets of {written} written record(s)")
            }
            Ok(()) => format!(
                "committed the offsets of {written} written record(s); {store_failures} offset(s) could not be stored"
            ),
            Err(e) => {
                // stderr may be gone too: never panic on it.
                let _ = writeln!(std::io::stderr(), "Final offset commit failed: {e}");
                format!("the offsets of {written} written record(s) were not committed")
            }
        }
    };
    let _ = writeln!(
        std::io::stderr(),
        "Output closed after {written} record(s); {outcome}."
    );
    crate::cli::record_audit_result(
        true,
        &format!("ended with a pipe error (output closed?); {outcome}"),
    );
    Err(error.into())
}

/// Whether a write failed because the reader of the output went away:
/// EPIPE (os error 32), on Windows ERROR_BROKEN_PIPE (109) or
/// ERROR_NO_DATA (232).
fn is_closed_output(error: &std::io::Error) -> bool {
    let closed_codes: &[i32] = if cfg!(windows) { &[109, 232] } else { &[32] };
    error.kind() == std::io::ErrorKind::BrokenPipe
        || error
            .raw_os_error()
            .is_some_and(|code| closed_codes.contains(&code))
}

/// A write to stdout: a closed pipe is kept in `closed` (the run then ends
/// and commits what was written) instead of being returned.
fn keep_open_output(
    result: std::io::Result<()>,
    closed: &mut Option<std::io::Error>,
) -> std::io::Result<()> {
    match result {
        Err(e) if is_closed_output(&e) => {
            *closed = Some(e);
            Ok(())
        }
        other => other,
    }
}

/// A writer that counts the bytes its inner writer accepted: under a
/// `BufWriter`, the bytes that actually reached stdout.
struct DeliveredBytes<W> {
    inner: W,
    delivered: u64,
}

impl<W> DeliveredBytes<W> {
    fn new(inner: W) -> Self {
        Self {
            inner,
            delivered: 0,
        }
    }
}

impl<W: Write> Write for DeliveredBytes<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.delivered += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

/// Records handed to the output, in order, and how far into the output each
/// one ends, so that the records fully written can be told from the ones
/// still buffered when the reader goes away.
#[derive(Debug, Default)]
struct OutputLedger {
    /// Output bytes handed to the writer so far.
    queued: u64,
    /// (partition, offset, output end, printed) of records not settled yet.
    pending: std::collections::VecDeque<(i32, i64, u64, bool)>,
    /// Printed records whose output was fully delivered.
    written: u64,
}

impl OutputLedger {
    /// A record whose output (`bytes` long, 0 for a record filtered out) was
    /// handed to the writer after everything added before.
    fn add(&mut self, partition: i32, offset: i64, bytes: u64) {
        self.queued += bytes;
        self.pending
            .push_back((partition, offset, self.queued, bytes > 0));
    }

    /// Settles the records whose output lies within the first `delivered`
    /// bytes. Returns the offset of the last settled record per partition;
    /// records after the first unsettled one are never included.
    fn settle(&mut self, delivered: u64) -> std::collections::BTreeMap<i32, i64> {
        let mut done = std::collections::BTreeMap::new();
        while let Some(&(partition, offset, end, printed)) = self.pending.front() {
            if end > delivered {
                break;
            }
            self.pending.pop_front();
            done.insert(partition, offset);
            if printed {
                self.written += 1;
            }
        }
        done
    }
}

/// `Err` when the consumer reported errors and nothing was read (no read
/// permission on the topic, brokers gone): "Consumed 0" alone would look
/// like an empty topic to a script.
fn read_outcome(errors: usize, received: u64) -> anyhow::Result<()> {
    if errors > 0 && received == 0 {
        anyhow::bail!("nothing was read and the consumer reported {errors} error(s) (see above)");
    }
    Ok(())
}

/// What to grant when the group is refused: Kafka lets Read on a group
/// imply Describe, Redpanda asks for Describe separately (FindCoordinator).
fn group_permission_hint(err: &crate::error::KafkaError, group: Option<&str>) -> Option<String> {
    let refused = matches!(
        err,
        crate::error::KafkaError::Driver(e)
            if e.code() == Some(crate::error::ErrorCode::GroupAuthorizationFailed)
    );
    refused.then(|| {
        let group = group.unwrap_or("<group>");
        format!(
            "The user needs Read on group '{group}' (Redpanda also needs Describe): \
             ferrika acl create -p <user> -o read -g {group}, and -o describe -g {group}"
        )
    })
}

/// `msg` with its key and value replaced by their decoded text where `-D`
/// decoded them, for `--grep` and `-k`.
fn decoded_view(
    msg: &ConsumedMessage,
    value: Option<&crate::codec::schema::DecodedPayload>,
    key: Option<&crate::codec::schema::DecodedPayload>,
) -> ConsumedMessage {
    let mut view = msg.clone();
    if let Some(value) = value {
        view.payload = Some(value.text.clone().into_bytes());
    }
    if let Some(key) = key {
        view.key = Some(crate::codec::schema::key_text(&key.text).into_bytes());
    }
    view
}

/// `--save` adds records to a file that already has some: said, or running
/// the command twice reads like one run's output. `None` for a new or empty
/// file.
fn append_notice(path: &Path) -> Option<String> {
    let len = std::fs::metadata(path).ok()?.len();
    (len > 0).then(|| {
        format!(
            "Appending to '{}', which already holds {len} byte(s).",
            path.display()
        )
    })
}

/// Adds `-D`'s decoded value and key to a JSON line (`decoded`,
/// `decoded_key`, and `decoded_with` naming the schema), as JSON when the
/// text is JSON. The raw fields stay, so the line still restores. Text that
/// is neither JSON nor decoded with a schema (a failed lookup of an Avro or
/// Protobuf schema leaves the raw bytes) is left out.
fn add_decoded(
    record: &mut serde_json::Value,
    value: Option<&crate::codec::schema::DecodedPayload>,
    key: Option<&crate::codec::schema::DecodedPayload>,
) {
    let as_json = |d: &crate::codec::schema::DecodedPayload| {
        serde_json::from_str::<serde_json::Value>(&d.text)
            .ok()
            .or_else(|| {
                d.schema_ref
                    .is_some()
                    .then(|| serde_json::Value::from(d.text.clone()))
            })
    };
    let Some(obj) = record.as_object_mut() else {
        return;
    };
    if let Some(d) = value {
        if let Some(v) = as_json(d) {
            obj.insert("decoded".into(), v);
            obj.insert("decoded_with".into(), d.label.clone().into());
        }
    }
    if let Some(k) = key.and_then(as_json) {
        obj.insert("decoded_key".into(), k);
    }
}

/// `Err` when `consume --group` could not commit where it got to: the next
/// run with the group reads (some of) these records again.
fn group_outcome(committed: bool, store_failures: usize) -> anyhow::Result<()> {
    if !committed {
        anyhow::bail!(
            "the group's offsets were not committed; the next run with this group reads these records again"
        );
    }
    if store_failures > 0 {
        anyhow::bail!(
            "{store_failures} record offset(s) could not be stored; the next run with this group may read them again"
        );
    }
    Ok(())
}

/// The next record from `reader` when there is one, else from `consumer`.
/// `None` when the reader read every partition to its end (`bounded`: each
/// was given one).
async fn next_event(
    consumer: &KafkaConsumerClient,
    reader: Option<&mut ParallelReader>,
    bounded: bool,
) -> Option<ReadEvent> {
    match reader {
        Some(reader) => match reader.recv().await {
            Some(event) => Some(event),
            None if bounded => None,
            // Every reader thread has stopped; the idle timeout or Ctrl+C ends the command.
            None => std::future::pending().await,
        },
        None => Some(match consumer.recv().await {
            Ok(msg) => ReadEvent::Record(msg),
            Err(err) => ReadEvent::Error(err),
        }),
    }
}

/// Start offsets from a `--from-timestamp` lookup. A partition the broker
/// could not answer for (a moving leader, a timeout) is an error: starting
/// it at the end would leave its records out without a word.
fn time_lookup_offsets(
    lookups: std::collections::HashMap<(String, i32), crate::kafka::consumer::Lookup>,
) -> anyhow::Result<std::collections::HashMap<(String, i32), StartOffset>> {
    use crate::kafka::consumer::Lookup;
    lookups
        .into_iter()
        .map(|(key, lookup)| match lookup {
            Lookup::At(offset) => Ok((key, StartOffset::At(offset))),
            Lookup::End => Ok((key, StartOffset::End)),
            Lookup::Failed(failure) => Err(anyhow::anyhow!(
                "cannot find where partition {} starts at --from-timestamp: {}",
                key.1,
                failure.message
            )),
        })
        .collect()
}

/// Whether reading from `start` finds nothing before `high` (an empty
/// partition, or a start at its end).
fn starts_at_end(start: StartOffset, low: i64, high: i64) -> bool {
    match start {
        StartOffset::Beginning => low >= high,
        StartOffset::End => true,
        StartOffset::At(o) => o >= high,
        StartOffset::Tail(n) => n <= 0 || low >= high,
        _ => false,
    }
}

/// Size of the stdout and --save buffers.
const OUTPUT_BUFFER_BYTES: usize = 256 * 1024;

/// Buffered output is flushed when no message arrives within this interval.
const OUTPUT_FLUSH_INTERVAL: Duration = Duration::from_millis(50);

/// An explicit `--offset` moved into a partition's available range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OffsetClamp {
    /// The offset actually used.
    pub to: i64,
    /// Requested offset was past the high watermark (else: before the low one).
    pub above: bool,
}

/// Moves a non-negative `--offset` into `[low, high]`; `None` when it already
/// lies inside. Past `high` nothing exists yet, so reading starts at `high`
/// (new records only) instead of silently falling back to the beginning.
pub(crate) fn clamp_offset(requested: i64, low: i64, high: i64) -> Option<OffsetClamp> {
    if requested > high {
        Some(OffsetClamp {
            to: high,
            above: true,
        })
    } else if requested < low {
        Some(OffsetClamp {
            to: low,
            above: false,
        })
    } else {
        None
    }
}

/// Start offsets for `--offset N` (N >= 0) per partition, clamped to each
/// partition's watermarks, with one stderr warning per adjusted partition.
async fn clamped_starts(
    consumer: &KafkaConsumerClient,
    topic: &str,
    partitions: &[i32],
    requested: i64,
) -> anyhow::Result<Vec<(i32, StartOffset)>> {
    let mut starts = Vec::with_capacity(partitions.len());
    for &p in partitions {
        let (low, high) = consumer.fetch_watermarks_async(topic, p, None).await?;
        let start = match clamp_offset(requested, low, high) {
            Some(c) => {
                eprintln!(
                    "{}",
                    format!(
                        "Warning: --offset {requested} is {} partition {p} ({low}..{high}); starting at offset {}.",
                        if c.above { "past the end of" } else { "before the start of" },
                        c.to
                    )
                    .yellow()
                );
                c.to
            }
            None => requested,
        };
        starts.push((p, StartOffset::At(start)));
    }
    Ok(starts)
}

/// Resolves the starting offset for an explicit partition assignment.
/// Without a group there are no stored offsets, so it starts at the end (new messages only)
/// unless --from-beginning is given.
fn start_offset(offset: Option<i64>, from_beginning: bool, use_group: bool) -> StartOffset {
    match offset {
        Some(off) if off >= 0 => StartOffset::At(off),
        Some(off) => StartOffset::Tail(off.unsigned_abs() as i64),
        None if from_beginning => StartOffset::Beginning,
        None if use_group => StartOffset::Committed,
        None => StartOffset::End,
    }
}

async fn sleep_until_or_forever(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(d) => tokio::time::sleep_until(d).await,
        None => std::future::pending::<()>().await,
    }
}

/// Formats a consumed message with terminal colors and optional pretty JSON, wire format decoding, or hex dump.
#[cfg(test)]
fn format_message_display_ext(
    msg: &ConsumedMessage,
    pretty_json: bool,
    show_headers: bool,
    decode_wire_format: bool,
    show_hex: bool,
) -> String {
    format_message(
        msg,
        pretty_json,
        show_headers,
        decode_wire_format,
        show_hex,
        None,
    )
}

/// `format_message_display_ext` with a payload already decoded through the
/// Schema Registry, shown in place of the raw wire-format body.
fn format_message(
    msg: &ConsumedMessage,
    pretty_json: bool,
    show_headers: bool,
    decode_wire_format: bool,
    show_hex: bool,
    decoded: Option<&crate::codec::schema::DecodedPayload>,
) -> String {
    let mut out = String::new();

    // Meta header line
    let partition_info = format!("[P:{} | O:{}]", msg.partition, msg.offset);
    let time_info = if let Some(ts) = msg.timestamp_ms {
        format!(" @ {ts}")
    } else {
        String::new()
    };

    let meta_str = format!("{partition_info}{time_info}").cyan().to_string();
    out.push_str(&meta_str);

    // Key
    if let Some(key) = msg.key_str() {
        out.push_str(&format!(
            "  {} {}",
            "key:".bright_black(),
            key.yellow().bold()
        ));
    }

    // Headers
    if show_headers && !msg.headers.is_empty() {
        let mut header_parts = Vec::new();
        for (k, v) in &msg.headers {
            let val_str = std::str::from_utf8(v).unwrap_or("<binary>");
            header_parts.push(format!("{k}={val_str}"));
        }
        out.push_str(&format!(
            "  {} [{}]",
            "headers:".bright_black(),
            header_parts.join(", ").magenta()
        ));
    }

    // Check Confluent Schema Registry wire format if requested
    let wire_format = if decode_wire_format {
        msg.payload.as_deref().and_then(parse_confluent_wire_format)
    } else {
        None
    };

    if let Some(decoded) = decoded {
        out.push_str(&format!(
            "  {}",
            format!("[{}]", decoded.label).magenta().bold()
        ));
    } else if let Some(ref wire) = wire_format {
        out.push_str(&format!(
            "  {} {}",
            "[Schema ID:".magenta().bold(),
            format!("{}]", wire.schema_id).yellow().bold()
        ));
    }

    out.push('\n');

    // Hex mode
    if show_hex {
        if let Some(ref wire) = wire_format {
            out.push_str(&format_hexdump(&wire.payload));
        } else if let Some(ref payload) = msg.payload {
            out.push_str(&format_hexdump(payload));
        } else {
            out.push_str("<null payload>");
        }
        return out;
    }

    // Payload text / JSON formatting
    if let Some(decoded) = decoded {
        let pretty = pretty_json
            .then(|| serde_json::from_str::<serde_json::Value>(&decoded.text).ok())
            .flatten()
            .and_then(|v| serde_json::to_string_pretty(&v).ok());
        out.push_str(pretty.as_deref().unwrap_or(&decoded.text));
    } else if let Some(ref wire) = wire_format {
        if let Some(payload_str) = wire.payload_as_str() {
            if pretty_json && wire.is_json {
                if let Ok(val) = serde_json::from_str::<serde_json::Value>(payload_str) {
                    if let Ok(pretty) = serde_json::to_string_pretty(&val) {
                        out.push_str(&pretty);
                        return out;
                    }
                }
            }
            out.push_str(payload_str);
        } else {
            out.push_str(&wire.preview(64));
        }
    } else if let Some(payload_str) = msg.payload_str() {
        if pretty_json {
            if let Ok(val) = serde_json::from_str::<serde_json::Value>(payload_str) {
                if let Ok(pretty) = serde_json::to_string_pretty(&val) {
                    out.push_str(&pretty);
                    return out;
                }
            }
        }
        out.push_str(payload_str);
    } else if let Some(ref payload) = msg.payload {
        out.push_str(&format_hexdump(payload));
    } else {
        out.push_str("<null payload>");
    }

    out
}

/// `format_message_display_ext` without decoding or hex.
#[cfg(test)]
fn format_message_display(msg: &ConsumedMessage, pretty_json: bool, show_headers: bool) -> String {
    format_message_display_ext(msg, pretty_json, show_headers, false, false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_records_that_fully_reached_stdout_are_settled() {
        let mut ledger = OutputLedger::default();
        ledger.add(0, 10, 5); // bytes 0..5
        ledger.add(1, 7, 4); // 5..9
        ledger.add(0, 11, 0); // filtered out
        ledger.add(0, 12, 6); // 9..15
        ledger.add(1, 8, 3); // 15..18
                             // Nothing reached stdout yet.
        assert!(ledger.settle(0).is_empty());
        // 10 bytes: the first two records and the filtered one after them;
        // the record at 0/12 was cut short.
        let done = ledger.settle(10);
        assert_eq!(done, [(0, 11), (1, 7)].into_iter().collect());
        assert_eq!(ledger.written, 2);
        // Settled records are not reported twice.
        assert!(ledger.settle(10).is_empty());
        let done = ledger.settle(18);
        assert_eq!(done, [(0, 12), (1, 8)].into_iter().collect());
        assert_eq!(ledger.written, 4);
    }

    #[test]
    fn a_filtered_record_after_an_unwritten_one_is_not_settled() {
        let mut ledger = OutputLedger::default();
        ledger.add(0, 1, 4);
        ledger.add(0, 2, 4);
        ledger.add(0, 3, 0);
        // The record at offset 2 is still buffered: offset 3 must wait too.
        assert_eq!(ledger.settle(6), [(0, 1)].into_iter().collect());
        assert_eq!(ledger.written, 1);
        // Filtered records with nothing printed before them settle at once.
        let mut ledger = OutputLedger::default();
        ledger.add(2, 40, 0);
        assert_eq!(ledger.settle(0), [(2, 40)].into_iter().collect());
        assert_eq!(ledger.written, 0);
    }

    #[test]
    fn delivered_bytes_count_what_reached_the_inner_writer() {
        // A writer that takes at most 3 bytes and then fails like a closed pipe.
        struct Pipe(usize);
        impl Write for Pipe {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                if self.0 == 0 {
                    return Err(std::io::ErrorKind::BrokenPipe.into());
                }
                let n = buf.len().min(self.0);
                self.0 -= n;
                Ok(n)
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let mut out = BufWriter::with_capacity(16, DeliveredBytes::new(Pipe(3)));
        let mut ledger = OutputLedger::default();
        let mut closed = None;
        for (offset, line) in [(0, "a"), (1, "b"), (2, "cc")] {
            ledger.add(0, offset, line.len() as u64 + 1);
            keep_open_output(writeln!(out, "{line}"), &mut closed).unwrap();
        }
        keep_open_output(out.flush(), &mut closed).unwrap();
        assert!(closed.is_some());
        // "a\n" and the first byte of "b\n" got through: only offset 0 counts.
        assert_eq!(out.get_ref().delivered, 3);
        assert_eq!(
            ledger.settle(out.get_ref().delivered),
            [(0, 0)].into_iter().collect()
        );
        assert_eq!(ledger.written, 1);
    }

    #[test]
    fn a_closed_pipe_is_kept_and_other_errors_returned() {
        let mut closed = None;
        assert!(keep_open_output(Ok(()), &mut closed).is_ok());
        assert!(closed.is_none());
        let other = std::io::Error::other("disk full");
        assert!(keep_open_output(Err(other), &mut closed).is_err());
        assert!(closed.is_none());
        let pipe = std::io::Error::from(std::io::ErrorKind::BrokenPipe);
        assert!(keep_open_output(Err(pipe), &mut closed).is_ok());
        assert!(closed.is_some());
    }

    #[test]
    fn partitions_with_nothing_to_read_are_recognised() {
        assert!(starts_at_end(StartOffset::Beginning, 5, 5));
        assert!(!starts_at_end(StartOffset::Beginning, 0, 5));
        assert!(starts_at_end(StartOffset::End, 0, 5));
        assert!(starts_at_end(StartOffset::At(5), 0, 5));
        assert!(!starts_at_end(StartOffset::At(4), 0, 5));
        assert!(!starts_at_end(StartOffset::Tail(3), 0, 5));
        assert!(starts_at_end(StartOffset::Tail(3), 5, 5));
        // A group's stored position is not known here.
        assert!(!starts_at_end(StartOffset::Committed, 5, 5));
    }

    #[test]
    fn a_failed_time_lookup_stops_instead_of_starting_at_the_end() {
        use crate::kafka::consumer::{Lookup, LookupFailure};
        let ok = time_lookup_offsets(
            [
                (("t".to_string(), 0), Lookup::At(7)),
                (("t".to_string(), 1), Lookup::End),
            ]
            .into(),
        )
        .unwrap();
        assert_eq!(ok[&("t".to_string(), 0)], StartOffset::At(7));
        assert_eq!(ok[&("t".to_string(), 1)], StartOffset::End);
        let failed = time_lookup_offsets(
            [(
                ("t".to_string(), 2),
                Lookup::Failed(LookupFailure::new(
                    "NotLeaderForPartition",
                    crate::error::ErrorClass::Retriable,
                )),
            )]
            .into(),
        )
        .unwrap_err()
        .to_string();
        assert!(
            failed.contains("partition 2") && failed.contains("NotLeaderForPartition"),
            "{failed}"
        );
    }

    #[test]
    fn a_refused_group_names_the_permissions_to_grant() {
        use rdkafka::types::RDKafkaErrorCode as Code;
        let refused = crate::error::KafkaError::from(
            rdkafka::error::KafkaError::MessageConsumption(Code::GroupAuthorizationFailed),
        );
        let hint = group_permission_hint(&refused, Some("billing")).unwrap_or_default();
        assert!(
            hint.contains("-o read -g billing") && hint.contains("-o describe -g billing"),
            "{hint}"
        );
        let other = crate::error::KafkaError::from(rdkafka::error::KafkaError::MessageConsumption(
            Code::TopicAuthorizationFailed,
        ));
        assert_eq!(group_permission_hint(&other, Some("billing")), None);
    }

    #[test]
    fn grep_sees_the_decoded_text() {
        use crate::codec::schema::DecodedPayload;
        let msg = ConsumedMessage {
            topic: "t".into(),
            partition: 0,
            offset: 0,
            key: Some(vec![0, 0, 0, 0, 1, 2]),
            payload: Some(vec![0, 0, 0, 0, 7, 0x0a, 0xff]),
            timestamp_ms: None,
            headers: Vec::new(),
            null_headers: Vec::new(),
        };
        let decoded = DecodedPayload {
            text: r#"{"name":"alice"}"#.into(),
            label: "AVRO schema 7".into(),
            schema_ref: None,
        };
        let grep = MessageFilter::new().with_grep("alice").unwrap();
        assert!(!grep.matches(&msg));
        assert!(grep.matches(&decoded_view(&msg, Some(&decoded), None)));
        // Undecoded parts stay as they were.
        assert_eq!(decoded_view(&msg, Some(&decoded), None).key, msg.key);
    }

    #[test]
    fn saving_into_a_non_empty_file_is_announced() {
        let dir = std::env::temp_dir().join(format!("ferrika-save-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("out.jsonl");
        assert_eq!(append_notice(&path), None);
        std::fs::write(&path, "").unwrap();
        assert_eq!(append_notice(&path), None);
        std::fs::write(&path, "{}\n").unwrap();
        assert!(append_notice(&path).is_some_and(|n| n.contains("3 byte(s)")));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn errors_with_nothing_read_fail_the_run() {
        assert!(read_outcome(3, 0).is_err());
        // Transient errors during a run that read records are only shown.
        assert!(read_outcome(3, 10).is_ok());
        // An empty topic read without errors is fine.
        assert!(read_outcome(0, 0).is_ok());
    }

    #[test]
    fn json_lines_carry_the_decoded_record() {
        use crate::codec::schema::DecodedPayload;
        let decoded = DecodedPayload {
            text: r#"{"amount":42}"#.into(),
            label: "AVRO schema 12".into(),
            schema_ref: None,
        };
        let failed = DecodedPayload {
            text: "not decoded".into(),
            label: "schema 12 (lookup failed: timeout)".into(),
            schema_ref: None,
        };
        let mut record = serde_json::json!({"payload_base64": "AAAA"});
        add_decoded(&mut record, Some(&decoded), Some(&failed));
        assert_eq!(record["decoded"]["amount"], 42);
        assert_eq!(record["decoded_with"], "AVRO schema 12");
        assert_eq!(record["payload_base64"], "AAAA");
        assert!(record.get("decoded_key").is_none(), "{record}");
    }

    #[test]
    fn group_runs_fail_when_their_position_was_not_saved() {
        assert!(group_outcome(true, 0).is_ok());
        let not_committed = group_outcome(false, 0).unwrap_err().to_string();
        assert!(not_committed.contains("not committed"), "{not_committed}");
        let not_stored = group_outcome(true, 2).unwrap_err().to_string();
        assert!(not_stored.starts_with("2 record offset(s)"), "{not_stored}");
    }

    #[test]
    fn clamp_offset_past_high_watermark_starts_at_high() {
        assert_eq!(
            clamp_offset(99, 0, 10),
            Some(OffsetClamp {
                to: 10,
                above: true
            })
        );
    }

    #[test]
    fn clamp_offset_before_low_watermark_starts_at_low() {
        assert_eq!(
            clamp_offset(2, 5, 10),
            Some(OffsetClamp {
                to: 5,
                above: false
            })
        );
    }

    #[test]
    fn clamp_offset_inside_range_is_untouched() {
        assert_eq!(clamp_offset(0, 0, 10), None);
        assert_eq!(clamp_offset(7, 5, 10), None);
        assert_eq!(
            clamp_offset(10, 5, 10),
            None,
            "high itself waits for new records"
        );
        assert_eq!(clamp_offset(0, 0, 0), None);
    }

    fn header(msg: &ConsumedMessage, name: &str) -> Vec<u8> {
        msg.headers
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.clone())
            .unwrap()
    }

    #[test]
    fn masked_message_hides_pii_in_key_payload_and_headers() {
        let msg = ConsumedMessage {
            topic: "users".into(),
            partition: 0,
            offset: 7,
            key: Some(b"kim@example.com".to_vec()),
            payload: Some(
                br#"{"name":"kim","password":"hunter2","card":"4111111111111111"}"#.to_vec(),
            ),
            timestamp_ms: None,
            headers: Vec::from([
                ("trace".to_string(), b"ok".to_vec()),
                ("bin".to_string(), vec![0xff, 0xfe]),
            ]),
            null_headers: Vec::new(),
        };
        let masked = msg.masked();
        let payload = String::from_utf8(masked.payload.clone().unwrap()).unwrap();
        assert!(!payload.contains("hunter2") && !payload.contains("4111111111111111"));
        // The name is masked by its key, not hidden whole.
        assert!(payload.contains(r#""name":"k**""#), "{payload}");
        assert_ne!(masked.key.clone().unwrap(), b"kim@example.com".to_vec());
        assert_eq!(header(&masked, "trace"), b"ok".to_vec());
        assert_eq!(header(&masked, "bin"), b"[binary 2 bytes hidden]".to_vec());
    }

    #[test]
    fn masked_message_judges_header_values_by_name() {
        let msg = ConsumedMessage {
            topic: "users".into(),
            partition: 0,
            offset: 1,
            key: None,
            payload: None,
            timestamp_ms: None,
            headers: Vec::from([
                ("customer_name".to_string(), "김민준".as_bytes().to_vec()),
                ("x-api-key".to_string(), b"abc".to_vec()),
            ]),
            null_headers: Vec::new(),
        };
        let masked = msg.masked();
        assert_eq!(
            header(&masked, "customer_name"),
            "김*준".as_bytes().to_vec()
        );
        assert_eq!(header(&masked, "x-api-key"), b"****".to_vec());
    }

    #[test]
    fn filters_on_the_masked_rendering_cannot_see_raw_values() {
        let msg = ConsumedMessage {
            topic: "users".into(),
            partition: 0,
            offset: 1,
            key: Some(b"kim@example.com".to_vec()),
            payload: Some(br#"{"rrn":"900101-1234567","note":"kim@example.com"}"#.to_vec()),
            timestamp_ms: None,
            headers: Vec::from([("x-api-key".to_string(), b"abc123".to_vec())]),
            null_headers: Vec::new(),
        };
        let masked = msg.masked();
        let checks = [
            MessageFilter::new().with_grep("1234567").unwrap(),
            MessageFilter::new().with_key("kim@"),
            MessageFilter::new().with_header("x-api-key", "abc123"),
        ];
        for f in &checks {
            // The raw record matches, the masked one the policy shows does not.
            assert!(f.matches(&msg) && !f.matches(&masked), "{f:?}");
        }
        let expr = crate::model::expr::Expr::parse(r#"$.rrn =~ "^900101-1234""#).unwrap();
        assert!(crate::kafka::filter::expression_matches(
            &expr, &msg, None, None
        ));
        assert!(!crate::kafka::filter::expression_matches(
            &expr, &masked, None, None
        ));
        let decoded = crate::codec::schema::DecodedPayload {
            text: mask_payload(r#"{"rrn":"900101-1234567"}"#, MaskStrategy::Partial),
            label: "AVRO schema 1".into(),
            schema_ref: Some(crate::model::schema::SchemaRef::id(1)),
        };
        assert!(!crate::kafka::filter::expression_matches(
            &expr,
            &masked,
            Some(&decoded),
            None
        ));
    }

    #[test]
    fn test_format_message_display_plain() {
        let msg = ConsumedMessage {
            topic: "events".into(),
            partition: 1,
            offset: 42,
            key: Some(b"order_key".to_vec()),
            payload: Some(b"hello world".to_vec()),
            timestamp_ms: Some(1726900000000),
            headers: Vec::new(),
            null_headers: Vec::new(),
        };

        let formatted = format_message_display(&msg, false, false);
        assert!(formatted.contains("P:1 | O:42"));
        assert!(formatted.contains("order_key"));
        assert!(formatted.contains("hello world"));
    }

    #[test]
    fn test_format_message_display_pretty_json() {
        let msg = ConsumedMessage {
            topic: "events".into(),
            partition: 0,
            offset: 10,
            key: None,
            payload: Some(b"{\"name\":\"Alice\",\"age\":30}".to_vec()),
            timestamp_ms: None,
            headers: Vec::new(),
            null_headers: Vec::new(),
        };

        let formatted = format_message_display(&msg, true, false);
        assert!(formatted.contains("\"name\": \"Alice\""));
        assert!(formatted.contains("\"age\": 30"));
    }

    #[test]
    fn show_headers_lists_repeated_keys_in_order() {
        let msg = ConsumedMessage {
            topic: "events".into(),
            partition: 0,
            offset: 10,
            key: None,
            payload: Some(b"data".to_vec()),
            timestamp_ms: None,
            headers: vec![
                ("x".to_string(), b"1".to_vec()),
                ("x".to_string(), b"2".to_vec()),
                ("y".to_string(), b"z".to_vec()),
            ],
            null_headers: Vec::new(),
        };
        let formatted = format_message_display(&msg, false, true);
        assert!(formatted.contains("x=1, x=2, y=z"), "{formatted}");
    }

    #[test]
    fn saved_jsonl_keeps_repeated_headers_and_unique_ones_as_before() {
        let mut msg = ConsumedMessage {
            topic: "t".into(),
            partition: 0,
            offset: 1,
            key: None,
            payload: Some(b"p".to_vec()),
            timestamp_ms: None,
            headers: vec![
                ("a".to_string(), b"1".to_vec()),
                ("b".to_string(), vec![0xff]),
            ],
            null_headers: Vec::new(),
        };
        let plain = jsonl_record(&msg);
        assert!(plain.get("header_list").is_none());
        assert_eq!(plain["headers"]["a"], "1");
        assert_eq!(plain["headers_base64"]["b"], "/w==");

        msg.headers = vec![
            ("x".to_string(), b"1".to_vec()),
            ("x".to_string(), b"2".to_vec()),
        ];
        let line = jsonl_record(&msg).to_string();
        let record: crate::ops::backup::DumpRecord = serde_json::from_str(&line).unwrap();
        assert_eq!(record.extract_headers().unwrap(), msg.headers, "{line}");
    }

    #[test]
    fn test_format_message_display_with_headers() {
        let headers = vec![("trace_id".to_string(), b"xyz789".to_vec())];

        let msg = ConsumedMessage {
            topic: "events".into(),
            partition: 0,
            offset: 10,
            key: None,
            payload: Some(b"data".to_vec()),
            timestamp_ms: None,
            headers,
            null_headers: Vec::new(),
        };

        let formatted = format_message_display(&msg, false, true);
        assert!(formatted.contains("headers:"));
        assert!(formatted.contains("trace_id=xyz789"));
    }

    #[test]
    fn test_clap_consume_args_parsing() {
        use clap::Parser;

        #[derive(Parser, Debug)]
        struct TestCli {
            #[command(flatten)]
            args: ConsumeArgs,
        }

        let cli = TestCli::try_parse_from([
            "test",
            "my-topic",
            "-g",
            "group-1",
            "-b",
            "-n",
            "50",
            "-t",
            "1000",
            "--pretty-json",
            "--show-headers",
        ])
        .unwrap();

        assert_eq!(cli.args.topic, "my-topic");
        assert_eq!(cli.args.group.as_deref(), Some("group-1"));
        assert!(cli.args.from_beginning);
        assert_eq!(cli.args.max_messages, Some(50));
        assert_eq!(cli.args.timeout, Some(1000));
        assert!(cli.args.pretty_json);
        assert!(cli.args.show_headers);
    }

    #[test]
    fn test_clap_consume_args_filtering_and_save() {
        use clap::Parser;

        #[derive(Parser, Debug)]
        struct TestCli {
            #[command(flatten)]
            args: ConsumeArgs,
        }

        let cli = TestCli::try_parse_from([
            "test",
            "orders",
            "-G",
            "ERROR",
            "-k",
            "order-",
            "--header-filter",
            "env:prod",
            "--case-sensitive",
            "-s",
            "output.jsonl",
        ])
        .unwrap();

        assert_eq!(cli.args.topic, "orders");
        assert_eq!(cli.args.grep.as_deref(), Some("ERROR"));
        assert_eq!(cli.args.key_filter.as_deref(), Some("order-"));
        assert_eq!(cli.args.header_filter, vec!["env:prod".to_string()]);
        assert!(cli.args.case_sensitive);
        assert_eq!(cli.args.save.as_deref(), Some("output.jsonl"));
    }

    #[test]
    fn test_format_message_display_confluent_wire_format() {
        use crate::codec::schema::encode_confluent_wire_format;

        let wire_payload = encode_confluent_wire_format(42, b"{\"order_id\":101}");
        let msg = ConsumedMessage {
            topic: "orders".into(),
            partition: 0,
            offset: 5,
            key: Some(b"k1".to_vec()),
            payload: Some(wire_payload),
            timestamp_ms: None,
            headers: Vec::new(),
            null_headers: Vec::new(),
        };

        // Without decode_wire_format
        let raw = format_message_display_ext(&msg, false, false, false, false);
        assert!(!raw.contains("Schema ID"));

        // With decode_wire_format
        let decoded = format_message_display_ext(&msg, false, false, true, false);
        assert!(decoded.contains("Schema ID:"));
        assert!(decoded.contains("42"));
        assert!(decoded.contains("{\"order_id\":101}"));
    }

    #[test]
    fn test_format_message_display_hex_dump() {
        let msg = ConsumedMessage {
            topic: "telemetry".into(),
            partition: 0,
            offset: 12,
            key: None,
            payload: Some(vec![0x00, 0x01, 0x02, 0xfe, 0xff]),
            timestamp_ms: None,
            headers: Vec::new(),
            null_headers: Vec::new(),
        };

        let dump = format_message_display_ext(&msg, false, false, false, true);
        assert!(dump.contains("00000000"));
        assert!(dump.contains("00 01 02 fe ff"));
    }

    #[test]
    fn test_clap_consume_args_wire_format_and_hex() {
        use clap::Parser;

        #[derive(Parser, Debug)]
        struct TestCli {
            #[command(flatten)]
            args: ConsumeArgs,
        }

        let cli = TestCli::try_parse_from([
            "test",
            "telemetry",
            "-D",
            "--hex",
            "-S",
            "http://registry:8081",
        ])
        .unwrap();

        assert!(cli.args.decode_wire_format);
        assert!(cli.args.hex);
        assert_eq!(
            cli.args.schema_registry.as_deref(),
            Some("http://registry:8081")
        );
    }
}
