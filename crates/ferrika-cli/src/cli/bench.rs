use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::{Args, Subcommand};
use colored::Colorize;
use comfy_table::{Cell, Color};

use crate::cli::{output, print_connection_hint, GlobalOpts, OutputFormat};
use crate::kafka::config::AutoOffsetReset;
use crate::kafka::consumer::StartOffset;
use crate::kafka::consumer::{KafkaConsumerClient, ParallelReader, PartitionRead, ReadEvent};
use crate::kafka::producer::{
    DeliveryOutcome, DeliveryPipeline, KafkaProducerClient, OwnedProducerRecord,
};
use crate::ops::bench::{generate_synthetic_payload, BenchmarkStats, BenchmarkSummary};

/// Command line arguments for Kafka performance benchmarking.
#[derive(Args, Debug, Clone)]
pub struct BenchArgs {
    #[command(subcommand)]
    pub command: BenchSubcommands,
}

/// Supported benchmarking subcommands.
#[derive(Subcommand, Debug, Clone, PartialEq, Eq)]
pub enum BenchSubcommands {
    /// Benchmark producer throughput and delivery latencies
    Produce(BenchProduceArgs),
    /// Benchmark consumer throughput and reading bandwidth
    ///
    /// Throughput is measured from the first record received to the last, so
    /// connection and group setup are excluded. With few records the measured
    /// time is very short; use a large --records for stable numbers.
    Consume(BenchConsumeArgs),
}

/// Options for producer benchmarking.
#[derive(Args, Debug, Clone, PartialEq, Eq)]
pub struct BenchProduceArgs {
    /// Target topic to produce benchmark records to
    pub topic: String,

    /// Total number of records to produce
    #[arg(short = 'n', long, default_value_t = 10_000)]
    pub records: usize,

    /// Size of each record payload in bytes
    #[arg(short = 's', long, default_value_t = 512)]
    pub record_size: usize,

    /// Compression codec to use (none, gzip, snappy, lz4, zstd)
    #[arg(short = 'c', long)]
    pub compression: Option<String>,

    /// Target throughput in records per second (0 = as fast as possible)
    #[arg(long, default_value_t = 0)]
    pub throughput: u64,

    /// Maximum number of unacknowledged records in flight
    #[arg(long, default_value_t = 10_000)]
    pub max_in_flight: usize,
}

/// Options for consumer benchmarking.
#[derive(Args, Debug, Clone, PartialEq, Eq)]
pub struct BenchConsumeArgs {
    /// Target topic to consume benchmark records from
    pub topic: String,

    /// Maximum number of records to consume
    #[arg(short = 'n', long, default_value_t = 10_000)]
    pub records: usize,

    /// Maximum duration to wait for messages in seconds
    #[arg(short = 't', long, default_value_t = 30)]
    pub timeout: u64,
}

/// Runs the performance benchmark suite.
pub async fn run(args: BenchArgs, global: &GlobalOpts) -> anyhow::Result<()> {
    match args.command {
        BenchSubcommands::Produce(produce_args) => handle_produce(global, produce_args).await,
        BenchSubcommands::Consume(consume_args) => handle_consume(global, consume_args).await,
    }
}

async fn handle_produce(global: &GlobalOpts, args: BenchProduceArgs) -> anyhow::Result<()> {
    let mut kafka_config = global.to_kafka_config();
    if let Some(ref comp) = args.compression {
        kafka_config
            .extra_properties
            .insert("compression.codec".to_string(), comp.clone());
    }

    let producer = match KafkaProducerClient::new(&kafka_config) {
        Ok(p) => p,
        Err(err) => {
            print_connection_hint(&global.to_kafka_config(), &err);
            return Err(err.into());
        }
    };

    eprintln!(
        "{} Starting produce benchmark: {} records ({} bytes each) to topic '{}'...",
        "🚀".cyan(),
        args.records.to_string().cyan().bold(),
        args.record_size.to_string().yellow(),
        args.topic.green().bold()
    );

    let payload = generate_synthetic_payload(args.record_size, 42);
    let mut stats = BenchmarkStats::default();
    let mut failures = 0usize;
    let mut pipeline = DeliveryPipeline::new(args.max_in_flight);
    let mut on_outcome = |outcome: DeliveryOutcome| match outcome.result {
        Ok(_) => stats.record(
            outcome.latency.as_secs_f64() * 1000.0,
            outcome.payload_bytes,
        ),
        Err(err) => {
            failures += 1;
            if failures <= 5 {
                eprintln!("{} Delivery failed: {err}", "✖".red());
            }
        }
    };

    let start_total = Instant::now();

    for i in 0..args.records {
        if args.throughput > 0 {
            let due = start_total + Duration::from_secs_f64(i as f64 / args.throughput as f64);
            tokio::time::sleep_until(due.into()).await;
        }

        let record = OwnedProducerRecord::new(&args.topic)
            .with_key(format!("key-{i}").into_bytes())
            .with_payload(payload.clone());

        if let Some(outcome) = pipeline.submit(&producer, &record).await? {
            on_outcome(outcome);
        }
    }

    while let Some(outcome) = pipeline.next_outcome().await {
        on_outcome(outcome);
    }
    stats.elapsed = start_total.elapsed();

    if failures > 0 {
        eprintln!(
            "{} {failures} of {} records failed delivery.",
            "✖".red(),
            args.records
        );
    }

    let summary = stats.summarize();
    output_benchmark_summary(&summary, global.format, "Produce Benchmark Summary")?;

    if failures > 0 {
        anyhow::bail!("{failures} records failed delivery");
    }
    Ok(())
}

async fn handle_consume(global: &GlobalOpts, args: BenchConsumeArgs) -> anyhow::Result<()> {
    let mut kafka_config = global.to_kafka_config();
    kafka_config = kafka_config
        .with_auto_offset_reset(AutoOffsetReset::Earliest)
        .with_auto_commit(false, 5000);

    let consumer = match KafkaConsumerClient::new(&kafka_config) {
        Ok(c) => c,
        Err(err) => {
            print_connection_hint(&global.to_kafka_config(), &err);
            return Err(err.into());
        }
    };

    // Assign partitions directly from the beginning: no consumer group is created and
    // the benchmark does not depend on committed offsets of a previous run.
    let partitions = consumer.topic_partitions(&args.topic, None)?;
    drop(consumer);
    let reads: Vec<PartitionRead> = partitions
        .iter()
        .map(|&partition| PartitionRead {
            topic: &args.topic,
            partition,
            start: StartOffset::Beginning,
            end: None,
        })
        .collect();
    // Records are counted where they are read and never handed over, as a
    // search that matches nothing would read them.
    let bytes = Arc::new(AtomicUsize::new(0));
    let counted = bytes.clone();
    let mut reader = ParallelReader::start(&kafka_config, &reads, move |msg| {
        counted.fetch_add(msg.payload.as_ref().map_or(0, Vec::len), Ordering::Relaxed);
        false
    })?;

    eprintln!(
        "{} Starting consume benchmark: up to {} records from topic '{}'...",
        "🚀".cyan(),
        args.records.to_string().cyan().bold(),
        args.topic.green().bold()
    );

    let mut stats = BenchmarkStats::default();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(args.timeout);
    // Throughput is measured from the first report of read records, so
    // connection and partition assignment time do not distort the result.
    // Reports come every 4096 records or 100 ms.
    let mut first_record_at: Option<Instant> = None;
    let mut last_record_at = Instant::now();
    let ctrl_c = tokio::signal::ctrl_c();
    tokio::pin!(ctrl_c);

    while stats.total_records < args.records {
        tokio::select! {
            event = reader.recv() => {
                match event {
                    Some(ReadEvent::Skipped(count)) => {
                        let now = Instant::now();
                        first_record_at.get_or_insert(now);
                        last_record_at = now;
                        stats.total_records += count as usize;
                    }
                    Some(ReadEvent::Record(_)) => {}
                    Some(ReadEvent::Error(err)) => {
                        eprintln!("{} Consumer error: {err}", "✖".red());
                        break;
                    }
                    None => break,
                }
            }
            _ = tokio::time::sleep_until(deadline) => {
                eprintln!(
                    "{}",
                    format!("Benchmark timeout of {}s reached.", args.timeout).bright_black()
                );
                break;
            }
            _ = &mut ctrl_c => {
                eprintln!("\n{}", "Benchmark interrupted by user (Ctrl+C).".yellow());
                break;
            }
        }
    }

    stats.total_bytes = bytes.load(Ordering::Relaxed);
    stats.elapsed = first_record_at
        .map(|first| last_record_at.duration_since(first))
        .unwrap_or_default();

    let summary = stats.summarize();
    output_benchmark_summary(&summary, global.format, "Consume Benchmark Summary")?;

    Ok(())
}

fn output_benchmark_summary(
    summary: &BenchmarkSummary,
    format: OutputFormat,
    title: &str,
) -> anyhow::Result<()> {
    if format == OutputFormat::Json {
        output::print_json(summary)?;
        return Ok(());
    }

    let mut table = output::table(vec![
        Cell::new(title).fg(Color::Cyan),
        Cell::new("RESULT").fg(Color::Cyan),
    ]);

    table.add_row(vec![
        Cell::new("Total Records Processed"),
        Cell::new(summary.total_records.to_string()).fg(Color::Green),
    ]);

    table.add_row(vec![
        Cell::new("Total Data Transferred"),
        Cell::new(format!(
            "{:.2} MB",
            crate::model::format::mib(summary.total_bytes as u64)
        ))
        .fg(Color::Cyan),
    ]);

    table.add_row(vec![
        Cell::new("Elapsed Time"),
        Cell::new(format!("{:.3} s", summary.elapsed_secs)).fg(Color::White),
    ]);

    table.add_row(vec![
        Cell::new("Throughput"),
        Cell::new(format!(
            "{:.2} records/s ({:.2} MB/s)",
            summary.records_per_sec, summary.mb_per_sec
        ))
        .fg(Color::Yellow),
    ]);

    if summary.latency_samples > 0 {
        table.add_row(vec![
            Cell::new("Avg Delivery Latency"),
            Cell::new(format!("{:.2} ms", summary.avg_latency_ms)).fg(Color::White),
        ]);

        table.add_row(vec![
            Cell::new("p50 Latency"),
            Cell::new(format!("{:.2} ms", summary.p50_latency_ms)).fg(Color::Green),
        ]);

        table.add_row(vec![
            Cell::new("p95 Latency"),
            Cell::new(format!("{:.2} ms", summary.p95_latency_ms)).fg(Color::Yellow),
        ]);

        table.add_row(vec![
            Cell::new("p99 Latency"),
            Cell::new(format!("{:.2} ms", summary.p99_latency_ms)).fg(Color::Magenta),
        ]);

        table.add_row(vec![
            Cell::new("Max Latency"),
            Cell::new(format!("{:.2} ms", summary.max_latency_ms)).fg(Color::Red),
        ]);
    }

    println!("{table}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_clap_bench_args_produce_parsing() {
        use clap::Parser;

        #[derive(Parser, Debug)]
        struct TestCli {
            #[command(subcommand)]
            cmd: BenchSubcommands,
        }

        let parsed = TestCli::parse_from([
            "bench",
            "produce",
            "perf-topic",
            "-n",
            "50000",
            "-s",
            "1024",
            "-c",
            "lz4",
        ]);

        match parsed.cmd {
            BenchSubcommands::Produce(args) => {
                assert_eq!(args.topic, "perf-topic");
                assert_eq!(args.records, 50_000);
                assert_eq!(args.record_size, 1024);
                assert_eq!(args.compression.as_deref(), Some("lz4"));
            }
            _ => panic!("Expected Produce subcommand"),
        }
    }

    #[test]
    fn test_clap_bench_args_consume_parsing() {
        use clap::Parser;

        #[derive(Parser, Debug)]
        struct TestCli {
            #[command(subcommand)]
            cmd: BenchSubcommands,
        }

        let parsed =
            TestCli::parse_from(["bench", "consume", "perf-topic", "-n", "25000", "-t", "60"]);

        match parsed.cmd {
            BenchSubcommands::Consume(args) => {
                assert_eq!(args.topic, "perf-topic");
                assert_eq!(args.records, 25_000);
                assert_eq!(args.timeout, 60);
            }
            _ => panic!("Expected Consume subcommand"),
        }
    }
}
