use std::time::Duration;

use clap::Args;
use comfy_table::{Cell, CellAlignment, Color};
use rdkafka::Offset;

use crate::cli::{output, GlobalOpts};
use crate::error::{KafkaError, KafkaResult};
use crate::kafka::consumer::KafkaConsumerClient;
use crate::kafka::producer::{KafkaProducerClient, OwnedProducerRecord};
use crate::ops::probe::{ProbePayload, ProbeSample, ProbeStats};

#[derive(Debug, Args)]
#[command(
    name = "probe",
    about = "Kafka End-to-End Latency & Heartbeat Sentinel: measure true produce-to-consume RTT and SLA jitter"
)]
pub struct ProbeArgs {
    /// Topic to use for canary heartbeat probes
    #[arg(value_name = "TOPIC", default_value = "__ferrika_heartbeat")]
    pub topic: String,

    /// Number of canary probe pings to transmit
    #[arg(short = 'c', long, default_value = "5")]
    pub count: usize,

    /// Interval between probes in milliseconds
    #[arg(short = 'i', long, default_value = "500")]
    pub interval_ms: u64,

    /// Timeout waiting for probe echo in milliseconds
    #[arg(long, default_value = "3000")]
    pub timeout_ms: u64,

    /// Maximum permissible P99 latency threshold in milliseconds (fails if exceeded)
    #[arg(long)]
    pub max_latency_ms: Option<f64>,

    /// Output results in JSON format
    #[arg(long)]
    pub json: bool,
}

pub async fn run(args: ProbeArgs, global: &GlobalOpts) -> KafkaResult<()> {
    let probe_id = format!("ferrika-probe-{}", std::process::id());

    eprintln!("\n📡 Ferrika Latency & Heartbeat Sentinel");
    eprintln!("  ├─ Target Probe Topic: {}", args.topic);
    eprintln!("  ├─ Probes Count:       {}", args.count);
    eprintln!("  └─ Interval:           {}ms", args.interval_ms);
    eprintln!("{}", "━".repeat(70));

    let samples = execute_probes(&args, global, &probe_id).await?;
    let stats = ProbeStats::compute(&samples, args.count);

    if global.wants_json(args.json) {
        output::print_json(&serde_json::json!({
            "topic": args.topic,
            "probe_id": probe_id,
            "stats": stats,
            "samples": samples,
        }))?;
        return enforce_latency_threshold(&args, &stats);
    }

    println!("\n📊 End-to-End Latency Distribution & Quality of Service:");
    let mut table = output::table(vec![
        Cell::new("Telemetry Metric").fg(Color::Cyan),
        Cell::new("Observed Value")
            .fg(Color::Cyan)
            .set_alignment(CellAlignment::Right),
        Cell::new("Benchmark Reference").fg(Color::Cyan),
    ]);

    table.add_row(vec![
        Cell::new("Transmitted Probes"),
        Cell::new(stats.sent.to_string()).set_alignment(CellAlignment::Right),
        Cell::new("Total canaries sent"),
    ]);

    table.add_row(vec![
        Cell::new("Received Echoes"),
        Cell::new(stats.received.to_string())
            .set_alignment(CellAlignment::Right)
            .fg(Color::Green),
        Cell::new("Successfully consumed"),
    ]);

    table.add_row(vec![
        Cell::new("Packet Loss %"),
        Cell::new(format!("{:.1}%", stats.loss_pct))
            .set_alignment(CellAlignment::Right)
            .fg(if stats.loss_pct > 0.0 {
                Color::Red
            } else {
                Color::Green
            }),
        Cell::new(if stats.loss_pct == 0.0 {
            "✓ 0% Loss"
        } else {
            "⚠️ Packet Loss Detected"
        }),
    ]);

    table.add_row(vec![
        Cell::new("Min Round-Trip Time"),
        Cell::new(format!("{:.2} ms", stats.min_ms))
            .set_alignment(CellAlignment::Right)
            .fg(Color::Green),
        Cell::new("Fastest flight time"),
    ]);

    table.add_row(vec![
        Cell::new("Mean Round-Trip Time"),
        Cell::new(format!("{:.2} ms", stats.mean_ms))
            .set_alignment(CellAlignment::Right)
            .fg(Color::Cyan),
        Cell::new("Arithmetic average"),
    ]);

    table.add_row(vec![
        Cell::new("P50 (Median) RTT"),
        Cell::new(format!("{:.2} ms", stats.p50_ms)).set_alignment(CellAlignment::Right),
        Cell::new("50th percentile"),
    ]);

    table.add_row(vec![
        Cell::new("P95 RTT"),
        Cell::new(format!("{:.2} ms", stats.p95_ms))
            .set_alignment(CellAlignment::Right)
            .fg(Color::Yellow),
        Cell::new("95th percentile SLA"),
    ]);

    let p99_color = if let Some(max_lat) = args.max_latency_ms {
        if stats.p99_ms > max_lat {
            Color::Red
        } else {
            Color::Green
        }
    } else {
        Color::Yellow
    };

    table.add_row(vec![
        Cell::new("P99 RTT"),
        Cell::new(format!("{:.2} ms", stats.p99_ms))
            .set_alignment(CellAlignment::Right)
            .fg(p99_color),
        Cell::new("99th percentile SLA"),
    ]);

    table.add_row(vec![
        Cell::new("Jitter (Latency Variance)"),
        Cell::new(format!("±{:.2} ms", stats.jitter_ms)).set_alignment(CellAlignment::Right),
        Cell::new("Mean consecutive step variance"),
    ]);

    println!("{table}\n");

    if let Some(threshold) = args.max_latency_ms {
        if let Some(reason) = probe_failure(args.max_latency_ms, &stats) {
            eprintln!(
                "❌ {reason} (P99 {:.2}ms, limit {threshold:.2}ms)\n",
                stats.p99_ms
            );
        } else {
            println!(
                "✅ SLA SATISFIED: P99 latency ({:.2}ms) within SLA limit ({:.2}ms).\n",
                stats.p99_ms, threshold
            );
        }
    }

    enforce_latency_threshold(&args, &stats)
}

fn enforce_latency_threshold(args: &ProbeArgs, stats: &ProbeStats) -> KafkaResult<()> {
    match probe_failure(args.max_latency_ms, stats) {
        Some(reason) => Err(KafkaError::Check(reason)),
        None => Ok(()),
    }
}

/// Why the probe run fails, if it does. Without `--max-latency-ms` only a run
/// that received nothing fails (its p99 of 0 would otherwise look healthy).
/// With the flag, any lost probe fails too, since the p99 covers only the
/// echoes that came back.
fn probe_failure(max_latency_ms: Option<f64>, stats: &ProbeStats) -> Option<String> {
    if stats.sent > 0 && stats.received == 0 {
        return Some(format!(
            "probe failed: none of the {} probe(s) came back",
            stats.sent
        ));
    }
    let threshold = max_latency_ms?;
    if stats.lost > 0 {
        return Some(format!(
            "probe SLA violation: {} of {} probe(s) lost ({:.1}%)",
            stats.lost, stats.sent, stats.loss_pct
        ));
    }
    if is_latency_threshold_exceeded(max_latency_ms, stats.p99_ms) {
        return Some(format!(
            "probe SLA violation: p99 latency {:.2}ms exceeds configured limit {:.2}ms",
            stats.p99_ms, threshold
        ));
    }
    None
}

fn is_latency_threshold_exceeded(max_latency_ms: Option<f64>, p99_ms: f64) -> bool {
    max_latency_ms.is_some_and(|threshold| p99_ms > threshold)
}

/// Executes probe transmissions and collects round-trip samples
async fn execute_probes(
    args: &ProbeArgs,
    global: &GlobalOpts,
    probe_id: &str,
) -> KafkaResult<Vec<ProbeSample>> {
    let config = global.to_kafka_config();
    let producer = KafkaProducerClient::new(&config)?;
    // Read from the current end of each partition without joining a group:
    // no rebalance delay before the first probe, and no group left behind.
    // The end is resolved up front so the first probe cannot slip past it.
    let consumer = KafkaConsumerClient::scanner(&config)?;
    let partitions = consumer.topic_partitions(&args.topic, None)?;
    let keys: Vec<(&str, i32)> = partitions
        .iter()
        .map(|&p| (args.topic.as_str(), p))
        .collect();
    let ends = consumer.fetch_watermarks_batch(&keys, None)?;
    let assignments: Vec<(&str, i32, Offset)> = partitions
        .iter()
        .map(|&p| {
            let start = ends
                .get(&(args.topic.clone(), p))
                .map_or(Offset::End, |&(_, high)| Offset::Offset(high));
            (args.topic.as_str(), p, start)
        })
        .collect();
    consumer.assign_with_offsets(&assignments)?;
    let mut samples = Vec::new();

    for seq in 1..=args.count {
        let probe = ProbePayload::new(probe_id, seq as u64);
        let payload_bytes = match probe.to_bytes() {
            Ok(b) => b,
            Err(_) => continue,
        };

        let record = OwnedProducerRecord::new(&args.topic)
            .with_key(format!("{probe_id}-{seq}"))
            .with_payload(payload_bytes)
            .with_header("x-probe-seq", seq.to_string().into_bytes());

        let send_res = producer
            .send(&record, Some(Duration::from_millis(args.timeout_ms)))
            .await;
        if send_res.is_err() {
            eprintln!("  probe seq={seq} -> send failed (loss)");
            continue;
        }

        // Await matching consumer echo
        let deadline = Duration::from_millis(args.timeout_ms);
        let start_recv = tokio::time::Instant::now();
        let mut matched = false;

        while start_recv.elapsed() < deadline {
            if let Ok(Ok(msg)) =
                tokio::time::timeout(Duration::from_millis(200), consumer.recv()).await
            {
                if let Some(payload) = &msg.payload {
                    if let Ok(received_probe) = ProbePayload::from_bytes(payload) {
                        if received_probe.probe_id == probe_id && received_probe.seq == seq as u64 {
                            let rtt = received_probe.calculate_rtt_ms();
                            eprintln!(
                                        "  probe seq={:<2} partition={:<2} offset={:<8} rtt={:.2}ms  status=OK",
                                        seq, msg.partition, msg.offset, rtt
                                    );
                            samples.push(ProbeSample {
                                seq: seq as u64,
                                rtt_ms: rtt,
                                partition: msg.partition,
                                offset: msg.offset,
                            });
                            matched = true;
                            break;
                        }
                    }
                }
            }
        }

        if !matched {
            eprintln!("  probe seq={seq} -> timeout waiting for echo (loss)");
        }

        tokio::time::sleep(Duration::from_millis(args.interval_ms)).await;
    }

    Ok(samples)
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser)]
    struct Wrapper {
        #[command(subcommand)]
        cmd: Subcommands,
    }

    #[derive(clap::Subcommand)]
    enum Subcommands {
        Probe(ProbeArgs),
    }

    #[test]
    fn test_clap_probe_args_parsing() {
        let parsed = Wrapper::parse_from([
            "ferrika",
            "probe",
            "heartbeat-v1",
            "-c",
            "10",
            "-i",
            "200",
            "--max-latency-ms",
            "15.5",
            "--json",
        ]);

        match parsed.cmd {
            Subcommands::Probe(args) => {
                assert_eq!(args.topic, "heartbeat-v1");
                assert_eq!(args.count, 10);
                assert_eq!(args.interval_ms, 200);
                assert_eq!(args.max_latency_ms, Some(15.5));
                assert!(args.json);
            }
        }
    }

    #[test]
    fn total_loss_always_fails_and_loss_fails_with_a_threshold() {
        let sample = |rtt_ms| ProbeSample {
            seq: 1,
            rtt_ms,
            partition: 0,
            offset: 0,
        };
        let none = ProbeStats::compute(&[], 5);
        assert!(probe_failure(None, &none).is_some());
        assert!(probe_failure(Some(100.0), &none).is_some());

        let some = ProbeStats::compute(&[sample(1.0), sample(2.0)], 5);
        assert!(probe_failure(None, &some).is_none());
        assert!(probe_failure(Some(100.0), &some).is_some());

        let all = ProbeStats::compute(&[sample(1.0), sample(2.0)], 2);
        assert!(probe_failure(Some(100.0), &all).is_none());
        assert!(probe_failure(Some(1.5), &all).is_some());
    }

    #[test]
    fn test_latency_threshold_is_strictly_enforced() {
        assert!(!is_latency_threshold_exceeded(None, 100.0));
        assert!(!is_latency_threshold_exceeded(Some(10.0), 10.0));
        assert!(is_latency_threshold_exceeded(Some(10.0), 10.01));
    }
}
