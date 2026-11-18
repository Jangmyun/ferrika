use std::io::Write;
use std::str::FromStr;
use std::time::{Duration, Instant};

use clap::Args;
use colored::Colorize;
use comfy_table::{Cell, Color};
use serde_json::json;

use crate::cli::{output, print_connection_hint, GlobalOpts, OutputFormat};
use crate::error::KafkaError;
use crate::error::KafkaResult;
use crate::kafka::producer::{
    DeliveryOutcome, DeliveryPipeline, KafkaProducerClient, OwnedProducerRecord,
};
use crate::ops::generator::{EventGenerator, EventTemplate};

/// Unacknowledged events allowed while generating.
const GENERATE_MAX_IN_FLIGHT: usize = 1000;

/// Delivery results of a generate run.
#[derive(Default)]
struct DeliveryTally {
    sent: u64,
    bytes: usize,
    failed: u64,
}

impl DeliveryTally {
    fn record(&mut self, outcome: DeliveryOutcome) {
        match outcome.result {
            Ok(_) => {
                self.sent += 1;
                self.bytes += outcome.payload_bytes;
            }
            Err(e) => self.record_error(&e),
        }
    }

    fn record_error(&mut self, error: &KafkaError) {
        self.failed += 1;
        // Report the first few; a broken broker would otherwise flood the terminal.
        if self.failed <= 5 {
            eprintln!("{} Failed to produce event: {error}", "✖".red());
        }
    }

    /// Error to exit with when any delivery failed, so scripts see a non-zero status.
    fn failure(&self) -> Option<KafkaError> {
        (self.failed > 0).then(|| {
            KafkaError::Check(format!(
                "{} of {} events failed delivery",
                self.failed,
                self.sent + self.failed
            ))
        })
    }
}

/// Command line arguments for generating synthetic mock events.
#[derive(Args, Debug, Clone, PartialEq)]
pub struct GenerateArgs {
    /// Topic name to publish generated events to
    pub topic: String,

    /// Built-in template: ecommerce, iot, web, financial, or custom
    #[arg(short = 'T', long, default_value = "ecommerce")]
    pub template: String,

    /// Custom JSON template with tokens (e.g. '{"id":"{{uuid}}","count":{{index}}}')
    #[arg(long)]
    pub custom: Option<String>,

    /// Events per second rate limit (e.g. 10, 50, 100)
    #[arg(short = 'r', long, default_value_t = 10.0, value_parser = parse_rate)]
    pub rate: f64,

    /// Total number of messages to produce before exiting (omit for continuous stream)
    #[arg(short = 'n', long)]
    pub count: Option<u64>,

    /// Optional Confluent Schema ID to prepend in wire-format header
    #[arg(long)]
    pub schema_id: Option<u32>,

    /// Partition key prefix (e.g. "order-")
    #[arg(short = 'k', long)]
    pub key_prefix: Option<String>,

    /// Random seed for reproducible generation sequences
    #[arg(long)]
    pub seed: Option<u64>,

    /// Print each generated message to stdout
    #[arg(short = 'v', long)]
    pub verbose: bool,
}

/// Main entry point for `ferrika generate`.
/// `--rate` must be a finite number above 0 (records per second).
fn parse_rate(raw: &str) -> Result<f64, String> {
    match raw.trim().parse::<f64>() {
        Ok(r) if r.is_finite() && r > 0.0 => Ok(r),
        Ok(_) => Err("must be a number greater than 0 (records per second)".to_string()),
        Err(e) => Err(format!("invalid number '{raw}': {e}")),
    }
}

pub async fn run(args: GenerateArgs, global: &GlobalOpts) -> KafkaResult<()> {
    let kafka_config = global.to_kafka_config();
    let producer = match KafkaProducerClient::new(&kafka_config) {
        Ok(p) => p,
        Err(err) => {
            print_connection_hint(&global.to_kafka_config(), &err);
            return Err(err);
        }
    };

    let template = if let Some(ref custom_str) = args.custom {
        EventTemplate::Custom(custom_str.clone())
    } else {
        EventTemplate::from_str(&args.template).map_err(KafkaError::config)?
    };

    let mut generator = EventGenerator::new(template);
    if let Some(seed) = args.seed {
        generator = generator.with_seed(seed);
    }
    if let Some(id) = args.schema_id {
        generator = generator.with_schema_id(id);
    }
    if let Some(ref prefix) = args.key_prefix {
        generator = generator.with_key_prefix(prefix);
    }

    if global.format != OutputFormat::Json {
        println!(
            "{} Starting synthetic event generator for topic '{}'",
            "⚡".yellow().bold(),
            args.topic.cyan().bold()
        );
        println!("  Template:     {}", args.template.green());
        println!(
            "  Target Rate:  {} events/sec",
            args.rate.to_string().yellow().bold()
        );
        if let Some(max) = args.count {
            println!("  Total Count:  {} events", max.to_string().yellow());
        } else {
            println!(
                "  Total Count:  {} (Press Ctrl+C to stop)",
                "Continuous".yellow()
            );
        }
        if let Some(id) = args.schema_id {
            println!(
                "  Wire Format:  Schema ID {}",
                id.to_string().magenta().bold()
            );
        }
        println!();
    }

    let interval = if args.rate > 0.0 {
        Duration::from_secs_f64(1.0 / args.rate)
    } else {
        Duration::from_millis(100)
    };

    let start_time = Instant::now();
    let mut generated: u64 = 0;
    let mut tally = DeliveryTally::default();
    let mut last_report = Instant::now();
    // Deliveries are pipelined: waiting for each acknowledgement before the
    // next send capped the rate at one round trip (about 20 events/s).
    let mut pipeline = DeliveryPipeline::new(GENERATE_MAX_IN_FLIGHT);
    let ctrl_c = tokio::signal::ctrl_c();
    tokio::pin!(ctrl_c);

    loop {
        if args.count.is_some_and(|max| generated >= max) {
            break;
        }
        // Absolute schedule, so time spent producing does not lower the rate.
        let due = tokio::time::Instant::from_std(start_time + interval.mul_f64(generated as f64));

        tokio::select! {
            biased;
            _ = &mut ctrl_c => {
                if global.format != OutputFormat::Json {
                    eprintln!("\n{}", "Generation interrupted by user (Ctrl+C).".yellow());
                }
                break;
            }
            _ = tokio::time::sleep_until(due) => {
                let (key, payload) = generator.generate_next();
                if args.verbose && global.format != OutputFormat::Json {
                    let preview = match std::str::from_utf8(&payload) {
                        Ok(s) if s.chars().count() > 60 => {
                            format!("{}...", s.chars().take(60).collect::<String>())
                        }
                        Ok(s) => s.to_string(),
                        Err(_) => format!("<binary {} bytes>", payload.len()),
                    };
                    println!(
                        "  {} key={} payload={}",
                        "➜".green(),
                        key.as_deref().unwrap_or("<no key>").yellow(),
                        preview.bright_black()
                    );
                }

                let mut record = OwnedProducerRecord::new(&args.topic).with_payload(payload);
                if let Some(ref k) = key {
                    record = record.with_key(k.as_bytes().to_vec());
                }
                generated += 1;
                match pipeline.submit(&producer, &record).await {
                    Ok(Some(outcome)) => tally.record(outcome),
                    Ok(None) => {}
                    Err(e) => tally.record_error(&e),
                }

                if !args.verbose
                    && global.format != OutputFormat::Json
                    && last_report.elapsed() >= Duration::from_secs(1)
                {
                    let elapsed = start_time.elapsed().as_secs_f64();
                    let current_eps = if elapsed > 0.0 { tally.sent as f64 / elapsed } else { 0.0 };
                    eprint!(
                        "\r{} Generated {} events ({:.1} EPS) | Elapsed: {:.1}s    ",
                        "⚡".yellow(),
                        tally.sent.to_string().cyan().bold(),
                        current_eps,
                        elapsed
                    );
                    let _ = std::io::stderr().flush();
                    last_report = Instant::now();
                }
            }
        }
    }

    while let Some(outcome) = pipeline.next_outcome().await {
        tally.record(outcome);
    }
    let total_sent = tally.sent;
    let total_bytes = tally.bytes;
    let total_failed = tally.failed;

    let elapsed = start_time.elapsed().as_secs_f64();
    let avg_eps = if elapsed > 0.0 {
        total_sent as f64 / elapsed
    } else {
        0.0
    };

    match global.format {
        OutputFormat::Json => {
            output::print_json(&json!({
                "topic": args.topic,
                "template": args.template,
                "total_events": total_sent,
                "failed_events": total_failed,
                "total_bytes": total_bytes,
                "elapsed_seconds": elapsed,
                "average_eps": avg_eps,
            }))?;
        }
        OutputFormat::Table => {
            println!("\n");
            let mut table = output::table(vec![
                Cell::new("METRIC").fg(Color::Cyan),
                Cell::new("VALUE").fg(Color::Cyan),
            ]);

            table.add_row(vec![
                Cell::new("Target Topic").fg(Color::DarkGrey),
                Cell::new(&args.topic).fg(Color::Green),
            ]);
            table.add_row(vec![
                Cell::new("Template").fg(Color::DarkGrey),
                Cell::new(&args.template).fg(Color::Yellow),
            ]);
            table.add_row(vec![
                Cell::new("Total Events Produced").fg(Color::DarkGrey),
                Cell::new(total_sent.to_string()).fg(Color::Cyan),
            ]);
            if total_failed > 0 {
                table.add_row(vec![
                    Cell::new("Failed Deliveries").fg(Color::DarkGrey),
                    Cell::new(total_failed.to_string()).fg(Color::Red),
                ]);
            }
            table.add_row(vec![
                Cell::new("Total Payload Data").fg(Color::DarkGrey),
                Cell::new(format!(
                    "{:.2} KB",
                    crate::model::format::kib(total_bytes as u64)
                ))
                .fg(Color::White),
            ]);
            table.add_row(vec![
                Cell::new("Elapsed Time").fg(Color::DarkGrey),
                Cell::new(format!("{:.2} s", elapsed)).fg(Color::Blue),
            ]);
            table.add_row(vec![
                Cell::new("Average Throughput").fg(Color::DarkGrey),
                Cell::new(format!("{:.1} events/sec", avg_eps)).fg(Color::Green),
            ]);

            println!("{}", table);
        }
    }

    match tally.failure() {
        Some(err) => Err(err),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::producer::DeliveryReceipt;

    fn outcome(result: KafkaResult<DeliveryReceipt>) -> DeliveryOutcome {
        DeliveryOutcome {
            result,
            latency: Duration::ZERO,
            payload_bytes: 10,
        }
    }

    #[test]
    fn test_tally_reports_failures_as_error() {
        let mut tally = DeliveryTally::default();
        tally.record(outcome(Ok(DeliveryReceipt {
            topic: "t".into(),
            partition: 0,
            offset: 1,
        })));
        assert!(tally.failure().is_none());

        tally.record(outcome(Err(KafkaError::DeliveryFailed {
            topic: "t".into(),
            partition: None,
            reason: "boom".into(),
        })));
        assert_eq!((tally.sent, tally.failed, tally.bytes), (1, 1, 10));
        let err = tally.failure().map(|e| e.to_string()).unwrap_or_default();
        assert_eq!(err, "1 of 2 events failed delivery");
    }

    #[test]
    fn test_clap_generate_rate_must_be_positive() {
        use clap::Parser;

        #[derive(Parser, Debug)]
        struct TestCli {
            #[command(flatten)]
            args: GenerateArgs,
        }

        for bad in ["0", "0.0", "-1", "nan", "inf", "x"] {
            assert!(
                TestCli::try_parse_from(["test", "orders", "-r", bad]).is_err(),
                "-r {bad} must be rejected"
            );
        }
        assert!(TestCli::try_parse_from(["test", "orders", "-r", "0.5"]).is_ok());
    }

    #[test]
    fn test_clap_generate_args_defaults() {
        use clap::Parser;

        #[derive(Parser, Debug)]
        struct TestCli {
            #[command(flatten)]
            args: GenerateArgs,
        }

        let cli = TestCli::try_parse_from(["test", "orders"]).unwrap();
        assert_eq!(cli.args.topic, "orders");
        assert_eq!(cli.args.template, "ecommerce");
        assert_eq!(cli.args.rate, 10.0);
        assert_eq!(cli.args.count, None);
        assert_eq!(cli.args.schema_id, None);
    }

    #[test]
    fn test_clap_generate_args_custom() {
        use clap::Parser;

        #[derive(Parser, Debug)]
        struct TestCli {
            #[command(flatten)]
            args: GenerateArgs,
        }

        let cli = TestCli::try_parse_from([
            "test",
            "iot-telemetry",
            "-T",
            "iot",
            "-r",
            "50",
            "-n",
            "100",
            "--schema-id",
            "42",
            "-k",
            "dev-",
            "-v",
        ])
        .unwrap();

        assert_eq!(cli.args.topic, "iot-telemetry");
        assert_eq!(cli.args.template, "iot");
        assert_eq!(cli.args.rate, 50.0);
        assert_eq!(cli.args.count, Some(100));
        assert_eq!(cli.args.schema_id, Some(42));
        assert_eq!(cli.args.key_prefix.as_deref(), Some("dev-"));
        assert!(cli.args.verbose);
    }
}
