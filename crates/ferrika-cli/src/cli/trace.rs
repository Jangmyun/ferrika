use clap::{Args, Subcommand};
use comfy_table::{Cell, CellAlignment, Color};

use crate::cli::{output, GlobalOpts};
use crate::error::{KafkaError, KafkaResult};
use crate::kafka::bounded_read::{read_topic, BoundedReadOptions};
use crate::ops::trace::{ExtractedTrace, TraceAuditSummary, TraceStandard, W3cTraceparent};

#[derive(Debug, Args)]
#[command(
    name = "trace",
    about = "Distributed Tracing & OpenTelemetry Analyzer: inspect W3C/B3 trace propagation and detect untraced traffic"
)]
pub struct TraceArgs {
    #[command(subcommand)]
    pub command: TraceSubcommands,
}

#[derive(Debug, Subcommand)]
pub enum TraceSubcommands {
    /// Inspect messages in a topic and extract distributed trace context (W3C, B3, Jaeger)
    Inspect(InspectTraceArgs),

    /// Audit trace coverage compliance and report percentage of untraced dark traffic
    Audit(AuditTraceArgs),

    /// Generate new W3C TraceContext headers for injection into test payloads
    Inject(InjectTraceArgs),
}

#[derive(Debug, Args)]
pub struct InspectTraceArgs {
    /// Topic name to inspect for trace context
    #[arg(value_name = "TOPIC")]
    pub topic: String,

    /// Specific partition to inspect (default: all partitions)
    #[arg(short, long)]
    pub partition: Option<i32>,

    /// Maximum number of messages to sample
    #[arg(short = 'n', long, default_value = "50")]
    pub limit: usize,

    /// Filter by tracing standard: 'w3c', 'b3', 'jaeger', or 'untraced'
    #[arg(long)]
    pub standard: Option<String>,

    /// Output inspection records in JSON format
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct AuditTraceArgs {
    /// Topic name to audit
    #[arg(value_name = "TOPIC")]
    pub topic: String,

    /// Number of messages to evaluate
    #[arg(short = 'n', long, default_value = "200")]
    pub limit: usize,

    /// Minimum expected trace coverage percentage (e.g. 95.0)
    #[arg(long, default_value = "90.0")]
    pub min_coverage: f64,

    /// Output audit metrics in JSON format
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct InjectTraceArgs {
    /// Whether the generated trace has the sampled flag set (01);
    /// `--sampled false` generates unsampled ones (00)
    #[arg(long, default_value_t = true, num_args = 0..=1, default_missing_value = "true",
          action = clap::ArgAction::Set)]
    pub sampled: bool,

    /// Number of fresh traceparent headers to generate
    #[arg(short = 'n', long, default_value = "1")]
    pub count: usize,

    /// Output in JSON format
    #[arg(long)]
    pub json: bool,
}

pub async fn run(args: TraceArgs, global: &GlobalOpts) -> KafkaResult<()> {
    match args.command {
        TraceSubcommands::Inspect(inspect_args) => run_inspect(inspect_args, global).await,
        TraceSubcommands::Audit(audit_args) => run_audit(audit_args, global).await,
        TraceSubcommands::Inject(inject_args) => run_inject(inject_args, global),
    }
}

/// Fetches trace contexts from up to `limit` messages of `topic` (or of one
/// `partition`), read from the start up to the high watermarks captured
/// before reading. Warns on stderr when the read is partial.
async fn fetch_traces(
    topic: &str,
    partition: Option<i32>,
    limit: usize,
    global: &GlobalOpts,
) -> KafkaResult<Vec<(i32, i64, ExtractedTrace)>> {
    let config = global.to_kafka_config();
    let opts = BoundedReadOptions::with_max_records(limit);
    let read = read_topic(&config, topic, partition, &opts, |_| true).await?;
    if let Some(warning) = read.partial_warning(topic, false) {
        eprintln!("⚠ {warning}");
    }
    Ok(read
        .records
        .iter()
        .map(|msg| {
            let extracted = ExtractedTrace::from_headers(&msg.headers);
            (msg.partition, msg.offset, extracted)
        })
        .collect())
}

/// Generates representative synthetic traces for offline testing or demonstration
async fn run_inspect(args: InspectTraceArgs, global: &GlobalOpts) -> KafkaResult<()> {
    let mut records = fetch_traces(&args.topic, args.partition, args.limit, global).await?;

    if let Some(ref filter) = args.standard {
        let filter_lower = filter.to_lowercase();
        records.retain(|(_, _, trace)| match trace.standard {
            TraceStandard::W3c => filter_lower.contains("w3c"),
            TraceStandard::B3Multi | TraceStandard::B3Single => filter_lower.contains("b3"),
            TraceStandard::Jaeger => filter_lower.contains("jaeger"),
            TraceStandard::Untraced => {
                filter_lower.contains("untraced") || filter_lower.contains("dark")
            }
        });
    }

    let policy = crate::governance::policy::current();
    crate::cli::note_masking(policy);
    let records = shown(records, policy);

    if global.wants_json(args.json) {
        output::print_json(&records)?;
        return Ok(());
    }

    println!("\n🔍 Ferrika Distributed Trace Inspector: {}", args.topic);
    println!("{}", "━".repeat(70));

    let mut table = output::table(vec![
        Cell::new("Kafka [P:O]").fg(Color::Cyan),
        Cell::new("Standard").fg(Color::Cyan),
        Cell::new("Trace ID").fg(Color::Cyan),
        Cell::new("Span / Parent ID").fg(Color::Cyan),
        Cell::new("Sampled").fg(Color::Cyan),
    ]);

    for (p, o, trace) in records {
        let std_cell = match trace.standard {
            TraceStandard::W3c => Cell::new("W3C").fg(Color::Green),
            TraceStandard::B3Multi | TraceStandard::B3Single => Cell::new("B3").fg(Color::Blue),
            TraceStandard::Jaeger => Cell::new("Jaeger").fg(Color::Yellow),
            TraceStandard::Untraced => Cell::new("Untraced").fg(Color::Red),
        };

        let trace_id_str = trace.trace_id.unwrap_or_else(|| "-".to_string());
        let span_id_str = trace.span_id.unwrap_or_else(|| "-".to_string());

        let sampled_cell = if trace.sampled {
            Cell::new("✓ Yes").fg(Color::Green)
        } else if trace.standard == TraceStandard::Untraced {
            Cell::new("-").fg(Color::DarkGrey)
        } else {
            Cell::new("✗ No").fg(Color::Yellow)
        };

        table.add_row(vec![
            Cell::new(format!("{p}:{o}")),
            std_cell,
            Cell::new(trace_id_str),
            Cell::new(span_id_str),
            sampled_cell,
        ]);
    }

    println!("{table}\n");

    Ok(())
}

/// The traces as `inspect` may show them: masked while the company policy
/// enforces masking, unchanged otherwise.
fn shown(
    records: Vec<(i32, i64, ExtractedTrace)>,
    policy: &crate::governance::policy::EffectivePolicy,
) -> Vec<(i32, i64, ExtractedTrace)> {
    if policy.masking_enforced() {
        records
            .into_iter()
            .map(|(p, o, trace)| (p, o, trace.masked()))
            .collect()
    } else {
        records
    }
}

async fn run_audit(args: AuditTraceArgs, global: &GlobalOpts) -> KafkaResult<()> {
    let records = fetch_traces(&args.topic, None, args.limit, global).await?;
    let traces: Vec<ExtractedTrace> = records.into_iter().map(|(_, _, t)| t).collect();
    let summary = TraceAuditSummary::audit(&traces);

    let passed = audit_passed(summary.coverage_pct, args.min_coverage);

    if global.wants_json(args.json) {
        let mut json = serde_json::to_value(&summary).map_err(|e| {
            KafkaError::Serialization(format!("Failed to serialize trace audit: {e}"))
        })?;
        if let Some(obj) = json.as_object_mut() {
            obj.insert("min_coverage".into(), args.min_coverage.into());
            obj.insert("passed".into(), passed.into());
        }
        output::print_json(&json)?;
        return audit_result(passed, summary.coverage_pct, args.min_coverage);
    }

    println!("\n🛡️  Distributed Trace Coverage Audit: {}", args.topic);
    println!("{}", "━".repeat(70));

    let mut table = output::table(vec![
        Cell::new("Audit Metric").fg(Color::Cyan),
        Cell::new("Observed Value")
            .fg(Color::Cyan)
            .set_alignment(CellAlignment::Right),
        Cell::new("Status / Compliance").fg(Color::Cyan),
    ]);

    table.add_row(vec![
        Cell::new("Scanned Messages"),
        Cell::new(summary.total_records.to_string()).set_alignment(CellAlignment::Right),
        Cell::new("Audit Sample Window"),
    ]);

    table.add_row(vec![
        Cell::new("Traced Messages"),
        Cell::new(summary.traced_records.to_string())
            .set_alignment(CellAlignment::Right)
            .fg(Color::Green),
        Cell::new("Contains valid distributed context"),
    ]);

    table.add_row(vec![
        Cell::new("Untraced ('Dark Traffic')"),
        Cell::new(summary.untraced_records.to_string())
            .set_alignment(CellAlignment::Right)
            .fg(if summary.untraced_records > 0 {
                Color::Red
            } else {
                Color::Green
            }),
        if summary.untraced_records > 0 {
            Cell::new("⚠️ Observability Blindspot").fg(Color::Yellow)
        } else {
            Cell::new("✓ Zero Dark Traffic").fg(Color::Green)
        },
    ]);

    let cov_color = if passed { Color::Green } else { Color::Red };
    table.add_row(vec![
        Cell::new("Trace Coverage %"),
        Cell::new(format!("{:.1}%", summary.coverage_pct))
            .set_alignment(CellAlignment::Right)
            .fg(cov_color),
        Cell::new(format!("Target: >= {:.1}%", args.min_coverage)),
    ]);

    table.add_row(vec![
        Cell::new("W3C TraceContext Share"),
        Cell::new(format!("{} msg", summary.w3c_count))
            .set_alignment(CellAlignment::Right)
            .fg(Color::Green),
        Cell::new("Standard OpenTelemetry header"),
    ]);

    table.add_row(vec![
        Cell::new("B3 / Zipkin Share"),
        Cell::new(format!("{} msg", summary.b3_count))
            .set_alignment(CellAlignment::Right)
            .fg(Color::Blue),
        Cell::new("Legacy Spring/Zipkin format"),
    ]);

    println!("{table}\n");

    if !passed {
        println!(
            "⚠️  AUDIT WARNING: Trace coverage ({:.1}%) is below configured threshold ({:.1}%)!\n",
            summary.coverage_pct, args.min_coverage
        );
    } else {
        println!(
            "✅ AUDIT PASSED: Trace coverage satisfies SLA policy requirement (>= {:.1}%).\n",
            args.min_coverage
        );
    }

    audit_result(passed, summary.coverage_pct, args.min_coverage)
}

/// The audit passes when coverage reaches the threshold; a NaN on either side fails.
fn audit_passed(coverage_pct: f64, min_coverage: f64) -> bool {
    coverage_pct >= min_coverage
}

/// Ok when the audit passed, otherwise a check failure (non-zero exit).
fn audit_result(passed: bool, coverage_pct: f64, min_coverage: f64) -> KafkaResult<()> {
    if passed {
        Ok(())
    } else {
        Err(KafkaError::Check(format!(
            "Trace coverage {coverage_pct:.1}% is below the required {min_coverage:.1}%"
        )))
    }
}

fn run_inject(args: InjectTraceArgs, global: &GlobalOpts) -> KafkaResult<()> {
    let mut generated = Vec::new();

    for _ in 0..args.count {
        let tp = W3cTraceparent::generate(args.sampled);
        generated.push(tp);
    }

    if global.wants_json(args.json) {
        output::print_json(&generated)?;
        return Ok(());
    }

    println!("\n⚡ Generated W3C TraceContext Headers:");
    println!("{}", "━".repeat(70));

    for (i, tp) in generated.iter().enumerate() {
        println!("Header {}:", i + 1);
        println!("  traceparent: {}", tp.to_header_value());
        println!("    ├─ Version:    {}", tp.version);
        println!("    ├─ Trace ID:   {}", tp.trace_id);
        println!("    ├─ Span ID:    {}", tp.parent_id);
        println!(
            "    └─ Flags:      {} (Sampled: {})",
            tp.flags,
            tp.is_sampled()
        );
    }
    println!();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn audit_below_threshold_is_a_check_failure() {
        assert!(audit_passed(90.0, 90.0));
        assert!(!audit_passed(89.9, 90.0));
        assert!(!audit_passed(100.0, f64::NAN));
        assert!(audit_result(true, 100.0, 90.0).is_ok());
        assert!(matches!(
            audit_result(false, 10.0, 90.0),
            Err(KafkaError::Check(_))
        ));
    }
    use clap::Parser;

    #[derive(Parser)]
    struct Wrapper {
        #[command(subcommand)]
        cmd: TraceSubcommands,
    }

    #[test]
    fn test_clap_trace_args_inspect_parsing() {
        let parsed = Wrapper::parse_from([
            "trace",
            "inspect",
            "order-events",
            "-n",
            "100",
            "--standard",
            "w3c",
            "--json",
        ]);
        match parsed.cmd {
            TraceSubcommands::Inspect(args) => {
                assert_eq!(args.topic, "order-events");
                assert_eq!(args.limit, 100);
                assert_eq!(args.standard.as_deref(), Some("w3c"));
                assert!(args.json);
            }
            _ => panic!("Expected Inspect subcommand"),
        }
    }

    #[test]
    fn inject_can_generate_unsampled_traces() {
        let sampled = |args: &[&str]| match Wrapper::parse_from(args).cmd {
            TraceSubcommands::Inject(a) => a.sampled,
            _ => panic!("Expected Inject subcommand"),
        };
        assert!(sampled(&["trace", "inject"]));
        assert!(sampled(&["trace", "inject", "--sampled"]));
        assert!(!sampled(&["trace", "inject", "--sampled", "false"]));
    }

    #[test]
    fn test_clap_trace_args_audit_and_inject_parsing() {
        let parsed_audit = Wrapper::parse_from([
            "trace",
            "audit",
            "payments",
            "-n",
            "500",
            "--min-coverage",
            "98.5",
        ]);
        match parsed_audit.cmd {
            TraceSubcommands::Audit(args) => {
                assert_eq!(args.topic, "payments");
                assert_eq!(args.limit, 500);
                assert!((args.min_coverage - 98.5).abs() < 0.01);
            }
            _ => panic!("Expected Audit subcommand"),
        }

        let parsed_inject = Wrapper::parse_from(["trace", "inject", "-n", "5", "--json"]);
        match parsed_inject.cmd {
            TraceSubcommands::Inject(args) => {
                assert_eq!(args.count, 5);
                assert!(args.json);
            }
            _ => panic!("Expected Inject subcommand"),
        }
    }

    #[test]
    fn inspect_masks_trace_header_text_only_under_an_enforcing_policy() {
        use crate::governance::test_policy::{leaks, masking, none, PII};
        let headers = Vec::from([
            ("X-B3-TraceId".to_string(), PII.as_bytes().to_vec()),
            ("X-B3-SpanId".to_string(), b"kim@example.com".to_vec()),
        ]);
        let records = vec![(0, 1, ExtractedTrace::from_headers(&headers))];
        let as_json = |r: &[(i32, i64, ExtractedTrace)]| serde_json::to_string(r).unwrap();

        assert!(leaks(&as_json(&shown(records.clone(), &none()))));
        let masked = shown(records, &masking());
        let text = as_json(&masked);
        assert!(!leaks(&text), "{text}");
        assert_eq!(masked[0].2.standard, TraceStandard::B3Multi);

        // A real W3C trace context is left readable.
        let w3c = Vec::from([(
            "traceparent".to_string(),
            b"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_vec(),
        )]);
        let masked = shown(vec![(0, 2, ExtractedTrace::from_headers(&w3c))], &masking());
        assert_eq!(
            masked[0].2.trace_id.as_deref(),
            Some("4bf92f3577b34da6a3ce929d0e0e4736")
        );
    }
}
