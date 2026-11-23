use clap::Args;
use comfy_table::{Cell, CellAlignment, Color};

use crate::cli::{output, GlobalOpts};
use crate::error::KafkaResult;
use crate::kafka::bounded_read::{
    plan_aligned_ranges, read_ranges, topic_watermarks, BoundedReadOptions,
};
use crate::ops::diff::{DiffRecord, RecordDiffType, TopicDiffSummary};

#[derive(Debug, Args)]
#[command(
    name = "diff",
    about = "Compare records and schema parity between two Kafka topics (e.g. blue/green or staging/prod)"
)]
pub struct DiffArgs {
    /// Source topic name (Topic A)
    #[arg(value_name = "TOPIC_A")]
    pub topic_a: String,

    /// Target topic name to compare against (Topic B)
    #[arg(value_name = "TOPIC_B")]
    pub topic_b: String,

    /// Maximum number of offsets to compare per partition (the same
    /// partition/offset positions are read from both topics)
    #[arg(short = 'n', long, default_value = "100", value_parser = parse_limit)]
    pub limit: usize,

    /// Include identical/matching records in the detailed report
    #[arg(long)]
    pub show_identical: bool,

    /// Output full comparison results in JSON format
    #[arg(long)]
    pub json: bool,

    /// Exit like `git diff --exit-code`: 1 when the topics differ in the
    /// compared range, 0 when they match, 2 on errors (without it: 0
    /// unless an error, then 1)
    #[arg(long)]
    pub exit_code: bool,
}

/// `--limit` must be at least 1: comparing 0 records proves nothing.
fn parse_limit(raw: &str) -> Result<usize, String> {
    match raw.trim().parse::<usize>() {
        Ok(0) => Err("must be at least 1 (comparing 0 records proves nothing)".to_string()),
        Ok(n) => Ok(n),
        Err(e) => Err(format!("invalid number '{raw}': {e}")),
    }
}

pub async fn run(args: DiffArgs, global: &GlobalOpts) -> KafkaResult<()> {
    let (records_a, records_b) = fetch_aligned_records(&args, global).await?;

    let policy = crate::governance::policy::current();
    crate::cli::note_masking(policy);
    let summary = shown(&args, &records_a, &records_b, policy);
    crate::cli::report_differences(args.exit_code, has_differences(&summary));

    if global.wants_json(args.json) {
        output::print_json(&summary)?;
        return Ok(());
    }

    println!("\n🔍 Ferrika Topic Data & Schema Diff");
    println!("  ├─ Topic A (Reference): {}", args.topic_a);
    println!("  └─ Topic B (Candidate): {}", args.topic_b);
    println!("{}", "━".repeat(75));

    if summary.total_compared_keys == 0 {
        println!("No records compared: both topics are empty in the compared range.\n");
        return Ok(());
    }

    // 1. High-level Parity & Summary Table
    let mut summary_table = output::table(vec![
        Cell::new("Comparison Metric").fg(Color::Cyan),
        Cell::new("Observed Count")
            .fg(Color::Cyan)
            .set_alignment(CellAlignment::Right),
        Cell::new("Parity Status").fg(Color::Cyan),
    ]);

    summary_table.add_row(vec![
        Cell::new("Total Records Compared"),
        Cell::new(summary.total_compared_keys.to_string()).set_alignment(CellAlignment::Right),
        Cell::new("Same partition/offset positions in both topics"),
    ]);

    summary_table.add_row(vec![
        Cell::new("Identical Payloads (Match)"),
        Cell::new(summary.identical_count.to_string())
            .set_alignment(CellAlignment::Right)
            .fg(Color::Green),
        Cell::new("100% binary/JSON content match"),
    ]);

    summary_table.add_row(vec![
        Cell::new("Modified Payloads (Drift)"),
        Cell::new(summary.modified_count.to_string())
            .set_alignment(CellAlignment::Right)
            .fg(if summary.modified_count > 0 {
                Color::Yellow
            } else {
                Color::Green
            }),
        Cell::new("Same position with altered key or field values"),
    ]);

    summary_table.add_row(vec![
        Cell::new(format!("Missing in Topic B (-{})", args.topic_b)),
        Cell::new(summary.missing_in_b_count.to_string())
            .set_alignment(CellAlignment::Right)
            .fg(if summary.missing_in_b_count > 0 {
                Color::Red
            } else {
                Color::Green
            }),
        Cell::new("Position exists in A but not in B"),
    ]);

    summary_table.add_row(vec![
        Cell::new(format!("Missing in Topic A (+{})", args.topic_b)),
        Cell::new(summary.missing_in_a_count.to_string())
            .set_alignment(CellAlignment::Right)
            .fg(if summary.missing_in_a_count > 0 {
                Color::Blue
            } else {
                Color::Green
            }),
        Cell::new("Position exists in B only"),
    ]);

    let parity_color = if summary.parity_pct >= 99.0 {
        Color::Green
    } else if summary.parity_pct >= 80.0 {
        Color::Yellow
    } else {
        Color::Red
    };

    summary_table.add_row(vec![
        Cell::new("Content Parity %"),
        Cell::new(format!("{:.1}%", summary.parity_pct))
            .set_alignment(CellAlignment::Right)
            .fg(parity_color),
        Cell::new(if summary.parity_pct == 100.0 {
            "✓ Perfect Mirror"
        } else {
            "⚠️ Content Drift Detected"
        })
        .fg(parity_color),
    ]);

    println!("{summary_table}\n");

    // 2. Detailed Record Differences Table
    let mut detailed_records: Vec<_> = summary
        .records
        .iter()
        .filter(|r| args.show_identical || r.diff_type != RecordDiffType::Identical)
        .collect();

    if detailed_records.is_empty() {
        println!("✨ No record differences detected across topics!\n");
        return Ok(());
    }

    println!(
        "📋 Record Differences Breakdown (Top {} shown):",
        detailed_records.len().min(25)
    );

    let mut diff_table = output::fixed_table(vec![
        Cell::new("P:O Key").fg(Color::Cyan),
        Cell::new("Diff Type").fg(Color::Cyan),
        Cell::new("Field-Level Drifts & Value Delta").fg(Color::Cyan),
    ]);

    for r in detailed_records.drain(..detailed_records.len().min(25)) {
        let (type_cell, delta_desc) = match r.diff_type {
            RecordDiffType::Identical => (
                Cell::new("Identical").fg(Color::Green),
                "Payload matches completely".to_string(),
            ),
            RecordDiffType::Modified => {
                let mut lines = Vec::new();
                for fd in &r.field_diffs {
                    match (&fd.val_a, &fd.val_b) {
                        (Some(a), Some(b)) => lines.push(format!("~ {}: {} -> {}", fd.path, a, b)),
                        (Some(a), None) => lines.push(format!("- {}: {}", fd.path, a)),
                        (None, Some(b)) => lines.push(format!("+ {}: {}", fd.path, b)),
                        (None, None) => {}
                    }
                }
                if lines.is_empty() {
                    lines.push("Binary payload modified".to_string());
                }
                (Cell::new("Modified").fg(Color::Yellow), lines.join("\n"))
            }
            RecordDiffType::MissingInB => (
                Cell::new("Missing in B").fg(Color::Red),
                format!(
                    "Present in Topic A only ({})",
                    r.payload_preview_a.as_deref().unwrap_or("-")
                ),
            ),
            RecordDiffType::MissingInA => (
                Cell::new("Missing in A").fg(Color::Blue),
                format!(
                    "New in Topic B only ({})",
                    r.payload_preview_b.as_deref().unwrap_or("-")
                ),
            ),
        };

        diff_table.add_row(vec![
            Cell::new(format!("{}:{} {}", r.partition, r.offset, r.key)).fg(Color::White),
            type_cell,
            Cell::new(delta_desc),
        ]);
    }

    println!("{diff_table}\n");

    Ok(())
}

/// Whether the compared range differs: a record modified or missing on
/// either side.
fn has_differences(summary: &TopicDiffSummary) -> bool {
    summary.modified_count + summary.missing_in_a_count + summary.missing_in_b_count > 0
}

/// The diff as this command may show it: masked while the company policy
/// enforces masking, unchanged otherwise.
fn shown(
    args: &DiffArgs,
    records_a: &[DiffRecord],
    records_b: &[DiffRecord],
    policy: &crate::governance::policy::EffectivePolicy,
) -> TopicDiffSummary {
    let summary = TopicDiffSummary::compute(&args.topic_a, records_a, &args.topic_b, records_b);
    if policy.masking_enforced() {
        summary.masked(records_a, records_b)
    } else {
        summary
    }
}

/// Reads the same partition/offset positions of both topics: per partition,
/// at most `--limit` offsets from the later of their low watermarks, up to
/// the high watermarks captured before reading. Warns on stderr when a read
/// is partial.
async fn fetch_aligned_records(
    args: &DiffArgs,
    global: &GlobalOpts,
) -> KafkaResult<(Vec<DiffRecord>, Vec<DiffRecord>)> {
    let config = global.to_kafka_config();
    let marks_a = topic_watermarks(&config, &args.topic_a, None).await?;
    let marks_b = topic_watermarks(&config, &args.topic_b, None).await?;
    let (ranges_a, ranges_b) = plan_aligned_ranges(&marks_a, &marks_b, args.limit);
    let opts = BoundedReadOptions::default();
    let mut sides = Vec::with_capacity(2);
    for (marks, ranges) in [(marks_a, ranges_a), (marks_b, ranges_b)] {
        let mut read = read_ranges(&config, &ranges, &opts, |_| true).await?;
        read.unavailable = marks.unavailable;
        if let Some(warning) = read.partial_warning(&marks.topic, false) {
            eprintln!("⚠ {warning}");
        }
        sides.push(read.records.iter().map(DiffRecord::from).collect());
    }
    let records_b = sides.pop().unwrap_or_default();
    let records_a = sides.pop().unwrap_or_default();
    Ok((records_a, records_b))
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
        Diff(DiffArgs),
    }

    #[test]
    fn test_clap_diff_args_parsing() {
        let parsed = Wrapper::parse_from([
            "ferrika",
            "diff",
            "orders-v1",
            "orders-v2",
            "-n",
            "250",
            "--show-identical",
            "--json",
        ]);

        match parsed.cmd {
            Subcommands::Diff(args) => {
                assert_eq!(args.topic_a, "orders-v1");
                assert_eq!(args.topic_b, "orders-v2");
                assert_eq!(args.limit, 250);
                assert!(args.show_identical);
                assert!(args.json);
            }
        }
    }

    #[test]
    fn diff_limit_zero_is_rejected() {
        assert!(Wrapper::try_parse_from(["ferrika", "diff", "a", "b", "-n", "0"]).is_err());
        assert!(Wrapper::try_parse_from(["ferrika", "diff", "a", "b", "-n", "x"]).is_err());
        assert!(Wrapper::try_parse_from(["ferrika", "diff", "a", "b", "-n", "1"]).is_ok());
    }

    #[test]
    fn diff_masks_pii_only_under_an_enforcing_policy() {
        use crate::governance::test_policy::{leaks, masking, none, PII};
        let args = DiffArgs {
            topic_a: "a".into(),
            topic_b: "b".into(),
            limit: 10,
            show_identical: true,
            json: true,
            exit_code: false,
        };
        let rec = |partition, offset, key: &str, payload: Vec<u8>| DiffRecord {
            partition,
            offset,
            key: Some(key.as_bytes().to_vec()),
            payload: Some(payload),
        };
        let a = vec![
            rec(
                0,
                0,
                "kim@example.com",
                format!(r#"{{"note":"{PII}","n":1}}"#).into_bytes(),
            ),
            rec(0, 1, "text", PII.as_bytes().to_vec()),
            rec(0, 2, "kim@example.com", b"same".to_vec()),
            rec(1, 0, "only-a", PII.as_bytes().to_vec()),
        ];
        let b = vec![
            rec(
                0,
                0,
                "kim@example.com",
                format!(r#"{{"note":"{PII}","n":2}}"#).into_bytes(),
            ),
            rec(0, 1, "text", b"010-1234-5678".to_vec()),
            // A key change lists both keys, which must be masked too.
            rec(0, 2, "010-1234-5678", b"same".to_vec()),
        ];
        let as_json = |s: &TopicDiffSummary| serde_json::to_string(s).unwrap();

        let plain = shown(&args, &a, &b, &none());
        assert!(leaks(&as_json(&plain)));

        let masked = shown(&args, &a, &b, &masking());
        let text = as_json(&masked);
        assert!(!leaks(&text), "{text}");
        // Comparison still ran on the real values.
        assert_eq!(masked.modified_count, plain.modified_count);
        assert_eq!(masked.missing_in_b_count, 1);
        let modified = masked
            .records
            .iter()
            .find(|r| r.field_diffs.iter().any(|f| f.path.contains('n')))
            .map(|r| r.diff_type);
        assert_eq!(modified, Some(RecordDiffType::Modified));
        assert!(has_differences(&plain));
        let same = shown(&args, &a[2..3], &a[2..3], &none());
        assert!(!has_differences(&same));
    }

    #[test]
    fn exit_code_is_off_by_default() {
        let parsed = Wrapper::parse_from(["ferrika", "diff", "a", "b"]);
        let Subcommands::Diff(args) = parsed.cmd;
        assert!(!args.exit_code);
        let parsed = Wrapper::parse_from(["ferrika", "diff", "a", "b", "--exit-code"]);
        let Subcommands::Diff(args) = parsed.cmd;
        assert!(args.exit_code);
    }
}
