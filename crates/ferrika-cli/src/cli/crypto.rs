use std::io::{self, BufRead};
use std::time::Duration;

use clap::{Args, Subcommand, ValueEnum};
use comfy_table::{Cell, CellAlignment, Color};
use serde_json::Value;

use crate::cli::{output, GlobalOpts};
use crate::codec::pii::{
    build_audit_report, mask_bytes, mask_payload, scan_json_for_pii, MaskStrategy, PiiAuditReport,
};
use crate::error::{KafkaError, KafkaResult};
use crate::kafka::consumer::KafkaConsumerClient;
use crate::kafka::consumer::StartOffset;

/// Field-level PII masking and Kafka security compliance auditor.
#[derive(Debug, Args)]
#[command(
    name = "crypto",
    about = "Audit topics for unmasked PII/secrets or mask sensitive JSON payloads in real-time"
)]
pub struct CryptoArgs {
    #[command(subcommand)]
    pub command: CryptoCommands,
}

#[derive(Debug, Subcommand)]
pub enum CryptoCommands {
    /// Scan topic messages for unmasked PII, credentials, or sensitive fields
    Audit(CryptoAuditArgs),
    /// Mask sensitive fields and PII in JSON payloads from stdin or topic stream
    Mask(CryptoMaskArgs),
}

#[derive(Debug, Args)]
pub struct CryptoAuditArgs {
    /// Topic to scan and audit
    #[arg(value_name = "TOPIC")]
    pub topic: String,

    /// Number of messages to sample and scan
    #[arg(short = 'n', long, default_value = "100")]
    pub limit: usize,

    /// Fail with non-zero exit code if any PII leak or sensitive violation is detected
    #[arg(long)]
    pub fail_on_leak: bool,

    /// Output audit report in JSON format
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Default)]
pub enum CliMaskStrategy {
    #[default]
    Partial,
    Redact,
}

impl From<CliMaskStrategy> for MaskStrategy {
    fn from(s: CliMaskStrategy) -> Self {
        match s {
            CliMaskStrategy::Partial => MaskStrategy::Partial,
            CliMaskStrategy::Redact => MaskStrategy::Redact,
        }
    }
}

#[derive(Debug, Args)]
pub struct CryptoMaskArgs {
    /// Optional topic to consume from directly (if omitted, reads JSON from stdin)
    #[arg(short = 't', long)]
    pub topic: Option<String>,

    /// Masking strategy to apply
    #[arg(long, value_enum, default_value_t = CliMaskStrategy::Partial)]
    pub strategy: CliMaskStrategy,

    /// Number of messages to mask before exiting (default 0 = infinite)
    #[arg(short = 'n', long, default_value = "0")]
    pub limit: usize,
}

/// Dispatches the crypto subcommand.
pub async fn handle_crypto_command(args: CryptoArgs, global: &GlobalOpts) -> KafkaResult<()> {
    match args.command {
        CryptoCommands::Audit(audit_args) => handle_audit(audit_args, global).await,
        CryptoCommands::Mask(mask_args) => handle_mask(mask_args, global).await,
    }
}

async fn handle_audit(args: CryptoAuditArgs, global: &GlobalOpts) -> KafkaResult<()> {
    let config = global.to_kafka_config();
    let consumer = KafkaConsumerClient::scanner(&config)?;
    consumer.assign_topic(&args.topic, StartOffset::Beginning)?;

    let mut scanned = 0;
    let mut findings = Vec::new();
    let timeout = Duration::from_millis(1500);

    while scanned < args.limit {
        match tokio::time::timeout(timeout, consumer.recv()).await {
            Ok(Ok(msg)) => {
                scanned += 1;
                let partition = msg.partition;
                let offset = msg.offset;

                if let Some(payload_str) = msg.payload_str() {
                    if let Ok(json_val) = serde_json::from_str::<Value>(payload_str) {
                        scan_json_for_pii(
                            &json_val,
                            "$",
                            Some(partition),
                            Some(offset),
                            &mut findings,
                        );
                    } else {
                        // Raw string fallback
                        let str_val = Value::String(payload_str.to_string());
                        scan_json_for_pii(
                            &str_val,
                            "$.raw_payload",
                            Some(partition),
                            Some(offset),
                            &mut findings,
                        );
                    }
                }
            }
            _ => break,
        }
    }

    let mut report = build_audit_report(&args.topic, scanned, findings);
    let policy = crate::governance::policy::current();
    // Includes the brokers the cluster reports when the policy matches them
    // (`export_clusters`); a lookup that fails counts as forbidden.
    if crate::governance::policy::export_forbidden_with(
        policy,
        &global.to_kafka_config(),
        crate::kafka::admin::known_brokers,
    ) {
        withhold_findings(&mut report);
        eprintln!(
            "Findings are left out: {} forbids exporting record data (export: forbidden), \
             so only the counts are shown.",
            policy.label()
        );
    }

    output::emit(global.wants_json(args.json), &report, render_audit_report)?;

    if args.fail_on_leak && report.total_violations > 0 {
        return Err(KafkaError::Check(format!(
            "PII Security Gate Failure: Detected {} violations in topic '{}'",
            report.total_violations, args.topic
        )));
    }

    Ok(())
}

/// Drops the findings, whose masked samples are still parts of records, and
/// keeps the counts: `crypto audit --fail-on-leak` runs in CI pipelines, so
/// under `export: forbidden` it keeps working instead of being refused.
fn withhold_findings(report: &mut PiiAuditReport) {
    report.findings.clear();
}

fn render_audit_report(report: &PiiAuditReport) {
    let status_badge = if report.total_violations == 0 {
        Cell::new(format!(
            "{:.1}% COMPLIANT (CLEAN)",
            report.compliance_score_pct
        ))
        .fg(Color::Green)
    } else {
        Cell::new(format!(
            "{:.1}% COMPLIANT ({} VIOLATIONS)",
            report.compliance_score_pct, report.total_violations
        ))
        .fg(Color::Red)
    };

    println!(
        "\n══ Kafka PII & Secret Security Audit: {} ══",
        report.topic
    );

    let mut summary_table = output::fixed_table(vec![
        Cell::new("Compliance Status").fg(Color::Cyan),
        Cell::new("Messages Scanned").fg(Color::Cyan),
        Cell::new("Credit Cards").fg(Color::Cyan),
        Cell::new("Emails").fg(Color::Cyan),
        Cell::new("SSNs").fg(Color::Cyan),
        Cell::new("Tokens/Keys").fg(Color::Cyan),
        Cell::new("Sensitive Keys").fg(Color::Cyan),
        Cell::new("Korean PII").fg(Color::Cyan),
        Cell::new("Names").fg(Color::Cyan),
    ]);

    summary_table.add_row(vec![
        status_badge,
        Cell::new(report.messages_scanned.to_string()).set_alignment(CellAlignment::Right),
        Cell::new(report.credit_cards_found.to_string()).set_alignment(CellAlignment::Right),
        Cell::new(report.emails_found.to_string()).set_alignment(CellAlignment::Right),
        Cell::new(report.ssns_found.to_string()).set_alignment(CellAlignment::Right),
        Cell::new(report.tokens_found.to_string()).set_alignment(CellAlignment::Right),
        Cell::new(report.sensitive_keys_found.to_string()).set_alignment(CellAlignment::Right),
        Cell::new(report.korean_pii_found.to_string()).set_alignment(CellAlignment::Right),
        Cell::new(report.names_found.to_string()).set_alignment(CellAlignment::Right),
    ]);

    println!("{summary_table}\n");

    if report.findings.is_empty() {
        // Violations without findings: the findings were withheld.
        if report.total_violations > 0 {
            return;
        }
        println!("✨ No unmasked PII or sensitive credential leaks detected.\n");
        return;
    }

    let mut violations_table = output::fixed_table(vec![
        Cell::new("Part:Offset").fg(Color::Cyan),
        Cell::new("Field Locator").fg(Color::Cyan),
        Cell::new("Violation Type").fg(Color::Cyan),
        Cell::new("Safe Masked Preview").fg(Color::Cyan),
    ]);

    for f in report.findings.iter().take(20) {
        let part_offset = match (f.partition, f.offset) {
            (Some(p), Some(o)) => format!("{p}:{o}"),
            _ => "-".to_string(),
        };

        violations_table.add_row(vec![
            Cell::new(part_offset).set_alignment(CellAlignment::Center),
            Cell::new(&f.field_path).fg(Color::Yellow),
            Cell::new(f.pii_type.display_name()).fg(Color::Red),
            Cell::new(&f.masked_sample).fg(Color::White),
        ]);
    }

    println!("{violations_table}");
    if report.findings.len() > 20 {
        println!(
            "ℹ Truncated {} additional violations. Use --json for full list.\n",
            report.findings.len() - 20
        );
    } else {
        println!();
    }
}

async fn handle_mask(args: CryptoMaskArgs, global: &GlobalOpts) -> KafkaResult<()> {
    let strategy: MaskStrategy = args.strategy.into();

    if let Some(topic) = args.topic {
        // Stream from Kafka topic and mask
        let config = global.to_kafka_config();
        let consumer = KafkaConsumerClient::scanner(&config)?;
        consumer.assign_topic(&topic, StartOffset::Beginning)?;

        let mut count = 0;
        let timeout = Duration::from_millis(1000);

        loop {
            if args.limit > 0 && count >= args.limit {
                break;
            }

            match tokio::time::timeout(timeout, consumer.recv()).await {
                Ok(Ok(msg)) => {
                    count += 1;
                    // Through `mask_bytes`: an undecoded schema record is
                    // hidden whole, not scanned as free text.
                    if let Some(payload) = msg.payload.as_deref() {
                        println!("{}", mask_bytes(payload, strategy));
                    }
                }
                Ok(Err(e)) => {
                    eprintln!("Error polling message: {e}");
                    break;
                }
                Err(_) => {}
            }
        }
    } else {
        // Read from stdin
        let stdin = io::stdin();
        let handle = stdin.lock();
        let mut count = 0;

        for line_res in handle.lines() {
            if args.limit > 0 && count >= args.limit {
                break;
            }

            match line_res {
                Ok(line) => {
                    count += 1;
                    println!("{}", mask_payload(&line, strategy));
                }
                Err(e) => {
                    eprintln!("Error reading stdin: {e}");
                    break;
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser, Debug)]
    struct CliTestWrapper {
        #[command(subcommand)]
        cmd: TestCommands,
    }

    #[derive(Subcommand, Debug)]
    enum TestCommands {
        Crypto(CryptoArgs),
    }

    #[test]
    fn test_clap_crypto_audit_parsing() {
        let args = CliTestWrapper::try_parse_from([
            "ferrika",
            "crypto",
            "audit",
            "orders-v1",
            "-n",
            "50",
            "--fail-on-leak",
            "--json",
        ]);
        assert!(args.is_ok());
        let TestCommands::Crypto(crypto_args) = args.unwrap().cmd;
        if let CryptoCommands::Audit(audit) = crypto_args.command {
            assert_eq!(audit.topic, "orders-v1");
            assert_eq!(audit.limit, 50);
            assert!(audit.fail_on_leak);
            assert!(audit.json);
        } else {
            panic!("Expected CryptoCommands::Audit");
        }
    }

    #[test]
    fn test_clap_crypto_mask_parsing() {
        let args = CliTestWrapper::try_parse_from([
            "ferrika",
            "crypto",
            "mask",
            "-t",
            "payments",
            "--strategy",
            "redact",
            "-n",
            "10",
        ]);
        assert!(args.is_ok());
        let TestCommands::Crypto(crypto_args) = args.unwrap().cmd;
        if let CryptoCommands::Mask(mask) = crypto_args.command {
            assert_eq!(mask.topic.as_deref(), Some("payments"));
            assert_eq!(mask.strategy, CliMaskStrategy::Redact);
            assert_eq!(mask.limit, 10);
        } else {
            panic!("Expected CryptoCommands::Mask");
        }
    }

    #[test]
    fn withheld_findings_keep_the_counts() {
        let mut findings = Vec::new();
        scan_json_for_pii(
            &serde_json::json!({ "email": "kim@example.com" }),
            "$",
            Some(0),
            Some(7),
            &mut findings,
        );
        let mut report = build_audit_report("users", 1, findings);
        assert!(report.total_violations > 0);
        let violations = report.total_violations;
        withhold_findings(&mut report);
        assert!(report.findings.is_empty());
        assert_eq!(report.total_violations, violations);
        let json = serde_json::to_string(&report).unwrap();
        assert!(!json.contains("example.com"), "{json}");
    }
}
