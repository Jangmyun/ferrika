//! `ferrika audit`: recent operations that changed a cluster.

use clap::Args;
use colored::Colorize;
use comfy_table::{Cell, Color};

use crate::cli::{output, GlobalOpts, OutputFormat};
use crate::governance::audit::{audit_path, AuditEntry};
use crate::governance::audit_anchor::{self as anchor, AnchorCheck, AuditAnchor, OsLogAnchor};

#[derive(clap::Subcommand, Debug, Clone)]
pub enum AuditCommand {
    /// Check the log's hash chain; reports the first edited, inserted or removed line
    Verify {
        /// Also compare the log's end with the newest anchor in the OS log
        /// (Windows Event Log) to detect a truncated or replaced log
        #[arg(long)]
        anchor: bool,
    },
    /// Send a test entry to the SIEM forwarding targets (policy `audit_forward`
    /// or FERRIKA_AUDIT_FORWARD) and print the result for each
    ForwardTest {
        /// Test this target instead: udp://, tcp:// or tls://host[:port], or
        /// spool:<dir>. The test entry is then not written to the local log.
        #[arg(long)]
        to: Option<String>,
    },
}

/// Which entries `ferrika audit` lists; the limit counts matching entries.
#[derive(Args, Debug, Clone, Default, PartialEq, Eq)]
pub struct AuditFilter {
    /// Only entries from this source: cli, gui or mcp
    #[arg(long, value_name = "SOURCE")]
    pub source: Option<String>,
    /// Only entries whose actor contains this text (e.g. an MCP client name
    /// or session id)
    #[arg(long, value_name = "TEXT")]
    pub actor: Option<String>,
    /// Only failed or refused operations
    #[arg(long)]
    pub failed: bool,
}

impl AuditFilter {
    fn keeps(&self, entry: &AuditEntry) -> bool {
        self.source
            .as_deref()
            .is_none_or(|s| entry.source.eq_ignore_ascii_case(s))
            && self.actor.as_deref().is_none_or(|text| {
                entry
                    .actor
                    .as_deref()
                    .is_some_and(|a| a.to_lowercase().contains(&text.to_lowercase()))
            })
            && (!self.failed || !entry.ok)
    }
}

pub fn run(limit: usize, filter: &AuditFilter, command: Option<AuditCommand>, global: &GlobalOpts) {
    match command {
        Some(AuditCommand::Verify { anchor }) => {
            verify(global, anchor);
            return;
        }
        Some(AuditCommand::ForwardTest { to }) => {
            forward_test(to.as_deref(), global);
            return;
        }
        None => {}
    }
    let entries = crate::governance::audit::recent_matching(limit, |e| filter.keeps(e));
    if global.format == OutputFormat::Json {
        if let Err(e) = output::print_json(&entries) {
            crate::cli::exit_with_error(&e);
        }
        return;
    }
    if entries.is_empty() {
        let what = if *filter == AuditFilter::default() {
            "No recorded operations yet"
        } else {
            "No matching entries"
        };
        println!(
            "{what} ({}).",
            audit_path().display().to_string().bright_black()
        );
        return;
    }
    let mut table = output::table(vec![
        "TIME (UTC)",
        "FROM",
        "USER",
        "ACTOR",
        "PROFILE",
        "ACTION",
        "TARGET",
        "OUTCOME",
    ]);
    for e in &entries {
        table.add_row(vec![
            Cell::new(crate::model::format::format_timestamp_ms(e.time_ms)),
            Cell::new(&e.source),
            Cell::new(&e.user),
            Cell::new(e.actor.as_deref().unwrap_or("-")),
            Cell::new(&e.profile),
            Cell::new(&e.action),
            Cell::new(&e.target),
            Cell::new(&e.outcome).fg(if e.ok { Color::Green } else { Color::Red }),
        ]);
    }
    println!("{table}");
}

fn verify(global: &GlobalOpts, with_anchor: bool) {
    let path = audit_path();
    let report = match crate::governance::audit::verify() {
        Ok(report) => report,
        Err(e) => crate::cli::exit_with_error(&format!("could not read {}: {e}", path.display())),
    };
    let anchor_check = with_anchor.then(|| {
        let latest = OsLogAnchor::new(&path).latest().unwrap_or_else(|e| {
            crate::cli::exit_with_error(&format!("could not read the audit anchor: {e}"))
        });
        anchor::compare(&path, latest).unwrap_or_else(|e| {
            crate::cli::exit_with_error(&format!("could not read {}: {e}", path.display()))
        })
    });
    if global.format == OutputFormat::Json {
        let printed = match &anchor_check {
            Some(check) => output::print_json(&serde_json::json!({
                "chain": &report,
                "anchor": check,
            })),
            None => output::print_json(&report),
        };
        if let Err(e) = printed {
            crate::cli::exit_with_error(&e);
        }
    } else if let Some((line, why)) = &report.broken {
        eprintln!(
            "{} {} line {line}: {why}",
            "✖ Audit log tampered:".red().bold(),
            path.display()
        );
    } else {
        println!(
            "{} {} — {} chained lines{}",
            "✔ Audit log OK:".green().bold(),
            path.display(),
            report.chained,
            if report.legacy > 0 {
                format!(" (+{} older unchained lines before them)", report.legacy)
            } else {
                String::new()
            }
        );
    }
    if global.format != OutputFormat::Json && !report.torn.is_empty() {
        let lines: Vec<String> = report.torn.iter().map(usize::to_string).collect();
        eprintln!(
            "{} line(s) {} cut short by an interrupted write (crash or power loss); \
             their contents are lost, the chain around them is intact",
            "⚠ Torn:".yellow().bold(),
            lines.join(", ")
        );
    }
    if global.format != OutputFormat::Json {
        if let Some(check) = &anchor_check {
            print_anchor_check(&path, check);
        }
    }
    if report.broken.is_some() || anchor_check.as_ref().is_some_and(AnchorCheck::is_failure) {
        std::process::exit(2);
    }
}

fn forward_test(to: Option<&str>, global: &GlobalOpts) {
    use crate::governance::audit::{append_local, now_ms, AuditEntry};
    use crate::governance::audit_forward::{self, ForwardOrigin, ForwardSettings};

    let settings = match to {
        Some(spec) => audit_forward::parse_local(spec)
            .and_then(|f| ForwardSettings::new(f, ForwardOrigin::CommandLine)),
        None => match audit_forward::settings_status() {
            Ok(Some(settings)) => Ok(settings.clone()),
            Ok(None) => Err(format!(
                "audit forwarding is not configured (company policy `rules.audit_forward`, \
                 or {} for individual use)",
                audit_forward::ENV_VAR
            )),
            Err(e) => Err(e.clone()),
        },
    };
    let settings = settings.unwrap_or_else(|e| crate::cli::exit_with_error(&e));
    let entry = AuditEntry {
        time_ms: now_ms(),
        source: "cli".to_string(),
        profile: String::new(),
        action: "audit.forward_test".to_string(),
        target: settings.describe(),
        outcome: "test".to_string(),
        ok: true,
        user: crate::governance::audit::os_user(),
        actor: None,
        op: String::new(),
        op_digest: String::new(),
    };
    // Through the configured path the test entry is a real, chained entry,
    // so the SIEM sees an unbroken chain; `--to` sends it unchained.
    let line = if to.is_some() {
        serde_json::to_string(&entry).map_err(|e| e.to_string())
    } else {
        append_local(&entry).map_err(|e| format!("could not write the local audit log: {e}"))
    };
    let line = line.unwrap_or_else(|e| crate::cli::exit_with_error(&e));
    let reports = audit_forward::send(&settings, &line, true, entry.time_ms);
    let failed = reports.iter().any(|r| !r.ok);
    if global.format == OutputFormat::Json {
        let out = serde_json::json!({
            "origin": settings.origin,
            "required": settings.config.required,
            "timeout_ms": settings.config.timeout_ms,
            "results": reports,
        });
        if let Err(e) = output::print_json(&out) {
            crate::cli::exit_with_error(&e);
        }
    } else {
        println!(
            "Audit forwarding from {} (timeout {} ms{})",
            settings.origin.label(),
            settings.config.timeout_ms,
            if settings.config.required {
                ", required"
            } else {
                ""
            }
        );
        for r in &reports {
            match &r.error {
                None => println!(
                    "  {} {} ({} ms)",
                    "✔ sent".green().bold(),
                    r.target,
                    r.elapsed_ms
                ),
                Some(e) => println!("  {} {}: {e}", "✖ failed".red().bold(), r.target),
            }
        }
        if !failed {
            println!(
                "{}",
                "Sent = handed to the network or written to the spool folder; check the collector."
                    .bright_black()
            );
        }
    }
    if failed {
        std::process::exit(1);
    }
}

fn print_anchor_check(path: &std::path::Path, check: &AnchorCheck) {
    match check {
        AnchorCheck::Match { seq, log_seq } => println!(
            "{} the log still holds anchored line seq {seq}{}",
            "✔ Anchor OK:".green().bold(),
            if log_seq > seq {
                format!(" ({} later line(s) not anchored)", log_seq - seq)
            } else {
                String::new()
            }
        ),
        AnchorCheck::Truncated {
            anchor_seq,
            log_seq,
        } => eprintln!(
            "{} {} — truncated or rewritten: the anchor recorded seq {anchor_seq}, \
             the log ends at seq {log_seq}",
            "✖ Audit log truncated:".red().bold(),
            path.display()
        ),
        AnchorCheck::Rewritten { seq, .. } => eprintln!(
            "{} {} — truncated or rewritten: line seq {seq} does not match the anchored hash",
            "✖ Audit log rewritten:".red().bold(),
            path.display()
        ),
        AnchorCheck::Unavailable => eprintln!(
            "{} {}",
            "⚠ No anchor:".yellow().bold(),
            if anchor::OS_LOG_READABLE {
                "no Ferrika anchor for this log in the Windows Event Log \
                 (policy rule audit_anchor: os_log turns it on)"
            } else {
                "anchors go to syslog on this OS and are not read back; \
                 compare with `journalctl -t ferrika` by hand"
            }
        ),
    }
}

#[cfg(test)]
mod filter_tests {
    use super::*;

    fn entry(source: &str, actor: Option<&str>, ok: bool) -> AuditEntry {
        AuditEntry {
            time_ms: 0,
            source: source.into(),
            profile: String::new(),
            action: "topics create".into(),
            target: "t".into(),
            outcome: String::new(),
            ok,
            user: "me".into(),
            actor: actor.map(str::to_string),
            op: String::new(),
            op_digest: String::new(),
        }
    }

    #[test]
    fn filters_by_source_actor_and_failure() {
        let mcp = entry("mcp", Some("mcp:claude-code/2.1.4#a1b2c3d4"), true);
        let cli_failed = entry("cli", None, false);
        let all = AuditFilter::default();
        assert!(all.keeps(&mcp) && all.keeps(&cli_failed));

        let by_source = AuditFilter {
            source: Some("MCP".into()),
            ..AuditFilter::default()
        };
        assert!(by_source.keeps(&mcp) && !by_source.keeps(&cli_failed));

        let by_actor = AuditFilter {
            actor: Some("Claude-Code".into()),
            ..AuditFilter::default()
        };
        assert!(by_actor.keeps(&mcp) && !by_actor.keeps(&cli_failed));

        let failed = AuditFilter {
            failed: true,
            ..AuditFilter::default()
        };
        assert!(!failed.keeps(&mcp) && failed.keeps(&cli_failed));
    }
}
