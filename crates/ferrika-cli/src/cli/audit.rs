//! `ferrika audit`: recent operations that changed a cluster.

use clap::Args;
use colored::Colorize;
use comfy_table::{Cell, Color};

use crate::cli::{output, GlobalOpts, OutputFormat};
use crate::governance::audit::{audit_path, AuditEntry};
use crate::governance::audit_anchor::{self as anchor, AnchorCheck, AuditAnchor, OsLogAnchor};

#[derive(clap::Subcommand, Debug, Clone)]
pub enum AuditCommand {
    /// Check the log's hash chain (reports the first edited, inserted or
    /// removed line) and the history entries against their `op_digest`
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
    let history = history_check(
        &crate::ops::history::Journal::default().load(),
        &crate::governance::audit::op_digests(),
    );
    if global.format == OutputFormat::Json {
        let printed = match &anchor_check {
            Some(check) => output::print_json(&serde_json::json!({
                "chain": &report,
                "anchor": check,
                "history": &history,
            })),
            None => {
                let mut value = serde_json::to_value(&report).unwrap_or_default();
                if let Some(map) = value.as_object_mut() {
                    map.insert("history".into(), serde_json::json!(&history));
                }
                output::print_json(&value)
            }
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
        print_history_check(&history);
    }
    if report.broken.is_some()
        || anchor_check.as_ref().is_some_and(AnchorCheck::is_failure)
        || !history.edited.is_empty()
    {
        std::process::exit(2);
    }
}

/// How the history journal's entries compare with the audit log.
#[derive(Debug, Default, PartialEq, Eq, serde::Serialize)]
struct HistoryCheck {
    /// Entries whose version 2 digest matches.
    verified: usize,
    /// Entries matching a version 1 digest (Ferrika 0.1): their scope,
    /// action and undo link are not covered.
    legacy: Vec<String>,
    /// Entries the audit log has no digest for (unfinished, or the log
    /// was rotated or kept elsewhere).
    missing: usize,
    /// Entries that do not match their digest: edited.
    edited: Vec<String>,
}

fn history_check(
    ops: &[crate::model::history::Operation],
    digests: &std::collections::HashMap<String, String>,
) -> HistoryCheck {
    use crate::model::history::DigestStatus;
    let mut check = HistoryCheck::default();
    for op in ops {
        match op.verify_digest(digests.get(&op.id).map(String::as_str)) {
            DigestStatus::Verified => check.verified += 1,
            DigestStatus::Legacy => check.legacy.push(op.id.clone()),
            DigestStatus::Missing => check.missing += 1,
            DigestStatus::Mismatch => check.edited.push(op.id.clone()),
        }
    }
    check
}

fn print_history_check(check: &HistoryCheck) {
    if !check.edited.is_empty() {
        eprintln!(
            "{} {} (they do not match the audit log's op_digest; undo refuses them)",
            "✖ History entries edited:".red().bold(),
            check.edited.join(", ")
        );
    }
    let total = check.verified + check.legacy.len() + check.missing + check.edited.len();
    if total == 0 {
        return;
    }
    println!(
        "{} {} of {total} entries match the audit log{}{}",
        "History:".bold(),
        check.verified + check.legacy.len(),
        if check.legacy.is_empty() {
            String::new()
        } else {
            format!(
                "; {} with a legacy digest (scope not covered): {}",
                check.legacy.len(),
                check.legacy.join(", ")
            )
        },
        if check.missing == 0 {
            String::new()
        } else {
            format!("; {} without an audit entry", check.missing)
        }
    );
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

    #[test]
    fn verify_sorts_history_entries_into_verified_legacy_missing_and_edited() {
        use crate::model::history::{
            fold, Begin, End, EventBody, HistoryEvent, Reversibility, Scope,
        };
        let event = |id: &str, body: EventBody| HistoryEvent {
            v: 1,
            id: id.into(),
            time_ms: 1,
            body,
        };
        let begin = || {
            EventBody::Begin(Box::new(Begin {
                source: "cli".into(),
                user: "u".into(),
                scope: Scope {
                    bootstrap: "a:9092".into(),
                    ..Scope::default()
                },
                action: "topics config".into(),
                target: "t".into(),
                reversibility: Reversibility::Reversible,
                limits: vec![],
                before: vec![],
                after: vec![],
                info: vec![],
                undoes: None,
            }))
        };
        let end = || {
            EventBody::End(End {
                ok: true,
                outcome: String::new(),
                after: None,
            })
        };
        let mut ops = fold(
            ["NEW", "OLD", "EDIT", "GONE"]
                .iter()
                .flat_map(|id| [event(id, begin()), event(id, end())]),
        );
        let digests: std::collections::HashMap<String, String> = [
            ("NEW".to_string(), ops[0].digest()),
            ("OLD".to_string(), ops[1].digest_v1()),
            ("EDIT".to_string(), ops[2].digest()),
        ]
        .into();
        // The scope of EDIT is changed after its digest was recorded.
        ops[2].begin.scope.bootstrap = "prod:9092".into();
        let check = history_check(&ops, &digests);
        assert_eq!(
            check,
            HistoryCheck {
                verified: 1,
                legacy: vec!["OLD".into()],
                missing: 1,
                edited: vec!["EDIT".into()],
            }
        );
    }
}
