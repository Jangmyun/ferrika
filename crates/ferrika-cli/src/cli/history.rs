//! `ferrika history` and `ferrika undo`: list the recorded cluster changes
//! and put one back. See `crates/ferrika-ops/src/history.rs`.

use clap::{Args, Subcommand};
use colored::Colorize;
use comfy_table::{Cell, Color};
use serde_json::json;

use crate::cli::offset::confirm;
use crate::cli::record::{self, Change};
use crate::cli::{output, print_connection_hint, GlobalOpts, OutputFormat, WriteContext};
use crate::error::{KafkaError, KafkaResult};
use crate::kafka::admin::KafkaAdminClient;
use crate::model::format::redact_url;
use crate::model::history::{Operation, Scope};
use crate::ops::history::apply::Target;
use crate::ops::history::conflict::{DriftRow, Verdict};
use crate::ops::history::undo::{self, DigestCheck, UndoPlan};
use crate::ops::history::Journal;

#[derive(Args, Debug, Clone)]
pub struct HistoryArgs {
    #[command(subcommand)]
    pub command: Option<HistoryCommand>,
    /// Number of operations to show (newest first)
    #[arg(short = 'n', long, default_value_t = 30, value_parser = parse_limit)]
    pub limit: usize,
    /// Every cluster, not only the current profile's
    #[arg(long)]
    pub all: bool,
    /// Only operations whose resources or target contain this text,
    /// e.g. `topic:orders` or `group:billing`
    #[arg(long, value_name = "TEXT")]
    pub resource: Option<String>,
    /// Only operations that can still be undone
    #[arg(long)]
    pub undoable: bool,
}

#[derive(Subcommand, Debug, Clone, PartialEq, Eq)]
pub enum HistoryCommand {
    /// One operation in full: before and after values, outcome, what undo
    /// would do and what it cannot restore
    Show {
        /// Operation id (at least 6 characters)
        id: String,
    },
}

#[derive(Args, Debug, Clone)]
pub struct UndoArgs {
    /// Operation id (at least 6 characters); the newest operation you ran on
    /// this cluster that can be undone when left out
    pub id: Option<String>,
    /// Show what undo would do and whether it would be refused, changing nothing
    #[arg(long)]
    pub dry_run: bool,
    /// Undo even though the state changed since, or later operations
    /// touched the same resources (overwrites those changes)
    #[arg(long)]
    pub force: bool,
    /// Do not ask for confirmation
    #[arg(short = 'y', long)]
    pub yes: bool,
    /// Undo an entry recorded by Ferrika 0.1 (legacy digest, which does not
    /// cover the cluster it ran on) without asking again
    #[arg(long)]
    pub accept_legacy: bool,
}

/// Where this run is connected: the profile's cluster (with its id when the
/// brokers answer), Schema Registry and Kafka Connect.
fn current_scope(global: &GlobalOpts, admin: Option<&KafkaAdminClient>) -> Scope {
    let config = global.to_kafka_config();
    Scope {
        registry: redact_url(config.schema_registry_url.as_deref().unwrap_or_default()),
        connect: redact_url(config.connect_url.as_deref().unwrap_or_default()),
        ..record::cluster_scope(global, admin)
    }
}

/// Whether an operation recorded in `recorded` ran where `current` points.
fn in_scope(recorded: &Scope, current: &Scope) -> bool {
    recorded.same_cluster(current)
}

/// `-n`: how many operations to list; at least 1, so a typo cannot report
/// "No recorded operations" about a log that has some.
fn parse_limit(raw: &str) -> Result<usize, String> {
    match raw.trim().parse::<usize>() {
        Ok(0) => Err("must be at least 1".to_string()),
        Ok(n) => Ok(n),
        Err(_) => Err(format!("'{raw}' is not a positive whole number")),
    }
}

pub async fn run_history(args: HistoryArgs, global: &GlobalOpts) -> KafkaResult<()> {
    let ops = Journal::default().load();
    let audited = crate::governance::audit::op_digests();
    let check = |op: &Operation| undo::digest_check(op, audited.get(&op.id).map(String::as_str));
    if let Some(HistoryCommand::Show { id }) = &args.command {
        let op = undo::find(&ops, id).map_err(KafkaError::config)?;
        return show(op, check(op), global);
    }
    let scope = (!args.all).then(|| {
        let admin = KafkaAdminClient::new(&global.to_kafka_config()).ok();
        current_scope(global, admin.as_ref())
    });
    let selected = select(&ops, scope.as_ref(), &args);
    if global.format == OutputFormat::Json {
        let items: Vec<_> = selected
            .iter()
            .map(|op| summary_json(op, check(op)))
            .collect();
        return output::print_json(&items);
    }
    if selected.is_empty() {
        println!(
            "No recorded operations{} ({}).",
            if args.all {
                ""
            } else {
                " on this cluster (try --all)"
            },
            Journal::default()
                .path()
                .display()
                .to_string()
                .bright_black()
        );
        return Ok(());
    }
    let mut table = output::table(vec![
        "ID",
        "TIME (UTC)",
        "USER",
        "FROM",
        "ACTION",
        "TARGET",
        "UNDO",
        "RESULT",
        "AUDIT",
    ]);
    let mut legacy = false;
    for op in &selected {
        let digest = check(op);
        legacy |= digest == DigestCheck::Legacy;
        table.add_row(vec![
            Cell::new(&op.id),
            Cell::new(crate::model::format::format_timestamp_ms(op.time_ms)),
            Cell::new(&op.begin.user),
            Cell::new(&op.begin.source),
            Cell::new(&op.begin.action),
            Cell::new(&op.begin.target),
            Cell::new(op.undo_label()),
            Cell::new(op.result_label()).fg(match op.ok() {
                Some(true) => Color::Green,
                Some(false) => Color::Red,
                None => Color::Yellow,
            }),
            digest_cell(digest),
        ]);
    }
    println!("{table}");
    if legacy {
        println!(
            "{}",
            "legacy digest: recorded by Ferrika 0.1; the audit log covers only the before- and \
             after-state, not the cluster, action or undo link, so `ferrika undo` shows the \
             cluster and asks again (--accept-legacy)."
                .bright_black()
        );
    }
    Ok(())
}

/// The AUDIT column: how the entry compares with the audit log.
fn digest_cell(digest: DigestCheck) -> Cell {
    let cell = Cell::new(digest.label());
    match digest {
        DigestCheck::Matches => cell.fg(Color::Green),
        DigestCheck::Legacy | DigestCheck::Missing => cell.fg(Color::Yellow),
        DigestCheck::Mismatch => cell.fg(Color::Red),
    }
}

/// The operations `args` asks for, newest first.
fn select<'a>(
    ops: &'a [Operation],
    scope: Option<&Scope>,
    args: &HistoryArgs,
) -> Vec<&'a Operation> {
    ops.iter()
        .rev()
        .filter(|op| scope.is_none_or(|scope| in_scope(&op.begin.scope, scope)))
        .filter(|op| {
            args.resource.as_deref().is_none_or(|text| {
                op.begin.target.contains(text)
                    || op.resource_keys().iter().any(|k| k.contains(text))
            })
        })
        .filter(|op| !args.undoable || undo::undoable(op))
        .take(args.limit)
        .collect()
}

fn summary_json(op: &Operation, digest: DigestCheck) -> serde_json::Value {
    json!({
        "digest": digest,
        "id": op.id,
        "time_ms": op.time_ms,
        "user": op.begin.user,
        "source": op.begin.source,
        "scope": op.begin.scope,
        "action": op.begin.action,
        "target": op.begin.target,
        "reversibility": op.begin.reversibility,
        "result": op.result_label(),
        "outcome": op.end.as_ref().map(|e| e.outcome.clone()),
        "undone_by": op.undone_by,
        "undoes": op.begin.undoes,
    })
}

/// Masks a value for display when the company policy enforces masking.
fn shown(value: &str) -> String {
    if crate::governance::policy::current().masking_enforced() {
        crate::codec::pii::mask_text(value, crate::codec::pii::MaskStrategy::Redact)
    } else {
        value.to_string()
    }
}

fn show(op: &Operation, digest: DigestCheck, global: &GlobalOpts) -> KafkaResult<()> {
    if global.format == OutputFormat::Json {
        let mut value = summary_json(op, digest);
        value["limits"] = json!(op.begin.limits);
        value["info"] = json!(op.begin.info);
        value["notes"] = json!(op.notes);
        value["states"] = json!(op.state_rows());
        return output::print_json(&value);
    }
    println!("{} {}", "Operation".bold(), op.id.cyan().bold());
    println!(
        "  {} {} by {} ({})",
        crate::model::format::format_timestamp_ms(op.time_ms),
        op.begin.scope.describe(),
        op.begin.user,
        op.begin.source
    );
    println!("  {}: {}", op.begin.action.bold(), op.begin.target);
    match &op.end {
        Some(end) => println!(
            "  result: {}",
            if end.ok {
                end.outcome.green()
            } else {
                end.outcome.red()
            }
        ),
        None => println!("  result: {}", "unknown (no result was recorded)".yellow()),
    }
    println!("  undo: {}", op.undo_label());
    println!("  audit: {}", digest.label());
    for limit in &op.begin.limits {
        println!("  {} {limit}", "limit:".yellow());
    }
    for line in &op.begin.info {
        println!("  {} {line}", "info:".bright_black());
    }
    for note in &op.notes {
        println!("  {} {note}", "note:".bright_black());
    }
    let rows = op.state_rows();
    if !rows.is_empty() {
        let mut table = output::table(vec!["RESOURCE", "ITEM", "BEFORE", "AFTER"]);
        for row in rows {
            table.add_row(vec![
                Cell::new(row.subject),
                Cell::new(row.item),
                Cell::new(shown(&row.before)),
                Cell::new(shown(&row.after)),
            ]);
        }
        println!("{table}");
    }
    Ok(())
}

pub async fn run_undo(args: UndoArgs, global: &GlobalOpts) -> KafkaResult<()> {
    let ops = Journal::default().load();
    let config = global.to_kafka_config();
    let admin = match KafkaAdminClient::new(&config) {
        Ok(admin) => admin,
        Err(err) => {
            print_connection_hint(&global.to_kafka_config(), &err);
            return Err(err);
        }
    };
    let scope = current_scope(global, Some(&admin));
    let op = match &args.id {
        Some(id) => undo::find(&ops, id).map_err(KafkaError::config)?,
        None => undo::latest_undoable(&ops, &scope, &crate::governance::audit::os_user()).ok_or_else(
            || KafkaError::config("Nothing to undo: you have no undoable operation on this cluster (see `ferrika history`)"),
        )?,
    };
    let policy = crate::governance::policy::current();
    if let Some(by) = &op.undone_by {
        // The `undoes` link is the journal's word; only a version 2 digest
        // in the audit log vouches for it.
        let undoer = ops.iter().find(|o| &o.id == by);
        let audited_by = crate::governance::audit::op_digest(by);
        let trusted = undoer.is_some_and(|undoer| {
            undo::undo_mark_trusted(undoer, audited_by.as_deref(), policy.is_active())
        });
        if trusted {
            return Err(KafkaError::config(format!(
                "Operation {} was already undone by {by}; undo {by} to apply it again",
                op.id
            )));
        }
        let check = undoer.map(|undoer| undo::digest_check(undoer, audited_by.as_deref()));
        if let (Some(undoer), Some(DigestCheck::Mismatch)) = (undoer, check) {
            record_tampered(
                global,
                undoer,
                "the history entry does not match the audit log's op_digest; its undo link is ignored",
            );
        }
        eprintln!(
            "{} the history says {by} undid {}, but the audit log does not vouch for that ({}); \
             ignoring it, the current state decides.",
            "warning:".yellow().bold(),
            op.id,
            check.map_or("no such entry", DigestCheck::label)
        );
    }
    if op.ok() == Some(false) {
        return Err(KafkaError::config(format!(
            "Operation {} failed, so there is nothing to undo",
            op.id
        )));
    }
    let audited = crate::governance::audit::op_digest(&op.id);
    let plan = undo::prepare(op, &ops, &scope, audited.as_deref(), &config, &admin).await?;
    let refusal = plan.refusal(args.force, policy.is_active());

    let json_out = global.format == OutputFormat::Json;
    if json_out && (args.dry_run || refusal.is_some()) {
        output::print_json(&plan_json(&plan, refusal.as_deref(), None))?;
    }
    if json_out && args.dry_run {
        // A refused undo fails a dry run too, with the same exit code.
        return match refusal {
            Some(reason) => Err(KafkaError::config(format!(
                "Cannot undo {}: {reason}",
                op.id
            ))),
            None => Ok(()),
        };
    }
    if !json_out {
        describe_plan(op, &plan);
    }

    if let Some(reason) = refusal {
        if plan.digest == DigestCheck::Mismatch {
            record_tampered(
                global,
                op,
                "the history entry does not match the audit log's op_digest; undo refused",
            );
        }
        for line in &op.begin.info {
            eprintln!("{} {line}", "info:".bright_black());
        }
        // A dry run shows the plan above and fails like the real undo
        // would, with the same exit code.
        return Err(KafkaError::config(format!(
            "Cannot undo {}: {reason}",
            op.id
        )));
    }
    if plan.digest == DigestCheck::Missing {
        eprintln!(
            "{} the audit log has no entry for {}, so the history entry cannot be checked against it.",
            "warning:".yellow().bold(),
            op.id
        );
    }
    if plan.digest == DigestCheck::Legacy {
        eprintln!(
            "{} {} has a legacy digest (scope not covered): it was recorded by Ferrika 0.1, \
             whose audit digest covers only the before- and after-state, not the cluster, \
             action or undo link. The history says it ran on {}; you are connected to {}.",
            "warning:".yellow().bold(),
            op.id,
            op.begin.scope.describe(),
            scope.describe()
        );
    }
    if plan.runnable_steps().is_empty() {
        if json_out {
            return output::print_json(&plan_json(&plan, None, Some("nothing to undo")));
        }
        println!(
            "{} {} is already in its previous state; nothing to undo.",
            "✔".green().bold(),
            op.id
        );
        return Ok(());
    }
    if args.dry_run {
        println!(
            "\n{} {}",
            "💡 [DRY-RUN MODE]".yellow().bold(),
            "Nothing was changed. Run without --dry-run to undo.".bright_white()
        );
        return Ok(());
    }

    if plan.digest == DigestCheck::Legacy && !args.accept_legacy {
        if args.yes {
            return Err(KafkaError::config(format!(
                "Cannot undo {}: its history entry has a legacy digest that does not cover the \
                 cluster; check that {} is the right cluster and pass --accept-legacy",
                op.id,
                scope.describe()
            )));
        }
        confirm(
            &format!(
                "Undo the legacy entry {} on {}? [y/N]: ",
                op.id,
                scope.describe().bold()
            ),
            "Aborted undo.",
        )?;
    }

    let target = format!("{} {} (op {})", op.begin.action, op.begin.target, op.id);
    // The undo's own operation id, named on its audit request entries.
    let undo_op = crate::model::history::new_op_id(crate::kafka::clock::now_ms());
    authorize(global, &plan, &target, &undo_op)?;
    if !args.yes {
        confirm(
            &format!(
                "Undo {} {}? [y/N]: ",
                op.begin.action,
                op.begin.target.bold()
            ),
            "Aborted undo.",
        )?;
    }

    let mut global = global.clone();
    global.write = Some(WriteContext {
        label: "undo".to_string(),
        target,
        op: Some(undo_op),
    });
    let forced = args.force && matches!(plan.verdict, Verdict::Drift { .. });
    let change = Change {
        reversibility: plan.redo,
        limits: plan.redo_limits.clone(),
        before: plan.current.clone(),
        after: op.begin.before.clone(),
        info: Vec::new(),
    };
    let pending = record::start_with(
        &global,
        op.begin.scope.clone(),
        change,
        None,
        Some(op.id.clone()),
    );
    // The failure as data; its text is what is shown and recorded.
    let result = undo::execute_steps(
        &plan,
        &Target {
            config: &config,
            admin: &admin,
            op: &op.id,
        },
    )
    .await
    .map_err(|failure| failure.to_string());
    let outcome = |text: &str| {
        if forced {
            format!("{text} (forced: state changed since)")
        } else {
            text.to_string()
        }
    };
    if let Some(pending) = pending {
        match &result {
            Ok(text) => pending.finish(true, &outcome(text), None),
            Err(e) => pending.finish(false, &outcome(e), None),
        }
    }
    match result {
        Ok(text) if json_out => output::print_json(&plan_json(&plan, None, Some(&text))),
        Ok(text) => {
            println!("{} {text}", "✔".green().bold());
            Ok(())
        }
        Err(e) => Err(KafkaError::admin(format!("Undo of {} failed: {e}", op.id))),
    }
}

/// The undo plan as `-f json` prints it: the plan's fields, `refused` (the
/// reason, or null) and `result`, the outcome text once the undo ran or
/// found nothing to do (null for a dry run or a refusal).
fn plan_json<T: serde::Serialize>(
    plan: &T,
    refusal: Option<&str>,
    result: Option<&str>,
) -> serde_json::Value {
    let mut value = serde_json::to_value(plan).unwrap_or_default();
    if let Some(obj) = value.as_object_mut() {
        obj.insert("refused".into(), json!(refusal));
        obj.insert("result".into(), json!(result));
    }
    value
}

/// Runs the write gate once for each policy category the undo steps fall
/// under (undoing a topic creation is a topic deletion); any refusal
/// refuses the whole undo.
fn authorize(global: &GlobalOpts, plan: &UndoPlan, target: &str, op: &str) -> KafkaResult<()> {
    let mut actions: Vec<_> = plan
        .runnable_steps()
        .iter()
        .map(|s| s.policy_action())
        .collect();
    actions.sort_by_key(|a| a.id());
    actions.dedup();
    let profile = global.resolved_profile_name().unwrap_or_default();
    let bootstrap = global.resolved_bootstrap_servers();
    for action in actions {
        let request = crate::governance::policy::WriteRequest {
            source: "cli",
            action,
            label: "undo",
            target,
            profile: &profile,
            bootstrap: &bootstrap,
            actor: None,
            op: Some(op),
        };
        crate::governance::policy::authorize_write(
            &request,
            crate::governance::policy::AuditMode::RecordRequest,
            || global.ensure_writable("undo").map_err(|e| e.to_string()),
        )
        .map_err(|refusal| KafkaError::config(refusal.to_string()))?;
    }
    Ok(())
}

/// Records an edited history entry found by undo as `history.tampered`.
fn record_tampered(global: &GlobalOpts, op: &Operation, outcome: &str) {
    crate::governance::audit::record(&crate::governance::audit::AuditEntry {
        time_ms: crate::governance::audit::now_ms(),
        source: "cli".to_string(),
        profile: global
            .resolved_profile_name()
            .unwrap_or_else(|| global.resolved_bootstrap_servers()),
        action: "history.tampered".to_string(),
        target: format!("op {}", op.id),
        outcome: outcome.to_string(),
        ok: false,
        user: String::new(),
        actor: None,
        op: op.id.clone(),
        op_digest: String::new(),
    });
}

/// Prints what undo found and would do.
fn describe_plan(op: &Operation, plan: &UndoPlan) {
    println!(
        "{} {} — {} {} ({})",
        "Undo".bold(),
        op.id.cyan().bold(),
        op.begin.action,
        op.begin.target,
        crate::model::format::format_timestamp_ms(op.time_ms)
    );
    match &plan.verdict {
        Verdict::Clean => println!("  state: {}", "unchanged since the operation".green()),
        Verdict::AlreadyReverted => {
            println!(
                "  state: {}",
                "already back to before the operation".green()
            )
        }
        Verdict::Drift { rows } if !rows.is_empty() => {
            println!("  state: {}", "changed since the operation".yellow());
            print_drift(rows);
        }
        Verdict::Drift { .. } => {}
    }
    for limit in &op.begin.limits {
        println!("  {} {limit}", "limit:".yellow());
    }
    if let Ok(steps) = &plan.steps {
        for step in steps {
            println!("  • {}", step.describe());
        }
    }
    for blocker in &plan.blockers {
        println!("  {} {blocker}", "blocked:".red().bold());
    }
    if !plan.later.is_empty() {
        println!(
            "  {} later operation(s) on the same resources: {}",
            "note:".yellow(),
            plan.later.join(", ")
        );
    }
    if plan.redo != crate::model::history::Reversibility::Reversible && plan.steps.is_ok() {
        for limit in &plan.redo_limits {
            println!(
                "  {} this undo cannot itself be undone: {limit}",
                "note:".yellow()
            );
        }
    }
}

fn print_drift(rows: &[DriftRow]) {
    let mut table = output::table(vec!["RESOURCE", "ITEM", "BEFORE", "RECORDED", "CURRENT"]);
    for row in rows {
        table.add_row(vec![
            Cell::new(&row.subject),
            Cell::new(&row.item),
            Cell::new(shown(&row.before)),
            Cell::new(shown(&row.recorded)),
            Cell::new(shown(&row.current)).fg(Color::Yellow),
        ]);
    }
    println!("{table}");
}

#[cfg(test)]
mod tests {
    #[test]
    fn undo_json_adds_refusal_and_result_to_the_plan() {
        let plan = serde_json::json!({"op": "op-1", "steps": []});
        let v = super::plan_json(&plan, Some("state drifted"), None);
        assert_eq!(v["op"], "op-1");
        assert_eq!(v["refused"], "state drifted");
        assert!(v["result"].is_null());
        let v = super::plan_json(&plan, None, Some("Deleted topic t"));
        assert!(v["refused"].is_null());
        assert_eq!(v["result"], "Deleted topic t");
    }

    use super::*;
    use crate::model::history::{Begin, End, Reversibility};

    fn op(id: &str, bootstrap: &str, target: &str, ok: bool) -> Operation {
        Operation {
            id: id.into(),
            time_ms: 1,
            begin: Begin {
                source: "cli".into(),
                user: "u".into(),
                scope: Scope {
                    bootstrap: bootstrap.into(),
                    ..Scope::default()
                },
                action: "topics config".into(),
                target: target.into(),
                reversibility: Reversibility::Reversible,
                limits: vec![],
                before: vec![],
                after: vec![],
                info: vec![],
                undoes: None,
            },
            end: Some(End {
                ok,
                outcome: String::new(),
                after: None,
            }),
            ended_ms: None,
            notes: vec![],
            undone_by: None,
        }
    }

    #[test]
    fn history_limit_must_be_positive() {
        assert_eq!(parse_limit("30"), Ok(30));
        assert!(parse_limit("0").is_err());
        assert!(parse_limit("-1").is_err());
        assert!(parse_limit("x").is_err());
    }

    fn args(all: bool, resource: Option<&str>, undoable: bool, limit: usize) -> HistoryArgs {
        HistoryArgs {
            command: None,
            limit,
            all,
            resource: resource.map(str::to_string),
            undoable,
        }
    }

    #[test]
    fn history_filters_by_cluster_resource_and_undoability_newest_first() {
        let ops = vec![
            op("A", "a:9092", "orders: retention.ms", true),
            op("B", "b:9092", "orders: retention.ms", true),
            op("C", "a:9092", "payments: retention.ms", false),
            op("D", "a:9092", "orders: cleanup.policy", true),
        ];
        let here = Scope {
            bootstrap: "a:9092".into(),
            ..Scope::default()
        };
        let ids = |selected: Vec<&Operation>| -> Vec<String> {
            selected.iter().map(|o| o.id.clone()).collect()
        };
        assert_eq!(
            ids(select(&ops, Some(&here), &args(false, None, false, 30))),
            vec!["D", "C", "A"]
        );
        assert_eq!(
            ids(select(&ops, None, &args(true, None, false, 2))),
            vec!["D", "C"]
        );
        assert_eq!(
            ids(select(
                &ops,
                Some(&here),
                &args(false, Some("orders"), true, 30)
            )),
            vec!["D", "A"]
        );
    }

    #[test]
    fn undo_column_names_the_undo() {
        let mut undone = op("A", "a", "t", true);
        undone.undone_by = Some("U".into());
        assert_eq!(undone.undo_label(), "undone by U");
        assert_eq!(op("B", "a", "t", false).undo_label(), "-");
        let mut redo = op("C", "a", "t", true);
        redo.begin.undoes = Some("A".into());
        assert_eq!(redo.undo_label(), "yes (undo of A)");
    }
}
