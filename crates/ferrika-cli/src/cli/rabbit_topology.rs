//! `ferrika rabbit queue|exchange|bind|unbind|history|undo`: RabbitMQ
//! topology changes through the management API, recorded in the operation
//! history (`ferrika_ops::history::rabbit`) so `rabbit undo` can reverse
//! them.
//!
//! `main` has already passed the write gate for the change (categories
//! `queues.create`, `queues.delete`, `queues.purge`, `exchanges.create`,
//! `exchanges.delete`, `bindings.change`) on the RabbitMQ profile and the
//! broker's addresses, and made the operation id its audit request entry
//! names. Each handler reads the state it is about to change, starts the
//! record, runs the change and finishes the record with the state read
//! back, which writes the audit result entry with the digest. `rabbit undo`
//! runs the gate itself, once per category of its steps, like `ferrika
//! undo`.

use std::future::Future;

use colored::Colorize;
use serde_json::{json, Map, Value};

use crate::cli::history::{self as kafka_history, HistoryArgs};
use crate::cli::offset::confirm;
use crate::cli::rabbit::{
    gate_address, gate_profile, BindingDestinationType, RabbitArgs, RabbitBindingArgs,
    RabbitExchangeSubcommands, RabbitHistoryArgs, RabbitHistoryCommand, RabbitQueueSubcommands,
    RabbitUndoArgs, ResolvedRabbit,
};
use crate::cli::record::{self, Change};
use crate::cli::{output, GlobalOpts, WriteContext};
use crate::model::history::Scope;
use crate::ops::history::rabbit as history;
use crate::ops::history::undo::{self, DigestCheck, UndoOptions, UndoPlan, UndoRefusal};
use crate::ops::history::Journal;
use crate::rabbitmq::client::ManagementClient;
use crate::rabbitmq::error::RabbitResult;
use crate::rabbitmq::model::{
    exchange_name, validate_name, BindingDeclaration, DestinationType, ExchangeDeclaration,
    QueueDeclaration,
};

/// One `rabbit` topology command's connection and options.
pub(crate) struct Context<'a> {
    pub args: &'a RabbitArgs,
    pub global: &'a GlobalOpts,
    pub client: &'a ManagementClient,
    pub resolved: &'a ResolvedRabbit,
    pub json: bool,
}

/// `KEY=VALUE` arguments of a declaration or binding as JSON
/// ([`crate::rabbitmq::model::parse_arguments`]).
pub(crate) fn parse_arguments(items: &[String]) -> Result<Map<String, Value>, String> {
    crate::rabbitmq::model::parse_arguments(items).map_err(|e| match e {
        crate::rabbitmq::error::RabbitError::Config(message) => message,
        other => other.to_string(),
    })
}

/// The binding `args` names ([`BindingDeclaration::parse`]).
pub(crate) fn binding_of(args: &RabbitBindingArgs) -> Result<BindingDeclaration, String> {
    let destination_type = match args.destination_type {
        BindingDestinationType::Queue => DestinationType::Queue,
        BindingDestinationType::Exchange => DestinationType::Exchange,
    };
    BindingDeclaration::parse(
        &args.source,
        &args.destination,
        destination_type,
        &args.routing_key,
        &args.arguments,
    )
    .map_err(|e| e.to_string())
}

fn msg(error: impl std::fmt::Display) -> anyhow::Error {
    anyhow::Error::msg(error.to_string())
}

impl Context<'_> {
    fn vhost(&self) -> &str {
        &self.resolved.vhost
    }

    /// Where the change runs, for the history: the RabbitMQ profile and
    /// the management `host:port`.
    fn scope(&self) -> anyhow::Result<Scope> {
        let address = self.resolved.address().map_err(anyhow::Error::msg)?;
        Ok(history::scope(&gate_profile(self.args), &address))
    }

    /// The profile the audit entries name: the RabbitMQ profile, else the
    /// addresses the gate matched (as the request entry names them).
    fn audit_profile(&self) -> String {
        let profile = gate_profile(self.args);
        if profile.is_empty() {
            gate_address(self.args).unwrap_or_default()
        } else {
            profile
        }
    }

    /// Writes the audit result entry of a change refused before anything
    /// was changed (so its request entry does not stand alone), and returns
    /// the error.
    fn refused(&self, reason: impl std::fmt::Display) -> anyhow::Error {
        let reason = reason.to_string();
        if let Some(write) = &self.global.write {
            crate::governance::audit::record(&crate::governance::audit::AuditEntry {
                time_ms: crate::governance::audit::now_ms(),
                source: "cli".to_string(),
                profile: self.audit_profile(),
                action: write.label.clone(),
                target: write.target.clone(),
                outcome: reason.clone(),
                ok: false,
                user: String::new(),
                actor: None,
                op: write.op.clone().unwrap_or_default(),
                op_digest: String::new(),
            });
        }
        anyhow::Error::msg(reason)
    }

    /// Records `change`, runs `action` and finishes the record with the
    /// state read back (or the error).
    async fn recorded<T>(
        &self,
        change: Change,
        action: impl Future<Output = RabbitResult<T>>,
        success: &str,
    ) -> anyhow::Result<T> {
        let pending = record::start_rabbit(
            self.global,
            self.scope()?,
            change,
            self.audit_profile(),
            None,
        );
        let result = action.await;
        if let Some(pending) = pending {
            let sink = crate::governance::policy::AuditLog;
            if let Err(e) =
                history::finish_record(pending.record, &sink, self.client, &result, success).await
            {
                tracing::warn!(%e, "Could not write the audit log");
            }
        }
        result.map_err(msg)
    }

    fn done(&self, text: String, value: Value) -> anyhow::Result<()> {
        if self.json {
            output::print_json(&value)?;
        } else {
            println!("{} {text}", "✔".green().bold());
        }
        Ok(())
    }

    pub async fn queue(&self, command: &RabbitQueueSubcommands) -> anyhow::Result<()> {
        let vhost = self.vhost();
        match command {
            RabbitQueueSubcommands::Declare {
                name,
                kind,
                transient,
                auto_delete,
                arguments,
            } => {
                validate_name("queue", name).map_err(|e| self.refused(e))?;
                let declaration = QueueDeclaration {
                    kind: kind.clone(),
                    durable: !transient,
                    auto_delete: *auto_delete,
                    arguments: parse_arguments(arguments).map_err(|e| self.refused(e))?,
                };
                let change = history::queue_declared(self.client, vhost, name, &declaration)
                    .await
                    .map_err(|e| self.refused(e))?;
                self.recorded(
                    change,
                    self.client.declare_queue(vhost, name, &declaration),
                    &format!("Declared queue {name}"),
                )
                .await?;
                let kind = self
                    .client
                    .queue(vhost, name)
                    .await
                    .ok()
                    .flatten()
                    .map(|q| q.kind)
                    .unwrap_or_default();
                self.done(
                    format!(
                        "Declared {} queue '{}' on vhost '{vhost}'.",
                        if kind.is_empty() { "a" } else { &kind },
                        name.cyan()
                    ),
                    json!({"queue": name, "vhost": vhost, "type": kind, "declared": true}),
                )
            }
            RabbitQueueSubcommands::Delete {
                name,
                if_empty,
                if_unused,
                yes,
            } => {
                validate_name("queue", name).map_err(|e| self.refused(e))?;
                let queue = self
                    .client
                    .queue(vhost, name)
                    .await
                    .map_err(|e| self.refused(e))?
                    .ok_or_else(|| {
                        self.refused(format!("Queue '{name}' does not exist on vhost '{vhost}'"))
                    })?;
                let messages = queue.messages.unwrap_or(0);
                if !*yes {
                    confirm(
                        &format!(
                            "Delete queue '{}' on vhost '{vhost}' with its messages (about \
                             {messages}) and its bindings? `ferrika rabbit undo` declares it \
                             again, empty. [y/N]: ",
                            name.red().bold()
                        ),
                        "Aborted queue deletion.",
                    )
                    .map_err(|e| self.refused(e))?;
                }
                let change = history::queue_deleted(self.client, vhost, name)
                    .await
                    .map_err(|e| self.refused(e))?;
                self.recorded(
                    change,
                    self.client.delete_queue(vhost, name, *if_empty, *if_unused),
                    &format!("Deleted queue {name} (the broker reported {messages} message(s))"),
                )
                .await?;
                self.done(
                    format!(
                        "Deleted queue '{}' on vhost '{vhost}' (the broker reported {messages} \
                         message(s)).",
                        name.cyan()
                    ),
                    json!({"queue": name, "vhost": vhost, "deleted": true, "messages": messages}),
                )
            }
            RabbitQueueSubcommands::Purge { name, yes } => {
                validate_name("queue", name).map_err(|e| self.refused(e))?;
                let change = history::queue_purged(self.client, vhost, name)
                    .await
                    .map_err(|e| self.refused(e))?;
                let messages = change
                    .before
                    .iter()
                    .find_map(|s| match s {
                        crate::model::history::State::RabbitQueueMessages { messages, .. } => {
                            *messages
                        }
                        _ => None,
                    })
                    .unwrap_or(0);
                if !*yes {
                    confirm(
                        &format!(
                            "Delete every ready message (about {messages}) of queue '{}' on \
                             vhost '{vhost}'? {} [y/N]: ",
                            name.red().bold(),
                            record::CANNOT_UNDO
                        ),
                        "Aborted purge.",
                    )
                    .map_err(|e| self.refused(e))?;
                }
                self.recorded(
                    change,
                    self.client.purge_queue(vhost, name),
                    &format!("Purged queue {name} (the broker reported {messages} message(s))"),
                )
                .await?;
                self.done(
                    format!(
                        "Purged queue '{}' on vhost '{vhost}' (the broker reported {messages} \
                         message(s)).",
                        name.cyan()
                    ),
                    json!({"queue": name, "vhost": vhost, "purged": messages}),
                )
            }
        }
    }

    pub async fn exchange(&self, command: &RabbitExchangeSubcommands) -> anyhow::Result<()> {
        let vhost = self.vhost();
        match command {
            RabbitExchangeSubcommands::Declare {
                name,
                kind,
                transient,
                auto_delete,
                internal,
                arguments,
            } => {
                validate_name("exchange", name).map_err(|e| self.refused(e))?;
                let declaration = ExchangeDeclaration {
                    kind: kind.clone(),
                    durable: !transient,
                    auto_delete: *auto_delete,
                    internal: *internal,
                    arguments: parse_arguments(arguments).map_err(|e| self.refused(e))?,
                };
                let change = history::exchange_declared(self.client, vhost, name, &declaration)
                    .await
                    .map_err(|e| self.refused(e))?;
                self.recorded(
                    change,
                    self.client.declare_exchange(vhost, name, &declaration),
                    &format!("Declared exchange {name}"),
                )
                .await?;
                self.done(
                    format!(
                        "Declared {kind} exchange '{}' on vhost '{vhost}'.",
                        name.cyan()
                    ),
                    json!({"exchange": name, "vhost": vhost, "type": kind, "declared": true}),
                )
            }
            RabbitExchangeSubcommands::Delete {
                name,
                if_unused,
                yes,
            } => {
                validate_name("exchange", name).map_err(|e| self.refused(e))?;
                let change = history::exchange_deleted(self.client, vhost, name)
                    .await
                    .map_err(|e| self.refused(e))?;
                let bindings = change.before.len().saturating_sub(1);
                if !*yes {
                    confirm(
                        &format!(
                            "Delete exchange '{}' on vhost '{vhost}' and its {bindings} \
                             binding(s)? Messages published to it meanwhile are dropped. \
                             `ferrika rabbit undo` restores both. [y/N]: ",
                            name.red().bold()
                        ),
                        "Aborted exchange deletion.",
                    )
                    .map_err(|e| self.refused(e))?;
                }
                self.recorded(
                    change,
                    self.client.delete_exchange(vhost, name, *if_unused),
                    &format!("Deleted exchange {name} with {bindings} binding(s)"),
                )
                .await?;
                self.done(
                    format!(
                        "Deleted exchange '{}' on vhost '{vhost}' ({bindings} binding(s) removed).",
                        name.cyan()
                    ),
                    json!({"exchange": name, "vhost": vhost, "deleted": true, "bindings": bindings}),
                )
            }
        }
    }

    pub async fn binding(&self, args: &RabbitBindingArgs, add: bool) -> anyhow::Result<()> {
        let vhost = self.vhost();
        let binding = binding_of(args).map_err(|e| self.refused(e))?;
        let change = history::binding_changed(self.client, vhost, &binding, add)
            .await
            .map_err(|e| self.refused(e))?;
        let present = matches!(
            change.before.first(),
            Some(crate::model::history::State::RabbitBinding { present: true, .. })
        );
        if !add && !present {
            return Err(self.refused(format!(
                "There is no binding {} on vhost '{vhost}'; nothing was changed",
                binding.label()
            )));
        }
        let label = binding.label();
        if add {
            self.recorded(
                change,
                self.client.bind(vhost, &binding),
                &format!("Bound {label}"),
            )
            .await?;
        } else {
            self.recorded(
                change,
                async {
                    match self.client.unbind(vhost, &binding).await {
                        Ok(true) => Ok(()),
                        Ok(false) => Err(crate::rabbitmq::error::RabbitError::config(
                            "the binding was removed meanwhile",
                        )),
                        Err(e) => Err(e),
                    }
                },
                &format!("Unbound {label}"),
            )
            .await?;
        }
        let verb = if add { "Bound" } else { "Unbound" };
        self.done(
            format!(
                "{verb} {} on vhost '{vhost}'{}.",
                label,
                if add && present {
                    " (it existed already)"
                } else {
                    ""
                }
            ),
            json!({
                "source": exchange_name(&binding.source),
                "destination": binding.destination,
                "destination_type": binding.destination_type.as_str(),
                "routing_key": binding.routing_key,
                "vhost": vhost,
                "bound": add,
                "changed": add != present,
            }),
        )
    }

    pub fn history(&self, args: &RabbitHistoryArgs) -> anyhow::Result<()> {
        let ops = Journal::default().load();
        if let Some(RabbitHistoryCommand::Show { id }) = &args.command {
            return Ok(kafka_history::show_op(&ops, id, self.global)?);
        }
        let scope = if args.all { None } else { Some(self.scope()?) };
        let list = HistoryArgs {
            command: None,
            limit: args.limit,
            all: args.all,
            resource: args.resource.clone(),
            undoable: args.undoable,
        };
        Ok(kafka_history::list(
            &ops,
            scope.as_ref(),
            &list,
            self.global,
        )?)
    }

    pub async fn undo(&self, args: &RabbitUndoArgs) -> anyhow::Result<()> {
        // A read-only RabbitMQ profile can never undo: say so before
        // reading the journal and the broker (the gate checks it again).
        if !args.dry_run {
            if let Err(reason) = crate::cli::rabbit::ensure_writable(self.args, "rabbit undo") {
                crate::governance::policy::record_blocked(
                    "cli",
                    &self.audit_profile(),
                    "rabbit undo",
                    &reason,
                );
                anyhow::bail!(reason);
            }
        }
        let ops = Journal::default().load();
        let scope = self.scope()?;
        let op = match &args.id {
            Some(id) => undo::find(&ops, id).map_err(anyhow::Error::msg)?,
            None => undo::latest_undoable(&ops, &scope, &crate::governance::audit::os_user())
                .ok_or_else(|| {
                    anyhow::Error::msg(
                        "Nothing to undo: you have no undoable operation on this RabbitMQ \
                         (see `ferrika rabbit history`)",
                    )
                })?,
        };
        if op.begin.scope.rabbitmq.is_empty() {
            anyhow::bail!(
                "Operation {} ran on {}, not on RabbitMQ; undo it with `ferrika undo {}`",
                op.id,
                op.begin.scope.describe(),
                op.id
            );
        }
        let policy = crate::governance::policy::current();
        let audited = crate::governance::audit::op_digests();
        let mark = undo::undone_mark(op, &ops, &audited, policy.is_active());
        if let Some(undoer) = mark.tampered_undoer() {
            self.record_tampered(undoer, undo::TAMPERED_UNDO_LINK);
        }
        if let Some(note) = mark.ignored_note(&op.id) {
            eprintln!("{} {note}.", "warning:".yellow().bold());
        }
        let options = UndoOptions {
            force: args.force,
            accept_legacy: false,
        };
        let request = history::prepare_request(
            op,
            &ops,
            &scope,
            &audited,
            policy.is_active(),
            options,
            self.client,
        )
        .await?;
        let Some(plan) = &request.plan else {
            let refusal = request.refusal.as_ref().map_or_else(
                || format!("Cannot undo {}", op.id),
                |refusal| refusal.message(&op.id),
            );
            anyhow::bail!(refusal);
        };
        let refused = request.refusal.as_ref().map(UndoRefusal::reason);
        if self.json && (args.dry_run || refused.is_some()) {
            output::print_json(&kafka_history::plan_json(plan, refused.as_deref(), None))?;
        }
        if self.json && args.dry_run {
            return match &request.refusal {
                Some(refusal) => Err(anyhow::Error::msg(refusal.message(&op.id))),
                None => Ok(()),
            };
        }
        if !self.json {
            kafka_history::describe_plan(op, plan);
        }
        if let Some(refusal) = &request.refusal {
            if request.op_tampered() {
                self.record_tampered(&op.id, undo::TAMPERED_REFUSED);
            }
            anyhow::bail!(refusal.message(&op.id));
        }
        // RabbitMQ entries are never legacy (they came after digest v2),
        // but an edited journal could claim one: refuse rather than ask.
        if let Some(refusal) = request.legacy_refusal("undo it on the broker it ran on") {
            anyhow::bail!(refusal);
        }
        if request.digest == DigestCheck::Missing {
            eprintln!(
                "{} the audit log has no entry for {}, so the history entry cannot be checked \
                 against it.",
                "warning:".yellow().bold(),
                op.id
            );
        }
        if plan.runnable_steps().is_empty() {
            if self.json {
                output::print_json(&kafka_history::plan_json(
                    plan,
                    None,
                    Some("nothing to undo"),
                ))?;
                return Ok(());
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

        let target = format!("{} {} (op {})", op.begin.action, op.begin.target, op.id);
        let undo_op = crate::model::history::new_op_id(crate::kafka::clock::now_ms());
        self.authorize(plan, &target, &undo_op)?;
        if !args.yes {
            confirm(
                &format!(
                    "Undo {} {} on {}? [y/N]: ",
                    op.begin.action,
                    op.begin.target.bold(),
                    scope.describe()
                ),
                "Aborted undo.",
            )?;
        }
        let mut global = self.global.clone();
        global.write = Some(WriteContext {
            label: "rabbit undo".to_string(),
            target,
            op: Some(undo_op),
        });
        let forced = request.forced;
        let change = Change {
            reversibility: plan.redo,
            limits: plan.redo_limits.clone(),
            before: plan.current.clone(),
            after: op.begin.before.clone(),
            info: Vec::new(),
        };
        let pending = record::start_rabbit(
            &global,
            op.begin.scope.clone(),
            change,
            self.audit_profile(),
            Some(op.id.clone()),
        );
        let result = history::execute_steps(plan, self.client)
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
            Ok(text) if self.json => {
                output::print_json(&kafka_history::plan_json(plan, None, Some(&text)))?;
                Ok(())
            }
            Ok(text) => {
                println!("{} {text}", "✔".green().bold());
                Ok(())
            }
            Err(e) => anyhow::bail!("Undo of {} failed: {e}", op.id),
        }
    }

    /// The write gate once per policy category of the undo's steps
    /// (undoing a queue declaration is a queue deletion), on the RabbitMQ
    /// profile and addresses; a read-only RabbitMQ profile refuses.
    fn authorize(&self, plan: &UndoPlan, target: &str, op: &str) -> anyhow::Result<()> {
        let mut actions: Vec<_> = plan
            .runnable_steps()
            .iter()
            .map(|s| s.policy_action())
            .collect();
        actions.sort_by_key(|a| a.id());
        actions.dedup();
        let profile = gate_profile(self.args);
        let bootstrap = gate_address(self.args).map_err(anyhow::Error::msg)?;
        for action in actions {
            let request = crate::governance::policy::WriteRequest {
                source: "cli",
                action,
                label: "rabbit undo",
                target,
                profile: &profile,
                bootstrap: &bootstrap,
                actor: None,
                op: Some(op),
            };
            crate::governance::policy::authorize_write(
                &request,
                crate::governance::policy::AuditMode::RecordRequest,
                || crate::cli::rabbit::ensure_writable(self.args, "rabbit undo"),
            )
            .map_err(|refusal| anyhow::Error::msg(refusal.to_string()))?;
        }
        Ok(())
    }

    /// Records an edited history entry found by undo as `history.tampered`.
    fn record_tampered(&self, op: &str, outcome: &str) {
        crate::governance::audit::record(&crate::governance::audit::AuditEntry {
            time_ms: crate::governance::audit::now_ms(),
            source: "cli".to_string(),
            profile: self.audit_profile(),
            action: "history.tampered".to_string(),
            target: format!("op {op}"),
            outcome: outcome.to_string(),
            ok: false,
            user: String::new(),
            actor: None,
            op: op.to_string(),
            op_digest: String::new(),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arguments_are_typed_like_the_management_ui_types_them() {
        let parsed = parse_arguments(&[
            "x-max-length=1000".to_string(),
            "x-dead-letter-exchange=dlx".to_string(),
            "x-single-active-consumer=true".to_string(),
            r#"x-labels=["a","b"]"#.to_string(),
            r#"x-code="42""#.to_string(),
            "x-empty=".to_string(),
        ])
        .unwrap();
        assert_eq!(parsed["x-max-length"], 1000);
        assert_eq!(parsed["x-dead-letter-exchange"], "dlx");
        assert_eq!(parsed["x-single-active-consumer"], true);
        assert_eq!(parsed["x-labels"], json!(["a", "b"]));
        assert_eq!(parsed["x-code"], "42");
        assert_eq!(parsed["x-empty"], "");
        assert!(parse_arguments(&["novalue".to_string()]).is_err());
        assert!(parse_arguments(&["=x".to_string()]).is_err());
        assert!(parse_arguments(&["x={bad".to_string()]).is_err());
    }

    #[test]
    fn bindings_from_the_default_exchange_are_refused() {
        let args = RabbitBindingArgs {
            source: "amq.default".into(),
            destination: "orders".into(),
            destination_type: BindingDestinationType::Queue,
            routing_key: String::new(),
            arguments: vec![],
        };
        assert!(binding_of(&args).is_err());
        let to_exchange = RabbitBindingArgs {
            source: "shop".into(),
            destination_type: BindingDestinationType::Exchange,
            arguments: vec!["x-match=all".into()],
            ..args
        };
        let binding = binding_of(&to_exchange).unwrap();
        assert_eq!(binding.destination_type, DestinationType::Exchange);
        assert_eq!(binding.arguments["x-match"], "all");
    }
}
