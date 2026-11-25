//! The write tools, offered with `ferrika mcp --allow-write`.
//!
//! Every write runs in two calls: `mode: "plan"` shows what would change
//! and returns a confirmation token, `mode: "execute"` with the same
//! arguments and the token runs it. Each call passes [`WriteGuard`]; the
//! plan is made again right before running, and a plan that no longer
//! matches the one confirmed is refused (`state_changed`).

use std::collections::{BTreeMap, HashMap};

use serde_json::{json, Map, Value};

use std::time::Duration;

use super::backend::{KafkaBackend, TransformRun, WriteBackend};
use super::guard::{
    args_digest, digest, is_rabbit_tool, policy_action, GuardConfig, Limits, ToolError, WriteGuard,
    WRITE_TOOLS,
};
use super::rabbit_writes::{self, LiveRabbit, RabbitBackend, RabbitWork};
use super::transform_tools::{self, EXECUTE_MAX};
use super::transport::{Peer, PeerError};
use super::{ConfirmMode, WriteOptions};
use ferrika_core::kafka::context::ResolvedContext;
use ferrika_core::kafka::offset_reset::{
    check_group_idle, OffsetResetPlan, ResetStrategy, UncommittedBase,
};
use ferrika_core::kafka::producer::OwnedProducerRecord;
use ferrika_core::model::history::{Reversibility, Scope};
use ferrika_core::model::policy::McpWriteRule;
use ferrika_ops::history::{capture, Change, Journal, Recorder};

/// Most records one produce_messages call sends.
const PRODUCE_MAX_RECORDS: usize = 100;
/// Largest record (key + value + headers) and call, unless the topic's
/// `max.message.bytes` is smaller.
const RECORD_MAX_BYTES: usize = 1024 * 1024;
const CALL_MAX_BYTES: usize = 4 * 1024 * 1024;
use ferrika_core::kafka::wire::ConfigChange;

/// How long the user has to answer a confirmation.
const ELICIT_TIMEOUT: Duration = Duration::from_secs(120);

/// The write side of a session.
pub(super) struct WriteTools {
    pub guard: WriteGuard,
    backend: Box<dyn WriteBackend>,
    /// The RabbitMQ brokers the `rabbit_*` tools reach.
    pub(super) rabbit: Box<dyn RabbitBackend>,
    confirm: ConfirmMode,
    elicit_timeout: Duration,
    /// Where executed changes are recorded for `ferrika history` / `undo`;
    /// `None` records nothing (tests).
    history: Option<Journal>,
}

/// The client request a tool call answers, and how to ask the client.
pub(super) struct CallContext<'a> {
    pub peer: &'a Peer,
    pub request_id: &'a Value,
    /// Whether the client declared form-mode elicitation.
    pub elicitation: bool,
}

/// What a plan found: shown to the agent, digested for the token, and the
/// work to run once confirmed.
struct Planned {
    /// The plan as the agent sees it; its digest binds the token.
    plan: Value,
    /// One line for the confirmation and the audit log.
    summary: String,
    /// The audit target: a topic, or `group@topic`.
    target: String,
    /// A person confirming it types the target name back (it loses data).
    destructive: bool,
    /// Why only a person may confirm it (elicitation), never a token.
    person_only: Option<String>,
    work: Work,
}

/// A planned change, ready to run.
enum Work {
    CreateTopic {
        topic: String,
        partitions: i32,
        replication_factor: i32,
        configs: HashMap<String, String>,
    },
    AlterConfig {
        topic: String,
        changes: Vec<ConfigChange>,
    },
    ResetOffsets {
        group: String,
        topic: String,
        strategy: String,
        plans: Vec<OffsetResetPlan>,
    },
    Produce {
        topic: String,
        records: Vec<OwnedProducerRecord>,
    },
    Transform(TransformRun),
    /// A RabbitMQ write (`rabbit_writes.rs`), run by `run_rabbit`.
    Rabbit(RabbitWork),
}

/// What a write did.
struct Executed {
    result: Value,
    /// The audit outcome: a summary without payloads or secrets.
    outcome: String,
    ok: bool,
    /// Records charged to the session's produce budget that were not
    /// written, given back (a transform that found fewer than its
    /// `max_records`).
    unused_budget: u64,
}

impl WriteTools {
    /// The write tools for `ferrika mcp` started with `args`; `None` without
    /// `--allow-write`.
    pub(super) fn from_options(args: &WriteOptions, context: &ResolvedContext) -> Self {
        let config = GuardConfig {
            tools: WRITE_TOOLS
                .into_iter()
                .filter(|t| args.allow_tools.is_empty() || args.allow_tools.iter().any(|a| a == t))
                .collect(),
            topics: args.topics.clone(),
            queues: args.queues.clone(),
            exchanges: args.exchanges.clone(),
            limits: Limits {
                writes_per_minute: args.max_writes_per_minute,
                produce_records: args.max_produce_records,
                offset_delta: args.max_offset_delta,
            },
            profile: context.profile_name.clone().unwrap_or_default(),
            bootstrap: context.bootstrap_servers.clone(),
            profile_refusal: context
                .ensure_writable("mcp --allow-write")
                .err()
                .map(|e| e.to_string()),
        };
        let guard = WriteGuard::new(
            config,
            ferrika_governance::policy::current(),
            Box::new(ferrika_governance::policy::AuditLog),
            Box::new(jiff::Timestamp::now),
        );
        let backend = Box::new(KafkaBackend {
            config: context.config.clone(),
        });
        let mut tools = Self::new(guard, backend);
        tools.confirm = args.confirm;
        tools.history = Some(Journal::default());
        tools
    }

    pub(super) fn new(guard: WriteGuard, backend: Box<dyn WriteBackend>) -> Self {
        Self {
            guard,
            backend,
            rabbit: Box::new(LiveRabbit),
            confirm: ConfirmMode::Auto,
            elicit_timeout: ELICIT_TIMEOUT,
            history: None,
        }
    }

    /// Checks that must pass just before `work` runs, before anything is
    /// recorded: a group must still be idle, since members that joined since
    /// the plan would overwrite the reset with their own commits.
    async fn precheck(&self, work: &Work) -> Result<(), ToolError> {
        if let Work::ResetOffsets { group, .. } = work {
            let state = self
                .backend
                .group_state(group)
                .await
                .map_err(ToolError::kafka)?;
            check_group_idle(group, state.as_ref().map(|(s, m)| (s.as_str(), *m)))
                .map_err(|e| ToolError::new("group_active", e.to_string()))?;
        }
        Ok(())
    }

    /// Starts the history record of `work` (the `begin` line, with the
    /// state it changes read now). `None` without a journal and for
    /// produce, which, as in the CLI, is not recorded (records cannot be
    /// taken back). The warning says when the change will not be undoable
    /// because its before-state could not be read.
    async fn start_record(
        &self,
        tool: &str,
        target: &str,
        work: &Work,
    ) -> (Option<Recorder>, Option<String>) {
        let Some(journal) = self.history.clone() else {
            return (None, None);
        };
        let mut warning = None;
        let change = match work {
            Work::CreateTopic { topic, .. } => Change::topic_created(topic),
            Work::AlterConfig { topic, changes } => {
                let keys = capture::change_keys(changes);
                let captured = self.backend.topic_configs(topic).await.map(|entries| {
                    let before = capture::topic_config_of(topic, &entries, &keys);
                    let after = capture::config_after(&before, changes);
                    (vec![before], vec![after])
                });
                if let Err(e) = &captured {
                    warning = Some(format!(
                        "The change ran, but `ferrika undo` cannot revert it: the topic's \
                         config could not be read first ({e})."
                    ));
                }
                Change::captured(Reversibility::Reversible, captured).sensitive_limits()
            }
            Work::ResetOffsets { group, plans, .. } => Change::offset_reset(group, plans),
            // Copies, like produce, cannot be taken back.
            Work::Produce { .. } | Work::Transform(_) | Work::Rabbit(_) => return (None, None),
        };
        let (profile, bootstrap) = self.guard.profile_and_bootstrap();
        let scope = Scope {
            profile,
            bootstrap,
            cluster_id: self.backend.cluster_id().await.unwrap_or_default(),
            ..Scope::default()
        };
        let begin = change.into_begin(
            "mcp",
            scope,
            format!("mcp {tool}"),
            target.to_string(),
            None,
        );
        (Some(Recorder::start(journal, begin)), warning)
    }

    /// One stderr line per reason some enabled write tools are hidden.
    pub(super) fn startup_warnings(&self) -> Vec<String> {
        let mut reasons: Vec<String> = Vec::new();
        for tool in &self.guard.config().tools {
            if let Some(reason) = self.guard.hidden_reason(tool) {
                if !reasons.contains(&reason) {
                    reasons.push(reason);
                }
            }
        }
        reasons
            .into_iter()
            .map(|r| format!("ferrika mcp: write tools disabled: {r}"))
            .collect()
    }

    /// The confirmation mode in force: `--confirm`, raised to `elicit` while
    /// the company policy says `mcp_write: confirm_human`.
    fn confirm_mode(&self) -> ConfirmMode {
        if self.guard.mcp_write_rule() == McpWriteRule::ConfirmHuman {
            ConfirmMode::Elicit
        } else {
            self.confirm
        }
    }

    /// Whether writes are confirmed by asking a person through the client
    /// (elicitation) for a client that declared `elicitation` or not.
    fn asks_person(&self, client_elicitation: bool) -> bool {
        match self.confirm_mode() {
            ConfirmMode::Elicit => true,
            ConfirmMode::Token => false,
            ConfirmMode::Auto => client_elicitation,
        }
    }

    /// How writes are confirmed, for `mcp.session_start`.
    pub(super) fn confirm_label(&self, client_elicitation: bool) -> &'static str {
        match (self.confirm_mode(), client_elicitation) {
            (ConfirmMode::Auto, true) => "auto (elicitation)",
            (ConfirmMode::Auto, false) => "auto (token)",
            (ConfirmMode::Elicit, true) => "elicit",
            (ConfirmMode::Elicit, false) => "elicit (client cannot ask: writes off)",
            (ConfirmMode::Token, _) => "token",
        }
    }

    /// The write tools `tools/list` shows: enabled and not refused outright
    /// by the profile or the policy (nor needing elicitation the client
    /// does not offer).
    pub(super) fn definitions(&self, client_elicitation: bool) -> Vec<Value> {
        if self.confirm_mode() == ConfirmMode::Elicit && !client_elicitation {
            return Vec::new();
        }
        definitions()
            .into_iter()
            .filter(|d| {
                let name = d["name"].as_str().unwrap_or_default();
                self.guard.enabled(name) && self.guard.hidden_reason(name).is_none()
            })
            .collect()
    }

    /// The `initialize` instructions about writing.
    pub(super) fn instructions(&self, client_elicitation: bool) -> String {
        let tools: Vec<String> = self
            .definitions(client_elicitation)
            .iter()
            .filter_map(|d| d["name"].as_str().map(str::to_string))
            .collect();
        if tools.is_empty() {
            return " Writing is enabled, but no write tool is offered (read-only profile, \
                    company policy, or a client that cannot ask the user)."
                .to_string();
        }
        let how = if self.asks_person(client_elicitation) {
            "then call it again with the same arguments and mode \"execute\": Ferrika asks the \
             user to confirm before it runs."
        } else {
            "then call it again with the same arguments, mode \"execute\" and the plan's \
             confirmation token."
        };
        format!(
            " Write tools: {}. Call one with mode \"plan\" first and show the plan to the user; {how}",
            tools.join(", ")
        )
    }

    /// Runs write tool `name` for the client request `ctx.request_id`.
    pub(super) async fn call(&mut self, name: &str, args: &Value, ctx: &CallContext<'_>) -> Value {
        let Some(tool) = WRITE_TOOLS.into_iter().find(|t| *t == name) else {
            return disabled(name);
        };
        if !self.guard.enabled(tool) {
            return disabled(tool);
        }
        match self.run(tool, args, ctx).await {
            Ok((result, executed)) => {
                self.guard.note_success(executed);
                result
            }
            Err(error) => {
                self.guard.note_refusal(tool, &error);
                error.into_result()
            }
        }
    }

    /// The plan or execute call; the flag says whether it executed.
    async fn run(
        &mut self,
        tool: &'static str,
        args: &Value,
        ctx: &CallContext<'_>,
    ) -> Result<(Value, bool), ToolError> {
        if self.confirm_mode() == ConfirmMode::Elicit && !ctx.elicitation {
            let why = if self.guard.mcp_write_rule() == McpWriteRule::ConfirmHuman {
                "The company policy requires a person to confirm every change an AI assistant \
                 makes (mcp_write: confirm_human)"
            } else {
                "This server confirms writes by asking the user (--confirm elicit)"
            };
            return Err(ToolError::new(
                "elicitation_unsupported",
                format!("{why}, and this client does not support MCP elicitation."),
            ));
        }
        if is_rabbit_tool(tool) {
            return self.run_rabbit(tool, args, ctx).await;
        }
        self.guard.check_cooldown()?;
        let execute = execute_mode(args)?;
        let request = parse(tool, args)?;
        for topic in request.topics() {
            self.guard.check_topic(topic)?;
        }
        // An execute takes its slot only once its confirmation holds, so one
        // refused for a missing or bad token does not use up the budget.
        if execute {
            self.guard.check_rate(true)?;
        } else {
            self.guard.admit(false)?;
        }
        self.authorize(tool, &request.target()).await?;
        let planned = self.plan(&request).await?;
        let plan_digest = plan_digest(&planned.plan);
        if !execute {
            let (token, expires) = self.guard.issue_token(tool, args_digest(args), plan_digest);
            let person = self.asks_person(ctx.elicitation);
            return Ok((plan_result(tool, &planned, &token, expires, person), false));
        }

        self.confirm(tool, args, &planned, &plan_digest, ctx)
            .await?;
        self.guard.admit(true)?;
        // The confirmation may have taken a while: ask the gate again.
        self.authorize(tool, &planned.target).await?;
        match &planned.work {
            Work::Produce { records, .. } => self.guard.charge_produce(records.len() as u64)?,
            // Charged in full now, the part not written given back after.
            Work::Transform(run) => self.guard.charge_produce(run.max_records)?,
            Work::CreateTopic { .. }
            | Work::AlterConfig { .. }
            | Work::ResetOffsets { .. }
            | Work::Rabbit(_) => {}
        }
        let target = planned.target.clone();
        // Refused here as it was inside the run: audited, not recorded.
        if let Err(error) = self.precheck(&planned.work).await {
            let _ = self
                .guard
                .record_result(tool, &target, &error.message, false, None);
            return Err(error);
        }
        let (recorder, history_warning) = self.start_record(tool, &target, &planned.work).await;
        // The journal's `end` line, and the id and digest the audit entry names.
        let finish = |ok: bool, outcome: &str| {
            let recorder = recorder.as_ref()?;
            let digest = recorder.finish(ok, outcome, None)?;
            Some((recorder.id().to_string(), digest))
        };
        match self.execute(planned.work).await {
            Ok(done) => {
                self.guard.refund_produce(done.unused_budget);
                let op = finish(done.ok, &done.outcome);
                let warning = self
                    .guard
                    .record_result(tool, &target, &done.outcome, done.ok, op);
                let mut result = execute_result(&done, warning);
                if let Some(text) = history_warning {
                    eprintln!("ferrika mcp: {text}");
                    result["structuredContent"]["history_warning"] = json!(text);
                }
                Ok((result, true))
            }
            Err(error) => {
                let op = finish(false, &error.message);
                let _ = self
                    .guard
                    .record_result(tool, &target, &error.message, false, op);
                Err(error)
            }
        }
    }

    /// [`Self::run`] for a RabbitMQ tool: the same plan -> confirm ->
    /// execute flow, on the RabbitMQ profile the arguments name, gated on
    /// that profile and the broker's addresses (`authorize_rabbit`). The
    /// connection is resolved again for the execute from the same
    /// arguments, which the token binds.
    async fn run_rabbit(
        &mut self,
        tool: &'static str,
        args: &Value,
        ctx: &CallContext<'_>,
    ) -> Result<(Value, bool), ToolError> {
        self.guard.check_cooldown()?;
        let execute = execute_mode(args)?;
        let request = rabbit_writes::parse(tool, args)?;
        request.check_scope(&self.guard)?;
        if execute {
            self.guard.check_rate(true)?;
        } else {
            self.guard.admit(false)?;
        }
        let conn = self
            .rabbit
            .connect(args)
            .map_err(|e| ToolError::new("invalid_argument", e.to_string()))?;
        let target = request.target(conn.vhost());
        let authorize = |guard: &WriteGuard| {
            guard.authorize_rabbit(
                tool,
                policy_action(tool),
                &target,
                conn.profile(),
                &conn.addresses,
                conn.read_only_refusal(tool),
            )
        };
        authorize(&self.guard)?;
        let planned =
            rabbit_writes::plan(&self.guard, self.rabbit.as_ref(), &conn, tool, &request).await?;
        let planned = Planned {
            plan: planned.plan,
            summary: planned.summary,
            target: target.clone(),
            destructive: false,
            person_only: None,
            work: Work::Rabbit(planned.work),
        };
        let digest = plan_digest(&planned.plan);
        if !execute {
            let (token, expires) = self.guard.issue_token(tool, args_digest(args), digest);
            let person = self.asks_person(ctx.elicitation);
            return Ok((plan_result(tool, &planned, &token, expires, person), false));
        }

        self.confirm(tool, args, &planned, &digest, ctx).await?;
        self.guard.admit(true)?;
        // The confirmation may have taken a while: ask the gate again.
        authorize(&self.guard)?;
        let records = request.produce_records();
        self.guard.charge_produce(records)?;
        let Work::Rabbit(work) = planned.work else {
            return Err(ToolError::new("write_disabled", "not a RabbitMQ write"));
        };
        let done = rabbit_writes::execute(
            &self.guard,
            self.rabbit.as_ref(),
            &conn,
            tool,
            &target,
            work,
            records,
        )
        .await;
        match done {
            Ok(done) => {
                self.guard.refund_produce(done.unused_budget);
                let warning = self.guard.record_at(
                    conn.audit_profile(),
                    format!("mcp {tool}"),
                    &target,
                    &done.outcome,
                    true,
                    None,
                );
                let executed = Executed {
                    result: done.result,
                    outcome: done.outcome,
                    ok: true,
                    unused_budget: done.unused_budget,
                };
                Ok((execute_result(&executed, warning), true))
            }
            Err(failed) => {
                self.guard.refund_produce(failed.unused_budget);
                let _ = self.guard.record_at(
                    conn.audit_profile(),
                    format!("mcp {tool}"),
                    &target,
                    &failed.outcome,
                    false,
                    None,
                );
                Err(failed.error)
            }
        }
    }

    /// The guard's policy gate, with the cluster's own broker addresses
    /// added when the policy matches by broker address. A lookup that fails
    /// refuses: the rule cannot be checked.
    async fn authorize(&self, tool: &'static str, target: &str) -> Result<(), ToolError> {
        let known = if self.guard.needs_broker_metadata() {
            self.backend.known_brokers().await.map_err(|e| {
                ToolError::new(
                    "policy_refused",
                    format!(
                        "Refused: the company policy matches broker addresses, and the \
                         cluster's brokers could not be read to check them ({e})."
                    ),
                )
            })?
        } else {
            Vec::new()
        };
        self.guard.authorize_with(tool, target, &known)
    }

    /// The confirmation before running: a token from the plan (checked
    /// whenever given) and, in elicitation mode, the user's answer. A plan
    /// that no longer matches the token's is refused.
    async fn confirm(
        &mut self,
        tool: &'static str,
        args: &Value,
        planned: &Planned,
        plan_digest: &str,
        ctx: &CallContext<'_>,
    ) -> Result<(), ToolError> {
        let person = self.asks_person(ctx.elicitation);
        let token = args.get("confirmation").and_then(Value::as_str);
        if !person {
            if let Some(why) = &planned.person_only {
                return Err(ToolError::new(
                    "elicitation_required",
                    format!(
                        "{why}, so a person must confirm it, and this session confirms with \
                         tokens (the client has no MCP elicitation, or --confirm token). Narrow \
                         the request or run it from the Ferrika CLI or GUI."
                    ),
                ));
            }
            if token.is_none() {
                return Err(ToolError::new(
                    "confirmation_required",
                    format!(
                        "Executing needs the confirmation token from a plan: call {tool} with \
                         mode \"plan\", show the plan to the user, then call again with mode \
                         \"execute\" and confirmation set to the token."
                    ),
                ));
            }
        }
        if let Some(token) = token {
            let planned_digest = self.guard.redeem_token(tool, token, &args_digest(args))?;
            if planned_digest != plan_digest {
                return Err(state_changed());
            }
        }
        if person {
            self.ask_person(tool, planned, ctx).await?;
        }
        Ok(())
    }

    /// Asks the user through the client (`elicitation/create`, form mode)
    /// whether to run `planned`; a destructive plan also needs the target
    /// name typed back. Anything but a matching accept refuses, recorded as
    /// `mcp.declined`.
    async fn ask_person(
        &self,
        tool: &str,
        planned: &Planned,
        ctx: &CallContext<'_>,
    ) -> Result<(), ToolError> {
        // A RabbitMQ summary names its own RabbitMQ profile.
        let profile = if is_rabbit_tool(tool) {
            ""
        } else {
            self.guard.config().profile.as_str()
        };
        let mut message = format!(
            "Ferrika{}: {}.",
            if profile.is_empty() {
                String::new()
            } else {
                format!(" (profile {profile})")
            },
            planned.summary
        );
        if let Some(warnings) = planned.plan["warnings"].as_array() {
            for warning in warnings.iter().filter_map(Value::as_str) {
                message.push_str(&format!("\nWarning: {warning}"));
            }
        }
        message.push_str("\nRun it?");
        let name = confirm_name(&planned.target);
        let mut properties = json!({
            "confirm": {
                "type": "boolean",
                "title": "Run it",
                "description": "Check to run this change on the cluster",
                "default": false
            }
        });
        let mut required = vec!["confirm"];
        if planned.destructive {
            properties["type_name"] = json!({
                "type": "string",
                "title": format!("Type '{name}' to confirm"),
                "description": "This change deletes data; type the name to confirm"
            });
            required.push("type_name");
        }
        let params = json!({
            "mode": "form",
            "message": message,
            "requestedSchema": {
                "type": "object",
                "properties": properties,
                "required": required
            }
        });
        let decline = |code: &'static str, reason: &str| {
            self.guard.record_declined(tool, &planned.target, reason);
            ToolError::new(code, format!("Not run: {reason}."))
        };
        let answer = tokio::time::timeout(
            self.elicit_timeout,
            ctx.peer
                .request("elicitation/create", params, ctx.request_id),
        )
        .await;
        let response = match answer {
            Err(_) => {
                return Err(decline(
                    "confirmation_timeout",
                    &format!(
                        "the user did not answer within {} s",
                        self.elicit_timeout.as_secs()
                    ),
                ))
            }
            Ok(Err(PeerError::Cancelled)) => {
                return Err(decline(
                    "user_cancelled",
                    "the client cancelled the request",
                ))
            }
            Ok(Err(PeerError::Disconnected)) => {
                return Err(decline("user_cancelled", "client disconnected"))
            }
            Ok(Ok(response)) => response,
        };
        if let Some(error) = response.get("error") {
            let text = error["message"].as_str().unwrap_or("unknown error");
            return Err(decline(
                "elicitation_unsupported",
                &format!("the client could not ask the user ({text})"),
            ));
        }
        let result = &response["result"];
        match result["action"].as_str() {
            Some("accept") => {}
            Some("decline") => return Err(decline("user_declined", "the user declined")),
            _ => return Err(decline("user_cancelled", "the user dismissed the question")),
        }
        let content = &result["content"];
        if content["confirm"] != json!(true) {
            return Err(decline(
                "user_declined",
                "the user did not check \"Run it\"",
            ));
        }
        if planned.destructive && content["type_name"].as_str().map(str::trim) != Some(name) {
            return Err(decline(
                "user_declined",
                &format!("the typed name did not match '{name}'"),
            ));
        }
        Ok(())
    }

    async fn plan(&self, request: &Request) -> Result<Planned, ToolError> {
        match request {
            Request::CreateTopic {
                topic,
                partitions,
                replication_factor,
                configs,
            } => {
                self.plan_create_topic(topic, *partitions, *replication_factor, configs)
                    .await
            }
            Request::AlterConfig { topic, set, delete } => {
                self.plan_alter_config(topic, set, delete).await
            }
            Request::ResetOffsets {
                group,
                topic,
                partition,
                strategy,
            } => self.plan_reset(group, topic, *partition, strategy).await,
            Request::Produce {
                topic,
                records,
                value_subject,
                key_subject,
            } => {
                self.plan_produce(
                    topic,
                    records,
                    value_subject.as_deref(),
                    key_subject.as_deref(),
                )
                .await
            }
            Request::Transform(run) => self.plan_transform(run).await,
        }
    }

    async fn execute(&self, work: Work) -> Result<Executed, ToolError> {
        match work {
            Work::CreateTopic {
                topic,
                partitions,
                replication_factor,
                configs,
            } => {
                self.backend
                    .create_topic(&topic, partitions, replication_factor, &configs, false)
                    .await
                    .map_err(ToolError::kafka)?;
                let rf = rf_text(replication_factor);
                Ok(Executed {
                    result: json!({
                        "topic": topic,
                        "partitions": partitions,
                        "replication_factor": rf,
                        "configs": configs.len(),
                    }),
                    outcome: format!("created ({partitions} partitions, replication factor {rf})"),
                    ok: true,
                    unused_budget: 0,
                })
            }
            Work::AlterConfig { topic, changes } => {
                self.backend
                    .alter_config(&topic, &changes, false)
                    .await
                    .map_err(ToolError::kafka)?;
                let shown = changes_text(&changes);
                Ok(Executed {
                    result: json!({ "topic": topic, "applied": changes.len(), "changes": shown }),
                    outcome: format!("applied: {shown}"),
                    ok: true,
                    unused_budget: 0,
                })
            }
            Work::ResetOffsets {
                group,
                topic,
                strategy,
                plans,
            } => {
                self.backend
                    .commit_reset(&group, &plans)
                    .await
                    .map_err(ToolError::kafka)?;
                let partitions: Vec<Value> = plans
                    .iter()
                    .map(|p| json!({ "partition": p.partition, "offset": p.target_offset }))
                    .collect();
                Ok(Executed {
                    result: json!({ "group": group, "topic": topic, "partitions": partitions }),
                    outcome: format!("{} partition(s) reset to {strategy}", plans.len()),
                    ok: true,
                    unused_budget: 0,
                })
            }
            Work::Produce { topic, records } => {
                let total = records.len();
                let results = self
                    .backend
                    .produce(records)
                    .await
                    .map_err(ToolError::kafka)?;
                let mut delivered = Vec::new();
                let rows: Vec<Value> = results
                    .iter()
                    .enumerate()
                    .map(|(i, r)| match r {
                        Ok(receipt) => {
                            delivered.push(format!("p{}@{}", receipt.partition, receipt.offset));
                            json!({ "index": i, "partition": receipt.partition, "offset": receipt.offset })
                        }
                        Err(e) => json!({ "index": i, "error": e.to_string() }),
                    })
                    .collect();
                let failed = total - delivered.len();
                let mut where_to = delivered
                    .iter()
                    .take(5)
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(", ");
                if delivered.len() > 5 {
                    where_to.push_str(", ...");
                }
                let outcome = if failed == 0 {
                    format!("{total} record(s) -> {where_to}")
                } else {
                    format!(
                        "{} of {total} record(s) delivered ({failed} failed){}",
                        delivered.len(),
                        if delivered.is_empty() {
                            String::new()
                        } else {
                            format!(": {where_to}")
                        }
                    )
                };
                Ok(Executed {
                    result: json!({
                        "topic": topic,
                        "delivered": delivered.len(),
                        "failed": failed,
                        "records": rows,
                    }),
                    outcome,
                    ok: failed == 0,
                    unused_budget: 0,
                })
            }
            Work::Rabbit(_) => Err(ToolError::new(
                "write_disabled",
                "RabbitMQ writes run through run_rabbit",
            )),
            Work::Transform(run) => {
                let summary = self
                    .backend
                    .transform_copy(&run)
                    .await
                    .map_err(ToolError::kafka)?;
                let attempted = summary.written.saturating_add(summary.failed);
                let mut result = serde_json::to_value(&summary)
                    .unwrap_or_else(|e| json!({ "summary_error": e.to_string() }));
                if let Some(error) = summary.failure() {
                    result["error"] = json!(error.to_string());
                }
                Ok(Executed {
                    result,
                    // Counts and the steps' digest only: a step can quote
                    // a record value.
                    outcome: summary.audit_detail(),
                    ok: summary.failed == 0,
                    unused_budget: run.max_records.saturating_sub(attempted),
                })
            }
        }
    }

    /// The plan of a transform: both topics exist, the steps parse, the
    /// copy passes the policy's copy checks (enforced masking needs a mask
    /// step first) and fits the session's produce budget. Nothing is read
    /// or written.
    async fn plan_transform(&self, run: &TransformRun) -> Result<Planned, ToolError> {
        let tool = "transform_execute";
        let (pipeline, _) =
            transform_tools::build_pipeline(&run.steps, false).map_err(ToolError::invalid)?;
        self.guard.authorize_copy(
            tool,
            &run.src_topic,
            &run.dst_topic,
            transform_tools::masks_first(&run.steps),
        )?;
        self.guard.check_produce(run.max_records)?;
        let Some(source) = self
            .backend
            .topic(&run.src_topic)
            .await
            .map_err(ToolError::kafka)?
        else {
            return Err(ToolError::invalid(format!(
                "Source topic '{}' does not exist",
                run.src_topic
            )));
        };
        if let Some(p) = run.partition {
            if p >= source.partitions {
                return Err(ToolError::invalid(format!(
                    "partition {p}: '{}' has partitions 0-{}",
                    run.src_topic,
                    source.partitions - 1
                )));
            }
        }
        let Some(destination) = self
            .backend
            .topic(&run.dst_topic)
            .await
            .map_err(ToolError::kafka)?
        else {
            return Err(ToolError::invalid(format!(
                "Destination topic '{}' does not exist (transform_execute never creates topics; \
                 create it first, e.g. with create_topic)",
                run.dst_topic
            )));
        };
        let mut warnings = vec![
            "Records already in the destination stay; running the same transform again copies \
             the range again."
                .to_string(),
        ];
        if run.decode {
            warnings.push(
                "Decoded values are written as JSON, not in the Schema Registry wire format."
                    .to_string(),
            );
        }
        let steps: Vec<Value> = run
            .steps
            .iter()
            .map(|(kind, text)| json!({ (kind.name()): text }))
            .collect();
        let steps_text = run
            .steps
            .iter()
            .map(|(kind, text)| format!("{} {text}", kind.name()))
            .collect::<Vec<_>>()
            .join("; ");
        let start = run.start.label();
        Ok(Planned {
            plan: json!({
                "action": tool,
                "source": run.src_topic,
                "destination": run.dst_topic,
                "source_partitions": source.partitions,
                "destination_partitions": destination.partitions,
                "partition": run.partition,
                "start": start,
                "until": "the end each partition has when the run starts",
                "max_records": run.max_records,
                "decode_wire_format": run.decode,
                "steps": steps,
                "steps_sha256": pipeline.digest(),
                "warnings": warnings,
            }),
            summary: format!(
                "copy up to {} record(s) from topic '{}' (from {start} to its current end) to \
                 topic '{}' through {} step(s): {steps_text}",
                run.max_records,
                run.src_topic,
                run.dst_topic,
                run.steps.len()
            ),
            target: format!("{} -> {}", run.src_topic, run.dst_topic),
            destructive: false,
            person_only: None,
            work: Work::Transform(run.clone()),
        })
    }

    async fn plan_produce(
        &self,
        topic: &str,
        records: &[RecordArg],
        value_subject: Option<&str>,
        key_subject: Option<&str>,
    ) -> Result<Planned, ToolError> {
        self.guard.check_produce(records.len() as u64)?;
        let Some(shape) = self.backend.topic(topic).await.map_err(ToolError::kafka)? else {
            return Err(ToolError::invalid(format!(
                "Topic '{topic}' does not exist (produce_messages never creates topics)"
            )));
        };
        let configs = self
            .backend
            .topic_configs(topic)
            .await
            .map_err(ToolError::kafka)?;
        let config = |key: &str| {
            configs
                .iter()
                .find(|e| e.name == key)
                .map(|e| e.value.clone())
        };
        let compacted = config("cleanup.policy").is_some_and(|p| p.contains("compact"));
        let record_max = config("max.message.bytes")
            .and_then(|v| v.parse::<usize>().ok())
            .map_or(RECORD_MAX_BYTES, |m| m.min(RECORD_MAX_BYTES));

        let mut built = Vec::with_capacity(records.len());
        let mut rows = Vec::with_capacity(records.len());
        let (mut total, mut tombstones) = (0usize, 0usize);
        for (i, record) in records.iter().enumerate() {
            if let Some(p) = record.partition {
                if p >= shape.partitions {
                    return Err(ToolError::invalid(format!(
                        "records[{i}].partition {p}: '{topic}' has partitions 0-{}",
                        shape.partitions - 1
                    )));
                }
            }
            let value = match (&record.value, value_subject) {
                (None, _) => None,
                (Some(text), Some(subject)) => Some(
                    self.backend
                        .encode_value(subject, text)
                        .await
                        .map_err(|e| {
                            ToolError::invalid(format!(
                                "records[{i}].value does not fit schema subject '{subject}': {e}"
                            ))
                        })?,
                ),
                (Some(text), None) => Some(text.as_bytes().to_vec()),
            };
            // An encoded key is what the partitioner hashes, as with a
            // producer that uses the key's schema serializer.
            let key = match (&record.key, key_subject) {
                (None, _) => None,
                (Some(text), Some(subject)) => Some(
                    self.backend
                        .encode_value(subject, text)
                        .await
                        .map_err(|e| {
                            ToolError::invalid(format!(
                                "records[{i}].key does not fit schema subject '{subject}': {e}"
                            ))
                        })?,
                ),
                (Some(text), None) => Some(text.as_bytes().to_vec()),
            };
            let size = key.as_ref().map_or(0, Vec::len)
                + value.as_ref().map_or(0, Vec::len)
                + record
                    .headers
                    .iter()
                    .map(|(k, v)| k.len() + v.len())
                    .sum::<usize>();
            if size > record_max {
                return Err(ToolError::invalid(format!(
                    "records[{i}] is {size} bytes; the most is {record_max}"
                )));
            }
            total += size;
            tombstones += usize::from(value.is_none());
            rows.push(json!({
                "index": i,
                "partition": match (record.partition, &record.key) {
                    (Some(p), _) => json!(p),
                    (None, Some(_)) => json!("by key hash"),
                    (None, None) => json!("any"),
                },
                "bytes": size,
                "value_sha256": value.as_deref().map(short_sha256),
                "tombstone": value.is_none(),
                "headers": record.headers.len(),
            }));
            let mut out = OwnedProducerRecord::new(topic);
            out.key = key;
            out.payload = value;
            out.partition = record.partition;
            out.headers = record
                .headers
                .iter()
                .map(|(k, v)| (k.clone(), v.as_bytes().to_vec()))
                .collect();
            built.push(out);
        }
        if total > CALL_MAX_BYTES {
            return Err(ToolError::invalid(format!(
                "The records total {total} bytes; one call sends at most {CALL_MAX_BYTES}"
            )));
        }
        let mut warnings = Vec::new();
        if tombstones > 0 && !compacted {
            warnings.push(format!(
                "{tombstones} record(s) have no value (tombstones), but '{topic}' is not compacted: \
                 consumers will just see empty records."
            ));
        }
        Ok(Planned {
            plan: json!({
                "action": "produce_messages",
                "topic": topic,
                "partitions": shape.partitions,
                "records": rows,
                "total_bytes": total,
                "tombstones": tombstones,
                "value_subject": value_subject,
                "key_subject": key_subject,
                "warnings": warnings,
            }),
            summary: format!(
                "produce {} record(s) ({total} bytes) to topic '{topic}'",
                records.len()
            ),
            target: topic.to_string(),
            destructive: false,
            person_only: None,
            work: Work::Produce {
                topic: topic.to_string(),
                records: built,
            },
        })
    }

    async fn plan_reset(
        &self,
        group: &str,
        topic: &str,
        partition: Option<i32>,
        strategy: &ResetStrategy,
    ) -> Result<Planned, ToolError> {
        if self
            .backend
            .topic(topic)
            .await
            .map_err(ToolError::kafka)?
            .is_none()
        {
            return Err(ToolError::invalid(format!(
                "Topic '{topic}' does not exist"
            )));
        }
        let plans = self
            .backend
            .plan_reset(group, topic, partition, strategy)
            .await
            .map_err(|e| match e {
                // The agent can fix the request: name a base or another
                // strategy. Refused at plan time, nothing changed.
                ferrika_core::error::KafkaError::UncommittedShift { .. } => {
                    ToolError::new("uncommitted_partitions", e.to_string())
                }
                other => ToolError::kafka(other),
            })?;
        let state = self
            .backend
            .group_state(group)
            .await
            .map_err(ToolError::kafka)?;
        let mut warnings = Vec::new();
        if let Err(e) = check_group_idle(group, state.as_ref().map(|(s, m)| (s.as_str(), *m))) {
            warnings.push(format!("{e} Executing is refused until then."));
        }
        let (mut reread, mut skipped) = (0i64, 0i64);
        let rows: Vec<Value> = plans
            .iter()
            .map(|p| {
                let from = if p.current_offset < 0 {
                    p.low_watermark
                } else {
                    p.current_offset
                };
                if p.delta < 0 {
                    reread += -p.delta;
                } else {
                    skipped += p.delta;
                }
                json!({
                    "partition": p.partition,
                    "current": (p.current_offset >= 0).then_some(p.current_offset),
                    "new": p.target_offset,
                    "lag_before": p.high_watermark - from,
                    "lag_after": p.high_watermark - p.target_offset,
                })
            })
            .collect();
        let limit = self.guard.config().limits.offset_delta;
        let moved = (reread + skipped) as u64;
        let person_only = (moved > limit).then(|| {
            format!(
                "This reset moves {moved} records' worth of offsets, more than --max-offset-delta {limit}"
            )
        });
        let shown_strategy = strategy.to_string();
        let mut effect = Vec::new();
        if reread > 0 {
            effect.push(format!("{reread} record(s) re-read"));
        }
        if skipped > 0 {
            effect.push(format!("{skipped} record(s) skipped"));
        }
        if effect.is_empty() {
            effect.push("no offset moves".to_string());
        }
        Ok(Planned {
            plan: json!({
                "action": "reset_consumer_group_offsets",
                "group": group,
                "topic": topic,
                "to": shown_strategy,
                "group_state": state.as_ref().map(|(s, m)| json!({ "state": s, "members": m })),
                "partitions": rows,
                "records_reread": reread,
                "records_skipped": skipped,
                "warnings": warnings,
            }),
            summary: format!(
                "reset the offsets of group '{group}' on topic '{topic}' to {shown_strategy}: \
                 {} partition(s), {}",
                plans.len(),
                effect.join(", ")
            ),
            target: format!("{group}@{topic}"),
            destructive: false,
            person_only,
            work: Work::ResetOffsets {
                group: group.to_string(),
                topic: topic.to_string(),
                strategy: shown_strategy,
                plans,
            },
        })
    }

    async fn plan_create_topic(
        &self,
        topic: &str,
        partitions: i32,
        replication_factor: Option<i32>,
        configs: &BTreeMap<String, String>,
    ) -> Result<Planned, ToolError> {
        if self
            .backend
            .topic(topic)
            .await
            .map_err(ToolError::kafka)?
            .is_some()
        {
            return Err(ToolError::invalid(format!(
                "Topic '{topic}' already exists"
            )));
        }
        let brokers = self
            .backend
            .broker_count()
            .await
            .map_err(ToolError::kafka)?;
        if let Some(rf) = replication_factor {
            if rf as usize > brokers {
                return Err(ToolError::invalid(format!(
                    "replication_factor {rf} is more than the {brokers} broker(s) in the cluster"
                )));
            }
        }
        let rf = replication_factor.unwrap_or(-1);
        let configs_map: HashMap<String, String> = configs.clone().into_iter().collect();
        self.backend
            .create_topic(topic, partitions, rf, &configs_map, true)
            .await
            .map_err(|e| ToolError::kafka(format!("The broker rejected the topic: {e}")))?;
        let mut warnings = Vec::new();
        if topic.contains('.') && topic.contains('_') {
            warnings.push(
                "The name mixes '.' and '_'; Kafka metric names treat them alike, so it can \
                 collide with a similar topic name."
                    .to_string(),
            );
        }
        if rf == 1 {
            warnings.push("Replication factor 1: losing that broker loses the data.".to_string());
        }
        let shown_configs: BTreeMap<&String, String> = configs
            .iter()
            .map(|(k, v)| (k, shown_value(k, v, false)))
            .collect();
        let summary = format!(
            "create topic '{topic}' with {partitions} partition(s), replication factor {}{}",
            rf_text(rf),
            if configs.is_empty() {
                String::new()
            } else {
                format!(" and {} config(s)", configs.len())
            }
        );
        Ok(Planned {
            plan: json!({
                "action": "create_topic",
                "topic": topic,
                "partitions": partitions,
                "replication_factor": rf_text(rf),
                "configs": shown_configs,
                "brokers": brokers,
                "validated_by_broker": true,
                "warnings": warnings,
            }),
            summary,
            target: topic.to_string(),
            destructive: false,
            person_only: None,
            work: Work::CreateTopic {
                topic: topic.to_string(),
                partitions,
                replication_factor: rf,
                configs: configs_map,
            },
        })
    }

    async fn plan_alter_config(
        &self,
        topic: &str,
        set: &BTreeMap<String, String>,
        delete: &[String],
    ) -> Result<Planned, ToolError> {
        let Some(shape) = self.backend.topic(topic).await.map_err(ToolError::kafka)? else {
            return Err(ToolError::invalid(format!(
                "Topic '{topic}' does not exist"
            )));
        };
        let current: HashMap<String, (String, bool, bool)> = self
            .backend
            .topic_configs(topic)
            .await
            .map_err(ToolError::kafka)?
            .into_iter()
            .map(|e| {
                let overridden =
                    e.source == ferrika_core::kafka::admin::ConfigOrigin::TopicOverride;
                (e.name, (e.value, overridden, e.sensitive))
            })
            .collect();
        let mut changes = Vec::new();
        let mut rows = Vec::new();
        let mut warnings = Vec::new();
        let mut destructive = false;
        for (key, value) in set {
            let now = current.get(key);
            let secret = now.is_some_and(|c| c.2);
            if let Some((old, _, _)) = now {
                if let Some(warning) = shrink_warning(key, old, value) {
                    warnings.push(warning);
                    destructive = true;
                }
            }
            if key == "cleanup.policy" && now.is_some_and(|c| c.0 != *value) {
                warnings.push(format!(
                    "cleanup.policy changes to '{value}': with 'compact' old values of a key are \
                     removed, with 'delete' records expire by retention."
                ));
                // Either way records the topic keeps today can go.
                destructive = true;
            }
            if key == "min.insync.replicas"
                && value
                    .parse::<i32>()
                    .is_ok_and(|n| n > shape.replication_factor)
            {
                warnings.push(format!(
                    "min.insync.replicas {value} is more than the replication factor {}: \
                     producers with acks=all will fail.",
                    shape.replication_factor
                ));
            }
            rows.push(json!({
                "key": key,
                "current": now.map(|c| shown_value(key, &c.0, secret)),
                "source": now.map(|c| if c.1 { "topic" } else { "default" }),
                "new": shown_value(key, value, secret),
            }));
            changes.push(ConfigChange::Set(key.clone(), value.clone()));
        }
        for key in delete {
            let now = current.get(key);
            let secret = now.is_some_and(|c| c.2);
            if now.is_some_and(|c| !c.1) {
                warnings.push(format!(
                    "'{key}' is not set on the topic; deleting it changes nothing."
                ));
            } else if now.is_some() && is_retention_key(key) {
                // The broker default takes over, and it may be shorter (an
                // override of -1, keep forever, is common on archives).
                warnings.push(format!(
                    "Deleting '{key}' falls back to the broker default, which may be shorter: \
                     the broker then deletes older records, which cannot be undone."
                ));
                destructive = true;
            }
            rows.push(json!({
                "key": key,
                "current": now.map(|c| shown_value(key, &c.0, secret)),
                "source": now.map(|c| if c.1 { "topic" } else { "default" }),
                "new": Value::Null,
                "reset_to_default": true,
            }));
            changes.push(ConfigChange::Delete(key.clone()));
        }
        self.backend
            .alter_config(topic, &changes, true)
            .await
            .map_err(|e| ToolError::kafka(format!("The broker rejected the change: {e}")))?;
        Ok(Planned {
            plan: json!({
                "action": "alter_topic_config",
                "topic": topic,
                "changes": rows,
                "validated_by_broker": true,
                "warnings": warnings,
                "destructive": destructive,
            }),
            summary: format!(
                "change the config of topic '{topic}': {}",
                changes_text_with(&changes, &current)
            ),
            target: topic.to_string(),
            destructive,
            person_only: None,
            work: Work::AlterConfig {
                topic: topic.to_string(),
                changes,
            },
        })
    }
}

/// A parsed write request.
enum Request {
    CreateTopic {
        topic: String,
        partitions: i32,
        replication_factor: Option<i32>,
        configs: BTreeMap<String, String>,
    },
    AlterConfig {
        topic: String,
        set: BTreeMap<String, String>,
        delete: Vec<String>,
    },
    ResetOffsets {
        group: String,
        topic: String,
        partition: Option<i32>,
        strategy: ResetStrategy,
    },
    Produce {
        topic: String,
        records: Vec<RecordArg>,
        value_subject: Option<String>,
        key_subject: Option<String>,
    },
    Transform(TransformRun),
}

/// One record of a produce_messages call.
struct RecordArg {
    key: Option<String>,
    /// `None` is a tombstone.
    value: Option<String>,
    partition: Option<i32>,
    headers: BTreeMap<String, String>,
}

impl Request {
    /// The topics the request changes, for the scope check.
    fn topics(&self) -> Vec<&str> {
        match self {
            Request::CreateTopic { topic, .. }
            | Request::AlterConfig { topic, .. }
            | Request::ResetOffsets { topic, .. }
            | Request::Produce { topic, .. } => vec![topic.as_str()],
            // The source is only read.
            Request::Transform(run) => vec![run.dst_topic.as_str()],
        }
    }

    /// The audit target before planning.
    fn target(&self) -> String {
        match self {
            Request::CreateTopic { topic, .. }
            | Request::AlterConfig { topic, .. }
            | Request::Produce { topic, .. } => topic.clone(),
            Request::ResetOffsets { group, topic, .. } => format!("{group}@{topic}"),
            Request::Transform(run) => format!("{} -> {}", run.src_topic, run.dst_topic),
        }
    }
}

/// Parses and checks `args` for `tool`; unknown fields are refused (a typo
/// must not silently change what runs).
fn parse(tool: &str, args: &Value) -> Result<Request, ToolError> {
    let fields = Fields::new(args)?;
    match tool {
        "create_topic" => {
            fields.only(&[
                "topic",
                "partitions",
                "replication_factor",
                "configs",
                "mode",
                "confirmation",
            ])?;
            let topic = fields.string("topic")?;
            if !valid_topic_name(&topic) {
                return Err(ToolError::invalid(format!(
                    "'{topic}' is not a valid topic name: 1-249 characters from A-Z a-z 0-9 . _ -, \
                     and not '.' or '..'"
                )));
            }
            Ok(Request::CreateTopic {
                topic,
                partitions: fields.int("partitions", 1, 1000)?.unwrap_or(1) as i32,
                replication_factor: fields
                    .int("replication_factor", 1, i16::MAX as i64)?
                    .map(|n| n as i32),
                configs: fields.string_map("configs")?,
            })
        }
        "alter_topic_config" => {
            fields.only(&["topic", "set", "delete", "mode", "confirmation"])?;
            let topic = fields.string("topic")?;
            let set = fields.string_map("set")?;
            let delete = fields.string_list("delete")?;
            if set.is_empty() && delete.is_empty() {
                return Err(ToolError::invalid(
                    "Nothing to change: give 'set' (key -> value) and/or 'delete' (keys)",
                ));
            }
            if let Some(key) = delete.iter().find(|k| set.contains_key(*k)) {
                return Err(ToolError::invalid(format!(
                    "'{key}' is both set and deleted"
                )));
            }
            if set.keys().chain(delete.iter()).any(|k| k.trim().is_empty()) {
                return Err(ToolError::invalid("A config key is empty"));
            }
            Ok(Request::AlterConfig { topic, set, delete })
        }
        "reset_consumer_group_offsets" => {
            fields.only(&["group", "topic", "partition", "to", "mode", "confirmation"])?;
            Ok(Request::ResetOffsets {
                group: fields.string("group")?,
                topic: fields.string("topic")?,
                partition: fields
                    .int("partition", 0, i32::MAX as i64)?
                    .map(|p| p as i32),
                strategy: reset_strategy(fields.0.get("to"))?,
            })
        }
        "produce_messages" => {
            fields.only(&[
                "topic",
                "records",
                "value_subject",
                "key_subject",
                "mode",
                "confirmation",
            ])?;
            let topic = fields.string("topic")?;
            let Some(Value::Array(items)) = fields.0.get("records") else {
                return Err(ToolError::invalid("'records' must be an array of records"));
            };
            if items.is_empty() || items.len() > PRODUCE_MAX_RECORDS {
                return Err(ToolError::invalid(format!(
                    "'records' must have 1 to {PRODUCE_MAX_RECORDS} records"
                )));
            }
            let records = items
                .iter()
                .enumerate()
                .map(|(i, item)| record_arg(i, item))
                .collect::<Result<Vec<_>, _>>()?;
            let subject = |name: &str| match fields.0.get(name) {
                None | Some(Value::Null) => Ok(None),
                Some(_) => fields.string(name).map(Some),
            };
            let value_subject = subject("value_subject")?;
            let key_subject = subject("key_subject")?;
            Ok(Request::Produce {
                topic,
                records,
                value_subject,
                key_subject,
            })
        }
        "transform_execute" => parse_transform(&fields, args),
        _ => Err(ToolError::new(
            "write_disabled",
            format!("{tool} is not available"),
        )),
    }
}

/// Arguments that would name another cluster: refused with a clear
/// message rather than as unknown.
const OTHER_CLUSTER_ARGS: [&str; 6] = [
    "dst_profile",
    "dst_bootstrap_servers",
    "destination_profile",
    "destination_cluster",
    "profile",
    "bootstrap_servers",
];

/// `transform_execute`'s arguments.
fn parse_transform(fields: &Fields<'_>, args: &Value) -> Result<Request, ToolError> {
    if let Some(key) = OTHER_CLUSTER_ARGS
        .iter()
        .find(|k| fields.0.contains_key(**k))
    {
        return Err(ToolError::new(
            "cross_cluster_unsupported",
            format!(
                "'{key}': transform_execute copies within the cluster this MCP server was \
                 started with. Copying to another cluster is not offered over MCP; run \
                 `ferrika transform --dst-profile ... --execute` from the CLI, where the \
                 export policy applies."
            ),
        ));
    }
    fields.only(&[
        "topic",
        "to",
        "steps",
        "from_beginning",
        "from_timestamp",
        "partition",
        "max_records",
        "decode_wire_format",
        "mode",
        "confirmation",
    ])?;
    let src_topic = fields.string("topic")?;
    let dst_topic = fields.string("to")?;
    if src_topic == dst_topic {
        return Err(ToolError::invalid(format!(
            "Source and destination are the same topic '{src_topic}'; the transform would read \
             its own output. Use a different destination topic."
        )));
    }
    if ferrika_core::kafka::internal_topic::is_internal_topic(&src_topic) {
        return Err(ToolError::new(
            "target_not_allowed",
            format!("'{src_topic}' is an internal topic; MCP tools do not copy those"),
        ));
    }
    if !valid_topic_name(&dst_topic) {
        return Err(ToolError::invalid(format!(
            "'{dst_topic}' is not a valid topic name"
        )));
    }
    let Some(max_records) = fields.int("max_records", 1, EXECUTE_MAX as i64)? else {
        return Err(ToolError::invalid(format!(
            "'max_records' is required (1 to {EXECUTE_MAX})"
        )));
    };
    Ok(Request::Transform(TransformRun {
        src_topic,
        dst_topic,
        steps: transform_tools::parse_steps(fields.0.get("steps")).map_err(ToolError::invalid)?,
        // Absolute times only: the plan and the execute must name the same range.
        start: transform_tools::parse_start(args, None).map_err(ToolError::invalid)?,
        partition: transform_tools::parse_partition(args).map_err(ToolError::invalid)?,
        max_records: max_records as u64,
        decode: transform_tools::parse_flag(args, "decode_wire_format", false)
            .map_err(ToolError::invalid)?,
    }))
}

/// `records[i]` of a produce_messages call.
fn record_arg(i: usize, item: &Value) -> Result<RecordArg, ToolError> {
    let fields = Fields::new(item)
        .map_err(|_| ToolError::invalid(format!("records[{i}] must be an object")))?;
    fields
        .only(&["key", "value", "partition", "headers"])
        .map_err(|e| ToolError::invalid(format!("records[{i}]: {}", e.message)))?;
    let text = |name: &str| match fields.0.get(name) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) => Ok(Some(s.clone())),
        Some(_) => Err(ToolError::invalid(format!(
            "records[{i}].{name} must be a string or null"
        ))),
    };
    if !fields.0.contains_key("value") {
        return Err(ToolError::invalid(format!(
            "records[{i}].value is required (null for a tombstone)"
        )));
    }
    Ok(RecordArg {
        key: text("key")?,
        value: text("value")?,
        partition: fields
            .int("partition", 0, i32::MAX as i64)
            .map_err(|e| ToolError::invalid(format!("records[{i}]: {}", e.message)))?
            .map(|p| p as i32),
        headers: fields
            .string_map("headers")
            .map_err(|e| ToolError::invalid(format!("records[{i}]: {}", e.message)))?,
    })
}

/// The first 12 hex digits of SHA-256: enough to tell values apart without
/// returning them.
pub(super) fn short_sha256(bytes: &[u8]) -> String {
    let d = ring::digest::digest(&ring::digest::SHA256, bytes);
    ferrika_governance::policy::to_hex(&d.as_ref()[..6])
}

/// `to`: `earliest`, `latest`, `{offset: N}`, `{shift: N}` (optionally with
/// `uncommitted_from: "earliest" | "latest"`) or `{timestamp: "RFC 3339"}`.
fn reset_strategy(to: Option<&Value>) -> Result<ResetStrategy, ToolError> {
    let bad = || {
        ToolError::invalid(
            "'to' must be \"earliest\", \"latest\", {\"offset\": N}, {\"shift\": N} (with \
             \"uncommitted_from\": \"earliest\" or \"latest\" to shift partitions without a \
             committed offset) or {\"timestamp\": \"RFC 3339\"}",
        )
    };
    match to {
        Some(Value::String(s)) if s == "earliest" => Ok(ResetStrategy::Earliest),
        Some(Value::String(s)) if s == "latest" => Ok(ResetStrategy::Latest),
        Some(Value::Object(map)) if map.len() == 2 && map.contains_key("uncommitted_from") => {
            let delta = map.get("shift").and_then(Value::as_i64).ok_or_else(bad)?;
            let base = match map.get("uncommitted_from").and_then(Value::as_str) {
                Some("earliest") => UncommittedBase::Earliest,
                Some("latest") => UncommittedBase::Latest,
                _ => return Err(bad()),
            };
            Ok(ResetStrategy::Shift {
                delta,
                uncommitted: Some(base),
            })
        }
        Some(Value::Object(map)) if map.len() == 1 => {
            let (key, value) = map.iter().next().ok_or_else(bad)?;
            match key.as_str() {
                "offset" => value
                    .as_i64()
                    .filter(|n| *n >= 0)
                    .map(ResetStrategy::Specific)
                    .ok_or_else(bad),
                "shift" => value.as_i64().map(ResetStrategy::shift).ok_or_else(bad),
                "timestamp" => value
                    .as_str()
                    .and_then(|t| t.parse::<jiff::Timestamp>().ok())
                    .map(|t| ResetStrategy::Timestamp(t.as_millisecond()))
                    .ok_or_else(bad),
                _ => Err(bad()),
            }
        }
        _ => Err(bad()),
    }
}

/// `mode`: `"plan"` (default) or `"execute"`.
fn execute_mode(args: &Value) -> Result<bool, ToolError> {
    match args.get("mode") {
        None | Some(Value::Null) => Ok(false),
        Some(Value::String(m)) if m == "plan" => Ok(false),
        Some(Value::String(m)) if m == "execute" => Ok(true),
        Some(other) => Err(ToolError::invalid(format!(
            "mode must be \"plan\" or \"execute\", not {other}"
        ))),
    }
}

/// Typed access to a tool's arguments object.
pub(super) struct Fields<'a>(pub(super) &'a Map<String, Value>);

impl<'a> Fields<'a> {
    pub(super) fn new(args: &'a Value) -> Result<Self, ToolError> {
        args.as_object()
            .map(Fields)
            .ok_or_else(|| ToolError::invalid("arguments must be an object"))
    }

    pub(super) fn only(&self, known: &[&str]) -> Result<(), ToolError> {
        match self.0.keys().find(|k| !known.contains(&k.as_str())) {
            Some(k) => Err(ToolError::invalid(format!(
                "Unknown argument '{k}' (expected: {})",
                known.join(", ")
            ))),
            None => Ok(()),
        }
    }

    pub(super) fn string(&self, name: &str) -> Result<String, ToolError> {
        match self.0.get(name) {
            Some(Value::String(s)) if !s.is_empty() => Ok(s.clone()),
            Some(Value::String(_)) | None | Some(Value::Null) => {
                Err(ToolError::invalid(format!("'{name}' is required")))
            }
            Some(_) => Err(ToolError::invalid(format!("'{name}' must be a string"))),
        }
    }

    pub(super) fn int(&self, name: &str, min: i64, max: i64) -> Result<Option<i64>, ToolError> {
        match self.0.get(name) {
            None | Some(Value::Null) => Ok(None),
            Some(v) => match v.as_i64() {
                Some(n) if (min..=max).contains(&n) => Ok(Some(n)),
                _ => Err(ToolError::invalid(format!(
                    "'{name}' must be an integer from {min} to {max}"
                ))),
            },
        }
    }

    pub(super) fn string_map(&self, name: &str) -> Result<BTreeMap<String, String>, ToolError> {
        match self.0.get(name) {
            None | Some(Value::Null) => Ok(BTreeMap::new()),
            Some(Value::Object(map)) => map
                .iter()
                .map(|(k, v)| match v {
                    Value::String(s) => Ok((k.clone(), s.clone())),
                    _ => Err(ToolError::invalid(format!("'{name}.{k}' must be a string"))),
                })
                .collect(),
            Some(_) => Err(ToolError::invalid(format!(
                "'{name}' must be an object of strings"
            ))),
        }
    }

    fn string_list(&self, name: &str) -> Result<Vec<String>, ToolError> {
        match self.0.get(name) {
            None | Some(Value::Null) => Ok(Vec::new()),
            Some(Value::Array(items)) => items
                .iter()
                .map(|v| {
                    v.as_str()
                        .map(str::to_string)
                        .ok_or_else(|| ToolError::invalid(format!("'{name}' must list strings")))
                })
                .collect(),
            Some(_) => Err(ToolError::invalid(format!("'{name}' must be an array"))),
        }
    }
}

fn valid_topic_name(name: &str) -> bool {
    (1..=249).contains(&name.len())
        && name != "."
        && name != ".."
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
}

fn rf_text(rf: i32) -> Value {
    if rf < 0 {
        json!("broker default")
    } else {
        json!(rf)
    }
}

/// Whether a config value may be a secret by its key alone.
fn secret_key(key: &str) -> bool {
    let key = key.to_ascii_lowercase();
    key.contains("password") || key.contains("secret") || key == "sasl.jaas.config"
}

/// A config value as plans and audit entries show it: secrets as `****`.
fn shown_value(key: &str, value: &str, sensitive: bool) -> String {
    if sensitive || secret_key(key) {
        "****".to_string()
    } else {
        value.to_string()
    }
}

/// A warning when setting `key` from `old` to `new` lets the broker delete
/// data sooner (`retention.ms` / `retention.bytes` lowered; -1 = unlimited).
/// Keys whose smaller value makes the broker delete records.
fn is_retention_key(key: &str) -> bool {
    matches!(
        key,
        "retention.ms" | "retention.bytes" | "local.retention.ms" | "local.retention.bytes"
    )
}

fn shrink_warning(key: &str, old: &str, new: &str) -> Option<String> {
    if !is_retention_key(key) {
        return None;
    }
    let (old, new) = (
        old.trim().parse::<i64>().ok()?,
        new.trim().parse::<i64>().ok()?,
    );
    let unlimited = |n: i64| n < 0;
    let shrinks = match (unlimited(old), unlimited(new)) {
        (true, false) => true,
        (false, false) => new < old,
        _ => false,
    };
    shrinks.then(|| {
        format!(
            "{key} goes down from {old} to {new}: the broker deletes records past the new limit, \
             which cannot be undone."
        )
    })
}

/// `retention.ms=86400000, delete cleanup.policy` (secrets masked).
fn changes_text(changes: &[ConfigChange]) -> String {
    changes_text_with(changes, &HashMap::new())
}

fn changes_text_with(
    changes: &[ConfigChange],
    current: &HashMap<String, (String, bool, bool)>,
) -> String {
    changes
        .iter()
        .map(|c| match c {
            ConfigChange::Set(k, v) => {
                let secret = current.get(k).is_some_and(|c| c.2);
                match current.get(k) {
                    Some((old, _, _)) => format!(
                        "{k} {} -> {}",
                        shown_value(k, old, secret),
                        shown_value(k, v, secret)
                    ),
                    None => format!("{k}={}", shown_value(k, v, secret)),
                }
            }
            ConfigChange::Delete(k) => format!("delete {k}"),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// The digest that binds a plan's confirmation token: the plan without its
/// `observed` part (counts a busy broker changes between plan and
/// execute).
fn plan_digest(plan: &Value) -> String {
    let mut stable = plan.clone();
    if let Some(map) = stable.as_object_mut() {
        map.remove("observed");
    }
    digest(&stable)
}

fn state_changed() -> ToolError {
    ToolError::new(
        "state_changed",
        "The cluster changed since the plan was made (or the plan differs); run mode \"plan\" \
         again and confirm the new plan.",
    )
}

/// The result of a plan call.
fn plan_result(
    tool: &str,
    planned: &Planned,
    token: &str,
    expires: jiff::Timestamp,
    asks_person: bool,
) -> Value {
    let next = if asks_person {
        format!(
            "To run it, call {tool} again with the same arguments and mode \"execute\"; the \
             user is asked to confirm."
        )
    } else {
        format!(
            "To run it, call {tool} again with the same arguments, mode \"execute\" and \
             confirmation \"{token}\" (valid until {}, once).",
            ferrika_core::model::time::rfc3339(expires)
        )
    };
    let structured = json!({
        "plan": planned.plan,
        "confirmation": {
            "token": token,
            "expires_at": ferrika_core::model::time::rfc3339(expires),
            "summary": planned.summary,
            "destructive": planned.destructive,
            "by": if asks_person { "user" } else { "token" },
        },
    });
    json!({
        "content": [{
            "type": "text",
            "text": format!(
                "Plan: {}.\n{next}\n{}",
                planned.summary,
                serde_json::to_string_pretty(&structured).unwrap_or_default()
            )
        }],
        "structuredContent": structured,
        "isError": false
    })
}

/// The result of an execute call.
fn execute_result(done: &Executed, audit_warning: Option<String>) -> Value {
    let mut structured = json!({ "executed": true, "result": done.result });
    if let Some(warning) = audit_warning {
        structured["audit_warning"] = json!(warning);
    }
    json!({
        "content": [{
            "type": "text",
            "text": serde_json::to_string_pretty(&structured).unwrap_or_default()
        }],
        "structuredContent": structured,
        "isError": false
    })
}

/// Tool annotations: hints for the client's approval UI, not a guarantee.
pub(super) fn annotations(title: &str, destructive: bool, idempotent: bool) -> Value {
    json!({
        "title": title,
        "readOnlyHint": false,
        "destructiveHint": destructive,
        "idempotentHint": idempotent,
        "openWorldHint": false
    })
}

/// `mode` and `confirmation`, shared by every write tool's schema.
pub(super) fn with_mode(mut properties: Value) -> Value {
    properties["mode"] = json!({
        "enum": ["plan", "execute"],
        "default": "plan",
        "description": "plan (default): show what would change and get a confirmation token. execute: run it."
    });
    properties["confirmation"] = json!({
        "type": "string",
        "description": "The confirmation token from the plan (mode execute)."
    });
    properties
}

/// Every write tool's definition: the Kafka ones, then the RabbitMQ ones.
fn definitions() -> Vec<Value> {
    let mut tools = kafka_definitions();
    tools.extend(rabbit_writes::definitions());
    tools
}

/// The Kafka write tools' definitions.
fn kafka_definitions() -> Vec<Value> {
    vec![
        json!({
            "name": "produce_messages",
            "title": "Produce messages",
            "description": "Sends up to 100 records (UTF-8 text) to an existing topic. The plan shows each record's partition, size and a short hash, never the values; execute with the plan's confirmation token. Results list each record's partition and offset, or its error.",
            "inputSchema": {
                "type": "object",
                "additionalProperties": false,
                "properties": with_mode(json!({
                    "topic": { "type": "string", "minLength": 1 },
                    "records": {
                        "type": "array", "minItems": 1, "maxItems": PRODUCE_MAX_RECORDS,
                        "items": {
                            "type": "object",
                            "additionalProperties": false,
                            "properties": {
                                "key": { "type": ["string", "null"] },
                                "value": { "type": ["string", "null"], "description": "UTF-8 text; null = tombstone" },
                                "partition": { "type": "integer", "minimum": 0 },
                                "headers": { "type": "object", "additionalProperties": { "type": "string" } }
                            },
                            "required": ["value"]
                        }
                    },
                    "value_subject": { "type": "string", "description": "Schema Registry subject to encode values with (Avro/Protobuf/JSON Schema); values are then JSON" },
                    "key_subject": { "type": "string", "description": "Schema Registry subject to encode keys with (e.g. <topic>-key); keys are then JSON" }
                })),
                "required": ["topic", "records"]
            },
            "annotations": annotations("Produce messages", false, false)
        }),
        json!({
            "name": "create_topic",
            "title": "Create topic",
            "description": "Creates a topic. Plan first (the broker validates the request without creating anything), then execute with the plan's confirmation token.",
            "inputSchema": {
                "type": "object",
                "additionalProperties": false,
                "properties": with_mode(json!({
                    "topic": { "type": "string", "pattern": "^[A-Za-z0-9._-]{1,249}$" },
                    "partitions": { "type": "integer", "minimum": 1, "maximum": 1000, "default": 1 },
                    "replication_factor": { "type": "integer", "minimum": 1, "description": "Default: the broker's default.replication.factor" },
                    "configs": { "type": "object", "additionalProperties": { "type": "string" }, "description": "Topic configs, e.g. {\"retention.ms\": \"86400000\"}" }
                })),
                "required": ["topic"]
            },
            "annotations": annotations("Create topic", false, true)
        }),
        json!({
            "name": "alter_topic_config",
            "title": "Change topic config",
            "description": "Sets or removes configs on a topic (other configs stay as they are). The plan shows each key's current and new value; lowering retention deletes data and needs the topic name typed back when a person confirms.",
            "inputSchema": {
                "type": "object",
                "additionalProperties": false,
                "properties": with_mode(json!({
                    "topic": { "type": "string", "minLength": 1 },
                    "set": { "type": "object", "additionalProperties": { "type": "string" }, "description": "key -> new value" },
                    "delete": { "type": "array", "items": { "type": "string" }, "description": "keys to reset to the broker default" }
                })),
                "required": ["topic"],
                "minProperties": 2
            },
            "annotations": annotations("Change topic config", true, true)
        }),
        json!({
            "name": "reset_consumer_group_offsets",
            "title": "Reset consumer group offsets",
            "description": "Moves a consumer group's committed offsets on a topic. The plan shows each partition's current and new offset and how many records are re-read or skipped. The group must have no active members. A shift is refused when the group has no committed offset on a partition it would move, unless 'uncommitted_from' says where those partitions start.",
            "inputSchema": {
                "type": "object",
                "additionalProperties": false,
                "properties": with_mode(json!({
                    "group": { "type": "string", "minLength": 1 },
                    "topic": { "type": "string", "minLength": 1 },
                    "partition": { "type": "integer", "minimum": 0, "description": "Only this partition (default: all)" },
                    "to": {
                        "description": "earliest | latest | {offset:N} | {shift:N, uncommitted_from?: earliest|latest} | {timestamp:\"RFC 3339\"}",
                        "oneOf": [
                            { "enum": ["earliest", "latest"] },
                            { "type": "object", "properties": { "offset": { "type": "integer", "minimum": 0 } }, "required": ["offset"], "additionalProperties": false },
                            { "type": "object", "properties": { "shift": { "type": "integer" }, "uncommitted_from": { "enum": ["earliest", "latest"], "description": "Where to shift partitions without a committed offset from; without it such a shift is refused" } }, "required": ["shift"], "additionalProperties": false },
                            { "type": "object", "properties": { "timestamp": { "type": "string", "format": "date-time" } }, "required": ["timestamp"], "additionalProperties": false }
                        ]
                    }
                })),
                "required": ["group", "topic", "to"]
            },
            "annotations": annotations("Reset consumer group offsets", true, true)
        }),
        json!({
            "name": "transform_execute",
            "title": "Transform records into another topic",
            "description": "Copies records of a topic into another existing topic on the same cluster through steps (as transform_preview: filter, drop, rename, set, rekey, mask, sample), reading from the beginning (or from_timestamp) up to the end each partition has when it starts, and writing at most max_records. Try the steps with transform_preview first. The plan shows the steps and the range; nothing is read or written until execute. When the company policy enforces PII masking the first step must be mask. Copying to another cluster is not offered over MCP.",
            "inputSchema": {
                "type": "object",
                "additionalProperties": false,
                "properties": with_mode(json!({
                    "topic": { "type": "string", "minLength": 1, "description": "Topic to read" },
                    "to": { "type": "string", "pattern": "^[A-Za-z0-9._-]{1,249}$", "description": "Existing topic on the same cluster to write to" },
                    "steps": transform_tools::steps_schema(),
                    "from_beginning": { "type": "boolean", "default": true, "description": "Start at the earliest offset (the default)" },
                    "from_timestamp": { "type": ["string", "integer"], "description": "Start at the first record at or after this time instead: ISO 8601 or epoch ms" },
                    "partition": { "type": "integer", "minimum": 0, "description": "Only this partition of the source" },
                    "max_records": { "type": "integer", "minimum": 1, "maximum": EXECUTE_MAX, "description": "Most records to write; counts against --max-produce-records" },
                    "decode_wire_format": { "type": "boolean", "default": false, "description": "Decode Schema Registry wire-format values to JSON first (written as JSON)" }
                })),
                "required": ["topic", "to", "steps", "max_records"]
            },
            "annotations": annotations("Transform records into another topic", false, false)
        }),
    ]
}

/// The result for a write tool this server does not offer.
pub(super) fn disabled(name: &str) -> Value {
    ToolError::new(
        "write_disabled",
        format!(
            "{name} is not enabled: start the server with `ferrika mcp --allow-write` \
             (and list it in --allow-tools if that is set)"
        ),
    )
    .into_result()
}

/// The name a person types back for a destructive change: the topic.
fn confirm_name(target: &str) -> &str {
    target.rsplit('@').next().unwrap_or(target)
}

/// Whether `name` is a write tool (offered or not).
pub(super) fn is_write_tool(name: &str) -> bool {
    WRITE_TOOLS.contains(&name)
}

#[cfg(test)]
pub(super) mod tests {
    use super::super::backend::fake::FakeCluster;
    use super::super::guard::tests::{config, guard_with, no_policy, MemoryLog, TestClock};
    use super::super::guard::GuardConfig;
    use super::*;

    pub fn tools_with(
        cluster: FakeCluster,
        config: GuardConfig,
    ) -> (WriteTools, &'static FakeCluster, MemoryLog, TestClock) {
        let cluster: &'static FakeCluster = Box::leak(Box::new(cluster));
        let (guard, log, clock) = guard_with(config, no_policy());
        (
            WriteTools::new(guard, Box::new(cluster)),
            cluster,
            log,
            clock,
        )
    }

    impl WriteTools {
        /// A call from a client without elicitation.
        pub async fn call_token(&mut self, name: &str, args: &Value) -> Value {
            let peer = Peer::new(tokio::sync::mpsc::unbounded_channel().0);
            let ctx = CallContext {
                peer: &peer,
                request_id: &json!(1),
                elicitation: false,
            };
            self.call(name, args, &ctx).await
        }
    }

    pub fn code(result: &Value) -> &str {
        result["structuredContent"]["error"]["code"]
            .as_str()
            .unwrap_or("")
    }

    pub fn token(result: &Value) -> String {
        result["structuredContent"]["confirmation"]["token"]
            .as_str()
            .unwrap_or_default()
            .to_string()
    }

    /// `args` with mode execute and the token.
    pub fn execute(args: &Value, token: &str) -> Value {
        let mut args = args.clone();
        args["mode"] = json!("execute");
        args["confirmation"] = json!(token);
        args
    }

    fn cluster() -> FakeCluster {
        FakeCluster {
            brokers: 3,
            ..Default::default()
        }
        .with_topic("orders", 6)
    }

    #[test]
    fn definitions_are_strict_and_annotated() {
        for def in definitions() {
            assert_eq!(def["inputSchema"]["additionalProperties"], false, "{def}");
            assert_eq!(def["annotations"]["readOnlyHint"], false);
            assert_eq!(def["annotations"]["openWorldHint"], false);
            assert!(is_write_tool(def["name"].as_str().unwrap()));
        }
        let destructive = |name: &str| {
            definitions()
                .into_iter()
                .find(|d| d["name"] == name)
                .map(|d| d["annotations"]["destructiveHint"].clone())
        };
        assert_eq!(destructive("create_topic"), Some(json!(false)));
        assert_eq!(destructive("alter_topic_config"), Some(json!(true)));
    }

    #[tokio::test]
    async fn create_topic_plans_then_executes_with_the_token() {
        let (mut tools, cluster, log, _) = tools_with(cluster(), config());
        let args = json!({ "topic": "payments.v1", "partitions": 3, "replication_factor": 2 });
        let plan = tools.call_token("create_topic", &args).await;
        assert_eq!(plan["isError"], false, "{plan}");
        assert_eq!(plan["structuredContent"]["plan"]["partitions"], 3);
        assert!(cluster.changes.borrow().is_empty());
        // No audit entry for a plan.
        assert!(log.entries.borrow().is_empty());

        let missing = tools
            .call_token("create_topic", &json!({ "topic": "payments.v1", "partitions": 3, "replication_factor": 2, "mode": "execute" }))
            .await;
        assert_eq!(code(&missing), "confirmation_required");

        let token = token(&plan);
        let done = tools
            .call_token("create_topic", &execute(&args, &token))
            .await;
        assert_eq!(done["structuredContent"]["executed"], true, "{done}");
        assert_eq!(*cluster.changes.borrow(), ["create payments.v1 3 2"]);
        {
            let entries = log.entries.borrow();
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].action, "mcp create_topic");
            assert_eq!(entries[0].target, "payments.v1");
            assert_eq!(entries[0].source, "mcp");
            assert!(entries[0].actor.is_some());
        }

        // The token is spent.
        let again = tools
            .call_token("create_topic", &execute(&args, &token))
            .await;
        assert_ne!(again["isError"], false);
    }

    #[tokio::test]
    async fn an_execute_refused_for_its_token_uses_no_rate_slot() {
        let mut one_write = config();
        one_write.limits.writes_per_minute = 1;
        let (mut tools, cluster, _, _) = tools_with(cluster(), one_write);
        let args = json!({ "topic": "payments.v1" });
        let plan = tools.call_token("create_topic", &args).await;
        let bad = tools
            .call_token("create_topic", &execute(&args, "not-a-token"))
            .await;
        assert_eq!(code(&bad), "confirmation_invalid");
        let done = tools
            .call_token("create_topic", &execute(&args, &token(&plan)))
            .await;
        assert_eq!(done["structuredContent"]["executed"], true, "{done}");
        assert_eq!(cluster.changes.borrow().len(), 1);
        // The one write of the minute is used now.
        let plan = tools
            .call_token("create_topic", &json!({ "topic": "t2" }))
            .await;
        let limited = tools
            .call_token(
                "create_topic",
                &execute(&json!({ "topic": "t2" }), &token(&plan)),
            )
            .await;
        assert_eq!(code(&limited), "rate_limited");
    }

    #[tokio::test]
    async fn create_topic_refuses_bad_input_and_existing_topics() {
        let (mut tools, cluster, _, _) = tools_with(cluster(), config());
        for (args, want) in [
            (json!({ "topic": "orders" }), "invalid_argument"),
            (json!({ "topic": "bad name" }), "invalid_argument"),
            (json!({ "topic": "t", "partitons": 3 }), "invalid_argument"),
            (
                json!({ "topic": "t", "replication_factor": 4 }),
                "invalid_argument",
            ),
            (json!({ "topic": "t", "mode": "now" }), "invalid_argument"),
            (json!({ "topic": "__internal" }), "target_not_allowed"),
        ] {
            let result = tools.call_token("create_topic", &args).await;
            assert_eq!(code(&result), want, "{args}: {result}");
        }
        assert!(cluster.changes.borrow().is_empty());
    }

    #[tokio::test]
    async fn changed_arguments_need_a_new_plan() {
        let (mut tools, cluster, _, _) = tools_with(cluster(), config());
        let args = json!({ "topic": "t1" });
        let token = token(&tools.call_token("create_topic", &args).await);
        let other = execute(&json!({ "topic": "t2" }), &token);
        assert_eq!(
            code(&tools.call_token("create_topic", &other).await),
            "args_changed"
        );
        assert!(cluster.changes.borrow().is_empty());
    }

    #[tokio::test]
    async fn alter_config_shows_current_values_masks_secrets_and_warns() {
        let c = cluster();
        c.set_config("orders", "retention.ms", "604800000", true);
        c.set_config("orders", "cleanup.policy", "delete", false);
        c.set_config("orders", "sasl.jaas.config", "secret-value", true);
        let (mut tools, cluster, log, _) = tools_with(c, config());
        let args = json!({
            "topic": "orders",
            "set": { "retention.ms": "86400000", "sasl.jaas.config": "new-secret" }
        });
        let plan = tools.call_token("alter_topic_config", &args).await;
        let p = &plan["structuredContent"]["plan"];
        assert_eq!(p["destructive"], true, "{plan}");
        let rows = p["changes"].as_array().unwrap();
        let retention = rows.iter().find(|r| r["key"] == "retention.ms").unwrap();
        assert_eq!(retention["current"], "604800000");
        assert_eq!(retention["source"], "topic");
        assert_eq!(retention["new"], "86400000");
        assert!(!plan.to_string().contains("secret-value"));
        assert!(!plan.to_string().contains("new-secret"));
        assert!(p["warnings"][0].as_str().unwrap().contains("retention.ms"));

        let done = tools
            .call_token("alter_topic_config", &execute(&args, &token(&plan)))
            .await;
        assert_eq!(done["structuredContent"]["executed"], true, "{done}");
        assert_eq!(cluster.changes.borrow().len(), 2);
        let entries = log.entries.borrow();
        assert!(entries[0].outcome.contains("retention.ms=86400000"));
        assert!(!entries[0].outcome.contains("new-secret"));
    }

    #[tokio::test]
    async fn a_config_changed_after_the_plan_is_refused() {
        let c = cluster();
        c.set_config("orders", "retention.ms", "604800000", true);
        let (mut tools, cluster, _, _) = tools_with(c, config());
        let args = json!({ "topic": "orders", "set": { "retention.ms": "700000000" } });
        let token = token(&tools.call_token("alter_topic_config", &args).await);
        cluster.set_config("orders", "retention.ms", "1000", true);
        let result = tools
            .call_token("alter_topic_config", &execute(&args, &token))
            .await;
        assert_eq!(code(&result), "state_changed");
        assert!(cluster.changes.borrow().is_empty());
    }

    #[tokio::test]
    async fn broker_validation_failures_stop_the_plan() {
        let c = FakeCluster {
            reject_validation: Some("Unknown topic config name: retention.nope".into()),
            ..cluster()
        };
        let (mut tools, _, _, _) = tools_with(c, config());
        let result = tools
            .call_token(
                "alter_topic_config",
                &json!({ "topic": "orders", "set": { "retention.nope": "1" } }),
            )
            .await;
        assert_eq!(code(&result), "kafka_error");
        assert!(result.to_string().contains("retention.nope"));
    }

    /// A journal in a fresh temporary directory.
    fn temp_journal(name: &str) -> Journal {
        let dir = std::env::temp_dir().join(format!(
            "ferrika-mcp-{name}-{}-{}",
            std::process::id(),
            ferrika_core::kafka::clock::now_ms()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        Journal::at(dir.join("history.jsonl"))
    }

    #[tokio::test]
    async fn executed_changes_are_recorded_for_undo_and_named_in_the_audit_log() {
        use ferrika_core::model::history::State;
        let c = reset_cluster();
        c.group_states
            .borrow_mut()
            .insert("billing".into(), ("Empty".into(), 0));
        c.set_config("orders", "retention.ms", "604800000", true);
        let (mut tools, _, log, _) = tools_with(c, config());
        let journal = temp_journal("history");
        tools.history = Some(journal.clone());

        let alter = json!({ "topic": "orders", "set": { "retention.ms": "86400000" } });
        let plan = tools.call_token("alter_topic_config", &alter).await;
        let done = tools
            .call_token("alter_topic_config", &execute(&alter, &token(&plan)))
            .await;
        assert_eq!(done["structuredContent"]["executed"], true, "{done}");
        let reset = json!({ "group": "billing", "topic": "orders", "to": "earliest" });
        let plan = tools
            .call_token("reset_consumer_group_offsets", &reset)
            .await;
        tools
            .call_token(
                "reset_consumer_group_offsets",
                &execute(&reset, &token(&plan)),
            )
            .await;
        // A plan alone records nothing.
        tools
            .call_token(
                "create_topic",
                &json!({ "topic": "audit-only", "partitions": 1 }),
            )
            .await;

        let ops = journal.load();
        assert_eq!(ops.len(), 2, "{ops:?}");
        let config = &ops[0];
        assert_eq!(config.begin.source, "mcp");
        assert_eq!(config.begin.action, "mcp alter_topic_config");
        assert_eq!(config.begin.reversibility, Reversibility::Reversible);
        let own = |states: &[State]| match &states[0] {
            State::TopicConfig { keys, .. } => keys["retention.ms"].own.clone(),
            other => panic!("{other:?}"),
        };
        assert_eq!(own(&config.begin.before).as_deref(), Some("604800000"));
        assert_eq!(own(&config.begin.after).as_deref(), Some("86400000"));
        assert!(config.end.as_ref().is_some_and(|e| e.ok));
        assert!(matches!(config.begin.before[0], State::TopicConfig { .. }));
        assert!(matches!(ops[1].begin.before[0], State::GroupOffsets { .. }));

        // Each result entry names its operation, as the CLI's do.
        let entries = log.entries.borrow();
        let results: Vec<&ferrika_governance::audit::AuditEntry> = entries
            .iter()
            .filter(|e| e.action.starts_with("mcp ") && !e.op.is_empty())
            .collect();
        assert_eq!(results.len(), 2, "{entries:?}");
        assert_eq!(results[0].op, ops[0].id);
        assert_eq!(results[1].op, ops[1].id);
        assert!(results.iter().all(|e| !e.op_digest.is_empty()));
    }

    #[tokio::test]
    async fn tools_outside_allow_tools_are_disabled_and_hidden() {
        let only_create = GuardConfig {
            tools: vec!["create_topic"],
            ..config()
        };
        let (mut tools, _, _, _) = tools_with(cluster(), only_create);
        let names: Vec<String> = tools
            .definitions(false)
            .iter()
            .map(|d| d["name"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(names, ["create_topic"]);
        let result = tools
            .call_token(
                "alter_topic_config",
                &json!({ "topic": "orders", "delete": ["x"] }),
            )
            .await;
        assert_eq!(code(&result), "write_disabled");
    }

    fn broker_policy(globs: &[&str]) -> &'static ferrika_governance::policy::EffectivePolicy {
        crate::guard::tests::policy(ferrika_core::model::policy::PolicyRules {
            read_only_brokers: globs.iter().map(|g| g.to_string()).collect(),
            ..Default::default()
        })
    }

    #[tokio::test]
    async fn broker_rules_see_the_brokers_the_cluster_reports() {
        let args = json!({ "topic": "payments.v1", "partitions": 3, "replication_factor": 2 });
        let ip_config = GuardConfig {
            bootstrap: "10.0.5.7:9092".into(),
            ..config()
        };
        // The user-given address matches nothing, the metadata broker does.
        let reported = FakeCluster {
            known: Some(Ok(vec!["kafka1.prod.corp:9092".into()])),
            ..cluster()
        };
        let cluster_ref: &'static FakeCluster = Box::leak(Box::new(reported));
        let (guard, _, _) =
            crate::guard::tests::guard_with(ip_config.clone(), broker_policy(&["*.prod.corp"]));
        let mut tools = WriteTools::new(guard, Box::new(cluster_ref));
        let refused = tools.call_token("create_topic", &args).await;
        assert_eq!(code(&refused), "policy_refused", "{refused}");
        assert_eq!(cluster_ref.known_calls.get(), 1);

        // A failing lookup refuses too.
        let failing: &'static FakeCluster = Box::leak(Box::new(FakeCluster {
            known: Some(Err("timed out".into())),
            ..cluster()
        }));
        let (guard, _, _) =
            crate::guard::tests::guard_with(ip_config.clone(), broker_policy(&["*.prod.corp"]));
        let mut tools = WriteTools::new(guard, Box::new(failing));
        let refused = tools.call_token("create_topic", &args).await;
        assert_eq!(code(&refused), "policy_refused", "{refused}");

        // No broker rule: no lookup at all, and the call goes through.
        let quiet: &'static FakeCluster = Box::leak(Box::new(FakeCluster {
            known: Some(Err("must not be asked".into())),
            ..cluster()
        }));
        let (guard, _, _) = crate::guard::tests::guard_with(ip_config, no_policy());
        let mut tools = WriteTools::new(guard, Box::new(quiet));
        let plan = tools.call_token("create_topic", &args).await;
        assert_eq!(plan["isError"], false, "{plan}");
        assert_eq!(quiet.known_calls.get(), 0);
    }

    fn mcp_write_policy(
        rule: ferrika_core::model::policy::McpWriteRule,
    ) -> &'static ferrika_governance::policy::EffectivePolicy {
        crate::guard::tests::policy(ferrika_core::model::policy::PolicyRules {
            mcp_write: rule,
            ..Default::default()
        })
    }

    #[tokio::test]
    async fn mcp_write_forbidden_hides_the_tools_and_refuses_calls() {
        let c: &'static FakeCluster = Box::leak(Box::new(cluster()));
        let (guard, log, _) =
            crate::guard::tests::guard_with(config(), mcp_write_policy(McpWriteRule::Forbidden));
        let mut tools = WriteTools::new(guard, Box::new(c));
        assert!(tools.definitions(true).is_empty());
        assert!(tools.startup_warnings()[0].contains("mcp_write: forbidden"));
        let result = tools
            .call_token("create_topic", &json!({ "topic": "t" }))
            .await;
        assert_eq!(code(&result), "policy_refused", "{result}");
        assert!(c.changes.borrow().is_empty());
        let entries = log.entries.borrow();
        let blocked = entries
            .iter()
            .find(|e| e.outcome.contains("mcp_write: forbidden"))
            .expect("the refusal is audited");
        // Recorded like every other company-policy refusal.
        assert_eq!(blocked.action, "policy.blocked");
        assert_eq!(blocked.target, "mcp create_topic t");
        assert_eq!(blocked.source, "mcp");
        assert!(!blocked.ok);
        assert!(blocked.actor.is_some());
    }

    #[tokio::test]
    async fn mcp_write_confirm_human_turns_token_mode_into_asking_a_person() {
        let c: &'static FakeCluster = Box::leak(Box::new(cluster()));
        let (guard, _, _) =
            crate::guard::tests::guard_with(config(), mcp_write_policy(McpWriteRule::ConfirmHuman));
        let mut tools = WriteTools::new(guard, Box::new(c));
        tools.confirm = ConfirmMode::Token;
        // A client that cannot ask the user gets no write tools, and a call
        // says why.
        assert!(tools.definitions(false).is_empty());
        assert!(!tools.definitions(true).is_empty());
        assert_eq!(tools.confirm_label(true), "elicit");
        let args = json!({ "topic": "t", "partitions": 1 });
        let result = tools.call_token("create_topic", &args).await;
        assert_eq!(code(&result), "elicitation_unsupported", "{result}");
        assert!(
            result.to_string().contains("mcp_write: confirm_human"),
            "{result}"
        );
        assert!(c.changes.borrow().is_empty());
    }

    #[tokio::test]
    async fn a_read_only_profile_hides_the_tools_and_refuses_calls() {
        let read_only = GuardConfig {
            profile_refusal: Some("Profile 'prod' is read-only".into()),
            ..config()
        };
        let (mut tools, cluster, _, _) = tools_with(cluster(), read_only);
        // The Kafka tools are hidden; the RabbitMQ ones use their own profile.
        assert!(tools
            .definitions(false)
            .iter()
            .all(|d| is_rabbit_tool(d["name"].as_str().unwrap_or_default())));
        assert!(tools.startup_warnings()[0].contains("read-only"));
        let result = tools
            .call_token("create_topic", &json!({ "topic": "t" }))
            .await;
        assert_eq!(code(&result), "read_only_profile");
        assert!(cluster.changes.borrow().is_empty());
    }

    fn reset_cluster() -> FakeCluster {
        let c = cluster();
        c.offsets.borrow_mut().insert(
            ("billing".into(), "orders".into()),
            vec![(100, 150), (40, 90), (-1, 30)],
        );
        c
    }

    #[tokio::test]
    async fn reset_offsets_plans_totals_and_commits_when_idle() {
        let c = reset_cluster();
        c.group_states
            .borrow_mut()
            .insert("billing".into(), ("Empty".into(), 0));
        let (mut tools, cluster, log, _) = tools_with(c, config());
        let args = json!({ "group": "billing", "topic": "orders", "to": "earliest" });
        let plan = tools
            .call_token("reset_consumer_group_offsets", &args)
            .await;
        let p = &plan["structuredContent"]["plan"];
        assert_eq!(p["records_reread"], 140, "{plan}");
        assert_eq!(p["records_skipped"], 0);
        assert_eq!(p["partitions"][2]["current"], Value::Null);
        assert_eq!(p["partitions"][0]["lag_after"], 150);

        let done = tools
            .call_token(
                "reset_consumer_group_offsets",
                &execute(&args, &token(&plan)),
            )
            .await;
        assert_eq!(done["structuredContent"]["executed"], true, "{done}");
        assert_eq!(cluster.changes.borrow().len(), 3);
        let entries = log.entries.borrow();
        assert_eq!(entries[0].target, "billing@orders");
        assert_eq!(entries[0].outcome, "3 partition(s) reset to earliest");
    }

    #[tokio::test]
    async fn reset_offsets_refuses_an_active_group_and_changed_offsets() {
        let c = reset_cluster();
        c.group_states
            .borrow_mut()
            .insert("billing".into(), ("Stable".into(), 2));
        let (mut tools, cluster, log, _) = tools_with(c, config());
        let journal = temp_journal("active");
        tools.history = Some(journal.clone());
        let args = json!({
            "group": "billing",
            "topic": "orders",
            "to": { "shift": -10, "uncommitted_from": "earliest" }
        });
        let plan = tools
            .call_token("reset_consumer_group_offsets", &args)
            .await;
        assert!(plan["structuredContent"]["plan"]["warnings"][0]
            .as_str()
            .unwrap()
            .contains("Stable"));
        let result = tools
            .call_token(
                "reset_consumer_group_offsets",
                &execute(&args, &token(&plan)),
            )
            .await;
        assert_eq!(code(&result), "group_active");
        // Refused before it ran: audited, but nothing for undo to see.
        assert!(journal.load().is_empty());
        assert!(log
            .entries
            .borrow()
            .iter()
            .any(|e| !e.ok && e.op.is_empty()));

        // A consumer joins after an idle plan: the plan no longer matches.
        cluster
            .group_states
            .borrow_mut()
            .insert("billing".into(), ("Empty".into(), 0));
        let plan = tools
            .call_token("reset_consumer_group_offsets", &args)
            .await;
        cluster
            .group_states
            .borrow_mut()
            .insert("billing".into(), ("Stable".into(), 1));
        let result = tools
            .call_token(
                "reset_consumer_group_offsets",
                &execute(&args, &token(&plan)),
            )
            .await;
        assert_eq!(code(&result), "state_changed");

        // The group committed after the plan.
        cluster
            .group_states
            .borrow_mut()
            .insert("billing".into(), ("Empty".into(), 0));
        let plan = tools
            .call_token("reset_consumer_group_offsets", &args)
            .await;
        cluster
            .offsets
            .borrow_mut()
            .get_mut(&("billing".to_string(), "orders".to_string()))
            .unwrap()[0]
            .0 = 120;
        let result = tools
            .call_token(
                "reset_consumer_group_offsets",
                &execute(&args, &token(&plan)),
            )
            .await;
        assert_eq!(code(&result), "state_changed");
        assert!(cluster.changes.borrow().is_empty());
    }

    #[tokio::test]
    async fn a_shift_over_uncommitted_partitions_is_refused_unless_given_a_base() {
        // Partition 2 of the fixture has no committed offset.
        let c = reset_cluster();
        c.group_states
            .borrow_mut()
            .insert("billing".into(), ("Empty".into(), 0));
        let (mut tools, cluster, _, _) = tools_with(c, config());
        let refused = json!({ "group": "billing", "topic": "orders", "to": { "shift": -10 } });
        let plan = tools
            .call_token("reset_consumer_group_offsets", &refused)
            .await;
        assert_eq!(code(&plan), "uncommitted_partitions", "{plan}");
        let text = plan.to_string();
        assert!(text.contains("partition(s) 2"), "{text}");
        assert!(text.contains("uncommitted_from"), "{text}");
        // Refused at plan time: no token, so it cannot be executed either.
        let result = tools
            .call_token(
                "reset_consumer_group_offsets",
                &execute(&refused, "not-a-token"),
            )
            .await;
        assert_eq!(code(&result), "uncommitted_partitions", "{result}");
        assert!(cluster.changes.borrow().is_empty());

        // Only the committed partitions: allowed, as before.
        let committed_only = json!({
            "group": "billing", "topic": "orders", "partition": 0, "to": { "shift": -10 }
        });
        let plan = tools
            .call_token("reset_consumer_group_offsets", &committed_only)
            .await;
        assert_eq!(
            plan["structuredContent"]["plan"]["partitions"][0]["new"], 90,
            "{plan}"
        );

        // With a base, every partition is planned and committed.
        let allowed = json!({
            "group": "billing",
            "topic": "orders",
            "to": { "shift": -10, "uncommitted_from": "latest" }
        });
        let plan = tools
            .call_token("reset_consumer_group_offsets", &allowed)
            .await;
        let p = &plan["structuredContent"]["plan"];
        assert_eq!(p["to"], "shift -10 (uncommitted from latest)", "{plan}");
        assert_eq!(p["partitions"][2]["current"], Value::Null);
        assert_eq!(p["partitions"][2]["new"], 20);
        let done = tools
            .call_token(
                "reset_consumer_group_offsets",
                &execute(&allowed, &token(&plan)),
            )
            .await;
        assert_eq!(done["structuredContent"]["executed"], true, "{done}");
        assert_eq!(cluster.changes.borrow().len(), 3);
    }

    #[tokio::test]
    async fn a_large_reset_needs_a_person() {
        let small_limit = GuardConfig {
            limits: Limits {
                offset_delta: 100,
                ..config().limits
            },
            ..config()
        };
        let (mut tools, cluster, _, _) = tools_with(reset_cluster(), small_limit);
        let args = json!({ "group": "billing", "topic": "orders", "to": "earliest" });
        let plan = tools
            .call_token("reset_consumer_group_offsets", &args)
            .await;
        let result = tools
            .call_token(
                "reset_consumer_group_offsets",
                &execute(&args, &token(&plan)),
            )
            .await;
        assert_eq!(code(&result), "elicitation_required");
        assert!(cluster.changes.borrow().is_empty());
    }

    #[test]
    fn reset_targets_parse() {
        assert_eq!(
            reset_strategy(Some(&json!("earliest"))),
            Ok(ResetStrategy::Earliest)
        );
        assert_eq!(
            reset_strategy(Some(&json!({ "offset": 5 }))),
            Ok(ResetStrategy::Specific(5))
        );
        assert_eq!(
            reset_strategy(Some(&json!({ "shift": -5 }))),
            Ok(ResetStrategy::shift(-5))
        );
        assert_eq!(
            reset_strategy(Some(&json!({ "shift": -5, "uncommitted_from": "latest" }))),
            Ok(ResetStrategy::Shift {
                delta: -5,
                uncommitted: Some(UncommittedBase::Latest)
            })
        );
        assert_eq!(
            reset_strategy(Some(&json!({ "timestamp": "2026-11-03T00:00:00Z" }))),
            Ok(ResetStrategy::Timestamp(1_793_664_000_000))
        );
        for bad in [
            json!("beginning"),
            json!({ "offset": -1 }),
            json!({ "offset": 1, "shift": 2 }),
            json!({ "uncommitted_from": "earliest" }),
            json!({ "shift": 1, "uncommitted_from": "zero" }),
            json!({ "offset": 1, "uncommitted_from": "earliest" }),
            json!({ "shift": 1, "uncommitted_from": "earliest", "offset": 2 }),
            json!({ "timestamp": "yesterday" }),
            json!(5),
        ] {
            assert!(reset_strategy(Some(&bad)).is_err(), "{bad}");
        }
        assert!(reset_strategy(None).is_err());
    }

    #[tokio::test]
    async fn produce_plans_without_values_then_sends_and_audits_no_payload() {
        let (mut tools, cluster, log, _) = tools_with(cluster(), config());
        let args = json!({
            "topic": "orders",
            "records": [
                { "key": "k1", "value": "card 4111-1111-1111-1111" },
                { "value": "plain", "partition": 2, "headers": { "trace": "zq-trace-9" } },
                { "key": "k3", "value": null }
            ]
        });
        let plan = tools.call_token("produce_messages", &args).await;
        assert_eq!(plan["isError"], false, "{plan}");
        let text = plan.to_string();
        assert!(!text.contains("4111-1111"), "{text}");
        let p = &plan["structuredContent"]["plan"];
        assert_eq!(p["records"][0]["partition"], "by key hash");
        assert_eq!(p["records"][1]["partition"], 2);
        assert_eq!(p["tombstones"], 1);
        assert!(p["warnings"][0].as_str().unwrap().contains("tombstone"));
        assert_eq!(p["records"][0]["value_sha256"].as_str().unwrap().len(), 12);

        let done = tools
            .call_token("produce_messages", &execute(&args, &token(&plan)))
            .await;
        let s = &done["structuredContent"];
        assert_eq!(s["result"]["delivered"], 3, "{done}");
        assert_eq!(s["result"]["records"][1]["partition"], 2);
        assert_eq!(cluster.changes.borrow().len(), 3);
        let entries = log.entries.borrow();
        let line = serde_json::to_string(&entries[0]).unwrap();
        // Needles that random hex (session ids, tokens) cannot spell by chance.
        assert!(
            !line.contains("4111-1111") && !line.contains("plain") && !line.contains("zq-trace-9")
        );
        assert!(
            entries[0].outcome.starts_with("3 record(s) -> "),
            "{}",
            entries[0].outcome
        );
    }

    #[tokio::test]
    async fn produce_reports_partial_failure_without_hiding_it() {
        let c = FakeCluster {
            fail_value: Some("too-big".into()),
            ..cluster()
        };
        let (mut tools, _, log, _) = tools_with(c, config());
        let args = json!({
            "topic": "orders",
            "records": [{ "value": "ok" }, { "value": "too-big" }]
        });
        let plan = tools.call_token("produce_messages", &args).await;
        let done = tools
            .call_token("produce_messages", &execute(&args, &token(&plan)))
            .await;
        assert_eq!(done["isError"], false);
        let result = &done["structuredContent"]["result"];
        assert_eq!(
            (result["delivered"].clone(), result["failed"].clone()),
            (json!(1), json!(1))
        );
        assert!(result["records"][1]["error"]
            .as_str()
            .unwrap()
            .contains("too large"));
        let entries = log.entries.borrow();
        assert!(!entries[0].ok);
        assert!(entries[0].outcome.contains("1 failed"));
    }

    #[tokio::test]
    async fn produce_checks_partitions_sizes_schema_and_budget() {
        let budget = GuardConfig {
            limits: Limits {
                produce_records: 3,
                ..config().limits
            },
            ..config()
        };
        let (mut tools, cluster, _, _) = tools_with(cluster(), budget);
        for (args, want) in [
            (
                json!({ "topic": "orders", "records": [{ "value": "x", "partition": 6 }] }),
                "invalid_argument",
            ),
            (
                json!({ "topic": "orders", "records": [] }),
                "invalid_argument",
            ),
            (
                json!({ "topic": "orders", "records": [{ "key": "k" }] }),
                "invalid_argument",
            ),
            (
                json!({ "topic": "orders", "records": [{ "value": "x", "extra": 1 }] }),
                "invalid_argument",
            ),
            (
                json!({ "topic": "orders", "records": [{ "value": "x".repeat(RECORD_MAX_BYTES + 1) }] }),
                "invalid_argument",
            ),
            (
                json!({ "topic": "missing", "records": [{ "value": "x" }] }),
                "invalid_argument",
            ),
            (
                json!({ "topic": "orders", "value_subject": "missing-value", "records": [{ "value": "{}" }] }),
                "invalid_argument",
            ),
        ] {
            let result = tools.call_token("produce_messages", &args).await;
            assert_eq!(code(&result), want, "{args}: {result}");
        }
        let encoded = tools
            .call_token(
                "produce_messages",
                &json!({ "topic": "orders", "value_subject": "orders-value", "records": [{ "value": "{}" }] }),
            )
            .await;
        assert_eq!(
            encoded["structuredContent"]["plan"]["records"][0]["bytes"],
            7
        );
        // Keys too: the encoded key (5-byte header + "{}") and the raw value.
        let keyed = tools
            .call_token(
                "produce_messages",
                &json!({ "topic": "orders", "key_subject": "orders-key", "records": [{ "key": "{}", "value": "x" }] }),
            )
            .await;
        let plan = &keyed["structuredContent"]["plan"];
        assert_eq!(plan["records"][0]["bytes"], 8, "{keyed}");
        assert_eq!(plan["key_subject"], "orders-key");
        let bad_key = tools
            .call_token(
                "produce_messages",
                &json!({ "topic": "orders", "key_subject": "missing-value", "records": [{ "key": "{}", "value": "x" }] }),
            )
            .await;
        assert_eq!(code(&bad_key), "invalid_argument");
        assert!(bad_key.to_string().contains("records[0].key"), "{bad_key}");

        let three = json!({ "topic": "orders", "records": [{ "value": "a" }, { "value": "b" }, { "value": "c" }] });
        let plan = tools.call_token("produce_messages", &three).await;
        tools
            .call_token("produce_messages", &execute(&three, &token(&plan)))
            .await;
        let more = tools
            .call_token(
                "produce_messages",
                &json!({ "topic": "orders", "records": [{ "value": "d" }] }),
            )
            .await;
        assert_eq!(code(&more), "produce_budget_exhausted");
        assert_eq!(cluster.changes.borrow().len(), 3);
    }

    /// What the fake client does with the elicitation request.
    enum Client {
        Answer(Value),
        Silent,
        Cancel,
        Disconnect,
    }

    /// Calls `name` from a client with elicitation; `client` answers the
    /// confirmation. Returns the result and the elicitation request sent.
    async fn call_asking(
        tools: &mut WriteTools,
        name: &str,
        args: &Value,
        client: Client,
    ) -> (Value, Option<Value>) {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let peer = Peer::new(tx);
        let other = peer.clone();
        // Ends the fake client when the call never asks (it holds a sender,
        // so the channel alone would never close).
        let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
        let asked = tokio::spawn(async move {
            let request = tokio::select! {
                request = rx.recv() => request?,
                _ = done_rx => return None,
            };
            match client {
                Client::Answer(result) => other
                    .resolve(json!({ "jsonrpc": "2.0", "id": request["id"], "result": result })),
                Client::Silent => {}
                Client::Cancel => other.cancel(&json!(9)),
                Client::Disconnect => other.close(),
            }
            Some(request)
        });
        let ctx = CallContext {
            peer: &peer,
            request_id: &json!(9),
            elicitation: true,
        };
        let result = tools.call(name, args, &ctx).await;
        let _ = done_tx.send(());
        (result, asked.await.unwrap())
    }

    fn shrink_cluster() -> FakeCluster {
        let c = cluster();
        c.set_config("orders", "retention.ms", "604800000", true);
        c
    }

    const SHRINK: &str =
        r#"{ "topic": "orders", "set": { "retention.ms": "86400000" }, "mode": "execute" }"#;

    #[tokio::test]
    async fn elicitation_asks_the_user_and_needs_the_name_for_destructive_changes() {
        let (mut tools, cluster, _, _) = tools_with(shrink_cluster(), config());
        let args: Value = serde_json::from_str(SHRINK).unwrap();
        let accept =
            json!({ "action": "accept", "content": { "confirm": true, "type_name": "orders" } });
        let (result, asked) = call_asking(
            &mut tools,
            "alter_topic_config",
            &args,
            Client::Answer(accept),
        )
        .await;
        assert_eq!(result["structuredContent"]["executed"], true, "{result}");
        let asked = asked.unwrap();
        assert_eq!(asked["method"], "elicitation/create");
        let params = &asked["params"];
        assert!(params["message"].as_str().unwrap().contains("retention.ms"));
        assert!(params["message"].as_str().unwrap().contains("profile dev"));
        assert_eq!(
            params["requestedSchema"]["required"],
            json!(["confirm", "type_name"])
        );
        assert_eq!(cluster.changes.borrow().len(), 1);
    }

    #[tokio::test]
    async fn elicitation_refusals_run_nothing_and_are_audited() {
        let wrong_name =
            json!({ "action": "accept", "content": { "confirm": true, "type_name": "order" } });
        let unchecked =
            json!({ "action": "accept", "content": { "confirm": false, "type_name": "orders" } });
        for (client, want) in [
            (Client::Answer(wrong_name), "user_declined"),
            (Client::Answer(unchecked), "user_declined"),
            (
                Client::Answer(json!({ "action": "decline" })),
                "user_declined",
            ),
            (
                Client::Answer(json!({ "action": "cancel" })),
                "user_cancelled",
            ),
            (Client::Cancel, "user_cancelled"),
            (Client::Disconnect, "user_cancelled"),
        ] {
            let (mut tools, cluster, log, _) = tools_with(shrink_cluster(), config());
            let args: Value = serde_json::from_str(SHRINK).unwrap();
            let (result, _) = call_asking(&mut tools, "alter_topic_config", &args, client).await;
            assert_eq!(code(&result), want, "{result}");
            assert!(cluster.changes.borrow().is_empty());
            let entries = log.entries.borrow();
            assert_eq!(entries.len(), 1, "{entries:?}");
            assert_eq!(entries[0].action, "mcp.declined");
            assert_eq!(entries[0].target, "alter_topic_config orders");
        }
    }

    #[tokio::test]
    async fn an_unanswered_confirmation_times_out() {
        let (mut tools, cluster, log, _) = tools_with(cluster(), config());
        tools.elicit_timeout = Duration::from_millis(20);
        let args = json!({ "topic": "t", "mode": "execute" });
        let (result, asked) = call_asking(&mut tools, "create_topic", &args, Client::Silent).await;
        assert_eq!(code(&result), "confirmation_timeout");
        // Not destructive: no name to type.
        assert_eq!(
            asked.unwrap()["params"]["requestedSchema"]["required"],
            json!(["confirm"])
        );
        assert!(cluster.changes.borrow().is_empty());
        assert!(log.entries.borrow()[0].outcome.contains("did not answer"));
    }

    #[tokio::test]
    async fn a_disconnect_is_recorded_as_such() {
        let (mut tools, _, log, _) = tools_with(cluster(), config());
        let args = json!({ "topic": "t", "mode": "execute" });
        call_asking(&mut tools, "create_topic", &args, Client::Disconnect).await;
        assert_eq!(log.entries.borrow()[0].outcome, "client disconnected");
    }

    #[tokio::test]
    async fn a_person_may_confirm_a_large_reset() {
        let small_limit = GuardConfig {
            limits: Limits {
                offset_delta: 100,
                ..config().limits
            },
            ..config()
        };
        let c = reset_cluster();
        c.group_states
            .borrow_mut()
            .insert("billing".into(), ("Empty".into(), 0));
        let (mut tools, cluster, _, _) = tools_with(c, small_limit);
        let args =
            json!({ "group": "billing", "topic": "orders", "to": "earliest", "mode": "execute" });
        let accept = json!({ "action": "accept", "content": { "confirm": true } });
        let (result, _) = call_asking(
            &mut tools,
            "reset_consumer_group_offsets",
            &args,
            Client::Answer(accept),
        )
        .await;
        assert_eq!(result["structuredContent"]["executed"], true, "{result}");
        assert_eq!(cluster.changes.borrow().len(), 3);
    }

    #[tokio::test]
    async fn elicit_mode_needs_a_client_that_can_ask() {
        let (mut tools, cluster, _, _) = tools_with(cluster(), config());
        tools.confirm = ConfirmMode::Elicit;
        assert!(tools.definitions(false).is_empty());
        assert_eq!(tools.definitions(true).len(), WRITE_TOOLS.len());
        let result = tools
            .call_token("create_topic", &json!({ "topic": "t" }))
            .await;
        assert_eq!(code(&result), "elicitation_unsupported");
        assert!(cluster.changes.borrow().is_empty());

        // --confirm token never asks, even a client that could.
        tools.confirm = ConfirmMode::Token;
        let (result, asked) = call_asking(
            &mut tools,
            "create_topic",
            &json!({ "topic": "t", "mode": "execute" }),
            Client::Silent,
        )
        .await;
        assert_eq!(code(&result), "confirmation_required");
        assert!(asked.is_none());
    }

    #[tokio::test]
    async fn plans_say_who_confirms() {
        let (mut tools, _, _, _) = tools_with(cluster(), config());
        let plan = tools
            .call_token("create_topic", &json!({ "topic": "t" }))
            .await;
        assert_eq!(plan["structuredContent"]["confirmation"]["by"], "token");
        let (plan, _) = call_asking(
            &mut tools,
            "create_topic",
            &json!({ "topic": "t" }),
            Client::Silent,
        )
        .await;
        assert_eq!(plan["structuredContent"]["confirmation"]["by"], "user");
    }

    #[test]
    fn retention_shrink_is_detected() {
        assert!(shrink_warning("retention.ms", "604800000", "86400000").is_some());
        assert!(shrink_warning("retention.ms", "-1", "86400000").is_some());
        assert!(shrink_warning("retention.ms", "86400000", "-1").is_none());
        assert!(shrink_warning("retention.ms", "1", "2").is_none());
        assert!(shrink_warning("segment.ms", "2", "1").is_none());
        // Tiered storage's local retention deletes too.
        assert!(shrink_warning("local.retention.ms", "-2", "3600000").is_some());
    }

    fn transform_cluster() -> FakeCluster {
        FakeCluster {
            brokers: 1,
            transform_records: 40,
            ..Default::default()
        }
        .with_topic("orders", 3)
        .with_topic("orders-clean", 3)
    }

    fn transform_args() -> Value {
        json!({
            "topic": "orders",
            "to": "orders-clean",
            "steps": [
                { "filter": "$.email == \"jane.doe@example.com\"" },
                { "drop": "card" }
            ],
            "max_records": 100
        })
    }

    #[tokio::test]
    async fn transform_plans_then_copies_and_audits_counts_not_steps() {
        let (mut tools, cluster, log, _) = tools_with(transform_cluster(), config());
        let args = transform_args();
        let plan = tools.call_token("transform_execute", &args).await;
        assert_eq!(plan["isError"], false, "{plan}");
        let p = &plan["structuredContent"]["plan"];
        assert_eq!(p["source"], "orders");
        assert_eq!(p["destination"], "orders-clean");
        assert_eq!(p["start"], "beginning");
        assert_eq!(p["max_records"], 100);
        assert_eq!(p["steps"][1]["drop"], "card");
        assert!(cluster.changes.borrow().is_empty());
        assert!(log.entries.borrow().is_empty());

        let done = tools
            .call_token("transform_execute", &execute(&args, &token(&plan)))
            .await;
        assert_eq!(done["structuredContent"]["executed"], true, "{done}");
        assert_eq!(done["structuredContent"]["result"]["written"], 40);
        assert_eq!(
            *cluster.changes.borrow(),
            ["transform orders -> orders-clean (40 records)"]
        );
        let entries = log.entries.borrow();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].action, "mcp transform_execute");
        assert_eq!(entries[0].target, "orders -> orders-clean");
        assert!(entries[0].ok);
        assert!(
            entries[0].outcome.contains("written 40") && entries[0].outcome.contains("sha256:"),
            "{}",
            entries[0].outcome
        );
        // The steps can quote values; the audit log has only their digest.
        let line = serde_json::to_string(&entries[0]).unwrap();
        assert!(
            !line.contains("jane.doe") && !line.contains("card"),
            "{line}"
        );
        drop(entries);
        // 100 were charged, 60 given back: 960 of 1000 are left.
        assert!(tools.guard.check_produce(960).is_ok());
        assert_eq!(
            tools.guard.check_produce(961).unwrap_err().code,
            "produce_budget_exhausted"
        );
    }

    #[tokio::test]
    async fn transform_refuses_other_clusters_and_bad_ranges() {
        let (mut tools, cluster, _, _) = tools_with(transform_cluster(), config());
        let mut other = transform_args();
        other["dst_profile"] = json!("prod");
        let refused = tools.call_token("transform_execute", &other).await;
        assert_eq!(code(&refused), "cross_cluster_unsupported");
        let text = refused["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("CLI"), "{text}");

        let mut same = transform_args();
        same["to"] = json!("orders");
        assert_eq!(
            code(&tools.call_token("transform_execute", &same).await),
            "invalid_argument"
        );
        let mut no_max = transform_args();
        no_max.as_object_mut().unwrap().remove("max_records");
        assert_eq!(
            code(&tools.call_token("transform_execute", &no_max).await),
            "invalid_argument"
        );
        let mut too_many = transform_args();
        too_many["max_records"] = json!(EXECUTE_MAX + 1);
        assert_eq!(
            code(&tools.call_token("transform_execute", &too_many).await),
            "invalid_argument"
        );
        // A relative start would differ between plan and execute.
        let mut relative = transform_args();
        relative["from_timestamp"] = json!("1h");
        assert_eq!(
            code(&tools.call_token("transform_execute", &relative).await),
            "invalid_argument"
        );
        let mut missing = transform_args();
        missing["to"] = json!("nowhere");
        let result = tools.call_token("transform_execute", &missing).await;
        assert_eq!(code(&result), "invalid_argument");
        assert!(result.to_string().contains("never creates topics"));
        let mut internal = transform_args();
        internal["topic"] = json!("__consumer_offsets");
        assert_eq!(
            code(&tools.call_token("transform_execute", &internal).await),
            "target_not_allowed"
        );
        // More than the session's produce budget.
        let (mut small, _, _, _) = tools_with(
            transform_cluster(),
            GuardConfig {
                limits: Limits {
                    produce_records: 50,
                    ..config().limits
                },
                ..config()
            },
        );
        assert_eq!(
            code(
                &small
                    .call_token("transform_execute", &transform_args())
                    .await
            ),
            "produce_budget_exhausted"
        );
        assert!(cluster.changes.borrow().is_empty());
    }

    #[tokio::test]
    async fn transform_destination_must_be_in_the_topics_scope() {
        let scoped = GuardConfig {
            topics: vec!["sandbox-*".into()],
            ..config()
        };
        let (mut tools, _, _, _) = tools_with(transform_cluster(), scoped);
        // The source may be anything; the destination must match --topics.
        let result = tools
            .call_token("transform_execute", &transform_args())
            .await;
        assert_eq!(code(&result), "target_not_allowed");
    }

    #[tokio::test]
    async fn enforced_masking_needs_a_mask_step_first() {
        use super::super::guard::tests::policy;
        use ferrika_core::model::policy::{MaskingRule, PolicyRules};
        let rules = PolicyRules {
            masking: MaskingRule::Enforced,
            ..Default::default()
        };
        let cluster: &'static FakeCluster = Box::leak(Box::new(transform_cluster()));
        let (guard, log, _) = guard_with(config(), policy(rules));
        let mut tools = WriteTools::new(guard, Box::new(cluster));
        let refused = tools
            .call_token("transform_execute", &transform_args())
            .await;
        assert_eq!(code(&refused), "policy_refused");
        assert!(refused.to_string().contains("mask"), "{refused}");
        assert_eq!(log.entries.borrow()[0].action, "policy.blocked");

        let mut masked = transform_args();
        masked["steps"] = json!([{ "mask": "partial" }, { "drop": "card" }]);
        let plan = tools.call_token("transform_execute", &masked).await;
        assert_eq!(plan["isError"], false, "{plan}");
    }

    #[test]
    fn transform_is_a_produce_for_the_policy() {
        assert_eq!(
            super::super::guard::policy_action("transform_execute"),
            ferrika_core::model::policy::PolicyAction::Produce
        );
        assert!(is_write_tool("transform_execute"));
    }
}
