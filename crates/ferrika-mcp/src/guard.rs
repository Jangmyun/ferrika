//! The gates every MCP write passes before it runs, and its audit entries.
//!
//! In order: the tool is enabled (`--allow-write`, `--allow-tools`), the
//! target is in scope (no internal topics, `--topics`), the company policy
//! and the profile allow it ([`authorize_write_at`]), the session's rate and
//! size limits, and for an execute the confirmation token from the plan.
//! The outcome is recorded with the MCP client as the entry's `actor`.

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;

use jiff::{SignedDuration, Timestamp};
use serde_json::{json, Value};

use ferrika_core::model::policy::{McpWriteRule, PolicyAction};
use ferrika_governance::audit::AuditEntry;
use ferrika_governance::policy::{
    authorize_write_at, AuditMode, AuditSink, EffectivePolicy, WriteRefusal, WriteRequest,
};

/// The write tools, in the order `tools/list` shows them.
pub const WRITE_TOOLS: [&str; 5] = [
    "produce_messages",
    "create_topic",
    "alter_topic_config",
    "reset_consumer_group_offsets",
    "transform_execute",
];

/// The policy action a write tool performs. A transform writes records to
/// its destination topic, as `ferrika transform --execute` does: `produce`.
pub(super) fn policy_action(tool: &str) -> PolicyAction {
    match tool {
        "produce_messages" | "transform_execute" => PolicyAction::Produce,
        "create_topic" => PolicyAction::TopicsCreate,
        "alter_topic_config" => PolicyAction::ConfigChange,
        _ => PolicyAction::OffsetsReset,
    }
}

/// How long a plan's confirmation token stays valid.
pub(super) const TOKEN_TTL_SECS: i64 = 120;
/// Most unredeemed tokens kept; the oldest is dropped first.
const TOKENS_MAX: usize = 32;
/// Plans allowed per minute (executes have their own `--max-writes-per-minute`).
pub(super) const PLANS_PER_MINUTE: u32 = 60;
/// The same refusal this many times in a row starts a cooldown.
const REPEATS_FOR_COOLDOWN: u32 = 5;
const COOLDOWN_SECS: i64 = 30;
/// Longest `actor` recorded.
const ACTOR_MAX: usize = 128;

/// A write tool's failure, returned as a tool result with `isError` and
/// `structuredContent.error = {code, message, retryable, ...}`.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct ToolError {
    pub code: &'static str,
    pub message: String,
    pub retryable: bool,
    /// More fields for `error`, e.g. `retry_after_seconds`.
    pub detail: serde_json::Map<String, Value>,
}

impl ToolError {
    pub(super) fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            retryable: false,
            detail: serde_json::Map::new(),
        }
    }

    pub(super) fn invalid(message: impl Into<String>) -> Self {
        Self::new("invalid_argument", message)
    }

    pub(super) fn retryable(mut self) -> Self {
        self.retryable = true;
        self
    }

    pub(super) fn with(mut self, key: &str, value: Value) -> Self {
        self.detail.insert(key.to_string(), value);
        self
    }

    /// A broker error; timeouts and connection failures may be retried.
    pub(super) fn kafka(err: impl std::fmt::Display) -> Self {
        let message = err.to_string();
        let lower = message.to_ascii_lowercase();
        let transient = [
            "timed out",
            "timeout",
            "transport",
            "connect",
            "unreachable",
        ]
        .iter()
        .any(|w| lower.contains(w));
        let error = Self::new("kafka_error", message);
        if transient {
            error.retryable()
        } else {
            error
        }
    }

    /// The `tools/call` result.
    pub(super) fn into_result(self) -> Value {
        let mut error = self.detail;
        error.insert("code".into(), json!(self.code));
        error.insert("message".into(), json!(self.message));
        error.insert("retryable".into(), json!(self.retryable));
        json!({
            "content": [{ "type": "text", "text": format!("{} ({})", self.message, self.code) }],
            "structuredContent": { "error": error },
            "isError": true
        })
    }
}

/// Session limits from the command line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Limits {
    pub writes_per_minute: u32,
    pub produce_records: u64,
    pub offset_delta: u64,
}

/// What `ferrika mcp` was started with, as the guard needs it.
#[derive(Debug, Clone)]
pub(super) struct GuardConfig {
    /// Write tools enabled by `--allow-write` / `--allow-tools`.
    pub tools: Vec<&'static str>,
    /// `--topics` globs; empty = every topic but internal ones.
    pub topics: Vec<String>,
    pub limits: Limits,
    /// Profile name (may be empty) and brokers, for the policy.
    pub profile: String,
    pub bootstrap: String,
    /// Why the profile refuses writes (it is read-only), if it does.
    pub profile_refusal: Option<String>,
}

/// The session's write gates and counters.
pub(super) struct WriteGuard {
    config: GuardConfig,
    policy: &'static EffectivePolicy,
    sink: DedupSink,
    clock: Box<dyn Fn() -> Timestamp>,
    actor: String,
    session: String,
    tokens: VecDeque<Token>,
    plans: VecDeque<Timestamp>,
    executes: VecDeque<Timestamp>,
    produced: u64,
    /// The last refusal and how many times in a row it came.
    streak: Option<(String, u32)>,
    cooldown_until: Option<Timestamp>,
    writes: u64,
    refused: u64,
}

struct Token {
    id: String,
    tool: &'static str,
    args_digest: String,
    plan_digest: String,
    created: Timestamp,
}

impl WriteGuard {
    pub(super) fn new(
        config: GuardConfig,
        policy: &'static EffectivePolicy,
        sink: Box<dyn AuditSink>,
        clock: Box<dyn Fn() -> Timestamp>,
    ) -> Self {
        let session = random_hex(4);
        Self {
            actor: format!("mcp:unknown#{session}"),
            session,
            config,
            policy,
            sink: DedupSink::new(sink),
            clock,
            tokens: VecDeque::new(),
            plans: VecDeque::new(),
            executes: VecDeque::new(),
            produced: 0,
            streak: None,
            cooldown_until: None,
            writes: 0,
            refused: 0,
        }
    }

    pub(super) fn now(&self) -> Timestamp {
        (self.clock)()
    }

    pub(super) fn config(&self) -> &GuardConfig {
        &self.config
    }

    /// The MCP client as recorded in `actor`.
    pub(super) fn actor(&self) -> &str {
        &self.actor
    }

    /// Names the client in `actor`: `mcp:<name>/<version>#<session>`.
    pub(super) fn set_client(&mut self, name: Option<&str>, version: Option<&str>) {
        let who = match (name, version) {
            (Some(n), Some(v)) => format!("{n}/{v}"),
            (Some(n), None) => n.to_string(),
            _ => "unknown".to_string(),
        };
        let mut actor: String = format!("mcp:{who}")
            .chars()
            .filter(|c| !c.is_control())
            .collect();
        let suffix = format!("#{}", self.session);
        let room = ACTOR_MAX - suffix.len();
        if actor.chars().count() > room {
            actor = actor.chars().take(room).collect();
        }
        self.actor = actor + &suffix;
    }

    pub(super) fn enabled(&self, tool: &str) -> bool {
        self.config.tools.contains(&tool)
    }

    /// Why `tool` is left out of `tools/list` although enabled: the profile
    /// is read-only or the policy refuses its action here. A call still
    /// goes through the gate.
    pub(super) fn hidden_reason(&self, tool: &str) -> Option<String> {
        if let Some(reason) = &self.config.profile_refusal {
            return Some(reason.clone());
        }
        if let Some(reason) = self.mcp_write_refusal() {
            return Some(reason);
        }
        self.policy
            .check_at(
                self.now(),
                policy_action(tool),
                &self.config.profile,
                &self.config.bootstrap,
            )
            .err()
            .map(String::from)
    }

    /// Refuses internal topics and topics outside `--topics`.
    pub(super) fn check_topic(&self, topic: &str) -> Result<(), ToolError> {
        if ferrika_core::kafka::internal_topic::is_internal_topic(topic) {
            return Err(ToolError::new(
                "target_not_allowed",
                format!("'{topic}' is an internal topic; MCP tools never change those"),
            ));
        }
        if !self.config.topics.is_empty()
            && !self
                .config
                .topics
                .iter()
                .any(|g| topic_glob_match(g, topic))
        {
            return Err(ToolError::new(
                "target_not_allowed",
                format!(
                    "'{topic}' is outside the topics this server may change (--topics {})",
                    self.config.topics.join(", ")
                ),
            ));
        }
        Ok(())
    }

    /// Refuses every write during a cooldown.
    pub(super) fn check_cooldown(&self) -> Result<(), ToolError> {
        match self.cooldown_until {
            Some(until) if self.now() < until => {
                let left = until.duration_since(self.now()).as_secs().max(1);
                Err(ToolError::new(
                    "cooldown",
                    format!(
                        "The same write was refused {REPEATS_FOR_COOLDOWN} times in a row, so \
                         writes are paused for {left} more second(s). Change the request \
                         instead of retrying it."
                    ),
                )
                .retryable()
                .with("retry_after_seconds", json!(left)))
            }
            _ => Ok(()),
        }
    }

    /// Takes one plan (`execute` false) or execute from the per-minute
    /// budget.
    pub(super) fn admit(&mut self, execute: bool) -> Result<(), ToolError> {
        self.check_rate(execute)?;
        let now = self.now();
        if execute {
            self.executes.push_back(now);
        } else {
            self.plans.push_back(now);
        }
        Ok(())
    }

    /// Whether one more plan (`execute` false) or execute fits in the
    /// per-minute budget, without taking it.
    pub(super) fn check_rate(&mut self, execute: bool) -> Result<(), ToolError> {
        let now = self.now();
        let (window, limit, what) = if execute {
            (
                &mut self.executes,
                self.config.limits.writes_per_minute,
                "writes",
            )
        } else {
            (&mut self.plans, PLANS_PER_MINUTE, "plans")
        };
        while window
            .front()
            .is_some_and(|t| now.duration_since(*t) >= SignedDuration::from_secs(60))
        {
            window.pop_front();
        }
        if window.len() >= limit as usize {
            let wait = window
                .front()
                .map(|t| 60 - now.duration_since(*t).as_secs())
                .unwrap_or(60)
                .max(1);
            return Err(ToolError::new(
                "rate_limited",
                format!("At most {limit} {what} per minute; try again in {wait} second(s)."),
            )
            .retryable()
            .with("retry_after_seconds", json!(wait)));
        }
        Ok(())
    }

    /// Counts `records` against the session's produce budget.
    pub(super) fn charge_produce(&mut self, records: u64) -> Result<(), ToolError> {
        self.check_produce(records)?;
        self.produced += records;
        Ok(())
    }

    /// Gives back records charged with [`Self::charge_produce`] that were
    /// not written (a transform that wrote fewer than its `max_records`).
    pub(super) fn refund_produce(&mut self, records: u64) {
        self.produced = self.produced.saturating_sub(records);
    }

    /// Whether `records` more fit in the session's produce budget.
    pub(super) fn check_produce(&self, records: u64) -> Result<(), ToolError> {
        let max = self.config.limits.produce_records;
        if self.produced + records > max {
            return Err(ToolError::new(
                "produce_budget_exhausted",
                format!(
                    "This session may produce {max} records (--max-produce-records) and has \
                     produced {}; {records} more would exceed it. Restart the MCP server to reset.",
                    self.produced
                ),
            ));
        }
        Ok(())
    }

    /// The policy and profile gate for `tool` on `target`. A policy refusal
    /// is recorded as `policy.blocked`, a read-only profile as the tool's
    /// failed outcome.
    #[cfg(test)]
    pub(super) fn authorize(&self, tool: &'static str, target: &str) -> Result<(), ToolError> {
        self.authorize_with(tool, target, &[])
    }

    /// Whether the policy judges writes by broker address, so the brokers
    /// from cluster metadata must be fetched for [`Self::authorize_with`].
    pub(super) fn needs_broker_metadata(&self) -> bool {
        self.policy.write_needs_broker_metadata()
    }

    /// The company policy's `mcp_write` at this moment.
    pub(super) fn mcp_write_rule(&self) -> McpWriteRule {
        self.policy.mcp_write_at(self.now())
    }

    /// The refusal when the policy forbids AI writes altogether.
    fn mcp_write_refusal(&self) -> Option<String> {
        (self.mcp_write_rule() == McpWriteRule::Forbidden).then(|| {
            "Refused by company policy: AI assistants may not change clusters \
             (mcp_write: forbidden)"
                .to_string()
        })
    }

    /// [`Self::authorize`] with the brokers learned from cluster metadata
    /// added to the bootstrap list the policy checks.
    pub(super) fn authorize_with(
        &self,
        tool: &'static str,
        target: &str,
        known_brokers: &[String],
    ) -> Result<(), ToolError> {
        if let Some(reason) = self.mcp_write_refusal() {
            let _ = self.record_result(tool, target, &reason, false, None);
            return Err(ToolError::new("policy_refused", reason));
        }
        let brokers =
            ferrika_governance::policy::with_known_brokers(&self.config.bootstrap, known_brokers);
        let label = format!("mcp {tool}");
        let request = WriteRequest {
            source: "mcp",
            action: policy_action(tool),
            label: &label,
            target,
            profile: &self.config.profile,
            bootstrap: &brokers,
            actor: Some(&self.actor),
            op: None,
        };
        let profile = self.config.profile_refusal.clone();
        let result = authorize_write_at(
            self.policy,
            &self.sink,
            &request,
            AuditMode::ProbeOnly,
            self.now(),
            || profile.map_or(Ok(()), Err),
        );
        match result {
            Ok(()) => Ok(()),
            Err(WriteRefusal::Policy(m)) => Err(ToolError::new("policy_refused", m)),
            Err(WriteRefusal::AuditUnavailable(m)) => Err(ToolError::new("audit_unavailable", m)),
            Err(WriteRefusal::Profile(m)) => {
                let _ = self.record_result(tool, target, &m, false, None);
                Err(ToolError::new("read_only_profile", m))
            }
        }
    }

    /// The copy checks of `ferrika transform --execute`
    /// ([`ferrika_ops::mirror::authorize_copy`]) for a transform from
    /// `src_topic` to `dst_topic` on this session's cluster: enforced PII
    /// masking refuses it unless the steps mask first. Source and
    /// destination are the same cluster, so there is no export; the write
    /// gate for the destination is [`Self::authorize_with`]. A refusal is
    /// recorded as `policy.blocked`.
    pub(super) fn authorize_copy(
        &self,
        tool: &str,
        src_topic: &str,
        dst_topic: &str,
        masks_first: bool,
    ) -> Result<(), ToolError> {
        use ferrika_ops::mirror::{authorize_copy, CopyCheck, CopyCluster};
        let cluster = ferrika_core::kafka::config::KafkaClientConfig::new(&self.config.bootstrap);
        let side = CopyCluster {
            profile: &self.config.profile,
            config: &cluster,
        };
        let label = format!("mcp {tool}");
        let check = CopyCheck {
            source: "mcp",
            label: &label,
            src_topic,
            dst_topic,
            src: side,
            dst: side,
            dst_named_by_brokers: false,
            check_destination: false,
            masks_first,
            masking_hint: " Put {\"mask\": \"partial\"} (or \"redact\") first in steps.",
            read_only_hint: "",
            audit_mode: AuditMode::ProbeOnly,
        };
        authorize_copy(
            self.policy,
            &self.sink,
            &check,
            ferrika_core::kafka::admin::known_brokers,
        )
        .map(|_| ())
        .map_err(|reason| ToolError::new("policy_refused", reason))
    }

    /// A one-time token for executing the plan just shown, and when it
    /// expires.
    pub(super) fn issue_token(
        &mut self,
        tool: &'static str,
        args_digest: String,
        plan_digest: String,
    ) -> (String, Timestamp) {
        let now = self.now();
        self.drop_expired(now);
        if self.tokens.len() >= TOKENS_MAX {
            self.tokens.pop_front();
        }
        let id = random_hex(16);
        self.tokens.push_back(Token {
            id: id.clone(),
            tool,
            args_digest,
            plan_digest,
            created: now,
        });
        (id, now + SignedDuration::from_secs(TOKEN_TTL_SECS))
    }

    /// Uses up `token` for `tool` with these arguments, returning the
    /// digest of the plan it was issued for.
    pub(super) fn redeem_token(
        &mut self,
        tool: &str,
        token: &str,
        args_digest: &str,
    ) -> Result<String, ToolError> {
        let now = self.now();
        let position = self.tokens.iter().position(|t| t.id == token);
        let Some(found) = position.and_then(|i| self.tokens.remove(i)) else {
            return Err(ToolError::new(
                "confirmation_invalid",
                "Unknown or already used confirmation token; run the tool with mode \"plan\" again.",
            ));
        };
        if now.duration_since(found.created) > SignedDuration::from_secs(TOKEN_TTL_SECS) {
            return Err(ToolError::new(
                "confirmation_expired",
                format!(
                    "The confirmation token expired ({TOKEN_TTL_SECS} s); run the tool with mode \"plan\" again."
                ),
            ));
        }
        if found.tool != tool {
            return Err(ToolError::new(
                "confirmation_invalid",
                format!(
                    "That confirmation token was issued for {}, not {tool}.",
                    found.tool
                ),
            ));
        }
        if found.args_digest != args_digest {
            return Err(ToolError::new(
                "args_changed",
                "The arguments differ from the plan the token was issued for; plan the new arguments first.",
            ));
        }
        Ok(found.plan_digest)
    }

    fn drop_expired(&mut self, now: Timestamp) {
        self.tokens
            .retain(|t| now.duration_since(t.created) <= SignedDuration::from_secs(TOKEN_TTL_SECS));
    }

    /// Notes a call that ended in `error`; the same refusal
    /// [`REPEATS_FOR_COOLDOWN`] times in a row starts a cooldown.
    pub(super) fn note_refusal(&mut self, tool: &str, error: &ToolError) {
        self.refused += 1;
        // Waiting out a limit is not a repeated mistake: no cooldown streak.
        if matches!(error.code, "cooldown" | "rate_limited") {
            return;
        }
        let key = format!("{tool}\u{0}{}\u{0}{}", error.code, error.message);
        let count = match &self.streak {
            Some((last, n)) if *last == key => n + 1,
            _ => 1,
        };
        if count >= REPEATS_FOR_COOLDOWN {
            self.cooldown_until = Some(self.now() + SignedDuration::from_secs(COOLDOWN_SECS));
            self.streak = None;
        } else {
            self.streak = Some((key, count));
        }
    }

    /// Notes a call that succeeded (a plan or a write).
    pub(super) fn note_success(&mut self, executed: bool) {
        self.streak = None;
        if executed {
            self.writes += 1;
        }
    }

    fn entry(&self, action: String, target: &str, outcome: &str, ok: bool) -> AuditEntry {
        AuditEntry {
            time_ms: self.now().as_millisecond(),
            source: "mcp".to_string(),
            profile: if self.config.profile.is_empty() {
                self.config.bootstrap.clone()
            } else {
                self.config.profile.clone()
            },
            action,
            target: target.to_string(),
            outcome: outcome.to_string(),
            ok,
            user: String::new(),
            actor: Some(self.actor.clone()),
            op: String::new(),
            op_digest: String::new(),
        }
    }

    /// The profile and bootstrap servers the session writes to.
    pub(super) fn profile_and_bootstrap(&self) -> (String, String) {
        (self.config.profile.clone(), self.config.bootstrap.clone())
    }

    /// Records what an executed tool did, with the operation id and digest
    /// of its history record when it has one. Returns a warning when the
    /// audit log could not be written (the change has already happened).
    pub(super) fn record_result(
        &self,
        tool: &str,
        target: &str,
        outcome: &str,
        ok: bool,
        op: Option<(String, String)>,
    ) -> Option<String> {
        let mut entry = self.entry(format!("mcp {tool}"), target, outcome, ok);
        if let Some((id, digest)) = op {
            entry.op = id;
            entry.op_digest = digest;
        }
        self.sink.record(&entry).err().map(|e| {
            let warning = format!("The change ran, but the audit log could not be written: {e}");
            eprintln!("ferrika mcp: {warning}");
            warning
        })
    }

    /// Records a confirmation the user declined or that never came.
    pub(super) fn record_declined(&self, tool: &str, target: &str, reason: &str) {
        let entry = self.entry(
            "mcp.declined".to_string(),
            &format!("{tool} {target}"),
            reason,
            false,
        );
        if let Err(e) = self.sink.record(&entry) {
            tracing::warn!(%e, "Could not write the audit log");
        }
    }

    /// `mcp.session_start`, when writes are enabled.
    pub(super) fn record_session_start(&self, confirm: &str) {
        let limits = self.config.limits;
        let topics = if self.config.topics.is_empty() {
            "all but internal".to_string()
        } else {
            self.config.topics.join(", ")
        };
        let outcome = format!(
            "confirm {confirm}; {} writes/min, {} records/session, offset delta {}; topics: {topics}",
            limits.writes_per_minute, limits.produce_records, limits.offset_delta
        );
        let entry = self.entry(
            "mcp.session_start".to_string(),
            &self.config.tools.join(","),
            &outcome,
            true,
        );
        if let Err(e) = self.sink.record(&entry) {
            tracing::warn!(%e, "Could not write the audit log");
        }
    }

    /// `mcp.session_end`: what the session did and refused.
    pub(super) fn record_session_end(&self) {
        let outcome = format!(
            "{} write(s), {} refused, {} repeated refusal(s) not logged",
            self.writes,
            self.refused,
            self.sink.suppressed.get()
        );
        let entry = self.entry("mcp.session_end".to_string(), "", &outcome, true);
        if let Err(e) = self.sink.record(&entry) {
            tracing::warn!(%e, "Could not write the audit log");
        }
    }
}

/// An [`AuditSink`] that drops a failure entry identical to the one just
/// recorded, so an agent retrying a refused write does not flood the log;
/// the dropped ones are counted for `mcp.session_end`.
struct DedupSink {
    inner: Box<dyn AuditSink>,
    last_failure: RefCell<Option<String>>,
    suppressed: Cell<u64>,
}

impl DedupSink {
    fn new(inner: Box<dyn AuditSink>) -> Self {
        Self {
            inner,
            last_failure: RefCell::new(None),
            suppressed: Cell::new(0),
        }
    }
}

impl AuditSink for DedupSink {
    fn record(&self, entry: &AuditEntry) -> std::io::Result<()> {
        let key = (!entry.ok).then(|| {
            format!(
                "{}\u{0}{}\u{0}{}",
                entry.action, entry.target, entry.outcome
            )
        });
        if key.is_some() && *self.last_failure.borrow() == key {
            self.suppressed.set(self.suppressed.get() + 1);
            return Ok(());
        }
        self.inner.record(entry)?;
        *self.last_failure.borrow_mut() = key;
        Ok(())
    }

    fn probe(&self) -> std::io::Result<()> {
        self.inner.probe()
    }
}

/// `bytes` random bytes as lowercase hex.
fn random_hex(bytes: usize) -> String {
    use ring::rand::SecureRandom;
    let mut buf = vec![0u8; bytes];
    if ring::rand::SystemRandom::new().fill(&mut buf).is_err() {
        // No OS randomness: fall back to the clock and a counter, which is
        // still unique within this process.
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let seed = ferrika_core::kafka::clock::now_ms() as u64
            ^ COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        for (i, b) in buf.iter_mut().enumerate() {
            *b = (seed >> ((i % 8) * 8)) as u8 ^ i as u8;
        }
    }
    ferrika_governance::policy::to_hex(&buf)
}

/// JSON text with object keys sorted at every level, so equal values give
/// equal digests whatever order their keys were built in.
pub(super) fn canonical(value: &Value) -> String {
    match value {
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            let fields: Vec<String> = keys
                .into_iter()
                .map(|k| format!("{}:{}", json!(k), canonical(&map[k])))
                .collect();
            format!("{{{}}}", fields.join(","))
        }
        Value::Array(items) => {
            let items: Vec<String> = items.iter().map(canonical).collect();
            format!("[{}]", items.join(","))
        }
        other => other.to_string(),
    }
}

/// SHA-256 of [`canonical`] text, hex.
pub(super) fn digest(value: &Value) -> String {
    let d = ring::digest::digest(&ring::digest::SHA256, canonical(value).as_bytes());
    ferrika_governance::policy::to_hex(d.as_ref())
}

/// The digest of a tool's arguments without `mode` and `confirmation`,
/// which differ between the plan and the execute call.
pub(super) fn args_digest(args: &Value) -> String {
    let mut args = args.clone();
    if let Some(map) = args.as_object_mut() {
        map.remove("mode");
        map.remove("confirmation");
    }
    digest(&args)
}

/// `--topics` patterns: `*` any run, `?` one character, and case-sensitive
/// like Kafka topic names (`dev-*` must not reach `DEV-payments`). The
/// policy's `glob_match` ignores case, which suits profile names and hosts.
fn topic_glob_match(pattern: &str, text: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let t: Vec<char> = text.chars().collect();
    let (mut pi, mut ti) = (0, 0);
    let (mut star, mut mark) = (None, 0);
    while ti < t.len() {
        if pi < p.len() && (p[pi] == '?' || p[pi] == t[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < p.len() && p[pi] == '*' {
            star = Some(pi);
            mark = ti;
            pi += 1;
        } else if let Some(s) = star {
            pi = s + 1;
            mark += 1;
            ti = mark;
        } else {
            return false;
        }
    }
    p[pi..].iter().all(|&c| c == '*')
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;

    #[test]
    fn topic_patterns_are_case_sensitive() {
        assert!(topic_glob_match("dev-*", "dev-orders"));
        assert!(!topic_glob_match("dev-*", "DEV-payments"));
        assert!(!topic_glob_match("dev-*", "Dev-Orders"));
        assert!(topic_glob_match("orders.v?", "orders.v1"));
        assert!(!topic_glob_match("orders.v?", "orders.v10"));
        assert!(topic_glob_match("*", "anything"));
        assert!(topic_glob_match("a*b*", "aXbY"));
        assert!(!topic_glob_match("", "x"));
    }
    use ferrika_core::model::policy::{ExpiryRule, PolicyRules};
    use ferrika_governance::policy::{PolicyExpiry, PolicyStatus};
    use std::rc::Rc;

    /// An audit log in memory; shared so tests can read what was written.
    #[derive(Clone, Default)]
    pub struct MemoryLog {
        pub entries: Rc<RefCell<Vec<AuditEntry>>>,
        pub broken: Rc<Cell<bool>>,
    }

    impl AuditSink for MemoryLog {
        fn record(&self, entry: &AuditEntry) -> std::io::Result<()> {
            if self.broken.get() {
                return Err(std::io::Error::other("disk full"));
            }
            self.entries.borrow_mut().push(entry.clone());
            Ok(())
        }

        fn probe(&self) -> std::io::Result<()> {
            if self.broken.get() {
                Err(std::io::Error::other("disk full"))
            } else {
                Ok(())
            }
        }
    }

    /// A clock tests move by hand.
    #[derive(Clone)]
    pub struct TestClock(pub Rc<Cell<Timestamp>>);

    impl TestClock {
        pub fn new() -> Self {
            Self(Rc::new(Cell::new("2026-11-03T09:00:00Z".parse().unwrap())))
        }

        pub fn advance(&self, secs: i64) {
            self.0.set(self.0.get() + SignedDuration::from_secs(secs));
        }

        pub fn boxed(&self) -> Box<dyn Fn() -> Timestamp> {
            let cell = self.0.clone();
            Box::new(move || cell.get())
        }
    }

    pub fn config() -> GuardConfig {
        GuardConfig {
            tools: WRITE_TOOLS.to_vec(),
            topics: Vec::new(),
            limits: Limits {
                writes_per_minute: 10,
                produce_records: 1000,
                offset_delta: 1_000_000,
            },
            profile: "dev".into(),
            bootstrap: "localhost:9092".into(),
            profile_refusal: None,
        }
    }

    pub fn no_policy() -> &'static EffectivePolicy {
        Box::leak(Box::new(ferrika_governance::test_policy::none()))
    }

    pub fn policy(rules: PolicyRules) -> &'static EffectivePolicy {
        let mut policy = ferrika_governance::test_policy::none();
        policy.status = PolicyStatus::Verified {
            issuer: "보안팀".into(),
            key_id: "test".into(),
        };
        policy.rules = rules;
        Box::leak(Box::new(policy))
    }

    pub fn guard_with(
        config: GuardConfig,
        policy: &'static EffectivePolicy,
    ) -> (WriteGuard, MemoryLog, TestClock) {
        let log = MemoryLog::default();
        let clock = TestClock::new();
        let guard = WriteGuard::new(config, policy, Box::new(log.clone()), clock.boxed());
        (guard, log, clock)
    }

    #[test]
    fn topics_scope_refuses_internal_and_unlisted_topics() {
        let (guard, _, _) = guard_with(config(), no_policy());
        assert_eq!(
            guard.check_topic("__consumer_offsets").unwrap_err().code,
            "target_not_allowed"
        );
        assert_eq!(
            guard.check_topic("_schemas").unwrap_err().code,
            "target_not_allowed"
        );
        assert!(guard.check_topic("orders").is_ok());
        let scoped = GuardConfig {
            topics: vec!["test-*".into()],
            ..config()
        };
        let (guard, _, _) = guard_with(scoped, no_policy());
        assert!(guard.check_topic("test-1").is_ok());
        assert_eq!(
            guard.check_topic("orders").unwrap_err().code,
            "target_not_allowed"
        );
    }

    #[test]
    fn policy_refusal_is_recorded_with_the_actor() {
        let rules = PolicyRules {
            forbid: vec!["produce".into()],
            ..Default::default()
        };
        let (mut guard, log, _) = guard_with(config(), policy(rules));
        guard.set_client(Some("claude-code"), Some("2.1.4"));
        assert!(guard.actor().starts_with("mcp:claude-code/2.1.4#"));
        let err = guard.authorize("produce_messages", "orders").unwrap_err();
        assert_eq!(err.code, "policy_refused");
        let entries = log.entries.borrow();
        assert_eq!(entries[0].action, "policy.blocked");
        assert_eq!(entries[0].source, "mcp");
        assert_eq!(entries[0].actor.as_deref(), Some(guard.actor()));
        // Other actions are not forbidden.
        assert!(guard.authorize("create_topic", "orders").is_ok());
        assert!(guard.hidden_reason("produce_messages").is_some());
        assert!(guard.hidden_reason("create_topic").is_none());
    }

    #[test]
    fn read_only_rules_profiles_and_invalid_policies_refuse() {
        let rules = PolicyRules {
            read_only: vec!["prod-*".into()],
            ..Default::default()
        };
        let prod = GuardConfig {
            profile: "prod-payments".into(),
            ..config()
        };
        let (guard, _, _) = guard_with(prod, policy(rules));
        assert_eq!(
            guard.authorize("create_topic", "t").unwrap_err().code,
            "policy_refused"
        );
        assert!(guard.hidden_reason("create_topic").is_some());

        let read_only = GuardConfig {
            profile_refusal: Some("Profile 'dev' is read-only".into()),
            ..config()
        };
        let (guard, log, _) = guard_with(read_only, no_policy());
        assert_eq!(
            guard.authorize("create_topic", "t").unwrap_err().code,
            "read_only_profile"
        );
        assert!(guard.hidden_reason("create_topic").is_some());
        assert!(!log.entries.borrow()[0].ok);

        let invalid: &'static EffectivePolicy = Box::leak(Box::new(EffectivePolicy::invalid(
            std::path::PathBuf::new(),
            "bad signature".into(),
        )));
        let (guard, _, _) = guard_with(config(), invalid);
        for tool in WRITE_TOOLS {
            assert_eq!(
                guard.authorize(tool, "t").unwrap_err().code,
                "policy_refused"
            );
        }
    }

    #[test]
    fn audit_required_with_a_broken_log_refuses_before_anything_runs() {
        let rules = PolicyRules {
            audit: ferrika_core::model::policy::AuditRule::Required,
            ..Default::default()
        };
        let (guard, log, _) = guard_with(config(), policy(rules));
        log.broken.set(true);
        assert_eq!(
            guard.authorize("create_topic", "t").unwrap_err().code,
            "audit_unavailable"
        );
    }

    #[test]
    fn a_policy_expiring_between_plan_and_execute_refuses_the_execute() {
        let mut p = ferrika_governance::test_policy::none();
        p.status = PolicyStatus::Verified {
            issuer: "보안팀".into(),
            key_id: "test".into(),
        };
        p.expiry = Some(PolicyExpiry {
            at: "2026-11-03T09:01:00Z".parse().unwrap(),
            text: "2026-11-03T09:01:00Z".into(),
            on_expiry: ExpiryRule::Strictest,
        });
        let (guard, _, clock) = guard_with(config(), Box::leak(Box::new(p)));
        assert!(guard.authorize("create_topic", "t").is_ok());
        clock.advance(90);
        assert_eq!(
            guard.authorize("create_topic", "t").unwrap_err().code,
            "policy_refused"
        );
    }

    #[test]
    fn tokens_are_single_use_bound_to_tool_and_arguments_and_expire() {
        let (mut guard, _, clock) = guard_with(config(), no_policy());
        let (token, expires) = guard.issue_token("create_topic", "a".into(), "p".into());
        assert_eq!(token.len(), 32);
        assert_eq!(
            expires,
            guard.now() + SignedDuration::from_secs(TOKEN_TTL_SECS)
        );
        assert_eq!(
            guard.redeem_token("create_topic", &token, "a"),
            Ok("p".into())
        );
        assert_eq!(
            guard
                .redeem_token("create_topic", &token, "a")
                .unwrap_err()
                .code,
            "confirmation_invalid"
        );

        let (token, _) = guard.issue_token("create_topic", "a".into(), "p".into());
        assert_eq!(
            guard
                .redeem_token("create_topic", &token, "b")
                .unwrap_err()
                .code,
            "args_changed"
        );
        let (token, _) = guard.issue_token("create_topic", "a".into(), "p".into());
        assert_eq!(
            guard
                .redeem_token("produce_messages", &token, "a")
                .unwrap_err()
                .code,
            "confirmation_invalid"
        );
        let (token, _) = guard.issue_token("create_topic", "a".into(), "p".into());
        clock.advance(TOKEN_TTL_SECS + 1);
        assert_eq!(
            guard
                .redeem_token("create_topic", &token, "a")
                .unwrap_err()
                .code,
            "confirmation_expired"
        );

        // The 33rd token pushes out the oldest.
        let first = guard.issue_token("create_topic", "a".into(), "p".into()).0;
        let mut last = String::new();
        for _ in 0..TOKENS_MAX {
            last = guard.issue_token("create_topic", "a".into(), "p".into()).0;
        }
        assert_eq!(
            guard
                .redeem_token("create_topic", &first, "a")
                .unwrap_err()
                .code,
            "confirmation_invalid"
        );
        assert!(guard.redeem_token("create_topic", &last, "a").is_ok());
    }

    #[test]
    fn rate_limits_and_produce_budget() {
        let (mut guard, _, clock) = guard_with(config(), no_policy());
        for _ in 0..10 {
            guard.admit(true).unwrap();
        }
        let err = guard.admit(true).unwrap_err();
        assert_eq!(err.code, "rate_limited");
        assert_eq!(err.detail["retry_after_seconds"], json!(60));
        assert_eq!(guard.check_rate(true).unwrap_err().code, "rate_limited");
        // A rate-limited call is a refusal in session_end, without a
        // cooldown streak.
        guard.note_refusal("produce_messages", &err);
        assert_eq!(guard.refused, 1);
        assert!(guard.streak.is_none());
        // Plans have their own budget.
        assert!(guard.admit(false).is_ok());
        clock.advance(61);
        assert!(guard.admit(true).is_ok());

        guard.charge_produce(999).unwrap();
        guard.charge_produce(1).unwrap();
        assert_eq!(
            guard.charge_produce(1).unwrap_err().code,
            "produce_budget_exhausted"
        );
    }

    #[test]
    fn repeated_refusals_cool_down_and_are_logged_once() {
        let rules = PolicyRules {
            forbid: vec!["produce".into()],
            ..Default::default()
        };
        let (mut guard, log, clock) = guard_with(config(), policy(rules));
        for _ in 0..REPEATS_FOR_COOLDOWN {
            assert!(guard.check_cooldown().is_ok());
            let err = guard.authorize("produce_messages", "orders").unwrap_err();
            guard.note_refusal("produce_messages", &err);
        }
        let err = guard.check_cooldown().unwrap_err();
        assert_eq!(err.code, "cooldown");
        assert_eq!(log.entries.borrow().len(), 1);
        clock.advance(COOLDOWN_SECS);
        assert!(guard.check_cooldown().is_ok());

        guard.record_session_end();
        let entries = log.entries.borrow();
        let end = entries.last().unwrap();
        assert_eq!(end.action, "mcp.session_end");
        assert!(end.outcome.contains("5 refused"), "{}", end.outcome);
        assert!(end.outcome.contains("4 repeated"), "{}", end.outcome);
    }

    #[test]
    fn actor_is_cleaned_and_bounded() {
        let (mut guard, _, _) = guard_with(config(), no_policy());
        guard.set_client(Some("evil\nname"), None);
        assert!(guard.actor().starts_with("mcp:evilname#"));
        guard.set_client(Some(&"x".repeat(500)), Some("1"));
        assert_eq!(guard.actor().chars().count(), ACTOR_MAX);
        guard.set_client(None, None);
        assert!(guard.actor().starts_with("mcp:unknown#"));
    }

    #[test]
    fn digests_ignore_key_order_mode_and_confirmation() {
        let a = json!({ "topic": "t", "set": { "a": "1", "b": "2" }, "mode": "plan" });
        let b = json!({ "set": { "b": "2", "a": "1" }, "topic": "t", "mode": "execute", "confirmation": "x" });
        assert_eq!(args_digest(&a), args_digest(&b));
        assert_ne!(args_digest(&a), args_digest(&json!({ "topic": "u" })));
    }
}
