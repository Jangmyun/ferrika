//! RabbitMQ write tools (preview), offered with `ferrika mcp --allow-write`:
//! `rabbit_peek`, `rabbit_dlq_inspect`, `rabbit_publish` and, with the
//! `amqp` feature, `rabbit_dlq_redrive`.
//!
//! Reading messages through the management API takes them and requeues
//! them, which marks them redelivered (and on a quorum queue counts towards
//! its delivery limit), so even a peek is a write here: like every write
//! tool it is planned, confirmed and run through `write_tools.rs`, and
//! gated as `ferrika rabbit` is (`messages.peek`, `produce`,
//! `messages.move` + `produce`) on the RabbitMQ profile and the broker's
//! management and AMQP `host:port`. Message bodies go back to the client
//! PII-masked unless `mask_pii` is false (never when the policy enforces
//! masking) and pass the export gate; they never reach the audit log.

use std::time::Duration;

use serde_json::{json, Value};

use ferrika_core::codec::pii::ellipsize;
use ferrika_core::error::{KafkaError, KafkaResult};
use ferrika_rabbitmq::connection::ResolvedRabbit;
use ferrika_rabbitmq::model::{
    exchange_name, Binding, Exchange, OutgoingMessage, PeekedMessage, PublishOutcome, Queue,
    DEFAULT_EXCHANGE,
};
use ferrika_rabbitmq::profile::RabbitProfileStore;
use ferrika_rabbitmq::view::{Inspection, ShownMessage};
#[cfg(feature = "amqp")]
use ferrika_rabbitmq::{
    amqp::{RedriveError, RedriveReport, RedriveRequest},
    view::RedrivePreview,
};

use super::backend::BoxFuture;
use super::guard::{ToolError, WriteGuard};
use super::write_tools::Fields;

/// Most messages one `rabbit_peek` reads.
pub(super) const PEEK_MAX: u32 = 20;
/// Most messages one `rabbit_dlq_inspect` reads.
pub(super) const INSPECT_MAX: u32 = 50;
/// Most messages one `rabbit_dlq_redrive` moves.
pub(super) const REDRIVE_MAX: u64 = 1000;
/// Most messages a redrive plan reads for its preview.
#[cfg(feature = "amqp")]
const REDRIVE_PREVIEW_MAX: u32 = 100;
/// Inspected messages returned in full (the groups count them all).
const INSPECT_SAMPLE: usize = 10;
/// Longest payload text returned per message.
const PAYLOAD_TEXT_MAX: usize = 2000;
/// Largest body `rabbit_publish` sends.
const PUBLISH_MAX_BYTES: usize = 1024 * 1024;
/// How long one management API request may take.
const MANAGEMENT_TIMEOUT: Duration = Duration::from_secs(10);
/// How long the AMQP connection and each broker answer of a redrive may take.
#[cfg(feature = "amqp")]
const AMQP_TIMEOUT: Duration = Duration::from_secs(30);

/// A RabbitMQ connection for one call: the profile the arguments name (or
/// the active one), the addresses the policy gates match, and the login.
pub(super) struct RabbitConn {
    pub resolved: ResolvedRabbit,
    /// The management and AMQP `host:port`, comma-separated.
    pub addresses: String,
    credentials: Option<(String, String)>,
}

impl RabbitConn {
    pub(super) fn new(
        resolved: ResolvedRabbit,
        addresses: String,
        credentials: Option<(String, String)>,
    ) -> Self {
        Self {
            resolved,
            addresses,
            credentials,
        }
    }

    /// The RabbitMQ profile name the policy matches (may be empty).
    pub(super) fn profile(&self) -> &str {
        self.resolved.profile.as_deref().unwrap_or_default()
    }

    /// The profile, or the addresses without one, as audit entries name it.
    pub(super) fn audit_profile(&self) -> &str {
        match self.resolved.profile.as_deref() {
            Some(name) if !name.is_empty() => name,
            _ => &self.addresses,
        }
    }

    pub(super) fn vhost(&self) -> &str {
        &self.resolved.vhost
    }

    /// The refusal of a RabbitMQ profile marked read-only.
    pub(super) fn read_only_refusal(&self, tool: &str) -> Option<String> {
        self.resolved.read_only.then(|| {
            format!(
                "RabbitMQ profile '{}' is read-only, so mcp {tool} is refused. Re-add it \
                 without --read-only to allow changes.",
                self.profile()
            )
        })
    }
}

/// The RabbitMQ operations behind the write tools, behind a trait so the
/// whole plan → confirm → execute flow is tested without a broker.
pub(super) trait RabbitBackend {
    /// The connection `args` name (`profile`, `vhost`).
    fn connect(&self, args: &Value) -> KafkaResult<RabbitConn>;
    fn queues<'a>(&'a self, conn: &'a RabbitConn) -> BoxFuture<'a, Vec<Queue>>;
    fn exchanges<'a>(&'a self, conn: &'a RabbitConn) -> BoxFuture<'a, Vec<Exchange>>;
    fn bindings<'a>(&'a self, conn: &'a RabbitConn) -> BoxFuture<'a, Vec<Binding>>;
    /// Takes up to `count` messages from the head of `queue` and requeues
    /// them.
    fn peek<'a>(
        &'a self,
        conn: &'a RabbitConn,
        queue: &'a str,
        count: u32,
    ) -> BoxFuture<'a, Vec<PeekedMessage>>;
    fn publish<'a>(
        &'a self,
        conn: &'a RabbitConn,
        exchange: &'a str,
        message: &'a OutgoingMessage,
    ) -> BoxFuture<'a, PublishOutcome>;
    /// Moves messages over AMQP with publisher confirms
    /// (`ferrika_rabbitmq::amqp::redrive`).
    #[cfg(feature = "amqp")]
    fn redrive<'a>(
        &'a self,
        conn: &'a RabbitConn,
        request: &'a RedriveRequest,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<RedriveReport, RedriveError>> + 'a>,
    >;
}

/// The broker of a RabbitMQ profile (`ferrika rabbit profile add`).
pub(super) struct LiveRabbit;

fn rabbit_error(e: ferrika_rabbitmq::error::RabbitError) -> KafkaError {
    KafkaError::config(e.to_string())
}

impl LiveRabbit {
    fn client(&self, conn: &RabbitConn) -> KafkaResult<ferrika_rabbitmq::client::ManagementClient> {
        conn.resolved
            .client(conn.credentials.as_ref(), MANAGEMENT_TIMEOUT)
            .map_err(KafkaError::config)
    }
}

impl RabbitBackend for LiveRabbit {
    fn connect(&self, args: &Value) -> KafkaResult<RabbitConn> {
        let resolved = super::rabbit_tools::connection(args, || {
            RabbitProfileStore::load().map_err(|e| e.to_string())
        })?;
        let credentials = super::rabbit_tools::credentials(&resolved)?;
        let addresses = resolved.gate_addresses().map_err(KafkaError::config)?;
        Ok(RabbitConn::new(resolved, addresses, credentials))
    }

    fn queues<'a>(&'a self, conn: &'a RabbitConn) -> BoxFuture<'a, Vec<Queue>> {
        Box::pin(async move {
            self.client(conn)?
                .queues(conn.vhost())
                .await
                .map_err(rabbit_error)
        })
    }

    fn exchanges<'a>(&'a self, conn: &'a RabbitConn) -> BoxFuture<'a, Vec<Exchange>> {
        Box::pin(async move {
            self.client(conn)?
                .exchanges(conn.vhost())
                .await
                .map_err(rabbit_error)
        })
    }

    fn bindings<'a>(&'a self, conn: &'a RabbitConn) -> BoxFuture<'a, Vec<Binding>> {
        Box::pin(async move {
            self.client(conn)?
                .bindings(conn.vhost())
                .await
                .map_err(rabbit_error)
        })
    }

    fn peek<'a>(
        &'a self,
        conn: &'a RabbitConn,
        queue: &'a str,
        count: u32,
    ) -> BoxFuture<'a, Vec<PeekedMessage>> {
        Box::pin(async move {
            self.client(conn)?
                .peek(conn.vhost(), queue, count)
                .await
                .map_err(rabbit_error)
        })
    }

    fn publish<'a>(
        &'a self,
        conn: &'a RabbitConn,
        exchange: &'a str,
        message: &'a OutgoingMessage,
    ) -> BoxFuture<'a, PublishOutcome> {
        Box::pin(async move {
            self.client(conn)?
                .publish(conn.vhost(), exchange, message)
                .await
                .map_err(rabbit_error)
        })
    }

    #[cfg(feature = "amqp")]
    fn redrive<'a>(
        &'a self,
        conn: &'a RabbitConn,
        request: &'a RedriveRequest,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<RedriveReport, RedriveError>> + 'a>,
    > {
        Box::pin(async move {
            let target = conn
                .resolved
                .amqp_target(conn.credentials.as_ref(), AMQP_TIMEOUT)
                .map_err(|detail| RedriveError {
                    report: RedriveReport::default(),
                    failure: ferrika_rabbitmq::amqp::RedriveFailure::Connect,
                    detail,
                })?;
            ferrika_rabbitmq::amqp::redrive(&target, request).await
        })
    }
}

/// A parsed RabbitMQ write request.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum RabbitRequest {
    Peek {
        queue: String,
        count: u32,
        /// `mask_pii` as asked (the policy may still enforce masking).
        mask: bool,
    },
    Inspect {
        queue: String,
        count: u32,
        mask: bool,
    },
    Publish {
        /// Empty for the default exchange.
        exchange: String,
        message: OutgoingMessage,
    },
    Redrive {
        queue: String,
        max: u64,
        to_exchange: Option<String>,
        routing_key: Option<String>,
    },
}

/// The default exchange as the empty name, whichever way it was given.
fn normalize_exchange(name: &str) -> String {
    if name == DEFAULT_EXCHANGE {
        String::new()
    } else {
        name.to_string()
    }
}

/// A string argument that may be empty (an exchange or routing key).
fn text_or_empty(fields: &Fields<'_>, name: &str) -> Result<Option<String>, ToolError> {
    match fields.0.get(name) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) => Ok(Some(s.clone())),
        Some(_) => Err(ToolError::invalid(format!("'{name}' must be a string"))),
    }
}

fn flag(fields: &Fields<'_>, name: &str, default: bool) -> Result<bool, ToolError> {
    match fields.0.get(name) {
        None | Some(Value::Null) => Ok(default),
        Some(Value::Bool(b)) => Ok(*b),
        Some(_) => Err(ToolError::invalid(format!(
            "'{name}' must be true or false"
        ))),
    }
}

/// Arguments every RabbitMQ write tool takes besides its own.
const COMMON_ARGS: [&str; 4] = ["profile", "vhost", "mode", "confirmation"];

/// Parses and checks `args` for RabbitMQ tool `tool`; unknown fields are
/// refused.
pub(super) fn parse(tool: &str, args: &Value) -> Result<RabbitRequest, ToolError> {
    let fields = Fields::new(args)?;
    let only = |own: &[&str]| {
        let known: Vec<&str> = own.iter().chain(COMMON_ARGS.iter()).copied().collect();
        fields.only(&known)
    };
    for name in ["profile", "vhost"] {
        if fields
            .0
            .get(name)
            .is_some_and(|v| !v.is_string() && !v.is_null())
        {
            return Err(ToolError::invalid(format!("'{name}' must be a string")));
        }
    }
    match tool {
        "rabbit_peek" | "rabbit_dlq_inspect" => {
            only(&["queue", "count", "mask_pii"])?;
            let (max, default) = if tool == "rabbit_peek" {
                (PEEK_MAX, 5)
            } else {
                (INSPECT_MAX, 20)
            };
            let queue = fields.string("queue")?;
            let count = fields.int("count", 1, i64::from(max))?.unwrap_or(default) as u32;
            let mask = flag(&fields, "mask_pii", true)?;
            Ok(if tool == "rabbit_peek" {
                RabbitRequest::Peek { queue, count, mask }
            } else {
                RabbitRequest::Inspect { queue, count, mask }
            })
        }
        "rabbit_publish" => {
            only(&[
                "exchange",
                "routing_key",
                "body",
                "headers",
                "content_type",
                "persistent",
            ])?;
            let Some(exchange) = text_or_empty(&fields, "exchange")? else {
                return Err(ToolError::invalid(
                    "'exchange' is required (\"\" or \"amq.default\" for the default exchange)",
                ));
            };
            let Some(payload) = text_or_empty(&fields, "body")? else {
                return Err(ToolError::invalid("'body' is required (UTF-8 text)"));
            };
            if payload.len() > PUBLISH_MAX_BYTES {
                return Err(ToolError::invalid(format!(
                    "'body' is {} bytes; the most is {PUBLISH_MAX_BYTES}",
                    payload.len()
                )));
            }
            let content_type = match fields.0.get("content_type") {
                None | Some(Value::Null) => None,
                Some(_) => Some(fields.string("content_type")?),
            };
            Ok(RabbitRequest::Publish {
                exchange: normalize_exchange(&exchange),
                message: OutgoingMessage {
                    routing_key: text_or_empty(&fields, "routing_key")?.unwrap_or_default(),
                    payload,
                    content_type,
                    headers: fields.string_map("headers")?.into_iter().collect(),
                    persistent: flag(&fields, "persistent", true)?,
                },
            })
        }
        "rabbit_dlq_redrive" => {
            only(&["queue", "max", "to_exchange", "routing_key"])?;
            let Some(max) = fields.int("max", 1, REDRIVE_MAX as i64)? else {
                return Err(ToolError::invalid(format!(
                    "'max' is required (1 to {REDRIVE_MAX})"
                )));
            };
            Ok(RabbitRequest::Redrive {
                queue: fields.string("queue")?,
                max: max as u64,
                to_exchange: text_or_empty(&fields, "to_exchange")?.map(|e| normalize_exchange(&e)),
                routing_key: text_or_empty(&fields, "routing_key")?,
            })
        }
        _ => Err(ToolError::new(
            "write_disabled",
            format!("{tool} is not available"),
        )),
    }
}

impl RabbitRequest {
    /// Refuses queues and exchanges outside `--queues` / `--exchanges`.
    pub(super) fn check_scope(&self, guard: &WriteGuard) -> Result<(), ToolError> {
        match self {
            Self::Peek { queue, .. } | Self::Inspect { queue, .. } => guard.check_queue(queue),
            Self::Publish { exchange, message } => {
                guard.check_exchange(exchange, Some(&message.routing_key))
            }
            Self::Redrive {
                queue,
                to_exchange,
                routing_key,
                ..
            } => {
                guard.check_queue(queue)?;
                match to_exchange {
                    Some(exchange) => guard.check_exchange(exchange, routing_key.as_deref()),
                    // Each message's own exchange cannot be checked before
                    // it is moved.
                    None if guard.rabbit_scoped() => Err(ToolError::new(
                        "target_not_allowed",
                        "This server limits RabbitMQ queues or exchanges (--queues, \
                         --exchanges), so a redrive must name its destination with to_exchange \
                         (and routing_key for the default exchange)",
                    )),
                    None => Ok(()),
                }
            }
        }
    }

    /// The audit target: queue, exchange, vhost and routing keys, never a
    /// message body.
    pub(super) fn target(&self, vhost: &str) -> String {
        match self {
            Self::Peek { queue, .. } | Self::Inspect { queue, .. } => {
                format!("queue {queue} on vhost {vhost}")
            }
            Self::Publish { exchange, message } => format!(
                "exchange {} on vhost {vhost}, routing key '{}'",
                exchange_name(exchange),
                message.routing_key
            ),
            Self::Redrive {
                queue,
                to_exchange,
                routing_key,
                ..
            } => format!(
                "queue {queue} on vhost {vhost} to {}",
                destination_text(to_exchange.as_deref(), routing_key.as_deref())
            ),
        }
    }

    /// Records charged to the session's produce budget before running.
    pub(super) fn produce_records(&self) -> u64 {
        match self {
            Self::Publish { .. } => 1,
            Self::Redrive { max, .. } => *max,
            Self::Peek { .. } | Self::Inspect { .. } => 0,
        }
    }
}

/// Where a redrive sends messages, in words.
fn destination_text(to_exchange: Option<&str>, routing_key: Option<&str>) -> String {
    match (to_exchange, routing_key) {
        (None, None) => "each message's original exchange and routing key".to_string(),
        (Some(exchange), None) => format!(
            "exchange {} with each message's original routing key",
            exchange_name(exchange)
        ),
        (None, Some(key)) => {
            format!("each message's original exchange with routing key '{key}'")
        }
        (Some(exchange), Some(key)) => {
            format!("exchange {}, routing key '{key}'", exchange_name(exchange))
        }
    }
}

/// A planned RabbitMQ write.
pub(super) struct RabbitPlanned {
    /// The plan as the agent sees it. Its `observed` part (message counts,
    /// the redrive preview) changes on a busy broker and is left out of
    /// the digest that binds the confirmation token.
    pub plan: Value,
    pub summary: String,
    /// What the plan itself did that changes the broker, for the audit log
    /// (the messages a redrive preview read and requeued).
    pub plan_side_effect: Option<String>,
    pub work: RabbitWork,
}

/// A RabbitMQ write, ready to run.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum RabbitWork {
    Peek {
        queue: String,
        count: u32,
        mask: bool,
    },
    Inspect {
        queue: String,
        count: u32,
        mask: bool,
    },
    Publish {
        exchange: String,
        message: OutgoingMessage,
    },
    #[cfg(feature = "amqp")]
    Redrive {
        queue: String,
        max: u64,
        to_exchange: Option<String>,
        routing_key: Option<String>,
    },
}

/// What a RabbitMQ write did; `outcome` is the audit text (counts and
/// names, no bodies).
pub(super) struct RabbitDone {
    pub result: Value,
    pub outcome: String,
    /// Records charged to the produce budget that were not written.
    pub unused_budget: u64,
}

/// A RabbitMQ write that failed or stopped part-way.
pub(super) struct RabbitFailed {
    pub error: ToolError,
    pub outcome: String,
    pub unused_budget: u64,
}

impl RabbitFailed {
    fn before_anything(error: ToolError, records: u64) -> Box<Self> {
        let outcome = error.message.clone();
        Box::new(Self {
            error,
            outcome,
            unused_budget: records,
        })
    }
}

fn find_queue<'q>(queues: &'q [Queue], name: &str, vhost: &str) -> Result<&'q Queue, ToolError> {
    queues.iter().find(|q| q.name == name).ok_or_else(|| {
        ToolError::invalid(format!("Queue '{name}' does not exist on vhost '{vhost}'"))
    })
}

/// The warnings about reading `queue`: it is requeued, and a quorum queue
/// counts the delivery.
fn requeue_warnings(queue: &Queue) -> Vec<String> {
    let mut warnings = vec![
        "Reading takes the messages and requeues them: the broker marks them redelivered, and \
         messages published meanwhile can change places with them."
            .to_string(),
    ];
    if queue.kind == "quorum" {
        warnings.push(
            "This is a quorum queue: each read counts towards its delivery-limit, and a message \
             past the limit is dead-lettered or dropped."
                .to_string(),
        );
    }
    if queue.consumers.unwrap_or(0) > 0 {
        warnings.push(
            "The queue has consumers: they may receive the requeued messages again.".to_string(),
        );
    }
    warnings
}

/// The plan of `request` on `conn`. A redrive plan with `preview` reads up
/// to [`REDRIVE_PREVIEW_MAX`] messages (and requeues them) to show where
/// they would go; the execute call plans without the preview.
pub(super) async fn plan(
    guard: &WriteGuard,
    backend: &dyn RabbitBackend,
    conn: &RabbitConn,
    tool: &'static str,
    request: &RabbitRequest,
    preview: bool,
) -> Result<RabbitPlanned, ToolError> {
    let vhost = conn.vhost();
    let rabbit = rabbit_tool_error;
    match request {
        RabbitRequest::Peek { queue, count, mask }
        | RabbitRequest::Inspect { queue, count, mask } => {
            // Refuse early; the execute records the export.
            if guard.export_forbidden(&conn.addresses) {
                return Err(ToolError::new(
                    "policy_refused",
                    format!(
                        "Refused by company policy: exporting record data is forbidden (export: \
                         forbidden), so mcp {tool} is not allowed."
                    ),
                ));
            }
            let queues = backend.queues(conn).await.map_err(rabbit)?;
            let found = find_queue(&queues, queue, vhost)?;
            let mask = *mask || guard.masking_enforced();
            let what = if tool == "rabbit_peek" {
                "read"
            } else {
                "inspect (x-death)"
            };
            Ok(RabbitPlanned {
                plan: json!({
                    "action": tool,
                    "rabbit_profile": conn.profile(),
                    "vhost": vhost,
                    "queue": queue,
                    "queue_type": found.kind,
                    "count": count,
                    "mask_pii": mask,
                    "warnings": requeue_warnings(found),
                    "observed": {
                        "messages_ready": found.messages_ready,
                        "consumers": found.consumers,
                    },
                }),
                summary: format!(
                    "{what} up to {count} message(s) of queue '{queue}' on vhost '{vhost}'{} and \
                     requeue them (marked redelivered); bodies {}",
                    profile_note(conn),
                    if mask { "PII-masked" } else { "NOT masked" }
                ),
                plan_side_effect: None,
                work: if tool == "rabbit_peek" {
                    RabbitWork::Peek {
                        queue: queue.clone(),
                        count: *count,
                        mask,
                    }
                } else {
                    RabbitWork::Inspect {
                        queue: queue.clone(),
                        count: *count,
                        mask,
                    }
                },
            })
        }
        RabbitRequest::Publish { exchange, message } => {
            guard.check_produce(1)?;
            let exchanges = backend.exchanges(conn).await.map_err(rabbit)?;
            let name = exchange_name(exchange);
            let Some(found) = exchanges.iter().find(|e| e.name == *exchange) else {
                return Err(ToolError::invalid(format!(
                    "Exchange '{name}' does not exist on vhost '{vhost}' (rabbit_publish never \
                     declares exchanges)"
                )));
            };
            if found.internal {
                return Err(ToolError::invalid(format!(
                    "Exchange '{name}' is internal: clients cannot publish to it"
                )));
            }
            let mut warnings = Vec::new();
            if exchange.is_empty() {
                let queues = backend.queues(conn).await.map_err(rabbit)?;
                if !queues.iter().any(|q| q.name == message.routing_key) {
                    warnings.push(format!(
                        "No queue is named '{}': through the default exchange the message would \
                         be unroutable.",
                        message.routing_key
                    ));
                }
            } else {
                let bindings = backend.bindings(conn).await.map_err(rabbit)?;
                if !bindings.iter().any(|b| b.source == *exchange) {
                    warnings.push(format!(
                        "Exchange '{name}' has no bindings: the message would be unroutable."
                    ));
                }
            }
            let mut header_names: Vec<&str> =
                message.headers.iter().map(|(k, _)| k.as_str()).collect();
            header_names.sort_unstable();
            let bytes = message.payload.len();
            Ok(RabbitPlanned {
                plan: json!({
                    "action": tool,
                    "rabbit_profile": conn.profile(),
                    "vhost": vhost,
                    "exchange": name,
                    "exchange_type": found.kind,
                    "routing_key": message.routing_key,
                    "bytes": bytes,
                    "body_sha256": super::write_tools::short_sha256(message.payload.as_bytes()),
                    "headers": header_names,
                    "content_type": message.content_type,
                    "persistent": message.persistent,
                    "warnings": warnings,
                }),
                summary: format!(
                    "publish one message ({bytes} bytes) to exchange '{name}' on vhost \
                     '{vhost}'{} with routing key '{}'",
                    profile_note(conn),
                    message.routing_key
                ),
                plan_side_effect: None,
                work: RabbitWork::Publish {
                    exchange: exchange.clone(),
                    message: message.clone(),
                },
            })
        }
        RabbitRequest::Redrive {
            queue,
            max,
            to_exchange,
            routing_key,
        } => {
            plan_redrive(
                guard,
                backend,
                conn,
                tool,
                (queue, *max, to_exchange.as_deref(), routing_key.as_deref()),
                preview,
            )
            .await
        }
    }
}

/// ` (RabbitMQ profile 'p')`, or nothing without a profile.
fn profile_note(conn: &RabbitConn) -> String {
    match conn.profile() {
        "" => String::new(),
        name => format!(" (RabbitMQ profile '{name}')"),
    }
}

#[cfg(not(feature = "amqp"))]
async fn plan_redrive(
    _guard: &WriteGuard,
    _backend: &dyn RabbitBackend,
    _conn: &RabbitConn,
    tool: &'static str,
    _request: (&str, u64, Option<&str>, Option<&str>),
    _preview: bool,
) -> Result<RabbitPlanned, ToolError> {
    Err(ToolError::new(
        "write_disabled",
        format!("{tool} needs Ferrika built with AMQP support (feature amqp)"),
    ))
}

#[cfg(feature = "amqp")]
async fn plan_redrive(
    guard: &WriteGuard,
    backend: &dyn RabbitBackend,
    conn: &RabbitConn,
    tool: &'static str,
    (queue, max, to_exchange, routing_key): (&str, u64, Option<&str>, Option<&str>),
    preview: bool,
) -> Result<RabbitPlanned, ToolError> {
    let vhost = conn.vhost();
    let rabbit = rabbit_tool_error;
    guard.check_produce(max)?;
    let queues = backend.queues(conn).await.map_err(rabbit)?;
    let found = find_queue(&queues, queue, vhost)?;
    if let Some(exchange) = to_exchange {
        let exchanges = backend.exchanges(conn).await.map_err(rabbit)?;
        match exchanges.iter().find(|e| e.name == exchange) {
            None => {
                return Err(ToolError::invalid(format!(
                    "Exchange '{}' does not exist on vhost '{vhost}'",
                    exchange_name(exchange)
                )))
            }
            Some(e) if e.internal => {
                return Err(ToolError::invalid(format!(
                    "Exchange '{}' is internal: clients cannot publish to it",
                    exchange_name(exchange)
                )))
            }
            Some(_) => {}
        }
    }
    let destination = destination_text(to_exchange, routing_key);
    let mut observed = json!({
        "messages_ready": found.messages_ready,
        "consumers": found.consumers,
    });
    let mut plan_side_effect = None;
    if preview {
        let target = format!("queue {queue} on vhost {vhost} (redrive preview)");
        // Reading for the preview requeues: the peek category applies.
        guard.authorize_rabbit(
            tool,
            ferrika_core::model::policy::PolicyAction::MessagesPeek,
            &target,
            conn.profile(),
            &conn.addresses,
            conn.read_only_refusal(tool),
        )?;
        let count = u32::try_from(max)
            .unwrap_or(REDRIVE_PREVIEW_MAX)
            .min(REDRIVE_PREVIEW_MAX);
        let messages = backend.peek(conn, queue, count).await.map_err(rabbit)?;
        let preview = RedrivePreview::new(queue, vhost, &messages, to_exchange, routing_key)
            .with_unmovable_problem("no x-death header; give to_exchange");
        // Destinations seen in the preview must be in scope too.
        for planned in &preview.messages {
            if let Some(d) = &planned.destination {
                guard.check_exchange(&d.exchange, Some(&d.routing_key))?;
            }
        }
        let mut warnings = Vec::new();
        if preview.unmovable > 0 {
            warnings.push(format!(
                "{} message(s) have no x-death header and no to_exchange is given: the move \
                 stops at the first of them.",
                preview.unmovable
            ));
        }
        if max > messages.len() as u64 && messages.len() as u64 == u64::from(count) {
            warnings.push(format!(
                "The preview read the first {} message(s); the move takes up to {max}, each to \
                 its own destination.",
                messages.len()
            ));
        }
        observed["preview"] = json!({
            "read": messages.len(),
            "destinations": preview.destinations,
            "unmovable": preview.unmovable,
            "warnings": warnings,
        });
        plan_side_effect = Some(format!(
            "read {} message(s) for the redrive preview; requeued (marked redelivered)",
            messages.len()
        ));
    }
    let mut warnings = vec![
        "Each message is published to its destination, confirmed by the broker, then removed \
         from the queue; the move stops at the first failure and the message being moved goes \
         back to the queue."
            .to_string(),
        "Redriven messages keep their headers (x-death included) and get \
         x-ferrika-redriven-from."
            .to_string(),
    ];
    if found.consumers.unwrap_or(0) > 0 {
        warnings.push("The queue has consumers, which may take messages meanwhile.".to_string());
    }
    Ok(RabbitPlanned {
        plan: json!({
            "action": tool,
            "rabbit_profile": conn.profile(),
            "vhost": vhost,
            "queue": queue,
            "max": max,
            "destination": destination,
            "broker": conn.addresses,
            "warnings": warnings,
            "observed": observed,
        }),
        summary: format!(
            "move up to {max} message(s) from queue '{queue}' on vhost '{vhost}'{} to \
             {destination} over AMQP",
            profile_note(conn)
        ),
        plan_side_effect,
        work: RabbitWork::Redrive {
            queue: queue.to_string(),
            max,
            to_exchange: to_exchange.map(str::to_string),
            routing_key: routing_key.map(str::to_string),
        },
    })
}

/// A message as the tools return it: masked as planned, its payload cut to
/// [`PAYLOAD_TEXT_MAX`] characters.
fn shown(message: &PeekedMessage, mask: bool) -> ShownMessage {
    let mut shown = ShownMessage::new(message, mask);
    cap_payload(&mut shown);
    shown
}

/// Cuts a shown payload to [`PAYLOAD_TEXT_MAX`] characters.
fn cap_payload(shown: &mut ShownMessage) {
    shown.payload = shown
        .payload
        .take()
        .map(|p| ellipsize(&p, PAYLOAD_TEXT_MAX));
    shown.payload_base64 = shown
        .payload_base64
        .take()
        .map(|p| ellipsize(&p, PAYLOAD_TEXT_MAX));
}

/// A management API or connection failure; timeouts and connection
/// failures may be retried.
fn rabbit_tool_error(e: impl std::fmt::Display) -> ToolError {
    let mut error = ToolError::kafka(e);
    error.code = "rabbitmq_error";
    error
}

/// Runs `work` on `conn`. `records` is what was charged to the produce
/// budget for it.
pub(super) async fn execute(
    guard: &WriteGuard,
    backend: &dyn RabbitBackend,
    conn: &RabbitConn,
    tool: &str,
    target: &str,
    work: RabbitWork,
    records: u64,
) -> Result<RabbitDone, Box<RabbitFailed>> {
    let vhost = conn.vhost();
    let rabbit = |e: KafkaError| RabbitFailed::before_anything(rabbit_tool_error(e), records);
    match work {
        RabbitWork::Peek { queue, count, mask } | RabbitWork::Inspect { queue, count, mask } => {
            let inspect = tool == "rabbit_dlq_inspect";
            guard
                .authorize_rabbit_export(tool, target, conn.profile(), &conn.addresses)
                .map_err(|e| RabbitFailed::before_anything(e, records))?;
            let messages = backend.peek(conn, &queue, count).await.map_err(rabbit)?;
            let masked = if mask { "masked" } else { "not masked" };
            let read = messages.len();
            let note = format!(
                "{read} message(s) read and requeued; the broker now marks them redelivered."
            );
            if inspect {
                let mut inspection = Inspection::new(&messages, mask);
                let groups = inspection.groups.len();
                inspection.messages.truncate(INSPECT_SAMPLE);
                for m in &mut inspection.messages {
                    cap_payload(&mut m.message);
                }
                Ok(RabbitDone {
                    result: json!({
                        "queue": queue,
                        "vhost": vhost,
                        "read": read,
                        "masked": mask,
                        "groups": inspection.groups,
                        "sample": inspection.messages,
                        "note": note,
                    }),
                    outcome: format!(
                        "read {read} message(s) in {groups} group(s), requeued; {masked}"
                    ),
                    unused_budget: 0,
                })
            } else {
                let messages: Vec<ShownMessage> = messages.iter().map(|m| shown(m, mask)).collect();
                Ok(RabbitDone {
                    result: json!({
                        "queue": queue,
                        "vhost": vhost,
                        "read": read,
                        "masked": mask,
                        "messages": messages,
                        "note": note,
                    }),
                    outcome: format!("read {read} message(s), requeued; {masked}"),
                    unused_budget: 0,
                })
            }
        }
        RabbitWork::Publish { exchange, message } => {
            let outcome = backend
                .publish(conn, &exchange, &message)
                .await
                .map_err(rabbit)?;
            let bytes = message.payload.len();
            let sha = super::write_tools::short_sha256(message.payload.as_bytes());
            let name = exchange_name(&exchange);
            if !outcome.routed {
                return Err(Box::new(RabbitFailed {
                    error: ToolError::new(
                        "unroutable",
                        format!(
                            "The message was not routed: exchange '{name}' has no binding \
                             matching routing key '{}', so the broker dropped it (or sent it to \
                             the exchange's alternate exchange)",
                            message.routing_key
                        ),
                    ),
                    outcome: format!("{bytes} bytes (sha256 {sha}) not routed"),
                    // A publish the broker took counts, routed or not.
                    unused_budget: 0,
                }));
            }
            Ok(RabbitDone {
                result: json!({
                    "exchange": name,
                    "vhost": vhost,
                    "routing_key": message.routing_key,
                    "bytes": bytes,
                    "routed": true,
                }),
                outcome: format!("published {bytes} bytes (sha256 {sha}), routed"),
                unused_budget: 0,
            })
        }
        #[cfg(feature = "amqp")]
        RabbitWork::Redrive {
            queue,
            max,
            to_exchange,
            routing_key,
        } => execute_redrive(backend, conn, queue, max, to_exchange, routing_key).await,
    }
}

#[cfg(feature = "amqp")]
async fn execute_redrive(
    backend: &dyn RabbitBackend,
    conn: &RabbitConn,
    queue: String,
    max: u64,
    to_exchange: Option<String>,
    routing_key: Option<String>,
) -> Result<RabbitDone, Box<RabbitFailed>> {
    use ferrika_rabbitmq::view::redrive_outcome;
    let request = RedriveRequest {
        queue: queue.clone(),
        limit: max,
        to_exchange,
        routing_key,
    };
    match backend.redrive(conn, &request).await {
        Ok(report) => Ok(RabbitDone {
            result: json!({
                "queue": queue,
                "vhost": conn.vhost(),
                "moved": report.moved,
                "destinations": report.destinations,
                "stopped": Value::Null,
            }),
            outcome: redrive_outcome(&queue, &report, None),
            unused_budget: max.saturating_sub(report.moved),
        }),
        Err(error) => {
            let report = &error.report;
            Err(Box::new(RabbitFailed {
                outcome: redrive_outcome(&queue, report, Some(error.failure)),
                unused_budget: max.saturating_sub(report.moved),
                error: ToolError::new("redrive_stopped", error.to_string())
                    .with("moved", json!(report.moved))
                    .with("destinations", json!(report.destinations))
                    .with("stopped", json!(error.failure.as_str()))
                    .with("requeued", json!(error.failure.requeued())),
            }))
        }
    }
}

/// The RabbitMQ write tools' definitions (filtered by the session as the
/// Kafka ones are).
pub(super) fn definitions() -> Vec<Value> {
    use super::write_tools::{annotations, with_mode};
    let connection = |mut properties: Value| {
        properties["profile"] = json!({ "type": "string", "description": "RabbitMQ profile name (ferrika rabbit profile list); default: the active RabbitMQ profile" });
        properties["vhost"] =
            json!({ "type": "string", "description": "Virtual host (default: the profile's)" });
        with_mode(properties)
    };
    let mask = json!({ "type": "boolean", "default": true, "description": "Mask emails, card numbers, SSNs and tokens in bodies and headers (always on when the company policy enforces masking)" });
    #[allow(unused_mut)]
    let mut tools = vec![
        json!({
            "name": "rabbit_peek",
            "title": "Peek at RabbitMQ messages",
            "description": "RabbitMQ (preview): reads up to 20 messages at the head of a queue and returns them PII-masked. The management API has no browse: the messages are taken and requeued, so the broker marks them redelivered (on a quorum queue this counts towards its delivery-limit). It is therefore a write: plan first, then execute with the plan's confirmation token.",
            "inputSchema": {
                "type": "object",
                "additionalProperties": false,
                "properties": connection(json!({
                    "queue": { "type": "string", "minLength": 1 },
                    "count": { "type": "integer", "minimum": 1, "maximum": PEEK_MAX, "default": 5 },
                    "mask_pii": mask.clone()
                })),
                "required": ["queue"]
            },
            "annotations": annotations("Peek at RabbitMQ messages", false, false)
        }),
        json!({
            "name": "rabbit_dlq_inspect",
            "title": "Inspect a RabbitMQ dead-letter queue",
            "description": "RabbitMQ (preview): reads up to 50 messages of a dead-letter queue and groups them by where and why they died (x-death: source queue, reason, exchange, routing key), with a PII-masked sample of 10. Messages are requeued (marked redelivered), so it is planned and confirmed like a write.",
            "inputSchema": {
                "type": "object",
                "additionalProperties": false,
                "properties": connection(json!({
                    "queue": { "type": "string", "minLength": 1 },
                    "count": { "type": "integer", "minimum": 1, "maximum": INSPECT_MAX, "default": 20 },
                    "mask_pii": mask
                })),
                "required": ["queue"]
            },
            "annotations": annotations("Inspect a RabbitMQ dead-letter queue", false, false)
        }),
        json!({
            "name": "rabbit_publish",
            "title": "Publish a RabbitMQ message",
            "description": "RabbitMQ (preview): publishes one UTF-8 message to an existing exchange (\"\" or \"amq.default\": the default exchange, where the routing key names the queue). The plan shows the size, a short hash and header names, never the body. A message no binding routes is an error (unroutable).",
            "inputSchema": {
                "type": "object",
                "additionalProperties": false,
                "properties": connection(json!({
                    "exchange": { "type": "string" },
                    "routing_key": { "type": "string", "default": "" },
                    "body": { "type": "string", "description": "UTF-8 text" },
                    "headers": { "type": "object", "additionalProperties": { "type": "string" } },
                    "content_type": { "type": "string", "description": "e.g. application/json" },
                    "persistent": { "type": "boolean", "default": true, "description": "delivery_mode 2 (stored on disk by durable queues)" }
                })),
                "required": ["exchange", "body"]
            },
            "annotations": annotations("Publish a RabbitMQ message", false, false)
        }),
    ];
    #[cfg(feature = "amqp")]
    tools.push(json!({
        "name": "rabbit_dlq_redrive",
        "title": "Redrive a RabbitMQ dead-letter queue",
        "description": "RabbitMQ (preview): moves up to max messages out of a dead-letter queue back to their original exchange and routing key (from x-death), or to to_exchange / routing_key. The plan reads up to 100 messages (requeued) to show how many go where. Execute moves them one by one over AMQP: publish with publisher confirms, then remove from the queue; it stops at the first failure and says how far it got.",
        "inputSchema": {
            "type": "object",
            "additionalProperties": false,
            "properties": connection(json!({
                "queue": { "type": "string", "minLength": 1 },
                "max": { "type": "integer", "minimum": 1, "maximum": REDRIVE_MAX, "description": "Most messages to move; counts against --max-produce-records" },
                "to_exchange": { "type": "string", "description": "Publish to this exchange instead of each message's original one" },
                "routing_key": { "type": "string", "description": "Publish with this routing key instead of each message's original one" }
            })),
            "required": ["queue", "max"]
        },
        "annotations": annotations("Redrive a RabbitMQ dead-letter queue", false, false)
    }));
    tools
}

#[cfg(test)]
mod tests {
    use std::cell::{Cell, RefCell};

    use super::*;
    use crate::backend::fake::FakeCluster;
    use crate::guard::tests::{config, guard_with, no_policy, policy, MemoryLog};
    use crate::guard::GuardConfig;
    use crate::write_tools::tests::{code, execute, token};
    use crate::write_tools::WriteTools;
    use ferrika_core::model::policy::PolicyAction;
    use ferrika_core::model::policy::{McpWriteRule, PolicyRules};
    use ferrika_governance::policy::EffectivePolicy;
    use ferrika_rabbitmq::profile::RabbitProfile;

    /// A RabbitMQ broker in memory.
    #[derive(Default)]
    struct FakeRabbit {
        read_only: bool,
        queues: RefCell<Vec<Queue>>,
        exchanges: Vec<Exchange>,
        bindings: Vec<Binding>,
        messages: Vec<PeekedMessage>,
        unroutable: bool,
        peeks: Cell<usize>,
        published: RefCell<Vec<(String, OutgoingMessage)>>,
        #[cfg(feature = "amqp")]
        redrives: RefCell<Vec<RedriveRequest>>,
        #[cfg(feature = "amqp")]
        redrive_failure: Option<ferrika_rabbitmq::amqp::RedriveFailure>,
    }

    impl RabbitBackend for &'static FakeRabbit {
        fn connect(&self, args: &Value) -> KafkaResult<RabbitConn> {
            let name = args["profile"].as_str().unwrap_or("dev-rabbit");
            if name == "missing" {
                return Err(KafkaError::config("RabbitMQ profile 'missing' not found"));
            }
            let profile = RabbitProfile {
                read_only: self.read_only,
                ..RabbitProfile::new("http://rabbit-dev:15672")
            };
            Ok(RabbitConn::new(
                ResolvedRabbit::from_profile(name, &profile),
                "rabbit-dev:15672,rabbit-dev:5672".into(),
                None,
            ))
        }

        fn queues<'a>(&'a self, _: &'a RabbitConn) -> BoxFuture<'a, Vec<Queue>> {
            let queues = self.queues.borrow().clone();
            Box::pin(async move { Ok(queues) })
        }

        fn exchanges<'a>(&'a self, _: &'a RabbitConn) -> BoxFuture<'a, Vec<Exchange>> {
            Box::pin(async move { Ok(self.exchanges.clone()) })
        }

        fn bindings<'a>(&'a self, _: &'a RabbitConn) -> BoxFuture<'a, Vec<Binding>> {
            Box::pin(async move { Ok(self.bindings.clone()) })
        }

        fn peek<'a>(
            &'a self,
            _: &'a RabbitConn,
            _queue: &'a str,
            count: u32,
        ) -> BoxFuture<'a, Vec<PeekedMessage>> {
            self.peeks.set(self.peeks.get() + 1);
            let messages = self.messages.iter().take(count as usize).cloned().collect();
            Box::pin(async move { Ok(messages) })
        }

        fn publish<'a>(
            &'a self,
            _: &'a RabbitConn,
            exchange: &'a str,
            message: &'a OutgoingMessage,
        ) -> BoxFuture<'a, PublishOutcome> {
            self.published
                .borrow_mut()
                .push((exchange.to_string(), message.clone()));
            let routed = !self.unroutable;
            Box::pin(async move { Ok(PublishOutcome { routed }) })
        }

        #[cfg(feature = "amqp")]
        fn redrive<'a>(
            &'a self,
            _: &'a RabbitConn,
            request: &'a RedriveRequest,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<RedriveReport, RedriveError>> + 'a>,
        > {
            self.redrives.borrow_mut().push(request.clone());
            let report = RedriveReport {
                moved: 2,
                destinations: [(
                    "exchange 'shop', routing key 'order.created'".to_string(),
                    2,
                )]
                .into_iter()
                .collect(),
            };
            let result = match self.redrive_failure {
                None => Ok(report),
                Some(failure) => Err(RedriveError {
                    report,
                    failure,
                    detail: "PRECONDITION_FAILED from the broker".into(),
                }),
            };
            Box::pin(async move { result })
        }
    }

    fn queue(name: &str, kind: &str, ready: u64) -> Queue {
        serde_json::from_value(json!({
            "name": name, "vhost": "/", "type": kind, "messages_ready": ready, "consumers": 0
        }))
        .unwrap()
    }

    fn dead_letter(body: &str) -> PeekedMessage {
        serde_json::from_value(json!({
            "exchange": "dlx",
            "routing_key": "order.created",
            "payload": body,
            "payload_bytes": body.len(),
            "payload_encoding": "string",
            "properties": {
                "headers": {
                    "customer": "kim@example.com",
                    "x-death": [{
                        "queue": "orders", "reason": "rejected", "count": 1,
                        "exchange": "shop", "routing-keys": ["order.created"]
                    }]
                }
            }
        }))
        .unwrap()
    }

    fn broker() -> FakeRabbit {
        FakeRabbit {
            queues: RefCell::new(vec![
                queue("orders", "classic", 0),
                queue("orders.dlq", "quorum", 3),
                queue("test-q", "classic", 0),
            ]),
            exchanges: serde_json::from_value(json!([
                { "name": "", "vhost": "/", "type": "direct" },
                { "name": "shop", "vhost": "/", "type": "topic" },
                { "name": "test.x", "vhost": "/", "type": "fanout" },
                { "name": "internal.x", "vhost": "/", "type": "topic", "internal": true },
                { "name": "amq.rabbitmq.trace", "vhost": "/", "type": "topic", "internal": true }
            ]))
            .unwrap(),
            bindings: serde_json::from_value(json!([
                { "source": "shop", "vhost": "/", "destination": "orders", "destination_type": "queue", "routing_key": "order.*" }
            ]))
            .unwrap(),
            messages: vec![
                dead_letter(r#"{"email":"kim@example.com","total":12}"#),
                dead_letter(r#"{"email":"lee@example.com","total":30}"#),
            ],
            ..Default::default()
        }
    }

    /// Write tools over `rabbit` with `config` and `policy`.
    fn tools_on(
        rabbit: FakeRabbit,
        config: GuardConfig,
        policy: &'static EffectivePolicy,
    ) -> (WriteTools, &'static FakeRabbit, MemoryLog) {
        let rabbit: &'static FakeRabbit = Box::leak(Box::new(rabbit));
        let cluster: &'static FakeCluster = Box::leak(Box::new(FakeCluster::default()));
        let (guard, log, _) = guard_with(config, policy);
        let mut tools = WriteTools::new(guard, Box::new(cluster));
        tools.rabbit = Box::new(rabbit);
        (tools, rabbit, log)
    }

    fn fresh(rabbit: FakeRabbit) -> (WriteTools, &'static FakeRabbit, MemoryLog) {
        tools_on(rabbit, config(), no_policy())
    }

    fn scoped(queues: &[&str], exchanges: &[&str]) -> GuardConfig {
        GuardConfig {
            queues: queues.iter().map(|q| q.to_string()).collect(),
            exchanges: exchanges.iter().map(|e| e.to_string()).collect(),
            ..config()
        }
    }

    #[tokio::test]
    async fn peek_plans_without_reading_then_returns_masked_messages() {
        let (mut tools, rabbit, log) = fresh(broker());
        let args = json!({ "queue": "orders.dlq", "count": 5 });
        let plan = tools.call_token("rabbit_peek", &args).await;
        assert_eq!(plan["isError"], false, "{plan}");
        let p = &plan["structuredContent"]["plan"];
        assert_eq!(p["queue_type"], "quorum");
        assert_eq!(p["mask_pii"], true);
        assert!(p["warnings"].to_string().contains("delivery-limit"), "{p}");
        // A plan reads nothing and records nothing.
        assert_eq!(rabbit.peeks.get(), 0);
        assert!(log.entries.borrow().is_empty());

        // New messages between plan and execute change only `observed`.
        rabbit.queues.borrow_mut()[1] = queue("orders.dlq", "quorum", 9);
        let done = tools
            .call_token("rabbit_peek", &execute(&args, &token(&plan)))
            .await;
        assert_eq!(done["structuredContent"]["executed"], true, "{done}");
        assert_eq!(rabbit.peeks.get(), 1);
        let result = &done["structuredContent"]["result"];
        assert_eq!(result["read"], 2);
        assert_eq!(result["masked"], true);
        let text = done.to_string();
        assert!(!text.contains("kim@example.com"), "{text}");
        assert!(text.contains("total"), "{text}");

        let entries = log.entries.borrow();
        let entry = entries
            .iter()
            .find(|e| e.action == "mcp rabbit_peek")
            .unwrap();
        assert_eq!(entry.profile, "dev-rabbit");
        assert_eq!(entry.target, "queue orders.dlq on vhost /");
        assert!(entry.ok);
        assert!(entry.outcome.contains("read 2 message(s), requeued"));
        assert!(!entry.outcome.contains("example.com"));
    }

    #[tokio::test]
    async fn mask_pii_false_is_honoured_unless_the_policy_enforces_masking() {
        let args = json!({ "queue": "orders.dlq", "mask_pii": false });
        let (mut open, _, _) = fresh(broker());
        let plan = open.call_token("rabbit_peek", &args).await;
        let done = open
            .call_token("rabbit_peek", &execute(&args, &token(&plan)))
            .await;
        assert!(done.to_string().contains("kim@example.com"), "{done}");

        let enforced: &'static EffectivePolicy =
            Box::leak(Box::new(ferrika_governance::test_policy::masking()));
        let (mut strict, _, _) = tools_on(broker(), config(), enforced);
        let plan = strict.call_token("rabbit_peek", &args).await;
        assert_eq!(plan["structuredContent"]["plan"]["mask_pii"], true);
        let done = strict
            .call_token("rabbit_peek", &execute(&args, &token(&plan)))
            .await;
        assert_eq!(done["structuredContent"]["executed"], true, "{done}");
        assert!(!done.to_string().contains("kim@example.com"), "{done}");
    }

    #[tokio::test]
    async fn queues_outside_the_scope_and_bad_arguments_are_refused() {
        let (mut tools, rabbit, _) = tools_on(broker(), scoped(&["test-*"], &[]), no_policy());
        for (tool, args, want) in [
            (
                "rabbit_peek",
                json!({ "queue": "orders.dlq" }),
                "target_not_allowed",
            ),
            (
                "rabbit_dlq_inspect",
                json!({ "queue": "orders.dlq" }),
                "target_not_allowed",
            ),
            (
                "rabbit_peek",
                json!({ "queue": "test-q", "count": 21 }),
                "invalid_argument",
            ),
            (
                "rabbit_dlq_inspect",
                json!({ "queue": "test-q", "count": 51 }),
                "invalid_argument",
            ),
            (
                "rabbit_peek",
                json!({ "queue": "test-q", "url": "http://x" }),
                "invalid_argument",
            ),
            (
                "rabbit_peek",
                json!({ "queue": "test-nope" }),
                "invalid_argument",
            ),
            (
                "rabbit_peek",
                json!({ "queue": "test-q", "profile": "missing" }),
                "invalid_argument",
            ),
        ] {
            let result = tools.call_token(tool, &args).await;
            assert_eq!(code(&result), want, "{tool} {args}: {result}");
        }
        assert!(
            tools
                .call_token("rabbit_peek", &json!({ "queue": "test-q" }))
                .await["isError"]
                == false
        );
        assert_eq!(rabbit.peeks.get(), 0);
    }

    #[tokio::test]
    async fn dlq_inspect_groups_by_x_death_with_a_masked_sample() {
        let (mut tools, _, log) = fresh(broker());
        let args = json!({ "queue": "orders.dlq" });
        let plan = tools.call_token("rabbit_dlq_inspect", &args).await;
        let done = tools
            .call_token("rabbit_dlq_inspect", &execute(&args, &token(&plan)))
            .await;
        let result = &done["structuredContent"]["result"];
        assert_eq!(result["groups"][0]["queue"], "orders", "{done}");
        assert_eq!(result["groups"][0]["reason"], "rejected");
        assert_eq!(result["groups"][0]["messages"], 2);
        assert_eq!(result["sample"].as_array().unwrap().len(), 2);
        assert!(!done.to_string().contains("kim@example.com"), "{done}");
        assert!(log
            .entries
            .borrow()
            .iter()
            .any(|e| e.action == "mcp rabbit_dlq_inspect" && e.outcome.contains("1 group(s)")));
    }

    #[tokio::test]
    async fn export_forbidden_refuses_reading_messages() {
        let forbidden: &'static EffectivePolicy =
            Box::leak(Box::new(ferrika_governance::test_policy::export_forbidden()));
        let (mut tools, rabbit, _) = tools_on(broker(), config(), forbidden);
        let result = tools
            .call_token("rabbit_peek", &json!({ "queue": "orders.dlq" }))
            .await;
        assert_eq!(code(&result), "policy_refused", "{result}");
        assert!(result.to_string().contains("export: forbidden"));
        assert_eq!(rabbit.peeks.get(), 0);
    }

    #[tokio::test]
    async fn publish_shows_no_body_and_unroutable_is_an_error() {
        let (mut tools, rabbit, log) = fresh(broker());
        let args = json!({
            "exchange": "shop",
            "routing_key": "order.created",
            "body": "{\"email\":\"kim@example.com\"}",
            "headers": { "trace": "abc" }
        });
        let plan = tools.call_token("rabbit_publish", &args).await;
        assert_eq!(plan["isError"], false, "{plan}");
        assert!(!plan.to_string().contains("kim@example.com"), "{plan}");
        assert_eq!(
            plan["structuredContent"]["plan"]["headers"],
            json!(["trace"])
        );
        assert!(rabbit.published.borrow().is_empty());
        let done = tools
            .call_token("rabbit_publish", &execute(&args, &token(&plan)))
            .await;
        assert_eq!(
            done["structuredContent"]["result"]["routed"], true,
            "{done}"
        );
        assert_eq!(rabbit.published.borrow()[0].0, "shop");
        assert!(rabbit.published.borrow()[0].1.persistent);
        let entry = log.entries.borrow()[0].clone();
        assert_eq!(entry.action, "mcp rabbit_publish");
        assert_eq!(
            entry.target,
            "exchange shop on vhost /, routing key 'order.created'"
        );
        assert!(!entry.outcome.contains("example.com"));

        let (mut tools, _, log) = tools_on(
            FakeRabbit {
                unroutable: true,
                ..broker()
            },
            config(),
            no_policy(),
        );
        let args = json!({ "exchange": "test.x", "body": "hi" });
        let plan = tools.call_token("rabbit_publish", &args).await;
        assert!(plan["structuredContent"]["plan"]["warnings"]
            .to_string()
            .contains("no bindings"));
        let result = tools
            .call_token("rabbit_publish", &execute(&args, &token(&plan)))
            .await;
        assert_eq!(code(&result), "unroutable", "{result}");
        assert!(!log.entries.borrow()[0].ok);
    }

    #[tokio::test]
    async fn publish_scope_covers_exchanges_and_the_default_exchange_queue() {
        let (mut tools, rabbit, _) = tools_on(
            broker(),
            scoped(&["test-*"], &["test.*", "amq.default"]),
            no_policy(),
        );
        for (args, want) in [
            (
                json!({ "exchange": "shop", "body": "x" }),
                "target_not_allowed",
            ),
            (
                json!({ "exchange": "", "routing_key": "orders", "body": "x" }),
                "target_not_allowed",
            ),
            (
                json!({ "exchange": "amq.rabbitmq.trace", "body": "x" }),
                "target_not_allowed",
            ),
            (
                json!({ "exchange": "test.nope", "body": "x" }),
                "invalid_argument",
            ),
            (json!({ "exchange": "test.x" }), "invalid_argument"),
        ] {
            let result = tools.call_token("rabbit_publish", &args).await;
            assert_eq!(code(&result), want, "{args}: {result}");
        }
        let (mut open, _, _) = fresh(broker());
        let internal = open
            .call_token(
                "rabbit_publish",
                &json!({ "exchange": "internal.x", "body": "x" }),
            )
            .await;
        assert_eq!(code(&internal), "invalid_argument", "{internal}");
        let ok = tools
            .call_token(
                "rabbit_publish",
                &json!({ "exchange": "amq.default", "routing_key": "test-q", "body": "x" }),
            )
            .await;
        assert_eq!(ok["isError"], false, "{ok}");
        assert!(rabbit.published.borrow().is_empty());
    }

    #[tokio::test]
    async fn a_read_only_rabbit_profile_refuses_but_a_read_only_kafka_profile_does_not() {
        let (mut tools, rabbit, log) = fresh(FakeRabbit {
            read_only: true,
            ..broker()
        });
        let result = tools
            .call_token("rabbit_peek", &json!({ "queue": "orders.dlq" }))
            .await;
        assert_eq!(code(&result), "read_only_profile", "{result}");
        assert_eq!(rabbit.peeks.get(), 0);
        assert_eq!(log.entries.borrow()[0].profile, "dev-rabbit");

        let kafka_read_only = GuardConfig {
            profile_refusal: Some("Profile 'dev' is read-only".into()),
            ..config()
        };
        let (mut tools, _, _) = tools_on(broker(), kafka_read_only, no_policy());
        let names: Vec<String> = tools
            .definitions(false)
            .iter()
            .filter_map(|d| d["name"].as_str().map(str::to_string))
            .collect();
        assert!(names.contains(&"rabbit_peek".to_string()), "{names:?}");
        assert!(!names.contains(&"create_topic".to_string()), "{names:?}");
        let plan = tools
            .call_token("rabbit_peek", &json!({ "queue": "orders.dlq" }))
            .await;
        assert_eq!(plan["isError"], false, "{plan}");
    }

    #[tokio::test]
    async fn policy_categories_and_mcp_write_apply() {
        let no_peek = policy(PolicyRules {
            forbid: vec!["messages.peek".into()],
            ..Default::default()
        });
        let (mut tools, _, log) = tools_on(broker(), config(), no_peek);
        assert!(!tools
            .definitions(false)
            .iter()
            .any(|d| d["name"] == "rabbit_peek" || d["name"] == "rabbit_dlq_inspect"));
        let result = tools
            .call_token("rabbit_peek", &json!({ "queue": "orders.dlq" }))
            .await;
        assert_eq!(code(&result), "policy_refused", "{result}");
        assert_eq!(log.entries.borrow()[0].action, "policy.blocked");
        let plan = tools
            .call_token(
                "rabbit_publish",
                &json!({ "exchange": "shop", "routing_key": "order.created", "body": "x" }),
            )
            .await;
        assert_eq!(plan["isError"], false, "{plan}");

        let forbidden = policy(PolicyRules {
            mcp_write: McpWriteRule::Forbidden,
            ..Default::default()
        });
        let (mut tools, rabbit, _) = tools_on(broker(), config(), forbidden);
        assert!(tools.definitions(true).is_empty());
        let result = tools
            .call_token(
                "rabbit_publish",
                &json!({ "exchange": "shop", "body": "x" }),
            )
            .await;
        assert_eq!(code(&result), "policy_refused", "{result}");
        assert!(rabbit.published.borrow().is_empty());
    }

    #[tokio::test]
    async fn read_only_broker_rules_match_the_rabbit_addresses() {
        let rules = policy(PolicyRules {
            read_only_brokers: vec!["rabbit-dev:5672".into()],
            ..Default::default()
        });
        let (mut tools, _, _) = tools_on(broker(), config(), rules);
        let result = tools
            .call_token(
                "rabbit_publish",
                &json!({ "exchange": "shop", "body": "x" }),
            )
            .await;
        assert_eq!(code(&result), "policy_refused", "{result}");
    }

    #[cfg(feature = "amqp")]
    #[tokio::test]
    async fn redrive_plan_previews_destinations_and_execute_moves() {
        let (mut tools, rabbit, log) = fresh(broker());
        let args = json!({ "queue": "orders.dlq", "max": 10 });
        let plan = tools.call_token("rabbit_dlq_redrive", &args).await;
        assert_eq!(plan["isError"], false, "{plan}");
        let preview = &plan["structuredContent"]["plan"]["observed"]["preview"];
        assert_eq!(preview["read"], 2, "{plan}");
        assert_eq!(
            preview["destinations"]["exchange 'shop', routing key 'order.created'"],
            2
        );
        assert_eq!(rabbit.peeks.get(), 1);
        // The preview's requeue is audited, without bodies.
        assert!(log
            .entries
            .borrow()
            .iter()
            .any(|e| e.action == "mcp rabbit_dlq_redrive plan" && e.outcome.contains("read 2")));

        let done = tools
            .call_token("rabbit_dlq_redrive", &execute(&args, &token(&plan)))
            .await;
        assert_eq!(done["structuredContent"]["result"]["moved"], 2, "{done}");
        // The execute does not read a preview again.
        assert_eq!(rabbit.peeks.get(), 1);
        let requests = rabbit.redrives.borrow();
        assert_eq!(requests[0].queue, "orders.dlq");
        assert_eq!(requests[0].limit, 10);
        let entries = log.entries.borrow();
        let entry = entries
            .iter()
            .find(|e| e.action == "mcp rabbit_dlq_redrive")
            .unwrap();
        assert!(entry.ok);
        assert!(entry
            .outcome
            .contains("moved 2 message(s) from queue orders.dlq"));
        assert_eq!(
            entry.target,
            "queue orders.dlq on vhost / to each message's original exchange and routing key"
        );
    }

    #[cfg(feature = "amqp")]
    #[tokio::test]
    async fn a_redrive_that_stops_says_how_far_it_got() {
        let (mut tools, _, log) = fresh(FakeRabbit {
            redrive_failure: Some(ferrika_rabbitmq::amqp::RedriveFailure::Unroutable),
            ..broker()
        });
        let args = json!({ "queue": "orders.dlq", "max": 5 });
        let plan = tools.call_token("rabbit_dlq_redrive", &args).await;
        let result = tools
            .call_token("rabbit_dlq_redrive", &execute(&args, &token(&plan)))
            .await;
        assert_eq!(code(&result), "redrive_stopped", "{result}");
        let error = &result["structuredContent"]["error"];
        assert_eq!(error["moved"], 2);
        assert_eq!(error["requeued"], true);
        let entries = log.entries.borrow();
        let entry = entries.last().unwrap();
        assert!(!entry.ok);
        assert!(entry.outcome.contains("then stopped"), "{}", entry.outcome);
        // The broker's own text goes to the client, not the audit log.
        assert!(!entry.outcome.contains("PRECONDITION_FAILED"));
    }

    #[cfg(feature = "amqp")]
    #[tokio::test]
    async fn a_redrive_needs_produce_too_and_a_named_destination_when_scoped() {
        let no_produce = policy(PolicyRules {
            forbid: vec!["produce".into()],
            ..Default::default()
        });
        let (mut tools, rabbit, log) = tools_on(broker(), config(), no_produce);
        let result = tools
            .call_token(
                "rabbit_dlq_redrive",
                &json!({ "queue": "orders.dlq", "max": 5 }),
            )
            .await;
        assert_eq!(code(&result), "policy_refused", "{result}");
        assert!(result.to_string().contains("`produce`"), "{result}");
        assert_eq!(rabbit.peeks.get(), 0);
        assert_eq!(log.entries.borrow()[0].action, "policy.blocked");

        let (mut tools, rabbit, _) =
            tools_on(broker(), scoped(&["orders*"], &["shop"]), no_policy());
        let unnamed = tools
            .call_token(
                "rabbit_dlq_redrive",
                &json!({ "queue": "orders.dlq", "max": 5 }),
            )
            .await;
        assert_eq!(code(&unnamed), "target_not_allowed", "{unnamed}");
        let elsewhere = tools
            .call_token(
                "rabbit_dlq_redrive",
                &json!({ "queue": "orders.dlq", "max": 5, "to_exchange": "test.x" }),
            )
            .await;
        assert_eq!(code(&elsewhere), "target_not_allowed", "{elsewhere}");
        let named = tools
            .call_token(
                "rabbit_dlq_redrive",
                &json!({ "queue": "orders.dlq", "max": 5, "to_exchange": "shop" }),
            )
            .await;
        assert_eq!(named["isError"], false, "{named}");
        assert_eq!(rabbit.peeks.get(), 1);
        let too_many = tools
            .call_token(
                "rabbit_dlq_redrive",
                &json!({ "queue": "orders.dlq", "max": 1001, "to_exchange": "shop" }),
            )
            .await;
        assert_eq!(code(&too_many), "invalid_argument");
    }

    #[test]
    fn every_rabbit_write_tool_has_a_definition_and_a_category() {
        let names: Vec<String> = definitions()
            .iter()
            .filter_map(|d| d["name"].as_str().map(str::to_string))
            .collect();
        let rabbit: Vec<&str> = crate::WRITE_TOOLS
            .into_iter()
            .filter(|t| crate::guard::is_rabbit_tool(t))
            .collect();
        assert_eq!(names, rabbit);
        assert_eq!(
            crate::guard::policy_action("rabbit_peek"),
            PolicyAction::MessagesPeek
        );
        assert_eq!(
            crate::guard::policy_action("rabbit_publish"),
            PolicyAction::Produce
        );
        assert_eq!(
            crate::guard::policy_action("rabbit_dlq_redrive"),
            PolicyAction::MessagesMove
        );
    }
}
