//! What the RabbitMQ management API returns and accepts, as typed data. No
//! I/O. Fields the API leaves out (statistics are missing while a queue
//! starts, or when the management agent disables them) default to empty.

use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Value};

use crate::error::{RabbitError, RabbitResult};

/// The name the management API uses for the default exchange (the one with
/// the empty name), in paths such as `/api/exchanges/%2F/amq.default/publish`.
pub const DEFAULT_EXCHANGE: &str = "amq.default";

/// The most messages one peek asks for. Each one is fetched and requeued
/// by the broker, so a large count is slow and reorders more of the queue.
pub const MAX_PEEK: u32 = 500;

/// How many payload bytes the broker returns per peeked message; longer
/// payloads come back cut (see [`PeekedMessage::is_truncated`]).
pub const PEEK_TRUNCATE_BYTES: u64 = 50_000;

/// One queue of a virtual host (`GET /api/queues/{vhost}`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Queue {
    pub name: String,
    pub vhost: String,
    /// `classic`, `quorum` or `stream`.
    #[serde(rename = "type", default)]
    pub kind: String,
    #[serde(default)]
    pub durable: bool,
    #[serde(default)]
    pub auto_delete: bool,
    #[serde(default)]
    pub exclusive: bool,
    /// `running`, `idle`, `flow`, `down`...; absent without statistics.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,
    /// Ready plus unacknowledged messages.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub messages: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub messages_ready: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub messages_unacknowledged: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consumers: Option<u64>,
    /// The `x-*` arguments the queue was declared with.
    #[serde(default, deserialize_with = "object_or_empty")]
    pub arguments: Map<String, Value>,
    /// The name of the policy that applies to the queue.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<String>,
    /// The definition of that policy (`dead-letter-exchange`, ...).
    #[serde(default, deserialize_with = "object_or_empty")]
    pub effective_policy_definition: Map<String, Value>,
}

impl Queue {
    /// Where rejected or expired messages of this queue go: the
    /// `x-dead-letter-exchange` argument, else the `dead-letter-exchange`
    /// of the policy applied to it. `Some("")` is the default exchange.
    pub fn dead_letter_exchange(&self) -> Option<&str> {
        self.arguments
            .get("x-dead-letter-exchange")
            .or_else(|| self.effective_policy_definition.get("dead-letter-exchange"))
            .and_then(Value::as_str)
    }
}

/// One exchange of a virtual host (`GET /api/exchanges/{vhost}`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Exchange {
    /// Empty for the default exchange.
    pub name: String,
    pub vhost: String,
    /// `direct`, `fanout`, `topic`, `headers` or a plugin's type.
    #[serde(rename = "type", default)]
    pub kind: String,
    #[serde(default)]
    pub durable: bool,
    #[serde(default)]
    pub auto_delete: bool,
    /// An internal exchange cannot be published to by clients.
    #[serde(default)]
    pub internal: bool,
    #[serde(default, deserialize_with = "object_or_empty")]
    pub arguments: Map<String, Value>,
}

impl Exchange {
    /// The name to show: the default exchange has none.
    pub fn display_name(&self) -> &str {
        if self.name.is_empty() {
            DEFAULT_EXCHANGE
        } else {
            &self.name
        }
    }
}

/// One binding of a virtual host (`GET /api/bindings/{vhost}`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Binding {
    /// The exchange; empty for the default exchange's implicit bindings.
    pub source: String,
    pub vhost: String,
    pub destination: String,
    /// `queue` or `exchange`.
    pub destination_type: String,
    #[serde(default)]
    pub routing_key: String,
    #[serde(default, deserialize_with = "object_or_empty")]
    pub arguments: Map<String, Value>,
    /// The broker's name for the binding among those between the same
    /// source and destination (routing key and a hash of the arguments),
    /// used to delete it.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub properties_key: String,
}

/// The longest queue or exchange name RabbitMQ accepts, in bytes.
pub const MAX_NAME_BYTES: usize = 255;

/// Checks a queue or exchange name (`what`: "queue" or "exchange") before
/// it is declared or deleted: not empty, at most [`MAX_NAME_BYTES`], and not
/// in the broker's reserved `amq.` namespace (the default exchange and the
/// `amq.*` exchanges cannot be declared or deleted by clients).
pub fn validate_name(what: &str, name: &str) -> RabbitResult<()> {
    if name.is_empty() {
        return Err(RabbitError::config(format!("The {what} name is empty")));
    }
    if name.len() > MAX_NAME_BYTES {
        return Err(RabbitError::config(format!(
            "The {what} name is longer than {MAX_NAME_BYTES} bytes"
        )));
    }
    if name.starts_with("amq.") {
        return Err(RabbitError::config(format!(
            "'{name}' is in the reserved amq. namespace; RabbitMQ does not let clients declare \
             or delete such a {what}"
        )));
    }
    Ok(())
}

/// One `KEY=VALUE` argument of a queue, exchange or binding declaration
/// (`x-max-length=1000`, `x-match=all`) as the JSON the management API
/// takes: a whole number and `true`/`false` as a number and a boolean, a
/// value starting with `{`, `[` or `"` as JSON, anything else as text
/// (`x-code=42` is the number 42; `x-code="42"` the text). The key is
/// trimmed; the value is taken as written.
pub fn parse_argument(item: &str) -> RabbitResult<(String, Value)> {
    let (key, raw) = item
        .split_once('=')
        .ok_or_else(|| RabbitError::config(format!("Argument '{item}' is not KEY=VALUE")))?;
    let key = key.trim();
    if key.is_empty() {
        return Err(RabbitError::config(format!(
            "Argument '{item}' has no name"
        )));
    }
    let value = if let Ok(number) = raw.parse::<i64>() {
        Value::from(number)
    } else if raw == "true" || raw == "false" {
        Value::from(raw == "true")
    } else if raw.starts_with(['{', '[', '"']) {
        serde_json::from_str(raw)
            .map_err(|e| RabbitError::config(format!("Argument '{key}' is not valid JSON: {e}")))?
    } else {
        Value::from(raw)
    };
    Ok((key.to_string(), value))
}

/// [`parse_argument`] for each of `items`, as the `arguments` object of a
/// declaration or binding; a later key replaces an earlier one.
pub fn parse_arguments<S: AsRef<str>>(items: &[S]) -> RabbitResult<Map<String, Value>> {
    let mut out = Map::new();
    for item in items {
        let (key, value) = parse_argument(item.as_ref())?;
        out.insert(key, value);
    }
    Ok(out)
}

/// What a binding delivers to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DestinationType {
    Queue,
    Exchange,
}

impl DestinationType {
    /// `queue` or `exchange`, as the API's `destination_type` says.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queue => "queue",
            Self::Exchange => "exchange",
        }
    }

    /// From the API's `destination_type`.
    pub fn parse(text: &str) -> Option<Self> {
        match text {
            "queue" => Some(Self::Queue),
            "exchange" => Some(Self::Exchange),
            _ => None,
        }
    }

    /// `q` or `e`, as binding paths name the destination's kind.
    pub(crate) fn path_letter(self) -> &'static str {
        match self {
            Self::Queue => "q",
            Self::Exchange => "e",
        }
    }
}

/// The body of `PUT /api/queues/{vhost}/{name}`: how to declare a queue.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct QueueDeclaration {
    /// `classic`, `quorum` or `stream` (sent as `x-queue-type`); `None`
    /// leaves it to the vhost's default queue type.
    pub kind: Option<String>,
    pub durable: bool,
    pub auto_delete: bool,
    /// `x-*` arguments, e.g. `x-dead-letter-exchange`, `x-max-length`.
    pub arguments: Map<String, Value>,
}

impl QueueDeclaration {
    /// How `queue` was declared, to declare it again: its type is named
    /// explicitly, so a vhost whose default type changed since does not
    /// make it another kind of queue.
    pub fn of(queue: &Queue) -> Self {
        let kind = Some(queue.kind.clone()).filter(|k| !k.is_empty());
        Self {
            kind,
            durable: queue.durable,
            auto_delete: queue.auto_delete,
            arguments: queue.arguments.clone(),
        }
    }

    pub fn to_request(&self) -> Value {
        let mut arguments = self.arguments.clone();
        if let Some(kind) = &self.kind {
            arguments.insert("x-queue-type".to_string(), Value::from(kind.clone()));
        }
        serde_json::json!({
            "durable": self.durable,
            "auto_delete": self.auto_delete,
            "arguments": arguments,
        })
    }
}

/// The body of `PUT /api/exchanges/{vhost}/{name}`: how to declare an
/// exchange.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExchangeDeclaration {
    /// `direct`, `fanout`, `topic`, `headers` or a plugin's type.
    pub kind: String,
    pub durable: bool,
    pub auto_delete: bool,
    /// An internal exchange takes messages only from other exchanges.
    pub internal: bool,
    /// e.g. `alternate-exchange`.
    pub arguments: Map<String, Value>,
}

impl ExchangeDeclaration {
    /// How `exchange` was declared, to declare it again.
    pub fn of(exchange: &Exchange) -> Self {
        Self {
            kind: exchange.kind.clone(),
            durable: exchange.durable,
            auto_delete: exchange.auto_delete,
            internal: exchange.internal,
            arguments: exchange.arguments.clone(),
        }
    }

    pub fn to_request(&self) -> Value {
        serde_json::json!({
            "type": self.kind,
            "durable": self.durable,
            "auto_delete": self.auto_delete,
            "internal": self.internal,
            "arguments": self.arguments,
        })
    }
}

/// A binding to add or remove: from exchange `source` to a queue or an
/// exchange, with a routing key and arguments (the `headers` exchange
/// matches on them).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BindingDeclaration {
    pub source: String,
    pub destination: String,
    pub destination_type: DestinationType,
    pub routing_key: String,
    pub arguments: Map<String, Value>,
}

impl BindingDeclaration {
    /// `binding` as the API listed it; `None` for the default exchange's
    /// implicit bindings, which cannot be added or removed.
    pub fn of(binding: &Binding) -> Option<Self> {
        if binding.source.is_empty() {
            return None;
        }
        Some(Self {
            source: binding.source.clone(),
            destination: binding.destination.clone(),
            destination_type: DestinationType::parse(&binding.destination_type)?,
            routing_key: binding.routing_key.clone(),
            arguments: binding.arguments.clone(),
        })
    }

    /// A binding from user input: `KEY=VALUE` arguments
    /// ([`parse_arguments`]), checked with [`Self::validate`].
    pub fn parse<S: AsRef<str>>(
        source: &str,
        destination: &str,
        destination_type: DestinationType,
        routing_key: &str,
        arguments: &[S],
    ) -> RabbitResult<Self> {
        let binding = Self {
            source: source.to_string(),
            destination: destination.to_string(),
            destination_type,
            routing_key: routing_key.to_string(),
            arguments: parse_arguments(arguments)?,
        };
        binding.validate()?;
        Ok(binding)
    }

    /// Checks the names: the default exchange has only implicit bindings.
    pub fn validate(&self) -> RabbitResult<()> {
        if self.source.is_empty() || self.source == DEFAULT_EXCHANGE {
            return Err(RabbitError::config(
                "The default exchange (amq.default) cannot be bound or unbound: it routes to \
                 every queue by name",
            ));
        }
        if self.destination.is_empty() {
            return Err(RabbitError::config("The binding destination is empty"));
        }
        Ok(())
    }

    /// Whether `binding` (as listed) is this one: same ends, routing key
    /// and arguments.
    pub fn matches(&self, binding: &Binding) -> bool {
        binding.source == self.source
            && binding.destination == self.destination
            && binding.destination_type == self.destination_type.as_str()
            && binding.routing_key == self.routing_key
            && binding.arguments == self.arguments
    }

    /// The body of `POST /api/bindings/{vhost}/e/{source}/{q|e}/{destination}`.
    pub fn to_request(&self) -> Value {
        serde_json::json!({
            "routing_key": self.routing_key,
            "arguments": self.arguments,
        })
    }

    /// "exchange orders -> queue billing, routing key 'order.*'".
    pub fn label(&self) -> String {
        format!(
            "exchange {} -> {} {}, routing key '{}'",
            self.source,
            self.destination_type.as_str(),
            self.destination,
            self.routing_key
        )
    }
}

/// One message read with requeueing (`POST /api/queues/{vhost}/{name}/get`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PeekedMessage {
    /// The exchange it was published to (empty: the default exchange).
    #[serde(default)]
    pub exchange: String,
    #[serde(default)]
    pub routing_key: String,
    /// Set by the broker once a message was delivered before, which a peek
    /// itself does: it requeues what it read.
    #[serde(default)]
    pub redelivered: bool,
    /// Messages left in the queue after this one was taken.
    #[serde(default)]
    pub message_count: u64,
    /// The whole payload's size; `payload` may be cut at
    /// [`PEEK_TRUNCATE_BYTES`].
    #[serde(rename = "payload_bytes", default)]
    pub size: u64,
    /// The payload as text (`payload_encoding: string`) or Base64.
    #[serde(default)]
    pub payload: String,
    #[serde(default)]
    pub payload_encoding: String,
    /// AMQP basic properties: `headers`, `content_type`, `delivery_mode`,
    /// `message_id`... (the API sends `[]` when there are none).
    #[serde(default, deserialize_with = "object_or_empty")]
    pub properties: Map<String, Value>,
}

impl PeekedMessage {
    /// The payload bytes the broker returned (Base64 decoded).
    pub fn body(&self) -> RabbitResult<Vec<u8>> {
        if self.payload_encoding.eq_ignore_ascii_case("base64") {
            ferrika_core::codec::base64::decode(&self.payload)
                .map_err(|e| RabbitError::Response(format!("payload is not valid Base64: {e}")))
        } else {
            Ok(self.payload.clone().into_bytes())
        }
    }

    /// Whether the broker returned only the start of the payload.
    pub fn is_truncated(&self) -> bool {
        self.body()
            .map(|body| (body.len() as u64) < self.size)
            .unwrap_or(false)
    }

    /// The `headers` property: name and value (a string, number, list or
    /// table such as `x-death`).
    pub fn headers(&self) -> Vec<(&str, &Value)> {
        match self.properties.get("headers") {
            Some(Value::Object(headers)) => headers.iter().map(|(k, v)| (k.as_str(), v)).collect(),
            _ => Vec::new(),
        }
    }
}

/// One entry of a dead-lettered message's `x-death` header: where the
/// message was dead-lettered from and why. The broker keeps one entry per
/// (queue, reason), newest first, and counts repeats in `count`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeathRecord {
    /// The queue the message was dead-lettered from.
    pub queue: String,
    /// `rejected`, `expired`, `maxlen` or `delivery_limit`.
    pub reason: String,
    /// How many times it was dead-lettered from `queue` for `reason`.
    pub count: u64,
    /// The exchange the message had been published to (empty: the default
    /// exchange).
    pub exchange: String,
    /// The routing keys it had been published with (CC/BCC keys included).
    pub routing_keys: Vec<String>,
}

impl DeathRecord {
    /// The `x-death` entries of a peeked message's headers, newest first.
    /// Entries that are not tables are skipped; missing fields are empty.
    pub fn from_headers(headers: &[(&str, &Value)]) -> Vec<Self> {
        let Some((_, Value::Array(entries))) = headers.iter().find(|(name, _)| *name == "x-death")
        else {
            return Vec::new();
        };
        entries
            .iter()
            .filter_map(Value::as_object)
            .map(|entry| {
                let text = |name: &str| {
                    entry
                        .get(name)
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string()
                };
                Self {
                    queue: text("queue"),
                    reason: text("reason"),
                    count: entry.get("count").and_then(Value::as_u64).unwrap_or(0),
                    exchange: text("exchange"),
                    routing_keys: entry
                        .get("routing-keys")
                        .and_then(Value::as_array)
                        .map(|keys| {
                            keys.iter()
                                .filter_map(Value::as_str)
                                .map(str::to_string)
                                .collect()
                        })
                        .unwrap_or_default(),
                }
            })
            .collect()
    }

    /// The routing key a redrive republishes with: the first one the
    /// message was published with (empty when the broker recorded none).
    pub fn routing_key(&self) -> &str {
        self.routing_keys.first().map(String::as_str).unwrap_or("")
    }
}

/// Where a redrive sends a dead-lettered message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RedriveDestination {
    /// Empty for the default exchange.
    pub exchange: String,
    pub routing_key: String,
}

impl RedriveDestination {
    /// The destination for a message whose newest `x-death` entry is
    /// `death`: its original exchange and routing key, each replaced by
    /// `to_exchange` / `routing_key` when given. A message without
    /// `x-death` needs `to_exchange` (its routing key is then
    /// `routing_key`, or empty).
    pub fn resolve(
        death: Option<&DeathRecord>,
        to_exchange: Option<&str>,
        routing_key: Option<&str>,
    ) -> RabbitResult<Self> {
        let exchange = match (to_exchange, death) {
            (Some(exchange), _) => exchange.to_string(),
            (None, Some(death)) => death.exchange.clone(),
            (None, None) => {
                return Err(RabbitError::config(
                    "The message has no x-death header, so its original exchange is unknown; \
                     name one with --to-exchange",
                ))
            }
        };
        let exchange = if exchange == DEFAULT_EXCHANGE {
            String::new()
        } else {
            exchange
        };
        let routing_key = match (routing_key, death) {
            (Some(key), _) => key.to_string(),
            (None, Some(death)) => death.routing_key().to_string(),
            (None, None) => String::new(),
        };
        Ok(Self {
            exchange,
            routing_key,
        })
    }

    /// The exchange as people read it: `amq.default` for the default one.
    pub fn exchange_name(&self) -> &str {
        if self.exchange.is_empty() {
            DEFAULT_EXCHANGE
        } else {
            &self.exchange
        }
    }
}

/// `exchange 'X', routing key 'K'`, as reports and the audit log name a
/// destination.
pub fn destination_label(destination: &RedriveDestination) -> String {
    format!(
        "exchange '{}', routing key '{}'",
        destination.exchange_name(),
        destination.routing_key
    )
}

/// An exchange name as people read it: `amq.default` for the default
/// exchange (the empty name).
pub fn exchange_name(exchange: &str) -> &str {
    if exchange.is_empty() {
        DEFAULT_EXCHANGE
    } else {
        exchange
    }
}

/// The body of a peek: take `count` messages and put them back
/// (`ack_requeue_true`), payloads as text when they are UTF-8, else Base64.
pub fn peek_request(count: u32) -> RabbitResult<Value> {
    if count == 0 || count > MAX_PEEK {
        return Err(RabbitError::config(format!(
            "The message count must be between 1 and {MAX_PEEK}, not {count}"
        )));
    }
    Ok(serde_json::json!({
        "count": count,
        "ackmode": "ack_requeue_true",
        "encoding": "auto",
        "truncate": PEEK_TRUNCATE_BYTES,
    }))
}

/// A message to publish to an exchange.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct OutgoingMessage {
    pub routing_key: String,
    /// UTF-8 text; sent with `payload_encoding: string`.
    pub payload: String,
    pub content_type: Option<String>,
    /// String headers, in order.
    pub headers: Vec<(String, String)>,
    /// `delivery_mode` 2 (stored on disk by durable queues) when set, else 1.
    pub persistent: bool,
}

impl OutgoingMessage {
    /// The body of `POST /api/exchanges/{vhost}/{name}/publish`.
    pub fn to_request(&self) -> Value {
        let mut properties = Map::new();
        properties.insert(
            "delivery_mode".to_string(),
            Value::from(if self.persistent { 2 } else { 1 }),
        );
        if let Some(content_type) = &self.content_type {
            properties.insert(
                "content_type".to_string(),
                Value::from(content_type.clone()),
            );
        }
        if !self.headers.is_empty() {
            let headers: Map<String, Value> = self
                .headers
                .iter()
                .map(|(k, v)| (k.clone(), Value::from(v.clone())))
                .collect();
            properties.insert("headers".to_string(), Value::Object(headers));
        }
        serde_json::json!({
            "properties": properties,
            "routing_key": self.routing_key,
            "payload": self.payload,
            "payload_encoding": "string",
        })
    }
}

/// The answer to a publish.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublishOutcome {
    /// Whether at least one queue received the message. `false` means the
    /// exchange had no matching binding and the message was dropped (or went
    /// to the exchange's alternate exchange).
    pub routed: bool,
}

/// An object, or the empty list the management API sends for "no
/// properties" / "no arguments" in some versions.
fn object_or_empty<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Map<String, Value>, D::Error> {
    match Value::deserialize(deserializer)? {
        Value::Object(map) => Ok(map),
        Value::Null => Ok(Map::new()),
        Value::Array(items) if items.is_empty() => Ok(Map::new()),
        other => Err(serde::de::Error::custom(format!(
            "expected an object, got {other}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn arguments_parse_as_the_json_the_api_takes() {
        let parsed = super::parse_arguments(&[
            "x-max-length=1000",
            " x-dead-letter-exchange =dlx",
            "x-single-active-consumer=true",
            r#"x-labels=["a","b"]"#,
            r#"x-code="42""#,
            "x-empty=",
            "x-text=a=b",
        ])
        .unwrap();
        assert_eq!(parsed["x-max-length"], 1000);
        assert_eq!(parsed["x-dead-letter-exchange"], "dlx");
        assert_eq!(parsed["x-single-active-consumer"], true);
        assert_eq!(parsed["x-labels"], serde_json::json!(["a", "b"]));
        assert_eq!(parsed["x-code"], "42");
        assert_eq!(parsed["x-empty"], "");
        assert_eq!(parsed["x-text"], "a=b");
        for bad in ["novalue", "=x", " =x", "x={bad"] {
            let err = super::parse_argument(bad).unwrap_err();
            assert!(matches!(err, super::RabbitError::Config(_)), "{bad}: {err}");
        }
        let empty: [&str; 0] = [];
        assert!(super::parse_arguments(&empty).unwrap().is_empty());
    }

    #[test]
    fn bindings_parse_from_user_input_and_are_checked() {
        use super::{BindingDeclaration, DestinationType};
        let binding = BindingDeclaration::parse(
            "shop",
            "audit",
            DestinationType::Exchange,
            "",
            &["x-match=all".to_string()],
        )
        .unwrap();
        assert_eq!(binding.destination_type, DestinationType::Exchange);
        assert_eq!(binding.arguments["x-match"], "all");
        let none: [&str; 0] = [];
        assert!(
            BindingDeclaration::parse("amq.default", "q", DestinationType::Queue, "", &none)
                .is_err()
        );
        assert!(BindingDeclaration::parse("shop", "", DestinationType::Queue, "", &none).is_err());
        assert!(
            BindingDeclaration::parse("shop", "q", DestinationType::Queue, "", &["bad"]).is_err()
        );
    }

    use super::*;

    #[test]
    fn queues_parse_with_and_without_statistics() {
        let body = r#"[
          {"name":"orders","vhost":"/","type":"quorum","durable":true,"auto_delete":false,
           "exclusive":false,"state":"running","messages":12,"messages_ready":10,
           "messages_unacknowledged":2,"consumers":1,
           "arguments":{"x-queue-type":"quorum","x-dead-letter-exchange":"dlx"},
           "policy":null,"effective_policy_definition":[]},
          {"name":"fresh","vhost":"/","durable":false,"auto_delete":true,"arguments":{}}
        ]"#;
        let queues: Vec<Queue> = serde_json::from_str(body).unwrap();
        assert_eq!(queues[0].kind, "quorum");
        assert_eq!(queues[0].messages, Some(12));
        assert_eq!(queues[0].dead_letter_exchange(), Some("dlx"));
        assert_eq!(queues[1].messages, None);
        assert_eq!(queues[1].state, None);
        assert_eq!(queues[1].dead_letter_exchange(), None);
    }

    #[test]
    fn a_policy_can_set_the_dead_letter_exchange() {
        let queue: Queue = serde_json::from_str(
            r#"{"name":"q","vhost":"/","arguments":{},"policy":"dlx",
                "effective_policy_definition":{"dead-letter-exchange":"orders.dlx"}}"#,
        )
        .unwrap();
        assert_eq!(queue.dead_letter_exchange(), Some("orders.dlx"));
    }

    #[test]
    fn exchanges_and_bindings_parse() {
        let exchanges: Vec<Exchange> = serde_json::from_str(
            r#"[{"name":"","vhost":"/","type":"direct","durable":true,"auto_delete":false,
                 "internal":false,"arguments":{}},
                {"name":"orders","vhost":"/","type":"topic","durable":true,"auto_delete":false,
                 "internal":false,"arguments":{}}]"#,
        )
        .unwrap();
        assert_eq!(exchanges[0].display_name(), DEFAULT_EXCHANGE);
        assert_eq!(exchanges[1].display_name(), "orders");
        assert_eq!(exchanges[1].kind, "topic");

        let bindings: Vec<Binding> = serde_json::from_str(
            r#"[{"source":"orders","vhost":"/","destination":"billing","destination_type":"queue",
                 "routing_key":"order.*","arguments":{},"properties_key":"order.*"}]"#,
        )
        .unwrap();
        assert_eq!(bindings[0].destination, "billing");
        assert_eq!(bindings[0].routing_key, "order.*");
    }

    #[test]
    fn peeked_messages_decode_text_and_base64_payloads() {
        let messages: Vec<PeekedMessage> = serde_json::from_str(
            r#"[{"payload_bytes":5,"redelivered":true,"exchange":"orders","routing_key":"o.1",
                 "message_count":3,"properties":{"headers":{"x-trace":"abc"},"delivery_mode":2},
                 "payload":"hello","payload_encoding":"string"},
                {"payload_bytes":3,"redelivered":false,"exchange":"","routing_key":"q",
                 "message_count":2,"properties":[],"payload":"AP8B","payload_encoding":"base64"}]"#,
        )
        .unwrap();
        assert_eq!(messages[0].body().unwrap(), b"hello");
        assert!(messages[0].redelivered);
        assert_eq!(messages[0].headers().len(), 1);
        assert_eq!(messages[1].body().unwrap(), vec![0x00, 0xFF, 0x01]);
        assert!(messages[1].properties.is_empty());
        assert!(messages[1].headers().is_empty());
        assert!(!messages[0].is_truncated());
    }

    #[test]
    fn a_cut_payload_is_reported_as_truncated() {
        let message: PeekedMessage = serde_json::from_str(
            r#"{"payload_bytes":100000,"payload":"abc","payload_encoding":"string","properties":{}}"#,
        )
        .unwrap();
        assert!(message.is_truncated());
    }

    #[test]
    fn peeks_requeue_and_stay_bounded() {
        let body = peek_request(5).unwrap();
        assert_eq!(body["ackmode"], "ack_requeue_true");
        assert_eq!(body["count"], 5);
        assert_eq!(body["encoding"], "auto");
        assert!(peek_request(0).is_err());
        assert!(peek_request(MAX_PEEK + 1).is_err());
        assert!(peek_request(MAX_PEEK).is_ok());
    }

    #[test]
    fn publish_requests_carry_key_payload_and_properties() {
        let message = OutgoingMessage {
            routing_key: "order.created".to_string(),
            payload: r#"{"id":1}"#.to_string(),
            content_type: Some("application/json".to_string()),
            headers: vec![("source".to_string(), "ferrika".to_string())],
            persistent: true,
        };
        let body = message.to_request();
        assert_eq!(body["routing_key"], "order.created");
        assert_eq!(body["payload"], r#"{"id":1}"#);
        assert_eq!(body["payload_encoding"], "string");
        assert_eq!(body["properties"]["delivery_mode"], 2);
        assert_eq!(body["properties"]["content_type"], "application/json");
        assert_eq!(body["properties"]["headers"]["source"], "ferrika");

        let plain = OutgoingMessage::default().to_request();
        assert_eq!(plain["properties"]["delivery_mode"], 1);
        assert!(plain["properties"].get("headers").is_none());
    }

    fn dead_lettered() -> PeekedMessage {
        serde_json::from_str(
            r#"{"payload":"x","payload_encoding":"string","properties":{"headers":{
                "x-death":[
                  {"count":3,"reason":"rejected","queue":"orders","time":1700000000,
                   "exchange":"shop","routing-keys":["order.created","cc.key"]},
                  {"count":1,"reason":"expired","queue":"orders.wait","exchange":"",
                   "routing-keys":[]},
                  "not a table"],
                "x-first-death-queue":"orders"}}}"#,
        )
        .unwrap()
    }

    #[test]
    fn x_death_entries_are_read_newest_first() {
        let message = dead_lettered();
        let deaths = DeathRecord::from_headers(&message.headers());
        assert_eq!(deaths.len(), 2);
        assert_eq!(
            deaths[0],
            DeathRecord {
                queue: "orders".to_string(),
                reason: "rejected".to_string(),
                count: 3,
                exchange: "shop".to_string(),
                routing_keys: vec!["order.created".to_string(), "cc.key".to_string()],
            }
        );
        assert_eq!(deaths[0].routing_key(), "order.created");
        assert_eq!(deaths[1].routing_key(), "");
        let plain: PeekedMessage =
            serde_json::from_str(r#"{"properties":{"headers":{"a":"b"}}}"#).unwrap();
        assert!(DeathRecord::from_headers(&plain.headers()).is_empty());
    }

    #[test]
    fn a_redrive_goes_back_to_the_original_exchange_unless_overridden() {
        let message = dead_lettered();
        let deaths = DeathRecord::from_headers(&message.headers());
        let original = RedriveDestination::resolve(deaths.first(), None, None).unwrap();
        assert_eq!(original.exchange, "shop");
        assert_eq!(original.routing_key, "order.created");

        let moved =
            RedriveDestination::resolve(deaths.first(), Some("retry"), Some("again")).unwrap();
        assert_eq!(
            (moved.exchange.as_str(), moved.routing_key.as_str()),
            ("retry", "again")
        );
        let key_kept = RedriveDestination::resolve(deaths.first(), Some("retry"), None).unwrap();
        assert_eq!(key_kept.routing_key, "order.created");

        // amq.default is the default exchange, published to by its empty name.
        let default =
            RedriveDestination::resolve(None, Some(DEFAULT_EXCHANGE), Some("orders")).unwrap();
        assert_eq!(default.exchange, "");
        assert_eq!(default.exchange_name(), DEFAULT_EXCHANGE);

        let unknown = RedriveDestination::resolve(None, None, Some("k")).unwrap_err();
        assert!(unknown.to_string().contains("--to-exchange"), "{unknown}");
    }

    #[test]
    fn declarations_name_the_queue_type_and_keep_arguments() {
        let queue: Queue = serde_json::from_str(
            r#"{"name":"orders","vhost":"/","type":"quorum","durable":true,"auto_delete":false,
                "arguments":{"x-queue-type":"quorum","x-max-length":10}}"#,
        )
        .unwrap();
        let declaration = QueueDeclaration::of(&queue);
        let body = declaration.to_request();
        assert_eq!(body["durable"], true);
        assert_eq!(body["auto_delete"], false);
        assert_eq!(body["arguments"]["x-queue-type"], "quorum");
        assert_eq!(body["arguments"]["x-max-length"], 10);
        // Without a type the vhost's default applies.
        let plain = QueueDeclaration {
            durable: true,
            ..QueueDeclaration::default()
        }
        .to_request();
        assert!(plain["arguments"].get("x-queue-type").is_none());

        let exchange: Exchange = serde_json::from_str(
            r#"{"name":"shop","vhost":"/","type":"topic","durable":true,"internal":true,
                "arguments":{"alternate-exchange":"ae"}}"#,
        )
        .unwrap();
        let body = ExchangeDeclaration::of(&exchange).to_request();
        assert_eq!(body["type"], "topic");
        assert_eq!(body["internal"], true);
        assert_eq!(body["arguments"]["alternate-exchange"], "ae");
    }

    #[test]
    fn bindings_match_by_ends_key_and_arguments() {
        let listed: Binding = serde_json::from_str(
            r#"{"source":"shop","vhost":"/","destination":"billing","destination_type":"queue",
                "routing_key":"order.*","arguments":{},"properties_key":"order.*"}"#,
        )
        .unwrap();
        assert_eq!(listed.properties_key, "order.*");
        let binding = BindingDeclaration::of(&listed).unwrap();
        assert!(binding.matches(&listed));
        assert_eq!(
            binding.label(),
            "exchange shop -> queue billing, routing key 'order.*'"
        );
        let other_key = BindingDeclaration {
            routing_key: "order.#".into(),
            ..binding.clone()
        };
        assert!(!other_key.matches(&listed));
        let to_exchange = BindingDeclaration {
            destination_type: DestinationType::Exchange,
            ..binding.clone()
        };
        assert!(!to_exchange.matches(&listed));
        assert_eq!(binding.to_request()["routing_key"], "order.*");
        // The default exchange's implicit bindings are not bindings to manage.
        let implicit = Binding {
            source: String::new(),
            ..listed
        };
        assert!(BindingDeclaration::of(&implicit).is_none());
        let default = BindingDeclaration {
            source: DEFAULT_EXCHANGE.into(),
            ..binding
        };
        assert!(default.validate().is_err());
    }

    #[test]
    fn names_are_checked_before_declaring() {
        assert!(validate_name("queue", "orders").is_ok());
        assert!(validate_name("queue", "").is_err());
        assert!(validate_name("queue", &"x".repeat(MAX_NAME_BYTES + 1)).is_err());
        let reserved = validate_name("exchange", "amq.topic").unwrap_err();
        assert!(reserved.to_string().contains("reserved"), "{reserved}");
        assert_eq!(
            DestinationType::parse("queue"),
            Some(DestinationType::Queue)
        );
        assert_eq!(DestinationType::parse("stream"), None);
    }

    #[test]
    fn non_object_properties_are_refused() {
        let parsed = serde_json::from_str::<PeekedMessage>(r#"{"properties":"x"}"#);
        assert!(parsed.is_err());
    }
}
