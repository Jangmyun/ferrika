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

    #[test]
    fn non_object_properties_are_refused() {
        let parsed = serde_json::from_str::<PeekedMessage>(r#"{"properties":"x"}"#);
        assert!(parsed.is_err());
    }
}
