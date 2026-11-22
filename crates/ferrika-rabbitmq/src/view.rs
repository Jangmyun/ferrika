//! Messages as a front end shows them: peeked messages with PII masking,
//! the dead-letter inspection grouped by `x-death` origin, the redrive
//! preview, the audit text of a redrive, and `KEY:VALUE` header parsing.
//! No I/O; every front end (the CLI's `ferrika rabbit`, a GUI) uses these so
//! they mask and summarise messages the same way.
//!
//! ```
//! use ferrika_rabbitmq::model::PeekedMessage;
//! use ferrika_rabbitmq::view::ShownMessage;
//!
//! let message: PeekedMessage = serde_json::from_str(
//!     r#"{"payload":"{\"email\":\"kim@example.com\"}","payload_encoding":"string","properties":{}}"#,
//! )
//! .unwrap();
//! let shown = ShownMessage::new(&message, true);
//! assert!(!shown.payload.unwrap().contains("kim@example.com"));
//! ```

use std::collections::BTreeMap;

use serde::Serialize;
use serde_json::{Map, Value};

use ferrika_core::codec::pii::{ellipsize, mask_bytes, mask_header_value, MaskStrategy};

use crate::model::{
    destination_label, exchange_name, DeathRecord, PeekedMessage, RedriveDestination,
};

/// The problem a redrive preview names for a message without `x-death`
/// when no destination exchange was given (see
/// [`RedrivePreview::with_unmovable_problem`]).
pub const NO_DEATH_PROBLEM: &str = "no x-death header; give a destination exchange";

/// One peeked message as it is shown: masked when asked, the payload as
/// text when it is UTF-8 (else `payload_base64`, never under masking).
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ShownMessage {
    pub exchange: String,
    pub routing_key: String,
    pub redelivered: bool,
    /// The whole payload's size in bytes.
    pub size: u64,
    /// Whether the broker returned only the start of the payload.
    pub truncated: bool,
    pub payload: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload_base64: Option<String>,
    pub properties: Map<String, Value>,
}

impl ShownMessage {
    /// `message` as shown; with `mask`, the payload and every text property
    /// are PII-masked (partial).
    pub fn new(message: &PeekedMessage, mask: bool) -> Self {
        let body = message.body();
        let (payload, payload_base64) = match &body {
            Ok(bytes) if mask => (Some(mask_bytes(bytes, MaskStrategy::Partial)), None),
            Ok(bytes) => match std::str::from_utf8(bytes) {
                Ok(text) => (Some(text.to_string()), None),
                Err(_) => (None, Some(ferrika_core::codec::base64::encode(bytes))),
            },
            Err(e) => (Some(format!("[{e}]")), None),
        };
        let properties = if mask {
            mask_properties(&message.properties)
        } else {
            message.properties.clone()
        };
        Self {
            exchange: message.exchange.clone(),
            routing_key: message.routing_key.clone(),
            redelivered: message.redelivered,
            size: message.size,
            truncated: message.is_truncated(),
            payload,
            payload_base64,
            properties,
        }
    }

    /// The payload as one line of at most `max_chars` characters (binary
    /// payloads as `base64:...`).
    pub fn payload_preview(&self, max_chars: usize) -> String {
        let payload = self
            .payload
            .clone()
            .or_else(|| self.payload_base64.as_ref().map(|b| format!("base64:{b}")))
            .unwrap_or_default();
        ellipsize(&payload, max_chars)
    }
}

/// AMQP properties with every text value masked by its name, the way
/// record headers are (`mask_header_value`); headers one by one, by
/// header name. Numbers and flags (`delivery_mode`, `priority`) stay.
pub fn mask_properties(properties: &Map<String, Value>) -> Map<String, Value> {
    let mask_value = |name: &str, value: &Value| -> Value {
        match value {
            Value::String(text) => Value::String(mask_header_value(
                name,
                text.as_bytes(),
                MaskStrategy::Partial,
            )),
            Value::Number(_) | Value::Bool(_) | Value::Null => value.clone(),
            // Lists and tables (`x-death`) are masked as JSON text.
            other => Value::String(mask_header_value(
                name,
                other.to_string().as_bytes(),
                MaskStrategy::Partial,
            )),
        }
    };
    properties
        .iter()
        .map(|(name, value)| {
            let shown = match (name.as_str(), value) {
                ("headers", Value::Object(headers)) => Value::Object(
                    headers
                        .iter()
                        .map(|(k, v)| (k.clone(), mask_value(k, v)))
                        .collect(),
                ),
                _ => mask_value(name, value),
            };
            (name.clone(), shown)
        })
        .collect()
}

/// A dead-letter inspection: the messages with their `x-death` history,
/// and how many came from each queue for each reason.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Inspection {
    pub groups: Vec<DeathGroup>,
    pub messages: Vec<InspectedMessage>,
}

/// Messages that died in the same queue for the same reason, published to
/// the same exchange with the same routing key (from the newest `x-death`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DeathGroup {
    pub queue: String,
    pub reason: String,
    pub exchange: String,
    pub routing_key: String,
    pub messages: u64,
}

/// One inspected message.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct InspectedMessage {
    /// The `x-death` entries, newest first (queue, reason, count, exchange
    /// and routing keys: names, not content).
    pub deaths: Vec<DeathRecord>,
    #[serde(flatten)]
    pub message: ShownMessage,
}

impl Inspection {
    /// Groups `messages` by their newest `x-death` entry (messages without
    /// one form the group with reason `no x-death`); with `mask`, the
    /// messages are PII-masked.
    pub fn new(messages: &[PeekedMessage], mask: bool) -> Self {
        let messages: Vec<InspectedMessage> = messages
            .iter()
            .map(|m| InspectedMessage {
                deaths: DeathRecord::from_headers(&m.headers()),
                message: ShownMessage::new(m, mask),
            })
            .collect();
        let mut groups: BTreeMap<(String, String, String, String), u64> = BTreeMap::new();
        for message in &messages {
            let key = match message.deaths.first() {
                Some(d) => (
                    d.queue.clone(),
                    d.reason.clone(),
                    exchange_name(&d.exchange).to_string(),
                    d.routing_key().to_string(),
                ),
                None => (
                    String::new(),
                    "no x-death".to_string(),
                    String::new(),
                    String::new(),
                ),
            };
            *groups.entry(key).or_default() += 1;
        }
        Self {
            groups: groups
                .into_iter()
                .map(
                    |((queue, reason, exchange, routing_key), messages)| DeathGroup {
                        queue,
                        reason,
                        exchange,
                        routing_key,
                        messages,
                    },
                )
                .collect(),
            messages,
        }
    }
}

/// What a redrive would do, before it runs: where each read message would
/// go. No message bodies.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RedrivePreview {
    pub dry_run: bool,
    pub queue: String,
    pub vhost: String,
    pub messages: Vec<PlannedMove>,
    /// Messages per destination (as [`destination_label`] names it).
    pub destinations: BTreeMap<String, u64>,
    /// Messages that could not be moved (no x-death and no destination
    /// exchange given).
    pub unmovable: u64,
}

/// Where one message would go.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PlannedMove {
    pub from_queue: String,
    pub reason: String,
    pub deaths: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub destination: Option<RedriveDestination>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub problem: Option<String>,
}

impl RedrivePreview {
    /// The plan for moving `messages` out of `queue` on `vhost`, to each
    /// message's original exchange and routing key or to the overrides.
    /// Unmovable messages name [`NO_DEATH_PROBLEM`].
    pub fn new(
        queue: &str,
        vhost: &str,
        messages: &[PeekedMessage],
        to_exchange: Option<&str>,
        routing_key: Option<&str>,
    ) -> Self {
        let mut destinations = BTreeMap::new();
        let mut unmovable = 0;
        let planned = messages
            .iter()
            .map(|m| {
                let deaths = DeathRecord::from_headers(&m.headers());
                let newest = deaths.first();
                let destination = RedriveDestination::resolve(newest, to_exchange, routing_key);
                match &destination {
                    Ok(d) => *destinations.entry(destination_label(d)).or_default() += 1,
                    Err(_) => unmovable += 1,
                }
                PlannedMove {
                    from_queue: newest.map(|d| d.queue.clone()).unwrap_or_default(),
                    reason: newest.map(|d| d.reason.clone()).unwrap_or_default(),
                    deaths: newest.map(|d| d.count).unwrap_or_default(),
                    problem: destination
                        .as_ref()
                        .err()
                        .map(|_| NO_DEATH_PROBLEM.to_string()),
                    destination: destination.ok(),
                }
            })
            .collect();
        Self {
            dry_run: true,
            queue: queue.to_string(),
            vhost: vhost.to_string(),
            messages: planned,
            destinations,
            unmovable,
        }
    }

    /// Replaces the problem text of the unmovable messages, e.g. with one
    /// that names how this front end sets a destination exchange.
    pub fn with_unmovable_problem(mut self, problem: &str) -> Self {
        for planned in &mut self.messages {
            if planned.problem.is_some() {
                planned.problem = Some(problem.to_string());
            }
        }
        self
    }
}

/// The audit result of a redrive: counts, source and destinations, never
/// message content or broker error text.
#[cfg(feature = "amqp")]
pub fn redrive_outcome(
    queue: &str,
    report: &crate::amqp::RedriveReport,
    failure: Option<crate::amqp::RedriveFailure>,
) -> String {
    let destinations = report
        .destinations
        .iter()
        .map(|(destination, n)| format!("{n} to {destination}"))
        .collect::<Vec<_>>()
        .join("; ");
    let mut outcome = format!("moved {} message(s) from queue {queue}", report.moved);
    if !destinations.is_empty() {
        outcome.push_str(&format!(" ({destinations})"));
    }
    if let Some(failure) = failure {
        outcome.push_str(&format!(", then stopped: {failure}"));
    }
    outcome
}

/// `KEY:VALUE` headers (the value may hold more colons; the key is
/// trimmed and must not be empty).
///
/// ```
/// use ferrika_rabbitmq::view::parse_headers;
/// assert_eq!(
///     parse_headers(&["trace: a:b".to_string()]).unwrap(),
///     vec![("trace".to_string(), " a:b".to_string())]
/// );
/// assert!(parse_headers(&["novalue".to_string()]).is_err());
/// ```
pub fn parse_headers(headers: &[String]) -> Result<Vec<(String, String)>, String> {
    headers
        .iter()
        .map(|h| {
            h.split_once(':')
                .map(|(k, v)| (k.trim().to_string(), v.to_string()))
                .filter(|(k, _)| !k.is_empty())
                .ok_or_else(|| format!("Invalid header '{h}': expected KEY:VALUE"))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::DEFAULT_EXCHANGE;

    fn peeked(json: &str) -> PeekedMessage {
        serde_json::from_str(json).unwrap()
    }

    #[test]
    fn headers_are_key_colon_value() {
        assert_eq!(
            parse_headers(&["a:1".to_string(), "trace: x:y".to_string()]).unwrap(),
            vec![
                ("a".to_string(), "1".to_string()),
                ("trace".to_string(), " x:y".to_string())
            ]
        );
        assert!(parse_headers(&["novalue".to_string()]).is_err());
        assert!(parse_headers(&[":v".to_string()]).is_err());
    }

    #[test]
    fn peeked_messages_are_masked_only_when_asked() {
        let message = peeked(
            r#"{"exchange":"users","routing_key":"u.1","redelivered":false,"payload_bytes":61,
                "payload":"{\"email\":\"kim@example.com\",\"password\":\"hunter2hunter2\"}",
                "payload_encoding":"string",
                "properties":{"delivery_mode":2,"user_id":"kim@example.com",
                  "headers":{"authorization":"Bearer abc.def.ghi","x-retries":3,
                    "x-death":[{"queue":"users","reason":"rejected","count":1}]}}}"#,
        );
        let plain = ShownMessage::new(&message, false);
        assert!(plain
            .payload
            .as_deref()
            .unwrap()
            .contains("kim@example.com"));
        assert_eq!(plain.properties, message.properties);

        let masked = ShownMessage::new(&message, true);
        let text = serde_json::to_string(&masked).unwrap();
        assert!(!text.contains("kim@example.com"), "{text}");
        assert!(!text.contains("hunter2hunter2"), "{text}");
        assert!(!text.contains("abc.def.ghi"), "{text}");
        assert_eq!(masked.properties["delivery_mode"], 2);
        assert_eq!(masked.properties["headers"]["x-retries"], 3);
        // Tables such as x-death stay readable, as masked JSON text.
        let death = masked.properties["headers"]["x-death"].as_str().unwrap();
        assert!(death.contains("rejected"), "{death}");
    }

    #[test]
    fn binary_payloads_are_base64_unless_masked() {
        let message = peeked(
            r#"{"payload_bytes":3,"payload":"AP8B","payload_encoding":"base64","properties":[]}"#,
        );
        let plain = ShownMessage::new(&message, false);
        assert_eq!(plain.payload, None);
        assert_eq!(plain.payload_base64.as_deref(), Some("AP8B"));
        assert_eq!(plain.payload_preview(120), "base64:AP8B");
        let masked = ShownMessage::new(&message, true);
        assert_eq!(masked.payload_base64, None);
        assert!(masked.payload.as_deref().unwrap().contains("hidden"));
    }

    fn dead_lettered(exchange: &str, key: &str, body: &str) -> PeekedMessage {
        peeked(&format!(
            r#"{{"payload":{body:?},"payload_encoding":"string","properties":{{"headers":{{
                "x-death":[{{"count":2,"reason":"rejected","queue":"orders",
                  "exchange":"{exchange}","routing-keys":["{key}"]}}]}}}}}}"#
        ))
    }

    #[test]
    fn inspect_groups_by_origin_and_masks_bodies() {
        let messages = vec![
            dead_lettered("shop", "order.created", r#"{"email":"kim@example.com"}"#),
            dead_lettered("shop", "order.created", "x"),
            dead_lettered("", "orders", "y"),
            peeked(r#"{"payload":"z","payload_encoding":"string","properties":{}}"#),
        ];
        let inspection = Inspection::new(&messages, true);
        assert_eq!(inspection.messages.len(), 4);
        assert_eq!(inspection.groups.len(), 3);
        let shop = inspection
            .groups
            .iter()
            .find(|g| g.exchange == "shop")
            .unwrap();
        assert_eq!(
            (shop.queue.as_str(), shop.reason.as_str(), shop.messages),
            ("orders", "rejected", 2)
        );
        assert!(inspection
            .groups
            .iter()
            .any(|g| g.exchange == DEFAULT_EXCHANGE));
        assert!(inspection.groups.iter().any(|g| g.reason == "no x-death"));
        let text = serde_json::to_string(&inspection).unwrap();
        assert!(!text.contains("kim@example.com"), "{text}");
        assert_eq!(inspection.messages[0].deaths[0].count, 2);
    }

    #[test]
    fn the_redrive_preview_shows_destinations_not_bodies() {
        let messages = vec![
            dead_lettered("shop", "order.created", "secret-body"),
            dead_lettered("shop", "order.created", "x"),
            peeked(r#"{"payload":"z","payload_encoding":"string","properties":{}}"#),
        ];
        let preview = RedrivePreview::new("orders.dlq", "/", &messages, None, None);
        assert!(preview.dry_run);
        assert_eq!(
            preview.destinations["exchange 'shop', routing key 'order.created'"],
            2
        );
        assert_eq!(preview.unmovable, 1);
        assert_eq!(
            preview.messages[2].problem.as_deref(),
            Some(NO_DEATH_PROBLEM)
        );
        let text = serde_json::to_string(&preview).unwrap();
        assert!(!text.contains("secret-body"), "{text}");
        let renamed = preview.with_unmovable_problem("pick one");
        assert_eq!(renamed.messages[2].problem.as_deref(), Some("pick one"));
        assert_eq!(renamed.messages[0].problem, None);

        let overridden =
            RedrivePreview::new("orders.dlq", "/", &messages, Some("retry"), Some("again"));
        assert_eq!(overridden.unmovable, 0);
        assert_eq!(
            overridden.destinations["exchange 'retry', routing key 'again'"],
            3
        );
    }

    #[cfg(feature = "amqp")]
    #[test]
    fn the_redrive_audit_outcome_has_counts_and_destinations_only() {
        use crate::amqp::{RedriveFailure, RedriveReport};
        let report = RedriveReport {
            moved: 3,
            destinations: BTreeMap::from([(
                "exchange 'shop', routing key 'order.created'".to_string(),
                3,
            )]),
        };
        assert_eq!(
            redrive_outcome("orders.dlq", &report, None),
            "moved 3 message(s) from queue orders.dlq (3 to exchange 'shop', routing key \
             'order.created')"
        );
        let stopped = redrive_outcome(
            "orders.dlq",
            &RedriveReport::default(),
            Some(RedriveFailure::Unroutable),
        );
        assert!(stopped.starts_with("moved 0 message(s) from queue orders.dlq, then stopped"));
    }
}
