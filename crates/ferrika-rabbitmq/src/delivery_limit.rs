//! Whether reading a queue through the management API can lose messages.
//!
//! The management API has no browse: a peek takes messages and requeues
//! them. On a quorum queue with a delivery limit (`x-delivery-limit`, or a
//! policy's `delivery-limit`), RabbitMQ before 4.0 counts every return as a
//! delivery attempt (`x-delivery-count`), so repeated peeks push a message
//! over the limit and the broker dead-letters it (reason `delivery_limit`),
//! or drops it when the queue has no dead-letter exchange. Checked against
//! RabbitMQ 3.13.7: with `x-delivery-limit: 2`, the third peek of a message
//! dead-lettered it.
//!
//! RabbitMQ 4.0 and later count only failed deliveries (a reject or nack
//! with requeue) towards the limit; the requeue of a peek is not one. Checked
//! against RabbitMQ 4.4.1: 28 peeks of a message on a queue with
//! `x-delivery-limit: 2` kept it (only `x-acquired-count` grew).
//!
//! [`ManagementClient::peek_risk`] finds out which case a queue is in, so a
//! front end can refuse the peek or ask for an explicit opt-in, and
//! [`PeekRisk::fate`] says per peeked message how many more peeks it
//! survives.

use serde::Serialize;
use serde_json::Value;

use crate::client::{segment, ManagementClient};
use crate::error::RabbitResult;
use crate::model::{PeekedMessage, Queue};

/// The queue fields the check reads.
const QUEUE_COLUMNS: &str = "name,vhost,type,arguments,effective_policy_definition,delivery_limit";

/// What a peek of one queue does to its messages' delivery counts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PeekRisk {
    pub queue: String,
    /// `classic`, `quorum` or `stream`.
    pub queue_type: String,
    /// The delivery limit in force: the lowest positive one of the
    /// `x-delivery-limit` argument, the policy's `delivery-limit` and what
    /// the broker reports. `None`: no limit.
    pub delivery_limit: Option<u64>,
    /// The broker's version (`rabbitmq_version`), when it could be read.
    pub broker_version: Option<String>,
    /// Messages over the limit go to a dead-letter exchange (else they are
    /// dropped).
    pub dead_letter_exchange: bool,
}

/// What one peek did to a message on a queue whose peeks count.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case", tag = "fate", content = "peeks_left")]
pub enum DeliveryFate {
    /// Still in the queue; this many more peeks keep it there (the one
    /// after them dead-letters or drops it).
    Kept(u64),
    /// This peek took it over the limit: the broker dead-lettered or
    /// dropped it when it was requeued.
    OverLimit,
}

impl PeekRisk {
    /// Whether the broker is RabbitMQ 4.0 or later (peeks do not count).
    /// An unknown version counts as older.
    pub fn broker_counts_failures_only(&self) -> bool {
        self.broker_version
            .as_deref()
            .and_then(major_version)
            .is_some_and(|major| major >= 4)
    }

    /// Whether each peek counts as a delivery attempt that can dead-letter
    /// or drop messages: a quorum queue with a delivery limit on a broker
    /// before 4.0 (or of unknown version).
    pub fn peeks_count(&self) -> bool {
        self.queue_type == "quorum"
            && self.delivery_limit.is_some()
            && !self.broker_counts_failures_only()
    }

    /// Where messages over the limit go, in words.
    fn over_limit(&self) -> &'static str {
        if self.dead_letter_exchange {
            "dead-lettered"
        } else {
            "dropped (the queue has no dead-letter exchange)"
        }
    }

    /// Why a peek of this queue is refused without an explicit opt-in;
    /// `opt_in` names it (`--allow-delivery-count`). `None` when peeks do
    /// not count.
    pub fn refusal(&self, opt_in: &str) -> Option<String> {
        if !self.peeks_count() {
            return None;
        }
        Some(format!(
            "Queue '{}' is a quorum queue with a delivery limit of {} on {}: every peek counts \
             as a delivery attempt, and a message read too often is {} by the broker. Peek \
             anyway with {opt_in} (each message's remaining peeks are shown), or read it with \
             a consumer.",
            self.queue,
            self.delivery_limit.unwrap_or_default(),
            self.version_text(),
            self.over_limit()
        ))
    }

    /// The warning to show when peeking anyway. `None` when peeks do not
    /// count.
    pub fn warning(&self) -> Option<String> {
        if !self.peeks_count() {
            return None;
        }
        Some(format!(
            "Queue '{}' counts this peek towards its delivery limit of {} ({}); a message over \
             the limit is {}.",
            self.queue,
            self.delivery_limit.unwrap_or_default(),
            self.version_text(),
            self.over_limit()
        ))
    }

    fn version_text(&self) -> String {
        match &self.broker_version {
            Some(version) => format!("RabbitMQ {version}"),
            None => "a RabbitMQ of unknown version (assumed before 4.0)".to_string(),
        }
    }

    /// What this peek did to `message` (read with its `x-delivery-count`
    /// before the peek's own requeue). `None` when peeks do not count.
    pub fn fate(&self, message: &PeekedMessage) -> Option<DeliveryFate> {
        if !self.peeks_count() {
            return None;
        }
        let limit = self.delivery_limit?;
        // The requeue adds one; over the limit the broker takes it.
        let after = delivery_count(message).saturating_add(1);
        Some(if after > limit {
            DeliveryFate::OverLimit
        } else {
            DeliveryFate::Kept(limit - after)
        })
    }

    /// From the queue as `GET /api/queues/{vhost}/{name}` returns it and the
    /// broker version.
    pub fn from_queue(queue: &Value, broker_version: Option<String>) -> RabbitResult<Self> {
        let parsed: Queue = serde_json::from_value(queue.clone())
            .map_err(|e| crate::error::RabbitError::Response(e.to_string()))?;
        let limits = [
            parsed.arguments.get("x-delivery-limit"),
            parsed.effective_policy_definition.get("delivery-limit"),
            queue.get("delivery_limit"),
        ];
        let delivery_limit = limits.into_iter().flatten().filter_map(positive).min();
        Ok(Self {
            dead_letter_exchange: parsed.dead_letter_exchange().is_some(),
            queue: parsed.name,
            queue_type: parsed.kind,
            delivery_limit,
            broker_version,
        })
    }
}

/// The `x-delivery-count` header of a peeked message (0 when absent: never
/// returned before).
pub fn delivery_count(message: &PeekedMessage) -> u64 {
    message
        .headers()
        .into_iter()
        .find(|(name, _)| *name == "x-delivery-count")
        .and_then(|(_, value)| positive(value))
        .unwrap_or(0)
}

/// A positive whole number (limits below 1 mean no limit).
fn positive(value: &Value) -> Option<u64> {
    match value {
        Value::Number(n) => n.as_u64().or_else(|| {
            n.as_f64()
                .filter(|f| f.fract() == 0.0 && *f >= 1.0)
                .map(|f| f as u64)
        }),
        Value::String(s) => s.trim().parse().ok(),
        _ => None,
    }
    .filter(|n| *n > 0)
}

/// The major number of `3.13.7` or `4.1.0-rc.1`.
fn major_version(version: &str) -> Option<u64> {
    version.trim().split('.').next()?.parse().ok()
}

impl ManagementClient {
    /// Whether peeking `queue` counts towards its delivery limit (see the
    /// module documentation). Reads the queue and, for a quorum queue with
    /// a limit, the broker version; a version that cannot be read counts
    /// as before 4.0.
    pub async fn peek_risk(&self, vhost: &str, queue: &str) -> RabbitResult<PeekRisk> {
        let path = format!(
            "/api/queues/{}/{}?columns={QUEUE_COLUMNS}",
            segment(vhost),
            segment(queue)
        );
        let info: Value = self.get_json(&path).await?;
        let risk = PeekRisk::from_queue(&info, None)?;
        if risk.queue_type != "quorum" || risk.delivery_limit.is_none() {
            return Ok(risk);
        }
        let version = self
            .get_json::<Value>("/api/overview?columns=rabbitmq_version")
            .await
            .ok()
            .and_then(|overview| {
                overview
                    .get("rabbitmq_version")
                    .and_then(Value::as_str)
                    .map(str::to_string)
            });
        Ok(PeekRisk {
            broker_version: version,
            ..risk
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn quorum(arguments: Value, version: Option<&str>) -> PeekRisk {
        PeekRisk::from_queue(
            &json!({
                "name": "orders", "vhost": "/", "type": "quorum", "arguments": arguments,
            }),
            version.map(str::to_string),
        )
        .unwrap()
    }

    fn peeked(delivery_count: Option<u64>) -> PeekedMessage {
        let properties = match delivery_count {
            Some(n) => json!({ "headers": { "x-delivery-count": n } }),
            None => json!([]),
        };
        serde_json::from_value(json!({ "payload": "m", "properties": properties })).unwrap()
    }

    #[test]
    fn only_quorum_queues_with_a_limit_before_4_0_count_peeks() {
        let old = quorum(json!({ "x-delivery-limit": 2 }), Some("3.13.7"));
        assert_eq!(old.delivery_limit, Some(2));
        assert!(old.peeks_count());
        assert!(!old.dead_letter_exchange);
        let refusal = old.refusal("--allow-delivery-count").unwrap();
        assert!(refusal.contains("delivery limit of 2"), "{refusal}");
        assert!(refusal.contains("RabbitMQ 3.13.7"), "{refusal}");
        assert!(refusal.contains("dropped"), "{refusal}");
        assert!(refusal.contains("--allow-delivery-count"), "{refusal}");
        assert!(old.warning().is_some());

        let new = quorum(json!({ "x-delivery-limit": 2 }), Some("4.4.1"));
        assert!(new.broker_counts_failures_only());
        assert!(!new.peeks_count());
        assert_eq!(new.refusal("--x"), None);
        assert_eq!(new.warning(), None);
        assert_eq!(new.fate(&peeked(Some(5))), None);

        // An unknown version is treated as older.
        let unknown = quorum(json!({ "x-delivery-limit": 2 }), None);
        assert!(unknown.peeks_count());
        assert!(unknown.refusal("--x").unwrap().contains("unknown version"));

        // No limit (or "unlimited" values): nothing to lose.
        assert!(!quorum(json!({}), Some("3.13.7")).peeks_count());
        assert!(!quorum(json!({ "x-delivery-limit": -1 }), Some("3.13.7")).peeks_count());
        assert!(!quorum(json!({ "x-delivery-limit": 0 }), Some("3.13.7")).peeks_count());

        let classic = PeekRisk::from_queue(
            &json!({ "name": "c", "vhost": "/", "type": "classic",
                     "arguments": { "x-delivery-limit": 2 } }),
            Some("3.13.7".to_string()),
        )
        .unwrap();
        assert!(!classic.peeks_count());
    }

    #[test]
    fn the_lowest_of_argument_policy_and_broker_limit_applies() {
        let risk = PeekRisk::from_queue(
            &json!({
                "name": "orders", "vhost": "/", "type": "quorum",
                "arguments": { "x-delivery-limit": 10, "x-dead-letter-exchange": "dlx" },
                "effective_policy_definition": { "delivery-limit": 3 },
                "delivery_limit": 20,
            }),
            Some("3.12.0".to_string()),
        )
        .unwrap();
        assert_eq!(risk.delivery_limit, Some(3));
        assert!(risk.dead_letter_exchange);
        assert!(risk.warning().unwrap().contains("dead-lettered"));
    }

    #[test]
    fn the_fate_follows_the_delivery_count() {
        // As observed on 3.13.7 with a limit of 2: the third peek
        // (x-delivery-count 2) dead-lettered the message.
        let risk = quorum(json!({ "x-delivery-limit": 2 }), Some("3.13.7"));
        assert_eq!(risk.fate(&peeked(None)), Some(DeliveryFate::Kept(1)));
        assert_eq!(risk.fate(&peeked(Some(1))), Some(DeliveryFate::Kept(0)));
        assert_eq!(risk.fate(&peeked(Some(2))), Some(DeliveryFate::OverLimit));
        assert_eq!(risk.fate(&peeked(Some(9))), Some(DeliveryFate::OverLimit));
        assert_eq!(delivery_count(&peeked(Some(7))), 7);
        assert_eq!(delivery_count(&peeked(None)), 0);
    }

    #[test]
    fn versions_parse_by_their_major_number() {
        assert_eq!(major_version("3.13.7"), Some(3));
        assert_eq!(major_version("4.1.0-rc.1"), Some(4));
        assert_eq!(major_version("garbage"), None);
    }

    #[tokio::test]
    async fn the_check_reads_the_queue_and_the_version() {
        use std::sync::Arc;
        let handler: crate::test_support::Handler = Arc::new(|path: &str| {
            if path.starts_with("/api/overview") {
                return (200, r#"{"rabbitmq_version":"3.13.7"}"#.to_string());
            }
            if path.starts_with("/api/queues/%2F/orders.q?") {
                return (
                    200,
                    json!({ "name": "orders.q", "vhost": "/", "type": "quorum",
                            "arguments": { "x-delivery-limit": 4 } })
                    .to_string(),
                );
            }
            (
                200,
                json!({ "name": "plain", "vhost": "/", "type": "classic", "arguments": {} })
                    .to_string(),
            )
        });
        let (url, seen) = crate::test_support::serve(handler).await;
        let client = ManagementClient::new(&url).unwrap();
        let risk = client.peek_risk("/", "orders.q").await.unwrap();
        assert!(risk.peeks_count());
        assert_eq!(risk.broker_version.as_deref(), Some("3.13.7"));
        assert_eq!(seen.lock().unwrap().len(), 2);
        seen.lock().unwrap().clear();
        // A classic queue needs no version lookup.
        let plain = client.peek_risk("/", "plain").await.unwrap();
        assert!(!plain.peeks_count());
        assert_eq!(seen.lock().unwrap().len(), 1);
    }
}
