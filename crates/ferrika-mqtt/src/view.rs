//! Messages as a front end shows them: PII-masked on request, the payload
//! as text when it is UTF-8 (else Base64, never under masking). No I/O;
//! every front end masks MQTT messages the same way, and the same way as
//! Kafka records and RabbitMQ messages.
//!
//! ```
//! use ferrika_mqtt::client::MqttMessage;
//! use ferrika_mqtt::view::ShownMessage;
//!
//! let message = MqttMessage {
//!     topic: "users/42/profile".to_string(),
//!     payload: br#"{"email":"kim@example.com"}"#.to_vec(),
//!     qos: 1,
//!     retain: true,
//!     user_properties: Vec::new(),
//!     content_type: None,
//! };
//! let shown = ShownMessage::new(&message, true);
//! assert!(!shown.payload.unwrap().contains("kim@example.com"));
//! ```

use serde::Serialize;
use serde_json::{Map, Value};

use ferrika_core::codec::pii::{
    ellipsize, mask_bytes, mask_header_value, mask_payload, MaskStrategy,
};

use crate::client::MqttMessage;

/// One message as it is shown.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ShownMessage {
    /// The topic; under masking, personal data in it (an e-mail address in
    /// a topic level) is masked like text.
    pub topic: String,
    pub qos: u8,
    pub retain: bool,
    /// The payload's size in bytes.
    pub size: usize,
    pub payload: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload_base64: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    /// MQTT 5 user properties (masked by name under masking, like record
    /// headers). A name given twice keeps its last value.
    #[serde(skip_serializing_if = "Map::is_empty")]
    pub user_properties: Map<String, Value>,
}

impl ShownMessage {
    /// `message` as shown; with `mask`, the payload, the topic and every
    /// user property are PII-masked (partial).
    pub fn new(message: &MqttMessage, mask: bool) -> Self {
        let (payload, payload_base64) = if mask {
            (
                Some(mask_bytes(&message.payload, MaskStrategy::Partial)),
                None,
            )
        } else {
            match std::str::from_utf8(&message.payload) {
                Ok(text) => (Some(text.to_string()), None),
                Err(_) => (
                    None,
                    Some(ferrika_core::codec::base64::encode(&message.payload)),
                ),
            }
        };
        let topic = if mask {
            mask_payload(&message.topic, MaskStrategy::Partial)
        } else {
            message.topic.clone()
        };
        let user_properties = message
            .user_properties
            .iter()
            .map(|(name, value)| {
                let shown = if mask {
                    mask_header_value(name, value.as_bytes(), MaskStrategy::Partial)
                } else {
                    value.clone()
                };
                (name.clone(), Value::String(shown))
            })
            .collect();
        Self {
            topic,
            qos: message.qos,
            retain: message.retain,
            size: message.payload.len(),
            payload,
            payload_base64,
            content_type: message.content_type.clone(),
            user_properties,
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
        ellipsize(&payload.replace(['\n', '\r'], " "), max_chars)
    }
}

/// `KEY:VALUE` user properties from the command line, in order.
pub fn parse_user_properties(items: &[String]) -> Result<Vec<(String, String)>, String> {
    items
        .iter()
        .map(|item| {
            let (key, value) = item
                .split_once(':')
                .ok_or_else(|| format!("Invalid property '{item}': use KEY:VALUE"))?;
            let key = key.trim();
            if key.is_empty() {
                return Err(format!("Invalid property '{item}': the key is empty"));
            }
            Ok((key.to_string(), value.to_string()))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(payload: &[u8]) -> MqttMessage {
        MqttMessage {
            topic: "users/kim@example.com/state".to_string(),
            payload: payload.to_vec(),
            qos: 1,
            retain: false,
            user_properties: vec![
                ("authorization".to_string(), "Bearer abc.def".to_string()),
                ("trace".to_string(), "t-1".to_string()),
            ],
            content_type: Some("application/json".to_string()),
        }
    }

    #[test]
    fn masking_covers_payload_topic_and_properties() {
        let shown = ShownMessage::new(&message(br#"{"email":"kim@example.com","n":1}"#), true);
        let text = serde_json::to_string(&shown).unwrap();
        assert!(!text.contains("kim@example.com"), "{text}");
        assert!(!text.contains("abc.def"), "{text}");
        assert!(text.contains("\"trace\":\"t-1\""), "{text}");
        assert_eq!(shown.size, 33);
    }

    #[test]
    fn unmasked_text_is_shown_as_is_and_binary_as_base64() {
        let text = ShownMessage::new(&message(b"hello\nworld"), false);
        assert_eq!(text.payload.as_deref(), Some("hello\nworld"));
        assert_eq!(text.topic, "users/kim@example.com/state");
        assert_eq!(text.payload_preview(100), "hello world");
        let binary = ShownMessage::new(&message(&[0xff, 0x00, 0x10]), false);
        assert_eq!(binary.payload, None);
        assert!(binary.payload_preview(100).starts_with("base64:"));
        // Under masking binary data is hidden, never Base64.
        let masked = ShownMessage::new(&message(&[0xff, 0x00, 0x10]), true);
        assert_eq!(masked.payload_base64, None);
        assert!(masked.payload.unwrap().contains("hidden"));
    }

    #[test]
    fn user_properties_parse_from_key_value_pairs() {
        assert_eq!(
            parse_user_properties(&["a:1".to_string(), "b: x:y".to_string()]).unwrap(),
            vec![
                ("a".to_string(), "1".to_string()),
                ("b".to_string(), " x:y".to_string())
            ]
        );
        assert!(parse_user_properties(&["novalue".to_string()]).is_err());
        assert!(parse_user_properties(&[":v".to_string()]).is_err());
    }
}
