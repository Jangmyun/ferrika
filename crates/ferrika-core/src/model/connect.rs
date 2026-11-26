//! Kafka Connect connector offsets (KIP-875), in the shape the Connect REST
//! API reads and writes them (`GET`/`PATCH /connectors/{name}/offsets`).
//!
//! A source connector's partition and offset are maps the connector
//! defines (e.g. `{"filename": "/data/in.txt"}` and `{"position": 1024}`).
//! A sink connector's partition is `{"kafka_topic": ..., "kafka_partition": ...}`
//! and its offset `{"kafka_offset": ...}`; [`ConnectorOffset::as_sink`] and
//! [`ConnectorOffset::sink`] convert from and to [`SinkOffset`].

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{KafkaError, KafkaResult};

const SINK_TOPIC: &str = "kafka_topic";
const SINK_PARTITION: &str = "kafka_partition";
const SINK_OFFSET: &str = "kafka_offset";

/// One partition of a connector and its offset. `offset` is `None` (JSON
/// `null`) for a partition without one; in an alter request it resets that
/// partition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConnectorOffset {
    pub partition: BTreeMap<String, Value>,
    #[serde(default)]
    pub offset: Option<BTreeMap<String, Value>>,
}

/// A sink connector's position in one Kafka topic partition: the next
/// record it reads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SinkOffset {
    pub topic: String,
    pub partition: i32,
    pub offset: Option<i64>,
}

impl ConnectorOffset {
    /// A sink connector's offset for `topic`/`partition`; `None` resets it.
    pub fn sink(topic: &str, partition: i32, offset: Option<i64>) -> Self {
        Self {
            partition: BTreeMap::from([
                (SINK_TOPIC.to_string(), Value::from(topic)),
                (SINK_PARTITION.to_string(), Value::from(partition)),
            ]),
            offset: offset.map(|o| BTreeMap::from([(SINK_OFFSET.to_string(), Value::from(o))])),
        }
    }

    /// The entry as a sink offset, when its partition is a Kafka topic
    /// partition (exactly `kafka_topic` and `kafka_partition`).
    pub fn as_sink(&self) -> Option<SinkOffset> {
        if self.partition.len() != 2 {
            return None;
        }
        let topic = self.partition.get(SINK_TOPIC)?.as_str()?.to_string();
        let partition = i32::try_from(self.partition.get(SINK_PARTITION)?.as_i64()?).ok()?;
        let offset = match &self.offset {
            None => None,
            Some(map) => Some(map.get(SINK_OFFSET)?.as_i64()?),
        };
        Some(SinkOffset {
            topic,
            partition,
            offset,
        })
    }

    /// The partition as compact JSON (keys sorted), e.g. `{"filename":"a.txt"}`.
    pub fn partition_text(&self) -> String {
        serde_json::to_string(&self.partition).unwrap_or_default()
    }

    /// The offset as compact JSON, `None` without one.
    pub fn offset_text(&self) -> Option<String> {
        self.offset
            .as_ref()
            .map(|o| serde_json::to_string(o).unwrap_or_default())
    }
}

/// Whether every entry is a sink offset (and there is at least one).
pub fn all_sink(offsets: &[ConnectorOffset]) -> bool {
    !offsets.is_empty() && offsets.iter().all(|o| o.as_sink().is_some())
}

/// Parses connector offsets from JSON: `{"offsets": [...]}` as the Connect
/// REST API reads and writes them, or the bare array. Each entry needs a
/// `partition` object; its `offset` is an object or `null`.
pub fn parse_offsets_json(text: &str) -> KafkaResult<Vec<ConnectorOffset>> {
    let json: Value = serde_json::from_str(text)
        .map_err(|e| KafkaError::config(format!("Connector offsets are not valid JSON: {e}")))?;
    let list = match &json {
        Value::Array(_) => &json,
        Value::Object(map) => map.get("offsets").ok_or_else(|| {
            KafkaError::config(r#"Connector offsets must be {"offsets": [...]} or an array"#)
        })?,
        _ => {
            return Err(KafkaError::config(
                r#"Connector offsets must be {"offsets": [...]} or an array"#,
            ))
        }
    };
    let entries = list
        .as_array()
        .ok_or_else(|| KafkaError::config("\"offsets\" must be an array"))?;
    entries
        .iter()
        .enumerate()
        .map(|(i, entry)| {
            let partition = entry
                .get("partition")
                .and_then(Value::as_object)
                .ok_or_else(|| {
                    KafkaError::config(format!("Offset entry {i} needs a \"partition\" object"))
                })?;
            let offset = match entry.get("offset") {
                None | Some(Value::Null) => None,
                Some(Value::Object(o)) => Some(o.clone().into_iter().collect()),
                Some(_) => {
                    return Err(KafkaError::config(format!(
                        "The \"offset\" of entry {i} must be an object or null"
                    )))
                }
            };
            Ok(ConnectorOffset {
                partition: partition.clone().into_iter().collect(),
                offset,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sink_offsets_convert_both_ways() {
        let o = ConnectorOffset::sink("orders", 2, Some(42));
        assert_eq!(
            o.as_sink(),
            Some(SinkOffset {
                topic: "orders".into(),
                partition: 2,
                offset: Some(42)
            })
        );
        assert_eq!(
            o.partition_text(),
            r#"{"kafka_partition":2,"kafka_topic":"orders"}"#
        );
        assert_eq!(o.offset_text().as_deref(), Some(r#"{"kafka_offset":42}"#));
        let reset = ConnectorOffset::sink("orders", 0, None);
        assert_eq!(reset.as_sink().map(|s| s.offset), Some(None));
        assert_eq!(
            serde_json::to_string(&reset).unwrap(),
            r#"{"partition":{"kafka_partition":0,"kafka_topic":"orders"},"offset":null}"#
        );
        assert!(all_sink(&[o, reset]));
        assert!(!all_sink(&[]));
    }

    #[test]
    fn source_offsets_are_not_sink_offsets() {
        let parsed = parse_offsets_json(
            r#"{"offsets":[{"partition":{"filename":"/data/in.txt"},"offset":{"position":1024}}]}"#,
        )
        .unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].as_sink(), None);
        assert!(!all_sink(&parsed));
        assert_eq!(parsed[0].partition_text(), r#"{"filename":"/data/in.txt"}"#);
        assert_eq!(
            parsed[0].offset_text().as_deref(),
            Some(r#"{"position":1024}"#)
        );
    }

    #[test]
    fn offsets_json_accepts_wrapped_and_bare_forms() {
        let bare = parse_offsets_json(
            r#"[{"partition":{"kafka_topic":"t","kafka_partition":1},"offset":null}]"#,
        )
        .unwrap();
        assert_eq!(bare[0].as_sink().unwrap().offset, None);
        let missing_offset = parse_offsets_json(
            r#"{"offsets":[{"partition":{"kafka_topic":"t","kafka_partition":1}}]}"#,
        )
        .unwrap();
        assert_eq!(missing_offset, bare);
        assert!(parse_offsets_json("nope").is_err());
        assert!(parse_offsets_json(r#"{"x":1}"#).is_err());
        assert!(parse_offsets_json(r#"[{"offset":{"a":1}}]"#).is_err());
        assert!(parse_offsets_json(r#"[{"partition":{"a":1},"offset":5}]"#).is_err());
    }
}
