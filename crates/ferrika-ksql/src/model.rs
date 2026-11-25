//! ksqlDB REST API responses as typed values. They read the camelCase
//! fields the server sends and serialize in snake_case for Ferrika's own
//! output. Fields the server may leave out default to empty.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// `GET /info`: `{"KsqlServerInfo": {...}}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all(deserialize = "camelCase"))]
pub struct ServerInfo {
    #[serde(default)]
    pub version: String,
    #[serde(default)]
    pub kafka_cluster_id: String,
    #[serde(default)]
    pub ksql_service_id: String,
    /// `RUNNING`, or `ERROR` while the server cannot serve (since 0.10).
    #[serde(default)]
    pub server_status: Option<String>,
}

/// A stream or table of `SHOW STREAMS` / `SHOW TABLES`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all(deserialize = "camelCase"))]
pub struct SourceInfo {
    pub name: String,
    /// `STREAM` or `TABLE`.
    #[serde(default, rename(deserialize = "type"))]
    pub kind: String,
    #[serde(default)]
    pub topic: String,
    #[serde(default)]
    pub key_format: String,
    #[serde(default)]
    pub value_format: String,
    #[serde(default)]
    pub is_windowed: bool,
}

/// A persistent query of `SHOW QUERIES` (also the read and write queries
/// of a `DESCRIBE`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all(deserialize = "camelCase"))]
pub struct QueryInfo {
    pub id: String,
    #[serde(default)]
    pub query_string: String,
    #[serde(default)]
    pub sinks: Vec<String>,
    #[serde(default)]
    pub sink_kafka_topics: Vec<String>,
    /// `PERSISTENT`, `PUSH`.
    #[serde(default)]
    pub query_type: Option<String>,
    /// `RUNNING`, `PAUSED`, `ERROR`... (the server's summary over its nodes).
    #[serde(default)]
    pub state: Option<String>,
    /// Nodes per state, e.g. `{"RUNNING": 1}`.
    #[serde(default)]
    pub status_count: BTreeMap<String, u64>,
}

/// A Kafka topic of `SHOW TOPICS`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all(deserialize = "camelCase"))]
pub struct TopicInfo {
    pub name: String,
    /// Replicas per partition.
    #[serde(default)]
    pub replica_info: Vec<u32>,
}

impl TopicInfo {
    pub fn partitions(&self) -> usize {
        self.replica_info.len()
    }
}

/// A column's type as `DESCRIBE` reports it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all(deserialize = "camelCase"))]
pub struct FieldSchema {
    #[serde(default, rename(deserialize = "type"))]
    pub type_name: String,
    /// The fields of a `STRUCT`.
    #[serde(default)]
    pub fields: Option<Vec<FieldInfo>>,
    /// The element type of an `ARRAY` or the value type of a `MAP`.
    #[serde(default)]
    pub member_schema: Option<Box<FieldSchema>>,
}

impl FieldSchema {
    /// The type as ksqlDB writes it: `STRING`, `ARRAY<INTEGER>`,
    /// `MAP<STRING, DOUBLE>`, `STRUCT<A STRING, B INTEGER>`.
    pub fn render(&self) -> String {
        match (self.type_name.as_str(), &self.member_schema, &self.fields) {
            ("ARRAY", Some(member), _) => format!("ARRAY<{}>", member.render()),
            ("MAP", Some(member), _) => format!("MAP<STRING, {}>", member.render()),
            ("STRUCT", _, Some(fields)) => format!(
                "STRUCT<{}>",
                fields
                    .iter()
                    .map(|f| format!("{} {}", f.name, f.schema.render()))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            (name, _, _) => name.to_string(),
        }
    }
}

/// A column of a stream or table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all(deserialize = "camelCase"))]
pub struct FieldInfo {
    pub name: String,
    pub schema: FieldSchema,
    /// `KEY` for key columns, `HEADER` for header columns, else none.
    #[serde(default, rename(deserialize = "type"))]
    pub column_type: Option<String>,
}

/// `DESCRIBE name [EXTENDED]`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all(deserialize = "camelCase"))]
pub struct SourceDescription {
    pub name: String,
    /// `STREAM` or `TABLE`.
    #[serde(default, rename(deserialize = "type"))]
    pub kind: String,
    #[serde(default)]
    pub topic: String,
    #[serde(default)]
    pub key_format: String,
    #[serde(default)]
    pub value_format: String,
    #[serde(default)]
    pub partitions: Option<u32>,
    #[serde(default)]
    pub replication: Option<u32>,
    /// `TUMBLING`, `HOPPING`, `SESSION` for windowed sources.
    #[serde(default)]
    pub window_type: Option<String>,
    /// The `CREATE` statement as the server stores it.
    #[serde(default)]
    pub statement: String,
    #[serde(default)]
    pub fields: Vec<FieldInfo>,
    /// Persistent queries reading from it.
    #[serde(default)]
    pub read_queries: Vec<QueryInfo>,
    /// Persistent queries writing into it.
    #[serde(default)]
    pub write_queries: Vec<QueryInfo>,
    /// Runtime statistics text (`EXTENDED` only).
    #[serde(default)]
    pub statistics: String,
    #[serde(default)]
    pub error_stats: String,
    #[serde(default)]
    pub extended: bool,
}

/// A column of a query result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    /// The ksqlDB type, e.g. `STRING`, `BIGINT`, `ARRAY<STRING>`.
    #[serde(rename = "type")]
    pub type_name: String,
}

/// The first line of a `/query-stream` response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QueryHeader {
    /// The server's id for the query, used to close it.
    pub query_id: String,
    pub columns: Vec<Column>,
}

/// Why a query stopped returning rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryEnd {
    /// The server ended it: a pull query's last row, or a push query's
    /// `LIMIT`.
    Completed,
    /// The client's row limit was reached; the query was closed.
    RowLimit,
    /// The client's time limit was reached; the query was closed.
    TimeLimit,
    /// The caller cancelled it (Ctrl+C); the query was closed.
    Cancelled,
}

/// The rows a query returned, with their columns.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct QueryResult {
    pub query_id: String,
    /// A push query (`EMIT CHANGES` / `EMIT FINAL`).
    pub push: bool,
    pub columns: Vec<Column>,
    /// One value per column, in column order.
    pub rows: Vec<Vec<Value>>,
    pub end: QueryEnd,
}

/// The server's answer to one statement sent to `/ksql`.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CommandResult {
    /// The response entity's `@type` (`currentStatus`, `streams`,
    /// `sourceDescription`...), or `none` when the server returned none
    /// (`INSERT ... VALUES`).
    pub entity: String,
    /// `SUCCESS`, `QUEUED`, `EXECUTING`... for commands; empty otherwise.
    pub status: String,
    /// The server's message, e.g. `Stream created`.
    pub message: String,
    /// The persistent query a `CREATE ... AS SELECT` or `INSERT ... SELECT`
    /// started.
    pub query_id: Option<String>,
    pub warnings: Vec<String>,
    /// The whole entity, for statements without a typed form (`SHOW`,
    /// `DESCRIBE`, `EXPLAIN` sent through `/ksql`).
    pub raw: Value,
}

impl CommandResult {
    /// Reads the first entity of a `/ksql` response (an empty array means
    /// the statement ran without a result entity).
    pub(crate) fn from_entities(entities: &[Value]) -> Self {
        let Some(entity) = entities.first() else {
            return Self {
                entity: "none".to_string(),
                status: "SUCCESS".to_string(),
                message: String::new(),
                query_id: None,
                warnings: Vec::new(),
                raw: Value::Null,
            };
        };
        let text = |v: Option<&Value>| v.and_then(Value::as_str).unwrap_or_default().to_string();
        let status = entity.get("commandStatus");
        Self {
            entity: text(entity.get("@type")),
            status: text(status.and_then(|s| s.get("status"))),
            message: text(status.and_then(|s| s.get("message"))),
            query_id: status
                .and_then(|s| s.get("queryId"))
                .and_then(Value::as_str)
                .map(str::to_string),
            warnings: warnings(entity),
            raw: entity.clone(),
        }
    }
}

/// The `warnings[].message` of a response entity.
pub(crate) fn warnings(entity: &Value) -> Vec<String> {
    entity
        .get("warnings")
        .and_then(Value::as_array)
        .map(|list| {
            list.iter()
                .filter_map(|w| w.get("message").and_then(Value::as_str))
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn describe_reads_nested_types() {
        let d: SourceDescription = serde_json::from_value(json!({
            "name": "KQ_ORDERS", "type": "STREAM", "topic": "kq_orders",
            "keyFormat": "KAFKA", "valueFormat": "JSON", "partitions": 1, "replication": 1,
            "statement": "CREATE STREAM ...", "windowType": null, "extended": false,
            "fields": [
                {"name": "ID", "schema": {"type": "INTEGER", "fields": null, "memberSchema": null}, "type": "KEY"},
                {"name": "TAGS", "schema": {"type": "ARRAY", "memberSchema": {"type": "STRING"}}},
                {"name": "ADDR", "schema": {"type": "STRUCT", "fields": [
                    {"name": "ZIP", "schema": {"type": "INTEGER"}}]}},
                {"name": "M", "schema": {"type": "MAP", "memberSchema": {"type": "DOUBLE"}}}
            ],
            "readQueries": [{"id": "CTAS_X_1", "queryString": "CREATE ...", "sinks": ["X"],
                             "statusCount": {"RUNNING": 1}, "queryType": "PERSISTENT", "state": "RUNNING"}],
            "writeQueries": [], "unknownField": 1
        }))
        .unwrap();
        let types: Vec<String> = d.fields.iter().map(|f| f.schema.render()).collect();
        assert_eq!(
            types,
            [
                "INTEGER",
                "ARRAY<STRING>",
                "STRUCT<ZIP INTEGER>",
                "MAP<STRING, DOUBLE>"
            ]
        );
        assert_eq!(d.fields[0].column_type.as_deref(), Some("KEY"));
        assert_eq!(d.read_queries[0].status_count["RUNNING"], 1);
        // Ferrika's own output is snake_case.
        let out = serde_json::to_value(&d).unwrap();
        assert!(out.get("key_format").is_some(), "{out}");
        assert_eq!(out["kind"], "STREAM");
    }

    #[test]
    fn command_results_read_the_status_and_tolerate_no_entity() {
        let r = CommandResult::from_entities(&[json!({
            "@type": "currentStatus",
            "commandStatus": {"status": "SUCCESS", "message": "Created query with ID CTAS_T_1", "queryId": "CTAS_T_1"},
            "warnings": [{"message": "w1"}]
        })]);
        assert_eq!(r.entity, "currentStatus");
        assert_eq!(r.status, "SUCCESS");
        assert_eq!(r.query_id.as_deref(), Some("CTAS_T_1"));
        assert_eq!(r.warnings, ["w1"]);
        let r = CommandResult::from_entities(&[]);
        assert_eq!((r.entity.as_str(), r.status.as_str()), ("none", "SUCCESS"));
    }
}
