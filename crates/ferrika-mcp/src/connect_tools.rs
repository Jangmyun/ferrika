//! Kafka Connect read tools: the connectors of the Connect cluster of the
//! profile the MCP server runs with (`connect_url`), with their state,
//! tasks and failure traces, and one connector's config and offsets. A URL
//! cannot be passed, so the profile's Connect login only ever goes to the
//! profile's Connect cluster.
//!
//! Sensitive config values (passwords, tokens, `sasl.jaas.config`) are
//! masked as `ferrika connect status` masks them. Changing connectors
//! (stop, offsets, configs) is left to the CLI.

use std::time::Duration;

use serde_json::{json, Value};

use ferrika_core::error::{KafkaError, KafkaResult};
use ferrika_core::kafka::connect::{display_value, ConnectClient, ConnectorView};
use ferrika_core::kafka::context::ResolvedContext;

/// The Kafka Connect read tools.
pub(super) const CONNECT_TOOLS: [&str; 2] = ["connect_list", "connect_status"];

/// Most connectors `connect_list` returns.
const LIST_MAX: usize = 500;
/// How long one Connect request may take.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Longest failure trace returned (the start, where the cause is named).
const TRACE_MAX: usize = 4000;

/// The tools' definitions for `tools/list`.
pub(super) fn definitions() -> Vec<Value> {
    let read_only = json!({ "readOnlyHint": true, "openWorldHint": false });
    vec![
        json!({
            "name": "connect_list",
            "description": "Kafka Connect: the connectors of the profile's Connect cluster (connect_url) with their type, state (RUNNING, PAUSED, STOPPED, FAILED, ...), class and tasks; failed tasks and connectors include the start of their stack trace.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "filter": { "type": "string", "description": "Only connectors whose name contains this text" }
                }
            },
            "annotations": read_only
        }),
        json!({
            "name": "connect_status",
            "description": "Kafka Connect: one connector's state, tasks with failure traces, config (secret values masked) and offsets (Kafka Connect 3.5+; sink connectors by topic and partition, source connectors as the maps they store).",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "name": { "type": "string", "description": "Connector name" }
                },
                "required": ["name"]
            },
            "annotations": read_only
        }),
    ]
}

/// The Connect client of the profile the MCP server runs with.
fn client(context: &ResolvedContext) -> KafkaResult<ConnectClient> {
    let url = context
        .config
        .connect_url
        .as_deref()
        .filter(|u| !u.trim().is_empty())
        .ok_or_else(|| {
            KafkaError::config(
                "No Kafka Connect URL: start the MCP server with a profile that has one \
                 (`ferrika profile add NAME -b BROKERS --connect URL`)",
            )
        })?;
    Ok(ConnectClient::new(url)?.with_timeout(CONNECT_TIMEOUT))
}

/// Runs Connect tool `name`.
pub(super) async fn call(
    name: &str,
    args: &Value,
    context: &ResolvedContext,
) -> KafkaResult<Value> {
    let text = |key: &str| {
        args.get(key)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
    };
    let client = client(context)?;
    match name {
        "connect_list" => {
            let filter = text("filter");
            let connectors: Vec<ConnectorView> = client
                .list()
                .await?
                .into_iter()
                .filter(|c| filter.is_none_or(|f| c.name.contains(f)))
                .collect();
            let shown: Vec<Value> = connectors
                .iter()
                .take(LIST_MAX)
                .map(|c| connector_json(c, false))
                .collect();
            Ok(json!({
                "count": connectors.len(),
                "truncated": connectors.len() > LIST_MAX,
                "connectors": shown,
            }))
        }
        "connect_status" => {
            let wanted = text("name").ok_or_else(|| KafkaError::config("'name' is required"))?;
            let connector = client
                .list()
                .await?
                .into_iter()
                .find(|c| c.name == wanted)
                .ok_or_else(|| KafkaError::Admin(format!("No connector named '{wanted}'")))?;
            let mut out = connector_json(&connector, true);
            // Older workers (before 3.5) cannot show offsets: say so
            // instead of failing the whole status.
            match client.offsets(wanted).await {
                Ok(offsets) => out["offsets"] = json!(offsets),
                Err(e) => out["offsets_error"] = json!(e.to_string()),
            }
            Ok(out)
        }
        other => Err(KafkaError::config(format!("Unknown tool '{other}'"))),
    }
}

/// A connector as the tools return it; `with_config` adds its config with
/// sensitive values masked.
fn connector_json(c: &ConnectorView, with_config: bool) -> Value {
    let tasks: Vec<Value> = c
        .tasks
        .iter()
        .map(|t| {
            let mut task = json!({ "id": t.id, "state": t.state, "worker": t.worker });
            if let Some(trace) = &t.trace {
                task["trace"] = json!(clip(trace));
            }
            task
        })
        .collect();
    let mut out = json!({
        "name": c.name,
        "type": c.kind,
        "state": c.state,
        "worker": c.worker,
        "class": c.class,
        "running_tasks": c.running_tasks(),
        "tasks": tasks,
    });
    if let Some(trace) = &c.trace {
        out["trace"] = json!(clip(trace));
    }
    if with_config {
        let config: serde_json::Map<String, Value> = c
            .config
            .iter()
            .map(|(k, v)| (k.clone(), json!(display_value(k, v))))
            .collect();
        out["config"] = Value::Object(config);
    }
    out
}

/// The start of a stack trace, at most [`TRACE_MAX`] bytes.
fn clip(trace: &str) -> String {
    if trace.len() <= TRACE_MAX {
        return trace.to_string();
    }
    let mut end = TRACE_MAX;
    while !trace.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}...", &trace[..end])
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrika_core::kafka::connect::TaskView;

    fn view() -> ConnectorView {
        ConnectorView {
            name: "jdbc-sink".into(),
            kind: "sink".into(),
            state: "RUNNING".into(),
            worker: "w1:8083".into(),
            trace: None,
            class: "JdbcSink".into(),
            config: [("connection.password", "hunter2"), ("topics", "orders")]
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            tasks: vec![TaskView {
                id: 0,
                state: "FAILED".into(),
                worker: "w1:8083".into(),
                trace: Some("x".repeat(TRACE_MAX + 10)),
            }],
        }
    }

    #[test]
    fn connectors_show_tasks_traces_and_masked_config() {
        let listed = connector_json(&view(), false);
        assert!(listed.get("config").is_none());
        assert_eq!(listed["tasks"][0]["state"], "FAILED");
        let trace = listed["tasks"][0]["trace"].as_str().unwrap();
        assert!(trace.ends_with("...") && trace.len() == TRACE_MAX + 3);
        let status = connector_json(&view(), true);
        assert_eq!(status["config"]["topics"], "orders");
        assert_eq!(status["config"]["connection.password"], "********");
        assert!(!status.to_string().contains("hunter2"));
    }

    #[test]
    fn definitions_are_read_only() {
        let defs = definitions();
        let names: Vec<&str> = defs.iter().filter_map(|d| d["name"].as_str()).collect();
        assert_eq!(names, CONNECT_TOOLS);
        assert!(defs
            .iter()
            .all(|d| d["annotations"]["readOnlyHint"] == true));
    }

    #[tokio::test]
    async fn a_profile_without_connect_is_refused_before_connecting() {
        let context = crate::protocol::tests::context();
        let e = call("connect_list", &json!({}), &context)
            .await
            .unwrap_err()
            .to_string();
        assert!(e.contains("No Kafka Connect URL"), "{e}");
    }
}
