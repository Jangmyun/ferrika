//! JSON-RPC framing and the MCP methods: `initialize`, `ping`, `tools/*`.

use serde_json::{json, Value};

use super::read_tools::{call_tool, peek_listed, tool_definitions};
use super::transport::Peer;
use super::write_tools::{self, WriteTools};
use ferrika_core::kafka::context::ResolvedContext;
use ferrika_governance::policy;

/// Protocol revisions this server speaks, newest first.
const PROTOCOL_VERSIONS: [&str; 4] = ["2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05"];

/// One line from the client, sorted.
#[derive(Debug, PartialEq)]
pub(super) enum Incoming {
    /// A line the server answers at once without handling (parse error,
    /// batch).
    Reply(Value),
    Request {
        id: Value,
        method: String,
        params: Value,
    },
    /// No id: gets no response.
    Notification { method: String, params: Value },
    /// The client's answer to a request the server sent (an id, no method).
    Response(Value),
}

/// Sorts one JSON-RPC message.
pub(super) fn classify(line: &str) -> Incoming {
    let message: Value = match serde_json::from_str(line) {
        Ok(v) => v,
        Err(e) => {
            return Incoming::Reply(rpc_error(Value::Null, -32700, &format!("Parse error: {e}")))
        }
    };
    // Batches were removed from MCP (2025-06-18); say so instead of
    // treating the array as a notification.
    if message.is_array() {
        return Incoming::Reply(rpc_error(
            Value::Null,
            -32600,
            "Batch requests are not supported",
        ));
    }
    let method = message.get("method").and_then(Value::as_str);
    let params = message.get("params").cloned().unwrap_or(Value::Null);
    match (message.get("id").cloned(), method) {
        (Some(id), Some(method)) => Incoming::Request {
            id,
            method: method.to_string(),
            params,
        },
        (Some(_), None) => Incoming::Response(message),
        (None, method) => Incoming::Notification {
            method: method.unwrap_or_default().to_string(),
            params,
        },
    }
}

pub(super) fn rpc_result(id: Value, result: Value) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "result": result })
}

pub(super) fn rpc_error(id: Value, code: i64, message: &str) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } })
}

/// What the client said about itself in `initialize`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct ClientInfo {
    pub name: Option<String>,
    pub version: Option<String>,
    /// Whether it declared form-mode elicitation (`capabilities.elicitation`
    /// as `{}` or with `form`).
    pub elicitation: bool,
}

impl ClientInfo {
    fn from_initialize(params: &Value) -> Self {
        let info = params.get("clientInfo");
        let text = |key: &str| {
            info.and_then(|i| i.get(key))
                .and_then(Value::as_str)
                .map(str::to_string)
        };
        let elicitation = params
            .get("capabilities")
            .and_then(|c| c.get("elicitation"))
            .and_then(Value::as_object)
            .is_some_and(|e| e.is_empty() || e.contains_key("form"));
        Self {
            name: text("name"),
            version: text("version"),
            elicitation,
        }
    }
}

/// One MCP connection: the client's requests are handled here, in order.
pub(super) struct Session<'a> {
    context: &'a ResolvedContext,
    pub peer: Peer,
    pub client: ClientInfo,
    /// `None` without `--allow-write`.
    pub writes: Option<WriteTools>,
}

impl<'a> Session<'a> {
    pub(super) fn new(
        context: &'a ResolvedContext,
        peer: Peer,
        writes: Option<WriteTools>,
    ) -> Self {
        Self {
            context,
            peer,
            client: ClientInfo::default(),
            writes,
        }
    }

    /// Records the end of a session that could write.
    pub(super) fn finish(&self) {
        if let Some(writes) = &self.writes {
            writes.guard.record_session_end();
        }
    }

    /// Read tools, then the write tools on offer.
    fn tools(&self) -> Value {
        let with_peek = peek_listed(policy::current(), self.context);
        let mut tools = match tool_definitions(with_peek) {
            Value::Array(tools) => tools,
            _ => Vec::new(),
        };
        if let Some(writes) = &self.writes {
            tools.extend(writes.definitions(self.client.elicitation));
        }
        Value::Array(tools)
    }

    /// The response to one client request.
    pub(super) async fn handle(&mut self, id: Value, method: &str, params: Value) -> Value {
        let result = match method {
            "initialize" => Ok(self.initialize(&params)),
            "ping" => Ok(json!({})),
            "tools/list" => Ok(json!({ "tools": self.tools() })),
            "tools/call" => self.call(&params, &id).await,
            _ => Err((-32601, format!("Method not found: {method}"))),
        };
        match result {
            Ok(result) => rpc_result(id, result),
            Err((code, msg)) => rpc_error(id, code, &msg),
        }
    }

    fn initialize(&mut self, params: &Value) -> Value {
        self.client = ClientInfo::from_initialize(params);
        if let Some(writes) = &mut self.writes {
            writes
                .guard
                .set_client(self.client.name.as_deref(), self.client.version.as_deref());
            let label = writes.confirm_label(self.client.elicitation);
            writes.guard.record_session_start(label);
        }
        let mut instructions =
            "Access to one Kafka cluster (the profile Ferrika was started with). \
            Start with cluster_health or list_topics; message_histogram shows when a topic's \
            records arrived (bursts, gaps). peek_messages masks emails, card numbers \
            and tokens unless mask_pii is false. transform_preview shows what a transform's \
            steps (filter, drop, rename, set, rekey, mask, sample) would write, without \
            writing. rabbit_queues, rabbit_exchanges and rabbit_bindings list a RabbitMQ \
            profile's virtual host (preview); reading RabbitMQ messages requeues them, so \
            rabbit_peek and rabbit_dlq_inspect are write tools."
                .to_string();
        match &self.writes {
            Some(writes) => instructions.push_str(&writes.instructions(self.client.elicitation)),
            None => instructions.push_str(" Read-only: no tool changes the cluster."),
        }
        let requested = params.get("protocolVersion").and_then(Value::as_str);
        let version = requested
            .filter(|v| PROTOCOL_VERSIONS.contains(v))
            .unwrap_or(PROTOCOL_VERSIONS[0]);
        json!({
            "protocolVersion": version,
            "capabilities": { "tools": {} },
            "serverInfo": { "name": "ferrika", "version": env!("CARGO_PKG_VERSION") },
            "instructions": instructions
        })
    }

    /// `tools/call`. An unknown tool name is a protocol error (-32602);
    /// a tool's own failures are results with `isError`.
    async fn call(&mut self, params: &Value, id: &Value) -> Result<Value, (i64, String)> {
        let name = params.get("name").and_then(Value::as_str).unwrap_or("");
        if write_tools::is_write_tool(name) {
            let args = params.get("arguments").cloned().unwrap_or(json!({}));
            return Ok(match &mut self.writes {
                Some(writes) => {
                    let ctx = write_tools::CallContext {
                        peer: &self.peer,
                        request_id: id,
                        elicitation: self.client.elicitation,
                    };
                    writes.call(name, &args, &ctx).await
                }
                None => write_tools::disabled(name),
            });
        }
        // A tool hidden by policy is still known: `call_tool` refuses it
        // and records the attempt.
        let known = tool_definitions(true)
            .as_array()
            .is_some_and(|tools| tools.iter().any(|t| t["name"] == name));
        if !known {
            return Err((-32602, format!("Unknown tool: {name}")));
        }
        // Inside an --allow-write session the write guard names the client;
        // the read tools' audit entries carry the same actor.
        let actor = self.writes.as_ref().map(|w| w.guard.actor());
        Ok(call_tool(
            params,
            self.context,
            policy::current(),
            &policy::AuditLog,
            actor,
        )
        .await)
    }
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;

    /// Handles one line as the server loop would; `None` when nothing is
    /// sent back.
    async fn handle_line(session: &mut Session<'_>, line: &str) -> Option<Value> {
        match classify(line) {
            Incoming::Reply(reply) => Some(reply),
            Incoming::Request { id, method, params } => {
                Some(session.handle(id, &method, params).await)
            }
            Incoming::Notification { .. } | Incoming::Response(_) => None,
        }
    }

    fn session(g: &ResolvedContext) -> Session<'_> {
        Session::new(g, Peer::new(tokio::sync::mpsc::unbounded_channel().0), None)
    }

    /// The default profile store with an unreachable broker.
    pub(crate) fn context() -> ResolvedContext {
        let selection = ferrika_core::kafka::context::ClusterSelection {
            profile: None,
            bootstrap_servers: Some("127.0.0.1:1".to_string()),
            timeout: std::time::Duration::from_secs(30),
        };
        ResolvedContext::from_store(
            &selection,
            ferrika_core::kafka::profile::ProfileStore::default(),
        )
        .expect("the default store resolves")
    }

    #[tokio::test]
    async fn speaks_the_mcp_handshake_and_lists_tools() {
        let g = context();
        let s = &mut session(&g);
        let init = handle_line(
            s,
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"t","version":"1"}}}"#,
        )
        .await
        .unwrap();
        assert_eq!(init["result"]["protocolVersion"], "2025-03-26");
        assert_eq!(init["result"]["serverInfo"]["name"], "ferrika");

        assert!(handle_line(
            s,
            r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#
        )
        .await
        .is_none());

        let tools = handle_line(s, r#"{"jsonrpc":"2.0","id":2,"method":"tools/list"}"#)
            .await
            .unwrap();
        let names: Vec<&str> = tools["result"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|t| t["name"].as_str())
            .collect();
        assert!(names.contains(&"peek_messages") && names.contains(&"cluster_health"));

        let unknown = handle_line(s, r#"{"jsonrpc":"2.0","id":3,"method":"nope"}"#)
            .await
            .unwrap();
        assert_eq!(unknown["error"]["code"], -32601);

        let batch = handle_line(s, r#"[{"jsonrpc":"2.0","id":5,"method":"ping"}]"#)
            .await
            .unwrap();
        assert_eq!(batch["error"]["code"], -32600);

        let bad = handle_line(
            s,
            r#"{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"describe_topic","arguments":{}}}"#,
        )
        .await
        .unwrap();
        assert_eq!(bad["result"]["isError"], true);

        let unknown_tool = handle_line(
            s,
            r#"{"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"drop_cluster","arguments":{}}}"#,
        )
        .await
        .unwrap();
        assert_eq!(unknown_tool["error"]["code"], -32602);

        // Without --allow-write no write tool is listed, and calling one says why.
        assert!(!names.iter().any(|n| write_tools::is_write_tool(n)));
        let write = handle_line(
            s,
            r#"{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"create_topic","arguments":{"topic":"t"}}}"#,
        )
        .await
        .unwrap();
        assert_eq!(write["result"]["isError"], true);
        assert_eq!(
            write["result"]["structuredContent"]["error"]["code"],
            "write_disabled"
        );
    }

    #[test]
    fn classify_sorts_requests_responses_and_notifications() {
        assert!(matches!(
            classify(r#"{"jsonrpc":"2.0","id":"ferrika-1","result":{"action":"accept"}}"#),
            Incoming::Response(_)
        ));
        assert!(matches!(
            classify(
                r#"{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":3}}"#
            ),
            Incoming::Notification { .. }
        ));
        assert!(matches!(classify("{nope"), Incoming::Reply(_)));
    }

    #[tokio::test]
    async fn initialize_keeps_client_info_and_negotiates_2025_11_25() {
        let g = context();
        let s = &mut session(&g);
        let init = handle_line(
            s,
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{"elicitation":{}},"clientInfo":{"name":"claude-code","version":"2.1.4"}}}"#,
        )
        .await
        .unwrap();
        assert_eq!(init["result"]["protocolVersion"], "2025-11-25");
        assert_eq!(s.client.name.as_deref(), Some("claude-code"));
        assert_eq!(s.client.version.as_deref(), Some("2.1.4"));
        assert!(s.client.elicitation);
        // URL-only elicitation is not form mode.
        let url_only = ClientInfo::from_initialize(
            &json!({ "capabilities": { "elicitation": { "url": {} } } }),
        );
        assert!(!url_only.elicitation);
        let none = ClientInfo::from_initialize(&json!({ "capabilities": {} }));
        assert_eq!(none, ClientInfo::default());
    }
}
