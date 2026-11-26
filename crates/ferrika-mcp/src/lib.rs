//! A Model Context Protocol server on stdin/stdout (`ferrika mcp`), so AI
//! assistants (Claude Code, Claude Desktop, IDE agents) can inspect a cluster.
//!
//! By default every tool is read-only: nothing creates, changes or deletes
//! cluster state, and peeked or previewed records never join or commit to a
//! consumer group. Record text is PII-masked unless the caller turns masking
//! off. RabbitMQ tools (preview) list queues, exchanges and bindings of a
//! RabbitMQ profile without reading message bodies. ksqlDB tools (preview)
//! list and describe the profile's ksqlDB server's streams and tables and
//! run bounded `SELECT` queries (rows PII-masked, through the export gate).
//!
//! [`WriteOptions`] add write tools (`write_tools.rs`). Each write is
//! planned, then confirmed, then run, and passes the same company policy,
//! read-only profile and audit gate as the CLI and the GUI (`guard.rs`).
//! The RabbitMQ write tools (`rabbit_writes.rs`) peek at and inspect
//! queues (reading requeues, so it is a write), publish, and with the
//! `amqp` feature move dead-lettered messages back. `ksql_exec`
//! (`ksql_writes.rs`) runs ksqlDB statements on the profile's ksqlDB
//! server, gated per statement like `ferrika ksql exec`.

mod backend;
mod guard;
mod ksql_tools;
mod ksql_writes;
mod protocol;
mod rabbit_tools;
mod rabbit_writes;
mod read_tools;
mod transform_tools;
mod transport;
mod write_tools;

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;

use ferrika_core::error::{KafkaError, KafkaResult};
use ferrika_core::kafka::context::ResolvedContext;

pub use guard::WRITE_TOOLS;

/// The write tools to offer and their limits. Without them the server only
/// reads.
#[derive(Debug, Clone)]
pub struct WriteOptions {
    /// The write tools to offer; empty means all of [`WRITE_TOOLS`].
    pub allow_tools: Vec<String>,
    /// Topics the write tools may change (globs); empty means every topic
    /// except internal ones (`__*`).
    pub topics: Vec<String>,
    /// RabbitMQ queues the `rabbit_*` write tools may read from, move out
    /// of or publish to through the default exchange (globs); empty means
    /// every queue.
    pub queues: Vec<String>,
    /// RabbitMQ exchanges `rabbit_publish` and `rabbit_dlq_redrive` may
    /// publish to (globs, `amq.default` for the default exchange); empty
    /// means every exchange except the broker's own `amq.rabbitmq.*`.
    pub exchanges: Vec<String>,
    /// How a write is confirmed before it runs.
    pub confirm: ConfirmMode,
    /// Most write executions per minute.
    pub max_writes_per_minute: u32,
    /// Most records produce_messages and transform_execute may write in
    /// this session.
    pub max_produce_records: u64,
    /// Most records one offset reset may skip or re-read without a person
    /// confirming it.
    pub max_offset_delta: u64,
}

/// How write tools are confirmed. There is deliberately no "none".
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ConfirmMode {
    /// Elicitation when the client supports it, else the plan's token.
    #[default]
    Auto,
    /// Always ask the user; clients without elicitation get no write tools.
    Elicit,
    /// The token from the plan (the agent can confirm on its own).
    Token,
}

/// An MCP server for the cluster `context` resolved.
pub struct McpServer<'a> {
    context: &'a ResolvedContext,
    writes: Option<write_tools::WriteTools>,
}

impl<'a> McpServer<'a> {
    /// A server with the write tools of `writes`, or read-only without.
    pub fn new(context: &'a ResolvedContext, writes: Option<&WriteOptions>) -> Self {
        Self {
            context,
            writes: writes.map(|w| write_tools::WriteTools::from_options(w, context)),
        }
    }

    /// Why write tools that were asked for are not offered (company policy,
    /// a read-only profile), one line each, for the log.
    pub fn startup_warnings(&self) -> Vec<String> {
        self.writes
            .as_ref()
            .map(write_tools::WriteTools::startup_warnings)
            .unwrap_or_default()
    }

    /// Serves one client on stdin/stdout until stdin ends.
    pub async fn serve_stdio(self) -> KafkaResult<()> {
        serve(
            tokio::io::stdin(),
            tokio::io::stdout(),
            self.context,
            self.writes,
        )
        .await
    }
}

/// Serves one client on `input`/`output` until `input` ends. Requests still
/// queued then are answered before returning.
async fn serve<R, W>(
    input: R,
    output: W,
    context: &ResolvedContext,
    writes: Option<write_tools::WriteTools>,
) -> KafkaResult<()>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let (out_tx, out_rx) = mpsc::unbounded_channel();
    let writer = tokio::spawn(transport::write_loop(output, out_rx));
    let peer = transport::Peer::new(out_tx);
    let (requests_tx, mut requests) = mpsc::unbounded_channel();
    let reader = tokio::spawn(transport::read_loop(input, peer.clone(), requests_tx));

    let mut session = protocol::Session::new(context, peer.clone(), writes);
    while let Some((id, method, params)) = requests.recv().await {
        // A request cancelled while queued is not run; one cancelled while
        // running gets no response.
        if peer.take_cancelled(&id) {
            continue;
        }
        let response = session.handle(id.clone(), &method, params).await;
        if !peer.take_cancelled(&id) {
            peer.send(response);
        }
    }
    session.finish();
    drop(session);
    let read = reader
        .await
        .map_err(|e| KafkaError::Io(format!("MCP reader stopped: {e}")))?;
    // The writer ends once every sender is gone.
    drop(peer);
    let written = writer
        .await
        .map_err(|e| KafkaError::Io(format!("MCP writer stopped: {e}")))?;
    read.and(written)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    /// Runs the server over in-memory pipes with `lines` as the client's
    /// input, returning every line the server wrote.
    async fn exchange(lines: &[&str]) -> Vec<Value> {
        let (mut client_in, server_in) = tokio::io::duplex(64 * 1024);
        let (server_out, client_out) = tokio::io::duplex(64 * 1024);
        for line in lines {
            client_in
                .write_all(format!("{line}\n").as_bytes())
                .await
                .unwrap();
        }
        drop(client_in);
        let g = protocol::tests::context();
        serve(server_in, server_out, &g, None).await.unwrap();
        let mut out = Vec::new();
        let mut reader = BufReader::new(client_out).lines();
        while let Some(line) = reader.next_line().await.unwrap() {
            out.push(serde_json::from_str(&line).unwrap());
        }
        out
    }

    #[tokio::test]
    async fn serves_requests_and_ignores_client_responses() {
        let out = exchange(&[
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{"elicitation":{}},"clientInfo":{"name":"t","version":"1"}}}"#,
            r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#,
            // A response to a request the server never sent.
            r#"{"jsonrpc":"2.0","id":"ferrika-42","result":{"action":"accept"}}"#,
            r#"{"jsonrpc":"2.0","id":2,"method":"tools/list"}"#,
            r#"{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":3}}"#,
            r#"{"jsonrpc":"2.0","id":3,"method":"tools/list"}"#,
            r#"{"jsonrpc":"2.0","id":4,"method":"ping"}"#,
            "not json",
        ])
        .await;
        let ids: Vec<Value> = out.iter().map(|m| m["id"].clone()).collect();
        // Ping and the parse error are answered by the reader, so they may
        // come before the queued requests; id 3 was cancelled.
        assert_eq!(out.len(), 4, "{out:?}");
        for id in [1, 2, 4] {
            assert!(ids.contains(&Value::from(id)), "{out:?}");
        }
        assert!(!ids.contains(&Value::from(3)));
        assert!(!ids.contains(&Value::from("ferrika-42")));
        assert!(out
            .iter()
            .any(|m| m["id"].is_null() && m["error"]["code"] == -32700));
        assert!(out.iter().all(|m| m["error"]["code"] != -32601));
    }

    async fn next(lines: &mut tokio::io::Lines<BufReader<tokio::io::DuplexStream>>) -> Value {
        let line = lines.next_line().await.unwrap().unwrap();
        serde_json::from_str(&line).unwrap()
    }

    /// A client with elicitation runs an execute, pings while Ferrika waits
    /// for the user, then accepts.
    #[tokio::test]
    async fn waits_for_the_user_while_answering_ping() {
        use super::backend::fake::FakeCluster;
        use super::guard::tests::config;

        let (tools, cluster, log, _) = write_tools::tests::tools_with(
            FakeCluster {
                brokers: 1,
                ..Default::default()
            },
            config(),
        );
        let (mut client_in, server_in) = tokio::io::duplex(64 * 1024);
        let (server_out, client_out) = tokio::io::duplex(64 * 1024);
        let g = protocol::tests::context();
        let server = serve(server_in, server_out, &g, Some(tools));
        let client = async move {
            let mut lines = BufReader::new(client_out).lines();
            for line in [
                r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{"elicitation":{}},"clientInfo":{"name":"t","version":"1"}}}"#,
                r#"{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"create_topic","arguments":{"topic":"t","mode":"execute"}}}"#,
            ] {
                client_in
                    .write_all(format!("{line}\n").as_bytes())
                    .await
                    .unwrap();
            }
            let init = next(&mut lines).await;
            assert_eq!(init["id"], 1);
            let ask = next(&mut lines).await;
            assert_eq!(ask["method"], "elicitation/create");
            client_in
                .write_all(b"{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\"ping\"}\n")
                .await
                .unwrap();
            let pong = next(&mut lines).await;
            assert_eq!(pong["id"], 3);
            let answer = serde_json::json!({
                "jsonrpc": "2.0",
                "id": ask["id"],
                "result": { "action": "accept", "content": { "confirm": true } }
            });
            client_in
                .write_all(format!("{answer}\n").as_bytes())
                .await
                .unwrap();
            let done = next(&mut lines).await;
            assert_eq!(done["id"], 2);
            assert_eq!(
                done["result"]["structuredContent"]["executed"], true,
                "{done}"
            );
            drop(client_in);
        };
        let (served, ()) = tokio::join!(server, client);
        served.unwrap();
        assert_eq!(*cluster.changes.borrow(), ["create t 1 -1"]);
        let actions: Vec<String> = log
            .entries
            .borrow()
            .iter()
            .map(|e| e.action.clone())
            .collect();
        assert_eq!(
            actions,
            ["mcp.session_start", "mcp create_topic", "mcp.session_end"]
        );
        assert!(log.entries.borrow()[0]
            .outcome
            .starts_with("confirm auto (elicitation)"));
    }
}
