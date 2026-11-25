//! ksqlDB read tools (preview), through `ferrika_ksql`: listings, `DESCRIBE`
//! and bounded queries on the ksqlDB server of the Kafka profile the MCP
//! server runs with (`ksql_url`). A URL cannot be passed, so the profile's
//! stored login only ever goes to the profile's server.
//!
//! `ksql_list` and `ksql_describe` return metadata. `ksql_query` returns
//! rows, which are record data: like `peek_messages` it passes the export
//! gate (channel `mcp`; hidden and refused under `export: forbidden`,
//! recorded under `export: audited`) and masks PII unless the caller turns
//! masking off and the policy does not enforce it. It runs one `SELECT`
//! only, with at most [`QUERY_MAX_ROWS`] rows and [`QUERY_MAX_SECS`]
//! seconds, and closes the query on the server when it stops. Statements
//! that change ksqlDB are not offered over MCP yet.

use std::time::Duration;

use serde_json::{json, Value};

use ferrika_core::error::{KafkaError, KafkaResult};
use ferrika_core::kafka::context::ResolvedContext;
use ferrika_governance::policy::{self, AuditSink, EffectivePolicy, ExportChannel, ExportRequest};
use ferrika_ksql::client::{KsqlClient, QueryLimits, Session};
use ferrika_ksql::config::KsqlConfig;
use ferrika_ksql::model::QueryResult;
use ferrika_ksql::statement::{Statement, StatementKind};

/// The ksqlDB read tools.
pub(super) const KSQL_TOOLS: [&str; 3] = ["ksql_list", "ksql_describe", "ksql_query"];

/// Most entries `ksql_list` returns.
const LIST_MAX: usize = 500;
/// Most rows `ksql_query` returns, and by default.
const QUERY_MAX_ROWS: u64 = 100;
const QUERY_DEFAULT_ROWS: u64 = 20;
/// Longest time `ksql_query` runs, and the default for push queries.
const QUERY_MAX_SECS: u64 = 30;
const QUERY_DEFAULT_SECS: u64 = 5;
/// How long one ksqlDB request may take.
const KSQL_TIMEOUT: Duration = Duration::from_secs(10);
/// Longest text value returned per cell.
const CELL_TEXT_MAX: usize = 2000;

/// The tools' definitions for `tools/list`; `ksql_query` only `with_query`
/// (not under `export: forbidden`).
pub(super) fn definitions(with_query: bool) -> Vec<Value> {
    let read_only = json!({ "readOnlyHint": true, "openWorldHint": false });
    let mut tools = vec![
        json!({
            "name": "ksql_list",
            "description": "ksqlDB (preview): streams, tables, persistent queries or Kafka topics of the ksqlDB server of the profile (ksql_url). Metadata only.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "kind": { "type": "string", "enum": ["streams", "tables", "queries", "topics"] },
                    "filter": { "type": "string", "description": "Only entries whose name (or query id) contains this text" }
                },
                "required": ["kind"]
            },
            "annotations": read_only
        }),
        json!({
            "name": "ksql_describe",
            "description": "ksqlDB (preview): columns and types, Kafka topic, key and value formats, and the persistent queries reading and writing a stream or table.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "name": { "type": "string", "description": "Stream or table name" },
                    "extended": { "type": "boolean", "default": false, "description": "Include runtime statistics" }
                },
                "required": ["name"]
            },
            "annotations": read_only
        }),
    ];
    if with_query {
        tools.push(json!({
            "name": "ksql_query",
            "description": "ksqlDB (preview): runs one SELECT on the profile's ksqlDB server and returns up to `limit` rows with the column names and types. A pull query returns what the server has now; a push query (EMIT CHANGES) stops after `limit` rows or `timeout_secs` and is closed on the server. Row values are PII-masked unless mask_pii is false. Statements that change ksqlDB are refused.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "sql": { "type": "string", "description": "One SELECT statement" },
                    "limit": { "type": "integer", "minimum": 1, "maximum": QUERY_MAX_ROWS, "default": QUERY_DEFAULT_ROWS },
                    "timeout_secs": { "type": "integer", "minimum": 1, "maximum": QUERY_MAX_SECS, "default": QUERY_DEFAULT_SECS, "description": "Most seconds to run (push queries wait for new rows until then)" },
                    "from_beginning": { "type": "boolean", "default": false, "description": "Push queries: read the source from its earliest records" },
                    "mask_pii": { "type": "boolean", "default": true, "description": "Mask emails, card numbers, SSNs and tokens" }
                },
                "required": ["sql"]
            },
            "annotations": read_only
        }));
    }
    tools
}

fn text_arg<'a>(args: &'a Value, name: &str) -> Option<&'a str> {
    args.get(name)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
}

fn ksql_error(e: ferrika_ksql::error::KsqlError) -> KafkaError {
    KafkaError::config(e.to_string())
}

/// The ksqlDB server of the profile the MCP server runs with.
pub(super) fn connection(context: &ResolvedContext) -> KafkaResult<KsqlConfig> {
    let profile = context
        .profile_name
        .as_deref()
        .and_then(|name| context.store.get(name))
        .ok_or_else(|| {
            KafkaError::config(
                "No ksqlDB server: start the MCP server with a profile that has one \
                 (`ferrika profile add NAME -b BROKERS --ksql-url URL`)",
            )
        })?;
    KsqlConfig::from_profile(profile)
        .map(|c| c.with_timeout(KSQL_TIMEOUT))
        .ok_or_else(|| {
            KafkaError::config(format!(
                "Profile '{}' has no ksqlDB server; add one with `ferrika profile add {} -b ... --ksql-url URL`",
                profile.name, profile.name
            ))
        })
}

/// Runs ksqlDB tool `name`. `ksql_query` passes the export gate first.
pub(super) async fn call(
    name: &str,
    args: &Value,
    context: &ResolvedContext,
    policy: &EffectivePolicy,
    sink: &impl AuditSink,
    actor: Option<&str>,
) -> KafkaResult<Value> {
    let config = connection(context)?;
    let client = KsqlClient::new(&config).map_err(ksql_error)?;
    let filter = text_arg(args, "filter");
    let keep = |name: &str| filter.is_none_or(|f| name.contains(f));
    let mut result = match name {
        "ksql_list" => match text_arg(args, "kind") {
            Some("streams") => listing("streams", client.streams().await, |s| keep(&s.name))?,
            Some("tables") => listing("tables", client.tables().await, |s| keep(&s.name))?,
            Some("queries") => listing("queries", client.queries().await, |q| keep(&q.id))?,
            Some("topics") => listing("topics", client.topics().await, |t| keep(&t.name))?,
            _ => {
                return Err(KafkaError::config(
                    "kind must be streams, tables, queries or topics",
                ))
            }
        },
        "ksql_describe" => {
            let name =
                text_arg(args, "name").ok_or_else(|| KafkaError::config("'name' is required"))?;
            let extended = args
                .get("extended")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let d = client.describe(name, extended).await.map_err(ksql_error)?;
            json!({ "description": d })
        }
        "ksql_query" => {
            let statement = query_statement(args)?;
            authorize_query(policy, sink, context, &client, &statement, actor)?;
            query(&client, &statement, args, policy).await?
        }
        other => return Err(KafkaError::config(format!("Unknown tool '{other}'"))),
    };
    result["profile"] = json!(context.profile_name);
    result["server"] = json!(client.address());
    Ok(result)
}

/// `{count, truncated, key: [...]}` of the entries `keep` keeps.
fn listing<T: serde::Serialize>(
    key: &str,
    items: ferrika_ksql::error::KsqlResult<Vec<T>>,
    keep: impl Fn(&T) -> bool,
) -> KafkaResult<Value> {
    let items = items.map_err(ksql_error)?;
    let kept: Vec<&T> = items.iter().filter(|i| keep(i)).collect();
    let mut out = json!({ "count": kept.len(), "truncated": kept.len() > LIST_MAX });
    out[key] = json!(kept.iter().take(LIST_MAX).collect::<Vec<_>>());
    Ok(out)
}

/// The one `SELECT` of `sql`; anything else is refused before it is sent.
fn query_statement(args: &Value) -> KafkaResult<Statement> {
    let sql = text_arg(args, "sql").ok_or_else(|| KafkaError::config("'sql' is required"))?;
    let statement = Statement::parse(sql).map_err(ksql_error)?;
    match statement.kind {
        StatementKind::Select { .. } => Ok(statement),
        _ => Err(KafkaError::config(format!(
            "ksql_query runs one SELECT; `{}` is not one (statements that change ksqlDB are not available over MCP)",
            statement.summary()
        ))),
    }
}

/// The export gate for `ksql_query`: the server's address and the
/// cluster's brokers are matched (`export_clusters`); the target names the
/// query's sources, never its literals.
fn authorize_query(
    policy: &EffectivePolicy,
    sink: &impl AuditSink,
    context: &ResolvedContext,
    client: &KsqlClient,
    statement: &Statement,
    actor: Option<&str>,
) -> KafkaResult<()> {
    let profile = context.profile_name.clone().unwrap_or_default();
    let brokers =
        policy::export_brokers(policy, &context.config.clone()).map_err(KafkaError::Check)?;
    let bootstrap = format!("{},{brokers}", client.address());
    let target = statement.summary();
    let request = ExportRequest {
        source: "mcp",
        channel: ExportChannel::Mcp,
        label: "ksql_query",
        target: &target,
        records: None,
        profile: &profile,
        bootstrap: &bootstrap,
        actor,
    };
    policy::authorize_export_with(policy, sink, &request)
        .map_err(|refusal| KafkaError::Check(refusal.to_string()))
}

async fn query(
    client: &KsqlClient,
    statement: &Statement,
    args: &Value,
    policy: &EffectivePolicy,
) -> KafkaResult<Value> {
    let limit = args
        .get("limit")
        .and_then(Value::as_u64)
        .unwrap_or(QUERY_DEFAULT_ROWS)
        .clamp(1, QUERY_MAX_ROWS);
    let secs = args
        .get("timeout_secs")
        .and_then(Value::as_u64)
        .unwrap_or(QUERY_DEFAULT_SECS)
        .clamp(1, QUERY_MAX_SECS);
    // A policy that enforces masking ignores mask_pii: false.
    let mask = args
        .get("mask_pii")
        .and_then(Value::as_bool)
        .unwrap_or(true)
        || policy.masking_enforced();
    let mut session = Session::default();
    if args.get("from_beginning").and_then(Value::as_bool) == Some(true) {
        session
            .properties
            .insert("auto.offset.reset".to_string(), "earliest".to_string());
    }
    let limits = QueryLimits {
        max_rows: limit as usize,
        timeout: Some(Duration::from_secs(secs)),
    };
    let result = client
        .run_query(
            &statement.text,
            &session,
            limits,
            std::future::pending(),
            |_, _| {},
        )
        .await
        .map_err(ksql_error)?;
    Ok(query_json(result, mask))
}

/// The tool result: columns, rows as objects (masked when `mask`, long
/// text shortened), how the query ended.
fn query_json(result: QueryResult, mask: bool) -> Value {
    let result = if mask { result.masked() } else { result };
    let rows: Vec<Value> = result
        .row_objects()
        .into_iter()
        .map(|mut row| {
            if let Some(map) = row.as_object_mut() {
                for value in map.values_mut() {
                    if let Value::String(s) = value {
                        *s = ferrika_core::codec::pii::ellipsize(s, CELL_TEXT_MAX);
                    }
                }
            }
            row
        })
        .collect();
    json!({
        "query_id": result.query_id,
        "push": result.push,
        "columns": result.columns,
        "count": rows.len(),
        "rows": rows,
        "end": result.end,
        "masked": mask,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrika_governance::test_policy::{self, MemoryLog};
    use ferrika_ksql::model::{Column, QueryEnd};

    #[test]
    fn query_only_takes_one_select() {
        assert!(query_statement(&json!({ "sql": "SELECT * FROM t;" })).is_ok());
        for sql in [
            "DROP STREAM t;",
            "INSERT INTO t (v) VALUES ('x');",
            "SELECT 1 FROM a; DROP STREAM b;",
            "PRINT t;",
            "SHOW STREAMS;",
        ] {
            assert!(query_statement(&json!({ "sql": sql })).is_err(), "{sql}");
        }
        assert!(query_statement(&json!({})).is_err());
    }

    #[test]
    fn query_tool_is_hidden_under_export_forbidden() {
        let names = |tools: Vec<Value>| -> Vec<String> {
            tools
                .iter()
                .filter_map(|t| t["name"].as_str().map(str::to_string))
                .collect()
        };
        assert_eq!(names(definitions(true)), KSQL_TOOLS);
        assert_eq!(names(definitions(false)), ["ksql_list", "ksql_describe"]);
        for tool in definitions(true) {
            assert_eq!(tool["annotations"]["readOnlyHint"], true);
        }
    }

    #[test]
    fn rows_are_masked_unless_turned_off() {
        let result = QueryResult {
            query_id: "q".into(),
            push: true,
            columns: vec![
                Column {
                    name: "EMAIL".into(),
                    type_name: "STRING".into(),
                },
                Column {
                    name: "NOTE".into(),
                    type_name: "STRING".into(),
                },
            ],
            rows: vec![vec![json!(test_policy::PII), json!("x".repeat(5000))]],
            end: QueryEnd::RowLimit,
        };
        let masked = query_json(result.clone(), true);
        assert!(!test_policy::leaks(&masked.to_string()), "{masked}");
        assert!(masked["rows"][0]["NOTE"].as_str().unwrap().len() < 2100);
        assert_eq!(masked["end"], "row_limit");
        let raw = query_json(result, false);
        assert!(test_policy::leaks(&raw.to_string()));
    }

    #[tokio::test]
    async fn the_export_gate_refuses_and_records_under_export_forbidden() {
        let context = crate::protocol::tests::context();
        let client = KsqlClient::new(&KsqlConfig::new("http://ksql.prod:8088")).unwrap();
        let statement =
            Statement::parse("SELECT * FROM kq_orders WHERE email = 'jane@example.com';").unwrap();
        let log = MemoryLog::default();
        let err = authorize_query(
            &test_policy::export_forbidden(),
            &log,
            &context,
            &client,
            &statement,
            Some("mcp:t/1#00"),
        )
        .unwrap_err();
        assert!(err.to_string().contains("export: forbidden"), "{err}");
        let entries = log.entries.take();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].action, "policy.blocked");
        assert_eq!(
            entries[0].target,
            "export:mcp ksql_query SELECT FROM KQ_ORDERS (pull query)"
        );
        assert!(!entries[0].target.contains("jane"));
        // Audited: recorded, then allowed.
        let log = MemoryLog::default();
        authorize_query(
            &test_policy::export_audited(),
            &log,
            &context,
            &client,
            &statement,
            None,
        )
        .unwrap();
        assert_eq!(log.entries.borrow()[0].action, "record.export");
    }

    #[tokio::test]
    async fn without_a_ksqldb_server_the_tools_say_how_to_add_one() {
        let context = crate::protocol::tests::context();
        let err = call(
            "ksql_list",
            &json!({ "kind": "streams" }),
            &context,
            &test_policy::none(),
            &MemoryLog::default(),
            None,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("--ksql-url"), "{err}");
    }
}
