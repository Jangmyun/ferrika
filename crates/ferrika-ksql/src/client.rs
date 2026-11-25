//! A client for the ksqlDB REST API.
//!
//! | Call | Request |
//! | --- | --- |
//! | [`KsqlClient::info`] | `GET /info` |
//! | listings, [`KsqlClient::describe`], [`KsqlClient::execute`] | `POST /ksql` with one statement |
//! | [`KsqlClient::query`], [`KsqlClient::run_query`] | `POST /query-stream` (delimited rows) |
//! | [`QueryStream::close`] | `POST /close-query`, then the connection is dropped |
//!
//! Everything goes over HTTP/1.1 with one request per connection. ksqlDB
//! serves `/query-stream` on HTTP/1.1 as a chunked response: a header line
//! (query id, column names and types), then one JSON array per row. A push
//! query runs until its `LIMIT`, until the client closes it, or until the
//! connection drops; the client closes it with `/close-query` and by
//! dropping the connection, since ksqlDB 0.29 answers a `/close-query` from
//! another HTTP/1.1 connection with an error even when it closed the query.
//!
//! The client sends exactly the statements it is given. The company policy
//! and the audit log are the caller's job: classify with
//! [`crate::statement`] and gate before calling [`KsqlClient::execute`].

use std::collections::BTreeMap;
use std::future::Future;
use std::time::Duration;

use serde::de::DeserializeOwned;
use serde_json::{json, Value};

use crate::config::KsqlConfig;
use crate::error::{KsqlError, KsqlResult};
use crate::http::{api_error, Body, Transport, MAX_BODY_BYTES};
use crate::model::{
    Column, CommandResult, QueryEnd, QueryHeader, QueryInfo, QueryResult, ServerInfo,
    SourceDescription, SourceInfo, TopicInfo,
};
use crate::statement::{Statement, StatementKind};

/// `Accept` for `/ksql` and `/info`.
const KSQL_JSON: &str = "application/vnd.ksql.v1+json";
/// `Accept` for `/query-stream`: a header line, then one row per line.
const DELIMITED: &str = "application/vnd.ksqlapi.delimited.v1";
/// The longest row line accepted from a query.
const MAX_ROW_BYTES: usize = 16 * 1024 * 1024;
/// How long closing a query on the server may take.
const CLOSE_TIMEOUT: Duration = Duration::from_secs(5);

/// Properties and variables that apply to the statements of one session:
/// what `SET`, `UNSET`, `DEFINE` and `UNDEFINE` change. The REST API is
/// stateless, so the client sends them with every statement.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Session {
    /// Streams properties, e.g. `auto.offset.reset` = `earliest`.
    pub properties: BTreeMap<String, String>,
    /// Values for `${variable}` references.
    pub variables: BTreeMap<String, String>,
}

impl Session {
    /// Applies a session statement; returns `false` for any other statement.
    pub fn apply(&mut self, statement: &Statement) -> bool {
        match &statement.kind {
            StatementKind::Set { property, value } => {
                self.properties.insert(property.clone(), value.clone());
            }
            StatementKind::Unset { property } => {
                self.properties.remove(property);
            }
            StatementKind::Define { variable, value } => {
                self.variables.insert(variable.clone(), value.clone());
            }
            StatementKind::Undefine { variable } => {
                self.variables.remove(variable);
            }
            _ => return false,
        }
        true
    }

    fn request_body(&self, field: &str, sql: &str) -> String {
        let mut body = json!({ field: sql });
        let properties_field = if field == "ksql" {
            "streamsProperties"
        } else {
            "properties"
        };
        body[properties_field] = json!(self.properties);
        if !self.variables.is_empty() {
            body["sessionVariables"] = json!(self.variables);
        }
        body.to_string()
    }
}

/// How much of a query to read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryLimits {
    /// Stop (and close the query) after this many rows; at least 1.
    pub max_rows: usize,
    /// Stop (and close the query) after this long, rows or not.
    pub timeout: Option<Duration>,
}

/// Talks to one ksqlDB server (or a load balancer in front of a cluster).
#[derive(Debug, Clone)]
pub struct KsqlClient {
    transport: Transport,
}

impl KsqlClient {
    /// A client for `config`. Fails on a bad URL or unreadable TLS files;
    /// nothing is sent yet.
    pub fn new(config: &KsqlConfig) -> KsqlResult<Self> {
        Ok(Self {
            transport: Transport::new(config)?,
        })
    }

    /// `host:port` of the server, without credentials.
    pub fn address(&self) -> String {
        self.transport.target()
    }

    /// The server's version, Kafka cluster id, service id and status.
    pub async fn info(&self) -> KsqlResult<ServerInfo> {
        let text = self
            .transport
            .request("GET", "/info", KSQL_JSON, None)
            .await?;
        let value: Value = parse_json(&text)?;
        let info = value.get("KsqlServerInfo").cloned().ok_or_else(|| {
            KsqlError::Response("no KsqlServerInfo in the /info response".to_string())
        })?;
        from_value(info)
    }

    /// Sends `sql` (one or more statements) to `/ksql` as is and returns the
    /// response entities. Gate the statements first: this runs writes too.
    pub async fn ksql(&self, sql: &str, session: &Session) -> KsqlResult<Vec<Value>> {
        let body = session.request_body("ksql", sql);
        let text = self
            .transport
            .request("POST", "/ksql", KSQL_JSON, Some(&body))
            .await?;
        match parse_json::<Value>(&text)? {
            Value::Array(entities) => {
                // An error entity inside a successful response.
                if let Some(error) = entities
                    .iter()
                    .find(|e| e.get("error_code").is_some() && e.get("message").is_some())
                {
                    return Err(api_error(200, &error.to_string()));
                }
                Ok(entities)
            }
            other => Err(api_error(200, &other.to_string())),
        }
    }

    /// One read-only listing statement's entity field, typed.
    async fn listing<T: DeserializeOwned>(&self, statement: &str, field: &str) -> KsqlResult<T> {
        let entities = self.ksql(statement, &Session::default()).await?;
        let list = entities
            .first()
            .and_then(|e| e.get(field))
            .cloned()
            .ok_or_else(|| {
                KsqlError::Response(format!("no '{field}' in the {statement} result"))
            })?;
        from_value(list)
    }

    /// `SHOW STREAMS`.
    pub async fn streams(&self) -> KsqlResult<Vec<SourceInfo>> {
        self.listing("SHOW STREAMS;", "streams").await
    }

    /// `SHOW TABLES`.
    pub async fn tables(&self) -> KsqlResult<Vec<SourceInfo>> {
        self.listing("SHOW TABLES;", "tables").await
    }

    /// `SHOW QUERIES`: the persistent queries.
    pub async fn queries(&self) -> KsqlResult<Vec<QueryInfo>> {
        self.listing("SHOW QUERIES;", "queries").await
    }

    /// `SHOW TOPICS`: the Kafka topics the server sees (internal ones left out).
    pub async fn topics(&self) -> KsqlResult<Vec<TopicInfo>> {
        self.listing("SHOW TOPICS;", "topics").await
    }

    /// `DESCRIBE name [EXTENDED]`. `name` is quoted when needed, so it can
    /// never add a statement.
    pub async fn describe(&self, name: &str, extended: bool) -> KsqlResult<SourceDescription> {
        let sql = format!(
            "DESCRIBE {}{};",
            quote_identifier(name),
            if extended { " EXTENDED" } else { "" }
        );
        let statement = Statement::parse(&sql)?;
        if statement.kind != StatementKind::Describe {
            return Err(KsqlError::config(format!(
                "'{name}' is not a stream or table name"
            )));
        }
        self.listing(&statement.text, "sourceDescription").await
    }

    /// Runs one statement through `/ksql` with the session's properties and
    /// variables. Queries (`SELECT`, `PRINT`) go through [`Self::query`].
    pub async fn execute(
        &self,
        statement: &Statement,
        session: &Session,
    ) -> KsqlResult<CommandResult> {
        if statement.is_query() {
            return Err(KsqlError::Statement(
                "queries (SELECT, PRINT) run through the query endpoint, not as statements"
                    .to_string(),
            ));
        }
        let entities = self.ksql(&statement.text, session).await?;
        Ok(CommandResult::from_entities(&entities))
    }

    /// Starts a query on `/query-stream` and returns once its header (query
    /// id and columns) arrived; read the rows from the stream. `sql` must be
    /// one `SELECT`. `PRINT` is not supported: its output is not rows.
    pub async fn query(&self, sql: &str, session: &Session) -> KsqlResult<QueryStream> {
        let statement = Statement::parse(sql)?;
        let StatementKind::Select { push } = statement.kind else {
            return Err(KsqlError::Statement(
                "only a SELECT can be run as a query".to_string(),
            ));
        };
        let body = session.request_body("sql", &statement.text);
        let response = self
            .transport
            .open("POST", "/query-stream", DELIMITED, Some(&body))
            .await?;
        let mut rows = response.body;
        if !(200..300).contains(&response.status) {
            let text = tokio::time::timeout(self.transport.timeout, rows.read_all(MAX_BODY_BYTES))
                .await
                .map_err(|_| KsqlError::Response("reading the error timed out".to_string()))??;
            return Err(api_error(response.status, &text));
        }
        let line = tokio::time::timeout(self.transport.timeout, rows.next_line(MAX_ROW_BYTES))
            .await
            .map_err(|_| KsqlError::Unreachable {
                target: self.address(),
                reason: "no query header in time".to_string(),
            })??
            .ok_or_else(|| KsqlError::Response("the query returned no header".to_string()))?;
        let header = parse_header(&line)?;
        Ok(QueryStream {
            header,
            push,
            rows,
            client: self.clone(),
            finished: false,
        })
    }

    /// Closes query `query_id` on the server. ksqlDB 0.29 answers a close
    /// from another HTTP/1.1 connection with HTTP 500 ("On wrong context or
    /// worker") while it still closes the query, and with HTTP 400 when the
    /// query has already ended; both count as closed.
    pub async fn close_query(&self, query_id: &str) -> KsqlResult<()> {
        let mut transport = self.transport.clone();
        transport.timeout = transport.timeout.min(CLOSE_TIMEOUT);
        let body = json!({ "queryId": query_id }).to_string();
        match transport
            .request("POST", "/close-query", "application/json", Some(&body))
            .await
        {
            Ok(_) => Ok(()),
            Err(KsqlError::Api {
                status, message, ..
            }) if status == 400 && message.contains("No query with id")
                || status == 500 && message.contains("wrong context") =>
            {
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    /// Runs a `SELECT` and collects its rows until the server ends it, the
    /// row or time limit is reached, or `cancel` completes (Ctrl+C); in the
    /// last three cases the query is closed on the server. `on_row` sees
    /// each row as it arrives (to print a push query live).
    pub async fn run_query(
        &self,
        sql: &str,
        session: &Session,
        limits: QueryLimits,
        cancel: impl Future<Output = ()>,
        mut on_row: impl FnMut(&QueryHeader, &[Value]),
    ) -> KsqlResult<QueryResult> {
        let max_rows = limits.max_rows.max(1);
        let mut stream = self.query(sql, session).await?;
        let deadline = limits.timeout.map(|t| tokio::time::Instant::now() + t);
        tokio::pin!(cancel);
        let mut rows = Vec::new();
        let end = loop {
            let sleep = async {
                match deadline {
                    Some(at) => tokio::time::sleep_until(at).await,
                    None => std::future::pending().await,
                }
            };
            let next = tokio::select! {
                row = stream.next_row() => row,
                () = sleep => break QueryEnd::TimeLimit,
                () = &mut cancel => break QueryEnd::Cancelled,
            };
            match next {
                Ok(Some(row)) => {
                    on_row(&stream.header, &row);
                    rows.push(row);
                    if rows.len() >= max_rows {
                        break QueryEnd::RowLimit;
                    }
                }
                Ok(None) => break QueryEnd::Completed,
                Err(e) => {
                    stream.close().await;
                    return Err(e);
                }
            }
        };
        let header = stream.header.clone();
        let push = stream.push;
        stream.close().await;
        Ok(QueryResult {
            query_id: header.query_id,
            push,
            columns: header.columns,
            rows,
            end,
        })
    }
}

/// A running query's rows. Dropping it drops the connection, which ends the
/// query on the server too; [`Self::close`] also asks the server to close it.
pub struct QueryStream {
    pub header: QueryHeader,
    /// A push query.
    pub push: bool,
    rows: Body,
    client: KsqlClient,
    finished: bool,
}

impl QueryStream {
    /// The next row (one value per column), or `None` when the server ended
    /// the query. Waits as long as the server sends nothing; put a time
    /// limit around it for push queries.
    pub async fn next_row(&mut self) -> KsqlResult<Option<Vec<Value>>> {
        loop {
            let Some(line) = self.rows.next_line(MAX_ROW_BYTES).await? else {
                self.finished = true;
                return Ok(None);
            };
            match parse_json::<Value>(&line)? {
                Value::Array(row) => return Ok(Some(row)),
                // An error after the header (the query failed while running).
                Value::Object(map)
                    if map.contains_key("error_code") || map.contains_key("errorCode") =>
                {
                    self.finished = true;
                    return Err(api_error(200, &line));
                }
                // Other messages (e.g. a final notice) carry no row.
                _ => continue,
            }
        }
    }

    /// Closes the query on the server unless it already ended, and drops
    /// the connection. A failed close is only logged: dropping the
    /// connection ends the query too.
    pub async fn close(self) {
        if !self.finished {
            if let Err(e) = self.client.close_query(&self.header.query_id).await {
                tracing::debug!(%e, query = %self.header.query_id, "closing the ksqlDB query failed");
            }
        }
        drop(self.rows);
    }
}

/// `name` as a ksqlDB identifier: as is when it is a plain identifier,
/// else in backticks (a backtick inside doubled).
pub fn quote_identifier(name: &str) -> String {
    let plain = name
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
    if plain {
        name.to_string()
    } else {
        format!("`{}`", name.replace('`', "``"))
    }
}

fn parse_header(line: &str) -> KsqlResult<QueryHeader> {
    let value: Value = parse_json(line)?;
    if value.get("error_code").is_some() {
        return Err(api_error(200, line));
    }
    let names: Vec<String> = from_value(value.get("columnNames").cloned().unwrap_or(json!([])))?;
    let types: Vec<String> = from_value(value.get("columnTypes").cloned().unwrap_or(json!([])))?;
    let query_id = value
        .get("queryId")
        .and_then(Value::as_str)
        .ok_or_else(|| KsqlError::Response("the query header has no queryId".to_string()))?
        .to_string();
    Ok(QueryHeader {
        query_id,
        columns: names
            .into_iter()
            .zip(types.into_iter().chain(std::iter::repeat(String::new())))
            .map(|(name, type_name)| Column { name, type_name })
            .collect(),
    })
}

fn parse_json<T: DeserializeOwned>(text: &str) -> KsqlResult<T> {
    serde_json::from_str(text.trim()).map_err(|e| KsqlError::Response(format!("not JSON: {e}")))
}

fn from_value<T: DeserializeOwned>(value: Value) -> KsqlResult<T> {
    serde_json::from_value(value).map_err(|e| KsqlError::Response(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::tests::{chunked, serve, with_length};

    #[test]
    fn identifiers_are_quoted_when_needed() {
        assert_eq!(quote_identifier("KQ_ORDERS"), "KQ_ORDERS");
        assert_eq!(quote_identifier("my stream"), "`my stream`");
        assert_eq!(quote_identifier("x; DROP STREAM y"), "`x; DROP STREAM y`");
        assert_eq!(quote_identifier("a`b"), "`a``b`");
        assert_eq!(quote_identifier("1abc"), "`1abc`");
    }

    #[test]
    fn session_statements_change_what_is_sent() {
        let mut s = Session::default();
        for sql in [
            "SET 'auto.offset.reset'='earliest';",
            "DEFINE t = 'kq';",
            "SET 'x'='1';",
            "UNSET 'x';",
        ] {
            assert!(s.apply(&Statement::parse(sql).unwrap()), "{sql}");
        }
        assert!(!s.apply(&Statement::parse("SHOW STREAMS;").unwrap()));
        let body: Value = serde_json::from_str(&s.request_body("ksql", "SHOW STREAMS;")).unwrap();
        assert_eq!(
            body,
            json!({"ksql": "SHOW STREAMS;", "streamsProperties": {"auto.offset.reset": "earliest"}, "sessionVariables": {"t": "kq"}})
        );
        let body: Value =
            serde_json::from_str(&Session::default().request_body("sql", "SELECT 1;")).unwrap();
        assert_eq!(body, json!({"sql": "SELECT 1;", "properties": {}}));
    }

    #[tokio::test]
    async fn describe_quotes_the_name_and_reads_the_description() {
        let (url, server) = serve(vec![with_length(
            "200 OK",
            r#"[{"@type":"sourceDescription","sourceDescription":{"name":"a b","type":"STREAM","fields":[]}}]"#,
        )])
        .await;
        let client = KsqlClient::new(&KsqlConfig::new(&url)).unwrap();
        let d = client.describe("a b; DROP STREAM c", false).await.unwrap();
        assert_eq!(d.name, "a b");
        let requests = server.await.unwrap();
        assert!(
            requests[0].contains(r#""ksql":"DESCRIBE `a b; DROP STREAM c`;""#),
            "{}",
            requests[0]
        );
    }

    #[tokio::test]
    async fn pull_queries_collect_rows_and_push_queries_stop_at_the_limit() {
        let header =
            r#"{"queryId":"q1","columnNames":["ID","EMAIL"],"columnTypes":["INTEGER","STRING"]}"#;
        let (url, server) = serve(vec![
            chunked("200 OK", &[header, "[1,\"a@example.com\"]", "[2,null]"]),
            chunked("200 OK", &[header, "[1,\"x\"]", "[2,\"y\"]", "[3,\"z\"]"]),
            // The close of the second query.
            with_length("500 Internal Server Error", r#"{"@type":"generic_error","error_code":50000,"message":"On wrong context or worker"}"#),
            chunked(
                "400 Bad Request",
                &[r#"{"@type":"statement_error","error_code":40001,"message":"NOPE does not exist."}"#],
            ),
        ])
        .await;
        let client = KsqlClient::new(&KsqlConfig::new(&url)).unwrap();
        let limits = QueryLimits {
            max_rows: 10,
            timeout: None,
        };
        let mut seen = 0;
        let pull = client
            .run_query(
                "SELECT * FROM t;",
                &Session::default(),
                limits,
                std::future::pending(),
                |_, _| seen += 1,
            )
            .await
            .unwrap();
        assert_eq!(seen, 2);
        assert!(!pull.push);
        assert_eq!(pull.end, QueryEnd::Completed);
        assert_eq!(pull.columns[1].type_name, "STRING");
        assert_eq!(pull.rows[1], vec![json!(2), Value::Null]);

        let push = client
            .run_query(
                "SELECT * FROM t EMIT CHANGES;",
                &Session::default(),
                QueryLimits {
                    max_rows: 2,
                    timeout: None,
                },
                std::future::pending(),
                |_, _| {},
            )
            .await
            .unwrap();
        assert!(push.push);
        assert_eq!(push.end, QueryEnd::RowLimit);
        assert_eq!(push.rows.len(), 2);

        let err = client
            .run_query(
                "SELECT * FROM nope;",
                &Session::default(),
                limits,
                std::future::pending(),
                |_, _| {},
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("NOPE does not exist"), "{err}");

        let requests = server.await.unwrap();
        assert!(requests[0].starts_with("POST /query-stream HTTP/1.1"));
        assert!(requests[0].contains("Accept: application/vnd.ksqlapi.delimited.v1"));
        assert!(requests[2].starts_with("POST /close-query HTTP/1.1"));
        assert!(
            requests[2].ends_with(r#"{"queryId":"q1"}"#),
            "{}",
            requests[2]
        );

        // Only a SELECT runs as a query; nothing is sent.
        assert!(client
            .query("SHOW STREAMS;", &Session::default())
            .await
            .is_err());
        assert!(client
            .query("SELECT 1; SELECT 2;", &Session::default())
            .await
            .is_err());
    }

    #[tokio::test]
    async fn push_queries_stop_at_the_time_limit_or_on_cancel() {
        // A server that sends the header and then nothing.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    break;
                };
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 4096];
                    let n = sock.read(&mut buf).await.unwrap_or(0);
                    let request = String::from_utf8_lossy(&buf[..n]).to_string();
                    if request.starts_with("POST /close-query") {
                        let _ = sock
                            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                            .await;
                        return;
                    }
                    let head = chunked(
                        "200 OK",
                        &[r#"{"queryId":"q","columnNames":["A"],"columnTypes":["INTEGER"]}"#],
                    );
                    // Without the final 0-chunk: the query stays open.
                    let open = &head[..head.len() - 5];
                    let _ = sock.write_all(open).await;
                    tokio::time::sleep(Duration::from_secs(30)).await;
                });
            }
        });
        let client = KsqlClient::new(&KsqlConfig::new(&url)).unwrap();
        let started = std::time::Instant::now();
        let r = client
            .run_query(
                "SELECT * FROM t EMIT CHANGES;",
                &Session::default(),
                QueryLimits {
                    max_rows: 5,
                    timeout: Some(Duration::from_millis(300)),
                },
                std::future::pending(),
                |_, _| {},
            )
            .await
            .unwrap();
        assert_eq!(r.end, QueryEnd::TimeLimit);
        assert!(started.elapsed() < Duration::from_secs(10));
        let r = client
            .run_query(
                "SELECT * FROM t EMIT CHANGES;",
                &Session::default(),
                QueryLimits {
                    max_rows: 5,
                    timeout: None,
                },
                tokio::time::sleep(Duration::from_millis(200)),
                |_, _| {},
            )
            .await
            .unwrap();
        assert_eq!(r.end, QueryEnd::Cancelled);
        assert!(r.rows.is_empty());
    }

    #[tokio::test]
    async fn listings_info_and_statement_errors() {
        let (url, _server) = serve(vec![
            with_length("200 OK", r#"{"KsqlServerInfo":{"version":"0.29.0","kafkaClusterId":"c","ksqlServiceId":"s_","serverStatus":"RUNNING"}}"#),
            with_length("200 OK", r#"[{"@type":"streams","statementText":"SHOW STREAMS;","streams":[{"type":"STREAM","name":"KQ","topic":"kq","keyFormat":"KAFKA","valueFormat":"JSON","isWindowed":false}],"warnings":[]}]"#),
            with_length("400 Bad Request", r#"{"@type":"statement_error","error_code":40001,"message":"Stream KQ already exists","statementText":"CREATE ...","entities":[]}"#),
            with_length("200 OK", "[]"),
        ])
        .await;
        let client = KsqlClient::new(&KsqlConfig::new(&url)).unwrap();
        let info = client.info().await.unwrap();
        assert_eq!(info.version, "0.29.0");
        assert_eq!(info.server_status.as_deref(), Some("RUNNING"));
        let streams = client.streams().await.unwrap();
        assert_eq!(streams[0].name, "KQ");
        assert_eq!(streams[0].kind, "STREAM");
        let create = Statement::parse(
            "CREATE STREAM kq (v STRING) WITH (KAFKA_TOPIC='kq', VALUE_FORMAT='JSON');",
        )
        .unwrap();
        let err = client
            .execute(&create, &Session::default())
            .await
            .unwrap_err();
        assert_eq!(
            err,
            KsqlError::Api {
                status: 400,
                code: Some(40001),
                message: "Stream KQ already exists".into()
            }
        );
        let insert = Statement::parse("INSERT INTO kq (v) VALUES ('x');").unwrap();
        let done = client.execute(&insert, &Session::default()).await.unwrap();
        assert_eq!(done.entity, "none");
        let select = Statement::parse("SELECT * FROM kq;").unwrap();
        assert!(client.execute(&select, &Session::default()).await.is_err());
    }
}
