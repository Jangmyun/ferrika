//! The ksqlDB write tool (preview) `ksql_exec`, offered with `ferrika mcp
//! --allow-write`: runs statements that change the ksqlDB server of the
//! Kafka profile the MCP server runs with, as `ferrika ksql exec` does.
//!
//! Like every write tool it is planned, confirmed and run through
//! `write_tools.rs`. The gate is the CLI's, per statement
//! ([`WriteGuard::authorize_ksql`]): every category of every write
//! statement (`ksql.create`, `ksql.drop`, `ksql.insert` with `produce`,
//! `ksql.terminate`, `ksql.alter`, and `topics.create`, `topics.delete` or
//! `connect.change` when the statement also does that) must be allowed on
//! the Kafka profile, the server's `host:port` and the cluster's brokers,
//! or nothing is sent.
//!
//! The plan names each statement by keywords and identifiers only
//! ([`Statement::summary`]) and the Kafka topic it creates, deletes or
//! writes, looked up on the server; `--topics` limits those topics as it
//! limits the Kafka tools, and a statement whose topic cannot be named is
//! refused under `--topics`. `DROP`, `TERMINATE` and dropping a connector
//! are destructive (a person types the name back); `DROP ... DELETE TOPIC`
//! and statements Ferrika does not recognise need a person to confirm.
//!
//! Each write gets an audit entry `mcp ksql_exec` with its summary and the
//! outcome (`ok: SUCCESS[, query <id>]`, `failed: ksqlDB error <code> (HTTP
//! <status>)`, `not run`): never the statement's literals or the server's
//! messages, which can quote record values.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;

use serde_json::{json, Value};

use ferrika_core::error::{KafkaError, KafkaResult};
use ferrika_core::kafka::context::ResolvedContext;
use ferrika_ksql::client::{KsqlClient, Session};
use ferrika_ksql::config::KsqlConfig;
use ferrika_ksql::error::{KsqlError, KsqlResult};
use ferrika_ksql::model::CommandResult;
use ferrika_ksql::statement::{split, Access, Statement, StatementKind};

use super::backend::BoxFuture;
use super::guard::{ToolError, WriteGuard, KSQL_EXEC};
use super::write_tools::Fields;

/// Longest script `ksql_exec` takes.
pub(super) const SQL_MAX_BYTES: usize = 64 * 1024;
/// Most statements in one script.
pub(super) const STATEMENTS_MAX: usize = 50;
/// Most statements named in the one-line summary.
const SUMMARY_STATEMENTS: usize = 5;

/// The ksqlDB calls behind `ksql_exec`, behind a trait so the whole plan →
/// confirm → execute flow is tested without a server.
pub(super) trait KsqlBackend {
    /// `host:port` of the profile's ksqlDB server; fails when the profile
    /// has none (saying how to add one).
    fn address(&self) -> KafkaResult<String>;
    /// The Kafka topic of stream or table `name`; `None` when the server
    /// does not know it.
    fn source_topic<'a>(&'a self, name: &'a str) -> BoxFuture<'a, Option<String>>;
    /// Runs one statement with the session's properties and variables.
    fn execute<'a>(
        &'a self,
        statement: &'a Statement,
        session: &'a Session,
    ) -> Pin<Box<dyn Future<Output = KsqlResult<CommandResult>> + 'a>>;
}

/// The ksqlDB server of the Kafka profile the MCP server runs with.
pub(super) struct LiveKsql {
    config: Result<KsqlConfig, String>,
}

impl LiveKsql {
    pub(super) fn from_context(context: &ResolvedContext) -> Self {
        Self {
            config: super::ksql_tools::connection(context).map_err(|e| e.to_string()),
        }
    }

    /// No server (tests and servers built without a context).
    pub(super) fn none() -> Self {
        Self {
            config: Err(
                "No ksqlDB server: start the MCP server with a profile that has one \
                 (`ferrika profile add NAME -b BROKERS --ksql-url URL`)"
                    .to_string(),
            ),
        }
    }

    fn client(&self) -> KafkaResult<KsqlClient> {
        let config = self.config.as_ref().map_err(KafkaError::config)?;
        KsqlClient::new(config).map_err(|e| KafkaError::config(e.to_string()))
    }
}

impl KsqlBackend for LiveKsql {
    fn address(&self) -> KafkaResult<String> {
        Ok(self.client()?.address())
    }

    fn source_topic<'a>(&'a self, name: &'a str) -> BoxFuture<'a, Option<String>> {
        Box::pin(async move {
            match self.client()?.describe(name, false).await {
                Ok(d) => Ok(Some(d.topic).filter(|t| !t.is_empty())),
                // The server answered: it does not know the name.
                Err(KsqlError::Api { .. }) => Ok(None),
                Err(e) => Err(KafkaError::config(e.to_string())),
            }
        })
    }

    fn execute<'a>(
        &'a self,
        statement: &'a Statement,
        session: &'a Session,
    ) -> Pin<Box<dyn Future<Output = KsqlResult<CommandResult>> + 'a>> {
        Box::pin(async move {
            let client = self
                .client()
                .map_err(|e| KsqlError::config(e.to_string()))?;
            client.execute(statement, session).await
        })
    }
}

/// A parsed `ksql_exec` call: the script's statements, in order.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct KsqlRequest {
    pub statements: Vec<Statement>,
}

impl KsqlRequest {
    /// Records the script writes that count against the session's produce
    /// budget: one per `INSERT ... VALUES`. (`INSERT ... SELECT` and `CREATE
    /// ... AS SELECT` start persistent queries whose output is not counted;
    /// the plan says so.)
    pub(super) fn produce_records(&self) -> u64 {
        self.statements
            .iter()
            .filter(|s| s.kind == StatementKind::Insert { values: true })
            .count() as u64
    }
}

/// Parses and checks `ksql_exec`'s arguments: a script of at most
/// [`STATEMENTS_MAX`] statements that changes the server. Queries are
/// refused (they go through `ksql_query`), and so is a script with no
/// statement that changes anything.
pub(super) fn parse(args: &Value) -> Result<KsqlRequest, ToolError> {
    let fields = Fields::new(args)?;
    fields.only(&["sql", "mode", "confirmation"])?;
    let sql = fields.string("sql")?;
    if sql.len() > SQL_MAX_BYTES {
        return Err(ToolError::invalid(format!(
            "'sql' is {} bytes; the most is {SQL_MAX_BYTES}",
            sql.len()
        )));
    }
    let statements = split(&sql).map_err(|e| ToolError::invalid(e.to_string()))?;
    if statements.len() > STATEMENTS_MAX {
        return Err(ToolError::invalid(format!(
            "The script has {} statements; ksql_exec runs at most {STATEMENTS_MAX}",
            statements.len()
        )));
    }
    if let Some(query) = statements.iter().find(|s| s.is_query()) {
        return Err(ToolError::invalid(format!(
            "ksql_exec does not run queries ({}); use ksql_query. Nothing was sent.",
            query.summary()
        )));
    }
    if !statements.iter().any(Statement::is_write) {
        return Err(ToolError::invalid(
            "No statement in the script changes ksqlDB; use ksql_list or ksql_describe to read",
        ));
    }
    Ok(KsqlRequest { statements })
}

/// What a statement does to a Kafka topic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TopicEffect {
    Create,
    Delete,
    Write,
    /// A connector or an unrecognised statement: which topics it touches
    /// cannot be known.
    Unknown,
}

impl TopicEffect {
    fn of(statement: &Statement) -> Option<Self> {
        match &statement.kind {
            StatementKind::Create {
                creates_topic: true,
                ..
            } => Some(Self::Create),
            StatementKind::Drop {
                delete_topic: true, ..
            } => Some(Self::Delete),
            StatementKind::Insert { .. } => Some(Self::Write),
            StatementKind::CreateConnector
            | StatementKind::DropConnector
            | StatementKind::Unknown => Some(Self::Unknown),
            StatementKind::Create { .. }
            | StatementKind::Drop { .. }
            | StatementKind::Select { .. }
            | StatementKind::Print
            | StatementKind::Show
            | StatementKind::Describe
            | StatementKind::Explain
            | StatementKind::Assert
            | StatementKind::Set { .. }
            | StatementKind::Unset { .. }
            | StatementKind::Define { .. }
            | StatementKind::Undefine { .. }
            | StatementKind::Terminate
            | StatementKind::Pause
            | StatementKind::Resume
            | StatementKind::Alter => None,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Create => "create",
            Self::Delete => "delete",
            Self::Write => "write",
            Self::Unknown => "unknown",
        }
    }
}

/// The Kafka topic each statement creates, deletes or writes (`None` for
/// statements that touch none), named by its `KAFKA_TOPIC` property, by a
/// `CREATE` earlier in the script, or by the server.
async fn topics_of(
    backend: &dyn KsqlBackend,
    statements: &[Statement],
) -> Result<Vec<Option<(TopicEffect, Option<String>)>>, ToolError> {
    let mut created: HashMap<String, Option<String>> = HashMap::new();
    let mut out = Vec::with_capacity(statements.len());
    for s in statements {
        let effect = TopicEffect::of(s);
        let object = s.object.clone().unwrap_or_default();
        let topic = match (&s.kind, effect) {
            (StatementKind::Create { .. }, _) => {
                let topic = s.with_property("KAFKA_TOPIC");
                created.insert(object, topic.clone());
                topic
            }
            (_, Some(TopicEffect::Write | TopicEffect::Delete)) => match created.get(&object) {
                Some(topic) => topic.clone(),
                None => backend
                    .source_topic(&object)
                    .await
                    .map_err(ToolError::kafka)?,
            },
            _ => None,
        };
        out.push(effect.map(|e| (e, topic)));
    }
    Ok(out)
}

/// Refuses topics the server may not change: internal ones, those outside
/// `--topics`, and, under `--topics`, statements whose topic cannot be
/// named.
fn check_topics(
    guard: &WriteGuard,
    statements: &[Statement],
    topics: &[Option<(TopicEffect, Option<String>)>],
) -> Result<(), ToolError> {
    let scoped = !guard.config().topics.is_empty();
    for (s, topic) in statements.iter().zip(topics) {
        match topic {
            Some((_, Some(name))) => guard.check_topic(name)?,
            Some((TopicEffect::Unknown, None)) if scoped => {
                return Err(ToolError::new(
                    "target_not_allowed",
                    format!(
                        "`{}` may change Kafka topics that cannot be checked against --topics \
                         ({}); run it from the Ferrika CLI or GUI",
                        s.summary(),
                        guard.config().topics.join(", ")
                    ),
                ))
            }
            Some((TopicEffect::Create, None)) if scoped => {
                return Err(ToolError::new(
                    "target_not_allowed",
                    format!(
                        "`{}` creates a Kafka topic; with --topics set, name it with \
                         KAFKA_TOPIC='...' in the WITH clause so it can be checked",
                        s.summary()
                    ),
                ))
            }
            Some((_, None)) if scoped => {
                return Err(ToolError::new(
                    "target_not_allowed",
                    format!(
                        "The Kafka topic of `{}` is unknown (the server does not know {}), so \
                         it cannot be checked against --topics",
                        s.summary(),
                        s.object.as_deref().unwrap_or("it")
                    ),
                ))
            }
            Some(_) | None => {}
        }
    }
    Ok(())
}

fn access_name(access: Access) -> &'static str {
    match access {
        Access::Read => "read",
        Access::Session => "session",
        Access::Write => "write",
    }
}

/// Whether a person confirming types a name back: the statement removes
/// something.
fn is_destructive(s: &Statement) -> bool {
    matches!(
        s.kind,
        StatementKind::Drop { .. } | StatementKind::DropConnector | StatementKind::Terminate
    )
}

/// The planned work: the statements to run.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct KsqlWork {
    pub statements: Vec<Statement>,
}

/// What `plan` found, for `write_tools.rs` to confirm and run.
pub(super) struct KsqlPlanned {
    pub plan: Value,
    pub summary: String,
    /// The audit target of a declined confirmation: the first write.
    pub target: String,
    pub destructive: bool,
    /// The name a person types back for a destructive script.
    pub confirm_name: Option<String>,
    pub person_only: Option<String>,
    pub work: KsqlWork,
}

/// The plan of `request` on the server at `server`: each statement with
/// its access, categories and Kafka topic, warnings, and who must confirm.
/// Fails when a topic is out of scope. Sends no statement.
pub(super) async fn plan(
    guard: &WriteGuard,
    backend: &dyn KsqlBackend,
    server: &str,
    request: &KsqlRequest,
) -> Result<KsqlPlanned, ToolError> {
    let statements = &request.statements;
    let topics = topics_of(backend, statements).await?;
    check_topics(guard, statements, &topics)?;
    let mut warnings: Vec<String> = Vec::new();
    let mut listed = Vec::with_capacity(statements.len());
    let mut person_only = None;
    for (s, topic) in statements.iter().zip(&topics) {
        let summary = s.summary();
        let categories: Vec<&str> = s.policy_actions().iter().map(|a| a.id()).collect();
        let mut entry = json!({
            "statement": summary,
            "access": access_name(s.access()),
            "categories": categories,
        });
        if let Some((effect, name)) = topic {
            entry["topic"] = json!({ "name": name, "effect": effect.name() });
        }
        if is_destructive(s) {
            entry["destructive"] = json!(true);
        }
        listed.push(entry);
        let topic_name = topic
            .as_ref()
            .and_then(|(_, n)| n.as_deref())
            .map(|n| format!("'{n}'"))
            .unwrap_or_else(|| "its topic".to_string());
        match &s.kind {
            StatementKind::Drop {
                delete_topic: true, ..
            } => {
                warnings.push(format!(
                    "`{summary}` deletes Kafka topic {topic_name} and its records; this cannot \
                     be undone."
                ));
                person_only.get_or_insert_with(|| {
                    format!("`{summary}` deletes a Kafka topic and its records")
                });
            }
            StatementKind::Create {
                as_select: true, ..
            }
            | StatementKind::Insert { values: false } => warnings.push(format!(
                "`{summary}` starts a persistent query that keeps writing to {topic_name} until \
                 it is terminated (not counted against --max-produce-records)."
            )),
            StatementKind::Terminate => {
                warnings.push(format!("`{summary}` stops a persistent query."))
            }
            StatementKind::CreateConnector | StatementKind::DropConnector => warnings.push(
                format!("`{summary}` changes a Kafka Connect connector (connect.change)."),
            ),
            StatementKind::Unknown => {
                warnings.push(format!(
                    "`{summary}` is not a statement Ferrika recognises, so it needs every \
                     category a ksqlDB statement can need."
                ));
                person_only.get_or_insert_with(|| {
                    format!("`{summary}` is not a statement Ferrika recognises")
                });
            }
            _ => {}
        }
    }
    let writes: Vec<&Statement> = statements.iter().filter(|s| s.is_write()).collect();
    let first = writes.first().map(|s| s.summary()).unwrap_or_default();
    let target = match writes.len() {
        0 | 1 => first,
        n => format!("{first} (+{} more)", n - 1),
    };
    let destructive = statements.iter().find(|s| is_destructive(s));
    let confirm_name = destructive.and_then(|s| s.object.clone());
    let mut names: Vec<String> = statements
        .iter()
        .take(SUMMARY_STATEMENTS)
        .map(Statement::summary)
        .collect();
    if statements.len() > SUMMARY_STATEMENTS {
        names.push(format!(
            "... {} more",
            statements.len() - SUMMARY_STATEMENTS
        ));
    }
    let summary = format!(
        "run {} ksqlDB statement(s), {} changing the server, on {server}: {}",
        statements.len(),
        writes.len(),
        names.join("; ")
    );
    let plan = json!({
        "server": server,
        "statements": listed,
        "writes": writes.len(),
        "warnings": warnings,
        "note": "Statements run in order; the first failure stops the script, and the \
                 statements before it stay done (ksqlDB has no transactions).",
    });
    Ok(KsqlPlanned {
        plan,
        summary,
        target,
        destructive: destructive.is_some(),
        confirm_name,
        person_only,
        work: KsqlWork {
            statements: statements.clone(),
        },
    })
}

/// Runs the statements in order with their session (`SET`, `DEFINE`...)
/// and records each write's outcome. The first failure stops the script:
/// the writes after it are recorded as not run, and the error lists every
/// statement's status. Returns the result and a warning when the audit log
/// could not be written.
pub(super) async fn execute(
    guard: &WriteGuard,
    backend: &dyn KsqlBackend,
    server: &str,
    work: &KsqlWork,
) -> Result<(Value, Option<String>), ToolError> {
    let mut session = Session::default();
    let mut reports = Vec::with_capacity(work.statements.len());
    let mut failure: Option<(usize, KsqlError)> = None;
    let mut audit_warning = None;
    let mut ran_write = false;
    let mut record = |summary: &str, ok: bool, outcome: &str| {
        let warning = guard.record_result(KSQL_EXEC, summary, outcome, ok, None);
        if audit_warning.is_none() {
            audit_warning = warning;
        }
    };
    for (i, s) in work.statements.iter().enumerate() {
        let summary = s.summary();
        if failure.is_some() {
            if s.is_write() {
                record(&summary, false, "not run (an earlier statement failed)");
            }
            reports.push(json!({ "statement": summary, "status": "NOT RUN" }));
            continue;
        }
        if session.apply(s) {
            reports.push(json!({ "statement": summary, "status": "SESSION" }));
            continue;
        }
        match backend.execute(s, &session).await {
            Ok(result) => {
                if s.is_write() {
                    ran_write = true;
                    let mut outcome = format!("ok: {}", result.status);
                    if let Some(id) = &result.query_id {
                        outcome.push_str(&format!(", query {id}"));
                    }
                    record(&summary, true, &outcome);
                }
                reports.push(json!({
                    "statement": summary,
                    "status": result.status,
                    "message": result.message,
                    "query_id": result.query_id,
                    "warnings": result.warnings,
                }));
            }
            Err(err) => {
                if s.is_write() {
                    record(&summary, false, &err.audit_reason());
                }
                reports.push(json!({
                    "statement": summary,
                    "status": "ERROR",
                    "error": err.to_string(),
                }));
                failure = Some((i, err));
            }
        }
    }
    if let Some((i, err)) = failure {
        // Retrying is safe only when nothing ran yet.
        let retry = !ran_write && matches!(err, KsqlError::Unreachable { .. });
        let mut error = ToolError::new(
            "ksql_error",
            format!(
                "Statement {} failed: {err}. Statements after it were not run{}.",
                i + 1,
                if ran_write {
                    "; the ones before it stay done"
                } else {
                    ""
                }
            ),
        )
        .with("server", json!(server))
        .with("statements", json!(reports));
        if let Some(warning) = audit_warning {
            error = error.with("audit_warning", json!(warning));
        }
        return Err(if retry { error.retryable() } else { error });
    }
    Ok((
        json!({ "server": server, "ok": true, "statements": reports }),
        audit_warning,
    ))
}

/// `ksql_exec`'s definition for `tools/list`.
pub(super) fn definitions() -> Vec<Value> {
    vec![json!({
        "name": KSQL_EXEC,
        "title": "Run ksqlDB statements",
        "description": "ksqlDB (preview): runs statements that change the profile's ksqlDB server (CREATE, DROP, INSERT, TERMINATE, PAUSE, RESUME, ALTER, connectors; SET, UNSET, DEFINE, UNDEFINE apply to the statements after them), in order, stopping at the first failure. Every write statement needs its company-policy categories (ksql.create, ksql.drop, ksql.insert and produce, ksql.terminate, ksql.alter; topics.create, topics.delete or connect.change when it also does that); one refusal refuses the whole script before anything is sent. The plan lists each statement by keywords and names, its categories and the Kafka topic it creates, deletes or writes. DROP ... DELETE TOPIC and unrecognised statements need a person to confirm. Queries go through ksql_query.",
        "inputSchema": {
            "type": "object",
            "additionalProperties": false,
            "properties": super::write_tools::with_mode(json!({
                "sql": {
                    "type": "string",
                    "minLength": 1,
                    "maxLength": SQL_MAX_BYTES,
                    "description": "ksqlDB statements separated by ';' (at most 50)"
                }
            })),
            "required": ["sql"]
        },
        "annotations": super::write_tools::annotations("Run ksqlDB statements", true, false)
    })]
}

#[cfg(test)]
pub(super) mod fake {
    use std::cell::RefCell;
    use std::collections::BTreeMap;

    use super::*;

    /// A ksqlDB server in memory: sources and their topics, the statements
    /// it ran, and statements it fails.
    #[derive(Default)]
    pub struct FakeKsql {
        pub sources: BTreeMap<String, String>,
        /// Statement summaries the server answers with an error.
        pub failing: Vec<String>,
        /// The statements run, with the session properties they were sent
        /// with.
        pub ran: RefCell<Vec<(String, BTreeMap<String, String>)>>,
        pub unreachable: bool,
    }

    impl FakeKsql {
        pub fn with_source(mut self, name: &str, topic: &str) -> Self {
            self.sources.insert(name.into(), topic.into());
            self
        }
    }

    impl KsqlBackend for &'static FakeKsql {
        fn address(&self) -> KafkaResult<String> {
            Ok("ksql.test:8088".to_string())
        }

        fn source_topic<'a>(&'a self, name: &'a str) -> BoxFuture<'a, Option<String>> {
            Box::pin(async move {
                if self.unreachable {
                    return Err(KafkaError::config("Cannot reach the ksqlDB server"));
                }
                Ok(self.sources.get(name).cloned())
            })
        }

        fn execute<'a>(
            &'a self,
            statement: &'a Statement,
            session: &'a Session,
        ) -> Pin<Box<dyn Future<Output = KsqlResult<CommandResult>> + 'a>> {
            Box::pin(async move {
                if self.unreachable {
                    return Err(KsqlError::Unreachable {
                        target: "ksql.test:8088".into(),
                        reason: "connection refused".into(),
                    });
                }
                let summary = statement.summary();
                if self.failing.contains(&summary) {
                    return Err(KsqlError::Api {
                        status: 400,
                        code: Some(40001),
                        message: format!("Failed: {}", statement.text),
                    });
                }
                self.ran
                    .borrow_mut()
                    .push((summary, session.properties.clone()));
                let query_id = matches!(
                    statement.kind,
                    StatementKind::Create {
                        as_select: true,
                        ..
                    }
                )
                .then(|| "CSAS_1".to_string());
                Ok(CommandResult {
                    entity: "currentStatus".into(),
                    status: "SUCCESS".into(),
                    message: "done".into(),
                    query_id,
                    warnings: Vec::new(),
                    raw: Value::Null,
                })
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn statements(sql: &str) -> Vec<Statement> {
        split(sql).unwrap()
    }

    #[test]
    fn parse_takes_one_script_of_writes() {
        let ok =
            parse(&json!({ "sql": "SET 'auto.offset.reset'='earliest'; DROP STREAM s;" })).unwrap();
        assert_eq!(ok.statements.len(), 2);
        for (args, why) in [
            (json!({}), "required"),
            (json!({ "sql": "" }), "required"),
            (json!({ "sql": 1 }), "string"),
            (
                json!({ "sql": "DROP STREAM s;", "x": 1 }),
                "Unknown argument",
            ),
            (
                json!({ "sql": "SELECT * FROM s; DROP STREAM s;" }),
                "ksql_query",
            ),
            (json!({ "sql": "PRINT t;" }), "ksql_query"),
            (
                json!({ "sql": "SHOW STREAMS; SET 'a'='b';" }),
                "No statement",
            ),
            (json!({ "sql": "DROP STREAM 's;" }), "unterminated"),
        ] {
            let err = parse(&args).unwrap_err();
            assert_eq!(err.code, "invalid_argument");
            assert!(err.message.contains(why), "{args}: {}", err.message);
        }
        let long = "DROP STREAM s;".repeat(STATEMENTS_MAX + 1);
        assert!(parse(&json!({ "sql": long }))
            .unwrap_err()
            .message
            .contains("at most"));
        let huge = format!("DROP STREAM s; -- {}", "x".repeat(SQL_MAX_BYTES));
        assert!(parse(&json!({ "sql": huge }))
            .unwrap_err()
            .message
            .contains("bytes"));
    }

    #[test]
    fn only_inserts_with_values_count_as_produced_records() {
        let request = KsqlRequest {
            statements: statements(
                "INSERT INTO s (v) VALUES ('a'); INSERT INTO s (v) VALUES ('b'); \
                 INSERT INTO s SELECT * FROM t; DROP STREAM x;",
            ),
        };
        assert_eq!(request.produce_records(), 2);
    }

    #[tokio::test]
    async fn topics_come_from_with_clauses_earlier_creates_and_the_server() {
        let fake: &'static fake::FakeKsql = Box::leak(Box::new(
            fake::FakeKsql::default().with_source("ORDERS", "orders.v1"),
        ));
        let s = statements(
            "CREATE STREAM n (v STRING) WITH (KAFKA_TOPIC='new.t', PARTITIONS=1, VALUE_FORMAT='JSON');\
             INSERT INTO n (v) VALUES ('x');\
             INSERT INTO orders (v) VALUES ('y');\
             DROP STREAM orders DELETE TOPIC;\
             CREATE STREAM big AS SELECT * FROM orders;\
             TERMINATE CSAS_BIG_1;\
             INSERT INTO nowhere (v) VALUES ('z');\
             DROP CONNECTOR c;",
        );
        let topics = topics_of(&fake, &s).await.unwrap();
        let named: Vec<Option<(&str, Option<&str>)>> = topics
            .iter()
            .map(|t| t.as_ref().map(|(e, n)| (e.name(), n.as_deref())))
            .collect();
        assert_eq!(
            named,
            [
                Some(("create", Some("new.t"))),
                Some(("write", Some("new.t"))),
                Some(("write", Some("orders.v1"))),
                Some(("delete", Some("orders.v1"))),
                // The sink topic of a CSAS without KAFKA_TOPIC is the
                // server's choice.
                Some(("create", None)),
                None,
                Some(("write", None)),
                Some(("unknown", None)),
            ]
        );
    }

    use super::super::backend::fake::FakeCluster;
    use super::super::guard::tests::{config, guard_with, no_policy, policy, MemoryLog};
    use super::super::guard::GuardConfig;
    use super::super::write_tools::tests::{call_asking, code, execute, token, Client};
    use super::super::write_tools::WriteTools;
    use ferrika_core::model::policy::{McpWriteRule, PolicyRules};
    use ferrika_governance::policy::EffectivePolicy;

    const PII: &str = "jane.doe@example.com";

    /// Write tools on a fake ksqlDB server, with `config` and `policy`.
    fn ksql_tools(
        server: fake::FakeKsql,
        config: GuardConfig,
        policy: &'static EffectivePolicy,
    ) -> (WriteTools, &'static fake::FakeKsql, MemoryLog) {
        let server: &'static fake::FakeKsql = Box::leak(Box::new(server));
        let cluster: &'static FakeCluster = Box::leak(Box::new(FakeCluster {
            brokers: 1,
            ..Default::default()
        }));
        let (guard, log, _) = guard_with(config, policy);
        let mut tools = WriteTools::new(guard, Box::new(cluster));
        tools.ksql = Box::new(server);
        (tools, server, log)
    }

    fn orders_server() -> fake::FakeKsql {
        fake::FakeKsql::default().with_source("ORDERS", "orders")
    }

    fn sql(text: &str) -> Value {
        json!({ "sql": text })
    }

    fn ran(server: &fake::FakeKsql) -> Vec<String> {
        server.ran.borrow().iter().map(|(s, _)| s.clone()).collect()
    }

    fn forbidding(categories: &[&str]) -> &'static EffectivePolicy {
        policy(PolicyRules {
            forbid: categories.iter().map(|c| c.to_string()).collect(),
            ..Default::default()
        })
    }

    #[tokio::test]
    async fn plans_then_runs_the_script_and_audits_summaries_only() {
        let (mut tools, server, log) = ksql_tools(orders_server(), config(), no_policy());
        let args = sql(&format!(
            "SET 'auto.offset.reset'='earliest';\n\
             CREATE STREAM big WITH (KAFKA_TOPIC='big') AS SELECT * FROM orders;\n\
             INSERT INTO orders (id, email) VALUES (1, '{PII}');"
        ));
        let plan = tools.call_token(KSQL_EXEC, &args).await;
        let structured = &plan["structuredContent"];
        assert_eq!(structured["plan"]["server"], "ksql.test:8088", "{plan}");
        assert_eq!(structured["plan"]["writes"], 2);
        let listed = &structured["plan"]["statements"];
        assert_eq!(listed[0]["access"], "session");
        assert_eq!(
            listed[1]["categories"],
            json!(["ksql.create", "topics.create"])
        );
        assert_eq!(
            listed[1]["topic"],
            json!({ "name": "big", "effect": "create" })
        );
        assert_eq!(listed[2]["categories"], json!(["ksql.insert", "produce"]));
        assert_eq!(listed[2]["topic"]["name"], "orders");
        assert!(structured["plan"]["warnings"][0]
            .as_str()
            .unwrap()
            .contains("persistent query"));
        assert_eq!(structured["confirmation"]["destructive"], false);
        // Plans show keywords and names, never the statement's values.
        assert!(!plan.to_string().contains(PII), "{plan}");
        assert!(ran(server).is_empty(), "a plan sends nothing");

        let done = tools
            .call_token(KSQL_EXEC, &execute(&args, &token(&plan)))
            .await;
        assert_eq!(done["structuredContent"]["executed"], true, "{done}");
        let statuses: Vec<&str> = done["structuredContent"]["result"]["statements"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s["status"].as_str().unwrap())
            .collect();
        assert_eq!(statuses, ["SESSION", "SUCCESS", "SUCCESS"]);
        assert_eq!(
            ran(server),
            ["CREATE STREAM BIG AS SELECT", "INSERT INTO ORDERS VALUES"]
        );
        // SET applies to the statements after it.
        assert_eq!(
            server.ran.borrow()[1]
                .1
                .get("auto.offset.reset")
                .map(String::as_str),
            Some("earliest")
        );
        let entries = log.entries.borrow();
        let writes: Vec<(&str, &str)> = entries
            .iter()
            .filter(|e| e.action == "mcp ksql_exec")
            .map(|e| (e.target.as_str(), e.outcome.as_str()))
            .collect();
        assert_eq!(
            writes,
            [
                ("CREATE STREAM BIG AS SELECT", "ok: SUCCESS, query CSAS_1"),
                ("INSERT INTO ORDERS VALUES", "ok: SUCCESS"),
            ]
        );
        assert!(entries
            .iter()
            .all(|e| e.source == "mcp" && e.actor.is_some()));
        assert!(!format!("{entries:?}").contains(PII));
    }

    #[tokio::test]
    async fn one_refused_category_refuses_the_whole_script_before_sending() {
        // INSERT also needs `produce`, as in `ferrika ksql exec`.
        let (mut tools, server, log) =
            ksql_tools(orders_server(), config(), forbidding(&["produce"]));
        let args = sql(&format!(
            "CREATE STREAM s (v STRING) WITH (KAFKA_TOPIC='s', VALUE_FORMAT='JSON');\
             INSERT INTO orders (email) VALUES ('{PII}');"
        ));
        let result = tools.call_token(KSQL_EXEC, &args).await;
        assert_eq!(code(&result), "policy_refused", "{result}");
        let message = result["structuredContent"]["error"]["message"]
            .as_str()
            .unwrap();
        assert!(
            message.contains("`INSERT INTO ORDERS VALUES` needs `produce`"),
            "{message}"
        );
        assert!(message.contains("Nothing was sent"));
        assert!(ran(server).is_empty());
        {
            let entries = log.entries.borrow();
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].action, "policy.blocked");
            assert_eq!(entries[0].target, "mcp ksql_exec INSERT INTO ORDERS VALUES");
            assert!(!format!("{entries:?}").contains(PII));
        }
        // Other ksqlDB categories stay allowed, so the tool is offered.
        assert!(tools
            .definitions(false)
            .iter()
            .any(|d| d["name"] == KSQL_EXEC));

        // DELETE TOPIC needs topics.delete too.
        let (mut tools, _, _) =
            ksql_tools(orders_server(), config(), forbidding(&["topics.delete"]));
        let drop = sql("DROP STREAM orders DELETE TOPIC;");
        assert_eq!(
            code(&tools.call_token(KSQL_EXEC, &drop).await),
            "policy_refused"
        );
        let plain = sql("DROP STREAM orders;");
        assert!(tools.call_token(KSQL_EXEC, &plain).await["isError"] == false);
    }

    #[tokio::test]
    async fn the_policy_matches_the_server_address() {
        let rules = PolicyRules {
            read_only_brokers: vec!["ksql.test:*".into()],
            ..Default::default()
        };
        let (mut tools, server, _) = ksql_tools(orders_server(), config(), policy(rules));
        let result = tools
            .call_token(KSQL_EXEC, &sql("DROP STREAM orders;"))
            .await;
        assert_eq!(code(&result), "policy_refused", "{result}");
        assert!(ran(server).is_empty());
    }

    #[tokio::test]
    async fn the_tool_is_hidden_when_every_ksql_category_or_ai_writes_are_refused() {
        let all = forbidding(&[
            "ksql.create",
            "ksql.drop",
            "ksql.insert",
            "ksql.terminate",
            "ksql.alter",
        ]);
        let (tools, _, _) = ksql_tools(orders_server(), config(), all);
        assert!(!tools
            .definitions(false)
            .iter()
            .any(|d| d["name"] == KSQL_EXEC));
        assert!(tools
            .definitions(false)
            .iter()
            .any(|d| d["name"] == "create_topic"));

        let forbidden = policy(PolicyRules {
            mcp_write: McpWriteRule::Forbidden,
            ..Default::default()
        });
        let (mut tools, server, log) = ksql_tools(orders_server(), config(), forbidden);
        assert!(tools.definitions(true).is_empty());
        let result = tools
            .call_token(KSQL_EXEC, &sql("DROP STREAM orders;"))
            .await;
        assert_eq!(code(&result), "policy_refused");
        assert!(ran(server).is_empty());
        let entries = log.entries.borrow();
        assert_eq!(entries[0].action, "policy.blocked");
        assert_eq!(entries[0].target, "mcp ksql_exec DROP STREAM ORDERS");
    }

    #[tokio::test]
    async fn a_read_only_profile_refuses_and_hides_the_tool() {
        let read_only = GuardConfig {
            profile_refusal: Some("Profile 'dev' is read-only".into()),
            ..config()
        };
        let (mut tools, server, log) = ksql_tools(orders_server(), read_only, no_policy());
        assert!(!tools
            .definitions(false)
            .iter()
            .any(|d| d["name"] == KSQL_EXEC));
        let result = tools
            .call_token(KSQL_EXEC, &sql("TERMINATE CSAS_X_1;"))
            .await;
        assert_eq!(code(&result), "read_only_profile");
        assert!(ran(server).is_empty());
        assert_eq!(log.entries.borrow()[0].target, "TERMINATE CSAS_X_1");
    }

    #[tokio::test]
    async fn topics_scope_limits_the_topics_statements_touch() {
        let scoped = GuardConfig {
            topics: vec!["ord*".into(), "kq_*".into()],
            ..config()
        };
        let (mut tools, server, _) = ksql_tools(
            orders_server().with_source("PAYMENTS", "payments"),
            scoped,
            no_policy(),
        );
        for ok in [
            "INSERT INTO orders (v) VALUES ('x');",
            "CREATE STREAM kq_a WITH (KAFKA_TOPIC='kq_a') AS SELECT * FROM orders;",
            // Touches no topic.
            "TERMINATE CSAS_X_1;",
            "DROP STREAM payments;",
        ] {
            let result = tools.call_token(KSQL_EXEC, &sql(ok)).await;
            assert_eq!(result["isError"], false, "{ok}: {result}");
        }
        for (refused, why) in [
            (
                "INSERT INTO payments (v) VALUES ('x');",
                "outside the topics",
            ),
            ("DROP STREAM payments DELETE TOPIC;", "outside the topics"),
            (
                "CREATE STREAM kq_b AS SELECT * FROM orders;",
                "KAFKA_TOPIC=",
            ),
            ("INSERT INTO nowhere (v) VALUES ('x');", "unknown"),
            ("DROP CONNECTOR c;", "cannot be checked"),
            ("RUN SCRIPT '/tmp/x.sql';", "cannot be checked"),
        ] {
            let result = tools.call_token(KSQL_EXEC, &sql(refused)).await;
            assert_eq!(code(&result), "target_not_allowed", "{refused}: {result}");
            assert!(result.to_string().contains(why), "{refused}: {result}");
        }
        assert!(ran(server).is_empty());

        // Internal topics are refused without --topics too.
        let (mut tools, _, _) = ksql_tools(
            fake::FakeKsql::default().with_source("OFFSETS", "__consumer_offsets"),
            config(),
            no_policy(),
        );
        let result = tools
            .call_token(KSQL_EXEC, &sql("INSERT INTO offsets (v) VALUES ('x');"))
            .await;
        assert_eq!(code(&result), "target_not_allowed", "{result}");
    }

    #[tokio::test]
    async fn deleting_a_topic_needs_a_person_who_types_the_name() {
        let (mut tools, server, _) = ksql_tools(orders_server(), config(), no_policy());
        let args = sql("DROP STREAM orders DELETE TOPIC;");
        let plan = tools.call_token(KSQL_EXEC, &args).await;
        assert_eq!(
            plan["structuredContent"]["confirmation"]["destructive"],
            true
        );
        assert!(plan.to_string().contains("deletes Kafka topic 'orders'"));
        let result = tools
            .call_token(KSQL_EXEC, &execute(&args, &token(&plan)))
            .await;
        assert_eq!(code(&result), "elicitation_required", "{result}");
        assert!(ran(server).is_empty());

        // A person confirms by typing the stream's name.
        let wrong =
            json!({ "action": "accept", "content": { "confirm": true, "type_name": "orders.v1" } });
        let mut execute_args = args.clone();
        execute_args["mode"] = json!("execute");
        let (result, asked) =
            call_asking(&mut tools, KSQL_EXEC, &execute_args, Client::Answer(wrong)).await;
        assert_eq!(code(&result), "user_declined", "{result}");
        let asked = asked.unwrap();
        assert!(asked["params"]["message"]
            .as_str()
            .unwrap()
            .contains("DROP STREAM ORDERS DELETE TOPIC"));
        assert!(ran(server).is_empty());
        let right =
            json!({ "action": "accept", "content": { "confirm": true, "type_name": "ORDERS" } });
        let (result, _) =
            call_asking(&mut tools, KSQL_EXEC, &execute_args, Client::Answer(right)).await;
        assert_eq!(result["structuredContent"]["executed"], true, "{result}");
        assert_eq!(ran(server), ["DROP STREAM ORDERS DELETE TOPIC"]);
    }

    #[tokio::test]
    async fn a_failure_stops_the_script_and_is_audited_without_the_server_message() {
        let failing = fake::FakeKsql {
            failing: vec!["INSERT INTO ORDERS VALUES".into()],
            ..orders_server()
        };
        let (mut tools, server, log) = ksql_tools(failing, config(), no_policy());
        let args = sql(&format!(
            "CREATE STREAM s (v STRING) WITH (KAFKA_TOPIC='s', VALUE_FORMAT='JSON');\
             INSERT INTO orders (email) VALUES ('{PII}');\
             DROP STREAM s;"
        ));
        let plan = tools.call_token(KSQL_EXEC, &args).await;
        let result = tools
            .call_token(KSQL_EXEC, &execute(&args, &token(&plan)))
            .await;
        assert_eq!(code(&result), "ksql_error", "{result}");
        let error = &result["structuredContent"]["error"];
        assert_eq!(error["retryable"], false);
        let statuses: Vec<&str> = error["statements"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s["status"].as_str().unwrap())
            .collect();
        assert_eq!(statuses, ["SUCCESS", "ERROR", "NOT RUN"]);
        assert_eq!(ran(server), ["CREATE STREAM S"]);
        let entries = log.entries.borrow();
        let outcomes: Vec<&str> = entries
            .iter()
            .filter(|e| e.action == "mcp ksql_exec")
            .map(|e| e.outcome.as_str())
            .collect();
        assert_eq!(
            outcomes,
            [
                "ok: SUCCESS",
                "failed: ksqlDB error 40001 (HTTP 400)",
                "not run (an earlier statement failed)"
            ]
        );
        // The server's message quotes the statement; the log never does.
        assert!(!format!("{entries:?}").contains(PII));
    }

    #[tokio::test]
    async fn inserts_count_against_the_produce_budget() {
        let small = GuardConfig {
            limits: super::super::guard::Limits {
                produce_records: 1,
                ..config().limits
            },
            ..config()
        };
        let (mut tools, server, _) = ksql_tools(orders_server(), small, no_policy());
        let args = sql("INSERT INTO orders (v) VALUES ('a'); INSERT INTO orders (v) VALUES ('b');");
        let plan = tools.call_token(KSQL_EXEC, &args).await;
        let result = tools
            .call_token(KSQL_EXEC, &execute(&args, &token(&plan)))
            .await;
        assert_eq!(code(&result), "produce_budget_exhausted", "{result}");
        assert!(ran(server).is_empty());
    }

    #[tokio::test]
    async fn without_a_server_the_call_says_how_to_add_one() {
        let (guard, _, _) = guard_with(config(), no_policy());
        let cluster: &'static FakeCluster = Box::leak(Box::new(FakeCluster::default()));
        let mut tools = WriteTools::new(guard, Box::new(cluster));
        let result = tools.call_token(KSQL_EXEC, &sql("DROP STREAM s;")).await;
        assert_eq!(code(&result), "invalid_argument");
        assert!(result.to_string().contains("--ksql-url"), "{result}");
    }

    #[test]
    fn the_definition_is_strict_and_destructive() {
        let def = &definitions()[0];
        assert_eq!(def["inputSchema"]["additionalProperties"], false);
        assert_eq!(def["annotations"]["destructiveHint"], true);
        assert_eq!(def["annotations"]["readOnlyHint"], false);
    }
}
