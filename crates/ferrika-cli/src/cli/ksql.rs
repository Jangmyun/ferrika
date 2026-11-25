//! `ferrika ksql` (preview): a client for an existing ksqlDB server. Ferrika
//! does not run or ship ksqlDB; it talks to the server's REST API.
//!
//! Governance (see `docs/plans/ksqldb-client.md`):
//!
//! - `info`, `streams`, `tables`, `queries`, `topics`, `describe` show
//!   metadata: no gate.
//! - `query` returns rows, so it passes the export gate in `cli/app.rs`
//!   like `consume` (a terminal is viewing; redirected stdout is an export,
//!   refused under `export: forbidden` and recorded under `export:
//!   audited`), and its rows are PII-masked with `--mask-pii` or when the
//!   policy enforces masking.
//! - `exec` gates itself, per statement: every write statement's policy
//!   categories ([`Statement::policy_actions`]) are checked before anything
//!   is sent, and one refusal refuses the whole script (recorded as
//!   `policy.blocked`). Then each write gets an audit request entry before
//!   it runs and a result entry after; statements after a failure are not
//!   run.
//!
//! The server is the one of the Kafka profile in use (`ksql_url` and its
//! TLS files), whose name and brokers the gates match, plus the server's
//! `host:port`. `--ksql-url` names a server by hand: then no profile is
//! used (its stored password never goes to a URL typed by hand) and the
//! gates match the server's address only.

use std::time::Duration;

use clap::{Args, Subcommand};
use colored::Colorize;
use comfy_table::Cell;
use serde_json::json;

use crate::cli::{output, GlobalOpts, OutputFormat};
use crate::governance;
use crate::ksql::client::{KsqlClient, QueryLimits, Session};
use crate::ksql::config::KsqlConfig;
use crate::ksql::model::{QueryEnd, QueryHeader};
use crate::ksql::statement::{split, Access, Statement, StatementKind};
use crate::ksql::view::{cell_text, mask_row};

/// Request timeout when `--timeout` is not given.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);
/// The most rows one `ksql query` returns.
const MAX_ROWS: usize = 100_000;
/// Longest query text shown in the `queries` table.
const TABLE_QUERY_CHARS: usize = 80;

/// Printed once per command when the URL carries credentials.
const URL_CREDENTIALS_WARNING: &str =
    "Warning: the ksqlDB URL contains credentials, which can end up in shell history. \
     Store them in the profile instead: ferrika profile add NAME -b BROKERS --ksql-url URL \
     --ksql-user USER --ksql-password-stdin";

#[derive(Args, Debug, Clone)]
pub struct KsqlArgs {
    #[command(subcommand)]
    pub command: KsqlSubcommands,
    #[command(flatten)]
    pub connection: KsqlConnection,
    /// The server the options and the Kafka profile name, filled in by
    /// `main` before the policy gates run (see [`resolve`]).
    #[arg(skip)]
    pub resolved: Option<ResolvedKsql>,
}

/// Which ksqlDB server to talk to: the Kafka profile's, or one given here.
#[derive(Args, Clone)]
pub struct KsqlConnection {
    /// ksqlDB server URL, e.g. http://ksql:8088 (default: the profile's
    /// ksql_url). Uses no profile: neither its login nor its TLS files
    #[arg(
        long = "ksql-url",
        env = "FERRIKA_KSQL_URL",
        global = true,
        value_name = "URL"
    )]
    pub url: Option<String>,
    /// ksqlDB user (replaces the profile's login)
    #[arg(long = "ksql-user", env = "FERRIKA_KSQL_USER", global = true)]
    pub user: Option<String>,
    /// ksqlDB password; prefer the environment variable or
    /// --ksql-password-stdin, which keep it out of the process list
    #[arg(
        long = "ksql-password",
        env = "FERRIKA_KSQL_PASSWORD",
        hide_env_values = true,
        global = true
    )]
    pub password: Option<String>,
    /// Read the ksqlDB password from the first line of stdin
    #[arg(long = "ksql-password-stdin", global = true)]
    pub password_stdin: bool,
}

// Manual so the password never reaches debug output.
impl std::fmt::Debug for KsqlConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KsqlConnection")
            .field(
                "url",
                &self.url.as_deref().map(crate::model::format::redact_url),
            )
            .field("user", &self.user)
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .field("password_stdin", &self.password_stdin)
            .finish()
    }
}

/// The server a `ksql` command talks to, resolved once in `main`.
#[derive(Debug, Clone)]
pub struct ResolvedKsql {
    pub config: KsqlConfig,
    /// The Kafka profile the server belongs to; `None` with `--ksql-url`.
    pub profile: Option<String>,
    /// The profile is read-only: `exec` refuses writes.
    pub read_only: bool,
    /// Credentials were given inside a hand-typed URL.
    pub url_has_credentials: bool,
}

#[derive(Subcommand, Debug, Clone, PartialEq, Eq)]
pub enum KsqlSubcommands {
    /// Server version, Kafka cluster id, service id and status
    Info,
    /// List the streams (SHOW STREAMS)
    Streams,
    /// List the tables (SHOW TABLES)
    Tables,
    /// List the persistent queries (SHOW QUERIES)
    Queries,
    /// List the Kafka topics the server sees (SHOW TOPICS)
    Topics,
    /// Show a stream's or table's columns, topic, formats and queries
    Describe {
        /// Stream or table name
        name: String,
        /// Include runtime statistics (DESCRIBE ... EXTENDED)
        #[arg(long)]
        extended: bool,
    },
    /// Run one SELECT: a pull query, or a push query (EMIT CHANGES) that
    /// stops after --limit rows, after --timeout, or on Ctrl+C, and is then
    /// closed on the server. Rows are record data: exporting them follows
    /// the company policy like `consume`
    Query(KsqlQueryArgs),
    /// Run statements (CREATE, DROP, INSERT, TERMINATE, ...; several
    /// separated by ;). Each write statement passes the company policy for
    /// its categories (ksql.create, ksql.drop, ksql.insert, ksql.terminate,
    /// ksql.alter; DELETE TOPIC also topics.delete, INSERT also produce)
    /// before anything is sent, and is recorded in the audit log
    Exec(KsqlExecArgs),
}

#[derive(Args, Debug, Clone, PartialEq, Eq)]
pub struct KsqlQueryArgs {
    /// The SELECT statement
    pub sql: String,
    /// Stop after this many rows and close the query (1 to 100000)
    #[arg(short = 'n', long, default_value_t = 100)]
    pub limit: usize,
    /// Stop after this many seconds and close the query
    #[arg(long, value_name = "SECS")]
    pub timeout: Option<u64>,
    /// Read the source from its earliest records (auto.offset.reset=earliest)
    #[arg(long)]
    pub from_beginning: bool,
    /// Query property KEY=VALUE (can be given several times)
    #[arg(short = 'P', long = "property", value_name = "KEY=VALUE")]
    pub properties: Vec<String>,
    /// Mask PII in the rows (always on when a company policy enforces masking)
    #[arg(long = "mask-pii")]
    pub mask_pii: bool,
}

#[derive(Args, Debug, Clone, PartialEq, Eq)]
pub struct KsqlExecArgs {
    /// Statements to run, separated by ;
    #[arg(required_unless_present = "file", conflicts_with = "file")]
    pub sql: Option<String>,
    /// Read the statements from a file (- for stdin)
    #[arg(short = 'f', long, value_name = "PATH")]
    pub file: Option<String>,
    /// Show the statements, their policy categories and whether the policy
    /// allows them, and send nothing
    #[arg(long)]
    pub dry_run: bool,
    /// Do not ask for confirmation
    #[arg(short = 'y', long)]
    pub yes: bool,
}

impl KsqlConnection {
    /// The password given by option, environment or stdin (read once).
    fn given_password(&self) -> Result<Option<String>, String> {
        if self.password_stdin {
            crate::cli::read_password_stdin().map(Some)
        } else {
            Ok(self.password.clone())
        }
    }
}

/// Fills in `args.resolved` from the options and the Kafka profile `main`
/// resolved in `global`.
pub fn resolve(args: &mut KsqlArgs, global: &GlobalOpts) -> Result<(), String> {
    let context = global.context();
    let profile = context
        .profile_name
        .as_deref()
        .and_then(|name| context.store.get(name));
    let timeout = global
        .timeout
        .map(Duration::from_secs)
        .unwrap_or(DEFAULT_TIMEOUT);
    let resolved = resolve_with(&args.connection, profile, timeout)?;
    args.resolved = Some(resolved);
    Ok(())
}

/// [`resolve`] against `profile` (the Kafka profile in use, if any).
fn resolve_with(
    connection: &KsqlConnection,
    profile: Option<&crate::kafka::profile::ClusterProfile>,
    timeout: Duration,
) -> Result<ResolvedKsql, String> {
    let (mut config, profile_name, read_only, url_has_credentials) = match &connection.url {
        Some(url) => (
            KsqlConfig::new(url),
            None,
            false,
            crate::kafka::profile::url_has_credentials(url),
        ),
        None => {
            let profile = profile.ok_or_else(|| {
                "No ksqlDB server: pass --ksql-url http://host:8088 (or set FERRIKA_KSQL_URL), \
                 or add one to the profile with `ferrika profile add NAME -b BROKERS --ksql-url URL`"
                    .to_string()
            })?;
            let config = KsqlConfig::from_profile(profile).ok_or_else(|| {
                format!(
                    "Profile '{}' has no ksqlDB server: add one with `ferrika profile add {} \
                     -b ... --ksql-url URL`, or pass --ksql-url",
                    profile.name, profile.name
                )
            })?;
            (config, Some(profile.name.clone()), profile.read_only, false)
        }
    };
    let password = connection.given_password()?;
    match (&connection.user, password) {
        (Some(user), Some(password)) => config = config.with_credentials(user, password),
        (Some(_), None) => {
            return Err(
                "The ksqlDB user needs a password: set FERRIKA_KSQL_PASSWORD or pass \
                 --ksql-password-stdin"
                    .to_string(),
            )
        }
        (None, Some(_)) => {
            return Err("A ksqlDB password was given without --ksql-user".to_string());
        }
        (None, None) => {}
    }
    config = config.with_timeout(timeout);
    // Fails early on a bad URL, before the gates use its address.
    config.address().map_err(|e| e.to_string())?;
    Ok(ResolvedKsql {
        config,
        profile: profile_name,
        read_only,
        url_has_credentials,
    })
}

fn resolved(args: &KsqlArgs) -> Result<&ResolvedKsql, String> {
    args.resolved
        .as_ref()
        .ok_or_else(|| "No ksqlDB server was resolved".to_string())
}

/// The profile name the gates and the audit log use: the Kafka profile the
/// server belongs to, or empty with `--ksql-url` (entries then name the
/// addresses).
pub fn gate_profile(args: &KsqlArgs) -> String {
    args.resolved
        .as_ref()
        .and_then(|r| r.profile.clone())
        .unwrap_or_default()
}

/// The addresses the policy rules match: the server's `host:port` and, for
/// a profile's server, the profile's brokers (plus, when `needed`, the
/// brokers its cluster reports): ksqlDB writes into that cluster.
pub fn gate_address(args: &KsqlArgs, global: &GlobalOpts, needed: bool) -> Result<String, String> {
    let resolved = resolved(args)?;
    let address = resolved.config.address().map_err(|e| e.to_string())?;
    if resolved.profile.is_none() {
        return Ok(address);
    }
    let brokers = governance::policy::brokers_for(&global.to_kafka_config(), needed)?;
    Ok(format!("{address},{brokers}"))
}

/// The write the gate in `main` checks for `args`. Always `None`: reads
/// need no write gate, and `exec` passes the gate itself once per write
/// statement, since one script can need several categories. No wildcard
/// arm: a new subcommand must be classified.
pub fn write_action(args: &KsqlArgs) -> Option<&'static str> {
    match &args.command {
        KsqlSubcommands::Info
        | KsqlSubcommands::Streams
        | KsqlSubcommands::Tables
        | KsqlSubcommands::Queries
        | KsqlSubcommands::Topics
        | KsqlSubcommands::Describe { .. }
        | KsqlSubcommands::Query(_) => None,
        // Gated per statement in `run_exec`.
        KsqlSubcommands::Exec(_) => None,
    }
}

/// The record data `args` shows, for the export gate: `(label, target)`.
/// The target names the query's sources, never the statement's literals.
pub(crate) fn export(args: &KsqlArgs) -> Option<(&'static str, String)> {
    match &args.command {
        KsqlSubcommands::Query(q) => {
            let target = Statement::parse(&q.sql)
                .map(|s| s.summary())
                .unwrap_or_else(|_| "unparsed query".to_string());
            Some(("ksql query", target))
        }
        // Metadata only; `exec` refuses queries.
        KsqlSubcommands::Info
        | KsqlSubcommands::Streams
        | KsqlSubcommands::Tables
        | KsqlSubcommands::Queries
        | KsqlSubcommands::Topics
        | KsqlSubcommands::Describe { .. }
        | KsqlSubcommands::Exec(_) => None,
    }
}

pub(crate) async fn run(args: KsqlArgs, global: &GlobalOpts) -> anyhow::Result<()> {
    let json = global.format == OutputFormat::Json;
    let resolved = resolved(&args).map_err(anyhow::Error::msg)?;
    if resolved.url_has_credentials {
        eprintln!("{}", URL_CREDENTIALS_WARNING.yellow());
    }
    let client = KsqlClient::new(&resolved.config)?;
    match &args.command {
        KsqlSubcommands::Info => {
            let info = client.info().await?;
            output::emit(json, &info, |info| {
                let mut table = output::untitled_table();
                table.add_row(vec!["Server", &client.address()]);
                table.add_row(vec!["Version", &info.version]);
                table.add_row(vec!["Kafka cluster id", &info.kafka_cluster_id]);
                table.add_row(vec!["Service id", &info.ksql_service_id]);
                table.add_row(vec![
                    "Status",
                    info.server_status.as_deref().unwrap_or("unknown"),
                ]);
                println!("{table}");
            })?;
        }
        KsqlSubcommands::Streams | KsqlSubcommands::Tables => {
            let sources = match args.command {
                KsqlSubcommands::Streams => client.streams().await?,
                _ => client.tables().await?,
            };
            output::emit(json, &sources, |sources| {
                let mut table = output::table(vec![
                    "NAME",
                    "TOPIC",
                    "KEY FORMAT",
                    "VALUE FORMAT",
                    "WINDOWED",
                ]);
                for s in sources {
                    table.add_row(vec![
                        Cell::new(&s.name),
                        Cell::new(&s.topic),
                        Cell::new(&s.key_format),
                        Cell::new(&s.value_format),
                        Cell::new(s.is_windowed),
                    ]);
                }
                println!("{table}");
            })?;
        }
        KsqlSubcommands::Queries => {
            let queries = client.queries().await?;
            output::emit(json, &queries, |queries| {
                let mut table = output::table(vec!["ID", "STATE", "SINKS", "QUERY"]);
                for q in queries {
                    table.add_row(vec![
                        Cell::new(&q.id),
                        Cell::new(q.state.as_deref().unwrap_or_default()),
                        Cell::new(q.sinks.join(", ")),
                        Cell::new(crate::codec::pii::ellipsize(
                            &q.query_string.replace('\n', " "),
                            TABLE_QUERY_CHARS,
                        )),
                    ]);
                }
                println!("{table}");
            })?;
        }
        KsqlSubcommands::Topics => {
            let topics = client.topics().await?;
            output::emit(json, &topics, |topics| {
                let mut table = output::table(vec!["NAME", "PARTITIONS", "REPLICAS"]);
                for t in topics {
                    let replicas = t.replica_info.iter().copied().max().unwrap_or_default();
                    table.add_row(vec![
                        Cell::new(&t.name),
                        Cell::new(t.partitions()),
                        Cell::new(replicas),
                    ]);
                }
                println!("{table}");
            })?;
        }
        KsqlSubcommands::Describe { name, extended } => {
            let d = client.describe(name, *extended).await?;
            output::emit(json, &d, |d| {
                let mut head = output::untitled_table();
                head.add_row(vec!["Name", &d.name]);
                head.add_row(vec!["Type", &d.kind]);
                head.add_row(vec!["Topic", &d.topic]);
                head.add_row(vec!["Key format", &d.key_format]);
                head.add_row(vec!["Value format", &d.value_format]);
                if let Some(window) = &d.window_type {
                    head.add_row(vec!["Window", window]);
                }
                let ids = |qs: &[crate::ksql::model::QueryInfo]| {
                    qs.iter()
                        .map(|q| q.id.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                };
                head.add_row(vec!["Read by", &ids(&d.read_queries)]);
                head.add_row(vec!["Written by", &ids(&d.write_queries)]);
                println!("{head}");
                let mut fields = output::table(vec!["COLUMN", "TYPE", "KIND"]);
                for f in &d.fields {
                    fields.add_row(vec![
                        Cell::new(&f.name),
                        Cell::new(f.schema.render()),
                        Cell::new(f.column_type.as_deref().unwrap_or_default()),
                    ]);
                }
                println!("{fields}");
                if !d.statistics.trim().is_empty() {
                    println!("{}", d.statistics.trim());
                }
            })?;
        }
        KsqlSubcommands::Query(q) => run_query(&client, q, json).await?,
        KsqlSubcommands::Exec(e) => run_exec(&client, &args, e, global, json).await?,
    }
    Ok(())
}

/// The session of a query: `-P` properties and `--from-beginning`.
fn query_session(q: &KsqlQueryArgs) -> anyhow::Result<Session> {
    let mut session = Session::default();
    if q.from_beginning {
        session
            .properties
            .insert("auto.offset.reset".to_string(), "earliest".to_string());
    }
    for property in &q.properties {
        let (key, value) = property
            .split_once('=')
            .ok_or_else(|| anyhow::anyhow!("--property needs KEY=VALUE, got '{property}'"))?;
        session
            .properties
            .insert(key.trim().to_string(), value.trim().to_string());
    }
    Ok(session)
}

/// Why `sql` cannot run as `ksql query`, if it cannot.
fn query_refusal(statement: &Statement) -> Option<String> {
    match statement.kind {
        StatementKind::Select { .. } => None,
        StatementKind::Print => Some(
            "PRINT shows raw topic records; read them with `ferrika consume TOPIC`, which \
             decodes and masks them"
                .to_string(),
        ),
        _ => Some(format!(
            "`ksql query` runs one SELECT; run `{}` with `ferrika ksql exec`",
            statement.summary()
        )),
    }
}

async fn run_query(client: &KsqlClient, q: &KsqlQueryArgs, json: bool) -> anyhow::Result<()> {
    let statement = Statement::parse(&q.sql)?;
    if let Some(reason) = query_refusal(&statement) {
        anyhow::bail!(reason);
    }
    if q.limit == 0 || q.limit > MAX_ROWS {
        anyhow::bail!("--limit must be between 1 and {MAX_ROWS}");
    }
    let session = query_session(q)?;
    let policy = governance::policy::current();
    let mask = q.mask_pii || policy.masking_enforced();
    crate::cli::note_masking(policy);
    let limits = QueryLimits {
        max_rows: q.limit,
        timeout: q.timeout.map(Duration::from_secs),
    };
    let push = matches!(statement.kind, StatementKind::Select { push: true });
    if push && !json {
        eprintln!(
            "{}",
            format!(
                "Push query: stops after {} row(s){}, or on Ctrl+C.",
                q.limit,
                q.timeout.map(|s| format!(" or {s}s")).unwrap_or_default()
            )
            .bright_black()
        );
    }
    let cancel = async {
        if tokio::signal::ctrl_c().await.is_err() {
            std::future::pending::<()>().await;
        }
    };
    let mut printed_header = false;
    let live = push && !json;
    let mut print_live = |header: &QueryHeader, row: &[serde_json::Value]| {
        if !live {
            return;
        }
        if !printed_header {
            let names: Vec<&str> = header.columns.iter().map(|c| c.name.as_str()).collect();
            println!("{}", names.join(" | ").bold());
            printed_header = true;
        }
        let shown = if mask {
            mask_row(&header.columns, row)
        } else {
            row.to_vec()
        };
        println!(
            "{}",
            shown.iter().map(cell_text).collect::<Vec<_>>().join(" | ")
        );
    };
    let result = client
        .run_query(&statement.text, &session, limits, cancel, &mut print_live)
        .await?;
    let result = if mask { result.masked() } else { result };
    crate::cli::export_finished(result.rows.len() as u64);
    if json {
        output::print_json(&json!({
            "query_id": result.query_id,
            "push": result.push,
            "columns": result.columns,
            "rows": result.row_objects(),
            "end": result.end,
            "masked": mask,
        }))?;
        return Ok(());
    }
    if !result.push {
        let mut table = output::table(
            result
                .columns
                .iter()
                .map(|c| c.name.clone())
                .collect::<Vec<_>>(),
        );
        for row in &result.rows {
            table.add_row(row.iter().map(cell_text).collect::<Vec<_>>());
        }
        println!("{table}");
    }
    let why = match result.end {
        QueryEnd::Completed => "the query ended",
        QueryEnd::RowLimit => "row limit reached, query closed",
        QueryEnd::TimeLimit => "time limit reached, query closed",
        QueryEnd::Cancelled => "cancelled, query closed",
    };
    eprintln!(
        "{}",
        format!("{} row(s); {why}.", result.rows.len()).bright_black()
    );
    Ok(())
}

/// The statements `exec` runs: the argument, a file, or stdin (`-f -`).
fn exec_text(e: &KsqlExecArgs) -> anyhow::Result<String> {
    match (&e.sql, e.file.as_deref()) {
        (Some(sql), _) => Ok(sql.clone()),
        (None, Some("-")) => {
            let mut text = String::new();
            std::io::Read::read_to_string(&mut std::io::stdin(), &mut text)
                .map_err(|err| anyhow::anyhow!("Cannot read statements from stdin: {err}"))?;
            Ok(text)
        }
        (None, Some(path)) => std::fs::read_to_string(path)
            .map_err(|err| anyhow::anyhow!("Cannot read '{path}': {err}")),
        (None, None) => anyhow::bail!("Give the statements or --file"),
    }
}

fn access_name(access: Access) -> &'static str {
    match access {
        Access::Read => "read",
        Access::Session => "session",
        Access::Write => "write",
    }
}

fn categories(statement: &Statement) -> Vec<&'static str> {
    statement
        .policy_actions()
        .into_iter()
        .map(|a| a.id())
        .collect()
}

/// The audit profile: the profile, or the addresses without one.
fn audit_profile<'a>(profile: &'a str, bootstrap: &'a str) -> &'a str {
    if profile.is_empty() {
        bootstrap
    } else {
        profile
    }
}

/// Why the policy or the profile refuses `statements`, checked for every
/// category of every write before anything is sent (see
/// [`authorize_all_with`]).
fn authorize_all(
    statements: &[Statement],
    resolved: &ResolvedKsql,
    profile: &str,
    bootstrap: &str,
) -> Result<(), String> {
    authorize_all_with(
        governance::policy::current(),
        &governance::policy::AuditLog,
        statements,
        resolved,
        profile,
        bootstrap,
    )
}

/// [`authorize_all`] against `policy`, recording to `sink`: one refused
/// category of one write refuses the whole script (recorded as
/// `policy.blocked`), and so does a read-only profile (recorded as a
/// refused request).
fn authorize_all_with(
    policy: &governance::policy::EffectivePolicy,
    sink: &impl governance::policy::AuditSink,
    statements: &[Statement],
    resolved: &ResolvedKsql,
    profile: &str,
    bootstrap: &str,
) -> Result<(), String> {
    let writes: Vec<&Statement> = statements.iter().filter(|s| s.is_write()).collect();
    for statement in &writes {
        for action in statement.policy_actions() {
            if let Err(reason) = policy.check(action, profile, bootstrap) {
                let attempted = format!("ksql exec {}", statement.summary());
                let entry = governance::policy::blocked_entry(
                    "cli",
                    audit_profile(profile, bootstrap),
                    &attempted,
                    &reason,
                );
                if let Err(e) = sink.record(&entry) {
                    tracing::warn!(%e, "Could not write the audit log");
                }
                return Err(format!(
                    "{} (`{}` needs `{}`). Nothing was sent.",
                    &*reason,
                    statement.summary(),
                    action.id()
                ));
            }
        }
    }
    if let (true, Some(first), Some(name)) = (resolved.read_only, writes.first(), &resolved.profile)
    {
        let reason =
            format!("Profile '{name}' is read-only, so `ksql exec` is refused. Nothing was sent.");
        let target = first.summary();
        let request = write_request(first, &target, profile, bootstrap);
        let _ = governance::policy::authorize_write_with(
            policy,
            sink,
            &request,
            governance::policy::AuditMode::RecordRequest,
            || Err(reason.clone()),
        );
        return Err(reason);
    }
    Ok(())
}

fn write_request<'a>(
    statement: &Statement,
    target: &'a str,
    profile: &'a str,
    bootstrap: &'a str,
) -> governance::policy::WriteRequest<'a> {
    governance::policy::WriteRequest {
        source: "cli",
        action: statement
            .policy_actions()
            .first()
            .copied()
            .unwrap_or(crate::model::policy::PolicyAction::ConfigChange),
        label: "ksql exec",
        target,
        profile,
        bootstrap,
        actor: None,
        op: None,
    }
}

/// Writes the result entry of one write statement.
fn record_result(profile: &str, bootstrap: &str, target: &str, ok: bool, outcome: &str) {
    governance::audit::record(&governance::audit::AuditEntry {
        time_ms: governance::audit::now_ms(),
        source: "cli".to_string(),
        profile: audit_profile(profile, bootstrap).to_string(),
        action: "ksql exec".to_string(),
        target: target.to_string(),
        outcome: outcome.to_string(),
        ok,
        user: String::new(),
        actor: None,
        op: String::new(),
        op_digest: String::new(),
    });
}

async fn run_exec(
    client: &KsqlClient,
    args: &KsqlArgs,
    e: &KsqlExecArgs,
    global: &GlobalOpts,
    json: bool,
) -> anyhow::Result<()> {
    let statements = split(&exec_text(e)?)?;
    if let Some(query) = statements.iter().find(|s| s.is_query()) {
        anyhow::bail!(
            "`ksql exec` does not run queries ({}); use `ferrika ksql query`. Nothing was sent.",
            query.summary()
        );
    }
    let resolved = resolved(args).map_err(anyhow::Error::msg)?;
    let policy = governance::policy::current();
    let profile = gate_profile(args);
    let bootstrap = gate_address(args, global, policy.write_needs_broker_metadata())
        .map_err(anyhow::Error::msg)?;
    let writes = statements.iter().filter(|s| s.is_write()).count();

    if e.dry_run {
        let verdicts: Vec<Option<String>> = statements
            .iter()
            .map(|s| {
                s.policy_actions()
                    .into_iter()
                    .find_map(|a| policy.check(a, &profile, &bootstrap).err())
                    .map(|reason| reason.to_string())
                    .or_else(|| {
                        (s.is_write() && resolved.read_only)
                            .then(|| "the profile is read-only".to_string())
                    })
            })
            .collect();
        if json {
            let list: Vec<serde_json::Value> = statements
                .iter()
                .zip(&verdicts)
                .map(|(s, refused)| {
                    json!({
                        "statement": s.summary(),
                        "access": access_name(s.access()),
                        "categories": categories(s),
                        "refused": refused,
                    })
                })
                .collect();
            output::print_json(
                &json!({ "dry_run": true, "server": client.address(), "statements": list }),
            )?;
            return Ok(());
        }
        let mut table = output::table(vec!["#", "STATEMENT", "ACCESS", "CATEGORIES", "POLICY"]);
        for (i, (s, refused)) in statements.iter().zip(&verdicts).enumerate() {
            table.add_row(vec![
                Cell::new(i + 1),
                Cell::new(s.summary()),
                Cell::new(access_name(s.access())),
                Cell::new(categories(s).join(", ")),
                Cell::new(match refused {
                    Some(reason) => format!("refused: {reason}"),
                    None if s.is_write() => "allowed".to_string(),
                    None => String::new(),
                }),
            ]);
        }
        println!("{table}");
        eprintln!("{}", "Dry run: nothing was sent.".bright_black());
        return Ok(());
    }

    if writes > 0 {
        authorize_all(&statements, resolved, &profile, &bootstrap).map_err(anyhow::Error::msg)?;
        if !e.yes {
            for (i, s) in statements.iter().enumerate() {
                eprintln!(
                    "  {}. {} {}",
                    i + 1,
                    s.summary(),
                    if s.is_write() {
                        format!("[{}]", categories(s).join(", ")).yellow()
                    } else {
                        "".normal()
                    }
                );
            }
            crate::cli::offset::confirm(
                &format!(
                    "Run {} statement(s), {writes} of them changing ksqlDB at {}? [y/N] ",
                    statements.len(),
                    client.address()
                ),
                "Aborted: nothing was sent.",
            )?;
        }
        // Each write's request entry, before anything is sent.
        let summaries: Vec<String> = statements.iter().map(Statement::summary).collect();
        for (i, s) in statements.iter().enumerate().filter(|(_, s)| s.is_write()) {
            let request = write_request(s, &summaries[i], &profile, &bootstrap);
            if let Err(refusal) = governance::policy::authorize_write(
                &request,
                governance::policy::AuditMode::RecordRequest,
                || Ok(()),
            ) {
                for (j, earlier) in statements.iter().enumerate().take(i) {
                    if earlier.is_write() {
                        record_result(&profile, &bootstrap, &summaries[j], false, "not run");
                    }
                }
                anyhow::bail!("{refusal} Nothing was sent.");
            }
        }
    }

    let mut session = Session::default();
    let mut reports = Vec::new();
    let mut failure = None;
    for s in &statements {
        let summary = s.summary();
        if failure.is_some() {
            if s.is_write() {
                record_result(
                    &profile,
                    &bootstrap,
                    &summary,
                    false,
                    "not run (an earlier statement failed)",
                );
            }
            reports.push(json!({ "statement": summary, "status": "NOT RUN" }));
            continue;
        }
        if session.apply(s) {
            reports.push(json!({ "statement": summary, "status": "SESSION" }));
            continue;
        }
        match client.execute(s, &session).await {
            Ok(result) => {
                if s.is_write() {
                    let mut outcome = format!("ok: {}", result.status);
                    if let Some(id) = &result.query_id {
                        outcome.push_str(&format!(", query {id}"));
                    }
                    record_result(&profile, &bootstrap, &summary, true, &outcome);
                }
                reports.push(json!({
                    "statement": summary,
                    "status": result.status,
                    "message": result.message,
                    "query_id": result.query_id,
                    "warnings": result.warnings,
                    "entity": result.entity,
                    "result": (s.access() == Access::Read).then_some(&result.raw),
                }));
            }
            Err(err) => {
                if s.is_write() {
                    record_result(&profile, &bootstrap, &summary, false, &err.audit_reason());
                }
                reports.push(json!({
                    "statement": summary,
                    "status": "ERROR",
                    "error": err.to_string(),
                }));
                failure = Some(err);
            }
        }
    }
    if json {
        output::print_json(&json!({
            "server": client.address(),
            "ok": failure.is_none(),
            "statements": reports,
        }))?;
    } else {
        let mut table = output::table(vec!["#", "STATEMENT", "STATUS", "MESSAGE"]);
        for (i, r) in reports.iter().enumerate() {
            let text = |key: &str| r[key].as_str().unwrap_or_default().to_string();
            let message = match (text("error"), text("message"), &r["result"]) {
                (error, _, _) if !error.is_empty() => error,
                (_, message, _) if !message.is_empty() => message,
                (_, _, serde_json::Value::Null) => String::new(),
                (_, _, raw) => crate::codec::pii::ellipsize(&raw.to_string(), 200),
            };
            table.add_row(vec![
                Cell::new(i + 1),
                Cell::new(text("statement")),
                Cell::new(text("status")),
                Cell::new(message),
            ]);
        }
        println!("{table}");
    }
    match failure {
        Some(err) => Err(anyhow::anyhow!(
            "{err}\nStatements after the failed one were not run."
        )),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::profile::ClusterProfile;

    fn connection(url: Option<&str>) -> KsqlConnection {
        KsqlConnection {
            url: url.map(str::to_string),
            user: None,
            password: None,
            password_stdin: false,
        }
    }

    #[test]
    fn the_profile_server_is_used_unless_a_url_is_given() {
        let mut profile = ClusterProfile::new("prod", "kafka.prod:9092");
        profile.read_only = true;
        let err = resolve_with(&connection(None), Some(&profile), DEFAULT_TIMEOUT).unwrap_err();
        assert!(err.contains("has no ksqlDB server"), "{err}");
        assert!(resolve_with(&connection(None), None, DEFAULT_TIMEOUT)
            .unwrap_err()
            .contains("--ksql-url"));

        profile.ksql_url = Some("https://ops:pw@ksql.prod:8088".into());
        let r = resolve_with(&connection(None), Some(&profile), DEFAULT_TIMEOUT).unwrap();
        assert_eq!(r.profile.as_deref(), Some("prod"));
        assert!(r.read_only);
        assert_eq!(r.config.user.as_deref(), Some("ops"));
        assert!(!r.url_has_credentials);

        // A URL by hand: no profile, so none of its login or read-only mark.
        let r = resolve_with(
            &connection(Some("http://other:8088")),
            Some(&profile),
            Duration::from_secs(5),
        )
        .unwrap();
        assert_eq!(r.profile, None);
        assert!(!r.read_only);
        assert_eq!(r.config.user, None);
        assert_eq!(r.config.timeout, Duration::from_secs(5));

        let mut c = connection(Some("http://other:8088"));
        c.user = Some("u".into());
        assert!(resolve_with(&c, None, DEFAULT_TIMEOUT)
            .unwrap_err()
            .contains("needs a password"));
        c.password = Some("p".into());
        let r = resolve_with(&c, None, DEFAULT_TIMEOUT).unwrap();
        assert!(r.config.has_password());
        let shown = format!("{c:?}");
        assert!(!shown.contains("\"p\""), "{shown}");
        c.user = None;
        assert!(resolve_with(&c, None, DEFAULT_TIMEOUT).is_err());
        assert!(
            resolve_with(
                &connection(Some("http://a:b@k:8088")),
                None,
                DEFAULT_TIMEOUT
            )
            .unwrap()
            .url_has_credentials
        );
    }

    #[test]
    fn query_accepts_selects_only() {
        let refusal = |sql: &str| query_refusal(&Statement::parse(sql).unwrap());
        assert_eq!(refusal("SELECT * FROM t;"), None);
        assert_eq!(refusal("select * from t emit changes"), None);
        assert!(refusal("PRINT t;").unwrap().contains("ferrika consume"));
        assert!(refusal("DROP STREAM t;").unwrap().contains("ksql exec"));
        assert!(refusal("SHOW STREAMS;").unwrap().contains("ksql exec"));
    }

    fn resolved_for(profile: Option<&str>, read_only: bool) -> ResolvedKsql {
        ResolvedKsql {
            config: KsqlConfig::new("http://ksql.prod.corp:8088"),
            profile: profile.map(str::to_string),
            read_only,
            url_has_credentials: false,
        }
    }

    #[test]
    fn one_refused_category_refuses_the_whole_script_before_sending() {
        use crate::governance::test_policy::{self, MemoryLog};
        let script = split(
            "CREATE STREAM kq_a (v STRING) WITH (KAFKA_TOPIC='kq_a', VALUE_FORMAT='JSON');             SHOW STREAMS;             DROP STREAM kq_a DELETE TOPIC;",
        )
        .unwrap();
        let resolved = resolved_for(Some("prod"), false);
        let bootstrap = "ksql.prod.corp:8088,kafka.prod.corp:9092";

        // No policy: allowed, nothing recorded yet (the requests are
        // recorded right before sending).
        let log = MemoryLog::default();
        authorize_all_with(
            &test_policy::none(),
            &log,
            &script,
            &resolved,
            "prod",
            bootstrap,
        )
        .unwrap();
        assert!(log.entries.borrow().is_empty());

        // `topics.delete` forbidden: the DROP ... DELETE TOPIC needs it, so
        // the CREATE before it is refused too.
        let mut policy = test_policy::masking();
        policy.rules.masking = crate::model::policy::MaskingRule::Optional;
        policy.rules.forbid = vec!["topics.delete".into()];
        let log = MemoryLog::default();
        let err =
            authorize_all_with(&policy, &log, &script, &resolved, "prod", bootstrap).unwrap_err();
        assert!(err.contains("topics.delete"), "{err}");
        assert!(err.contains("Nothing was sent"), "{err}");
        let entries = log.entries.take();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].action, "policy.blocked");
        assert_eq!(entries[0].target, "ksql exec DROP STREAM KQ_A DELETE TOPIC");
        assert_eq!(entries[0].profile, "prod");

        // `ksql.insert` forbidden: a script without INSERT passes.
        policy.rules.forbid = vec!["ksql.insert".into()];
        authorize_all_with(
            &policy,
            &MemoryLog::default(),
            &script,
            &resolved,
            "prod",
            bootstrap,
        )
        .unwrap();
        // ... and an unknown statement needs every category.
        let unknown = split("RUN SCRIPT '/tmp/x.sql';").unwrap();
        assert!(authorize_all_with(
            &policy,
            &MemoryLog::default(),
            &unknown,
            &resolved,
            "prod",
            bootstrap
        )
        .is_err());
        // An INSERT needs `produce` too.
        policy.rules.forbid = vec!["produce".into()];
        let insert = split("INSERT INTO kq_a (v) VALUES ('jane@example.com');").unwrap();
        let log = MemoryLog::default();
        let err =
            authorize_all_with(&policy, &log, &insert, &resolved, "prod", bootstrap).unwrap_err();
        assert!(err.contains("produce"), "{err}");
        // The audit entry never carries the inserted values.
        assert!(!format!("{:?}", log.entries.borrow()).contains("jane@example.com"));
    }

    #[test]
    fn a_read_only_profile_refuses_writes_but_not_reads() {
        use crate::governance::test_policy::{self, MemoryLog};
        let resolved = resolved_for(Some("prod"), true);
        let reads = split("SHOW STREAMS; DESCRIBE kq_a; SET 'a'='b';").unwrap();
        let log = MemoryLog::default();
        authorize_all_with(
            &test_policy::none(),
            &log,
            &reads,
            &resolved,
            "prod",
            "k:8088",
        )
        .unwrap();
        let writes = split("TERMINATE CSAS_X_1;").unwrap();
        let err = authorize_all_with(
            &test_policy::none(),
            &log,
            &writes,
            &resolved,
            "prod",
            "k:8088",
        )
        .unwrap_err();
        assert!(err.contains("read-only"), "{err}");
        let entries = log.entries.take();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].action, "ksql exec");
        assert!(!entries[0].ok);
    }

    #[test]
    fn query_properties() {
        let mut q = KsqlQueryArgs {
            sql: "SELECT 1 FROM t;".into(),
            limit: 10,
            timeout: None,
            from_beginning: true,
            properties: vec!["a.b = 1".into()],
            mask_pii: false,
        };
        let s = query_session(&q).unwrap();
        assert_eq!(s.properties["auto.offset.reset"], "earliest");
        assert_eq!(s.properties["a.b"], "1");
        q.properties = vec!["broken".into()];
        assert!(query_session(&q).is_err());
    }
}
