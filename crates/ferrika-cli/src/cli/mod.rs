pub mod acl;
pub mod app;
pub mod audit;
pub mod backup;
pub mod bench;
pub mod cluster_sync;
pub mod compact;
pub mod connect;
pub mod consume;
pub mod context;
pub mod crypto;
pub mod diff;
pub mod dlq;
pub mod generator;
pub mod health;
pub mod history;
pub mod lint;
pub mod mcp;
pub mod mirror;
pub mod monitor;
pub mod offset;
mod output;
pub mod policy;
pub mod probe;
pub mod produce;
pub mod profile;
pub mod quota;
pub mod reassign;
pub mod record;
pub mod report;
pub mod schema;
pub mod top;
pub mod topics;
pub mod topology;
pub mod trace;
pub mod user;

use clap::{Args, ValueEnum};
use colored::Colorize;

use crate::error::{KafkaError, KafkaResult};
use crate::kafka::config::KafkaClientConfig;
use crate::kafka::profile::ProfileStore;

pub use crate::kafka::context::ResolvedContext;

/// Output formats supported by Ferrika CLI.
#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum OutputFormat {
    #[default]
    Table,
    Json,
}

/// `--timeout` when neither it nor the profile sets one.
const DEFAULT_TIMEOUT_SECS: u64 = 30;

/// `--timeout` when given, else the profile's timeout (looked up only then),
/// else [`DEFAULT_TIMEOUT_SECS`].
fn effective_timeout(
    flag: Option<u64>,
    profile: impl FnOnce() -> Option<std::time::Duration>,
) -> std::time::Duration {
    match flag {
        Some(secs) => std::time::Duration::from_secs(secs),
        None => profile().unwrap_or(std::time::Duration::from_secs(DEFAULT_TIMEOUT_SECS)),
    }
}

/// Global CLI options shared across subcommands.
#[derive(Args, Debug, Clone)]
pub struct GlobalOpts {
    /// Select a cluster profile from configuration store
    #[arg(short = 'p', long, env = "FERRIKA_PROFILE")]
    pub profile: Option<String>,

    /// Kafka bootstrap servers (comma-separated list, overrides active profile)
    #[arg(short = 'b', long, env = "KAFKA_BOOTSTRAP_SERVERS")]
    pub bootstrap_servers: Option<String>,

    /// Output format (table or json)
    #[arg(short = 'f', long, value_enum, default_value_t = OutputFormat::Table)]
    pub format: OutputFormat,

    /// Request timeout in seconds (default: the profile's, else 30)
    #[arg(long)]
    pub timeout: Option<u64>,

    /// The profile and brokers `main` resolved for this run. Set for every
    /// command that talks to a cluster; `None` elsewhere (GUI, tests).
    #[arg(skip)]
    pub resolved: Option<std::sync::Arc<ResolvedContext>>,

    /// The change `main` authorized for this run (its audit label and
    /// target), for the operation history; `None` when nothing changes.
    #[arg(skip)]
    pub write: Option<WriteContext>,
}

/// A changing command as the audit log names it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteContext {
    /// e.g. `topics config`.
    pub label: String,
    /// e.g. `orders: retention.ms`.
    pub target: String,
    /// The history operation id written on the audit request entry, taken
    /// by the change's record (the first, for a command that records
    /// several); `None` for changes that are not recorded.
    pub op: Option<String>,
}

impl GlobalOpts {
    /// An empty `FERRIKA_PROFILE` or `KAFKA_BOOTSTRAP_SERVERS` (`export
    /// FERRIKA_PROFILE=` to unset it) means "not set", not a profile named
    /// "" or a cluster at "".
    pub fn ignore_empty_values(&mut self) {
        for value in [&mut self.profile, &mut self.bootstrap_servers] {
            if value.as_deref().is_some_and(|v| v.trim().is_empty()) {
                *value = None;
            }
        }
    }

    /// `--timeout` for HTTP requests (Schema Registry, Connect, webhooks).
    pub fn http_timeout(&self) -> std::time::Duration {
        self.request_timeout()
    }

    /// The request timeout: `--timeout`, else the profile's
    /// `request_timeout_ms`, else 30 s.
    pub fn request_timeout(&self) -> std::time::Duration {
        effective_timeout(self.timeout, || self.profile_request_timeout())
    }

    /// The `request_timeout_ms` of the profile in use (`-p`, else the
    /// active one), read without the OS credential store.
    fn profile_request_timeout(&self) -> Option<std::time::Duration> {
        let path = crate::kafka::profile::ProfileStore::default_path();
        if !path.exists() {
            return None;
        }
        let store = crate::kafka::profile::ProfileStore::load_from_path(&path).ok()?;
        let name = self
            .profile
            .clone()
            .or_else(|| store.active_profile.clone())?;
        let ms = store.profiles.get(&name)?.request_timeout_ms?;
        Some(std::time::Duration::from_millis(ms))
    }

    /// Whether to print JSON: a command's own `--json` flag or the global
    /// `--format json`.
    pub fn wants_json(&self, flag: bool) -> bool {
        flag || self.format == OutputFormat::Json
    }

    /// The context `main` resolved, or a lenient one built now for callers
    /// outside the CLI path (see [`ResolvedContext::lenient`]).
    pub fn context(&self) -> std::borrow::Cow<'_, ResolvedContext> {
        match &self.resolved {
            Some(ctx) => std::borrow::Cow::Borrowed(ctx),
            None => std::borrow::Cow::Owned(ResolvedContext::lenient(&self.selection())),
        }
    }

    /// The profile, brokers and timeout these options ask for.
    pub fn selection(&self) -> crate::kafka::context::ClusterSelection {
        crate::kafka::context::ClusterSelection {
            profile: self.profile.clone(),
            bootstrap_servers: self.bootstrap_servers.clone(),
            timeout: self.request_timeout(),
        }
    }

    /// Creates a KafkaClientConfig resolving cluster profile and command line overrides.
    pub fn to_kafka_config(&self) -> KafkaClientConfig {
        self.context().config.clone()
    }

    /// The profile store loaded for this run, for commands that look up
    /// other profiles by name.
    pub fn profile_store(&self) -> std::borrow::Cow<'_, ProfileStore> {
        match &self.resolved {
            Some(ctx) => std::borrow::Cow::Borrowed(&ctx.store),
            None => std::borrow::Cow::Owned(ProfileStore::load_or_default()),
        }
    }

    /// Fails when the resolved profile is read-only, naming `action`.
    pub fn ensure_writable(&self, action: &str) -> KafkaResult<()> {
        self.context().ensure_writable(action)
    }

    /// Returns the profile name `to_kafka_config` resolves, if any.
    pub fn resolved_profile_name(&self) -> Option<String> {
        self.context().profile_name.clone()
    }

    /// Returns the resolved bootstrap servers for hints or logging.
    pub fn resolved_bootstrap_servers(&self) -> String {
        self.context().bootstrap_servers.clone()
    }
}

/// Tells the user, on stderr (stdout may be JSON), that the record content
/// this command shows is masked because the company policy enforces it.
pub fn note_masking(policy: &crate::governance::policy::EffectivePolicy) {
    if policy.masking_enforced() {
        eprintln!(
            "{}",
            format!("PII masking is enforced by {}.", policy.label()).bright_black()
        );
    }
}

/// Added to a refused stdout export, so the user knows how to see the records.
const STDOUT_EXPORT_HINT: &str = "Records may only be shown on a terminal: run the command \
    without redirecting or piping its output. Older Git Bash (mintty) shows the console as a \
    pipe; run it in PowerShell or Windows Terminal instead.";

/// The export gate for a CLI command that takes record data out (see
/// [`crate::governance::policy::authorize_export`]), against the current policy.
pub fn authorize_record_export(
    request: &crate::governance::policy::ExportRequest<'_>,
    stdout_is_terminal: bool,
) -> Result<(), String> {
    authorize_record_export_with(
        crate::governance::policy::current(),
        &crate::governance::policy::AuditLog,
        request,
        stdout_is_terminal,
    )
}

/// [`authorize_record_export`] with a given policy and audit sink. Records
/// written to stdout are only exported when stdout is not a terminal (a
/// file or a pipe); on a terminal they are viewed, like the GUI's screen.
pub fn authorize_record_export_with(
    policy: &crate::governance::policy::EffectivePolicy,
    sink: &impl crate::governance::policy::AuditSink,
    request: &crate::governance::policy::ExportRequest<'_>,
    stdout_is_terminal: bool,
) -> Result<(), String> {
    use crate::governance::policy::{ExportChannel, WriteRefusal};
    let stdout = request.channel == ExportChannel::Stdout;
    if stdout && stdout_is_terminal {
        return Ok(());
    }
    crate::governance::policy::authorize_export_with(policy, sink, request).map_err(|refusal| {
        match refusal {
            WriteRefusal::Policy(reason) if stdout => format!("{reason}\n{STDOUT_EXPORT_HINT}"),
            other => other.to_string(),
        }
    })
}

/// The result line of the audited export this process runs, written by
/// [`export_finished`] once the count is known.
static AUDITED_EXPORT: std::sync::Mutex<Option<crate::governance::audit::AuditEntry>> =
    std::sync::Mutex::new(None);

/// Remembers an export that passed the gate, so that [`export_finished`]
/// can record how many records it wrote (only under `export: audited`).
pub fn remember_export(request: &crate::governance::policy::ExportRequest<'_>) {
    remember_export_with(crate::governance::policy::current(), request);
}

/// [`remember_export`] under a given policy.
pub fn remember_export_with(
    policy: &crate::governance::policy::EffectivePolicy,
    request: &crate::governance::policy::ExportRequest<'_>,
) {
    let entry = crate::governance::policy::export_result_entry(policy, request, 0);
    if let Ok(mut slot) = AUDITED_EXPORT.lock() {
        *slot = entry;
    }
}

/// Records the outcome `records: N` of the export remembered by
/// [`remember_export`], once; does nothing when it was not audited. The
/// request itself was recorded before the export ran.
pub fn export_finished(records: u64) {
    let entry = AUDITED_EXPORT.lock().ok().and_then(|mut slot| slot.take());
    if let Some(mut entry) = entry {
        entry.time_ms = crate::governance::audit::now_ms();
        entry.outcome = format!("records: {records}");
        if let Err(e) = crate::governance::audit::try_record(&entry) {
            tracing::warn!(%e, "Could not write the audit log");
        }
    }
}

/// Tells the user, on stderr, that the record export about to run is
/// recorded in the audit log (`export: audited`).
pub fn note_export(policy: &crate::governance::policy::EffectivePolicy, bootstrap: &str) {
    if policy.export_rule(bootstrap) == crate::model::policy::ExportRule::Audited {
        eprintln!(
            "{}",
            format!(
                "Record exports are recorded in the audit log by {}.",
                policy.label()
            )
            .bright_black()
        );
    }
}

/// Reads a password from the first line of `reader` (`--password-stdin`), so
/// it stays out of the process list and shell history. The line ending is
/// removed; an empty line is an error.
pub fn read_password_line(mut reader: impl std::io::BufRead) -> Result<String, String> {
    let mut line = String::new();
    reader
        .read_line(&mut line)
        .map_err(|e| format!("Cannot read the password from stdin: {e}"))?;
    let password = line.trim_end_matches(['\r', '\n']);
    if password.is_empty() {
        return Err("No password on stdin (expected one line)".to_string());
    }
    Ok(password.to_string())
}

/// [`read_password_line`] on the process's stdin.
pub fn read_password_stdin() -> Result<String, String> {
    read_password_line(std::io::stdin().lock())
}

/// Prints a friendly, helpful troubleshooting guide when Kafka operations fail.
/// Set once an error has been reported in full, so it is not printed again.
static ERROR_REPORTED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// The client configuration of the cluster command being run, for
/// [`hint_unreported`]; set by `main` before the command starts.
static HINT_CONFIG: std::sync::OnceLock<KafkaClientConfig> = std::sync::OnceLock::new();

/// Remembers the cluster the command talks to, so a failure its handler
/// returns as is still gets the troubleshooting hints.
pub fn set_hint_config(config: KafkaClientConfig) {
    let _ = HINT_CONFIG.set(config);
}

/// Shows the troubleshooting hints for a failed cluster command whose
/// handler returned the error as is, when the error came from the Kafka
/// client itself (see `fallback_hint_applies`).
pub fn hint_unreported(error: &KafkaError) {
    if ERROR_REPORTED.load(std::sync::atomic::Ordering::Relaxed) || !fallback_hint_applies(error) {
        return;
    }
    if let Some(config) = HINT_CONFIG.get() {
        print_connection_hint(config, error);
    }
}

/// Only librdkafka's own connection and authorization failures: `Io` and
/// `Config` also carry Schema Registry / Connect HTTP failures, local file
/// errors and usage errors, where broker hints would point the wrong way.
fn fallback_hint_applies(error: &KafkaError) -> bool {
    matches!(error, KafkaError::Driver(_))
        && matches!(
            hint_kind(error),
            HintKind::Connectivity | HintKind::Authorization
        )
}

/// The audit result entry still owed for the running write command, when
/// its handler does not write one itself (changes outside the operation
/// history: produce, generate, probe, ...). Its request entry was written
/// before the command ran; the result is written once, at the end.
static PENDING_RESULT: std::sync::Mutex<Option<crate::governance::audit::AuditEntry>> =
    std::sync::Mutex::new(None);

/// Owes the audit log the result of the write `request` describes.
pub fn expect_audit_result(request: crate::governance::audit::AuditEntry) {
    if let Ok(mut pending) = PENDING_RESULT.lock() {
        *pending = Some(request);
    }
}

/// Writes the owed result entry, if any. The outcome is a fixed word, not
/// the error message: these commands write records, and an encoding error
/// can quote a record's value, which the audit log must never hold.
pub fn record_audit_result(ok: bool, outcome: &str) {
    let owed = PENDING_RESULT.lock().ok().and_then(|mut p| p.take());
    if let Some(request) = owed {
        crate::governance::audit::record(&result_entry(request, ok, outcome));
    }
}

fn result_entry(
    request: crate::governance::audit::AuditEntry,
    ok: bool,
    outcome: &str,
) -> crate::governance::audit::AuditEntry {
    crate::governance::audit::AuditEntry {
        time_ms: crate::governance::audit::now_ms(),
        outcome: outcome.to_string(),
        ok,
        ..request
    }
}

/// What a quick TCP connect to a bootstrap server found.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Reach {
    /// An address accepted the connection.
    Open,
    /// Every address refused it: nothing listens there.
    Closed(String),
    /// No answer in time, the name did not resolve (DNS may recover), or
    /// the entry could not be read: the Kafka client decides, as before.
    Unclear,
}

fn connect(addr: std::net::SocketAddr, server: &str, timeout: std::time::Duration) -> Reach {
    match std::net::TcpStream::connect_timeout(&addr, timeout) {
        Ok(_) => Reach::Open,
        Err(e) if e.kind() == std::io::ErrorKind::ConnectionRefused => {
            Reach::Closed(format!("{server}: connection refused"))
        }
        Err(_) => Reach::Unclear,
    }
}

/// `host:port` of one bootstrap entry as librdkafka reads it: a
/// `scheme://` prefix (`SASL_SSL://`) is dropped and a missing port is
/// 9092. `None` when it cannot be told apart (a bare IPv6 address).
fn socket_target(entry: &str) -> Option<String> {
    let entry = entry.rsplit("://").next()?.trim().trim_end_matches('/');
    if entry.is_empty() {
        return None;
    }
    if let Some(rest) = entry.strip_prefix('[') {
        let (_, after) = rest.split_once(']')?;
        return match after {
            "" => Some(format!("{entry}:9092")),
            port if port.starts_with(':') => Some(entry.to_string()),
            _ => None,
        };
    }
    match entry.matches(':').count() {
        0 => Some(format!("{entry}:9092")),
        1 => Some(entry.to_string()),
        _ => None,
    }
}

/// Resolves `target` and connects to all its addresses at once; the first
/// open or unclear one decides, refused ones only when all are.
fn reach(target: &str, timeout: std::time::Duration) -> Reach {
    use std::net::ToSocketAddrs as _;
    let addrs: Vec<_> = match target.to_socket_addrs() {
        Ok(addrs) => addrs.collect(),
        Err(_) => return Reach::Unclear,
    };
    let (tx, rx) = std::sync::mpsc::channel();
    for &addr in &addrs {
        let (tx, target) = (tx.clone(), target.to_string());
        std::thread::spawn(move || {
            let _ = tx.send(connect(addr, &target, timeout));
        });
    }
    drop(tx);
    let mut refused = None;
    for _ in 0..addrs.len() {
        match rx.recv() {
            Ok(Reach::Closed(reason)) => refused = Some(reason),
            Ok(other) => return other,
            Err(_) => return Reach::Unclear,
        }
    }
    refused.map_or(Reach::Unclear, Reach::Closed)
}

/// Why nothing can be reached, when every server is certainly closed;
/// `None` once one is open or unclear.
fn closed_reason(results: &[Reach]) -> Option<String> {
    let mut reasons: Vec<&str> = Vec::new();
    for result in results {
        match result {
            Reach::Closed(reason) => {
                if !reasons.contains(&reason.as_str()) {
                    reasons.push(reason);
                }
            }
            Reach::Open | Reach::Unclear => return None,
        }
    }
    (!reasons.is_empty()).then(|| format!("Nothing accepts connections at {}", reasons.join("; ")))
}

/// Why no bootstrap server can be reached, when that is certain (every
/// address refused the connection), so a command can fail at once instead
/// of waiting `--timeout` (30 s) for a broker that is not running. Servers
/// and their addresses are tried in parallel, and the first open or
/// unclear one ends the wait: a refused IPv6 `localhost` (about 2 s on
/// Windows) must not slow down a broker that listens on IPv4. A slow name
/// lookup is not waited for past `timeout`.
pub fn unreachable_bootstrap(bootstrap: &str, timeout: std::time::Duration) -> Option<String> {
    let deadline = std::time::Instant::now() + timeout + std::time::Duration::from_millis(500);
    let (tx, rx) = std::sync::mpsc::channel();
    let mut servers = 0usize;
    for entry in bootstrap
        .split(|c: char| c == ',' || c.is_whitespace())
        .filter(|s| !s.is_empty())
    {
        let target = socket_target(entry)?;
        let tx = tx.clone();
        std::thread::spawn(move || {
            let _ = tx.send(reach(&target, timeout));
        });
        servers += 1;
    }
    drop(tx);
    let mut results = Vec::new();
    for _ in 0..servers {
        let left = deadline.saturating_duration_since(std::time::Instant::now());
        match rx.recv_timeout(left) {
            Ok(Reach::Closed(reason)) => results.push(Reach::Closed(reason)),
            // Open, unclear, too slow, or a check that died.
            _ => return None,
        }
    }
    closed_reason(&results)
}

/// Ends the process after a failed command. The error is printed unless
/// [`print_connection_hint`] already showed it.
pub fn exit_with_error(err: &dyn std::fmt::Display) -> ! {
    let message = err.to_string();
    // The reader of our stdout went away (`| head`): output simply ended.
    // EPIPE is os error 32; Windows reports ERROR_BROKEN_PIPE (109) or
    // ERROR_NO_DATA (232).
    if ["(os error 32)", "(os error 109)", "(os error 232)"]
        .iter()
        .any(|code| message.contains(code))
    {
        record_audit_result(true, "ended with a pipe error (output closed?)");
        std::process::exit(0);
    }
    record_audit_result(false, "failed (see the command's output)");
    if !ERROR_REPORTED.load(std::sync::atomic::Ordering::Relaxed) {
        eprintln!("{} {message}", "Error:".red().bold());
    }
    std::process::exit(1);
}

pub fn print_connection_hint(config: &crate::kafka::config::KafkaClientConfig, error: &KafkaError) {
    ERROR_REPORTED.store(true, std::sync::atomic::Ordering::Relaxed);
    eprintln!("{}", "─".repeat(64).bright_black());
    eprintln!(
        "{} {}",
        "✖".red().bold(),
        "Kafka Operation Failed:".red().bold()
    );
    eprintln!("  {}", error.to_string().bright_white());
    let hints = connection_hints(config, error);
    if !hints.is_empty() {
        eprintln!();
        eprintln!("{}", "💡 Troubleshooting Hints:".yellow().bold());
        for (ix, hint) in hints.iter().enumerate() {
            eprintln!("  {}. {hint}", ix + 1);
        }
    }
    eprintln!("{}", "─".repeat(64).bright_black());
}

/// Shown when a GSSAPI profile fails to connect or authenticate: librdkafka
/// reports a missing Kerberos ticket as a plain connection failure.
const KERBEROS_HINT: &str = "The profile uses GSSAPI (Kerberos): check for a valid ticket \
     ('klist', 'kinit') or a keytab, and the sasl.kerberos.service.name, \
     sasl.kerberos.principal and sasl.kerberos.keytab properties of the profile.";

/// The troubleshooting hints for `error` on a client configured as `config`.
fn connection_hints(
    config: &crate::kafka::config::KafkaClientConfig,
    error: &KafkaError,
) -> Vec<String> {
    let kerberos = config.sasl_mechanism == Some(crate::kafka::config::SaslMechanism::Gssapi);
    match hint_kind(error) {
        HintKind::Connectivity => {
            let mut hints = vec![
                format!(
                    "Verify Kafka broker is running and reachable at '{}'.",
                    config.bootstrap_servers.cyan()
                ),
                "Check firewall or network connectivity (e.g. ping, Test-NetConnection).".into(),
                "For Docker Kafka, verify advertised.listeners is accessible from host.".into(),
                "Provide custom broker address via '-b, --bootstrap-servers <HOST:PORT>'.".into(),
            ];
            if kerberos {
                hints.push(KERBEROS_HINT.into());
            }
            hints
        }
        HintKind::Authorization => vec![
            if kerberos {
                KERBEROS_HINT.into()
            } else {
                "Check the SASL username/password and mechanism of the active profile.".into()
            },
            "Ask an admin to grant the needed ACL (see 'ferrika acl list').".into(),
        ],
        HintKind::Configuration => {
            vec!["Check the active profile ('ferrika profile list') or the command options.".into()]
        }
        // The broker's message already says what to fix (e.g. an invalid config value).
        HintKind::None => Vec::new(),
    }
}

/// Which kind of advice fits an error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HintKind {
    Connectivity,
    Authorization,
    Configuration,
    None,
}

fn hint_kind(error: &KafkaError) -> HintKind {
    use crate::error::ErrorCode as Code;
    match error {
        KafkaError::Io(_) | KafkaError::Timeout(_) => HintKind::Connectivity,
        KafkaError::Acl(_) => HintKind::Authorization,
        KafkaError::Config(_) | KafkaError::Profile(_) => HintKind::Configuration,
        KafkaError::Driver(e) if e.is_client_config() => HintKind::Configuration,
        KafkaError::Driver(e) => match e.code() {
            Some(
                Code::TopicAuthorizationFailed
                | Code::GroupAuthorizationFailed
                | Code::ClusterAuthorizationFailed
                | Code::TransactionalIdAuthorizationFailed
                | Code::SaslAuthenticationFailed
                | Code::Authentication,
            ) => HintKind::Authorization,
            Some(
                Code::UnknownTopicOrPartition
                | Code::UnknownTopic
                | Code::UnknownPartition
                | Code::TopicAlreadyExists
                | Code::InvalidConfig
                | Code::InvalidPartitions
                | Code::InvalidReplicationFactor
                | Code::PolicyViolation
                | Code::MessageSizeTooLarge
                | Code::GroupIdNotFound
                | Code::NonEmptyGroup,
            ) => HintKind::None,
            _ => HintKind::Connectivity,
        },
        _ => HintKind::None,
    }
}

#[cfg(test)]
mod tests {
    use super::{connection_hints, hint_kind, read_password_line, HintKind, KERBEROS_HINT};

    #[test]
    fn the_profile_timeout_applies_unless_timeout_is_given() {
        use super::effective_timeout;
        use std::time::Duration;
        let profile = || Some(Duration::from_millis(5000));
        assert_eq!(effective_timeout(None, profile), Duration::from_secs(5));
        assert_eq!(effective_timeout(Some(2), profile), Duration::from_secs(2));
        assert_eq!(effective_timeout(None, || None), Duration::from_secs(30));
    }

    #[test]
    fn only_certain_failures_stop_a_command_early() {
        use super::{closed_reason, Reach};
        let closed = || Reach::Closed("h:1: connection refused".into());
        // The same server's addresses (IPv6 and IPv4) are named once.
        assert_eq!(
            closed_reason(&[closed(), closed()]).as_deref(),
            Some("Nothing accepts connections at h:1: connection refused")
        );
        // One open or unclear server: the client decides.
        assert_eq!(closed_reason(&[closed(), Reach::Open]), None);
        assert_eq!(closed_reason(&[closed(), Reach::Unclear]), None);
        assert_eq!(closed_reason(&[]), None);
    }

    #[test]
    fn bootstrap_entries_are_read_like_librdkafka() {
        use super::socket_target;
        assert_eq!(socket_target("kafka1").as_deref(), Some("kafka1:9092"));
        assert_eq!(socket_target("h:9093").as_deref(), Some("h:9093"));
        assert_eq!(
            socket_target("SASL_SSL://b1.prod.corp:9094").as_deref(),
            Some("b1.prod.corp:9094")
        );
        assert_eq!(socket_target("PLAINTEXT://h").as_deref(), Some("h:9092"));
        assert_eq!(socket_target("[::1]:9092").as_deref(), Some("[::1]:9092"));
        assert_eq!(socket_target("[::1]").as_deref(), Some("[::1]:9092"));
        // A bare IPv6 address cannot be split into host and port.
        assert_eq!(socket_target("::1"), None);
    }

    #[test]
    fn unreadable_or_unresolved_entries_never_stop_a_command() {
        use super::unreachable_bootstrap;
        use std::time::Duration;
        assert_eq!(unreachable_bootstrap("::1", Duration::from_secs(1)), None);
        assert_eq!(
            unreachable_bootstrap("no-such-host.invalid:9092", Duration::from_secs(1)),
            None
        );
    }

    #[test]
    fn a_listening_port_is_reachable_and_a_closed_one_is_not() {
        use super::unreachable_bootstrap;
        use std::time::Duration;
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let open = listener.local_addr().unwrap().to_string();
        assert_eq!(unreachable_bootstrap(&open, Duration::from_secs(3)), None);
        // A closed server next to an open one: reachable.
        let closed = {
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            l.local_addr().unwrap().to_string()
        };
        assert_eq!(
            unreachable_bootstrap(&format!("{closed},{open}"), Duration::from_secs(3)),
            None
        );
        drop(listener);
        // Nothing listens on either now (or the check is unclear).
        let reason = unreachable_bootstrap(&format!("{closed},{open}"), Duration::from_secs(3));
        if let Some(reason) = reason {
            assert!(reason.contains("connection refused"), "{reason}");
        }
    }

    #[test]
    fn audit_results_keep_the_request_and_set_the_outcome() {
        use crate::governance::audit::AuditEntry;
        let request = AuditEntry {
            time_ms: 1,
            source: "cli".into(),
            profile: "prod".into(),
            action: "produce".into(),
            target: "orders".into(),
            outcome: "requested".into(),
            ok: true,
            user: String::new(),
            actor: None,
            op: String::new(),
            op_digest: String::new(),
        };
        let result = super::result_entry(request, false, "failed (see the command's output)");
        assert_eq!(
            (result.action.as_str(), result.target.as_str()),
            ("produce", "orders")
        );
        assert_eq!(result.profile, "prod");
        assert!(!result.ok);
        assert_eq!(result.outcome, "failed (see the command's output)");
        assert!(result.time_ms > 1);
    }

    #[test]
    fn fallback_hints_only_for_kafka_client_failures() {
        use super::fallback_hint_applies;
        assert!(!fallback_hint_applies(&KafkaError::io("registry down")));
        assert!(!fallback_hint_applies(&KafkaError::config(
            "Nothing to undo"
        )));
        assert!(!fallback_hint_applies(&KafkaError::acl("denied")));
    }

    #[test]
    fn gssapi_profiles_get_a_kerberos_hint() {
        use crate::kafka::config::{KafkaClientConfig, SaslMechanism};
        let down = KafkaError::io("refused");
        let mut config = KafkaClientConfig::new("b:9092");
        assert!(!connection_hints(&config, &down)
            .iter()
            .any(|h| h == KERBEROS_HINT));
        config.sasl_mechanism = Some(SaslMechanism::Gssapi);
        assert!(connection_hints(&config, &down)
            .iter()
            .any(|h| h == KERBEROS_HINT));
        let denied = KafkaError::acl("denied");
        assert_eq!(connection_hints(&config, &denied)[0], KERBEROS_HINT);
        // One sentence: a lost line continuation once left runs of spaces in it.
        assert!(!KERBEROS_HINT.contains("  "), "{KERBEROS_HINT}");
    }

    #[test]
    fn record_stdout_is_an_export_only_when_redirected() {
        use crate::governance::policy::{ExportChannel, ExportRequest};
        use crate::governance::test_policy::{self, MemoryLog};
        let request = |channel| ExportRequest {
            source: "cli",
            channel,
            label: "consume",
            target: "orders",
            records: None,
            profile: "dev",
            bootstrap: "localhost:9092",
            actor: None,
        };
        let forbidden = test_policy::export_forbidden();
        let stdout = request(ExportChannel::Stdout);

        // A terminal only shows the records: allowed, nothing recorded.
        let log = MemoryLog::default();
        assert_eq!(
            super::authorize_record_export_with(&forbidden, &log, &stdout, true),
            Ok(())
        );
        assert!(log.entries.borrow().is_empty());

        // A pipe or a file: refused with a way out, and recorded.
        let reason =
            super::authorize_record_export_with(&forbidden, &log, &stdout, false).unwrap_err();
        assert!(reason.contains("export: forbidden"), "{reason}");
        assert!(
            reason.contains("PowerShell or Windows Terminal"),
            "{reason}"
        );
        assert_eq!(log.entries.borrow()[0].action, "policy.blocked");
        assert_eq!(
            log.entries.borrow()[0].target,
            "export:stdout consume orders"
        );

        // A file is refused whatever stdout is, without the terminal hint.
        let file = request(ExportChannel::File);
        let reason =
            super::authorize_record_export_with(&forbidden, &log, &file, true).unwrap_err();
        assert!(!reason.contains("PowerShell"), "{reason}");

        // No policy: allowed everywhere; audited: recorded when redirected.
        let none = test_policy::none();
        assert_eq!(
            super::authorize_record_export_with(&none, &log, &stdout, false),
            Ok(())
        );
        let audited = test_policy::export_audited();
        let log = MemoryLog::default();
        assert_eq!(
            super::authorize_record_export_with(&audited, &log, &stdout, true),
            Ok(())
        );
        assert!(log.entries.borrow().is_empty());
        assert_eq!(
            super::authorize_record_export_with(&audited, &log, &stdout, false),
            Ok(())
        );
        assert_eq!(log.entries.borrow()[0].action, "record.export");
    }

    #[test]
    fn password_line_strips_line_ending() {
        assert_eq!(
            read_password_line(&b"s3cr et\r\nnext\n"[..]).unwrap(),
            "s3cr et"
        );
        assert_eq!(read_password_line(&b"pw"[..]).unwrap(), "pw");
        assert!(read_password_line(&b"\n"[..]).is_err());
        assert!(read_password_line(&b""[..]).is_err());
    }
    use crate::error::KafkaError;
    use rdkafka::error::KafkaError as Driver;

    #[test]
    fn test_local_failures_get_no_broker_hints() {
        assert_eq!(
            hint_kind(&KafkaError::File("disk full".into())),
            HintKind::None
        );
        assert_eq!(
            hint_kind(&KafkaError::Check("p99 too high".into())),
            HintKind::None
        );
        assert_eq!(
            KafkaError::Check("p99 too high".into()).to_string(),
            "p99 too high"
        );
    }
    use rdkafka::types::RDKafkaErrorCode as Code;

    #[test]
    fn hints_match_the_error_kind() {
        assert_eq!(
            hint_kind(&KafkaError::io("refused")),
            HintKind::Connectivity
        );
        assert_eq!(
            hint_kind(&KafkaError::acl("denied")),
            HintKind::Authorization
        );
        assert_eq!(
            hint_kind(&KafkaError::config("bad")),
            HintKind::Configuration
        );
        assert_eq!(
            hint_kind(&KafkaError::Admin("INVALID_CONFIG (code 40)".into())),
            HintKind::None
        );
        let driver = |code| KafkaError::from(Driver::AdminOp(code));
        assert_eq!(
            hint_kind(&driver(Code::AllBrokersDown)),
            HintKind::Connectivity
        );
        assert_eq!(
            hint_kind(&driver(Code::TopicAuthorizationFailed)),
            HintKind::Authorization
        );
        assert_eq!(hint_kind(&driver(Code::TopicAlreadyExists)), HintKind::None);
    }
}
