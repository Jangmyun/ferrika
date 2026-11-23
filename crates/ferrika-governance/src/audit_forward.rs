//! Forwarding audit entries to a SIEM, after they are appended to the local
//! hash-chained log (see [`crate::audit`]).
//!
//! Targets: syslog RFC 5424 over UDP (RFC 5426), TCP with octet counting
//! (RFC 6587) or TLS (RFC 5425), and a spool folder a log collector watches
//! (one JSON line per entry). The syslog MSG and the spool line are the exact
//! chained JSON line, `prev` and `hash` included, so the SIEM side can check
//! the chain and notice a log that was cut short on the PC.
//!
//! Configuration comes from the signed policy (`rules.audit_forward`) or,
//! when the policy sets none, from `FERRIKA_AUDIT_FORWARD` for individual use.
//! Every send runs on a worker thread and the caller waits at most
//! `timeout_ms`, so neither the CLI nor the GUI can hang on a dead collector.

use std::io::Write;
use std::net::{SocketAddr, TcpStream, ToSocketAddrs, UdpSocket};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use serde::Serialize;

use crate::policy::{EffectivePolicy, PolicyStatus};
use ferrika_core::model::policy::{AuditForward, SyslogEndpoint, SyslogFacility, SyslogTransport};

/// Environment variable for forwarding without a policy:
/// `udp://host:514`, `tcp://host:514`, `tls://host:6514`, `spool:<dir>`, or
/// the JSON of a policy `audit_forward` section.
pub const ENV_VAR: &str = "FERRIKA_AUDIT_FORWARD";

/// RFC 5424 MSGID of every entry.
pub(crate) const MSGID: &str = "audit";

/// Severity of a successful operation (notice) and of a failed or refused
/// one (warning).
const SEVERITY_NOTICE: u8 = 5;
const SEVERITY_WARNING: u8 = 4;

/// Largest UDP syslog message sent; longer entries need TCP.
const MAX_UDP_MESSAGE: usize = 65_000;

/// Where the forwarding settings came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ForwardOrigin {
    /// The verified company policy.
    Policy,
    /// `FERRIKA_AUDIT_FORWARD`.
    Env,
    /// `ferrika audit forward-test --to`.
    CommandLine,
}

impl ForwardOrigin {
    pub fn label(self) -> &'static str {
        match self {
            Self::Policy => "company policy (rules.audit_forward)",
            Self::Env => ENV_VAR,
            Self::CommandLine => "--to",
        }
    }
}

/// Validated forwarding settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForwardSettings {
    pub config: AuditForward,
    pub syslog: Option<SyslogEndpoint>,
    pub origin: ForwardOrigin,
}

impl ForwardSettings {
    pub fn new(config: AuditForward, origin: ForwardOrigin) -> Result<Self, String> {
        config.validate()?;
        let syslog = config
            .syslog
            .as_deref()
            .map(SyslogEndpoint::parse)
            .transpose()?;
        Ok(Self {
            config,
            syslog,
            origin,
        })
    }

    pub fn timeout(&self) -> Duration {
        Duration::from_millis(self.config.timeout_ms)
    }

    /// Whether a failed forward refuses the action: only when a signed
    /// policy both requires auditing and marks forwarding `required`.
    pub fn blocks_actions(&self, audit_required: bool) -> bool {
        self.origin == ForwardOrigin::Policy && self.config.required && audit_required
    }

    /// The targets as text, e.g. `tcp://siem:514, spool:C:\spool`.
    pub fn describe(&self) -> String {
        self.targets()
            .iter()
            .map(Target::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn targets(&self) -> Vec<Target> {
        let mut targets = Vec::new();
        if let Some(endpoint) = &self.syslog {
            targets.push(Target::Syslog(endpoint.clone()));
        }
        if let Some(dir) = &self.config.spool_dir {
            targets.push(Target::Spool(PathBuf::from(dir)));
        }
        targets
    }
}

#[derive(Debug, Clone)]
enum Target {
    Syslog(SyslogEndpoint),
    Spool(PathBuf),
}

impl std::fmt::Display for Target {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Syslog(endpoint) => write!(f, "{endpoint}"),
            Self::Spool(dir) => write!(f, "spool:{}", dir.display()),
        }
    }
}

/// Parses `FERRIKA_AUDIT_FORWARD` (or `forward-test --to`). `required` is
/// always off: only a signed policy can make forwarding block an action.
pub fn parse_local(value: &str) -> Result<AuditForward, String> {
    let value = value.trim();
    let mut forward = if value.starts_with('{') {
        serde_json::from_str::<AuditForward>(value).map_err(|e| format!("{ENV_VAR}: {e}"))?
    } else if let Some(dir) = value.strip_prefix("spool:") {
        AuditForward::new(None, Some(dir.trim().to_string()))
    } else {
        AuditForward::new(Some(value.to_string()), None)
    };
    forward.required = false;
    forward.validate()?;
    Ok(forward)
}

/// The settings in force: a verified policy's `audit_forward` wins over the
/// environment variable, which is used only when the policy sets none.
pub fn resolve(
    policy: &EffectivePolicy,
    env: Option<&str>,
) -> Result<Option<ForwardSettings>, String> {
    if let (PolicyStatus::Verified { .. }, Some(forward)) =
        (&policy.status, &policy.rules.audit_forward)
    {
        return ForwardSettings::new(forward.clone(), ForwardOrigin::Policy).map(Some);
    }
    match env.map(str::trim).filter(|v| !v.is_empty()) {
        None => Ok(None),
        Some(value) => ForwardSettings::new(parse_local(value)?, ForwardOrigin::Env).map(Some),
    }
}

static SETTINGS: OnceLock<Result<Option<ForwardSettings>, String>> = OnceLock::new();

/// [`resolve`] for this process, read once (like the policy).
pub fn settings_status() -> &'static Result<Option<ForwardSettings>, String> {
    SETTINGS.get_or_init(|| {
        let env = std::env::var(ENV_VAR).ok();
        let resolved = resolve(crate::policy::current(), env.as_deref());
        if let Err(e) = &resolved {
            tracing::warn!(%e, "Audit forwarding is not configured correctly; not forwarding");
        }
        resolved
    })
}

/// The forwarding settings in force, if any.
pub fn settings() -> Option<&'static ForwardSettings> {
    settings_status().as_ref().ok().and_then(Option::as_ref)
}

static FAILURES: AtomicU64 = AtomicU64::new(0);

/// Failed sends (one per target) in this process so far.
#[cfg(test)]
fn failure_count() -> u64 {
    FAILURES.load(Ordering::Relaxed)
}

/// Result of sending one entry to one target.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ForwardReport {
    pub target: String,
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub elapsed_ms: u64,
}

/// Sends one chained audit `line` to every target, in parallel, waiting at
/// most the configured timeout. `ok` and `time_ms` come from the entry and
/// set the syslog severity and timestamp. Failures are counted.
pub fn send(settings: &ForwardSettings, line: &str, ok: bool, time_ms: i64) -> Vec<ForwardReport> {
    let timeout = settings.timeout();
    let started = Instant::now();
    let deadline = started + timeout;
    let severity = if ok {
        SEVERITY_NOTICE
    } else {
        SEVERITY_WARNING
    };
    let pending: Vec<(String, Result<JobResult, String>)> = settings
        .targets()
        .into_iter()
        .map(|target| {
            let name = target.to_string();
            let job = job_for(&target, settings, line, severity, time_ms, timeout);
            (name, spawn_job(job))
        })
        .collect();
    let reports: Vec<ForwardReport> = pending
        .into_iter()
        .map(|(target, rx)| {
            let result = rx.and_then(|rx| wait(&rx, deadline, timeout));
            ForwardReport {
                target,
                ok: result.is_ok(),
                error: result.err(),
                elapsed_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
            }
        })
        .collect();
    let failed = reports.iter().filter(|r| !r.ok).count() as u64;
    FAILURES.fetch_add(failed, Ordering::Relaxed);
    reports
}

type Job = Box<dyn FnOnce() -> std::io::Result<()> + Send + 'static>;
/// Where a running [`Job`] delivers its result.
type JobResult = mpsc::Receiver<std::io::Result<()>>;

fn job_for(
    target: &Target,
    settings: &ForwardSettings,
    line: &str,
    severity: u8,
    time_ms: i64,
    timeout: Duration,
) -> Job {
    match target {
        Target::Syslog(endpoint) => {
            let message = rfc5424(
                settings.config.facility,
                severity,
                time_ms,
                hostname(),
                &settings.config.app_name,
                std::process::id(),
                line,
            );
            let endpoint = endpoint.clone();
            let ca_file = settings.config.ca_file.clone();
            Box::new(move || match endpoint.transport {
                SyslogTransport::Udp => send_udp(&endpoint, &message, timeout),
                SyslogTransport::Tcp => send_stream(&endpoint, &message, timeout, None),
                SyslogTransport::Tls => {
                    send_stream(&endpoint, &message, timeout, Some(ca_file.as_deref()))
                }
            })
        }
        Target::Spool(dir) => {
            let dir = dir.clone();
            let line = line.to_string();
            Box::new(move || append_spool(&dir, &line))
        }
    }
}

fn spawn_job(job: Job) -> Result<JobResult, String> {
    let (tx, rx) = mpsc::channel();
    std::thread::Builder::new()
        .name("audit-forward".into())
        .spawn(move || {
            // The receiver is gone when the caller stopped waiting.
            let _ = tx.send(job());
        })
        .map_err(|e| format!("could not start the forwarding thread: {e}"))?;
    Ok(rx)
}

/// Waits for a worker until `deadline`. A worker still running then is left
/// to finish on its own (its sockets have the same timeout).
fn wait(rx: &JobResult, deadline: Instant, timeout: Duration) -> Result<(), String> {
    let left = deadline.saturating_duration_since(Instant::now());
    match rx.recv_timeout(left) {
        Ok(result) => result.map_err(|e| e.to_string()),
        Err(mpsc::RecvTimeoutError::Timeout) => {
            Err(format!("timed out after {} ms", timeout.as_millis()))
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            Err("the forwarding thread stopped".to_string())
        }
    }
}

/// One RFC 5424 message:
/// `<PRI>1 TIMESTAMP HOSTNAME APP-NAME PROCID MSGID STRUCTURED-DATA MSG`,
/// with no structured data (`-`) and the JSON line as MSG (no BOM, so JSON
/// parsers on the collector side read it as is).
pub(crate) fn rfc5424(
    facility: SyslogFacility,
    severity: u8,
    time_ms: i64,
    hostname: &str,
    app_name: &str,
    procid: u32,
    msg: &str,
) -> String {
    let pri = u16::from(facility.code()) * 8 + u16::from(severity.min(7));
    format!(
        "<{pri}>1 {} {} {} {procid} {MSGID} - {msg}",
        timestamp(time_ms),
        header_field(hostname, 255),
        header_field(app_name, 48),
    )
}

/// RFC 3339 UTC with milliseconds, e.g. `2026-11-02T09:30:00.123Z`.
fn timestamp(time_ms: i64) -> String {
    jiff::Timestamp::from_millisecond(time_ms)
        .ok()
        .map(|t| t.strftime("%Y-%m-%dT%H:%M:%S%.3fZ").to_string())
        .unwrap_or_else(|| "-".to_string())
}

/// A header field: printable ASCII without spaces, at most `max` bytes, or
/// `-` (the RFC 5424 NILVALUE) when nothing is left.
fn header_field(value: &str, max: usize) -> String {
    let clean: String = value
        .chars()
        .filter(|c| c.is_ascii_graphic())
        .take(max)
        .collect();
    if clean.is_empty() {
        "-".to_string()
    } else {
        clean
    }
}

/// RFC 6587 octet counting: `MSG-LEN SP SYSLOG-MSG`, the length in bytes.
pub(crate) fn octet_frame(message: &str) -> Vec<u8> {
    let mut frame = format!("{} ", message.len()).into_bytes();
    frame.extend_from_slice(message.as_bytes());
    frame
}

fn resolve_addrs(endpoint: &SyslogEndpoint) -> std::io::Result<Vec<SocketAddr>> {
    let addrs: Vec<SocketAddr> = (endpoint.host.as_str(), endpoint.port)
        .to_socket_addrs()?
        .collect();
    if addrs.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("{} has no address", endpoint.host),
        ));
    }
    Ok(addrs)
}

fn send_udp(endpoint: &SyslogEndpoint, message: &str, timeout: Duration) -> std::io::Result<()> {
    if message.len() > MAX_UDP_MESSAGE {
        return Err(std::io::Error::other(format!(
            "entry is {} bytes, too long for UDP syslog; use tcp://",
            message.len()
        )));
    }
    let addr = resolve_addrs(endpoint)?[0];
    let local: SocketAddr = if addr.is_ipv4() {
        ([0, 0, 0, 0], 0).into()
    } else {
        ([0u16; 8], 0).into()
    };
    let socket = UdpSocket::bind(local)?;
    socket.set_write_timeout(Some(timeout))?;
    let sent = socket.send_to(message.as_bytes(), addr)?;
    if sent != message.len() {
        return Err(std::io::Error::other("datagram was cut short"));
    }
    Ok(())
}

/// TCP (or TLS when `tls` is `Some(ca_file)`) with octet-counting framing,
/// one connection per entry.
fn send_stream(
    endpoint: &SyslogEndpoint,
    message: &str,
    timeout: Duration,
    tls: Option<Option<&str>>,
) -> std::io::Result<()> {
    let mut last_error = None;
    let mut stream = None;
    for addr in resolve_addrs(endpoint)? {
        match TcpStream::connect_timeout(&addr, timeout) {
            Ok(s) => {
                stream = Some(s);
                break;
            }
            Err(e) => last_error = Some(e),
        }
    }
    let Some(tcp) = stream else {
        return Err(last_error.unwrap_or_else(|| std::io::Error::other("no address to connect")));
    };
    tcp.set_write_timeout(Some(timeout))?;
    tcp.set_read_timeout(Some(timeout))?;
    tcp.set_nodelay(true)?;
    let frame = octet_frame(message);
    match tls {
        None => {
            let mut tcp = tcp;
            tcp.write_all(&frame)?;
            tcp.flush()?;
            let _ = tcp.shutdown(std::net::Shutdown::Write);
        }
        Some(ca_file) => {
            let mut stream = tls_connect(endpoint, tcp, ca_file)?;
            stream.write_all(&frame)?;
            stream.flush()?;
            stream.conn.send_close_notify();
            let _ = stream.conn.complete_io(&mut stream.sock);
        }
    }
    Ok(())
}

fn tls_connect(
    endpoint: &SyslogEndpoint,
    tcp: TcpStream,
    ca_file: Option<&str>,
) -> std::io::Result<rustls::StreamOwned<rustls::ClientConnection, TcpStream>> {
    let client = ferrika_core::kafka::config::KafkaClientConfig {
        ssl_ca_location: ca_file.map(str::to_string),
        ..Default::default()
    };
    let config = ferrika_core::kafka::http::broker_tls_config(&client)
        .map_err(|e| std::io::Error::other(e.to_string()))?;
    let name = rustls::pki_types::ServerName::try_from(endpoint.host.clone())
        .map_err(|e| std::io::Error::other(format!("invalid TLS host: {e}")))?;
    let conn = rustls::ClientConnection::new(config, name)
        .map_err(|e| std::io::Error::other(format!("TLS setup failed: {e}")))?;
    let mut stream = rustls::StreamOwned::new(conn, tcp);
    while stream.conn.is_handshaking() {
        stream
            .conn
            .complete_io(&mut stream.sock)
            .map_err(|e| std::io::Error::new(e.kind(), format!("TLS handshake failed: {e}")))?;
    }
    Ok(stream)
}

/// The start of this account's spool file names,
/// `ferrika-audit-<host>-<user>.`; a random suffix and `.jsonl` follow.
fn spool_stem() -> String {
    let safe = |s: &str| -> String {
        let s: String = s
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' {
                    c
                } else {
                    '_'
                }
            })
            .collect();
        if s.is_empty() {
            "unknown".to_string()
        } else {
            s
        }
    };
    format!(
        "ferrika-audit-{}-{}.",
        safe(hostname()),
        safe(&crate::audit::os_user())
    )
}

/// Hex digits of the random part of a spool file name.
const SPOOL_SUFFIX_LEN: usize = 16;

/// Whether `name` is one of this account's spool files: `stem`, 16 lowercase
/// hex digits, `.jsonl`.
fn is_spool_name(name: &str, stem: &str) -> bool {
    name.strip_prefix(stem)
        .and_then(|rest| rest.strip_suffix(".jsonl"))
        .is_some_and(|random| {
            random.len() == SPOOL_SUFFIX_LEN
                && random
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        })
}

/// This account's spool files in `dir`, newest first.
fn spool_candidates(dir: &Path) -> Vec<PathBuf> {
    let stem = spool_stem();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut found: Vec<(Option<std::time::SystemTime>, PathBuf)> = entries
        .filter_map(Result::ok)
        .filter(|entry| is_spool_name(&entry.file_name().to_string_lossy(), &stem))
        .map(|entry| {
            // Not following links: a link's own time is enough to order it.
            let modified = std::fs::symlink_metadata(entry.path())
                .and_then(|m| m.modified())
                .ok();
            (modified, entry.path())
        })
        .collect();
    found.sort_by(|a, b| b.cmp(a));
    found.into_iter().map(|(_, path)| path).collect()
}

/// The spool file in `dir` that this process would append to now: the newest
/// of this account's files (`ferrika-audit-<host>-<user>.<random>.jsonl`)
/// that passes the ownership checks. `None` before the first entry is
/// spooled (a new file is then created).
pub fn spool_file(dir: &Path) -> Option<PathBuf> {
    spool_candidates(dir)
        .into_iter()
        .find(|path| open_existing_spool(path).is_ok())
}

/// Opens an existing spool file for appending, refusing a symbolic link (or
/// any non-regular file) and a file other accounts may change: on Unix one
/// another account owns, one that group or others may write, or one with
/// more than one hard link; on Windows one owned by another account or whose
/// DACL lets another account write, append, delete or change permissions
/// (see [`crate::file_security::check_private_file`]).
fn open_existing_spool(path: &Path) -> std::io::Result<std::fs::File> {
    let meta = std::fs::symlink_metadata(path)?;
    if meta.file_type().is_symlink() {
        return Err(std::io::Error::other("is a symbolic link"));
    }
    if !meta.is_file() {
        return Err(std::io::Error::other("is not a regular file"));
    }
    let mut options = std::fs::OpenOptions::new();
    options.append(true);
    no_follow(&mut options);
    let file = options.open(path)?;
    crate::file_security::check_private_file(&file)?;
    Ok(file)
}

#[cfg(unix)]
fn no_follow(options: &mut std::fs::OpenOptions) {
    use std::os::unix::fs::OpenOptionsExt;
    options.custom_flags(libc::O_NOFOLLOW);
}

#[cfg(windows)]
fn no_follow(options: &mut std::fs::OpenOptions) {
    use std::os::windows::fs::OpenOptionsExt;
    // Opens a link itself instead of its target; the check below then sees
    // a reparse point and refuses it.
    options.custom_flags(windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OPEN_REPARSE_POINT);
}

#[cfg(not(any(unix, windows)))]
fn no_follow(_options: &mut std::fs::OpenOptions) {}

/// Creates a new spool file with a random name (create-new, so an existing
/// file or link is never opened), owner read/write only on Unix. On Windows
/// it gets the folder's inherited ACL, which the folder check vetted.
fn create_spool_file(dir: &Path) -> std::io::Result<(PathBuf, std::fs::File)> {
    let stem = spool_stem();
    let mut last = None;
    for _ in 0..4 {
        let path = dir.join(format!("{stem}{}.jsonl", random_hex(SPOOL_SUFFIX_LEN / 2)?));
        let mut options = std::fs::OpenOptions::new();
        options.append(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        match options.open(&path) {
            Ok(file) => return Ok((path, file)),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => last = Some(e),
            Err(e) => return Err(e),
        }
    }
    Err(last.unwrap_or_else(|| std::io::Error::other("could not create a spool file")))
}

/// `bytes` random bytes from the OS generator, as lowercase hex.
fn random_hex(bytes: usize) -> std::io::Result<String> {
    use ring::rand::SecureRandom;
    let mut buf = vec![0u8; bytes];
    ring::rand::SystemRandom::new()
        .fill(&mut buf)
        .map_err(|_| std::io::Error::other("the OS random generator failed"))?;
    Ok(buf.iter().map(|b| format!("{b:02x}")).collect())
}

/// Creates the spool folder for this account only (mode 0700 on Unix, a
/// protected owner-only DACL on Windows) or, when it exists, checks that no
/// other account can change it; see [`crate::file_security::check_private_dir`].
fn prepare_spool_dir(dir: &Path) -> std::io::Result<()> {
    if !dir.exists() {
        return crate::file_security::create_private_dir(dir, SPOOL_FOLDER);
    }
    crate::file_security::check_private_dir(dir, SPOOL_FOLDER)
}

/// How errors name the spool folder.
const SPOOL_FOLDER: &str = "spool folder";

/// Appends `line` and a newline in one write (append mode, so concurrent
/// writers never overwrite each other) to this account's newest spool file
/// that passes the checks, or to a new one. A file that fails them is left
/// alone and reported.
fn append_spool(dir: &Path, line: &str) -> std::io::Result<()> {
    prepare_spool_dir(dir)?;
    let mut chosen = None;
    for path in spool_candidates(dir) {
        match open_existing_spool(&path) {
            Ok(file) => {
                chosen = Some(file);
                break;
            }
            Err(e) => ferrika_core::notice::warn(&format!(
                "audit forwarding: skipping spool file {} ({e}); writing to another file",
                path.display()
            )),
        }
    }
    let mut file = match chosen {
        Some(file) => file,
        None => create_spool_file(dir)?.1,
    };
    let mut out = String::with_capacity(line.len() + 1);
    out.push_str(line);
    out.push('\n');
    file.write_all(out.as_bytes())?;
    file.flush()
}

/// This machine's name for the syslog HOSTNAME field.
pub fn hostname() -> &'static str {
    static NAME: OnceLock<String> = OnceLock::new();
    NAME.get_or_init(|| {
        os_hostname()
            .filter(|n| !n.is_empty())
            .or_else(|| {
                ["COMPUTERNAME", "HOSTNAME"]
                    .iter()
                    .find_map(|k| std::env::var(k).ok().filter(|v| !v.is_empty()))
            })
            .unwrap_or_else(|| "-".to_string())
    })
}

#[cfg(windows)]
fn os_hostname() -> Option<String> {
    use windows_sys::Win32::System::WindowsProgramming::GetComputerNameW;
    // MAX_COMPUTERNAME_LENGTH (15) is the NetBIOS limit; leave room anyway.
    let mut buf = [0u16; 256];
    let mut len = buf.len() as u32;
    // SAFETY: `buf` holds `len` UTF-16 units; GetComputerNameW writes at most
    // that many and stores the count it wrote (without the NUL) in `len`.
    let ok = unsafe { GetComputerNameW(buf.as_mut_ptr(), &mut len) };
    if ok == 0 {
        return None;
    }
    String::from_utf16(&buf[..(len as usize).min(buf.len())]).ok()
}

#[cfg(unix)]
fn os_hostname() -> Option<String> {
    let mut buf = [0u8; 256];
    // SAFETY: `buf` is valid for `buf.len()` bytes; gethostname writes a
    // (possibly unterminated when truncated) name into it.
    let rc = unsafe { libc::gethostname(buf.as_mut_ptr().cast(), buf.len()) };
    if rc != 0 {
        return None;
    }
    let end = buf.iter().position(|b| *b == 0).unwrap_or(buf.len());
    String::from_utf8(buf[..end].to_vec()).ok()
}

#[cfg(not(any(windows, unix)))]
fn os_hostname() -> Option<String> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    use std::net::TcpListener;

    use ferrika_core::model::policy::PolicyRules;

    const LINE: &str = r#"{"time_ms":1793611800123,"source":"cli","profile":"prod","action":"topics delete","target":"주문","outcome":"requested","ok":true,"user":"tester","prev":"00","hash":"ab"}"#;

    fn settings(config: AuditForward) -> ForwardSettings {
        ForwardSettings::new(config, ForwardOrigin::Policy).unwrap()
    }

    fn syslog(url: &str) -> AuditForward {
        AuditForward::new(Some(url.to_string()), None)
    }

    fn temp_dir(name: &str) -> PathBuf {
        crate::file_security::test_dir(&format!(
            "ferrika-forward-{name}-{}-{}",
            std::process::id(),
            ferrika_core::kafka::clock::now_ms()
        ))
    }

    /// Splits an RFC 5424 message into its seven header fields and MSG.
    fn fields(message: &str) -> Vec<&str> {
        message.splitn(8, ' ').collect()
    }

    #[test]
    fn rfc5424_header_fields() {
        let message = rfc5424(
            SyslogFacility::Auth,
            SEVERITY_NOTICE,
            1_793_611_800_123,
            "pc-01",
            "ferrika",
            4242,
            LINE,
        );
        let f = fields(&message);
        assert_eq!(f[0], "<37>1"); // auth (4) * 8 + notice (5)
        assert_eq!(f[1], "2026-11-02T09:30:00.123Z");
        assert_eq!(f[2], "pc-01");
        assert_eq!(f[3], "ferrika");
        assert_eq!(f[4], "4242");
        assert_eq!(f[5], "audit");
        assert_eq!(f[6], "-");
        assert_eq!(f[7], LINE);

        // local7 warning, and header fields cleaned of spaces / non-ASCII.
        let message = rfc5424(
            SyslogFacility::Local7,
            SEVERITY_WARNING,
            0,
            "my pc",
            "",
            1,
            "x",
        );
        let f = fields(&message);
        assert_eq!(f[0], "<188>1");
        assert_eq!(f[1], "1970-01-01T00:00:00.000Z");
        assert_eq!(f[2], "mypc");
        assert_eq!(f[3], "-");
    }

    #[test]
    fn octet_counting_counts_bytes() {
        assert_eq!(octet_frame("<37>1 abc"), b"9 <37>1 abc".to_vec());
        // "주" is three bytes in UTF-8.
        assert_eq!(octet_frame("주"), "3 주".as_bytes().to_vec());
    }

    #[test]
    fn udp_sends_one_rfc5424_datagram() {
        let server = UdpSocket::bind("127.0.0.1:0").unwrap();
        server
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let port = server.local_addr().unwrap().port();
        let mut config = syslog(&format!("udp://127.0.0.1:{port}"));
        config.facility = SyslogFacility::Local0;
        config.app_name = "ferrika-test".into();
        let reports = send(&settings(config), LINE, false, 1_793_611_800_123);
        assert!(reports.iter().all(|r| r.ok), "{reports:?}");

        let mut buf = [0u8; 4096];
        let (n, _) = server.recv_from(&mut buf).unwrap();
        let message = std::str::from_utf8(&buf[..n]).unwrap();
        let f = fields(message);
        assert_eq!(f[0], "<132>1"); // local0 (16) * 8 + warning (4)
        assert_eq!(f[1], "2026-11-02T09:30:00.123Z");
        assert_eq!(f[2], header_field(hostname(), 255));
        assert_eq!(f[3], "ferrika-test");
        assert_eq!(f[4], std::process::id().to_string());
        assert_eq!(f[5], "audit");
        assert_eq!(f[6], "-");
        assert_eq!(
            f[7], LINE,
            "MSG is the chained JSON line, prev/hash included"
        );
    }

    #[test]
    fn tcp_uses_octet_counting_framing() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = std::thread::spawn(move || {
            let mut frames = Vec::new();
            for _ in 0..2 {
                let (mut conn, _) = listener.accept().unwrap();
                conn.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
                let mut data = Vec::new();
                conn.read_to_end(&mut data).unwrap();
                frames.push(data);
            }
            frames
        });
        let s = settings(syslog(&format!("tcp://127.0.0.1:{port}")));
        assert!(send(&s, LINE, true, 0).iter().all(|r| r.ok));
        assert!(send(&s, "second", true, 0).iter().all(|r| r.ok));
        let frames = server.join().unwrap();
        for (frame, msg) in frames.iter().zip([LINE, "second"]) {
            let text = std::str::from_utf8(frame).unwrap();
            let (len, rest) = text.split_once(' ').unwrap();
            assert_eq!(len.parse::<usize>().unwrap(), rest.len());
            assert!(rest.starts_with("<37>1 "), "{rest}");
            assert!(rest.ends_with(&format!(" audit - {msg}")), "{rest}");
        }
    }

    #[test]
    fn refused_connection_is_reported_and_counted() {
        let port = {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            listener.local_addr().unwrap().port()
        };
        let before = failure_count();
        let reports = send(
            &settings(syslog(&format!("tcp://127.0.0.1:{port}"))),
            LINE,
            true,
            0,
        );
        assert_eq!(reports.len(), 1);
        assert!(!reports[0].ok);
        assert!(reports[0].error.is_some());
        assert!(failure_count() > before);
    }

    #[test]
    fn a_silent_collector_cannot_hang_the_caller() {
        // Accepts the connection but never answers the TLS handshake.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let _server = std::thread::spawn(move || {
            let conn = listener.accept();
            std::thread::sleep(Duration::from_secs(5));
            drop(conn);
        });
        let mut config = syslog(&format!("tls://127.0.0.1:{port}"));
        config.timeout_ms = 200;
        let started = Instant::now();
        let reports = send(&settings(config), LINE, true, 0);
        assert!(started.elapsed() < Duration::from_secs(2));
        assert!(!reports[0].ok);
    }

    #[test]
    fn waiting_stops_at_the_deadline() {
        let job: Job = Box::new(|| {
            std::thread::sleep(Duration::from_secs(3));
            Ok(())
        });
        let rx = spawn_job(job).unwrap();
        let timeout = Duration::from_millis(100);
        let started = Instant::now();
        let err = wait(&rx, started + timeout, timeout).unwrap_err();
        assert!(started.elapsed() < Duration::from_secs(1));
        assert!(err.contains("timed out"), "{err}");
    }

    #[test]
    fn spool_appends_json_lines() {
        let dir = temp_dir("spool").join("nested");
        let s = settings(AuditForward::new(None, Some(dir.display().to_string())));
        assert!(send(&s, LINE, true, 0).iter().all(|r| r.ok));
        assert!(send(&s, r#"{"second":true}"#, true, 0).iter().all(|r| r.ok));
        let file = spool_file(&dir).unwrap();
        let name = file.file_name().unwrap().to_string_lossy().to_string();
        assert!(name.starts_with("ferrika-audit-") && name.ends_with(".jsonl"));
        let text = std::fs::read_to_string(&file).unwrap();
        assert_eq!(text, format!("{LINE}\n{{\"second\":true}}\n"));
        let _ = std::fs::remove_dir_all(dir.parent().unwrap());
    }

    fn spool_names(dir: &Path) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
            .collect();
        names.sort();
        names
    }

    #[test]
    fn spool_file_names_are_random_and_strictly_matched() {
        let stem = spool_stem();
        assert!(stem.starts_with("ferrika-audit-") && stem.ends_with('.'));
        let ok = format!("{stem}0123456789abcdef.jsonl");
        assert!(is_spool_name(&ok, &stem));
        for bad in [
            format!("{stem}jsonl"),
            format!("{stem}0123456789ABCDEF.jsonl"),
            format!("{stem}0123456789abcde.jsonl"),
            format!("{stem}0123456789abcdef.jsonl.tmp"),
            format!("{}jsonl", stem.trim_end_matches('.')),
        ] {
            assert!(!is_spool_name(&bad, &stem), "{bad}");
        }
        let a = random_hex(8).unwrap();
        assert_eq!(a.len(), 16);
        assert_ne!(a, random_hex(8).unwrap());

        // Create-new: every file gets its own random name.
        let dir = temp_dir("spool-names");
        std::fs::create_dir_all(&dir).unwrap();
        let (first, _) = create_spool_file(&dir).unwrap();
        let (second, _) = create_spool_file(&dir).unwrap();
        assert_ne!(first, second);
        for path in [&first, &second] {
            assert!(is_spool_name(
                &path.file_name().unwrap().to_string_lossy(),
                &stem
            ));
        }
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn spool_reuses_this_accounts_file_and_ignores_other_names() {
        let dir = temp_dir("spool-reuse");
        std::fs::create_dir_all(&dir).unwrap();
        // The old fixed name and foreign files are never written to.
        let legacy = dir.join(format!("{}jsonl", spool_stem()));
        std::fs::write(&legacy, "").unwrap();
        std::fs::write(dir.join("other.jsonl"), "").unwrap();
        append_spool(&dir, "one").unwrap();
        append_spool(&dir, "two").unwrap();
        assert_eq!(spool_names(&dir).len(), 3);
        let file = spool_file(&dir).unwrap();
        assert_eq!(std::fs::read_to_string(file).unwrap(), "one\ntwo\n");
        assert_eq!(std::fs::read_to_string(&legacy).unwrap(), "");
        let _ = std::fs::remove_dir_all(dir);
    }

    #[cfg(unix)]
    mod unix_spool {
        use super::super::*;
        use super::{spool_names, temp_dir};
        use std::os::unix::fs::{symlink, PermissionsExt};

        fn mode(path: &Path) -> u32 {
            std::fs::metadata(path).unwrap().permissions().mode() & 0o7777
        }

        fn private_dir(name: &str) -> PathBuf {
            let dir = temp_dir(name);
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
            dir
        }

        #[test]
        fn new_folder_and_files_are_owner_only() {
            let dir = temp_dir("spool-mode").join("nested");
            append_spool(&dir, "x").unwrap();
            assert_eq!(mode(&dir), 0o700);
            assert_eq!(mode(&spool_file(&dir).unwrap()), 0o600);
            let _ = std::fs::remove_dir_all(dir.parent().unwrap());
        }

        #[test]
        fn a_symlinked_spool_file_is_skipped_not_followed() {
            let dir = private_dir("spool-link");
            let target = dir.join("victim.txt");
            std::fs::write(&target, "keep").unwrap();
            let link = dir.join(format!("{}ffffffffffffffff.jsonl", spool_stem()));
            symlink(&target, &link).unwrap();
            append_spool(&dir, "entry").unwrap();
            assert_eq!(std::fs::read_to_string(&target).unwrap(), "keep");
            // The link is left in place; the entry went to a new file.
            assert!(std::fs::symlink_metadata(&link).is_ok());
            let file = spool_file(&dir).unwrap();
            assert_ne!(file, link);
            assert_eq!(std::fs::read_to_string(file).unwrap(), "entry\n");
            assert_eq!(spool_names(&dir).len(), 3);
            let _ = std::fs::remove_dir_all(dir);
        }

        #[test]
        fn a_group_writable_or_hard_linked_spool_file_is_skipped() {
            let dir = private_dir("spool-perm");
            let open = dir.join(format!("{}aaaaaaaaaaaaaaaa.jsonl", spool_stem()));
            std::fs::write(&open, "").unwrap();
            std::fs::set_permissions(&open, std::fs::Permissions::from_mode(0o666)).unwrap();
            let linked = dir.join(format!("{}bbbbbbbbbbbbbbbb.jsonl", spool_stem()));
            std::fs::write(&linked, "").unwrap();
            std::fs::set_permissions(&linked, std::fs::Permissions::from_mode(0o600)).unwrap();
            std::fs::hard_link(&linked, dir.join("second-name")).unwrap();
            append_spool(&dir, "entry").unwrap();
            assert_eq!(std::fs::read_to_string(&open).unwrap(), "");
            assert_eq!(std::fs::read_to_string(&linked).unwrap(), "");
            let file = spool_file(&dir).unwrap();
            assert!(file != open && file != linked);
            assert_eq!(std::fs::read_to_string(file).unwrap(), "entry\n");
            let _ = std::fs::remove_dir_all(dir);
        }

        #[test]
        fn a_world_writable_folder_is_refused_unless_sticky() {
            let dir = temp_dir("spool-shared");
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o777)).unwrap();
            let err = append_spool(&dir, "entry").unwrap_err().to_string();
            assert!(err.contains("group or others"), "{err}");
            assert!(spool_names(&dir).is_empty());
            std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o1777)).unwrap();
            append_spool(&dir, "entry").unwrap();
            assert_eq!(spool_names(&dir).len(), 1);
            let _ = std::fs::remove_dir_all(dir);
        }
    }

    #[cfg(windows)]
    #[test]
    fn own_spool_files_pass_the_windows_checks() {
        let dir = temp_dir("spool-owner");
        std::fs::create_dir_all(&dir).unwrap();
        let (path, file) = create_spool_file(&dir).unwrap();
        crate::file_security::check_private_file(&file).unwrap();
        drop(file);
        assert!(open_existing_spool(&path).is_ok());
        let _ = std::fs::remove_dir_all(dir);
    }

    #[cfg(windows)]
    #[test]
    fn a_spool_folder_users_may_write_is_refused_on_windows() {
        let dir = temp_dir("spool-acl");
        std::fs::create_dir_all(&dir).unwrap();
        // Grant the Users group write access, inherited by files.
        let status = std::process::Command::new("icacls")
            .arg(&dir)
            .args(["/grant", "*S-1-5-32-545:(OI)(CI)W", "/q"])
            .status()
            .unwrap();
        assert!(status.success());
        let err = append_spool(&dir, "entry").unwrap_err().to_string();
        assert!(
            err.contains("spool folder") && err.contains("S-1-5-32-545"),
            "{err}"
        );
        assert!(spool_names(&dir).is_empty());
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn syslog_and_spool_both_receive_the_entry() {
        let server = UdpSocket::bind("127.0.0.1:0").unwrap();
        server
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let port = server.local_addr().unwrap().port();
        let dir = temp_dir("both");
        let s = settings(AuditForward::new(
            Some(format!("udp://127.0.0.1:{port}")),
            Some(dir.display().to_string()),
        ));
        let reports = send(&s, LINE, true, 0);
        assert_eq!(reports.len(), 2);
        assert!(reports.iter().all(|r| r.ok), "{reports:?}");
        let mut buf = [0u8; 4096];
        let (n, _) = server.recv_from(&mut buf).unwrap();
        assert!(std::str::from_utf8(&buf[..n]).unwrap().ends_with(LINE));
        assert!(std::fs::read_to_string(spool_file(&dir).unwrap())
            .unwrap()
            .contains(LINE));
        let _ = std::fs::remove_dir_all(dir);
    }

    fn verified(rules: PolicyRules) -> EffectivePolicy {
        EffectivePolicy {
            status: PolicyStatus::Verified {
                issuer: "sec".into(),
                key_id: "k".into(),
            },
            rules,
            dir: PathBuf::new(),
            issued_at: String::new(),
            expiry: None,
            trusted_keys: Vec::new(),
        }
    }

    #[test]
    fn policy_wins_over_the_environment() {
        let env = Some("udp://127.0.0.1:5514");
        // No policy: the environment variable applies, never `required`.
        let absent = EffectivePolicy::absent(PathBuf::new());
        let s = resolve(&absent, env).unwrap().unwrap();
        assert_eq!(s.origin, ForwardOrigin::Env);
        assert!(!s.blocks_actions(true));
        assert_eq!(resolve(&absent, None).unwrap(), None);
        assert_eq!(resolve(&absent, Some("  ")).unwrap(), None);

        // A policy without forwarding leaves the environment variable alone.
        let no_forward = verified(PolicyRules::default());
        assert_eq!(
            resolve(&no_forward, env).unwrap().unwrap().origin,
            ForwardOrigin::Env
        );

        // A policy with forwarding replaces it.
        let mut forward = syslog("tcp://siem.corp:6601");
        forward.required = true;
        let rules = PolicyRules {
            audit_forward: Some(forward),
            ..PolicyRules::default()
        };
        let s = resolve(&verified(rules.clone()), env).unwrap().unwrap();
        assert_eq!(s.origin, ForwardOrigin::Policy);
        assert_eq!(s.describe(), "tcp://siem.corp:6601");
        assert!(s.blocks_actions(true));
        assert!(!s.blocks_actions(false), "only with audit: required");

        // An invalid policy's section is not trusted (strictest rules).
        let invalid = EffectivePolicy::invalid(PathBuf::new(), "bad".into());
        assert_eq!(resolve(&invalid, None).unwrap(), None);
    }

    /// A policy signed before `audit_forward` existed (fixed key, fixed bytes).
    const OLD_POLICY_PUB: &str = "f6413c274683ecd520bb5ce14e15068eba5266a747f5f80c00b52fb184e9a755";
    const OLD_POLICY: &str = r#"{
  "format": 1,
  "key_id": "988d3f6a92c3b649",
  "payload": "{\"issuer\":\"security\",\"issued_at\":\"2026-11-01\",\"rules\":{\"read_only\":[\"prod-*\"],\"forbid\":[\"topics.delete\"],\"masking\":\"enforced\",\"unmask\":\"with_reason\",\"audit\":\"required\"}}",
  "signature": "a2f7c44d068d433fa417a1dc61b52ca8ef39640096d16edf897b0fc8e35cfbab37d19e4a506b64ddbe18aa89c340832b584643db5135099b8e51f9d0a3ad7c05"
}"#;

    #[test]
    fn old_signed_policies_still_verify_and_forward_nothing() {
        use crate::policy::{from_hex, verify};
        let key = from_hex(OLD_POLICY_PUB).unwrap();
        let payload = verify(OLD_POLICY, &key).unwrap();
        assert_eq!(payload.rules.audit_forward, None);
        assert_eq!(
            payload.rules.audit,
            ferrika_core::model::policy::AuditRule::Required
        );
        let policy = verified(payload.rules);
        assert_eq!(resolve(&policy, None).unwrap(), None);

        // Adding the section to a signed payload is tampering.
        let tampered = OLD_POLICY.replace(
            r#"\"audit\":\"required\"}"#,
            r#"\"audit\":\"required\",\"audit_forward\":{\"syslog\":\"udp://evil:514\"}}"#,
        );
        assert_ne!(tampered, OLD_POLICY);
        assert!(verify(&tampered, &key).is_err());
    }

    #[test]
    fn policies_with_the_section_sign_verify_and_load() {
        use crate::policy::{
            generate_keypair, load_from, sign, to_hex, verify, POLICY_FILE, PUBLIC_KEY_FILE,
        };
        let payload = r#"{"issuer":"security","rules":{"audit":"required","audit_forward":{"syslog":"tcp://siem.corp:601","facility":"auth","app_name":"ferrika","required":true}}}"#;
        let (private, public) = generate_keypair().unwrap();
        let text = serde_json::to_string_pretty(&sign(&private, payload).unwrap()).unwrap();
        let rules = verify(&text, &public).unwrap().rules;
        assert!(rules.audit_forward.as_ref().is_some_and(|f| f.required));

        let dir = temp_dir("policy");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(PUBLIC_KEY_FILE), to_hex(&public)).unwrap();
        std::fs::write(dir.join(POLICY_FILE), &text).unwrap();
        let policy = load_from(&dir);
        let s = resolve(&policy, Some("udp://127.0.0.1:1"))
            .unwrap()
            .unwrap();
        assert_eq!(s.origin, ForwardOrigin::Policy);
        assert!(s.blocks_actions(policy.audit_required()));

        // A bad section cannot be signed (and fails verification the same way).
        let bad = r#"{"rules":{"audit_forward":{"syslog":"ftp://siem"}}}"#;
        assert!(sign(&private, bad).is_err());
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn local_settings_parse() {
        let f = parse_local("tls://siem:6514").unwrap();
        assert_eq!(f.syslog.as_deref(), Some("tls://siem:6514"));
        let f = parse_local(r"spool:C:\ProgramData\Ferrika\spool").unwrap();
        assert_eq!(
            f.spool_dir.as_deref(),
            Some(r"C:\ProgramData\Ferrika\spool")
        );
        let f = parse_local(r#"{"syslog":"udp://h","facility":"local3","required":true}"#).unwrap();
        assert_eq!(f.facility, SyslogFacility::Local3);
        assert!(!f.required, "only a signed policy can block actions");
        assert!(parse_local("siem:514").is_err());
        assert!(parse_local("spool:").is_err());
        assert!(parse_local(r#"{"syslog":"udp://h","bogus":1}"#).is_err());
    }
}
