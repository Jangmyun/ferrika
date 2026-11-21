//! Append-only log of operations that changed a cluster, in
//! `~/.ferrika/audit.log` next to `profiles.json` (one JSON object per line).
//!
//! Lines are hash-chained (see [`verify_chain`]). The chain has no key: it
//! is tamper-evident against accidental edits, truncation and interrupted
//! writes, not against a determined local user, who can rewrite the file and
//! recompute every hash. Each chained line also has a `seq`; with an
//! external anchor (`crates/ferrika-governance/src/audit_anchor.rs`) cutting the end off the log
//! is detected too.

use std::io::{BufRead, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use ferrika_core::kafka::profile::ProfileStore;

/// One recorded operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditEntry {
    /// Unix time in milliseconds.
    pub time_ms: i64,
    /// `gui`, `cli` or `mcp`.
    pub source: String,
    /// Profile (or cluster label) the operation ran against.
    pub profile: String,
    /// What was done, e.g. "Deleting topics" or "topics delete".
    pub action: String,
    /// What it was done to, e.g. a topic name; never record payloads or secrets.
    pub target: String,
    /// The result shown to the user, or "requested" when it is not known.
    pub outcome: String,
    pub ok: bool,
    /// OS user who ran Ferrika; filled in by [`record`] when empty.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub user: String,
    /// The program that made the change for `user`, e.g.
    /// `mcp:claude-code/2.1.4#a1b2c3d4` (MCP client and session). The client
    /// names itself, so this says which tool, not who: `user` is the
    /// identity. Absent on lines without one, which keeps their hashes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor: Option<String>,
    /// Id of the operation in the history journal (`history.jsonl`), for
    /// changes `ferrika undo` can find.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub op: String,
    /// SHA-256 of that operation's recorded before- and after-state; undo
    /// refuses a journal entry that no longer matches it.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub op_digest: String,
}

/// `prev` of the first chained line.
const GENESIS: &str = "0000000000000000000000000000000000000000000000000000000000000000";

/// File name of the log.
const AUDIT_FILE: &str = "audit.log";

/// The OS account that runs Ferrika, asked from the OS (`GetUserNameW` on
/// Windows, `getuid` + passwd on Unix). The `USERNAME`/`USER`/`LOGNAME`
/// environment variables, which any process can set, are only a fallback
/// for when the OS call fails.
pub fn os_user() -> String {
    os_account_name()
        .filter(|name| !name.is_empty())
        .or_else(env_user)
        .unwrap_or_else(|| "unknown".to_string())
}

fn env_user() -> Option<String> {
    ["USERNAME", "USER", "LOGNAME"]
        .iter()
        .find_map(|k| std::env::var(k).ok().filter(|v| !v.is_empty()))
}

#[cfg(windows)]
fn os_account_name() -> Option<String> {
    use windows_sys::Win32::System::WindowsProgramming::GetUserNameW;
    // UNLEN (256) plus the terminating NUL.
    let mut buf = [0u16; 257];
    let mut len = buf.len() as u32;
    // SAFETY: `buf` holds `len` UTF-16 units; GetUserNameW writes at most
    // that many (NUL included) and stores the count it wrote in `len`.
    let ok = unsafe { GetUserNameW(buf.as_mut_ptr(), &mut len) };
    if ok == 0 {
        return None;
    }
    let chars = (len as usize).saturating_sub(1).min(buf.len());
    String::from_utf16(&buf[..chars]).ok()
}

#[cfg(unix)]
fn os_account_name() -> Option<String> {
    passwd_entry().map(|(name, _)| name)
}

#[cfg(not(any(windows, unix)))]
fn os_account_name() -> Option<String> {
    None
}

/// The current user's name and home directory from the passwd database.
#[cfg(unix)]
fn passwd_entry() -> Option<(String, PathBuf)> {
    use std::ffi::{CStr, OsStr};
    use std::os::unix::ffi::OsStrExt;

    // SAFETY: getuid cannot fail.
    let uid = unsafe { libc::getuid() };
    let mut buf: Vec<libc::c_char> = vec![0; 4096];
    loop {
        // SAFETY: an all-zero passwd is a valid value for getpwuid_r to fill.
        let mut pwd: libc::passwd = unsafe { std::mem::zeroed() };
        let mut result: *mut libc::passwd = std::ptr::null_mut();
        // SAFETY: every pointer is valid for the call and `buf.len()` is the
        // buffer's real size; the strings in `pwd` point into `buf`.
        let rc =
            unsafe { libc::getpwuid_r(uid, &mut pwd, buf.as_mut_ptr(), buf.len(), &mut result) };
        if rc == libc::ERANGE && buf.len() < (1 << 20) {
            let grown = buf.len() * 2;
            buf.resize(grown, 0);
            continue;
        }
        if rc != 0 || result.is_null() || pwd.pw_name.is_null() {
            return None;
        }
        // SAFETY: getpwuid_r succeeded, so pw_name (and pw_dir when not null)
        // are NUL-terminated strings inside `buf`, which is still alive.
        let name = unsafe { CStr::from_ptr(pwd.pw_name) }
            .to_string_lossy()
            .into_owned();
        let home = if pwd.pw_dir.is_null() {
            PathBuf::new()
        } else {
            // SAFETY: as above.
            let dir = unsafe { CStr::from_ptr(pwd.pw_dir) };
            PathBuf::from(OsStr::from_bytes(dir.to_bytes()))
        };
        return Some((name, home));
    }
}

/// The account's home directory from the OS rather than `HOME`/`USERPROFILE`
/// (on Windows the Profile known folder, on Unix the passwd entry).
#[cfg(windows)]
fn os_home_dir() -> Option<PathBuf> {
    dirs::home_dir()
}

#[cfg(unix)]
fn os_home_dir() -> Option<PathBuf> {
    passwd_entry()
        .map(|(_, home)| home)
        .filter(|home| !home.as_os_str().is_empty())
}

#[cfg(not(any(windows, unix)))]
fn os_home_dir() -> Option<PathBuf> {
    None
}

/// `HOME` (or `USERPROFILE` on Windows), when the OS gives no home directory.
fn env_home_dir() -> Option<PathBuf> {
    let keys: &[&str] = if cfg!(windows) {
        &["USERPROFILE", "HOME"]
    } else {
        &["HOME"]
    };
    keys.iter()
        .find_map(|k| std::env::var_os(k).filter(|v| !v.is_empty()))
        .map(PathBuf::from)
}

fn sha256_hex(text: &str) -> String {
    let digest = ring::digest::digest(&ring::digest::SHA256, text.as_bytes());
    ferrika_core::model::format::to_hex(digest.as_ref())
}

/// Where the log is written and read.
///
/// Normally `audit.log` in the directory that holds `profiles.json`, which
/// follows `FERRIKA_CONFIG_PATH` / `FERRIKA_CONFIG_DIR`. When the company
/// policy requires auditing, those overrides are ignored and the log stays
/// in the account's own `~/.ferrika` folder (home directory from the OS), so
/// setting a variable is not enough to send it to a throwaway directory.
///
/// This only raises the bar: the hash chain has no key, so it reveals
/// accidental edits but not a determined local user who rewrites the file.
pub fn audit_path() -> PathBuf {
    audit_path_for(
        crate::policy::current().audit_required(),
        ProfileStore::default_path(),
    )
}

/// [`audit_path`] for a given policy and `profiles.json` location.
fn audit_path_for(audit_required: bool, profiles_path: PathBuf) -> PathBuf {
    if audit_required {
        if let Some(home) = os_home_dir().or_else(env_home_dir) {
            return home.join(".ferrika").join(AUDIT_FILE);
        }
    }
    profiles_path.with_file_name(AUDIT_FILE)
}

/// Appends `entry` as one line. Failures are logged, never fatal: auditing
/// must not block the operation it describes.
pub fn record(entry: &AuditEntry) {
    if let Err(e) = try_record(entry) {
        tracing::warn!(%e, "Could not write the audit log");
    }
}

/// [`record`] that reports a failure, for policies that require auditing.
///
/// After the line is in the local log it is forwarded to the SIEM targets
/// in force (see [`crate::audit_forward`]), if any, and the policy's
/// external anchor (if any) gets the new line's `seq`/`hash`.
pub fn try_record(entry: &AuditEntry) -> std::io::Result<()> {
    let line = append_local(entry)?;
    forward_appended(
        &line,
        entry,
        crate::audit_forward::settings(),
        crate::policy::current().audit_required(),
    )
}

/// Appends `entry` to the local log only (anchored like any entry), returning
/// the chained line (for `audit forward-test`, which forwards it itself and
/// reports each target).
pub fn append_local(entry: &AuditEntry) -> std::io::Result<String> {
    let path = audit_path();
    let anchor = crate::audit_anchor::configured(&path);
    append_line(&path, entry, anchor.anchor.as_ref(), anchor.required).map(|(_, line)| line)
}

/// Appends without an external anchor; returns the line written.
#[cfg(test)]
fn append(path: &Path, entry: &AuditEntry) -> std::io::Result<String> {
    append_line(path, entry, &crate::audit_anchor::NoAnchor, false).map(|(_, line)| line)
}

/// [`append_line`] returning only the new line's `seq`.
#[cfg(test)]
pub(crate) fn append_with(
    path: &Path,
    entry: &AuditEntry,
    anchor: &dyn crate::audit_anchor::AuditAnchor,
    anchor_required: bool,
) -> std::io::Result<u64> {
    append_line(path, entry, anchor, anchor_required).map(|(seq, _)| seq)
}

/// Forwards a line that is already in the local log. Waits at most the
/// configured timeout. A failure is counted, logged and, for the CLI, shown
/// on stderr; it is never written into the chain. It refuses the action
/// (returns an error) only when a signed policy requires auditing and marks
/// forwarding `required`; the entry stays in the local log either way.
fn forward_appended(
    line: &str,
    entry: &AuditEntry,
    settings: Option<&crate::audit_forward::ForwardSettings>,
    audit_required: bool,
) -> std::io::Result<()> {
    let Some(settings) = settings else {
        return Ok(());
    };
    let failures: Vec<String> = crate::audit_forward::send(settings, line, entry.ok, entry.time_ms)
        .into_iter()
        .filter_map(|r| r.error.map(|e| format!("{}: {e}", r.target)))
        .collect();
    if failures.is_empty() {
        return Ok(());
    }
    let summary = failures.join("; ");
    tracing::warn!(%summary, "Could not forward the audit entry");
    if settings.blocks_actions(audit_required) {
        return Err(std::io::Error::other(format!(
            "the entry is in the local log, but forwarding it is required and failed ({summary})"
        )));
    }
    if entry.source == "cli" {
        ferrika_core::notice::warn(&format!(
            "audit entry saved locally but not forwarded to the SIEM ({summary})"
        ));
    }
    Ok(())
}

/// Appends one hash-chained line: `prev` is the previous line's `hash` (or
/// the SHA-256 of an older unchained or torn line), and `hash` is the SHA-256
/// of the line's JSON without `hash`. Editing, inserting or removing a line in
/// the middle breaks the chain; see [`verify_chain`].
///
/// Reading the last line and appending happen under an exclusive lock on
/// `audit.log.lock`, so writers in several processes (GUI and CLI) or threads
/// never chain two lines to the same predecessor. The lock is a separate file
/// so readers of the log are never blocked (Windows locks are mandatory).
///
/// A last line without its newline was cut short by an interrupted write.
/// It is kept as it is: the new line starts on a fresh line and chains to the
/// SHA-256 of the fragment, which [`verify_chain`] reports as torn.
///
/// Each chained line also gets `seq`, its 1-based position among the
/// chained lines (see [`next_seq`]). After the line is written, still under
/// the lock, `anchor` records `seq` and `hash`; a failed anchor write only
/// warns unless `anchor_required` (the line stays written either way).
/// Returns the new line's `seq` and the line written (without its newline).
fn append_line(
    path: &Path,
    entry: &AuditEntry,
    anchor: &dyn crate::audit_anchor::AuditAnchor,
    anchor_required: bool,
) -> std::io::Result<(u64, String)> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let mut entry = entry.clone();
    if entry.user.is_empty() {
        entry.user = os_user();
    }
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(lock_path(path))?;
    // Released when `lock` is dropped (or the process exits).
    lock.lock()?;
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .append(true)
        .open(path)?;
    let tail = read_tail(&mut file)?;
    let prev = tail
        .last_line
        .as_deref()
        .map(chain_hash_of)
        .unwrap_or_else(|| GENESIS.to_string());
    let seq = next_seq(&mut file, &tail)?;
    let (line, hash) = chained_line(&entry, &prev, seq)?;
    let mut out = String::with_capacity(line.len() + 2);
    if tail.unterminated {
        out.push('\n');
    }
    out.push_str(&line);
    out.push('\n');
    // One write, so a crash leaves at most one torn line at the end.
    file.write_all(out.as_bytes())?;
    file.flush()?;
    if let Err(e) = anchor.record(seq, &hash) {
        crate::audit_anchor::on_record_error(e, anchor_required)?;
    }
    Ok((seq, line))
}

/// `seq` for the next chained line: one more than the last line's `seq`.
///
/// When the last line has none (empty file, lines written before `seq`
/// existed, or a torn fragment) the file is read once and the chained lines
/// (lines with a `hash`) are counted, so the first line written by this
/// version gets `seq` = number of existing chained lines + 1.
fn next_seq(file: &mut std::fs::File, tail: &Tail) -> std::io::Result<u64> {
    if let Some(seq) = tail.last_line.as_deref().and_then(seq_of) {
        return Ok(seq + 1);
    }
    file.seek(SeekFrom::Start(0))?;
    let mut chained = 0u64;
    for line in std::io::BufReader::new(&mut *file).lines() {
        let line = line.unwrap_or_default();
        let value = serde_json::from_str::<serde_json::Value>(&line).ok();
        if value.as_ref().and_then(|v| v.get("hash")).is_some() {
            chained += 1;
        }
    }
    Ok(chained + 1)
}

fn seq_of(line: &str) -> Option<u64> {
    serde_json::from_str::<serde_json::Value>(line)
        .ok()?
        .get("seq")?
        .as_u64()
}

/// `audit.log.lock` next to `audit.log`.
fn lock_path(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(".lock");
    PathBuf::from(name)
}

/// The line to append and its `hash`.
fn chained_line(entry: &AuditEntry, prev: &str, seq: u64) -> std::io::Result<(String, String)> {
    let serde_json::Value::Object(mut map) =
        serde_json::to_value(entry).map_err(std::io::Error::other)?
    else {
        return Err(std::io::Error::other("audit entry is not an object"));
    };
    map.insert("seq".into(), seq.into());
    map.insert("prev".into(), prev.into());
    let hash = sha256_hex(&serde_json::Value::Object(map.clone()).to_string());
    map.insert("hash".into(), hash.clone().into());
    Ok((serde_json::Value::Object(map).to_string(), hash))
}

/// What the next line's `prev` must be after `line`.
fn chain_hash_of(line: &str) -> String {
    serde_json::from_str::<serde_json::Value>(line)
        .ok()
        .and_then(|v| v.get("hash").and_then(|h| h.as_str()).map(str::to_string))
        .unwrap_or_else(|| sha256_hex(line))
}

/// The end of the log, as far as [`append`] needs it.
#[derive(Debug, PartialEq, Eq)]
struct Tail {
    /// The last non-blank line (without its line break).
    last_line: Option<String>,
    /// The file is not empty and does not end with a newline.
    unterminated: bool,
}

/// Reads backwards from the end of `file` until the last non-blank line is
/// complete, so appending stays cheap however long the log grows.
fn read_tail(file: &mut std::fs::File) -> std::io::Result<Tail> {
    const CHUNK: u64 = 8192;
    let len = file.seek(SeekFrom::End(0))?;
    let mut pos = len;
    let mut buf: Vec<u8> = Vec::new();
    while pos > 0 {
        let start = pos.saturating_sub(CHUNK);
        let mut chunk = vec![0u8; (pos - start) as usize];
        file.seek(SeekFrom::Start(start))?;
        file.read_exact(&mut chunk)?;
        chunk.extend_from_slice(&buf);
        buf = chunk;
        pos = start;
        let content_end = buf.iter().rposition(|b| !b.is_ascii_whitespace());
        if let Some(end) = content_end {
            if buf[..end].contains(&b'\n') {
                break;
            }
        }
    }
    let text = String::from_utf8_lossy(&buf);
    let text = if pos == 0 {
        text.trim_start_matches('\u{feff}')
    } else {
        &text
    };
    let last_line = text
        .lines()
        .rev()
        .find(|l| !l.trim().is_empty())
        .map(str::to_string);
    Ok(Tail {
        last_line,
        unterminated: buf.last().is_some_and(|b| *b != b'\n'),
    })
}

const NO_HASH: &str = "line has no hash after chained lines (inserted or edited)";

/// Result of checking the audit log's hash chain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ChainReport {
    /// Non-empty lines read.
    pub lines: usize,
    /// Lines with a hash.
    pub chained: usize,
    /// Older lines written before chaining, at the start of the file.
    pub legacy: usize,
    /// 1-based number of the first line that breaks the chain, and why.
    pub broken: Option<(usize, String)>,
    /// 1-based numbers of lines cut short by an interrupted write (not JSON,
    /// and either the last line or chained to by the next line). Their
    /// contents are lost, but the chain around them is intact.
    pub torn: Vec<usize>,
    /// `hash` of the last chained line read (with `chained`, its `seq`), for
    /// comparing with an external anchor.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_hash: Option<String>,
}

/// Checks the audit log at the default location.
pub fn verify() -> std::io::Result<ChainReport> {
    verify_chain(&audit_path())
}

/// Checks every line's `prev` and `hash`. Unchained lines are accepted only
/// before the first chained line (logs written by older versions). After
/// that, a line that is not JSON is accepted as torn (see `append`) only if
/// it is the last line or the next line's `prev` is its SHA-256.
pub fn verify_chain(path: &Path) -> std::io::Result<ChainReport> {
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(e) => return Err(e),
    };
    // Editors (and PowerShell's Set-Content) may add a byte order mark.
    let text = text.trim_start_matches('\u{feff}');
    let mut report = ChainReport {
        lines: 0,
        chained: 0,
        legacy: 0,
        broken: None,
        torn: Vec::new(),
        last_hash: None,
    };
    let mut expected_prev = GENESIS.to_string();
    // A non-JSON line after chained lines, accepted once the next line
    // chains to it (or nothing follows it).
    let mut pending_torn: Option<usize> = None;
    for (ix, line) in text.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        report.lines += 1;
        let number = ix + 1;
        let parsed = serde_json::from_str::<serde_json::Value>(line).ok();
        let map = parsed.as_ref().and_then(|v| v.as_object());
        let hash = map.and_then(|m| m.get("hash")).and_then(|h| h.as_str());
        let (Some(map), Some(hash)) = (map, hash) else {
            if report.chained > 0 && parsed.is_none() && pending_torn.is_none() {
                pending_torn = Some(number);
                expected_prev = sha256_hex(line);
                continue;
            }
            if let Some(torn) = pending_torn {
                report.broken = Some((torn, NO_HASH.to_string()));
                return Ok(report);
            }
            if report.chained > 0 {
                report.broken = Some((number, NO_HASH.to_string()));
                return Ok(report);
            }
            report.legacy += 1;
            expected_prev = sha256_hex(line);
            continue;
        };
        let prev = map.get("prev").and_then(|p| p.as_str()).unwrap_or_default();
        if prev != expected_prev {
            if let Some(torn) = pending_torn {
                report.broken = Some((torn, NO_HASH.to_string()));
                return Ok(report);
            }
            report.broken = Some((
                number,
                "prev does not match the line before (a line was removed, inserted or reordered)"
                    .to_string(),
            ));
            return Ok(report);
        }
        let without_hash: serde_json::Map<String, serde_json::Value> = map
            .iter()
            .filter(|(k, _)| k.as_str() != "hash")
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        if sha256_hex(&serde_json::Value::Object(without_hash).to_string()) != hash {
            report.broken = Some((
                number,
                "hash does not match the line's contents (the line was edited)".to_string(),
            ));
            return Ok(report);
        }
        // A duplicated key keeps the hash above (the map holds one copy) but
        // makes the readers skip the line (`recent`, `op_digest`): an edit
        // that hides an entry.
        if has_duplicate_keys(line) {
            report.broken = Some((
                number,
                "the line is not a well-formed entry (a duplicated field)".to_string(),
            ));
            return Ok(report);
        }
        // `seq` is the line's position among chained lines. Lines written
        // before `seq` existed have none and still count.
        if let Some(seq) = map.get("seq") {
            let expected = report.chained as u64 + 1;
            if seq.as_u64() != Some(expected) {
                report.broken = Some((
                    number,
                    format!("seq is {seq}, expected {expected} (lines out of sequence)"),
                ));
                return Ok(report);
            }
        }
        report.torn.extend(pending_torn.take());
        report.chained += 1;
        expected_prev = hash.to_string();
        report.last_hash = Some(hash.to_string());
    }
    report.torn.extend(pending_torn);
    Ok(report)
}

/// The last `limit` entries, oldest first. Unreadable lines are skipped.
pub fn recent(limit: usize) -> Vec<AuditEntry> {
    read_recent(&audit_path(), limit, |_| true)
}

/// The last `limit` entries that `keep` accepts, oldest first.
pub fn recent_matching(limit: usize, keep: impl Fn(&AuditEntry) -> bool) -> Vec<AuditEntry> {
    read_recent(&audit_path(), limit, keep)
}

fn read_recent(path: &Path, limit: usize, keep: impl Fn(&AuditEntry) -> bool) -> Vec<AuditEntry> {
    let Ok(file) = std::fs::File::open(path) else {
        return Vec::new();
    };
    let mut entries: Vec<AuditEntry> = std::io::BufReader::new(file)
        .lines()
        .map_while(Result::ok)
        .filter_map(|line| serde_json::from_str(&line).ok())
        .filter(|entry| keep(entry))
        .collect();
    let skip = entries.len().saturating_sub(limit);
    entries.drain(..skip);
    entries
}

/// Whether the JSON object `line` names a key twice. `serde_json::Value`
/// keeps one copy, so the chain hash does not notice; the typed readers
/// refuse the line.
fn has_duplicate_keys(line: &str) -> bool {
    struct Keys(Vec<String>);
    impl<'de> serde::Deserialize<'de> for Keys {
        fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
            struct Visit;
            impl<'de> serde::de::Visitor<'de> for Visit {
                type Value = Keys;
                fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                    f.write_str("a JSON object")
                }
                fn visit_map<A: serde::de::MapAccess<'de>>(
                    self,
                    mut map: A,
                ) -> Result<Keys, A::Error> {
                    let mut keys = Vec::new();
                    while let Some((key, _)) = map.next_entry::<String, serde::de::IgnoredAny>()? {
                        keys.push(key);
                    }
                    Ok(Keys(keys))
                }
            }
            d.deserialize_map(Visit)
        }
    }
    match serde_json::from_str::<Keys>(line) {
        Ok(Keys(mut keys)) => {
            let count = keys.len();
            keys.sort_unstable();
            keys.dedup();
            keys.len() != count
        }
        Err(_) => false,
    }
}

/// What [`op_digest`] returns when the log names two different digests for
/// one operation: never a real digest, so the check fails.
pub const CONFLICTING_OP_DIGESTS: &str = "conflicting op_digest entries";

/// The `op_digest` the audit log holds for history operation `op`, or
/// `None` when no entry names one. An operation is finished once, so a
/// second, different digest for it means a line was added afterwards (the
/// chain has no key, so one can be appended that verifies): that gives
/// [`CONFLICTING_OP_DIGESTS`], which matches no journal entry.
pub fn op_digest(op: &str) -> Option<String> {
    op_digest_in(&audit_path(), op)
}

fn op_digest_in(path: &Path, op: &str) -> Option<String> {
    let file = std::fs::File::open(path).ok()?;
    let mut found: Option<String> = None;
    for entry in std::io::BufReader::new(file)
        .lines()
        .map_while(Result::ok)
        .filter_map(|line| serde_json::from_str::<AuditEntry>(&line).ok())
        .filter(|entry| entry.op == op && !entry.op_digest.is_empty())
    {
        match &found {
            Some(first) if *first != entry.op_digest => {
                return Some(CONFLICTING_OP_DIGESTS.to_string());
            }
            Some(_) => {}
            None => found = Some(entry.op_digest),
        }
    }
    found
}

/// Current Unix time in milliseconds (kept here for existing callers).
pub use ferrika_core::kafka::clock::now_ms;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn audit_log_appends_and_reads_back_the_latest_entries() {
        let dir = std::env::temp_dir().join(format!("ferrika-audit-{}", now_ms()));
        let path = dir.join("audit.log");
        for i in 0..3 {
            let entry = AuditEntry {
                time_ms: i,
                source: "gui".into(),
                profile: "local".into(),
                action: "Deleting topics".into(),
                target: format!("t{i}"),
                outcome: "ok".into(),
                ok: true,
                user: String::new(),
                actor: None,
                op: String::new(),
                op_digest: String::new(),
            };
            append(&path, &entry).unwrap();
        }
        std::fs::write(
            &path,
            std::fs::read_to_string(&path).unwrap() + "not json\n",
        )
        .unwrap();
        let last = read_recent(&path, 2, |_| true);
        assert_eq!(last.len(), 2);
        assert_eq!(last[0].target, "t1");
        assert_eq!(last[1].target, "t2");
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn op_fields_chain_and_are_found_by_operation() {
        let dir = std::env::temp_dir().join(format!(
            "ferrika-audit-op-{}-{}",
            std::process::id(),
            now_ms()
        ));
        let path = dir.join("audit.log");
        append(&path, &entry(1)).unwrap();
        let recorded = AuditEntry {
            op: "01JBX3K8Q2M7".into(),
            op_digest: "abc".into(),
            ..entry(2)
        };
        let line = append(&path, &recorded).unwrap();
        assert!(line.contains(r#""op":"01JBX3K8Q2M7""#), "{line}");
        // Entries without an operation keep their old shape.
        assert!(!std::fs::read_to_string(&path)
            .unwrap()
            .lines()
            .next()
            .unwrap()
            .contains("\"op"));
        assert_eq!(verify_chain(&path).unwrap().broken, None);
        assert_eq!(op_digest_in(&path, "01JBX3K8Q2M7").as_deref(), Some("abc"));
        assert_eq!(op_digest_in(&path, "OTHER"), None);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn digests_are_found_in_both_the_old_and_the_new_entry_layout() {
        let dir = std::env::temp_dir().join(format!(
            "ferrika-audit-layout-{}-{}",
            std::process::id(),
            now_ms()
        ));
        let path = dir.join("audit.log");
        // Before 2026-11: a request entry without `op`, then a separate
        // `<label> recorded` entry with `op` and `op_digest`.
        append(&path, &entry(1)).unwrap();
        let old = AuditEntry {
            action: "topics delete recorded".into(),
            outcome: "Deleted orders".into(),
            ok: true,
            op: "01JBX3K8Q2M7".into(),
            op_digest: "old".into(),
            ..entry(2)
        };
        append(&path, &old).unwrap();
        // Now: the request entry names the operation, and its result entry
        // (same action) adds the digest.
        let request = AuditEntry {
            op: "01JBX3K8Q2M8".into(),
            ..entry(3)
        };
        append(&path, &request).unwrap();
        assert_eq!(op_digest_in(&path, "01JBX3K8Q2M8"), None, "not finished");
        let result = AuditEntry {
            outcome: "Deleted orders".into(),
            ok: true,
            op_digest: "new".into(),
            ..request.clone()
        };
        append(&path, &result).unwrap();
        assert_eq!(op_digest_in(&path, "01JBX3K8Q2M7").as_deref(), Some("old"));
        assert_eq!(op_digest_in(&path, "01JBX3K8Q2M8").as_deref(), Some("new"));
        assert_eq!(verify_chain(&path).unwrap().broken, None);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn a_second_digest_for_an_operation_does_not_win() {
        let (dir, path) = temp_log("second-digest");
        let finished = AuditEntry {
            op: "01JBX3K8Q2MB".into(),
            op_digest: "original".into(),
            ..entry(1)
        };
        append(&path, &finished).unwrap();
        // The same digest again (a repeated line) is fine.
        append(&path, &finished).unwrap();
        assert_eq!(
            op_digest_in(&path, "01JBX3K8Q2MB").as_deref(),
            Some("original")
        );
        // A later line with another digest, chained correctly.
        let forged = AuditEntry {
            op_digest: "forged".into(),
            ..finished.clone()
        };
        append(&path, &forged).unwrap();
        assert_eq!(verify_chain(&path).unwrap().broken, None);
        assert_eq!(
            op_digest_in(&path, "01JBX3K8Q2MB").as_deref(),
            Some(CONFLICTING_OP_DIGESTS)
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn a_duplicated_key_breaks_the_chain() {
        let (dir, path) = temp_log("duplicate-key");
        let hidden = AuditEntry {
            op: "01JBX3K8Q2MC".into(),
            op_digest: "abc".into(),
            ..entry(1)
        };
        append(&path, &hidden).unwrap();
        assert_eq!(verify_chain(&path).unwrap().broken, None);
        // Insert a duplicate of "op" before the real one: the parsed map (and
        // so the hash) is unchanged, but readers would skip the line.
        let text = std::fs::read_to_string(&path).unwrap();
        let edited = text.replacen(
            r#""op":"01JBX3K8Q2MC""#,
            r#""op":"junk","op":"01JBX3K8Q2MC""#,
            1,
        );
        assert_ne!(text, edited);
        std::fs::write(&path, edited).unwrap();
        let report = verify_chain(&path).unwrap();
        assert!(
            report
                .broken
                .as_ref()
                .is_some_and(|(_, why)| why.contains("duplicated")),
            "{:?}",
            report.broken
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    fn entry(i: i64) -> AuditEntry {
        AuditEntry {
            time_ms: i,
            source: "cli".into(),
            profile: "local".into(),
            action: "topics delete".into(),
            target: format!("t{i}"),
            outcome: "requested".into(),
            ok: true,
            user: "tester".into(),
            actor: None,
            op: String::new(),
            op_digest: String::new(),
        }
    }

    #[test]
    fn actor_is_optional_and_keeps_older_lines_verifiable() {
        let dir = std::env::temp_dir().join(format!(
            "ferrika-audit-actor-{}-{}",
            std::process::id(),
            now_ms()
        ));
        let path = dir.join("audit.log");
        std::fs::create_dir_all(&dir).unwrap();
        // A line written before `actor` existed, then one with it.
        append(&path, &entry(1)).unwrap();
        let first = std::fs::read_to_string(&path).unwrap();
        assert!(!first.contains("actor"));
        let mcp = AuditEntry {
            source: "mcp".into(),
            actor: Some("mcp:claude-code/2.1.4#a1b2c3d4".into()),
            ..entry(2)
        };
        append(&path, &mcp).unwrap();
        let report = verify_chain(&path).unwrap();
        assert_eq!((report.chained, report.broken), (2, None));
        let back = read_recent(&path, 10, |_| true);
        assert_eq!(back[0].actor, None);
        assert_eq!(
            back[1].actor.as_deref(),
            Some("mcp:claude-code/2.1.4#a1b2c3d4")
        );
        // Editing the actor breaks the chain like any other field.
        let edited = std::fs::read_to_string(&path)
            .unwrap()
            .replace("claude-code", "someone-else");
        std::fs::write(&path, edited).unwrap();
        assert_eq!(
            verify_chain(&path).unwrap().broken.as_ref().map(|b| b.0),
            Some(2)
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn audit_chain_verifies_and_detects_edits() {
        let dir = std::env::temp_dir().join(format!(
            "ferrika-audit-chain-{}-{}",
            std::process::id(),
            now_ms()
        ));
        let path = dir.join("audit.log");
        std::fs::create_dir_all(&dir).unwrap();
        // An older, unchained line first stays readable and accepted.
        std::fs::write(&path, serde_json::to_string(&entry(0)).unwrap() + "\n").unwrap();
        for i in 1..4 {
            append(&path, &entry(i)).unwrap();
        }
        let report = verify_chain(&path).unwrap();
        assert_eq!(report.lines, 4);
        assert_eq!(report.legacy, 1);
        assert_eq!(report.chained, 3);
        assert_eq!(report.broken, None);
        assert_eq!(read_recent(&path, 10, |_| true).len(), 4);

        let original = std::fs::read_to_string(&path).unwrap();

        // Edit one character of line 3.
        let edited: Vec<String> = original
            .lines()
            .enumerate()
            .map(|(ix, l)| {
                if ix == 2 {
                    l.replace("\"t2\"", "\"t9\"")
                } else {
                    l.to_string()
                }
            })
            .collect();
        std::fs::write(&path, edited.join("\n") + "\n").unwrap();
        let report = verify_chain(&path).unwrap();
        assert_eq!(report.broken.as_ref().map(|b| b.0), Some(3));

        // Remove line 2.
        let removed: Vec<&str> = original
            .lines()
            .enumerate()
            .filter(|(ix, _)| *ix != 1)
            .map(|(_, l)| l)
            .collect();
        std::fs::write(&path, removed.join("\n") + "\n").unwrap();
        let report = verify_chain(&path).unwrap();
        assert_eq!(report.broken.as_ref().map(|b| b.0), Some(2));

        // Edit the legacy line: the first chained line no longer follows it.
        let legacy_edit = original.replacen("\"t0\"", "\"tX\"", 1);
        std::fs::write(&path, legacy_edit).unwrap();
        assert_eq!(verify_chain(&path).unwrap().broken.map(|b| b.0), Some(2));

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn a_line_verifies_in_the_key_order_it_was_written() {
        // `verify_chain` serializes the parsed line again, so the hash only
        // matches if parsing keeps the key order (serde_json `preserve_order`).
        for body in [
            format!(r#"{{"time_ms":1,"source":"cli","seq":1,"prev":"{GENESIS}"}}"#),
            format!(r#"{{"prev":"{GENESIS}","seq":1,"source":"cli","time_ms":1}}"#),
        ] {
            let (dir, path) = temp_log("key-order");
            let hash = sha256_hex(&body);
            let line = format!(r#"{},"hash":"{hash}"}}"#, body.trim_end_matches('}'));
            std::fs::write(&path, line + "\n").unwrap();
            assert_eq!(verify_chain(&path).unwrap().broken, None, "{body}");
            let _ = std::fs::remove_dir_all(dir);
        }
    }

    fn temp_log(name: &str) -> (PathBuf, PathBuf) {
        let dir = std::env::temp_dir().join(format!(
            "ferrika-audit-{name}-{}-{}",
            std::process::id(),
            now_ms()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("audit.log");
        (dir, path)
    }

    #[test]
    fn concurrent_appends_keep_a_valid_chain() {
        let (dir, path) = temp_log("concurrent");
        let threads: Vec<_> = (0..8)
            .map(|t| {
                let path = path.clone();
                std::thread::spawn(move || {
                    for i in 0..25 {
                        append(&path, &entry(t * 100 + i)).unwrap();
                    }
                })
            })
            .collect();
        for thread in threads {
            thread.join().unwrap();
        }
        let report = verify_chain(&path).unwrap();
        assert_eq!(report.broken, None);
        assert_eq!(report.lines, 200);
        assert_eq!(report.chained, 200);
        assert!(report.torn.is_empty());
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn torn_last_line_is_reported_and_the_chain_continues_past_it() {
        let (dir, path) = temp_log("torn");
        append(&path, &entry(1)).unwrap();
        append(&path, &entry(2)).unwrap();
        // A crash mid-write leaves part of a line without its newline.
        let mut text = std::fs::read_to_string(&path).unwrap();
        text.push_str(r#"{"time_ms":3,"source":"cli","pro"#);
        std::fs::write(&path, &text).unwrap();
        let report = verify_chain(&path).unwrap();
        assert_eq!(report.broken, None);
        assert_eq!(report.torn, vec![3]);
        assert_eq!(report.chained, 2);

        // The next entry starts on a fresh line and chains to the fragment.
        append(&path, &entry(4)).unwrap();
        append(&path, &entry(5)).unwrap();
        let report = verify_chain(&path).unwrap();
        assert_eq!(report.broken, None);
        assert_eq!(report.torn, vec![3]);
        assert_eq!(report.lines, 5);
        assert_eq!(report.chained, 4);
        let targets: Vec<String> = read_recent(&path, 10, |_| true)
            .into_iter()
            .map(|e| e.target)
            .collect();
        assert_eq!(targets, ["t1", "t2", "t4", "t5"]);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn inserted_garbage_line_is_still_a_break() {
        let (dir, path) = temp_log("garbage");
        for i in 1..4 {
            append(&path, &entry(i)).unwrap();
        }
        let lines: Vec<String> = std::fs::read_to_string(&path)
            .unwrap()
            .lines()
            .map(str::to_string)
            .collect();
        let tampered = format!("{}\n{}\nnot json\n{}\n", lines[0], lines[1], lines[2]);
        std::fs::write(&path, tampered).unwrap();
        let report = verify_chain(&path).unwrap();
        assert_eq!(report.broken.map(|b| b.0), Some(3));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn tail_reads_only_the_last_line_across_chunks() {
        let (dir, path) = temp_log("tail");
        // A long last line spans several read chunks, after a BOM and CRLF.
        let long = format!("{{\"target\":\"{}\"}}", "x".repeat(20_000));
        std::fs::write(&path, format!("\u{feff}first\r\n{long}\r\n\r\n")).unwrap();
        let mut file = std::fs::File::open(&path).unwrap();
        let tail = read_tail(&mut file).unwrap();
        assert_eq!(tail.last_line.as_deref(), Some(long.as_str()));
        assert!(!tail.unterminated);

        std::fs::write(&path, "\u{feff}only").unwrap();
        let mut file = std::fs::File::open(&path).unwrap();
        let tail = read_tail(&mut file).unwrap();
        assert_eq!(tail.last_line.as_deref(), Some("only"));
        assert!(tail.unterminated);

        std::fs::write(&path, "").unwrap();
        let mut file = std::fs::File::open(&path).unwrap();
        let tail = read_tail(&mut file).unwrap();
        assert_eq!(
            tail,
            Tail {
                last_line: None,
                unterminated: false
            }
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn appends_after_many_lines_match_a_full_read() {
        let (dir, path) = temp_log("long");
        for i in 0..300 {
            append(&path, &entry(i)).unwrap();
        }
        let report = verify_chain(&path).unwrap();
        assert_eq!(report.broken, None);
        assert_eq!(report.chained, 300);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn required_audit_ignores_the_config_dir_override() {
        let config = std::env::temp_dir()
            .join("ferrika-elsewhere")
            .join("profiles.json");
        assert_eq!(
            audit_path_for(false, config.clone()),
            config.with_file_name("audit.log")
        );
        let required = audit_path_for(true, config.clone());
        assert_ne!(required, config.with_file_name("audit.log"));
        assert!(required.ends_with(Path::new(".ferrika").join("audit.log")));
        if let Some(home) = os_home_dir() {
            assert_eq!(required, home.join(".ferrika").join("audit.log"));
        }
    }

    #[test]
    fn appended_lines_are_forwarded_and_failures_stay_out_of_the_chain() {
        use crate::audit_forward::{spool_file, ForwardOrigin, ForwardSettings};
        use ferrika_core::model::policy::AuditForward;

        let (dir, path) = temp_log("forward");
        let spool = dir.join("spool");
        let to_spool = ForwardSettings::new(
            AuditForward::new(None, Some(spool.display().to_string())),
            ForwardOrigin::Policy,
        )
        .unwrap();
        let line = append(&path, &entry(1)).unwrap();
        forward_appended(&line, &entry(1), Some(&to_spool), true).unwrap();
        // The spool gets the exact chained line, prev and hash included.
        let spooled = std::fs::read_to_string(spool_file(&spool)).unwrap();
        assert_eq!(spooled, format!("{line}\n"));
        assert!(line.contains("\"prev\"") && line.contains("\"hash\""));

        // A dead collector: a closed port.
        let port = {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            listener.local_addr().unwrap().port()
        };
        let mut dead = AuditForward::new(Some(format!("tcp://127.0.0.1:{port}")), None);
        dead.timeout_ms = 500;
        let warn_only = ForwardSettings::new(dead.clone(), ForwardOrigin::Policy).unwrap();
        let line = append(&path, &entry(2)).unwrap();
        assert!(forward_appended(&line, &entry(2), Some(&warn_only), true).is_ok());

        dead.required = true;
        let required = ForwardSettings::new(dead, ForwardOrigin::Policy).unwrap();
        let line = append(&path, &entry(3)).unwrap();
        // Blocks only with audit: required as well.
        assert!(forward_appended(&line, &entry(3), Some(&required), false).is_ok());
        let err = forward_appended(&line, &entry(3), Some(&required), true).unwrap_err();
        assert!(err.to_string().contains("local log"), "{err}");

        // Every entry is in the local log and nothing about the failures is.
        let report = verify_chain(&path).unwrap();
        assert_eq!(report.broken, None);
        assert_eq!(report.chained, 3);
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(!text.contains("forward"), "{text}");
        let _ = std::fs::remove_dir_all(dir);
    }

    #[cfg(any(windows, unix))]
    #[test]
    fn os_user_comes_from_the_os() {
        let name = os_account_name().expect("the OS knows the current account");
        assert!(!name.is_empty());
        assert_eq!(os_user(), name);
    }
}
