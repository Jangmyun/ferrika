//! External anchor for the audit log's hash chain (policy pack v2).
//!
//! The chain in `audit.log` reveals edits, insertions and removals inside the
//! log, but cutting lines off the end (or replacing the whole file with a
//! fresh chain) leaves a valid chain. After every append the line's `seq` and
//! `hash` are therefore also written somewhere the user cannot edit: the
//! Windows Event Log (Application log, source `Ferrika`) or syslog.
//! `ferrika audit verify --anchor` compares the newest anchor with the file.
//!
//! Limits: any process of the same user can write an event with source
//! `Ferrika`, so a deliberate attacker could add a forged, older-looking
//! anchor before truncating; the forged event stays in the OS log, which only
//! an administrator can clear. On Linux and macOS the anchor goes to syslog
//! (`journalctl -t ferrika`) but is not read back: [`AuditAnchor::latest`]
//! returns `Ok(None)` there and the comparison is a manual step.

use std::io;
use std::path::Path;

use serde::Serialize;

use ferrika_core::model::policy::{AuditAnchorRule, PolicyRules};

/// Where an audit append's `seq` and `hash` are copied.
pub trait AuditAnchor {
    /// Records the chain head after a successful append.
    fn record(&self, seq: u64, hash: &str) -> io::Result<()>;
    /// The newest recorded `(seq, hash)`, or `None` when there is none or the
    /// anchor cannot be read back on this OS.
    fn latest(&self) -> io::Result<Option<(u64, String)>>;
}

/// No external anchor (the default).
pub struct NoAnchor;

impl AuditAnchor for NoAnchor {
    fn record(&self, _seq: u64, _hash: &str) -> io::Result<()> {
        Ok(())
    }

    fn latest(&self) -> io::Result<Option<(u64, String)>> {
        Ok(None)
    }
}

/// Whether a failed anchor write refuses the change when the policy does not
/// say (`anchor_required`). Product decision pending; until then it warns.
pub const ANCHOR_REQUIRED_DEFAULT: bool = false;

/// Whether [`OsLogAnchor::latest`] can read anchors back on this OS.
pub const OS_LOG_READABLE: bool = cfg!(windows);

/// The anchor the policy asks for, and whether its failure is fatal.
pub struct AnchorConfig {
    pub anchor: Box<dyn AuditAnchor>,
    pub required: bool,
}

/// The anchor for the audit log at `log_path` under the current policy.
pub fn configured(log_path: &Path) -> AnchorConfig {
    let policy = crate::policy::current();
    if !policy.is_active() {
        return AnchorConfig {
            anchor: Box::new(NoAnchor),
            required: ANCHOR_REQUIRED_DEFAULT,
        };
    }
    for_rules(&policy.rules, log_path)
}

fn for_rules(rules: &PolicyRules, log_path: &Path) -> AnchorConfig {
    let anchor: Box<dyn AuditAnchor> = match rules.audit_anchor {
        AuditAnchorRule::None => Box::new(NoAnchor),
        AuditAnchorRule::OsLog => Box::new(OsLogAnchor::new(log_path)),
    };
    AnchorConfig {
        anchor,
        required: rules.anchor_required || ANCHOR_REQUIRED_DEFAULT,
    }
}

/// What to do when [`AuditAnchor::record`] failed: an error when the anchor
/// is required, otherwise a warning on stderr.
pub fn on_record_error(e: io::Error, required: bool) -> io::Result<()> {
    if required {
        return Err(io::Error::other(format!(
            "the audit anchor could not be written (anchor_required): {e}"
        )));
    }
    tracing::warn!(%e, "Could not write the audit anchor");
    ferrika_core::notice::warn(&format!("could not write the audit anchor: {e}"));
    Ok(())
}

/// Start of every anchor message.
const MARKER: &str = "ferrika-audit-anchor";

/// One line: `ferrika-audit-anchor seq=<n> hash=<hex> log=<path>`. The log
/// path comes last (it may contain spaces) and tells apart the logs of
/// several accounts on one machine.
fn anchor_message(log: &str, seq: u64, hash: &str) -> String {
    format!("{MARKER} seq={seq} hash={hash} log={log}")
}

/// `(seq, hash)` of an anchor message for `log`.
fn parse_anchor_message(text: &str, log: &str) -> Option<(u64, String)> {
    let rest = text.trim().strip_prefix(MARKER)?.strip_prefix(" seq=")?;
    let (seq, rest) = rest.split_once(" hash=")?;
    let (hash, path) = rest.split_once(" log=")?;
    if path != log || hash.is_empty() {
        return None;
    }
    Some((seq.parse().ok()?, hash.to_string()))
}

/// Windows Event Log or syslog.
pub struct OsLogAnchor {
    log: String,
}

impl OsLogAnchor {
    pub fn new(log_path: &Path) -> Self {
        Self {
            log: log_path.display().to_string(),
        }
    }
}

impl AuditAnchor for OsLogAnchor {
    fn record(&self, seq: u64, hash: &str) -> io::Result<()> {
        os::write(&anchor_message(&self.log, seq, hash))
    }

    fn latest(&self) -> io::Result<Option<(u64, String)>> {
        os::find_newest(|text| parse_anchor_message(text, &self.log))
    }
}

/// How the log compares with the anchor's newest record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum AnchorCheck {
    /// The anchor has no record for this log (or cannot be read back).
    Unavailable,
    /// The log contains the anchored line; `log_seq` may be larger when
    /// later anchor writes failed.
    Match { seq: u64, log_seq: u64 },
    /// The log ends before the anchored line.
    Truncated { anchor_seq: u64, log_seq: u64 },
    /// The line at the anchored `seq` has another hash (the log was replaced
    /// or rewritten).
    Rewritten {
        seq: u64,
        anchor_hash: String,
        log_hash: Option<String>,
    },
}

impl AnchorCheck {
    /// Truncated or rewritten.
    pub fn is_failure(&self) -> bool {
        matches!(self, Self::Truncated { .. } | Self::Rewritten { .. })
    }
}

/// Compares the audit log at `path` with an anchor's newest `(seq, hash)`.
/// Chained lines (lines with a `hash`) are counted from 1; that count is
/// `seq` (see `audit::verify_chain`).
pub fn compare(path: &Path, latest: Option<(u64, String)>) -> io::Result<AnchorCheck> {
    let Some((anchor_seq, anchor_hash)) = latest else {
        return Ok(AnchorCheck::Unavailable);
    };
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(e) if e.kind() == io::ErrorKind::NotFound => String::new(),
        Err(e) => return Err(e),
    };
    let mut log_seq = 0u64;
    let mut at_anchor: Option<String> = None;
    for line in text.lines() {
        let Ok(value) = serde_json::from_str::<serde_json::Value>(line) else {
            continue;
        };
        let Some(hash) = value.get("hash").and_then(|h| h.as_str()) else {
            continue;
        };
        log_seq += 1;
        if log_seq == anchor_seq {
            at_anchor = Some(hash.to_string());
        }
    }
    if log_seq < anchor_seq {
        return Ok(AnchorCheck::Truncated {
            anchor_seq,
            log_seq,
        });
    }
    if at_anchor.as_deref() != Some(anchor_hash.as_str()) {
        return Ok(AnchorCheck::Rewritten {
            seq: anchor_seq,
            anchor_hash,
            log_hash: at_anchor,
        });
    }
    Ok(AnchorCheck::Match {
        seq: anchor_seq,
        log_seq,
    })
}

#[cfg(windows)]
mod os {
    //! Application log, source `Ferrika`, event 1000. Writing needs no
    //! administrator rights; without the registered source (see
    //! `docs/POLICY.md`) Event Viewer shows "description not found" but the
    //! message is stored and read back all the same.

    use std::io;

    use windows_sys::Win32::System::EventLog::{
        CloseEventLog, DeregisterEventSource, OpenEventLogW, ReadEventLogW, RegisterEventSourceW,
        ReportEventW, EVENTLOG_INFORMATION_TYPE, EVENTLOG_SEQUENTIAL_READ,
    };

    const SOURCE: &str = "Ferrika";
    const EVENT_ID: u32 = 1000;
    /// Not exported by windows-sys under this name.
    const EVENTLOG_BACKWARDS_READ: u32 = 0x0008;
    const ERROR_HANDLE_EOF: i32 = 38;
    const ERROR_INSUFFICIENT_BUFFER: i32 = 122;
    /// `size_of::<EVENTLOGRECORD>()`; the source name follows it.
    const RECORD_HEADER: usize = 56;

    fn wide(s: &str) -> Vec<u16> {
        s.encode_utf16().chain(std::iter::once(0)).collect()
    }

    pub fn write(message: &str) -> io::Result<()> {
        let source = wide(SOURCE);
        // SAFETY: `source` is NUL-terminated and outlives the call.
        let handle = unsafe { RegisterEventSourceW(std::ptr::null(), source.as_ptr()) };
        if handle.is_null() {
            return Err(io::Error::last_os_error());
        }
        let text = wide(message);
        let strings = [text.as_ptr()];
        // SAFETY: `handle` is open; `strings` holds one NUL-terminated string
        // that outlives the call; no SID and no raw data.
        let ok = unsafe {
            ReportEventW(
                handle,
                EVENTLOG_INFORMATION_TYPE,
                0,
                EVENT_ID,
                std::ptr::null_mut(),
                1,
                0,
                strings.as_ptr(),
                std::ptr::null(),
            )
        };
        let result = if ok == 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        };
        // SAFETY: `handle` came from RegisterEventSourceW and is closed once.
        unsafe { DeregisterEventSource(handle) };
        result
    }

    /// Reads the Application log newest first and returns the first value
    /// `pick` finds in a Ferrika anchor event's message.
    pub fn find_newest<T>(mut pick: impl FnMut(&str) -> Option<T>) -> io::Result<Option<T>> {
        let log = wide("Application");
        // SAFETY: `log` is NUL-terminated and outlives the call.
        let handle = unsafe { OpenEventLogW(std::ptr::null(), log.as_ptr()) };
        if handle.is_null() {
            return Err(io::Error::last_os_error());
        }
        let mut buf = vec![0u8; 64 * 1024];
        let result = loop {
            let (mut read, mut needed) = (0u32, 0u32);
            // SAFETY: `buf` is writable for `buf.len()` bytes; the out
            // pointers are valid.
            let ok = unsafe {
                ReadEventLogW(
                    handle,
                    EVENTLOG_SEQUENTIAL_READ | EVENTLOG_BACKWARDS_READ,
                    0,
                    buf.as_mut_ptr().cast(),
                    buf.len() as u32,
                    &mut read,
                    &mut needed,
                )
            };
            if ok == 0 {
                let e = io::Error::last_os_error();
                match e.raw_os_error() {
                    Some(ERROR_HANDLE_EOF) => break Ok(None),
                    Some(ERROR_INSUFFICIENT_BUFFER) if needed as usize > buf.len() => {
                        buf.resize(needed as usize, 0);
                        continue;
                    }
                    _ => break Err(e),
                }
            }
            let filled = &buf[..(read as usize).min(buf.len())];
            if let Some(found) = records(filled)
                .filter_map(anchor_text)
                .find_map(|text| pick(&text))
            {
                break Ok(Some(found));
            }
        };
        // SAFETY: `handle` came from OpenEventLogW and is closed once.
        unsafe { CloseEventLog(handle) };
        result
    }

    /// The EVENTLOGRECORDs packed in `buf`.
    fn records(mut buf: &[u8]) -> impl Iterator<Item = &[u8]> {
        std::iter::from_fn(move || {
            let len = u32_at(buf, 0)? as usize;
            if len < RECORD_HEADER || len > buf.len() {
                return None;
            }
            let (record, rest) = buf.split_at(len);
            buf = rest;
            Some(record)
        })
    }

    /// The first insertion string of a Ferrika anchor event.
    fn anchor_text(record: &[u8]) -> Option<String> {
        if u32_at(record, 20)? & 0xFFFF != EVENT_ID {
            return None;
        }
        if utf16z_at(record, RECORD_HEADER)? != SOURCE {
            return None;
        }
        if u16_at(record, 26)? == 0 {
            return None;
        }
        utf16z_at(record, u32_at(record, 36)? as usize)
    }

    fn u32_at(b: &[u8], at: usize) -> Option<u32> {
        Some(u32::from_le_bytes(b.get(at..at + 4)?.try_into().ok()?))
    }

    fn u16_at(b: &[u8], at: usize) -> Option<u16> {
        Some(u16::from_le_bytes(b.get(at..at + 2)?.try_into().ok()?))
    }

    /// A NUL-terminated UTF-16 string starting at byte `at`.
    fn utf16z_at(b: &[u8], at: usize) -> Option<String> {
        let units: Vec<u16> = b
            .get(at..)?
            .as_chunks::<2>()
            .0
            .iter()
            .map(|c| u16::from_le_bytes(*c))
            .take_while(|u| *u != 0)
            .collect();
        String::from_utf16(&units).ok()
    }
}

#[cfg(unix)]
mod os {
    //! syslog (facility auth, ident `ferrika`). Reading it back is not
    //! supported: journald and rsyslog files differ per distribution.

    use std::io;

    pub fn write(message: &str) -> io::Result<()> {
        let text = std::ffi::CString::new(message).map_err(io::Error::other)?;
        // SAFETY: the ident is a static C string (openlog keeps the pointer);
        // the format is a constant "%s" and `text` is NUL-terminated.
        unsafe {
            libc::openlog(c"ferrika".as_ptr(), libc::LOG_PID, libc::LOG_AUTH);
            libc::syslog(libc::LOG_NOTICE, c"%s".as_ptr(), text.as_ptr());
        }
        Ok(())
    }

    pub fn find_newest<T>(_pick: impl FnMut(&str) -> Option<T>) -> io::Result<Option<T>> {
        Ok(None)
    }
}

#[cfg(not(any(windows, unix)))]
mod os {
    use std::io;

    pub fn write(_message: &str) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "no OS log on this platform",
        ))
    }

    pub fn find_newest<T>(_pick: impl FnMut(&str) -> Option<T>) -> io::Result<Option<T>> {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::path::PathBuf;

    use super::*;
    use crate::audit::{append_with, verify_chain, AuditEntry};

    /// Keeps anchors in memory; can be told to fail.
    #[derive(Default)]
    struct MemoryAnchor {
        records: RefCell<Vec<(u64, String)>>,
        fail: bool,
    }

    impl AuditAnchor for MemoryAnchor {
        fn record(&self, seq: u64, hash: &str) -> io::Result<()> {
            if self.fail {
                return Err(io::Error::other("anchor offline"));
            }
            self.records.borrow_mut().push((seq, hash.to_string()));
            Ok(())
        }

        fn latest(&self) -> io::Result<Option<(u64, String)>> {
            Ok(self.records.borrow().last().cloned())
        }
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

    fn temp_log(name: &str) -> (PathBuf, PathBuf) {
        let dir = std::env::temp_dir().join(format!(
            "ferrika-anchor-{name}-{}-{}",
            std::process::id(),
            ferrika_core::kafka::clock::now_ms()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("audit.log");
        (dir, path)
    }

    fn sha256_hex(text: &str) -> String {
        let digest = ring::digest::digest(&ring::digest::SHA256, text.as_bytes());
        ferrika_core::model::format::to_hex(digest.as_ref())
    }

    /// A chained line as written before `seq` existed.
    fn pre_seq_line(i: i64, prev: &str) -> (String, String) {
        let serde_json::Value::Object(mut map) = serde_json::to_value(entry(i)).unwrap() else {
            unreachable!()
        };
        map.insert("prev".into(), prev.into());
        let hash = sha256_hex(&serde_json::Value::Object(map.clone()).to_string());
        map.insert("hash".into(), hash.clone().into());
        (serde_json::Value::Object(map).to_string(), hash)
    }

    fn lines(path: &Path) -> Vec<String> {
        std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(str::to_string)
            .collect()
    }

    #[test]
    fn seq_counts_up_and_every_append_is_anchored() {
        let (dir, path) = temp_log("seq");
        let anchor = MemoryAnchor::default();
        for i in 1..=4 {
            assert_eq!(
                append_with(&path, &entry(i), &anchor, false).unwrap(),
                i as u64
            );
        }
        let seqs: Vec<u64> = lines(&path)
            .iter()
            .map(|l| {
                serde_json::from_str::<serde_json::Value>(l).unwrap()["seq"]
                    .as_u64()
                    .unwrap()
            })
            .collect();
        assert_eq!(seqs, [1, 2, 3, 4]);
        let report = verify_chain(&path).unwrap();
        assert_eq!(report.broken, None);
        assert_eq!(anchor.records.borrow().len(), 4);
        let (seq, hash) = anchor.latest().unwrap().unwrap();
        assert_eq!(seq, 4);
        assert_eq!(report.last_hash.as_deref(), Some(hash.as_str()));
        assert_eq!(
            compare(&path, anchor.latest().unwrap()).unwrap(),
            AnchorCheck::Match { seq: 4, log_seq: 4 }
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn seq_continues_after_older_chained_lines_and_torn_fragments() {
        let (dir, path) = temp_log("upgrade");
        // A legacy line, then two chained lines from a version without seq.
        let legacy = serde_json::to_string(&entry(0)).unwrap();
        let (l1, h1) = pre_seq_line(1, &sha256_hex(&legacy));
        let (l2, _) = pre_seq_line(2, &h1);
        std::fs::write(&path, format!("{legacy}\n{l1}\n{l2}\n")).unwrap();
        let anchor = MemoryAnchor::default();
        assert_eq!(append_with(&path, &entry(3), &anchor, false).unwrap(), 3);
        assert_eq!(append_with(&path, &entry(4), &anchor, false).unwrap(), 4);

        // A crash leaves a fragment; the next line still gets the next seq.
        let mut text = std::fs::read_to_string(&path).unwrap();
        text.push_str(r#"{"time_ms":5,"sou"#);
        std::fs::write(&path, text).unwrap();
        assert_eq!(append_with(&path, &entry(6), &anchor, false).unwrap(), 5);

        let report = verify_chain(&path).unwrap();
        assert_eq!(report.broken, None);
        assert_eq!(report.legacy, 1);
        assert_eq!(report.chained, 5);
        assert_eq!(report.torn, vec![6]);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn verify_reports_a_seq_gap() {
        let (dir, path) = temp_log("gap");
        let anchor = NoAnchor;
        for i in 1..=2 {
            append_with(&path, &entry(i), &anchor, false).unwrap();
        }
        let first = lines(&path).remove(0);
        // A line with a valid hash and prev but the wrong seq.
        let first_hash = serde_json::from_str::<serde_json::Value>(&first).unwrap()["hash"]
            .as_str()
            .unwrap()
            .to_string();
        let serde_json::Value::Object(mut map) = serde_json::to_value(entry(2)).unwrap() else {
            unreachable!()
        };
        map.insert("seq".into(), 5.into());
        map.insert("prev".into(), first_hash.into());
        let hash = sha256_hex(&serde_json::Value::Object(map.clone()).to_string());
        map.insert("hash".into(), hash.into());
        std::fs::write(
            &path,
            format!("{first}\n{}\n", serde_json::Value::Object(map)),
        )
        .unwrap();
        let report = verify_chain(&path).unwrap();
        let (line, why) = report.broken.unwrap();
        assert_eq!(line, 2);
        assert!(why.contains("expected 2"), "{why}");
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn truncation_is_caught_by_the_anchor_but_not_the_chain() {
        let (dir, path) = temp_log("truncate");
        let anchor = MemoryAnchor::default();
        for i in 1..=5 {
            append_with(&path, &entry(i), &anchor, false).unwrap();
        }
        let kept = lines(&path)[..3].join("\n") + "\n";
        std::fs::write(&path, kept).unwrap();
        // The shorter chain is still valid on its own...
        assert_eq!(verify_chain(&path).unwrap().broken, None);
        // ...but the anchor knows seq 5 existed.
        let check = compare(&path, anchor.latest().unwrap()).unwrap();
        assert_eq!(
            check,
            AnchorCheck::Truncated {
                anchor_seq: 5,
                log_seq: 3
            }
        );
        assert!(check.is_failure());

        // Deleting the whole file is a truncation to nothing.
        std::fs::remove_file(&path).unwrap();
        assert!(compare(&path, anchor.latest().unwrap())
            .unwrap()
            .is_failure());
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn a_rewritten_log_is_caught_by_the_anchor() {
        let (dir, path) = temp_log("rewrite");
        let anchor = MemoryAnchor::default();
        for i in 1..=3 {
            append_with(&path, &entry(i), &anchor, false).unwrap();
        }
        // Replace the log with a fresh, valid chain of the same length.
        std::fs::remove_file(&path).unwrap();
        for i in 11..=13 {
            append_with(&path, &entry(i), &NoAnchor, false).unwrap();
        }
        assert_eq!(verify_chain(&path).unwrap().broken, None);
        let check = compare(&path, anchor.latest().unwrap()).unwrap();
        assert!(
            matches!(check, AnchorCheck::Rewritten { seq: 3, .. }),
            "{check:?}"
        );
        assert!(check.is_failure());
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn a_log_ahead_of_its_anchor_matches_and_no_anchor_is_unavailable() {
        let (dir, path) = temp_log("ahead");
        let anchor = MemoryAnchor::default();
        for i in 1..=2 {
            append_with(&path, &entry(i), &anchor, false).unwrap();
        }
        append_with(&path, &entry(3), &NoAnchor, false).unwrap();
        assert_eq!(
            compare(&path, anchor.latest().unwrap()).unwrap(),
            AnchorCheck::Match { seq: 2, log_seq: 3 }
        );
        assert_eq!(compare(&path, None).unwrap(), AnchorCheck::Unavailable);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn anchor_failure_warns_unless_required() {
        let (dir, path) = temp_log("fail");
        let broken = MemoryAnchor {
            fail: true,
            ..MemoryAnchor::default()
        };
        // Not required: the append succeeds.
        assert_eq!(append_with(&path, &entry(1), &broken, false).unwrap(), 1);
        // Required: the caller gets an error (a required-audit change is
        // refused); the line itself is already in the log.
        let err = append_with(&path, &entry(2), &broken, true).unwrap_err();
        assert!(err.to_string().contains("anchor_required"), "{err}");
        let report = verify_chain(&path).unwrap();
        assert_eq!(report.broken, None);
        assert_eq!(report.chained, 2);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn policy_rules_pick_the_anchor() {
        let path = Path::new("audit.log");
        let none = for_rules(&PolicyRules::default(), path);
        assert!(!none.required);
        assert_eq!(none.anchor.latest().unwrap(), None);
        let os = for_rules(
            &PolicyRules {
                audit_anchor: AuditAnchorRule::OsLog,
                anchor_required: true,
                ..PolicyRules::default()
            },
            path,
        );
        assert!(os.required);
        const { assert!(!ANCHOR_REQUIRED_DEFAULT) };
    }

    #[test]
    fn anchor_messages_round_trip_per_log() {
        let log = r"C:\Users\some one\.ferrika\audit.log";
        let text = anchor_message(log, 42, "abc123");
        assert_eq!(
            parse_anchor_message(&text, log),
            Some((42, "abc123".to_string()))
        );
        assert_eq!(parse_anchor_message(&text, r"C:\other\audit.log"), None);
        assert_eq!(parse_anchor_message("something else", log), None);
        assert_eq!(
            parse_anchor_message("ferrika-audit-anchor seq=x hash=a log=l", "l"),
            None
        );
    }

    /// Writes to the real Application log (Windows) and reads it back. Run by
    /// hand: `cargo test os_log_anchor_round_trip -- --ignored`. Works without
    /// the registered event source; see docs/POLICY.md for registering it.
    #[test]
    #[ignore = "writes to the machine's Event Log / syslog"]
    fn os_log_anchor_round_trip() {
        let (dir, path) = temp_log("oslog");
        let anchor = OsLogAnchor::new(&path);
        let hash = sha256_hex(&path.display().to_string());
        anchor.record(7, &hash).unwrap();
        anchor.record(8, &hash).unwrap();
        if OS_LOG_READABLE {
            assert_eq!(anchor.latest().unwrap(), Some((8, hash)));
        } else {
            assert_eq!(anchor.latest().unwrap(), None);
        }
        let _ = std::fs::remove_dir_all(dir);
    }
}
