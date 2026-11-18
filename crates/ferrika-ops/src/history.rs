//! The operation journal, `history.jsonl` next to the audit log: one JSON
//! event per line (see [`ferrika_core::model::history`]), appended under a lock
//! file like the audit log, and trimmed to the last [`KEEP_OPERATIONS`]
//! operations and [`KEEP_DAYS`] days.
//!
//! The journal holds before-states (config values, ACL lists, connector
//! configs), so it is not part of the audit log, which is forwarded to a
//! SIEM and never records values. The two are tied together by the audit
//! entry's `op` and `op_digest` (see [`ferrika_governance::audit::AuditEntry`]).

use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};

use ferrika_core::model::history::{
    fold, Begin, End, EventBody, HistoryEvent, Operation, Reversibility, State, FORMAT_VERSION,
};

pub mod apply;
pub mod capture;
pub mod conflict;
pub mod inverse;
pub mod undo;

/// File name of the journal.
pub const HISTORY_FILE: &str = "history.jsonl";

/// Operations kept when the journal is trimmed.
pub const KEEP_OPERATIONS: usize = 1000;

/// Days of operations kept when the journal is trimmed.
pub const KEEP_DAYS: i64 = 90;

/// The journal is trimmed when an append finds it larger than this.
const TRIM_ABOVE_BYTES: u64 = 1024 * 1024;

/// Where connector secrets of an operation are kept in the OS credential
/// store (`ferrika-history/<op id>`, one entry per config key).
pub fn secret_store_profile(op: &str) -> String {
    format!("ferrika-history/{op}")
}

/// The journal in the audit log's folder.
pub fn journal_path() -> PathBuf {
    ferrika_governance::audit::audit_path().with_file_name(HISTORY_FILE)
}

/// The journal file and its lock.
#[derive(Debug, Clone)]
pub struct Journal {
    path: PathBuf,
}

impl Default for Journal {
    fn default() -> Self {
        Self::at(journal_path())
    }
}

impl Journal {
    pub fn at(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Appends one event. Holds `history.jsonl.lock` while writing so two
    /// processes never interleave lines, and trims the journal when it has
    /// grown past 1 MiB.
    pub fn append(&self, event: &HistoryEvent) -> std::io::Result<()> {
        let line = serde_json::to_string(event).map_err(std::io::Error::other)?;
        if let Some(dir) = self.path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let lock = private_options()
            .read(true)
            .write(true)
            .truncate(false)
            .open(lock_path(&self.path))?;
        lock.lock()?;
        let mut file = private_options().append(true).open(&self.path)?;
        // A line cut short by a crash has no newline; start on a fresh one.
        let len = file.metadata()?.len();
        let mut out = String::with_capacity(line.len() + 2);
        if len > 0 && !ends_with_newline(&self.path, len) {
            out.push('\n');
        }
        out.push_str(&line);
        out.push('\n');
        file.write_all(out.as_bytes())?;
        file.flush()?;
        drop(file);
        if len + out.len() as u64 > TRIM_ABOVE_BYTES {
            if let Err(e) = self.trim_locked(ferrika_core::kafka::clock::now_ms()) {
                tracing::warn!(%e, "Could not trim the operation history");
            }
        }
        Ok(())
    }

    /// Every operation in the journal, oldest first. Lines that are not
    /// events (cut short, edited) are skipped.
    pub fn load(&self) -> Vec<Operation> {
        fold(self.events())
    }

    fn events(&self) -> Vec<HistoryEvent> {
        let Ok(file) = std::fs::File::open(&self.path) else {
            return Vec::new();
        };
        std::io::BufReader::new(file)
            .lines()
            .map_while(Result::ok)
            .filter_map(|line| serde_json::from_str(line.trim_start_matches('\u{feff}')).ok())
            .collect()
    }

    /// Keeps the last [`KEEP_OPERATIONS`] operations of the last
    /// [`KEEP_DAYS`] days (an operation's lines stay together), rewriting
    /// the file through a temporary file. The caller holds the lock.
    /// Connector secrets of dropped operations leave the credential store.
    fn trim_locked(&self, now_ms: i64) -> std::io::Result<()> {
        let ops = self.load();
        let cutoff = now_ms - KEEP_DAYS * 24 * 60 * 60 * 1000;
        let keep_from = ops.len().saturating_sub(KEEP_OPERATIONS);
        let mut dropped = Vec::new();
        let mut kept_ids = std::collections::HashSet::new();
        for (ix, op) in ops.iter().enumerate() {
            if ix < keep_from || op.time_ms < cutoff {
                dropped.push(op);
            } else {
                kept_ids.insert(op.id.as_str());
            }
        }
        if dropped.is_empty() {
            return Ok(());
        }
        let lines: Vec<String> = self
            .events()
            .into_iter()
            .filter(|event| kept_ids.contains(event.id.as_str()))
            .filter_map(|event| serde_json::to_string(&event).ok())
            .collect();
        let tmp = self.path.with_extension("jsonl.tmp");
        {
            let mut file = private_options().write(true).truncate(true).open(&tmp)?;
            for line in &lines {
                file.write_all(line.as_bytes())?;
                file.write_all(b"\n")?;
            }
            file.flush()?;
        }
        std::fs::rename(&tmp, &self.path)?;
        for op in dropped {
            forget_secrets(op);
        }
        Ok(())
    }
}

/// Removes the connector secrets an operation kept in the credential store.
pub fn forget_secrets(op: &Operation) {
    let profile = secret_store_profile(&op.id);
    for state in op.begin.before.iter().chain(op.after()) {
        if let State::Connector { secret_keys, .. } = state {
            for key in secret_keys {
                let _ = ferrika_core::kafka::secrets::delete(&profile, key);
            }
        }
    }
}

/// What a change does to the history: the states it alters and whether
/// undo can reverse it.
#[derive(Debug, Clone)]
pub struct Change {
    pub reversibility: Reversibility,
    pub limits: Vec<String>,
    pub before: Vec<State>,
    pub after: Vec<State>,
    pub info: Vec<String>,
}

impl Change {
    pub fn new(reversibility: Reversibility, before: Vec<State>, after: Vec<State>) -> Self {
        Self {
            reversibility,
            limits: Vec::new(),
            before,
            after,
            info: Vec::new(),
        }
    }

    /// A change recorded only for the record (nothing to restore).
    pub fn irreversible(reversibility: Reversibility, limit: impl Into<String>) -> Self {
        Self {
            limits: vec![limit.into()],
            ..Self::new(reversibility, Vec::new(), Vec::new())
        }
    }

    /// The change from a before-state read that may have failed: a failed
    /// read is recorded as not undoable, with the reason (the change still
    /// runs).
    pub fn captured(
        reversibility: Reversibility,
        captured: ferrika_core::error::KafkaResult<(Vec<State>, Vec<State>)>,
    ) -> Self {
        match captured {
            Ok((before, after)) => Self::new(reversibility, before, after),
            Err(e) => Self::irreversible(
                Reversibility::Irreversible,
                format!("before-state could not be read: {e}"),
            ),
        }
    }

    pub fn limit(mut self, limit: impl Into<String>) -> Self {
        self.limits.push(limit.into());
        self
    }

    pub fn info(mut self, line: impl Into<String>) -> Self {
        self.info.push(line.into());
        self
    }

    /// Marks a config change partially reversible when its keys include
    /// sensitive ones (their values were never readable).
    pub fn sensitive_limits(self) -> Self {
        let keys: Vec<String> = self
            .before
            .iter()
            .flat_map(capture::sensitive_keys)
            .collect();
        if keys.is_empty() || self.reversibility != Reversibility::Reversible {
            return self;
        }
        Self {
            reversibility: Reversibility::Partial,
            ..self
        }
        .limit(format!(
            "sensitive values are not restored: {}",
            keys.join(", ")
        ))
    }

    /// Creating `topic`: undone by deleting it while it is still unused.
    pub fn topic_created(topic: &str) -> Self {
        let state = |exists| State::Topic {
            topic: topic.to_string(),
            exists,
        };
        Self::new(
            Reversibility::Conditional,
            vec![state(false)],
            vec![state(true)],
        )
        .limit("undo deletes the topic, only while it has no records and no committed offsets")
    }

    /// An offset reset of `group`: each partition's committed offset before
    /// (`-1` = none) and after.
    pub fn offset_reset(
        group: &str,
        plans: &[ferrika_core::kafka::offset_reset::OffsetResetPlan],
    ) -> Self {
        use ferrika_core::model::history::PartitionOffset;
        type Pick = fn(&ferrika_core::kafka::offset_reset::OffsetResetPlan) -> Option<i64>;
        let state = |offset: Pick| State::GroupOffsets {
            group: group.to_string(),
            offsets: plans
                .iter()
                .map(|p| PartitionOffset {
                    topic: p.topic.clone(),
                    partition: p.partition,
                    offset: offset(p),
                })
                .collect(),
        };
        Self::new(
            Reversibility::Reversible,
            vec![state(|p| {
                (p.current_offset >= 0).then_some(p.current_offset)
            })],
            vec![state(|p| Some(p.target_offset))],
        )
    }

    /// The `begin` line of the change.
    pub fn into_begin(
        self,
        source: &str,
        scope: ferrika_core::model::history::Scope,
        action: String,
        target: String,
        undoes: Option<String>,
    ) -> Begin {
        Begin {
            source: source.to_string(),
            user: ferrika_governance::audit::os_user(),
            scope,
            action,
            target,
            reversibility: self.reversibility,
            limits: self.limits,
            before: self.before,
            after: self.after,
            info: self.info,
            undoes,
        }
    }
}

/// Records one operation: the `begin` line before the change runs, the
/// `end` line after. Writing the journal never stops the change; a failure
/// is logged and the operation is simply not undoable.
#[derive(Debug)]
pub struct Recorder {
    journal: Journal,
    id: String,
    before: Vec<State>,
    after: Vec<State>,
    started: bool,
    reversibility: Reversibility,
}

impl Recorder {
    /// Writes the `begin` line of a new operation. Connector secrets in
    /// `begin` move to the OS credential store first (never into the
    /// journal); when that store is unavailable they are dropped and the
    /// operation becomes partially reversible.
    pub fn start(journal: Journal, begin: Begin) -> Self {
        let id = ferrika_core::model::history::new_op_id(ferrika_core::kafka::clock::now_ms());
        Self::start_with_id(journal, id, begin)
    }

    /// [`Recorder::start`] under an id made before the change was
    /// authorized, so the audit request entry could name it.
    pub fn start_with_id(journal: Journal, id: String, begin: Begin) -> Self {
        Self::start_with(journal, id, begin, &|profile, key, value| {
            ferrika_core::kafka::secrets::enabled()
                && ferrika_core::kafka::secrets::set(profile, key, value).is_ok()
        })
    }

    /// [`Recorder::start_with_id`] with `store(profile, key, value)`
    /// keeping connector secrets (true when kept).
    fn start_with(journal: Journal, id: String, mut begin: Begin, store: &SecretStore<'_>) -> Self {
        let time_ms = ferrika_core::kafka::clock::now_ms();
        if let Some(lost) = keep_secrets(&id, &mut begin.before, store) {
            begin.limits.push(lost);
            if begin.reversibility == Reversibility::Reversible {
                begin.reversibility = Reversibility::Partial;
            }
        }
        for state in &mut begin.after {
            blank_secrets(state);
        }
        let (before, after) = (begin.before.clone(), begin.after.clone());
        let reversibility = begin.reversibility;
        let event = HistoryEvent {
            v: FORMAT_VERSION,
            id: id.clone(),
            time_ms,
            body: EventBody::Begin(Box::new(begin)),
        };
        let started = match journal.append(&event) {
            Ok(()) => true,
            Err(e) => {
                tracing::warn!(%e, "Could not write the operation history");
                false
            }
        };
        Self {
            journal,
            id,
            before,
            after,
            started,
            reversibility,
        }
    }

    /// How far undo can reverse the operation.
    pub fn reversibility(&self) -> Reversibility {
        self.reversibility
    }

    /// The operation id.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Whether the `begin` line was written.
    pub fn started(&self) -> bool {
        self.started
    }

    /// Writes the `end` line; `after` replaces the recorded after-state
    /// when it was only known once the change ran. Returns the digest the
    /// audit entry records as `op_digest`, or `None` when the operation is
    /// not in the journal.
    pub fn finish(&self, ok: bool, outcome: &str, after: Option<Vec<State>>) -> Option<String> {
        if !self.started {
            return None;
        }
        let after = after.map(|mut states| {
            states.iter_mut().for_each(blank_secrets);
            states
        });
        let digest = ferrika_core::model::history::state_digest(
            &self.before,
            after.as_deref().unwrap_or(&self.after),
        );
        let event = HistoryEvent {
            v: FORMAT_VERSION,
            id: self.id.clone(),
            time_ms: ferrika_core::kafka::clock::now_ms(),
            body: EventBody::End(End {
                ok,
                outcome: outcome.to_string(),
                after,
            }),
        };
        match self.journal.append(&event) {
            Ok(()) => Some(digest),
            Err(e) => {
                tracing::warn!(%e, "Could not write the operation history");
                None
            }
        }
    }
}

/// Moves the secret values of connector states into the credential store
/// under `op` and blanks them. Returns what was lost when the store could
/// not take them.
type SecretStore<'a> = dyn Fn(&str, &str, &str) -> bool + 'a;

fn keep_secrets(op: &str, states: &mut [State], store: &SecretStore<'_>) -> Option<String> {
    let profile = secret_store_profile(op);
    let mut lost = Vec::new();
    for state in states.iter_mut() {
        if let State::Connector {
            name,
            config: Some(config),
            secret_keys,
            ..
        } = state
        {
            for key in secret_keys.iter() {
                let Some(value) = config.get_mut(key) else {
                    continue;
                };
                if !store(&profile, key, value) {
                    lost.push(format!("{name}: {key}"));
                }
                value.clear();
            }
        }
    }
    (!lost.is_empty()).then(|| {
        format!(
            "connector secrets were not kept (no OS credential store): {}",
            lost.join(", ")
        )
    })
}

/// Blanks the secret values of a connector state.
fn blank_secrets(state: &mut State) {
    if let State::Connector {
        config: Some(config),
        secret_keys,
        ..
    } = state
    {
        for key in secret_keys.iter() {
            if let Some(value) = config.get_mut(key) {
                value.clear();
            }
        }
    }
}

/// Create-or-open options; new files are readable by the owner only on Unix
/// (the journal holds config values).
fn private_options() -> std::fs::OpenOptions {
    let mut options = std::fs::OpenOptions::new();
    options.create(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options
}

fn ends_with_newline(path: &Path, len: u64) -> bool {
    use std::io::{Read, Seek, SeekFrom};
    let Ok(mut file) = std::fs::File::open(path) else {
        return true;
    };
    let mut last = [0u8; 1];
    let read = file.seek(SeekFrom::Start(len - 1)).is_ok() && file.read_exact(&mut last).is_ok();
    !read || last[0] == b'\n'
}

/// `history.jsonl.lock` next to the journal.
fn lock_path(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(".lock");
    PathBuf::from(name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrika_core::model::history::{
        Begin, ConfigValue, End, EventBody, Reversibility, Scope, FORMAT_VERSION,
    };
    use std::collections::BTreeMap;

    fn temp_journal(name: &str) -> (PathBuf, Journal) {
        let dir = std::env::temp_dir().join(format!(
            "ferrika-history-{name}-{}-{}",
            std::process::id(),
            ferrika_core::kafka::clock::now_ms()
        ));
        let journal = Journal::at(dir.join(HISTORY_FILE));
        (dir, journal)
    }

    fn begin_event(id: &str, time_ms: i64) -> HistoryEvent {
        HistoryEvent {
            v: FORMAT_VERSION,
            id: id.into(),
            time_ms,
            body: EventBody::Begin(Box::new(Begin {
                source: "cli".into(),
                user: "tester".into(),
                scope: Scope::default(),
                action: "topics config".into(),
                target: "t".into(),
                reversibility: Reversibility::Reversible,
                limits: vec![],
                before: vec![State::TopicConfig {
                    topic: "t".into(),
                    keys: BTreeMap::from([(
                        "retention.ms".to_string(),
                        ConfigValue {
                            own: None,
                            sensitive: false,
                        },
                    )]),
                }],
                after: vec![],
                info: vec![],
                undoes: None,
            })),
        }
    }

    fn end_event(id: &str) -> HistoryEvent {
        HistoryEvent {
            v: FORMAT_VERSION,
            id: id.into(),
            time_ms: 0,
            body: EventBody::End(End {
                ok: true,
                outcome: "ok".into(),
                after: None,
            }),
        }
    }

    #[test]
    fn appended_events_fold_back_and_torn_lines_are_skipped() {
        let (dir, journal) = temp_journal("append");
        journal.append(&begin_event("A", 1)).unwrap();
        // A crash cut the next line short.
        let mut text = std::fs::read_to_string(journal.path()).unwrap();
        text.push_str(r#"{"v":1,"id":"A","ev":"en"#);
        std::fs::write(journal.path(), text).unwrap();
        journal.append(&end_event("A")).unwrap();
        journal.append(&begin_event("B", 2)).unwrap();
        let ops = journal.load();
        assert_eq!(ops.len(), 2);
        assert_eq!(ops[0].ok(), Some(true));
        assert_eq!(ops[1].ok(), None);
        let text = std::fs::read_to_string(journal.path()).unwrap();
        assert_eq!(text.lines().count(), 4, "{text}");
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn concurrent_appends_keep_every_line_whole() {
        let (dir, journal) = temp_journal("threads");
        let handles: Vec<_> = (0..4)
            .map(|t| {
                let journal = journal.clone();
                std::thread::spawn(move || {
                    for i in 0..25 {
                        journal
                            .append(&begin_event(&format!("T{t}-{i}"), i))
                            .unwrap();
                    }
                })
            })
            .collect();
        for handle in handles {
            handle.join().unwrap();
        }
        assert_eq!(journal.load().len(), 100);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn trimming_keeps_recent_operations_with_all_their_lines() {
        let (dir, journal) = temp_journal("trim");
        let day = 24 * 60 * 60 * 1000;
        let now = 200 * day;
        journal.append(&begin_event("OLD", now - 91 * day)).unwrap();
        journal.append(&end_event("OLD")).unwrap();
        for i in 0..(KEEP_OPERATIONS + 2) {
            journal
                .append(&begin_event(&format!("N{i:05}"), now - day))
                .unwrap();
        }
        journal.append(&end_event("N01001")).unwrap();
        journal.trim_locked(now).unwrap();
        let ops = journal.load();
        assert_eq!(ops.len(), KEEP_OPERATIONS);
        assert!(ops.iter().all(|op| op.id != "OLD"));
        assert_eq!(ops[0].id, "N00002");
        assert_eq!(
            ops.iter()
                .find(|op| op.id == "N01001")
                .and_then(|op| op.ok()),
            Some(true)
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    fn connector(secret: &str) -> State {
        State::Connector {
            name: "jdbc".into(),
            config: Some(BTreeMap::from([
                ("connector.class".to_string(), "X".to_string()),
                ("connection.password".to_string(), secret.to_string()),
            ])),
            paused: false,
            secret_keys: vec!["connection.password".into()],
        }
    }

    fn begin_with(before: Vec<State>, after: Vec<State>) -> Begin {
        match begin_event("X", 0).body {
            EventBody::Begin(begin) => Begin {
                action: "connect apply".into(),
                before,
                after,
                ..*begin
            },
            _ => unreachable!(),
        }
    }

    #[test]
    fn recorder_writes_begin_and_end_with_a_matching_digest() {
        let (dir, journal) = temp_journal("recorder");
        let kept = std::cell::RefCell::new(Vec::new());
        let recorder = Recorder::start_with(
            journal.clone(),
            "01JBX3K8Q2M7".to_string(),
            begin_with(vec![connector("hunter2")], vec![connector("s3cret")]),
            &|profile, key, value| {
                kept.borrow_mut()
                    .push((profile.to_string(), key.to_string(), value.to_string()));
                true
            },
        );
        let digest = recorder.finish(true, "Applied", None).unwrap();
        let ops = journal.load();
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].id, "01JBX3K8Q2M7");
        assert_eq!(recorder.id(), "01JBX3K8Q2M7");
        assert_eq!(ops[0].digest(), digest);
        assert_eq!(ops[0].ok(), Some(true));
        // The secret went to the store, not into the journal.
        assert_eq!(
            kept.borrow().as_slice(),
            &[(
                secret_store_profile(recorder.id()),
                "connection.password".to_string(),
                "hunter2".to_string()
            )]
        );
        let text = std::fs::read_to_string(journal.path()).unwrap();
        assert!(
            !text.contains("hunter2") && !text.contains("s3cret"),
            "{text}"
        );
        assert_eq!(ops[0].begin.reversibility, Reversibility::Reversible);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn secrets_without_a_store_make_the_operation_partial() {
        let (dir, journal) = temp_journal("nostore");
        let recorder = Recorder::start_with(
            journal.clone(),
            "01JBX3K8Q2M8".to_string(),
            begin_with(vec![connector("hunter2")], vec![]),
            &|_, _, _| false,
        );
        let op = &journal.load()[0];
        assert_eq!(op.begin.reversibility, Reversibility::Partial);
        assert!(op.begin.limits[0].contains("connection.password"));
        assert!(!std::fs::read_to_string(journal.path())
            .unwrap()
            .contains("hunter2"));
        // An unwritable journal does not fail the change.
        let blocked = Recorder::start_with(
            Journal::at(dir.join(HISTORY_FILE).join("not-a-dir").join("x")),
            "01JBX3K8Q2M9".to_string(),
            begin_with(vec![], vec![]),
            &|_, _, _| true,
        );
        assert!(!blocked.started());
        assert_eq!(blocked.finish(true, "", None), None);
        drop(recorder);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[cfg(unix)]
    #[test]
    fn journal_is_private_on_unix() {
        use std::os::unix::fs::PermissionsExt;
        let (dir, journal) = temp_journal("mode");
        journal.append(&begin_event("A", 1)).unwrap();
        let mode = std::fs::metadata(journal.path())
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
        let _ = std::fs::remove_dir_all(dir);
    }
}
