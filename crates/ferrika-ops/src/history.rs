//! The operation journal, `history.jsonl` next to the audit log: one JSON
//! event per line (see [`ferrika_core::model::history`]), appended under a lock
//! file like the audit log, and trimmed to the last [`KEEP_OPERATIONS`]
//! operations and [`KEEP_DAYS`] days.
//!
//! Size: an append only writes its line. Once the file passes
//! [`TRIM_ABOVE_BYTES`] it is trimmed (one full read); when that leaves it
//! above half the limit (the operations are all recent), it is rotated
//! instead: renamed to `history.jsonl.1`, replacing the previous one, whose
//! operations are dropped. [`Journal::load`] reads both files, so the
//! journal holds at most about twice the limit and a full read on append
//! happens at most once per half a limit of new lines.
//!
//! The journal holds before-states (config values, ACL lists, connector
//! configs), so it is not part of the audit log, which is forwarded to a
//! SIEM and never records values. The two are tied together by the audit
//! entry's `op` and `op_digest` (see [`ferrika_governance::audit::AuditEntry`]).

use std::io::{BufRead, Write};

use serde::Deserialize;
use std::path::{Path, PathBuf};

use ferrika_core::model::history::{
    fold, Begin, End, EventBody, HistoryEvent, Operation, Reversibility, State, FORMAT_VERSION,
};
use ferrika_governance::audit::AuditEntry;
use ferrika_governance::policy::AuditSink;

pub mod apply;
pub mod capture;
pub mod conflict;
pub mod inverse;
pub mod rabbit;
pub mod undo;

/// File name of the journal.
pub(crate) const HISTORY_FILE: &str = "history.jsonl";

/// Operations kept when the journal is trimmed.
pub const KEEP_OPERATIONS: usize = 1000;

/// Days of operations kept when the journal is trimmed.
pub const KEEP_DAYS: i64 = 90;

/// The journal is trimmed (or rotated) when an append finds it larger than
/// this.
pub const TRIM_ABOVE_BYTES: u64 = 1024 * 1024;

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

    /// The previous, rotated journal (`history.jsonl.1`).
    pub fn rotated_path(&self) -> PathBuf {
        let mut name = self.path.as_os_str().to_owned();
        name.push(".1");
        PathBuf::from(name)
    }

    /// Appends one event. Holds `history.jsonl.lock` while writing so two
    /// processes never interleave lines, and trims or rotates the journal
    /// when it has grown past [`TRIM_ABOVE_BYTES`] (see the module docs).
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
            if let Err(e) = self.shrink_locked(ferrika_core::kafka::clock::now_ms()) {
                tracing::warn!(%e, "Could not trim the operation history");
            }
        }
        Ok(())
    }

    /// Trims the journal, then rotates it when it is still above half of
    /// [`TRIM_ABOVE_BYTES`], so the next full read is at least that many
    /// bytes of appends away. The caller holds the lock.
    fn shrink_locked(&self, now_ms: i64) -> std::io::Result<()> {
        self.trim_locked(now_ms)?;
        if std::fs::metadata(&self.path)?.len() > TRIM_ABOVE_BYTES / 2 {
            self.rotate_locked()?;
        }
        Ok(())
    }

    /// Renames the journal to `history.jsonl.1`, replacing the previous
    /// one. Connector secrets of the operations that leave with it go too,
    /// unless the current file still names the operation. The caller holds
    /// the lock.
    fn rotate_locked(&self) -> std::io::Result<()> {
        let rotated = self.rotated_path();
        let current = read_events(&self.path);
        let leaving = fold(read_events(&rotated));
        std::fs::rename(&self.path, &rotated)?;
        let kept: std::collections::HashSet<&str> =
            current.iter().map(|event| event.id.as_str()).collect();
        for op in leaving.iter().filter(|op| !kept.contains(op.id.as_str())) {
            forget_secrets(op);
        }
        Ok(())
    }

    /// Every operation in the journal (the rotated file, then the current
    /// one), oldest first. Lines that are not events (cut short, edited)
    /// are skipped.
    pub fn load(&self) -> Vec<Operation> {
        let mut events = read_events(&self.rotated_path());
        events.extend(self.events());
        fold(events)
    }

    /// The events of the current file only.
    fn events(&self) -> Vec<HistoryEvent> {
        read_events(&self.path)
    }

    /// Keeps the last [`KEEP_OPERATIONS`] operations of the last
    /// [`KEEP_DAYS`] days (an operation's lines stay together), rewriting
    /// the file through a temporary file. The caller holds the lock.
    /// Connector secrets of dropped operations leave the credential store.
    /// Only the current file is trimmed; lines of operations that began in
    /// the rotated file are kept.
    ///
    /// Kept lines are copied byte for byte, never re-serialized: a journal
    /// shared with a newer Ferrika may hold event kinds and fields this
    /// version does not know, and digests cover what was written. Only the
    /// lines of dropped operations (by id, whatever their event kind) go,
    /// plus lines that are not JSON at all (cut short by a crash).
    fn trim_locked(&self, now_ms: i64) -> std::io::Result<()> {
        let lines = read_lines(&self.path);
        let ops = fold(lines.iter().filter_map(|line| line.event.clone()));
        let cutoff = now_ms - KEEP_DAYS * 24 * 60 * 60 * 1000;
        let keep_from = ops.len().saturating_sub(KEEP_OPERATIONS);
        let mut dropped = Vec::new();
        let mut dropped_ids = std::collections::HashSet::new();
        for (ix, op) in ops.iter().enumerate() {
            if ix < keep_from || op.time_ms < cutoff {
                dropped.push(op);
                dropped_ids.insert(op.id.as_str());
            }
        }
        if dropped.is_empty() {
            return Ok(());
        }
        let tmp = self.path.with_extension("jsonl.tmp");
        {
            let mut file = private_options().write(true).truncate(true).open(&tmp)?;
            for line in &lines {
                let keep = match &line.id {
                    Some(id) => !dropped_ids.contains(id.as_str()),
                    None => line.is_json,
                };
                if keep {
                    file.write_all(&line.raw)?;
                    file.write_all(b"\n")?;
                }
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

/// One line of a journal file as read, for writing it back unchanged.
struct RawLine {
    /// The bytes without the trailing newline.
    raw: Vec<u8>,
    /// The line as an event this version understands.
    event: Option<HistoryEvent>,
    /// The operation id of a JSON line that has one, known event or not.
    id: Option<String>,
    /// Whether the line is JSON at all (not cut short by a crash).
    is_json: bool,
}

/// Just the id of a journal line, whatever its event kind.
#[derive(serde::Deserialize)]
struct LineId {
    id: String,
}

/// The text of a line without a byte-order mark and `\r`; `None` when it
/// is not UTF-8.
fn line_text(bytes: &[u8]) -> Option<&str> {
    let line = std::str::from_utf8(bytes).ok()?;
    Some(line.trim_start_matches('\u{feff}').trim_end_matches('\r'))
}

/// The lines of a journal file, split on bytes: a line a crash cut inside
/// a multi-byte character is not UTF-8, and `lines()` would stop there,
/// losing every later operation (and the trim would then rewrite the file
/// without them). Nothing when the file cannot be opened.
fn split_lines(path: &Path) -> Vec<Vec<u8>> {
    let Ok(file) = std::fs::File::open(path) else {
        return Vec::new();
    };
    std::io::BufReader::new(file)
        .split(b'\n')
        .map_while(Result::ok)
        .collect()
}

/// Every non-blank line of a journal file with what it says.
fn read_lines(path: &Path) -> Vec<RawLine> {
    split_lines(path)
        .into_iter()
        .filter_map(|raw| {
            let text = line_text(&raw);
            if text.is_some_and(|t| t.trim().is_empty()) {
                return None;
            }
            let value = text.and_then(|t| serde_json::from_str::<serde_json::Value>(t).ok());
            let event = value
                .as_ref()
                .and_then(|v| HistoryEvent::deserialize(v).ok());
            let id = value
                .as_ref()
                .and_then(|v| LineId::deserialize(v).ok())
                .map(|line| line.id);
            Some(RawLine {
                raw,
                event,
                id,
                is_json: value.is_some(),
            })
        })
        .collect()
}

/// The events of one journal file; nothing when it cannot be opened.
/// Lines this version does not understand are skipped.
fn read_events(path: &Path) -> Vec<HistoryEvent> {
    split_lines(path)
        .iter()
        .filter_map(|bytes| serde_json::from_str(line_text(bytes)?).ok())
        .collect()
}

/// Removes the connector secrets an operation kept in the credential store
/// (also where versions before per-directory entries kept them: an
/// operation id belongs to one journal only).
pub fn forget_secrets(op: &Operation) {
    let profile = secret_store_profile(&op.id);
    for state in op.begin.before.iter().chain(op.after()) {
        if let State::Connector { secret_keys, .. } = state {
            for key in secret_keys {
                let _ = ferrika_core::kafka::secrets::delete_with_legacy(&profile, key);
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
    /// The `begin` line as written (secrets blanked), for the digest.
    begin: Begin,
    started: bool,
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
        let event = HistoryEvent {
            v: FORMAT_VERSION,
            id: id.clone(),
            time_ms,
            body: EventBody::Begin(Box::new(begin.clone())),
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
            begin,
            started,
        }
    }

    /// How far undo can reverse the operation.
    pub fn reversibility(&self) -> Reversibility {
        self.begin.reversibility
    }

    /// The operation id.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Whether the `begin` line was written.
    pub fn started(&self) -> bool {
        self.started
    }

    /// The before-state recorded in the `begin` line (secrets blanked).
    pub fn before(&self) -> &[State] {
        &self.begin.before
    }

    /// Writes the `end` line; `after` replaces the recorded after-state
    /// when it was only known once the change ran. Returns the digest the
    /// audit entry records as `op_digest` (version 2, see
    /// [`ferrika_core::model::history::operation_digest`]), or `None` when
    /// the operation is not in the journal.
    pub fn finish(&self, ok: bool, outcome: &str, after: Option<Vec<State>>) -> Option<String> {
        if !self.started {
            return None;
        }
        let after = after.map(|mut states| {
            states.iter_mut().for_each(blank_secrets);
            states
        });
        let digest = ferrika_core::model::history::operation_digest(
            &self.id,
            &self.begin,
            after.as_deref().unwrap_or(&self.begin.after),
            Some(ok),
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

/// The audit entry that closes a recorded change: the fields of the
/// change's audit request entry, which the result entry repeats with the
/// outcome, the operation id and its digest (`op`, `op_digest`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AuditTarget {
    /// Who made the change: `cli`, `gui`, `mcp`...
    pub source: String,
    /// The profile the request entry named (for a RabbitMQ change the
    /// RabbitMQ profile, else the broker addresses the gate matched).
    pub profile: String,
    /// The request entry's action, e.g. `rabbit queue delete`.
    pub action: String,
    /// What was changed, e.g. a queue name.
    pub target: String,
    /// An agent acting for the user (an MCP client), if any.
    pub actor: Option<String>,
}

/// One change recorded in the operation journal and closed by an audit
/// result entry tied to it: [`AuditedRecord::start`] writes the `begin`
/// line before the change runs, [`AuditedRecord::finish`] the `end` line
/// and the result entry carrying the operation id and the digest
/// (`op_digest`) that `undo` checks the journal against.
///
/// The request entry (written by the gate, e.g.
/// [`ferrika_governance::policy::authorize_write`]) should name the same
/// operation id: make it with
/// [`ferrika_core::model::history::new_op_id`] before the gate and pass it
/// as `op`.
#[derive(Debug)]
pub struct AuditedRecord {
    recorder: Recorder,
    audit: AuditTarget,
}

impl AuditedRecord {
    /// Writes the `begin` line of `change` (made in `scope`) to `journal`
    /// under `op` (a new id when `None`); `undoes` names the operation an
    /// undo reverses. A journal that cannot be written does not stop the
    /// change: it is logged and the change is not undoable.
    pub fn start(
        journal: Journal,
        op: Option<String>,
        scope: ferrika_core::model::history::Scope,
        change: Change,
        audit: AuditTarget,
        undoes: Option<String>,
    ) -> Self {
        let begin = change.into_begin(
            &audit.source,
            scope,
            audit.action.clone(),
            audit.target.clone(),
            undoes,
        );
        let recorder = match op {
            Some(id) => Recorder::start_with_id(journal, id, begin),
            None => Recorder::start(journal, begin),
        };
        Self { recorder, audit }
    }

    /// The operation id.
    pub fn id(&self) -> &str {
        self.recorder.id()
    }

    /// The journal record.
    pub fn recorder(&self) -> &Recorder {
        &self.recorder
    }

    /// The result entry for `outcome` with the journal's `digest`.
    pub fn result_entry(&self, ok: bool, outcome: &str, digest: String) -> AuditEntry {
        AuditEntry {
            time_ms: ferrika_governance::audit::now_ms(),
            source: self.audit.source.clone(),
            profile: self.audit.profile.clone(),
            action: self.audit.action.clone(),
            target: self.audit.target.clone(),
            outcome: outcome.to_string(),
            ok,
            user: String::new(),
            actor: self.audit.actor.clone(),
            op: self.recorder.id().to_string(),
            op_digest: digest,
        }
    }

    /// Writes the `end` line (`after` replaces the recorded after-state
    /// when it was only known once the change ran), then the result entry
    /// to `sink`. `Ok(false)` when the operation is not in the journal
    /// (its `begin` or `end` line could not be written): then no result
    /// entry is written, since there is no digest to tie it to, and the
    /// caller may record the outcome on its own.
    pub fn finish(
        self,
        sink: &impl AuditSink,
        ok: bool,
        outcome: &str,
        after: Option<Vec<State>>,
    ) -> std::io::Result<bool> {
        let Some(digest) = self.recorder.finish(ok, outcome, after) else {
            return Ok(false);
        };
        sink.record(&self.result_entry(ok, outcome, digest))?;
        Ok(true)
    }
}

/// Moves the secret values of connector states into the credential store
/// under `op` and blanks them. Returns what was lost when the store could
/// not take them.
type SecretStore<'a> = dyn Fn(&str, &str, &str) -> bool + 'a;

fn keep_secrets(op: &str, states: &mut [State], store: &SecretStore<'_>) -> Option<String> {
    let profile = secret_store_profile(op);
    let mut lost = Vec::new();
    let mut unknown = Vec::new();
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
                // An empty value is a secret that was not known when the
                // state was read (an undo reads the connector back with its
                // secrets blanked). Keeping "" would later put the connector
                // back with empty passwords; leave it out so undoing this
                // operation is refused for want of the secret instead.
                if value.is_empty() {
                    unknown.push(format!("{name}: {key}"));
                    continue;
                }
                if !store(&profile, key, value) {
                    lost.push(format!("{name}: {key}"));
                }
                value.clear();
            }
        }
    }
    let mut reasons = Vec::new();
    if !lost.is_empty() {
        reasons.push(format!(
            "connector secrets were not kept (no OS credential store): {}",
            lost.join(", ")
        ));
    }
    if !unknown.is_empty() {
        reasons.push(format!(
            "connector secrets were not known when the state was read: {}",
            unknown.join(", ")
        ));
    }
    (!reasons.is_empty()).then(|| reasons.join("; "))
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

    fn audit_target() -> AuditTarget {
        AuditTarget {
            source: "gui".into(),
            profile: "rabbit-dev".into(),
            action: "rabbit queue delete".into(),
            target: "jobs".into(),
            actor: None,
        }
    }

    fn topic_change() -> Change {
        let state = |exists| State::Topic {
            topic: "t".into(),
            exists,
        };
        Change::new(
            Reversibility::Reversible,
            vec![state(true)],
            vec![state(false)],
        )
    }

    #[test]
    fn an_audited_record_ties_the_result_entry_to_the_journal() {
        let (dir, journal) = temp_journal("audited");
        let log = ferrika_governance::test_policy::MemoryLog::default();
        let record = AuditedRecord::start(
            journal.clone(),
            Some("01JBX3K8Q2M7".into()),
            Scope::default(),
            topic_change(),
            audit_target(),
            None,
        );
        assert_eq!(record.id(), "01JBX3K8Q2M7");
        assert!(record.recorder().started());
        assert_eq!(record.recorder().before().len(), 1);
        assert!(record.finish(&log, true, "Deleted", None).unwrap());
        let entries = log.entries.borrow();
        assert_eq!(entries.len(), 1);
        let entry = &entries[0];
        // The request entry's fields, the outcome, and the operation's key.
        assert_eq!(
            (
                entry.source.as_str(),
                entry.profile.as_str(),
                entry.action.as_str()
            ),
            ("gui", "rabbit-dev", "rabbit queue delete")
        );
        assert_eq!(entry.target, "jobs");
        assert_eq!(entry.outcome, "Deleted");
        assert!(entry.ok);
        assert_eq!(entry.op, "01JBX3K8Q2M7");
        let op = journal.load().remove(0);
        assert_eq!(op.id, "01JBX3K8Q2M7");
        assert_eq!(op.begin.source, "gui");
        assert_eq!(op.begin.action, "rabbit queue delete");
        assert_eq!(entry.op_digest, op.digest());
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn without_a_journal_no_result_entry_is_written() {
        let (dir, _) = temp_journal("audited-broken");
        std::fs::create_dir_all(&dir).unwrap();
        // The journal's directory is a file, so no line can be written.
        let blocker = dir.join("not-a-dir");
        std::fs::write(&blocker, "x").unwrap();
        let journal = Journal::at(blocker.join(HISTORY_FILE));
        let log = ferrika_governance::test_policy::MemoryLog::default();
        let record = AuditedRecord::start(
            journal,
            None,
            Scope::default(),
            topic_change(),
            audit_target(),
            None,
        );
        assert!(!record.recorder().started());
        assert!(!record.id().is_empty(), "a new id is made");
        assert!(!record.finish(&log, false, "failed", None).unwrap());
        assert!(log.entries.borrow().is_empty());
        let _ = std::fs::remove_dir_all(dir);
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
    fn a_line_torn_inside_a_character_does_not_hide_later_operations() {
        let (dir, journal) = temp_journal("torn-utf8");
        journal.append(&begin_event("A", 1)).unwrap();
        let mut bytes = std::fs::read(journal.path()).unwrap();
        bytes.extend_from_slice(br#"{"v":1,"id":"A","ev":"end","outcome":""#);
        bytes.push(0xED);
        std::fs::write(journal.path(), bytes).unwrap();
        journal.append(&end_event("A")).unwrap();
        journal.append(&begin_event("B", 2)).unwrap();
        let ops = journal.load();
        assert_eq!(ops.len(), 2);
        assert_eq!(ops[0].ok(), Some(true));
        assert_eq!(ops[1].id, "B");
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

    #[test]
    fn trimming_and_rotation_keep_lines_of_unknown_kinds_and_fields_byte_for_byte() {
        let (dir, journal) = temp_journal("trim-unknown");
        std::fs::create_dir_all(&dir).unwrap();
        let day = 24 * 60 * 60 * 1000;
        let now = 200 * day;
        let line = |event: &HistoryEvent| serde_json::to_string(event).unwrap();
        // A known `begin` with a field only a newer version writes, in a
        // key order and spacing this version would not produce.
        let mut known: serde_json::Value =
            serde_json::to_value(begin_event("NEW", now - day)).unwrap();
        known["future_field"] = serde_json::json!({"nested": [1, 2]});
        let known = format!("{{\"future_first\": true, {}", &known.to_string()[1..]);
        let kept = [
            known.clone(),
            // An event kind this version does not know, of a kept operation.
            format!(r#"{{"v":9,"id":"NEW","time_ms":{now},"ev":"checkpoint","data":{{"x":1}}}}"#),
            line(&end_event("NEW")),
            // An unknown kind of an operation this version never saw begin.
            r#"{"v":9,"id":"OTHER","time_ms":0,"ev":"begin_v9","what":"?"}"#.to_string(),
            // JSON without an id.
            r#"{"v":9,"ev":"marker"}"#.to_string(),
        ];
        let text = [
            line(&begin_event("OLD", now - 91 * day)),
            r#"{"v":9,"id":"OLD","time_ms":1,"ev":"checkpoint"}"#.to_string(),
            kept[0].clone(),
            kept[1].clone(),
            r#"{"v":1,"id":"TORN","ev":"en"#.to_string(),
            kept[2].clone(),
            kept[3].clone(),
            line(&end_event("OLD")),
            kept[4].clone(),
        ]
        .join("\n")
            + "\n";
        std::fs::write(journal.path(), text).unwrap();
        // The known `begin` with an unknown field is still read.
        assert_eq!(journal.load().len(), 2);
        journal.trim_locked(now).unwrap();
        let expected = kept.join("\n") + "\n";
        assert_eq!(std::fs::read_to_string(journal.path()).unwrap(), expected);
        let ops = journal.load();
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].id, "NEW");
        assert_eq!(ops[0].ok(), Some(true));
        // Rotation moves the file as it is.
        journal.rotate_locked().unwrap();
        assert_eq!(
            std::fs::read_to_string(journal.rotated_path()).unwrap(),
            expected
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn a_large_journal_of_recent_operations_is_rotated_not_reread_on_every_append() {
        let (dir, journal) = temp_journal("rotate");
        let now = ferrika_core::kafka::clock::now_ms();
        // Large entries (many config keys): fewer than KEEP_OPERATIONS of
        // them pass the limit, so trimming drops nothing.
        let big = |id: &str| {
            let mut event = begin_event(id, now);
            if let EventBody::Begin(begin) = &mut event.body {
                begin.target = "x".repeat(2000);
            }
            event
        };
        let line_len = serde_json::to_string(&big("R00000")).unwrap().len() as u64 + 1;
        let count = (TRIM_ABOVE_BYTES / line_len + 2) as usize;
        assert!(count < KEEP_OPERATIONS);
        for i in 0..count {
            journal.append(&big(&format!("R{i:05}"))).unwrap();
        }
        let rotated = journal.rotated_path();
        assert!(rotated.exists(), "rotated at the limit");
        let current = std::fs::metadata(journal.path()).unwrap().len();
        assert!(current < TRIM_ABOVE_BYTES / 2, "{current}");
        // History still sees every operation, across both files.
        let ops = journal.load();
        assert_eq!(ops.len(), count);
        assert_eq!(ops[0].id, "R00000");
        assert_eq!(ops.last().unwrap().id, format!("R{:05}", count - 1));
        // An operation that began before the rotation still gets its end.
        journal.append(&end_event("R00000")).unwrap();
        let ops = journal.load();
        assert_eq!(
            ops.iter()
                .find(|op| op.id == "R00000")
                .and_then(Operation::ok),
            Some(true)
        );
        // A second rotation drops the first file's operations: bounded.
        for i in 0..count {
            journal.append(&big(&format!("S{i:05}"))).unwrap();
        }
        let ops = journal.load();
        // The first file's operations are gone (lines appended after the
        // first rotation moved on with the second).
        assert!(ops.iter().all(|op| op.id != "R00000"), "dropped");
        assert!(ops.len() < 2 * count, "{}", ops.len());
        let total = std::fs::metadata(journal.path()).unwrap().len()
            + std::fs::metadata(&rotated).unwrap().len();
        assert!(total <= 2 * TRIM_ABOVE_BYTES + line_len, "{total}");
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn trimming_keeps_the_end_of_an_operation_that_began_in_the_rotated_file() {
        let (dir, journal) = temp_journal("trim-orphan");
        let day = 24 * 60 * 60 * 1000;
        let now = 200 * day;
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            journal.rotated_path(),
            format!(
                "{}\n",
                serde_json::to_string(&begin_event("EARLY", now - day)).unwrap()
            ),
        )
        .unwrap();
        journal.append(&end_event("EARLY")).unwrap();
        journal.append(&begin_event("OLD", now - 91 * day)).unwrap();
        journal.trim_locked(now).unwrap();
        let ops = journal.load();
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].id, "EARLY");
        assert_eq!(ops[0].ok(), Some(true));
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
    fn blanked_secrets_are_not_stored_as_empty_passwords() {
        let (dir, journal) = temp_journal("blanked");
        let kept = std::cell::RefCell::new(Vec::new());
        // An undo records the connector as read back: its secret blanked.
        let _recorder = Recorder::start_with(
            journal.clone(),
            "01JBX3K8Q2MA".to_string(),
            begin_with(vec![connector("")], vec![]),
            &|_, key, value| {
                kept.borrow_mut().push((key.to_string(), value.to_string()));
                true
            },
        );
        assert!(kept.borrow().is_empty(), "{:?}", kept.borrow());
        let op = &journal.load()[0];
        assert_eq!(op.begin.reversibility, Reversibility::Partial);
        assert!(
            op.begin.limits[0].contains("not known"),
            "{:?}",
            op.begin.limits
        );
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

    /// The journal line shapes of Ferrika 0.2, which knows none of the
    /// `rabbit_*` state kinds. A state's fields do not decide whether a line
    /// parses there, only its kind, so the variants are empty.
    mod v020 {
        #[derive(serde::Deserialize)]
        #[serde(tag = "kind", rename_all = "snake_case")]
        #[allow(dead_code)]
        pub enum State {
            TopicConfig {},
            BrokerConfig {},
            Topic {},
            Replicas {},
            GroupOffsets {},
            Acls {},
            Quota {},
            Scram {},
            SubjectCompat {},
            SubjectVersions {},
            Connector {},
        }

        #[derive(serde::Deserialize)]
        #[allow(dead_code)]
        pub struct Begin {
            pub action: String,
            #[serde(default)]
            pub before: Vec<State>,
            #[serde(default)]
            pub after: Vec<State>,
        }

        #[derive(serde::Deserialize)]
        #[allow(dead_code)]
        pub struct End {
            pub ok: bool,
            #[serde(default)]
            pub after: Option<Vec<State>>,
        }

        #[derive(serde::Deserialize)]
        #[serde(tag = "ev", rename_all = "snake_case")]
        #[allow(dead_code)]
        pub enum Body {
            Begin(Box<Begin>),
            End(End),
            Note { text: String },
        }

        #[derive(serde::Deserialize)]
        #[allow(dead_code)]
        pub struct Event {
            pub v: u32,
            pub id: String,
            pub time_ms: i64,
            #[serde(flatten)]
            pub body: Body,
        }
    }

    #[test]
    fn a_0_2_reader_skips_rabbit_operations_whole_and_keeps_the_others() {
        use ferrika_core::model::history::RabbitQueueDeclaration;
        let (dir, journal) = temp_journal("v020");
        journal.append(&begin_event("KAFKA", 1)).unwrap();
        journal.append(&end_event("KAFKA")).unwrap();
        // A RabbitMQ queue deletion, as `rabbit queue delete` records it.
        let queue = |declaration| State::RabbitQueue {
            vhost: "/".into(),
            name: "orders".into(),
            declaration,
        };
        let mut rabbit = begin_event("RABBIT", 2);
        if let EventBody::Begin(begin) = &mut rabbit.body {
            begin.scope = Scope {
                profile: "dev".into(),
                rabbitmq: "rabbit:15672".into(),
                ..Scope::default()
            };
            begin.action = "rabbit queue delete".into();
            begin.before = vec![queue(Some(RabbitQueueDeclaration {
                kind: "classic".into(),
                durable: true,
                auto_delete: false,
                arguments: BTreeMap::new(),
            }))];
            begin.after = vec![queue(None)];
        }
        journal.append(&rabbit).unwrap();
        let mut rabbit_end = end_event("RABBIT");
        if let EventBody::End(end) = &mut rabbit_end.body {
            end.after = Some(vec![queue(None)]);
        }
        journal.append(&rabbit_end).unwrap();

        // This version reads both.
        let ops = journal.load();
        assert_eq!(ops.len(), 2);
        assert_eq!(ops[1].begin.scope.rabbitmq, "rabbit:15672");

        // 0.2 reads lines one by one and skips those it cannot parse, as
        // `read_events` does: only the Kafka operation's lines are left, so
        // `history`, `undo` and `audit verify` there never see the RabbitMQ
        // one (no "edited" alarm for a scope it cannot hash).
        let text = std::fs::read_to_string(journal.path()).unwrap();
        let parsed: Vec<Option<v020::Event>> = text
            .lines()
            .map(|line| serde_json::from_str::<v020::Event>(line).ok())
            .collect();
        assert_eq!(parsed.len(), 4);
        let ids: Vec<&str> = parsed
            .iter()
            .flatten()
            .map(|event| event.id.as_str())
            .collect();
        assert_eq!(ids, vec!["KAFKA", "KAFKA"]);
        // Its trim keeps lines it cannot parse byte for byte (see
        // `trimming_and_rotation_keep_lines_of_unknown_kinds_and_fields_byte_for_byte`),
        // so the RabbitMQ operation survives a 0.2 trim.
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
