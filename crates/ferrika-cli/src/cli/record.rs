//! Records CLI changes in the operation history (`ferrika history`), so
//! `ferrika undo` can put them back. See `crates/ferrika-ops/src/history.rs`.
//!
//! `main` makes the operation id before the write gate, so the audit
//! request entry names it (`op`). A handler reads the state it is about to
//! change, starts a [`Pending`] record under that id, runs the change and
//! finishes the record with the outcome; the finish also writes the result
//! entry of the same operation, which adds the digest that ties the journal
//! line to the audit log (`op_digest`).

use std::time::Duration;

use colored::Colorize;

use crate::cli::{GlobalOpts, WriteContext};
use crate::error::KafkaResult;
use crate::governance::audit::AuditEntry;
use crate::kafka::admin::KafkaAdminClient;
use crate::model::format::redact_url;
use crate::model::history::{Reversibility, Scope, State};
use crate::ops::history::{Journal, Recorder};

/// The suffix of confirmation prompts for changes undo cannot reverse.
pub const CANNOT_UNDO: &str = "This cannot be undone with `ferrika undo`.";

pub use crate::ops::history::Change;

/// The change from a before-state read that may have failed. A failed read
/// does not stop the change: it is recorded as not undoable, and the user is
/// told so first. (A function, not an inherent `Change` method: `Change`
/// belongs to the core, this prints to the console.)
pub fn change_from_capture(
    reversibility: Reversibility,
    captured: KafkaResult<(Vec<State>, Vec<State>)>,
) -> Change {
    if let Err(e) = &captured {
        eprintln!(
            "{} the current state could not be read ({e}); this change will be recorded as not undoable.",
            "warning:".yellow().bold()
        );
    }
    Change::captured(reversibility, captured)
}

/// Where a CLI change runs, for the record: the profile, its brokers and
/// the cluster id (one Metadata round trip, only for changes).
pub fn cluster_scope(global: &GlobalOpts, admin: Option<&KafkaAdminClient>) -> Scope {
    let cluster_id = match admin {
        Some(admin) => admin.cluster_id(Some(Duration::from_secs(5))),
        None => KafkaAdminClient::new(&global.to_kafka_config())
            .ok()
            .and_then(|admin| admin.cluster_id(Some(Duration::from_secs(5)))),
    };
    Scope {
        profile: global.resolved_profile_name().unwrap_or_default(),
        bootstrap: global.resolved_bootstrap_servers(),
        cluster_id: cluster_id.unwrap_or_default(),
        ..Scope::default()
    }
}

/// The scope of a change to the Schema Registry at `url`.
pub fn registry_scope(global: &GlobalOpts, url: &str) -> Scope {
    Scope {
        profile: global.resolved_profile_name().unwrap_or_default(),
        bootstrap: global.resolved_bootstrap_servers(),
        registry: redact_url(url),
        ..Scope::default()
    }
}

/// The scope of a change to the Kafka Connect cluster at `url`.
pub fn connect_scope(global: &GlobalOpts, url: &str) -> Scope {
    Scope {
        profile: global.resolved_profile_name().unwrap_or_default(),
        bootstrap: global.resolved_bootstrap_servers(),
        connect: redact_url(url),
        ..Scope::default()
    }
}

/// A started record, finished with [`Pending::finish`].
pub struct Pending {
    recorder: Recorder,
    label: String,
    target: String,
    profile: String,
}

/// Starts recording the change `main` authorized for this run (no-op
/// without one, e.g. in tests that call handlers directly), under the
/// operation id the audit request entry names.
pub fn start(global: &GlobalOpts, scope: Scope, change: Change) -> Option<Pending> {
    start_with(global, scope, change, None, None)
}

/// [`start`] for operation `nth` (from 0) of several a command runs, with
/// its own target (e.g. one group of `offset delete-group a b`). The first
/// takes the id the request entry names; the others get ids of their own,
/// which their result entries carry.
pub fn start_for(
    global: &GlobalOpts,
    scope: Scope,
    change: Change,
    target: String,
    nth: usize,
) -> Option<Pending> {
    let write = global.write.as_ref()?;
    let id = write.op.clone().filter(|_| nth == 0);
    start_as(write, id, scope, change, Some(target), None, None)
}

/// [`start`] with a target of its own and, for an undo, the operation it
/// undoes.
pub fn start_with(
    global: &GlobalOpts,
    scope: Scope,
    change: Change,
    target: Option<String>,
    undoes: Option<String>,
) -> Option<Pending> {
    let write = global.write.as_ref()?;
    start_as(write, write.op.clone(), scope, change, target, undoes, None)
}

/// [`start_with`] for a RabbitMQ change: the result entry names
/// `audit_profile`, as the request entry did (the RabbitMQ profile, else
/// the broker addresses the gate matched).
pub fn start_rabbit(
    global: &GlobalOpts,
    scope: Scope,
    change: Change,
    audit_profile: String,
    undoes: Option<String>,
) -> Option<Pending> {
    let write = global.write.as_ref()?;
    start_as(
        write,
        write.op.clone(),
        scope,
        change,
        None,
        undoes,
        Some(audit_profile),
    )
}

/// Starts the record under `id`, or a new id when the request entry named
/// none.
fn start_as(
    write: &WriteContext,
    id: Option<String>,
    scope: Scope,
    change: Change,
    target: Option<String>,
    undoes: Option<String>,
    audit_profile: Option<String>,
) -> Option<Pending> {
    let target = target.unwrap_or_else(|| write.target.clone());
    let profile = audit_profile.unwrap_or_else(|| {
        if scope.profile.is_empty() {
            scope.bootstrap.clone()
        } else {
            scope.profile.clone()
        }
    });
    let begin = change.into_begin("cli", scope, write.label.clone(), target.clone(), undoes);
    let recorder = match id {
        Some(id) => Recorder::start_with_id(Journal::default(), id, begin),
        None => Recorder::start(Journal::default(), begin),
    };
    Some(Pending {
        recorder,
        label: write.label.clone(),
        target,
        profile,
    })
}

impl Pending {
    /// Writes the outcome to the journal and the operation's result entry
    /// to the audit log: the request's action, the outcome, the operation
    /// id and its digest.
    pub fn finish(self, ok: bool, outcome: &str, after: Option<Vec<State>>) {
        let Some(digest) = self.recorder.finish(ok, outcome, after) else {
            return;
        };
        crate::governance::audit::record(&self.result_entry(ok, outcome, digest));
    }

    fn result_entry(&self, ok: bool, outcome: &str, digest: String) -> AuditEntry {
        AuditEntry {
            time_ms: crate::governance::audit::now_ms(),
            source: "cli".to_string(),
            profile: self.profile.clone(),
            action: self.label.clone(),
            target: self.target.clone(),
            outcome: outcome.to_string(),
            ok,
            user: String::new(),
            actor: None,
            op: self.recorder.id().to_string(),
            op_digest: digest,
        }
    }

    /// [`Pending::finish`] with a result: its error text when it failed.
    pub fn finish_with<T, E: std::fmt::Display>(self, result: &Result<T, E>, success: &str) {
        match result {
            Ok(_) => self.finish(true, success, None),
            Err(e) => self.finish(false, &e.to_string(), None),
        }
    }
}

/// [`Pending::finish_with`] for an optional record.
pub fn finish<T, E: std::fmt::Display>(
    pending: Option<Pending>,
    result: &Result<T, E>,
    success: &str,
) {
    if let Some(pending) = pending {
        pending.finish_with(result, success);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::KafkaError;

    #[test]
    fn unreadable_before_state_is_recorded_as_not_undoable() {
        let change = crate::cli::record::change_from_capture(
            Reversibility::Reversible,
            Err(KafkaError::admin("describe denied")),
        );
        assert_eq!(change.reversibility, Reversibility::Irreversible);
        assert!(change.before.is_empty());
        assert!(
            change.limits[0].contains("describe denied"),
            "{:?}",
            change.limits
        );
    }

    #[test]
    fn nothing_is_recorded_without_an_authorized_change() {
        use clap::Parser;
        #[derive(Parser)]
        struct Wrap {
            #[command(flatten)]
            g: GlobalOpts,
        }
        let global = Wrap::parse_from(["ferrika", "-b", "127.0.0.1:1"]).g;
        assert!(global.write.is_none());
        let change = Change::new(Reversibility::Reversible, vec![], vec![]);
        assert!(start(&global, Scope::default(), change).is_none());
    }

    #[test]
    fn the_result_entry_is_keyed_by_the_operation_the_request_named() {
        let dir = std::env::temp_dir().join(format!(
            "ferrika-record-{}-{}",
            std::process::id(),
            crate::kafka::clock::now_ms()
        ));
        let journal = Journal::at(dir.join("history.jsonl"));
        let state = |v: &str| State::Topic {
            topic: v.into(),
            exists: true,
        };
        let begin = Change::new(
            Reversibility::Reversible,
            vec![state("a")],
            vec![state("b")],
        )
        .into_begin(
            "cli",
            Scope::default(),
            "topics config".into(),
            "orders".into(),
            None,
        );
        let pending = Pending {
            recorder: Recorder::start_with_id(journal.clone(), "01JBX3K8Q2M7".into(), begin),
            label: "topics config".into(),
            target: "orders".into(),
            profile: "dev".into(),
        };
        let digest = pending.recorder.finish(true, "Applied", None).unwrap();
        let entry = pending.result_entry(true, "Applied", digest.clone());
        // Same action as the request entry; the operation id is the key.
        assert_eq!(entry.action, "topics config");
        assert_eq!(entry.op, "01JBX3K8Q2M7");
        assert_eq!(entry.op_digest, digest);
        assert_eq!(journal.load()[0].id, "01JBX3K8Q2M7");
        assert_eq!(journal.load()[0].digest(), digest);
        let _ = std::fs::remove_dir_all(dir);
    }
}
