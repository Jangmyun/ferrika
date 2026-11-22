//! Preparing an undo, shared by `ferrika undo` and the GUI: find the
//! operation, read the current state, compare, plan the steps and decide
//! whether undo may run. The callers run the policy gate, ask, record and
//! run [`execute`].

use serde::Serialize;

use ferrika_core::error::KafkaResult;
use ferrika_core::kafka::admin::KafkaAdminClient;
use ferrika_core::kafka::config::KafkaClientConfig;
use ferrika_core::kafka::visibility::{await_config, CONFIG_VISIBILITY};
use ferrika_core::model::history::{DigestStatus, Operation, Reversibility, Scope, State};

use super::apply::{self, Target};
use super::capture::recapture;
use super::conflict::{self, Verdict};
use super::inverse::{self, NotReversible, Step};

/// Shortest id prefix accepted.
pub const MIN_PREFIX: usize = 6;

/// The operation `id` names: the whole id or a unique prefix of at least
/// [`MIN_PREFIX`] characters (case ignored).
pub fn find<'a>(ops: &'a [Operation], id: &str) -> Result<&'a Operation, String> {
    let wanted = id.trim().to_ascii_uppercase();
    if let Some(op) = ops.iter().find(|op| op.id == wanted) {
        return Ok(op);
    }
    if wanted.len() < MIN_PREFIX {
        return Err(format!(
            "'{id}' is too short; give at least {MIN_PREFIX} characters of the id (see `ferrika history`)"
        ));
    }
    let matches: Vec<&Operation> = ops.iter().filter(|op| op.id.starts_with(&wanted)).collect();
    match matches.as_slice() {
        [op] => Ok(op),
        [] => Err(format!(
            "No recorded operation with id '{id}' (see `ferrika history`)"
        )),
        many => Err(format!(
            "'{id}' matches {} operations ({}); give more of the id",
            many.len(),
            many.iter()
                .map(|op| op.id.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        )),
    }
}

/// Whether `op` is a candidate for undo at all: it succeeded, has not been
/// undone and its kind can be undone.
pub fn undoable(op: &Operation) -> bool {
    op.ok() == Some(true) && op.undone_by.is_none() && op.begin.reversibility.undoable()
}

/// The newest operation `user` ran in `scope` that can still be undone
/// (what `ferrika undo` without an id means).
pub fn latest_undoable<'a>(
    ops: &'a [Operation],
    scope: &Scope,
    user: &str,
) -> Option<&'a Operation> {
    ops.iter()
        .rev()
        .find(|op| undoable(op) && op.begin.user == user && op.begin.scope.same_cluster(scope))
}

/// What the audit log says about the journal entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DigestCheck {
    /// The audit entry's `op_digest` (version 2) matches the whole journal
    /// entry.
    Matches,
    /// A version 1 `op_digest` (Ferrika 0.1) matches: the before- and
    /// after-state are as recorded, but the scope, action, `undoes` link
    /// and reversibility are not covered. Undo shows the target cluster
    /// and asks again (`--accept-legacy`).
    Legacy,
    /// No audit entry names the operation (log rotated, other location).
    Missing,
    /// The journal no longer matches what was audited: it was edited.
    Mismatch,
}

impl DigestCheck {
    /// How `history`, `undo` and `audit verify` name it.
    pub fn label(self) -> &'static str {
        self.status().label()
    }

    fn status(self) -> DigestStatus {
        match self {
            Self::Matches => DigestStatus::Verified,
            Self::Legacy => DigestStatus::Legacy,
            Self::Missing => DigestStatus::Missing,
            Self::Mismatch => DigestStatus::Mismatch,
        }
    }
}

/// Compares the journal entry of `op` with the audit log's `op_digest`
/// (see [`Operation::verify_digest`]).
pub fn digest_check(op: &Operation, audited: Option<&str>) -> DigestCheck {
    match op.verify_digest(audited) {
        DigestStatus::Verified => DigestCheck::Matches,
        DigestStatus::Legacy => DigestCheck::Legacy,
        DigestStatus::Missing => DigestCheck::Missing,
        DigestStatus::Mismatch => DigestCheck::Mismatch,
    }
}

/// Whether the journal's claim that `undoer` undid an operation can be
/// believed, given the audit log's `op_digest` for `undoer`. Only a
/// version 2 digest covers the `undoes` link; without any audit entry the
/// claim stands unless a company policy is installed (as for undo itself).
/// A claim that is not believed does not stop an undo: the current state
/// is read and compared anyway, so a change that really was undone shows
/// as already reverted.
pub fn undo_mark_trusted(undoer: &Operation, audited: Option<&str>, policy_active: bool) -> bool {
    match digest_check(undoer, audited) {
        DigestCheck::Matches => true,
        DigestCheck::Missing => !policy_active,
        DigestCheck::Legacy | DigestCheck::Mismatch => false,
    }
}

/// Everything known about undoing one operation, before anything runs.
#[derive(Debug, Clone, Serialize)]
pub struct UndoPlan {
    pub op: String,
    /// The state read now, one per recorded before-state.
    #[serde(skip)]
    pub current: Vec<State>,
    pub verdict: Verdict,
    /// The steps, or why there are none.
    #[serde(serialize_with = "serialize_steps")]
    pub steps: Result<Vec<Step>, NotReversible>,
    /// Problems `--force` cannot override.
    pub blockers: Vec<String>,
    /// Later operations on the same resources still in effect.
    pub later: Vec<String>,
    pub digest: DigestCheck,
    /// The operation ran on the cluster (or registry, Connect) in use now,
    /// as [`Scope::check_undo_target`] decides (when not, `steps` says why).
    pub same_scope: bool,
    /// Whether redoing (undoing the undo) will be possible, and why not.
    pub redo: Reversibility,
    pub redo_limits: Vec<String>,
}

fn serialize_steps<S: serde::Serializer>(
    steps: &Result<Vec<Step>, NotReversible>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    use serde::ser::SerializeMap;
    let mut map = serializer.serialize_map(Some(1))?;
    match steps {
        Ok(steps) => map.serialize_entry("steps", steps)?,
        Err(reason) => map.serialize_entry("not_reversible", &reason.0)?,
    }
    map.end()
}

impl UndoPlan {
    /// The steps, when there are any to run.
    pub fn runnable_steps(&self) -> &[Step] {
        self.steps.as_deref().unwrap_or_default()
    }

    /// Why undo must stop, or `None` when it may go on. `force` passes
    /// drift and later changes, never blockers, a foreign scope or an
    /// edited journal. With a company policy, an operation the audit log
    /// does not know is refused too.
    pub fn refusal(&self, force: bool, policy_active: bool) -> Option<String> {
        match self.digest {
            DigestCheck::Mismatch => {
                return Some(
                    "the history entry does not match the audit log (it was edited); refusing to undo"
                        .to_string(),
                )
            }
            DigestCheck::Missing if policy_active => {
                return Some(
                    "the audit log has no entry for this operation, which the company policy requires before undo"
                        .to_string(),
                )
            }
            _ => {}
        }
        // After the digest: an edited scope is reported as an edit.
        if !self.same_scope {
            return Some(match &self.steps {
                Err(reason) => reason.0.clone(),
                Ok(_) => {
                    "this operation was recorded on another cluster; connect to that one to undo it"
                        .to_string()
                }
            });
        }
        if let Err(reason) = &self.steps {
            return Some(format!("it cannot be undone: {reason}"));
        }
        if !self.blockers.is_empty() {
            return Some(self.blockers.join("; "));
        }
        if force {
            return None;
        }
        if !self.later.is_empty() {
            return Some(format!(
                "later operation(s) {} changed the same resources; undo them first (or use --force)",
                self.later.join(", ")
            ));
        }
        if let Verdict::Drift { .. } = self.verdict {
            return Some(
                "the state changed since this operation (see the table); use --force to overwrite it"
                    .to_string(),
            );
        }
        None
    }
}

/// How long after an operation ended its change may still be on its way
/// to every broker (KRaft applies configs through the metadata log).
pub(crate) const RECENT_MS: i64 = 5_000;

/// Whether reading `op`'s resources at `now_ms` may still see the state
/// before it: it succeeded moments ago and recorded the state it left, so
/// there is a state to wait for. Otherwise one read is authoritative.
pub(crate) fn may_be_settling(op: &Operation, now_ms: i64) -> bool {
    op.ok() == Some(true)
        && !op.after().is_empty()
        && op.after().len() == op.begin.before.len()
        && op
            .ended_ms
            .is_some_and(|ended| (0..=RECENT_MS).contains(&(now_ms - ended)))
}

async fn read_states(
    op: &Operation,
    config: &KafkaClientConfig,
    admin: &KafkaAdminClient,
) -> KafkaResult<Vec<State>> {
    let mut current = Vec::with_capacity(op.begin.before.len());
    for state in &op.begin.before {
        current.push(recapture(state, config, admin).await?);
    }
    Ok(current)
}

/// Reads the current state of `op`'s resources. Right after the operation
/// ended, the state it left may not be visible yet: the read is repeated
/// (with backoff, for at most [`CONFIG_VISIBILITY`]'s deadline) until it is.
pub(crate) async fn read_current(
    op: &Operation,
    config: &KafkaClientConfig,
    admin: &KafkaAdminClient,
) -> KafkaResult<(Vec<State>, Verdict)> {
    let current = if may_be_settling(op, ferrika_core::kafka::clock::now_ms()) {
        await_config(
            CONFIG_VISIBILITY,
            || read_states(op, config, admin),
            |current| conflict::check(op, current) == Verdict::Clean,
        )
        .await?
    } else {
        read_states(op, config, admin).await?
    };
    let verdict = conflict::check(op, &current);
    Ok((current, verdict))
}

/// Builds the plan for undoing `op` (one of `all`, the whole journal).
/// `current_scope` is where the caller is connected; `audited` the audit
/// log's `op_digest` for the operation.
pub async fn prepare(
    op: &Operation,
    all: &[Operation],
    current_scope: &Scope,
    audited: Option<&str>,
    config: &KafkaClientConfig,
    admin: &KafkaAdminClient,
) -> KafkaResult<UndoPlan> {
    let scope_check = op.begin.scope.check_undo_target(current_scope);
    let same_scope = scope_check.is_ok();
    let digest = digest_check(op, audited);
    let later: Vec<String> = conflict::later_overlaps(op, all)
        .iter()
        .map(|o| o.id.clone())
        .collect();
    let (current, verdict, steps) = if let Err(reason) = scope_check {
        (
            Vec::new(),
            Verdict::Drift { rows: Vec::new() },
            Err(NotReversible(reason)),
        )
    } else if !op.begin.reversibility.undoable() || op.begin.before.is_empty() {
        // Nothing to read for an operation that cannot be undone anyway.
        (
            Vec::new(),
            Verdict::Drift { rows: Vec::new() },
            inverse::plan(op, &[]),
        )
    } else {
        let (current, verdict) = read_current(op, config, admin).await?;
        let steps = inverse::plan(op, &current);
        (current, verdict, steps)
    };
    let blockers = match &steps {
        Ok(steps) => {
            apply::blockers(
                steps,
                &Target {
                    config,
                    admin,
                    op: &op.id,
                },
            )
            .await
        }
        Err(_) => Vec::new(),
    };
    let (redo, redo_limits) = redo_reversibility(&current, &op.begin.before);
    Ok(UndoPlan {
        op: op.id.clone(),
        current,
        verdict,
        steps,
        blockers,
        later,
        digest,
        same_scope,
        redo,
        redo_limits,
    })
}

/// Whether the undo itself can be undone: going from the state it restores
/// back to `current`.
pub fn redo_reversibility(current: &[State], restored: &[State]) -> (Reversibility, Vec<String>) {
    let mut limits = Vec::new();
    for (now, target) in current.iter().zip(restored) {
        if let Err(reason) = inverse::restore_steps(now, target) {
            limits.push(reason.0);
        }
    }
    if limits.is_empty() {
        (Reversibility::Reversible, limits)
    } else {
        (Reversibility::Irreversible, limits)
    }
}

/// Runs the plan's steps in order, stopping at the first failure. Returns
/// the outcome text, or the error naming exactly which steps were applied
/// before the failure and which were not run (see [`partial_failure`]).
/// The applied steps are not rolled back.
pub async fn execute(plan: &UndoPlan, target: &Target<'_>) -> Result<String, String> {
    let steps = plan.runnable_steps();
    if steps.is_empty() {
        return Ok("already in its previous state; nothing changed".to_string());
    }
    for (ix, step) in steps.iter().enumerate() {
        if let Err(e) = apply::run(step, target).await {
            return Err(partial_failure(steps, ix, &e.to_string()));
        }
    }
    Ok(format!(
        "Undone: {}",
        steps
            .iter()
            .map(Step::describe)
            .collect::<Vec<_>>()
            .join("; ")
    ))
}

/// The outcome of an undo whose step `failed` (from 0) failed with
/// `error`: the failed step, then every step applied before it (those
/// changes stay in place) and every step not run.
pub fn partial_failure(steps: &[Step], failed: usize, error: &str) -> String {
    let Some(step) = steps.get(failed) else {
        return error.to_string();
    };
    let mut text = format!("{}: {error}", step.describe());
    let list = |steps: &[Step]| {
        steps
            .iter()
            .map(Step::describe)
            .collect::<Vec<_>>()
            .join("; ")
    };
    if failed == 0 {
        text.push_str(". Nothing was changed");
    } else {
        text.push_str(&format!(
            ". Applied before it and left in place ({failed} of {}): {}",
            steps.len(),
            list(&steps[..failed])
        ));
    }
    if failed + 1 < steps.len() {
        text.push_str(&format!(". Not run: {}", list(&steps[failed + 1..])));
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrika_core::model::history::{Begin, ConfigValue, End};
    use std::collections::BTreeMap;

    fn op(id: &str, user: &str, ok: bool) -> Operation {
        let state = |v: &str| State::TopicConfig {
            topic: "t".into(),
            keys: BTreeMap::from([(
                "k".to_string(),
                ConfigValue {
                    own: Some(v.to_string()),
                    sensitive: false,
                },
            )]),
        };
        Operation {
            id: id.into(),
            time_ms: 1,
            begin: Begin {
                source: "cli".into(),
                user: user.into(),
                scope: Scope {
                    bootstrap: "a:9092".into(),
                    ..Scope::default()
                },
                action: "topics config".into(),
                target: "t".into(),
                reversibility: Reversibility::Reversible,
                limits: vec![],
                before: vec![state("1")],
                after: vec![state("2")],
                info: vec![],
                undoes: None,
            },
            end: Some(End {
                ok,
                outcome: String::new(),
                after: None,
            }),
            ended_ms: None,
            notes: vec![],
            undone_by: None,
        }
    }

    #[test]
    fn ids_resolve_by_unique_prefix() {
        let ops = vec![op("01JBX3K8Q2M7", "u", true), op("01JBX3K9AAAA", "u", true)];
        assert_eq!(find(&ops, "01jbx3k8q2m7").unwrap().id, "01JBX3K8Q2M7");
        assert_eq!(find(&ops, "01JBX3K8").unwrap().id, "01JBX3K8Q2M7");
        assert!(find(&ops, "01JBX3K").unwrap_err().contains("matches 2"));
        assert!(find(&ops, "01JB").unwrap_err().contains("too short"));
        assert!(find(&ops, "ZZZZZZZ").is_err());
    }

    #[test]
    fn latest_undoable_skips_failed_undone_and_other_users() {
        let mut ops = vec![
            op("A", "me", true),
            op("B", "me", true),
            op("C", "other", true),
            op("D", "me", false),
        ];
        let scope = Scope {
            bootstrap: "a:9092".into(),
            ..Scope::default()
        };
        assert_eq!(latest_undoable(&ops, &scope, "me").unwrap().id, "B");
        ops[1].undone_by = Some("U".into());
        assert_eq!(latest_undoable(&ops, &scope, "me").unwrap().id, "A");
        let elsewhere = Scope {
            bootstrap: "b:9092".into(),
            ..Scope::default()
        };
        assert!(latest_undoable(&ops, &elsewhere, "me").is_none());
    }

    fn plan(verdict: Verdict) -> UndoPlan {
        UndoPlan {
            op: "A".into(),
            current: vec![],
            verdict,
            steps: Ok(vec![]),
            blockers: vec![],
            later: vec![],
            digest: DigestCheck::Matches,
            same_scope: true,
            redo: Reversibility::Reversible,
            redo_limits: vec![],
        }
    }

    #[test]
    fn force_passes_drift_and_later_changes_but_not_blockers_or_tampering() {
        let drift = plan(Verdict::Drift { rows: vec![] });
        assert!(drift.refusal(false, false).is_some());
        assert!(drift.refusal(true, false).is_none());
        let later = UndoPlan {
            later: vec!["B".into()],
            ..plan(Verdict::Clean)
        };
        assert!(later.refusal(false, false).unwrap().contains('B'));
        assert!(later.refusal(true, false).is_none());
        let blocked = UndoPlan {
            blockers: vec!["topic t has records".into()],
            ..plan(Verdict::Clean)
        };
        assert!(blocked.refusal(true, false).is_some());
        let edited = UndoPlan {
            digest: DigestCheck::Mismatch,
            ..plan(Verdict::Clean)
        };
        assert!(edited.refusal(true, false).is_some());
        let unaudited = UndoPlan {
            digest: DigestCheck::Missing,
            ..plan(Verdict::Clean)
        };
        assert!(unaudited.refusal(false, false).is_none());
        assert!(unaudited.refusal(false, true).is_some());
        let elsewhere = UndoPlan {
            same_scope: false,
            ..plan(Verdict::Clean)
        };
        assert!(elsewhere.refusal(true, false).is_some());
        assert!(plan(Verdict::Clean).refusal(false, false).is_none());
    }

    #[test]
    fn only_an_operation_that_just_ended_is_waited_for() {
        let mut op = op("A", "u", true);
        assert!(!may_be_settling(&op, 10_000), "no end time recorded");
        op.ended_ms = Some(10_000);
        assert!(may_be_settling(&op, 10_000));
        assert!(may_be_settling(&op, 10_000 + RECENT_MS));
        assert!(!may_be_settling(&op, 10_001 + RECENT_MS), "long ago");
        let failed = Operation {
            end: Some(End {
                ok: false,
                outcome: String::new(),
                after: None,
            }),
            ..op.clone()
        };
        assert!(!may_be_settling(&failed, 10_000));
        let mut unknown_after = op.clone();
        unknown_after.begin.after.clear();
        unknown_after.end = Some(End {
            ok: true,
            outcome: String::new(),
            after: None,
        });
        assert!(
            !may_be_settling(&unknown_after, 10_000),
            "nothing to wait for"
        );
    }

    #[test]
    fn digest_check_compares_with_the_audit_log() {
        let op = op("A", "u", true);
        assert_eq!(digest_check(&op, Some(&op.digest())), DigestCheck::Matches);
        assert_eq!(
            digest_check(&op, Some(&op.digest_v1())),
            DigestCheck::Legacy
        );
        assert_eq!(digest_check(&op, Some("x")), DigestCheck::Mismatch);
        assert_eq!(digest_check(&op, None), DigestCheck::Missing);
        assert_eq!(
            DigestCheck::Legacy.label(),
            "legacy digest (scope not covered)"
        );
    }

    #[test]
    fn an_edited_scope_is_detected_before_undo() {
        let op = op("A", "u", true);
        let audited = op.digest();
        let mut moved = op.clone();
        moved.begin.scope.bootstrap = "prod:9092".into();
        assert_eq!(digest_check(&moved, Some(&audited)), DigestCheck::Mismatch);
        let plan = UndoPlan {
            digest: digest_check(&moved, Some(&audited)),
            ..plan(Verdict::Clean)
        };
        assert!(plan.refusal(true, false).unwrap().contains("edited"));
        // Edited to name another cluster: reported as the edit it is.
        let moved_plan = UndoPlan {
            same_scope: false,
            steps: Err(NotReversible("recorded on another cluster".into())),
            ..plan
        };
        assert!(moved_plan.refusal(true, false).unwrap().contains("edited"));
    }

    #[test]
    fn an_edited_undoes_link_does_not_block_the_real_undo() {
        // U really undid nothing; someone pointed its `undoes` at A.
        let mut undoer = op("U", "u", true);
        let audited = undoer.digest();
        undoer.begin.undoes = Some("A".into());
        assert!(!undo_mark_trusted(&undoer, Some(&audited), false));
        // A real, untouched undo is believed; without an audit entry only
        // when no policy is installed; a legacy digest never covers it.
        let real = op("V", "u", true);
        assert!(undo_mark_trusted(&real, Some(&real.digest()), true));
        assert!(undo_mark_trusted(&real, None, false));
        assert!(!undo_mark_trusted(&real, None, true));
        assert!(!undo_mark_trusted(&real, Some(&real.digest_v1()), false));
    }

    #[test]
    fn a_scope_mismatch_refuses_with_both_clusters_named() {
        let reason = Scope {
            bootstrap: "a:9092".into(),
            cluster_id: "PROD".into(),
            ..Scope::default()
        }
        .check_undo_target(&Scope {
            bootstrap: "a:9092".into(),
            cluster_id: "DEV".into(),
            ..Scope::default()
        })
        .unwrap_err();
        let elsewhere = UndoPlan {
            same_scope: false,
            steps: Err(NotReversible(reason)),
            ..plan(Verdict::Clean)
        };
        let refusal = elsewhere.refusal(true, false).unwrap();
        assert!(
            refusal.contains("PROD") && refusal.contains("DEV"),
            "{refusal}"
        );
    }

    #[test]
    fn a_failed_undo_names_the_steps_it_applied_and_the_ones_it_did_not_run() {
        let step = |key: &str| Step::TopicConfig {
            topic: "t".into(),
            set: vec![(key.to_string(), "1".to_string())],
            delete: vec![],
        };
        let steps = vec![step("a"), step("b"), step("c")];
        let text = partial_failure(&steps, 1, "denied");
        assert!(text.contains("denied"), "{text}");
        assert!(
            text.contains("Applied before it and left in place (1 of 3)"),
            "{text}"
        );
        assert!(text.contains("Not run"), "{text}");
        let first = partial_failure(&steps, 0, "denied");
        assert!(first.contains("Nothing was changed"), "{first}");
        let last = partial_failure(&steps, 2, "denied");
        assert!(!last.contains("Not run"), "{last}");
    }

    #[test]
    fn undoing_a_topic_creation_cannot_be_redone() {
        let topic = |exists| State::Topic {
            topic: "t".into(),
            exists,
        };
        let (redo, limits) = redo_reversibility(&[topic(true)], &[topic(false)]);
        assert_eq!(redo, Reversibility::Irreversible);
        assert!(limits[0].contains("deleted"));
        let (redo, _) = redo_reversibility(&[topic(false)], &[topic(false)]);
        assert_eq!(redo, Reversibility::Reversible);
    }
}
