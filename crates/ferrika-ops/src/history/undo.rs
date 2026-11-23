//! Preparing an undo, shared by `ferrika undo` and the GUI: find the
//! operation, read the current state, compare, plan the steps and decide
//! whether undo may run. [`prepare_request`] makes the whole decision (the
//! "undone by" mark, the audit digest, the scope, the company policy and
//! the plan) and [`undo_eligibility`] the part a list can make without the
//! cluster, so every caller decides alike. The callers run the policy
//! gate, ask, record and run [`execute`].

use std::collections::HashMap;

use serde::Serialize;

use ferrika_core::error::{KafkaError, KafkaResult};
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

/// The audit log's `op_digest`s by operation id, as
/// `ferrika_governance::audit::op_digests` reads them.
pub type AuditDigests = HashMap<String, String>;

/// What the journal's "undone by" mark on an operation amounts to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "mark", rename_all = "snake_case")]
pub enum UndoneMark {
    /// The operation has no mark.
    None,
    /// The audit log vouches that `by` undid the operation (see
    /// [`undo_mark_trusted`]): it is not undone again.
    Trusted { by: String },
    /// The audit log does not vouch for the mark, so it is ignored and
    /// the current state decides.
    Ignored {
        by: String,
        /// How the undoing operation compares with the audit log; `None`
        /// when the journal has no such operation.
        audit: Option<DigestCheck>,
        /// The undoing operation's entry was edited (record it as
        /// `history.tampered` with [`TAMPERED_UNDO_LINK`]).
        tampered: bool,
    },
}

impl UndoneMark {
    /// Why the mark on operation `op` is ignored, or `None` when it is
    /// not: "the history says … undid …, but the audit log does not vouch
    /// for that (…); ignoring it, the current state decides".
    pub fn ignored_note(&self, op: &str) -> Option<String> {
        match self {
            Self::Ignored { by, audit, .. } => Some(format!(
                "the history says {by} undid {op}, but the audit log does not vouch for that \
                 ({}); ignoring it, the current state decides",
                audit.map_or("no such entry", DigestCheck::label)
            )),
            Self::None | Self::Trusted { .. } => None,
        }
    }

    /// The undoing operation whose edited entry is to be recorded as
    /// `history.tampered` (with [`TAMPERED_UNDO_LINK`]).
    pub fn tampered_undoer(&self) -> Option<&str> {
        match self {
            Self::Ignored {
                by, tampered: true, ..
            } => Some(by),
            _ => None,
        }
    }
}

/// The `history.tampered` outcome for an undoing operation whose entry
/// does not match the audit log (its undo link is ignored).
pub const TAMPERED_UNDO_LINK: &str =
    "the history entry does not match the audit log's op_digest; its undo link is ignored";

/// The `history.tampered` outcome for an operation whose undo is refused
/// because its entry does not match the audit log.
pub const TAMPERED_REFUSED: &str =
    "the history entry does not match the audit log's op_digest; undo refused";

/// Checks `op`'s "undone by" mark against the audit log (`audited`) with
/// [`undo_mark_trusted`]; `all` is the whole journal.
pub fn undone_mark(
    op: &Operation,
    all: &[Operation],
    audited: &AuditDigests,
    policy_active: bool,
) -> UndoneMark {
    let Some(by) = &op.undone_by else {
        return UndoneMark::None;
    };
    let undoer = all.iter().find(|o| &o.id == by);
    let audited_by = audited.get(by).map(String::as_str);
    if undoer.is_some_and(|undoer| undo_mark_trusted(undoer, audited_by, policy_active)) {
        return UndoneMark::Trusted { by: by.clone() };
    }
    let audit = undoer.map(|undoer| digest_check(undoer, audited_by));
    UndoneMark::Ignored {
        by: by.clone(),
        audit,
        tampered: audit == Some(DigestCheck::Mismatch),
    }
}

/// Whether the history of the cluster `current` points at lists `op`
/// ([`Scope::same_cluster`]). Looser than undo, which needs
/// [`Scope::check_undo_target`]: [`undo_eligibility`] says when a listed
/// operation would be refused.
pub fn listed_on(op: &Operation, current: &Scope) -> bool {
    op.begin.scope.same_cluster(current)
}

/// Whether the cluster in use is where an operation ran, as undo decides
/// ([`Scope::check_undo_target`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "scope", rename_all = "snake_case")]
pub enum ScopeCheck {
    Same,
    /// Undo would change another cluster (or one whose id could not be
    /// read to confirm it).
    Mismatch {
        /// Where the history says the operation ran.
        recorded: String,
        /// Where the caller is connected.
        current: String,
        /// The refusal, naming both.
        reason: String,
    },
}

impl ScopeCheck {
    /// Compares the scope `op` was recorded with and `current`.
    pub fn of(op: &Operation, current: &Scope) -> Self {
        match op.begin.scope.check_undo_target(current) {
            Ok(()) => Self::Same,
            Err(reason) => Self::Mismatch {
                recorded: op.begin.scope.describe(),
                current: current.describe(),
                reason,
            },
        }
    }

    pub fn is_same(&self) -> bool {
        matches!(self, Self::Same)
    }
}

/// The two places a legacy entry's undo asks the user to compare: its
/// digest (Ferrika 0.1) does not cover the scope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LegacyScopes {
    /// Where the history says the operation ran (possibly edited).
    pub recorded: String,
    /// Where the caller is connected.
    pub current: String,
}

/// Why an undo is refused.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "refusal", content = "reason", rename_all = "snake_case")]
pub enum UndoRefusal {
    /// The audit log vouches that `by` already undid the operation.
    AlreadyUndone { by: String },
    /// The operation failed: there is nothing to undo.
    Failed,
    /// Refused before the cluster is read ([`undo_eligibility`]): the
    /// entry was edited, the company policy needs an audit entry, or the
    /// cluster in use is another one.
    Audit(String),
    /// Refused by the plan ([`UndoPlan::refusal`]), which also checks the
    /// audit digest and scope first.
    Plan(String),
}

impl UndoRefusal {
    /// The reason alone, without the operation id.
    pub fn reason(&self) -> String {
        match self {
            Self::AlreadyUndone { by } => format!("it was already undone by {by}"),
            Self::Failed => "it failed, so there is nothing to undo".to_string(),
            Self::Audit(reason) | Self::Plan(reason) => reason.clone(),
        }
    }

    /// The error `ferrika undo` reports for operation `op`.
    pub fn message(&self, op: &str) -> String {
        match self {
            Self::AlreadyUndone { by } => {
                format!("Operation {op} was already undone by {by}; undo {by} to apply it again")
            }
            Self::Failed => format!("Operation {op} failed, so there is nothing to undo"),
            Self::Audit(reason) | Self::Plan(reason) => format!("Cannot undo {op}: {reason}"),
        }
    }
}

/// Why undo stops for an entry with `digest` before anything else is
/// checked, or `None`. [`UndoPlan::refusal`] and [`undo_eligibility`]
/// both start here.
fn digest_refusal(digest: DigestCheck, policy_active: bool) -> Option<String> {
    match digest {
        DigestCheck::Mismatch => Some(
            "the history entry does not match the audit log (it was edited); refusing to undo"
                .to_string(),
        ),
        DigestCheck::Missing if policy_active => Some(
            "the audit log has no entry for this operation, which the company policy requires before undo"
                .to_string(),
        ),
        _ => None,
    }
}

/// Whether undo is offered for an operation in a list.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum Eligibility {
    /// Not offered: the operation failed, cannot be undone by its kind,
    /// or was undone (and the audit log vouches for that).
    NotUndoable,
    /// Offered, but undo would be refused before the cluster is read.
    Refused { refusal: UndoRefusal },
    /// Undo can be planned (the plan may still refuse it, e.g. for
    /// drift); a legacy entry needs the cluster confirmed.
    Allowed { needs_legacy_confirmation: bool },
}

/// What a list knows about undoing one operation without contacting the
/// cluster: the rules [`prepare_request`] starts with, in the same order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct UndoEligibility {
    pub mark: UndoneMark,
    pub digest: DigestCheck,
    pub scope: ScopeCheck,
    pub status: Eligibility,
}

impl UndoEligibility {
    /// Whether undo is offered at all (refused or not).
    pub fn offered(&self) -> bool {
        !matches!(self.status, Eligibility::NotUndoable)
    }

    /// Why undo would be refused, when it would be.
    pub fn refusal(&self) -> Option<&UndoRefusal> {
        match &self.status {
            Eligibility::Refused { refusal } => Some(refusal),
            _ => None,
        }
    }
}

/// [`UndoEligibility`] of `op` (one of `all`, the whole journal) for a
/// caller connected to `current`, given the audit log's digests and
/// whether a company policy is installed. Reads nothing from the cluster:
/// the scope is compared by cluster id when `current` has one.
pub fn undo_eligibility(
    op: &Operation,
    all: &[Operation],
    current: &Scope,
    audited: &AuditDigests,
    policy_active: bool,
) -> UndoEligibility {
    let mark = undone_mark(op, all, audited, policy_active);
    let digest = digest_check(op, audited.get(&op.id).map(String::as_str));
    let scope = ScopeCheck::of(op, current);
    let status = if matches!(mark, UndoneMark::Trusted { .. })
        || op.ok() != Some(true)
        || !op.begin.reversibility.undoable()
    {
        Eligibility::NotUndoable
    } else if let Some(reason) = digest_refusal(digest, policy_active) {
        Eligibility::Refused {
            refusal: UndoRefusal::Audit(reason),
        }
    } else if let ScopeCheck::Mismatch { reason, .. } = &scope {
        Eligibility::Refused {
            refusal: UndoRefusal::Audit(reason.clone()),
        }
    } else {
        Eligibility::Allowed {
            needs_legacy_confirmation: digest == DigestCheck::Legacy,
        }
    };
    UndoEligibility {
        mark,
        digest,
        scope,
        status,
    }
}

/// What the caller asked for.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct UndoOptions {
    /// Overwrite drift and later changes (`--force`).
    pub force: bool,
    /// The user confirmed the cluster of a legacy entry
    /// (`--accept-legacy`).
    pub accept_legacy: bool,
}

/// The decision on one undo request, made the same way for every caller
/// (`ferrika undo`, the GUI): the "undone by" mark, the result, the audit
/// digest, the scope, the company policy and the plan. The caller still
/// runs the write gate per step, asks, records and runs [`execute_steps`].
#[derive(Debug, Clone)]
pub struct UndoRequest {
    pub op: String,
    pub mark: UndoneMark,
    pub digest: DigestCheck,
    pub scope: ScopeCheck,
    /// The plan; `None` when the request was refused before the cluster
    /// was read (already undone, or the operation failed).
    pub plan: Option<UndoPlan>,
    /// Why undo is refused with the options given.
    pub refusal: Option<UndoRefusal>,
    /// Why undo is refused even with `force` (`None`: forcing works).
    pub forced_refusal: Option<UndoRefusal>,
    /// Set for a legacy entry (Ferrika 0.1 digest): both places to compare.
    pub legacy: Option<LegacyScopes>,
    /// A legacy entry whose cluster the user has not confirmed: ask (or
    /// refuse with [`UndoRequest::legacy_refusal`]) before running.
    pub needs_legacy_confirmation: bool,
    /// The undo overwrites changes made since the operation (`force` with
    /// drift); the outcome says so.
    pub forced: bool,
}

impl UndoRequest {
    /// Whether undo was refused because the entry itself was edited:
    /// record it as `history.tampered` with [`TAMPERED_REFUSED`].
    pub fn op_tampered(&self) -> bool {
        self.digest == DigestCheck::Mismatch && matches!(self.refusal, Some(UndoRefusal::Plan(_)))
    }

    /// The refusal of a legacy entry whose cluster was not confirmed,
    /// ending with how to confirm it (`confirm`, e.g. "pass
    /// --accept-legacy"); `None` when no confirmation is needed.
    pub fn legacy_refusal(&self, confirm: &str) -> Option<String> {
        let legacy = self
            .legacy
            .as_ref()
            .filter(|_| self.needs_legacy_confirmation)?;
        Some(format!(
            "Cannot undo {}: its history entry has a legacy digest that does not cover the \
             cluster; check that {} is the right cluster and {confirm}",
            self.op, legacy.current
        ))
    }
}

/// Decides on undoing `op` (one of `all`, the whole journal) for a caller
/// connected to `current`: checks the "undone by" mark and the result,
/// then plans with [`prepare`] (reading the cluster) and applies
/// [`UndoPlan::refusal`] with the company policy (`policy_active`) and
/// `options`. `audited` holds the audit log's `op_digest`s by id.
#[allow(clippy::too_many_arguments)]
pub async fn prepare_request(
    op: &Operation,
    all: &[Operation],
    current: &Scope,
    audited: &AuditDigests,
    policy_active: bool,
    options: UndoOptions,
    config: &KafkaClientConfig,
    admin: &KafkaAdminClient,
) -> KafkaResult<UndoRequest> {
    let mut request = check_request(op, all, current, audited, policy_active);
    if request.refusal.is_some() {
        return Ok(request);
    }
    let plan = prepare(
        op,
        all,
        current,
        audited.get(&op.id).map(String::as_str),
        config,
        admin,
    )
    .await?;
    decide(&mut request, plan, policy_active, options);
    Ok(request)
}

/// The part of [`prepare_request`] before the cluster is read.
pub(crate) fn check_request(
    op: &Operation,
    all: &[Operation],
    current: &Scope,
    audited: &AuditDigests,
    policy_active: bool,
) -> UndoRequest {
    let eligibility = undo_eligibility(op, all, current, audited, policy_active);
    let legacy = (eligibility.digest == DigestCheck::Legacy).then(|| LegacyScopes {
        recorded: op.begin.scope.describe(),
        current: current.describe(),
    });
    let early = match &eligibility.mark {
        UndoneMark::Trusted { by } => Some(UndoRefusal::AlreadyUndone { by: by.clone() }),
        _ => (op.ok() == Some(false)).then_some(UndoRefusal::Failed),
    };
    UndoRequest {
        op: op.id.clone(),
        mark: eligibility.mark,
        digest: eligibility.digest,
        scope: eligibility.scope,
        plan: None,
        refusal: early.clone(),
        forced_refusal: early,
        legacy,
        needs_legacy_confirmation: false,
        forced: false,
    }
}

/// The part of [`prepare_request`] after the plan is made.
pub(crate) fn decide(
    request: &mut UndoRequest,
    plan: UndoPlan,
    policy_active: bool,
    options: UndoOptions,
) {
    request.refusal = plan
        .refusal(options.force, policy_active)
        .map(UndoRefusal::Plan);
    request.forced_refusal = plan.refusal(true, policy_active).map(UndoRefusal::Plan);
    request.needs_legacy_confirmation = request.legacy.is_some() && !options.accept_legacy;
    request.forced = options.force && matches!(plan.verdict, Verdict::Drift { .. });
    request.plan = Some(plan);
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
        if let Some(reason) = digest_refusal(self.digest, policy_active) {
            return Some(reason);
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
/// the outcome text, or the failure as text (see [`execute_steps`] for the
/// failure as data). The applied steps are not rolled back.
pub async fn execute(plan: &UndoPlan, target: &Target<'_>) -> Result<String, String> {
    execute_steps(plan, target).await.map_err(|e| e.to_string())
}

/// [`execute`] with the failure as an [`UndoFailure`] (boxed, it is large): which steps were
/// applied before the failing one (left in place), which were not run, and
/// the error.
pub async fn execute_steps(
    plan: &UndoPlan,
    target: &Target<'_>,
) -> Result<String, Box<UndoFailure>> {
    let steps = plan.runnable_steps();
    if steps.is_empty() {
        return Ok("already in its previous state; nothing changed".to_string());
    }
    for (ix, step) in steps.iter().enumerate() {
        if let Err(error) = apply::run(step, target).await {
            return Err(Box::new(UndoFailure::new(steps, ix, error)));
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

/// An undo that stopped at a failing step. The steps are their
/// [`Step::describe`] texts. Its `Display` is the outcome text `ferrika
/// undo` prints and records: the failed step and error, then the steps
/// applied before it and the steps not run.
#[derive(Debug)]
pub struct UndoFailure {
    /// The step that failed.
    pub failed: String,
    /// Steps applied before it, in order; their changes stay in place.
    pub applied: Vec<String>,
    /// Steps after it, never run.
    pub not_run: Vec<String>,
    /// Why the failed step failed.
    pub error: KafkaError,
}

impl UndoFailure {
    /// The failure of `steps[failed]` (from 0) with `error`.
    pub fn new(steps: &[Step], failed: usize, error: KafkaError) -> Self {
        let describe = |steps: &[Step]| steps.iter().map(Step::describe).collect::<Vec<_>>();
        let at = failed.min(steps.len());
        Self {
            failed: steps.get(failed).map(Step::describe).unwrap_or_default(),
            applied: describe(&steps[..at]),
            not_run: describe(steps.get(at + 1..).unwrap_or_default()),
            error,
        }
    }

    /// Whether any step changed the cluster before the failure.
    pub fn changed_anything(&self) -> bool {
        !self.applied.is_empty()
    }
}

impl std::fmt::Display for UndoFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.failed.is_empty() {
            return write!(f, "{}", self.error);
        }
        write!(f, "{}: {}", self.failed, self.error)?;
        if self.applied.is_empty() {
            write!(f, ". Nothing was changed")?;
        } else {
            let total = self.applied.len() + 1 + self.not_run.len();
            write!(
                f,
                ". Applied before it and left in place ({} of {total}): {}",
                self.applied.len(),
                self.applied.join("; ")
            )?;
        }
        if !self.not_run.is_empty() {
            write!(f, ". Not run: {}", self.not_run.join("; "))?;
        }
        Ok(())
    }
}

impl std::error::Error for UndoFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
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
        let denied = || KafkaError::admin("denied");
        let failure = UndoFailure::new(&steps, 1, denied());
        assert_eq!(failure.failed, steps[1].describe());
        assert_eq!(failure.applied, vec![steps[0].describe()]);
        assert_eq!(failure.not_run, vec![steps[2].describe()]);
        assert!(failure.changed_anything());
        assert!(failure.error.to_string().contains("denied"));
        let text = failure.to_string();
        assert_eq!(
            text,
            format!(
                "{}: {}. Applied before it and left in place (1 of 3): {}. Not run: {}",
                steps[1].describe(),
                denied(),
                steps[0].describe(),
                steps[2].describe()
            )
        );
        let first = UndoFailure::new(&steps, 0, denied());
        assert!(first.applied.is_empty() && first.not_run.len() == 2);
        assert!(!first.changed_anything());
        assert!(first.to_string().contains("Nothing was changed"));
        let last = UndoFailure::new(&steps, 2, denied());
        assert_eq!(last.applied.len(), 2);
        assert!(last.not_run.is_empty());
        assert!(!last.to_string().contains("Not run"));
    }

    fn digests(pairs: &[(&str, String)]) -> AuditDigests {
        pairs
            .iter()
            .map(|(id, digest)| (id.to_string(), digest.clone()))
            .collect()
    }

    fn here() -> Scope {
        Scope {
            bootstrap: "a:9092".into(),
            ..Scope::default()
        }
    }

    #[test]
    fn the_undone_mark_is_trusted_only_when_the_audit_log_vouches_for_it() {
        let mut target = op("A", "u", true);
        target.undone_by = Some("U".into());
        let undoer = op("U", "u", true);
        let all = vec![target.clone(), undoer.clone()];
        assert_eq!(
            undone_mark(&op("B", "u", true), &all, &AuditDigests::new(), false),
            UndoneMark::None
        );
        let vouched = digests(&[("U", undoer.digest())]);
        assert_eq!(
            undone_mark(&target, &all, &vouched, true),
            UndoneMark::Trusted { by: "U".into() }
        );
        let edited = digests(&[("U", "v2:00".to_string())]);
        let mark = undone_mark(&target, &all, &edited, false);
        assert_eq!(mark.tampered_undoer(), Some("U"));
        let note = mark.ignored_note("A").unwrap();
        assert!(
            note.starts_with("the history says U undid A, but the audit log does not vouch")
                && note.contains(DigestCheck::Mismatch.label()),
            "{note}"
        );
        // No undoer in the journal: ignored, nothing to record.
        let alone = undone_mark(&target, std::slice::from_ref(&target), &vouched, false);
        assert_eq!(alone.tampered_undoer(), None);
        assert!(alone.ignored_note("A").unwrap().contains("(no such entry)"));
        // Without an audit entry the mark stands unless a policy is installed.
        assert!(matches!(
            undone_mark(&target, &all, &AuditDigests::new(), false),
            UndoneMark::Trusted { .. }
        ));
        assert!(matches!(
            undone_mark(&target, &all, &AuditDigests::new(), true),
            UndoneMark::Ignored {
                tampered: false,
                ..
            }
        ));
    }

    #[test]
    fn eligibility_refuses_like_the_plan_without_reading_the_cluster() {
        let a = op("A", "u", true);
        let audited = digests(&[("A", a.digest())]);
        let all = vec![a.clone()];
        let ok = undo_eligibility(&a, &all, &here(), &audited, true);
        assert_eq!(
            ok.status,
            Eligibility::Allowed {
                needs_legacy_confirmation: false
            }
        );
        assert!(ok.offered() && ok.refusal().is_none());
        // Failed: not offered.
        let failed = op("F", "u", false);
        assert_eq!(
            undo_eligibility(&failed, &all, &here(), &audited, false).status,
            Eligibility::NotUndoable
        );
        // Edited: refused with the plan's wording.
        let edited = digests(&[("A", "v2:00".to_string())]);
        let refused = undo_eligibility(&a, &all, &here(), &edited, false);
        let plan_refusal = UndoPlan {
            digest: DigestCheck::Mismatch,
            ..plan(Verdict::Clean)
        }
        .refusal(true, false);
        assert_eq!(
            refused.refusal().map(UndoRefusal::reason),
            plan_refusal,
            "{refused:?}"
        );
        // No audit entry: refused only under a company policy.
        let none = AuditDigests::new();
        assert!(undo_eligibility(&a, &all, &here(), &none, false)
            .refusal()
            .is_none());
        assert!(undo_eligibility(&a, &all, &here(), &none, true)
            .refusal()
            .is_some());
        // Legacy: allowed with the cluster to confirm.
        let legacy = digests(&[("A", a.digest_v1())]);
        assert_eq!(
            undo_eligibility(&a, &all, &here(), &legacy, false).status,
            Eligibility::Allowed {
                needs_legacy_confirmation: true
            }
        );
    }

    #[test]
    fn a_listed_operation_whose_cluster_id_cannot_be_confirmed_would_be_refused() {
        let mut a = op("A", "u", true);
        a.begin.scope.cluster_id = "PROD".into();
        let audited = digests(&[("A", a.digest())]);
        // Same brokers, but the cluster id could not be read: listed, refused.
        let unknown = here();
        assert!(listed_on(&a, &unknown));
        let eligibility = undo_eligibility(&a, std::slice::from_ref(&a), &unknown, &audited, false);
        let ScopeCheck::Mismatch {
            recorded,
            current,
            reason,
        } = &eligibility.scope
        else {
            panic!("{eligibility:?}");
        };
        assert!(recorded.contains("PROD") && current.contains("a:9092"));
        assert_eq!(
            eligibility.refusal(),
            Some(&UndoRefusal::Audit(reason.clone()))
        );
        assert_eq!(
            Some(reason.clone()),
            a.begin.scope.check_undo_target(&unknown).err()
        );
        // Another cluster with the same brokers: not listed, refused too.
        let dev = Scope {
            cluster_id: "DEV".into(),
            ..here()
        };
        assert!(!listed_on(&a, &dev));
        let refusal = undo_eligibility(&a, std::slice::from_ref(&a), &dev, &audited, false)
            .refusal()
            .map(UndoRefusal::reason)
            .unwrap();
        assert!(
            refusal.contains("PROD") && refusal.contains("DEV"),
            "{refusal}"
        );
        let prod = Scope {
            cluster_id: "PROD".into(),
            ..here()
        };
        assert!(
            undo_eligibility(&a, std::slice::from_ref(&a), &prod, &audited, false)
                .scope
                .is_same()
        );
    }

    #[test]
    fn a_request_is_refused_early_when_already_undone_or_failed() {
        let mut target = op("A", "u", true);
        target.undone_by = Some("U".into());
        let undoer = op("U", "u", true);
        let all = vec![target.clone(), undoer.clone()];
        let audited = digests(&[("U", undoer.digest())]);
        let request = check_request(&target, &all, &here(), &audited, false);
        assert_eq!(
            request.refusal,
            Some(UndoRefusal::AlreadyUndone { by: "U".into() })
        );
        assert_eq!(
            request.refusal.unwrap().message("A"),
            "Operation A was already undone by U; undo U to apply it again"
        );
        let failed = op("F", "u", false);
        let request = check_request(&failed, &all, &here(), &audited, false);
        assert_eq!(request.refusal, Some(UndoRefusal::Failed));
        assert!(request.plan.is_none() && !request.op_tampered());
        assert_eq!(
            UndoRefusal::Failed.message("F"),
            "Operation F failed, so there is nothing to undo"
        );
        // An ignored mark does not refuse.
        let edited = digests(&[("U", "v2:00".to_string())]);
        let request = check_request(&target, &all, &here(), &edited, false);
        assert_eq!(request.refusal, None);
        assert_eq!(request.mark.tampered_undoer(), Some("U"));
    }

    #[test]
    fn the_request_applies_force_policy_and_the_legacy_confirmation() {
        let a = op("A", "u", true);
        let all = vec![a.clone()];
        let decided = |audited: &AuditDigests, plan: UndoPlan, policy, options| {
            let mut request = check_request(&a, &all, &here(), audited, policy);
            decide(&mut request, plan, policy, options);
            request
        };
        let force = UndoOptions {
            force: true,
            ..UndoOptions::default()
        };
        let audited = digests(&[("A", a.digest())]);
        let drift = || plan(Verdict::Drift { rows: vec![] });
        let plain = decided(&audited, drift(), false, UndoOptions::default());
        assert!(matches!(plain.refusal, Some(UndoRefusal::Plan(_))));
        assert_eq!(plain.forced_refusal, None);
        assert!(!plain.forced && !plain.needs_legacy_confirmation);
        assert_eq!(
            plain.refusal.unwrap().message("A"),
            format!("Cannot undo A: {}", drift().refusal(false, false).unwrap())
        );
        let forced = decided(&audited, drift(), false, force);
        assert_eq!(forced.refusal, None);
        assert!(forced.forced && forced.plan.is_some());
        // An edited entry: refused even when forced, recorded as tampered.
        let edited = digests(&[("A", "v2:00".to_string())]);
        let edited_plan = UndoPlan {
            digest: DigestCheck::Mismatch,
            ..plan(Verdict::Clean)
        };
        let tampered = decided(&edited, edited_plan, false, force);
        assert!(tampered.forced_refusal.is_some() && tampered.op_tampered());
        // Legacy: confirmation needed until accepted; both places named.
        let legacy = digests(&[("A", a.digest_v1())]);
        let legacy_plan = || UndoPlan {
            digest: DigestCheck::Legacy,
            ..plan(Verdict::Clean)
        };
        let ask = decided(&legacy, legacy_plan(), false, UndoOptions::default());
        assert_eq!(ask.refusal, None);
        assert!(ask.needs_legacy_confirmation);
        let scopes = ask.legacy.clone().unwrap();
        assert_eq!(scopes.recorded, a.begin.scope.describe());
        assert_eq!(scopes.current, here().describe());
        assert_eq!(
            ask.legacy_refusal("pass --accept-legacy").unwrap(),
            format!(
                "Cannot undo A: its history entry has a legacy digest that does not cover the \
                 cluster; check that {} is the right cluster and pass --accept-legacy",
                here().describe()
            )
        );
        let accept = UndoOptions {
            accept_legacy: true,
            ..UndoOptions::default()
        };
        let accepted = decided(&legacy, legacy_plan(), false, accept);
        assert!(!accepted.needs_legacy_confirmation);
        assert_eq!(accepted.legacy_refusal("confirm it"), None);
        // Under a policy, an operation the audit log does not know is refused.
        let unaudited_plan = UndoPlan {
            digest: DigestCheck::Missing,
            ..plan(Verdict::Clean)
        };
        let unaudited = decided(
            &AuditDigests::new(),
            unaudited_plan,
            true,
            UndoOptions::default(),
        );
        assert!(unaudited
            .refusal
            .unwrap()
            .reason()
            .contains("company policy"));
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
