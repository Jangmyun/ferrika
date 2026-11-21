//! Applies lint fixes for `ferrika lint --fix`: each one re-checked against
//! the cluster (it may have changed since the lint), then through the
//! company policy's write gate, then run with the existing admin and wire
//! calls. Each applied fix is one operation in the history journal
//! (`ferrika history`, `ferrika undo`), recorded with the state it replaced
//! like any other CLI change, and its outcome is in the audit log under the
//! same operation id.
//!
//! The GUI does not use this: it sends each fix as the matching
//! `UiToKafkaCommand`, which its own gate audits.

use std::collections::HashMap;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::time::Duration;

use crate::history::{capture, Change, Journal, Recorder};
use ferrika_core::error::{KafkaError, KafkaResult};
use ferrika_core::kafka::admin::{ConfigOrigin, KafkaAdminClient};
use ferrika_core::kafka::config::KafkaClientConfig;
use ferrika_core::kafka::wire::{self, ConfigChange};
use ferrika_core::model::cluster::ConsumerState;
use ferrika_core::model::history::{
    PartitionOffset, PartitionReplicas, Reversibility, Scope, State,
};
use ferrika_governance::policy::{
    authorize_write_with, blocked_entry, AuditMode, AuditSink, EffectivePolicy, WriteRefusal,
    WriteRequest,
};

use super::{lint, Fix, LintFinding, LintInput, LintOptions};

const TIMEOUT: Duration = Duration::from_secs(15);

/// Whether a fix is applied or only checked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyMode {
    Apply,
    /// Change nothing: config changes are sent with `validate_only`,
    /// replication changes are only planned.
    Validate,
}

pub(crate) type LocalBoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

/// Reads and changes the cluster for [`apply_fixes`]; a trait so the loop
/// can be tested without a cluster.
pub trait FixRunner {
    /// Re-reads into `input` what `fix` is about (topic configs and
    /// metadata, broker configs, the group), so the finding can be checked
    /// again before it is applied.
    fn refresh<'a>(
        &'a self,
        input: &'a mut LintInput,
        fix: &'a Fix,
    ) -> LocalBoxFuture<'a, KafkaResult<()>>;

    /// The history record of `fix`: the state it is about to change, read
    /// after [`FixRunner::refresh`] (from `input` where that holds it).
    fn change<'a>(&'a self, input: &'a LintInput, fix: &'a Fix) -> LocalBoxFuture<'a, Change>;

    /// Applies (or validates) `fix`; the message says what happened.
    fn run<'a>(&'a self, fix: &'a Fix, mode: ApplyMode) -> LocalBoxFuture<'a, KafkaResult<String>>;
}

/// The write gate and audit log the fixes go through.
pub struct FixGate<'a, S: AuditSink> {
    pub policy: &'a EffectivePolicy,
    pub sink: &'a S,
    pub profile: &'a str,
    pub bootstrap: &'a str,
    /// The caller's own refusal, e.g. a read-only profile.
    pub profile_check: &'a dyn Fn() -> Result<(), String>,
    /// Where applied fixes are recorded for `ferrika history` and
    /// `ferrika undo`.
    pub journal: &'a Journal,
    /// The cluster the fixes run on, for the record.
    pub scope: &'a Scope,
}

/// What became of one fix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Applied(String),
    /// Checked by the broker (or planned) without changing anything.
    Validated(String),
    Failed(String),
    /// The finding no longer holds, or its fix changed, since the lint.
    Changed,
    /// The cluster does not allow this change (`POLICY_VIOLATION` or
    /// `INVALID_CONFIG`, e.g. a managed service); skipped, not failed.
    NotAllowed(String),
    /// Refused by the company policy or the profile.
    Refused(String),
    /// The user said no.
    Declined,
    /// Not tried: an earlier fix found the audit log unwritable under
    /// `audit: required`, which refuses every later fix too.
    Stopped,
}

#[derive(Debug, Clone)]
pub struct FixResult {
    pub finding: LintFinding,
    pub outcome: Outcome,
}

/// One config value a fix changed, kept in lint's own record file
/// ([`save_undo`]); `ferrika undo` works from the history journal instead.
/// Only lint's own keys, none of them sensitive.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct UndoChange {
    /// `topic orders`, `broker 2` or `cluster default`.
    pub resource: String,
    pub key: String,
    /// The override (topic) or dynamic value (broker) before the fix;
    /// `None` when there was none.
    pub before: Option<String>,
    /// The value set; `None` when the fix removed it.
    pub after: Option<String>,
}

/// The undo record of one `lint --fix` run.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct UndoRecord {
    pub time_ms: i64,
    pub profile: String,
    pub changes: Vec<UndoChange>,
}

/// The finding for `rule` on `target` in `input` now, if any.
fn current_finding(
    input: &LintInput,
    opts: &LintOptions,
    finding: &LintFinding,
) -> Option<LintFinding> {
    let narrowed = LintOptions {
        env: opts.env,
        rules: vec![finding.rule.to_string()],
        skip: Vec::new(),
        topics: Vec::new(),
    };
    lint(input, &narrowed)
        .into_iter()
        .find(|f| f.target == finding.target)
}

/// The config values `fix` replaces, read from `input` before it runs.
pub(crate) fn undo_changes(input: &LintInput, fix: &Fix) -> Vec<UndoChange> {
    let (resource, entries, origin) = match fix {
        Fix::TopicConfig { topic, .. } => (
            format!("topic {topic}"),
            input.topic_configs.get(topic),
            ConfigOrigin::TopicOverride,
        ),
        Fix::BrokerConfig {
            broker: Some(id), ..
        } => (
            format!("broker {id}"),
            input.broker_configs.get(id),
            ConfigOrigin::BrokerDynamic,
        ),
        Fix::BrokerConfig { broker: None, .. } => (
            "cluster default".to_string(),
            input.broker_configs.values().next(),
            ConfigOrigin::BrokerDynamicDefault,
        ),
        _ => return Vec::new(),
    };
    fix.config_changes()
        .iter()
        .map(|change| {
            let (key, after) = match change {
                ConfigChange::Set(k, v) => (k, Some(v.clone())),
                ConfigChange::Delete(k) => (k, None),
            };
            let before = entries
                .and_then(|e| e.iter().find(|e| &e.name == key))
                .filter(|e| e.source == origin && !e.sensitive)
                .map(|e| e.value.clone());
            UndoChange {
                resource: resource.clone(),
                key: key.clone(),
                before,
                after,
            }
        })
        .collect()
}

/// Applies `plan` in order. Before each fix the cluster is re-read and the
/// rule run again: a finding that is gone or now suggests another fix is
/// skipped as [`Outcome::Changed`]. `confirm` is asked per fix (guided
/// fixes); with [`ApplyMode::Apply`] each fix then goes through the write
/// gate (recording the request), and its outcome is recorded as one more
/// audit line. Validation changes nothing and is neither gated nor audited,
/// like `topics config --dry-run`. `on_result` sees each result as it comes.
///
/// Returns the results and the undo record of what was changed.
#[allow(clippy::too_many_arguments)]
pub async fn apply_fixes<S: AuditSink>(
    runner: &impl FixRunner,
    gate: &FixGate<'_, S>,
    input: &mut LintInput,
    opts: &LintOptions,
    plan: &[LintFinding],
    mode: ApplyMode,
    mut confirm: impl FnMut(&LintFinding) -> bool,
    mut on_result: impl FnMut(&FixResult),
) -> (Vec<FixResult>, Vec<UndoChange>) {
    let mut results = Vec::with_capacity(plan.len());
    let mut undo = Vec::new();
    let mut stopped = false;
    for finding in plan {
        let outcome = if stopped {
            Outcome::Stopped
        } else {
            let (outcome, changes) =
                apply_one(runner, gate, input, opts, finding, mode, &mut confirm).await;
            if matches!(changes, Applied::AuditUnavailable) {
                stopped = true;
            }
            if let Applied::Changes(changes) = changes {
                undo.extend(changes);
            }
            outcome
        };
        let result = FixResult {
            finding: finding.clone(),
            outcome,
        };
        on_result(&result);
        results.push(result);
    }
    (results, undo)
}

enum Applied {
    Nothing,
    AuditUnavailable,
    Changes(Vec<UndoChange>),
}

async fn apply_one<S: AuditSink>(
    runner: &impl FixRunner,
    gate: &FixGate<'_, S>,
    input: &mut LintInput,
    opts: &LintOptions,
    finding: &LintFinding,
    mode: ApplyMode,
    confirm: &mut impl FnMut(&LintFinding) -> bool,
) -> (Outcome, Applied) {
    let fix = &finding.fix;
    let Some(action) = fix.policy_action() else {
        return (Outcome::Changed, Applied::Nothing);
    };
    if let Err(e) = runner.refresh(input, fix).await {
        return (
            Outcome::Failed(format!("could not re-read before the fix: {e}")),
            Applied::Nothing,
        );
    }
    if current_finding(input, opts, finding).is_none_or(|now| now.fix != *fix) {
        return (Outcome::Changed, Applied::Nothing);
    }
    if !confirm(finding) {
        return (Outcome::Declined, Applied::Nothing);
    }
    if mode == ApplyMode::Validate {
        return match runner.run(fix, mode).await {
            Ok(message) => (Outcome::Validated(message), Applied::Nothing),
            Err(e) => (Outcome::Failed(e.to_string()), Applied::Nothing),
        };
    }

    let label = format!("lint fix {}", finding.rule);
    let target = fix.audit_target();
    // Made before the gate so the request entry names the operation the
    // journal records, as `main` does for the other CLI changes.
    let op = ferrika_core::model::history::new_op_id(ferrika_core::kafka::clock::now_ms());
    let request = WriteRequest {
        source: "cli",
        action,
        label: &label,
        target: &target,
        profile: gate.profile,
        bootstrap: gate.bootstrap,
        actor: None,
        op: Some(&op),
    };
    match authorize_write_with(
        gate.policy,
        gate.sink,
        &request,
        AuditMode::RecordRequest,
        gate.profile_check,
    ) {
        Ok(()) => {}
        Err(refusal @ WriteRefusal::AuditUnavailable(_)) => {
            return (
                Outcome::Refused(refusal.to_string()),
                Applied::AuditUnavailable,
            );
        }
        Err(refusal) => return (Outcome::Refused(refusal.to_string()), Applied::Nothing),
    }

    let changes = undo_changes(input, fix);
    let begin = runner.change(input, fix).await.into_begin(
        "cli",
        gate.scope.clone(),
        label.clone(),
        target.clone(),
        None,
    );
    let recorder = Recorder::start_with_id(gate.journal.clone(), op.clone(), begin);
    let (outcome, ok, text) = match runner.run(fix, mode).await {
        Ok(message) => {
            let text = format!("ok: {message}");
            (Outcome::Applied(message), true, text)
        }
        Err(e) if not_allowed(fix, &e.to_string()) => {
            let text = format!("cannot be changed on this cluster: {e}");
            (Outcome::NotAllowed(text.clone()), false, text)
        }
        Err(e) => (Outcome::Failed(e.to_string()), false, e.to_string()),
    };
    // The request line is already written; this one records the outcome.
    let profile = if gate.profile.is_empty() {
        gate.bootstrap
    } else {
        gate.profile
    };
    let mut entry = blocked_entry("cli", profile, &target, &text);
    entry.action = label;
    entry.ok = ok;
    // The digest ties the journal line to this entry (`op_digest`).
    if let Some(digest) = recorder.finish(ok, &text, None) {
        entry.op = op;
        entry.op_digest = digest;
    }
    if let Err(e) = gate.sink.record(&entry) {
        tracing::warn!(%e, "Could not write the audit log");
    }
    let applied = if ok {
        Applied::Changes(changes)
    } else {
        Applied::Nothing
    };
    (outcome, applied)
}

/// The history record of `fix` from `input` as [`FixRunner::refresh`] left
/// it, for the fixes whose before-state `input` holds; `None` for the ones
/// that need a read of their own (the replication plan, a group's
/// offsets). Mirrors what the GUI records for the same change.
pub(crate) fn change_from_input(input: &LintInput, fix: &Fix) -> Option<Change> {
    let config = |state: KafkaResult<State>, changes: &[ConfigChange]| {
        let captured = state.map(|before| {
            let after = capture::config_after(&before, changes);
            (vec![before], vec![after])
        });
        Change::captured(Reversibility::Reversible, captured).sensitive_limits()
    };
    Some(match fix {
        Fix::TopicConfig { topic, changes } => config(
            input
                .topic_configs
                .get(topic)
                .map(|entries| {
                    capture::topic_config_of(topic, entries, &capture::change_keys(changes))
                })
                .ok_or_else(|| KafkaError::admin(format!("the config of {topic} was not read"))),
            changes,
        ),
        Fix::BrokerConfig {
            broker: Some(id),
            changes,
        } => config(
            input
                .broker_configs
                .get(id)
                .map(|entries| capture::broker_config_of(*id, entries, &capture::change_keys(changes)))
                .ok_or_else(|| KafkaError::admin(format!("the config of broker {id} was not read"))),
            changes,
        ),
        Fix::BrokerConfig { broker: None, .. } => Change::irreversible(
            Reversibility::Irreversible,
            "the cluster-wide default's previous values cannot be read (the default broker resource cannot be described), so undo cannot restore them",
        ),
        Fix::ElectLeaders { .. } => Change::irreversible(
            Reversibility::NotApplicable,
            "leadership is transient; nothing to undo",
        ),
        Fix::AddPartitions { topic, .. } => {
            let change =
                Change::irreversible(Reversibility::Irreversible, "Kafka cannot remove partitions");
            match input.data.topics.iter().find(|t| &t.name == topic) {
                Some(t) => change.info(format!("it had {} partition(s)", t.partition_count)),
                None => change,
            }
        }
        Fix::Manual { .. } | Fix::None { .. } => Change::irreversible(
            Reversibility::NotApplicable,
            "this fix is not run by Ferrika",
        ),
        Fix::Replication { .. } | Fix::DeleteGroup { .. } => return None,
    })
}

/// Whether the broker refused a config change because the cluster does not
/// allow it, rather than because something went wrong.
fn not_allowed(fix: &Fix, error: &str) -> bool {
    !fix.config_changes().is_empty()
        && (error.contains("POLICY_VIOLATION") || error.contains("INVALID_CONFIG"))
}

/// Writes `undo` to `~/.ferrika/lint/<time>.json` (next to
/// `profiles.json`) and returns the path.
pub fn save_undo(profile: &str, undo: Vec<UndoChange>) -> std::io::Result<PathBuf> {
    let record = UndoRecord {
        time_ms: ferrika_governance::audit::now_ms(),
        profile: profile.to_string(),
        changes: undo,
    };
    let dir = undo_dir();
    std::fs::create_dir_all(&dir)?;
    let stamp = jiff::ZonedDateTime::now().strftime("%Y%m%d-%H%M%S");
    let path = dir.join(format!("{stamp}.json"));
    let json = serde_json::to_string_pretty(&record).map_err(std::io::Error::other)?;
    std::fs::write(&path, json)?;
    Ok(path)
}

/// Where `lint --fix` keeps its undo records.
pub(crate) fn undo_dir() -> PathBuf {
    ferrika_core::kafka::profile::ProfileStore::default_path().with_file_name("lint")
}

/// Runs fixes against the cluster with the existing admin and wire calls.
pub struct ClusterRunner<'a> {
    pub admin: &'a KafkaAdminClient,
    pub config: &'a KafkaClientConfig,
    /// `host:port` of each broker, for per-broker config changes.
    pub brokers: HashMap<i32, String>,
}

impl<'a> ClusterRunner<'a> {
    pub fn new(
        admin: &'a KafkaAdminClient,
        config: &'a KafkaClientConfig,
        input: &LintInput,
    ) -> Self {
        Self {
            admin,
            config,
            brokers: input
                .data
                .brokers
                .iter()
                .map(|b| (b.id, format!("{}:{}", b.host, b.port)))
                .collect(),
        }
    }

    fn refresh_topic(&self, input: &mut LintInput, topic: &str) -> KafkaResult<()> {
        let summary = self.admin.describe_topic(topic, Some(TIMEOUT))?;
        match input.data.topics.iter_mut().find(|t| t.name == topic) {
            Some(t) => *t = summary,
            None => input.data.topics.push(summary),
        }
        Ok(())
    }

    async fn reread(&self, input: &mut LintInput, fix: &Fix) -> KafkaResult<()> {
        match fix {
            Fix::TopicConfig { topic, .. } => {
                self.refresh_topic(input, topic)?;
                let entries = self
                    .admin
                    .describe_topic_configs(topic, Some(TIMEOUT))
                    .await?;
                input.topic_configs.insert(topic.clone(), entries);
                // T013 compares with the source topic's retention.
                if let Some(source) = crate::dlq::infer_original_topic_from_dlq_name(topic) {
                    if let Ok(entries) = self
                        .admin
                        .describe_topic_configs(&source, Some(TIMEOUT))
                        .await
                    {
                        input.topic_configs.insert(source, entries);
                    }
                }
            }
            Fix::BrokerConfig { .. } => {
                // B001, B004 and B006 compare brokers with each other.
                let ids: Vec<i32> = input.broker_configs.keys().copied().collect();
                for id in ids {
                    let entries = self
                        .admin
                        .describe_broker_configs(id, Some(TIMEOUT))
                        .await?;
                    input.broker_configs.insert(id, entries);
                }
            }
            Fix::Replication { topic, .. } | Fix::AddPartitions { topic, .. } => {
                self.refresh_topic(input, topic)?;
            }
            Fix::ElectLeaders { partitions } => {
                let mut topics: Vec<&str> = partitions.iter().map(|(t, _)| t.as_str()).collect();
                topics.dedup();
                for topic in topics {
                    self.refresh_topic(input, topic)?;
                }
            }
            Fix::DeleteGroup { group } => {
                let listing = self
                    .admin
                    .list_consumer_group_listings(Some(TIMEOUT))?
                    .into_iter()
                    .find(|g| &g.name == group);
                match listing {
                    Some(g) => {
                        // Read the committed offsets again: a group whose
                        // OffsetFetch failed during the scan (a coordinator
                        // loading after a restart) looked like one that
                        // committed nothing, and its delete cannot be undone.
                        let offsets = ferrika_core::kafka::wire::fetch_group_offsets(
                            self.config,
                            std::slice::from_ref(group),
                        )?;
                        let Some(positions) = offsets.get(group) else {
                            return Err(KafkaError::admin(format!(
                                "cannot read the committed offsets of group '{group}'"
                            )));
                        };
                        let partitions =
                            crate::group_lag::group_partition_lags(positions, &input.data.topics);
                        if let Some(summary) = input
                            .data
                            .consumers
                            .iter_mut()
                            .find(|c| &c.group_id == group)
                        {
                            summary.active_members = g.members.len();
                            summary.state = ConsumerState::from_broker_state(&g.state);
                            summary.partitions = partitions;
                        }
                    }
                    None => input.data.consumers.retain(|c| &c.group_id != group),
                }
            }
            Fix::Manual { .. } | Fix::None { .. } => {}
        }
        Ok(())
    }

    /// The history record of `fix`, as the GUI records the same change.
    async fn change_of(&self, input: &LintInput, fix: &Fix) -> Change {
        if let Some(change) = change_from_input(input, fix) {
            return change;
        }
        match fix {
            Fix::Replication { topic, target } => {
                let captured = ferrika_core::kafka::reassign::plan_replication_change(
                    self.admin, topic, *target,
                )
                .and_then(|plan| {
                    let partitions: Vec<i32> = plan.iter().map(|a| a.partition).collect();
                    let before = capture::replicas(self.admin, topic, Some(&partitions))?;
                    let after = State::Replicas {
                        partitions: plan
                            .iter()
                            .map(|a| PartitionReplicas {
                                topic: a.topic.clone(),
                                partition: a.partition,
                                replicas: a.replicas.clone(),
                            })
                            .collect(),
                    };
                    Ok((vec![before], vec![after]))
                });
                Change::captured(Reversibility::Reversible, captured)
            }
            Fix::DeleteGroup { group } => {
                let captured = capture::group_committed(self.config, group, None)
                    .await
                    .map(|before| {
                        let after = cleared_offsets(&before);
                        (vec![before], vec![after])
                    });
                Change::captured(Reversibility::Partial, captured).limit(
                    "undo commits the group's offsets again; its members and their metadata are not restored",
                )
            }
            _ => Change::irreversible(
                Reversibility::NotApplicable,
                "this fix is not run by Ferrika",
            ),
        }
    }

    async fn execute(&self, fix: &Fix, mode: ApplyMode) -> KafkaResult<String> {
        let validate = mode == ApplyMode::Validate;
        let verb = if validate { "valid" } else { "applied" };
        match fix {
            Fix::TopicConfig { topic, changes } => {
                wire::incremental_alter_topic_config(self.config, topic, changes, validate)?;
                Ok(format!("{verb}: {}", wire::change_keys(changes)))
            }
            Fix::BrokerConfig {
                broker: Some(id),
                changes,
            } => {
                let addr = self
                    .brokers
                    .get(id)
                    .ok_or_else(|| KafkaError::Admin(format!("broker {id} is not known")))?;
                wire::incremental_alter_broker_config(self.config, *id, addr, changes, validate)?;
                Ok(format!("{verb}: {}", wire::change_keys(changes)))
            }
            Fix::BrokerConfig {
                broker: None,
                changes,
            } => {
                wire::incremental_alter_cluster_broker_default(self.config, changes, validate)?;
                Ok(format!("{verb}: {}", wire::change_keys(changes)))
            }
            Fix::Replication { topic, target } => {
                let plan = ferrika_core::kafka::reassign::plan_replication_change(
                    self.admin, topic, *target,
                )?;
                if plan.is_empty() {
                    return Ok(format!("already at replication {target}"));
                }
                if validate {
                    let moves: Vec<String> = plan
                        .iter()
                        .map(|a| format!("{} -> {:?}", a.partition, a.replicas))
                        .collect();
                    return Ok(format!(
                        "would reassign {} partition(s): {}",
                        plan.len(),
                        moves.join(", ")
                    ));
                }
                let (results, _) = wire::reassign_or_set_replication(self.config, &plan)?;
                let failed: Vec<String> = results
                    .iter()
                    .filter_map(|r| r.error.as_ref().map(|e| format!("{}: {e}", r.partition)))
                    .collect();
                if failed.is_empty() {
                    Ok(format!(
                        "reassignment started for {} partition(s); `ferrika topics describe {topic}` shows progress",
                        results.len()
                    ))
                } else {
                    Err(KafkaError::Check(format!(
                        "{} partition(s) were not reassigned ({})",
                        failed.len(),
                        failed.join("; ")
                    )))
                }
            }
            Fix::ElectLeaders { partitions } => {
                if validate {
                    return Ok(format!(
                        "would elect the preferred leader of {} partition(s)",
                        partitions.len()
                    ));
                }
                let outcome = wire::elect_preferred_leaders(self.config, Some(partitions))?;
                if outcome.failed.is_empty() {
                    Ok(format!(
                        "moved {} partition(s) to the preferred leader",
                        outcome.elected.len()
                    ))
                } else {
                    let failed: Vec<String> = outcome
                        .failed
                        .iter()
                        .map(|(t, p, why)| format!("{t}-{p}: {why}"))
                        .collect();
                    Err(KafkaError::Check(format!(
                        "moved {}, failed {}",
                        outcome.elected.len(),
                        failed.join("; ")
                    )))
                }
            }
            Fix::AddPartitions { topic, total } => {
                if validate {
                    return Ok(format!("would raise {topic} to {total} partitions"));
                }
                self.admin
                    .add_partitions(topic, *total, Some(TIMEOUT))
                    .await?;
                Ok(format!("{topic} now has {total} partitions"))
            }
            Fix::DeleteGroup { group } => {
                if validate {
                    return Ok(format!("would delete group {group}"));
                }
                self.admin
                    .delete_consumer_group(group, Some(TIMEOUT))
                    .await?;
                Ok(format!("deleted group {group}"))
            }
            Fix::Manual { .. } | Fix::None { .. } => Err(KafkaError::Check(
                "this fix is not run by Ferrika".to_string(),
            )),
        }
    }
}

impl FixRunner for ClusterRunner<'_> {
    fn refresh<'a>(
        &'a self,
        input: &'a mut LintInput,
        fix: &'a Fix,
    ) -> LocalBoxFuture<'a, KafkaResult<()>> {
        Box::pin(self.reread(input, fix))
    }

    fn change<'a>(&'a self, input: &'a LintInput, fix: &'a Fix) -> LocalBoxFuture<'a, Change> {
        Box::pin(self.change_of(input, fix))
    }

    fn run<'a>(&'a self, fix: &'a Fix, mode: ApplyMode) -> LocalBoxFuture<'a, KafkaResult<String>> {
        Box::pin(self.execute(fix, mode))
    }
}

/// The same partitions with no committed offset.
fn cleared_offsets(state: &State) -> State {
    match state {
        State::GroupOffsets { group, offsets } => State::GroupOffsets {
            group: group.clone(),
            offsets: offsets
                .iter()
                .map(|p| PartitionOffset {
                    offset: None,
                    ..p.clone()
                })
                .collect(),
        },
        other => other.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lint::{Env, LintOptions};
    use ferrika_core::kafka::admin::ConfigEntryView;
    use ferrika_core::model::cluster::{
        BrokerInfo, ClusterSnapshot, PartitionDetail, TopicSummary,
    };
    use ferrika_core::model::policy::AuditRule;
    use ferrika_governance::audit::AuditEntry;
    use ferrika_governance::policy::PolicyStatus;
    use std::cell::{Cell, RefCell};
    use std::collections::BTreeMap;

    struct FakeRunner {
        runs: RefCell<Vec<Fix>>,
        /// Applied to the input on refresh, to simulate a change since the lint.
        on_refresh: Box<dyn Fn(&mut LintInput)>,
        fail: bool,
    }

    impl FakeRunner {
        fn new() -> Self {
            Self {
                runs: RefCell::new(Vec::new()),
                on_refresh: Box::new(|_| {}),
                fail: false,
            }
        }
    }

    impl FixRunner for FakeRunner {
        fn refresh<'a>(
            &'a self,
            input: &'a mut LintInput,
            _fix: &'a Fix,
        ) -> LocalBoxFuture<'a, KafkaResult<()>> {
            (self.on_refresh)(input);
            Box::pin(async { Ok(()) })
        }

        fn change<'a>(&'a self, input: &'a LintInput, fix: &'a Fix) -> LocalBoxFuture<'a, Change> {
            let change = change_from_input(input, fix).unwrap_or_else(|| {
                Change::irreversible(Reversibility::NotApplicable, "not read by the fake")
            });
            Box::pin(async move { change })
        }

        fn run<'a>(
            &'a self,
            fix: &'a Fix,
            _mode: ApplyMode,
        ) -> LocalBoxFuture<'a, KafkaResult<String>> {
            self.runs.borrow_mut().push(fix.clone());
            let fail = self.fail;
            Box::pin(async move {
                if fail {
                    Err(KafkaError::Admin("broker said no".into()))
                } else {
                    Ok("done".into())
                }
            })
        }
    }

    struct FakeLog {
        writable: bool,
        entries: RefCell<Vec<AuditEntry>>,
        probes: Cell<usize>,
    }

    impl FakeLog {
        fn new(writable: bool) -> Self {
            Self {
                writable,
                entries: RefCell::new(Vec::new()),
                probes: Cell::new(0),
            }
        }
    }

    impl AuditSink for FakeLog {
        fn record(&self, entry: &AuditEntry) -> std::io::Result<()> {
            if !self.writable {
                return Err(std::io::Error::other("read-only folder"));
            }
            self.entries.borrow_mut().push(entry.clone());
            Ok(())
        }

        fn probe(&self) -> std::io::Result<()> {
            self.probes.set(self.probes.get() + 1);
            Ok(())
        }
    }

    fn policy(forbid: &[&str], audit_required: bool) -> EffectivePolicy {
        let mut p = EffectivePolicy::absent(std::env::temp_dir());
        p.status = PolicyStatus::Verified {
            issuer: "test".into(),
            key_id: "k".into(),
        };
        p.rules.forbid = forbid.iter().map(|s| s.to_string()).collect();
        if audit_required {
            p.rules.audit = AuditRule::Required;
        }
        p
    }

    fn entry(name: &str, value: &str, source: ConfigOrigin) -> ConfigEntryView {
        ConfigEntryView {
            name: name.into(),
            value: value.into(),
            source,
            read_only: false,
            sensitive: false,
        }
    }

    /// Three brokers; `orders` has RF 1 (T001), `pay` has min.isr 3 over RF 2 (T002).
    fn input() -> LintInput {
        let mut data = ClusterSnapshot::empty("prod", "b1:9092");
        data.brokers = (1..=3)
            .map(|id| BrokerInfo {
                id,
                host: format!("b{id}"),
                port: 9092,
                is_controller: id == 1,
                partition_leaders: 0,
                replicas_hosted: 0,
                is_online: true,
                log_dir_bytes: None,
                log_dirs: vec![],
            })
            .collect();
        let topic = |name: &str, rf: i32| TopicSummary {
            name: name.into(),
            partition_count: 1,
            replication_factor: rf as usize,
            total_messages: 0,
            cleanup_policy: "delete".into(),
            is_internal: false,
            size_bytes: None,
            msg_rate: None,
            partitions: vec![PartitionDetail {
                partition_id: 0,
                leader: 1,
                replicas: (1..=rf).collect(),
                isr: (1..=rf).collect(),
                high_watermark: 0,
                low_watermark: 0,
            }],
        };
        data.topics = vec![topic("orders", 1), topic("pay", 2)];
        LintInput {
            data,
            topic_configs: [(
                "pay".to_string(),
                vec![entry(
                    "min.insync.replicas",
                    "3",
                    ConfigOrigin::TopicOverride,
                )],
            )]
            .into_iter()
            .collect(),
            broker_configs: BTreeMap::new(),
            skipped: vec![],
            group_offsets: true,
        }
    }

    fn opts() -> LintOptions {
        LintOptions {
            env: Env::Prod,
            ..LintOptions::default()
        }
    }

    /// A journal in a folder of its own, removed when dropped.
    struct TempJournal {
        dir: PathBuf,
        journal: Journal,
    }

    impl TempJournal {
        fn new() -> Self {
            static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
            let dir = std::env::temp_dir().join(format!(
                "ferrika-lint-history-{}-{}-{}",
                std::process::id(),
                ferrika_core::kafka::clock::now_ms(),
                NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            ));
            let journal = Journal::at(dir.join("history.jsonl"));
            Self { dir, journal }
        }
    }

    impl Drop for TempJournal {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    fn scope() -> Scope {
        Scope {
            profile: "prod".into(),
            bootstrap: "b1:9092".into(),
            cluster_id: "c1".into(),
            ..Scope::default()
        }
    }

    fn run(
        runner: &FakeRunner,
        policy: &EffectivePolicy,
        log: &FakeLog,
        read_only: bool,
        mode: ApplyMode,
    ) -> (Vec<FixResult>, Vec<UndoChange>) {
        run_in(
            &TempJournal::new().journal,
            runner,
            policy,
            log,
            read_only,
            mode,
        )
    }

    fn run_in(
        journal: &Journal,
        runner: &FakeRunner,
        policy: &EffectivePolicy,
        log: &FakeLog,
        read_only: bool,
        mode: ApplyMode,
    ) -> (Vec<FixResult>, Vec<UndoChange>) {
        let mut input = input();
        let plan = lint(&input, &opts());
        assert_eq!(
            plan.iter().map(|f| f.rule).collect::<Vec<_>>(),
            vec!["T001", "T002"]
        );
        let check = move || {
            if read_only {
                Err("Profile 'prod' is read-only".to_string())
            } else {
                Ok(())
            }
        };
        let gate = FixGate {
            policy,
            sink: log,
            profile: "prod",
            bootstrap: "b1:9092",
            profile_check: &check,
            journal,
            scope: &scope(),
        };
        block_on(apply_fixes(
            runner,
            &gate,
            &mut input,
            &opts(),
            &plan,
            mode,
            |_| true,
            |_| {},
        ))
    }

    fn block_on<F: Future>(future: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(future)
    }

    fn outcomes(results: &[FixResult]) -> Vec<(&str, &Outcome)> {
        results
            .iter()
            .map(|r| (r.finding.rule, &r.outcome))
            .collect()
    }

    #[test]
    fn forbidden_config_change_is_refused_and_recorded_while_others_run() {
        let runner = FakeRunner::new();
        let log = FakeLog::new(true);
        let (results, undo) = run(
            &runner,
            &policy(&["config.change"], false),
            &log,
            false,
            ApplyMode::Apply,
        );
        assert!(
            matches!(results[0].outcome, Outcome::Applied(_)),
            "{results:?}"
        );
        assert!(matches!(&results[1].outcome, Outcome::Refused(m) if m.contains("config.change")));
        assert_eq!(runner.runs.borrow().len(), 1, "only T001 ran");
        let entries = log.entries.borrow();
        let actions: Vec<(&str, bool)> =
            entries.iter().map(|e| (e.action.as_str(), e.ok)).collect();
        assert_eq!(
            actions,
            vec![
                ("lint fix T001", true), // request
                ("lint fix T001", true), // outcome
                ("policy.blocked", false),
            ]
        );
        assert_eq!(entries[0].outcome, "requested");
        assert_eq!(entries[1].outcome, "ok: done");
        assert_eq!(
            entries[2].target,
            "lint fix T002 topic pay: min.insync.replicas (set)"
        );
        assert!(undo.is_empty(), "replication is not a config change");
    }

    #[test]
    fn read_only_profile_refuses_every_fix_and_records_the_requests() {
        let runner = FakeRunner::new();
        let log = FakeLog::new(true);
        let (results, _) = run(&runner, &policy(&[], false), &log, true, ApplyMode::Apply);
        assert!(results
            .iter()
            .all(|r| matches!(&r.outcome, Outcome::Refused(m) if m.contains("read-only"))));
        assert!(runner.runs.borrow().is_empty());
        let entries = log.entries.borrow();
        assert_eq!(entries.len(), 2);
        assert!(entries
            .iter()
            .all(|e| !e.ok && e.action.starts_with("lint fix")));
    }

    #[test]
    fn unwritable_audit_log_under_required_stops_before_any_change() {
        let runner = FakeRunner::new();
        let log = FakeLog::new(false);
        let (results, _) = run(&runner, &policy(&[], true), &log, false, ApplyMode::Apply);
        assert!(
            matches!(&results[0].outcome, Outcome::Refused(m) if m.contains("auditing is required"))
        );
        assert_eq!(results[1].outcome, Outcome::Stopped);
        assert!(runner.runs.borrow().is_empty());
    }

    #[test]
    fn validation_is_neither_gated_nor_audited() {
        let runner = FakeRunner::new();
        let log = FakeLog::new(true);
        let (results, undo) = run(
            &runner,
            &policy(&["*"], true),
            &log,
            true,
            ApplyMode::Validate,
        );
        assert!(results
            .iter()
            .all(|r| matches!(r.outcome, Outcome::Validated(_))));
        assert!(log.entries.borrow().is_empty());
        assert!(undo.is_empty());
    }

    #[test]
    fn failures_are_recorded_and_the_rest_continue() {
        let mut runner = FakeRunner::new();
        runner.fail = true;
        let log = FakeLog::new(true);
        let (results, undo) = run(&runner, &policy(&[], false), &log, false, ApplyMode::Apply);
        assert!(results
            .iter()
            .all(|r| matches!(r.outcome, Outcome::Failed(_))));
        assert_eq!(runner.runs.borrow().len(), 2);
        let entries = log.entries.borrow();
        assert_eq!(entries.len(), 4);
        assert!(!entries[1].ok && entries[1].outcome.contains("broker said no"));
        assert!(undo.is_empty());
    }

    #[test]
    fn config_changes_the_cluster_does_not_allow_are_skipped() {
        let fix = Fix::TopicConfig {
            topic: "t".into(),
            changes: vec![ConfigChange::Set("segment.bytes".into(), "1".into())],
        };
        assert!(not_allowed(
            &fix,
            "Admin error: POLICY_VIOLATION (code 44): no"
        ));
        assert!(!not_allowed(&fix, "REQUEST_TIMED_OUT (code 7)"));
        let replication = Fix::Replication {
            topic: "t".into(),
            target: 3,
        };
        assert!(!not_allowed(&replication, "INVALID_CONFIG"));
    }

    #[test]
    fn findings_changed_since_the_lint_are_skipped() {
        let mut runner = FakeRunner::new();
        // Someone fixed pay and raised orders to RF 2 meanwhile (T001 now
        // suggests the same fix, but the topic summary says RF 2: gone).
        runner.on_refresh = Box::new(|input: &mut LintInput| {
            input.topic_configs.insert(
                "pay".into(),
                vec![entry(
                    "min.insync.replicas",
                    "1",
                    ConfigOrigin::TopicOverride,
                )],
            );
            input.data.topics[0].replication_factor = 2;
        });
        let log = FakeLog::new(true);
        let (results, _) = run(&runner, &policy(&[], false), &log, false, ApplyMode::Apply);
        assert_eq!(
            outcomes(&results),
            vec![("T001", &Outcome::Changed), ("T002", &Outcome::Changed)]
        );
        assert!(runner.runs.borrow().is_empty());
        assert!(log.entries.borrow().is_empty());
    }

    #[test]
    fn applied_config_changes_leave_an_undo_record() {
        let runner = FakeRunner::new();
        let log = FakeLog::new(true);
        let (results, undo) = run(&runner, &policy(&[], false), &log, false, ApplyMode::Apply);
        assert!(results
            .iter()
            .all(|r| matches!(r.outcome, Outcome::Applied(_))));
        assert_eq!(
            undo,
            vec![UndoChange {
                resource: "topic pay".into(),
                key: "min.insync.replicas".into(),
                before: Some("3".into()),
                after: Some("1".into()),
            }]
        );
    }

    #[test]
    fn undo_before_is_only_the_resources_own_value() {
        let mut input = input();
        input.topic_configs.insert(
            "orders".into(),
            vec![entry("segment.bytes", "1073741824", ConfigOrigin::Default)],
        );
        let fix = Fix::TopicConfig {
            topic: "orders".into(),
            changes: vec![ConfigChange::Delete("segment.bytes".into())],
        };
        assert_eq!(undo_changes(&input, &fix)[0].before, None);
        input.broker_configs.insert(
            2,
            vec![entry(
                "unclean.leader.election.enable",
                "true",
                ConfigOrigin::BrokerDynamicDefault,
            )],
        );
        let cluster = Fix::BrokerConfig {
            broker: None,
            changes: vec![ConfigChange::Set(
                "unclean.leader.election.enable".into(),
                "false".into(),
            )],
        };
        assert_eq!(
            undo_changes(&input, &cluster),
            vec![UndoChange {
                resource: "cluster default".into(),
                key: "unclean.leader.election.enable".into(),
                before: Some("true".into()),
                after: Some("false".into()),
            }]
        );
    }

    #[test]
    fn applied_fixes_are_in_the_history_and_can_be_undone() {
        use crate::history::{conflict, inverse, undo};
        use ferrika_core::model::history::ConfigValue;
        let journal = TempJournal::new();
        let runner = FakeRunner::new();
        let log = FakeLog::new(true);
        let (results, _) = run_in(
            &journal.journal,
            &runner,
            &policy(&[], false),
            &log,
            false,
            ApplyMode::Apply,
        );
        assert!(results
            .iter()
            .all(|r| matches!(r.outcome, Outcome::Applied(_))));

        // One operation per fix, finished, under the id the audit request
        // and result entries carry, with the result entry's digest.
        let ops = journal.journal.load();
        assert_eq!(ops.len(), 2, "{ops:?}");
        let entries = log.entries.borrow();
        for (n, op) in ops.iter().enumerate() {
            assert_eq!(op.ok(), Some(true));
            assert_eq!(op.begin.source, "cli");
            assert_eq!(op.begin.scope, scope());
            let (request, result) = (&entries[2 * n], &entries[2 * n + 1]);
            assert_eq!(request.op, op.id, "request entry names the operation");
            assert_eq!(result.op, op.id);
            assert_eq!(
                undo::digest_check(op, Some(&result.op_digest)),
                undo::DigestCheck::Matches
            );
        }

        // T002 set min.insync.replicas on pay from 3 to 1: the newest
        // undoable operation, and undo sets it back to 3.
        let op = &ops[1];
        assert_eq!(op.begin.action, "lint fix T002");
        assert_eq!(op.begin.target, "topic pay: min.insync.replicas (set)");
        let own = |v: &str| ConfigValue {
            own: Some(v.into()),
            sensitive: false,
        };
        let config = |v: &str| State::TopicConfig {
            topic: "pay".into(),
            keys: [("min.insync.replicas".to_string(), own(v))]
                .into_iter()
                .collect(),
        };
        assert_eq!(op.begin.before, vec![config("3")]);
        assert_eq!(op.after(), &[config("1")]);
        assert_eq!(op.begin.reversibility, Reversibility::Reversible);
        let latest = undo::latest_undoable(&ops, &scope(), &ferrika_governance::audit::os_user());
        assert_eq!(latest.map(|o| &o.id), Some(&op.id));
        assert_eq!(conflict::check(op, op.after()), conflict::Verdict::Clean);
        assert_eq!(
            inverse::plan(op, op.after()),
            Ok(vec![inverse::Step::TopicConfig {
                topic: "pay".into(),
                set: vec![("min.insync.replicas".into(), "3".into())],
                delete: vec![],
            }])
        );
    }

    #[test]
    fn dry_runs_and_refused_fixes_are_not_in_the_history() {
        let journal = TempJournal::new();
        let runner = FakeRunner::new();
        let log = FakeLog::new(true);
        run_in(
            &journal.journal,
            &runner,
            &policy(&[], false),
            &log,
            false,
            ApplyMode::Validate,
        );
        run_in(
            &journal.journal,
            &runner,
            &policy(&[], false),
            &log,
            true,
            ApplyMode::Apply,
        );
        assert!(journal.journal.load().is_empty());
    }

    #[test]
    fn failed_fixes_are_recorded_as_failed() {
        let journal = TempJournal::new();
        let mut runner = FakeRunner::new();
        runner.fail = true;
        let log = FakeLog::new(true);
        run_in(
            &journal.journal,
            &runner,
            &policy(&[], false),
            &log,
            false,
            ApplyMode::Apply,
        );
        let ops = journal.journal.load();
        assert_eq!(ops.len(), 2);
        assert!(ops.iter().all(|op| op.ok() == Some(false)));
        assert!(ops.iter().all(|op| !crate::history::undo::undoable(op)));
    }

    #[test]
    fn broker_fixes_record_the_brokers_own_value_only() {
        let mut input = input();
        input.broker_configs.insert(
            2,
            vec![entry(
                "unclean.leader.election.enable",
                "true",
                ConfigOrigin::BrokerDynamicDefault,
            )],
        );
        let changes = vec![ConfigChange::Set(
            "unclean.leader.election.enable".into(),
            "false".into(),
        )];
        let broker = change_from_input(
            &input,
            &Fix::BrokerConfig {
                broker: Some(2),
                changes: changes.clone(),
            },
        )
        .expect("read from the input");
        assert_eq!(broker.reversibility, Reversibility::Reversible);
        // Inherited from the cluster default: undo removes the broker's
        // own value rather than setting the default's.
        let State::BrokerConfig { keys, .. } = &broker.before[0] else {
            panic!("{broker:?}");
        };
        assert_eq!(keys["unclean.leader.election.enable"].own, None);
        let cluster = change_from_input(
            &input,
            &Fix::BrokerConfig {
                broker: None,
                changes,
            },
        )
        .expect("recorded");
        assert_eq!(cluster.reversibility, Reversibility::Irreversible);
        assert!(change_from_input(
            &input,
            &Fix::Replication {
                topic: "orders".into(),
                target: 3
            }
        )
        .is_none());
    }
}
