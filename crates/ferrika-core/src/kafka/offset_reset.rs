use crate::kafka::consumer::CommitMode;
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use serde::{Deserialize, Serialize};

use crate::error::{KafkaError, KafkaResult};
use crate::kafka::admin::KafkaAdminClient;
use crate::kafka::config::KafkaClientConfig;
use crate::kafka::consumer::{KafkaConsumerClient, Lookup};

/// Supported strategy for resetting consumer group offsets.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResetStrategy {
    /// Reset to the earliest available offset (beginning)
    Earliest,
    /// Reset to the latest offset / high watermark (skip all lag)
    Latest,
    /// Reset to an explicit offset number
    Specific(i64),
    /// Shift offset relative to current committed offset (e.g. -100 or +50).
    ///
    /// A partition on which the group has no committed offset has nothing to
    /// shift from: planning refuses the whole reset
    /// ([`KafkaError::UncommittedShift`]) unless `uncommitted` names where
    /// those partitions are shifted from.
    Shift {
        /// The number of records to move: negative rewinds, positive skips.
        delta: i64,
        /// The base for partitions without a committed offset; `None`
        /// refuses them.
        #[serde(default)]
        uncommitted: Option<UncommittedBase>,
    },
    /// Reset to offset matching an epoch millisecond timestamp
    Timestamp(i64),
}

impl ResetStrategy {
    /// A shift by `delta` that refuses partitions without a committed offset.
    pub fn shift(delta: i64) -> Self {
        ResetStrategy::Shift {
            delta,
            uncommitted: None,
        }
    }
}

/// Where a shift starts on a partition on which the group has no committed
/// offset (an explicit opt-in: without it such a shift is refused).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum UncommittedBase {
    /// From the low watermark (the first available record). Ferrika 0.2 and
    /// earlier did this without being asked.
    Earliest,
    /// From the high watermark (the end of the partition).
    Latest,
}

impl std::fmt::Display for UncommittedBase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            UncommittedBase::Earliest => "earliest",
            UncommittedBase::Latest => "latest",
        })
    }
}

impl std::fmt::Display for ResetStrategy {
    /// `earliest`, `latest`, `offset 150`, `shift -5`,
    /// `shift -5 (uncommitted from earliest)`, `timestamp 1700000000000`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResetStrategy::Earliest => write!(f, "earliest"),
            ResetStrategy::Latest => write!(f, "latest"),
            ResetStrategy::Specific(offset) => write!(f, "offset {offset}"),
            ResetStrategy::Shift {
                delta,
                uncommitted: None,
            } => write!(f, "shift {delta:+}"),
            ResetStrategy::Shift {
                delta,
                uncommitted: Some(base),
            } => write!(f, "shift {delta:+} (uncommitted from {base})"),
            ResetStrategy::Timestamp(ts) => write!(f, "timestamp {ts}"),
        }
    }
}

/// Simulation plan describing the proposed offset changes for a partition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OffsetResetPlan {
    /// Consumer group identifier
    pub group_id: String,
    /// Target topic name
    pub topic: String,
    /// Partition number
    pub partition: i32,
    /// Currently committed offset (-1 if not yet committed)
    pub current_offset: i64,
    /// Earliest available offset in this partition
    pub low_watermark: i64,
    /// Latest offset (log end) in this partition
    pub high_watermark: i64,
    /// Calculated new target offset
    pub target_offset: i64,
    /// Difference: target_offset - current_offset
    pub delta: i64,
}

/// Calculates the target offset according to the reset strategy, safely clamped within watermarks.
///
/// `current` is the group's committed offset, `-1` when it has none. A
/// [`ResetStrategy::Shift`] on such a partition has nothing to shift from:
/// the result is `None` unless the strategy names an [`UncommittedBase`].
/// Every other strategy always has a target.
///
/// [`ResetStrategy::Timestamp`] needs a broker lookup, so callers resolve it to
/// a [`ResetStrategy::Specific`] offset first (see [`prepare_offset_reset`]);
/// left unresolved it means no record at or after the timestamp: the end.
pub fn calculate_target_offset(
    strategy: &ResetStrategy,
    current: i64,
    low: i64,
    high: i64,
) -> Option<i64> {
    let raw = match strategy {
        ResetStrategy::Earliest => low,
        ResetStrategy::Latest => high,
        ResetStrategy::Specific(off) => *off,
        ResetStrategy::Shift { delta, uncommitted } => {
            let from = if current >= 0 {
                current
            } else {
                match uncommitted {
                    Some(UncommittedBase::Earliest) => low,
                    Some(UncommittedBase::Latest) => high,
                    None => return None,
                }
            };
            from.saturating_add(*delta)
        }
        ResetStrategy::Timestamp(_) => high,
    };

    Some(raw.clamp(low, high))
}

/// What was read for one partition before planning its reset.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionPosition {
    /// Partition number
    pub partition: i32,
    /// The group's committed offset, `-1` when it has none
    pub committed: i64,
    /// Earliest available offset in this partition
    pub low_watermark: i64,
    /// Latest offset (log end) in this partition
    pub high_watermark: i64,
}

/// Plans a reset of `group_id` on `topic` from offsets already read, without
/// touching the cluster. All partitions or none: when a shift finds
/// partitions without a committed offset (and the strategy names no base for
/// them), the whole plan is refused with [`KafkaError::UncommittedShift`]
/// naming them, so a reset is never applied to only part of the topic.
///
/// [`ResetStrategy::Timestamp`] is not resolved here (see
/// [`calculate_target_offset`]); [`prepare_offset_reset`] resolves it.
pub fn plan_offset_reset(
    group_id: &str,
    topic: &str,
    strategy: &ResetStrategy,
    positions: &[PartitionPosition],
) -> KafkaResult<Vec<OffsetResetPlan>> {
    let resolved: Vec<(PartitionPosition, ResetStrategy)> = positions
        .iter()
        .map(|pos| (*pos, strategy.clone()))
        .collect();
    plan_resolved(group_id, topic, &resolved)
}

/// [`plan_offset_reset`] with a strategy per partition (a timestamp resolved
/// to each partition's offset).
fn plan_resolved(
    group_id: &str,
    topic: &str,
    resolved: &[(PartitionPosition, ResetStrategy)],
) -> KafkaResult<Vec<OffsetResetPlan>> {
    let mut plans = Vec::with_capacity(resolved.len());
    let mut uncommitted = Vec::new();
    for (pos, strategy) in resolved {
        let (current, low, high) = (pos.committed, pos.low_watermark, pos.high_watermark);
        let Some(target) = calculate_target_offset(strategy, current, low, high) else {
            uncommitted.push(pos.partition);
            continue;
        };
        let delta = if current >= 0 {
            target.saturating_sub(current)
        } else {
            target.saturating_sub(low)
        };
        plans.push(OffsetResetPlan {
            group_id: group_id.to_string(),
            topic: topic.to_string(),
            partition: pos.partition,
            current_offset: current,
            low_watermark: low,
            high_watermark: high,
            target_offset: target,
            delta,
        });
    }
    if !uncommitted.is_empty() {
        return Err(KafkaError::UncommittedShift {
            group: group_id.to_string(),
            topic: topic.to_string(),
            partitions: uncommitted,
        });
    }
    Ok(plans)
}

/// Prepares and simulates the offset reset plans across partitions without modifying cluster state.
pub async fn prepare_offset_reset(
    config: &KafkaClientConfig,
    group_id: &str,
    topic: &str,
    target_partition: Option<i32>,
    strategy: &ResetStrategy,
) -> KafkaResult<Vec<OffsetResetPlan>> {
    // Every read below is bounded by the profile's request timeout (`None`),
    // like the rest of the client: a fixed wait would outlast a profile that
    // asks for less and cut short one that allows more.
    let admin = KafkaAdminClient::new(config)?;
    let topic_metadata = admin.describe_topic(topic, None)?;

    let mut consumer_cfg = config.clone();
    consumer_cfg = consumer_cfg.with_group_id(group_id.to_string());
    let consumer = KafkaConsumerClient::new(&consumer_cfg)?;

    let mut resolved: Vec<(PartitionPosition, ResetStrategy)> = Vec::new();

    let partitions_to_check: Vec<i32> = match target_partition {
        Some(p) => vec![p],
        None => topic_metadata
            .partitions
            .iter()
            .map(|p| p.partition_id)
            .collect(),
    };

    // The first offset at or after the timestamp, per partition.
    let by_time = match strategy {
        ResetStrategy::Timestamp(ts) => {
            let keys: Vec<(&str, i32)> = partitions_to_check.iter().map(|&p| (topic, p)).collect();
            Some(consumer.offsets_at_time(&keys, *ts, None)?)
        }
        _ => None,
    };

    // A partition whose watermarks or committed offset cannot be read would
    // otherwise get a made-up target (0, or a rewind from "uncommitted"), so
    // any failure refuses the whole plan.
    let mut failures: Vec<(i32, String)> = Vec::new();

    for p in partitions_to_check {
        if !topic_metadata
            .partitions
            .iter()
            .any(|meta| meta.partition_id == p)
        {
            failures.push((p, format!("partition does not exist in '{topic}'")));
            continue;
        }
        let (low, high) = match consumer.fetch_watermarks_async(topic, p, None).await {
            Ok(marks) => marks,
            Err(e) => {
                failures.push((p, format!("cannot read watermarks: {e}")));
                continue;
            }
        };
        let strategy = match &by_time {
            Some(lookups) => match time_target(lookups.get(&(topic.to_string(), p)), high) {
                Ok(offset) => ResetStrategy::Specific(offset),
                Err(why) => {
                    failures.push((p, why));
                    continue;
                }
            },
            None => strategy.clone(),
        };

        let current_committed = match consumer
            .committed_list_for(&[(topic, p)], None)
            .and_then(|tpl| committed_offset_of(&tpl, topic, p))
        {
            Ok(o) => o,
            Err(e) => {
                failures.push((p, format!("cannot read committed offset: {e}")));
                continue;
            }
        };

        resolved.push((
            PartitionPosition {
                partition: p,
                committed: current_committed,
                low_watermark: low,
                high_watermark: high,
            },
            strategy,
        ));
    }

    if !failures.is_empty() {
        return Err(plan_failures_error(group_id, topic, &failures));
    }

    // A shift that finds partitions without a committed offset (and no base
    // for them) refuses the whole plan.
    plan_resolved(group_id, topic, &resolved)
}

/// The group's committed offset for `topic`/`partition` in a committed-offsets
/// lookup result: `-1` when the group has not committed one, an error when the
/// lookup failed for this partition.
fn committed_offset_of(tpl: &TopicPartitionList, topic: &str, partition: i32) -> KafkaResult<i64> {
    let elem = tpl.find_partition(topic, partition).ok_or_else(|| {
        KafkaError::Offset(format!(
            "no committed-offset result for partition {partition}"
        ))
    })?;
    elem.error()?;
    Ok(match elem.offset() {
        Offset::Offset(o) if o >= 0 => o,
        _ => -1,
    })
}

/// The error refusing a reset plan because some partitions could not be read.
fn plan_failures_error(group_id: &str, topic: &str, failures: &[(i32, String)]) -> KafkaError {
    let details: Vec<String> = failures
        .iter()
        .map(|(p, reason)| format!("partition {p}: {reason}"))
        .collect();
    KafkaError::Offset(format!(
        "refusing to plan an offset reset for group '{group_id}' on '{topic}': {} of the partitions could not be read ({}). No offsets were changed.",
        failures.len(),
        details.join("; ")
    ))
}

/// Refuses an offset reset while the group is consuming: members' own
/// commits would overwrite the reset (or the commit is rejected). `state` is
/// the broker-reported state and member count of the group, `None` when the
/// broker does not know it (a new group: nothing to disturb).
pub fn check_group_idle(group_id: &str, state: Option<(&str, usize)>) -> KafkaResult<()> {
    match state {
        None => Ok(()),
        Some(("Empty" | "Dead", 0)) => Ok(()),
        Some((state, members)) => Err(KafkaError::Offset(format!(
            "consumer group '{group_id}' is {state} with {members} member(s); stop its \
             consumers (the group must be Empty) before resetting its offsets. \
             No offsets were changed."
        ))),
    }
}

/// The broker-reported state and member count of `group_id`, `None` when
/// the broker does not know the group.
pub fn group_state(
    admin: &KafkaAdminClient,
    group_id: &str,
) -> KafkaResult<Option<(String, usize)>> {
    Ok(admin
        .list_consumer_group_states(None)?
        .into_iter()
        .find(|(name, _, _)| name == group_id)
        .map(|(_, state, members)| (state, members)))
}

/// Atomically commits the calculated target offsets to the Kafka consumer group.
pub fn commit_offset_reset(
    config: &KafkaClientConfig,
    group_id: &str,
    plans: &[OffsetResetPlan],
) -> KafkaResult<()> {
    if plans.is_empty() {
        return Ok(());
    }

    let mut consumer_cfg = config.clone();
    consumer_cfg = consumer_cfg.with_group_id(group_id.to_string());
    let client = KafkaConsumerClient::new(&consumer_cfg)?;

    let offsets: Vec<(&str, i32, i64)> = plans
        .iter()
        .map(|plan| (plan.topic.as_str(), plan.partition, plan.target_offset))
        .collect();
    client.commit(&offsets, CommitMode::Sync)?;

    Ok(())
}

/// The target of a timestamp reset on one partition: the first offset at or
/// after the time, or the end when there is none. A lookup that failed (a
/// moving leader, a timeout) or did not answer is an error, not the end: it
/// would otherwise skip the partition's unread records for good.
fn time_target(lookup: Option<&Lookup>, high: i64) -> Result<i64, String> {
    match lookup {
        Some(Lookup::At(offset)) => Ok(*offset),
        Some(Lookup::End) => Ok(high),
        Some(Lookup::Failed(failure)) => Err(format!(
            "cannot find the offset at the time: {}",
            failure.message
        )),
        None => Err("no answer for the offset at the time".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_failed_time_lookup_refuses_instead_of_moving_to_the_end() {
        assert_eq!(time_target(Some(&Lookup::At(42)), 100), Ok(42));
        assert_eq!(time_target(Some(&Lookup::End), 100), Ok(100));
        assert!(time_target(None, 100).is_err());
        let failed = Lookup::Failed(crate::kafka::consumer::LookupFailure::new(
            "NotLeaderForPartition",
            crate::error::ErrorClass::Retriable,
        ));
        let why = time_target(Some(&failed), 100).unwrap_err();
        assert!(why.contains("NotLeaderForPartition"), "{why}");
    }

    #[test]
    fn test_check_group_idle() {
        assert!(check_group_idle("g", None).is_ok());
        assert!(check_group_idle("g", Some(("Empty", 0))).is_ok());
        assert!(check_group_idle("g", Some(("Dead", 0))).is_ok());
        let err = check_group_idle("g", Some(("Stable", 2)))
            .unwrap_err()
            .to_string();
        assert!(err.contains("Stable with 2 member(s)"));
        assert!(check_group_idle("g", Some(("PreparingRebalance", 1))).is_err());
        assert!(check_group_idle("g", Some(("CompletingRebalance", 0))).is_err());
    }

    #[test]
    fn test_unresolved_timestamp_targets_the_end() {
        // Resolved timestamps arrive as Specific; an unresolved one means no
        // record at or after it, so the group must not be rewound.
        let target = calculate_target_offset(&ResetStrategy::Timestamp(1), 50, 10, 100);
        assert_eq!(target, Some(100));
    }

    #[test]
    fn test_calculate_target_offset_earliest() {
        let target = calculate_target_offset(&ResetStrategy::Earliest, 50, 10, 100);
        assert_eq!(target, Some(10));
    }

    #[test]
    fn test_calculate_target_offset_latest() {
        let target = calculate_target_offset(&ResetStrategy::Latest, 50, 10, 100);
        assert_eq!(target, Some(100));
    }

    #[test]
    fn test_calculate_target_offset_specific_clamped() {
        // Within bounds
        let target = calculate_target_offset(&ResetStrategy::Specific(45), 50, 10, 100);
        assert_eq!(target, Some(45));

        // Clamped to low
        let target = calculate_target_offset(&ResetStrategy::Specific(5), 50, 10, 100);
        assert_eq!(target, Some(10));

        // Clamped to high
        let target = calculate_target_offset(&ResetStrategy::Specific(150), 50, 10, 100);
        assert_eq!(target, Some(100));
    }

    #[test]
    fn test_calculate_target_offset_shift_forward_and_backward() {
        // Shift backward (rewind)
        let target = calculate_target_offset(&ResetStrategy::shift(-20), 50, 10, 100);
        assert_eq!(target, Some(30));

        // Shift forward
        let target = calculate_target_offset(&ResetStrategy::shift(30), 50, 10, 100);
        assert_eq!(target, Some(80));

        // Shift beyond low clamp
        let target = calculate_target_offset(&ResetStrategy::shift(-100), 50, 10, 100);
        assert_eq!(target, Some(10));
    }

    #[test]
    fn a_shift_without_a_committed_offset_has_no_target_unless_given_a_base() {
        let from = |base| ResetStrategy::Shift {
            delta: 15,
            uncommitted: base,
        };
        assert_eq!(calculate_target_offset(&from(None), -1, 10, 100), None);
        assert_eq!(
            calculate_target_offset(&from(Some(UncommittedBase::Earliest)), -1, 10, 100),
            Some(25)
        );
        // From the end, a forward shift stays at the end; a rewind goes back.
        assert_eq!(
            calculate_target_offset(&from(Some(UncommittedBase::Latest)), -1, 10, 100),
            Some(100)
        );
        let rewind = ResetStrategy::Shift {
            delta: -30,
            uncommitted: Some(UncommittedBase::Latest),
        };
        assert_eq!(calculate_target_offset(&rewind, -1, 10, 100), Some(70));
        // The base does not apply where an offset is committed.
        assert_eq!(
            calculate_target_offset(&from(Some(UncommittedBase::Latest)), 50, 10, 100),
            Some(65)
        );
        // The other strategies never need one.
        for strategy in [
            ResetStrategy::Earliest,
            ResetStrategy::Latest,
            ResetStrategy::Specific(40),
        ] {
            assert!(calculate_target_offset(&strategy, -1, 10, 100).is_some());
        }
    }

    #[test]
    fn shift_by_extreme_values_clamps_instead_of_wrapping() {
        let shift = |d, cur| {
            let strategy = ResetStrategy::Shift {
                delta: d,
                uncommitted: Some(UncommittedBase::Earliest),
            };
            calculate_target_offset(&strategy, cur, 10, 100)
        };
        assert_eq!(shift(i64::MAX, 50), Some(100));
        assert_eq!(shift(i64::MAX, i64::MAX), Some(100));
        assert_eq!(shift(i64::MAX, -1), Some(100));
        assert_eq!(shift(i64::MIN, 50), Some(10));
        assert_eq!(shift(i64::MIN, i64::MAX), Some(10));
        assert_eq!(shift(i64::MIN, -1), Some(10));
    }

    fn pos(partition: i32, committed: i64) -> PartitionPosition {
        PartitionPosition {
            partition,
            committed,
            low_watermark: 10,
            high_watermark: 100,
        }
    }

    #[test]
    fn a_shift_on_uncommitted_partitions_is_refused() {
        let err = plan_offset_reset("grp", "events", &ResetStrategy::shift(-5), &[pos(0, -1)])
            .unwrap_err();
        match &err {
            KafkaError::UncommittedShift {
                group,
                topic,
                partitions,
            } => {
                assert_eq!((group.as_str(), topic.as_str()), ("grp", "events"));
                assert_eq!(partitions, &[0]);
            }
            other => panic!("expected UncommittedShift, got {other:?}"),
        }
        let text = err.to_string();
        assert!(text.contains("partition(s) 0"), "{text}");
        assert!(text.contains("--shift-uncommitted-from"), "{text}");
        assert!(text.contains("No offsets were changed"), "{text}");
    }

    #[test]
    fn a_shift_on_some_uncommitted_partitions_refuses_all_of_them() {
        // Partitions 0 and 2 committed, 1 and 3 not: no plan at all (not a
        // plan for 0 and 2), naming both uncommitted ones.
        let positions = [pos(0, 50), pos(1, -1), pos(2, 60), pos(3, -1)];
        let err =
            plan_offset_reset("grp", "events", &ResetStrategy::shift(-5), &positions).unwrap_err();
        assert!(
            matches!(&err, KafkaError::UncommittedShift { partitions, .. } if partitions == &[1, 3]),
            "{err:?}"
        );
        assert!(err.to_string().contains("partition(s) 1, 3"), "{err}");
    }

    #[test]
    fn a_shift_with_a_base_plans_uncommitted_partitions_from_it() {
        let positions = [pos(0, 50), pos(1, -1)];
        let plan = |base| {
            let strategy = ResetStrategy::Shift {
                delta: -5,
                uncommitted: Some(base),
            };
            plan_offset_reset("grp", "events", &strategy, &positions).unwrap()
        };
        let earliest = plan(UncommittedBase::Earliest);
        assert_eq!(earliest[0].target_offset, 45);
        assert_eq!(earliest[0].delta, -5);
        // The 0.2 behaviour: from the low watermark (rewinding past it clamps).
        assert_eq!(earliest[1].target_offset, 10);
        assert_eq!(earliest[1].current_offset, -1);
        let latest = plan(UncommittedBase::Latest);
        assert_eq!(latest[0].target_offset, 45);
        assert_eq!(latest[1].target_offset, 95);
        assert_eq!(latest[1].delta, 85, "relative to the low watermark");
    }

    #[test]
    fn a_shift_on_committed_partitions_is_unchanged() {
        let positions = [pos(0, 50), pos(1, 20), pos(2, 100)];
        let plans =
            plan_offset_reset("grp", "events", &ResetStrategy::shift(-15), &positions).unwrap();
        let targets: Vec<(i32, i64, i64)> = plans
            .iter()
            .map(|p| (p.partition, p.target_offset, p.delta))
            .collect();
        assert_eq!(targets, vec![(0, 35, -15), (1, 10, -10), (2, 85, -15)]);
        // A base changes nothing where every partition is committed.
        let with_base = ResetStrategy::Shift {
            delta: -15,
            uncommitted: Some(UncommittedBase::Latest),
        };
        assert_eq!(
            plan_offset_reset("grp", "events", &with_base, &positions).unwrap(),
            plans
        );
    }

    #[test]
    fn other_strategies_still_plan_uncommitted_partitions() {
        let positions = [pos(0, 50), pos(1, -1)];
        let plans =
            plan_offset_reset("grp", "events", &ResetStrategy::Earliest, &positions).unwrap();
        assert_eq!(plans.len(), 2);
        assert_eq!(plans[1].target_offset, 10);
    }

    #[test]
    fn reset_strategy_display_names_the_base() {
        assert_eq!(ResetStrategy::shift(-5).to_string(), "shift -5");
        let with_base = ResetStrategy::Shift {
            delta: 5,
            uncommitted: Some(UncommittedBase::Earliest),
        };
        assert_eq!(
            with_base.to_string(),
            "shift +5 (uncommitted from earliest)"
        );
    }

    #[test]
    fn test_committed_offset_of_reads_offset_or_uncommitted() {
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset("events", 0, Offset::Offset(42))
            .unwrap();
        tpl.add_partition_offset("events", 1, Offset::Invalid)
            .unwrap();
        assert_eq!(committed_offset_of(&tpl, "events", 0).unwrap(), 42);
        assert_eq!(committed_offset_of(&tpl, "events", 1).unwrap(), -1);
    }

    #[test]
    fn test_committed_offset_of_missing_partition_is_an_error() {
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset("events", 0, Offset::Offset(42))
            .unwrap();
        // A missing result must not read as "uncommitted" (-1).
        assert!(committed_offset_of(&tpl, "events", 3).is_err());
    }

    #[test]
    fn test_plan_failures_error_lists_every_partition() {
        let err = plan_failures_error(
            "grp",
            "events",
            &[
                (1, "cannot read watermarks: timed out".into()),
                (4, "partition does not exist in 'events'".into()),
            ],
        )
        .to_string();
        assert!(err.contains("refusing"));
        assert!(err.contains("partition 1: cannot read watermarks"));
        assert!(err.contains("partition 4: partition does not exist"));
        assert!(err.contains("No offsets were changed"));
    }

    #[test]
    fn test_offset_reset_plan_delta() {
        let plan = OffsetResetPlan {
            group_id: "test-grp".into(),
            topic: "events".into(),
            partition: 0,
            current_offset: 100,
            low_watermark: 0,
            high_watermark: 200,
            target_offset: 80,
            delta: -20,
        };
        assert_eq!(plan.delta, -20);
        assert_eq!(plan.target_offset, 80);
    }
}
