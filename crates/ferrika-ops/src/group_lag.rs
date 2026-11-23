//! Consumer group offsets and lag as the GUI and `monitor lag` show them.

use std::collections::HashMap;

use ferrika_core::kafka::metrics::calculate_partition_lag;
use ferrika_core::model::cluster::{
    ConsumerGroupSummary, ConsumerState, GroupPartitionLag, TopicSummary,
};

/// The committed offset [`calculate_partition_lag`] reads as "no commit" (any
/// negative offset does).
const NO_COMMIT: i64 = -1001;

/// Expands a group's committed offsets to every partition of the topics it has
/// committed to, the shape [`apply_group_offsets`] expects: partitions with no
/// commit are present with `None` so they count as lag.
pub fn committed_partition_offsets(
    committed: &HashMap<(String, i32), i64>,
    topics: &[TopicSummary],
) -> HashMap<(String, i32), Option<i64>> {
    let mut offsets = HashMap::new();
    for topic in topics.iter().filter(|t| !t.is_internal) {
        if !committed.keys().any(|(name, _)| name == &topic.name) {
            continue;
        }
        for p in &topic.partitions {
            let key = (topic.name.clone(), p.partition_id);
            let offset = committed.get(&key).copied();
            offsets.insert(key, offset);
        }
    }
    offsets
}

/// Fills offsets, lag, and topics of `summary` from the group's committed
/// offsets, keyed by `(topic, partition)` with `None` for a partition without
/// a committed offset (the shape
/// [`KafkaConsumerClient::fetch_committed_offsets_for`] returns).
///
/// Matches `monitor lag`: within a topic the group has committed to, a partition
/// without a committed offset counts its whole retained range as lag.
/// Partitions without known watermarks are left out.
///
/// [`KafkaConsumerClient::fetch_committed_offsets_for`]: ferrika_core::kafka::consumer::KafkaConsumerClient::fetch_committed_offsets_for
pub fn apply_group_offsets(
    summary: &mut ConsumerGroupSummary,
    committed: &HashMap<(String, i32), Option<i64>>,
    watermarks: &HashMap<(String, i32), (i64, i64)>,
) {
    let mut topics: Vec<String> = committed
        .iter()
        .filter(|(_, offset)| offset.is_some())
        .map(|((topic, _), _)| topic.clone())
        .collect();
    topics.sort();
    topics.dedup();

    for ((topic, partition), &committed) in committed {
        if !topics.iter().any(|t| t == topic) {
            continue;
        }
        let Some(&(low, high)) = watermarks.get(&(topic.clone(), *partition)) else {
            continue;
        };
        let lag = calculate_partition_lag(committed.unwrap_or(NO_COMMIT), high, low);
        summary.current_offset += committed.unwrap_or(0);
        summary.end_offset += high;
        summary.total_lag += lag;
        summary.partitions.push(GroupPartitionLag {
            topic: topic.clone(),
            partition: *partition,
            committed,
            end_offset: high,
            lag,
            behind_ms: None,
            owner: None,
        });
    }
    summary
        .partitions
        .sort_by(|a, b| (&a.topic, a.partition).cmp(&(&b.topic, b.partition)));
    summary.target_topics = topics;
}

/// One group's per-partition committed offsets and lag against the topics of
/// a snapshot, by the same rules as [`apply_group_offsets`]: a partition of a
/// topic the group committed to that has no commit is a row with `committed:
/// None` whose lag is its whole retained range.
///
/// Commits the expansion cannot place (a topic missing from `topics`, an
/// internal topic, a partition the topic no longer has) still get a row, with
/// lag measured against an end offset of 0, so callers can tell which topics
/// the group committed to.
pub fn group_partition_lags(
    committed: &HashMap<(String, i32), i64>,
    topics: &[TopicSummary],
) -> Vec<GroupPartitionLag> {
    let watermarks: HashMap<(String, i32), (i64, i64)> = topics
        .iter()
        .flat_map(|t| {
            t.partitions.iter().map(move |p| {
                (
                    (t.name.clone(), p.partition_id),
                    (p.low_watermark, p.high_watermark),
                )
            })
        })
        .collect();
    let mut scratch = ConsumerGroupSummary {
        group_id: String::new(),
        state: ConsumerState::Empty,
        active_members: 0,
        target_topics: Vec::new(),
        current_offset: 0,
        end_offset: 0,
        total_lag: 0,
        coordinator_node: String::new(),
        consume_rate: None,
        partitions: Vec::new(),
    };
    apply_group_offsets(
        &mut scratch,
        &committed_partition_offsets(committed, topics),
        &watermarks,
    );
    let mut rows = scratch.partitions;
    for ((topic, partition), &offset) in committed {
        if rows
            .iter()
            .any(|r| &r.topic == topic && r.partition == *partition)
        {
            continue;
        }
        rows.push(GroupPartitionLag {
            lag: calculate_partition_lag(offset, 0, 0),
            topic: topic.clone(),
            partition: *partition,
            committed: Some(offset),
            end_offset: 0,
            owner: None,
            behind_ms: None,
        });
    }
    rows.sort_by(|a, b| (&a.topic, a.partition).cmp(&(&b.topic, b.partition)));
    rows
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrika_core::model::cluster::{ClusterSnapshot, ConsumerState};

    fn empty_summary() -> ConsumerGroupSummary {
        ConsumerGroupSummary {
            group_id: "g".into(),
            state: ConsumerState::Empty,
            active_members: 0,
            target_topics: Vec::new(),
            current_offset: 0,
            end_offset: 0,
            total_lag: 0,
            coordinator_node: "-".into(),
            consume_rate: None,
            partitions: Vec::new(),
        }
    }

    #[test]
    fn test_committed_partition_offsets_expand_consumed_topics_only() {
        let topics: Vec<TopicSummary> = ClusterSnapshot::demo()
            .topics
            .into_iter()
            .filter(|t| !t.is_internal)
            .collect();
        let consumed = &topics[0];
        let committed = HashMap::from([((consumed.name.clone(), 0), 7)]);
        let offsets = committed_partition_offsets(&committed, &topics);
        assert_eq!(offsets.len(), consumed.partitions.len());
        assert!(offsets.keys().all(|(topic, _)| topic == &consumed.name));
        assert_eq!(offsets.get(&(consumed.name.clone(), 0)), Some(&Some(7)));
        assert_eq!(offsets.get(&(consumed.name.clone(), 1)), Some(&None));
    }

    #[test]
    fn test_group_lag_counts_uncommitted_partitions_of_consumed_topics() {
        let committed = HashMap::from([
            (("orders".to_string(), 0), None),
            (("orders".to_string(), 1), Some(2)),
            (("audit".to_string(), 0), None),
        ]);
        let watermarks = HashMap::from([
            (("orders".to_string(), 0), (0, 2)),
            (("orders".to_string(), 1), (0, 3)),
            (("audit".to_string(), 0), (0, 9)),
        ]);
        let mut summary = empty_summary();
        apply_group_offsets(&mut summary, &committed, &watermarks);
        assert_eq!(summary.target_topics, vec!["orders".to_string()]);
        assert_eq!(summary.total_lag, 3);
        assert_eq!(summary.current_offset, 2);
        assert_eq!(summary.end_offset, 5);
        assert_eq!(summary.partitions.len(), 2);
        assert_eq!(summary.partitions[0].committed, None);
        assert_eq!(summary.partitions[0].lag, 2);
        assert_eq!(summary.partitions[1].committed, Some(2));
    }

    /// The edge cases where the GUI's former inline formula
    /// `(high - committed.unwrap_or(low)).max(0)` and `calculate_partition_lag`
    /// could differ: none of them changes what the GUI shows.
    #[test]
    fn test_group_lag_edge_cases_match_former_gui_formula() {
        let committed: HashMap<(String, i32), Option<i64>> = [
            (0, Some(5)),   // empty partition (high 0) with a stale commit
            (1, None),      // no commit, records truncated to 40..100
            (2, Some(10)),  // commit below the low watermark
            (3, Some(120)), // commit past the high watermark
            (4, None),      // no commit, empty partition
        ]
        .into_iter()
        .map(|(p, offset)| (("t".to_string(), p), offset))
        .collect();
        let watermarks = HashMap::from([
            (("t".to_string(), 0), (0, 0)),
            (("t".to_string(), 1), (40, 100)),
            (("t".to_string(), 2), (40, 100)),
            (("t".to_string(), 3), (0, 100)),
            (("t".to_string(), 4), (0, 0)),
        ]);
        let mut summary = empty_summary();
        apply_group_offsets(&mut summary, &committed, &watermarks);
        let lags: Vec<i64> = summary.partitions.iter().map(|p| p.lag).collect();
        assert_eq!(lags, vec![0, 60, 90, 0, 0]);
        assert_eq!(summary.total_lag, 150);
        assert_eq!(summary.current_offset, 5 + 10 + 120);
        assert_eq!(summary.end_offset, 300);
    }

    #[test]
    fn test_group_lag_skips_partitions_without_watermarks() {
        let committed = HashMap::from([
            (("t".to_string(), 0), Some(1)),
            (("t".to_string(), 1), Some(1)),
        ]);
        let watermarks = HashMap::from([(("t".to_string(), 0), (0, 4))]);
        let mut summary = empty_summary();
        apply_group_offsets(&mut summary, &committed, &watermarks);
        assert_eq!(summary.partitions.len(), 1);
        assert_eq!(summary.total_lag, 3);
        assert_eq!(summary.target_topics, vec!["t".to_string()]);
    }

    #[test]
    fn test_group_partition_lags_include_uncommitted_partitions_and_unplaced_commits() {
        let mut topics: Vec<TopicSummary> = ClusterSnapshot::demo()
            .topics
            .into_iter()
            .filter(|t| !t.is_internal)
            .collect();
        let name = topics[0].name.clone();
        for (i, p) in topics[0].partitions.iter_mut().enumerate() {
            p.low_watermark = 0;
            p.high_watermark = if i == 1 { 100 } else { 10 };
        }
        let committed = HashMap::from([
            ((name.clone(), 0), 4),
            (("deleted-topic".to_string(), 0), 7),
        ]);
        let rows = group_partition_lags(&committed, &topics);
        let row = |t: &str, p: i32| rows.iter().find(|r| r.topic == t && r.partition == p);
        let p0 = row(&name, 0).unwrap();
        assert_eq!((p0.committed, p0.lag, p0.end_offset), (Some(4), 6, 10));
        let p1 = row(&name, 1).unwrap();
        assert_eq!((p1.committed, p1.lag, p1.end_offset), (None, 100, 100));
        let gone = row("deleted-topic", 0).unwrap();
        assert_eq!((gone.committed, gone.lag, gone.end_offset), (Some(7), 0, 0));
        assert!(rows
            .windows(2)
            .all(|w| (&w[0].topic, w[0].partition) <= (&w[1].topic, w[1].partition)));
    }
}
