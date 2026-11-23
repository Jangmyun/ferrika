use std::collections::HashMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::error::KafkaResult;
use crate::kafka::consumer::KafkaConsumerClient;
use crate::kafka::wire::BrokerLogDirs;
use crate::model::cluster::{
    BrokerInfo, ConsumerGroupSummary, ConsumerState, GroupPartitionLag, TopicSummary,
};

/// Lag details for an individual topic partition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionLag {
    pub topic: String,
    pub partition: i32,
    pub current_offset: i64,
    pub high_watermark: i64,
    pub low_watermark: i64,
    pub lag: i64,
    pub is_caught_up: bool,
}

/// Aggregated lag for a single topic across its partitions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicLagSummary {
    pub topic: String,
    pub partitions: Vec<PartitionLag>,
    pub total_lag: i64,
}

/// Comprehensive consumer group lag summary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerGroupLag {
    pub group_id: String,
    pub state: String,
    pub topic_lags: Vec<TopicLagSummary>,
    pub total_lag: i64,
}

/// Pure helper function to compute partition lag safely without underflow/overflow.
pub fn calculate_partition_lag(
    committed_offset: i64,
    high_watermark: i64,
    low_watermark: i64,
) -> i64 {
    // If high watermark is zero or negative, the partition is completely empty.
    if high_watermark <= 0 {
        return 0;
    }

    // Negative committed offset indicates no offset has been committed yet (e.g. -1001 in rdkafka).
    // In this case, the lag is the total number of unconsumed messages in the partition.
    if committed_offset < 0 {
        let low = low_watermark.max(0);
        return (high_watermark - low).max(0);
    }

    // If consumer has committed past the high watermark (e.g. retention truncation), lag is 0.
    if committed_offset >= high_watermark {
        return 0;
    }

    high_watermark - committed_offset
}

/// Records retained in one partition: `high - low`, never negative.
pub fn partition_depth(low_watermark: i64, high_watermark: i64) -> i64 {
    (high_watermark - low_watermark).max(0)
}

/// Records retained in a topic: the [`partition_depth`] of each
/// `(low, high)` watermark pair, summed.
pub fn topic_depth<I>(watermarks: I) -> i64
where
    I: IntoIterator<Item = (i64, i64)>,
{
    watermarks
        .into_iter()
        .map(|(low, high)| partition_depth(low, high))
        .sum()
}

/// Records per second between two offset totals taken `secs` apart.
///
/// `None` without an earlier total or a positive interval; a decrease (topic
/// recreated, offsets reset) counts as no progress rather than a negative rate.
pub fn rate_per_sec(before: Option<i64>, after: i64, secs: f64) -> Option<f64> {
    let before = before?;
    (secs > 0.0).then(|| (after - before).max(0) as f64 / secs)
}

/// Fills topic sizes (largest replica of each partition, i.e. one copy) and
/// broker disk usage from DescribeLogDirs answers. Leaves both untouched when
/// no broker answered.
pub fn apply_disk_usage(
    topics: &mut [TopicSummary],
    brokers: &mut [BrokerInfo],
    dirs: &HashMap<i32, BrokerLogDirs>,
) {
    if dirs.is_empty() {
        return;
    }
    for broker in brokers.iter_mut() {
        broker.log_dir_bytes = dirs.get(&broker.id).map(BrokerLogDirs::total_bytes);
        broker.log_dirs = dirs
            .get(&broker.id)
            .map(|d| d.dirs.clone())
            .unwrap_or_default();
    }
    for topic in topics.iter_mut() {
        let size = topic
            .partitions
            .iter()
            .map(|p| {
                let key = (topic.name.clone(), p.partition_id);
                dirs.values()
                    .filter_map(|d| d.partitions.get(&key))
                    .copied()
                    .max()
                    .unwrap_or(0)
            })
            .sum();
        topic.size_bytes = Some(size);
    }
}

/// Computes consumer group lag by querying partition watermarks and committed offsets.
pub struct MetricsService;

impl MetricsService {
    /// Calculates `PartitionLag` for a single partition.
    pub fn compute_partition_lag(
        topic: &str,
        partition: i32,
        committed_offset: i64,
        high_watermark: i64,
        low_watermark: i64,
    ) -> PartitionLag {
        let lag = calculate_partition_lag(committed_offset, high_watermark, low_watermark);
        PartitionLag {
            topic: topic.to_string(),
            partition,
            current_offset: committed_offset,
            high_watermark,
            low_watermark,
            lag,
            is_caught_up: lag == 0,
        }
    }

    /// Aggregates a list of `PartitionLag` items by topic into `TopicLagSummary`.
    pub fn aggregate_by_topic(partition_lags: Vec<PartitionLag>) -> Vec<TopicLagSummary> {
        let mut map: HashMap<String, Vec<PartitionLag>> = HashMap::new();
        for p in partition_lags {
            map.entry(p.topic.clone()).or_default().push(p);
        }

        let mut summaries: Vec<TopicLagSummary> = map
            .into_iter()
            .map(|(topic, mut partitions)| {
                partitions.sort_by_key(|p| p.partition);
                let total_lag: i64 = partitions.iter().map(|p| p.lag).sum();
                TopicLagSummary {
                    topic,
                    partitions,
                    total_lag,
                }
            })
            .collect();

        summaries.sort_by(|a, b| a.topic.cmp(&b.topic));
        summaries
    }

    /// Queries watermarks and committed offsets using a consumer client to compute real-time consumer group lag.
    ///
    /// Blocking: broker round trips per partition, and up to 3.5 s of thread
    /// sleep per partition while its leader moves (see
    /// [`KafkaConsumerClient::fetch_watermarks`]). Call it from a blocking
    /// context, e.g. `tokio::task::spawn_blocking`.
    pub fn fetch_consumer_group_lag(
        consumer: &KafkaConsumerClient,
        group_id: &str,
        topic_partitions: &[(&str, i32)],
        timeout: Option<Duration>,
    ) -> KafkaResult<ConsumerGroupLag> {
        let committed = consumer.fetch_committed_offsets_for(topic_partitions, timeout)?;

        let mut partition_lags = Vec::new();
        for &(t, p) in topic_partitions {
            let (low, high) = consumer.fetch_watermarks(t, p, timeout)?;
            let current_offset = committed
                .get(&(t.to_string(), p))
                .copied()
                .flatten()
                .unwrap_or(-1);

            partition_lags.push(Self::compute_partition_lag(t, p, current_offset, high, low));
        }

        let topic_lags = Self::aggregate_by_topic(partition_lags);
        let total_lag = topic_lags.iter().map(|t| t.total_lag).sum();

        Ok(ConsumerGroupLag {
            group_id: group_id.to_string(),
            state: "Active".to_string(),
            topic_lags,
            total_lag,
        })
    }

    /// Converts detailed `ConsumerGroupLag` into Ferrika's UI domain `ConsumerGroupSummary`.
    pub fn to_model_summary(lag: &ConsumerGroupLag, active_members: usize) -> ConsumerGroupSummary {
        let mut target_topics: Vec<String> =
            lag.topic_lags.iter().map(|t| t.topic.clone()).collect();
        target_topics.sort();

        let current_offset: i64 = lag
            .topic_lags
            .iter()
            .flat_map(|t| &t.partitions)
            .map(|p| p.current_offset.max(0))
            .sum();

        let end_offset: i64 = lag
            .topic_lags
            .iter()
            .flat_map(|t| &t.partitions)
            .map(|p| p.high_watermark)
            .sum();

        let state = ConsumerState::from_broker_state(&lag.state);

        ConsumerGroupSummary {
            group_id: lag.group_id.clone(),
            state,
            active_members,
            target_topics,
            current_offset,
            end_offset,
            total_lag: lag.total_lag,
            coordinator_node: "-".to_string(),
            consume_rate: None,
            partitions: lag
                .topic_lags
                .iter()
                .flat_map(|t| &t.partitions)
                .map(|p| GroupPartitionLag {
                    topic: p.topic.clone(),
                    partition: p.partition,
                    committed: (p.current_offset >= 0).then_some(p.current_offset),
                    end_offset: p.high_watermark,
                    lag: p.lag,
                    behind_ms: None,
                    owner: None,
                })
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_partition_lag_normal() {
        // High watermark 100, committed 90 -> lag 10
        assert_eq!(calculate_partition_lag(90, 100, 0), 10);
        // Fully caught up
        assert_eq!(calculate_partition_lag(100, 100, 0), 0);
    }

    #[test]
    fn test_calculate_partition_lag_empty_partition() {
        assert_eq!(calculate_partition_lag(0, 0, 0), 0);
        assert_eq!(calculate_partition_lag(-1, 0, 0), 0);
    }

    #[test]
    fn test_calculate_partition_lag_uncommitted() {
        // Uncommitted (-1), watermarks 20 to 100 -> lag 80
        assert_eq!(calculate_partition_lag(-1001, 100, 20), 80);
        // Uncommitted (-1), watermarks 0 to 500 -> lag 500
        assert_eq!(calculate_partition_lag(-1, 500, 0), 500);
    }

    #[test]
    fn test_calculate_partition_lag_ahead_of_watermark() {
        // Committed offset ahead of high watermark (e.g. truncated) -> lag 0
        assert_eq!(calculate_partition_lag(150, 100, 0), 0);
    }

    #[test]
    fn test_partition_and_topic_depth() {
        assert_eq!(partition_depth(20, 100), 80);
        assert_eq!(partition_depth(0, 0), 0);
        // Watermarks read out of order never count negative.
        assert_eq!(partition_depth(100, 20), 0);
        assert_eq!(topic_depth([(0, 10), (5, 5), (3, 1), (40, 100)]), 70);
        assert_eq!(topic_depth(std::iter::empty()), 0);
    }

    #[test]
    fn test_rate_per_sec_ignores_resets_and_first_samples() {
        assert_eq!(rate_per_sec(Some(100), 160, 2.0), Some(30.0));
        assert_eq!(rate_per_sec(Some(100), 40, 2.0), Some(0.0));
        assert_eq!(rate_per_sec(None, 40, 2.0), None);
        assert_eq!(rate_per_sec(Some(1), 2, 0.0), None);
    }

    #[test]
    fn test_apply_disk_usage_counts_one_copy_per_partition() {
        let mut topics = crate::model::cluster::ClusterSnapshot::demo().topics;
        topics.truncate(1);
        let name = topics[0].name.clone();
        let mut brokers = crate::model::cluster::ClusterSnapshot::demo().brokers;
        let mut dirs = HashMap::new();
        for (id, size) in [(1, 100u64), (2, 120)] {
            let mut d = BrokerLogDirs::default();
            d.partitions.insert((name.clone(), 0), size);
            dirs.insert(id, d);
        }
        apply_disk_usage(&mut topics, &mut brokers, &dirs);
        assert_eq!(topics[0].size_bytes, Some(120));
        assert_eq!(brokers[0].log_dir_bytes, Some(100));
        assert_eq!(brokers[2].log_dir_bytes, None);
    }

    #[test]
    fn test_apply_disk_usage_leaves_sizes_unknown_without_answers() {
        let mut topics = crate::model::cluster::ClusterSnapshot::demo().topics;
        let mut brokers = crate::model::cluster::ClusterSnapshot::demo().brokers;
        let before: Vec<_> = topics.iter().map(|t| t.size_bytes).collect();
        apply_disk_usage(&mut topics, &mut brokers, &HashMap::new());
        let after: Vec<_> = topics.iter().map(|t| t.size_bytes).collect();
        assert_eq!(before, after);
    }

    #[test]
    fn test_aggregate_by_topic() {
        let lags = vec![
            PartitionLag {
                topic: "orders".into(),
                partition: 0,
                current_offset: 100,
                high_watermark: 120,
                low_watermark: 0,
                lag: 20,
                is_caught_up: false,
            },
            PartitionLag {
                topic: "orders".into(),
                partition: 1,
                current_offset: 50,
                high_watermark: 50,
                low_watermark: 0,
                lag: 0,
                is_caught_up: true,
            },
            PartitionLag {
                topic: "users".into(),
                partition: 0,
                current_offset: 10,
                high_watermark: 15,
                low_watermark: 0,
                lag: 5,
                is_caught_up: false,
            },
        ];

        let summaries = MetricsService::aggregate_by_topic(lags);
        assert_eq!(summaries.len(), 2);

        let orders = summaries.iter().find(|s| s.topic == "orders").unwrap();
        assert_eq!(orders.total_lag, 20);
        assert_eq!(orders.partitions.len(), 2);

        let users = summaries.iter().find(|s| s.topic == "users").unwrap();
        assert_eq!(users.total_lag, 5);
        assert_eq!(users.partitions.len(), 1);
    }

    #[test]
    fn test_to_model_summary() {
        let topic_lag = TopicLagSummary {
            topic: "events".into(),
            partitions: vec![
                PartitionLag {
                    topic: "events".into(),
                    partition: 0,
                    current_offset: 80,
                    high_watermark: 100,
                    low_watermark: 0,
                    lag: 20,
                    is_caught_up: false,
                },
                PartitionLag {
                    topic: "events".into(),
                    partition: 1,
                    current_offset: 95,
                    high_watermark: 100,
                    low_watermark: 0,
                    lag: 5,
                    is_caught_up: false,
                },
            ],
            total_lag: 25,
        };

        let group_lag = ConsumerGroupLag {
            group_id: "event-processor".into(),
            state: "Stable".into(),
            topic_lags: vec![topic_lag],
            total_lag: 25,
        };

        let summary = MetricsService::to_model_summary(&group_lag, 2);
        assert_eq!(summary.group_id, "event-processor");
        assert_eq!(summary.state, ConsumerState::Stable);
        assert_eq!(summary.active_members, 2);
        assert_eq!(summary.total_lag, 25);
        assert_eq!(summary.current_offset, 175);
        assert_eq!(summary.end_offset, 200);
        assert_eq!(summary.target_topics, vec!["events"]);
    }
}
