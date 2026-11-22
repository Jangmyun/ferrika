use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Instant;

/// Sorting criteria for the top dashboard
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum TopSortOrder {
    #[default]
    Lag,
    Throughput,
    Name,
}

impl TopSortOrder {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Lag => "Lag",
            Self::Throughput => "Throughput",
            Self::Name => "Name",
        }
    }
}

/// Dynamic rate calculation engine tracking per-second deltas across refresh cycles
#[derive(Debug, Default)]
pub struct RateTracker {
    prev_state: HashMap<String, (i64, Instant)>,
}

impl RateTracker {
    pub fn new() -> Self {
        Self {
            prev_state: HashMap::new(),
        }
    }

    /// Computes rate per second for a tracked metric key
    pub fn track(&mut self, key: &str, current_val: i64) -> f64 {
        let now = Instant::now();
        if let Some((prev_val, prev_time)) =
            self.prev_state.insert(key.to_string(), (current_val, now))
        {
            let elapsed_secs = now.duration_since(prev_time).as_secs_f64();
            if elapsed_secs > 0.001 {
                let delta = (current_val - prev_val).max(0);
                (delta as f64) / elapsed_secs
            } else {
                0.0
            }
        } else {
            0.0
        }
    }
}

/// Real-time metrics for a monitored topic in Top view
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopTopicItem {
    pub name: String,
    pub partitions: usize,
    pub total_messages: i64,
    pub msg_rate_per_sec: f64,
}

/// Real-time metrics for a consumer group in Top view
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopConsumerItem {
    pub group_id: String,
    pub state: String,
    pub total_lag: i64,
    pub members: usize,
    pub target_topics: Vec<String>,
}

/// Complete instant snapshot of Kafka cluster health and resource consumption
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopClusterSnapshot {
    pub timestamp_epoch_s: u64,
    pub brokers_online: usize,
    pub brokers_total: usize,
    pub controller_id: i32,
    pub active_topics_count: usize,
    pub total_partitions: usize,
    pub total_lag: i64,
    pub topics: Vec<TopTopicItem>,
    pub consumers: Vec<TopConsumerItem>,
    /// What could not be read (the group list, committed or end offsets):
    /// lag and groups shown are then incomplete. Empty when all was read.
    #[serde(default)]
    pub warnings: Vec<String>,
}

impl TopClusterSnapshot {
    /// Sorts top elements according to specified criteria
    pub fn sort_data(&mut self, order: TopSortOrder) {
        match order {
            TopSortOrder::Lag => {
                self.consumers
                    .sort_by_key(|c| std::cmp::Reverse(c.total_lag));
            }
            TopSortOrder::Throughput => {
                self.topics.sort_by(|a, b| {
                    b.msg_rate_per_sec
                        .partial_cmp(&a.msg_rate_per_sec)
                        .unwrap_or(std::cmp::Ordering::Equal)
                });
            }
            TopSortOrder::Name => {
                self.topics.sort_by(|a, b| a.name.cmp(&b.name));
                self.consumers.sort_by(|a, b| a.group_id.cmp(&b.group_id));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rate_tracker_computation() {
        let mut tracker = RateTracker::new();
        let r1 = tracker.track("topic-orders", 1000);
        assert_eq!(r1, 0.0); // First tick has no prior baseline

        std::thread::sleep(std::time::Duration::from_millis(50));
        let r2 = tracker.track("topic-orders", 1100);
        assert!(r2 > 0.0); // ~2000 msg/sec
    }

    #[test]
    fn test_top_cluster_snapshot_sorting() {
        let mut snapshot = TopClusterSnapshot {
            timestamp_epoch_s: 1700000000,
            brokers_online: 3,
            brokers_total: 3,
            controller_id: 1,
            active_topics_count: 2,
            total_partitions: 6,
            total_lag: 500,
            topics: vec![
                TopTopicItem {
                    name: "z-topic".to_string(),
                    partitions: 3,
                    total_messages: 5000,
                    msg_rate_per_sec: 120.0,
                },
                TopTopicItem {
                    name: "a-topic".to_string(),
                    partitions: 3,
                    total_messages: 2000,
                    msg_rate_per_sec: 450.0,
                },
            ],
            consumers: vec![
                TopConsumerItem {
                    group_id: "cg-low-lag".to_string(),
                    state: "Stable".to_string(),
                    total_lag: 10,
                    members: 2,
                    target_topics: vec!["a-topic".to_string()],
                },
                TopConsumerItem {
                    group_id: "cg-high-lag".to_string(),
                    state: "Stable".to_string(),
                    total_lag: 490,
                    members: 3,
                    target_topics: vec!["z-topic".to_string()],
                },
            ],
            warnings: Vec::new(),
        };

        // Sort by Lag (default)
        snapshot.sort_data(TopSortOrder::Lag);
        assert_eq!(snapshot.consumers[0].group_id, "cg-high-lag");

        // Sort by Throughput
        snapshot.sort_data(TopSortOrder::Throughput);
        assert_eq!(snapshot.topics[0].name, "a-topic");

        // Sort by Name
        snapshot.sort_data(TopSortOrder::Name);
        assert_eq!(snapshot.topics[0].name, "a-topic");
        assert_eq!(snapshot.consumers[0].group_id, "cg-high-lag");
    }
}
