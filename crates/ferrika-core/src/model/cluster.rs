use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrokerInfo {
    pub id: i32,
    pub host: String,
    pub port: u16,
    pub is_controller: bool,
    pub partition_leaders: usize,
    pub replicas_hosted: usize,
    pub is_online: bool,
    /// Bytes stored in the broker's log dirs; `None` until measured.
    #[serde(default)]
    pub log_dir_bytes: Option<u64>,
    /// Each log dir with its size, or why it failed; empty until measured.
    #[serde(default)]
    pub log_dirs: Vec<LogDirSummary>,
}

/// One log dir of a broker (DescribeLogDirs).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogDirSummary {
    pub path: String,
    pub bytes: u64,
    /// Set when the broker reports the dir as failed (e.g. KAFKA_STORAGE_ERROR).
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionDetail {
    pub partition_id: i32,
    pub leader: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
    pub high_watermark: i64,
    /// First retained offset; 0 when unknown.
    #[serde(default)]
    pub low_watermark: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicSummary {
    pub name: String,
    pub partition_count: usize,
    pub replication_factor: usize,
    pub total_messages: i64,
    pub cleanup_policy: String,
    pub is_internal: bool,
    /// Bytes on disk for one copy of the topic (largest replica of each
    /// partition); `None` until measured.
    #[serde(default)]
    pub size_bytes: Option<u64>,
    /// Records produced per second since the previous poll; `None` until
    /// two polls have been seen.
    #[serde(default)]
    pub msg_rate: Option<f64>,
    pub partitions: Vec<PartitionDetail>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConsumerState {
    Stable,
    Rebalancing,
    /// The group exists (it has committed offsets) but has no members.
    Empty,
    Dead,
}

impl ConsumerState {
    pub fn as_str(&self) -> &'static str {
        match self {
            ConsumerState::Stable => "Stable",
            ConsumerState::Rebalancing => "Rebalancing",
            ConsumerState::Empty => "Empty",
            ConsumerState::Dead => "Dead",
        }
    }

    /// Parses a group state as reported by the broker (`PreparingRebalance`)
    /// or by Ferrika (`preparing_rebalance`), ignoring case.
    pub fn from_broker_state(state: &str) -> Self {
        let normalized: String = state
            .chars()
            .filter(|c| *c != '_')
            .flat_map(char::to_lowercase)
            .collect();
        match normalized.as_str() {
            "stable" => ConsumerState::Stable,
            "rebalancing" | "preparingrebalance" | "completingrebalance" => {
                ConsumerState::Rebalancing
            }
            "empty" => ConsumerState::Empty,
            _ => ConsumerState::Dead,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerGroupSummary {
    pub group_id: String,
    pub state: ConsumerState,
    pub active_members: usize,
    pub target_topics: Vec<String>,
    pub current_offset: i64,
    pub end_offset: i64,
    pub total_lag: i64,
    pub coordinator_node: String,
    /// Records committed per second since the previous poll (consumption speed).
    #[serde(default)]
    pub consume_rate: Option<f64>,
    /// Per-partition offsets of the topics this group has committed to.
    #[serde(default)]
    pub partitions: Vec<GroupPartitionLag>,
}

/// Committed position and lag of a consumer group on one partition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupPartitionLag {
    pub topic: String,
    pub partition: i32,
    /// `None` when the group has not committed on this partition.
    pub committed: Option<i64>,
    pub end_offset: i64,
    pub lag: i64,
    /// The member consuming this partition (`client-id @ host`), if assigned.
    #[serde(default)]
    pub owner: Option<String>,
    /// How long ago the first unconsumed record was written; `None` until
    /// measured (only for the group shown in detail).
    #[serde(default)]
    pub behind_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterMetrics {
    pub total_brokers: usize,
    pub online_brokers: usize,
    pub controller_broker_id: i32,
    pub active_topics: usize,
    pub total_partitions: usize,
    pub isr_sync_rate_pct: u8,
    pub under_replicated_partitions: usize,
    pub total_consumer_lag: i64,
    pub active_consumer_groups: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SampleMessage {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub key: Option<String>,
    pub payload: String,
    pub timestamp_ms: i64,
    /// Record headers as UTF-8 (lossy) key/value pairs, in record order.
    #[serde(default)]
    pub headers: Vec<(String, String)>,
    /// The payload bytes when they are not valid UTF-8 (`payload` is then a
    /// lossy copy); `None` when `payload` holds them exactly.
    #[serde(skip)]
    pub raw: Option<std::sync::Arc<[u8]>>,
    /// How the payload was decoded from a Schema Registry wire format,
    /// e.g. "AVRO schema 12"; `None` for plain payloads.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    /// The registry schema the payload was decoded with (what `schema`
    /// describes); `None` for plain payloads.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_ref: Option<crate::model::schema::SchemaRef>,
}

impl SampleMessage {
    /// The payload exactly as stored in Kafka.
    pub fn payload_bytes(&self) -> &[u8] {
        self.raw.as_deref().unwrap_or(self.payload.as_bytes())
    }
}

/// A partition whose replicas are being moved (ListPartitionReassignments).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OngoingReassignment {
    pub topic: String,
    pub partition: i32,
    /// The full replica set during the move (old and new replicas).
    pub replicas: Vec<i32>,
    pub adding: Vec<i32>,
    pub removing: Vec<i32>,
}

/// One replica of the KRaft metadata log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuorumReplica {
    pub id: i32,
    /// `-1` when the leader has not heard from the replica.
    pub log_end_offset: i64,
}

/// State of the KRaft metadata quorum (DescribeQuorum).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuorumInfo {
    pub leader_id: i32,
    pub leader_epoch: i32,
    pub high_watermark: i64,
    pub voters: Vec<QuorumReplica>,
    pub observers: Vec<QuorumReplica>,
}

impl QuorumInfo {
    /// How far `replica` is behind the high watermark (`None` if unknown).
    pub fn lag(&self, replica: &QuorumReplica) -> Option<i64> {
        (replica.log_end_offset >= 0).then(|| (self.high_watermark - replica.log_end_offset).max(0))
    }
}

#[derive(Debug, Clone)]
pub struct ClusterSnapshot {
    pub cluster_name: String,
    pub bootstrap_server: String,
    pub cluster_id: String,
    pub kafka_version: String,
    pub metrics: ClusterMetrics,
    pub brokers: Vec<BrokerInfo>,
    pub topics: Vec<TopicSummary>,
    pub consumers: Vec<ConsumerGroupSummary>,
    pub sample_messages: Vec<SampleMessage>,
    /// KRaft metadata quorum; `None` on ZooKeeper clusters or when unknown.
    pub quorum: Option<QuorumInfo>,
    /// Partition moves in progress.
    pub reassignments: Vec<OngoingReassignment>,
}

impl ClusterSnapshot {
    /// A made-up cluster for demos, screenshots and tests.
    pub fn demo() -> Self {
        let brokers = vec![
            BrokerInfo {
                id: 1,
                host: "localhost".into(),
                port: 9092,
                is_controller: true,
                partition_leaders: 12,
                replicas_hosted: 36,
                is_online: true,
                log_dir_bytes: None,
                log_dirs: Vec::new(),
            },
            BrokerInfo {
                id: 2,
                host: "localhost".into(),
                port: 9093,
                is_controller: false,
                partition_leaders: 12,
                replicas_hosted: 36,
                is_online: true,
                log_dir_bytes: None,
                log_dirs: Vec::new(),
            },
            BrokerInfo {
                id: 3,
                host: "localhost".into(),
                port: 9094,
                is_controller: false,
                partition_leaders: 12,
                replicas_hosted: 36,
                is_online: true,
                log_dir_bytes: None,
                log_dirs: Vec::new(),
            },
        ];

        let topics = vec![
            TopicSummary {
                name: "order-events".into(),
                partition_count: 3,
                replication_factor: 3,
                total_messages: 1_420_591,
                cleanup_policy: "delete".into(),
                is_internal: false,
                size_bytes: None,
                msg_rate: None,
                partitions: vec![
                    PartitionDetail {
                        partition_id: 0,
                        leader: 1,
                        replicas: vec![1, 2, 3],
                        isr: vec![1, 2, 3],
                        high_watermark: 473_530,
                        low_watermark: 0,
                    },
                    PartitionDetail {
                        partition_id: 1,
                        leader: 2,
                        replicas: vec![2, 3, 1],
                        isr: vec![2, 3, 1],
                        high_watermark: 473_520,
                        low_watermark: 0,
                    },
                    PartitionDetail {
                        partition_id: 2,
                        leader: 3,
                        replicas: vec![3, 1, 2],
                        isr: vec![3, 1, 2],
                        high_watermark: 473_541,
                        low_watermark: 0,
                    },
                ],
            },
            TopicSummary {
                name: "payment-completed".into(),
                partition_count: 3,
                replication_factor: 3,
                total_messages: 894_120,
                cleanup_policy: "delete".into(),
                is_internal: false,
                size_bytes: None,
                msg_rate: None,
                partitions: vec![
                    PartitionDetail {
                        partition_id: 0,
                        leader: 1,
                        replicas: vec![1, 2, 3],
                        isr: vec![1, 2, 3],
                        high_watermark: 298_040,
                        low_watermark: 0,
                    },
                    PartitionDetail {
                        partition_id: 1,
                        // Led by a follower although the preferred replica is
                        // in sync: the config check demo flags it (T014).
                        leader: 3,
                        replicas: vec![2, 3, 1],
                        isr: vec![2, 3, 1],
                        high_watermark: 298_040,
                        low_watermark: 0,
                    },
                    PartitionDetail {
                        partition_id: 2,
                        leader: 3,
                        replicas: vec![3, 1, 2],
                        isr: vec![3, 1, 2],
                        high_watermark: 298_040,
                        low_watermark: 0,
                    },
                ],
            },
            TopicSummary {
                name: "user-notifications".into(),
                partition_count: 6,
                // One copy only: the config check demo flags it (T001).
                replication_factor: 1,
                total_messages: 3_110_402,
                cleanup_policy: "delete".into(),
                is_internal: false,
                size_bytes: None,
                msg_rate: None,
                partitions: vec![],
            },
            TopicSummary {
                name: "inventory-state".into(),
                partition_count: 2,
                replication_factor: 3,
                total_messages: 45_012,
                cleanup_policy: "compact".into(),
                is_internal: false,
                size_bytes: None,
                msg_rate: None,
                partitions: vec![],
            },
            TopicSummary {
                name: "__consumer_offsets".into(),
                partition_count: 50,
                replication_factor: 3,
                total_messages: 520_190,
                cleanup_policy: "compact".into(),
                is_internal: true,
                size_bytes: None,
                msg_rate: None,
                partitions: vec![],
            },
        ];

        let consumers = vec![
            ConsumerGroupSummary {
                group_id: "order-processor-v1".into(),
                state: ConsumerState::Stable,
                active_members: 3,
                target_topics: vec!["order-events".into()],
                current_offset: 1_420_577,
                end_offset: 1_420_591,
                total_lag: 14,
                coordinator_node: "broker-1:9092".into(),
                consume_rate: None,
                partitions: Vec::new(),
            },
            ConsumerGroupSummary {
                group_id: "notification-dispatch-group".into(),
                state: ConsumerState::Stable,
                active_members: 6,
                target_topics: vec!["user-notifications".into()],
                current_offset: 3_110_402,
                end_offset: 3_110_402,
                total_lag: 0,
                coordinator_node: "broker-2:9092".into(),
                consume_rate: None,
                partitions: Vec::new(),
            },
            ConsumerGroupSummary {
                group_id: "analytics-etl-pipeline".into(),
                state: ConsumerState::Rebalancing,
                active_members: 2,
                target_topics: vec!["payment-completed".into()],
                current_offset: 892_840,
                end_offset: 894_120,
                total_lag: 1280,
                coordinator_node: "broker-3:9092".into(),
                consume_rate: None,
                partitions: Vec::new(),
            },
            ConsumerGroupSummary {
                group_id: "audit-logging-worker".into(),
                state: ConsumerState::Stable,
                active_members: 1,
                target_topics: vec!["order-events".into(), "payment-completed".into()],
                current_offset: 2_314_711,
                end_offset: 2_314_711,
                total_lag: 0,
                coordinator_node: "broker-1:9092".into(),
                consume_rate: None,
                partitions: Vec::new(),
            },
        ];

        let metrics = ClusterMetrics {
            total_brokers: 3,
            online_brokers: 3,
            controller_broker_id: 1,
            active_topics: 12,
            total_partitions: 36,
            isr_sync_rate_pct: 100,
            under_replicated_partitions: 0,
            total_consumer_lag: 14,
            active_consumer_groups: 4,
        };

        let sample_messages = vec![
            SampleMessage {
                topic: "order-events".into(),
                partition: 1,
                offset: 473_520,
                key: Some("ord_99214A".into()),
                payload: r#"{
  "order_id": "ord_99214A",
  "user_id": "usr_4821",
  "customer": {
    "name": "김민수",
    "phone": "010-1234-5678",
    "email": "minsu.kim@example.com",
    "resident_no": "900101-1234567",
    "refund_account": "110-123-456789"
  },
  "amount": 49000,
  "currency": "KRW",
  "status": "CREATED",
  "timestamp": 1726921800120
}"#
                .into(),
                timestamp_ms: 1726921800120,
                headers: Vec::new(),
                raw: None,
                schema: None,
                schema_ref: None,
            },
            SampleMessage {
                topic: "order-events".into(),
                partition: 2,
                offset: 473_521,
                key: Some("ord_99215B".into()),
                payload: r#"{
  "order_id": "ord_99215B",
  "user_id": "usr_7712",
  "amount": 79000,
  "currency": "KRW",
  "items": ["Rust Programming Masterclass", "Ferrika License"],
  "status": "ORDER_SUBMITTED"
}"#
                .into(),
                timestamp_ms: 1726921815300,
                headers: vec![
                    (
                        "traceparent".into(),
                        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".into(),
                    ),
                    ("content-type".into(), "application/json".into()),
                ],
                raw: None,
                schema: None,
                schema_ref: None,
            },
            SampleMessage {
                topic: "payment-completed".into(),
                partition: 0,
                offset: 298_041,
                key: Some("pay_8801A".into()),
                payload: r#"{
  "payment_id": "pay_8801A",
  "order_id": "ord_99215B",
  "method": "CREDIT_CARD",
  "gateway_ref": "pg_toss_991823",
  "amount": 79000,
  "status": "SETTLED"
}"#
                .into(),
                timestamp_ms: 1726921820450,
                headers: Vec::new(),
                raw: None,
                schema: None,
                schema_ref: None,
            },
            SampleMessage {
                topic: "user-notifications".into(),
                partition: 3,
                offset: 3_110_403,
                key: Some("usr_7712".into()),
                payload: r#"{
  "notification_id": "notif_3321",
  "channel": "KAKAO_ALIMTALK",
  "recipient": "usr_7712",
  "title": "Payment Confirmed",
  "delivered": true
}"#
                .into(),
                timestamp_ms: 1726921822100,
                headers: Vec::new(),
                raw: None,
                schema: None,
                schema_ref: None,
            },
            SampleMessage {
                topic: "orders-v1-dlq".into(),
                partition: 0,
                offset: 104,
                key: Some("ord_99999X".into()),
                payload: r#"{
  "error": "JsonParseException",
  "raw": "{ malformed_json_field : missing_quote }",
  "retry_count": 3
}"#
                .into(),
                timestamp_ms: 1726921830000,
                headers: Vec::new(),
                raw: None,
                schema: None,
                schema_ref: None,
            },
        ];

        let mut data = Self {
            cluster_name: "Localhost:9092 (Dev)".into(),
            bootstrap_server: "PLAINTEXT://localhost:9092".into(),
            cluster_id: "4L622nQISt-yTwP0Mc15PQ".into(),
            kafka_version: "3.6.0".into(),
            metrics,
            brokers,
            topics,
            consumers,
            sample_messages,
            quorum: Some(QuorumInfo {
                leader_id: 1,
                leader_epoch: 12,
                high_watermark: 48_210,
                voters: [(1, 48_210), (2, 48_210), (3, 48_190)]
                    .into_iter()
                    .map(|(id, log_end_offset)| QuorumReplica { id, log_end_offset })
                    .collect(),
                observers: Vec::new(),
            }),
            reassignments: Vec::new(),
        };
        data.fill_mock_group_partitions();
        // Keep the summary numbers consistent with the demo topics shown in tables.
        data.metrics.active_topics = data.topics.iter().filter(|t| !t.is_internal).count();
        data.metrics.total_partitions = data.topics.iter().map(|t| t.partition_count).sum();
        data
    }

    /// A cluster with no data yet, shown until the first metadata poll completes.
    pub fn empty(cluster_name: impl Into<String>, bootstrap_server: impl Into<String>) -> Self {
        Self {
            cluster_name: cluster_name.into(),
            bootstrap_server: bootstrap_server.into(),
            cluster_id: String::new(),
            kafka_version: String::new(),
            metrics: ClusterMetrics {
                total_brokers: 0,
                online_brokers: 0,
                controller_broker_id: -1,
                active_topics: 0,
                total_partitions: 0,
                isr_sync_rate_pct: 100,
                under_replicated_partitions: 0,
                total_consumer_lag: 0,
                active_consumer_groups: 0,
            },
            brokers: Vec::new(),
            topics: Vec::new(),
            consumers: Vec::new(),
            sample_messages: Vec::new(),
            quorum: None,
            reassignments: Vec::new(),
        }
    }

    /// Spreads each demo group's lag over the partitions of its topics.
    fn fill_mock_group_partitions(&mut self) {
        for group in &mut self.consumers {
            let partitions: Vec<(String, i64)> = group
                .target_topics
                .iter()
                .flat_map(|name| {
                    self.topics
                        .iter()
                        .filter(move |t| &t.name == name)
                        .flat_map(|t| {
                            t.partitions
                                .iter()
                                .map(move |p| (t.name.clone(), i64::from(p.partition_id)))
                        })
                })
                .collect();
            let count = partitions.len().max(1) as i64;
            group.partitions = partitions
                .into_iter()
                .enumerate()
                .map(|(i, (topic, partition))| {
                    let end = group.end_offset / count;
                    // Put the remainder on the first partition so the lags add up.
                    let lag =
                        group.total_lag / count + if i == 0 { group.total_lag % count } else { 0 };
                    GroupPartitionLag {
                        topic,
                        partition: partition as i32,
                        committed: Some(end - lag),
                        end_offset: end,
                        lag,
                        behind_ms: None,
                        owner: (group.active_members > 0).then(|| {
                            format!(
                                "{}-{} @ 10.0.0.{}",
                                group.group_id,
                                i % group.active_members,
                                10 + i % group.active_members
                            )
                        }),
                    }
                })
                .collect();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mock_group_partition_lag_adds_up() {
        let data = ClusterSnapshot::demo();
        for group in &data.consumers {
            let sum: i64 = group.partitions.iter().map(|p| p.lag).sum();
            assert_eq!(sum, group.total_lag, "{}", group.group_id);
        }
    }

    #[test]
    fn test_consumer_state_from_broker_state() {
        use ConsumerState::*;
        assert_eq!(ConsumerState::from_broker_state("Stable"), Stable);
        assert_eq!(
            ConsumerState::from_broker_state("PreparingRebalance"),
            Rebalancing
        );
        assert_eq!(
            ConsumerState::from_broker_state("completing_rebalance"),
            Rebalancing
        );
        assert_eq!(ConsumerState::from_broker_state("Empty"), Empty);
        assert_eq!(ConsumerState::from_broker_state("Dead"), Dead);
        assert_eq!(ConsumerState::from_broker_state("Unknown"), Dead);
    }
}
