use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use ferrika_core::kafka::admin::KafkaAdminClient;
use ferrika_core::kafka::profile::ClusterProfile;

/// Health status classification for a Kafka cluster ping.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClusterHealthStatus {
    /// Broker metadata responded promptly and a controller is recognized.
    Healthy,
    /// Responded but reporting 0 active brokers or negative controller ID.
    Degraded,
    /// Connection timed out, refused, or failed authentication.
    Unreachable(String),
}

/// Result of pinging and benchmarking a cluster connection.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClusterPingResult {
    /// Name or profile alias of the cluster.
    pub cluster_name: String,
    /// Bootstrap servers configured for this cluster.
    pub bootstrap_servers: String,
    /// Round-trip time in milliseconds to connect and fetch metadata.
    pub rtt_ms: f64,
    /// Number of online brokers observed.
    pub broker_count: usize,
    /// Broker ID of the active cluster controller.
    pub controller_id: i32,
    /// Health status classification.
    pub status: ClusterHealthStatus,
}

/// Parity comparison state for an individual topic across two clusters.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TopicParityState {
    /// Topic exists in both clusters with identical partition count and replication factor.
    Synced,
    /// Partition count differs between clusters.
    PartitionMismatch {
        src_partitions: usize,
        dst_partitions: usize,
    },
    /// Replication factor differs between clusters.
    ReplicationMismatch { src_rf: usize, dst_rf: usize },
    /// Both partition count and replication factor differ.
    BothMismatch {
        src_partitions: usize,
        dst_partitions: usize,
        src_rf: usize,
        dst_rf: usize,
    },
    /// Topic is present in source cluster but completely missing in destination cluster.
    MissingInTarget,
    /// Topic is present in destination cluster but missing in source cluster.
    MissingInSource,
}

/// Parity comparison detail for an individual topic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicSyncItem {
    /// Topic name.
    pub topic_name: String,
    /// Partition count in source cluster (if topic exists there).
    pub src_partitions: Option<usize>,
    /// Replication factor in source cluster.
    pub src_replication_factor: Option<usize>,
    /// Partition count in destination cluster (if topic exists there).
    pub dst_partitions: Option<usize>,
    /// Replication factor in destination cluster.
    pub dst_replication_factor: Option<usize>,
    /// Comparison classification.
    pub status: TopicParityState,
}

/// Summary of a cross-cluster metadata diff.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClusterDiffSummary {
    /// Name/profile of source cluster.
    pub src_cluster_name: String,
    /// Name/profile of destination cluster.
    pub dst_cluster_name: String,
    /// Total topics in source cluster (excluding internal topics if filtered).
    pub src_topic_count: usize,
    /// Total topics in destination cluster.
    pub dst_topic_count: usize,
    /// Number of topics perfectly synchronized.
    pub synced_count: usize,
    /// Number of topics with partition or replication factor drift.
    pub drifted_count: usize,
    /// Topics missing in destination cluster.
    pub missing_in_dst_count: usize,
    /// Topics present only in destination cluster.
    pub missing_in_src_count: usize,
    /// Overall Parity Score percentage (0.0% to 100.0%).
    pub parity_score_pct: f64,
    /// Individual topic diff records.
    pub items: Vec<TopicSyncItem>,
}

/// Suggested action to reconcile a destination cluster with the source cluster.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemediationStep {
    /// Topic requiring action.
    pub topic_name: String,
    /// Human-readable explanation of required change.
    pub description: String,
    /// Suggested ferrika CLI command to execute the remediation.
    pub cli_command: String,
}

/// Healthy only when brokers answered and a controller was identified;
/// otherwise Degraded (`controller_id < 0` means it could not be determined).
pub fn classify_health(broker_count: usize, controller_id: i32) -> ClusterHealthStatus {
    if broker_count == 0 || controller_id < 0 {
        ClusterHealthStatus::Degraded
    } else {
        ClusterHealthStatus::Healthy
    }
}

/// `(partitions, replication_factor)` of a topic from its per-partition
/// replica counts. The replication factor is the largest replica count so a
/// partition that is mid-reassignment (extra replicas) is not hidden by an
/// unaffected first partition. A topic without partitions yields `None`.
pub fn topic_shape(replica_counts: &[usize]) -> Option<(usize, usize)> {
    let rf = replica_counts.iter().copied().max()?;
    Some((replica_counts.len(), rf))
}

/// Pings a cluster profile and measures round-trip metadata retrieval latency.
pub async fn ping_cluster(profile: &ClusterProfile, timeout: Duration) -> ClusterPingResult {
    let start = Instant::now();
    let config = profile.to_kafka_config();

    match KafkaAdminClient::new(&config) {
        Ok(client) => match client.fetch_raw_metadata(None, Some(timeout)) {
            Ok(metadata) => {
                let rtt_ms = start.elapsed().as_secs_f64() * 1000.0;
                let broker_count = metadata.brokers().len();
                let controller_id = client.active_controller_id().unwrap_or(-1);

                let status = classify_health(broker_count, controller_id);

                ClusterPingResult {
                    cluster_name: profile.name.clone(),
                    bootstrap_servers: profile.bootstrap_servers.clone(),
                    rtt_ms,
                    broker_count,
                    controller_id,
                    status,
                }
            }
            Err(e) => {
                let rtt_ms = start.elapsed().as_secs_f64() * 1000.0;
                ClusterPingResult {
                    cluster_name: profile.name.clone(),
                    bootstrap_servers: profile.bootstrap_servers.clone(),
                    rtt_ms,
                    broker_count: 0,
                    controller_id: -1,
                    status: ClusterHealthStatus::Unreachable(e.to_string()),
                }
            }
        },
        Err(e) => {
            let rtt_ms = start.elapsed().as_secs_f64() * 1000.0;
            ClusterPingResult {
                cluster_name: profile.name.clone(),
                bootstrap_servers: profile.bootstrap_servers.clone(),
                rtt_ms,
                broker_count: 0,
                controller_id: -1,
                status: ClusterHealthStatus::Unreachable(e.to_string()),
            }
        }
    }
}

/// Pure computation function: compares topic maps from source and destination clusters.
/// Each map entry is `topic_name -> (partitions_count, replication_factor)`.
pub fn compute_cluster_diff(
    src_name: &str,
    src_topics: &HashMap<String, (usize, usize)>,
    dst_name: &str,
    dst_topics: &HashMap<String, (usize, usize)>,
    include_internal: bool,
) -> ClusterDiffSummary {
    let mut all_topic_names: BTreeMap<String, ()> = BTreeMap::new();

    for name in src_topics.keys() {
        if include_internal || !ferrika_core::kafka::internal_topic::is_internal_topic(name) {
            all_topic_names.insert(name.clone(), ());
        }
    }
    for name in dst_topics.keys() {
        if include_internal || !ferrika_core::kafka::internal_topic::is_internal_topic(name) {
            all_topic_names.insert(name.clone(), ());
        }
    }

    let mut items = Vec::new();
    let mut synced_count = 0;
    let mut drifted_count = 0;
    let mut missing_in_dst_count = 0;
    let mut missing_in_src_count = 0;

    for (topic_name, _) in all_topic_names {
        let src_spec = src_topics.get(&topic_name).copied();
        let dst_spec = dst_topics.get(&topic_name).copied();

        match (src_spec, dst_spec) {
            (Some((src_p, src_rf)), Some((dst_p, dst_rf))) => {
                if src_p == dst_p && src_rf == dst_rf {
                    synced_count += 1;
                    items.push(TopicSyncItem {
                        topic_name,
                        src_partitions: Some(src_p),
                        src_replication_factor: Some(src_rf),
                        dst_partitions: Some(dst_p),
                        dst_replication_factor: Some(dst_rf),
                        status: TopicParityState::Synced,
                    });
                } else if src_p != dst_p && src_rf != dst_rf {
                    drifted_count += 1;
                    items.push(TopicSyncItem {
                        topic_name,
                        src_partitions: Some(src_p),
                        src_replication_factor: Some(src_rf),
                        dst_partitions: Some(dst_p),
                        dst_replication_factor: Some(dst_rf),
                        status: TopicParityState::BothMismatch {
                            src_partitions: src_p,
                            dst_partitions: dst_p,
                            src_rf,
                            dst_rf,
                        },
                    });
                } else if src_p != dst_p {
                    drifted_count += 1;
                    items.push(TopicSyncItem {
                        topic_name,
                        src_partitions: Some(src_p),
                        src_replication_factor: Some(src_rf),
                        dst_partitions: Some(dst_p),
                        dst_replication_factor: Some(dst_rf),
                        status: TopicParityState::PartitionMismatch {
                            src_partitions: src_p,
                            dst_partitions: dst_p,
                        },
                    });
                } else {
                    drifted_count += 1;
                    items.push(TopicSyncItem {
                        topic_name,
                        src_partitions: Some(src_p),
                        src_replication_factor: Some(src_rf),
                        dst_partitions: Some(dst_p),
                        dst_replication_factor: Some(dst_rf),
                        status: TopicParityState::ReplicationMismatch { src_rf, dst_rf },
                    });
                }
            }
            (Some((src_p, src_rf)), None) => {
                missing_in_dst_count += 1;
                items.push(TopicSyncItem {
                    topic_name,
                    src_partitions: Some(src_p),
                    src_replication_factor: Some(src_rf),
                    dst_partitions: None,
                    dst_replication_factor: None,
                    status: TopicParityState::MissingInTarget,
                });
            }
            (None, Some((dst_p, dst_rf))) => {
                missing_in_src_count += 1;
                items.push(TopicSyncItem {
                    topic_name,
                    src_partitions: None,
                    src_replication_factor: None,
                    dst_partitions: Some(dst_p),
                    dst_replication_factor: Some(dst_rf),
                    status: TopicParityState::MissingInSource,
                });
            }
            (None, None) => {}
        }
    }

    let total_unique = items.len();
    let parity_score_pct = if total_unique == 0 {
        100.0
    } else {
        (synced_count as f64 / total_unique as f64) * 100.0
    };

    let src_topic_count = src_topics
        .keys()
        .filter(|k| include_internal || !ferrika_core::kafka::internal_topic::is_internal_topic(k))
        .count();
    let dst_topic_count = dst_topics
        .keys()
        .filter(|k| include_internal || !ferrika_core::kafka::internal_topic::is_internal_topic(k))
        .count();

    ClusterDiffSummary {
        src_cluster_name: src_name.to_string(),
        dst_cluster_name: dst_name.to_string(),
        src_topic_count,
        dst_topic_count,
        synced_count,
        drifted_count,
        missing_in_dst_count,
        missing_in_src_count,
        parity_score_pct,
        items,
    }
}

/// Generates automated remediation steps to bring the destination cluster in sync with the source cluster.
pub fn generate_remediation_plan(
    diff: &ClusterDiffSummary,
    dst_profile: &str,
) -> Vec<RemediationStep> {
    let mut plan = Vec::new();

    for item in &diff.items {
        match &item.status {
            TopicParityState::MissingInTarget => {
                let p = item.src_partitions.unwrap_or(1);
                let rf = item.src_replication_factor.unwrap_or(1);
                plan.push(RemediationStep {
                    topic_name: item.topic_name.clone(),
                    description: format!(
                        "Topic missing on {dst_profile}. Create topic with {p} partitions, RF {rf}."
                    ),
                    cli_command: format!(
                        "ferrika --profile {} topics create {} -p {} -r {}",
                        dst_profile, item.topic_name, p, rf
                    ),
                });
            }
            TopicParityState::PartitionMismatch {
                src_partitions,
                dst_partitions,
            } => {
                if src_partitions > dst_partitions {
                    plan.push(RemediationStep {
                        topic_name: item.topic_name.clone(),
                        description: format!(
                            "Partition count deficit on {dst_profile} (Source: {src_partitions}, Target: {dst_partitions}). Expand partition count."
                        ),
                        cli_command: format!(
                            "# Note: Expand partitions on destination\nkafka-topics.sh --bootstrap-server <target> --alter --topic {} --partitions {}",
                            item.topic_name, src_partitions
                        ),
                    });
                } else {
                    plan.push(RemediationStep {
                        topic_name: item.topic_name.clone(),
                        description: format!(
                            "Destination {dst_profile} has more partitions ({dst_partitions}) than source ({src_partitions}). Kafka partitions cannot be decreased."
                        ),
                        cli_command: format!(
                            "# Manual review required: source ({src_partitions}) vs target ({dst_partitions})"
                        ),
                    });
                }
            }
            TopicParityState::ReplicationMismatch { src_rf, dst_rf } => {
                plan.push(RemediationStep {
                    topic_name: item.topic_name.clone(),
                    description: format!(
                        "Replication factor mismatch on {dst_profile} (Source: {src_rf}, Target: {dst_rf}). Reassign replicas to match."
                    ),
                    cli_command: format!(
                        "ferrika --profile {} reassign plan -t {} --rf {} --brokers <broker-ids>",
                        dst_profile, item.topic_name, src_rf
                    ),
                });
            }
            TopicParityState::BothMismatch {
                src_partitions,
                dst_partitions,
                src_rf,
                dst_rf,
            } => {
                plan.push(RemediationStep {
                    topic_name: item.topic_name.clone(),
                    description: format!(
                        "Both partitions ({src_partitions} vs {dst_partitions}) and RF ({src_rf} vs {dst_rf}) differ on {dst_profile}."
                    ),
                    cli_command: format!(
                        "# Review partition count and reassign replicas for topic {}",
                        item.topic_name
                    ),
                });
            }
            TopicParityState::MissingInSource => {
                plan.push(RemediationStep {
                    topic_name: item.topic_name.clone(),
                    description: format!(
                        "Topic exists only on target {dst_profile}. Verify if it is an orphan topic."
                    ),
                    cli_command: format!(
                        "# Verify if {} should be deleted: ferrika --profile {} topics delete {}",
                        item.topic_name, dst_profile, item.topic_name
                    ),
                });
            }
            TopicParityState::Synced => {}
        }
    }

    plan
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn health_requires_brokers_and_controller() {
        assert_eq!(classify_health(3, 1), ClusterHealthStatus::Healthy);
        assert_eq!(classify_health(3, 0), ClusterHealthStatus::Healthy);
        assert_eq!(classify_health(3, -1), ClusterHealthStatus::Degraded);
        assert_eq!(classify_health(0, 1), ClusterHealthStatus::Degraded);
    }

    #[test]
    fn topic_shape_uses_max_rf_and_rejects_empty() {
        assert_eq!(topic_shape(&[]), None);
        assert_eq!(topic_shape(&[3, 3, 3]), Some((3, 3)));
        // Mid-reassignment: a later partition has an extra replica.
        assert_eq!(topic_shape(&[2, 3, 2]), Some((3, 3)));
    }

    #[test]
    fn schemas_topic_is_internal_in_cluster_diff() {
        let src = HashMap::from([("_schemas".to_string(), (1, 3)), ("a".to_string(), (1, 3))]);
        let dst = HashMap::from([("a".to_string(), (1, 3))]);
        let hidden = compute_cluster_diff("s", &src, "d", &dst, false);
        assert_eq!(hidden.src_topic_count, 1);
        assert_eq!(hidden.missing_in_dst_count, 0);
        let shown = compute_cluster_diff("s", &src, "d", &dst, true);
        assert_eq!(shown.src_topic_count, 2);
    }

    #[test]
    fn test_compute_cluster_diff_perfect_sync() {
        let mut src_topics = HashMap::new();
        src_topics.insert("orders-v1".to_string(), (6, 3));
        src_topics.insert("payments-v1".to_string(), (12, 3));

        let mut dst_topics = HashMap::new();
        dst_topics.insert("orders-v1".to_string(), (6, 3));
        dst_topics.insert("payments-v1".to_string(), (12, 3));

        let diff = compute_cluster_diff("prod", &src_topics, "dr", &dst_topics, false);

        assert_eq!(diff.src_topic_count, 2);
        assert_eq!(diff.dst_topic_count, 2);
        assert_eq!(diff.synced_count, 2);
        assert_eq!(diff.drifted_count, 0);
        assert_eq!(diff.missing_in_dst_count, 0);
        assert_eq!(diff.missing_in_src_count, 0);
        assert!((diff.parity_score_pct - 100.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_compute_cluster_diff_with_mismatches_and_missing() {
        let mut src_topics = HashMap::new();
        src_topics.insert("orders-v1".to_string(), (12, 3)); // drifted partition
        src_topics.insert("users-v1".to_string(), (6, 3)); // missing in target
        src_topics.insert("audit-logs".to_string(), (3, 3)); // rf mismatch
        src_topics.insert("both-diff".to_string(), (10, 3)); // both mismatch
        src_topics.insert("synced-topic".to_string(), (4, 2)); // synced
        src_topics.insert("__consumer_offsets".to_string(), (50, 3)); // internal

        let mut dst_topics = HashMap::new();
        dst_topics.insert("orders-v1".to_string(), (6, 3));
        dst_topics.insert("audit-logs".to_string(), (3, 1));
        dst_topics.insert("both-diff".to_string(), (5, 1));
        dst_topics.insert("synced-topic".to_string(), (4, 2));
        dst_topics.insert("extra-target-only".to_string(), (2, 1)); // missing in src
        dst_topics.insert("__consumer_offsets".to_string(), (50, 3));

        let diff = compute_cluster_diff("prod", &src_topics, "dr", &dst_topics, false);

        // Internal topic ignored when false
        assert_eq!(diff.src_topic_count, 5);
        assert_eq!(diff.dst_topic_count, 5);
        assert_eq!(diff.synced_count, 1);
        assert_eq!(diff.drifted_count, 3);
        assert_eq!(diff.missing_in_dst_count, 1);
        assert_eq!(diff.missing_in_src_count, 1);
        // Total unique non-internal: orders, users, audit, both, synced, extra = 6 topics
        // Synced = 1 => 1/6 = 16.6666...%
        assert!((diff.parity_score_pct - (1.0 / 6.0 * 100.0)).abs() < 0.01);

        let plan = generate_remediation_plan(&diff, "dr");
        assert_eq!(plan.len(), 5); // 5 non-synced items have remediation steps
        let missing_cmd = plan.iter().find(|s| s.topic_name == "users-v1").unwrap();
        assert!(missing_cmd
            .cli_command
            .contains("ferrika --profile dr topics create users-v1 -p 6 -r 3"));
    }

    #[test]
    fn test_empty_clusters_diff() {
        let src_topics = HashMap::new();
        let dst_topics = HashMap::new();

        let diff = compute_cluster_diff("src", &src_topics, "dst", &dst_topics, false);
        assert_eq!(diff.synced_count, 0);
        assert_eq!(diff.items.len(), 0);
        assert!((diff.parity_score_pct - 100.0).abs() < f64::EPSILON);
    }

    #[test]
    fn diff_classifies_every_state_and_hides_internal_topics() {
        let src: HashMap<String, (usize, usize)> = [
            ("same", (3, 3)),
            ("parts", (3, 3)),
            ("rf", (3, 3)),
            ("both", (3, 3)),
            ("only-src", (1, 1)),
            ("__consumer_offsets", (50, 3)),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();
        let dst: HashMap<String, (usize, usize)> = [
            ("same", (3, 3)),
            ("parts", (6, 3)),
            ("rf", (3, 1)),
            ("both", (6, 1)),
            ("only-dst", (1, 1)),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();
        let d = compute_cluster_diff("a", &src, "b", &dst, false);
        assert_eq!(d.synced_count, 1);
        assert_eq!(d.drifted_count, 3);
        assert_eq!(d.missing_in_dst_count, 1);
        assert_eq!(d.missing_in_src_count, 1);
        assert!(d.items.iter().all(|i| i.topic_name != "__consumer_offsets"));
        let state = |n: &str| {
            d.items
                .iter()
                .find(|i| i.topic_name == n)
                .unwrap()
                .status
                .clone()
        };
        assert!(matches!(
            state("both"),
            TopicParityState::BothMismatch { .. }
        ));
        assert!(matches!(
            state("rf"),
            TopicParityState::ReplicationMismatch {
                src_rf: 3,
                dst_rf: 1
            }
        ));
        assert!(matches!(
            state("parts"),
            TopicParityState::PartitionMismatch { .. }
        ));
        assert_eq!(state("only-dst"), TopicParityState::MissingInSource);
        assert!(d.parity_score_pct > 0.0 && d.parity_score_pct < 100.0);
        let with_internal = compute_cluster_diff("a", &src, "b", &dst, true);
        assert_eq!(with_internal.missing_in_dst_count, 2);
    }
}
