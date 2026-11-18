use std::collections::{BTreeMap, HashMap};

use rdkafka::metadata::Metadata;
use serde::{Deserialize, Serialize};

/// Detailed topology and workload profile of an individual Kafka broker node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerTopology {
    /// Unique Broker ID.
    pub id: i32,
    /// Hostname or IP address.
    pub host: String,
    /// TCP port number.
    pub port: u16,
    /// Availability zone or hardware rack if configured.
    pub rack: Option<String>,
    /// Number of partition leaders hosted on this broker.
    pub leader_partitions: usize,
    /// Total partition replicas (leader + followers) hosted on this broker.
    pub total_replicas: usize,
    /// True if this node is the active cluster controller.
    pub is_controller: bool,
}

/// Detailed topology, placement, and replication health of an individual topic partition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionTopology {
    /// Topic name.
    pub topic: String,
    /// Partition index.
    pub partition_id: i32,
    /// Active leader broker ID (-1 if no leader currently elected).
    pub leader: i32,
    /// Preferred leader broker ID (first broker designated in the replica assignment list).
    pub preferred_leader: i32,
    /// Configured replica broker IDs.
    pub replicas: Vec<i32>,
    /// Active in-sync replica (ISR) broker IDs.
    pub isr: Vec<i32>,
    /// True if ISR size is smaller than the assigned replica count.
    pub is_under_replicated: bool,
    /// True if the current active leader differs from the preferred leader.
    pub is_leader_imbalanced: bool,
}

/// Aggregated cluster topology, rack awareness, and partition health report.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClusterTopologyReport {
    /// Total online brokers discovered.
    pub brokers_count: usize,
    /// Total topics analyzed.
    pub topics_count: usize,
    /// Total partition instances across all topics.
    pub total_partitions: usize,
    /// Count of degraded under-replicated partitions (URP).
    pub under_replicated_count: usize,
    /// Count of partitions with leader election drift from preferred leader.
    pub imbalanced_leaders_count: usize,
    /// True if at least one broker has an assigned rack.
    pub rack_aware: bool,
    /// Mapping of rack identifier to broker IDs located within that rack.
    pub rack_distribution: BTreeMap<String, Vec<i32>>,
    /// Workload and status per broker node.
    pub brokers: Vec<BrokerTopology>,
    /// Individual partition placements and health statuses.
    pub partitions: Vec<PartitionTopology>,
}

/// Raw representation of a broker for topology evaluation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawBrokerInput {
    pub id: i32,
    pub host: String,
    pub port: u16,
    pub rack: Option<String>,
    pub is_controller: bool,
}

/// Raw representation of a partition for topology evaluation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawPartitionInput {
    pub topic: String,
    pub partition_id: i32,
    pub leader: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
}

/// Evaluates cluster topology, rack distribution, and partition health.
pub fn build_topology_report(
    raw_brokers: &[RawBrokerInput],
    raw_partitions: &[RawPartitionInput],
) -> ClusterTopologyReport {
    let mut leader_counts: HashMap<i32, usize> = HashMap::new();
    let mut replica_counts: HashMap<i32, usize> = HashMap::new();
    let mut rack_map: BTreeMap<String, Vec<i32>> = BTreeMap::new();
    let mut unique_topics = std::collections::HashSet::new();

    let mut under_replicated_count = 0;
    let mut imbalanced_leaders_count = 0;
    let mut evaluated_partitions = Vec::with_capacity(raw_partitions.len());

    for p in raw_partitions {
        unique_topics.insert(&p.topic);

        if p.leader >= 0 {
            *leader_counts.entry(p.leader).or_insert(0) += 1;
        }

        for &r in &p.replicas {
            *replica_counts.entry(r).or_insert(0) += 1;
        }

        let preferred_leader = p.replicas.first().copied().unwrap_or(-1);
        let is_under_replicated = p.isr.len() < p.replicas.len();
        let is_leader_imbalanced = p.leader >= 0 && p.leader != preferred_leader;

        if is_under_replicated {
            under_replicated_count += 1;
        }
        if is_leader_imbalanced {
            imbalanced_leaders_count += 1;
        }

        evaluated_partitions.push(PartitionTopology {
            topic: p.topic.clone(),
            partition_id: p.partition_id,
            leader: p.leader,
            preferred_leader,
            replicas: p.replicas.clone(),
            isr: p.isr.clone(),
            is_under_replicated,
            is_leader_imbalanced,
        });
    }

    let mut has_rack = false;
    let mut evaluated_brokers = Vec::with_capacity(raw_brokers.len());

    for b in raw_brokers {
        if let Some(ref r) = b.rack {
            has_rack = true;
            rack_map.entry(r.clone()).or_default().push(b.id);
        }

        let leader_partitions = leader_counts.get(&b.id).copied().unwrap_or(0);
        let total_replicas = replica_counts.get(&b.id).copied().unwrap_or(0);

        evaluated_brokers.push(BrokerTopology {
            id: b.id,
            host: b.host.clone(),
            port: b.port,
            rack: b.rack.clone(),
            leader_partitions,
            total_replicas,
            is_controller: b.is_controller,
        });
    }

    evaluated_brokers.sort_by_key(|b| b.id);
    evaluated_partitions.sort_by(|a, b| match a.topic.cmp(&b.topic) {
        std::cmp::Ordering::Equal => a.partition_id.cmp(&b.partition_id),
        other => other,
    });

    ClusterTopologyReport {
        brokers_count: evaluated_brokers.len(),
        topics_count: unique_topics.len(),
        total_partitions: evaluated_partitions.len(),
        under_replicated_count,
        imbalanced_leaders_count,
        rack_aware: has_rack,
        rack_distribution: rack_map,
        brokers: evaluated_brokers,
        partitions: evaluated_partitions,
    }
}

/// Converts rdkafka cluster metadata into `ClusterTopologyReport`, marking
/// `controller_id` (see `KafkaAdminClient::active_controller_id`) as controller.
pub fn from_rdkafka_metadata(
    metadata: &Metadata,
    controller_id: Option<i32>,
) -> ClusterTopologyReport {
    let raw_brokers: Vec<RawBrokerInput> = metadata
        .brokers()
        .iter()
        .map(|b| RawBrokerInput {
            id: b.id(),
            host: b.host().to_string(),
            port: b.port() as u16,
            rack: None, // rdkafka metadata doesn't expose rack directly
            is_controller: Some(b.id()) == controller_id,
        })
        .collect();

    let mut raw_partitions = Vec::new();
    for topic in metadata.topics() {
        let topic_name = topic.name();
        for p in topic.partitions() {
            let replicas: Vec<i32> = p.replicas().to_vec();
            let isr: Vec<i32> = p.isr().to_vec();
            raw_partitions.push(RawPartitionInput {
                topic: topic_name.to_string(),
                partition_id: p.id(),
                leader: p.leader(),
                replicas,
                isr,
            });
        }
    }

    build_topology_report(&raw_brokers, &raw_partitions)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_topology_report_balanced_and_healthy() {
        let brokers = vec![
            RawBrokerInput {
                id: 1,
                host: "kafka1".to_string(),
                port: 9092,
                rack: Some("us-east-1a".to_string()),
                is_controller: true,
            },
            RawBrokerInput {
                id: 2,
                host: "kafka2".to_string(),
                port: 9092,
                rack: Some("us-east-1b".to_string()),
                is_controller: false,
            },
            RawBrokerInput {
                id: 3,
                host: "kafka3".to_string(),
                port: 9092,
                rack: Some("us-east-1c".to_string()),
                is_controller: false,
            },
        ];

        let partitions = vec![
            RawPartitionInput {
                topic: "orders-v1".to_string(),
                partition_id: 0,
                leader: 1,
                replicas: vec![1, 2, 3],
                isr: vec![1, 2, 3],
            },
            RawPartitionInput {
                topic: "orders-v1".to_string(),
                partition_id: 1,
                leader: 2,
                replicas: vec![2, 3, 1],
                isr: vec![2, 3, 1],
            },
            RawPartitionInput {
                topic: "orders-v1".to_string(),
                partition_id: 2,
                leader: 3,
                replicas: vec![3, 1, 2],
                isr: vec![3, 1, 2],
            },
        ];

        let report = build_topology_report(&brokers, &partitions);

        assert_eq!(report.brokers_count, 3);
        assert_eq!(report.topics_count, 1);
        assert_eq!(report.total_partitions, 3);
        assert_eq!(report.under_replicated_count, 0);
        assert_eq!(report.imbalanced_leaders_count, 0);
        assert!(report.rack_aware);
        assert_eq!(report.rack_distribution.len(), 3);

        // Every broker has exactly 1 leader and 3 total replicas
        for b in &report.brokers {
            assert_eq!(b.leader_partitions, 1);
            assert_eq!(b.total_replicas, 3);
        }
    }

    #[test]
    fn test_build_topology_report_with_urp_and_imbalance() {
        let brokers = vec![
            RawBrokerInput {
                id: 1,
                host: "k1".to_string(),
                port: 9092,
                rack: None,
                is_controller: false,
            },
            RawBrokerInput {
                id: 2,
                host: "k2".to_string(),
                port: 9092,
                rack: None,
                is_controller: false,
            },
        ];

        let partitions = vec![
            // URP: replicas [1, 2], isr [1]
            RawPartitionInput {
                topic: "degraded-topic".to_string(),
                partition_id: 0,
                leader: 1,
                replicas: vec![1, 2],
                isr: vec![1],
            },
            // Imbalanced: preferred leader is 1, but leader is 2
            RawPartitionInput {
                topic: "imbalanced-topic".to_string(),
                partition_id: 0,
                leader: 2,
                replicas: vec![1, 2],
                isr: vec![1, 2],
            },
        ];

        let report = build_topology_report(&brokers, &partitions);

        assert_eq!(report.under_replicated_count, 1);
        assert_eq!(report.imbalanced_leaders_count, 1);
        assert!(!report.rack_aware);

        let p0 = &report.partitions[0];
        assert!(p0.is_under_replicated);
        assert!(!p0.is_leader_imbalanced);

        let p1 = &report.partitions[1];
        assert!(!p1.is_under_replicated);
        assert!(p1.is_leader_imbalanced);
    }

    fn part(topic: &str, id: i32, leader: i32, replicas: &[i32], isr: &[i32]) -> RawPartitionInput {
        RawPartitionInput {
            topic: topic.into(),
            partition_id: id,
            leader,
            replicas: replicas.to_vec(),
            isr: isr.to_vec(),
        }
    }

    #[test]
    fn empty_input_gives_an_empty_report() {
        let r = build_topology_report(&[], &[]);
        assert_eq!(
            (r.brokers_count, r.topics_count, r.total_partitions),
            (0, 0, 0)
        );
        assert!(!r.rack_aware);
        assert!(r.rack_distribution.is_empty());
    }

    #[test]
    fn leaderless_partition_is_not_counted_as_imbalanced_and_sorting_is_stable() {
        let brokers = vec![
            RawBrokerInput {
                id: 2,
                host: "b".into(),
                port: 9092,
                rack: Some("r1".into()),
                is_controller: false,
            },
            RawBrokerInput {
                id: 1,
                host: "a".into(),
                port: 9092,
                rack: Some("r1".into()),
                is_controller: true,
            },
        ];
        let parts = vec![
            part("z", 1, -1, &[1, 2], &[1]),
            part("a", 10, 2, &[1, 2], &[1, 2]),
            part("a", 2, 1, &[1, 2], &[1, 2]),
        ];
        let r = build_topology_report(&brokers, &parts);
        assert_eq!(
            r.imbalanced_leaders_count, 1,
            "only a-10 leads off the preferred replica"
        );
        assert_eq!(r.under_replicated_count, 1);
        assert_eq!(r.topics_count, 2);
        assert_eq!(
            r.brokers.iter().map(|b| b.id).collect::<Vec<_>>(),
            vec![1, 2]
        );
        let order: Vec<_> = r
            .partitions
            .iter()
            .map(|p| (p.topic.as_str(), p.partition_id))
            .collect();
        assert_eq!(order, vec![("a", 2), ("a", 10), ("z", 1)]);
        assert_eq!(r.rack_distribution["r1"], vec![2, 1]);
        // The leaderless partition leads on no broker, but its replicas count.
        assert_eq!(r.brokers[0].leader_partitions, 1);
        assert_eq!(r.brokers[0].total_replicas, 3);
    }
}
