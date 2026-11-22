//! Cluster health checks, shown on the GUI Overview and by `ferrika health`.

use std::time::Duration;

use ferrika_core::error::KafkaResult;
use ferrika_core::kafka::admin::KafkaAdminClient;
use ferrika_core::kafka::config::KafkaClientConfig;
use ferrika_core::model::cluster::{ClusterSnapshot, ConsumerGroupSummary, ConsumerState};

/// How urgent a finding is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize)]
pub enum Severity {
    Critical,
    Warning,
    Info,
}

/// What a finding links to.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub enum HealthTarget {
    Topic(String),
    Group(String),
    Brokers,
    /// One broker, by node id.
    Broker(i32),
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct HealthIssue {
    pub severity: Severity,
    pub message: String,
    pub target: Option<HealthTarget>,
}

/// Leader share at which a broker counts as overloaded (vs. the average).
const LEADER_SKEW_PCT: f64 = 20.0;

/// Topics smaller than this are not checked for uneven partitions.
const SKEW_MIN_RECORDS: i64 = 1000;
/// A partition holding this many times its even share counts as skewed.
const SKEW_FACTOR: f64 = 2.0;

/// Metadata-log records a quorum voter may trail the leader by before it is flagged.
const QUORUM_LAG_RECORDS: i64 = 1000;

/// Findings for `data`, most severe first.
pub fn health_checks(data: &ClusterSnapshot) -> Vec<HealthIssue> {
    let mut issues = Vec::new();
    let mut push = |severity, message: String, target| {
        issues.push(HealthIssue {
            severity,
            message,
            target,
        })
    };

    let offline: Vec<String> = data
        .brokers
        .iter()
        .filter(|b| !b.is_online)
        .map(|b| format!("node {}", b.id))
        .collect();
    if !offline.is_empty() {
        push(
            Severity::Critical,
            format!("Broker offline: {}", offline.join(", ")),
            Some(HealthTarget::Brokers),
        );
    }

    for b in &data.brokers {
        for dir in b.log_dirs.iter().filter(|d| d.error.is_some()) {
            push(
                Severity::Critical,
                format!(
                    "Node {}: log dir {} failed ({})",
                    b.id,
                    dir.path,
                    dir.error.as_deref().unwrap_or_default()
                ),
                Some(HealthTarget::Brokers),
            );
        }
    }

    if let Some(quorum) = &data.quorum {
        let lagging: Vec<String> = quorum
            .voters
            .iter()
            .filter(|v| v.id != quorum.leader_id)
            .filter_map(|v| match quorum.lag(v) {
                None => Some(format!("node {} (no contact)", v.id)),
                Some(n) if n > QUORUM_LAG_RECORDS => Some(format!("node {} ({n} behind)", v.id)),
                _ => None,
            })
            .collect();
        if !lagging.is_empty() {
            push(
                Severity::Warning,
                format!("Metadata quorum voters lagging: {}", lagging.join(", ")),
                Some(HealthTarget::Brokers),
            );
        }
    }

    if !data.reassignments.is_empty() {
        push(
            Severity::Info,
            format!("{} partition move(s) in progress", data.reassignments.len()),
            Some(HealthTarget::Brokers),
        );
    }

    for t in &data.topics {
        let leaderless = t.partitions.iter().filter(|p| p.leader < 0).count();
        if leaderless > 0 {
            push(
                Severity::Critical,
                format!("{}: {leaderless} partition(s) without a leader", t.name),
                Some(HealthTarget::Topic(t.name.clone())),
            );
        }
        let under = t
            .partitions
            .iter()
            .filter(|p| p.isr.len() < p.replicas.len())
            .count();
        if under > 0 {
            push(
                Severity::Warning,
                format!("{}: {under} under-replicated partition(s)", t.name),
                Some(HealthTarget::Topic(t.name.clone())),
            );
        }
    }

    let online: Vec<_> = data.brokers.iter().filter(|b| b.is_online).collect();
    let leaders: usize = online.iter().map(|b| b.partition_leaders).sum();
    if online.len() > 1 && leaders > 0 {
        let average = leaders as f64 / online.len() as f64;
        let skewed: Vec<String> = online
            .iter()
            .filter(|b| (b.partition_leaders as f64 - average) / average * 100.0 >= LEADER_SKEW_PCT)
            .map(|b| format!("node {} leads {}", b.id, b.partition_leaders))
            .collect();
        if !skewed.is_empty() {
            push(
                Severity::Warning,
                format!(
                    "Leadership is uneven ({}, average {average:.0}); rebalance leaders on Brokers",
                    skewed.join(", ")
                ),
                Some(HealthTarget::Brokers),
            );
        }
    }

    let mut idle: Vec<(&str, i64)> = Vec::new();
    for g in &data.consumers {
        // Rates are known from the second poll on; zero with members and lag
        // means the consumers are stuck (or paused).
        if g.active_members > 0 && g.total_lag > 0 && g.consume_rate == Some(0.0) {
            push(
                Severity::Warning,
                format!(
                    "{}: {} member(s) but no progress, lag {}",
                    g.group_id, g.active_members, g.total_lag
                ),
                Some(HealthTarget::Group(g.group_id.clone())),
            );
        } else if g.state == ConsumerState::Empty && g.total_lag > 0 {
            idle.push((&g.group_id, g.total_lag));
        }
    }
    // Idle groups are common (stopped jobs); one line, largest lag first.
    if let Some(&(largest, lag)) = idle.iter().max_by_key(|(_, lag)| *lag) {
        let message = if idle.len() == 1 {
            format!("{largest}: no members, lag {lag}")
        } else {
            format!(
                "{} groups have lag but no members (largest: {largest}, {lag})",
                idle.len()
            )
        };
        push(
            Severity::Info,
            message,
            Some(HealthTarget::Group(largest.to_string())),
        );
    }

    // Records concentrated on a few partitions usually mean a hot key.
    let skewed: Vec<String> = data
        .topics
        .iter()
        .filter(|t| !t.is_internal && t.partitions.len() > 1)
        .filter_map(|t| {
            let counts: Vec<i64> = t
                .partitions
                .iter()
                .map(|p| {
                    ferrika_core::kafka::metrics::partition_depth(p.low_watermark, p.high_watermark)
                })
                .collect();
            let total: i64 = counts.iter().sum();
            if total < SKEW_MIN_RECORDS {
                return None;
            }
            let (ix, max) = counts.iter().enumerate().max_by_key(|(_, c)| **c)?;
            let share = *max as f64 / total as f64;
            let even = 1.0 / counts.len() as f64;
            (share >= even * SKEW_FACTOR).then(|| {
                format!(
                    "{} (partition {} holds {:.0}%)",
                    t.name,
                    t.partitions[ix].partition_id,
                    share * 100.0
                )
            })
        })
        .collect();
    if !skewed.is_empty() {
        push(
            Severity::Info,
            format!("Uneven partitions: {}", skewed.join(", ")),
            None,
        );
    }

    let single: Vec<&str> = data
        .topics
        .iter()
        .filter(|t| !t.is_internal && t.replication_factor < 2 && online.len() > 1)
        .map(|t| t.name.as_str())
        .collect();
    if !single.is_empty() {
        push(
            Severity::Info,
            format!("No redundancy (replication 1): {}", single.join(", ")),
            None,
        );
    }

    // What could not be read: an empty group list may hide idle or lagging
    // groups, so say so instead of reporting a clean bill of health.
    for warning in &data.warnings {
        push(
            Severity::Warning,
            format!("{INCOMPLETE_PREFIX}{warning}"),
            None,
        );
    }

    issues.sort_by_key(|i| i.severity);
    issues
}

/// Starts the message of a finding made from a [`ClusterSnapshot::warnings`]
/// entry: part of the cluster could not be checked.
pub const INCOMPLETE_PREFIX: &str = "Incomplete check: ";

/// Reads what the health checks need from the cluster: brokers, topics, the
/// metadata quorum and each group's lag. Groups carry their summed committed
/// and end offsets, so two snapshots give a consume rate (see
/// `kafka::report::fill_consume_rates`). Shared by `health`, `report` and
/// `lint`.
pub async fn snapshot(
    admin: &KafkaAdminClient,
    config: &KafkaClientConfig,
    profile_name: &str,
) -> KafkaResult<ClusterSnapshot> {
    let timeout = Some(Duration::from_secs(10));
    let mut data =
        ClusterSnapshot::empty(profile_name.to_string(), config.bootstrap_servers.clone());
    data.brokers = admin.list_brokers(timeout)?;
    data.cluster_id = admin.cluster_id(timeout).unwrap_or_default();
    data.topics = admin.list_topic_summaries(timeout)?;
    data.quorum = ferrika_core::kafka::wire::describe_quorum(config).unwrap_or_default();
    // Only offsets are used here: skip the record reads for time lag.
    let (lags, failed) = crate::exporter::scrape(admin, config, false).await;
    let listings = match admin.list_consumer_group_listings(timeout) {
        Ok(listings) => listings,
        Err(e) => {
            data.warnings
                .push(format!("Consumer groups could not be listed: {e}"));
            Vec::new()
        }
    };
    let listed = data.warnings.is_empty();
    data.warnings.extend(lag_warnings(&failed, listed));
    data.consumers = listings
        .into_iter()
        .map(|g| {
            let mine: Vec<_> = lags
                .consumer_lags
                .iter()
                .filter(|l| l.group == g.name)
                .collect();
            let mut topics: Vec<String> = mine.iter().map(|l| l.topic.clone()).collect();
            topics.sort();
            topics.dedup();
            ConsumerGroupSummary {
                state: ConsumerState::from_broker_state(&g.state),
                active_members: g.members.len(),
                target_topics: topics,
                current_offset: mine.iter().map(|l| l.current_offset.max(0)).sum(),
                end_offset: mine.iter().map(|l| l.high_watermark).sum(),
                total_lag: mine.iter().map(|l| l.lag).sum(),
                coordinator_node: "-".to_string(),
                // One sample: stalled consumers need two polls, see the GUI.
                consume_rate: None,
                partitions: Vec::new(),
                group_id: g.name,
            }
        })
        .collect();
    Ok(data)
}

/// What the lag lookup (`failed`, the steps of
/// [`crate::exporter::scrape_cluster_metrics_checked`] that failed) leaves
/// unknown. `listed` is whether the snapshot's own group listing worked; if
/// not, that warning already covers a failed group list here.
pub fn lag_warnings(failed: &[&str], listed: bool) -> Vec<String> {
    failed
        .iter()
        .filter_map(|step| match *step {
            "groups" if listed => {
                Some("Consumer group lag is unknown: the groups could not be listed".to_string())
            }
            "topics" => {
                Some("Consumer group lag is incomplete: the topics could not be listed".to_string())
            }
            "watermarks" => Some(
                "Consumer group lag is incomplete: partition end offsets could not be read"
                    .to_string(),
            ),
            "offsets" => Some(
                "Consumer group lag is incomplete: the committed offsets of some groups could not be read"
                    .to_string(),
            ),
            // Brokers are read (and fail the snapshot) on their own.
            _ => None,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn health_checks_flag_offline_brokers_isr_skew_and_stuck_groups() {
        let mut data = ClusterSnapshot::demo();
        assert!(health_checks(&data)
            .iter()
            .all(|i| i.severity != Severity::Critical));

        data.brokers[1].is_online = false;
        data.topics[0].partitions[0].isr.truncate(1);
        data.topics[0].partitions[0].leader = -1;
        data.brokers[0].partition_leaders = 100;
        data.consumers[0].active_members = 2;
        data.consumers[0].total_lag = 500;
        data.consumers[0].consume_rate = Some(0.0);
        if let Some(q) = data.quorum.as_mut() {
            q.voters[2].log_end_offset = -1;
        }
        data.brokers[2].log_dirs = vec![ferrika_core::model::cluster::LogDirSummary {
            path: "/data/kafka".into(),
            bytes: 0,
            error: Some("KAFKA_STORAGE_ERROR (code 56)".into()),
        }];

        let issues = health_checks(&data);
        assert_eq!(issues[0].severity, Severity::Critical);
        let text: Vec<&str> = issues.iter().map(|i| i.message.as_str()).collect();
        assert!(text.iter().any(|m| m.starts_with("Broker offline: node")));
        assert!(text.iter().any(|m| m.contains("without a leader")));
        assert!(text.iter().any(|m| m.contains("under-replicated")));
        assert!(text.iter().any(|m| m.contains("Leadership is uneven")));
        assert!(text.iter().any(|m| m.contains("no progress")));
        assert!(text.iter().any(|m| m.contains("node 3 (no contact)")));
        assert!(text
            .iter()
            .any(|m| m.contains("log dir /data/kafka failed")));
        // Sorted by severity.
        assert!(issues.windows(2).all(|w| w[0].severity <= w[1].severity));
    }

    #[test]
    fn health_checks_flag_uneven_partitions() {
        let mut data = ClusterSnapshot::demo();
        let t = &mut data.topics[0];
        for (i, p) in t.partitions.iter_mut().enumerate() {
            p.low_watermark = 0;
            p.high_watermark = if i == 0 { 9_000 } else { 100 };
        }
        let name = t.name.clone();
        let issues = health_checks(&data);
        assert!(
            issues
                .iter()
                .any(|i| i.message.starts_with("Uneven partitions") && i.message.contains(&name)),
            "{issues:?}"
        );
    }

    #[test]
    fn single_online_broker_and_zero_leaders_do_not_flag_skew() {
        let mut data = ClusterSnapshot::demo();
        for b in &mut data.brokers {
            b.partition_leaders = 0;
        }
        assert!(!health_checks(&data)
            .iter()
            .any(|i| i.message.contains("Leadership is uneven")));
        for b in data.brokers.iter_mut().skip(1) {
            b.is_online = false;
        }
        data.brokers[0].partition_leaders = 50;
        let issues = health_checks(&data);
        assert!(!issues.iter().any(|i| i.message.contains("Leadership")));
        // Redundancy is not reported when only one broker is online.
        assert!(!issues
            .iter()
            .any(|i| i.message.starts_with("No redundancy")));
    }

    #[test]
    fn idle_groups_are_summarized_by_the_largest_lag() {
        let mut data = ClusterSnapshot::demo();
        data.consumers.clear();
        let mut g = ClusterSnapshot::demo().consumers[0].clone();
        g.active_members = 0;
        g.state = ConsumerState::Empty;
        g.consume_rate = None;
        for (name, lag) in [("g-small", 5), ("g-big", 900), ("g-zero", 0)] {
            let mut x = g.clone();
            x.group_id = name.into();
            x.total_lag = lag;
            data.consumers.push(x);
        }
        let idle: Vec<_> = health_checks(&data)
            .into_iter()
            .filter(|i| i.message.contains("no members"))
            .collect();
        assert_eq!(idle.len(), 1);
        assert!(idle[0].message.contains("2 groups"), "{:?}", idle[0]);
        assert!(idle[0].message.contains("g-big, 900"), "{:?}", idle[0]);
        assert_eq!(idle[0].target, Some(HealthTarget::Group("g-big".into())));
    }

    #[test]
    fn empty_cluster_has_no_findings() {
        let data = ClusterSnapshot::empty("c", "h:9092");
        assert!(health_checks(&data).is_empty());
    }

    #[test]
    fn snapshot_warnings_become_incomplete_check_findings() {
        let mut data = ClusterSnapshot::empty("c", "h:9092");
        data.warnings
            .push("Consumer groups could not be listed: timed out".into());
        let issues = health_checks(&data);
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].severity, Severity::Warning);
        assert_eq!(
            issues[0].message,
            "Incomplete check: Consumer groups could not be listed: timed out"
        );
        assert_eq!(issues[0].target, None);
    }

    #[test]
    fn failed_lag_steps_become_warnings() {
        assert!(lag_warnings(&[], true).is_empty());
        // Brokers fail the snapshot itself; nothing to add.
        assert!(lag_warnings(&["brokers"], true).is_empty());
        let w = lag_warnings(&["groups", "watermarks", "offsets"], true);
        assert_eq!(w.len(), 3, "{w:?}");
        assert!(w[0].contains("groups could not be listed"), "{w:?}");
        assert!(w[1].contains("end offsets"), "{w:?}");
        assert!(w[2].contains("committed offsets"), "{w:?}");
        // The snapshot's own failed listing already says the groups are unknown.
        let w = lag_warnings(&["groups", "topics"], false);
        assert_eq!(w.len(), 1, "{w:?}");
        assert!(w[0].contains("topics could not be listed"), "{w:?}");
    }
}
