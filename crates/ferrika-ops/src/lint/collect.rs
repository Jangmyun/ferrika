//! Reads what the lint rules need from the cluster: the snapshot `health`
//! uses, every topic's and online broker's configs, and each group's
//! committed offsets.

use std::collections::{BTreeMap, HashSet};
use std::time::Duration;

use ferrika_core::error::KafkaResult;
use ferrika_core::kafka::admin::KafkaAdminClient;
use ferrika_core::kafka::config::KafkaClientConfig;
use ferrika_core::model::cluster::{ClusterSnapshot, GroupPartitionLag};

use super::{LintInput, LintOptions};

const TIMEOUT: Duration = Duration::from_secs(15);

/// Builds the lint input. `data` is reused when the caller already has a
/// snapshot (the GUI poller); otherwise one is taken. Topic configs are read
/// for the topics `opts` covers (and the sources of dead letter topics),
/// broker configs for every online broker. Whatever cannot be read goes to
/// [`LintInput::skipped`].
pub async fn gather(
    admin: &KafkaAdminClient,
    config: &KafkaClientConfig,
    profile_name: &str,
    data: Option<ClusterSnapshot>,
    opts: &LintOptions,
) -> KafkaResult<LintInput> {
    let mut data = match data {
        Some(data) => data,
        None => crate::health::snapshot(admin, config, profile_name).await?,
    };
    let mut skipped = Vec::new();

    let mut wanted: Vec<String> = data
        .topics
        .iter()
        .filter(|t| {
            !t.is_internal
                && !ferrika_core::kafka::internal_topic::is_internal_topic(&t.name)
                && opts.covers_topic(&t.name)
        })
        .map(|t| t.name.clone())
        .collect();
    let known: HashSet<&str> = data.topics.iter().map(|t| t.name.as_str()).collect();
    let sources: Vec<String> = wanted
        .iter()
        .filter_map(|t| crate::dlq::infer_original_topic_from_dlq_name(t))
        .filter(|s| known.contains(s.as_str()))
        .collect();
    wanted.extend(sources);
    wanted.sort();
    wanted.dedup();
    let (topic_configs, missing) = admin.describe_topics_configs(&wanted, Some(TIMEOUT)).await;
    skipped.extend(missing.into_iter().map(|t| format!("topic {t}")));

    let mut broker_configs = BTreeMap::new();
    for broker in data.brokers.iter().filter(|b| b.is_online) {
        match admin
            .describe_broker_configs(broker.id, Some(TIMEOUT))
            .await
        {
            Ok(entries) => {
                broker_configs.insert(broker.id, entries);
            }
            Err(_) => skipped.push(format!("broker {}", broker.id)),
        }
    }

    let group_offsets = fill_committed_offsets(config, &mut data);
    if !group_offsets {
        skipped.push("committed offsets of the consumer groups".to_string());
    }

    Ok(LintInput {
        data,
        topic_configs,
        broker_configs,
        skipped,
        group_offsets,
    })
}

/// Fills each group's per-partition committed offsets where the snapshot
/// has none. Returns whether they could be read.
fn fill_committed_offsets(config: &KafkaClientConfig, data: &mut ClusterSnapshot) -> bool {
    let groups: Vec<String> = data
        .consumers
        .iter()
        .filter(|g| g.partitions.is_empty())
        .map(|g| g.group_id.clone())
        .collect();
    if groups.is_empty() {
        return true;
    }
    let Ok(offsets) = ferrika_core::kafka::wire::fetch_group_offsets(config, &groups) else {
        return false;
    };
    let filled: Vec<(String, Vec<GroupPartitionLag>)> = offsets
        .into_iter()
        .map(|(group, positions)| {
            (
                group,
                crate::group_lag::group_partition_lags(&positions, &data.topics),
            )
        })
        .collect();
    for (group, partitions) in filled {
        if let Some(g) = data.consumers.iter_mut().find(|g| g.group_id == group) {
            g.partitions = partitions;
        }
    }
    true
}
