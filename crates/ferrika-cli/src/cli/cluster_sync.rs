use std::collections::HashMap;
use std::time::Duration;

use clap::{Args, Subcommand};
use comfy_table::{Cell, CellAlignment, Color};

use crate::cli::{output, GlobalOpts};
use crate::error::{KafkaError, KafkaResult};
use crate::kafka::admin::KafkaAdminClient;
use crate::kafka::profile::{ClusterProfile, ProfileStore};
use crate::ops::cluster_sync::{
    compute_cluster_diff, generate_remediation_plan, ping_cluster, topic_shape, ClusterDiffSummary,
    ClusterHealthStatus, ClusterPingResult, TopicParityState,
};

/// Multi-cluster management, health pinging, and cross-datacenter sync checking.
#[derive(Debug, Args)]
#[command(
    name = "cluster",
    about = "Manage, ping, and check synchronization across multiple Kafka clusters"
)]
pub struct ClusterArgs {
    #[command(subcommand)]
    pub command: ClusterCommands,
}

#[derive(Debug, Subcommand)]
pub enum ClusterCommands {
    /// Ping Kafka clusters concurrently and display connectivity/latency matrix
    Ping(ClusterPingArgs),
    /// Compare topic inventory and partition count/replication drift between two clusters
    Diff(ClusterDiffArgs),
    /// Generate remediation plan and CLI commands to bring target cluster in sync with source
    ReconcilePlan(ClusterReconcileArgs),
}

#[derive(Debug, Args)]
pub struct ClusterPingArgs {
    /// List of cluster profiles to ping. If omitted, all configured profiles are checked.
    #[arg(value_name = "CLUSTERS")]
    pub clusters: Vec<String>,

    /// Timeout in milliseconds per cluster ping
    #[arg(short, long, default_value = "3000")]
    pub timeout_ms: u64,

    /// Output results as JSON
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct ClusterDiffArgs {
    /// Source / reference cluster profile name
    #[arg(value_name = "SRC_CLUSTER")]
    pub src_cluster: String,

    /// Destination / target cluster profile name to compare against
    #[arg(value_name = "DST_CLUSTER")]
    pub dst_cluster: String,

    /// Optional substring filter for topic names
    #[arg(short, long)]
    pub filter: Option<String>,

    /// Include internal topics starting with underscore (e.g. __consumer_offsets)
    #[arg(long)]
    pub include_internal: bool,

    /// Output diff analysis as JSON
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct ClusterReconcileArgs {
    /// Source / reference cluster profile name
    #[arg(value_name = "SRC_CLUSTER")]
    pub src_cluster: String,

    /// Destination / target cluster profile name to reconcile
    #[arg(value_name = "DST_CLUSTER")]
    pub dst_cluster: String,

    /// Include internal topics starting with underscore
    #[arg(long)]
    pub include_internal: bool,

    /// Output remediation plan as JSON
    #[arg(long)]
    pub json: bool,
}

/// Dispatches the cluster subcommand.
pub async fn handle_cluster_command(args: ClusterArgs, global: &GlobalOpts) -> KafkaResult<()> {
    match args.command {
        ClusterCommands::Ping(ping_args) => handle_ping(ping_args, global).await,
        ClusterCommands::Diff(diff_args) => handle_diff(diff_args, global).await,
        ClusterCommands::ReconcilePlan(plan_args) => handle_reconcile_plan(plan_args, global).await,
    }
}

async fn handle_ping(args: ClusterPingArgs, global: &GlobalOpts) -> KafkaResult<()> {
    let store = global.profile_store();
    let timeout = Duration::from_millis(args.timeout_ms);

    let profiles_to_ping: Vec<ClusterProfile> = if args.clusters.is_empty() {
        store.profiles.values().cloned().collect()
    } else {
        args.clusters
            .iter()
            .map(|name| resolve_profile(&store, name))
            .collect::<KafkaResult<Vec<_>>>()?
    };

    if profiles_to_ping.is_empty() {
        println!("No cluster profiles found. Configure clusters using 'ferrika profile add'.");
        return Ok(());
    }

    // Ping all clusters concurrently
    let mut tasks = Vec::new();
    for prof in profiles_to_ping {
        tasks.push(tokio::spawn(
            async move { ping_cluster(&prof, timeout).await },
        ));
    }

    let mut results = Vec::new();
    for t in tasks {
        if let Ok(res) = t.await {
            results.push(res);
        }
    }

    output::emit(global.wants_json(args.json), &results, |r| {
        render_ping_table(r)
    })?;

    // Non-zero exit when any cluster is down, so scripts can use ping as a check.
    let unreachable: Vec<&str> = results
        .iter()
        .filter(|r| matches!(r.status, ClusterHealthStatus::Unreachable(_)))
        .map(|r| r.cluster_name.as_str())
        .collect();
    if unreachable.is_empty() {
        Ok(())
    } else {
        Err(KafkaError::io(format!(
            "Unreachable cluster(s): {}",
            unreachable.join(", ")
        )))
    }
}

fn render_ping_table(results: &[ClusterPingResult]) {
    let mut table = output::fixed_table(vec![
        Cell::new("Cluster Profile").fg(Color::Cyan),
        Cell::new("Bootstrap Servers").fg(Color::Cyan),
        Cell::new("RTT Latency").fg(Color::Cyan),
        Cell::new("Brokers").fg(Color::Cyan),
        Cell::new("Controller").fg(Color::Cyan),
        Cell::new("Health Status").fg(Color::Cyan),
    ]);

    for r in results {
        let (status_cell, latency_cell) = match &r.status {
            ClusterHealthStatus::Healthy => {
                let lat_color = if r.rtt_ms < 50.0 {
                    Color::Green
                } else if r.rtt_ms < 150.0 {
                    Color::Yellow
                } else {
                    Color::Magenta
                };
                (
                    Cell::new("● Healthy").fg(Color::Green),
                    Cell::new(format!("{:.1} ms", r.rtt_ms)).fg(lat_color),
                )
            }
            ClusterHealthStatus::Degraded => (
                Cell::new("▲ Degraded").fg(Color::Yellow),
                Cell::new(format!("{:.1} ms", r.rtt_ms)).fg(Color::Yellow),
            ),
            ClusterHealthStatus::Unreachable(err) => (
                Cell::new(format!("✖ Unreachable: {err}")).fg(Color::Red),
                Cell::new("N/A").fg(Color::DarkGrey),
            ),
        };

        table.add_row(vec![
            Cell::new(&r.cluster_name).fg(Color::White),
            Cell::new(&r.bootstrap_servers).fg(Color::DarkGrey),
            latency_cell.set_alignment(CellAlignment::Right),
            Cell::new(r.broker_count.to_string()).set_alignment(CellAlignment::Right),
            Cell::new(if r.controller_id >= 0 {
                format!("#{}", r.controller_id)
            } else {
                "None".to_string()
            }),
            status_cell,
        ]);
    }

    println!("{table}");
}

/// Topic shapes by name, plus the topics skipped for a metadata error.
type TopicsMap = (HashMap<String, (usize, usize)>, Vec<String>);

/// Helper to fetch topic map: `topic_name -> (partitions, replication_factor)`.
/// Topics whose metadata carries an error (or that report no partitions) are
/// left out of the map and returned separately, so they are not compared as
/// `(0, 1)` and shown as false drift.
fn fetch_topics_map(profile: &ClusterProfile, filter: Option<&str>) -> KafkaResult<TopicsMap> {
    let client = KafkaAdminClient::new(&profile.to_kafka_config())?;
    let metadata = client.fetch_raw_metadata(None, None)?;
    let mut map = HashMap::new();
    let mut unreadable = Vec::new();

    for topic in metadata.topics() {
        let name = topic.name();
        if let Some(f) = filter {
            if !name.contains(f) {
                continue;
            }
        }
        let counts: Vec<usize> = topic
            .partitions()
            .iter()
            .map(|p| p.replicas().len())
            .collect();
        match (topic.error(), topic_shape(&counts)) {
            (None, Some(shape)) => {
                map.insert(name.to_string(), shape);
            }
            _ => unreadable.push(name.to_string()),
        }
    }
    unreadable.sort();

    Ok((map, unreadable))
}

/// Tells the user which topics were left out of a comparison.
fn warn_unreadable(cluster: &str, unreadable: &[String]) {
    if !unreadable.is_empty() {
        eprintln!(
            "warning: {} topic(s) on {cluster} have a metadata error and were skipped: {}",
            unreadable.len(),
            unreadable.join(", ")
        );
    }
}

/// A saved profile by name, or an ad-hoc one when `name` is a bootstrap
/// address (`host:port`, comma-separated). Anything else is most likely a
/// mistyped profile name, which is reported instead of being dialed.
fn resolve_profile(store: &ProfileStore, name: &str) -> KafkaResult<ClusterProfile> {
    if let Some(profile) = store.profiles.get(name) {
        return Ok(profile.clone());
    }
    let looks_like_address = name.split(',').all(|server| {
        server
            .rsplit_once(':')
            .is_some_and(|(host, port)| !host.is_empty() && port.parse::<u16>().is_ok())
    });
    if looks_like_address {
        Ok(ClusterProfile::new(name, name))
    } else {
        Err(KafkaError::profile(format!(
            "Unknown cluster profile '{name}'. Use a saved profile ('ferrika profile list') or a bootstrap address such as host:9092"
        )))
    }
}

async fn handle_diff(args: ClusterDiffArgs, global: &GlobalOpts) -> KafkaResult<()> {
    let store = global.profile_store();
    let src_profile = resolve_profile(&store, &args.src_cluster)?;
    let dst_profile = resolve_profile(&store, &args.dst_cluster)?;

    let (src_topics, src_bad) = fetch_topics_map(&src_profile, args.filter.as_deref())?;
    let (dst_topics, dst_bad) = fetch_topics_map(&dst_profile, args.filter.as_deref())?;
    warn_unreadable(&args.src_cluster, &src_bad);
    warn_unreadable(&args.dst_cluster, &dst_bad);

    let summary = compute_cluster_diff(
        &args.src_cluster,
        &src_topics,
        &args.dst_cluster,
        &dst_topics,
        args.include_internal,
    );

    output::emit(global.wants_json(args.json), &summary, render_diff_summary)
}

fn render_diff_summary(diff: &ClusterDiffSummary) {
    let parity_color = if diff.parity_score_pct >= 99.9 {
        Color::Green
    } else if diff.parity_score_pct >= 80.0 {
        Color::Yellow
    } else {
        Color::Red
    };

    println!(
        "\n══ Multi-Cluster Parity Audit: {} ───▶ {} ══",
        diff.src_cluster_name, diff.dst_cluster_name
    );

    let mut summary_table = output::fixed_table(vec![
        Cell::new("Parity Score").fg(Color::Cyan),
        Cell::new(format!("{}: Total Topics", diff.src_cluster_name)).fg(Color::Cyan),
        Cell::new(format!("{}: Total Topics", diff.dst_cluster_name)).fg(Color::Cyan),
        Cell::new("Synchronized").fg(Color::Cyan),
        Cell::new("Drifted").fg(Color::Cyan),
        Cell::new("Missing in Target").fg(Color::Cyan),
    ]);

    summary_table.add_row(vec![
        Cell::new(format!("{:.1}%", diff.parity_score_pct))
            .fg(parity_color)
            .set_alignment(CellAlignment::Right),
        Cell::new(diff.src_topic_count.to_string()).set_alignment(CellAlignment::Right),
        Cell::new(diff.dst_topic_count.to_string()).set_alignment(CellAlignment::Right),
        Cell::new(diff.synced_count.to_string())
            .fg(Color::Green)
            .set_alignment(CellAlignment::Right),
        Cell::new(diff.drifted_count.to_string())
            .fg(if diff.drifted_count > 0 {
                Color::Yellow
            } else {
                Color::Green
            })
            .set_alignment(CellAlignment::Right),
        Cell::new(diff.missing_in_dst_count.to_string())
            .fg(if diff.missing_in_dst_count > 0 {
                Color::Red
            } else {
                Color::Green
            })
            .set_alignment(CellAlignment::Right),
    ]);
    println!("{summary_table}\n");

    let mut items_table = output::fixed_table(vec![
        Cell::new("Topic Name").fg(Color::Cyan),
        Cell::new(format!("{} (Partitions/RF)", diff.src_cluster_name)).fg(Color::Cyan),
        Cell::new(format!("{} (Partitions/RF)", diff.dst_cluster_name)).fg(Color::Cyan),
        Cell::new("Sync Status").fg(Color::Cyan),
    ]);

    for item in &diff.items {
        let src_spec = match (item.src_partitions, item.src_replication_factor) {
            (Some(p), Some(rf)) => format!("{p}p / {rf}rf"),
            _ => "-".to_string(),
        };

        let dst_spec = match (item.dst_partitions, item.dst_replication_factor) {
            (Some(p), Some(rf)) => format!("{p}p / {rf}rf"),
            _ => "-".to_string(),
        };

        let status_cell = match &item.status {
            TopicParityState::Synced => Cell::new("✓ Synced").fg(Color::Green),
            TopicParityState::PartitionMismatch {
                src_partitions,
                dst_partitions,
            } => Cell::new(format!(
                "⚠ Partition Drift ({src_partitions} vs {dst_partitions})"
            ))
            .fg(Color::Yellow),
            TopicParityState::ReplicationMismatch { src_rf, dst_rf } => {
                Cell::new(format!("⚠ RF Drift ({src_rf} vs {dst_rf})")).fg(Color::Yellow)
            }
            TopicParityState::BothMismatch { .. } => {
                Cell::new("⚠ Partition & RF Drift").fg(Color::Magenta)
            }
            TopicParityState::MissingInTarget => {
                Cell::new("✖ Missing in Destination").fg(Color::Red)
            }
            TopicParityState::MissingInSource => {
                Cell::new("ℹ Target Only (Orphan)").fg(Color::DarkGrey)
            }
        };

        items_table.add_row(vec![
            Cell::new(&item.topic_name).fg(Color::White),
            Cell::new(src_spec).set_alignment(CellAlignment::Center),
            Cell::new(dst_spec).set_alignment(CellAlignment::Center),
            status_cell,
        ]);
    }

    println!("{items_table}\n");
}

async fn handle_reconcile_plan(args: ClusterReconcileArgs, global: &GlobalOpts) -> KafkaResult<()> {
    let store = global.profile_store();
    let src_profile = resolve_profile(&store, &args.src_cluster)?;
    let dst_profile = resolve_profile(&store, &args.dst_cluster)?;

    let (src_topics, src_bad) = fetch_topics_map(&src_profile, None)?;
    let (dst_topics, dst_bad) = fetch_topics_map(&dst_profile, None)?;
    warn_unreadable(&args.src_cluster, &src_bad);
    warn_unreadable(&args.dst_cluster, &dst_bad);

    let summary = compute_cluster_diff(
        &args.src_cluster,
        &src_topics,
        &args.dst_cluster,
        &dst_topics,
        args.include_internal,
    );

    let plan = generate_remediation_plan(&summary, &args.dst_cluster);

    if global.wants_json(args.json) {
        output::print_json(&plan)?;
        return Ok(());
    }

    println!(
        "\n══ Cluster Reconciliation Plan: {} ───▶ {} ══",
        args.src_cluster, args.dst_cluster
    );

    if plan.is_empty() {
        println!("✨ Clusters are 100% in sync! No reconciliation actions needed.\n");
        return Ok(());
    }

    let mut table = output::fixed_table(vec![
        Cell::new("Topic").fg(Color::Cyan),
        Cell::new("Remediation Action").fg(Color::Cyan),
        Cell::new("Command / Instruction").fg(Color::Cyan),
    ]);

    for step in &plan {
        table.add_row(vec![
            Cell::new(&step.topic_name).fg(Color::White),
            Cell::new(&step.description).fg(Color::Yellow),
            Cell::new(&step.cli_command).fg(Color::Green),
        ]);
    }

    println!("{table}\n");
    println!("💡 Tip: Review commands above before executing against target cluster.\n");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser, Debug)]
    struct CliTestWrapper {
        #[command(subcommand)]
        cmd: TestCommands,
    }

    #[derive(Subcommand, Debug)]
    enum TestCommands {
        Cluster(ClusterArgs),
    }

    #[test]
    fn test_clap_cluster_ping_parsing() {
        let args =
            CliTestWrapper::try_parse_from(["ferrika", "cluster", "ping", "prod", "dr", "--json"]);
        assert!(args.is_ok());
        let TestCommands::Cluster(cluster_args) = args.unwrap().cmd;
        if let ClusterCommands::Ping(ping) = cluster_args.command {
            assert_eq!(ping.clusters, vec!["prod".to_string(), "dr".to_string()]);
            assert!(ping.json);
            assert_eq!(ping.timeout_ms, 3000);
        } else {
            panic!("Expected ClusterCommands::Ping");
        }
    }

    #[test]
    fn test_clap_cluster_diff_parsing() {
        let args = CliTestWrapper::try_parse_from([
            "ferrika",
            "cluster",
            "diff",
            "prod",
            "dr",
            "--filter",
            "orders",
            "--include-internal",
        ]);
        assert!(args.is_ok());
        let TestCommands::Cluster(cluster_args) = args.unwrap().cmd;
        if let ClusterCommands::Diff(diff) = cluster_args.command {
            assert_eq!(diff.src_cluster, "prod");
            assert_eq!(diff.dst_cluster, "dr");
            assert_eq!(diff.filter.as_deref(), Some("orders"));
            assert!(diff.include_internal);
            assert!(!diff.json);
        } else {
            panic!("Expected ClusterCommands::Diff");
        }
    }

    #[test]
    fn test_clap_cluster_reconcile_parsing() {
        let args = CliTestWrapper::try_parse_from([
            "ferrika",
            "cluster",
            "reconcile-plan",
            "staging",
            "prod",
            "--json",
        ]);
        assert!(args.is_ok());
        let TestCommands::Cluster(cluster_args) = args.unwrap().cmd;
        if let ClusterCommands::ReconcilePlan(plan) = cluster_args.command {
            assert_eq!(plan.src_cluster, "staging");
            assert_eq!(plan.dst_cluster, "prod");
            assert!(plan.json);
        } else {
            panic!("Expected ClusterCommands::ReconcilePlan");
        }
    }
}
