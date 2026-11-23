use clap::Args;
use comfy_table::{Cell, CellAlignment, Color};

use crate::cli::{output, GlobalOpts};
use crate::error::KafkaResult;
use crate::kafka::admin::KafkaAdminClient;
use crate::ops::topology::ClusterTopologyReport;

/// Command line arguments for Kafka cluster topology and partition placement visualizer.
#[derive(Debug, Args)]
#[command(
    name = "topology",
    about = "Inspect cluster topology, broker workload, rack distribution, and under-replicated partitions"
)]
pub struct TopologyArgs {
    /// Filter partition results by topic name substring
    #[arg(short = 't', long)]
    pub topic: Option<String>,

    /// Show only under-replicated partitions (URP) with missing in-sync replicas
    #[arg(long)]
    pub under_replicated_only: bool,

    /// Show only partitions where active leader differs from preferred leader
    #[arg(long)]
    pub imbalanced_only: bool,

    /// Output topology report in JSON format
    #[arg(long)]
    pub json: bool,
}

/// Executes the topology command.
pub async fn run(args: TopologyArgs, global: &GlobalOpts) -> KafkaResult<()> {
    let config = global.to_kafka_config();
    let admin = KafkaAdminClient::new(&config)?;
    let mut report = crate::ops::topology::collect(&admin)?;

    // Apply filtering
    if let Some(ref t_filter) = args.topic {
        report.partitions.retain(|p| p.topic.contains(t_filter));
    }
    if args.under_replicated_only {
        report.partitions.retain(|p| p.is_under_replicated);
    }
    if args.imbalanced_only {
        report.partitions.retain(|p| p.is_leader_imbalanced);
    }

    output::emit(global.wants_json(args.json), &report, |r| {
        render_topology_report(r, &args)
    })
}

fn render_topology_report(report: &ClusterTopologyReport, args: &TopologyArgs) {
    println!("\n══ Kafka Cluster Topology & Partition Placement ══");

    // 1. Overview Table
    let mut overview_table = output::fixed_table(vec![
        Cell::new("Brokers").fg(Color::Cyan),
        Cell::new("Topics").fg(Color::Cyan),
        Cell::new("Total Partitions").fg(Color::Cyan),
        Cell::new("Under-Replicated (URP)").fg(Color::Cyan),
        Cell::new("Leader Imbalance").fg(Color::Cyan),
        Cell::new("Rack Aware").fg(Color::Cyan),
    ]);

    let urp_cell = if report.under_replicated_count > 0 {
        Cell::new(format!("⚠ {}", report.under_replicated_count))
            .fg(Color::Red)
            .set_alignment(CellAlignment::Right)
    } else {
        Cell::new("0 (Clean)")
            .fg(Color::Green)
            .set_alignment(CellAlignment::Right)
    };

    let imbalance_cell = if report.imbalanced_leaders_count > 0 {
        Cell::new(format!("▲ {}", report.imbalanced_leaders_count))
            .fg(Color::Yellow)
            .set_alignment(CellAlignment::Right)
    } else {
        Cell::new("0 (Clean)")
            .fg(Color::Green)
            .set_alignment(CellAlignment::Right)
    };

    let rack_cell = if report.rack_aware {
        Cell::new(format!("Yes ({} Racks)", report.rack_distribution.len())).fg(Color::Green)
    } else {
        Cell::new("No (Default Rack)").fg(Color::DarkGrey)
    };

    overview_table.add_row(vec![
        Cell::new(report.brokers_count.to_string()).set_alignment(CellAlignment::Right),
        Cell::new(report.topics_count.to_string()).set_alignment(CellAlignment::Right),
        Cell::new(report.total_partitions.to_string()).set_alignment(CellAlignment::Right),
        urp_cell,
        imbalance_cell,
        rack_cell,
    ]);

    println!("{overview_table}\n");

    // 2. Broker Workload Table
    let mut broker_table = output::fixed_table(vec![
        Cell::new("Broker ID").fg(Color::Cyan),
        Cell::new("Endpoint").fg(Color::Cyan),
        Cell::new("Rack").fg(Color::Cyan),
        Cell::new("Role").fg(Color::Cyan),
        Cell::new("Leader Partitions")
            .fg(Color::Cyan)
            .set_alignment(CellAlignment::Right),
        Cell::new("Total Replicas")
            .fg(Color::Cyan)
            .set_alignment(CellAlignment::Right),
    ]);

    for b in &report.brokers {
        let role = if b.is_controller {
            Cell::new("Controller ★").fg(Color::Yellow)
        } else {
            Cell::new("Broker").fg(Color::White)
        };

        broker_table.add_row(vec![
            Cell::new(format!("#{}", b.id)).fg(Color::White),
            Cell::new(format!("{}:{}", b.host, b.port)).fg(Color::DarkGrey),
            Cell::new(b.rack.as_deref().unwrap_or("-")).fg(Color::Cyan),
            role,
            Cell::new(b.leader_partitions.to_string()).set_alignment(CellAlignment::Right),
            Cell::new(b.total_replicas.to_string()).set_alignment(CellAlignment::Right),
        ]);
    }

    println!("{broker_table}\n");

    // 3. Partition Placement Table
    let mut part_table = output::fixed_table(vec![
        Cell::new("Topic").fg(Color::Cyan),
        Cell::new("Part").fg(Color::Cyan),
        Cell::new("Leader").fg(Color::Cyan),
        Cell::new("Preferred").fg(Color::Cyan),
        Cell::new("Replicas").fg(Color::Cyan),
        Cell::new("In-Sync Replicas (ISR)").fg(Color::Cyan),
        Cell::new("Status").fg(Color::Cyan),
    ]);

    let display_limit = if args.under_replicated_only || args.imbalanced_only {
        report.partitions.len()
    } else {
        30
    };

    for p in report.partitions.iter().take(display_limit) {
        let status_cell = if p.is_under_replicated {
            Cell::new("✖ Under-Replicated (URP)").fg(Color::Red)
        } else if p.is_leader_imbalanced {
            Cell::new("▲ Leader Imbalanced").fg(Color::Yellow)
        } else {
            Cell::new("✓ Optimal").fg(Color::Green)
        };

        let leader_str = if p.leader >= 0 {
            format!("#{}", p.leader)
        } else {
            "None".to_string()
        };

        let pref_str = if p.preferred_leader >= 0 {
            format!("#{}", p.preferred_leader)
        } else {
            "-".to_string()
        };

        let rep_str = p
            .replicas
            .iter()
            .map(|r| format!("#{r}"))
            .collect::<Vec<_>>()
            .join(", ");
        let isr_str = p
            .isr
            .iter()
            .map(|r| format!("#{r}"))
            .collect::<Vec<_>>()
            .join(", ");

        part_table.add_row(vec![
            Cell::new(&p.topic).fg(Color::White),
            Cell::new(p.partition_id.to_string()).set_alignment(CellAlignment::Right),
            Cell::new(leader_str).set_alignment(CellAlignment::Center),
            Cell::new(pref_str).set_alignment(CellAlignment::Center),
            Cell::new(rep_str),
            Cell::new(isr_str),
            status_cell,
        ]);
    }

    println!("{part_table}");

    if report.partitions.len() > display_limit {
        println!(
            "ℹ Truncated {} additional partitions. Filter by topic (-t) or use --json for full inventory.\n",
            report.partitions.len() - display_limit
        );
    } else {
        println!();
    }
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

    #[derive(clap::Subcommand, Debug)]
    enum TestCommands {
        Topology(TopologyArgs),
    }

    #[test]
    fn test_clap_topology_args_parsing() {
        let args = CliTestWrapper::try_parse_from([
            "ferrika",
            "topology",
            "-t",
            "orders-v1",
            "--under-replicated-only",
            "--json",
        ]);
        assert!(args.is_ok());
        let TestCommands::Topology(topo) = args.unwrap().cmd;
        assert_eq!(topo.topic.as_deref(), Some("orders-v1"));
        assert!(topo.under_replicated_only);
        assert!(!topo.imbalanced_only);
        assert!(topo.json);
    }
}
