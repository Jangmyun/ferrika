use std::time::Duration;

use clap::Args;
use comfy_table::{Cell, CellAlignment, Color};

use crate::cli::{output, GlobalOpts};
use crate::error::KafkaResult;
use crate::kafka::admin::KafkaAdminClient;
use crate::kafka::consumer::KafkaConsumerClient;
use crate::kafka::internal_topic::is_internal_topic;
use crate::ops::exporter::scrape_cluster_metrics;
use crate::ops::top::{
    RateTracker, TopClusterSnapshot, TopConsumerItem, TopSortOrder, TopTopicItem,
};

/// Timeout for each metadata and watermark request in a snapshot.
const SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Debug, Args)]
#[command(
    name = "top",
    about = "Live read-only dashboard: brokers, topics, consumer group lag and health findings",
    long_about = concat!(
        "Live read-only dashboard: brokers, topics, consumer group lag and health findings.\n\n",
        "On a terminal this opens an interactive dashboard (Tab switches views, / filters, ",
        "s sorts, r refreshes, ? shows the keys, q quits). With --plain, --iterations, --json, ",
        "or when stdout is not a terminal, it prints a refreshing text view instead."
    )
)]
pub struct TopArgs {
    /// Refresh interval in seconds
    #[arg(
        short = 'i',
        long = "interval",
        alias = "interval-secs",
        default_value = "5"
    )]
    pub interval_secs: u64,

    /// Number of refresh iterations before exiting (0 = continuous); prints the text view
    #[arg(short = 'n', long, default_value = "0")]
    pub iterations: usize,

    /// Sorting criteria: 'lag', 'throughput', or 'name'
    #[arg(long, default_value = "lag")]
    pub sort_by: String,

    /// Output a single-shot cluster snapshot in JSON format
    #[arg(long)]
    pub json: bool,

    /// Print the refreshing text view instead of the interactive dashboard
    #[arg(long)]
    pub plain: bool,
}

/// Whether to open the interactive dashboard: only for a person at a
/// terminal who asked for neither text, JSON nor a fixed number of refreshes.
#[cfg(any(feature = "tui", test))]
fn wants_dashboard(args: &TopArgs, json: bool, terminal: bool) -> bool {
    terminal && !json && !args.plain && args.iterations == 0
}

fn parse_sort(sort_by: &str) -> TopSortOrder {
    match sort_by.to_lowercase().as_str() {
        "throughput" | "tps" => TopSortOrder::Throughput,
        "name" => TopSortOrder::Name,
        _ => TopSortOrder::Lag,
    }
}

/// The dashboard's starting sort for `--sort-by`.
#[cfg(feature = "tui")]
fn dashboard_sort(order: TopSortOrder) -> crate::cli::tui::InitialSort {
    use crate::cli::tui::state::{GroupColumn, Sort, TopicColumn};
    let mut sort = crate::cli::tui::InitialSort::default();
    match order {
        TopSortOrder::Lag => {}
        TopSortOrder::Throughput => sort.topics = Sort::by(TopicColumn::Rate),
        TopSortOrder::Name => sort.groups = Sort::by(GroupColumn::Name),
    }
    sort
}

pub async fn run(args: TopArgs, global: &GlobalOpts) -> KafkaResult<()> {
    let sort_order = parse_sort(&args.sort_by);

    #[cfg(feature = "tui")]
    {
        use std::io::IsTerminal;
        let terminal = std::io::stdout().is_terminal() && std::io::stdin().is_terminal();
        if wants_dashboard(&args, global.wants_json(args.json), terminal) {
            return crate::cli::tui::run(
                global,
                Duration::from_secs(args.interval_secs.max(1)),
                dashboard_sort(sort_order),
            );
        }
    }

    let mut rate_tracker = RateTracker::new();
    let mut iteration_count = 0;

    loop {
        iteration_count += 1;

        let mut snapshot = fetch_cluster_snapshot(global, &mut rate_tracker).await?;

        if global.wants_json(args.json) {
            // Throughput needs two samples, so take a second one after the interval.
            tokio::time::sleep(Duration::from_secs(args.interval_secs.max(1))).await;
            snapshot = fetch_cluster_snapshot(global, &mut rate_tracker).await?;
            snapshot.sort_data(sort_order);
            output::print_json(&snapshot)?;
            return Ok(());
        }
        snapshot.sort_data(sort_order);

        // Render full ANSI dashboard screen
        print!("\x1b[2J\x1b[H"); // Clear screen and home cursor
        render_top_dashboard(&snapshot, &args, iteration_count, global);

        if args.iterations > 0 && iteration_count >= args.iterations {
            break;
        }

        tokio::time::sleep(Duration::from_secs(args.interval_secs)).await;
    }

    Ok(())
}

fn render_top_dashboard(
    snapshot: &TopClusterSnapshot,
    args: &TopArgs,
    iteration: usize,
    global: &GlobalOpts,
) {
    let now_epoch = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let hours = (now_epoch % 86400) / 3600;
    let mins = (now_epoch % 3600) / 60;
    let secs = now_epoch % 60;
    let now = format!("{hours:02}:{mins:02}:{secs:02} UTC");
    let bootstrap = global.resolved_bootstrap_servers();

    println!("╔════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗");
    println!("║  🚀 FERRIKA CLUSTER TOP - Real-Time Terminal Sentinel                                                         ║");
    println!("╚════════════════════════════════════════════════════════════════════════════════════════════════════════════════╝");
    println!(
        "  Cluster: {} | Time: {} | Refresh: {}s | Tick: #{}",
        bootstrap, now, args.interval_secs, iteration
    );

    // 1. Cluster Status Bar
    let mut status_table = output::table(vec![
        Cell::new("Brokers Online").fg(Color::Cyan),
        Cell::new("Active Controller").fg(Color::Cyan),
        Cell::new("Total Topics").fg(Color::Cyan),
        Cell::new("Total Partitions").fg(Color::Cyan),
        Cell::new("Global Lag").fg(Color::Cyan),
    ]);

    status_table.add_row(vec![
        Cell::new(format!(
            "{}/{} Online",
            snapshot.brokers_online, snapshot.brokers_total
        ))
        .fg(Color::Green),
        Cell::new(if snapshot.controller_id >= 0 {
            format!("Broker ID #{}", snapshot.controller_id)
        } else {
            "unknown".to_string()
        })
        .fg(Color::Yellow),
        Cell::new(snapshot.active_topics_count.to_string()),
        Cell::new(snapshot.total_partitions.to_string()),
        Cell::new(snapshot.total_lag.to_string()).fg(if snapshot.total_lag > 0 {
            Color::Yellow
        } else {
            Color::Green
        }),
    ]);

    println!("{status_table}\n");

    // 2. Active Consumer Groups (Sorted by Lag)
    println!("👥 Consumer Groups (Sorted by Lag):");
    let mut cg_table = output::fixed_table(vec![
        Cell::new("Group ID").fg(Color::Cyan),
        Cell::new("State").fg(Color::Cyan),
        Cell::new("Total Lag")
            .fg(Color::Cyan)
            .set_alignment(CellAlignment::Right),
        Cell::new("Members")
            .fg(Color::Cyan)
            .set_alignment(CellAlignment::Right),
        Cell::new("Subscribed Topics").fg(Color::Cyan),
    ]);

    for cg in &snapshot.consumers {
        let state_cell = match cg.state.as_str() {
            "Stable" => Cell::new("Stable").fg(Color::Green),
            "Rebalancing" => Cell::new("Rebalancing").fg(Color::Yellow),
            _ => Cell::new(&cg.state).fg(Color::Red),
        };

        let lag_cell = if cg.total_lag > 1000 {
            Cell::new(cg.total_lag.to_string())
                .fg(Color::Red)
                .set_alignment(CellAlignment::Right)
        } else if cg.total_lag > 0 {
            Cell::new(cg.total_lag.to_string())
                .fg(Color::Yellow)
                .set_alignment(CellAlignment::Right)
        } else {
            Cell::new("0")
                .fg(Color::Green)
                .set_alignment(CellAlignment::Right)
        };

        cg_table.add_row(vec![
            Cell::new(&cg.group_id).fg(Color::White),
            state_cell,
            lag_cell,
            Cell::new(cg.members.to_string()).set_alignment(CellAlignment::Right),
            Cell::new(cg.target_topics.join(", ")),
        ]);
    }
    println!("{cg_table}\n");

    // 3. Topics Throughput & Depth
    println!("📈 Top Topics Activity & Message Depth:");
    let mut topic_table = output::fixed_table(vec![
        Cell::new("Topic Name").fg(Color::Cyan),
        Cell::new("Partitions")
            .fg(Color::Cyan)
            .set_alignment(CellAlignment::Right),
        Cell::new("Total Records")
            .fg(Color::Cyan)
            .set_alignment(CellAlignment::Right),
        Cell::new("Throughput (msg/s)")
            .fg(Color::Cyan)
            .set_alignment(CellAlignment::Right),
    ]);

    for t in snapshot.topics.iter().take(10) {
        topic_table.add_row(vec![
            Cell::new(&t.name).fg(Color::White),
            Cell::new(t.partitions.to_string()).set_alignment(CellAlignment::Right),
            Cell::new(t.total_messages.to_string()).set_alignment(CellAlignment::Right),
            Cell::new(format!("{:.1}", t.msg_rate_per_sec))
                .set_alignment(CellAlignment::Right)
                .fg(Color::Green),
        ]);
    }
    println!("{topic_table}\n");

    println!("⌨️  Press Ctrl+C to exit monitor.");
}

/// Fetches cluster metrics from the configured cluster.
///
/// Topic depth is the sum of (high - low) watermarks, throughput is the change of the
/// summed high watermarks between two snapshots, and lag comes from each group's
/// committed offsets.
async fn fetch_cluster_snapshot(
    global: &GlobalOpts,
    rate_tracker: &mut RateTracker,
) -> KafkaResult<TopClusterSnapshot> {
    let config = global.to_kafka_config();
    let admin = KafkaAdminClient::new(&config)?;
    let meta = admin.fetch_raw_metadata(None, Some(SNAPSHOT_TIMEOUT))?;
    let brokers_online = meta.brokers().len();

    // Used only to read watermarks: it never subscribes or commits, so no group is created.
    let probe = KafkaConsumerClient::lookup(&config)?;

    let mut topics = Vec::new();
    let mut total_partitions = 0;
    let keys: Vec<(&str, i32)> = meta
        .topics()
        .iter()
        .filter(|t| !is_internal_topic(t.name()))
        .flat_map(|t| t.partitions().iter().map(move |p| (t.name(), p.id())))
        .collect();
    let watermarks = probe
        .fetch_watermarks_batch(&keys, Some(SNAPSHOT_TIMEOUT))
        .unwrap_or_default();

    for t in meta.topics() {
        let p_count = t.partitions().len();
        total_partitions += p_count;
        if is_internal_topic(t.name()) {
            continue;
        }

        let mut depth: i64 = 0;
        let mut produced: i64 = 0;
        for p in t.partitions() {
            if let Some(&(low, high)) = watermarks.get(&(t.name().to_string(), p.id())) {
                depth += crate::kafka::metrics::partition_depth(low, high);
                produced += high;
            }
        }
        let rate = rate_tracker.track(t.name(), produced);

        topics.push(TopTopicItem {
            name: t.name().to_string(),
            partitions: p_count,
            total_messages: depth,
            msg_rate_per_sec: rate,
        });
    }

    let lag_metrics = scrape_cluster_metrics(&admin, &config).await;
    let group_states = admin
        .list_consumer_group_states(Some(SNAPSHOT_TIMEOUT))
        .unwrap_or_default();
    let mut consumers = Vec::new();
    for (group_id, state, members) in group_states {
        let mut total_lag = 0;
        let mut target_topics: Vec<String> = Vec::new();
        for l in lag_metrics
            .consumer_lags
            .iter()
            .filter(|l| l.group == group_id)
        {
            total_lag += l.lag;
            if !target_topics.contains(&l.topic) {
                target_topics.push(l.topic.clone());
            }
        }
        target_topics.sort();
        consumers.push(TopConsumerItem {
            group_id,
            state,
            total_lag,
            members,
            target_topics,
        });
    }
    let total_lag = consumers.iter().map(|c| c.total_lag).sum();

    Ok(TopClusterSnapshot {
        timestamp_epoch_s: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0),
        brokers_online,
        // Metadata only lists reachable brokers, so the total cannot be known here.
        brokers_total: brokers_online,
        controller_id: admin.active_controller_id().unwrap_or(-1),
        active_topics_count: topics.len(),
        total_partitions,
        total_lag,
        topics,
        consumers,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser)]
    struct Wrapper {
        #[command(subcommand)]
        cmd: Subcommands,
    }

    #[derive(clap::Subcommand)]
    enum Subcommands {
        Top(TopArgs),
    }

    #[test]
    fn test_clap_top_args_parsing() {
        let parsed = Wrapper::parse_from([
            "ferrika",
            "top",
            "-i",
            "5",
            "-n",
            "3",
            "--sort-by",
            "throughput",
            "--json",
        ]);

        match parsed.cmd {
            Subcommands::Top(args) => {
                assert_eq!(args.interval_secs, 5);
                assert_eq!(args.iterations, 3);
                assert_eq!(args.sort_by, "throughput");
                assert!(args.json);
                assert!(!args.plain);
            }
        }
    }

    fn parse(extra: &[&str]) -> TopArgs {
        let mut argv = vec!["ferrika", "top"];
        argv.extend_from_slice(extra);
        match Wrapper::parse_from(argv).cmd {
            Subcommands::Top(args) => args,
        }
    }

    #[test]
    fn interval_defaults_to_five_seconds_and_keeps_its_old_name() {
        assert_eq!(parse(&[]).interval_secs, 5);
        assert_eq!(parse(&["--interval", "7"]).interval_secs, 7);
        assert_eq!(parse(&["--interval-secs", "3"]).interval_secs, 3);
    }

    #[test]
    fn dashboard_only_for_an_open_ended_run_on_a_terminal() {
        assert!(wants_dashboard(&parse(&[]), false, true));
        assert!(!wants_dashboard(&parse(&[]), false, false));
        assert!(!wants_dashboard(&parse(&[]), true, true));
        assert!(!wants_dashboard(&parse(&["--plain"]), false, true));
        assert!(!wants_dashboard(&parse(&["-n", "3"]), false, true));
    }

    #[test]
    fn sort_by_maps_to_the_order() {
        assert_eq!(parse_sort("TPS"), TopSortOrder::Throughput);
        assert_eq!(parse_sort("name"), TopSortOrder::Name);
        assert_eq!(parse_sort("anything"), TopSortOrder::Lag);
    }

    #[cfg(feature = "tui")]
    #[test]
    fn sort_by_sets_the_dashboard_sort() {
        use crate::cli::tui::state::{GroupColumn, TopicColumn};
        let sort = dashboard_sort(TopSortOrder::Throughput);
        assert_eq!(sort.topics.column, TopicColumn::Rate);
        assert_eq!(sort.groups.column, GroupColumn::Lag);
        let sort = dashboard_sort(TopSortOrder::Name);
        assert_eq!(sort.topics.column, TopicColumn::Name);
        assert_eq!(sort.groups.column, GroupColumn::Name);
    }
}
