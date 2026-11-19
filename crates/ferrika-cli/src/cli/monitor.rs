use std::time::Duration;

use clap::{Args, Subcommand};
use colored::Colorize;
use comfy_table::{Cell, CellAlignment, Color};

use crate::cli::{output, print_connection_hint, GlobalOpts, OutputFormat};
use crate::error::KafkaError;
use crate::kafka::admin::KafkaAdminClient;
use crate::kafka::consumer::KafkaConsumerClient;
use crate::kafka::metrics::{calculate_partition_lag, PartitionLag};
use crate::model::cluster::{BrokerInfo, ConsumerGroupSummary, ConsumerState};

/// Command line arguments for cluster and consumer group monitoring.
#[derive(Args, Debug, Clone, PartialEq, Eq)]
pub struct MonitorArgs {
    #[command(subcommand)]
    pub command: MonitorSubcommands,

    /// Continuously refresh terminal view
    #[arg(short = 'w', long, global = true)]
    pub watch: bool,
}

/// Supported subcommands for `ferrika monitor`.
#[derive(Subcommand, Debug, Clone, PartialEq, Eq)]
pub enum MonitorSubcommands {
    /// Monitor Consumer Group lag across partitions
    Lag {
        /// Target Consumer Group ID (if omitted, lists all consumer groups in cluster)
        group_id: Option<String>,

        /// Filter by specific topic name
        #[arg(short, long)]
        topic: Option<String>,

        /// Refresh interval in seconds when in watch mode
        #[arg(short = 'i', long, default_value_t = 2)]
        interval: u64,
    },
    /// Monitor Kafka cluster brokers, controllers, and partition distribution
    Brokers {
        /// Refresh interval in seconds when in watch mode
        #[arg(short = 'i', long, default_value_t = 2)]
        interval: u64,
    },
    /// Run a lightweight Prometheus metrics exporter HTTP server (Kubernetes ready)
    Export {
        /// Port to bind the Prometheus metrics HTTP server to
        #[arg(short = 'P', long, default_value_t = 9102)]
        port: u16,

        /// Host or IP address to bind to. Defaults to loopback because the
        /// endpoints have no authentication; pass `--host 0.0.0.0` to expose
        /// them (for example inside a Kubernetes pod)
        #[arg(long, default_value = "127.0.0.1")]
        host: String,

        /// Background metrics scrape interval in seconds
        #[arg(short = 'i', long, default_value_t = 15)]
        scrape_interval: u64,
    },
}

/// Executes the monitor CLI command.
pub async fn run(args: MonitorArgs, global: &GlobalOpts) -> anyhow::Result<()> {
    // If running exporter server, bypass terminal watch loop
    if let MonitorSubcommands::Export {
        port,
        ref host,
        scrape_interval,
    } = args.command
    {
        return handle_export(global, host, port, scrape_interval).await;
    }

    let interval_secs = match &args.command {
        MonitorSubcommands::Lag { interval, .. } | MonitorSubcommands::Brokers { interval } => {
            *interval
        }
        MonitorSubcommands::Export { .. } => unreachable!(),
    };
    let interval = Duration::from_secs(interval_secs.max(1));

    loop {
        if args.watch {
            // Clear screen and reset cursor to (1,1)
            print!("\x1B[2J\x1B[1;1H");
            let now = chrono_or_simple_time();
            println!(
                "{}",
                format!(
                    "── Ferrika Live Monitor ── [{}] (Refresh: {}s | Ctrl+C to exit) ──",
                    now, interval_secs
                )
                .bright_black()
                .bold()
            );
            println!();
        }

        match &args.command {
            MonitorSubcommands::Lag {
                group_id, topic, ..
            } => {
                handle_lag(global, group_id.as_deref(), topic.as_deref()).await?;
            }
            MonitorSubcommands::Brokers { .. } => {
                handle_brokers(global).await?;
            }
            MonitorSubcommands::Export { .. } => unreachable!(),
        }

        if !args.watch {
            break;
        }

        tokio::select! {
            _ = tokio::time::sleep(interval) => {},
            _ = tokio::signal::ctrl_c() => {
                println!("\n{}", "Live monitor stopped by user.".yellow());
                break;
            }
        }
    }

    Ok(())
}

async fn handle_brokers(global: &GlobalOpts) -> anyhow::Result<()> {
    let kafka_config = global.to_kafka_config();
    let admin = match KafkaAdminClient::new(&kafka_config) {
        Ok(client) => client,
        Err(err) => {
            print_connection_hint(&global.to_kafka_config(), &err);
            return Err(err.into());
        }
    };

    match admin.list_brokers(None) {
        Ok(brokers) => {
            match global.format {
                OutputFormat::Table => {
                    let table_str = render_broker_table(&brokers);
                    println!("{table_str}");
                    println!(
                        "{}",
                        format!("Total Brokers: {}", brokers.len())
                            .bright_black()
                            .italic()
                    );
                }
                OutputFormat::Json => {
                    output::print_json(&brokers)?;
                }
            }
            Ok(())
        }
        Err(err) => {
            print_connection_hint(&global.to_kafka_config(), &err);
            Err(err.into())
        }
    }
}

/// Errors unless `group` is one of the cluster's `groups`.
fn ensure_group_exists(groups: &[String], group: &str) -> Result<(), KafkaError> {
    if groups.iter().any(|g| g == group) {
        Ok(())
    } else {
        Err(KafkaError::config(format!(
            "Consumer group '{group}' does not exist"
        )))
    }
}

async fn handle_lag(
    global: &GlobalOpts,
    group_id: Option<&str>,
    topic_filter: Option<&str>,
) -> anyhow::Result<()> {
    let kafka_config = global.to_kafka_config();
    let admin = match KafkaAdminClient::new(&kafka_config) {
        Ok(client) => client,
        Err(err) => {
            print_connection_hint(&global.to_kafka_config(), &err);
            return Err(err.into());
        }
    };

    if let Some(target_group) = group_id {
        // A group that does not exist has no lag to report; "0" would hide a typo.
        let groups = match admin.list_consumer_groups(None) {
            Ok(g) => g,
            Err(err) => {
                print_connection_hint(&global.to_kafka_config(), &err);
                return Err(err.into());
            }
        };
        ensure_group_exists(&groups, target_group)?;

        // Detailed partition lag for a specific consumer group
        let summaries = match admin.list_topic_summaries(None) {
            Ok(s) => s,
            Err(err) => {
                print_connection_hint(&global.to_kafka_config(), &err);
                return Err(err.into());
            }
        };

        let mut consumer_cfg = kafka_config.clone();
        consumer_cfg.group_id = Some(target_group.to_string());
        let consumer = match KafkaConsumerClient::new(&consumer_cfg) {
            Ok(c) => c,
            Err(err) => {
                print_connection_hint(&global.to_kafka_config(), &err);
                return Err(err.into());
            }
        };

        let mut queried_partitions = rdkafka::TopicPartitionList::new();
        for topic in &summaries {
            if topic.is_internal || topic_filter.is_some_and(|filter| topic.name != filter) {
                continue;
            }
            for partition in &topic.partitions {
                queried_partitions.add_partition(&topic.name, partition.partition_id);
            }
        }
        let committed_tpl = consumer.fetch_committed_offsets_for(&queried_partitions, None)?;

        // Without --topic, only report topics the group has committed offsets for.
        // Counting every topic in the cluster as unconsumed would inflate the group's lag.
        let consumed_topics: std::collections::HashSet<String> = committed_tpl
            .elements()
            .iter()
            .filter(|elem| matches!(elem.offset(), rdkafka::Offset::Offset(_)))
            .map(|elem| elem.topic().to_string())
            .collect();

        let mut partition_lags = Vec::new();
        for topic in &summaries {
            if let Some(filter) = topic_filter {
                if topic.name != filter {
                    continue;
                }
            } else if !consumed_topics.contains(&topic.name) {
                continue;
            }
            if topic.is_internal {
                continue;
            }

            for p in &topic.partitions {
                let (low, high) = consumer.fetch_watermarks(&topic.name, p.partition_id, None)?;

                let current_offset = committed_tpl
                    .find_partition(&topic.name, p.partition_id)
                    .map(|elem| match elem.offset() {
                        rdkafka::Offset::Offset(off) => off,
                        _ => -1,
                    })
                    .unwrap_or(-1);

                let lag = calculate_partition_lag(current_offset, high, low);
                partition_lags.push(PartitionLag {
                    topic: topic.name.clone(),
                    partition: p.partition_id,
                    current_offset,
                    high_watermark: high,
                    low_watermark: low,
                    lag,
                    is_caught_up: lag == 0,
                });
            }
        }

        // How old the first unconsumed record is on each lagging partition.
        let positions: Vec<(String, i32, i64)> = partition_lags
            .iter()
            .filter(|p| p.lag > 0)
            .map(|p| (p.topic.clone(), p.partition, p.current_offset.max(-1)))
            .collect();
        let now = jiff::Timestamp::now().as_millisecond();
        let behind: std::collections::HashMap<(String, i32), i64> =
            crate::kafka::consumer::record_timestamps(
                &kafka_config,
                &positions,
                std::time::Duration::from_secs(5),
            )
            .await
            .unwrap_or_default()
            .into_iter()
            .map(|(k, ts)| (k, (now - ts).max(0)))
            .collect();

        match global.format {
            OutputFormat::Table => {
                println!(
                    "{} {}",
                    "Consumer Group:".bright_black(),
                    target_group.cyan().bold()
                );
                let table_str = render_lag_table_with(&partition_lags, &behind);
                println!("{table_str}");

                let total_lag: i64 = partition_lags.iter().map(|p| p.lag).sum();
                println!(
                    "{}",
                    format!("Total Group Lag: {}", total_lag)
                        .bright_white()
                        .bold()
                );
            }
            OutputFormat::Json => {
                let items: Vec<serde_json::Value> = partition_lags
                    .iter()
                    .map(|p| {
                        let mut v = serde_json::to_value(p).unwrap_or_default();
                        if let (Some(obj), Some(ms)) = (
                            v.as_object_mut(),
                            behind.get(&(p.topic.clone(), p.partition)),
                        ) {
                            obj.insert("behind_ms".into(), (*ms).into());
                        }
                        v
                    })
                    .collect();
                output::print_json(&items)?;
            }
        }
    } else {
        // Overview of all consumer groups
        let groups = match admin.list_consumer_groups(None) {
            Ok(g) => g,
            Err(err) => {
                print_connection_hint(&global.to_kafka_config(), &err);
                return Err(err.into());
            }
        };

        let group_summaries: Vec<ConsumerGroupSummary> = groups
            .into_iter()
            .map(|id| ConsumerGroupSummary {
                group_id: id,
                state: ConsumerState::Stable,
                active_members: 0,
                target_topics: Vec::new(),
                current_offset: 0,
                end_offset: 0,
                total_lag: 0,
                coordinator_node: "-".to_string(),
                consume_rate: None,
                partitions: Vec::new(),
            })
            .collect();

        match global.format {
            OutputFormat::Table => {
                let table_str = render_group_list_table(&group_summaries);
                println!("{table_str}");
                println!(
                    "{}",
                    "Tip: Run 'ferrika monitor lag <group-id>' to view detailed partition lag."
                        .bright_black()
                        .italic()
                );
            }
            OutputFormat::Json => {
                output::print_json(&group_summaries)?;
            }
        }
    }

    Ok(())
}

async fn handle_export(
    global: &GlobalOpts,
    host: &str,
    port: u16,
    scrape_interval_secs: u64,
) -> anyhow::Result<()> {
    let kafka_config = global.to_kafka_config();
    let admin = match KafkaAdminClient::new(&kafka_config) {
        Ok(client) => std::sync::Arc::new(client),
        Err(err) => {
            print_connection_hint(&global.to_kafka_config(), &err);
            return Err(err.into());
        }
    };

    let interval = Duration::from_secs(scrape_interval_secs.max(1));

    println!(
        "{}",
        "── Ferrika Prometheus Metrics Exporter ──".cyan().bold()
    );
    println!(
        "  {} http://{}:{}/metrics",
        "Metrics URL:".green().bold(),
        host,
        port
    );
    println!(
        "  {} http://{}:{}/healthz",
        "Healthz URL:".green().bold(),
        host,
        port
    );
    println!(
        "  {} {}s",
        "Scrape Interval:".bright_black(),
        scrape_interval_secs
    );
    println!(
        "  {} {}",
        "Bootstrap Servers:".bright_black(),
        global.resolved_bootstrap_servers().cyan()
    );
    println!(
        "{}",
        "Press Ctrl+C to stop the exporter server."
            .bright_black()
            .italic()
    );
    println!();

    crate::ops::exporter::start_exporter_server(admin, kafka_config, host, port, interval).await?;
    Ok(())
}

/// Formats broker metadata into a Catppuccin-styled comfy-table.
pub fn render_broker_table(brokers: &[BrokerInfo]) -> String {
    let mut table = output::table(vec![
        Cell::new("BROKER ID")
            .fg(Color::Cyan)
            .set_alignment(CellAlignment::Right),
        Cell::new("ENDPOINT (HOST:PORT)").fg(Color::Blue),
        Cell::new("ROLE").fg(Color::Magenta),
        Cell::new("LEADERS")
            .fg(Color::Green)
            .set_alignment(CellAlignment::Right),
        Cell::new("REPLICAS")
            .fg(Color::Yellow)
            .set_alignment(CellAlignment::Right),
        Cell::new("STATUS")
            .fg(Color::Green)
            .set_alignment(CellAlignment::Center),
    ]);

    for b in brokers {
        let role_cell = if b.is_controller {
            Cell::new("Controller").fg(Color::Magenta)
        } else {
            Cell::new("Broker").fg(Color::DarkGrey)
        };

        let status_cell = if b.is_online {
            Cell::new("Online").fg(Color::Green)
        } else {
            Cell::new("Offline").fg(Color::Red)
        };

        table.add_row(vec![
            Cell::new(b.id.to_string())
                .fg(Color::Cyan)
                .set_alignment(CellAlignment::Right),
            Cell::new(format!("{}:{}", b.host, b.port)).fg(Color::Blue),
            role_cell,
            Cell::new(b.partition_leaders.to_string())
                .fg(Color::Green)
                .set_alignment(CellAlignment::Right),
            Cell::new(b.replicas_hosted.to_string())
                .fg(Color::Yellow)
                .set_alignment(CellAlignment::Right),
            status_cell.set_alignment(CellAlignment::Center),
        ]);
    }

    table.to_string()
}

/// Formats partition lag details into a color-highlighted table.
pub fn render_lag_table(lags: &[PartitionLag]) -> String {
    render_lag_table_with(lags, &std::collections::HashMap::new())
}

use crate::model::format::format_age;

/// [`render_lag_table`] with a BEHIND column: the age of the first
/// unconsumed record per partition, where known.
pub fn render_lag_table_with(
    lags: &[PartitionLag],
    behind: &std::collections::HashMap<(String, i32), i64>,
) -> String {
    let mut table = output::table(vec![
        Cell::new("TOPIC").fg(Color::Cyan),
        Cell::new("PARTITION")
            .fg(Color::Cyan)
            .set_alignment(CellAlignment::Right),
        Cell::new("CURRENT OFFSET")
            .fg(Color::Blue)
            .set_alignment(CellAlignment::Right),
        Cell::new("LOG END OFFSET")
            .fg(Color::Blue)
            .set_alignment(CellAlignment::Right),
        Cell::new("LAG")
            .fg(Color::Yellow)
            .set_alignment(CellAlignment::Right),
        Cell::new("BEHIND")
            .fg(Color::Yellow)
            .set_alignment(CellAlignment::Right),
        Cell::new("STATUS").set_alignment(CellAlignment::Center),
    ]);

    for p in lags {
        let (lag_color, status_text) = if p.lag == 0 {
            (Color::Green, "OK")
        } else if p.lag < 1000 {
            (Color::Yellow, "WARNING")
        } else {
            (Color::Red, "CRITICAL")
        };

        let current_str = if p.current_offset < 0 {
            "-".to_string()
        } else {
            p.current_offset.to_string()
        };

        table.add_row(vec![
            Cell::new(&p.topic).fg(Color::Cyan),
            Cell::new(p.partition.to_string())
                .fg(Color::Cyan)
                .set_alignment(CellAlignment::Right),
            Cell::new(current_str)
                .fg(Color::Blue)
                .set_alignment(CellAlignment::Right),
            Cell::new(p.high_watermark.to_string())
                .fg(Color::Blue)
                .set_alignment(CellAlignment::Right),
            Cell::new(p.lag.to_string())
                .fg(lag_color)
                .set_alignment(CellAlignment::Right),
            Cell::new(match behind.get(&(p.topic.clone(), p.partition)) {
                Some(ms) if p.lag > 0 => format_age(*ms),
                _ => "-".to_string(),
            })
            .fg(lag_color)
            .set_alignment(CellAlignment::Right),
            Cell::new(status_text)
                .fg(lag_color)
                .set_alignment(CellAlignment::Center),
        ]);
    }

    table.to_string()
}

/// Formats consumer group summary list into a comfy-table.
pub fn render_group_list_table(groups: &[ConsumerGroupSummary]) -> String {
    let mut table = output::table(vec![
        Cell::new("CONSUMER GROUP ID").fg(Color::Cyan),
        Cell::new("STATE").fg(Color::Green),
        Cell::new("COORDINATOR").fg(Color::Blue),
    ]);

    for g in groups {
        table.add_row(vec![
            Cell::new(&g.group_id).fg(Color::Cyan),
            Cell::new(g.state.as_str()).fg(Color::Green),
            Cell::new(&g.coordinator_node).fg(Color::Blue),
        ]);
    }

    table.to_string()
}

fn chrono_or_simple_time() -> String {
    let now = std::time::SystemTime::now();
    let duration = now
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let total_secs = duration.as_secs();
    let hours = (total_secs / 3600) % 24;
    let mins = (total_secs / 60) % 60;
    let secs = total_secs % 60;
    format!("{:02}:{:02}:{:02} UTC", hours, mins, secs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lag_for_a_missing_group_is_an_error() {
        let groups = vec!["a".to_string()];
        assert!(ensure_group_exists(&groups, "a").is_ok());
        assert!(ensure_group_exists(&groups, "b").is_err());
        assert!(ensure_group_exists(&[], "a").is_err());
    }

    #[test]
    fn test_render_broker_table() {
        let brokers = vec![
            BrokerInfo {
                id: 1,
                host: "localhost".into(),
                port: 9092,
                is_controller: true,
                partition_leaders: 8,
                replicas_hosted: 16,
                is_online: true,
                log_dir_bytes: None,
                log_dirs: Vec::new(),
            },
            BrokerInfo {
                id: 2,
                host: "localhost".into(),
                port: 9093,
                is_controller: false,
                partition_leaders: 8,
                replicas_hosted: 16,
                is_online: true,
                log_dir_bytes: None,
                log_dirs: Vec::new(),
            },
        ];

        let output = render_broker_table(&brokers);
        assert!(output.contains("BROKER ID"));
        assert!(output.contains("ENDPOINT"));
        assert!(output.contains("localhost:9092"));
        assert!(output.contains("Controller"));
    }

    #[test]
    fn test_render_lag_table_colors() {
        let lags = vec![
            PartitionLag {
                topic: "orders".into(),
                partition: 0,
                current_offset: 100,
                high_watermark: 100,
                low_watermark: 0,
                lag: 0,
                is_caught_up: true,
            },
            PartitionLag {
                topic: "orders".into(),
                partition: 1,
                current_offset: 50,
                high_watermark: 550,
                low_watermark: 0,
                lag: 500,
                is_caught_up: false,
            },
            PartitionLag {
                topic: "orders".into(),
                partition: 2,
                current_offset: 10,
                high_watermark: 2500,
                low_watermark: 0,
                lag: 2490,
                is_caught_up: false,
            },
        ];

        let output = render_lag_table(&lags);
        assert!(output.contains("OK"));
        assert!(output.contains("WARNING"));
        assert!(output.contains("CRITICAL"));
        assert!(output.contains("2490"));
    }

    #[test]
    fn test_clap_monitor_args_parsing() {
        use clap::Parser;

        #[derive(Parser, Debug)]
        struct TestCli {
            #[command(subcommand)]
            cmd: MonitorSubcommands,
        }

        #[derive(Parser, Debug)]
        struct MonitorCli {
            #[command(flatten)]
            args: MonitorArgs,
        }

        let cli =
            MonitorCli::try_parse_from(["test", "lag", "my-group", "--watch", "--interval", "2"])
                .unwrap();
        assert!(cli.args.watch);
        match cli.args.command {
            MonitorSubcommands::Lag { interval, .. } => assert_eq!(interval, 2),
            _ => panic!("Expected lag command"),
        }

        let cli = TestCli::try_parse_from(["test", "lag", "my-group", "-t", "orders"]).unwrap();
        assert_eq!(
            cli.cmd,
            MonitorSubcommands::Lag {
                group_id: Some("my-group".to_string()),
                topic: Some("orders".to_string()),
                interval: 2,
            }
        );

        let cli = TestCli::try_parse_from(["test", "brokers"]).unwrap();
        assert_eq!(cli.cmd, MonitorSubcommands::Brokers { interval: 2 });

        let cli = TestCli::try_parse_from([
            "test",
            "export",
            "-P",
            "9200",
            "--host",
            "127.0.0.1",
            "-i",
            "30",
        ])
        .unwrap();

        match cli.cmd {
            MonitorSubcommands::Export {
                port,
                host,
                scrape_interval,
            } => {
                assert_eq!(port, 9200);
                assert_eq!(host, "127.0.0.1");
                assert_eq!(scrape_interval, 30);
            }
            _ => panic!("Expected Export subcommand"),
        }
    }
}
