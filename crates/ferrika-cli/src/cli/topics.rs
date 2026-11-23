use std::collections::HashMap;

use clap::{Args, Subcommand};
use colored::Colorize;
use comfy_table::{Cell, CellAlignment, Color};
use serde_json::json;

use crate::cli::offset::confirm;
use crate::cli::record::{self, Change, CANNOT_UNDO};
use crate::cli::{output, print_connection_hint, GlobalOpts, OutputFormat};
use crate::kafka::admin::{KafkaAdminClient, TopicCreation, BROKER_DEFAULT};
use crate::kafka::consumer::KafkaConsumerClient;
use crate::kafka::wire::{incremental_alter_topic_config, ConfigChange};
use crate::model::cluster::TopicSummary;
use crate::model::history::{Reversibility, State};
use crate::ops::history::capture;
use crate::ops::time_histogram::{
    self, BucketSpec, BucketState, CostPolicy, HistogramRequest, TimeHistogram, TimeRange,
};

/// Command line arguments for topic management.
#[derive(Args, Debug, Clone)]
pub struct TopicsArgs {
    #[command(subcommand)]
    pub command: TopicsSubcommands,
}

/// Supported subcommands for `ferrika topics`.
#[derive(Subcommand, Debug, Clone, PartialEq, Eq)]
pub enum TopicsSubcommands {
    /// List all topics in the Kafka cluster
    List {
        /// Include internal system topics (e.g. __consumer_offsets)
        #[arg(short = 'a', long)]
        include_internal: bool,
    },
    /// Describe partition and replica metadata for a topic
    Describe {
        /// Name of the topic to describe
        topic: String,
    },
    /// Read a whole topic once and summarise its records: counts per
    /// partition, key and value sizes, tombstones, distinct keys, time span
    Analyze {
        /// Topic name
        topic: String,
    },
    /// Count messages per time bucket from offsets, without reading
    /// records: bursts, gaps and partition skew (last 24 hours by default)
    #[command(visible_alias = "timeline")]
    Histogram {
        /// Topic name
        topic: String,
        /// Only these partitions (repeatable; default all)
        #[arg(short = 'p', long = "partition")]
        partitions: Vec<i32>,
        /// How far back from now (e.g. 30m, 6h, 7d)
        #[arg(long, value_parser = parse_since, conflicts_with_all = ["from", "all"])]
        since: Option<std::time::Duration>,
        /// Start: 30m (ago), epoch ms or YYYY-MM-DD[ HH:MM[:SS]] (UTC)
        #[arg(long, value_parser = parse_cli_time, conflicts_with = "all")]
        from: Option<i64>,
        /// End, in the same formats (default now)
        #[arg(long, value_parser = parse_cli_time, requires = "from")]
        to: Option<i64>,
        /// From the oldest retained record until now (reads that record)
        #[arg(long)]
        all: bool,
        /// Bucket width: auto, or e.g. 1m, 5m, 1h, 1d
        #[arg(long, value_parser = parse_bucket, default_value = "auto")]
        bucket: std::time::Duration,
        /// About how many buckets an auto width aims for
        #[arg(long, default_value_t = 48, value_parser = clap::value_parser!(u16).range(1..=500))]
        buckets: u16,
        /// One column per partition
        #[arg(long)]
        by_partition: bool,
        /// Run even when it needs more lookups than the cost limit
        #[arg(short = 'y', long)]
        yes: bool,
        /// Read the records in the range and count them by their own
        /// timestamps (timestamps only; needs READ)
        #[arg(long)]
        exact: bool,
        /// With --exact: refuse ranges holding more records than this
        #[arg(long, default_value_t = time_histogram::EXACT_LIMIT, requires = "exact")]
        limit: u64,
    },
    /// Create a new Kafka topic
    ///
    /// Without -p or -r the broker's defaults apply (`num.partitions`,
    /// `default.replication.factor`), as with kafka-topics.sh; brokers older
    /// than Kafka 2.4 get 1 instead. The resulting counts are shown.
    Create {
        /// Topic name to create
        name: String,
        /// Number of partitions (default: the broker's `num.partitions`)
        #[arg(short, long, value_parser = clap::value_parser!(i32).range(1..))]
        partitions: Option<i32>,
        /// Replication factor (default: the broker's `default.replication.factor`)
        #[arg(short, long, value_parser = clap::value_parser!(i32).range(1..))]
        replication: Option<i32>,
        /// Optional configuration overrides in KEY=VALUE format
        #[arg(short = 'c', long = "config")]
        configs: Vec<String>,
    },
    /// Create a topic with the partitions, replication factor and config
    /// overrides of another (records are not copied)
    Clone {
        /// Topic to copy the settings of
        source: String,
        /// Name of the new topic
        target: String,
        /// Partition count (defaults to the source's)
        #[arg(short, long)]
        partitions: Option<i32>,
        /// Replication factor (defaults to the source's)
        #[arg(short, long)]
        replication: Option<i32>,
    },
    /// Show or change a topic's config overrides
    ///
    /// Without --set/--delete, lists the overrides. Only the listed keys change;
    /// other overrides are kept.
    Config {
        /// Topic name
        topic: String,
        /// Set an override, KEY=VALUE (repeatable)
        #[arg(short = 's', long = "set", value_name = "KEY=VALUE")]
        set: Vec<String>,
        /// Remove an override so the broker default applies (repeatable)
        #[arg(short = 'd', long = "delete", value_name = "KEY")]
        delete: Vec<String>,
        /// Let the broker validate the changes without applying them
        #[arg(long)]
        dry_run: bool,
    },
    /// Delete every record of a topic, keeping the topic and its configs
    ///
    /// Moves each partition's start (low watermark) to its current end.
    /// Compacted topics cannot be purged.
    Purge {
        /// Topic name
        topic: String,
        /// Only these partitions (repeatable); all partitions by default
        #[arg(short = 'p', long = "partition", value_name = "N")]
        partitions: Vec<i32>,
        /// Skip confirmation prompt
        #[arg(short = 'y', long)]
        yes: bool,
    },
    /// Move partition leadership back to each partition's preferred (first)
    /// replica, e.g. after a broker restart left leaders unbalanced
    ElectLeaders {
        /// Only these topics; every partition of the cluster by default
        topics: Vec<String>,
    },
    /// Change a topic's replication factor with as little data movement as
    /// possible (current replicas stay; new ones go to the least-loaded brokers)
    SetReplication {
        /// Topic name
        topic: String,
        /// New replication factor
        replication: usize,
        /// Apply without asking
        #[arg(short = 'y', long)]
        yes: bool,
    },
    /// Delete an existing Kafka topic
    Delete {
        /// Topic name to delete
        name: String,
        /// Skip confirmation prompt
        #[arg(short = 'y', long)]
        yes: bool,
    },
}

/// Executes the topic command with the provided global options.
pub async fn run(args: TopicsArgs, global: &GlobalOpts) -> anyhow::Result<()> {
    let kafka_config = global.to_kafka_config();
    let admin = match KafkaAdminClient::new(&kafka_config) {
        Ok(client) => client,
        Err(err) => {
            print_connection_hint(&global.to_kafka_config(), &err);
            return Err(err.into());
        }
    };

    match args.command {
        TopicsSubcommands::List { include_internal } => {
            handle_list(&admin, global, include_internal).await
        }
        TopicsSubcommands::Describe { topic } => handle_describe(&admin, global, &topic).await,
        TopicsSubcommands::Analyze { topic } => handle_analyze(&kafka_config, global, &topic)
            .await
            .map_err(Into::into),
        TopicsSubcommands::Histogram {
            topic,
            partitions,
            since,
            from,
            to,
            all,
            bucket,
            buckets,
            by_partition,
            yes,
            exact,
            limit,
        } => {
            let range = match (from, all) {
                (Some(from), _) => {
                    TimeRange::Between(from, to.unwrap_or_else(time_histogram::now_ms))
                }
                (None, true) => TimeRange::All,
                (None, false) => TimeRange::Since(since.unwrap_or(DEFAULT_SINCE)),
            };
            let request = HistogramRequest {
                topic,
                partitions: (!partitions.is_empty()).then_some(partitions),
                range,
                bucket: if bucket.is_zero() {
                    BucketSpec::Auto {
                        target: usize::from(buckets),
                    }
                } else {
                    BucketSpec::Fixed(bucket)
                },
                tz_offset_ms: 0,
                cost: if yes {
                    CostPolicy::Allow
                } else {
                    CostPolicy::Refuse
                },
                exact: exact.then_some(limit),
            };
            handle_histogram(&kafka_config, global, request, by_partition)
                .await
                .map_err(Into::into)
        }
        TopicsSubcommands::Create {
            name,
            partitions,
            replication,
            configs,
        } => handle_create(&admin, global, &name, partitions, replication, &configs).await,
        TopicsSubcommands::Config {
            topic,
            set,
            delete,
            dry_run,
        } => handle_config(&admin, global, &topic, &set, &delete, dry_run).await,
        TopicsSubcommands::Clone {
            source,
            target,
            partitions,
            replication,
        } => {
            let pending = record::start(
                global,
                record::cluster_scope(global, Some(&admin)),
                created_topic(&target),
            );
            let result = admin
                .clone_topic(&source, &target, partitions, replication, None)
                .await;
            record::finish(pending, &result, &format!("Created {target} from {source}"));
            let (partitions, replication, configs) = result?;
            match global.format {
                OutputFormat::Table => println!(
                    "{} Created '{}' from '{}' ({} partitions, replication {}, {} config override(s)).",
                    "✔".green().bold(),
                    target.cyan().bold(),
                    source,
                    partitions,
                    replication,
                    configs
                ),
                OutputFormat::Json => output::print_json(&json!({
                        "status": "created",
                        "topic": target,
                        "source": source,
                        "partitions": partitions,
                        "replication": replication,
                        "config_overrides": configs,
                    }))?,
            }
            Ok(())
        }
        TopicsSubcommands::Delete { name, yes } => handle_delete(&admin, global, &name, yes).await,
        TopicsSubcommands::Purge {
            topic,
            partitions,
            yes,
        } => handle_purge(&admin, global, &topic, &partitions, yes).await,
        TopicsSubcommands::SetReplication {
            topic,
            replication,
            yes,
        } => handle_set_replication(&admin, global, &topic, replication, yes).await,
        TopicsSubcommands::ElectLeaders { topics } => {
            handle_elect_leaders(&admin, global, &topics).await
        }
    }
}

async fn handle_list(
    admin: &KafkaAdminClient,
    global: &GlobalOpts,
    include_internal: bool,
) -> anyhow::Result<()> {
    match admin.list_topic_summaries(None) {
        Ok(all_topics) => {
            let mut filtered: Vec<TopicSummary> = all_topics
                .into_iter()
                .filter(|t| include_internal || !t.is_internal)
                .collect();
            let counted = fill_message_counts(global, &mut filtered).await;

            match global.format {
                OutputFormat::Table => {
                    let table_str = render_topics_table(&filtered, counted);
                    println!("{table_str}");
                    println!(
                        "{}",
                        format!("Total Topics: {}", filtered.len())
                            .bright_black()
                            .italic()
                    );
                }
                OutputFormat::Json => {
                    let mut json = serde_json::to_value(&filtered)?;
                    if !counted {
                        if let Some(items) = json.as_array_mut() {
                            items.iter_mut().for_each(null_message_counts);
                        }
                    }
                    output::print_json(&json)?;
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

async fn handle_describe(
    admin: &KafkaAdminClient,
    global: &GlobalOpts,
    topic_name: &str,
) -> anyhow::Result<()> {
    match admin.describe_topic(topic_name, None) {
        Ok(mut topic) => {
            // Metadata has no configs; read the effective policy and the topic's overrides.
            // Both are best effort: a principal without DescribeConfigs still gets metadata.
            let names = [topic.name.clone()];
            if let Ok(policies) = admin
                .describe_topic_config(&names, "cleanup.policy", None)
                .await
            {
                if let Some(policy) = policies.get(&topic.name) {
                    topic.cleanup_policy = policy.clone();
                }
            }
            // Without DescribeConfigs the overrides are left out, but say why
            // (on stderr, so `--format json` output stays parseable).
            let overrides = match admin.describe_topic_overrides(&topic.name, None).await {
                Ok(overrides) => Some(overrides),
                Err(e) => {
                    eprintln!(
                        "{} config overrides not shown: {e}",
                        "warning:".yellow().bold()
                    );
                    None
                }
            };
            let counted = fill_message_counts(global, std::slice::from_mut(&mut topic)).await;
            match global.format {
                OutputFormat::Table => {
                    println!("{}", format!("Topic: {}", topic.name).cyan().bold());
                    println!(
                        "  {} {}   {} {}   {} {}",
                        "Partitions:".bright_black(),
                        topic.partition_count.to_string().green().bold(),
                        "Replication:".bright_black(),
                        topic.replication_factor.to_string().yellow().bold(),
                        "Internal:".bright_black(),
                        if topic.is_internal {
                            "Yes".yellow()
                        } else {
                            "No".green()
                        }
                    );
                    println!(
                        "  {} {}   {} {}",
                        "Cleanup policy:".bright_black(),
                        topic.cleanup_policy.cyan(),
                        "Messages:".bright_black(),
                        if counted {
                            topic.total_messages.to_string().green().bold()
                        } else {
                            "unknown".bright_black()
                        }
                    );
                    match &overrides {
                        Some(o) if o.is_empty() => println!(
                            "  {} {}",
                            "Config overrides:".bright_black(),
                            "none (broker defaults)".bright_black()
                        ),
                        Some(o) => {
                            println!("  {}", "Config overrides:".bright_black());
                            for (name, value) in o {
                                println!("    {} = {}", name.yellow(), value);
                            }
                        }
                        None => {}
                    }
                    println!();

                    let table_str = render_describe_table(&topic, counted);
                    println!("{table_str}");
                }
                OutputFormat::Json => {
                    let mut json = serde_json::to_value(&topic)?;
                    if !counted {
                        null_message_counts(&mut json);
                    }
                    if let (Some(obj), Some(o)) = (json.as_object_mut(), &overrides) {
                        let map: serde_json::Map<String, serde_json::Value> = o
                            .iter()
                            .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
                            .collect();
                        obj.insert("config_overrides".into(), serde_json::Value::Object(map));
                    }
                    output::print_json(&json)?;
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

async fn handle_create(
    admin: &KafkaAdminClient,
    global: &GlobalOpts,
    name: &str,
    partitions: Option<i32>,
    replication: Option<i32>,
    raw_configs: &[String],
) -> anyhow::Result<()> {
    let parsed_configs = parse_topic_configs(raw_configs)?;

    let pending = record::start(
        global,
        record::cluster_scope(global, Some(admin)),
        created_topic(name),
    );
    let result = admin
        .create_topic_with_broker_defaults(
            name,
            partitions,
            replication,
            Some(&parsed_configs),
            None,
        )
        .await;
    record::finish(pending, &result, &format!("Created topic {name}"));
    match result {
        Ok(created) => {
            if created.fell_back {
                eprintln!(
                    "{} the broker cannot apply its default partition count or replication \
                     factor (Kafka 2.4+ needed), so 1 was used for what -p/-r left out.",
                    "Note:".yellow().bold()
                );
            }
            let (partitions, replication) = created_shape(admin, name, created).await;
            let shown = |n: Option<i32>| n.map_or("unknown".to_string(), |n| n.to_string());
            match global.format {
                OutputFormat::Table => {
                    println!(
                        "{} Topic '{}' created successfully (partitions: {}, replication: {})",
                        "✔".green().bold(),
                        name.cyan().bold(),
                        shown(partitions).green(),
                        shown(replication).yellow()
                    );
                }
                OutputFormat::Json => {
                    let res = json!({
                        "status": "success",
                        "topic": name,
                        "partitions": partitions,
                        "replication": replication,
                        "broker_defaults_unsupported": created.fell_back,
                    });
                    output::print_json(&res)?;
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

/// The partition count and replication factor `name` was created with.
/// Values the broker chose are read back from the metadata, which may lag
/// the creation briefly; `None` if they still cannot be read.
async fn created_shape(
    admin: &KafkaAdminClient,
    name: &str,
    created: TopicCreation,
) -> (Option<i32>, Option<i32>) {
    let sent = |n: i32| (n != BROKER_DEFAULT).then_some(n);
    for attempt in 0..5 {
        if attempt > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        }
        let Ok(topic) = admin.describe_topic(name, Some(std::time::Duration::from_secs(5))) else {
            continue;
        };
        if let Some(shape) = shape_of(&topic) {
            return (Some(shape.0), Some(shape.1));
        }
    }
    (sent(created.partitions), sent(created.replication_factor))
}

/// Partition count and replication factor of `topic`; `None` while the
/// metadata has no partitions for it yet.
fn shape_of(topic: &TopicSummary) -> Option<(i32, i32)> {
    let first = topic.partitions.first()?;
    Some((topic.partitions.len() as i32, first.replicas.len() as i32))
}

/// Builds the change list for `topics config`, rejecting malformed entries and
/// keys that are both set and deleted.
pub fn config_changes(set: &[String], delete: &[String]) -> anyhow::Result<Vec<ConfigChange>> {
    let mut changes = Vec::with_capacity(set.len() + delete.len());
    for entry in set {
        let Some((key, value)) = entry.split_once('=') else {
            anyhow::bail!(
                "Invalid --set '{entry}'. Expected KEY=VALUE (e.g. retention.ms=86400000)"
            );
        };
        let key = key.trim();
        if key.is_empty() {
            anyhow::bail!("Invalid --set '{entry}': the key is empty");
        }
        changes.push(ConfigChange::Set(key.to_string(), value.trim().to_string()));
    }
    for key in delete {
        let key = key.trim();
        if key.is_empty() {
            anyhow::bail!("--delete needs a config key");
        }
        if set
            .iter()
            .any(|e| e.split_once('=').map(|(k, _)| k.trim()) == Some(key))
        {
            anyhow::bail!("'{key}' is both set and deleted");
        }
        changes.push(ConfigChange::Delete(key.to_string()));
    }
    Ok(changes)
}

async fn handle_config(
    admin: &KafkaAdminClient,
    global: &GlobalOpts,
    topic: &str,
    set: &[String],
    delete: &[String],
    dry_run: bool,
) -> anyhow::Result<()> {
    let changes = config_changes(set, delete)?;
    // A missing topic is an error, not an empty config.
    admin.describe_topic(topic, None)?;
    if !changes.is_empty() {
        let pending = if dry_run {
            None
        } else {
            let captured = capture::topic_config(admin, topic, &capture::change_keys(&changes))
                .await
                .map(|before| {
                    let after = capture::config_after(&before, &changes);
                    (vec![before], vec![after])
                });
            record::start(
                global,
                record::cluster_scope(global, Some(admin)),
                config_change(captured),
            )
        };
        let kafka_config = global.to_kafka_config();
        let result = incremental_alter_topic_config(&kafka_config, topic, &changes, dry_run);
        record::finish(
            pending,
            &result,
            &format!("Applied {} config change(s) on {topic}", changes.len()),
        );
        if let Err(err) = result {
            print_connection_hint(&global.to_kafka_config(), &err);
            return Err(err.into());
        }
    }
    let overrides = admin.describe_topic_overrides(topic, None).await?;

    match global.format {
        OutputFormat::Table => {
            if !changes.is_empty() {
                let verb = if dry_run { "Validated" } else { "Applied" };
                println!(
                    "{} {verb} {} config change(s) on '{}'{}",
                    "✔".green().bold(),
                    changes.len(),
                    topic.cyan().bold(),
                    if dry_run {
                        " (dry run, nothing changed)"
                    } else {
                        ""
                    }
                );
            }
            if overrides.is_empty() {
                println!("No config overrides on '{topic}' (broker defaults apply)");
            } else {
                let mut table = output::table(vec![
                    Cell::new("Config").fg(Color::Cyan),
                    Cell::new("Value").fg(Color::Cyan),
                ]);
                for (key, value) in &overrides {
                    table.add_row(vec![Cell::new(key), Cell::new(value)]);
                }
                println!("{table}");
            }
        }
        OutputFormat::Json => {
            let overrides: serde_json::Map<String, serde_json::Value> = overrides
                .into_iter()
                .map(|(k, v)| (k, serde_json::Value::String(v)))
                .collect();
            let res = json!({
                "topic": topic,
                "changes": changes.len(),
                "dry_run": dry_run,
                "config_overrides": overrides,
            });
            output::print_json(&res)?;
        }
    }
    Ok(())
}

async fn handle_delete(
    admin: &KafkaAdminClient,
    global: &GlobalOpts,
    name: &str,
    yes: bool,
) -> anyhow::Result<()> {
    if !yes {
        confirm(
            &format!(
                "Are you sure you want to delete topic '{}'? {CANNOT_UNDO} [y/N]: ",
                name.red().bold()
            ),
            "Aborted topic deletion.",
        )?;
    }

    let pending = record::start(
        global,
        record::cluster_scope(global, Some(admin)),
        deleted_topic(admin, name).await,
    );
    let result = admin.delete_topic(name, None).await;
    record::finish(pending, &result, &format!("Deleted topic {name}"));
    match result {
        Ok(_) => {
            match global.format {
                OutputFormat::Table => {
                    println!(
                        "{} Topic '{}' deleted successfully.",
                        "✔".green().bold(),
                        name.cyan().bold()
                    );
                }
                OutputFormat::Json => {
                    let res = json!({
                        "status": "deleted",
                        "topic": name
                    });
                    output::print_json(&res)?;
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

/// The replicas of `partitions` once each has `replication` of them (the
/// broker places them after a `replication.factor` change), waiting up to
/// ten seconds; `None` when they do not settle in time.
async fn settled_replicas(
    admin: &KafkaAdminClient,
    topic: &str,
    partitions: &[i32],
    replication: usize,
) -> Option<Vec<State>> {
    for _ in 0..10 {
        if let Ok(state @ State::Replicas { .. }) =
            capture::replicas(admin, topic, Some(partitions))
        {
            if let State::Replicas { partitions: now } = &state {
                if now.iter().all(|p| p.replicas.len() == replication) {
                    return Some(vec![state]);
                }
            }
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
    None
}

async fn handle_set_replication(
    admin: &KafkaAdminClient,
    global: &GlobalOpts,
    topic: &str,
    replication: usize,
    yes: bool,
) -> anyhow::Result<()> {
    let plan = crate::kafka::reassign::plan_replication_change(admin, topic, replication)?;
    if plan.is_empty() {
        println!("'{topic}' already has replication factor {replication}.");
        return Ok(());
    }
    println!("Planned replica changes for '{}':", topic.cyan().bold());
    for a in &plan {
        println!("  partition {:>3} -> {:?}", a.partition, a.replicas);
    }
    if !yes {
        confirm(
            "Apply? Data is copied to the new replicas in the background. [y/N]: ",
            "Aborted.",
        )?;
    }
    let partitions: Vec<i32> = plan.iter().map(|a| a.partition).collect();
    let after = State::Replicas {
        partitions: plan
            .iter()
            .map(|a| crate::model::history::PartitionReplicas {
                topic: a.topic.clone(),
                partition: a.partition,
                replicas: a.replicas.clone(),
            })
            .collect(),
    };
    let pending = record::start(
        global,
        record::cluster_scope(global, Some(admin)),
        record::change_from_capture(
            Reversibility::Reversible,
            capture::replicas(admin, topic, Some(&partitions))
                .map(|before| (vec![before], vec![after])),
        ),
    );
    let config = global.to_kafka_config();
    let results = tokio::task::spawn_blocking(move || {
        crate::kafka::wire::reassign_or_set_replication(&config, &plan)
    })
    .await?;
    let (results, via_config) = match results {
        Ok((results, via_config)) => (Ok(results), via_config),
        Err(e) => (Err(e), false),
    };
    if via_config {
        println!(
            "  {} This cluster changes replication through the topic's replication.factor; set it to {replication} (the broker places the replicas).",
            "ℹ".cyan()
        );
    }
    let outcome = results
        .as_ref()
        .map_err(ToString::to_string)
        .and_then(
            |results| match results.iter().filter(|r| r.error.is_some()).count() {
                0 => Ok(()),
                n => Err(format!("{n} partition(s) were not reassigned")),
            },
        );
    if let Some(pending) = pending {
        match &outcome {
            Ok(()) => {
                // Through replication.factor the broker picks the replicas:
                // record where they went, or undo sees a drift from the plan.
                let after = if via_config {
                    settled_replicas(admin, topic, &partitions, replication).await
                } else {
                    None
                };
                pending.finish(
                    true,
                    &format!("Reassignment started for {} partition(s)", partitions.len()),
                    after,
                );
            }
            Err(e) => pending.finish(false, e, None),
        }
    }
    let results = results?;
    let failed: Vec<_> = results.iter().filter(|r| r.error.is_some()).collect();
    for r in &failed {
        println!(
            "  {} partition {}: {}",
            "✖".red(),
            r.partition,
            r.error.as_deref().unwrap_or_default()
        );
    }
    if failed.is_empty() {
        println!(
            "{} Reassignment started for {} partition(s); `ferrika topics describe {topic}` shows progress.",
            "✔".green().bold(),
            results.len()
        );
        Ok(())
    } else {
        Err(crate::error::KafkaError::Check(format!(
            "{} partition(s) were not reassigned",
            failed.len()
        ))
        .into())
    }
}

/// What the history and audit log record for an election: partitions the
/// cluster refused make it a failure, not a success.
fn election_result(
    outcome: &Result<crate::kafka::wire::ElectionOutcome, crate::error::KafkaError>,
) -> Result<(), String> {
    let outcome = outcome.as_ref().map_err(ToString::to_string)?;
    match outcome.failed.first() {
        None => Ok(()),
        Some((topic, partition, reason)) => Err(format!(
            "Moved {} partition(s); {} could not move, e.g. {topic}-{partition}: {reason}",
            outcome.elected.len(),
            outcome.failed.len()
        )),
    }
}

async fn handle_elect_leaders(
    admin: &KafkaAdminClient,
    global: &GlobalOpts,
    topics: &[String],
) -> anyhow::Result<()> {
    let scope = if topics.is_empty() {
        None
    } else {
        let mut partitions = Vec::new();
        for topic in topics {
            let detail = admin.describe_topic(topic, None)?;
            partitions.extend(
                detail
                    .partitions
                    .iter()
                    .map(|p| (topic.clone(), p.partition_id)),
            );
        }
        Some(partitions)
    };
    let pending = record::start(
        global,
        record::cluster_scope(global, Some(admin)),
        Change::irreversible(
            Reversibility::NotApplicable,
            "leadership is transient; nothing to undo",
        ),
    );
    let config = global.to_kafka_config();
    let outcome = tokio::task::spawn_blocking(move || {
        crate::kafka::wire::elect_preferred_leaders(&config, scope.as_deref())
    })
    .await?;
    record::finish(
        pending,
        &election_result(&outcome),
        &match &outcome {
            Ok(o) => format!(
                "Moved {} partition(s) to the preferred leader",
                o.elected.len()
            ),
            Err(_) => String::new(),
        },
    );
    let outcome = outcome?;
    match global.format {
        OutputFormat::Json => output::print_json(&json!({
            "elected": outcome.elected,
            "already_preferred": outcome.already_preferred,
            "failed": outcome.failed,
        }))?,
        OutputFormat::Table => {
            let moved = if outcome.elected.len() == 1 {
                "1 partition".to_string()
            } else {
                format!("{} partitions", outcome.elected.len())
            };
            // Kafka lists unchanged partitions only when they were named.
            let already = match outcome.already_preferred {
                0 => String::new(),
                n => format!("; {n} already there"),
            };
            println!(
                "{} Moved {moved} to the preferred leader{already}.",
                "✔".green().bold()
            );
            for (topic, partition) in &outcome.elected {
                println!("  {topic}-{partition}");
            }
            for (topic, partition, reason) in &outcome.failed {
                println!("  {} {topic}-{partition}: {reason}", "✖".red());
            }
        }
    }
    if outcome.failed.is_empty() {
        Ok(())
    } else {
        Err(crate::error::KafkaError::Check(format!(
            "{} partition(s) could not be moved to their preferred leader",
            outcome.failed.len()
        ))
        .into())
    }
}

async fn handle_purge(
    admin: &KafkaAdminClient,
    global: &GlobalOpts,
    topic: &str,
    partitions: &[i32],
    yes: bool,
) -> anyhow::Result<()> {
    // A missing topic or partition is reported before asking to confirm.
    admin.resolve_partitions(topic, partitions, None)?;
    if !yes {
        let scope = if partitions.is_empty() {
            "all partitions".to_string()
        } else {
            format!(
                "partition(s) {}",
                partitions
                    .iter()
                    .map(i32::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };
        confirm(
            &format!(
                "Delete every record in {scope} of '{}'? The topic is kept. {CANNOT_UNDO} [y/N]: ",
                topic.red().bold()
            ),
            "Aborted purge.",
        )?;
    }

    let pending = record::start(
        global,
        record::cluster_scope(global, Some(admin)),
        Change::irreversible(
            Reversibility::Irreversible,
            "purged records cannot be restored",
        ),
    );
    let result = admin.purge_topic(topic, partitions, None).await;
    record::finish(pending, &result, &format!("Purged {topic}"));
    let purged = result?;
    match global.format {
        OutputFormat::Table => println!(
            "{} Purged {} of '{}'.",
            "✔".green().bold(),
            if purged == 1 {
                "1 partition".to_string()
            } else {
                format!("{purged} partitions")
            },
            topic.cyan().bold()
        ),
        OutputFormat::Json => output::print_json(&json!({
            "status": "purged",
            "topic": topic,
            "partitions": purged,
        }))?,
    }
    Ok(())
}

/// The record of creating `topic`: undone by deleting it while it is still
/// unused.
fn created_topic(topic: &str) -> Change {
    Change::topic_created(topic)
}

/// The record of a config change from its before-state read.
fn config_change(captured: crate::error::KafkaResult<(Vec<State>, Vec<State>)>) -> Change {
    record::change_from_capture(Reversibility::Reversible, captured).sensitive_limits()
}

/// The record of deleting `topic`: not undoable, with what it takes to
/// recreate its structure (the records are gone).
async fn deleted_topic(admin: &KafkaAdminClient, topic: &str) -> Change {
    let mut change = Change::irreversible(
        Reversibility::Irreversible,
        "a deleted topic's records cannot be restored",
    );
    if let Ok(summary) = admin.describe_topic(topic, None) {
        let overrides = admin
            .describe_topic_overrides(topic, None)
            .await
            .unwrap_or_default();
        let mut command = format!(
            "ferrika topics create {topic} -p {} -r {}",
            summary.partition_count, summary.replication_factor
        );
        for (key, value) in &overrides {
            command.push_str(&format!(" -c {key}={value}"));
        }
        change = change.info(format!("recreate the structure with: {command}"));
    }
    change
}

/// Parses CLI KEY=VALUE configs into a HashMap.
pub fn parse_topic_configs(configs: &[String]) -> anyhow::Result<HashMap<String, String>> {
    let mut map = HashMap::new();
    for entry in configs {
        let parts: Vec<&str> = entry.splitn(2, '=').collect();
        if parts.len() != 2 {
            anyhow::bail!(
                "Invalid config format '{}'. Expected KEY=VALUE (e.g. cleanup.policy=compact)",
                entry
            );
        }
        map.insert(parts[0].trim().to_string(), parts[1].trim().to_string());
    }
    Ok(map)
}

/// Fills `total_messages` and each partition's `high_watermark` from one
/// batched watermark lookup. Best effort: returns `false` (leaving the zeros)
/// when the lookup fails, e.g. without Describe permission on a topic.
async fn fill_message_counts(global: &GlobalOpts, topics: &mut [TopicSummary]) -> bool {
    // Reads watermarks only; no group is joined or created.
    let Ok(consumer) = KafkaConsumerClient::lookup(&global.to_kafka_config()) else {
        return false;
    };
    let keys: Vec<(&str, i32)> = topics
        .iter()
        .flat_map(|t| {
            t.partitions
                .iter()
                .map(|p| (t.name.as_str(), p.partition_id))
        })
        .collect();
    let watermarks = match consumer.fetch_watermarks_batch_async(&keys, None).await {
        Ok(w) if w.len() == keys.len() => w,
        _ => return false,
    };
    for topic in topics.iter_mut() {
        let mut total = 0;
        for p in &mut topic.partitions {
            if let Some(&(low, high)) = watermarks.get(&(topic.name.clone(), p.partition_id)) {
                p.high_watermark = high;
                p.low_watermark = low;
                total += crate::kafka::metrics::partition_depth(low, high);
            }
        }
        topic.total_messages = total;
    }
    true
}

/// Replaces message counts with `null` in a serialized topic when they could
/// not be read, so a zero is never mistaken for an empty topic.
fn null_message_counts(topic: &mut serde_json::Value) {
    if let Some(obj) = topic.as_object_mut() {
        obj.insert("total_messages".into(), serde_json::Value::Null);
        if let Some(parts) = obj.get_mut("partitions").and_then(|p| p.as_array_mut()) {
            for p in parts.iter_mut().filter_map(|p| p.as_object_mut()) {
                p.insert("high_watermark".into(), serde_json::Value::Null);
                p.insert("low_watermark".into(), serde_json::Value::Null);
            }
        }
    }
}

/// Formats the topic list into a Catppuccin-styled comfy-table. Message counts
/// show "?" unless `counted`.
pub fn render_topics_table(topics: &[TopicSummary], counted: bool) -> String {
    let mut table = output::table(vec![
        Cell::new("TOPIC NAME")
            .fg(Color::Cyan)
            .set_alignment(CellAlignment::Left),
        Cell::new("PARTITIONS")
            .fg(Color::Green)
            .set_alignment(CellAlignment::Right),
        Cell::new("REPLICATION")
            .fg(Color::Yellow)
            .set_alignment(CellAlignment::Right),
        Cell::new("MESSAGES")
            .fg(Color::Blue)
            .set_alignment(CellAlignment::Right),
        Cell::new("INTERNAL")
            .fg(Color::Magenta)
            .set_alignment(CellAlignment::Center),
    ]);

    for topic in topics {
        let internal_str = if topic.is_internal { "yes" } else { "-" };
        let internal_color = if topic.is_internal {
            Color::Yellow
        } else {
            Color::DarkGrey
        };

        table.add_row(vec![
            Cell::new(&topic.name).fg(Color::Cyan),
            Cell::new(topic.partition_count.to_string())
                .fg(Color::Green)
                .set_alignment(CellAlignment::Right),
            Cell::new(topic.replication_factor.to_string())
                .fg(Color::Yellow)
                .set_alignment(CellAlignment::Right),
            Cell::new(count_or_unknown(topic.total_messages, counted))
                .fg(Color::Blue)
                .set_alignment(CellAlignment::Right),
            Cell::new(internal_str)
                .fg(internal_color)
                .set_alignment(CellAlignment::Center),
        ]);
    }

    table.to_string()
}

fn count_or_unknown(value: i64, counted: bool) -> String {
    if counted {
        value.to_string()
    } else {
        "?".to_string()
    }
}

/// Formats partition metadata for `describe` into a Catppuccin-styled comfy-table.
pub fn render_describe_table(topic: &TopicSummary, counted: bool) -> String {
    let mut table = output::table(vec![
        Cell::new("PARTITION")
            .fg(Color::Cyan)
            .set_alignment(CellAlignment::Right),
        Cell::new("LEADER")
            .fg(Color::Green)
            .set_alignment(CellAlignment::Right),
        Cell::new("REPLICAS")
            .fg(Color::Yellow)
            .set_alignment(CellAlignment::Left),
        Cell::new("ISR (IN-SYNC)")
            .fg(Color::Magenta)
            .set_alignment(CellAlignment::Left),
        Cell::new("LOW WATERMARK")
            .fg(Color::Blue)
            .set_alignment(CellAlignment::Right),
        Cell::new("HIGH WATERMARK")
            .fg(Color::Blue)
            .set_alignment(CellAlignment::Right),
    ]);

    for p in &topic.partitions {
        let replicas_str = p
            .replicas
            .iter()
            .map(|r| r.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        let isr_str = p
            .isr
            .iter()
            .map(|i| i.to_string())
            .collect::<Vec<_>>()
            .join(", ");

        table.add_row(vec![
            Cell::new(p.partition_id.to_string())
                .fg(Color::Cyan)
                .set_alignment(CellAlignment::Right),
            Cell::new(p.leader.to_string())
                .fg(Color::Green)
                .set_alignment(CellAlignment::Right),
            Cell::new(format!("[{replicas_str}]")).fg(Color::Yellow),
            Cell::new(format!("[{isr_str}]")).fg(Color::Magenta),
            Cell::new(count_or_unknown(p.low_watermark, counted))
                .fg(Color::Blue)
                .set_alignment(CellAlignment::Right),
            Cell::new(count_or_unknown(p.high_watermark, counted))
                .fg(Color::Blue)
                .set_alignment(CellAlignment::Right),
        ]);
    }

    table.to_string()
}

/// `topics histogram` without `--since`, `--from` or `--all`.
const DEFAULT_SINCE: std::time::Duration = std::time::Duration::from_secs(24 * 3600);

fn parse_since(text: &str) -> Result<std::time::Duration, String> {
    crate::ops::report::parse_window(text)
}

/// `auto` (zero) or a duration such as `5m`.
fn parse_bucket(text: &str) -> Result<std::time::Duration, String> {
    if text.trim().eq_ignore_ascii_case("auto") {
        return Ok(std::time::Duration::ZERO);
    }
    crate::ops::report::parse_window(text)
}

/// A CLI time in UTC: `30m` (ago), epoch ms or `YYYY-MM-DD[ HH:MM[:SS]]`.
fn parse_cli_time(text: &str) -> Result<i64, String> {
    crate::model::time::parse_time_ms_in(text.trim(), time_histogram::now_ms(), 0).ok_or_else(
        || format!("'{text}' is not a time: use 30m, epoch ms or YYYY-MM-DD[ HH:MM[:SS]] (UTC)"),
    )
}

async fn handle_histogram(
    config: &crate::kafka::config::KafkaClientConfig,
    global: &GlobalOpts,
    request: HistogramRequest,
    by_partition: bool,
) -> crate::error::KafkaResult<()> {
    let interactive = global.format == OutputFormat::Table;
    let hist = time_histogram::run(config, request, move |done, total| {
        if interactive {
            eprint!("\rLooked up {done}/{total} partition boundaries…");
        }
    })
    .await?;
    if interactive {
        eprint!("\r{:60}\r", "");
    }
    if hist.partitions.is_empty() && !hist.unavailable.is_empty() {
        return Err(crate::error::KafkaError::Consumer(format!(
            "no partition of '{}' answered (unavailable: {})",
            hist.topic,
            join_ids(&hist.unavailable)
        )));
    }
    if global.format == OutputFormat::Json {
        return output::print_json(&hist);
    }
    let width = std::env::var("COLUMNS")
        .ok()
        .and_then(|c| c.parse::<usize>().ok())
        .map_or(60, |c| c.saturating_sub(30))
        .max(10);
    println!("{}", histogram_text(&hist, by_partition, width));
    Ok(())
}

fn join_ids(ids: &[i32]) -> String {
    ids.iter()
        .map(|p| p.to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

/// `1234567` as `1,234,567`.
fn thousands(n: u64) -> String {
    let digits = n.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (i, c) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(c);
    }
    out
}

/// A bar of `count / max` of `width` cells, in eighths of a cell.
fn bar(count: u64, max: u64, width: usize) -> String {
    const EIGHTHS: [char; 8] = ['▏', '▎', '▍', '▌', '▋', '▊', '▉', '█'];
    if count == 0 || max == 0 || width == 0 {
        return String::new();
    }
    let eighths = (u128::from(count) * width as u128 * 8)
        .div_ceil(u128::from(max))
        .max(1) as usize;
    let mut out = "█".repeat(eighths / 8);
    if !eighths.is_multiple_of(8) {
        out.push(EIGHTHS[eighths % 8 - 1]);
    }
    out
}

/// A bucket's start: minutes, or seconds for buckets under a minute.
fn bucket_time(ms: i64, bucket_ms: i64) -> String {
    let full = crate::model::format::format_timestamp_ms(ms);
    let len = if bucket_ms < 60_000 { 19 } else { 16 };
    full.chars().take(len).collect()
}

fn histogram_text(hist: &TimeHistogram, by_partition: bool, width: usize) -> String {
    use std::fmt::Write as _;
    let mut out = String::new();
    let stamp = |ms: i64| bucket_time(ms, hist.bucket_ms);
    let kind = match hist.timestamp_type.as_deref() {
        _ if hist.exact => "record timestamps (exact)".to_string(),
        Some("LogAppendTime") => "LogAppendTime (exact edges)".to_string(),
        Some(t) => format!("{t} (approximate)"),
        None => "timestamp type unknown (approximate)".to_string(),
    };
    let _ = writeln!(
        out,
        "Topic {} · {} → {} UTC · {} buckets · {kind}",
        hist.topic.bold(),
        stamp(hist.from_ms),
        stamp(hist.to_ms),
        duration_label(hist.bucket_ms)
    );
    if hist.no_timestamps {
        let _ = writeln!(out, "{}", time_histogram::NOTE_NO_TIMESTAMPS.yellow());
        return out.trim_end().to_string();
    }
    if by_partition && hist.buckets.iter().any(|b| !b.by_partition.is_empty()) {
        out.push_str(&partition_table(hist, &stamp));
        out.push('\n');
    } else {
        let max = hist.buckets.iter().map(|b| b.count).max().unwrap_or(0);
        let time_width = stamp(hist.from_ms).chars().count();
        let _ = writeln!(out, "{:time_width$}  {:>9}", "TIME (UTC)", "COUNT");
        for b in &hist.buckets {
            let line = match b.state {
                BucketState::Trimmed | BucketState::Unknown => {
                    bucket_note(b.state).dimmed().to_string()
                }
                BucketState::InProgress => {
                    format!("{} ({})", bar(b.count, max, width), bucket_note(b.state))
                        .dimmed()
                        .to_string()
                }
                BucketState::Complete => bar(b.count, max, width),
            };
            let _ = writeln!(
                out,
                "{:time_width$}  {:>9}  {line}",
                stamp(b.start_ms),
                thousands(b.count)
            );
        }
    }
    let after = format!("after range: {}", thousands(hist.after));
    let mut summary = format!(
        "before range: {} · {} · {} partition{}",
        thousands(hist.before),
        if hist.after > 0 {
            after.yellow().to_string()
        } else {
            after
        },
        hist.partitions.len(),
        if hist.partitions.len() == 1 { "" } else { "s" }
    );
    if !hist.unavailable.is_empty() {
        let _ = write!(
            summary,
            " ({} unavailable: {})",
            hist.unavailable.len(),
            join_ids(&hist.unavailable)
        );
    }
    if hist.removed_by_compaction > 0 {
        let _ = write!(
            summary,
            " · removed by compaction: {}",
            thousands(hist.removed_by_compaction)
        );
    }
    if hist.unknown > 0 {
        let what = if hist.exact {
            "no timestamp"
        } else {
            "unknown bucket"
        };
        let _ = write!(summary, " · {what}: {}", thousands(hist.unknown));
    }
    if let Some(offsets) = hist.offset_total {
        let _ = write!(summary, " · offsets read: {}", thousands(offsets));
    }
    let _ = writeln!(out, "{summary}");
    if let Some(error) = &hist.lookup_error {
        let _ = writeln!(
            out,
            "{}",
            format!("a boundary lookup failed: {error}").yellow()
        );
    }
    if let Some(partial) = &hist.partial {
        let _ = writeln!(out, "{}", partial.yellow());
    }
    match hist.peak() {
        Some(peak) => {
            let gaps: Vec<String> = hist
                .gaps(1)
                .iter()
                .map(|(a, b)| format!("{}–{}", stamp(*a), stamp(*b)))
                .collect();
            let gaps = if gaps.is_empty() {
                String::new()
            } else {
                format!(" · gaps: {}", gaps.join(", "))
            };
            let _ = writeln!(
                out,
                "peak {} ({}){gaps}",
                stamp(peak.start_ms),
                thousands(peak.count)
            );
        }
        None => {
            let _ = writeln!(out, "no records in the range");
        }
    }
    for note in hist.notes() {
        let _ = writeln!(out, "{}", note.dimmed());
    }
    if let Some(i) = hist
        .peak()
        .and_then(|p| hist.buckets.iter().position(|b| b.start_ms == p.start_ms))
    {
        let _ = writeln!(
            out,
            "read the peak: ferrika consume {} --from-timestamp {} --to-timestamp {}",
            hist.topic,
            hist.buckets[i].start_ms,
            hist.bucket_end(i) - 1
        );
    }
    out.trim_end().to_string()
}

/// What a bucket's state means, for the chart and the partition table.
fn bucket_note(state: BucketState) -> &'static str {
    match state {
        BucketState::Trimmed => "deleted (before the oldest retained record)",
        BucketState::Unknown => "? (lookup failed or timed out)",
        BucketState::InProgress => "in progress",
        BucketState::Complete => "",
    }
}

/// Per-partition counts: the 15 busiest partitions and the rest as "others".
fn partition_table(hist: &TimeHistogram, stamp: &dyn Fn(i64) -> String) -> String {
    const SHOWN: usize = 15;
    let mut order: Vec<usize> = (0..hist.partitions.len()).collect();
    let totals: Vec<u64> = order
        .iter()
        .map(|&c| hist.buckets.iter().map(|b| b.by_partition[c]).sum())
        .collect();
    let others = order.len() > SHOWN + 1;
    if others {
        order.sort_by_key(|&c| (std::cmp::Reverse(totals[c]), hist.partitions[c]));
        order.truncate(SHOWN);
        order.sort_by_key(|&c| hist.partitions[c]);
    }
    let mut headers = vec!["TIME (UTC)".to_string(), "COUNT".to_string()];
    headers.extend(order.iter().map(|&c| format!("P{}", hist.partitions[c])));
    if others {
        headers.push("OTHERS".to_string());
    }
    // Deleted, unknown and in-progress buckets are marked as in the chart.
    let noted = hist
        .buckets
        .iter()
        .any(|b| b.state != BucketState::Complete);
    if noted {
        headers.push("NOTE".to_string());
    }
    let mut table = output::table(headers);
    let right = |text: String| Cell::new(text).set_alignment(CellAlignment::Right);
    for b in &hist.buckets {
        let mut row = vec![Cell::new(stamp(b.start_ms)), right(thousands(b.count))];
        let shown: u64 = order.iter().map(|&c| b.by_partition[c]).sum();
        row.extend(order.iter().map(|&c| right(thousands(b.by_partition[c]))));
        if others {
            row.push(right(thousands(b.count - shown)));
        }
        if noted {
            row.push(Cell::new(bucket_note(b.state)));
        }
        table.add_row(row);
    }
    table.to_string()
}

/// `90000` as `1m30s`, `3600000` as `1h`.
fn duration_label(ms: i64) -> String {
    crate::model::format::format_duration_ms(ms, "")
}

async fn handle_analyze(
    config: &crate::kafka::config::KafkaClientConfig,
    global: &GlobalOpts,
    topic: &str,
) -> crate::error::KafkaResult<()> {
    let interactive = global.format == OutputFormat::Table;
    let analysis = crate::ops::topic_stats::analyze_topic(config, topic, 5_000, |a, done| {
        if interactive && !done {
            eprint!("\rRead {} records…", a.records);
        }
        true
    })
    .await?;
    if interactive {
        eprint!("\r");
    }
    if global.format == OutputFormat::Json {
        output::print_json(&analysis)?;
        return Ok(());
    }
    let bytes = crate::model::format::format_bytes;
    let sizes = |s: &crate::ops::topic_stats::SizeStats| match (s.min, s.max, s.average()) {
        (Some(min), Some(max), Some(avg)) => format!(
            "min {} · avg {} · max {} · total {}",
            bytes(min),
            bytes(avg.round() as u64),
            bytes(max),
            bytes(s.total)
        ),
        _ => "-".to_string(),
    };
    let time = |ms: Option<i64>| {
        ms.map_or_else(
            || "-".to_string(),
            crate::model::format::format_timestamp_ms,
        )
    };
    let mut table = output::untitled_table();
    let mut row = |k: &str, v: String| {
        table.add_row(vec![Cell::new(k).fg(Color::Cyan), Cell::new(v)]);
    };
    row("Records", analysis.records.to_string());
    let per_partition: Vec<String> = analysis
        .partitions
        .iter()
        .map(|(p, n)| format!("{p}: {n}"))
        .collect();
    row("Per partition", per_partition.join(", "));
    row("Value sizes", sizes(&analysis.values));
    row("Key sizes", sizes(&analysis.keys));
    row("Null keys", analysis.null_keys.to_string());
    row(
        "Distinct keys",
        if analysis.distinct_keys_capped {
            format!("{}+", analysis.distinct_keys)
        } else {
            analysis.distinct_keys.to_string()
        },
    );
    row("Tombstones", analysis.tombstones.to_string());
    row("With headers", analysis.with_headers.to_string());
    row(
        "Time span (UTC)",
        format!(
            "{} → {}",
            time(analysis.first_timestamp_ms),
            time(analysis.last_timestamp_ms)
        ),
    );
    let histogram: Vec<String> = analysis
        .value_size_buckets
        .iter()
        .filter(|(_, n)| *n > 0)
        .map(|(bound, n)| match bound {
            Some(b) => format!("≤{}: {n}", bytes(*b)),
            None => format!(">10.0 MB: {n}"),
        })
        .collect();
    row("Value size histogram", histogram.join(", "));
    println!("{} {}", "Topic".bold(), topic.cyan().bold());
    println!("{table}");
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn election_with_refused_partitions_is_recorded_as_failed() {
        use super::election_result;
        use crate::kafka::wire::ElectionOutcome;
        let mut outcome = ElectionOutcome {
            elected: vec![("t".to_string(), 0)],
            ..Default::default()
        };
        assert!(election_result(&Ok(outcome.clone())).is_ok());
        outcome.failed.push((
            "t".to_string(),
            1,
            "PREFERRED_LEADER_NOT_AVAILABLE".to_string(),
        ));
        let err = election_result(&Ok(outcome)).unwrap_err();
        assert!(
            err.contains("1 could not move") && err.contains("t-1"),
            "{err}"
        );
    }

    use super::*;

    #[test]
    fn config_changes_with_sensitive_keys_are_partly_undoable() {
        use crate::model::history::ConfigValue;
        let state = |sensitive| State::TopicConfig {
            topic: "t".into(),
            keys: std::collections::BTreeMap::from([(
                "k".to_string(),
                ConfigValue {
                    own: None,
                    sensitive,
                },
            )]),
        };
        let plain = config_change(Ok((vec![state(false)], vec![state(false)])));
        assert_eq!(plain.reversibility, Reversibility::Reversible);
        let hidden = config_change(Ok((vec![state(true)], vec![state(true)])));
        assert_eq!(hidden.reversibility, Reversibility::Partial);
        assert!(hidden.limits[0].contains('k'));
        let created = created_topic("t");
        assert_eq!(created.reversibility, Reversibility::Conditional);
    }
    use crate::model::cluster::PartitionDetail;

    #[test]
    fn created_topic_shape_comes_from_its_partitions() {
        let mut topic = crate::model::cluster::ClusterSnapshot::demo().topics[0].clone();
        topic.partitions = Vec::new();
        assert_eq!(shape_of(&topic), None, "metadata not there yet");
        topic.partitions = (0..6)
            .map(|id| PartitionDetail {
                partition_id: id,
                leader: 1,
                replicas: vec![1, 2, 3],
                isr: vec![1, 2, 3],
                high_watermark: 0,
                low_watermark: 0,
            })
            .collect();
        assert_eq!(shape_of(&topic), Some((6, 3)));
    }

    #[test]
    fn test_config_changes_parses_set_and_delete() {
        let changes = config_changes(
            &["retention.ms = 3600000".to_string(), "x=a=b".to_string()],
            &["cleanup.policy".to_string()],
        )
        .unwrap();
        assert_eq!(
            changes,
            vec![
                ConfigChange::Set("retention.ms".into(), "3600000".into()),
                ConfigChange::Set("x".into(), "a=b".into()),
                ConfigChange::Delete("cleanup.policy".into()),
            ]
        );
        assert!(config_changes(&[], &[]).unwrap().is_empty());
    }

    #[test]
    fn test_config_changes_rejects_bad_input() {
        assert!(config_changes(&["retention.ms".to_string()], &[]).is_err());
        assert!(config_changes(&["=1".to_string()], &[]).is_err());
        assert!(config_changes(&[], &[" ".to_string()]).is_err());
        assert!(config_changes(&["a=1".to_string()], &["a".to_string()]).is_err());
    }

    #[test]
    fn test_parse_topic_configs_valid() {
        let configs = vec![
            "cleanup.policy=compact".to_string(),
            "retention.ms=86400000".to_string(),
        ];
        let map = parse_topic_configs(&configs).unwrap();
        assert_eq!(map.get("cleanup.policy").unwrap(), "compact");
        assert_eq!(map.get("retention.ms").unwrap(), "86400000");
    }

    #[test]
    fn test_parse_topic_configs_invalid() {
        let configs = vec!["invalid_config_without_equals".to_string()];
        assert!(parse_topic_configs(&configs).is_err());
    }

    #[test]
    fn test_render_topics_table() {
        let topics = vec![
            TopicSummary {
                name: "order-events".into(),
                partition_count: 3,
                replication_factor: 2,
                total_messages: 100,
                cleanup_policy: "delete".into(),
                is_internal: false,
                size_bytes: None,
                msg_rate: None,
                partitions: vec![],
            },
            TopicSummary {
                name: "__consumer_offsets".into(),
                partition_count: 50,
                replication_factor: 3,
                total_messages: 500,
                cleanup_policy: "compact".into(),
                is_internal: true,
                size_bytes: None,
                msg_rate: None,
                partitions: vec![],
            },
        ];

        let table_output = render_topics_table(&topics, true);
        assert!(table_output.contains("TOPIC NAME"));
        assert!(table_output.contains("order-events"));
        assert!(table_output.contains("__consumer_offsets"));
        assert!(table_output.contains("50"));
    }

    #[test]
    fn test_render_describe_table() {
        let topic = TopicSummary {
            name: "test-topic".into(),
            partition_count: 2,
            replication_factor: 2,
            total_messages: 0,
            cleanup_policy: "delete".into(),
            is_internal: false,
            size_bytes: None,
            msg_rate: None,
            partitions: vec![
                PartitionDetail {
                    partition_id: 0,
                    leader: 1,
                    replicas: vec![1, 2],
                    isr: vec![1, 2],
                    high_watermark: 100,
                    low_watermark: 0,
                },
                PartitionDetail {
                    partition_id: 1,
                    leader: 2,
                    replicas: vec![2, 1],
                    isr: vec![2],
                    high_watermark: 80,
                    low_watermark: 0,
                },
            ],
        };

        let table_output = render_describe_table(&topic, true);
        assert!(table_output.contains("PARTITION"));
        assert!(table_output.contains("LEADER"));
        assert!(table_output.contains("[1, 2]"));
        assert!(table_output.contains("[2]"));
    }

    #[test]
    fn histogram_arguments_parse() {
        use clap::Parser;

        #[derive(Parser, Debug)]
        struct TestCli {
            #[command(subcommand)]
            cmd: TopicsSubcommands,
        }
        let cli = TestCli::try_parse_from([
            "test",
            "histogram",
            "orders",
            "--since",
            "6h",
            "--bucket",
            "10m",
            "-p",
            "0",
            "-p",
            "1",
        ])
        .unwrap();
        let TopicsSubcommands::Histogram {
            topic,
            partitions,
            since,
            bucket,
            buckets,
            ..
        } = cli.cmd
        else {
            panic!("not histogram");
        };
        assert_eq!(topic, "orders");
        assert_eq!(partitions, vec![0, 1]);
        assert_eq!(since, Some(std::time::Duration::from_secs(6 * 3600)));
        assert_eq!(bucket, std::time::Duration::from_secs(600));
        assert_eq!(buckets, 48);
        let exact =
            TestCli::try_parse_from(["test", "histogram", "orders", "--exact", "--limit", "10"])
                .unwrap();
        assert!(matches!(
            exact.cmd,
            TopicsSubcommands::Histogram {
                exact: true,
                limit: 10,
                ..
            }
        ));

        let alias = TestCli::try_parse_from(["test", "timeline", "orders", "--all"]).unwrap();
        assert!(matches!(
            alias.cmd,
            TopicsSubcommands::Histogram { all: true, .. }
        ));
        let from = TestCli::try_parse_from([
            "test",
            "histogram",
            "orders",
            "--from",
            "2024-11-02 09:00",
            "--to",
            "1730541600000",
        ])
        .unwrap();
        assert!(matches!(
            from.cmd,
            TopicsSubcommands::Histogram {
                from: Some(1_730_538_000_000),
                to: Some(1_730_541_600_000),
                ..
            }
        ));
        for bad in [
            &["test", "histogram", "orders", "--bucket", "0m"][..],
            &["test", "histogram", "orders", "--since", "1h", "--all"][..],
            &["test", "histogram", "orders", "--to", "1h"][..],
            &["test", "histogram", "orders", "--from", "yesterday"][..],
            &["test", "histogram", "orders", "--buckets", "501"][..],
            &["test", "histogram", "orders", "--limit", "5"][..],
        ] {
            assert!(TestCli::try_parse_from(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn bars_use_eighth_blocks() {
        assert_eq!(bar(0, 10, 10), "");
        assert_eq!(bar(10, 10, 10), "██████████");
        assert_eq!(bar(5, 10, 10), "█████");
        // The smallest non-zero count still shows.
        assert_eq!(bar(1, 1_000_000, 10), "▏");
        assert_eq!(bar(3, 16, 2), "▍");
        assert_eq!(thousands(0), "0");
        assert_eq!(thousands(1_234_567), "1,234,567");
        assert_eq!(thousands(999), "999");
        assert_eq!(duration_label(1_800_000), "30m");
        assert_eq!(duration_label(90_000), "1m30s");
        assert_eq!(duration_label(86_400_000), "1d");
    }

    #[test]
    fn histogram_text_shows_peak_gaps_and_how_to_read_it() {
        use crate::kafka::consumer::Lookup;
        let slots = vec![vec![
            Some(Lookup::At(0)),
            Some(Lookup::At(4)),
            Some(Lookup::At(4)),
            Some(Lookup::End),
        ]];
        let from = 1_730_538_000_000;
        let b = [from, from + 1_800_000, from + 3_600_000, from + 5_400_000];
        let hist = time_histogram::assemble("orders", &b, &[0], &slots, &[(0, 10)], i64::MAX);
        let text = histogram_text(&hist, false, 20);
        assert!(text.contains("2024-11-02 09:00"), "{text}");
        assert!(text.contains("30m buckets"), "{text}");
        assert!(text.contains("peak 2024-11-02 10:00 (6)"), "{text}");
        assert!(
            text.contains("gaps: 2024-11-02 09:30–2024-11-02 10:00"),
            "{text}"
        );
        assert!(text.contains(&format!(
            "--from-timestamp {} --to-timestamp {}",
            from + 3_600_000,
            from + 5_400_000 - 1
        )));
        let table = histogram_text(&hist, true, 20);
        assert!(table.contains("P0"), "{table}");
        assert!(!table.contains("NOTE"), "{table}");
    }

    #[test]
    fn partition_table_marks_deleted_buckets() {
        use crate::kafka::consumer::Lookup;
        let slots = vec![vec![
            Some(Lookup::At(5)),
            Some(Lookup::At(5)),
            Some(Lookup::End),
        ]];
        let from = 1_730_538_000_000;
        let b = [from, from + 1_800_000, from + 3_600_000];
        let mut hist = time_histogram::assemble("orders", &b, &[0], &slots, &[(5, 10)], i64::MAX);
        hist.mark_trimmed(Some(from + 2_000_000));
        let table = histogram_text(&hist, true, 20);
        assert!(table.contains("NOTE"), "{table}");
        assert!(table.contains("deleted"), "{table}");
    }

    #[test]
    fn test_clap_topics_subcommands_parsing() {
        use clap::Parser;

        #[derive(Parser, Debug)]
        struct TestCli {
            #[command(subcommand)]
            cmd: TopicsSubcommands,
        }

        let cli = TestCli::try_parse_from(["test", "list", "-a"]).unwrap();
        assert_eq!(
            cli.cmd,
            TopicsSubcommands::List {
                include_internal: true
            }
        );

        let cli = TestCli::try_parse_from(["test", "describe", "orders"]).unwrap();
        assert_eq!(
            cli.cmd,
            TopicsSubcommands::Describe {
                topic: "orders".to_string()
            }
        );

        let cli = TestCli::try_parse_from([
            "test",
            "create",
            "orders",
            "-p",
            "3",
            "-r",
            "2",
            "-c",
            "cleanup.policy=compact",
        ])
        .unwrap();
        assert_eq!(
            cli.cmd,
            TopicsSubcommands::Create {
                name: "orders".to_string(),
                partitions: Some(3),
                replication: Some(2),
                configs: vec!["cleanup.policy=compact".to_string()],
            }
        );

        // Omitted counts are left to the broker; zero or less is refused.
        let cli = TestCli::try_parse_from(["test", "create", "orders"]).unwrap();
        assert_eq!(
            cli.cmd,
            TopicsSubcommands::Create {
                name: "orders".to_string(),
                partitions: None,
                replication: None,
                configs: Vec::new(),
            }
        );
        assert!(TestCli::try_parse_from(["test", "create", "orders", "-p", "0"]).is_err());
        assert!(TestCli::try_parse_from(["test", "create", "orders", "-r", "-1"]).is_err());

        let cli = TestCli::try_parse_from(["test", "delete", "orders", "-y"]).unwrap();
        assert_eq!(
            cli.cmd,
            TopicsSubcommands::Delete {
                name: "orders".to_string(),
                yes: true
            }
        );
    }
}
