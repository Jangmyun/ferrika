use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::RwLock;

use ferrika_core::error::{KafkaError, KafkaResult};
use ferrika_core::kafka::admin::KafkaAdminClient;
use ferrika_core::kafka::config::KafkaClientConfig;
use ferrika_core::kafka::consumer::KafkaConsumerClient;
use ferrika_core::kafka::metrics::calculate_partition_lag;
use ferrika_core::kafka::wire;

/// Metric for a single partition's consumer group lag.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionLagMetric {
    pub group: String,
    pub topic: String,
    pub partition: i32,
    pub current_offset: i64,
    pub high_watermark: i64,
    pub low_watermark: i64,
    pub lag: i64,
}

/// Aggregated cluster metrics snapshot collected during a scrape.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ScrapedMetrics {
    pub scrape_duration_secs: f64,
    pub brokers_total: usize,
    pub brokers_online: usize,
    pub broker_statuses: Vec<(i32, bool)>,
    pub topic_count: usize,
    pub partition_count: usize,
    pub consumer_lags: Vec<PartitionLagMetric>,
    /// Age in seconds of the first unconsumed record, per lagging
    /// `(group, topic, partition)`.
    pub lag_seconds: Vec<(String, String, i32, f64)>,
}

/// At most this many distinct committed offsets are timed per partition
/// (one read round each), keeping scrapes short with many groups.
const MAX_TIME_LAG_ROUNDS: usize = 4;

/// Timestamps of the records at each `(topic, partition, offset)`; offsets
/// of the same partition are read in separate rounds.
async fn timestamps_at(
    config: &KafkaClientConfig,
    positions: &[(String, i32, i64)],
) -> std::collections::HashMap<(String, i32, i64), i64> {
    let mut by_partition: std::collections::BTreeMap<(String, i32), Vec<i64>> =
        std::collections::BTreeMap::new();
    for (t, p, o) in positions {
        let offsets = by_partition.entry((t.clone(), *p)).or_default();
        if !offsets.contains(o) {
            offsets.push(*o);
        }
    }
    let mut found = std::collections::HashMap::new();
    for round in 0..MAX_TIME_LAG_ROUNDS {
        let batch: Vec<(String, i32, i64)> = by_partition
            .iter()
            .filter_map(|((t, p), offsets)| offsets.get(round).map(|o| (t.clone(), *p, *o)))
            .collect();
        if batch.is_empty() {
            break;
        }
        let times = ferrika_core::kafka::consumer::record_timestamps(
            config,
            &batch,
            Duration::from_secs(3),
        )
        .await
        .unwrap_or_default();
        for (t, p, o) in batch {
            if let Some(ts) = times.get(&(t.clone(), p)) {
                found.insert((t, p, o), *ts);
            }
        }
    }
    found
}

/// Renders a `ScrapedMetrics` snapshot into Prometheus text format 0.0.4.
pub fn render_prometheus_metrics(metrics: &ScrapedMetrics) -> String {
    let mut buf = String::with_capacity(4096);

    // 1. Brokers Total
    buf.push_str("# HELP kafka_brokers_total Total number of known brokers in cluster.\n");
    buf.push_str("# TYPE kafka_brokers_total gauge\n");
    buf.push_str(&format!(
        "kafka_brokers_total {}\n\n",
        metrics.brokers_total
    ));

    // 2. Brokers Online
    buf.push_str("# HELP kafka_brokers_online Number of currently reachable brokers.\n");
    buf.push_str("# TYPE kafka_brokers_online gauge\n");
    buf.push_str(&format!(
        "kafka_brokers_online {}\n\n",
        metrics.brokers_online
    ));

    // 3. Per-Broker Online Status
    buf.push_str("# HELP kafka_broker_online Online status of a specific broker (1 for online, 0 for offline).\n");
    buf.push_str("# TYPE kafka_broker_online gauge\n");
    for (broker_id, is_online) in &metrics.broker_statuses {
        let val = if *is_online { 1 } else { 0 };
        buf.push_str(&format!(
            "kafka_broker_online{{broker_id=\"{broker_id}\"}} {val}\n"
        ));
    }
    buf.push('\n');

    // 4. Topics Total
    buf.push_str("# HELP kafka_topics_total Total number of topics in cluster.\n");
    buf.push_str("# TYPE kafka_topics_total gauge\n");
    buf.push_str(&format!("kafka_topics_total {}\n\n", metrics.topic_count));

    // 5. Partitions Total
    buf.push_str("# HELP kafka_partitions_total Total number of partitions across all topics.\n");
    buf.push_str("# TYPE kafka_partitions_total gauge\n");
    buf.push_str(&format!(
        "kafka_partitions_total {}\n\n",
        metrics.partition_count
    ));

    // 6. Consumer Group Lag
    buf.push_str(
        "# HELP kafka_consumergroup_lag Number of unconsumed messages for a group partition.\n",
    );
    buf.push_str("# TYPE kafka_consumergroup_lag gauge\n");
    for lag in &metrics.consumer_lags {
        buf.push_str(&format!(
            "kafka_consumergroup_lag{{group=\"{}\",topic=\"{}\",partition=\"{}\"}} {}\n",
            escape_label(&lag.group),
            escape_label(&lag.topic),
            lag.partition,
            lag.lag
        ));
    }
    buf.push('\n');

    // 6b. Consumer Group Time Lag
    buf.push_str("# HELP kafka_consumergroup_lag_seconds Age of the first unconsumed record of a group partition.\n");
    buf.push_str("# TYPE kafka_consumergroup_lag_seconds gauge\n");
    for (group, topic, partition, secs) in &metrics.lag_seconds {
        buf.push_str(&format!(
            "kafka_consumergroup_lag_seconds{{group=\"{}\",topic=\"{}\",partition=\"{}\"}} {:.3}\n",
            escape_label(group),
            escape_label(topic),
            partition,
            secs
        ));
    }
    buf.push('\n');

    // 7. Consumer Group Current Offset
    buf.push_str("# HELP kafka_consumergroup_current_offset Current committed offset of a consumer group partition.\n");
    buf.push_str("# TYPE kafka_consumergroup_current_offset gauge\n");
    for lag in &metrics.consumer_lags {
        buf.push_str(&format!(
            "kafka_consumergroup_current_offset{{group=\"{}\",topic=\"{}\",partition=\"{}\"}} {}\n",
            escape_label(&lag.group),
            escape_label(&lag.topic),
            lag.partition,
            lag.current_offset
        ));
    }
    buf.push('\n');

    // 8. Topic Partition High Watermark
    buf.push_str(
        "# HELP kafka_topic_partition_high_watermark High watermark offset of a topic partition.\n",
    );
    buf.push_str("# TYPE kafka_topic_partition_high_watermark gauge\n");
    // One sample per (topic, partition): every group repeats the same watermark,
    // and Prometheus rejects a scrape with duplicate label sets.
    let mut seen_partitions = std::collections::HashSet::new();
    for lag in &metrics.consumer_lags {
        if !seen_partitions.insert((lag.topic.as_str(), lag.partition)) {
            continue;
        }
        buf.push_str(&format!(
            "kafka_topic_partition_high_watermark{{topic=\"{}\",partition=\"{}\"}} {}\n",
            escape_label(&lag.topic),
            lag.partition,
            lag.high_watermark
        ));
    }
    buf.push('\n');

    // 9. Scrape Duration
    buf.push_str("# HELP ferrika_scrape_duration_seconds Total time in seconds taken to collect cluster metrics.\n");
    buf.push_str("# TYPE ferrika_scrape_duration_seconds gauge\n");
    buf.push_str(&format!(
        "ferrika_scrape_duration_seconds {:.6}\n",
        metrics.scrape_duration_secs
    ));

    buf
}

#[inline]
fn escape_label(val: &str) -> String {
    val.replace('\\', "\\\\")
        .replace('\"', "\\\"")
        .replace('\n', "\\n")
}

/// Collects live metrics from the Kafka cluster.
pub async fn scrape_cluster_metrics(
    admin: &KafkaAdminClient,
    config: &KafkaClientConfig,
) -> ScrapedMetrics {
    let start = Instant::now();
    let mut metrics = ScrapedMetrics::default();

    // 1. Brokers
    if let Ok(brokers) = admin.list_brokers(Some(Duration::from_secs(5))) {
        metrics.brokers_total = brokers.len();
        metrics.brokers_online = brokers.iter().filter(|b| b.is_online).count();
        metrics.broker_statuses = brokers.iter().map(|b| (b.id, b.is_online)).collect();
    }

    // 2. Topics & Partitions
    let summaries = admin
        .list_topic_summaries(Some(Duration::from_secs(5)))
        .unwrap_or_default();
    metrics.topic_count = summaries.len();
    metrics.partition_count = summaries.iter().map(|t| t.partition_count).sum();

    // 3. Consumer Groups & Lag
    let groups = admin
        .list_consumer_groups(Some(Duration::from_secs(5)))
        .unwrap_or_default();
    if !groups.is_empty() {
        // Watermarks are shared by all groups: one batched lookup per scrape.
        let partitions: Vec<(&str, i32)> = summaries
            .iter()
            .filter(|t| !t.is_internal)
            .flat_map(|t| {
                t.partitions
                    .iter()
                    .map(move |p| (t.name.as_str(), p.partition_id))
            })
            .collect();
        let watermarks = KafkaConsumerClient::lookup(config)
            .and_then(|c| c.fetch_watermarks_batch(&partitions, Some(Duration::from_secs(5))))
            .unwrap_or_default();
        // One OffsetFetch per coordinator for all groups; librdkafka needs a
        // client per group, so that path is only the fallback.
        let committed = match wire::fetch_group_offsets(config, &groups) {
            Ok(offsets) => offsets,
            Err(e) => {
                tracing::debug!(%e, "Batched offset lookup failed; using a client per group");
                committed_via_clients(config, &groups, &partitions)
            }
        };
        for group_id in &groups {
            if let Some(offsets) = committed.get(group_id) {
                metrics.consumer_lags.extend(group_lag_metrics(
                    group_id,
                    offsets,
                    &summaries,
                    &watermarks,
                ));
            }
        }
    }

    // 4. Time lag of lagging partitions.
    let positions: Vec<(String, i32, i64)> = metrics
        .consumer_lags
        .iter()
        .filter(|l| l.lag > 0)
        .map(|l| (l.topic.clone(), l.partition, l.current_offset.max(-1)))
        .collect();
    if !positions.is_empty() {
        let now = jiff::Timestamp::now().as_millisecond();
        let times = timestamps_at(config, &positions).await;
        metrics.lag_seconds = metrics
            .consumer_lags
            .iter()
            .filter(|l| l.lag > 0)
            .filter_map(|l| {
                let ts = times.get(&(l.topic.clone(), l.partition, l.current_offset.max(-1)))?;
                Some((
                    l.group.clone(),
                    l.topic.clone(),
                    l.partition,
                    (now - ts).max(0) as f64 / 1000.0,
                ))
            })
            .collect();
    }

    metrics.scrape_duration_secs = start.elapsed().as_secs_f64();
    metrics
}

/// Committed offsets of `groups`, read with one librdkafka client per group.
fn committed_via_clients(
    config: &KafkaClientConfig,
    groups: &[String],
    partitions: &[(&str, i32)],
) -> wire::GroupOffsets {
    let mut queried = rdkafka::TopicPartitionList::new();
    for &(topic, partition) in partitions {
        queried.add_partition(topic, partition);
    }
    let mut result = wire::GroupOffsets::new();
    for group_id in groups {
        let mut consumer_cfg = config.clone();
        consumer_cfg.group_id = Some(group_id.clone());
        let Ok(consumer) = KafkaConsumerClient::new(&consumer_cfg) else {
            continue;
        };
        let Ok(committed) =
            consumer.fetch_committed_offsets_for(&queried, Some(Duration::from_secs(5)))
        else {
            continue;
        };
        let offsets = committed
            .elements()
            .iter()
            .filter_map(|e| match e.offset() {
                rdkafka::Offset::Offset(off) => Some(((e.topic().to_string(), e.partition()), off)),
                _ => None,
            })
            .collect();
        result.insert(group_id.clone(), offsets);
    }
    result
}

/// Lag of `group` on every partition of the topics it has committed offsets
/// on (other topics are skipped, or every group would report lag on every
/// topic). Partitions without a commit report offset -1.
fn group_lag_metrics(
    group: &str,
    committed: &std::collections::HashMap<(String, i32), i64>,
    summaries: &[ferrika_core::model::cluster::TopicSummary],
    watermarks: &std::collections::HashMap<(String, i32), (i64, i64)>,
) -> Vec<PartitionLagMetric> {
    let consumed: std::collections::HashSet<&str> =
        committed.keys().map(|(t, _)| t.as_str()).collect();
    let mut lags = Vec::new();
    for topic in summaries
        .iter()
        .filter(|t| !t.is_internal && consumed.contains(t.name.as_str()))
    {
        for p in &topic.partitions {
            let key = (topic.name.clone(), p.partition_id);
            let Some(&(low, high)) = watermarks.get(&key) else {
                continue;
            };
            let current_offset = committed.get(&key).copied().unwrap_or(-1);
            lags.push(PartitionLagMetric {
                group: group.to_string(),
                topic: topic.name.clone(),
                partition: p.partition_id,
                current_offset,
                high_watermark: high,
                low_watermark: low,
                lag: calculate_partition_lag(current_offset, high, low),
            });
        }
    }
    lags
}

/// How long a client gets to send its request line before the connection is dropped.
const REQUEST_READ_TIMEOUT: Duration = Duration::from_secs(5);

/// The path of an HTTP request target, without `?query` or `#fragment`.
fn request_path(target: &str) -> &str {
    target.split(['?', '#']).next().unwrap_or(target)
}

/// Spawns the lightweight Prometheus HTTP exporter server.
pub async fn start_exporter_server(
    admin: Arc<KafkaAdminClient>,
    config: KafkaClientConfig,
    host: &str,
    port: u16,
    scrape_interval: Duration,
) -> KafkaResult<()> {
    let bind_addr = format!("{host}:{port}");
    let listener = TcpListener::bind(&bind_addr)
        .await
        .map_err(|e| KafkaError::io(format!("cannot listen on {bind_addr}: {e}")))?;

    let initial_metrics = scrape_cluster_metrics(&admin, &config).await;
    let initial_text = render_prometheus_metrics(&initial_metrics);
    let cached_metrics = Arc::new(RwLock::new(initial_text));

    // Background scrape loop
    let background_cache = Arc::clone(&cached_metrics);
    let background_admin = Arc::clone(&admin);
    let background_config = config.clone();

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(scrape_interval);
        loop {
            interval.tick().await;
            let snapshot = scrape_cluster_metrics(&background_admin, &background_config).await;
            let rendered = render_prometheus_metrics(&snapshot);
            let mut writer = background_cache.write().await;
            *writer = rendered;
        }
    });

    // Accept loop for incoming HTTP requests
    loop {
        let (mut socket, _) = listener
            .accept()
            .await
            .map_err(|e| KafkaError::io(format!("cannot accept a connection: {e}")))?;
        let cache_ref = Arc::clone(&cached_metrics);

        tokio::spawn(async move {
            let mut buf = [0u8; 1024];
            let read = tokio::time::timeout(REQUEST_READ_TIMEOUT, socket.read(&mut buf)).await;
            if let Ok(Ok(n)) = read {
                if n == 0 {
                    return;
                }
                let request = String::from_utf8_lossy(&buf[..n]);
                let first_line = request.lines().next().unwrap_or("");
                let parts: Vec<&str> = first_line.split_whitespace().collect();

                let (status, content_type, body) = match parts.get(1).map(|t| request_path(t)) {
                    Some("/metrics") => {
                        let text = cache_ref.read().await.clone();
                        ("200 OK", "text/plain; version=0.0.4; charset=utf-8", text)
                    }
                    Some("/healthz") | Some("/livez") | Some("/readyz") => {
                        ("200 OK", "text/plain; charset=utf-8", "OK\n".to_string())
                    }
                    _ => (
                        "404 Not Found",
                        "text/plain; charset=utf-8",
                        "Not Found. Try /metrics or /healthz\n".to_string(),
                    ),
                };

                let response = format!(
                    "HTTP/1.1 {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    status,
                    content_type,
                    body.len(),
                    body
                );

                let _ = socket.write_all(response.as_bytes()).await;
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_group_lag_metrics_covers_consumed_topics_only() {
        let topics = ferrika_core::model::cluster::ClusterSnapshot::demo().topics;
        let consumed = &topics[0];
        let mut watermarks = std::collections::HashMap::new();
        for t in &topics {
            for p in &t.partitions {
                watermarks.insert((t.name.clone(), p.partition_id), (0, 100));
            }
        }
        let mut committed = std::collections::HashMap::new();
        committed.insert((consumed.name.clone(), 0), 40);
        let lags = group_lag_metrics("g", &committed, &topics, &watermarks);
        assert_eq!(lags.len(), consumed.partitions.len());
        assert!(lags.iter().all(|l| l.topic == consumed.name));
        let first = lags.iter().find(|l| l.partition == 0).unwrap();
        assert_eq!((first.current_offset, first.lag), (40, 60));
        // Uncommitted partitions of a consumed topic count from the start.
        assert!(lags
            .iter()
            .filter(|l| l.partition != 0)
            .all(|l| l.current_offset == -1));
    }

    #[test]
    fn test_render_prometheus_metrics_format() {
        let metrics = ScrapedMetrics {
            scrape_duration_secs: 0.042,
            brokers_total: 3,
            brokers_online: 2,
            broker_statuses: vec![(1, true), (2, true), (3, false)],
            topic_count: 5,
            partition_count: 12,
            consumer_lags: vec![
                PartitionLagMetric {
                    group: "order-service".to_string(),
                    topic: "orders".to_string(),
                    partition: 0,
                    current_offset: 1000,
                    high_watermark: 1050,
                    low_watermark: 100,
                    lag: 50,
                },
                PartitionLagMetric {
                    group: "order-service".to_string(),
                    topic: "orders".to_string(),
                    partition: 1,
                    current_offset: 2000,
                    high_watermark: 2000,
                    low_watermark: 200,
                    lag: 0,
                },
            ],
            lag_seconds: vec![("g".into(), "t".into(), 0, 1.5)],
        };

        let rendered = render_prometheus_metrics(&metrics);

        assert!(rendered.contains("# HELP kafka_brokers_total"));
        assert!(rendered.contains(
            "kafka_consumergroup_lag_seconds{group=\"g\",topic=\"t\",partition=\"0\"} 1.500"
        ));
        assert!(rendered.contains("kafka_brokers_total 3"));
        assert!(rendered.contains("kafka_brokers_online 2"));
        assert!(rendered.contains("kafka_broker_online{broker_id=\"1\"} 1"));
        assert!(rendered.contains("kafka_broker_online{broker_id=\"3\"} 0"));
        assert!(rendered.contains("kafka_topics_total 5"));
        assert!(rendered.contains("kafka_partitions_total 12"));

        assert!(rendered.contains(
            "kafka_consumergroup_lag{group=\"order-service\",topic=\"orders\",partition=\"0\"} 50"
        ));
        assert!(rendered.contains(
            "kafka_consumergroup_lag{group=\"order-service\",topic=\"orders\",partition=\"1\"} 0"
        ));
        assert!(rendered.contains(
            "kafka_consumergroup_current_offset{group=\"order-service\",topic=\"orders\",partition=\"0\"} 1000"
        ));
        assert!(rendered.contains(
            "kafka_topic_partition_high_watermark{topic=\"orders\",partition=\"0\"} 1050"
        ));
        assert!(rendered.contains("ferrika_scrape_duration_seconds 0.042000"));
    }

    #[test]
    fn high_watermark_is_emitted_once_per_partition_across_groups() {
        let lag = |group: &str| PartitionLagMetric {
            group: group.to_string(),
            topic: "orders".to_string(),
            partition: 0,
            current_offset: 1,
            high_watermark: 9,
            low_watermark: 0,
            lag: 8,
        };
        let metrics = ScrapedMetrics {
            scrape_duration_secs: 0.0,
            brokers_total: 1,
            brokers_online: 1,
            broker_statuses: vec![],
            topic_count: 1,
            partition_count: 1,
            consumer_lags: vec![lag("a"), lag("b")],
            lag_seconds: vec![],
        };
        let rendered = render_prometheus_metrics(&metrics);
        let samples = rendered
            .lines()
            .filter(|l| l.starts_with("kafka_topic_partition_high_watermark{"))
            .count();
        assert_eq!(samples, 1);
    }

    #[test]
    fn request_path_drops_query_and_fragment() {
        assert_eq!(request_path("/metrics?x=1"), "/metrics");
        assert_eq!(request_path("/metrics#a"), "/metrics");
        assert_eq!(request_path("/healthz"), "/healthz");
        assert_eq!(request_path("/metricsx"), "/metricsx");
    }

    #[test]
    fn test_escape_label() {
        assert_eq!(escape_label("normal_group"), "normal_group");
        assert_eq!(escape_label("quote\"test"), "quote\\\"test");
        assert_eq!(escape_label("slash\\test"), "slash\\\\test");
    }
}
