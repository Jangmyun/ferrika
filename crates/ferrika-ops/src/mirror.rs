use rdkafka::consumer::CommitMode;
use rdkafka::Offset;
use std::collections::{BTreeSet, HashMap};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use ferrika_core::error::{KafkaError, KafkaResult};
use ferrika_core::kafka::config::{AutoOffsetReset, KafkaClientConfig};
use ferrika_core::kafka::consumer::{ConsumedMessage, KafkaConsumerClient};
use ferrika_core::kafka::filter::MessageFilter;
use ferrika_core::kafka::producer::{
    DeliveryOutcome, KafkaProducerClient, OwnedProducerRecord, TaggedDeliveryPipeline,
};

/// Unacknowledged records allowed in flight while mirroring. librdkafka
/// sends a batch after `linger.ms` (5 ms), so a window of N records caps the
/// rate near N / 5 ms: 1000 held 500k small records at 160k/s (3.2 s), and
/// 10 000 (the restore window) finished them in 1.5 s.
const MIRROR_MAX_IN_FLIGHT: usize = 10_000;

/// Failed deliveries after which the mirror stops instead of reading on.
pub const DEFAULT_MAX_FAILURES: u64 = 100;

/// Configuration for a topic-to-topic or cross-cluster mirror operation.
#[derive(Debug, Clone)]
pub struct MirrorConfig {
    /// Source topic name
    pub src_topic: String,
    /// Destination topic name
    pub dst_topic: String,
    /// Whether to start consuming from the earliest available offset
    pub from_beginning: bool,
    /// Optional single partition to mirror
    pub partition: Option<i32>,
    /// Maximum number of messages to forward (delivered or failed) before stopping
    pub max_messages: Option<usize>,
    /// Failed deliveries after which the pipeline stops early
    pub max_failures: u64,
    /// Target throughput limit in messages per second
    pub rate_limit: Option<f64>,
    /// Real-time message filter (grep, key, headers, timestamps)
    pub filter: MessageFilter,
}

impl MirrorConfig {
    /// Creates a new default mirror configuration between two topics.
    pub fn new(src_topic: impl Into<String>, dst_topic: impl Into<String>) -> Self {
        Self {
            src_topic: src_topic.into(),
            dst_topic: dst_topic.into(),
            from_beginning: false,
            partition: None,
            max_messages: None,
            max_failures: DEFAULT_MAX_FAILURES,
            rate_limit: None,
            filter: MessageFilter::new(),
        }
    }

    /// Sets how many failed deliveries stop the pipeline (at least 1).
    pub fn with_max_failures(mut self, max_failures: u64) -> Self {
        self.max_failures = max_failures.max(1);
        self
    }

    /// Sets whether to consume from earliest offset.
    pub fn with_from_beginning(mut self, from_beginning: bool) -> Self {
        self.from_beginning = from_beginning;
        self
    }

    /// Sets target partition.
    pub fn with_partition(mut self, partition: Option<i32>) -> Self {
        self.partition = partition;
        self
    }

    /// Sets max messages limit.
    pub fn with_max_messages(mut self, max: Option<usize>) -> Self {
        self.max_messages = max;
        self
    }

    /// Sets rate limit.
    pub fn with_rate_limit(mut self, rate: Option<f64>) -> Self {
        self.rate_limit = rate;
        self
    }

    /// Sets message filter.
    pub fn with_filter(mut self, filter: MessageFilter) -> Self {
        self.filter = filter;
        self
    }
}

/// Aggregated real-time metrics for a mirroring pipeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MirrorStats {
    /// Total messages pulled from the source topic
    pub read_count: u64,
    /// Total messages successfully mirrored to the destination topic
    pub mirrored_count: u64,
    /// Messages skipped due to filter non-match
    pub filtered_count: u64,
    /// Messages that could not be delivered to the destination topic
    #[serde(default)]
    pub failed_count: u64,
    /// The pipeline stopped early because too many deliveries failed
    #[serde(default)]
    pub stopped_on_failures: bool,
    /// Total payload bytes mirrored
    pub total_bytes: u64,
    /// Pipeline elapsed time in seconds
    pub elapsed_secs: f64,
    /// Average throughput in events per second
    pub average_eps: f64,
    /// Average throughput in Megabytes per second
    pub average_mbps: f64,
    #[serde(skip)]
    start_time: Option<Instant>,
}

impl Default for MirrorStats {
    fn default() -> Self {
        Self {
            read_count: 0,
            mirrored_count: 0,
            filtered_count: 0,
            failed_count: 0,
            stopped_on_failures: false,
            total_bytes: 0,
            elapsed_secs: 0.0,
            average_eps: 0.0,
            average_mbps: 0.0,
            start_time: Some(Instant::now()),
        }
    }
}

impl MirrorStats {
    /// Starts the internal timer.
    pub fn new() -> Self {
        Self {
            start_time: Some(Instant::now()),
            ..Default::default()
        }
    }

    /// Records that a message was read from the source topic.
    pub fn record_read(&mut self) {
        self.read_count += 1;
    }

    /// Records that a message was skipped by the filter.
    pub fn record_filtered(&mut self) {
        self.filtered_count += 1;
    }

    /// Records a successfully mirrored message and its byte payload size.
    pub fn record_mirrored(&mut self, bytes: usize) {
        self.mirrored_count += 1;
        self.total_bytes += bytes as u64;
    }

    /// Records a message that could not be delivered.
    pub fn record_failed(&mut self, error: &KafkaError) {
        self.failed_count += 1;
        // Log the first few; a broken destination would otherwise flood the log.
        if self.failed_count <= 5 {
            tracing::error!("Failed to mirror message: {error}");
        }
    }

    /// Records the delivery report of a pipelined message.
    pub fn record_outcome(&mut self, outcome: DeliveryOutcome) {
        match outcome.result {
            Ok(_) => self.record_mirrored(outcome.payload_bytes),
            Err(e) => self.record_failed(&e),
        }
    }

    /// Error to exit with when any delivery failed, so scripts see a non-zero status.
    pub fn failure(&self) -> Option<KafkaError> {
        if self.failed_count == 0 {
            return None;
        }
        let stopped = if self.stopped_on_failures {
            "; stopped early after reaching the failure limit"
        } else {
            ""
        };
        Some(KafkaError::Check(format!(
            "{} of {} mirrored messages failed delivery{stopped}",
            self.failed_count,
            self.failed_count + self.mirrored_count
        )))
    }

    /// Updates elapsed time and derived rates (EPS, MB/s).
    pub fn update_rates(&mut self) {
        if let Some(start) = self.start_time {
            let secs = start.elapsed().as_secs_f64();
            self.elapsed_secs = secs;
            if secs > 0.0001 {
                self.average_eps = self.mirrored_count as f64 / secs;
                self.average_mbps = (self.total_bytes as f64 / 1_048_576.0) / secs;
            }
        }
    }
}

/// One event of the mirror loop.
enum MirrorStep {
    Stop,
    Delivered(SourcePosition, DeliveryOutcome),
    Received(KafkaResult<ConsumedMessage>),
}

/// Partition and offset of the source record a copy was made from.
type SourcePosition = (i32, i64);

/// Decides which source offsets a mirror with a consumer group may commit:
/// only those whose copy was delivered (or that the filter skipped), and
/// never past a record whose copy failed. The mirror thereby copies each
/// record at least once across restarts; records after a failure may be
/// copied again.
#[derive(Debug, Default)]
struct CommitTracker {
    partitions: HashMap<i32, PartitionProgress>,
}

#[derive(Debug, Default)]
struct PartitionProgress {
    /// Records handed to the producer whose delivery is not reported yet.
    pending: BTreeSet<i64>,
    /// Highest offset delivered or skipped.
    highest_done: Option<i64>,
    /// Offsets whose copy failed and was not delivered since (a rebalance
    /// can deliver the record again); nothing from the lowest on is committed.
    failed: BTreeSet<i64>,
    /// Last offset returned for storing.
    stored: Option<i64>,
}

impl CommitTracker {
    /// A copy of the record at `offset` was handed to the producer.
    fn sent(&mut self, partition: i32, offset: i64) {
        self.partitions
            .entry(partition)
            .or_default()
            .pending
            .insert(offset);
    }

    /// The record at `offset` is finished: its copy was delivered (`done`),
    /// it failed, or the filter skipped it (`done` without [`Self::sent`]).
    /// Returns the offset of the last record that may now be marked
    /// processed, when that moved forward.
    fn finished(&mut self, partition: i32, offset: i64, done: bool) -> Option<i64> {
        let progress = self.partitions.entry(partition).or_default();
        progress.pending.remove(&offset);
        if done {
            progress.highest_done = progress.highest_done.max(Some(offset));
            progress.failed.remove(&offset);
        } else {
            progress.failed.insert(offset);
        }
        // Records arrive in offset order per partition, so everything below
        // the lowest pending offset has been seen and finished.
        let mut safe = match progress.pending.first() {
            Some(&lowest) => Some(lowest - 1),
            None => progress.highest_done,
        };
        if let Some(&failed) = progress.failed.first() {
            safe = safe.map(|s| s.min(failed - 1));
        }
        match safe {
            Some(s) if s >= 0 && progress.stored.is_none_or(|stored| s > stored) => {
                progress.stored = Some(s);
                Some(s)
            }
            _ => None,
        }
    }
}

/// Builds the destination record for a source message, keeping it byte-for-byte:
/// a missing key or payload stays missing (a null payload is a tombstone on
/// compacted topics), and the source timestamp and headers are carried over.
pub fn mirror_record(msg: &ConsumedMessage, dst_topic: &str) -> OwnedProducerRecord {
    OwnedProducerRecord {
        topic: dst_topic.to_string(),
        key: msg.key.clone(),
        payload: msg.payload.clone(),
        partition: None,
        timestamp: msg.timestamp_ms,
        headers: msg.headers.clone(),
    }
}

/// Whether mirroring `src_topic` to `dst_topic` would read its own output:
/// the same topic name on a cluster both sides reach (a shared broker address).
/// The same name on different clusters is a normal cross-cluster copy.
pub fn is_self_mirror(
    src_bootstrap: &str,
    src_topic: &str,
    dst_bootstrap: &str,
    dst_topic: &str,
) -> bool {
    src_topic == dst_topic
        && ferrika_governance::policy::shares_broker(src_bootstrap, dst_bootstrap)
}

/// Executes an asynchronous streaming pipe from source topic to destination topic.
///
/// Delivery failures do not end the call with an error; they are counted in
/// [`MirrorStats::failed_count`] (and stop the pipeline once
/// [`MirrorConfig::max_failures`] is reached) so the caller can report the
/// totals before failing.
pub async fn run_mirror_pipeline<F>(
    src_config: &KafkaClientConfig,
    dst_config: &KafkaClientConfig,
    mirror_config: MirrorConfig,
    mut progress_callback: F,
) -> KafkaResult<MirrorStats>
where
    F: FnMut(&MirrorStats),
{
    // Configure consumer
    let mut consumer_cfg = src_config.clone();
    if mirror_config.from_beginning {
        consumer_cfg = consumer_cfg.with_auto_offset_reset(AutoOffsetReset::Earliest);
    }
    ferrika_core::kafka::consumer::limit_prefetch(&mut consumer_cfg);
    // With a configured group the mirror resumes from its committed offsets.
    // Without one it reads directly (no throwaway group is created) from the
    // beginning or the current end.
    let use_group = consumer_cfg.group_id.is_some();
    if use_group {
        // Offsets are stored by hand once the copy is delivered (see
        // `CommitTracker`) and committed by the periodic auto-commit and at
        // the end; storing them on receipt would commit records in flight.
        consumer_cfg = consumer_cfg.with_property("enable.auto.offset.store", "false");
    }
    let consumer = if use_group {
        KafkaConsumerClient::new(&consumer_cfg)?
    } else {
        KafkaConsumerClient::scanner(&consumer_cfg)?
    };
    if let Some(partition) = mirror_config.partition {
        // An unknown partition would otherwise only log errors and wait forever.
        let existing = consumer.topic_partitions(&mirror_config.src_topic, None)?;
        ferrika_core::kafka::filter::ensure_partition(
            &mirror_config.src_topic,
            &existing,
            partition,
        )?;
    }
    let producer = KafkaProducerClient::new(dst_config)?;
    let start = if mirror_config.from_beginning {
        Offset::Beginning
    } else {
        Offset::End
    };

    match (mirror_config.partition, use_group) {
        (Some(partition), true) => {
            consumer.assign_partitions(&[(&mirror_config.src_topic, partition)])?
        }
        (Some(partition), false) => {
            consumer.assign_with_offsets(&[(&mirror_config.src_topic, partition, start)])?
        }
        (None, true) => consumer.subscribe(&[&mirror_config.src_topic])?,
        (None, false) => {
            consumer.assign_topic(&mirror_config.src_topic, start)?;
        }
    }

    let mut stats = MirrorStats::new();
    let interval = mirror_config
        .rate_limit
        .filter(|&r| r > 0.0)
        .map(|r| Duration::from_secs_f64(1.0 / r));

    let mut last_tick = Instant::now();
    let mut pipeline = TaggedDeliveryPipeline::<SourcePosition>::new(MIRROR_MAX_IN_FLIGHT);
    let mut tracker = CommitTracker::default();
    let mut forwarded: u64 = 0;
    // With a group, marks a finished record and stores the offset it frees.
    let store = |tracker: &mut CommitTracker, (partition, offset): SourcePosition, done: bool| {
        if !use_group {
            return;
        }
        if let Some(last) = tracker.finished(partition, offset, done) {
            if let Err(e) =
                consumer.store_processed_offset(&mirror_config.src_topic, partition, last)
            {
                // Usually a partition this member lost in a rebalance; the
                // new owner reads from the last commit, so nothing is skipped.
                tracing::debug!("Could not store mirror offset {partition}@{last}: {e}");
            }
        }
    };

    loop {
        if let Some(max) = mirror_config.max_messages {
            if forwarded >= max as u64 {
                break;
            }
        }
        if stats.failed_count >= mirror_config.max_failures {
            stats.stopped_on_failures = true;
            break;
        }

        let loop_start = Instant::now();

        let step = tokio::select! {
            _ = tokio::signal::ctrl_c() => MirrorStep::Stop,
            Some((source, outcome)) = pipeline.next_outcome(), if pipeline.in_flight() > 0 => {
                MirrorStep::Delivered(source, outcome)
            }
            msg_res = consumer.recv() => MirrorStep::Received(msg_res),
        };

        match step {
            MirrorStep::Stop => break,
            MirrorStep::Delivered(source, outcome) => {
                store(&mut tracker, source, outcome.result.is_ok());
                stats.record_outcome(outcome);
            }
            MirrorStep::Received(Ok(msg)) => {
                stats.record_read();
                let source = (msg.partition, msg.offset);

                if !mirror_config.filter.matches(&msg) {
                    stats.record_filtered();
                    store(&mut tracker, source, true);
                    continue;
                }

                let record = mirror_record(&msg, &mirror_config.dst_topic);
                forwarded += 1;
                if let Some((done, outcome)) = pipeline.make_room().await {
                    store(&mut tracker, done, outcome.result.is_ok());
                    stats.record_outcome(outcome);
                }
                if use_group {
                    tracker.sent(source.0, source.1);
                }
                if let Err(e) = pipeline.push(&producer, &record, source).await {
                    store(&mut tracker, source, false);
                    stats.record_failed(&e);
                }

                // Rate throttling
                if let Some(target_interval) = interval {
                    let elapsed = loop_start.elapsed();
                    if target_interval > elapsed {
                        tokio::time::sleep(target_interval - elapsed).await;
                    }
                }
            }
            MirrorStep::Received(Err(e)) => {
                tracing::warn!("Consumer error in mirror pipeline: {e}");
            }
        }

        // Periodic progress callback
        if last_tick.elapsed() >= Duration::from_millis(500) {
            stats.update_rates();
            progress_callback(&stats);
            last_tick = Instant::now();
        }
    }

    // Wait for every record already handed to the producer so the counts are final.
    while let Some((source, outcome)) = pipeline.next_outcome().await {
        store(&mut tracker, source, outcome.result.is_ok());
        stats.record_outcome(outcome);
    }
    if use_group {
        if let Err(e) = consumer.commit_stored(CommitMode::Sync) {
            tracing::warn!("Could not commit the mirror's consumer group offsets: {e}");
        }
    }

    stats.update_rates();
    progress_callback(&stats);
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn self_mirror_is_same_topic_on_same_cluster() {
        assert!(is_self_mirror("localhost:9092", "t", "localhost:9092", "t"));
        assert!(is_self_mirror("a:9092,b:9092", "t", "b:9092", "t"));
        assert!(!is_self_mirror(
            "localhost:9092",
            "t",
            "localhost:9092",
            "u"
        ));
        assert!(
            !is_self_mirror("prod:9092", "t", "staging:9092", "t"),
            "same name on another cluster is a valid copy"
        );
    }

    #[test]
    fn test_mirror_config_defaults() {
        let config = MirrorConfig::new("topic-a", "topic-b")
            .with_from_beginning(true)
            .with_partition(Some(2))
            .with_max_messages(Some(500))
            .with_rate_limit(Some(100.0));

        assert_eq!(config.src_topic, "topic-a");
        assert_eq!(config.dst_topic, "topic-b");
        assert!(config.from_beginning);
        assert_eq!(config.partition, Some(2));
        assert_eq!(config.max_messages, Some(500));
        assert_eq!(config.rate_limit, Some(100.0));
    }

    fn source_message() -> ConsumedMessage {
        let headers = vec![
            ("dup".to_string(), b"1".to_vec()),
            ("bin".to_string(), vec![0xff, 0x00, b' ']),
            ("dup".to_string(), b"2".to_vec()),
        ];
        ConsumedMessage {
            topic: "src".into(),
            partition: 3,
            offset: 42,
            key: Some(vec![0xc3, 0x28]),
            payload: None,
            timestamp_ms: Some(1_726_900_000_000),
            headers,
        }
    }

    #[test]
    fn test_mirror_record_keeps_tombstone_key_timestamp_and_headers() {
        let msg = source_message();
        let record = mirror_record(&msg, "dst");
        assert_eq!(record.topic, "dst");
        assert_eq!(record.payload, None, "null payload must stay a tombstone");
        assert_eq!(record.key, Some(vec![0xc3, 0x28]));
        assert_eq!(record.timestamp, Some(1_726_900_000_000));
        assert_eq!(
            record.headers,
            [
                ("dup".to_string(), b"1".to_vec()),
                ("bin".to_string(), vec![0xff, 0x00, b' ']),
                ("dup".to_string(), b"2".to_vec()),
            ]
        );
        assert_eq!(record.partition, None);
    }

    #[test]
    fn test_mirror_record_keeps_empty_payload_distinct_from_null() {
        let mut msg = source_message();
        msg.payload = Some(Vec::new());
        msg.key = None;
        let record = mirror_record(&msg, "dst");
        assert_eq!(record.payload, Some(Vec::new()));
        assert_eq!(record.key, None);
    }

    #[test]
    fn test_mirror_stats_counts_failed_outcomes() {
        let mut stats = MirrorStats::new();
        stats.record_outcome(DeliveryOutcome {
            result: Err(KafkaError::DeliveryFailed {
                topic: "dst".into(),
                partition: None,
                reason: "boom".into(),
            }),
            latency: Duration::ZERO,
            payload_bytes: 5,
        });
        stats.record_outcome(DeliveryOutcome {
            result: Ok(ferrika_core::kafka::producer::DeliveryReceipt {
                topic: "dst".into(),
                partition: 0,
                offset: 7,
            }),
            latency: Duration::ZERO,
            payload_bytes: 5,
        });
        assert_eq!(stats.failed_count, 1);
        assert_eq!(stats.mirrored_count, 1);
        assert_eq!(stats.total_bytes, 5);
        let err = stats.failure().map(|e| e.to_string()).unwrap_or_default();
        assert_eq!(err, "1 of 2 mirrored messages failed delivery");
        assert!(MirrorStats::new().failure().is_none());
    }

    #[test]
    fn commit_tracker_waits_for_earlier_deliveries() {
        let mut t = CommitTracker::default();
        for offset in 0..3 {
            t.sent(0, offset);
        }
        // Out of order: 1 and 2 are delivered before 0.
        assert_eq!(t.finished(0, 2, true), None);
        assert_eq!(t.finished(0, 1, true), None);
        assert_eq!(t.finished(0, 0, true), Some(2));
        // Nothing new: nothing to store.
        assert_eq!(t.finished(0, 2, true), None);
    }

    #[test]
    fn commit_tracker_never_passes_a_failed_copy() {
        let mut t = CommitTracker::default();
        for offset in 10..14 {
            t.sent(3, offset);
        }
        assert_eq!(t.finished(3, 10, true), Some(10));
        assert_eq!(t.finished(3, 11, false), None);
        assert_eq!(t.finished(3, 12, true), None);
        assert_eq!(t.finished(3, 13, true), None);
        // Later records stay behind the failure too.
        t.sent(3, 14);
        assert_eq!(t.finished(3, 14, true), None);
    }

    #[test]
    fn commit_tracker_resumes_once_a_failed_record_is_delivered_again() {
        let mut t = CommitTracker::default();
        t.sent(0, 0);
        t.sent(0, 1);
        assert_eq!(t.finished(0, 0, false), None);
        assert_eq!(t.finished(0, 1, true), None);
        // After a rebalance the record is read and copied again.
        t.sent(0, 0);
        assert_eq!(t.finished(0, 0, true), Some(1));
    }

    #[test]
    fn commit_tracker_counts_skipped_records_and_keeps_partitions_apart() {
        let mut t = CommitTracker::default();
        t.sent(0, 5);
        // Partition 1 is not held back by partition 0.
        assert_eq!(t.finished(1, 7, true), Some(7));
        // A record the filter skipped (never sent) behind a pending one.
        assert_eq!(t.finished(0, 6, true), Some(4));
        assert_eq!(t.finished(0, 5, true), Some(6));
        // A gap (compaction, transaction markers) is not waited for.
        t.sent(0, 20);
        assert_eq!(t.finished(0, 20, true), Some(20));
    }

    #[test]
    fn test_max_failures_is_at_least_one() {
        let config = MirrorConfig::new("a", "b");
        assert_eq!(config.max_failures, DEFAULT_MAX_FAILURES);
        assert_eq!(config.with_max_failures(0).max_failures, 1);
    }

    #[test]
    fn test_mirror_stats_computations() {
        let mut stats = MirrorStats::new();
        stats.record_read();
        stats.record_read();
        stats.record_read();
        stats.record_filtered();
        stats.record_mirrored(1024);
        stats.record_mirrored(2048);

        assert_eq!(stats.read_count, 3);
        assert_eq!(stats.filtered_count, 1);
        assert_eq!(stats.mirrored_count, 2);
        assert_eq!(stats.total_bytes, 3072);

        // Simulate artificial elapsed time
        stats.elapsed_secs = 2.0;
        stats.average_eps = stats.mirrored_count as f64 / 2.0;
        stats.average_mbps = (stats.total_bytes as f64 / 1_048_576.0) / 2.0;

        assert_eq!(stats.average_eps, 1.0);
        assert!(stats.average_mbps > 0.0);
    }
}
