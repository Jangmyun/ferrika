use std::collections::{BTreeSet, HashMap};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use ferrika_core::error::{KafkaError, KafkaResult};
use ferrika_core::kafka::config::{AutoOffsetReset, KafkaClientConfig};
use ferrika_core::kafka::consumer::{CommitMode, StartOffset};
use ferrika_core::kafka::consumer::{ConsumedMessage, KafkaConsumerClient};
use ferrika_core::kafka::filter::MessageFilter;
use ferrika_core::kafka::producer::{
    DeliveryOutcome, KafkaProducerClient, OwnedProducerRecord, TaggedDeliveryPipeline,
};
use ferrika_core::model::policy::{CopyDownRule, PolicyAction};
use ferrika_governance::audit::AuditEntry;
use ferrika_governance::policy::{
    self, AuditMode, AuditSink, EffectivePolicy, ExportChannel, ExportRequest, WriteRequest,
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
    /// Start each partition at its first record at or after this epoch
    /// millisecond (overrides `from_beginning`; not with a consumer group)
    pub from_timestamp: Option<i64>,
    /// Stop once every partition is read up to the end it had when the
    /// pipeline started, instead of waiting for new records
    pub until_end: bool,
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
            from_timestamp: None,
            until_end: false,
        }
    }

    /// Sets the start timestamp (epoch milliseconds).
    pub fn with_from_timestamp(mut self, from_timestamp: Option<i64>) -> Self {
        self.from_timestamp = from_timestamp;
        self
    }

    /// Sets whether to stop at the end each partition had at the start.
    pub fn with_until_end(mut self, until_end: bool) -> Self {
        self.until_end = until_end;
        self
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
/// copied again. `dlq redrive --group` uses it the same way for the DLQ
/// records it sends one by one.
#[derive(Debug, Default)]
pub struct CommitTracker {
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
    pub fn sent(&mut self, partition: i32, offset: i64) {
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
    pub fn finished(&mut self, partition: i32, offset: i64, done: bool) -> Option<i64> {
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

/// Turns a source record into the record to write, or `None` to skip it.
/// The copy pipeline calls it for each record [`MirrorConfig::filter`]
/// kept, in offset order per partition.
pub trait RecordTransform {
    fn apply(
        &mut self,
        msg: &ConsumedMessage,
    ) -> impl std::future::Future<Output = Option<OwnedProducerRecord>>;
}

/// Copies every record unchanged ([`mirror_record`]) to `dst_topic`.
#[derive(Debug, Clone)]
pub(crate) struct Verbatim {
    pub dst_topic: String,
}

impl RecordTransform for Verbatim {
    fn apply(
        &mut self,
        msg: &ConsumedMessage,
    ) -> impl std::future::Future<Output = Option<OwnedProducerRecord>> {
        std::future::ready(Some(mirror_record(msg, &self.dst_topic)))
    }
}

/// Where a copy pipeline sends its records.
pub enum CopyDestination<'a> {
    /// Produce them to this cluster.
    Cluster(&'a KafkaClientConfig),
    /// Hand them to the callback and write nothing (a dry run). No consumer
    /// group is joined and no offset is committed.
    Preview(&'a mut dyn FnMut(&OwnedProducerRecord)),
}

/// The open side of a [`CopyDestination`].
enum Output<'a> {
    Producer(Box<KafkaProducerClient>),
    Preview(&'a mut dyn FnMut(&OwnedProducerRecord)),
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
    progress_callback: F,
) -> KafkaResult<MirrorStats>
where
    F: FnMut(&MirrorStats),
{
    let mut verbatim = Verbatim {
        dst_topic: mirror_config.dst_topic.clone(),
    };
    run_copy_pipeline(
        src_config,
        CopyDestination::Cluster(dst_config),
        mirror_config,
        &mut verbatim,
        progress_callback,
    )
    .await
}

/// [`run_mirror_pipeline`] with each kept record passed through `transform`
/// on its way, to a cluster or to a preview callback.
///
/// With a consumer group (in `src_config`) an offset is committed only after
/// the record's copy was acknowledged (or the record was skipped), so the
/// copy is at least once across restarts. With [`MirrorConfig::until_end`]
/// the pipeline stops once each partition is read to the end it had at the
/// start; records past that end are left for a later run.
pub async fn run_copy_pipeline<T, F>(
    src_config: &KafkaClientConfig,
    destination: CopyDestination<'_>,
    mirror_config: MirrorConfig,
    transform: &mut T,
    mut progress_callback: F,
) -> KafkaResult<MirrorStats>
where
    T: RecordTransform,
    F: FnMut(&MirrorStats),
{
    let topic = mirror_config.src_topic.as_str();
    // Configure consumer
    let mut consumer_cfg = src_config.clone();
    if matches!(destination, CopyDestination::Preview(_)) {
        // A preview must not join (and rebalance) a group or commit for it.
        consumer_cfg.group_id = None;
    }
    if mirror_config.from_beginning {
        consumer_cfg = consumer_cfg.with_auto_offset_reset(AutoOffsetReset::Earliest);
    }
    ferrika_core::kafka::consumer::limit_prefetch(&mut consumer_cfg);
    // With a configured group the mirror resumes from its committed offsets.
    // Without one it reads directly (no throwaway group is created) from the
    // beginning or the current end.
    let use_group = consumer_cfg.group_id.is_some();
    if use_group && mirror_config.from_timestamp.is_some() {
        return Err(KafkaError::config(
            "A start timestamp cannot be combined with a consumer group, which resumes from its committed offsets",
        ));
    }
    if use_group {
        // Offsets are stored by hand once the copy is delivered (see
        // `CommitTracker`) and committed by the periodic auto-commit and at
        // the end; storing them on receipt would commit records in flight.
        consumer_cfg = consumer_cfg.with_property("enable.auto.offset.store", "false");
    }
    if mirror_config.until_end {
        // A partition that ends in transaction markers never delivers its
        // last offset; the end-of-partition notice finishes it instead.
        consumer_cfg = consumer_cfg.with_property("enable.partition.eof", "true");
    }
    let consumer = if use_group {
        KafkaConsumerClient::new(&consumer_cfg)?
    } else {
        KafkaConsumerClient::scanner(&consumer_cfg)?
    };
    // The partitions read, when the start or the end needs them listed.
    let needs_partitions = mirror_config.partition.is_some()
        || mirror_config.from_timestamp.is_some()
        || mirror_config.until_end;
    let partitions: Vec<i32> = if needs_partitions {
        let existing = consumer.topic_partitions(topic, None)?;
        match mirror_config.partition {
            Some(partition) => {
                // An unknown partition would otherwise only log errors and wait forever.
                ferrika_core::kafka::filter::ensure_partition(topic, &existing, partition)?;
                vec![partition]
            }
            None => existing,
        }
    } else {
        Vec::new()
    };
    let keys: Vec<(&str, i32)> = partitions.iter().map(|&p| (topic, p)).collect();
    let mut output = match destination {
        CopyDestination::Cluster(dst_config) => {
            Output::Producer(Box::new(KafkaProducerClient::new(dst_config)?))
        }
        CopyDestination::Preview(sink) => Output::Preview(sink),
    };
    let start = if mirror_config.from_beginning {
        StartOffset::Beginning
    } else {
        StartOffset::End
    };
    let timestamp_starts = match mirror_config.from_timestamp {
        Some(ts) => {
            let lookup = consumer.offsets_for_timestamp(&keys, ts, None)?;
            Some(ferrika_core::kafka::consumer::timestamp_start_offsets(
                topic,
                &partitions,
                &lookup,
            ))
        }
        None => None,
    };

    // With `until_end`, the offset each unfinished partition stops before.
    let mut ends: HashMap<i32, i64> = HashMap::new();
    if mirror_config.until_end {
        let marks = consumer.fetch_watermarks_batch(&keys, None)?;
        for &p in &partitions {
            let Some(&(low, high)) = marks.get(&(topic.to_string(), p)) else {
                return Err(KafkaError::Consumer(format!(
                    "Could not read the end offset of partition {p} of '{topic}'"
                )));
            };
            let first = match (use_group, &timestamp_starts) {
                (false, Some(starts)) => match starts.iter().find(|(sp, _)| *sp == p) {
                    Some((_, StartOffset::At(o))) => *o,
                    _ => high,
                },
                (false, None) if !mirror_config.from_beginning => high,
                // From the beginning, or from the group's committed offset
                // (the end-of-partition notice finishes it when that is
                // already the end).
                _ => low,
            };
            if first < high {
                ends.insert(p, high);
            }
        }
    }

    match (mirror_config.partition, use_group, &timestamp_starts) {
        (_, false, Some(starts)) => {
            let assignments: Vec<(&str, i32, StartOffset)> =
                starts.iter().map(|&(p, o)| (topic, p, o)).collect();
            consumer.assign_with_offsets(&assignments)?
        }
        (Some(partition), true, _) => consumer.assign_partitions(&[(topic, partition)])?,
        (Some(partition), false, None) => {
            consumer.assign_with_offsets(&[(topic, partition, start)])?
        }
        (None, true, _) => consumer.subscribe(&[topic])?,
        (None, false, None) => {
            consumer.assign_topic(topic, start)?;
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
            if let Err(e) = consumer.store_processed_offset(topic, partition, last) {
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
        if mirror_config.until_end && ends.is_empty() {
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
                if mirror_config.until_end && !within_end(&mut ends, msg.partition, msg.offset) {
                    // Past the end captured at the start: left for a later
                    // run (with a group its offset is not stored).
                    continue;
                }
                stats.record_read();
                let source = (msg.partition, msg.offset);

                if !mirror_config.filter.matches(&msg) {
                    stats.record_filtered();
                    store(&mut tracker, source, true);
                    continue;
                }

                let Some(record) = transform.apply(&msg).await else {
                    stats.record_filtered();
                    store(&mut tracker, source, true);
                    continue;
                };
                forwarded += 1;
                match &mut output {
                    Output::Producer(producer) => {
                        if let Some((done, outcome)) = pipeline.make_room().await {
                            store(&mut tracker, done, outcome.result.is_ok());
                            stats.record_outcome(outcome);
                        }
                        if use_group {
                            tracker.sent(source.0, source.1);
                        }
                        if let Err(e) = pipeline.push(producer, &record, source).await {
                            store(&mut tracker, source, false);
                            stats.record_failed(&e);
                        }
                    }
                    Output::Preview(sink) => {
                        stats.record_mirrored(record.payload.as_ref().map_or(0, Vec::len));
                        sink(&record);
                    }
                }

                // Rate throttling
                if let Some(target_interval) = interval {
                    let elapsed = loop_start.elapsed();
                    if target_interval > elapsed {
                        tokio::time::sleep(target_interval - elapsed).await;
                    }
                }
            }
            MirrorStep::Received(Err(KafkaError::Driver(e))) if e.partition_eof().is_some() => {
                if let Some(partition) = e.partition_eof() {
                    ends.remove(&partition);
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

/// For a run bounded by the ends captured at its start (`ends`, by
/// partition): whether the record at `offset` is before its partition's end.
/// Removes the partition once its last record (or anything past it) arrives.
fn within_end(ends: &mut HashMap<i32, i64>, partition: i32, offset: i64) -> bool {
    let Some(&end) = ends.get(&partition) else {
        // Finished, or empty when the run started.
        return false;
    };
    if offset + 1 >= end {
        ends.remove(&partition);
    }
    offset < end
}

/// One side of a record copy, as the front end resolved it.
#[derive(Debug, Clone, Copy)]
pub struct CopyCluster<'a> {
    /// The profile name the policy's read-only rules and the audit log name
    /// the cluster by; empty when there is none (entries then name its
    /// bootstrap servers).
    pub profile: &'a str,
    /// The resolved connection: its bootstrap servers and whether the
    /// profile is marked read-only.
    pub config: &'a KafkaClientConfig,
}

impl CopyCluster<'_> {
    /// The profile name, else the bootstrap servers.
    fn audit_name(&self) -> &str {
        if self.profile.is_empty() {
            &self.config.bootstrap_servers
        } else {
            self.profile
        }
    }
}

/// A record copy to authorise (`mirror`, a transform that writes): the
/// plain inputs [`authorize_copy`] checks, so every front end applies the
/// same rules as the CLI.
#[derive(Debug, Clone, Copy)]
pub struct CopyCheck<'a> {
    /// Who asks, for the audit log: `cli`, `gui`, `mcp`.
    pub source: &'a str,
    /// The copy as the audit log names it, e.g. `mirror`.
    pub label: &'a str,
    pub src_topic: &'a str,
    pub dst_topic: &'a str,
    pub src: CopyCluster<'a>,
    pub dst: CopyCluster<'a>,
    /// The destination was named by its bootstrap servers rather than a
    /// profile: the export entry then names it by them.
    pub dst_named_by_brokers: bool,
    /// Whether to run the write gate for the destination here. `false` when
    /// the destination is the cluster the caller's own write gate already
    /// checked and recorded (the CLI without cluster options).
    pub check_destination: bool,
    /// Whether every record is PII-masked before anything else reads it,
    /// so enforced masking does not refuse the copy.
    pub masks_first: bool,
    /// Appended to the enforced-masking refusal: how to make the copy
    /// acceptable (may be empty).
    pub masking_hint: &'a str,
    /// Appended to the read-only destination refusal: how to turn read-only
    /// off (may be empty).
    pub read_only_hint: &'a str,
    /// How the write gate satisfies auditing.
    pub audit_mode: AuditMode,
}

/// What [`authorize_copy`] allowed and recorded.
#[derive(Debug, Clone, Default)]
pub struct CopyAuthorized {
    /// The name the destination's write request entry used (its profile, or
    /// its brokers without one), when one was recorded: pair a result entry
    /// with it after the copy.
    pub write_requested: Option<String>,
    /// The copy goes to another cluster, so it passed the export gate.
    pub cross_cluster: bool,
    /// The export's result entry to complete with the record count (outcome
    /// `records: N`) after the copy; `None` unless exports are audited.
    pub export_result: Option<AuditEntry>,
}

/// The policy checks for copying records from one cluster to another, in
/// order:
///
/// 1. Enforced PII masking refuses the copy unless it masks first.
/// 2. A destination whose brokers are not all among the source's is another
///    cluster (`destination_within_source`). A copy from a higher
///    environment tier to a lower one (or to a cluster in no environment)
///    is refused under `copy_down: forbid`, and under `copy_down: masked`
///    unless it masks first. Then the export gate decides it for the
///    source's brokers (`export: forbidden` refuses, `audited` records it).
/// 3. With [`CopyCheck::check_destination`], the write gate for the
///    destination as a `produce`: a read-only rule, `forbid: produce` or a
///    read-only destination profile refuses it.
///
/// Refusals are recorded as `policy.blocked` in `sink`. `fetch` lists the
/// brokers a cluster reports, for `read_only_brokers` and `export_clusters`
/// (a tunnel or an IP address must not dodge them); it is called only when
/// the policy has such rules.
pub fn authorize_copy(
    policy: &EffectivePolicy,
    sink: &impl AuditSink,
    check: &CopyCheck<'_>,
    fetch: impl Fn(&KafkaClientConfig) -> Result<Vec<String>, String>,
) -> Result<CopyAuthorized, String> {
    let label = check.label;
    let target = format!("{} -> {}", check.src_topic, check.dst_topic);
    let blocked = |named: &str, reason: &str, target: &str| {
        let entry =
            policy::blocked_entry(check.source, named, &format!("{label} {target}"), reason);
        if let Err(e) = sink.record(&entry) {
            tracing::warn!(%e, "Could not write the audit log");
        }
    };
    if policy.masking_enforced() && !check.masks_first {
        let reason = format!(
            "Refused by {}: PII masking is enforced, and `{label}` would copy unmasked records to another cluster.{}",
            policy.label(),
            check.masking_hint
        );
        blocked(check.src.audit_name(), &reason, &target);
        return Err(reason);
    }

    let mut authorized = CopyAuthorized::default();
    let src = check.src.config;
    let dst = check.dst.config;
    if !policy::destination_within_source(&src.bootstrap_servers, &dst.bootstrap_servers) {
        let destination = if check.dst.profile.is_empty() || check.dst_named_by_brokers {
            dst.bootstrap_servers.as_str()
        } else {
            check.dst.profile
        };
        let export_target = format!("{target} (to {destination})");
        if let Err(reason) = check_copy_down(policy, check, &fetch) {
            blocked(check.src.audit_name(), &reason, &export_target);
            return Err(reason);
        }
        let src_brokers = match policy::export_brokers_with(policy, src, &fetch) {
            Ok(brokers) => brokers,
            Err(reason) => {
                blocked(check.src.audit_name(), &reason, &export_target);
                return Err(reason);
            }
        };
        let request = ExportRequest {
            source: check.source,
            channel: ExportChannel::Cluster,
            label,
            target: &export_target,
            records: None,
            profile: check.src.profile,
            bootstrap: &src_brokers,
            actor: None,
        };
        policy::authorize_export_with(policy, sink, &request)
            .map_err(|refusal| refusal.to_string())?;
        authorized.cross_cluster = true;
        authorized.export_result = policy::export_result_entry(policy, &request, 0);
    }

    if !check.check_destination {
        return Ok(authorized);
    }
    let profile = check.dst.profile;
    let brokers = match policy::brokers_for_with(dst, policy.write_needs_broker_metadata(), &fetch)
    {
        Ok(brokers) => brokers,
        Err(reason) => {
            blocked(check.dst.audit_name(), &reason, &target);
            return Err(reason);
        }
    };
    let request = WriteRequest {
        source: check.source,
        action: PolicyAction::Produce,
        label,
        target: &target,
        profile,
        bootstrap: &brokers,
        actor: None,
        op: None,
    };
    policy::authorize_write_with(policy, sink, &request, check.audit_mode, || {
        if !dst.read_only {
            return Ok(());
        }
        let name = if profile.is_empty() {
            "the destination profile"
        } else {
            profile
        };
        Err(format!(
            "Destination profile '{name}' is read-only, so `{label}` is refused.{}",
            check.read_only_hint
        ))
    })
    .map_err(|refusal| refusal.to_string())?;
    authorized.write_requested = Some(if profile.is_empty() {
        brokers
    } else {
        profile.to_string()
    });
    Ok(authorized)
}

/// The policy's `copy_down` rule for a copy between two clusters: the
/// refusal when it goes from a higher environment tier to a lower one and
/// the rule forbids that, or asks for masking the copy does not do first.
/// The environments are matched against the brokers each cluster reports
/// when the policy tells them apart by broker address; a lookup that fails
/// refuses the copy.
fn check_copy_down(
    policy: &EffectivePolicy,
    check: &CopyCheck<'_>,
    fetch: impl Fn(&KafkaClientConfig) -> Result<Vec<String>, String>,
) -> Result<(), String> {
    let needed = policy.copy_down_needs_broker_metadata();
    let src_brokers = policy::brokers_for_with(check.src.config, needed, &fetch)?;
    let dst_brokers = policy::brokers_for_with(check.dst.config, needed, &fetch)?;
    let Some(down) = policy.copy_down(
        check.src.profile,
        &src_brokers,
        check.dst.profile,
        &dst_brokers,
    ) else {
        return Ok(());
    };
    let label = check.label;
    match down.rule {
        CopyDownRule::Allow => Ok(()),
        CopyDownRule::Masked if check.masks_first => Ok(()),
        CopyDownRule::Masked => Err(format!(
            "Refused by {}: `{label}` would copy unmasked records {}, a lower environment, \
             and the policy requires them masked (copy_down: masked).{}",
            policy.label(),
            down.describe(),
            check.masking_hint
        )),
        CopyDownRule::Forbid => Err(format!(
            "Refused by {}: `{label}` would copy records {}, a lower environment, \
             which the policy forbids (copy_down: forbid).",
            policy.label(),
            down.describe()
        )),
    }
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

    #[test]
    fn within_end_stops_each_partition_at_its_captured_end() {
        let mut ends = HashMap::from([(0, 3), (1, 1)]);
        assert!(within_end(&mut ends, 0, 1));
        assert!(
            within_end(&mut ends, 0, 2),
            "the last record is still copied"
        );
        assert!(!ends.contains_key(&0), "partition 0 is finished");
        assert!(!within_end(&mut ends, 0, 3), "past the end: left for later");
        // A compacted partition can jump over its last offset.
        assert!(!within_end(&mut ends, 1, 5));
        assert!(ends.is_empty());
        assert!(
            !within_end(&mut ends, 7, 0),
            "never tracked: empty at the start"
        );
    }

    #[test]
    fn mirror_config_bounds_default_off() {
        let config = MirrorConfig::new("a", "b");
        assert_eq!(config.from_timestamp, None);
        assert!(!config.until_end);
        let config = config
            .with_from_timestamp(Some(1_700_000_000_000))
            .with_until_end(true);
        assert_eq!(config.from_timestamp, Some(1_700_000_000_000));
        assert!(config.until_end);
    }

    /// Collects audit entries.
    #[derive(Default)]
    struct FakeLog {
        entries: std::cell::RefCell<Vec<AuditEntry>>,
    }

    impl AuditSink for FakeLog {
        fn record(&self, entry: &AuditEntry) -> std::io::Result<()> {
            self.entries.borrow_mut().push(entry.clone());
            Ok(())
        }

        fn probe(&self) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn copy_check<'a>(src: CopyCluster<'a>, dst: CopyCluster<'a>) -> CopyCheck<'a> {
        CopyCheck {
            source: "gui",
            label: "Copying records",
            src_topic: "orders",
            dst_topic: "orders-copy",
            src,
            dst,
            dst_named_by_brokers: false,
            check_destination: true,
            masks_first: false,
            masking_hint: " Mask first.",
            read_only_hint: "",
            audit_mode: AuditMode::RecordRequest,
        }
    }

    fn no_brokers(_: &KafkaClientConfig) -> Result<Vec<String>, String> {
        Err("no cluster in this test".to_string())
    }

    #[test]
    fn a_copy_within_a_writable_cluster_is_requested() {
        let dev = KafkaClientConfig::new("localhost:9092");
        let side = CopyCluster {
            profile: "dev",
            config: &dev,
        };
        let log = FakeLog::default();
        let none = ferrika_governance::test_policy::none();
        let authorized = authorize_copy(&none, &log, &copy_check(side, side), no_brokers).unwrap();
        assert_eq!(authorized.write_requested.as_deref(), Some("dev"));
        assert!(!authorized.cross_cluster);
        let entries = log.entries.borrow();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].action, "Copying records");
        assert_eq!(entries[0].source, "gui");

        // Without the destination check, nothing is recorded.
        let log = FakeLog::default();
        let check = CopyCheck {
            check_destination: false,
            ..copy_check(side, side)
        };
        let authorized = authorize_copy(&none, &log, &check, no_brokers).unwrap();
        assert_eq!(authorized.write_requested, None);
        assert!(log.entries.borrow().is_empty());
    }

    #[test]
    fn enforced_masking_refuses_unless_the_copy_masks_first() {
        let dev = KafkaClientConfig::new("localhost:9092");
        let side = CopyCluster {
            profile: "",
            config: &dev,
        };
        let policy = ferrika_governance::test_policy::masking();
        let log = FakeLog::default();
        let reason =
            authorize_copy(&policy, &log, &copy_check(side, side), no_brokers).unwrap_err();
        assert!(reason.contains("PII masking is enforced"), "{reason}");
        assert!(reason.ends_with(" Mask first."), "{reason}");
        let entries = log.entries.borrow();
        assert_eq!(entries[0].action, "policy.blocked");
        // Without a profile the entry names the brokers.
        assert_eq!(entries[0].profile, "localhost:9092");

        let check = CopyCheck {
            masks_first: true,
            ..copy_check(side, side)
        };
        assert!(authorize_copy(&policy, &FakeLog::default(), &check, no_brokers).is_ok());
    }

    #[test]
    fn another_cluster_passes_the_export_gate() {
        let src = KafkaClientConfig::new("kafka.prod:9092");
        let dst = KafkaClientConfig::new("kafka.dev:9092");
        let check = copy_check(
            CopyCluster {
                profile: "prod",
                config: &src,
            },
            CopyCluster {
                profile: "dev",
                config: &dst,
            },
        );
        let forbidden = ferrika_governance::test_policy::export_forbidden();
        let log = FakeLog::default();
        assert!(authorize_copy(&forbidden, &log, &check, no_brokers).is_err());
        assert_eq!(log.entries.borrow()[0].action, "policy.blocked");

        let audited = ferrika_governance::test_policy::export_audited();
        let log = FakeLog::default();
        let authorized = authorize_copy(&audited, &log, &check, no_brokers).unwrap();
        assert!(authorized.cross_cluster);
        let result = authorized.export_result.unwrap();
        assert_eq!(result.action, "record.export");
        assert!(result.target.ends_with("(to dev)"), "{}", result.target);
        // Named by brokers, the destination shows them.
        let by_brokers = CopyCheck {
            dst_named_by_brokers: true,
            ..check
        };
        let authorized =
            authorize_copy(&audited, &FakeLog::default(), &by_brokers, no_brokers).unwrap();
        let target = authorized.export_result.unwrap().target;
        assert!(target.ends_with("(to kafka.dev:9092)"), "{target}");
    }

    #[test]
    fn copy_down_needs_masking_or_is_forbidden() {
        let prod = KafkaClientConfig::new("kafka1.prod.corp:9092");
        let dev = KafkaClientConfig::new("localhost:9092");
        let down = copy_check(
            CopyCluster {
                profile: "prod-eu",
                config: &prod,
            },
            CopyCluster {
                profile: "dev",
                config: &dev,
            },
        );
        let reported = |c: &KafkaClientConfig| Ok(vec![c.bootstrap_servers.clone()]);

        let masked = ferrika_governance::test_policy::environments(CopyDownRule::Masked);
        let log = FakeLog::default();
        let reason = authorize_copy(&masked, &log, &down, reported).unwrap_err();
        assert!(
            reason.contains("from 'prod' (tier 3) to 'dev' (tier 1)"),
            "{reason}"
        );
        assert!(reason.contains("copy_down: masked"), "{reason}");
        assert!(reason.ends_with(" Mask first."), "{reason}");
        let entries = log.entries.borrow();
        assert_eq!(entries[0].action, "policy.blocked");
        assert_eq!(entries[0].profile, "prod-eu");
        assert!(
            entries[0].target.ends_with("(to dev)"),
            "{}",
            entries[0].target
        );
        drop(entries);
        // Masking first makes it acceptable; the destination's request
        // entry names its environment.
        let log = FakeLog::default();
        let masks = CopyCheck {
            masks_first: true,
            ..down
        };
        assert!(authorize_copy(&masked, &log, &masks, reported).is_ok());
        let entries = log.entries.borrow();
        assert_eq!(entries[0].outcome, "requested (environment dev)");
        drop(entries);
        // Up is not down.
        let up = CopyCheck {
            src: down.dst,
            dst: down.src,
            ..down
        };
        let up_forbid = ferrika_governance::test_policy::environments(CopyDownRule::Forbid);
        // (`prod` forbids no `produce`, so only `copy_down` could refuse.)
        assert!(authorize_copy(&up_forbid, &FakeLog::default(), &up, reported).is_ok());

        let forbid = ferrika_governance::test_policy::environments(CopyDownRule::Forbid);
        let reason = authorize_copy(&forbid, &FakeLog::default(), &masks, reported).unwrap_err();
        assert!(reason.contains("copy_down: forbid"), "{reason}");

        // The brokers decide, not the profile name: a "dev" profile that
        // reports prod's brokers is prod.
        let unknown = KafkaClientConfig::new("10.9.9.9:9092");
        let renamed = copy_check(
            CopyCluster {
                profile: "dev-alias",
                config: &dev,
            },
            CopyCluster {
                profile: "",
                config: &unknown,
            },
        );
        let prod_brokers = |c: &KafkaClientConfig| {
            Ok(if c.bootstrap_servers == "localhost:9092" {
                vec!["kafka1.prod.corp:9092".to_string()]
            } else {
                Vec::new()
            })
        };
        let reason =
            authorize_copy(&forbid, &FakeLog::default(), &renamed, prod_brokers).unwrap_err();
        assert!(
            reason.contains("to a cluster in no environment"),
            "{reason}"
        );
        // The brokers cannot be read: refused, not let through.
        assert!(authorize_copy(&forbid, &FakeLog::default(), &renamed, no_brokers).is_err());

        // Within one cluster nothing goes down.
        let side = CopyCluster {
            profile: "prod-eu",
            config: &prod,
        };
        assert!(authorize_copy(
            &forbid,
            &FakeLog::default(),
            &copy_check(side, side),
            reported
        )
        .is_ok());
    }

    #[test]
    fn a_read_only_destination_profile_is_refused() {
        let src = KafkaClientConfig::new("localhost:9092");
        let mut dst = KafkaClientConfig::new("localhost:9092");
        dst.read_only = true;
        let check = CopyCheck {
            read_only_hint: " Turn it off first.",
            ..copy_check(
                CopyCluster {
                    profile: "dev",
                    config: &src,
                },
                CopyCluster {
                    profile: "locked",
                    config: &dst,
                },
            )
        };
        let none = ferrika_governance::test_policy::none();
        let reason = authorize_copy(&none, &FakeLog::default(), &check, no_brokers).unwrap_err();
        assert_eq!(
            reason,
            "Destination profile 'locked' is read-only, so `Copying records` is refused. Turn it off first."
        );
    }
}
