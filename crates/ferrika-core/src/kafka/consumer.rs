use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rdkafka::consumer::{
    BaseConsumer, CommitMode, Consumer, DefaultConsumerContext, StreamConsumer,
};
use rdkafka::message::{Headers, Message};
use rdkafka::{Offset, TopicPartitionList};
use serde::de::DeserializeOwned;

use crate::error::{KafkaError, KafkaResult};
use crate::kafka::config::KafkaClientConfig;
use crate::kafka::detached::DetachedDrop;

/// Deserialized/owned representation of a message consumed from a Kafka topic.
#[derive(Debug, Clone)]
pub struct ConsumedMessage {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub key: Option<Vec<u8>>,
    pub payload: Option<Vec<u8>>,
    pub timestamp_ms: Option<i64>,
    /// Record headers in wire order. Kafka allows repeated keys, and order is
    /// meaningful, so this is a list, not a map. Null-valued headers are skipped.
    pub headers: Vec<(String, Vec<u8>)>,
}

/// Timestamps of the records at the given `(topic, partition, offset)`
/// positions (a negative offset: the oldest retained record), read with a group-less scanner. Positions with no record
/// (yet) are left out; gives up after `timeout`.
pub async fn record_timestamps(
    config: &KafkaClientConfig,
    positions: &[(String, i32, i64)],
    timeout: Duration,
) -> KafkaResult<HashMap<(String, i32), i64>> {
    let mut found = HashMap::new();
    if positions.is_empty() {
        return Ok(found);
    }
    let consumer = KafkaConsumerClient::scanner(config)?;
    let assignments: Vec<(&str, i32, rdkafka::Offset)> = positions
        .iter()
        .map(|(t, p, o)| {
            // A negative offset means the oldest retained record.
            let offset = if *o < 0 {
                rdkafka::Offset::Beginning
            } else {
                rdkafka::Offset::Offset(*o)
            };
            (t.as_str(), *p, offset)
        })
        .collect();
    consumer.assign_with_offsets(&assignments)?;
    let deadline = tokio::time::Instant::now() + timeout;
    while found.len() < positions.len() {
        let Ok(msg) = tokio::time::timeout_at(deadline, consumer.recv()).await else {
            break;
        };
        // One unreadable position must not lose the others: skip the error
        // and keep reading until the deadline.
        let Ok(msg) = msg else {
            continue;
        };
        let key = (msg.topic.clone(), msg.partition);
        if let (std::collections::hash_map::Entry::Vacant(e), Some(ts)) =
            (found.entry(key), msg.timestamp_ms)
        {
            e.insert(ts);
        }
    }
    Ok(found)
}

/// One partition's answer to a time → offset lookup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Lookup {
    /// The first offset whose timestamp is at or after the time.
    At(i64),
    /// No record at or after the time: reading starts at the end.
    End,
    /// The broker could not answer for this partition.
    Failed(LookupFailure),
}

/// Why a time → offset lookup failed, and whether retrying can help.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LookupFailure {
    pub message: String,
    pub class: crate::error::ErrorClass,
}

impl LookupFailure {
    pub fn new(message: impl Into<String>, class: crate::error::ErrorClass) -> Self {
        Self {
            message: message.into(),
            class,
        }
    }

    pub fn from_error(error: &KafkaError) -> Self {
        Self::new(error.to_string(), error.class())
    }
}

impl Lookup {
    fn from_offset(offset: Offset) -> Self {
        match offset {
            Offset::Offset(o) if o >= 0 => Lookup::At(o),
            _ => Lookup::End,
        }
    }

    /// The start position: [`Offset::End`] unless an offset was found.
    pub fn as_offset(&self) -> Offset {
        match self {
            Lookup::At(o) => Offset::Offset(*o),
            Lookup::End | Lookup::Failed(_) => Offset::End,
        }
    }
}

/// Start offsets per partition from an [`KafkaConsumerClient::offsets_for_timestamp`]
/// lookup: the first offset at or after the timestamp, [`Offset::End`] when
/// the partition has no such record (or no lookup result).
pub fn timestamp_start_offsets(
    topic: &str,
    partitions: &[i32],
    lookup: &HashMap<(String, i32), Offset>,
) -> Vec<(i32, Offset)> {
    partitions
        .iter()
        .map(|&p| {
            let start = match lookup.get(&(topic.to_string(), p)) {
                Some(Offset::Offset(o)) if *o >= 0 => Offset::Offset(*o),
                _ => Offset::End,
            };
            (p, start)
        })
        .collect()
}

/// Stops reading partitions once a record is past an end timestamp
/// (`--to-timestamp`). Without one it admits every record and never finishes.
#[derive(Debug, Clone, Default)]
pub struct TimestampStop {
    to: Option<i64>,
    /// Partitions not yet read past `to`.
    remaining: HashSet<i32>,
}

impl TimestampStop {
    pub fn new(to: Option<i64>, partitions: &[i32]) -> Self {
        Self {
            to,
            remaining: partitions.iter().copied().collect(),
        }
    }

    /// Whether a record with this timestamp is past `to`.
    pub fn is_past(to: Option<i64>, timestamp_ms: Option<i64>) -> bool {
        matches!((to, timestamp_ms), (Some(to), Some(ts)) if ts > to)
    }

    /// Whether to process a record of `partition`. A record past the end
    /// timestamp finishes its partition; records of finished partitions are
    /// not processed.
    pub fn admit(&mut self, partition: i32, timestamp_ms: Option<i64>) -> bool {
        if self.to.is_none() {
            return true;
        }
        if !self.remaining.contains(&partition) {
            return false;
        }
        if Self::is_past(self.to, timestamp_ms) {
            self.remaining.remove(&partition);
            return false;
        }
        true
    }

    /// Marks a partition finished (e.g. it has nothing to read).
    pub fn finish(&mut self, partition: i32) {
        self.remaining.remove(&partition);
    }

    /// Whether every partition has been read past the end timestamp.
    pub fn all_done(&self) -> bool {
        self.to.is_some() && self.remaining.is_empty()
    }
}

impl ConsumedMessage {
    /// A copy of this record with PII masked in its key, payload and header values
    /// (each header value judged by its name, as JSON fields are by key).
    /// Binary values are hidden whole, since they cannot be scanned.
    pub fn masked(&self) -> Self {
        let masked = crate::codec::pii::mask_record(
            self.key.as_deref(),
            self.payload.as_deref(),
            self.headers.iter().map(|(k, v)| (k.as_str(), v.as_slice())),
            crate::codec::pii::MaskStrategy::Partial,
        );
        ConsumedMessage {
            key: masked.key.map(String::into_bytes),
            payload: masked.payload.map(String::into_bytes),
            headers: masked
                .headers
                .into_iter()
                .map(|(k, v)| (k, v.into_bytes()))
                .collect(),
            ..self.clone()
        }
    }

    /// Returns the message key as a UTF-8 string if valid.
    pub fn key_str(&self) -> Option<&str> {
        self.key
            .as_deref()
            .and_then(|k| std::str::from_utf8(k).ok())
    }

    /// Returns the message payload as a UTF-8 string if valid.
    pub fn payload_str(&self) -> Option<&str> {
        self.payload
            .as_deref()
            .and_then(|p| std::str::from_utf8(p).ok())
    }

    /// Deserializes the message payload as JSON into the specified type `T`.
    pub fn payload_json<T: DeserializeOwned>(&self) -> KafkaResult<T> {
        let payload = self.payload.as_deref().ok_or_else(|| {
            KafkaError::Serialization("Message payload is empty (null)".to_string())
        })?;
        serde_json::from_slice(payload)
            .map_err(|e| KafkaError::Serialization(format!("JSON deserialization error: {e}")))
    }
}

/// librdkafka consumer property, set lower than its default in [`consumer_config`].
const FETCH_QUEUE_BACKOFF: &str = "fetch.queue.backoff.ms";

/// The rdkafka configuration for a consumer.
fn consumer_config(cfg: &KafkaClientConfig) -> KafkaResult<rdkafka::ClientConfig> {
    let mut rd_config = cfg.to_rdkafka_config()?;
    // librdkafka pauses a partition's fetching for this long whenever its
    // local queue is full. The 1 s default stalls every bulk read that
    // processes records a little slower than they arrive: reading 10M
    // records took 49 s instead of 13 s, at the same memory.
    if !cfg.extra_properties.contains_key(FETCH_QUEUE_BACKOFF) {
        rd_config.set(FETCH_QUEUE_BACKOFF, "10");
    }
    Ok(rd_config)
}

/// Copies a record out of librdkafka's buffer.
fn owned_message<M: Message>(msg: &M) -> ConsumedMessage {
    let mut owned = ConsumedMessage {
        topic: String::new(),
        partition: 0,
        offset: 0,
        key: None,
        payload: None,
        timestamp_ms: None,
        headers: Vec::new(),
    };
    copy_message(msg, &mut owned);
    owned
}

/// Copies a record into `out`, reusing its buffers.
fn copy_message<M: Message>(msg: &M, out: &mut ConsumedMessage) {
    fn copy_bytes(out: &mut Option<Vec<u8>>, bytes: Option<&[u8]>) {
        match bytes {
            Some(bytes) => {
                let buf = out.get_or_insert_with(Vec::new);
                buf.clear();
                buf.extend_from_slice(bytes);
            }
            None => *out = None,
        }
    }
    out.topic.clear();
    out.topic.push_str(msg.topic());
    out.partition = msg.partition();
    out.offset = msg.offset();
    copy_bytes(&mut out.key, msg.key());
    copy_bytes(&mut out.payload, msg.payload());
    out.timestamp_ms = msg.timestamp().to_millis();
    out.headers.clear();
    if let Some(msg_headers) = msg.headers() {
        for idx in 0..msg_headers.count() {
            let header = msg_headers.get(idx);
            if let Some(val) = header.value {
                out.headers.push((header.key.to_string(), val.to_vec()));
            }
        }
    }
}

/// What a [`ParallelReader`] hands over.
#[derive(Debug)]
pub enum ReadEvent {
    /// A record the reader's filter kept.
    Record(ConsumedMessage),
    /// This many records were read and dropped by the filter.
    Skipped(u64),
    /// A consumer error; reading goes on.
    Error(KafkaError),
}

/// Most consumers a [`ParallelReader`] runs at once.
const MAX_READERS: usize = 8;

/// librdkafka consumer property, set lower than its default for each reader
/// of a [`ParallelReader`].
const QUEUED_MAX_KBYTES: &str = "queued.max.messages.kbytes";

/// What all readers of a [`ParallelReader`] may prefetch together, in KiB,
/// before the per-reader floor applies.
const QUEUE_BUDGET_KBYTES: u64 = 12 * 1024;

/// Most one reader prefetches, and with a lone reader the whole budget.
const QUEUE_MAX_KBYTES_PER_READER: u64 = 8 * 1024;

/// Least one reader prefetches. Below this, small-record reads slow down:
/// 1 MiB cost 3-6 % and 512 KiB 10-30 % on 200k-500k record reads.
const QUEUE_MIN_KBYTES_PER_READER: u64 = 2 * 1024;

/// The `queued.max.messages.kbytes` of each of `readers` readers: an equal
/// share of [`QUEUE_BUDGET_KBYTES`], between the floor and the cap.
///
/// Six readers with 8 MiB each peaked at 224 MB reading 500k 100-byte
/// records (a lone reader: 58 MB); with 2 MiB each they peak at 98 MB at
/// the same speed. Queued bytes count payload only, so a small record
/// costs several times its size in memory.
fn reader_queue_kbytes(readers: usize) -> u64 {
    let readers = readers.max(1) as u64;
    (QUEUE_BUDGET_KBYTES / readers).clamp(QUEUE_MIN_KBYTES_PER_READER, QUEUE_MAX_KBYTES_PER_READER)
}

/// `queued.max.messages.kbytes` for a consumer that one loop reads from.
const SINGLE_QUEUE_KBYTES: u64 = 8 * 1024;

/// Lowers librdkafka's 64 MiB prefetch queue to 8 MiB for a consumer read
/// by a single loop (mirror, dump), unless the configuration sets it. That
/// loop does not keep 64 MiB busy: a 6-partition mirror of 500k 100-byte
/// records peaked at 225 MB and a dump of 200k 1 KiB records at 120 MB, and
/// with 8 MiB they run as fast in about half the memory.
pub fn limit_prefetch(config: &mut KafkaClientConfig) {
    config
        .extra_properties
        .entry(QUEUED_MAX_KBYTES.to_string())
        .or_insert_with(|| SINGLE_QUEUE_KBYTES.to_string());
}

/// How long each of the two watermark lookups of
/// [`KafkaConsumerClient::resolve_logical_offsets`] may take.
const WATERMARK_RESOLVE_TIMEOUT: Duration = Duration::from_secs(3);

/// Whether `offset` is `Beginning` or `End`, which librdkafka resolves itself.
fn is_logical(offset: Offset) -> bool {
    matches!(offset, Offset::Beginning | Offset::End)
}

/// `offset` as an explicit offset, given the partition's `(low, high)`
/// watermarks. Anything but `Beginning` and `End`, or an unknown partition,
/// is returned unchanged.
fn resolve_offset(offset: Offset, watermarks: Option<(i64, i64)>) -> Offset {
    match (offset, watermarks) {
        (Offset::Beginning, Some((low, _))) => Offset::Offset(low),
        (Offset::End, Some((_, high))) => Offset::Offset(high),
        _ => offset,
    }
}

/// Records a reader drops before it reports them as [`ReadEvent::Skipped`].
const SKIP_REPORT_EVERY: u64 = 4096;

/// One partition for a [`ParallelReader`] to read.
#[derive(Debug, Clone, Copy)]
pub struct PartitionRead<'a> {
    pub topic: &'a str,
    pub partition: i32,
    pub start: Offset,
    /// The offset to stop before; `None` reads on as records arrive.
    pub end: Option<i64>,
}

/// Reads partitions with one consumer per thread, up to `MAX_READERS`,
/// and filters records on those threads.
///
/// One librdkafka consumer reads a broker over one connection with one
/// thread, which caps a bulk read: 10M records took 7.2 s with one consumer
/// and 3.9 s with three (one per partition) over the same network. Filtering
/// on the reading threads also keeps the caller from copying every record.
/// Dropping the reader stops its threads.
pub struct ParallelReader {
    rx: tokio::sync::mpsc::Receiver<ReadEvent>,
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl ParallelReader {
    /// Starts reading `partitions`, keeping the records `keep` accepts.
    /// When every partition has an end, [`Self::recv`] returns `None` once
    /// all of them are read.
    pub fn start<F>(
        config: &KafkaClientConfig,
        partitions: &[PartitionRead],
        keep: F,
    ) -> KafkaResult<Self>
    where
        F: Fn(&ConsumedMessage) -> bool + Send + Sync + 'static,
    {
        let mut cfg = config.clone();
        cfg.enable_auto_commit = false;
        if cfg.group_id.is_none() {
            cfg.group_id = Some(format!("ferrika-consumer-{}", uuid_or_timestamp()));
        }
        // Explicit starting offsets spare each reader librdkafka's 500 ms
        // retry of a logical one (see `resolve_logical_offsets`).
        let starts: Vec<Offset> = if partitions.iter().any(|p| is_logical(p.start)) {
            match KafkaConsumerClient::lookup(&cfg) {
                Ok(lookup) => {
                    let assignments: Vec<(&str, i32, Offset)> = partitions
                        .iter()
                        .map(|p| (p.topic, p.partition, p.start))
                        .collect();
                    lookup.resolve_logical_offsets(&assignments)
                }
                Err(_) => partitions.iter().map(|p| p.start).collect(),
            }
        } else {
            partitions.iter().map(|p| p.start).collect()
        };
        let readers = partitions.len().clamp(1, MAX_READERS);
        let (tx, rx) = tokio::sync::mpsc::channel(1024);
        let stop = Arc::new(AtomicBool::new(false));
        let keep = Arc::new(keep);
        for i in 0..readers {
            let mine: Vec<&PartitionRead> = partitions.iter().skip(i).step_by(readers).collect();
            let mut tpl = TopicPartitionList::new();
            for (n, p) in partitions.iter().enumerate().skip(i).step_by(readers) {
                tpl.add_partition_offset(p.topic, p.partition, starts[n])?;
            }
            let mut rd_config = consumer_config(&cfg)?;
            // Each reader prefetches up to this much. librdkafka's 64 MiB
            // default tripled peak memory with three readers; 8 MiB read
            // 10M records as fast. The readers share a budget (see
            // `reader_queue_kbytes`) so that more partitions do not mean
            // proportionally more memory.
            if !cfg.extra_properties.contains_key(QUEUED_MAX_KBYTES) {
                rd_config.set(QUEUED_MAX_KBYTES, reader_queue_kbytes(readers).to_string());
            }
            // A partition ending in transaction markers never delivers its
            // end offset; reaching its end tells the reader it is done.
            let ends: HashMap<i32, Option<i64>> =
                mine.iter().map(|p| (p.partition, p.end)).collect();
            let bounded = ends.values().all(Option::is_some);
            if bounded {
                rd_config.set("enable.partition.eof", "true");
            }
            let (oauth, consumer) = crate::kafka::oauth_refresh::with_oauth(
                &cfg,
                || Ok(rd_config.create::<BaseConsumer>()?),
                |consumer| consumer.client(),
            )?;
            consumer.assign(&tpl)?;
            let worker = ReadWorker {
                _oauth: oauth,
                consumer,
                ends,
                bounded,
                tx: tx.clone(),
                stop: stop.clone(),
            };
            let keep = keep.clone();
            std::thread::spawn(move || worker.run(&*keep));
        }
        Ok(Self { rx, stop })
    }

    /// The next record or report; `None` once every reader has stopped.
    pub async fn recv(&mut self) -> Option<ReadEvent> {
        self.rx.recv().await
    }
}

impl Drop for ParallelReader {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

/// One thread of a [`ParallelReader`].
struct ReadWorker {
    // Declared before `consumer` so it stops before the client is destroyed.
    _oauth: Option<crate::kafka::oauth_refresh::OAuthRefresher>,
    consumer: BaseConsumer,
    /// The partitions still being read, with the offset that ends each.
    ends: HashMap<i32, Option<i64>>,
    /// Whether every partition has an end, so the worker stops after them.
    bounded: bool,
    tx: tokio::sync::mpsc::Sender<ReadEvent>,
    stop: Arc<AtomicBool>,
}

impl ReadWorker {
    fn run(mut self, keep: &dyn Fn(&ConsumedMessage) -> bool) {
        let mut skipped = 0u64;
        let mut reported = std::time::Instant::now();
        // Records are copied into one buffer, and only the kept ones into
        // new allocations.
        let mut record: Option<ConsumedMessage> = None;
        while !self.stop.load(Ordering::Relaxed) && !(self.bounded && self.ends.is_empty()) {
            let event = match self.consumer.poll(Duration::from_millis(100)) {
                Some(Ok(msg)) => match self.ends.get(&msg.partition()) {
                    Some(&Some(end)) if msg.offset() >= end => {
                        self.ends.remove(&msg.partition());
                        None
                    }
                    // Records of a partition already read to its end.
                    None => None,
                    Some(&end) => {
                        if end.is_some_and(|end| msg.offset() + 1 >= end) {
                            self.ends.remove(&msg.partition());
                        }
                        let copy = match record.as_mut() {
                            Some(copy) => {
                                copy_message(&msg, copy);
                                copy
                            }
                            None => record.insert(owned_message(&msg)),
                        };
                        if keep(copy) {
                            Some(ReadEvent::Record(copy.clone()))
                        } else {
                            skipped += 1;
                            None
                        }
                    }
                },
                Some(Err(rdkafka::error::KafkaError::PartitionEOF(partition))) => {
                    self.ends.remove(&partition);
                    None
                }
                Some(Err(e)) => Some(ReadEvent::Error(e.into())),
                None => None,
            };
            // Skips are reported in batches, and at least every 100 ms so
            // the caller sees that reading goes on.
            if skipped >= SKIP_REPORT_EVERY
                || (skipped > 0
                    && (event.is_some() || reported.elapsed() >= Duration::from_millis(100)))
            {
                if self.tx.blocking_send(ReadEvent::Skipped(skipped)).is_err() {
                    return;
                }
                skipped = 0;
                reported = std::time::Instant::now();
            }
            if let Some(event) = event {
                if self.tx.blocking_send(event).is_err() {
                    return;
                }
            }
        }
        if skipped > 0 {
            let _ = self.tx.blocking_send(ReadEvent::Skipped(skipped));
        }
    }
}

/// Asynchronous Kafka consumer client built on top of rdkafka's `StreamConsumer`.
pub struct KafkaConsumerClient {
    // Declared before `consumer` so it stops before the client is destroyed.
    _oauth: Option<crate::kafka::oauth_refresh::OAuthRefresher>,
    consumer: DetachedDrop<StreamConsumer<DefaultConsumerContext>>,
    config: KafkaClientConfig,
}

impl KafkaConsumerClient {
    /// Creates a new `KafkaConsumerClient` from the configuration.
    /// If no `group_id` is set, a default unique group ID is generated.
    pub fn new(config: &KafkaClientConfig) -> KafkaResult<Self> {
        let mut cfg = config.clone();
        if cfg.group_id.is_none() {
            cfg.group_id = Some(format!("ferrika-consumer-{}", uuid_or_timestamp()));
        }
        Self::build(cfg)
    }

    fn build(cfg: KafkaClientConfig) -> KafkaResult<Self> {
        let rd_config = consumer_config(&cfg)?;
        let (oauth, consumer) = crate::kafka::oauth_refresh::with_oauth(
            &cfg,
            || Ok(rd_config.create::<StreamConsumer<DefaultConsumerContext>>()?),
            |consumer| consumer.client(),
        )?;

        Ok(Self {
            _oauth: oauth,
            consumer: DetachedDrop::in_place(consumer),
            config: cfg,
        })
    }

    /// Lets this client shut down on another thread when dropped, so the
    /// command ends without waiting for librdkafka (about 100 ms). Only for
    /// a client that has no group state to hand back: the shutdown may not
    /// finish before the process exits. [`Self::subscribe`] and every commit
    /// method undo this.
    pub fn detached(self) -> Self {
        self.consumer.set_detach(true);
        self
    }

    /// Makes this client shut down in place again: a group member must leave
    /// the group and commit before the process ends.
    fn pin(&self) {
        self.consumer.set_detach(false);
    }

    /// Returns the client configuration.
    pub fn config(&self) -> &KafkaClientConfig {
        &self.config
    }

    /// Subscribes to a list of topics.
    pub fn subscribe(&self, topics: &[&str]) -> KafkaResult<()> {
        self.pin();
        self.consumer.subscribe(topics)?;
        Ok(())
    }

    /// Unsubscribes from all currently subscribed topics.
    pub fn unsubscribe(&self) {
        self.consumer.unsubscribe();
    }

    /// Assigns specific topic partitions to this consumer manually.
    pub fn assign_partitions(&self, topic_partitions: &[(&str, i32)]) -> KafkaResult<()> {
        let mut tpl = TopicPartitionList::new();
        for &(topic, partition) in topic_partitions {
            tpl.add_partition(topic, partition);
        }
        self.consumer.assign(&tpl)?;
        Ok(())
    }

    /// Assigns partitions with explicit starting offsets, without joining a consumer group.
    /// Prefer this over `assign_partitions` + `seek`: seeking right after an assign fails
    /// with "Erroneous state" until the partition fetcher has started.
    pub fn assign_with_offsets(&self, assignments: &[(&str, i32, Offset)]) -> KafkaResult<()> {
        let offsets = self.resolve_logical_offsets(assignments);
        let mut tpl = TopicPartitionList::new();
        for (&(topic, partition, _), offset) in assignments.iter().zip(offsets) {
            tpl.add_partition_offset(topic, partition, offset)?;
        }
        self.consumer.assign(&tpl)?;
        Ok(())
    }

    /// The starting offsets of `assignments` with `Beginning` and `End`
    /// replaced by the partition's low and high watermark.
    ///
    /// A fresh consumer assigned a logical offset has no partition leader
    /// yet, so librdkafka's first offset query fails and is retried 500 ms
    /// later: `consume -b -n 1` took 614 ms and `consume --offset 0 -n 1`
    /// (an explicit offset) 114 ms. If the watermarks cannot be read, the
    /// logical offsets are kept and librdkafka resolves them as before.
    pub fn resolve_logical_offsets(&self, assignments: &[(&str, i32, Offset)]) -> Vec<Offset> {
        let logical: Vec<(&str, i32)> = assignments
            .iter()
            .filter(|(_, _, offset)| is_logical(*offset))
            .map(|&(topic, partition, _)| (topic, partition))
            .collect();
        if logical.is_empty() {
            return assignments.iter().map(|&(_, _, offset)| offset).collect();
        }
        // A short limit: when the brokers do not answer, the logical
        // offsets below cost only the 500 ms retry they always did.
        let marks = self
            .fetch_watermarks_batch(&logical, Some(WATERMARK_RESOLVE_TIMEOUT))
            .unwrap_or_default();
        assignments
            .iter()
            .map(|&(topic, partition, offset)| {
                resolve_offset(offset, marks.get(&(topic.to_string(), partition)).copied())
            })
            .collect()
    }

    /// A consumer for one-off reads (inspect, audit, diff...): it never joins
    /// or commits to a group, so it starts reading at once and leaves no
    /// consumer group behind. Pair it with [`Self::assign_topic`].
    pub fn scanner(config: &KafkaClientConfig) -> KafkaResult<Self> {
        let mut cfg = config.clone();
        cfg.group_id = None;
        cfg.enable_auto_commit = false;
        Ok(Self::new(&cfg)?.detached())
    }

    /// A client for metadata lookups only (watermarks, offsets for times): it
    /// has no `group.id`, so it cannot assign or consume. With a group id,
    /// librdkafka connects to a group coordinator even without subscribing,
    /// and closing the client while that connection is being set up logs
    /// spurious authentication errors.
    pub fn lookup(config: &KafkaClientConfig) -> KafkaResult<Self> {
        let mut cfg = config.clone();
        cfg.group_id = None;
        cfg.enable_auto_commit = false;
        Self::build(cfg)
    }

    /// Assigns every partition of `topic`, starting at `start`, and returns
    /// the partition ids.
    pub fn assign_topic(&self, topic: &str, start: Offset) -> KafkaResult<Vec<i32>> {
        let partitions = self.topic_partitions(topic, None)?;
        let assignments: Vec<(&str, i32, Offset)> =
            partitions.iter().map(|&p| (topic, p, start)).collect();
        self.assign_with_offsets(&assignments)?;
        Ok(partitions)
    }

    /// Returns the partition ids of a topic from cluster metadata.
    pub fn topic_partitions(
        &self,
        topic: &str,
        timeout: Option<Duration>,
    ) -> KafkaResult<Vec<i32>> {
        let to = timeout.unwrap_or(self.config.request_timeout);
        let metadata = self.consumer.fetch_metadata(Some(topic), to)?;
        let partitions: Vec<i32> = metadata
            .topics()
            .iter()
            .filter(|t| t.name() == topic && t.error().is_none())
            .flat_map(|t| t.partitions().iter().map(|p| p.id()))
            .collect();
        if partitions.is_empty() {
            return Err(KafkaError::TopicNotFound(topic.to_string()));
        }
        Ok(partitions)
    }

    /// Seeks to a specific offset on a topic partition.
    pub fn seek(
        &self,
        topic: &str,
        partition: i32,
        offset: Offset,
        timeout: Duration,
    ) -> KafkaResult<()> {
        self.consumer.seek(topic, partition, offset, timeout)?;
        Ok(())
    }

    /// Receives the next message from the stream asynchronously.
    pub async fn recv(&self) -> KafkaResult<ConsumedMessage> {
        let msg = self.consumer.recv().await?;
        Ok(owned_message(&msg))
    }

    /// Fetches low and high watermarks for a given topic partition, retrying
    /// for a few seconds while its leader is not ready yet.
    pub fn fetch_watermarks(
        &self,
        topic: &str,
        partition: i32,
        timeout: Option<Duration>,
    ) -> KafkaResult<(i64, i64)> {
        let to = timeout.unwrap_or(self.config.request_timeout);
        crate::kafka::retry::retry_leader_change(|| {
            Ok(self.consumer.fetch_watermarks(topic, partition, to)?)
        })
    }

    /// Fetches low and high watermarks for many partitions at once.
    ///
    /// Uses two ListOffsets lookups (earliest and latest) that librdkafka batches
    /// per partition leader, instead of one round trip per partition.
    /// Partitions whose offsets could not be resolved are omitted. A
    /// partition whose leader is not ready yet (a topic created a moment ago)
    /// is retried for a few seconds first.
    pub fn fetch_watermarks_batch(
        &self,
        partitions: &[(&str, i32)],
        timeout: Option<Duration>,
    ) -> KafkaResult<HashMap<(String, i32), (i64, i64)>> {
        match self.fetch_watermarks_once(partitions, timeout) {
            Ok(marks) => Ok(marks),
            // One unavailable partition (e.g. its only replica's broker is
            // down) fails the whole request; retry per topic so the others
            // still get their watermarks.
            Err(e) => {
                let mut topics: Vec<&str> = partitions.iter().map(|(t, _)| *t).collect();
                topics.sort_unstable();
                topics.dedup();
                if topics.len() < 2 {
                    return Err(e);
                }
                let mut marks = HashMap::new();
                for topic in topics {
                    let part: Vec<(&str, i32)> = partitions
                        .iter()
                        .filter(|(t, _)| *t == topic)
                        .copied()
                        .collect();
                    match self.fetch_watermarks_once(&part, timeout) {
                        Ok(m) => marks.extend(m),
                        Err(e) => tracing::debug!(topic, %e, "Watermarks unavailable"),
                    }
                }
                Ok(marks)
            }
        }
    }

    fn fetch_watermarks_once(
        &self,
        partitions: &[(&str, i32)],
        timeout: Option<Duration>,
    ) -> KafkaResult<HashMap<(String, i32), (i64, i64)>> {
        if partitions.is_empty() {
            return Ok(HashMap::new());
        }
        let to = timeout.unwrap_or(self.config.request_timeout);
        // offsets_for_times treats the special offsets as "earliest" / "latest".
        let lookup = |offset: Offset| -> KafkaResult<TopicPartitionList> {
            let mut tpl = TopicPartitionList::new();
            for &(topic, partition) in partitions {
                tpl.add_partition_offset(topic, partition, offset)?;
            }
            Ok(self.consumer.offsets_for_times(tpl, to)?)
        };
        // Right after a topic is created (or while a leader moves) the
        // lookup can fail with NotLeaderForPartition for a moment.
        let (low, high) = crate::kafka::retry::retry_leader_change(|| {
            Ok((lookup(Offset::Beginning)?, lookup(Offset::End)?))
        })?;

        let mut watermarks = HashMap::with_capacity(partitions.len());
        for elem in high.elements() {
            let Offset::Offset(high) = elem.offset() else {
                continue;
            };
            let low = match low
                .find_partition(elem.topic(), elem.partition())
                .map(|e| e.offset())
            {
                Some(Offset::Offset(low)) => low,
                _ => 0,
            };
            watermarks.insert((elem.topic().to_string(), elem.partition()), (low, high));
        }
        Ok(watermarks)
    }

    /// Resolves, per partition, the first offset whose timestamp is at or after
    /// `timestamp_ms`. Partitions with no such record (or a failed lookup) map
    /// to [`Offset::End`]; see [`Self::offsets_at_time`] to tell them apart.
    pub fn offsets_for_timestamp(
        &self,
        partitions: &[(&str, i32)],
        timestamp_ms: i64,
        timeout: Option<Duration>,
    ) -> KafkaResult<HashMap<(String, i32), Offset>> {
        Ok(self
            .offsets_at_time(partitions, timestamp_ms, timeout)?
            .into_iter()
            .map(|(k, lookup)| (k, lookup.as_offset()))
            .collect())
    }

    /// Like [`Self::offsets_for_timestamp`], but keeps "no record at or after
    /// the time" ([`Lookup::End`]) apart from a per-partition error
    /// ([`Lookup::Failed`]).
    pub fn offsets_at_time(
        &self,
        partitions: &[(&str, i32)],
        timestamp_ms: i64,
        timeout: Option<Duration>,
    ) -> KafkaResult<HashMap<(String, i32), Lookup>> {
        let to = timeout.unwrap_or(self.config.request_timeout);
        let mut tpl = TopicPartitionList::new();
        for &(topic, partition) in partitions {
            tpl.add_partition_offset(topic, partition, Offset::Offset(timestamp_ms))?;
        }
        let resolved = self.consumer.offsets_for_times(tpl, to)?;
        Ok(resolved
            .elements()
            .iter()
            .map(|e| {
                let lookup = match e.error() {
                    Err(err) => Lookup::Failed(LookupFailure::new(
                        err.to_string(),
                        crate::error::ErrorClass::of_driver(&err),
                    )),
                    Ok(()) => Lookup::from_offset(e.offset()),
                };
                ((e.topic().to_string(), e.partition()), lookup)
            })
            .collect())
    }

    /// Fetches committed offsets for the currently assigned partitions.
    pub fn fetch_committed_offsets(
        &self,
        timeout: Option<Duration>,
    ) -> KafkaResult<TopicPartitionList> {
        let to = timeout.unwrap_or(self.config.request_timeout);
        let committed = self.consumer.committed(to)?;
        Ok(committed)
    }

    /// Fetches committed offsets for the supplied partitions.
    ///
    /// Use this for administrative lag inspection. A newly-created consumer has
    /// no assignment, so [`Self::fetch_committed_offsets`] would otherwise
    /// return an empty list even when the group has committed offsets.
    pub fn fetch_committed_offsets_for(
        &self,
        partitions: &TopicPartitionList,
        timeout: Option<Duration>,
    ) -> KafkaResult<TopicPartitionList> {
        let to = timeout.unwrap_or(self.config.request_timeout);
        let committed = self.consumer.committed_offsets(partitions.clone(), to)?;
        Ok(committed)
    }

    /// Commits the given consumed message.
    pub fn commit_message(&self, message: &ConsumedMessage, mode: CommitMode) -> KafkaResult<()> {
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(
            &message.topic,
            message.partition,
            Offset::Offset(message.offset + 1),
        )
        .map_err(|e| KafkaError::Offset(e.to_string()))?;

        self.pin();
        self.consumer.commit(&tpl, mode)?;
        Ok(())
    }

    /// Commits all pending offsets.
    pub fn commit_all(&self, mode: CommitMode) -> KafkaResult<()> {
        let tpl = self
            .consumer
            .assignment()
            .map_err(|e| KafkaError::Consumer(e.to_string()))?;
        self.pin();
        self.consumer.commit(&tpl, mode)?;
        Ok(())
    }

    /// Marks `message` as processed, so the next commit (auto or explicit)
    /// resumes after it. Requires `enable.auto.offset.store=false`.
    pub fn store_processed(&self, message: &ConsumedMessage) -> KafkaResult<()> {
        // librdkafka stores `offset + 1`, the next record to read.
        self.pin();
        self.consumer
            .store_offset(&message.topic, message.partition, message.offset)?;
        Ok(())
    }

    /// Like [`Self::store_processed`] for the record at `offset` of a
    /// partition, for callers that no longer hold the message.
    pub fn store_processed_offset(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> KafkaResult<()> {
        self.pin();
        self.consumer.store_offset(topic, partition, offset)?;
        Ok(())
    }

    /// Commits the offsets stored with [`Self::store_processed`].
    pub fn commit_stored(&self, mode: CommitMode) -> KafkaResult<()> {
        self.pin();
        match self.consumer.commit_consumer_state(mode) {
            Ok(()) => Ok(()),
            // Nothing was stored since the last commit.
            Err(e)
                if e.rdkafka_error_code() == Some(rdkafka::types::RDKafkaErrorCode::NoOffset) =>
            {
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Commits offsets specified in a TopicPartitionList.
    pub fn commit(&self, tpl: &TopicPartitionList, mode: CommitMode) -> KafkaResult<()> {
        self.pin();
        self.consumer.commit(tpl, mode)?;
        Ok(())
    }

    /// Returns a reference to the inner rdkafka StreamConsumer.
    pub fn inner(&self) -> &StreamConsumer<DefaultConsumerContext> {
        &self.consumer
    }
}

fn uuid_or_timestamp() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limit_prefetch_keeps_a_configured_value() {
        let mut config = KafkaClientConfig::new("b:9092");
        limit_prefetch(&mut config);
        assert_eq!(
            config.extra_properties.get(QUEUED_MAX_KBYTES).unwrap(),
            "8192"
        );
        config
            .extra_properties
            .insert(QUEUED_MAX_KBYTES.to_string(), "1024".to_string());
        limit_prefetch(&mut config);
        assert_eq!(
            config.extra_properties.get(QUEUED_MAX_KBYTES).unwrap(),
            "1024"
        );
    }

    #[test]
    fn logical_offsets_become_watermarks() {
        let marks = Some((5, 90));
        assert_eq!(resolve_offset(Offset::Beginning, marks), Offset::Offset(5));
        assert_eq!(resolve_offset(Offset::End, marks), Offset::Offset(90));
        // Explicit and stored offsets are not touched, nor is an unknown partition.
        assert_eq!(resolve_offset(Offset::Offset(7), marks), Offset::Offset(7));
        assert_eq!(resolve_offset(Offset::Stored, marks), Offset::Stored);
        assert_eq!(resolve_offset(Offset::Beginning, None), Offset::Beginning);
        assert_eq!(resolve_offset(Offset::End, None), Offset::End);
        assert!(is_logical(Offset::Beginning) && is_logical(Offset::End));
        assert!(!is_logical(Offset::Offset(0)) && !is_logical(Offset::Stored));
    }

    #[test]
    fn reader_queues_share_a_budget_with_a_floor_and_a_cap() {
        // A lone reader keeps the size it always had.
        assert_eq!(reader_queue_kbytes(1), 8192);
        assert_eq!(reader_queue_kbytes(0), 8192);
        assert_eq!(reader_queue_kbytes(2), 6144);
        assert_eq!(reader_queue_kbytes(3), 4096);
        assert_eq!(reader_queue_kbytes(6), 2048);
        // Past the floor each reader still gets 2 MiB: 8 readers, 16 MiB.
        assert_eq!(reader_queue_kbytes(8), 2048);
        for readers in 1..=MAX_READERS {
            let each = reader_queue_kbytes(readers);
            assert!((2048..=8192).contains(&each));
        }
    }

    #[test]
    fn test_timestamp_start_offsets_missing_or_end_is_end() {
        let mut lookup = HashMap::new();
        lookup.insert(("t".to_string(), 0), Offset::Offset(17));
        lookup.insert(("t".to_string(), 1), Offset::End);
        let starts = timestamp_start_offsets("t", &[0, 1, 2], &lookup);
        assert_eq!(
            starts,
            vec![(0, Offset::Offset(17)), (1, Offset::End), (2, Offset::End)]
        );
    }

    #[test]
    fn test_lookup_keeps_end_apart_from_failure_but_both_start_at_end() {
        assert_eq!(Lookup::from_offset(Offset::Offset(7)), Lookup::At(7));
        assert_eq!(Lookup::from_offset(Offset::Offset(-1)), Lookup::End);
        assert_eq!(Lookup::from_offset(Offset::End), Lookup::End);
        assert_eq!(Lookup::At(7).as_offset(), Offset::Offset(7));
        assert_eq!(Lookup::End.as_offset(), Offset::End);
        let failed = LookupFailure::new("x", crate::error::ErrorClass::Other);
        assert_eq!(Lookup::Failed(failed).as_offset(), Offset::End);
    }

    #[test]
    fn test_timestamp_stop_finishes_partitions_past_the_end() {
        let mut stop = TimestampStop::new(Some(100), &[0, 1]);
        assert!(stop.admit(0, Some(50)));
        assert!(stop.admit(0, Some(100)));
        // Past the end: partition 0 is finished, later records ignored.
        assert!(!stop.admit(0, Some(101)));
        assert!(!stop.admit(0, Some(60)));
        assert!(!stop.all_done());
        // A record without a timestamp cannot be past the end.
        assert!(stop.admit(1, None));
        stop.finish(1);
        assert!(stop.all_done());
    }

    #[test]
    fn test_timestamp_stop_without_end_admits_everything() {
        let mut stop = TimestampStop::new(None, &[0]);
        assert!(stop.admit(0, Some(i64::MAX)));
        assert!(stop.admit(5, None));
        stop.finish(0);
        assert!(!stop.all_done());
    }

    #[test]
    fn test_consumed_message_helpers() {
        let msg = ConsumedMessage {
            topic: "orders".into(),
            partition: 0,
            offset: 42,
            key: Some(b"order_100".to_vec()),
            payload: Some(b"{\"id\": 100, \"amount\": 250}".to_vec()),
            timestamp_ms: Some(1726900000000),
            headers: Vec::new(),
        };

        assert_eq!(msg.key_str(), Some("order_100"));
        assert_eq!(msg.payload_str(), Some("{\"id\": 100, \"amount\": 250}"));

        #[derive(serde::Deserialize, PartialEq, Debug)]
        struct Order {
            id: i32,
            amount: i32,
        }

        let parsed: Order = msg.payload_json().unwrap();
        assert_eq!(
            parsed,
            Order {
                id: 100,
                amount: 250
            }
        );
    }

    #[test]
    fn test_consumed_message_invalid_json() {
        let msg = ConsumedMessage {
            topic: "orders".into(),
            partition: 0,
            offset: 42,
            key: None,
            payload: Some(b"not a valid json".to_vec()),
            timestamp_ms: None,
            headers: Vec::new(),
        };

        let result: Result<serde_json::Value, _> = msg.payload_json();
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_consumer_client_creation() {
        let config = KafkaClientConfig::new("localhost:9092")
            .with_group_id("consumer-test-group")
            .with_client_id("consumer-test-client");
        let client = KafkaConsumerClient::new(&config);
        assert!(client.is_ok());
        let client = client.unwrap();
        assert_eq!(
            client.config().group_id.as_deref(),
            Some("consumer-test-group")
        );
    }

    #[test]
    fn test_consumer_config_shortens_fetch_queue_backoff() {
        let config = KafkaClientConfig::new("localhost:9092");
        let rd = consumer_config(&config).unwrap();
        assert_eq!(rd.get(FETCH_QUEUE_BACKOFF), Some("10"));

        let mut config = KafkaClientConfig::new("localhost:9092");
        config
            .extra_properties
            .insert(FETCH_QUEUE_BACKOFF.to_string(), "500".to_string());
        let rd = consumer_config(&config).unwrap();
        assert_eq!(rd.get(FETCH_QUEUE_BACKOFF), Some("500"));
    }
}
