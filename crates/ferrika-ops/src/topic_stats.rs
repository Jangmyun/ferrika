//! Whole-topic statistics: reads every retained record once and summarises
//! counts, sizes, keys and time span (like kafka-ui's topic "Statistics").

use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Duration;

use rdkafka::Offset;
use serde::Serialize;

use ferrika_core::error::KafkaResult;
use ferrika_core::kafka::config::KafkaClientConfig;
use ferrika_core::kafka::consumer::{ConsumedMessage, KafkaConsumerClient};

/// Distinct keys are counted exactly up to this many, then reported as "at least".
const MAX_TRACKED_KEYS: usize = 100_000;

/// Sizes of one part of the records (keys or values), in bytes.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct SizeStats {
    pub count: u64,
    pub total: u64,
    pub min: Option<u64>,
    pub max: Option<u64>,
}

impl SizeStats {
    fn add(&mut self, len: usize) {
        let len = len as u64;
        self.count += 1;
        self.total += len;
        self.min = Some(self.min.map_or(len, |m| m.min(len)));
        self.max = Some(self.max.map_or(len, |m| m.max(len)));
    }

    pub fn average(&self) -> Option<f64> {
        (self.count > 0).then(|| self.total as f64 / self.count as f64)
    }
}

/// Statistics of the records read so far.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct TopicAnalysis {
    pub records: u64,
    /// Records per partition.
    pub partitions: BTreeMap<i32, u64>,
    pub null_keys: u64,
    /// Records without a value (deletes on compacted topics).
    pub tombstones: u64,
    pub with_headers: u64,
    pub keys: SizeStats,
    pub values: SizeStats,
    pub first_timestamp_ms: Option<i64>,
    pub last_timestamp_ms: Option<i64>,
    /// Distinct keys seen, capped at `MAX_TRACKED_KEYS`.
    pub distinct_keys: usize,
    pub distinct_keys_capped: bool,
    /// Value-size histogram: `(upper bound in bytes, records)`; the last
    /// bucket has no bound.
    pub value_size_buckets: Vec<(Option<u64>, u64)>,
    #[serde(skip)]
    seen_keys: HashSet<Vec<u8>>,
}

/// Upper bounds of the value-size histogram buckets.
const BUCKET_BOUNDS: [u64; 6] = [100, 1_024, 10_240, 102_400, 1_048_576, 10_485_760];

impl TopicAnalysis {
    pub fn add(&mut self, msg: &ConsumedMessage) {
        self.records += 1;
        *self.partitions.entry(msg.partition).or_default() += 1;
        match &msg.key {
            Some(key) => {
                self.keys.add(key.len());
                if self.seen_keys.len() < MAX_TRACKED_KEYS {
                    self.seen_keys.insert(key.clone());
                } else if !self.seen_keys.contains(key) {
                    self.distinct_keys_capped = true;
                }
                self.distinct_keys = self.seen_keys.len();
            }
            None => self.null_keys += 1,
        }
        match &msg.payload {
            Some(value) => {
                self.values.add(value.len());
                if self.value_size_buckets.is_empty() {
                    self.value_size_buckets = BUCKET_BOUNDS
                        .iter()
                        .map(|b| (Some(*b), 0))
                        .chain(std::iter::once((None, 0)))
                        .collect();
                }
                let len = value.len() as u64;
                if let Some(bucket) = self
                    .value_size_buckets
                    .iter_mut()
                    .find(|(bound, _)| bound.is_none_or(|b| len <= b))
                {
                    bucket.1 += 1;
                }
            }
            None => self.tombstones += 1,
        }
        if !msg.headers.is_empty() {
            self.with_headers += 1;
        }
        if let Some(ts) = msg.timestamp_ms.filter(|t| *t > 0) {
            self.first_timestamp_ms = Some(self.first_timestamp_ms.map_or(ts, |f| f.min(ts)));
            self.last_timestamp_ms = Some(self.last_timestamp_ms.map_or(ts, |l| l.max(ts)));
        }
    }
}

/// Reads `topic` from its start to its current end, calling `progress` with
/// the statistics so far every `every` records and once at the end. Stops
/// early when `progress` returns `false`.
pub async fn analyze_topic(
    config: &KafkaClientConfig,
    topic: &str,
    every: u64,
    mut progress: impl FnMut(&TopicAnalysis, bool) -> bool,
) -> KafkaResult<TopicAnalysis> {
    let consumer = KafkaConsumerClient::scanner(config)?;
    let partitions = consumer.topic_partitions(topic, Some(Duration::from_secs(5)))?;
    let keys: Vec<(&str, i32)> = partitions.iter().map(|&p| (topic, p)).collect();
    let marks = consumer.fetch_watermarks_batch(&keys, Some(Duration::from_secs(5)))?;
    let mut ends: HashMap<i32, i64> = partitions
        .iter()
        .filter_map(|&p| {
            let &(low, high) = marks.get(&(topic.to_string(), p))?;
            (high > low).then_some((p, high))
        })
        .collect();
    let assignments: Vec<(&str, i32, Offset)> = ends
        .keys()
        .map(|&p| (topic, p, Offset::Beginning))
        .collect();
    let mut analysis = TopicAnalysis::default();
    if !assignments.is_empty() {
        consumer.assign_with_offsets(&assignments)?;
    }
    while !ends.is_empty() {
        // Transaction markers at the end of a partition are never delivered;
        // stop once nothing arrives for a while.
        let msg = match tokio::time::timeout(Duration::from_secs(5), consumer.recv()).await {
            Ok(msg) => msg?,
            Err(_) => break,
        };
        let Some(&end) = ends.get(&msg.partition) else {
            continue;
        };
        if msg.offset >= end {
            ends.remove(&msg.partition);
            continue;
        }
        if msg.offset + 1 >= end {
            ends.remove(&msg.partition);
        }
        analysis.add(&msg);
        if analysis.records % every.max(1) == 0 && !progress(&analysis, false) {
            return Ok(analysis);
        }
    }
    progress(&analysis, true);
    Ok(analysis)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn msg(partition: i32, key: Option<&[u8]>, value: Option<usize>, ts: i64) -> ConsumedMessage {
        ConsumedMessage {
            topic: "t".into(),
            partition,
            offset: 0,
            key: key.map(<[u8]>::to_vec),
            payload: value.map(|n| vec![b'x'; n]),
            timestamp_ms: Some(ts),
            headers: Vec::new(),
        }
    }

    #[test]
    fn analysis_counts_sizes_keys_and_time_span() {
        let mut a = TopicAnalysis::default();
        a.add(&msg(0, Some(b"k1"), Some(10), 2_000));
        a.add(&msg(1, Some(b"k1"), Some(2_000), 1_000));
        a.add(&msg(1, None, None, 3_000));
        assert_eq!(a.records, 3);
        assert_eq!(a.partitions, BTreeMap::from([(0, 1), (1, 2)]));
        assert_eq!((a.null_keys, a.tombstones, a.distinct_keys), (1, 1, 1));
        assert_eq!(
            (a.values.min, a.values.max, a.values.total),
            (Some(10), Some(2_000), 2_010)
        );
        assert_eq!(a.values.average(), Some(1_005.0));
        assert_eq!(
            (a.first_timestamp_ms, a.last_timestamp_ms),
            (Some(1_000), Some(3_000))
        );
        // 10 B in the first bucket (<= 100 B), 2000 B in the third (<= 10 KB).
        assert_eq!(a.value_size_buckets[0], (Some(100), 1));
        assert_eq!(a.value_size_buckets[2], (Some(10_240), 1));
    }
}
