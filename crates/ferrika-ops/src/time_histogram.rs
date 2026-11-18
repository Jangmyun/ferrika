//! Messages over time: how many records arrived in each time bucket of a
//! topic, without reading them.
//!
//! For every bucket boundary the broker is asked which offset is the first
//! one at or after that time (ListOffsets by timestamp); the count of a
//! bucket is the offset difference between its two boundaries. The cost is
//! `boundaries × leader brokers` requests whatever the topic's size, and
//! partitions with nothing in a span are not asked about its inner
//! boundaries ([`search`]).
//!
//! Counts are offset ranges: transaction markers and aborted records are
//! included. On a compacted topic, offsets below the oldest kept record are
//! reported apart ([`TimeHistogram::removed_by_compaction`]), but records
//! compaction removed between kept ones still count
//! ([`NOTE_TRANSACTIONS`], [`NOTE_COMPACTED`]).

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::Serialize;

use ferrika_core::error::{ErrorClass, KafkaError, KafkaResult};
use ferrika_core::kafka::bounded_read::{read_ranges, BoundedReadOptions, PartitionRange};
use ferrika_core::kafka::config::KafkaClientConfig;
use ferrika_core::kafka::consumer::{ConsumedMessage, KafkaConsumerClient, Lookup, LookupFailure};

/// Bucket sizes to pick from, in milliseconds.
pub const BUCKET_CANDIDATES_MS: [i64; 19] = [
    1_000,
    5_000,
    10_000,
    15_000,
    30_000,
    60_000,
    120_000,
    300_000,
    600_000,
    900_000,
    1_800_000,
    3_600_000,
    7_200_000,
    10_800_000,
    21_600_000,
    43_200_000,
    86_400_000,
    172_800_000,
    604_800_000,
];
/// More buckets than this are refused.
pub const MAX_BUCKETS: usize = 500;
/// Partition lookups (`partitions × boundaries`) above this need consent
/// ([`CostPolicy`]).
pub const COST_LIMIT: usize = 200_000;
/// The fewest buckets [`CostPolicy::Coarsen`] goes down to.
pub const MIN_COARSE_BUCKETS: usize = 24;
/// Above this many partitions the per-partition numbers are left out.
pub const MAX_PER_PARTITION: usize = 256;
/// Timeout of one boundary lookup.
pub const LOOKUP_TIMEOUT: Duration = Duration::from_secs(10);
/// After this long the result is reported at the resolution reached.
pub const OVERALL_TIMEOUT: Duration = Duration::from_secs(60);

pub const NOTE_TRANSACTIONS: &str =
    "offset-based counts include transaction markers and aborted records";
pub const NOTE_COMPACTED: &str =
    "compacted topic: counts are offset ranges, so records compaction removed between kept ones still count (--exact counts records)";
pub const NOTE_CREATE_TIME: &str =
    "CreateTime: records are placed by position, so out-of-order producer times make it approximate";
pub const NOTE_LOG_APPEND_TIME: &str = "LogAppendTime: bucket edges are exact";
pub const NOTE_COARSENED: &str =
    "some spans could not be split in time; their records are counted as unknown";
pub const NOTE_NO_TIMESTAMPS: &str =
    "this topic has no timestamp index (records without timestamps); nothing to chart";

/// The time span to chart.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TimeRange {
    /// From this long ago until now.
    Since(Duration),
    /// From `.0` to `.1`, epoch milliseconds.
    Between(i64, i64),
    /// From the oldest retained record until now.
    All,
}

impl TimeRange {
    /// `(from, to)` in epoch milliseconds. [`TimeRange::All`] needs the
    /// oldest retained record's time.
    pub fn resolve(&self, now_ms: i64, earliest_ms: Option<i64>) -> Result<(i64, i64), String> {
        match self {
            TimeRange::Since(d) => Ok((now_ms - d.as_millis() as i64, now_ms)),
            TimeRange::Between(from, to) if from < to => Ok((*from, *to)),
            TimeRange::Between(..) => Err("the start must be before the end".to_string()),
            TimeRange::All => earliest_ms
                .map(|from| (from.min(now_ms - 1), now_ms))
                .ok_or_else(|| "the topic has no retained records to start from".to_string()),
        }
    }
}

/// How wide each bucket is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BucketSpec {
    /// The smallest candidate giving at most about `target` buckets.
    Auto {
        target: usize,
    },
    Fixed(Duration),
}

/// What to do when a chart would need more than [`COST_LIMIT`] lookups.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CostPolicy {
    /// Fail and say how to narrow it (the CLI without `--yes`).
    Refuse,
    /// Run anyway.
    Allow,
    /// Use wider buckets, down to [`MIN_COARSE_BUCKETS`] (the GUI).
    Coarsen,
}

#[derive(Debug, Clone)]
pub struct HistogramRequest {
    pub topic: String,
    /// `None`: every partition.
    pub partitions: Option<Vec<i32>>,
    pub range: TimeRange,
    pub bucket: BucketSpec,
    /// Buckets start at midnight of a zone this far ahead of UTC.
    pub tz_offset_ms: i64,
    pub cost: CostPolicy,
    /// Count from the records' own timestamps, reading at most this many.
    pub exact: Option<u64>,
}

/// How far a bucket's count can be trusted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BucketState {
    Complete,
    /// The bucket ends in the future: counts so far.
    InProgress,
    /// Before the oldest retained record: deleted, not empty.
    Trimmed,
    /// A boundary lookup failed or ran out of time.
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Bucket {
    pub start_ms: i64,
    pub count: u64,
    /// In [`TimeHistogram::partitions`] order; empty above
    /// [`MAX_PER_PARTITION`] partitions.
    pub by_partition: Vec<u64>,
    /// The first offset of the bucket per partition (-1: unknown), in
    /// [`TimeHistogram::partitions`] order; empty above
    /// [`MAX_PER_PARTITION`] partitions.
    pub start_offsets: Vec<i64>,
    pub state: BucketState,
}

/// Why the counts are not exact.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Approx {
    Compacted,
    CreateTime,
    Transactions,
    Coarsened,
}

impl Approx {
    pub fn note(self) -> &'static str {
        match self {
            Approx::Compacted => NOTE_COMPACTED,
            Approx::CreateTime => NOTE_CREATE_TIME,
            Approx::Transactions => NOTE_TRANSACTIONS,
            Approx::Coarsened => NOTE_COARSENED,
        }
    }
}

/// Where the last bucket ends.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EndKind {
    /// The last stable offset (read_committed, the default).
    Lso,
    HighWatermark,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TimeHistogram {
    pub topic: String,
    pub from_ms: i64,
    pub to_ms: i64,
    pub bucket_ms: i64,
    pub partitions: Vec<i32>,
    /// Selected partitions left out (no watermarks or no answer).
    pub unavailable: Vec<i32>,
    pub buckets: Vec<Bucket>,
    /// Records before the range.
    pub before: u64,
    /// Offsets of a compacted partition before its oldest retained record:
    /// removed by compaction, so neither before nor in the range.
    pub removed_by_compaction: u64,
    /// Records after the range (future timestamps included).
    pub after: u64,
    /// Records in the range whose bucket is unknown.
    pub unknown: u64,
    pub total_in_range: u64,
    /// `CreateTime` or `LogAppendTime`, when the config could be read.
    pub timestamp_type: Option<String>,
    pub approximate: Vec<Approx>,
    pub end_kind: EndKind,
    /// Counted from the records' own timestamps (`--exact`).
    pub exact: bool,
    /// The oldest retained record's time, when it could be read.
    pub earliest_retained_ms: Option<i64>,
    /// The topic has records but no timestamp index.
    pub no_timestamps: bool,
    /// An exact count: how many offsets it read (from the oldest kept
    /// record), to compare with the records found.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset_total: Option<u64>,
    /// An exact count that stopped early: why.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partial: Option<String>,
    /// Partitions whose oldest retained record may fall inside the range.
    #[serde(skip)]
    pub may_be_trimmed: Vec<i32>,
    /// The first boundary lookup that failed, when one did: why buckets are
    /// unknown or partitions unavailable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lookup_error: Option<String>,
}

impl TimeHistogram {
    /// The bucket with the most records.
    pub fn peak(&self) -> Option<&Bucket> {
        self.buckets
            .iter()
            .filter(|b| b.count > 0)
            .max_by_key(|b| (b.count, std::cmp::Reverse(b.start_ms)))
    }

    /// Runs of at least `min_buckets` empty buckets, as `(start, end)` ms.
    /// Deleted and unknown buckets are not gaps.
    pub fn gaps(&self, min_buckets: usize) -> Vec<(i64, i64)> {
        let mut gaps = Vec::new();
        let mut run: Option<(i64, usize)> = None;
        let empty = |b: &Bucket| {
            b.count == 0 && matches!(b.state, BucketState::Complete | BucketState::InProgress)
        };
        for b in &self.buckets {
            if empty(b) {
                let (start, n) = run.unwrap_or((b.start_ms, 0));
                run = Some((start, n + 1));
            } else if let Some((start, n)) = run.take() {
                if n >= min_buckets.max(1) {
                    gaps.push((start, b.start_ms));
                }
            }
        }
        if let Some((start, n)) = run {
            if n >= min_buckets.max(1) {
                gaps.push((start, self.to_ms));
            }
        }
        gaps
    }

    /// The end of bucket `i`.
    pub fn bucket_end(&self, i: usize) -> i64 {
        self.buckets
            .get(i + 1)
            .map(|b| b.start_ms)
            .unwrap_or(self.to_ms)
    }

    /// The notes to print under the chart.
    pub fn notes(&self) -> Vec<&'static str> {
        let mut notes: Vec<&'static str> = self.approximate.iter().map(|a| a.note()).collect();
        if self.timestamp_type.as_deref() == Some("LogAppendTime") && !self.exact {
            notes.push(NOTE_LOG_APPEND_TIME);
        }
        if self.no_timestamps {
            notes.push(NOTE_NO_TIMESTAMPS);
        }
        notes
    }

    /// Marks buckets that end before the oldest retained record as deleted.
    pub fn mark_trimmed(&mut self, earliest_ms: Option<i64>) {
        self.earliest_retained_ms = earliest_ms;
        let Some(earliest) = earliest_ms else {
            return;
        };
        let ends: Vec<i64> = (0..self.buckets.len())
            .map(|i| self.bucket_end(i))
            .collect();
        for (b, end) in self.buckets.iter_mut().zip(ends) {
            if end <= earliest && b.count == 0 && b.state == BucketState::Complete {
                b.state = BucketState::Trimmed;
            }
        }
    }
}

/// The bucket width for `span` milliseconds.
pub fn bucket_ms(spec: &BucketSpec, span_ms: i64) -> Result<i64, String> {
    match spec {
        BucketSpec::Fixed(d) => {
            let ms = d.as_millis() as i64;
            if ms <= 0 {
                return Err("the bucket must be longer than zero".to_string());
            }
            Ok(ms)
        }
        BucketSpec::Auto { target } => {
            let target = (*target).max(1) as i64;
            let want = (span_ms.max(1) + target - 1) / target;
            Ok(BUCKET_CANDIDATES_MS
                .iter()
                .copied()
                .find(|c| *c >= want)
                .unwrap_or(BUCKET_CANDIDATES_MS[BUCKET_CANDIDATES_MS.len() - 1]))
        }
    }
}

/// Bucket boundaries covering `[from, to]`, aligned to whole buckets in a
/// zone `tz_offset_ms` ahead of UTC (a day bucket starts at that zone's
/// midnight). `n` buckets give `n + 1` boundaries.
pub fn plan_boundaries(
    from_ms: i64,
    to_ms: i64,
    spec: &BucketSpec,
    tz_offset_ms: i64,
) -> Result<Vec<i64>, String> {
    if from_ms >= to_ms {
        return Err("the start must be before the end".to_string());
    }
    let width = bucket_ms(spec, to_ms - from_ms)?;
    let floor = |t: i64| (t + tz_offset_ms).div_euclid(width) * width - tz_offset_ms;
    let first = floor(from_ms);
    let mut last = floor(to_ms);
    if last < to_ms {
        last += width;
    }
    let n = ((last - first) / width) as usize;
    if n > MAX_BUCKETS {
        return Err(format!(
            "{n} buckets is more than {MAX_BUCKETS}: shorten the range or use wider buckets"
        ));
    }
    Ok((0..=n as i64).map(|i| first + i * width).collect())
}

/// Partition lookups a chart needs.
pub fn lookup_cost(partitions: usize, boundaries: usize) -> usize {
    partitions.saturating_mul(boundaries)
}

/// Boundaries for `[from, to]` within the cost policy; `true` when buckets
/// were widened to fit.
pub fn plan_within_cost(
    from_ms: i64,
    to_ms: i64,
    spec: &BucketSpec,
    tz_offset_ms: i64,
    partitions: usize,
    cost: CostPolicy,
) -> Result<(Vec<i64>, bool), String> {
    let boundaries = plan_boundaries(from_ms, to_ms, spec, tz_offset_ms)?;
    let needed = lookup_cost(partitions, boundaries.len());
    if needed <= COST_LIMIT || cost == CostPolicy::Allow {
        return Ok((boundaries, false));
    }
    if cost == CostPolicy::Refuse {
        return Err(format!(
            "{partitions} partitions × {} boundaries = {needed} lookups (limit {COST_LIMIT}): \
             use wider buckets, a shorter range or --partition, or pass --yes",
            boundaries.len()
        ));
    }
    let target = (COST_LIMIT / partitions.max(1))
        .saturating_sub(1)
        .max(MIN_COARSE_BUCKETS);
    let coarse = plan_boundaries(from_ms, to_ms, &BucketSpec::Auto { target }, tz_offset_ms)?;
    Ok((coarse, true))
}

/// Answers "first offset at or after this time" for some partitions.
pub trait TimeLookup {
    fn at(&mut self, partitions: &[i32], ts_ms: i64)
        -> Result<HashMap<i32, Lookup>, LookupFailure>;

    /// How long to wait before retry `attempt` (0-based) of a retriable
    /// failure: 0.5 s, 1 s, 2 s.
    fn backoff(&self, attempt: u32) -> Duration {
        ferrika_core::kafka::retry::backoff(attempt)
    }
}

/// [`TimeLookup`] through a group-less consumer.
pub struct ClientLookup<'a> {
    pub client: &'a KafkaConsumerClient,
    pub topic: &'a str,
    pub timeout: Duration,
}

impl TimeLookup for ClientLookup<'_> {
    fn at(
        &mut self,
        partitions: &[i32],
        ts_ms: i64,
    ) -> Result<HashMap<i32, Lookup>, LookupFailure> {
        let wanted: Vec<(&str, i32)> = partitions.iter().map(|&p| (self.topic, p)).collect();
        self.client
            .offsets_at_time(&wanted, ts_ms, Some(self.timeout))
            .map(|found| {
                found
                    .into_iter()
                    .filter(|((t, _), _)| t == self.topic)
                    .map(|((_, p), l)| (p, l))
                    .collect()
            })
            .map_err(|e| LookupFailure::from_error(&e))
    }
}

/// One boundary's answers for some partitions, see [`ask_boundary`].
#[derive(Debug, Default)]
struct Asked {
    found: HashMap<i32, Lookup>,
    /// The first failure, when one happened.
    error: Option<LookupFailure>,
    /// A failure no retry can fix (not allowed, or the topic is gone): the
    /// whole search stops.
    fatal: Option<LookupFailure>,
}

/// Asks boundary `ts_ms` for `partitions`. Failures Kafka documents as
/// retriable (leader elections, timeouts) are retried with backoff, up to
/// [`LOOKUP_TRIES`] tries; other failures leave the partition unanswered,
/// and authorization or unknown-topic failures stop the search.
fn ask_boundary(lookup: &mut dyn TimeLookup, partitions: &[i32], ts_ms: i64) -> Asked {
    let mut asked = Asked::default();
    let mut pending = partitions.to_vec();
    for attempt in 0..LOOKUP_TRIES {
        let result = lookup.at(&pending, ts_ms);
        let mut retry = Vec::new();
        for &p in &pending {
            let failure = match &result {
                Err(f) => f.clone(),
                Ok(answers) => match answers.get(&p) {
                    Some(l @ (Lookup::At(_) | Lookup::End)) => {
                        asked.found.insert(p, l.clone());
                        continue;
                    }
                    Some(Lookup::Failed(f)) => f.clone(),
                    None => LookupFailure::new(
                        format!("the broker gave no answer for partition {p}"),
                        ErrorClass::Other,
                    ),
                },
            };
            match failure.class {
                ErrorClass::Denied | ErrorClass::Gone => {
                    asked.error.get_or_insert_with(|| failure.clone());
                    asked.fatal = Some(failure);
                    return asked;
                }
                ErrorClass::Retriable => retry.push((p, failure)),
                ErrorClass::Other => {
                    asked.error.get_or_insert(failure);
                }
            }
        }
        // A retriable failure counts only once no retry is left: one a
        // later try answered leaves no bucket unknown.
        if retry.is_empty() || attempt + 1 == LOOKUP_TRIES {
            if let Some((_, failure)) = retry.into_iter().next() {
                asked.error.get_or_insert(failure);
            }
            break;
        }
        std::thread::sleep(lookup.backoff(attempt));
        pending = retry.into_iter().map(|(p, _)| p).collect();
    }
    asked
}

/// Boundary answers per partition: `slots[partition index][boundary index]`,
/// `None` where unknown (failed, or not asked in time).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Search {
    pub slots: Vec<Vec<Option<Lookup>>>,
    /// The deadline stopped the search before every span was split.
    pub coarsened: bool,
    pub lookups: usize,
    /// The first failed lookup's message.
    pub error: Option<String>,
    /// A failure that stopped the search (see `ask_boundary`).
    pub fatal: Option<LookupFailure>,
}

/// Asks each boundary of `boundaries` for `partitions`, coarse to fine: the
/// two ends first, then the middle of every span whose ends differ. A
/// partition whose ends of a span are equal has no records in it, so the
/// inner boundaries are not asked. A retriable failure is retried with
/// backoff; still failing, or failing otherwise, the boundary stays
/// unknown. Authorization and unknown-topic failures stop the search.
pub fn search(
    lookup: &mut dyn TimeLookup,
    boundaries: &[i64],
    partitions: &[i32],
    deadline: Instant,
    progress: &mut dyn FnMut(usize, usize),
) -> Search {
    let last = boundaries.len().saturating_sub(1);
    let mut slots: Vec<Vec<Option<Lookup>>> = vec![vec![None; boundaries.len()]; partitions.len()];
    let total = partitions.len() * boundaries.len();
    let mut done = 0usize;
    let mut coarsened = false;
    let mut error: Option<String> = None;
    let mut fatal: Option<LookupFailure> = None;
    let all: Vec<usize> = (0..partitions.len()).collect();

    // Asks one boundary; false once a fatal failure stops the search.
    let mut ask = |idx: usize, who: &[usize], slots: &mut [Vec<Option<Lookup>>]| -> bool {
        let ids: Vec<i32> = who.iter().map(|&i| partitions[i]).collect();
        let asked = ask_boundary(lookup, &ids, boundaries[idx]);
        for &i in who {
            slots[i][idx] = asked.found.get(&partitions[i]).cloned();
        }
        if let Some(e) = asked.error {
            error.get_or_insert(e.message);
        }
        done += who.len();
        progress(done, total);
        match asked.fatal {
            Some(f) => {
                fatal = Some(f);
                false
            }
            None => true,
        }
    };

    let mut go = ask(0, &all, &mut slots);
    if go && last > 0 {
        go = ask(last, &all, &mut slots);
    }

    // Spans still to split, with the partitions that have records in them.
    let mut spans: Vec<(usize, usize, Vec<usize>)> = Vec::new();
    if go && last > 1 {
        let busy = split_or_fill(&mut slots, &all, 0, last);
        if !busy.is_empty() {
            spans.push((0, last, busy));
        }
    }
    while !spans.is_empty() {
        if Instant::now() >= deadline {
            coarsened = true;
            break;
        }
        let mut next = Vec::new();
        for (lo, hi, who) in spans {
            let mid = lo + (hi - lo) / 2;
            if !ask(mid, &who, &mut slots) {
                next.clear();
                break;
            }
            for (a, b) in [(lo, mid), (mid, hi)] {
                if b - a < 2 {
                    continue;
                }
                let busy = split_or_fill(&mut slots, &who, a, b);
                if !busy.is_empty() {
                    next.push((a, b, busy));
                }
            }
        }
        spans = next;
    }
    Search {
        slots,
        coarsened,
        lookups: done,
        error,
        fatal,
    }
}

/// The error for a search that a failure stopped.
fn fatal_error(topic: &str, failure: LookupFailure) -> KafkaError {
    match failure.class {
        ErrorClass::Denied => KafkaError::Acl(failure.message),
        _ => KafkaError::Consumer(format!(
            "looking up offsets of '{topic}' failed: {}",
            failure.message
        )),
    }
}

/// For the span `(a, b)`: partitions whose two ends are known and equal get
/// the same answer on every inner boundary (no records); returns the ones
/// whose ends differ. A partition with an unknown end is left alone.
fn split_or_fill(
    slots: &mut [Vec<Option<Lookup>>],
    who: &[usize],
    a: usize,
    b: usize,
) -> Vec<usize> {
    let mut busy = Vec::new();
    for &i in who {
        match (&slots[i][a], &slots[i][b]) {
            (Some(x), Some(y)) if x == y => {
                let same = Some(x.clone());
                for slot in &mut slots[i][a + 1..b] {
                    *slot = same.clone();
                }
            }
            (Some(_), Some(_)) => busy.push(i),
            _ => {}
        }
    }
    busy
}

/// Counts from boundary answers. `marks[i]` is partition `i`'s
/// `(low, high)`: the low watermark read before the lookups and the high
/// watermark read after them. Answers are clamped to `[low, high]` and made
/// non-decreasing, so retention or new records during the lookups never
/// give a negative count. `End` means `high`.
pub fn assemble(
    topic: &str,
    boundaries: &[i64],
    partitions: &[i32],
    slots: &[Vec<Option<Lookup>>],
    marks: &[(i64, i64)],
    now_ms: i64,
) -> TimeHistogram {
    assemble_retained(topic, boundaries, partitions, slots, marks, &[], now_ms)
}

/// [`assemble`] for a compacted topic: `retained[i]` is partition `i`'s
/// oldest retained record (the first offset at or after time 0), when
/// known. The offsets between the low watermark and it were removed by
/// compaction: they are counted as [`TimeHistogram::removed_by_compaction`],
/// not as before the range.
pub fn assemble_retained(
    topic: &str,
    boundaries: &[i64],
    partitions: &[i32],
    slots: &[Vec<Option<Lookup>>],
    marks: &[(i64, i64)],
    retained: &[Option<i64>],
    now_ms: i64,
) -> TimeHistogram {
    let n = boundaries.len().saturating_sub(1);
    let last = n;
    // Partitions with both ends known are charted; the rest are unavailable.
    let mut kept = Vec::new();
    let mut unavailable = Vec::new();
    for (i, &p) in partitions.iter().enumerate() {
        let ends_known = matches!(
            (slots[i].first(), slots[i].get(last)),
            (Some(Some(_)), Some(Some(_)))
        );
        if ends_known {
            kept.push(i);
        } else {
            unavailable.push(p);
        }
    }
    let per_partition = kept.len() <= MAX_PER_PARTITION;
    let mut buckets: Vec<Bucket> = (0..n)
        .map(|i| Bucket {
            start_ms: boundaries[i],
            count: 0,
            by_partition: if per_partition {
                vec![0; kept.len()]
            } else {
                Vec::new()
            },
            start_offsets: if per_partition {
                vec![-1; kept.len()]
            } else {
                Vec::new()
            },
            state: if boundaries[i + 1] > now_ms {
                BucketState::InProgress
            } else {
                BucketState::Complete
            },
        })
        .collect();
    let (mut before, mut after, mut unknown) = (0u64, 0u64, 0u64);
    let mut removed = 0u64;
    let mut may_be_trimmed = Vec::new();

    for (col, &i) in kept.iter().enumerate() {
        let (low, high) = marks[i];
        let high = high.max(low);
        let mut floor = low;
        let values: Vec<Option<i64>> = slots[i]
            .iter()
            .map(|slot| {
                let raw = match slot {
                    Some(Lookup::At(o)) => *o,
                    Some(Lookup::End) => high,
                    _ => return None,
                };
                floor = raw.clamp(low, high).max(floor);
                Some(floor)
            })
            .collect();
        let (Some(first), Some(end)) = (values[0], values[last]) else {
            continue;
        };
        let oldest = retained
            .get(i)
            .copied()
            .flatten()
            .map_or(low, |o| o.clamp(low, first));
        removed += (oldest - low) as u64;
        before += (first - oldest) as u64;
        after += (high - end) as u64;
        // Only a log start above 0 means records were deleted (retention or
        // DeleteRecords); from offset 0 nothing was, and the buckets before
        // the first record are empty.
        if low > 0 && first == low && high > low {
            may_be_trimmed.push(partitions[i]);
        }
        let mut b = 0;
        while b < n {
            let Some(start) = values[b] else {
                b += 1;
                continue;
            };
            if per_partition {
                buckets[b].start_offsets[col] = start;
            }
            let next = (b + 1..=n).find(|&j| values[j].is_some()).unwrap_or(n);
            let Some(stop) = values[next] else {
                break;
            };
            let count = (stop - start) as u64;
            if next == b + 1 {
                buckets[b].count += count;
                if per_partition {
                    buckets[b].by_partition[col] += count;
                }
            } else {
                unknown += count;
                for bucket in &mut buckets[b..next] {
                    bucket.state = BucketState::Unknown;
                }
            }
            b = next;
        }
    }
    let total_in_range = buckets.iter().map(|b| b.count).sum::<u64>() + unknown;
    TimeHistogram {
        topic: topic.to_string(),
        from_ms: boundaries.first().copied().unwrap_or_default(),
        to_ms: boundaries.last().copied().unwrap_or_default(),
        bucket_ms: boundaries
            .get(1)
            .zip(boundaries.first())
            .map(|(b, a)| b - a)
            .unwrap_or_default(),
        partitions: kept.iter().map(|&i| partitions[i]).collect(),
        unavailable,
        buckets,
        before,
        removed_by_compaction: removed,
        after,
        unknown,
        total_in_range,
        timestamp_type: None,
        approximate: vec![Approx::Transactions],
        end_kind: EndKind::Lso,
        exact: false,
        earliest_retained_ms: None,
        no_timestamps: false,
        offset_total: None,
        partial: None,
        may_be_trimmed,
        lookup_error: None,
    }
}

/// What is known about the topic before counting.
#[derive(Debug, Clone, Default)]
pub struct TopicFacts {
    pub timestamp_type: Option<String>,
    pub compacted: bool,
    /// The oldest retained record's time per partition (needs READ).
    pub earliest_ms: BTreeMap<i32, i64>,
}

impl TopicFacts {
    pub fn earliest(&self) -> Option<i64> {
        self.earliest_ms.values().copied().min()
    }
}

/// Reads the timestamp type and cleanup policy (best effort) and, when
/// `with_earliest`, the oldest retained record's time per partition.
pub async fn topic_facts(
    config: &KafkaClientConfig,
    topic: &str,
    partitions: &[i32],
    with_earliest: bool,
) -> TopicFacts {
    let mut facts = TopicFacts::default();
    if let Ok(admin) = ferrika_core::kafka::admin::KafkaAdminClient::new(config) {
        if let Ok(entries) = admin.describe_topic_configs(topic, None).await {
            for e in entries {
                match e.name.as_str() {
                    "message.timestamp.type" => facts.timestamp_type = Some(e.value),
                    "cleanup.policy" => facts.compacted = e.value.contains("compact"),
                    _ => {}
                }
            }
        }
    }
    if with_earliest {
        facts.earliest_ms = earliest_timestamps(config, topic, partitions).await;
    }
    facts
}

/// The partitions to chart: all of `topic`'s, or the requested ones.
pub fn selected_partitions(
    client: &KafkaConsumerClient,
    topic: &str,
    only: Option<&[i32]>,
) -> KafkaResult<Vec<i32>> {
    let all = client.topic_partitions(topic, None)?;
    match only {
        None => Ok(all),
        Some(wanted) => {
            for &partition in wanted {
                if !all.contains(&partition) {
                    return Err(KafkaError::PartitionNotFound {
                        topic: topic.to_string(),
                        partition,
                    });
                }
            }
            let mut wanted = wanted.to_vec();
            wanted.sort_unstable();
            wanted.dedup();
            Ok(wanted)
        }
    }
}

/// Counts records per bucket from boundary lookups (blocking).
pub fn compute(
    config: &KafkaClientConfig,
    req: &HistogramRequest,
    facts: &TopicFacts,
    now_ms: i64,
    progress: &mut dyn FnMut(usize, usize),
) -> KafkaResult<TimeHistogram> {
    let client = KafkaConsumerClient::lookup(config)?;
    let selected = selected_partitions(&client, &req.topic, req.partitions.as_deref())?;
    let (from, to) = req
        .range
        .resolve(now_ms, facts.earliest())
        .map_err(KafkaError::Check)?;

    let wanted: Vec<(&str, i32)> = selected.iter().map(|&p| (req.topic.as_str(), p)).collect();
    let first_marks = retry_metadata(|| client.fetch_watermarks_batch(&wanted, None))?;
    let mark =
        |m: &HashMap<(String, i32), (i64, i64)>, p: i32| m.get(&(req.topic.clone(), p)).copied();
    let (readable, mut unavailable): (Vec<i32>, Vec<i32>) = selected
        .iter()
        .partition(|&&p| mark(&first_marks, p).is_some());

    let (boundaries, widened) = plan_within_cost(
        from,
        to,
        &req.bucket,
        req.tz_offset_ms,
        readable.len(),
        req.cost,
    )
    .map_err(KafkaError::Check)?;

    let mut lookup = ClientLookup {
        client: &client,
        topic: &req.topic,
        timeout: LOOKUP_TIMEOUT,
    };
    let found = search(
        &mut lookup,
        &boundaries,
        &readable,
        Instant::now() + OVERALL_TIMEOUT,
        progress,
    );
    if let Some(failure) = found.fatal {
        return Err(fatal_error(&req.topic, failure));
    }

    let retained = if facts.compacted {
        oldest_retained(&mut lookup, &readable)
    } else {
        Vec::new()
    };

    let last_marks = client
        .fetch_watermarks_batch(&wanted, None)
        .unwrap_or_default();
    let marks: Vec<(i64, i64)> = readable
        .iter()
        .map(|&p| {
            let (low, high0) = mark(&first_marks, p).unwrap_or_default();
            let high = mark(&last_marks, p).map(|(_, h)| h).unwrap_or(high0);
            (low, high.max(high0))
        })
        .collect();
    let mut hist = assemble_retained(
        &req.topic,
        &boundaries,
        &readable,
        &found.slots,
        &marks,
        &retained,
        now_ms,
    );
    hist.unavailable.append(&mut unavailable);
    hist.unavailable.sort_unstable();
    hist.lookup_error = found.error;

    // Nothing in or after the range, yet records exist: is there a
    // timestamp index at all?
    let has_records = marks.iter().any(|(low, high)| high > low);
    if has_records && hist.total_in_range == 0 && hist.after == 0 && facts.earliest().is_none() {
        let mut lookup = ClientLookup {
            client: &client,
            topic: &req.topic,
            timeout: LOOKUP_TIMEOUT,
        };
        let with_records: Vec<i32> = readable
            .iter()
            .zip(&marks)
            .filter(|(_, (low, high))| high > low)
            .map(|(&p, _)| p)
            .collect();
        if let Ok(answers) = lookup.at(&with_records, 0) {
            hist.no_timestamps = with_records
                .iter()
                .all(|p| matches!(answers.get(p), Some(Lookup::End)));
        }
    }

    finish(&mut hist, facts, config, widened || found.coarsened);
    Ok(hist)
}

/// Tries of [`retry_metadata`] and of one boundary lookup.
const LOOKUP_TRIES: u32 = ferrika_core::kafka::retry::TRIES;

use ferrika_core::kafka::retry::retry_metadata;

/// The oldest retained record of each partition: the first offset at or
/// after time 0 (asked twice before giving up). On a compacted topic the
/// offsets below it were removed by compaction.
fn oldest_retained(lookup: &mut dyn TimeLookup, partitions: &[i32]) -> Vec<Option<i64>> {
    let answers = ask_boundary(lookup, partitions, 0).found;
    partitions
        .iter()
        .map(|p| match answers.get(p) {
            Some(Lookup::At(o)) => Some(*o),
            _ => None,
        })
        .collect()
}

/// Fills in what the counting itself does not know.
fn finish(hist: &mut TimeHistogram, facts: &TopicFacts, config: &KafkaClientConfig, coarse: bool) {
    hist.timestamp_type = facts.timestamp_type.clone();
    let mut approx = Vec::new();
    if facts.compacted {
        approx.push(Approx::Compacted);
    }
    if facts.timestamp_type.as_deref() != Some("LogAppendTime") {
        approx.push(Approx::CreateTime);
    }
    approx.push(Approx::Transactions);
    if coarse || hist.unknown > 0 {
        approx.push(Approx::Coarsened);
    }
    hist.approximate = approx;
    hist.end_kind = end_kind(config);
    hist.mark_trimmed(facts.earliest());
}

/// Where reads end: the LSO unless the client reads uncommitted records.
fn end_kind(config: &KafkaClientConfig) -> EndKind {
    let uncommitted = config
        .extra_properties
        .get("isolation.level")
        .is_some_and(|v| v == "read_uncommitted");
    if uncommitted {
        EndKind::HighWatermark
    } else {
        EndKind::Lso
    }
}

/// Reads what is needed about the topic, then counts on a blocking thread.
pub async fn run(
    config: &KafkaClientConfig,
    req: HistogramRequest,
    progress: impl FnMut(usize, usize) + Send + 'static,
) -> KafkaResult<TimeHistogram> {
    let partitions = {
        let config = config.clone();
        let topic = req.topic.clone();
        let only = req.partitions.clone();
        tokio::task::spawn_blocking(move || {
            let client = KafkaConsumerClient::lookup(&config)?;
            selected_partitions(&client, &topic, only.as_deref())
        })
        .await
        .map_err(|e| KafkaError::Consumer(e.to_string()))??
    };
    let facts = topic_facts(config, &req.topic, &partitions, req.range == TimeRange::All).await;
    if let Some(limit) = req.exact {
        return run_exact(config, req, facts, limit, progress).await;
    }
    let mut hist = {
        let config = config.clone();
        let mut progress = progress;
        tokio::task::spawn_blocking(move || compute(&config, &req, &facts, now_ms(), &mut progress))
            .await
            .map_err(|e| KafkaError::Consumer(e.to_string()))??
    };
    mark_trimmed_inside(config, &mut hist).await;
    Ok(hist)
}

/// Default record limit of an exact count (`--limit`).
pub const EXACT_LIMIT: u64 = 1_000_000;

/// Records per bucket from their own timestamps, for an exact count.
#[derive(Debug, Clone, Default)]
pub struct ExactBins {
    boundaries: Vec<i64>,
    /// Partition id -> column.
    columns: HashMap<i32, usize>,
    /// `counts[bucket][column]`.
    counts: Vec<Vec<u64>>,
    /// The first offset seen per bucket and column (-1: none).
    first_offsets: Vec<Vec<i64>>,
    /// Records read whose time falls before or after the range.
    before: u64,
    after: u64,
    /// Records read without a timestamp.
    no_time: u64,
    seen: u64,
}

impl ExactBins {
    pub fn new(boundaries: &[i64], partitions: &[i32]) -> Self {
        let n = boundaries.len().saturating_sub(1);
        Self {
            boundaries: boundaries.to_vec(),
            columns: partitions
                .iter()
                .enumerate()
                .map(|(c, &p)| (p, c))
                .collect(),
            counts: vec![vec![0; partitions.len()]; n],
            first_offsets: vec![vec![-1; partitions.len()]; n],
            ..Self::default()
        }
    }

    /// Counts one record. Only its partition, offset and time are looked at.
    pub fn add(&mut self, partition: i32, offset: i64, timestamp_ms: Option<i64>) {
        self.seen += 1;
        let (Some(&col), Some(&first), Some(&last)) = (
            self.columns.get(&partition),
            self.boundaries.first(),
            self.boundaries.last(),
        ) else {
            return;
        };
        let Some(ts) = timestamp_ms.filter(|t| *t >= 0) else {
            self.no_time += 1;
            return;
        };
        if ts < first {
            self.before += 1;
            return;
        }
        if ts >= last {
            self.after += 1;
            return;
        }
        let b = self.boundaries.partition_point(|&x| x <= ts) - 1;
        self.counts[b][col] += 1;
        let seen = &mut self.first_offsets[b][col];
        if *seen < 0 || offset < *seen {
            *seen = offset;
        }
    }
}

/// The offset ranges an exact count reads: from the first record at or
/// after the range start to the first at or after its end.
#[derive(Debug, Clone)]
struct ExactPlan {
    boundaries: Vec<i64>,
    partitions: Vec<i32>,
    unavailable: Vec<i32>,
    ranges: Vec<PartitionRange>,
    /// Records before and after the ranges, by offset.
    before: u64,
    after: u64,
    /// Offsets removed by compaction before the oldest retained record.
    removed: u64,
    widened: bool,
    /// Partitions whose oldest retained record may fall inside the range.
    may_be_trimmed: Vec<i32>,
    /// The first failed boundary lookup's message.
    lookup_error: Option<String>,
}

fn plan_exact(
    config: &KafkaClientConfig,
    req: &HistogramRequest,
    facts: &TopicFacts,
    now_ms: i64,
) -> KafkaResult<ExactPlan> {
    let client = KafkaConsumerClient::lookup(config)?;
    let selected = selected_partitions(&client, &req.topic, req.partitions.as_deref())?;
    let (from, to) = req
        .range
        .resolve(now_ms, facts.earliest())
        .map_err(KafkaError::Check)?;
    let wanted: Vec<(&str, i32)> = selected.iter().map(|&p| (req.topic.as_str(), p)).collect();
    let marks = retry_metadata(|| client.fetch_watermarks_batch(&wanted, None))?;
    let mut unavailable: Vec<i32> = Vec::new();
    let mut readable: Vec<(i32, (i64, i64))> = Vec::new();
    for &p in &selected {
        match marks.get(&(req.topic.clone(), p)) {
            Some(&mark) => readable.push((p, mark)),
            None => unavailable.push(p),
        }
    }
    let ids: Vec<i32> = readable.iter().map(|(p, _)| *p).collect();
    let (boundaries, widened) =
        plan_within_cost(from, to, &req.bucket, req.tz_offset_ms, ids.len(), req.cost)
            .map_err(KafkaError::Check)?;
    let ends = [boundaries[0], boundaries[boundaries.len() - 1]];
    let mut lookup = ClientLookup {
        client: &client,
        topic: &req.topic,
        timeout: LOOKUP_TIMEOUT,
    };
    let found = search(
        &mut lookup,
        &ends,
        &ids,
        Instant::now() + OVERALL_TIMEOUT,
        &mut |_, _| {},
    );
    if let Some(failure) = found.fatal {
        return Err(fatal_error(&req.topic, failure));
    }
    let retained = if facts.compacted {
        oldest_retained(&mut lookup, &ids)
    } else {
        Vec::new()
    };
    let mut plan = ExactPlan {
        boundaries,
        partitions: Vec::new(),
        unavailable,
        ranges: Vec::new(),
        before: 0,
        after: 0,
        removed: 0,
        widened,
        may_be_trimmed: Vec::new(),
        lookup_error: found.error,
    };
    for (i, &(p, (low, high))) in readable.iter().enumerate() {
        let resolve = |slot: &Option<Lookup>| match slot {
            Some(Lookup::At(o)) => Some((*o).clamp(low, high)),
            Some(Lookup::End) => Some(high),
            _ => None,
        };
        let (Some(start), Some(end)) = (resolve(&found.slots[i][0]), resolve(&found.slots[i][1]))
        else {
            plan.unavailable.push(p);
            continue;
        };
        let end = end.max(start);
        let oldest = retained
            .get(i)
            .copied()
            .flatten()
            .map_or(low, |o| o.clamp(low, start));
        plan.removed += (oldest - low) as u64;
        plan.before += (start - oldest) as u64;
        if low > 0 && start == low && high > low {
            plan.may_be_trimmed.push(p);
        }
        plan.after += (high - end) as u64;
        plan.partitions.push(p);
        plan.ranges.push(PartitionRange {
            topic: req.topic.clone(),
            partition: p,
            start,
            end,
        });
    }
    plan.unavailable.sort_unstable();
    Ok(plan)
}

/// The record filter of an exact count: it counts each record into `bins`
/// and keeps none, so nothing is held in memory.
fn exact_keep(
    bins: Arc<Mutex<ExactBins>>,
    total: usize,
    progress: Arc<Mutex<dyn FnMut(usize, usize) + Send>>,
) -> impl Fn(&ConsumedMessage) -> bool + Send + Sync + 'static {
    move |m: &ConsumedMessage| {
        if let Ok(mut bins) = bins.lock() {
            bins.add(m.partition, m.offset, m.timestamp_ms);
            if bins.seen.is_multiple_of(10_000) {
                if let Ok(mut progress) = progress.lock() {
                    progress(bins.seen as usize, total);
                }
            }
        }
        false
    }
}

/// A histogram from an exact count. `offset_total` is the size of the
/// offset ranges read, for comparison with the offset-based count.
fn exact_histogram(
    topic: &str,
    plan: &ExactPlan,
    bins: &ExactBins,
    offset_total: u64,
    now_ms: i64,
) -> TimeHistogram {
    let per_partition = plan.partitions.len() <= MAX_PER_PARTITION;
    let n = plan.boundaries.len().saturating_sub(1);
    let buckets: Vec<Bucket> = (0..n)
        .map(|b| {
            let counts = bins.counts.get(b).cloned().unwrap_or_default();
            Bucket {
                start_ms: plan.boundaries[b],
                count: counts.iter().sum(),
                by_partition: if per_partition { counts } else { Vec::new() },
                start_offsets: if per_partition {
                    bins.first_offsets.get(b).cloned().unwrap_or_default()
                } else {
                    Vec::new()
                },
                state: if plan.boundaries[b + 1] > now_ms {
                    BucketState::InProgress
                } else {
                    BucketState::Complete
                },
            }
        })
        .collect();
    let total_in_range = buckets.iter().map(|b| b.count).sum::<u64>() + bins.no_time;
    TimeHistogram {
        topic: topic.to_string(),
        from_ms: plan.boundaries.first().copied().unwrap_or_default(),
        to_ms: plan.boundaries.last().copied().unwrap_or_default(),
        bucket_ms: plan
            .boundaries
            .get(1)
            .zip(plan.boundaries.first())
            .map(|(b, a)| b - a)
            .unwrap_or_default(),
        partitions: plan.partitions.clone(),
        unavailable: plan.unavailable.clone(),
        buckets,
        before: plan.before + bins.before,
        removed_by_compaction: plan.removed,
        after: plan.after + bins.after,
        unknown: bins.no_time,
        total_in_range,
        timestamp_type: None,
        approximate: if plan.widened {
            vec![Approx::Coarsened]
        } else {
            Vec::new()
        },
        end_kind: EndKind::Lso,
        exact: true,
        earliest_retained_ms: None,
        no_timestamps: false,
        offset_total: Some(offset_total),
        partial: None,
        may_be_trimmed: plan.may_be_trimmed.clone(),
        lookup_error: plan.lookup_error.clone(),
    }
}

/// Counts from the records' own timestamps: reads every record in the
/// range (timestamps only), refusing more than `limit` records.
async fn run_exact(
    config: &KafkaClientConfig,
    req: HistogramRequest,
    facts: TopicFacts,
    limit: u64,
    progress: impl FnMut(usize, usize) + Send + 'static,
) -> KafkaResult<TimeHistogram> {
    let now = now_ms();
    let plan = {
        let (config, req, facts) = (config.clone(), req.clone(), facts.clone());
        tokio::task::spawn_blocking(move || plan_exact(&config, &req, &facts, now))
            .await
            .map_err(|e| KafkaError::Consumer(e.to_string()))??
    };
    let total: u64 = plan
        .ranges
        .iter()
        .map(|r| (r.end - r.start).max(0) as u64)
        .sum();
    if total > limit {
        return Err(KafkaError::Check(format!(
            "{total} records in the range is more than the limit of {limit}: \
             shorten the range or raise --limit"
        )));
    }
    let bins = Arc::new(Mutex::new(ExactBins::new(
        &plan.boundaries,
        &plan.partitions,
    )));
    let progress: Arc<Mutex<dyn FnMut(usize, usize) + Send>> = Arc::new(Mutex::new(progress));
    let keep = exact_keep(bins.clone(), total as usize, progress);
    let read = read_ranges(config, &plan.ranges, &BoundedReadOptions::default(), keep).await?;
    let bins = bins.lock().map(|b| b.clone()).unwrap_or_default();
    let mut hist = exact_histogram(&req.topic, &plan, &bins, total, now);
    hist.partial = read.partial_warning(&req.topic, false);
    hist.timestamp_type = facts.timestamp_type.clone();
    hist.end_kind = end_kind(config);
    hist.mark_trimmed(facts.earliest());
    mark_trimmed_inside(config, &mut hist).await;
    Ok(hist)
}

/// No partition has records before the range: the oldest retained record
/// may be inside it, and the buckets before it are deleted, not empty.
async fn mark_trimmed_inside(config: &KafkaClientConfig, hist: &mut TimeHistogram) {
    if hist.before == 0 && hist.earliest_retained_ms.is_none() && !hist.may_be_trimmed.is_empty() {
        let earliest = earliest_timestamps(config, &hist.topic, &hist.may_be_trimmed).await;
        hist.mark_trimmed(earliest.values().copied().min());
    }
}

/// The oldest retained record's time per partition (reads one record of
/// each; needs READ). Partitions that could not be read are left out.
pub async fn earliest_timestamps(
    config: &KafkaClientConfig,
    topic: &str,
    partitions: &[i32],
) -> BTreeMap<i32, i64> {
    let positions: Vec<(String, i32, i64)> = partitions
        .iter()
        .map(|&p| (topic.to_string(), p, -1))
        .collect();
    ferrika_core::kafka::consumer::record_timestamps(config, &positions, Duration::from_secs(10))
        .await
        .map(|found| found.into_iter().map(|((_, p), ts)| (p, ts)).collect())
        .unwrap_or_default()
}

pub use ferrika_core::kafka::clock::now_ms;

#[cfg(test)]
mod tests {
    use super::*;

    const H: i64 = 3_600_000;

    /// A partition's records by timestamp; `End` past the last one.
    struct Fake {
        records: HashMap<i32, Vec<i64>>,
        calls: usize,
        fail_once: Option<i64>,
    }

    impl Fake {
        fn new(records: &[(i32, &[i64])]) -> Self {
            Self {
                records: records.iter().map(|(p, ts)| (*p, ts.to_vec())).collect(),
                calls: 0,
                fail_once: None,
            }
        }
    }

    impl TimeLookup for Fake {
        fn backoff(&self, _: u32) -> Duration {
            Duration::ZERO
        }

        fn at(
            &mut self,
            partitions: &[i32],
            ts: i64,
        ) -> Result<HashMap<i32, Lookup>, LookupFailure> {
            self.calls += 1;
            if self.fail_once == Some(ts) {
                self.fail_once = None;
                return Err(LookupFailure::new(
                    "Request timed out",
                    ErrorClass::Retriable,
                ));
            }
            Ok(partitions
                .iter()
                .map(|p| {
                    let recs = &self.records[p];
                    let l = recs
                        .iter()
                        .position(|t| *t >= ts)
                        .map(|o| Lookup::At(o as i64))
                        .unwrap_or(Lookup::End);
                    (*p, l)
                })
                .collect())
        }
    }

    /// Answers each call with the next scripted result.
    struct Scripted {
        answers: Vec<Result<HashMap<i32, Lookup>, LookupFailure>>,
        calls: usize,
    }

    impl TimeLookup for Scripted {
        fn at(&mut self, _: &[i32], _: i64) -> Result<HashMap<i32, Lookup>, LookupFailure> {
            self.calls += 1;
            if self.answers.is_empty() {
                return Ok(HashMap::new());
            }
            self.answers.remove(0)
        }

        fn backoff(&self, _: u32) -> Duration {
            Duration::ZERO
        }
    }

    fn failed(message: &str, class: ErrorClass) -> LookupFailure {
        LookupFailure::new(message, class)
    }

    #[test]
    fn an_authorization_failure_stops_the_search() {
        let denied = failed("Broker: Topic authorization failed", ErrorClass::Denied);
        let mut lookup = Scripted {
            answers: vec![Ok(HashMap::from([(0, Lookup::Failed(denied.clone()))]))],
            calls: 0,
        };
        let b: Vec<i64> = (0..=8).map(|i| i * 10).collect();
        let found = search(&mut lookup, &b, &[0], far(), &mut |_, _| {});
        assert_eq!(lookup.calls, 1, "not retried, nothing more asked");
        assert_eq!(found.fatal, Some(denied.clone()));
        assert!(matches!(fatal_error("t", denied), KafkaError::Acl(_)));
    }

    #[test]
    fn other_failures_are_kept_and_not_retried() {
        let odd = failed("Broker: Invalid request", ErrorClass::Other);
        let mut lookup = Scripted {
            answers: vec![Err(odd), Ok(HashMap::from([(0, Lookup::End)]))],
            calls: 0,
        };
        let found = search(&mut lookup, &[0, 10], &[0], far(), &mut |_, _| {});
        assert_eq!(lookup.calls, 2, "one call per boundary, no retry");
        assert_eq!(found.slots[0][0], None);
        assert_eq!(found.slots[0][1], Some(Lookup::End));
        assert_eq!(found.error.as_deref(), Some("Broker: Invalid request"));
        assert!(found.fatal.is_none());
    }

    #[test]
    fn retriable_failures_are_retried_a_bounded_number_of_times() {
        let busy = || failed("Broker: Not leader for partition", ErrorClass::Retriable);
        let mut lookup = Scripted {
            answers: (0..10).map(|_| Err(busy())).collect(),
            calls: 0,
        };
        let asked = ask_boundary(&mut lookup, &[0, 1], 5);
        assert_eq!(lookup.calls, LOOKUP_TRIES as usize);
        assert!(asked.found.is_empty());
        assert_eq!(asked.error, Some(busy()));
        // A partition answered on a retry is kept.
        let mut lookup = Scripted {
            answers: vec![
                Ok(HashMap::from([
                    (0, Lookup::At(3)),
                    (1, Lookup::Failed(busy())),
                ])),
                Ok(HashMap::from([(1, Lookup::At(4))])),
            ],
            calls: 0,
        };
        let asked = ask_boundary(&mut lookup, &[0, 1], 5);
        assert_eq!(lookup.calls, 2);
        assert_eq!(asked.found.len(), 2);
        // Answered on a retry: no failure is left to report.
        assert_eq!(asked.error, None);
    }

    fn far() -> Instant {
        Instant::now() + Duration::from_secs(3600)
    }

    fn chart(fake: &mut Fake, boundaries: &[i64], parts: &[i32]) -> TimeHistogram {
        let found = search(fake, boundaries, parts, far(), &mut |_, _| {});
        let marks: Vec<(i64, i64)> = parts
            .iter()
            .map(|p| (0, fake.records[p].len() as i64))
            .collect();
        assemble("t", boundaries, parts, &found.slots, &marks, i64::MAX)
    }

    #[test]
    fn counts_offsets_between_boundaries() {
        let b = [0, 10, 20, 30];
        let mut fake = Fake::new(&[(0, &[-5, 1, 2, 15, 25, 25, 40]), (1, &[11, 12])]);
        let h = chart(&mut fake, &b, &[0, 1]);
        let counts: Vec<u64> = h.buckets.iter().map(|b| b.count).collect();
        assert_eq!(counts, vec![2, 3, 2]);
        assert_eq!(h.buckets[1].by_partition, vec![1, 2]);
        assert_eq!(h.before, 1);
        assert_eq!(h.after, 1);
        assert_eq!(h.total_in_range, 7);
        assert_eq!(h.buckets[1].start_offsets, vec![3, 0]);
        assert_eq!(h.bucket_ms, 10);
    }

    #[test]
    fn skipping_empty_spans_matches_asking_everything() {
        // 64 buckets, records only around bucket 40 in partition 0.
        let b: Vec<i64> = (0..=64).map(|i| i * 10).collect();
        let mut fake = Fake::new(&[(0, &[401, 402, 405, 411]), (1, &[])]);
        let h = chart(&mut fake, &b, &[0, 1]);
        let mut expected = vec![0u64; 64];
        expected[40] = 3;
        expected[41] = 1;
        assert_eq!(
            h.buckets.iter().map(|b| b.count).collect::<Vec<_>>(),
            expected
        );
        // Two ends, then one path down to bucket 40 (and 41) for partition 0.
        assert!(fake.calls < 20, "asked {} times", fake.calls);
    }

    #[test]
    fn answers_are_clamped_and_never_decrease() {
        let slots = vec![vec![
            Some(Lookup::At(5)),
            Some(Lookup::At(3)), // retention moved under us
            Some(Lookup::At(50)),
            Some(Lookup::End),
        ]];
        let h = assemble("t", &[0, 1, 2, 3], &[0], &slots, &[(4, 20)], i64::MAX);
        let counts: Vec<u64> = h.buckets.iter().map(|b| b.count).collect();
        assert_eq!(counts, vec![0, 15, 0]);
        assert_eq!(h.before, 1);
        assert_eq!(h.after, 0);
    }

    #[test]
    fn unknown_boundaries_merge_their_spans() {
        let slots = vec![
            vec![
                Some(Lookup::At(0)),
                None,
                Some(Lookup::At(8)),
                Some(Lookup::At(9)),
            ],
            vec![
                None,
                Some(Lookup::At(0)),
                Some(Lookup::At(0)),
                Some(Lookup::End),
            ],
        ];
        let h = assemble(
            "t",
            &[0, 1, 2, 3],
            &[0, 1],
            &slots,
            &[(0, 9), (0, 4)],
            i64::MAX,
        );
        assert_eq!(h.partitions, vec![0]);
        assert_eq!(h.unavailable, vec![1]);
        assert_eq!(h.unknown, 8);
        assert_eq!(h.buckets[0].state, BucketState::Unknown);
        assert_eq!(h.buckets[1].state, BucketState::Unknown);
        assert_eq!(h.buckets[2].count, 1);
        assert_eq!(h.total_in_range, 9);
    }

    #[test]
    fn a_failed_lookup_is_retried_once() {
        let b = [0, 10, 20];
        let mut fake = Fake::new(&[(0, &[5, 15])]);
        fake.fail_once = Some(10);
        let h = chart(&mut fake, &b, &[0]);
        assert_eq!(
            h.buckets.iter().map(|b| b.count).collect::<Vec<_>>(),
            vec![1, 1]
        );
        assert_eq!(h.unknown, 0);
    }

    #[test]
    fn a_past_deadline_leaves_spans_unknown() {
        let b: Vec<i64> = (0..=8).map(|i| i * 10).collect();
        let mut fake = Fake::new(&[(0, &[1, 25, 71])]);
        let found = search(&mut fake, &b, &[0], Instant::now(), &mut |_, _| {});
        assert!(found.coarsened);
        let h = assemble("t", &b, &[0], &found.slots, &[(0, 3)], i64::MAX);
        assert_eq!(h.unknown, 3);
        assert!(h.buckets.iter().all(|b| b.state == BucketState::Unknown));
    }

    #[test]
    fn empty_topic_and_future_buckets() {
        let slots = vec![vec![Some(Lookup::End), Some(Lookup::End)]];
        let h = assemble("t", &[0, 100], &[0], &slots, &[(0, 0)], 50);
        assert_eq!(h.total_in_range, 0);
        assert_eq!(h.buckets[0].state, BucketState::InProgress);
        assert!(h.peak().is_none());
    }

    #[test]
    fn auto_bucket_picks_the_smallest_candidate() {
        assert_eq!(bucket_ms(&BucketSpec::Auto { target: 48 }, H), Ok(120_000));
        assert_eq!(
            bucket_ms(&BucketSpec::Auto { target: 48 }, 24 * H),
            Ok(1_800_000)
        );
        assert_eq!(
            bucket_ms(&BucketSpec::Auto { target: 1 }, 400 * 24 * H),
            Ok(604_800_000)
        );
        assert!(bucket_ms(&BucketSpec::Fixed(Duration::ZERO), H).is_err());
    }

    #[test]
    fn boundaries_align_to_the_zone_midnight() {
        let day = BucketSpec::Fixed(Duration::from_secs(86_400));
        let kst = 9 * H;
        // 2024-11-02 09:00 UTC .. 2024-11-04 09:00 UTC
        let from = 1_730_538_000_000;
        let utc = plan_boundaries(from, from + 48 * H, &day, 0).unwrap();
        assert_eq!(utc[0] % (24 * H), 0);
        assert_eq!(utc.len(), 4);
        let local = plan_boundaries(from, from + 48 * H, &day, kst).unwrap();
        assert_eq!((local[0] + kst) % (24 * H), 0);
        // 09:00 UTC is 18:00 KST: the first KST day starts at 15:00 UTC the day before.
        assert_eq!(local[0], from - 18 * H);
    }

    #[test]
    fn too_many_buckets_and_cost() {
        let one_s = BucketSpec::Fixed(Duration::from_secs(1));
        assert!(plan_boundaries(0, 1_000_000, &one_s, 0).is_err());
        assert!(plan_boundaries(5, 5, &one_s, 0).is_err());
        let spec = BucketSpec::Auto { target: 48 };
        let refused = plan_within_cost(0, 24 * H, &spec, 0, 10_000, CostPolicy::Refuse);
        assert!(refused.unwrap_err().contains("--yes"));
        let (coarse, widened) =
            plan_within_cost(0, 24 * H, &spec, 0, 10_000, CostPolicy::Coarsen).unwrap();
        assert!(widened);
        assert!(coarse.len() - 1 <= 24);
        let (all, widened) =
            plan_within_cost(0, 24 * H, &spec, 0, 10_000, CostPolicy::Allow).unwrap();
        assert!(!widened);
        assert_eq!(all.len(), 49);
    }

    #[test]
    fn range_resolution() {
        let now = 10 * H;
        assert_eq!(
            TimeRange::Since(Duration::from_secs(3600)).resolve(now, None),
            Ok((9 * H, now))
        );
        assert!(TimeRange::Between(5, 5).resolve(now, None).is_err());
        assert!(TimeRange::All.resolve(now, None).is_err());
        assert_eq!(TimeRange::All.resolve(now, Some(H)), Ok((H, now)));
    }

    #[test]
    fn peak_gaps_and_trimmed() {
        let slots = vec![vec![
            Some(Lookup::At(0)),
            Some(Lookup::At(0)),
            Some(Lookup::At(0)),
            Some(Lookup::At(5)),
            Some(Lookup::At(5)),
            Some(Lookup::At(5)),
            Some(Lookup::At(7)),
        ]];
        let b = [0, 10, 20, 30, 40, 50, 60];
        let mut h = assemble("t", &b, &[0], &slots, &[(0, 7)], i64::MAX);
        assert_eq!(h.peak().map(|b| b.start_ms), Some(20));
        assert_eq!(h.gaps(2), vec![(0, 20), (30, 50)]);
        h.mark_trimmed(Some(15));
        assert_eq!(h.buckets[0].state, BucketState::Trimmed);
        assert_eq!(h.buckets[1].state, BucketState::Complete);
        assert_eq!(h.gaps(1), vec![(10, 20), (30, 50)]);
    }

    #[test]
    fn exact_bins_place_records_by_their_own_time() {
        let mut bins = ExactBins::new(&[0, 10, 20], &[0, 1]);
        bins.add(0, 5, Some(3));
        bins.add(0, 6, Some(15));
        bins.add(0, 7, Some(1)); // out of order: still bucket 0
        bins.add(1, 2, Some(-5)); // a negative time is no time
        bins.add(1, 3, Some(20)); // the end is exclusive
        bins.add(1, 4, None);
        bins.add(9, 1, Some(5)); // not a selected partition
        assert_eq!(bins.counts, vec![vec![2, 0], vec![1, 0]]);
        assert_eq!(bins.first_offsets[0], vec![5, -1]);
        assert_eq!((bins.before, bins.after, bins.no_time), (0, 1, 2));
        assert_eq!(bins.seen, 7);
    }

    #[test]
    fn exact_keep_counts_and_keeps_nothing() {
        let bins = Arc::new(Mutex::new(ExactBins::new(&[0, 10], &[0])));
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let seen = calls.clone();
        let progress: Arc<Mutex<dyn FnMut(usize, usize) + Send>> =
            Arc::new(Mutex::new(move |_: usize, _: usize| {
                seen.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }));
        let keep = exact_keep(bins.clone(), 1, progress);
        let record = ConsumedMessage {
            topic: "t".into(),
            partition: 0,
            offset: 0,
            key: Some(b"secret".to_vec()),
            payload: Some(b"secret".to_vec()),
            timestamp_ms: Some(5),
            headers: Vec::new(),
        };
        assert!(!keep(&record));
        assert_eq!(bins.lock().unwrap().counts, vec![vec![1]]);
    }

    #[test]
    fn exact_histogram_adds_offset_and_time_outliers() {
        let plan = ExactPlan {
            boundaries: vec![0, 10, 20],
            partitions: vec![0],
            unavailable: vec![3],
            ranges: Vec::new(),
            before: 4,
            after: 1,
            removed: 0,
            widened: false,
            may_be_trimmed: vec![0],
            lookup_error: None,
        };
        let mut bins = ExactBins::new(&plan.boundaries, &plan.partitions);
        bins.add(0, 10, Some(25));
        bins.add(0, 11, Some(12));
        let h = exact_histogram("t", &plan, &bins, 2, i64::MAX);
        assert!(h.exact);
        assert_eq!(h.offset_total, Some(2));
        assert_eq!(h.buckets[1].count, 1);
        assert_eq!(h.buckets[1].start_offsets, vec![11]);
        assert_eq!((h.before, h.after), (4, 2));
        assert_eq!(h.total_in_range, 1);
        assert_eq!(h.unavailable, vec![3]);
        assert!(h.approximate.is_empty());
        assert_eq!(h.may_be_trimmed, vec![0]);
    }

    #[test]
    fn partitions_starting_at_their_low_watermark_may_be_trimmed() {
        let slots = vec![
            vec![Some(Lookup::At(4)), Some(Lookup::End)],
            vec![Some(Lookup::At(9)), Some(Lookup::End)],
            vec![Some(Lookup::End), Some(Lookup::End)],
        ];
        let h = assemble(
            "t",
            &[0, 10],
            &[0, 1, 2],
            &slots,
            &[(4, 8), (0, 10), (0, 0)],
            i64::MAX,
        );
        // Partition 0 starts at its low watermark; 1 has records before;
        // 2 is empty.
        assert_eq!(h.may_be_trimmed, vec![0]);
    }

    #[test]
    fn offsets_removed_by_compaction_are_not_before_the_range() {
        // 600 records written inside the range, compacted down to the last
        // 6 (offsets 595..601): the lookup at the range start lands on 595.
        let slots = vec![vec![Some(Lookup::At(595)), Some(Lookup::End)]];
        let plain = assemble("t", &[0, 10], &[0], &slots, &[(0, 601)], i64::MAX);
        assert_eq!(plain.before, 595);
        let h = assemble_retained(
            "t",
            &[0, 10],
            &[0],
            &slots,
            &[(0, 601)],
            &[Some(595)],
            i64::MAX,
        );
        assert_eq!(h.before, 0);
        assert_eq!(h.removed_by_compaction, 595);
        assert_eq!(h.total_in_range, 6);
        // A retained record before the range is still before it.
        let slots = vec![vec![Some(Lookup::At(595)), Some(Lookup::End)]];
        let h = assemble_retained(
            "t",
            &[0, 10],
            &[0],
            &slots,
            &[(0, 601)],
            &[Some(590)],
            i64::MAX,
        );
        assert_eq!((h.removed_by_compaction, h.before), (590, 5));
    }

    #[test]
    fn retriable_metadata_errors_are_retried_a_bounded_number_of_times() {
        use rdkafka::types::RDKafkaErrorCode as Code;
        let not_leader = || {
            KafkaError::from(rdkafka::error::KafkaError::MetadataFetch(
                Code::NotLeaderForPartition,
            ))
        };
        let mut calls = 0;
        let ok = retry_metadata(|| {
            calls += 1;
            if calls < 2 {
                Err(not_leader())
            } else {
                Ok(7)
            }
        });
        assert_eq!(ok.unwrap(), 7);
        assert_eq!(calls, 2);
        let mut calls = 0;
        let other: KafkaResult<()> = retry_metadata(|| {
            calls += 1;
            Err(KafkaError::Check("no".into()))
        });
        assert!(other.is_err());
        assert_eq!(calls, 1);
    }

    #[test]
    fn oldest_retained_asks_time_zero() {
        let mut fake = Fake::new(&[(0, &[5, 15]), (1, &[])]);
        assert_eq!(oldest_retained(&mut fake, &[0, 1]), vec![Some(0), None]);
    }

    #[test]
    fn a_partition_starting_at_offset_zero_was_never_trimmed() {
        // A fresh topic: every record is in the last bucket, nothing was
        // deleted, so the empty buckets before it stay empty.
        let slots = vec![vec![
            Some(Lookup::At(0)),
            Some(Lookup::At(0)),
            Some(Lookup::End),
        ]];
        let h = assemble("t", &[0, 10, 20], &[0], &slots, &[(0, 100)], i64::MAX);
        assert!(h.may_be_trimmed.is_empty());
        assert_eq!(h.buckets[0].state, BucketState::Complete);
    }

    #[test]
    fn result_carries_no_record_content() {
        let slots = vec![vec![Some(Lookup::At(0)), Some(Lookup::End)]];
        let h = assemble("t", &[0, 10], &[0], &slots, &[(0, 3)], i64::MAX);
        let json = serde_json::to_value(&h).unwrap();
        for field in ["key", "value", "payload", "headers"] {
            assert!(json.get(field).is_none());
            assert!(json["buckets"][0].get(field).is_none());
        }
    }
}
