//! One-off reads of a topic "as it is now": every selected partition from a
//! start offset up to the high watermark captured before reading.
//!
//! Commands that inspect a topic (compact, diff, dlq inspect, trace) used to
//! read until the first `recv` timed out. A slow first fetch then looked like
//! an empty topic, and a fixed deadline silently cut big topics short. A
//! bounded read knows where each partition ends, so it stops when the data
//! is read, and says so when it is not ([`BoundedRead::partial_warning`]).

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use rdkafka::Offset;

use crate::error::{KafkaError, KafkaResult};
use crate::kafka::config::KafkaClientConfig;
use crate::kafka::consumer::{
    ConsumedMessage, KafkaConsumerClient, ParallelReader, PartitionRead, ReadEvent,
};

/// One partition to read, from `start` up to (not including) `end`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionRange {
    pub topic: String,
    pub partition: i32,
    pub start: i64,
    /// The high watermark captured before reading (or an earlier cap).
    pub end: i64,
}

impl PartitionRange {
    fn is_empty(&self) -> bool {
        self.start >= self.end
    }
}

/// Low and high watermarks of a topic's selected partitions.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TopicMarks {
    pub topic: String,
    /// `partition -> (low, high)`.
    pub marks: BTreeMap<i32, (i64, i64)>,
    /// Selected partitions whose watermarks could not be fetched; they are
    /// not read.
    pub unavailable: Vec<i32>,
}

/// How long a bounded read may wait.
#[derive(Debug, Clone)]
pub struct BoundedReadOptions {
    /// Wait for the first record this long: the first fetch includes
    /// connecting to every partition leader.
    pub first_fetch_grace: Duration,
    /// After the first record, give up when nothing arrives for this long.
    pub idle_timeout: Duration,
    /// Give up after this long in total.
    pub overall_timeout: Duration,
    /// Stop after this many kept records.
    pub max_records: Option<usize>,
}

impl Default for BoundedReadOptions {
    fn default() -> Self {
        Self {
            first_fetch_grace: Duration::from_secs(15),
            idle_timeout: Duration::from_secs(10),
            overall_timeout: Duration::from_secs(300),
            max_records: None,
        }
    }
}

impl BoundedReadOptions {
    /// The defaults, stopping after `max_records` kept records.
    pub fn with_max_records(max_records: usize) -> Self {
        Self {
            max_records: Some(max_records),
            ..Self::default()
        }
    }
}

/// Why a bounded read stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopReason {
    /// Every range was read to its end.
    Complete,
    /// [`BoundedReadOptions::max_records`] records were kept first.
    LimitReached,
    /// No record arrived within the first-fetch grace period.
    NoFirstFetch,
    /// Records stopped arriving for the idle timeout.
    Stalled,
    /// The overall timeout ran out.
    TimedOut,
}

/// What a bounded read returns.
#[derive(Debug)]
pub struct BoundedRead {
    /// Kept records, ordered by topic, partition and offset.
    pub records: Vec<ConsumedMessage>,
    /// Records read and dropped by the filter.
    pub skipped: u64,
    pub stop: StopReason,
    /// Ranges not read to their end, from the next unread offset.
    pub unread: Vec<PartitionRange>,
    /// Partitions left out because their watermarks were unavailable.
    pub unavailable: Vec<i32>,
    /// The last consumer error seen while reading.
    pub last_error: Option<String>,
}

impl BoundedRead {
    /// A read with nothing to do.
    fn empty() -> Self {
        Self {
            records: Vec::new(),
            skipped: 0,
            stop: StopReason::Complete,
            unread: Vec::new(),
            unavailable: Vec::new(),
            last_error: None,
        }
    }

    /// Whether some selected data was not read, other than by the record
    /// limit.
    pub fn is_incomplete(&self) -> bool {
        !self.unavailable.is_empty()
            || !matches!(self.stop, StopReason::Complete | StopReason::LimitReached)
    }

    /// A warning that the results cover only part of `topic`, or `None`
    /// when they cover all of it. `limit_is_partial`: count stopping at the
    /// record limit as partial (for analyses of the whole topic, not for
    /// samples).
    pub fn partial_warning(&self, topic: &str, limit_is_partial: bool) -> Option<String> {
        let mut reasons = Vec::new();
        match self.stop {
            StopReason::Complete => {}
            StopReason::LimitReached => {
                if limit_is_partial && !self.unread.is_empty() {
                    reasons.push(format!(
                        "the record limit was reached with {} offset(s) left unread (raise --limit)",
                        unread_offsets(&self.unread)
                    ));
                }
            }
            StopReason::NoFirstFetch => {
                reasons.push("no record arrived within the first-fetch grace period".to_string())
            }
            StopReason::Stalled => reasons.push("records stopped arriving".to_string()),
            StopReason::TimedOut => reasons.push("the read timed out".to_string()),
        }
        if !matches!(self.stop, StopReason::Complete | StopReason::LimitReached)
            && !self.unread.is_empty()
        {
            reasons.push(format!(
                "{} offset(s) left unread in partition(s) {}",
                unread_offsets(&self.unread),
                partition_list(self.unread.iter().map(|r| r.partition))
            ));
        }
        if !self.unavailable.is_empty() {
            reasons.push(format!(
                "watermarks unavailable for partition(s) {}",
                partition_list(self.unavailable.iter().copied())
            ));
        }
        if reasons.is_empty() {
            return None;
        }
        let mut text = format!("Results for '{topic}' are partial: {}.", reasons.join("; "));
        if let Some(e) = &self.last_error {
            if self.is_incomplete() {
                text.push_str(&format!(" Last consumer error: {e}"));
            }
        }
        Some(text)
    }
}

fn unread_offsets(unread: &[PartitionRange]) -> i64 {
    unread.iter().map(|r| (r.end - r.start).max(0)).sum()
}

fn partition_list(partitions: impl Iterator<Item = i32>) -> String {
    partitions
        .map(|p| p.to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

/// The partitions to read: all of them, or only `only`.
pub fn select_partitions(
    topic: &str,
    partitions: &[i32],
    only: Option<i32>,
) -> KafkaResult<Vec<i32>> {
    match only {
        None => Ok(partitions.to_vec()),
        Some(p) if partitions.contains(&p) => Ok(vec![p]),
        Some(partition) => Err(KafkaError::PartitionNotFound {
            topic: topic.to_string(),
            partition,
        }),
    }
}

/// Fetches the watermarks of `topic`'s partitions (or of `only`).
pub fn topic_watermarks(
    config: &KafkaClientConfig,
    topic: &str,
    only: Option<i32>,
) -> KafkaResult<TopicMarks> {
    let client = KafkaConsumerClient::lookup(config)?;
    let all = client.topic_partitions(topic, None)?;
    let selected = select_partitions(topic, &all, only)?;
    let wanted: Vec<(&str, i32)> = selected.iter().map(|&p| (topic, p)).collect();
    let fetched = client.fetch_watermarks_batch(&wanted, None)?;
    Ok(collect_marks(topic, &selected, &fetched))
}

/// [`TopicMarks`] from fetched watermarks; selected partitions missing from
/// `fetched` are unavailable.
fn collect_marks(
    topic: &str,
    selected: &[i32],
    fetched: &HashMap<(String, i32), (i64, i64)>,
) -> TopicMarks {
    let mut marks = TopicMarks {
        topic: topic.to_string(),
        ..TopicMarks::default()
    };
    for &p in selected {
        match fetched.get(&(topic.to_string(), p)) {
            Some(&mark) => {
                marks.marks.insert(p, mark);
            }
            None => marks.unavailable.push(p),
        }
    }
    marks
}

/// Ranges reading each partition from its low watermark up to its high
/// watermark, or at most `per_partition` records of it.
pub fn plan_ranges(marks: &TopicMarks, per_partition: Option<usize>) -> Vec<PartitionRange> {
    marks
        .marks
        .iter()
        .map(|(&partition, &(low, high))| PartitionRange {
            topic: marks.topic.clone(),
            partition,
            start: low,
            end: capped_end(low, high, per_partition),
        })
        .filter(|r| !r.is_empty())
        .collect()
}

fn capped_end(start: i64, high: i64, cap: Option<usize>) -> i64 {
    match cap {
        Some(n) => high.min(start.saturating_add(i64::try_from(n).unwrap_or(i64::MAX))),
        None => high,
    }
}

/// Ranges comparing two topics position by position: per partition, both
/// start at the later low watermark of the two and read at most
/// `per_partition` offsets, so the same partition/offset positions are read
/// from each. A partition only one topic has is read from its low watermark.
pub fn plan_aligned_ranges(
    a: &TopicMarks,
    b: &TopicMarks,
    per_partition: usize,
) -> (Vec<PartitionRange>, Vec<PartitionRange>) {
    let mut ranges_a = Vec::new();
    let mut ranges_b = Vec::new();
    let mut partitions: Vec<i32> = a.marks.keys().chain(b.marks.keys()).copied().collect();
    partitions.sort_unstable();
    partitions.dedup();
    for p in partitions {
        let mark_a = a.marks.get(&p).copied();
        let mark_b = b.marks.get(&p).copied();
        let start = match (mark_a, mark_b) {
            (Some((low_a, _)), Some((low_b, _))) => low_a.max(low_b),
            (Some((low, _)), None) | (None, Some((low, _))) => low,
            (None, None) => continue,
        };
        for (marks, mark, out) in [(a, mark_a, &mut ranges_a), (b, mark_b, &mut ranges_b)] {
            if let Some((_, high)) = mark {
                let range = PartitionRange {
                    topic: marks.topic.clone(),
                    partition: p,
                    start,
                    end: capped_end(start, high, Some(per_partition)),
                };
                if !range.is_empty() {
                    out.push(range);
                }
            }
        }
    }
    (ranges_a, ranges_b)
}

/// How long the read may still wait for its next record, or `None` when it
/// should stop (and why).
fn wait_budget(
    opts: &BoundedReadOptions,
    since_start: Duration,
    since_progress: Duration,
    got_any: bool,
) -> Result<Duration, StopReason> {
    let overall = opts.overall_timeout.saturating_sub(since_start);
    let (idle_limit, idle_reason) = if got_any {
        (opts.idle_timeout, StopReason::Stalled)
    } else {
        (opts.first_fetch_grace, StopReason::NoFirstFetch)
    };
    let idle = idle_limit.saturating_sub(since_progress);
    if overall.is_zero() {
        Err(StopReason::TimedOut)
    } else if idle.is_zero() {
        Err(idle_reason)
    } else {
        Ok(overall.min(idle))
    }
}

/// Tracks the next offset to read in each range.
struct Progress {
    next: HashMap<(String, i32), (i64, i64)>,
}

impl Progress {
    fn new(ranges: &[PartitionRange]) -> Self {
        Self {
            next: ranges
                .iter()
                .map(|r| ((r.topic.clone(), r.partition), (r.start, r.end)))
                .collect(),
        }
    }

    fn observe(&mut self, topic: &str, partition: i32, offset: i64) {
        if let Some((next, _)) = self.next.get_mut(&(topic.to_string(), partition)) {
            *next = (*next).max(offset + 1);
        }
    }

    /// Ranges not read to their end. A range ending in compacted-away
    /// records or transaction markers can look unread although it is not;
    /// only call this when the reader did not finish.
    fn unread(&self) -> Vec<PartitionRange> {
        let mut unread: Vec<PartitionRange> = self
            .next
            .iter()
            .filter(|(_, (next, end))| next < end)
            .map(|((topic, partition), &(next, end))| PartitionRange {
                topic: topic.clone(),
                partition: *partition,
                start: next,
                end,
            })
            .collect();
        unread.sort_by(|a, b| (&a.topic, a.partition).cmp(&(&b.topic, b.partition)));
        unread
    }
}

/// Reads `ranges`, keeping the records `keep` accepts, until every range is
/// read, the record limit is reached or a timeout runs out.
pub async fn read_ranges<F>(
    config: &KafkaClientConfig,
    ranges: &[PartitionRange],
    opts: &BoundedReadOptions,
    keep: F,
) -> KafkaResult<BoundedRead>
where
    F: Fn(&ConsumedMessage) -> bool + Send + Sync + 'static,
{
    let ranges: Vec<&PartitionRange> = ranges.iter().filter(|r| !r.is_empty()).collect();
    let mut read = BoundedRead::empty();
    if ranges.is_empty() || opts.max_records == Some(0) {
        return Ok(read);
    }
    let reads: Vec<PartitionRead> = ranges
        .iter()
        .map(|r| PartitionRead {
            topic: &r.topic,
            partition: r.partition,
            start: Offset::Offset(r.start),
            end: Some(r.end),
        })
        .collect();
    let owned: Vec<PartitionRange> = ranges.into_iter().cloned().collect();
    let mut progress = Progress::new(&owned);
    let mut reader = ParallelReader::start(config, &reads, keep)?;

    let started = tokio::time::Instant::now();
    let mut last_progress = started;
    let mut got_any = false;
    let stop = loop {
        let now = tokio::time::Instant::now();
        let budget = match wait_budget(opts, now - started, now - last_progress, got_any) {
            Ok(budget) => budget,
            Err(reason) => break reason,
        };
        let Ok(event) = tokio::time::timeout(budget, reader.recv()).await else {
            // Loop once more so `wait_budget` names the timeout.
            continue;
        };
        match event {
            None => break StopReason::Complete,
            Some(ReadEvent::Record(msg)) => {
                got_any = true;
                last_progress = tokio::time::Instant::now();
                progress.observe(&msg.topic, msg.partition, msg.offset);
                read.records.push(msg);
                if opts
                    .max_records
                    .is_some_and(|max| read.records.len() >= max)
                {
                    break StopReason::LimitReached;
                }
            }
            Some(ReadEvent::Skipped(n)) => {
                got_any = true;
                last_progress = tokio::time::Instant::now();
                read.skipped += n;
            }
            Some(ReadEvent::Error(e)) => {
                tracing::debug!(%e, "Consumer error during bounded read");
                read.last_error = Some(e.to_string());
            }
        }
    };
    drop(reader);

    if stop != StopReason::Complete {
        read.unread = progress.unread();
    }
    // Reaching the limit exactly at the end of the data is a complete read.
    read.stop = if stop == StopReason::LimitReached && read.unread.is_empty() {
        StopReason::Complete
    } else {
        stop
    };
    read.records
        .sort_by(|a, b| (&a.topic, a.partition, a.offset).cmp(&(&b.topic, b.partition, b.offset)));
    // Nothing read and an error seen: report the error rather than an empty
    // result.
    if read.records.is_empty() && read.skipped == 0 && read.is_incomplete() {
        if let Some(e) = read.last_error.take() {
            return Err(KafkaError::Consumer(e));
        }
    }
    Ok(read)
}

/// Reads `topic` (or only partition `only`) from its low watermarks up to
/// the high watermarks captured before reading.
pub async fn read_topic<F>(
    config: &KafkaClientConfig,
    topic: &str,
    only: Option<i32>,
    opts: &BoundedReadOptions,
    keep: F,
) -> KafkaResult<BoundedRead>
where
    F: Fn(&ConsumedMessage) -> bool + Send + Sync + 'static,
{
    let marks = topic_watermarks(config, topic, only)?;
    let ranges = plan_ranges(&marks, None);
    let mut read = read_ranges(config, &ranges, opts, keep).await?;
    read.unavailable = marks.unavailable;
    Ok(read)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn marks(topic: &str, marks: &[(i32, (i64, i64))]) -> TopicMarks {
        TopicMarks {
            topic: topic.to_string(),
            marks: marks.iter().copied().collect(),
            unavailable: Vec::new(),
        }
    }

    fn range(topic: &str, partition: i32, start: i64, end: i64) -> PartitionRange {
        PartitionRange {
            topic: topic.to_string(),
            partition,
            start,
            end,
        }
    }

    #[test]
    fn select_partitions_honours_the_partition_option() {
        assert_eq!(select_partitions("t", &[0, 1, 2], None).unwrap(), [0, 1, 2]);
        assert_eq!(select_partitions("t", &[0, 1, 2], Some(1)).unwrap(), [1]);
        assert!(matches!(
            select_partitions("t", &[0, 1], Some(5)),
            Err(KafkaError::PartitionNotFound { partition: 5, .. })
        ));
    }

    #[test]
    fn collect_marks_reports_partitions_without_watermarks() {
        let fetched: HashMap<(String, i32), (i64, i64)> = [
            (("t".to_string(), 0), (3, 9)),
            (("other".to_string(), 1), (0, 1)),
        ]
        .into();
        let m = collect_marks("t", &[0, 1], &fetched);
        assert_eq!(m.marks, BTreeMap::from([(0, (3, 9))]));
        assert_eq!(m.unavailable, [1]);
    }

    #[test]
    fn plan_ranges_reads_to_the_high_watermark_and_skips_empty_partitions() {
        let m = marks("t", &[(0, (5, 20)), (1, (7, 7)), (2, (0, 3))]);
        assert_eq!(
            plan_ranges(&m, None),
            [range("t", 0, 5, 20), range("t", 2, 0, 3)]
        );
        assert_eq!(
            plan_ranges(&m, Some(4)),
            [range("t", 0, 5, 9), range("t", 2, 0, 3)]
        );
        assert_eq!(
            capped_end(i64::MAX - 1, i64::MAX, Some(usize::MAX)),
            i64::MAX
        );
    }

    #[test]
    fn aligned_ranges_read_the_same_positions_from_both_topics() {
        let a = marks("a", &[(0, (0, 100)), (1, (10, 12)), (2, (0, 5))]);
        let b = marks("b", &[(0, (4, 50)), (1, (0, 30)), (3, (2, 4))]);
        let (ra, rb) = plan_aligned_ranges(&a, &b, 10);
        // Partition 0 starts at the later low watermark in both; partition 1
        // likewise, with A ending at its high watermark; partitions only one
        // topic has are read from their own low watermark.
        assert_eq!(
            ra,
            [
                range("a", 0, 4, 14),
                range("a", 1, 10, 12),
                range("a", 2, 0, 5)
            ]
        );
        assert_eq!(
            rb,
            [
                range("b", 0, 4, 14),
                range("b", 1, 10, 20),
                range("b", 3, 2, 4)
            ]
        );
    }

    #[test]
    fn aligned_ranges_skip_a_side_that_ends_before_the_common_start() {
        let a = marks("a", &[(0, (0, 3))]);
        let b = marks("b", &[(0, (8, 12))]);
        let (ra, rb) = plan_aligned_ranges(&a, &b, 10);
        assert!(ra.is_empty());
        assert_eq!(rb, [range("b", 0, 8, 12)]);
    }

    #[test]
    fn wait_budget_gives_the_first_fetch_a_longer_grace() {
        let opts = BoundedReadOptions {
            first_fetch_grace: Duration::from_secs(15),
            idle_timeout: Duration::from_secs(2),
            overall_timeout: Duration::from_secs(60),
            max_records: None,
        };
        let s = Duration::from_secs;
        // A slow first fetch is waited for well past the idle timeout.
        assert_eq!(wait_budget(&opts, s(5), s(5), false), Ok(s(10)));
        assert_eq!(
            wait_budget(&opts, s(15), s(15), false),
            Err(StopReason::NoFirstFetch)
        );
        // Once records flow, a stall ends the read sooner.
        assert_eq!(wait_budget(&opts, s(20), s(1), true), Ok(s(1)));
        assert_eq!(
            wait_budget(&opts, s(20), s(2), true),
            Err(StopReason::Stalled)
        );
        // The overall timeout caps everything.
        assert_eq!(wait_budget(&opts, s(59), s(0), true), Ok(s(1)));
        assert_eq!(
            wait_budget(&opts, s(60), s(0), true),
            Err(StopReason::TimedOut)
        );
    }

    #[test]
    fn progress_lists_ranges_not_read_to_their_end() {
        let ranges = [range("t", 0, 0, 10), range("t", 1, 5, 8)];
        let mut p = Progress::new(&ranges);
        p.observe("t", 0, 3);
        p.observe("t", 1, 7);
        p.observe("t", 9, 100); // not a range of this read
        assert_eq!(p.unread(), [range("t", 0, 4, 10)]);
    }

    fn read_with(stop: StopReason, unread: Vec<PartitionRange>) -> BoundedRead {
        BoundedRead {
            stop,
            unread,
            ..BoundedRead::empty()
        }
    }

    #[test]
    fn partial_warning_names_what_was_not_read() {
        let complete = read_with(StopReason::Complete, Vec::new());
        assert_eq!(complete.partial_warning("t", true), None);
        assert!(!complete.is_incomplete());

        let stalled = read_with(StopReason::Stalled, vec![range("t", 2, 40, 100)]);
        let text = stalled.partial_warning("t", false).unwrap();
        assert!(text.contains("partial"), "{text}");
        assert!(text.contains("60 offset(s)"), "{text}");
        assert!(text.contains("partition(s) 2"), "{text}");

        let first = read_with(StopReason::NoFirstFetch, vec![range("t", 0, 0, 5)]);
        let text = first.partial_warning("t", false).unwrap();
        assert!(text.contains("first-fetch"), "{text}");

        // A sample that stops at its limit is not partial; an analysis is.
        let limited = read_with(StopReason::LimitReached, vec![range("t", 0, 50, 80)]);
        assert_eq!(limited.partial_warning("t", false), None);
        let text = limited.partial_warning("t", true).unwrap();
        assert!(text.contains("--limit") && text.contains("30"), "{text}");
        assert!(!limited.is_incomplete());

        let mut missing = read_with(StopReason::Complete, Vec::new());
        missing.unavailable = vec![3, 4];
        missing.last_error = Some("boom".into());
        let text = missing.partial_warning("t", false).unwrap();
        assert!(text.contains("partition(s) 3, 4"), "{text}");
        assert!(text.contains("boom"), "{text}");
    }
}
