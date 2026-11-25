use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

use ferrika_core::codec::pii::{ellipsize, mask_bytes, mask_payload, mask_record, MaskStrategy};
use ferrika_core::kafka::consumer::ConsumedMessage;

/// Characters of payload shown in a DLQ preview.
const PREVIEW_CHARS: usize = 120;

/// Known header keys commonly used by Kafka DLQ frameworks (Spring Kafka's
/// dead-letter publishing recoverer, Kafka Connect's errant record
/// reporter, Strimzi, Micronaut, Sarama, etc.)
pub const ORIGINAL_TOPIC_HEADERS: &[&str] = &[
    "x-original-topic",
    "kafka_dlt-original-topic",
    "__connect.errors.topic",
    "kafka_originalTopic",
    "original-topic",
    "x-orig-topic",
    "x-death-topic",
];

pub const ORIGINAL_PARTITION_HEADERS: &[&str] = &[
    "x-original-partition",
    "kafka_dlt-original-partition",
    "__connect.errors.partition",
    "kafka_originalPartition",
    "original-partition",
    "x-orig-partition",
];

pub const ORIGINAL_OFFSET_HEADERS: &[&str] = &[
    "x-original-offset",
    "kafka_dlt-original-offset",
    "__connect.errors.offset",
    "kafka_originalOffset",
    "original-offset",
    "x-orig-offset",
];

pub const EXCEPTION_MESSAGE_HEADERS: &[&str] = &[
    "x-exception-message",
    "kafka_dlt-exception-message",
    "__connect.errors.exception.message",
    "kafka_exceptionMessage",
    "x-error-message",
    "error-message",
    "x-death-reason",
    "exception-message",
];

pub const EXCEPTION_STACK_HEADERS: &[&str] = &[
    "x-exception-stacktrace",
    "kafka_dlt-exception-stacktrace",
    "__connect.errors.exception.stacktrace",
    "kafka_exceptionStacktrace",
    "x-stacktrace",
    "exception-stacktrace",
];

pub const RETRY_COUNT_HEADERS: &[&str] = &[
    "x-retry-count",
    "kafka_retryCount",
    "x-delivery-attempt",
    "retry-count",
];

/// Header ferrika adds to every record it redrives.
pub const REDRIVEN_BY_HEADER: &str = "x-redriven-by";
/// Header carrying the time of the redrive (ms since the UNIX epoch).
pub const REDRIVE_TIMESTAMP_HEADER: &str = "x-redrive-timestamp";

/// A parsed dead-letter queue message containing failure context and original routing information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqMessage {
    pub partition: i32,
    pub offset: i64,
    pub timestamp_ms: Option<i64>,
    pub key: Option<String>,
    pub payload_preview: Option<String>,
    #[serde(skip_serializing)]
    pub raw_payload: Vec<u8>,
    pub original_topic: Option<String>,
    pub original_partition: Option<i32>,
    pub original_offset: Option<i64>,
    pub exception_message: Option<String>,
    pub exception_stacktrace: Option<String>,
    /// From a retry-count header; `None` when the record has none.
    pub retry_count: Option<u32>,
    pub headers: Vec<(String, String)>,
    /// The key exactly as stored (`key` is only its UTF-8 view); redrive uses this.
    #[serde(skip)]
    pub raw_key: Option<Vec<u8>>,
    /// The headers exactly as stored (`headers` is a display view); redrive uses these.
    #[serde(skip)]
    pub raw_headers: Vec<(String, Vec<u8>)>,
    /// The record had no payload at all (`raw_payload` is then empty).
    #[serde(skip)]
    pub payload_is_null: bool,
}

impl DlqMessage {
    /// Parses a DLQ message from a consumed Kafka message and its hosting DLQ topic name
    pub fn from_consumed_message(msg: &ConsumedMessage, dlq_topic: &str) -> Self {
        let mut headers_map: HashMap<String, String> = HashMap::new();
        let mut string_headers: Vec<(String, String)> = Vec::new();
        let mut raw_headers: Vec<(String, Vec<u8>)> = Vec::new();

        for (k, v) in &msg.headers {
            let val_str = String::from_utf8_lossy(v).trim().to_string();
            headers_map.insert(k.to_lowercase(), val_str.clone());
            string_headers.push((k.clone(), val_str));
            raw_headers.push((k.clone(), v.clone()));
        }

        let original_topic = find_header_value(&headers_map, ORIGINAL_TOPIC_HEADERS)
            .or_else(|| infer_original_topic_from_dlq_name(dlq_topic));

        let original_partition = header_number(&msg.headers, ORIGINAL_PARTITION_HEADERS)
            .and_then(|n| i32::try_from(n).ok());

        let original_offset = header_number(&msg.headers, ORIGINAL_OFFSET_HEADERS);

        let exception_message = find_header_value(&headers_map, EXCEPTION_MESSAGE_HEADERS);
        let exception_stacktrace = find_header_value(&headers_map, EXCEPTION_STACK_HEADERS);

        let retry_count = find_header_value(&headers_map, RETRY_COUNT_HEADERS)
            .and_then(|v| v.parse::<u32>().ok());

        let raw_payload = msg.payload.clone().unwrap_or_default();
        let payload_preview = (!raw_payload.is_empty())
            .then(|| ellipsize(String::from_utf8_lossy(&raw_payload).trim(), PREVIEW_CHARS));

        Self {
            partition: msg.partition,
            offset: msg.offset,
            timestamp_ms: msg.timestamp_ms,
            key: msg.key_str().map(|s| s.to_string()),
            payload_preview,
            raw_payload,
            original_topic,
            original_partition,
            original_offset,
            exception_message,
            exception_stacktrace,
            retry_count,
            headers: string_headers,
            raw_key: msg.key.clone(),
            raw_headers,
            payload_is_null: msg.payload.is_none(),
        }
    }

    /// Whether ferrika already redrove this record (it carries the redrive marker),
    /// e.g. it failed again after a redrive and came back to the DLQ.
    pub fn already_redriven(&self) -> bool {
        self.raw_headers
            .iter()
            .any(|(k, _)| k.eq_ignore_ascii_case(REDRIVEN_BY_HEADER))
    }

    /// A copy for display while the company policy enforces masking: PII is
    /// masked in the key, payload preview (masked before it is cut, so a cut
    /// cannot expose part of a value), exception text and header values.
    /// The raw payload is dropped; redrives use the unmasked messages.
    pub fn masked(&self) -> Self {
        let mask = |text: &str| mask_payload(text, MaskStrategy::Partial);
        let payload_preview = (!self.raw_payload.is_empty()).then(|| {
            ellipsize(
                mask_bytes(&self.raw_payload, MaskStrategy::Partial).trim(),
                PREVIEW_CHARS,
            )
        });
        Self {
            key: self.key.as_deref().map(mask),
            payload_preview,
            raw_payload: Vec::new(),
            raw_key: None,
            raw_headers: Vec::new(),
            exception_message: self.exception_message.as_deref().map(mask),
            exception_stacktrace: self.exception_stacktrace.as_deref().map(mask),
            // From the original bytes, so a binary value is hidden whole and
            // not judged through a lossy text copy. A message without them
            // (already masked) falls back to the text view.
            headers: if self.raw_headers.is_empty() {
                mask_record(
                    None,
                    None,
                    self.headers.iter().map(|(k, v)| (k.as_str(), v.as_bytes())),
                    MaskStrategy::Partial,
                )
                .headers
            } else {
                mask_record(
                    None,
                    None,
                    self.raw_headers
                        .iter()
                        .map(|(k, v)| (k.as_str(), v.as_slice())),
                    MaskStrategy::Partial,
                )
                .headers
            },
            ..self.clone()
        }
    }
}

/// The first of `candidates` present in `headers` as a number: decimal text,
/// or a 4- or 8-byte big-endian integer as Spring Kafka writes its
/// `kafka_dlt-original-partition` and `-offset` headers.
fn header_number(headers: &[(String, Vec<u8>)], candidates: &[&str]) -> Option<i64> {
    candidates.iter().find_map(|candidate| {
        let (_, value) = headers
            .iter()
            .find(|(k, v)| k.eq_ignore_ascii_case(candidate) && !v.is_empty())?;
        if let Some(n) = std::str::from_utf8(value)
            .ok()
            .and_then(|text| text.trim().parse::<i64>().ok())
        {
            return Some(n);
        }
        match value.len() {
            4 => Some(i64::from(i32::from_be_bytes(
                value.as_slice().try_into().ok()?,
            ))),
            8 => Some(i64::from_be_bytes(value.as_slice().try_into().ok()?)),
            _ => None,
        }
    })
}

/// Whether a record's `headers` name the topic it came from. Without one,
/// redrive would send it to a topic guessed from the DLQ's name.
pub fn names_original_topic(headers: &[(String, Vec<u8>)]) -> bool {
    headers.iter().any(|(k, v)| {
        ORIGINAL_TOPIC_HEADERS
            .iter()
            .any(|h| h.eq_ignore_ascii_case(k))
            && !String::from_utf8_lossy(v).trim().is_empty()
    })
}

/// Helper function to match a header against common synonym keys
fn find_header_value(headers: &HashMap<String, String>, candidates: &[&str]) -> Option<String> {
    for candidate in candidates {
        if let Some(v) = headers.get(&candidate.to_lowercase()) {
            if !v.is_empty() {
                return Some(v.clone());
            }
        }
    }
    None
}

/// Infers the original topic from conventional DLQ naming patterns:
/// e.g., "orders-dlq" -> "orders", "payments.dlq" -> "payments", "dead-letter.events" -> "events"
pub fn infer_original_topic_from_dlq_name(dlq_topic: &str) -> Option<String> {
    if let Some(stripped) = dlq_topic.strip_suffix("-dlq") {
        if !stripped.is_empty() {
            return Some(stripped.to_string());
        }
    }
    if let Some(stripped) = dlq_topic.strip_suffix(".dlq") {
        if !stripped.is_empty() {
            return Some(stripped.to_string());
        }
    }
    if let Some(stripped) = dlq_topic.strip_suffix("_dlq") {
        if !stripped.is_empty() {
            return Some(stripped.to_string());
        }
    }
    if let Some(stripped) = dlq_topic.strip_prefix("dead-letter.") {
        if !stripped.is_empty() {
            return Some(stripped.to_string());
        }
    }
    if let Some(stripped) = dlq_topic.strip_prefix("dlq-") {
        if !stripped.is_empty() {
            return Some(stripped.to_string());
        }
    }
    None
}

/// Statistical summary of failed messages in a Dead Letter Queue
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqAnalysis {
    pub total_messages: usize,
    pub unique_keys: usize,
    pub total_bytes: usize,
    pub error_categories: Vec<(String, usize)>,
    pub original_topics: Vec<(String, usize)>,
    pub earliest_timestamp_ms: Option<i64>,
    pub latest_timestamp_ms: Option<i64>,
}

impl DlqAnalysis {
    /// [`Self::analyze`], or, while masking is enforced, the same summary
    /// computed from the masked messages: the error categories (a cut of the
    /// exception text) and the distinct-key count would otherwise reflect what
    /// masking hides. The byte total is a size, not content, so it is kept.
    pub fn analyze_for_display(messages: &[DlqMessage], mask: bool) -> Self {
        if !mask {
            return Self::analyze(messages);
        }
        let masked: Vec<DlqMessage> = messages.iter().map(DlqMessage::masked).collect();
        let mut analysis = Self::analyze(&masked);
        analysis.total_bytes = messages.iter().map(|m| m.raw_payload.len()).sum();
        analysis
    }

    /// Computes summary metrics and categorization from a batch of DLQ messages
    pub fn analyze(messages: &[DlqMessage]) -> Self {
        let total_messages = messages.len();
        let mut key_set = std::collections::HashSet::new();
        let mut total_bytes = 0;

        let mut error_counts: HashMap<String, usize> = HashMap::new();
        let mut topic_counts: HashMap<String, usize> = HashMap::new();

        let mut earliest_ts: Option<i64> = None;
        let mut latest_ts: Option<i64> = None;

        for msg in messages {
            total_bytes += msg.raw_payload.len();

            if let Some(k) = &msg.key {
                key_set.insert(k.clone());
            }

            let reason = msg
                .exception_message
                .as_deref()
                .map(classify_error_reason)
                .unwrap_or_else(|| "Unknown Exception".to_string());
            *error_counts.entry(reason).or_insert(0) += 1;

            let topic = msg
                .original_topic
                .clone()
                .unwrap_or_else(|| "Unknown Origin".to_string());
            *topic_counts.entry(topic).or_insert(0) += 1;

            if let Some(ts) = msg.timestamp_ms {
                earliest_ts = Some(earliest_ts.map_or(ts, |prev| prev.min(ts)));
                latest_ts = Some(latest_ts.map_or(ts, |prev| prev.max(ts)));
            }
        }

        let mut error_categories: Vec<(String, usize)> = error_counts.into_iter().collect();
        error_categories.sort_by_key(|a| std::cmp::Reverse(a.1));

        let mut original_topics: Vec<(String, usize)> = topic_counts.into_iter().collect();
        original_topics.sort_by_key(|a| std::cmp::Reverse(a.1));

        Self {
            total_messages,
            unique_keys: key_set.len(),
            total_bytes,
            error_categories,
            original_topics,
            earliest_timestamp_ms: earliest_ts,
            latest_timestamp_ms: latest_ts,
        }
    }
}

/// Normalizes common stacktrace / error messages into concise categorized buckets
pub(crate) fn classify_error_reason(msg: &str) -> String {
    let lower = msg.to_lowercase();
    if lower.contains("deserializ")
        || lower.contains("serde")
        || (lower.contains("json") && lower.contains("parse"))
    {
        "Deserialization Error".to_string()
    } else if lower.contains("timeout") || lower.contains("timed out") {
        "Downstream Timeout".to_string()
    } else if lower.contains("schema") || lower.contains("avro") || lower.contains("protobuf") {
        "Schema Validation Error".to_string()
    } else if lower.contains("nullpointer")
        || lower.contains("null pointer")
        || lower.contains("null reference")
    {
        "Null Reference / Panic".to_string()
    } else if lower.contains("duplicate") || lower.contains("unique constraint") {
        "Duplicate Key / DB Conflict".to_string()
    } else if lower.contains("network")
        || lower.contains("connection refused")
        || lower.contains("io error")
    {
        "Network / I/O Error".to_string()
    } else {
        // Truncate message if long
        let trimmed = msg.trim();
        ellipsize(trimmed, 60)
    }
}

/// Redrive specification for re-injecting DLQ messages back to target topic
#[derive(Debug, Clone)]
pub struct RedrivePlanItem {
    pub source_partition: i32,
    pub source_offset: i64,
    pub target_topic: String,
    /// Key bytes exactly as read from the DLQ
    pub key: Option<Vec<u8>>,
    /// Payload bytes exactly as read; `None` for a record without payload
    pub payload: Option<Vec<u8>>,
    pub headers: Vec<(String, Vec<u8>)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedriveSummary {
    pub total_examined: usize,
    pub planned_redrives: usize,
    pub skipped_unresolved_topic: usize,
    /// Records skipped because they already carry the redrive marker header
    #[serde(default)]
    pub skipped_already_redriven: usize,
    pub dry_run: bool,
    pub target_topic_distribution: Vec<(String, usize)>,
}

/// [`build_redrive_plan_from`] without a source topic.
#[cfg(test)]
fn build_redrive_plan(
    messages: &[DlqMessage],
    target_topic_override: Option<&str>,
    dry_run: bool,
    skip_redriven: bool,
) -> (Vec<RedrivePlanItem>, RedriveSummary) {
    build_redrive_plan_from(
        None,
        messages,
        target_topic_override,
        dry_run,
        skip_redriven,
    )
}

/// Prepares a redrive execution plan from dead-letter messages read from
/// `source_topic`.
///
/// Keys, payloads and headers are carried byte-for-byte. With
/// `skip_redriven`, records that already carry [`REDRIVEN_BY_HEADER`] are
/// left out so a record is not redriven twice. A record whose destination is
/// `source_topic` itself would only land back in the DLQ (and be found again
/// by the next redrive), so it counts as unresolved and is skipped.
pub fn build_redrive_plan_from(
    source_topic: Option<&str>,
    messages: &[DlqMessage],
    target_topic_override: Option<&str>,
    dry_run: bool,
    skip_redriven: bool,
) -> (Vec<RedrivePlanItem>, RedriveSummary) {
    let mut plan_items = Vec::new();
    let mut skipped = 0;
    let mut skipped_redriven = 0;
    let mut topic_counts: HashMap<String, usize> = HashMap::new();

    let now_ms = ferrika_core::kafka::clock::now_ms().to_string();

    for msg in messages {
        if skip_redriven && msg.already_redriven() {
            skipped_redriven += 1;
            continue;
        }
        let destination = target_topic_override
            .map(|s| s.to_string())
            .or_else(|| msg.original_topic.clone());

        let target_topic = match destination {
            Some(t) if source_topic != Some(t.as_str()) => t,
            _ => {
                skipped += 1;
                continue;
            }
        };

        *topic_counts.entry(target_topic.clone()).or_insert(0) += 1;

        // Carry forward the original headers byte-for-byte, replacing any
        // earlier redrive markers.
        let mut headers: Vec<(String, Vec<u8>)> = msg
            .raw_headers
            .iter()
            .filter(|(k, _)| {
                !k.eq_ignore_ascii_case(REDRIVEN_BY_HEADER)
                    && !k.eq_ignore_ascii_case(REDRIVE_TIMESTAMP_HEADER)
            })
            .cloned()
            .collect();
        headers.push((REDRIVEN_BY_HEADER.to_string(), b"ferrika".to_vec()));
        headers.push((
            REDRIVE_TIMESTAMP_HEADER.to_string(),
            now_ms.as_bytes().to_vec(),
        ));

        plan_items.push(RedrivePlanItem {
            source_partition: msg.partition,
            source_offset: msg.offset,
            target_topic,
            key: msg.raw_key.clone(),
            payload: (!msg.payload_is_null).then(|| msg.raw_payload.clone()),
            headers,
        });
    }

    let mut target_topic_distribution: Vec<(String, usize)> = topic_counts.into_iter().collect();
    target_topic_distribution.sort_by_key(|a| std::cmp::Reverse(a.1));

    let summary = RedriveSummary {
        total_examined: messages.len(),
        planned_redrives: plan_items.len(),
        skipped_unresolved_topic: skipped,
        skipped_already_redriven: skipped_redriven,
        dry_run,
        target_topic_distribution,
    };

    (plan_items, summary)
}

/// Where a redrive starts reading the DLQ: a default offset for every
/// partition and/or explicit offsets for single partitions.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RedriveStart {
    /// Offset for partitions without an explicit one (`None`: the beginning)
    pub default: Option<i64>,
    /// Explicit start offsets by partition
    pub partitions: BTreeMap<i32, i64>,
}

impl RedriveStart {
    /// Parses `--from-offset` values: `N` sets the offset for every partition,
    /// `P:N` sets it for partition `P` only.
    pub fn parse(values: &[String]) -> Result<Self, String> {
        let mut start = Self::default();
        for value in values {
            let value = value.trim();
            let parse_offset = |text: &str| {
                text.trim()
                    .parse::<i64>()
                    .ok()
                    .filter(|o| *o >= 0)
                    .ok_or_else(|| format!("invalid offset '{text}' in --from-offset '{value}'"))
            };
            match value.split_once(':') {
                Some((partition, offset)) => {
                    let partition = partition
                        .trim()
                        .parse::<i32>()
                        .ok()
                        .filter(|p| *p >= 0)
                        .ok_or_else(|| {
                            format!("invalid partition '{partition}' in --from-offset '{value}'")
                        })?;
                    start.partitions.insert(partition, parse_offset(offset)?);
                }
                None => start.default = Some(parse_offset(value)?),
            }
        }
        Ok(start)
    }

    /// Start offset for `partition`, `None` meaning the beginning.
    pub fn offset_for(&self, partition: i32) -> Option<i64> {
        self.partitions.get(&partition).copied().or(self.default)
    }

    /// The position to pass to the next run so it continues after this one.
    ///
    /// Per partition: after the last examined record, or at the first record
    /// that failed to publish so a rerun retries it (records published after
    /// it in that partition are then sent again). Partitions this run did not
    /// read keep their start offset.
    pub fn resume_after(&self, examined: &[(i32, i64)], failed: &[(i32, i64)]) -> RedriveStart {
        let mut next = self.clone();
        for &(partition, offset) in examined {
            let entry = next.partitions.entry(partition).or_insert(offset + 1);
            *entry = (*entry).max(offset + 1);
        }
        let mut first_failed: BTreeMap<i32, i64> = BTreeMap::new();
        for &(partition, offset) in failed {
            let entry = first_failed.entry(partition).or_insert(offset);
            *entry = (*entry).min(offset);
        }
        for (partition, offset) in first_failed {
            next.partitions.insert(partition, offset);
        }
        next
    }

    /// The `--from-offset` arguments that reproduce this position.
    pub fn to_args(&self) -> Vec<String> {
        self.default
            .map(|d| d.to_string())
            .into_iter()
            .chain(self.partitions.iter().map(|(p, o)| format!("{p}:{o}")))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn analysis_under_masking_is_computed_from_masked_messages() {
        let msg = |key: &str, error: &str| {
            let mut headers = HashMap::new();
            headers.insert("x-exception-message".to_string(), error.as_bytes().to_vec());
            DlqMessage::from_consumed_message(
                &ConsumedMessage {
                    topic: "t-dlq".into(),
                    partition: 0,
                    offset: 1,
                    timestamp_ms: None,
                    key: Some(key.as_bytes().to_vec()),
                    payload: Some(b"{}".to_vec()),
                    headers: headers.into_iter().collect(),
                    null_headers: Vec::new(),
                },
                "t-dlq",
            )
        };
        let messages = vec![
            msg("alice@example.com", "rejected alice@example.com"),
            msg("bob@example.com", "rejected bob@example.com"),
        ];
        let raw = DlqAnalysis::analyze_for_display(&messages, false);
        assert_eq!(raw.error_categories.len(), 2);
        assert_eq!(raw.unique_keys, 2);

        let shown = DlqAnalysis::analyze_for_display(&messages, true);
        let dump = format!("{shown:?}");
        assert!(!dump.contains("alice") && !dump.contains("bob"), "{dump}");
        assert_eq!(shown.total_messages, 2);
        assert_eq!(shown.total_bytes, raw.total_bytes);
        assert_eq!(shown.error_categories.len(), 2);
        assert!(shown
            .error_categories
            .iter()
            .all(|(c, _)| c.contains("***")));
    }

    #[test]
    fn masked_hides_credential_headers_by_name() {
        let mut headers = HashMap::new();
        headers.insert("authorization".to_string(), b"Bearer abc.def.ghi".to_vec());
        headers.insert("x-api-key".to_string(), b"k-123456".to_vec());
        headers.insert("x-retry-count".to_string(), b"3".to_vec());
        let consumed = ConsumedMessage {
            topic: "t-dlq".into(),
            partition: 0,
            offset: 1,
            timestamp_ms: None,
            key: None,
            payload: Some(b"{}".to_vec()),
            headers: headers.into_iter().collect(),
            null_headers: Vec::new(),
        };
        let masked = DlqMessage::from_consumed_message(&consumed, "t-dlq").masked();
        let dump = format!("{:?}", masked.headers);
        assert!(!dump.contains("abc.def.ghi"), "{dump}");
        assert!(!dump.contains("k-123456"), "{dump}");
        assert!(masked
            .headers
            .iter()
            .any(|(k, v)| k == "x-retry-count" && v == "3"));
    }

    #[test]
    fn classify_error_reason_does_not_panic_on_multibyte_text() {
        let msg = "알 수 없는 오류가 발생했습니다 ".repeat(6);
        let out = classify_error_reason(&msg);
        assert!(out.ends_with("..."));
    }

    #[test]
    fn spring_and_connect_dead_letter_headers_name_the_origin() {
        let msg = |headers: &[(&str, &str)]| ConsumedMessage {
            topic: "orders-dlq".into(),
            partition: 0,
            offset: 0,
            key: None,
            payload: Some(b"x".to_vec()),
            timestamp_ms: None,
            headers: headers
                .iter()
                .map(|(k, v)| (k.to_string(), v.as_bytes().to_vec()))
                .collect(),
            null_headers: Vec::new(),
        };
        let spring = msg(&[
            ("kafka_dlt-original-topic", "orders-v2"),
            ("kafka_dlt-exception-message", "boom"),
        ]);
        let parsed = DlqMessage::from_consumed_message(&spring, "orders-dlq");
        assert_eq!(parsed.original_topic.as_deref(), Some("orders-v2"));
        assert_eq!(parsed.exception_message.as_deref(), Some("boom"));
        assert!(names_original_topic(&spring.headers));

        // Spring writes the partition and offset as big-endian integers.
        let mut binary = msg(&[("kafka_dlt-original-topic", "orders-v2")]);
        binary.headers.push((
            "kafka_dlt-original-partition".into(),
            3i32.to_be_bytes().to_vec(),
        ));
        binary.headers.push((
            "kafka_dlt-original-offset".into(),
            4711i64.to_be_bytes().to_vec(),
        ));
        let parsed = DlqMessage::from_consumed_message(&binary, "orders-dlq");
        assert_eq!(parsed.original_partition, Some(3));
        assert_eq!(parsed.original_offset, Some(4711));

        let connect = msg(&[("__connect.errors.topic", "payments")]);
        let parsed = DlqMessage::from_consumed_message(&connect, "dlq-s3-sink");
        assert_eq!(parsed.original_topic.as_deref(), Some("payments"));

        // No header: the topic is only guessed from the DLQ name.
        assert!(!names_original_topic(&msg(&[]).headers));
    }

    #[test]
    fn test_infer_original_topic_from_dlq_name() {
        assert_eq!(
            infer_original_topic_from_dlq_name("orders-v1-dlq"),
            Some("orders-v1".to_string())
        );
        assert_eq!(
            infer_original_topic_from_dlq_name("payments.dlq"),
            Some("payments".to_string())
        );
        assert_eq!(
            infer_original_topic_from_dlq_name("notifications_dlq"),
            Some("notifications".to_string())
        );
        assert_eq!(
            infer_original_topic_from_dlq_name("dead-letter.events"),
            Some("events".to_string())
        );
        assert_eq!(
            infer_original_topic_from_dlq_name("dlq-inventory"),
            Some("inventory".to_string())
        );
        assert_eq!(infer_original_topic_from_dlq_name("normal-topic"), None);
    }

    #[test]
    fn test_classify_error_reason() {
        assert_eq!(
            classify_error_reason("JsonParseException: Unexpected character at line 1"),
            "Deserialization Error"
        );
        assert_eq!(
            classify_error_reason("SocketTimeoutException: Read timed out after 5000ms"),
            "Downstream Timeout"
        );
        assert_eq!(
            classify_error_reason("SchemaViolationException: Field 'id' missing in avro record"),
            "SchemaValidation Error".replace("Validation", " Validation")
        );
        assert_eq!(
            classify_error_reason("NullPointerException: customerProfile was null"),
            "Null Reference / Panic"
        );
        assert_eq!(
            classify_error_reason("Duplicate key violates unique constraint pk_orders"),
            "Duplicate Key / DB Conflict"
        );
        assert_eq!(
            classify_error_reason("CustomAppError: Something unusual"),
            "CustomAppError: Something unusual"
        );
    }

    #[test]
    fn test_from_consumed_message_with_headers() {
        let mut headers = HashMap::new();
        headers.insert("x-original-topic".to_string(), b"orders-v1".to_vec());
        headers.insert("x-original-partition".to_string(), b"2".to_vec());
        headers.insert("x-original-offset".to_string(), b"104".to_vec());
        headers.insert(
            "x-exception-message".to_string(),
            b"JsonParseException: Malformed token".to_vec(),
        );
        headers.insert("x-retry-count".to_string(), b"3".to_vec());

        let consumed = ConsumedMessage {
            topic: "orders-v1-dlq".to_string(),
            partition: 0,
            offset: 42,
            timestamp_ms: Some(1700000000000),
            key: Some(b"order-99".to_vec()),
            payload: Some(b"{\"order_id\": 99}".to_vec()),
            headers: headers.into_iter().collect(),
            null_headers: Vec::new(),
        };

        let dlq_msg = DlqMessage::from_consumed_message(&consumed, "orders-v1-dlq");
        assert_eq!(dlq_msg.partition, 0);
        assert_eq!(dlq_msg.offset, 42);
        assert_eq!(dlq_msg.original_topic.as_deref(), Some("orders-v1"));
        assert_eq!(dlq_msg.original_partition, Some(2));
        assert_eq!(dlq_msg.original_offset, Some(104));
        assert_eq!(
            dlq_msg.exception_message.as_deref(),
            Some("JsonParseException: Malformed token")
        );
        assert_eq!(dlq_msg.retry_count, Some(3));
        assert_eq!(dlq_msg.key.as_deref(), Some("order-99"));
    }

    #[test]
    fn test_dlq_analysis_aggregation() {
        let msg1 = DlqMessage {
            partition: 0,
            offset: 1,
            timestamp_ms: Some(1000),
            key: Some("k1".to_string()),
            payload_preview: Some("data1".to_string()),
            raw_payload: b"data1".to_vec(),
            original_topic: Some("topic-a".to_string()),
            original_partition: Some(0),
            original_offset: Some(10),
            exception_message: Some("JsonParseException: broken".to_string()),
            exception_stacktrace: None,
            retry_count: Some(1),
            headers: vec![],
            raw_key: Some(b"k".to_vec()),
            raw_headers: vec![],
            payload_is_null: false,
        };

        let msg2 = DlqMessage {
            partition: 0,
            offset: 2,
            timestamp_ms: Some(2000),
            key: Some("k2".to_string()),
            payload_preview: Some("data2".to_string()),
            raw_payload: b"data2".to_vec(),
            original_topic: Some("topic-a".to_string()),
            original_partition: Some(1),
            original_offset: Some(20),
            exception_message: Some("SocketTimeoutException: timeout".to_string()),
            exception_stacktrace: None,
            retry_count: Some(2),
            headers: vec![],
            raw_key: Some(b"k".to_vec()),
            raw_headers: vec![],
            payload_is_null: false,
        };

        let msg3 = DlqMessage {
            partition: 1,
            offset: 3,
            timestamp_ms: Some(3000),
            key: Some("k1".to_string()),
            payload_preview: Some("data3".to_string()),
            raw_payload: b"data3".to_vec(),
            original_topic: Some("topic-b".to_string()),
            original_partition: Some(0),
            original_offset: Some(30),
            exception_message: Some("serde error".to_string()),
            exception_stacktrace: None,
            retry_count: Some(1),
            headers: vec![],
            raw_key: Some(b"k".to_vec()),
            raw_headers: vec![],
            payload_is_null: false,
        };

        let analysis = DlqAnalysis::analyze(&[msg1, msg2, msg3]);
        assert_eq!(analysis.total_messages, 3);
        assert_eq!(analysis.unique_keys, 2);
        assert_eq!(analysis.total_bytes, 15);
        assert_eq!(analysis.earliest_timestamp_ms, Some(1000));
        assert_eq!(analysis.latest_timestamp_ms, Some(3000));

        // Deserialization Error should have count 2 (msg1 and msg3)
        let serde_err = analysis
            .error_categories
            .iter()
            .find(|(name, _)| name == "Deserialization Error");
        assert_eq!(serde_err.map(|(_, count)| *count), Some(2));
    }

    #[test]
    fn redrive_keeps_repeated_headers_in_order_and_replaces_old_markers() {
        let consumed = binary_dlq_record(vec![
            ("x".to_string(), b"1".to_vec()),
            ("x-redriven-by".to_string(), b"old".to_vec()),
            ("x".to_string(), b"2".to_vec()),
            ("bin".to_string(), vec![0xff, 0x00]),
        ]);
        let msg = DlqMessage::from_consumed_message(&consumed, "orders-dlq");
        let (items, _) = build_redrive_plan(&[msg], Some("orders"), true, false);
        let sent: Vec<(&str, &[u8])> = items[0]
            .headers
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_slice()))
            .collect();
        assert_eq!(
            &sent[..3],
            &[("x", &b"1"[..]), ("x", b"2"), ("bin", &[0xff, 0x00])]
        );
        assert_eq!(sent[3].0, REDRIVEN_BY_HEADER);
    }

    #[test]
    fn masked_judges_a_binary_header_by_its_bytes_not_a_lossy_copy() {
        // Valid text only after a lossy conversion would be wrong: these bytes
        // are not UTF-8, so the whole value must be hidden.
        let consumed = binary_dlq_record(vec![
            ("trace".to_string(), b"ok".to_vec()),
            ("blob".to_string(), vec![0xff, b'@', b'a', 0xfe]),
        ]);
        let masked = DlqMessage::from_consumed_message(&consumed, "d").masked();
        assert_eq!(masked.headers[0], ("trace".to_string(), "ok".to_string()));
        assert_eq!(masked.headers[1].0, "blob");
        assert!(
            masked.headers[1].1.contains("binary"),
            "{:?}",
            masked.headers
        );
    }

    #[test]
    fn redrive_skips_records_that_point_back_at_the_dlq() {
        let record = |original: &str| DlqMessage {
            partition: 0,
            offset: 1,
            timestamp_ms: None,
            key: None,
            payload_preview: None,
            raw_payload: b"x".to_vec(),
            original_topic: Some(original.to_string()),
            original_partition: None,
            original_offset: None,
            exception_message: None,
            exception_stacktrace: None,
            retry_count: None,
            headers: vec![],
            raw_key: None,
            raw_headers: vec![],
            payload_is_null: false,
        };
        let own = record("orders-dlq");
        let other = record("orders");
        let msgs = [own.clone(), other];
        let (items, summary) =
            build_redrive_plan_from(Some("orders-dlq"), &msgs, None, true, false);
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].target_topic, "orders");
        assert_eq!(summary.skipped_unresolved_topic, 1);
        // An explicit override naming the DLQ itself sends nothing.
        let (items, summary) =
            build_redrive_plan_from(Some("orders-dlq"), &msgs, Some("orders-dlq"), true, false);
        assert!(items.is_empty());
        assert_eq!(summary.skipped_unresolved_topic, 2);
    }

    #[test]
    fn test_build_redrive_plan() {
        let msg = DlqMessage {
            partition: 0,
            offset: 10,
            timestamp_ms: Some(1000),
            key: Some("k1".to_string()),
            payload_preview: Some("data".to_string()),
            raw_payload: b"payload-bytes".to_vec(),
            original_topic: Some("orders-v1".to_string()),
            original_partition: Some(0),
            original_offset: Some(5),
            exception_message: Some("error".to_string()),
            exception_stacktrace: None,
            retry_count: Some(1),
            headers: vec![("x-app-source".to_string(), "billing".to_string())],
            raw_key: Some(b"k1".to_vec()),
            raw_headers: vec![("x-app-source".to_string(), b"billing".to_vec())],
            payload_is_null: false,
        };

        let (items, summary) = build_redrive_plan(&[msg], None, true, true);
        assert_eq!(summary.total_examined, 1);
        assert_eq!(summary.planned_redrives, 1);
        assert_eq!(summary.skipped_unresolved_topic, 0);
        assert!(summary.dry_run);
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].target_topic, "orders-v1");
        assert_eq!(items[0].payload.as_deref(), Some(&b"payload-bytes"[..]));

        // Check redrive headers injected
        let has_redrive_header = items[0]
            .headers
            .iter()
            .any(|(k, v)| k == "x-redriven-by" && v == b"ferrika");
        assert!(has_redrive_header);
    }

    fn binary_dlq_record(headers: Vec<(String, Vec<u8>)>) -> ConsumedMessage {
        ConsumedMessage {
            topic: "orders-dlq".to_string(),
            partition: 1,
            offset: 7,
            timestamp_ms: None,
            key: Some(vec![0xff, 0xfe, 0x00]),
            payload: Some(vec![0x00, 0x9f]),
            headers,
            null_headers: Vec::new(),
        }
    }

    #[test]
    fn test_redrive_keeps_binary_key_and_header_bytes() {
        let headers = Vec::from([
            ("trace".to_string(), b"  padded  ".to_vec()),
            ("bin".to_string(), vec![0x80, 0x00, 0xff]),
        ]);
        let dlq_msg = DlqMessage::from_consumed_message(&binary_dlq_record(headers), "orders-dlq");
        assert_eq!(dlq_msg.key, None, "non-UTF-8 key has no text view");

        let (items, summary) = build_redrive_plan(&[dlq_msg], None, false, true);
        assert_eq!(summary.planned_redrives, 1);
        let item = &items[0];
        assert_eq!(item.target_topic, "orders");
        assert_eq!(item.key, Some(vec![0xff, 0xfe, 0x00]));
        assert_eq!(item.payload, Some(vec![0x00, 0x9f]));
        let header = |name: &str| {
            item.headers
                .iter()
                .find(|(k, _)| k == name)
                .map(|(_, v)| v.clone())
        };
        assert_eq!(header("trace"), Some(b"  padded  ".to_vec()));
        assert_eq!(header("bin"), Some(vec![0x80, 0x00, 0xff]));
    }

    #[test]
    fn test_redrive_keeps_null_payload_null() {
        let mut record = binary_dlq_record(Vec::new());
        record.payload = None;
        let dlq_msg = DlqMessage::from_consumed_message(&record, "orders-dlq");
        let (items, _) = build_redrive_plan(&[dlq_msg], None, false, true);
        assert_eq!(items[0].payload, None);
    }

    #[test]
    fn test_redrive_skips_already_redriven_records() {
        let headers = Vec::from([
            (REDRIVEN_BY_HEADER.to_string(), b"ferrika".to_vec()),
            (REDRIVE_TIMESTAMP_HEADER.to_string(), b"1".to_vec()),
        ]);
        let again = DlqMessage::from_consumed_message(&binary_dlq_record(headers), "orders-dlq");
        let fresh = DlqMessage::from_consumed_message(&binary_dlq_record(Vec::new()), "orders-dlq");
        let messages = [again, fresh];

        let (items, summary) = build_redrive_plan(&messages, None, false, true);
        assert_eq!(items.len(), 1);
        assert_eq!(summary.skipped_already_redriven, 1);
        assert_eq!(summary.total_examined, 2);

        // Opting in redrives it again with exactly one fresh marker of each kind.
        let (items, summary) = build_redrive_plan(&messages, None, false, false);
        assert_eq!(items.len(), 2);
        assert_eq!(summary.skipped_already_redriven, 0);
        let markers = items[0]
            .headers
            .iter()
            .filter(|(k, _)| k == REDRIVEN_BY_HEADER || k == REDRIVE_TIMESTAMP_HEADER)
            .count();
        assert_eq!(markers, 2);
    }

    #[test]
    fn test_redrive_start_parse() {
        let start = RedriveStart::parse(&["10".into(), "2:500".into(), " 0 : 7 ".into()]);
        let start = start.unwrap_or_default();
        assert_eq!(start.default, Some(10));
        assert_eq!(start.offset_for(2), Some(500));
        assert_eq!(start.offset_for(0), Some(7));
        assert_eq!(start.offset_for(9), Some(10));
        assert_eq!(RedriveStart::default().offset_for(0), None);

        assert!(RedriveStart::parse(&["-1".into()]).is_err());
        assert!(RedriveStart::parse(&["x:1".into()]).is_err());
        assert!(RedriveStart::parse(&["1:abc".into()]).is_err());
    }

    #[test]
    fn test_redrive_start_resume_after() {
        let start = RedriveStart {
            default: Some(5),
            partitions: BTreeMap::from([(3, 40)]),
        };
        let examined = [(0, 5), (0, 6), (1, 9), (1, 10), (1, 11)];
        let failed = [(1, 11), (1, 10)];
        let next = start.resume_after(&examined, &failed);
        assert_eq!(next.default, Some(5));
        assert_eq!(next.partitions.get(&0), Some(&7));
        assert_eq!(
            next.partitions.get(&1),
            Some(&10),
            "retry the first failure"
        );
        assert_eq!(
            next.partitions.get(&3),
            Some(&40),
            "unread partition keeps its start"
        );
        assert_eq!(next.to_args(), vec!["5", "0:7", "1:10", "3:40"]);
    }
}
