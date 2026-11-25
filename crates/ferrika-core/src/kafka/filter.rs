use regex::{Regex, RegexBuilder};

use crate::error::{KafkaError, KafkaResult};
use crate::kafka::consumer::ConsumedMessage;

/// Splits a `KEY:VALUE` header argument at the first colon.
///
/// Both sides are trimmed of surrounding whitespace (the one rule shared by
/// `consume`/`mirror --header-filter` and `produce -H`). A missing colon or an
/// empty key is an error, so a typo can never silently drop a filter.
pub fn parse_header_pair(raw: &str) -> KafkaResult<(&str, &str)> {
    match raw.split_once(':') {
        Some((key, value)) if !key.trim().is_empty() => Ok((key.trim(), value.trim())),
        _ => Err(KafkaError::config(format!(
            "Invalid header '{raw}'. Expected KEY:VALUE (e.g. trace_id:abc123)"
        ))),
    }
}

/// One `--header-filter`: the header `key` (compared case-insensitively)
/// must be present with a value that contains `value` (`KEY:VALUE`,
/// case-insensitive unless the filter is case-sensitive) or equals it
/// exactly (`KEY=VALUE`, always case-sensitive).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeaderFilter {
    pub key: String,
    pub value: String,
    /// `KEY=VALUE`: the whole value must be equal.
    pub exact: bool,
}

/// Parses a `--header-filter` argument: `KEY:VALUE` (the value contains
/// VALUE) or `KEY=VALUE` (the value is exactly VALUE). The first `:` or `=`
/// splits, so `a=b:c` is an exact match for `b:c`. Both sides are trimmed;
/// a missing separator or an empty key is an error.
pub fn parse_header_filter(raw: &str) -> KafkaResult<HeaderFilter> {
    match raw.find([':', '=']) {
        Some(at) if !raw[..at].trim().is_empty() => Ok(HeaderFilter {
            key: raw[..at].trim().to_string(),
            value: raw[at + 1..].trim().to_string(),
            exact: raw[at..].starts_with('='),
        }),
        _ => Err(KafkaError::config(format!(
            "Invalid header filter '{raw}'. Expected KEY:VALUE (value contains) or \
             KEY=VALUE (exact value), e.g. trace_id=abc123"
        ))),
    }
}

/// Checks that `partition` is one of the topic's `existing` partition ids.
pub fn ensure_partition(topic: &str, existing: &[i32], partition: i32) -> KafkaResult<()> {
    if existing.contains(&partition) {
        Ok(())
    } else {
        Err(KafkaError::PartitionNotFound {
            topic: topic.to_string(),
            partition,
        })
    }
}

/// Filter criteria for matching Kafka consumed messages in real time.
#[derive(Debug, Clone, Default)]
pub struct MessageFilter {
    /// Regular expression to match against the message payload.
    pub grep_pattern: Option<String>,
    grep_regex: Option<Regex>,
    /// Pattern to match against message key (substring search).
    pub key_pattern: Option<String>,
    /// Required headers; every one must match.
    pub header_filters: Vec<HeaderFilter>,
    /// Minimum message timestamp in epoch milliseconds (inclusive).
    pub from_timestamp_ms: Option<i64>,
    /// Maximum message timestamp in epoch milliseconds (inclusive).
    pub to_timestamp_ms: Option<i64>,
    /// Whether pattern matching should be case-sensitive. Defaults to false.
    pub case_sensitive: bool,
}

impl MessageFilter {
    /// Creates a new empty filter that accepts all messages.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets and compiles the payload regular expression.
    ///
    /// Matching is case-insensitive unless [`Self::with_case_sensitive`] was
    /// called first. Invalid expressions are rejected before consumption starts.
    pub fn with_grep(mut self, pattern: impl Into<String>) -> KafkaResult<Self> {
        let pattern = pattern.into();
        let regex = RegexBuilder::new(&pattern)
            .case_insensitive(!self.case_sensitive)
            .build()
            .map_err(|err| {
                KafkaError::config(format!("Invalid --grep regex '{pattern}': {err}"))
            })?;
        self.grep_pattern = Some(pattern);
        self.grep_regex = Some(regex);
        Ok(self)
    }

    /// Sets the key search pattern.
    pub fn with_key(mut self, pattern: impl Into<String>) -> Self {
        self.key_pattern = Some(pattern.into());
        self
    }

    /// Adds a required header whose value contains `value`. Every header
    /// filter added must match, also several for one key.
    pub fn with_header(self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.with_header_filter(HeaderFilter {
            key: key.into(),
            value: value.into(),
            exact: false,
        })
    }

    /// Adds a required header whose value is exactly `value`.
    pub fn with_header_exact(self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.with_header_filter(HeaderFilter {
            key: key.into(),
            value: value.into(),
            exact: true,
        })
    }

    /// Adds a required header (see [`parse_header_filter`]).
    pub fn with_header_filter(mut self, filter: HeaderFilter) -> Self {
        self.header_filters.push(filter);
        self
    }

    /// Sets timestamp range in epoch milliseconds.
    pub fn with_timestamp_range(mut self, from: Option<i64>, to: Option<i64>) -> Self {
        self.from_timestamp_ms = from;
        self.to_timestamp_ms = to;
        self
    }

    /// Configures case-sensitivity.
    pub fn with_case_sensitive(mut self, case_sensitive: bool) -> Self {
        self.case_sensitive = case_sensitive;
        self
    }

    /// Returns true if at least one filter criterion is active.
    pub fn is_active(&self) -> bool {
        self.grep_pattern.is_some()
            || self.key_pattern.is_some()
            || !self.header_filters.is_empty()
            || self.from_timestamp_ms.is_some()
            || self.to_timestamp_ms.is_some()
    }

    /// Evaluates whether a given `ConsumedMessage` satisfies all filter criteria.
    pub fn matches(&self, msg: &ConsumedMessage) -> bool {
        if !self.is_active() {
            return true;
        }

        // 1. Key check
        if let Some(ref pattern) = self.key_pattern {
            let key_str = match msg.key_str() {
                Some(k) => k,
                None => return false,
            };
            if !self.str_matches(key_str, pattern) {
                return false;
            }
        }

        // 2. Payload grep check
        if let Some(ref regex) = self.grep_regex {
            let payload_str = match msg.payload_str() {
                Some(p) => p,
                None => return false,
            };
            if !regex.is_match(payload_str) {
                return false;
            }
        }

        // 3. Header checks: every filter must match.
        for required in &self.header_filters {
            // Find header with case-insensitive key comparison; of a repeated
            // key the last one counts, as when headers were a map.
            let found_val = msg.headers.iter().rev().find_map(|(k, v)| {
                if k.eq_ignore_ascii_case(&required.key) {
                    Some(String::from_utf8_lossy(v).to_string())
                } else {
                    None
                }
            });

            let matched = match found_val {
                Some(val) if required.exact => val == required.value,
                Some(val) => self.str_matches(&val, &required.value),
                None => false,
            };
            if !matched {
                return false;
            }
        }

        // 4. Timestamp range check
        if let Some(from_ts) = self.from_timestamp_ms {
            match msg.timestamp_ms {
                Some(ts) if ts >= from_ts => {}
                _ => return false,
            }
        }

        if let Some(to_ts) = self.to_timestamp_ms {
            match msg.timestamp_ms {
                Some(ts) if ts <= to_ts => {}
                _ => return false,
            }
        }

        true
    }

    #[inline]
    fn str_matches(&self, haystack: &str, needle: &str) -> bool {
        if self.case_sensitive {
            haystack.contains(needle)
        } else {
            haystack.to_lowercase().contains(&needle.to_lowercase())
        }
    }
}

/// Statistics tracking message consumption and filtering results.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FilterStats {
    /// Total messages received from broker.
    pub total_received: u64,
    /// Messages that matched the active filter criteria.
    pub matched: u64,
    /// Messages skipped because they did not match the criteria.
    pub skipped: u64,
}

impl FilterStats {
    /// Records a message that successfully matched.
    pub fn record_match(&mut self) {
        self.total_received += 1;
        self.matched += 1;
    }

    /// Records a message that was skipped.
    pub fn record_skip(&mut self) {
        self.record_skips(1);
    }

    /// Records `count` skipped messages.
    pub fn record_skips(&mut self, count: u64) {
        self.total_received += count;
        self.skipped += count;
    }

    /// Returns a formatted summary string.
    pub fn summary_string(&self) -> String {
        format!(
            "Total Processed: {} | Matched: {} | Skipped: {}",
            self.total_received, self.matched, self.skipped
        )
    }
}

/// Evaluates a field expression (`consume --where`) on a record, using the decoded key and value when the
/// record was schema-encoded.
pub fn expression_matches(
    expr: &crate::model::expr::Expr,
    msg: &ConsumedMessage,
    decoded: Option<&crate::codec::schema::DecodedPayload>,
    decoded_key: Option<&crate::codec::schema::DecodedPayload>,
) -> bool {
    let value = match decoded {
        Some(d) => Some(d.text.clone()),
        None => msg
            .payload
            .as_deref()
            .map(|p| String::from_utf8_lossy(p).into_owned()),
    };
    let key = match decoded_key {
        Some(d) => Some(crate::codec::schema::key_text(&d.text)),
        None => msg
            .key
            .as_deref()
            .map(|k| String::from_utf8_lossy(k).into_owned()),
    };
    let headers: Vec<(String, String)> = msg
        .headers
        .iter()
        .map(|(k, v)| (k.clone(), String::from_utf8_lossy(v).into_owned()))
        .collect();
    expr.matches(&crate::model::expr::RecordView {
        key: key.as_deref(),
        value: value.as_deref(),
        headers: &headers,
        partition: msg.partition,
        offset: msg.offset,
        timestamp_ms: msg.timestamp_ms.unwrap_or(0),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_message(
        key: Option<&str>,
        payload: Option<&str>,
        headers: &[(&str, &str)],
        timestamp: Option<i64>,
    ) -> ConsumedMessage {
        let header_map: Vec<(String, Vec<u8>)> = headers
            .iter()
            .map(|(k, v)| (k.to_string(), v.as_bytes().to_vec()))
            .collect();

        ConsumedMessage {
            topic: "test-topic".to_string(),
            partition: 0,
            offset: 100,
            timestamp_ms: timestamp,
            key: key.map(|k| k.as_bytes().to_vec()),
            payload: payload.map(|p| p.as_bytes().to_vec()),
            headers: header_map,
            null_headers: Vec::new(),
        }
    }

    #[test]
    fn test_empty_filter_matches_all() {
        let filter = MessageFilter::new();
        assert!(!filter.is_active());

        let msg = make_test_message(Some("k1"), Some("Hello World"), &[], Some(1000));
        assert!(filter.matches(&msg));
    }

    #[test]
    fn test_grep_payload_case_insensitive() {
        let filter = MessageFilter::new().with_grep("error").unwrap();
        assert!(filter.is_active());

        let matching_msg = make_test_message(None, Some("Fatal ERROR: timeout"), &[], None);
        assert!(filter.matches(&matching_msg));

        let non_matching_msg = make_test_message(None, Some("INFO: order completed"), &[], None);
        assert!(!filter.matches(&non_matching_msg));
    }

    #[test]
    fn test_grep_payload_case_sensitive() {
        let filter = MessageFilter::new()
            .with_case_sensitive(true)
            .with_grep("Error")
            .unwrap();

        let matching_msg = make_test_message(None, Some("Fatal Error occurred"), &[], None);
        assert!(filter.matches(&matching_msg));

        let non_matching_msg = make_test_message(None, Some("Fatal ERROR occurred"), &[], None);
        assert!(!filter.matches(&non_matching_msg));
    }

    #[test]
    fn test_grep_payload_regex() {
        let filter = MessageFilter::new()
            .with_grep(r"ORDER_(CREATED|UPDATED)")
            .unwrap();

        assert!(filter.matches(&make_test_message(
            None,
            Some("event=order_created"),
            &[],
            None,
        )));
        assert!(filter.matches(&make_test_message(
            None,
            Some("event=ORDER_UPDATED"),
            &[],
            None,
        )));
        assert!(!filter.matches(&make_test_message(
            None,
            Some("event=ORDER_DELETED"),
            &[],
            None,
        )));
    }

    #[test]
    fn test_grep_rejects_invalid_regex() {
        let err = MessageFilter::new().with_grep("ORDER_(").unwrap_err();
        assert!(err.to_string().contains("Invalid --grep regex"));
    }

    #[test]
    fn test_key_filter() {
        let filter = MessageFilter::new().with_key("order-99");

        let matching_msg = make_test_message(Some("order-9941"), Some("payload"), &[], None);
        assert!(filter.matches(&matching_msg));

        let non_matching_msg = make_test_message(Some("order-1234"), Some("payload"), &[], None);
        assert!(!filter.matches(&non_matching_msg));

        let no_key_msg = make_test_message(None, Some("payload"), &[], None);
        assert!(!filter.matches(&no_key_msg));
    }

    #[test]
    fn header_filter_judges_the_last_header_of_a_repeated_key() {
        let filter = MessageFilter::new().with_header("x", "2");
        let msg = make_test_message(None, Some("d"), &[("x", "1"), ("X", "2")], None);
        assert!(filter.matches(&msg));
        let msg = make_test_message(None, Some("d"), &[("x", "2"), ("x", "1")], None);
        assert!(!filter.matches(&msg));
        // Every filter must match, also several for one key.
        let filter = MessageFilter::new()
            .with_header("x", "1")
            .with_header("x", "2");
        assert!(!filter.matches(&make_test_message(None, Some("d"), &[("x", "2")], None)));
        assert!(!filter.matches(&make_test_message(None, Some("d"), &[("x", "1")], None)));
        assert!(filter.matches(&make_test_message(None, Some("d"), &[("x", "12")], None)));
    }

    #[test]
    fn exact_header_filter_needs_the_whole_value() {
        let filter = MessageFilter::new().with_header_exact("env", "prod");
        assert!(filter.matches(&make_test_message(
            None,
            Some("d"),
            &[("ENV", "prod")],
            None
        )));
        assert!(!filter.matches(&make_test_message(
            None,
            Some("d"),
            &[("env", "prod-eu")],
            None
        )));
        assert!(!filter.matches(&make_test_message(
            None,
            Some("d"),
            &[("env", "PROD")],
            None
        )));
        // A substring filter still matches the longer value.
        let filter = MessageFilter::new().with_header("env", "prod");
        assert!(filter.matches(&make_test_message(
            None,
            Some("d"),
            &[("env", "prod-eu")],
            None
        )));
        // Both kinds together: all must match.
        let filter = MessageFilter::new()
            .with_header_exact("env", "prod")
            .with_header("region", "eu");
        assert!(filter.matches(&make_test_message(
            None,
            Some("d"),
            &[("env", "prod"), ("region", "eu-west")],
            None
        )));
        assert!(!filter.matches(&make_test_message(
            None,
            Some("d"),
            &[("env", "prod"), ("region", "us")],
            None
        )));
    }

    #[test]
    fn parse_header_filter_splits_at_the_first_colon_or_equals_sign() {
        let f = parse_header_filter("env:prod").unwrap();
        assert_eq!(
            (f.key.as_str(), f.value.as_str(), f.exact),
            ("env", "prod", false)
        );
        let f = parse_header_filter(" env = prod ").unwrap();
        assert_eq!(
            (f.key.as_str(), f.value.as_str(), f.exact),
            ("env", "prod", true)
        );
        let f = parse_header_filter("a=b:c").unwrap();
        assert_eq!(
            (f.key.as_str(), f.value.as_str(), f.exact),
            ("a", "b:c", true)
        );
        let f = parse_header_filter("a:b=c").unwrap();
        assert_eq!(
            (f.key.as_str(), f.value.as_str(), f.exact),
            ("a", "b=c", false)
        );
        let f = parse_header_filter("empty=").unwrap();
        assert_eq!((f.value.as_str(), f.exact), ("", true));
        assert!(parse_header_filter("noSeparator").is_err());
        assert!(parse_header_filter("=value").is_err());
        assert!(parse_header_filter(" :value").is_err());
    }

    #[test]
    fn test_header_filter() {
        let filter = MessageFilter::new().with_header("trace-id", "xyz-123");

        let matching_msg = make_test_message(
            None,
            Some("data"),
            &[("TRACE-ID", "xyz-123"), ("env", "prod")],
            None,
        );
        assert!(filter.matches(&matching_msg));

        let wrong_header_msg =
            make_test_message(None, Some("data"), &[("trace-id", "other-id")], None);
        assert!(!filter.matches(&wrong_header_msg));

        let missing_header_msg = make_test_message(None, Some("data"), &[("env", "prod")], None);
        assert!(!filter.matches(&missing_header_msg));
    }

    #[test]
    fn parse_header_pair_splits_at_first_colon_and_trims() {
        assert_eq!(parse_header_pair("env:prod").unwrap(), ("env", "prod"));
        assert_eq!(
            parse_header_pair(" env : a:b ").unwrap(),
            ("env", "a:b"),
            "only the first colon splits; both sides are trimmed"
        );
        assert_eq!(parse_header_pair("empty:").unwrap(), ("empty", ""));
    }

    #[test]
    fn parse_header_pair_rejects_missing_colon_and_empty_key() {
        assert!(parse_header_pair("noColon").is_err());
        assert!(parse_header_pair(":value").is_err());
        assert!(parse_header_pair("  :value").is_err());
    }

    #[test]
    fn ensure_partition_rejects_unknown_partition() {
        assert!(ensure_partition("t", &[0, 1], 1).is_ok());
        assert!(matches!(
            ensure_partition("t", &[0, 1], 7),
            Err(KafkaError::PartitionNotFound { partition: 7, .. })
        ));
    }

    #[test]
    fn test_timestamp_range_filter() {
        let filter = MessageFilter::new().with_timestamp_range(Some(1000), Some(2000));

        let msg_in_range = make_test_message(None, None, &[], Some(1500));
        assert!(filter.matches(&msg_in_range));

        let msg_too_early = make_test_message(None, None, &[], Some(999));
        assert!(!filter.matches(&msg_too_early));

        let msg_too_late = make_test_message(None, None, &[], Some(2001));
        assert!(!filter.matches(&msg_too_late));

        let msg_no_ts = make_test_message(None, None, &[], None);
        assert!(!filter.matches(&msg_no_ts));
    }

    #[test]
    fn test_filter_stats() {
        let mut stats = FilterStats::default();
        stats.record_match();
        stats.record_match();
        stats.record_skip();

        assert_eq!(stats.total_received, 3);
        assert_eq!(stats.matched, 2);
        assert_eq!(stats.skipped, 1);
        assert_eq!(
            stats.summary_string(),
            "Total Processed: 3 | Matched: 2 | Skipped: 1"
        );
    }
}
