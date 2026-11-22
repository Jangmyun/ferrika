use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};

use ferrika_core::codec::pii::{ellipsize, mask_bytes, mask_payload, MaskStrategy};
use ferrika_core::kafka::consumer::ConsumedMessage;

/// Type of difference found between the records at the same partition and
/// offset of Topic A and Topic B
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RecordDiffType {
    /// Keys and payloads are identical
    Identical,
    /// The position exists in both topics, but the key or payload differs
    Modified,
    /// The position exists in Topic B, but is missing in Topic A
    MissingInA,
    /// The position exists in Topic A, but is missing in Topic B
    MissingInB,
}

impl RecordDiffType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Identical => "Identical",
            Self::Modified => "Modified",
            Self::MissingInA => "Missing in A (+B)",
            Self::MissingInB => "Missing in B (-B)",
        }
    }

    pub fn symbol(&self) -> &'static str {
        match self {
            Self::Identical => " ",
            Self::Modified => "~",
            Self::MissingInA => "+",
            Self::MissingInB => "-",
        }
    }
}

/// A specific field-level change between two JSON payloads
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FieldDiff {
    pub path: String,
    pub val_a: Option<String>,
    pub val_b: Option<String>,
}

/// The [`FieldDiff::path`] of a key change.
pub const KEY_PATH: &str = "(key)";

/// Shown for a tombstone (a record without a payload), which differs from
/// an empty payload.
pub const TOMBSTONE: &str = "(tombstone)";

/// Shown for an empty payload.
pub(crate) const EMPTY: &str = "(empty)";

/// One record to compare, at its partition and offset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiffRecord {
    pub partition: i32,
    pub offset: i64,
    pub key: Option<Vec<u8>>,
    /// `None` for a tombstone.
    pub payload: Option<Vec<u8>>,
}

impl From<&ConsumedMessage> for DiffRecord {
    fn from(msg: &ConsumedMessage) -> Self {
        Self {
            partition: msg.partition,
            offset: msg.offset,
            key: msg.key.clone(),
            payload: msg.payload.clone(),
        }
    }
}

/// Comparison result for one partition/offset position across Topic A and Topic B
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordDiff {
    pub partition: i32,
    pub offset: i64,
    /// The record key (Topic A's when both have the position); a key change
    /// is listed in `field_diffs` under [`KEY_PATH`].
    pub key: String,
    pub diff_type: RecordDiffType,
    pub field_diffs: Vec<FieldDiff>,
    pub payload_preview_a: Option<String>,
    pub payload_preview_b: Option<String>,
}

/// Statistical summary of topic-to-topic content parity and schema drift
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicDiffSummary {
    pub topic_a: String,
    pub topic_b: String,
    /// Partition/offset positions compared.
    pub total_compared_keys: usize,
    pub identical_count: usize,
    pub modified_count: usize,
    pub missing_in_a_count: usize,
    pub missing_in_b_count: usize,
    pub parity_pct: f64,
    pub records: Vec<RecordDiff>,
}

type Position = (i32, i64);

fn by_position(records: &[DiffRecord]) -> BTreeMap<Position, &DiffRecord> {
    records
        .iter()
        .map(|r| ((r.partition, r.offset), r))
        .collect()
}

/// A key as shown.
fn key_text(key: Option<&[u8]>) -> String {
    match key {
        // A non-UTF-8 key still identifies the record; show it lossily.
        Some(k) => String::from_utf8_lossy(k).into_owned(),
        None => "(no key)".to_string(),
    }
}

/// The differences between two records at the same position, or `None`
/// when they are identical. Tombstones and empty payloads differ.
fn record_differences(a: &DiffRecord, b: &DiffRecord) -> Option<Vec<FieldDiff>> {
    let key_differs = a.key != b.key;
    let payload_diffs = payload_differences(a.payload.as_deref(), b.payload.as_deref());
    if !key_differs && payload_diffs.is_none() {
        return None;
    }
    let mut diffs = Vec::new();
    if key_differs {
        diffs.push(FieldDiff {
            path: KEY_PATH.to_string(),
            val_a: Some(key_text(a.key.as_deref())),
            val_b: Some(key_text(b.key.as_deref())),
        });
    }
    diffs.extend(payload_diffs.unwrap_or_default());
    Some(diffs)
}

/// The differences between two payloads (`None`: a tombstone), or `None`
/// when they are equal (byte for byte, or as JSON).
fn payload_differences(a: Option<&[u8]>, b: Option<&[u8]>) -> Option<Vec<FieldDiff>> {
    match (a, b) {
        (None, None) => None,
        (Some(a), Some(b)) => {
            if a == b || is_semantically_equal_json(a, b) {
                None
            } else {
                Some(diff_payloads(a, b))
            }
        }
        (a, b) => Some(vec![FieldDiff {
            path: "$".to_string(),
            val_a: Some(payload_text(a)),
            val_b: Some(payload_text(b)),
        }]),
    }
}

/// A whole payload as shown in a field diff.
fn payload_text(payload: Option<&[u8]>) -> String {
    match payload {
        None => TOMBSTONE.to_string(),
        Some([]) => EMPTY.to_string(),
        Some(p) => String::from_utf8_lossy(p).into_owned(),
    }
}

impl TopicDiffSummary {
    /// Compares the records at the same partition/offset positions of
    /// Topic A and Topic B.
    pub fn compute(
        topic_a: &str,
        records_a: &[DiffRecord],
        topic_b: &str,
        records_b: &[DiffRecord],
    ) -> Self {
        let map_a = by_position(records_a);
        let map_b = by_position(records_b);
        let mut positions: Vec<Position> = map_a.keys().chain(map_b.keys()).copied().collect();
        positions.sort_unstable();
        positions.dedup();

        let mut diff_records = Vec::with_capacity(positions.len());
        let mut identical_count = 0;
        let mut modified_count = 0;
        let mut missing_in_a_count = 0;
        let mut missing_in_b_count = 0;

        for (partition, offset) in positions {
            let a = map_a.get(&(partition, offset)).copied();
            let b = map_b.get(&(partition, offset)).copied();
            let (diff_type, field_diffs) = match (a, b) {
                (Some(a), Some(b)) => match record_differences(a, b) {
                    None => {
                        identical_count += 1;
                        (RecordDiffType::Identical, Vec::new())
                    }
                    Some(diffs) => {
                        modified_count += 1;
                        (RecordDiffType::Modified, diffs)
                    }
                },
                (Some(_), None) => {
                    missing_in_b_count += 1;
                    (RecordDiffType::MissingInB, Vec::new())
                }
                (None, Some(_)) => {
                    missing_in_a_count += 1;
                    (RecordDiffType::MissingInA, Vec::new())
                }
                (None, None) => continue,
            };
            let key = a
                .or(b)
                .map(|r| key_text(r.key.as_deref()))
                .unwrap_or_default();
            diff_records.push(RecordDiff {
                partition,
                offset,
                key,
                diff_type,
                field_diffs,
                payload_preview_a: a.map(|r| preview_payload(r.payload.as_deref())),
                payload_preview_b: b.map(|r| preview_payload(r.payload.as_deref())),
            });
        }

        let total = diff_records.len();
        let parity_pct = if total > 0 {
            (identical_count as f64 / total as f64) * 100.0
        } else {
            100.0
        };

        Self {
            topic_a: topic_a.to_string(),
            topic_b: topic_b.to_string(),
            total_compared_keys: total,
            identical_count,
            modified_count,
            missing_in_a_count,
            missing_in_b_count,
            parity_pct,
            records: diff_records,
        }
    }
}

impl TopicDiffSummary {
    /// A copy for display while the company policy enforces masking. Records
    /// are still compared unmasked (masking could make different values look
    /// alike); keys, previews and field deltas are rebuilt from masked
    /// payloads. `records_a`/`records_b` are the inputs given to [`Self::compute`].
    pub fn masked(&self, records_a: &[DiffRecord], records_b: &[DiffRecord]) -> Self {
        fn masked_records(records: &[DiffRecord]) -> BTreeMap<Position, DiffRecord> {
            records
                .iter()
                .map(|r| {
                    let masked = DiffRecord {
                        partition: r.partition,
                        offset: r.offset,
                        key: r
                            .key
                            .as_deref()
                            .map(|k| mask_bytes(k, MaskStrategy::Partial).into_bytes()),
                        payload: r
                            .payload
                            .as_deref()
                            .map(|p| mask_bytes(p, MaskStrategy::Partial).into_bytes()),
                    };
                    ((r.partition, r.offset), masked)
                })
                .collect()
        }
        let (map_a, map_b) = (masked_records(records_a), masked_records(records_b));
        let mut summary = self.clone();
        for r in &mut summary.records {
            let a = map_a.get(&(r.partition, r.offset));
            let b = map_b.get(&(r.partition, r.offset));
            r.payload_preview_a = a.map(|p| preview_payload(p.payload.as_deref()));
            r.payload_preview_b = b.map(|p| preview_payload(p.payload.as_deref()));
            if r.diff_type == RecordDiffType::Modified {
                r.field_diffs = match (a, b) {
                    (Some(a), Some(b)) => record_differences(a, b).unwrap_or_default(),
                    _ => Vec::new(),
                };
                if r.field_diffs.is_empty() {
                    // Only masked values changed.
                    r.field_diffs.push(FieldDiff {
                        path: "$".to_string(),
                        val_a: Some("(masked)".to_string()),
                        val_b: Some("(masked)".to_string()),
                    });
                }
            }
            r.key = mask_payload(&r.key, MaskStrategy::Partial);
        }
        summary
    }
}

/// Truncates payload for concise terminal and table display
fn preview_payload(payload: Option<&[u8]>) -> String {
    match payload {
        None => TOMBSTONE.to_string(),
        Some([]) => EMPTY.to_string(),
        Some(bytes) => ellipsize(String::from_utf8_lossy(bytes).trim(), 60),
    }
}

/// Compares two payloads, performing deep recursive JSON field comparison if both are valid JSON
fn diff_payloads(bytes_a: &[u8], bytes_b: &[u8]) -> Vec<FieldDiff> {
    let json_a: Result<serde_json::Value, _> = serde_json::from_slice(bytes_a);
    let json_b: Result<serde_json::Value, _> = serde_json::from_slice(bytes_b);

    if let (Ok(val_a), Ok(val_b)) = (json_a, json_b) {
        let mut diffs = Vec::new();
        diff_json_values("$", &val_a, &val_b, &mut diffs);
        diffs
    } else {
        // Fallback to raw string comparison
        let str_a = String::from_utf8_lossy(bytes_a).to_string();
        let str_b = String::from_utf8_lossy(bytes_b).to_string();
        vec![FieldDiff {
            path: "$".to_string(),
            val_a: Some(str_a),
            val_b: Some(str_b),
        }]
    }
}

/// Recursively traverses two JSON trees to detect added, removed, or modified fields
pub(crate) fn diff_json_values(
    path: &str,
    a: &serde_json::Value,
    b: &serde_json::Value,
    diffs: &mut Vec<FieldDiff>,
) {
    match (a, b) {
        (serde_json::Value::Object(map_a), serde_json::Value::Object(map_b)) => {
            let mut keys: HashSet<&str> = HashSet::new();
            for k in map_a.keys() {
                keys.insert(k.as_str());
            }
            for k in map_b.keys() {
                keys.insert(k.as_str());
            }

            let mut sorted_keys: Vec<&str> = keys.into_iter().collect();
            sorted_keys.sort();

            for key in sorted_keys {
                let current_path = if path == "$" {
                    format!("$.{key}")
                } else {
                    format!("{path}.{key}")
                };

                match (map_a.get(key), map_b.get(key)) {
                    (Some(val_a), Some(val_b)) => {
                        diff_json_values(&current_path, val_a, val_b, diffs);
                    }
                    (Some(val_a), None) => {
                        diffs.push(FieldDiff {
                            path: current_path,
                            val_a: Some(val_a.to_string()),
                            val_b: None,
                        });
                    }
                    (None, Some(val_b)) => {
                        diffs.push(FieldDiff {
                            path: current_path,
                            val_a: None,
                            val_b: Some(val_b.to_string()),
                        });
                    }
                    (None, None) => {}
                }
            }
        }
        (serde_json::Value::Array(arr_a), serde_json::Value::Array(arr_b)) => {
            if arr_a != arr_b {
                diffs.push(FieldDiff {
                    path: path.to_string(),
                    val_a: Some(serde_json::to_string(arr_a).unwrap_or_default()),
                    val_b: Some(serde_json::to_string(arr_b).unwrap_or_default()),
                });
            }
        }
        (val_a, val_b) => {
            if val_a != val_b {
                diffs.push(FieldDiff {
                    path: path.to_string(),
                    val_a: Some(val_a.to_string()),
                    val_b: Some(val_b.to_string()),
                });
            }
        }
    }
}

/// Checks if two byte slices parse into equivalent JSON objects regardless of formatting
fn is_semantically_equal_json(a: &[u8], b: &[u8]) -> bool {
    if let (Ok(val_a), Ok(val_b)) = (
        serde_json::from_slice::<serde_json::Value>(a),
        serde_json::from_slice::<serde_json::Value>(b),
    ) {
        val_a == val_b
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_diff_json_values_nested() {
        let json_a: serde_json::Value = serde_json::json!({
            "order_id": "101",
            "amount": 5000,
            "status": "PENDING",
            "user": {
                "id": "u1",
                "email": "alice@example.com"
            }
        });

        let json_b: serde_json::Value = serde_json::json!({
            "order_id": "101",
            "amount": 5500,
            "status": "SETTLED",
            "user": {
                "id": "u1",
                "email": "alice@example.com",
                "verified": true
            }
        });

        let mut diffs = Vec::new();
        diff_json_values("$", &json_a, &json_b, &mut diffs);

        assert_eq!(diffs.len(), 3);

        // amount modified
        let amount_diff = diffs.iter().find(|d| d.path == "$.amount").unwrap();
        assert_eq!(amount_diff.val_a.as_deref(), Some("5000"));
        assert_eq!(amount_diff.val_b.as_deref(), Some("5500"));

        // status modified
        let status_diff = diffs.iter().find(|d| d.path == "$.status").unwrap();
        assert_eq!(status_diff.val_a.as_deref(), Some("\"PENDING\""));
        assert_eq!(status_diff.val_b.as_deref(), Some("\"SETTLED\""));

        // user.verified added in B
        let user_diff = diffs.iter().find(|d| d.path == "$.user.verified").unwrap();
        assert_eq!(user_diff.val_a, None);
        assert_eq!(user_diff.val_b.as_deref(), Some("true"));
    }

    fn rec(partition: i32, offset: i64, key: Option<&str>, payload: Option<&[u8]>) -> DiffRecord {
        DiffRecord {
            partition,
            offset,
            key: key.map(|k| k.as_bytes().to_vec()),
            payload: payload.map(<[u8]>::to_vec),
        }
    }

    #[test]
    fn test_identical_topics_compare_identical_whatever_the_read_order() {
        let a = vec![
            rec(0, 0, Some("k"), Some(b"1")),
            rec(1, 0, Some("k"), Some(b"2")),
            rec(0, 1, None, Some(b"x")),
        ];
        // The same records, read in another partition interleaving.
        let b = vec![a[1].clone(), a[2].clone(), a[0].clone()];
        let summary = TopicDiffSummary::compute("a", &a, "b", &b);
        assert_eq!(summary.total_compared_keys, 3);
        assert_eq!(summary.identical_count, 3);
        assert_eq!(summary.parity_pct, 100.0);
        let positions: Vec<_> = summary
            .records
            .iter()
            .map(|r| (r.partition, r.offset))
            .collect();
        assert_eq!(positions, [(0, 0), (0, 1), (1, 0)]);
    }

    #[test]
    fn test_tombstones_differ_from_empty_payloads() {
        let a = vec![
            rec(0, 0, Some("gone"), None),
            rec(0, 1, Some("both-dead"), None),
            rec(0, 2, Some("empty"), Some(b"")),
        ];
        let b = vec![
            rec(0, 0, Some("gone"), Some(b"")),
            rec(0, 1, Some("both-dead"), None),
            rec(0, 2, Some("empty"), Some(b"")),
        ];
        let summary = TopicDiffSummary::compute("a", &a, "b", &b);
        assert_eq!(summary.identical_count, 2);
        assert_eq!(summary.modified_count, 1);
        let changed = &summary.records[0];
        assert_eq!(changed.diff_type, RecordDiffType::Modified);
        assert_eq!(changed.field_diffs[0].val_a.as_deref(), Some(TOMBSTONE));
        assert_eq!(changed.field_diffs[0].val_b.as_deref(), Some(EMPTY));
        assert_eq!(changed.payload_preview_a.as_deref(), Some(TOMBSTONE));
    }

    #[test]
    fn test_key_change_at_the_same_position_is_a_modification() {
        let a = vec![rec(0, 5, Some("k1"), Some(br#"{"n":1}"#))];
        let b = vec![rec(0, 5, Some("k2"), Some(br#"{ "n": 1 }"#))];
        let summary = TopicDiffSummary::compute("a", &a, "b", &b);
        assert_eq!(summary.modified_count, 1);
        let diffs = &summary.records[0].field_diffs;
        assert_eq!(diffs.len(), 1, "{diffs:?}");
        assert_eq!(diffs[0].path, KEY_PATH);
        assert_eq!(diffs[0].val_b.as_deref(), Some("k2"));
    }

    #[test]
    fn test_topic_diff_summary_computation() {
        let records_a = vec![
            rec(0, 0, Some("k1"), Some(br#"{"id": 1, "status": "ok"}"#)),
            rec(0, 1, Some("k2"), Some(br#"{"id": 2, "status": "ok"}"#)),
            rec(0, 2, Some("k3"), Some(br#"{"id": 3, "status": "ok"}"#)),
        ];

        let records_b = vec![
            rec(0, 0, Some("k1"), Some(br#"{"id": 1, "status": "ok"}"#)), // Identical
            rec(0, 1, Some("k2"), Some(br#"{"id": 2, "status": "changed"}"#)), // Modified
            rec(1, 0, Some("k4"), Some(br#"{"id": 4, "status": "ok"}"#)), // Missing in A
        ];

        let summary = TopicDiffSummary::compute("topic-a", &records_a, "topic-b", &records_b);

        assert_eq!(summary.total_compared_keys, 4);
        assert_eq!(summary.identical_count, 1);
        assert_eq!(summary.modified_count, 1);
        assert_eq!(summary.missing_in_b_count, 1); // 0@2
        assert_eq!(summary.missing_in_a_count, 1); // 1@0
        assert_eq!(summary.parity_pct, 25.0);
        let modified = &summary.records[1];
        assert_eq!(modified.field_diffs[0].path, "$.status");
    }
}
