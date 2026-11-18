use std::collections::HashMap;
use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};

use ferrika_core::error::{KafkaError, KafkaResult};

/// An individual message record entry evaluated during topic compaction analysis
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactedRecord {
    pub key: String,
    /// The record has no key at all (a null key). An empty-string key is a
    /// real key and leaves this `false`.
    #[serde(default)]
    pub null_key: bool,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: i64,
    pub is_tombstone: bool,
    pub payload_bytes: usize,
    pub payload_preview: Option<String>,
}

impl CompactedRecord {
    /// Marks the record as having a null key (no key bytes at all).
    pub fn with_null_key(mut self) -> Self {
        self.null_key = true;
        self.key.clear();
        self
    }

    pub fn new(
        key: impl Into<String>,
        partition: i32,
        offset: i64,
        timestamp: i64,
        is_tombstone: bool,
        payload_bytes: usize,
        payload_preview: Option<String>,
    ) -> Self {
        Self {
            key: key.into(),
            null_key: false,
            partition,
            offset,
            timestamp,
            is_tombstone,
            payload_bytes,
            payload_preview,
        }
    }
}

/// Aggregated compaction and deduplication efficiency metrics
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct CompactionMetrics {
    pub total_messages: usize,
    pub unique_keys: usize,
    pub duplicate_messages: usize,
    pub tombstone_count: usize,
    pub null_key_count: usize,
    pub total_payload_bytes: usize,
    pub latest_payload_bytes: usize,
    pub redundant_payload_bytes: usize,
    pub dirty_ratio_pct: f64,
    pub savings_pct: f64,
}

impl CompactionMetrics {
    /// Computes compaction metrics across a sequential stream of consumed records
    pub fn compute(records: &[CompactedRecord]) -> Self {
        let mut key_latest: HashMap<&str, &CompactedRecord> = HashMap::new();
        let mut total_bytes = 0;
        let mut tombstones = 0;
        let mut null_keys = 0;

        for r in records {
            total_bytes += r.payload_bytes;
            if r.is_tombstone {
                tombstones += 1;
            }
            if r.null_key {
                null_keys += 1;
            } else {
                key_latest.insert(&r.key, r);
            }
        }

        let total_messages = records.len();
        let unique_keys = key_latest.len();
        let duplicate_messages = total_messages.saturating_sub(unique_keys + null_keys);

        let latest_payload_bytes: usize = key_latest.values().map(|r| r.payload_bytes).sum();
        let redundant_payload_bytes = total_bytes.saturating_sub(latest_payload_bytes);

        let dirty_ratio_pct = if total_messages > 0 {
            (duplicate_messages as f64) / (total_messages as f64) * 100.0
        } else {
            0.0
        };

        let savings_pct = if total_bytes > 0 {
            (redundant_payload_bytes as f64) / (total_bytes as f64) * 100.0
        } else {
            0.0
        };

        Self {
            total_messages,
            unique_keys,
            duplicate_messages,
            tombstone_count: tombstones,
            null_key_count: null_keys,
            total_payload_bytes: total_bytes,
            latest_payload_bytes,
            redundant_payload_bytes,
            dirty_ratio_pct,
            savings_pct,
        }
    }
}

/// Deduplicated Key-Value State Snapshot derived from a compacted topic
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactedStateSnapshot {
    pub topic: String,
    pub snapshot_timestamp: i64,
    pub entries: Vec<SnapshotEntry>,
}

/// Single key-value entry in the compacted state snapshot
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotEntry {
    pub key: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: i64,
    pub payload: Option<String>,
    pub is_tombstone: bool,
}

impl CompactedStateSnapshot {
    /// Builds a deduplicated snapshot retaining only the latest record for each key
    pub fn build(topic: &str, records: &[CompactedRecord], include_tombstones: bool) -> Self {
        let mut key_map: HashMap<&str, &CompactedRecord> = HashMap::new();

        for r in records {
            if !r.null_key {
                key_map.insert(&r.key, r);
            }
        }

        let mut entries = Vec::new();
        for (_, r) in key_map {
            if !include_tombstones && r.is_tombstone {
                continue;
            }
            entries.push(SnapshotEntry {
                key: r.key.clone(),
                partition: r.partition,
                offset: r.offset,
                timestamp: r.timestamp,
                payload: r.payload_preview.clone(),
                is_tombstone: r.is_tombstone,
            });
        }

        // Sort by key for deterministic exports
        entries.sort_by(|a, b| a.key.cmp(&b.key));

        let now = ferrika_core::kafka::clock::now_ms();

        Self {
            topic: topic.to_string(),
            snapshot_timestamp: now,
            entries,
        }
    }

    /// Saves the snapshot entries as JSON Lines (.jsonl) to disk
    pub fn save_jsonl(&self, path: &Path) -> KafkaResult<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| {
                KafkaError::Io(format!(
                    "Failed to create directory {}: {e}",
                    parent.display()
                ))
            })?;
        }

        let mut lines = Vec::with_capacity(self.entries.len());
        for entry in &self.entries {
            let line = serde_json::to_string(entry).map_err(|e| {
                KafkaError::Serialization(format!("Failed to serialize snapshot entry: {e}"))
            })?;
            lines.push(line);
        }

        let content = lines.join("\n");
        fs::write(path, content).map_err(|e| {
            KafkaError::Io(format!(
                "Failed to write snapshot to {}: {e}",
                path.display()
            ))
        })?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_string_key_is_a_key_and_null_key_is_not() {
        let records = vec![
            CompactedRecord::new("", 0, 0, 1, false, 1, Some("empty-old".into())),
            CompactedRecord::new("", 0, 1, 2, false, 1, Some("empty-new".into())),
            CompactedRecord::new("", 0, 2, 3, false, 1, Some("nokey".into())).with_null_key(),
        ];
        let snap = CompactedStateSnapshot::build("t", &records, false);
        assert_eq!(snap.entries.len(), 1);
        assert_eq!(snap.entries[0].key, "");
        assert_eq!(snap.entries[0].payload.as_deref(), Some("empty-new"));
        let m = CompactionMetrics::compute(&records);
        assert_eq!(m.null_key_count, 1);
        assert_eq!(m.unique_keys, 1);
    }

    #[test]
    fn test_compaction_metrics_computation() {
        let records = vec![
            // Key1: 3 revisions (100, 150, 200 bytes)
            CompactedRecord::new("user-1", 0, 1, 1000, false, 100, Some("v1".into())),
            CompactedRecord::new("user-1", 0, 2, 1010, false, 150, Some("v2".into())),
            CompactedRecord::new("user-1", 0, 3, 1020, false, 200, Some("v3".into())),
            // Key2: 1 revision
            CompactedRecord::new("user-2", 0, 4, 1030, false, 80, Some("profile".into())),
            // Key3: created then tombstoned (0 bytes)
            CompactedRecord::new("user-3", 0, 5, 1040, false, 50, Some("initial".into())),
            CompactedRecord::new("user-3", 0, 6, 1050, true, 0, None),
            // Null key
            CompactedRecord::new("", 0, 7, 1060, false, 30, Some("unkeyed".into())).with_null_key(),
        ];

        let metrics = CompactionMetrics::compute(&records);

        assert_eq!(metrics.total_messages, 7);
        assert_eq!(metrics.unique_keys, 3); // user-1, user-2, user-3
        assert_eq!(metrics.duplicate_messages, 3); // user-1 (x2) + user-3 (x1)
        assert_eq!(metrics.tombstone_count, 1);
        assert_eq!(metrics.null_key_count, 1);

        // Total bytes = 100 + 150 + 200 + 80 + 50 + 0 + 30 = 610
        assert_eq!(metrics.total_payload_bytes, 610);
        // Latest bytes = user-1 (200) + user-2 (80) + user-3 (0) = 280
        assert_eq!(metrics.latest_payload_bytes, 280);
        // Redundant bytes = 610 - 280 = 330
        assert_eq!(metrics.redundant_payload_bytes, 330);

        assert!(metrics.dirty_ratio_pct > 40.0);
        assert!(metrics.savings_pct > 50.0);
    }

    #[test]
    fn test_compacted_state_snapshot_deduplication() {
        let records = vec![
            CompactedRecord::new("k1", 0, 1, 100, false, 10, Some("v1".into())),
            CompactedRecord::new("k1", 0, 2, 200, false, 12, Some("v2".into())),
            CompactedRecord::new("k2", 0, 3, 300, false, 15, Some("v_k2".into())),
            CompactedRecord::new("k3", 0, 4, 400, true, 0, None),
        ];

        // Without tombstones
        let snapshot = CompactedStateSnapshot::build("orders", &records, false);
        assert_eq!(snapshot.entries.len(), 2);
        assert_eq!(snapshot.entries[0].key, "k1");
        assert_eq!(snapshot.entries[0].payload.as_deref(), Some("v2"));
        assert_eq!(snapshot.entries[1].key, "k2");

        // With tombstones
        let snapshot_with_tombstones = CompactedStateSnapshot::build("orders", &records, true);
        assert_eq!(snapshot_with_tombstones.entries.len(), 3);
        assert_eq!(snapshot_with_tombstones.entries[2].key, "k3");
        assert!(snapshot_with_tombstones.entries[2].is_tombstone);
    }
}
