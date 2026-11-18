//! Message Studio filters the user saved, in `~/.ferrika/filters.json` next
//! to `profiles.json`.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{KafkaError, KafkaResult};
use crate::kafka::profile::ProfileStore;

/// One saved filter: the text typed in the filter box, and whether it is a
/// regular expression.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SavedFilter {
    pub query: String,
    #[serde(default)]
    pub regex: bool,
}

/// `filters.json` in the directory that holds `profiles.json`.
pub fn filters_path() -> PathBuf {
    ProfileStore::default_path().with_file_name("filters.json")
}

/// The saved filters; none when the file is missing or unreadable.
pub fn load(path: &Path) -> Vec<SavedFilter> {
    std::fs::read_to_string(path)
        .ok()
        .and_then(|text| serde_json::from_str(&text).ok())
        .unwrap_or_default()
}

pub fn save(path: &Path, filters: &[SavedFilter]) -> KafkaResult<()> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)
            .map_err(|e| KafkaError::File(format!("Cannot create '{}': {e}", dir.display())))?;
    }
    let text = serde_json::to_string_pretty(filters)
        .map_err(|e| KafkaError::Serialization(e.to_string()))?;
    std::fs::write(path, text)
        .map_err(|e| KafkaError::File(format!("Cannot write '{}': {e}", path.display())))
}

/// Adds `filter` (moving it to the front if it is already saved) and keeps
/// at most `limit` filters.
pub fn remember(filters: &mut Vec<SavedFilter>, filter: SavedFilter, limit: usize) {
    filters.retain(|f| f != &filter);
    filters.insert(0, filter);
    filters.truncate(limit);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filters_round_trip_and_stay_unique() {
        let dir = std::env::temp_dir().join(format!("ferrika-filters-{}", std::process::id()));
        let path = dir.join("filters.json");
        assert!(load(&path).is_empty());
        let mut filters = Vec::new();
        let f = |q: &str, regex| SavedFilter {
            query: q.to_string(),
            regex,
        };
        remember(&mut filters, f("error", false), 2);
        remember(&mut filters, f("^user-\\d+", true), 2);
        remember(&mut filters, f("error", false), 2);
        assert_eq!(filters, vec![f("error", false), f("^user-\\d+", true)]);
        remember(&mut filters, f("third", false), 2);
        assert_eq!(filters.len(), 2);
        save(&path, &filters).unwrap();
        assert_eq!(load(&path), filters);
        let _ = std::fs::remove_dir_all(dir);
    }
}
