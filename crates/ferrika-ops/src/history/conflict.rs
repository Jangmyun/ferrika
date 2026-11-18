//! Whether a recorded change is still in place: the current state against
//! the recorded before- and after-state (pure functions).

use std::collections::BTreeSet;

use serde::Serialize;

use ferrika_core::model::history::{Operation, State, NONE};

/// One item whose current value is not what the change left.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DriftRow {
    /// The resource, e.g. "topic orders".
    pub subject: String,
    /// The item, e.g. a config key or a partition.
    pub item: String,
    pub before: String,
    /// What the change set it to.
    pub recorded: String,
    pub current: String,
}

/// How the current state compares with the recorded change.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "verdict", rename_all = "snake_case")]
pub enum Verdict {
    /// The change is in place; undo can run.
    Clean,
    /// The state is already what it was before; nothing to undo.
    AlreadyReverted,
    /// Something changed it since; undo would overwrite that.
    Drift { rows: Vec<DriftRow> },
}

/// Compares `current` (one state per recorded before-state) with `op`.
/// When the after-state was not recorded (the change did not report back),
/// only "already reverted" can be told apart; anything else is drift.
pub fn check(op: &Operation, current: &[State]) -> Verdict {
    let before = &op.begin.before;
    let after = op.after();
    let mut all_clean = after.len() == before.len();
    let mut all_reverted = true;
    let mut rows = Vec::new();
    for (ix, (was, now)) in before.iter().zip(current).enumerate() {
        let recorded = after.get(ix).filter(|a| same_kind(a, was));
        let clean = recorded.is_some_and(|a| now.same_as(a));
        let reverted = now.same_as(was);
        all_clean &= clean;
        all_reverted &= reverted;
        if !clean {
            rows.extend(drift_rows(was, recorded, now));
        }
    }
    if current.len() != before.len() {
        all_clean = false;
        all_reverted = false;
    }
    if all_clean {
        Verdict::Clean
    } else if all_reverted {
        Verdict::AlreadyReverted
    } else {
        Verdict::Drift { rows }
    }
}

fn same_kind(a: &State, b: &State) -> bool {
    std::mem::discriminant(a) == std::mem::discriminant(b)
}

/// The items of one resource whose current value is not the recorded one.
fn drift_rows(before: &State, recorded: Option<&State>, current: &State) -> Vec<DriftRow> {
    let before_entries = before.entries();
    let recorded_entries = recorded.map(State::entries);
    let current_entries = current.entries();
    let items: BTreeSet<&String> = before_entries
        .keys()
        .chain(recorded_entries.iter().flat_map(|e| e.keys()))
        .chain(current_entries.keys())
        .collect();
    let value = |map: &std::collections::BTreeMap<String, String>, item: &String| {
        map.get(item).cloned().unwrap_or_else(|| NONE.to_string())
    };
    items
        .into_iter()
        .filter_map(|item| {
            let recorded = match &recorded_entries {
                Some(map) => value(map, item),
                None => "(not recorded)".to_string(),
            };
            let current = value(&current_entries, item);
            (current != recorded).then(|| DriftRow {
                subject: before.subject(),
                item: item.clone(),
                before: value(&before_entries, item),
                recorded,
                current,
            })
        })
        .collect()
}

/// Later operations that touched something `op` touched and are still in
/// effect (not failed, not undone, not an undo of `op`). Undoing `op` first
/// would silently drop their changes, so undo asks for them to be undone
/// first (or `--force`).
pub fn later_overlaps<'a>(op: &Operation, all: &'a [Operation]) -> Vec<&'a Operation> {
    let keys: BTreeSet<String> = op.resource_keys().into_iter().collect();
    let Some(position) = all.iter().position(|o| o.id == op.id) else {
        return Vec::new();
    };
    all[position + 1..]
        .iter()
        .filter(|later| {
            later.ok() != Some(false)
                && later.undone_by.is_none()
                && later.begin.undoes.as_deref() != Some(op.id.as_str())
                && later.begin.reversibility
                    != ferrika_core::model::history::Reversibility::NotApplicable
                && later.resource_keys().iter().any(|k| keys.contains(k))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrika_core::model::history::{Begin, ConfigValue, End, Reversibility, Scope};
    use std::collections::BTreeMap;

    fn config(value: Option<&str>) -> State {
        State::TopicConfig {
            topic: "orders".into(),
            keys: BTreeMap::from([(
                "retention.ms".to_string(),
                ConfigValue {
                    own: value.map(str::to_string),
                    sensitive: false,
                },
            )]),
        }
    }

    fn op(id: &str, before: Option<&str>, after: Option<&str>) -> Operation {
        Operation {
            id: id.into(),
            time_ms: 1,
            begin: Begin {
                source: "cli".into(),
                user: "u".into(),
                scope: Scope::default(),
                action: "topics config".into(),
                target: "orders".into(),
                reversibility: Reversibility::Reversible,
                limits: vec![],
                before: vec![config(before)],
                after: vec![config(after)],
                info: vec![],
                undoes: None,
            },
            end: Some(End {
                ok: true,
                outcome: String::new(),
                after: None,
            }),
            ended_ms: None,
            notes: vec![],
            undone_by: None,
        }
    }

    #[test]
    fn current_state_is_clean_reverted_or_drifted() {
        let changed = op("A", Some("1"), Some("2"));
        assert_eq!(check(&changed, &[config(Some("2"))]), Verdict::Clean);
        assert_eq!(
            check(&changed, &[config(Some("1"))]),
            Verdict::AlreadyReverted
        );
        let Verdict::Drift { rows } = check(&changed, &[config(Some("3"))]) else {
            panic!("expected drift");
        };
        assert_eq!(
            rows,
            vec![DriftRow {
                subject: "topic orders".into(),
                item: "retention.ms".into(),
                before: "1".into(),
                recorded: "2".into(),
                current: "3".into(),
            }]
        );
        // A default before the change shows as such.
        let Verdict::Drift { rows } = check(&op("B", None, Some("2")), &[config(Some("3"))]) else {
            panic!("expected drift");
        };
        assert_eq!(rows[0].before, "(default)");
    }

    #[test]
    fn unrecorded_after_state_is_only_clean_when_reverted() {
        let mut unknown = op("A", Some("1"), Some("2"));
        unknown.begin.after.clear();
        assert_eq!(
            check(&unknown, &[config(Some("1"))]),
            Verdict::AlreadyReverted
        );
        let Verdict::Drift { rows } = check(&unknown, &[config(Some("2"))]) else {
            panic!("expected drift");
        };
        assert_eq!(rows[0].recorded, "(not recorded)");
    }

    #[test]
    fn later_changes_to_the_same_key_must_be_undone_first() {
        let first = op("A", Some("1"), Some("2"));
        let second = op("B", Some("2"), Some("3"));
        let mut failed = op("C", Some("3"), Some("4"));
        failed.end.as_mut().unwrap().ok = false;
        let all = vec![first.clone(), second, failed];
        let overlaps: Vec<&str> = later_overlaps(&first, &all)
            .iter()
            .map(|o| o.id.as_str())
            .collect();
        assert_eq!(overlaps, vec!["B"]);
        let mut undone = all.clone();
        undone[1].undone_by = Some("U".into());
        assert!(later_overlaps(&first, &undone).is_empty());
    }
}
