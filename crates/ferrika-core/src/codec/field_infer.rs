//! Field paths, header names and (when allowed) low-cardinality values
//! inferred from records already on screen, for filter autocomplete when a
//! topic has no schema or the schema leaves gaps. No IO.
//!
//! Only the rendering the user sees goes in (masked when masking is on).
//! Keys that may be data rather than field names (emails, ids, masked keys)
//! fold into one "any key" step, so they never become candidates.

use std::collections::{HashMap, HashSet};

use serde_json::Value;

use crate::codec::schema_fields::{FieldKind, PathStep};
use crate::model::cluster::SampleMessage;

/// What the records on screen show.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SampleCatalog {
    pub entries: Vec<SampleEntry>,
    /// Header names with the number of records carrying them, most seen first.
    pub headers: Vec<(String, u32)>,
    /// Records read.
    pub records: u32,
}

/// One path seen in the records.
#[derive(Debug, Clone, PartialEq)]
pub struct SampleEntry {
    pub steps: Vec<PathStep>,
    pub kind: FieldKind,
    /// Records the path was present in.
    pub seen: u32,
    /// Distinct string values, when they passed every masking condition and
    /// look like an enum (see [`infer`]).
    pub values: Option<Vec<String>>,
}

#[derive(Debug, Clone, Copy)]
pub struct InferOptions {
    pub max_records: usize,
    /// Larger payloads are skipped.
    pub max_payload: usize,
    pub max_depth: usize,
    pub max_entries: usize,
    /// Collect value candidates; only when [`values_allowed`] says so.
    pub collect_values: bool,
}

impl Default for InferOptions {
    fn default() -> Self {
        Self {
            max_records: 200,
            max_payload: 64 * 1024,
            max_depth: 8,
            max_entries: 500,
            collect_values: false,
        }
    }
}

/// Distinct keys at one place above which an object is a map.
const MAX_OBJECT_KEYS: usize = 50;
/// Array elements read per array.
const MAX_ARRAY_ITEMS: usize = 20;
/// A value candidate is at most this long.
const MAX_VALUE_CHARS: usize = 64;
/// Value candidates need this many string observations...
const MIN_VALUE_OBSERVATIONS: u32 = 10;
/// ...and at most this many distinct values.
const MAX_DISTINCT_VALUES: usize = 20;

/// Whether record values may be offered as candidates: never while the
/// company policy enforces masking (even after an audited unmask, which is
/// for reading, not for spreading values into a list), never while the
/// panel masks.
pub fn values_allowed(masking_enforced: bool, panel_masks: bool) -> bool {
    !masking_enforced && !panel_masks
}

/// Whether a key name reads as a field name rather than data: no PII, not
/// a number, id or hash, nothing a mask produced.
fn is_field_like(key: &str) -> bool {
    use crate::codec::pii::{mask_text, MaskStrategy};
    let hex = |s: &str| s.chars().all(|c| c.is_ascii_hexdigit());
    let uuid = key.len() == 36
        && key.chars().filter(|&c| c == '-').count() == 4
        && hex(&key.replace('-', ""));
    !key.is_empty()
        && key.chars().all(crate::model::expr::is_name_char)
        && !key.chars().all(|c| c.is_ascii_digit() || c == '-')
        && !uuid
        && !(key.len() >= 16 && hex(key))
        && mask_text(key, MaskStrategy::Redact) == key
}

/// Whether values under this path may be candidates by its field names.
fn path_allows_values(steps: &[PathStep]) -> bool {
    use crate::codec::pii::{is_account_key, is_name_key, is_sensitive_key};
    steps.iter().all(|s| match s {
        PathStep::Field(name) => {
            !is_sensitive_key(name) && !is_account_key(name) && !is_name_key(name)
        }
        _ => true,
    })
}

#[derive(Default)]
struct Acc {
    kinds: Vec<FieldKind>,
    seen: u32,
    /// The last record counted in `seen`.
    last_record: Option<u32>,
    /// Distinct object keys seen here (up to one past the map limit).
    keys: HashSet<String>,
    values: HashMap<String, u32>,
    observations: u32,
    /// A value failed a condition; no candidates here.
    poisoned: bool,
}

struct Infer {
    opts: InferOptions,
    accs: HashMap<Vec<PathStep>, Acc>,
    order: Vec<Vec<PathStep>>,
    /// Places whose keys are data.
    maps: HashSet<Vec<PathStep>>,
    record: u32,
}

impl Infer {
    fn acc(&mut self, steps: &[PathStep]) -> Option<&mut Acc> {
        if !self.accs.contains_key(steps) {
            if self.accs.len() >= self.opts.max_entries {
                return None;
            }
            self.order.push(steps.to_vec());
            self.accs.insert(steps.to_vec(), Acc::default());
        }
        self.accs.get_mut(steps)
    }

    fn visit(&mut self, value: &Value, steps: &mut Vec<PathStep>) {
        let record = self.record;
        let collect = self.opts.collect_values && path_allows_values(steps);
        if !steps.is_empty() {
            let Some(acc) = self.acc(steps) else {
                return;
            };
            if acc.last_record != Some(record) {
                acc.last_record = Some(record);
                acc.seen += 1;
            }
            let kind = match value {
                Value::Null => FieldKind::Null,
                Value::Bool(_) => FieldKind::Boolean,
                Value::Number(n) if n.is_f64() => FieldKind::Number,
                Value::Number(_) => FieldKind::Integer,
                Value::String(_) => FieldKind::String,
                Value::Array(_) => FieldKind::Array(Box::new(FieldKind::Unknown)),
                Value::Object(_) => FieldKind::Record,
            };
            if !acc.kinds.contains(&kind) {
                acc.kinds.push(kind);
            }
            if let Value::String(s) = value {
                acc.observations += 1;
                let masked =
                    crate::codec::pii::mask_text(s, crate::codec::pii::MaskStrategy::Redact);
                if !collect || s.chars().count() > MAX_VALUE_CHARS || masked != *s {
                    acc.poisoned = true;
                } else if acc.values.len() <= MAX_DISTINCT_VALUES || acc.values.contains_key(s) {
                    *acc.values.entry(s.clone()).or_default() += 1;
                }
            }
        }
        if steps.len() >= self.opts.max_depth {
            return;
        }
        match value {
            Value::Object(map) => {
                let mut fold = self.maps.contains(steps.as_slice())
                    || map.len() > MAX_OBJECT_KEYS
                    || map.keys().any(|k| !is_field_like(k));
                if !fold && !steps.is_empty() {
                    if let Some(acc) = self.accs.get_mut(steps.as_slice()) {
                        for k in map.keys() {
                            if acc.keys.len() > MAX_OBJECT_KEYS {
                                break;
                            }
                            acc.keys.insert(k.clone());
                        }
                        fold = acc.keys.len() > MAX_OBJECT_KEYS;
                    }
                }
                if fold {
                    self.maps.insert(steps.clone());
                }
                for (k, v) in map {
                    steps.push(if fold {
                        PathStep::AnyKey
                    } else {
                        PathStep::Field(k.clone())
                    });
                    self.visit(v, steps);
                    steps.pop();
                }
            }
            Value::Array(items) => {
                steps.push(PathStep::AnyIndex);
                for item in items.iter().take(MAX_ARRAY_ITEMS) {
                    self.visit(item, steps);
                }
                steps.pop();
            }
            _ => {}
        }
    }

    fn finish(self, headers: HashMap<String, u32>) -> SampleCatalog {
        // Places found to be maps after some keys were recorded as fields.
        let mut merged: HashMap<Vec<PathStep>, Acc> = HashMap::new();
        let mut order: Vec<Vec<PathStep>> = Vec::new();
        let mut accs = self.accs;
        for steps in &self.order {
            let Some(acc) = accs.remove(steps) else {
                continue;
            };
            let key = {
                let mut out: Vec<PathStep> = Vec::with_capacity(steps.len());
                for step in steps {
                    let step = match step {
                        PathStep::Field(_) if self.maps.contains(&out) => PathStep::AnyKey,
                        other => other.clone(),
                    };
                    out.push(step);
                }
                out
            };
            match merged.get_mut(&key) {
                Some(into) => {
                    for k in acc.kinds {
                        if !into.kinds.contains(&k) {
                            into.kinds.push(k);
                        }
                    }
                    into.seen = into.seen.max(acc.seen);
                    into.observations += acc.observations;
                    into.poisoned |= acc.poisoned;
                    for (v, n) in acc.values {
                        *into.values.entry(v).or_default() += n;
                    }
                }
                None => {
                    order.push(key.clone());
                    merged.insert(key, acc);
                }
            }
        }
        let maps: HashSet<Vec<PathStep>> = self.maps.iter().map(|m| m.to_vec()).collect();
        let kind_at = |steps: &[PathStep], merged: &HashMap<Vec<PathStep>, Acc>| -> FieldKind {
            kind_of(steps, merged, &maps, 0)
        };
        let entries = order
            .iter()
            .filter_map(|steps| {
                let acc = merged.get(steps)?;
                let values = (!acc.poisoned
                    && acc.observations >= MIN_VALUE_OBSERVATIONS
                    && !acc.values.is_empty()
                    && acc.values.len() <= MAX_DISTINCT_VALUES)
                    .then(|| {
                        let mut values: Vec<(String, u32)> =
                            acc.values.iter().map(|(v, n)| (v.clone(), *n)).collect();
                        values.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
                        values.into_iter().map(|(v, _)| v).collect()
                    });
                Some(SampleEntry {
                    steps: steps.clone(),
                    kind: kind_at(steps, &merged),
                    seen: acc.seen,
                    values,
                })
            })
            .collect();
        let mut headers: Vec<(String, u32)> = headers.into_iter().collect();
        headers.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        SampleCatalog {
            entries,
            headers,
            records: self.record,
        }
    }
}

/// The kind at `steps`, with array elements and map values filled in.
fn kind_of(
    steps: &[PathStep],
    merged: &HashMap<Vec<PathStep>, Acc>,
    maps: &HashSet<Vec<PathStep>>,
    depth: usize,
) -> FieldKind {
    let Some(acc) = merged.get(steps) else {
        return FieldKind::Unknown;
    };
    let child = |step: PathStep| {
        let mut path = steps.to_vec();
        path.push(step);
        if depth > 8 {
            FieldKind::Unknown
        } else {
            kind_of(&path, merged, maps, depth + 1)
        }
    };
    let only_null = acc.kinds.iter().all(|k| *k == FieldKind::Null);
    let kinds = acc
        .kinds
        .iter()
        .filter(|k| **k != FieldKind::Null)
        .map(|k| match k {
            FieldKind::Array(_) => FieldKind::Array(Box::new(child(PathStep::AnyIndex))),
            FieldKind::Record if maps.contains(steps) => {
                FieldKind::Map(Box::new(child(PathStep::AnyKey)))
            }
            other => other.clone(),
        })
        .collect();
    FieldKind::combine(kinds, only_null)
}

/// Infers paths from `records` (the newest first), as shown on screen.
pub fn infer<'a>(
    records: impl Iterator<Item = &'a SampleMessage>,
    opts: &InferOptions,
) -> SampleCatalog {
    let mut infer = Infer {
        opts: *opts,
        accs: HashMap::new(),
        order: Vec::new(),
        maps: HashSet::new(),
        record: 0,
    };
    let mut headers: HashMap<String, u32> = HashMap::new();
    for record in records.take(opts.max_records) {
        for name in record
            .headers
            .iter()
            .map(|(k, _)| k)
            .collect::<HashSet<_>>()
        {
            *headers.entry(name.clone()).or_default() += 1;
        }
        infer.record += 1;
        if record.payload.len() > opts.max_payload {
            continue;
        }
        let Ok(value) = serde_json::from_str::<Value>(&record.payload) else {
            continue;
        };
        infer.visit(&value, &mut Vec::new());
    }
    infer.finish(headers)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::schema_fields::render_path;

    fn record(payload: &str, headers: &[(&str, &str)]) -> SampleMessage {
        SampleMessage {
            topic: "t".into(),
            partition: 0,
            offset: 0,
            key: None,
            payload: payload.into(),
            timestamp_ms: 0,
            headers: headers
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            raw: None,
            schema: None,
            schema_ref: None,
        }
    }

    fn with_values() -> InferOptions {
        InferOptions {
            collect_values: true,
            ..InferOptions::default()
        }
    }

    fn paths(c: &SampleCatalog) -> Vec<String> {
        c.entries.iter().map(|e| render_path(&e.steps)).collect()
    }

    fn entry<'a>(c: &'a SampleCatalog, path: &str) -> &'a SampleEntry {
        c.entries
            .iter()
            .find(|e| render_path(&e.steps) == path)
            .unwrap_or_else(|| panic!("{path} not in {:?}", paths(c)))
    }

    #[test]
    fn reads_json_records_and_skips_the_rest() {
        let big = format!(r#"{{"big":"{}"}}"#, "x".repeat(70 * 1024));
        let records = [
            record(
                r#"{"a":1,"b":{"c":"x"},"items":[{"sku":"s"},{"sku":"t","qty":2}]}"#,
                &[("trace", "1")],
            ),
            record("plain text", &[("trace", "2"), ("auth", "secret")]),
            record(&big, &[]),
            record(r#"{"a":2.5,"n":null}"#, &[]),
        ];
        let c = infer(records.iter(), &InferOptions::default());
        assert_eq!(c.records, 4);
        assert_eq!(
            paths(&c),
            [
                "$.a",
                "$.b",
                "$.b.c",
                "$.items",
                "$.items[0]",
                "$.items[0].sku",
                "$.items[0].qty",
                "$.n"
            ]
        );
        assert_eq!(entry(&c, "$.a").kind, FieldKind::Number);
        assert_eq!(entry(&c, "$.a").seen, 2);
        assert_eq!(
            entry(&c, "$.items").kind,
            FieldKind::Array(Box::new(FieldKind::Record))
        );
        assert_eq!(entry(&c, "$.items[0].sku").seen, 1);
        assert_eq!(entry(&c, "$.n").kind, FieldKind::Null);
        assert_eq!(
            c.headers,
            [("trace".to_string(), 2), ("auth".to_string(), 1)]
        );
        // No values unless asked for.
        assert!(c.entries.iter().all(|e| e.values.is_none()));
    }

    #[test]
    fn data_keys_fold_into_any_key() {
        let records = [
            record(
                r#"{"by_email":{"jane@x.com":1},"by_id":{"123":true},"ok":{"color":"red"}}"#,
                &[],
            ),
            record(
                r#"{"by_uuid":{"550e8400-e29b-41d4-a716-446655440000":1},"masked":{"j***@corp.com":1}}"#,
                &[],
            ),
            record(
                r#"{"hash":{"0123456789abcdef0123":1},"spaced":{"two words":1}}"#,
                &[],
            ),
        ];
        let c = infer(records.iter(), &InferOptions::default());
        let p = paths(&c);
        for folded in ["by_email", "by_id", "by_uuid", "masked", "hash", "spaced"] {
            assert!(p.contains(&format!("$.{folded}[\"…\"]")), "{folded}: {p:?}");
        }
        assert!(p.contains(&"$.ok.color".to_string()), "{p:?}");
        assert!(
            !p.iter().any(|x| x.contains('@') || x.contains("550e")),
            "{p:?}"
        );
        assert_eq!(
            entry(&c, "$.by_email").kind,
            FieldKind::Map(Box::new(FieldKind::Integer))
        );
        // Many distinct keys at one place: a map, also for keys seen earlier.
        let many: Vec<SampleMessage> = (0..60)
            .map(|i| record(&format!(r#"{{"m":{{"k{i}":1}}}}"#), &[]))
            .collect();
        let c = infer(many.iter(), &InferOptions::default());
        assert_eq!(paths(&c), ["$.m", "$.m[\"…\"]"]);
    }

    #[test]
    fn values_pass_every_masking_condition() {
        let mut records = Vec::new();
        for i in 0..12 {
            let status = ["PAID", "NEW"][i % 2];
            records.push(record(
                &format!(
                    r#"{{"status":"{status}","password":"p{}","last_name":"Kim","email":"a{i}@x.com","note":"4111 1111 1111 1111","few":"x"}}"#,
                    i % 2
                ),
                &[],
            ));
        }
        records.push(record(r#"{"few":"y"}"#, &[]));
        let c = infer(records.iter(), &with_values());
        assert_eq!(
            entry(&c, "$.status").values.as_deref(),
            Some(&["NEW".to_string(), "PAID".to_string()][..])
        );
        // Sensitive and name keys, PII values, too many distinct values: none.
        for path in ["$.password", "$.last_name", "$.email", "$.note"] {
            assert!(entry(&c, path).values.is_none(), "{path}");
        }
        // Low cardinality but enough observations.
        assert_eq!(entry(&c, "$.few").values.as_ref().map(Vec::len), Some(2));
        // Too few observations.
        let c = infer(records[..5].iter(), &with_values());
        assert!(entry(&c, "$.status").values.is_none());
        // Off unless allowed.
        let c = infer(records.iter(), &InferOptions::default());
        assert!(entry(&c, "$.status").values.is_none());
    }

    #[test]
    fn values_are_never_allowed_under_masking() {
        assert!(values_allowed(false, false));
        assert!(!values_allowed(true, false));
        assert!(!values_allowed(false, true));
        // Enforced policy stays off even after an audited unmask turned the
        // panel's masking off.
        assert!(!values_allowed(true, false));
    }
}
