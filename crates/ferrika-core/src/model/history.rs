//! Operation history: what a cluster change looked like before and after,
//! so `ferrika undo` can put it back (see `crates/ferrika-ops/src/history.rs` for the
//! journal file).
//!
//! A change is recorded as resource states ([`State`]): the state just
//! before it ran and the state it was meant to leave. Undo restores the
//! before-state; comparing the current state with both tells whether the
//! change is still in place (clean), already gone, or changed since (drift).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::model::acl::AclBinding;
use crate::model::schema::SchemaReference;

/// Version of the journal line format.
pub const FORMAT_VERSION: u32 = 1;

/// Whether, and how far, an operation can be undone.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Reversibility {
    /// Undo restores the before-state.
    Reversible,
    /// Undo restores only part of it; `limits` says what is not restored.
    Partial,
    /// Undo works only while a condition holds (e.g. a new topic is empty).
    Conditional,
    /// Cannot be undone (deleted data, unknown passwords, appended records).
    Irreversible,
    /// A transient effect (leader election, restarts); recorded only.
    NotApplicable,
}

impl Reversibility {
    /// Whether `undo` has anything to run for it.
    pub fn undoable(self) -> bool {
        matches!(self, Self::Reversible | Self::Partial | Self::Conditional)
    }

    /// The UNDO column of `ferrika history`.
    pub fn label(self) -> &'static str {
        match self {
            Self::Reversible => "yes",
            Self::Partial => "partial",
            Self::Conditional => "if empty",
            Self::Irreversible => "no",
            Self::NotApplicable => "n/a",
        }
    }
}

/// Which cluster (or Schema Registry / Kafka Connect) an operation ran on.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Scope {
    /// Profile name at the time; may be empty.
    #[serde(default)]
    pub profile: String,
    /// Bootstrap servers as configured.
    #[serde(default)]
    pub bootstrap: String,
    /// The Kafka cluster id from Metadata; empty when it could not be read.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub cluster_id: String,
    /// Schema Registry URL without credentials, for schema operations.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub registry: String,
    /// Kafka Connect URL without credentials, for connector operations.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub connect: String,
}

impl Scope {
    /// Whether an operation recorded with `self` belongs to the cluster
    /// `current` describes: same cluster id when both have one, else the
    /// same set of bootstrap servers (order and spacing ignored). Registry
    /// and Connect operations compare those URLs instead.
    pub fn same_cluster(&self, current: &Scope) -> bool {
        if !self.registry.is_empty() {
            return self.registry == current.registry;
        }
        if !self.connect.is_empty() {
            return self.connect == current.connect;
        }
        if !self.cluster_id.is_empty() && !current.cluster_id.is_empty() {
            return self.cluster_id == current.cluster_id;
        }
        bootstrap_set(&self.bootstrap) == bootstrap_set(&current.bootstrap)
    }

    /// "cluster 4L62… (profile prod-a)" for messages.
    pub fn describe(&self) -> String {
        let place = if !self.registry.is_empty() {
            format!("Schema Registry {}", self.registry)
        } else if !self.connect.is_empty() {
            format!("Kafka Connect {}", self.connect)
        } else if !self.cluster_id.is_empty() {
            format!("cluster {}", self.cluster_id)
        } else {
            format!("brokers {}", self.bootstrap)
        };
        if self.profile.is_empty() {
            place
        } else {
            format!("{place} (profile {})", self.profile)
        }
    }
}

/// The bootstrap list as a sorted set of lower-case `host:port`.
fn bootstrap_set(servers: &str) -> Vec<String> {
    let mut set: Vec<String> = servers
        .split(',')
        .map(|s| s.trim().to_ascii_lowercase())
        .filter(|s| !s.is_empty())
        .collect();
    set.sort();
    set.dedup();
    set
}

/// One config key's state on a topic or broker.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigValue {
    /// The value set on the resource itself (topic override, per-broker
    /// dynamic config); `None` when it comes from a default.
    pub own: Option<String>,
    /// The broker hides the value; it cannot be restored.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub sensitive: bool,
}

/// A partition's replica list.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionReplicas {
    pub topic: String,
    pub partition: i32,
    pub replicas: Vec<i32>,
}

/// A group's committed offset on one partition (`None`: not committed).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionOffset {
    pub topic: String,
    pub partition: i32,
    pub offset: Option<i64>,
}

/// Whether one ACL binding exists.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AclPresence {
    pub binding: AclBinding,
    pub present: bool,
}

/// One live version of a subject.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaVersion {
    pub version: u32,
    pub id: u32,
    #[serde(default)]
    pub schema_type: String,
    /// The schema text; empty when only ids were needed.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub schema: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub references: Vec<SchemaReference>,
}

/// The state of one resource an operation touched, limited to what the
/// operation changes (the keys it sets, the partitions it moves, ...).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum State {
    TopicConfig {
        topic: String,
        keys: BTreeMap<String, ConfigValue>,
    },
    BrokerConfig {
        broker: i32,
        keys: BTreeMap<String, ConfigValue>,
    },
    /// Whether a topic exists (topic creation).
    Topic {
        topic: String,
        exists: bool,
    },
    Replicas {
        partitions: Vec<PartitionReplicas>,
    },
    GroupOffsets {
        group: String,
        offsets: Vec<PartitionOffset>,
    },
    Acls {
        bindings: Vec<AclPresence>,
    },
    Quota {
        entity: Vec<(String, Option<String>)>,
        key: String,
        value: Option<f64>,
    },
    Scram {
        user: String,
        mechanism: String,
        exists: bool,
    },
    SubjectCompat {
        subject: String,
        /// The subject's own level; `None` = the registry default applies.
        level: Option<String>,
    },
    SubjectVersions {
        subject: String,
        versions: Vec<SchemaVersion>,
    },
    Connector {
        name: String,
        /// `None` when there is no such connector.
        config: Option<BTreeMap<String, String>>,
        #[serde(default)]
        paused: bool,
        /// Config keys whose values are kept out of the journal (secrets).
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        secret_keys: Vec<String>,
    },
}

/// The value [`State::entries`] gives an item that is absent.
pub const NONE: &str = "(none)";

impl State {
    /// What the state is about, e.g. "topic orders" or "group billing".
    pub fn subject(&self) -> String {
        match self {
            Self::TopicConfig { topic, .. } | Self::Topic { topic, .. } => format!("topic {topic}"),
            Self::BrokerConfig { broker, .. } => format!("broker {broker}"),
            Self::Replicas { partitions } => {
                let mut topics: Vec<&str> = partitions.iter().map(|p| p.topic.as_str()).collect();
                topics.dedup();
                format!("replicas of {}", topics.join(", "))
            }
            Self::GroupOffsets { group, .. } => format!("group {group}"),
            Self::Acls { .. } => "ACLs".to_string(),
            Self::Quota { entity, .. } => {
                format!("quota {}", crate::model::quota::quota_entity_label(entity))
            }
            Self::Scram { user, .. } => format!("user {user}"),
            Self::SubjectCompat { subject, .. } | Self::SubjectVersions { subject, .. } => {
                format!("subject {subject}")
            }
            Self::Connector { name, .. } => format!("connector {name}"),
        }
    }

    /// The state as item -> value text, for comparing two states and for
    /// showing before / recorded / current side by side. Absent items are
    /// left out (read as [`NONE`]); values that cannot be read (sensitive
    /// configs, connector secrets) are a fixed placeholder, so they never
    /// count as a difference.
    pub fn entries(&self) -> BTreeMap<String, String> {
        let mut out = BTreeMap::new();
        match self {
            Self::TopicConfig { keys, .. } | Self::BrokerConfig { keys, .. } => {
                for (key, value) in keys {
                    let text = if value.sensitive {
                        "(sensitive)".to_string()
                    } else {
                        value.own.clone().unwrap_or_else(|| "(default)".to_string())
                    };
                    out.insert(key.clone(), text);
                }
            }
            Self::Topic { exists, .. } => {
                out.insert("exists".into(), yes_no(*exists));
            }
            Self::Replicas { partitions } => {
                for p in partitions {
                    out.insert(
                        format!("{}-{}", p.topic, p.partition),
                        format!("{:?}", p.replicas),
                    );
                }
            }
            Self::GroupOffsets { offsets, .. } => {
                for p in offsets {
                    if let Some(offset) = p.offset {
                        out.insert(format!("{}-{}", p.topic, p.partition), offset.to_string());
                    }
                }
            }
            Self::Acls { bindings } => {
                for b in bindings {
                    out.insert(acl_label(&b.binding), present_absent(b.present));
                }
            }
            Self::Quota { key, value, .. } => {
                if let Some(value) = value {
                    out.insert(key.clone(), value.to_string());
                }
            }
            Self::Scram {
                mechanism, exists, ..
            } => {
                out.insert(mechanism.clone(), present_absent(*exists));
            }
            Self::SubjectCompat { level, .. } => {
                out.insert(
                    "compatibility".into(),
                    level.clone().unwrap_or_else(|| "(inherited)".to_string()),
                );
            }
            Self::SubjectVersions { versions, .. } => {
                for v in versions {
                    out.insert(format!("schema id {}", v.id), format!("v{}", v.version));
                }
            }
            Self::Connector {
                config,
                paused,
                secret_keys,
                ..
            } => {
                out.insert("exists".into(), yes_no(config.is_some()));
                if let Some(config) = config {
                    out.insert("paused".into(), yes_no(*paused));
                    for (key, value) in config {
                        let text = if secret_keys.contains(key) {
                            "(secret)".to_string()
                        } else {
                            value.clone()
                        };
                        out.insert(format!("config {key}"), text);
                    }
                }
            }
        }
        out
    }

    /// Whether `self` and `other` describe the same state, as far as undo
    /// can tell. Schema versions compare by schema id only: re-registering
    /// gives a schema a new version number but the same id.
    pub fn same_as(&self, other: &State) -> bool {
        if let (
            Self::SubjectVersions { versions: a, .. },
            Self::SubjectVersions { versions: b, .. },
        ) = (self, other)
        {
            let ids = |v: &[SchemaVersion]| {
                let mut ids: Vec<u32> = v.iter().map(|s| s.id).collect();
                ids.sort_unstable();
                ids
            };
            return ids(a) == ids(b);
        }
        self.entries() == other.entries()
    }

    /// Keys naming what the state covers, e.g. `topic:orders/config:retention.ms`.
    /// Two operations whose keys overlap touched the same thing.
    pub fn resource_keys(&self) -> Vec<String> {
        match self {
            Self::TopicConfig { topic, keys } => keys
                .keys()
                .map(|k| format!("topic:{topic}/config:{k}"))
                .collect(),
            Self::BrokerConfig { broker, keys } => keys
                .keys()
                .map(|k| format!("broker:{broker}/config:{k}"))
                .collect(),
            Self::Topic { topic, .. } => vec![format!("topic:{topic}")],
            Self::Replicas { partitions } => partitions
                .iter()
                .map(|p| format!("topic:{}/replicas:{}", p.topic, p.partition))
                .collect(),
            Self::GroupOffsets { group, offsets } => offsets
                .iter()
                .map(|p| format!("group:{group}/offset:{}-{}", p.topic, p.partition))
                .collect(),
            Self::Acls { bindings } => bindings
                .iter()
                .map(|b| format!("acl:{}", acl_label(&b.binding)))
                .collect(),
            Self::Quota { entity, key, .. } => vec![format!(
                "quota:{}/{key}",
                crate::model::quota::quota_entity_label(entity)
            )],
            Self::Scram {
                user, mechanism, ..
            } => vec![format!("scram:{user}/{mechanism}")],
            Self::SubjectCompat { subject, .. } => vec![format!("subject:{subject}/compat")],
            Self::SubjectVersions { subject, .. } => vec![format!("subject:{subject}/versions")],
            Self::Connector { name, .. } => vec![format!("connector:{name}")],
        }
    }
}

fn yes_no(value: bool) -> String {
    if value { "yes" } else { "no" }.to_string()
}

fn present_absent(value: bool) -> String {
    if value { "present" } else { "absent" }.to_string()
}

/// "ALLOW READ on TOPIC 'orders' (LITERAL) for User:alice from *".
pub fn acl_label(b: &AclBinding) -> String {
    format!(
        "{} {} on {} '{}' ({}) for {} from {}",
        b.permission_type,
        b.operation,
        b.resource_type,
        b.resource_name,
        b.pattern_type,
        b.principal,
        b.host
    )
}

/// The journal line of an operation starting: what it is and the state
/// before it ran.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Begin {
    /// `cli` or `gui`.
    pub source: String,
    /// OS user who ran Ferrika.
    #[serde(default)]
    pub user: String,
    pub scope: Scope,
    /// The action as the audit log names it, e.g. `topics config`.
    pub action: String,
    /// What it was done to, e.g. `orders: retention.ms`.
    pub target: String,
    pub reversibility: Reversibility,
    /// What undo does not restore, or why it cannot.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub limits: Vec<String>,
    #[serde(default)]
    pub before: Vec<State>,
    /// The state the change is meant to leave; empty when only known after
    /// it ran (the `end` line then has it).
    #[serde(default)]
    pub after: Vec<State>,
    /// Facts for the reader that undo does not use (e.g. how to recreate a
    /// deleted topic's structure).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub info: Vec<String>,
    /// The operation this one undoes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub undoes: Option<String>,
}

/// The journal line of an operation finishing.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct End {
    pub ok: bool,
    pub outcome: String,
    /// The state after the change, when it was read afterwards (replaces
    /// `Begin::after`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<Vec<State>>,
}

/// What a journal line says.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "ev", rename_all = "snake_case")]
pub enum EventBody {
    Begin(Box<Begin>),
    End(End),
    /// A remark added later (e.g. why the change could not be recorded).
    Note {
        text: String,
    },
}

/// One line of the journal.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HistoryEvent {
    pub v: u32,
    pub id: String,
    pub time_ms: i64,
    #[serde(flatten)]
    pub body: EventBody,
}

/// One operation, folded from its journal lines.
#[derive(Debug, Clone, PartialEq)]
pub struct Operation {
    pub id: String,
    pub time_ms: i64,
    pub begin: Begin,
    /// `None`: no result recorded (the process ended first).
    pub end: Option<End>,
    /// When the `end` line was written.
    pub ended_ms: Option<i64>,
    pub notes: Vec<String>,
    /// The operation that undid this one.
    pub undone_by: Option<String>,
}

impl Operation {
    /// The state the change left, as recorded.
    pub fn after(&self) -> &[State] {
        self.end
            .as_ref()
            .and_then(|e| e.after.as_deref())
            .unwrap_or(&self.begin.after)
    }

    /// Whether the change ran and succeeded (`None`: result unknown).
    pub fn ok(&self) -> Option<bool> {
        self.end.as_ref().map(|e| e.ok)
    }

    /// SHA-256 of the before- and after-state, as the audit log's
    /// `op_digest` records it; a journal edited afterwards no longer matches.
    pub fn digest(&self) -> String {
        state_digest(&self.begin.before, self.after())
    }

    /// Every resource key the operation touched.
    pub fn resource_keys(&self) -> Vec<String> {
        let mut keys: Vec<String> = self
            .begin
            .before
            .iter()
            .chain(self.after())
            .flat_map(State::resource_keys)
            .collect();
        keys.sort();
        keys.dedup();
        keys
    }

    /// The UNDO column: how far undo can reverse it, or who undid it.
    pub fn undo_label(&self) -> String {
        if let Some(by) = &self.undone_by {
            return format!("undone by {by}");
        }
        if self.ok() == Some(false) {
            return "-".to_string();
        }
        let label = self.begin.reversibility.label().to_string();
        match &self.begin.undoes {
            Some(original) => format!("{label} (undo of {original})"),
            None => label,
        }
    }

    /// Every item the operation recorded, with its value before and after.
    pub fn state_rows(&self) -> Vec<StateRow> {
        let mut rows = Vec::new();
        for (ix, before) in self.begin.before.iter().enumerate() {
            let after = self.after().get(ix).map(State::entries);
            let before_entries = before.entries();
            let mut items: Vec<&String> = before_entries
                .keys()
                .chain(after.iter().flat_map(|a| a.keys()))
                .collect();
            items.sort();
            items.dedup();
            for item in items {
                let value = |map: &BTreeMap<String, String>| {
                    map.get(item).cloned().unwrap_or_else(|| NONE.to_string())
                };
                rows.push(StateRow {
                    subject: before.subject(),
                    item: item.clone(),
                    before: value(&before_entries),
                    after: after
                        .as_ref()
                        .map(value)
                        .unwrap_or_else(|| "(not recorded)".to_string()),
                });
            }
        }
        rows
    }

    /// The RESULT column: `ok`, `failed` or `result unknown`.
    pub fn result_label(&self) -> &'static str {
        match self.ok() {
            Some(true) => "ok",
            Some(false) => "failed",
            None => "result unknown",
        }
    }
}

/// One recorded item of an operation, for display.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StateRow {
    /// The resource, e.g. "topic orders".
    pub subject: String,
    pub item: String,
    pub before: String,
    pub after: String,
}

/// See [`Operation::digest`].
pub fn state_digest(before: &[State], after: &[State]) -> String {
    let text = serde_json::json!({ "before": before, "after": after }).to_string();
    let digest = ring::digest::digest(&ring::digest::SHA256, text.as_bytes());
    crate::model::format::to_hex(digest.as_ref())
}

/// Folds journal lines into operations, oldest first. Lines of an id
/// without a `begin` are ignored; a repeated `begin` keeps the first.
pub fn fold(events: impl IntoIterator<Item = HistoryEvent>) -> Vec<Operation> {
    let mut order: Vec<String> = Vec::new();
    let mut ops: BTreeMap<String, Operation> = BTreeMap::new();
    let mut undone: Vec<(String, String)> = Vec::new();
    for event in events {
        match event.body {
            EventBody::Begin(begin) => {
                if ops.contains_key(&event.id) {
                    continue;
                }
                if let Some(target) = &begin.undoes {
                    undone.push((target.clone(), event.id.clone()));
                }
                order.push(event.id.clone());
                ops.insert(
                    event.id.clone(),
                    Operation {
                        id: event.id,
                        time_ms: event.time_ms,
                        begin: *begin,
                        end: None,
                        ended_ms: None,
                        notes: Vec::new(),
                        undone_by: None,
                    },
                );
            }
            EventBody::End(end) => {
                if let Some(op) = ops.get_mut(&event.id) {
                    if op.end.is_none() {
                        op.end = Some(end);
                        op.ended_ms = Some(event.time_ms);
                    }
                }
            }
            EventBody::Note { text } => {
                if let Some(op) = ops.get_mut(&event.id) {
                    op.notes.push(text);
                }
            }
        }
    }
    // An undo counts once it succeeded; a failed one leaves the original
    // undoable.
    for (target, by) in undone {
        let succeeded = ops.get(&by).and_then(Operation::ok) == Some(true);
        if let (true, Some(op)) = (succeeded, ops.get_mut(&target)) {
            op.undone_by = Some(by);
        }
    }
    order.into_iter().filter_map(|id| ops.remove(&id)).collect()
}

/// Crockford base32, which sorts like the numbers it encodes.
const BASE32: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";

/// A new operation id: 12 base32 characters, the time in milliseconds
/// (48 bits) then 12 random bits, so ids sort by time.
pub fn new_op_id(time_ms: i64) -> String {
    use ring::rand::SecureRandom;
    let mut random = [0u8; 2];
    // Without randomness two ids of the same millisecond could collide;
    // the time part alone still sorts.
    let _ = ring::rand::SystemRandom::new().fill(&mut random);
    let bits = ((time_ms.max(0) as u64 & 0xFFFF_FFFF_FFFF) << 12)
        | (u64::from(u16::from_be_bytes(random)) & 0xFFF);
    (0..12)
        .rev()
        .map(|i| BASE32[((bits >> (i * 5)) & 31) as usize] as char)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(topic: &str, key: &str, own: Option<&str>) -> State {
        State::TopicConfig {
            topic: topic.into(),
            keys: BTreeMap::from([(
                key.to_string(),
                ConfigValue {
                    own: own.map(str::to_string),
                    sensitive: false,
                },
            )]),
        }
    }

    fn begin(undoes: Option<&str>) -> Begin {
        Begin {
            source: "cli".into(),
            user: "tester".into(),
            scope: Scope::default(),
            action: "topics config".into(),
            target: "orders: retention.ms".into(),
            reversibility: Reversibility::Reversible,
            limits: vec![],
            before: vec![config("orders", "retention.ms", Some("1"))],
            after: vec![config("orders", "retention.ms", Some("2"))],
            info: vec![],
            undoes: undoes.map(str::to_string),
        }
    }

    fn event(id: &str, time_ms: i64, body: EventBody) -> HistoryEvent {
        HistoryEvent {
            v: FORMAT_VERSION,
            id: id.into(),
            time_ms,
            body,
        }
    }

    fn end(ok: bool) -> EventBody {
        EventBody::End(End {
            ok,
            outcome: "done".into(),
            after: None,
        })
    }

    #[test]
    fn journal_lines_round_trip_and_keep_the_documented_shape() {
        let line = event("A", 1, EventBody::Begin(Box::new(begin(None))));
        let json = serde_json::to_value(&line).unwrap();
        assert_eq!(json["ev"], "begin");
        assert_eq!(json["reversibility"], "reversible");
        assert_eq!(json["before"][0]["kind"], "topic_config");
        let back: HistoryEvent = serde_json::from_value(json).unwrap();
        assert_eq!(back, line);
        let end_line: HistoryEvent = serde_json::from_str(
            r#"{"v":1,"id":"A","time_ms":2,"ev":"end","ok":true,"outcome":"x"}"#,
        )
        .unwrap();
        assert_eq!(end_line.body, end(true).with_outcome("x"));
    }

    impl EventBody {
        fn with_outcome(self, outcome: &str) -> Self {
            match self {
                EventBody::End(mut e) => {
                    e.outcome = outcome.into();
                    EventBody::End(e)
                }
                other => other,
            }
        }
    }

    #[test]
    fn fold_joins_begin_and_end_and_marks_successful_undos() {
        let ops = fold(vec![
            event("A", 1, EventBody::Begin(Box::new(begin(None)))),
            event("B", 2, EventBody::Begin(Box::new(begin(None)))),
            event("A", 3, end(true)),
            // A repeated begin is ignored.
            event("A", 4, EventBody::Begin(Box::new(begin(Some("Z"))))),
            event("U1", 5, EventBody::Begin(Box::new(begin(Some("A"))))),
            event("U1", 6, end(false)),
            event("U2", 7, EventBody::Begin(Box::new(begin(Some("B"))))),
            event("U2", 8, end(true)),
            // No begin: ignored.
            event("X", 9, end(true)),
        ]);
        let ids: Vec<&str> = ops.iter().map(|o| o.id.as_str()).collect();
        assert_eq!(ids, vec!["A", "B", "U1", "U2"]);
        assert_eq!(ops[0].ok(), Some(true));
        // B has no end: result unknown.
        assert_eq!(ops[1].ok(), None);
        assert_eq!(ops[1].result_label(), "result unknown");
        // U1 failed, so A is still undoable; U2 undid B.
        assert_eq!(ops[0].undone_by, None);
        assert_eq!(ops[1].undone_by.as_deref(), Some("U2"));
    }

    #[test]
    fn digest_covers_before_and_the_final_after() {
        let mut op = fold(vec![event("A", 1, EventBody::Begin(Box::new(begin(None))))]).remove(0);
        let recorded = op.digest();
        op.begin.before = vec![config("orders", "retention.ms", Some("9"))];
        assert_ne!(op.digest(), recorded);
        op.begin.before = vec![config("orders", "retention.ms", Some("1"))];
        assert_eq!(op.digest(), recorded);
        op.end = Some(End {
            ok: true,
            outcome: String::new(),
            after: Some(vec![config("orders", "retention.ms", Some("3"))]),
        });
        assert_ne!(op.digest(), recorded);
    }

    #[test]
    fn states_compare_by_what_undo_can_restore() {
        let hidden = |own: Option<&str>| State::TopicConfig {
            topic: "t".into(),
            keys: BTreeMap::from([(
                "ssl.key.password".to_string(),
                ConfigValue {
                    own: own.map(str::to_string),
                    sensitive: true,
                },
            )]),
        };
        assert!(hidden(Some("a")).same_as(&hidden(None)));
        assert!(!config("t", "k", Some("1")).same_as(&config("t", "k", None)));
        let versions = |pairs: &[(u32, u32)]| State::SubjectVersions {
            subject: "s".into(),
            versions: pairs
                .iter()
                .map(|&(version, id)| SchemaVersion {
                    version,
                    id,
                    schema_type: String::new(),
                    schema: String::new(),
                    references: vec![],
                })
                .collect(),
        };
        // Re-registered: new version numbers, same ids.
        assert!(versions(&[(1, 5), (2, 9)]).same_as(&versions(&[(3, 5), (4, 9)])));
        assert!(!versions(&[(1, 5)]).same_as(&versions(&[(1, 5), (2, 9)])));
    }

    #[test]
    fn scopes_match_by_cluster_id_then_bootstrap_set() {
        let scope = |id: &str, bootstrap: &str| Scope {
            profile: "p".into(),
            bootstrap: bootstrap.into(),
            cluster_id: id.into(),
            ..Scope::default()
        };
        assert!(scope("X", "a:9092").same_cluster(&scope("X", "b:9092")));
        assert!(!scope("X", "a:9092").same_cluster(&scope("Y", "a:9092")));
        assert!(scope("", "a:9092, B:9092").same_cluster(&scope("Y", "b:9092,a:9092")));
        assert!(!scope("", "a:9092").same_cluster(&scope("", "a:9092,b:9092")));
        let registry = |url: &str| Scope {
            registry: url.into(),
            ..Scope::default()
        };
        assert!(registry("http://sr:8081").same_cluster(&registry("http://sr:8081")));
        assert!(!registry("http://sr:8081").same_cluster(&registry("http://other:8081")));
    }

    #[test]
    fn op_ids_sort_by_time() {
        let a = new_op_id(1_762_070_400_000);
        let b = new_op_id(1_762_070_400_001);
        assert_eq!(a.len(), 12);
        assert!(a < b, "{a} < {b}");
        assert!(a.chars().all(|c| BASE32.contains(&(c as u8))));
    }
}
