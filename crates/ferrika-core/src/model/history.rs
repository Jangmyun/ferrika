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
pub use crate::model::connect::ConnectorOffset;
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
    /// The RabbitMQ management API's `host:port` (never credentials), for
    /// RabbitMQ operations; `bootstrap` is empty for them.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub rabbitmq: String,
}

impl Scope {
    /// Whether an operation recorded with `self` belongs to the cluster
    /// `current` describes: same cluster id when both have one, else the
    /// same set of bootstrap servers (order and spacing ignored). Registry
    /// and Connect operations compare those URLs instead.
    pub fn same_cluster(&self, current: &Scope) -> bool {
        if !self.rabbitmq.is_empty() {
            return same_address(&self.rabbitmq, &current.rabbitmq);
        }
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

    /// Whether undo may change `current` for an operation recorded with
    /// `self`, or why not. Stricter than [`Scope::same_cluster`]: an
    /// operation recorded with a cluster id is only undone on a cluster
    /// whose id was read and is the same (matching bootstrap servers are not
    /// enough, they may point at another cluster now).
    pub fn check_undo_target(&self, current: &Scope) -> Result<(), String> {
        let refuse = |why: String| {
            Err(format!(
                "this operation was recorded on {}, {why}; connect to that one to undo it",
                self.describe()
            ))
        };
        if !self.rabbitmq.is_empty() {
            if !same_address(&self.rabbitmq, &current.rabbitmq) {
                return refuse(format!(
                    "but the RabbitMQ in use is {}",
                    or_none(&current.rabbitmq)
                ));
            }
            return Ok(());
        }
        if !self.registry.is_empty() {
            if self.registry != current.registry {
                return refuse(format!(
                    "but the Schema Registry in use is {}",
                    or_none(&current.registry)
                ));
            }
            return Ok(());
        }
        if !self.connect.is_empty() {
            if self.connect != current.connect {
                return refuse(format!(
                    "but the Kafka Connect in use is {}",
                    or_none(&current.connect)
                ));
            }
            return Ok(());
        }
        if !self.cluster_id.is_empty() {
            if current.cluster_id.is_empty() {
                return refuse(format!(
                    "but the id of the cluster at {} could not be read to confirm it",
                    current.bootstrap
                ));
            }
            if self.cluster_id != current.cluster_id {
                return refuse(format!(
                    "but the brokers in use ({}) belong to cluster {}",
                    current.bootstrap, current.cluster_id
                ));
            }
            return Ok(());
        }
        if bootstrap_set(&self.bootstrap) != bootstrap_set(&current.bootstrap) {
            return refuse(format!("but the brokers in use are {}", current.bootstrap));
        }
        Ok(())
    }

    /// "cluster 4L62… (profile prod-a)" for messages.
    pub fn describe(&self) -> String {
        let place = if !self.rabbitmq.is_empty() {
            format!("RabbitMQ {}", self.rabbitmq)
        } else if !self.registry.is_empty() {
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

/// Two `host:port` addresses, case ignored (an empty one matches nothing).
fn same_address(a: &str, b: &str) -> bool {
    !a.trim().is_empty() && a.trim().eq_ignore_ascii_case(b.trim())
}

fn or_none(text: &str) -> &str {
    if text.is_empty() {
        "none"
    } else {
        text
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

/// How a RabbitMQ queue was declared: what re-declaring it needs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RabbitQueueDeclaration {
    /// `classic`, `quorum` or `stream`.
    #[serde(rename = "type")]
    pub kind: String,
    pub durable: bool,
    #[serde(default)]
    pub auto_delete: bool,
    /// The `x-*` arguments it was declared with.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub arguments: BTreeMap<String, serde_json::Value>,
}

/// How a RabbitMQ exchange was declared.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RabbitExchangeDeclaration {
    /// `direct`, `fanout`, `topic`, `headers` or a plugin's type.
    #[serde(rename = "type")]
    pub kind: String,
    pub durable: bool,
    #[serde(default)]
    pub auto_delete: bool,
    #[serde(default)]
    pub internal: bool,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub arguments: BTreeMap<String, serde_json::Value>,
}

/// One RabbitMQ binding, by what the broker tells bindings apart by.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RabbitBindingKey {
    /// The exchange messages come from.
    pub source: String,
    pub destination: String,
    /// `queue` or `exchange`.
    pub destination_type: String,
    #[serde(default)]
    pub routing_key: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub arguments: BTreeMap<String, serde_json::Value>,
}

impl RabbitBindingKey {
    /// "exchange orders -> queue billing, routing key 'order.*'".
    pub fn label(&self) -> String {
        let mut text = format!(
            "exchange {} -> {} {}, routing key '{}'",
            self.source, self.destination_type, self.destination, self.routing_key
        );
        if !self.arguments.is_empty() {
            text.push_str(&format!(
                ", arguments {}",
                serde_json::to_string(&self.arguments).unwrap_or_default()
            ));
        }
        text
    }
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
        /// Whether the connector is stopped (Kafka Connect 3.5+, KIP-875):
        /// no tasks run. Left out of the journal when false, so lines
        /// recorded before it existed read the same.
        #[serde(default, skip_serializing_if = "is_false")]
        stopped: bool,
        /// Config keys whose values are kept out of the journal (secrets).
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        secret_keys: Vec<String>,
    },
    /// Offsets of a Kafka Connect connector (KIP-875), limited to the
    /// partitions an operation changes; `offset: None` = no offset.
    ConnectorOffsets {
        name: String,
        offsets: Vec<ConnectorOffset>,
    },
    /// A RabbitMQ queue's declaration; `None` when there is no such queue.
    /// Ferrika 0.2 does not know the `rabbit_*` kinds and skips the journal
    /// lines holding them, so every RabbitMQ operation records at least one.
    RabbitQueue {
        vhost: String,
        name: String,
        declaration: Option<RabbitQueueDeclaration>,
    },
    /// How many messages a RabbitMQ queue holds (for a purge); `None` when
    /// the broker did not say.
    RabbitQueueMessages {
        vhost: String,
        name: String,
        messages: Option<u64>,
    },
    /// A RabbitMQ exchange's declaration; `None` when there is no such
    /// exchange.
    RabbitExchange {
        vhost: String,
        name: String,
        declaration: Option<RabbitExchangeDeclaration>,
    },
    /// Whether one RabbitMQ binding exists.
    RabbitBinding {
        vhost: String,
        binding: RabbitBindingKey,
        present: bool,
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
            Self::ConnectorOffsets { name, .. } => format!("offsets of connector {name}"),
            Self::RabbitQueue { vhost, name, .. }
            | Self::RabbitQueueMessages { vhost, name, .. } => {
                format!("queue {name} on vhost {vhost}")
            }
            Self::RabbitExchange { vhost, name, .. } => {
                format!("exchange {name} on vhost {vhost}")
            }
            Self::RabbitBinding { vhost, .. } => format!("bindings on vhost {vhost}"),
        }
    }

    /// Whether the state is about a RabbitMQ resource (read and restored
    /// through the management API, not a Kafka client).
    pub fn is_rabbitmq(&self) -> bool {
        match self {
            Self::RabbitQueue { .. }
            | Self::RabbitQueueMessages { .. }
            | Self::RabbitExchange { .. }
            | Self::RabbitBinding { .. } => true,
            Self::TopicConfig { .. }
            | Self::BrokerConfig { .. }
            | Self::Topic { .. }
            | Self::Replicas { .. }
            | Self::GroupOffsets { .. }
            | Self::Acls { .. }
            | Self::Quota { .. }
            | Self::Scram { .. }
            | Self::SubjectCompat { .. }
            | Self::SubjectVersions { .. }
            | Self::Connector { .. }
            | Self::ConnectorOffsets { .. } => false,
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
                stopped,
                secret_keys,
                ..
            } => {
                out.insert("exists".into(), yes_no(config.is_some()));
                if let Some(config) = config {
                    out.insert("paused".into(), yes_no(*paused));
                    // Only when stopped, so states recorded before Ferrika
                    // knew stopped connectors compare as before.
                    if *stopped {
                        out.insert("stopped".into(), yes_no(true));
                    }
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
            Self::ConnectorOffsets { offsets, .. } => {
                for o in offsets {
                    if let Some(offset) = o.offset_text() {
                        out.insert(format!("offset {}", o.partition_text()), offset);
                    }
                }
            }
            Self::RabbitQueue { declaration, .. } => {
                out.insert("exists".into(), yes_no(declaration.is_some()));
                if let Some(d) = declaration {
                    out.insert("type".into(), d.kind.clone());
                    out.insert("durable".into(), yes_no(d.durable));
                    out.insert("auto_delete".into(), yes_no(d.auto_delete));
                    arguments_entries(&mut out, &d.arguments);
                }
            }
            Self::RabbitQueueMessages { messages, .. } => {
                if let Some(messages) = messages {
                    out.insert("messages".into(), messages.to_string());
                }
            }
            Self::RabbitExchange { declaration, .. } => {
                out.insert("exists".into(), yes_no(declaration.is_some()));
                if let Some(d) = declaration {
                    out.insert("type".into(), d.kind.clone());
                    out.insert("durable".into(), yes_no(d.durable));
                    out.insert("auto_delete".into(), yes_no(d.auto_delete));
                    out.insert("internal".into(), yes_no(d.internal));
                    arguments_entries(&mut out, &d.arguments);
                }
            }
            Self::RabbitBinding {
                binding, present, ..
            } => {
                out.insert(binding.label(), present_absent(*present));
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
            Self::ConnectorOffsets { name, offsets } => offsets
                .iter()
                .map(|o| format!("connector:{name}/offset:{}", o.partition_text()))
                .collect(),
            Self::RabbitQueue { vhost, name, .. } => vec![format!("rabbit-queue:{name}@{vhost}")],
            Self::RabbitQueueMessages { vhost, name, .. } => {
                vec![format!("rabbit-queue-messages:{name}@{vhost}")]
            }
            Self::RabbitExchange { vhost, name, .. } => {
                vec![format!("rabbit-exchange:{name}@{vhost}")]
            }
            Self::RabbitBinding { vhost, binding, .. } => {
                vec![format!("rabbit-binding:{}@{vhost}", binding.label())]
            }
        }
    }
}

/// One `argument NAME` entry per declaration argument (text as is, other
/// values as JSON).
fn arguments_entries(
    out: &mut BTreeMap<String, String>,
    arguments: &BTreeMap<String, serde_json::Value>,
) {
    for (key, value) in arguments {
        let text = match value {
            serde_json::Value::String(text) => text.clone(),
            other => other.to_string(),
        };
        out.insert(format!("argument {key}"), text);
    }
}

fn is_false(value: &bool) -> bool {
    !*value
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

    /// The digest the audit log's `op_digest` records for the operation
    /// (version 2, see [`operation_digest`]); a journal edited afterwards no
    /// longer matches.
    pub fn digest(&self) -> String {
        operation_digest(&self.id, &self.begin, self.after(), self.ok())
    }

    /// The version 1 digest older Ferrika versions recorded: SHA-256 of
    /// the before- and after-state only (see [`state_digest`]).
    pub fn digest_v1(&self) -> String {
        state_digest(&self.begin.before, self.after())
    }

    /// Compares the journal entry with the `op_digest` the audit log holds
    /// for it. The audit log's digest says which version to check (a `v2:`
    /// prefix or none), so editing the journal cannot pick the weaker one.
    pub fn verify_digest(&self, audited: Option<&str>) -> DigestStatus {
        match audited {
            None => DigestStatus::Missing,
            Some(digest) if digest.starts_with(DIGEST_V2_PREFIX) => {
                if digest == self.digest() {
                    DigestStatus::Verified
                } else {
                    DigestStatus::Mismatch
                }
            }
            Some(digest) if digest == self.digest_v1() => DigestStatus::Legacy,
            Some(_) => DigestStatus::Mismatch,
        }
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

/// The version 1 digest (Ferrika 0.1): SHA-256 of the before- and
/// after-state, 64 hex digits without a prefix. It does not cover the scope,
/// action, `undoes` link or reversibility; kept to verify older entries.
pub fn state_digest(before: &[State], after: &[State]) -> String {
    sha256_hex(&serde_json::json!({ "before": before, "after": after }).to_string())
}

/// Prefix of version 2 digests. A reader that only knows version 1
/// compares the whole string with its 64-digit digest, so an entry
/// recorded with version 2 never verifies there by mistake: it fails
/// closed (undo refused as edited) instead.
pub const DIGEST_V2_PREFIX: &str = "v2:";

/// The version 2 digest of operation `id`: `v2:` and SHA-256 of every field
/// of its `begin` line (source, user, scope, action, target,
/// reversibility, limits, info, before-state, `undoes`), the final
/// after-state and whether it succeeded. Editing anything that changes what
/// `history` shows or `undo` does (where it runs, what it restores, which
/// operation it marks undone, whether it is undoable) breaks it.
pub fn operation_digest(id: &str, begin: &Begin, after: &[State], ok: Option<bool>) -> String {
    let text = serde_json::json!({
        "digest": 2,
        "id": id,
        "source": begin.source,
        "user": begin.user,
        "scope": begin.scope,
        "action": begin.action,
        "target": begin.target,
        "reversibility": begin.reversibility,
        "limits": begin.limits,
        "info": begin.info,
        "before": begin.before,
        "after": after,
        "undoes": begin.undoes,
        "ok": ok,
    })
    .to_string();
    format!("{DIGEST_V2_PREFIX}{}", sha256_hex(&text))
}

fn sha256_hex(text: &str) -> String {
    let digest = ring::digest::digest(&ring::digest::SHA256, text.as_bytes());
    crate::model::format::to_hex(digest.as_ref())
}

/// How a journal entry compares with the audit log's `op_digest`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DigestStatus {
    /// A version 2 digest matches: the whole entry is as recorded.
    Verified,
    /// A version 1 digest (Ferrika 0.1) matches: the before- and
    /// after-state are as recorded, but the scope, action, `undoes` link and
    /// reversibility are not covered and may have been edited.
    Legacy,
    /// No audit entry names a digest for the operation.
    Missing,
    /// The entry does not match the audit log: it was edited.
    Mismatch,
}

impl DigestStatus {
    /// How `history`, `undo` and `audit verify` name it.
    pub fn label(self) -> &'static str {
        match self {
            Self::Verified => "verified",
            Self::Legacy => "legacy digest (scope not covered)",
            Self::Missing => "no audit entry",
            Self::Mismatch => "EDITED (does not match the audit log)",
        }
    }
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

    #[test]
    fn connector_stop_and_offsets_states() {
        let running = State::Connector {
            name: "c".into(),
            config: Some(BTreeMap::new()),
            paused: false,
            stopped: false,
            secret_keys: vec![],
        };
        // Not stopped: written as before the field existed.
        let line = serde_json::to_string(&running).unwrap();
        assert!(!line.contains("stopped"), "{line}");
        let old: State =
            serde_json::from_str(r#"{"kind":"connector","name":"c","config":{},"paused":false}"#)
                .unwrap();
        assert_eq!(old, running);
        let mut stopped = running.clone();
        if let State::Connector { stopped: s, .. } = &mut stopped {
            *s = true;
        }
        assert!(!stopped.same_as(&running));
        assert_eq!(
            stopped.entries().get("stopped").map(String::as_str),
            Some("yes")
        );

        let offsets = State::ConnectorOffsets {
            name: "sink".into(),
            offsets: vec![
                ConnectorOffset::sink("orders", 0, Some(5)),
                ConnectorOffset::sink("orders", 1, None),
            ],
        };
        assert_eq!(offsets.subject(), "offsets of connector sink");
        let entries = offsets.entries();
        assert_eq!(entries.len(), 1);
        assert_eq!(
            entries
                .get(r#"offset {"kafka_partition":0,"kafka_topic":"orders"}"#)
                .map(String::as_str),
            Some(r#"{"kafka_offset":5}"#)
        );
        assert_eq!(offsets.resource_keys().len(), 2);
        assert!(!offsets.is_rabbitmq());
        let line = serde_json::to_string(&offsets).unwrap();
        assert!(line.contains(r#""kind":"connector_offsets""#), "{line}");
        assert_eq!(serde_json::from_str::<State>(&line).unwrap(), offsets);
    }

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

    fn finished(begin: Begin) -> Operation {
        fold(vec![
            event("A", 1, EventBody::Begin(Box::new(begin))),
            event("A", 2, end(true)),
        ])
        .remove(0)
    }

    type Edit = Box<dyn Fn(&mut Operation)>;

    #[test]
    fn digest_v2_covers_scope_action_undoes_reversibility_and_result() {
        let mut original = begin(None);
        original.scope = Scope {
            bootstrap: "prod:9092".into(),
            cluster_id: "PROD".into(),
            ..Scope::default()
        };
        let op = finished(original);
        let audited = op.digest();
        assert!(audited.starts_with(DIGEST_V2_PREFIX), "{audited}");
        assert_eq!(op.verify_digest(Some(&audited)), DigestStatus::Verified);
        let edits: Vec<(&str, Edit)> = vec![
            (
                "scope cluster id",
                Box::new(|o| o.begin.scope.cluster_id = "DEV".into()),
            ),
            (
                "scope bootstrap",
                Box::new(|o| o.begin.scope.bootstrap = "dev:9092".into()),
            ),
            ("undoes", Box::new(|o| o.begin.undoes = Some("B".into()))),
            ("action", Box::new(|o| o.begin.action = "acl create".into())),
            (
                "reversibility",
                Box::new(|o| o.begin.reversibility = Reversibility::Partial),
            ),
            ("user", Box::new(|o| o.begin.user = "mallory".into())),
            (
                "result",
                Box::new(|o| {
                    if let Some(end) = o.end.as_mut() {
                        end.ok = false;
                    }
                }),
            ),
            ("id", Box::new(|o| o.id = "B".into())),
            (
                "before",
                Box::new(|o| o.begin.before = vec![config("orders", "retention.ms", Some("9"))]),
            ),
        ];
        for (what, edit) in edits {
            let mut edited = op.clone();
            edit(&mut edited);
            assert_eq!(
                edited.verify_digest(Some(&audited)),
                DigestStatus::Mismatch,
                "{what} edit not detected"
            );
        }
    }

    #[test]
    fn v1_digests_still_verify_but_only_as_legacy() {
        let op = finished(begin(None));
        let v1 = op.digest_v1();
        assert_eq!(v1.len(), 64);
        assert_eq!(op.verify_digest(Some(&v1)), DigestStatus::Legacy);
        assert_eq!(op.verify_digest(None), DigestStatus::Missing);
        assert_eq!(op.verify_digest(Some("x")), DigestStatus::Mismatch);
        // Version 1 does not cover the scope: the edit stays legacy, which
        // is why undo asks again for such entries.
        let mut moved = op.clone();
        moved.begin.scope.bootstrap = "elsewhere:9092".into();
        assert_eq!(moved.verify_digest(Some(&v1)), DigestStatus::Legacy);
        // A version 2 digest is never checked with the version 1 rule, and
        // a version 1 reader comparing whole strings never accepts it.
        let v2 = op.digest();
        assert_ne!(v2, v1);
        assert_eq!(
            moved.verify_digest(Some(&v2)),
            DigestStatus::Mismatch,
            "no downgrade"
        );
    }

    #[test]
    fn undo_target_needs_the_same_cluster_id_when_one_was_recorded() {
        let scope = |id: &str, bootstrap: &str| Scope {
            bootstrap: bootstrap.into(),
            cluster_id: id.into(),
            ..Scope::default()
        };
        assert!(scope("X", "a:9092")
            .check_undo_target(&scope("X", "b:9092"))
            .is_ok());
        let other = scope("X", "a:9092")
            .check_undo_target(&scope("Y", "a:9092"))
            .unwrap_err();
        assert!(
            other.contains("cluster X") && other.contains("cluster Y"),
            "{other}"
        );
        // Same brokers, but the id could not be read: not confirmed.
        let unread = scope("X", "a:9092")
            .check_undo_target(&scope("", "a:9092"))
            .unwrap_err();
        assert!(unread.contains("could not be read"), "{unread}");
        // Without a recorded id the bootstrap set decides.
        assert!(scope("", "a:9092,b:9092")
            .check_undo_target(&scope("Z", "B:9092, a:9092"))
            .is_ok());
        assert!(scope("", "a:9092")
            .check_undo_target(&scope("", "c:9092"))
            .is_err());
        let registry = |url: &str| Scope {
            registry: url.into(),
            ..Scope::default()
        };
        assert!(registry("http://sr:8081")
            .check_undo_target(&registry("http://sr:8081"))
            .is_ok());
        assert!(registry("http://sr:8081")
            .check_undo_target(&Scope::default())
            .is_err());
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

    fn rabbit_queue(declared: bool) -> State {
        State::RabbitQueue {
            vhost: "/".into(),
            name: "orders".into(),
            declaration: declared.then(|| RabbitQueueDeclaration {
                kind: "quorum".into(),
                durable: true,
                auto_delete: false,
                arguments: BTreeMap::from([("x-delivery-limit".to_string(), serde_json::json!(5))]),
            }),
        }
    }

    #[test]
    fn rabbit_states_compare_show_and_name_their_resources() {
        let json = serde_json::to_value(rabbit_queue(true)).unwrap();
        assert_eq!(json["kind"], "rabbit_queue");
        assert_eq!(json["declaration"]["type"], "quorum");
        let back: State = serde_json::from_value(json).unwrap();
        assert_eq!(back, rabbit_queue(true));
        let entries = rabbit_queue(true).entries();
        assert_eq!(entries["exists"], "yes");
        assert_eq!(entries["type"], "quorum");
        assert_eq!(entries["argument x-delivery-limit"], "5");
        assert_eq!(rabbit_queue(false).entries().len(), 1);
        assert!(!rabbit_queue(true).same_as(&rabbit_queue(false)));
        assert_eq!(
            rabbit_queue(true).resource_keys(),
            vec!["rabbit-queue:orders@/".to_string()]
        );
        assert_eq!(rabbit_queue(true).subject(), "queue orders on vhost /");
        assert!(rabbit_queue(true).is_rabbitmq());
        assert!(!config("t", "k", None).is_rabbitmq());

        let binding = |present| State::RabbitBinding {
            vhost: "/".into(),
            binding: RabbitBindingKey {
                source: "shop".into(),
                destination: "orders".into(),
                destination_type: "queue".into(),
                routing_key: "order.*".into(),
                arguments: BTreeMap::new(),
            },
            present,
        };
        let label = "exchange shop -> queue orders, routing key 'order.*'";
        assert_eq!(binding(true).entries()[label], "present");
        assert_eq!(binding(false).entries()[label], "absent");
        assert_eq!(
            binding(true).resource_keys(),
            vec![format!("rabbit-binding:{label}@/")]
        );
        // A purge's message count is its own resource: it never overlaps
        // the queue's declaration.
        let messages = State::RabbitQueueMessages {
            vhost: "/".into(),
            name: "orders".into(),
            messages: Some(3),
        };
        assert_eq!(messages.entries()["messages"], "3");
        assert_ne!(messages.resource_keys(), rabbit_queue(true).resource_keys());
    }

    #[test]
    fn rabbit_scopes_match_by_management_address_and_are_digested() {
        let rabbit = |address: &str| Scope {
            profile: "dev".into(),
            rabbitmq: address.into(),
            ..Scope::default()
        };
        assert!(rabbit("rabbit:15672").same_cluster(&rabbit("RABBIT:15672")));
        assert!(!rabbit("rabbit:15672").same_cluster(&rabbit("other:15672")));
        // A Kafka cluster without brokers named is not that RabbitMQ.
        assert!(!rabbit("rabbit:15672").same_cluster(&Scope::default()));
        let refused = rabbit("rabbit:15672")
            .check_undo_target(&Scope {
                bootstrap: "localhost:9092".into(),
                ..Scope::default()
            })
            .unwrap_err();
        assert!(refused.contains("RabbitMQ rabbit:15672"), "{refused}");
        assert!(refused.contains("in use is none"), "{refused}");
        assert_eq!(
            rabbit("rabbit:15672").describe(),
            "RabbitMQ rabbit:15672 (profile dev)"
        );
        // Kafka scopes serialise as before (no empty `rabbitmq` key).
        let kafka = serde_json::to_value(Scope::default()).unwrap();
        assert!(kafka.get("rabbitmq").is_none());

        let mut recorded = begin(None);
        recorded.scope = rabbit("rabbit:15672");
        recorded.before = vec![rabbit_queue(true)];
        recorded.after = vec![rabbit_queue(false)];
        let op = finished(recorded);
        let audited = op.digest();
        let mut moved = op.clone();
        moved.begin.scope.rabbitmq = "other:15672".into();
        assert_eq!(moved.verify_digest(Some(&audited)), DigestStatus::Mismatch);
        let mut edited = op.clone();
        edited.begin.before = vec![rabbit_queue(false)];
        assert_eq!(edited.verify_digest(Some(&audited)), DigestStatus::Mismatch);
        assert_eq!(op.verify_digest(Some(&audited)), DigestStatus::Verified);
    }
}
