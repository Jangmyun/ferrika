//! The changes that put a recorded before-state back: pure functions from
//! states to [`Step`]s, run by [`super::apply`].

use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;

use ferrika_core::model::acl::AclBinding;
use ferrika_core::model::history::{
    acl_label, ConnectorOffset, Operation, PartitionReplicas, RabbitBindingKey,
    RabbitExchangeDeclaration, RabbitQueueDeclaration, Reversibility, SchemaVersion, State,
};
use ferrika_core::model::policy::PolicyAction;

/// One change undo makes.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "step", rename_all = "snake_case")]
pub enum Step {
    TopicConfig {
        topic: String,
        set: Vec<(String, String)>,
        delete: Vec<String>,
    },
    BrokerConfig {
        broker: i32,
        set: Vec<(String, String)>,
        delete: Vec<String>,
    },
    DeleteTopic {
        topic: String,
    },
    Reassign {
        partitions: Vec<PartitionReplicas>,
    },
    CommitOffsets {
        group: String,
        /// `(topic, partition, offset)`.
        offsets: Vec<(String, i32, i64)>,
    },
    DeleteOffsets {
        group: String,
        topic: String,
        partitions: Vec<i32>,
    },
    CreateAcls {
        bindings: Vec<AclBinding>,
    },
    DeleteAcls {
        bindings: Vec<AclBinding>,
    },
    SetQuota {
        entity: Vec<(String, Option<String>)>,
        key: String,
        value: Option<f64>,
    },
    DeleteScram {
        user: String,
        mechanism: String,
    },
    SetCompatibility {
        subject: String,
        level: String,
    },
    DeleteCompatibility {
        subject: String,
    },
    DeleteSchemaVersions {
        subject: String,
        versions: Vec<u32>,
    },
    RegisterSchemas {
        subject: String,
        versions: Vec<SchemaVersion>,
    },
    PutConnector {
        name: String,
        config: BTreeMap<String, String>,
        /// Keys whose values come from the credential store.
        secret_keys: Vec<String>,
    },
    DeleteConnector {
        name: String,
    },
    PauseConnector {
        name: String,
    },
    ResumeConnector {
        name: String,
    },
    /// Stops a connector (Kafka Connect 3.5+).
    StopConnector {
        name: String,
    },
    /// Sets connector offsets back (Kafka Connect 3.6+, the connector
    /// stopped); an entry without an offset resets its partition.
    AlterConnectorOffsets {
        name: String,
        offsets: Vec<ConnectorOffset>,
    },
    /// Declares a RabbitMQ queue again (its messages are not restored).
    DeclareQueue {
        vhost: String,
        name: String,
        declaration: RabbitQueueDeclaration,
    },
    /// Deletes a RabbitMQ queue, only while it is empty and unused.
    DeleteQueue {
        vhost: String,
        name: String,
    },
    DeclareExchange {
        vhost: String,
        name: String,
        declaration: RabbitExchangeDeclaration,
    },
    /// Deletes a RabbitMQ exchange, only while no binding has it as source.
    DeleteExchange {
        vhost: String,
        name: String,
    },
    Bind {
        vhost: String,
        binding: RabbitBindingKey,
    },
    Unbind {
        vhost: String,
        binding: RabbitBindingKey,
    },
}

impl Step {
    /// The policy category the step falls under: undoing a topic creation
    /// is a topic deletion, and is refused where deleting topics is.
    pub fn policy_action(&self) -> PolicyAction {
        match self {
            Self::TopicConfig { .. } | Self::BrokerConfig { .. } => PolicyAction::ConfigChange,
            Self::DeleteTopic { .. } => PolicyAction::TopicsDelete,
            Self::Reassign { .. } => PolicyAction::TopicsAlter,
            Self::CommitOffsets { .. } | Self::DeleteOffsets { .. } => PolicyAction::OffsetsReset,
            Self::CreateAcls { .. } | Self::DeleteAcls { .. } => PolicyAction::AclChange,
            Self::SetQuota { .. } => PolicyAction::QuotaChange,
            Self::DeleteScram { .. } => PolicyAction::UsersChange,
            Self::SetCompatibility { .. }
            | Self::DeleteCompatibility { .. }
            | Self::DeleteSchemaVersions { .. }
            | Self::RegisterSchemas { .. } => PolicyAction::SchemaChange,
            Self::PutConnector { .. }
            | Self::DeleteConnector { .. }
            | Self::PauseConnector { .. }
            | Self::ResumeConnector { .. }
            | Self::StopConnector { .. }
            | Self::AlterConnectorOffsets { .. } => PolicyAction::ConnectChange,
            Self::DeclareQueue { .. } => PolicyAction::QueuesCreate,
            Self::DeleteQueue { .. } => PolicyAction::QueuesDelete,
            Self::DeclareExchange { .. } => PolicyAction::ExchangesCreate,
            Self::DeleteExchange { .. } => PolicyAction::ExchangesDelete,
            Self::Bind { .. } | Self::Unbind { .. } => PolicyAction::BindingsChange,
        }
    }

    /// Whether the step changes a RabbitMQ broker (run through its
    /// management API, see [`super::rabbit`]).
    pub fn is_rabbitmq(&self) -> bool {
        self.rabbit_phase().is_some()
    }

    /// The order RabbitMQ steps run in, whatever the order of the recorded
    /// states: bindings are removed first, then queues and exchanges
    /// deleted, then declared, then bindings added (a binding needs both
    /// ends). `None` for other steps.
    fn rabbit_phase(&self) -> Option<u8> {
        match self {
            Self::Unbind { .. } => Some(0),
            Self::DeleteQueue { .. } | Self::DeleteExchange { .. } => Some(1),
            Self::DeclareExchange { .. } | Self::DeclareQueue { .. } => Some(2),
            Self::Bind { .. } => Some(3),
            Self::TopicConfig { .. }
            | Self::BrokerConfig { .. }
            | Self::DeleteTopic { .. }
            | Self::Reassign { .. }
            | Self::CommitOffsets { .. }
            | Self::DeleteOffsets { .. }
            | Self::CreateAcls { .. }
            | Self::DeleteAcls { .. }
            | Self::SetQuota { .. }
            | Self::DeleteScram { .. }
            | Self::SetCompatibility { .. }
            | Self::DeleteCompatibility { .. }
            | Self::DeleteSchemaVersions { .. }
            | Self::RegisterSchemas { .. }
            | Self::PutConnector { .. }
            | Self::DeleteConnector { .. }
            | Self::PauseConnector { .. }
            | Self::ResumeConnector { .. }
            | Self::StopConnector { .. }
            | Self::AlterConnectorOffsets { .. } => None,
        }
    }

    /// One line saying what the step does (keys and names, no config values).
    pub fn describe(&self) -> String {
        let keys = |set: &[(String, String)], delete: &[String]| {
            set.iter()
                .map(|(k, _)| format!("{k} (set)"))
                .chain(delete.iter().map(|k| format!("{k} (reset)")))
                .collect::<Vec<_>>()
                .join(", ")
        };
        match self {
            Self::TopicConfig { topic, set, delete } => {
                format!("restore configs of topic {topic}: {}", keys(set, delete))
            }
            Self::BrokerConfig {
                broker,
                set,
                delete,
            } => {
                format!("restore configs of broker {broker}: {}", keys(set, delete))
            }
            Self::DeleteTopic { topic } => format!("delete topic {topic}"),
            Self::Reassign { partitions } => format!(
                "move {} back to their previous replicas",
                count(partitions.len(), "partition")
            ),
            Self::CommitOffsets { group, offsets } => format!(
                "commit the previous offsets of group {group} on {}",
                count(offsets.len(), "partition")
            ),
            Self::DeleteOffsets {
                group,
                topic,
                partitions,
            } => format!(
                "delete the offsets of group {group} on {topic} ({})",
                count(partitions.len(), "partition")
            ),
            Self::CreateAcls { bindings } => format!(
                "create {}",
                bindings
                    .iter()
                    .map(acl_label)
                    .collect::<Vec<_>>()
                    .join("; ")
            ),
            Self::DeleteAcls { bindings } => format!(
                "delete {}",
                bindings
                    .iter()
                    .map(acl_label)
                    .collect::<Vec<_>>()
                    .join("; ")
            ),
            Self::SetQuota { entity, key, value } => {
                let entity = ferrika_core::model::quota::quota_entity_label(entity);
                match value {
                    Some(v) => format!("set quota {key}={v} for {entity}"),
                    None => format!("remove quota {key} for {entity}"),
                }
            }
            Self::DeleteScram { user, mechanism } => {
                format!("delete the {mechanism} credential of user {user}")
            }
            Self::SetCompatibility { subject, level } => {
                format!("set compatibility of {subject} to {level}")
            }
            Self::DeleteCompatibility { subject } => {
                format!("remove the compatibility level of {subject} (inherit the default)")
            }
            Self::DeleteSchemaVersions { subject, versions } => format!(
                "soft-delete version(s) {} of {subject}",
                versions
                    .iter()
                    .map(u32::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            Self::RegisterSchemas { subject, versions } => format!(
                "register {} again under {subject}",
                count(versions.len(), "schema")
            ),
            Self::PutConnector { name, .. } => format!("restore the config of connector {name}"),
            Self::DeleteConnector { name } => format!("delete connector {name}"),
            Self::PauseConnector { name } => format!("pause connector {name}"),
            Self::ResumeConnector { name } => format!("resume connector {name}"),
            Self::StopConnector { name } => format!("stop connector {name}"),
            Self::AlterConnectorOffsets { name, offsets } => format!(
                "set the previous offsets of connector {name} on {} (the connector must be stopped)",
                count(offsets.len(), "partition")
            ),
            Self::DeclareQueue {
                vhost,
                name,
                declaration,
            } => format!(
                "declare {} queue {name} on vhost {vhost} again (empty)",
                declaration.kind
            ),
            Self::DeleteQueue { vhost, name } => {
                format!("delete queue {name} on vhost {vhost} (only while empty and unused)")
            }
            Self::DeclareExchange {
                vhost,
                name,
                declaration,
            } => format!(
                "declare {} exchange {name} on vhost {vhost} again",
                declaration.kind
            ),
            Self::DeleteExchange { vhost, name } => {
                format!("delete exchange {name} on vhost {vhost}")
            }
            Self::Bind { vhost, binding } => {
                format!("bind {} on vhost {vhost}", binding.label())
            }
            Self::Unbind { vhost, binding } => {
                format!("unbind {} on vhost {vhost}", binding.label())
            }
        }
    }
}

/// Whether a connector runs, is paused or is stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RunState {
    Running,
    Paused,
    Stopped,
}

impl RunState {
    fn of(paused: bool, stopped: bool) -> Self {
        if stopped {
            Self::Stopped
        } else if paused {
            Self::Paused
        } else {
            Self::Running
        }
    }
}

fn count(n: usize, what: &str) -> String {
    if n == 1 {
        format!("1 {what}")
    } else {
        format!("{n} {what}s")
    }
}

/// Why an operation cannot be undone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotReversible(pub String);

impl std::fmt::Display for NotReversible {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// The steps that undo `op`, starting from `current` (the state read now,
/// one entry per before-state; the recorded after-state for a dry run
/// without reading).
pub fn plan(op: &Operation, current: &[State]) -> Result<Vec<Step>, NotReversible> {
    if !op.begin.reversibility.undoable() {
        let reason =
            op.begin
                .limits
                .first()
                .cloned()
                .unwrap_or_else(|| match op.begin.reversibility {
                    Reversibility::NotApplicable => {
                        "it had no lasting effect to undo (recorded only)".to_string()
                    }
                    _ => "it cannot be undone".to_string(),
                });
        return Err(NotReversible(reason));
    }
    if op.begin.before.is_empty() {
        return Err(NotReversible(
            "its before-state was not recorded".to_string(),
        ));
    }
    if current.len() != op.begin.before.len() {
        return Err(NotReversible(
            "the current state does not match the recorded resources".to_string(),
        ));
    }
    let mut steps = Vec::new();
    for (before, now) in op.begin.before.iter().zip(current) {
        steps.extend(restore_steps(before, now)?);
    }
    // Stable: other steps keep their order (they never share a plan with
    // RabbitMQ ones).
    if steps.iter().any(Step::is_rabbitmq) {
        steps.sort_by_key(|step| step.rabbit_phase().unwrap_or(0));
    }
    Ok(steps)
}

/// The steps that turn `current` back into `before` (two states of the
/// same resource).
pub fn restore_steps(before: &State, current: &State) -> Result<Vec<Step>, NotReversible> {
    let mismatch = || {
        NotReversible(format!(
            "the recorded {} and the current one are different kinds of state",
            before.subject()
        ))
    };
    Ok(match (before, current) {
        (State::TopicConfig { topic, keys }, State::TopicConfig { keys: now_keys, .. }) => {
            let (set, delete) = config_changes(keys, now_keys);
            if set.is_empty() && delete.is_empty() {
                vec![]
            } else {
                vec![Step::TopicConfig {
                    topic: topic.clone(),
                    set,
                    delete,
                }]
            }
        }
        (State::BrokerConfig { broker, keys }, State::BrokerConfig { keys: now_keys, .. }) => {
            let (set, delete) = config_changes(keys, now_keys);
            if set.is_empty() && delete.is_empty() {
                vec![]
            } else {
                vec![Step::BrokerConfig {
                    broker: *broker,
                    set,
                    delete,
                }]
            }
        }
        (State::Topic { topic, exists }, State::Topic { exists: now, .. }) => match (exists, now) {
            (false, true) => vec![Step::DeleteTopic {
                topic: topic.clone(),
            }],
            (true, false) => {
                return Err(NotReversible(format!(
                    "topic {topic} was deleted; its records cannot be restored"
                )))
            }
            _ => vec![],
        },
        (
            State::Replicas { partitions },
            State::Replicas {
                partitions: now_partitions,
            },
        ) => {
            let moved: Vec<PartitionReplicas> = partitions
                .iter()
                .filter(|p| {
                    now_partitions
                        .iter()
                        .find(|n| n.topic == p.topic && n.partition == p.partition)
                        .is_none_or(|n| n.replicas != p.replicas)
                })
                .cloned()
                .collect();
            if moved.is_empty() {
                vec![]
            } else {
                vec![Step::Reassign { partitions: moved }]
            }
        }
        (
            State::GroupOffsets { group, offsets },
            State::GroupOffsets {
                offsets: now_offsets,
                ..
            },
        ) => {
            let now = |topic: &str, partition: i32| {
                now_offsets
                    .iter()
                    .find(|n| n.topic == topic && n.partition == partition)
                    .and_then(|n| n.offset)
            };
            let mut commit = Vec::new();
            let mut delete: BTreeMap<String, Vec<i32>> = BTreeMap::new();
            for p in offsets {
                let current = now(&p.topic, p.partition);
                match p.offset {
                    Some(offset) if current != Some(offset) => {
                        commit.push((p.topic.clone(), p.partition, offset))
                    }
                    None if current.is_some() => {
                        delete.entry(p.topic.clone()).or_default().push(p.partition)
                    }
                    _ => {}
                }
            }
            let mut steps = Vec::new();
            if !commit.is_empty() {
                steps.push(Step::CommitOffsets {
                    group: group.clone(),
                    offsets: commit,
                });
            }
            for (topic, partitions) in delete {
                steps.push(Step::DeleteOffsets {
                    group: group.clone(),
                    topic,
                    partitions,
                });
            }
            steps
        }
        (
            State::Acls { bindings },
            State::Acls {
                bindings: now_bindings,
            },
        ) => {
            let present_now =
                |b: &AclBinding| now_bindings.iter().any(|n| n.present && n.binding == *b);
            let create: Vec<AclBinding> = bindings
                .iter()
                .filter(|b| b.present && !present_now(&b.binding))
                .map(|b| b.binding.clone())
                .collect();
            let delete: Vec<AclBinding> = bindings
                .iter()
                .filter(|b| !b.present && present_now(&b.binding))
                .map(|b| b.binding.clone())
                .collect();
            let mut steps = Vec::new();
            if !delete.is_empty() {
                steps.push(Step::DeleteAcls { bindings: delete });
            }
            if !create.is_empty() {
                steps.push(Step::CreateAcls { bindings: create });
            }
            steps
        }
        (
            State::Quota { entity, key, value },
            State::Quota {
                value: now_value, ..
            },
        ) => {
            if value == now_value {
                vec![]
            } else {
                vec![Step::SetQuota {
                    entity: entity.clone(),
                    key: key.clone(),
                    value: *value,
                }]
            }
        }
        (
            State::Scram {
                user,
                mechanism,
                exists,
            },
            State::Scram { exists: now, .. },
        ) => match (exists, now) {
            (false, true) => vec![Step::DeleteScram {
                user: user.clone(),
                mechanism: mechanism.clone(),
            }],
            (true, false) => {
                return Err(NotReversible(format!(
                    "the {mechanism} credential of {user} was deleted; its password is not known"
                )))
            }
            _ => vec![],
        },
        (State::SubjectCompat { subject, level }, State::SubjectCompat { level: now, .. }) => {
            if level == now {
                vec![]
            } else {
                match level {
                    Some(level) => vec![Step::SetCompatibility {
                        subject: subject.clone(),
                        level: level.clone(),
                    }],
                    None => vec![Step::DeleteCompatibility {
                        subject: subject.clone(),
                    }],
                }
            }
        }
        (
            State::SubjectVersions { subject, versions },
            State::SubjectVersions { versions: now, .. },
        ) => {
            let before_ids: BTreeSet<u32> = versions.iter().map(|v| v.id).collect();
            let now_ids: BTreeSet<u32> = now.iter().map(|v| v.id).collect();
            let delete: Vec<u32> = now
                .iter()
                .filter(|v| !before_ids.contains(&v.id))
                .map(|v| v.version)
                .collect();
            let register: Vec<SchemaVersion> = versions
                .iter()
                .filter(|v| !now_ids.contains(&v.id))
                .cloned()
                .collect();
            if register.iter().any(|v| v.schema.is_empty()) {
                return Err(NotReversible(format!(
                    "the schema text of {subject} was not recorded"
                )));
            }
            let mut steps = Vec::new();
            if !delete.is_empty() {
                steps.push(Step::DeleteSchemaVersions {
                    subject: subject.clone(),
                    versions: delete,
                });
            }
            if !register.is_empty() {
                steps.push(Step::RegisterSchemas {
                    subject: subject.clone(),
                    versions: register,
                });
            }
            steps
        }
        (
            State::Connector {
                name,
                config,
                paused,
                stopped,
                secret_keys,
            },
            State::Connector {
                config: now_config,
                paused: now_paused,
                stopped: now_stopped,
                secret_keys: now_secrets,
                ..
            },
        ) => match (config, now_config) {
            (None, Some(_)) => vec![Step::DeleteConnector { name: name.clone() }],
            (None, None) => vec![],
            (Some(config), now_config) => {
                let mut steps = Vec::new();
                let visible = |c: &BTreeMap<String, String>, hidden: &[String]| {
                    c.iter()
                        .filter(|(k, _)| !hidden.contains(k))
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect::<BTreeMap<_, _>>()
                };
                let config_differs = match now_config {
                    None => true,
                    Some(now) => {
                        visible(config, secret_keys) != visible(now, now_secrets)
                            || config.keys().ne(now.keys())
                    }
                };
                if config_differs {
                    steps.push(Step::PutConnector {
                        name: name.clone(),
                        config: config.clone(),
                        secret_keys: secret_keys.clone(),
                    });
                }
                // A re-created connector starts running.
                let exists = now_config.is_some();
                let was = RunState::of(*paused, *stopped);
                let now = RunState::of(exists && *now_paused, exists && *now_stopped);
                if was != now {
                    let name = name.clone();
                    steps.push(match was {
                        RunState::Running => Step::ResumeConnector { name },
                        RunState::Paused => Step::PauseConnector { name },
                        RunState::Stopped => Step::StopConnector { name },
                    });
                }
                steps
            }
        },
        (
            State::ConnectorOffsets { name, offsets },
            State::ConnectorOffsets {
                offsets: now_offsets,
                ..
            },
        ) => {
            let changed: Vec<ConnectorOffset> = offsets
                .iter()
                .filter(|before| {
                    let now = now_offsets
                        .iter()
                        .find(|n| n.partition == before.partition)
                        .and_then(|n| n.offset.as_ref());
                    now != before.offset.as_ref()
                })
                .cloned()
                .collect();
            if changed.is_empty() {
                vec![]
            } else {
                vec![Step::AlterConnectorOffsets {
                    name: name.clone(),
                    offsets: changed,
                }]
            }
        }
        (
            State::RabbitQueue {
                vhost,
                name,
                declaration,
            },
            State::RabbitQueue {
                declaration: now, ..
            },
        ) => match (declaration, now) {
            (None, Some(_)) => vec![Step::DeleteQueue {
                vhost: vhost.clone(),
                name: name.clone(),
            }],
            (Some(declaration), None) => vec![Step::DeclareQueue {
                vhost: vhost.clone(),
                name: name.clone(),
                declaration: declaration.clone(),
            }],
            (Some(was), Some(now)) if was != now => {
                return Err(NotReversible(format!(
                    "queue {name} on vhost {vhost} was declared again with other settings; \
                     RabbitMQ cannot change a queue's settings in place"
                )))
            }
            _ => vec![],
        },
        (
            State::RabbitQueueMessages {
                vhost,
                name,
                messages,
            },
            State::RabbitQueueMessages { messages: now, .. },
        ) => {
            if messages == now {
                vec![]
            } else {
                return Err(NotReversible(format!(
                    "the messages of queue {name} on vhost {vhost} cannot be restored"
                )));
            }
        }
        (
            State::RabbitExchange {
                vhost,
                name,
                declaration,
            },
            State::RabbitExchange {
                declaration: now, ..
            },
        ) => match (declaration, now) {
            (None, Some(_)) => vec![Step::DeleteExchange {
                vhost: vhost.clone(),
                name: name.clone(),
            }],
            (Some(declaration), None) => vec![Step::DeclareExchange {
                vhost: vhost.clone(),
                name: name.clone(),
                declaration: declaration.clone(),
            }],
            (Some(was), Some(now)) if was != now => {
                return Err(NotReversible(format!(
                    "exchange {name} on vhost {vhost} was declared again with other settings; \
                     RabbitMQ cannot change an exchange's settings in place"
                )))
            }
            _ => vec![],
        },
        (
            State::RabbitBinding {
                vhost,
                binding,
                present,
            },
            State::RabbitBinding { present: now, .. },
        ) => match (present, now) {
            (true, false) => vec![Step::Bind {
                vhost: vhost.clone(),
                binding: binding.clone(),
            }],
            (false, true) => vec![Step::Unbind {
                vhost: vhost.clone(),
                binding: binding.clone(),
            }],
            _ => vec![],
        },
        _ => return Err(mismatch()),
    })
}

/// Sets and deletes that give every key of `before` its recorded own value
/// again. Sensitive keys are skipped (their values were never readable).
fn config_changes(
    before: &BTreeMap<String, ferrika_core::model::history::ConfigValue>,
    now: &BTreeMap<String, ferrika_core::model::history::ConfigValue>,
) -> (Vec<(String, String)>, Vec<String>) {
    let mut set = Vec::new();
    let mut delete = Vec::new();
    for (key, value) in before {
        if value.sensitive {
            continue;
        }
        let current = now.get(key).and_then(|v| v.own.as_ref());
        if current == value.own.as_ref() {
            continue;
        }
        match &value.own {
            Some(old) => set.push((key.clone(), old.clone())),
            None => delete.push(key.clone()),
        }
    }
    (set, delete)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrika_core::model::acl::{
        AclOperation, AclPermissionType, AclResourcePatternType, AclResourceType,
    };
    use ferrika_core::model::history::{
        AclPresence, Begin, ConfigValue, PartitionOffset, Scope, FORMAT_VERSION,
    };

    fn op(reversibility: Reversibility, before: Vec<State>, after: Vec<State>) -> Operation {
        let _ = FORMAT_VERSION;
        Operation {
            id: "A".into(),
            time_ms: 1,
            begin: Begin {
                source: "cli".into(),
                user: "u".into(),
                scope: Scope::default(),
                action: "x".into(),
                target: "t".into(),
                reversibility,
                limits: vec![],
                before,
                after,
                info: vec![],
                undoes: None,
            },
            end: None,
            ended_ms: None,
            notes: vec![],
            undone_by: None,
        }
    }

    fn cfg(pairs: &[(&str, Option<&str>, bool)]) -> BTreeMap<String, ConfigValue> {
        pairs
            .iter()
            .map(|(k, v, sensitive)| {
                (
                    k.to_string(),
                    ConfigValue {
                        own: v.map(str::to_string),
                        sensitive: *sensitive,
                    },
                )
            })
            .collect()
    }

    #[test]
    fn config_undo_sets_overrides_back_and_resets_defaults() {
        let before = State::TopicConfig {
            topic: "orders".into(),
            keys: cfg(&[
                ("retention.ms", Some("604800000"), false),
                ("cleanup.policy", None, false),
                ("ssl.key.password", None, true),
            ]),
        };
        let after = State::TopicConfig {
            topic: "orders".into(),
            keys: cfg(&[
                ("retention.ms", Some("1"), false),
                ("cleanup.policy", Some("compact"), false),
                ("ssl.key.password", None, true),
            ]),
        };
        let steps = plan(
            &op(Reversibility::Reversible, vec![before], vec![after.clone()]),
            &[after],
        )
        .unwrap();
        assert_eq!(
            steps,
            vec![Step::TopicConfig {
                topic: "orders".into(),
                set: vec![("retention.ms".into(), "604800000".into())],
                delete: vec!["cleanup.policy".into()],
            }]
        );
        assert_eq!(steps[0].policy_action(), PolicyAction::ConfigChange);
        // Values never appear in the description.
        assert!(!steps[0].describe().contains("604800000"));
    }

    #[test]
    fn offsets_commit_old_values_and_delete_uncommitted_partitions() {
        let offsets = |values: &[Option<i64>]| State::GroupOffsets {
            group: "g".into(),
            offsets: values
                .iter()
                .enumerate()
                .map(|(p, offset)| PartitionOffset {
                    topic: "orders".into(),
                    partition: p as i32,
                    offset: *offset,
                })
                .collect(),
        };
        let steps = restore_steps(
            &offsets(&[Some(10), None, Some(5)]),
            &offsets(&[Some(0), Some(0), Some(5)]),
        )
        .unwrap();
        assert_eq!(
            steps,
            vec![
                Step::CommitOffsets {
                    group: "g".into(),
                    offsets: vec![("orders".into(), 0, 10)],
                },
                Step::DeleteOffsets {
                    group: "g".into(),
                    topic: "orders".into(),
                    partitions: vec![1],
                },
            ]
        );
    }

    fn binding(principal: &str) -> AclBinding {
        AclBinding::new(
            AclResourceType::Topic,
            "orders",
            AclResourcePatternType::Literal,
            principal,
            "*",
            AclOperation::Read,
            AclPermissionType::Allow,
        )
    }

    #[test]
    fn acl_undo_deletes_created_and_recreates_deleted_bindings() {
        let state = |pairs: &[(&str, bool)]| State::Acls {
            bindings: pairs
                .iter()
                .map(|(p, present)| AclPresence {
                    binding: binding(p),
                    present: *present,
                })
                .collect(),
        };
        // An ACL create whose binding already existed changed nothing.
        assert!(
            restore_steps(&state(&[("a", true)]), &state(&[("a", true)]))
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            restore_steps(&state(&[("a", false)]), &state(&[("a", true)])).unwrap(),
            vec![Step::DeleteAcls {
                bindings: vec![binding("a")]
            }]
        );
        assert_eq!(
            restore_steps(
                &state(&[("a", true), ("b", true)]),
                &state(&[("a", false), ("b", false)])
            )
            .unwrap(),
            vec![Step::CreateAcls {
                bindings: vec![binding("a"), binding("b")]
            }]
        );
    }

    #[test]
    fn replaced_scram_credentials_and_deleted_topics_are_not_reversible() {
        let scram = |exists| State::Scram {
            user: "alice".into(),
            mechanism: "SCRAM-SHA-256".into(),
            exists,
        };
        assert_eq!(
            restore_steps(&scram(false), &scram(true)).unwrap(),
            vec![Step::DeleteScram {
                user: "alice".into(),
                mechanism: "SCRAM-SHA-256".into()
            }]
        );
        assert!(restore_steps(&scram(true), &scram(false)).is_err());
        let topic = |exists| State::Topic {
            topic: "t".into(),
            exists,
        };
        let steps = restore_steps(&topic(false), &topic(true)).unwrap();
        assert_eq!(steps[0].policy_action(), PolicyAction::TopicsDelete);
        assert!(restore_steps(&topic(true), &topic(false)).is_err());
        let irreversible = op(Reversibility::Irreversible, vec![], vec![]);
        assert!(plan(&irreversible, &[]).is_err());
    }

    #[test]
    fn schema_undo_deletes_new_versions_and_reregisters_deleted_ones() {
        let version = |version, id, schema: &str| SchemaVersion {
            version,
            id,
            schema_type: "AVRO".into(),
            schema: schema.into(),
            references: vec![],
        };
        let state = |versions: Vec<SchemaVersion>| State::SubjectVersions {
            subject: "s".into(),
            versions,
        };
        assert_eq!(
            restore_steps(
                &state(vec![version(1, 5, "")]),
                &state(vec![version(1, 5, ""), version(2, 9, "")])
            )
            .unwrap(),
            vec![Step::DeleteSchemaVersions {
                subject: "s".into(),
                versions: vec![2]
            }]
        );
        let steps =
            restore_steps(&state(vec![version(1, 5, "\"string\"")]), &state(vec![])).unwrap();
        assert!(matches!(&steps[0], Step::RegisterSchemas { versions, .. } if versions.len() == 1));
        // Without the text a deleted version cannot come back.
        assert!(restore_steps(&state(vec![version(1, 5, "")]), &state(vec![])).is_err());
        let compat = |level: Option<&str>| State::SubjectCompat {
            subject: "s".into(),
            level: level.map(str::to_string),
        };
        assert_eq!(
            restore_steps(&compat(None), &compat(Some("FULL"))).unwrap(),
            vec![Step::DeleteCompatibility {
                subject: "s".into()
            }]
        );
    }

    #[test]
    fn connector_undo_recreates_and_restores_the_paused_state() {
        let config = BTreeMap::from([("connector.class".to_string(), "X".to_string())]);
        let state = |config: Option<BTreeMap<String, String>>, paused| State::Connector {
            name: "c".into(),
            config,
            paused,
            stopped: false,
            secret_keys: vec![],
        };
        assert_eq!(
            restore_steps(&state(None, false), &state(Some(config.clone()), false)).unwrap(),
            vec![Step::DeleteConnector { name: "c".into() }]
        );
        assert_eq!(
            restore_steps(&state(Some(config.clone()), true), &state(None, false)).unwrap(),
            vec![
                Step::PutConnector {
                    name: "c".into(),
                    config: config.clone(),
                    secret_keys: vec![]
                },
                Step::PauseConnector { name: "c".into() }
            ]
        );
        assert_eq!(
            restore_steps(
                &state(Some(config.clone()), false),
                &state(Some(config), true)
            )
            .unwrap(),
            vec![Step::ResumeConnector { name: "c".into() }]
        );
    }

    #[test]
    fn connector_stop_is_undone_by_resuming_or_pausing() {
        let config = BTreeMap::from([("connector.class".to_string(), "X".to_string())]);
        let state = |paused, stopped| State::Connector {
            name: "c".into(),
            config: Some(config.clone()),
            paused,
            stopped,
            secret_keys: vec![],
        };
        let name = || "c".to_string();
        // Stopped a running connector: resume it.
        assert_eq!(
            restore_steps(&state(false, false), &state(false, true)).unwrap(),
            vec![Step::ResumeConnector { name: name() }]
        );
        // Stopped a paused one: pause it again.
        assert_eq!(
            restore_steps(&state(true, false), &state(false, true)).unwrap(),
            vec![Step::PauseConnector { name: name() }]
        );
        // Resumed a stopped one: stop it again.
        let undo = restore_steps(&state(false, true), &state(false, false)).unwrap();
        assert_eq!(undo, vec![Step::StopConnector { name: name() }]);
        assert_eq!(undo[0].policy_action(), PolicyAction::ConnectChange);
        assert!(restore_steps(&state(false, true), &state(false, true))
            .unwrap()
            .is_empty());
    }

    #[test]
    fn connector_offsets_undo_sets_the_recorded_offsets_back() {
        let offsets = |values: &[Option<i64>]| State::ConnectorOffsets {
            name: "sink".into(),
            offsets: values
                .iter()
                .enumerate()
                .map(|(p, o)| ConnectorOffset::sink("orders", p as i32, *o))
                .collect(),
        };
        // Partition 0 was moved, 1 unchanged, 2 had no offset before.
        let steps = restore_steps(
            &offsets(&[Some(10), Some(5), None]),
            &offsets(&[Some(0), Some(5), Some(3)]),
        )
        .unwrap();
        assert_eq!(
            steps,
            vec![Step::AlterConnectorOffsets {
                name: "sink".into(),
                offsets: vec![
                    ConnectorOffset::sink("orders", 0, Some(10)),
                    ConnectorOffset::sink("orders", 2, None),
                ],
            }]
        );
        assert_eq!(steps[0].policy_action(), PolicyAction::ConnectChange);
        assert!(steps[0].describe().contains("2 partitions"));
        // After a reset nothing is left: every recorded offset comes back.
        let now = State::ConnectorOffsets {
            name: "sink".into(),
            offsets: vec![],
        };
        let steps = restore_steps(&offsets(&[Some(10), Some(5)]), &now).unwrap();
        assert!(
            matches!(&steps[..], [Step::AlterConnectorOffsets { offsets, .. }] if offsets.len() == 2)
        );
        assert!(restore_steps(&offsets(&[Some(1)]), &offsets(&[Some(1)]))
            .unwrap()
            .is_empty());
    }

    #[test]
    fn mismatched_states_are_refused() {
        let topic = State::Topic {
            topic: "t".into(),
            exists: false,
        };
        let quota = State::Quota {
            entity: vec![],
            key: "k".into(),
            value: None,
        };
        assert!(restore_steps(&topic, &quota).is_err());
    }
}
