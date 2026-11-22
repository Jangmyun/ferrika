//! Reads the state a change is about to alter (before it runs), and the
//! same state again when undoing, so the two can be compared.

use std::collections::BTreeMap;

use ferrika_core::error::{KafkaError, KafkaResult};
use ferrika_core::kafka::admin::{ConfigOrigin, KafkaAdminClient};
use ferrika_core::kafka::config::KafkaClientConfig;
use ferrika_core::kafka::connect::ConnectClient;
use ferrika_core::kafka::schema::SchemaRegistryClient;
use ferrika_core::kafka::wire;
use ferrika_core::model::acl::{AclBinding, AclFilter};
use ferrika_core::model::history::{
    AclPresence, ConfigValue, PartitionOffset, PartitionReplicas, SchemaVersion, State,
};

/// Runs a blocking (wire protocol) call off the async threads.
pub(crate) async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> KafkaResult<T> + Send + 'static,
) -> KafkaResult<T> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| KafkaError::admin(format!("background task failed: {e}")))?
}

/// The Schema Registry client of `config`'s profile.
pub fn registry(config: &KafkaClientConfig) -> KafkaResult<SchemaRegistryClient> {
    config
        .schema_registry_url
        .as_deref()
        .filter(|u| !u.trim().is_empty())
        .map(|url| SchemaRegistryClient::new(url).with_timeout(config.request_timeout))
        .ok_or_else(|| KafkaError::config("This profile has no Schema Registry URL."))
}

/// The Kafka Connect client of `config`'s profile.
pub fn connect(config: &KafkaClientConfig) -> KafkaResult<ConnectClient> {
    let url = config
        .connect_url
        .as_deref()
        .filter(|u| !u.trim().is_empty())
        .ok_or_else(|| KafkaError::config("This profile has no Kafka Connect URL."))?;
    Ok(ConnectClient::new(url)?.with_timeout(config.request_timeout))
}

/// `keys` of `entries` as recorded: the value set on the resource itself
/// (`own_origin`), or `None` when it comes from a default.
fn config_keys(
    entries: &[ferrika_core::kafka::admin::ConfigEntryView],
    keys: &[String],
    own_origin: ConfigOrigin,
) -> BTreeMap<String, ConfigValue> {
    keys.iter()
        .map(|key| {
            let entry = entries.iter().find(|e| &e.name == key);
            let sensitive = entry.is_some_and(|e| e.sensitive);
            let own = entry
                .filter(|e| e.source == own_origin && !e.sensitive)
                .map(|e| e.value.clone());
            (key.clone(), ConfigValue { own, sensitive })
        })
        .collect()
}

/// The overrides of `keys` on `topic`.
pub async fn topic_config(
    admin: &KafkaAdminClient,
    topic: &str,
    keys: &[String],
) -> KafkaResult<State> {
    let entries = admin.describe_topic_configs(topic, None).await?;
    Ok(topic_config_of(topic, &entries, keys))
}

/// [`topic_config`] from `topic`'s config entries already read.
pub fn topic_config_of(
    topic: &str,
    entries: &[ferrika_core::kafka::admin::ConfigEntryView],
    keys: &[String],
) -> State {
    State::TopicConfig {
        topic: topic.to_string(),
        keys: config_keys(entries, keys, ConfigOrigin::TopicOverride),
    }
}

/// The per-broker dynamic values of `keys` on broker `broker`. A value
/// inherited from the cluster-wide default counts as not set: removing the
/// broker's own value brings it back.
pub async fn broker_config(
    admin: &KafkaAdminClient,
    broker: i32,
    keys: &[String],
) -> KafkaResult<State> {
    let entries = admin.describe_broker_configs(broker, None).await?;
    Ok(broker_config_of(broker, &entries, keys))
}

/// [`broker_config`] from broker `broker`'s config entries already read.
pub fn broker_config_of(
    broker: i32,
    entries: &[ferrika_core::kafka::admin::ConfigEntryView],
    keys: &[String],
) -> State {
    State::BrokerConfig {
        broker,
        keys: config_keys(entries, keys, ConfigOrigin::BrokerDynamic),
    }
}

/// The keys of `changes`, for reading their before-state.
pub fn change_keys(changes: &[wire::ConfigChange]) -> Vec<String> {
    changes
        .iter()
        .map(|c| match c {
            wire::ConfigChange::Set(k, _) | wire::ConfigChange::Delete(k) => k.clone(),
        })
        .collect()
}

/// A topic or broker config state with `changes` applied: the state the
/// change is meant to leave.
pub fn config_after(before: &State, changes: &[wire::ConfigChange]) -> State {
    let mut after = before.clone();
    if let State::TopicConfig { keys, .. } | State::BrokerConfig { keys, .. } = &mut after {
        for change in changes {
            let (key, own) = match change {
                wire::ConfigChange::Set(k, v) => (k, Some(v.clone())),
                wire::ConfigChange::Delete(k) => (k, None),
            };
            let entry = keys.entry(key.clone()).or_insert(ConfigValue {
                own: None,
                sensitive: false,
            });
            if !entry.sensitive {
                entry.own = own;
            }
        }
    }
    after
}

/// Whether a config state holds keys whose values cannot be read back.
pub fn sensitive_keys(state: &State) -> Vec<String> {
    match state {
        State::TopicConfig { keys, .. } | State::BrokerConfig { keys, .. } => keys
            .iter()
            .filter(|(_, v)| v.sensitive)
            .map(|(k, _)| k.clone())
            .collect(),
        _ => Vec::new(),
    }
}

/// Whether `topic` exists.
pub fn topic(admin: &KafkaAdminClient, topic: &str) -> KafkaResult<State> {
    let topics = admin.list_topics(None)?;
    Ok(State::Topic {
        topic: topic.to_string(),
        exists: topics.iter().any(|t| t == topic),
    })
}

/// The replicas of `topic`'s partitions (only `partitions` when given).
pub fn replicas(
    admin: &KafkaAdminClient,
    topic: &str,
    partitions: Option<&[i32]>,
) -> KafkaResult<State> {
    replicas_given_moves(admin, topic, partitions, &ongoing_moves(admin))
}

/// The partition moves in progress; none when they cannot be listed.
fn ongoing_moves(
    admin: &KafkaAdminClient,
) -> Vec<ferrika_core::model::cluster::OngoingReassignment> {
    match ferrika_core::kafka::wire::list_partition_reassignments(admin.config()) {
        Ok(moves) => moves,
        Err(e) => {
            tracing::warn!("Cannot check ongoing partition moves: {e}");
            Vec::new()
        }
    }
}

/// [`replicas`] with the moves in progress already listed. A partition being
/// moved lists its old and new replicas together; the state to return to is
/// the old set. (Its order is not known during a move, so the first replica,
/// the preferred leader, may differ from the one before the move.)
fn replicas_given_moves(
    admin: &KafkaAdminClient,
    topic: &str,
    partitions: Option<&[i32]>,
    moving: &[ferrika_core::model::cluster::OngoingReassignment],
) -> KafkaResult<State> {
    let summary = admin.describe_topic(topic, None)?;
    Ok(State::Replicas {
        partitions: summary
            .partitions
            .iter()
            .filter(|p| partitions.is_none_or(|only| only.contains(&p.partition_id)))
            .map(|p| PartitionReplicas {
                topic: topic.to_string(),
                partition: p.partition_id,
                replicas: ferrika_core::kafka::reassign::pre_move_replicas(
                    &p.replicas,
                    moving
                        .iter()
                        .find(|m| m.topic == topic && m.partition == p.partition_id),
                ),
            })
            .collect(),
    })
}

/// The replicas of the given partitions, across topics.
pub fn replicas_of(admin: &KafkaAdminClient, partitions: &[(String, i32)]) -> KafkaResult<State> {
    let mut topics: Vec<&str> = partitions.iter().map(|(t, _)| t.as_str()).collect();
    topics.sort_unstable();
    topics.dedup();
    let moving = ongoing_moves(admin);
    let mut out = Vec::new();
    for topic in topics {
        let only: Vec<i32> = partitions
            .iter()
            .filter(|(t, _)| t == topic)
            .map(|(_, p)| *p)
            .collect();
        if let State::Replicas { partitions } =
            replicas_given_moves(admin, topic, Some(&only), &moving)?
        {
            out.extend(partitions);
        }
    }
    Ok(State::Replicas { partitions: out })
}

/// `group`'s committed offsets on exactly `partitions` (`None` where it has
/// none).
pub async fn group_offsets(
    config: &KafkaClientConfig,
    group: &str,
    partitions: &[(String, i32)],
) -> KafkaResult<State> {
    let committed = committed_offsets(config, group).await?;
    Ok(State::GroupOffsets {
        group: group.to_string(),
        offsets: partitions
            .iter()
            .map(|(topic, partition)| PartitionOffset {
                topic: topic.clone(),
                partition: *partition,
                offset: committed.get(&(topic.clone(), *partition)).copied(),
            })
            .collect(),
    })
}

/// Every offset `group` has committed (on `topic` only, when given).
pub async fn group_committed(
    config: &KafkaClientConfig,
    group: &str,
    topic: Option<&str>,
) -> KafkaResult<State> {
    let committed = committed_offsets(config, group).await?;
    let mut offsets: Vec<PartitionOffset> = committed
        .into_iter()
        .filter(|((t, _), _)| topic.is_none_or(|only| only == t))
        .map(|((topic, partition), offset)| PartitionOffset {
            topic,
            partition,
            offset: Some(offset),
        })
        .collect();
    offsets.sort_by(|a, b| (&a.topic, a.partition).cmp(&(&b.topic, b.partition)));
    Ok(State::GroupOffsets {
        group: group.to_string(),
        offsets,
    })
}

async fn committed_offsets(
    config: &KafkaClientConfig,
    group: &str,
) -> KafkaResult<std::collections::HashMap<(String, i32), i64>> {
    let cfg = config.clone();
    let group = group.to_string();
    blocking(move || {
        let mut all = wire::fetch_group_offsets(&cfg, std::slice::from_ref(&group))?;
        Ok(all.remove(&group).unwrap_or_default())
    })
    .await
}

/// Whether each of `bindings` exists (exact match).
pub async fn acls(config: &KafkaClientConfig, bindings: &[AclBinding]) -> KafkaResult<State> {
    let cfg = config.clone();
    let bindings = bindings.to_vec();
    blocking(move || {
        let mut out = Vec::new();
        for binding in bindings {
            let found = wire::describe_acls(&cfg, &AclFilter::exact(&binding), true)?;
            let present = found.contains(&binding);
            out.push(AclPresence { binding, present });
        }
        Ok(State::Acls { bindings: out })
    })
    .await
}

/// One quota value of `entity`.
pub async fn quota(
    config: &KafkaClientConfig,
    entity: &ferrika_core::model::quota::QuotaEntity,
    key: &str,
) -> KafkaResult<State> {
    let cfg = config.clone();
    let (entity, key) = (entity.clone(), key.to_string());
    blocking(move || {
        let value = wire::describe_client_quotas(&cfg)?
            .into_iter()
            .find(|q| {
                ferrika_core::model::quota::same_quota_entity(&q.entity, &entity) && q.key == key
            })
            .map(|q| q.value);
        Ok(State::Quota { entity, key, value })
    })
    .await
}

/// Whether `user` has a credential for `mechanism` (e.g. `SCRAM-SHA-256`).
pub async fn scram(config: &KafkaClientConfig, user: &str, mechanism: &str) -> KafkaResult<State> {
    let cfg = config.clone();
    let (user, mechanism) = (user.to_string(), mechanism.to_string());
    blocking(move || {
        let exists = wire::describe_scram_users(&cfg)?
            .iter()
            .filter(|u| u.name == user)
            .flat_map(|u| &u.credentials)
            .any(|(hash, _)| hash.mechanism() == mechanism);
        Ok(State::Scram {
            user,
            mechanism,
            exists,
        })
    })
    .await
}

/// `subject`'s own compatibility level.
pub async fn subject_compat(registry: &SchemaRegistryClient, subject: &str) -> KafkaResult<State> {
    Ok(State::SubjectCompat {
        subject: subject.to_string(),
        level: registry.subject_compatibility_own(subject).await?,
    })
}

/// `subject`'s live versions; with `with_text`, their schemas too (needed
/// to register them again after the subject is deleted).
pub async fn subject_versions(
    registry: &SchemaRegistryClient,
    subject: &str,
    with_text: bool,
) -> KafkaResult<State> {
    let versions = registry
        .subject_versions_with_schemas(subject)
        .await?
        .into_iter()
        .map(|def| SchemaVersion {
            version: def.version.unwrap_or_default(),
            id: def.id,
            schema_type: def.schema_type,
            schema: if with_text { def.schema } else { String::new() },
            references: if with_text {
                def.references
            } else {
                Vec::new()
            },
        })
        .collect();
    Ok(State::SubjectVersions {
        subject: subject.to_string(),
        versions,
    })
}

/// Connector `name`: its config (with secret values, which the recorder
/// moves to the credential store) and whether it is paused.
pub async fn connector(connect: &ConnectClient, name: &str) -> KafkaResult<State> {
    let config = connect.get_config(name).await?;
    let paused = match &config {
        Some(_) => connect.status(name).await?.as_deref() == Some("PAUSED"),
        None => false,
    };
    let secret_keys = config
        .iter()
        .flatten()
        .filter(|(k, v)| ferrika_core::kafka::connect::is_sensitive_key(k) && !v.starts_with("${"))
        .map(|(k, _)| k.clone())
        .collect();
    Ok(State::Connector {
        name: name.to_string(),
        config,
        paused,
        secret_keys,
    })
}

/// Reads the resource `state` describes again, for the same items, as it
/// is now. Schema texts and connector secrets are not read back (undo uses
/// the recorded ones).
pub async fn recapture(
    state: &State,
    config: &KafkaClientConfig,
    admin: &KafkaAdminClient,
) -> KafkaResult<State> {
    match state {
        State::TopicConfig { topic, keys } => {
            let keys: Vec<String> = keys.keys().cloned().collect();
            topic_config(admin, topic, &keys).await
        }
        State::BrokerConfig { broker, keys } => {
            let keys: Vec<String> = keys.keys().cloned().collect();
            broker_config(admin, *broker, &keys).await
        }
        State::Topic { topic: name, .. } => topic(admin, name),
        State::Replicas { partitions } => {
            let wanted: Vec<(String, i32)> = partitions
                .iter()
                .map(|p| (p.topic.clone(), p.partition))
                .collect();
            replicas_of(admin, &wanted)
        }
        State::GroupOffsets { group, offsets } => {
            let partitions: Vec<(String, i32)> = offsets
                .iter()
                .map(|p| (p.topic.clone(), p.partition))
                .collect();
            group_offsets(config, group, &partitions).await
        }
        State::Acls { bindings } => {
            let bindings: Vec<AclBinding> = bindings.iter().map(|b| b.binding.clone()).collect();
            acls(config, &bindings).await
        }
        State::Quota { entity, key, .. } => quota(config, entity, key).await,
        State::Scram {
            user, mechanism, ..
        } => scram(config, user, mechanism).await,
        State::SubjectCompat { subject, .. } => subject_compat(&registry(config)?, subject).await,
        State::SubjectVersions { subject, .. } => {
            subject_versions(&registry(config)?, subject, false).await
        }
        State::Connector { name, .. } => {
            let mut state = connector(&connect(config)?, name).await?;
            // Secret values stay out of comparisons and memory alike.
            if let State::Connector {
                config: Some(values),
                secret_keys,
                ..
            } = &mut state
            {
                for key in secret_keys.iter() {
                    values.insert(key.clone(), String::new());
                }
            }
            Ok(state)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrika_core::kafka::admin::ConfigEntryView;

    fn entry(name: &str, value: &str, source: ConfigOrigin, sensitive: bool) -> ConfigEntryView {
        ConfigEntryView {
            name: name.into(),
            value: value.into(),
            source,
            read_only: false,
            sensitive,
        }
    }

    #[test]
    fn only_values_set_on_the_resource_itself_are_recorded() {
        let entries = vec![
            entry("retention.ms", "1", ConfigOrigin::TopicOverride, false),
            entry("cleanup.policy", "delete", ConfigOrigin::Default, false),
            entry(
                "ssl.key.password",
                "******",
                ConfigOrigin::TopicOverride,
                true,
            ),
        ];
        let keys: Vec<String> = [
            "retention.ms",
            "cleanup.policy",
            "ssl.key.password",
            "new.key",
        ]
        .iter()
        .map(|k| k.to_string())
        .collect();
        let state = config_keys(&entries, &keys, ConfigOrigin::TopicOverride);
        assert_eq!(state["retention.ms"].own.as_deref(), Some("1"));
        assert_eq!(state["cleanup.policy"].own, None);
        assert!(state["ssl.key.password"].sensitive);
        assert_eq!(state["ssl.key.password"].own, None);
        assert_eq!(state["new.key"].own, None);
        // On a broker, a cluster-wide default is not the broker's own value.
        let broker = vec![
            entry(
                "log.retention.ms",
                "5",
                ConfigOrigin::BrokerDynamicDefault,
                false,
            ),
            entry("num.io.threads", "8", ConfigOrigin::BrokerDynamic, false),
        ];
        let keys = vec!["log.retention.ms".to_string(), "num.io.threads".to_string()];
        let state = config_keys(&broker, &keys, ConfigOrigin::BrokerDynamic);
        assert_eq!(state["log.retention.ms"].own, None);
        assert_eq!(state["num.io.threads"].own.as_deref(), Some("8"));
    }

    #[test]
    fn config_after_applies_sets_and_deletes_but_not_to_sensitive_keys() {
        let mut keys = BTreeMap::new();
        keys.insert(
            "a".to_string(),
            ConfigValue {
                own: Some("1".into()),
                sensitive: false,
            },
        );
        keys.insert(
            "secret".to_string(),
            ConfigValue {
                own: None,
                sensitive: true,
            },
        );
        let before = State::TopicConfig {
            topic: "t".into(),
            keys,
        };
        let changes = vec![
            wire::ConfigChange::Set("a".into(), "2".into()),
            wire::ConfigChange::Set("secret".into(), "x".into()),
            wire::ConfigChange::Set("new".into(), "n".into()),
            wire::ConfigChange::Delete("gone".into()),
        ];
        assert_eq!(change_keys(&changes), vec!["a", "secret", "new", "gone"]);
        let State::TopicConfig { keys, .. } = config_after(&before, &changes) else {
            panic!("kind changed");
        };
        assert_eq!(keys["a"].own.as_deref(), Some("2"));
        assert_eq!(
            keys["secret"].own, None,
            "sensitive values are never recorded"
        );
        assert_eq!(keys["new"].own.as_deref(), Some("n"));
        assert_eq!(keys["gone"].own, None);
        assert_eq!(sensitive_keys(&before), vec!["secret"]);
        assert!(change_keys(&[]).is_empty());
        let topic = State::Topic {
            topic: "t".into(),
            exists: true,
        };
        assert!(sensitive_keys(&topic).is_empty());
    }
}
