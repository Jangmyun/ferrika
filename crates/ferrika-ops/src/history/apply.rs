//! Runs undo [`Step`]s with the existing admin, wire, registry and Connect
//! calls, after checking the conditions some of them need ([`blockers`]).

use std::time::Duration;

use ferrika_core::error::{KafkaError, KafkaResult};
use ferrika_core::kafka::admin::KafkaAdminClient;
use ferrika_core::kafka::config::KafkaClientConfig;
use ferrika_core::kafka::offset_reset::{commit_offset_reset, OffsetResetPlan};
use ferrika_core::kafka::reassign::PartitionAssignment;
use ferrika_core::kafka::wire::{self, ConfigChange};
use ferrika_core::model::acl::AclFilter;

use super::capture::{blocking, connect, registry};
use super::inverse::Step;

/// What undo runs against.
pub struct Target<'a> {
    pub config: &'a KafkaClientConfig,
    pub admin: &'a KafkaAdminClient,
    /// The operation being undone; its connector secrets are in the
    /// credential store under this id.
    pub op: &'a str,
}

/// Conditions that stop undo outright, even with `--force`: a topic made
/// by the change now holds records or committed offsets (deleting it would
/// lose data), a group to rewind has active members, connector secrets
/// were not kept. One message per problem.
pub async fn blockers(steps: &[Step], target: &Target<'_>) -> Vec<String> {
    let mut problems = Vec::new();
    for step in steps {
        match step {
            Step::DeleteTopic { topic } => {
                if let Err(e) = topic_is_unused(target, topic).await {
                    problems.push(e);
                }
            }
            Step::CommitOffsets { group, .. } | Step::DeleteOffsets { group, .. } => {
                match target.admin.list_consumer_group_states(None) {
                    Ok(groups) => {
                        if let Some((_, state, members)) =
                            groups.iter().find(|(name, _, _)| name == group)
                        {
                            if *members > 0 {
                                problems.push(format!(
                                    "group {group} has {members} active member(s) ({state}); stop its consumers first"
                                ));
                            }
                        }
                    }
                    Err(e) => problems.push(format!("cannot read the state of group {group}: {e}")),
                }
            }
            Step::PutConnector {
                name, secret_keys, ..
            } => {
                let profile = super::secret_store_profile(target.op);
                let missing: Vec<&String> = secret_keys
                    .iter()
                    .filter(|key| {
                        !matches!(
                            ferrika_core::kafka::secrets::get(&profile, key),
                            Ok(Some(_))
                        )
                    })
                    .collect();
                if !missing.is_empty() {
                    problems.push(format!(
                        "the secret values of connector {name} were not kept ({}); restoring it without them would break it",
                        missing.iter().map(|k| k.as_str()).collect::<Vec<_>>().join(", ")
                    ));
                }
            }
            _ => {}
        }
    }
    problems
}

/// `Err` with the reason when `topic` has records or committed offsets.
async fn topic_is_unused(target: &Target<'_>, topic: &str) -> Result<(), String> {
    let summary = target
        .admin
        .describe_topic(topic, None)
        .map_err(|e| format!("cannot read topic {topic}: {e}"))?;
    let partitions: Vec<(String, i32)> = summary
        .partitions
        .iter()
        .map(|p| (topic.to_string(), p.partition_id))
        .collect();
    let cfg = target.config.clone();
    let wanted = partitions.clone();
    let marks = blocking(move || {
        let consumer = ferrika_core::kafka::consumer::KafkaConsumerClient::new(&cfg)?;
        let keys: Vec<(&str, i32)> = wanted.iter().map(|(t, p)| (t.as_str(), *p)).collect();
        consumer.fetch_watermarks_batch(&keys, Some(Duration::from_secs(10)))
    })
    .await
    .map_err(|e| format!("cannot read the offsets of topic {topic}: {e}"))?;
    if marks.len() < partitions.len() {
        return Err(format!(
            "cannot read the offsets of every partition of topic {topic}"
        ));
    }
    let written: i64 = marks.values().map(|(_, high)| *high).sum();
    if written > 0 {
        return Err(format!(
            "topic {topic} has received {written} record(s) since it was created; deleting it would lose them"
        ));
    }
    let groups = target
        .admin
        .list_consumer_groups(None)
        .map_err(|e| format!("cannot list consumer groups: {e}"))?;
    let cfg = target.config.clone();
    let offsets = blocking(move || wire::fetch_group_offsets(&cfg, &groups))
        .await
        .map_err(|e| format!("cannot read committed offsets: {e}"))?;
    let mut using: Vec<&String> = offsets
        .iter()
        .filter(|(_, committed)| committed.keys().any(|(t, _)| t == topic))
        .map(|(group, _)| group)
        .collect();
    using.sort();
    if !using.is_empty() {
        return Err(format!(
            "group(s) {} have committed offsets on topic {topic}",
            using
                .iter()
                .map(|g| g.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    Ok(())
}

/// Runs one step.
pub async fn run(step: &Step, target: &Target<'_>) -> KafkaResult<()> {
    let config = target.config;
    match step {
        Step::TopicConfig { topic, set, delete } => {
            let changes = config_changes(set, delete);
            let (cfg, topic) = (config.clone(), topic.clone());
            blocking(move || wire::incremental_alter_topic_config(&cfg, &topic, &changes, false))
                .await
        }
        Step::BrokerConfig {
            broker,
            set,
            delete,
        } => {
            let addr = target
                .admin
                .list_brokers(None)?
                .into_iter()
                .find(|b| b.id == *broker)
                .map(|b| format!("{}:{}", b.host, b.port))
                .ok_or_else(|| {
                    KafkaError::admin(format!("broker {broker} is not in the cluster"))
                })?;
            let changes = config_changes(set, delete);
            let (cfg, broker) = (config.clone(), *broker);
            blocking(move || {
                wire::incremental_alter_broker_config(&cfg, broker, &addr, &changes, false)
            })
            .await
        }
        Step::DeleteTopic { topic } => target.admin.delete_topic(topic, None).await,
        Step::Reassign { partitions } => {
            let assignments: Vec<PartitionAssignment> = partitions
                .iter()
                .map(|p| PartitionAssignment::new(&p.topic, p.partition, p.replicas.clone()))
                .collect();
            let cfg = config.clone();
            let (results, _) =
                blocking(move || wire::reassign_or_set_replication(&cfg, &assignments)).await?;
            let failed: Vec<String> = results
                .into_iter()
                .filter_map(|r| r.error.map(|e| format!("{}-{}: {e}", r.topic, r.partition)))
                .collect();
            if failed.is_empty() {
                Ok(())
            } else {
                Err(KafkaError::admin(format!(
                    "reassignment refused for {}",
                    failed.join("; ")
                )))
            }
        }
        Step::CommitOffsets { group, offsets } => {
            let plans: Vec<OffsetResetPlan> = offsets
                .iter()
                .map(|(topic, partition, offset)| OffsetResetPlan {
                    group_id: group.clone(),
                    topic: topic.clone(),
                    partition: *partition,
                    current_offset: -1,
                    low_watermark: 0,
                    high_watermark: 0,
                    target_offset: *offset,
                    delta: 0,
                })
                .collect();
            let (cfg, group) = (config.clone(), group.clone());
            blocking(move || commit_offset_reset(&cfg, &group, &plans)).await
        }
        Step::DeleteOffsets {
            group,
            topic,
            partitions,
        } => {
            let (cfg, group, topic, partitions) = (
                config.clone(),
                group.clone(),
                topic.clone(),
                partitions.clone(),
            );
            blocking(move || wire::delete_group_offsets(&cfg, &group, &topic, &partitions))
                .await
                .map(|_| ())
        }
        Step::CreateAcls { bindings } => {
            let (cfg, bindings) = (config.clone(), bindings.clone());
            blocking(move || wire::create_acls(&cfg, &bindings)).await
        }
        Step::DeleteAcls { bindings } => {
            let (cfg, bindings) = (config.clone(), bindings.clone());
            blocking(move || {
                delete_each_acl(&bindings, |b| {
                    wire::delete_acls(&cfg, &AclFilter::exact(b)).map(|_| ())
                })
            })
            .await
        }
        Step::SetQuota { entity, key, value } => {
            let (cfg, entity, key, value) = (config.clone(), entity.clone(), key.clone(), *value);
            blocking(move || wire::alter_client_quota(&cfg, &entity, &key, value)).await
        }
        Step::DeleteScram { user, mechanism } => {
            let hash =
                ferrika_core::kafka::scram::ScramHash::parse(mechanism).ok_or_else(|| {
                    KafkaError::config(format!("unknown SCRAM mechanism {mechanism}"))
                })?;
            let (cfg, user) = (config.clone(), user.clone());
            blocking(move || wire::alter_scram_user(&cfg, &user, hash, None, 8192)).await
        }
        Step::SetCompatibility { subject, level } => {
            registry(config)?.set_compatibility(subject, level).await
        }
        Step::DeleteCompatibility { subject } => {
            registry(config)?.delete_compatibility(subject).await
        }
        Step::DeleteSchemaVersions { subject, versions } => {
            let client = registry(config)?;
            for version in versions {
                client.delete_version(subject, *version).await?;
            }
            Ok(())
        }
        Step::RegisterSchemas { subject, versions } => {
            let client = registry(config)?;
            for version in versions {
                let def = ferrika_core::codec::schema::SchemaDefinition {
                    id: version.id,
                    schema: version.schema.clone(),
                    schema_type: version.schema_type.clone(),
                    version: None,
                    subject: Some(subject.clone()),
                    references: version.references.clone(),
                    resolved_references: Vec::new(),
                    resolved_reference_names: Vec::new(),
                };
                client.register_definition(subject, &def).await?;
            }
            Ok(())
        }
        Step::PutConnector {
            name,
            config: settings,
            secret_keys,
        } => {
            let mut settings = settings.clone();
            let profile = super::secret_store_profile(target.op);
            for key in secret_keys {
                let value = ferrika_core::kafka::secrets::get(&profile, key)?.ok_or_else(|| {
                    KafkaError::config(format!("the secret value of {key} was not kept"))
                })?;
                settings.insert(key.clone(), value);
            }
            connect(config)?.put_config(name, &settings).await
        }
        Step::DeleteConnector { name } => connect(config)?.delete(name).await,
        Step::PauseConnector { name } => {
            connect(config)?
                .act(name, ferrika_core::kafka::connect::ConnectorAction::Pause)
                .await
        }
        Step::ResumeConnector { name } => {
            connect(config)?
                .act(name, ferrika_core::kafka::connect::ConnectorAction::Resume)
                .await
        }
        Step::DeclareQueue { .. }
        | Step::DeleteQueue { .. }
        | Step::DeclareExchange { .. }
        | Step::DeleteExchange { .. }
        | Step::Bind { .. }
        | Step::Unbind { .. } => Err(KafkaError::config(format!(
            "'{}' changes RabbitMQ; it runs through the management API \
             (ferrika_ops::history::rabbit), not a Kafka client",
            step.describe()
        ))),
    }
}

fn config_changes(set: &[(String, String)], delete: &[String]) -> Vec<ConfigChange> {
    set.iter()
        .map(|(k, v)| ConfigChange::Set(k.clone(), v.clone()))
        .chain(delete.iter().map(|k| ConfigChange::Delete(k.clone())))
        .collect()
}

/// Deletes `bindings` one by one with `delete`. It stops at the first failure
/// and says how many were already deleted and which ones were not attempted,
/// so a partially applied undo is visible instead of looking like a clean failure.
fn delete_each_acl(
    bindings: &[ferrika_core::model::acl::AclBinding],
    mut delete: impl FnMut(&ferrika_core::model::acl::AclBinding) -> KafkaResult<()>,
) -> KafkaResult<()> {
    for (done, binding) in bindings.iter().enumerate() {
        if let Err(e) = delete(binding) {
            if done == 0 {
                return Err(e);
            }
            let remaining: Vec<String> = bindings[done..]
                .iter()
                .map(|b| {
                    format!(
                        "{:?} {} {} {:?}",
                        b.resource_type, b.resource_name, b.principal, b.operation
                    )
                })
                .collect();
            return Err(KafkaError::admin(format!(
                "partially applied: {done} of {} ACL bindings deleted; failed on #{} ({e}); not deleted: {}",
                bindings.len(),
                done + 1,
                remaining.join(", ")
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrika_core::model::acl::{
        AclBinding, AclOperation, AclPermissionType, AclResourcePatternType, AclResourceType,
    };

    fn binding(topic: &str) -> AclBinding {
        AclBinding::new(
            AclResourceType::Topic,
            topic,
            AclResourcePatternType::Literal,
            "User:alice",
            "*",
            AclOperation::Read,
            AclPermissionType::Allow,
        )
    }

    #[test]
    fn delete_each_acl_succeeds_and_accepts_empty() {
        assert!(delete_each_acl(&[], |_| Err(KafkaError::admin("never"))).is_ok());
        let bs = [binding("a"), binding("b")];
        assert!(delete_each_acl(&bs, |_| Ok(())).is_ok());
    }

    #[test]
    fn delete_each_acl_first_failure_is_plain() {
        let bs = [binding("a"), binding("b")];
        let err = delete_each_acl(&bs, |_| Err(KafkaError::admin("boom"))).unwrap_err();
        assert!(!err.to_string().contains("partially applied"));
    }

    #[test]
    fn delete_each_acl_reports_partial_application() {
        let bs = [binding("a"), binding("b"), binding("c")];
        let mut calls = 0;
        let err = delete_each_acl(&bs, |_| {
            calls += 1;
            if calls == 2 {
                Err(KafkaError::admin("boom"))
            } else {
                Ok(())
            }
        })
        .unwrap_err()
        .to_string();
        assert_eq!(calls, 2);
        assert!(err.contains("partially applied: 1 of 3"), "{err}");
        assert!(err.contains(" b ") && err.contains(" c "), "{err}");
        assert!(!err.contains("Topic a "), "{err}");
    }
}
