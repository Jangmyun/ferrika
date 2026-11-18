//! The fix a lint finding suggests, and how it maps to the policy, the
//! audit log and a command to run by hand.

use ferrika_core::kafka::wire::{change_keys, ConfigChange};
use ferrika_core::model::policy::PolicyAction;

/// How a fix may be applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FixClass {
    /// Reversible dynamic config change without data movement: `--fix`
    /// applies it after one confirmation.
    Auto,
    /// Moves data, changes client behaviour or cannot be undone: applied
    /// only when its rule is named (`--rule ID`), confirmed one by one.
    Guided,
    /// Static config or a business decision: explained, never run.
    Manual,
}

impl FixClass {
    pub fn label(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Guided => "guided",
            Self::Manual => "manual",
        }
    }
}

/// A suggested fix, built only from the existing admin and wire calls.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Fix {
    /// IncrementalAlterConfigs on one topic.
    TopicConfig {
        topic: String,
        changes: Vec<ConfigChange>,
    },
    /// IncrementalAlterConfigs on one broker, or on the cluster-wide
    /// dynamic default when `broker` is `None`.
    BrokerConfig {
        broker: Option<i32>,
        changes: Vec<ConfigChange>,
    },
    /// Change a topic's replication factor (copies data).
    Replication { topic: String, target: usize },
    /// Preferred-leader election of these partitions only.
    ElectLeaders { partitions: Vec<(String, i32)> },
    /// Raise a topic's partition count (keyed records may move).
    AddPartitions { topic: String, total: usize },
    /// Delete a consumer group and its committed offsets.
    DeleteGroup { group: String },
    /// Steps for a person: commands or server.properties lines.
    Manual { steps: Vec<String> },
    /// Nothing to apply: a precondition is not met (`reason`).
    None { reason: String },
}

impl Fix {
    /// Whether `lint --fix` can run this fix.
    pub fn is_executable(&self) -> bool {
        !matches!(self, Self::Manual { .. } | Self::None { .. })
    }

    /// The company policy category the fix needs; `None` for fixes that
    /// change nothing.
    pub fn policy_action(&self) -> Option<PolicyAction> {
        Some(match self {
            Self::TopicConfig { .. } | Self::BrokerConfig { .. } => PolicyAction::ConfigChange,
            Self::Replication { .. } | Self::ElectLeaders { .. } | Self::AddPartitions { .. } => {
                PolicyAction::TopicsAlter
            }
            Self::DeleteGroup { .. } => PolicyAction::GroupsDelete,
            Self::Manual { .. } | Self::None { .. } => return None,
        })
    }

    /// What the fix applies to, for the audit log: names and keys, never
    /// config values.
    pub fn audit_target(&self) -> String {
        match self {
            Self::TopicConfig { topic, changes } => {
                format!("topic {topic}: {}", change_keys(changes))
            }
            Self::BrokerConfig {
                broker: Some(id),
                changes,
            } => format!("broker {id}: {}", change_keys(changes)),
            Self::BrokerConfig {
                broker: None,
                changes,
            } => format!("cluster default: {}", change_keys(changes)),
            Self::Replication { topic, target } => format!("topic {topic} to replication {target}"),
            Self::ElectLeaders { partitions } => {
                let mut topics: Vec<&str> = partitions.iter().map(|(t, _)| t.as_str()).collect();
                topics.dedup();
                format!("{} partition(s) of {}", partitions.len(), topics.join(", "))
            }
            Self::AddPartitions { topic, total } => format!("topic {topic} to {total} partitions"),
            Self::DeleteGroup { group } => format!("group {group}"),
            Self::Manual { .. } | Self::None { .. } => String::new(),
        }
    }

    /// The same fix as a command to run by hand: a `ferrika` command where
    /// Ferrika has one, else the Kafka tool's. `bootstrap` fills
    /// `--bootstrap-server` of the Kafka tools.
    pub fn cli_command(&self, bootstrap: &str) -> Option<String> {
        let server = if bootstrap.is_empty() {
            "<bootstrap>".to_string()
        } else {
            shell_arg(bootstrap)
        };
        Some(match self {
            Self::TopicConfig { topic, changes } => {
                let mut cmd = format!("ferrika topics config {}", shell_arg(topic));
                for change in changes {
                    match change {
                        ConfigChange::Set(k, v) => {
                            cmd.push_str(&format!(" -s {}", shell_arg(&format!("{k}={v}"))))
                        }
                        ConfigChange::Delete(k) => cmd.push_str(&format!(" -d {}", shell_arg(k))),
                    }
                }
                cmd
            }
            Self::BrokerConfig { broker, changes } => {
                let entity = match broker {
                    Some(id) => format!("--entity-name {id}"),
                    None => "--entity-default".to_string(),
                };
                let set: Vec<String> = changes
                    .iter()
                    .filter_map(|c| match c {
                        ConfigChange::Set(k, v) => Some(format!("{k}={v}")),
                        ConfigChange::Delete(_) => None,
                    })
                    .collect();
                let delete: Vec<&str> = changes
                    .iter()
                    .filter_map(|c| match c {
                        ConfigChange::Delete(k) => Some(k.as_str()),
                        ConfigChange::Set(..) => None,
                    })
                    .collect();
                let mut cmd = format!(
                    "kafka-configs.sh --bootstrap-server {server} --entity-type brokers {entity} --alter"
                );
                if !set.is_empty() {
                    cmd.push_str(&format!(" --add-config {}", shell_arg(&set.join(","))));
                }
                if !delete.is_empty() {
                    cmd.push_str(&format!(" --delete-config {}", shell_arg(&delete.join(","))));
                }
                cmd
            }
            Self::Replication { topic, target } => {
                format!("ferrika topics set-replication {} {target}", shell_arg(topic))
            }
            Self::ElectLeaders { partitions } => {
                // The command elects whole topics; lint elects only the
                // partitions it found.
                let mut topics: Vec<String> = partitions.iter().map(|(t, _)| shell_arg(t)).collect();
                topics.dedup();
                format!("ferrika topics elect-leaders {}", topics.join(" "))
            }
            Self::AddPartitions { topic, total } => format!(
                "kafka-topics.sh --bootstrap-server {server} --alter --topic {} --partitions {total}",
                shell_arg(topic)
            ),
            Self::DeleteGroup { group } => {
                format!("ferrika offset delete-group {}", shell_arg(group))
            }
            Self::Manual { .. } | Self::None { .. } => return None,
        })
    }

    /// What the fix does beyond its change, for the confirmation of a
    /// guided fix; empty when nothing.
    pub fn impact(&self) -> &'static str {
        match self {
            Self::Replication { .. } => "Data is copied to the new replicas in the background.",
            Self::AddPartitions { .. } => "Keyed records will map to other partitions.",
            Self::DeleteGroup { .. } => "The group and its committed offsets cannot be restored.",
            Self::TopicConfig { changes, .. }
                if changes.iter().any(
                    |c| matches!(c, ConfigChange::Set(k, _) if k == "min.insync.replicas"),
                ) =>
            {
                "acks=all producers fail while fewer replicas are in sync."
            }
            Self::TopicConfig { changes, .. }
                if changes
                    .iter()
                    .any(|c| matches!(c, ConfigChange::Set(k, _) if k == "segment.ms")) =>
            {
                "The topic will roll more segments."
            }
            Self::BrokerConfig { .. } => "Brokers apply the new value at once.",
            _ => "",
        }
    }

    /// The config changes of a config fix, empty for the others.
    pub fn config_changes(&self) -> &[ConfigChange] {
        match self {
            Self::TopicConfig { changes, .. } | Self::BrokerConfig { changes, .. } => changes,
            _ => &[],
        }
    }
}

/// Quotes `text` for a POSIX shell when it has more than name characters.
pub fn shell_arg(text: &str) -> String {
    let plain = !text.is_empty()
        && text.chars().all(|c| {
            c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-' | '=' | ',' | ':' | '/')
        });
    if plain {
        text.to_string()
    } else {
        format!("'{}'", text.replace('\'', r"'\''"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixes_map_to_policy_categories() {
        let config = Fix::TopicConfig {
            topic: "t".into(),
            changes: vec![ConfigChange::Set("min.insync.replicas".into(), "2".into())],
        };
        assert_eq!(config.policy_action(), Some(PolicyAction::ConfigChange));
        assert_eq!(
            Fix::BrokerConfig {
                broker: None,
                changes: vec![]
            }
            .policy_action(),
            Some(PolicyAction::ConfigChange)
        );
        for fix in [
            Fix::Replication {
                topic: "t".into(),
                target: 3,
            },
            Fix::ElectLeaders {
                partitions: vec![("t".into(), 0)],
            },
            Fix::AddPartitions {
                topic: "t".into(),
                total: 6,
            },
        ] {
            assert_eq!(
                fix.policy_action(),
                Some(PolicyAction::TopicsAlter),
                "{fix:?}"
            );
        }
        assert_eq!(
            Fix::DeleteGroup { group: "g".into() }.policy_action(),
            Some(PolicyAction::GroupsDelete)
        );
        assert_eq!(Fix::Manual { steps: vec![] }.policy_action(), None);
        assert_eq!(
            Fix::None {
                reason: String::new()
            }
            .policy_action(),
            None
        );
    }

    #[test]
    fn audit_targets_name_keys_but_never_values() {
        let fix = Fix::TopicConfig {
            topic: "orders".into(),
            changes: vec![ConfigChange::Set(
                "min.insync.replicas".into(),
                "secret-ish-42".into(),
            )],
        };
        assert_eq!(
            fix.audit_target(),
            "topic orders: min.insync.replicas (set)"
        );
        let broker = Fix::BrokerConfig {
            broker: None,
            changes: vec![ConfigChange::Set(
                "unclean.leader.election.enable".into(),
                "false".into(),
            )],
        };
        assert_eq!(
            broker.audit_target(),
            "cluster default: unclean.leader.election.enable (set)"
        );
        assert!(!broker.audit_target().contains("false"));
        assert_eq!(
            Fix::ElectLeaders {
                partitions: vec![("orders".into(), 0), ("orders".into(), 3)]
            }
            .audit_target(),
            "2 partition(s) of orders"
        );
        assert_eq!(
            Fix::Replication {
                topic: "orders".into(),
                target: 3
            }
            .audit_target(),
            "topic orders to replication 3"
        );
        assert_eq!(
            Fix::DeleteGroup {
                group: "old-etl".into()
            }
            .audit_target(),
            "group old-etl"
        );
    }

    #[test]
    fn shell_args_are_quoted_only_when_needed() {
        assert_eq!(shell_arg("orders.v1"), "orders.v1");
        assert_eq!(shell_arg("my group"), "'my group'");
        assert_eq!(shell_arg("it's"), r"'it'\''s'");
    }
}
