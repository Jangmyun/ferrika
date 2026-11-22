use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{KafkaError, KafkaResult};
use crate::model::acl::{
    AclBinding, AclFilter, AclOperation, AclPermissionType, AclResourcePatternType, AclResourceType,
};

/// In-memory and disk-persisted Kafka ACL store
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AclStore {
    pub acls: Vec<AclBinding>,
    #[serde(skip)]
    pub storage_path: Option<PathBuf>,
}

impl Default for AclStore {
    fn default() -> Self {
        Self {
            acls: Self::default_sample_acls(),
            storage_path: None,
        }
    }
}

/// `acls.json` in the folder of the profiles file `profiles`.
fn storage_path_next_to(profiles: &std::path::Path) -> PathBuf {
    profiles
        .parent()
        .map_or_else(|| PathBuf::from("acls.json"), |dir| dir.join("acls.json"))
}

impl AclStore {
    /// Returns default file location: `acls.json` next to `profiles.json`
    /// (`~/.ferrika/acls.json` unless `FERRIKA_CONFIG_DIR` or
    /// `FERRIKA_CONFIG_PATH` moves the configuration).
    pub fn default_storage_path() -> PathBuf {
        storage_path_next_to(&crate::kafka::profile::ProfileStore::default_path())
    }

    /// Loads the ACL store from the default location or initializes sample rules
    pub fn load_or_default() -> Self {
        let path = Self::default_storage_path();
        if path.exists() {
            match Self::load_from_file(&path) {
                Ok(mut store) => {
                    store.storage_path = Some(path);
                    return store;
                }
                Err(err) => {
                    tracing::warn!(%err, "Failed to load ACL store from disk; using default store");
                }
            }
        }

        Self {
            acls: Self::default_sample_acls(),
            storage_path: Some(path),
        }
    }

    /// Loads the ACL store from a specified file path
    pub fn load_from_file(path: &Path) -> KafkaResult<Self> {
        let content = fs::read_to_string(path).map_err(|e| {
            KafkaError::Io(format!(
                "Failed to read ACL store from {}: {e}",
                path.display()
            ))
        })?;
        let mut store: Self = serde_json::from_str(&content).map_err(|e| {
            KafkaError::Config(format!(
                "Failed to parse ACL JSON in {}: {e}",
                path.display()
            ))
        })?;
        store.storage_path = Some(path.to_path_buf());
        Ok(store)
    }

    /// Saves the current ACL store to the configured storage path
    pub fn save(&self) -> KafkaResult<()> {
        let path = self
            .storage_path
            .clone()
            .unwrap_or_else(Self::default_storage_path);
        self.save_to_file(&path)
    }

    /// Saves the ACL store to a specific file path
    pub fn save_to_file(&self, path: &Path) -> KafkaResult<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| {
                KafkaError::Io(format!(
                    "Failed to create directory {}: {e}",
                    parent.display()
                ))
            })?;
        }
        let json = serde_json::to_string_pretty(self)
            .map_err(|e| KafkaError::Config(format!("Failed to serialize ACLs to JSON: {e}")))?;
        fs::write(path, json).map_err(|e| {
            KafkaError::Io(format!(
                "Failed to write ACL store to {}: {e}",
                path.display()
            ))
        })?;
        Ok(())
    }

    /// Lists ACL bindings matching the provided filter
    pub fn list(&self, filter: &AclFilter) -> Vec<AclBinding> {
        self.acls
            .iter()
            .filter(|b| filter.matches(b))
            .cloned()
            .collect()
    }

    /// Lists the bindings equal to `filter` on every set field (see `matches_exact`)
    pub fn list_exact(&self, filter: &AclFilter) -> Vec<AclBinding> {
        self.acls
            .iter()
            .filter(|b| filter.matches_exact(b))
            .cloned()
            .collect()
    }

    /// Deletes only the bindings `matches_exact` selects, returning the count
    pub fn delete_exact(&mut self, filter: &AclFilter) -> usize {
        let before_len = self.acls.len();
        self.acls.retain(|b| !filter.matches_exact(b));
        before_len - self.acls.len()
    }

    /// Adds a new ACL binding (returns true if added, false if already exists)
    pub fn add(&mut self, binding: AclBinding) -> bool {
        if self.acls.iter().any(|b| b == &binding) {
            false
        } else {
            self.acls.push(binding);
            true
        }
    }

    /// Deletes all ACL bindings that match the provided filter, returning deleted count
    pub fn delete(&mut self, filter: &AclFilter) -> usize {
        let before_len = self.acls.len();
        self.acls.retain(|b| !filter.matches(b));
        before_len - self.acls.len()
    }

    /// Sample standard enterprise Kafka ACL rules
    pub fn default_sample_acls() -> Vec<AclBinding> {
        vec![
            // Admin full cluster access
            AclBinding::new(
                AclResourceType::Cluster,
                "kafka-cluster",
                AclResourcePatternType::Literal,
                "User:admin",
                "*",
                AclOperation::All,
                AclPermissionType::Allow,
            ),
            // Billing service producer
            AclBinding::new(
                AclResourceType::Topic,
                "orders-v1",
                AclResourcePatternType::Literal,
                "User:billing-service",
                "*",
                AclOperation::Write,
                AclPermissionType::Allow,
            ),
            AclBinding::new(
                AclResourceType::Topic,
                "orders-v1",
                AclResourcePatternType::Literal,
                "User:billing-service",
                "*",
                AclOperation::Describe,
                AclPermissionType::Allow,
            ),
            // Analytics consumer group read access
            AclBinding::new(
                AclResourceType::Topic,
                "orders-",
                AclResourcePatternType::Prefixed,
                "User:analytics-reader",
                "*",
                AclOperation::Read,
                AclPermissionType::Allow,
            ),
            AclBinding::new(
                AclResourceType::Group,
                "analytics-group",
                AclResourcePatternType::Literal,
                "User:analytics-reader",
                "*",
                AclOperation::Read,
                AclPermissionType::Allow,
            ),
            // Block untrusted public host access to audit logs
            AclBinding::new(
                AclResourceType::Topic,
                "audit-log",
                AclResourcePatternType::Literal,
                "User:*",
                "198.51.100.0",
                AclOperation::All,
                AclPermissionType::Deny,
            ),
        ]
    }
}

/// Security Risk Level for Kafka ACL Audit
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum AclRiskLevel {
    Info,
    Low,
    Medium,
    High,
    Critical,
}

impl fmt::Display for AclRiskLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Info => write!(f, "INFO"),
            Self::Low => write!(f, "LOW"),
            Self::Medium => write!(f, "MEDIUM"),
            Self::High => write!(f, "HIGH"),
            Self::Critical => write!(f, "CRITICAL"),
        }
    }
}

/// Security audit finding produced by evaluating Kafka ACL rules
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AclSecurityFinding {
    pub risk_level: AclRiskLevel,
    pub title: String,
    pub description: String,
    pub binding: AclBinding,
    pub recommendation: String,
}

/// Performs automated cluster security analysis over the given set of ACL bindings
pub fn audit_security_rules(bindings: &[AclBinding]) -> Vec<AclSecurityFinding> {
    let mut findings = Vec::new();

    for binding in bindings {
        // 1. Critical: Wildcard User: * granted Write or All on Topics or Cluster
        if binding.principal == "User:*"
            && (binding.operation == AclOperation::All || binding.operation == AclOperation::Write)
            && binding.permission_type == AclPermissionType::Allow
        {
            findings.push(AclSecurityFinding {
                risk_level: AclRiskLevel::Critical,
                title: "Wildcard Principal with Write/All Authorization".to_string(),
                description: format!(
                    "Principal 'User:*' is granted {} permission on {} '{}'. Any unauthenticated or arbitrary client can modify/delete data.",
                    binding.operation, binding.resource_type, binding.resource_name
                ),
                binding: binding.clone(),
                recommendation: "Restrict principal to explicit authenticated service identities (e.g. 'User:app-worker') instead of 'User:*'.".to_string(),
            });
        }

        // 2. High: Cluster-Wide Administrative Permission granted to non-admin user
        if binding.resource_type == AclResourceType::Cluster
            && (binding.operation == AclOperation::All
                || binding.operation == AclOperation::ClusterAction
                || binding.operation == AclOperation::Alter)
            && binding.permission_type == AclPermissionType::Allow
            && !binding.principal.to_lowercase().contains("admin")
        {
            findings.push(AclSecurityFinding {
                risk_level: AclRiskLevel::High,
                title: "Non-Admin Principal Granted Cluster-Level Authority".to_string(),
                description: format!(
                    "Principal '{}' has administrative operation '{}' on Cluster resource. This allows cluster reconfiguration or shutdown.",
                    binding.principal, binding.operation
                ),
                binding: binding.clone(),
                recommendation: "Limit Cluster and ClusterAction operations strictly to trusted administrator principals.".to_string(),
            });
        }

        // 3. Medium: Broad Unrestricted Host Wildcard on Destructive Operations
        if binding.host == "*"
            && (binding.operation == AclOperation::Delete
                || binding.operation == AclOperation::Alter
                || binding.operation == AclOperation::AlterConfigs)
            && binding.permission_type == AclPermissionType::Allow
        {
            findings.push(AclSecurityFinding {
                risk_level: AclRiskLevel::Medium,
                title: "Destructive Operation Authorized from Wildcard Host '*'".to_string(),
                description: format!(
                    "Principal '{}' is permitted to perform '{}' from any network host ('*').",
                    binding.principal, binding.operation
                ),
                binding: binding.clone(),
                recommendation: "Bind destructive administrative permissions to trusted private management subnets or jump-hosts.".to_string(),
            });
        }

        // 4. Low: Wildcard Topic Literal Pattern
        if binding.resource_type == AclResourceType::Topic
            && binding.resource_name == "*"
            && binding.pattern_type == AclResourcePatternType::Literal
            && binding.permission_type == AclPermissionType::Allow
        {
            findings.push(AclSecurityFinding {
                risk_level: AclRiskLevel::Low,
                title: "Literal Wildcard Topic Matching".to_string(),
                description: "Literal match with '*' matches only the literal topic named '*'. If intending to match all topics, use Prefixed or specific topic lists.".to_string(),
                binding: binding.clone(),
                recommendation: "Verify whether literal topic '*' was intended or if a domain topic prefix should be specified.".to_string(),
            });
        }
    }

    // 5. High: Allow / Deny Conflict Detection
    // When the same principal has both ALLOW and DENY on identical resource and operation
    for i in 0..bindings.len() {
        for j in (i + 1)..bindings.len() {
            let b1 = &bindings[i];
            let b2 = &bindings[j];

            if b1.principal == b2.principal
                && b1.resource_type == b2.resource_type
                && b1.resource_name == b2.resource_name
                && b1.pattern_type == b2.pattern_type
                && (b1.operation == b2.operation
                    || b1.operation == AclOperation::All
                    || b2.operation == AclOperation::All)
                && b1.permission_type != b2.permission_type
            {
                findings.push(AclSecurityFinding {
                    risk_level: AclRiskLevel::High,
                    title: "Conflicting ALLOW and DENY Rules".to_string(),
                    description: format!(
                        "Principal '{}' on {} '{}' has contradictory ALLOW and DENY rules. In Kafka, DENY always takes precedence, which may cause unexpected request rejections.",
                        b1.principal, b1.resource_type, b1.resource_name
                    ),
                    binding: b1.clone(),
                    recommendation: "Remove redundant ALLOW rule or scope the DENY rule to explicit network hosts.".to_string(),
                });
            }
        }
    }

    findings
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    #[test]
    fn the_local_acl_file_sits_next_to_the_profiles() {
        let dir = std::path::Path::new("cfg").join("ferrika");
        assert_eq!(
            super::storage_path_next_to(&dir.join("profiles.json")),
            dir.join("acls.json")
        );
    }

    use super::*;

    #[test]
    fn test_acl_enum_from_str_and_display() {
        assert_eq!(
            AclResourceType::from_str("topic").unwrap(),
            AclResourceType::Topic
        );
        assert_eq!(
            AclResourceType::from_str("cluster").unwrap(),
            AclResourceType::Cluster
        );
        assert_eq!(
            AclResourceType::from_str("group").unwrap(),
            AclResourceType::Group
        );
        assert_eq!(AclResourceType::Topic.to_string(), "Topic");

        assert_eq!(AclOperation::from_str("read").unwrap(), AclOperation::Read);
        assert_eq!(
            AclOperation::from_str("write").unwrap(),
            AclOperation::Write
        );
        assert_eq!(AclOperation::from_str("all").unwrap(), AclOperation::All);
        assert_eq!(
            AclOperation::from_str("cluster_action").unwrap(),
            AclOperation::ClusterAction
        );

        assert_eq!(
            AclPermissionType::from_str("allow").unwrap(),
            AclPermissionType::Allow
        );
        assert_eq!(
            AclPermissionType::from_str("deny").unwrap(),
            AclPermissionType::Deny
        );

        assert_eq!(
            AclResourcePatternType::from_str("prefixed").unwrap(),
            AclResourcePatternType::Prefixed
        );
    }

    #[test]
    fn test_acl_filter_literal_and_prefixed_matching() {
        let binding_literal = AclBinding::new(
            AclResourceType::Topic,
            "orders-v1",
            AclResourcePatternType::Literal,
            "User:alice",
            "*",
            AclOperation::Read,
            AclPermissionType::Allow,
        );

        let binding_prefixed = AclBinding::new(
            AclResourceType::Topic,
            "orders-",
            AclResourcePatternType::Prefixed,
            "User:bob",
            "*",
            AclOperation::Write,
            AclPermissionType::Allow,
        );

        // Exact match
        let filter1 = AclFilter {
            principal: Some("alice".to_string()),
            ..Default::default()
        };
        assert!(filter1.matches(&binding_literal));
        assert!(!filter1.matches(&binding_prefixed));

        // Prefixed resource match
        let filter2 = AclFilter {
            resource_name: Some("orders-v2".to_string()),
            ..Default::default()
        };
        assert!(filter2.matches(&binding_prefixed));
        assert!(!filter2.matches(&binding_literal));
    }

    #[test]
    fn test_acl_store_add_list_delete() {
        let mut store = AclStore {
            acls: Vec::new(),
            storage_path: None,
        };

        let b1 = AclBinding::new(
            AclResourceType::Topic,
            "payments",
            AclResourcePatternType::Literal,
            "User:service-pay",
            "*",
            AclOperation::Write,
            AclPermissionType::Allow,
        );

        // Add
        assert!(store.add(b1.clone()));
        // Duplicate should return false
        assert!(!store.add(b1.clone()));
        assert_eq!(store.acls.len(), 1);

        // List
        let list = store.list(&AclFilter {
            resource_type: Some(AclResourceType::Topic),
            ..Default::default()
        });
        assert_eq!(list.len(), 1);

        // Delete
        let deleted = store.delete(&AclFilter {
            principal: Some("service-pay".to_string()),
            ..Default::default()
        });
        assert_eq!(deleted, 1);
        assert_eq!(store.acls.len(), 0);
    }

    #[test]
    fn local_exact_delete_keeps_wildcard_and_prefix_bindings() {
        let mk = |name: &str, pat| {
            AclBinding::new(
                AclResourceType::Topic,
                name,
                pat,
                "User:x",
                "*",
                AclOperation::Read,
                AclPermissionType::Allow,
            )
        };
        let mut store = AclStore {
            acls: vec![
                mk("*", AclResourcePatternType::Literal),
                mk("billing", AclResourcePatternType::Literal),
                mk("bill", AclResourcePatternType::Prefixed),
            ],
            storage_path: None,
        };
        let filter = AclFilter {
            resource_type: Some(AclResourceType::Topic),
            resource_name: Some("billing".into()),
            principal: Some("x".into()),
            ..Default::default()
        };
        assert_eq!(store.list_exact(&filter).len(), 1);
        assert_eq!(store.delete_exact(&filter), 1);
        assert_eq!(store.acls.len(), 2);
        assert!(store.acls.iter().all(|b| b.resource_name != "billing"));
    }

    #[test]
    fn test_audit_security_rules() {
        let dangerous_rules = vec![
            // Critical: User:* Write on all topics
            AclBinding::new(
                AclResourceType::Topic,
                "orders",
                AclResourcePatternType::Literal,
                "User:*",
                "*",
                AclOperation::Write,
                AclPermissionType::Allow,
            ),
            // High: Non-admin has ClusterAction
            AclBinding::new(
                AclResourceType::Cluster,
                "kafka-cluster",
                AclResourcePatternType::Literal,
                "User:contractor",
                "*",
                AclOperation::ClusterAction,
                AclPermissionType::Allow,
            ),
            // Conflict
            AclBinding::new(
                AclResourceType::Topic,
                "telemetry",
                AclResourcePatternType::Literal,
                "User:analyst",
                "*",
                AclOperation::Read,
                AclPermissionType::Allow,
            ),
            AclBinding::new(
                AclResourceType::Topic,
                "telemetry",
                AclResourcePatternType::Literal,
                "User:analyst",
                "*",
                AclOperation::Read,
                AclPermissionType::Deny,
            ),
        ];

        let findings = audit_security_rules(&dangerous_rules);
        assert!(findings.len() >= 3);

        let critical_count = findings
            .iter()
            .filter(|f| f.risk_level == AclRiskLevel::Critical)
            .count();
        assert_eq!(critical_count, 1);

        let conflict_count = findings
            .iter()
            .filter(|f| f.title.contains("Conflicting"))
            .count();
        assert_eq!(conflict_count, 1);
    }
}
