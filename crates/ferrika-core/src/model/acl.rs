//! Kafka ACL bindings and filters: the resource, pattern, operation and
//! permission enums with their text forms.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::error::KafkaError;

/// Kafka ACL Resource Type (e.g. Topic, Group, Cluster)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AclResourceType {
    Topic,
    Group,
    Cluster,
    TransactionalId,
    DelegationToken,
}

impl fmt::Display for AclResourceType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Topic => write!(f, "Topic"),
            Self::Group => write!(f, "Group"),
            Self::Cluster => write!(f, "Cluster"),
            Self::TransactionalId => write!(f, "TransactionalId"),
            Self::DelegationToken => write!(f, "DelegationToken"),
        }
    }
}

impl FromStr for AclResourceType {
    type Err = KafkaError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "TOPIC" => Ok(Self::Topic),
            "GROUP" => Ok(Self::Group),
            "CLUSTER" => Ok(Self::Cluster),
            "TRANSACTIONAL_ID" | "TRANSACTIONALID" => Ok(Self::TransactionalId),
            "DELEGATION_TOKEN" | "DELEGATIONTOKEN" => Ok(Self::DelegationToken),
            other => Err(KafkaError::Acl(format!(
                "Unknown resource type: '{other}'. Expected: Topic, Group, Cluster, TransactionalId, DelegationToken"
            ))),
        }
    }
}

/// Kafka ACL Resource Pattern Type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AclResourcePatternType {
    #[default]
    Literal,
    Prefixed,
    Match,
}

impl fmt::Display for AclResourcePatternType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Literal => write!(f, "Literal"),
            Self::Prefixed => write!(f, "Prefixed"),
            Self::Match => write!(f, "Match"),
        }
    }
}

impl FromStr for AclResourcePatternType {
    type Err = KafkaError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "LITERAL" => Ok(Self::Literal),
            "PREFIXED" => Ok(Self::Prefixed),
            "MATCH" => Ok(Self::Match),
            other => Err(KafkaError::Acl(format!(
                "Unknown pattern type: '{other}'. Expected: Literal, Prefixed, Match"
            ))),
        }
    }
}

/// Kafka ACL Operation (e.g. Read, Write, All, Create)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AclOperation {
    All,
    Read,
    Write,
    Create,
    Delete,
    Alter,
    Describe,
    ClusterAction,
    DescribeConfigs,
    AlterConfigs,
    IdempotentWrite,
}

impl fmt::Display for AclOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::All => write!(f, "All"),
            Self::Read => write!(f, "Read"),
            Self::Write => write!(f, "Write"),
            Self::Create => write!(f, "Create"),
            Self::Delete => write!(f, "Delete"),
            Self::Alter => write!(f, "Alter"),
            Self::Describe => write!(f, "Describe"),
            Self::ClusterAction => write!(f, "ClusterAction"),
            Self::DescribeConfigs => write!(f, "DescribeConfigs"),
            Self::AlterConfigs => write!(f, "AlterConfigs"),
            Self::IdempotentWrite => write!(f, "IdempotentWrite"),
        }
    }
}

impl FromStr for AclOperation {
    type Err = KafkaError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "ALL" => Ok(Self::All),
            "READ" => Ok(Self::Read),
            "WRITE" => Ok(Self::Write),
            "CREATE" => Ok(Self::Create),
            "DELETE" => Ok(Self::Delete),
            "ALTER" => Ok(Self::Alter),
            "DESCRIBE" => Ok(Self::Describe),
            "CLUSTERACTION" | "CLUSTER_ACTION" => Ok(Self::ClusterAction),
            "DESCRIBECONFIGS" | "DESCRIBE_CONFIGS" => Ok(Self::DescribeConfigs),
            "ALTERCONFIGS" | "ALTER_CONFIGS" => Ok(Self::AlterConfigs),
            "IDEMPOTENTWRITE" | "IDEMPOTENT_WRITE" => Ok(Self::IdempotentWrite),
            other => Err(KafkaError::Acl(format!(
                "Unknown operation: '{other}'. Expected: All, Read, Write, Create, Delete, Alter, Describe, ClusterAction, DescribeConfigs, AlterConfigs, IdempotentWrite"
            ))),
        }
    }
}

/// Kafka ACL Permission Type (Allow or Deny)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AclPermissionType {
    #[default]
    Allow,
    Deny,
}

impl fmt::Display for AclPermissionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Allow => write!(f, "ALLOW"),
            Self::Deny => write!(f, "DENY"),
        }
    }
}

impl FromStr for AclPermissionType {
    type Err = KafkaError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "ALLOW" => Ok(Self::Allow),
            "DENY" => Ok(Self::Deny),
            other => Err(KafkaError::Acl(format!(
                "Unknown permission type: '{other}'. Expected: Allow, Deny"
            ))),
        }
    }
}

/// Concrete Kafka ACL binding entry representing an authorization grant or denial
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AclBinding {
    pub resource_type: AclResourceType,
    pub resource_name: String,
    pub pattern_type: AclResourcePatternType,
    pub principal: String,
    pub host: String,
    pub operation: AclOperation,
    pub permission_type: AclPermissionType,
}

impl AclBinding {
    pub fn new(
        resource_type: AclResourceType,
        resource_name: impl Into<String>,
        pattern_type: AclResourcePatternType,
        principal: impl Into<String>,
        host: impl Into<String>,
        operation: AclOperation,
        permission_type: AclPermissionType,
    ) -> Self {
        let principal_str = principal.into();
        let formatted_principal = if principal_str.starts_with("User:") {
            principal_str
        } else {
            format!("User:{principal_str}")
        };

        Self {
            resource_type,
            resource_name: resource_name.into(),
            pattern_type,
            principal: formatted_principal,
            host: host.into(),
            operation,
            permission_type,
        }
    }
}

/// Query filter for listing or deleting matching ACL bindings
#[derive(Debug, Clone, Default)]
pub struct AclFilter {
    pub resource_type: Option<AclResourceType>,
    pub resource_name: Option<String>,
    pub pattern_type: Option<AclResourcePatternType>,
    pub principal: Option<String>,
    pub host: Option<String>,
    pub operation: Option<AclOperation>,
    pub permission_type: Option<AclPermissionType>,
}

impl AclFilter {
    /// A filter that matches exactly `binding`.
    pub fn exact(binding: &AclBinding) -> Self {
        Self {
            resource_type: Some(binding.resource_type),
            resource_name: Some(binding.resource_name.clone()),
            pattern_type: Some(binding.pattern_type),
            principal: Some(binding.principal.clone()),
            host: Some(binding.host.clone()),
            operation: Some(binding.operation),
            permission_type: Some(binding.permission_type),
        }
    }

    /// Returns true if the target `AclBinding` matches all non-empty filter criteria
    pub fn matches(&self, binding: &AclBinding) -> bool {
        if let Some(ref rt) = self.resource_type {
            if *rt != binding.resource_type {
                return false;
            }
        }

        if let Some(ref rn) = self.resource_name {
            match binding.pattern_type {
                AclResourcePatternType::Literal => {
                    if binding.resource_name != "*" && binding.resource_name != *rn {
                        return false;
                    }
                }
                AclResourcePatternType::Prefixed => {
                    if !rn.starts_with(&binding.resource_name) {
                        return false;
                    }
                }
                AclResourcePatternType::Match => {
                    if binding.resource_name != *rn {
                        return false;
                    }
                }
            }
        }

        if let Some(ref pt) = self.pattern_type {
            if *pt != binding.pattern_type {
                return false;
            }
        }

        if let Some(ref p) = self.principal {
            let target_p = if p.starts_with("User:") {
                p.to_string()
            } else {
                format!("User:{p}")
            };
            if binding.principal != "*"
                && !binding.principal.eq_ignore_ascii_case(&target_p)
                && !binding.principal.eq_ignore_ascii_case(p)
            {
                return false;
            }
        }

        if let Some(ref h) = self.host {
            if binding.host != "*" && binding.host != *h {
                return false;
            }
        }

        if let Some(ref op) = self.operation {
            if *op != AclOperation::All
                && binding.operation != AclOperation::All
                && *op != binding.operation
            {
                return false;
            }
        }

        if let Some(ref perm) = self.permission_type {
            if *perm != binding.permission_type {
                return false;
            }
        }

        true
    }

    /// Like `matches`, but every set field must be equal: no wildcard bindings
    /// or prefix rules widen the match, and a named resource matches only its
    /// literal binding (what Kafka's DeleteAcls does with a literal filter).
    pub fn matches_exact(&self, binding: &AclBinding) -> bool {
        if self
            .resource_type
            .is_some_and(|t| t != binding.resource_type)
        {
            return false;
        }
        if let Some(name) = &self.resource_name {
            let pattern = self.pattern_type.unwrap_or(AclResourcePatternType::Literal);
            if binding.resource_name != *name || binding.pattern_type != pattern {
                return false;
            }
        } else if self.pattern_type.is_some_and(|p| p != binding.pattern_type) {
            return false;
        }
        if let Some(p) = &self.principal {
            let target = if p.contains(':') {
                p.clone()
            } else {
                format!("User:{p}")
            };
            if binding.principal != target {
                return false;
            }
        }
        if self.host.as_ref().is_some_and(|h| *h != binding.host) {
            return false;
        }
        if self.operation.is_some_and(|o| o != binding.operation) {
            return false;
        }
        if self
            .permission_type
            .is_some_and(|t| t != binding.permission_type)
        {
            return false;
        }
        true
    }
}
