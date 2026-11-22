//! Policies for tests of the masking and export paths.

use crate::policy::{EffectivePolicy, PolicyStatus};
use ferrika_core::model::policy::{CopyDownRule, Environment, ExportRule, MaskingRule};

/// No policy installed.
pub fn none() -> EffectivePolicy {
    EffectivePolicy::absent(std::path::PathBuf::new())
}

/// A verified policy whose only rule is `masking: enforced`.
pub fn masking() -> EffectivePolicy {
    let mut policy = none();
    policy.status = PolicyStatus::Verified {
        issuer: "보안팀".into(),
        key_id: "test".into(),
    };
    policy.rules.masking = MaskingRule::Enforced;
    policy
}

/// A verified policy whose only rule is `export: forbidden`.
pub fn export_forbidden() -> EffectivePolicy {
    with_export(ExportRule::Forbidden)
}

/// A verified policy whose only rule is `export: audited`.
pub fn export_audited() -> EffectivePolicy {
    with_export(ExportRule::Audited)
}

/// `export: forbidden` for the clusters matching `export_clusters` only.
pub fn export_forbidden_for_clusters(clusters: &[&str]) -> EffectivePolicy {
    let mut policy = export_forbidden();
    policy.rules.export_clusters = clusters.iter().map(|c| c.to_string()).collect();
    policy
}

/// A verified policy with three environments and `copy_down`:
/// `prod` (tier 3: profiles `prod-*`, brokers `*.prod.corp`, forbids
/// `topics.delete`), `staging` (tier 2: brokers `*.staging.corp`,
/// read-only) and `dev` (tier 1: profiles `dev*`, brokers `localhost`); no
/// default environment.
pub fn environments(copy_down: CopyDownRule) -> EffectivePolicy {
    let mut policy = masking();
    policy.rules.masking = MaskingRule::Optional;
    let env = |tier, profiles: &[&str], brokers: &[&str]| Environment {
        tier,
        profiles: profiles.iter().map(|s| s.to_string()).collect(),
        brokers: brokers.iter().map(|s| s.to_string()).collect(),
        ..Environment::default()
    };
    policy.rules.environments.insert(
        "prod".into(),
        Environment {
            forbid: vec!["topics.delete".into()],
            ..env(3, &["prod-*"], &["*.prod.corp"])
        },
    );
    policy.rules.environments.insert(
        "staging".into(),
        Environment {
            read_only: true,
            ..env(2, &[], &["*.staging.corp"])
        },
    );
    policy
        .rules
        .environments
        .insert("dev".into(), env(1, &["dev*"], &["localhost"]));
    policy.rules.copy_down = copy_down;
    policy
}

fn with_export(rule: ExportRule) -> EffectivePolicy {
    let mut policy = masking();
    policy.rules.masking = MaskingRule::Optional;
    policy.rules.export = rule;
    policy
}

/// An audit log in memory that records every entry.
#[derive(Default)]
pub struct MemoryLog {
    pub entries: std::cell::RefCell<Vec<crate::audit::AuditEntry>>,
}

impl crate::policy::AuditSink for MemoryLog {
    fn record(&self, entry: &crate::audit::AuditEntry) -> std::io::Result<()> {
        self.entries.borrow_mut().push(entry.clone());
        Ok(())
    }

    fn probe(&self) -> std::io::Result<()> {
        Ok(())
    }
}

/// A record field with a Korean mobile number and an email address.
pub const PII: &str = "연락처 010-1234-5678, kim@example.com";

/// Whether `text` still shows either value of [`PII`].
pub fn leaks(text: &str) -> bool {
    text.contains("010-1234-5678") || text.contains("kim@example.com")
}
