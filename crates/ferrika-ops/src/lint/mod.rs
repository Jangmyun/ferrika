//! `ferrika lint`: risky or inconsistent topic, broker and consumer group
//! configs, each with a fixed rule id, a severity and a suggested fix.
//!
//! [`lint`] is pure: the cluster is read once into a [`LintInput`]
//! (`collect`), and fixes are applied by `apply` through the existing admin
//! and wire calls, each behind the company policy's write gate. Runtime
//! state (offline brokers, missing ISR, stuck groups) stays with `health`.

pub mod apply;
pub mod collect;
pub mod fix;
pub mod rules;

use std::collections::{BTreeMap, HashMap};

use crate::health::{HealthTarget, Severity};
use ferrika_core::kafka::admin::ConfigEntryView;
use ferrika_core::model::cluster::ClusterSnapshot;
use ferrika_governance::policy::glob_match;

pub use fix::{Fix, FixClass};
pub use rules::{RuleScope, RuleSpec, RULES};

/// The kind of cluster the findings are graded for: the same finding is
/// often more severe on production.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Env {
    Prod,
    Dev,
}

/// What the rules look at, read from the cluster once.
#[derive(Debug, Clone)]
pub struct LintInput {
    /// Brokers, topics (with partitions) and groups (with committed offsets).
    pub data: ClusterSnapshot,
    /// Every config of each topic whose configs could be read.
    pub topic_configs: HashMap<String, Vec<ConfigEntryView>>,
    /// Every config of each broker whose configs could be read.
    pub broker_configs: BTreeMap<i32, Vec<ConfigEntryView>>,
    /// Resources that could not be read (e.g. no `DESCRIBE_CONFIGS`), such
    /// as `topic orders` or `broker 2`. Their rules stay silent.
    pub skipped: Vec<String>,
    /// Whether the groups' committed offsets were read, so a group without
    /// any is known to have committed nothing (G002).
    pub group_offsets: bool,
}

/// Which findings to report.
#[derive(Debug, Clone)]
pub struct LintOptions {
    pub env: Env,
    /// Only these rule ids; every rule when empty.
    pub rules: Vec<String>,
    /// Rule ids to leave out, everywhere or (with a glob) for matching
    /// targets only (`T001:orders-*`).
    pub skip: Vec<(String, Option<String>)>,
    /// Only topics matching one of these globs; every topic when empty.
    /// Broker and group findings are not narrowed.
    pub topics: Vec<String>,
}

impl Default for LintOptions {
    fn default() -> Self {
        Self {
            env: Env::Prod,
            rules: Vec::new(),
            skip: Vec::new(),
            topics: Vec::new(),
        }
    }
}

impl LintOptions {
    /// Whether topic `name` is within `--topic`.
    pub fn covers_topic(&self, name: &str) -> bool {
        self.topics.is_empty() || self.topics.iter().any(|g| glob_match(g, name))
    }

    fn reports(&self, finding: &LintFinding) -> bool {
        if !self.rules.is_empty()
            && !self
                .rules
                .iter()
                .any(|r| r.eq_ignore_ascii_case(finding.rule))
        {
            return false;
        }
        let name = target_name(&finding.target);
        !self.skip.iter().any(|(rule, glob)| {
            rule.eq_ignore_ascii_case(finding.rule)
                && glob.as_ref().is_none_or(|g| glob_match(g, &name))
        })
    }
}

/// One problem found.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct LintFinding {
    /// Rule id, e.g. `T002`.
    pub rule: &'static str,
    /// Rule name, e.g. `min-isr-unreachable`.
    pub name: &'static str,
    pub severity: Severity,
    pub target: HealthTarget,
    pub message: String,
    /// The config values the finding is about, e.g. `min.insync.replicas=3`.
    pub current: Vec<(String, String)>,
    pub fix: Fix,
    /// How the fix is applied; `Manual` for a fix that only explains.
    pub fix_class: FixClass,
    /// The same fix as a command to run by hand, when there is one.
    pub command: Option<String>,
}

impl LintFinding {
    /// Whether `lint --fix` can apply this fix (auto or guided, not manual
    /// or none).
    pub fn applicable(&self) -> bool {
        self.fix.is_executable()
    }

    /// The change the fix makes, with the current values:
    /// `min.insync.replicas 3 -> 2`, `replication 1 -> 3`.
    pub fn change_summary(&self) -> String {
        use ferrika_core::kafka::wire::ConfigChange;
        let current = |key: &str| {
            self.current
                .iter()
                .find(|(k, _)| k == key)
                .map(|(_, v)| v.as_str())
                .unwrap_or("?")
        };
        match &self.fix {
            Fix::TopicConfig { changes, .. } | Fix::BrokerConfig { changes, .. } => {
                let scope = match &self.fix {
                    Fix::BrokerConfig { broker: None, .. } => "cluster default ",
                    _ => "",
                };
                let parts: Vec<String> = changes
                    .iter()
                    .map(|c| match c {
                        ConfigChange::Set(k, v) => format!("{scope}{k} {} -> {v}", current(k)),
                        ConfigChange::Delete(k) => {
                            format!("{scope}{k} {} -> (default)", current(k))
                        }
                    })
                    .collect();
                parts.join(", ")
            }
            Fix::Replication { target, .. } => {
                format!("replication {} -> {target}", current("replication.factor"))
            }
            Fix::ElectLeaders { partitions } => format!(
                "elect the preferred leader of {} partition(s)",
                partitions.len()
            ),
            Fix::AddPartitions { topic, total } => format!("raise {topic} to {total} partitions"),
            Fix::DeleteGroup { group } => format!("delete group {group}"),
            Fix::Manual { steps } => steps.join("; "),
            Fix::None { reason } => format!("not now: {reason}"),
        }
    }
}

/// The findings for `input`, most severe first (then by rule and target).
pub fn lint(input: &LintInput, opts: &LintOptions) -> Vec<LintFinding> {
    let mut findings: Vec<LintFinding> = rules::check_all(input, opts)
        .into_iter()
        .filter(|f| opts.reports(f))
        .collect();
    findings.sort_by(|a, b| {
        (a.severity, a.rule, target_name(&a.target)).cmp(&(
            b.severity,
            b.rule,
            target_name(&b.target),
        ))
    });
    findings
}

/// The name `--skip ID:GLOB` matches: a topic or group name, a broker id,
/// or `cluster`.
pub fn target_name(target: &HealthTarget) -> String {
    match target {
        HealthTarget::Topic(name) | HealthTarget::Group(name) => name.clone(),
        HealthTarget::Broker(id) => id.to_string(),
        HealthTarget::Brokers => "cluster".to_string(),
    }
}

/// `target` as the table shows it: `topic orders`, `group etl`, `broker 2`.
pub fn target_label(target: &HealthTarget) -> String {
    match target {
        HealthTarget::Topic(name) => format!("topic {name}"),
        HealthTarget::Group(name) => format!("group {name}"),
        HealthTarget::Broker(id) => format!("broker {id}"),
        HealthTarget::Brokers => "cluster".to_string(),
    }
}

/// Parses `--skip ID[:GLOB]`.
pub fn parse_skip(text: &str) -> (String, Option<String>) {
    match text.split_once(':') {
        Some((rule, glob)) if !glob.is_empty() => (rule.trim().to_string(), Some(glob.to_string())),
        Some((rule, _)) => (rule.trim().to_string(), None),
        None => (text.trim().to_string(), None),
    }
}

/// What `--env auto` decides from: the profile and the policy's verdict.
#[derive(Debug, Clone, Default)]
pub struct EnvHints<'a> {
    pub profile: &'a str,
    pub description: Option<&'a str>,
    /// The profile is read-only.
    pub read_only: bool,
    /// The company policy's read-only glob that matches, if any.
    pub policy_read_only: Option<String>,
}

/// `--env auto`: production when the profile is read-only, a company
/// policy read-only glob matches it, or its name or description has the
/// word `prod`, `prd` or `live`. Returns the env and why, for the first
/// line of the output.
pub fn detect_env(hints: &EnvHints) -> (Env, String) {
    if hints.read_only {
        return (
            Env::Prod,
            format!("profile '{}' is read-only", hints.profile),
        );
    }
    if let Some(glob) = &hints.policy_read_only {
        return (
            Env::Prod,
            format!("company policy read-only rule \"{glob}\""),
        );
    }
    let word = |text: &str| {
        text.split(|c: char| !c.is_ascii_alphanumeric()).any(|w| {
            ["prod", "prd", "live", "production"].contains(&w.to_ascii_lowercase().as_str())
        })
    };
    if word(hints.profile) {
        return (Env::Prod, format!("profile '{}' name", hints.profile));
    }
    if hints.description.is_some_and(word) {
        return (
            Env::Prod,
            format!("profile '{}' description", hints.profile),
        );
    }
    (
        Env::Dev,
        "no production signs; use --env prod to grade as production".to_string(),
    )
}

/// `--env auto` for the cluster `context` resolved: its profile, the
/// profile's description and read-only flag, and the company policy.
pub fn auto_env(context: &ferrika_core::kafka::context::ResolvedContext) -> (Env, String) {
    let profile = context.profile_name.clone().unwrap_or_default();
    detect_env(&EnvHints {
        profile: &profile,
        description: context
            .profile
            .as_ref()
            .and_then(|p| p.description.as_deref()),
        read_only: context.config.read_only,
        policy_read_only: ferrika_governance::policy::current()
            .read_only_match(&profile, &context.bootstrap_servers),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_auto_reads_production_signs() {
        let hints = |profile, description, read_only| EnvHints {
            profile,
            description,
            read_only,
            policy_read_only: None,
        };
        assert_eq!(
            detect_env(&hints("payments-prod", None, false)).0,
            Env::Prod
        );
        assert_eq!(detect_env(&hints("PRD_kr", None, false)).0, Env::Prod);
        assert_eq!(
            detect_env(&hints("local", Some("Live cluster"), false)).0,
            Env::Prod
        );
        assert_eq!(detect_env(&hints("dev", None, true)).0, Env::Prod);
        // A word, not a substring.
        assert_eq!(detect_env(&hints("product-dev", None, false)).0, Env::Dev);
        assert_eq!(detect_env(&hints("deliver", None, false)).0, Env::Dev);
        let (env, why) = detect_env(&EnvHints {
            profile: "x",
            policy_read_only: Some("x*".into()),
            ..EnvHints::default()
        });
        assert_eq!(env, Env::Prod);
        assert!(why.contains("x*"), "{why}");
    }

    #[test]
    fn skip_parses_an_optional_glob() {
        assert_eq!(parse_skip("T001"), ("T001".into(), None));
        assert_eq!(
            parse_skip("T001:orders-*"),
            ("T001".into(), Some("orders-*".into()))
        );
        assert_eq!(parse_skip("T001:"), ("T001".into(), None));
    }
}
