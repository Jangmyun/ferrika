//! The lint rule catalog and the rules themselves (pure, no IO).
//!
//! `ferrika lint rules`, the JSON output and the docs all come from
//! [`RULES`]. Thresholds are constants, like `health`'s.

use std::collections::{BTreeMap, HashMap, HashSet};

use crate::health::{HealthTarget, Severity};
use ferrika_core::kafka::admin::{ConfigEntryView, ConfigOrigin};
use ferrika_core::kafka::wire::ConfigChange;
use ferrika_core::model::cluster::TopicSummary;

use super::fix::{Fix, FixClass};
use super::{Env, LintFinding, LintInput, LintOptions};

/// What a rule looks at.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RuleScope {
    Topic,
    Broker,
    Group,
}

/// One rule of the catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub struct RuleSpec {
    pub id: &'static str,
    pub name: &'static str,
    pub scope: RuleScope,
    /// Severity on a production cluster.
    pub severity_prod: Severity,
    /// Severity elsewhere.
    pub severity_other: Severity,
    /// How its fix is applied (a finding may lower it to manual, e.g. a
    /// static broker config).
    pub fix_class: FixClass,
    pub summary: &'static str,
}

impl RuleSpec {
    pub fn severity(&self, env: Env) -> Severity {
        match env {
            Env::Prod => self.severity_prod,
            Env::Dev => self.severity_other,
        }
    }
}

use FixClass::{Auto, Guided, Manual};
use RuleScope::{Broker, Group, Topic};
use Severity::{Critical, Info, Warning};

const fn rule(
    id: &'static str,
    name: &'static str,
    scope: RuleScope,
    severity_prod: Severity,
    severity_other: Severity,
    fix_class: FixClass,
    summary: &'static str,
) -> RuleSpec {
    RuleSpec {
        id,
        name,
        scope,
        severity_prod,
        severity_other,
        fix_class,
        summary,
    }
}

/// Every rule, in catalog order.
pub const RULES: &[RuleSpec] = &[
    rule("T001", "replication-1", Topic, Critical, Info, Guided,
        "Replication factor 1 with two or more brokers online: one broker loss loses data. Fix: raise replication (copies data)."),
    rule("T002", "min-isr-unreachable", Topic, Critical, Critical, Auto,
        "min.insync.replicas is above the replication factor: every acks=all write fails. Fix: min.insync.replicas = max(1, RF-1)."),
    rule("T003", "min-isr-equals-rf", Topic, Warning, Info, Guided,
        "min.insync.replicas equals the replication factor: one broker down stops acks=all writes. Fix: min.insync.replicas = RF-1."),
    rule("T004", "min-isr-weak", Topic, Warning, Info, Guided,
        "Replication 3+ with min.insync.replicas 1: acks=all accepts writes held by one replica. Fix: min.insync.replicas = 2 (acks=all producers may start failing)."),
    rule("T005", "unclean-election-on", Topic, Critical, Warning, Auto,
        "unclean.leader.election.enable=true on the topic: an out-of-sync replica may lead and drop records. Fix: remove the override or set false."),
    rule("T006", "retention-short", Topic, Warning, Info, Manual,
        "Delete policy with retention.ms under an hour: consumers that stop briefly lose records."),
    rule("T007", "retention-infinite", Topic, Info, Info, Manual,
        "Delete policy with no time or size limit: the topic grows forever."),
    rule("T008", "segment-outlives-retention", Topic, Warning, Info, Guided,
        "segment.ms is above retention.ms: the active segment keeps records past their retention. Fix: segment.ms = min(retention.ms, 7 days) (more segments)."),
    rule("T009", "segment-bytes-extreme", Topic, Warning, Info, Auto,
        "segment.bytes override under 16 MiB (many files and handles) or over 1 GiB. Fix: remove the override."),
    rule("T010", "compaction-lag-inverted", Topic, Warning, Warning, Auto,
        "Compacted topic with min.compaction.lag.ms above max.compaction.lag.ms. Fix: remove both overrides."),
    rule("T011", "tombstone-retention-short", Topic, Warning, Info, Auto,
        "Compacted topic with delete.retention.ms under an hour: consumers may miss deletes. Fix: delete.retention.ms = 1 day."),
    rule("T012", "dirty-ratio-extreme", Topic, Info, Info, Auto,
        "Compacted topic with min.cleanable.dirty.ratio under 0.01 or over 0.9. Fix: remove the override."),
    rule("T013", "dlq-retention-shorter", Topic, Warning, Info, Auto,
        "A dead letter topic keeps records for less time than its source topic. Fix: the source's retention.ms."),
    rule("T014", "preferred-leader-lost", Topic, Warning, Info, Auto,
        "Partitions not led by their preferred replica although it is in sync (warning from 10% of a topic's partitions). Fix: elect those partitions' preferred leaders (clients reconnect)."),
    rule("B001", "unclean-election-default", Broker, Critical, Warning, Auto,
        "Brokers allow unclean leader election by default. Fix: the dynamic default false, or server.properties when static."),
    rule("B002", "internal-topic-rf-1", Broker, Critical, Warning, Guided,
        "__consumer_offsets or __transaction_state has replication 1 with three or more brokers. Fix: replication 3 (copies data)."),
    rule("B003", "default-rf-1", Broker, Warning, Info, Manual,
        "default.replication.factor=1 with three or more brokers: new topics get no redundancy."),
    rule("B004", "default-min-isr-vs-rf", Broker, Warning, Info, Guided,
        "The default min.insync.replicas is not below default.replication.factor. Fix: the cluster default min.insync.replicas = default RF - 1 (also lowers it for existing topics without an override)."),
    rule("B005", "auto-create-on", Broker, Warning, Info, Manual,
        "auto.create.topics.enable=true: a typo in a client creates a topic."),
    rule("B006", "broker-config-drift", Broker, Warning, Info, Guided,
        "A key that should match differs between brokers. Fix: per-broker dynamic values follow the majority; others by hand."),
    rule("B007", "rack-partial", Broker, Warning, Info, Manual,
        "Only some brokers set broker.rack, so replicas are not spread by rack."),
    rule("G001", "committed-before-log-start", Group, Warning, Info, Manual,
        "A group without members committed offsets before the log start: on restart auto.offset.reset decides (lost or repeated records)."),
    rule("G002", "orphan-group", Group, Info, Info, Guided,
        "A group without members whose topics are gone or that committed nothing. Fix: delete the group (cannot be undone)."),
    rule("G003", "members-exceed-partitions", Group, Info, Info, Guided,
        "More members than partitions: some members idle. Fix: add partitions (keyed records map to other partitions)."),
];

/// Looks up a rule by id (case-insensitive).
pub fn spec(id: &str) -> Option<&'static RuleSpec> {
    RULES.iter().find(|r| r.id.eq_ignore_ascii_case(id))
}

/// Delete-policy retention below this is short (T006); so is a tombstone
/// retention (T011).
pub const SHORT_RETENTION_MS: i64 = 60 * 60 * 1000;
/// The longest segment.ms T008 suggests.
pub const SEGMENT_MS_CAP: i64 = 7 * 24 * 60 * 60 * 1000;
/// T009 bounds for a segment.bytes override.
pub const SEGMENT_BYTES_MIN: i64 = 16 * 1024 * 1024;
pub const SEGMENT_BYTES_MAX: i64 = 1024 * 1024 * 1024;
/// T011's suggested delete.retention.ms (Kafka's default, one day).
pub const TOMBSTONE_RETENTION_MS: i64 = 24 * 60 * 60 * 1000;
/// T012 bounds for min.cleanable.dirty.ratio.
pub const DIRTY_RATIO_MIN: f64 = 0.01;
pub const DIRTY_RATIO_MAX: f64 = 0.9;
/// Share of a topic's partitions off their preferred leader at which T014
/// warns.
pub const PREFERRED_LEADER_WARN_PCT: f64 = 10.0;
/// Keys B006 compares across brokers.
pub const DRIFT_KEYS: &[&str] = &[
    "default.replication.factor",
    "min.insync.replicas",
    "unclean.leader.election.enable",
    "log.retention.ms",
    "log.retention.hours",
    "message.max.bytes",
    "num.partitions",
    "auto.create.topics.enable",
];

/// Internal topics B002 checks.
const INTERNAL_TOPICS: &[&str] = &["__consumer_offsets", "__transaction_state"];

const UNCLEAN: &str = "unclean.leader.election.enable";
const MIN_ISR: &str = "min.insync.replicas";

/// A resource's configs by key.
struct Configs<'a>(HashMap<&'a str, &'a ConfigEntryView>);

impl<'a> Configs<'a> {
    fn new(entries: &'a [ConfigEntryView]) -> Self {
        Self(entries.iter().map(|e| (e.name.as_str(), e)).collect())
    }

    fn entry(&self, key: &str) -> Option<&'a ConfigEntryView> {
        self.0.get(key).copied()
    }

    fn str(&self, key: &str) -> Option<&'a str> {
        self.entry(key)
            .filter(|e| !e.sensitive)
            .map(|e| e.value.as_str())
    }

    fn i64(&self, key: &str) -> Option<i64> {
        self.str(key)?.trim().parse().ok()
    }

    fn f64(&self, key: &str) -> Option<f64> {
        self.str(key)?.trim().parse().ok()
    }

    fn is_true(&self, key: &str) -> bool {
        self.str(key)
            .is_some_and(|v| v.eq_ignore_ascii_case("true"))
    }

    fn is_override(&self, key: &str) -> bool {
        self.entry(key)
            .is_some_and(|e| e.source == ConfigOrigin::TopicOverride)
    }

    fn policy(&self) -> (bool, bool) {
        let policy = self.str("cleanup.policy").unwrap_or("delete");
        (policy.contains("delete"), policy.contains("compact"))
    }
}

/// Builds findings with the rule's severity for the chosen env.
struct Findings<'a> {
    env: Env,
    bootstrap: &'a str,
    out: Vec<LintFinding>,
}

impl Findings<'_> {
    fn push(
        &mut self,
        id: &str,
        target: HealthTarget,
        message: String,
        current: Vec<(&str, String)>,
        fix: Fix,
    ) {
        let Some(spec) = spec(id) else {
            return;
        };
        self.push_with(spec, spec.severity(self.env), target, message, current, fix);
    }

    fn push_with(
        &mut self,
        spec: &'static RuleSpec,
        severity: Severity,
        target: HealthTarget,
        message: String,
        current: Vec<(&str, String)>,
        fix: Fix,
    ) {
        // Managed services refuse broker config changes; their console does it.
        let fix = match fix {
            Fix::BrokerConfig { changes, .. } if is_managed(self.bootstrap) => Fix::Manual {
                steps: vec![format!(
                    "This looks like a managed Kafka service; change {} in the provider's console or API",
                    ferrika_core::kafka::wire::change_keys(&changes)
                )],
            },
            fix => fix,
        };
        let fix_class = if matches!(fix, Fix::Manual { .. }) {
            Manual
        } else {
            spec.fix_class
        };
        self.out.push(LintFinding {
            rule: spec.id,
            name: spec.name,
            severity,
            target,
            message,
            current: current
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
            command: fix.cli_command(self.bootstrap),
            fix,
            fix_class,
        });
    }
}

/// Whether `bootstrap` points at a managed service (Confluent Cloud, Amazon
/// MSK), which does not take broker config changes from clients.
pub fn is_managed(bootstrap: &str) -> bool {
    bootstrap.split(',').any(|address| {
        let host = address
            .trim()
            .rsplit_once(':')
            .map_or(address.trim(), |(host, _)| host)
            .to_ascii_lowercase();
        host.ends_with(".confluent.cloud") || host.ends_with(".amazonaws.com")
    })
}

fn is_internal(topic: &TopicSummary) -> bool {
    topic.is_internal || ferrika_core::kafka::internal_topic::is_internal_topic(&topic.name)
}

fn set(key: &str, value: impl ToString) -> ConfigChange {
    ConfigChange::Set(key.to_string(), value.to_string())
}

fn topic_set(topic: &str, changes: Vec<ConfigChange>) -> Fix {
    Fix::TopicConfig {
        topic: topic.to_string(),
        changes,
    }
}

/// Every rule's findings, unfiltered and unsorted.
pub fn check_all(input: &LintInput, opts: &LintOptions) -> Vec<LintFinding> {
    let mut findings = Findings {
        env: opts.env,
        bootstrap: &input.data.bootstrap_server,
        out: Vec::new(),
    };
    let online = input.data.brokers.iter().filter(|b| b.is_online).count();
    for topic in input
        .data
        .topics
        .iter()
        .filter(|t| !is_internal(t) && opts.covers_topic(&t.name))
    {
        topic_metadata_rules(&mut findings, topic, online);
        if let Some(entries) = input.topic_configs.get(&topic.name) {
            topic_config_rules(&mut findings, input, topic, &Configs::new(entries));
        }
    }
    broker_rules(&mut findings, input, opts, online);
    group_rules(&mut findings, input);
    findings.out
}

/// T001 and T014: from metadata alone, so they run without configs.
fn topic_metadata_rules(f: &mut Findings, topic: &TopicSummary, online: usize) {
    let name = &topic.name;
    if topic.replication_factor == 1 && online >= 2 {
        let target = online.min(3);
        f.push(
            "T001",
            HealthTarget::Topic(name.clone()),
            format!("{name}: replication factor 1 with {online} brokers online"),
            vec![("replication.factor", "1".to_string())],
            Fix::Replication {
                topic: name.clone(),
                target,
            },
        );
    }

    let lost: Vec<(String, i32)> = topic
        .partitions
        .iter()
        .filter(|p| {
            p.replicas.first().is_some_and(|preferred| {
                p.leader >= 0 && p.leader != *preferred && p.isr.contains(preferred)
            })
        })
        .map(|p| (name.clone(), p.partition_id))
        .collect();
    if !lost.is_empty() && !topic.partitions.is_empty() {
        let pct = lost.len() as f64 * 100.0 / topic.partitions.len() as f64;
        let Some(spec) = spec("T014") else {
            return;
        };
        let severity = if pct >= PREFERRED_LEADER_WARN_PCT {
            spec.severity(f.env)
        } else {
            Info
        };
        f.push_with(
            spec,
            severity,
            HealthTarget::Topic(name.clone()),
            format!(
                "{name}: {} of {} partition(s) not led by their preferred replica",
                lost.len(),
                topic.partitions.len()
            ),
            vec![],
            Fix::ElectLeaders { partitions: lost },
        );
    }
}

fn topic_config_rules(f: &mut Findings, input: &LintInput, topic: &TopicSummary, c: &Configs) {
    let name = &topic.name;
    let target = || HealthTarget::Topic(name.clone());
    let rf = topic.replication_factor as i64;
    let (delete, compact) = c.policy();

    if let Some(min_isr) = c.i64(MIN_ISR) {
        let current = vec![
            (MIN_ISR, min_isr.to_string()),
            ("replication.factor", rf.to_string()),
        ];
        if min_isr > rf {
            f.push(
                "T002",
                target(),
                format!("{name}: {MIN_ISR} {min_isr} is above the replication factor {rf}; acks=all writes always fail"),
                current,
                topic_set(name, vec![set(MIN_ISR, (rf - 1).max(1))]),
            );
        } else if rf >= 2 && min_isr == rf {
            f.push(
                "T003",
                target(),
                format!("{name}: {MIN_ISR} equals the replication factor {rf}; one broker down stops acks=all writes"),
                current,
                topic_set(name, vec![set(MIN_ISR, rf - 1)]),
            );
        } else if rf >= 3 && min_isr == 1 {
            let short = topic.partitions.iter().filter(|p| p.isr.len() < 2).count();
            let fix = if short == 0 {
                topic_set(name, vec![set(MIN_ISR, 2)])
            } else {
                Fix::None {
                    reason: format!(
                        "{short} partition(s) have fewer than 2 in-sync replicas; acks=all writes would fail now"
                    ),
                }
            };
            f.push(
                "T004",
                target(),
                format!("{name}: replication {rf} but {MIN_ISR} 1; acks=all accepts writes held by one replica"),
                current,
                fix,
            );
        }
    }

    if c.is_override(UNCLEAN) && c.is_true(UNCLEAN) {
        // Removing the override is enough when every broker defaults to false.
        let brokers_false = !input.broker_configs.is_empty()
            && input.broker_configs.values().all(|b| {
                Configs::new(b)
                    .str(UNCLEAN)
                    .is_some_and(|v| v.eq_ignore_ascii_case("false"))
            });
        let change = if brokers_false {
            ConfigChange::Delete(UNCLEAN.to_string())
        } else {
            set(UNCLEAN, "false")
        };
        f.push(
            "T005",
            target(),
            format!("{name}: unclean leader election is on; an out-of-sync replica may lead and drop records"),
            vec![(UNCLEAN, "true".to_string())],
            topic_set(name, vec![change]),
        );
    }

    let retention = c.i64("retention.ms");
    if delete {
        if let Some(ms) = retention.filter(|ms| (0..SHORT_RETENTION_MS).contains(ms)) {
            f.push(
                "T006",
                target(),
                format!("{name}: records are kept only {} (retention.ms {ms})", duration(ms)),
                vec![("retention.ms", ms.to_string())],
                Fix::Manual {
                    steps: vec![format!(
                        "If consumers may stop for longer, raise it, e.g. ferrika topics config {} -s retention.ms=86400000",
                        super::fix::shell_arg(name)
                    )],
                },
            );
        }
        if retention == Some(-1) && c.i64("retention.bytes") == Some(-1) {
            f.push(
                "T007",
                target(),
                format!("{name}: delete policy without a time or size limit"),
                vec![
                    ("retention.ms", "-1".to_string()),
                    ("retention.bytes", "-1".to_string()),
                ],
                Fix::Manual {
                    steps: vec![format!(
                        "Set a limit if the topic should not grow forever, e.g. ferrika topics config {} -s retention.ms=604800000",
                        super::fix::shell_arg(name)
                    )],
                },
            );
        }
        if let (Some(ret), Some(segment)) = (retention.filter(|ms| *ms > 0), c.i64("segment.ms")) {
            if segment > ret {
                let suggested = ret.min(SEGMENT_MS_CAP);
                f.push(
                    "T008",
                    target(),
                    format!(
                        "{name}: segment.ms {} is above retention.ms {}; records outlive their retention in the active segment",
                        duration(segment),
                        duration(ret)
                    ),
                    vec![
                        ("segment.ms", segment.to_string()),
                        ("retention.ms", ret.to_string()),
                    ],
                    topic_set(name, vec![set("segment.ms", suggested)]),
                );
            }
        }
    }

    if c.is_override("segment.bytes") {
        if let Some(bytes) = c
            .i64("segment.bytes")
            .filter(|b| !(SEGMENT_BYTES_MIN..=SEGMENT_BYTES_MAX).contains(b))
        {
            f.push(
                "T009",
                target(),
                format!("{name}: segment.bytes {bytes} is outside 16 MiB..1 GiB"),
                vec![("segment.bytes", bytes.to_string())],
                topic_set(name, vec![ConfigChange::Delete("segment.bytes".into())]),
            );
        }
    }

    if compact {
        let (min_lag, max_lag) = (
            c.i64("min.compaction.lag.ms"),
            c.i64("max.compaction.lag.ms"),
        );
        if let (Some(min), Some(max)) = (min_lag, max_lag) {
            if min > max {
                let changes: Vec<ConfigChange> = ["min.compaction.lag.ms", "max.compaction.lag.ms"]
                    .into_iter()
                    .filter(|k| c.is_override(k))
                    .map(|k| ConfigChange::Delete(k.to_string()))
                    .collect();
                let fix = if changes.is_empty() {
                    Fix::None {
                        reason:
                            "neither value is a topic override; the broker defaults are inverted"
                                .into(),
                    }
                } else {
                    topic_set(name, changes)
                };
                f.push(
                    "T010",
                    target(),
                    format!("{name}: min.compaction.lag.ms is above max.compaction.lag.ms"),
                    vec![
                        ("min.compaction.lag.ms", min.to_string()),
                        ("max.compaction.lag.ms", max.to_string()),
                    ],
                    fix,
                );
            }
        }
        if let Some(ms) = c
            .i64("delete.retention.ms")
            .filter(|ms| (0..SHORT_RETENTION_MS).contains(ms))
        {
            f.push(
                "T011",
                target(),
                format!(
                    "{name}: tombstones are kept only {}; consumers may miss deletes",
                    duration(ms)
                ),
                vec![("delete.retention.ms", ms.to_string())],
                topic_set(
                    name,
                    vec![set("delete.retention.ms", TOMBSTONE_RETENTION_MS)],
                ),
            );
        }
        if let Some(ratio) = c
            .f64("min.cleanable.dirty.ratio")
            .filter(|r| !(DIRTY_RATIO_MIN..=DIRTY_RATIO_MAX).contains(r))
        {
            let fix = if c.is_override("min.cleanable.dirty.ratio") {
                topic_set(
                    name,
                    vec![ConfigChange::Delete("min.cleanable.dirty.ratio".into())],
                )
            } else {
                Fix::None {
                    reason: "the value is the broker default, not a topic override".into(),
                }
            };
            f.push(
                "T012",
                target(),
                format!("{name}: min.cleanable.dirty.ratio {ratio} is outside 0.01..0.9"),
                vec![("min.cleanable.dirty.ratio", ratio.to_string())],
                fix,
            );
        }
    }

    if let Some(source) = crate::dlq::infer_original_topic_from_dlq_name(name) {
        let source_retention = input
            .topic_configs
            .get(&source)
            .and_then(|e| Configs::new(e).i64("retention.ms"));
        if let (Some(dlq), Some(original)) = (retention, source_retention) {
            if shorter(dlq, original) {
                f.push(
                    "T013",
                    target(),
                    format!(
                        "{name}: keeps failed records {} but its source {source} keeps records {}",
                        duration(dlq),
                        duration(original)
                    ),
                    vec![("retention.ms", dlq.to_string())],
                    topic_set(name, vec![set("retention.ms", original)]),
                );
            }
        }
    }
}

/// Whether retention `a` is shorter than `b` (-1 is forever).
fn shorter(a: i64, b: i64) -> bool {
    match (a < 0, b < 0) {
        (true, _) => false,
        (false, true) => true,
        (false, false) => a < b,
    }
}

fn broker_rules(f: &mut Findings, input: &LintInput, opts: &LintOptions, online: usize) {
    for topic in input
        .data
        .topics
        .iter()
        .filter(|t| INTERNAL_TOPICS.contains(&t.name.as_str()) && opts.covers_topic(&t.name))
    {
        if topic.replication_factor == 1 && online >= 3 {
            f.push(
                "B002",
                HealthTarget::Topic(topic.name.clone()),
                format!(
                    "{}: replication factor 1 with {online} brokers online",
                    topic.name
                ),
                vec![("replication.factor", "1".to_string())],
                Fix::Replication {
                    topic: topic.name.clone(),
                    target: 3,
                },
            );
        }
    }

    let brokers: BTreeMap<i32, Configs> = input
        .broker_configs
        .iter()
        .map(|(id, entries)| (*id, Configs::new(entries)))
        .collect();
    if brokers.is_empty() {
        return;
    }

    // B001: one finding for the cluster default, one per broker otherwise.
    let mut default_on = false;
    for (id, c) in &brokers {
        if !c.is_true(UNCLEAN) {
            continue;
        }
        let source = c.entry(UNCLEAN).map(|e| e.source);
        let fix = match source {
            Some(ConfigOrigin::BrokerDynamicDefault) => {
                default_on = true;
                continue;
            }
            Some(ConfigOrigin::BrokerDynamic) => Fix::BrokerConfig {
                broker: Some(*id),
                changes: vec![set(UNCLEAN, "false")],
            },
            _ => Fix::Manual {
                steps: vec![format!(
                    "In broker {id}'s server.properties: {UNCLEAN}=false, then restart it"
                )],
            },
        };
        f.push(
            "B001",
            HealthTarget::Broker(*id),
            format!("Broker {id}: unclean leader election is on"),
            vec![(UNCLEAN, "true".to_string())],
            fix,
        );
    }
    if default_on {
        f.push(
            "B001",
            HealthTarget::Brokers,
            "Unclean leader election is on as the cluster default".to_string(),
            vec![(UNCLEAN, "true".to_string())],
            Fix::BrokerConfig {
                broker: None,
                changes: vec![set(UNCLEAN, "false")],
            },
        );
    }

    let first = brokers.values().next();
    if let Some(c) = first {
        let default_rf = c.i64("default.replication.factor");
        if default_rf == Some(1) && online >= 3 {
            f.push(
                "B003",
                HealthTarget::Brokers,
                format!("default.replication.factor is 1 with {online} brokers online; new topics get no redundancy"),
                vec![("default.replication.factor", "1".to_string())],
                Fix::Manual {
                    steps: vec![
                        "In each broker's server.properties: default.replication.factor=3, then restart the brokers one at a time".to_string(),
                    ],
                },
            );
        }
        if let (Some(rf), Some(min_isr)) = (default_rf, c.i64(MIN_ISR)) {
            if rf > 1 && min_isr >= rf {
                f.push(
                    "B004",
                    HealthTarget::Brokers,
                    format!("The default {MIN_ISR} {min_isr} is not below default.replication.factor {rf}; new topics stop acks=all writes when one broker is down"),
                    vec![
                        (MIN_ISR, min_isr.to_string()),
                        ("default.replication.factor", rf.to_string()),
                    ],
                    Fix::BrokerConfig {
                        broker: None,
                        changes: vec![set(MIN_ISR, rf - 1)],
                    },
                );
            }
        }
        if c.is_true("auto.create.topics.enable") {
            f.push(
                "B005",
                HealthTarget::Brokers,
                "auto.create.topics.enable is on; a client typo creates a topic".to_string(),
                vec![("auto.create.topics.enable", "true".to_string())],
                Fix::Manual {
                    steps: vec![
                        "In each broker's server.properties: auto.create.topics.enable=false, then restart the brokers one at a time".to_string(),
                    ],
                },
            );
        }
    }

    if brokers.len() >= 2 {
        for key in DRIFT_KEYS {
            drift(f, &brokers, key);
        }
    }

    let racks: Vec<(i32, bool)> = brokers
        .iter()
        .map(|(id, c)| {
            (
                *id,
                c.str("broker.rack").is_some_and(|r| !r.trim().is_empty()),
            )
        })
        .collect();
    let with_rack = racks.iter().filter(|(_, has)| *has).count();
    if with_rack > 0 && with_rack < racks.len() {
        let missing: Vec<String> = racks
            .iter()
            .filter(|(_, has)| !has)
            .map(|(id, _)| id.to_string())
            .collect();
        f.push(
            "B007",
            HealthTarget::Brokers,
            format!("broker.rack is set on {with_rack} of {} brokers (missing on {})", racks.len(), missing.join(", ")),
            vec![],
            Fix::Manual {
                steps: vec![format!(
                    "Set broker.rack in server.properties of broker(s) {}, then restart them one at a time",
                    missing.join(", ")
                )],
            },
        );
    }
}

/// A broker config value for a message; Kafka reports an unset one (such
/// as log.retention.ms) as empty.
fn shown(value: &str) -> &str {
    if value.is_empty() {
        "not set"
    } else {
        value
    }
}

/// B006 for one key: brokers whose value differs from the majority.
fn drift(f: &mut Findings, brokers: &BTreeMap<i32, Configs>, key: &str) {
    let values: Vec<(i32, &str, Option<ConfigOrigin>)> = brokers
        .iter()
        .filter_map(|(id, c)| Some((*id, c.str(key)?, c.entry(key).map(|e| e.source))))
        .collect();
    let distinct: HashSet<&str> = values.iter().map(|(_, v, _)| *v).collect();
    if values.len() < 2 || distinct.len() < 2 {
        return;
    }
    let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
    for (_, v, _) in &values {
        *counts.entry(v).or_default() += 1;
    }
    let top = counts.values().copied().max().unwrap_or(0);
    let leaders: Vec<&str> = counts
        .iter()
        .filter(|(_, n)| **n == top)
        .map(|(v, _)| *v)
        .collect();
    // Without a strict majority there is no value to follow.
    let majority = (leaders.len() == 1 && top * 2 > values.len()).then(|| leaders[0]);
    for (id, value, source) in &values {
        if Some(*value) == majority {
            continue;
        }
        let fix = match (majority, source) {
            // Most brokers leave it unset (Kafka reports no value, as for
            // log.retention.ms): remove this broker's override. Setting ""
            // is refused.
            (Some(""), Some(ConfigOrigin::BrokerDynamic)) => Fix::BrokerConfig {
                broker: Some(*id),
                changes: vec![ConfigChange::Delete(key.to_string())],
            },
            (Some(""), _) => Fix::Manual {
                steps: vec![format!(
                    "In broker {id}'s server.properties: remove {key}, then restart it"
                )],
            },
            (Some(m), Some(ConfigOrigin::BrokerDynamic)) => Fix::BrokerConfig {
                broker: Some(*id),
                changes: vec![set(key, m)],
            },
            (Some(m), _) => Fix::Manual {
                steps: vec![format!(
                    "In broker {id}'s server.properties: {key}={m}, then restart it"
                )],
            },
            (None, _) => Fix::Manual {
                steps: vec![format!(
                    "Decide on one {key} for every broker; the values differ without a majority"
                )],
            },
        };
        let others = majority.map_or_else(
            || "the others".to_string(),
            |m| format!("most brokers ({})", shown(m)),
        );
        f.push(
            "B006",
            HealthTarget::Broker(*id),
            format!("Broker {id}: {key} is {}, unlike {others}", shown(value)),
            vec![(key, value.to_string())],
            fix,
        );
    }
}

fn group_rules(f: &mut Findings, input: &LintInput) {
    let topics: HashMap<&str, &TopicSummary> = input
        .data
        .topics
        .iter()
        .map(|t| (t.name.as_str(), t))
        .collect();
    for g in &input.data.consumers {
        let target = || HealthTarget::Group(g.group_id.clone());
        if g.active_members == 0 {
            // G001: committed offsets the log no longer holds.
            let mut behind: BTreeMap<&str, usize> = BTreeMap::new();
            for p in &g.partitions {
                let Some(committed) = p.committed.filter(|c| *c >= 0) else {
                    continue;
                };
                let start = topics
                    .get(p.topic.as_str())
                    .and_then(|t| {
                        t.partitions
                            .iter()
                            .find(|tp| tp.partition_id == p.partition)
                    })
                    .map(|tp| tp.low_watermark);
                if start.is_some_and(|s| committed < s) {
                    *behind.entry(p.topic.as_str()).or_default() += 1;
                }
            }
            if !behind.is_empty() {
                let list: Vec<String> = behind.iter().map(|(t, n)| format!("{t} ({n})")).collect();
                f.push(
                    "G001",
                    target(),
                    format!(
                        "{}: committed offsets before the log start on {}; on restart auto.offset.reset decides",
                        g.group_id,
                        list.join(", ")
                    ),
                    vec![],
                    Fix::Manual {
                        steps: behind
                            .keys()
                            .map(|t| {
                                format!(
                                    "ferrika offset reset {} -t {} --to-earliest --execute",
                                    super::fix::shell_arg(&g.group_id),
                                    super::fix::shell_arg(t)
                                )
                            })
                            .collect(),
                    },
                );
            }

            // G002: nothing left to consume.
            let committed_topics: HashSet<&str> = g
                .target_topics
                .iter()
                .map(String::as_str)
                .chain(g.partitions.iter().map(|p| p.topic.as_str()))
                .collect();
            let known = input.group_offsets || !committed_topics.is_empty();
            if known && committed_topics.iter().all(|t| !topics.contains_key(t)) {
                let why = if committed_topics.is_empty() {
                    "no committed offsets".to_string()
                } else {
                    let mut gone: Vec<&str> = committed_topics.into_iter().collect();
                    gone.sort();
                    format!("its topics are gone ({})", gone.join(", "))
                };
                f.push(
                    "G002",
                    target(),
                    format!("{}: no members and {why}", g.group_id),
                    vec![],
                    Fix::DeleteGroup {
                        group: g.group_id.clone(),
                    },
                );
            }
        } else if !g.target_topics.is_empty() {
            // G003: more members than partitions to share.
            let known: Option<Vec<&TopicSummary>> = g
                .target_topics
                .iter()
                .map(|t| topics.get(t.as_str()).copied())
                .collect();
            let Some(known) = known else {
                continue;
            };
            let partitions: usize = known.iter().map(|t| t.partition_count).sum();
            if partitions > 0 && g.active_members > partitions {
                let fix = match known.as_slice() {
                    [only] => Fix::AddPartitions {
                        topic: only.name.clone(),
                        total: g.active_members,
                    },
                    _ => Fix::Manual {
                        steps: vec![format!(
                            "Add partitions to {} or run fewer members",
                            g.target_topics.join(", ")
                        )],
                    },
                };
                f.push(
                    "G003",
                    target(),
                    format!(
                        "{}: {} members share {partitions} partition(s); {} idle",
                        g.group_id,
                        g.active_members,
                        g.active_members - partitions
                    ),
                    vec![],
                    fix,
                );
            }
        }
    }
}

/// `3600000` ms as `1h`, `90000` as `1m 30s`.
fn duration(ms: i64) -> String {
    if ms < 0 {
        return "forever".to_string();
    }
    ferrika_core::model::format::format_duration_ms(ms, " ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lint::lint;

    #[test]
    fn fixes_that_weaken_durability_are_never_automatic() {
        // Lowering min.insync.replicas lets acks=all accept writes held by
        // fewer replicas: only when the rule is named, confirmed one by one.
        for id in ["T003", "B004"] {
            let spec = RULES.iter().find(|r| r.id == id).unwrap();
            assert_eq!(spec.fix_class, FixClass::Guided, "{id}");
        }
    }
    use ferrika_core::model::cluster::{
        BrokerInfo, ClusterSnapshot, ConsumerGroupSummary, ConsumerState, GroupPartitionLag,
        PartitionDetail,
    };

    fn entry(name: &str, value: &str, source: ConfigOrigin) -> ConfigEntryView {
        ConfigEntryView {
            name: name.into(),
            value: value.into(),
            source,
            read_only: false,
            sensitive: false,
        }
    }

    fn broker(id: i32) -> BrokerInfo {
        BrokerInfo {
            id,
            host: format!("b{id}"),
            port: 9092,
            is_controller: id == 1,
            partition_leaders: 0,
            replicas_hosted: 0,
            is_online: true,
            log_dir_bytes: None,
            log_dirs: vec![],
        }
    }

    fn topic(name: &str, rf: usize, partitions: usize) -> TopicSummary {
        let replicas: Vec<i32> = (1..=rf as i32).collect();
        TopicSummary {
            name: name.into(),
            partition_count: partitions,
            replication_factor: rf,
            total_messages: 0,
            cleanup_policy: "delete".into(),
            is_internal: false,
            size_bytes: None,
            msg_rate: None,
            partitions: (0..partitions as i32)
                .map(|p| PartitionDetail {
                    partition_id: p,
                    leader: replicas[0],
                    replicas: replicas.clone(),
                    isr: replicas.clone(),
                    high_watermark: 100,
                    low_watermark: 0,
                })
                .collect(),
        }
    }

    #[test]
    fn underscore_topics_are_internal() {
        assert!(is_internal(&topic("_schemas", 3, 1)));
        assert!(is_internal(&topic("__consumer_offsets", 3, 1)));
        assert!(!is_internal(&topic("orders", 3, 1)));
    }

    /// Three online brokers, no topics, groups or configs.
    fn input(brokers: usize) -> LintInput {
        let mut data = ClusterSnapshot::empty("test", "b1:9092");
        data.brokers = (1..=brokers as i32).map(broker).collect();
        LintInput {
            data,
            topic_configs: HashMap::new(),
            broker_configs: BTreeMap::new(),
            skipped: vec![],
            group_offsets: true,
        }
    }

    fn with_topic(input: &mut LintInput, t: TopicSummary, configs: &[(&str, &str, ConfigOrigin)]) {
        input.topic_configs.insert(
            t.name.clone(),
            configs.iter().map(|(k, v, s)| entry(k, v, *s)).collect(),
        );
        input.data.topics.push(t);
    }

    fn opts(env: Env) -> LintOptions {
        LintOptions {
            env,
            ..LintOptions::default()
        }
    }

    fn rules_of(findings: &[LintFinding]) -> Vec<&'static str> {
        findings.iter().map(|f| f.rule).collect()
    }

    fn only<'a>(findings: &'a [LintFinding], rule: &str) -> &'a LintFinding {
        let hits: Vec<&LintFinding> = findings.iter().filter(|f| f.rule == rule).collect();
        assert_eq!(hits.len(), 1, "{rule}: {findings:#?}");
        hits[0]
    }

    use ConfigOrigin::{
        BrokerDynamic, BrokerDynamicDefault, BrokerStatic, Default as Dflt, TopicOverride,
    };

    #[test]
    fn catalog_ids_are_unique_and_known() {
        let ids: HashSet<&str> = RULES.iter().map(|r| r.id).collect();
        assert_eq!(ids.len(), RULES.len());
        assert!(spec("t002").is_some());
        assert!(spec("G004").is_none(), "G004 is a later step");
    }

    #[test]
    fn replication_one_needs_two_online_brokers() {
        let mut one = input(1);
        with_topic(&mut one, topic("orders", 1, 3), &[]);
        assert!(lint(&one, &opts(Env::Prod)).is_empty());

        let mut three = input(3);
        with_topic(&mut three, topic("orders", 1, 3), &[]);
        let prod = lint(&three, &opts(Env::Prod));
        let t001 = only(&prod, "T001");
        assert_eq!(t001.severity, Critical);
        assert_eq!(t001.fix_class, Guided);
        assert_eq!(
            t001.fix,
            Fix::Replication {
                topic: "orders".into(),
                target: 3
            }
        );
        assert_eq!(only(&lint(&three, &opts(Env::Dev)), "T001").severity, Info);

        let mut two = input(2);
        with_topic(&mut two, topic("orders", 1, 3), &[]);
        assert_eq!(
            only(&lint(&two, &opts(Env::Prod)), "T001").fix,
            Fix::Replication {
                topic: "orders".into(),
                target: 2
            }
        );
    }

    #[test]
    fn min_isr_rules_suggest_rf_minus_one() {
        let mut i = input(4);
        with_topic(&mut i, topic("a", 3, 1), &[(MIN_ISR, "4", TopicOverride)]);
        with_topic(&mut i, topic("b", 3, 1), &[(MIN_ISR, "3", TopicOverride)]);
        with_topic(&mut i, topic("c", 3, 1), &[(MIN_ISR, "2", Dflt)]);
        with_topic(&mut i, topic("d", 1, 1), &[(MIN_ISR, "1", Dflt)]);
        let findings = lint(&i, &opts(Env::Prod));
        let t002 = only(&findings, "T002");
        assert_eq!(t002.target, HealthTarget::Topic("a".into()));
        assert_eq!(t002.severity, Critical);
        assert_eq!(t002.fix, topic_set("a", vec![set(MIN_ISR, 2)]));
        let t003 = only(&findings, "T003");
        assert_eq!(t003.target, HealthTarget::Topic("b".into()));
        assert_eq!(t003.fix, topic_set("b", vec![set(MIN_ISR, 2)]));
        // RF 1 = min.isr 1 is not T003 (RF < 2), and c is fine.
        assert!(!findings
            .iter()
            .any(|f| f.target == HealthTarget::Topic("c".into())));
        assert!(!findings
            .iter()
            .any(|f| f.target == HealthTarget::Topic("d".into()) && f.rule != "T001"));

        // RF 1 with min.isr 2: the fix stays at 1.
        let mut j = input(1);
        with_topic(&mut j, topic("e", 1, 1), &[(MIN_ISR, "2", Dflt)]);
        assert_eq!(
            only(&lint(&j, &opts(Env::Dev)), "T002").fix,
            topic_set("e", vec![set(MIN_ISR, 1)])
        );
    }

    #[test]
    fn weak_min_isr_waits_for_in_sync_replicas() {
        let mut i = input(3);
        with_topic(&mut i, topic("ok", 3, 2), &[(MIN_ISR, "1", Dflt)]);
        let mut short = topic("short", 3, 2);
        short.partitions[1].isr = vec![1];
        with_topic(&mut i, short, &[(MIN_ISR, "1", Dflt)]);
        let findings = lint(&i, &opts(Env::Prod));
        let fixes: Vec<&Fix> = findings
            .iter()
            .filter(|f| f.rule == "T004")
            .map(|f| &f.fix)
            .collect();
        assert_eq!(fixes.len(), 2);
        assert!(fixes.contains(&&topic_set("ok", vec![set(MIN_ISR, 2)])));
        assert!(fixes
            .iter()
            .any(|f| matches!(f, Fix::None { reason } if reason.contains("1 partition(s)"))));
        assert!(findings
            .iter()
            .all(|f| f.rule != "T004" || f.fix_class == Guided));
    }

    #[test]
    fn unclean_override_is_removed_when_brokers_default_to_false() {
        let mut i = input(3);
        with_topic(
            &mut i,
            topic("t", 3, 1),
            &[(UNCLEAN, "true", TopicOverride)],
        );
        // A broker-level true is B001's, not T005's.
        with_topic(
            &mut i,
            topic("u", 3, 1),
            &[(UNCLEAN, "true", BrokerDynamicDefault)],
        );
        let unknown = lint(&i, &opts(Env::Prod));
        assert_eq!(
            only(&unknown, "T005").fix,
            topic_set("t", vec![set(UNCLEAN, "false")])
        );

        for id in 1..=3 {
            i.broker_configs
                .insert(id, vec![entry(UNCLEAN, "false", Dflt)]);
        }
        let known = lint(&i, &opts(Env::Dev));
        let t005 = only(&known, "T005");
        assert_eq!(t005.severity, Warning);
        assert_eq!(
            t005.fix,
            topic_set("t", vec![ConfigChange::Delete(UNCLEAN.into())])
        );
    }

    #[test]
    fn retention_and_segment_rules() {
        let mut i = input(3);
        with_topic(
            &mut i,
            topic("short", 3, 1),
            &[("retention.ms", "60000", TopicOverride)],
        );
        with_topic(
            &mut i,
            topic("edge", 3, 1),
            &[("retention.ms", "3600000", TopicOverride)],
        );
        with_topic(
            &mut i,
            topic("forever", 3, 1),
            &[
                ("retention.ms", "-1", TopicOverride),
                ("retention.bytes", "-1", Dflt),
            ],
        );
        with_topic(
            &mut i,
            topic("seg", 3, 1),
            &[
                ("retention.ms", "86400000", TopicOverride),
                ("segment.ms", "604800000", Dflt),
            ],
        );
        with_topic(
            &mut i,
            topic("tiny", 3, 1),
            &[("segment.bytes", "1048576", TopicOverride)],
        );
        with_topic(
            &mut i,
            topic("tiny-default", 3, 1),
            &[("segment.bytes", "1048576", BrokerStatic)],
        );
        let findings = lint(&i, &opts(Env::Prod));
        let t006 = only(&findings, "T006");
        assert_eq!(t006.target, HealthTarget::Topic("short".into()));
        assert_eq!(t006.fix_class, Manual);
        assert!(t006.command.is_none());
        assert_eq!(
            only(&findings, "T007").target,
            HealthTarget::Topic("forever".into())
        );
        assert_eq!(
            only(&findings, "T008").fix,
            topic_set("seg", vec![set("segment.ms", 86_400_000)])
        );
        let t009 = only(&findings, "T009");
        assert_eq!(t009.target, HealthTarget::Topic("tiny".into()));
        assert_eq!(
            t009.fix,
            topic_set("tiny", vec![ConfigChange::Delete("segment.bytes".into())])
        );
    }

    #[test]
    fn compaction_rules_look_at_compacted_topics_only() {
        let mut i = input(3);
        let compacted = [
            ("cleanup.policy", "compact", TopicOverride),
            ("min.compaction.lag.ms", "7200000", TopicOverride),
            ("max.compaction.lag.ms", "3600000", Dflt),
            ("delete.retention.ms", "1000", TopicOverride),
            ("min.cleanable.dirty.ratio", "0.95", TopicOverride),
        ];
        with_topic(&mut i, topic("c", 3, 1), &compacted);
        let mut deleting = compacted;
        deleting[0] = ("cleanup.policy", "delete", Dflt);
        with_topic(&mut i, topic("d", 3, 1), &deleting);
        let findings = lint(&i, &opts(Env::Prod));
        assert_eq!(
            only(&findings, "T010").fix,
            topic_set(
                "c",
                vec![ConfigChange::Delete("min.compaction.lag.ms".into())]
            )
        );
        assert_eq!(
            only(&findings, "T011").fix,
            topic_set(
                "c",
                vec![set("delete.retention.ms", TOMBSTONE_RETENTION_MS)]
            )
        );
        assert_eq!(only(&findings, "T012").severity, Info);
    }

    #[test]
    fn dlq_retention_follows_its_source() {
        let mut i = input(3);
        with_topic(
            &mut i,
            topic("orders", 3, 1),
            &[("retention.ms", "-1", TopicOverride)],
        );
        with_topic(
            &mut i,
            topic("orders-dlq", 3, 1),
            &[("retention.ms", "86400000", Dflt)],
        );
        with_topic(
            &mut i,
            topic("pay", 3, 1),
            &[("retention.ms", "1000000", Dflt)],
        );
        with_topic(
            &mut i,
            topic("pay.dlq", 3, 1),
            &[("retention.ms", "-1", TopicOverride)],
        );
        let findings = lint(&i, &opts(Env::Prod));
        let t013 = only(&findings, "T013");
        assert_eq!(t013.target, HealthTarget::Topic("orders-dlq".into()));
        assert_eq!(
            t013.fix,
            topic_set("orders-dlq", vec![set("retention.ms", -1)])
        );
    }

    #[test]
    fn preferred_leaders_warn_from_ten_percent() {
        let mut i = input(3);
        let mut t = topic("orders", 3, 20);
        t.partitions[4].leader = 2;
        with_topic(&mut i, t.clone(), &[]);
        let few = lint(&i, &opts(Env::Prod));
        let t014 = only(&few, "T014");
        assert_eq!(t014.severity, Info, "1 of 20 is under 10%");
        assert_eq!(
            t014.fix,
            Fix::ElectLeaders {
                partitions: vec![("orders".into(), 4)]
            }
        );
        // The preferred replica out of sync: electing it would fail.
        t.partitions[7].leader = 3;
        t.partitions[7].isr = vec![2, 3];
        t.partitions[9].leader = 2;
        i.data.topics = vec![t];
        let more = lint(&i, &opts(Env::Prod));
        let t014 = only(&more, "T014");
        assert_eq!(t014.severity, Warning);
        assert_eq!(
            t014.fix,
            Fix::ElectLeaders {
                partitions: vec![("orders".into(), 4), ("orders".into(), 9)]
            }
        );
    }

    #[test]
    fn internal_topics_are_skipped_except_by_b002() {
        let mut i = input(3);
        with_topic(
            &mut i,
            topic("__consumer_offsets", 1, 50),
            &[(MIN_ISR, "3", Dflt)],
        );
        let findings = lint(&i, &opts(Env::Prod));
        assert_eq!(rules_of(&findings), vec!["B002"]);
        assert_eq!(
            findings[0].fix,
            Fix::Replication {
                topic: "__consumer_offsets".into(),
                target: 3
            }
        );
    }

    #[test]
    fn topics_without_configs_only_get_metadata_rules() {
        let mut i = input(3);
        i.data.topics.push(topic("secret", 1, 1));
        i.skipped.push("topic secret".into());
        assert_eq!(rules_of(&lint(&i, &opts(Env::Prod))), vec!["T001"]);
    }

    #[test]
    fn unclean_default_is_fixed_where_it_is_set() {
        let mut i = input(3);
        i.broker_configs
            .insert(1, vec![entry(UNCLEAN, "true", BrokerDynamicDefault)]);
        i.broker_configs
            .insert(2, vec![entry(UNCLEAN, "true", BrokerDynamic)]);
        i.broker_configs
            .insert(3, vec![entry(UNCLEAN, "true", BrokerStatic)]);
        let findings: Vec<LintFinding> = lint(&i, &opts(Env::Prod))
            .into_iter()
            .filter(|f| f.rule == "B001")
            .collect();
        assert_eq!(findings.len(), 3, "{findings:#?}");
        let by_target = |t: HealthTarget| findings.iter().find(|f| f.target == t).unwrap();
        assert_eq!(
            by_target(HealthTarget::Brokers).fix,
            Fix::BrokerConfig {
                broker: None,
                changes: vec![set(UNCLEAN, "false")]
            }
        );
        assert_eq!(by_target(HealthTarget::Brokers).fix_class, Auto);
        assert_eq!(
            by_target(HealthTarget::Broker(2)).fix,
            Fix::BrokerConfig {
                broker: Some(2),
                changes: vec![set(UNCLEAN, "false")]
            }
        );
        let static_one = by_target(HealthTarget::Broker(3));
        assert_eq!(static_one.fix_class, Manual);
        assert!(static_one.command.is_none());
    }

    #[test]
    fn managed_services_get_manual_broker_fixes() {
        assert!(is_managed(
            "b-1.msk.abc.kafka.ap-northeast-2.amazonaws.com:9098"
        ));
        assert!(is_managed("x:1,pkc-123.us-east-1.aws.confluent.cloud:9092"));
        assert!(!is_managed("kafka-1.internal:9092"));
        let mut i = input(3);
        i.data.bootstrap_server = "pkc-1.confluent.cloud:9092".into();
        i.broker_configs
            .insert(1, vec![entry(UNCLEAN, "true", BrokerDynamicDefault)]);
        let b001 = lint(&i, &opts(Env::Prod));
        assert_eq!(only(&b001, "B001").fix_class, Manual);
    }

    #[test]
    fn broker_defaults_auto_create_and_racks() {
        let mut i = input(3);
        for id in 1..=3 {
            let mut entries = vec![
                entry("default.replication.factor", "1", BrokerStatic),
                entry(MIN_ISR, "1", Dflt),
                entry("auto.create.topics.enable", "true", Dflt),
            ];
            if id != 2 {
                entries.push(entry("broker.rack", "az-a", BrokerStatic));
            }
            i.broker_configs.insert(id, entries);
        }
        let findings = lint(&i, &opts(Env::Prod));
        assert_eq!(only(&findings, "B003").fix_class, Manual);
        assert_eq!(only(&findings, "B005").fix_class, Manual);
        assert!(only(&findings, "B007").message.contains("missing on 2"));
        assert!(findings.iter().all(|f| f.rule != "B004"));

        for entries in i.broker_configs.values_mut() {
            entries[0] = entry("default.replication.factor", "3", BrokerStatic);
            entries[1] = entry(MIN_ISR, "3", BrokerDynamicDefault);
        }
        let findings = lint(&i, &opts(Env::Prod));
        assert_eq!(
            only(&findings, "B004").fix,
            Fix::BrokerConfig {
                broker: None,
                changes: vec![set(MIN_ISR, 2)]
            }
        );
        assert!(findings.iter().all(|f| f.rule != "B003"));
    }

    #[test]
    fn drift_follows_the_majority_for_dynamic_values() {
        let mut i = input(3);
        i.broker_configs
            .insert(1, vec![entry("message.max.bytes", "1048588", Dflt)]);
        i.broker_configs
            .insert(2, vec![entry("message.max.bytes", "1048588", Dflt)]);
        i.broker_configs.insert(
            3,
            vec![entry("message.max.bytes", "5000000", BrokerDynamic)],
        );
        let findings = lint(&i, &opts(Env::Prod));
        let b006 = only(&findings, "B006");
        assert_eq!(b006.target, HealthTarget::Broker(3));
        assert_eq!(
            b006.fix,
            Fix::BrokerConfig {
                broker: Some(3),
                changes: vec![set("message.max.bytes", "1048588")]
            }
        );

        i.broker_configs
            .insert(3, vec![entry("message.max.bytes", "5000000", BrokerStatic)]);
        assert_eq!(only(&lint(&i, &opts(Env::Prod)), "B006").fix_class, Manual);

        // Most brokers leave log.retention.ms unset (no value): the fix
        // removes broker 3's override instead of setting it to "".
        let mut unset = input(3);
        for b in 1..=2 {
            unset
                .broker_configs
                .insert(b, vec![entry("log.retention.ms", "", Dflt)]);
        }
        unset.broker_configs.insert(
            3,
            vec![entry("log.retention.ms", "86400000", BrokerDynamic)],
        );
        let findings = lint(&unset, &opts(Env::Prod));
        let b006 = only(&findings, "B006");
        assert_eq!(
            b006.fix,
            Fix::BrokerConfig {
                broker: Some(3),
                changes: vec![ConfigChange::Delete("log.retention.ms".into())]
            }
        );
        assert!(
            b006.message.contains("most brokers (not set)"),
            "{}",
            b006.message
        );

        // Two brokers, two values: no majority, both reported, nothing to apply.
        let mut two = input(2);
        two.broker_configs
            .insert(1, vec![entry("num.partitions", "1", Dflt)]);
        two.broker_configs
            .insert(2, vec![entry("num.partitions", "3", BrokerDynamic)]);
        let findings = lint(&two, &opts(Env::Prod));
        assert_eq!(findings.len(), 2);
        assert!(findings.iter().all(|f| !f.applicable()));
    }

    fn group(id: &str, members: usize, topics: &[&str]) -> ConsumerGroupSummary {
        ConsumerGroupSummary {
            group_id: id.into(),
            state: if members == 0 {
                ConsumerState::Empty
            } else {
                ConsumerState::Stable
            },
            active_members: members,
            target_topics: topics.iter().map(|t| t.to_string()).collect(),
            current_offset: 0,
            end_offset: 0,
            total_lag: 0,
            coordinator_node: "-".into(),
            consume_rate: None,
            partitions: vec![],
        }
    }

    #[test]
    fn group_rules() {
        let mut i = input(3);
        let mut t = topic("orders", 3, 2);
        t.partitions[1].low_watermark = 500;
        i.data.topics.push(t);

        let mut stale = group("stale", 0, &["orders"]);
        stale.partitions = vec![
            GroupPartitionLag {
                topic: "orders".into(),
                partition: 0,
                committed: Some(10),
                end_offset: 100,
                lag: 90,
                owner: None,
                behind_ms: None,
            },
            GroupPartitionLag {
                topic: "orders".into(),
                partition: 1,
                committed: Some(100),
                end_offset: 900,
                lag: 800,
                owner: None,
                behind_ms: None,
            },
        ];
        i.data.consumers = vec![
            stale,
            group("orphan", 0, &["deleted-topic"]),
            group("never", 0, &[]),
            group("crowd", 5, &["orders"]),
            group("fine", 2, &["orders"]),
        ];
        let findings = lint(&i, &opts(Env::Prod));
        let g001 = only(&findings, "G001");
        assert_eq!(g001.target, HealthTarget::Group("stale".into()));
        assert!(g001.message.contains("orders (1)"));
        assert_eq!(
            g001.fix,
            Fix::Manual {
                steps: vec!["ferrika offset reset stale -t orders --to-earliest --execute".into()]
            }
        );
        let orphans: Vec<&HealthTarget> = findings
            .iter()
            .filter(|f| f.rule == "G002")
            .map(|f| &f.target)
            .collect();
        assert_eq!(
            orphans,
            vec![
                &HealthTarget::Group("never".into()),
                &HealthTarget::Group("orphan".into())
            ]
        );
        assert_eq!(
            only(&findings, "G003").fix,
            Fix::AddPartitions {
                topic: "orders".into(),
                total: 5
            }
        );
    }

    #[test]
    fn uncommitted_partition_does_not_change_group_findings() {
        // The group committed partition 0 only; partition 1 holds 100 records.
        let mut i = input(3);
        let mut t = topic("orders", 3, 2);
        t.partitions[0].high_watermark = 100;
        t.partitions[1].high_watermark = 100;
        i.data.topics.push(t);
        let mut idle = group("idle", 0, &[]);
        let committed = HashMap::from([(("orders".to_string(), 0), 40)]);
        idle.partitions = crate::group_lag::group_partition_lags(&committed, &i.data.topics);
        assert_eq!(idle.partitions.len(), i.data.topics[0].partitions.len());
        let p1 = idle.partitions.iter().find(|p| p.partition == 1).unwrap();
        assert_eq!((p1.committed, p1.lag), (None, 100));
        i.data.consumers = vec![idle];
        let findings = lint(&i, &opts(Env::Prod));
        assert!(
            findings.iter().all(|f| !f.rule.starts_with('G')),
            "{findings:?}"
        );
    }

    #[test]
    fn groups_without_read_offsets_are_not_orphans() {
        let mut i = input(3);
        i.group_offsets = false;
        i.data.consumers = vec![group("never", 0, &[]), group("gone", 0, &["x"])];
        let findings = lint(&i, &opts(Env::Prod));
        assert_eq!(
            findings.iter().map(|f| &f.target).collect::<Vec<_>>(),
            vec![&HealthTarget::Group("gone".into())]
        );
    }

    #[test]
    fn rule_filters_and_skips() {
        let mut i = input(3);
        with_topic(&mut i, topic("orders-1", 1, 1), &[(MIN_ISR, "2", Dflt)]);
        with_topic(&mut i, topic("orders-2", 1, 1), &[]);
        with_topic(&mut i, topic("billing", 1, 1), &[]);
        let all = lint(&i, &opts(Env::Prod));
        assert_eq!(all.iter().filter(|f| f.rule == "T001").count(), 3);

        let mut o = opts(Env::Prod);
        o.skip = vec![super::super::parse_skip("t001:orders-*")];
        let skipped = lint(&i, &o);
        assert_eq!(
            skipped.iter().filter(|f| f.rule == "T001").count(),
            1,
            "{skipped:#?}"
        );

        let mut o = opts(Env::Prod);
        o.rules = vec!["T002".into()];
        assert_eq!(rules_of(&lint(&i, &o)), vec!["T002"]);

        let mut o = opts(Env::Prod);
        o.topics = vec!["bill*".into()];
        assert_eq!(rules_of(&lint(&i, &o)), vec!["T001"]);

        // Most severe first.
        assert!(all.windows(2).all(|w| w[0].severity <= w[1].severity));
    }

    #[test]
    fn manual_rules_never_build_an_executable_fix() {
        let mut i = input(3);
        with_topic(
            &mut i,
            topic("short", 3, 1),
            &[("retention.ms", "1000", TopicOverride)],
        );
        for id in 1..=3 {
            i.broker_configs.insert(
                id,
                vec![
                    entry("auto.create.topics.enable", "true", Dflt),
                    entry("default.replication.factor", "1", BrokerStatic),
                ],
            );
        }
        for f in lint(&i, &opts(Env::Prod)) {
            let spec = spec(f.rule).unwrap();
            if spec.fix_class == Manual {
                assert!(!f.applicable(), "{f:?}");
            }
        }
    }

    #[test]
    fn durations_read_well() {
        assert_eq!(duration(3_600_000), "1h");
        assert_eq!(duration(90_000), "1m 30s");
        assert_eq!(duration(500), "500ms");
        assert_eq!(duration(-1), "forever");
    }
}
