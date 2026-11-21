//! `ferrika health`: the Overview's health checks for scripts and CI, and
//! with `--watch` a small alerting loop (`--webhook` posts what changed).

use std::collections::BTreeMap;
use std::time::Duration;

use clap::{Args, ValueEnum};
use colored::Colorize;

use crate::cli::{output, GlobalOpts, OutputFormat};
use crate::error::{KafkaError, KafkaResult};
use crate::kafka::admin::KafkaAdminClient;
use crate::model::cluster::ClusterSnapshot;
use crate::ops::health::{health_checks, HealthIssue, Severity};

/// `Debug` is written by hand: an incoming-webhook URL carries its secret in
/// the path.
#[derive(Args, Clone)]
pub struct HealthArgs {
    /// Exit with an error when a finding is at least this severe
    #[arg(long, value_enum, default_value_t = FailOn::Critical)]
    pub fail_on: FailOn,
    /// Check again every SECONDS and report only findings that appear or
    /// clear. Runs until stopped; an unreachable cluster is a finding too.
    #[arg(long, value_name = "SECONDS", value_parser = clap::value_parser!(u64).range(5..))]
    pub watch: Option<u64>,
    /// POST findings as JSON with a `text` field (Slack, Microsoft Teams and
    /// Mattermost incoming webhooks). With --watch, only changes are sent.
    #[arg(long, value_name = "URL")]
    pub webhook: Option<String>,
    /// Lowest severity to report with --watch or --webhook
    #[arg(long, value_enum, default_value_t = MinSeverity::Warning)]
    pub min_severity: MinSeverity,
}

impl std::fmt::Debug for HealthArgs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HealthArgs")
            .field("fail_on", &self.fail_on)
            .field("watch", &self.watch)
            .field("webhook", &self.webhook.as_ref().map(|_| "<redacted>"))
            .field("min_severity", &self.min_severity)
            .finish()
    }
}

#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum MinSeverity {
    Critical,
    Warning,
    Info,
}

impl MinSeverity {
    pub(crate) fn admits(self, severity: Severity) -> bool {
        let limit = match self {
            MinSeverity::Critical => Severity::Critical,
            MinSeverity::Warning => Severity::Warning,
            MinSeverity::Info => Severity::Info,
        };
        severity <= limit
    }
}

#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailOn {
    Critical,
    Warning,
    Info,
    /// Never fail; only report.
    Never,
}

impl FailOn {
    /// How many of `severities` are at or above this level.
    pub(crate) fn failing(self, severities: impl Iterator<Item = Severity>) -> usize {
        let threshold = match self {
            FailOn::Critical => Severity::Critical,
            FailOn::Warning => Severity::Warning,
            FailOn::Info => Severity::Info,
            FailOn::Never => return 0,
        };
        severities.filter(|s| *s <= threshold).count()
    }
}

pub async fn run(args: HealthArgs, global: &GlobalOpts) -> KafkaResult<()> {
    if let Some(seconds) = args.watch {
        return watch(&args, global, Duration::from_secs(seconds)).await;
    }
    let issues = collect(global).await?;
    if global.format == OutputFormat::Json {
        output::print_json(&issues)?;
    } else if issues.is_empty() {
        println!("{} No problems found.", "✔".green().bold());
    } else {
        for issue in &issues {
            println!("{} {}", severity_tag(issue.severity), issue.message);
        }
    }
    if let Some(url) = &args.webhook {
        let shown: Vec<HealthIssue> = issues
            .iter()
            .filter(|i| args.min_severity.admits(i.severity))
            .cloned()
            .collect();
        if !shown.is_empty() {
            post_webhook(url, &cluster_name(global), &shown, &[]).await?;
        }
    }

    let failing = args.fail_on.failing(issues.iter().map(|i| i.severity));
    if failing > 0 {
        return Err(KafkaError::Check(format!(
            "{failing} finding(s) at or above the --fail-on level"
        )));
    }
    Ok(())
}

pub(crate) fn severity_tag(severity: Severity) -> colored::ColoredString {
    match severity {
        Severity::Critical => "CRITICAL".red().bold(),
        Severity::Warning => "WARNING ".yellow().bold(),
        Severity::Info => "INFO    ".bright_black(),
    }
}

fn cluster_name(global: &GlobalOpts) -> String {
    global
        .resolved_profile_name()
        .unwrap_or_else(|| global.resolved_bootstrap_servers())
}

/// Identifies a finding across polls. Counts in the message (lag, records
/// behind) change every poll, so standalone numbers are ignored. Digits that
/// are part of a name (`orders-1`) and node ids (`node 3`) are kept.
fn finding_key(issue: &HealthIssue) -> String {
    let name_char = |c: char| c.is_alphanumeric() || matches!(c, '-' | '_' | '.');
    let chars: Vec<char> = issue.message.chars().collect();
    let mut key = format!("{:?}|", issue.severity);
    let mut i = 0;
    while i < chars.len() {
        let start = i;
        while i < chars.len() && name_char(chars[i]) {
            i += 1;
        }
        if start == i {
            key.push(chars[i]);
            i += 1;
            continue;
        }
        let word: String = chars[start..i].iter().collect();
        let count = word.chars().all(|c| c.is_ascii_digit() || c == '.') && !key.ends_with("node ");
        key.push_str(if count { "#" } else { &word });
    }
    key
}

/// What appeared and what cleared between two polls.
fn diff_findings(
    before: &BTreeMap<String, HealthIssue>,
    now: &BTreeMap<String, HealthIssue>,
) -> (Vec<HealthIssue>, Vec<HealthIssue>) {
    let appeared = now
        .iter()
        .filter(|(k, _)| !before.contains_key(*k))
        .map(|(_, i)| i.clone())
        .collect();
    let cleared = before
        .iter()
        .filter(|(k, _)| !now.contains_key(*k))
        .map(|(_, i)| i.clone())
        .collect();
    (appeared, cleared)
}

/// One key for every connection failure, whatever its error text.
const UNREACHABLE_KEY: &str = "unreachable";

async fn watch(args: &HealthArgs, global: &GlobalOpts, every: Duration) -> KafkaResult<()> {
    let cluster = cluster_name(global);
    let mut before: BTreeMap<String, HealthIssue> = BTreeMap::new();
    println!(
        "Watching {} every {}s (Ctrl+C to stop)…",
        cluster.cyan(),
        every.as_secs()
    );
    loop {
        let now: BTreeMap<String, HealthIssue> = match collect(global).await {
            Ok(issues) => issues
                .into_iter()
                .filter(|i| args.min_severity.admits(i.severity))
                .map(|i| (finding_key(&i), i))
                .collect(),
            // The previous findings are unknown now, not cleared.
            Err(e) => {
                let mut now = before.clone();
                now.insert(
                    UNREACHABLE_KEY.to_string(),
                    HealthIssue {
                        severity: Severity::Critical,
                        message: format!("Cannot check the cluster: {e}"),
                        target: None,
                    },
                );
                now
            }
        };
        let (appeared, cleared) = diff_findings(&before, &now);
        let stamp = jiff::ZonedDateTime::now().strftime("%Y-%m-%d %H:%M:%S");
        for issue in &appeared {
            println!("{stamp} {} {}", severity_tag(issue.severity), issue.message);
        }
        for issue in &cleared {
            println!("{stamp} {} {}", "CLEARED ".green().bold(), issue.message);
        }
        if let Some(url) = &args.webhook {
            if !appeared.is_empty() || !cleared.is_empty() {
                if let Err(e) = post_webhook(url, &cluster, &appeared, &cleared).await {
                    eprintln!("{stamp} {} {e}", "Webhook failed:".yellow());
                }
            }
        }
        before = now;
        tokio::time::sleep(every).await;
    }
}

/// The webhook body: a readable `text` plus the findings as data.
fn webhook_body(cluster: &str, appeared: &[HealthIssue], cleared: &[HealthIssue]) -> String {
    let mut lines = vec![format!(
        "Ferrika health — {cluster}: {} new, {} cleared",
        appeared.len(),
        cleared.len()
    )];
    for issue in appeared {
        let tag = match issue.severity {
            Severity::Critical => "CRITICAL",
            Severity::Warning => "WARNING",
            Severity::Info => "INFO",
        };
        lines.push(format!("• {tag}: {}", issue.message));
    }
    for issue in cleared {
        lines.push(format!("• CLEARED: {}", issue.message));
    }
    serde_json::json!({
        "text": lines.join("\n"),
        "cluster": cluster,
        "new": appeared,
        "cleared": cleared,
    })
    .to_string()
}

/// Puts a `/` between host and query (`https://host?token=x`), so the query
/// is not read as part of the host.
fn with_path(url: &str) -> String {
    let start = url.find("://").map_or(0, |i| i + 3);
    let rest = &url[start..];
    match (rest.find('?'), rest.find('/')) {
        (Some(q), slash) if slash.is_none_or(|s| s > q) => {
            format!("{}/{}", &url[..start + q], &url[start + q..])
        }
        _ => url.to_string(),
    }
}

async fn post_webhook(
    url: &str,
    cluster: &str,
    appeared: &[HealthIssue],
    cleared: &[HealthIssue],
) -> KafkaResult<()> {
    let url = with_path(url);
    let endpoint = crate::kafka::http::Endpoint::parse(&url, 80)?;
    let path = if url
        .trim_end_matches('/')
        .split("://")
        .nth(1)
        .is_some_and(|r| r.contains('/'))
    {
        ""
    } else {
        "/"
    };
    let body = webhook_body(cluster, appeared, cleared);
    let response = endpoint
        .send("POST", path, "application/json", Some(&body))
        .await?;
    if !response.is_success() {
        return Err(KafkaError::Io(format!(
            "Webhook returned {}: {}",
            response.status,
            response.error_message()
        )));
    }
    Ok(())
}

/// Runs the health checks against the cluster.
pub(crate) async fn collect(global: &GlobalOpts) -> KafkaResult<Vec<HealthIssue>> {
    Ok(health_checks(&snapshot(global).await?))
}

/// Reads what the health checks need from the cluster; see
/// [`crate::ops::health::snapshot`].
pub(crate) async fn snapshot(global: &GlobalOpts) -> KafkaResult<ClusterSnapshot> {
    let config = global.to_kafka_config();
    let admin = KafkaAdminClient::new(&config)?;
    crate::ops::health::snapshot(
        &admin,
        &config,
        &global.resolved_profile_name().unwrap_or_default(),
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_output_redacts_the_webhook_url() {
        let cli = <crate::cli::app::Cli as clap::Parser>::try_parse_from([
            "ferrika",
            "health",
            "--webhook",
            "https://hooks.slack.com/services/T0/B0/xoxsecret",
        ])
        .unwrap();
        let shown = format!("{cli:?}");
        assert!(shown.contains("<redacted>"), "{shown}");
        assert!(!shown.contains("xoxsecret"), "{shown}");
    }

    fn issue(severity: Severity, message: &str) -> HealthIssue {
        HealthIssue {
            severity,
            message: message.to_string(),
            target: None,
        }
    }

    #[test]
    fn changing_counts_do_not_make_a_new_finding() {
        let a = issue(Severity::Warning, "orders: no members, lag 120");
        let b = issue(Severity::Warning, "orders: no members, lag 4500");
        assert_eq!(finding_key(&a), finding_key(&b));
        // Names and node ids with digits stay apart.
        assert_ne!(
            finding_key(&issue(
                Severity::Critical,
                "orders-1: 2 partition(s) without a leader"
            )),
            finding_key(&issue(
                Severity::Critical,
                "orders-2: 2 partition(s) without a leader"
            ))
        );
        assert_ne!(
            finding_key(&issue(Severity::Critical, "Broker offline: node 2")),
            finding_key(&issue(Severity::Critical, "Broker offline: node 3"))
        );
        assert_eq!(
            finding_key(&issue(
                Severity::Warning,
                "Metadata quorum voters lagging: node 3 (1200 behind)"
            )),
            finding_key(&issue(
                Severity::Warning,
                "Metadata quorum voters lagging: node 3 (5000 behind)"
            ))
        );
        assert_ne!(
            finding_key(&a),
            finding_key(&issue(Severity::Critical, "orders: no members, lag 120"))
        );

        let before = BTreeMap::from([(finding_key(&a), a)]);
        let offline = issue(Severity::Critical, "Broker offline: node 2");
        let now = BTreeMap::from([
            (finding_key(&b), b),
            (finding_key(&offline), offline.clone()),
        ]);
        let (appeared, cleared) = diff_findings(&before, &now);
        assert_eq!(appeared, vec![offline]);
        assert!(cleared.is_empty());
        let (appeared, cleared) = diff_findings(&now, &BTreeMap::new());
        assert!(appeared.is_empty());
        assert_eq!(cleared.len(), 2);
    }

    #[test]
    fn webhook_query_without_path_gets_one() {
        assert_eq!(with_path("https://h.io?t=1"), "https://h.io/?t=1");
        assert_eq!(with_path("https://h.io/a?t=1"), "https://h.io/a?t=1");
        assert_eq!(with_path("http://h:80"), "http://h:80");
    }

    #[test]
    fn webhook_body_has_slack_text_and_data() {
        let body = webhook_body(
            "prod",
            &[issue(Severity::Critical, "Broker offline: node 2")],
            &[issue(Severity::Warning, "orders: no members, lag 5")],
        );
        let json: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(
            json["text"],
            "Ferrika health — prod: 1 new, 1 cleared\n• CRITICAL: Broker offline: node 2\n• CLEARED: orders: no members, lag 5"
        );
        assert_eq!(json["new"][0]["severity"], "Critical");
    }
}
