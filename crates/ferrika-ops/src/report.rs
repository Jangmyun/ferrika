//! Incident report: one Markdown (or JSON) document for the active cluster,
//! built by `ferrika report` and the Overview's Report button.
//!
//! It combines the health checks, a consumer lag snapshot, a lag trend and
//! the audit log for a time window. It never contains record data: the only
//! free text comes from the audit log (which holds names and outcomes, never
//! payloads), and that text is PII-masked before it is added, because a
//! report is written to be passed around.
//!
//! Ferrika keeps no lag history on disk, so the trend covers what the caller
//! observed: lag sampled while the CLI builds the report (`--samples`), or the
//! GUI's recent polls. A single sample is reported as a snapshot.

use std::collections::BTreeMap;
use std::time::Duration;

use serde::Serialize;

use crate::health::{health_checks, HealthIssue, Severity};
use ferrika_core::codec::pii::{mask_text, MaskStrategy};
use ferrika_core::model::cluster::ClusterSnapshot;
use ferrika_governance::audit::AuditEntry;
use ferrika_governance::policy::EffectivePolicy;

/// Audit entries kept when no window is given (the most recent ones).
pub(crate) const AUDIT_DEFAULT_LIMIT: usize = 100;
/// Audit entries kept inside a window; older ones in it are counted, not listed.
pub(crate) const AUDIT_WINDOW_LIMIT: usize = 500;
/// Audit lines read before filtering to the cluster and the window.
pub const AUDIT_SCAN: usize = 100_000;
/// Groups listed in the lag tables; the rest are counted.
const MAX_GROUP_ROWS: usize = 50;

/// Longest window accepted: 100 years, far past any audit log and far from
/// overflowing the millisecond arithmetic.
const MAX_WINDOW_SECS: u64 = 100 * 365 * 86_400;

/// Start of a window of `secs` seconds ending at `end_ms`, without overflow.
fn window_start_ms(end_ms: i64, secs: u64) -> i64 {
    let ms = i64::try_from(secs).unwrap_or(i64::MAX).saturating_mul(1000);
    end_ms.saturating_sub(ms)
}

/// Parses a window such as `90s`, `30m`, `2h` or `1d`.
pub fn parse_window(text: &str) -> Result<Duration, String> {
    let text = text.trim();
    let split = text
        .find(|c: char| !c.is_ascii_digit())
        .ok_or_else(|| format!("'{text}' needs a unit: s, m, h or d (e.g. 30m)"))?;
    let (number, unit) = text.split_at(split);
    let n: u64 = number
        .parse()
        .map_err(|_| format!("'{text}' is not a duration (e.g. 30m, 2h, 1d)"))?;
    let secs = match unit {
        "s" => n,
        "m" => n.saturating_mul(60),
        "h" => n.saturating_mul(3_600),
        "d" => n.saturating_mul(86_400),
        _ => {
            return Err(format!(
                "unknown unit '{unit}' in '{text}' (use s, m, h or d)"
            ))
        }
    };
    if secs == 0 {
        return Err("the window must be longer than zero".to_string());
    }
    if secs > MAX_WINDOW_SECS {
        return Err(format!(
            "'{text}' is too long; the window is at most 100 years"
        ));
    }
    Ok(Duration::from_secs(secs))
}

/// `30m`, `2h`, `1d 6h`: a window for display.
pub(crate) fn format_window(window: Duration) -> String {
    let parts = ferrika_core::model::format::duration_parts(
        i64::try_from(window.as_secs()).unwrap_or(i64::MAX),
        " ",
    );
    if parts.is_empty() {
        "0s".to_string()
    } else {
        parts
    }
}

/// Bootstrap servers without credentials: `user:pass@host` loses the user
/// part and `scheme://user:pass@host` its password.
pub(crate) fn mask_bootstrap(bootstrap: &str) -> String {
    bootstrap
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|server| {
            if server.contains("://") {
                ferrika_core::kafka::profile::mask_url_password(server, "****")
            } else {
                match server.rsplit_once('@') {
                    Some((_, host)) => format!("****@{host}"),
                    None => server.to_string(),
                }
            }
        })
        .collect::<Vec<_>>()
        .join(",")
}

/// The company policy as the report header shows it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PolicySummary {
    /// "no policy", "company policy by …", or the strictest-mode label.
    pub label: String,
    pub active: bool,
    pub invalid: bool,
    pub masking_enforced: bool,
    pub audit_required: bool,
    /// The read-only rule that covers this cluster, if any.
    pub read_only_rule: Option<String>,
}

impl PolicySummary {
    pub fn of(policy: &EffectivePolicy, profile: &str, bootstrap: &str) -> Self {
        Self {
            label: policy.label(),
            active: policy.is_active(),
            invalid: policy.is_invalid(),
            masking_enforced: policy.masking_enforced(),
            audit_required: policy.audit_required(),
            read_only_rule: policy.read_only_match(profile, bootstrap),
        }
    }

    fn describe(&self) -> String {
        let mut rules = Vec::new();
        if self.masking_enforced {
            rules.push("masking enforced".to_string());
        }
        if self.audit_required {
            rules.push("audit required".to_string());
        }
        if let Some(rule) = &self.read_only_rule {
            rules.push(format!("read-only (\"{rule}\")"));
        }
        if rules.is_empty() {
            self.label.clone()
        } else {
            format!("{} — {}", self.label, rules.join(", "))
        }
    }
}

/// What the report is about and when it was made.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReportContext {
    /// Profile name; empty when the cluster was given by address only.
    pub profile: String,
    /// Bootstrap servers as configured (masked when the report is built).
    pub bootstrap: String,
    pub generated_at_ms: i64,
    /// The audit window ends at `generated_at_ms`; `None` = latest entries.
    pub window: Option<Duration>,
    pub policy: PolicySummary,
    /// Why the cluster could not be read, if it could not.
    pub cluster_error: Option<String>,
    /// Anything else the reader should know (skipped samples and the like).
    pub notes: Vec<String>,
}

/// Every consumer group's lag at one moment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LagSample {
    pub time_ms: i64,
    pub lags: BTreeMap<String, i64>,
}

impl LagSample {
    pub fn of(time_ms: i64, data: &ClusterSnapshot) -> Self {
        Self {
            time_ms,
            lags: data
                .consumers
                .iter()
                .map(|g| (g.group_id.clone(), g.total_lag))
                .collect(),
        }
    }
}

/// Where the lag trend comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TrendSource {
    /// Sampled by `ferrika report --samples` while building the report.
    Sampled,
    /// The GUI's polls since it connected (or the last few minutes of them).
    GuiPolls,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ClusterSummary {
    pub cluster_id: String,
    pub kafka_version: String,
    pub brokers_online: usize,
    pub brokers_total: usize,
    pub controller: Option<i32>,
    pub topics: usize,
    pub partitions: usize,
    pub under_replicated_partitions: usize,
    pub consumer_groups: usize,
    pub total_lag: i64,
}

impl ClusterSummary {
    fn of(data: &ClusterSnapshot) -> Self {
        let topics: Vec<_> = data.topics.iter().filter(|t| !t.is_internal).collect();
        Self {
            cluster_id: data.cluster_id.clone(),
            kafka_version: data.kafka_version.clone(),
            brokers_online: data.brokers.iter().filter(|b| b.is_online).count(),
            brokers_total: data.brokers.len(),
            controller: data.brokers.iter().find(|b| b.is_controller).map(|b| b.id),
            topics: topics.len(),
            partitions: data.topics.iter().map(|t| t.partitions.len()).sum(),
            under_replicated_partitions: data
                .topics
                .iter()
                .flat_map(|t| &t.partitions)
                .filter(|p| p.isr.len() < p.replicas.len())
                .count(),
            consumer_groups: data.consumers.len(),
            total_lag: data.consumers.iter().map(|g| g.total_lag).sum(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct GroupLag {
    pub group_id: String,
    pub state: String,
    pub members: usize,
    pub lag: i64,
    /// Records committed per second, when known (GUI polls or several samples).
    pub consume_rate: Option<f64>,
    pub topics: Vec<String>,
}

/// One group's lag over the samples.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GroupTrend {
    pub group_id: String,
    pub first: i64,
    pub last: i64,
    pub min: i64,
    pub max: i64,
    /// One value per sample the group was seen in, oldest first.
    pub values: Vec<i64>,
}

impl GroupTrend {
    pub fn change(&self) -> i64 {
        self.last - self.first
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LagTrend {
    pub source: TrendSource,
    pub samples: usize,
    pub from_ms: i64,
    pub to_ms: i64,
    /// Groups that had lag in at least one sample, largest growth first.
    pub groups: Vec<GroupTrend>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AuditTimeline {
    /// Entries for this cluster in the window, oldest first, PII-masked.
    pub entries: Vec<AuditEntry>,
    /// Matching entries left out because of the row limit (the oldest ones).
    pub omitted: usize,
}

/// The whole report; render it with [`render_markdown`] or serialize it.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Report {
    pub ferrika_version: String,
    pub profile: String,
    pub bootstrap: String,
    pub generated_at_ms: i64,
    pub window_secs: Option<u64>,
    pub policy: PolicySummary,
    pub cluster_error: Option<String>,
    pub summary: ClusterSummary,
    pub health: Vec<HealthIssue>,
    pub lag_snapshot_ms: Option<i64>,
    pub lag: Vec<GroupLag>,
    /// `None` with a single sample: the snapshot is all there is.
    pub lag_trend: Option<LagTrend>,
    pub audit: AuditTimeline,
    pub notes: Vec<String>,
}

/// Fills each group's consume rate in `last` from the committed offsets it
/// moved since `first`, where the rate is not known yet. Needs two samples;
/// the stalled-consumer health check depends on it.
pub fn fill_consume_rates(
    first: &ClusterSnapshot,
    first_ms: i64,
    last: &mut ClusterSnapshot,
    last_ms: i64,
) {
    let secs = (last_ms - first_ms) as f64 / 1000.0;
    if secs <= 0.0 {
        return;
    }
    for group in last
        .consumers
        .iter_mut()
        .filter(|g| g.consume_rate.is_none())
    {
        let Some(before) = first
            .consumers
            .iter()
            .find(|g| g.group_id == group.group_id)
        else {
            continue;
        };
        if before.current_offset > 0 && group.current_offset >= before.current_offset {
            group.consume_rate = Some((group.current_offset - before.current_offset) as f64 / secs);
        }
    }
}

/// Assembles the report. `latest` is the newest cluster snapshot, `samples`
/// the lag over time (oldest first, the newest matching `latest`), `audit`
/// the audit log (any order; it is filtered to this cluster and the window).
pub fn build(
    ctx: ReportContext,
    latest: &ClusterSnapshot,
    samples: &[LagSample],
    source: TrendSource,
    audit: &[AuditEntry],
) -> Report {
    let mut health = Vec::new();
    if let Some(error) = &ctx.cluster_error {
        health.push(HealthIssue {
            severity: Severity::Critical,
            message: format!("Cannot read the cluster: {error}"),
            target: None,
        });
    }
    if !latest.brokers.is_empty() {
        health.extend(health_checks(latest));
    }

    let mut lag: Vec<GroupLag> = latest
        .consumers
        .iter()
        .map(|g| GroupLag {
            group_id: g.group_id.clone(),
            state: g.state.as_str().to_string(),
            members: g.active_members,
            lag: g.total_lag,
            consume_rate: g.consume_rate,
            topics: g.target_topics.clone(),
        })
        .collect();
    lag.sort_by(|a, b| b.lag.cmp(&a.lag).then_with(|| a.group_id.cmp(&b.group_id)));

    let audit = audit_timeline(
        audit,
        &ctx.profile,
        &ctx.bootstrap,
        ctx.window
            .map(|w| window_start_ms(ctx.generated_at_ms, w.as_secs())),
        ctx.generated_at_ms,
    );

    Report {
        ferrika_version: env!("CARGO_PKG_VERSION").to_string(),
        profile: ctx.profile,
        bootstrap: mask_bootstrap(&ctx.bootstrap),
        generated_at_ms: ctx.generated_at_ms,
        window_secs: ctx.window.map(|w| w.as_secs()),
        policy: ctx.policy,
        cluster_error: ctx.cluster_error,
        summary: ClusterSummary::of(latest),
        health,
        lag_snapshot_ms: samples.last().map(|s| s.time_ms),
        lag,
        lag_trend: lag_trend(samples, source),
        audit,
        notes: ctx.notes,
    }
}

/// Per-group lag over `samples`; `None` with fewer than two samples.
pub(crate) fn lag_trend(samples: &[LagSample], source: TrendSource) -> Option<LagTrend> {
    let (first, last) = (samples.first()?, samples.last()?);
    if samples.len() < 2 {
        return None;
    }
    let mut groups: Vec<GroupTrend> = last
        .lags
        .keys()
        .filter_map(|id| {
            let values: Vec<i64> = samples
                .iter()
                .filter_map(|s| s.lags.get(id))
                .copied()
                .collect();
            let (min, max) = (*values.iter().min()?, *values.iter().max()?);
            (max > 0).then(|| GroupTrend {
                group_id: id.clone(),
                first: values[0],
                last: values[values.len() - 1],
                min,
                max,
                values,
            })
        })
        .collect();
    groups.sort_by(|a, b| {
        b.change()
            .cmp(&a.change())
            .then_with(|| b.last.cmp(&a.last))
            .then_with(|| a.group_id.cmp(&b.group_id))
    });
    Some(LagTrend {
        source,
        samples: samples.len(),
        from_ms: first.time_ms,
        to_ms: last.time_ms,
        groups,
    })
}

/// Whether an audit entry is about this cluster. Entries without a profile
/// (an invalid policy, for one) concern every cluster.
fn concerns(entry: &AuditEntry, profile: &str, bootstrap: &str) -> bool {
    entry.profile.is_empty()
        || (!profile.is_empty() && entry.profile == profile)
        || (!bootstrap.is_empty() && entry.profile == bootstrap)
}

/// The audit entries for this cluster between `from_ms` and `to_ms`, oldest
/// first. Without `from_ms` only the latest [`AUDIT_DEFAULT_LIMIT`] are kept.
/// Targets and outcomes are PII-masked: they are names and messages, never
/// payloads, but an unmask reason or an error text may quote a value.
pub(crate) fn audit_timeline(
    entries: &[AuditEntry],
    profile: &str,
    bootstrap: &str,
    from_ms: Option<i64>,
    to_ms: i64,
) -> AuditTimeline {
    let mut matching: Vec<&AuditEntry> = entries
        .iter()
        .filter(|e| concerns(e, profile, bootstrap))
        .filter(|e| e.time_ms <= to_ms && from_ms.is_none_or(|from| e.time_ms >= from))
        .collect();
    matching.sort_by_key(|e| e.time_ms);
    let limit = if from_ms.is_some() {
        AUDIT_WINDOW_LIMIT
    } else {
        AUDIT_DEFAULT_LIMIT
    };
    let omitted = matching.len().saturating_sub(limit);
    AuditTimeline {
        entries: matching[omitted..]
            .iter()
            .map(|e| AuditEntry {
                target: mask_text(&e.target, MaskStrategy::Redact),
                outcome: mask_text(&e.outcome, MaskStrategy::Redact),
                ..(*e).clone()
            })
            .collect(),
        omitted,
    }
}

/// `2026-11-02 09:30:00 UTC`.
fn utc(ms: i64) -> String {
    let text = ferrika_core::model::format::format_timestamp_ms(ms);
    match text.get(..19) {
        Some(seconds) => format!("{seconds} UTC"),
        None => text,
    }
}

/// `09:30:00` (UTC), for timeline rows of one report.
fn utc_time(ms: i64) -> String {
    ferrika_core::model::format::format_timestamp_ms(ms)
        .get(11..19)
        .unwrap_or("-")
        .to_string()
}

/// Text that is safe inside a Markdown table cell.
fn cell(text: &str) -> String {
    let text = text.replace(['\r', '\n'], " ").replace('|', "\\|");
    if text.trim().is_empty() {
        "-".to_string()
    } else {
        text
    }
}

/// `1,234,567`.
fn thousands(n: i64) -> String {
    let digits = n.unsigned_abs().to_string();
    let mut out = String::new();
    for (i, c) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(c);
    }
    if n < 0 {
        format!("-{out}")
    } else {
        out
    }
}

fn signed(n: i64) -> String {
    if n > 0 {
        format!("+{}", thousands(n))
    } else {
        thousands(n)
    }
}

/// A one-line chart of `values` with block characters, scaled to their max.
fn sparkline(values: &[i64]) -> String {
    const BARS: [char; 8] = ['▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'];
    let max = values.iter().copied().max().unwrap_or(0).max(1);
    values
        .iter()
        .map(|v| {
            let ix = (*v).max(0) as f64 / max as f64 * (BARS.len() - 1) as f64;
            BARS[(ix.round() as usize).min(BARS.len() - 1)]
        })
        .collect()
}

fn severity_label(severity: Severity) -> &'static str {
    match severity {
        Severity::Critical => "🔴 Critical",
        Severity::Warning => "🟠 Warning",
        Severity::Info => "⚪ Info",
    }
}

/// The report as Markdown.
pub fn render_markdown(report: &Report) -> String {
    let mut md = String::new();
    let mut line = |text: &str| {
        md.push_str(text);
        md.push('\n');
    };
    let title = if report.profile.is_empty() {
        report.bootstrap.as_str()
    } else {
        report.profile.as_str()
    };
    line(&format!("# Incident report — {}", cell(title)));
    line("");
    line("| | |");
    line("|---|---|");
    line(&format!(
        "| Profile | {} |",
        if report.profile.is_empty() {
            "(none)".to_string()
        } else {
            cell(&report.profile)
        }
    ));
    line(&format!("| Bootstrap | `{}` |", cell(&report.bootstrap)));
    if !report.summary.cluster_id.is_empty() {
        line(&format!(
            "| Cluster ID | `{}` |",
            cell(&report.summary.cluster_id)
        ));
    }
    if !report.summary.kafka_version.is_empty() {
        line(&format!(
            "| Kafka version | {} |",
            cell(&report.summary.kafka_version)
        ));
    }
    line(&format!("| Generated | {} |", utc(report.generated_at_ms)));
    line(&format!(
        "| Window | {} |",
        match report.window_secs {
            Some(secs) => format!(
                "last {} ({} → {})",
                format_window(Duration::from_secs(secs)),
                utc(window_start_ms(report.generated_at_ms, secs)),
                utc(report.generated_at_ms)
            ),
            None => format!("not set (latest {AUDIT_DEFAULT_LIMIT} audit entries)"),
        }
    ));
    line(&format!("| Ferrika | {} |", cell(&report.ferrika_version)));
    line(&format!("| Policy | {} |", cell(&report.policy.describe())));
    line("");

    let s = &report.summary;
    let count = |severity| {
        report
            .health
            .iter()
            .filter(|i| i.severity == severity)
            .count()
    };
    line("## Summary");
    line("");
    if let Some(error) = &report.cluster_error {
        line(&format!(
            "- **The cluster could not be read:** {}",
            cell(error)
        ));
    }
    line(&format!(
        "- Brokers online: {}/{}{}",
        s.brokers_online,
        s.brokers_total,
        s.controller
            .map(|id| format!(" (controller node {id})"))
            .unwrap_or_default()
    ));
    line(&format!(
        "- Topics: {} ({} partitions), under-replicated partitions: {}",
        s.topics, s.partitions, s.under_replicated_partitions
    ));
    line(&format!(
        "- Consumer groups: {}, total lag: {}",
        s.consumer_groups,
        thousands(s.total_lag)
    ));
    line(&format!(
        "- Health: {} critical, {} warning, {} info",
        count(Severity::Critical),
        count(Severity::Warning),
        count(Severity::Info)
    ));
    line(&format!(
        "- Audit entries in the report: {}",
        report.audit.entries.len()
    ));
    line("");

    line("## Health checks");
    line("");
    if report.health.is_empty() {
        line("No problems found.");
    } else {
        line("| Severity | Finding |");
        line("|---|---|");
        for issue in &report.health {
            line(&format!(
                "| {} | {} |",
                severity_label(issue.severity),
                cell(&issue.message)
            ));
        }
    }
    line("");

    line("## Consumer lag");
    line("");
    if let Some(at) = report.lag_snapshot_ms {
        line(&format!("Snapshot at {}.", utc(at)));
        line("");
    }
    if report.lag.is_empty() {
        line("No consumer groups.");
    } else {
        line("| Group | State | Members | Lag | Consumed/s | Topics |");
        line("|---|---|---:|---:|---:|---|");
        for g in report.lag.iter().take(MAX_GROUP_ROWS) {
            line(&format!(
                "| {} | {} | {} | {} | {} | {} |",
                cell(&g.group_id),
                g.state,
                g.members,
                thousands(g.lag),
                g.consume_rate
                    .map(|r| format!("{r:.1}"))
                    .unwrap_or_else(|| "-".to_string()),
                cell(&g.topics.join(", "))
            ));
        }
        let more = report.lag.len().saturating_sub(MAX_GROUP_ROWS);
        if more > 0 {
            line("");
            line(&format!("+{more} more group(s) with less lag."));
        }
    }
    line("");

    line("## Lag trend");
    line("");
    match &report.lag_trend {
        None => {
            line(
                "Snapshot only (one sample). Ferrika keeps no lag history on disk; \
                 run `ferrika report --samples N --interval SECONDS` to sample lag \
                 over time, or use the GUI's Report button after it has polled for a while.",
            );
        }
        Some(trend) => {
            let source = match trend.source {
                TrendSource::Sampled => "sampled while building this report",
                TrendSource::GuiPolls => "from the GUI's recent polls",
            };
            line(&format!(
                "{} samples {source}, {} → {} (lag history before that is not recorded).",
                trend.samples,
                utc(trend.from_ms),
                utc(trend.to_ms)
            ));
            line("");
            if trend.groups.is_empty() {
                line("No group had lag in any sample.");
            } else {
                line("| Group | First | Last | Change | Min | Max | Trend |");
                line("|---|---:|---:|---:|---:|---:|---|");
                for g in trend.groups.iter().take(MAX_GROUP_ROWS) {
                    line(&format!(
                        "| {} | {} | {} | {} | {} | {} | `{}` |",
                        cell(&g.group_id),
                        thousands(g.first),
                        thousands(g.last),
                        signed(g.change()),
                        thousands(g.min),
                        thousands(g.max),
                        sparkline(&g.values)
                    ));
                }
                let more = trend.groups.len().saturating_sub(MAX_GROUP_ROWS);
                if more > 0 {
                    line("");
                    line(&format!("+{more} more group(s)."));
                }
            }
        }
    }
    line("");

    line("## Audit timeline");
    line("");
    if report.audit.entries.is_empty() {
        line("No recorded operations for this cluster in the window.");
    } else {
        line("| Time (UTC) | From | User | Action | Target | Outcome |");
        line("|---|---|---|---|---|---|");
        for e in &report.audit.entries {
            let time = if report.window_secs.is_some_and(|s| s <= 86_400) {
                utc_time(e.time_ms)
            } else {
                utc(e.time_ms).trim_end_matches(" UTC").to_string()
            };
            line(&format!(
                "| {time} | {} | {} | {} | {} | {}{} |",
                cell(&e.source),
                cell(&e.user),
                cell(&e.action),
                cell(&e.target),
                if e.ok { "" } else { "❌ " },
                cell(&e.outcome)
            ));
        }
        if report.audit.omitted > 0 {
            line("");
            line(&format!(
                "{} older matching entries are not listed.",
                report.audit.omitted
            ));
        }
    }
    line("");

    if !report.notes.is_empty() {
        line("## Notes");
        line("");
        for note in &report.notes {
            line(&format!("- {}", cell(note)));
        }
        line("");
    }

    line("---");
    line("");
    line(&format!(
        "Generated by Ferrika {}. No record contents are included; audit targets and \
         outcomes are PII-masked and credentials are removed from addresses.",
        report.ferrika_version
    ));
    md
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrika_core::model::cluster::ConsumerState;

    /// 2026-11-02 10:00:00 UTC.
    const NOW: i64 = 1_793_613_600_000;

    fn ctx() -> ReportContext {
        ReportContext {
            profile: "prod".into(),
            bootstrap: "admin:hunter2@k1:9092, k2:9092".into(),
            generated_at_ms: NOW,
            window: Some(Duration::from_secs(30 * 60)),
            policy: PolicySummary::of(
                &EffectivePolicy::absent(std::path::PathBuf::new()),
                "prod",
                "k1:9092",
            ),
            cluster_error: None,
            notes: Vec::new(),
        }
    }

    fn audit(minutes_ago: i64, profile: &str, target: &str, outcome: &str) -> AuditEntry {
        AuditEntry {
            time_ms: NOW - minutes_ago * 60_000,
            source: "gui".into(),
            profile: profile.into(),
            action: "Deleting topics".into(),
            target: target.into(),
            outcome: outcome.into(),
            ok: !outcome.starts_with("failed"),
            user: "kim".into(),
            actor: None,
            op: String::new(),
            op_digest: String::new(),
        }
    }

    fn sample(minutes_ago: i64, lags: &[(&str, i64)]) -> LagSample {
        LagSample {
            time_ms: NOW - minutes_ago * 60_000,
            lags: lags.iter().map(|(g, l)| (g.to_string(), *l)).collect(),
        }
    }

    #[test]
    fn huge_windows_are_rejected_or_saturate() {
        assert!(parse_window("18446744073709551615s").is_err());
        assert!(parse_window("9223372036854775d").is_err());
        assert!(parse_window("100000d").is_err());
        assert!(parse_window("36500d").is_ok());
        assert_eq!(window_start_ms(10_000, 3), 7_000);
        assert_eq!(window_start_ms(0, u64::MAX), -i64::MAX);
    }

    #[test]
    fn windows_parse_and_format() {
        assert_eq!(parse_window("30m"), Ok(Duration::from_secs(1800)));
        assert_eq!(parse_window("2h"), Ok(Duration::from_secs(7200)));
        assert_eq!(parse_window("1d"), Ok(Duration::from_secs(86_400)));
        assert_eq!(parse_window("45s"), Ok(Duration::from_secs(45)));
        for bad in ["30", "m", "0m", "3w", "1.5h", ""] {
            assert!(parse_window(bad).is_err(), "{bad}");
        }
        assert_eq!(format_window(Duration::from_secs(1800)), "30m");
        assert_eq!(format_window(Duration::from_secs(93_600)), "1d 2h");
    }

    #[test]
    fn bootstrap_loses_credentials() {
        assert_eq!(
            mask_bootstrap("admin:hunter2@k1:9092, k2:9092"),
            "****@k1:9092,k2:9092"
        );
        assert_eq!(
            mask_bootstrap("sasl://user:pw@k1:9093"),
            "sasl://user:****@k1:9093"
        );
    }

    #[test]
    fn audit_timeline_keeps_this_cluster_in_the_window_and_masks_text() {
        let entries = vec![
            audit(5, "prod", "orders", "ok"),
            audit(90, "prod", "too-old", "ok"),
            audit(10, "staging", "other-cluster", "ok"),
            audit(20, "", "policy.json", "signature invalid"),
            audit(
                1,
                "prod",
                "orders",
                "reason: customer kim@example.com 010-1234-5678",
            ),
            audit(-5, "prod", "future", "ok"),
        ];
        let timeline = audit_timeline(&entries, "prod", "k1:9092", Some(NOW - 1_800_000), NOW);
        let targets: Vec<&str> = timeline.entries.iter().map(|e| e.target.as_str()).collect();
        assert_eq!(targets, vec!["policy.json", "orders", "orders"]);
        assert_eq!(timeline.omitted, 0);
        let text = format!("{:?}", timeline.entries);
        assert!(!ferrika_governance::test_policy::leaks(&text), "{text}");

        // Without a window: the latest entries, any age.
        let latest = audit_timeline(&entries, "prod", "", None, NOW);
        assert_eq!(
            latest.entries.first().map(|e| e.target.as_str()),
            Some("too-old")
        );
    }

    #[test]
    fn audit_timeline_keeps_the_newest_entries_over_the_limit() {
        let entries: Vec<AuditEntry> = (0..(AUDIT_DEFAULT_LIMIT as i64 + 7))
            .map(|i| audit(i, "prod", &format!("t{i}"), "ok"))
            .collect();
        let timeline = audit_timeline(&entries, "prod", "", None, NOW);
        assert_eq!(timeline.entries.len(), AUDIT_DEFAULT_LIMIT);
        assert_eq!(timeline.omitted, 7);
        assert_eq!(
            timeline.entries.last().map(|e| e.target.as_str()),
            Some("t0")
        );
    }

    #[test]
    fn lag_trend_needs_two_samples_and_sorts_by_growth() {
        assert_eq!(
            lag_trend(&[sample(0, &[("a", 5)])], TrendSource::Sampled),
            None
        );
        let samples = [
            sample(2, &[("a", 100), ("b", 50), ("idle", 0)]),
            sample(1, &[("a", 80), ("b", 400), ("idle", 0), ("new", 3)]),
            sample(0, &[("a", 60), ("b", 900), ("idle", 0), ("new", 1)]),
        ];
        let trend = lag_trend(&samples, TrendSource::Sampled).unwrap();
        assert_eq!(trend.samples, 3);
        let ids: Vec<&str> = trend.groups.iter().map(|g| g.group_id.as_str()).collect();
        assert_eq!(ids, vec!["b", "new", "a"]);
        let b = &trend.groups[0];
        assert_eq!(
            (b.first, b.last, b.min, b.max, b.change()),
            (50, 900, 50, 900, 850)
        );
        assert_eq!(trend.groups[1].values, vec![3, 1]);
    }

    #[test]
    fn consume_rates_come_from_committed_offsets_between_samples() {
        let first = ClusterSnapshot::demo();
        let mut last = first.clone();
        for g in last.consumers.iter_mut() {
            g.consume_rate = None;
        }
        last.consumers[0].current_offset = first.consumers[0].current_offset + 500;
        fill_consume_rates(&first, 0, &mut last, 10_000);
        assert_eq!(last.consumers[0].consume_rate, Some(50.0));
    }

    #[test]
    fn report_from_mock_cluster_renders_every_section() {
        let mut data = ClusterSnapshot::demo();
        data.brokers[1].is_online = false;
        data.consumers[0].total_lag = 1_234_567;
        data.consumers[0].state = ConsumerState::Stable;
        let first = data.consumers[0].group_id.clone();
        let samples = [
            sample(2, &[(first.as_str(), 1_000)]),
            LagSample::of(NOW, &data),
        ];
        let entries = vec![
            audit(3, "prod", "orders|eu", "failed: kim@example.com"),
            audit(40, "prod", "old", "ok"),
        ];
        let report = build(ctx(), &data, &samples, TrendSource::Sampled, &entries);
        assert_eq!(report.bootstrap, "****@k1:9092,k2:9092");
        assert_eq!(report.lag[0].group_id, first);
        assert!(report
            .health
            .iter()
            .any(|i| i.message.starts_with("Broker offline")));

        let md = render_markdown(&report);
        for heading in [
            "# Incident report — prod",
            "## Summary",
            "## Health checks",
            "## Consumer lag",
            "## Lag trend",
            "## Audit timeline",
        ] {
            assert!(md.contains(heading), "missing {heading}:\n{md}");
        }
        assert!(
            md.contains("| Bootstrap | `****@k1:9092,k2:9092` |"),
            "{md}"
        );
        assert!(
            md.contains(
                "| Window | last 30m (2026-11-02 09:30:00 UTC → 2026-11-02 10:00:00 UTC) |"
            ),
            "{md}"
        );
        assert!(md.contains("| Policy | no policy |"), "{md}");
        assert!(md.contains("Broker offline: node 2"), "{md}");
        assert!(md.contains("1,234,567"), "{md}");
        assert!(md.contains("+1,233,567"), "{md}");
        assert!(md.contains("orders\\|eu"), "pipes are escaped:\n{md}");
        assert!(
            md.contains("| 09:57:00 | gui | kim | Deleting topics |"),
            "{md}"
        );
        assert!(!md.contains("| old |"), "outside the window:\n{md}");
        assert!(!md.contains("hunter2") && !md.contains("admin:"), "{md}");
        assert!(!ferrika_governance::test_policy::leaks(&md), "{md}");

        let json = serde_json::to_value(&report).unwrap();
        assert_eq!(json["profile"], "prod");
        assert_eq!(json["lag_trend"]["source"], "sampled");
    }

    #[test]
    fn unreachable_cluster_still_gets_a_report() {
        let mut context = ctx();
        context.cluster_error = Some("all brokers down".into());
        context.window = None;
        let data = ClusterSnapshot::empty("prod", "k1:9092");
        let report = build(context, &data, &[], TrendSource::Sampled, &[]);
        assert_eq!(report.health.len(), 1);
        assert_eq!(report.health[0].severity, Severity::Critical);
        let md = render_markdown(&report);
        assert!(
            md.contains("Cannot read the cluster: all brokers down"),
            "{md}"
        );
        assert!(md.contains("Snapshot only"), "{md}");
        assert!(md.contains("not set (latest 100 audit entries)"), "{md}");
        assert!(md.contains("No recorded operations"), "{md}");
    }

    #[test]
    fn policy_header_names_the_rules_in_force() {
        let policy = ferrika_governance::test_policy::masking();
        let summary = PolicySummary::of(&policy, "prod", "k1:9092");
        assert!(summary.masking_enforced);
        assert!(
            summary.describe().ends_with("— masking enforced"),
            "{}",
            summary.describe()
        );
    }

    #[test]
    fn sparkline_scales_to_the_max() {
        assert_eq!(sparkline(&[0, 50, 100]), "▁▅█");
        assert_eq!(sparkline(&[0, 0]), "▁▁");
    }
}
