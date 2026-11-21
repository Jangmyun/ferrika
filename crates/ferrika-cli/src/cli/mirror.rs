use std::io::Write;
use std::time::Duration;

use clap::Args;
use colored::Colorize;
use comfy_table::{Cell, Color};
use serde_json::json;

use crate::cli::{output, print_connection_hint, GlobalOpts, OutputFormat};
use crate::error::{KafkaError, KafkaResult};
use crate::governance::policy::{
    self, AuditMode, AuditSink, EffectivePolicy, ExportChannel, ExportRequest, WriteRequest,
};
use crate::kafka::config::KafkaClientConfig;
use crate::kafka::filter::MessageFilter;
use crate::kafka::profile::ProfileStore;
use crate::model::policy::PolicyAction;
use crate::ops::mirror::{run_mirror_pipeline, MirrorConfig};

/// Command line arguments for topic mirroring and replication.
#[derive(Args, Debug, Clone, PartialEq)]
pub struct MirrorArgs {
    /// Source topic to read messages from
    pub src_topic: String,

    /// Destination topic to write messages to
    pub dst_topic: String,

    /// Profile name for the source cluster
    #[arg(long)]
    pub src_profile: Option<String>,

    /// Profile name for the destination cluster (defaults to src-profile or active)
    #[arg(long)]
    pub dst_profile: Option<String>,

    /// Kafka bootstrap servers for the source cluster (overrides profile)
    #[arg(long)]
    pub src_bootstrap_servers: Option<String>,

    /// Kafka bootstrap servers for the destination cluster (overrides profile)
    #[arg(long)]
    pub dst_bootstrap_servers: Option<String>,

    /// Start mirroring from the earliest available offset (with --group:
    /// only when the group has no committed offset yet)
    #[arg(short = 'b', long)]
    pub from_beginning: bool,

    /// Consumer group on the source cluster: a later run with the same group
    /// resumes where this one stopped. An offset is committed only after its
    /// record was delivered to the destination, so a failed or interrupted
    /// run copies records again rather than skipping them.
    #[arg(short = 'g', long, value_name = "GROUP")]
    pub group: Option<String>,

    /// Specific partition to mirror (mirrors all partitions if omitted)
    #[arg(short = 'p', long)]
    pub partition: Option<i32>,

    /// Maximum number of messages to mirror before exiting
    #[arg(short = 'n', long)]
    pub max_messages: Option<usize>,

    /// Rate limit in messages per second
    #[arg(short = 'r', long)]
    pub rate_limit: Option<f64>,

    /// Filter messages with a payload regular expression (case-insensitive)
    #[arg(short = 'G', long = "grep")]
    pub grep: Option<String>,

    /// Filter messages by key pattern
    #[arg(short = 'k', long = "key-filter")]
    pub key_filter: Option<String>,

    /// Filter messages by required header in KEY:VALUE format
    #[arg(long = "header-filter")]
    pub header_filter: Vec<String>,
}

/// Resolves a KafkaClientConfig for a given profile name and bootstrap server override.
fn resolve_cluster_config(
    profile_name: Option<&str>,
    bootstrap_override: Option<&str>,
    global: &GlobalOpts,
) -> KafkaClientConfig {
    let store = global.profile_store();
    cluster_config(&store, profile_name, bootstrap_override, global)
}

/// [`resolve_cluster_config`] against `store`.
fn cluster_config(
    store: &ProfileStore,
    profile_name: Option<&str>,
    bootstrap_override: Option<&str>,
    global: &GlobalOpts,
) -> KafkaClientConfig {
    let base_profile = if let Some(p_name) = profile_name {
        store.get(p_name).cloned()
    } else if let Some(ref g_p) = global.profile {
        store.get(g_p).cloned()
    } else {
        store.get_active().cloned()
    };

    let mut config = if let Some(profile) = base_profile {
        profile.to_kafka_config()
    } else {
        KafkaClientConfig::new(crate::kafka::context::FALLBACK_BOOTSTRAP)
    };

    if let Some(bs) = bootstrap_override {
        config.bootstrap_servers = bs.to_string();
    } else if let Some(ref g_bs) = global.bootstrap_servers {
        config.bootstrap_servers = g_bs.clone();
    }

    config.request_timeout = Duration::from_secs(global.timeout);
    config
}

/// The profile name [`cluster_config`] resolves (empty when there is none):
/// what the policy's read-only rules and the audit log name the cluster by.
fn cluster_profile_name(
    store: &ProfileStore,
    profile_name: Option<&str>,
    global: &GlobalOpts,
) -> String {
    profile_name
        .map(str::to_string)
        .or_else(|| global.profile.clone())
        .or_else(|| store.active_profile.clone())
        .unwrap_or_default()
}

/// Whether `args` name the clusters to copy between; then [`authorize`]
/// checks the destination, and the gate in `main` stays out (see
/// `main_gate_action`).
pub fn names_clusters(args: &MirrorArgs) -> bool {
    args.dst_profile.is_some() || args.src_profile.is_some() || args.dst_bootstrap_servers.is_some()
}

/// The write gate in `main` checks the cluster the global options name, but
/// `mirror` writes to its destination cluster; see `authorize_checking_brokers`.
pub fn authorize(args: &MirrorArgs, global: &GlobalOpts) -> Result<(), String> {
    let store = global.profile_store();
    let requested = authorize_checking_brokers(
        crate::governance::policy::current(),
        &crate::governance::policy::AuditLog,
        &store,
        args,
        global,
        crate::kafka::admin::known_brokers,
    )?;
    // The destination's request entry was written here, not in `main`: owe
    // its result line too (see `cli::record_audit_result`).
    if let Some(profile) = requested {
        crate::cli::expect_audit_result(crate::governance::audit::AuditEntry {
            time_ms: 0,
            source: "cli".to_string(),
            profile,
            action: "mirror".to_string(),
            target: format!("{} -> {}", args.src_topic, args.dst_topic),
            outcome: String::new(),
            ok: true,
            user: String::new(),
            actor: None,
            op: String::new(),
            op_digest: String::new(),
        });
    }
    Ok(())
}

/// [`authorize_checking_brokers`] without a cluster to ask: tests whose
/// policies need no broker metadata.
#[cfg(test)]
fn authorize_with(
    policy: &EffectivePolicy,
    sink: &impl AuditSink,
    store: &ProfileStore,
    args: &MirrorArgs,
    global: &GlobalOpts,
) -> Result<(), String> {
    authorize_checking_brokers(policy, sink, store, args, global, |_| {
        Err("no cluster in this test".to_string())
    })
    .map(|_| ())
}

/// Refuses `mirror` under enforced PII masking (it would copy unmasked
/// records to another cluster), then the export gate when the destination is
/// another cluster (see [`authorize_cross_cluster`]), then the write gate for the destination
/// cluster as a `produce`: a read-only target or `forbid: produce` refuses it
/// with a `policy.blocked` entry, and so does a read-only destination profile.
///
/// `fetch` lists the brokers a cluster reports, for `read_only_brokers`:
/// a tunnel or an IP address of the destination must not dodge it (the gate
/// in `main` does the same for the global cluster).
fn authorize_checking_brokers(
    policy: &EffectivePolicy,
    sink: &impl AuditSink,
    store: &ProfileStore,
    args: &MirrorArgs,
    global: &GlobalOpts,
    fetch: impl Fn(&KafkaClientConfig) -> Result<Vec<String>, String>,
) -> Result<Option<String>, String> {
    // An unknown profile would silently fall back to localhost.
    for name in [args.src_profile.as_deref(), args.dst_profile.as_deref()]
        .into_iter()
        .flatten()
    {
        if store.get(name).is_none() {
            return Err(format!(
                "Profile '{name}' does not exist (see `ferrika profile list`)"
            ));
        }
    }
    let target = format!("{} -> {}", args.src_topic, args.dst_topic);
    if policy.masking_enforced() {
        let reason = format!(
            "Refused by {}: PII masking is enforced, and `mirror` would copy unmasked records to another cluster.",
            policy.label()
        );
        let src_profile = args.src_profile.as_deref();
        let mut profile = cluster_profile_name(store, src_profile, global);
        if profile.is_empty() {
            profile = cluster_config(
                store,
                src_profile,
                args.src_bootstrap_servers.as_deref(),
                global,
            )
            .bootstrap_servers;
        }
        let attempted = format!("mirror {target}");
        let entry = policy::blocked_entry("cli", &profile, &attempted, &reason);
        if let Err(e) = sink.record(&entry) {
            tracing::warn!(%e, "Could not write the audit log");
        }
        return Err(reason);
    }
    authorize_cross_cluster(policy, sink, store, args, global, &target, &fetch)?;

    // Without any cluster option the destination is the cluster the gate in
    // `main` already checked and recorded.
    if !names_clusters(args) {
        return Ok(None);
    }
    let dst_name = args.dst_profile.as_deref().or(args.src_profile.as_deref());
    let profile = cluster_profile_name(store, dst_name, global);
    let config = cluster_config(
        store,
        dst_name,
        args.dst_bootstrap_servers.as_deref(),
        global,
    );
    let brokers =
        match policy::brokers_for_with(&config, policy.write_needs_broker_metadata(), &fetch) {
            Ok(brokers) => brokers,
            Err(reason) => {
                let named = if profile.is_empty() {
                    config.bootstrap_servers.as_str()
                } else {
                    profile.as_str()
                };
                let entry =
                    policy::blocked_entry("cli", named, &format!("mirror {target}"), &reason);
                if let Err(e) = sink.record(&entry) {
                    tracing::warn!(%e, "Could not write the audit log");
                }
                return Err(reason);
            }
        };
    let request = WriteRequest {
        source: "cli",
        action: PolicyAction::Produce,
        label: "mirror",
        target: &target,
        profile: &profile,
        bootstrap: &brokers,
        actor: None,
        op: None,
    };
    policy::authorize_write_with(policy, sink, &request, AuditMode::RecordRequest, || {
        if !config.read_only {
            return Ok(());
        }
        let name = if profile.is_empty() {
            "the destination profile"
        } else {
            profile.as_str()
        };
        Err(format!(
            "Destination profile '{name}' is read-only, so `mirror` is refused. \
             Turn it off with `ferrika profile read-only {name} --off`."
        ))
    })
    .map_err(|refusal| refusal.to_string())?;
    // The profile the request entry names (its brokers without a name), for
    // the result entry to pair with.
    Ok(Some(if profile.is_empty() { brokers } else { profile }))
}

/// Copying records to another cluster takes them out of the source cluster:
/// the export gate decides it (`export: forbidden` refuses, `audited`
/// records it), for the source cluster's brokers. Destinations whose brokers are all
/// among the source's are the same cluster, where `mirror` is
/// only a `produce`.
fn authorize_cross_cluster(
    policy: &EffectivePolicy,
    sink: &impl AuditSink,
    store: &ProfileStore,
    args: &MirrorArgs,
    global: &GlobalOpts,
    target: &str,
    fetch: &impl Fn(&KafkaClientConfig) -> Result<Vec<String>, String>,
) -> Result<(), String> {
    let src_name = args.src_profile.as_deref();
    let dst_name = args.dst_profile.as_deref().or(src_name);
    let src = cluster_config(
        store,
        src_name,
        args.src_bootstrap_servers.as_deref(),
        global,
    );
    let dst = cluster_config(
        store,
        dst_name,
        args.dst_bootstrap_servers.as_deref(),
        global,
    );
    if policy::destination_within_source(&src.bootstrap_servers, &dst.bootstrap_servers) {
        return Ok(());
    }
    let mut destination = cluster_profile_name(store, dst_name, global);
    if destination.is_empty() || args.dst_bootstrap_servers.is_some() {
        destination = dst.bootstrap_servers.clone();
    }
    let target = format!("{target} (to {destination})");
    let profile = cluster_profile_name(store, src_name, global);
    // `export_clusters` matches the brokers the source reports too, as the
    // export gate in `main` does: a tunnel or an IP must not dodge it.
    let src_brokers = match policy::export_brokers_with(policy, &src, fetch) {
        Ok(brokers) => brokers,
        Err(reason) => {
            let named = if profile.is_empty() {
                src.bootstrap_servers.as_str()
            } else {
                profile.as_str()
            };
            let entry = policy::blocked_entry("cli", named, &format!("mirror {target}"), &reason);
            if let Err(e) = sink.record(&entry) {
                tracing::warn!(%e, "Could not write the audit log");
            }
            return Err(reason);
        }
    };
    let request = ExportRequest {
        source: "cli",
        channel: ExportChannel::Cluster,
        label: "mirror",
        target: &target,
        records: None,
        profile: &profile,
        bootstrap: &src_brokers,
        actor: None,
    };
    policy::authorize_export_with(policy, sink, &request).map_err(|refusal| refusal.to_string())?;
    crate::cli::remember_export_with(policy, &request);
    Ok(())
}

/// Main entry point for `ferrika mirror`.
pub async fn run(args: MirrorArgs, global: &GlobalOpts) -> KafkaResult<()> {
    let mut src_cfg = resolve_cluster_config(
        args.src_profile.as_deref(),
        args.src_bootstrap_servers.as_deref(),
        global,
    );
    if let Some(group) = &args.group {
        src_cfg = src_cfg.with_group_id(group);
    }

    let dst_profile = args.dst_profile.as_deref().or(args.src_profile.as_deref());
    let dst_cfg =
        resolve_cluster_config(dst_profile, args.dst_bootstrap_servers.as_deref(), global);

    if crate::ops::mirror::is_self_mirror(
        &src_cfg.bootstrap_servers,
        &args.src_topic,
        &dst_cfg.bootstrap_servers,
        &args.dst_topic,
    ) {
        return Err(KafkaError::config(format!(
            "Source and destination are the same topic '{}' on the same cluster; \
             mirroring would read its own output forever. Use a different destination topic.",
            args.src_topic
        )));
    }

    let mut filter = MessageFilter::new();
    if let Some(ref g) = args.grep {
        filter = filter.with_grep(g)?;
    }
    if let Some(ref k) = args.key_filter {
        filter = filter.with_key(k);
    }
    for h in &args.header_filter {
        let (k, v) = crate::kafka::filter::parse_header_pair(h)
            .map_err(|e| KafkaError::config(format!("--header-filter: {e}")))?;
        filter = filter.with_header(k, v);
    }

    let mirror_config = MirrorConfig::new(&args.src_topic, &args.dst_topic)
        .with_from_beginning(args.from_beginning)
        .with_partition(args.partition)
        .with_max_messages(args.max_messages)
        .with_rate_limit(args.rate_limit)
        .with_filter(filter);

    if global.format != OutputFormat::Json {
        println!(
            "{} Starting topic mirror pipeline: '{}' ➜ '{}'",
            "🔁".cyan().bold(),
            args.src_topic.yellow().bold(),
            args.dst_topic.green().bold()
        );
        println!(
            "  Source Broker:      {}",
            src_cfg.bootstrap_servers.bright_white()
        );
        println!(
            "  Destination Broker: {}",
            dst_cfg.bootstrap_servers.bright_white()
        );
        if args.from_beginning {
            println!(
                "  Offset Reset:       {}",
                "earliest (from beginning)".yellow()
            );
        }
        if let Some(p) = args.partition {
            println!("  Partition:          {}", p.to_string().yellow());
        }
        if let Some(group) = &args.group {
            println!(
                "  Consumer Group:     {} (resumes from its committed offsets)",
                group.yellow()
            );
        }
        if let Some(max) = args.max_messages {
            println!(
                "  Limit:              {} messages",
                max.to_string().yellow()
            );
        }
        if let Some(r) = args.rate_limit {
            println!(
                "  Rate Limit:         {} events/sec",
                r.to_string().yellow()
            );
        }
        println!();
    }

    let progress_format = global.format;
    let result = run_mirror_pipeline(&src_cfg, &dst_cfg, mirror_config, move |stats| {
        if progress_format != OutputFormat::Json {
            print!(
                "\r{} Mirrored: {} msgs ({:.1} EPS, {:.2} MB/s) | Skipped: {} | Elapsed: {:.1}s    ",
                "🔁".cyan(),
                stats.mirrored_count.to_string().green().bold(),
                stats.average_eps,
                stats.average_mbps,
                stats.filtered_count.to_string().bright_black(),
                stats.elapsed_secs
            );
            let _ = std::io::stdout().flush();
        }
    })
    .await;

    let stats = match result {
        Ok(s) => s,
        Err(err) => {
            print_connection_hint(&src_cfg, &err);
            return Err(err);
        }
    };
    crate::cli::export_finished(stats.mirrored_count);

    match global.format {
        OutputFormat::Json => {
            output::print_json(&json!({
                "src_topic": args.src_topic,
                "dst_topic": args.dst_topic,
                "read_count": stats.read_count,
                "mirrored_count": stats.mirrored_count,
                "filtered_count": stats.filtered_count,
                "failed_count": stats.failed_count,
                "stopped_on_failures": stats.stopped_on_failures,
                "total_bytes": stats.total_bytes,
                "elapsed_secs": stats.elapsed_secs,
                "average_eps": stats.average_eps,
                "average_mbps": stats.average_mbps,
            }))?;
        }
        OutputFormat::Table => {
            println!("\n");
            let mut table = output::table(vec![
                Cell::new("PIPELINE STATISTIC").fg(Color::Cyan),
                Cell::new("VALUE").fg(Color::Cyan),
            ]);

            table.add_row(vec![
                Cell::new("Source Topic").fg(Color::DarkGrey),
                Cell::new(&args.src_topic).fg(Color::Yellow),
            ]);
            table.add_row(vec![
                Cell::new("Destination Topic").fg(Color::DarkGrey),
                Cell::new(&args.dst_topic).fg(Color::Green),
            ]);
            table.add_row(vec![
                Cell::new("Total Messages Mirrored").fg(Color::DarkGrey),
                Cell::new(stats.mirrored_count.to_string()).fg(Color::Cyan),
            ]);
            table.add_row(vec![
                Cell::new("Filtered (Skipped)").fg(Color::DarkGrey),
                Cell::new(stats.filtered_count.to_string()).fg(Color::DarkGrey),
            ]);
            if stats.failed_count > 0 {
                table.add_row(vec![
                    Cell::new("Failed Deliveries").fg(Color::DarkGrey),
                    Cell::new(stats.failed_count.to_string()).fg(Color::Red),
                ]);
            }
            table.add_row(vec![
                Cell::new("Total Data Transferred").fg(Color::DarkGrey),
                Cell::new(format!(
                    "{:.2} MB",
                    crate::model::format::mib(stats.total_bytes)
                ))
                .fg(Color::White),
            ]);
            table.add_row(vec![
                Cell::new("Duration").fg(Color::DarkGrey),
                Cell::new(format!("{:.2} s", stats.elapsed_secs)).fg(Color::Blue),
            ]);
            table.add_row(vec![
                Cell::new("Average Throughput").fg(Color::DarkGrey),
                Cell::new(format!(
                    "{:.1} events/sec ({:.2} MB/s)",
                    stats.average_eps, stats.average_mbps
                ))
                .fg(Color::Green),
            ]);

            println!("{}", table);
        }
    }

    match stats.failure() {
        Some(err) => Err(err),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_clap_mirror_args_defaults() {
        use clap::Parser;

        #[derive(Parser, Debug)]
        struct TestCli {
            #[command(flatten)]
            args: MirrorArgs,
        }

        let cli = TestCli::try_parse_from(["test", "orders-src", "orders-dst"]).unwrap();
        assert_eq!(cli.args.src_topic, "orders-src");
        assert_eq!(cli.args.dst_topic, "orders-dst");
        assert!(!cli.args.from_beginning);
        assert_eq!(cli.args.max_messages, None);
        assert_eq!(cli.args.rate_limit, None);
    }

    #[test]
    fn test_clap_mirror_args_cross_cluster_and_filters() {
        use clap::Parser;

        #[derive(Parser, Debug)]
        struct TestCli {
            #[command(flatten)]
            args: MirrorArgs,
        }

        let cli = TestCli::try_parse_from([
            "test",
            "telemetry",
            "telemetry-backup",
            "--src-profile",
            "prod",
            "--dst-profile",
            "staging",
            "-b",
            "-n",
            "1000",
            "-r",
            "200",
            "-G",
            "CRITICAL",
            "-k",
            "sensor-",
            "--header-filter",
            "env:prod",
        ])
        .unwrap();

        assert_eq!(cli.args.src_topic, "telemetry");
        assert_eq!(cli.args.dst_topic, "telemetry-backup");
        assert_eq!(cli.args.src_profile.as_deref(), Some("prod"));
        assert_eq!(cli.args.dst_profile.as_deref(), Some("staging"));
        assert!(cli.args.from_beginning);
        assert_eq!(cli.args.max_messages, Some(1000));
        assert_eq!(cli.args.rate_limit, Some(200.0));
        assert_eq!(cli.args.grep.as_deref(), Some("CRITICAL"));
        assert_eq!(cli.args.key_filter.as_deref(), Some("sensor-"));
        assert_eq!(cli.args.header_filter, vec!["env:prod".to_string()]);
    }

    use crate::governance::audit::AuditEntry;
    use crate::governance::policy::PolicyStatus;
    use crate::governance::test_policy;
    use crate::kafka::profile::ClusterProfile;
    use std::cell::RefCell;

    /// Collects the gate's audit entries.
    #[derive(Default)]
    struct FakeLog {
        entries: RefCell<Vec<AuditEntry>>,
    }

    impl AuditSink for FakeLog {
        fn record(&self, entry: &AuditEntry) -> std::io::Result<()> {
            self.entries.borrow_mut().push(entry.clone());
            Ok(())
        }

        fn probe(&self) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn global() -> GlobalOpts {
        GlobalOpts {
            profile: None,
            bootstrap_servers: None,
            format: OutputFormat::Table,
            timeout: 30,
            resolved: None,
            write: None,
        }
    }

    fn mirror_args(args: &[&str]) -> MirrorArgs {
        use clap::Parser;

        #[derive(Parser, Debug)]
        struct TestCli {
            #[command(flatten)]
            args: MirrorArgs,
        }

        TestCli::try_parse_from(["test", "orders", "orders-copy"].iter().chain(args))
            .unwrap_or_else(|e| panic!("{args:?} should parse: {e}"))
            .args
    }

    /// `dev` (active, writable), `prod-eu` and `locked` (a read-only profile).
    fn store() -> ProfileStore {
        let mut store = ProfileStore::default();
        store.add(ClusterProfile::new("dev", "localhost:9092"), true);
        store.add(ClusterProfile::new("prod-eu", "kafka.prod:9092"), false);
        let mut locked = ClusterProfile::new("locked", "kafka.locked:9092");
        locked.read_only = true;
        store.add(locked, false);
        store
    }

    fn verified(read_only: &[&str], forbid: &[&str]) -> EffectivePolicy {
        let mut policy = test_policy::none();
        policy.status = PolicyStatus::Verified {
            issuer: "보안팀".into(),
            key_id: "test".into(),
        };
        policy.rules.read_only = read_only.iter().map(|s| s.to_string()).collect();
        policy.rules.forbid = forbid.iter().map(|s| s.to_string()).collect();
        policy
    }

    #[test]
    fn read_only_destination_is_refused_and_recorded() {
        let policy = verified(&["prod-*"], &[]);
        let log = FakeLog::default();
        let args = mirror_args(&["--src-profile", "dev", "--dst-profile", "prod-eu"]);
        let reason = authorize_with(&policy, &log, &store(), &args, &global()).unwrap_err();
        assert!(reason.contains("'prod-eu' is read-only"), "{reason}");
        let entries = log.entries.borrow();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].action, "policy.blocked");
        assert_eq!(entries[0].profile, "prod-eu");
        assert_eq!(entries[0].target, "mirror orders -> orders-copy");
        assert!(!entries[0].ok);

        // A read-only broker address matches too, here from the override.
        let policy = verified(&["kafka.prod:*"], &[]);
        let log = FakeLog::default();
        let args = mirror_args(&["--dst-bootstrap-servers", "kafka.prod:9092"]);
        let reason = authorize_with(&policy, &log, &store(), &args, &global()).unwrap_err();
        assert!(reason.contains("'dev' is read-only"), "{reason}");
        assert_eq!(log.entries.borrow()[0].action, "policy.blocked");

        // The destination defaults to the source profile.
        let policy = verified(&["prod-*"], &[]);
        let log = FakeLog::default();
        let args = mirror_args(&["--src-profile", "prod-eu"]);
        assert!(authorize_with(&policy, &log, &store(), &args, &global()).is_err());
    }

    #[test]
    fn read_only_brokers_match_the_destinations_reported_brokers() {
        let mut policy = verified(&[], &[]);
        policy.rules.read_only_brokers = vec!["*.prod.corp:*".to_string()];
        let args = mirror_args(&["--dst-profile", "prod-eu"]);
        // Reached through a tunnel: the bootstrap does not match, the
        // brokers the cluster reports do.
        let log = FakeLog::default();
        let reason = authorize_checking_brokers(&policy, &log, &store(), &args, &global(), |_| {
            Ok(vec!["b1.prod.corp:9092".to_string()])
        })
        .unwrap_err();
        assert!(reason.contains("read-only"), "{reason}");
        assert_eq!(log.entries.borrow()[0].action, "policy.blocked");
        // Other brokers: allowed; the result line pairs with the request's profile.
        let log = FakeLog::default();
        assert_eq!(
            authorize_checking_brokers(&policy, &log, &store(), &args, &global(), |_| {
                Ok(vec!["b1.dev.corp:9092".to_string()])
            }),
            Ok(Some("prod-eu".to_string()))
        );
        // The brokers cannot be read: refused, not let through.
        let log = FakeLog::default();
        assert!(
            authorize_checking_brokers(&policy, &log, &store(), &args, &global(), |_| {
                Err("unreachable".to_string())
            })
            .is_err()
        );
        assert_eq!(log.entries.borrow()[0].action, "policy.blocked");
    }

    #[test]
    fn export_clusters_match_the_sources_reported_brokers() {
        let mut policy = verified(&[], &[]);
        policy.rules.export = crate::model::policy::ExportRule::Forbidden;
        policy.rules.export_clusters = vec!["*.prod.corp:*".to_string()];
        let args = mirror_args(&["--src-profile", "dev", "--dst-profile", "prod-eu"]);
        let dev = store().get("dev").unwrap().bootstrap_servers.clone();
        // The source is reached through an alias; it reports prod's brokers.
        let log = FakeLog::default();
        let refused = authorize_checking_brokers(&policy, &log, &store(), &args, &global(), |c| {
            Ok(if c.bootstrap_servers == dev {
                vec!["b1.prod.corp:9092".to_string()]
            } else {
                vec!["b1.other:9092".to_string()]
            })
        });
        assert!(refused.is_err(), "{refused:?}");
        // A source that reports other brokers may copy.
        let log = FakeLog::default();
        let allowed = authorize_checking_brokers(&policy, &log, &store(), &args, &global(), |_| {
            Ok(vec!["b1.other:9092".to_string()])
        });
        assert!(allowed.is_ok(), "{allowed:?}");
    }

    #[test]
    fn forbidden_produce_refuses_the_destination() {
        let policy = verified(&[], &["produce"]);
        let log = FakeLog::default();
        let args = mirror_args(&["--dst-profile", "prod-eu"]);
        let reason = authorize_with(&policy, &log, &store(), &args, &global()).unwrap_err();
        assert!(reason.contains("`produce` is forbidden"), "{reason}");
        assert_eq!(log.entries.borrow()[0].action, "policy.blocked");
    }

    #[test]
    fn writable_destination_is_allowed_and_requested() {
        // The read-only rule matches only the source, which mirror reads.
        let policy = verified(&["prod-*"], &[]);
        let log = FakeLog::default();
        let args = mirror_args(&["--src-profile", "prod-eu", "--dst-profile", "dev"]);
        assert_eq!(
            authorize_with(&policy, &log, &store(), &args, &global()),
            Ok(())
        );
        let entries = log.entries.borrow();
        assert_eq!(entries.len(), 1);
        assert_eq!(
            (
                entries[0].action.as_str(),
                entries[0].profile.as_str(),
                entries[0].outcome.as_str()
            ),
            ("mirror", "dev", "requested")
        );

        // Without cluster options the destination is what `main` checked.
        let log = FakeLog::default();
        let args = mirror_args(&[]);
        assert_eq!(
            authorize_with(&policy, &log, &store(), &args, &global()),
            Ok(())
        );
        assert!(log.entries.borrow().is_empty());
    }

    #[test]
    fn read_only_destination_profile_is_refused_without_a_policy() {
        let log = FakeLog::default();
        let args = mirror_args(&["--dst-profile", "locked"]);
        let reason =
            authorize_with(&test_policy::none(), &log, &store(), &args, &global()).unwrap_err();
        assert!(
            reason.contains("Destination profile 'locked' is read-only"),
            "{reason}"
        );
        let entries = log.entries.borrow();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].action, "mirror");
        assert!(!entries[0].ok);
    }

    #[test]
    fn enforced_masking_refuses_mirror_entirely() {
        let policy = test_policy::masking();
        for args in [
            &[][..],
            &["--src-profile", "prod-eu", "--dst-profile", "dev"][..],
        ] {
            let log = FakeLog::default();
            let reason =
                authorize_with(&policy, &log, &store(), &mirror_args(args), &global()).unwrap_err();
            assert!(reason.contains("PII masking is enforced"), "{reason}");
            let entries = log.entries.borrow();
            assert_eq!(entries.len(), 1, "{args:?}");
            assert_eq!(entries[0].action, "policy.blocked");
            assert_eq!(entries[0].target, "mirror orders -> orders-copy");
            assert_eq!(entries[0].outcome, reason);
        }
        // The entry names the source cluster, which mirror reads from.
        let log = FakeLog::default();
        let args = mirror_args(&["--src-profile", "prod-eu"]);
        let _ = authorize_with(&policy, &log, &store(), &args, &global());
        assert_eq!(log.entries.borrow()[0].profile, "prod-eu");

        // No policy, no refusal.
        let log = FakeLog::default();
        assert_eq!(
            authorize_with(
                &test_policy::none(),
                &log,
                &store(),
                &mirror_args(&[]),
                &global()
            ),
            Ok(())
        );
    }

    #[test]
    fn mirror_to_another_cluster_is_an_export() {
        let policy = test_policy::export_forbidden();
        let log = FakeLog::default();
        let args = mirror_args(&["--src-profile", "prod-eu", "--dst-profile", "dev"]);
        let reason = authorize_with(&policy, &log, &store(), &args, &global()).unwrap_err();
        assert!(reason.contains("export: forbidden"), "{reason}");
        let entries = log.entries.borrow();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].action, "policy.blocked");
        assert_eq!(entries[0].profile, "prod-eu");
        assert_eq!(
            entries[0].target,
            "export:cluster mirror orders -> orders-copy (to dev)"
        );
        drop(entries);

        // A source given by address is another cluster too.
        let log = FakeLog::default();
        let args = mirror_args(&["--src-bootstrap-servers", "kafka.prod:9092"]);
        assert!(authorize_with(&policy, &log, &store(), &args, &global()).is_err());

        // The same brokers under another name are the same cluster: only the
        // write gate runs.
        for args in [
            &[][..],
            &["--dst-bootstrap-servers", "LOCALHOST"][..],
            &["--src-profile", "dev", "--dst-profile", "dev"][..],
        ] {
            let log = FakeLog::default();
            let args = mirror_args(args);
            assert_eq!(
                authorize_with(&policy, &log, &store(), &args, &global()),
                Ok(()),
                "{args:?}"
            );
            assert!(log
                .entries
                .borrow()
                .iter()
                .all(|e| e.action != "policy.blocked"));
        }

        // `export_clusters` names the source's brokers.
        let mut dev_only = policy.clone();
        dev_only.rules.export_clusters = vec!["kafka.prod".into()];
        let log = FakeLog::default();
        let args = mirror_args(&["--src-profile", "dev", "--dst-profile", "prod-eu"]);
        // The source's brokers are read for the rule: here, what it is
        // configured with.
        assert!(
            authorize_checking_brokers(&dev_only, &log, &store(), &args, &global(), |c| {
                Ok(vec![c.bootstrap_servers.clone()])
            })
            .is_ok()
        );

        // Audited: recorded, then the destination's write gate.
        let log = FakeLog::default();
        let args = mirror_args(&["--src-profile", "prod-eu", "--dst-profile", "dev"]);
        assert_eq!(
            authorize_with(
                &test_policy::export_audited(),
                &log,
                &store(),
                &args,
                &global()
            ),
            Ok(())
        );
        let actions: Vec<String> = log
            .entries
            .borrow()
            .iter()
            .map(|e| e.action.clone())
            .collect();
        assert_eq!(actions, vec!["record.export", "mirror"]);
    }

    #[test]
    fn unknown_mirror_profiles_are_refused() {
        let policy = test_policy::none();
        for flag in ["--src-profile", "--dst-profile"] {
            let log = FakeLog::default();
            let args = mirror_args(&[flag, "nope"]);
            let reason = authorize_with(&policy, &log, &store(), &args, &global()).unwrap_err();
            assert!(reason.contains("'nope' does not exist"), "{reason}");
        }
    }
}
