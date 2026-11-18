//! Command line: arguments, the policy and audit gates in front of a command,
//! and the dispatch to the command handlers.

use clap::{Parser, Subcommand};

use crate::{cli, governance, kafka, model};

#[derive(Parser, Debug)]
#[command(name = "ferrika")]
#[command(author = "Ferrika Contributors")]
#[command(version = "0.1.0")]
#[command(about = "Ultra-lightweight & high-performance native Kafka client in Rust", long_about = None)]
pub struct Cli {
    #[command(flatten)]
    pub global_opts: cli::GlobalOpts,

    #[command(subcommand)]
    pub command: Option<Commands>,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Manage Kafka topics (CLI mode)
    Topics(cli::topics::TopicsArgs),
    /// Produce messages to Kafka topics (CLI mode)
    Produce(cli::produce::ProduceArgs),
    /// Consume messages from Kafka topics (CLI mode)
    Consume(cli::consume::ConsumeArgs),
    /// Real-time monitoring of Consumer Groups and Brokers (CLI mode)
    Monitor(cli::monitor::MonitorArgs),
    /// Manage Kafka cluster connection profiles (CLI mode)
    Profile(cli::profile::ProfileArgs),
    /// High-throughput performance benchmark suite (CLI mode)
    Bench(cli::bench::BenchArgs),
    /// Confluent Schema Registry inspection and wire-format decoding (CLI mode)
    Schema(cli::schema::SchemaArgs),
    /// Generate synthetic domain mock events (e.g. ecommerce, iot, web, financial) (CLI mode)
    Generate(cli::generator::GenerateArgs),
    /// Replicate and pipe messages between topics or cross-cluster (CLI mode)
    Mirror(cli::mirror::MirrorArgs),
    /// Inspect and reset consumer group offsets (CLI mode)
    Offset(cli::offset::OffsetArgs),
    /// Dump topic data to portable JSON Lines (.jsonl) file (CLI mode)
    Dump(cli::backup::DumpArgs),
    /// Restore topic data from portable JSON Lines (.jsonl) file (CLI mode)
    Restore(cli::backup::RestoreArgs),
    /// Manage and audit Kafka Access Control Lists (ACLs) and cluster security (CLI mode)
    Acl(cli::acl::AclArgs),
    /// List, set and remove client quotas (byte rates, request share) (CLI mode)
    Quota(cli::quota::QuotaArgs),
    /// List, create and delete SASL/SCRAM users (CLI mode)
    User(cli::user::UserArgs),
    /// Plan, execute, verify, and rollback partition reassignments across brokers (CLI mode)
    Reassign(cli::reassign::ReassignArgs),
    /// Inspect topic log compaction efficiency, dirty ratios, and snapshot key-value state (CLI mode)
    Compact(cli::compact::CompactArgs),
    /// Manage Dead Letter Queues (DLQ), inspect failures, and redrive poison pill records (CLI mode)
    Dlq(cli::dlq::DlqArgs),
    /// Inspect distributed tracing headers (W3C/B3/Jaeger), audit coverage, and inject trace context (CLI mode)
    Trace(cli::trace::TraceArgs),
    /// Compare records and schema parity between two Kafka topics (CLI mode)
    Diff(cli::diff::DiffArgs),
    /// Measure end-to-end produce-to-consume latency, jitter, and packet loss with canary heartbeat probes (CLI mode)
    Probe(cli::probe::ProbeArgs),
    /// Real-time terminal cluster monitor with throughput deltas, lag leaderboards, and broker stats (CLI mode)
    Top(cli::top::TopArgs),
    /// Manage, ping, and check synchronization across multiple Kafka clusters (CLI mode)
    Cluster(cli::cluster_sync::ClusterArgs),
    /// Field-level PII masking and sensitive data security compliance auditor (CLI mode)
    Crypto(cli::crypto::CryptoArgs),
    /// Inspect cluster topology, broker workload, rack distribution, and under-replicated partitions (CLI mode)
    Topology(cli::topology::TopologyArgs),
    /// Check cluster health (offline brokers, under-replication, leader skew, idle lag) for scripts and CI (CLI mode)
    Health(cli::health::HealthArgs),
    /// Check topic, broker and consumer group configs for risky or inconsistent values, and fix them (CLI mode)
    Lint(cli::lint::LintArgs),
    /// Write one Markdown incident report: health, lag and its trend, audit timeline (CLI mode)
    Report(cli::report::ReportArgs),
    /// List and control Kafka Connect connectors (CLI mode)
    Connect(cli::connect::ConnectArgs),
    /// Serve cluster tools to AI assistants over MCP (stdin/stdout); read-only unless --allow-write
    Mcp(cli::mcp::McpArgs),
    /// Show recent operations that changed a cluster, from the GUI and the CLI (CLI mode)
    Audit {
        /// Number of entries to show
        #[arg(short = 'n', long, default_value_t = 30)]
        limit: usize,
        #[command(subcommand)]
        command: Option<cli::audit::AuditCommand>,
    },
    /// Signed company policy: keygen, sign, verify, show (CLI mode)
    Policy(cli::policy::PolicyArgs),
    /// Cluster changes recorded for undo, newest first; `history show <id>` for one (CLI mode)
    History(cli::history::HistoryArgs),
    /// Put a recorded change back: checks the state first, refuses on drift unless --force (CLI mode)
    Undo(cli::history::UndoArgs),
}

/// Sends `tracing` output and the library's notices (`warning: ...`) to
/// stderr. The `tracing` filter comes from `FERRIKA_LOG`, then
/// `RUST_LOG` (e.g. `FERRIKA_LOG=ferrika=debug`), and defaults to errors only so
/// CLI output stays clean. `quiet_driver` also hides librdkafka's own errors
/// by default, for commands that report connection problems themselves.
pub fn init_tracing(quiet_driver: bool) {
    // The library's warnings for the user (policy expiry, audit forwarding)
    // go to stderr like the CLI's own.
    crate::notice::set_handler(|message| eprintln!("warning: {message}"));
    let default = if quiet_driver {
        "error,librdkafka=off,rdkafka=off"
    } else {
        "error"
    };
    let filter = tracing_subscriber::EnvFilter::try_from_env("FERRIKA_LOG")
        .or_else(|_| tracing_subscriber::EnvFilter::try_from_default_env())
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(default));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .try_init();
}

/// Whether a panic message is std's report of a write to a closed stdout or
/// stderr (`ferrika ... | head`, `2>&1 | head`): println!/eprintln! panic with
/// "failed printing to stdout" / "failed printing to stderr".
fn is_closed_pipe_panic(message: &str) -> bool {
    message.contains("failed printing to stdout") || message.contains("failed printing to stderr")
}

/// `ferrika ... | head` closes stdout early; println! then panics with
/// "failed printing to stdout" (and eprintln! on a closed stderr with
/// "failed printing to stderr"; with `panic = "abort"` that was exit code 134).
/// Treat both as a normal end of output (exit code 0, like `exit_with_error`).
pub fn exit_quietly_on_broken_pipe() {
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let message = info
            .payload()
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| info.payload().downcast_ref::<&str>().copied())
            .unwrap_or("");
        if is_closed_pipe_panic(message) {
            std::process::exit(0);
        }
        default_hook(info);
    }));
}

/// The text shown for a command line without a command.
fn no_command_help() -> String {
    <Cli as clap::CommandFactory>::command()
        .render_help()
        .to_string()
}

/// Entry point of a CLI-only binary: parses the command line, runs the
/// command, and prints the help (exit status 0) when there is none.
pub fn main_cli() {
    exit_quietly_on_broken_pipe();
    run(Cli::parse());
}

/// Runs a CLI command line: the policy and audit gates, then the command.
/// Without a command it prints the help and returns.
pub fn run(mut cli: Cli) {
    if cli.command.is_none() {
        print!("{}", no_command_help());
        return;
    }
    // `health --watch` turns an unreachable broker into a finding; the
    // driver's retry errors would only interleave with it.
    // MCP clients log stderr; driver retries there are noise too. `report`
    // puts an unreachable cluster in the document itself.
    init_tracing(
        matches!(
            &cli.command,
            Some(Commands::Health(args)) if args.watch.is_some()
        ) || matches!(cli.command, Some(Commands::Mcp(_) | Commands::Report(_))),
    );

    let policy = governance::policy::current();
    match &cli.command {
        Some(Commands::Policy(_)) | Some(Commands::Audit { .. }) => {}
        None => {}
        Some(_) => governance::policy::record_if_invalid("cli"),
    }
    // Resolve the profile once, before the policy gate reads its name: a
    // broken profiles.json or an unknown --profile stops the command instead
    // of running with no profile name (which no read_only glob matches).
    if needs_profiles(cli.command.as_ref()) {
        match cli::context::resolve(&cli.global_opts) {
            Ok(context) => cli.global_opts.resolved = Some(std::sync::Arc::new(context)),
            Err(err) => cli::exit_with_error(&err),
        }
    }
    // Record data leaving the app: refused under `export: forbidden` (before
    // the masking refusal of `dump` below, so that is the reason shown),
    // recorded under `export: audited`.
    if let Some(gate) = cli.command.as_ref().and_then(export_gate) {
        let profile = cli.global_opts.resolved_profile_name().unwrap_or_default();
        // The brokers the cluster reports too, only when the policy matches
        // broker addresses (`export_clusters`): else no request is made.
        let bootstrap = match crate::governance::policy::brokers_for(
            &cli.global_opts.to_kafka_config(),
            policy.export_needs_broker_metadata(),
        ) {
            Ok(brokers) => brokers,
            Err(reason) => {
                governance::policy::record_blocked("cli", &profile, gate.label, &reason);
                cli::exit_with_error(&reason)
            }
        };
        let request = governance::policy::ExportRequest {
            source: "cli",
            channel: gate.channel,
            label: gate.label,
            target: &gate.target,
            records: None,
            profile: &profile,
            bootstrap: &bootstrap,
            actor: None,
        };
        let terminal = std::io::IsTerminal::is_terminal(&std::io::stdout());
        if let Err(reason) = cli::authorize_record_export(&request, terminal) {
            cli::exit_with_error(&reason);
        }
        if gate.channel != governance::policy::ExportChannel::Stdout || !terminal {
            cli::note_export(policy, &bootstrap);
            cli::remember_export(&request);
        }
    }
    if policy.masking_enforced() {
        if let Some(Commands::Dump(_)) = cli.command {
            let reason = format!(
                "Refused by {}: PII masking is enforced, so raw topic dumps are not allowed.",
                policy.label()
            );
            let profile = cli
                .global_opts
                .resolved_profile_name()
                .unwrap_or_else(|| cli.global_opts.resolved_bootstrap_servers());
            governance::policy::record_blocked("cli", &profile, "dump", &reason);
            cli::exit_with_error(&reason);
        }
    }
    // `mirror` writes to its destination cluster, which the gate below does
    // not see; it is refused outright under enforced masking.
    if let Some(Commands::Mirror(args)) = &cli.command {
        if let Err(refusal) = cli::mirror::authorize(args, &cli.global_opts) {
            cli::exit_with_error(&refusal);
        }
    }

    if let Some(action) = cli.command.as_ref().and_then(write_action) {
        let profile = cli.global_opts.resolved_profile_name().unwrap_or_default();
        // The brokers the cluster reports too (a tunnel or IP address must
        // not dodge `read_only_brokers`), only when the policy has such a rule.
        let bootstrap = match crate::governance::policy::brokers_for(
            &cli.global_opts.to_kafka_config(),
            policy.write_needs_broker_metadata(),
        ) {
            Ok(brokers) => brokers,
            Err(reason) => {
                governance::policy::record_blocked("cli", &profile, action, &reason);
                cli::exit_with_error(&reason)
            }
        };
        let target = cli.command.as_ref().map(write_target).unwrap_or_default();
        // The id is made here so the request entry can name the operation
        // the handler records (`op`); its result entry adds `op_digest`.
        let op = records_history(action).then(|| model::history::new_op_id(kafka::clock::now_ms()));
        let request = governance::policy::WriteRequest {
            source: "cli",
            action: model::policy::PolicyAction::from_cli(action),
            label: action,
            target: &target,
            profile: &profile,
            bootstrap: &bootstrap,
            actor: None,
            op: op.as_deref(),
        };
        // The CLI exits from many places, so its entries record the request;
        // the GUI records each outcome.
        if let Err(refusal) = governance::policy::authorize_write(
            &request,
            governance::policy::AuditMode::RecordRequest,
            || {
                cli.global_opts
                    .ensure_writable(action)
                    .map_err(|e| e.to_string())
            },
        ) {
            cli::exit_with_error(&refusal_message(&refusal, cli.command.as_ref()));
        }
        cli.global_opts.write = Some(cli::WriteContext {
            label: action.to_string(),
            target,
            op,
        });
    }

    let global = &cli.global_opts;
    match cli.command {
        Some(Commands::Topics(args)) => run_async(cli::topics::run(args, global)),
        Some(Commands::Produce(args)) => run_async(cli::produce::run(args, global)),
        Some(Commands::Consume(args)) => run_async(cli::consume::run(args, global)),
        Some(Commands::Monitor(args)) => run_async(cli::monitor::run(args, global)),
        Some(Commands::Profile(args)) => run_async(cli::profile::run(args, global)),
        Some(Commands::Bench(args)) => run_async(cli::bench::run(args, global)),
        Some(Commands::Schema(args)) => run_async(cli::schema::run(args, global)),
        Some(Commands::Generate(args)) => run_async(cli::generator::run(args, global)),
        Some(Commands::Mirror(args)) => run_async(cli::mirror::run(args, global)),
        Some(Commands::Offset(args)) => run_async(cli::offset::run(args, global)),
        Some(Commands::Dump(args)) => run_async(cli::backup::run_dump(args, global)),
        Some(Commands::Restore(args)) => run_async(cli::backup::run_restore(args, global)),
        Some(Commands::Acl(args)) => exit_on_error(cli::acl::run(args, global)),
        Some(Commands::Quota(args)) => exit_on_error(cli::quota::run(args, global)),
        Some(Commands::User(args)) => exit_on_error(cli::user::run(args, global)),
        Some(Commands::Reassign(args)) => exit_on_error(cli::reassign::run(args, global)),
        Some(Commands::Compact(args)) => run_async(cli::compact::run(args, global)),
        Some(Commands::Dlq(args)) => run_async(cli::dlq::handle_dlq(args, global)),
        Some(Commands::Trace(args)) => run_async(cli::trace::run(args, global)),
        Some(Commands::Diff(args)) => run_async(cli::diff::run(args, global)),
        Some(Commands::Probe(args)) => run_async(cli::probe::run(args, global)),
        Some(Commands::Top(args)) => run_async(cli::top::run(args, global)),
        Some(Commands::Cluster(args)) => {
            run_async(cli::cluster_sync::handle_cluster_command(args, global))
        }
        Some(Commands::Crypto(args)) => run_async(cli::crypto::handle_crypto_command(args, global)),
        Some(Commands::Topology(args)) => run_async(cli::topology::run(args, global)),
        Some(Commands::Audit { limit, command }) => cli::audit::run(limit, command, global),
        Some(Commands::Policy(args)) => exit_on_error(cli::policy::run(args, global)),
        Some(Commands::Health(args)) => run_async(cli::health::run(args, global)),
        Some(Commands::Lint(args)) => run_async(cli::lint::run(args, global)),
        Some(Commands::Report(args)) => run_async(cli::report::run(args, global)),
        Some(Commands::Connect(args)) => run_async(cli::connect::run(args, global)),
        Some(Commands::History(args)) => run_async(cli::history::run_history(args, global)),
        Some(Commands::Undo(args)) => run_async(cli::history::run_undo(args, global)),
        Some(Commands::Mcp(args)) => run_async(cli::mcp::run(args, global)),
        None => print!("{}", no_command_help()),
    }
}

/// Runs an async CLI command on a fresh tokio runtime, exiting with the
/// command's error (see [`cli::exit_with_error`]) if it fails. Accepts both
/// `KafkaResult` and `anyhow::Result` commands.
fn run_async<F, E>(command: F)
where
    F: std::future::Future<Output = Result<(), E>>,
    E: std::fmt::Display,
{
    let rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("Failed to initialize async runtime: {e}");
            std::process::exit(1);
        }
    };
    exit_on_error(rt.block_on(command));
}

/// Exits with the command's error (see [`cli::exit_with_error`]) if it failed.
fn exit_on_error<E: std::fmt::Display>(result: Result<(), E>) {
    if let Err(err) = result {
        cli::exit_with_error(&err);
    }
}

/// Whether `command` uses the cluster profiles, so `main` resolves a
/// [`cli::ResolvedContext`] for it. `profile` manages the store itself (and
/// reports a broken file with its path), and `policy` and `audit` never
/// connect.
fn needs_profiles(command: Option<&Commands>) -> bool {
    !matches!(
        command,
        None | Some(Commands::Profile(_) | Commands::Policy(_) | Commands::Audit { .. })
    )
}

/// The command line a read-only profile refuses, or `None` for commands that
/// only read the cluster (or only preview a change).
///
/// There is no wildcard arm, at any level, on purpose: a new command or
/// subcommand stops this from compiling until its author decides whether it
/// writes, so a policy check cannot be skipped by forgetting to register it.
fn write_action(command: &Commands) -> Option<&'static str> {
    use cli::acl::AclSubcommands;
    use cli::offset::OffsetSubcommands;
    use cli::reassign::ReassignSubcommands;
    use cli::topics::TopicsSubcommands;
    match command {
        Commands::Topics(args) => match &args.command {
            TopicsSubcommands::Create { .. } => Some("topics create"),
            TopicsSubcommands::Clone { .. } => Some("topics clone"),
            TopicsSubcommands::Delete { .. } => Some("topics delete"),
            TopicsSubcommands::Purge { .. } => Some("topics purge"),
            TopicsSubcommands::SetReplication { .. } => Some("topics set-replication"),
            TopicsSubcommands::ElectLeaders { .. } => Some("topics elect-leaders"),
            TopicsSubcommands::Config {
                set,
                delete,
                dry_run: false,
                ..
            } if !set.is_empty() || !delete.is_empty() => Some("topics config"),
            // Without changes, or only a preview (--dry-run).
            TopicsSubcommands::Config { .. } => None,
            TopicsSubcommands::List { .. }
            | TopicsSubcommands::Describe { .. }
            | TopicsSubcommands::Analyze { .. }
            | TopicsSubcommands::Histogram { .. } => None,
        },
        Commands::Produce(_) => Some("produce"),
        // With a group, the offsets of the records read are committed.
        Commands::Consume(args) if args.group.is_some() => Some("consume --group"),
        Commands::Consume(_) => None,
        Commands::Generate(_) => Some("generate"),
        Commands::Mirror(_) => Some("mirror"),
        Commands::Restore(_) => Some("restore"),
        Commands::Probe(_) => Some("probe"),
        Commands::Offset(args) => match &args.command {
            OffsetSubcommands::Reset(reset) if reset.execute && !reset.dry_run => {
                Some("offset reset --execute")
            }
            // A preview, or --execute left out.
            OffsetSubcommands::Reset(_) => None,
            OffsetSubcommands::DeleteGroup(_) => Some("offset delete-group"),
            OffsetSubcommands::Delete(_) => Some("offset delete"),
        },
        Commands::User(args) => match &args.command {
            cli::user::UserSubcommands::Create { .. } => Some("user create"),
            cli::user::UserSubcommands::Delete { .. } => Some("user delete"),
            cli::user::UserSubcommands::List => None,
        },
        Commands::Quota(args) => match &args.command {
            cli::quota::QuotaSubcommands::Set { .. } => Some("quota set"),
            cli::quota::QuotaSubcommands::Remove { .. } => Some("quota remove"),
            cli::quota::QuotaSubcommands::List => None,
        },
        Commands::Acl(args) if !args.local => match &args.command {
            AclSubcommands::Create(_) => Some("acl create"),
            AclSubcommands::Delete(delete) if delete.force => Some("acl delete --force"),
            // Without --force, delete only lists what it would remove.
            AclSubcommands::Delete(_) => None,
            AclSubcommands::List(_) | AclSubcommands::Audit(_) => None,
        },
        // `--local` works on the local ACL store, never the cluster.
        Commands::Acl(_) => None,
        Commands::Reassign(args) => match &args.command {
            ReassignSubcommands::Execute(exec) if exec.execute => Some("reassign execute"),
            ReassignSubcommands::Rollback(_) => Some("reassign rollback"),
            // Without --execute, execute only previews.
            ReassignSubcommands::Execute(_) => None,
            ReassignSubcommands::Plan(_)
            | ReassignSubcommands::Verify(_)
            | ReassignSubcommands::Status => None,
        },
        Commands::Dlq(args) => match &args.command {
            cli::dlq::DlqSubcommands::Redrive(redrive) if redrive.execute => {
                Some("dlq redrive --execute")
            }
            // Without --execute, redrive only previews.
            cli::dlq::DlqSubcommands::Redrive(_) => None,
            cli::dlq::DlqSubcommands::Inspect(_) | cli::dlq::DlqSubcommands::Export(_) => None,
        },
        Commands::Bench(args) => match &args.command {
            cli::bench::BenchSubcommands::Produce(_) => Some("bench produce"),
            cli::bench::BenchSubcommands::Consume(_) => None,
        },
        Commands::Schema(args) => match &args.command {
            cli::schema::SchemaSubcommands::Register { .. } => Some("schema register"),
            cli::schema::SchemaSubcommands::Compat { set: Some(_), .. } => {
                Some("schema compat --set")
            }
            cli::schema::SchemaSubcommands::DeleteSubject { .. } => Some("schema delete-subject"),
            // Without --set, compat only shows the level.
            cli::schema::SchemaSubcommands::Compat { .. } => None,
            cli::schema::SchemaSubcommands::List { .. }
            | cli::schema::SchemaSubcommands::Get { .. }
            | cli::schema::SchemaSubcommands::Subject { .. }
            | cli::schema::SchemaSubcommands::Fields { .. }
            | cli::schema::SchemaSubcommands::Decode { .. }
            | cli::schema::SchemaSubcommands::Check { .. }
            | cli::schema::SchemaSubcommands::DiffSchema { .. } => None,
        },
        // Undo's policy category depends on the recorded operation, so it
        // runs the gate itself, once per category of its steps.
        Commands::Undo(_) => None,
        // Each fix passes the write gate on its own, see `lint_is_gated_per_fix`.
        Commands::Lint(_) => None,
        // Each MCP write tool call passes the gate itself.
        Commands::Mcp(_) => None,
        Commands::Connect(args) => match &args.command {
            cli::connect::ConnectSubcommands::Pause { .. } => Some("connect pause"),
            cli::connect::ConnectSubcommands::Resume { .. } => Some("connect resume"),
            cli::connect::ConnectSubcommands::Restart { .. } => Some("connect restart"),
            cli::connect::ConnectSubcommands::RestartTask { .. } => Some("connect restart-task"),
            cli::connect::ConnectSubcommands::Apply { .. } => Some("connect apply"),
            cli::connect::ConnectSubcommands::Delete { .. } => Some("connect delete"),
            cli::connect::ConnectSubcommands::List
            | cli::connect::ConnectSubcommands::Status { .. }
            | cli::connect::ConnectSubcommands::Plugins
            | cli::connect::ConnectSubcommands::Validate { .. } => None,
        },
        // Read the cluster, show metadata, or only touch local files.
        Commands::Monitor(_)
        | Commands::Profile(_)
        | Commands::Dump(_)
        | Commands::Compact(_)
        | Commands::Trace(_)
        | Commands::Diff(_)
        | Commands::Top(_)
        | Commands::Cluster(_)
        | Commands::Crypto(_)
        | Commands::Topology(_)
        | Commands::Health(_)
        | Commands::Report(_)
        | Commands::History(_)
        | Commands::Audit { .. }
        | Commands::Policy(_) => None,
    }
}

/// Whether the handler of write command `label` (from [`write_action`])
/// records its change in the operation history. Produce-like commands have
/// nothing to undo and are not recorded; neither is `reassign rollback`,
/// which only saves a plan.
fn records_history(label: &str) -> bool {
    const RECORDED: [&str; 8] = [
        "topics ",
        "offset ",
        "user ",
        "quota ",
        "acl ",
        "schema ",
        "connect ",
        "reassign execute",
    ];
    RECORDED.iter().any(|prefix| label.starts_with(prefix))
}

/// A command that takes record data (keys, payloads, header values) out of
/// the app, for the export gate in `main`.
#[derive(Debug, PartialEq, Eq)]
struct ExportGate {
    channel: governance::policy::ExportChannel,
    /// The command as the audit log names it.
    label: &'static str,
    /// Topics only, never the output path.
    target: String,
}

/// Where `command` writes record data: a file, or stdout (an export only
/// when redirected). `None` for commands that show only metadata or counts,
/// read records in, or are checked elsewhere (`mirror`, `mcp`, `crypto
/// audit`).
///
/// Like `write_action`, it has no wildcard arm: a new command or subcommand
/// stops this from compiling until its author decides whether it exports.
fn export_gate(command: &Commands) -> Option<ExportGate> {
    use governance::policy::ExportChannel::{File, Stdout};
    let gate = |channel, label, target: &str| {
        Some(ExportGate {
            channel,
            label,
            target: target.to_string(),
        })
    };
    match command {
        Commands::Dump(a) => gate(File, "dump", &a.topic),
        Commands::Consume(a) if a.save.is_some() => gate(File, "consume --save", &a.topic),
        Commands::Consume(a) => gate(Stdout, "consume", &a.topic),
        Commands::Dlq(args) => match &args.command {
            cli::dlq::DlqSubcommands::Export(a) => gate(File, "dlq export", &a.topic),
            cli::dlq::DlqSubcommands::Inspect(a) => gate(Stdout, "dlq inspect", &a.topic),
            cli::dlq::DlqSubcommands::Redrive(_) => None,
        },
        Commands::Compact(args) => match &args.command {
            cli::compact::CompactSubcommands::Snapshot(a) => {
                gate(File, "compact snapshot", &a.topic)
            }
            cli::compact::CompactSubcommands::Tombstones(a) => {
                gate(Stdout, "compact tombstones", &a.topic)
            }
            cli::compact::CompactSubcommands::Analyze(_) => None,
        },
        Commands::Diff(a) => gate(Stdout, "diff", &format!("{} vs {}", a.topic_a, a.topic_b)),
        Commands::Trace(args) => match &args.command {
            cli::trace::TraceSubcommands::Inspect(a) => gate(Stdout, "trace inspect", &a.topic),
            cli::trace::TraceSubcommands::Audit(_) | cli::trace::TraceSubcommands::Inject(_) => {
                None
            }
        },
        // `crypto mask` from stdin masks what the user typed in.
        Commands::Crypto(args) => match &args.command {
            cli::crypto::CryptoCommands::Mask(cli::crypto::CryptoMaskArgs {
                topic: Some(topic),
                ..
            }) => gate(Stdout, "crypto mask", topic),
            cli::crypto::CryptoCommands::Mask(_) | cli::crypto::CryptoCommands::Audit(_) => None,
        },
        // Metadata or counts only, records read in, or checked elsewhere.
        Commands::Mirror(_) | Commands::Mcp(_) => None,
        Commands::Topics(_)
        | Commands::Produce(_)
        | Commands::Monitor(_)
        | Commands::Profile(_)
        | Commands::Bench(_)
        | Commands::Schema(_)
        | Commands::Generate(_)
        | Commands::Offset(_)
        | Commands::Restore(_)
        | Commands::Acl(_)
        | Commands::Quota(_)
        | Commands::User(_)
        | Commands::Reassign(_)
        | Commands::Probe(_)
        | Commands::Top(_)
        | Commands::Cluster(_)
        | Commands::Topology(_)
        | Commands::Health(_)
        | Commands::Lint(_)
        | Commands::Report(_)
        | Commands::Connect(_)
        | Commands::History(_)
        | Commands::Undo(_)
        | Commands::Audit { .. }
        | Commands::Policy(_) => None,
    }
}

/// A refused write's message, with a way to do the same without the write
/// where there is one.
fn refusal_message(refusal: &dyn std::fmt::Display, command: Option<&Commands>) -> String {
    match command {
        Some(Commands::Consume(args)) if args.group.is_some() => format!(
            "{refusal}\nConsuming with --group commits the group's offsets. Run without \
             --group to read without committing (add --from-beginning to start at the \
             earliest offset)."
        ),
        _ => refusal.to_string(),
    }
}

/// What a changing command applies to, for the audit log: topics, groups,
/// principals — never payloads or config values.
fn write_target(command: &Commands) -> String {
    use cli::acl::AclSubcommands;
    use cli::offset::OffsetSubcommands;
    use cli::reassign::ReassignSubcommands;
    use cli::topics::TopicsSubcommands;
    let resource =
        |topic: &Option<String>, group: &Option<String>, cluster: bool| match (topic, group) {
            (Some(t), _) => format!("topic {t}"),
            (_, Some(g)) => format!("group {g}"),
            _ if cluster => "cluster".to_string(),
            _ => "any resource".to_string(),
        };
    match command {
        Commands::Topics(args) => match &args.command {
            TopicsSubcommands::Create { name, .. } | TopicsSubcommands::Delete { name, .. } => {
                name.clone()
            }
            TopicsSubcommands::Clone { source, target, .. } => format!("{target} from {source}"),
            TopicsSubcommands::Purge { topic, .. } => topic.clone(),
            TopicsSubcommands::SetReplication {
                topic, replication, ..
            } => format!("{topic} to replication {replication}"),
            TopicsSubcommands::ElectLeaders { topics } if topics.is_empty() => {
                "all partitions".to_string()
            }
            TopicsSubcommands::ElectLeaders { topics } => topics.join(", "),
            TopicsSubcommands::Config {
                topic, set, delete, ..
            } => {
                let keys: Vec<String> = set
                    .iter()
                    .map(|kv| kv.split('=').next().unwrap_or_default().to_string())
                    .chain(delete.iter().map(|k| format!("{k} (reset)")))
                    .collect();
                format!("{topic}: {}", keys.join(", "))
            }
            _ => String::new(),
        },
        Commands::Produce(a) => a.topic.clone(),
        Commands::Consume(a) => match &a.group {
            Some(group) => format!("{group} on {}", a.topic),
            None => String::new(),
        },
        Commands::Generate(a) => a.topic.clone(),
        Commands::Mirror(a) => format!("{} -> {}", a.src_topic, a.dst_topic),
        Commands::Restore(a) => format!("{} from {}", a.topic, a.input),
        Commands::Probe(a) => a.topic.clone(),
        Commands::Offset(args) => match &args.command {
            OffsetSubcommands::Reset(r) => format!("{} on {}", r.group_id, r.topic),
            OffsetSubcommands::DeleteGroup(d) => d.groups.join(", "),
            OffsetSubcommands::Delete(d) => format!("{} on {}", d.group_id, d.topic),
        },
        // Never the password.
        Commands::User(args) => match &args.command {
            cli::user::UserSubcommands::Create {
                name, mechanism, ..
            }
            | cli::user::UserSubcommands::Delete { name, mechanism } => {
                format!("user {name} ({mechanism})")
            }
            cli::user::UserSubcommands::List => String::new(),
        },
        Commands::Quota(args) => match &args.command {
            cli::quota::QuotaSubcommands::Set { entity, key, value } => entity
                .entity()
                .map(|e| {
                    format!(
                        "{key}={value} for {}",
                        crate::model::quota::quota_entity_label(&e)
                    )
                })
                .unwrap_or_default(),
            cli::quota::QuotaSubcommands::Remove { entity, key } => entity
                .entity()
                .map(|e| format!("{key} for {}", crate::model::quota::quota_entity_label(&e)))
                .unwrap_or_default(),
            cli::quota::QuotaSubcommands::List => String::new(),
        },
        Commands::Acl(args) => match &args.command {
            AclSubcommands::Create(c) => format!(
                "{} {} on {} for {}",
                if c.deny { "DENY" } else { "ALLOW" },
                c.operation,
                resource(&c.topic, &c.group, c.cluster),
                c.principal
            ),
            AclSubcommands::Delete(d) => format!(
                "{} for {}",
                resource(&d.topic, &d.group, d.cluster),
                d.principal.as_deref().unwrap_or("any principal")
            ),
            _ => String::new(),
        },
        Commands::Reassign(args) => match &args.command {
            ReassignSubcommands::Execute(e) => e.plan.display().to_string(),
            ReassignSubcommands::Rollback(r) => r.topics.join(", "),
            _ => String::new(),
        },
        Commands::Dlq(args) => match &args.command {
            cli::dlq::DlqSubcommands::Redrive(r) => match &r.target_topic {
                Some(target) => format!("{} -> {target}", r.topic),
                None => r.topic.clone(),
            },
            _ => String::new(),
        },
        Commands::Bench(args) => match &args.command {
            cli::bench::BenchSubcommands::Produce(b) => b.topic.clone(),
            _ => String::new(),
        },
        Commands::Schema(args) => match &args.command {
            cli::schema::SchemaSubcommands::Register { subject, .. }
            | cli::schema::SchemaSubcommands::DeleteSubject { subject, .. } => subject.clone(),
            cli::schema::SchemaSubcommands::Compat {
                subject,
                set: Some(level),
                ..
            } => format!("{subject} to {level}"),
            _ => String::new(),
        },
        Commands::Connect(args) => match &args.command {
            cli::connect::ConnectSubcommands::Pause { name }
            | cli::connect::ConnectSubcommands::Resume { name }
            | cli::connect::ConnectSubcommands::Restart { name }
            | cli::connect::ConnectSubcommands::RestartTask { name, .. }
            | cli::connect::ConnectSubcommands::Delete { name, .. } => format!("connector {name}"),
            cli::connect::ConnectSubcommands::Apply { file, name } => match name {
                Some(name) => format!("connector {name} from {file}"),
                None => format!("connector from {file}"),
            },
            _ => String::new(),
        },
        Commands::Undo(a) => match &a.id {
            Some(id) => format!("op {id}"),
            None => "the latest undoable operation".to_string(),
        },
        _ => String::new(),
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn closed_stdout_and_stderr_panics_are_recognised() {
        assert!(is_closed_pipe_panic(
            "failed printing to stdout: Broken pipe (os error 32)"
        ));
        assert!(is_closed_pipe_panic(
            "failed printing to stderr: Broken pipe (os error 32)"
        ));
        assert!(!is_closed_pipe_panic("index out of bounds"));
    }

    /// Every command, with whether it can change a cluster. `write_action`
    /// is exhaustive itself; this keeps `write_target` (audit text, which
    /// still falls back to an empty string) in step: a new `Commands`
    /// variant stops this test from compiling until someone decides whether
    /// `write_target` must cover it.
    fn may_write(command: &Commands) -> bool {
        match command {
            Commands::Topics(_)
            | Commands::Produce(_)
            | Commands::Consume(_)
            | Commands::Generate(_)
            | Commands::Mirror(_)
            | Commands::Restore(_)
            | Commands::Probe(_)
            | Commands::Offset(_)
            | Commands::User(_)
            | Commands::Quota(_)
            | Commands::Acl(_)
            | Commands::Reassign(_)
            | Commands::Dlq(_)
            | Commands::Bench(_)
            | Commands::Schema(_)
            | Commands::Connect(_)
            // Gated per fix inside the command, see `lint_is_gated_per_fix`.
            | Commands::Lint(_)
            | Commands::Undo(_) => true,
            Commands::History(_)
            | Commands::Monitor(_)
            | Commands::Profile(_)
            | Commands::Dump(_)
            | Commands::Compact(_)
            | Commands::Trace(_)
            | Commands::Diff(_)
            | Commands::Top(_)
            | Commands::Cluster(_)
            | Commands::Crypto(_)
            | Commands::Topology(_)
            | Commands::Health(_)
            | Commands::Report(_)
            // Each MCP write tool call passes the gate itself.
            | Commands::Mcp(_)
            | Commands::Audit { .. }
            | Commands::Policy(_) => false,
        }
    }

    fn command(args: &[&str]) -> Commands {
        let cli = Cli::try_parse_from(std::iter::once("ferrika").chain(args.iter().copied()))
            .unwrap_or_else(|e| panic!("{args:?} should parse: {e}"));
        cli.command
            .unwrap_or_else(|| panic!("{args:?} should name a command"))
    }

    #[test]
    fn write_commands_have_an_action_and_a_target() {
        for (args, action, target) in [
            (
                &["topics", "create", "orders"][..],
                "topics create",
                "orders",
            ),
            (&["produce", "orders", "-m", "hi"][..], "produce", "orders"),
            (
                &["consume", "orders", "--group", "billing"][..],
                "consume --group",
                "billing on orders",
            ),
        ] {
            let cmd = command(args);
            assert!(may_write(&cmd), "{args:?}");
            assert_eq!(write_action(&cmd), Some(action), "{args:?}");
            assert_eq!(write_target(&cmd), target, "{args:?}");
        }
    }

    #[test]
    fn only_commands_that_record_history_get_an_operation_id() {
        for label in [
            "topics config",
            "topics delete",
            "offset delete-group",
            "acl delete --force",
            "schema compat --set",
            "connect apply",
            "reassign execute",
        ] {
            assert!(records_history(label), "{label}");
        }
        for label in [
            "produce",
            "consume --group",
            "generate",
            "mirror",
            "restore",
            "probe",
            "dlq redrive --execute",
            "bench produce",
            "reassign rollback",
        ] {
            assert!(!records_history(label), "{label}");
        }
    }

    #[test]
    fn only_local_commands_skip_profile_resolution() {
        for args in [
            &["policy", "show"][..],
            &["profile", "list"][..],
            &["audit"][..],
            &["audit", "verify"][..],
        ] {
            assert!(!needs_profiles(Some(&command(args))), "{args:?}");
        }
        assert!(!needs_profiles(None));
        for args in [
            &["topics", "list"][..],
            &["topics", "create", "orders"][..],
            &["produce", "orders", "-m", "hi"][..],
            &["dump", "orders", "-o", "x.jsonl"][..],
            &["mcp"][..],
        ] {
            let cmd = command(args);
            assert!(needs_profiles(Some(&cmd)), "{args:?}");
        }
    }

    #[test]
    fn no_command_prints_the_help() {
        assert!(Cli::try_parse_from(["ferrika"]).unwrap().command.is_none());
        let help = no_command_help();
        assert!(help.contains("Usage: ferrika"), "{help}");
        assert!(help.contains("Commands:"), "{help}");
        assert!(help.contains("topics"), "{help}");
        // The desktop is not a CLI command.
        assert!(!help.contains("gui"), "{help}");
    }

    #[test]
    fn read_commands_have_no_action() {
        for args in [
            &["topics", "list"][..],
            &["consume", "orders", "--from-beginning"][..],
            &["bench", "consume", "orders"][..],
            &["mcp"][..],
            &["mcp", "--allow-write"][..],
            &["report", "--since", "30m"][..],
            &["lint"][..],
            &["lint", "rules"][..],
            &["schema", "fields", "orders-value"][..],
            &["schema", "fields", "--topic", "orders", "--plain"][..],
            &["topics", "histogram", "orders"][..],
            &["topics", "timeline", "orders", "--all"][..],
        ] {
            let cmd = command(args);
            assert_eq!(write_action(&cmd), None, "{args:?}");
            if !may_write(&cmd) {
                assert_eq!(write_target(&cmd), "", "{args:?}");
            }
        }
    }

    /// Every command, with whether it reads record data (keys, payloads,
    /// header values) from a cluster. Checks that `export_gate` only gates
    /// commands that read records (see `record_exports_have_a_channel_and_a_target`).
    fn reads_records(command: &Commands) -> bool {
        match command {
            Commands::Consume(_)
            | Commands::Dump(_)
            | Commands::Dlq(_)
            | Commands::Compact(_)
            | Commands::Trace(_)
            | Commands::Diff(_)
            | Commands::Crypto(_)
            // Gated in `cli::mirror::authorize` and the MCP server.
            | Commands::Mirror(_)
            | Commands::Mcp(_) => true,
            // Counts, sizes and offsets only; or records read in.
            Commands::Topics(_)
            | Commands::Produce(_)
            | Commands::Monitor(_)
            | Commands::Profile(_)
            | Commands::Bench(_)
            | Commands::Schema(_)
            | Commands::Generate(_)
            | Commands::Offset(_)
            | Commands::Restore(_)
            | Commands::Acl(_)
            | Commands::Quota(_)
            | Commands::User(_)
            | Commands::Reassign(_)
            | Commands::Probe(_)
            | Commands::Top(_)
            | Commands::Cluster(_)
            | Commands::Topology(_)
            | Commands::Health(_)
            | Commands::Report(_)
            | Commands::Connect(_)
            | Commands::Lint(_)
            | Commands::History(_)
            | Commands::Undo(_)
            | Commands::Audit { .. }
            | Commands::Policy(_) => false,
        }
    }

    #[test]
    fn record_exports_have_a_channel_and_a_target() {
        use governance::policy::ExportChannel::{File, Stdout};
        for (args, expected) in [
            (
                &["dump", "orders", "-o", "x.jsonl"][..],
                Some((File, "dump", "orders")),
            ),
            (
                &["consume", "orders", "--save", "x.jsonl"][..],
                Some((File, "consume --save", "orders")),
            ),
            (
                &["-f", "json", "consume", "orders"][..],
                Some((Stdout, "consume", "orders")),
            ),
            (
                &["dlq", "export", "orders-dlq", "-o", "x.json"][..],
                Some((File, "dlq export", "orders-dlq")),
            ),
            (
                &["dlq", "inspect", "orders-dlq"][..],
                Some((Stdout, "dlq inspect", "orders-dlq")),
            ),
            (
                &["compact", "snapshot", "users", "-o", "x.jsonl"][..],
                Some((File, "compact snapshot", "users")),
            ),
            (
                &["compact", "tombstones", "users"][..],
                Some((Stdout, "compact tombstones", "users")),
            ),
            (&["diff", "a", "b"][..], Some((Stdout, "diff", "a vs b"))),
            (
                &["trace", "inspect", "orders"][..],
                Some((Stdout, "trace inspect", "orders")),
            ),
            (
                &["crypto", "mask", "--topic", "users"][..],
                Some((Stdout, "crypto mask", "users")),
            ),
            (&["crypto", "mask"][..], None),
            (&["crypto", "audit", "users"][..], None),
            (&["dlq", "redrive", "orders-dlq"][..], None),
            (&["compact", "analyze", "users"][..], None),
            (&["trace", "audit", "orders"][..], None),
            (&["mirror", "a", "b"][..], None),
            (&["mcp"][..], None),
            (&["topics", "analyze", "orders"][..], None),
            (&["report", "--since", "30m"][..], None),
            (&["bench", "consume", "orders"][..], None),
        ] {
            let cmd = command(args);
            let gate = export_gate(&cmd);
            assert_eq!(
                gate.as_ref()
                    .map(|g| (g.channel, g.label, g.target.as_str())),
                expected,
                "{args:?}"
            );
            if gate.is_some() {
                assert!(reads_records(&cmd), "{args:?}");
            }
        }
    }

    /// `lint` mixes config changes, replication changes and group deletes in
    /// one run, so `main`'s single gate cannot classify it: each fix goes
    /// through the write gate on its own (`cli::lint`).
    #[test]
    fn lint_is_gated_per_fix() {
        for args in [
            &["lint"][..],
            &["lint", "--fix", "--yes"][..],
            &["lint", "--fix", "--dry-run", "--rule", "T001"][..],
        ] {
            let cmd = command(args);
            assert!(may_write(&cmd), "{args:?}");
            assert_eq!(write_action(&cmd), None, "{args:?}");
        }
    }

    #[test]
    fn history_reads_and_undo_gates_itself() {
        for args in [
            &["history"][..],
            &["history", "--all", "--undoable"][..],
            &["history", "show", "01JBX3"][..],
        ] {
            let cmd = command(args);
            assert!(!may_write(&cmd), "{args:?}");
            assert_eq!(write_action(&cmd), None, "{args:?}");
            assert!(needs_profiles(Some(&cmd)), "{args:?}");
        }
        for (args, target) in [
            (&["undo", "01JBX3K8"][..], "op 01JBX3K8"),
            (&["undo", "--dry-run"][..], "the latest undoable operation"),
            (&["undo", "01JBX3K8", "--force", "-y"][..], "op 01JBX3K8"),
        ] {
            let cmd = command(args);
            assert!(may_write(&cmd), "{args:?}");
            // The gate runs inside `undo`, per policy category of its steps.
            assert_eq!(write_action(&cmd), None, "{args:?}");
            assert_eq!(write_target(&cmd), target, "{args:?}");
        }
    }

    #[test]
    fn refused_group_consume_suggests_reading_without_a_group() {
        let grouped = command(&["consume", "orders", "-g", "billing"]);
        let message = refusal_message(&"Refused.", Some(&grouped));
        assert!(message.starts_with("Refused.\n"), "{message}");
        assert!(message.contains("without --group"), "{message}");
        let produce = command(&["produce", "orders", "-m", "hi"]);
        assert_eq!(refusal_message(&"Refused.", Some(&produce)), "Refused.");
    }
}
