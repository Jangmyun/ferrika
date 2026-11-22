//! Command line: arguments, the policy and audit gates in front of a command,
//! and the dispatch to the command handlers.

use clap::{Parser, Subcommand};

use crate::{cli, governance, kafka, model};

#[derive(Parser, Debug)]
#[command(name = "ferrika")]
#[command(author = "Ferrika Contributors")]
#[command(version)]
#[command(about = "Kafka command-line client: topics, records, consumer groups, schemas, Connect and cluster checks", long_about = None)]
pub struct Cli {
    #[command(flatten)]
    pub(crate) global_opts: cli::GlobalOpts,

    #[command(subcommand)]
    pub(crate) command: Option<Commands>,
}

#[derive(Subcommand, Debug)]
pub(crate) enum Commands {
    /// List, describe, create, configure, purge and delete topics
    Topics(cli::topics::TopicsArgs),
    /// Send records to a topic
    Produce(cli::produce::ProduceArgs),
    /// Read records from a topic, with key, header and field filters
    Consume(cli::consume::ConsumeArgs),
    /// Show consumer group lag and broker status; serve Prometheus metrics
    Monitor(cli::monitor::MonitorArgs),
    /// Manage saved cluster connection profiles
    Profile(cli::profile::ProfileArgs),
    /// Measure produce and consume throughput
    Bench(cli::bench::BenchArgs),
    /// Schema Registry: subjects, schemas, compatibility, decoding
    Schema(cli::schema::SchemaArgs),
    /// Produce synthetic sample events (ecommerce, iot, web, financial)
    Generate(cli::generator::GenerateArgs),
    /// Copy records between topics, also across clusters
    Mirror(cli::mirror::MirrorArgs),
    /// Copy records to another topic through filter, drop, rename, set, rekey, mask and
    /// sample steps (a dry run unless --execute)
    Transform(cli::transform::TransformArgs),
    /// Show and reset consumer group offsets
    Offset(cli::offset::OffsetArgs),
    /// Back up topic records to a JSON Lines (.jsonl) file
    Dump(cli::backup::DumpArgs),
    /// Restore topic records from a JSON Lines (.jsonl) file
    Restore(cli::backup::RestoreArgs),
    /// List, create and delete cluster ACLs
    Acl(cli::acl::AclArgs),
    /// List, set and remove client quotas (byte rates, request share)
    Quota(cli::quota::QuotaArgs),
    /// List, create and delete SASL/SCRAM users
    User(cli::user::UserArgs),
    /// Plan, run, verify and roll back partition reassignments
    Reassign(cli::reassign::ReassignArgs),
    /// Analyze log compaction and tombstones; snapshot the latest value per key
    Compact(cli::compact::CompactArgs),
    /// Inspect, export and redrive dead letter queue records
    Dlq(cli::dlq::DlqArgs),
    /// Inspect, audit and inject tracing headers (W3C, B3, Jaeger)
    Trace(cli::trace::TraceArgs),
    /// Compare the records and schemas of two topics
    Diff(cli::diff::DiffArgs),
    /// Measure produce-to-consume latency with canary records
    Probe(cli::probe::ProbeArgs),
    /// Live read-only dashboard: brokers, topics, group lag, health findings
    #[command(visible_alias = "tui")]
    Top(cli::top::TopArgs),
    /// Ping several clusters, compare their topics and plan reconciliation
    Cluster(cli::cluster_sync::ClusterArgs),
    /// Scan records for personal data (PII) and mask fields
    Crypto(cli::crypto::CryptoArgs),
    /// Show brokers, racks, partition placement and under-replication
    Topology(cli::topology::TopologyArgs),
    /// Check cluster health for scripts and CI (exit code on findings)
    Health(cli::health::HealthArgs),
    /// Check topic, broker and group configs for risky values, and fix them
    Lint(cli::lint::LintArgs),
    /// Write a Markdown incident report: health, lag trend, audit timeline
    Report(cli::report::ReportArgs),
    /// List and control Kafka Connect connectors
    Connect(cli::connect::ConnectArgs),
    /// Serve cluster tools to AI assistants over MCP (stdin/stdout); read-only unless --allow-write
    Mcp(cli::mcp::McpArgs),
    /// RabbitMQ (preview): queues, exchanges, bindings, peek, publish, DLQ inspect/redrive, profiles
    Rabbit(cli::rabbit::RabbitArgs),
    /// Show the local audit log of cluster changes (GUI, CLI and MCP)
    Audit {
        /// Number of entries to show
        #[arg(short = 'n', long, default_value_t = 30)]
        limit: usize,
        #[command(flatten)]
        filter: cli::audit::AuditFilter,
        #[command(subcommand)]
        command: Option<cli::audit::AuditCommand>,
    },
    /// Signed company policy: keygen, sign, verify, show
    Policy(cli::policy::PolicyArgs),
    /// List cluster changes recorded for undo, newest first
    History(cli::history::HistoryArgs),
    /// Revert a recorded change; refuses if the state drifted, unless --force
    Undo(cli::history::UndoArgs),
    /// Print a shell completion script (bash, zsh, fish, powershell, elvish)
    #[command(after_help = COMPLETIONS_HELP)]
    Completions {
        #[arg(value_enum)]
        shell: clap_complete::Shell,
    },
}

const COMPLETIONS_HELP: &str = "Install, for example:
  bash:        ferrika completions bash > ~/.local/share/bash-completion/completions/ferrika
  zsh:         ferrika completions zsh > \"${fpath[1]}/_ferrika\"
  fish:        ferrika completions fish > ~/.config/fish/completions/ferrika.fish
  PowerShell:  ferrika completions powershell | Out-String | Invoke-Expression   (add to $PROFILE)
Homebrew installs them for you.";

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
            cli::record_audit_result(true, "ended with a pipe error (output closed?)");
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
    let mut command = <Cli as clap::CommandFactory>::command();
    let matches = command.get_matches_mut();
    let brokers_from_env =
        matches.value_source("bootstrap_servers") == Some(clap::parser::ValueSource::EnvVariable);
    let cli = <Cli as clap::FromArgMatches>::from_arg_matches(&matches)
        .map_err(|e| e.format(&mut command))
        .unwrap_or_else(|e| e.exit());
    if brokers_from_env {
        // The profile the command uses (the chosen one, else the active
        // one), when the profiles file has it. Read without the OS
        // credential store: only the name is needed. Without a profiles file
        // the variable simply names the brokers.
        let path = crate::kafka::profile::ProfileStore::default_path();
        let store = path
            .exists()
            .then(|| crate::kafka::profile::ProfileStore::load_from_path(&path).ok())
            .flatten();
        let profile = store.and_then(|store| {
            let name = cli
                .global_opts
                .profile
                .clone()
                .filter(|p| !p.trim().is_empty())
                .or(store.active_profile)?;
            store.profiles.contains_key(&name).then_some(name)
        });
        if let Some(warning) = env_brokers_warning(
            profile.as_deref(),
            cli.global_opts.bootstrap_servers.as_deref(),
            brokers_from_env,
        ) {
            eprintln!("{warning}");
        }
    }
    run(cli);
    // `diff ... --exit-code` that found differences ends with status 1.
    let code = cli::finish_exit_code();
    if code != 0 {
        std::process::exit(code);
    }
}

/// A warning when `KAFKA_BOOTSTRAP_SERVERS` (often left set by a
/// docker-compose setup) replaces the brokers of the profile in use (chosen
/// with `-p`/`FERRIKA_PROFILE`, or the active one): the command then sends
/// that profile's credentials to other brokers while its name shows in
/// audit entries.
fn env_brokers_warning(
    profile: Option<&str>,
    brokers: Option<&str>,
    brokers_from_env: bool,
) -> Option<String> {
    let profile = profile.map(str::trim).filter(|p| !p.is_empty())?;
    let brokers = brokers.map(str::trim).filter(|b| !b.is_empty())?;
    brokers_from_env.then(|| {
        format!(
            "Warning: KAFKA_BOOTSTRAP_SERVERS={brokers} replaces the brokers of profile \
             '{profile}', and its credentials go there. Unset it to use the profile's \
             brokers, or pass -b to choose them explicitly."
        )
    })
}

/// Runs a CLI command line: the policy and audit gates, then the command.
/// Without a command it prints the help and returns.
pub fn run(mut cli: Cli) {
    if cli.command.is_none() {
        print!("{}", no_command_help());
        return;
    }
    cli.global_opts.ignore_empty_values();
    // `--exit-code` on a diff command: errors exit with 2 (1 means
    // "differences found"), from the first check on.
    if cli.command.as_ref().is_some_and(wants_diff_exit_codes) {
        cli::use_diff_exit_codes();
    }
    // Reads nothing and changes nothing: no policy, profile or audit.
    if let Some(Commands::Completions { shell }) = &cli.command {
        let mut command = <Cli as clap::CommandFactory>::command();
        let mut script = Vec::new();
        clap_complete::generate(*shell, &mut command, "ferrika", &mut script);
        // `| head` closing the pipe early is not an error.
        if let Err(e) = std::io::Write::write_all(&mut std::io::stdout(), &script) {
            if e.kind() != std::io::ErrorKind::BrokenPipe {
                cli::exit_with_error(&format!("could not write the completion script: {e}"));
            }
        }
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
    // `rabbit` names its broker with a RabbitMQ profile and its own
    // options; resolve them once, before the gates read the profile name
    // and the address (an unknown profile or no URL stops the command).
    if let Some(Commands::Rabbit(args)) = &mut cli.command {
        if let Err(err) = cli::rabbit::resolve(args) {
            cli::exit_with_error(&err);
        }
    }
    // Record data leaving the app: refused under `export: forbidden` (before
    // the masking refusal of `dump` below, so that is the reason shown),
    // recorded under `export: audited`.
    if let Some(gate) = cli.command.as_ref().and_then(export_gate) {
        let profile = gate_profile(cli.command.as_ref(), &cli.global_opts);
        // The brokers the cluster reports too, only when the policy matches
        // broker addresses (`export_clusters`): else no request is made.
        let bootstrap = match gate_brokers(
            cli.command.as_ref(),
            &cli.global_opts,
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
    // The same for `transform --execute`, unless its first step masks PII.
    if let Some(Commands::Transform(args)) = &cli.command {
        if args.execute {
            if let Err(refusal) = cli::transform::authorize(args, &cli.global_opts) {
                cli::exit_with_error(&refusal);
            }
        }
    }

    if let Some(action) = cli.command.as_ref().and_then(main_gate_action) {
        let profile = gate_profile(cli.command.as_ref(), &cli.global_opts);
        // The brokers the cluster reports too (a tunnel or IP address must
        // not dodge `read_only_brokers`), only when the policy has such a rule.
        let bootstrap = match gate_brokers(
            cli.command.as_ref(),
            &cli.global_opts,
            policy.write_needs_broker_metadata(),
        ) {
            Ok(brokers) => brokers,
            Err(reason) => {
                governance::policy::record_blocked("cli", &profile, action, &reason);
                cli::exit_with_error(&reason)
            }
        };
        // A change that does the work of more than one category needs each
        // of them allowed (a RabbitMQ redrive republishes: `produce` too).
        if let Some(reason) = governance::policy::extra_category_refusal(
            policy,
            model::policy::PolicyAction::from_cli(action),
            action,
            &profile,
            &bootstrap,
        ) {
            let audit_profile = if profile.is_empty() {
                bootstrap.as_str()
            } else {
                profile.as_str()
            };
            governance::policy::record_blocked("cli", audit_profile, action, &reason);
            cli::exit_with_error(&reason);
        }
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
                // A read-only Kafka profile says nothing about a RabbitMQ
                // broker, which has its own profiles.
                match cli.command.as_ref() {
                    Some(Commands::Rabbit(args)) => cli::rabbit::ensure_writable(args, action),
                    _ => cli
                        .global_opts
                        .ensure_writable(action)
                        .map_err(|e| e.to_string()),
                }
            },
        ) {
            cli::exit_with_error(&refusal_message(&refusal, cli.command.as_ref()));
        }
        // Changes recorded in the history write their result entry when the
        // record finishes; for the others it is written at the end of `main`
        // (or on the way out after an error).
        if op.is_none() {
            cli::expect_audit_result(governance::audit::AuditEntry {
                time_ms: 0,
                source: "cli".to_string(),
                // As the request entry names it (the brokers without a
                // profile), so the two lines pair up.
                profile: if profile.is_empty() {
                    bootstrap.clone()
                } else {
                    profile.clone()
                },
                action: action.to_string(),
                target: target.clone(),
                outcome: String::new(),
                ok: true,
                user: String::new(),
                actor: None,
                op: String::new(),
                op_digest: String::new(),
            });
        }
        cli.global_opts.write = Some(cli::WriteContext {
            label: action.to_string(),
            target,
            op,
        });
    }

    let global = &cli.global_opts;
    // `cluster` and `mirror` talk to other profiles' clusters too; hints
    // naming the global bootstrap would point at the wrong one.
    if needs_profiles(cli.command.as_ref())
        && !matches!(
            cli.command,
            Some(Commands::Cluster(_) | Commands::Mirror(_))
        )
    {
        cli::set_hint_config(global.to_kafka_config());
    }
    // Only for a person at a terminal: a script may start the broker and
    // the command together (`docker compose up -d && ferrika ...`) and rely
    // on the client waiting for it.
    if std::io::IsTerminal::is_terminal(&std::io::stderr())
        && cli.command.as_ref().is_some_and(fails_fast_without_broker)
    {
        let config = global.to_kafka_config();
        if let Some(reason) =
            cli::unreachable_bootstrap(&config.bootstrap_servers, std::time::Duration::from_secs(3))
        {
            cli::print_connection_hint(&config, &crate::error::KafkaError::io(reason.clone()));
            cli::exit_with_error(&reason);
        }
    }
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
        Some(Commands::Transform(args)) => run_async(cli::transform::run(args, global)),
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
        Some(Commands::Audit {
            limit,
            filter,
            command,
        }) => cli::audit::run(limit, &filter, command, global),
        Some(Commands::Policy(args)) => exit_on_error(cli::policy::run(args, global)),
        // Printed at the start of `run`.
        Some(Commands::Completions { .. }) => {}
        Some(Commands::Health(args)) => run_async(cli::health::run(args, global)),
        Some(Commands::Lint(args)) => run_async(cli::lint::run(args, global)),
        Some(Commands::Report(args)) => run_async(cli::report::run(args, global)),
        Some(Commands::Connect(args)) => run_async(cli::connect::run(args, global)),
        Some(Commands::History(args)) => run_async(cli::history::run_history(args, global)),
        Some(Commands::Undo(args)) => run_async(cli::history::run_undo(args, global)),
        Some(Commands::Mcp(args)) => run_async(cli::mcp::run(args, global)),
        Some(Commands::Rabbit(args)) => run_async(cli::rabbit::run(args, global)),
        None => print!("{}", no_command_help()),
    }
    // The handler returned without an error (an interrupted batch too).
    cli::record_audit_result(true, "finished without error");
}

/// Runs an async CLI command on a fresh tokio runtime, exiting with the
/// command's error (see [`cli::exit_with_error`]) if it fails. Accepts both
/// `KafkaResult` and `anyhow::Result` commands.
fn run_async<F, E>(command: F)
where
    F: std::future::Future<Output = Result<(), E>>,
    E: std::fmt::Display + 'static,
{
    let rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("Failed to initialize async runtime: {e}");
            cli::record_audit_result(false, "failed (see the command's output)");
            std::process::exit(cli::error_exit_code());
        }
    };
    exit_on_error(rt.block_on(command));
}

/// Whether `command` is a comparison run with `--exit-code` (`diff`,
/// `cluster diff`, `schema diff-schema`): it then exits like
/// `git diff --exit-code`, 0 without and 1 with differences, 2 on errors.
fn wants_diff_exit_codes(command: &Commands) -> bool {
    match command {
        Commands::Diff(args) => args.exit_code,
        Commands::Cluster(args) => matches!(
            &args.command,
            cli::cluster_sync::ClusterCommands::Diff(diff) if diff.exit_code
        ),
        Commands::Schema(args) => matches!(
            &args.command,
            cli::schema::SchemaSubcommands::DiffSchema {
                exit_code: true,
                ..
            }
        ),
        _ => false,
    }
}

/// Exits with the command's error (see [`cli::exit_with_error`]) if it
/// failed, after the troubleshooting hints a Kafka error calls for.
fn exit_on_error<E: std::fmt::Display + 'static>(result: Result<(), E>) {
    if let Err(err) = result {
        if let Some(kafka) = kafka_error_of(&err) {
            cli::hint_unreported(kafka);
        }
        cli::exit_with_error(&err);
    }
}

/// The Kafka error a command failed with, directly or inside an
/// `anyhow::Error`.
fn kafka_error_of(err: &dyn std::any::Any) -> Option<&crate::error::KafkaError> {
    err.downcast_ref::<crate::error::KafkaError>().or_else(|| {
        err.downcast_ref::<anyhow::Error>()
            .and_then(|e| e.downcast_ref::<crate::error::KafkaError>())
    })
}

/// Whether `command` uses the cluster profiles, so `main` resolves a
/// [`cli::ResolvedContext`] for it. `profile` manages the store itself (and
/// reports a broken file with its path), and `policy` and `audit` never
/// connect.
fn needs_profiles(command: Option<&Commands>) -> bool {
    !matches!(
        command,
        None | Some(
            Commands::Profile(_)
                | Commands::Policy(_)
                | Commands::Audit { .. }
                | Commands::Completions { .. }
                | Commands::Rabbit(_)
        )
    )
}

/// The profile name the policy and audit gates use for `command`: the
/// resolved Kafka profile, or for `rabbit` the RabbitMQ profile (none when
/// the broker is named by options only; the audit entries then name its
/// address).
fn gate_profile(command: Option<&Commands>, global: &cli::GlobalOpts) -> String {
    match command {
        Some(Commands::Rabbit(args)) => cli::rabbit::gate_profile(args),
        _ => global.resolved_profile_name().unwrap_or_default(),
    }
}

/// The broker addresses the policy rules (`read_only`, `read_only_brokers`,
/// `export_clusters`) are matched against for `command`: the Kafka
/// bootstrap servers plus, when `needed`, the brokers the cluster reports;
/// for `rabbit`, the management API's `host:port`.
fn gate_brokers(
    command: Option<&Commands>,
    global: &cli::GlobalOpts,
    needed: bool,
) -> Result<String, String> {
    match command {
        Some(Commands::Rabbit(args)) => cli::rabbit::gate_address(args),
        _ => crate::governance::policy::brokers_for(&global.to_kafka_config(), needed),
    }
}

/// Commands that need the global cluster's brokers from the start and fail
/// without them anyway: they stop at once when nothing listens at the
/// bootstrap servers (see `cli::unreachable_bootstrap`). Left out: commands
/// meant to keep watching or to run without a broker (`health --watch`,
/// `monitor`, `top`, `probe`, `report`, `mcp`), ones that talk to other
/// clusters or services (`mirror`, `cluster`, `diff`, `schema`, `connect`)
/// or to local files (`acl --local`, `lint rules`, `history`), ones with
/// offline modes (`reassign plan --partitions`, `trace inject`), and `undo`.
fn fails_fast_without_broker(command: &Commands) -> bool {
    match command {
        Commands::Topics(_)
        | Commands::Produce(_)
        | Commands::Consume(_)
        | Commands::Offset(_)
        | Commands::Dump(_)
        | Commands::Restore(_)
        | Commands::Generate(_)
        | Commands::Compact(_)
        | Commands::Dlq(_)
        | Commands::Topology(_)
        | Commands::Quota(_)
        | Commands::User(_)
        | Commands::Bench(_) => true,
        Commands::Health(args) => args.watch.is_none(),
        Commands::Lint(args) => args.command.is_none(),
        Commands::Acl(args) => !args.local,
        _ => false,
    }
}

/// The write the gate in `main` checks and records for `command`: its
/// [`write_action`], except a `mirror` that names its clusters
/// (`--src-profile`, `--dst-profile`, `--dst-bootstrap-servers`), whose
/// destination `mirror::authorize` checks and records; the global profile
/// may then be a cluster it never writes to (and refuse it when read-only).
fn main_gate_action(command: &Commands) -> Option<&'static str> {
    match command {
        Commands::Mirror(args) if cli::mirror::names_clusters(args) => None,
        Commands::Transform(args) if cli::mirror::names_clusters(args) => None,
        _ => write_action(command),
    }
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
        Commands::Transform(args) if args.execute => Some(cli::transform::EXECUTE_LABEL),
        // Without --execute, transform only prints the records it would write.
        Commands::Transform(_) => None,
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
        Commands::Rabbit(args) => cli::rabbit::write_action(args),
        Commands::Completions { .. } => None,
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
        // A dry run prints the transformed records.
        Commands::Transform(a) if !a.execute => gate(Stdout, "transform --dry-run", &a.src_topic),
        Commands::Rabbit(args) => {
            cli::rabbit::export(args).and_then(|(label, target)| gate(Stdout, label, &target))
        }
        // Metadata or counts only, records read in, or checked elsewhere.
        Commands::Mirror(_)
        | Commands::Transform(_)
        | Commands::Mcp(_)
        | Commands::Completions { .. } => None,
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
        Commands::Transform(a) => format!("{} -> {}", a.src_topic, a.dst_topic),
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
            AclSubcommands::Delete(d) if d.all => "all ACL bindings".to_string(),
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
        Commands::Rabbit(args) => cli::rabbit::write_target(args),
        Commands::Undo(a) => match &a.id {
            Some(id) => format!("op {id}"),
            None => "the latest undoable operation".to_string(),
        },
        _ => String::new(),
    }
}
#[cfg(test)]
mod tests {
    #[test]
    fn env_brokers_with_a_chosen_profile_are_flagged() {
        use super::env_brokers_warning;
        let warning = env_brokers_warning(Some("staging"), Some("localhost:9092"), true);
        assert!(
            warning
                .as_deref()
                .is_some_and(|w| w.contains("'staging'") && w.contains("localhost:9092")),
            "{warning:?}"
        );
        // -b on the command line is a choice, not a leftover variable.
        assert_eq!(
            env_brokers_warning(Some("staging"), Some("h:9092"), false),
            None
        );
        // Without a chosen profile the variable just names the brokers.
        assert_eq!(env_brokers_warning(None, Some("h:9092"), true), None);
        assert_eq!(env_brokers_warning(Some(" "), Some("h:9092"), true), None);
        assert_eq!(env_brokers_warning(Some("staging"), Some(""), true), None);
    }

    #[test]
    fn empty_profile_and_brokers_mean_not_set() {
        use clap::Parser as _;
        let mut cli =
            super::Cli::try_parse_from(["ferrika", "-p", "", "-b", " ", "topics", "list"]).unwrap();
        cli.global_opts.ignore_empty_values();
        assert_eq!(cli.global_opts.profile, None);
        assert_eq!(cli.global_opts.bootstrap_servers, None);
        let mut cli =
            super::Cli::try_parse_from(["ferrika", "-p", "prod", "topics", "list"]).unwrap();
        cli.global_opts.ignore_empty_values();
        assert_eq!(cli.global_opts.profile.as_deref(), Some("prod"));
    }

    #[test]
    fn kafka_errors_are_found_directly_or_in_anyhow() {
        use super::kafka_error_of;
        use crate::error::KafkaError;
        let direct = KafkaError::io("refused");
        assert!(kafka_error_of(&direct).is_some());
        let wrapped = anyhow::Error::new(KafkaError::io("refused"));
        assert!(kafka_error_of(&wrapped).is_some());
        assert!(kafka_error_of(&anyhow::anyhow!("plain")).is_none());
        assert!(kafka_error_of(&"text".to_string()).is_none());
    }
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
            | Commands::Transform(_)
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
            | Commands::Rabbit(_)
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
            | Commands::Completions { .. }
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
    fn only_broker_first_commands_fail_fast() {
        use super::fails_fast_without_broker;
        for args in [
            &["topics", "list"][..],
            &["produce", "t", "-m", "x"][..],
            &["consume", "t"][..],
            &["health"][..],
            &["lint"][..],
            &["acl", "list"][..],
        ] {
            assert!(fails_fast_without_broker(&command(args)), "{args:?}");
        }
        for args in [
            &["health", "--watch", "60"][..],
            &["lint", "rules"][..],
            &["acl", "--local", "list"][..],
            &["mirror", "a", "b"][..],
            &["report"][..],
            &["mcp"][..],
            &["schema", "list"][..],
            &[
                "reassign",
                "plan",
                "-t",
                "orders",
                "--brokers",
                "1,2",
                "--partitions",
                "3",
            ][..],
            &["trace", "inject"][..],
        ] {
            assert!(!fails_fast_without_broker(&command(args)), "{args:?}");
        }
    }

    #[test]
    fn transform_writes_only_with_execute() {
        use super::main_gate_action;
        assert_eq!(
            write_action(&command(&["transform", "orders", "--to", "copy"])),
            None
        );
        let execute = command(&["transform", "orders", "--to", "copy", "--execute"]);
        assert_eq!(write_action(&execute), Some("transform --execute"));
        assert_eq!(main_gate_action(&execute), Some("transform --execute"));
        assert_eq!(write_target(&execute), "orders -> copy");
        // A named destination is checked by `transform::authorize` instead.
        for args in [
            &["--dst-profile", "staging"][..],
            &["--dst-bootstrap-servers", "h:9092"][..],
        ] {
            let argv: Vec<&str> = ["transform", "orders", "--to", "copy", "--execute"]
                .iter()
                .chain(args)
                .copied()
                .collect();
            assert_eq!(main_gate_action(&command(&argv)), None, "{args:?}");
        }
        assert_eq!(
            model::policy::PolicyAction::from_cli("transform --execute"),
            model::policy::PolicyAction::Produce
        );
    }

    #[test]
    fn mirror_between_named_clusters_skips_the_global_gate() {
        use super::main_gate_action;
        assert_eq!(
            main_gate_action(&command(&["mirror", "a", "b"])),
            Some("mirror")
        );
        for args in [
            &["mirror", "a", "b", "--src-profile", "prod"][..],
            &["mirror", "a", "b", "--dst-profile", "staging"][..],
            &["mirror", "a", "b", "--dst-bootstrap-servers", "h:9092"][..],
        ] {
            assert_eq!(main_gate_action(&command(args)), None, "{args:?}");
        }
        assert_eq!(
            main_gate_action(&command(&["produce", "t", "-m", "x"])),
            Some("produce")
        );
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
            | Commands::Transform(_)
            | Commands::Rabbit(_)
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
            | Commands::Completions { .. }
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
            (
                &["transform", "orders", "--to", "copy"][..],
                Some((Stdout, "transform --dry-run", "orders")),
            ),
            (
                &["transform", "orders", "--to", "copy", "--execute"][..],
                None,
            ),
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
    fn rabbit_publish_is_gated_as_a_write_and_peek_as_an_export() {
        use governance::policy::ExportChannel::Stdout;
        let url = "--rabbit-url=http://rabbit.prod:15672";
        let publish = command(&[
            "rabbit", url, "publish", "orders", "-k", "o.1", "-m", "body",
        ]);
        assert!(may_write(&publish));
        assert_eq!(write_action(&publish), Some("rabbit publish"));
        assert_eq!(super::main_gate_action(&publish), Some("rabbit publish"));
        assert_eq!(
            model::policy::PolicyAction::from_cli("rabbit publish"),
            model::policy::PolicyAction::Produce
        );
        assert_eq!(
            write_target(&publish),
            "exchange orders on vhost /, routing key 'o.1'"
        );
        assert!(!records_history("rabbit publish"));
        assert_eq!(export_gate(&publish), None);

        // A peek takes messages and requeues them (marked redelivered): a
        // write in its own category, and an export of record data.
        let peek = command(&["rabbit", url, "peek", "orders.dlq", "-n", "5"]);
        assert!(may_write(&peek));
        assert_eq!(write_action(&peek), Some("rabbit peek"));
        assert_eq!(
            model::policy::PolicyAction::from_cli("rabbit peek"),
            model::policy::PolicyAction::MessagesPeek
        );
        assert_eq!(write_target(&peek), "queue orders.dlq on vhost /");
        assert!(!records_history("rabbit peek"));
        let gate = export_gate(&peek).expect("peek shows record data");
        assert_eq!(
            (gate.channel, gate.label, gate.target.as_str()),
            (Stdout, "rabbit peek", "queue orders.dlq on vhost /")
        );
        assert!(reads_records(&peek));

        let inspect = command(&["rabbit", url, "dlq", "inspect", "orders.dlq"]);
        assert_eq!(write_action(&inspect), Some("rabbit dlq inspect"));
        let gate = export_gate(&inspect).expect("inspect shows record data");
        assert_eq!(gate.label, "rabbit dlq inspect");

        let preview = command(&["rabbit", url, "dlq", "redrive", "orders.dlq"]);
        assert_eq!(write_action(&preview), Some("rabbit dlq redrive"));
        assert_eq!(export_gate(&preview), None);
        let execute = command(&[
            "rabbit",
            url,
            "dlq",
            "redrive",
            "orders.dlq",
            "--execute",
            "--yes",
        ]);
        assert_eq!(write_action(&execute), Some("rabbit dlq redrive --execute"));
        assert_eq!(
            model::policy::PolicyAction::from_cli("rabbit dlq redrive --execute"),
            model::policy::PolicyAction::MessagesMove
        );
        assert!(!records_history("rabbit dlq redrive --execute"));
        assert_eq!(export_gate(&execute), None);

        for args in [
            &["rabbit", url, "queues"][..],
            &["rabbit", url, "bindings"][..],
            &["rabbit", "profile", "list"][..],
            &["rabbit", "profile", "use", "prod"][..],
        ] {
            let cmd = command(args);
            assert_eq!(write_action(&cmd), None, "{args:?}");
            assert_eq!(export_gate(&cmd), None, "{args:?}");
        }
    }

    #[test]
    fn rabbit_gates_match_the_management_address_not_a_kafka_profile() {
        use super::{gate_brokers, gate_profile};
        let mut publish = command(&[
            "-p",
            "prod-kafka",
            "rabbit",
            "--rabbit-url",
            "https://ops:pw@rabbit.prod.corp",
            "publish",
            "orders",
            "-m",
            "x",
        ]);
        let global = Cli::try_parse_from(["ferrika", "-p", "prod-kafka", "topics", "list"])
            .unwrap()
            .global_opts;
        assert!(!needs_profiles(Some(&publish)));
        // `main` resolves the connection before the gates; a URL without
        // --rabbit-profile reads no RabbitMQ profile.
        let Commands::Rabbit(args) = &mut publish else {
            panic!("a rabbit command")
        };
        args.connection.profile = None;
        cli::rabbit::resolve(args).unwrap();
        assert_eq!(gate_profile(Some(&publish), &global), "");
        assert_eq!(
            gate_brokers(Some(&publish), &global, true).unwrap(),
            "rabbit.prod.corp:443,rabbit.prod.corp:5671"
        );
        assert!(!super::fails_fast_without_broker(&publish));
    }

    #[test]
    fn a_rabbit_redrive_needs_produce_as_well_as_messages_move() {
        use model::policy::PolicyAction;
        // The gate checks the categories of the label's own category.
        let refusal = |policy: &governance::policy::EffectivePolicy, action: &str| {
            governance::policy::extra_category_refusal(
                policy,
                PolicyAction::from_cli(action),
                action,
                "",
                "rabbit.prod:15672,rabbit.prod:5672",
            )
        };
        let action = "rabbit dlq redrive --execute";
        assert_eq!(
            PolicyAction::from_cli(action).also_requires(),
            &[PolicyAction::Produce]
        );
        assert!(PolicyAction::from_cli("rabbit peek")
            .also_requires()
            .is_empty());
        assert!(PolicyAction::from_cli("produce").also_requires().is_empty());

        let mut policy = crate::governance::test_policy::masking();
        assert_eq!(refusal(&policy, action), None);
        policy.rules.forbid = vec!["produce".to_string()];
        let text = refusal(&policy, action).unwrap();
        assert!(text.contains("`produce`"), "{text}");
        assert!(text.contains("messages.move"), "{text}");
        // Other writes are judged by their own category only.
        assert_eq!(refusal(&policy, "rabbit peek"), None);
    }

    #[test]
    fn a_read_only_rule_on_the_amqp_address_covers_rabbit_commands() {
        use model::policy::PolicyAction;
        let mut policy = crate::governance::test_policy::masking();
        policy.rules.read_only_brokers = vec!["amqp.prod.corp:*".to_string()];
        // The gate address lists the management and the AMQP listener.
        let address = "mgmt.prod.corp:15672,amqp.prod.corp:5672";
        assert!(policy
            .check(PolicyAction::MessagesPeek, "", address)
            .is_err());
        assert!(policy
            .check(PolicyAction::MessagesPeek, "", "mgmt.prod.corp:15672")
            .is_ok());
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
