//! `ferrika lint`: risky or inconsistent topic, broker and group configs,
//! with a fix for each (see `ferrika_ops::lint`).

use clap::{Args, Subcommand, ValueEnum};
use colored::Colorize;

use crate::cli::health::{severity_tag, FailOn, MinSeverity};
use crate::cli::{output, print_connection_hint, GlobalOpts, OutputFormat};
use crate::error::{KafkaError, KafkaResult};
use crate::kafka::admin::KafkaAdminClient;
use crate::ops::lint::{
    self, parse_skip, target_label, Env, Fix, FixClass, LintFinding, LintOptions, RULES,
};

#[derive(Args, Debug, Clone)]
#[command(args_conflicts_with_subcommands = true)]
pub struct LintArgs {
    #[command(subcommand)]
    pub command: Option<LintCommand>,
    /// Only topics matching this glob (repeatable); broker and group
    /// findings are not narrowed
    #[arg(long = "topic", value_name = "GLOB")]
    pub topics: Vec<String>,
    /// Only this rule (repeatable), e.g. T002
    #[arg(long = "rule", value_name = "ID")]
    pub rules: Vec<String>,
    /// Leave out a rule, everywhere or for targets matching GLOB (repeatable)
    #[arg(long, value_name = "ID[:GLOB]")]
    pub skip: Vec<String>,
    /// Grade findings for production or not; `auto` decides from the profile
    #[arg(long, value_enum, default_value_t = EnvArg::Auto)]
    pub env: EnvArg,
    /// Lowest severity to report
    #[arg(long, value_enum, default_value_t = MinSeverity::Info)]
    pub min_severity: MinSeverity,
    /// Exit with an error when a finding is at least this severe (CI)
    #[arg(long, value_enum, default_value_t = FailOn::Never)]
    pub fail_on: FailOn,
    /// Apply the auto fixes (and the guided fixes of rules named with
    /// --rule), after showing the plan and asking once
    #[arg(long)]
    pub fix: bool,
    /// With --fix: change nothing; the brokers validate config changes and
    /// replication changes are only planned
    #[arg(long, requires = "fix")]
    pub dry_run: bool,
    /// With --fix: do not ask (deleting a group still asks for its name)
    #[arg(short = 'y', long, requires = "fix")]
    pub yes: bool,
    /// With --fix: only write the plan to FILE as JSON
    #[arg(
        long,
        value_name = "FILE",
        requires = "fix",
        conflicts_with = "dry_run"
    )]
    pub save_plan: Option<std::path::PathBuf>,
}

#[derive(Subcommand, Debug, Clone, PartialEq, Eq)]
pub enum LintCommand {
    /// List the rules: id, severity (production / other), fix class
    Rules,
}

#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnvArg {
    Auto,
    Prod,
    Dev,
}

pub async fn run(args: LintArgs, global: &GlobalOpts) -> KafkaResult<()> {
    if args.command == Some(LintCommand::Rules) {
        return print_rules(global);
    }
    let config = global.to_kafka_config();
    let admin = KafkaAdminClient::new(&config).inspect_err(|e| {
        print_connection_hint(&global.to_kafka_config(), e);
    })?;
    let profile = global.resolved_profile_name().unwrap_or_default();
    let (env, why) = env_for(args.env, global);
    let opts = options(&args, env);
    let mut input = lint::collect::gather(&admin, &config, &profile, None, &opts).await?;
    let findings: Vec<LintFinding> = lint::lint(&input, &opts)
        .into_iter()
        .filter(|f| args.min_severity.admits(f.severity))
        .collect();

    if args.fix {
        println!(
            "{}",
            format!("env: {} ({why})", env_name(env)).bright_black()
        );
        print_skipped(&input.skipped, false);
        return run_fixes(&args, global, &admin, &config, &mut input, &opts, findings).await;
    }

    if global.format == OutputFormat::Json {
        eprintln!("env: {} ({why})", env_name(env));
        output::print_json(&findings)?;
        print_skipped(&input.skipped, true);
    } else {
        println!(
            "{}",
            format!("env: {} ({why})", env_name(env)).bright_black()
        );
        print_findings(&findings);
        print_skipped(&input.skipped, false);
        let auto = findings
            .iter()
            .filter(|f| f.fix_class == FixClass::Auto && f.applicable())
            .count();
        if auto > 0 {
            println!("{auto} auto fix(es) can be applied with `ferrika lint --fix`.");
        }
    }

    let failing = args.fail_on.failing(findings.iter().map(|f| f.severity));
    if failing > 0 {
        return Err(KafkaError::Check(format!(
            "{failing} finding(s) at or above the --fail-on level"
        )));
    }
    Ok(())
}

/// Whether `--fix` applies `finding`: auto fixes always, guided ones only
/// when their rule is named with `--rule`; manual never.
fn selected(finding: &LintFinding, named: &[String]) -> bool {
    finding.applicable()
        && match finding.fix_class {
            FixClass::Auto => true,
            FixClass::Guided => named.iter().any(|r| r.eq_ignore_ascii_case(finding.rule)),
            FixClass::Manual => false,
        }
}

/// The plan as `--save-plan` writes it.
#[derive(serde::Serialize)]
struct SavedPlan<'a> {
    created_ms: i64,
    profile: &'a str,
    bootstrap: &'a str,
    fixes: &'a [LintFinding],
}

#[allow(clippy::too_many_arguments)]
async fn run_fixes(
    args: &LintArgs,
    global: &GlobalOpts,
    admin: &KafkaAdminClient,
    config: &crate::kafka::config::KafkaClientConfig,
    input: &mut lint::LintInput,
    opts: &LintOptions,
    findings: Vec<LintFinding>,
) -> KafkaResult<()> {
    use crate::ops::lint::apply::{
        apply_fixes, save_undo, ApplyMode, ClusterRunner, FixGate, Outcome,
    };

    let profile = global.resolved_profile_name().unwrap_or_default();
    let bootstrap = global.resolved_bootstrap_servers();
    let policy = crate::governance::policy::current();
    // What `read_only_brokers` is judged against: the cluster's own brokers
    // too, when the policy has such a rule. A lookup that fails refuses every
    // write, as the policy cannot be checked.
    let (policy_brokers, lookup_refusal) = match crate::governance::policy::brokers_for(
        config,
        policy.write_needs_broker_metadata(),
    ) {
        Ok(brokers) => (brokers, None),
        Err(reason) => (bootstrap.clone(), Some(reason)),
    };
    let read_only = global
        .ensure_writable("lint --fix")
        .err()
        .map(|e| e.to_string());

    // The plan, with what the policy or the profile would refuse left out.
    // Nothing is recorded here: nothing was attempted yet.
    let mut plan = Vec::new();
    let mut excluded = Vec::new();
    for f in findings.into_iter().filter(|f| selected(f, &args.rules)) {
        let refusal = match f.fix.policy_action() {
            Some(action) => lookup_refusal
                .clone()
                .or_else(|| {
                    policy
                        .check(action, &profile, &policy_brokers)
                        .err()
                        .map(String::from)
                })
                .or_else(|| read_only.clone()),
            None => None,
        };
        match refusal {
            // A dry run changes nothing, so it is not refused.
            Some(reason) if !args.dry_run => excluded.push((f, reason)),
            _ => plan.push(f),
        }
    }
    let guided_skipped = lint::lint(input, opts)
        .iter()
        .filter(|f| f.fix_class == FixClass::Guided && f.applicable() && !selected(f, &args.rules))
        .count();

    if plan.is_empty() && excluded.is_empty() {
        println!("{} Nothing to fix.", "✔".green().bold());
        if guided_skipped > 0 {
            println!("{guided_skipped} guided fix(es) run only when their rule is named, e.g. --rule T001.");
        }
        return Ok(());
    }
    println!("{}", "Plan:".bold());
    for f in &plan {
        println!(
            "  {} {} {}: {} {}",
            f.rule.bold(),
            target_label(&f.target).cyan(),
            f.fix_class.label().bright_black(),
            f.change_summary(),
            "allowed".green()
        );
    }
    for (f, reason) in &excluded {
        println!(
            "  {} {} {}: {} {}",
            f.rule.bold(),
            target_label(&f.target).cyan(),
            f.fix_class.label().bright_black(),
            f.change_summary(),
            format!("excluded ({reason})").red()
        );
    }
    if guided_skipped > 0 {
        println!("{guided_skipped} guided fix(es) left out; name their rule with --rule to include them.");
    }

    if let Some(path) = &args.save_plan {
        let saved = SavedPlan {
            created_ms: crate::governance::audit::now_ms(),
            profile: &profile,
            bootstrap: &bootstrap,
            fixes: &plan,
        };
        let json = serde_json::to_string_pretty(&saved)
            .map_err(|e| KafkaError::Serialization(e.to_string()))?;
        std::fs::write(path, json)
            .map_err(|e| KafkaError::io(format!("Cannot write {}: {e}", path.display())))?;
        println!("Plan written to {}; nothing was changed.", path.display());
        return Ok(());
    }
    if plan.is_empty() {
        return Err(KafkaError::Check(format!(
            "every fix is refused ({} excluded)",
            excluded.len()
        )));
    }

    let mode = if args.dry_run {
        ApplyMode::Validate
    } else {
        ApplyMode::Apply
    };
    if mode == ApplyMode::Apply && !args.yes {
        crate::cli::offset::confirm(
            &format!("Apply {} fix(es)? [y/N]: ", plan.len()),
            "Aborted; nothing was changed.",
        )?;
    }

    let runner = ClusterRunner::new(admin, config, input);
    let check = || {
        global
            .ensure_writable("lint --fix")
            .map_err(|e| e.to_string())
    };
    // Applied fixes are recorded like the other CLI changes (`ferrika
    // history`, `ferrika undo`); a dry run records nothing, so it skips the
    // cluster id read.
    let journal = crate::ops::history::Journal::default();
    let scope = match mode {
        ApplyMode::Apply => crate::cli::record::cluster_scope(global, Some(admin)),
        ApplyMode::Validate => crate::model::history::Scope::default(),
    };
    let gate = FixGate {
        policy,
        sink: &crate::governance::policy::AuditLog,
        profile: &profile,
        bootstrap: &policy_brokers,
        profile_check: &check,
        journal: &journal,
        scope: &scope,
    };
    let yes = args.yes;
    let confirm = |f: &LintFinding| {
        if mode == ApplyMode::Validate {
            return true;
        }
        match &f.fix {
            // Cannot be undone: the name is typed even with --yes.
            Fix::DeleteGroup { group } => confirm_name(group),
            _ if f.fix_class == FixClass::Guided && !yes => crate::cli::offset::confirm(
                &format!(
                    "{} {}: {}. {} Apply? [y/N]: ",
                    f.rule,
                    target_label(&f.target),
                    f.change_summary(),
                    f.fix.impact()
                ),
                "Skipped.",
            )
            .is_ok(),
            _ => true,
        }
    };
    let show = |r: &lint::apply::FixResult| {
        let head = format!("{} {}", r.finding.rule, target_label(&r.finding.target));
        match &r.outcome {
            Outcome::Applied(m) => println!("{} {head}: {m}", "✔".green().bold()),
            Outcome::Validated(m) => println!("{} {head}: {m}", "✔".green()),
            Outcome::Failed(m) => println!("{} {head}: {m}", "✖".red().bold()),
            Outcome::NotAllowed(m) => println!("{} {head}: {m}", "-".yellow()),
            Outcome::Changed => {
                println!("{} {head}: changed since the lint, skipped", "-".yellow())
            }
            Outcome::Refused(m) => println!("{} {head}: {m}", "✖".red()),
            Outcome::Declined => println!("{} {head}: skipped", "-".bright_black()),
            Outcome::Stopped => println!("{} {head}: not tried", "-".bright_black()),
        }
    };
    let (results, undo) =
        apply_fixes(&runner, &gate, input, opts, &plan, mode, confirm, show).await;

    let count = |p: fn(&Outcome) -> bool| results.iter().filter(|r| p(&r.outcome)).count();
    let applied = count(|o| matches!(o, Outcome::Applied(_) | Outcome::Validated(_)));
    let failed = count(|o| matches!(o, Outcome::Failed(_)));
    let skipped = count(|o| {
        matches!(
            o,
            Outcome::Changed | Outcome::Declined | Outcome::Stopped | Outcome::NotAllowed(_)
        )
    });
    let refused = count(|o| matches!(o, Outcome::Refused(_))) + excluded.len();
    let verb = if mode == ApplyMode::Validate {
        "valid"
    } else {
        "applied"
    };
    println!("{applied} {verb}, {failed} failed, {skipped} skipped, {refused} refused by policy or profile.");
    if !undo.is_empty() {
        match save_undo(&profile, undo) {
            Ok(path) => println!("Previous values saved to {}.", path.display()),
            Err(e) => eprintln!(
                "{} could not save the previous values: {e}",
                "Warning:".yellow()
            ),
        }
    }
    if failed > 0 {
        return Err(KafkaError::Check(format!("{failed} fix(es) failed")));
    }
    Ok(())
}

/// Asks for a group's name before deleting it.
fn confirm_name(group: &str) -> bool {
    use std::io::Write as _;
    eprint!("Deleting group '{group}' cannot be undone. Type its name to delete it: ");
    std::io::stderr().flush().ok();
    let mut answer = String::new();
    std::io::stdin().read_line(&mut answer).is_ok() && answer.trim() == group
}

fn options(args: &LintArgs, env: Env) -> LintOptions {
    LintOptions {
        env,
        rules: args.rules.clone(),
        skip: args.skip.iter().map(|s| parse_skip(s)).collect(),
        topics: args.topics.clone(),
    }
}

/// The env to grade for and why.
fn env_for(arg: EnvArg, global: &GlobalOpts) -> (Env, String) {
    match arg {
        EnvArg::Prod => (Env::Prod, "--env prod".to_string()),
        EnvArg::Dev => (Env::Dev, "--env dev".to_string()),
        EnvArg::Auto => lint::auto_env(&global.context()),
    }
}

fn env_name(env: Env) -> &'static str {
    match env {
        Env::Prod => "prod",
        Env::Dev => "dev",
    }
}

fn print_findings(findings: &[LintFinding]) {
    if findings.is_empty() {
        println!("{} No config problems found.", "✔".green().bold());
        return;
    }
    for f in findings {
        println!(
            "{} {} {} {}  {}",
            severity_tag(f.severity),
            f.rule.bold(),
            target_label(&f.target).cyan(),
            message_body(f),
            format!("[{}]", f.fix_class.label()).bright_black()
        );
        if let Some(line) = fix_line(f) {
            println!("         {} {line}", "fix:".bright_black());
        }
    }
}

/// The message without the target name it starts with (`orders: ...`,
/// `Broker 2: ...`); the table line already shows the target before it.
fn message_body(f: &LintFinding) -> &str {
    use crate::ops::health::HealthTarget;
    let prefix = match &f.target {
        HealthTarget::Topic(name) | HealthTarget::Group(name) => format!("{name}: "),
        HealthTarget::Broker(id) => format!("Broker {id}: "),
        HealthTarget::Brokers => return &f.message,
    };
    f.message.strip_prefix(&prefix).unwrap_or(&f.message)
}

/// One line on how to fix `f`: the change, the manual step or why there is
/// no fix.
fn fix_line(f: &LintFinding) -> Option<String> {
    match &f.fix {
        Fix::Manual { steps } => Some(steps.join("; ")),
        Fix::None { reason } => Some(format!("not now: {reason}")),
        _ => f.command.clone(),
    }
}

fn print_skipped(skipped: &[String], to_stderr: bool) {
    if skipped.is_empty() {
        return;
    }
    let shown: Vec<&str> = skipped.iter().take(10).map(String::as_str).collect();
    let more = skipped.len().saturating_sub(shown.len());
    let mut line = format!(
        "Could not check {} resource(s): {}",
        skipped.len(),
        shown.join(", ")
    );
    if more > 0 {
        line.push_str(&format!(" and {more} more"));
    }
    if to_stderr {
        eprintln!("{line}");
    } else {
        println!("{}", line.yellow());
    }
}

fn print_rules(global: &GlobalOpts) -> KafkaResult<()> {
    output::emit(global.format == OutputFormat::Json, RULES, |rules| {
        let mut table = output::table(vec!["RULE", "NAME", "PROD", "OTHER", "FIX", "SUMMARY"]);
        for r in rules {
            table.add_row(vec![
                r.id.to_string(),
                r.name.to_string(),
                format!("{:?}", r.severity_prod),
                format!("{:?}", r.severity_other),
                r.fix_class.label().to_string(),
                r.summary.to_string(),
            ]);
        }
        println!("{table}");
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser)]
    struct Cli {
        #[command(flatten)]
        args: LintArgs,
    }

    fn parse(args: &[&str]) -> LintArgs {
        Cli::try_parse_from(std::iter::once("lint").chain(args.iter().copied()))
            .unwrap_or_else(|e| panic!("{args:?}: {e}"))
            .args
    }

    #[test]
    fn lint_flags_parse() {
        let args = parse(&[
            "--topic",
            "orders-*",
            "--skip",
            "T001:orders-*",
            "--rule",
            "T002",
            "--env",
            "prod",
            "--fail-on",
            "warning",
        ]);
        assert_eq!(args.topics, vec!["orders-*"]);
        assert_eq!(args.env, EnvArg::Prod);
        assert_eq!(args.fail_on, FailOn::Warning);
        let opts = options(&args, Env::Prod);
        assert_eq!(opts.skip, vec![("T001".into(), Some("orders-*".into()))]);
        assert_eq!(parse(&[]).fail_on, FailOn::Never);
        assert_eq!(parse(&["rules"]).command, Some(LintCommand::Rules));

        let fix = parse(&["--fix", "--dry-run"]);
        assert!(fix.fix && fix.dry_run);
        assert!(
            Cli::try_parse_from(["lint", "--dry-run"]).is_err(),
            "needs --fix"
        );
        assert!(
            Cli::try_parse_from(["lint", "--fix", "--dry-run", "--save-plan", "p.json"]).is_err()
        );
    }

    fn finding(rule: &'static str, fix_class: FixClass, fix: Fix) -> LintFinding {
        LintFinding {
            rule,
            name: "",
            severity: crate::ops::health::Severity::Warning,
            target: crate::ops::health::HealthTarget::Topic("t".into()),
            message: String::new(),
            current: vec![("replication.factor".into(), "1".into())],
            command: None,
            fix,
            fix_class,
        }
    }

    #[test]
    fn message_body_drops_the_target_name_the_line_already_shows() {
        use crate::ops::health::HealthTarget;
        let mut f = finding("G002", FixClass::Guided, Fix::Manual { steps: vec![] });
        f.target = HealthTarget::Group("billing".into());
        f.message = "billing: no members and no committed offsets".into();
        assert_eq!(message_body(&f), "no members and no committed offsets");
        f.target = HealthTarget::Broker(2);
        f.message = "Broker 2: unclean leader election is on".into();
        assert_eq!(message_body(&f), "unclean leader election is on");
        // A message that does not start with the target stays whole.
        f.target = HealthTarget::Topic("orders".into());
        f.message = "orders-v2: something".into();
        assert_eq!(message_body(&f), "orders-v2: something");
        f.target = HealthTarget::Brokers;
        f.message = "default.replication.factor is 1".into();
        assert_eq!(message_body(&f), "default.replication.factor is 1");
    }

    #[test]
    fn fix_applies_auto_and_named_guided_fixes_only() {
        let auto = finding(
            "T002",
            FixClass::Auto,
            Fix::TopicConfig {
                topic: "t".into(),
                changes: vec![],
            },
        );
        let guided = finding(
            "T001",
            FixClass::Guided,
            Fix::Replication {
                topic: "t".into(),
                target: 3,
            },
        );
        let manual = finding("B005", FixClass::Manual, Fix::Manual { steps: vec![] });
        let blocked = finding(
            "T004",
            FixClass::Guided,
            Fix::None {
                reason: "isr".into(),
            },
        );
        assert!(selected(&auto, &[]));
        assert!(!selected(&guided, &[]));
        assert!(selected(&guided, &["t001".into()]));
        assert!(!selected(&manual, &["B005".into()]));
        assert!(!selected(&blocked, &["T004".into()]));
        assert_eq!(guided.change_summary(), "replication 1 -> 3");
    }

    /// Every `ferrika ...` command a fix suggests is one the CLI accepts.
    #[test]
    fn suggested_commands_parse() {
        use crate::kafka::wire::ConfigChange;
        #[derive(Parser)]
        struct Ferrika {
            #[command(subcommand)]
            command: Top,
        }
        #[derive(clap::Subcommand)]
        enum Top {
            Topics(crate::cli::topics::TopicsArgs),
            Offset(crate::cli::offset::OffsetArgs),
        }
        let fixes = [
            Fix::TopicConfig {
                topic: "orders".into(),
                changes: vec![
                    ConfigChange::Set("min.insync.replicas".into(), "2".into()),
                    ConfigChange::Delete("segment.bytes".into()),
                ],
            },
            Fix::Replication {
                topic: "orders".into(),
                target: 3,
            },
            Fix::ElectLeaders {
                partitions: vec![("orders".into(), 1), ("pay".into(), 0)],
            },
            Fix::DeleteGroup {
                group: "old-etl".into(),
            },
        ];
        for fix in fixes {
            let command = fix.cli_command("b1:9092").unwrap();
            let words: Vec<&str> = command.split_whitespace().collect();
            assert_eq!(words[0], "ferrika", "{command}");
            Ferrika::try_parse_from(&words).unwrap_or_else(|e| panic!("{command}: {e}"));
        }
        let manual = "ferrika offset reset stale -t orders --to-earliest --execute";
        Ferrika::try_parse_from(manual.split_whitespace()).unwrap();
    }
}
