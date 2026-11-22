use std::str::FromStr;

use clap::{Args, Subcommand};
use colored::Colorize;
use comfy_table::{Cell, CellAlignment, Color};

use crate::cli::record::{self, Change};
use crate::cli::{output, GlobalOpts};
use crate::error::{KafkaError, KafkaResult};
use crate::kafka::acl::{audit_security_rules, AclRiskLevel, AclStore};
use crate::kafka::config::KafkaClientConfig;
use crate::kafka::wire;
use crate::model::acl::{
    AclBinding, AclFilter, AclOperation, AclPermissionType, AclResourcePatternType, AclResourceType,
};

/// Command line arguments for Kafka ACL & Cluster Security management.
#[derive(Args, Debug, Clone)]
pub struct AclArgs {
    #[command(subcommand)]
    pub command: AclSubcommands,

    /// Work on the local policy file (~/.ferrika/acls.json) instead of the
    /// cluster's ACLs, e.g. to draft or audit rules offline
    #[arg(long, global = true)]
    pub local: bool,
}

/// Where ACL bindings are read from and written to.
enum AclBackend {
    /// The cluster's authorizer, through DescribeAcls/CreateAcls/DeleteAcls.
    Cluster(Box<KafkaClientConfig>),
    /// The local policy file.
    Local(AclStore),
}

impl AclBackend {
    fn new(local: bool, global: &GlobalOpts) -> Self {
        if local {
            Self::Local(AclStore::load_or_default())
        } else {
            Self::Cluster(Box::new(global.to_kafka_config()))
        }
    }

    /// Bindings matching `filter`; with `exact`, a named resource matches only
    /// its literal binding (the ones `delete` removes).
    fn list(&self, filter: &AclFilter, exact: bool) -> KafkaResult<Vec<AclBinding>> {
        match self {
            Self::Cluster(config) => wire::describe_acls(config, filter, exact),
            Self::Local(store) => Ok(if exact {
                store.list_exact(filter)
            } else {
                store.list(filter)
            }),
        }
    }

    /// Adds `binding`; returns false if it already existed.
    fn add(&mut self, binding: AclBinding) -> KafkaResult<bool> {
        match self {
            Self::Cluster(config) => {
                let exists = wire::describe_acls(config, &AclFilter::exact(&binding), true)?
                    .contains(&binding);
                if !exists {
                    wire::create_acls(config, std::slice::from_ref(&binding))?;
                }
                Ok(!exists)
            }
            Self::Local(store) => {
                let added = store.add(binding);
                if added {
                    store.save()?;
                }
                Ok(added)
            }
        }
    }

    /// Deletes `listed`, the bindings `filter` matched when they were shown.
    /// On the cluster only those are deleted (one exact filter each): the
    /// filter itself could also remove kinds the listing cannot show (User
    /// resources, token operations) and bindings added since, which the
    /// preview and the undo record would then miss.
    fn delete(&mut self, filter: &AclFilter, listed: &[AclBinding]) -> KafkaResult<usize> {
        match self {
            Self::Cluster(config) => wire::delete_acl_bindings(config, listed).map(|d| d.len()),
            Self::Local(store) => {
                let deleted = store.delete_exact(filter);
                store.save()?;
                Ok(deleted)
            }
        }
    }

    fn where_label(&self) -> &'static str {
        match self {
            Self::Cluster(_) => "cluster",
            Self::Local(_) => "local policy file",
        }
    }
}

/// Supported subcommands for `ferrika acl`.
#[derive(Subcommand, Debug, Clone, PartialEq)]
pub enum AclSubcommands {
    /// List ACL bindings matching specified filter criteria
    List(AclListArgs),
    /// Create a new Kafka ACL authorization grant or denial rule
    Create(AclCreateArgs),
    /// Delete matching ACL bindings from the cluster authorization store
    Delete(AclDeleteArgs),
    /// Run automated security audit across ACL rules to detect vulnerabilities
    Audit(AclAuditArgs),
}

/// Arguments for `ferrika acl list`.
#[derive(Args, Debug, Clone, PartialEq, Default)]
pub struct AclListArgs {
    /// Filter by principal identity (e.g. 'User:alice' or 'alice')
    #[arg(short = 'p', long)]
    pub principal: Option<String>,

    /// Filter by topic resource name
    #[arg(short = 't', long, conflicts_with_all = ["group", "cluster"])]
    pub topic: Option<String>,

    /// Filter by consumer group resource name
    #[arg(short = 'g', long, conflicts_with_all = ["topic", "cluster"])]
    pub group: Option<String>,

    /// Filter for cluster-level resource
    #[arg(short = 'c', long, conflicts_with_all = ["topic", "group"])]
    pub cluster: bool,

    /// Filter by operation (Read, Write, All, Create, Delete, Alter, Describe, ClusterAction)
    #[arg(short = 'o', long)]
    pub operation: Option<String>,

    /// Filter by permission type (Allow, Deny)
    #[arg(long)]
    pub permission: Option<String>,

    /// Filter by resource pattern type (Literal, Prefixed, Match)
    #[arg(long)]
    pub pattern_type: Option<String>,

    /// Print output in JSON format
    #[arg(long)]
    pub json: bool,
}

/// Arguments for `ferrika acl create`.
#[derive(Args, Debug, Clone, PartialEq)]
pub struct AclCreateArgs {
    /// Principal identity to authorize (e.g. 'User:billing-svc' or 'billing-svc')
    #[arg(short = 'p', long)]
    pub principal: String,

    /// Authorize permission as ALLOW (Default)
    #[arg(long, default_value_t = true, conflicts_with = "deny")]
    pub allow: bool,

    /// Authorize permission as DENY
    #[arg(long, conflicts_with = "allow")]
    pub deny: bool,

    /// Operation permitted or denied (Read, Write, All, Create, Delete, Alter, Describe, ClusterAction)
    #[arg(short = 'o', long)]
    pub operation: String,

    /// Apply ACL to topic resource
    #[arg(short = 't', long, conflicts_with_all = ["group", "cluster"])]
    pub topic: Option<String>,

    /// Apply ACL to consumer group resource
    #[arg(short = 'g', long, conflicts_with_all = ["topic", "cluster"])]
    pub group: Option<String>,

    /// Apply ACL to cluster-wide resource
    #[arg(short = 'c', long, conflicts_with_all = ["topic", "group"])]
    pub cluster: bool,

    /// Pattern type matching mode (Literal or Prefixed)
    #[arg(long, default_value = "literal")]
    pub pattern_type: String,

    /// Client network host IP or CIDR (default: '*' for all hosts)
    #[arg(short = 'H', long, default_value = "*")]
    pub host: String,
}

/// Arguments for `ferrika acl delete`.
#[derive(Args, Debug, Clone, PartialEq, Default)]
pub struct AclDeleteArgs {
    /// Match principal identity to delete
    #[arg(short = 'p', long)]
    pub principal: Option<String>,

    /// Match topic resource name
    #[arg(short = 't', long, conflicts_with_all = ["group", "cluster"])]
    pub topic: Option<String>,

    /// Match consumer group resource name
    #[arg(short = 'g', long, conflicts_with_all = ["topic", "cluster"])]
    pub group: Option<String>,

    /// Match cluster resource
    #[arg(short = 'c', long, conflicts_with_all = ["topic", "group"])]
    pub cluster: bool,

    /// Match operation
    #[arg(short = 'o', long)]
    pub operation: Option<String>,

    /// Match permission type (Allow, Deny)
    #[arg(long)]
    pub permission: Option<String>,

    /// Force deletion without interactive prompt
    #[arg(short = 'f', long)]
    pub force: bool,
}

/// Arguments for `ferrika acl audit`.
#[derive(Args, Debug, Clone, PartialEq, Default)]
pub struct AclAuditArgs {
    /// Output security findings in JSON format
    #[arg(long)]
    pub json: bool,
}

/// Executes the requested `ferrika acl` subcommand.
pub fn run(args: AclArgs, global: &GlobalOpts) -> KafkaResult<()> {
    let backend = AclBackend::new(args.local, global);
    match args.command {
        AclSubcommands::List(list_args) => run_list(list_args, &backend, global),
        AclSubcommands::Create(create_args) => run_create(create_args, backend, global),
        AclSubcommands::Delete(delete_args) => run_delete(delete_args, backend, global),
        AclSubcommands::Audit(audit_args) => run_audit(audit_args, &backend, global),
    }
}

/// Builds an `AclFilter` from user-supplied CLI criteria
fn build_filter(
    principal: Option<String>,
    topic: Option<String>,
    group: Option<String>,
    cluster: bool,
    operation: Option<String>,
    permission: Option<String>,
    pattern_type: Option<String>,
) -> KafkaResult<AclFilter> {
    let (resource_type, resource_name) = if cluster {
        (
            Some(AclResourceType::Cluster),
            Some("kafka-cluster".to_string()),
        )
    } else if let Some(t) = topic {
        (Some(AclResourceType::Topic), Some(t))
    } else if let Some(g) = group {
        (Some(AclResourceType::Group), Some(g))
    } else {
        (None, None)
    };

    let op = match operation {
        Some(s) => Some(AclOperation::from_str(&s)?),
        None => None,
    };

    let perm = match permission {
        Some(s) => Some(AclPermissionType::from_str(&s)?),
        None => None,
    };

    let pt = match pattern_type {
        Some(s) => Some(AclResourcePatternType::from_str(&s)?),
        None => None,
    };

    Ok(AclFilter {
        resource_type,
        resource_name,
        pattern_type: pt,
        principal,
        host: None,
        operation: op,
        permission_type: perm,
    })
}

/// Handles `ferrika acl list`
fn run_list(args: AclListArgs, backend: &AclBackend, global: &GlobalOpts) -> KafkaResult<()> {
    let filter = build_filter(
        args.principal,
        args.topic,
        args.group,
        args.cluster,
        args.operation,
        args.permission,
        args.pattern_type,
    )?;

    let bindings = backend.list(&filter, false)?;

    if global.wants_json(args.json) {
        output::print_json(&bindings)?;
        return Ok(());
    }

    if bindings.is_empty() {
        println!(
            "{}",
            "No ACL bindings match the specified criteria.".yellow()
        );
        return Ok(());
    }

    let mut table = output::table(vec![
        Cell::new("Resource Type")
            .fg(Color::Cyan)
            .set_alignment(CellAlignment::Left),
        Cell::new("Resource Name")
            .fg(Color::Cyan)
            .set_alignment(CellAlignment::Left),
        Cell::new("Pattern")
            .fg(Color::Cyan)
            .set_alignment(CellAlignment::Center),
        Cell::new("Principal")
            .fg(Color::Cyan)
            .set_alignment(CellAlignment::Left),
        Cell::new("Host")
            .fg(Color::Cyan)
            .set_alignment(CellAlignment::Center),
        Cell::new("Operation")
            .fg(Color::Cyan)
            .set_alignment(CellAlignment::Center),
        Cell::new("Permission")
            .fg(Color::Cyan)
            .set_alignment(CellAlignment::Center),
    ]);

    for b in &bindings {
        let perm_cell = match b.permission_type {
            AclPermissionType::Allow => Cell::new("ALLOW")
                .fg(Color::Green)
                .set_alignment(CellAlignment::Center),
            AclPermissionType::Deny => Cell::new("DENY")
                .fg(Color::Red)
                .set_alignment(CellAlignment::Center),
        };

        let op_color = match b.operation {
            AclOperation::All | AclOperation::ClusterAction => Color::Magenta,
            AclOperation::Write | AclOperation::Delete | AclOperation::Alter => Color::Yellow,
            _ => Color::White,
        };

        table.add_row(vec![
            Cell::new(b.resource_type.to_string()),
            Cell::new(&b.resource_name),
            Cell::new(b.pattern_type.to_string()).set_alignment(CellAlignment::Center),
            Cell::new(&b.principal),
            Cell::new(&b.host).set_alignment(CellAlignment::Center),
            Cell::new(b.operation.to_string())
                .fg(op_color)
                .set_alignment(CellAlignment::Center),
            perm_cell,
        ]);
    }

    println!("{table}");
    println!(
        "\n{} Total ACL bindings displayed: {}",
        "✓".green().bold(),
        bindings.len().to_string().cyan().bold()
    );

    Ok(())
}

/// Handles `ferrika acl create`
fn run_create(
    args: AclCreateArgs,
    mut backend: AclBackend,
    global: &GlobalOpts,
) -> KafkaResult<()> {
    let (resource_type, resource_name) = if args.cluster {
        (AclResourceType::Cluster, "kafka-cluster".to_string())
    } else if let Some(t) = args.topic {
        (AclResourceType::Topic, t)
    } else if let Some(g) = args.group {
        (AclResourceType::Group, g)
    } else {
        return Err(KafkaError::acl(
            "Must specify target resource: --topic <name>, --group <id>, or --cluster".to_string(),
        ));
    };

    let pattern_type = AclResourcePatternType::from_str(&args.pattern_type)?;
    let operation = AclOperation::from_str(&args.operation)?;
    let permission_type = if args.deny {
        AclPermissionType::Deny
    } else {
        AclPermissionType::Allow
    };

    let binding = AclBinding::new(
        resource_type,
        resource_name,
        pattern_type,
        args.principal,
        args.host,
        operation,
        permission_type,
    );

    let existed = backend
        .list(&AclFilter::exact(&binding), true)
        .map(|found| found.contains(&binding));
    let pending = record::start(
        global,
        record::cluster_scope(global, None),
        record::change_from_capture(
            crate::model::history::Reversibility::Reversible,
            existed.map(|existed| {
                (
                    vec![acl_state(std::slice::from_ref(&binding), existed)],
                    vec![acl_state(std::slice::from_ref(&binding), true)],
                )
            }),
        ),
    );
    let added = backend.add(binding.clone());
    record::finish(
        pending,
        &added,
        match &added {
            Ok(false) => "ACL binding already existed; no change made",
            _ => "Created the ACL binding",
        },
    );
    let added = added?;
    if added {
        println!(
            "{} Created ACL binding in the {}: {} {} on {} '{}' for {}",
            "✓".green().bold(),
            backend.where_label(),
            binding.permission_type,
            binding.operation.to_string().cyan(),
            binding.resource_type,
            binding.resource_name.bold(),
            binding.principal.yellow()
        );
    } else {
        println!(
            "{} ACL binding already exists; no change made.",
            "!".yellow().bold()
        );
    }

    Ok(())
}

/// Handles `ferrika acl delete`
fn run_delete(
    args: AclDeleteArgs,
    mut backend: AclBackend,
    global: &GlobalOpts,
) -> KafkaResult<()> {
    let filter = build_filter(
        args.principal,
        args.topic,
        args.group,
        args.cluster,
        args.operation,
        args.permission,
        None,
    )?;

    let matching = backend.list(&filter, true)?;
    if matching.is_empty() {
        println!(
            "{}",
            "No ACL bindings match the specified deletion criteria.".yellow()
        );
        return Ok(());
    }

    println!(
        "Found {} matching ACL binding(s) to delete:",
        matching.len().to_string().cyan().bold()
    );
    for b in &matching {
        println!(
            "  • [{}] {} {} on {} '{}' ({})",
            b.permission_type, b.principal, b.operation, b.resource_type, b.resource_name, b.host
        );
    }

    if !args.force {
        println!(
            "\n{}",
            "Run with '--force' (-f) to confirm and execute deletion.".yellow()
        );
        return Ok(());
    }

    let pending = record::start(
        global,
        record::cluster_scope(global, None),
        Change::new(
            crate::model::history::Reversibility::Reversible,
            vec![acl_state(&matching, true)],
            vec![acl_state(&matching, false)],
        ),
    );
    let deleted = backend.delete(&filter, &matching);
    record::finish(
        pending,
        &deleted,
        &match &deleted {
            Ok(n) => format!("Deleted {n} ACL binding(s)"),
            Err(_) => String::new(),
        },
    );
    let deleted_count = deleted?;

    println!(
        "\n{} Successfully deleted {} ACL binding(s).",
        "✓".green().bold(),
        deleted_count.to_string().cyan().bold()
    );

    Ok(())
}

/// `bindings`, each present or absent, as a history state.
fn acl_state(bindings: &[AclBinding], present: bool) -> crate::model::history::State {
    crate::model::history::State::Acls {
        bindings: bindings
            .iter()
            .map(|binding| crate::model::history::AclPresence {
                binding: binding.clone(),
                present,
            })
            .collect(),
    }
}

/// Handles `ferrika acl audit`
fn run_audit(args: AclAuditArgs, backend: &AclBackend, global: &GlobalOpts) -> KafkaResult<()> {
    let bindings = backend.list(&AclFilter::default(), false)?;
    let findings = audit_security_rules(&bindings);

    if global.wants_json(args.json) {
        output::print_json(&findings)?;
        return Ok(());
    }

    println!(
        "\n{}",
        "🛡️  Ferrika Kafka Cluster Security & ACL Audit".bold()
    );
    println!("═════════════════════════════════════════════════════════════════════════════");

    if findings.is_empty() {
        println!(
            "\n{} All evaluated ACL rules adhere to security best practices. No critical risks detected.\n",
            "✓".green().bold()
        );
        return Ok(());
    }

    let critical_count = findings
        .iter()
        .filter(|f| f.risk_level == AclRiskLevel::Critical)
        .count();
    let high_count = findings
        .iter()
        .filter(|f| f.risk_level == AclRiskLevel::High)
        .count();
    let medium_count = findings
        .iter()
        .filter(|f| f.risk_level == AclRiskLevel::Medium)
        .count();

    println!(
        "Security Assessment Summary: {} findings ({} Critical, {} High, {} Medium)\n",
        findings.len().to_string().cyan().bold(),
        critical_count.to_string().red().bold(),
        high_count.to_string().yellow().bold(),
        medium_count.to_string().blue().bold(),
    );

    let mut table = output::table(vec![
        Cell::new("Risk")
            .fg(Color::Cyan)
            .set_alignment(CellAlignment::Center),
        Cell::new("Security Issue")
            .fg(Color::Cyan)
            .set_alignment(CellAlignment::Left),
        Cell::new("Affected Binding")
            .fg(Color::Cyan)
            .set_alignment(CellAlignment::Left),
        Cell::new("Remediation Recommendation")
            .fg(Color::Cyan)
            .set_alignment(CellAlignment::Left),
    ]);

    for f in &findings {
        let risk_cell = match f.risk_level {
            AclRiskLevel::Critical => Cell::new("CRITICAL")
                .fg(Color::Red)
                .set_alignment(CellAlignment::Center),
            AclRiskLevel::High => Cell::new("HIGH")
                .fg(Color::Yellow)
                .set_alignment(CellAlignment::Center),
            AclRiskLevel::Medium => Cell::new("MEDIUM")
                .fg(Color::Blue)
                .set_alignment(CellAlignment::Center),
            AclRiskLevel::Low | AclRiskLevel::Info => Cell::new("LOW")
                .fg(Color::DarkGrey)
                .set_alignment(CellAlignment::Center),
        };

        let binding_summary = format!(
            "[{}] {} {} on {} '{}'",
            f.binding.permission_type,
            f.binding.principal,
            f.binding.operation,
            f.binding.resource_type,
            f.binding.resource_name
        );

        table.add_row(vec![
            risk_cell,
            Cell::new(format!("{}\n{}", f.title.bold(), f.description)),
            Cell::new(binding_summary),
            Cell::new(&f.recommendation).fg(Color::Green),
        ]);
    }

    println!("{table}\n");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_clap_acl_args_list_parsing() {
        use clap::Parser;

        #[derive(Parser, Debug)]
        struct Wrapper {
            #[command(subcommand)]
            cmd: AclSubcommands,
        }

        let parsed = Wrapper::parse_from(["acl", "list", "-p", "alice", "-t", "orders", "--json"]);
        match parsed.cmd {
            AclSubcommands::List(args) => {
                assert_eq!(args.principal.as_deref(), Some("alice"));
                assert_eq!(args.topic.as_deref(), Some("orders"));
                assert!(args.json);
            }
            _ => panic!("Expected List subcommand"),
        }
    }

    #[test]
    fn test_clap_acl_args_create_parsing() {
        use clap::Parser;

        #[derive(Parser, Debug)]
        struct Wrapper {
            #[command(subcommand)]
            cmd: AclSubcommands,
        }

        let parsed = Wrapper::parse_from([
            "acl",
            "create",
            "-p",
            "User:bob",
            "-o",
            "Write",
            "-t",
            "telemetry",
            "--pattern-type",
            "literal",
        ]);

        match parsed.cmd {
            AclSubcommands::Create(args) => {
                assert_eq!(args.principal, "User:bob");
                assert_eq!(args.operation, "Write");
                assert_eq!(args.topic.as_deref(), Some("telemetry"));
                assert!(args.allow);
                assert!(!args.deny);
            }
            _ => panic!("Expected Create subcommand"),
        }
    }

    #[test]
    fn test_clap_acl_args_delete_and_audit_parsing() {
        use clap::Parser;

        #[derive(Parser, Debug)]
        struct Wrapper {
            #[command(subcommand)]
            cmd: AclSubcommands,
        }

        let parsed_delete = Wrapper::parse_from(["acl", "delete", "-p", "alice", "-f"]);
        match parsed_delete.cmd {
            AclSubcommands::Delete(args) => {
                assert_eq!(args.principal.as_deref(), Some("alice"));
                assert!(args.force);
            }
            _ => panic!("Expected Delete subcommand"),
        }

        let parsed_audit = Wrapper::parse_from(["acl", "audit", "--json"]);
        match parsed_audit.cmd {
            AclSubcommands::Audit(args) => {
                assert!(args.json);
            }
            _ => panic!("Expected Audit subcommand"),
        }
    }
}
