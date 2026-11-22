//! `ferrika rabbit` (preview): RabbitMQ queues, exchanges and bindings,
//! peeking at messages, publishing, dead-letter inspection and redrive, and
//! RabbitMQ connection profiles.
//!
//! The policy and audit gates in `cli/app.rs` cover these commands like the
//! Kafka ones. Writes: `publish` (policy category `produce`); `peek`, `dlq
//! inspect` and the `dlq redrive` preview (`messages.peek`: the management
//! API has no browse, so reading takes the messages and requeues them,
//! marked redelivered); `dlq redrive --execute` (`messages.move`). `peek`
//! and `dlq inspect` also show record data, so they pass the export gate.
//! The gates match the management API's `host:port` and the RabbitMQ
//! profile, never a Kafka profile.

use std::collections::BTreeMap;
use std::time::Duration;

use clap::{Args, Subcommand};
use colored::Colorize;
use comfy_table::Cell;
use serde::Serialize;
use serde_json::{Map, Value};

use crate::cli::{output, GlobalOpts, OutputFormat};
use crate::codec::pii::{mask_bytes, mask_header_value, MaskStrategy};
use crate::rabbitmq::amqp::{AmqpTarget, RedriveRequest};
use crate::rabbitmq::client::ManagementClient;
use crate::rabbitmq::model::{
    DeathRecord, OutgoingMessage, PeekedMessage, RedriveDestination, DEFAULT_EXCHANGE, MAX_PEEK,
};
use crate::rabbitmq::profile::{RabbitProfile, RabbitProfileStore};

/// Environment variable with the RabbitMQ password.
pub const PASSWORD_ENV: &str = "FERRIKA_RABBIT_PASSWORD";

/// Request timeout when `--timeout` is not given. The Kafka profile's
/// timeout does not apply: these commands do not use a Kafka profile.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// Printed once per command when the management URL carries credentials.
/// Names no user or password.
const URL_CREDENTIALS_WARNING: &str =
    "Warning: the RabbitMQ URL contains credentials, which can end up in shell history. \
     Store them in a profile instead: ferrika rabbit profile add NAME --rabbit-url URL \
     --rabbit-user USER --rabbit-password-stdin";

/// Longest payload shown in the `peek` table.
const TABLE_PAYLOAD_CHARS: usize = 120;

#[derive(Args, Debug, Clone)]
pub struct RabbitArgs {
    #[command(subcommand)]
    pub command: RabbitSubcommands,
    #[command(flatten)]
    pub connection: RabbitConnection,
    /// The connection the options and the RabbitMQ profile name, filled in
    /// by `main` before the policy gates run (see [`resolve`]).
    #[arg(skip)]
    pub resolved: Option<ResolvedRabbit>,
}

/// Which RabbitMQ to talk to: a profile, overridden by the options.
#[derive(Args, Clone)]
pub struct RabbitConnection {
    /// RabbitMQ profile (default: the active one, unless --rabbit-url is
    /// given without --rabbit-profile)
    #[arg(
        long = "rabbit-profile",
        env = "FERRIKA_RABBIT_PROFILE",
        global = true,
        value_name = "NAME"
    )]
    pub profile: Option<String>,
    /// RabbitMQ management API URL, e.g. http://rabbit:15672 (port 15672
    /// when left out); overrides the profile's
    #[arg(
        long = "rabbit-url",
        env = "FERRIKA_RABBIT_URL",
        global = true,
        value_name = "URL"
    )]
    pub url: Option<String>,
    /// Virtual host (default: the profile's, else /)
    #[arg(long, env = "FERRIKA_RABBIT_VHOST", global = true)]
    pub vhost: Option<String>,
    /// RabbitMQ user (else the profile's, else the user in the URL)
    #[arg(long = "rabbit-user", env = "FERRIKA_RABBIT_USER", global = true)]
    pub user: Option<String>,
    /// RabbitMQ password; prefer the environment variable or
    /// --rabbit-password-stdin, which keep it out of the process list
    #[arg(
        long = "rabbit-password",
        env = "FERRIKA_RABBIT_PASSWORD",
        hide_env_values = true,
        global = true
    )]
    pub password: Option<String>,
    /// Read the password from the first line of stdin (wins over the
    /// environment variable)
    #[arg(long = "rabbit-password-stdin", global = true)]
    pub password_stdin: bool,
    /// AMQP URL for moving messages, e.g. amqps://rabbit:5671 (default: the
    /// profile's, else the management host on 5672, or 5671 when the
    /// management URL is https)
    #[arg(
        long = "amqp-url",
        env = "FERRIKA_RABBIT_AMQP_URL",
        global = true,
        value_name = "URL"
    )]
    pub amqp_url: Option<String>,
}

// Manual so the password never reaches debug output.
impl std::fmt::Debug for RabbitConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RabbitConnection")
            .field("profile", &self.profile)
            .field(
                "url",
                &self.url.as_deref().map(crate::model::format::redact_url),
            )
            .field("vhost", &self.vhost)
            .field("user", &self.user)
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .field("password_stdin", &self.password_stdin)
            .field(
                "amqp_url",
                &self
                    .amqp_url
                    .as_deref()
                    .map(crate::model::format::redact_url),
            )
            .finish()
    }
}

impl RabbitConnection {
    /// The `--rabbit-url` given, if any (blank counts as none).
    fn given_url(&self) -> Option<&str> {
        self.url.as_deref().map(str::trim).filter(|u| !u.is_empty())
    }

    /// The password given by option, environment or stdin (read once).
    fn given_password(&self) -> Result<Option<String>, String> {
        if self.password_stdin {
            crate::cli::read_password_stdin().map(Some)
        } else {
            Ok(self.password.clone())
        }
    }
}

/// The connection a command uses: options over the RabbitMQ profile.
#[derive(Clone, PartialEq, Eq)]
pub struct ResolvedRabbit {
    /// The RabbitMQ profile used, if any: what `read_only` globs match.
    pub profile: Option<String>,
    /// The profile is marked read-only.
    pub read_only: bool,
    pub management_url: String,
    pub vhost: String,
    pub user: Option<String>,
    /// The profile's password (from the OS credential store); a password
    /// given by option or environment wins.
    profile_password: Option<String>,
    pub amqp_url: Option<String>,
}

// Manual so the password never reaches debug output.
impl std::fmt::Debug for ResolvedRabbit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResolvedRabbit")
            .field("profile", &self.profile)
            .field("read_only", &self.read_only)
            .field(
                "management_url",
                &crate::model::format::redact_url(&self.management_url),
            )
            .field("vhost", &self.vhost)
            .field("user", &self.user)
            .field(
                "profile_password",
                &self.profile_password.as_ref().map(|_| "<redacted>"),
            )
            .field("amqp_url", &self.amqp_url)
            .finish()
    }
}

impl ResolvedRabbit {
    /// `host:port` of the management API (no credentials), which the
    /// policy's broker-address rules and the audit log see.
    pub fn address(&self) -> Result<String, String> {
        ManagementClient::new(&self.management_url)
            .map(|client| client.address())
            .map_err(|e| e.to_string())
    }

    /// The AMQP URL: `--amqp-url`, the profile's, or the management host
    /// on the AMQP port.
    fn amqp_url(&self) -> Result<String, String> {
        match &self.amqp_url {
            Some(url) => Ok(url.clone()),
            None => crate::rabbitmq::profile::derive_amqp_url(&self.management_url)
                .map_err(|e| e.to_string()),
        }
    }

    /// The addresses the policy's broker-address rules match, as a
    /// comma-separated list: the management API's `host:port` and the AMQP
    /// listener's, so a rule naming either covers every command on this
    /// connection.
    pub fn gate_addresses(&self) -> Result<String, String> {
        let management = self.address()?;
        let amqp = crate::rabbitmq::profile::amqp_host_port(&self.amqp_url()?)
            .map_err(|e| e.to_string())?;
        Ok(if amqp == management {
            management
        } else {
            format!("{management},{amqp}")
        })
    }

    /// Whether the management URL carries a user or password.
    fn url_has_credentials(&self) -> bool {
        crate::rabbitmq::profile::url_credentials(&self.management_url).is_some()
    }

    /// The user and password to log in with, or `None` for credentials in
    /// the URL (or none). Reads stdin for `--rabbit-password-stdin`, so
    /// call it once.
    fn credentials(
        &self,
        connection: &RabbitConnection,
    ) -> Result<Option<(String, String)>, String> {
        let password = connection
            .given_password()?
            .or_else(|| self.profile_password.clone());
        match (&self.user, password) {
            (Some(user), Some(password)) => Ok(Some((user.clone(), password))),
            (Some(_), None) => Err(format!(
                "The RabbitMQ user needs a password: set {PASSWORD_ENV}, pass \
                 --rabbit-password-stdin or store one in the profile"
            )),
            (None, Some(_)) => {
                Err("A RabbitMQ password was given without --rabbit-user".to_string())
            }
            // Credentials in the URL, or none.
            (None, None) => Ok(None),
        }
    }

    /// The management API client.
    fn client(
        &self,
        credentials: Option<&(String, String)>,
        timeout: Duration,
    ) -> Result<ManagementClient, String> {
        let client = ManagementClient::new(&self.management_url)
            .map_err(|e| e.to_string())?
            .with_timeout(timeout);
        Ok(match credentials {
            Some((user, password)) => client.with_credentials(user, password),
            None => client,
        })
    }

    /// Where to connect over AMQP (see [`Self::amqp_url`]), logged in as
    /// `credentials`, else as the user and password in the management URL
    /// (the same RabbitMQ user serves both).
    fn amqp_target(
        &self,
        credentials: Option<&(String, String)>,
        timeout: Duration,
    ) -> Result<AmqpTarget, String> {
        let login = credentials
            .cloned()
            .or_else(|| crate::rabbitmq::profile::url_credentials(&self.management_url));
        Ok(AmqpTarget {
            url: self.amqp_url()?,
            vhost: self.vhost.clone(),
            user: login.as_ref().map(|(user, _)| user.clone()),
            password: login.map(|(_, password)| password),
            timeout,
        })
    }
}

/// Fills in `args.resolved` for the commands that talk to a broker (not
/// `rabbit profile`), loading the RabbitMQ profiles when needed.
pub fn resolve(args: &mut RabbitArgs) -> Result<(), String> {
    if !needs_connection(&args.command) {
        return Ok(());
    }
    let resolved = resolve_with(&args.connection, || {
        RabbitProfileStore::load().map_err(|e| e.to_string())
    })?;
    args.resolved = Some(resolved);
    Ok(())
}

/// Whether `command` talks to a broker. No wildcard arm.
fn needs_connection(command: &RabbitSubcommands) -> bool {
    match command {
        RabbitSubcommands::Profile(_) => false,
        RabbitSubcommands::Queues
        | RabbitSubcommands::Exchanges
        | RabbitSubcommands::Bindings
        | RabbitSubcommands::Peek { .. }
        | RabbitSubcommands::Publish { .. }
        | RabbitSubcommands::Dlq(_) => true,
    }
}

/// The connection for `connection`: the profile it names, else the active
/// profile unless `--rabbit-url` is given (a URL without
/// `--rabbit-profile` means no profile: a profile's password is never sent
/// to a broker the user named by hand); each option overrides the
/// profile's value. `load_store` runs only when a profile can apply.
fn resolve_with(
    connection: &RabbitConnection,
    load_store: impl FnOnce() -> Result<RabbitProfileStore, String>,
) -> Result<ResolvedRabbit, String> {
    let profile_name = connection
        .profile
        .as_deref()
        .map(str::trim)
        .filter(|p| !p.is_empty());
    let wants_profile = profile_name.is_some() || connection.given_url().is_none();
    let chosen: Option<(String, RabbitProfile)> = if wants_profile {
        let store = load_store()?;
        match profile_name {
            Some(name) => Some((
                name.to_string(),
                store.get(name).map_err(|e| e.to_string())?.clone(),
            )),
            None => store.active().map(|(n, p)| (n.to_string(), p.clone())),
        }
    } else {
        None
    };
    let profile = chosen.as_ref().map(|(_, p)| p);
    let management_url = connection
        .given_url()
        .map(str::to_string)
        .or_else(|| profile.map(|p| p.management_url.clone()))
        .ok_or_else(|| {
            "No RabbitMQ connection: pass --rabbit-url http://host:15672 (or set \
             FERRIKA_RABBIT_URL), or add a profile with `ferrika rabbit profile add NAME \
             --rabbit-url URL`"
                .to_string()
        })?;
    let user = connection
        .user
        .clone()
        .or_else(|| profile.and_then(|p| p.user.clone()));
    // The stored password belongs to the profile's user only.
    let profile_password = profile
        .filter(|p| connection.user.is_none() || connection.user == p.user)
        .and_then(|p| p.password.clone());
    Ok(ResolvedRabbit {
        profile: chosen.as_ref().map(|(name, _)| name.clone()),
        read_only: profile.is_some_and(|p| p.read_only),
        management_url,
        vhost: connection
            .vhost
            .clone()
            .or_else(|| profile.map(|p| p.vhost.clone()))
            .unwrap_or_else(|| "/".to_string()),
        user,
        profile_password,
        amqp_url: connection
            .amqp_url
            .as_deref()
            .map(str::trim)
            .filter(|u| !u.is_empty())
            .map(str::to_string)
            .or_else(|| profile.and_then(|p| p.amqp_url.clone())),
    })
}

/// The addresses the policy gates match for `args`: the management API's
/// and the AMQP listener's `host:port` (see
/// [`ResolvedRabbit::gate_addresses`]).
pub fn gate_address(args: &RabbitArgs) -> Result<String, String> {
    args.resolved
        .as_ref()
        .ok_or_else(|| "No RabbitMQ connection was resolved".to_string())?
        .gate_addresses()
}

/// The profile name the gates and the audit log use: the RabbitMQ
/// profile, or empty (the entries then name the address).
pub fn gate_profile(args: &RabbitArgs) -> String {
    args.resolved
        .as_ref()
        .and_then(|r| r.profile.clone())
        .unwrap_or_default()
}

/// Refuses `action` on a RabbitMQ profile marked read-only.
pub fn ensure_writable(args: &RabbitArgs, action: &str) -> Result<(), String> {
    match &args.resolved {
        Some(ResolvedRabbit {
            read_only: true,
            profile: Some(name),
            ..
        }) => Err(format!(
            "RabbitMQ profile '{name}' is read-only, so `{action}` is refused. Re-add it \
             without --read-only to allow changes."
        )),
        _ => Ok(()),
    }
}

#[derive(Subcommand, Debug, Clone, PartialEq, Eq)]
pub enum RabbitSubcommands {
    /// List the queues of the vhost with message counts and dead-letter exchange
    Queues,
    /// List the exchanges of the vhost
    Exchanges,
    /// List the bindings of the vhost
    Bindings,
    /// Show messages at the head of a queue. They are taken and requeued,
    /// so the broker marks them redelivered: a write under the company
    /// policy (category messages.peek)
    Peek {
        queue: String,
        /// Number of messages
        #[arg(short = 'n', long = "count", default_value_t = 10)]
        count: u32,
        /// Mask PII in payloads and headers (always on when a company policy
        /// enforces masking)
        #[arg(long = "mask-pii")]
        mask_pii: bool,
    },
    /// Publish one message to an exchange (amq.default is the default exchange)
    Publish {
        exchange: String,
        /// Routing key
        #[arg(short = 'k', long = "routing-key", default_value = "")]
        routing_key: String,
        /// Message body (UTF-8 text)
        #[arg(short = 'm', long)]
        message: String,
        /// Headers in KEY:VALUE format (can be given several times)
        #[arg(short = 'H', long = "header")]
        headers: Vec<String>,
        /// content_type property, e.g. application/json
        #[arg(long)]
        content_type: Option<String>,
        /// Send with delivery_mode 1 (not persisted) instead of 2
        #[arg(long)]
        transient: bool,
    },
    /// Dead-lettered messages: why they died and moving them back
    Dlq(RabbitDlqArgs),
    /// Named RabbitMQ connections (rabbit-profiles.json, passwords in the OS
    /// credential store)
    Profile(crate::cli::rabbit_profile::RabbitProfileArgs),
}

#[derive(Args, Debug, Clone, PartialEq, Eq)]
pub struct RabbitDlqArgs {
    #[command(subcommand)]
    pub command: RabbitDlqSubcommands,
}

#[derive(Subcommand, Debug, Clone, PartialEq, Eq)]
pub enum RabbitDlqSubcommands {
    /// Show where a dead-letter queue's messages came from and why (x-death);
    /// they are taken and requeued like `peek`
    Inspect {
        queue: String,
        /// Number of messages to read
        #[arg(short = 'n', long = "count", default_value_t = 50)]
        count: u32,
        /// Mask PII in payloads and headers (always on when a company policy
        /// enforces masking)
        #[arg(long = "mask-pii")]
        mask_pii: bool,
    },
    /// Move dead-lettered messages back to their original exchange and
    /// routing key (from x-death). A preview unless --execute. Redriven
    /// messages keep their headers, x-death included, plus
    /// x-ferrika-redriven-from. Needs the policy categories messages.move
    /// and produce
    Redrive {
        queue: String,
        /// Publish to this exchange instead of each message's original one
        #[arg(long, value_name = "EXCHANGE")]
        to_exchange: Option<String>,
        /// Publish with this routing key instead of each message's original one
        #[arg(short = 'k', long, value_name = "KEY")]
        routing_key: Option<String>,
        /// The most messages to move (the preview shows at most 500)
        #[arg(short = 'n', long = "count", default_value_t = 100)]
        count: u64,
        /// Move the messages over AMQP (publisher confirms, ack after the
        /// confirm); without it nothing is moved
        #[arg(long)]
        execute: bool,
        /// Do not ask for confirmation
        #[arg(short = 'y', long, requires = "execute")]
        yes: bool,
    },
}

/// The write the gate checks for `args`, as the audit log names it, or
/// `None` for reads. No wildcard arm: a new subcommand must be classified.
pub fn write_action(args: &RabbitArgs) -> Option<&'static str> {
    use crate::cli::rabbit_profile::RabbitProfileSubcommands as P;
    match &args.command {
        RabbitSubcommands::Publish { .. } => Some("rabbit publish"),
        // Reading through the management API requeues: a write.
        RabbitSubcommands::Peek { .. } => Some("rabbit peek"),
        RabbitSubcommands::Dlq(dlq) => match &dlq.command {
            RabbitDlqSubcommands::Inspect { .. } => Some("rabbit dlq inspect"),
            RabbitDlqSubcommands::Redrive { execute: true, .. } => {
                Some("rabbit dlq redrive --execute")
            }
            RabbitDlqSubcommands::Redrive { execute: false, .. } => Some("rabbit dlq redrive"),
        },
        RabbitSubcommands::Queues | RabbitSubcommands::Exchanges | RabbitSubcommands::Bindings => {
            None
        }
        // Local configuration only.
        RabbitSubcommands::Profile(profile) => match &profile.command {
            P::Add(_) | P::List | P::Use { .. } | P::Remove { .. } | P::Show { .. } => None,
        },
    }
}

/// The vhost commands use: the resolved one, else the option, else `/`.
fn vhost(args: &RabbitArgs) -> &str {
    match &args.resolved {
        Some(resolved) => &resolved.vhost,
        None => args.connection.vhost.as_deref().unwrap_or("/"),
    }
}

/// What a write applies to, for the audit log: exchange, queue, vhost and
/// routing keys, never message bodies.
pub fn write_target(args: &RabbitArgs) -> String {
    use crate::cli::rabbit_profile::RabbitProfileSubcommands as P;
    let vhost = vhost(args);
    match &args.command {
        RabbitSubcommands::Publish {
            exchange,
            routing_key,
            ..
        } => format!(
            "exchange {} on vhost {vhost}, routing key '{routing_key}'",
            exchange_name(exchange),
        ),
        RabbitSubcommands::Peek { queue, .. } => format!("queue {queue} on vhost {vhost}"),
        RabbitSubcommands::Dlq(dlq) => match &dlq.command {
            RabbitDlqSubcommands::Inspect { queue, .. }
            | RabbitDlqSubcommands::Redrive {
                queue,
                execute: false,
                ..
            } => {
                format!("queue {queue} on vhost {vhost}")
            }
            RabbitDlqSubcommands::Redrive {
                queue,
                to_exchange,
                routing_key,
                execute: true,
                ..
            } => format!(
                "queue {queue} on vhost {vhost} to {}",
                redrive_target(to_exchange.as_deref(), routing_key.as_deref())
            ),
        },
        RabbitSubcommands::Queues | RabbitSubcommands::Exchanges | RabbitSubcommands::Bindings => {
            String::new()
        }
        RabbitSubcommands::Profile(profile) => match &profile.command {
            P::Add(_) | P::List | P::Use { .. } | P::Remove { .. } | P::Show { .. } => {
                String::new()
            }
        },
    }
}

/// Where a redrive sends messages, in words.
fn redrive_target(to_exchange: Option<&str>, routing_key: Option<&str>) -> String {
    match (to_exchange, routing_key) {
        (None, None) => "each message's original exchange and routing key".to_string(),
        (Some(exchange), None) => format!(
            "exchange {} with each message's original routing key",
            exchange_name(exchange)
        ),
        (None, Some(key)) => {
            format!("each message's original exchange with routing key '{key}'")
        }
        (Some(exchange), Some(key)) => {
            format!("exchange {}, routing key '{key}'", exchange_name(exchange))
        }
    }
}

/// The record data `args` shows, for the export gate: `(label, target)`.
pub fn export(args: &RabbitArgs) -> Option<(&'static str, String)> {
    use crate::cli::rabbit_profile::RabbitProfileSubcommands as P;
    let target = |queue: &str| format!("queue {queue} on vhost {}", vhost(args));
    match &args.command {
        RabbitSubcommands::Peek { queue, .. } => Some(("rabbit peek", target(queue))),
        RabbitSubcommands::Dlq(dlq) => match &dlq.command {
            RabbitDlqSubcommands::Inspect { queue, .. } => {
                Some(("rabbit dlq inspect", target(queue)))
            }
            // Origins and destinations only, no bodies.
            RabbitDlqSubcommands::Redrive { .. } => None,
        },
        RabbitSubcommands::Queues
        | RabbitSubcommands::Exchanges
        | RabbitSubcommands::Bindings
        | RabbitSubcommands::Publish { .. } => None,
        RabbitSubcommands::Profile(profile) => match &profile.command {
            P::Add(_) | P::List | P::Use { .. } | P::Remove { .. } | P::Show { .. } => None,
        },
    }
}

/// The exchange as given, with the default exchange named `amq.default`.
fn exchange_name(exchange: &str) -> &str {
    if exchange.is_empty() {
        DEFAULT_EXCHANGE
    } else {
        exchange
    }
}

pub async fn run(args: RabbitArgs, global: &GlobalOpts) -> anyhow::Result<()> {
    let json = global.format == OutputFormat::Json;
    if let RabbitSubcommands::Profile(profile) = &args.command {
        return crate::cli::rabbit_profile::run(profile, &args.connection, json);
    }
    let timeout = global
        .timeout
        .map(Duration::from_secs)
        .unwrap_or(DEFAULT_TIMEOUT);
    let resolved = args
        .resolved
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("No RabbitMQ connection was resolved"))?;
    if resolved.url_has_credentials() {
        eprintln!("{}", URL_CREDENTIALS_WARNING.yellow());
    }
    let credentials = resolved
        .credentials(&args.connection)
        .map_err(anyhow::Error::msg)?;
    let client = resolved
        .client(credentials.as_ref(), timeout)
        .map_err(anyhow::Error::msg)?;
    let vhost = resolved.vhost.as_str();
    match &args.command {
        RabbitSubcommands::Queues => {
            let queues = client.queues(vhost).await?;
            output::emit(json, &queues, |queues| {
                let mut table = output::table(vec![
                    "NAME",
                    "TYPE",
                    "STATE",
                    "READY",
                    "UNACKED",
                    "CONSUMERS",
                    "DEAD-LETTER EXCHANGE",
                ]);
                let count = |n: Option<u64>| n.map(|n| n.to_string()).unwrap_or_default();
                for q in queues {
                    table.add_row(vec![
                        Cell::new(&q.name),
                        Cell::new(&q.kind),
                        Cell::new(q.state.as_deref().unwrap_or_default()),
                        Cell::new(count(q.messages_ready)),
                        Cell::new(count(q.messages_unacknowledged)),
                        Cell::new(count(q.consumers)),
                        Cell::new(
                            q.dead_letter_exchange()
                                .map(exchange_name)
                                .unwrap_or_default(),
                        ),
                    ]);
                }
                println!("{table}");
            })?;
        }
        RabbitSubcommands::Exchanges => {
            let exchanges = client.exchanges(vhost).await?;
            output::emit(json, &exchanges, |exchanges| {
                let mut table =
                    output::table(vec!["NAME", "TYPE", "DURABLE", "AUTO-DELETE", "INTERNAL"]);
                for e in exchanges {
                    table.add_row(vec![
                        Cell::new(e.display_name()),
                        Cell::new(&e.kind),
                        Cell::new(e.durable),
                        Cell::new(e.auto_delete),
                        Cell::new(e.internal),
                    ]);
                }
                println!("{table}");
            })?;
        }
        RabbitSubcommands::Bindings => {
            let bindings = client.bindings(vhost).await?;
            output::emit(json, &bindings, |bindings| {
                let mut table = output::table(vec!["SOURCE", "DESTINATION", "TYPE", "ROUTING KEY"]);
                for b in bindings {
                    table.add_row(vec![
                        Cell::new(exchange_name(&b.source)),
                        Cell::new(&b.destination),
                        Cell::new(&b.destination_type),
                        Cell::new(&b.routing_key),
                    ]);
                }
                println!("{table}");
            })?;
        }
        RabbitSubcommands::Peek {
            queue,
            count,
            mask_pii,
        } => {
            let policy = crate::governance::policy::current();
            let mask = *mask_pii || policy.masking_enforced();
            crate::cli::note_masking(policy);
            let messages = client.peek(vhost, queue, *count).await?;
            let shown: Vec<ShownMessage> = messages
                .iter()
                .map(|m| ShownMessage::new(m, mask))
                .collect();
            crate::cli::export_finished(shown.len() as u64);
            output::emit(json, &shown, |shown| {
                let mut table = output::table(vec![
                    "#",
                    "EXCHANGE",
                    "ROUTING KEY",
                    "REDELIVERED",
                    "PAYLOAD",
                ]);
                for (i, m) in shown.iter().enumerate() {
                    table.add_row(vec![
                        Cell::new(i + 1),
                        Cell::new(exchange_name(&m.exchange)),
                        Cell::new(&m.routing_key),
                        Cell::new(m.redelivered),
                        Cell::new(m.payload_preview()),
                    ]);
                }
                println!("{table}");
            })?;
            note_requeued(shown.len(), queue);
        }
        RabbitSubcommands::Publish {
            exchange,
            routing_key,
            message,
            headers,
            content_type,
            transient,
        } => {
            let outgoing = OutgoingMessage {
                routing_key: routing_key.clone(),
                payload: message.clone(),
                content_type: content_type.clone(),
                headers: parse_headers(headers).map_err(anyhow::Error::msg)?,
                persistent: !transient,
            };
            let outcome = client.publish(vhost, exchange, &outgoing).await?;
            if !outcome.routed {
                anyhow::bail!(
                    "The message was not routed: exchange '{}' has no binding matching routing \
                     key '{routing_key}', so the broker dropped it",
                    exchange_name(exchange)
                );
            }
            if json {
                output::print_json(&serde_json::json!({
                    "exchange": exchange_name(exchange),
                    "vhost": vhost,
                    "routing_key": routing_key,
                    "routed": true,
                }))?;
            } else {
                println!(
                    "{} Published to exchange '{}' with routing key '{}'.",
                    "✔".green().bold(),
                    exchange_name(exchange).cyan(),
                    routing_key
                );
            }
        }
        RabbitSubcommands::Dlq(dlq) => match &dlq.command {
            RabbitDlqSubcommands::Inspect {
                queue,
                count,
                mask_pii,
            } => {
                let policy = crate::governance::policy::current();
                let mask = *mask_pii || policy.masking_enforced();
                crate::cli::note_masking(policy);
                let messages = client.peek(vhost, queue, *count).await?;
                let inspection = Inspection::new(&messages, mask);
                crate::cli::export_finished(inspection.messages.len() as u64);
                output::emit(json, &inspection, print_inspection)?;
                note_requeued(inspection.messages.len(), queue);
            }
            RabbitDlqSubcommands::Redrive {
                queue,
                to_exchange,
                routing_key,
                count,
                execute: false,
                ..
            } => {
                let preview_count = u32::try_from(*count).unwrap_or(MAX_PEEK).min(MAX_PEEK);
                let messages = client.peek(vhost, queue, preview_count.max(1)).await?;
                let preview = RedrivePreview::new(
                    queue,
                    vhost,
                    &messages,
                    to_exchange.as_deref(),
                    routing_key.as_deref(),
                );
                output::emit(json, &preview, print_preview)?;
                note_requeued(messages.len(), queue);
                if !json {
                    println!(
                        "{}",
                        format!(
                            "Preview only: nothing was moved. Run again with --execute to move up \
                             to {count} message(s) over AMQP."
                        )
                        .yellow()
                    );
                    if *count > u64::from(MAX_PEEK) {
                        println!(
                            "{}",
                            format!("The preview shows the first {MAX_PEEK} message(s).")
                                .bright_black()
                        );
                    }
                }
            }
            RabbitDlqSubcommands::Redrive {
                queue,
                to_exchange,
                routing_key,
                count,
                execute: true,
                yes,
            } => {
                let target = resolved
                    .amqp_target(credentials.as_ref(), timeout)
                    .map_err(anyhow::Error::msg)?;
                let destination = redrive_target(to_exchange.as_deref(), routing_key.as_deref());
                if !*yes {
                    let address = target.address().map_err(anyhow::Error::msg)?;
                    crate::cli::offset::confirm(
                        &format!(
                            "Move up to {count} message(s) from queue '{queue}' on vhost \
                             '{vhost}' ({address}) to {destination}? [y/N] "
                        ),
                        "Redrive cancelled; nothing was moved.",
                    )?;
                }
                let request = RedriveRequest {
                    queue: queue.clone(),
                    limit: *count,
                    to_exchange: to_exchange.clone(),
                    routing_key: routing_key.clone(),
                };
                match crate::rabbitmq::amqp::redrive(&target, &request).await {
                    Ok(report) => {
                        crate::cli::record_audit_result(
                            true,
                            &redrive_outcome(queue, &report, None),
                        );
                        print_redrive(json, queue, &report, None)?;
                    }
                    Err(error) => {
                        crate::cli::record_audit_result(
                            false,
                            &redrive_outcome(queue, &error.report, Some(error.failure)),
                        );
                        print_redrive(json, queue, &error.report, Some(error.failure))?;
                        return Err(anyhow::Error::msg(error.to_string()));
                    }
                }
            }
        },
        RabbitSubcommands::Profile(_) => {}
    }
    Ok(())
}

/// Says on stderr that peeked messages went back marked redelivered.
fn note_requeued(count: usize, queue: &str) {
    if count > 0 {
        eprintln!(
            "{}",
            format!(
                "Read {count} message(s) from '{queue}'; they were requeued and are now marked \
                 redelivered (on a quorum queue this can count towards its delivery limit)."
            )
            .bright_black()
        );
    }
}

/// The audit result of a redrive: counts, source and destinations, never
/// message content or broker error text.
fn redrive_outcome(
    queue: &str,
    report: &crate::rabbitmq::amqp::RedriveReport,
    failure: Option<crate::rabbitmq::amqp::RedriveFailure>,
) -> String {
    let destinations = report
        .destinations
        .iter()
        .map(|(destination, n)| format!("{n} to {destination}"))
        .collect::<Vec<_>>()
        .join("; ");
    let mut outcome = format!("moved {} message(s) from queue {queue}", report.moved);
    if !destinations.is_empty() {
        outcome.push_str(&format!(" ({destinations})"));
    }
    if let Some(failure) = failure {
        outcome.push_str(&format!(", then stopped: {failure}"));
    }
    outcome
}

fn print_redrive(
    json: bool,
    queue: &str,
    report: &crate::rabbitmq::amqp::RedriveReport,
    failure: Option<crate::rabbitmq::amqp::RedriveFailure>,
) -> anyhow::Result<()> {
    if json {
        output::print_json(&serde_json::json!({
            "queue": queue,
            "moved": report.moved,
            "destinations": report.destinations,
            "stopped": failure.map(|f| f.as_str()),
            "requeued": failure.map(|f| f.requeued()),
        }))?;
        return Ok(());
    }
    if report.moved > 0 || failure.is_none() {
        println!(
            "{} Moved {} message(s) from '{}'.",
            "✔".green().bold(),
            report.moved,
            queue.cyan()
        );
        for (destination, n) in &report.destinations {
            println!("  {n} to {destination}");
        }
    }
    if let Some(failure) = failure {
        if failure.requeued() {
            eprintln!(
                "{}",
                "The message being moved when it stopped went back to the queue.".yellow()
            );
        }
    }
    Ok(())
}

/// `KEY:VALUE` headers, as `produce` takes them.
fn parse_headers(headers: &[String]) -> Result<Vec<(String, String)>, String> {
    headers
        .iter()
        .map(|h| {
            h.split_once(':')
                .map(|(k, v)| (k.trim().to_string(), v.to_string()))
                .filter(|(k, _)| !k.is_empty())
                .ok_or_else(|| format!("Invalid header '{h}': expected KEY:VALUE"))
        })
        .collect()
}

/// One peeked message as `peek` prints it: masked when `mask` is set, the
/// payload as text when it is UTF-8 (else `payload_base64`, never under
/// masking).
#[derive(Debug, Clone, PartialEq, Serialize)]
struct ShownMessage {
    exchange: String,
    routing_key: String,
    redelivered: bool,
    /// The whole payload's size in bytes.
    size: u64,
    /// Whether the broker returned only the start of the payload.
    truncated: bool,
    payload: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    payload_base64: Option<String>,
    properties: Map<String, Value>,
}

impl ShownMessage {
    fn new(message: &PeekedMessage, mask: bool) -> Self {
        let body = message.body();
        let (payload, payload_base64) = match &body {
            Ok(bytes) if mask => (Some(mask_bytes(bytes, MaskStrategy::Partial)), None),
            Ok(bytes) => match std::str::from_utf8(bytes) {
                Ok(text) => (Some(text.to_string()), None),
                Err(_) => (None, Some(crate::codec::base64::encode(bytes))),
            },
            Err(e) => (Some(format!("[{e}]")), None),
        };
        let properties = if mask {
            mask_properties(&message.properties)
        } else {
            message.properties.clone()
        };
        Self {
            exchange: message.exchange.clone(),
            routing_key: message.routing_key.clone(),
            redelivered: message.redelivered,
            size: message.size,
            truncated: message.is_truncated(),
            payload,
            payload_base64,
            properties,
        }
    }

    /// The payload as one table cell.
    fn payload_preview(&self) -> String {
        let payload = self
            .payload
            .clone()
            .or_else(|| self.payload_base64.as_ref().map(|b| format!("base64:{b}")))
            .unwrap_or_default();
        crate::codec::pii::ellipsize(&payload, TABLE_PAYLOAD_CHARS)
    }
}

/// AMQP properties with every text value masked by its name, the way
/// record headers are (`mask_header_value`); headers one by one, by
/// header name. Numbers and flags (`delivery_mode`, `priority`) stay.
fn mask_properties(properties: &Map<String, Value>) -> Map<String, Value> {
    let mask_value = |name: &str, value: &Value| -> Value {
        match value {
            Value::String(text) => Value::String(mask_header_value(
                name,
                text.as_bytes(),
                MaskStrategy::Partial,
            )),
            Value::Number(_) | Value::Bool(_) | Value::Null => value.clone(),
            // Lists and tables (`x-death`) are masked as JSON text.
            other => Value::String(mask_header_value(
                name,
                other.to_string().as_bytes(),
                MaskStrategy::Partial,
            )),
        }
    };
    properties
        .iter()
        .map(|(name, value)| {
            let shown = match (name.as_str(), value) {
                ("headers", Value::Object(headers)) => Value::Object(
                    headers
                        .iter()
                        .map(|(k, v)| (k.clone(), mask_value(k, v)))
                        .collect(),
                ),
                _ => mask_value(name, value),
            };
            (name.clone(), shown)
        })
        .collect()
}

/// `rabbit dlq inspect`: the messages with their `x-death` history, and
/// how many came from each queue for each reason.
#[derive(Debug, Clone, PartialEq, Serialize)]
struct Inspection {
    groups: Vec<DeathGroup>,
    messages: Vec<InspectedMessage>,
}

/// Messages that died in the same queue for the same reason, published to
/// the same exchange with the same routing key (from the newest `x-death`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct DeathGroup {
    queue: String,
    reason: String,
    exchange: String,
    routing_key: String,
    messages: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
struct InspectedMessage {
    /// The `x-death` entries, newest first (queue, reason, count, exchange
    /// and routing keys: names, not content).
    deaths: Vec<DeathRecord>,
    #[serde(flatten)]
    message: ShownMessage,
}

impl Inspection {
    fn new(messages: &[PeekedMessage], mask: bool) -> Self {
        let messages: Vec<InspectedMessage> = messages
            .iter()
            .map(|m| InspectedMessage {
                deaths: DeathRecord::from_headers(&m.headers()),
                message: ShownMessage::new(m, mask),
            })
            .collect();
        let mut groups: BTreeMap<(String, String, String, String), u64> = BTreeMap::new();
        for message in &messages {
            let key = match message.deaths.first() {
                Some(d) => (
                    d.queue.clone(),
                    d.reason.clone(),
                    exchange_name(&d.exchange).to_string(),
                    d.routing_key().to_string(),
                ),
                None => (
                    String::new(),
                    "no x-death".to_string(),
                    String::new(),
                    String::new(),
                ),
            };
            *groups.entry(key).or_default() += 1;
        }
        Self {
            groups: groups
                .into_iter()
                .map(
                    |((queue, reason, exchange, routing_key), messages)| DeathGroup {
                        queue,
                        reason,
                        exchange,
                        routing_key,
                        messages,
                    },
                )
                .collect(),
            messages,
        }
    }
}

fn print_inspection(inspection: &Inspection) {
    let mut groups = output::table(vec![
        "FROM QUEUE",
        "REASON",
        "EXCHANGE",
        "ROUTING KEY",
        "MESSAGES",
    ]);
    for g in &inspection.groups {
        groups.add_row(vec![
            Cell::new(&g.queue),
            Cell::new(&g.reason),
            Cell::new(&g.exchange),
            Cell::new(&g.routing_key),
            Cell::new(g.messages),
        ]);
    }
    println!("{groups}");
    let mut messages = output::table(vec!["#", "FROM QUEUE", "REASON", "DEATHS", "PAYLOAD"]);
    for (i, m) in inspection.messages.iter().enumerate() {
        let newest = m.deaths.first();
        messages.add_row(vec![
            Cell::new(i + 1),
            Cell::new(newest.map(|d| d.queue.as_str()).unwrap_or_default()),
            Cell::new(newest.map(|d| d.reason.as_str()).unwrap_or("no x-death")),
            Cell::new(newest.map(|d| d.count).unwrap_or_default()),
            Cell::new(m.message.payload_preview()),
        ]);
    }
    println!("{messages}");
}

/// What `rabbit dlq redrive` without `--execute` shows: where each read
/// message would go. No message bodies.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct RedrivePreview {
    dry_run: bool,
    queue: String,
    vhost: String,
    messages: Vec<PlannedMove>,
    /// Messages per destination.
    destinations: BTreeMap<String, u64>,
    /// Messages that could not be moved (no x-death and no --to-exchange).
    unmovable: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct PlannedMove {
    from_queue: String,
    reason: String,
    deaths: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    destination: Option<RedriveDestination>,
    #[serde(skip_serializing_if = "Option::is_none")]
    problem: Option<String>,
}

impl RedrivePreview {
    fn new(
        queue: &str,
        vhost: &str,
        messages: &[PeekedMessage],
        to_exchange: Option<&str>,
        routing_key: Option<&str>,
    ) -> Self {
        let mut destinations = BTreeMap::new();
        let mut unmovable = 0;
        let planned = messages
            .iter()
            .map(|m| {
                let deaths = DeathRecord::from_headers(&m.headers());
                let newest = deaths.first();
                let destination = RedriveDestination::resolve(newest, to_exchange, routing_key);
                match &destination {
                    Ok(d) => {
                        *destinations
                            .entry(crate::rabbitmq::amqp::destination_label(d))
                            .or_default() += 1;
                    }
                    Err(_) => unmovable += 1,
                }
                PlannedMove {
                    from_queue: newest.map(|d| d.queue.clone()).unwrap_or_default(),
                    reason: newest.map(|d| d.reason.clone()).unwrap_or_default(),
                    deaths: newest.map(|d| d.count).unwrap_or_default(),
                    problem: destination
                        .as_ref()
                        .err()
                        .map(|_| "no x-death header; pass --to-exchange".to_string()),
                    destination: destination.ok(),
                }
            })
            .collect();
        Self {
            dry_run: true,
            queue: queue.to_string(),
            vhost: vhost.to_string(),
            messages: planned,
            destinations,
            unmovable,
        }
    }
}

fn print_preview(preview: &RedrivePreview) {
    let mut table = output::table(vec![
        "#",
        "FROM QUEUE",
        "REASON",
        "DEATHS",
        "TO EXCHANGE",
        "ROUTING KEY",
    ]);
    for (i, m) in preview.messages.iter().enumerate() {
        let (exchange, key) = match &m.destination {
            Some(d) => (d.exchange_name().to_string(), d.routing_key.clone()),
            None => (m.problem.clone().unwrap_or_default(), String::new()),
        };
        table.add_row(vec![
            Cell::new(i + 1),
            Cell::new(&m.from_queue),
            Cell::new(&m.reason),
            Cell::new(m.deaths),
            Cell::new(exchange),
            Cell::new(key),
        ]);
    }
    println!("{table}");
    for (destination, n) in &preview.destinations {
        println!("  {n} to {destination}");
    }
    if preview.unmovable > 0 {
        println!(
            "  {}",
            format!(
                "{} message(s) have no x-death header; a redrive stops at the first of them \
                 unless --to-exchange is given",
                preview.unmovable
            )
            .yellow()
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser)]
    struct Wrapper {
        #[command(flatten)]
        args: RabbitArgs,
    }

    fn parse(args: &[&str]) -> RabbitArgs {
        Wrapper::try_parse_from(std::iter::once("rabbit").chain(args.iter().copied()))
            .unwrap_or_else(|e| panic!("{args:?} should parse: {e}"))
            .args
    }

    fn peeked(json: &str) -> PeekedMessage {
        serde_json::from_str(json).unwrap()
    }

    /// A connection with only what the test sets (no environment).
    fn connection() -> RabbitConnection {
        RabbitConnection {
            profile: None,
            url: None,
            vhost: None,
            user: None,
            password: None,
            password_stdin: false,
            amqp_url: None,
        }
    }

    fn store() -> RabbitProfileStore {
        let mut store = RabbitProfileStore::default();
        store
            .insert(
                "prod",
                RabbitProfile {
                    vhost: "shop".to_string(),
                    user: Some("ops".to_string()),
                    password: Some("stored-pw".to_string()),
                    read_only: true,
                    ..RabbitProfile::new("https://rabbit.prod.corp")
                },
            )
            .unwrap();
        store
            .insert("dev", RabbitProfile::new("http://localhost:15672"))
            .unwrap();
        store
    }

    #[test]
    fn writes_are_publish_peek_inspect_and_redrive() {
        let publish = parse(&[
            "publish",
            "orders",
            "-k",
            "order.created",
            "-m",
            "secret-body",
            "--vhost",
            "shop",
        ]);
        assert_eq!(write_action(&publish), Some("rabbit publish"));
        let target = write_target(&publish);
        assert_eq!(
            target,
            "exchange orders on vhost shop, routing key 'order.created'"
        );
        assert!(!target.contains("secret-body"));
        let default = parse(&["publish", "", "-k", "q", "-m", "x"]);
        assert!(write_target(&default).starts_with("exchange amq.default on vhost /"));

        let peek = parse(&["peek", "orders"]);
        assert_eq!(write_action(&peek), Some("rabbit peek"));
        assert_eq!(write_target(&peek), "queue orders on vhost /");

        let inspect = parse(&["dlq", "inspect", "orders.dlq"]);
        assert_eq!(write_action(&inspect), Some("rabbit dlq inspect"));
        let preview = parse(&["dlq", "redrive", "orders.dlq"]);
        assert_eq!(write_action(&preview), Some("rabbit dlq redrive"));
        assert_eq!(write_target(&preview), "queue orders.dlq on vhost /");
        let execute = parse(&["dlq", "redrive", "orders.dlq", "--execute"]);
        assert_eq!(write_action(&execute), Some("rabbit dlq redrive --execute"));
        assert_eq!(
            write_target(&execute),
            "queue orders.dlq on vhost / to each message's original exchange and routing key"
        );
        let overridden = parse(&[
            "dlq",
            "redrive",
            "orders.dlq",
            "--to-exchange",
            "retry",
            "-k",
            "again",
            "--execute",
            "--yes",
        ]);
        assert_eq!(
            write_target(&overridden),
            "queue orders.dlq on vhost / to exchange retry, routing key 'again'"
        );

        for args in [
            &["queues"][..],
            &["exchanges"][..],
            &["bindings"][..],
            &["profile", "list"][..],
            &["profile", "use", "prod"][..],
            &["profile", "remove", "prod"][..],
            &["profile", "show"][..],
            &["profile", "add", "prod", "--rabbit-url", "http://r"][..],
        ] {
            let parsed = parse(args);
            assert_eq!(write_action(&parsed), None, "{args:?}");
            assert_eq!(write_target(&parsed), "", "{args:?}");
        }
    }

    #[test]
    fn yes_needs_execute() {
        let refused = Wrapper::try_parse_from(["rabbit", "dlq", "redrive", "q", "--yes"]);
        assert!(refused.is_err());
    }

    #[test]
    fn peek_and_inspect_export_record_data_and_redrive_does_not() {
        let peek = parse(&["peek", "orders.dlq", "-n", "3"]);
        assert_eq!(
            export(&peek),
            Some(("rabbit peek", "queue orders.dlq on vhost /".to_string()))
        );
        let inspect = parse(&["dlq", "inspect", "orders.dlq", "--vhost", "shop"]);
        assert_eq!(
            export(&inspect),
            Some((
                "rabbit dlq inspect",
                "queue orders.dlq on vhost shop".to_string()
            ))
        );
        for args in [
            &["queues"][..],
            &["exchanges"][..],
            &["bindings"][..],
            &["publish", "x", "-m", "y"][..],
            &["dlq", "redrive", "q"][..],
            &["dlq", "redrive", "q", "--execute"][..],
            &["profile", "list"][..],
        ] {
            assert_eq!(export(&parse(args)), None, "{args:?}");
        }
    }

    #[test]
    fn the_active_profile_is_used_when_nothing_is_named() {
        let resolved = resolve_with(&connection(), || {
            let mut store = store();
            store.set_active("prod").unwrap();
            Ok(store)
        })
        .unwrap();
        assert_eq!(resolved.profile.as_deref(), Some("prod"));
        assert!(resolved.read_only);
        assert_eq!(resolved.vhost, "shop");
        assert_eq!(resolved.user.as_deref(), Some("ops"));
        assert_eq!(resolved.address().unwrap(), "rabbit.prod.corp:443");
        assert_eq!(
            resolved.credentials(&connection()).unwrap(),
            Some(("ops".to_string(), "stored-pw".to_string()))
        );
    }

    #[test]
    fn options_override_the_named_profile() {
        let conn = RabbitConnection {
            profile: Some("prod".to_string()),
            vhost: Some("billing".to_string()),
            amqp_url: Some("amqps://amqp.prod.corp".to_string()),
            ..connection()
        };
        let resolved = resolve_with(&conn, || Ok(store())).unwrap();
        assert_eq!(resolved.profile.as_deref(), Some("prod"));
        assert_eq!(resolved.vhost, "billing");
        assert_eq!(resolved.amqp_url.as_deref(), Some("amqps://amqp.prod.corp"));

        // Another user does not get the profile's password.
        let other_user = RabbitConnection {
            user: Some("someone".to_string()),
            ..conn.clone()
        };
        let resolved = resolve_with(&other_user, || Ok(store())).unwrap();
        assert!(resolved.credentials(&connection()).is_err());

        let unknown = RabbitConnection {
            profile: Some("nope".to_string()),
            ..connection()
        };
        assert!(resolve_with(&unknown, || Ok(store())).is_err());
    }

    #[test]
    fn a_url_without_a_profile_option_uses_no_profile() {
        let conn = RabbitConnection {
            url: Some("http://admin:pw@rabbit.dev".to_string()),
            ..connection()
        };
        let resolved = resolve_with(&conn, || panic!("the store must not be read")).unwrap();
        assert_eq!(resolved.profile, None);
        assert!(!resolved.read_only);
        assert_eq!(resolved.vhost, "/");
        assert_eq!(resolved.address().unwrap(), "rabbit.dev:15672");
        assert_eq!(resolved.credentials(&connection()).unwrap(), None);
        // The URL's user and password log in over AMQP too, and a warning
        // (which names neither) is due.
        assert!(resolved.url_has_credentials());
        assert!(!URL_CREDENTIALS_WARNING.contains("admin"));
        let target = resolved.amqp_target(None, DEFAULT_TIMEOUT).unwrap();
        assert_eq!(target.url, "amqp://rabbit.dev:5672");
        assert_eq!(target.user.as_deref(), Some("admin"));
        assert_eq!(target.password.as_deref(), Some("pw"));
        // Credentials given explicitly win.
        let given = Some(("ops".to_string(), "x".to_string()));
        let target = resolved
            .amqp_target(given.as_ref(), DEFAULT_TIMEOUT)
            .unwrap();
        assert_eq!(target.user.as_deref(), Some("ops"));
    }

    #[test]
    fn the_gate_matches_the_management_and_the_amqp_address() {
        let mut resolved = resolve_with(
            &RabbitConnection {
                url: Some("http://rabbit.dev".to_string()),
                ..connection()
            },
            || unreachable!(),
        )
        .unwrap();
        assert_eq!(
            resolved.gate_addresses().unwrap(),
            "rabbit.dev:15672,rabbit.dev:5672"
        );
        resolved.amqp_url = Some("amqps://amqp.dev".to_string());
        assert_eq!(
            resolved.gate_addresses().unwrap(),
            "rabbit.dev:15672,amqp.dev:5671"
        );
        assert!(!resolved.url_has_credentials());
    }

    #[test]
    fn no_url_and_no_active_profile_is_an_error() {
        let err = resolve_with(&connection(), || Ok(RabbitProfileStore::default())).unwrap_err();
        assert!(err.contains("--rabbit-url"), "{err}");
        assert!(err.contains("rabbit profile add"), "{err}");
    }

    #[test]
    fn a_read_only_profile_refuses_writes() {
        let mut args = parse(&["publish", "x", "-m", "y"]);
        args.resolved = Some(
            resolve_with(
                &RabbitConnection {
                    profile: Some("prod".to_string()),
                    ..connection()
                },
                || Ok(store()),
            )
            .unwrap(),
        );
        assert_eq!(gate_profile(&args), "prod");
        assert_eq!(
            gate_address(&args).unwrap(),
            "rabbit.prod.corp:443,rabbit.prod.corp:5671"
        );
        let err = ensure_writable(&args, "rabbit publish").unwrap_err();
        assert!(err.contains("read-only"), "{err}");
        args.resolved = Some(
            resolve_with(
                &RabbitConnection {
                    profile: Some("dev".to_string()),
                    ..connection()
                },
                || Ok(store()),
            )
            .unwrap(),
        );
        assert!(ensure_writable(&args, "rabbit publish").is_ok());
        args.resolved = None;
        assert!(gate_address(&args).is_err());
    }

    #[test]
    fn a_user_needs_a_password_and_a_password_a_user() {
        let mut resolved = resolve_with(
            &RabbitConnection {
                url: Some("http://rabbit".to_string()),
                ..connection()
            },
            || unreachable!(),
        )
        .unwrap();
        resolved.user = Some("ops".to_string());
        let err = resolved.credentials(&connection()).unwrap_err();
        assert!(err.contains(PASSWORD_ENV), "{err}");
        resolved.user = None;
        let with_password = RabbitConnection {
            password: Some("pw".to_string()),
            ..connection()
        };
        assert!(resolved.credentials(&with_password).is_err());
        resolved.user = Some("ops".to_string());
        assert_eq!(
            resolved.credentials(&with_password).unwrap(),
            Some(("ops".to_string(), "pw".to_string()))
        );
    }

    #[test]
    fn the_amqp_target_defaults_to_the_management_host() {
        let resolved = resolve_with(
            &RabbitConnection {
                profile: Some("prod".to_string()),
                ..connection()
            },
            || Ok(store()),
        )
        .unwrap();
        let credentials = Some(("ops".to_string(), "pw".to_string()));
        let target = resolved
            .amqp_target(credentials.as_ref(), DEFAULT_TIMEOUT)
            .unwrap();
        assert_eq!(target.url, "amqps://rabbit.prod.corp:5671");
        assert_eq!(target.vhost, "shop");
        assert_eq!(target.user.as_deref(), Some("ops"));
    }

    #[test]
    fn debug_output_hides_passwords() {
        let conn = RabbitConnection {
            url: Some("http://admin:url-pw@rabbit".to_string()),
            password: Some("flag-pw".to_string()),
            ..connection()
        };
        let debug = format!("{conn:?}");
        assert!(!debug.contains("url-pw"), "{debug}");
        assert!(!debug.contains("flag-pw"), "{debug}");
        let resolved = resolve_with(
            &RabbitConnection {
                profile: Some("prod".to_string()),
                ..connection()
            },
            || Ok(store()),
        )
        .unwrap();
        let debug = format!("{resolved:?}");
        assert!(!debug.contains("stored-pw"), "{debug}");
    }

    #[test]
    fn headers_are_key_colon_value() {
        assert_eq!(
            parse_headers(&["a:1".to_string(), "trace: x:y".to_string()]).unwrap(),
            vec![
                ("a".to_string(), "1".to_string()),
                ("trace".to_string(), " x:y".to_string())
            ]
        );
        assert!(parse_headers(&["novalue".to_string()]).is_err());
        assert!(parse_headers(&[":v".to_string()]).is_err());
    }

    #[test]
    fn peeked_messages_are_masked_only_when_asked() {
        let message = peeked(
            r#"{"exchange":"users","routing_key":"u.1","redelivered":false,"payload_bytes":61,
                "payload":"{\"email\":\"kim@example.com\",\"password\":\"hunter2hunter2\"}",
                "payload_encoding":"string",
                "properties":{"delivery_mode":2,"user_id":"kim@example.com",
                  "headers":{"authorization":"Bearer abc.def.ghi","x-retries":3,
                    "x-death":[{"queue":"users","reason":"rejected","count":1}]}}}"#,
        );
        let plain = ShownMessage::new(&message, false);
        assert!(plain
            .payload
            .as_deref()
            .unwrap()
            .contains("kim@example.com"));
        assert_eq!(plain.properties, message.properties);

        let masked = ShownMessage::new(&message, true);
        let text = serde_json::to_string(&masked).unwrap();
        assert!(!text.contains("kim@example.com"), "{text}");
        assert!(!text.contains("hunter2hunter2"), "{text}");
        assert!(!text.contains("abc.def.ghi"), "{text}");
        assert_eq!(masked.properties["delivery_mode"], 2);
        assert_eq!(masked.properties["headers"]["x-retries"], 3);
        // Tables such as x-death stay readable, as masked JSON text.
        let death = masked.properties["headers"]["x-death"].as_str().unwrap();
        assert!(death.contains("rejected"), "{death}");
    }

    #[test]
    fn binary_payloads_are_base64_unless_masked() {
        let message = peeked(
            r#"{"payload_bytes":3,"payload":"AP8B","payload_encoding":"base64","properties":[]}"#,
        );
        let plain = ShownMessage::new(&message, false);
        assert_eq!(plain.payload, None);
        assert_eq!(plain.payload_base64.as_deref(), Some("AP8B"));
        let masked = ShownMessage::new(&message, true);
        assert_eq!(masked.payload_base64, None);
        assert!(masked.payload.as_deref().unwrap().contains("hidden"));
    }

    fn dead_lettered(exchange: &str, key: &str, body: &str) -> PeekedMessage {
        peeked(&format!(
            r#"{{"payload":{body:?},"payload_encoding":"string","properties":{{"headers":{{
                "x-death":[{{"count":2,"reason":"rejected","queue":"orders",
                  "exchange":"{exchange}","routing-keys":["{key}"]}}]}}}}}}"#
        ))
    }

    #[test]
    fn inspect_groups_by_origin_and_masks_bodies() {
        let messages = vec![
            dead_lettered("shop", "order.created", r#"{"email":"kim@example.com"}"#),
            dead_lettered("shop", "order.created", "x"),
            dead_lettered("", "orders", "y"),
            peeked(r#"{"payload":"z","payload_encoding":"string","properties":{}}"#),
        ];
        let inspection = Inspection::new(&messages, true);
        assert_eq!(inspection.messages.len(), 4);
        assert_eq!(inspection.groups.len(), 3);
        let shop = inspection
            .groups
            .iter()
            .find(|g| g.exchange == "shop")
            .unwrap();
        assert_eq!(
            (shop.queue.as_str(), shop.reason.as_str(), shop.messages),
            ("orders", "rejected", 2)
        );
        assert!(inspection
            .groups
            .iter()
            .any(|g| g.exchange == DEFAULT_EXCHANGE));
        assert!(inspection.groups.iter().any(|g| g.reason == "no x-death"));
        let text = serde_json::to_string(&inspection).unwrap();
        assert!(!text.contains("kim@example.com"), "{text}");
        assert_eq!(inspection.messages[0].deaths[0].count, 2);
    }

    #[test]
    fn the_redrive_preview_shows_destinations_not_bodies() {
        let messages = vec![
            dead_lettered("shop", "order.created", "secret-body"),
            dead_lettered("shop", "order.created", "x"),
            peeked(r#"{"payload":"z","payload_encoding":"string","properties":{}}"#),
        ];
        let preview = RedrivePreview::new("orders.dlq", "/", &messages, None, None);
        assert!(preview.dry_run);
        assert_eq!(
            preview.destinations["exchange 'shop', routing key 'order.created'"],
            2
        );
        assert_eq!(preview.unmovable, 1);
        assert!(preview.messages[2].problem.is_some());
        let text = serde_json::to_string(&preview).unwrap();
        assert!(!text.contains("secret-body"), "{text}");

        let overridden =
            RedrivePreview::new("orders.dlq", "/", &messages, Some("retry"), Some("again"));
        assert_eq!(overridden.unmovable, 0);
        assert_eq!(
            overridden.destinations["exchange 'retry', routing key 'again'"],
            3
        );
    }

    #[test]
    fn the_redrive_audit_outcome_has_counts_and_destinations_only() {
        use crate::rabbitmq::amqp::{RedriveFailure, RedriveReport};
        let report = RedriveReport {
            moved: 3,
            destinations: BTreeMap::from([(
                "exchange 'shop', routing key 'order.created'".to_string(),
                3,
            )]),
        };
        assert_eq!(
            redrive_outcome("orders.dlq", &report, None),
            "moved 3 message(s) from queue orders.dlq (3 to exchange 'shop', routing key \
             'order.created')"
        );
        let stopped = redrive_outcome(
            "orders.dlq",
            &RedriveReport::default(),
            Some(RedriveFailure::Unroutable),
        );
        assert!(stopped.starts_with("moved 0 message(s) from queue orders.dlq, then stopped"));
    }
}
