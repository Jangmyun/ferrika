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
//!
//! Topology writes (`queue declare|delete|purge`, `exchange
//! declare|delete`, `bind`, `unbind`; categories `queues.*`,
//! `exchanges.*`, `bindings.change`) are recorded in the operation history;
//! `rabbit history` lists them and `rabbit undo` reverses them (see
//! `cli/rabbit_topology.rs`).

use std::time::Duration;

use clap::{Args, Subcommand};
use colored::Colorize;
use comfy_table::Cell;

use crate::cli::{output, GlobalOpts, OutputFormat};
use crate::rabbitmq::amqp::RedriveRequest;
pub use crate::rabbitmq::connection::ResolvedRabbit;
use crate::rabbitmq::connection::{CredentialsError, RabbitOverrides, ResolveError};
use crate::rabbitmq::delivery_limit::{DeliveryFate, PeekRisk};
use crate::rabbitmq::listing::{ListQuery, Listing};
use crate::rabbitmq::model::{exchange_name, OutgoingMessage, MAX_PEEK};
use crate::rabbitmq::profile::RabbitProfileStore;
use crate::rabbitmq::view::{
    parse_headers, redrive_outcome, Inspection, RedrivePreview, ShownMessage,
};

/// Environment variable with the RabbitMQ password.
pub(crate) const PASSWORD_ENV: &str = "FERRIKA_RABBIT_PASSWORD";

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
    pub resolved: Option<Box<ResolvedRabbit>>,
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
    /// CA file (PEM) for https:// and amqps:// when the broker's certificate
    /// is signed by a private CA (default: the profile's, else the OS trust
    /// store). The server's certificate is always verified
    #[arg(
        long = "rabbit-ca-cert",
        env = "FERRIKA_RABBIT_CA_CERT",
        global = true,
        value_name = "FILE"
    )]
    pub ca_cert: Option<String>,
    /// Client certificate (PEM) for mutual TLS, with --rabbit-client-key
    #[arg(
        long = "rabbit-client-cert",
        env = "FERRIKA_RABBIT_CLIENT_CERT",
        global = true,
        value_name = "FILE"
    )]
    pub client_cert: Option<String>,
    /// The client certificate's key (PEM, not encrypted)
    #[arg(
        long = "rabbit-client-key",
        env = "FERRIKA_RABBIT_CLIENT_KEY",
        global = true,
        value_name = "FILE"
    )]
    pub client_key: Option<String>,
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
            .field("ca_cert", &self.ca_cert)
            .field("client_cert", &self.client_cert)
            .field("client_key", &self.client_key)
            .finish()
    }
}

impl RabbitConnection {
    /// The password given by option, environment or stdin (read once).
    fn given_password(&self) -> Result<Option<String>, String> {
        if self.password_stdin {
            crate::cli::read_password_stdin().map(Some)
        } else {
            Ok(self.password.clone())
        }
    }

    /// The options as `ResolvedRabbit::resolve` takes them.
    fn overrides(&self) -> RabbitOverrides<'_> {
        RabbitOverrides {
            profile: self.profile.as_deref(),
            url: self.url.as_deref(),
            vhost: self.vhost.as_deref(),
            user: self.user.as_deref(),
            amqp_url: self.amqp_url.as_deref(),
            ca_cert: self.ca_cert.as_deref(),
            client_cert: self.client_cert.as_deref(),
            client_key: self.client_key.as_deref(),
        }
    }
}

/// The user and password to log in with for `resolved`
/// (`ResolvedRabbit::credentials`), with the password the options give.
/// Reads stdin for `--rabbit-password-stdin`, so call it once.
fn credentials(
    resolved: &ResolvedRabbit,
    connection: &RabbitConnection,
) -> Result<Option<(String, String)>, String> {
    resolved
        .credentials(connection.given_password()?)
        .map_err(|e| match e {
            CredentialsError::MissingPassword => format!(
                "The RabbitMQ user needs a password: set {PASSWORD_ENV}, pass \
                 --rabbit-password-stdin or store one in the profile"
            ),
            CredentialsError::PasswordWithoutUser => {
                "A RabbitMQ password was given without --rabbit-user".to_string()
            }
        })
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
    args.resolved = Some(Box::new(resolved));
    Ok(())
}

/// Whether `command` talks to a broker. No wildcard arm.
fn needs_connection(command: &RabbitSubcommands) -> bool {
    match command {
        RabbitSubcommands::Profile(_) => false,
        RabbitSubcommands::Queues(_)
        | RabbitSubcommands::Exchanges(_)
        | RabbitSubcommands::Bindings(_)
        | RabbitSubcommands::Peek { .. }
        | RabbitSubcommands::Publish { .. }
        | RabbitSubcommands::Dlq(_)
        | RabbitSubcommands::Queue(_)
        | RabbitSubcommands::Exchange(_)
        | RabbitSubcommands::Bind(_)
        | RabbitSubcommands::Unbind(_)
        | RabbitSubcommands::History(_)
        | RabbitSubcommands::Undo(_) => true,
    }
}

/// The connection for `connection` (`ResolvedRabbit::resolve`): the
/// profile it names, else the active profile unless `--rabbit-url` is
/// given; each option overrides the profile's value. `load_store` runs only
/// when a profile can apply.
fn resolve_with(
    connection: &RabbitConnection,
    load_store: impl FnOnce() -> Result<RabbitProfileStore, String>,
) -> Result<ResolvedRabbit, String> {
    ResolvedRabbit::resolve(&connection.overrides(), load_store).map_err(|e| match e {
        ResolveError::NoConnection => {
            "No RabbitMQ connection: pass --rabbit-url http://host:15672 (or set \
             FERRIKA_RABBIT_URL), or add a profile with `ferrika rabbit profile add NAME \
             --rabbit-url URL`"
                .to_string()
        }
        ResolveError::Profile(reason) => reason,
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
    match args.resolved.as_deref() {
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
    /// List the queues of the vhost with message counts and dead-letter
    /// exchange (fetched in pages, up to --limit)
    Queues(RabbitListArgs),
    /// List the exchanges of the vhost (fetched in pages, up to --limit)
    Exchanges(RabbitListArgs),
    /// List the bindings of the vhost (--name matches the source or the
    /// destination)
    Bindings(RabbitListArgs),
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
        /// Peek a quorum queue even when the broker counts each peek
        /// towards its delivery limit (RabbitMQ before 4.0), which can
        /// dead-letter or drop messages; shows each message's remaining peeks
        #[arg(long)]
        allow_delivery_count: bool,
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
    /// Declare, delete or purge a queue (recorded; `rabbit undo` reverses
    /// declare and delete)
    Queue(RabbitQueueArgs),
    /// Declare or delete an exchange (recorded; `rabbit undo` reverses them)
    Exchange(RabbitExchangeArgs),
    /// Bind a queue or an exchange to an exchange (recorded; `rabbit undo`
    /// removes the binding)
    Bind(RabbitBindingArgs),
    /// Remove a binding (recorded; `rabbit undo` adds it back)
    Unbind(RabbitBindingArgs),
    /// RabbitMQ changes recorded on this broker: queue, exchange and binding
    /// declarations, deletions and purges
    History(RabbitHistoryArgs),
    /// Reverse a recorded RabbitMQ change: delete a declared queue or
    /// exchange (only while empty and unbound), declare a deleted one again
    /// with its bindings (its messages are lost), restore a binding
    Undo(RabbitUndoArgs),
    /// Named RabbitMQ connections (rabbit-profiles.json, passwords in the OS
    /// credential store)
    Profile(crate::cli::rabbit_profile::RabbitProfileArgs),
}

/// Which entries `rabbit queues | exchanges | bindings` list.
#[derive(Args, Debug, Clone, PartialEq, Eq)]
pub struct RabbitListArgs {
    /// Only names containing this text (case-insensitive), filtered by the
    /// broker; with --regex a regular expression
    #[arg(long, value_name = "PATTERN")]
    pub name: Option<String>,
    /// Treat --name as a regular expression
    #[arg(long, requires = "name")]
    pub regex: bool,
    /// The most entries to list, fetched in pages of up to 500; 0 lists
    /// all. Stderr says when more match
    #[arg(long, default_value_t = crate::rabbitmq::listing::DEFAULT_LIST_LIMIT)]
    pub limit: usize,
}

impl RabbitListArgs {
    fn query(&self) -> ListQuery {
        ListQuery {
            name: self.name.clone(),
            regex: self.regex,
            limit: self.limit,
        }
    }
}

#[derive(Args, Debug, Clone, PartialEq, Eq)]
pub struct RabbitQueueArgs {
    #[command(subcommand)]
    pub command: RabbitQueueSubcommands,
}

#[derive(Subcommand, Debug, Clone, PartialEq, Eq)]
pub enum RabbitQueueSubcommands {
    /// Declare a new queue (an existing one is refused, so undo never
    /// deletes a queue this command did not make)
    Declare {
        name: String,
        /// Queue type (default: the vhost's default queue type)
        #[arg(long = "type", value_name = "TYPE", value_parser = ["classic", "quorum", "stream"])]
        kind: Option<String>,
        /// Not durable: the queue does not survive a broker restart
        #[arg(long)]
        transient: bool,
        /// Delete the queue when its last consumer goes away
        #[arg(long)]
        auto_delete: bool,
        /// An x-argument, KEY=VALUE (repeatable), e.g. x-max-length=1000 or
        /// x-dead-letter-exchange=dlx; whole numbers and true/false are
        /// sent as numbers and booleans, JSON in quotes as JSON
        #[arg(long = "arg", value_name = "KEY=VALUE")]
        arguments: Vec<String>,
    },
    /// Delete a queue with its messages and bindings. `rabbit undo`
    /// declares it again with its bindings, but empty
    Delete {
        name: String,
        /// Refuse when the queue holds messages
        #[arg(long)]
        if_empty: bool,
        /// Refuse when the queue has consumers
        #[arg(long)]
        if_unused: bool,
        /// Do not ask for confirmation
        #[arg(short = 'y', long)]
        yes: bool,
    },
    /// Delete every ready message of a queue, keeping the queue. Cannot be
    /// undone
    Purge {
        name: String,
        /// Do not ask for confirmation
        #[arg(short = 'y', long)]
        yes: bool,
    },
}

#[derive(Args, Debug, Clone, PartialEq, Eq)]
pub struct RabbitExchangeArgs {
    #[command(subcommand)]
    pub command: RabbitExchangeSubcommands,
}

#[derive(Subcommand, Debug, Clone, PartialEq, Eq)]
pub enum RabbitExchangeSubcommands {
    /// Declare a new exchange (an existing one is refused)
    Declare {
        name: String,
        /// Exchange type: direct, fanout, topic, headers or a plugin's type
        #[arg(long = "type", value_name = "TYPE", default_value = "direct")]
        kind: String,
        /// Not durable: the exchange does not survive a broker restart
        #[arg(long)]
        transient: bool,
        /// Delete the exchange when its last binding goes away
        #[arg(long)]
        auto_delete: bool,
        /// Clients cannot publish to it; only other exchanges route to it
        #[arg(long)]
        internal: bool,
        /// An argument, KEY=VALUE (repeatable), e.g.
        /// alternate-exchange=unrouted
        #[arg(long = "arg", value_name = "KEY=VALUE")]
        arguments: Vec<String>,
    },
    /// Delete an exchange with every binding from or to it. `rabbit undo`
    /// declares it again and restores those bindings
    Delete {
        name: String,
        /// Refuse when the exchange is the source of a binding
        #[arg(long)]
        if_unused: bool,
        /// Do not ask for confirmation
        #[arg(short = 'y', long)]
        yes: bool,
    },
}

/// What a binding delivers to.
#[derive(clap::ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum BindingDestinationType {
    Queue,
    Exchange,
}

#[derive(Args, Debug, Clone, PartialEq, Eq)]
pub struct RabbitBindingArgs {
    /// The exchange messages come from
    pub source: String,
    /// The queue (or, with --destination-type exchange, the exchange)
    /// messages go to
    pub destination: String,
    /// Whether the destination is a queue or an exchange
    #[arg(long, value_enum, default_value = "queue")]
    pub destination_type: BindingDestinationType,
    /// Routing key (a pattern for topic exchanges)
    #[arg(short = 'k', long = "routing-key", default_value = "")]
    pub routing_key: String,
    /// A binding argument, KEY=VALUE (repeatable), e.g. x-match=all for a
    /// headers exchange
    #[arg(long = "arg", value_name = "KEY=VALUE")]
    pub arguments: Vec<String>,
}

#[derive(Args, Debug, Clone, PartialEq, Eq)]
pub struct RabbitHistoryArgs {
    #[command(subcommand)]
    pub command: Option<RabbitHistoryCommand>,
    /// Number of operations to show (newest first)
    #[arg(short = 'n', long, default_value_t = 30, value_parser = crate::cli::history::parse_limit)]
    pub limit: usize,
    /// Every broker and cluster, not only this RabbitMQ
    #[arg(long)]
    pub all: bool,
    /// Only operations whose resources or target contain this text, e.g.
    /// `queue:orders`
    #[arg(long, value_name = "TEXT")]
    pub resource: Option<String>,
    /// Only operations that can still be undone
    #[arg(long)]
    pub undoable: bool,
}

#[derive(Subcommand, Debug, Clone, PartialEq, Eq)]
pub enum RabbitHistoryCommand {
    /// One operation in full: before and after values, outcome, what undo
    /// would do and what it cannot restore
    Show {
        /// Operation id (at least 6 characters)
        id: String,
    },
}

#[derive(Args, Debug, Clone, PartialEq, Eq)]
pub struct RabbitUndoArgs {
    /// Operation id (at least 6 characters); the newest operation you ran on
    /// this broker that can be undone when left out
    pub id: Option<String>,
    /// Show what undo would do and whether it would be refused, changing nothing
    #[arg(long)]
    pub dry_run: bool,
    /// Undo even though the state changed since, or later operations
    /// touched the same resources (never deletes a queue holding messages)
    #[arg(long)]
    pub force: bool,
    /// Do not ask for confirmation
    #[arg(short = 'y', long)]
    pub yes: bool,
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
        /// Read a quorum queue even when each read counts towards its
        /// delivery limit (RabbitMQ before 4.0); see `rabbit peek --help`
        #[arg(long)]
        allow_delivery_count: bool,
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
        /// Preview a quorum queue even when each read counts towards its
        /// delivery limit (RabbitMQ before 4.0); see `rabbit peek --help`
        #[arg(long)]
        allow_delivery_count: bool,
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
        RabbitSubcommands::Queue(queue) => Some(match &queue.command {
            RabbitQueueSubcommands::Declare { .. } => "rabbit queue declare",
            RabbitQueueSubcommands::Delete { .. } => "rabbit queue delete",
            RabbitQueueSubcommands::Purge { .. } => "rabbit queue purge",
        }),
        RabbitSubcommands::Exchange(exchange) => Some(match &exchange.command {
            RabbitExchangeSubcommands::Declare { .. } => "rabbit exchange declare",
            RabbitExchangeSubcommands::Delete { .. } => "rabbit exchange delete",
        }),
        RabbitSubcommands::Bind(_) => Some("rabbit bind"),
        RabbitSubcommands::Unbind(_) => Some("rabbit unbind"),
        // Undo's policy categories depend on the recorded operation, so it
        // runs the gate itself, once per category of its steps.
        RabbitSubcommands::Undo(_) => None,
        RabbitSubcommands::Queues(_)
        | RabbitSubcommands::Exchanges(_)
        | RabbitSubcommands::Bindings(_)
        | RabbitSubcommands::History(_) => None,
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
        RabbitSubcommands::Queue(queue) => match &queue.command {
            RabbitQueueSubcommands::Declare { name, .. }
            | RabbitQueueSubcommands::Delete { name, .. }
            | RabbitQueueSubcommands::Purge { name, .. } => {
                format!("queue {name} on vhost {vhost}")
            }
        },
        RabbitSubcommands::Exchange(exchange) => match &exchange.command {
            RabbitExchangeSubcommands::Declare { name, .. }
            | RabbitExchangeSubcommands::Delete { name, .. } => {
                format!("exchange {name} on vhost {vhost}")
            }
        },
        RabbitSubcommands::Bind(binding) | RabbitSubcommands::Unbind(binding) => {
            format!("{} on vhost {vhost}", binding_label(binding))
        }
        RabbitSubcommands::Undo(undo) => match &undo.id {
            Some(id) => format!("op {id}"),
            None => "the latest undoable operation".to_string(),
        },
        RabbitSubcommands::Queues(_)
        | RabbitSubcommands::Exchanges(_)
        | RabbitSubcommands::Bindings(_)
        | RabbitSubcommands::History(_) => String::new(),
        RabbitSubcommands::Profile(profile) => match &profile.command {
            P::Add(_) | P::List | P::Use { .. } | P::Remove { .. } | P::Show { .. } => {
                String::new()
            }
        },
    }
}

/// "binding exchange shop -> queue orders, routing key 'order.*'", as the
/// audit log names it (no argument values).
fn binding_label(binding: &RabbitBindingArgs) -> String {
    let kind = match binding.destination_type {
        BindingDestinationType::Queue => "queue",
        BindingDestinationType::Exchange => "exchange",
    };
    format!(
        "binding exchange {} -> {kind} {}, routing key '{}'",
        exchange_name(&binding.source),
        binding.destination,
        binding.routing_key
    )
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
pub(crate) fn export(args: &RabbitArgs) -> Option<(&'static str, String)> {
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
        RabbitSubcommands::Queues(_)
        | RabbitSubcommands::Exchanges(_)
        | RabbitSubcommands::Bindings(_)
        | RabbitSubcommands::Publish { .. }
        // Topology and history: names and counts, never message bodies.
        | RabbitSubcommands::Queue(_)
        | RabbitSubcommands::Exchange(_)
        | RabbitSubcommands::Bind(_)
        | RabbitSubcommands::Unbind(_)
        | RabbitSubcommands::History(_)
        | RabbitSubcommands::Undo(_) => None,
        RabbitSubcommands::Profile(profile) => match &profile.command {
            P::Add(_) | P::List | P::Use { .. } | P::Remove { .. } | P::Show { .. } => None,
        },
    }
}

pub(crate) async fn run(args: RabbitArgs, global: &GlobalOpts) -> anyhow::Result<()> {
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
    let credentials = credentials(resolved, &args.connection).map_err(anyhow::Error::msg)?;
    let client = resolved
        .client(credentials.as_ref(), timeout)
        .map_err(anyhow::Error::msg)?;
    let vhost = resolved.vhost.as_str();
    let topology = crate::cli::rabbit_topology::Context {
        args: &args,
        global,
        client: &client,
        resolved,
        json,
    };
    match &args.command {
        RabbitSubcommands::Queue(queue) => topology.queue(&queue.command).await?,
        RabbitSubcommands::Exchange(exchange) => topology.exchange(&exchange.command).await?,
        RabbitSubcommands::Bind(binding) => topology.binding(binding, true).await?,
        RabbitSubcommands::Unbind(binding) => topology.binding(binding, false).await?,
        RabbitSubcommands::History(history) => topology.history(history)?,
        RabbitSubcommands::Undo(undo) => topology.undo(undo).await?,
        RabbitSubcommands::Queues(list) => {
            let listing = client.list_queues(vhost, &list.query()).await?;
            note_truncated(&listing, "queue", list);
            output::emit(json, &listing.items, |queues| {
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
        RabbitSubcommands::Exchanges(list) => {
            let listing = client.list_exchanges(vhost, &list.query()).await?;
            note_truncated(&listing, "exchange", list);
            output::emit(json, &listing.items, |exchanges| {
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
        RabbitSubcommands::Bindings(list) => {
            let listing = client.list_bindings(vhost, &list.query()).await?;
            note_truncated(&listing, "binding", list);
            output::emit(json, &listing.items, |bindings| {
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
            allow_delivery_count,
        } => {
            let policy = crate::governance::policy::current();
            let mask = *mask_pii || policy.masking_enforced();
            crate::cli::note_masking(policy);
            let risk = check_delivery_limit(&client, vhost, queue, *allow_delivery_count).await?;
            let messages = client.peek(vhost, queue, *count).await?;
            note_delivery_fates(&risk, &messages);
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
                        Cell::new(m.payload_preview(TABLE_PAYLOAD_CHARS)),
                    ]);
                }
                println!("{table}");
            })?;
            note_requeued(shown.len(), queue, &risk);
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
                allow_delivery_count,
            } => {
                let policy = crate::governance::policy::current();
                let mask = *mask_pii || policy.masking_enforced();
                crate::cli::note_masking(policy);
                let risk =
                    check_delivery_limit(&client, vhost, queue, *allow_delivery_count).await?;
                let messages = client.peek(vhost, queue, *count).await?;
                note_delivery_fates(&risk, &messages);
                let inspection = Inspection::new(&messages, mask);
                crate::cli::export_finished(inspection.messages.len() as u64);
                output::emit(json, &inspection, print_inspection)?;
                note_requeued(inspection.messages.len(), queue, &risk);
            }
            RabbitDlqSubcommands::Redrive {
                queue,
                to_exchange,
                routing_key,
                count,
                execute: false,
                allow_delivery_count,
                ..
            } => {
                let preview_count = u32::try_from(*count).unwrap_or(MAX_PEEK).min(MAX_PEEK);
                let risk =
                    check_delivery_limit(&client, vhost, queue, *allow_delivery_count).await?;
                let messages = client.peek(vhost, queue, preview_count.max(1)).await?;
                note_delivery_fates(&risk, &messages);
                let preview = RedrivePreview::new(
                    queue,
                    vhost,
                    &messages,
                    to_exchange.as_deref(),
                    routing_key.as_deref(),
                )
                .with_unmovable_problem("no x-death header; pass --to-exchange");
                output::emit(json, &preview, print_preview)?;
                note_requeued(messages.len(), queue, &risk);
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
                ..
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

/// Says on stderr that peeked messages went back marked redelivered, and
/// whether that counted towards a quorum queue's delivery limit.
fn note_requeued(count: usize, queue: &str, risk: &PeekRisk) {
    if count == 0 {
        return;
    }
    let quorum = if risk.peeks_count() {
        "; each read counted towards the queue's delivery limit"
    } else if risk.queue_type == "quorum" {
        " (on this quorum queue a peek does not count towards a delivery limit: it has none, or \
         the broker is RabbitMQ 4.0 or later)"
    } else {
        ""
    };
    eprintln!(
        "{}",
        format!(
            "Read {count} message(s) from '{queue}'; they were requeued and are now marked \
             redelivered{quorum}."
        )
        .bright_black()
    );
}

/// Says on stderr when more entries match than were listed.
fn note_truncated<T>(listing: &Listing<T>, what: &str, list: &RabbitListArgs) {
    if let Some(note) = truncation_note(listing, what, list) {
        eprintln!("{}", note.yellow());
    }
}

fn truncation_note<T>(listing: &Listing<T>, what: &str, list: &RabbitListArgs) -> Option<String> {
    if !listing.truncated() {
        return None;
    }
    let narrow = if list.name.is_some() {
        "narrow --name"
    } else {
        "filter with --name"
    };
    Some(format!(
        "Showing {} of {} matching {what}(s) ({} in the vhost); raise --limit (0 for all) or \
         {narrow}.",
        listing.items.len(),
        listing.matched,
        listing.total
    ))
}

/// Checks whether peeking `queue` can push messages over a quorum queue's
/// delivery limit: refuses unless `allowed`, else warns.
async fn check_delivery_limit(
    client: &crate::rabbitmq::client::ManagementClient,
    vhost: &str,
    queue: &str,
    allowed: bool,
) -> anyhow::Result<PeekRisk> {
    let risk = client.peek_risk(vhost, queue).await?;
    delivery_limit_gate(&risk, allowed)?;
    Ok(risk)
}

/// The refusal or warning for `risk` (see [`check_delivery_limit`]).
fn delivery_limit_gate(risk: &PeekRisk, allowed: bool) -> anyhow::Result<()> {
    if !allowed {
        if let Some(refusal) = risk.refusal("--allow-delivery-count") {
            anyhow::bail!(refusal);
        }
    } else if let Some(warning) = risk.warning() {
        eprintln!("{}", format!("Warning: {warning}").yellow());
    }
    Ok(())
}

/// Says on stderr, per message, how many more peeks it survives, or that
/// this peek took it over the delivery limit. Only when peeks count.
fn note_delivery_fates(risk: &PeekRisk, messages: &[crate::rabbitmq::model::PeekedMessage]) {
    for line in delivery_fate_lines(risk, messages) {
        eprintln!("{}", line.yellow());
    }
}

fn delivery_fate_lines(
    risk: &PeekRisk,
    messages: &[crate::rabbitmq::model::PeekedMessage],
) -> Vec<String> {
    let over_limit = if risk.dead_letter_exchange {
        "went over the delivery limit with this peek and was dead-lettered"
    } else {
        "went over the delivery limit with this peek and was dropped"
    };
    messages
        .iter()
        .enumerate()
        .filter_map(|(i, message)| {
            Some(match risk.fate(message)? {
                DeliveryFate::OverLimit => format!("  message #{}: {over_limit}", i + 1),
                DeliveryFate::Kept(0) => format!(
                    "  message #{}: the next peek takes it over the delivery limit",
                    i + 1
                ),
                DeliveryFate::Kept(left) => format!(
                    "  message #{}: {left} more peek(s) leave it in the queue",
                    i + 1
                ),
            })
        })
        .collect()
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
            Cell::new(m.message.payload_preview(TABLE_PAYLOAD_CHARS)),
        ]);
    }
    println!("{messages}");
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
    use crate::rabbitmq::profile::RabbitProfile;
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
            ca_cert: None,
            client_cert: None,
            client_key: None,
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
            &["history"][..],
            &["history", "show", "01JBX3K8Q2M7"][..],
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
    fn topology_writes_are_gated_by_their_own_categories() {
        use crate::model::policy::PolicyAction;
        for (args, label, category, target) in [
            (
                &["queue", "declare", "jobs", "--type", "quorum"][..],
                "rabbit queue declare",
                "queues.create",
                "queue jobs on vhost /",
            ),
            (
                &["queue", "delete", "jobs", "-y"][..],
                "rabbit queue delete",
                "queues.delete",
                "queue jobs on vhost /",
            ),
            (
                &["queue", "purge", "jobs", "--vhost", "shop"][..],
                "rabbit queue purge",
                "queues.purge",
                "queue jobs on vhost shop",
            ),
            (
                &["exchange", "declare", "shop.x", "--type", "topic"][..],
                "rabbit exchange declare",
                "exchanges.create",
                "exchange shop.x on vhost /",
            ),
            (
                &["exchange", "delete", "shop.x"][..],
                "rabbit exchange delete",
                "exchanges.delete",
                "exchange shop.x on vhost /",
            ),
            (
                &["bind", "shop.x", "jobs", "-k", "job.*"][..],
                "rabbit bind",
                "bindings.change",
                "binding exchange shop.x -> queue jobs, routing key 'job.*' on vhost /",
            ),
            (
                &[
                    "unbind",
                    "shop.x",
                    "audit.x",
                    "--destination-type",
                    "exchange",
                ][..],
                "rabbit unbind",
                "bindings.change",
                "binding exchange shop.x -> exchange audit.x, routing key '' on vhost /",
            ),
        ] {
            let parsed = parse(args);
            assert_eq!(write_action(&parsed), Some(label), "{args:?}");
            assert_eq!(PolicyAction::from_cli(label).id(), category, "{args:?}");
            assert_eq!(write_target(&parsed), target, "{args:?}");
            assert_eq!(export(&parsed), None, "{args:?}");
        }
        // Undo gates itself per step category.
        let undo = parse(&["undo", "01JBX3K8Q2M7", "--dry-run"]);
        assert_eq!(write_action(&undo), None);
        assert_eq!(write_target(&undo), "op 01JBX3K8Q2M7");
        // A queue type the broker does not have is refused by the parser.
        assert!(
            Wrapper::try_parse_from(["rabbit", "queue", "declare", "q", "--type", "lazy"]).is_err()
        );
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
            credentials(&resolved, &connection()).unwrap(),
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
        assert!(credentials(&resolved, &connection()).is_err());

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
        assert_eq!(credentials(&resolved, &connection()).unwrap(), None);
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
        args.resolved = Some(Box::new(
            resolve_with(
                &RabbitConnection {
                    profile: Some("prod".to_string()),
                    ..connection()
                },
                || Ok(store()),
            )
            .unwrap(),
        ));
        assert_eq!(gate_profile(&args), "prod");
        assert_eq!(
            gate_address(&args).unwrap(),
            "rabbit.prod.corp:443,rabbit.prod.corp:5671"
        );
        let err = ensure_writable(&args, "rabbit publish").unwrap_err();
        assert!(err.contains("read-only"), "{err}");
        args.resolved = Some(Box::new(
            resolve_with(
                &RabbitConnection {
                    profile: Some("dev".to_string()),
                    ..connection()
                },
                || Ok(store()),
            )
            .unwrap(),
        ));
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
        let err = credentials(&resolved, &connection()).unwrap_err();
        assert!(err.contains(PASSWORD_ENV), "{err}");
        resolved.user = None;
        let with_password = RabbitConnection {
            password: Some("pw".to_string()),
            ..connection()
        };
        assert!(credentials(&resolved, &with_password).is_err());
        resolved.user = Some("ops".to_string());
        assert_eq!(
            credentials(&resolved, &with_password).unwrap(),
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
    fn listings_take_a_name_filter_and_a_limit() {
        let plain = parse(&["queues"]);
        let RabbitSubcommands::Queues(list) = &plain.command else {
            panic!("{plain:?}");
        };
        assert_eq!(list.query(), ListQuery::default());

        let filtered = parse(&["exchanges", "--name", "^orders", "--regex", "--limit", "0"]);
        let RabbitSubcommands::Exchanges(list) = &filtered.command else {
            panic!("{filtered:?}");
        };
        assert_eq!(
            list.query(),
            ListQuery {
                name: Some("^orders".to_string()),
                regex: true,
                limit: 0,
            }
        );
        let bindings = parse(&["bindings", "--name", "dlq"]);
        assert!(matches!(bindings.command, RabbitSubcommands::Bindings(_)));
        // --regex without --name is refused.
        assert!(Wrapper::try_parse_from(["rabbit", "queues", "--regex"]).is_err());
    }

    #[test]
    fn a_cut_listing_says_so() {
        let list = RabbitListArgs {
            name: None,
            regex: false,
            limit: 2,
        };
        let cut = Listing {
            items: vec![1, 2],
            matched: 1200,
            total: 1300,
        };
        let note = truncation_note(&cut, "queue", &list).unwrap();
        assert_eq!(
            note,
            "Showing 2 of 1200 matching queue(s) (1300 in the vhost); raise --limit (0 for all) \
             or filter with --name."
        );
        let whole = Listing {
            items: vec![1, 2],
            matched: 2,
            total: 1300,
        };
        assert_eq!(truncation_note(&whole, "queue", &list), None);
    }

    fn quorum_risk(version: &str) -> PeekRisk {
        PeekRisk::from_queue(
            &serde_json::json!({
                "name": "orders", "vhost": "/", "type": "quorum",
                "arguments": { "x-delivery-limit": 2, "x-dead-letter-exchange": "dlx" },
            }),
            Some(version.to_string()),
        )
        .unwrap()
    }

    #[test]
    fn peeks_that_count_towards_the_delivery_limit_need_the_flag() {
        let old = quorum_risk("3.13.7");
        let refused = delivery_limit_gate(&old, false).unwrap_err().to_string();
        assert!(refused.contains("--allow-delivery-count"), "{refused}");
        assert!(refused.contains("dead-lettered"), "{refused}");
        assert!(delivery_limit_gate(&old, true).is_ok());
        assert!(delivery_limit_gate(&quorum_risk("4.4.1"), false).is_ok());

        let peeked = |count: Option<u64>| -> crate::rabbitmq::model::PeekedMessage {
            let properties = match count {
                Some(n) => serde_json::json!({ "headers": { "x-delivery-count": n } }),
                None => serde_json::json!([]),
            };
            serde_json::from_value(serde_json::json!({ "payload": "m", "properties": properties }))
                .unwrap()
        };
        let lines = delivery_fate_lines(&old, &[peeked(None), peeked(Some(2)), peeked(Some(1))]);
        assert_eq!(
            lines,
            vec![
                "  message #1: 1 more peek(s) leave it in the queue".to_string(),
                "  message #2: went over the delivery limit with this peek and was dead-lettered"
                    .to_string(),
                "  message #3: the next peek takes it over the delivery limit".to_string(),
            ]
        );
        assert!(delivery_fate_lines(&quorum_risk("4.4.1"), &[peeked(Some(5))]).is_empty());
        let parsed = parse(&["peek", "orders", "--allow-delivery-count"]);
        assert!(matches!(
            parsed.command,
            RabbitSubcommands::Peek {
                allow_delivery_count: true,
                ..
            }
        ));
    }

    #[test]
    fn tls_options_reach_the_connection() {
        let parsed = parse(&[
            "--rabbit-url",
            "https://rabbit:15671",
            "--rabbit-ca-cert",
            "ca.pem",
            "--rabbit-client-cert",
            "me.pem",
            "--rabbit-client-key",
            "me.key",
            "queues",
        ]);
        let resolved = resolve_with(&parsed.connection, || unreachable!()).unwrap();
        assert_eq!(resolved.tls.ca_file.as_deref(), Some("ca.pem"));
        assert_eq!(resolved.tls.client_cert.as_deref(), Some("me.pem"));
        assert_eq!(resolved.tls.client_key.as_deref(), Some("me.key"));
    }
}
