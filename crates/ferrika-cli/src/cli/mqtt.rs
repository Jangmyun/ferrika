//! `ferrika mqtt` (preview): bounded subscriptions, publishing, listing and
//! clearing retained messages, and MQTT connection profiles, for MQTT
//! 3.1.1 and 5 brokers.
//!
//! The policy and audit gates in `cli/app.rs` cover these commands like the
//! Kafka and RabbitMQ ones. Writes: `pub` (policy category `produce`) and
//! `retained clear` (`messages.delete`, which also needs `produce`).
//! Reads: `sub` and `retained` show message data, so they pass the export
//! gate and mask PII under the policy; they use a clean session with a
//! fresh client id and refuse shared subscriptions, so they change nothing
//! on the broker. The gates match the broker's `host:port` and the MQTT
//! profile, never a Kafka or RabbitMQ profile. See `docs/plans/mqtt.md`.

use std::path::PathBuf;
use std::time::Duration;

use clap::{Args, Subcommand};
use colored::Colorize;
use comfy_table::Cell;

use crate::cli::{output, GlobalOpts, OutputFormat};
use crate::mqtt::client::{OutgoingMessage, Session};
use crate::mqtt::collect::{
    await_publish, await_subscription, collect_retained, read_messages, Limits, RetainedStop,
    RetainedWindow, StopReason,
};
pub use crate::mqtt::connection::ResolvedMqtt;
use crate::mqtt::connection::{CredentialsError, MqttOverrides, ResolveError};
use crate::mqtt::profile::{MqttProfileStore, MqttVersion};
use crate::mqtt::view::{parse_user_properties, ShownMessage};

/// Environment variable with the MQTT password.
pub(crate) const PASSWORD_ENV: &str = "FERRIKA_MQTT_PASSWORD";

/// Time limit of each step (connect, acknowledgement) when `--timeout` is
/// not given. The Kafka profile's timeout does not apply.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

/// Longest payload shown in the `retained` table.
const TABLE_PAYLOAD_CHARS: usize = 120;

/// How long `retained clear` looks for the message it is about to clear.
const CLEAR_LOOKUP: Duration = Duration::from_secs(2);

#[derive(Args, Debug, Clone)]
pub struct MqttArgs {
    #[command(subcommand)]
    pub command: MqttSubcommands,
    #[command(flatten)]
    pub connection: MqttConnection,
    /// The connection the options and the MQTT profile name, filled in by
    /// `main` before the policy gates run (see [`resolve`]).
    #[arg(skip)]
    pub resolved: Option<ResolvedMqtt>,
}

fn parse_version(text: &str) -> Result<MqttVersion, String> {
    MqttVersion::parse(text).map_err(|e| e.to_string())
}

/// Which broker to talk to: a profile, overridden by the options.
#[derive(Args, Clone)]
pub struct MqttConnection {
    /// MQTT profile (default: the active one, unless --mqtt-url is given
    /// without --mqtt-profile)
    #[arg(
        long = "mqtt-profile",
        env = "FERRIKA_MQTT_PROFILE",
        global = true,
        value_name = "NAME"
    )]
    pub profile: Option<String>,
    /// Broker URL: mqtt://host:1883 or mqtts://host:8883 (port optional); overrides the
    /// profile's. Never with a user or password in it
    #[arg(
        long = "mqtt-url",
        env = "FERRIKA_MQTT_URL",
        global = true,
        value_name = "URL"
    )]
    pub url: Option<String>,
    /// MQTT user (else the profile's)
    #[arg(long = "mqtt-user", env = "FERRIKA_MQTT_USER", global = true)]
    pub user: Option<String>,
    /// MQTT password; prefer the environment variable or
    /// --mqtt-password-stdin, which keep it out of the process list
    #[arg(
        long = "mqtt-password",
        env = "FERRIKA_MQTT_PASSWORD",
        hide_env_values = true,
        global = true
    )]
    pub password: Option<String>,
    /// Read the password from the first line of stdin (wins over the
    /// environment variable)
    #[arg(long = "mqtt-password-stdin", global = true)]
    pub password_stdin: bool,
    /// Protocol version: 3.1.1 or 5 (default: the profile's, else 3.1.1)
    #[arg(
        long = "mqtt-version",
        env = "FERRIKA_MQTT_VERSION",
        global = true,
        value_name = "VERSION",
        value_parser = parse_version
    )]
    pub version: Option<MqttVersion>,
    /// CA certificates (PEM) to trust for mqtts:// instead of the OS trust
    /// store (default: the profile's)
    #[arg(
        long = "mqtt-ca-file",
        env = "FERRIKA_MQTT_CA_FILE",
        global = true,
        value_name = "FILE"
    )]
    pub ca_file: Option<PathBuf>,
}

// Manual so the password never reaches debug output.
impl std::fmt::Debug for MqttConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MqttConnection")
            .field("profile", &self.profile)
            .field("url", &self.url)
            .field("user", &self.user)
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .field("password_stdin", &self.password_stdin)
            .field("version", &self.version)
            .field("ca_file", &self.ca_file)
            .finish()
    }
}

impl MqttConnection {
    /// The password given by option, environment or stdin (read once).
    pub(crate) fn given_password(&self) -> Result<Option<String>, String> {
        if self.password_stdin {
            crate::cli::read_password_stdin().map(Some)
        } else {
            Ok(self.password.clone())
        }
    }

    /// The options as `ResolvedMqtt::resolve` takes them.
    fn overrides(&self) -> MqttOverrides<'_> {
        MqttOverrides {
            profile: self.profile.as_deref(),
            url: self.url.as_deref(),
            user: self.user.as_deref(),
            version: self.version,
            ca_file: self.ca_file.clone(),
        }
    }
}

#[derive(Subcommand, Debug, Clone, PartialEq, Eq)]
pub enum MqttSubcommands {
    /// Show messages published to a topic filter, until --count messages or
    /// --duration seconds (clean session: nothing stays on the broker;
    /// shared subscriptions are refused)
    Sub {
        /// Topic filter, e.g. sensors/+/temperature or devices/#
        filter: String,
        /// Stop after this many messages
        #[arg(short = 'n', long = "count", default_value_t = 10)]
        count: u64,
        /// Stop after this many seconds
        #[arg(long, default_value_t = 30, value_name = "SECONDS")]
        duration: u64,
        /// Maximum QoS to subscribe with
        #[arg(long, default_value_t = 0, value_parser = clap::value_parser!(u8).range(0..=2))]
        qos: u8,
        /// Mask PII in payloads, topics and user properties (always on when
        /// a company policy enforces masking)
        #[arg(long = "mask-pii")]
        mask_pii: bool,
    },
    /// Publish one message to a topic (policy category produce)
    Pub {
        /// Topic name (no wildcards)
        topic: String,
        /// Message body (UTF-8 text)
        #[arg(short = 'm', long)]
        message: String,
        /// QoS 0, 1 or 2 (1 and 2 wait for the broker's acknowledgement)
        #[arg(long, default_value_t = 1, value_parser = clap::value_parser!(u8).range(0..=2))]
        qos: u8,
        /// Keep the message as the topic's retained message (replacing the
        /// one there); new subscribers get it at once
        #[arg(long)]
        retain: bool,
        /// Content type (MQTT 5)
        #[arg(long)]
        content_type: Option<String>,
        /// User property in KEY:VALUE format (MQTT 5; can be given several
        /// times)
        #[arg(short = 'H', long = "property", value_name = "KEY:VALUE")]
        properties: Vec<String>,
    },
    /// List the retained messages under a topic filter, or clear one
    Retained(MqttRetainedArgs),
    /// Named MQTT connections (mqtt-profiles.json, passwords in the OS
    /// credential store)
    Profile(crate::cli::mqtt_profile::MqttProfileArgs),
}

/// `mqtt retained FILTER` lists; `mqtt retained clear TOPIC` clears.
#[derive(Args, Debug, Clone, PartialEq, Eq)]
#[command(args_conflicts_with_subcommands = true, subcommand_negates_reqs = true)]
pub struct MqttRetainedArgs {
    #[command(subcommand)]
    pub command: Option<MqttRetainedSubcommands>,
    /// Topic filter whose retained messages to list, e.g. devices/+/config
    #[arg(required = true)]
    pub filter: Option<String>,
    /// Wait at most this many seconds for the broker's retained messages
    /// (it stops earlier once they stop coming for a second)
    #[arg(long, default_value_t = 5, value_name = "SECONDS")]
    pub wait: u64,
    /// The most retained messages to list
    #[arg(short = 'n', long = "count", default_value_t = 500)]
    pub count: usize,
    /// Mask PII in payloads, topics and user properties (always on when a
    /// company policy enforces masking)
    #[arg(long = "mask-pii")]
    pub mask_pii: bool,
}

#[derive(Subcommand, Debug, Clone, PartialEq, Eq)]
pub enum MqttRetainedSubcommands {
    /// Delete a topic's retained message by publishing an empty retained
    /// message (policy category messages.delete, which also needs produce).
    /// Asks for confirmation unless --yes
    Clear {
        /// Topic name (no wildcards)
        topic: String,
        /// Do not ask for confirmation
        #[arg(short = 'y', long)]
        yes: bool,
    },
}

/// The password the user gave, with the error messages of this command.
fn credentials(
    resolved: &ResolvedMqtt,
    connection: &MqttConnection,
) -> Result<Option<(String, String)>, String> {
    resolved
        .credentials(connection.given_password()?)
        .map_err(|e| match e {
            CredentialsError::MissingPassword => format!(
                "The MQTT user needs a password: set {PASSWORD_ENV}, pass \
                 --mqtt-password-stdin or store one in the profile"
            ),
            CredentialsError::PasswordWithoutUser => {
                "An MQTT password was given without --mqtt-user".to_string()
            }
        })
}

/// Fills in `args.resolved` for the commands that talk to a broker (not
/// `mqtt profile`), loading the MQTT profiles when needed.
pub fn resolve(args: &mut MqttArgs) -> Result<(), String> {
    if !needs_connection(&args.command) {
        return Ok(());
    }
    let resolved = resolve_with(&args.connection, || {
        MqttProfileStore::load().map_err(|e| e.to_string())
    })?;
    // A bad URL or argument stops the command before the gates.
    resolved.address()?;
    check_arguments(&args.command, resolved.version)?;
    args.resolved = Some(resolved);
    Ok(())
}

/// The checks of `args` that need no broker: topics and filters, limits,
/// MQTT 5 fields only on MQTT 5. Run by [`resolve`], before the policy
/// gates, so a mistyped command leaves no audit entry.
fn check_arguments(command: &MqttSubcommands, version: MqttVersion) -> Result<(), String> {
    match command {
        MqttSubcommands::Sub {
            filter,
            count,
            duration,
            ..
        } => {
            crate::mqtt::topic::validate_read_filter(filter).map_err(|e| e.to_string())?;
            Limits {
                count: *count,
                duration: Duration::from_secs(*duration),
            }
            .validate()
            .map_err(|e| e.to_string())?;
        }
        MqttSubcommands::Pub {
            topic,
            message,
            qos,
            retain,
            content_type,
            properties,
        } => {
            outgoing(
                topic,
                message,
                *qos,
                *retain,
                content_type.as_deref(),
                properties,
                version,
            )?;
        }
        MqttSubcommands::Retained(retained) => match (&retained.command, &retained.filter) {
            (Some(MqttRetainedSubcommands::Clear { topic, .. }), _) => {
                crate::mqtt::topic::validate_topic(topic).map_err(|e| e.to_string())?;
            }
            (None, filter) => {
                crate::mqtt::topic::validate_read_filter(filter.as_deref().unwrap_or_default())
                    .map_err(|e| e.to_string())?;
                RetainedWindow::new(Duration::from_secs(retained.wait), retained.count)
                    .validate()
                    .map_err(|e| e.to_string())?;
            }
        },
        MqttSubcommands::Profile(_) => {}
    }
    Ok(())
}

/// Whether `command` talks to a broker. No wildcard arm.
fn needs_connection(command: &MqttSubcommands) -> bool {
    match command {
        MqttSubcommands::Profile(_) => false,
        MqttSubcommands::Sub { .. }
        | MqttSubcommands::Pub { .. }
        | MqttSubcommands::Retained(_) => true,
    }
}

fn resolve_with(
    connection: &MqttConnection,
    load_store: impl FnOnce() -> Result<MqttProfileStore, String>,
) -> Result<ResolvedMqtt, String> {
    ResolvedMqtt::resolve(&connection.overrides(), load_store).map_err(|e| match e {
        ResolveError::NoConnection => "No MQTT connection: pass --mqtt-url mqtt://host:1883 (or \
                                       set FERRIKA_MQTT_URL), or add a profile with `ferrika \
                                       mqtt profile add NAME --mqtt-url URL`"
            .to_string(),
        ResolveError::Profile(reason) => reason,
    })
}

/// The address the policy gates match for `args`: the broker's `host:port`.
pub fn gate_address(args: &MqttArgs) -> Result<String, String> {
    args.resolved
        .as_ref()
        .ok_or_else(|| "No MQTT connection was resolved".to_string())?
        .address()
}

/// The profile name the gates and the audit log use: the MQTT profile, or
/// empty (the entries then name the address).
pub fn gate_profile(args: &MqttArgs) -> String {
    args.resolved
        .as_ref()
        .and_then(|r| r.profile.clone())
        .unwrap_or_default()
}

/// Refuses `action` on an MQTT profile marked read-only.
pub fn ensure_writable(args: &MqttArgs, action: &str) -> Result<(), String> {
    match &args.resolved {
        Some(ResolvedMqtt {
            read_only: true,
            profile: Some(name),
            ..
        }) => Err(format!(
            "MQTT profile '{name}' is read-only, so `{action}` is refused. Re-add it without \
             --read-only to allow changes."
        )),
        _ => Ok(()),
    }
}

/// The write the gate checks for `args`, as the audit log names it, or
/// `None` for reads. No wildcard arm: a new subcommand must be classified.
pub fn write_action(args: &MqttArgs) -> Option<&'static str> {
    use crate::cli::mqtt_profile::MqttProfileSubcommands as P;
    match &args.command {
        MqttSubcommands::Pub { .. } => Some("mqtt pub"),
        MqttSubcommands::Retained(retained) => match &retained.command {
            Some(MqttRetainedSubcommands::Clear { .. }) => Some("mqtt retained clear"),
            // Listing subscribes with a clean session: nothing changes.
            None => None,
        },
        // A clean session and no shared subscription: nothing changes on
        // the broker (see docs/plans/mqtt.md).
        MqttSubcommands::Sub { .. } => None,
        // Local configuration only.
        MqttSubcommands::Profile(profile) => match &profile.command {
            P::Add(_) | P::List | P::Use { .. } | P::Remove { .. } | P::Show { .. } => None,
        },
    }
}

/// What a write applies to, for the audit log: topics and flags, never
/// message bodies.
pub fn write_target(args: &MqttArgs) -> String {
    use crate::cli::mqtt_profile::MqttProfileSubcommands as P;
    match &args.command {
        MqttSubcommands::Pub {
            topic, qos, retain, ..
        } => format!(
            "topic {topic} (QoS {qos}{})",
            if *retain { ", retained" } else { "" }
        ),
        MqttSubcommands::Retained(retained) => match &retained.command {
            Some(MqttRetainedSubcommands::Clear { topic, .. }) => {
                format!("retained message on topic {topic}")
            }
            None => String::new(),
        },
        MqttSubcommands::Sub { .. } => String::new(),
        MqttSubcommands::Profile(profile) => match &profile.command {
            P::Add(_) | P::List | P::Use { .. } | P::Remove { .. } | P::Show { .. } => {
                String::new()
            }
        },
    }
}

/// The message data `args` shows, for the export gate: `(label, target)`.
pub(crate) fn export(args: &MqttArgs) -> Option<(&'static str, String)> {
    use crate::cli::mqtt_profile::MqttProfileSubcommands as P;
    match &args.command {
        MqttSubcommands::Sub { filter, .. } => Some(("mqtt sub", format!("topic filter {filter}"))),
        MqttSubcommands::Retained(retained) => match (&retained.command, &retained.filter) {
            (None, Some(filter)) => {
                Some(("mqtt retained", format!("retained messages under {filter}")))
            }
            (None, None) => None,
            // Shows the size of what it clears, never the payload.
            (Some(MqttRetainedSubcommands::Clear { .. }), _) => None,
        },
        MqttSubcommands::Pub { .. } => None,
        MqttSubcommands::Profile(profile) => match &profile.command {
            P::Add(_) | P::List | P::Use { .. } | P::Remove { .. } | P::Show { .. } => None,
        },
    }
}

/// A publish's checks before any connection: the topic, MQTT 5 fields only
/// on MQTT 5, and no empty retained message (that clears the topic's
/// retained message, which `retained clear` does under its own policy
/// category).
fn outgoing(
    topic: &str,
    message: &str,
    qos: u8,
    retain: bool,
    content_type: Option<&str>,
    properties: &[String],
    version: MqttVersion,
) -> Result<OutgoingMessage, String> {
    if retain && message.is_empty() {
        return Err(format!(
            "An empty retained message clears the topic's retained message; use `ferrika mqtt \
             retained clear {topic}`"
        ));
    }
    let outgoing = OutgoingMessage {
        topic: topic.to_string(),
        payload: message.as_bytes().to_vec(),
        qos,
        retain,
        content_type: content_type.map(str::to_string),
        user_properties: parse_user_properties(properties)?,
    };
    outgoing.validate(version).map_err(|e| e.to_string())?;
    Ok(outgoing)
}

pub(crate) async fn run(args: MqttArgs, global: &GlobalOpts) -> anyhow::Result<()> {
    let json = global.format == OutputFormat::Json;
    if let MqttSubcommands::Profile(profile) = &args.command {
        return crate::cli::mqtt_profile::run(profile, &args.connection, json);
    }
    let timeout = global
        .timeout
        .map(Duration::from_secs)
        .unwrap_or(DEFAULT_TIMEOUT);
    let resolved = args
        .resolved
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("No MQTT connection was resolved"))?;
    let login = credentials(resolved, &args.connection).map_err(anyhow::Error::msg)?;
    let target = resolved
        .target(login, timeout)
        .map_err(anyhow::Error::msg)?;
    let mut session = Session::connect(&target).await?;
    let result = match &args.command {
        MqttSubcommands::Sub {
            filter,
            count,
            duration,
            qos,
            mask_pii,
        } => {
            let limits = Limits {
                count: *count,
                duration: Duration::from_secs(*duration),
            };
            subscribe(&mut session, filter, *qos, limits, *mask_pii, json).await
        }
        MqttSubcommands::Pub {
            topic,
            message,
            qos,
            retain,
            content_type,
            properties,
        } => {
            let outgoing = outgoing(
                topic,
                message,
                *qos,
                *retain,
                content_type.as_deref(),
                properties,
                resolved.version,
            )
            .map_err(anyhow::Error::msg)?;
            publish(&mut session, &outgoing, json).await
        }
        MqttSubcommands::Retained(retained) => match &retained.command {
            Some(MqttRetainedSubcommands::Clear { topic, yes }) => {
                clear_retained(&mut session, topic, *yes, json).await
            }
            None => {
                let filter = retained.filter.as_deref().unwrap_or_default();
                let window =
                    RetainedWindow::new(Duration::from_secs(retained.wait), retained.count);
                list_retained(&mut session, filter, window, retained.mask_pii, json).await
            }
        },
        MqttSubcommands::Profile(_) => Ok(()),
    };
    session.disconnect().await;
    result
}

/// Whether to mask: asked for, or enforced by the company policy.
fn masking(mask_pii: bool) -> bool {
    let policy = crate::governance::policy::current();
    crate::cli::note_masking(policy);
    mask_pii || policy.masking_enforced()
}

async fn subscribe(
    session: &mut Session,
    filter: &str,
    qos: u8,
    limits: Limits,
    mask_pii: bool,
    json: bool,
) -> anyhow::Result<()> {
    let mask = masking(mask_pii);
    let timeout = session.timeout();
    session.subscribe(filter, qos).await?;
    let (granted, early) = await_subscription(session, filter, timeout).await?;
    eprintln!(
        "{}",
        format!(
            "Subscribed to '{filter}' on {} (QoS {granted}); stopping after {} message(s) or {} s.",
            session.address(),
            limits.count,
            limits.duration.as_secs()
        )
        .bright_black()
    );
    let mut shown_count = 0u64;
    let result = read_messages(session, early, limits, |message| {
        shown_count += 1;
        let shown = ShownMessage::new(message, mask);
        if json {
            match serde_json::to_string(&shown) {
                Ok(line) => println!("{line}"),
                Err(e) => eprintln!("Cannot write a message as JSON: {e}"),
            }
        } else {
            println!("{}", message_line(&shown));
        }
    })
    .await;
    // Whatever was shown counts, also when the connection broke.
    crate::cli::export_finished(shown_count);
    let (received, stopped) = result?;
    eprintln!(
        "{}",
        format!(
            "Read {received} message(s); stopped {}.",
            match stopped {
                StopReason::Count => "at --count",
                StopReason::Duration => "at --duration",
            }
        )
        .bright_black()
    );
    Ok(())
}

/// One message on one line for the text output: topic, flags, payload.
fn message_line(shown: &ShownMessage) -> String {
    let mut flags = format!("qos{}", shown.qos);
    if shown.retain {
        flags.push_str(" retained");
    }
    let payload = shown
        .payload
        .clone()
        .or_else(|| shown.payload_base64.as_ref().map(|b| format!("base64:{b}")))
        .unwrap_or_default();
    format!(
        "{} {} {}",
        shown.topic.cyan(),
        format!("[{flags}]").bright_black(),
        payload
    )
}

async fn list_retained(
    session: &mut Session,
    filter: &str,
    window: RetainedWindow,
    mask_pii: bool,
    json: bool,
) -> anyhow::Result<()> {
    let mask = masking(mask_pii);
    let timeout = session.timeout();
    session.subscribe(filter, 0).await?;
    let (_, early) = await_subscription(session, filter, timeout).await?;
    let report = collect_retained(session, early, filter, window).await?;
    let shown: Vec<ShownMessage> = report
        .messages
        .iter()
        .map(|m| ShownMessage::new(m, mask))
        .collect();
    crate::cli::export_finished(shown.len() as u64);
    if json {
        output::print_json(&serde_json::json!({
            "filter": filter,
            "messages": shown,
            "stopped": report.stopped,
            "live_skipped": report.live_skipped,
        }))?;
    } else if shown.is_empty() {
        println!("No retained messages under '{filter}'.");
    } else {
        let mut table = output::table(vec!["TOPIC", "QOS", "SIZE", "PAYLOAD"]);
        for m in &shown {
            table.add_row(vec![
                Cell::new(&m.topic),
                Cell::new(m.qos),
                Cell::new(m.size),
                Cell::new(m.payload_preview(TABLE_PAYLOAD_CHARS)),
            ]);
        }
        println!("{table}");
    }
    match report.stopped {
        RetainedStop::Quiet => {}
        RetainedStop::Wait => eprintln!(
            "{}",
            format!(
                "Retained messages were still arriving after --wait {} s; there may be more.",
                window.wait.as_secs()
            )
            .yellow()
        ),
        RetainedStop::Max => eprintln!(
            "{}",
            format!(
                "Stopped at --count {}; there may be more retained messages.",
                window.max
            )
            .yellow()
        ),
    }
    Ok(())
}

async fn publish(
    session: &mut Session,
    message: &OutgoingMessage,
    json: bool,
) -> anyhow::Result<()> {
    let timeout = session.timeout();
    session.publish(message).await?;
    let outcome = await_publish(session, &message.topic, message.qos, timeout).await?;
    crate::cli::record_audit_result(
        true,
        if outcome.acknowledged {
            "acknowledged by the broker"
        } else {
            "sent (QoS 0: no acknowledgement)"
        },
    );
    if json {
        output::print_json(&serde_json::json!({
            "topic": message.topic,
            "qos": message.qos,
            "retain": message.retain,
            "bytes": message.payload.len(),
            "acknowledged": outcome.acknowledged,
            "no_subscribers": outcome.no_subscribers,
        }))?;
        return Ok(());
    }
    println!(
        "{} {} {} byte(s) to '{}' (QoS {}{}).",
        "✔".green().bold(),
        if outcome.acknowledged {
            "Published"
        } else {
            "Sent"
        },
        message.payload.len(),
        message.topic.cyan(),
        message.qos,
        if message.retain { ", retained" } else { "" }
    );
    if outcome.no_subscribers {
        eprintln!(
            "{}",
            "The broker reports no subscriber for this topic.".bright_black()
        );
    }
    Ok(())
}

async fn clear_retained(
    session: &mut Session,
    topic: &str,
    yes: bool,
    json: bool,
) -> anyhow::Result<()> {
    // Look first, so the confirmation can say what goes and a topic with
    // nothing retained is not written to at all.
    let existing = match lookup_retained(session, topic).await {
        Ok(existing) => Some(existing),
        Err(e) => {
            eprintln!(
                "{}",
                format!("Cannot check for a retained message on '{topic}': {e}").yellow()
            );
            None
        }
    };
    if existing == Some(None) {
        crate::cli::record_audit_result(true, "no retained message; nothing cleared");
        if json {
            output::print_json(&serde_json::json!({
                "topic": topic,
                "cleared": false,
                "previous_bytes": null,
            }))?;
        } else {
            println!("No retained message on '{topic}'; nothing to clear.");
        }
        return Ok(());
    }
    let size = existing.flatten();
    if !yes {
        let what = match size {
            Some(bytes) => format!("the retained message on '{topic}' ({bytes} bytes)"),
            None => format!("any retained message on '{topic}'"),
        };
        crate::cli::offset::confirm(
            &format!(
                "Clear {what} at {}? New subscribers will no longer get it. [y/N] ",
                session.address()
            ),
            "Clear cancelled; nothing was changed.",
        )?;
    }
    let message = OutgoingMessage {
        topic: topic.to_string(),
        payload: Vec::new(),
        qos: 1,
        retain: true,
        content_type: None,
        user_properties: Vec::new(),
    };
    let timeout = session.timeout();
    session.publish(&message).await?;
    await_publish(session, topic, 1, timeout).await?;
    crate::cli::record_audit_result(
        true,
        &match size {
            Some(bytes) => format!("cleared the retained message ({bytes} bytes)"),
            None => "cleared (the previous message could not be checked)".to_string(),
        },
    );
    if json {
        output::print_json(&serde_json::json!({
            "topic": topic,
            "cleared": true,
            "previous_bytes": size,
        }))?;
    } else {
        println!(
            "{} Cleared the retained message on '{}'.",
            "✔".green().bold(),
            topic.cyan()
        );
    }
    Ok(())
}

/// The size of `topic`'s retained message, `None` when there is none.
async fn lookup_retained(session: &mut Session, topic: &str) -> anyhow::Result<Option<usize>> {
    let timeout = session.timeout();
    session.subscribe(topic, 0).await?;
    let (_, early) = await_subscription(session, topic, timeout).await?;
    let report =
        collect_retained(session, early, topic, RetainedWindow::new(CLEAR_LOOKUP, 1)).await?;
    Ok(report
        .messages
        .iter()
        .find(|m| m.topic == topic)
        .map(|m| m.payload.len()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mqtt::profile::MqttProfile;
    use clap::Parser;

    #[derive(Parser)]
    struct Wrapper {
        #[command(flatten)]
        args: MqttArgs,
    }

    fn parse(args: &[&str]) -> MqttArgs {
        Wrapper::try_parse_from(std::iter::once("mqtt").chain(args.iter().copied()))
            .unwrap_or_else(|e| panic!("{args:?} should parse: {e}"))
            .args
    }

    fn connection() -> MqttConnection {
        MqttConnection {
            profile: None,
            url: None,
            user: None,
            password: None,
            password_stdin: false,
            version: None,
            ca_file: None,
        }
    }

    fn store() -> MqttProfileStore {
        let mut store = MqttProfileStore::default();
        store
            .insert(
                "prod",
                MqttProfile {
                    user: Some("ops".to_string()),
                    password: Some("stored-pw".to_string()),
                    read_only: true,
                    ..MqttProfile::new("mqtts://broker.prod.corp")
                },
            )
            .unwrap();
        store
            .insert("dev", MqttProfile::new("mqtt://localhost"))
            .unwrap();
        store
    }

    #[test]
    fn writes_are_pub_and_retained_clear() {
        let publish = parse(&["pub", "devices/1/cmd", "-m", "secret-body", "--retain"]);
        assert_eq!(write_action(&publish), Some("mqtt pub"));
        let target = write_target(&publish);
        assert_eq!(target, "topic devices/1/cmd (QoS 1, retained)");
        assert!(!target.contains("secret-body"));
        let qos0 = parse(&["pub", "t", "-m", "x", "--qos", "0"]);
        assert_eq!(write_target(&qos0), "topic t (QoS 0)");

        let clear = parse(&["retained", "clear", "devices/1/config", "--yes"]);
        assert_eq!(write_action(&clear), Some("mqtt retained clear"));
        assert_eq!(
            write_target(&clear),
            "retained message on topic devices/1/config"
        );

        for args in [
            &["sub", "devices/#"][..],
            &["retained", "devices/+/config"][..],
            &["profile", "list"][..],
            &["profile", "use", "prod"][..],
            &["profile", "remove", "prod"][..],
            &["profile", "show"][..],
            &["profile", "add", "prod", "--mqtt-url", "mqtt://b"][..],
        ] {
            let parsed = parse(args);
            assert_eq!(write_action(&parsed), None, "{args:?}");
            assert_eq!(write_target(&parsed), "", "{args:?}");
        }
    }

    #[test]
    fn sub_and_retained_listing_export_message_data_and_clear_does_not() {
        assert_eq!(
            export(&parse(&["sub", "sensors/+/temp", "-n", "5"])),
            Some(("mqtt sub", "topic filter sensors/+/temp".to_string()))
        );
        assert_eq!(
            export(&parse(&["retained", "devices/#", "--wait", "2"])),
            Some((
                "mqtt retained",
                "retained messages under devices/#".to_string()
            ))
        );
        for args in [
            &["retained", "clear", "t"][..],
            &["pub", "t", "-m", "x"][..],
            &["profile", "list"][..],
        ] {
            assert_eq!(export(&parse(args)), None, "{args:?}");
        }
    }

    #[test]
    fn arguments_are_checked() {
        // `retained` needs a filter or `clear`.
        assert!(Wrapper::try_parse_from(["mqtt", "retained"]).is_err());
        // A filter and `clear` do not mix.
        assert!(Wrapper::try_parse_from(["mqtt", "retained", "a/#", "clear", "t"]).is_err());
        assert!(Wrapper::try_parse_from(["mqtt", "pub", "t", "-m", "x", "--qos", "3"]).is_err());
        assert!(Wrapper::try_parse_from(["mqtt", "sub", "t", "--qos", "7"]).is_err());
        assert!(Wrapper::try_parse_from(["mqtt", "--mqtt-version", "4.0", "sub", "t"]).is_err());
        let v5 = parse(&["--mqtt-version", "5", "sub", "t"]);
        assert_eq!(v5.connection.version, Some(MqttVersion::V5));
        let sub = parse(&["sub", "t"]);
        assert_eq!(
            sub.command,
            MqttSubcommands::Sub {
                filter: "t".to_string(),
                count: 10,
                duration: 30,
                qos: 0,
                mask_pii: false,
            }
        );
    }

    #[test]
    fn bad_arguments_stop_before_the_gates() {
        let check = |args: &[&str]| check_arguments(&parse(args).command, MqttVersion::V311);
        assert!(check(&["sub", "devices/#"]).is_ok());
        assert!(check(&["retained", "devices/+/config"]).is_ok());
        assert!(check(&["retained", "clear", "devices/1/config"]).is_ok());
        assert!(check(&["profile", "list"]).is_ok());
        for bad in [
            &["sub", "$share/workers/jobs/#"][..],
            &["sub", "a/#/b"][..],
            &["sub", "a", "-n", "0"][..],
            &["sub", "a", "--duration", "0"][..],
            &["retained", "$queue/a"][..],
            &["retained", "a/#", "--wait", "0"][..],
            &["retained", "clear", "a/+"][..],
            &["pub", "a/+", "-m", "x"][..],
            &["pub", "a", "-m", "", "--retain"][..],
            &["pub", "a", "-m", "x", "-H", "k:v"][..],
        ] {
            assert!(check(bad).is_err(), "{bad:?}");
        }
        // With a URL, `resolve` runs the checks too.
        let mut args = parse(&["--mqtt-url", "mqtt://b", "sub", "$share/g/a"]);
        let err = resolve(&mut args).unwrap_err();
        assert!(err.contains("shared subscription"), "{err}");
    }

    #[test]
    fn an_empty_retained_publish_must_go_through_clear() {
        let err = outgoing("t", "", 1, true, None, &[], MqttVersion::V311).unwrap_err();
        assert!(err.contains("retained clear"), "{err}");
        // Empty but not retained is an ordinary message.
        assert!(outgoing("t", "", 1, false, None, &[], MqttVersion::V311).is_ok());
        // MQTT 5 fields need MQTT 5.
        let props = ["k:v".to_string()];
        assert!(outgoing("t", "x", 1, false, None, &props, MqttVersion::V311).is_err());
        let message = outgoing(
            "t",
            "x",
            2,
            true,
            Some("text/plain"),
            &props,
            MqttVersion::V5,
        )
        .unwrap();
        assert_eq!(
            message.user_properties,
            [("k".to_string(), "v".to_string())]
        );
        assert!(outgoing("a/+", "x", 1, false, None, &[], MqttVersion::V5).is_err());
    }

    #[test]
    fn the_active_profile_is_used_and_options_override_it() {
        let resolved = resolve_with(&connection(), || {
            let mut store = store();
            store.set_active("prod").unwrap();
            Ok(store)
        })
        .unwrap();
        assert_eq!(resolved.profile.as_deref(), Some("prod"));
        assert_eq!(resolved.address().unwrap(), "broker.prod.corp:8883");
        assert_eq!(
            credentials(&resolved, &connection()).unwrap(),
            Some(("ops".to_string(), "stored-pw".to_string()))
        );

        let by_url = MqttConnection {
            url: Some("mqtt://other:1884".to_string()),
            ..connection()
        };
        let resolved = resolve_with(&by_url, || panic!("no profile read")).unwrap();
        assert_eq!(resolved.profile, None);
        assert_eq!(credentials(&resolved, &connection()).unwrap(), None);

        let err = resolve_with(&connection(), || Ok(MqttProfileStore::default())).unwrap_err();
        assert!(err.contains("--mqtt-url"), "{err}");
        assert!(err.contains("mqtt profile add"), "{err}");
    }

    #[test]
    fn a_read_only_profile_refuses_writes_and_the_gate_sees_its_address() {
        let mut args = parse(&["pub", "t", "-m", "x"]);
        args.resolved = Some(
            resolve_with(
                &MqttConnection {
                    profile: Some("prod".to_string()),
                    ..connection()
                },
                || Ok(store()),
            )
            .unwrap(),
        );
        assert_eq!(gate_profile(&args), "prod");
        assert_eq!(gate_address(&args).unwrap(), "broker.prod.corp:8883");
        let err = ensure_writable(&args, "mqtt pub").unwrap_err();
        assert!(err.contains("read-only"), "{err}");
        args.resolved = Some(
            resolve_with(
                &MqttConnection {
                    profile: Some("dev".to_string()),
                    ..connection()
                },
                || Ok(store()),
            )
            .unwrap(),
        );
        assert!(ensure_writable(&args, "mqtt pub").is_ok());
        args.resolved = None;
        assert!(gate_address(&args).is_err());
    }

    #[test]
    fn profile_commands_need_no_connection() {
        let mut args = parse(&["profile", "list"]);
        resolve(&mut args).unwrap();
        assert!(args.resolved.is_none());
    }

    #[test]
    fn text_lines_show_topic_flags_and_payload() {
        let shown = ShownMessage {
            topic: "a/b".to_string(),
            qos: 1,
            retain: true,
            size: 2,
            payload: Some("hi".to_string()),
            payload_base64: None,
            content_type: None,
            user_properties: serde_json::Map::new(),
        };
        colored::control::set_override(false);
        assert_eq!(message_line(&shown), "a/b [qos1 retained] hi");
    }

    #[test]
    fn debug_output_hides_the_password() {
        let conn = MqttConnection {
            password: Some("flag-pw".to_string()),
            ..connection()
        };
        assert!(!format!("{conn:?}").contains("flag-pw"));
    }
}
