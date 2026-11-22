//! `ferrika rabbit` (preview): RabbitMQ queues, exchanges and bindings,
//! peeking at messages and publishing, through the management HTTP API.
//!
//! The policy and audit gates in `cli/app.rs` cover these commands like the
//! Kafka ones: `publish` is the write `rabbit publish` (policy category
//! `produce`), `peek` an export of record data to stdout. They match the
//! management API's `host:port`, never a Kafka profile.

use std::time::Duration;

use clap::{Args, Subcommand};
use colored::Colorize;
use comfy_table::Cell;
use serde::Serialize;
use serde_json::{Map, Value};

use crate::cli::{output, GlobalOpts, OutputFormat};
use crate::codec::pii::{mask_bytes, mask_header_value, MaskStrategy};
use crate::rabbitmq::client::ManagementClient;
use crate::rabbitmq::model::{OutgoingMessage, PeekedMessage, DEFAULT_EXCHANGE};

/// Environment variable with the management API password.
pub const PASSWORD_ENV: &str = "FERRIKA_RABBIT_PASSWORD";

/// Request timeout when `--timeout` is not given. The Kafka profile's
/// timeout does not apply: these commands do not use a Kafka profile.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// Longest payload shown in the `peek` table.
const TABLE_PAYLOAD_CHARS: usize = 120;

#[derive(Args, Debug, Clone)]
pub struct RabbitArgs {
    #[command(subcommand)]
    pub command: RabbitSubcommands,
    #[command(flatten)]
    pub connection: RabbitConnection,
}

/// Where the management API is and how to log in.
#[derive(Args, Clone)]
pub struct RabbitConnection {
    /// RabbitMQ management API URL, e.g. http://rabbit:15672 (port 15672
    /// when left out)
    #[arg(
        long = "rabbit-url",
        env = "FERRIKA_RABBIT_URL",
        global = true,
        value_name = "URL"
    )]
    pub url: Option<String>,
    /// Virtual host
    #[arg(long, env = "FERRIKA_RABBIT_VHOST", global = true, default_value = "/")]
    pub vhost: String,
    /// Management API user (else the user in the URL)
    #[arg(long = "rabbit-user", env = "FERRIKA_RABBIT_USER", global = true)]
    pub user: Option<String>,
    /// Management API password; prefer the environment variable or
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
}

// Manual so the password never reaches debug output.
impl std::fmt::Debug for RabbitConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RabbitConnection")
            .field(
                "url",
                &self.url.as_deref().map(crate::model::format::redact_url),
            )
            .field("vhost", &self.vhost)
            .field("user", &self.user)
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .field("password_stdin", &self.password_stdin)
            .finish()
    }
}

impl RabbitConnection {
    fn url(&self) -> Result<&str, String> {
        self.url
            .as_deref()
            .map(str::trim)
            .filter(|u| !u.is_empty())
            .ok_or_else(|| {
                "No RabbitMQ management URL: pass --rabbit-url http://host:15672 or set \
                 FERRIKA_RABBIT_URL"
                    .to_string()
            })
    }

    /// `host:port` of the management API (no credentials), which the
    /// policy's broker-address rules and the audit log see. Reads no
    /// password.
    pub fn address(&self) -> Result<String, String> {
        ManagementClient::new(self.url()?)
            .map(|client| client.address())
            .map_err(|e| e.to_string())
    }

    /// The client, logged in as `--rabbit-user` when given.
    fn client(&self, timeout: Duration) -> Result<ManagementClient, String> {
        let client = ManagementClient::new(self.url()?)
            .map_err(|e| e.to_string())?
            .with_timeout(timeout);
        let password = if self.password_stdin {
            Some(crate::cli::read_password_stdin()?)
        } else {
            self.password.clone()
        };
        match (&self.user, password) {
            (Some(user), Some(password)) => Ok(client.with_credentials(user, &password)),
            (Some(_), None) => Err(format!(
                "--rabbit-user needs a password: set {PASSWORD_ENV} or pass \
                 --rabbit-password-stdin"
            )),
            (None, Some(_)) => {
                Err("A RabbitMQ password was given without --rabbit-user".to_string())
            }
            // Credentials in the URL, or none.
            (None, None) => Ok(client),
        }
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
    /// Show messages at the head of a queue without consuming them (they are
    /// requeued and marked redelivered)
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
}

/// The write the gate checks for `args`, as the audit log names it, or
/// `None` for reads. No wildcard arm: a new subcommand must be classified.
pub fn write_action(args: &RabbitArgs) -> Option<&'static str> {
    match &args.command {
        RabbitSubcommands::Publish { .. } => Some("rabbit publish"),
        RabbitSubcommands::Queues
        | RabbitSubcommands::Exchanges
        | RabbitSubcommands::Bindings
        | RabbitSubcommands::Peek { .. } => None,
    }
}

/// What a publish applies to, for the audit log: exchange, vhost and
/// routing key, never the message.
pub fn write_target(args: &RabbitArgs) -> String {
    match &args.command {
        RabbitSubcommands::Publish {
            exchange,
            routing_key,
            ..
        } => format!(
            "exchange {} on vhost {}, routing key '{routing_key}'",
            exchange_name(exchange),
            args.connection.vhost
        ),
        RabbitSubcommands::Queues
        | RabbitSubcommands::Exchanges
        | RabbitSubcommands::Bindings
        | RabbitSubcommands::Peek { .. } => String::new(),
    }
}

/// The record data `args` shows, for the export gate: `(label, target)`.
pub fn export(args: &RabbitArgs) -> Option<(&'static str, String)> {
    match &args.command {
        RabbitSubcommands::Peek { queue, .. } => Some((
            "rabbit peek",
            format!("queue {queue} on vhost {}", args.connection.vhost),
        )),
        RabbitSubcommands::Queues
        | RabbitSubcommands::Exchanges
        | RabbitSubcommands::Bindings
        | RabbitSubcommands::Publish { .. } => None,
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
    let timeout = global
        .timeout
        .map(Duration::from_secs)
        .unwrap_or(DEFAULT_TIMEOUT);
    let client = args
        .connection
        .client(timeout)
        .map_err(anyhow::Error::msg)?;
    let vhost = args.connection.vhost.as_str();
    let json = global.format == OutputFormat::Json;
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
                    let payload = m
                        .payload
                        .clone()
                        .or_else(|| m.payload_base64.as_ref().map(|b| format!("base64:{b}")))
                        .unwrap_or_default();
                    table.add_row(vec![
                        Cell::new(i + 1),
                        Cell::new(exchange_name(&m.exchange)),
                        Cell::new(&m.routing_key),
                        Cell::new(m.redelivered),
                        Cell::new(crate::codec::pii::ellipsize(&payload, TABLE_PAYLOAD_CHARS)),
                    ]);
                }
                println!("{table}");
            })?;
            if !shown.is_empty() {
                eprintln!(
                    "{}",
                    format!(
                        "Peeked {} message(s) from '{queue}'; they were requeued and are now \
                         marked redelivered.",
                        shown.len()
                    )
                    .bright_black()
                );
            }
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

    #[test]
    fn only_publish_writes_and_its_target_names_no_payload() {
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
        for args in [
            &["queues"][..],
            &["exchanges"][..],
            &["bindings"][..],
            &["peek", "orders"][..],
        ] {
            let parsed = parse(args);
            assert_eq!(write_action(&parsed), None, "{args:?}");
            assert_eq!(write_target(&parsed), "", "{args:?}");
        }
    }

    #[test]
    fn only_peek_exports_record_data() {
        let peek = parse(&["peek", "orders.dlq", "-n", "3"]);
        assert_eq!(
            export(&peek),
            Some(("rabbit peek", "queue orders.dlq on vhost /".to_string()))
        );
        for args in [
            &["queues"][..],
            &["exchanges"][..],
            &["bindings"][..],
            &["publish", "x", "-m", "y"][..],
        ] {
            assert_eq!(export(&parse(args)), None, "{args:?}");
        }
    }

    #[test]
    fn the_gate_address_needs_a_url_but_no_password() {
        let parsed = parse(&["queues", "--rabbit-url", "http://admin:pw@rabbit.prod"]);
        assert_eq!(parsed.connection.address().unwrap(), "rabbit.prod:15672");
        let mut missing = parse(&["queues"]);
        missing.connection.url = None;
        assert!(missing
            .connection
            .address()
            .unwrap_err()
            .contains("--rabbit-url"));
    }

    #[test]
    fn a_user_needs_a_password_and_a_password_a_user() {
        let mut connection = parse(&["queues"]).connection;
        connection.url = Some("http://rabbit".to_string());
        connection.password = None;
        connection.user = Some("ops".to_string());
        let err = connection.client(DEFAULT_TIMEOUT).unwrap_err();
        assert!(err.contains(PASSWORD_ENV), "{err}");
        connection.user = None;
        connection.password = Some("pw".to_string());
        assert!(connection.client(DEFAULT_TIMEOUT).is_err());
        connection.user = Some("ops".to_string());
        assert!(connection.client(DEFAULT_TIMEOUT).is_ok());
    }

    #[test]
    fn connection_debug_output_hides_the_password() {
        let mut connection = parse(&["queues"]).connection;
        connection.url = Some("http://admin:url-pw@rabbit".to_string());
        connection.password = Some("flag-pw".to_string());
        let debug = format!("{connection:?}");
        assert!(!debug.contains("url-pw"), "{debug}");
        assert!(!debug.contains("flag-pw"), "{debug}");
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
}
