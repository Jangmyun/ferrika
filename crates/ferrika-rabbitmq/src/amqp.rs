//! Moving dead-lettered messages over AMQP 0-9-1 (feature `amqp`, on by
//! default), with [lapin](https://crates.io/crates/lapin) on tokio.
//!
//! The management API can read and publish but cannot remove one given
//! message, so a move through it would either lose messages (read with
//! `ack_requeue_false` before the publish succeeded) or duplicate them. Over
//! AMQP each message is handled on its own:
//!
//! 1. `basic.get` without auto-ack: the message is held for this client;
//! 2. publish it to its destination with `mandatory` set, on a channel in
//!    publisher-confirm mode, and wait for the broker's confirm;
//! 3. `basic.ack` it only after the confirm, so it leaves the source queue
//!    only once its copy is stored.
//!
//! On the first failure the held message is rejected with requeue (it goes
//! back to the source queue) and the move stops. A failure between steps 2
//! and 3 (the ack is lost) leaves the message in both queues; the report
//! says so.
//!
//! This module does no policy, audit or masking; the caller applies them.

use std::collections::BTreeMap;
use std::time::Duration;

use lapin::options::{
    BasicAckOptions, BasicGetOptions, BasicNackOptions, BasicPublishOptions, ConfirmSelectOptions,
};
use lapin::tcp::{OwnedIdentity, OwnedTLSConfig};
use lapin::types::{AMQPValue, FieldTable, LongString, ShortString};
use lapin::{
    BasicProperties, Channel, Confirmation, Connection, ConnectionProperties,
    DefaultConnectionBuilder,
};
use serde::Serialize;

use crate::client::TlsFiles;
use crate::error::{RabbitError, RabbitResult};
use crate::model::{DeathRecord, RedriveDestination};

/// The header a redriven message carries: the queue it was moved from.
pub const REDRIVEN_FROM_HEADER: &str = "x-ferrika-redriven-from";

/// Where and how to connect over AMQP.
#[derive(Clone)]
pub struct AmqpTarget {
    /// `amqp://host:5672` or `amqps://host:5671`, without credentials.
    pub url: String,
    pub vhost: String,
    pub user: Option<String>,
    pub password: Option<String>,
    /// Time allowed to connect and for each broker answer.
    pub timeout: Duration,
    /// For `amqps://`: a CA file trusted besides the OS trust store, and a
    /// client certificate and key (PEM) for mutual TLS.
    pub tls: TlsFiles,
}

// Manual so the password never reaches debug output.
impl std::fmt::Debug for AmqpTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AmqpTarget")
            .field("url", &self.address().unwrap_or_default())
            .field("vhost", &self.vhost)
            .field("user", &self.user)
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .field("timeout", &self.timeout)
            .field("tls", &self.tls)
            .finish()
    }
}

impl AmqpTarget {
    /// `host:port` of the AMQP listener, for messages.
    pub fn address(&self) -> RabbitResult<String> {
        crate::profile::amqp_host_port(&self.url)
    }

    /// The URI lapin connects with: the URL's host, port and scheme, the
    /// vhost and the user and password set here (lapin's default user is
    /// `guest` when none is given).
    fn uri(&self) -> RabbitResult<lapin::uri::AMQPUri> {
        // The parse error could quote the URL; say only what is wrong.
        let mut uri: lapin::uri::AMQPUri = self.url.trim().parse().map_err(|_| {
            RabbitError::config(
                "Invalid AMQP URL: expected amqp://host[:port] or amqps://host[:port]",
            )
        })?;
        uri.vhost = self.vhost.clone();
        if let Some(user) = &self.user {
            uri.authority.userinfo.username = user.clone();
            uri.authority.userinfo.password = self.password.clone().unwrap_or_default();
        }
        Ok(uri)
    }

    /// The TLS settings lapin connects with: the CA file's certificates as
    /// extra roots and the client certificate and key, read from their
    /// files. The OS trust store stays in use; the server's certificate is
    /// always verified.
    fn tls_config(&self) -> RabbitResult<OwnedTLSConfig> {
        let read = |path: &str, what: &str| {
            std::fs::read(path)
                .map_err(|e| RabbitError::config(format!("Cannot read the {what} '{path}': {e}")))
        };
        let cert_chain = match &self.tls.ca_file {
            Some(path) => Some(String::from_utf8(read(path, "CA file")?).map_err(|_| {
                RabbitError::config(format!("The CA file '{path}' is not PEM text"))
            })?),
            None => None,
        };
        let identity = match self
            .tls
            .client_pair()
            .map_err(|e| RabbitError::config(e.to_string()))?
        {
            Some((cert, key)) => Some(OwnedIdentity::PKCS8 {
                pem: read(cert, "client certificate")?,
                key: read(key, "client key")?,
            }),
            None => None,
        };
        Ok(OwnedTLSConfig {
            identity,
            cert_chain,
        })
    }
}

/// Opens the AMQP connection, with [`AmqpTarget::tls_config`] when TLS
/// files are set.
async fn connect(
    target: &AmqpTarget,
    uri: lapin::uri::AMQPUri,
    properties: ConnectionProperties,
) -> Result<Connection, String> {
    if target.tls.is_empty() {
        return within(target.timeout, Connection::connect_uri(uri, properties)).await;
    }
    let tls = target.tls_config().map_err(|e| e.to_string())?;
    let builder = DefaultConnectionBuilder::new()
        .map_err(|e| e.to_string())?
        .with_uri(uri)
        .with_properties(properties)
        .with_tls_config(tls);
    within(target.timeout, builder.connect()).await
}

/// What to move.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedriveRequest {
    /// The dead-letter queue to move messages out of.
    pub queue: String,
    /// The most messages to move.
    pub limit: u64,
    /// Publish to this exchange instead of each message's original one.
    pub to_exchange: Option<String>,
    /// Publish with this routing key instead of each message's original one.
    pub routing_key: Option<String>,
}

/// How far a move got.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct RedriveReport {
    /// Messages published, confirmed and removed from the source queue.
    pub moved: u64,
    /// Moved messages per destination, as `exchange 'X', routing key 'K'`.
    pub destinations: BTreeMap<String, u64>,
}

/// Why a move stopped early. The text is fixed (no message content, no
/// broker error text), so it can go into the audit log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RedriveFailure {
    /// No connection, channel or confirm mode.
    Connect,
    /// Reading the next message failed.
    Read,
    /// The message has no `x-death` and no `--to-exchange` was given.
    NoDestination,
    /// The destination has no binding for the routing key.
    Unroutable,
    /// The broker refused the publish (nack), or did not confirm in time.
    NotConfirmed,
    /// The publish could not be sent.
    Publish,
    /// The copy was stored, but removing the original failed: the message
    /// is in both queues now.
    Ack,
}

impl RedriveFailure {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Connect => "could not connect over AMQP",
            Self::Read => "could not read the next message",
            Self::NoDestination => "a message had no x-death header and no --to-exchange was given",
            Self::Unroutable => "a message could not be routed by the destination exchange",
            Self::NotConfirmed => "the broker did not confirm a publish",
            Self::Publish => "a publish could not be sent",
            Self::Ack => {
                "a message was republished but could not be removed from the source queue \
                 (it is now in both)"
            }
        }
    }

    /// Whether the message being moved went back to the source queue.
    pub fn requeued(self) -> bool {
        !matches!(self, Self::Connect | Self::Read | Self::Ack)
    }
}

impl std::fmt::Display for RedriveFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A move that stopped early: what was moved before, why it stopped, and
/// the broker's error for the user (never for the audit log).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedriveError {
    pub report: RedriveReport,
    pub failure: RedriveFailure,
    pub detail: String,
}

impl std::fmt::Display for RedriveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Stopped after moving {} message(s): {}",
            self.report.moved, self.failure
        )?;
        if !self.detail.is_empty() {
            write!(f, " ({})", self.detail)?;
        }
        Ok(())
    }
}

impl std::error::Error for RedriveError {}

/// Moves up to `request.limit` messages out of `request.queue` to their
/// original exchange and routing key (from the newest `x-death` entry) or
/// to the overrides; see the module documentation for the order of steps.
pub async fn redrive(
    target: &AmqpTarget,
    request: &RedriveRequest,
) -> Result<RedriveReport, RedriveError> {
    let connect_failed = |detail: String| RedriveError {
        report: RedriveReport::default(),
        failure: RedriveFailure::Connect,
        detail,
    };
    let uri = target.uri().map_err(|e| connect_failed(e.to_string()))?;
    let queue = ShortString::try_new(request.queue.clone())
        .map_err(|_| connect_failed("the queue name is longer than 255 bytes".to_string()))?;
    let address = target.address().unwrap_or_default();
    let properties = ConnectionProperties::default().with_connection_name("ferrika".into());
    let connection = connect(target, uri, properties)
        .await
        .map_err(|e| connect_failed(crate::client::with_tls_hint(format!("{address}: {e}"))))?;
    let channel = match open_confirm_channel(&connection, target.timeout).await {
        Ok(channel) => channel,
        Err(e) => {
            let _ = connection.close(200, "ferrika done".into()).await;
            return Err(connect_failed(e));
        }
    };
    let mut mover = LapinMover {
        channel: channel.clone(),
        queue,
        source: request.queue.clone(),
        timeout: target.timeout,
    };
    let result = move_messages(&mut mover, request).await;
    // Closing returns any message still held (a failed reject) to the queue.
    let _ = channel.close(200, "ferrika done".into()).await;
    let _ = connection.close(200, "ferrika done".into()).await;
    result
}

async fn open_confirm_channel(
    connection: &Connection,
    timeout: Duration,
) -> Result<Channel, String> {
    let channel = within(timeout, connection.create_channel()).await?;
    within(
        timeout,
        channel.confirm_select(ConfirmSelectOptions::default()),
    )
    .await?;
    Ok(channel)
}

/// `future` with a time limit; lapin errors as text.
async fn within<T>(
    timeout: Duration,
    future: impl std::future::Future<Output = lapin::Result<T>>,
) -> Result<T, String> {
    match tokio::time::timeout(timeout, future).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(e)) => Err(e.to_string()),
        Err(_) => Err(format!("no answer within {} s", timeout.as_secs())),
    }
}

/// A message held for this client (not yet acknowledged).
#[derive(Debug, Clone)]
pub(crate) struct HeldMessage {
    pub(crate) delivery_tag: u64,
    pub(crate) deaths: Vec<DeathRecord>,
    pub(crate) payload: Vec<u8>,
    pub(crate) properties: BasicProperties,
}

/// What the broker said about a publish.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PublishOutcome {
    /// Stored by every queue it was routed to.
    Confirmed,
    /// Returned: no queue was bound for its routing key.
    Returned,
    /// Refused, or no confirm came.
    NotConfirmed,
}

/// The broker operations a move needs; lapin in production, a fake in tests.
pub(crate) trait Mover {
    async fn get(&mut self) -> Result<Option<HeldMessage>, String>;
    async fn publish(
        &mut self,
        destination: &RedriveDestination,
        message: &HeldMessage,
    ) -> Result<PublishOutcome, String>;
    async fn ack(&mut self, delivery_tag: u64) -> Result<(), String>;
    async fn reject_requeue(&mut self, delivery_tag: u64) -> Result<(), String>;
}

/// The move loop: get, publish, wait for the confirm, ack; on the first
/// failure reject the held message with requeue and stop.
pub(crate) async fn move_messages(
    mover: &mut impl Mover,
    request: &RedriveRequest,
) -> Result<RedriveReport, RedriveError> {
    let mut report = RedriveReport::default();
    while report.moved < request.limit {
        let message = match mover.get().await {
            Ok(Some(message)) => message,
            Ok(None) => break,
            Err(detail) => return Err(stopped(report, RedriveFailure::Read, detail)),
        };
        let tag = message.delivery_tag;
        let destination = match RedriveDestination::resolve(
            message.deaths.first(),
            request.to_exchange.as_deref(),
            request.routing_key.as_deref(),
        ) {
            Ok(destination) => destination,
            Err(e) => {
                let detail = requeue_detail(mover, tag, e.to_string()).await;
                return Err(stopped(report, RedriveFailure::NoDestination, detail));
            }
        };
        let failure = match mover.publish(&destination, &message).await {
            Ok(PublishOutcome::Confirmed) => None,
            Ok(PublishOutcome::Returned) => Some((
                RedriveFailure::Unroutable,
                format!(
                    "exchange '{}', routing key '{}'",
                    destination.exchange_name(),
                    destination.routing_key
                ),
            )),
            Ok(PublishOutcome::NotConfirmed) => Some((RedriveFailure::NotConfirmed, String::new())),
            Err(detail) => Some((RedriveFailure::Publish, detail)),
        };
        if let Some((failure, detail)) = failure {
            let detail = requeue_detail(mover, tag, detail).await;
            return Err(stopped(report, failure, detail));
        }
        if let Err(detail) = mover.ack(tag).await {
            return Err(stopped(report, RedriveFailure::Ack, detail));
        }
        report.moved += 1;
        *report
            .destinations
            .entry(destination_label(&destination))
            .or_default() += 1;
    }
    Ok(report)
}

/// Rejects the held message with requeue; `detail` plus what went wrong
/// with that (the message then returns to the queue when the channel
/// closes).
async fn requeue_detail(mover: &mut impl Mover, tag: u64, detail: String) -> String {
    match mover.reject_requeue(tag).await {
        Ok(()) => detail,
        Err(e) => {
            let note = format!(
                "returning the message failed ({e}); it goes back when the connection closes"
            );
            if detail.is_empty() {
                note
            } else {
                format!("{detail}; {note}")
            }
        }
    }
}

fn stopped(report: RedriveReport, failure: RedriveFailure, detail: String) -> RedriveError {
    RedriveError {
        report,
        failure,
        detail,
    }
}

/// `exchange 'X', routing key 'K'` (moved to [`crate::model`]; re-exported
/// here).
pub use crate::model::destination_label;

/// The lapin side of [`Mover`]: one channel in confirm mode.
struct LapinMover {
    channel: Channel,
    queue: ShortString,
    source: String,
    timeout: Duration,
}

impl Mover for LapinMover {
    async fn get(&mut self) -> Result<Option<HeldMessage>, String> {
        let got = within(
            self.timeout,
            self.channel
                .basic_get(self.queue.clone(), BasicGetOptions { no_ack: false }),
        )
        .await?;
        Ok(got.map(|message| {
            let delivery = message.delivery;
            let deaths = delivery
                .properties
                .headers()
                .as_ref()
                .map(deaths_from_table)
                .unwrap_or_default();
            HeldMessage {
                delivery_tag: delivery.delivery_tag,
                deaths,
                payload: delivery.data,
                properties: delivery.properties,
            }
        }))
    }

    async fn publish(
        &mut self,
        destination: &RedriveDestination,
        message: &HeldMessage,
    ) -> Result<PublishOutcome, String> {
        let exchange = ShortString::try_new(destination.exchange.clone())
            .map_err(|_| "the exchange name is longer than 255 bytes".to_string())?;
        let routing_key = ShortString::try_new(destination.routing_key.clone())
            .map_err(|_| "the routing key is longer than 255 bytes".to_string())?;
        let properties = message.properties.clone().with_headers(redriven_headers(
            message.properties.headers().as_ref(),
            &self.source,
        ));
        let confirm = within(
            self.timeout,
            self.channel.basic_publish(
                exchange,
                routing_key,
                BasicPublishOptions {
                    mandatory: true,
                    immediate: false,
                },
                &message.payload,
                properties,
            ),
        )
        .await?;
        // A publish that was sent but not confirmed in time counts as not
        // confirmed: the original is requeued (the copy may still arrive).
        match tokio::time::timeout(self.timeout, confirm).await {
            Ok(Ok(confirmation)) => Ok(publish_outcome(confirmation)),
            Ok(Err(_)) | Err(_) => Ok(PublishOutcome::NotConfirmed),
        }
    }

    async fn ack(&mut self, delivery_tag: u64) -> Result<(), String> {
        within(
            self.timeout,
            self.channel
                .basic_ack(delivery_tag, BasicAckOptions { multiple: false }),
        )
        .await
    }

    async fn reject_requeue(&mut self, delivery_tag: u64) -> Result<(), String> {
        within(
            self.timeout,
            self.channel.basic_nack(
                delivery_tag,
                BasicNackOptions {
                    multiple: false,
                    requeue: true,
                },
            ),
        )
        .await
    }
}

fn publish_outcome(confirmation: Confirmation) -> PublishOutcome {
    match confirmation {
        // An ack that carries a returned message: confirmed, but unroutable.
        Confirmation::Ack(Some(_)) => PublishOutcome::Returned,
        Confirmation::Ack(None) => PublishOutcome::Confirmed,
        Confirmation::Nack(_) | Confirmation::NotRequested => PublishOutcome::NotConfirmed,
    }
}

/// The headers a redriven message is published with: its own (`x-death`
/// included, so the broker's history of it stays) plus
/// [`REDRIVEN_FROM_HEADER`] naming the queue it was moved from.
pub(crate) fn redriven_headers(headers: Option<&FieldTable>, source_queue: &str) -> FieldTable {
    let mut headers = headers.cloned().unwrap_or_default();
    headers.insert(
        REDRIVEN_FROM_HEADER.into(),
        AMQPValue::LongString(LongString::from(source_queue)),
    );
    headers
}

/// The `x-death` entries of AMQP headers, newest first.
pub(crate) fn deaths_from_table(headers: &FieldTable) -> Vec<DeathRecord> {
    let Some(AMQPValue::FieldArray(entries)) = headers.inner().get("x-death") else {
        return Vec::new();
    };
    entries
        .as_slice()
        .iter()
        .filter_map(|entry| match entry {
            AMQPValue::FieldTable(table) => Some(table),
            _ => None,
        })
        .map(|entry| {
            let field = |name: &str| entry.inner().get(name);
            DeathRecord {
                queue: field("queue").and_then(text).unwrap_or_default(),
                reason: field("reason").and_then(text).unwrap_or_default(),
                count: field("count").and_then(number).unwrap_or(0),
                exchange: field("exchange").and_then(text).unwrap_or_default(),
                routing_keys: match field("routing-keys") {
                    Some(AMQPValue::FieldArray(keys)) => {
                        keys.as_slice().iter().filter_map(text).collect()
                    }
                    _ => Vec::new(),
                },
            }
        })
        .collect()
}

fn text(value: &AMQPValue) -> Option<String> {
    match value {
        AMQPValue::LongString(s) => Some(String::from_utf8_lossy(s.as_bytes()).into_owned()),
        AMQPValue::ShortString(s) => Some(s.as_str().to_string()),
        _ => None,
    }
}

fn number(value: &AMQPValue) -> Option<u64> {
    match *value {
        AMQPValue::ShortShortUInt(n) => Some(n.into()),
        AMQPValue::ShortUInt(n) => Some(n.into()),
        AMQPValue::LongUInt(n) => Some(n.into()),
        AMQPValue::Timestamp(n) => Some(n),
        AMQPValue::ShortShortInt(n) => u64::try_from(n).ok(),
        AMQPValue::ShortInt(n) => u64::try_from(n).ok(),
        AMQPValue::LongInt(n) => u64::try_from(n).ok(),
        AMQPValue::LongLongInt(n) => u64::try_from(n).ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;

    fn death_table(exchange: &str, keys: &[&str], count: i64) -> FieldTable {
        let mut table = FieldTable::default();
        table.insert("queue".into(), AMQPValue::LongString("orders".into()));
        table.insert("reason".into(), AMQPValue::LongString("rejected".into()));
        table.insert("count".into(), AMQPValue::LongLongInt(count));
        table.insert("exchange".into(), AMQPValue::LongString(exchange.into()));
        table.insert(
            "routing-keys".into(),
            AMQPValue::FieldArray(
                keys.iter()
                    .map(|k| AMQPValue::LongString((*k).into()))
                    .collect::<Vec<_>>()
                    .into(),
            ),
        );
        table
    }

    fn headers_with_death(exchange: &str, key: &str) -> FieldTable {
        let mut headers = FieldTable::default();
        headers.insert(
            "x-death".into(),
            AMQPValue::FieldArray(
                vec![AMQPValue::FieldTable(death_table(exchange, &[key], 2))].into(),
            ),
        );
        headers
    }

    fn held(tag: u64, deaths: Vec<DeathRecord>) -> HeldMessage {
        HeldMessage {
            delivery_tag: tag,
            deaths,
            payload: b"body".to_vec(),
            properties: BasicProperties::default(),
        }
    }

    fn death(exchange: &str, key: &str) -> DeathRecord {
        DeathRecord {
            queue: "orders".to_string(),
            reason: "rejected".to_string(),
            count: 1,
            exchange: exchange.to_string(),
            routing_keys: vec![key.to_string()],
        }
    }

    /// Records every call; publishes answer from `outcomes` (confirmed
    /// when it runs out).
    #[derive(Default)]
    struct FakeMover {
        queue: VecDeque<HeldMessage>,
        outcomes: VecDeque<Result<PublishOutcome, String>>,
        fail_ack: bool,
        calls: Vec<String>,
    }

    impl Mover for FakeMover {
        async fn get(&mut self) -> Result<Option<HeldMessage>, String> {
            self.calls.push("get".to_string());
            Ok(self.queue.pop_front())
        }
        async fn publish(
            &mut self,
            destination: &RedriveDestination,
            message: &HeldMessage,
        ) -> Result<PublishOutcome, String> {
            self.calls.push(format!(
                "publish {} {} {}",
                message.delivery_tag,
                destination.exchange_name(),
                destination.routing_key
            ));
            self.outcomes
                .pop_front()
                .unwrap_or(Ok(PublishOutcome::Confirmed))
        }
        async fn ack(&mut self, delivery_tag: u64) -> Result<(), String> {
            self.calls.push(format!("ack {delivery_tag}"));
            if self.fail_ack {
                Err("channel closed".to_string())
            } else {
                Ok(())
            }
        }
        async fn reject_requeue(&mut self, delivery_tag: u64) -> Result<(), String> {
            self.calls.push(format!("requeue {delivery_tag}"));
            Ok(())
        }
    }

    fn request(limit: u64) -> RedriveRequest {
        RedriveRequest {
            queue: "orders.dlq".to_string(),
            limit,
            to_exchange: None,
            routing_key: None,
        }
    }

    fn run<T>(future: impl std::future::Future<Output = T>) -> T {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(future)
    }

    #[test]
    fn each_message_is_acked_only_after_its_confirm() {
        let mut mover = FakeMover {
            queue: VecDeque::from([
                held(1, vec![death("shop", "order.created")]),
                held(2, vec![death("", "orders")]),
            ]),
            ..FakeMover::default()
        };
        let report = run(move_messages(&mut mover, &request(10))).unwrap();
        assert_eq!(report.moved, 2);
        assert_eq!(
            mover.calls,
            vec![
                "get",
                "publish 1 shop order.created",
                "ack 1",
                "get",
                "publish 2 amq.default orders",
                "ack 2",
                "get",
            ]
        );
        assert_eq!(
            report.destinations["exchange 'shop', routing key 'order.created'"],
            1
        );
    }

    #[test]
    fn the_limit_stops_before_reading_more() {
        let mut mover = FakeMover {
            queue: VecDeque::from([
                held(1, vec![death("shop", "a")]),
                held(2, vec![death("shop", "b")]),
            ]),
            ..FakeMover::default()
        };
        let report = run(move_messages(&mut mover, &request(1))).unwrap();
        assert_eq!(report.moved, 1);
        assert_eq!(mover.calls, vec!["get", "publish 1 shop a", "ack 1"]);
        assert_eq!(mover.queue.len(), 1);
    }

    #[test]
    fn the_first_failed_publish_requeues_and_stops() {
        for (outcome, failure) in [
            (
                Ok(PublishOutcome::NotConfirmed),
                RedriveFailure::NotConfirmed,
            ),
            (Ok(PublishOutcome::Returned), RedriveFailure::Unroutable),
            (Err("socket closed".to_string()), RedriveFailure::Publish),
        ] {
            let mut mover = FakeMover {
                queue: VecDeque::from([
                    held(1, vec![death("shop", "a")]),
                    held(2, vec![death("shop", "b")]),
                    held(3, vec![death("shop", "c")]),
                ]),
                outcomes: VecDeque::from([Ok(PublishOutcome::Confirmed), outcome]),
                ..FakeMover::default()
            };
            let error = run(move_messages(&mut mover, &request(10))).unwrap_err();
            assert_eq!(error.failure, failure);
            assert!(failure.requeued());
            assert_eq!(error.report.moved, 1);
            assert_eq!(
                mover.calls,
                vec![
                    "get",
                    "publish 1 shop a",
                    "ack 1",
                    "get",
                    "publish 2 shop b",
                    "requeue 2",
                ]
            );
            // Message 3 was never read.
            assert_eq!(mover.queue.len(), 1);
        }
    }

    #[test]
    fn a_message_without_x_death_needs_an_exchange() {
        let mut mover = FakeMover {
            queue: VecDeque::from([held(7, Vec::new())]),
            ..FakeMover::default()
        };
        let error = run(move_messages(&mut mover, &request(10))).unwrap_err();
        assert_eq!(error.failure, RedriveFailure::NoDestination);
        assert_eq!(mover.calls, vec!["get", "requeue 7"]);

        let mut mover = FakeMover {
            queue: VecDeque::from([held(7, Vec::new())]),
            ..FakeMover::default()
        };
        let overridden = RedriveRequest {
            to_exchange: Some("retry".to_string()),
            routing_key: Some("again".to_string()),
            ..request(10)
        };
        let report = run(move_messages(&mut mover, &overridden)).unwrap();
        assert_eq!(report.moved, 1);
        assert_eq!(mover.calls[1], "publish 7 retry again");
    }

    #[test]
    fn a_lost_ack_is_reported_as_a_duplicate() {
        let mut mover = FakeMover {
            queue: VecDeque::from([held(1, vec![death("shop", "a")])]),
            fail_ack: true,
            ..FakeMover::default()
        };
        let error = run(move_messages(&mut mover, &request(10))).unwrap_err();
        assert_eq!(error.failure, RedriveFailure::Ack);
        assert!(!error.failure.requeued());
        assert!(error.failure.as_str().contains("in both"));
        assert_eq!(error.report.moved, 0);
    }

    #[test]
    fn failure_texts_carry_no_message_content() {
        let error = RedriveError {
            report: RedriveReport {
                moved: 3,
                ..RedriveReport::default()
            },
            failure: RedriveFailure::Unroutable,
            detail: "exchange 'shop', routing key 'k'".to_string(),
        };
        let text = error.to_string();
        assert!(
            text.starts_with("Stopped after moving 3 message(s)"),
            "{text}"
        );
        for failure in [
            RedriveFailure::Connect,
            RedriveFailure::Read,
            RedriveFailure::NoDestination,
            RedriveFailure::Unroutable,
            RedriveFailure::NotConfirmed,
            RedriveFailure::Publish,
            RedriveFailure::Ack,
        ] {
            assert!(!failure.as_str().is_empty());
        }
    }

    #[test]
    fn x_death_is_read_from_amqp_headers() {
        let headers = headers_with_death("shop", "order.created");
        let deaths = deaths_from_table(&headers);
        assert_eq!(
            deaths,
            vec![DeathRecord {
                count: 2,
                ..death("shop", "order.created")
            }]
        );
        assert!(deaths_from_table(&FieldTable::default()).is_empty());
    }

    #[test]
    fn redriven_messages_keep_their_headers_and_name_the_source() {
        let headers = headers_with_death("shop", "k");
        let redriven = redriven_headers(Some(&headers), "orders.dlq");
        assert!(redriven.contains_key("x-death"));
        assert_eq!(
            redriven.inner().get(REDRIVEN_FROM_HEADER),
            Some(&AMQPValue::LongString("orders.dlq".into()))
        );
        let fresh = redriven_headers(None, "q");
        assert_eq!(fresh.inner().len(), 1);
    }

    #[test]
    fn confirmations_map_to_outcomes() {
        assert_eq!(
            publish_outcome(Confirmation::Ack(None)),
            PublishOutcome::Confirmed
        );
        assert_eq!(
            publish_outcome(Confirmation::Nack(None)),
            PublishOutcome::NotConfirmed
        );
        assert_eq!(
            publish_outcome(Confirmation::NotRequested),
            PublishOutcome::NotConfirmed
        );
    }

    #[test]
    fn the_uri_takes_vhost_and_credentials_from_the_target() {
        let target = AmqpTarget {
            url: "amqps://rabbit.prod.corp".to_string(),
            vhost: "/".to_string(),
            user: Some("ops".to_string()),
            password: Some("pw-secret".to_string()),
            timeout: Duration::from_secs(5),
            tls: TlsFiles::default(),
        };
        let uri = target.uri().unwrap();
        assert_eq!(uri.vhost, "/");
        assert_eq!(uri.authority.host, "rabbit.prod.corp");
        assert_eq!(uri.authority.port, 5671);
        assert_eq!(uri.authority.userinfo.username, "ops");
        assert_eq!(target.address().unwrap(), "rabbit.prod.corp:5671");
        let debug = format!("{target:?}");
        assert!(!debug.contains("pw-secret"), "{debug}");
        let bad = AmqpTarget {
            url: "http://rabbit".to_string(),
            ..target
        };
        assert!(bad.uri().is_err());
    }

    #[test]
    fn tls_files_are_read_into_lapins_config() {
        let dir = std::env::temp_dir().join(format!("ferrika-amqp-tls-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let file = |name: &str, text: &str| {
            let path = dir.join(name);
            std::fs::write(&path, text).unwrap();
            path.display().to_string()
        };
        let ca = file(
            "ca.pem",
            "-----BEGIN CERTIFICATE-----
AA==
-----END CERTIFICATE-----
",
        );
        let cert = file("client.pem", "client-cert");
        let key = file("client.key", "client-key");
        let target = |tls: TlsFiles| AmqpTarget {
            url: "amqps://rabbit".to_string(),
            vhost: "/".to_string(),
            user: None,
            password: None,
            timeout: Duration::from_secs(5),
            tls,
        };
        let none = target(TlsFiles::default()).tls_config().unwrap();
        assert_eq!(none, OwnedTLSConfig::default());
        let full = target(TlsFiles {
            ca_file: Some(ca.clone()),
            client_cert: Some(cert),
            client_key: Some(key),
        })
        .tls_config()
        .unwrap();
        assert!(full.cert_chain.unwrap().contains("BEGIN CERTIFICATE"));
        assert_eq!(
            full.identity,
            Some(OwnedIdentity::PKCS8 {
                pem: b"client-cert".to_vec(),
                key: b"client-key".to_vec(),
            })
        );
        let half = target(TlsFiles {
            ca_file: Some(ca),
            client_cert: Some("x.pem".to_string()),
            client_key: None,
        });
        assert!(half.tls_config().is_err());
        let missing = target(TlsFiles {
            ca_file: Some(dir.join("nope.pem").display().to_string()),
            ..TlsFiles::default()
        });
        let err = missing.tls_config().unwrap_err().to_string();
        assert!(err.contains("Cannot read the CA file"), "{err}");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
