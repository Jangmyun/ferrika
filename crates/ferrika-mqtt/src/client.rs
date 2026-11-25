//! One MQTT connection ([`Session`]), MQTT 3.1.1 or 5, plain TCP or TLS
//! (tokio-rustls with the ring backend). Packets are encoded and decoded
//! with the codec of [rumqttc](https://crates.io/crates/rumqttc)
//! (`mqttbytes`); the connection itself is Ferrika's own, small loop.
//!
//! Every session is a clean session with a fresh client id (see
//! [`client_id`]): it never resumes or takes over a session of another
//! client, and the broker keeps nothing for it after it disconnects (MQTT
//! 3.1.1 `clean_session = 1`; MQTT 5 `clean_start = 1` and session expiry
//! 0). No last will is set.
//!
//! The session turns the broker's packets into [`Event`]s; the bounded
//! loops in [`crate::collect`] read them through [`EventSource`], so they can
//! be tested without a broker.

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use rumqttc::mqttbytes::v4;
use rumqttc::mqttbytes::QoS;
use rumqttc::v5::mqttbytes::v5;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::Instant;

use crate::error::{MqttError, MqttResult};
use crate::profile::{BrokerUrl, MqttVersion};

/// The start of every client id Ferrika connects with.
pub const CLIENT_ID_PREFIX: &str = "ferrika";

/// The largest packet accepted from the broker (16 MiB); bigger messages
/// end the session with an error rather than filling memory.
pub const MAX_INCOMING_PACKET: usize = 16 * 1024 * 1024;

/// Keep-alive interval sent to the broker.
const KEEP_ALIVE: Duration = Duration::from_secs(30);

/// How long a clean disconnect may take.
const DISCONNECT_TIMEOUT: Duration = Duration::from_secs(2);

/// A fresh client id: `ferrika` and 12 random hexadecimal digits (19
/// characters, letters and digits only, which every MQTT 3.1.1 broker
/// accepts). Random, so a Ferrika session never takes over (and
/// disconnects) another client that uses the same id.
pub fn client_id() -> String {
    use std::hash::{BuildHasher, Hasher};
    let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    hasher.write_u128(nanos);
    hasher.write_u32(std::process::id());
    format!(
        "{CLIENT_ID_PREFIX}{:012x}",
        hasher.finish() & 0xffff_ffff_ffff
    )
}

/// Where and how to connect.
#[derive(Clone)]
pub struct ConnectTarget {
    pub url: BrokerUrl,
    pub version: MqttVersion,
    /// User and password, or `None` for an anonymous login.
    pub credentials: Option<(String, String)>,
    /// CA certificates (PEM) for TLS instead of the OS trust store.
    pub ca_file: Option<PathBuf>,
    pub client_id: String,
    /// The time limit of each step: connecting, an acknowledgement.
    pub timeout: Duration,
}

// Manual so the password never reaches debug output.
impl std::fmt::Debug for ConnectTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectTarget")
            .field("url", &self.url)
            .field("version", &self.version)
            .field(
                "user",
                &self.credentials.as_ref().map(|(user, _)| user.as_str()),
            )
            .field("ca_file", &self.ca_file)
            .field("client_id", &self.client_id)
            .field("timeout", &self.timeout)
            .finish()
    }
}

/// A message received from the broker.
#[derive(Clone, PartialEq, Eq)]
pub struct MqttMessage {
    pub topic: String,
    pub payload: Vec<u8>,
    /// 0, 1 or 2.
    pub qos: u8,
    /// Set when the broker sent a stored retained message (on subscribe),
    /// not for live messages.
    pub retain: bool,
    /// MQTT 5 user properties, in order.
    pub user_properties: Vec<(String, String)>,
    /// MQTT 5 content type.
    pub content_type: Option<String>,
}

// Manual so payloads (which can hold personal data) never reach logs.
impl std::fmt::Debug for MqttMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MqttMessage")
            .field("topic", &self.topic)
            .field("payload_bytes", &self.payload.len())
            .field("qos", &self.qos)
            .field("retain", &self.retain)
            .field("user_properties", &self.user_properties.len())
            .field("content_type", &self.content_type)
            .finish()
    }
}

/// A message to publish.
#[derive(Clone, PartialEq, Eq)]
pub struct OutgoingMessage {
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: u8,
    pub retain: bool,
    /// MQTT 5 only.
    pub content_type: Option<String>,
    /// MQTT 5 only.
    pub user_properties: Vec<(String, String)>,
}

impl std::fmt::Debug for OutgoingMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OutgoingMessage")
            .field("topic", &self.topic)
            .field("payload_bytes", &self.payload.len())
            .field("qos", &self.qos)
            .field("retain", &self.retain)
            .finish()
    }
}

impl OutgoingMessage {
    /// Checks the topic, the QoS and that MQTT 5 fields are only used with
    /// MQTT 5.
    pub fn validate(&self, version: MqttVersion) -> MqttResult<()> {
        crate::topic::validate_topic(&self.topic)?;
        if self.qos > 2 {
            return Err(MqttError::config("QoS must be 0, 1 or 2"));
        }
        if version == MqttVersion::V311
            && (self.content_type.is_some() || !self.user_properties.is_empty())
        {
            return Err(MqttError::config(
                "Content types and user properties need MQTT 5 (--mqtt-version 5)",
            ));
        }
        Ok(())
    }
}

/// The broker's answer to one topic filter of a SUBSCRIBE.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubscribeOutcome {
    /// Granted with this maximum QoS.
    Granted(u8),
    /// Refused, with the reason.
    Refused(String),
}

/// The broker's answer to a publish (PUBACK for QoS 1, PUBREC and PUBCOMP
/// for QoS 2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishAck {
    /// Set when an MQTT 5 broker refused the message (e.g. not
    /// authorized). MQTT 3.1.1 has no way to say so: a broker that drops a
    /// message it may not accept still acknowledges it.
    pub refusal: Option<String>,
    /// MQTT 5: the broker had no subscriber for the message.
    pub no_subscribers: bool,
}

/// What happened on the connection, as far as the bounded loops care.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    /// A SUBACK.
    Subscribed(Vec<SubscribeOutcome>),
    /// A PUBLISH from the broker.
    Message(MqttMessage),
    /// A publish of ours was handed to the network (all a QoS 0 publish
    /// gets).
    PublishSent,
    /// A PUBREC (the first half of a QoS 2 acknowledgement).
    PublishReceived(PublishAck),
    /// A PUBACK (QoS 1) or PUBCOMP (QoS 2).
    PublishAcked(PublishAck),
    /// Anything else (pings, our own acknowledgements, the CONNACK).
    Other,
}

/// Where the bounded loops read events from: a [`Session`], or a fake in
/// tests.
pub trait EventSource {
    /// The next event; an error ends the session.
    fn next_event(&mut self) -> impl std::future::Future<Output = MqttResult<Event>>;
}

/// The byte stream under a session: TCP, or TLS over TCP.
trait Io: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send {}
impl<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send> Io for T {}

/// One connection to a broker.
///
/// The client side of MQTT that Ferrika needs, on rumqttc's packet codec
/// (`mqttbytes`) and tokio: connect, subscribe, publish with QoS 0 to 2,
/// acknowledge what the broker sends, keep-alive pings and disconnect.
/// Nothing is retried or resent: a broken connection ends the command.
pub struct Session {
    io: Box<dyn Io>,
    version: MqttVersion,
    read_buf: BytesMut,
    /// Events made while sending (a publish handed to the network),
    /// returned before anything is read.
    pending: VecDeque<Event>,
    next_packet_id: u16,
    last_sent: Instant,
    target: String,
    timeout: Duration,
}

impl std::fmt::Debug for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Session")
            .field("target", &self.target)
            .field("version", &self.version)
            .finish_non_exhaustive()
    }
}

/// A packet to send, of either protocol version.
enum Outgoing {
    V311(v4::Packet),
    /// Boxed: MQTT 5 packets are several times larger.
    V5(Box<v5::Packet>),
}

fn qos_of(level: u8) -> QoS {
    match level {
        0 => QoS::AtMostOnce,
        1 => QoS::AtLeastOnce,
        _ => QoS::ExactlyOnce,
    }
}

fn qos5_of(level: u8) -> rumqttc::v5::mqttbytes::QoS {
    match level {
        0 => rumqttc::v5::mqttbytes::QoS::AtMostOnce,
        1 => rumqttc::v5::mqttbytes::QoS::AtLeastOnce,
        _ => rumqttc::v5::mqttbytes::QoS::ExactlyOnce,
    }
}

/// TLS for `mqtts://`: the CA file's certificates (else the OS trust
/// store), rustls with the ring backend.
fn tls_config(ca_file: Option<&std::path::Path>) -> MqttResult<Arc<rustls::ClientConfig>> {
    use rustls::pki_types::pem::PemObject;
    use rustls::pki_types::CertificateDer;

    let tls_err = |msg: String| MqttError::config(format!("TLS setup failed: {msg}"));
    let mut roots = rustls::RootCertStore::empty();
    match ca_file {
        Some(path) => {
            let shown = path.display();
            let certs = CertificateDer::pem_file_iter(path)
                .map_err(|e| tls_err(format!("cannot read CA file '{shown}': {e}")))?;
            for cert in certs {
                let cert = cert.map_err(|e| tls_err(format!("bad CA file '{shown}': {e}")))?;
                roots
                    .add(cert)
                    .map_err(|e| tls_err(format!("bad CA certificate in '{shown}': {e}")))?;
            }
        }
        None => {
            for cert in rustls_native_certs::load_native_certs().certs {
                // Skip certificates rustls cannot parse rather than failing.
                let _ = roots.add(cert);
            }
        }
    }
    if roots.is_empty() {
        return Err(tls_err("no trusted root certificates".to_string()));
    }
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| tls_err(e.to_string()))?
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(Arc::new(config))
}

/// A CONNACK refusal in words.
fn refusal_text(code: &str) -> String {
    match code {
        "BadUserNamePassword" => "bad user name or password".to_string(),
        "NotAuthorized" => "not authorized".to_string(),
        "RefusedProtocolVersion" | "UnsupportedProtocolVersion" => {
            "the broker does not speak this MQTT version (try --mqtt-version 3.1.1)".to_string()
        }
        "BadClientId" | "ClientIdentifierNotValid" => "client id refused".to_string(),
        "ServiceUnavailable" | "ServerUnavailable" | "ServerBusy" => {
            "broker unavailable".to_string()
        }
        other => other.to_string(),
    }
}

/// The CONNECT packet of `target`: a clean session that ends with the
/// connection, no last will.
fn connect_packet(target: &ConnectTarget) -> Outgoing {
    match target.version {
        MqttVersion::V311 => {
            let mut connect = v4::Connect::new(target.client_id.clone());
            connect.keep_alive = KEEP_ALIVE.as_secs() as u16;
            connect.clean_session = true;
            connect.login = target
                .credentials
                .as_ref()
                .map(|(user, password)| v4::Login::new(user.clone(), password.clone()));
            Outgoing::V311(v4::Packet::Connect(connect))
        }
        MqttVersion::V5 => {
            let mut properties = v5::ConnectProperties::new();
            // Session expiry 0 (absent): the session ends with the connection.
            properties.session_expiry_interval = None;
            properties.max_packet_size = Some(MAX_INCOMING_PACKET as u32);
            let connect = v5::Connect {
                keep_alive: KEEP_ALIVE.as_secs() as u16,
                client_id: target.client_id.clone(),
                clean_start: true,
                properties: Some(properties),
            };
            let login = target
                .credentials
                .as_ref()
                .map(|(user, password)| v5::Login::new(user.clone(), password.clone()));
            Outgoing::V5(Box::new(v5::Packet::Connect(connect, None, login)))
        }
    }
}

/// What to do with one packet from the broker: the event it means, and the
/// acknowledgement to send back. An unexpected packet is an error.
fn incoming_v311(packet: v4::Packet) -> MqttResult<(Event, Option<v4::Packet>)> {
    Ok(match packet {
        v4::Packet::Publish(p) => {
            let reply = match p.qos {
                QoS::AtMostOnce => None,
                QoS::AtLeastOnce => Some(v4::Packet::PubAck(v4::PubAck::new(p.pkid))),
                QoS::ExactlyOnce => Some(v4::Packet::PubRec(v4::PubRec::new(p.pkid))),
            };
            let message = MqttMessage {
                topic: p.topic,
                payload: p.payload.to_vec(),
                qos: p.qos as u8,
                retain: p.retain,
                user_properties: Vec::new(),
                content_type: None,
            };
            (Event::Message(message), reply)
        }
        v4::Packet::PubRel(rel) => (
            Event::Other,
            Some(v4::Packet::PubComp(v4::PubComp::new(rel.pkid))),
        ),
        v4::Packet::SubAck(ack) => (
            Event::Subscribed(
                ack.return_codes
                    .iter()
                    .map(|code| match code {
                        v4::SubscribeReasonCode::Success(qos) => {
                            SubscribeOutcome::Granted(*qos as u8)
                        }
                        v4::SubscribeReasonCode::Failure => {
                            SubscribeOutcome::Refused("failure (0x80)".to_string())
                        }
                    })
                    .collect(),
            ),
            None,
        ),
        v4::Packet::PubAck(_) | v4::Packet::PubComp(_) => (
            Event::PublishAcked(PublishAck {
                refusal: None,
                no_subscribers: false,
            }),
            None,
        ),
        v4::Packet::PubRec(rec) => (
            Event::PublishReceived(PublishAck {
                refusal: None,
                no_subscribers: false,
            }),
            Some(v4::Packet::PubRel(v4::PubRel::new(rec.pkid))),
        ),
        v4::Packet::PingResp | v4::Packet::UnsubAck(_) => (Event::Other, None),
        other => {
            return Err(MqttError::Connection(format!(
                "unexpected packet from the broker: {}",
                packet_kind_v311(&other)
            )))
        }
    })
}

fn packet_kind_v311(packet: &v4::Packet) -> &'static str {
    match packet {
        v4::Packet::Connect(_) => "CONNECT",
        v4::Packet::ConnAck(_) => "CONNACK",
        v4::Packet::Publish(_) => "PUBLISH",
        v4::Packet::PubAck(_) => "PUBACK",
        v4::Packet::PubRec(_) => "PUBREC",
        v4::Packet::PubRel(_) => "PUBREL",
        v4::Packet::PubComp(_) => "PUBCOMP",
        v4::Packet::Subscribe(_) => "SUBSCRIBE",
        v4::Packet::SubAck(_) => "SUBACK",
        v4::Packet::Unsubscribe(_) => "UNSUBSCRIBE",
        v4::Packet::UnsubAck(_) => "UNSUBACK",
        v4::Packet::PingReq => "PINGREQ",
        v4::Packet::PingResp => "PINGRESP",
        v4::Packet::Disconnect => "DISCONNECT",
    }
}

/// [`incoming_v311`] for MQTT 5: reason codes on acknowledgements become
/// refusals, and a server DISCONNECT is an error naming its reason.
fn incoming_v5(packet: v5::Packet) -> MqttResult<(Event, Option<v5::Packet>)> {
    use rumqttc::v5::mqttbytes::QoS as Q;
    Ok(match packet {
        v5::Packet::Publish(p) => {
            let reply = match p.qos {
                Q::AtMostOnce => None,
                Q::AtLeastOnce => Some(v5::Packet::PubAck(v5::PubAck::new(p.pkid, None))),
                Q::ExactlyOnce => Some(v5::Packet::PubRec(v5::PubRec::new(p.pkid, None))),
            };
            let (user_properties, content_type) = match p.properties {
                Some(props) => (props.user_properties, props.content_type),
                None => (Vec::new(), None),
            };
            let message = MqttMessage {
                topic: String::from_utf8_lossy(&p.topic).into_owned(),
                payload: p.payload.to_vec(),
                qos: p.qos as u8,
                retain: p.retain,
                user_properties,
                content_type,
            };
            (Event::Message(message), reply)
        }
        v5::Packet::PubRel(rel) => (
            Event::Other,
            Some(v5::Packet::PubComp(v5::PubComp::new(rel.pkid, None))),
        ),
        v5::Packet::SubAck(ack) => (
            Event::Subscribed(
                ack.return_codes
                    .iter()
                    .map(|code| match code {
                        v5::SubscribeReasonCode::Success(qos) => {
                            SubscribeOutcome::Granted(*qos as u8)
                        }
                        other => SubscribeOutcome::Refused(format!("{other:?}")),
                    })
                    .collect(),
            ),
            None,
        ),
        v5::Packet::PubAck(ack) => (
            Event::PublishAcked(PublishAck {
                refusal: match ack.reason {
                    v5::PubAckReason::Success | v5::PubAckReason::NoMatchingSubscribers => None,
                    other => Some(format!("{other:?}")),
                },
                no_subscribers: ack.reason == v5::PubAckReason::NoMatchingSubscribers,
            }),
            None,
        ),
        v5::Packet::PubRec(rec) => {
            let refusal = match rec.reason {
                v5::PubRecReason::Success | v5::PubRecReason::NoMatchingSubscribers => None,
                other => Some(format!("{other:?}")),
            };
            // A refused QoS 2 publish ends there: no PUBREL.
            let reply = refusal
                .is_none()
                .then(|| v5::Packet::PubRel(v5::PubRel::new(rec.pkid, None)));
            (
                Event::PublishReceived(PublishAck {
                    refusal,
                    no_subscribers: rec.reason == v5::PubRecReason::NoMatchingSubscribers,
                }),
                reply,
            )
        }
        v5::Packet::PubComp(comp) => (
            Event::PublishAcked(PublishAck {
                refusal: match comp.reason {
                    v5::PubCompReason::Success => None,
                    other => Some(format!("{other:?}")),
                },
                no_subscribers: false,
            }),
            None,
        ),
        v5::Packet::PingResp(_) | v5::Packet::UnsubAck(_) => (Event::Other, None),
        v5::Packet::Disconnect(disconnect) => {
            let reason = disconnect
                .properties
                .and_then(|p| p.reason_string)
                .map(|text| format!(" ({text})"))
                .unwrap_or_default();
            return Err(MqttError::Connection(format!(
                "the broker disconnected: {:?}{reason}",
                disconnect.reason_code
            )));
        }
        other => {
            return Err(MqttError::Connection(format!(
                "unexpected packet from the broker: {}",
                packet_kind_v5(&other)
            )))
        }
    })
}

fn packet_kind_v5(packet: &v5::Packet) -> &'static str {
    match packet {
        v5::Packet::Auth(_) => "AUTH",
        v5::Packet::Connect(..) => "CONNECT",
        v5::Packet::ConnAck(_) => "CONNACK",
        v5::Packet::Publish(_) => "PUBLISH",
        v5::Packet::PubAck(_) => "PUBACK",
        v5::Packet::PingReq(_) => "PINGREQ",
        v5::Packet::PingResp(_) => "PINGRESP",
        v5::Packet::Subscribe(_) => "SUBSCRIBE",
        v5::Packet::SubAck(_) => "SUBACK",
        v5::Packet::PubRec(_) => "PUBREC",
        v5::Packet::PubRel(_) => "PUBREL",
        v5::Packet::PubComp(_) => "PUBCOMP",
        v5::Packet::Unsubscribe(_) => "UNSUBSCRIBE",
        v5::Packet::UnsubAck(_) => "UNSUBACK",
        v5::Packet::Disconnect(_) => "DISCONNECT",
    }
}

/// One decoded packet of either protocol version.
enum Decoded {
    V311(v4::Packet),
    V5(Box<v5::Packet>),
}

/// The next packet in `buf`, or `None` when it does not hold a whole one
/// yet.
fn decode(version: MqttVersion, buf: &mut BytesMut) -> MqttResult<Option<Decoded>> {
    let protocol = |e: String| MqttError::Connection(format!("malformed packet: {e}"));
    match version {
        MqttVersion::V311 => match v4::Packet::read(buf, MAX_INCOMING_PACKET) {
            Ok(packet) => Ok(Some(Decoded::V311(packet))),
            Err(rumqttc::mqttbytes::Error::InsufficientBytes(_)) => Ok(None),
            Err(e) => Err(protocol(e.to_string())),
        },
        MqttVersion::V5 => match v5::Packet::read(buf, Some(MAX_INCOMING_PACKET as u32)) {
            Ok(packet) => Ok(Some(Decoded::V5(Box::new(packet)))),
            Err(rumqttc::v5::mqttbytes::Error::InsufficientBytes(_)) => Ok(None),
            Err(e) => Err(protocol(e.to_string())),
        },
    }
}

impl Session {
    /// Connects (TCP, then TLS for `mqtts://`), sends CONNECT and waits for
    /// the broker's CONNACK, each step within the target's timeout.
    pub async fn connect(target: &ConnectTarget) -> MqttResult<Self> {
        let address = target.url.address();
        let unreachable = |reason: String| MqttError::Unreachable {
            target: address.clone(),
            reason,
        };
        let seconds = target.timeout.as_secs();
        let tcp = tokio::time::timeout(
            target.timeout,
            tokio::net::TcpStream::connect((target.url.host.as_str(), target.url.port)),
        )
        .await
        .map_err(|_| unreachable(format!("no TCP connection within {seconds} s")))?
        .map_err(|e| unreachable(e.to_string()))?;
        let _ = tcp.set_nodelay(true);
        let io: Box<dyn Io> = if target.url.tls {
            let config = tls_config(target.ca_file.as_deref())?;
            let name = rustls::pki_types::ServerName::try_from(target.url.host.clone())
                .map_err(|e| MqttError::config(format!("Invalid TLS server name: {e}")))?;
            let tls = tokio::time::timeout(
                target.timeout,
                tokio_rustls::TlsConnector::from(config).connect(name, tcp),
            )
            .await
            .map_err(|_| unreachable(format!("no TLS handshake within {seconds} s")))?
            .map_err(|e| unreachable(format!("TLS: {e}")))?;
            Box::new(tls)
        } else {
            Box::new(tcp)
        };
        let mut session = Self {
            io,
            version: target.version,
            read_buf: BytesMut::with_capacity(8 * 1024),
            pending: VecDeque::new(),
            next_packet_id: 0,
            last_sent: Instant::now(),
            target: address.clone(),
            timeout: target.timeout,
        };
        session.send(connect_packet(target)).await?;
        let connack = tokio::time::timeout(target.timeout, session.read_packet())
            .await
            .map_err(|_| unreachable(format!("no CONNACK within {seconds} s")))??;
        let refused = |code: String| MqttError::Refused {
            target: address.clone(),
            reason: refusal_text(&code),
        };
        match connack {
            Decoded::V311(v4::Packet::ConnAck(ack)) => {
                if ack.code != v4::ConnectReturnCode::Success {
                    return Err(refused(format!("{:?}", ack.code)));
                }
            }
            Decoded::V5(packet) => match *packet {
                v5::Packet::ConnAck(ack) => {
                    if ack.code != v5::ConnectReturnCode::Success {
                        return Err(refused(format!("{:?}", ack.code)));
                    }
                }
                _ => {
                    return Err(MqttError::Connection(
                        "the broker did not answer CONNECT with CONNACK".to_string(),
                    ))
                }
            },
            _ => {
                return Err(MqttError::Connection(
                    "the broker did not answer CONNECT with CONNACK".to_string(),
                ))
            }
        }
        Ok(session)
    }

    /// `host:port` of the broker.
    pub fn address(&self) -> &str {
        &self.target
    }

    /// The time limit of each step.
    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    /// A packet id for a SUBSCRIBE or a QoS 1/2 PUBLISH (1 to 65535).
    fn packet_id(&mut self) -> u16 {
        self.next_packet_id = self.next_packet_id.wrapping_add(1).max(1);
        self.next_packet_id
    }

    async fn send(&mut self, packet: Outgoing) -> MqttResult<()> {
        let mut buf = BytesMut::new();
        let encoded = match &packet {
            Outgoing::V311(p) => p
                .write(&mut buf, MAX_INCOMING_PACKET)
                .map(|_| ())
                .map_err(|e| e.to_string()),
            Outgoing::V5(p) => p
                .write(&mut buf, None)
                .map(|_| ())
                .map_err(|e| e.to_string()),
        };
        encoded.map_err(|e| MqttError::config(format!("Cannot encode the packet: {e}")))?;
        let timeout = self.timeout;
        let io = &mut self.io;
        let write = async {
            io.write_all(&buf).await?;
            io.flush().await
        };
        tokio::time::timeout(timeout, write)
            .await
            .map_err(|_| MqttError::Timeout {
                seconds: timeout.as_secs(),
                waiting_for: "sending to the broker".to_string(),
            })?
            .map_err(|e| MqttError::Connection(e.to_string()))?;
        self.last_sent = Instant::now();
        Ok(())
    }

    /// The next whole packet, reading as needed. Sends a PINGREQ when
    /// nothing was sent for half the keep-alive time, so a long quiet
    /// subscription stays connected.
    async fn read_packet(&mut self) -> MqttResult<Decoded> {
        loop {
            if let Some(packet) = decode(self.version, &mut self.read_buf)? {
                return Ok(packet);
            }
            let ping_at = self.last_sent + KEEP_ALIVE / 2;
            let read = tokio::time::timeout_at(ping_at, self.io.read_buf(&mut self.read_buf)).await;
            match read {
                Err(_) => {
                    let ping = match self.version {
                        MqttVersion::V311 => Outgoing::V311(v4::Packet::PingReq),
                        MqttVersion::V5 => Outgoing::V5(Box::new(v5::Packet::PingReq(v5::PingReq))),
                    };
                    self.send(ping).await?;
                }
                Ok(Ok(0)) => {
                    return Err(MqttError::Connection(
                        "the broker closed the connection".to_string(),
                    ))
                }
                Ok(Ok(_)) => {}
                Ok(Err(e)) => return Err(MqttError::Connection(e.to_string())),
            }
        }
    }

    /// Sends a SUBSCRIBE for `filter` (the SUBACK arrives as an
    /// [`Event::Subscribed`]).
    pub async fn subscribe(&mut self, filter: &str, qos: u8) -> MqttResult<()> {
        crate::topic::validate_filter(filter)?;
        let id = self.packet_id();
        let packet = match self.version {
            MqttVersion::V311 => {
                let mut subscribe = v4::Subscribe::new(filter, qos_of(qos));
                subscribe.pkid = id;
                Outgoing::V311(v4::Packet::Subscribe(subscribe))
            }
            MqttVersion::V5 => {
                let mut subscribe = v5::Subscribe::new(v5::Filter::new(filter, qos5_of(qos)), None);
                subscribe.pkid = id;
                Outgoing::V5(Box::new(v5::Packet::Subscribe(subscribe)))
            }
        };
        self.send(packet).await
    }

    /// Sends a PUBLISH; [`Event::PublishSent`] follows, then the broker's
    /// acknowledgement for QoS 1 and 2.
    pub async fn publish(&mut self, message: &OutgoingMessage) -> MqttResult<()> {
        message.validate(self.version)?;
        let id = if message.qos > 0 { self.packet_id() } else { 0 };
        let packet = match self.version {
            MqttVersion::V311 => {
                let mut publish = v4::Publish::new(
                    message.topic.clone(),
                    qos_of(message.qos),
                    message.payload.clone(),
                );
                publish.retain = message.retain;
                publish.pkid = id;
                Outgoing::V311(v4::Packet::Publish(publish))
            }
            MqttVersion::V5 => {
                let properties = (message.content_type.is_some()
                    || !message.user_properties.is_empty())
                .then(|| v5::PublishProperties {
                    content_type: message.content_type.clone(),
                    user_properties: message.user_properties.clone(),
                    ..Default::default()
                });
                let mut publish = v5::Publish::new(
                    message.topic.clone(),
                    qos5_of(message.qos),
                    message.payload.clone(),
                    properties,
                );
                publish.retain = message.retain;
                publish.pkid = id;
                Outgoing::V5(Box::new(v5::Packet::Publish(publish)))
            }
        };
        self.send(packet).await?;
        self.pending.push_back(Event::PublishSent);
        Ok(())
    }

    /// Sends a DISCONNECT and closes the connection. Errors are ignored:
    /// the session is over either way.
    pub async fn disconnect(mut self) {
        let packet = match self.version {
            MqttVersion::V311 => Outgoing::V311(v4::Packet::Disconnect),
            MqttVersion::V5 => Outgoing::V5(Box::new(v5::Packet::Disconnect(v5::Disconnect {
                reason_code: v5::DisconnectReasonCode::NormalDisconnection,
                properties: None,
            }))),
        };
        self.timeout = self.timeout.min(DISCONNECT_TIMEOUT);
        if self.send(packet).await.is_ok() {
            let _ = tokio::time::timeout(DISCONNECT_TIMEOUT, self.io.shutdown()).await;
        }
    }
}

impl EventSource for Session {
    async fn next_event(&mut self) -> MqttResult<Event> {
        if let Some(event) = self.pending.pop_front() {
            return Ok(event);
        }
        let (event, reply) = match self.read_packet().await? {
            Decoded::V311(packet) => {
                let (event, reply) = incoming_v311(packet)?;
                (event, reply.map(Outgoing::V311))
            }
            Decoded::V5(packet) => {
                let (event, reply) = incoming_v5(*packet)?;
                (event, reply.map(|p| Outgoing::V5(Box::new(p))))
            }
        };
        if let Some(reply) = reply {
            self.send(reply).await?;
        }
        Ok(event)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_ids_are_short_alphanumeric_and_fresh() {
        let a = client_id();
        let b = client_id();
        assert_ne!(a, b);
        assert!(a.starts_with(CLIENT_ID_PREFIX));
        assert_eq!(a.len(), CLIENT_ID_PREFIX.len() + 12);
        assert!(a.len() <= 23);
        assert!(a.chars().all(|c| c.is_ascii_alphanumeric()), "{a}");
    }

    #[test]
    fn mqtt5_fields_need_mqtt5() {
        let message = OutgoingMessage {
            topic: "a/b".to_string(),
            payload: b"x".to_vec(),
            qos: 1,
            retain: false,
            content_type: Some("application/json".to_string()),
            user_properties: Vec::new(),
        };
        assert!(message.validate(MqttVersion::V5).is_ok());
        assert!(message.validate(MqttVersion::V311).is_err());
        let wildcard = OutgoingMessage {
            topic: "a/#".to_string(),
            content_type: None,
            ..message.clone()
        };
        assert!(wildcard.validate(MqttVersion::V311).is_err());
        let bad_qos = OutgoingMessage {
            qos: 3,
            content_type: None,
            ..message
        };
        assert!(bad_qos.validate(MqttVersion::V311).is_err());
    }

    fn target(version: MqttVersion) -> ConnectTarget {
        ConnectTarget {
            url: BrokerUrl::parse("mqtt://b").unwrap(),
            version,
            credentials: Some(("ops".to_string(), "hidden-pw".to_string())),
            ca_file: None,
            client_id: client_id(),
            timeout: Duration::from_secs(1),
        }
    }

    #[test]
    fn debug_output_hides_passwords_and_payloads() {
        let debug = format!("{:?}", target(MqttVersion::V311));
        assert!(!debug.contains("hidden-pw"), "{debug}");
        let message = MqttMessage {
            topic: "t".to_string(),
            payload: b"secret-body".to_vec(),
            qos: 0,
            retain: false,
            user_properties: Vec::new(),
            content_type: None,
        };
        assert!(!format!("{message:?}").contains("secret-body"));
    }

    #[test]
    fn connect_asks_for_a_clean_session_that_ends_with_the_connection() {
        match connect_packet(&target(MqttVersion::V311)) {
            Outgoing::V311(v4::Packet::Connect(c)) => {
                assert!(c.clean_session);
                assert!(c.last_will.is_none());
                assert_eq!(c.login.map(|l| l.username), Some("ops".to_string()));
            }
            _ => panic!("an MQTT 3.1.1 CONNECT"),
        }
        let Outgoing::V5(packet) = connect_packet(&target(MqttVersion::V5)) else {
            panic!("an MQTT 5 packet")
        };
        match *packet {
            v5::Packet::Connect(c, will, login) => {
                assert!(c.clean_start);
                assert!(will.is_none());
                assert!(login.is_some());
                let props = c.properties.unwrap();
                assert_eq!(props.session_expiry_interval, None);
                assert_eq!(props.max_packet_size, Some(MAX_INCOMING_PACKET as u32));
            }
            _ => panic!("an MQTT 5 CONNECT"),
        }
    }

    #[test]
    fn incoming_messages_are_acknowledged_by_their_qos() {
        let mut publish = v4::Publish::new("a/b", QoS::AtLeastOnce, b"v".to_vec());
        publish.retain = true;
        publish.pkid = 7;
        let (event, reply) = incoming_v311(v4::Packet::Publish(publish)).unwrap();
        let Event::Message(message) = event else {
            panic!("a message")
        };
        assert_eq!(
            (message.topic.as_str(), message.qos, message.retain),
            ("a/b", 1, true)
        );
        assert!(matches!(reply, Some(v4::Packet::PubAck(ack)) if ack.pkid == 7));

        let mut exactly_once = v4::Publish::new("a/b", QoS::ExactlyOnce, b"v".to_vec());
        exactly_once.pkid = 8;
        let (_, reply) = incoming_v311(v4::Packet::Publish(exactly_once)).unwrap();
        assert!(matches!(reply, Some(v4::Packet::PubRec(rec)) if rec.pkid == 8));
        let (_, reply) = incoming_v311(v4::Packet::PubRel(v4::PubRel::new(8))).unwrap();
        assert!(matches!(reply, Some(v4::Packet::PubComp(c)) if c.pkid == 8));

        let at_most_once = v4::Publish::new("a/b", QoS::AtMostOnce, b"v".to_vec());
        let (_, reply) = incoming_v311(v4::Packet::Publish(at_most_once)).unwrap();
        assert!(reply.is_none());

        // Our QoS 2 publish: PUBREC is answered with PUBREL.
        let (event, reply) = incoming_v311(v4::Packet::PubRec(v4::PubRec::new(3))).unwrap();
        assert!(matches!(event, Event::PublishReceived(_)));
        assert!(matches!(reply, Some(v4::Packet::PubRel(r)) if r.pkid == 3));
        assert!(incoming_v311(v4::Packet::Disconnect).is_err());
    }

    #[test]
    fn mqtt5_reason_codes_become_refusals() {
        let mut ack = v5::PubAck::new(1, None);
        ack.reason = v5::PubAckReason::NotAuthorized;
        let (event, _) = incoming_v5(v5::Packet::PubAck(ack)).unwrap();
        assert_eq!(
            event,
            Event::PublishAcked(PublishAck {
                refusal: Some("NotAuthorized".to_string()),
                no_subscribers: false
            })
        );
        let mut ack = v5::PubAck::new(1, None);
        ack.reason = v5::PubAckReason::NoMatchingSubscribers;
        let (event, _) = incoming_v5(v5::Packet::PubAck(ack)).unwrap();
        assert_eq!(
            event,
            Event::PublishAcked(PublishAck {
                refusal: None,
                no_subscribers: true
            })
        );
        let mut rec = v5::PubRec::new(2, None);
        rec.reason = v5::PubRecReason::QuotaExceeded;
        let (event, reply) = incoming_v5(v5::Packet::PubRec(rec)).unwrap();
        assert!(matches!(
            event,
            Event::PublishReceived(PublishAck {
                refusal: Some(_),
                ..
            })
        ));
        assert!(reply.is_none());
        let disconnect = v5::Disconnect {
            reason_code: v5::DisconnectReasonCode::NotAuthorized,
            properties: None,
        };
        let err = incoming_v5(v5::Packet::Disconnect(disconnect)).unwrap_err();
        assert!(err.to_string().contains("NotAuthorized"), "{err}");
    }

    #[test]
    fn packets_are_decoded_once_whole() {
        let mut buf = BytesMut::new();
        v4::Packet::PubAck(v4::PubAck::new(5))
            .write(&mut buf, MAX_INCOMING_PACKET)
            .unwrap();
        let whole = buf.clone();
        let mut partial = BytesMut::from(&whole[..1]);
        assert!(decode(MqttVersion::V311, &mut partial).unwrap().is_none());
        let mut buf = whole;
        assert!(matches!(
            decode(MqttVersion::V311, &mut buf).unwrap(),
            Some(Decoded::V311(v4::Packet::PubAck(_)))
        ));
        assert!(buf.is_empty());
        let mut garbage = BytesMut::from(&[0xf0u8, 0x00][..]);
        assert!(decode(MqttVersion::V311, &mut garbage).is_err());
    }

    #[test]
    fn tls_needs_a_readable_ca_file() {
        let missing = std::env::temp_dir().join("ferrika-mqtt-no-such-ca.pem");
        let err = tls_config(Some(&missing)).unwrap_err();
        assert!(err.to_string().contains("CA file"), "{err}");
    }
}
