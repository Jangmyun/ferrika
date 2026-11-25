//! MQTT support for Ferrika (preview): bounded subscriptions, publishing,
//! listing and clearing retained messages, and connection profiles, for
//! MQTT 3.1.1 and 5 brokers (Mosquitto, EMQX, HiveMQ, MQTT bridges).
//!
//! - [`profile`]: named connections in `mqtt-profiles.json`, passwords in
//!   the OS credential store; broker URLs (`mqtt://`, `mqtts://`).
//! - [`connection`]: a profile with the user's overrides applied, and from
//!   it the login and the connection target.
//! - [`client`]: one clean MQTT session over
//!   [rumqttc](https://crates.io/crates/rumqttc) (TLS through rustls with
//!   ring), and the [`client::Event`]s it yields.
//! - [`collect`]: the bounded loops: a subscription up to a count or a
//!   time limit, the retained messages under a filter, a publish until its
//!   acknowledgement.
//! - [`topic`]: topic name and filter checks, filter matching.
//! - [`view`]: messages as a front end shows them, PII-masked on request.
//!
//! The company policy and the audit log are applied by the caller (the
//! `ferrika mqtt` command, or another front end), through the same gates
//! as the Kafka and RabbitMQ commands (`ferrika_governance::policy`); see
//! `docs/plans/mqtt.md`.
//!
//! # Example
//!
//! ```no_run
//! use std::time::Duration;
//! use ferrika_mqtt::client::Session;
//! use ferrika_mqtt::collect::{await_subscription, collect_retained, RetainedWindow};
//! use ferrika_mqtt::connection::ResolvedMqtt;
//! use ferrika_mqtt::profile::MqttProfile;
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let resolved = ResolvedMqtt::from_profile("dev", &MqttProfile::new("mqtt://localhost"));
//! let target = resolved.target(None, Duration::from_secs(10))?;
//! let mut session = Session::connect(&target).await?;
//! session.subscribe("devices/+/state", 0).await?;
//! let (_, early) = await_subscription(&mut session, "devices/+/state", target.timeout).await?;
//! let window = RetainedWindow::new(Duration::from_secs(5), 500);
//! let report = collect_retained(&mut session, early, "devices/+/state", window).await?;
//! for message in &report.messages {
//!     println!("{} ({} bytes)", message.topic, message.payload.len());
//! }
//! session.disconnect().await;
//! # Ok(())
//! # }
//! ```

pub mod client;
pub mod collect;
pub mod connection;
pub mod error;
pub mod profile;
pub mod topic;
pub mod view;
