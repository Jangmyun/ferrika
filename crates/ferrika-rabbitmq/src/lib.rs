//! RabbitMQ support for Ferrika (preview): a typed client for the RabbitMQ
//! management HTTP API.
//!
//! - [`model`]: queues, exchanges, bindings and peeked messages as the API
//!   returns them, plus the request bodies. No I/O.
//! - [`client`]: [`client::ManagementClient`], which lists queues, exchanges
//!   and bindings of a virtual host, peeks at messages and publishes to an
//!   exchange over HTTP(S).
//!
//! This crate only talks to the broker. The company policy, the audit log
//! and PII masking are applied by the caller (the `ferrika rabbit` command),
//! through the same gates as the Kafka commands; see
//! `docs/plans/multi-broker.md`.
//!
//! # Example
//!
//! ```no_run
//! use ferrika_rabbitmq::client::ManagementClient;
//!
//! # async fn run() -> ferrika_rabbitmq::error::RabbitResult<()> {
//! let client = ManagementClient::new("http://localhost:15672")?
//!     .with_credentials("guest", "guest");
//! for queue in client.queues("/").await? {
//!     println!("{} {}", queue.name, queue.messages.unwrap_or_default());
//! }
//! # Ok(())
//! # }
//! ```

pub mod client;
pub mod error;
pub mod model;
