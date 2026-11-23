//! RabbitMQ support for Ferrika (preview): a typed client for the RabbitMQ
//! management HTTP API, connection profiles and dead-letter redrive over
//! AMQP.
//!
//! - [`model`]: queues, exchanges, bindings and peeked messages as the API
//!   returns them, the request bodies, and the `x-death` history of
//!   dead-lettered messages. No I/O.
//! - [`client`]: [`client::ManagementClient`], which lists queues, exchanges
//!   and bindings of a virtual host, peeks at messages and publishes to an
//!   exchange over HTTP(S).
//! - [`profile`]: named connections in `rabbit-profiles.json`, passwords in
//!   the OS credential store.
//! - `amqp` (feature `amqp`, default): moving dead-lettered messages back
//!   to their exchange with manual acks and publisher confirms.
//! - [`connection`]: a profile with the user's overrides applied, and from
//!   it the login, the management client and the AMQP target.
//! - [`view`]: messages as a front end shows them, PII-masked on request;
//!   the dead-letter inspection, the redrive preview and the redrive's
//!   audit text.
//!
//! The company policy and the audit log are applied by the caller (the
//! `ferrika rabbit` command, or another front end), through the same gates
//! as the Kafka commands (`ferrika_governance::policy`, including
//! `extra_category_refusal` for a redrive); see
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

#[cfg(feature = "amqp")]
pub mod amqp;
pub mod client;
pub mod connection;
pub mod error;
/// An in-memory management API for tests (feature `test-util`).
#[cfg(any(test, feature = "test-util"))]
#[doc(hidden)]
pub mod fake;
pub mod model;
pub mod profile;
pub mod view;
