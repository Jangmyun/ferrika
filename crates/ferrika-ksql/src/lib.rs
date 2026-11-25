//! ksqlDB support for Ferrika (preview): a client for an existing ksqlDB
//! server's REST API, and the statement classification the company policy
//! gates need. Ferrika does not embed or ship ksqlDB.
//!
//! - [`config`]: [`config::KsqlConfig`], the server's URL, login, TLS files
//!   and time limit, by hand or from a cluster profile (`ksql_url`).
//! - [`client`]: [`client::KsqlClient`]: server info, `SHOW` listings,
//!   `DESCRIBE`, statements (`/ksql`), pull and push queries
//!   (`/query-stream`) with row and time limits and closing the query.
//! - [`statement`]: splits a script into statements and classifies each as
//!   a read, a session statement or a write with the policy categories it
//!   needs.
//! - [`view`]: query rows PII-masked per column, as objects or table cells.
//! - [`model`]: the REST API's responses as typed values.
//!
//! The company policy and the audit log are applied by the caller (the
//! `ferrika ksql` command, the MCP server, or another front end) with
//! `ferrika_governance::policy`, per statement and before anything is sent;
//! see `docs/plans/ksqldb-client.md`.
//!
//! # Example
//!
//! ```no_run
//! use ferrika_ksql::client::{KsqlClient, QueryLimits, Session};
//! use ferrika_ksql::config::KsqlConfig;
//!
//! # async fn run() -> ferrika_ksql::error::KsqlResult<()> {
//! let client = KsqlClient::new(&KsqlConfig::new("http://localhost:8088"))?;
//! println!("ksqlDB {}", client.info().await?.version);
//! let result = client
//!     .run_query(
//!         "SELECT * FROM orders EMIT CHANGES;",
//!         &Session::default(),
//!         QueryLimits { max_rows: 10, timeout: Some(std::time::Duration::from_secs(5)) },
//!         std::future::pending(),
//!         |_, _| {},
//!     )
//!     .await?
//!     .masked();
//! println!("{} rows, ended by {:?}", result.rows.len(), result.end);
//! # Ok(())
//! # }
//! ```

pub mod client;
pub mod config;
pub mod error;
mod http;
pub mod model;
pub mod statement;
pub mod view;
