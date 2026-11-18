//! Operations built on `ferrika-core` clients and `ferrika-governance`
//! policy: backups and restores, mirroring, dead-letter queues, topic diffs,
//! configuration lint, health checks, incident reports and the change history
//! behind `ferrika undo`.

pub mod backup;
pub mod bench;
pub mod cluster_sync;
pub mod compact;
pub mod diff;
pub mod dlq;
pub mod exporter;
pub mod generator;
pub mod group_lag;
pub mod health;
pub mod history;
pub mod lint;
pub mod mirror;
pub mod probe;
pub mod report;
pub mod time_histogram;
pub mod top;
pub mod topic_stats;
pub mod topology;
pub mod trace;
