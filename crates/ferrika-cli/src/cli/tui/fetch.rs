//! Reads the cluster on a thread of its own, so a slow or unreachable
//! cluster never holds up the screen. Only reads: metadata, watermarks and
//! committed group offsets.

use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender};
use std::time::{Duration, Instant};

use super::state::{Update, Watermarks};
use crate::cli::GlobalOpts;
use crate::error::KafkaResult;
use crate::kafka::admin::KafkaAdminClient;
use crate::kafka::consumer::KafkaConsumerClient;
use crate::kafka::internal_topic::is_internal_topic;
use crate::model::cluster::ClusterSnapshot;

/// Starts the worker. It refreshes every `interval` and whenever `refresh`
/// receives, and stops once the screen drops either channel. The thread is
/// not joined: a refresh stuck on a timeout must not delay quitting.
pub(super) fn spawn(
    global: GlobalOpts,
    interval: Duration,
    refresh: Receiver<()>,
    updates: Sender<Update>,
) -> std::io::Result<()> {
    std::thread::Builder::new()
        .name("ferrika-top-fetch".to_string())
        .spawn(move || worker(&global, interval, &refresh, &updates))
        .map(|_| ())
}

fn worker(
    global: &GlobalOpts,
    interval: Duration,
    refresh: &Receiver<()>,
    updates: &Sender<Update>,
) {
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            let _ = updates.send(Update::Failed(format!(
                "Cannot start the refresh runtime: {e}"
            )));
            return;
        }
    };
    let mut clients: Option<Clients> = None;
    loop {
        if updates.send(Update::Started).is_err() {
            return;
        }
        let update = match runtime.block_on(read(global, &mut clients)) {
            Ok((snapshot, watermarks, warning)) => Update::Fetched {
                snapshot: Box::new(snapshot),
                watermarks,
                at: Instant::now(),
                warning,
            },
            Err(e) => Update::Failed(e.to_string()),
        };
        if updates.send(update).is_err() {
            return;
        }
        match refresh.recv_timeout(interval) {
            // Several presses while a refresh ran make one refresh.
            Ok(()) => while refresh.try_recv().is_ok() {},
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => return,
        }
    }
}

/// Clients kept between refreshes; librdkafka reconnects on its own.
struct Clients {
    admin: KafkaAdminClient,
    /// Reads watermarks only: never subscribes or commits, so no group is created.
    probe: KafkaConsumerClient,
}

/// One refresh: what `ferrika health` reads (brokers, topics, quorum, group
/// lag) plus every partition's watermarks for record counts and rates.
/// Watermarks that cannot be read are reported as a warning, not an error.
async fn read(
    global: &GlobalOpts,
    clients: &mut Option<Clients>,
) -> KafkaResult<(ClusterSnapshot, Watermarks, Option<String>)> {
    let config = global.to_kafka_config();
    if clients.is_none() {
        *clients = Some(Clients {
            admin: KafkaAdminClient::new(&config)?,
            probe: KafkaConsumerClient::lookup(&config)?,
        });
    }
    let Some(c) = clients.as_ref() else {
        return Err(crate::error::KafkaError::admin("No Kafka client"));
    };

    let mut snapshot = crate::ops::health::snapshot(
        &c.admin,
        &config,
        &global.resolved_profile_name().unwrap_or_default(),
    )
    .await?;

    let keys: Vec<(&str, i32)> = snapshot
        .topics
        .iter()
        .filter(|t| !is_internal_topic(&t.name))
        .flat_map(|t| {
            t.partitions
                .iter()
                .map(move |p| (t.name.as_str(), p.partition_id))
        })
        .collect();
    let (watermarks, warning) = match c
        .probe
        .fetch_watermarks_batch(&keys, Some(global.request_timeout()))
    {
        Ok(w) => (w, None),
        Err(e) => (
            Watermarks::new(),
            Some(format!(
                "Cannot read watermarks (no record counts or rates): {e}"
            )),
        ),
    };
    fill_watermarks(&mut snapshot, &watermarks);
    Ok((snapshot, watermarks, warning))
}

/// Puts the watermarks into the snapshot's partitions, for the health checks
/// that look at partition sizes.
fn fill_watermarks(snapshot: &mut ClusterSnapshot, watermarks: &Watermarks) {
    for t in &mut snapshot.topics {
        let mut total = 0;
        for p in &mut t.partitions {
            if let Some(&(low, high)) = watermarks.get(&(t.name.clone(), p.partition_id)) {
                p.low_watermark = low;
                p.high_watermark = high;
                total += (high - low).max(0);
            }
        }
        t.total_messages = total;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::tui::state::tests::{sample_snapshot, sample_watermarks};

    #[test]
    fn watermarks_go_into_the_snapshot_partitions() {
        let mut snapshot = sample_snapshot();
        fill_watermarks(&mut snapshot, &sample_watermarks(100));
        let orders = &snapshot.topics[0];
        assert_eq!(orders.partitions[0].high_watermark, 100);
        assert_eq!(orders.partitions[1].high_watermark, 10);
        assert_eq!(orders.total_messages, 110);
        let payments = &snapshot.topics[1];
        assert_eq!(payments.partitions[0].low_watermark, 5);
        assert_eq!(payments.total_messages, 20);
        // Not read: left at zero.
        assert_eq!(snapshot.topics[2].total_messages, 0);
    }
}
