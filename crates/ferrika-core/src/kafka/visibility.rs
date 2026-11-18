//! Waiting for a change to become visible. KRaft brokers apply config
//! changes through the metadata log, so a DescribeConfigs right after an
//! IncrementalAlterConfigs can still answer with the old value. Instead of
//! sleeping a fixed time, callers read again with a growing delay until the
//! value they expect is there or a deadline passes.

use std::future::Future;
use std::time::Duration;

use crate::kafka::admin::{ConfigEntryView, ConfigOrigin};
use crate::kafka::wire::ConfigChange;

/// How long to keep reading and how soon to read again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Backoff {
    /// The wait before the second read; each later wait doubles.
    pub first: Duration,
    /// No read starts after this much time; the last read is returned.
    pub deadline: Duration,
}

/// Reads of a config change: 100, 200, 400 ms... for up to 3 s.
pub const CONFIG_VISIBILITY: Backoff = Backoff {
    first: Duration::from_millis(100),
    deadline: Duration::from_secs(3),
};

/// Reads with `read` until `visible` accepts the result or `backoff`'s
/// deadline passes, and returns the last read either way. The first read
/// is immediate; a read error ends the wait.
pub async fn await_config<T, E, F, Fut, P>(
    backoff: Backoff,
    mut read: F,
    visible: P,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    P: Fn(&T) -> bool,
{
    let start = tokio::time::Instant::now();
    let mut delay = backoff.first;
    loop {
        let value = read().await?;
        if visible(&value) {
            return Ok(value);
        }
        let remaining = backoff.deadline.saturating_sub(start.elapsed());
        if remaining.is_zero() {
            return Ok(value);
        }
        tokio::time::sleep(delay.min(remaining)).await;
        delay = delay.saturating_mul(2);
    }
}

/// Whether `entries` (one resource's DescribeConfigs answer) show every
/// change of `changes` made at the resource's own level `own`
/// (`TopicOverride` for a topic, `BrokerDynamic` for one broker): a set key
/// has the new value from that level, a removed key no longer comes from
/// it. A sensitive value is not returned, so only its level is compared.
pub fn changes_visible(
    entries: &[ConfigEntryView],
    changes: &[ConfigChange],
    own: ConfigOrigin,
) -> bool {
    changes.iter().all(|change| match change {
        ConfigChange::Set(key, value) => entries
            .iter()
            .any(|e| &e.name == key && e.source == own && (e.sensitive || &e.value == value)),
        ConfigChange::Delete(key) => !entries.iter().any(|e| &e.name == key && e.source == own),
    })
}

/// [`changes_visible`] for a change of the cluster-wide broker default, as
/// one broker's answer shows it. A broker with a value of its own keeps
/// showing that value, so the default cannot be seen there and the key
/// counts as visible.
pub fn default_changes_visible(entries: &[ConfigEntryView], changes: &[ConfigChange]) -> bool {
    let masked = |key: &str| {
        entries
            .iter()
            .any(|e| e.name == key && e.source == ConfigOrigin::BrokerDynamic)
    };
    changes.iter().all(|change| match change {
        ConfigChange::Set(key, _) if masked(key) => true,
        ConfigChange::Delete(key) if masked(key) => true,
        _ => changes_visible(
            entries,
            std::slice::from_ref(change),
            ConfigOrigin::BrokerDynamicDefault,
        ),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    const FAST: Backoff = Backoff {
        first: Duration::from_millis(1),
        deadline: Duration::from_millis(40),
    };

    fn block_on<F: Future>(future: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("runtime")
            .block_on(future)
    }

    /// A reader whose `n`-th read (from 1) returns `n`.
    fn counter(reads: &Cell<u32>) -> impl FnMut() -> std::future::Ready<Result<u32, String>> + '_ {
        move || {
            reads.set(reads.get() + 1);
            std::future::ready(Ok(reads.get()))
        }
    }

    #[test]
    fn a_visible_first_read_is_returned_without_waiting() {
        let reads = Cell::new(0);
        let started = std::time::Instant::now();
        let got = block_on(await_config(CONFIG_VISIBILITY, counter(&reads), |_| true));
        assert_eq!(got, Ok(1));
        assert_eq!(reads.get(), 1);
        assert!(started.elapsed() < Duration::from_millis(100));
    }

    #[test]
    fn reads_again_until_the_value_is_visible() {
        let reads = Cell::new(0);
        let got = block_on(await_config(FAST, counter(&reads), |n| *n >= 3));
        assert_eq!(got, Ok(3));
        assert_eq!(reads.get(), 3);
    }

    #[test]
    fn a_value_never_visible_returns_the_last_read_at_the_deadline() {
        let reads = Cell::new(0);
        let started = std::time::Instant::now();
        let got = block_on(await_config(FAST, counter(&reads), |_| false));
        assert!(started.elapsed() >= FAST.deadline);
        // 1, 2, 4, 8, 16 ms waits, then the rest of the 40 ms: a few reads.
        let last = reads.get();
        assert!((2..=8).contains(&last), "{last} reads");
        assert_eq!(got, Ok(last));
    }

    #[test]
    fn a_read_error_ends_the_wait() {
        let reads = Cell::new(0);
        let got: Result<u32, String> = block_on(await_config(
            FAST,
            || {
                reads.set(reads.get() + 1);
                std::future::ready(Err("denied".to_string()))
            },
            |_| false,
        ));
        assert_eq!(got, Err("denied".to_string()));
        assert_eq!(reads.get(), 1);
    }

    fn entry(name: &str, value: &str, source: ConfigOrigin) -> ConfigEntryView {
        ConfigEntryView {
            name: name.into(),
            value: value.into(),
            source,
            read_only: false,
            sensitive: false,
        }
    }

    fn set(key: &str, value: &str) -> ConfigChange {
        ConfigChange::Set(key.into(), value.into())
    }

    #[test]
    fn topic_changes_are_visible_once_the_override_shows_them() {
        let old = vec![entry("retention.ms", "1", ConfigOrigin::TopicOverride)];
        let new = vec![entry("retention.ms", "2", ConfigOrigin::TopicOverride)];
        let changes = [set("retention.ms", "2")];
        assert!(!changes_visible(
            &old,
            &changes,
            ConfigOrigin::TopicOverride
        ));
        assert!(changes_visible(&new, &changes, ConfigOrigin::TopicOverride));

        let reset = [ConfigChange::Delete("retention.ms".into())];
        let default = vec![entry("retention.ms", "604800000", ConfigOrigin::Default)];
        assert!(!changes_visible(&new, &reset, ConfigOrigin::TopicOverride));
        assert!(changes_visible(
            &default,
            &reset,
            ConfigOrigin::TopicOverride
        ));
    }

    #[test]
    fn a_sensitive_value_is_visible_by_its_level_alone() {
        let mut secret = entry("sasl.jaas.config", "", ConfigOrigin::BrokerDynamic);
        secret.sensitive = true;
        let changes = [set("sasl.jaas.config", "x")];
        assert!(changes_visible(
            &[secret],
            &changes,
            ConfigOrigin::BrokerDynamic
        ));
    }

    #[test]
    fn a_broker_value_of_its_own_hides_the_cluster_default() {
        let changes = [set("log.retention.ms", "5")];
        let pending = vec![entry("log.retention.ms", "1", ConfigOrigin::BrokerStatic)];
        let applied = vec![entry(
            "log.retention.ms",
            "5",
            ConfigOrigin::BrokerDynamicDefault,
        )];
        let masked = vec![entry("log.retention.ms", "9", ConfigOrigin::BrokerDynamic)];
        assert!(!default_changes_visible(&pending, &changes));
        assert!(default_changes_visible(&applied, &changes));
        assert!(default_changes_visible(&masked, &changes));
    }
}
