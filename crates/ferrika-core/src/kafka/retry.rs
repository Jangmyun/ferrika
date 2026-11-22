//! Retrying metadata calls that fail for a moment right after a change, such
//! as `NotLeaderForPartition` once a topic was created.

use std::time::Duration;

use crate::error::{ErrorClass, ErrorCode, KafkaError, KafkaResult};

/// Tries of [`retry_metadata`] and of one boundary lookup.
pub const TRIES: u32 = 4;

/// How long to wait before retry `attempt` (0-based): 0.5 s, 1 s, 2 s.
pub fn backoff(attempt: u32) -> Duration {
    Duration::from_millis(500) * 2u32.pow(attempt)
}

/// Runs `op`, retrying with [`backoff`] while it fails with a retriable
/// metadata error, at most [`TRIES`] times.
pub fn retry_metadata<T>(mut op: impl FnMut() -> KafkaResult<T>) -> KafkaResult<T> {
    for attempt in 0..TRIES - 1 {
        match op() {
            Err(e) if e.class() == ErrorClass::Retriable => {
                tracing::debug!(%e, "retrying after a metadata error");
                std::thread::sleep(backoff(attempt));
            }
            other => return other,
        }
    }
    op()
}

/// The error says a partition has no ready leader yet or its leader just
/// moved: a topic created a moment ago (brokers that open partitions
/// asynchronously, such as AutoMQ, answer this way for about a second), or a
/// leader election. The broker answers such requests at once, so retrying
/// them is cheap, unlike timeouts or an unreachable cluster.
pub fn is_leader_change(error: &KafkaError) -> bool {
    let KafkaError::Driver(driver) = error else {
        return false;
    };
    matches!(
        driver.code(),
        Some(
            ErrorCode::NotLeaderForPartition
                | ErrorCode::LeaderNotAvailable
                | ErrorCode::UnknownLeaderEpoch
                | ErrorCode::FencedLeaderEpoch
                | ErrorCode::OffsetNotAvailable
        )
    )
}

/// Runs `op`, retrying with [`backoff`] while it fails with a
/// [leader change](is_leader_change), at most [`TRIES`] times. Other errors,
/// timeouts included, are returned at once.
pub fn retry_leader_change<T>(mut op: impl FnMut() -> KafkaResult<T>) -> KafkaResult<T> {
    for attempt in 0..TRIES - 1 {
        match op() {
            Err(e) if is_leader_change(&e) => {
                tracing::debug!(%e, "retrying while the partition leader settles");
                std::thread::sleep(backoff(attempt));
            }
            other => return other,
        }
    }
    op()
}

#[cfg(test)]
mod tests {
    use super::*;
    use rdkafka::types::RDKafkaErrorCode as Code;

    fn driver(code: Code) -> KafkaError {
        KafkaError::from(rdkafka::error::KafkaError::MetadataFetch(code))
    }

    #[test]
    fn leader_changes_are_told_apart_from_slow_or_missing_brokers() {
        assert!(is_leader_change(&driver(Code::NotLeaderForPartition)));
        assert!(is_leader_change(&driver(Code::LeaderNotAvailable)));
        assert!(is_leader_change(&driver(Code::OffsetNotAvailable)));
        // These wait out a timeout each time, or mean the topic is gone.
        assert!(!is_leader_change(&driver(Code::RequestTimedOut)));
        assert!(!is_leader_change(&driver(Code::AllBrokersDown)));
        assert!(!is_leader_change(&driver(Code::UnknownTopicOrPartition)));
        assert!(!is_leader_change(&KafkaError::config("bad")));
    }

    #[test]
    fn a_leader_change_is_retried_until_it_settles() {
        let mut calls = 0;
        let result = retry_leader_change(|| {
            calls += 1;
            if calls < 2 {
                Err(driver(Code::NotLeaderForPartition))
            } else {
                Ok(calls)
            }
        });
        assert_eq!(result.unwrap(), 2);
    }

    #[test]
    fn a_timeout_is_not_retried_as_a_leader_change() {
        let mut calls = 0;
        let result: KafkaResult<()> = retry_leader_change(|| {
            calls += 1;
            Err(driver(Code::RequestTimedOut))
        });
        assert!(result.is_err());
        assert_eq!(calls, 1);
    }

    #[test]
    fn backoff_doubles_from_half_a_second() {
        assert_eq!(backoff(0), Duration::from_millis(500));
        assert_eq!(backoff(1), Duration::from_secs(1));
        assert_eq!(backoff(2), Duration::from_secs(2));
    }

    #[test]
    fn a_non_retriable_error_is_returned_at_once() {
        let mut calls = 0;
        let result: KafkaResult<()> = retry_metadata(|| {
            calls += 1;
            Err(KafkaError::config("bad"))
        });
        assert!(result.is_err());
        assert_eq!(calls, 1);
    }

    #[test]
    fn success_is_returned_at_once() {
        let mut calls = 0;
        let result = retry_metadata(|| {
            calls += 1;
            Ok(calls)
        });
        assert_eq!(result.unwrap(), 1);
    }
}
