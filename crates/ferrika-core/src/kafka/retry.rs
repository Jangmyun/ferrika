//! Retrying metadata calls that fail for a moment right after a change, such
//! as `NotLeaderForPartition` once a topic was created.

use std::time::Duration;

use crate::error::{ErrorClass, KafkaResult};

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::KafkaError;

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
