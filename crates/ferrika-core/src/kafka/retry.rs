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
///
/// Blocking: sleeps the calling thread between tries (up to 3.5 s in all,
/// on top of the tries themselves). From async code, call it inside
/// `tokio::task::spawn_blocking`.
pub fn retry_metadata<T>(op: impl FnMut() -> KafkaResult<T>) -> KafkaResult<T> {
    retry_blocking(op, |e| e.class() == ErrorClass::Retriable)
}

/// The retry policy shared by the blocking and the async executors: after
/// try `attempt` (0-based) failed with `error`, how long to wait before the
/// next try, or `None` to return the error (not `retriable`, or the last of
/// [`TRIES`]).
fn next_delay(
    attempt: u32,
    error: &KafkaError,
    retriable: impl Fn(&KafkaError) -> bool,
) -> Option<Duration> {
    (attempt + 1 < TRIES && retriable(error)).then(|| backoff(attempt))
}

/// The blocking executor of [`next_delay`]: sleeps with `std::thread::sleep`.
fn retry_blocking<T>(
    mut op: impl FnMut() -> KafkaResult<T>,
    retriable: impl Fn(&KafkaError) -> bool,
) -> KafkaResult<T> {
    let mut attempt = 0;
    loop {
        match op() {
            Err(e) => match next_delay(attempt, &e, &retriable) {
                Some(delay) => {
                    tracing::debug!(%e, ?delay, "retrying after a transient metadata error");
                    std::thread::sleep(delay);
                    attempt += 1;
                }
                None => return Err(e),
            },
            ok => return ok,
        }
    }
}

/// The async executor of [`next_delay`]: waits with `tokio::time::sleep`,
/// so the runtime thread is free between tries.
async fn retry_async<T, F, Fut>(
    mut op: F,
    retriable: impl Fn(&KafkaError) -> bool,
) -> KafkaResult<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = KafkaResult<T>>,
{
    let mut attempt = 0;
    loop {
        match op().await {
            Err(e) => match next_delay(attempt, &e, &retriable) {
                Some(delay) => {
                    tracing::debug!(%e, ?delay, "retrying after a transient metadata error");
                    tokio::time::sleep(delay).await;
                    attempt += 1;
                }
                None => return Err(e),
            },
            ok => return ok,
        }
    }
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
///
/// Blocking: while a partition leader moves, this sleeps the calling thread
/// for up to 3.5 s (0.5 + 1 + 2 s) besides the tries themselves. Call it from
/// a blocking context (a plain thread, or `tokio::task::spawn_blocking` in
/// async code), or use [`retry_leader_change_async`].
pub fn retry_leader_change<T>(op: impl FnMut() -> KafkaResult<T>) -> KafkaResult<T> {
    retry_blocking(op, is_leader_change)
}

/// [`retry_leader_change`] for async code: the same errors, tries and
/// backoff, but it waits with `tokio::time::sleep` (it must run inside a
/// tokio runtime with the time driver enabled). `op` itself runs on the
/// calling task; a blocking call inside it still holds the thread for that
/// one try.
pub async fn retry_leader_change_async<T, F, Fut>(op: F) -> KafkaResult<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = KafkaResult<T>>,
{
    retry_async(op, is_leader_change).await
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
    fn the_policy_retries_leader_changes_up_to_the_last_try() {
        let leader = driver(Code::NotLeaderForPartition);
        let timeout = driver(Code::RequestTimedOut);
        assert_eq!(
            next_delay(0, &leader, is_leader_change),
            Some(Duration::from_millis(500))
        );
        assert_eq!(
            next_delay(TRIES - 2, &leader, is_leader_change),
            Some(backoff(TRIES - 2))
        );
        assert_eq!(next_delay(TRIES - 1, &leader, is_leader_change), None);
        assert_eq!(next_delay(0, &timeout, is_leader_change), None);
    }

    #[test]
    fn a_lasting_leader_change_gives_up_after_all_tries() {
        let mut calls = 0;
        let result: KafkaResult<()> = retry_leader_change(|| {
            calls += 1;
            Err(driver(Code::LeaderNotAvailable))
        });
        assert!(result.is_err());
        assert_eq!(calls, TRIES);
    }

    #[tokio::test(start_paused = true)]
    async fn the_async_executor_waits_the_same_backoff_without_blocking() {
        let start = tokio::time::Instant::now();
        let mut calls = 0;
        let result = retry_leader_change_async(|| {
            calls += 1;
            let n = calls;
            async move {
                if n < 3 {
                    Err(driver(Code::NotLeaderForPartition))
                } else {
                    Ok(n)
                }
            }
        })
        .await;
        assert_eq!(result.unwrap(), 3);
        // 0.5 s + 1 s of paused (virtual) time.
        assert_eq!(start.elapsed(), Duration::from_millis(1500));

        let mut calls = 0;
        let result: KafkaResult<()> = retry_leader_change_async(|| {
            calls += 1;
            async { Err(driver(Code::RequestTimedOut)) }
        })
        .await;
        assert!(result.is_err());
        assert_eq!(calls, 1);

        let mut calls = 0;
        let result: KafkaResult<()> = retry_leader_change_async(|| {
            calls += 1;
            async { Err(driver(Code::OffsetNotAvailable)) }
        })
        .await;
        assert!(result.is_err());
        assert_eq!(calls, TRIES);
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
