//! Drops a client on a background thread.
//!
//! rdkafka's `AdminClient` and `ThreadedProducer` each run a polling thread
//! that waits up to 100 ms per round, and their `Drop` joins it. So every
//! short command paid up to 100 ms at exit for nothing: `topics list` took
//! 120 ms, of which about 95 ms was this wait. A consumer's `rd_kafka_destroy`
//! costs about 100 ms the same way (`dump -n 1`). Handing the client to a
//! throwaway thread lets the command return at once; the thread still
//! shuts the client down in order, unless the process exits first. That is
//! only safe for a client with nothing left to flush, commit or leave.

use std::ops::Deref;
use std::sync::atomic::{AtomicBool, Ordering};

/// Owns `T` and drops it on a new thread instead of the caller's, unless
/// told to drop it in place.
pub struct DetachedDrop<T: Send + 'static> {
    inner: Option<T>,
    detach: AtomicBool,
}

impl<T: Send + 'static> DetachedDrop<T> {
    /// Drops `inner` on another thread.
    pub fn new(inner: T) -> Self {
        Self {
            inner: Some(inner),
            detach: AtomicBool::new(true),
        }
    }

    /// Drops `inner` in place until [`Self::set_detach`] says otherwise.
    pub fn in_place(inner: T) -> Self {
        let held = Self::new(inner);
        held.set_detach(false);
        held
    }

    /// Chooses where `inner` is dropped.
    pub fn set_detach(&self, detach: bool) {
        self.detach.store(detach, Ordering::Relaxed);
    }
}

impl<T: Send + 'static> Deref for DetachedDrop<T> {
    type Target = T;

    fn deref(&self) -> &T {
        // `None` only while dropping.
        self.inner.as_ref().expect("value is present until dropped")
    }
}

impl<T: Send + 'static> Drop for DetachedDrop<T> {
    fn drop(&mut self) {
        let Some(inner) = self.inner.take() else {
            return;
        };
        if !self.detach.load(Ordering::Relaxed) {
            drop(inner);
            return;
        }
        let worker = std::thread::Builder::new()
            .name("client shutdown".into())
            .spawn(move || drop(inner));
        // If no thread can be started, the value was moved into the closure
        // and is dropped with it, here, as before.
        drop(worker);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use std::time::{Duration, Instant};

    struct Slow(mpsc::Sender<()>);

    impl Drop for Slow {
        fn drop(&mut self) {
            std::thread::sleep(Duration::from_millis(200));
            let _ = self.0.send(());
        }
    }

    #[test]
    fn in_place_values_are_dropped_by_the_caller() {
        let (tx, rx) = mpsc::channel();
        let held = DetachedDrop::in_place(Slow(tx));
        let started = Instant::now();
        drop(held);
        assert!(started.elapsed() >= Duration::from_millis(200));
        assert!(rx.try_recv().is_ok());
    }

    #[test]
    fn dropping_returns_before_the_slow_drop_ends_but_it_still_runs() {
        let (tx, rx) = mpsc::channel();
        let held = DetachedDrop::new(Slow(tx));
        let started = Instant::now();
        drop(held);
        assert!(started.elapsed() < Duration::from_millis(100));
        assert!(rx.recv_timeout(Duration::from_secs(5)).is_ok());
    }
}
