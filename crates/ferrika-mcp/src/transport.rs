//! Bidirectional stdio: a reader task that sorts incoming lines, a writer
//! task that owns stdout, and the bookkeeping for requests the server sends
//! to the client (elicitation) and then waits on.
//!
//! Client requests are still handled one at a time, in order. While one of
//! them waits for the client's answer, `ping` is answered by the reader and
//! everything else queues behind it.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::sync::{mpsc, oneshot, Notify};

use super::protocol::{self, Incoming};
use ferrika_core::error::{KafkaError, KafkaResult};

/// Most cancelled request ids remembered; a client that cancels requests
/// the server already answered would otherwise grow the set forever.
const CANCELLED_MAX: usize = 1024;

/// A client request waiting in the queue: id, method, params.
pub(super) type QueuedRequest = (Value, String, Value);

/// Why a request to the client got no answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PeerError {
    /// stdin closed: the client is gone.
    Disconnected,
    /// The client cancelled the request this one was made for.
    Cancelled,
}

/// The client end of the connection: sends messages and matches the
/// client's responses to the server's own requests.
#[derive(Clone)]
pub(super) struct Peer {
    inner: Arc<PeerInner>,
}

struct PeerInner {
    out: mpsc::UnboundedSender<Value>,
    /// Server request id → where its response goes.
    pending: Mutex<HashMap<String, oneshot::Sender<Value>>>,
    /// Client request ids (as JSON text) named by `notifications/cancelled`.
    cancelled: Mutex<HashSet<String>>,
    closed: AtomicBool,
    /// Woken on a cancellation and when the client disconnects.
    changed: Notify,
    next_id: AtomicU64,
}

impl Peer {
    pub(super) fn new(out: mpsc::UnboundedSender<Value>) -> Self {
        Self {
            inner: Arc::new(PeerInner {
                out,
                pending: Mutex::default(),
                cancelled: Mutex::default(),
                closed: AtomicBool::new(false),
                changed: Notify::new(),
                next_id: AtomicU64::new(1),
            }),
        }
    }

    /// Queues one message for stdout. A writer that has stopped (stdout
    /// closed) drops it.
    pub(super) fn send(&self, message: Value) {
        let _ = self.inner.out.send(message);
    }

    /// Sends a request to the client on behalf of the client request
    /// `for_request` and waits for the response message (`result` or
    /// `error`). Fails when the client disconnects or cancels `for_request`.
    pub(super) async fn request(
        &self,
        method: &str,
        params: Value,
        for_request: &Value,
    ) -> Result<Value, PeerError> {
        let n = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        let id = format!("ferrika-{n}");
        let (tx, mut rx) = oneshot::channel();
        lock(&self.inner.pending).insert(id.clone(), tx);
        let _forget = Forget {
            peer: self,
            id: id.clone(),
        };
        self.send(json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params }));
        loop {
            let changed = self.inner.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.is_cancelled(for_request) {
                return Err(PeerError::Cancelled);
            }
            if self.inner.closed.load(Ordering::SeqCst) {
                return Err(PeerError::Disconnected);
            }
            tokio::select! {
                response = &mut rx => return response.map_err(|_| PeerError::Disconnected),
                _ = &mut changed => {}
            }
        }
    }

    /// Hands a client response to the request waiting for it; responses
    /// nobody waits for (late, or unknown ids) are dropped.
    pub(super) fn resolve(&self, message: Value) {
        let Some(id) = message.get("id").and_then(Value::as_str) else {
            return;
        };
        if let Some(tx) = lock(&self.inner.pending).remove(id) {
            let _ = tx.send(message);
        }
    }

    /// Records `notifications/cancelled` for the client request `id`.
    pub(super) fn cancel(&self, id: &Value) {
        let mut cancelled = lock(&self.inner.cancelled);
        if cancelled.len() >= CANCELLED_MAX {
            cancelled.clear();
        }
        cancelled.insert(id.to_string());
        drop(cancelled);
        self.inner.changed.notify_waiters();
    }

    pub(super) fn is_cancelled(&self, id: &Value) -> bool {
        lock(&self.inner.cancelled).contains(&id.to_string())
    }

    /// Whether the client cancelled `id`, forgetting the cancellation.
    pub(super) fn take_cancelled(&self, id: &Value) -> bool {
        lock(&self.inner.cancelled).remove(&id.to_string())
    }

    /// stdin closed: every waiting request fails, and later ones fail at once.
    pub(super) fn close(&self) {
        self.inner.closed.store(true, Ordering::SeqCst);
        lock(&self.inner.pending).clear();
        self.inner.changed.notify_waiters();
    }
}

/// Removes a server request from `pending` however its wait ends.
struct Forget<'a> {
    peer: &'a Peer,
    id: String,
}

impl Drop for Forget<'_> {
    fn drop(&mut self) {
        lock(&self.peer.inner.pending).remove(&self.id);
    }
}

/// A poisoned lock still holds consistent data here (plain inserts and
/// removes), so it is used as is.
fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Sorts stdin lines until EOF: requests go to the queue (`ping` is answered
/// here), client responses to [`Peer::resolve`], cancellations to
/// [`Peer::cancel`]. Closes the peer at the end.
pub(super) async fn read_loop<R>(
    reader: R,
    peer: Peer,
    requests: mpsc::UnboundedSender<QueuedRequest>,
) -> KafkaResult<()>
where
    R: AsyncRead + Unpin,
{
    let mut lines = BufReader::new(reader).lines();
    let result = loop {
        let line = match lines.next_line().await {
            Ok(Some(line)) => line,
            Ok(None) => break Ok(()),
            Err(e) => break Err(KafkaError::Io(e.to_string())),
        };
        if line.trim().is_empty() {
            continue;
        }
        match protocol::classify(&line) {
            Incoming::Reply(reply) => peer.send(reply),
            Incoming::Request { id, method, .. } if method == "ping" => {
                peer.send(protocol::rpc_result(id, json!({})));
            }
            Incoming::Request { id, method, params } => {
                if requests.send((id, method, params)).is_err() {
                    break Ok(());
                }
            }
            Incoming::Notification { method, params } => {
                if method == "notifications/cancelled" {
                    if let Some(id) = params.get("requestId") {
                        peer.cancel(id);
                    }
                }
            }
            Incoming::Response(message) => peer.resolve(message),
        }
    };
    peer.close();
    result
}

/// Writes each queued message as one line, flushing after each.
pub(super) async fn write_loop<W>(
    mut writer: W,
    mut messages: mpsc::UnboundedReceiver<Value>,
) -> KafkaResult<()>
where
    W: AsyncWrite + Unpin,
{
    while let Some(message) = messages.recv().await {
        writer
            .write_all(format!("{message}\n").as_bytes())
            .await
            .map_err(|e| KafkaError::Io(e.to_string()))?;
        writer
            .flush()
            .await
            .map_err(|e| KafkaError::Io(e.to_string()))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn request_gets_the_matching_response() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let peer = Peer::new(tx);
        let client = peer.clone();
        let answer = tokio::spawn(async move {
            let sent = rx.recv().await.unwrap();
            assert_eq!(sent["method"], "elicitation/create");
            let id = sent["id"].clone();
            assert!(id.as_str().unwrap().starts_with("ferrika-"));
            // An unknown id is ignored.
            client.resolve(json!({ "jsonrpc": "2.0", "id": "ferrika-999", "result": {} }));
            client.resolve(json!({ "jsonrpc": "2.0", "id": id, "result": { "action": "accept" } }));
        });
        let response = peer
            .request("elicitation/create", json!({}), &json!(7))
            .await
            .unwrap();
        assert_eq!(response["result"]["action"], "accept");
        answer.await.unwrap();
        assert!(lock(&peer.inner.pending).is_empty());
    }

    #[tokio::test]
    async fn cancellation_and_disconnect_end_the_wait() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let peer = Peer::new(tx);
        let other = peer.clone();
        let cancel = tokio::spawn(async move {
            tokio::task::yield_now().await;
            other.cancel(&json!(7));
        });
        let result = peer
            .request("elicitation/create", json!({}), &json!(7))
            .await;
        assert_eq!(result, Err(PeerError::Cancelled));
        cancel.await.unwrap();
        assert!(peer.take_cancelled(&json!(7)));
        assert!(!peer.is_cancelled(&json!(7)));

        let other = peer.clone();
        let close = tokio::spawn(async move {
            tokio::task::yield_now().await;
            other.close();
        });
        let result = peer
            .request("elicitation/create", json!({}), &json!(8))
            .await;
        assert_eq!(result, Err(PeerError::Disconnected));
        close.await.unwrap();
        // After the close, requests fail at once.
        let result = peer
            .request("elicitation/create", json!({}), &json!(9))
            .await;
        assert_eq!(result, Err(PeerError::Disconnected));
    }
}
