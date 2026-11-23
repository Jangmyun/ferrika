//! A RabbitMQ management API in memory, over real HTTP on `127.0.0.1`, for
//! tests of code that talks to [`crate::client::ManagementClient`] (feature
//! `test-util`; never in a shipped binary).
//!
//! It covers the topology part of the API: queues, exchanges and bindings
//! (list, read, declare, delete, purge, bind, unbind) with the broker's
//! rules that matter to callers: a re-declaration with other settings is
//! refused, deleting a queue or an exchange removes its bindings,
//! `if-empty` / `if-unused` are honoured, and a binding is deleted by its
//! `properties_key`. Every request is logged.
//!
//! ```no_run
//! # async fn run() {
//! use ferrika_rabbitmq::fake::FakeManagement;
//!
//! let fake = FakeManagement::start().await;
//! fake.with(|broker| broker.add_queue("/", "orders", 3));
//! let queues = fake.client().queues("/").await.unwrap();
//! assert_eq!(queues.len(), 1);
//! # }
//! ```

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use serde_json::{json, Map, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::client::ManagementClient;
use crate::model::{Binding, Exchange, Queue};

/// The broker's state.
#[derive(Debug, Default)]
pub struct Broker {
    /// By `(vhost, name)`.
    pub queues: BTreeMap<(String, String), Queue>,
    /// Ready messages by `(vhost, queue)`.
    pub messages: BTreeMap<(String, String), u64>,
    /// By `(vhost, name)`.
    pub exchanges: BTreeMap<(String, String), Exchange>,
    pub bindings: Vec<Binding>,
    /// `METHOD path?query` of every request, in order.
    pub requests: Vec<String>,
    /// Requests whose `METHOD path` starts with the key are answered with
    /// the status (once each).
    pub failures: Vec<(String, u16)>,
}

impl Broker {
    /// A classic, durable queue with `messages` ready messages.
    pub fn add_queue(&mut self, vhost: &str, name: &str, messages: u64) {
        let queue = queue_json(vhost, name, "classic", true, false, Map::new());
        if let Ok(queue) = serde_json::from_value(queue) {
            self.queues.insert(key(vhost, name), queue);
            self.messages.insert(key(vhost, name), messages);
        }
    }

    /// A durable exchange of `kind`.
    pub fn add_exchange(&mut self, vhost: &str, name: &str, kind: &str) {
        let exchange = json!({
            "name": name, "vhost": vhost, "type": kind, "durable": true,
            "auto_delete": false, "internal": false, "arguments": {}
        });
        if let Ok(exchange) = serde_json::from_value(exchange) {
            self.exchanges.insert(key(vhost, name), exchange);
        }
    }

    /// A binding from exchange `source` to a queue or exchange.
    pub fn add_binding(
        &mut self,
        vhost: &str,
        source: &str,
        destination_type: &str,
        destination: &str,
        routing_key: &str,
    ) {
        self.bindings.push(binding(
            vhost,
            source,
            destination_type,
            destination,
            routing_key,
            Map::new(),
        ));
    }

    /// Whether a binding with these ends and routing key exists.
    pub fn has_binding(&self, source: &str, destination: &str, routing_key: &str) -> bool {
        self.bindings.iter().any(|b| {
            b.source == source && b.destination == destination && b.routing_key == routing_key
        })
    }

    /// Ready messages of a queue (`None`: no such queue).
    pub fn messages_of(&self, vhost: &str, queue: &str) -> Option<u64> {
        self.queues
            .contains_key(&key(vhost, queue))
            .then(|| self.messages.get(&key(vhost, queue)).copied().unwrap_or(0))
    }

    /// The requests that change something (not `GET`).
    pub fn writes(&self) -> Vec<String> {
        self.requests
            .iter()
            .filter(|r| !r.starts_with("GET "))
            .cloned()
            .collect()
    }
}

/// The fake API, serving until dropped with the test's runtime.
#[derive(Clone)]
pub struct FakeManagement {
    url: String,
    state: Arc<Mutex<Broker>>,
}

impl FakeManagement {
    /// Listens on a free port of `127.0.0.1`.
    pub async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap_or_else(|e| panic!("cannot listen: {e}"));
        let url = match listener.local_addr() {
            Ok(addr) => format!("http://{addr}"),
            Err(e) => panic!("no local address: {e}"),
        };
        let state = Arc::new(Mutex::new(Broker::default()));
        let shared = state.clone();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let state = shared.clone();
                tokio::spawn(async move { serve(stream, state).await });
            }
        });
        Self { url, state }
    }

    /// `http://127.0.0.1:PORT`.
    pub fn url(&self) -> String {
        self.url.clone()
    }

    /// A client for the fake.
    pub fn client(&self) -> ManagementClient {
        ManagementClient::new(&self.url)
            .unwrap_or_else(|e| panic!("bad fake URL: {e}"))
            .with_credentials("guest", "guest")
    }

    /// Runs `f` on the broker's state.
    pub fn with<R>(&self, f: impl FnOnce(&mut Broker) -> R) -> R {
        let mut broker = self.state.lock().unwrap_or_else(|e| e.into_inner());
        f(&mut broker)
    }
}

fn key(vhost: &str, name: &str) -> (String, String) {
    (vhost.to_string(), name.to_string())
}

fn queue_json(
    vhost: &str,
    name: &str,
    kind: &str,
    durable: bool,
    auto_delete: bool,
    arguments: Map<String, Value>,
) -> Value {
    json!({
        "name": name, "vhost": vhost, "type": kind, "durable": durable,
        "auto_delete": auto_delete, "exclusive": false, "state": "running",
        "consumers": 0, "arguments": arguments
    })
}

fn binding(
    vhost: &str,
    source: &str,
    destination_type: &str,
    destination: &str,
    routing_key: &str,
    arguments: Map<String, Value>,
) -> Binding {
    let properties_key = match (routing_key.is_empty(), arguments.is_empty()) {
        (true, true) => "~".to_string(),
        (false, true) => routing_key.to_string(),
        (_, false) => format!("{routing_key}~{}", arguments.len()),
    };
    Binding {
        source: source.to_string(),
        vhost: vhost.to_string(),
        destination: destination.to_string(),
        destination_type: destination_type.to_string(),
        routing_key: routing_key.to_string(),
        arguments,
        properties_key,
    }
}

/// Reads one request, answers it and closes.
async fn serve(mut stream: TcpStream, state: Arc<Mutex<Broker>>) {
    let mut raw = Vec::new();
    let mut buf = [0u8; 4096];
    let (head_end, length) = loop {
        let Ok(n) = stream.read(&mut buf).await else {
            return;
        };
        if n == 0 {
            return;
        }
        raw.extend_from_slice(&buf[..n]);
        if let Some(end) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
            let head = String::from_utf8_lossy(&raw[..end]).to_ascii_lowercase();
            let length = head
                .lines()
                .find_map(|l| l.strip_prefix("content-length:"))
                .and_then(|v| v.trim().parse::<usize>().ok())
                .unwrap_or(0);
            break (end + 4, length);
        }
    };
    while raw.len() < head_end + length {
        match stream.read(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(n) => raw.extend_from_slice(&buf[..n]),
        }
    }
    let head = String::from_utf8_lossy(&raw[..head_end]).into_owned();
    let body = String::from_utf8_lossy(&raw[head_end..]).into_owned();
    let mut words = head.split_whitespace();
    let method = words.next().unwrap_or_default().to_string();
    let target = words.next().unwrap_or_default().to_string();
    let accept = head
        .lines()
        .find_map(|l| {
            l.split_once(':')
                .filter(|(name, _)| name.trim().eq_ignore_ascii_case("accept"))
                .map(|(_, value)| value.trim().to_string())
        })
        .unwrap_or_default();
    let (status, answer) = {
        let mut broker = state.lock().unwrap_or_else(|e| e.into_inner());
        // Like the real API: the purge resource offers no JSON.
        if target.ends_with("/contents") && !accept.contains("*/*") {
            broker.requests.push(format!("{method} {target}"));
            (406, None)
        } else {
            handle(&mut broker, &method, &target, &body)
        }
    };
    let text = answer.map(|v| v.to_string()).unwrap_or_default();
    let response = format!(
        "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\
         Connection: close\r\n\r\n{text}",
        text.len()
    );
    let _ = stream.write_all(response.as_bytes()).await;
    let _ = stream.shutdown().await;
}

fn decode(segment: &str) -> String {
    let bytes = segment.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            if let Some(v) = segment
                .get(i + 1..i + 3)
                .and_then(|h| u8::from_str_radix(h, 16).ok())
            {
                out.push(v);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn not_found() -> (u16, Option<Value>) {
    (
        404,
        Some(json!({"error": "Object Not Found", "reason": "Not Found"})),
    )
}

fn bad_request(reason: &str) -> (u16, Option<Value>) {
    (400, Some(json!({"error": "bad_request", "reason": reason})))
}

fn to_value<T: serde::Serialize>(value: &T) -> Option<Value> {
    serde_json::to_value(value).ok()
}

fn handle(broker: &mut Broker, method: &str, target: &str, body: &str) -> (u16, Option<Value>) {
    broker.requests.push(format!("{method} {target}"));
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    let line = format!("{method} {path}");
    if let Some(ix) = broker
        .failures
        .iter()
        .position(|(prefix, _)| line.starts_with(prefix.as_str()))
    {
        let (_, status) = broker.failures.remove(ix);
        return (
            status,
            Some(json!({"error": "injected", "reason": "injected failure"})),
        );
    }
    let flag = |name: &str| query.split('&').any(|p| p == format!("{name}=true"));
    let request: Value = serde_json::from_str(body).unwrap_or(Value::Null);
    let arguments = request
        .get("arguments")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let parts: Vec<String> = path
        .trim_start_matches("/api/")
        .split('/')
        .map(decode)
        .collect();
    let parts: Vec<&str> = parts.iter().map(String::as_str).collect();
    match (method, parts.as_slice()) {
        ("GET", ["queues", vhost]) => {
            let list: Vec<Queue> = broker
                .queues
                .iter()
                .filter(|((v, _), _)| v == vhost)
                .map(|(k, q)| with_messages(q, broker.messages.get(k)))
                .collect();
            (200, to_value(&list))
        }
        ("GET", ["queues", vhost, name]) => match broker.queues.get(&key(vhost, name)) {
            Some(q) => (
                200,
                to_value(&with_messages(q, broker.messages.get(&key(vhost, name)))),
            ),
            None => not_found(),
        },
        ("PUT", ["queues", vhost, name]) => {
            let mut arguments = arguments;
            let kind = arguments
                .remove("x-queue-type")
                .and_then(|v| v.as_str().map(str::to_string))
                .unwrap_or_else(|| "classic".to_string());
            if kind != "classic" {
                arguments.insert("x-queue-type".into(), Value::from(kind.clone()));
            }
            let durable = request["durable"].as_bool().unwrap_or(false);
            let auto_delete = request["auto_delete"].as_bool().unwrap_or(false);
            let wanted = queue_json(vhost, name, &kind, durable, auto_delete, arguments);
            let Ok(wanted) = serde_json::from_value::<Queue>(wanted) else {
                return bad_request("bad queue");
            };
            match broker.queues.get(&key(vhost, name)) {
                Some(existing)
                    if existing.kind == wanted.kind
                        && existing.durable == wanted.durable
                        && existing.auto_delete == wanted.auto_delete
                        && existing.arguments == wanted.arguments =>
                {
                    (204, None)
                }
                Some(_) => bad_request("PRECONDITION_FAILED - inequivalent arg"),
                None => {
                    broker.queues.insert(key(vhost, name), wanted);
                    broker.messages.insert(key(vhost, name), 0);
                    (201, None)
                }
            }
        }
        ("DELETE", ["queues", vhost, name]) => {
            let Some(queue) = broker.queues.get(&key(vhost, name)) else {
                return not_found();
            };
            // Like RabbitMQ 4: quorum and stream queues take neither flag.
            if queue.kind != "classic" && (flag("if-empty") || flag("if-unused")) {
                return bad_request("if-empty / if-unused are not supported by quorum queues");
            }
            if flag("if-empty") && broker.messages.get(&key(vhost, name)).copied().unwrap_or(0) > 0
            {
                return bad_request("PRECONDITION_FAILED - queue not empty");
            }
            broker.queues.remove(&key(vhost, name));
            broker.messages.remove(&key(vhost, name));
            broker.bindings.retain(|b| {
                !(b.vhost == *vhost && b.destination_type == "queue" && b.destination == *name)
            });
            (204, None)
        }
        ("DELETE", ["queues", vhost, name, "contents"]) => {
            if !broker.queues.contains_key(&key(vhost, name)) {
                return not_found();
            }
            broker.messages.insert(key(vhost, name), 0);
            (204, None)
        }
        ("GET", ["queues", vhost, name, "bindings"]) => {
            if !broker.queues.contains_key(&key(vhost, name)) {
                return not_found();
            }
            let mut list = vec![binding(vhost, "", "queue", name, name, Map::new())];
            list.extend(
                broker
                    .bindings
                    .iter()
                    .filter(|b| {
                        b.vhost == *vhost && b.destination_type == "queue" && b.destination == *name
                    })
                    .cloned(),
            );
            (200, to_value(&list))
        }
        ("GET", ["exchanges", vhost]) => {
            let list: Vec<&Exchange> = broker
                .exchanges
                .iter()
                .filter(|((v, _), _)| v == vhost)
                .map(|(_, e)| e)
                .collect();
            (200, to_value(&list))
        }
        ("GET", ["exchanges", vhost, name]) => match broker.exchanges.get(&key(vhost, name)) {
            Some(e) => (200, to_value(e)),
            None => not_found(),
        },
        ("PUT", ["exchanges", vhost, name]) => {
            let wanted = json!({
                "name": name, "vhost": vhost, "type": request["type"].as_str().unwrap_or("direct"),
                "durable": request["durable"].as_bool().unwrap_or(false),
                "auto_delete": request["auto_delete"].as_bool().unwrap_or(false),
                "internal": request["internal"].as_bool().unwrap_or(false),
                "arguments": arguments,
            });
            let Ok(wanted) = serde_json::from_value::<Exchange>(wanted) else {
                return bad_request("bad exchange");
            };
            match broker.exchanges.get(&key(vhost, name)) {
                Some(existing) if *existing == wanted => (204, None),
                Some(_) => bad_request("PRECONDITION_FAILED - inequivalent arg"),
                None => {
                    broker.exchanges.insert(key(vhost, name), wanted);
                    (201, None)
                }
            }
        }
        ("DELETE", ["exchanges", vhost, name]) => {
            if !broker.exchanges.contains_key(&key(vhost, name)) {
                return not_found();
            }
            if flag("if-unused")
                && broker
                    .bindings
                    .iter()
                    .any(|b| b.vhost == *vhost && b.source == *name)
            {
                return bad_request("PRECONDITION_FAILED - exchange in use");
            }
            broker.exchanges.remove(&key(vhost, name));
            broker.bindings.retain(|b| {
                !(b.vhost == *vhost
                    && (b.source == *name
                        || (b.destination_type == "exchange" && b.destination == *name)))
            });
            (204, None)
        }
        ("GET", ["exchanges", vhost, name, "bindings", side]) => {
            if !broker.exchanges.contains_key(&key(vhost, name)) {
                return not_found();
            }
            let list: Vec<&Binding> = broker
                .bindings
                .iter()
                .filter(|b| {
                    b.vhost == *vhost
                        && match *side {
                            "source" => b.source == *name,
                            _ => b.destination_type == "exchange" && b.destination == *name,
                        }
                })
                .collect();
            (200, to_value(&list))
        }
        ("GET", ["bindings", vhost]) => {
            let list: Vec<&Binding> = broker
                .bindings
                .iter()
                .filter(|b| b.vhost == *vhost)
                .collect();
            (200, to_value(&list))
        }
        (_, ["bindings", vhost, "e", source, letter, destination, rest @ ..]) => {
            let destination_type = if *letter == "q" { "queue" } else { "exchange" };
            let between = |b: &&Binding| {
                b.vhost == *vhost
                    && b.source == *source
                    && b.destination_type == destination_type
                    && b.destination == *destination
            };
            match (method, rest) {
                ("GET", []) => {
                    let list: Vec<&Binding> = broker.bindings.iter().filter(between).collect();
                    (200, to_value(&list))
                }
                ("POST", []) => {
                    let destination_exists = if destination_type == "queue" {
                        broker.queues.contains_key(&key(vhost, destination))
                    } else {
                        broker.exchanges.contains_key(&key(vhost, destination))
                    };
                    if !broker.exchanges.contains_key(&key(vhost, source)) || !destination_exists {
                        return not_found();
                    }
                    let routing_key = request["routing_key"].as_str().unwrap_or_default();
                    let new = binding(
                        vhost,
                        source,
                        destination_type,
                        destination,
                        routing_key,
                        arguments,
                    );
                    if !broker.bindings.contains(&new) {
                        broker.bindings.push(new);
                    }
                    (201, None)
                }
                ("DELETE", [props]) => {
                    let before = broker.bindings.len();
                    broker
                        .bindings
                        .retain(|b| !(between(&b) && b.properties_key == *props));
                    if broker.bindings.len() == before {
                        not_found()
                    } else {
                        (204, None)
                    }
                }
                _ => not_found(),
            }
        }
        _ => not_found(),
    }
}

/// `queue` with its message counts as the API reports them.
fn with_messages(queue: &Queue, messages: Option<&u64>) -> Queue {
    let count = messages.copied().unwrap_or(0);
    Queue {
        messages: Some(count),
        messages_ready: Some(count),
        messages_unacknowledged: Some(0),
        ..queue.clone()
    }
}
