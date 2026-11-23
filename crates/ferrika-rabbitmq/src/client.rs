//! A client for the RabbitMQ management HTTP API (the `rabbitmq_management`
//! plugin), over the same small HTTP/1.1 client Ferrika uses for the Schema
//! Registry and Kafka Connect: `http://` and `https://` (OS trust store),
//! basic authentication, one request per connection.

use std::time::Duration;

use serde::de::DeserializeOwned;

use ferrika_core::error::KafkaError;
use ferrika_core::kafka::http::{Endpoint, Response};

use crate::error::{RabbitError, RabbitResult};
use crate::model::{
    peek_request, Binding, BindingDeclaration, Exchange, ExchangeDeclaration, OutgoingMessage,
    PeekedMessage, PublishOutcome, Queue, QueueDeclaration, DEFAULT_EXCHANGE,
};

/// The management plugin's port.
pub const DEFAULT_PORT: u16 = 15672;

/// Talks to one RabbitMQ node's (or load balancer's) management API.
#[derive(Clone)]
pub struct ManagementClient {
    endpoint: Endpoint,
    /// `Basic ...` from [`Self::with_credentials`]; replaces credentials
    /// given in the URL.
    authorization: Option<String>,
}

// Manual so the credentials never reach logs or error output.
impl std::fmt::Debug for ManagementClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ManagementClient")
            .field("endpoint", &self.endpoint)
            .field(
                "authorization",
                &self.authorization.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

impl ManagementClient {
    /// `url` like `http://rabbit:15672`, `https://user:pass@rabbit` or
    /// `http://lb/rabbitmq` (a path prefix is kept). Without a port, 15672
    /// (443 for https).
    pub fn new(url: &str) -> RabbitResult<Self> {
        let endpoint = Endpoint::parse(url, DEFAULT_PORT).map_err(|e| match e {
            KafkaError::Config(message) => RabbitError::Config(message),
            other => RabbitError::Config(other.to_string()),
        })?;
        Ok(Self {
            endpoint,
            authorization: None,
        })
    }

    /// Authenticates as `user` (HTTP basic authentication).
    pub fn with_credentials(mut self, user: &str, password: &str) -> Self {
        let token = ferrika_core::codec::base64::encode(format!("{user}:{password}").as_bytes());
        self.authorization = Some(format!("Basic {token}"));
        self
    }

    /// Sets the time allowed to connect and, separately, for each exchange.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.endpoint = self.endpoint.with_timeout(timeout);
        self
    }

    /// `host:port` of the management API, without credentials: what the
    /// company policy's broker-address rules and the audit log see.
    pub fn address(&self) -> String {
        self.endpoint.target()
    }

    /// The queues of `vhost`.
    pub async fn queues(&self, vhost: &str) -> RabbitResult<Vec<Queue>> {
        self.get_json(&format!("/api/queues/{}", segment(vhost)))
            .await
    }

    /// The exchanges of `vhost`, the default one (empty name) included.
    pub async fn exchanges(&self, vhost: &str) -> RabbitResult<Vec<Exchange>> {
        self.get_json(&format!("/api/exchanges/{}", segment(vhost)))
            .await
    }

    /// The bindings of `vhost`.
    pub async fn bindings(&self, vhost: &str) -> RabbitResult<Vec<Binding>> {
        self.get_json(&format!("/api/bindings/{}", segment(vhost)))
            .await
    }

    /// Reads up to `count` messages from the head of `queue` and puts them
    /// back (`ack_requeue_true`). Not side-effect free: the broker marks
    /// them redelivered, requeued messages may change places with ones
    /// published meanwhile, and a quorum queue may count each peek as a
    /// delivery attempt towards its `delivery-limit`.
    pub async fn peek(
        &self,
        vhost: &str,
        queue: &str,
        count: u32,
    ) -> RabbitResult<Vec<PeekedMessage>> {
        let body = peek_request(count)?.to_string();
        let response = self
            .send("POST", &peek_path(vhost, queue), Some(&body))
            .await?;
        parse_json(&success_body(response)?)
    }

    /// Publishes `message` to `exchange` of `vhost` (`""` or
    /// [`DEFAULT_EXCHANGE`] for the default exchange).
    pub async fn publish(
        &self,
        vhost: &str,
        exchange: &str,
        message: &OutgoingMessage,
    ) -> RabbitResult<PublishOutcome> {
        let body = message.to_request().to_string();
        let response = self
            .send("POST", &publish_path(vhost, exchange), Some(&body))
            .await?;
        parse_json(&success_body(response)?)
    }

    /// One queue of `vhost`, `None` when there is no such queue (or vhost).
    pub async fn queue(&self, vhost: &str, name: &str) -> RabbitResult<Option<Queue>> {
        self.get_optional(&queue_path(vhost, name)).await
    }

    /// One exchange of `vhost`, `None` when there is no such exchange.
    pub async fn exchange(&self, vhost: &str, name: &str) -> RabbitResult<Option<Exchange>> {
        self.get_optional(&exchange_path(vhost, name)).await
    }

    /// The bindings that deliver to `queue`, without the default exchange's
    /// implicit one (which cannot be added or removed).
    pub async fn queue_bindings(&self, vhost: &str, queue: &str) -> RabbitResult<Vec<Binding>> {
        let bindings: Vec<Binding> = self
            .get_json(&format!("{}/bindings", queue_path(vhost, queue)))
            .await?;
        Ok(bindings
            .into_iter()
            .filter(|b| !b.source.is_empty())
            .collect())
    }

    /// The bindings `exchange` is the source or the destination of (an
    /// exchange bound to itself is listed once).
    pub async fn exchange_bindings(
        &self,
        vhost: &str,
        exchange: &str,
    ) -> RabbitResult<Vec<Binding>> {
        let base = exchange_path(vhost, exchange);
        let mut bindings: Vec<Binding> = self.get_json(&format!("{base}/bindings/source")).await?;
        let to_it: Vec<Binding> = self
            .get_json(&format!("{base}/bindings/destination"))
            .await?;
        for binding in to_it {
            if !bindings.contains(&binding) {
                bindings.push(binding);
            }
        }
        Ok(bindings)
    }

    /// The bindings from exchange `source` to the destination of `binding`
    /// (any routing key and arguments).
    pub async fn bindings_between(
        &self,
        vhost: &str,
        binding: &BindingDeclaration,
    ) -> RabbitResult<Vec<Binding>> {
        self.get_json(&binding_path(vhost, binding)).await
    }

    /// Declares queue `name` (`PUT /api/queues/{vhost}/{name}`). Declaring
    /// a queue that exists with the same settings changes nothing; with
    /// other settings the broker refuses (400, `PRECONDITION_FAILED`).
    pub async fn declare_queue(
        &self,
        vhost: &str,
        name: &str,
        declaration: &QueueDeclaration,
    ) -> RabbitResult<()> {
        let body = declaration.to_request().to_string();
        self.send_ok("PUT", &queue_path(vhost, name), Some(&body))
            .await
    }

    /// Deletes queue `name` and its messages. With `if_empty` / `if_unused`
    /// the broker refuses when the queue holds messages / has consumers.
    pub async fn delete_queue(
        &self,
        vhost: &str,
        name: &str,
        if_empty: bool,
        if_unused: bool,
    ) -> RabbitResult<()> {
        let path = format!(
            "{}{}",
            queue_path(vhost, name),
            conditions(&[("if-empty", if_empty), ("if-unused", if_unused)])
        );
        self.send_ok("DELETE", &path, None).await
    }

    /// Deletes every ready message of queue `name`, keeping the queue.
    pub async fn purge_queue(&self, vhost: &str, name: &str) -> RabbitResult<()> {
        self.send_ok(
            "DELETE",
            &format!("{}/contents", queue_path(vhost, name)),
            None,
        )
        .await
    }

    /// Declares exchange `name` (`PUT /api/exchanges/{vhost}/{name}`).
    pub async fn declare_exchange(
        &self,
        vhost: &str,
        name: &str,
        declaration: &ExchangeDeclaration,
    ) -> RabbitResult<()> {
        let body = declaration.to_request().to_string();
        self.send_ok("PUT", &exchange_path(vhost, name), Some(&body))
            .await
    }

    /// Deletes exchange `name`; the broker removes its bindings with it.
    /// With `if_unused` it refuses while the exchange is a binding's source.
    pub async fn delete_exchange(
        &self,
        vhost: &str,
        name: &str,
        if_unused: bool,
    ) -> RabbitResult<()> {
        let path = format!(
            "{}{}",
            exchange_path(vhost, name),
            conditions(&[("if-unused", if_unused)])
        );
        self.send_ok("DELETE", &path, None).await
    }

    /// Adds `binding` (adding one that exists changes nothing).
    pub async fn bind(&self, vhost: &str, binding: &BindingDeclaration) -> RabbitResult<()> {
        binding.validate()?;
        let body = binding.to_request().to_string();
        self.send_ok("POST", &binding_path(vhost, binding), Some(&body))
            .await
    }

    /// Removes `binding`. Returns `false` when there was no such binding
    /// (nothing changed).
    pub async fn unbind(&self, vhost: &str, binding: &BindingDeclaration) -> RabbitResult<bool> {
        binding.validate()?;
        let listed = self.bindings_between(vhost, binding).await?;
        let Some(found) = listed.iter().find(|b| binding.matches(b)) else {
            return Ok(false);
        };
        let path = format!(
            "{}/{}",
            binding_path(vhost, binding),
            segment(&found.properties_key)
        );
        self.send_ok("DELETE", &path, None).await?;
        Ok(true)
    }

    /// GETs `path`; a 404 is `None`.
    async fn get_optional<T: DeserializeOwned>(&self, path: &str) -> RabbitResult<Option<T>> {
        let response = self.send("GET", path, None).await?;
        if response.status == 404 {
            return Ok(None);
        }
        parse_json(&success_body(response)?).map(Some)
    }

    /// Sends a request whose answer has no body worth reading.
    async fn send_ok(&self, method: &str, path: &str, body: Option<&str>) -> RabbitResult<()> {
        let response = self.send(method, path, body).await?;
        success_body(response).map(|_| ())
    }

    async fn get_json<T: DeserializeOwned>(&self, path: &str) -> RabbitResult<T> {
        let response = self.send("GET", path, None).await?;
        parse_json(&success_body(response)?)
    }

    /// Sends one request and returns the response whatever its status.
    async fn send(&self, method: &str, path: &str, body: Option<&str>) -> RabbitResult<Response> {
        let headers: Vec<(&str, &str)> = self
            .authorization
            .as_deref()
            .map(|auth| ("Authorization", auth))
            .into_iter()
            .collect();
        // Some DELETE resources (a queue's `contents`, the purge) offer no
        // JSON and answer `Accept: application/json` with 406.
        let content_type = if method == "DELETE" && body.is_none() {
            "*/*"
        } else {
            "application/json"
        };
        self.endpoint
            .send_with_headers(method, path, content_type, body, &headers)
            .await
            .map_err(|e| RabbitError::Unreachable {
                target: self.address(),
                reason: match e {
                    KafkaError::Io(reason) | KafkaError::Config(reason) => reason,
                    other => other.to_string(),
                },
            })
    }
}

/// `/api/queues/{vhost}/{queue}`.
fn queue_path(vhost: &str, queue: &str) -> String {
    format!("/api/queues/{}/{}", segment(vhost), segment(queue))
}

/// `/api/exchanges/{vhost}/{exchange}`.
fn exchange_path(vhost: &str, exchange: &str) -> String {
    format!("/api/exchanges/{}/{}", segment(vhost), segment(exchange))
}

/// `/api/bindings/{vhost}/e/{source}/{q|e}/{destination}`.
fn binding_path(vhost: &str, binding: &BindingDeclaration) -> String {
    format!(
        "/api/bindings/{}/e/{}/{}/{}",
        segment(vhost),
        segment(&binding.source),
        binding.destination_type.path_letter(),
        segment(&binding.destination)
    )
}

/// `?if-empty=true&...` for the conditions that are set; empty for none.
fn conditions(flags: &[(&str, bool)]) -> String {
    let set: Vec<String> = flags
        .iter()
        .filter(|(_, on)| *on)
        .map(|(name, _)| format!("{name}=true"))
        .collect();
    if set.is_empty() {
        String::new()
    } else {
        format!("?{}", set.join("&"))
    }
}

/// `/api/queues/{vhost}/{queue}/get`.
fn peek_path(vhost: &str, queue: &str) -> String {
    format!("/api/queues/{}/{}/get", segment(vhost), segment(queue))
}

/// `/api/exchanges/{vhost}/{exchange}/publish`, with the default exchange
/// named `amq.default` as the API wants it.
fn publish_path(vhost: &str, exchange: &str) -> String {
    let exchange = if exchange.is_empty() {
        DEFAULT_EXCHANGE
    } else {
        exchange
    };
    format!(
        "/api/exchanges/{}/{}/publish",
        segment(vhost),
        segment(exchange)
    )
}

/// Percent-encodes `text` as one path segment: the default vhost `/` becomes
/// `%2F`, and a name holding `/`, `?`, `..` or CR/LF cannot change the
/// request path or inject headers. A name of dots only is encoded too.
fn segment(text: &str) -> String {
    if !text.is_empty() && text.bytes().all(|b| b == b'.') {
        return "%2E".repeat(text.len());
    }
    text.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

/// The body of a successful response, else the API's error.
fn success_body(response: Response) -> RabbitResult<String> {
    if response.is_success() {
        return Ok(response.body);
    }
    Err(RabbitError::Api {
        status: response.status,
        message: api_error_message(&response),
    })
}

/// The `error` and `reason` of the API's JSON error body, with a hint for
/// the statuses people usually hit; else the body or the status line.
fn api_error_message(response: &Response) -> String {
    let parsed = serde_json::from_str::<serde_json::Value>(&response.body).ok();
    let field = |name: &str| {
        parsed
            .as_ref()
            .and_then(|v| v.get(name))
            .and_then(|v| v.as_str())
            .map(str::to_string)
    };
    let detail = match (field("error"), field("reason")) {
        (Some(error), Some(reason)) if error != reason => format!("{error}: {reason}"),
        (Some(error), _) => error,
        (None, Some(reason)) => reason,
        (None, None) => Some(response.body.trim().to_string())
            .filter(|b| !b.is_empty())
            .unwrap_or_else(|| response.status_line.clone()),
    };
    match response.status {
        401 => format!("{detail} (check the user and password)"),
        403 => format!("{detail} (the user lacks a management tag or vhost permission)"),
        404 => format!("{detail} (no such vhost, queue or exchange)"),
        _ => detail,
    }
}

fn parse_json<T: DeserializeOwned>(body: &str) -> RabbitResult<T> {
    serde_json::from_str(body).map_err(|e| RabbitError::Response(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn response(status: u16, body: &str) -> Response {
        Response {
            status,
            status_line: format!("HTTP/1.1 {status} X"),
            body: body.to_string(),
        }
    }

    #[test]
    fn names_are_encoded_as_single_path_segments() {
        assert_eq!(segment("/"), "%2F");
        assert_eq!(segment("orders.dlq"), "orders.dlq");
        assert_eq!(segment("a/b?c"), "a%2Fb%3Fc");
        assert_eq!(segment(".."), "%2E%2E");
        assert_eq!(segment("x\r\nHost: evil"), "x%0D%0AHost%3A%20evil");
        assert_eq!(peek_path("/", "orders"), "/api/queues/%2F/orders/get");
        assert_eq!(
            publish_path("prod", "orders"),
            "/api/exchanges/prod/orders/publish"
        );
    }

    #[test]
    fn topology_paths_encode_every_name() {
        use crate::model::DestinationType;
        assert_eq!(queue_path("/", "a b"), "/api/queues/%2F/a%20b");
        assert_eq!(exchange_path("shop", "x/y"), "/api/exchanges/shop/x%2Fy");
        let binding = BindingDeclaration {
            source: "shop".into(),
            destination: "orders.q".into(),
            destination_type: DestinationType::Queue,
            routing_key: "k".into(),
            arguments: Default::default(),
        };
        assert_eq!(
            binding_path("/", &binding),
            "/api/bindings/%2F/e/shop/q/orders.q"
        );
        let to_exchange = BindingDeclaration {
            destination_type: DestinationType::Exchange,
            ..binding
        };
        assert_eq!(
            binding_path("/", &to_exchange),
            "/api/bindings/%2F/e/shop/e/orders.q"
        );
        assert_eq!(conditions(&[("if-empty", false), ("if-unused", false)]), "");
        assert_eq!(
            conditions(&[("if-empty", true), ("if-unused", true)]),
            "?if-empty=true&if-unused=true"
        );
    }

    #[test]
    fn the_default_exchange_is_published_to_as_amq_default() {
        assert_eq!(
            publish_path("/", ""),
            "/api/exchanges/%2F/amq.default/publish"
        );
        assert_eq!(
            publish_path("/", DEFAULT_EXCHANGE),
            "/api/exchanges/%2F/amq.default/publish"
        );
    }

    #[test]
    fn the_url_sets_host_port_and_default_port() {
        let client = ManagementClient::new("http://rabbit").unwrap();
        assert_eq!(client.address(), "rabbit:15672");
        let client = ManagementClient::new("https://u:p@rabbit.example.com/mgmt").unwrap();
        assert_eq!(client.address(), "rabbit.example.com:443");
        assert!(matches!(
            ManagementClient::new("http://:1"),
            Err(RabbitError::Config(_))
        ));
    }

    #[test]
    fn credentials_never_show_in_debug_output() {
        let client = ManagementClient::new("http://admin:url-secret@rabbit:15672")
            .unwrap()
            .with_credentials("admin", "flag-secret");
        let debug = format!("{client:?}");
        assert!(!debug.contains("url-secret"), "{debug}");
        assert!(!debug.contains("flag-secret"), "{debug}");
        assert!(debug.contains("<redacted>"), "{debug}");
    }

    #[test]
    fn api_errors_carry_the_reason_and_a_hint() {
        let not_found = success_body(response(
            404,
            r#"{"error":"Object Not Found","reason":"Not Found"}"#,
        ))
        .unwrap_err();
        assert_eq!(
            not_found,
            RabbitError::Api {
                status: 404,
                message: "Object Not Found: Not Found (no such vhost, queue or exchange)"
                    .to_string()
            }
        );
        let denied = success_body(response(
            401,
            r#"{"error":"not_authorised","reason":"Login failed"}"#,
        ))
        .unwrap_err()
        .to_string();
        assert!(denied.contains("Login failed"), "{denied}");
        assert!(denied.contains("user and password"), "{denied}");
        let plain = success_body(response(500, "")).unwrap_err().to_string();
        assert!(plain.contains("HTTP/1.1 500"), "{plain}");
        assert_eq!(success_body(response(200, "[]")).unwrap(), "[]");
    }

    #[test]
    fn unexpected_bodies_are_response_errors() {
        let parsed: RabbitResult<Vec<Queue>> = parse_json("{\"not\":\"a list\"}");
        assert!(matches!(parsed, Err(RabbitError::Response(_))));
        let routed: PublishOutcome = parse_json(r#"{"routed":false}"#).unwrap();
        assert!(!routed.routed);
    }
    #[tokio::test]
    async fn topology_writes_reach_the_management_api() {
        use crate::fake::FakeManagement;
        use crate::model::{DestinationType, ExchangeDeclaration};
        let fake = FakeManagement::start().await;
        let client = fake.client();
        assert_eq!(client.queue("/", "orders").await.unwrap(), None);
        let declaration = QueueDeclaration {
            kind: Some("classic".into()),
            durable: true,
            ..QueueDeclaration::default()
        };
        client
            .declare_queue("/", "orders", &declaration)
            .await
            .unwrap();
        let queue = client.queue("/", "orders").await.unwrap().unwrap();
        assert_eq!(queue.kind, "classic");
        assert!(queue.durable);
        // Another declaration of the same queue is refused by the broker.
        let other = QueueDeclaration {
            durable: false,
            ..declaration.clone()
        };
        assert!(matches!(
            client.declare_queue("/", "orders", &other).await,
            Err(RabbitError::Api { status: 400, .. })
        ));

        let exchange = ExchangeDeclaration {
            kind: "topic".into(),
            durable: true,
            auto_delete: false,
            internal: false,
            arguments: Default::default(),
        };
        client
            .declare_exchange("/", "shop", &exchange)
            .await
            .unwrap();
        assert_eq!(
            client.exchange("/", "shop").await.unwrap().unwrap().kind,
            "topic"
        );
        let binding = BindingDeclaration {
            source: "shop".into(),
            destination: "orders".into(),
            destination_type: DestinationType::Queue,
            routing_key: "order.*".into(),
            arguments: Default::default(),
        };
        client.bind("/", &binding).await.unwrap();
        let bound = client.queue_bindings("/", "orders").await.unwrap();
        assert_eq!(bound.len(), 1, "the default exchange's binding is left out");
        assert!(binding.matches(&bound[0]));
        assert_eq!(
            client.exchange_bindings("/", "shop").await.unwrap().len(),
            1
        );
        assert!(client.unbind("/", &binding).await.unwrap());
        assert!(!client.unbind("/", &binding).await.unwrap(), "already gone");

        fake.with(|broker| broker.messages.insert(("/".into(), "orders".into()), 5));
        assert!(matches!(
            client.delete_queue("/", "orders", true, false).await,
            Err(RabbitError::Api { status: 400, .. })
        ));
        client.purge_queue("/", "orders").await.unwrap();
        assert_eq!(fake.with(|b| b.messages_of("/", "orders")), Some(0));
        client
            .delete_queue("/", "orders", true, true)
            .await
            .unwrap();
        assert_eq!(client.queue("/", "orders").await.unwrap(), None);
        client.delete_exchange("/", "shop", true).await.unwrap();
        let writes = fake.with(|b| b.writes());
        assert!(
            writes.contains(
                &"DELETE /api/queues/%2F/orders?if-empty=true&if-unused=true".to_string()
            ),
            "{writes:?}"
        );
        assert!(
            writes.contains(&"DELETE /api/bindings/%2F/e/shop/q/orders/order.%2A".to_string()),
            "{writes:?}"
        );
        // Binding to or from the default exchange is refused before any request.
        let default = BindingDeclaration {
            source: String::new(),
            ..binding
        };
        assert!(matches!(
            client.bind("/", &default).await,
            Err(RabbitError::Config(_))
        ));
    }
}
