//! A client for the RabbitMQ management HTTP API (the `rabbitmq_management`
//! plugin), over the same small HTTP/1.1 client Ferrika uses for the Schema
//! Registry and Kafka Connect: `http://` and `https://` (OS trust store),
//! basic authentication, one request per connection. A CA file and a
//! client certificate replace the OS trust store with
//! [`ManagementClient::with_tls`]; there is no mode that skips verifying
//! the server's certificate.

use std::time::Duration;

use serde::de::DeserializeOwned;

use ferrika_core::error::KafkaError;
/// The CA file and client certificate for `https://` and `amqps://`.
pub use ferrika_core::kafka::http::TlsFiles;
use ferrika_core::kafka::http::{Endpoint, Response};

use crate::error::{RabbitError, RabbitResult};
use crate::model::{
    peek_request, Binding, Exchange, OutgoingMessage, PeekedMessage, PublishOutcome, Queue,
    DEFAULT_EXCHANGE,
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

    /// Uses `files` for `https://`: the CA file is trusted instead of the
    /// OS trust store, and the client certificate and key are presented
    /// for mutual TLS. The files are read now.
    pub fn with_tls(mut self, files: &TlsFiles) -> RabbitResult<Self> {
        self.endpoint = self.endpoint.with_tls_files(files).map_err(|e| match e {
            KafkaError::Config(message) => RabbitError::Config(message),
            other => RabbitError::Config(other.to_string()),
        })?;
        Ok(self)
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

    pub(crate) async fn get_json<T: DeserializeOwned>(&self, path: &str) -> RabbitResult<T> {
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
        self.endpoint
            .send_with_headers(method, path, "application/json", body, &headers)
            .await
            .map_err(|e| RabbitError::Unreachable {
                target: self.address(),
                reason: with_tls_hint(match e {
                    KafkaError::Io(reason) | KafkaError::Config(reason) => reason,
                    other => other.to_string(),
                }),
            })
    }
}

/// `reason` with a hint when the server's TLS certificate was not trusted
/// (rustls says `invalid peer certificate: UnknownIssuer`, `NotValidForName`
/// ...), since that usually means a private CA.
pub(crate) fn with_tls_hint(reason: String) -> String {
    if reason.contains("CertificateRequired") {
        return format!(
            "{reason} (the server requires a client certificate: give --rabbit-client-cert and \
             --rabbit-client-key, or the profile's client_cert and client_key)"
        );
    }
    if !reason.contains("invalid peer certificate") {
        return reason;
    }
    let hint = if reason.contains("NotValidForName") {
        "the certificate does not name this host; connect with a host name it lists"
    } else if reason.contains("Expired") || reason.contains("NotValidYet") {
        "the certificate is outside its validity period"
    } else {
        "the server's certificate is not trusted; if it is signed by a private CA, give the CA \
         file with --rabbit-ca-cert or the profile's ca_cert"
    };
    format!("{reason} ({hint})")
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
pub(crate) fn segment(text: &str) -> String {
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
    fn untrusted_certificates_get_a_hint() {
        let unknown = with_tls_hint(
            "TLS handshake with h:1 failed: invalid peer certificate: UnknownIssuer".to_string(),
        );
        assert!(unknown.contains("--rabbit-ca-cert"), "{unknown}");
        let name = with_tls_hint(
            "invalid peer certificate: certificate not valid for name NotValidForName".to_string(),
        );
        assert!(name.contains("does not name this host"), "{name}");
        let required = with_tls_hint("received fatal alert: CertificateRequired".to_string());
        assert!(required.contains("--rabbit-client-cert"), "{required}");
        assert_eq!(with_tls_hint("refused".to_string()), "refused");
    }

    #[test]
    fn tls_files_are_read_when_set() {
        let client = ManagementClient::new("https://rabbit:15671").unwrap();
        assert!(client.clone().with_tls(&TlsFiles::default()).is_ok());
        let missing = TlsFiles {
            ca_file: Some("/no/such/ca.pem".to_string()),
            ..TlsFiles::default()
        };
        assert!(matches!(
            client.with_tls(&missing),
            Err(RabbitError::Config(_))
        ));
    }

    #[test]
    fn unexpected_bodies_are_response_errors() {
        let parsed: RabbitResult<Vec<Queue>> = parse_json("{\"not\":\"a list\"}");
        assert!(matches!(parsed, Err(RabbitError::Response(_))));
        let routed: PublishOutcome = parse_json(r#"{"routed":false}"#).unwrap();
        assert!(!routed.routed);
    }
}
