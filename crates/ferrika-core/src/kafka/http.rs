//! A small HTTP/1.1 client for the Schema Registry and Kafka Connect REST
//! APIs: `http://` and `https://` (rustls with the OS trust store), basic
//! auth from the URL (`https://key:secret@host`), one request per connection.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::error::{KafkaError, KafkaResult};

/// How long connecting (TCP + TLS) and, separately, the request/response
/// exchange may take before the call fails, so a hung server cannot hang the
/// command.
const HTTP_TIMEOUT: Duration = Duration::from_secs(30);

/// The largest response accepted; a bigger one fails instead of exhausting
/// memory.
const MAX_RESPONSE_BYTES: u64 = 64 * 1024 * 1024;

/// TLS files for an endpoint other than the defaults: a CA file (PEM)
/// trusted instead of the OS trust store, and a client certificate and key
/// (PEM, unencrypted key) for mutual TLS. Paths, not contents.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TlsFiles {
    pub ca_file: Option<String>,
    pub client_cert: Option<String>,
    pub client_key: Option<String>,
}

impl TlsFiles {
    /// Whether nothing is set (the OS trust store, no client certificate).
    pub fn is_empty(&self) -> bool {
        self.ca_file.is_none() && self.client_cert.is_none() && self.client_key.is_none()
    }

    /// The client certificate and key, which go together.
    pub fn client_pair(&self) -> KafkaResult<Option<(&str, &str)>> {
        match (&self.client_cert, &self.client_key) {
            (Some(cert), Some(key)) => Ok(Some((cert.as_str(), key.as_str()))),
            (None, None) => Ok(None),
            (Some(_), None) => Err(KafkaError::config(
                "A TLS client certificate needs its key file too",
            )),
            (None, Some(_)) => Err(KafkaError::config(
                "A TLS client key needs its certificate file too",
            )),
        }
    }
}

/// A TLS configuration built from [`TlsFiles`]; compared by its files.
#[derive(Clone)]
struct CustomTls {
    files: TlsFiles,
    config: Arc<rustls::ClientConfig>,
}

impl PartialEq for CustomTls {
    fn eq(&self, other: &Self) -> bool {
        self.files == other.files
    }
}

impl Eq for CustomTls {}

/// Where requests go: scheme, host, port, path prefix and credentials.
#[derive(Clone, PartialEq, Eq)]
pub struct Endpoint {
    pub tls: bool,
    pub host: String,
    pub port: u16,
    /// Path prefix from the URL, without a trailing slash ("" for none).
    pub base_path: String,
    /// `Basic ...` header value when the URL carries credentials.
    authorization: Option<String>,
    /// Limit for connecting, the TLS handshake and the exchange, each.
    timeout: Duration,
    /// TLS from [`Endpoint::with_tls_files`], else the OS trust store.
    custom_tls: Option<CustomTls>,
}

/// A response's status code and body (chunked encoding already removed).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Response {
    pub status: u16,
    pub status_line: String,
    pub body: String,
}

impl Response {
    pub fn is_success(&self) -> bool {
        (200..300).contains(&self.status)
    }

    /// The `message` of a JSON error body (`{"error_code": .., "message": ..}`),
    /// else the body or the status line.
    pub fn error_message(&self) -> String {
        serde_json::from_str::<serde_json::Value>(&self.body)
            .ok()
            .and_then(|v| {
                v.get("message")
                    .and_then(|m| m.as_str())
                    .map(str::to_string)
            })
            .or_else(|| Some(self.body.trim().to_string()).filter(|b| !b.is_empty()))
            .unwrap_or_else(|| self.status_line.clone())
    }
}

// Manual so the `Basic ...` credentials never reach logs or error output.
impl std::fmt::Debug for Endpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Endpoint")
            .field("tls", &self.tls)
            .field("host", &self.host)
            .field("port", &self.port)
            .field("base_path", &self.base_path)
            .field(
                "authorization",
                &self.authorization.as_ref().map(|_| "<redacted>"),
            )
            .field("tls_files", &self.custom_tls.as_ref().map(|c| &c.files))
            .finish()
    }
}

impl Endpoint {
    /// Parses `http(s)://[user:pass@]host[:port][/prefix]`; a URL without a
    /// scheme is plain HTTP.
    pub fn parse(url: &str, default_port: u16) -> KafkaResult<Self> {
        // Never print the user and password of the URL back.
        let invalid = || {
            KafkaError::config(format!(
                "Invalid URL '{}'",
                crate::model::format::redact_url(url)
            ))
        };
        let trimmed = url.trim().trim_end_matches('/');
        // URL schemes are case-insensitive (RFC 3986 section 3.1).
        let strip_scheme = |scheme: &str| {
            let head = trimmed.get(..scheme.len())?;
            head.eq_ignore_ascii_case(scheme)
                .then(|| &trimmed[scheme.len()..])
        };
        let (tls, rest) = if let Some(rest) = strip_scheme("https://") {
            (true, rest)
        } else {
            (false, strip_scheme("http://").unwrap_or(trimmed))
        };
        let (authority, base_path) = match rest.find('/') {
            Some(i) => (&rest[..i], rest[i..].to_string()),
            None => (rest, String::new()),
        };
        let (userinfo, hostport) = match authority.rsplit_once('@') {
            Some((u, h)) => (Some(u), h),
            None => (None, authority),
        };
        let (host, port) = match hostport.rsplit_once(':') {
            // An IPv6 literal ("[::1]") without a port has colons but no "]:".
            Some((h, p)) if !p.contains(']') => (h, p.parse().map_err(|_| invalid())?),
            _ => (hostport, if tls { 443 } else { default_port }),
        };
        let host = host.trim_start_matches('[').trim_end_matches(']');
        if host.is_empty() {
            return Err(invalid());
        }
        let authorization = userinfo.map(|u| {
            format!(
                "Basic {}",
                crate::codec::base64::encode(percent_decode(u).as_bytes())
            )
        });
        Ok(Self {
            tls,
            host: host.to_string(),
            port,
            base_path,
            authorization,
            timeout: HTTP_TIMEOUT,
            custom_tls: None,
        })
    }

    /// Uses `files` for `https://`: the CA file instead of the OS trust
    /// store, and the client certificate and key for mutual TLS. The files
    /// are read now, so a missing or bad file fails here. Empty `files`
    /// keep the defaults; a plain `http://` endpoint ignores them.
    pub fn with_tls_files(mut self, files: &TlsFiles) -> KafkaResult<Self> {
        if files.is_empty() {
            self.custom_tls = None;
            return Ok(self);
        }
        let config = build_tls_config(files.ca_file.as_deref(), files.client_pair()?)?;
        self.custom_tls = Some(CustomTls {
            files: files.clone(),
            config,
        });
        Ok(self)
    }

    /// Sets the time allowed to connect, for the TLS handshake and for the
    /// request and response (each; at least one second). Default 30 s.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout.max(Duration::from_secs(1));
        self
    }

    /// `host:port` (credentials left out), for messages.
    pub fn target(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    /// Sends one request and reads the whole response.
    pub async fn send(
        &self,
        method: &str,
        path: &str,
        content_type: &str,
        body: Option<&str>,
    ) -> KafkaResult<Response> {
        self.send_with_headers(method, path, content_type, body, &[])
            .await
    }

    /// [`Self::send`] with extra request headers. An `Authorization` header
    /// here replaces the credentials from the URL.
    pub async fn send_with_headers(
        &self,
        method: &str,
        path: &str,
        content_type: &str,
        body: Option<&str>,
        headers: &[(&str, &str)],
    ) -> KafkaResult<Response> {
        let request = self.build_request(method, path, content_type, body, headers)?;
        let target = self.target();
        let timed_out = || {
            KafkaError::Io(format!(
                "Timed out after {}s reaching {target}",
                self.timeout.as_secs()
            ))
        };
        let tcp = tokio::time::timeout(self.timeout, TcpStream::connect(&target))
            .await
            .map_err(|_| timed_out())?
            .map_err(|e| KafkaError::Io(format!("Cannot reach {target}: {e}")))?;
        let raw = if self.tls {
            let name = rustls::pki_types::ServerName::try_from(self.host.clone()).map_err(|e| {
                KafkaError::config(format!("Invalid TLS host '{}': {e}", self.host))
            })?;
            let config = match &self.custom_tls {
                Some(custom) => custom.config.clone(),
                None => tls_config()?,
            };
            let handshake = tokio_rustls::TlsConnector::from(config).connect(name, tcp);
            let stream = tokio::time::timeout(self.timeout, handshake)
                .await
                .map_err(|_| timed_out())?
                .map_err(|e| KafkaError::Io(format!("TLS handshake with {target} failed: {e}")))?;
            exchange(stream, &request, self.timeout, MAX_RESPONSE_BYTES).await?
        } else {
            exchange(tcp, &request, self.timeout, MAX_RESPONSE_BYTES).await?
        };
        parse_response(&raw)
    }

    /// The request bytes [`Self::send_with_headers`] writes.
    fn build_request(
        &self,
        method: &str,
        path: &str,
        content_type: &str,
        body: Option<&str>,
        headers: &[(&str, &str)],
    ) -> KafkaResult<String> {
        let payload = body.unwrap_or_default();
        let mut target_path = format!("{}{path}", self.base_path);
        if !target_path.starts_with('/') {
            target_path.insert(0, '/');
        }
        let mut request = format!(
            "{method} {target_path} HTTP/1.1\r\nHost: {}\r\nUser-Agent: Ferrika\r\n\
             Accept: {content_type}, application/json\r\nContent-Type: {content_type}\r\n\
             Content-Length: {}\r\nConnection: close\r\n",
            self.target(),
            payload.len()
        );
        let mut has_auth = false;
        for (name, value) in headers {
            // A CR or LF would let a value inject headers.
            if name.contains(['\r', '\n']) || value.contains(['\r', '\n']) {
                return Err(KafkaError::config(format!(
                    "Invalid HTTP header '{name}': line breaks are not allowed"
                )));
            }
            has_auth |= name.eq_ignore_ascii_case("authorization");
            request.push_str(&format!("{name}: {value}\r\n"));
        }
        if let (Some(auth), false) = (&self.authorization, has_auth) {
            request.push_str(&format!("Authorization: {auth}\r\n"));
        }
        request.push_str("\r\n");
        request.push_str(payload);
        Ok(request)
    }
}

/// Writes `request` and reads the response until the server closes, failing
/// when that takes longer than `timeout` or the response exceeds `max_bytes`.
async fn exchange<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: S,
    request: &str,
    timeout: Duration,
    max_bytes: u64,
) -> KafkaResult<Vec<u8>> {
    let io = async {
        stream
            .write_all(request.as_bytes())
            .await
            .map_err(|e| KafkaError::Io(format!("HTTP request failed: {e}")))?;
        let mut buf = Vec::new();
        // One byte past the cap tells "exactly at the cap" from "too big".
        let read = (&mut stream)
            .take(max_bytes.saturating_add(1))
            .read_to_end(&mut buf)
            .await;
        if buf.len() as u64 > max_bytes {
            return Err(KafkaError::Io(format!(
                "HTTP response is larger than the {} MiB limit",
                max_bytes / (1024 * 1024)
            )));
        }
        match read {
            Ok(_) => Ok(buf),
            // Servers often close TLS without close_notify; the body is complete.
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof && !buf.is_empty() => Ok(buf),
            Err(e) => Err(KafkaError::Io(format!("HTTP response failed: {e}"))),
        }
    };
    tokio::time::timeout(timeout, io).await.map_err(|_| {
        KafkaError::Io(format!(
            "HTTP response timed out after {}s",
            timeout.as_secs_f32()
        ))
    })?
}

/// The client TLS config for REST APIs: OS trust store roots, built once.
fn tls_config() -> KafkaResult<Arc<rustls::ClientConfig>> {
    static CONFIG: OnceLock<Result<Arc<rustls::ClientConfig>, String>> = OnceLock::new();
    CONFIG
        .get_or_init(|| build_tls_config(None, None).map_err(|e| e.to_string()))
        .clone()
        .map_err(|e| KafkaError::config(format!("TLS setup failed: {e}")))
}

/// TLS for broker connections: the profile's CA file (else the OS trust
/// store) and, when both are set, a client certificate and key (PEM) for
/// mutual TLS.
pub fn broker_tls_config(
    config: &crate::kafka::config::KafkaClientConfig,
) -> KafkaResult<Arc<rustls::ClientConfig>> {
    if config.ssl_key_password.is_some() {
        return Err(KafkaError::config(
            "Encrypted client keys (ssl.key.password) are not supported by the native \
             admin requests; use an unencrypted PEM key",
        ));
    }
    let client = match (&config.ssl_certificate_location, &config.ssl_key_location) {
        (Some(cert), Some(key)) => Some((cert.as_str(), key.as_str())),
        _ => None,
    };
    build_tls_config(config.ssl_ca_location.as_deref(), client)
}

/// TLS for a REST client with its own transport (the ksqlDB client, for
/// one), from the same [`TlsFiles`] as [`Endpoint::with_tls_files`]: the CA
/// file (PEM) or, without one, the OS trust store, and the client
/// certificate and unencrypted key for mutual TLS. Empty `files` give the
/// shared OS-trust-store config.
pub fn rest_tls_config(files: &TlsFiles) -> KafkaResult<Arc<rustls::ClientConfig>> {
    if files.is_empty() {
        return tls_config();
    }
    build_tls_config(files.ca_file.as_deref(), files.client_pair()?)
}

fn build_tls_config(
    ca_file: Option<&str>,
    client: Option<(&str, &str)>,
) -> KafkaResult<Arc<rustls::ClientConfig>> {
    use rustls::pki_types::pem::PemObject;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};

    let tls_err = |msg: String| KafkaError::config(format!("TLS setup failed: {msg}"));
    let mut roots = rustls::RootCertStore::empty();
    match ca_file {
        Some(path) => {
            let certs = CertificateDer::pem_file_iter(path)
                .map_err(|e| tls_err(format!("cannot read CA file '{path}': {e}")))?;
            for cert in certs {
                let cert = cert.map_err(|e| tls_err(format!("bad CA file '{path}': {e}")))?;
                roots
                    .add(cert)
                    .map_err(|e| tls_err(format!("bad CA certificate in '{path}': {e}")))?;
            }
        }
        None => {
            for cert in rustls_native_certs::load_native_certs().certs {
                // Skip certificates rustls cannot parse rather than failing.
                let _ = roots.add(cert);
            }
        }
    }
    if roots.is_empty() {
        return Err(tls_err("no trusted root certificates".to_string()));
    }
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| tls_err(e.to_string()))?
        .with_root_certificates(roots);
    let config = match client {
        None => builder.with_no_client_auth(),
        Some((cert_path, key_path)) => {
            let certs = CertificateDer::pem_file_iter(cert_path)
                .and_then(|certs| certs.collect::<Result<Vec<_>, _>>())
                .map_err(|e| tls_err(format!("cannot read certificate '{cert_path}': {e}")))?;
            let key = PrivateKeyDer::from_pem_file(key_path)
                .map_err(|e| tls_err(format!("cannot read key '{key_path}': {e}")))?;
            builder
                .with_client_auth_cert(certs, key)
                .map_err(|e| tls_err(e.to_string()))?
        }
    };
    Ok(Arc::new(config))
}

fn parse_response(raw: &[u8]) -> KafkaResult<Response> {
    // Split on bytes: the body is only decoded as UTF-8 once unframed.
    let find = |needle: &[u8]| raw.windows(needle.len()).position(|w| w == needle);
    let (head_end, body_start) = find(b"\r\n\r\n")
        .map(|i| (i, i + 4))
        .or_else(|| find(b"\n\n").map(|i| (i, i + 2)))
        .ok_or_else(|| KafkaError::Io("Malformed HTTP response".to_string()))?;
    let head = String::from_utf8_lossy(&raw[..head_end]).into_owned();
    let body = &raw[body_start..];
    let status_line = head.lines().next().unwrap_or_default().to_string();
    let status = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| KafkaError::Io(format!("Malformed HTTP status line '{status_line}'")))?;
    let chunked = head
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked");
    let body = if chunked {
        String::from_utf8_lossy(&decode_chunked_body(body))
            .trim()
            .to_string()
    } else {
        String::from_utf8_lossy(body).trim().to_string()
    };
    Ok(Response {
        status,
        status_line,
        body,
    })
}

/// Decodes `%XX` escapes in URL credentials.
fn percent_decode(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            if let Some(v) = text
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

/// Removes HTTP chunked transfer framing. Works on bytes: chunk sizes
/// count bytes, and a multi-byte character may span two chunks.
pub(crate) fn decode_chunked_body(input: &[u8]) -> Vec<u8> {
    let mut result = Vec::with_capacity(input.len());
    let mut remaining = input;
    while !remaining.is_empty() {
        let Some(idx) = remaining.windows(2).position(|w| w == b"\r\n") else {
            result.extend_from_slice(remaining);
            break;
        };
        let size = std::str::from_utf8(&remaining[..idx])
            .ok()
            // Chunk extensions (";name=value") follow the size.
            .and_then(|line| usize::from_str_radix(line.split(';').next()?.trim(), 16).ok());
        let Some(size) = size else {
            result.extend_from_slice(remaining);
            break;
        };
        if size == 0 {
            break;
        }
        let data_start = idx + 2;
        let data_end = data_start.saturating_add(size).min(remaining.len());
        result.extend_from_slice(&remaining[data_start..data_end]);
        // Skip the CRLF after the data.
        remaining = remaining.get(data_end + 2..).unwrap_or_default();
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_scheme_credentials_port_and_prefix() {
        let e = Endpoint::parse("https://KEY:s%40cret@psrc.example.com/registry/", 8081).unwrap();
        assert!(e.tls);
        assert_eq!(e.target(), "psrc.example.com:443");
        assert_eq!(e.base_path, "/registry");
        // base64("KEY:s@cret")
        assert_eq!(e.authorization.as_deref(), Some("Basic S0VZOnNAY3JldA=="));

        let e = Endpoint::parse("localhost:18081", 8081).unwrap();
        assert!(!e.tls);
        assert_eq!(e.target(), "localhost:18081");
        assert_eq!(e.authorization, None);
        assert_eq!(Endpoint::parse("http://connect", 8083).unwrap().port, 8083);
        assert_eq!(Endpoint::parse("http://[::1]:9000", 1).unwrap().host, "::1");
        assert!(Endpoint::parse("http://:80", 1).is_err());
        for bad in [
            "https://KEY:s3cr3t@host:notaport/x",
            "KEY:s3cr3t@:80",
            "http://KEY:s3cr3t@:80/registry",
        ] {
            let message = Endpoint::parse(bad, 1).unwrap_err().to_string();
            assert!(message.contains("Invalid URL"), "{message}");
            assert!(
                !message.contains("s3cr3t") && !message.contains("KEY"),
                "{message}"
            );
        }
        assert!(Endpoint::parse("http://h:port", 1).is_err());
    }

    #[test]
    fn url_scheme_is_case_insensitive() {
        let e = Endpoint::parse("HTTPS://idp.example.com:8443/t", 80).unwrap();
        assert!(e.tls);
        assert_eq!(e.target(), "idp.example.com:8443");
        assert_eq!(e.base_path, "/t");
        let e = Endpoint::parse("Http://Connect:8083", 1).unwrap();
        assert!(!e.tls);
        assert_eq!(e.host, "Connect");
        assert!(Endpoint::parse("HtTpS://h", 1).unwrap().tls);
    }

    #[test]
    fn explicit_headers_replace_url_credentials() {
        let e = Endpoint::parse("https://KEY:pw@idp.example.com", 443).unwrap();
        let r = e
            .build_request(
                "POST",
                "",
                "x",
                Some("a=b"),
                &[("Authorization", "Basic Zm9v")],
            )
            .unwrap();
        assert!(r.starts_with("POST / HTTP/1.1\r\n"), "{r}");
        assert!(r.contains("Authorization: Basic Zm9v\r\n"), "{r}");
        assert_eq!(r.matches("Authorization").count(), 1, "{r}");
        assert!(r.ends_with("\r\n\r\na=b"), "{r}");
        let r = e.build_request("GET", "/x", "x", None, &[]).unwrap();
        assert!(r.contains("Authorization: Basic S0VZOnB3"), "{r}");
        assert!(e
            .build_request("GET", "/", "x", None, &[("X-A", "v\r\nEvil: 1")])
            .is_err());
    }

    #[test]
    fn rest_tls_config_reports_missing_files() {
        let ca = TlsFiles {
            ca_file: Some("/no/such/ca.pem".into()),
            ..TlsFiles::default()
        };
        let err = rest_tls_config(&ca).unwrap_err();
        assert!(err.to_string().contains("cannot read CA file"), "{err}");
        // The PEM files are checked when given, even with the OS trust store.
        let client = TlsFiles {
            client_cert: Some("/no/such/c.pem".into()),
            client_key: Some("/no/such/k.pem".into()),
            ..TlsFiles::default()
        };
        let err = rest_tls_config(&client).unwrap_err();
        assert!(err.to_string().contains("cannot read certificate"), "{err}");
        // Half a client certificate is refused before any file is read.
        let half = TlsFiles {
            client_cert: Some("/no/such/c.pem".into()),
            ..TlsFiles::default()
        };
        assert!(rest_tls_config(&half).is_err());
    }

    #[test]
    fn debug_redacts_credentials() {
        let e = Endpoint::parse("https://KEY:topsecret@psrc.example.com", 8081).unwrap();
        let shown = format!("{e:?}");
        assert!(shown.contains("<redacted>"), "{shown}");
        assert!(!shown.contains("S0VZ"), "{shown}");
        assert!(!shown.contains("Basic"), "{shown}");
    }

    /// A one-shot local server that answers any request with `reply`, then
    /// (when `hang` is set) keeps the connection open without closing.
    async fn serve_once(reply: Vec<u8>, hang: bool) -> TcpStream {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut req = [0u8; 1024];
            let _ = sock.read(&mut req).await;
            let _ = sock.write_all(&reply).await;
            if hang {
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
        });
        TcpStream::connect(addr).await.unwrap()
    }

    #[tokio::test]
    async fn rejects_responses_over_the_size_cap() {
        let mut reply = b"HTTP/1.1 200 OK\r\n\r\n".to_vec();
        reply.extend(vec![b'x'; 4096]);
        let tcp = serve_once(reply.clone(), false).await;
        let err = exchange(tcp, "GET / HTTP/1.1\r\n\r\n", HTTP_TIMEOUT, 1024)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("limit"), "{err}");

        let tcp = serve_once(reply.clone(), false).await;
        let raw = exchange(tcp, "GET / HTTP/1.1\r\n\r\n", HTTP_TIMEOUT, 1 << 20)
            .await
            .unwrap();
        assert_eq!(raw, reply);
    }

    #[tokio::test]
    async fn times_out_on_a_hung_server() {
        let tcp = serve_once(b"HTTP/1.1 200 OK\r\n".to_vec(), true).await;
        let err = exchange(
            tcp,
            "GET / HTTP/1.1\r\n\r\n",
            Duration::from_millis(200),
            MAX_RESPONSE_BYTES,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("timed out"), "{err}");
    }

    #[tokio::test]
    async fn send_uses_the_endpoint_timeout() {
        // Accepts and reads, never answers.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut req = [0u8; 1024];
            let _ = sock.read(&mut req).await;
            tokio::time::sleep(Duration::from_secs(30)).await;
        });
        let endpoint = Endpoint::parse(&format!("http://127.0.0.1:{port}"), 80)
            .unwrap()
            .with_timeout(Duration::from_secs(1));
        let started = std::time::Instant::now();
        let err = endpoint
            .send("GET", "/", "text/plain", None)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("timed out after 1s"), "{err}");
        assert!(started.elapsed() < Duration::from_secs(10));
        // Below one second is raised to one.
        let e = Endpoint::parse("http://x", 80)
            .unwrap()
            .with_timeout(Duration::ZERO);
        assert_eq!(e.timeout, Duration::from_secs(1));
    }

    #[test]
    fn parses_plain_and_chunked_responses() {
        let r = parse_response(b"HTTP/1.1 404 Not Found\r\nContent-Type: x\r\n\r\n{\"error_code\":40401,\"message\":\"Subject not found\"}").unwrap();
        assert_eq!(r.status, 404);
        assert!(!r.is_success());
        assert_eq!(r.error_message(), "Subject not found");
        let r = parse_response(
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n2\r\n[]\r\n0\r\n\r\n",
        )
        .unwrap();
        assert!(r.is_success());
        assert_eq!(r.body, "[]");
        assert!(parse_response(b"garbage").is_err());
    }

    #[test]
    fn chunked_bodies_decode() {
        let chunked_input = "7\r\nMozilla\r\n9\r\nDeveloper\r\n7\r\nNetwork\r\n0\r\n\r\n";
        let decoded = decode_chunked_body(chunked_input.as_bytes());
        assert_eq!(decoded, b"MozillaDeveloperNetwork");
        // "한" (3 bytes) split across two chunks must not panic or garble.
        let han = "한".as_bytes();
        let mut split = b"2\r\n".to_vec();
        split.extend_from_slice(&han[..2]);
        split.extend_from_slice(b"\r\n1;ext=1\r\n");
        split.extend_from_slice(&han[2..]);
        split.extend_from_slice(b"\r\n0\r\n\r\n");
        assert_eq!(
            String::from_utf8(decode_chunked_body(&split)).unwrap(),
            "한"
        );
    }

    #[test]
    fn tls_files_are_checked_when_set() {
        let plain = Endpoint::parse("https://rabbit:15671", 1).unwrap();
        let same = plain.clone().with_tls_files(&TlsFiles::default()).unwrap();
        assert_eq!(same, plain);

        let missing = TlsFiles {
            ca_file: Some("/no/such/ferrika-ca.pem".into()),
            ..TlsFiles::default()
        };
        let err = plain
            .clone()
            .with_tls_files(&missing)
            .unwrap_err()
            .to_string();
        assert!(err.contains("cannot read CA file"), "{err}");
        assert!(err.contains("ferrika-ca.pem"), "{err}");

        let half = TlsFiles {
            client_cert: Some("client.pem".into()),
            ..TlsFiles::default()
        };
        assert!(half.client_pair().is_err());
        let err = plain.clone().with_tls_files(&half).unwrap_err().to_string();
        assert!(err.contains("key file"), "{err}");
        let key_only = TlsFiles {
            client_key: Some("client.key".into()),
            ..TlsFiles::default()
        };
        assert!(key_only.client_pair().is_err());

        // A CA file without any certificate in it trusts nothing: refused.
        let dir = std::env::temp_dir().join(format!("ferrika-http-tls-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let empty = dir.join("empty-ca.pem");
        std::fs::write(
            &empty,
            "not a certificate
",
        )
        .unwrap();
        let none = TlsFiles {
            ca_file: Some(empty.display().to_string()),
            ..TlsFiles::default()
        };
        let err = plain.with_tls_files(&none).unwrap_err().to_string();
        assert!(err.contains("no trusted root certificates"), "{err}");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
