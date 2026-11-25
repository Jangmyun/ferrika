//! The HTTP/1.1 transport of the ksqlDB client: one request per connection
//! (`Connection: close`), `http://` or `https://` (rustls, the OS trust store
//! or a CA file, optionally a client certificate), basic authentication,
//! and a body that can be read whole or line by line while it streams
//! (chunked transfer encoding), which push queries need.
//!
//! ferrika-core's HTTP client reads a response until the server closes it,
//! so it cannot hand out the rows of a push query as they arrive; this one
//! can, and it applies the same limits (time, size).

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

use crate::config::KsqlConfig;
use crate::error::{KsqlError, KsqlResult};

/// The largest response body read whole (listings, `DESCRIBE`, errors).
pub(crate) const MAX_BODY_BYTES: usize = 64 * 1024 * 1024;
/// The largest response head (status line and headers).
const MAX_HEAD_BYTES: usize = 64 * 1024;
/// The largest chunk-size line of a chunked body.
const MAX_CHUNK_LINE: usize = 1024;

/// A connection that can be read and written: TCP or TLS over TCP.
pub(crate) trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}

/// Where requests go and how they authenticate.
#[derive(Clone)]
pub(crate) struct Transport {
    host: String,
    port: u16,
    base_path: String,
    authorization: Option<String>,
    /// `None` for plain HTTP.
    tls: Option<Arc<rustls::ClientConfig>>,
    pub(crate) timeout: Duration,
}

// Manual so the `Basic ...` credentials never reach logs or error output.
impl std::fmt::Debug for Transport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Transport")
            .field("target", &self.target())
            .field("base_path", &self.base_path)
            .field("tls", &self.tls.is_some())
            .field(
                "authorization",
                &self.authorization.as_ref().map(|_| "<redacted>"),
            )
            .field("timeout", &self.timeout)
            .finish()
    }
}

/// A response whose body has not been read yet.
pub(crate) struct Response {
    pub(crate) status: u16,
    pub(crate) body: Body,
}

impl Transport {
    pub(crate) fn new(config: &KsqlConfig) -> KsqlResult<Self> {
        let endpoint = config.endpoint()?;
        let tls = if endpoint.tls {
            let client = config.client_certificate()?;
            Some(
                ferrika_core::kafka::http::rest_tls_config(config.ca_location.as_deref(), client)
                    .map_err(|e| KsqlError::config(e.to_string()))?,
            )
        } else {
            if config.ca_location.is_some() || config.client_certificate()?.is_some() {
                return Err(KsqlError::config(
                    "TLS files are set for the ksqlDB server, but its URL is http://; use https://",
                ));
            }
            None
        };
        Ok(Self {
            host: endpoint.host.clone(),
            port: endpoint.port,
            base_path: endpoint.base_path.clone(),
            authorization: config.authorization()?,
            tls,
            timeout: config.timeout,
        })
    }

    /// `host:port`, for messages and the policy.
    pub(crate) fn target(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    fn unreachable(&self, reason: impl Into<String>) -> KsqlError {
        KsqlError::Unreachable {
            target: self.target(),
            reason: reason.into(),
        }
    }

    fn timed_out(&self, limit: Duration) -> KsqlError {
        self.unreachable(format!("timed out after {}s", limit.as_secs()))
    }

    async fn connect(&self) -> KsqlResult<Box<dyn Io>> {
        let target = self.target();
        let tcp = tokio::time::timeout(self.timeout, TcpStream::connect(&target))
            .await
            .map_err(|_| self.timed_out(self.timeout))?
            .map_err(|e| self.unreachable(e.to_string()))?;
        let Some(tls) = &self.tls else {
            return Ok(Box::new(tcp));
        };
        let name = rustls::pki_types::ServerName::try_from(self.host.clone())
            .map_err(|e| KsqlError::config(format!("Invalid TLS host '{}': {e}", self.host)))?;
        let handshake = tokio_rustls::TlsConnector::from(tls.clone()).connect(name, tcp);
        let stream = tokio::time::timeout(self.timeout, handshake)
            .await
            .map_err(|_| self.timed_out(self.timeout))?
            .map_err(|e| self.unreachable(format!("TLS handshake failed: {e}")))?;
        Ok(Box::new(stream))
    }

    /// The request bytes: method, path under the URL's prefix, headers, body.
    fn build_request(&self, method: &str, path: &str, accept: &str, body: Option<&str>) -> String {
        let payload = body.unwrap_or_default();
        let mut request = format!(
            "{method} {}{path} HTTP/1.1\r\nHost: {}\r\nUser-Agent: Ferrika\r\nAccept: {accept}\r\n",
            self.base_path,
            self.target()
        );
        if body.is_some() {
            request.push_str(&format!(
                "Content-Type: application/json\r\nContent-Length: {}\r\n",
                payload.len()
            ));
        }
        request.push_str("Connection: close\r\n");
        if let Some(auth) = &self.authorization {
            request.push_str(&format!("Authorization: {auth}\r\n"));
        }
        request.push_str("\r\n");
        request.push_str(payload);
        request
    }

    /// Sends a request and reads the response head; the body is left to
    /// the caller. Connecting, sending and the head each get the timeout.
    pub(crate) async fn open(
        &self,
        method: &str,
        path: &str,
        accept: &str,
        body: Option<&str>,
    ) -> KsqlResult<Response> {
        let mut stream = self.connect().await?;
        let request = self.build_request(method, path, accept, body);
        let exchange = async {
            stream
                .write_all(request.as_bytes())
                .await
                .map_err(|e| self.unreachable(format!("sending the request failed: {e}")))?;
            stream
                .flush()
                .await
                .map_err(|e| self.unreachable(format!("sending the request failed: {e}")))?;
            let mut reader = BufReader::new(stream);
            let head = read_head(&mut reader).await?;
            Ok::<_, KsqlError>((reader, head))
        };
        let (reader, head) = tokio::time::timeout(self.timeout, exchange)
            .await
            .map_err(|_| self.timed_out(self.timeout))??;
        Ok(Response {
            status: head.status,
            body: Body::new(reader, head.framing),
        })
    }

    /// Sends a request and reads the whole body (at most
    /// [`MAX_BODY_BYTES`]) within the timeout. Error statuses become
    /// [`KsqlError::Api`].
    pub(crate) async fn request(
        &self,
        method: &str,
        path: &str,
        accept: &str,
        body: Option<&str>,
    ) -> KsqlResult<String> {
        let response = self.open(method, path, accept, body).await?;
        let status = response.status;
        let mut body = response.body;
        let text = tokio::time::timeout(self.timeout, body.read_all(MAX_BODY_BYTES))
            .await
            .map_err(|_| self.timed_out(self.timeout))??;
        if (200..300).contains(&status) {
            Ok(text)
        } else {
            Err(api_error(status, &text))
        }
    }
}

/// The error a ksqlDB error body (`{"@type": "statement_error",
/// "error_code": 40001, "message": ...}`) describes.
pub(crate) fn api_error(status: u16, body: &str) -> KsqlError {
    let json = serde_json::from_str::<serde_json::Value>(body.trim()).ok();
    let code = json
        .as_ref()
        .and_then(|v| v.get("error_code"))
        .and_then(serde_json::Value::as_i64);
    let message = json
        .as_ref()
        .and_then(|v| v.get("message"))
        .and_then(serde_json::Value::as_str)
        .map(|m| m.trim().to_string())
        .or_else(|| Some(body.trim().to_string()).filter(|b| !b.is_empty()))
        .unwrap_or_else(|| match status {
            401 => "unauthorized: check the ksqlDB user and password".to_string(),
            403 => "forbidden: the ksqlDB user may not do this".to_string(),
            _ => "no error message".to_string(),
        });
    KsqlError::Api {
        status,
        code,
        message,
    }
}

/// How the body of a response is delimited.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Framing {
    Chunked,
    Length(u64),
    /// Until the server closes the connection.
    Close,
}

struct Head {
    status: u16,
    framing: Framing,
}

async fn read_head<R: AsyncBufReadExt + Unpin>(reader: &mut R) -> KsqlResult<Head> {
    let bad = |m: &str| KsqlError::Response(format!("malformed HTTP response: {m}"));
    let mut total = 0;
    let mut status = None;
    let mut chunked = false;
    let mut length = None;
    loop {
        let mut line = Vec::new();
        let n = (&mut *reader)
            .take((MAX_HEAD_BYTES - total) as u64 + 1)
            .read_until(b'\n', &mut line)
            .await
            .map_err(|e| KsqlError::Response(format!("reading the response failed: {e}")))?;
        if n == 0 {
            return Err(bad("the server closed the connection"));
        }
        total += n;
        if total > MAX_HEAD_BYTES {
            return Err(bad("headers too large"));
        }
        let line = String::from_utf8_lossy(&line);
        let line = line.trim_end_matches(['\r', '\n']);
        if status.is_none() {
            status = Some(
                line.split_whitespace()
                    .nth(1)
                    .and_then(|s| s.parse::<u16>().ok())
                    .ok_or_else(|| bad("no status line"))?,
            );
            continue;
        }
        if line.is_empty() {
            break;
        }
        if let Some((name, value)) = line.split_once(':') {
            let value = value.trim();
            match name.trim().to_ascii_lowercase().as_str() {
                "transfer-encoding" => chunked |= value.to_ascii_lowercase().contains("chunked"),
                "content-length" => length = value.parse::<u64>().ok(),
                _ => {}
            }
        }
    }
    let status = status.ok_or_else(|| bad("no status line"))?;
    let framing = if chunked {
        Framing::Chunked
    } else if let Some(n) = length {
        Framing::Length(n)
    } else if status == 204 || status == 304 {
        Framing::Length(0)
    } else {
        Framing::Close
    };
    Ok(Head { status, framing })
}

/// A response body, decoded from its transfer framing, read whole or one
/// line at a time.
pub(crate) struct Body {
    reader: BufReader<Box<dyn Io>>,
    framing: Framing,
    /// Decoded bytes not handed out yet.
    pending: Vec<u8>,
    done: bool,
}

impl Body {
    fn new(reader: BufReader<Box<dyn Io>>, framing: Framing) -> Self {
        Self {
            reader,
            done: framing == Framing::Length(0),
            framing,
            pending: Vec::new(),
        }
    }

    fn read_error(e: std::io::Error) -> KsqlError {
        KsqlError::Response(format!("reading the response failed: {e}"))
    }

    /// Reads more of the body into `pending`; `false` once it has ended.
    async fn fill(&mut self) -> KsqlResult<bool> {
        if self.done {
            return Ok(false);
        }
        match self.framing {
            Framing::Chunked => {
                let mut line = Vec::new();
                let n = match (&mut self.reader)
                    .take(MAX_CHUNK_LINE as u64)
                    .read_until(b'\n', &mut line)
                    .await
                {
                    Ok(n) => n,
                    // A closed connection ends the body.
                    Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => 0,
                    Err(e) => return Err(Self::read_error(e)),
                };
                if n == 0 {
                    self.done = true;
                    return Ok(false);
                }
                let text = String::from_utf8_lossy(&line);
                let size_text = text.trim().split(';').next().unwrap_or_default().trim();
                if size_text.is_empty() {
                    // The CRLF after a chunk's data, read on its own.
                    return Ok(true);
                }
                let size = usize::from_str_radix(size_text, 16)
                    .map_err(|_| KsqlError::Response(format!("bad chunk size '{size_text}'")))?;
                if size == 0 {
                    self.done = true;
                    return Ok(false);
                }
                if size > MAX_BODY_BYTES {
                    return Err(KsqlError::Response("chunk too large".to_string()));
                }
                let mut data = vec![0u8; size];
                self.reader
                    .read_exact(&mut data)
                    .await
                    .map_err(Self::read_error)?;
                self.pending.extend_from_slice(&data);
                Ok(true)
            }
            Framing::Length(remaining) => {
                let mut buf = vec![0u8; remaining.min(64 * 1024) as usize];
                let n = self.reader.read(&mut buf).await.map_err(Self::read_error)?;
                if n == 0 {
                    self.done = true;
                    return Ok(false);
                }
                self.pending.extend_from_slice(&buf[..n]);
                let left = remaining - n as u64;
                self.framing = Framing::Length(left);
                self.done = left == 0;
                Ok(true)
            }
            Framing::Close => {
                let mut buf = vec![0u8; 64 * 1024];
                let n = match self.reader.read(&mut buf).await {
                    Ok(n) => n,
                    // Servers often close TLS without close_notify.
                    Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => 0,
                    Err(e) => return Err(Self::read_error(e)),
                };
                if n == 0 {
                    self.done = true;
                    return Ok(false);
                }
                self.pending.extend_from_slice(&buf[..n]);
                Ok(true)
            }
        }
    }

    /// The next non-empty line (without its line ending), or `None` at the
    /// end of the body. A line longer than `max` bytes is an error.
    pub(crate) async fn next_line(&mut self, max: usize) -> KsqlResult<Option<String>> {
        let too_long = || {
            KsqlError::Response(format!(
                "a response line is larger than the {} MiB limit",
                max / (1024 * 1024)
            ))
        };
        loop {
            if let Some(pos) = self.pending.iter().position(|b| *b == b'\n') {
                if pos > max {
                    return Err(too_long());
                }
                let line: Vec<u8> = self.pending.drain(..=pos).collect();
                let text = String::from_utf8_lossy(&line);
                let text = text.trim_end_matches(['\r', '\n']);
                if text.trim().is_empty() {
                    continue;
                }
                return Ok(Some(text.to_string()));
            }
            if self.pending.len() > max {
                return Err(too_long());
            }
            if !self.fill().await? {
                let rest = std::mem::take(&mut self.pending);
                let text = String::from_utf8_lossy(&rest).trim().to_string();
                return Ok((!text.is_empty()).then_some(text));
            }
        }
    }

    /// The rest of the body as text, at most `max` bytes.
    pub(crate) async fn read_all(&mut self, max: usize) -> KsqlResult<String> {
        while self.fill().await? {
            if self.pending.len() > max {
                return Err(KsqlError::Response(format!(
                    "the response is larger than the {} MiB limit",
                    max / (1024 * 1024)
                )));
            }
        }
        Ok(String::from_utf8_lossy(&std::mem::take(&mut self.pending)).into_owned())
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// A local server that answers each connection with the next of
    /// `replies` (written in pieces, to exercise incremental reads) and
    /// returns the requests it received.
    pub(crate) async fn serve(
        replies: Vec<Vec<u8>>,
    ) -> (String, tokio::task::JoinHandle<Vec<String>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let handle = tokio::spawn(async move {
            let mut requests = Vec::new();
            for reply in replies {
                let (mut sock, _) = listener.accept().await.unwrap();
                let mut buf = vec![0u8; 64 * 1024];
                let mut request = Vec::new();
                // Read the head and the body announced by Content-Length.
                loop {
                    let n = sock.read(&mut buf).await.unwrap();
                    request.extend_from_slice(&buf[..n]);
                    let text = String::from_utf8_lossy(&request).to_string();
                    if let Some(end) = text.find("\r\n\r\n") {
                        let length = text
                            .lines()
                            .find_map(|l| l.strip_prefix("Content-Length: "))
                            .and_then(|v| v.trim().parse::<usize>().ok())
                            .unwrap_or(0);
                        if request.len() >= end + 4 + length {
                            break;
                        }
                    }
                    if n == 0 {
                        break;
                    }
                }
                requests.push(String::from_utf8_lossy(&request).to_string());
                for piece in reply.chunks(7) {
                    if sock.write_all(piece).await.is_err() {
                        break;
                    }
                    let _ = sock.flush().await;
                }
                let _ = sock.shutdown().await;
            }
            requests
        });
        (url, handle)
    }

    pub(crate) fn with_length(status: &str, body: &str) -> Vec<u8> {
        format!(
            "HTTP/1.1 {status}\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        )
        .into_bytes()
    }

    pub(crate) fn chunked(status: &str, lines: &[&str]) -> Vec<u8> {
        let mut out =
            format!("HTTP/1.1 {status}\r\nTransfer-Encoding: chunked\r\n\r\n").into_bytes();
        for line in lines {
            let data = format!("{line}\n");
            out.extend_from_slice(format!("{:x}\r\n{data}\r\n", data.len()).as_bytes());
        }
        out.extend_from_slice(b"0\r\n\r\n");
        out
    }

    #[tokio::test]
    async fn reads_chunked_lines_and_whole_bodies() {
        let (url, server) = serve(vec![
            chunked("200 OK", &["{\"a\":1}", "[1,\"x\"]", "[2,\"y\"]"]),
            b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\n[42]".to_vec(),
            with_length(
                "400 Bad Request",
                r#"{"@type":"statement_error","error_code":40001,"message":"no"}"#,
            ),
        ])
        .await;
        let config = KsqlConfig::new(&url).with_credentials("u", "p");
        let t = Transport::new(&config).unwrap();
        let mut r = t
            .open("POST", "/query-stream", "x", Some("{}"))
            .await
            .unwrap();
        assert_eq!(r.status, 200);
        let mut lines = Vec::new();
        while let Some(line) = r.body.next_line(1024).await.unwrap() {
            lines.push(line);
        }
        assert_eq!(lines, ["{\"a\":1}", "[1,\"x\"]", "[2,\"y\"]"]);
        assert_eq!(t.request("GET", "/info", "x", None).await.unwrap(), "[42]");
        let err = t
            .request("POST", "/ksql", "x", Some("{}"))
            .await
            .unwrap_err();
        assert_eq!(
            err,
            KsqlError::Api {
                status: 400,
                code: Some(40001),
                message: "no".into()
            }
        );
        let requests = server.await.unwrap();
        assert!(requests[0].starts_with("POST /query-stream HTTP/1.1\r\n"));
        assert!(requests[0].contains("Authorization: Basic dTpw\r\n"));
        assert!(requests[0].ends_with("\r\n\r\n{}"));
        assert!(requests[1].starts_with("GET /info HTTP/1.1\r\n"));
        assert!(!requests[1].contains("Content-Length"));
    }

    #[tokio::test]
    async fn lines_over_the_limit_and_unreachable_servers_fail() {
        let long = "x".repeat(4096);
        let (url, _server) = serve(vec![chunked("200 OK", &[&long])]).await;
        let t = Transport::new(&KsqlConfig::new(&url)).unwrap();
        let mut r = t.open("GET", "/", "x", None).await.unwrap();
        assert!(r.body.next_line(100).await.is_err());

        // Nothing listens on a port that was just freed.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let t = Transport::new(&KsqlConfig::new(&format!("http://127.0.0.1:{port}"))).unwrap();
        let err = t.request("GET", "/info", "x", None).await.unwrap_err();
        assert!(
            matches!(err, KsqlError::Unreachable { ref target, .. } if target == &format!("127.0.0.1:{port}")),
            "{err}"
        );
    }

    #[tokio::test]
    async fn a_silent_server_times_out() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let (_sock, _) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(30)).await;
        });
        let config = KsqlConfig::new(&format!("http://127.0.0.1:{port}"))
            .with_timeout(Duration::from_secs(1));
        let t = Transport::new(&config).unwrap();
        let started = std::time::Instant::now();
        let err = t.request("GET", "/info", "x", None).await.unwrap_err();
        assert!(err.to_string().contains("timed out after 1s"), "{err}");
        assert!(started.elapsed() < Duration::from_secs(10));
    }

    #[test]
    fn tls_files_need_https_and_errors_are_parsed() {
        let mut config = KsqlConfig::new("http://k:8088");
        config.ca_location = Some("ca.pem".into());
        assert!(Transport::new(&config).is_err());
        let t = Transport::new(&KsqlConfig::new("http://ops:pw@k:8088/prefix")).unwrap();
        let request = t.build_request("GET", "/info", "application/json", None);
        assert!(request.starts_with("GET /prefix/info HTTP/1.1\r\nHost: k:8088\r\n"));
        assert!(!format!("{t:?}").contains("Basic"));
        assert_eq!(
            api_error(401, ""),
            KsqlError::Api {
                status: 401,
                code: None,
                message: "unauthorized: check the ksqlDB user and password".into()
            }
        );
        assert_eq!(
            api_error(500, "oops"),
            KsqlError::Api {
                status: 500,
                code: None,
                message: "oops".into()
            }
        );
    }
}
