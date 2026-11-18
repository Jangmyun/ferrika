//! SASL OAUTHBEARER tokens: where they come from (OIDC client credentials,
//! AWS MSK IAM), a process-wide cache, and the SASL initial response.
//!
//! The same tokens feed both Kafka paths: librdkafka clients get them through
//! `rd_kafka_oauthbearer_set_token` (see [`crate::kafka::oauth_refresh`]) and
//! the native wire requests send them in a SaslAuthenticate request.

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use chrono::{DateTime, Utc};

use crate::error::{KafkaError, KafkaResult};
use crate::kafka::aws_sigv4::{self, AwsCredentials};

/// A bearer token with what librdkafka needs besides the value.
#[derive(Clone, PartialEq, Eq)]
pub struct OAuthToken {
    pub value: String,
    /// The Kafka principal the token stands for (client side only).
    pub principal: String,
    /// When the token was obtained, in ms since the Unix epoch.
    pub issued_at_ms: i64,
    /// When the token expires, in ms since the Unix epoch.
    pub expires_at_ms: i64,
    /// SASL extensions (`k=v`) sent with the token.
    pub extensions: BTreeMap<String, String>,
}

impl std::fmt::Debug for OAuthToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OAuthToken")
            .field("value", &"<redacted>")
            .field("principal", &self.principal)
            .field("issued_at_ms", &self.issued_at_ms)
            .field("expires_at_ms", &self.expires_at_ms)
            .field("extensions", &self.extensions)
            .finish()
    }
}

/// A token is refreshed once less than this much of its life is left...
const REFRESH_LEFT_FRACTION: i64 = 5; // 1/5 = 20 %
/// ...or less than this many milliseconds.
const REFRESH_LEFT_MIN_MS: i64 = 60_000;

impl OAuthToken {
    /// Total lifetime in ms (at least 1).
    pub fn lifetime_ms(&self) -> i64 {
        (self.expires_at_ms - self.issued_at_ms).max(1)
    }

    /// Whether the token should be replaced at `now_ms`: under 20 % of its
    /// life or under 60 s left.
    pub fn needs_refresh(&self, now_ms: i64) -> bool {
        let left = self.expires_at_ms - now_ms;
        left <= REFRESH_LEFT_MIN_MS || left * REFRESH_LEFT_FRACTION <= self.lifetime_ms()
    }

    /// When a background refresher should replace it: at 80 % of its life,
    /// and no later than 60 s before it expires.
    pub fn refresh_at_ms(&self) -> i64 {
        let at_80 = self.issued_at_ms + self.lifetime_ms() * 4 / 5;
        at_80.min(self.expires_at_ms - REFRESH_LEFT_MIN_MS)
    }

    /// The SASL OAUTHBEARER client initial response (RFC 7628):
    /// `n,,\x01auth=Bearer <token>\x01k=v\x01\x01`.
    pub fn sasl_initial_response(&self) -> Vec<u8> {
        let mut out = format!("n,,\x01auth=Bearer {}\x01", self.value);
        for (k, v) in &self.extensions {
            out.push_str(&format!("{k}={v}\x01"));
        }
        out.push('\x01');
        out.into_bytes()
    }
}

pub use crate::kafka::clock::now_ms;

/// Checks SASL extensions the way RFC 7628 (and librdkafka) do: keys are
/// letters only and not `auth`, values are printable ASCII, spaces or tabs.
pub fn validate_extensions(extensions: &BTreeMap<String, String>) -> KafkaResult<()> {
    for (k, v) in extensions {
        if k.is_empty() || !k.bytes().all(|b| b.is_ascii_alphabetic()) || k == "auth" {
            return Err(KafkaError::config(format!(
                "Invalid OAUTHBEARER extension name '{k}': letters only, not 'auth'"
            )));
        }
        if !v
            .bytes()
            .all(|b| (0x21..=0x7e).contains(&b) || matches!(b, b' ' | b'\t' | b'\r' | b'\n'))
        {
            return Err(KafkaError::config(format!(
                "Invalid value for OAUTHBEARER extension '{k}': printable ASCII only"
            )));
        }
    }
    Ok(())
}

/// Parses `k=v,k2=v2` (librdkafka's `sasl.oauthbearer.extensions` format).
pub fn parse_extensions(text: &str) -> KafkaResult<BTreeMap<String, String>> {
    let mut out = BTreeMap::new();
    for pair in text.split(',').map(str::trim).filter(|p| !p.is_empty()) {
        let (k, v) = pair.split_once('=').ok_or_else(|| {
            KafkaError::config(format!(
                "Invalid OAUTHBEARER extension '{pair}': expected key=value"
            ))
        })?;
        out.insert(k.trim().to_string(), v.trim().to_string());
    }
    validate_extensions(&out)?;
    Ok(out)
}

/// `k=v,k2=v2`, the inverse of [`parse_extensions`].
pub fn format_extensions(extensions: &BTreeMap<String, String>) -> String {
    extensions
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join(",")
}

/// Decodes base64url, with or without padding (JWT segments).
pub fn base64url_decode(text: &str) -> Option<Vec<u8>> {
    let text = text.trim_end_matches('=');
    let mut out = Vec::with_capacity(text.len() * 3 / 4);
    let mut acc = 0u32;
    let mut bits = 0;
    for c in text.bytes() {
        let v = match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a' + 26,
            b'0'..=b'9' => c - b'0' + 52,
            b'-' | b'+' => 62,
            b'_' | b'/' => 63,
            _ => return None,
        };
        acc = (acc << 6) | u32::from(v);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
            acc &= (1 << bits) - 1;
        }
    }
    Some(out)
}

/// The claims of a JWT (`header.claims.signature`), unverified: Ferrika only
/// reads `exp` and `sub` from its own token; the broker verifies it.
pub fn jwt_claims(token: &str) -> Option<serde_json::Map<String, serde_json::Value>> {
    let mut parts = token.split('.');
    let (_header, claims) = (parts.next()?, parts.next()?);
    parts.next()?;
    let bytes = base64url_decode(claims)?;
    match serde_json::from_slice(&bytes).ok()? {
        serde_json::Value::Object(map) => Some(map),
        _ => None,
    }
}

/// A boxed future, so [`TokenSource`] can be used as a trait object.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Where OAUTHBEARER tokens come from.
pub trait TokenSource: Send + Sync {
    /// Identifies the tokens this source hands out, for the shared cache.
    fn cache_key(&self) -> String;
    /// Gets a new token.
    fn fetch(&self) -> BoxFuture<'_, KafkaResult<OAuthToken>>;
}

/// Tokens by [`TokenSource::cache_key`], shared by every client in the
/// process so one OIDC token serves the admin, producer and consumer.
fn cache() -> &'static Mutex<HashMap<String, OAuthToken>> {
    static CACHE: OnceLock<Mutex<HashMap<String, OAuthToken>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// A cached token that does not need a refresh yet, else a new one.
pub async fn cached_token(source: &dyn TokenSource) -> KafkaResult<OAuthToken> {
    let key = source.cache_key();
    let cached = cache()
        .lock()
        .ok()
        .and_then(|map| map.get(&key).cloned())
        .filter(|token| !token.needs_refresh(now_ms()));
    if let Some(token) = cached {
        return Ok(token);
    }
    let token = source.fetch().await?;
    if let Ok(mut map) = cache().lock() {
        map.insert(key, token.clone());
    }
    Ok(token)
}

/// Drops the cached token of `source` (after the broker rejected it).
pub fn forget(source: &dyn TokenSource) {
    if let Ok(mut map) = cache().lock() {
        map.remove(&source.cache_key());
    }
}

/// Resolves AWS credentials (environment, shared files, `credential_process`).
pub type CredentialResolver = Arc<dyn Fn() -> KafkaResult<AwsCredentials> + Send + Sync>;

/// AWS MSK IAM: a SigV4-presigned `kafka-cluster:Connect` URL as the token.
pub struct MskIamSource {
    region: String,
    resolver: CredentialResolver,
    credentials: Mutex<AwsCredentials>,
}

/// Temporary credentials are resolved again this long before they expire.
const CREDENTIALS_EARLY_MS: i64 = 5 * 60_000;

impl MskIamSource {
    /// Resolves credentials once up front, so a missing key fails here.
    pub fn new(region: impl Into<String>, resolver: CredentialResolver) -> KafkaResult<Self> {
        let credentials = resolver()?;
        Ok(Self {
            region: region.into(),
            resolver,
            credentials: Mutex::new(credentials),
        })
    }

    /// The stored credentials, resolved again when they are about to expire.
    fn current_credentials(&self) -> KafkaResult<AwsCredentials> {
        let stored = self
            .credentials
            .lock()
            .map_err(|_| KafkaError::Other("AWS credential lock poisoned".to_string()))?
            .clone();
        let expiring = stored
            .expiration
            .is_some_and(|exp| exp.timestamp_millis() - now_ms() < CREDENTIALS_EARLY_MS);
        if !expiring {
            return Ok(stored);
        }
        let fresh = (self.resolver)()?;
        if let Ok(mut slot) = self.credentials.lock() {
            *slot = fresh.clone();
        }
        Ok(fresh)
    }

    /// A token signed at `now` with `credentials`.
    pub fn token_at(credentials: &AwsCredentials, region: &str, now: DateTime<Utc>) -> OAuthToken {
        let issued = now.timestamp_millis();
        let mut expires = issued + aws_sigv4::MSK_TOKEN_EXPIRES_SECS as i64 * 1000;
        if let Some(exp) = credentials.expiration {
            expires = expires.min(exp.timestamp_millis());
        }
        OAuthToken {
            value: aws_sigv4::msk_token(credentials, region, now),
            principal: credentials.access_key_id.clone(),
            issued_at_ms: issued,
            expires_at_ms: expires,
            extensions: BTreeMap::new(),
        }
    }
}

impl TokenSource for MskIamSource {
    fn cache_key(&self) -> String {
        let access_key = self
            .credentials
            .lock()
            .map(|c| c.access_key_id.clone())
            .unwrap_or_default();
        format!("msk-iam|{access_key}|{}", self.region)
    }

    fn fetch(&self) -> BoxFuture<'_, KafkaResult<OAuthToken>> {
        Box::pin(async move {
            let credentials = self.current_credentials()?;
            let token = Self::token_at(&credentials, &self.region, Utc::now());
            if token.expires_at_ms <= now_ms() {
                return Err(KafkaError::config(
                    "The AWS credentials have expired; refresh them (aws sso login, ...)",
                ));
            }
            Ok(token)
        })
    }
}

/// How the client authenticates to the token endpoint (RFC 6749 2.3.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OAuthClientAuth {
    /// HTTP Basic with the form-encoded client id and secret.
    #[default]
    Basic,
    /// `client_id` and `client_secret` in the form body.
    Post,
}

/// `application/x-www-form-urlencoded` encoding of one value.
pub fn form_encode(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for b in text.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'*' => {
                out.push(b as char)
            }
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Token lifetime assumed when the endpoint gives neither `expires_in`
/// nor a JWT `exp`.
const DEFAULT_TOKEN_LIFETIME_MS: i64 = 3_600_000;

/// OIDC / OAuth 2.0 client credentials grant against a token endpoint.
#[derive(Clone)]
pub struct OidcSource {
    pub token_endpoint: String,
    pub client_id: String,
    pub client_secret: String,
    pub scope: Option<String>,
    pub client_auth: OAuthClientAuth,
    pub extensions: BTreeMap<String, String>,
    /// Bound on the whole token request.
    pub timeout: Duration,
}

impl OidcSource {
    /// The token endpoint without `user:password@`, for messages.
    fn shown_endpoint(&self) -> String {
        crate::model::format::redact_url(&self.token_endpoint)
    }
}

/// Whether `host` is this machine: `localhost` or a loopback address.
fn is_loopback_host(host: &str) -> bool {
    let host = host.trim_end_matches('.');
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
}

/// Refuses a token endpoint that would send the client id and secret in the
/// clear: it must be `https://`, except on this machine (`localhost`,
/// `127.0.0.1`, `::1`), where nothing crosses a network. A URL with no scheme
/// counts as plain HTTP. Messages never carry the URL's user and password.
pub fn validate_token_endpoint(url: &str) -> KafkaResult<()> {
    let endpoint = crate::kafka::http::Endpoint::parse(url, 80)?;
    if endpoint.tls || is_loopback_host(&endpoint.host) {
        return Ok(());
    }
    Err(KafkaError::config(format!(
        "oauth_token_endpoint {} is not https: the client secret would be sent unencrypted \
         (plain http is only allowed for localhost, 127.0.0.1 and ::1)",
        crate::model::format::redact_url(url)
    )))
}

impl std::fmt::Debug for OidcSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OidcSource")
            .field("token_endpoint", &self.shown_endpoint())
            .field("client_id", &self.client_id)
            .field("client_secret", &"<redacted>")
            .field("scope", &self.scope)
            .field("client_auth", &self.client_auth)
            .field("extensions", &self.extensions)
            .field("timeout", &self.timeout)
            .finish()
    }
}

impl OidcSource {
    /// The form body and extra headers of the token request.
    fn request_parts(&self) -> (String, Vec<(&'static str, String)>) {
        let mut body = "grant_type=client_credentials".to_string();
        if let Some(scope) = self.scope.as_deref().filter(|s| !s.trim().is_empty()) {
            body.push_str(&format!("&scope={}", form_encode(scope.trim())));
        }
        let mut headers = Vec::new();
        match self.client_auth {
            OAuthClientAuth::Basic => {
                let pair = format!(
                    "{}:{}",
                    form_encode(&self.client_id),
                    form_encode(&self.client_secret)
                );
                headers.push((
                    "Authorization",
                    format!("Basic {}", crate::codec::base64::encode(pair.as_bytes())),
                ));
            }
            OAuthClientAuth::Post => {
                body.push_str(&format!(
                    "&client_id={}&client_secret={}",
                    form_encode(&self.client_id),
                    form_encode(&self.client_secret)
                ));
            }
        }
        (body, headers)
    }

    /// Turns a token endpoint response into a token obtained at `now`.
    pub fn parse_token_response(
        &self,
        response: &crate::kafka::http::Response,
        now: i64,
    ) -> KafkaResult<OAuthToken> {
        let json: Option<serde_json::Value> = serde_json::from_str(&response.body).ok();
        if !response.is_success() {
            let reason = json
                .as_ref()
                .and_then(|v| {
                    let error = v.get("error")?.as_str()?;
                    Some(match v.get("error_description").and_then(|d| d.as_str()) {
                        Some(desc) => format!("{error}: {desc}"),
                        None => error.to_string(),
                    })
                })
                .unwrap_or_else(|| response.status_line.clone());
            return Err(KafkaError::config(format!(
                "OAuth token endpoint {} refused the client credentials: {reason}",
                self.shown_endpoint()
            )));
        }
        let json = json.ok_or_else(|| {
            KafkaError::config(format!(
                "OAuth token endpoint {} did not answer with JSON",
                self.shown_endpoint()
            ))
        })?;
        let value = json
            .get("access_token")
            .and_then(|v| v.as_str())
            .filter(|v| !v.is_empty())
            .ok_or_else(|| {
                KafkaError::config(format!(
                    "OAuth token endpoint {} returned no access_token",
                    self.shown_endpoint()
                ))
            })?
            .to_string();
        let claims = jwt_claims(&value);
        let expires_in_ms = json
            .get("expires_in")
            .and_then(|v| v.as_i64().or_else(|| v.as_str()?.trim().parse().ok()))
            .filter(|s| *s > 0)
            .map(|s| s.saturating_mul(1000));
        let jwt_exp_ms = claims
            .as_ref()
            .and_then(|c| c.get("exp")?.as_i64())
            .map(|s| s.saturating_mul(1000));
        let expires_at_ms = match (expires_in_ms, jwt_exp_ms) {
            (Some(ms), _) => now.saturating_add(ms),
            (None, Some(exp)) => exp,
            (None, None) => {
                tracing::warn!(
                    "OAuth token from {} has no expires_in or exp; assuming one hour",
                    self.shown_endpoint()
                );
                now + DEFAULT_TOKEN_LIFETIME_MS
            }
        };
        if expires_at_ms <= now {
            return Err(KafkaError::config(format!(
                "OAuth token endpoint {} returned an expired token",
                self.shown_endpoint()
            )));
        }
        let principal = claims
            .as_ref()
            .and_then(|c| c.get("sub")?.as_str().map(str::to_string))
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| self.client_id.clone());
        Ok(OAuthToken {
            value,
            principal,
            issued_at_ms: now,
            expires_at_ms,
            extensions: self.extensions.clone(),
        })
    }
}

impl TokenSource for OidcSource {
    fn cache_key(&self) -> String {
        format!(
            "oidc|{}|{}|{}",
            self.token_endpoint,
            self.client_id,
            self.scope.as_deref().unwrap_or_default()
        )
    }

    fn fetch(&self) -> BoxFuture<'_, KafkaResult<OAuthToken>> {
        Box::pin(async move {
            // Checked again here: the client secret goes in this request.
            validate_token_endpoint(&self.token_endpoint)?;
            let endpoint = crate::kafka::http::Endpoint::parse(&self.token_endpoint, 80)?;
            let (body, headers) = self.request_parts();
            let headers: Vec<(&str, &str)> =
                headers.iter().map(|(k, v)| (*k, v.as_str())).collect();
            let request = endpoint.send_with_headers(
                "POST",
                "",
                "application/x-www-form-urlencoded",
                Some(&body),
                &headers,
            );
            let response = tokio::time::timeout(self.timeout, request)
                .await
                .map_err(|_| {
                    KafkaError::Timeout(format!(
                        "No OAuth token from {} within {:.1}s",
                        self.shown_endpoint(),
                        self.timeout.as_secs_f32()
                    ))
                })??;
            self.parse_token_response(&response, now_ms())
        })
    }
}

/// The token source a client configuration describes. For MSK IAM this
/// resolves the region and the AWS credentials, so missing ones fail here.
pub fn source_for(
    config: &crate::kafka::config::KafkaClientConfig,
) -> KafkaResult<Arc<dyn TokenSource>> {
    use crate::kafka::aws_credentials;
    use crate::kafka::config::OAuthMethod;
    let oauth = &config.oauth;
    let required = |v: &Option<String>, name: &str| {
        v.clone()
            .filter(|v| !v.trim().is_empty())
            .ok_or_else(|| KafkaError::config(format!("SASL OAUTHBEARER (OIDC) needs {name}")))
    };
    match oauth.method() {
        OAuthMethod::Oidc => Ok(Arc::new(OidcSource {
            token_endpoint: required(&oauth.token_endpoint, "oauth_token_endpoint")?,
            client_id: required(&oauth.client_id, "oauth_client_id")?,
            client_secret: required(&oauth.client_secret, "oauth_client_secret")?,
            scope: oauth.scope.clone(),
            client_auth: oauth.client_auth.unwrap_or_default(),
            extensions: oauth.extensions.clone(),
            timeout: config.request_timeout,
        })),
        OAuthMethod::AwsMskIam => {
            let lookup = aws_credentials::default_lookup();
            let region = aws_credentials::resolve_region(
                &lookup,
                oauth.aws_region.as_deref(),
                oauth.aws_profile.as_deref(),
                &config.bootstrap_servers,
            )?;
            Ok(Arc::new(MskIamSource::new(
                region,
                aws_credentials::resolver(oauth.aws_profile.clone()),
            )?))
        }
    }
}

/// [`cached_token`] for synchronous callers (the wire client, client
/// constructors): runs on its own thread and runtime, so it works whether
/// or not the caller is inside a Tokio runtime.
pub fn blocking_token(source: &dyn TokenSource) -> KafkaResult<OAuthToken> {
    std::thread::scope(|scope| {
        scope
            .spawn(|| {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|e| KafkaError::Other(format!("Cannot start a runtime: {e}")))?;
                runtime.block_on(cached_token(source))
            })
            .join()
            .unwrap_or_else(|_| Err(KafkaError::Other("OAuth token thread panicked".to_string())))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    /// A one-shot token endpoint: reads one request (headers and
    /// Content-Length body), answers `reply`, and hands the request back.
    async fn mock_endpoint(
        reply: &'static str,
    ) -> (String, tokio::sync::oneshot::Receiver<String>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (tx, rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = Vec::new();
            let mut chunk = [0u8; 1024];
            loop {
                let n = sock.read(&mut chunk).await.unwrap();
                if n == 0 {
                    break;
                }
                buf.extend_from_slice(&chunk[..n]);
                let text = String::from_utf8_lossy(&buf).to_string();
                if let Some(end) = text.find("\r\n\r\n") {
                    let len: usize = text
                        .lines()
                        .find_map(|l| l.strip_prefix("Content-Length: "))
                        .and_then(|v| v.trim().parse().ok())
                        .unwrap_or(0);
                    if buf.len() >= end + 4 + len {
                        break;
                    }
                }
            }
            let _ = sock.write_all(reply.as_bytes()).await;
            let _ = tx.send(String::from_utf8_lossy(&buf).to_string());
        });
        (format!("http://127.0.0.1:{port}/realms/k/token"), rx)
    }

    fn oidc(endpoint: String, auth: OAuthClientAuth) -> OidcSource {
        OidcSource {
            token_endpoint: endpoint,
            client_id: "svc client".into(),
            client_secret: "s3cr:t".into(),
            scope: Some("kafka read".into()),
            client_auth: auth,
            extensions: BTreeMap::new(),
            timeout: Duration::from_secs(5),
        }
    }

    #[tokio::test]
    async fn oidc_client_secret_basic_request_and_expires_in() {
        let (url, rx) = mock_endpoint(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n\
             {\"access_token\":\"opaque-1\",\"token_type\":\"Bearer\",\"expires_in\":300}",
        )
        .await;
        let source = oidc(url, OAuthClientAuth::Basic);
        let before = now_ms();
        let token = source.fetch().await.unwrap();
        let request = rx.await.unwrap();
        assert!(
            request.starts_with("POST /realms/k/token HTTP/1.1\r\n"),
            "{request}"
        );
        // base64("svc+client:s3cr%3At")
        let basic = crate::codec::base64::encode(b"svc+client:s3cr%3At");
        assert!(
            request.contains(&format!("Authorization: Basic {basic}\r\n")),
            "{request}"
        );
        assert!(request.contains("Content-Type: application/x-www-form-urlencoded"));
        assert!(
            request.ends_with("\r\n\r\ngrant_type=client_credentials&scope=kafka+read"),
            "{request}"
        );
        assert_eq!(token.value, "opaque-1");
        assert_eq!(token.principal, "svc client");
        let life = token.expires_at_ms - token.issued_at_ms;
        assert_eq!(life, 300_000);
        assert!(token.issued_at_ms >= before);
    }

    #[tokio::test]
    async fn oidc_client_secret_post_and_jwt_exp_sub() {
        let header = aws_sigv4::base64url(br#"{"alg":"none"}"#);
        let exp = now_ms() / 1000 + 600;
        let claims = aws_sigv4::base64url(format!(r#"{{"sub":"svc-42","exp":{exp}}}"#).as_bytes());
        let reply: &'static str = Box::leak(
            format!("HTTP/1.1 200 OK\r\n\r\n{{\"access_token\":\"{header}.{claims}.\"}}")
                .into_boxed_str(),
        );
        let (url, rx) = mock_endpoint(reply).await;
        let token = oidc(url, OAuthClientAuth::Post).fetch().await.unwrap();
        let request = rx.await.unwrap();
        assert!(!request.contains("Authorization:"), "{request}");
        assert!(
            request.ends_with(
                "grant_type=client_credentials&scope=kafka+read\
                 &client_id=svc+client&client_secret=s3cr%3At"
            ),
            "{request}"
        );
        assert_eq!(token.principal, "svc-42");
        assert_eq!(token.expires_at_ms, exp * 1000);
    }

    #[tokio::test]
    async fn oidc_reports_endpoint_errors() {
        let (url, _rx) = mock_endpoint(
            "HTTP/1.1 401 Unauthorized\r\n\r\n\
             {\"error\":\"invalid_client\",\"error_description\":\"bad secret\"}",
        )
        .await;
        let err = oidc(url, OAuthClientAuth::Basic).fetch().await.unwrap_err();
        let text = err.to_string();
        assert!(text.contains("invalid_client: bad secret"), "{text}");
        assert!(!text.contains("s3cr"), "{text}");

        let (url, _rx) = mock_endpoint("HTTP/1.1 200 OK\r\n\r\n{\"token_type\":\"Bearer\"}").await;
        let err = oidc(url, OAuthClientAuth::Basic).fetch().await.unwrap_err();
        assert!(err.to_string().contains("no access_token"), "{err}");
    }

    #[tokio::test]
    async fn oidc_request_is_bounded_by_the_timeout() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let (_sock, _) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(10)).await;
        });
        let mut source = oidc(format!("http://127.0.0.1:{port}/t"), OAuthClientAuth::Basic);
        source.timeout = Duration::from_millis(200);
        let err = source.fetch().await.unwrap_err();
        assert!(matches!(err, KafkaError::Timeout(_)), "{err}");
    }

    #[test]
    fn token_endpoints_must_be_https_except_on_loopback() {
        for ok in [
            "https://idp.example.com/token",
            "HTTPS://idp.example.com:8443/t",
            "http://localhost:8080/t",
            "http://127.0.0.1:9000/t",
            "http://127.0.0.5/t",
            "http://[::1]:9000/t",
        ] {
            assert!(validate_token_endpoint(ok).is_ok(), "{ok}");
        }
        for bad in [
            "http://idp.example.com/token",
            "idp.example.com/token",
            "http://10.0.0.5/token",
            "http://localhost.evil.example/t",
            "http://KEY:s3cr3t@idp.example.com/token",
        ] {
            let err = validate_token_endpoint(bad).unwrap_err().to_string();
            assert!(err.contains("not https"), "{bad}: {err}");
            assert!(!err.contains("s3cr3t") && !err.contains("KEY"), "{err}");
        }
    }

    #[test]
    fn oidc_messages_and_debug_carry_no_url_credentials() {
        let source = oidc(
            "https://KEY:s3cr3t@idp.example.com/token".into(),
            OAuthClientAuth::Basic,
        );
        assert!(!format!("{source:?}").contains("s3cr3t"));
        let err = source
            .parse_token_response(
                &crate::kafka::http::Response {
                    status: 200,
                    status_line: "HTTP/1.1 200 OK".into(),
                    body: "{\"token_type\":\"Bearer\"}".into(),
                },
                1,
            )
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("idp.example.com") && !err.contains("s3cr3t"),
            "{err}"
        );
    }

    #[test]
    fn oidc_debug_hides_the_secret() {
        let shown = format!("{:?}", oidc("http://x".into(), OAuthClientAuth::Basic));
        assert!(!shown.contains("s3cr"), "{shown}");
        assert_eq!(form_encode("a b&c=d~"), "a+b%26c%3Dd%7E");
    }

    fn token(issued: i64, expires: i64) -> OAuthToken {
        OAuthToken {
            value: "abc".into(),
            principal: "p".into(),
            issued_at_ms: issued,
            expires_at_ms: expires,
            extensions: BTreeMap::new(),
        }
    }

    #[test]
    fn refresh_window_is_20_percent_or_60_seconds() {
        // One hour token: refresh in the last 12 minutes.
        let t = token(0, 3_600_000);
        assert!(!t.needs_refresh(0));
        assert!(!t.needs_refresh(2_879_999));
        assert!(t.needs_refresh(2_880_000));
        assert_eq!(t.refresh_at_ms(), 2_880_000);
        // Two minute token: 20 % is 24 s, so the 60 s floor wins.
        let t = token(0, 120_000);
        assert!(!t.needs_refresh(59_999));
        assert!(t.needs_refresh(60_001));
        assert_eq!(t.refresh_at_ms(), 60_000);
        // Already expired.
        assert!(token(0, 10).needs_refresh(20));
    }

    #[test]
    fn initial_response_follows_rfc7628() {
        let mut t = token(0, 1);
        t.value = "eyJ.x.y".into();
        assert_eq!(
            t.sasl_initial_response(),
            b"n,,\x01auth=Bearer eyJ.x.y\x01\x01"
        );
        t.extensions.insert("traceId".into(), "123".into());
        t.extensions.insert("logicalCluster".into(), "lkc-1".into());
        assert_eq!(
            t.sasl_initial_response(),
            b"n,,\x01auth=Bearer eyJ.x.y\x01logicalCluster=lkc-1\x01traceId=123\x01\x01"
        );
    }

    #[test]
    fn extensions_parse_and_validate() {
        let ext = parse_extensions("logicalCluster=lkc-1, identityPoolId=pool-2").unwrap();
        assert_eq!(
            ext.get("identityPoolId").map(String::as_str),
            Some("pool-2")
        );
        assert_eq!(
            format_extensions(&ext),
            "identityPoolId=pool-2,logicalCluster=lkc-1"
        );
        assert!(parse_extensions("").unwrap().is_empty());
        assert!(parse_extensions("auth=x").is_err());
        assert!(parse_extensions("bad-key=x").is_err());
        assert!(parse_extensions("novalue").is_err());
        assert!(parse_extensions("k=\u{1}").is_err());
    }

    #[test]
    fn reads_jwt_claims_without_verifying() {
        // {"alg":"none"} . {"sub":"svc-1","exp":1700000000} . (empty)
        let header = aws_sigv4::base64url(br#"{"alg":"none"}"#);
        let claims = aws_sigv4::base64url(br#"{"sub":"svc-1","exp":1700000000}"#);
        let jwt = format!("{header}.{claims}.");
        let map = jwt_claims(&jwt).unwrap();
        assert_eq!(map["sub"], "svc-1");
        assert_eq!(map["exp"], 1_700_000_000);
        assert!(jwt_claims("opaque-token").is_none());
        assert!(jwt_claims("a.!!!.c").is_none());
        assert_eq!(base64url_decode("Zm9vYmE=").unwrap(), b"fooba");
        assert_eq!(base64url_decode("-_8").unwrap(), [0xfb, 0xff]);
    }

    #[test]
    fn msk_token_lifetime_is_capped_by_the_credentials() {
        let now = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let mut credentials = AwsCredentials::new("AKIDX", "secret");
        let t = MskIamSource::token_at(&credentials, "us-east-1", now);
        assert_eq!(t.expires_at_ms - t.issued_at_ms, 900_000);
        assert_eq!(t.principal, "AKIDX");
        let url = String::from_utf8(base64url_decode(&t.value).unwrap()).unwrap();
        assert!(url.starts_with("https://kafka.us-east-1.amazonaws.com/?Action="));

        credentials.expiration = Some(now + chrono::Duration::seconds(300));
        let t = MskIamSource::token_at(&credentials, "us-east-1", now);
        assert_eq!(t.expires_at_ms - t.issued_at_ms, 300_000);
    }

    /// A source counting its fetches, for the cache tests.
    struct Counting {
        key: String,
        lifetime_ms: i64,
        fetches: std::sync::atomic::AtomicUsize,
    }

    impl TokenSource for Counting {
        fn cache_key(&self) -> String {
            self.key.clone()
        }
        fn fetch(&self) -> BoxFuture<'_, KafkaResult<OAuthToken>> {
            Box::pin(async move {
                let n = self
                    .fetches
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let now = now_ms();
                let mut t = token(now, now + self.lifetime_ms);
                t.value = format!("token-{n}");
                Ok(t)
            })
        }
    }

    #[tokio::test]
    async fn cache_reuses_fresh_tokens_and_refetches_stale_ones() {
        let fresh = Counting {
            key: format!("test-fresh-{}", std::process::id()),
            lifetime_ms: 3_600_000,
            fetches: Default::default(),
        };
        let a = cached_token(&fresh).await.unwrap();
        let b = cached_token(&fresh).await.unwrap();
        assert_eq!(a.value, b.value);
        assert_eq!(fresh.fetches.load(std::sync::atomic::Ordering::SeqCst), 1);
        forget(&fresh);
        assert_eq!(cached_token(&fresh).await.unwrap().value, "token-1");

        // A 30 s token is always inside the 60 s refresh floor.
        let short = Counting {
            key: format!("test-short-{}", std::process::id()),
            lifetime_ms: 30_000,
            fetches: Default::default(),
        };
        cached_token(&short).await.unwrap();
        cached_token(&short).await.unwrap();
        assert_eq!(short.fetches.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn msk_source_resolves_credentials_and_signs() {
        let resolver: CredentialResolver = Arc::new(|| Ok(AwsCredentials::new("AKIDM", "s")));
        let source = MskIamSource::new("ap-northeast-2", resolver).unwrap();
        assert_eq!(source.cache_key(), "msk-iam|AKIDM|ap-northeast-2");
        let t = source.fetch().await.unwrap();
        assert!(t.expires_at_ms > now_ms());

        let failing: CredentialResolver =
            Arc::new(|| Err(KafkaError::config("no AWS credentials")));
        assert!(MskIamSource::new("us-east-1", failing).is_err());
    }
}
