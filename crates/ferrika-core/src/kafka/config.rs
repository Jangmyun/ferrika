use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use rdkafka::ClientConfig;
use serde::{Deserialize, Serialize};

use crate::error::{KafkaError, KafkaResult};

/// Initial `reconnect.backoff.ms` of every client (librdkafka's default: 100).
const RECONNECT_BACKOFF_MS: &str = "10";

/// Supported Kafka security protocols.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SecurityProtocol {
    #[default]
    Plaintext,
    Ssl,
    SaslPlaintext,
    SaslSsl,
}

impl SecurityProtocol {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Plaintext => "plaintext",
            Self::Ssl => "ssl",
            Self::SaslPlaintext => "sasl_plaintext",
            Self::SaslSsl => "sasl_ssl",
        }
    }
}

/// Supported SASL authentication mechanisms.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SaslMechanism {
    Plain,
    ScramSha256,
    ScramSha512,
    Gssapi,
    Oauthbearer,
}

impl SaslMechanism {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Plain => "PLAIN",
            Self::ScramSha256 => "SCRAM-SHA-256",
            Self::ScramSha512 => "SCRAM-SHA-512",
            Self::Gssapi => "GSSAPI",
            Self::Oauthbearer => "OAUTHBEARER",
        }
    }
}

/// Where OAUTHBEARER tokens come from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum OAuthMethod {
    /// OIDC / OAuth 2.0 client credentials grant against a token endpoint.
    #[default]
    Oidc,
    /// AWS MSK IAM: a SigV4-presigned URL from AWS credentials.
    AwsMskIam,
}

impl OAuthMethod {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Oidc => "oidc",
            Self::AwsMskIam => "aws_msk_iam",
        }
    }
}

pub use crate::kafka::oauth::OAuthClientAuth;

/// SASL OAUTHBEARER settings (used when `sasl_mechanism` is OAUTHBEARER).
#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OAuthConfig {
    /// Token source; OIDC when unset.
    pub method: Option<OAuthMethod>,
    /// OIDC token endpoint URL.
    pub token_endpoint: Option<String>,
    /// OIDC client id.
    pub client_id: Option<String>,
    /// OIDC client secret.
    pub client_secret: Option<String>,
    /// How the client authenticates to the token endpoint (Basic when unset).
    pub client_auth: Option<OAuthClientAuth>,
    /// OIDC scope.
    pub scope: Option<String>,
    /// SASL extensions sent with the token.
    pub extensions: BTreeMap<String, String>,
    /// AWS region for MSK IAM (else AWS_REGION or the bootstrap host).
    pub aws_region: Option<String>,
    /// AWS shared-config profile for MSK IAM (else AWS_PROFILE / default).
    pub aws_profile: Option<String>,
}

impl OAuthConfig {
    /// The token source, OIDC when unset.
    pub fn method(&self) -> OAuthMethod {
        self.method.unwrap_or_default()
    }
}

impl std::fmt::Debug for OAuthConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OAuthConfig")
            .field("method", &self.method)
            .field("token_endpoint", &self.token_endpoint)
            .field("client_id", &self.client_id)
            .field(
                "client_secret",
                &self.client_secret.as_ref().map(|_| "<redacted>"),
            )
            .field("client_auth", &self.client_auth)
            .field("scope", &self.scope)
            .field("extensions", &self.extensions)
            .field("aws_region", &self.aws_region)
            .field("aws_profile", &self.aws_profile)
            .finish()
    }
}

/// Reset policy for consumer offset when no initial offset is found.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum AutoOffsetReset {
    #[default]
    Earliest,
    Latest,
    Error,
}

impl AutoOffsetReset {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Earliest => "earliest",
            Self::Latest => "latest",
            Self::Error => "error",
        }
    }
}

/// Comprehensive Kafka client configuration for Admin, Producer, and Consumer.
#[derive(Clone, Serialize, Deserialize)]
pub struct KafkaClientConfig {
    /// Comma-separated list of host:port broker endpoints.
    pub bootstrap_servers: String,
    /// Identifier for this client instance.
    pub client_id: Option<String>,
    /// Consumer group identifier (if consuming).
    pub group_id: Option<String>,
    /// Protocol used to communicate with brokers.
    pub security_protocol: SecurityProtocol,
    /// SASL mechanism when using SASL.
    pub sasl_mechanism: Option<SaslMechanism>,
    /// Username for SASL authentication.
    pub sasl_username: Option<String>,
    /// Password for SASL authentication.
    pub sasl_password: Option<String>,
    /// File or directory path to CA certificate(s) for verifying broker's key.
    pub ssl_ca_location: Option<String>,
    /// Path to client's public certificate.
    pub ssl_certificate_location: Option<String>,
    /// Path to client's private key.
    pub ssl_key_location: Option<String>,
    /// Password for client's private key.
    pub ssl_key_password: Option<String>,
    /// Request timeout for broker requests.
    pub request_timeout: Duration,
    /// Refuse operations that change the cluster (from a read-only profile).
    pub read_only: bool,
    /// Confluent Schema Registry base URL, used to decode payloads.
    pub schema_registry_url: Option<String>,
    /// Kafka Connect REST URL.
    pub connect_url: Option<String>,
    /// Maximum time message delivery can take before failing.
    pub message_timeout: Duration,
    /// Timeout for network socket operations.
    pub socket_timeout: Duration,
    /// Consumer session timeout.
    pub session_timeout: Duration,
    /// Offset reset behavior for consumer.
    pub auto_offset_reset: AutoOffsetReset,
    /// Whether the consumer should automatically commit offsets.
    pub enable_auto_commit: bool,
    /// Frequency in milliseconds at which the consumer commits offsets.
    pub auto_commit_interval_ms: u64,
    /// Additional librdkafka key-value configuration overrides.
    pub extra_properties: HashMap<String, String>,
    /// SASL OAUTHBEARER settings.
    #[serde(default)]
    pub oauth: OAuthConfig,
}

/// Whether an extra librdkafka property holds a credential
/// (`sasl.jaas.config`, `ssl.keystore.password`, `sasl.oauthbearer.client.secret`, ...).
fn is_secret_property(key: &str) -> bool {
    let key = key.to_ascii_lowercase();
    ["password", "secret", "jaas", "token", "credential"]
        .iter()
        .any(|word| key.contains(word))
}

// Manual so passwords never reach logs or error output.
impl std::fmt::Debug for KafkaClientConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        const HIDDEN: &str = "<redacted>";
        let redact = |v: &Option<String>| v.as_ref().map(|_| HIDDEN);
        let mask = |v: &Option<String>| {
            v.as_deref()
                .map(|u| crate::kafka::profile::mask_url_password(u, HIDDEN))
        };
        let extra: std::collections::BTreeMap<&str, &str> = self
            .extra_properties
            .iter()
            .map(|(k, v)| {
                let shown = if is_secret_property(k) {
                    HIDDEN
                } else {
                    v.as_str()
                };
                (k.as_str(), shown)
            })
            .collect();
        f.debug_struct("KafkaClientConfig")
            .field("bootstrap_servers", &self.bootstrap_servers)
            .field("client_id", &self.client_id)
            .field("group_id", &self.group_id)
            .field("security_protocol", &self.security_protocol)
            .field("sasl_mechanism", &self.sasl_mechanism)
            .field("sasl_username", &self.sasl_username)
            .field("sasl_password", &redact(&self.sasl_password))
            .field("ssl_ca_location", &self.ssl_ca_location)
            .field("ssl_certificate_location", &self.ssl_certificate_location)
            .field("ssl_key_location", &self.ssl_key_location)
            .field("ssl_key_password", &redact(&self.ssl_key_password))
            .field("request_timeout", &self.request_timeout)
            .field("read_only", &self.read_only)
            .field("schema_registry_url", &mask(&self.schema_registry_url))
            .field("connect_url", &mask(&self.connect_url))
            .field("message_timeout", &self.message_timeout)
            .field("socket_timeout", &self.socket_timeout)
            .field("session_timeout", &self.session_timeout)
            .field("auto_offset_reset", &self.auto_offset_reset)
            .field("enable_auto_commit", &self.enable_auto_commit)
            .field("auto_commit_interval_ms", &self.auto_commit_interval_ms)
            .field("extra_properties", &extra)
            .field("oauth", &self.oauth)
            .finish()
    }
}

impl Default for KafkaClientConfig {
    fn default() -> Self {
        Self::new("localhost:9092")
    }
}

impl KafkaClientConfig {
    /// Creates a new configuration with default settings pointing to the provided bootstrap servers.
    pub fn new(bootstrap_servers: impl Into<String>) -> Self {
        Self {
            bootstrap_servers: bootstrap_servers.into(),
            client_id: Some("ferrika-client".to_string()),
            group_id: None,
            security_protocol: SecurityProtocol::Plaintext,
            sasl_mechanism: None,
            sasl_username: None,
            sasl_password: None,
            ssl_ca_location: None,
            ssl_certificate_location: None,
            ssl_key_location: None,
            ssl_key_password: None,
            request_timeout: Duration::from_secs(30),
            read_only: false,
            schema_registry_url: None,
            connect_url: None,
            message_timeout: Duration::from_secs(30),
            socket_timeout: Duration::from_secs(60),
            session_timeout: Duration::from_secs(45),
            auto_offset_reset: AutoOffsetReset::Earliest,
            enable_auto_commit: true,
            auto_commit_interval_ms: 5000,
            extra_properties: HashMap::new(),
            oauth: OAuthConfig::default(),
        }
    }

    /// Sets the client ID.
    pub fn with_client_id(mut self, client_id: impl Into<String>) -> Self {
        self.client_id = Some(client_id.into());
        self
    }

    /// Sets the consumer group ID.
    pub fn with_group_id(mut self, group_id: impl Into<String>) -> Self {
        self.group_id = Some(group_id.into());
        self
    }

    /// Configures SASL/PLAIN authentication.
    pub fn with_sasl_plain(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
        use_ssl: bool,
    ) -> Self {
        self.security_protocol = if use_ssl {
            SecurityProtocol::SaslSsl
        } else {
            SecurityProtocol::SaslPlaintext
        };
        self.sasl_mechanism = Some(SaslMechanism::Plain);
        self.sasl_username = Some(username.into());
        self.sasl_password = Some(password.into());
        self
    }

    /// Configures SASL/SCRAM-SHA-256 authentication.
    pub fn with_sasl_scram256(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
        use_ssl: bool,
    ) -> Self {
        self.security_protocol = if use_ssl {
            SecurityProtocol::SaslSsl
        } else {
            SecurityProtocol::SaslPlaintext
        };
        self.sasl_mechanism = Some(SaslMechanism::ScramSha256);
        self.sasl_username = Some(username.into());
        self.sasl_password = Some(password.into());
        self
    }

    /// Configures SASL/SCRAM-SHA-512 authentication.
    pub fn with_sasl_scram512(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
        use_ssl: bool,
    ) -> Self {
        self.security_protocol = if use_ssl {
            SecurityProtocol::SaslSsl
        } else {
            SecurityProtocol::SaslPlaintext
        };
        self.sasl_mechanism = Some(SaslMechanism::ScramSha512);
        self.sasl_username = Some(username.into());
        self.sasl_password = Some(password.into());
        self
    }

    /// Configures SSL/TLS encryption.
    pub fn with_ssl(
        mut self,
        ca_location: Option<String>,
        cert_location: Option<String>,
        key_location: Option<String>,
    ) -> Self {
        self.security_protocol = SecurityProtocol::Ssl;
        self.ssl_ca_location = ca_location;
        self.ssl_certificate_location = cert_location;
        self.ssl_key_location = key_location;
        self
    }

    /// Sets request timeout.
    pub fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    /// Sets message timeout.
    pub fn with_message_timeout(mut self, timeout: Duration) -> Self {
        self.message_timeout = timeout;
        self
    }

    /// Sets auto offset reset policy.
    pub fn with_auto_offset_reset(mut self, reset: AutoOffsetReset) -> Self {
        self.auto_offset_reset = reset;
        self
    }

    /// Sets auto commit configuration.
    pub fn with_auto_commit(mut self, enable: bool, interval_ms: u64) -> Self {
        self.enable_auto_commit = enable;
        self.auto_commit_interval_ms = interval_ms;
        self
    }

    /// Adds an arbitrary custom property.
    pub fn with_property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.extra_properties.insert(key.into(), value.into());
        self
    }

    /// Validates the configuration parameters.
    pub fn validate(&self) -> KafkaResult<()> {
        if self.bootstrap_servers.trim().is_empty() {
            return Err(KafkaError::config("bootstrap_servers must not be empty"));
        }

        match self.security_protocol {
            SecurityProtocol::SaslPlaintext | SecurityProtocol::SaslSsl => {
                match self.sasl_mechanism {
                    None => {
                        return Err(KafkaError::config(
                            "sasl_mechanism must be specified when using SASL",
                        ))
                    }
                    Some(SaslMechanism::Oauthbearer) => return self.validate_oauth(),
                    // Kerberos authenticates with a ticket or keytab
                    // (`sasl.kerberos.*` properties), not a password.
                    Some(SaslMechanism::Gssapi) => return Ok(()),
                    Some(_) => {}
                }
                if self.sasl_username.is_none() || self.sasl_password.is_none() {
                    return Err(KafkaError::config(
                        "sasl_username and sasl_password must be specified when using SASL",
                    ));
                }
            }
            SecurityProtocol::Plaintext | SecurityProtocol::Ssl => {}
        }

        Ok(())
    }

    /// Whether the clients authenticate with SASL OAUTHBEARER tokens that
    /// Ferrika fetches (OIDC or MSK IAM).
    pub fn uses_oauth(&self) -> bool {
        matches!(
            self.security_protocol,
            SecurityProtocol::SaslPlaintext | SecurityProtocol::SaslSsl
        ) && self.sasl_mechanism == Some(SaslMechanism::Oauthbearer)
    }

    /// The OAUTHBEARER settings each token source needs.
    fn validate_oauth(&self) -> KafkaResult<()> {
        let missing = |v: &Option<String>| v.as_deref().is_none_or(|v| v.trim().is_empty());
        match self.oauth.method() {
            OAuthMethod::Oidc => {
                let needed = [
                    ("oauth_token_endpoint", &self.oauth.token_endpoint),
                    ("oauth_client_id", &self.oauth.client_id),
                    ("oauth_client_secret", &self.oauth.client_secret),
                ];
                let absent: Vec<&str> = needed
                    .iter()
                    .filter(|(_, v)| missing(v))
                    .map(|(name, _)| *name)
                    .collect();
                if !absent.is_empty() {
                    return Err(KafkaError::config(format!(
                        "SASL OAUTHBEARER (OIDC) needs {}",
                        absent.join(", ")
                    )));
                }
                if let Some(endpoint) = self.oauth.token_endpoint.as_deref() {
                    crate::kafka::oauth::validate_token_endpoint(endpoint)?;
                }
            }
            OAuthMethod::AwsMskIam => {}
        }
        crate::kafka::oauth::validate_extensions(&self.oauth.extensions)
    }

    /// Converts this high-level configuration into a low-level `rdkafka::ClientConfig`.
    pub(crate) fn to_rdkafka_config(&self) -> KafkaResult<ClientConfig> {
        self.validate()?;
        if self.uses_oauth() && !cfg!(feature = "ssl") {
            return Err(KafkaError::config(
                "SASL OAUTHBEARER needs a Ferrika build with TLS support (`--features ssl`): \
                 librdkafka enables OAUTHBEARER only with OpenSSL. The native admin requests \
                 (ACLs, quotas, config edits) work in this build.",
            ));
        }

        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", &self.bootstrap_servers);

        if let Some(ref client_id) = self.client_id {
            config.set("client.id", client_id);
        }

        if let Some(ref group_id) = self.group_id {
            config.set("group.id", group_id);
        }

        config.set("security.protocol", self.security_protocol.as_str());

        if let Some(mechanism) = self.sasl_mechanism {
            config.set("sasl.mechanism", mechanism.as_str());
        }

        // OAUTHBEARER tokens are set on the client after it is created.
        if !self.uses_oauth() {
            if let Some(ref username) = self.sasl_username {
                config.set("sasl.username", username);
            }

            if let Some(ref password) = self.sasl_password {
                config.set("sasl.password", password);
            }
        }

        if let Some(ref ca) = self.ssl_ca_location {
            config.set("ssl.ca.location", ca);
        }

        if let Some(ref cert) = self.ssl_certificate_location {
            config.set("ssl.certificate.location", cert);
        }

        if let Some(ref key) = self.ssl_key_location {
            config.set("ssl.key.location", key);
        }

        if let Some(ref key_pw) = self.ssl_key_password {
            config.set("ssl.key.password", key_pw);
        }

        config.set(
            "request.timeout.ms",
            self.request_timeout.as_millis().to_string(),
        );
        config.set(
            "message.timeout.ms",
            self.message_timeout.as_millis().to_string(),
        );
        config.set(
            "socket.timeout.ms",
            self.socket_timeout.as_millis().to_string(),
        );
        config.set(
            "session.timeout.ms",
            self.session_timeout.as_millis().to_string(),
        );

        config.set("auto.offset.reset", self.auto_offset_reset.as_str());
        config.set("enable.auto.commit", self.enable_auto_commit.to_string());
        config.set(
            "auto.commit.interval.ms",
            self.auto_commit_interval_ms.to_string(),
        );

        // Looking a topic up must not create it: the broker auto-creates a
        // topic on a metadata request unless the client says it does not
        // want that. The producer opts back in (see `KafkaProducerClient`).
        // `extra_properties` below can still override it.
        config.set("allow.auto.create.topics", "false");

        // Starting a client includes a wait that follows this value:
        // `topics list` took 63 ms with librdkafka's 100 ms default, 36 ms
        // with 50 and 20 ms with 10. The wait still doubles on each failed
        // attempt, up to `reconnect.backoff.max.ms` (10 s).
        config.set("reconnect.backoff.ms", RECONNECT_BACKOFF_MS);

        for (k, v) in &self.extra_properties {
            config.set(k, v);
        }

        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clients_do_not_auto_create_topics_unless_asked() {
        let c = KafkaClientConfig::new("b:9092");
        assert_eq!(
            c.to_rdkafka_config()
                .unwrap()
                .get("allow.auto.create.topics"),
            Some("false")
        );
        let mut c = KafkaClientConfig::new("b:9092");
        c.extra_properties
            .insert("allow.auto.create.topics".into(), "true".into());
        assert_eq!(
            c.to_rdkafka_config()
                .unwrap()
                .get("allow.auto.create.topics"),
            Some("true")
        );
    }

    #[test]
    fn clients_reconnect_after_10_ms_unless_configured() {
        let c = KafkaClientConfig::new("b:9092");
        assert_eq!(
            c.to_rdkafka_config().unwrap().get("reconnect.backoff.ms"),
            Some("10")
        );
        let mut c = KafkaClientConfig::new("b:9092");
        c.extra_properties
            .insert("reconnect.backoff.ms".into(), "250".into());
        assert_eq!(
            c.to_rdkafka_config().unwrap().get("reconnect.backoff.ms"),
            Some("250")
        );
    }

    #[test]
    fn gssapi_needs_no_password() {
        let mut c = KafkaClientConfig::new("b:9092");
        c.security_protocol = SecurityProtocol::SaslPlaintext;
        c.sasl_mechanism = Some(SaslMechanism::Gssapi);
        assert!(c.validate().is_ok());
        c.sasl_mechanism = Some(SaslMechanism::Plain);
        assert!(c.validate().is_err());
    }

    #[test]
    fn debug_redacts_client_secrets() {
        let mut c = KafkaClientConfig::new("b:9092");
        c.sasl_username = Some("alice".into());
        c.sasl_password = Some("hunter2-sasl".into());
        c.ssl_key_password = Some("hunter2-key".into());
        c.connect_url = Some("http://u:hunter2-url@connect:8083".into());
        c.extra_properties.insert(
            "sasl.jaas.config".into(),
            "x required password=\"hunter2-jaas\";".into(),
        );
        c.extra_properties.insert("linger.ms".into(), "5".into());
        let shown = format!("{c:?}");
        assert!(!shown.contains("hunter2"), "{shown}");
        assert!(
            shown.contains("alice") && shown.contains("linger.ms"),
            "{shown}"
        );
    }

    #[test]
    fn test_default_config_creation() {
        let config = KafkaClientConfig::new("localhost:9092");
        assert_eq!(config.bootstrap_servers, "localhost:9092");
        assert_eq!(config.security_protocol, SecurityProtocol::Plaintext);
        assert!(config.validate().is_ok());

        let rd_config = config.to_rdkafka_config().unwrap();
        assert_eq!(rd_config.get("bootstrap.servers"), Some("localhost:9092"));
        assert_eq!(rd_config.get("security.protocol"), Some("plaintext"));
    }

    #[test]
    fn test_sasl_validation_failure() {
        let mut config = KafkaClientConfig::new("localhost:9092");
        config.security_protocol = SecurityProtocol::SaslPlaintext;
        // Missing sasl_mechanism & username/password
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_sasl_plain_builder() {
        let config = KafkaClientConfig::new("broker1:9092")
            .with_sasl_plain("alice", "secret123", true)
            .with_group_id("my-group");

        assert_eq!(config.security_protocol, SecurityProtocol::SaslSsl);
        assert_eq!(config.sasl_mechanism, Some(SaslMechanism::Plain));
        assert_eq!(config.sasl_username.as_deref(), Some("alice"));
        assert_eq!(config.sasl_password.as_deref(), Some("secret123"));
        assert_eq!(config.group_id.as_deref(), Some("my-group"));
        assert!(config.validate().is_ok());

        let rd_config = config.to_rdkafka_config().unwrap();
        assert_eq!(rd_config.get("security.protocol"), Some("sasl_ssl"));
        assert_eq!(rd_config.get("sasl.mechanism"), Some("PLAIN"));
        assert_eq!(rd_config.get("sasl.username"), Some("alice"));
    }

    fn oidc_config() -> KafkaClientConfig {
        let mut c = KafkaClientConfig::new("b:9093");
        c.security_protocol = SecurityProtocol::SaslSsl;
        c.sasl_mechanism = Some(SaslMechanism::Oauthbearer);
        c.oauth.token_endpoint = Some("https://idp/token".into());
        c.oauth.client_id = Some("svc".into());
        c.oauth.client_secret = Some("hunter2-oauth".into());
        c
    }

    #[test]
    fn oauth_validation_per_method() {
        let c = oidc_config();
        assert!(c.validate().is_ok());
        assert!(!format!("{c:?}").contains("hunter2"));

        let mut c = oidc_config();
        c.oauth.client_secret = None;
        c.oauth.token_endpoint = Some(" ".into());
        let err = c.validate().unwrap_err().to_string();
        assert!(
            err.contains("oauth_token_endpoint") && err.contains("oauth_client_secret"),
            "{err}"
        );

        let mut c = KafkaClientConfig::new("b:9098");
        c.security_protocol = SecurityProtocol::SaslSsl;
        c.sasl_mechanism = Some(SaslMechanism::Oauthbearer);
        c.oauth.method = Some(OAuthMethod::AwsMskIam);
        assert!(c.validate().is_ok());
        c.oauth.extensions.insert("auth".into(), "x".into());
        assert!(c.validate().is_err());
    }

    #[test]
    fn oidc_token_endpoint_must_be_https_or_loopback() {
        let mut c = oidc_config();
        c.oauth.token_endpoint = Some("http://idp.corp/token".into());
        let err = c.validate().unwrap_err().to_string();
        assert!(err.contains("not https"), "{err}");
        c.oauth.token_endpoint = Some("http://localhost:8080/token".into());
        assert!(c.validate().is_ok());
    }

    #[test]
    fn oauthbearer_needs_the_ssl_build_for_librdkafka() {
        let result = oidc_config().to_rdkafka_config();
        if cfg!(feature = "ssl") {
            let rd = result.unwrap();
            assert_eq!(rd.get("sasl.mechanism"), Some("OAUTHBEARER"));
            assert_eq!(rd.get("sasl.username"), None);
        } else {
            let err = result.unwrap_err().to_string();
            assert!(err.contains("--features ssl"), "{err}");
        }
    }

    #[test]
    fn test_empty_bootstrap_servers_fails() {
        let config = KafkaClientConfig::new("   ");
        assert!(config.validate().is_err());
    }
}
