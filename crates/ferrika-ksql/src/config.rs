//! A ksqlDB connection: URL, basic-auth login, TLS files and time limit.

use std::time::Duration;

use ferrika_core::kafka::http::{Endpoint, TlsFiles};
use ferrika_core::kafka::profile::{split_url_credentials, ClusterProfile};

use crate::error::{KsqlError, KsqlResult};

/// The ksqlDB server's default listener port.
pub const DEFAULT_PORT: u16 = 8088;

/// How long connecting, the TLS handshake and one request may take when no
/// other limit is set.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// Where and how to reach a ksqlDB server. `Debug` redacts the password.
#[derive(Clone, PartialEq, Eq)]
pub struct KsqlConfig {
    /// `http(s)://host[:port][/prefix]`, without credentials.
    pub url: String,
    /// Basic-auth user.
    pub user: Option<String>,
    password: Option<String>,
    /// CA certificates (PEM) to trust instead of the OS trust store.
    pub ca_location: Option<String>,
    /// Client certificate (PEM) for mutual TLS; needs [`Self::key_location`].
    pub certificate_location: Option<String>,
    /// Client private key (PEM, unencrypted).
    pub key_location: Option<String>,
    /// Limit for connecting, the TLS handshake and each request (not for the
    /// rows of a push query, which have their own limits).
    pub timeout: Duration,
}

impl std::fmt::Debug for KsqlConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KsqlConfig")
            .field("url", &self.url)
            .field("user", &self.user)
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .field("ca_location", &self.ca_location)
            .field("certificate_location", &self.certificate_location)
            .field("key_location", &self.key_location)
            .field("timeout", &self.timeout)
            .finish()
    }
}

impl KsqlConfig {
    /// A connection to `url`. Credentials in the URL (`https://user:pass@host`)
    /// are moved to [`Self::user`] and the password.
    pub fn new(url: &str) -> Self {
        let (url, credentials) = split_url_credentials(url.trim());
        let (user, password) = match credentials {
            Some((user, password)) => (Some(user), Some(password)),
            None => (None, None),
        };
        Self {
            url,
            user,
            password,
            ca_location: None,
            certificate_location: None,
            key_location: None,
            timeout: DEFAULT_TIMEOUT,
        }
    }

    /// The ksqlDB server of `profile` (`ksql_url` and the `ksql_*` TLS
    /// paths), or `None` when the profile names none.
    pub fn from_profile(profile: &ClusterProfile) -> Option<Self> {
        let url = profile.ksql_url.as_deref()?.trim();
        if url.is_empty() {
            return None;
        }
        let mut config = Self::new(url);
        config.ca_location = profile.ksql_ca_location.clone();
        config.certificate_location = profile.ksql_certificate_location.clone();
        config.key_location = profile.ksql_key_location.clone();
        Some(config)
    }

    /// Logs in as `user` with `password` (HTTP basic authentication),
    /// replacing credentials from the URL.
    pub fn with_credentials(
        mut self,
        user: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.user = Some(user.into());
        self.password = Some(password.into());
        self
    }

    /// Sets the time limit for connecting and each request (at least 1 s).
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout.max(Duration::from_secs(1));
        self
    }

    /// Whether a password is set.
    pub fn has_password(&self) -> bool {
        self.password.is_some()
    }

    /// `host:port` of the server (credentials left out): what the company
    /// policy's broker-address rules and the audit log see.
    pub fn address(&self) -> KsqlResult<String> {
        Ok(self.endpoint()?.target())
    }

    /// The parsed URL.
    pub(crate) fn endpoint(&self) -> KsqlResult<Endpoint> {
        Endpoint::parse(&self.url, DEFAULT_PORT).map_err(|e| KsqlError::config(e.to_string()))
    }

    /// The `Authorization` header value, when a user is set.
    pub(crate) fn authorization(&self) -> KsqlResult<Option<String>> {
        match (&self.user, &self.password) {
            (Some(user), Some(password)) => {
                let token =
                    ferrika_core::codec::base64::encode(format!("{user}:{password}").as_bytes());
                Ok(Some(format!("Basic {token}")))
            }
            (Some(_), None) => Err(KsqlError::config(
                "a ksqlDB user is set but no password; give one or remove the user",
            )),
            (None, _) => Ok(None),
        }
    }

    /// The CA, client certificate and key paths as Ferrika's shared TLS
    /// settings for HTTP clients.
    pub fn tls_files(&self) -> TlsFiles {
        TlsFiles {
            ca_file: self.ca_location.clone(),
            client_cert: self.certificate_location.clone(),
            client_key: self.key_location.clone(),
        }
    }

    /// The client certificate and key paths, when mutual TLS is configured.
    pub(crate) fn client_certificate(&self) -> KsqlResult<Option<(&str, &str)>> {
        match (&self.certificate_location, &self.key_location) {
            (Some(cert), Some(key)) => Ok(Some((cert.as_str(), key.as_str()))),
            (None, None) => Ok(None),
            _ => Err(KsqlError::config(
                "a ksqlDB client certificate needs both the certificate and the key file",
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_credentials_become_the_login() {
        let c = KsqlConfig::new("https://ops:s%40cret@ksql.example.com/ksql");
        assert_eq!(c.url, "https://ksql.example.com/ksql");
        assert_eq!(c.user.as_deref(), Some("ops"));
        assert!(c.has_password());
        assert_eq!(c.address().unwrap(), "ksql.example.com:443");
        // base64("ops:s@cret")
        assert_eq!(
            c.authorization().unwrap().as_deref(),
            Some("Basic b3BzOnNAY3JldA==")
        );
        let shown = format!("{c:?}");
        assert!(!shown.contains("cret"), "{shown}");

        let c = KsqlConfig::new("localhost");
        assert_eq!(c.address().unwrap(), "localhost:8088");
        assert_eq!(c.authorization().unwrap(), None);
        let c = c.with_credentials("u", "p");
        assert!(c.authorization().unwrap().is_some());
    }

    #[test]
    fn a_user_without_password_or_half_a_client_certificate_is_refused() {
        let mut c = KsqlConfig::new("http://k:8088");
        c.user = Some("ops".into());
        assert!(c.authorization().is_err());
        c.certificate_location = Some("c.pem".into());
        assert!(c.client_certificate().is_err());
        c.key_location = Some("k.pem".into());
        assert_eq!(c.client_certificate().unwrap(), Some(("c.pem", "k.pem")));
    }

    #[test]
    fn tls_files_carry_the_three_paths() {
        let mut c = KsqlConfig::new("https://k:8088");
        assert!(c.tls_files().is_empty());
        c.ca_location = Some("ca.pem".into());
        c.certificate_location = Some("c.pem".into());
        c.key_location = Some("k.pem".into());
        let files = c.tls_files();
        assert_eq!(files.ca_file.as_deref(), Some("ca.pem"));
        assert_eq!(files.client_pair().unwrap(), Some(("c.pem", "k.pem")));
    }

    #[test]
    fn profiles_carry_the_server_and_its_tls_files() {
        let mut profile = ClusterProfile::new("prod", "kafka:9092");
        assert!(KsqlConfig::from_profile(&profile).is_none());
        profile.ksql_url = Some("https://ops:pw@ksql.prod:8088".into());
        profile.ksql_ca_location = Some("/etc/ca.pem".into());
        let c = KsqlConfig::from_profile(&profile).unwrap();
        assert_eq!(c.url, "https://ksql.prod:8088");
        assert_eq!(c.user.as_deref(), Some("ops"));
        assert_eq!(c.ca_location.as_deref(), Some("/etc/ca.pem"));
        assert_eq!(c.timeout, DEFAULT_TIMEOUT);
        assert_eq!(
            c.with_timeout(Duration::ZERO).timeout,
            Duration::from_secs(1)
        );
    }
}
