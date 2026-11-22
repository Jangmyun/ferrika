use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::error::{KafkaError, KafkaResult};
use crate::files::write_private_file;
use crate::kafka::config::{
    KafkaClientConfig, OAuthClientAuth, OAuthConfig, OAuthMethod, SaslMechanism, SecurityProtocol,
};
use crate::kafka::secrets;

/// Represents a configured Kafka cluster connection profile.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterProfile {
    /// Unique identifier / nickname for this cluster (e.g. "local", "staging", "prod").
    pub name: String,
    /// Optional human-readable description or notes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Kafka bootstrap servers (comma-separated "host1:9092,host2:9092").
    pub bootstrap_servers: String,
    /// Security protocol to use (plaintext, ssl, sasl_plaintext, sasl_ssl).
    #[serde(default)]
    pub security_protocol: SecurityProtocol,
    /// SASL mechanism if SASL is enabled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sasl_mechanism: Option<SaslMechanism>,
    /// SASL username.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sasl_username: Option<String>,
    /// SASL password.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sasl_password: Option<String>,
    /// File path to CA certificate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ssl_ca_location: Option<String>,
    /// File path to client SSL certificate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ssl_certificate_location: Option<String>,
    /// File path to client SSL private key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ssl_key_location: Option<String>,
    /// SSL private key passphrase.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ssl_key_password: Option<String>,
    /// Request timeout in milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_timeout_ms: Option<u64>,
    /// Refuse operations that change the cluster (topics, offsets, ACLs,
    /// produced records). Meant for production clusters.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub read_only: bool,
    /// Confluent Schema Registry URL (http://host:8081), used to decode
    /// Avro payloads in the GUI.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_registry_url: Option<String>,
    /// Kafka Connect REST URL (http://host:8083).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connect_url: Option<String>,
    /// SASL OAUTHBEARER token source (OIDC when unset).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub oauth_method: Option<OAuthMethod>,
    /// OIDC token endpoint URL.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub oauth_token_endpoint: Option<String>,
    /// OIDC client id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub oauth_client_id: Option<String>,
    /// OIDC client secret (kept in the OS credential store).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub oauth_client_secret: Option<String>,
    /// Token endpoint client authentication (basic when unset).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub oauth_client_auth: Option<OAuthClientAuth>,
    /// OIDC scope.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub oauth_scope: Option<String>,
    /// SASL OAUTHBEARER extensions.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub oauth_extensions: BTreeMap<String, String>,
    /// AWS region for MSK IAM.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aws_region: Option<String>,
    /// AWS shared-config profile for MSK IAM.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aws_profile: Option<String>,
}

impl ClusterProfile {
    /// Creates a new profile with the given name and bootstrap servers.
    pub fn new(name: impl Into<String>, bootstrap_servers: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            description: None,
            bootstrap_servers: bootstrap_servers.into(),
            security_protocol: SecurityProtocol::Plaintext,
            sasl_mechanism: None,
            sasl_username: None,
            sasl_password: None,
            ssl_ca_location: None,
            ssl_certificate_location: None,
            ssl_key_location: None,
            ssl_key_password: None,
            request_timeout_ms: None,
            read_only: false,
            schema_registry_url: None,
            connect_url: None,
            oauth_method: None,
            oauth_token_endpoint: None,
            oauth_client_id: None,
            oauth_client_secret: None,
            oauth_client_auth: None,
            oauth_scope: None,
            oauth_extensions: BTreeMap::new(),
            aws_region: None,
            aws_profile: None,
        }
    }

    /// Sets the description.
    pub fn with_description(mut self, desc: impl Into<String>) -> Self {
        self.description = Some(desc.into());
        self
    }

    /// Configures SASL PLAIN authentication.
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

    /// Converts this profile into a `KafkaClientConfig` ready for client instantiation.
    pub fn to_kafka_config(&self) -> KafkaClientConfig {
        let mut config = KafkaClientConfig::new(&self.bootstrap_servers);
        config.security_protocol = self.security_protocol;
        config.sasl_mechanism = self.sasl_mechanism;
        config.sasl_username = self.sasl_username.clone();
        config.sasl_password = self.sasl_password.clone();
        config.ssl_ca_location = self.ssl_ca_location.clone();
        config.ssl_certificate_location = self.ssl_certificate_location.clone();
        config.ssl_key_location = self.ssl_key_location.clone();
        config.ssl_key_password = self.ssl_key_password.clone();
        config.read_only = self.read_only;
        config.schema_registry_url = self.schema_registry_url.clone();
        config.connect_url = self.connect_url.clone();
        config.oauth = OAuthConfig {
            method: self.oauth_method,
            token_endpoint: self.oauth_token_endpoint.clone(),
            client_id: self.oauth_client_id.clone(),
            client_secret: self.oauth_client_secret.clone(),
            client_auth: self.oauth_client_auth,
            scope: self.oauth_scope.clone(),
            extensions: self.oauth_extensions.clone(),
            aws_region: self.aws_region.clone(),
            aws_profile: self.aws_profile.clone(),
        };

        if let Some(ms) = self.request_timeout_ms {
            config.request_timeout = Duration::from_millis(ms);
        }

        config
    }

    /// Constructs a profile from an existing `KafkaClientConfig`.
    pub fn from_kafka_config(name: impl Into<String>, config: &KafkaClientConfig) -> Self {
        Self {
            name: name.into(),
            description: None,
            bootstrap_servers: config.bootstrap_servers.clone(),
            security_protocol: config.security_protocol,
            sasl_mechanism: config.sasl_mechanism,
            sasl_username: config.sasl_username.clone(),
            sasl_password: config.sasl_password.clone(),
            ssl_ca_location: config.ssl_ca_location.clone(),
            ssl_certificate_location: config.ssl_certificate_location.clone(),
            ssl_key_location: config.ssl_key_location.clone(),
            ssl_key_password: config.ssl_key_password.clone(),
            request_timeout_ms: Some(config.request_timeout.as_millis() as u64),
            read_only: config.read_only,
            schema_registry_url: config.schema_registry_url.clone(),
            connect_url: config.connect_url.clone(),
            oauth_method: config.oauth.method,
            oauth_token_endpoint: config.oauth.token_endpoint.clone(),
            oauth_client_id: config.oauth.client_id.clone(),
            oauth_client_secret: config.oauth.client_secret.clone(),
            oauth_client_auth: config.oauth.client_auth,
            oauth_scope: config.oauth.scope.clone(),
            oauth_extensions: config.oauth.extensions.clone(),
            aws_region: config.oauth.aws_region.clone(),
            aws_profile: config.oauth.aws_profile.clone(),
        }
    }

    /// Whether the profile authenticates with SASL OAUTHBEARER through AWS
    /// MSK IAM.
    pub fn uses_msk_iam(&self) -> bool {
        self.sasl_mechanism == Some(SaslMechanism::Oauthbearer)
            && self.oauth_method == Some(OAuthMethod::AwsMskIam)
    }
}

/// Persistent store managing cluster profiles and the currently active profile.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProfileStore {
    /// The name of the currently active cluster profile.
    pub active_profile: Option<String>,
    /// Map of profile name to profile configuration (ordered by name).
    pub profiles: BTreeMap<String, ClusterProfile>,
    /// Profiles whose passwords live in the OS credential store rather than
    /// in this file (see [`secrets`]).
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub keyring_profiles: BTreeSet<String>,
    /// Profiles whose stored passwords could not be read in this session.
    /// Saving leaves their entries untouched so a locked keychain does not
    /// erase them.
    #[serde(skip)]
    unreadable: BTreeSet<String>,
}

/// Fields kept in the OS credential store. URL fields go there only when
/// they carry a password (`https://KEY:SECRET@host`).
const SECRET_FIELDS: [&str; 5] = [
    "sasl_password",
    "ssl_key_password",
    "schema_registry_url",
    "connect_url",
    "oauth_client_secret",
];

fn secret_field<'a>(profile: &'a mut ClusterProfile, field: &str) -> &'a mut Option<String> {
    match field {
        "sasl_password" => &mut profile.sasl_password,
        "ssl_key_password" => &mut profile.ssl_key_password,
        "schema_registry_url" => &mut profile.schema_registry_url,
        "oauth_client_secret" => &mut profile.oauth_client_secret,
        _ => &mut profile.connect_url,
    }
}

/// What the profiles file keeps in place of a secret value: nothing for a
/// password, the URL without its password for a URL. `None` when the value
/// holds no secret and stays in the file as is.
fn public_part(field: &str, value: &str) -> Option<Option<String>> {
    if field.ends_with("_url") {
        url_without_password(value).map(Some)
    } else {
        Some(None)
    }
}

/// `https://KEY:SECRET@host` -> `https://KEY:<mask>@host`, for display.
pub fn mask_url_password(url: &str, mask: &str) -> String {
    match url_password_span(url) {
        Some((colon, at)) => format!("{}:{mask}{}", &url[..colon], &url[at..]),
        None => url.to_string(),
    }
}

/// `https://KEY:SECRET@host/x` -> `https://KEY@host/x`; `None` when the URL
/// has no password.
fn url_without_password(url: &str) -> Option<String> {
    let (colon, at) = url_password_span(url)?;
    Some(format!("{}{}", &url[..colon], &url[at..]))
}

/// Byte positions of the `:` before the password and the `@` after it.
fn url_password_span(url: &str) -> Option<(usize, usize)> {
    let start = url.find("://")? + 3;
    let authority_end = url[start..].find('/').map_or(url.len(), |i| start + i);
    let at = start + url[start..authority_end].rfind('@')?;
    let colon = start + url[start..at].find(':')?;
    Some((colon, at))
}

impl Default for ProfileStore {
    fn default() -> Self {
        let mut profiles = BTreeMap::new();
        let default_profile = ClusterProfile::new("local", "localhost:9092")
            .with_description("Default local Kafka broker");
        profiles.insert(default_profile.name.clone(), default_profile);

        Self {
            active_profile: Some("local".to_string()),
            profiles,
            keyring_profiles: BTreeSet::new(),
            unreadable: BTreeSet::new(),
        }
    }
}

impl ProfileStore {
    /// Returns the default filesystem path to the `profiles.json` configuration file.
    ///
    /// Search order:
    /// 1. `FERRIKA_CONFIG_PATH` environment variable (direct file path)
    /// 2. `FERRIKA_CONFIG_DIR` environment variable + `profiles.json`
    /// 3. Windows: `%USERPROFILE%\.ferrika\profiles.json`
    /// 4. Unix/macOS: `$HOME/.ferrika/profiles.json`
    /// 5. Current directory fallback: `.ferrika/profiles.json`
    pub fn default_path() -> PathBuf {
        if let Ok(path) = std::env::var("FERRIKA_CONFIG_PATH") {
            return PathBuf::from(path);
        }

        if let Ok(dir) = std::env::var("FERRIKA_CONFIG_DIR") {
            return PathBuf::from(dir).join("profiles.json");
        }

        #[cfg(target_os = "windows")]
        let home = std::env::var("USERPROFILE")
            .or_else(|_| std::env::var("HOME"))
            .ok();

        #[cfg(not(target_os = "windows"))]
        let home = std::env::var("HOME").ok();

        if let Some(h) = home {
            PathBuf::from(h).join(".ferrika").join("profiles.json")
        } else {
            PathBuf::from(".ferrika").join("profiles.json")
        }
    }

    /// Loads the profile store from the default path. If the file does not exist,
    /// returns a default store containing the `local` profile.
    /// Passwords stored in the OS credential store are filled in.
    pub fn load() -> KafkaResult<Self> {
        let mut store = Self::load_from_path(&Self::default_path())?;
        store.read_secrets();
        Ok(store)
    }

    /// Fills in passwords of the profiles listed in `keyring_profiles`.
    fn read_secrets(&mut self) {
        for name in &self.keyring_profiles {
            let Some(profile) = self.profiles.get_mut(name) else {
                continue;
            };
            for field in SECRET_FIELDS {
                match secrets::get(name, field) {
                    // Ignore a stored value the file no longer matches (a URL
                    // or password edited by hand).
                    Ok(Some(value)) => {
                        let current = secret_field(profile, field);
                        if public_part(field, &value).flatten() == *current {
                            *current = Some(value);
                        }
                    }
                    Ok(None) => {}
                    Err(e) => {
                        tracing::warn!(
                            "Cannot read {field} of profile '{name}' from the OS credential store: {e}"
                        );
                        self.unreadable.insert(name.clone());
                    }
                }
            }
        }
    }

    /// Loads the profile store from the default path or returns default if any error occurs.
    pub fn load_or_default() -> Self {
        Self::load().unwrap_or_default()
    }

    /// Loads the profile store from the specified file path.
    pub fn load_from_path(path: &Path) -> KafkaResult<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }

        let content = fs::read_to_string(path).map_err(|e| {
            KafkaError::profile(format!(
                "Failed to read profiles file at {}: {e}",
                path.display()
            ))
        })?;

        let store: Self = serde_json::from_str(&content).map_err(|e| {
            KafkaError::profile(format!(
                "Failed to parse profiles JSON at {}: {e}",
                path.display()
            ))
        })?;

        Ok(store)
    }

    /// Saves the profile store to the default filesystem path.
    ///
    /// Passwords go to the OS credential store when one is available; a
    /// profile whose secrets cannot be stored there keeps them in the file.
    pub fn save(&self) -> KafkaResult<()> {
        let path = Self::default_path();
        let previous = Self::load_from_path(&path)
            .map(|s| s.keyring_profiles)
            .unwrap_or_default();
        let stored = self.write_secrets(&previous);
        stored.save_to_path(&path)?;
        // Drop entries of removed or renamed profiles only after the file
        // no longer points at them.
        for name in previous.difference(&stored.keyring_profiles) {
            // Entries that could not be read are never deleted: after a
            // rename this session never saw the secrets it would drop.
            if !self.profiles.contains_key(name) && !self.unreadable.contains(name) {
                for field in SECRET_FIELDS {
                    if let Err(e) = secrets::delete(name, field) {
                        tracing::warn!("Cannot remove {field} of profile '{name}': {e}");
                    }
                }
            }
        }
        Ok(())
    }

    /// Moves passwords into the OS credential store and returns the store
    /// to write to disk, without those passwords.
    fn write_secrets(&self, previous: &BTreeSet<String>) -> Self {
        let mut stored = self.clone();
        stored.keyring_profiles.clear();
        if !secrets::enabled() {
            // Their secrets are still in the OS store; keep pointing there.
            stored.keyring_profiles = self
                .unreadable
                .iter()
                .filter(|name| self.profiles.contains_key(*name))
                .cloned()
                .collect();
            let kept = stored.keep_os_secrets_out(previous, |name, field| {
                secrets::get(name, field).ok().flatten()
            });
            stored.keyring_profiles.extend(kept);
            return stored;
        }
        for (name, profile) in &mut stored.profiles {
            // Entries that could not be read are kept, not overwritten with
            // the empty values this session saw.
            let keep_entries = self.unreadable.contains(name);
            let mut ok = true;
            let mut has_secret = false;
            for field in SECRET_FIELDS {
                let value = secret_field(profile, field).clone();
                let result = match value.filter(|v| public_part(field, v).is_some()) {
                    Some(value) => {
                        has_secret = true;
                        secrets::set(name, field, &value)
                    }
                    None if previous.contains(name) && !keep_entries => {
                        secrets::delete(name, field)
                    }
                    None => Ok(()),
                };
                if let Err(e) = result {
                    tracing::warn!(
                        "Cannot store {field} of profile '{name}' in the OS credential store, \
                         keeping it in the profiles file: {e}"
                    );
                    ok = false;
                    break;
                }
            }
            if !ok {
                continue;
            }
            for field in SECRET_FIELDS {
                let current = secret_field(profile, field);
                if let Some(public) = current.as_deref().and_then(|v| public_part(field, v)) {
                    *current = public;
                }
            }
            if has_secret || keep_entries {
                stored.keyring_profiles.insert(name.clone());
            }
        }
        stored
    }

    /// With `FERRIKA_SECRET_STORE=file`: removes from the profiles of
    /// `previous` (kept in the OS store) each secret that is still the one the
    /// OS store holds (`stored`), so saving does not copy it to the file in
    /// plain text. Returns the profiles that keep secrets there. A secret
    /// changed in this session differs from the stored one and goes to the
    /// file, as new secrets do in this mode.
    fn keep_os_secrets_out(
        &mut self,
        previous: &BTreeSet<String>,
        stored: impl Fn(&str, &str) -> Option<String>,
    ) -> BTreeSet<String> {
        let mut kept = BTreeSet::new();
        for name in previous {
            let Some(profile) = self.profiles.get_mut(name) else {
                continue;
            };
            for field in SECRET_FIELDS {
                let Some(value) = stored(name, field) else {
                    continue;
                };
                let current = secret_field(profile, field);
                if current.as_deref() != Some(value.as_str()) {
                    continue;
                }
                if let Some(public) = public_part(field, &value) {
                    *current = public;
                    kept.insert(name.clone());
                }
            }
        }
        kept
    }

    /// Whether the profile's passwords are kept in the OS credential store.
    pub fn uses_keyring(&self, name: &str) -> bool {
        self.keyring_profiles.contains(name)
    }

    /// Profiles in the profiles file that still hold a password in plain
    /// text. Meant for a store read with [`Self::load_from_path`].
    pub fn plaintext_secret_profiles(&self) -> Vec<String> {
        self.profiles
            .iter()
            .filter(|(_, profile)| {
                let mut profile = (*profile).clone();
                SECRET_FIELDS.iter().any(|field| {
                    secret_field(&mut profile, field)
                        .as_deref()
                        .is_some_and(|v| public_part(field, v).is_some())
                })
            })
            .map(|(name, _)| name.clone())
            .collect()
    }

    /// Saves the profile store to the specified file path with pretty formatting.
    pub fn save_to_path(&self, path: &Path) -> KafkaResult<()> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() && !parent.exists() {
                create_private_dir(parent).map_err(|e| {
                    KafkaError::profile(format!(
                        "Failed to create config directory {}: {e}",
                        parent.display()
                    ))
                })?;
            }
        }

        let json = serde_json::to_string_pretty(self).map_err(|e| {
            KafkaError::profile(format!("Failed to serialize profiles to JSON: {e}"))
        })?;

        write_private_file(path, json.as_bytes()).map_err(|e| {
            KafkaError::profile(format!(
                "Failed to write profiles to {}: {e}",
                path.display()
            ))
        })?;

        Ok(())
    }

    /// Returns a reference to the currently active cluster profile, if found.
    pub fn get_active(&self) -> Option<&ClusterProfile> {
        self.active_profile
            .as_ref()
            .and_then(|name| self.profiles.get(name))
            .or_else(|| self.profiles.values().next())
    }

    /// Retrieves a profile by name.
    pub fn get(&self, name: &str) -> Option<&ClusterProfile> {
        self.profiles.get(name)
    }

    /// Adds or replaces a cluster profile in the store.
    pub fn add(&mut self, profile: ClusterProfile, set_active: bool) {
        let name = profile.name.clone();
        self.profiles.insert(name.clone(), profile);
        if set_active || self.active_profile.is_none() {
            self.active_profile = Some(name);
        }
    }

    /// Removes a profile by name. Fails if the profile does not exist.
    pub fn remove(&mut self, name: &str) -> KafkaResult<ClusterProfile> {
        let removed = self
            .profiles
            .remove(name)
            .ok_or_else(|| KafkaError::profile(format!("Profile '{name}' not found")))?;

        // If we removed the active profile, reset active to another profile if one exists
        if self.active_profile.as_deref() == Some(name) {
            self.active_profile = self.profiles.keys().next().cloned();
        }

        Ok(removed)
    }

    /// Sets the active cluster profile. Fails if the profile does not exist.
    pub fn set_active(&mut self, name: &str) -> KafkaResult<()> {
        if !self.profiles.contains_key(name) {
            return Err(KafkaError::profile(format!(
                "Cannot activate profile '{name}': profile does not exist"
            )));
        }
        self.active_profile = Some(name.to_string());
        Ok(())
    }

    /// Returns an ordered list of profiles paired with a boolean indicating if it is active.
    pub fn list(&self) -> Vec<(&ClusterProfile, bool)> {
        self.profiles
            .values()
            .map(|p| {
                let is_active = self.active_profile.as_deref() == Some(&p.name);
                (p, is_active)
            })
            .collect()
    }
}

/// Parses Java-style `.properties` text: `key=value` or `key: value`
/// lines, `#`/`!` comments, `\` line continuations and escapes
/// (`\\`, `\=`, `\:`, `\t`, `\n`, `\uXXXX`).
pub fn parse_properties(text: &str) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    let mut pending = String::new();
    for raw in text.lines() {
        let line = raw.trim_start();
        if pending.is_empty() && (line.is_empty() || line.starts_with('#') || line.starts_with('!'))
        {
            continue;
        }
        // An odd number of trailing backslashes continues the line.
        let trailing = line.chars().rev().take_while(|c| *c == '\\').count();
        if trailing % 2 == 1 {
            pending.push_str(&line[..line.len() - 1]);
            continue;
        }
        pending.push_str(line);
        let entry = std::mem::take(&mut pending);
        // The key ends at the first unescaped '=' or ':'.
        let mut split = entry.len();
        let mut escaped = false;
        for (i, c) in entry.char_indices() {
            match c {
                '\\' if !escaped => escaped = true,
                '=' | ':' if !escaped => {
                    split = i;
                    break;
                }
                _ => escaped = false,
            }
        }
        let key = unescape(entry[..split].trim());
        let value = unescape(entry.get(split + 1..).unwrap_or_default().trim());
        if !key.is_empty() {
            out.insert(key, value);
        }
    }
    out
}

fn unescape(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('t') => out.push('\t'),
            Some('n') => out.push('\n'),
            Some('r') => out.push('\r'),
            Some('f') => out.push('\x0c'),
            Some('u') => {
                let hex: String = chars.by_ref().take(4).collect();
                match u32::from_str_radix(&hex, 16).ok().and_then(char::from_u32) {
                    Some(ch) => out.push(ch),
                    None => out.push_str(&hex),
                }
            }
            Some(other) => out.push(other),
            None => {}
        }
    }
    out
}

// Manual so passwords (and passwords inside REST URLs) never reach logs.
impl std::fmt::Debug for ClusterProfile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let redact = |v: &Option<String>| v.as_ref().map(|_| "<redacted>");
        let mask = |v: &Option<String>| v.as_deref().map(|u| mask_url_password(u, "<redacted>"));
        f.debug_struct("ClusterProfile")
            .field("name", &self.name)
            .field("description", &self.description)
            .field("bootstrap_servers", &self.bootstrap_servers)
            .field("security_protocol", &self.security_protocol)
            .field("sasl_mechanism", &self.sasl_mechanism)
            .field("sasl_username", &self.sasl_username)
            .field("sasl_password", &redact(&self.sasl_password))
            .field("ssl_ca_location", &self.ssl_ca_location)
            .field("ssl_certificate_location", &self.ssl_certificate_location)
            .field("ssl_key_location", &self.ssl_key_location)
            .field("ssl_key_password", &redact(&self.ssl_key_password))
            .field("request_timeout_ms", &self.request_timeout_ms)
            .field("read_only", &self.read_only)
            .field("schema_registry_url", &mask(&self.schema_registry_url))
            .field("connect_url", &mask(&self.connect_url))
            .field("oauth_method", &self.oauth_method)
            .field("oauth_token_endpoint", &self.oauth_token_endpoint)
            .field("oauth_client_id", &self.oauth_client_id)
            .field("oauth_client_secret", &redact(&self.oauth_client_secret))
            .field("oauth_client_auth", &self.oauth_client_auth)
            .field("oauth_scope", &self.oauth_scope)
            .field("oauth_extensions", &self.oauth_extensions)
            .field("aws_region", &self.aws_region)
            .field("aws_profile", &self.aws_profile)
            .finish()
    }
}

/// Creates `dir` (and missing parents); owner-only (0700) on Unix since it
/// holds the profiles file.
fn create_private_dir(dir: &Path) -> std::io::Result<()> {
    let mut builder = fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(dir)
}

/// `extension_<name>="value"` entries of an OAuthBearerLoginModule line.
fn jaas_extensions(jaas: &str) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    let mut rest = jaas;
    while let Some(i) = rest.find("extension_") {
        rest = &rest[i + "extension_".len()..];
        let Some((name, after)) = rest.split_once("=\"") else {
            break;
        };
        let Some(end) = after.find('"') else {
            break;
        };
        if !name.is_empty() && name.bytes().all(|b| b.is_ascii_alphabetic()) {
            out.insert(name.to_string(), after[..end].to_string());
        }
        rest = &after[end..];
    }
    out
}

/// `key="value"` from a JAAS config line (`username="x" password="y"`).
fn jaas_value(jaas: &str, key: &str) -> Option<String> {
    let start = jaas.find(&format!("{key}=\""))? + key.len() + 2;
    let end = jaas[start..].find('"')? + start;
    Some(jaas[start..end].to_string())
}

impl ClusterProfile {
    /// Builds a profile from a Kafka client `.properties` file (Java client
    /// or librdkafka keys, Schema Registry settings included). Returns the
    /// profile and notes about settings that were not taken over.
    pub fn from_properties(name: &str, text: &str) -> KafkaResult<(Self, Vec<String>)> {
        let props = parse_properties(text);
        let get = |k: &str| props.get(k).map(String::as_str).filter(|v| !v.is_empty());
        let bootstrap = get("bootstrap.servers")
            .ok_or_else(|| KafkaError::config("The file has no bootstrap.servers"))?;
        let mut profile = Self::new(name, bootstrap);
        let mut notes = Vec::new();
        if let Some(p) = get("security.protocol") {
            profile.security_protocol = match p.to_ascii_uppercase().as_str() {
                "PLAINTEXT" => SecurityProtocol::Plaintext,
                "SSL" => SecurityProtocol::Ssl,
                "SASL_PLAINTEXT" => SecurityProtocol::SaslPlaintext,
                "SASL_SSL" => SecurityProtocol::SaslSsl,
                other => {
                    return Err(KafkaError::config(format!(
                        "Unknown security.protocol '{other}'"
                    )))
                }
            };
        }
        if let Some(m) = get("sasl.mechanism").or_else(|| get("sasl.mechanisms")) {
            profile.sasl_mechanism = Some(match m.to_ascii_uppercase().as_str() {
                "PLAIN" => SaslMechanism::Plain,
                "SCRAM-SHA-256" => SaslMechanism::ScramSha256,
                "SCRAM-SHA-512" => SaslMechanism::ScramSha512,
                "GSSAPI" => SaslMechanism::Gssapi,
                "OAUTHBEARER" => SaslMechanism::Oauthbearer,
                // The MSK IAM library's own mechanism; Ferrika sends the
                // same signed token over OAUTHBEARER.
                "AWS_MSK_IAM" => {
                    profile.oauth_method = Some(OAuthMethod::AwsMskIam);
                    SaslMechanism::Oauthbearer
                }
                other => {
                    return Err(KafkaError::config(format!(
                        "Unknown sasl.mechanism '{other}'"
                    )))
                }
            });
        }
        if let Some(jaas) = get("sasl.jaas.config") {
            if jaas.contains("IAMLoginModule") {
                profile.sasl_mechanism = Some(SaslMechanism::Oauthbearer);
                profile.oauth_method = Some(OAuthMethod::AwsMskIam);
                profile.aws_profile = jaas_value(jaas, "awsProfileName");
                if jaas.contains("awsRoleArn") {
                    notes.push(
                        "awsRoleArn (assume role) is not supported yet; use an AWS profile \
                         with credential_process instead"
                            .to_string(),
                    );
                }
            } else if jaas.contains("OAuthBearerLoginModule") {
                profile.oauth_client_id = jaas_value(jaas, "clientId");
                profile.oauth_client_secret = jaas_value(jaas, "clientSecret");
                profile.oauth_scope = jaas_value(jaas, "scope");
                profile.oauth_extensions.extend(jaas_extensions(jaas));
            } else {
                profile.sasl_username = jaas_value(jaas, "username");
                profile.sasl_password = jaas_value(jaas, "password");
            }
        }
        if get("sasl.login.callback.handler.class").is_some_and(|c| c.contains("msk.auth.iam")) {
            profile.oauth_method = Some(OAuthMethod::AwsMskIam);
        }
        if get("sasl.oauthbearer.method").is_some_and(|m| m.eq_ignore_ascii_case("oidc")) {
            profile.oauth_method = Some(OAuthMethod::Oidc);
        }
        let first = |keys: &[&str]| keys.iter().find_map(|k| get(k)).map(str::to_string);
        if let Some(url) = first(&["sasl.oauthbearer.token.endpoint.url"]) {
            profile.oauth_token_endpoint = Some(url);
        }
        if let Some(id) = first(&[
            "sasl.oauthbearer.client.id",
            "sasl.oauthbearer.client.credentials.client.id",
        ]) {
            profile.oauth_client_id = Some(id);
        }
        if let Some(secret) = first(&[
            "sasl.oauthbearer.client.secret",
            "sasl.oauthbearer.client.credentials.client.secret",
        ]) {
            profile.oauth_client_secret = Some(secret);
        }
        if let Some(scope) = first(&["sasl.oauthbearer.scope"]) {
            profile.oauth_scope = Some(scope);
        }
        if let Some(ext) = get("sasl.oauthbearer.extensions") {
            match crate::kafka::oauth::parse_extensions(ext) {
                Ok(ext) => profile.oauth_extensions.extend(ext),
                Err(e) => notes.push(format!("sasl.oauthbearer.extensions skipped: {e}")),
            }
        }
        if let Some(u) = get("sasl.username") {
            profile.sasl_username = Some(u.to_string());
        }
        if let Some(p) = get("sasl.password") {
            profile.sasl_password = Some(p.to_string());
        }
        if profile.sasl_mechanism.is_none() && profile.sasl_username.is_some() {
            profile.sasl_mechanism = Some(SaslMechanism::Plain);
        }
        profile.ssl_ca_location = get("ssl.ca.location").map(str::to_string);
        profile.ssl_certificate_location = get("ssl.certificate.location").map(str::to_string);
        profile.ssl_key_location = get("ssl.key.location").map(str::to_string);
        profile.ssl_key_password = get("ssl.key.password").map(str::to_string);
        for jks in ["ssl.truststore.location", "ssl.keystore.location"] {
            if get(jks).is_some() {
                notes.push(format!(
                    "{jks} is a Java keystore; export it to PEM and set ssl_ca_location / \
                     ssl_certificate_location instead"
                ));
            }
        }
        if let Some(ms) = get("request.timeout.ms").and_then(|v| v.parse().ok()) {
            profile.request_timeout_ms = Some(ms);
        }
        if let Some(url) = get("schema.registry.url") {
            // Basic auth: "KEY:SECRET" goes into the URL, as Ferrika expects.
            let user_info =
                get("basic.auth.user.info").or_else(|| get("schema.registry.basic.auth.user.info"));
            profile.schema_registry_url = Some(match user_info {
                Some(info) => match url.split_once("://") {
                    Some((scheme, rest)) => {
                        // Keep the user:secret colon; encode the rest so '/',
                        // '@' or '%' in a secret cannot break the URL.
                        let (user, secret) = info.split_once(':').unwrap_or((info, ""));
                        let encoded = format!(
                            "{}:{}",
                            crate::kafka::schema::path_segment(user),
                            crate::kafka::schema::path_segment(secret)
                        );
                        format!("{scheme}://{encoded}@{rest}")
                    }
                    None => url.to_string(),
                },
                None => url.to_string(),
            });
        }
        Ok((profile, notes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_mode_does_not_copy_os_stored_secrets_into_the_file() {
        let mut store = ProfileStore::default();
        let mut prod = ClusterProfile::new("prod", "b:9092");
        prod.sasl_password = Some("from-keyring".into());
        prod.schema_registry_url = Some("https://KEY:url-secret@sr.example.com".into());
        store.profiles.insert("prod".into(), prod);
        let mut dev = ClusterProfile::new("dev", "b:9092");
        // Changed in this session: no longer the stored value.
        dev.sasl_password = Some("new-password".into());
        store.profiles.insert("dev".into(), dev);
        let previous: BTreeSet<String> = ["prod".to_string(), "dev".to_string()].into();
        let kept = store.keep_os_secrets_out(&previous, |name, field| match (name, field) {
            ("prod", "sasl_password") => Some("from-keyring".into()),
            ("prod", "schema_registry_url") => Some("https://KEY:url-secret@sr.example.com".into()),
            ("dev", "sasl_password") => Some("old-password".into()),
            _ => None,
        });
        assert_eq!(kept, ["prod".to_string()].into());
        let prod = &store.profiles["prod"];
        assert_eq!(prod.sasl_password, None);
        let url = prod.schema_registry_url.as_deref().unwrap_or_default();
        assert!(!url.contains("url-secret"), "{url}");
        assert_eq!(
            store.profiles["dev"].sasl_password.as_deref(),
            Some("new-password")
        );
    }

    #[test]
    fn debug_redacts_profile_secrets() {
        let mut p = ClusterProfile::new("prod", "b:9092");
        p.sasl_username = Some("alice".into());
        p.sasl_password = Some("hunter2-sasl".into());
        p.ssl_key_password = Some("hunter2-key".into());
        p.schema_registry_url = Some("https://KEY:hunter2-url@sr.example.com".into());
        let shown = format!("{p:?}");
        assert!(!shown.contains("hunter2"), "{shown}");
        assert!(
            shown.contains("alice") && shown.contains("sr.example.com"),
            "{shown}"
        );
    }

    #[test]
    fn oauth_fields_roundtrip_and_stay_out_of_debug() {
        let mut p = ClusterProfile::new("cc", "b:9093");
        p.security_protocol = SecurityProtocol::SaslSsl;
        p.sasl_mechanism = Some(SaslMechanism::Oauthbearer);
        p.oauth_token_endpoint = Some("https://idp/token".into());
        p.oauth_client_id = Some("svc".into());
        p.oauth_client_secret = Some("hunter2-oauth".into());
        p.oauth_extensions
            .insert("logicalCluster".into(), "lkc-1".into());
        let shown = format!("{p:?}");
        assert!(!shown.contains("hunter2"), "{shown}");
        let config = p.to_kafka_config();
        assert_eq!(config.oauth.client_id.as_deref(), Some("svc"));
        assert!(config.validate().is_ok());
        let back = ClusterProfile::from_kafka_config("cc", &config);
        assert_eq!(back.oauth_extensions, p.oauth_extensions);
        assert_eq!(back.oauth_client_secret, p.oauth_client_secret);

        // Unset OAuth fields are not written.
        let json = serde_json::to_string(&ClusterProfile::new("x", "b:1")).unwrap();
        assert!(!json.contains("oauth") && !json.contains("aws"), "{json}");
        let json = serde_json::to_string(&p).unwrap();
        let parsed: ClusterProfile = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, p);

        // The client secret is a secret field for the OS credential store.
        let mut store = ProfileStore::default();
        store.add(p, false);
        assert_eq!(store.plaintext_secret_profiles(), vec!["cc".to_string()]);
    }

    #[test]
    fn imports_oauthbearer_properties() {
        let java = "bootstrap.servers=pkc:9092\nsecurity.protocol=SASL_SSL\n\
                    sasl.mechanism=OAUTHBEARER\n\
                    sasl.oauthbearer.token.endpoint.url=https://idp/oauth2/token\n\
                    sasl.login.callback.handler.class=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler\n\
                    sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required \
                    clientId=\"svc\" clientSecret=\"sec\" scope=\"kafka\" extension_logicalCluster=\"lkc-1\" extension_identityPoolId=\"pool\";\n";
        let (p, notes) = ClusterProfile::from_properties("cc", java).unwrap();
        assert!(notes.is_empty(), "{notes:?}");
        assert_eq!(p.sasl_mechanism, Some(SaslMechanism::Oauthbearer));
        assert_eq!(
            p.oauth_token_endpoint.as_deref(),
            Some("https://idp/oauth2/token")
        );
        assert_eq!(p.oauth_client_id.as_deref(), Some("svc"));
        assert_eq!(p.oauth_client_secret.as_deref(), Some("sec"));
        assert_eq!(p.oauth_scope.as_deref(), Some("kafka"));
        assert_eq!(p.oauth_extensions.len(), 2);
        assert_eq!(p.sasl_username, None);
        assert!(p.to_kafka_config().validate().is_ok());

        let rdkafka = "bootstrap.servers=b:9093\nsecurity.protocol=sasl_ssl\nsasl.mechanisms=OAUTHBEARER\n\
                       sasl.oauthbearer.method=oidc\nsasl.oauthbearer.client.id=c\n\
                       sasl.oauthbearer.client.secret=s\nsasl.oauthbearer.token.endpoint.url=https://t\n\
                       sasl.oauthbearer.extensions=a=1,b=2\n";
        let (p, _) = ClusterProfile::from_properties("r", rdkafka).unwrap();
        assert_eq!(p.oauth_method, Some(OAuthMethod::Oidc));
        assert_eq!(p.oauth_client_secret.as_deref(), Some("s"));
        assert_eq!(p.oauth_extensions.get("b").map(String::as_str), Some("2"));

        let msk = "bootstrap.servers=b-1.x.c2.kafka.us-east-1.amazonaws.com:9098\n\
                   security.protocol=SASL_SSL\nsasl.mechanism=AWS_MSK_IAM\n\
                   sasl.jaas.config=software.amazon.msk.auth.iam.IAMLoginModule required awsProfileName=\"prod\";\n\
                   sasl.client.callback.handler.class=software.amazon.msk.auth.iam.IAMClientCallbackHandler\n";
        let (p, _) = ClusterProfile::from_properties("msk", msk).unwrap();
        assert!(p.uses_msk_iam());
        assert_eq!(p.aws_profile.as_deref(), Some("prod"));

        let msk_oauth = "bootstrap.servers=b:9098\nsecurity.protocol=SASL_SSL\nsasl.mechanism=OAUTHBEARER\n\
                         sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;\n\
                         sasl.login.callback.handler.class=software.amazon.msk.auth.iam.IAMOAuthBearerLoginCallbackHandler\n";
        let (p, _) = ClusterProfile::from_properties("m2", msk_oauth).unwrap();
        assert!(p.uses_msk_iam());
    }

    #[test]
    fn save_writes_atomically_with_private_permissions() {
        let dir = std::env::temp_dir().join(format!("ferrika_private_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let path = dir.join("nested").join("profiles.json");
        write_private_file_in(&path, b"one");
        write_private_file_in(&path, b"two");
        assert_eq!(fs::read(&path).unwrap(), b"two");
        let leftovers: Vec<_> = fs::read_dir(path.parent().unwrap())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name() != "profiles.json")
            .collect();
        assert!(leftovers.is_empty(), "temp file left behind");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = fs::metadata(&path).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600);
            let mode = fs::metadata(path.parent().unwrap())
                .unwrap()
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o700);
        }
        let _ = fs::remove_dir_all(&dir);
    }

    fn write_private_file_in(path: &Path, contents: &[u8]) {
        create_private_dir(path.parent().unwrap()).unwrap();
        write_private_file(path, contents).unwrap();
    }

    #[test]
    fn profiles_import_from_client_properties() {
        let text = r#"
# Confluent Cloud style
bootstrap.servers=pkc-1.confluent.cloud:9092
security.protocol=SASL_SSL
sasl.mechanisms=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
    username="KEY" password="SE@CRET";
schema.registry.url=https://psrc-1.confluent.cloud
basic.auth.credentials.source=USER_INFO
basic.auth.user.info=SR:p@s/s
ssl.truststore.location=/etc/truststore.jks
"#;
        let (p, notes) = ClusterProfile::from_properties("cloud", text).unwrap();
        assert_eq!(p.bootstrap_servers, "pkc-1.confluent.cloud:9092");
        assert_eq!(p.security_protocol, SecurityProtocol::SaslSsl);
        assert_eq!(p.sasl_mechanism, Some(SaslMechanism::Plain));
        assert_eq!(p.sasl_username.as_deref(), Some("KEY"));
        assert_eq!(p.sasl_password.as_deref(), Some("SE@CRET"));
        assert_eq!(
            p.schema_registry_url.as_deref(),
            Some("https://SR:p%40s%2Fs@psrc-1.confluent.cloud")
        );
        assert_eq!(notes.len(), 1);
        assert!(ClusterProfile::from_properties("x", "security.protocol=SSL").is_err());
        let props = parse_properties("a\\:b=c\\\\\nd=x\\u0041\\=");
        assert_eq!(props.get("a:b").map(String::as_str), Some("c\\"));
        assert_eq!(props.get("d").map(String::as_str), Some("xA="));
    }

    #[test]
    fn url_password_is_split_off() {
        assert_eq!(
            url_without_password("https://KEY:SE:CRET@psrc.example.com/x@y").as_deref(),
            Some("https://KEY@psrc.example.com/x@y")
        );
        assert_eq!(url_without_password("https://KEY@host"), None);
        assert_eq!(
            mask_url_password("https://KEY:SECRET@host/a", "***"),
            "https://KEY:***@host/a"
        );
        assert_eq!(mask_url_password("http://host:1", "***"), "http://host:1");
        assert_eq!(url_without_password("http://host:8081"), None);
        assert_eq!(public_part("sasl_password", "pw"), Some(None));
        assert_eq!(public_part("connect_url", "http://host:8083"), None);
    }

    #[test]
    fn test_profile_default_store() {
        let store = ProfileStore::default();
        assert_eq!(store.active_profile.as_deref(), Some("local"));
        assert_eq!(store.profiles.len(), 1);

        let active = store.get_active().expect("active profile exists");
        assert_eq!(active.name, "local");
        assert_eq!(active.bootstrap_servers, "localhost:9092");
    }

    #[test]
    fn test_profile_add_and_set_active() {
        let mut store = ProfileStore::default();
        let staging = ClusterProfile::new("staging", "staging-broker:9092")
            .with_description("Staging cluster")
            .with_sasl_plain("alice", "secret123", true);

        store.add(staging, true);
        assert_eq!(store.active_profile.as_deref(), Some("staging"));
        assert_eq!(store.profiles.len(), 2);

        let active = store.get_active().expect("active profile");
        assert_eq!(active.name, "staging");
        assert_eq!(active.security_protocol, SecurityProtocol::SaslSsl);
        assert_eq!(active.sasl_username.as_deref(), Some("alice"));

        let kafka_cfg = active.to_kafka_config();
        assert_eq!(kafka_cfg.bootstrap_servers, "staging-broker:9092");
        assert_eq!(kafka_cfg.security_protocol, SecurityProtocol::SaslSsl);

        // Switch back to local
        store.set_active("local").expect("set active local");
        assert_eq!(store.active_profile.as_deref(), Some("local"));
    }

    #[test]
    fn test_profile_remove() {
        let mut store = ProfileStore::default();
        let prod = ClusterProfile::new("prod", "prod-broker:9092");
        store.add(prod, true);
        assert_eq!(store.active_profile.as_deref(), Some("prod"));

        let removed = store.remove("prod").expect("remove prod");
        assert_eq!(removed.name, "prod");
        assert_eq!(store.profiles.len(), 1);
        // Active should fallback to remaining profile
        assert_eq!(store.active_profile.as_deref(), Some("local"));

        // Removing non-existent profile should error
        let err = store.remove("unknown");
        assert!(err.is_err());
    }

    #[test]
    fn test_profile_file_save_and_load() {
        let temp_dir = std::env::temp_dir().join(format!("ferrika_test_{}", std::process::id()));
        let config_path = temp_dir.join("profiles.json");

        let mut store = ProfileStore::default();
        store.add(ClusterProfile::new("custom", "custom-broker:9092"), true);

        store.save_to_path(&config_path).expect("save to temp file");
        assert!(config_path.exists());

        let loaded = ProfileStore::load_from_path(&config_path).expect("load from temp file");
        assert_eq!(loaded.active_profile.as_deref(), Some("custom"));
        assert_eq!(loaded.profiles.len(), 2);

        // Cleanup
        let _ = fs::remove_dir_all(&temp_dir);
    }
}
