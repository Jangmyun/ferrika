//! MQTT connection profiles: `mqtt-profiles.json` next to the Kafka
//! `profiles.json` (so `FERRIKA_CONFIG_DIR` and `FERRIKA_CONFIG_PATH` move
//! both), kept apart from the Kafka and RabbitMQ profiles: an MQTT broker
//! is neither a Kafka cluster nor a RabbitMQ management API.
//!
//! Passwords go to the OS credential store through
//! `ferrika_core::kafka::secrets`, under the account
//! `<profile>/mqtt_password`. No Kafka or RabbitMQ profile field has that
//! name, so profiles of different brokers with the same name never share an
//! entry. Where no credential store is available (or
//! `FERRIKA_SECRET_STORE=file`), the password stays in the file, which is
//! written owner-only.
//!
//! Broker URLs never carry credentials: the user and password are fields
//! of their own.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{MqttError, MqttResult};

/// The file name, next to `profiles.json`.
pub const FILE_NAME: &str = "mqtt-profiles.json";

/// The credential store field of a profile's password.
pub const PASSWORD_FIELD: &str = "mqtt_password";

/// The plain MQTT port.
pub const MQTT_PORT: u16 = 1883;
/// The MQTT over TLS port.
pub const MQTTS_PORT: u16 = 8883;

/// The MQTT protocol version to speak.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum MqttVersion {
    /// MQTT 3.1.1: every broker and MQTT bridge speaks it.
    #[default]
    #[serde(rename = "3.1.1")]
    V311,
    /// MQTT 5: reason codes on acknowledgements, user properties and
    /// content types.
    #[serde(rename = "5")]
    V5,
}

impl MqttVersion {
    /// `3.1.1` or `5`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::V311 => "3.1.1",
            Self::V5 => "5",
        }
    }

    /// Parses `3.1.1` (also `3`, `311`, `4`, the protocol level) or `5`.
    pub fn parse(text: &str) -> MqttResult<Self> {
        match text.trim() {
            "3.1.1" | "3" | "311" | "4" => Ok(Self::V311),
            "5" | "5.0" => Ok(Self::V5),
            other => Err(MqttError::config(format!(
                "Unknown MQTT version '{other}': use 3.1.1 or 5"
            ))),
        }
    }
}

impl std::fmt::Display for MqttVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A broker address from an `mqtt://` or `mqtts://` URL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerUrl {
    /// `mqtts://`: TLS.
    pub tls: bool,
    /// The host; an IPv6 address without brackets.
    pub host: String,
    pub port: u16,
}

impl BrokerUrl {
    /// Parses `mqtt://host[:port]` (port 1883) or `mqtts://host[:port]`
    /// (port 8883). A URL with a user or password is refused, so no
    /// password ends up in a profile file or in shell history by way of the
    /// URL; so is a path, which MQTT has no use for.
    pub fn parse(url: &str) -> MqttResult<Self> {
        let url = url.trim();
        let (scheme, rest) = url.split_once("://").ok_or_else(|| {
            MqttError::config("The broker URL needs mqtt:// or mqtts://, e.g. mqtt://broker:1883")
        })?;
        let tls = match scheme.to_ascii_lowercase().as_str() {
            "mqtt" | "tcp" => false,
            "mqtts" | "ssl" => true,
            other => {
                return Err(MqttError::config(format!(
                    "Unsupported broker URL scheme '{other}': use mqtt:// or mqtts://"
                )))
            }
        };
        let (authority, path) = match rest.find('/') {
            Some(i) => (&rest[..i], &rest[i..]),
            None => (rest, ""),
        };
        if authority.contains('@') {
            return Err(MqttError::config(
                "The broker URL must not contain a user or password; give them with \
                 --mqtt-user and FERRIKA_MQTT_PASSWORD or --mqtt-password-stdin",
            ));
        }
        if !path.is_empty() && path != "/" {
            return Err(MqttError::config(
                "The broker URL must not have a path: use mqtt://host:port",
            ));
        }
        if authority.contains(['?', '#']) {
            return Err(MqttError::config(
                "The broker URL must not have a query: use mqtt://host:port",
            ));
        }
        let (host, port) = if let Some(inner) = authority.strip_prefix('[') {
            let end = inner
                .find(']')
                .ok_or_else(|| MqttError::config("The broker URL has an unclosed [ in its host"))?;
            let host = &inner[..end];
            let after = &inner[end + 1..];
            let port = match after.strip_prefix(':') {
                Some(port) => Some(port),
                None if after.is_empty() => None,
                None => return Err(MqttError::config("The broker URL has no valid port")),
            };
            (host, port)
        } else {
            match authority.rsplit_once(':') {
                Some((host, port)) => (host, Some(port)),
                None => (authority, None),
            }
        };
        if host.is_empty() || host.chars().any(|c| c.is_whitespace() || c.is_control()) {
            return Err(MqttError::config("The broker URL has no valid host"));
        }
        let port = match port {
            Some(port) => port
                .parse::<u16>()
                .ok()
                .filter(|p| *p != 0)
                .ok_or_else(|| MqttError::config(format!("Invalid port '{port}'")))?,
            None if tls => MQTTS_PORT,
            None => MQTT_PORT,
        };
        Ok(Self {
            tls,
            host: host.to_string(),
            port,
        })
    }

    /// `host:port` (IPv6 hosts in brackets): what the company policy's
    /// broker-address rules and the audit log see.
    pub fn address(&self) -> String {
        if self.host.contains(':') {
            format!("[{}]:{}", self.host, self.port)
        } else {
            format!("{}:{}", self.host, self.port)
        }
    }
}

/// One MQTT broker connection.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MqttProfile {
    /// The broker, e.g. `mqtts://broker.prod.corp:8883`. What the company
    /// policy's broker-address rules match (its `host:port`).
    pub url: String,
    /// The protocol version (default 3.1.1).
    #[serde(default)]
    pub version: MqttVersion,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
    /// The password: filled in from the OS credential store on load, and
    /// written to the file only when the store cannot keep it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
    /// Whether the password is in the OS credential store.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub password_in_store: bool,
    /// A PEM file with the CA certificates to trust for `mqtts://` (instead
    /// of the OS trust store).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ca_file: Option<PathBuf>,
    /// Refuse commands that change this broker's messages (`pub`,
    /// `retained clear`).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub read_only: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

// Manual so the password never reaches debug output.
impl std::fmt::Debug for MqttProfile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MqttProfile")
            .field("url", &self.url)
            .field("version", &self.version)
            .field("user", &self.user)
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .field("password_in_store", &self.password_in_store)
            .field("ca_file", &self.ca_file)
            .field("read_only", &self.read_only)
            .field("description", &self.description)
            .finish()
    }
}

impl MqttProfile {
    /// A profile for the broker at `url`, MQTT 3.1.1, no login.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            version: MqttVersion::default(),
            user: None,
            password: None,
            password_in_store: false,
            ca_file: None,
            read_only: false,
            description: None,
        }
    }

    /// Checks the URL (parseable, no credentials) and that a CA file goes
    /// with TLS.
    pub fn validate(&self) -> MqttResult<()> {
        let url = BrokerUrl::parse(&self.url)?;
        if self.ca_file.is_some() && !url.tls {
            return Err(MqttError::config("A CA file needs an mqtts:// URL (TLS)"));
        }
        if self.password.is_some() && self.user.is_none() {
            return Err(MqttError::config("A password needs a user"));
        }
        Ok(())
    }
}

/// Where passwords are kept. The OS credential store in production; a
/// map in tests.
pub trait SecretStore {
    /// Whether new secrets should be written to the store.
    fn enabled(&self) -> bool;
    fn get(&self, profile: &str) -> MqttResult<Option<String>>;
    fn set(&self, profile: &str, password: &str) -> MqttResult<()>;
    fn delete(&self, profile: &str) -> MqttResult<()>;
}

/// The OS credential store (`ferrika_core::kafka::secrets`), account
/// `<profile>/mqtt_password`.
#[derive(Debug, Clone, Copy, Default)]
pub struct OsSecretStore;

impl SecretStore for OsSecretStore {
    fn enabled(&self) -> bool {
        ferrika_core::kafka::secrets::enabled()
    }

    fn get(&self, profile: &str) -> MqttResult<Option<String>> {
        ferrika_core::kafka::secrets::get(profile, PASSWORD_FIELD).map_err(secret_error)
    }

    fn set(&self, profile: &str, password: &str) -> MqttResult<()> {
        ferrika_core::kafka::secrets::set(profile, PASSWORD_FIELD, password).map_err(secret_error)
    }

    fn delete(&self, profile: &str) -> MqttResult<()> {
        ferrika_core::kafka::secrets::delete(profile, PASSWORD_FIELD).map_err(secret_error)
    }
}

fn secret_error(e: ferrika_core::error::KafkaError) -> MqttError {
    MqttError::config(format!("OS credential store: {e}"))
}

/// The MQTT profiles and which one is active.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct MqttProfileStore {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_profile: Option<String>,
    #[serde(default)]
    pub profiles: BTreeMap<String, MqttProfile>,
}

impl MqttProfileStore {
    /// `mqtt-profiles.json` in the directory of `profiles.json`.
    pub fn default_path() -> PathBuf {
        path_next_to(&ferrika_core::kafka::profile::ProfileStore::default_path())
    }

    /// Loads the store from the default path, passwords from the OS
    /// credential store. A missing file is an empty store.
    pub fn load() -> MqttResult<Self> {
        Self::load_with(&Self::default_path(), &OsSecretStore)
    }

    /// Saves the store to the default path, passwords to the OS
    /// credential store where possible.
    pub fn save(&self) -> MqttResult<()> {
        self.save_with(&Self::default_path(), &OsSecretStore)
    }

    /// Loads `path` and fills in the passwords kept in `secrets`. A
    /// password the store cannot read is left out (with a warning): the
    /// command then fails to log in rather than not start.
    pub fn load_with(path: &Path, secrets: &impl SecretStore) -> MqttResult<Self> {
        let mut store = Self::load_file(path)?;
        for (name, profile) in &mut store.profiles {
            if !profile.password_in_store {
                continue;
            }
            match secrets.get(name) {
                Ok(password) => profile.password = password,
                Err(e) => tracing::warn!(
                    "Cannot read the password of MQTT profile '{name}' from the OS credential \
                     store: {e}"
                ),
            }
        }
        Ok(store)
    }

    /// The file as it is, without passwords from the credential store.
    pub fn load_file(path: &Path) -> MqttResult<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let content = std::fs::read_to_string(path)
            .map_err(|e| MqttError::config(format!("Cannot read {}: {e}", path.display())))?;
        serde_json::from_str(&content)
            .map_err(|e| MqttError::config(format!("Cannot parse {}: {e}", path.display())))
    }

    /// Writes the store to `path`: passwords to `secrets` when it is
    /// enabled (else, or when storing fails, into the file), and removes
    /// the stored passwords of profiles that are gone.
    pub fn save_with(&self, path: &Path, secrets: &impl SecretStore) -> MqttResult<()> {
        let previous = Self::load_file(path).unwrap_or_default();
        let stored = self.for_file(secrets);
        stored.write_file(path)?;
        // Only once the file no longer points at them.
        for (name, old) in &previous.profiles {
            let gone = stored
                .profiles
                .get(name)
                .is_none_or(|p| !p.password_in_store);
            if old.password_in_store && gone {
                if let Err(e) = secrets.delete(name) {
                    tracing::warn!(
                        "Cannot remove the password of MQTT profile '{name}' from the OS \
                         credential store: {e}"
                    );
                }
            }
        }
        Ok(())
    }

    /// The store as written to disk: passwords moved to `secrets` where
    /// possible.
    fn for_file(&self, secrets: &impl SecretStore) -> Self {
        let mut stored = self.clone();
        for (name, profile) in &mut stored.profiles {
            let Some(password) = profile.password.clone() else {
                continue;
            };
            if !secrets.enabled() {
                profile.password_in_store = false;
                continue;
            }
            match secrets.set(name, &password) {
                Ok(()) => {
                    profile.password = None;
                    profile.password_in_store = true;
                }
                Err(e) => {
                    tracing::warn!(
                        "Cannot store the password of MQTT profile '{name}' in the OS \
                         credential store, keeping it in {FILE_NAME}: {e}"
                    );
                    profile.password_in_store = false;
                }
            }
        }
        stored
    }

    fn write_file(&self, path: &Path) -> MqttResult<()> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() && !parent.exists() {
                create_private_dir(parent).map_err(|e| {
                    MqttError::config(format!("Cannot create {}: {e}", parent.display()))
                })?;
            }
        }
        let json = serde_json::to_string_pretty(self)
            .map_err(|e| MqttError::config(format!("Cannot serialize the profiles: {e}")))?;
        ferrika_core::files::write_private_file(path, json.as_bytes())
            .map_err(|e| MqttError::config(format!("Cannot write {}: {e}", path.display())))
    }

    /// Adds or replaces `name` after checking it. The first profile becomes
    /// the active one.
    pub fn insert(&mut self, name: &str, profile: MqttProfile) -> MqttResult<()> {
        validate_name(name)?;
        profile.validate()?;
        self.profiles.insert(name.to_string(), profile);
        if self.active_profile.is_none() {
            self.active_profile = Some(name.to_string());
        }
        Ok(())
    }

    /// Removes `name`; the active profile is cleared when it was that one.
    pub fn remove(&mut self, name: &str) -> MqttResult<MqttProfile> {
        let removed = self
            .profiles
            .remove(name)
            .ok_or_else(|| unknown_profile(name))?;
        if self.active_profile.as_deref() == Some(name) {
            self.active_profile = None;
        }
        Ok(removed)
    }

    /// Makes `name` the profile `mqtt` commands use by default.
    pub fn set_active(&mut self, name: &str) -> MqttResult<()> {
        if !self.profiles.contains_key(name) {
            return Err(unknown_profile(name));
        }
        self.active_profile = Some(name.to_string());
        Ok(())
    }

    /// The named profile.
    pub fn get(&self, name: &str) -> MqttResult<&MqttProfile> {
        self.profiles.get(name).ok_or_else(|| unknown_profile(name))
    }

    /// The active profile, if one is set and exists.
    pub fn active(&self) -> Option<(&str, &MqttProfile)> {
        let name = self.active_profile.as_deref()?;
        self.profiles.get(name).map(|p| (name, p))
    }
}

/// `mqtt-profiles.json` next to `profiles`.
pub fn path_next_to(profiles: &Path) -> PathBuf {
    profiles
        .parent()
        .map_or_else(|| PathBuf::from(FILE_NAME), |dir| dir.join(FILE_NAME))
}

/// A profile name: letters, digits, `-`, `_` and `.`, at most 64.
pub fn validate_name(name: &str) -> MqttResult<()> {
    let valid = !name.is_empty()
        && name.len() <= 64
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'));
    if valid {
        Ok(())
    } else {
        Err(MqttError::config(format!(
            "Invalid profile name '{name}': use letters, digits, '-', '_' or '.' (at most 64)"
        )))
    }
}

fn unknown_profile(name: &str) -> MqttError {
    MqttError::config(format!(
        "No MQTT profile '{name}' (see `ferrika mqtt profile list`)"
    ))
}

/// Creates `dir` (and missing parents); owner-only (0700) on Unix.
fn create_private_dir(dir: &Path) -> std::io::Result<()> {
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(dir)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::collections::HashMap;

    /// An in-memory credential store.
    #[derive(Default)]
    pub(crate) struct MemorySecrets {
        pub(crate) disabled: bool,
        pub(crate) entries: RefCell<HashMap<String, String>>,
    }

    impl SecretStore for MemorySecrets {
        fn enabled(&self) -> bool {
            !self.disabled
        }
        fn get(&self, profile: &str) -> MqttResult<Option<String>> {
            Ok(self.entries.borrow().get(profile).cloned())
        }
        fn set(&self, profile: &str, password: &str) -> MqttResult<()> {
            self.entries
                .borrow_mut()
                .insert(profile.to_string(), password.to_string());
            Ok(())
        }
        fn delete(&self, profile: &str) -> MqttResult<()> {
            self.entries.borrow_mut().remove(profile);
            Ok(())
        }
    }

    fn temp_path(test: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "ferrika-mqtt-profiles-{test}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        dir.join(FILE_NAME)
    }

    fn prod() -> MqttProfile {
        MqttProfile {
            user: Some("ops".to_string()),
            password: Some("s3cret-pw".to_string()),
            version: MqttVersion::V5,
            ..MqttProfile::new("mqtts://broker.prod.corp")
        }
    }

    #[test]
    fn the_file_sits_next_to_profiles_json() {
        assert_eq!(
            path_next_to(Path::new("/home/u/.ferrika/profiles.json")),
            Path::new("/home/u/.ferrika").join(FILE_NAME)
        );
        assert_eq!(path_next_to(Path::new("")), PathBuf::from(FILE_NAME));
    }

    #[test]
    fn passwords_go_to_the_credential_store_not_the_file() {
        let path = temp_path("keyring");
        let secrets = MemorySecrets::default();
        let mut store = MqttProfileStore::default();
        store.insert("prod", prod()).unwrap();
        assert_eq!(store.active_profile.as_deref(), Some("prod"));
        store.save_with(&path, &secrets).unwrap();

        let file = std::fs::read_to_string(&path).unwrap();
        assert!(!file.contains("s3cret-pw"), "{file}");
        assert!(file.contains("password_in_store"), "{file}");
        assert!(file.contains("\"version\": \"5\""), "{file}");
        assert_eq!(
            secrets.entries.borrow().get("prod").map(String::as_str),
            Some("s3cret-pw")
        );

        let loaded = MqttProfileStore::load_with(&path, &secrets).unwrap();
        let profile = loaded.get("prod").unwrap();
        assert_eq!(profile.password.as_deref(), Some("s3cret-pw"));
        assert_eq!(profile.version, MqttVersion::V5);
        assert_eq!(loaded.active().unwrap().0, "prod");

        // Removing the profile removes its stored password.
        let mut loaded = loaded;
        loaded.remove("prod").unwrap();
        assert_eq!(loaded.active_profile, None);
        loaded.save_with(&path, &secrets).unwrap();
        assert!(secrets.entries.borrow().is_empty());
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn without_a_credential_store_the_password_stays_in_the_file() {
        let path = temp_path("file");
        let secrets = MemorySecrets {
            disabled: true,
            ..MemorySecrets::default()
        };
        let mut store = MqttProfileStore::default();
        store.insert("prod", prod()).unwrap();
        store.save_with(&path, &secrets).unwrap();
        let loaded = MqttProfileStore::load_with(&path, &secrets).unwrap();
        let profile = loaded.get("prod").unwrap();
        assert_eq!(profile.password.as_deref(), Some("s3cret-pw"));
        assert!(!profile.password_in_store);
        assert!(secrets.entries.borrow().is_empty());
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn a_missing_file_is_an_empty_store_and_old_files_default_to_311() {
        let store =
            MqttProfileStore::load_with(&temp_path("missing"), &MemorySecrets::default()).unwrap();
        assert!(store.profiles.is_empty());
        assert!(store.active().is_none());
        let parsed: MqttProfileStore =
            serde_json::from_str(r#"{"profiles":{"a":{"url":"mqtt://b"}}}"#).unwrap();
        assert_eq!(parsed.get("a").unwrap().version, MqttVersion::V311);
    }

    #[test]
    fn profiles_are_checked_before_they_are_stored() {
        let mut store = MqttProfileStore::default();
        assert!(store
            .insert("a", MqttProfile::new("mqtt://ops:pw@broker"))
            .is_err());
        assert!(store
            .insert("a", MqttProfile::new("http://broker"))
            .is_err());
        assert!(store
            .insert("bad name", MqttProfile::new("mqtt://broker"))
            .is_err());
        let ca_without_tls = MqttProfile {
            ca_file: Some(PathBuf::from("ca.pem")),
            ..MqttProfile::new("mqtt://broker")
        };
        assert!(store.insert("a", ca_without_tls).is_err());
        let password_without_user = MqttProfile {
            password: Some("pw".to_string()),
            ..MqttProfile::new("mqtt://broker")
        };
        assert!(store.insert("a", password_without_user).is_err());
        assert!(store.set_active("nope").is_err());
        assert!(store.remove("nope").is_err());
        assert!(store.profiles.is_empty());
    }

    #[test]
    fn debug_output_hides_the_password() {
        let debug = format!("{:?}", prod());
        assert!(!debug.contains("s3cret-pw"), "{debug}");
        assert!(debug.contains("<redacted>"), "{debug}");
    }

    #[test]
    fn broker_urls_get_their_default_port() {
        let plain = BrokerUrl::parse("mqtt://broker").unwrap();
        assert_eq!((plain.tls, plain.address()), (false, "broker:1883".into()));
        let tls = BrokerUrl::parse(" MQTTS://broker.prod.corp/ ").unwrap();
        assert_eq!(
            (tls.tls, tls.address()),
            (true, "broker.prod.corp:8883".into())
        );
        let v6 = BrokerUrl::parse("mqtt://[::1]:1884").unwrap();
        assert_eq!(v6.host, "::1");
        assert_eq!(v6.address(), "[::1]:1884");
        assert_eq!(
            BrokerUrl::parse("ssl://b:9000").unwrap().address(),
            "b:9000"
        );
        for bad in [
            "broker:1883",
            "http://broker",
            "mqtt://",
            "mqtt://:1883",
            "mqtt://b:0",
            "mqtt://b:x",
            "mqtt://u:p@b",
            "mqtt://b/topic",
            "mqtt://[::1",
        ] {
            assert!(BrokerUrl::parse(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn versions_parse_and_print() {
        assert_eq!(MqttVersion::parse("3.1.1").unwrap(), MqttVersion::V311);
        assert_eq!(MqttVersion::parse("4").unwrap(), MqttVersion::V311);
        assert_eq!(MqttVersion::parse(" 5 ").unwrap(), MqttVersion::V5);
        assert!(MqttVersion::parse("3.1").is_err());
        assert_eq!(MqttVersion::V5.to_string(), "5");
    }
}
