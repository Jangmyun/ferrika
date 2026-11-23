//! RabbitMQ connection profiles: `rabbit-profiles.json` next to the Kafka
//! `profiles.json` (so `FERRIKA_CONFIG_DIR` and `FERRIKA_CONFIG_PATH` move
//! both), kept apart from the Kafka profiles on purpose: a RabbitMQ
//! connection is not a Kafka cluster, and the Kafka `ClusterProfile` stays
//! as it is.
//!
//! Passwords go to the OS credential store through
//! `ferrika_core::kafka::secrets`, under the account
//! `<profile>/rabbitmq_password` (with the config directory's suffix outside
//! the default directory, see `ferrika_core::kafka::secrets`). No Kafka
//! profile field has that name, so a RabbitMQ and a Kafka profile with the
//! same name never share an entry.
//! Where no credential store is available (or `FERRIKA_SECRET_STORE=file`),
//! the password stays in the file, as for Kafka profiles.
//!
//! URLs in a profile must not carry credentials: the user and password are
//! fields of their own, so the file never holds a password in a URL.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{RabbitError, RabbitResult};

/// The file name, next to `profiles.json`.
pub const FILE_NAME: &str = "rabbit-profiles.json";

/// The credential store field of a profile's password.
pub const PASSWORD_FIELD: &str = "rabbitmq_password";

/// AMQP 0-9-1 ports.
pub const AMQP_PORT: u16 = 5672;
pub const AMQPS_PORT: u16 = 5671;

/// One RabbitMQ connection.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RabbitProfile {
    /// The management API, e.g. `https://rabbit.prod.corp:15671`. What the
    /// company policy's broker-address rules match (its `host:port`).
    pub management_url: String,
    /// The virtual host commands use.
    #[serde(default = "default_vhost")]
    pub vhost: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
    /// The password: filled in from the OS credential store on load, and
    /// written to the file only when the store cannot keep it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
    /// Whether the password is in the OS credential store.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub password_in_store: bool,
    /// The AMQP listener for moving messages, e.g. `amqps://rabbit:5671`.
    /// Without it: the management host on 5672 (5671 with TLS when the
    /// management URL is https).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub amqp_url: Option<String>,
    /// Refuse commands that change this broker's messages.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub read_only: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

fn default_vhost() -> String {
    "/".to_string()
}

// Manual so the password never reaches debug output.
impl std::fmt::Debug for RabbitProfile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RabbitProfile")
            .field("management_url", &self.management_url)
            .field("vhost", &self.vhost)
            .field("user", &self.user)
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .field("password_in_store", &self.password_in_store)
            .field("amqp_url", &self.amqp_url)
            .field("read_only", &self.read_only)
            .field("description", &self.description)
            .finish()
    }
}

impl RabbitProfile {
    /// A profile for the management API at `management_url`, vhost `/`.
    pub fn new(management_url: impl Into<String>) -> Self {
        Self {
            management_url: management_url.into(),
            vhost: default_vhost(),
            user: None,
            password: None,
            password_in_store: false,
            amqp_url: None,
            read_only: false,
            description: None,
        }
    }

    /// Checks the URLs: parseable, no credentials in them.
    pub fn validate(&self) -> RabbitResult<()> {
        if has_userinfo(&self.management_url) {
            return Err(RabbitError::config(
                "The management URL must not contain a user or password; use the profile's \
                 user and password",
            ));
        }
        crate::client::ManagementClient::new(&self.management_url)?;
        if let Some(amqp) = &self.amqp_url {
            if has_userinfo(amqp) {
                return Err(RabbitError::config(
                    "The AMQP URL must not contain a user or password; use the profile's user \
                     and password",
                ));
            }
            amqp_host_port(amqp)?;
        }
        if self.vhost.is_empty() {
            return Err(RabbitError::config("The vhost must not be empty"));
        }
        Ok(())
    }

    /// The AMQP URL for moving messages: [`Self::amqp_url`], else derived
    /// from the management URL (see [`derive_amqp_url`]). Never holds
    /// credentials.
    pub fn amqp_url(&self) -> RabbitResult<String> {
        match &self.amqp_url {
            Some(url) => Ok(url.clone()),
            None => derive_amqp_url(&self.management_url),
        }
    }
}

/// The AMQP URL of the broker behind `management_url`: the same host,
/// `amqps://host:5671` when the management API is https, else
/// `amqp://host:5672`. Credentials and paths in `management_url` are
/// dropped.
pub fn derive_amqp_url(management_url: &str) -> RabbitResult<String> {
    let url = management_url.trim();
    let (scheme, rest) = url
        .split_once("://")
        .ok_or_else(|| RabbitError::config("The management URL needs http:// or https://"))?;
    let tls = match scheme.to_ascii_lowercase().as_str() {
        "https" => true,
        "http" => false,
        _ => {
            return Err(RabbitError::config(
                "The management URL needs http:// or https://",
            ))
        }
    };
    let host = host_of(rest)?;
    Ok(if tls {
        format!("amqps://{host}:{AMQPS_PORT}")
    } else {
        format!("amqp://{host}:{AMQP_PORT}")
    })
}

/// `host:port` of an `amqp://` or `amqps://` URL (5672 / 5671 when it has
/// no port): what error messages name.
pub fn amqp_host_port(url: &str) -> RabbitResult<String> {
    let url = url.trim();
    let (scheme, rest) = url
        .split_once("://")
        .ok_or_else(|| RabbitError::config("The AMQP URL needs amqp:// or amqps://"))?;
    let default_port = match scheme.to_ascii_lowercase().as_str() {
        "amqps" => AMQPS_PORT,
        "amqp" => AMQP_PORT,
        _ => {
            return Err(RabbitError::config(
                "The AMQP URL needs amqp:// or amqps://",
            ))
        }
    };
    let host = host_of(rest)?;
    let authority = authority_of(rest);
    let port = port_of(authority).unwrap_or(default_port);
    Ok(format!("{host}:{port}"))
}

/// The authority of `rest` (what follows `scheme://`) without user info.
fn authority_of(rest: &str) -> &str {
    let end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let authority = &rest[..end];
    authority
        .rfind('@')
        .map_or(authority, |at| &authority[at + 1..])
}

/// The host of `rest` (an IPv6 address keeps its brackets).
fn host_of(rest: &str) -> RabbitResult<String> {
    let authority = authority_of(rest);
    let host = if authority.starts_with('[') {
        authority
            .find(']')
            .map(|end| &authority[..=end])
            .unwrap_or(authority)
    } else {
        authority
            .rsplit_once(':')
            .map_or(authority, |(host, _)| host)
    };
    if host.is_empty() || host.chars().any(|c| c.is_whitespace() || c.is_control()) {
        return Err(RabbitError::config("The URL has no valid host"));
    }
    Ok(host.to_string())
}

/// The port of an authority, when it names one.
fn port_of(authority: &str) -> Option<u16> {
    let after_host = match authority.rfind(']') {
        Some(end) => &authority[end + 1..],
        None => authority,
    };
    after_host
        .rsplit_once(':')
        .and_then(|(_, port)| port.parse().ok())
}

/// Whether a URL carries user info (`user@` or `user:pass@`).
fn has_userinfo(url: &str) -> bool {
    let Some((_, rest)) = url.split_once("://") else {
        return false;
    };
    let end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    rest[..end].contains('@')
}

/// The user and password in a URL's user info, percent-decoded (moved to
/// `ferrika_core::kafka::profile::url_credentials`; re-exported here).
pub use ferrika_core::kafka::profile::url_credentials;

/// Where passwords are kept. The OS credential store in production; a
/// map in tests.
pub trait SecretStore {
    /// Whether new secrets should be written to the store.
    fn enabled(&self) -> bool;
    fn get(&self, profile: &str) -> RabbitResult<Option<String>>;
    fn set(&self, profile: &str, password: &str) -> RabbitResult<()>;
    fn delete(&self, profile: &str) -> RabbitResult<()>;
}

/// The OS credential store (`ferrika_core::kafka::secrets`), account
/// `<profile>/rabbitmq_password`.
#[derive(Debug, Clone, Copy, Default)]
pub struct OsSecretStore;

impl SecretStore for OsSecretStore {
    fn enabled(&self) -> bool {
        ferrika_core::kafka::secrets::enabled()
    }

    fn get(&self, profile: &str) -> RabbitResult<Option<String>> {
        ferrika_core::kafka::secrets::get(profile, PASSWORD_FIELD).map_err(secret_error)
    }

    fn set(&self, profile: &str, password: &str) -> RabbitResult<()> {
        ferrika_core::kafka::secrets::set(profile, PASSWORD_FIELD, password).map_err(secret_error)
    }

    fn delete(&self, profile: &str) -> RabbitResult<()> {
        ferrika_core::kafka::secrets::delete(profile, PASSWORD_FIELD).map_err(secret_error)
    }
}

fn secret_error(e: ferrika_core::error::KafkaError) -> RabbitError {
    RabbitError::config(format!("OS credential store: {e}"))
}

/// The RabbitMQ profiles and which one is active.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct RabbitProfileStore {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_profile: Option<String>,
    #[serde(default)]
    pub profiles: BTreeMap<String, RabbitProfile>,
}

impl RabbitProfileStore {
    /// `rabbit-profiles.json` in the directory of `profiles.json`.
    pub fn default_path() -> PathBuf {
        path_next_to(&ferrika_core::kafka::profile::ProfileStore::default_path())
    }

    /// Loads the store from the default path, passwords from the OS
    /// credential store. A missing file is an empty store.
    pub fn load() -> RabbitResult<Self> {
        Self::load_with(&Self::default_path(), &OsSecretStore)
    }

    /// Saves the store to the default path, passwords to the OS
    /// credential store where possible.
    pub fn save(&self) -> RabbitResult<()> {
        self.save_with(&Self::default_path(), &OsSecretStore)
    }

    /// Loads `path` and fills in the passwords kept in `secrets`. A
    /// password the store cannot read is left out (with a warning): the
    /// command then fails to log in rather than not start.
    pub fn load_with(path: &Path, secrets: &impl SecretStore) -> RabbitResult<Self> {
        let mut store = Self::load_file(path)?;
        for (name, profile) in &mut store.profiles {
            if !profile.password_in_store {
                continue;
            }
            match secrets.get(name) {
                Ok(password) => profile.password = password,
                Err(e) => tracing::warn!(
                    "Cannot read the password of RabbitMQ profile '{name}' from the OS \
                     credential store: {e}"
                ),
            }
        }
        Ok(store)
    }

    /// The file as it is, without passwords from the credential store.
    pub fn load_file(path: &Path) -> RabbitResult<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let content = std::fs::read_to_string(path)
            .map_err(|e| RabbitError::config(format!("Cannot read {}: {e}", path.display())))?;
        serde_json::from_str(&content)
            .map_err(|e| RabbitError::config(format!("Cannot parse {}: {e}", path.display())))
    }

    /// Writes the store to `path`: passwords to `secrets` when it is
    /// enabled (else, or when storing fails, into the file), and removes
    /// the stored passwords of profiles that are gone.
    pub fn save_with(&self, path: &Path, secrets: &impl SecretStore) -> RabbitResult<()> {
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
                        "Cannot remove the password of RabbitMQ profile '{name}' from the OS \
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
            // No password: none was set (a new profile), or the stored one
            // could not be read this time; that entry is kept.
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
                        "Cannot store the password of RabbitMQ profile '{name}' in the OS \
                         credential store, keeping it in {FILE_NAME}: {e}"
                    );
                    profile.password_in_store = false;
                }
            }
        }
        stored
    }

    fn write_file(&self, path: &Path) -> RabbitResult<()> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() && !parent.exists() {
                create_private_dir(parent).map_err(|e| {
                    RabbitError::config(format!("Cannot create {}: {e}", parent.display()))
                })?;
            }
        }
        let json = serde_json::to_string_pretty(self)
            .map_err(|e| RabbitError::config(format!("Cannot serialize the profiles: {e}")))?;
        ferrika_core::files::write_private_file(path, json.as_bytes())
            .map_err(|e| RabbitError::config(format!("Cannot write {}: {e}", path.display())))
    }

    /// Adds or replaces `name` after checking it. The first profile becomes
    /// the active one.
    pub fn insert(&mut self, name: &str, profile: RabbitProfile) -> RabbitResult<()> {
        validate_name(name)?;
        profile.validate()?;
        self.profiles.insert(name.to_string(), profile);
        if self.active_profile.is_none() {
            self.active_profile = Some(name.to_string());
        }
        Ok(())
    }

    /// Removes `name`; the active profile is cleared when it was that one.
    pub fn remove(&mut self, name: &str) -> RabbitResult<RabbitProfile> {
        let removed = self
            .profiles
            .remove(name)
            .ok_or_else(|| unknown_profile(name))?;
        if self.active_profile.as_deref() == Some(name) {
            self.active_profile = None;
        }
        Ok(removed)
    }

    /// Makes `name` the profile `rabbit` commands use by default.
    pub fn set_active(&mut self, name: &str) -> RabbitResult<()> {
        if !self.profiles.contains_key(name) {
            return Err(unknown_profile(name));
        }
        self.active_profile = Some(name.to_string());
        Ok(())
    }

    /// The named profile.
    pub fn get(&self, name: &str) -> RabbitResult<&RabbitProfile> {
        self.profiles.get(name).ok_or_else(|| unknown_profile(name))
    }

    /// The active profile, if one is set and exists.
    pub fn active(&self) -> Option<(&str, &RabbitProfile)> {
        let name = self.active_profile.as_deref()?;
        self.profiles.get(name).map(|p| (name, p))
    }
}

/// `rabbit-profiles.json` next to `profiles`.
pub fn path_next_to(profiles: &Path) -> PathBuf {
    profiles
        .parent()
        .map_or_else(|| PathBuf::from(FILE_NAME), |dir| dir.join(FILE_NAME))
}

/// A profile name: letters, digits, `-`, `_` and `.`, at most 64.
pub fn validate_name(name: &str) -> RabbitResult<()> {
    let valid = !name.is_empty()
        && name.len() <= 64
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'));
    if valid {
        Ok(())
    } else {
        Err(RabbitError::config(format!(
            "Invalid profile name '{name}': use letters, digits, '-', '_' or '.' (at most 64)"
        )))
    }
}

fn unknown_profile(name: &str) -> RabbitError {
    RabbitError::config(format!(
        "No RabbitMQ profile '{name}' (see `ferrika rabbit profile list`)"
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
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::collections::HashMap;

    /// An in-memory credential store.
    #[derive(Default)]
    struct MemorySecrets {
        disabled: bool,
        entries: RefCell<HashMap<String, String>>,
    }

    impl SecretStore for MemorySecrets {
        fn enabled(&self) -> bool {
            !self.disabled
        }
        fn get(&self, profile: &str) -> RabbitResult<Option<String>> {
            Ok(self.entries.borrow().get(profile).cloned())
        }
        fn set(&self, profile: &str, password: &str) -> RabbitResult<()> {
            self.entries
                .borrow_mut()
                .insert(profile.to_string(), password.to_string());
            Ok(())
        }
        fn delete(&self, profile: &str) -> RabbitResult<()> {
            self.entries.borrow_mut().remove(profile);
            Ok(())
        }
    }

    fn temp_path(test: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "ferrika-rabbit-profiles-{test}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        dir.join(FILE_NAME)
    }

    fn prod() -> RabbitProfile {
        RabbitProfile {
            user: Some("ops".to_string()),
            password: Some("s3cret-pw".to_string()),
            vhost: "shop".to_string(),
            ..RabbitProfile::new("https://rabbit.prod.corp")
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
        let mut store = RabbitProfileStore::default();
        store.insert("prod", prod()).unwrap();
        assert_eq!(store.active_profile.as_deref(), Some("prod"));
        store.save_with(&path, &secrets).unwrap();

        let file = std::fs::read_to_string(&path).unwrap();
        assert!(!file.contains("s3cret-pw"), "{file}");
        assert!(file.contains("password_in_store"), "{file}");
        assert_eq!(
            secrets.entries.borrow().get("prod").map(String::as_str),
            Some("s3cret-pw")
        );

        let loaded = RabbitProfileStore::load_with(&path, &secrets).unwrap();
        assert_eq!(
            loaded.get("prod").unwrap().password.as_deref(),
            Some("s3cret-pw")
        );
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
        let mut store = RabbitProfileStore::default();
        store.insert("prod", prod()).unwrap();
        store.save_with(&path, &secrets).unwrap();
        let loaded = RabbitProfileStore::load_with(&path, &secrets).unwrap();
        let profile = loaded.get("prod").unwrap();
        assert_eq!(profile.password.as_deref(), Some("s3cret-pw"));
        assert!(!profile.password_in_store);
        assert!(secrets.entries.borrow().is_empty());
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn a_missing_file_is_an_empty_store() {
        let store = RabbitProfileStore::load_with(&temp_path("missing"), &MemorySecrets::default())
            .unwrap();
        assert!(store.profiles.is_empty());
        assert!(store.active().is_none());
    }

    #[test]
    fn profiles_are_checked_before_they_are_stored() {
        let mut store = RabbitProfileStore::default();
        let with_password_in_url = RabbitProfile::new("http://ops:pw@rabbit");
        assert!(store.insert("a", with_password_in_url).is_err());
        let amqp_with_user = RabbitProfile {
            amqp_url: Some("amqp://ops@rabbit".to_string()),
            ..RabbitProfile::new("http://rabbit")
        };
        assert!(store.insert("a", amqp_with_user).is_err());
        assert!(store
            .insert("bad name", RabbitProfile::new("http://rabbit"))
            .is_err());
        assert!(store
            .insert("a", RabbitProfile::new("ftp://rabbit"))
            .is_err());
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
    fn the_amqp_url_follows_the_management_url() {
        assert_eq!(
            derive_amqp_url("http://rabbit:15672").unwrap(),
            "amqp://rabbit:5672"
        );
        assert_eq!(
            derive_amqp_url("HTTPS://ops:pw@rabbit.prod.corp/mgmt").unwrap(),
            "amqps://rabbit.prod.corp:5671"
        );
        assert_eq!(
            derive_amqp_url("http://[::1]:15672").unwrap(),
            "amqp://[::1]:5672"
        );
        assert!(derive_amqp_url("rabbit:15672").is_err());
        assert!(derive_amqp_url("http://:15672").is_err());

        let explicit = RabbitProfile {
            amqp_url: Some("amqps://amqp.prod.corp:5671".to_string()),
            ..RabbitProfile::new("https://mgmt.prod.corp")
        };
        assert_eq!(explicit.amqp_url().unwrap(), "amqps://amqp.prod.corp:5671");
        assert_eq!(
            RabbitProfile::new("https://mgmt.prod.corp")
                .amqp_url()
                .unwrap(),
            "amqps://mgmt.prod.corp:5671"
        );
    }

    #[test]
    fn credentials_are_read_from_a_url() {
        assert_eq!(
            url_credentials("http://ops:p%40ss%3Aw@rabbit:15672/x"),
            Some(("ops".to_string(), "p@ss:w".to_string()))
        );
        assert_eq!(
            url_credentials("https://ops@rabbit"),
            Some(("ops".to_string(), String::new()))
        );
        assert_eq!(url_credentials("http://rabbit:15672/a@b"), None);
        assert_eq!(url_credentials("rabbit"), None);
        assert_eq!(ferrika_core::kafka::profile::percent_decode("100%"), "100%");
    }

    #[test]
    fn amqp_addresses_get_their_default_port() {
        assert_eq!(amqp_host_port("amqp://rabbit").unwrap(), "rabbit:5672");
        assert_eq!(amqp_host_port("amqps://rabbit/%2F").unwrap(), "rabbit:5671");
        assert_eq!(
            amqp_host_port("amqp://u:p@rabbit:6000").unwrap(),
            "rabbit:6000"
        );
        assert!(amqp_host_port("http://rabbit").is_err());
    }
}
