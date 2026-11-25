//! The connection a front end uses: an MQTT profile with the user's
//! overrides applied ([`ResolvedMqtt::resolve`]), and from it the login
//! ([`ResolvedMqtt::credentials`]) and the [`ConnectTarget`]. A profile's
//! stored password only ever goes to that profile's broker and user.
//!
//! ```
//! use ferrika_mqtt::connection::ResolvedMqtt;
//! use ferrika_mqtt::profile::MqttProfile;
//!
//! let profile = MqttProfile {
//!     user: Some("ops".to_string()),
//!     password: Some("secret".to_string()),
//!     ..MqttProfile::new("mqtt://broker")
//! };
//! let resolved = ResolvedMqtt::from_profile("dev", &profile);
//! let login = resolved.credentials(None).unwrap();
//! assert_eq!(login, Some(("ops".to_string(), "secret".to_string())));
//! assert_eq!(resolved.address().unwrap(), "broker:1883");
//! ```

use std::path::PathBuf;
use std::time::Duration;

use crate::client::ConnectTarget;
use crate::profile::{BrokerUrl, MqttProfile, MqttProfileStore, MqttVersion};

/// What the user gave besides (or instead of) a profile. Blank values
/// count as not given for the profile name and the URL.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MqttOverrides<'a> {
    /// The MQTT profile to use (else the active one, unless `url` is
    /// given).
    pub profile: Option<&'a str>,
    /// The broker URL.
    pub url: Option<&'a str>,
    pub user: Option<&'a str>,
    pub version: Option<MqttVersion>,
    pub ca_file: Option<PathBuf>,
}

/// Why [`ResolvedMqtt::resolve`] found no connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolveError {
    /// No URL was given and there is no profile to take one from.
    NoConnection,
    /// The profiles could not be loaded, or the named one does not exist.
    Profile(String),
}

impl std::fmt::Display for ResolveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoConnection => {
                f.write_str("No MQTT connection: give a broker URL, or add an MQTT profile")
            }
            Self::Profile(reason) => f.write_str(reason),
        }
    }
}

impl std::error::Error for ResolveError {}

/// Why [`ResolvedMqtt::credentials`] cannot log in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CredentialsError {
    /// A user without a password.
    MissingPassword,
    /// A password without a user.
    PasswordWithoutUser,
}

impl std::fmt::Display for CredentialsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::MissingPassword => "The MQTT user needs a password",
            Self::PasswordWithoutUser => "An MQTT password was given without a user",
        })
    }
}

impl std::error::Error for CredentialsError {}

/// The connection a command uses: overrides over the MQTT profile.
#[derive(Clone, PartialEq, Eq)]
pub struct ResolvedMqtt {
    /// The MQTT profile used, if any: what the policy's `read_only` globs
    /// match.
    pub profile: Option<String>,
    /// The profile is marked read-only.
    pub read_only: bool,
    pub url: String,
    pub version: MqttVersion,
    pub user: Option<String>,
    /// The profile's password, only when the user is the profile's own;
    /// a password given by the user wins.
    profile_password: Option<String>,
    pub ca_file: Option<PathBuf>,
}

// Manual so the password never reaches debug output.
impl std::fmt::Debug for ResolvedMqtt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResolvedMqtt")
            .field("profile", &self.profile)
            .field("read_only", &self.read_only)
            .field("url", &self.url)
            .field("version", &self.version)
            .field("user", &self.user)
            .field(
                "profile_password",
                &self.profile_password.as_ref().map(|_| "<redacted>"),
            )
            .field("ca_file", &self.ca_file)
            .finish()
    }
}

/// `text` trimmed, `None` when blank.
fn given(text: Option<&str>) -> Option<&str> {
    text.map(str::trim).filter(|t| !t.is_empty())
}

impl ResolvedMqtt {
    /// The connection `overrides` name: the profile they name, else the
    /// active profile unless a URL is given (a URL without a profile means
    /// no profile: a profile's password is never sent to a broker the user
    /// named by hand); each override replaces the profile's value.
    /// `load_store` runs only when a profile can apply.
    pub fn resolve(
        overrides: &MqttOverrides<'_>,
        load_store: impl FnOnce() -> Result<MqttProfileStore, String>,
    ) -> Result<Self, ResolveError> {
        let profile_name = given(overrides.profile);
        let url = given(overrides.url);
        let wants_profile = profile_name.is_some() || url.is_none();
        let chosen: Option<(String, MqttProfile)> = if wants_profile {
            let store = load_store().map_err(ResolveError::Profile)?;
            match profile_name {
                Some(name) => Some((
                    name.to_string(),
                    store
                        .get(name)
                        .map_err(|e| ResolveError::Profile(e.to_string()))?
                        .clone(),
                )),
                None => store.active().map(|(n, p)| (n.to_string(), p.clone())),
            }
        } else {
            None
        };
        let profile = chosen.as_ref().map(|(_, p)| p);
        let url = url
            .map(str::to_string)
            .or_else(|| profile.map(|p| p.url.clone()))
            .ok_or(ResolveError::NoConnection)?;
        let user = overrides
            .user
            .map(str::to_string)
            .or_else(|| profile.and_then(|p| p.user.clone()));
        // The stored password belongs to the profile's user only.
        let profile_password = profile
            .filter(|p| overrides.user.is_none() || overrides.user == p.user.as_deref())
            .and_then(|p| p.password.clone());
        Ok(Self {
            profile: chosen.as_ref().map(|(name, _)| name.clone()),
            read_only: profile.is_some_and(|p| p.read_only),
            url,
            version: overrides
                .version
                .or_else(|| profile.map(|p| p.version))
                .unwrap_or_default(),
            user,
            profile_password,
            ca_file: overrides
                .ca_file
                .clone()
                .or_else(|| profile.and_then(|p| p.ca_file.clone())),
        })
    }

    /// The connection of profile `name` with no overrides.
    pub fn from_profile(name: &str, profile: &MqttProfile) -> Self {
        Self {
            profile: Some(name.to_string()),
            read_only: profile.read_only,
            url: profile.url.clone(),
            version: profile.version,
            user: profile.user.clone(),
            profile_password: profile.password.clone(),
            ca_file: profile.ca_file.clone(),
        }
    }

    /// `host:port` of the broker, which the policy's broker-address rules
    /// and the audit log see.
    pub fn address(&self) -> Result<String, String> {
        BrokerUrl::parse(&self.url)
            .map(|u| u.address())
            .map_err(|e| e.to_string())
    }

    /// The user and password to log in with, or `None` for an anonymous
    /// login. `given_password` is a password the user gave now; it wins
    /// over the profile's.
    pub fn credentials(
        &self,
        given_password: Option<String>,
    ) -> Result<Option<(String, String)>, CredentialsError> {
        let password = given_password.or_else(|| self.profile_password.clone());
        match (&self.user, password) {
            (Some(user), Some(password)) => Ok(Some((user.clone(), password))),
            (Some(_), None) => Err(CredentialsError::MissingPassword),
            (None, Some(_)) => Err(CredentialsError::PasswordWithoutUser),
            (None, None) => Ok(None),
        }
    }

    /// What [`crate::client::Session::connect`] needs, with the login from
    /// [`Self::credentials`] and a time limit for each step.
    pub fn target(
        &self,
        credentials: Option<(String, String)>,
        timeout: Duration,
    ) -> Result<ConnectTarget, String> {
        let url = BrokerUrl::parse(&self.url).map_err(|e| e.to_string())?;
        if self.ca_file.is_some() && !url.tls {
            return Err("A CA file needs an mqtts:// URL (TLS)".to_string());
        }
        Ok(ConnectTarget {
            url,
            version: self.version,
            credentials,
            ca_file: self.ca_file.clone(),
            client_id: crate::client::client_id(),
            timeout,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> MqttProfileStore {
        let mut store = MqttProfileStore::default();
        store
            .insert(
                "prod",
                MqttProfile {
                    user: Some("ops".to_string()),
                    password: Some("stored-pw".to_string()),
                    read_only: true,
                    version: MqttVersion::V5,
                    ca_file: Some(PathBuf::from("ca.pem")),
                    ..MqttProfile::new("mqtts://broker.prod.corp")
                },
            )
            .unwrap();
        store
            .insert("dev", MqttProfile::new("mqtt://localhost"))
            .unwrap();
        store
    }

    #[test]
    fn the_active_profile_is_used_when_nothing_is_named() {
        let resolved = ResolvedMqtt::resolve(&MqttOverrides::default(), || {
            let mut store = store();
            store.set_active("prod").unwrap();
            Ok(store)
        })
        .unwrap();
        assert_eq!(resolved.profile.as_deref(), Some("prod"));
        assert!(resolved.read_only);
        assert_eq!(resolved.version, MqttVersion::V5);
        assert_eq!(resolved.address().unwrap(), "broker.prod.corp:8883");
        assert_eq!(
            resolved.credentials(None).unwrap(),
            Some(("ops".to_string(), "stored-pw".to_string()))
        );
    }

    #[test]
    fn options_override_the_named_profile_and_another_user_gets_no_password() {
        let overrides = MqttOverrides {
            profile: Some("prod"),
            version: Some(MqttVersion::V311),
            ..MqttOverrides::default()
        };
        let resolved = ResolvedMqtt::resolve(&overrides, || Ok(store())).unwrap();
        assert_eq!(resolved.version, MqttVersion::V311);
        assert_eq!(resolved.ca_file, Some(PathBuf::from("ca.pem")));

        let other_user = MqttOverrides {
            user: Some("someone"),
            ..overrides.clone()
        };
        let resolved = ResolvedMqtt::resolve(&other_user, || Ok(store())).unwrap();
        assert_eq!(
            resolved.credentials(None),
            Err(CredentialsError::MissingPassword)
        );
        assert_eq!(
            resolved.credentials(Some("given".to_string())).unwrap(),
            Some(("someone".to_string(), "given".to_string()))
        );

        let unknown = MqttOverrides {
            profile: Some("nope"),
            ..MqttOverrides::default()
        };
        assert!(matches!(
            ResolvedMqtt::resolve(&unknown, || Ok(store())),
            Err(ResolveError::Profile(_))
        ));
    }

    #[test]
    fn a_url_without_a_profile_option_uses_no_profile() {
        let overrides = MqttOverrides {
            url: Some("mqtt://broker.dev:1884"),
            ..MqttOverrides::default()
        };
        let resolved =
            ResolvedMqtt::resolve(&overrides, || panic!("the store must not be read")).unwrap();
        assert_eq!(resolved.profile, None);
        assert!(!resolved.read_only);
        assert_eq!(resolved.version, MqttVersion::V311);
        assert_eq!(resolved.address().unwrap(), "broker.dev:1884");
        assert_eq!(resolved.credentials(None).unwrap(), None);
        assert_eq!(
            resolved.credentials(Some("pw".to_string())),
            Err(CredentialsError::PasswordWithoutUser)
        );
    }

    #[test]
    fn no_url_and_no_active_profile_is_an_error() {
        assert_eq!(
            ResolvedMqtt::resolve(
                &MqttOverrides::default(),
                || Ok(MqttProfileStore::default())
            ),
            Err(ResolveError::NoConnection)
        );
    }

    #[test]
    fn targets_check_tls_and_get_a_fresh_client_id() {
        let mut resolved = ResolvedMqtt::resolve(
            &MqttOverrides {
                profile: Some("prod"),
                ..MqttOverrides::default()
            },
            || Ok(store()),
        )
        .unwrap();
        let target = resolved.target(None, Duration::from_secs(5)).unwrap();
        assert!(target.url.tls);
        assert!(target
            .client_id
            .starts_with(crate::client::CLIENT_ID_PREFIX));
        resolved.url = "mqtt://broker.prod.corp".to_string();
        assert!(resolved.target(None, Duration::from_secs(5)).is_err());
    }

    #[test]
    fn debug_output_hides_the_password() {
        let resolved = ResolvedMqtt::resolve(
            &MqttOverrides {
                profile: Some("prod"),
                ..MqttOverrides::default()
            },
            || Ok(store()),
        )
        .unwrap();
        let debug = format!("{resolved:?}");
        assert!(!debug.contains("stored-pw"), "{debug}");
    }
}
