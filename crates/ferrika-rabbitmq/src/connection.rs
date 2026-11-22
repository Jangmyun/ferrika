//! The connection a front end uses: a RabbitMQ profile with the user's
//! overrides applied ([`ResolvedRabbit::resolve`]), and from it the login
//! ([`ResolvedRabbit::credentials`]), the management API client and the
//! AMQP target. The CLI's `ferrika rabbit` and other front ends resolve
//! connections the same way, so a profile's stored password only ever
//! goes to that profile's broker and user.
//!
//! ```
//! use ferrika_rabbitmq::connection::{RabbitOverrides, ResolvedRabbit};
//! use ferrika_rabbitmq::profile::RabbitProfile;
//!
//! let profile = RabbitProfile {
//!     user: Some("ops".to_string()),
//!     password: Some("secret".to_string()),
//!     ..RabbitProfile::new("http://rabbit:15672")
//! };
//! let resolved = ResolvedRabbit::from_profile("dev", &profile);
//! let login = resolved.credentials(None).unwrap();
//! assert_eq!(login, Some(("ops".to_string(), "secret".to_string())));
//! assert_eq!(resolved.address().unwrap(), "rabbit:15672");
//! # let _ = RabbitOverrides::default();
//! ```

use std::time::Duration;

use crate::client::ManagementClient;
use crate::profile::{url_credentials, RabbitProfile, RabbitProfileStore};

#[cfg(feature = "amqp")]
use crate::amqp::AmqpTarget;

/// What the user gave besides (or instead of) a profile. Blank values
/// count as not given for the profile name and the URLs.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RabbitOverrides<'a> {
    /// The RabbitMQ profile to use (else the active one, unless `url` is
    /// given).
    pub profile: Option<&'a str>,
    /// The management API URL.
    pub url: Option<&'a str>,
    pub vhost: Option<&'a str>,
    pub user: Option<&'a str>,
    /// The AMQP URL for moving messages.
    pub amqp_url: Option<&'a str>,
}

/// Why [`ResolvedRabbit::resolve`] found no connection.
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
            Self::NoConnection => f.write_str(
                "No RabbitMQ connection: give a management URL, or add a RabbitMQ profile",
            ),
            Self::Profile(reason) => f.write_str(reason),
        }
    }
}

impl std::error::Error for ResolveError {}

/// Why [`ResolvedRabbit::credentials`] cannot log in.
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
            Self::MissingPassword => "The RabbitMQ user needs a password",
            Self::PasswordWithoutUser => "A RabbitMQ password was given without a user",
        })
    }
}

impl std::error::Error for CredentialsError {}

/// The connection a command uses: overrides over the RabbitMQ profile.
#[derive(Clone, PartialEq, Eq)]
pub struct ResolvedRabbit {
    /// The RabbitMQ profile used, if any: what the policy's `read_only`
    /// globs match.
    pub profile: Option<String>,
    /// The profile is marked read-only.
    pub read_only: bool,
    pub management_url: String,
    pub vhost: String,
    pub user: Option<String>,
    /// The profile's password (from the OS credential store), only when
    /// the user is the profile's own; a password given by the user wins.
    profile_password: Option<String>,
    pub amqp_url: Option<String>,
}

// Manual so the password never reaches debug output.
impl std::fmt::Debug for ResolvedRabbit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResolvedRabbit")
            .field("profile", &self.profile)
            .field("read_only", &self.read_only)
            .field(
                "management_url",
                &ferrika_core::model::format::redact_url(&self.management_url),
            )
            .field("vhost", &self.vhost)
            .field("user", &self.user)
            .field(
                "profile_password",
                &self.profile_password.as_ref().map(|_| "<redacted>"),
            )
            .field("amqp_url", &self.amqp_url)
            .finish()
    }
}

/// `text` trimmed, `None` when blank.
fn given(text: Option<&str>) -> Option<&str> {
    text.map(str::trim).filter(|t| !t.is_empty())
}

impl ResolvedRabbit {
    /// The connection `overrides` name: the profile they name, else the
    /// active profile unless a URL is given (a URL without a profile means
    /// no profile: a profile's password is never sent to a broker the user
    /// named by hand); each override replaces the profile's value.
    /// `load_store` runs only when a profile can apply.
    pub fn resolve(
        overrides: &RabbitOverrides<'_>,
        load_store: impl FnOnce() -> Result<RabbitProfileStore, String>,
    ) -> Result<Self, ResolveError> {
        let profile_name = given(overrides.profile);
        let url = given(overrides.url);
        let wants_profile = profile_name.is_some() || url.is_none();
        let chosen: Option<(String, RabbitProfile)> = if wants_profile {
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
        let management_url = url
            .map(str::to_string)
            .or_else(|| profile.map(|p| p.management_url.clone()))
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
            management_url,
            vhost: overrides
                .vhost
                .map(str::to_string)
                .or_else(|| profile.map(|p| p.vhost.clone()))
                .unwrap_or_else(|| "/".to_string()),
            user,
            profile_password,
            amqp_url: given(overrides.amqp_url)
                .map(str::to_string)
                .or_else(|| profile.and_then(|p| p.amqp_url.clone())),
        })
    }

    /// The connection of profile `name` with no overrides.
    pub fn from_profile(name: &str, profile: &RabbitProfile) -> Self {
        Self {
            profile: Some(name.to_string()),
            read_only: profile.read_only,
            management_url: profile.management_url.clone(),
            vhost: profile.vhost.clone(),
            user: profile.user.clone(),
            profile_password: profile.password.clone(),
            amqp_url: profile.amqp_url.clone(),
        }
    }

    /// `host:port` of the management API (no credentials), which the
    /// policy's broker-address rules and the audit log see.
    pub fn address(&self) -> Result<String, String> {
        ManagementClient::new(&self.management_url)
            .map(|client| client.address())
            .map_err(|e| e.to_string())
    }

    /// The AMQP URL: the override, the profile's, or the management host
    /// on the AMQP port.
    pub fn amqp_url(&self) -> Result<String, String> {
        match &self.amqp_url {
            Some(url) => Ok(url.clone()),
            None => {
                crate::profile::derive_amqp_url(&self.management_url).map_err(|e| e.to_string())
            }
        }
    }

    /// The addresses the policy's broker-address rules match, as a
    /// comma-separated list: the management API's `host:port` and the AMQP
    /// listener's, so a rule naming either covers every command on this
    /// connection.
    pub fn gate_addresses(&self) -> Result<String, String> {
        let management = self.address()?;
        let amqp = crate::profile::amqp_host_port(&self.amqp_url()?).map_err(|e| e.to_string())?;
        Ok(if amqp == management {
            management
        } else {
            format!("{management},{amqp}")
        })
    }

    /// Whether the management URL carries a user or password (worth a
    /// warning: such URLs end up in shell history and logs).
    pub fn url_has_credentials(&self) -> bool {
        url_credentials(&self.management_url).is_some()
    }

    /// The user and password to log in with, or `None` for credentials in
    /// the URL (or none). `given_password` is a password the user gave
    /// now; it wins over the profile's.
    pub fn credentials(
        &self,
        given_password: Option<String>,
    ) -> Result<Option<(String, String)>, CredentialsError> {
        let password = given_password.or_else(|| self.profile_password.clone());
        match (&self.user, password) {
            (Some(user), Some(password)) => Ok(Some((user.clone(), password))),
            (Some(_), None) => Err(CredentialsError::MissingPassword),
            (None, Some(_)) => Err(CredentialsError::PasswordWithoutUser),
            // Credentials in the URL, or none.
            (None, None) => Ok(None),
        }
    }

    /// The management API client, logged in as `credentials` (else as the
    /// URL's user info, if any).
    pub fn client(
        &self,
        credentials: Option<&(String, String)>,
        timeout: Duration,
    ) -> Result<ManagementClient, String> {
        let client = ManagementClient::new(&self.management_url)
            .map_err(|e| e.to_string())?
            .with_timeout(timeout);
        Ok(match credentials {
            Some((user, password)) => client.with_credentials(user, password),
            None => client,
        })
    }

    /// Where to connect over AMQP (see [`Self::amqp_url`]), logged in as
    /// `credentials`, else as the user and password in the management URL
    /// (the same RabbitMQ user serves both).
    #[cfg(feature = "amqp")]
    pub fn amqp_target(
        &self,
        credentials: Option<&(String, String)>,
        timeout: Duration,
    ) -> Result<AmqpTarget, String> {
        let login = credentials
            .cloned()
            .or_else(|| url_credentials(&self.management_url));
        Ok(AmqpTarget {
            url: self.amqp_url()?,
            vhost: self.vhost.clone(),
            user: login.as_ref().map(|(user, _)| user.clone()),
            password: login.map(|(_, password)| password),
            timeout,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TIMEOUT: Duration = Duration::from_secs(30);

    fn store() -> RabbitProfileStore {
        let mut store = RabbitProfileStore::default();
        store
            .insert(
                "prod",
                RabbitProfile {
                    vhost: "shop".to_string(),
                    user: Some("ops".to_string()),
                    password: Some("stored-pw".to_string()),
                    read_only: true,
                    ..RabbitProfile::new("https://rabbit.prod.corp")
                },
            )
            .unwrap();
        store
            .insert("dev", RabbitProfile::new("http://localhost:15672"))
            .unwrap();
        store
    }

    #[test]
    fn the_active_profile_is_used_when_nothing_is_named() {
        let resolved = ResolvedRabbit::resolve(&RabbitOverrides::default(), || {
            let mut store = store();
            store.set_active("prod").unwrap();
            Ok(store)
        })
        .unwrap();
        assert_eq!(resolved.profile.as_deref(), Some("prod"));
        assert!(resolved.read_only);
        assert_eq!(resolved.vhost, "shop");
        assert_eq!(resolved.address().unwrap(), "rabbit.prod.corp:443");
        assert_eq!(
            resolved.credentials(None).unwrap(),
            Some(("ops".to_string(), "stored-pw".to_string()))
        );
        let named = ResolvedRabbit::from_profile("prod", store().get("prod").unwrap());
        assert_eq!(named, resolved);
    }

    #[test]
    fn another_user_does_not_get_the_profiles_password() {
        let overrides = RabbitOverrides {
            profile: Some("prod"),
            user: Some("someone"),
            ..RabbitOverrides::default()
        };
        let resolved = ResolvedRabbit::resolve(&overrides, || Ok(store())).unwrap();
        assert_eq!(
            resolved.credentials(None),
            Err(CredentialsError::MissingPassword)
        );
        assert_eq!(
            resolved.credentials(Some("pw".to_string())).unwrap(),
            Some(("someone".to_string(), "pw".to_string()))
        );
        let unknown = RabbitOverrides {
            profile: Some("nope"),
            ..RabbitOverrides::default()
        };
        assert!(matches!(
            ResolvedRabbit::resolve(&unknown, || Ok(store())),
            Err(ResolveError::Profile(_))
        ));
    }

    #[test]
    fn a_url_without_a_profile_uses_no_profile() {
        let overrides = RabbitOverrides {
            url: Some("http://admin:pw@rabbit.dev"),
            ..RabbitOverrides::default()
        };
        let resolved =
            ResolvedRabbit::resolve(&overrides, || panic!("the store must not be read")).unwrap();
        assert_eq!(resolved.profile, None);
        assert!(resolved.url_has_credentials());
        assert_eq!(resolved.credentials(None).unwrap(), None);
        assert_eq!(
            resolved.credentials(Some("x".to_string())),
            Err(CredentialsError::PasswordWithoutUser)
        );
        assert_eq!(
            resolved.gate_addresses().unwrap(),
            "rabbit.dev:15672,rabbit.dev:5672"
        );
        assert!(resolved.client(None, TIMEOUT).is_ok());
        assert_eq!(
            ResolvedRabbit::resolve(&RabbitOverrides::default(), || Ok(
                RabbitProfileStore::default()
            )),
            Err(ResolveError::NoConnection)
        );
    }

    #[cfg(feature = "amqp")]
    #[test]
    fn the_amqp_target_logs_in_like_the_management_api() {
        let overrides = RabbitOverrides {
            url: Some("http://admin:pw@rabbit.dev"),
            ..RabbitOverrides::default()
        };
        let resolved = ResolvedRabbit::resolve(&overrides, || unreachable!()).unwrap();
        let target = resolved.amqp_target(None, TIMEOUT).unwrap();
        assert_eq!(target.url, "amqp://rabbit.dev:5672");
        assert_eq!(target.user.as_deref(), Some("admin"));
        assert_eq!(target.password.as_deref(), Some("pw"));
        let given = Some(("ops".to_string(), "x".to_string()));
        let target = resolved.amqp_target(given.as_ref(), TIMEOUT).unwrap();
        assert_eq!(target.user.as_deref(), Some("ops"));
    }

    #[test]
    fn debug_output_hides_the_password() {
        let resolved = ResolvedRabbit::from_profile("prod", store().get("prod").unwrap());
        let debug = format!("{resolved:?}");
        assert!(!debug.contains("stored-pw"), "{debug}");
    }
}
