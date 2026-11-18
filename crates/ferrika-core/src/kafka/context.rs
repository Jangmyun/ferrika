//! The cluster a command talks to: a profile from the store with the
//! caller's overrides, resolved once.
//!
//! Loading `profiles.json` per call (and treating a broken file or an
//! unknown profile name as "no profile") let an empty name slip past policy
//! `read_only` globs such as `prod-*`. [`ResolvedContext::from_store`] fails
//! instead; [`ResolvedContext::lenient`] is for callers that cannot.

use std::path::Path;
use std::time::Duration;

use crate::error::{KafkaError, KafkaResult};
use crate::kafka::config::KafkaClientConfig;
use crate::kafka::profile::{ClusterProfile, ProfileStore};

/// Brokers used when no profile exists at all.
pub const FALLBACK_BOOTSTRAP: &str = "localhost:9092";

/// What the caller asked for: a profile name, a broker list replacing the
/// profile's, and the request timeout.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterSelection {
    /// The profile to use; `None` means the store's active one.
    pub profile: Option<String>,
    /// Brokers replacing the profile's bootstrap servers.
    pub bootstrap_servers: Option<String>,
    /// Request timeout of the clients built from the config.
    pub timeout: Duration,
}

/// The profile and brokers one run uses.
#[derive(Debug, Clone)]
pub struct ResolvedContext {
    /// The profile the command uses: the selected one, else the active one.
    /// `None` only when the store has no profiles.
    pub profile_name: Option<String>,
    /// That profile as stored.
    pub profile: Option<ClusterProfile>,
    /// The selected bootstrap servers, else the profile's.
    pub bootstrap_servers: String,
    /// The client config with the selection's overrides applied.
    pub config: KafkaClientConfig,
    /// The whole store, for commands that name other profiles.
    pub store: ProfileStore,
}

impl ResolvedContext {
    /// Loads `profiles.json` from its default path. A file that cannot be
    /// read or parsed, or a profile it does not have, is an error.
    pub fn resolve(selection: &ClusterSelection) -> KafkaResult<Self> {
        Self::from_store(selection, ProfileStore::load()?)
    }

    /// [`Self::resolve`] with the store at `path` (no OS credential store).
    pub fn resolve_from_path(selection: &ClusterSelection, path: &Path) -> KafkaResult<Self> {
        Self::from_store(selection, ProfileStore::load_from_path(path)?)
    }

    /// Resolves `selection` against an already loaded store.
    pub fn from_store(selection: &ClusterSelection, store: ProfileStore) -> KafkaResult<Self> {
        let profile = match &selection.profile {
            Some(name) => Some(store.get(name).cloned().ok_or_else(|| {
                KafkaError::profile(format!(
                    "Profile '{name}' does not exist (see `ferrika profile list`)"
                ))
            })?),
            None => store.get_active().cloned(),
        };
        Ok(Self::build(selection, store, profile))
    }

    /// Never fails: an unreadable store counts as the default one and an
    /// unknown profile as none (its name is still reported). Only for
    /// callers that resolve nothing up front (the GUI launcher, tests).
    pub fn lenient(selection: &ClusterSelection) -> Self {
        let store = ProfileStore::load_or_default();
        let profile = match &selection.profile {
            Some(name) => store.get(name).cloned(),
            None => store.get_active().cloned(),
        };
        let mut context = Self::build(selection, store, profile);
        if context.profile.is_none() {
            context.profile_name = selection.profile.clone();
        }
        context
    }

    /// Fails when the profile is read-only, naming `action`.
    pub fn ensure_writable(&self, action: &str) -> KafkaResult<()> {
        if !self.config.read_only {
            return Ok(());
        }
        let profile = self
            .profile_name
            .clone()
            .unwrap_or_else(|| "the active profile".to_string());
        Err(KafkaError::config(format!(
            "Profile '{profile}' is read-only, so `{action}` is refused. \
             Turn it off with `ferrika profile read-only {profile} --off`."
        )))
    }

    fn build(
        selection: &ClusterSelection,
        store: ProfileStore,
        profile: Option<ClusterProfile>,
    ) -> Self {
        let mut config = profile
            .as_ref()
            .map(ClusterProfile::to_kafka_config)
            .unwrap_or_else(|| KafkaClientConfig::new(FALLBACK_BOOTSTRAP));
        if let Some(bs) = &selection.bootstrap_servers {
            config.bootstrap_servers = bs.clone();
        }
        config.request_timeout = selection.timeout;
        Self {
            profile_name: profile.as_ref().map(|p| p.name.clone()),
            bootstrap_servers: config.bootstrap_servers.clone(),
            profile,
            config,
            store,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn selection(profile: Option<&str>, bootstrap: Option<&str>) -> ClusterSelection {
        ClusterSelection {
            profile: profile.map(str::to_string),
            bootstrap_servers: bootstrap.map(str::to_string),
            timeout: Duration::from_secs(7),
        }
    }

    fn store() -> ProfileStore {
        let mut store = ProfileStore::default();
        store.add(ClusterProfile::new("prod-eu", "kafka.prod:9092"), true);
        store
    }

    #[test]
    fn unknown_profile_is_an_error() {
        let err = ResolvedContext::from_store(&selection(Some("prod-us"), None), store())
            .expect_err("an unknown profile must not resolve");
        assert!(
            err.to_string().contains("'prod-us' does not exist"),
            "{err}"
        );
    }

    #[test]
    fn active_profile_and_overrides_apply() {
        let ctx = ResolvedContext::from_store(&selection(None, None), store()).unwrap();
        assert_eq!(ctx.profile_name.as_deref(), Some("prod-eu"));
        assert_eq!(ctx.bootstrap_servers, "kafka.prod:9092");
        assert_eq!(ctx.config.request_timeout, Duration::from_secs(7));
        // A read_only glob on `prod-*` still matches with other brokers.
        let ctx =
            ResolvedContext::from_store(&selection(Some("prod-eu"), Some("other:9092")), store())
                .unwrap();
        assert_eq!(ctx.profile_name.as_deref(), Some("prod-eu"));
        assert_eq!(ctx.config.bootstrap_servers, "other:9092");
    }
}
