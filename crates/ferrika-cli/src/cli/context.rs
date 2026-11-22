//! The cluster a CLI command talks to, resolved once in `main` (see
//! [`crate::kafka::context`]). A broken `profiles.json` gets a repair hint.

use crate::cli::GlobalOpts;
use crate::error::{KafkaError, KafkaResult};
use crate::kafka::context::ResolvedContext;
use crate::kafka::profile::ProfileStore;

/// Loads `profiles.json` from its default path and resolves `opts` (see
/// [`ResolvedContext::from_store`]).
pub fn resolve(opts: &GlobalOpts) -> KafkaResult<ResolvedContext> {
    let store = ProfileStore::load().map_err(with_repair_hint)?;
    ResolvedContext::from_store(&opts.selection(), store)
}

/// [`resolve`] with the store at `path` (no OS credential store).
#[cfg(test)]
fn resolve_from_path(opts: &GlobalOpts, path: &std::path::Path) -> KafkaResult<ResolvedContext> {
    let store = ProfileStore::load_from_path(path).map_err(with_repair_hint)?;
    ResolvedContext::from_store(&opts.selection(), store)
}

/// Tells the user how to get past a broken `profiles.json`.
pub(crate) fn with_repair_hint(err: KafkaError) -> KafkaError {
    let detail = match err {
        KafkaError::Profile(message) => message,
        other => other.to_string(),
    };
    KafkaError::profile(format!(
        "{detail}. Fix the file, or move it away to start over with the default \
         `local` profile (`ferrika profile` commands recreate it)."
    ))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::kafka::profile::ClusterProfile;

    fn opts(args: &[&str]) -> GlobalOpts {
        use clap::Parser;
        #[derive(Parser)]
        struct Wrap {
            #[command(flatten)]
            g: GlobalOpts,
        }
        Wrap::parse_from(std::iter::once("ferrika").chain(args.iter().copied())).g
    }

    fn temp_store(name: &str, content: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "ferrika-context-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or_default()
        ));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        let path = dir.join("profiles.json");
        std::fs::write(&path, content).expect("write profiles.json");
        path
    }

    fn valid_store() -> String {
        let mut store = ProfileStore::default();
        store.add(ClusterProfile::new("prod-eu", "kafka.prod:9092"), true);
        serde_json::to_string(&store).expect("serialize store")
    }

    #[test]
    fn broken_profiles_file_is_an_error() {
        let path = temp_store("broken", "{ not json");
        let err = resolve_from_path(&opts(&[]), &path).expect_err("a broken file must not resolve");
        let message = err.to_string();
        assert!(message.contains("profiles JSON"), "{message}");
        assert!(message.contains("move it away"), "{message}");
        // Even an explicit -b does not bypass it.
        assert!(resolve_from_path(&opts(&["-b", "h:9092"]), &path).is_err());
    }

    #[test]
    fn unknown_profile_is_an_error() {
        let path = temp_store("missing", &valid_store());
        let err = resolve_from_path(&opts(&["-p", "prod-us"]), &path)
            .expect_err("an unknown --profile must not resolve");
        assert!(
            err.to_string().contains("'prod-us' does not exist"),
            "{err}"
        );
    }

    #[test]
    fn missing_file_is_the_default_store() {
        let path = std::env::temp_dir().join("ferrika-context-absent/none/profiles.json");
        let ctx = resolve_from_path(&opts(&[]), &path).expect("no file means the default store");
        assert_eq!(ctx.profile_name.as_deref(), Some("local"));
        assert_eq!(ctx.bootstrap_servers, "localhost:9092");
    }

    #[test]
    fn valid_store_resolves_once_and_answers_without_reloading() {
        let path = temp_store("valid", &valid_store());
        let mut global = opts(&["--timeout", "7"]);
        let ctx = resolve_from_path(&global, &path).expect("valid store resolves");
        assert_eq!(ctx.profile_name.as_deref(), Some("prod-eu"));
        assert_eq!(ctx.bootstrap_servers, "kafka.prod:9092");
        global.resolved = Some(std::sync::Arc::new(ctx));

        // The file is gone; everything still comes from the one load.
        std::fs::remove_file(&path).expect("remove profiles.json");
        assert_eq!(global.resolved_profile_name().as_deref(), Some("prod-eu"));
        assert_eq!(global.resolved_bootstrap_servers(), "kafka.prod:9092");
        let config = global.to_kafka_config();
        assert_eq!(config.bootstrap_servers, "kafka.prod:9092");
        assert_eq!(config.request_timeout, Duration::from_secs(7));
        assert!(global.profile_store().get("prod-eu").is_some());
    }

    #[test]
    fn bootstrap_override_keeps_the_profile_name() {
        let path = temp_store("override", &valid_store());
        let ctx = resolve_from_path(&opts(&["-p", "prod-eu", "-b", "other:9092"]), &path)
            .expect("resolves");
        // A read_only glob on `prod-*` still matches.
        assert_eq!(ctx.profile_name.as_deref(), Some("prod-eu"));
        assert_eq!(ctx.bootstrap_servers, "other:9092");
    }
}
