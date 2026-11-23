//! OS credential store for profile secrets.
//!
//! Profile passwords go to Windows Credential Manager, the macOS Keychain or
//! the Linux Secret Service instead of `profiles.json`. Entries use the
//! service `ferrika` and an account named after the profile, the field and
//! the config directory (the directory of `profiles.json`, see
//! [`ProfileStore::default_path`](crate::kafka::profile::ProfileStore::default_path)):
//!
//! - the default directory (`~/.ferrika`): `<profile>/<field>`, as before
//!   accounts named the directory;
//! - any other directory (`FERRIKA_CONFIG_DIR`, `FERRIKA_CONFIG_PATH`):
//!   `<profile>/<field>@<dir hash>`, where `<dir hash>` is the first 16
//!   hex digits (8 bytes) of the SHA-256 of the directory's absolute path
//!   (canonicalized as far as it exists, UTF-8, no trailing separator; on
//!   Windows without the `\\?\` prefix, with `\` separators and in lower
//!   case). Two directories with a profile of the same name so never share
//!   an entry.
//!
//! Reading in another directory falls back to the `<profile>/<field>`
//! account when the directory's own entry is missing (secrets written
//! before accounts were per directory) and copies the value to the
//! directory's own account, so the fallback happens once. The old entry
//! stays: it may belong to the default directory's profile of that name.
//!
//! `FERRIKA_SECRET_STORE=file` keeps new secrets in the file (headless Linux
//! without a Secret Service, CI). Secrets already in the OS store are still
//! read.

use std::path::{Path, PathBuf};

use crate::error::KafkaResult;

/// Whether new secrets should be written to the OS store.
pub fn enabled() -> bool {
    cfg!(any(windows, target_os = "macos", target_os = "linux"))
        && !std::env::var("FERRIKA_SECRET_STORE").is_ok_and(|v| v.eq_ignore_ascii_case("file"))
}

/// Which config directory's entries an account belongs to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Namespace {
    /// The default config directory: accounts without a suffix.
    Default,
    /// Another config directory: accounts end in `@<hash>`.
    Dir(String),
}

impl Namespace {
    /// The namespace of the current config directory.
    pub fn current() -> Self {
        let profiles = crate::kafka::profile::ProfileStore::default_path();
        let dir = match profiles.parent() {
            Some(dir) if !dir.as_os_str().is_empty() => dir.to_path_buf(),
            _ => PathBuf::from("."),
        };
        Self::of(
            &dir,
            &crate::kafka::profile::ProfileStore::home_config_dir(),
        )
    }

    /// The namespace of `config_dir` when `default_dir` is the default.
    pub fn of(config_dir: &Path, default_dir: &Path) -> Self {
        let dir = normalized(config_dir);
        if dir == normalized(default_dir) {
            Self::Default
        } else {
            Self::Dir(dir_hash(&dir))
        }
    }

    /// The account of `field` of `profile` in this namespace.
    pub fn account(&self, profile: &str, field: &str) -> String {
        let base = account(profile, field);
        match self {
            Self::Default => base,
            Self::Dir(hash) => format!("{base}@{hash}"),
        }
    }
}

/// The path a directory hash is made from: absolute, with the part that
/// exists canonicalized (so a directory created later keeps its hash).
fn normalized(dir: &Path) -> String {
    let absolute = std::path::absolute(dir).unwrap_or_else(|_| dir.to_path_buf());
    let mut existing = absolute.as_path();
    let mut missing = Vec::new();
    let resolved = loop {
        if let Ok(mut path) = existing.canonicalize() {
            for name in missing.iter().rev() {
                path.push(name);
            }
            break path;
        }
        match (existing.parent(), existing.file_name()) {
            (Some(parent), Some(name)) => {
                missing.push(name.to_os_string());
                existing = parent;
            }
            _ => break absolute.clone(),
        }
    };
    let text = resolved.to_string_lossy().into_owned();
    if cfg!(windows) {
        let text = if let Some(rest) = text.strip_prefix(r"\\?\UNC\") {
            format!(r"\\{rest}")
        } else {
            text.strip_prefix(r"\\?\").unwrap_or(&text).to_string()
        };
        // Windows paths do not depend on case.
        text.replace('/', "\\")
            .trim_end_matches('\\')
            .to_lowercase()
    } else {
        let trimmed = text.trim_end_matches('/');
        if trimmed.is_empty() { "/" } else { trimmed }.to_string()
    }
}

/// The first 16 hex digits of the SHA-256 of a normalized path.
fn dir_hash(normalized: &str) -> String {
    let digest = ring::digest::digest(&ring::digest::SHA256, normalized.as_bytes());
    digest.as_ref()[..8]
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// The account name for a secret, without the namespace. Windows matches
/// credential names without regard to case, so upper-case letters are
/// marked (`Prod` -> `^prod`) to keep profiles such as `Prod` and `prod`
/// apart.
fn account(profile: &str, field: &str) -> String {
    let mut name = String::new();
    for c in profile.chars() {
        if c == '^' {
            name.push_str("^^");
        } else if c.is_uppercase() {
            name.push('^');
            name.extend(c.to_lowercase());
        } else {
            name.push(c);
        }
    }
    format!("{name}/{field}")
}

/// Reads `field` of `profile` in `namespace` through `read(account)`; in a
/// directory other than the default, falls back to the account without a
/// namespace and copies a value found there with `write(account, value)`
/// (a failed copy is only logged; the value is still returned).
fn get_in(
    namespace: &Namespace,
    profile: &str,
    field: &str,
    read: impl Fn(&str) -> KafkaResult<Option<String>>,
    write: impl Fn(&str, &str) -> KafkaResult<()>,
) -> KafkaResult<Option<String>> {
    let own = namespace.account(profile, field);
    if let Some(value) = read(&own)? {
        return Ok(Some(value));
    }
    if *namespace == Namespace::Default {
        return Ok(None);
    }
    let Some(value) = read(&account(profile, field))? else {
        return Ok(None);
    };
    if let Err(e) = write(&own, &value) {
        tracing::warn!(
            "Cannot copy {field} of profile '{profile}' to this config directory's entry \
             in the OS credential store: {e}"
        );
    }
    Ok(Some(value))
}

#[cfg(any(windows, target_os = "macos", target_os = "linux"))]
mod os {
    use crate::error::{KafkaError, KafkaResult};

    const SERVICE: &str = "ferrika";

    fn entry(account: &str) -> KafkaResult<keyring::Entry> {
        keyring::Entry::new(SERVICE, account).map_err(store_error)
    }

    fn store_error(e: keyring::Error) -> KafkaError {
        KafkaError::SecretStore(e.to_string())
    }

    pub(super) fn set(account: &str, value: &str) -> KafkaResult<()> {
        entry(account)?.set_password(value).map_err(store_error)
    }

    pub(super) fn get(account: &str) -> KafkaResult<Option<String>> {
        match entry(account)?.get_password() {
            Ok(value) => Ok(Some(value)),
            Err(keyring::Error::NoEntry) => Ok(None),
            Err(e) => Err(store_error(e)),
        }
    }

    pub(super) fn delete(account: &str) -> KafkaResult<()> {
        match entry(account)?.delete_credential() {
            Ok(()) | Err(keyring::Error::NoEntry) => Ok(()),
            Err(e) => Err(store_error(e)),
        }
    }
}

#[cfg(not(any(windows, target_os = "macos", target_os = "linux")))]
mod os {
    use crate::error::{KafkaError, KafkaResult};

    pub(super) fn set(_account: &str, _value: &str) -> KafkaResult<()> {
        Err(KafkaError::SecretStore(
            "no OS credential store on this platform".to_string(),
        ))
    }

    pub(super) fn get(_account: &str) -> KafkaResult<Option<String>> {
        Ok(None)
    }

    pub(super) fn delete(_account: &str) -> KafkaResult<()> {
        Ok(())
    }
}

/// Stores a secret of the current config directory, replacing any
/// previous value.
pub fn set(profile: &str, field: &str, value: &str) -> KafkaResult<()> {
    os::set(&Namespace::current().account(profile, field), value)
}

/// Reads a secret of the current config directory; `Ok(None)` when there
/// is no entry (see the module docs for the fallback to older entries).
pub fn get(profile: &str, field: &str) -> KafkaResult<Option<String>> {
    get_in(&Namespace::current(), profile, field, os::get, os::set)
}

/// Removes a secret of the current config directory; a missing entry is
/// not an error. An entry without a namespace that a read in another
/// directory fell back to stays (it may be the default directory's).
pub fn delete(profile: &str, field: &str) -> KafkaResult<()> {
    os::delete(&Namespace::current().account(profile, field))
}

/// [`delete`], and in a directory other than the default also the entry
/// without a namespace: for secrets whose profile name belongs to one
/// directory only (such as an operation id), which older versions kept
/// there.
pub fn delete_with_legacy(profile: &str, field: &str) -> KafkaResult<()> {
    let namespace = Namespace::current();
    os::delete(&namespace.account(profile, field))?;
    if namespace != Namespace::Default {
        os::delete(&account(profile, field))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::KafkaError;
    use std::cell::RefCell;
    use std::collections::BTreeMap;

    #[test]
    fn accounts_differ_by_case_even_when_compared_without_it() {
        let a = account("Prod", "sasl_password");
        let b = account("prod", "sasl_password");
        assert_ne!(a.to_lowercase(), b.to_lowercase());
        assert_eq!(b, "prod/sasl_password");
        assert_ne!(
            account("^p", "x").to_lowercase(),
            account("P", "x").to_lowercase()
        );
    }

    fn temp_dir(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("ferrika-secrets-{name}-{}", std::process::id()))
    }

    #[test]
    fn the_default_directory_keeps_the_old_account_names() {
        let home = temp_dir("home");
        let ns = Namespace::of(&home, &home);
        assert_eq!(ns, Namespace::Default);
        assert_eq!(ns.account("prod", "sasl_password"), "prod/sasl_password");
        // A trailing separator or a `.` component is the same directory.
        let mut with_dot = home.join(".");
        with_dot.push("");
        assert_eq!(Namespace::of(&with_dot, &home), Namespace::Default);
    }

    #[test]
    fn other_directories_get_their_own_accounts() {
        let home = temp_dir("home2");
        let a = Namespace::of(&temp_dir("a"), &home);
        let b = Namespace::of(&temp_dir("b"), &home);
        let Namespace::Dir(hash_a) = &a else {
            panic!("namespaced: {a:?}");
        };
        assert!(matches!(b, Namespace::Dir(_)), "{b:?}");
        assert_eq!(hash_a.len(), 16);
        assert!(hash_a.chars().all(|c| c.is_ascii_hexdigit()));
        assert_ne!(a.account("prod", "x"), b.account("prod", "x"));
        assert_eq!(a.account("prod", "x"), format!("prod/x@{hash_a}"));
        // Stable: the same directory gives the same account, whether or not
        // it exists yet.
        let dir = temp_dir("created-later");
        let before = Namespace::of(&dir, &home);
        std::fs::create_dir_all(&dir).unwrap();
        assert_eq!(Namespace::of(&dir, &home), before);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn the_hash_is_the_start_of_the_sha256_of_the_path() {
        assert_eq!(dir_hash("abc"), "ba7816bf8f01cfea");
    }

    #[cfg(windows)]
    #[test]
    fn windows_paths_hash_without_regard_to_case_or_separators() {
        let home = temp_dir("home3");
        let dir = temp_dir("Case");
        let upper = PathBuf::from(dir.to_string_lossy().to_uppercase());
        let slashes = PathBuf::from(dir.to_string_lossy().replace('\\', "/"));
        assert_eq!(Namespace::of(&dir, &home), Namespace::of(&upper, &home));
        assert_eq!(Namespace::of(&dir, &home), Namespace::of(&slashes, &home));
    }

    /// A credential store in memory.
    #[derive(Default)]
    struct Fake(RefCell<BTreeMap<String, String>>);

    impl Fake {
        fn get(&self, account: &str) -> KafkaResult<Option<String>> {
            Ok(self.0.borrow().get(account).cloned())
        }
        fn set(&self, account: &str, value: &str) -> KafkaResult<()> {
            self.0
                .borrow_mut()
                .insert(account.to_string(), value.to_string());
            Ok(())
        }
    }

    fn dir_ns() -> Namespace {
        Namespace::Dir("0123456789abcdef".into())
    }

    #[test]
    fn another_directory_reads_its_own_entry_first() {
        let store = Fake::default();
        let ns = dir_ns();
        store.set("prod/p", "legacy").unwrap();
        store.set(&ns.account("prod", "p"), "own").unwrap();
        let got = get_in(&ns, "prod", "p", |a| store.get(a), |a, v| store.set(a, v));
        assert_eq!(got.unwrap().as_deref(), Some("own"));
        assert_eq!(store.get("prod/p").unwrap().as_deref(), Some("legacy"));
    }

    #[test]
    fn another_directory_migrates_an_old_entry_once_and_leaves_it() {
        let store = Fake::default();
        let ns = dir_ns();
        store.set("prod/p", "legacy").unwrap();
        let got = get_in(&ns, "prod", "p", |a| store.get(a), |a, v| store.set(a, v));
        assert_eq!(got.unwrap().as_deref(), Some("legacy"));
        assert_eq!(
            store.get("prod/p@0123456789abcdef").unwrap().as_deref(),
            Some("legacy")
        );
        // The old entry stays for the default directory.
        assert_eq!(store.get("prod/p").unwrap().as_deref(), Some("legacy"));
        // The default directory changing its secret no longer reaches this
        // directory, which now has its own.
        store.set("prod/p", "changed").unwrap();
        let got = get_in(&ns, "prod", "p", |a| store.get(a), |a, v| store.set(a, v));
        assert_eq!(got.unwrap().as_deref(), Some("legacy"));
    }

    #[test]
    fn a_failed_migration_still_returns_the_value() {
        let store = Fake::default();
        store.set("prod/p", "legacy").unwrap();
        let got = get_in(
            &dir_ns(),
            "prod",
            "p",
            |a| store.get(a),
            |_, _| Err(KafkaError::SecretStore("locked".into())),
        );
        assert_eq!(got.unwrap().as_deref(), Some("legacy"));
    }

    #[test]
    fn the_default_directory_never_falls_back_and_nothing_is_migrated_from_nothing() {
        let store = Fake::default();
        store.set("prod/p@0123456789abcdef", "other dir").unwrap();
        let got = get_in(
            &Namespace::Default,
            "prod",
            "p",
            |a| store.get(a),
            |_, _| panic!("no write"),
        );
        assert_eq!(got.unwrap(), None);
        let got = get_in(
            &Namespace::Dir("fedcba9876543210".into()),
            "prod",
            "p",
            |a| store.get(a),
            |_, _| panic!("no write"),
        );
        assert_eq!(got.unwrap(), None);
    }

    #[test]
    fn read_errors_are_reported() {
        let got = get_in(
            &dir_ns(),
            "prod",
            "p",
            |_| Err(KafkaError::SecretStore("denied".into())),
            |_, _| Ok(()),
        );
        assert!(got.is_err());
    }
}
