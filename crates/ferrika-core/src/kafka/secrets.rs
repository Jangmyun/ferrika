//! OS credential store for profile secrets.
//!
//! Profile passwords go to Windows Credential Manager, the macOS Keychain or
//! the Linux Secret Service instead of `profiles.json`. Entries use the
//! service `ferrika` and the account `<profile>/<field>`.
//!
//! `FERRIKA_SECRET_STORE=file` keeps new secrets in the file (headless Linux
//! without a Secret Service, CI). Secrets already in the OS store are still
//! read.

use crate::error::{KafkaError, KafkaResult};

#[cfg(any(windows, target_os = "macos", target_os = "linux"))]
const SERVICE: &str = "ferrika";

/// Whether new secrets should be written to the OS store.
pub fn enabled() -> bool {
    cfg!(any(windows, target_os = "macos", target_os = "linux"))
        && !std::env::var("FERRIKA_SECRET_STORE").is_ok_and(|v| v.eq_ignore_ascii_case("file"))
}

#[cfg(any(windows, target_os = "macos", target_os = "linux"))]
fn entry(profile: &str, field: &str) -> KafkaResult<keyring::Entry> {
    keyring::Entry::new(SERVICE, &account(profile, field)).map_err(store_error)
}

#[cfg(any(windows, target_os = "macos", target_os = "linux"))]
fn store_error(e: keyring::Error) -> KafkaError {
    KafkaError::SecretStore(e.to_string())
}

/// The account name for a secret. Windows matches credential names without
/// regard to case, so upper-case letters are marked (`Prod` -> `^prod`) to
/// keep profiles such as `Prod` and `prod` apart.
#[cfg_attr(
    not(any(windows, target_os = "macos", target_os = "linux")),
    allow(dead_code)
)]
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

/// Stores a secret, replacing any previous value.
#[cfg(any(windows, target_os = "macos", target_os = "linux"))]
pub fn set(profile: &str, field: &str, value: &str) -> KafkaResult<()> {
    entry(profile, field)?
        .set_password(value)
        .map_err(store_error)
}

/// Reads a secret; `Ok(None)` when there is no entry.
#[cfg(any(windows, target_os = "macos", target_os = "linux"))]
pub fn get(profile: &str, field: &str) -> KafkaResult<Option<String>> {
    match entry(profile, field)?.get_password() {
        Ok(value) => Ok(Some(value)),
        Err(keyring::Error::NoEntry) => Ok(None),
        Err(e) => Err(store_error(e)),
    }
}

/// Removes a secret; a missing entry is not an error.
#[cfg(any(windows, target_os = "macos", target_os = "linux"))]
pub fn delete(profile: &str, field: &str) -> KafkaResult<()> {
    match entry(profile, field)?.delete_credential() {
        Ok(()) | Err(keyring::Error::NoEntry) => Ok(()),
        Err(e) => Err(store_error(e)),
    }
}

#[cfg(not(any(windows, target_os = "macos", target_os = "linux")))]
pub fn set(_profile: &str, _field: &str, _value: &str) -> KafkaResult<()> {
    Err(KafkaError::SecretStore(
        "no OS credential store on this platform".to_string(),
    ))
}

#[cfg(not(any(windows, target_os = "macos", target_os = "linux")))]
pub fn get(_profile: &str, _field: &str) -> KafkaResult<Option<String>> {
    Ok(None)
}

#[cfg(not(any(windows, target_os = "macos", target_os = "linux")))]
pub fn delete(_profile: &str, _field: &str) -> KafkaResult<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
