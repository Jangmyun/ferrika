//! Best-effort check that the policy directory cannot be written by
//! ordinary accounts. Anyone who can add or replace a file there can at
//! least put Ferrika into strictest mode (a broken `policy.keys` makes the
//! policy invalid), so `ferrika policy show` warns about it. The install
//! scripts under `packaging/` set and verify the intended permissions.

use std::path::Path;

/// The files in the policy directory whose permissions are checked besides
/// the directory itself.
const POLICY_FILES: [&str; 3] = [
    crate::policy::POLICY_FILE,
    crate::policy::PUBLIC_KEY_FILE,
    crate::policy::KEY_LIST_FILE,
];

/// Why `dir` (or a policy file in it) may be writable by accounts other than
/// administrators; empty when it looks locked down or does not exist.
///
/// The rules are those of `file_security::admin_only`: on Unix, owned by
/// root and not writable by group or others; on Windows, owned by
/// Administrators, SYSTEM or TrustedInstaller, and the DACL gives no other
/// account a right to add, change or delete files or change permissions
/// (deny and inherited entries are evaluated in order, inherit-only entries
/// are ignored). Other systems: no check.
pub fn dir_write_warnings(dir: &Path) -> Vec<String> {
    if !dir.is_dir() {
        return Vec::new();
    }
    let mut warnings = Vec::new();
    if let Some(reason) = writable_by_others(dir) {
        warnings.push(format!("{}: {reason}", dir.display()));
    }
    for name in POLICY_FILES {
        let path = dir.join(name);
        if std::fs::symlink_metadata(&path).is_ok() {
            if let Some(reason) = writable_by_others(&path) {
                warnings.push(format!("{}: {reason}", path.display()));
            }
        }
    }
    warnings
}

/// Why `path` may be changed by a non-administrator; `None` when it looks
/// locked down or its permissions cannot be read.
fn writable_by_others(path: &Path) -> Option<String> {
    crate::file_security::admin_only(path).ok().flatten()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_missing_directory_has_no_warnings() {
        assert!(dir_write_warnings(Path::new("/definitely/not/here/ferrika")).is_empty());
    }

    #[test]
    fn a_folder_the_user_owns_is_reported() {
        // A temp folder belongs to the current, non-root account (Unix) or
        // gives the user write rights (Windows). Skipped when run as root.
        #[cfg(unix)]
        // SAFETY: getuid has no preconditions.
        if unsafe { libc::getuid() } == 0 {
            return;
        }
        let dir = std::env::temp_dir().join(format!(
            "ferrika-policy-perm-{}-{}",
            std::process::id(),
            ferrika_core::kafka::clock::now_ms()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(crate::policy::KEY_LIST_FILE), "").unwrap();
        let warnings = dir_write_warnings(&dir);
        let _ = std::fs::remove_dir_all(&dir);
        #[cfg(any(unix, windows))]
        {
            assert!(!warnings.is_empty());
            assert!(
                warnings
                    .iter()
                    .any(|w| w.contains(crate::policy::KEY_LIST_FILE)),
                "{warnings:?}"
            );
        }
        #[cfg(not(any(unix, windows)))]
        assert!(warnings.is_empty());
    }
}
