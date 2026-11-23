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
/// Unix: the directory and files must be owned by root and not writable by
/// group or others. Windows: owner and allow entries of the DACL must be
/// Administrators, SYSTEM or TrustedInstaller only, for rights that add,
/// change or delete files or change permissions (inherit-only entries are
/// ignored). Other systems: no check.
pub fn dir_write_warnings(dir: &Path) -> Vec<String> {
    if !dir.is_dir() {
        return Vec::new();
    }
    let mut warnings = Vec::new();
    if let Some(reason) = writable_by_others(dir, true) {
        warnings.push(format!("{}: {reason}", dir.display()));
    }
    for name in POLICY_FILES {
        let path = dir.join(name);
        if std::fs::symlink_metadata(&path).is_ok() {
            if let Some(reason) = writable_by_others(&path, false) {
                warnings.push(format!("{}: {reason}", path.display()));
            }
        }
    }
    warnings
}

#[cfg(unix)]
fn writable_by_others(path: &Path, _is_dir: bool) -> Option<String> {
    use std::os::unix::fs::MetadataExt;
    let meta = std::fs::symlink_metadata(path).ok()?;
    if meta.file_type().is_symlink() {
        return Some("is a symbolic link".to_string());
    }
    unix_reason(meta.uid(), meta.mode())
}

/// The problem with an owner `uid` and `mode`, if any.
#[cfg(any(unix, test))]
fn unix_reason(uid: u32, mode: u32) -> Option<String> {
    let mut reasons = Vec::new();
    if uid != 0 {
        reasons.push(format!("owned by uid {uid}, not root"));
    }
    if mode & 0o022 != 0 {
        reasons.push(format!(
            "writable by group or others (mode {:o})",
            mode & 0o7777
        ));
    }
    (!reasons.is_empty()).then(|| reasons.join("; "))
}

#[cfg(windows)]
fn writable_by_others(path: &Path, is_dir: bool) -> Option<String> {
    win::check(path, is_dir)
}

#[cfg(not(any(unix, windows)))]
fn writable_by_others(_path: &Path, _is_dir: bool) -> Option<String> {
    None
}

#[cfg(windows)]
mod win {
    use std::os::windows::ffi::OsStrExt;
    use std::path::Path;
    use windows_sys::Win32::Foundation::LocalFree;
    use windows_sys::Win32::Security::Authorization::{
        ConvertSidToStringSidW, GetNamedSecurityInfoW, SE_FILE_OBJECT,
    };
    use windows_sys::Win32::Security::{
        GetAce, IsWellKnownSid, WinBuiltinAdministratorsSid, WinLocalSystemSid, ACCESS_ALLOWED_ACE,
        ACL, DACL_SECURITY_INFORMATION, INHERIT_ONLY_ACE, OWNER_SECURITY_INFORMATION,
        PSECURITY_DESCRIPTOR, PSID,
    };

    const TRUSTED_INSTALLER: &str =
        "S-1-5-80-956008885-3418522649-1831038044-1853292631-2271478464";
    const ACCESS_ALLOWED_ACE_TYPE: u8 = 0;

    /// Rights that let an account change what Ferrika reads: create files or
    /// folders (= write/append data on a file), delete children, delete,
    /// change the DACL or owner, and the generic write/all rights.
    const WRITE_RIGHTS: u32 = 0x0002 // FILE_ADD_FILE / FILE_WRITE_DATA
        | 0x0004 // FILE_ADD_SUBDIRECTORY / FILE_APPEND_DATA
        | 0x0040 // FILE_DELETE_CHILD
        | 0x0001_0000 // DELETE
        | 0x0004_0000 // WRITE_DAC
        | 0x0008_0000 // WRITE_OWNER
        | 0x1000_0000 // GENERIC_ALL
        | 0x4000_0000; // GENERIC_WRITE

    pub(super) fn check(path: &Path, _is_dir: bool) -> Option<String> {
        let wide: Vec<u16> = path.as_os_str().encode_wide().chain(Some(0)).collect();
        let mut owner: PSID = std::ptr::null_mut();
        let mut dacl: *mut ACL = std::ptr::null_mut();
        let mut descriptor: PSECURITY_DESCRIPTOR = std::ptr::null_mut();
        // SAFETY: a NUL-terminated path; owner and DACL point into the
        // descriptor, which is freed below.
        let rc = unsafe {
            GetNamedSecurityInfoW(
                wide.as_ptr(),
                SE_FILE_OBJECT,
                OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION,
                &mut owner,
                std::ptr::null_mut(),
                &mut dacl,
                std::ptr::null_mut(),
                &mut descriptor,
            )
        };
        if rc != 0 {
            return None;
        }
        let mut reasons = Vec::new();
        if !owner.is_null() && !trusted(owner) {
            reasons.push(format!("owned by {}, not Administrators", sid_text(owner)));
        }
        if dacl.is_null() {
            reasons.push("has no DACL (everyone has full access)".to_string());
        } else {
            // SAFETY: a valid ACL from the descriptor.
            let count = unsafe { (*dacl).AceCount };
            for index in 0..u32::from(count) {
                let mut ace: *mut core::ffi::c_void = std::ptr::null_mut();
                // SAFETY: index < AceCount; GetAce returns a pointer into the ACL.
                if unsafe { GetAce(dacl, index, &mut ace) } == 0 || ace.is_null() {
                    continue;
                }
                // SAFETY: every ACE starts with an ACE_HEADER; an allow ACE
                // is an ACCESS_ALLOWED_ACE whose SID starts at SidStart.
                let allowed = unsafe { &*(ace as *const ACCESS_ALLOWED_ACE) };
                if allowed.Header.AceType != ACCESS_ALLOWED_ACE_TYPE
                    || u32::from(allowed.Header.AceFlags) & INHERIT_ONLY_ACE != 0
                    || allowed.Mask & WRITE_RIGHTS == 0
                {
                    continue;
                }
                let sid = std::ptr::addr_of!(allowed.SidStart) as PSID;
                if !trusted(sid) {
                    reasons.push(format!(
                        "{} may write, create or delete files (rights 0x{:x})",
                        sid_text(sid),
                        allowed.Mask
                    ));
                }
            }
        }
        // SAFETY: allocated by GetNamedSecurityInfoW; owner/dacl unused after.
        unsafe { LocalFree(descriptor as _) };
        (!reasons.is_empty()).then(|| reasons.join("; "))
    }

    fn trusted(sid: PSID) -> bool {
        // SAFETY: a valid SID for the duration of the calls.
        let well_known = unsafe {
            IsWellKnownSid(sid, WinBuiltinAdministratorsSid) != 0
                || IsWellKnownSid(sid, WinLocalSystemSid) != 0
        };
        // TrustedInstaller has no well-known SID type.
        well_known || sid_text(sid) == TRUSTED_INSTALLER
    }

    /// `S-1-5-32-545` style text of `sid`.
    fn sid_text(sid: PSID) -> String {
        let mut raw: windows_sys::core::PWSTR = std::ptr::null_mut();
        // SAFETY: a valid SID; the string is LocalAlloc'd and freed below.
        if unsafe { ConvertSidToStringSidW(sid, &mut raw) } == 0 || raw.is_null() {
            return "an unknown account".to_string();
        }
        // SAFETY: a NUL-terminated UTF-16 string.
        let text = unsafe {
            let len = (0..).take_while(|&i| *raw.add(i) != 0).count();
            String::from_utf16_lossy(std::slice::from_raw_parts(raw, len))
        };
        // SAFETY: allocated by ConvertSidToStringSidW.
        unsafe { LocalFree(raw as _) };
        text
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unix_reasons_name_the_owner_and_mode() {
        assert_eq!(unix_reason(0, 0o40755), None);
        assert_eq!(unix_reason(0, 0o100644), None);
        let both = unix_reason(1000, 0o40777).unwrap();
        assert!(
            both.contains("uid 1000") && both.contains("mode 777"),
            "{both}"
        );
        assert!(unix_reason(0, 0o40775).unwrap().contains("group or others"));
    }

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
