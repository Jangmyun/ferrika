//! Whether a file or folder Ferrika relies on can be changed only by the
//! accounts it trusts. One definition shared by the policy directory warning
//! (`policy_dir_perm`) and the audit forwarding spool (`audit_forward`).
//!
//! Unix: the owner uid, the group/other write bits and, for spool files,
//! the hard-link count. Windows: reparse points, the owner SID and the DACL.
//! An allow entry that grants another account a right to write, append,
//! create or delete files or to change the permissions or owner makes the
//! object unsafe, unless an earlier deny entry for the same account (or for
//! Everyone) takes that right away. Entries are evaluated in the order
//! Windows evaluates them, inherited entries included; inherit-only entries
//! are skipped except where a folder passes them on to the files created in
//! it.

use std::path::Path;

/// Why `path` (the policy directory or a file in it) may be changed by an
/// account other than an administrator, or `None` when it looks locked
/// down. An error means its metadata or permissions could not be read.
///
/// Unix: it must not be a symbolic link, must be owned by root and not
/// writable by group or others. Windows: it must not be a symbolic link or
/// junction, the owner must be Administrators, SYSTEM or TrustedInstaller,
/// and the DACL may give the rights listed in the module docs only to them.
/// Other systems: no check.
pub(crate) fn admin_only(path: &Path) -> std::io::Result<Option<String>> {
    let meta = std::fs::symlink_metadata(path)?;
    if meta.file_type().is_symlink() {
        return Ok(Some(LINK_REASON.to_string()));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        Ok(unix_admin_reason(meta.uid(), meta.mode()))
    }
    #[cfg(windows)]
    {
        let descriptor = win::Descriptor::of_path(path)?;
        Ok(join(win::problems(
            &descriptor,
            &win::Rules::administrators(),
        )))
    }
    #[cfg(not(any(unix, windows)))]
    {
        Ok(None)
    }
}

#[cfg(unix)]
const LINK_REASON: &str = "is a symbolic link";
#[cfg(not(unix))]
const LINK_REASON: &str = "is a symbolic link or junction";

#[cfg(any(windows, test))]
fn join(reasons: Vec<String>) -> Option<String> {
    (!reasons.is_empty()).then(|| reasons.join("; "))
}

/// The problem with an owner `uid` and `mode` of a policy path, if any.
#[cfg(any(unix, test))]
fn unix_admin_reason(uid: u32, mode: u32) -> Option<String> {
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

/// Creates `dir` (and missing parents) for this account only: mode 0700 on
/// Unix; on Windows the folder gets a protected DACL that gives full access
/// to this account, SYSTEM and Administrators only, inherited by the files
/// created in it. Then checks it with [`check_private_dir`].
pub(crate) fn create_private_dir(dir: &Path, label: &str) -> std::io::Result<()> {
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(dir)?;
    #[cfg(windows)]
    win::protect_private_dir(dir)?;
    check_private_dir(dir, label)
}

/// Checks a folder that holds this account's private files (`label` names
/// it in errors, e.g. "spool folder").
///
/// Unix: a directory owned by this account or root, not writable by group
/// or others unless the sticky bit is set (then no one can rename or remove
/// another account's files, as in `/tmp`). Windows: a directory owned by
/// this account (or the default owner of its new objects), Administrators,
/// SYSTEM or TrustedInstaller, whose DACL lets other accounts at most add
/// files and subfolders (the Windows counterpart of the sticky bit: no
/// delete-child, delete, write-DAC or write-owner right), and passes no write
/// right for other accounts on to the files created in it.
pub(crate) fn check_private_dir(dir: &Path, label: &str) -> std::io::Result<()> {
    let meta = std::fs::metadata(dir)?;
    if !meta.is_dir() {
        return Err(std::io::Error::other(format!(
            "{label} {} is not a directory",
            dir.display()
        )));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let uid = current_uid();
        if meta.uid() != uid && meta.uid() != 0 {
            return Err(std::io::Error::other(format!(
                "{label} {} is owned by uid {}, not by this account or root; refusing to use it",
                dir.display(),
                meta.uid()
            )));
        }
        let sticky = meta.mode() & 0o1000 != 0;
        if meta.mode() & 0o022 != 0 && !sticky {
            return Err(std::io::Error::other(format!(
                "{label} {} may be written by group or others (mode {:o}); \
                 refusing to use it (chmod go-w, or set the sticky bit)",
                dir.display(),
                meta.mode() & 0o7777
            )));
        }
    }
    #[cfg(windows)]
    {
        let account = win::Account::current()?;
        let descriptor = win::Descriptor::of_path(dir)?;
        let reasons = win::problems(&descriptor, &win::Rules::private_dir(&account));
        if let Some(reasons) = join(reasons) {
            return Err(std::io::Error::other(format!(
                "{label} {} may be changed by other accounts ({reasons}); refusing to use it \
                 (remove those permissions, e.g. with icacls, or use a folder only this account can write)",
                dir.display()
            )));
        }
    }
    Ok(())
}

/// Checks an open file that only this account may change: a regular file
/// (not a link) owned by this account. Unix: not writable by group or
/// others and with exactly one hard link. Windows: owned by this account
/// (or the default owner of its new objects, Administrators for an elevated
/// process), and the DACL gives write rights only to this account, SYSTEM
/// and Administrators.
pub(crate) fn check_private_file(file: &std::fs::File) -> std::io::Result<()> {
    let meta = file.metadata()?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if !meta.is_file() {
            return Err(std::io::Error::other("is not a regular file"));
        }
        let uid = current_uid();
        if meta.uid() != uid {
            return Err(std::io::Error::other(format!(
                "is owned by uid {}, not by this account (uid {uid})",
                meta.uid()
            )));
        }
        if meta.mode() & 0o022 != 0 {
            return Err(std::io::Error::other(format!(
                "may be written by group or others (mode {:o})",
                meta.mode() & 0o777
            )));
        }
        if meta.nlink() != 1 {
            return Err(std::io::Error::other(format!(
                "has {} hard links",
                meta.nlink()
            )));
        }
    }
    #[cfg(windows)]
    {
        if meta.file_type().is_symlink() || !meta.is_file() {
            return Err(std::io::Error::other(
                "is a junction, symbolic link or not a regular file",
            ));
        }
        let account = win::Account::current()?;
        let descriptor = win::Descriptor::of_file(file)?;
        if let Some(reasons) = join(win::problems(
            &descriptor,
            &win::Rules::private_file(&account),
        )) {
            return Err(std::io::Error::other(reasons));
        }
    }
    #[cfg(not(any(unix, windows)))]
    if !meta.is_file() {
        return Err(std::io::Error::other("is not a regular file"));
    }
    Ok(())
}

#[cfg(unix)]
fn current_uid() -> u32 {
    // SAFETY: getuid has no preconditions and cannot fail.
    unsafe { libc::getuid() }
}

#[cfg(windows)]
mod win {
    use std::os::windows::ffi::OsStrExt;
    use std::os::windows::io::AsRawHandle;
    use std::path::Path;
    use windows_sys::Win32::Foundation::{CloseHandle, LocalFree, HANDLE};
    use windows_sys::Win32::Security::Authorization::{
        ConvertSidToStringSidW, GetNamedSecurityInfoW, GetSecurityInfo, SetEntriesInAclW,
        SetNamedSecurityInfoW, EXPLICIT_ACCESS_W, NO_MULTIPLE_TRUSTEE, SET_ACCESS, SE_FILE_OBJECT,
        TRUSTEE_IS_SID, TRUSTEE_IS_UNKNOWN, TRUSTEE_W,
    };
    use windows_sys::Win32::Security::{
        CreateWellKnownSid, EqualSid, GetAce, GetTokenInformation, IsWellKnownSid, TokenOwner,
        TokenUser, WinAnonymousSid, WinBuiltinAdministratorsSid, WinCreatorOwnerRightsSid,
        WinCreatorOwnerSid, WinLocalSystemSid, WinWorldSid, ACCESS_ALLOWED_ACE, ACE_HEADER, ACL,
        DACL_SECURITY_INFORMATION, INHERIT_ONLY_ACE, OBJECT_INHERIT_ACE,
        OWNER_SECURITY_INFORMATION, PROTECTED_DACL_SECURITY_INFORMATION, PSECURITY_DESCRIPTOR,
        PSID, SECURITY_MAX_SID_SIZE, SUB_CONTAINERS_AND_OBJECTS_INHERIT, TOKEN_INFORMATION_CLASS,
        TOKEN_OWNER, TOKEN_QUERY, TOKEN_USER, WELL_KNOWN_SID_TYPE,
    };
    use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};

    const TRUSTED_INSTALLER: &str =
        "S-1-5-80-956008885-3418522649-1831038044-1853292631-2271478464";

    const ACCESS_ALLOWED_ACE_TYPE: u8 = 0;
    const ACCESS_DENIED_ACE_TYPE: u8 = 1;
    /// A conditional allow entry; treated as an allow entry (it may apply).
    const ACCESS_ALLOWED_CALLBACK_ACE_TYPE: u8 = 9;

    pub(super) const FILE_WRITE_DATA: u32 = 0x0002; // also FILE_ADD_FILE
    pub(super) const FILE_APPEND_DATA: u32 = 0x0004; // also FILE_ADD_SUBDIRECTORY
    pub(super) const FILE_DELETE_CHILD: u32 = 0x0040;
    const DELETE: u32 = 0x0001_0000;
    const WRITE_DAC: u32 = 0x0004_0000;
    const WRITE_OWNER: u32 = 0x0008_0000;
    pub(super) const GENERIC_ALL: u32 = 0x1000_0000;
    const GENERIC_WRITE: u32 = 0x4000_0000;

    /// Rights that let an account change what Ferrika reads or writes:
    /// write or append data (= add files or folders on a folder), delete
    /// children, delete, and change the DACL or owner.
    const WRITE_RIGHTS: u32 =
        FILE_WRITE_DATA | FILE_APPEND_DATA | FILE_DELETE_CHILD | DELETE | WRITE_DAC | WRITE_OWNER;

    /// The [`WRITE_RIGHTS`] in `mask`, with the generic rights mapped the
    /// way the file system maps them.
    fn write_rights(mask: u32) -> u32 {
        let mut rights = mask & WRITE_RIGHTS;
        if mask & GENERIC_ALL != 0 {
            rights |= WRITE_RIGHTS;
        }
        if mask & GENERIC_WRITE != 0 {
            rights |= FILE_WRITE_DATA | FILE_APPEND_DATA;
        }
        rights
    }

    /// An object's security descriptor with its owner and DACL.
    pub(super) struct Descriptor {
        raw: PSECURITY_DESCRIPTOR,
        owner: PSID,
        pub(super) dacl: *mut ACL,
    }

    impl Descriptor {
        pub(super) fn of_path(path: &Path) -> std::io::Result<Self> {
            let wide = wide(path);
            let mut out = Self::empty();
            // SAFETY: a NUL-terminated path; the owner and DACL point into
            // the descriptor, which Drop frees.
            let rc = unsafe {
                GetNamedSecurityInfoW(
                    wide.as_ptr(),
                    SE_FILE_OBJECT,
                    OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION,
                    &mut out.owner,
                    std::ptr::null_mut(),
                    &mut out.dacl,
                    std::ptr::null_mut(),
                    &mut out.raw,
                )
            };
            out.result(rc)
        }

        pub(super) fn of_file(file: &std::fs::File) -> std::io::Result<Self> {
            let mut out = Self::empty();
            // SAFETY: a valid file handle; the owner and DACL point into the
            // descriptor, which Drop frees.
            let rc = unsafe {
                GetSecurityInfo(
                    file.as_raw_handle() as HANDLE,
                    SE_FILE_OBJECT,
                    OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION,
                    &mut out.owner,
                    std::ptr::null_mut(),
                    &mut out.dacl,
                    std::ptr::null_mut(),
                    &mut out.raw,
                )
            };
            out.result(rc)
        }

        fn empty() -> Self {
            Self {
                raw: std::ptr::null_mut(),
                owner: std::ptr::null_mut(),
                dacl: std::ptr::null_mut(),
            }
        }

        fn result(self, rc: u32) -> std::io::Result<Self> {
            if rc != 0 {
                return Err(std::io::Error::from_raw_os_error(rc as i32));
            }
            Ok(self)
        }
    }

    impl Drop for Descriptor {
        fn drop(&mut self) {
            if !self.raw.is_null() {
                // SAFETY: allocated by Get(Named)SecurityInfo, freed once;
                // owner and dacl are not used after this.
                unsafe { LocalFree(self.raw as _) };
            }
        }
    }

    /// The SIDs of this process's token: its user and the default owner of
    /// the objects it creates (Administrators for an elevated process).
    pub(super) struct Account {
        user: Vec<u64>,
        owner: Vec<u64>,
    }

    impl Account {
        pub(super) fn current() -> std::io::Result<Self> {
            let mut token: HANDLE = std::ptr::null_mut();
            // SAFETY: the pseudo handle of this process and an out pointer.
            if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) } == 0 {
                return Err(std::io::Error::last_os_error());
            }
            let result = token_info(token, TokenUser)
                .and_then(|user| Ok((user, token_info(token, TokenOwner)?)));
            // SAFETY: opened above, closed once.
            unsafe { CloseHandle(token) };
            let (user, owner) = result?;
            Ok(Self { user, owner })
        }

        pub(super) fn user_sid(&self) -> PSID {
            // SAFETY: the buffer (8-byte aligned) holds a TOKEN_USER whose
            // SID points into the same buffer, which lives as long as self.
            unsafe { (*self.user.as_ptr().cast::<TOKEN_USER>()).User.Sid }
        }

        fn owner_sid(&self) -> PSID {
            // SAFETY: as above, for TOKEN_OWNER.
            unsafe { (*self.owner.as_ptr().cast::<TOKEN_OWNER>()).Owner }
        }

        /// Whether `sid` is this account's user or default owner.
        fn is(&self, sid: PSID) -> bool {
            // SAFETY: all SIDs are valid for the duration of the calls.
            unsafe { EqualSid(sid, self.user_sid()) != 0 || EqualSid(sid, self.owner_sid()) != 0 }
        }
    }

    /// Token information of `class` in an 8-byte aligned buffer.
    fn token_info(token: HANDLE, class: TOKEN_INFORMATION_CLASS) -> std::io::Result<Vec<u64>> {
        let mut len = 0u32;
        // SAFETY: a size query with no buffer.
        unsafe { GetTokenInformation(token, class, std::ptr::null_mut(), 0, &mut len) };
        if len == 0 {
            return Err(std::io::Error::last_os_error());
        }
        let mut buf = vec![0u64; (len as usize).div_ceil(8)];
        // SAFETY: `buf` has at least `len` bytes.
        let ok =
            unsafe { GetTokenInformation(token, class, buf.as_mut_ptr().cast(), len, &mut len) };
        if ok == 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(buf)
    }

    /// Who may own an object and who besides administrators may change it.
    pub(super) struct Rules<'a> {
        /// `None`: administrators only. `Some`: also this account, and
        /// CREATOR OWNER / OWNER RIGHTS (the owner, which must be trusted).
        account: Option<&'a Account>,
        /// Whether Administrators, SYSTEM or TrustedInstaller may own it
        /// when an account is given.
        admin_owner: bool,
        /// A folder that other accounts may add files to, like a Unix
        /// folder with the sticky bit: on the folder itself, add-file and
        /// add-subfolder rights of other accounts are accepted (they cannot
        /// delete, rename or change this account's files without the other
        /// rights), and the entries it passes on to new files are checked.
        shared_folder: bool,
    }

    impl<'a> Rules<'a> {
        pub(super) fn administrators() -> Self {
            Self {
                account: None,
                admin_owner: true,
                shared_folder: false,
            }
        }

        pub(super) fn private_dir(account: &'a Account) -> Self {
            Self {
                account: Some(account),
                admin_owner: true,
                shared_folder: true,
            }
        }

        pub(super) fn private_file(account: &'a Account) -> Self {
            Self {
                account: Some(account),
                admin_owner: false,
                shared_folder: false,
            }
        }

        fn owner_ok(&self, sid: PSID) -> bool {
            match self.account {
                None => trusted_admin(sid),
                Some(account) => account.is(sid) || (self.admin_owner && trusted_admin(sid)),
            }
        }

        fn writer_ok(&self, sid: PSID) -> bool {
            if trusted_admin(sid) {
                return true;
            }
            let Some(account) = self.account else {
                return false;
            };
            // SAFETY: a valid SID for the duration of the calls.
            account.is(sid)
                || unsafe {
                    IsWellKnownSid(sid, WinCreatorOwnerSid) != 0
                        || IsWellKnownSid(sid, WinCreatorOwnerRightsSid) != 0
                }
        }
    }

    /// Why the object of `descriptor` breaks `rules`; empty when it does not.
    pub(super) fn problems(descriptor: &Descriptor, rules: &Rules<'_>) -> Vec<String> {
        let mut reasons = Vec::new();
        if !descriptor.owner.is_null() && !rules.owner_ok(descriptor.owner) {
            let expected = if rules.account.is_some() {
                "by this account"
            } else {
                "Administrators"
            };
            reasons.push(format!(
                "owned by {}, not {expected}",
                sid_text(descriptor.owner)
            ));
        }
        reasons.extend(dacl_problems(descriptor.dacl, rules));
        reasons
    }

    /// One access entry: its type, flags, access mask and SID.
    struct Ace {
        kind: u8,
        flags: u32,
        mask: u32,
        sid: PSID,
    }

    /// The allow and deny entries of `dacl`, in order.
    fn aces(dacl: *const ACL) -> Vec<Ace> {
        // SAFETY: a valid ACL (from a descriptor or built by the caller).
        let count = unsafe { (*dacl).AceCount };
        let mut out = Vec::with_capacity(usize::from(count));
        for index in 0..u32::from(count) {
            let mut ace: *mut core::ffi::c_void = std::ptr::null_mut();
            // SAFETY: index < AceCount; GetAce returns a pointer into the ACL.
            if unsafe { GetAce(dacl, index, &mut ace) } == 0 || ace.is_null() {
                continue;
            }
            // SAFETY: every ACE starts with an ACE_HEADER.
            let header = unsafe { &*(ace as *const ACE_HEADER) };
            if !matches!(
                header.AceType,
                ACCESS_ALLOWED_ACE_TYPE | ACCESS_DENIED_ACE_TYPE | ACCESS_ALLOWED_CALLBACK_ACE_TYPE
            ) {
                continue;
            }
            // SAFETY: allow, deny and allow-callback entries share the
            // ACCESS_ALLOWED_ACE layout; the SID starts at SidStart.
            let entry = unsafe { &*(ace as *const ACCESS_ALLOWED_ACE) };
            out.push(Ace {
                kind: header.AceType,
                flags: u32::from(header.AceFlags),
                mask: entry.Mask,
                sid: std::ptr::addr_of!(entry.SidStart) as PSID,
            });
        }
        out
    }

    /// Why `dacl` gives write rights to an account outside `rules`: on the
    /// object itself (entries that are not inherit-only) and, for a folder
    /// whose new files matter, on the files created in it (entries
    /// inherited by files).
    pub(super) fn dacl_problems(dacl: *const ACL, rules: &Rules<'_>) -> Vec<String> {
        if dacl.is_null() {
            return vec!["has no DACL (everyone has full access)".to_string()];
        }
        let all = aces(dacl);
        let mut reasons = Vec::new();
        if rules.shared_folder {
            evaluate(
                all.iter().filter(|a| a.flags & INHERIT_ONLY_ACE == 0),
                rules,
                FILE_WRITE_DATA | FILE_APPEND_DATA,
                "may delete files or change permissions",
                &mut reasons,
            );
            evaluate(
                all.iter().filter(|a| a.flags & OBJECT_INHERIT_ACE != 0),
                rules,
                0,
                "may write the files created in it",
                &mut reasons,
            );
        } else {
            evaluate(
                all.iter().filter(|a| a.flags & INHERIT_ONLY_ACE == 0),
                rules,
                0,
                "may write, create or delete files",
                &mut reasons,
            );
        }
        reasons.dedup();
        reasons
    }

    /// Walks `aces` in order as the access check does: a deny entry takes
    /// its rights away from later allow entries of the same SID (a deny for
    /// Everyone from every SID but Anonymous, which a token may hold without
    /// Everyone). A write right an untrusted SID still gets, apart from
    /// `accepted`, is a problem.
    fn evaluate<'x>(
        aces: impl Iterator<Item = &'x Ace>,
        rules: &Rules<'_>,
        accepted: u32,
        what: &str,
        reasons: &mut Vec<String>,
    ) {
        let mut denied_everyone = 0u32;
        let mut denied: Vec<(PSID, u32)> = Vec::new();
        for ace in aces {
            let rights = write_rights(ace.mask);
            if ace.kind == ACCESS_DENIED_ACE_TYPE {
                // SAFETY: a SID inside the ACL.
                if unsafe { IsWellKnownSid(ace.sid, WinWorldSid) } != 0 {
                    denied_everyone |= rights;
                } else {
                    denied.push((ace.sid, rights));
                }
                continue;
            }
            let mut granted = rights & !accepted;
            // SAFETY: a SID inside the ACL.
            if unsafe { IsWellKnownSid(ace.sid, WinAnonymousSid) } == 0 {
                granted &= !denied_everyone;
            }
            for &(sid, mask) in &denied {
                // SAFETY: both SIDs are inside the ACL.
                if unsafe { EqualSid(sid, ace.sid) } != 0 {
                    granted &= !mask;
                }
            }
            if granted != 0 && !rules.writer_ok(ace.sid) {
                reasons.push(format!(
                    "{} {what} (rights 0x{granted:x})",
                    sid_text(ace.sid)
                ));
            }
        }
    }

    fn trusted_admin(sid: PSID) -> bool {
        // SAFETY: a valid SID for the duration of the calls.
        let well_known = unsafe {
            IsWellKnownSid(sid, WinBuiltinAdministratorsSid) != 0
                || IsWellKnownSid(sid, WinLocalSystemSid) != 0
        };
        // TrustedInstaller has no well-known SID type.
        well_known || sid_text(sid) == TRUSTED_INSTALLER
    }

    /// A well-known SID in a 4-byte aligned buffer.
    pub(super) fn well_known(kind: WELL_KNOWN_SID_TYPE) -> std::io::Result<Vec<u32>> {
        let mut buf = vec![0u32; (SECURITY_MAX_SID_SIZE as usize).div_ceil(4)];
        let mut len = SECURITY_MAX_SID_SIZE;
        // SAFETY: `buf` has SECURITY_MAX_SID_SIZE bytes; no domain SID.
        let ok = unsafe {
            CreateWellKnownSid(
                kind,
                std::ptr::null_mut(),
                buf.as_mut_ptr().cast(),
                &mut len,
            )
        };
        if ok == 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(buf)
    }

    /// An explicit-access entry for `sid` (which must outlive its use).
    pub(super) fn explicit_access(
        sid: PSID,
        rights: u32,
        mode: i32,
        inheritance: u32,
    ) -> EXPLICIT_ACCESS_W {
        EXPLICIT_ACCESS_W {
            grfAccessPermissions: rights,
            grfAccessMode: mode,
            grfInheritance: inheritance,
            Trustee: TRUSTEE_W {
                pMultipleTrustee: std::ptr::null_mut(),
                MultipleTrusteeOperation: NO_MULTIPLE_TRUSTEE,
                TrusteeForm: TRUSTEE_IS_SID,
                TrusteeType: TRUSTEE_IS_UNKNOWN,
                ptstrName: sid.cast(),
            },
        }
    }

    /// Replaces the DACL of `path` with `entries` merged into `old` (null:
    /// start empty); `protect` stops inheritance from the parent.
    pub(super) fn set_dacl(
        path: &Path,
        entries: &[EXPLICIT_ACCESS_W],
        old: *const ACL,
        protect: bool,
    ) -> std::io::Result<()> {
        let mut acl: *mut ACL = std::ptr::null_mut();
        let count = u32::try_from(entries.len()).map_err(std::io::Error::other)?;
        // SAFETY: `entries` holds `count` entries whose SIDs are alive; the
        // new ACL is LocalAlloc'd and freed below.
        let rc = unsafe { SetEntriesInAclW(count, entries.as_ptr(), old, &mut acl) };
        if rc != 0 {
            return Err(std::io::Error::from_raw_os_error(rc as i32));
        }
        let wide = wide(path);
        let mut info = DACL_SECURITY_INFORMATION;
        if protect {
            info |= PROTECTED_DACL_SECURITY_INFORMATION;
        }
        // SAFETY: a NUL-terminated path and the ACL built above.
        let rc = unsafe {
            SetNamedSecurityInfoW(
                wide.as_ptr(),
                SE_FILE_OBJECT,
                info,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                acl,
                std::ptr::null(),
            )
        };
        // SAFETY: allocated by SetEntriesInAclW, freed once.
        unsafe { LocalFree(acl as _) };
        if rc != 0 {
            return Err(std::io::Error::from_raw_os_error(rc as i32));
        }
        Ok(())
    }

    /// Gives `dir` a protected DACL: full access for this account, SYSTEM
    /// and Administrators, inherited by everything created in it.
    pub(super) fn protect_private_dir(dir: &Path) -> std::io::Result<()> {
        let account = Account::current()?;
        let system = well_known(WinLocalSystemSid)?;
        let admins = well_known(WinBuiltinAdministratorsSid)?;
        let entries = [
            account.user_sid(),
            system.as_ptr() as PSID,
            admins.as_ptr() as PSID,
        ]
        .map(|sid| {
            explicit_access(
                sid,
                GENERIC_ALL,
                SET_ACCESS,
                SUB_CONTAINERS_AND_OBJECTS_INHERIT,
            )
        });
        set_dacl(dir, &entries, std::ptr::null(), true)
    }

    fn wide(path: &Path) -> Vec<u16> {
        path.as_os_str().encode_wide().chain(Some(0)).collect()
    }

    /// `S-1-5-32-545` style text of `sid`.
    pub(super) fn sid_text(sid: PSID) -> String {
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

/// A fresh (not yet created) path `name` for a test, inside a folder only
/// this account can change. The system temp folder may grant other accounts
/// write access (sandbox or CI accounts on Windows), which the checks here
/// rightly reject.
#[cfg(test)]
pub(crate) fn test_dir(name: &str) -> std::path::PathBuf {
    static BASE: std::sync::OnceLock<std::path::PathBuf> = std::sync::OnceLock::new();
    // Once per process: resetting the DACL propagates to every test folder
    // in it and would race with tests that change their own entries.
    let base = BASE.get_or_init(|| {
        let base = std::env::temp_dir().join("ferrika-test-private");
        if check_private_dir(&base, "test folder").is_err() {
            if let Err(e) = create_private_dir(&base, "test folder") {
                panic!("cannot prepare {}: {e}", base.display());
            }
        }
        base
    });
    let dir = base.join(name);
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unix_admin_reasons_name_the_owner_and_mode() {
        assert_eq!(unix_admin_reason(0, 0o40755), None);
        assert_eq!(unix_admin_reason(0, 0o100644), None);
        let both = unix_admin_reason(1000, 0o40777).unwrap();
        assert!(
            both.contains("uid 1000") && both.contains("mode 777"),
            "{both}"
        );
        assert!(unix_admin_reason(0, 0o40775)
            .unwrap()
            .contains("group or others"));
        assert_eq!(join(Vec::new()), None);
        assert_eq!(join(vec!["a".into(), "b".into()]).as_deref(), Some("a; b"));
    }

    fn temp_dir(name: &str) -> std::path::PathBuf {
        test_dir(&format!(
            "ferrika-filesec-{name}-{}-{}",
            std::process::id(),
            ferrika_core::kafka::clock::now_ms()
        ))
    }

    #[test]
    fn a_created_private_dir_and_its_files_pass() {
        let dir = temp_dir("create").join("nested");
        create_private_dir(&dir, "test folder").unwrap();
        check_private_dir(&dir, "test folder").unwrap();
        let path = dir.join("file");
        std::fs::write(&path, "x").unwrap();
        let file = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        check_private_file(&file).unwrap();
        drop(file);
        let _ = std::fs::remove_dir_all(dir.parent().unwrap());
    }

    #[cfg(windows)]
    mod windows {
        use super::super::win::*;
        use super::super::*;
        use super::temp_dir;
        use windows_sys::Win32::Security::Authorization::{DENY_ACCESS, GRANT_ACCESS};
        use windows_sys::Win32::Security::{
            AddAccessAllowedAceEx, AddAccessDeniedAceEx, InitializeAcl, WinBuiltinUsersSid,
            WinWorldSid, ACL, ACL_REVISION, INHERITED_ACE, INHERIT_ONLY_ACE, NO_INHERITANCE,
            OBJECT_INHERIT_ACE, PSID, SUB_CONTAINERS_AND_OBJECTS_INHERIT, WELL_KNOWN_SID_TYPE,
        };

        const WRITE: u32 = FILE_WRITE_DATA | FILE_APPEND_DATA;

        /// Adds an entry for a well-known SID to the DACL of `path`, the way
        /// `icacls /grant` or `/deny` does (Windows keeps deny entries first).
        fn add(path: &Path, who: WELL_KNOWN_SID_TYPE, mode: i32, inheritance: u32) {
            add_rights(path, who, WRITE, mode, inheritance);
        }

        fn add_rights(
            path: &Path,
            who: WELL_KNOWN_SID_TYPE,
            rights: u32,
            mode: i32,
            inheritance: u32,
        ) {
            let sid = well_known(who).unwrap();
            let old = Descriptor::of_path(path).unwrap();
            let entry = explicit_access(sid.as_ptr() as PSID, rights, mode, inheritance);
            set_dacl(path, &[entry], old.dacl, false).unwrap();
        }

        fn file_in(dir: &Path, name: &str) -> std::path::PathBuf {
            let path = dir.join(name);
            std::fs::write(&path, "x").unwrap();
            path
        }

        fn check_file(path: &Path) -> std::io::Result<()> {
            // Read access is enough (and works when writing is denied).
            check_private_file(&std::fs::File::open(path).unwrap())
        }

        #[test]
        fn the_default_per_user_acl_is_safe() {
            let dir = temp_dir("default");
            std::fs::create_dir_all(&dir).unwrap();
            let path = file_in(&dir, "a");
            check_private_dir(&dir, "folder").unwrap();
            check_file(&path).unwrap();
            let _ = std::fs::remove_dir_all(dir);
        }

        #[test]
        fn a_write_grant_to_users_or_everyone_is_unsafe() {
            let dir = temp_dir("grant");
            std::fs::create_dir_all(&dir).unwrap();
            let users = file_in(&dir, "users");
            let everyone = file_in(&dir, "everyone");
            add(&users, WinBuiltinUsersSid, GRANT_ACCESS, NO_INHERITANCE);
            add(&everyone, WinWorldSid, GRANT_ACCESS, NO_INHERITANCE);
            let err = check_file(&users).unwrap_err().to_string();
            assert!(err.contains("S-1-5-32-545 may write"), "{err}");
            let err = check_file(&everyone).unwrap_err().to_string();
            assert!(err.contains("S-1-1-0 may write"), "{err}");
            // The folder itself: deleting other files is not accepted.
            add_rights(
                &dir,
                WinBuiltinUsersSid,
                FILE_DELETE_CHILD,
                GRANT_ACCESS,
                NO_INHERITANCE,
            );
            let err = check_private_dir(&dir, "spool folder")
                .unwrap_err()
                .to_string();
            assert!(
                err.contains("spool folder") && err.contains("S-1-5-32-545"),
                "{err}"
            );
            let _ = std::fs::remove_dir_all(dir);
        }

        #[test]
        fn a_shared_folder_where_users_only_add_files_is_accepted() {
            // The ACL Install-FerrikaPolicy.ps1 -SpoolDir sets: Users may
            // add files to the folder, CREATOR OWNER modifies its own files.
            let dir = temp_dir("shared");
            std::fs::create_dir_all(&dir).unwrap();
            add_rights(
                &dir,
                WinBuiltinUsersSid,
                WRITE,
                GRANT_ACCESS,
                NO_INHERITANCE,
            );
            add_rights(
                &dir,
                windows_sys::Win32::Security::WinCreatorOwnerSid,
                0x0013_01bf, // Modify
                GRANT_ACCESS,
                OBJECT_INHERIT_ACE | INHERIT_ONLY_ACE,
            );
            check_private_dir(&dir, "folder").unwrap();
            check_file(&file_in(&dir, "mine")).unwrap();
            let _ = std::fs::remove_dir_all(dir);
        }

        #[test]
        fn a_deny_entry_before_the_grant_makes_it_safe() {
            let dir = temp_dir("deny");
            std::fs::create_dir_all(&dir).unwrap();
            let path = file_in(&dir, "a");
            add(&path, WinBuiltinUsersSid, GRANT_ACCESS, NO_INHERITANCE);
            assert!(check_file(&path).is_err());
            add(&path, WinWorldSid, DENY_ACCESS, NO_INHERITANCE);
            check_file(&path).unwrap();
            let _ = std::fs::remove_dir_all(dir);
        }

        #[test]
        fn an_entry_inherited_from_the_folder_is_detected() {
            let dir = temp_dir("inherit");
            std::fs::create_dir_all(&dir).unwrap();
            let before = file_in(&dir, "before");
            add(
                &dir,
                WinBuiltinUsersSid,
                GRANT_ACCESS,
                SUB_CONTAINERS_AND_OBJECTS_INHERIT,
            );
            let after = file_in(&dir, "after");
            for path in [&before, &after] {
                // The file has no entry of its own for Users, only the
                // inherited one.
                let descriptor = Descriptor::of_path(path).unwrap();
                let flags = inherited_users_flags(&descriptor);
                assert!(flags.iter().all(|f| f & INHERITED_ACE != 0), "{flags:?}");
                assert!(!flags.is_empty());
                let err = check_file(path).unwrap_err().to_string();
                assert!(err.contains("S-1-5-32-545"), "{err}");
            }
            let _ = std::fs::remove_dir_all(dir);
        }

        #[test]
        fn an_inherit_only_entry_for_new_files_refuses_the_folder() {
            let dir = temp_dir("inherit-only");
            std::fs::create_dir_all(&dir).unwrap();
            add(
                &dir,
                WinBuiltinUsersSid,
                GRANT_ACCESS,
                OBJECT_INHERIT_ACE | INHERIT_ONLY_ACE,
            );
            let err = check_private_dir(&dir, "folder").unwrap_err().to_string();
            assert!(err.contains("files created in it"), "{err}");
            // A protected private folder drops it again.
            create_private_dir(&dir, "folder").unwrap();
            let _ = std::fs::remove_dir_all(dir);
        }

        fn inherited_users_flags(descriptor: &Descriptor) -> Vec<u32> {
            let users = well_known(WinBuiltinUsersSid).unwrap();
            let acl = descriptor.dacl;
            // SAFETY (test): a valid ACL from the descriptor.
            let count = unsafe { (*acl).AceCount };
            let mut out = Vec::new();
            for i in 0..u32::from(count) {
                let mut ace = std::ptr::null_mut();
                unsafe {
                    windows_sys::Win32::Security::GetAce(acl, i, &mut ace);
                    let entry = &*(ace as *const windows_sys::Win32::Security::ACCESS_ALLOWED_ACE);
                    let sid = std::ptr::addr_of!(entry.SidStart) as PSID;
                    if windows_sys::Win32::Security::EqualSid(sid, users.as_ptr() as PSID) != 0 {
                        out.push(u32::from(entry.Header.AceFlags));
                    }
                }
            }
            out
        }

        /// Builds an ACL in memory with entries in the given order (unlike
        /// SetEntriesInAcl, which sorts deny entries first).
        fn acl(entries: &[(bool, WELL_KNOWN_SID_TYPE, u32, u32)]) -> Vec<u32> {
            let mut buf = vec![0u32; 256];
            let size = (buf.len() * 4) as u32;
            let acl = buf.as_mut_ptr() as *mut ACL;
            unsafe {
                assert_ne!(InitializeAcl(acl, size, ACL_REVISION), 0);
                for &(allow, who, mask, flags) in entries {
                    let sid = well_known(who).unwrap();
                    let ok = if allow {
                        AddAccessAllowedAceEx(acl, ACL_REVISION, flags, mask, sid.as_ptr() as _)
                    } else {
                        AddAccessDeniedAceEx(acl, ACL_REVISION, flags, mask, sid.as_ptr() as _)
                    };
                    assert_ne!(ok, 0);
                }
            }
            buf
        }

        fn eval(entries: &[(bool, WELL_KNOWN_SID_TYPE, u32, u32)], dir: bool) -> Vec<String> {
            let account = Account::current().unwrap();
            let rules = if dir {
                Rules::private_dir(&account)
            } else {
                Rules::private_file(&account)
            };
            let buf = acl(entries);
            dacl_problems(buf.as_ptr() as *const ACL, &rules)
        }

        #[test]
        fn deny_entries_count_only_before_the_grant() {
            let users = WinBuiltinUsersSid;
            // Deny first: safe. Deny after the grant: the grant wins.
            assert!(eval(&[(false, users, WRITE, 0), (true, users, WRITE, 0)], false).is_empty());
            assert!(!eval(&[(true, users, WRITE, 0), (false, users, WRITE, 0)], false).is_empty());
            // A deny for Everyone covers Users; a deny for Users does not
            // cover Everyone.
            assert!(eval(
                &[(false, WinWorldSid, WRITE, 0), (true, users, WRITE, 0)],
                false
            )
            .is_empty());
            assert!(!eval(
                &[(false, users, WRITE, 0), (true, WinWorldSid, WRITE, 0)],
                false
            )
            .is_empty());
            // A partial deny leaves the other rights.
            let left = eval(
                &[(false, users, FILE_WRITE_DATA, 0), (true, users, WRITE, 0)],
                false,
            );
            assert!(left.iter().any(|r| r.contains("0x4")), "{left:?}");
            // GENERIC_ALL counts as write rights.
            assert!(!eval(&[(true, users, GENERIC_ALL, 0)], false).is_empty());
            // Read-only grants are fine.
            assert!(eval(&[(true, users, 0x0012_0089, 0)], false).is_empty());
        }

        #[test]
        fn inherit_only_entries_matter_only_for_new_files_of_a_folder() {
            let users = WinBuiltinUsersSid;
            let for_files = OBJECT_INHERIT_ACE | INHERIT_ONLY_ACE;
            assert!(eval(&[(true, users, WRITE, for_files)], false).is_empty());
            assert!(!eval(&[(true, users, WRITE, for_files)], true).is_empty());
            // On the folder itself, adding files is accepted (as with the
            // sticky bit), deleting or changing permissions is not.
            assert!(eval(&[(true, users, WRITE, 0)], true).is_empty());
            assert!(!eval(&[(true, users, FILE_DELETE_CHILD, 0)], true).is_empty());
            assert!(!eval(&[(true, users, GENERIC_ALL, 0)], true).is_empty());
            // A grant inherited by files and the folder counts for new files.
            assert!(!eval(&[(true, users, WRITE, OBJECT_INHERIT_ACE)], true).is_empty());
            // Inherited by subfolders only: not a problem for files.
            let for_folders = 0x2 | INHERIT_ONLY_ACE;
            assert!(eval(&[(true, users, WRITE, for_folders)], true).is_empty());
            // CREATOR OWNER becomes this account on new files.
            let creator = windows_sys::Win32::Security::WinCreatorOwnerSid;
            assert!(eval(&[(true, creator, GENERIC_ALL, for_files)], true).is_empty());
        }

        #[test]
        fn administrators_only_rules_reject_this_account() {
            let me = Account::current().unwrap();
            let mut buf = vec![0u32; 256];
            let acl = buf.as_mut_ptr() as *mut ACL;
            unsafe {
                assert_ne!(InitializeAcl(acl, 1024, ACL_REVISION), 0);
                assert_ne!(
                    AddAccessAllowedAceEx(acl, ACL_REVISION, 0, WRITE, me.user_sid()),
                    0
                );
            }
            assert!(!dacl_problems(acl, &Rules::administrators()).is_empty());
            assert!(dacl_problems(acl, &Rules::private_file(&me)).is_empty());
            assert!(sid_text(me.user_sid()).starts_with("S-1-5-"));
        }
    }
}
