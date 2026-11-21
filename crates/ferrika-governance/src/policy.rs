//! Signed company policy: key generation, signing, verification and the
//! effective policy every command and GUI action is checked against.
//!
//! The policy lives in a machine-wide directory normal users cannot write
//! (the ProgramData known folder + `Ferrika` on Windows, `/etc/ferrika`
//! elsewhere; `FERRIKA_POLICY_DIR` overrides it in debug and `dev-policy-dir`
//! builds only). With no policy files Ferrika behaves as before. A policy
//! that is present but cannot be verified fails closed: the strictest rules
//! apply.

use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use ring::signature::{Ed25519KeyPair, KeyPair, UnparsedPublicKey, ED25519};

use crate::audit::AuditEntry;
use ferrika_core::kafka::config::KafkaClientConfig;

use jiff::Timestamp;

use ferrika_core::model::policy::{
    ExpiryRule, ExportRule, MaskingRule, PolicyAction, PolicyPayload, PolicyRules, SignedPolicy,
    UnmaskRule, WatermarkRule, POLICY_FORMAT_V1, POLICY_FORMAT_V2,
};

/// How long before `expires_at` Ferrika starts warning.
pub const EXPIRY_WARNING_DAYS: i64 = 14;

/// File names inside the policy directory.
pub const POLICY_FILE: &str = "company.policy";
pub const PUBLIC_KEY_FILE: &str = "policy.pub";
/// Optional further trusted keys, for key rotation (one per line).
pub const KEY_LIST_FILE: &str = "policy.keys";
/// Default name of the private key `policy keygen` writes.
pub const PRIVATE_KEY_FILE: &str = "policy-signing.pk8";

#[derive(Debug, thiserror::Error)]
pub enum PolicyError {
    #[error("could not read {path}: {source}")]
    Read {
        path: String,
        source: std::io::Error,
    },
    #[error("policy file is not valid JSON: {0}")]
    Format(String),
    #[error(
        "unsupported policy format {0} (this build reads formats {POLICY_FORMAT_V1} and {POLICY_FORMAT_V2})"
    )]
    Version(u32),
    #[error("public key is not a 32-byte Ed25519 key in hex")]
    PublicKey,
    #[error("private key is not an Ed25519 PKCS#8 key")]
    PrivateKey,
    #[error("policy was signed by key {found}, but the trusted key is {expected}")]
    KeyMismatch { expected: String, found: String },
    #[error("policy was signed by key {found}, which is not among the trusted keys ({trusted})")]
    UnknownKey { found: String, trusted: String },
    #[error("{path} line {line}: {reason}")]
    KeyList {
        path: String,
        line: usize,
        reason: String,
    },
    #[error("signature does not match the policy (the file was changed after signing)")]
    BadSignature,
    #[error("policy rules are invalid: {0}")]
    Rules(String),
    #[error("{0} is missing")]
    Missing(String),
}

/// Why the policy refuses an action. The text is the full message shown to
/// the user and recorded in the audit log; it reads as a `&str`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct PolicyRefusal(String);

impl std::ops::Deref for PolicyRefusal {
    type Target = str;

    fn deref(&self) -> &str {
        &self.0
    }
}

impl From<PolicyRefusal> for String {
    fn from(refusal: PolicyRefusal) -> Self {
        refusal.0
    }
}

/// Lowercase hex.
pub fn to_hex(bytes: &[u8]) -> String {
    ferrika_core::model::format::to_hex(bytes)
}

/// Parses hex (whitespace around it is ignored).
pub fn from_hex(text: &str) -> Option<Vec<u8>> {
    let text = text.trim();
    if !text.len().is_multiple_of(2) {
        return None;
    }
    (0..text.len())
        .step_by(2)
        .map(|i| {
            text.get(i..i + 2)
                .and_then(|h| u8::from_str_radix(h, 16).ok())
        })
        .collect()
}

/// Short id of a public key: the first 16 hex digits of its SHA-256.
pub fn key_id(public_key: &[u8]) -> String {
    let digest = ring::digest::digest(&ring::digest::SHA256, public_key);
    to_hex(&digest.as_ref()[..8])
}

/// A new signing key: (PKCS#8 private key, raw 32-byte public key).
pub fn generate_keypair() -> Result<(Vec<u8>, Vec<u8>), PolicyError> {
    let rng = ring::rand::SystemRandom::new();
    let pkcs8 = Ed25519KeyPair::generate_pkcs8(&rng).map_err(|_| PolicyError::PrivateKey)?;
    let pair = Ed25519KeyPair::from_pkcs8(pkcs8.as_ref()).map_err(|_| PolicyError::PrivateKey)?;
    Ok((pkcs8.as_ref().to_vec(), pair.public_key().as_ref().to_vec()))
}

/// Parses a payload and checks its rules make sense.
pub fn parse_payload(payload: &str) -> Result<PolicyPayload, PolicyError> {
    let parsed: PolicyPayload =
        serde_json::from_str(payload).map_err(|e| PolicyError::Rules(e.to_string()))?;
    parse_expiry(&parsed)?;
    let unknown = parsed.rules.unknown_forbid_entries();
    if !unknown.is_empty() {
        return Err(PolicyError::Rules(format!(
            "unknown forbid entries: {} (known: {})",
            unknown.join(", "),
            PolicyAction::ALL.map(PolicyAction::id).join(", ")
        )));
    }
    if let Some(reason) = parsed.rules.export_rules_error() {
        return Err(PolicyError::Rules(reason));
    }
    Ok(parsed)
}

/// Signs `payload` (exact bytes) with a PKCS#8 Ed25519 key.
/// The file is format 1 unless the payload uses a format 2 field, so
/// binaries that only read format 1 keep accepting it.
pub fn sign(pkcs8: &[u8], payload: &str) -> Result<SignedPolicy, PolicyError> {
    let format = parse_payload(payload)?.format();
    let pair = Ed25519KeyPair::from_pkcs8(pkcs8).map_err(|_| PolicyError::PrivateKey)?;
    let signature = pair.sign(payload.as_bytes());
    Ok(SignedPolicy {
        format,
        key_id: key_id(pair.public_key().as_ref()),
        payload: payload.to_string(),
        signature: to_hex(signature.as_ref()),
    })
}

/// Verifies a policy file's text against a trusted public key.
pub fn verify(policy_text: &str, public_key: &[u8]) -> Result<PolicyPayload, PolicyError> {
    if public_key.len() != 32 {
        return Err(PolicyError::PublicKey);
    }
    let key = TrustedKey::new(public_key.to_vec(), String::new(), String::new());
    verify_with(policy_text, std::slice::from_ref(&key)).map(|(payload, _)| payload)
}

/// Verifies a policy file's text against a set of trusted keys, using the
/// one its `key_id` names. Returns the payload and that key's id.
pub fn verify_with(
    policy_text: &str,
    keys: &[TrustedKey],
) -> Result<(PolicyPayload, String), PolicyError> {
    let signed: SignedPolicy =
        serde_json::from_str(policy_text).map_err(|e| PolicyError::Format(e.to_string()))?;
    if signed.format != POLICY_FORMAT_V1 && signed.format != POLICY_FORMAT_V2 {
        return Err(PolicyError::Version(signed.format));
    }
    let candidates: Vec<&TrustedKey> = keys.iter().filter(|k| k.key_id == signed.key_id).collect();
    if candidates.is_empty() {
        let mut ids: Vec<String> = keys.iter().map(|k| k.key_id.clone()).collect();
        ids.dedup();
        return Err(match ids.as_slice() {
            [only] => PolicyError::KeyMismatch {
                expected: only.clone(),
                found: signed.key_id,
            },
            _ => PolicyError::UnknownKey {
                found: signed.key_id,
                trusted: if ids.is_empty() {
                    "none".to_string()
                } else {
                    ids.join(", ")
                },
            },
        });
    }
    let signature = from_hex(&signed.signature).ok_or(PolicyError::BadSignature)?;
    // Two different keys could share a 64-bit id; the signature decides.
    if !candidates.iter().any(|k| {
        UnparsedPublicKey::new(&ED25519, &k.public_key)
            .verify(signed.payload.as_bytes(), &signature)
            .is_ok()
    }) {
        return Err(PolicyError::BadSignature);
    }
    let payload = parse_payload(&signed.payload)?;
    // Format 1 stays exactly what format 1 binaries read.
    if signed.format == POLICY_FORMAT_V1 && payload.uses_v2() {
        return Err(PolicyError::Rules(
            "format 2 fields (expires_at, on_expiry, audit_forward, audit_anchor, \
             anchor_required, read_only_brokers, export, export_clusters, watermark, \
             watermark_text, screen_capture, mcp_write) need format 2"
                .to_string(),
        ));
    }
    Ok((payload, signed.key_id))
}

/// A public key the policy directory trusts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrustedKey {
    pub key_id: String,
    /// Raw 32-byte Ed25519 key.
    pub public_key: Vec<u8>,
    /// The text after the key in `policy.keys`; empty for `policy.pub`.
    pub label: String,
    /// The file it came from (`policy.pub` or `policy.keys`).
    pub file: String,
}

impl TrustedKey {
    fn new(public_key: Vec<u8>, label: String, file: String) -> Self {
        Self {
            key_id: key_id(&public_key),
            public_key,
            label,
            file,
        }
    }
}

/// Parses a `policy.keys`: one `<64 hex digits> [label]` per line; `#`
/// starts a comment and blank lines are skipped. Any other line makes the
/// whole file (and so the policy) invalid. `path` is for messages.
pub(crate) fn parse_key_list(text: &str, path: &str) -> Result<Vec<TrustedKey>, PolicyError> {
    let file = Path::new(path)
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| path.to_string());
    let mut keys = Vec::new();
    for (index, raw) in text.lines().enumerate() {
        let line = raw.split('#').next().unwrap_or_default().trim();
        if line.is_empty() {
            continue;
        }
        let (hex, label) = match line.split_once(char::is_whitespace) {
            Some((hex, label)) => (hex, label.trim()),
            None => (line, ""),
        };
        let key = from_hex(hex)
            .filter(|k| k.len() == 32)
            .ok_or_else(|| PolicyError::KeyList {
                path: path.to_string(),
                line: index + 1,
                reason: "expected a 32-byte Ed25519 key in hex (64 digits), then an optional label"
                    .to_string(),
            })?;
        keys.push(TrustedKey::new(key, label.to_string(), file.clone()));
    }
    Ok(keys)
}

/// The keys `dir` trusts: `policy.pub` and every key in `policy.keys`, each
/// key once. Either file may be missing; one that is present but malformed
/// is an error.
pub fn trusted_keys(dir: &Path) -> Result<Vec<TrustedKey>, PolicyError> {
    let mut keys = Vec::new();
    let pub_path = dir.join(PUBLIC_KEY_FILE);
    if present(&pub_path)? {
        let key = read_public_key(&pub_path)?;
        keys.push(TrustedKey::new(
            key,
            String::new(),
            PUBLIC_KEY_FILE.to_string(),
        ));
    }
    let list_path = dir.join(KEY_LIST_FILE);
    if present(&list_path)? {
        let text = read_text(&list_path)?;
        for key in parse_key_list(&text, &list_path.display().to_string())? {
            if !keys.iter().any(|k| k.public_key == key.public_key) {
                keys.push(key);
            }
        }
    }
    Ok(keys)
}

/// When a policy expires and what happens then.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyExpiry {
    pub at: Timestamp,
    /// `expires_at` as written in the policy, for messages.
    pub text: String,
    pub on_expiry: ExpiryRule,
}

/// The payload's expiry. An `expires_at` that is not RFC 3339 is an error,
/// so the policy is invalid (fail closed).
pub fn parse_expiry(payload: &PolicyPayload) -> Result<Option<PolicyExpiry>, PolicyError> {
    let Some(text) = &payload.expires_at else {
        return Ok(None);
    };
    let text = text.trim();
    let at = text.parse::<Timestamp>().map_err(|e| {
        PolicyError::Rules(format!(
            "expires_at \"{text}\" is not an RFC 3339 time such as 2027-06-30T23:59:59+09:00 ({e})"
        ))
    })?;
    Ok(Some(PolicyExpiry {
        at,
        text: text.to_string(),
        on_expiry: payload.on_expiry.unwrap_or_default(),
    }))
}

/// Where a policy stands against its `expires_at` at a given time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpiryState {
    /// No `expires_at`, or more than [`EXPIRY_WARNING_DAYS`] away.
    Current,
    /// Expires within [`EXPIRY_WARNING_DAYS`]; `days_left` is rounded up.
    ExpiresSoon { days_left: i64 },
    /// Expired; the rules still apply (`on_expiry: warn`).
    ExpiredWarn,
    /// Expired; the strictest mode applies (`on_expiry: strictest`).
    ExpiredStrictest,
}

impl PolicyExpiry {
    pub fn state_at(&self, now: Timestamp) -> ExpiryState {
        if now >= self.at {
            return match self.on_expiry {
                ExpiryRule::Warn => ExpiryState::ExpiredWarn,
                ExpiryRule::Strictest => ExpiryState::ExpiredStrictest,
            };
        }
        let left = self.at.duration_since(now);
        if left > jiff::SignedDuration::from_hours(EXPIRY_WARNING_DAYS * 24) {
            return ExpiryState::Current;
        }
        let secs = left.as_secs().max(1);
        ExpiryState::ExpiresSoon {
            days_left: (secs + 86_399) / 86_400,
        }
    }
}

/// Reads a `policy.pub` (hex).
pub fn read_public_key(path: &Path) -> Result<Vec<u8>, PolicyError> {
    let text = read_text(path)?;
    from_hex(&text)
        .filter(|k| k.len() == 32)
        .ok_or(PolicyError::PublicKey)
}

/// Whether `path` exists. Only "not found" counts as missing: a permission
/// or other I/O error is an error, never "no file" (`Path::exists` would
/// turn it into `false`, and an unreadable policy would then enforce nothing).
fn present(path: &Path) -> Result<bool, PolicyError> {
    path.try_exists().map_err(|source| PolicyError::Read {
        path: path.display().to_string(),
        source,
    })
}

fn read_text(path: &Path) -> Result<String, PolicyError> {
    std::fs::read_to_string(path)
        .map(|t| t.trim_start_matches('\u{feff}').to_string())
        .map_err(|source| PolicyError::Read {
            path: path.display().to_string(),
            source,
        })
}

/// Environment variable that points Ferrika at another policy directory.
pub const POLICY_DIR_ENV: &str = "FERRIKA_POLICY_DIR";

/// Whether this build honours [`POLICY_DIR_ENV`]: debug builds and builds
/// with the `dev-policy-dir` feature (tests, demos). A release build ignores
/// it, so a user cannot point Ferrika at a policy they signed themselves.
pub(crate) const DIR_OVERRIDE_ALLOWED: bool =
    cfg!(any(debug_assertions, feature = "dev-policy-dir"));

/// The policy directory from its inputs: the override (the value of
/// [`POLICY_DIR_ENV`]) when `allow_override` and it is not empty, otherwise
/// `Ferrika` in the ProgramData known folder on Windows (`C:\ProgramData`
/// when the folder cannot be resolved; never the `%ProgramData%` variable)
/// and `/etc/ferrika` elsewhere.
pub(crate) fn policy_dir_from(
    override_dir: Option<&OsStr>,
    allow_override: bool,
    known_folder: Option<PathBuf>,
) -> PathBuf {
    if let Some(dir) = override_dir.filter(|d| allow_override && !d.is_empty()) {
        return PathBuf::from(dir);
    }
    if cfg!(windows) {
        known_folder
            .filter(|p| p.is_absolute())
            .unwrap_or_else(|| PathBuf::from(r"C:\ProgramData"))
            .join("Ferrika")
    } else {
        PathBuf::from("/etc/ferrika")
    }
}

/// Whether an override was given that this build ignores.
pub(crate) fn override_ignored(override_dir: Option<&OsStr>, allow_override: bool) -> bool {
    !allow_override && override_dir.is_some_and(|d| !d.is_empty())
}

/// The machine-wide policy directory. A release build ignores
/// [`POLICY_DIR_ENV`] and says so (see [`ferrika_core::notice`]), once per process.
pub fn policy_dir() -> PathBuf {
    let env = std::env::var_os(POLICY_DIR_ENV);
    let dir = policy_dir_from(env.as_deref(), DIR_OVERRIDE_ALLOWED, program_data());
    if override_ignored(env.as_deref(), DIR_OVERRIDE_ALLOWED) {
        static WARNED: std::sync::Once = std::sync::Once::new();
        WARNED.call_once(|| ferrika_core::notice::warn(&override_ignored_message(&dir)));
    }
    dir
}

fn override_ignored_message(dir: &Path) -> String {
    format!(
        "{POLICY_DIR_ENV} is ignored in release builds; the company policy is read from {}.",
        dir.display()
    )
}

/// The ProgramData known folder (`SHGetKnownFolderPath(FOLDERID_ProgramData)`).
#[cfg(windows)]
fn program_data() -> Option<PathBuf> {
    use std::os::windows::ffi::OsStringExt;
    use windows_sys::Win32::System::Com::CoTaskMemFree;
    use windows_sys::Win32::UI::Shell::{FOLDERID_ProgramData, SHGetKnownFolderPath};

    let mut raw: windows_sys::core::PWSTR = std::ptr::null_mut();
    // SAFETY: valid GUID pointer, no flags, no token (the current user), and
    // an out pointer the call fills with a CoTaskMemAlloc'd string.
    let hr =
        unsafe { SHGetKnownFolderPath(&FOLDERID_ProgramData, 0, std::ptr::null_mut(), &mut raw) };
    let path = if hr >= 0 && !raw.is_null() {
        // SAFETY: on success `raw` is a NUL-terminated UTF-16 string.
        let wide = unsafe {
            let len = (0..).take_while(|&i| *raw.add(i) != 0).count();
            std::slice::from_raw_parts(raw, len)
        };
        Some(PathBuf::from(std::ffi::OsString::from_wide(wide)))
    } else {
        None
    };
    // SAFETY: the string must be freed whether the call succeeded or not;
    // freeing null is a no-op.
    unsafe { CoTaskMemFree(raw as *const std::ffi::c_void) };
    path
}

#[cfg(not(windows))]
fn program_data() -> Option<PathBuf> {
    None
}

/// Whether a policy is in force and whether it could be trusted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PolicyStatus {
    /// No policy files: nothing is enforced.
    Absent,
    /// Signature verified.
    Verified { issuer: String, key_id: String },
    /// Present but untrusted; the strictest rules apply.
    Invalid { reason: String },
}

/// The policy this process enforces.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EffectivePolicy {
    pub status: PolicyStatus,
    pub rules: PolicyRules,
    /// The directory the policy was looked for in.
    pub dir: PathBuf,
    pub issued_at: String,
    /// Format 2 `expires_at`. Checked whenever the policy is consulted, since
    /// the GUI can run past it.
    pub expiry: Option<PolicyExpiry>,
    /// The keys the directory trusts (`policy.pub` + `policy.keys`); empty
    /// when they could not be read.
    pub trusted_keys: Vec<TrustedKey>,
}

impl EffectivePolicy {
    pub fn absent(dir: PathBuf) -> Self {
        Self {
            status: PolicyStatus::Absent,
            rules: PolicyRules::default(),
            dir,
            issued_at: String::new(),
            expiry: None,
            trusted_keys: Vec::new(),
        }
    }

    /// Fail closed: the policy could not be trusted.
    pub fn invalid(dir: PathBuf, reason: String) -> Self {
        Self {
            status: PolicyStatus::Invalid { reason },
            rules: PolicyRules::strictest(),
            dir,
            issued_at: String::new(),
            expiry: None,
            trusted_keys: Vec::new(),
        }
    }

    pub fn is_active(&self) -> bool {
        !matches!(self.status, PolicyStatus::Absent)
    }

    pub fn is_invalid(&self) -> bool {
        matches!(self.status, PolicyStatus::Invalid { .. })
    }

    /// Where the policy stands against its expiry at `now`.
    pub fn expiry_state_at(&self, now: Timestamp) -> ExpiryState {
        match (&self.status, &self.expiry) {
            (PolicyStatus::Verified { .. }, Some(expiry)) => expiry.state_at(now),
            _ => ExpiryState::Current,
        }
    }

    /// Whether the strictest rules apply at `now`: the policy cannot be
    /// verified, or it expired with `on_expiry: strictest`.
    pub fn is_strictest_at(&self, now: Timestamp) -> bool {
        self.is_invalid() || self.expiry_state_at(now) == ExpiryState::ExpiredStrictest
    }

    pub fn is_strictest(&self) -> bool {
        self.is_strictest_at(Timestamp::now())
    }

    /// Whether a write is judged by broker addresses (`read_only_brokers`),
    /// so the brokers named by cluster metadata matter too. False when no
    /// policy is installed or it sets no such rule: nothing to fetch then.
    pub fn write_needs_broker_metadata(&self) -> bool {
        self.is_active() && !self.rules_at(Timestamp::now()).read_only_brokers.is_empty()
    }

    /// Whether an export is judged by broker addresses (`export_clusters`
    /// together with a non-allowed `export`).
    pub fn export_needs_broker_metadata(&self) -> bool {
        if !self.is_active() {
            return false;
        }
        let rules = self.rules_at(Timestamp::now());
        !rules.export.is_allowed() && !rules.export_clusters.is_empty()
    }

    /// The rules in force at `now`: the strictest ones once a policy with
    /// `on_expiry: strictest` has expired.
    pub fn rules_at(&self, now: Timestamp) -> std::borrow::Cow<'_, PolicyRules> {
        if self.expiry_state_at(now) == ExpiryState::ExpiredStrictest {
            std::borrow::Cow::Owned(PolicyRules::strictest())
        } else {
            std::borrow::Cow::Borrowed(&self.rules)
        }
    }

    /// A short note on the expiry for labels and badges (`expires in 3
    /// days`, `expired 2026-11-01T00:00:00Z`), or `None` when none is due.
    pub fn expiry_note_at(&self, now: Timestamp) -> Option<String> {
        let expiry = self.expiry.as_ref()?;
        match self.expiry_state_at(now) {
            ExpiryState::Current => None,
            ExpiryState::ExpiresSoon { days_left: 1 } => Some("expires in 1 day".to_string()),
            ExpiryState::ExpiresSoon { days_left } => Some(format!("expires in {days_left} days")),
            ExpiryState::ExpiredWarn => Some(format!("expired {}", expiry.text)),
            ExpiryState::ExpiredStrictest => {
                Some(format!("expired {}, strictest mode", expiry.text))
            }
        }
    }

    /// The expiry warning the CLI prints on stderr, if one is due at `now`.
    pub fn expiry_warning_at(&self, now: Timestamp) -> Option<String> {
        let expiry = self.expiry.as_ref()?;
        let name = self.name();
        match self.expiry_state_at(now) {
            ExpiryState::Current => None,
            ExpiryState::ExpiresSoon { .. } => Some(format!(
                "The {name} {} ({}). Ask the security team for a renewed policy.",
                self.expiry_note_at(now).unwrap_or_default(),
                expiry.text
            )),
            ExpiryState::ExpiredWarn => Some(format!(
                "The {name} expired {}; its rules still apply. Ask the security team for a renewed policy.",
                expiry.text
            )),
            ExpiryState::ExpiredStrictest => Some(format!(
                "The {name} expired {}; the strictest mode applies until a renewed policy is installed.",
                expiry.text
            )),
        }
    }

    /// Who the policy names, for messages; with the expiry note once one is
    /// due (`company policy by 보안팀, key …, expires in 3 days`).
    pub fn label(&self) -> String {
        self.label_at(Timestamp::now())
    }

    /// [`Self::label`] at `now`.
    pub fn label_at(&self, now: Timestamp) -> String {
        let name = self.name();
        match self.expiry_note_at(now) {
            Some(note) => format!("{name} ({note})"),
            None => name,
        }
    }

    /// The label without the expiry note.
    fn name(&self) -> String {
        match &self.status {
            PolicyStatus::Absent => "no policy".to_string(),
            PolicyStatus::Verified { issuer, key_id } if issuer.is_empty() => {
                format!("company policy, key {key_id}")
            }
            PolicyStatus::Verified { issuer, key_id } => {
                format!("company policy by {issuer}, key {key_id}")
            }
            PolicyStatus::Invalid { .. } => {
                "company policy (signature invalid, strictest mode)".to_string()
            }
        }
    }

    /// Refuses `action` against a profile / its brokers when the policy says so.
    pub fn check(
        &self,
        action: PolicyAction,
        profile: &str,
        bootstrap: &str,
    ) -> Result<(), PolicyRefusal> {
        self.check_at(Timestamp::now(), action, profile, bootstrap)
    }

    /// [`Self::check`] at `now`.
    pub fn check_at(
        &self,
        now: Timestamp,
        action: PolicyAction,
        profile: &str,
        bootstrap: &str,
    ) -> Result<(), PolicyRefusal> {
        if !self.is_active() {
            return Ok(());
        }
        if let PolicyStatus::Invalid { reason } = &self.status {
            return Err(PolicyRefusal(format!(
                "Refused by {}: `{}` is not allowed while the policy cannot be verified ({reason}).",
                self.label_at(now),
                action.id()
            )));
        }
        if self.expiry_state_at(now) == ExpiryState::ExpiredStrictest {
            return Err(PolicyRefusal(format!(
                "Refused by {}: `{}` is not allowed while the policy is expired.",
                self.label_at(now),
                action.id()
            )));
        }
        if self
            .rules
            .forbid
            .iter()
            .any(|f| f == "*" || f == action.id())
        {
            return Err(PolicyRefusal(format!(
                "Refused by {}: `{}` is forbidden.",
                self.label_at(now),
                action.id()
            )));
        }
        if let Some(ReadOnlyHit { pattern, target }) =
            self.read_only_hit_at(now, profile, bootstrap)
        {
            return Err(PolicyRefusal(format!(
                "Refused by {}: '{target}' is read-only (rule \"{pattern}\"), so `{}` is not allowed.",
                self.label_at(now),
                action.id()
            )));
        }
        Ok(())
    }

    /// The read-only glob that matches the profile name or one of its
    /// brokers (`read_only`), or one of its brokers (`read_only_brokers`).
    /// `bootstrap` is a comma-separated list of broker addresses.
    pub fn read_only_match(&self, profile: &str, bootstrap: &str) -> Option<String> {
        self.read_only_match_at(Timestamp::now(), profile, bootstrap)
    }

    /// [`Self::read_only_match`] at `now`.
    pub fn read_only_match_at(
        &self,
        now: Timestamp,
        profile: &str,
        bootstrap: &str,
    ) -> Option<String> {
        self.read_only_hit_at(now, profile, bootstrap)
            .map(|hit| hit.pattern)
    }

    /// The matching read-only rule and what it matched, for messages.
    fn read_only_hit_at(
        &self,
        now: Timestamp,
        profile: &str,
        bootstrap: &str,
    ) -> Option<ReadOnlyHit> {
        let rules = self.rules_at(now);
        let brokers = || broker_list(bootstrap);
        if let Some(pattern) = rules.read_only.iter().find(|pattern| {
            glob_match(pattern, profile) || brokers().any(|b| glob_match(pattern, b))
        }) {
            let target = if profile.is_empty() {
                bootstrap
            } else {
                profile
            };
            return Some(ReadOnlyHit {
                pattern: pattern.clone(),
                target: target.to_string(),
            });
        }
        // Broker addresses only: the profile name is the user's to choose.
        rules.read_only_brokers.iter().find_map(|pattern| {
            brokers()
                .find(|b| broker_glob_match(pattern, b))
                .map(|b| ReadOnlyHit {
                    pattern: pattern.clone(),
                    target: b.to_string(),
                })
        })
    }

    pub fn masking_enforced(&self) -> bool {
        self.masking_enforced_at(Timestamp::now())
    }

    pub fn masking_enforced_at(&self, now: Timestamp) -> bool {
        self.is_active() && self.rules_at(now).masking == MaskingRule::Enforced
    }

    /// How unmasking is handled when masking is enforced.
    pub fn unmask(&self) -> UnmaskRule {
        self.unmask_at(Timestamp::now())
    }

    pub fn unmask_at(&self, now: Timestamp) -> UnmaskRule {
        if self.masking_enforced_at(now) {
            self.rules_at(now).unmask
        } else {
            UnmaskRule::Allowed
        }
    }

    pub fn audit_required(&self) -> bool {
        self.audit_required_at(Timestamp::now())
    }

    pub fn audit_required_at(&self, now: Timestamp) -> bool {
        self.is_active()
            && self.rules_at(now).audit == ferrika_core::model::policy::AuditRule::Required
    }

    /// How exporting record data from the cluster at `bootstrap` (a
    /// comma-separated broker list) is handled now; see
    /// [`Self::export_rule_at`].
    pub fn export_rule(&self, bootstrap: &str) -> ExportRule {
        self.export_rule_at(Timestamp::now(), bootstrap)
    }

    /// The `export` rule at `now`, for the cluster at `bootstrap`: `allowed`
    /// when no policy is installed or `export_clusters` names none of its
    /// brokers. Like `read_only_brokers`, `export_clusters` matches broker
    /// addresses only, never the (user-chosen) profile name.
    pub fn export_rule_at(&self, now: Timestamp, bootstrap: &str) -> ExportRule {
        if !self.is_active() {
            return ExportRule::Allowed;
        }
        let rules = self.rules_at(now);
        if rules.export.is_allowed() {
            return ExportRule::Allowed;
        }
        let covered = rules.export_clusters.is_empty()
            || broker_list(bootstrap).any(|b| {
                rules
                    .export_clusters
                    .iter()
                    .any(|pattern| broker_glob_match(pattern, b))
            });
        if covered {
            rules.export
        } else {
            ExportRule::Allowed
        }
    }

    /// The watermark the GUI draws at `now`, if the rules turn it on.
    pub fn watermark_at(&self, now: Timestamp) -> Option<WatermarkSpec> {
        if !self.is_active() {
            return None;
        }
        let rules = self.rules_at(now);
        (rules.watermark == WatermarkRule::On).then(|| WatermarkSpec {
            text: rules.watermark_text.clone(),
        })
    }
}

impl EffectivePolicy {
    /// Whether the GUI keeps its window out of screen capture at `now`
    /// (`screen_capture: blocked`).
    pub fn screen_capture_blocked_at(&self, now: Timestamp) -> bool {
        self.is_active() && !self.rules_at(now).screen_capture.is_allowed()
    }

    /// What AI assistants may change through the MCP server at `now`
    /// (`mcp_write`); `allowed` without an active policy.
    pub fn mcp_write_at(&self, now: Timestamp) -> ferrika_core::model::policy::McpWriteRule {
        if self.is_active() {
            self.rules_at(now).mcp_write
        } else {
            ferrika_core::model::policy::McpWriteRule::Allowed
        }
    }
}

/// What the policy watermark shows besides user, host, time and cluster.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatermarkSpec {
    /// `watermark_text`, put in front.
    pub text: Option<String>,
}

/// The non-empty, trimmed entries of a comma-separated broker list.
fn broker_list(bootstrap: &str) -> impl Iterator<Item = &str> {
    bootstrap
        .split(',')
        .map(str::trim)
        .filter(|b| !b.is_empty())
}

/// A read-only rule that matched, and the profile or broker it matched.
struct ReadOnlyHit {
    pattern: String,
    target: String,
}

/// Kafka's port, assumed for a broker address that names none.
pub(crate) const DEFAULT_BROKER_PORT: &str = "9092";

/// A broker address split into lowercase host and port, the way
/// `read_only_brokers` compares them: surrounding spaces, a
/// `SASL_SSL://`-style scheme and a trailing `/` are dropped. IPv6 hosts are
/// kept in (or put into) brackets; a bare IPv6 address has no port. `None`
/// for an empty entry.
fn split_broker(address: &str) -> Option<(String, Option<String>)> {
    let mut text = address.trim();
    if let Some((_, rest)) = text.split_once("://") {
        text = rest;
    }
    let text = text.trim_end_matches('/').trim().to_lowercase();
    if text.is_empty() {
        return None;
    }
    // A fully qualified name may end in a dot (`kafka1.prod.corp.`): the
    // same host, so drop it before comparing.
    let strip_dot = |host: &str| host.trim_end_matches('.').to_string();
    let port = |p: &str| (!p.is_empty()).then(|| p.to_string());
    if let Some(rest) = text.strip_prefix('[') {
        return Some(match rest.split_once(']') {
            Some((host, after)) => (format!("[{host}]"), after.strip_prefix(':').and_then(port)),
            None => (text.clone(), None),
        });
    }
    Some(match text.matches(':').count() {
        0 => (strip_dot(&text), None),
        1 => match text.split_once(':') {
            Some((host, p)) => (strip_dot(host), port(p)),
            None => (strip_dot(&text), None),
        },
        _ => (format!("[{text}]"), None),
    })
}

/// `address` as `host:port` (see `split_broker`), with port 9092 when it
/// names none. `None` for an empty entry.
pub fn normalize_broker(address: &str) -> Option<String> {
    let (host, port) = split_broker(address)?;
    let port = port.unwrap_or_else(|| DEFAULT_BROKER_PORT.to_string());
    Some(format!("{host}:{port}"))
}

fn broker_set(list: &str) -> std::collections::HashSet<String> {
    broker_list(list).filter_map(normalize_broker).collect()
}

/// Whether two comma-separated broker lists share at least one broker
/// address once normalized (see [`normalize_broker`]). The safe test for
/// "might be the same cluster": `mirror` uses it to refuse reading its own
/// output, where a false "different" is the dangerous answer. Do NOT use it
/// to waive a restriction; see [`destination_within_source`]. The same
/// cluster reached by another name (IP vs DNS, another listener) counts as
/// a different one.
pub fn shares_broker(a: &str, b: &str) -> bool {
    let left = broker_set(a);
    broker_set(b).iter().any(|broker| left.contains(broker))
}

/// Whether every broker of `destination` is also a broker of `source` (and
/// `destination` names at least one): the only case where copying records
/// there stays inside the source cluster. The export gate waives itself on
/// this; one shared broker is not enough, or `--dst-bootstrap-servers
/// "prod1:9092,mine:9092"` would slip a copy out of `prod1`'s cluster.
pub fn destination_within_source(source: &str, destination: &str) -> bool {
    let src = broker_set(source);
    let dst = broker_set(destination);
    !dst.is_empty() && dst.iter().all(|broker| src.contains(broker))
}

/// `bootstrap` plus the brokers learned from cluster metadata, as one
/// comma-separated list for [`EffectivePolicy::check`].
pub fn with_known_brokers(bootstrap: &str, known: &[String]) -> String {
    std::iter::once(bootstrap)
        .chain(known.iter().map(String::as_str))
        .filter(|b| !b.trim().is_empty())
        .collect::<Vec<_>>()
        .join(",")
}

/// The broker list a rule is checked against: `bootstrap` alone when `needed`
/// is false (no request is made), otherwise `bootstrap` plus the brokers
/// `fetch` learns from cluster metadata. A failed fetch is an error, so the
/// caller refuses: a rule that cannot be checked is not waived.
pub(crate) fn resolve_brokers(
    bootstrap: &str,
    needed: bool,
    fetch: impl FnOnce() -> Result<Vec<String>, String>,
) -> Result<String, String> {
    if !needed {
        return Ok(bootstrap.to_string());
    }
    let known = fetch().map_err(|e| {
        format!(
            "Refused: the company policy matches broker addresses, and the cluster's brokers \
             could not be read to check them ({e})."
        )
    })?;
    Ok(with_known_brokers(bootstrap, &known))
}

/// A `read_only_brokers` glob against one broker address. Both are
/// normalized first. A pattern without a port matches the host on any port
/// (`*.prod.corp`); one with a port matches host and port (`10.0.0.*:9093`).
pub(crate) fn broker_glob_match(pattern: &str, address: &str) -> bool {
    let (Some((pattern_host, pattern_port)), Some((host, port))) =
        (split_broker(pattern), split_broker(address))
    else {
        return false;
    };
    if !glob_match(&pattern_host, &host) {
        return false;
    }
    match pattern_port {
        None => true,
        Some(pattern_port) => glob_match(
            &pattern_port,
            port.as_deref().unwrap_or(DEFAULT_BROKER_PORT),
        ),
    }
}

/// `*` matches any run of characters, `?` one character; case-insensitive.
pub fn glob_match(pattern: &str, text: &str) -> bool {
    let p: Vec<char> = pattern.to_lowercase().chars().collect();
    let t: Vec<char> = text.to_lowercase().chars().collect();
    let (mut pi, mut ti) = (0, 0);
    let (mut star, mut mark) = (None, 0);
    while ti < t.len() {
        if pi < p.len() && (p[pi] == '?' || p[pi] == t[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < p.len() && p[pi] == '*' {
            star = Some(pi);
            mark = ti;
            pi += 1;
        } else if let Some(s) = star {
            pi = s + 1;
            mark += 1;
            ti = mark;
        } else {
            return false;
        }
    }
    p[pi..].iter().all(|c| *c == '*')
}

/// Loads and verifies the policy in `dir` against the keys it trusts
/// (`policy.pub` and `policy.keys`).
pub fn load_from(dir: &Path) -> EffectivePolicy {
    let policy_path = dir.join(POLICY_FILE);
    let key_path = dir.join(PUBLIC_KEY_FILE);
    let presence = (|| -> Result<(bool, bool), PolicyError> {
        let has_policy = present(&policy_path)?;
        let has_key = present(&key_path)? || present(&dir.join(KEY_LIST_FILE))?;
        Ok((has_policy, has_key))
    })();
    // An unreadable policy directory is not "no policy": fail closed.
    let (has_policy, has_key) = match presence {
        Ok(found) => found,
        Err(e) => return EffectivePolicy::invalid(dir.to_path_buf(), e.to_string()),
    };
    if !has_policy && !has_key {
        return EffectivePolicy::absent(dir.to_path_buf());
    }
    let keys = trusted_keys(dir);
    let result = (|| {
        let keys = keys.as_ref().map_err(ToString::to_string)?;
        if keys.is_empty() {
            return Err(PolicyError::Missing(key_path.display().to_string()).to_string());
        }
        // A trusted key without a policy means the policy was removed.
        if !has_policy {
            return Err(PolicyError::Missing(policy_path.display().to_string()).to_string());
        }
        let text = read_text(&policy_path).map_err(|e| e.to_string())?;
        verify_with(&text, keys).map_err(|e| e.to_string())
    })();
    let trusted_keys = keys.unwrap_or_default();
    match result {
        Ok((payload, key_id)) => EffectivePolicy {
            trusted_keys,
            // `verify` has checked it parses.
            expiry: parse_expiry(&payload).ok().flatten(),
            status: PolicyStatus::Verified {
                issuer: payload.issuer,
                key_id,
            },
            rules: payload.rules,
            dir: dir.to_path_buf(),
            issued_at: payload.issued_at,
        },
        Err(e) => EffectivePolicy {
            trusted_keys,
            ..EffectivePolicy::invalid(dir.to_path_buf(), e)
        },
    }
}

static CURRENT: OnceLock<EffectivePolicy> = OnceLock::new();

/// The policy this process enforces, loaded once.
pub fn current() -> &'static EffectivePolicy {
    CURRENT.get_or_init(|| {
        let policy = load_from(&policy_dir());
        if let PolicyStatus::Invalid { reason } = &policy.status {
            tracing::warn!(%reason, "Company policy could not be verified; strictest mode");
        }
        // The CLI warns once per run; the GUI shows it in the badge, which
        // re-checks the time on every render.
        if let Some(warning) = policy.expiry_warning_at(Timestamp::now()) {
            ferrika_core::notice::warn(&warning);
        }
        policy
    })
}

/// Records an invalid policy as `policy.invalid`, and an ignored
/// [`POLICY_DIR_ENV`] as `policy.override_ignored` (`source` is `cli` or
/// `gui`). Called once per run of a command the policy governs, not for
/// `policy` and `audit` themselves.
pub fn record_if_invalid(source: &str) {
    let policy = current();
    let env = std::env::var_os(POLICY_DIR_ENV);
    if override_ignored(env.as_deref(), DIR_OVERRIDE_ALLOWED) {
        crate::audit::record(&crate::audit::AuditEntry {
            time_ms: crate::audit::now_ms(),
            source: source.to_string(),
            profile: String::new(),
            action: "policy.override_ignored".to_string(),
            target: env.unwrap_or_default().to_string_lossy().into_owned(),
            outcome: override_ignored_message(&policy.dir),
            ok: false,
            user: String::new(),
            actor: None,
            op: String::new(),
            op_digest: String::new(),
        });
    }
    if let PolicyStatus::Invalid { reason } = &policy.status {
        crate::audit::record(&crate::audit::AuditEntry {
            time_ms: crate::audit::now_ms(),
            source: source.to_string(),
            profile: String::new(),
            action: "policy.invalid".to_string(),
            target: policy.dir.join(POLICY_FILE).display().to_string(),
            outcome: reason.clone(),
            ok: false,
            user: String::new(),
            actor: None,
            op: String::new(),
            op_digest: String::new(),
        });
    }
}

/// Records a refused attempt as `policy.blocked`.
pub fn record_blocked(source: &str, profile: &str, attempted: &str, reason: &str) {
    crate::audit::record(&blocked_entry(source, profile, attempted, reason));
}

/// The `policy.blocked` entry for a refused attempt, for callers that write
/// it to their own [`AuditSink`].
pub fn blocked_entry(source: &str, profile: &str, attempted: &str, reason: &str) -> AuditEntry {
    AuditEntry {
        time_ms: crate::audit::now_ms(),
        source: source.to_string(),
        profile: profile.to_string(),
        action: "policy.blocked".to_string(),
        target: attempted.to_string(),
        outcome: reason.to_string(),
        ok: false,
        user: String::new(),
        actor: None,
        op: String::new(),
        op_digest: String::new(),
    }
}

/// The refusal when `policy` forbids one of the categories `action` also
/// needs ([`PolicyAction::also_requires`]) on the cluster `profile` /
/// `bootstrap`, naming that category; `None` when they are all allowed.
/// `label` is the change as the caller names it (the CLI's command, a GUI's
/// action name). Check it besides `action` itself, before the write gate,
/// so every front end refuses the same changes.
///
/// ```
/// use ferrika_core::model::policy::PolicyAction;
/// use ferrika_governance::policy::{extra_category_refusal, EffectivePolicy};
///
/// let policy = EffectivePolicy::absent(std::path::PathBuf::new());
/// let refusal = extra_category_refusal(
///     &policy,
///     PolicyAction::MessagesMove,
///     "rabbit dlq redrive --execute",
///     "",
///     "rabbit:15672",
/// );
/// assert_eq!(refusal, None);
/// ```
pub fn extra_category_refusal(
    policy: &EffectivePolicy,
    action: PolicyAction,
    label: &str,
    profile: &str,
    bootstrap: &str,
) -> Option<String> {
    action.also_requires().iter().find_map(|extra| {
        policy
            .check(*extra, profile, bootstrap)
            .err()
            .map(|reason| {
                let why = match action {
                    PolicyAction::MessagesMove => "republishes messages",
                    _ => "does the work of more than one category",
                };
                format!(
                    "{} `{label}` {why}, so it needs `{}` allowed as well as `{}`.",
                    &*reason,
                    extra.id(),
                    action.id()
                )
            })
    })
}

/// Where the write gate's audit entries go. [`AuditLog`] is the real log.
pub trait AuditSink {
    /// Appends one entry.
    fn record(&self, entry: &AuditEntry) -> std::io::Result<()>;
    /// Whether an entry could be appended, checked without writing one.
    fn probe(&self) -> std::io::Result<()>;
}

/// The audit log in the profile directory.
#[derive(Debug, Clone, Copy, Default)]
pub struct AuditLog;

impl AuditSink for AuditLog {
    fn record(&self, entry: &AuditEntry) -> std::io::Result<()> {
        crate::audit::try_record(entry)
    }

    fn probe(&self) -> std::io::Result<()> {
        appendable(&crate::audit::audit_path())
    }
}

/// The directory creation and append-open that each audit entry needs,
/// without writing a line.
fn appendable(path: &Path) -> std::io::Result<()> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map(drop)
}

/// How [`authorize_write`] satisfies auditing for a change it allows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuditMode {
    /// Record the request now, before the change runs (the CLI: it exits
    /// from many places, so its entries record the request).
    RecordRequest,
    /// Only check that the log can be appended to; the caller records the
    /// outcome after the change runs (the GUI).
    ProbeOnly,
}

/// One cluster change to authorise.
#[derive(Debug, Clone, Copy)]
pub struct WriteRequest<'a> {
    /// `cli`, `gui` or `mcp`.
    pub source: &'a str,
    pub action: PolicyAction,
    /// The action as the audit log names it, e.g. `topics delete` (CLI) or
    /// `Deleting topics` (GUI).
    pub label: &'a str,
    /// What it is done to, e.g. a topic name.
    pub target: &'a str,
    /// Profile name the policy's read-only globs match; may be empty.
    pub profile: &'a str,
    pub bootstrap: &'a str,
    /// The program acting for the user (an MCP client), recorded as the
    /// entries' `actor`.
    pub actor: Option<&'a str>,
    /// The history operation the change will be recorded as, written as
    /// the request entry's `op`; `None` for changes not recorded.
    pub op: Option<&'a str>,
}

impl WriteRequest<'_> {
    /// The profile, or the brokers when there is no profile name.
    fn audit_profile(&self) -> &str {
        if self.profile.is_empty() {
            self.bootstrap
        } else {
            self.profile
        }
    }

    fn attempted(&self) -> String {
        format!("{} {}", self.label, self.target).trim().to_string()
    }

    /// The `policy.blocked` entry refusing this request for `reason`.
    fn blocked(&self, reason: &str) -> AuditEntry {
        AuditEntry {
            actor: self.actor.map(str::to_string),
            ..blocked_entry(self.source, self.audit_profile(), &self.attempted(), reason)
        }
    }
}

/// Why [`authorize_write`] refused a change; `Display` is the message the
/// user sees.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteRefusal {
    /// The company policy forbids it (recorded as `policy.blocked`).
    Policy(String),
    /// Auditing is required and the audit log cannot be written.
    AuditUnavailable(String),
    /// The caller's own check refused it, e.g. a read-only profile.
    Profile(String),
}

impl std::fmt::Display for WriteRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Policy(m) | Self::AuditUnavailable(m) | Self::Profile(m) => f.write_str(m),
        }
    }
}

/// The write gate shared by the CLI and the GUI, against the current policy
/// and the real audit log; see [`authorize_write_with`].
pub fn authorize_write(
    request: &WriteRequest<'_>,
    mode: AuditMode,
    profile_check: impl FnOnce() -> Result<(), String>,
) -> Result<(), WriteRefusal> {
    authorize_write_with(current(), &AuditLog, request, mode, profile_check)
}

/// The write gate: the policy check (a refusal is recorded as
/// `policy.blocked`), then `profile_check` (the caller's own rule, e.g. a
/// read-only profile), then auditing per `mode`. Under `audit: required` an
/// allowed change is refused when the audit log cannot be written.
///
/// With [`AuditMode::RecordRequest`] the request entry records the
/// `profile_check` refusal too; with [`AuditMode::ProbeOnly`] that refusal is
/// returned unrecorded, for the caller to record with its outcome.
pub fn authorize_write_with(
    policy: &EffectivePolicy,
    sink: &impl AuditSink,
    request: &WriteRequest<'_>,
    mode: AuditMode,
    profile_check: impl FnOnce() -> Result<(), String>,
) -> Result<(), WriteRefusal> {
    authorize_write_at(policy, sink, request, mode, Timestamp::now(), profile_check)
}

/// [`authorize_write_with`] at `now` (the policy's expiry is judged then).
pub fn authorize_write_at(
    policy: &EffectivePolicy,
    sink: &impl AuditSink,
    request: &WriteRequest<'_>,
    mode: AuditMode,
    now: Timestamp,
    profile_check: impl FnOnce() -> Result<(), String>,
) -> Result<(), WriteRefusal> {
    if let Err(reason) = policy.check_at(now, request.action, request.profile, request.bootstrap) {
        record_or_warn(sink, &request.blocked(&reason));
        return Err(WriteRefusal::Policy(reason.into()));
    }
    let refused = profile_check().err();
    match mode {
        AuditMode::ProbeOnly => {
            if let Some(reason) = refused {
                return Err(WriteRefusal::Profile(reason));
            }
            if !policy.audit_required_at(now) {
                return Ok(());
            }
            let Err(error) = sink.probe() else {
                return Ok(());
            };
            let reason = format!(
                "Refused by company policy: the audit log cannot be written (audit: required). {error}"
            );
            record_or_warn(sink, &request.blocked(&reason));
            Err(WriteRefusal::AuditUnavailable(reason))
        }
        AuditMode::RecordRequest => {
            let entry = AuditEntry {
                time_ms: crate::audit::now_ms(),
                source: request.source.to_string(),
                profile: request.audit_profile().to_string(),
                action: request.label.to_string(),
                target: request.target.to_string(),
                outcome: refused.clone().unwrap_or_else(|| "requested".to_string()),
                ok: refused.is_none(),
                user: String::new(),
                actor: request.actor.map(str::to_string),
                op: request.op.unwrap_or_default().to_string(),
                op_digest: String::new(),
            };
            if let Err(e) = sink.record(&entry) {
                if policy.audit_required_at(now) && refused.is_none() {
                    return Err(WriteRefusal::AuditUnavailable(format!(
                        "Refused by {}: auditing is required, but the audit log could not be written ({e}).",
                        policy.label_at(now)
                    )));
                }
                tracing::warn!(%e, "Could not write the audit log");
            }
            match refused {
                Some(reason) => Err(WriteRefusal::Profile(reason)),
                None => Ok(()),
            }
        }
    }
}

fn record_or_warn(sink: &impl AuditSink, entry: &AuditEntry) {
    if let Err(e) = sink.record(entry) {
        tracing::warn!(%e, "Could not write the audit log");
    }
}

/// Where exported record data goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExportChannel {
    File,
    Clipboard,
    /// stdout redirected to a file or a pipe (a terminal is only viewing).
    Stdout,
    /// Another Kafka cluster (`mirror`).
    Cluster,
    /// An AI assistant over MCP.
    Mcp,
}

impl ExportChannel {
    /// The name in audit targets (`export:file dump orders`).
    pub fn id(self) -> &'static str {
        match self {
            Self::File => "file",
            Self::Clipboard => "clipboard",
            Self::Stdout => "stdout",
            Self::Cluster => "cluster",
            Self::Mcp => "mcp",
        }
    }
}

/// One export of record data (keys, payloads, header values) to authorise.
#[derive(Debug, Clone, Copy)]
pub struct ExportRequest<'a> {
    /// `cli`, `gui` or `mcp`.
    pub source: &'a str,
    pub channel: ExportChannel,
    /// The command or button, e.g. `dump` or `Export JSONL`.
    pub label: &'a str,
    /// What is exported: a topic, and a partition/offset where known. Never
    /// a file path, which can name a customer.
    pub target: &'a str,
    /// How many records, when known.
    pub records: Option<u64>,
    /// Profile name, for the audit log only (`export_clusters` matches
    /// brokers); may be empty.
    pub profile: &'a str,
    pub bootstrap: &'a str,
    /// Who asked besides the OS user (an MCP client), for the audit
    /// entries' `actor`.
    pub actor: Option<&'a str>,
}

impl ExportRequest<'_> {
    /// An audit entry about this export.
    fn entry(&self, action: &str, outcome: String, ok: bool) -> AuditEntry {
        AuditEntry {
            action: action.to_string(),
            outcome,
            ok,
            actor: self.actor.map(str::to_string),
            ..blocked_entry(self.source, self.audit_profile(), &self.attempted(), "")
        }
    }

    fn audit_profile(&self) -> &str {
        if self.profile.is_empty() {
            self.bootstrap
        } else {
            self.profile
        }
    }

    fn attempted(&self) -> String {
        format!(
            "export:{} {} {}",
            self.channel.id(),
            self.label,
            self.target
        )
        .trim()
        .to_string()
    }
}

/// The export gate shared by the CLI, the GUI and the MCP server, against
/// the current policy and the real audit log; see [`authorize_export_with`].
pub fn authorize_export(request: &ExportRequest<'_>) -> Result<(), WriteRefusal> {
    authorize_export_with(current(), &AuditLog, request)
}

/// The export gate. Under `export: forbidden` the export is refused and
/// recorded as `policy.blocked`. Under `export: audited` it is recorded as
/// `record.export` first; when that fails, `audit: required` refuses it and
/// otherwise it goes ahead with a warning. With no policy, `allowed`, or a
/// cluster outside `export_clusters`, nothing is recorded. Masking is a
/// separate rule: masked records are still record data.
pub fn authorize_export_with(
    policy: &EffectivePolicy,
    sink: &impl AuditSink,
    request: &ExportRequest<'_>,
) -> Result<(), WriteRefusal> {
    let now = Timestamp::now();
    match policy.export_rule_at(now, request.bootstrap) {
        ExportRule::Allowed => Ok(()),
        ExportRule::Forbidden => {
            let reason = format!(
                "Refused by {}: exporting record data is forbidden (export: forbidden), so `{}` is not allowed.",
                policy.label_at(now),
                request.label
            );
            record_or_warn(
                sink,
                &request.entry("policy.blocked", reason.clone(), false),
            );
            Err(WriteRefusal::Policy(reason))
        }
        ExportRule::Audited => {
            let outcome = match request.records {
                Some(n) => format!("records: {n}"),
                None => "requested".to_string(),
            };
            let entry = request.entry("record.export", outcome, true);
            match sink.record(&entry) {
                Ok(()) => Ok(()),
                Err(e) if policy.audit_required_at(now) => Err(WriteRefusal::AuditUnavailable(
                    format!(
                        "Refused by {}: exports are audited, but the audit log could not be written ({e}).",
                        policy.label_at(now)
                    ),
                )),
                Err(e) => {
                    tracing::warn!(%e, "Could not write the audit log");
                    Ok(())
                }
            }
        }
    }
}

/// The result line of an audited export: `record.export` with outcome
/// `records: N`, written after the export finished when the command knows
/// how many records it wrote (the request line, written before the export,
/// says `requested`). `None` when the export is not audited.
pub fn export_result_entry(
    policy: &EffectivePolicy,
    request: &ExportRequest<'_>,
    records: u64,
) -> Option<AuditEntry> {
    if policy.export_rule(request.bootstrap) != ExportRule::Audited {
        return None;
    }
    Some(request.entry("record.export", format!("records: {records}"), true))
}

/// The broker list a policy rule is checked against: the bootstrap servers
/// of `config`, plus (only when `needed`) the brokers its cluster reports.
/// Fails when they are needed and cannot be read, so the caller refuses.
pub fn brokers_for(config: &KafkaClientConfig, needed: bool) -> Result<String, String> {
    brokers_for_with(config, needed, ferrika_core::kafka::admin::known_brokers)
}

/// [`brokers_for`] with the metadata lookup given.
pub(crate) fn brokers_for_with(
    config: &KafkaClientConfig,
    needed: bool,
    fetch: impl FnOnce(&KafkaClientConfig) -> Result<Vec<String>, String>,
) -> Result<String, String> {
    resolve_brokers(&config.bootstrap_servers, needed, || fetch(config))
}

/// The broker list an export rule is checked against (see [`brokers_for`]):
/// the cluster's own brokers are read only when the policy has `export_clusters`.
pub fn export_brokers(
    policy: &EffectivePolicy,
    config: &KafkaClientConfig,
) -> Result<String, String> {
    export_brokers_with(policy, config, ferrika_core::kafka::admin::known_brokers)
}

/// [`export_brokers`] with the metadata lookup given.
pub(crate) fn export_brokers_with(
    policy: &EffectivePolicy,
    config: &KafkaClientConfig,
    fetch: impl FnOnce(&KafkaClientConfig) -> Result<Vec<String>, String>,
) -> Result<String, String> {
    brokers_for_with(config, policy.export_needs_broker_metadata(), fetch)
}

/// Whether exports are forbidden for `config`'s cluster. A rule that cannot be
/// checked (the broker lookup failed) counts as forbidden.
pub fn export_forbidden_with(
    policy: &EffectivePolicy,
    config: &KafkaClientConfig,
    fetch: impl FnOnce(&KafkaClientConfig) -> Result<Vec<String>, String>,
) -> bool {
    match export_brokers_with(policy, config, fetch) {
        Ok(brokers) => policy.export_rule(&brokers) == ExportRule::Forbidden,
        Err(_) => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrika_core::kafka::admin::broker_address;

    const PAYLOAD: &str = r#"{"issuer":"보안팀","issued_at":"2026-11-02","rules":{"read_only":["prod-*"],"forbid":["topics.delete","topics.purge"],"masking":"enforced","unmask":"with_reason","audit":"required"}}"#;

    #[test]
    fn a_move_needs_produce_as_well() {
        let action = PolicyAction::MessagesMove;
        let label = "rabbit dlq redrive --execute";
        let address = "rabbit.prod:15672,rabbit.prod:5672";
        let mut policy = crate::test_policy::masking();
        assert_eq!(
            extra_category_refusal(&policy, action, label, "", address),
            None
        );
        policy.rules.forbid = vec!["produce".to_string()];
        let refusal = extra_category_refusal(&policy, action, label, "", address).unwrap();
        assert!(refusal.contains("`produce`"), "{refusal}");
        assert!(refusal.contains("messages.move"), "{refusal}");
        assert!(refusal.contains(label), "{refusal}");
        // Other changes are judged by their own category only.
        assert_eq!(
            extra_category_refusal(&policy, PolicyAction::MessagesPeek, "peek", "", address),
            None
        );
    }

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "ferrika-policy-{name}-{}-{}",
            std::process::id(),
            crate::audit::now_ms()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn signed_text(pkcs8: &[u8], payload: &str) -> String {
        serde_json::to_string_pretty(&sign(pkcs8, payload).unwrap()).unwrap()
    }

    #[test]
    fn sign_then_verify_round_trips() {
        let (private, public) = generate_keypair().unwrap();
        let text = signed_text(&private, PAYLOAD);
        let payload = verify(&text, &public).unwrap();
        assert_eq!(payload.issuer, "보안팀");
        assert_eq!(payload.rules.masking, MaskingRule::Enforced);
        assert_eq!(payload.rules.unmask, UnmaskRule::WithReason);
    }

    #[test]
    fn tampered_payload_fails() {
        let (private, public) = generate_keypair().unwrap();
        let text = signed_text(&private, PAYLOAD);
        let tampered = text.replace("topics.delete", "topics.create");
        assert!(matches!(
            verify(&tampered, &public),
            Err(PolicyError::BadSignature)
        ));
        // Even a one-character change in the issuer.
        let tampered = text.replace("보안팀", "보안틤");
        assert!(matches!(
            verify(&tampered, &public),
            Err(PolicyError::BadSignature)
        ));
    }

    #[test]
    fn wrong_key_fails() {
        let (private, _) = generate_keypair().unwrap();
        let (_, other_public) = generate_keypair().unwrap();
        let text = signed_text(&private, PAYLOAD);
        assert!(matches!(
            verify(&text, &other_public),
            Err(PolicyError::KeyMismatch { .. })
        ));
        // Same key id claimed, different key: the signature check catches it.
        let mut signed: SignedPolicy = serde_json::from_str(&text).unwrap();
        signed.key_id = key_id(&other_public);
        let forged = serde_json::to_string(&signed).unwrap();
        assert!(matches!(
            verify(&forged, &other_public),
            Err(PolicyError::BadSignature)
        ));
    }

    #[test]
    fn signing_refuses_unknown_rules() {
        let (private, _) = generate_keypair().unwrap();
        assert!(sign(&private, r#"{"rules":{"forbid":["topics.nuke"]}}"#).is_err());
        assert!(sign(&private, r#"{"rules":{"readonly":["*"]}}"#).is_err());
    }

    #[test]
    fn no_files_means_no_policy() {
        let dir = temp_dir("absent");
        let policy = load_from(&dir);
        assert_eq!(policy.status, PolicyStatus::Absent);
        assert!(policy
            .check(PolicyAction::TopicsDelete, "prod-a", "b:9092")
            .is_ok());
        assert!(!policy.masking_enforced());
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn valid_policy_loads_and_tampering_fails_closed() {
        let dir = temp_dir("load");
        let (private, public) = generate_keypair().unwrap();
        std::fs::write(dir.join(PUBLIC_KEY_FILE), to_hex(&public)).unwrap();
        let text = signed_text(&private, PAYLOAD);
        std::fs::write(dir.join(POLICY_FILE), &text).unwrap();
        let policy = load_from(&dir);
        assert!(
            matches!(&policy.status, PolicyStatus::Verified { issuer, .. } if issuer == "보안팀")
        );
        assert!(policy
            .check(PolicyAction::Produce, "dev-local", "localhost:9092")
            .is_ok());

        std::fs::write(dir.join(POLICY_FILE), text.replace("prod-*", "prod-x")).unwrap();
        let policy = load_from(&dir);
        assert!(policy.is_invalid());
        assert_eq!(policy.rules, PolicyRules::strictest());
        assert!(policy.masking_enforced());
        assert_eq!(policy.unmask(), UnmaskRule::Forbidden);
        let err = policy
            .check(PolicyAction::Produce, "dev-local", "localhost:9092")
            .unwrap_err();
        assert!(err.contains("cannot be verified"), "{err}");

        // Unparsable, and policy removed while the key stays.
        std::fs::write(dir.join(POLICY_FILE), "not json").unwrap();
        assert!(load_from(&dir).is_invalid());
        std::fs::remove_file(dir.join(POLICY_FILE)).unwrap();
        assert!(load_from(&dir).is_invalid());
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn forbid_and_read_only_are_enforced() {
        let payload = parse_payload(PAYLOAD).unwrap();
        let policy = EffectivePolicy {
            status: PolicyStatus::Verified {
                issuer: payload.issuer,
                key_id: "abcd".into(),
            },
            rules: payload.rules,
            dir: PathBuf::new(),
            issued_at: String::new(),
            expiry: None,
            trusted_keys: Vec::new(),
        };
        let err = policy
            .check(PolicyAction::TopicsDelete, "dev", "localhost:9092")
            .unwrap_err();
        assert!(
            err.contains("보안팀") && err.contains("topics.delete"),
            "{err}"
        );
        assert!(policy
            .check(PolicyAction::TopicsPurge, "dev", "localhost:9092")
            .is_err());
        assert!(policy
            .check(PolicyAction::Produce, "dev", "localhost:9092")
            .is_ok());
        // Read-only by profile name.
        let err = policy
            .check(PolicyAction::Produce, "prod-payments", "10.0.0.1:9092")
            .unwrap_err();
        assert!(err.contains("read-only"), "{err}");
        assert!(policy
            .check(PolicyAction::Produce, "PROD-orders", "10.0.0.1:9092")
            .is_err());

        // Read-only by broker address.
        let mut by_broker = policy.clone();
        by_broker.rules.read_only = vec!["*.prod.corp:*".into()];
        assert!(by_broker
            .check(
                PolicyAction::Produce,
                "x",
                "localhost:9092, kafka1.prod.corp:9093"
            )
            .is_err());
        assert!(by_broker
            .check(PolicyAction::Produce, "x", "localhost:9092")
            .is_ok());

        let mut everything = policy.clone();
        everything.rules.forbid = vec!["*".into()];
        assert!(everything
            .check(PolicyAction::AclChange, "dev", "localhost:9092")
            .is_err());
    }

    #[test]
    fn broker_addresses_are_normalized() {
        let cases = [
            ("kafka1.prod.corp:9093", Some("kafka1.prod.corp:9093")),
            ("  Kafka1.PROD.corp  ", Some("kafka1.prod.corp:9092")),
            (
                "SASL_SSL://Kafka1.prod.corp:9094",
                Some("kafka1.prod.corp:9094"),
            ),
            ("plaintext://10.0.0.5", Some("10.0.0.5:9092")),
            ("10.0.0.5:", Some("10.0.0.5:9092")),
            ("kafka1.prod.corp:9093/", Some("kafka1.prod.corp:9093")),
            ("[::1]:9093", Some("[::1]:9093")),
            ("[FD00::5]", Some("[fd00::5]:9092")),
            ("fd00::5", Some("[fd00::5]:9092")),
            ("", None),
            ("   ", None),
            ("SSL://", None),
            ("kafka1.prod.corp.:9092", Some("kafka1.prod.corp:9092")),
            ("Kafka1.Prod.Corp.", Some("kafka1.prod.corp:9092")),
        ];
        for (input, expected) in cases {
            assert_eq!(normalize_broker(input).as_deref(), expected, "{input:?}");
        }
        assert_eq!(broker_address("::1", 9092), "[::1]:9092");
        assert_eq!(broker_address("b1.corp", 9093), "b1.corp:9093");
        assert_eq!(
            with_known_brokers("prod.corp:9092", &["10.0.0.5:9092".into()]),
            "prod.corp:9092,10.0.0.5:9092"
        );
        assert!(shares_broker(
            "kafka1.prod.corp:9092,kafka2.prod.corp:9092",
            "SASL_SSL://KAFKA2.prod.corp"
        ));
        assert!(!shares_broker(
            "kafka1.prod.corp:9092",
            "kafka1.prod.corp:9093"
        ));
        assert!(!shares_broker("10.0.0.5:9092", "kafka1.prod.corp:9092"));
        assert!(!shares_broker("", ""));
        assert!(!destination_within_source("", ""));
        assert!(!destination_within_source("a:9092", ""));
        assert_eq!(with_known_brokers("a:1", &[]), "a:1");
        assert_eq!(with_known_brokers("", &["b:2".into()]), "b:2");
    }

    #[test]
    fn partial_overlap_is_the_same_cluster_for_self_mirror_only() {
        let src = "prod1:9092";
        let dst = "prod1:9092,mine:9092";
        // self-mirror: one shared broker means "might be the same cluster".
        assert!(shares_broker(src, dst));
        // export gate: not contained in the source, so it is a copy out.
        assert!(!destination_within_source(src, dst));
        // Contained (any order, any spelling) stays inside the source.
        assert!(destination_within_source(
            "prod1:9092,prod2:9092",
            "PROD2,plaintext://prod1:9092"
        ));
        assert!(!destination_within_source("prod1:9092", "prod1:9093"));
    }

    #[test]
    fn broker_metadata_is_fetched_only_when_a_rule_needs_it() {
        use std::cell::Cell;
        let calls = Cell::new(0);
        let fetch = || {
            calls.set(calls.get() + 1);
            Ok(vec!["kafka1.prod.corp:9092".to_string()])
        };
        // No rule that needs it: no request, the bootstrap list unchanged.
        assert_eq!(
            resolve_brokers("10.0.5.7:9092", false, fetch).unwrap(),
            "10.0.5.7:9092"
        );
        assert_eq!(calls.get(), 0);
        let none = EffectivePolicy::absent(PathBuf::new());
        assert!(!none.write_needs_broker_metadata());
        assert!(!none.export_needs_broker_metadata());
        // With the rule: the learned broker is added and the glob matches.
        let merged = resolve_brokers("10.0.5.7:9092", true, fetch).unwrap();
        assert_eq!(calls.get(), 1);
        assert!(merged.contains("kafka1.prod.corp:9092"));
        assert!(broker_glob_match(
            "*.prod.corp",
            merged.split(',').nth(1).unwrap()
        ));
        // A fetch that fails refuses instead of waiving the rule.
        let err = resolve_brokers("10.0.5.7:9092", true, || Err("timed out".into())).unwrap_err();
        assert!(
            err.contains("could not be read") && err.contains("timed out"),
            "{err}"
        );
    }

    #[test]
    fn trailing_dot_fqdn_matches_globs() {
        assert!(broker_glob_match("*.prod.corp", "kafka1.prod.corp.:9092"));
        assert!(broker_glob_match(
            "kafka1.prod.corp:9092",
            "kafka1.prod.corp.:9092"
        ));
    }

    #[cfg(unix)]
    #[test]
    fn unreadable_policy_dir_fails_closed() {
        use std::os::unix::fs::PermissionsExt;
        let dir = temp_dir("eacces");
        std::fs::write(dir.join(POLICY_FILE), "{}").unwrap();
        std::fs::write(dir.join(PUBLIC_KEY_FILE), "00").unwrap();
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o000)).unwrap();
        let policy = load_from(&dir);
        let keys = trusted_keys(&dir);
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
        assert!(matches!(policy.status, PolicyStatus::Invalid { .. }));
        assert!(policy.is_active());
        assert!(keys.is_err());
        // A missing directory is still just "no policy".
        let _ = std::fs::remove_dir_all(&dir);
        assert!(matches!(load_from(&dir).status, PolicyStatus::Absent));
    }

    #[test]
    fn broker_globs_match_host_and_optional_port() {
        // No port in the pattern: any port.
        assert!(broker_glob_match("*.prod.corp", "kafka1.prod.corp:9093"));
        assert!(broker_glob_match(
            "*.prod.corp",
            "SASL_SSL://KAFKA1.prod.corp"
        ));
        assert!(!broker_glob_match("*.prod.corp", "kafka1.dev.corp:9092"));
        // A port in the pattern; a missing port in the address is 9092.
        assert!(broker_glob_match("10.0.0.*:9092", "10.0.0.7"));
        assert!(!broker_glob_match("10.0.0.*:9093", "10.0.0.7"));
        assert!(broker_glob_match("10.0.0.*:*", "10.0.0.7:19092"));
        assert!(broker_glob_match(
            "sasl_ssl://kafka1.prod.corp",
            "kafka1.prod.corp:1"
        ));
        assert!(broker_glob_match("[fd00::*]", "[fd00::5]:9092"));
        assert!(broker_glob_match("*", "anything:1"));
        // The profile-name-looking text never matches as a host by accident.
        assert!(!broker_glob_match("prod-*", "localhost:9092"));
        assert!(!broker_glob_match("", "localhost:9092"));
        assert!(!broker_glob_match("*", ""));
    }

    #[test]
    fn read_only_brokers_ignore_the_profile_name() {
        let policy = verified(PolicyRules {
            read_only_brokers: vec!["*.prod.corp".into(), "10.20.0.*".into()],
            ..PolicyRules::default()
        });
        // A prod cluster renamed to "dev" stays read-only.
        let err = policy
            .check(
                PolicyAction::Produce,
                "dev",
                "localhost:9092, SASL_SSL://Kafka1.PROD.corp:9093",
            )
            .unwrap_err();
        assert!(
            err.contains("'SASL_SSL://Kafka1.PROD.corp:9093' is read-only (rule \"*.prod.corp\")"),
            "{err}"
        );
        assert!(policy
            .check(PolicyAction::TopicsCreate, "anything", "10.20.0.4")
            .is_err());
        assert_eq!(
            policy.read_only_match("dev", "10.20.0.4:9092").as_deref(),
            Some("10.20.0.*")
        );
        // A profile named like the rule is not read-only by name.
        assert!(policy
            .check(PolicyAction::Produce, "kafka1.prod.corp", "localhost:9092")
            .is_ok());
        assert_eq!(policy.read_only_match("x.prod.corp", "localhost"), None);

        // The write gate refuses and records it.
        let log = FakeLog::new(true);
        let req = WriteRequest {
            bootstrap: "kafka2.prod.corp",
            actor: None,
            ..request(PolicyAction::Produce, "dev")
        };
        let result = authorize_write_with(&policy, &log, &req, AuditMode::ProbeOnly, || Ok(()));
        let Err(WriteRefusal::Policy(reason)) = result else {
            panic!("expected a policy refusal, got {result:?}");
        };
        assert!(
            reason.contains("'kafka2.prod.corp' is read-only"),
            "{reason}"
        );
        assert_eq!(log.entries.borrow()[0].profile, "dev");

        // An IP alias in the profile is caught by the brokers' advertised
        // addresses learned from metadata.
        let learned = with_known_brokers("10.9.9.9:9092", &["kafka1.prod.corp:9092".into()]);
        assert!(policy
            .check(PolicyAction::Produce, "dev", "10.9.9.9:9092")
            .is_ok());
        assert!(policy
            .check(PolicyAction::Produce, "dev", &learned)
            .is_err());
    }

    #[test]
    fn v1_policies_sign_byte_for_byte_as_before() {
        let (private, public) = generate_keypair().unwrap();
        let signed = sign(&private, PAYLOAD).unwrap();
        assert_eq!(signed.format, POLICY_FORMAT_V1);
        assert_eq!(signed.payload, PAYLOAD);
        // The signature is plain Ed25519 over the payload bytes, as before.
        let pair = Ed25519KeyPair::from_pkcs8(&private).unwrap();
        assert_eq!(
            signed.signature,
            to_hex(pair.sign(PAYLOAD.as_bytes()).as_ref())
        );
        // The parsed rules serialize without the new field.
        let payload = verify(&serde_json::to_string(&signed).unwrap(), &public).unwrap();
        assert!(payload.rules.read_only_brokers.is_empty());
        let rules = serde_json::to_string(&payload.rules).unwrap();
        assert!(!rules.contains("read_only_brokers"), "{rules}");

        // With read_only_brokers the file is format 2, and refused as format 1.
        let v2 = PAYLOAD.replacen(
            "\"read_only\"",
            "\"read_only_brokers\":[\"*.prod.corp\"],\"read_only\"",
            1,
        );
        let mut signed = sign(&private, &v2).unwrap();
        assert_eq!(signed.format, POLICY_FORMAT_V2);
        let payload = verify(&serde_json::to_string(&signed).unwrap(), &public).unwrap();
        assert_eq!(
            payload.rules.read_only_brokers,
            vec!["*.prod.corp".to_string()]
        );
        signed.format = POLICY_FORMAT_V1;
        let err = verify(&serde_json::to_string(&signed).unwrap(), &public).unwrap_err();
        let err = err.to_string();
        assert!(
            err.contains("read_only_brokers") && err.contains("need format 2"),
            "{err}"
        );
    }

    #[test]
    fn export_and_watermark_rules_sign_as_format_2() {
        let (private, public) = generate_keypair().unwrap();
        // A v1 policy still serializes without the new rules.
        let payload = verify(&signed_text(&private, PAYLOAD), &public).unwrap();
        let rules = serde_json::to_string(&payload.rules).unwrap();
        assert!(
            !rules.contains("export") && !rules.contains("watermark"),
            "{rules}"
        );

        let v2 = PAYLOAD.replacen(
            "\"masking\"",
            "\"export\":\"forbidden\",\"watermark\":\"on\",\"watermark_text\":\"대외비\",\"masking\"",
            1,
        );
        let mut signed = sign(&private, &v2).unwrap();
        assert_eq!(signed.format, POLICY_FORMAT_V2);
        let payload = verify(&serde_json::to_string(&signed).unwrap(), &public).unwrap();
        assert_eq!(payload.rules.export, ExportRule::Forbidden);
        assert_eq!(payload.rules.watermark, WatermarkRule::On);
        signed.format = POLICY_FORMAT_V1;
        let err = verify(&serde_json::to_string(&signed).unwrap(), &public).unwrap_err();
        let err = err.to_string();
        assert!(
            err.contains("export") && err.contains("need format 2"),
            "{err}"
        );

        // Nonsense values are refused when signing and when verifying.
        for bad in [
            r#"{"rules":{"export":"forbiden"}}"#,
            r#"{"rules":{"export_clusters":[""]}}"#,
            r#"{"rules":{"watermark_text":"이 문구는 사십 자를 훨씬 넘겨서 워터마크로 쓰기에는 너무 길기 때문에 서명이 거부되어야 한다"}}"#,
        ] {
            assert!(sign(&private, bad).is_err(), "{bad}");
        }
        let payload = r#"{"rules":{"watermark_text":"a\u0007b"}}"#;
        let pair = Ed25519KeyPair::from_pkcs8(&private).unwrap();
        let forged = SignedPolicy {
            format: 2,
            key_id: key_id(&public),
            signature: to_hex(pair.sign(payload.as_bytes()).as_ref()),
            payload: payload.to_string(),
        };
        assert!(matches!(
            verify(&serde_json::to_string(&forged).unwrap(), &public),
            Err(PolicyError::Rules(_))
        ));
    }

    #[test]
    fn glob_matching() {
        assert!(glob_match("*", "anything"));
        assert!(glob_match("prod-*", "prod-eu"));
        assert!(!glob_match("prod-*", "staging"));
        assert!(glob_match("kafka?.corp:*", "kafka1.corp:9092"));
        assert!(!glob_match("kafka?.corp", "kafka12.corp"));
        assert!(glob_match("*prod*", "eu-prod-1"));
    }

    /// An audit log in memory; `writable: false` fails every write and probe.
    struct FakeLog {
        writable: bool,
        entries: std::cell::RefCell<Vec<AuditEntry>>,
        probes: std::cell::Cell<usize>,
    }

    impl FakeLog {
        fn new(writable: bool) -> Self {
            Self {
                writable,
                entries: Default::default(),
                probes: Default::default(),
            }
        }

        fn fail(&self) -> std::io::Result<()> {
            if self.writable {
                Ok(())
            } else {
                Err(std::io::Error::other("read-only folder"))
            }
        }
    }

    impl AuditSink for FakeLog {
        fn record(&self, entry: &AuditEntry) -> std::io::Result<()> {
            self.fail()?;
            self.entries.borrow_mut().push(entry.clone());
            Ok(())
        }

        fn probe(&self) -> std::io::Result<()> {
            self.probes.set(self.probes.get() + 1);
            self.fail()
        }
    }

    fn verified(rules: PolicyRules) -> EffectivePolicy {
        EffectivePolicy {
            status: PolicyStatus::Verified {
                issuer: "보안팀".into(),
                key_id: "abcd".into(),
            },
            rules,
            dir: PathBuf::new(),
            issued_at: String::new(),
            expiry: None,
            trusted_keys: Vec::new(),
        }
    }

    fn request(action: PolicyAction, profile: &'static str) -> WriteRequest<'static> {
        WriteRequest {
            source: "cli",
            action,
            label: "topics delete",
            target: "orders",
            profile,
            bootstrap: "localhost:9092",
            actor: None,
            op: None,
        }
    }

    const BOTH: [AuditMode; 2] = [AuditMode::RecordRequest, AuditMode::ProbeOnly];

    #[test]
    fn authorize_write_records_the_actor() {
        let policy = verified(PolicyRules {
            forbid: vec!["produce".into()],
            ..Default::default()
        });
        let req = WriteRequest {
            source: "mcp",
            actor: Some("mcp:t/1#00"),
            ..request(PolicyAction::Produce, "dev")
        };
        let log = FakeLog::new(true);
        let result = authorize_write_with(&policy, &log, &req, AuditMode::ProbeOnly, || Ok(()));
        assert!(matches!(result, Err(WriteRefusal::Policy(_))));
        let entries = log.entries.borrow();
        assert_eq!(entries[0].action, "policy.blocked");
        assert_eq!(entries[0].source, "mcp");
        assert_eq!(entries[0].actor.as_deref(), Some("mcp:t/1#00"));

        let policy = verified(PolicyRules::default());
        let log = FakeLog::new(true);
        let req = WriteRequest {
            action: PolicyAction::TopicsDelete,
            ..req
        };
        authorize_write_with(&policy, &log, &req, AuditMode::RecordRequest, || Ok(())).unwrap();
        assert_eq!(log.entries.borrow()[0].actor.as_deref(), Some("mcp:t/1#00"));
    }

    #[test]
    fn the_request_entry_names_the_operation() {
        let policy = verified(PolicyRules::default());
        let log = FakeLog::new(true);
        let req = WriteRequest {
            op: Some("01JBX3K8Q2M7"),
            ..request(PolicyAction::TopicsDelete, "dev")
        };
        authorize_write_with(&policy, &log, &req, AuditMode::RecordRequest, || Ok(())).unwrap();
        let entries = log.entries.borrow();
        assert_eq!(entries[0].outcome, "requested");
        assert_eq!(entries[0].op, "01JBX3K8Q2M7");
        assert!(entries[0].op_digest.is_empty(), "known only at the end");
    }

    #[test]
    fn authorize_write_allows_and_audits_per_mode() {
        let policy = verified(PolicyRules::default());
        let req = request(PolicyAction::TopicsDelete, "dev");

        let log = FakeLog::new(true);
        let result = authorize_write_with(&policy, &log, &req, AuditMode::RecordRequest, || Ok(()));
        assert_eq!(result, Ok(()));
        let entries = log.entries.borrow();
        assert_eq!(entries.len(), 1);
        assert_eq!(
            (
                entries[0].source.as_str(),
                entries[0].profile.as_str(),
                entries[0].action.as_str(),
                entries[0].target.as_str(),
                entries[0].outcome.as_str(),
                entries[0].ok
            ),
            ("cli", "dev", "topics delete", "orders", "requested", true)
        );

        // The GUI records the outcome itself; audit is optional, so no probe.
        let log = FakeLog::new(true);
        let result = authorize_write_with(&policy, &log, &req, AuditMode::ProbeOnly, || Ok(()));
        assert_eq!(result, Ok(()));
        assert!(log.entries.borrow().is_empty());
        assert_eq!(log.probes.get(), 0);

        // Without a policy an unwritable log never blocks a change.
        let none = EffectivePolicy::absent(PathBuf::new());
        for mode in BOTH {
            let log = FakeLog::new(false);
            assert_eq!(
                authorize_write_with(&none, &log, &req, mode, || Ok(())),
                Ok(())
            );
        }
    }

    #[test]
    fn authorize_write_refuses_forbidden_actions_and_records_them() {
        let policy = verified(parse_payload(PAYLOAD).unwrap().rules);
        for mode in BOTH {
            let log = FakeLog::new(true);
            let checked = std::cell::Cell::new(false);
            let result = authorize_write_with(
                &policy,
                &log,
                &request(PolicyAction::TopicsDelete, ""),
                mode,
                || {
                    checked.set(true);
                    Ok(())
                },
            );
            let Err(WriteRefusal::Policy(reason)) = result else {
                panic!("expected a policy refusal, got {result:?}");
            };
            assert_eq!(
                reason,
                "Refused by company policy by 보안팀, key abcd: `topics.delete` is forbidden."
            );
            assert!(
                !checked.get(),
                "the profile check runs only after the policy"
            );
            let entries = log.entries.borrow();
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].action, "policy.blocked");
            assert_eq!(entries[0].target, "topics delete orders");
            // No profile name: the brokers stand in for it.
            assert_eq!(entries[0].profile, "localhost:9092");
            assert_eq!(entries[0].outcome, reason);
            assert!(!entries[0].ok);
        }
    }

    #[test]
    fn authorize_write_refuses_read_only_targets() {
        let policy = verified(parse_payload(PAYLOAD).unwrap().rules);
        let log = FakeLog::new(true);
        let result = authorize_write_with(
            &policy,
            &log,
            &request(PolicyAction::Produce, "prod-payments"),
            AuditMode::RecordRequest,
            || Ok(()),
        );
        let Err(WriteRefusal::Policy(reason)) = result else {
            panic!("expected a policy refusal, got {result:?}");
        };
        assert!(reason.contains("'prod-payments' is read-only"), "{reason}");
        assert_eq!(log.entries.borrow()[0].profile, "prod-payments");

        // A read-only profile (the caller's check) is recorded in the request
        // entry and is not turned into an audit refusal.
        let read_only = || Err("Profile 'dev' is read-only".to_string());
        let log = FakeLog::new(true);
        let req = request(PolicyAction::Produce, "dev");
        let result = authorize_write_with(&policy, &log, &req, AuditMode::RecordRequest, read_only);
        assert_eq!(
            result,
            Err(WriteRefusal::Profile("Profile 'dev' is read-only".into()))
        );
        let entries = log.entries.borrow();
        assert_eq!(entries[0].outcome, "Profile 'dev' is read-only");
        assert!(!entries[0].ok);

        let log = FakeLog::new(false);
        let result = authorize_write_with(&policy, &log, &req, AuditMode::RecordRequest, read_only);
        assert!(
            matches!(result, Err(WriteRefusal::Profile(_))),
            "{result:?}"
        );
    }

    #[test]
    fn authorize_write_refuses_when_required_audit_cannot_be_written() {
        let policy = verified(PolicyRules {
            audit: ferrika_core::model::policy::AuditRule::Required,
            ..PolicyRules::default()
        });
        let req = request(PolicyAction::Produce, "dev");

        // CLI: writing the request entry fails.
        let log = FakeLog::new(false);
        let result = authorize_write_with(&policy, &log, &req, AuditMode::RecordRequest, || Ok(()));
        assert_eq!(
            result,
            Err(WriteRefusal::AuditUnavailable(
                "Refused by company policy by 보안팀, key abcd: auditing is required, \
                 but the audit log could not be written (read-only folder)."
                    .into()
            ))
        );

        // GUI: the probe fails before the change runs.
        let log = FakeLog::new(false);
        let result = authorize_write_with(&policy, &log, &req, AuditMode::ProbeOnly, || Ok(()));
        assert_eq!(
            result,
            Err(WriteRefusal::AuditUnavailable(
                "Refused by company policy: the audit log cannot be written \
                 (audit: required). read-only folder"
                    .into()
            ))
        );
        assert_eq!(log.probes.get(), 1);

        // A writable log passes; the probe writes nothing.
        let log = FakeLog::new(true);
        assert_eq!(
            authorize_write_with(&policy, &log, &req, AuditMode::ProbeOnly, || Ok(())),
            Ok(())
        );
        assert_eq!(log.probes.get(), 1);
        assert!(log.entries.borrow().is_empty());

        // The strictest mode of an unverifiable policy refuses every change
        // in the policy check already.
        let invalid = EffectivePolicy::invalid(PathBuf::new(), "bad".into());
        assert!(invalid.audit_required());
        let result = authorize_write_with(
            &invalid,
            &FakeLog::new(false),
            &req,
            AuditMode::ProbeOnly,
            || Ok(()),
        );
        assert!(matches!(result, Err(WriteRefusal::Policy(_))), "{result:?}");
    }

    fn export(channel: ExportChannel, bootstrap: &'static str) -> ExportRequest<'static> {
        ExportRequest {
            source: "cli",
            channel,
            label: "dump",
            target: "orders",
            records: None,
            profile: "dev",
            bootstrap,
            actor: None,
        }
    }

    fn export_policy(rule: ExportRule, clusters: &[&str]) -> EffectivePolicy {
        verified(PolicyRules {
            export: rule,
            export_clusters: clusters.iter().map(|c| c.to_string()).collect(),
            ..PolicyRules::default()
        })
    }

    #[test]
    fn authorize_export_records_nothing_when_allowed() {
        let req = export(ExportChannel::File, "localhost:9092");
        for policy in [
            EffectivePolicy::absent(PathBuf::new()),
            export_policy(ExportRule::Allowed, &[]),
        ] {
            let log = FakeLog::new(false);
            assert_eq!(authorize_export_with(&policy, &log, &req), Ok(()));
            assert!(log.entries.borrow().is_empty());
        }
    }

    #[test]
    fn authorize_export_refuses_forbidden_exports_and_records_them() {
        let policy = export_policy(ExportRule::Forbidden, &[]);
        let log = FakeLog::new(true);
        let req = ExportRequest {
            target: "orders p3@1200",
            ..export(ExportChannel::Clipboard, "localhost:9092")
        };
        let result = authorize_export_with(&policy, &log, &req);
        let Err(WriteRefusal::Policy(reason)) = result else {
            panic!("expected a policy refusal, got {result:?}");
        };
        assert_eq!(
            reason,
            "Refused by company policy by 보안팀, key abcd: exporting record data is \
             forbidden (export: forbidden), so `dump` is not allowed."
        );
        let entries = log.entries.borrow();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].action, "policy.blocked");
        assert_eq!(entries[0].target, "export:clipboard dump orders p3@1200");
        assert_eq!(entries[0].profile, "dev");
        assert_eq!(entries[0].outcome, reason);
        assert!(!entries[0].ok);

        // Masking does not make an export allowed, and an unwritable log
        // still refuses.
        let mut masked = policy.clone();
        masked.rules.masking = MaskingRule::Enforced;
        let result = authorize_export_with(&masked, &FakeLog::new(false), &req);
        assert!(matches!(result, Err(WriteRefusal::Policy(_))), "{result:?}");
    }

    #[test]
    fn authorize_export_records_audited_exports_first() {
        let policy = export_policy(ExportRule::Audited, &[]);
        let log = FakeLog::new(true);
        let req = ExportRequest {
            records: Some(42),
            profile: "",
            ..export(ExportChannel::File, "localhost:9092")
        };
        assert_eq!(authorize_export_with(&policy, &log, &req), Ok(()));
        let entries = log.entries.borrow();
        assert_eq!(entries.len(), 1);
        assert_eq!(
            (
                entries[0].source.as_str(),
                entries[0].profile.as_str(),
                entries[0].action.as_str(),
                entries[0].target.as_str(),
                entries[0].outcome.as_str(),
                entries[0].ok
            ),
            (
                "cli",
                "localhost:9092",
                "record.export",
                "export:file dump orders",
                "records: 42",
                true
            )
        );

        // An unwritable log: optional auditing warns and goes on ...
        let log = FakeLog::new(false);
        assert_eq!(authorize_export_with(&policy, &log, &req), Ok(()));
        // ... required auditing refuses.
        let mut required = policy.clone();
        required.rules.audit = ferrika_core::model::policy::AuditRule::Required;
        assert_eq!(
            authorize_export_with(&required, &log, &req),
            Err(WriteRefusal::AuditUnavailable(
                "Refused by company policy by 보안팀, key abcd: exports are audited, \
                 but the audit log could not be written (read-only folder)."
                    .into()
            ))
        );
    }

    #[test]
    fn audited_exports_get_a_result_line_with_the_count() {
        let req = export(ExportChannel::File, "localhost:9092");
        let audited = export_policy(ExportRule::Audited, &[]);
        let entry = export_result_entry(&audited, &req, 560).unwrap();
        assert_eq!(
            (
                entry.action.as_str(),
                entry.target.as_str(),
                entry.outcome.as_str(),
                entry.ok
            ),
            (
                "record.export",
                "export:file dump orders",
                "records: 560",
                true
            )
        );
        let allowed = export_policy(ExportRule::Allowed, &[]);
        assert!(export_result_entry(&allowed, &req, 560).is_none());
        let elsewhere = export_policy(ExportRule::Audited, &["*.prod.corp"]);
        assert!(export_result_entry(&elsewhere, &req, 560).is_none());
    }

    #[test]
    fn export_clusters_match_broker_addresses_only() {
        let policy = export_policy(ExportRule::Forbidden, &["*.prod.corp", "10.20.0.*:9093"]);
        assert_eq!(
            policy.export_rule("localhost:9092, SASL_SSL://Kafka1.PROD.corp:9094"),
            ExportRule::Forbidden
        );
        assert_eq!(policy.export_rule("10.20.0.7:9093"), ExportRule::Forbidden);
        assert_eq!(policy.export_rule("10.20.0.7"), ExportRule::Allowed);
        assert_eq!(policy.export_rule("localhost:9092"), ExportRule::Allowed);
        assert_eq!(policy.export_rule(""), ExportRule::Allowed);
        // A profile named like the rule does not count.
        let log = FakeLog::new(true);
        let req = ExportRequest {
            profile: "kafka1.prod.corp",
            ..export(ExportChannel::Stdout, "localhost:9092")
        };
        assert_eq!(authorize_export_with(&policy, &log, &req), Ok(()));
        assert!(log.entries.borrow().is_empty());
        let req = export(ExportChannel::Stdout, "kafka2.prod.corp:9092");
        assert!(authorize_export_with(&policy, &log, &req).is_err());

        // No list: every cluster.
        let everywhere = export_policy(ExportRule::Audited, &[]);
        assert_eq!(
            everywhere.export_rule("localhost:9092"),
            ExportRule::Audited
        );
        assert_eq!(everywhere.export_rule(""), ExportRule::Audited);
    }

    #[test]
    fn screen_capture_is_blocked_only_by_an_active_rule() {
        use ferrika_core::model::policy::ScreenCaptureRule;
        let now = Timestamp::now();
        assert!(!EffectivePolicy::absent(PathBuf::new()).screen_capture_blocked_at(now));
        // An untrusted policy falls back to the strictest mode, which does
        // not block (see PolicyRules::strictest).
        assert!(
            !EffectivePolicy::invalid(PathBuf::new(), "bad".into()).screen_capture_blocked_at(now)
        );
        let mut policy = export_policy(ExportRule::Allowed, &[]);
        assert!(!policy.screen_capture_blocked_at(now));
        policy.rules.screen_capture = ScreenCaptureRule::Blocked;
        assert!(policy.screen_capture_blocked_at(now));
    }

    #[test]
    fn export_and_watermark_follow_strictest_and_expiry() {
        let invalid = EffectivePolicy::invalid(PathBuf::new(), "bad".into());
        assert_eq!(invalid.export_rule("localhost:9092"), ExportRule::Forbidden);
        assert_eq!(
            invalid.watermark_at(Timestamp::now()),
            Some(WatermarkSpec { text: None })
        );
        let absent = EffectivePolicy::absent(PathBuf::new());
        assert_eq!(absent.watermark_at(Timestamp::now()), None);

        let mut policy = export_policy(ExportRule::Audited, &["*.prod.corp"]);
        policy.rules.watermark = WatermarkRule::On;
        policy.rules.watermark_text = Some("대외비".into());
        policy.expiry = Some(PolicyExpiry {
            at: at("2026-12-01T00:00:00Z"),
            text: "2026-12-01T00:00:00Z".into(),
            on_expiry: ExpiryRule::Strictest,
        });
        let before = at("2026-11-30T00:00:00Z");
        let after = at("2026-12-02T00:00:00Z");
        assert_eq!(
            policy.export_rule_at(before, "localhost:9092"),
            ExportRule::Allowed
        );
        assert_eq!(
            policy.watermark_at(before),
            Some(WatermarkSpec {
                text: Some("대외비".into())
            })
        );
        // Expired into the strictest mode: forbidden everywhere, the
        // watermark on without the policy's own text.
        assert_eq!(
            policy.export_rule_at(after, "localhost:9092"),
            ExportRule::Forbidden
        );
        assert_eq!(
            policy.watermark_at(after),
            Some(WatermarkSpec { text: None })
        );
        // `warn` keeps the rules.
        if let Some(expiry) = policy.expiry.as_mut() {
            expiry.on_expiry = ExpiryRule::Warn;
        }
        assert_eq!(
            policy.export_rule_at(after, "localhost:9092"),
            ExportRule::Allowed
        );
        assert_eq!(
            policy.export_rule_at(after, "k1.prod.corp"),
            ExportRule::Audited
        );
    }

    #[test]
    fn audit_probe_opens_for_append_without_writing() {
        let dir = temp_dir("audit-probe");
        let ok = dir.join("ok").join("audit.log");
        assert!(appendable(&ok).is_ok());
        assert_eq!(std::fs::metadata(&ok).map(|m| m.len()).ok(), Some(0));
        let blocked = dir.join("blocked").join("audit.log");
        std::fs::create_dir_all(&blocked).unwrap();
        assert!(appendable(&blocked).is_err());
        let _ = std::fs::remove_dir_all(dir);
    }

    fn at(text: &str) -> Timestamp {
        text.parse().unwrap()
    }

    fn payload_with(extra: &str) -> String {
        PAYLOAD.replacen("{\"issuer\"", &format!("{{{extra},\"issuer\""), 1)
    }

    /// A verified policy loaded from a temp dir, signed from `payload`.
    fn load_signed(name: &str, payload: &str) -> EffectivePolicy {
        let dir = temp_dir(name);
        let (private, public) = generate_keypair().unwrap();
        std::fs::write(dir.join(PUBLIC_KEY_FILE), to_hex(&public)).unwrap();
        std::fs::write(dir.join(POLICY_FILE), signed_text(&private, payload)).unwrap();
        let policy = load_from(&dir);
        let _ = std::fs::remove_dir_all(dir);
        policy
    }

    #[test]
    fn sign_writes_format_1_unless_v2_fields_are_used() {
        let (private, public) = generate_keypair().unwrap();
        assert_eq!(sign(&private, PAYLOAD).unwrap().format, 1);
        let v2 = payload_with(r#""expires_at":"2027-01-01T00:00:00Z""#);
        let signed = sign(&private, &v2).unwrap();
        assert_eq!(signed.format, 2);
        let payload = verify(&serde_json::to_string(&signed).unwrap(), &public).unwrap();
        assert_eq!(payload.expires_at.as_deref(), Some("2027-01-01T00:00:00Z"));
        let v2 = payload_with(r#""on_expiry":"strictest""#);
        assert_eq!(sign(&private, &v2).unwrap().format, 2);

        // Formats other than 1 and 2 are refused.
        let mut signed = sign(&private, PAYLOAD).unwrap();
        signed.format = 3;
        assert!(matches!(
            verify(&serde_json::to_string(&signed).unwrap(), &public),
            Err(PolicyError::Version(3))
        ));
    }

    #[test]
    fn format_1_files_with_v2_fields_are_refused() {
        let (private, public) = generate_keypair().unwrap();
        let mut signed = sign(
            &private,
            &payload_with(r#""expires_at":"2027-01-01T00:00:00Z""#),
        )
        .unwrap();
        // Re-labelled as format 1; the signature covers only the payload.
        signed.format = 1;
        let err = verify(&serde_json::to_string(&signed).unwrap(), &public).unwrap_err();
        assert!(err.to_string().contains("need format 2"), "{err}");
    }

    #[test]
    fn invalid_expiry_date_fails_closed() {
        let (private, _) = generate_keypair().unwrap();
        for bad in [
            r#""expires_at":"2027-01-01""#,
            r#""expires_at":"next year""#,
            r#""expires_at":"2027-13-01T00:00:00Z""#,
        ] {
            let err = sign(&private, &payload_with(bad)).unwrap_err();
            assert!(err.to_string().contains("RFC 3339"), "{err}");
        }
        // A signed file with a bad date (signed by a tool that did not check)
        // loads as invalid: the strictest mode.
        let dir = temp_dir("bad-expiry");
        let (private, public) = generate_keypair().unwrap();
        let payload = payload_with(r#""expires_at":"soon""#);
        let pair = Ed25519KeyPair::from_pkcs8(&private).unwrap();
        let signed = SignedPolicy {
            format: 2,
            key_id: key_id(&public),
            signature: to_hex(pair.sign(payload.as_bytes()).as_ref()),
            payload,
        };
        std::fs::write(dir.join(PUBLIC_KEY_FILE), to_hex(&public)).unwrap();
        std::fs::write(
            dir.join(POLICY_FILE),
            serde_json::to_string(&signed).unwrap(),
        )
        .unwrap();
        let policy = load_from(&dir);
        assert!(policy.is_invalid(), "{:?}", policy.status);
        assert_eq!(policy.rules, PolicyRules::strictest());
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn expiry_warns_two_weeks_ahead_and_keeps_rules_after() {
        let policy = load_signed(
            "expiry-warn",
            &payload_with(r#""expires_at":"2026-12-01T00:00:00+09:00""#),
        );
        assert!(matches!(policy.status, PolicyStatus::Verified { .. }));
        let expiry = policy.expiry.as_ref().unwrap();
        assert_eq!(expiry.on_expiry, ExpiryRule::Warn);
        assert_eq!(expiry.at, at("2026-11-30T15:00:00Z"));

        let early = at("2026-11-02T00:00:00Z");
        assert_eq!(policy.expiry_state_at(early), ExpiryState::Current);
        assert_eq!(policy.expiry_note_at(early), None);
        assert_eq!(policy.expiry_warning_at(early), None);
        assert_eq!(
            policy.label_at(early),
            format!("company policy by 보안팀, key {}", key_of(&policy))
        );

        let soon = at("2026-11-20T15:00:00Z");
        assert_eq!(
            policy.expiry_state_at(soon),
            ExpiryState::ExpiresSoon { days_left: 10 }
        );
        assert!(policy.label_at(soon).ends_with("(expires in 10 days)"));
        let warning = policy.expiry_warning_at(soon).unwrap();
        assert!(warning.contains("expires in 10 days"), "{warning}");
        assert_eq!(
            policy.expiry_state_at(at("2026-11-30T14:00:00Z")),
            ExpiryState::ExpiresSoon { days_left: 1 }
        );
        assert_eq!(
            policy.expiry_state_at(at("2026-11-16T15:00:00Z")),
            ExpiryState::ExpiresSoon { days_left: 14 }
        );

        // Expired with `warn`: the same rules, never "no policy".
        let late = at("2026-12-05T00:00:00Z");
        assert_eq!(policy.expiry_state_at(late), ExpiryState::ExpiredWarn);
        assert!(policy.is_active());
        assert!(!policy.is_strictest_at(late));
        assert_eq!(*policy.rules_at(late), policy.rules);
        assert!(policy.masking_enforced_at(late));
        assert!(policy
            .check_at(late, PolicyAction::Produce, "dev", "localhost:9092")
            .is_ok());
        let err = policy
            .check_at(late, PolicyAction::TopicsDelete, "dev", "localhost:9092")
            .unwrap_err();
        assert!(
            err.contains("expired 2026-12-01T00:00:00+09:00") && err.contains("forbidden"),
            "{err}"
        );
        let warning = policy.expiry_warning_at(late).unwrap();
        assert!(warning.contains("its rules still apply"), "{warning}");
    }

    fn key_of(policy: &EffectivePolicy) -> String {
        match &policy.status {
            PolicyStatus::Verified { key_id, .. } => key_id.clone(),
            other => panic!("not verified: {other:?}"),
        }
    }

    #[test]
    fn expired_strictest_policy_switches_to_the_strictest_mode() {
        let policy = load_signed(
            "expiry-strict",
            &payload_with(r#""expires_at":"2026-12-01T00:00:00Z","on_expiry":"strictest""#),
        );
        let before = at("2026-11-30T23:59:59Z");
        assert!(!policy.is_strictest_at(before));
        assert!(policy
            .check_at(before, PolicyAction::Produce, "dev", "localhost:9092")
            .is_ok());
        assert_eq!(policy.unmask_at(before), UnmaskRule::WithReason);

        let after = at("2026-12-01T00:00:00Z");
        assert_eq!(policy.expiry_state_at(after), ExpiryState::ExpiredStrictest);
        assert!(policy.is_strictest_at(after));
        assert!(policy.is_active());
        assert_eq!(*policy.rules_at(after), PolicyRules::strictest());
        assert_eq!(policy.unmask_at(after), UnmaskRule::Forbidden);
        assert!(policy.audit_required_at(after));
        assert!(policy
            .read_only_match_at(after, "dev", "localhost:9092")
            .is_some());
        let err = policy
            .check_at(after, PolicyAction::Produce, "dev", "localhost:9092")
            .unwrap_err();
        assert!(err.contains("while the policy is expired"), "{err}");
        assert!(
            policy
                .label_at(after)
                .ends_with("(expired 2026-12-01T00:00:00Z, strictest mode)"),
            "{}",
            policy.label_at(after)
        );
        // The policy's own rules are kept for display.
        assert_eq!(policy.rules.read_only, vec!["prod-*".to_string()]);
    }

    #[test]
    fn key_list_parses_keys_labels_and_comments() {
        let (_, a) = generate_keypair().unwrap();
        let (_, b) = generate_keypair().unwrap();
        let text = format!(
            "\u{feff}# rotation 2027\n\n{}  security team 2026\n  {} # new key\r\n{}\n",
            to_hex(&a),
            to_hex(&b).to_uppercase(),
            to_hex(&a)
        );
        let keys = parse_key_list(text.trim_start_matches('\u{feff}'), "x/policy.keys").unwrap();
        assert_eq!(keys.len(), 3);
        assert_eq!(keys[0].key_id, key_id(&a));
        assert_eq!(keys[0].label, "security team 2026");
        assert_eq!(keys[0].file, "policy.keys");
        assert_eq!(keys[1].public_key, b);
        assert_eq!(keys[1].label, "");
        assert!(parse_key_list("# only comments\n\n", "k")
            .unwrap()
            .is_empty());

        for bad in [
            "abcd",
            "zz00000000000000000000000000000000000000000000000000000000000000",
            &format!("{}00", to_hex(&a)),
            &format!("key {}", to_hex(&a)),
        ] {
            let text = format!("{}\n{bad}\n", to_hex(&b));
            let err = parse_key_list(&text, "policy.keys").unwrap_err();
            assert!(
                err.to_string().starts_with("policy.keys line 2:"),
                "{bad}: {err}"
            );
        }
    }

    #[test]
    fn rotation_trusts_policy_pub_and_policy_keys() {
        let dir = temp_dir("rotation");
        let (old_private, old_public) = generate_keypair().unwrap();
        let (new_private, new_public) = generate_keypair().unwrap();
        std::fs::write(dir.join(PUBLIC_KEY_FILE), to_hex(&old_public)).unwrap();
        std::fs::write(
            dir.join(KEY_LIST_FILE),
            format!(
                "# old key again (ignored as a duplicate), then the new one\n{} old\n{} 2027 key\n",
                to_hex(&old_public),
                to_hex(&new_public)
            ),
        )
        .unwrap();
        let keys = trusted_keys(&dir).unwrap();
        assert_eq!(
            keys.iter()
                .map(|k| (k.key_id.clone(), k.file.as_str(), k.label.as_str()))
                .collect::<Vec<_>>(),
            vec![
                (key_id(&old_public), PUBLIC_KEY_FILE, ""),
                (key_id(&new_public), KEY_LIST_FILE, "2027 key"),
            ]
        );

        // Signed with either key: verified, and the key that signed it named.
        for (private, public) in [(&old_private, &old_public), (&new_private, &new_public)] {
            std::fs::write(dir.join(POLICY_FILE), signed_text(private, PAYLOAD)).unwrap();
            let policy = load_from(&dir);
            assert_eq!(key_of(&policy), key_id(public));
            assert_eq!(policy.trusted_keys.len(), 2);
        }

        // Signed by a key in neither file: invalid, naming the trusted ids.
        let (other_private, other_public) = generate_keypair().unwrap();
        std::fs::write(dir.join(POLICY_FILE), signed_text(&other_private, PAYLOAD)).unwrap();
        let policy = load_from(&dir);
        let PolicyStatus::Invalid { reason } = &policy.status else {
            panic!("expected invalid, got {:?}", policy.status);
        };
        assert!(
            reason.contains(&key_id(&other_public))
                && reason.contains(&key_id(&old_public))
                && reason.contains(&key_id(&new_public)),
            "{reason}"
        );
        assert_eq!(policy.rules, PolicyRules::strictest());
        // The keys are still listed for `policy show`.
        assert_eq!(policy.trusted_keys.len(), 2);

        // A malformed line in policy.keys makes the policy invalid even
        // though policy.pub alone would verify it.
        std::fs::write(dir.join(POLICY_FILE), signed_text(&old_private, PAYLOAD)).unwrap();
        std::fs::write(dir.join(KEY_LIST_FILE), "not-a-key\n").unwrap();
        let policy = load_from(&dir);
        assert!(policy.is_invalid());
        assert!(
            matches!(&policy.status, PolicyStatus::Invalid { reason } if reason.contains("line 1")),
            "{:?}",
            policy.status
        );

        // policy.keys alone (no policy.pub) is enough.
        std::fs::remove_file(dir.join(PUBLIC_KEY_FILE)).unwrap();
        std::fs::write(
            dir.join(KEY_LIST_FILE),
            format!("{} 2027 key\n", to_hex(&new_public)),
        )
        .unwrap();
        std::fs::write(dir.join(POLICY_FILE), signed_text(&new_private, PAYLOAD)).unwrap();
        assert_eq!(key_of(&load_from(&dir)), key_id(&new_public));
        // ... and a key list without a policy means the policy was removed.
        std::fs::remove_file(dir.join(POLICY_FILE)).unwrap();
        assert!(load_from(&dir).is_invalid());
        // An empty key list with a policy: no trusted key.
        std::fs::write(dir.join(POLICY_FILE), signed_text(&new_private, PAYLOAD)).unwrap();
        std::fs::write(dir.join(KEY_LIST_FILE), "# nothing yet\n").unwrap();
        assert!(load_from(&dir).is_invalid());
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn policy_dir_override_only_where_allowed() {
        let demo = OsStr::new("demo-policy");
        let known = Some(PathBuf::from(r"D:\Data"));
        let system = policy_dir_from(None, true, known.clone());
        if cfg!(windows) {
            assert_eq!(system, PathBuf::from(r"D:\Data\Ferrika"));
            // An unresolvable or relative known folder falls back to the
            // fixed path, never to an environment variable.
            assert_eq!(
                policy_dir_from(None, false, None),
                PathBuf::from(r"C:\ProgramData\Ferrika")
            );
            assert_eq!(
                policy_dir_from(None, false, Some(PathBuf::from("relative"))),
                PathBuf::from(r"C:\ProgramData\Ferrika")
            );
        } else {
            assert_eq!(system, PathBuf::from("/etc/ferrika"));
        }
        // Debug / dev-policy-dir builds honour the override.
        assert_eq!(
            policy_dir_from(Some(demo), true, known.clone()),
            PathBuf::from("demo-policy")
        );
        assert!(!override_ignored(Some(demo), true));
        // Release builds ignore it and report that.
        assert_eq!(policy_dir_from(Some(demo), false, known.clone()), system);
        assert!(override_ignored(Some(demo), false));
        // An empty value is no override at all.
        assert_eq!(policy_dir_from(Some(OsStr::new("")), true, known), system);
        assert!(!override_ignored(Some(OsStr::new("")), false));
        assert!(!override_ignored(None, false));
    }

    #[cfg(windows)]
    #[test]
    fn program_data_comes_from_the_known_folder() {
        let dir = program_data().expect("ProgramData known folder");
        assert!(dir.is_absolute(), "{}", dir.display());
    }

    #[test]
    fn hex_round_trips() {
        let bytes = [0u8, 1, 0xab, 0xff];
        assert_eq!(from_hex(&to_hex(&bytes)).unwrap(), bytes);
        assert!(from_hex("abc").is_none());
        assert!(from_hex("zz").is_none());
    }

    #[test]
    fn export_rule_sees_the_clusters_own_brokers() {
        let config = KafkaClientConfig::new("10.0.0.5:9092");
        let policy = crate::test_policy::export_forbidden_for_clusters(&["*.prod.corp"]);
        // A tunnel address alone is not forbidden; the brokers it reports are.
        let looked_up = std::cell::Cell::new(0);
        let fetch_prod = |_: &KafkaClientConfig| {
            looked_up.set(looked_up.get() + 1);
            Ok(vec!["kafka1.prod.corp:9092".to_string()])
        };
        assert!(export_forbidden_with(&policy, &config, fetch_prod));
        assert_eq!(looked_up.get(), 1);
        let fetch_dev = |_: &KafkaClientConfig| Ok(vec!["kafka1.dev.corp:9092".to_string()]);
        assert!(!export_forbidden_with(&policy, &config, fetch_dev));
        // A lookup that fails refuses: the rule cannot be checked.
        let fail = |_: &KafkaClientConfig| Err("timed out".to_string());
        assert!(export_forbidden_with(&policy, &config, fail));
        assert!(export_brokers_with(&policy, &config, fail)
            .unwrap_err()
            .contains("timed out"));
    }

    #[test]
    fn no_export_clusters_rule_makes_no_lookup() {
        let config = KafkaClientConfig::new("b:9092");
        let boom = |_: &KafkaClientConfig| -> Result<Vec<String>, String> {
            panic!("no metadata request expected")
        };
        for policy in [
            crate::test_policy::none(),
            crate::test_policy::export_forbidden(),
            crate::test_policy::export_audited(),
        ] {
            assert_eq!(
                export_brokers_with(&policy, &config, boom),
                Ok("b:9092".to_string())
            );
        }
        assert!(export_forbidden_with(
            &crate::test_policy::export_forbidden(),
            &config,
            boom
        ));
    }
}
