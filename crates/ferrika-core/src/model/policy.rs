//! Signed company policy pack: the file format and the rules it carries.
//!
//! A security team signs one `company.policy`; every Ferrika install that
//! finds it in the machine-wide policy directory enforces it. The signature
//! covers the exact bytes of `payload`, so no JSON canonicalisation is needed.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// The original format: issuer, issued_at and rules.
pub const POLICY_FORMAT_V1: u32 = 1;
/// Adds `expires_at` and `on_expiry`, and the rules older builds do not know
/// (see [`PolicyPayload::uses_v2`]). `sign` writes format 1 when none is
/// used, so binaries that only read format 1 keep accepting those policies.
pub const POLICY_FORMAT_V2: u32 = 2;

/// `company.policy` as stored on disk.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SignedPolicy {
    pub format: u32,
    /// Short hex SHA-256 of the signer's public key.
    pub key_id: String,
    /// The policy JSON ([`PolicyPayload`]) as a string; signed byte for byte.
    pub payload: String,
    /// Hex Ed25519 signature over `payload`.
    pub signature: String,
}

/// What a policy says: who issued it, until when, and its rules.
///
/// Unknown fields are refused. The format 2 fields are known to this build;
/// a format 1 file that uses them is refused too (see [`Self::uses_v2`]).
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyPayload {
    #[serde(default)]
    pub issuer: String,
    #[serde(default)]
    pub issued_at: String,
    /// Format 2: RFC 3339 time after which the policy is expired.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<String>,
    /// Format 2: what an expired policy does; `warn` when not given.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_expiry: Option<ExpiryRule>,
    #[serde(default)]
    pub rules: PolicyRules,
}

impl PolicyPayload {
    /// Whether the payload uses a field that needs format 2.
    pub fn uses_v2(&self) -> bool {
        self.expires_at.is_some()
            || self.on_expiry.is_some()
            // Rules older builds reject as unknown: mark them format 2 too.
            || self.rules.audit_forward.is_some()
            || !self.rules.audit_anchor.is_none()
            || self.rules.anchor_required
            || !self.rules.read_only_brokers.is_empty()
            || !self.rules.export.is_allowed()
            || !self.rules.export_clusters.is_empty()
            || !self.rules.watermark.is_off()
            || self.rules.watermark_text.is_some()
            || !self.rules.screen_capture.is_allowed()
            || !self.rules.mcp_write.is_allowed()
            || !self.rules.environments.is_empty()
            || self.rules.default_environment.is_some()
            || !self.rules.copy_down.is_allow()
    }

    /// The `format` a signed file with this payload carries.
    pub fn format(&self) -> u32 {
        if self.uses_v2() {
            POLICY_FORMAT_V2
        } else {
            POLICY_FORMAT_V1
        }
    }
}

/// What happens once `expires_at` has passed. Neither choice relaxes the
/// rules: an expired policy is never treated as no policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExpiryRule {
    /// Keep enforcing the rules and warn (CLI stderr, GUI badge).
    #[default]
    Warn,
    /// Switch to the strictest mode, as for a policy that cannot be verified.
    Strictest,
}

/// Rules. Unknown fields are refused, so a misspelt rule cannot silently
/// weaken a policy (the policy is then invalid and the strictest mode applies).
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyRules {
    /// Profile-name or broker-address globs that are read-only; `*` = all.
    #[serde(default)]
    pub read_only: Vec<String>,
    /// Policy pack v2: broker-address globs that are read-only. Unlike
    /// `read_only` these never match the (user-chosen) profile name, so
    /// renaming a profile cannot get around them. Left out of the payload
    /// when empty, so v1 policies serialize unchanged.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub read_only_brokers: Vec<String>,
    /// Action categories ([`PolicyAction::id`]) refused everywhere; `*` = every change.
    #[serde(default)]
    pub forbid: Vec<String>,
    #[serde(default)]
    pub masking: MaskingRule,
    #[serde(default)]
    pub unmask: UnmaskRule,
    #[serde(default)]
    pub audit: AuditRule,
    /// Where every audit entry is also sent (syslog and/or a spool folder).
    /// Optional: policies without it verify and behave exactly as before.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_audit_forward"
    )]
    pub audit_forward: Option<AuditForward>,
    /// Policy pack v2: where each audit append's `seq`/`hash` is copied so
    /// truncating the log can be detected. Left out of the payload when
    /// `none`, so v1 policies serialize unchanged.
    #[serde(default, skip_serializing_if = "AuditAnchorRule::is_none")]
    pub audit_anchor: AuditAnchorRule,
    /// Policy pack v2: refuse a required-audit change when the anchor cannot
    /// be written. Off by default: a failed anchor write only warns.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub anchor_required: bool,
    /// Policy pack v2: whether record data (keys, payloads, header values)
    /// may leave the app: files, the clipboard, redirected stdout, another
    /// cluster, MCP. Left out of the payload when `allowed`.
    #[serde(default, skip_serializing_if = "ExportRule::is_allowed")]
    pub export: ExportRule,
    /// Policy pack v2: broker-address globs `export` applies to (matched
    /// like `read_only_brokers`, never the profile name); empty = every
    /// cluster.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub export_clusters: Vec<String>,
    /// Policy pack v2: draw a watermark (user, host, time, cluster) over the
    /// whole GUI. Independent of `export`.
    #[serde(default, skip_serializing_if = "WatermarkRule::is_off")]
    pub watermark: WatermarkRule,
    /// Policy pack v2: short text put in front of the watermark (1-40
    /// characters, no control characters).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub watermark_text: Option<String>,
    /// Policy pack v2: `blocked` keeps the desktop app's window out of
    /// screenshots, screen recordings and screen shares (Windows). Left out
    /// of the payload when `allowed`.
    #[serde(default, skip_serializing_if = "ScreenCaptureRule::is_allowed")]
    pub screen_capture: ScreenCaptureRule,
    /// Policy pack v2: what `ferrika mcp --allow-write` may do on behalf of
    /// an AI assistant, on top of the other rules. Left out of the payload
    /// when `allowed`.
    #[serde(default, skip_serializing_if = "McpWriteRule::is_allowed")]
    pub mcp_write: McpWriteRule,
    /// Policy pack v2: environment tiers by name (`prod`, `staging`,
    /// `dev`), each with the clusters it covers and its own stricter rules.
    /// Left out of the payload when empty.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub environments: BTreeMap<String, Environment>,
    /// Policy pack v2: the environment of a cluster no environment matches;
    /// none when not given (such a cluster has no environment).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_environment: Option<String>,
    /// Policy pack v2: what a record copy from a higher tier to a lower one
    /// (or to a cluster without an environment) needs. Left out when `allow`.
    #[serde(default, skip_serializing_if = "CopyDownRule::is_allow")]
    pub copy_down: CopyDownRule,
}

/// Longest environment name, in characters.
pub const ENVIRONMENT_NAME_MAX: usize = 32;

/// One environment of `rules.environments` (policy pack v2).
///
/// ```json
/// { "tier": 3, "profiles": ["prod-*"], "brokers": ["*.prod.corp"],
///   "read_only": false, "forbid": ["topics.delete"] }
/// ```
///
/// A cluster is in the environment when its profile name matches one of
/// `profiles` or one of its broker addresses matches one of `brokers`
/// (matched like `read_only_brokers`). Unknown fields are refused.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Environment {
    /// Higher is more sensitive: a copy from a higher tier to a lower one
    /// is a copy down (see [`CopyDownRule`]).
    pub tier: u32,
    /// Profile-name globs. The profile name is the user's to choose, so
    /// prefer `brokers` for clusters that must not be reclassified.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub profiles: Vec<String>,
    /// Broker-address globs (`*.prod.corp`, `10.20.*:9093`), never the
    /// profile name.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub brokers: Vec<String>,
    /// Every change to a cluster of this environment is refused.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub read_only: bool,
    /// Categories ([`PolicyAction::id`]) refused in this environment, on top
    /// of the global `forbid`; `*` = every change.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub forbid: Vec<String>,
}

impl Environment {
    /// Whether the environment restricts changes itself (`read_only` or
    /// `forbid`), besides classifying clusters.
    pub fn restricts_changes(&self) -> bool {
        self.read_only || !self.forbid.is_empty()
    }
}

/// What a record copy from a higher environment tier to a lower one needs
/// (policy pack v2). Without `environments` nothing is a copy down.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CopyDownRule {
    /// No extra condition (the default, as before).
    #[default]
    Allow,
    /// Allowed only when every record is PII-masked before anything else
    /// reads it (`transform --mask` as the first step, `mirror --mask`).
    Masked,
    /// Refused and recorded as `policy.blocked`.
    Forbid,
}

impl CopyDownRule {
    pub fn is_allow(&self) -> bool {
        *self == Self::Allow
    }

    /// The name in the policy file.
    pub fn id(self) -> &'static str {
        match self {
            Self::Allow => "allow",
            Self::Masked => "masked",
            Self::Forbid => "forbid",
        }
    }
}

/// Longest `watermark_text`, in characters.
pub const WATERMARK_TEXT_MAX: usize = 40;

impl PolicyRules {
    /// Everything read-only, masking on, no unmasking, no record export and
    /// the watermark on: used when a policy is present but cannot be
    /// trusted. An untrusted policy's forwarding target is not trusted
    /// either, so nothing is forwarded on its behalf.
    pub fn strictest() -> Self {
        Self {
            read_only: vec!["*".to_string()],
            // `read_only: *` already covers every broker.
            read_only_brokers: Vec::new(),
            forbid: vec!["*".to_string()],
            masking: MaskingRule::Enforced,
            unmask: UnmaskRule::Forbidden,
            audit: AuditRule::Required,
            audit_forward: None,
            // The OS log source may not be registered on this machine, and an
            // untrusted policy should not start writing to it.
            audit_anchor: AuditAnchorRule::None,
            anchor_required: false,
            export: ExportRule::Forbidden,
            // Empty: forbidden on every cluster.
            export_clusters: Vec::new(),
            watermark: WatermarkRule::On,
            watermark_text: None,
            // Not blocked: with an untrusted policy people need to show the
            // badge to whoever fixes it, often over a screen share; the
            // watermark still marks what they share.
            screen_capture: ScreenCaptureRule::Allowed,
            // Every change is refused anyway (`forbid: *`); a refusal names
            // the untrusted policy rather than this rule.
            mcp_write: McpWriteRule::Allowed,
            // `read_only: *` and `forbid: *` already refuse every change
            // and copy; an untrusted classification is not used.
            environments: BTreeMap::new(),
            default_environment: None,
            copy_down: CopyDownRule::Allow,
        }
    }

    /// Why the export and watermark rules make no sense, if they do not: an
    /// empty `export_clusters` glob, or a `watermark_text` that is empty,
    /// too long or has control characters.
    pub fn export_rules_error(&self) -> Option<String> {
        if self.export_clusters.iter().any(|g| g.trim().is_empty()) {
            return Some("export_clusters has an empty entry".to_string());
        }
        let text = self.watermark_text.as_deref()?;
        let chars = text.chars().count();
        if text.trim().is_empty() || chars > WATERMARK_TEXT_MAX {
            return Some(format!(
                "watermark_text must be 1-{WATERMARK_TEXT_MAX} characters (it has {chars})"
            ));
        }
        if text.chars().any(char::is_control) {
            return Some("watermark_text has a control character".to_string());
        }
        None
    }

    /// Forbid entries that name no known category: the global ones as
    /// written, an environment's as `<environment>: <entry>`.
    pub fn unknown_forbid_entries(&self) -> Vec<String> {
        let unknown = |f: &&String| f.as_str() != "*" && PolicyAction::from_id(f).is_none();
        self.forbid
            .iter()
            .filter(unknown)
            .cloned()
            .chain(self.environments.iter().flat_map(|(name, env)| {
                env.forbid
                    .iter()
                    .filter(unknown)
                    .map(move |f| format!("{name}: {f}"))
            }))
            .collect()
    }

    /// Why the environment rules make no sense, if they do not: a bad
    /// environment name, an empty glob, a `default_environment` that names
    /// no environment, or `copy_down` without environments to compare.
    pub fn environment_rules_error(&self) -> Option<String> {
        for (name, env) in &self.environments {
            let chars = name.chars().count();
            if chars == 0
                || chars > ENVIRONMENT_NAME_MAX
                || !name
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
            {
                return Some(format!(
                    "environment name '{name}' must be 1-{ENVIRONMENT_NAME_MAX} letters, \
                     digits, '-', '_' or '.'"
                ));
            }
            if env
                .profiles
                .iter()
                .chain(&env.brokers)
                .any(|g| g.trim().is_empty())
            {
                return Some(format!("environment '{name}' has an empty glob"));
            }
        }
        if let Some(default) = &self.default_environment {
            if !self.environments.contains_key(default) {
                return Some(format!(
                    "default_environment '{default}' is not one of the environments"
                ));
            }
        }
        if !self.copy_down.is_allow() && self.environments.is_empty() {
            return Some("copy_down needs environments to compare".to_string());
        }
        None
    }
}

/// Whether PII masking is the user's choice or mandatory.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MaskingRule {
    #[default]
    Optional,
    Enforced,
}

/// Whether masked payloads may be shown in full.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnmaskRule {
    #[default]
    Allowed,
    /// Allowed after the user gives a reason, which is audited.
    WithReason,
    Forbidden,
}

/// Whether an operation may proceed when its audit entry cannot be written.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditRule {
    #[default]
    Optional,
    Required,
}

/// External copy of the audit chain's head (policy pack v2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditAnchorRule {
    #[default]
    None,
    /// Windows Event Log (Application, source `Ferrika`) or syslog.
    OsLog,
}

impl AuditAnchorRule {
    pub fn is_none(&self) -> bool {
        *self == Self::None
    }
}

/// Whether record data may be exported (policy pack v2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExportRule {
    #[default]
    Allowed,
    /// Allowed, and each export is recorded as `record.export` first.
    Audited,
    /// Refused and recorded as `policy.blocked`.
    Forbidden,
}

impl ExportRule {
    pub fn is_allowed(&self) -> bool {
        *self == Self::Allowed
    }
}

/// Whether the GUI draws the policy watermark (policy pack v2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WatermarkRule {
    #[default]
    Off,
    On,
}

impl WatermarkRule {
    pub fn is_off(&self) -> bool {
        *self == Self::Off
    }
}

/// Whether the desktop app's window may appear in screenshots, recordings
/// and screen shares (policy pack v2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScreenCaptureRule {
    #[default]
    Allowed,
    Blocked,
}

impl ScreenCaptureRule {
    pub fn is_allowed(&self) -> bool {
        *self == Self::Allowed
    }
}

/// What AI assistants may change through `ferrika mcp --allow-write`
/// (policy pack v2), on top of the rules every change passes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpWriteRule {
    /// Like any other change (the server's own confirmation still applies).
    #[default]
    Allowed,
    /// Only after a person confirms in the client (MCP elicitation); the
    /// one-time token, which the model can pass on its own, is refused.
    ConfirmHuman,
    /// No write tools at all.
    Forbidden,
}

impl McpWriteRule {
    pub fn is_allowed(&self) -> bool {
        *self == Self::Allowed
    }
}

/// `rules.audit_forward`: send every audit entry, after it is appended to the
/// local hash-chained log, to a SIEM as well.
///
/// ```json
/// { "syslog": "tcp://siem.corp:601", "facility": "auth", "app_name": "ferrika",
///   "spool_dir": "C:\\ProgramData\\Ferrika\\spool", "timeout_ms": 1000, "required": false }
/// ```
///
/// At least one of `syslog` and `spool_dir` is needed; both may be set.
/// Unknown fields are refused, like everywhere else in the rules.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuditForward {
    /// `udp://host[:port]`, `tcp://host[:port]` (RFC 6587 octet counting) or
    /// `tls://host[:port]` (RFC 5425). Default ports 514, 514 and 6514.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub syslog: Option<String>,
    /// A folder a log collector watches; each entry is appended as a JSON
    /// line to `ferrika-audit-<host>-<user>.jsonl` in it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spool_dir: Option<String>,
    #[serde(default)]
    pub facility: SyslogFacility,
    /// RFC 5424 APP-NAME (1–48 printable ASCII characters, no spaces).
    #[serde(default = "default_app_name")]
    pub app_name: String,
    /// How long a CLI command or GUI action waits for one forward at most.
    #[serde(default = "default_forward_timeout_ms")]
    pub timeout_ms: u64,
    /// With `audit: required`, a failed forward refuses the action (the
    /// entry is still in the local log). Default: warn and go on.
    #[serde(default)]
    pub required: bool,
    /// PEM CA file for `tls://`; the OS trust store when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ca_file: Option<String>,
}

pub const DEFAULT_APP_NAME: &str = "ferrika";
pub const DEFAULT_FORWARD_TIMEOUT_MS: u64 = 1000;
/// Bounds for `timeout_ms`, so a policy cannot make every action hang.
pub const FORWARD_TIMEOUT_RANGE_MS: std::ops::RangeInclusive<u64> = 50..=10_000;

fn default_app_name() -> String {
    DEFAULT_APP_NAME.to_string()
}

fn default_forward_timeout_ms() -> u64 {
    DEFAULT_FORWARD_TIMEOUT_MS
}

impl AuditForward {
    /// Forwarding with defaults to one syslog URL or spool folder.
    pub fn new(syslog: Option<String>, spool_dir: Option<String>) -> Self {
        Self {
            syslog,
            spool_dir,
            facility: SyslogFacility::default(),
            app_name: default_app_name(),
            timeout_ms: DEFAULT_FORWARD_TIMEOUT_MS,
            required: false,
            ca_file: None,
        }
    }

    /// Checks the settings make sense; the message names the bad field.
    pub fn validate(&self) -> Result<(), String> {
        if self.syslog.is_none() && self.spool_dir.is_none() {
            return Err("audit_forward needs `syslog` or `spool_dir`".to_string());
        }
        if let Some(url) = &self.syslog {
            SyslogEndpoint::parse(url)?;
        }
        if self
            .spool_dir
            .as_deref()
            .is_some_and(|d| d.trim().is_empty())
        {
            return Err("audit_forward.spool_dir is empty".to_string());
        }
        if self.app_name.is_empty()
            || self.app_name.len() > 48
            || !self.app_name.bytes().all(|b| (33..=126).contains(&b))
        {
            return Err(format!(
                "audit_forward.app_name '{}' must be 1-48 printable ASCII characters without spaces",
                self.app_name
            ));
        }
        if !FORWARD_TIMEOUT_RANGE_MS.contains(&self.timeout_ms) {
            return Err(format!(
                "audit_forward.timeout_ms {} is outside {}..={}",
                self.timeout_ms,
                FORWARD_TIMEOUT_RANGE_MS.start(),
                FORWARD_TIMEOUT_RANGE_MS.end()
            ));
        }
        if self.ca_file.is_some()
            && !self
                .syslog
                .as_deref()
                .and_then(|u| SyslogEndpoint::parse(u).ok())
                .is_some_and(|e| e.transport == SyslogTransport::Tls)
        {
            return Err("audit_forward.ca_file only applies to a tls:// syslog target".to_string());
        }
        Ok(())
    }
}

/// Parses `rules.audit_forward` and validates it, so a bad section makes the
/// whole policy invalid (fail closed) just like an unknown rule.
fn deserialize_audit_forward<'de, D>(deserializer: D) -> Result<Option<AuditForward>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let forward = Option::<AuditForward>::deserialize(deserializer)?;
    if let Some(f) = &forward {
        f.validate().map_err(serde::de::Error::custom)?;
    }
    Ok(forward)
}

/// How syslog messages travel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyslogTransport {
    /// RFC 5426: one message per datagram.
    Udp,
    /// RFC 6587 octet counting: `LEN SP MSG`.
    Tcp,
    /// RFC 5425: octet counting inside TLS.
    Tls,
}

impl SyslogTransport {
    pub fn scheme(self) -> &'static str {
        match self {
            Self::Udp => "udp",
            Self::Tcp => "tcp",
            Self::Tls => "tls",
        }
    }

    fn default_port(self) -> u16 {
        match self {
            Self::Udp | Self::Tcp => 514,
            Self::Tls => 6514,
        }
    }
}

/// A parsed `udp://`, `tcp://` or `tls://` syslog URL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyslogEndpoint {
    pub transport: SyslogTransport,
    /// Host name or IP address (IPv6 without brackets).
    pub host: String,
    pub port: u16,
}

impl SyslogEndpoint {
    pub fn parse(url: &str) -> Result<Self, String> {
        let bad = |why: &str| format!("audit_forward.syslog '{url}': {why}");
        let (scheme, rest) = url
            .trim()
            .split_once("://")
            .ok_or_else(|| bad("expected udp://, tcp:// or tls://host[:port]"))?;
        let transport = match scheme.to_ascii_lowercase().as_str() {
            "udp" => SyslogTransport::Udp,
            "tcp" => SyslogTransport::Tcp,
            "tls" => SyslogTransport::Tls,
            _ => return Err(bad("the scheme must be udp, tcp or tls")),
        };
        let rest = rest.trim_end_matches('/');
        if rest.contains('/') || rest.contains('@') || rest.contains('?') {
            return Err(bad("only host and port are allowed"));
        }
        let (host, port) = if let Some(v6) = rest.strip_prefix('[') {
            let (host, after) = v6
                .split_once(']')
                .ok_or_else(|| bad("unclosed [ in an IPv6 address"))?;
            let port = match after.strip_prefix(':') {
                Some(p) => Some(p),
                None if after.is_empty() => None,
                None => return Err(bad("unexpected text after the IPv6 address")),
            };
            (host, port)
        } else {
            match rest.rsplit_once(':') {
                Some((host, port)) => (host, Some(port)),
                None => (rest, None),
            }
        };
        if host.is_empty() || host.contains(char::is_whitespace) {
            return Err(bad("missing host"));
        }
        let port = match port {
            Some(p) => p
                .parse::<u16>()
                .ok()
                .filter(|p| *p != 0)
                .ok_or_else(|| bad("the port must be 1-65535"))?,
            None => transport.default_port(),
        };
        Ok(Self {
            transport,
            host: host.to_string(),
            port,
        })
    }

    /// `host:port` for connecting (IPv6 in brackets).
    pub fn address(&self) -> String {
        if self.host.contains(':') {
            format!("[{}]:{}", self.host, self.port)
        } else {
            format!("{}:{}", self.host, self.port)
        }
    }
}

impl std::fmt::Display for SyslogEndpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}://{}", self.transport.scheme(), self.address())
    }
}

/// RFC 5424 facility (the PRI is `facility * 8 + severity`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SyslogFacility {
    Kern,
    User,
    Mail,
    Daemon,
    /// Security/authorization messages: the usual choice for audit trails.
    #[default]
    Auth,
    Syslog,
    Lpr,
    News,
    Uucp,
    Cron,
    #[serde(rename = "authpriv")]
    AuthPriv,
    Ftp,
    Ntp,
    /// "log audit" (13).
    Security,
    /// "log alert" (14).
    Console,
    Local0,
    Local1,
    Local2,
    Local3,
    Local4,
    Local5,
    Local6,
    Local7,
}

impl SyslogFacility {
    pub fn code(self) -> u8 {
        match self {
            Self::Kern => 0,
            Self::User => 1,
            Self::Mail => 2,
            Self::Daemon => 3,
            Self::Auth => 4,
            Self::Syslog => 5,
            Self::Lpr => 6,
            Self::News => 7,
            Self::Uucp => 8,
            Self::Cron => 9,
            Self::AuthPriv => 10,
            Self::Ftp => 11,
            Self::Ntp => 12,
            Self::Security => 13,
            Self::Console => 14,
            Self::Local0 => 16,
            Self::Local1 => 17,
            Self::Local2 => 18,
            Self::Local3 => 19,
            Self::Local4 => 20,
            Self::Local5 => 21,
            Self::Local6 => 22,
            Self::Local7 => 23,
        }
    }
}

/// Categories of cluster changes a policy can forbid.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PolicyAction {
    TopicsCreate,
    TopicsDelete,
    TopicsPurge,
    /// Partitions, replication, leader election, reassignment.
    TopicsAlter,
    /// Topic and broker configs.
    ConfigChange,
    Produce,
    /// Resetting or deleting committed offsets.
    OffsetsReset,
    /// Committing a group's offsets while consuming (`consume --group`).
    OffsetsCommit,
    GroupsDelete,
    AclChange,
    UsersChange,
    QuotaChange,
    SchemaChange,
    ConnectChange,
    /// Reading messages in a way that changes them on the broker: a
    /// RabbitMQ peek takes messages and requeues them, which marks them
    /// redelivered and can count towards a quorum queue's delivery limit.
    MessagesPeek,
    /// Moving messages between queues, e.g. redriving dead-lettered
    /// RabbitMQ messages to their original exchange.
    MessagesMove,
}

impl PolicyAction {
    pub const ALL: [PolicyAction; 16] = [
        Self::TopicsCreate,
        Self::TopicsDelete,
        Self::TopicsPurge,
        Self::TopicsAlter,
        Self::ConfigChange,
        Self::Produce,
        Self::OffsetsReset,
        Self::OffsetsCommit,
        Self::GroupsDelete,
        Self::AclChange,
        Self::UsersChange,
        Self::QuotaChange,
        Self::SchemaChange,
        Self::ConnectChange,
        Self::MessagesPeek,
        Self::MessagesMove,
    ];

    /// The name used in a policy's `forbid` list.
    pub fn id(self) -> &'static str {
        match self {
            Self::TopicsCreate => "topics.create",
            Self::TopicsDelete => "topics.delete",
            Self::TopicsPurge => "topics.purge",
            Self::TopicsAlter => "topics.alter",
            Self::ConfigChange => "config.change",
            Self::Produce => "produce",
            Self::OffsetsReset => "offsets.reset",
            Self::OffsetsCommit => "offsets.commit",
            Self::GroupsDelete => "groups.delete",
            Self::AclChange => "acl.change",
            Self::UsersChange => "users.change",
            Self::QuotaChange => "quota.change",
            Self::SchemaChange => "schema.change",
            Self::ConnectChange => "connect.change",
            Self::MessagesPeek => "messages.peek",
            Self::MessagesMove => "messages.move",
        }
    }

    pub fn from_id(id: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|a| a.id() == id)
    }

    /// Policy categories a change of this category also needs allowed: a
    /// RabbitMQ redrive moves messages (`messages.move`) by publishing them
    /// again, so `forbid: ["produce"]` refuses it too. Every front end that
    /// authorises a change checks these besides the change's own category
    /// (see `ferrika_governance::policy::extra_category_refusal`).
    pub fn also_requires(self) -> &'static [PolicyAction] {
        // No wildcard arm: a new category must decide what it also needs.
        match self {
            Self::MessagesMove => &[Self::Produce],
            Self::TopicsCreate
            | Self::TopicsDelete
            | Self::TopicsPurge
            | Self::TopicsAlter
            | Self::ConfigChange
            | Self::Produce
            | Self::OffsetsReset
            | Self::OffsetsCommit
            | Self::GroupsDelete
            | Self::AclChange
            | Self::UsersChange
            | Self::QuotaChange
            | Self::SchemaChange
            | Self::ConnectChange
            | Self::MessagesPeek => &[],
        }
    }

    /// The category of a CLI write action (as named by `main.rs`).
    pub fn from_cli(action: &str) -> Self {
        let head = action.split_whitespace().next().unwrap_or_default();
        match action {
            "topics create" | "topics clone" => Self::TopicsCreate,
            "topics delete" => Self::TopicsDelete,
            "topics purge" => Self::TopicsPurge,
            "topics config" => Self::ConfigChange,
            "offset delete-group" => Self::GroupsDelete,
            "consume --group" => Self::OffsetsCommit,
            // Publishing to a RabbitMQ exchange writes messages like
            // `produce`: one `forbid: ["produce"]` covers both brokers.
            "rabbit publish" => Self::Produce,
            // The management API has no browse: reading takes messages and
            // requeues them (marked redelivered). A redrive preview reads
            // the same way.
            "rabbit peek" | "rabbit dlq inspect" | "rabbit dlq redrive" => Self::MessagesPeek,
            "rabbit dlq redrive --execute" => Self::MessagesMove,
            _ => match head {
                "topics" | "reassign" => Self::TopicsAlter,
                "produce" | "generate" | "mirror" | "transform" | "restore" | "probe" | "bench"
                | "dlq" => Self::Produce,
                "offset" => Self::OffsetsReset,
                "acl" => Self::AclChange,
                "user" => Self::UsersChange,
                "quota" => Self::QuotaChange,
                "schema" => Self::SchemaChange,
                "connect" => Self::ConnectChange,
                // Unknown changes fall under the broadest category a policy
                // is likely to forbid.
                _ => Self::ConfigChange,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_move_needs_another_category() {
        for action in PolicyAction::ALL {
            let extra = action.also_requires();
            if action == PolicyAction::MessagesMove {
                assert_eq!(extra, &[PolicyAction::Produce]);
            } else {
                assert!(extra.is_empty(), "{action:?}");
            }
        }
    }

    #[test]
    fn action_ids_round_trip() {
        for action in PolicyAction::ALL {
            assert_eq!(PolicyAction::from_id(action.id()), Some(action));
        }
        assert_eq!(PolicyAction::from_id("topics.nuke"), None);
        assert_eq!(
            PolicyAction::from_id("messages.peek"),
            Some(PolicyAction::MessagesPeek)
        );
        assert_eq!(
            PolicyAction::from_id("messages.move"),
            Some(PolicyAction::MessagesMove)
        );
    }

    #[test]
    fn cli_actions_map_to_categories() {
        assert_eq!(
            PolicyAction::from_cli("topics delete"),
            PolicyAction::TopicsDelete
        );
        assert_eq!(
            PolicyAction::from_cli("topics purge"),
            PolicyAction::TopicsPurge
        );
        assert_eq!(
            PolicyAction::from_cli("offset reset --execute"),
            PolicyAction::OffsetsReset
        );
        assert_eq!(
            PolicyAction::from_cli("acl create"),
            PolicyAction::AclChange
        );
        assert_eq!(PolicyAction::from_cli("produce"), PolicyAction::Produce);
        assert_eq!(
            PolicyAction::from_cli("transform --execute"),
            PolicyAction::Produce
        );
        assert_eq!(
            PolicyAction::from_cli("rabbit publish"),
            PolicyAction::Produce
        );
        for peek in ["rabbit peek", "rabbit dlq inspect", "rabbit dlq redrive"] {
            assert_eq!(PolicyAction::from_cli(peek), PolicyAction::MessagesPeek);
        }
        assert_eq!(
            PolicyAction::from_cli("rabbit dlq redrive --execute"),
            PolicyAction::MessagesMove
        );
        // Kafka's redrive stays a produce.
        assert_eq!(
            PolicyAction::from_cli("dlq redrive --execute"),
            PolicyAction::Produce
        );
        assert_eq!(
            PolicyAction::from_cli("consume --group"),
            PolicyAction::OffsetsCommit
        );
        assert_eq!(
            PolicyAction::from_cli("topics config"),
            PolicyAction::ConfigChange
        );
    }

    #[test]
    fn rules_reject_unknown_fields_and_flag_unknown_categories() {
        let typo = r#"{"readonly":["*"]}"#;
        assert!(serde_json::from_str::<PolicyRules>(typo).is_err());
        let rules: PolicyRules =
            serde_json::from_str(r#"{"forbid":["topics.delete","topics.nuke","*"]}"#).unwrap();
        assert_eq!(rules.unknown_forbid_entries(), vec!["topics.nuke"]);
    }

    #[test]
    fn rules_without_audit_forward_parse_as_before() {
        let rules: PolicyRules =
            serde_json::from_str(r#"{"audit":"required","masking":"enforced"}"#).unwrap();
        assert_eq!(rules.audit_forward, None);
        assert_eq!(rules.audit, AuditRule::Required);
        // Serialising an old policy does not invent the new field.
        assert!(!serde_json::to_string(&rules)
            .unwrap()
            .contains("audit_forward"));
        assert!(
            serde_json::from_str::<PolicyRules>(r#"{"audit_forward":null}"#)
                .unwrap()
                .audit_forward
                .is_none()
        );
    }

    #[test]
    fn audit_forward_parses_with_defaults() {
        let rules: PolicyRules =
            serde_json::from_str(r#"{"audit_forward":{"syslog":"tcp://siem.corp:6601"}}"#).unwrap();
        let forward = rules.audit_forward.unwrap();
        assert_eq!(forward.facility, SyslogFacility::Auth);
        assert_eq!(forward.app_name, "ferrika");
        assert_eq!(forward.timeout_ms, DEFAULT_FORWARD_TIMEOUT_MS);
        assert!(!forward.required);
        let endpoint = SyslogEndpoint::parse(forward.syslog.as_deref().unwrap()).unwrap();
        assert_eq!(endpoint.transport, SyslogTransport::Tcp);
        assert_eq!(endpoint.address(), "siem.corp:6601");

        let full: AuditForward = serde_json::from_str(
            r#"{"syslog":"tls://[::1]","spool_dir":"C:\\ProgramData\\Ferrika\\spool",
                "facility":"local4","app_name":"ferrika-dev","timeout_ms":250,
                "required":true,"ca_file":"ca.pem"}"#,
        )
        .unwrap();
        assert!(full.validate().is_ok());
        assert_eq!(full.facility.code(), 20);
        let endpoint = SyslogEndpoint::parse(full.syslog.as_deref().unwrap()).unwrap();
        assert_eq!(endpoint.port, 6514);
        assert_eq!(endpoint.to_string(), "tls://[::1]:6514");
        assert_eq!(
            serde_json::from_str::<SyslogFacility>(r#""authpriv""#).unwrap(),
            SyslogFacility::AuthPriv
        );
    }

    #[test]
    fn bad_audit_forward_sections_are_refused() {
        for bad in [
            r#"{"audit_forward":{}}"#,
            r#"{"audit_forward":{"syslog":"http://siem:514"}}"#,
            r#"{"audit_forward":{"syslog":"tcp://:514"}}"#,
            r#"{"audit_forward":{"syslog":"tcp://siem:99999"}}"#,
            r#"{"audit_forward":{"syslog":"udp://siem","facility":"nope"}}"#,
            r#"{"audit_forward":{"syslog":"udp://siem","app_name":"has space"}}"#,
            r#"{"audit_forward":{"syslog":"udp://siem","timeout_ms":60000}}"#,
            r#"{"audit_forward":{"syslog":"udp://siem","ca_file":"ca.pem"}}"#,
            r#"{"audit_forward":{"spool_dir":"  "}}"#,
            r#"{"audit_forward":{"syslog":"udp://siem","requried":true}}"#,
        ] {
            assert!(serde_json::from_str::<PolicyRules>(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn audit_anchor_rule_is_optional_and_omitted_when_off() {
        let v1: PolicyRules = serde_json::from_str(r#"{"audit":"required"}"#).unwrap();
        assert_eq!(v1.audit_anchor, AuditAnchorRule::None);
        assert!(!v1.anchor_required);
        let text = serde_json::to_string(&v1).unwrap();
        assert!(!text.contains("audit_anchor") && !text.contains("anchor_required"));

        let v2: PolicyRules =
            serde_json::from_str(r#"{"audit_anchor":"os_log","anchor_required":true}"#).unwrap();
        assert_eq!(v2.audit_anchor, AuditAnchorRule::OsLog);
        assert!(v2.anchor_required);
        assert!(serde_json::from_str::<PolicyRules>(r#"{"audit_anchor":"http"}"#).is_err());
    }

    #[test]
    fn payload_format_follows_the_fields_used() {
        let v1: PolicyPayload = serde_json::from_str(r#"{"issuer":"x","rules":{}}"#).unwrap();
        assert!(!v1.uses_v2());
        assert_eq!(v1.format(), POLICY_FORMAT_V1);
        let v2: PolicyPayload =
            serde_json::from_str(r#"{"expires_at":"2027-01-01T00:00:00Z"}"#).unwrap();
        assert_eq!(v2.format(), POLICY_FORMAT_V2);
        let v2: PolicyPayload = serde_json::from_str(r#"{"on_expiry":"strictest"}"#).unwrap();
        assert_eq!(v2.on_expiry, Some(ExpiryRule::Strictest));
        assert_eq!(v2.format(), POLICY_FORMAT_V2);
        // Still strict about everything else.
        assert!(serde_json::from_str::<PolicyPayload>(r#"{"expires":"2027"}"#).is_err());
        assert!(serde_json::from_str::<PolicyPayload>(r#"{"on_expiry":"ignore"}"#).is_err());
        let anchored: PolicyPayload =
            serde_json::from_str(r#"{"rules":{"audit_anchor":"os_log"}}"#).unwrap();
        assert_eq!(anchored.format(), POLICY_FORMAT_V2);
        let brokers: PolicyPayload =
            serde_json::from_str(r#"{"rules":{"read_only_brokers":["*.prod.corp"]}}"#).unwrap();
        assert_eq!(
            brokers.rules.read_only_brokers,
            vec!["*.prod.corp".to_string()]
        );
        assert_eq!(brokers.format(), POLICY_FORMAT_V2);
    }

    #[test]
    fn read_only_brokers_is_omitted_when_empty() {
        let v1: PolicyRules = serde_json::from_str(r#"{"read_only":["prod-*"]}"#).unwrap();
        assert!(v1.read_only_brokers.is_empty());
        let text = serde_json::to_string(&v1).unwrap();
        assert!(!text.contains("read_only_brokers"), "{text}");
        let v2 = PolicyRules {
            read_only_brokers: vec!["10.0.0.*".into()],
            ..v1
        };
        assert!(serde_json::to_string(&v2)
            .unwrap()
            .contains(r#""read_only_brokers":["10.0.0.*"]"#));
    }

    #[test]
    fn export_and_watermark_rules_parse_and_are_omitted_when_default() {
        let v1: PolicyRules = serde_json::from_str(r#"{"masking":"enforced"}"#).unwrap();
        assert_eq!(v1.export, ExportRule::Allowed);
        assert!(v1.export_clusters.is_empty());
        assert_eq!(v1.watermark, WatermarkRule::Off);
        assert_eq!(v1.watermark_text, None);
        let text = serde_json::to_string(&v1).unwrap();
        assert!(
            !text.contains("export") && !text.contains("watermark"),
            "{text}"
        );

        let v2: PolicyRules = serde_json::from_str(
            r#"{"export":"forbidden","export_clusters":["*.prod.corp:*"],
                "watermark":"on","watermark_text":"대외비"}"#,
        )
        .unwrap();
        assert_eq!(v2.export, ExportRule::Forbidden);
        assert_eq!(v2.export_clusters, vec!["*.prod.corp:*".to_string()]);
        assert_eq!(v2.watermark, WatermarkRule::On);
        assert_eq!(v2.watermark_text.as_deref(), Some("대외비"));
        assert_eq!(v2.export_rules_error(), None);
        let audited: PolicyRules = serde_json::from_str(r#"{"export":"audited"}"#).unwrap();
        assert_eq!(audited.export, ExportRule::Audited);

        // Typos are refused, so the policy fails closed.
        for bad in [
            r#"{"export":"forbiden"}"#,
            r#"{"watermark":"yes"}"#,
            r#"{"export_cluster":["prod"]}"#,
        ] {
            assert!(serde_json::from_str::<PolicyRules>(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn mcp_write_rule_parses_needs_format_2_and_is_omitted_when_allowed() {
        let parse = |json: &str| serde_json::from_str::<PolicyRules>(json);
        assert_eq!(parse("{}").unwrap().mcp_write, McpWriteRule::Allowed);
        for (text, rule) in [
            ("confirm_human", McpWriteRule::ConfirmHuman),
            ("forbidden", McpWriteRule::Forbidden),
        ] {
            let rules = parse(&format!(r#"{{"mcp_write":"{text}"}}"#)).unwrap();
            assert_eq!(rules.mcp_write, rule);
            let payload = PolicyPayload {
                rules,
                ..serde_json::from_str("{}").unwrap()
            };
            assert_eq!(payload.format(), POLICY_FORMAT_V2);
        }
        assert!(parse(r#"{"mcp_write":"human"}"#).is_err());
        let text = serde_json::to_string(&PolicyRules::default()).unwrap();
        assert!(!text.contains("mcp_write"), "{text}");
    }

    #[test]
    fn screen_capture_rule_parses_needs_format_2_and_is_omitted_when_allowed() {
        let parse = |json: &str| serde_json::from_str::<PolicyRules>(json);
        assert_eq!(
            parse("{}").unwrap().screen_capture,
            ScreenCaptureRule::Allowed
        );
        let blocked = parse(r#"{"screen_capture":"blocked"}"#).unwrap();
        assert_eq!(blocked.screen_capture, ScreenCaptureRule::Blocked);
        assert!(parse(r#"{"screen_capture":"off"}"#).is_err());
        let payload = |rules: PolicyRules| PolicyPayload {
            rules,
            ..serde_json::from_str("{}").unwrap()
        };
        assert_eq!(payload(blocked.clone()).format(), POLICY_FORMAT_V2);
        assert_eq!(payload(PolicyRules::default()).format(), POLICY_FORMAT_V1);
        let text = serde_json::to_string(&PolicyRules::default()).unwrap();
        assert!(!text.contains("screen_capture"), "{text}");
        // An untrusted policy does not block: people need to show it to support.
        assert!(PolicyRules::strictest().screen_capture.is_allowed());
    }

    #[test]
    fn bad_export_clusters_and_watermark_text_are_reported() {
        let rules = |json: &str| serde_json::from_str::<PolicyRules>(json).unwrap();
        for bad in [
            r#"{"export_clusters":["prod.corp",""]}"#,
            r#"{"export_clusters":["  "]}"#,
            r#"{"watermark_text":""}"#,
            r#"{"watermark_text":"   "}"#,
            r#"{"watermark_text":"line\nbreak"}"#,
            r#"{"watermark_text":"12345678901234567890123456789012345678901"}"#,
        ] {
            assert!(rules(bad).export_rules_error().is_some(), "{bad}");
        }
        let forty = format!(
            r#"{{"watermark_text":"{}"}}"#,
            "가".repeat(WATERMARK_TEXT_MAX)
        );
        assert_eq!(rules(&forty).export_rules_error(), None);
    }

    #[test]
    fn export_and_watermark_rules_need_format_2() {
        for json in [
            r#"{"rules":{"export":"audited"}}"#,
            r#"{"rules":{"export_clusters":["prod.corp"]}}"#,
            r#"{"rules":{"watermark":"on"}}"#,
            r#"{"rules":{"watermark_text":"x"}}"#,
        ] {
            let payload: PolicyPayload = serde_json::from_str(json).unwrap();
            assert_eq!(payload.format(), POLICY_FORMAT_V2, "{json}");
        }
        // The defaults written out are still format 1.
        let defaults: PolicyPayload =
            serde_json::from_str(r#"{"rules":{"export":"allowed","watermark":"off"}}"#).unwrap();
        assert_eq!(defaults.format(), POLICY_FORMAT_V1);
    }

    const ENVIRONMENTS: &str = r#"{
        "environments": {
            "prod": {"tier": 3, "profiles": ["prod-*"], "brokers": ["*.prod.corp"],
                     "forbid": ["topics.delete"]},
            "staging": {"tier": 2, "brokers": ["*.staging.corp"], "read_only": true},
            "dev": {"tier": 1}
        },
        "default_environment": "dev",
        "copy_down": "masked"
    }"#;

    #[test]
    fn environments_parse_and_need_format_2() {
        let rules: PolicyRules = serde_json::from_str(ENVIRONMENTS).unwrap();
        assert_eq!(rules.environments.len(), 3);
        let prod = &rules.environments["prod"];
        assert_eq!(prod.tier, 3);
        assert_eq!(prod.profiles, vec!["prod-*".to_string()]);
        assert!(!prod.read_only && prod.restricts_changes());
        assert!(rules.environments["staging"].read_only);
        assert!(!rules.environments["dev"].restricts_changes());
        assert_eq!(rules.default_environment.as_deref(), Some("dev"));
        assert_eq!(rules.copy_down, CopyDownRule::Masked);
        assert_eq!(rules.environment_rules_error(), None);
        assert!(rules.unknown_forbid_entries().is_empty());

        let payload = PolicyPayload {
            rules,
            ..PolicyPayload::default()
        };
        assert_eq!(payload.format(), POLICY_FORMAT_V2);
        for json in [
            r#"{"rules":{"environments":{"dev":{"tier":1}}}}"#,
            r#"{"rules":{"copy_down":"forbid"}}"#,
            r#"{"rules":{"default_environment":"dev"}}"#,
        ] {
            let payload: PolicyPayload = serde_json::from_str(json).unwrap();
            assert_eq!(payload.format(), POLICY_FORMAT_V2, "{json}");
        }
        // The defaults are not written out and stay format 1.
        let v1: PolicyRules = serde_json::from_str(r#"{"copy_down":"allow"}"#).unwrap();
        let text = serde_json::to_string(&v1).unwrap();
        assert!(
            !text.contains("environment") && !text.contains("copy_down"),
            "{text}"
        );
    }

    #[test]
    fn bad_environment_sections_are_refused() {
        // Typos are refused while parsing, so the policy fails closed.
        for bad in [
            r#"{"environments":{"prod":{"tier":3,"readonly":true}}}"#,
            r#"{"environments":{"prod":{"profiles":["prod-*"]}}}"#,
            r#"{"environments":{"prod":{"tier":-1}}}"#,
            r#"{"copy_down":"masked_only"}"#,
            r#"{"environment":{}}"#,
        ] {
            assert!(serde_json::from_str::<PolicyRules>(bad).is_err(), "{bad}");
        }
        let rules = |json: &str| serde_json::from_str::<PolicyRules>(json).unwrap();
        for bad in [
            r#"{"environments":{"":{"tier":1}}}"#,
            r#"{"environments":{"pro d":{"tier":1}}}"#,
            r#"{"environments":{"prod":{"tier":1,"brokers":[" "]}}}"#,
            r#"{"environments":{"prod":{"tier":1}},"default_environment":"dev"}"#,
            r#"{"copy_down":"forbid"}"#,
        ] {
            assert!(rules(bad).environment_rules_error().is_some(), "{bad}");
        }
        assert_eq!(
            rules(r#"{"environments":{"prod":{"tier":3,"forbid":["topics.nuke"]}}}"#)
                .unknown_forbid_entries(),
            vec!["prod: topics.nuke"]
        );
    }

    #[test]
    fn strictest_forbids_export_and_turns_the_watermark_on() {
        let rules = PolicyRules::strictest();
        assert_eq!(rules.export, ExportRule::Forbidden);
        assert!(rules.export_clusters.is_empty());
        assert_eq!(rules.watermark, WatermarkRule::On);
        assert_eq!(rules.watermark_text, None);
        assert!(rules.environments.is_empty());
        assert_eq!(rules.copy_down, CopyDownRule::Allow);
    }
}
