//! `ferrika policy`: create, sign, verify and show the signed company policy.

use std::path::{Path, PathBuf};

use clap::{Args, Subcommand};
use colored::Colorize;

use crate::cli::{output, GlobalOpts};
use crate::governance::policy::{
    self, policy_dir, read_public_key, EffectivePolicy, ExpiryState, PolicyExpiry, PolicyStatus,
    TrustedKey, KEY_LIST_FILE, POLICY_FILE, PRIVATE_KEY_FILE, PUBLIC_KEY_FILE,
};
use crate::model::policy::{ExpiryRule, PolicyAction, PolicyRules, WatermarkRule};

#[derive(Args, Debug, Clone)]
pub struct PolicyArgs {
    #[command(subcommand)]
    pub command: PolicyCommand,
}

#[derive(Subcommand, Debug, Clone)]
pub enum PolicyCommand {
    /// Create a signing key pair: <out>/policy-signing.pk8 (keep it secret) and <out>/policy.pub
    Keygen {
        /// Directory to write the keys to
        #[arg(long)]
        out: PathBuf,
        /// Overwrite existing key files
        #[arg(long)]
        force: bool,
    },
    /// Sign a policy JSON file into a company.policy
    Sign {
        /// Private key from `policy keygen`
        #[arg(long)]
        key: PathBuf,
        /// Policy JSON (see `ferrika policy example`)
        #[arg(long = "in")]
        input: PathBuf,
        /// Signed policy file to write
        #[arg(long, default_value = POLICY_FILE)]
        out: PathBuf,
    },
    /// Verify a signed policy (defaults to the installed one)
    Verify {
        /// Signed policy file
        file: Option<PathBuf>,
        /// Trusted public key (defaults to policy.pub in the policy directory)
        #[arg(long = "pub")]
        public_key: Option<PathBuf>,
    },
    /// Show the policy this machine enforces
    Show,
    /// Print an example policy JSON to start from
    Example {
        /// Write it to this file (UTF-8) instead of stdout
        #[arg(long)]
        out: Option<PathBuf>,
    },
}

const EXAMPLE: &str = r#"{
  "issuer": "보안팀",
  "issued_at": "2026-11-02",
  "rules": {
    "read_only": ["prod-*"],
    "forbid": ["topics.delete", "topics.purge", "offsets.reset", "acl.change"],
    "masking": "enforced",
    "unmask": "with_reason",
    "audit": "required"
  }
}
"#;

pub fn run(args: PolicyArgs, global: &GlobalOpts) -> anyhow::Result<()> {
    match args.command {
        PolicyCommand::Keygen { out, force } => keygen(&out, force),
        PolicyCommand::Sign { key, input, out } => sign(&key, &input, &out),
        PolicyCommand::Verify { file, public_key } => verify(file, public_key, global),
        PolicyCommand::Show => {
            show(policy::current(), global);
            Ok(())
        }
        PolicyCommand::Example { out: None } => {
            print!("{EXAMPLE}");
            Ok(())
        }
        // A file, because Windows PowerShell 5 re-encodes piped output.
        PolicyCommand::Example { out: Some(path) } => {
            std::fs::write(&path, EXAMPLE)?;
            println!("{} {}", "✔ Wrote".green().bold(), path.display());
            Ok(())
        }
    }
}

fn keygen(out: &Path, force: bool) -> anyhow::Result<()> {
    let private_path = out.join(PRIVATE_KEY_FILE);
    let public_path = out.join(PUBLIC_KEY_FILE);
    // `symlink_metadata`: a dangling symlink is something there too.
    let taken = |p: &Path| std::fs::symlink_metadata(p).is_ok();
    if !force && (taken(&private_path) || taken(&public_path)) {
        anyhow::bail!(
            "{} or {} already exists (use --force to replace them)",
            private_path.display(),
            public_path.display()
        );
    }
    std::fs::create_dir_all(out)?;
    let (private, public) = policy::generate_keypair()?;
    write_key_file(&private_path, &private, true, force)?;
    write_key_file(
        &public_path,
        (policy::to_hex(&public) + "\n").as_bytes(),
        false,
        force,
    )?;
    println!(
        "{} key {}",
        "✔ Created signing".green().bold(),
        policy::key_id(&public).cyan()
    );
    println!(
        "  private: {}  {}",
        private_path.display(),
        "(keep secret; only the security team signs)".yellow()
    );
    println!(
        "  public:  {}  (install next to company.policy in {})",
        public_path.display(),
        policy_dir().display()
    );
    println!(
        "  {}",
        format!(
            "Rotating keys? Add the new key as a line of {KEY_LIST_FILE} instead (see docs/POLICY.md)."
        )
        .bright_black()
    );
    Ok(())
}

/// Creates `path` holding `bytes`. A private key is born with mode 0600 on
/// unix (never world-readable, not even briefly). An existing file is only
/// replaced with `force`, through a new file renamed over it; a symlink is
/// refused either way, so a link cannot redirect the key elsewhere.
fn write_key_file(path: &Path, bytes: &[u8], private: bool, force: bool) -> std::io::Result<()> {
    use std::io::{Error, ErrorKind, Write};
    let replace = match std::fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_symlink() => {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "{} is a symbolic link; refusing to write through it",
                    path.display()
                ),
            ));
        }
        Ok(_) if !force => {
            return Err(Error::new(
                ErrorKind::AlreadyExists,
                format!("{} already exists", path.display()),
            ));
        }
        Ok(_) => true,
        Err(e) if e.kind() == ErrorKind::NotFound => false,
        Err(e) => return Err(e),
    };
    // `create_new` (O_EXCL) never follows a symlink planted in the meantime.
    let write_new = |target: &Path| -> std::io::Result<()> {
        let mut file = open_new(target, private)?;
        file.write_all(bytes)?;
        file.sync_all()
    };
    if !replace {
        return write_new(path);
    }
    let name = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    let temp = path.with_file_name(format!(".{name}.tmp{}", std::process::id()));
    let _ = std::fs::remove_file(&temp);
    let result = write_new(&temp).and_then(|()| std::fs::rename(&temp, path));
    if result.is_err() {
        let _ = std::fs::remove_file(&temp);
    }
    result
}

#[cfg(unix)]
fn open_new(path: &Path, private: bool) -> std::io::Result<std::fs::File> {
    use std::os::unix::fs::OpenOptionsExt;
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(if private { 0o600 } else { 0o644 })
        .open(path)
}

#[cfg(not(unix))]
fn open_new(path: &Path, _private: bool) -> std::io::Result<std::fs::File> {
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
}

fn sign(key: &Path, input: &Path, out: &Path) -> anyhow::Result<()> {
    let pkcs8 =
        std::fs::read(key).map_err(|e| anyhow::anyhow!("could not read {}: {e}", key.display()))?;
    let payload = std::fs::read_to_string(input)
        .map_err(|e| anyhow::anyhow!("could not read {}: {e}", input.display()))?;
    let payload = payload.trim_start_matches('\u{feff}').trim();
    let signed = policy::sign(&pkcs8, payload)?;
    let parsed = policy::parse_payload(payload)?;
    std::fs::write(out, serde_json::to_string_pretty(&signed)? + "\n")?;
    println!(
        "{} {} (issuer {}, key {}, format {})",
        "✔ Signed".green().bold(),
        out.display(),
        if parsed.issuer.is_empty() {
            "-"
        } else {
            &parsed.issuer
        },
        signed.key_id.cyan(),
        signed.format
    );
    if let Ok(Some(expiry)) = policy::parse_expiry(&parsed) {
        println!("  {}", expiry_line(&expiry, jiff::Timestamp::now()));
    }
    Ok(())
}

fn verify(
    file: Option<PathBuf>,
    public_key: Option<PathBuf>,
    global: &GlobalOpts,
) -> anyhow::Result<()> {
    let dir = policy_dir();
    let file = file.unwrap_or_else(|| dir.join(POLICY_FILE));
    // `--pub` checks against that one key; by default against every key the
    // policy directory trusts (policy.pub and policy.keys).
    let keys = match public_key {
        Some(path) => read_public_key(&path).map(|key| {
            vec![TrustedKey {
                key_id: policy::key_id(&key),
                public_key: key,
                label: String::new(),
                file: path.display().to_string(),
            }]
        }),
        None => policy::trusted_keys(&dir).and_then(|keys| {
            if keys.is_empty() {
                Err(policy::PolicyError::Missing(
                    dir.join(PUBLIC_KEY_FILE).display().to_string(),
                ))
            } else {
                Ok(keys)
            }
        }),
    };
    let result = keys.and_then(|keys| {
        let text = std::fs::read_to_string(&file).map_err(|source| policy::PolicyError::Read {
            path: file.display().to_string(),
            source,
        })?;
        policy::verify_with(text.trim_start_matches('\u{feff}'), &keys)
    });
    if global.wants_json(false) {
        let value = match &result {
            Ok((payload, key_id)) => serde_json::json!({
                "valid": true, "file": file, "format": payload.format(), "key_id": key_id,
                "issuer": payload.issuer, "issued_at": payload.issued_at,
                "expires_at": payload.expires_at,
                "on_expiry": payload.expires_at.as_ref().map(|_| payload.on_expiry.unwrap_or_default()),
                "rules": payload.rules,
            }),
            Err(e) => serde_json::json!({ "valid": false, "file": file, "error": e.to_string() }),
        };
        output::print_json(&value)?;
    }
    match result {
        Ok((payload, key_id)) => {
            if !global.wants_json(false) {
                println!(
                    "{} {} — signed by key {} (issuer {}, issued {})",
                    "✔ Signature valid:".green().bold(),
                    file.display(),
                    key_id.cyan(),
                    payload.issuer,
                    payload.issued_at
                );
                if let Ok(Some(expiry)) = policy::parse_expiry(&payload) {
                    println!("  {}", expiry_line(&expiry, jiff::Timestamp::now()));
                }
            }
            Ok(())
        }
        Err(e) => anyhow::bail!("Policy {} is NOT valid: {e}", file.display()),
    }
}

/// `expires 2027-06-30T23:59:59+09:00 (on expiry: warn)`, with how it stands
/// at `now` once it is due.
fn expiry_line(expiry: &PolicyExpiry, now: jiff::Timestamp) -> String {
    let rule = match expiry.on_expiry {
        ExpiryRule::Warn => "warn",
        ExpiryRule::Strictest => "strictest",
    };
    let state = match expiry.state_at(now) {
        ExpiryState::Current => String::new(),
        ExpiryState::ExpiresSoon { days_left: 1 } => " · expires in 1 day".to_string(),
        ExpiryState::ExpiresSoon { days_left } => format!(" · expires in {days_left} days"),
        ExpiryState::ExpiredWarn => " · EXPIRED, rules still apply".to_string(),
        ExpiryState::ExpiredStrictest => " · EXPIRED, strictest mode".to_string(),
    };
    format!(
        "expires:           {} (on expiry: {rule}){state}",
        expiry.text
    )
}

/// `trusted keys:      <id> (policy.pub) · signed this policy`, then one
/// aligned line per further key.
fn trusted_key_lines(keys: &[TrustedKey], signer: Option<&str>) -> Vec<String> {
    if keys.is_empty() {
        return vec!["trusted keys:      -".to_string()];
    }
    keys.iter()
        .enumerate()
        .map(|(i, key)| {
            let head = if i == 0 {
                "trusted keys:     "
            } else {
                "                  "
            };
            let source = if key.label.is_empty() {
                key.file.clone()
            } else {
                format!("{}: {}", key.file, key.label)
            };
            let signed = if signer == Some(key.key_id.as_str()) {
                " · signed this policy"
            } else {
                ""
            };
            format!("{head} {} ({source}){signed}", key.key_id)
        })
        .collect()
}

/// The read-only rules for `policy show`. `read-only brokers` only when the
/// policy has that (format 2) rule, so format 1 output is unchanged.
fn read_only_lines(rules: &PolicyRules) -> Vec<String> {
    let list = |items: &[String]| {
        if items.is_empty() {
            "-".to_string()
        } else {
            items.join(", ")
        }
    };
    let mut lines = vec![format!("read-only targets: {}", list(&rules.read_only))];
    if !rules.read_only_brokers.is_empty() {
        lines.push(format!(
            "read-only brokers: {} (broker addresses only)",
            list(&rules.read_only_brokers)
        ));
    }
    lines
}

/// The export and watermark rules for `policy show`, always listed (their
/// defaults say what is not enforced).
fn export_lines(rules: &PolicyRules) -> Vec<String> {
    let name = |v: serde_json::Value| v.as_str().unwrap_or_default().to_string();
    let mut export = format!(
        "record export:     {}",
        name(serde_json::json!(rules.export))
    );
    if !rules.export.is_allowed() && !rules.export_clusters.is_empty() {
        export.push_str(&format!(" on brokers {}", rules.export_clusters.join(", ")));
    }
    let mut watermark = format!(
        "watermark:         {}",
        name(serde_json::json!(rules.watermark))
    );
    if let (WatermarkRule::On, Some(text)) = (rules.watermark, &rules.watermark_text) {
        watermark.push_str(&format!(" (\"{text}\")"));
    }
    let capture = format!(
        "screen capture:    {}",
        name(serde_json::json!(rules.screen_capture))
    );
    let mcp = format!(
        "AI (MCP) writes:   {}",
        name(serde_json::json!(rules.mcp_write))
    );
    vec![export, watermark, capture, mcp]
}

/// `rules` for `-f json policy show`: as signed, plus `export`,
/// `export_clusters` and `watermark` even at their defaults, which the
/// payload leaves out.
fn rules_json(rules: &PolicyRules) -> serde_json::Value {
    let mut value = serde_json::json!(rules);
    if let Some(map) = value.as_object_mut() {
        map.entry("export")
            .or_insert_with(|| serde_json::json!(rules.export));
        map.entry("export_clusters")
            .or_insert_with(|| serde_json::json!(rules.export_clusters));
        map.entry("watermark")
            .or_insert_with(|| serde_json::json!(rules.watermark));
        map.entry("screen_capture")
            .or_insert_with(|| serde_json::json!(rules.screen_capture));
        map.entry("mcp_write")
            .or_insert_with(|| serde_json::json!(rules.mcp_write));
    }
    value
}

fn show(policy: &EffectivePolicy, global: &GlobalOpts) {
    let now = jiff::Timestamp::now();
    if global.wants_json(false) {
        let (status, issuer, key_id, error) = match &policy.status {
            PolicyStatus::Absent => ("absent", None, None, None),
            PolicyStatus::Verified { issuer, key_id } => {
                ("verified", Some(issuer), Some(key_id), None)
            }
            PolicyStatus::Invalid { reason } => ("invalid", None, None, Some(reason)),
        };
        let expiry_state = match policy.expiry_state_at(now) {
            ExpiryState::Current => "current",
            ExpiryState::ExpiresSoon { .. } => "expires_soon",
            ExpiryState::ExpiredWarn => "expired",
            ExpiryState::ExpiredStrictest => "expired_strictest",
        };
        // `rules` are the ones in force now (the strictest once an expired
        // policy says so).
        let value = serde_json::json!({
            "status": status, "dir": policy.dir, "issuer": issuer, "key_id": key_id,
            "issued_at": policy.issued_at, "error": error,
            "expires_at": policy.expiry.as_ref().map(|e| &e.text),
            "on_expiry": policy.expiry.as_ref().map(|e| e.on_expiry),
            "expiry": policy.expiry.as_ref().map(|_| expiry_state),
            "strictest": policy.is_strictest_at(now),
            "rules": rules_json(&policy.rules_at(now)),
            "trusted_keys": policy.trusted_keys.iter().map(|k| serde_json::json!({
                "key_id": k.key_id, "file": k.file, "label": k.label,
            })).collect::<Vec<_>>(),
        });
        if let Err(e) = output::print_json(&value) {
            crate::cli::exit_with_error(&e);
        }
        return;
    }
    println!("Policy directory: {}", policy.dir.display());
    match &policy.status {
        PolicyStatus::Absent => {
            println!(
                "{}",
                "No company policy installed: nothing is enforced.".bright_black()
            );
            return;
        }
        PolicyStatus::Verified { issuer, key_id } => println!(
            "{} {} (key {}, issued {})",
            "✔ Company policy active · signature verified:"
                .green()
                .bold(),
            issuer,
            key_id.cyan(),
            policy.issued_at
        ),
        PolicyStatus::Invalid { reason } => {
            println!(
                "{} {}",
                "✖ Policy signature invalid · strictest mode:".red().bold(),
                reason
            );
        }
    }
    if let Some(expiry) = &policy.expiry {
        let line = format!("  {}", expiry_line(expiry, now));
        match policy.expiry_state_at(now) {
            ExpiryState::Current => println!("{line}"),
            ExpiryState::ExpiresSoon { .. } | ExpiryState::ExpiredWarn => {
                println!("{}", line.yellow())
            }
            ExpiryState::ExpiredStrictest => println!("{}", line.red().bold()),
        }
    }
    let signer = match &policy.status {
        PolicyStatus::Verified { key_id, .. } => Some(key_id.as_str()),
        _ => None,
    };
    for line in trusted_key_lines(&policy.trusted_keys, signer) {
        println!("  {line}");
    }
    // The rules in force now: the strictest ones once an expired policy says so.
    let rules = policy.rules_at(now);
    let list = |items: &[String]| {
        if items.is_empty() {
            "-".to_string()
        } else {
            items.join(", ")
        }
    };
    for line in read_only_lines(&rules) {
        println!("  {line}");
    }
    let forbidden: Vec<String> = if rules.forbid.iter().any(|f| f == "*") {
        vec!["* (every change)".to_string()]
    } else {
        rules.forbid.clone()
    };
    println!("  forbidden actions: {}", list(&forbidden));
    let name = |v: serde_json::Value| v.as_str().unwrap_or_default().to_string();
    println!(
        "  PII masking:       {}",
        name(serde_json::json!(rules.masking))
    );
    println!(
        "  unmasking:         {}",
        name(serde_json::json!(rules.unmask))
    );
    println!(
        "  audit:             {}",
        name(serde_json::json!(rules.audit))
    );
    for line in export_lines(&rules) {
        println!("  {line}");
    }
    if let Some(forward) = &rules.audit_forward {
        let targets: Vec<&str> = [forward.syslog.as_deref(), forward.spool_dir.as_deref()]
            .into_iter()
            .flatten()
            .collect();
        println!(
            "  audit forwarding:  {}{}",
            targets.join(", "),
            if forward.required { " (required)" } else { "" }
        );
    }
    println!(
        "  {}",
        format!(
            "categories: {}",
            PolicyAction::ALL.map(PolicyAction::id).join(", ")
        )
        .bright_black()
    );
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    mod key_files {
        use super::super::write_key_file;
        use std::os::unix::fs::{symlink, PermissionsExt};

        fn dir(name: &str) -> std::path::PathBuf {
            let dir = std::env::temp_dir().join(format!(
                "ferrika-keygen-{name}-{}-{}",
                std::process::id(),
                crate::governance::audit::now_ms()
            ));
            std::fs::create_dir_all(&dir).unwrap();
            dir
        }

        fn mode(path: &std::path::Path) -> u32 {
            std::fs::metadata(path).unwrap().permissions().mode() & 0o777
        }

        #[test]
        fn private_key_is_created_0600_and_replaced_only_with_force() {
            let dir = dir("mode");
            let path = dir.join("policy.key");
            write_key_file(&path, b"one", true, false).unwrap();
            assert_eq!(mode(&path), 0o600);
            assert!(write_key_file(&path, b"two", true, false).is_err());
            assert_eq!(std::fs::read(&path).unwrap(), b"one");
            write_key_file(&path, b"three", true, true).unwrap();
            assert_eq!(std::fs::read(&path).unwrap(), b"three");
            assert_eq!(mode(&path), 0o600);
            // No temporary file is left behind.
            assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 1);
            let public = dir.join("policy.pub");
            write_key_file(&public, b"ab\n", false, false).unwrap();
            assert_eq!(mode(&public), 0o644);
            let _ = std::fs::remove_dir_all(&dir);
        }

        #[test]
        fn symlinks_are_refused_with_and_without_force() {
            let dir = dir("link");
            let victim = dir.join("victim");
            std::fs::write(&victim, "keep").unwrap();
            let link = dir.join("policy.key");
            symlink(&victim, &link).unwrap();
            for force in [false, true] {
                let err = write_key_file(&link, b"secret", true, force).unwrap_err();
                assert!(err.to_string().contains("symbolic link"), "{err}");
                assert_eq!(std::fs::read(&victim).unwrap(), b"keep");
            }
            // A dangling link is refused too, and nothing is created behind it.
            let dangling = dir.join("dangling.key");
            symlink(dir.join("nowhere"), &dangling).unwrap();
            assert!(write_key_file(&dangling, b"secret", true, true).is_err());
            assert!(!dir.join("nowhere").exists());
            let _ = std::fs::remove_dir_all(&dir);
        }
    }

    use super::*;

    fn key(id: &str, file: &str, label: &str) -> TrustedKey {
        TrustedKey {
            key_id: id.into(),
            public_key: Vec::new(),
            label: label.into(),
            file: file.into(),
        }
    }

    #[test]
    fn show_lists_read_only_brokers_only_when_set() {
        let v1 = PolicyRules {
            read_only: vec!["prod-*".into()],
            ..PolicyRules::default()
        };
        assert_eq!(read_only_lines(&v1), vec!["read-only targets: prod-*"]);
        let v2 = PolicyRules {
            read_only_brokers: vec!["*.prod.corp".into(), "10.20.*".into()],
            ..PolicyRules::default()
        };
        assert_eq!(
            read_only_lines(&v2),
            vec![
                "read-only targets: -",
                "read-only brokers: *.prod.corp, 10.20.* (broker addresses only)",
            ]
        );
    }

    #[test]
    fn show_lists_export_and_watermark() {
        let v1 = PolicyRules::default();
        assert_eq!(
            export_lines(&v1),
            vec![
                "record export:     allowed",
                "watermark:         off",
                "screen capture:    allowed",
                "AI (MCP) writes:   allowed"
            ]
        );
        let v2 = PolicyRules {
            export: crate::model::policy::ExportRule::Forbidden,
            export_clusters: vec!["*.prod.corp".into(), "10.20.*".into()],
            watermark: WatermarkRule::On,
            watermark_text: Some("대외비".into()),
            screen_capture: crate::model::policy::ScreenCaptureRule::Blocked,
            ..PolicyRules::default()
        };
        assert_eq!(
            export_lines(&v2),
            vec![
                "record export:     forbidden on brokers *.prod.corp, 10.20.*",
                "watermark:         on (\"대외비\")",
                "screen capture:    blocked",
                "AI (MCP) writes:   allowed",
            ]
        );

        // JSON names the rules even at their defaults.
        let json = rules_json(&v1);
        assert_eq!(json["export"], "allowed");
        assert_eq!(json["export_clusters"], serde_json::json!([]));
        assert_eq!(json["watermark"], "off");
        assert_eq!(json["screen_capture"], "allowed");
        let json = rules_json(&v2);
        assert_eq!(json["screen_capture"], "blocked");
        assert_eq!(json["export"], "forbidden");
        assert_eq!(json["watermark_text"], "대외비");
    }

    #[test]
    fn example_stays_format_1_for_older_builds() {
        let payload = policy::parse_payload(EXAMPLE).unwrap();
        assert!(payload.rules.read_only_brokers.is_empty());
        assert_ne!(payload.format(), crate::model::policy::POLICY_FORMAT_V2);
        assert!(payload.rules.unknown_forbid_entries().is_empty());
    }

    #[test]
    fn show_lists_trusted_keys_and_the_signer() {
        let keys = [
            key("1111", "policy.pub", ""),
            key("2222", "policy.keys", "2027 key"),
        ];
        assert_eq!(
            trusted_key_lines(&keys, Some("2222")),
            vec![
                "trusted keys:      1111 (policy.pub)".to_string(),
                "                   2222 (policy.keys: 2027 key) · signed this policy".to_string(),
            ]
        );
        assert_eq!(
            trusted_key_lines(&[], None),
            vec!["trusted keys:      -".to_string()]
        );
    }
}
