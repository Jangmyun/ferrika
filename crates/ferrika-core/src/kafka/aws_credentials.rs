//! AWS credentials and region for MSK IAM, resolved like the AWS CLI does
//! for the common cases: environment variables, the shared `credentials` and
//! `config` files (`aws_profile` / `AWS_PROFILE`), and `credential_process`.
//!
//! Not yet supported: SSO, `role_arn` (assume role), web identity, ECS and
//! EC2 instance metadata. Those profiles can still be used through
//! `credential_process = aws configure export-credentials --profile <name> --format process`.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use jiff::Timestamp;

use crate::error::{KafkaError, KafkaResult};
use crate::kafka::aws_sigv4::AwsCredentials;
use crate::kafka::oauth::CredentialResolver;

/// Where settings are looked up: environment variables and the home folder.
/// Tests pass their own.
pub struct Lookup<'a> {
    pub env: &'a (dyn Fn(&str) -> Option<String> + Sync),
    pub home: Option<PathBuf>,
}

fn process_env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.trim().is_empty())
}

/// The process environment and the user's home folder.
pub fn default_lookup() -> Lookup<'static> {
    let home = process_env("HOME")
        .or_else(|| process_env("USERPROFILE"))
        .map(PathBuf::from);
    Lookup {
        env: &process_env,
        home,
    }
}

impl Lookup<'_> {
    fn var(&self, name: &str) -> Option<String> {
        (self.env)(name).filter(|v| !v.trim().is_empty())
    }

    fn file(&self, var: &str, default_name: &str) -> Option<PathBuf> {
        self.var(var).map(PathBuf::from).or_else(|| {
            self.home
                .as_ref()
                .map(|h| h.join(".aws").join(default_name))
        })
    }

    fn credentials_file(&self) -> Option<PathBuf> {
        self.file("AWS_SHARED_CREDENTIALS_FILE", "credentials")
    }

    fn config_file(&self) -> Option<PathBuf> {
        self.file("AWS_CONFIG_FILE", "config")
    }

    fn profile_name(&self, explicit: Option<&str>) -> String {
        explicit
            .map(str::to_string)
            .filter(|p| !p.trim().is_empty())
            .or_else(|| self.var("AWS_PROFILE"))
            .unwrap_or_else(|| "default".to_string())
    }
}

type Ini = BTreeMap<String, BTreeMap<String, String>>;

/// Parses the INI dialect of the AWS shared files: `[section]`,
/// `key = value`, `#`/`;` comments. Keys are lower-cased.
pub(crate) fn parse_ini(text: &str) -> Ini {
    let mut out = Ini::new();
    let mut section: Option<String> = None;
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
            continue;
        }
        if let Some(name) = line.strip_prefix('[').and_then(|l| l.strip_suffix(']')) {
            let name = name.split_whitespace().collect::<Vec<_>>().join(" ");
            out.entry(name.clone()).or_default();
            section = Some(name);
            continue;
        }
        if let (Some(section), Some((k, v))) = (&section, line.split_once('=')) {
            out.entry(section.clone())
                .or_default()
                .insert(k.trim().to_ascii_lowercase(), v.trim().to_string());
        }
    }
    out
}

fn read_ini(path: Option<&Path>) -> Ini {
    path.and_then(|p| std::fs::read_to_string(p).ok())
        .map(|text| parse_ini(&text))
        .unwrap_or_default()
}

/// The settings of profile `name`: the config file's `[profile name]`
/// (`[default]` for the default one), overridden by the credentials file's
/// `[name]`.
fn profile_settings(lookup: &Lookup<'_>, name: &str) -> Option<BTreeMap<String, String>> {
    let config = read_ini(lookup.config_file().as_deref());
    let credentials = read_ini(lookup.credentials_file().as_deref());
    let from_config = config
        .get(&format!("profile {name}"))
        .or_else(|| config.get(name));
    let from_credentials = credentials.get(name);
    if from_config.is_none() && from_credentials.is_none() {
        return None;
    }
    let mut merged = from_config.cloned().unwrap_or_default();
    if let Some(creds) = from_credentials {
        merged.extend(creds.clone());
    }
    Some(merged)
}

/// Resolves credentials: with no explicit profile, the `AWS_ACCESS_KEY_ID` /
/// `AWS_SECRET_ACCESS_KEY` (/ `AWS_SESSION_TOKEN`) variables first; then
/// the profile (explicit, else `AWS_PROFILE`, else `default`) from the
/// shared files, static keys or `credential_process`.
pub(crate) fn resolve_credentials(
    lookup: &Lookup<'_>,
    profile: Option<&str>,
) -> KafkaResult<AwsCredentials> {
    let explicit = profile.is_some_and(|p| !p.trim().is_empty());
    if !explicit {
        if let (Some(id), Some(secret)) = (
            lookup.var("AWS_ACCESS_KEY_ID"),
            lookup.var("AWS_SECRET_ACCESS_KEY"),
        ) {
            let mut creds = AwsCredentials::new(id, secret);
            creds.session_token = lookup.var("AWS_SESSION_TOKEN");
            return Ok(creds);
        }
    }
    let name = lookup.profile_name(profile);
    let Some(settings) = profile_settings(lookup, &name) else {
        return Err(KafkaError::config(format!(
            "No AWS credentials found: AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY are not set \
             and profile '{name}' is not in {} or {}",
            show(lookup.credentials_file()),
            show(lookup.config_file())
        )));
    };
    let get = |k: &str| settings.get(k).filter(|v| !v.is_empty()).cloned();
    if let (Some(id), Some(secret)) = (get("aws_access_key_id"), get("aws_secret_access_key")) {
        let mut creds = AwsCredentials::new(id, secret);
        creds.session_token = get("aws_session_token");
        return Ok(creds);
    }
    if let Some(command) = get("credential_process") {
        return run_credential_process(&command);
    }
    let unsupported = [
        "sso_session",
        "sso_start_url",
        "role_arn",
        "web_identity_token_file",
    ]
    .into_iter()
    .find(|k| settings.contains_key(*k));
    Err(KafkaError::config(match unsupported {
        Some(key) => format!(
            "AWS profile '{name}' uses {key}, which Ferrika does not resolve yet; add \
             `credential_process = aws configure export-credentials --profile {name} --format process` \
             to a profile and use that one"
        ),
        None => format!("AWS profile '{name}' has no access keys and no credential_process"),
    }))
}

fn show(path: Option<PathBuf>) -> String {
    path.map_or_else(
        || "(no home folder)".to_string(),
        |p| p.display().to_string(),
    )
}

/// Runs a `credential_process` command (through the shell) and parses its
/// JSON output (`Version` 1).
pub(crate) fn run_credential_process(command: &str) -> KafkaResult<AwsCredentials> {
    // Passed verbatim: Rust's argument quoting (\") means nothing to cmd and
    // would break JSON or quoted paths. `/S /C "..."` strips exactly the
    // outer quotes.
    #[cfg(windows)]
    let output = {
        use std::os::windows::process::CommandExt;
        std::process::Command::new("cmd")
            .raw_arg(format!("/S /C \"{command}\""))
            .output()
    };
    #[cfg(not(windows))]
    let output = std::process::Command::new("sh")
        .args(["-c", command])
        .output();
    let output = output
        .map_err(|e| KafkaError::config(format!("Cannot run AWS credential_process: {e}")))?;
    if !output.status.success() {
        return Err(KafkaError::config(format!(
            "AWS credential_process failed ({}): {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    parse_credential_process_output(&String::from_utf8_lossy(&output.stdout))
}

/// Parses `{"Version":1,"AccessKeyId":..,"SecretAccessKey":..,
/// "SessionToken":..,"Expiration":"<RFC 3339>"}`.
pub(crate) fn parse_credential_process_output(text: &str) -> KafkaResult<AwsCredentials> {
    let bad = |why: &str| KafkaError::config(format!("AWS credential_process output {why}"));
    let json: serde_json::Value =
        serde_json::from_str(text.trim()).map_err(|_| bad("is not JSON"))?;
    if json.get("Version").and_then(|v| v.as_i64()) != Some(1) {
        return Err(bad("is not Version 1"));
    }
    let field = |k: &str| json.get(k).and_then(|v| v.as_str()).map(str::to_string);
    let (Some(id), Some(secret)) = (field("AccessKeyId"), field("SecretAccessKey")) else {
        return Err(bad("lacks AccessKeyId or SecretAccessKey"));
    };
    let mut creds = AwsCredentials::new(id, secret);
    creds.session_token = field("SessionToken");
    creds.expiration = match field("Expiration") {
        Some(text) => Some(
            text.parse::<Timestamp>()
                .map_err(|_| bad("has an invalid Expiration"))?,
        ),
        None => None,
    };
    Ok(creds)
}

/// The region in an MSK bootstrap host name
/// (`b-1.c.abc.c2.kafka.us-east-1.amazonaws.com`,
/// `boot-x.c1.kafka-serverless.eu-west-1.amazonaws.com`).
pub(crate) fn region_from_bootstrap(bootstrap: &str) -> Option<String> {
    bootstrap.split(',').find_map(|server| {
        let host = server
            .trim()
            .rsplit_once(':')
            .map_or(server.trim(), |(h, _)| h);
        let labels: Vec<&str> = host.split('.').collect();
        labels.windows(3).find_map(|w| {
            (matches!(w[0], "kafka" | "kafka-serverless")
                && w[2] == "amazonaws"
                && !w[1].is_empty())
            .then(|| w[1].to_string())
        })
    })
}

/// The region to sign for: the profile's `aws_region`, `AWS_REGION`,
/// `AWS_DEFAULT_REGION`, the bootstrap host name, then the AWS profile's
/// `region`.
pub fn resolve_region(
    lookup: &Lookup<'_>,
    explicit: Option<&str>,
    aws_profile: Option<&str>,
    bootstrap: &str,
) -> KafkaResult<String> {
    explicit
        .map(str::trim)
        .filter(|r| !r.is_empty())
        .map(str::to_string)
        .or_else(|| lookup.var("AWS_REGION"))
        .or_else(|| lookup.var("AWS_DEFAULT_REGION"))
        .or_else(|| region_from_bootstrap(bootstrap))
        .or_else(|| {
            let name = lookup.profile_name(aws_profile);
            profile_settings(lookup, &name)?.get("region").cloned()
        })
        .ok_or_else(|| {
            KafkaError::config(
                "No AWS region for MSK IAM: set aws_region on the profile or AWS_REGION",
            )
        })
}

/// A resolver for [`crate::kafka::oauth::MskIamSource`] over the process
/// environment.
pub fn resolver(profile: Option<String>) -> CredentialResolver {
    Arc::new(move || resolve_credentials(&default_lookup(), profile.as_deref()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    struct Fixture {
        dir: PathBuf,
    }

    impl Fixture {
        fn new(tag: &str) -> Self {
            let dir =
                std::env::temp_dir().join(format!("ferrika_aws_{tag}_{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(dir.join(".aws")).unwrap();
            Self { dir }
        }
        fn write(&self, name: &str, text: &str) {
            std::fs::write(self.dir.join(".aws").join(name), text).unwrap();
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    fn with_env<T>(vars: &[(&str, &str)], home: &Path, f: impl FnOnce(&Lookup<'_>) -> T) -> T {
        let map: HashMap<String, String> = vars
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let env = move |k: &str| map.get(k).cloned();
        let lookup = Lookup {
            env: &env,
            home: Some(home.to_path_buf()),
        };
        f(&lookup)
    }

    #[test]
    fn environment_variables_come_first() {
        let fx = Fixture::new("env");
        fx.write(
            "credentials",
            "[default]\naws_access_key_id = FILEKEY\naws_secret_access_key = f\n",
        );
        let creds = with_env(
            &[
                ("AWS_ACCESS_KEY_ID", "ENVKEY"),
                ("AWS_SECRET_ACCESS_KEY", "e"),
                ("AWS_SESSION_TOKEN", "tok"),
            ],
            &fx.dir,
            |l| resolve_credentials(l, None),
        )
        .unwrap();
        assert_eq!(creds.access_key_id, "ENVKEY");
        assert_eq!(creds.session_token.as_deref(), Some("tok"));
        // An explicit profile skips the variables.
        let creds = with_env(
            &[
                ("AWS_ACCESS_KEY_ID", "ENVKEY"),
                ("AWS_SECRET_ACCESS_KEY", "e"),
            ],
            &fx.dir,
            |l| resolve_credentials(l, Some("default")),
        )
        .unwrap();
        assert_eq!(creds.access_key_id, "FILEKEY");
    }

    #[test]
    fn shared_files_by_profile_name() {
        let fx = Fixture::new("files");
        fx.write(
            "credentials",
            "# comment\n[default]\naws_access_key_id=DEF\naws_secret_access_key=d\n\n\
             [team]\naws_access_key_id = TEAM\naws_secret_access_key = t\naws_session_token = st\n",
        );
        fx.write(
            "config",
            "[default]\nregion = us-east-1\n[profile team]\nregion = eu-west-1\n\
             [profile cfgonly]\naws_access_key_id = CFG\naws_secret_access_key = c\n",
        );
        let get = |vars: &[(&str, &str)], p: Option<&str>| {
            with_env(vars, &fx.dir, |l| resolve_credentials(l, p)).unwrap()
        };
        assert_eq!(get(&[], None).access_key_id, "DEF");
        assert_eq!(get(&[("AWS_PROFILE", "team")], None).access_key_id, "TEAM");
        let team = get(&[], Some("team"));
        assert_eq!(team.session_token.as_deref(), Some("st"));
        assert_eq!(get(&[], Some("cfgonly")).access_key_id, "CFG");
        let err = with_env(&[], &fx.dir, |l| resolve_credentials(l, Some("nope"))).unwrap_err();
        assert!(err.to_string().contains("profile 'nope'"), "{err}");

        // Region: explicit, env, bootstrap host, then the profile's config.
        let region = |vars: &[(&str, &str)], explicit: Option<&str>, bootstrap: &str| {
            with_env(vars, &fx.dir, |l| {
                resolve_region(l, explicit, Some("team"), bootstrap)
            })
            .unwrap()
        };
        let msk = "b-1.demo.abc123.c2.kafka.ap-northeast-2.amazonaws.com:9098";
        assert_eq!(
            region(&[("AWS_REGION", "us-west-2")], Some("sa-east-1"), msk),
            "sa-east-1"
        );
        assert_eq!(
            region(&[("AWS_REGION", "us-west-2")], None, msk),
            "us-west-2"
        );
        assert_eq!(region(&[], None, msk), "ap-northeast-2");
        assert_eq!(region(&[], None, "localhost:9092"), "eu-west-1");
    }

    #[test]
    fn credential_process_output_is_parsed() {
        let creds = parse_credential_process_output(
            r#"{"Version":1,"AccessKeyId":"ASIAX","SecretAccessKey":"s","SessionToken":"t",
                "Expiration":"2030-01-02T03:04:05Z"}"#,
        )
        .unwrap();
        assert_eq!(creds.access_key_id, "ASIAX");
        assert_eq!(creds.session_token.as_deref(), Some("t"));
        assert_eq!(
            creds.expiration.map(crate::model::time::rfc3339),
            Some("2030-01-02T03:04:05+00:00".to_string())
        );
        assert!(parse_credential_process_output(r#"{"Version":2}"#).is_err());
        assert!(parse_credential_process_output("nope").is_err());
        assert!(parse_credential_process_output(r#"{"Version":1,"AccessKeyId":"A"}"#).is_err());
    }

    #[test]
    fn credential_process_runs_from_the_profile() {
        let fx = Fixture::new("process");
        #[cfg(windows)]
        let command = r#"echo {"Version": 1, "AccessKeyId": "PROC", "SecretAccessKey": "p"}"#;
        #[cfg(not(windows))]
        let command = r#"echo '{"Version": 1, "AccessKeyId": "PROC", "SecretAccessKey": "p"}'"#;
        fx.write(
            "config",
            &format!("[profile proc]\ncredential_process = {command}\n"),
        );
        let creds = with_env(&[], &fx.dir, |l| resolve_credentials(l, Some("proc"))).unwrap();
        assert_eq!(creds.access_key_id, "PROC");

        fx.write("config", "[profile failing]\ncredential_process = exit 3\n");
        let err = with_env(&[], &fx.dir, |l| resolve_credentials(l, Some("failing"))).unwrap_err();
        assert!(
            err.to_string().contains("credential_process failed"),
            "{err}"
        );

        fx.write("config", "[profile sso]\nsso_session = corp\n");
        let err = with_env(&[], &fx.dir, |l| resolve_credentials(l, Some("sso"))).unwrap_err();
        assert!(err.to_string().contains("export-credentials"), "{err}");
    }

    #[test]
    fn region_from_msk_host_names() {
        assert_eq!(
            region_from_bootstrap("b-2.x.y.c3.kafka.us-east-1.amazonaws.com:9098,b-1.x:9098")
                .as_deref(),
            Some("us-east-1")
        );
        assert_eq!(
            region_from_bootstrap("boot-abc.c1.kafka-serverless.eu-west-1.amazonaws.com:9098")
                .as_deref(),
            Some("eu-west-1")
        );
        assert_eq!(region_from_bootstrap("kafka.internal:9092"), None);
        assert!(parse_ini("[ profile  a ]\nk=v")["profile a"].contains_key("k"));
    }
}
