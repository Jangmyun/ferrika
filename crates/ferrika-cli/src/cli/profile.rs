use clap::{Args, Subcommand};
use colored::Colorize;
use comfy_table::{Cell, CellAlignment, Color};
use serde_json::json;

use crate::cli::preset::{self, Preset};
use crate::cli::{output, GlobalOpts, OutputFormat};
use crate::kafka::config::{OAuthClientAuth, OAuthMethod, SaslMechanism, SecurityProtocol};
use crate::kafka::profile::{ClusterProfile, ProfileStore};

/// Command line arguments for cluster profile management.
#[derive(Args, Debug, Clone)]
pub struct ProfileArgs {
    #[command(subcommand)]
    pub command: ProfileSubcommands,
}

/// Supported subcommands for `ferrika profile`.
#[derive(Subcommand, Debug, Clone, PartialEq, Eq)]
pub enum ProfileSubcommands {
    /// List all configured cluster profiles
    List,
    /// Show detailed configuration for a specific profile (or active profile)
    Show {
        /// Profile name to show (defaults to active profile)
        name: Option<String>,
    },
    /// Add or update a cluster profile
    Add(Box<ProfileAddArgs>),
    /// Switch the active default cluster profile
    Use {
        /// Name of the profile to activate
        name: String,
    },
    /// Move passwords still stored in profiles.json into the OS credential store
    Secure,
    /// Mark a profile read-only (refuse changes to its cluster), or undo it with --off
    ReadOnly {
        /// Profile name
        name: String,
        /// Allow changes again
        #[arg(long)]
        off: bool,
    },
    /// Create or update a profile from a Kafka client .properties file
    /// (bootstrap.servers, security.protocol, sasl.*, ssl.*, schema.registry.*)
    Import {
        /// Profile name
        name: String,
        /// The .properties file
        #[arg(short = 'f', long)]
        file: String,
        /// Make it the active profile
        #[arg(long = "use")]
        set_active: bool,
    },
    /// Remove a cluster profile from the configuration store
    Remove {
        /// Name of the profile to remove
        name: String,
    },
}

/// Command line arguments for adding or updating a cluster profile.
/// `Debug` is written by hand to redact the SASL password.
#[derive(Args, Clone, PartialEq, Eq)]
pub struct ProfileAddArgs {
    /// Unique nickname for this profile (e.g. "prod", "staging")
    pub name: String,
    /// Kafka bootstrap servers (comma-separated list, e.g. "kafka.prod:9092")
    #[arg(short = 'b', long)]
    pub bootstrap_servers: String,
    /// Optional human-readable description
    #[arg(short = 'd', long)]
    pub description: Option<String>,
    /// Connection preset for a Kafka-compatible broker or managed service:
    /// confluent-cloud, msk-iam, msk-scram, redpanda-cloud, warpstream,
    /// automq. It fills in the security protocol and SASL mechanism the
    /// service expects; flags given explicitly win
    #[arg(long, value_name = "NAME")]
    pub preset: Option<String>,
    /// Security protocol (plaintext, ssl, sasl_plaintext, sasl_ssl; default:
    /// the preset's, else plaintext)
    #[arg(long)]
    pub security_protocol: Option<String>,
    /// SASL mechanism (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, GSSAPI, OAUTHBEARER
    /// for OIDC client credentials, AWS-MSK-IAM)
    #[arg(long)]
    pub sasl_mechanism: Option<String>,
    /// SASL username
    #[arg(long)]
    pub sasl_username: Option<String>,
    /// SASL password (visible in the process list; prefer --sasl-password-stdin)
    #[arg(long)]
    pub sasl_password: Option<String>,
    /// Read the SASL password from the first line of stdin
    #[arg(long, conflicts_with = "sasl_password")]
    pub sasl_password_stdin: bool,
    /// SSL CA certificate path
    #[arg(long)]
    pub ssl_ca_location: Option<String>,
    /// SSL client certificate path
    #[arg(long)]
    pub ssl_certificate_location: Option<String>,
    /// SSL client private key path
    #[arg(long)]
    pub ssl_key_location: Option<String>,
    /// Set this profile as currently active
    #[arg(long)]
    pub set_active: bool,
    /// Refuse operations that change this cluster
    #[arg(long)]
    pub read_only: bool,
    /// Schema Registry URL: `schema` commands, Avro/Protobuf/JSON Schema decoding (`consume -D`) and encoding (`produce --schema`), the GUI and MCP
    #[arg(long, value_name = "URL")]
    pub schema_registry: Option<String>,
    /// Schema Registry basic-auth user (Confluent Cloud: the Schema Registry API key)
    #[arg(long, requires = "schema_registry")]
    pub schema_registry_user: Option<String>,
    /// Schema Registry basic-auth password (visible in the process list;
    /// prefer --schema-registry-password-stdin)
    #[arg(long, requires = "schema_registry_user")]
    pub schema_registry_password: Option<String>,
    /// Read the Schema Registry password from stdin: the first line, or the
    /// second when --sasl-password-stdin or --oauth-client-secret-stdin reads the first
    #[arg(
        long,
        requires = "schema_registry_user",
        conflicts_with = "schema_registry_password"
    )]
    pub schema_registry_password_stdin: bool,
    /// Kafka Connect REST URL for `ferrika connect` and the GUI
    #[arg(long, value_name = "URL")]
    pub connect: Option<String>,
    #[command(flatten)]
    pub oauth: OAuthArgs,
}

/// SASL OAUTHBEARER options of `profile add`. `Debug` is written by hand
/// to redact the client secret.
#[derive(Args, Clone, PartialEq, Eq, Default)]
pub struct OAuthArgs {
    /// OIDC token endpoint (OAUTHBEARER)
    #[arg(long, value_name = "URL")]
    pub oauth_token_endpoint: Option<String>,
    /// OIDC client id (OAUTHBEARER)
    #[arg(long)]
    pub oauth_client_id: Option<String>,
    /// OIDC client secret (visible in the process list; prefer --oauth-client-secret-stdin)
    #[arg(long)]
    pub oauth_client_secret: Option<String>,
    /// Read the OIDC client secret from the first line of stdin
    #[arg(long, conflicts_with_all = ["oauth_client_secret", "sasl_password_stdin"])]
    pub oauth_client_secret_stdin: bool,
    /// How the client authenticates to the token endpoint: basic (default) or post
    #[arg(long, value_name = "METHOD")]
    pub oauth_client_auth: Option<String>,
    /// OIDC scope
    #[arg(long)]
    pub oauth_scope: Option<String>,
    /// SASL extensions sent with the token, as key=value,key=value
    #[arg(long, value_name = "K=V,...")]
    pub oauth_extensions: Option<String>,
    /// AWS region for AWS-MSK-IAM (default: AWS_REGION or the bootstrap host name)
    #[arg(long)]
    pub aws_region: Option<String>,
    /// AWS shared-config profile for AWS-MSK-IAM (default: AWS_PROFILE or default)
    #[arg(long)]
    pub aws_profile: Option<String>,
}

/// What `Debug` shows in place of a secret that was given.
fn redacted(secret: &Option<String>) -> Option<&'static str> {
    secret.as_ref().map(|_| "<redacted>")
}

/// A URL for `Debug` without its `user:password@` part.
fn redacted_url(url: &Option<String>) -> Option<String> {
    url.as_deref().map(crate::model::format::redact_url)
}

impl std::fmt::Debug for ProfileAddArgs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProfileAddArgs")
            .field("name", &self.name)
            .field("bootstrap_servers", &self.bootstrap_servers)
            .field("description", &self.description)
            .field("preset", &self.preset)
            .field("security_protocol", &self.security_protocol)
            .field("sasl_mechanism", &self.sasl_mechanism)
            .field("sasl_username", &self.sasl_username)
            .field("sasl_password", &redacted(&self.sasl_password))
            .field("sasl_password_stdin", &self.sasl_password_stdin)
            .field("ssl_ca_location", &self.ssl_ca_location)
            .field("ssl_certificate_location", &self.ssl_certificate_location)
            .field("ssl_key_location", &self.ssl_key_location)
            .field("set_active", &self.set_active)
            .field("read_only", &self.read_only)
            // Their URLs may carry `user:password@`.
            .field("schema_registry", &redacted_url(&self.schema_registry))
            .field("schema_registry_user", &self.schema_registry_user)
            .field(
                "schema_registry_password",
                &redacted(&self.schema_registry_password),
            )
            .field(
                "schema_registry_password_stdin",
                &self.schema_registry_password_stdin,
            )
            .field("connect", &redacted_url(&self.connect))
            .field("oauth", &self.oauth)
            .finish()
    }
}

impl std::fmt::Debug for OAuthArgs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OAuthArgs")
            .field("oauth_token_endpoint", &self.oauth_token_endpoint)
            .field("oauth_client_id", &self.oauth_client_id)
            .field("oauth_client_secret", &redacted(&self.oauth_client_secret))
            .field("oauth_client_secret_stdin", &self.oauth_client_secret_stdin)
            .field("oauth_client_auth", &self.oauth_client_auth)
            .field("oauth_scope", &self.oauth_scope)
            .field("oauth_extensions", &self.oauth_extensions)
            .field("aws_region", &self.aws_region)
            .field("aws_profile", &self.aws_profile)
            .finish()
    }
}

/// Executes the profile command with the provided global options.
pub async fn run(args: ProfileArgs, global: &GlobalOpts) -> anyhow::Result<()> {
    let mut store = ProfileStore::load().map_err(crate::cli::context::with_repair_hint)?;

    match args.command {
        ProfileSubcommands::List => handle_list(&store, global),
        ProfileSubcommands::Show { name } => handle_show(&store, global, name.as_deref()),
        ProfileSubcommands::Add(mut add_args) => {
            // Check the preset before reading secrets from stdin.
            let preset = add_args.preset.as_deref().map(Preset::parse).transpose()?;
            let sasl_password = if add_args.sasl_password_stdin {
                Some(crate::cli::read_password_stdin().map_err(anyhow::Error::msg)?)
            } else {
                add_args.sasl_password
            };
            if add_args.oauth.oauth_client_secret_stdin {
                add_args.oauth.oauth_client_secret =
                    Some(crate::cli::read_password_stdin().map_err(anyhow::Error::msg)?);
            }
            // Stdin is buffered process-wide, so this reads the next line.
            let registry_password = if add_args.schema_registry_password_stdin {
                Some(crate::cli::read_password_stdin().map_err(anyhow::Error::msg)?)
            } else {
                add_args.schema_registry_password
            };
            let schema_registry = registry_url(
                add_args.schema_registry,
                add_args.schema_registry_user.as_deref(),
                registry_password.as_deref(),
            )?;
            let resolved = preset::resolve(
                preset,
                add_args.security_protocol.as_deref(),
                add_args.sasl_mechanism.as_deref(),
            );
            let warnings = match preset {
                Some(p) => preset::missing_settings(
                    p,
                    add_args.sasl_username.is_some(),
                    sasl_password.is_some(),
                    schema_registry.as_deref(),
                ),
                None => Vec::new(),
            };
            handle_add(
                &mut store,
                add_args.name,
                add_args.bootstrap_servers,
                add_args.description,
                &resolved.security_protocol,
                resolved.sasl_mechanism.as_deref(),
                add_args.sasl_username,
                sasl_password,
                add_args.ssl_ca_location,
                add_args.ssl_certificate_location,
                add_args.ssl_key_location,
                add_args.set_active,
                add_args.read_only,
                schema_registry,
                add_args.connect,
                add_args.oauth,
            )?;
            if let Some(p) = preset {
                report_preset(p, &resolved.filled, &warnings);
            }
            Ok(())
        }
        ProfileSubcommands::Use { name } => {
            handle_use(&mut store, &name)?;
            Ok(())
        }
        ProfileSubcommands::Remove { name } => {
            handle_remove(&mut store, &name)?;
            Ok(())
        }
        ProfileSubcommands::Import {
            name,
            file,
            set_active,
        } => {
            let text = std::fs::read_to_string(&file)
                .map_err(|e| anyhow::anyhow!("Cannot read '{file}': {e}"))?;
            let (mut profile, notes) = ClusterProfile::from_properties(&name, &text)?;
            if let Some(existing) = store.profiles.get(&name) {
                report_replaced(&name, &carry_over(existing, &mut profile));
            }
            let summary = format!(
                "{} · {}{}",
                profile.bootstrap_servers,
                profile.security_protocol.as_str(),
                profile
                    .sasl_mechanism
                    .map(|m| format!(" ({})", m.as_str()))
                    .unwrap_or_default()
            );
            store.add(profile, set_active);
            store.save()?;
            println!(
                "{} Imported profile '{}' from {file}: {summary}",
                "✔".green().bold(),
                name.cyan().bold()
            );
            for note in notes {
                println!("  {} {note}", "!".yellow());
            }
            Ok(())
        }
        ProfileSubcommands::Secure => {
            if !crate::kafka::secrets::enabled() {
                anyhow::bail!(
                    "No OS credential store is in use (unsupported platform or FERRIKA_SECRET_STORE=file)"
                );
            }
            let path = ProfileStore::default_path();
            let before = ProfileStore::load_from_path(&path)?.plaintext_secret_profiles();
            if before.is_empty() {
                println!(
                    "{} No passwords are stored in {}.",
                    "✔".green().bold(),
                    path.display()
                );
                return Ok(());
            }
            store.save()?;
            let left = ProfileStore::load_from_path(&path)?.plaintext_secret_profiles();
            for name in &before {
                if left.contains(name) {
                    println!(
                        "{} {}: could not be stored in the OS credential store (run with FERRIKA_LOG=warn for the reason)",
                        "✖".red(),
                        name.cyan()
                    );
                } else {
                    println!(
                        "{} {}: passwords moved to the OS credential store",
                        "✔".green().bold(),
                        name.cyan()
                    );
                }
            }
            if !left.is_empty() {
                anyhow::bail!("{} profile(s) still keep passwords in the file", left.len());
            }
            Ok(())
        }
        ProfileSubcommands::ReadOnly { name, off } => {
            let profile = store
                .profiles
                .get_mut(&name)
                .ok_or_else(|| anyhow::anyhow!("Profile '{name}' not found"))?;
            profile.read_only = !off;
            store.save()?;
            if off {
                println!(
                    "{} Profile '{}' allows changes.",
                    "✔".green().bold(),
                    name.cyan()
                );
            } else {
                println!(
                    "{} Profile '{}' is read-only: commands and GUI actions that change the cluster are refused.",
                    "✔".green().bold(),
                    name.cyan()
                );
            }
            Ok(())
        }
    }
}

/// The Schema Registry URL with `user` and `password` added as basic auth,
/// the form Ferrika keeps them in (the URL then goes to the OS credential
/// store, like the other secrets). Without a user the URL is kept as given.
fn registry_url(
    url: Option<String>,
    user: Option<&str>,
    password: Option<&str>,
) -> anyhow::Result<Option<String>> {
    match (url, user) {
        (Some(url), Some(user)) => {
            let password = password.ok_or_else(|| {
                anyhow::anyhow!(
                    "--schema-registry-user needs --schema-registry-password-stdin (or --schema-registry-password)"
                )
            })?;
            crate::kafka::profile::url_with_basic_auth(&url, user, password)
                .map(Some)
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "--schema-registry-user needs a Schema Registry URL like https://host, without credentials in it"
                    )
                })
        }
        (url, _) => Ok(url),
    }
}

/// Prints what a preset filled in, its notes, and settings still missing.
fn report_preset(p: Preset, filled: &[(&'static str, String)], warnings: &[String]) {
    if filled.is_empty() {
        println!(
            "  Preset {}: every setting it covers was given explicitly.",
            p.name().cyan()
        );
    } else {
        let settings: Vec<String> = filled
            .iter()
            .map(|(label, value)| format!("{label} {value}"))
            .collect();
        println!("  Preset {}: {}.", p.name().cyan(), settings.join(", "));
    }
    for note in p.notes() {
        println!("  {} {note}", "i".blue());
    }
    for warning in warnings {
        eprintln!("  {} {warning}", "!".yellow());
    }
}

/// Shown in place of a stored password.
const MASKED_SECRET: &str = "********";

fn handle_list(store: &ProfileStore, global: &GlobalOpts) -> anyhow::Result<()> {
    let profiles = store.list();

    if global.format == OutputFormat::Json {
        let json_list: Vec<_> = profiles
            .iter()
            .map(|(p, is_active)| {
                json!({
                    "name": p.name,
                    "is_active": is_active,
                    "bootstrap_servers": p.bootstrap_servers,
                    "security_protocol": p.security_protocol.as_str(),
                    "description": p.description,
                })
            })
            .collect();
        output::print_json(&json_list)?;
        return Ok(());
    }

    if profiles.is_empty() {
        println!("{}", "No cluster profiles configured.".yellow());
        println!("Add one using: ferrika profile add <NAME> -b <BOOTSTRAP_SERVERS>");
        return Ok(());
    }

    let mut table = output::table(vec![
        Cell::new("ACTIVE")
            .fg(Color::Cyan)
            .set_alignment(CellAlignment::Center),
        Cell::new("PROFILE NAME").fg(Color::Cyan),
        Cell::new("BOOTSTRAP SERVERS").fg(Color::Cyan),
        Cell::new("SECURITY").fg(Color::Cyan),
        Cell::new("DESCRIPTION").fg(Color::Cyan),
    ]);

    for (p, is_active) in profiles {
        let active_cell = if is_active {
            Cell::new("●")
                .fg(Color::Green)
                .set_alignment(CellAlignment::Center)
        } else {
            Cell::new(" ").set_alignment(CellAlignment::Center)
        };

        let name_cell = if is_active {
            Cell::new(&p.name).fg(Color::Green)
        } else {
            Cell::new(&p.name).fg(Color::White)
        };

        table.add_row(vec![
            active_cell,
            name_cell,
            Cell::new(&p.bootstrap_servers).fg(Color::DarkCyan),
            Cell::new(p.security_protocol.as_str()).fg(Color::Magenta),
            Cell::new(p.description.as_deref().unwrap_or("-")).fg(Color::DarkGrey),
        ]);
    }

    println!("{table}");
    println!(
        "Config file: {}",
        ProfileStore::default_path().display().to_string().cyan()
    );
    Ok(())
}

fn handle_show(
    store: &ProfileStore,
    global: &GlobalOpts,
    name: Option<&str>,
) -> anyhow::Result<()> {
    let profile = match name {
        Some(n) => store
            .get(n)
            .ok_or_else(|| anyhow::anyhow!("Profile '{n}' not found"))?,
        None => store
            .get_active()
            .ok_or_else(|| anyhow::anyhow!("No active profile configured"))?,
    };

    let is_active = store.active_profile.as_deref() == Some(&profile.name);
    let secret_store = if store.uses_keyring(&profile.name) {
        "OS credential store"
    } else {
        "profiles file"
    };
    let has_secret = profile.sasl_password.is_some()
        || profile.ssl_key_password.is_some()
        || profile.oauth_client_secret.is_some();

    if global.format == OutputFormat::Json {
        let mut val = serde_json::to_value(profile)?;
        if let Some(obj) = val.as_object_mut() {
            obj.insert("is_active".to_string(), json!(is_active));
            // Mask like the table view; JSON output often ends up in logs or tickets.
            for field in ["sasl_password", "ssl_key_password", "oauth_client_secret"] {
                if let Some(password) = obj.get_mut(field) {
                    if !password.is_null() {
                        *password = json!(MASKED_SECRET);
                    }
                }
            }
            for field in ["schema_registry_url", "connect_url"] {
                if let Some(serde_json::Value::String(url)) = obj.get_mut(field) {
                    *url = crate::kafka::profile::mask_url_password(url, MASKED_SECRET);
                }
            }
            if has_secret {
                obj.insert("secret_store".to_string(), json!(secret_store));
            }
        }
        output::print_json(&val)?;
        return Ok(());
    }

    let mut table = output::table(vec![
        Cell::new("PROPERTY").fg(Color::Cyan),
        Cell::new("VALUE").fg(Color::Cyan),
    ]);

    table.add_row(vec![
        Cell::new("Profile Name").fg(Color::White),
        Cell::new(format!(
            "{}{}",
            profile.name,
            if is_active { " (Active)" } else { "" }
        ))
        .fg(if is_active {
            Color::Green
        } else {
            Color::White
        }),
    ]);

    table.add_row(vec![
        Cell::new("Bootstrap Servers").fg(Color::White),
        Cell::new(&profile.bootstrap_servers).fg(Color::DarkCyan),
    ]);

    table.add_row(vec![
        Cell::new("Security Protocol").fg(Color::White),
        Cell::new(profile.security_protocol.as_str()).fg(Color::Magenta),
    ]);

    if let Some(ref mech) = profile.sasl_mechanism {
        table.add_row(vec![
            Cell::new("SASL Mechanism").fg(Color::White),
            Cell::new(mech.as_str()).fg(Color::Yellow),
        ]);
    }

    if let Some(ref user) = profile.sasl_username {
        table.add_row(vec![
            Cell::new("SASL Username").fg(Color::White),
            Cell::new(user).fg(Color::Yellow),
        ]);
    }

    if profile.sasl_password.is_some() {
        table.add_row(vec![
            Cell::new("SASL Password").fg(Color::White),
            Cell::new(MASKED_SECRET).fg(Color::DarkGrey),
        ]);
    }

    if profile.sasl_mechanism == Some(SaslMechanism::Oauthbearer) {
        let method = profile.oauth_method.unwrap_or_default();
        let mut rows = vec![("OAuth Token Source", method.as_str().to_string())];
        let mut optional = |label: &'static str, value: &Option<String>| {
            if let Some(v) = value {
                rows.push((label, v.clone()));
            }
        };
        match method {
            OAuthMethod::Oidc => {
                optional("OAuth Token Endpoint", &profile.oauth_token_endpoint);
                optional("OAuth Client ID", &profile.oauth_client_id);
                optional(
                    "OAuth Client Secret",
                    &profile
                        .oauth_client_secret
                        .as_ref()
                        .map(|_| MASKED_SECRET.to_string()),
                );
                optional(
                    "OAuth Client Auth",
                    &profile
                        .oauth_client_auth
                        .map(|a| client_auth_name(a).to_string()),
                );
                optional("OAuth Scope", &profile.oauth_scope);
            }
            OAuthMethod::AwsMskIam => {
                optional("AWS Region", &profile.aws_region);
                optional("AWS Profile", &profile.aws_profile);
            }
        }
        if !profile.oauth_extensions.is_empty() {
            rows.push((
                "OAuth Extensions",
                crate::kafka::oauth::format_extensions(&profile.oauth_extensions),
            ));
        }
        for (label, value) in rows {
            table.add_row(vec![
                Cell::new(label).fg(Color::White),
                Cell::new(value).fg(Color::Yellow),
            ]);
        }
    }

    if has_secret {
        table.add_row(vec![
            Cell::new("Secrets Stored In").fg(Color::White),
            Cell::new(secret_store).fg(Color::DarkGrey),
        ]);
    }

    if let Some(ref ca) = profile.ssl_ca_location {
        table.add_row(vec![
            Cell::new("SSL CA Location").fg(Color::White),
            Cell::new(ca).fg(Color::Blue),
        ]);
    }

    if let Some(ref cert) = profile.ssl_certificate_location {
        table.add_row(vec![
            Cell::new("SSL Certificate").fg(Color::White),
            Cell::new(cert).fg(Color::Blue),
        ]);
    }

    if let Some(ref key) = profile.ssl_key_location {
        table.add_row(vec![
            Cell::new("SSL Key Location").fg(Color::White),
            Cell::new(key).fg(Color::Blue),
        ]);
    }

    if let Some(ref desc) = profile.description {
        table.add_row(vec![
            Cell::new("Description").fg(Color::White),
            Cell::new(desc).fg(Color::DarkGrey),
        ]);
    }

    println!("{table}");
    Ok(())
}

fn parse_security_protocol(s: &str) -> anyhow::Result<SecurityProtocol> {
    match s.to_lowercase().as_str() {
        "plaintext" => Ok(SecurityProtocol::Plaintext),
        "ssl" => Ok(SecurityProtocol::Ssl),
        "sasl_plaintext" | "sasl-plaintext" => Ok(SecurityProtocol::SaslPlaintext),
        "sasl_ssl" | "sasl-ssl" => Ok(SecurityProtocol::SaslSsl),
        other => anyhow::bail!(
            "Unsupported security protocol '{other}'. Supported: plaintext, ssl, sasl_plaintext, sasl_ssl"
        ),
    }
}

/// The SASL mechanism and, for OAUTHBEARER, the token source
/// (`AWS-MSK-IAM` is OAUTHBEARER with MSK IAM tokens).
fn parse_mechanism(s: &str) -> anyhow::Result<(SaslMechanism, Option<OAuthMethod>)> {
    match s.to_uppercase().as_str() {
        "PLAIN" => Ok((SaslMechanism::Plain, None)),
        "SCRAM-SHA-256" | "SCRAM_SHA_256" | "SCRAM256" => Ok((SaslMechanism::ScramSha256, None)),
        "SCRAM-SHA-512" | "SCRAM_SHA_512" | "SCRAM512" => Ok((SaslMechanism::ScramSha512, None)),
        "GSSAPI" => Ok((SaslMechanism::Gssapi, None)),
        "OAUTHBEARER" => Ok((SaslMechanism::Oauthbearer, Some(OAuthMethod::Oidc))),
        "AWS-MSK-IAM" | "AWS_MSK_IAM" => {
            Ok((SaslMechanism::Oauthbearer, Some(OAuthMethod::AwsMskIam)))
        }
        other => anyhow::bail!(
            "Unsupported SASL mechanism '{other}'. Supported: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, GSSAPI, OAUTHBEARER, AWS-MSK-IAM"
        ),
    }
}

fn client_auth_name(auth: OAuthClientAuth) -> &'static str {
    match auth {
        OAuthClientAuth::Basic => "basic",
        OAuthClientAuth::Post => "post",
    }
}

fn parse_client_auth(s: &str) -> anyhow::Result<OAuthClientAuth> {
    match s.to_lowercase().as_str() {
        "basic" | "client_secret_basic" => Ok(OAuthClientAuth::Basic),
        "post" | "client_secret_post" => Ok(OAuthClientAuth::Post),
        other => anyhow::bail!("Unsupported OAuth client auth '{other}'. Supported: basic, post"),
    }
}

/// Copies the OAUTHBEARER options onto `profile` and checks that the
/// mechanism's required settings are there.
fn apply_oauth(
    profile: &mut ClusterProfile,
    method: Option<OAuthMethod>,
    oauth: OAuthArgs,
) -> anyhow::Result<()> {
    let given = oauth.oauth_token_endpoint.is_some()
        || oauth.oauth_client_id.is_some()
        || oauth.oauth_client_secret.is_some()
        || oauth.oauth_scope.is_some()
        || oauth.oauth_extensions.is_some()
        || oauth.oauth_client_auth.is_some()
        || oauth.aws_region.is_some()
        || oauth.aws_profile.is_some();
    if profile.sasl_mechanism != Some(SaslMechanism::Oauthbearer) {
        if given {
            anyhow::bail!(
                "--oauth-* and --aws-* options need --sasl-mechanism oauthbearer or aws-msk-iam"
            );
        }
        return Ok(());
    }
    if !matches!(
        profile.security_protocol,
        SecurityProtocol::SaslPlaintext | SecurityProtocol::SaslSsl
    ) {
        anyhow::bail!("SASL OAUTHBEARER needs --security-protocol sasl_ssl (or sasl_plaintext)");
    }
    profile.oauth_method = method;
    profile.oauth_token_endpoint = oauth.oauth_token_endpoint;
    profile.oauth_client_id = oauth.oauth_client_id;
    profile.oauth_client_secret = oauth.oauth_client_secret;
    profile.oauth_client_auth = oauth
        .oauth_client_auth
        .as_deref()
        .map(parse_client_auth)
        .transpose()?;
    profile.oauth_scope = oauth.oauth_scope;
    profile.oauth_extensions = match oauth.oauth_extensions.as_deref() {
        Some(text) => crate::kafka::oauth::parse_extensions(text)?,
        None => Default::default(),
    };
    profile.aws_region = oauth.aws_region;
    profile.aws_profile = oauth.aws_profile;
    profile.to_kafka_config().validate()?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn handle_add(
    store: &mut ProfileStore,
    name: String,
    bootstrap_servers: String,
    description: Option<String>,
    sec_proto: &str,
    sasl_mech: Option<&str>,
    sasl_user: Option<String>,
    sasl_pass: Option<String>,
    ssl_ca: Option<String>,
    ssl_cert: Option<String>,
    ssl_key: Option<String>,
    set_active: bool,
    read_only: bool,
    schema_registry: Option<String>,
    connect: Option<String>,
    oauth: OAuthArgs,
) -> anyhow::Result<()> {
    let protocol = parse_security_protocol(sec_proto)?;
    let (mechanism, oauth_method) = match sasl_mech {
        Some(m) => {
            let (mechanism, method) = parse_mechanism(m)?;
            (Some(mechanism), method)
        }
        None => (None, None),
    };

    let mut profile = ClusterProfile::new(&name, &bootstrap_servers);
    profile.description = description;
    profile.security_protocol = protocol;
    profile.sasl_mechanism = mechanism;
    profile.sasl_username = sasl_user;
    profile.sasl_password = sasl_pass;
    profile.ssl_ca_location = ssl_ca;
    profile.ssl_certificate_location = ssl_cert;
    profile.ssl_key_location = ssl_key;
    profile.read_only = read_only;
    profile.schema_registry_url = schema_registry;
    profile.connect_url = connect;
    apply_oauth(&mut profile, oauth_method, oauth)?;

    let is_new = !store.profiles.contains_key(&name);
    if let Some(existing) = store.profiles.get(&name) {
        report_replaced(&name, &carry_over(existing, &mut profile));
    }
    store.add(profile, set_active);
    store.save()?;

    if is_new {
        println!(
            "{} Profile '{}' successfully created.",
            "✔".green().bold(),
            name.cyan().bold()
        );
    } else {
        println!(
            "{} Profile '{}' successfully updated.",
            "✔".green().bold(),
            name.cyan().bold()
        );
    }

    if set_active {
        println!("  Active profile set to '{}'.", name.cyan());
    }

    Ok(())
}

/// What replacing `existing` with `new` (same name) keeps and drops.
#[derive(Debug, Default, PartialEq, Eq)]
struct Replaced {
    /// `existing` was read-only, so `new` stays read-only.
    kept_read_only: bool,
    /// Settings `existing` had and `new` does not.
    dropped: Vec<&'static str>,
}

/// Keeps what an update must not lose silently: the read-only mark (only
/// `profile read-only --off` lifts it) and the SSL key password, which no
/// flag sets, while the key file is the same. Lists the other settings the
/// new profile leaves out, which `add` and `import` replace.
fn carry_over(existing: &ClusterProfile, new: &mut ClusterProfile) -> Replaced {
    let mut replaced = Replaced::default();
    if existing.read_only && !new.read_only {
        new.read_only = true;
        replaced.kept_read_only = true;
    }
    if new.ssl_key_password.is_none()
        && new.ssl_key_location.is_some()
        && new.ssl_key_location == existing.ssl_key_location
    {
        new.ssl_key_password = existing.ssl_key_password.clone();
    }
    let settings: [(&'static str, bool, bool); 6] = [
        (
            "SASL password",
            existing.sasl_password.is_some(),
            new.sasl_password.is_some(),
        ),
        (
            "OAuth client secret",
            existing.oauth_client_secret.is_some(),
            new.oauth_client_secret.is_some(),
        ),
        (
            "SSL key password",
            existing.ssl_key_password.is_some(),
            new.ssl_key_password.is_some(),
        ),
        (
            "Schema Registry URL",
            existing.schema_registry_url.is_some(),
            new.schema_registry_url.is_some(),
        ),
        (
            "Connect URL",
            existing.connect_url.is_some(),
            new.connect_url.is_some(),
        ),
        (
            "description",
            existing.description.is_some(),
            new.description.is_some(),
        ),
    ];
    replaced.dropped = settings
        .into_iter()
        .filter(|(_, had, has)| *had && !*has)
        .map(|(what, _, _)| what)
        .collect();
    replaced
}

fn report_replaced(name: &str, replaced: &Replaced) {
    if replaced.kept_read_only {
        eprintln!(
            "  {} '{name}' stays read-only (`ferrika profile read-only {name} --off` lifts it).",
            "!".yellow()
        );
    }
    if !replaced.dropped.is_empty() {
        eprintln!(
            "  {} The new settings replace the old ones; not given, so removed: {}.",
            "!".yellow(),
            replaced.dropped.join(", ")
        );
    }
}

fn handle_use(store: &mut ProfileStore, name: &str) -> anyhow::Result<()> {
    store.set_active(name)?;
    store.save()?;
    println!(
        "{} Switched active cluster profile to '{}'.",
        "✔".green().bold(),
        name.cyan().bold()
    );
    Ok(())
}

fn handle_remove(store: &mut ProfileStore, name: &str) -> anyhow::Result<()> {
    store.remove(name)?;
    store.save()?;
    println!(
        "{} Profile '{}' removed from configuration.",
        "✔".green().bold(),
        name.cyan().bold()
    );
    if let Some(ref active) = store.active_profile {
        println!("  Active profile is now '{}'.", active.cyan());
    } else {
        println!("  No active profile remains.");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn updating_a_profile_keeps_read_only_and_names_what_it_drops() {
        let mut old = ClusterProfile::new("prod", "a:9092");
        old.read_only = true;
        old.sasl_password = Some("pw".into());
        old.schema_registry_url = Some("https://sr".into());
        old.ssl_key_location = Some("/k.pem".into());
        old.ssl_key_password = Some("keypw".into());
        let mut new = ClusterProfile::new("prod", "b:9092");
        new.ssl_key_location = Some("/k.pem".into());
        let replaced = carry_over(&old, &mut new);
        assert!(new.read_only);
        assert_eq!(new.ssl_key_password.as_deref(), Some("keypw"));
        assert_eq!(
            replaced,
            Replaced {
                kept_read_only: true,
                dropped: vec!["SASL password", "Schema Registry URL"],
            }
        );

        // Another key file: its old password is not carried over.
        let mut other_key = ClusterProfile::new("prod", "b:9092");
        other_key.ssl_key_location = Some("/new.pem".into());
        let replaced = carry_over(&old, &mut other_key);
        assert_eq!(other_key.ssl_key_password, None);
        assert!(replaced.dropped.contains(&"SSL key password"));
    }

    #[test]
    fn debug_output_redacts_the_password_and_client_secret() {
        let cli = <crate::cli::app::Cli as clap::Parser>::try_parse_from([
            "ferrika",
            "profile",
            "add",
            "prod",
            "-b",
            "kafka:9092",
            "--sasl-username",
            "alice",
            "--sasl-password",
            "hunter2-pw",
            "--oauth-client-secret",
            "s3cret-oauth",
            "--schema-registry",
            "https://KEY:url-secret@registry.example.com",
        ])
        .unwrap();
        let shown = format!("{cli:?}");
        assert!(shown.contains("alice"), "{shown}");
        assert!(shown.contains("<redacted>"), "{shown}");
        assert!(!shown.contains("hunter2-pw"), "{shown}");
        assert!(!shown.contains("s3cret-oauth"), "{shown}");
        assert!(shown.contains("registry.example.com"), "{shown}");
        assert!(!shown.contains("url-secret"), "{shown}");
    }

    #[test]
    fn registry_credentials_go_into_the_url() {
        assert_eq!(
            registry_url(
                Some("https://psrc.example.com".into()),
                Some("KEY"),
                Some("s/e@c")
            )
            .unwrap()
            .as_deref(),
            Some("https://KEY:s%2Fe%40c@psrc.example.com")
        );
        assert_eq!(
            registry_url(Some("http://sr:8081".into()), None, None).unwrap(),
            Some("http://sr:8081".to_string())
        );
        assert_eq!(registry_url(None, None, None).unwrap(), None);
        // A user without a password, or a URL that already has credentials.
        assert!(registry_url(Some("https://sr".into()), Some("KEY"), None).is_err());
        assert!(registry_url(Some("https://A:B@sr".into()), Some("KEY"), Some("S")).is_err());
    }

    #[test]
    fn preset_and_registry_flags_parse() {
        use clap::Parser;

        #[derive(Parser, Debug)]
        struct Wrapper {
            #[command(subcommand)]
            cmd: ProfileSubcommands,
        }
        let parsed = Wrapper::parse_from([
            "profile",
            "add",
            "cc",
            "-b",
            "pkc-1.us-east-1.aws.confluent.cloud:9092",
            "--preset",
            "confluent-cloud",
            "--sasl-username",
            "KEY",
            "--sasl-password-stdin",
            "--schema-registry",
            "https://psrc-1.us-east-1.aws.confluent.cloud",
            "--schema-registry-user",
            "SRKEY",
            "--schema-registry-password-stdin",
        ]);
        let ProfileSubcommands::Add(args) = parsed.cmd else {
            panic!("expected add");
        };
        assert_eq!(args.preset.as_deref(), Some("confluent-cloud"));
        assert_eq!(args.security_protocol, None);
        assert!(args.schema_registry_password_stdin);
        // A registry user needs the registry URL.
        assert!(Wrapper::try_parse_from([
            "profile",
            "add",
            "x",
            "-b",
            "b:1",
            "--schema-registry-user",
            "K",
        ])
        .is_err());
        // The registry password is redacted in Debug output.
        let parsed = Wrapper::parse_from([
            "profile",
            "add",
            "x",
            "-b",
            "b:1",
            "--schema-registry",
            "https://sr",
            "--schema-registry-user",
            "K",
            "--schema-registry-password",
            "sr-hunter2",
        ]);
        let shown = format!("{parsed:?}");
        assert!(!shown.contains("sr-hunter2"), "{shown}");
    }

    #[test]
    fn test_parse_security_protocols() {
        assert_eq!(
            parse_security_protocol("plaintext").unwrap(),
            SecurityProtocol::Plaintext
        );
        assert_eq!(
            parse_security_protocol("sasl_ssl").unwrap(),
            SecurityProtocol::SaslSsl
        );
        assert_eq!(
            parse_security_protocol("SASL-SSL").unwrap(),
            SecurityProtocol::SaslSsl
        );
        assert!(parse_security_protocol("unknown").is_err());
    }

    #[test]
    fn test_parse_sasl_mechanisms() {
        assert_eq!(parse_mechanism("plain").unwrap().0, SaslMechanism::Plain);
        assert_eq!(
            parse_mechanism("scram-sha-256").unwrap().0,
            SaslMechanism::ScramSha256
        );
        assert!(parse_mechanism("kerberos_custom").is_err());
        assert_eq!(
            parse_mechanism("aws-msk-iam").unwrap(),
            (SaslMechanism::Oauthbearer, Some(OAuthMethod::AwsMskIam))
        );
        assert_eq!(
            parse_mechanism("oauthbearer").unwrap(),
            (SaslMechanism::Oauthbearer, Some(OAuthMethod::Oidc))
        );
    }

    #[test]
    fn test_oauth_options_are_applied_and_checked() {
        let oidc = OAuthArgs {
            oauth_token_endpoint: Some("https://idp/token".into()),
            oauth_client_id: Some("svc".into()),
            oauth_client_secret: Some("sec".into()),
            oauth_client_auth: Some("post".into()),
            oauth_extensions: Some("logicalCluster=lkc-1".into()),
            ..Default::default()
        };
        let mut p = ClusterProfile::new("cc", "b:9093");
        p.security_protocol = SecurityProtocol::SaslSsl;
        p.sasl_mechanism = Some(SaslMechanism::Oauthbearer);
        apply_oauth(&mut p, Some(OAuthMethod::Oidc), oidc.clone()).unwrap();
        assert_eq!(p.oauth_client_auth, Some(OAuthClientAuth::Post));
        assert_eq!(p.oauth_extensions.len(), 1);

        // Missing secret.
        let mut p2 = p.clone();
        let partial = OAuthArgs {
            oauth_client_secret: None,
            ..oidc.clone()
        };
        let err = apply_oauth(&mut p2, Some(OAuthMethod::Oidc), partial).unwrap_err();
        assert!(err.to_string().contains("oauth_client_secret"), "{err}");

        // Not a SASL protocol.
        let mut p3 = p.clone();
        p3.security_protocol = SecurityProtocol::Plaintext;
        assert!(apply_oauth(&mut p3, Some(OAuthMethod::Oidc), oidc.clone()).is_err());

        // OAuth options without the mechanism.
        let mut plain = ClusterProfile::new("p", "b:9092");
        assert!(apply_oauth(&mut plain, None, oidc).is_err());
        assert!(apply_oauth(&mut plain, None, OAuthArgs::default()).is_ok());

        // MSK IAM needs nothing else.
        let mut msk = p.clone();
        apply_oauth(
            &mut msk,
            Some(OAuthMethod::AwsMskIam),
            OAuthArgs {
                aws_profile: Some("prod".into()),
                ..Default::default()
            },
        )
        .unwrap();
        assert!(msk.uses_msk_iam());
        assert_eq!(msk.oauth_client_secret, None);
    }

    #[test]
    fn test_oauth_flags_parse() {
        use clap::Parser;

        #[derive(Parser, Debug)]
        struct Wrapper {
            #[command(subcommand)]
            cmd: ProfileSubcommands,
        }
        let parsed = Wrapper::parse_from([
            "profile",
            "add",
            "msk",
            "-b",
            "b-1.x.c2.kafka.us-east-1.amazonaws.com:9098",
            "--security-protocol",
            "sasl_ssl",
            "--sasl-mechanism",
            "aws-msk-iam",
            "--aws-profile",
            "prod",
            "--aws-region",
            "us-east-1",
        ]);
        let ProfileSubcommands::Add(args) = parsed.cmd else {
            panic!("expected add");
        };
        assert_eq!(args.oauth.aws_profile.as_deref(), Some("prod"));
        assert!(Wrapper::try_parse_from([
            "profile",
            "add",
            "x",
            "-b",
            "b:1",
            "--oauth-client-secret",
            "a",
            "--oauth-client-secret-stdin",
        ])
        .is_err());
    }

    #[test]
    fn test_profile_args_parsing() {
        use clap::Parser;

        #[derive(Parser, Debug)]
        struct Wrapper {
            #[command(subcommand)]
            cmd: ProfileSubcommands,
        }

        let parsed = Wrapper::parse_from(["profile", "list"]);
        assert_eq!(parsed.cmd, ProfileSubcommands::List);

        let parsed = Wrapper::parse_from(["profile", "use", "prod"]);
        assert_eq!(
            parsed.cmd,
            ProfileSubcommands::Use {
                name: "prod".to_string()
            }
        );

        let parsed = Wrapper::parse_from([
            "profile",
            "add",
            "staging",
            "-b",
            "staging:9092",
            "--set-active",
        ]);
        match parsed.cmd {
            ProfileSubcommands::Add(add_args) => {
                assert_eq!(add_args.name, "staging");
                assert_eq!(add_args.bootstrap_servers, "staging:9092");
                assert!(add_args.set_active);
            }
            _ => panic!("Expected ProfileSubcommands::Add"),
        }
    }
}
