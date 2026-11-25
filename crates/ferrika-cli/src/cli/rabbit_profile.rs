//! `ferrika rabbit profile`: named RabbitMQ connections in
//! `rabbit-profiles.json` (next to `profiles.json`), passwords in the OS
//! credential store. Separate from the Kafka profiles (`ferrika profile`).
//!
//! `add` takes the connection from the same options every `rabbit` command
//! takes (`--rabbit-url`, `--vhost`, `--rabbit-user`,
//! `--rabbit-password-stdin`, `--amqp-url`).

use clap::{Args, Subcommand};
use colored::Colorize;
use comfy_table::Cell;
use serde::Serialize;

use crate::cli::output;
use crate::cli::rabbit::RabbitConnection;
use crate::rabbitmq::profile::{RabbitProfile, RabbitProfileStore};

#[derive(Args, Debug, Clone, PartialEq, Eq)]
pub struct RabbitProfileArgs {
    #[command(subcommand)]
    pub command: RabbitProfileSubcommands,
}

#[derive(Subcommand, Debug, Clone, PartialEq, Eq)]
pub enum RabbitProfileSubcommands {
    /// Add or replace a profile from --rabbit-url, --vhost, --rabbit-user,
    /// the password (--rabbit-password-stdin or FERRIKA_RABBIT_PASSWORD) and
    /// --amqp-url
    Add(RabbitProfileAddArgs),
    /// List the profiles
    List,
    /// Make a profile the one `rabbit` commands use by default
    Use { name: String },
    /// Remove a profile and its stored password
    Remove { name: String },
    /// Show a profile (default: the active one); never the password
    Show { name: Option<String> },
}

#[derive(Args, Debug, Clone, PartialEq, Eq)]
pub struct RabbitProfileAddArgs {
    /// Profile name (letters, digits, '-', '_', '.')
    pub name: String,
    /// Refuse commands that change this broker's messages (publish, peek,
    /// dlq inspect and redrive)
    #[arg(long)]
    pub read_only: bool,
    /// Description
    #[arg(short = 'd', long)]
    pub description: Option<String>,
    /// Make it the active profile
    #[arg(long = "use")]
    pub set_active: bool,
}

/// A profile as `list` and `show` print it: no password.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct ProfileView {
    name: String,
    active: bool,
    management_url: String,
    vhost: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    user: Option<String>,
    /// `os-credential-store`, `file` or `none`.
    password: &'static str,
    amqp_url: String,
    read_only: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    /// TLS file paths (not secret: the key file's path, never its content).
    #[serde(skip_serializing_if = "Option::is_none")]
    ca_cert: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    client_cert: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    client_key: Option<String>,
}

impl ProfileView {
    fn new(name: &str, profile: &RabbitProfile, active: bool) -> Self {
        Self {
            name: name.to_string(),
            active,
            management_url: profile.management_url.clone(),
            vhost: profile.vhost.clone(),
            user: profile.user.clone(),
            password: password_location(profile),
            amqp_url: profile
                .amqp_url()
                .unwrap_or_else(|e| format!("(invalid: {e})")),
            read_only: profile.read_only,
            description: profile.description.clone(),
            ca_cert: profile.ca_cert.clone(),
            client_cert: profile.client_cert.clone(),
            client_key: profile.client_key.clone(),
        }
    }
}

/// Where a profile's password is kept, never the password.
fn password_location(profile: &RabbitProfile) -> &'static str {
    match (profile.password_in_store, profile.password.is_some()) {
        (true, _) => "os-credential-store",
        (false, true) => "file",
        (false, false) => "none",
    }
}

pub fn run(
    args: &RabbitProfileArgs,
    connection: &RabbitConnection,
    json: bool,
) -> anyhow::Result<()> {
    let mut store = RabbitProfileStore::load()?;
    match &args.command {
        RabbitProfileSubcommands::Add(add) => {
            let password = connection_password(connection)?;
            let profile = profile_from(add, connection, password, &store)?;
            let replaced = store.profiles.contains_key(&add.name);
            store.insert(&add.name, profile)?;
            if add.set_active {
                store.set_active(&add.name)?;
            }
            store.save()?;
            // Read back what was stored: where the password went.
            let saved = RabbitProfileStore::load_file(&RabbitProfileStore::default_path())?;
            let stored = saved.get(&add.name)?;
            if json {
                output::print_json(&ProfileView::new(
                    &add.name,
                    stored,
                    saved.active_profile.as_deref() == Some(add.name.as_str()),
                ))?;
            } else {
                println!(
                    "{} {} RabbitMQ profile '{}' ({}).",
                    "✔".green().bold(),
                    if replaced { "Replaced" } else { "Added" },
                    add.name.cyan(),
                    stored.management_url
                );
                match password_location(stored) {
                    "file" => eprintln!(
                        "{}",
                        format!(
                            "No OS credential store: the password is kept in {}.",
                            RabbitProfileStore::default_path().display()
                        )
                        .yellow()
                    ),
                    "os-credential-store" => {
                        println!("  The password is in the OS credential store.")
                    }
                    _ => {}
                }
            }
        }
        RabbitProfileSubcommands::List => {
            let views: Vec<ProfileView> = store
                .profiles
                .iter()
                .map(|(name, p)| {
                    ProfileView::new(name, p, store.active_profile.as_deref() == Some(name))
                })
                .collect();
            output::emit(json, &views, |views| {
                if views.is_empty() {
                    println!(
                        "No RabbitMQ profiles. Add one with `ferrika rabbit profile add NAME \
                         --rabbit-url URL`."
                    );
                    return;
                }
                let mut table = output::table(vec![
                    "",
                    "NAME",
                    "MANAGEMENT URL",
                    "VHOST",
                    "USER",
                    "READ-ONLY",
                ]);
                for v in views {
                    table.add_row(vec![
                        Cell::new(if v.active { "*" } else { "" }),
                        Cell::new(&v.name),
                        Cell::new(&v.management_url),
                        Cell::new(&v.vhost),
                        Cell::new(v.user.as_deref().unwrap_or_default()),
                        Cell::new(v.read_only),
                    ]);
                }
                println!("{table}");
            })?;
        }
        RabbitProfileSubcommands::Use { name } => {
            store.set_active(name)?;
            store.save()?;
            if json {
                output::print_json(&profile_action_json(name, "activated"))?;
                return Ok(());
            }
            println!(
                "{} RabbitMQ commands now use profile '{}'.",
                "✔".green().bold(),
                name.cyan()
            );
        }
        RabbitProfileSubcommands::Remove { name } => {
            store.remove(name)?;
            store.save()?;
            if json {
                output::print_json(&profile_action_json(name, "removed"))?;
                return Ok(());
            }
            println!(
                "{} Removed RabbitMQ profile '{}'.",
                "✔".green().bold(),
                name.cyan()
            );
        }
        RabbitProfileSubcommands::Show { name } => {
            let (name, profile) = match name {
                Some(name) => (name.as_str(), store.get(name)?),
                None => store.active().ok_or_else(|| {
                    anyhow::anyhow!(
                        "No active RabbitMQ profile (see `ferrika rabbit profile list`)"
                    )
                })?,
            };
            let view =
                ProfileView::new(name, profile, store.active_profile.as_deref() == Some(name));
            output::emit(json, &view, |v| {
                println!("{}", v.name.cyan().bold());
                println!("  management URL: {}", v.management_url);
                println!("  vhost:          {}", v.vhost);
                println!(
                    "  user:           {}",
                    v.user.as_deref().unwrap_or("(none)")
                );
                println!("  password:       {}", v.password);
                println!("  AMQP URL:       {}", v.amqp_url);
                println!("  read-only:      {}", v.read_only);
                println!("  active:         {}", v.active);
                if let Some(ca) = &v.ca_cert {
                    println!("  CA file:        {ca}");
                }
                if let (Some(cert), Some(key)) = (&v.client_cert, &v.client_key) {
                    println!("  client cert:    {cert} (key {key})");
                }
                if let Some(d) = &v.description {
                    println!("  description:    {d}");
                }
            })?;
        }
    }
    Ok(())
}

/// The password given for `add`: stdin, option or environment.
fn connection_password(connection: &RabbitConnection) -> anyhow::Result<Option<String>> {
    if connection.password_stdin {
        crate::cli::read_password_stdin()
            .map(Some)
            .map_err(anyhow::Error::msg)
    } else {
        Ok(connection.password.clone())
    }
}

/// The profile `add` stores. Replacing a profile keeps its password when
/// no new one is given and the user stays the same.
fn profile_from(
    add: &RabbitProfileAddArgs,
    connection: &RabbitConnection,
    password: Option<String>,
    store: &RabbitProfileStore,
) -> anyhow::Result<RabbitProfile> {
    let url = connection
        .url
        .as_deref()
        .map(str::trim)
        .filter(|u| !u.is_empty())
        .ok_or_else(|| {
            anyhow::anyhow!("`rabbit profile add` needs --rabbit-url http://host:15672")
        })?;
    let user = connection.user.clone();
    let password = password.or_else(|| {
        store
            .profiles
            .get(&add.name)
            .filter(|old| old.user == user)
            .and_then(|old| old.password.clone())
    });
    if password.is_some() && user.is_none() {
        anyhow::bail!("A password needs --rabbit-user");
    }
    Ok(RabbitProfile {
        management_url: url.to_string(),
        vhost: connection.vhost.clone().unwrap_or_else(|| "/".to_string()),
        user,
        password,
        password_in_store: false,
        amqp_url: connection
            .amqp_url
            .as_deref()
            .map(str::trim)
            .filter(|u| !u.is_empty())
            .map(str::to_string),
        read_only: add.read_only,
        description: add.description.clone(),
        ca_cert: tls_file(connection.ca_cert.as_deref(), "--rabbit-ca-cert")?,
        client_cert: tls_file(connection.client_cert.as_deref(), "--rabbit-client-cert")?,
        client_key: tls_file(connection.client_key.as_deref(), "--rabbit-client-key")?,
    })
}

/// A TLS file for a profile: it must exist, and is stored as an absolute
/// path so the profile works from any directory.
fn tls_file(path: Option<&str>, option: &str) -> anyhow::Result<Option<String>> {
    let Some(path) = path.map(str::trim).filter(|p| !p.is_empty()) else {
        return Ok(None);
    };
    let absolute =
        std::path::absolute(path).map_err(|e| anyhow::anyhow!("{option} '{path}': {e}"))?;
    if !absolute.is_file() {
        anyhow::bail!("{option}: no such file '{}'", absolute.display());
    }
    Ok(Some(absolute.display().to_string()))
}

/// `rabbit profile use|remove -f json`: the profile and what was done
/// (`activated` or `removed`).
fn profile_action_json(name: &str, action: &str) -> serde_json::Value {
    serde_json::json!({ "profile": name, "action": action })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn profile_actions_name_the_profile_and_what_happened() {
        let value = profile_action_json("prod", "removed");
        assert_eq!(
            value,
            serde_json::json!({"profile": "prod", "action": "removed"})
        );
    }

    fn add(name: &str) -> RabbitProfileAddArgs {
        RabbitProfileAddArgs {
            name: name.to_string(),
            read_only: true,
            description: None,
            set_active: false,
        }
    }

    fn connection(url: Option<&str>, user: Option<&str>) -> RabbitConnection {
        RabbitConnection {
            profile: None,
            url: url.map(str::to_string),
            vhost: Some("shop".to_string()),
            user: user.map(str::to_string),
            password: None,
            password_stdin: false,
            amqp_url: Some(" amqps://amqp.prod ".to_string()),
            ca_cert: None,
            client_cert: None,
            client_key: None,
        }
    }

    #[test]
    fn add_stores_tls_files_as_absolute_paths_and_checks_they_exist() {
        let dir = std::env::temp_dir().join(format!("ferrika-rabbit-tls-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let ca = dir.join("ca.pem");
        std::fs::write(&ca, "pem").unwrap();
        let mut with_ca = connection(Some("https://rabbit.prod"), None);
        with_ca.ca_cert = Some(ca.display().to_string());
        let profile =
            profile_from(&add("prod"), &with_ca, None, &RabbitProfileStore::default()).unwrap();
        let stored = profile.ca_cert.unwrap();
        assert!(std::path::Path::new(&stored).is_absolute(), "{stored}");
        assert!(stored.ends_with("ca.pem"), "{stored}");

        with_ca.ca_cert = Some(dir.join("missing.pem").display().to_string());
        let err = profile_from(&add("prod"), &with_ca, None, &RabbitProfileStore::default())
            .unwrap_err()
            .to_string();
        assert!(err.contains("--rabbit-ca-cert: no such file"), "{err}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn add_takes_the_connection_options() {
        let profile = profile_from(
            &add("prod"),
            &connection(Some("https://rabbit.prod"), Some("ops")),
            Some("pw".to_string()),
            &RabbitProfileStore::default(),
        )
        .unwrap();
        assert_eq!(profile.management_url, "https://rabbit.prod");
        assert_eq!(profile.vhost, "shop");
        assert_eq!(profile.user.as_deref(), Some("ops"));
        assert_eq!(profile.password.as_deref(), Some("pw"));
        assert_eq!(profile.amqp_url.as_deref(), Some("amqps://amqp.prod"));
        assert!(profile.read_only);
    }

    #[test]
    fn add_needs_a_url_and_a_user_for_a_password() {
        let store = RabbitProfileStore::default();
        assert!(profile_from(&add("p"), &connection(None, None), None, &store).is_err());
        assert!(profile_from(
            &add("p"),
            &connection(Some("http://r"), None),
            Some("pw".to_string()),
            &store
        )
        .is_err());
    }

    #[test]
    fn replacing_a_profile_keeps_the_password_of_the_same_user() {
        let mut store = RabbitProfileStore::default();
        store
            .insert(
                "prod",
                RabbitProfile {
                    user: Some("ops".to_string()),
                    password: Some("old-pw".to_string()),
                    ..RabbitProfile::new("https://rabbit.prod")
                },
            )
            .unwrap();
        let same_user = profile_from(
            &add("prod"),
            &connection(Some("https://rabbit.prod"), Some("ops")),
            None,
            &store,
        )
        .unwrap();
        assert_eq!(same_user.password.as_deref(), Some("old-pw"));
        let other_user = profile_from(
            &add("prod"),
            &connection(Some("https://rabbit.prod"), Some("admin")),
            None,
            &store,
        )
        .unwrap();
        assert_eq!(other_user.password, None);
    }

    #[test]
    fn views_say_where_the_password_is_never_what_it_is() {
        let in_file = RabbitProfile {
            user: Some("ops".to_string()),
            password: Some("file-pw".to_string()),
            ..RabbitProfile::new("http://rabbit")
        };
        let view = ProfileView::new("dev", &in_file, true);
        assert_eq!(view.password, "file");
        assert_eq!(view.amqp_url, "amqp://rabbit:5672");
        let text = serde_json::to_string(&view).unwrap();
        assert!(!text.contains("file-pw"), "{text}");
        let stored = RabbitProfile {
            password: None,
            password_in_store: true,
            ..in_file
        };
        assert_eq!(password_location(&stored), "os-credential-store");
        assert_eq!(
            password_location(&RabbitProfile::new("http://rabbit")),
            "none"
        );
    }
}
