//! `ferrika mqtt profile`: named MQTT connections in `mqtt-profiles.json`
//! (next to `profiles.json`), passwords in the OS credential store.
//! Separate from the Kafka (`ferrika profile`) and RabbitMQ (`ferrika rabbit
//! profile`) profiles.
//!
//! `add` takes the connection from the same options every `mqtt` command
//! takes (`--mqtt-url`, `--mqtt-user`, `--mqtt-password-stdin`,
//! `--mqtt-version`, `--mqtt-ca-file`).

use clap::{Args, Subcommand};
use colored::Colorize;
use comfy_table::Cell;
use serde::Serialize;

use crate::cli::mqtt::MqttConnection;
use crate::cli::output;
use crate::mqtt::profile::{MqttProfile, MqttProfileStore};

#[derive(Args, Debug, Clone, PartialEq, Eq)]
pub struct MqttProfileArgs {
    #[command(subcommand)]
    pub command: MqttProfileSubcommands,
}

#[derive(Subcommand, Debug, Clone, PartialEq, Eq)]
pub enum MqttProfileSubcommands {
    /// Add or replace a profile from --mqtt-url, --mqtt-user, the password
    /// (--mqtt-password-stdin or FERRIKA_MQTT_PASSWORD), --mqtt-version and
    /// --mqtt-ca-file
    Add(MqttProfileAddArgs),
    /// List the profiles
    List,
    /// Make a profile the one `mqtt` commands use by default
    Use { name: String },
    /// Remove a profile and its stored password
    Remove { name: String },
    /// Show a profile (default: the active one); never the password
    Show { name: Option<String> },
}

#[derive(Args, Debug, Clone, PartialEq, Eq)]
pub struct MqttProfileAddArgs {
    /// Profile name (letters, digits, '-', '_', '.')
    pub name: String,
    /// Refuse commands that change this broker's messages (pub, retained
    /// clear)
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
    url: String,
    version: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    user: Option<String>,
    /// `os-credential-store`, `file` or `none`.
    password: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    ca_file: Option<String>,
    read_only: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
}

impl ProfileView {
    fn new(name: &str, profile: &MqttProfile, active: bool) -> Self {
        Self {
            name: name.to_string(),
            active,
            url: profile.url.clone(),
            version: profile.version.as_str(),
            user: profile.user.clone(),
            password: password_location(profile),
            ca_file: profile.ca_file.as_ref().map(|p| p.display().to_string()),
            read_only: profile.read_only,
            description: profile.description.clone(),
        }
    }
}

/// Where a profile's password is kept, never the password.
fn password_location(profile: &MqttProfile) -> &'static str {
    match (profile.password_in_store, profile.password.is_some()) {
        (true, _) => "os-credential-store",
        (false, true) => "file",
        (false, false) => "none",
    }
}

pub fn run(args: &MqttProfileArgs, connection: &MqttConnection, json: bool) -> anyhow::Result<()> {
    let mut store = MqttProfileStore::load()?;
    match &args.command {
        MqttProfileSubcommands::Add(add) => {
            let password = connection.given_password().map_err(anyhow::Error::msg)?;
            let profile = profile_from(add, connection, password, &store)?;
            let replaced = store.profiles.contains_key(&add.name);
            store.insert(&add.name, profile)?;
            if add.set_active {
                store.set_active(&add.name)?;
            }
            store.save()?;
            // Read back what was stored: where the password went.
            let saved = MqttProfileStore::load_file(&MqttProfileStore::default_path())?;
            let stored = saved.get(&add.name)?;
            if json {
                output::print_json(&ProfileView::new(
                    &add.name,
                    stored,
                    saved.active_profile.as_deref() == Some(add.name.as_str()),
                ))?;
            } else {
                println!(
                    "{} {} MQTT profile '{}' ({}).",
                    "✔".green().bold(),
                    if replaced { "Replaced" } else { "Added" },
                    add.name.cyan(),
                    stored.url
                );
                match password_location(stored) {
                    "file" => eprintln!(
                        "{}",
                        format!(
                            "No OS credential store: the password is kept in {}.",
                            MqttProfileStore::default_path().display()
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
        MqttProfileSubcommands::List => {
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
                        "No MQTT profiles. Add one with `ferrika mqtt profile add NAME \
                         --mqtt-url URL`."
                    );
                    return;
                }
                let mut table =
                    output::table(vec!["", "NAME", "URL", "VERSION", "USER", "READ-ONLY"]);
                for v in views {
                    table.add_row(vec![
                        Cell::new(if v.active { "*" } else { "" }),
                        Cell::new(&v.name),
                        Cell::new(&v.url),
                        Cell::new(v.version),
                        Cell::new(v.user.as_deref().unwrap_or_default()),
                        Cell::new(v.read_only),
                    ]);
                }
                println!("{table}");
            })?;
        }
        MqttProfileSubcommands::Use { name } => {
            store.set_active(name)?;
            store.save()?;
            if json {
                output::print_json(&profile_action_json(name, "activated"))?;
                return Ok(());
            }
            println!(
                "{} MQTT commands now use profile '{}'.",
                "✔".green().bold(),
                name.cyan()
            );
        }
        MqttProfileSubcommands::Remove { name } => {
            store.remove(name)?;
            store.save()?;
            if json {
                output::print_json(&profile_action_json(name, "removed"))?;
                return Ok(());
            }
            println!(
                "{} Removed MQTT profile '{}'.",
                "✔".green().bold(),
                name.cyan()
            );
        }
        MqttProfileSubcommands::Show { name } => {
            let (name, profile) = match name {
                Some(name) => (name.as_str(), store.get(name)?),
                None => store.active().ok_or_else(|| {
                    anyhow::anyhow!("No active MQTT profile (see `ferrika mqtt profile list`)")
                })?,
            };
            let view =
                ProfileView::new(name, profile, store.active_profile.as_deref() == Some(name));
            output::emit(json, &view, |v| {
                println!("{}", v.name.cyan().bold());
                println!("  URL:        {}", v.url);
                println!("  version:    {}", v.version);
                println!("  user:       {}", v.user.as_deref().unwrap_or("(none)"));
                println!("  password:   {}", v.password);
                println!(
                    "  CA file:    {}",
                    v.ca_file.as_deref().unwrap_or("(OS trust store)")
                );
                println!("  read-only:  {}", v.read_only);
                println!("  active:     {}", v.active);
                if let Some(d) = &v.description {
                    println!("  description: {d}");
                }
            })?;
        }
    }
    Ok(())
}

/// The profile `add` stores. Replacing a profile keeps its password when
/// no new one is given and the user stays the same. A CA file is stored as
/// an absolute path, so the profile works from any directory.
fn profile_from(
    add: &MqttProfileAddArgs,
    connection: &MqttConnection,
    password: Option<String>,
    store: &MqttProfileStore,
) -> anyhow::Result<MqttProfile> {
    let url = connection
        .url
        .as_deref()
        .map(str::trim)
        .filter(|u| !u.is_empty())
        .ok_or_else(|| anyhow::anyhow!("`mqtt profile add` needs --mqtt-url mqtt://host:1883"))?;
    let user = connection.user.clone();
    let password = password.or_else(|| {
        store
            .profiles
            .get(&add.name)
            .filter(|old| old.user == user)
            .and_then(|old| old.password.clone())
    });
    if password.is_some() && user.is_none() {
        anyhow::bail!("A password needs --mqtt-user");
    }
    let ca_file = match &connection.ca_file {
        Some(path) => {
            let absolute = std::path::absolute(path)
                .map_err(|e| anyhow::anyhow!("Cannot use CA file {}: {e}", path.display()))?;
            if !absolute.is_file() {
                anyhow::bail!("No CA file at {}", absolute.display());
            }
            Some(absolute)
        }
        None => None,
    };
    Ok(MqttProfile {
        url: url.to_string(),
        version: connection.version.unwrap_or_default(),
        user,
        password,
        password_in_store: false,
        ca_file,
        read_only: add.read_only,
        description: add.description.clone(),
    })
}

/// `mqtt profile use|remove -f json`: the profile and what was done.
fn profile_action_json(name: &str, action: &str) -> serde_json::Value {
    serde_json::json!({ "profile": name, "action": action })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mqtt::profile::MqttVersion;

    fn add(name: &str) -> MqttProfileAddArgs {
        MqttProfileAddArgs {
            name: name.to_string(),
            read_only: true,
            description: None,
            set_active: false,
        }
    }

    fn connection(url: Option<&str>, user: Option<&str>) -> MqttConnection {
        MqttConnection {
            profile: None,
            url: url.map(str::to_string),
            user: user.map(str::to_string),
            password: None,
            password_stdin: false,
            version: Some(MqttVersion::V5),
            ca_file: None,
        }
    }

    #[test]
    fn add_takes_the_connection_options() {
        let profile = profile_from(
            &add("prod"),
            &connection(Some(" mqtts://broker.prod "), Some("ops")),
            Some("pw".to_string()),
            &MqttProfileStore::default(),
        )
        .unwrap();
        assert_eq!(profile.url, "mqtts://broker.prod");
        assert_eq!(profile.version, MqttVersion::V5);
        assert_eq!(profile.user.as_deref(), Some("ops"));
        assert_eq!(profile.password.as_deref(), Some("pw"));
        assert!(profile.read_only);
    }

    #[test]
    fn add_needs_a_url_a_user_for_a_password_and_an_existing_ca_file() {
        let store = MqttProfileStore::default();
        assert!(profile_from(&add("p"), &connection(None, None), None, &store).is_err());
        assert!(profile_from(
            &add("p"),
            &connection(Some("mqtt://b"), None),
            Some("pw".to_string()),
            &store
        )
        .is_err());
        let missing_ca = MqttConnection {
            ca_file: Some(std::path::PathBuf::from("no-such-ca-file.pem")),
            ..connection(Some("mqtts://b"), None)
        };
        let err = profile_from(&add("p"), &missing_ca, None, &store).unwrap_err();
        assert!(err.to_string().contains("No CA file"), "{err}");
    }

    #[test]
    fn replacing_a_profile_keeps_the_password_of_the_same_user() {
        let mut store = MqttProfileStore::default();
        store
            .insert(
                "prod",
                MqttProfile {
                    user: Some("ops".to_string()),
                    password: Some("old-pw".to_string()),
                    ..MqttProfile::new("mqtt://broker")
                },
            )
            .unwrap();
        let same_user = profile_from(
            &add("prod"),
            &connection(Some("mqtt://broker"), Some("ops")),
            None,
            &store,
        )
        .unwrap();
        assert_eq!(same_user.password.as_deref(), Some("old-pw"));
        let other_user = profile_from(
            &add("prod"),
            &connection(Some("mqtt://broker"), Some("admin")),
            None,
            &store,
        )
        .unwrap();
        assert_eq!(other_user.password, None);
    }

    #[test]
    fn views_say_where_the_password_is_never_what_it_is() {
        let in_file = MqttProfile {
            user: Some("ops".to_string()),
            password: Some("file-pw".to_string()),
            ..MqttProfile::new("mqtt://broker")
        };
        let view = ProfileView::new("dev", &in_file, true);
        assert_eq!(view.password, "file");
        assert_eq!(view.version, "3.1.1");
        let text = serde_json::to_string(&view).unwrap();
        assert!(!text.contains("file-pw"), "{text}");
        let stored = MqttProfile {
            password: None,
            password_in_store: true,
            ..in_file
        };
        assert_eq!(password_location(&stored), "os-credential-store");
        assert_eq!(password_location(&MqttProfile::new("mqtt://b")), "none");
        assert_eq!(
            profile_action_json("prod", "removed"),
            serde_json::json!({"profile": "prod", "action": "removed"})
        );
    }
}
