//! RabbitMQ read tools (preview): queues, exchanges and bindings of a
//! virtual host through the management HTTP API (`ferrika_rabbitmq`).
//!
//! They read metadata only: no message body is fetched. Peeking at a
//! RabbitMQ queue takes messages and requeues them, which changes state,
//! so it is a write tool (`rabbit_writes.rs`, with `--allow-write`). The
//! connection is always a RabbitMQ profile
//! (`ferrika rabbit profile add`): the one named by `profile`, else the
//! active one. A URL cannot be passed, so a profile's stored password only
//! ever goes to that profile's broker.

use std::time::Duration;

use serde_json::{json, Value};

use ferrika_core::error::{KafkaError, KafkaResult};
use ferrika_rabbitmq::connection::{
    CredentialsError, RabbitOverrides, ResolveError, ResolvedRabbit,
};
use ferrika_rabbitmq::listing::{ListQuery, Listing};
use ferrika_rabbitmq::model::{Binding, Exchange, Queue};
use ferrika_rabbitmq::profile::RabbitProfileStore;

/// The RabbitMQ read tools.
pub(super) const RABBIT_TOOLS: [&str; 3] = ["rabbit_queues", "rabbit_exchanges", "rabbit_bindings"];

/// Most entries one call returns. The broker pages and filters the
/// listing, so a vhost with thousands of queues is never fetched whole
/// (bindings excepted: the management API does not page them).
const LIST_MAX: usize = 500;
/// How long one management API request may take.
const RABBIT_TIMEOUT: Duration = Duration::from_secs(10);

/// The tools' definitions for `tools/list`.
pub(super) fn definitions() -> Vec<Value> {
    let read_only = json!({ "readOnlyHint": true, "openWorldHint": false });
    let schema = json!({
        "type": "object",
        "properties": {
            "profile": { "type": "string", "description": "RabbitMQ profile name (ferrika rabbit profile list); default: the active RabbitMQ profile" },
            "vhost": { "type": "string", "description": "Virtual host (default: the profile's)" },
            "filter": { "type": "string", "description": "Only entries whose name contains this text (case-insensitive; for bindings the source or the destination)" },
            "regex": { "type": "boolean", "description": "Treat filter as a regular expression (default false)" }
        }
    });
    vec![
        json!({
            "name": "rabbit_queues",
            "description": "RabbitMQ (preview): queues of a virtual host with type, state, ready and unacknowledged message counts, consumers and dead-letter exchange. No message bodies are read.",
            "inputSchema": schema.clone(),
            "annotations": read_only
        }),
        json!({
            "name": "rabbit_exchanges",
            "description": "RabbitMQ (preview): exchanges of a virtual host with type, durability and whether they are internal.",
            "inputSchema": schema.clone(),
            "annotations": read_only
        }),
        json!({
            "name": "rabbit_bindings",
            "description": "RabbitMQ (preview): bindings of a virtual host: source exchange, destination queue or exchange, routing key. filter matches the source or the destination.",
            "inputSchema": schema,
            "annotations": read_only
        }),
    ]
}

fn text_arg<'a>(args: &'a Value, name: &str) -> Option<&'a str> {
    args.get(name)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
}

/// The connection `args` name: the RabbitMQ profile `profile`, else the
/// active one; `vhost` overrides the profile's. `load_store` reads the
/// profiles (and their passwords).
pub(super) fn connection(
    args: &Value,
    load_store: impl FnOnce() -> Result<RabbitProfileStore, String>,
) -> KafkaResult<ResolvedRabbit> {
    let overrides = RabbitOverrides {
        profile: text_arg(args, "profile"),
        vhost: text_arg(args, "vhost"),
        ..RabbitOverrides::default()
    };
    ResolvedRabbit::resolve(&overrides, load_store).map_err(|e| match e {
        ResolveError::NoConnection => KafkaError::config(
            "No RabbitMQ profile: name one with 'profile', or add one and make it active with \
             `ferrika rabbit profile add NAME --rabbit-url URL`",
        ),
        ResolveError::Profile(reason) => KafkaError::config(reason),
    })
}

/// The login of `resolved`: the profile's user and stored password.
pub(super) fn credentials(resolved: &ResolvedRabbit) -> KafkaResult<Option<(String, String)>> {
    resolved.credentials(None).map_err(|e| {
        KafkaError::config(match e {
            CredentialsError::MissingPassword => {
                "The RabbitMQ profile has a user but no stored password; store one with \
                 `ferrika rabbit profile add`"
            }
            CredentialsError::PasswordWithoutUser => "The RabbitMQ profile has no user",
        })
    })
}

/// Runs RabbitMQ read tool `name`.
pub(super) async fn call(name: &str, args: &Value) -> KafkaResult<Value> {
    let resolved = connection(args, || {
        RabbitProfileStore::load().map_err(|e| e.to_string())
    })?;
    let credentials = credentials(&resolved)?;
    let client = resolved
        .client(credentials.as_ref(), RABBIT_TIMEOUT)
        .map_err(KafkaError::config)?;
    let vhost = resolved.vhost.as_str();
    let query = list_query(args);
    let rabbit = |e: ferrika_rabbitmq::error::RabbitError| KafkaError::config(e.to_string());
    let mut result = match name {
        "rabbit_queues" => queues_json(&client.list_queues(vhost, &query).await.map_err(rabbit)?),
        "rabbit_exchanges" => {
            exchanges_json(&client.list_exchanges(vhost, &query).await.map_err(rabbit)?)
        }
        "rabbit_bindings" => {
            bindings_json(&client.list_bindings(vhost, &query).await.map_err(rabbit)?)
        }
        other => return Err(KafkaError::config(format!("Unknown tool '{other}'"))),
    };
    result["profile"] = json!(resolved.profile);
    result["vhost"] = json!(vhost);
    Ok(result)
}

/// The listing `args` ask for: `filter` (and `regex`), at most
/// [`LIST_MAX`] entries.
fn list_query(args: &Value) -> ListQuery {
    ListQuery {
        name: text_arg(args, "filter").map(str::to_string),
        regex: args.get("regex").and_then(Value::as_bool).unwrap_or(false),
        limit: LIST_MAX,
    }
}

/// `listing` as `{count, truncated, key: [...]}`; `count` is every
/// matching entry, fetched or not.
fn listing_json<T>(key: &str, listing: &Listing<T>, show: impl Fn(&T) -> Value) -> Value {
    let mut out = json!({
        "count": listing.matched,
        "truncated": listing.truncated(),
    });
    out[key] = Value::Array(listing.items.iter().map(show).collect());
    out
}

pub(super) fn queues_json(queues: &Listing<Queue>) -> Value {
    listing_json("queues", queues, |q| {
        json!({
            "name": q.name,
            "type": q.kind,
            "state": q.state,
            "durable": q.durable,
            "auto_delete": q.auto_delete,
            "exclusive": q.exclusive,
            "messages": q.messages,
            "messages_ready": q.messages_ready,
            "messages_unacknowledged": q.messages_unacknowledged,
            "consumers": q.consumers,
            "dead_letter_exchange": q.dead_letter_exchange(),
            "policy": q.policy,
            "arguments": q.arguments,
        })
    })
}

pub(super) fn exchanges_json(exchanges: &Listing<Exchange>) -> Value {
    listing_json("exchanges", exchanges, |e| {
        json!({
            "name": e.display_name(),
            "type": e.kind,
            "durable": e.durable,
            "auto_delete": e.auto_delete,
            "internal": e.internal,
            "arguments": e.arguments,
        })
    })
}

pub(super) fn bindings_json(bindings: &Listing<Binding>) -> Value {
    listing_json("bindings", bindings, |b| {
        json!({
            "source": b.source,
            "destination": b.destination,
            "destination_type": b.destination_type,
            "routing_key": b.routing_key,
            "arguments": b.arguments,
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrika_rabbitmq::profile::RabbitProfile;

    fn store() -> RabbitProfileStore {
        let mut store = RabbitProfileStore::default();
        store.profiles.insert(
            "dev".into(),
            RabbitProfile {
                user: Some("ops".into()),
                password: Some("s3cret".into()),
                vhost: "orders".into(),
                ..RabbitProfile::new("http://rabbit-dev:15672")
            },
        );
        store.profiles.insert(
            "prod".into(),
            RabbitProfile::new("http://rabbit-prod:15672"),
        );
        store.active_profile = Some("dev".into());
        store
    }

    #[test]
    fn the_connection_is_the_named_or_the_active_profile() {
        let active = connection(&json!({}), || Ok(store())).unwrap();
        assert_eq!(active.profile.as_deref(), Some("dev"));
        assert_eq!(active.vhost, "orders");
        assert_eq!(
            active.credentials(None).unwrap(),
            Some(("ops".to_string(), "s3cret".to_string()))
        );
        let named =
            connection(&json!({ "profile": "prod", "vhost": "/" }), || Ok(store())).unwrap();
        assert_eq!(named.profile.as_deref(), Some("prod"));
        assert_eq!(named.vhost, "/");
        assert!(connection(&json!({ "profile": "nope" }), || Ok(store())).is_err());
        let none = connection(&json!({}), || Ok(RabbitProfileStore::default())).unwrap_err();
        assert!(none.to_string().contains("No RabbitMQ profile"), "{none}");
        // A URL argument is not an option: it is ignored, never connected to.
        let url = connection(
            &json!({ "url": "http://evil:15672", "profile": "dev" }),
            || Ok(store()),
        )
        .unwrap();
        assert_eq!(url.management_url, "http://rabbit-dev:15672");
    }

    #[test]
    fn listings_report_the_matching_count_and_the_cut() {
        let queue = |name: &str| -> Queue {
            serde_json::from_value(json!({
                "name": name,
                "vhost": "/",
                "type": "quorum",
                "messages_ready": 3,
                "arguments": { "x-dead-letter-exchange": "dlx" }
            }))
            .unwrap()
        };
        let cut = Listing {
            items: (0..LIST_MAX)
                .map(|i| queue(&format!("orders-{i}")))
                .collect(),
            matched: LIST_MAX as u64 + 6,
            total: 2000,
        };
        let all = queues_json(&cut);
        assert_eq!(all["count"], LIST_MAX + 6);
        assert_eq!(all["truncated"], true);
        assert_eq!(all["queues"].as_array().unwrap().len(), LIST_MAX);
        let one = queues_json(&Listing {
            items: vec![queue("payments")],
            matched: 1,
            total: 2000,
        });
        assert_eq!(one["count"], 1);
        assert_eq!(one["truncated"], false);
        assert_eq!(one["queues"][0]["dead_letter_exchange"], "dlx");
        assert_eq!(one["queues"][0]["messages_ready"], 3);

        let exchanges: Vec<Exchange> = serde_json::from_value(json!([
            { "name": "", "vhost": "/", "type": "direct" },
        ]))
        .unwrap();
        let shown = exchanges_json(&Listing {
            items: exchanges,
            matched: 1,
            total: 8,
        });
        assert_eq!(shown["exchanges"][0]["name"], "amq.default");

        let bindings: Vec<Binding> = serde_json::from_value(json!([
            { "source": "dlx", "vhost": "/", "destination": "orders.dlq", "destination_type": "queue", "routing_key": "#" },
        ]))
        .unwrap();
        let shown = bindings_json(&Listing {
            items: bindings,
            matched: 1,
            total: 1,
        });
        assert_eq!(shown["bindings"][0]["routing_key"], "#");
    }

    #[test]
    fn the_filter_goes_to_the_broker_with_the_cap() {
        let query = list_query(&json!({ "filter": " orders ", "regex": true }));
        assert_eq!(query.name.as_deref(), Some("orders"));
        assert!(query.regex);
        assert_eq!(query.limit, LIST_MAX);
        let none = list_query(&json!({ "filter": "  " }));
        assert_eq!(none.name, None);
        assert!(!none.regex);
    }

    #[test]
    fn definitions_are_read_only() {
        let defs = definitions();
        assert_eq!(defs.len(), RABBIT_TOOLS.len());
        for (def, name) in defs.iter().zip(RABBIT_TOOLS) {
            assert_eq!(def["name"], name);
            assert_eq!(def["annotations"]["readOnlyHint"], true);
            assert!(def["inputSchema"]["properties"].get("url").is_none());
        }
    }
}
