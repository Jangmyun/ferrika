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
use ferrika_rabbitmq::model::{Binding, Exchange, Queue};
use ferrika_rabbitmq::profile::RabbitProfileStore;

/// The RabbitMQ read tools.
pub(super) const RABBIT_TOOLS: [&str; 3] = ["rabbit_queues", "rabbit_exchanges", "rabbit_bindings"];

/// Most entries one call returns.
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
            "filter": { "type": "string", "description": "Only entries whose name contains this text" }
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
    let filter = text_arg(args, "filter");
    let rabbit = |e: ferrika_rabbitmq::error::RabbitError| KafkaError::config(e.to_string());
    let mut result = match name {
        "rabbit_queues" => queues_json(&client.queues(vhost).await.map_err(rabbit)?, filter),
        "rabbit_exchanges" => {
            exchanges_json(&client.exchanges(vhost).await.map_err(rabbit)?, filter)
        }
        "rabbit_bindings" => bindings_json(&client.bindings(vhost).await.map_err(rabbit)?, filter),
        other => return Err(KafkaError::config(format!("Unknown tool '{other}'"))),
    };
    result["profile"] = json!(resolved.profile);
    result["vhost"] = json!(vhost);
    Ok(result)
}

/// `items` kept by `keep`, at most [`LIST_MAX`], as `{count, truncated, key: [...]}`.
fn listing<T>(
    key: &str,
    items: &[T],
    keep: impl Fn(&T) -> bool,
    show: impl Fn(&T) -> Value,
) -> Value {
    let kept: Vec<&T> = items.iter().filter(|i| keep(i)).collect();
    let shown: Vec<Value> = kept.iter().take(LIST_MAX).map(|i| show(i)).collect();
    let mut out = json!({
        "count": kept.len(),
        "truncated": kept.len() > LIST_MAX,
    });
    out[key] = Value::Array(shown);
    out
}

fn contains(text: &str, filter: Option<&str>) -> bool {
    filter.is_none_or(|f| text.contains(f))
}

pub(super) fn queues_json(queues: &[Queue], filter: Option<&str>) -> Value {
    listing(
        "queues",
        queues,
        |q| contains(&q.name, filter),
        |q| {
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
        },
    )
}

pub(super) fn exchanges_json(exchanges: &[Exchange], filter: Option<&str>) -> Value {
    listing(
        "exchanges",
        exchanges,
        |e| contains(e.display_name(), filter),
        |e| {
            json!({
                "name": e.display_name(),
                "type": e.kind,
                "durable": e.durable,
                "auto_delete": e.auto_delete,
                "internal": e.internal,
                "arguments": e.arguments,
            })
        },
    )
}

pub(super) fn bindings_json(bindings: &[Binding], filter: Option<&str>) -> Value {
    listing(
        "bindings",
        bindings,
        |b| contains(&b.source, filter) || contains(&b.destination, filter),
        |b| {
            json!({
                "source": b.source,
                "destination": b.destination,
                "destination_type": b.destination_type,
                "routing_key": b.routing_key,
                "arguments": b.arguments,
            })
        },
    )
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
    fn listings_filter_and_cap() {
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
        let queues: Vec<Queue> = (0..LIST_MAX + 5)
            .map(|i| queue(&format!("orders-{i}")))
            .chain([queue("payments")])
            .collect();
        let all = queues_json(&queues, None);
        assert_eq!(all["count"], LIST_MAX + 6);
        assert_eq!(all["truncated"], true);
        assert_eq!(all["queues"].as_array().unwrap().len(), LIST_MAX);
        let one = queues_json(&queues, Some("pay"));
        assert_eq!(one["count"], 1);
        assert_eq!(one["queues"][0]["dead_letter_exchange"], "dlx");
        assert_eq!(one["queues"][0]["messages_ready"], 3);

        let exchanges: Vec<Exchange> = serde_json::from_value(json!([
            { "name": "", "vhost": "/", "type": "direct" },
            { "name": "dlx", "vhost": "/", "type": "fanout" }
        ]))
        .unwrap();
        let shown = exchanges_json(&exchanges, Some("dlx"));
        assert_eq!(shown["count"], 1);
        assert_eq!(shown["exchanges"][0]["type"], "fanout");

        let bindings: Vec<Binding> = serde_json::from_value(json!([
            { "source": "dlx", "vhost": "/", "destination": "orders.dlq", "destination_type": "queue", "routing_key": "#" },
            { "source": "events", "vhost": "/", "destination": "audit", "destination_type": "queue" }
        ]))
        .unwrap();
        assert_eq!(bindings_json(&bindings, Some("dlq"))["count"], 1);
        assert_eq!(bindings_json(&bindings, Some("events"))["count"], 1);
        assert_eq!(bindings_json(&bindings, None)["count"], 2);
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
