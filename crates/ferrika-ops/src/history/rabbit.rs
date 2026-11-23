//! History and undo for RabbitMQ topology changes (queues, exchanges,
//! bindings), through the management API: reading the states a change is
//! about to alter ([`queue_declared`], [`queue_deleted`], ...), reading them
//! again ([`recapture`]), and planning and running an undo
//! ([`prepare_request`], [`execute_steps`]) with the same decision rules as
//! the Kafka undo in [`super::undo`].
//!
//! Every RabbitMQ change records at least one `rabbit_*` state in its
//! before-state. Ferrika 0.2 does not know those kinds, so it skips the
//! whole journal line (and the operation) instead of showing it as an
//! operation on an unknown cluster whose audit digest does not match.

use std::collections::BTreeMap;

use serde_json::{Map, Value};

use ferrika_core::error::{KafkaError, KafkaResult};
use ferrika_core::model::history::{
    Operation, RabbitBindingKey, RabbitExchangeDeclaration, RabbitQueueDeclaration, Reversibility,
    Scope, State,
};
use ferrika_rabbitmq::client::ManagementClient;
use ferrika_rabbitmq::error::{RabbitError, RabbitResult};
use ferrika_rabbitmq::model::{
    Binding, BindingDeclaration, DestinationType, Exchange, ExchangeDeclaration, Queue,
    QueueDeclaration,
};

use super::conflict;
use super::inverse::{self, NotReversible, Step};
use super::undo::{self, AuditDigests, UndoFailure, UndoOptions, UndoPlan, UndoRequest};
use super::Change;

/// The scope of a change to the RabbitMQ broker whose management API is at
/// `address` (`host:port`), made with RabbitMQ profile `profile` (may be
/// empty).
pub fn scope(profile: &str, address: &str) -> Scope {
    Scope {
        profile: profile.to_string(),
        rabbitmq: address.to_string(),
        ..Scope::default()
    }
}

fn to_btree(map: &Map<String, Value>) -> BTreeMap<String, Value> {
    map.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
}

fn to_map(map: &BTreeMap<String, Value>) -> Map<String, Value> {
    map.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
}

/// How `queue` is declared, as the history keeps it: its type once (the
/// `x-queue-type` argument, which some versions list, is left out).
pub fn queue_declaration(queue: &Queue) -> RabbitQueueDeclaration {
    let mut arguments = to_btree(&queue.arguments);
    arguments.remove("x-queue-type");
    RabbitQueueDeclaration {
        kind: if queue.kind.is_empty() {
            "classic".to_string()
        } else {
            queue.kind.clone()
        },
        durable: queue.durable,
        auto_delete: queue.auto_delete,
        arguments,
    }
}

/// The request that declares a queue as `declaration` says.
pub fn queue_request(declaration: &RabbitQueueDeclaration) -> QueueDeclaration {
    QueueDeclaration {
        kind: Some(declaration.kind.clone()).filter(|k| !k.is_empty()),
        durable: declaration.durable,
        auto_delete: declaration.auto_delete,
        arguments: to_map(&declaration.arguments),
    }
}

pub fn exchange_declaration(exchange: &Exchange) -> RabbitExchangeDeclaration {
    RabbitExchangeDeclaration {
        kind: exchange.kind.clone(),
        durable: exchange.durable,
        auto_delete: exchange.auto_delete,
        internal: exchange.internal,
        arguments: to_btree(&exchange.arguments),
    }
}

pub fn exchange_request(declaration: &RabbitExchangeDeclaration) -> ExchangeDeclaration {
    ExchangeDeclaration {
        kind: declaration.kind.clone(),
        durable: declaration.durable,
        auto_delete: declaration.auto_delete,
        internal: declaration.internal,
        arguments: to_map(&declaration.arguments),
    }
}

pub fn binding_key(binding: &BindingDeclaration) -> RabbitBindingKey {
    RabbitBindingKey {
        source: binding.source.clone(),
        destination: binding.destination.clone(),
        destination_type: binding.destination_type.as_str().to_string(),
        routing_key: binding.routing_key.clone(),
        arguments: to_btree(&binding.arguments),
    }
}

/// The binding `key` names, to add or remove it.
pub fn binding_request(key: &RabbitBindingKey) -> RabbitResult<BindingDeclaration> {
    let destination_type = DestinationType::parse(&key.destination_type).ok_or_else(|| {
        RabbitError::config(format!(
            "unknown binding destination type '{}'",
            key.destination_type
        ))
    })?;
    Ok(BindingDeclaration {
        source: key.source.clone(),
        destination: key.destination.clone(),
        destination_type,
        routing_key: key.routing_key.clone(),
        arguments: to_map(&key.arguments),
    })
}

/// Queue `name` of `vhost` as it is now.
pub async fn queue_state(
    client: &ManagementClient,
    vhost: &str,
    name: &str,
) -> RabbitResult<State> {
    let queue = client.queue(vhost, name).await?;
    Ok(State::RabbitQueue {
        vhost: vhost.to_string(),
        name: name.to_string(),
        declaration: queue.as_ref().map(queue_declaration),
    })
}

/// Exchange `name` of `vhost` as it is now.
pub async fn exchange_state(
    client: &ManagementClient,
    vhost: &str,
    name: &str,
) -> RabbitResult<State> {
    let exchange = client.exchange(vhost, name).await?;
    Ok(State::RabbitExchange {
        vhost: vhost.to_string(),
        name: name.to_string(),
        declaration: exchange.as_ref().map(exchange_declaration),
    })
}

/// Whether `binding` exists now.
pub async fn binding_state(
    client: &ManagementClient,
    vhost: &str,
    binding: &BindingDeclaration,
) -> RabbitResult<State> {
    let present = match client.bindings_between(vhost, binding).await {
        Ok(listed) => listed.iter().any(|b| binding.matches(b)),
        // The source or the destination does not exist: no binding.
        Err(RabbitError::Api { status: 404, .. }) => false,
        Err(e) => return Err(e),
    };
    Ok(State::RabbitBinding {
        vhost: vhost.to_string(),
        binding: binding_key(binding),
        present,
    })
}

/// How many messages queue `name` holds now.
pub async fn messages_state(
    client: &ManagementClient,
    vhost: &str,
    name: &str,
) -> RabbitResult<State> {
    let queue = client.queue(vhost, name).await?;
    Ok(State::RabbitQueueMessages {
        vhost: vhost.to_string(),
        name: name.to_string(),
        messages: queue.and_then(|q| q.messages),
    })
}

/// The listed bindings as present binding states (the default exchange's
/// implicit ones left out).
fn present_bindings(vhost: &str, bindings: &[Binding]) -> Vec<State> {
    bindings
        .iter()
        .filter_map(BindingDeclaration::of)
        .map(|binding| State::RabbitBinding {
            vhost: vhost.to_string(),
            binding: binding_key(&binding),
            present: true,
        })
        .collect()
}

/// The same states with every binding marked absent and every queue and
/// exchange gone: what a deletion leaves.
fn all_gone(states: &[State]) -> Vec<State> {
    states
        .iter()
        .map(|state| match state {
            State::RabbitQueue { vhost, name, .. } => State::RabbitQueue {
                vhost: vhost.clone(),
                name: name.clone(),
                declaration: None,
            },
            State::RabbitExchange { vhost, name, .. } => State::RabbitExchange {
                vhost: vhost.clone(),
                name: name.clone(),
                declaration: None,
            },
            State::RabbitBinding { vhost, binding, .. } => State::RabbitBinding {
                vhost: vhost.clone(),
                binding: binding.clone(),
                present: false,
            },
            other => other.clone(),
        })
        .collect()
}

/// Declaring queue `name`: refused when it exists already (undoing the
/// declaration would then delete a queue this change did not make). Undo
/// deletes it again, only while it is empty and unused.
pub async fn queue_declared(
    client: &ManagementClient,
    vhost: &str,
    name: &str,
    declaration: &QueueDeclaration,
) -> RabbitResult<Change> {
    if client.queue(vhost, name).await?.is_some() {
        return Err(RabbitError::config(format!(
            "Queue '{name}' already exists on vhost '{vhost}'; nothing was changed"
        )));
    }
    let before = State::RabbitQueue {
        vhost: vhost.to_string(),
        name: name.to_string(),
        declaration: None,
    };
    let mut arguments = to_btree(&declaration.arguments);
    arguments.remove("x-queue-type");
    let after = State::RabbitQueue {
        vhost: vhost.to_string(),
        name: name.to_string(),
        declaration: Some(RabbitQueueDeclaration {
            kind: declaration
                .kind
                .clone()
                .unwrap_or_else(|| "classic".to_string()),
            durable: declaration.durable,
            auto_delete: declaration.auto_delete,
            arguments,
        }),
    };
    Ok(
        Change::new(Reversibility::Conditional, vec![before], vec![after]).limit(
            "undo deletes the queue, only while it holds no messages, has no consumers and no \
         bindings added since",
        ),
    )
}

/// Deleting queue `name` with its bindings: undo declares it again with
/// the same settings and bindings, but its messages are lost.
pub async fn queue_deleted(
    client: &ManagementClient,
    vhost: &str,
    name: &str,
) -> RabbitResult<Change> {
    let queue = client.queue(vhost, name).await?.ok_or_else(|| {
        RabbitError::config(format!("Queue '{name}' does not exist on vhost '{vhost}'"))
    })?;
    let mut before = vec![State::RabbitQueue {
        vhost: vhost.to_string(),
        name: name.to_string(),
        declaration: Some(queue_declaration(&queue)),
    }];
    before.extend(present_bindings(
        vhost,
        &client.queue_bindings(vhost, name).await?,
    ));
    let after = all_gone(&before);
    let messages = queue.messages.unwrap_or(0);
    if queue.exclusive {
        return Ok(
            Change::new(Reversibility::Irreversible, before, after).limit(
                "an exclusive queue belongs to the connection that declared it and cannot be \
             declared again by Ferrika",
            ),
        );
    }
    Ok(
        Change::new(Reversibility::Partial, before, after).limit(format!(
            "undo declares the queue and its bindings again, empty: its messages ({messages} when \
         it was read) are not restored"
        )),
    )
}

/// Purging queue `name`: its messages are gone for good.
pub async fn queue_purged(
    client: &ManagementClient,
    vhost: &str,
    name: &str,
) -> RabbitResult<Change> {
    let before = messages_state(client, vhost, name).await?;
    let State::RabbitQueueMessages {
        messages: Some(_), ..
    } = &before
    else {
        return Err(RabbitError::config(format!(
            "Queue '{name}' does not exist on vhost '{vhost}'"
        )));
    };
    let after = State::RabbitQueueMessages {
        vhost: vhost.to_string(),
        name: name.to_string(),
        messages: Some(0),
    };
    Ok(
        Change::new(Reversibility::Irreversible, vec![before], vec![after])
            .limit("purged messages cannot be restored"),
    )
}

/// Declaring exchange `name`: refused when it exists already. Undo deletes
/// it, only while it has no bindings.
pub async fn exchange_declared(
    client: &ManagementClient,
    vhost: &str,
    name: &str,
    declaration: &ExchangeDeclaration,
) -> RabbitResult<Change> {
    if client.exchange(vhost, name).await?.is_some() {
        return Err(RabbitError::config(format!(
            "Exchange '{name}' already exists on vhost '{vhost}'; nothing was changed"
        )));
    }
    let state = |declaration| State::RabbitExchange {
        vhost: vhost.to_string(),
        name: name.to_string(),
        declaration,
    };
    let after = RabbitExchangeDeclaration {
        kind: declaration.kind.clone(),
        durable: declaration.durable,
        auto_delete: declaration.auto_delete,
        internal: declaration.internal,
        arguments: to_btree(&declaration.arguments),
    };
    Ok(Change::new(
        Reversibility::Conditional,
        vec![state(None)],
        vec![state(Some(after))],
    )
    .limit("undo deletes the exchange, only while it has no bindings added since"))
}

/// Deleting exchange `name` with every binding from or to it: undo
/// declares it again and restores those bindings (where their other end
/// still exists).
pub async fn exchange_deleted(
    client: &ManagementClient,
    vhost: &str,
    name: &str,
) -> RabbitResult<Change> {
    let exchange = client.exchange(vhost, name).await?.ok_or_else(|| {
        RabbitError::config(format!(
            "Exchange '{name}' does not exist on vhost '{vhost}'"
        ))
    })?;
    let mut before = vec![State::RabbitExchange {
        vhost: vhost.to_string(),
        name: name.to_string(),
        declaration: Some(exchange_declaration(&exchange)),
    }];
    before.extend(present_bindings(
        vhost,
        &client.exchange_bindings(vhost, name).await?,
    ));
    let after = all_gone(&before);
    Ok(Change::new(Reversibility::Reversible, before, after))
}

/// Adding (`add`) or removing `binding`.
pub async fn binding_changed(
    client: &ManagementClient,
    vhost: &str,
    binding: &BindingDeclaration,
    add: bool,
) -> RabbitResult<Change> {
    let before = binding_state(client, vhost, binding).await?;
    let after = State::RabbitBinding {
        vhost: vhost.to_string(),
        binding: binding_key(binding),
        present: add,
    };
    Ok(Change::new(
        Reversibility::Reversible,
        vec![before],
        vec![after],
    ))
}

/// Reads the resource `state` describes again, as it is now.
pub async fn recapture(state: &State, client: &ManagementClient) -> KafkaResult<State> {
    let read = match state {
        State::RabbitQueue { vhost, name, .. } => queue_state(client, vhost, name).await,
        State::RabbitQueueMessages { vhost, name, .. } => messages_state(client, vhost, name).await,
        State::RabbitExchange { vhost, name, .. } => exchange_state(client, vhost, name).await,
        State::RabbitBinding { vhost, binding, .. } => {
            let binding = binding_request(binding).map_err(rabbit_error)?;
            binding_state(client, vhost, &binding).await
        }
        other => {
            return Err(KafkaError::config(format!(
                "{} is not a RabbitMQ resource",
                other.subject()
            )))
        }
    };
    read.map_err(rabbit_error)
}

/// Reads every state again (the after-state of a change that just ran).
pub async fn recapture_all(states: &[State], client: &ManagementClient) -> KafkaResult<Vec<State>> {
    let mut out = Vec::with_capacity(states.len());
    for state in states {
        out.push(recapture(state, client).await?);
    }
    Ok(out)
}

/// A RabbitMQ error as the history's error type (the message is kept; it
/// never holds credentials).
pub fn rabbit_error(error: RabbitError) -> KafkaError {
    match error {
        RabbitError::Config(message) => KafkaError::config(message),
        other => KafkaError::admin(other.to_string()),
    }
}

/// Conditions that stop undo outright, even with `--force`: a queue to
/// delete holds messages, has consumers or bindings the undo does not
/// remove; an exchange to delete has such bindings; a binding to add has an
/// end that neither exists nor is declared by an earlier step.
pub async fn blockers(steps: &[Step], client: &ManagementClient) -> Vec<String> {
    let mut problems = Vec::new();
    let unbound: Vec<(&str, &RabbitBindingKey)> = steps
        .iter()
        .filter_map(|step| match step {
            Step::Unbind { vhost, binding } => Some((vhost.as_str(), binding)),
            _ => None,
        })
        .collect();
    // Bindings the plan does not remove itself.
    let kept = |vhost: &str, bindings: Vec<Binding>| -> Vec<BindingDeclaration> {
        bindings
            .iter()
            .filter_map(BindingDeclaration::of)
            .filter(|b| {
                let key = binding_key(b);
                !unbound.iter().any(|(v, k)| *v == vhost && **k == key)
            })
            .collect()
    };
    let mut declared: Vec<(&str, &str, DestinationType)> = Vec::new();
    for step in steps {
        match step {
            Step::DeleteQueue { vhost, name } => match client.queue(vhost, name).await {
                Ok(Some(queue)) => {
                    let messages = queue.messages.unwrap_or(0);
                    if messages > 0 {
                        problems.push(format!(
                            "queue {name} holds {messages} message(s); deleting it would lose \
                             them (purge it first if they are not needed)"
                        ));
                    }
                    let consumers = queue.consumers.unwrap_or(0);
                    if consumers > 0 {
                        problems.push(format!(
                            "queue {name} has {consumers} consumer(s); stop them first"
                        ));
                    }
                    match client.queue_bindings(vhost, name).await {
                        Ok(bindings) => {
                            let left = kept(vhost, bindings);
                            if !left.is_empty() {
                                problems.push(format!(
                                    "queue {name} has binding(s) added since ({}); remove them \
                                     first",
                                    labels(&left)
                                ));
                            }
                        }
                        Err(e) => {
                            problems.push(format!("cannot read the bindings of queue {name}: {e}"))
                        }
                    }
                }
                Ok(None) => {}
                Err(e) => problems.push(format!("cannot read queue {name}: {e}")),
            },
            Step::DeleteExchange { vhost, name } => {
                match client.exchange_bindings(vhost, name).await {
                    Ok(bindings) => {
                        let left = kept(vhost, bindings);
                        if !left.is_empty() {
                            problems.push(format!(
                                "exchange {name} has binding(s) added since ({}); remove them \
                                 first",
                                labels(&left)
                            ));
                        }
                    }
                    Err(RabbitError::Api { status: 404, .. }) => {}
                    Err(e) => {
                        problems.push(format!("cannot read the bindings of exchange {name}: {e}"))
                    }
                }
            }
            Step::DeclareQueue { vhost, name, .. } => {
                declared.push((vhost, name, DestinationType::Queue));
            }
            Step::DeclareExchange { vhost, name, .. } => {
                declared.push((vhost, name, DestinationType::Exchange));
            }
            Step::Bind { vhost, binding } => {
                let ends = [
                    (binding.source.as_str(), DestinationType::Exchange),
                    (
                        binding.destination.as_str(),
                        DestinationType::parse(&binding.destination_type)
                            .unwrap_or(DestinationType::Queue),
                    ),
                ];
                for (name, kind) in ends {
                    if declared
                        .iter()
                        .any(|(v, n, k)| *v == vhost && *n == name && *k == kind)
                    {
                        continue;
                    }
                    let exists = match kind {
                        DestinationType::Queue => {
                            client.queue(vhost, name).await.map(|q| q.is_some())
                        }
                        DestinationType::Exchange => {
                            client.exchange(vhost, name).await.map(|e| e.is_some())
                        }
                    };
                    match exists {
                        Ok(true) => {}
                        Ok(false) => problems.push(format!(
                            "{} {name} no longer exists, so {} cannot be restored",
                            kind.as_str(),
                            binding.label()
                        )),
                        Err(e) => {
                            problems.push(format!("cannot read {} {name}: {e}", kind.as_str()))
                        }
                    }
                }
            }
            _ => {}
        }
    }
    problems
}

fn labels(bindings: &[BindingDeclaration]) -> String {
    bindings
        .iter()
        .map(BindingDeclaration::label)
        .collect::<Vec<_>>()
        .join("; ")
}

/// Runs one RabbitMQ step. Deleting a queue sends `if-empty` and
/// `if-unused`, so a queue that got messages or consumers after the
/// blockers were checked is refused by the broker, not emptied.
pub async fn run(step: &Step, client: &ManagementClient) -> KafkaResult<()> {
    let result = match step {
        Step::DeclareQueue {
            vhost,
            name,
            declaration,
        } => {
            client
                .declare_queue(vhost, name, &queue_request(declaration))
                .await
        }
        Step::DeleteQueue { vhost, name } => client.delete_queue(vhost, name, true, true).await,
        Step::DeclareExchange {
            vhost,
            name,
            declaration,
        } => {
            client
                .declare_exchange(vhost, name, &exchange_request(declaration))
                .await
        }
        Step::DeleteExchange { vhost, name } => client.delete_exchange(vhost, name, true).await,
        Step::Bind { vhost, binding } => match binding_request(binding) {
            Ok(binding) => client.bind(vhost, &binding).await,
            Err(e) => Err(e),
        },
        Step::Unbind { vhost, binding } => match binding_request(binding) {
            Ok(binding) => client.unbind(vhost, &binding).await.map(|_| ()),
            Err(e) => Err(e),
        },
        other => {
            return Err(KafkaError::config(format!(
                "'{}' is not a RabbitMQ step",
                other.describe()
            )))
        }
    };
    result.map_err(rabbit_error)
}

/// [`undo::prepare_request`] for an operation on RabbitMQ: the same checks
/// (the "undone by" mark, the result, the audit digest, the scope, the
/// company policy), then the plan from the broker's current state.
pub async fn prepare_request(
    op: &Operation,
    all: &[Operation],
    current: &Scope,
    audited: &AuditDigests,
    policy_active: bool,
    options: UndoOptions,
    client: &ManagementClient,
) -> KafkaResult<UndoRequest> {
    let mut request = undo::check_request(op, all, current, audited, policy_active);
    if request.refusal.is_some() {
        return Ok(request);
    }
    let plan = prepare(
        op,
        all,
        current,
        audited.get(&op.id).map(String::as_str),
        client,
    )
    .await?;
    undo::decide(&mut request, plan, policy_active, options);
    Ok(request)
}

/// [`undo::prepare`] for an operation on RabbitMQ.
pub async fn prepare(
    op: &Operation,
    all: &[Operation],
    current_scope: &Scope,
    audited: Option<&str>,
    client: &ManagementClient,
) -> KafkaResult<UndoPlan> {
    let scope_check = op.begin.scope.check_undo_target(current_scope);
    let same_scope = scope_check.is_ok();
    let digest = undo::digest_check(op, audited);
    let later: Vec<String> = conflict::later_overlaps(op, all)
        .iter()
        .map(|o| o.id.clone())
        .collect();
    let (current, verdict, steps) = if let Err(reason) = scope_check {
        (
            Vec::new(),
            conflict::Verdict::Drift { rows: Vec::new() },
            Err(NotReversible(reason)),
        )
    } else if !op.begin.reversibility.undoable() || op.begin.before.is_empty() {
        (
            Vec::new(),
            conflict::Verdict::Drift { rows: Vec::new() },
            inverse::plan(op, &[]),
        )
    } else {
        let current = recapture_all(&op.begin.before, client).await?;
        let verdict = conflict::check(op, &current);
        let steps = inverse::plan(op, &current);
        (current, verdict, steps)
    };
    let blockers = match &steps {
        Ok(steps) => blockers(steps, client).await,
        Err(_) => Vec::new(),
    };
    let (redo, redo_limits) = undo::redo_reversibility(&current, &op.begin.before);
    Ok(UndoPlan {
        op: op.id.clone(),
        current,
        verdict,
        steps,
        blockers,
        later,
        digest,
        same_scope,
        redo,
        redo_limits,
    })
}

/// [`undo::execute_steps`] for a RabbitMQ plan: runs the steps in order,
/// stopping at the first failure (applied steps stay in place).
pub async fn execute_steps(
    plan: &UndoPlan,
    client: &ManagementClient,
) -> Result<String, Box<UndoFailure>> {
    let steps = plan.runnable_steps();
    if steps.is_empty() {
        return Ok("already in its previous state; nothing changed".to_string());
    }
    for (ix, step) in steps.iter().enumerate() {
        if let Err(error) = run(step, client).await {
            return Err(Box::new(UndoFailure::new(steps, ix, error)));
        }
    }
    Ok(format!(
        "Undone: {}",
        steps
            .iter()
            .map(Step::describe)
            .collect::<Vec<_>>()
            .join("; ")
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrika_core::model::history::{fold, Begin, End, EventBody, HistoryEvent, FORMAT_VERSION};
    use ferrika_rabbitmq::fake::FakeManagement;

    const ADDRESS: &str = "rabbit.test:15672";

    /// `change` as a finished operation `id` on the fake broker, its
    /// after-state read back as the CLI does.
    async fn recorded(id: &str, change: Change, client: &ManagementClient) -> Operation {
        let after = recapture_all(&change.before, client).await.unwrap();
        let begin: Begin = change.into_begin(
            "cli",
            scope("dev", ADDRESS),
            "rabbit test".into(),
            "t".into(),
            None,
        );
        let events = vec![
            HistoryEvent {
                v: FORMAT_VERSION,
                id: id.into(),
                time_ms: 1,
                body: EventBody::Begin(Box::new(begin)),
            },
            HistoryEvent {
                v: FORMAT_VERSION,
                id: id.into(),
                time_ms: 2,
                body: EventBody::End(End {
                    ok: true,
                    outcome: "done".into(),
                    after: Some(after),
                }),
            },
        ];
        fold(events).remove(0)
    }

    async fn plan_for(op: &Operation, client: &ManagementClient) -> UndoRequest {
        prepare_request(
            op,
            std::slice::from_ref(op),
            &scope("dev", ADDRESS),
            &AuditDigests::new(),
            false,
            UndoOptions::default(),
            client,
        )
        .await
        .unwrap()
    }

    fn shop_to_orders() -> BindingDeclaration {
        BindingDeclaration {
            source: "shop".into(),
            destination: "orders".into(),
            destination_type: DestinationType::Queue,
            routing_key: "order.*".into(),
            arguments: Map::new(),
        }
    }

    #[tokio::test]
    async fn undoing_a_queue_declaration_deletes_it_only_while_empty() {
        let fake = FakeManagement::start().await;
        let client = fake.client();
        let declaration = QueueDeclaration {
            kind: Some("quorum".into()),
            durable: true,
            ..QueueDeclaration::default()
        };
        let change = queue_declared(&client, "/", "jobs", &declaration)
            .await
            .unwrap();
        assert_eq!(change.reversibility, Reversibility::Conditional);
        client
            .declare_queue("/", "jobs", &declaration)
            .await
            .unwrap();
        let op = recorded("A", change, &client).await;
        assert!(op.after()[0].same_as(&State::RabbitQueue {
            vhost: "/".into(),
            name: "jobs".into(),
            declaration: Some(RabbitQueueDeclaration {
                kind: "quorum".into(),
                durable: true,
                auto_delete: false,
                arguments: BTreeMap::new(),
            }),
        }));
        // A second declaration of an existing queue is refused up front.
        assert!(queue_declared(&client, "/", "jobs", &declaration)
            .await
            .is_err());

        fake.with(|b| b.messages.insert(("/".into(), "jobs".into()), 2));
        let request = plan_for(&op, &client).await;
        let refusal = request.refusal.unwrap().reason();
        assert!(refusal.contains("2 message(s)"), "{refusal}");
        // --force does not pass it.
        assert!(request.forced_refusal.is_some());

        fake.with(|b| b.messages.insert(("/".into(), "jobs".into()), 0));
        let request = plan_for(&op, &client).await;
        assert_eq!(request.refusal, None);
        let plan = request.plan.unwrap();
        assert_eq!(
            plan.runnable_steps(),
            &[Step::DeleteQueue {
                vhost: "/".into(),
                name: "jobs".into()
            }]
        );
        assert_eq!(
            plan.runnable_steps()[0].policy_action(),
            ferrika_core::model::policy::PolicyAction::QueuesDelete
        );
        execute_steps(&plan, &client).await.unwrap();
        assert_eq!(fake.with(|b| b.messages_of("/", "jobs")), None);
        assert!(fake
            .with(|b| b.writes())
            .contains(&"DELETE /api/queues/%2F/jobs?if-empty=true&if-unused=true".to_string()));
    }

    #[tokio::test]
    async fn undoing_a_queue_deletion_declares_it_and_its_bindings_again() {
        let fake = FakeManagement::start().await;
        fake.with(|b| {
            b.add_queue("/", "orders", 7);
            b.add_exchange("/", "shop", "topic");
            b.add_binding("/", "shop", "queue", "orders", "order.*");
        });
        let client = fake.client();
        let change = queue_deleted(&client, "/", "orders").await.unwrap();
        assert_eq!(change.reversibility, Reversibility::Partial);
        assert!(change.limits[0].contains('7'), "{:?}", change.limits);
        assert_eq!(change.before.len(), 2, "the queue and its binding");
        client
            .delete_queue("/", "orders", false, false)
            .await
            .unwrap();
        let op = recorded("D", change, &client).await;
        assert!(!fake.with(|b| b.has_binding("shop", "orders", "order.*")));

        let request = plan_for(&op, &client).await;
        assert_eq!(request.refusal, None, "{:?}", request.plan);
        let plan = request.plan.unwrap();
        let steps = plan.runnable_steps();
        assert!(matches!(steps[0], Step::DeclareQueue { .. }), "{steps:?}");
        assert!(matches!(steps[1], Step::Bind { .. }), "{steps:?}");
        let text = execute_steps(&plan, &client).await.unwrap();
        assert!(text.contains("declare classic queue orders"), "{text}");
        assert_eq!(fake.with(|b| b.messages_of("/", "orders")), Some(0));
        assert!(fake.with(|b| b.has_binding("shop", "orders", "order.*")));
        // Undoing it again finds nothing to do.
        let plan = plan_for(&op, &client).await.plan.unwrap();
        assert!(plan.runnable_steps().is_empty());
    }

    #[tokio::test]
    async fn a_binding_whose_other_end_is_gone_blocks_the_undo() {
        let fake = FakeManagement::start().await;
        fake.with(|b| {
            b.add_queue("/", "orders", 0);
            b.add_exchange("/", "shop", "topic");
            b.add_binding("/", "shop", "queue", "orders", "order.*");
        });
        let client = fake.client();
        let change = queue_deleted(&client, "/", "orders").await.unwrap();
        client
            .delete_queue("/", "orders", false, false)
            .await
            .unwrap();
        let op = recorded("D", change, &client).await;
        client.delete_exchange("/", "shop", false).await.unwrap();
        let refusal = plan_for(&op, &client).await.refusal.unwrap().reason();
        assert!(
            refusal.contains("exchange shop no longer exists"),
            "{refusal}"
        );
    }

    #[tokio::test]
    async fn undoing_an_exchange_deletion_restores_its_bindings_both_ways() {
        let fake = FakeManagement::start().await;
        fake.with(|b| {
            b.add_queue("/", "orders", 0);
            b.add_exchange("/", "shop", "topic");
            b.add_exchange("/", "upstream", "fanout");
            b.add_binding("/", "shop", "queue", "orders", "order.*");
            b.add_binding("/", "upstream", "exchange", "shop", "");
        });
        let client = fake.client();
        let change = exchange_deleted(&client, "/", "shop").await.unwrap();
        assert_eq!(change.before.len(), 3);
        assert_eq!(change.reversibility, Reversibility::Reversible);
        client.delete_exchange("/", "shop", false).await.unwrap();
        assert!(fake.with(|b| b.bindings.is_empty()));
        let op = recorded("E", change, &client).await;
        let plan = plan_for(&op, &client).await.plan.unwrap();
        execute_steps(&plan, &client).await.unwrap();
        assert!(fake.with(|b| b.has_binding("shop", "orders", "order.*")));
        assert!(fake.with(|b| b.has_binding("upstream", "shop", "")));
        assert_eq!(
            client.exchange("/", "shop").await.unwrap().unwrap().kind,
            "topic"
        );
    }

    #[tokio::test]
    async fn undoing_an_exchange_declaration_waits_for_its_bindings_to_go() {
        let fake = FakeManagement::start().await;
        fake.with(|b| b.add_queue("/", "orders", 0));
        let client = fake.client();
        let declaration = ExchangeDeclaration {
            kind: "direct".into(),
            durable: true,
            auto_delete: false,
            internal: false,
            arguments: Map::new(),
        };
        let change = exchange_declared(&client, "/", "shop", &declaration)
            .await
            .unwrap();
        client
            .declare_exchange("/", "shop", &declaration)
            .await
            .unwrap();
        let op = recorded("X", change, &client).await;
        client.bind("/", &shop_to_orders()).await.unwrap();
        let refusal = plan_for(&op, &client).await.refusal.unwrap().reason();
        assert!(refusal.contains("binding(s) added since"), "{refusal}");
        client.unbind("/", &shop_to_orders()).await.unwrap();
        let plan = plan_for(&op, &client).await.plan.unwrap();
        assert_eq!(
            plan.runnable_steps()[0].policy_action(),
            ferrika_core::model::policy::PolicyAction::ExchangesDelete
        );
        execute_steps(&plan, &client).await.unwrap();
        assert_eq!(client.exchange("/", "shop").await.unwrap(), None);
    }

    #[tokio::test]
    async fn bind_and_unbind_undo_each_other() {
        let fake = FakeManagement::start().await;
        fake.with(|b| {
            b.add_queue("/", "orders", 0);
            b.add_exchange("/", "shop", "topic");
        });
        let client = fake.client();
        let binding = shop_to_orders();
        let change = binding_changed(&client, "/", &binding, true).await.unwrap();
        client.bind("/", &binding).await.unwrap();
        let bound = recorded("B", change, &client).await;
        let plan = plan_for(&bound, &client).await.plan.unwrap();
        assert!(matches!(plan.runnable_steps(), [Step::Unbind { .. }]));
        assert_eq!(
            plan.runnable_steps()[0].policy_action(),
            ferrika_core::model::policy::PolicyAction::BindingsChange
        );
        execute_steps(&plan, &client).await.unwrap();
        assert!(!fake.with(|b| b.has_binding("shop", "orders", "order.*")));

        client.bind("/", &binding).await.unwrap();
        let change = binding_changed(&client, "/", &binding, false)
            .await
            .unwrap();
        client.unbind("/", &binding).await.unwrap();
        let unbound = recorded("U", change, &client).await;
        let plan = plan_for(&unbound, &client).await.plan.unwrap();
        assert!(matches!(plan.runnable_steps(), [Step::Bind { .. }]));
        execute_steps(&plan, &client).await.unwrap();
        assert!(fake.with(|b| b.has_binding("shop", "orders", "order.*")));
    }

    #[tokio::test]
    async fn a_failed_step_is_reported_with_what_ran_before_it() {
        let fake = FakeManagement::start().await;
        fake.with(|b| {
            b.add_queue("/", "orders", 0);
            b.add_exchange("/", "shop", "topic");
            b.add_binding("/", "shop", "queue", "orders", "order.*");
        });
        let client = fake.client();
        let change = queue_deleted(&client, "/", "orders").await.unwrap();
        client
            .delete_queue("/", "orders", false, false)
            .await
            .unwrap();
        let op = recorded("D", change, &client).await;
        let plan = plan_for(&op, &client).await.plan.unwrap();
        fake.with(|b| b.failures.push(("POST /api/bindings".into(), 500)));
        let failure = execute_steps(&plan, &client).await.unwrap_err();
        assert!(failure.changed_anything());
        assert!(
            failure.failed.starts_with("bind exchange shop"),
            "{failure}"
        );
    }

    #[tokio::test]
    async fn a_purge_is_recorded_but_cannot_be_undone() {
        let fake = FakeManagement::start().await;
        fake.with(|b| b.add_queue("/", "orders", 4));
        let client = fake.client();
        let change = queue_purged(&client, "/", "orders").await.unwrap();
        assert_eq!(change.reversibility, Reversibility::Irreversible);
        assert!(change.before.iter().all(State::is_rabbitmq));
        client.purge_queue("/", "orders").await.unwrap();
        let op = recorded("P", change, &client).await;
        let rows = op.state_rows();
        assert_eq!(rows[0].before, "4");
        assert_eq!(rows[0].after, "0");
        assert!(!undo::undoable(&op));
        let plan = plan_for(&op, &client).await;
        assert!(plan.refusal.is_some());
        assert!(queue_purged(&client, "/", "missing").await.is_err());
    }

    #[tokio::test]
    async fn undo_refuses_another_broker_and_a_kafka_client() {
        let fake = FakeManagement::start().await;
        fake.with(|b| b.add_queue("/", "orders", 0));
        let client = fake.client();
        let change = queue_deleted(&client, "/", "orders").await.unwrap();
        client
            .delete_queue("/", "orders", false, false)
            .await
            .unwrap();
        let op = recorded("D", change, &client).await;
        let elsewhere = prepare_request(
            &op,
            std::slice::from_ref(&op),
            &scope("dev", "other:15672"),
            &AuditDigests::new(),
            false,
            UndoOptions {
                force: true,
                ..UndoOptions::default()
            },
            &client,
        )
        .await
        .unwrap();
        let reason = elsewhere.refusal.unwrap().reason();
        assert!(
            reason.contains("RabbitMQ in use is other:15672"),
            "{reason}"
        );
        // The Kafka undo never reads a RabbitMQ state with a Kafka client.
        let kafka_scope = Scope {
            bootstrap: "localhost:9092".into(),
            ..Scope::default()
        };
        assert!(op.begin.scope.check_undo_target(&kafka_scope).is_err());
        assert!(!op.begin.scope.same_cluster(&kafka_scope));
    }

    #[test]
    fn declarations_keep_the_queue_type_once() {
        let queue: Queue = serde_json::from_value(serde_json::json!({
            "name": "q", "vhost": "/", "type": "quorum", "durable": true,
            "arguments": {"x-queue-type": "quorum", "x-delivery-limit": 5}
        }))
        .unwrap();
        let declaration = queue_declaration(&queue);
        assert_eq!(declaration.kind, "quorum");
        assert_eq!(declaration.arguments.len(), 1);
        let request = queue_request(&declaration).to_request();
        assert_eq!(request["arguments"]["x-queue-type"], "quorum");
        assert_eq!(request["arguments"]["x-delivery-limit"], 5);
    }
}
