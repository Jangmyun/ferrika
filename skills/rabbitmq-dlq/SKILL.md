---
name: rabbitmq-dlq
description: Inspect a RabbitMQ dead-letter queue and move its messages back with the ferrika CLI (preview) - group them by x-death origin and reason, preview where each would go, then redrive in small batches over AMQP with publisher confirms. Use when someone asks what is in a RabbitMQ DLQ or dead-letter queue, why messages were dead-lettered or rejected, or to requeue, retry or redrive them.
---

# RabbitMQ dead-letter inspect and redrive

`ferrika rabbit` (preview) works through the RabbitMQ management API and
moves messages over AMQP 0-9-1. Dead-lettered messages carry an `x-death`
header with their original queue, the reason (`rejected`, `expired`,
`maxlen`, `delivery_limit`), the exchange and the routing key; Ferrika
groups by it and sends messages back to where they came from.

## Before you start

- `ferrika --version` works, and a RabbitMQ profile exists:
  `ferrika rabbit profile list`. If none, ask the user to add one
  (`ferrika rabbit profile add NAME --rabbit-url URL --rabbit-user USER
  --rabbit-password-stdin`); never put a password on the command line.
- Use `--rabbit-profile NAME` when the user names a broker. RabbitMQ
  profiles are separate from Kafka profiles.
- `DLQ`, `NAME`, `RETRY_EXCHANGE` and `ROUTING_KEY` below are placeholders.

Reading messages is not free in RabbitMQ: the management API has no
browse, so `peek`, `dlq inspect` and the redrive preview take messages and
requeue them, and the broker marks them redelivered. Under the company
policy these count as writes (`messages.peek`).

## Steps

### 1. Find the queue

```bash
ferrika -f json rabbit queues --name dlq
ferrika -f json rabbit bindings --name DLQ
```

Queues with message counts and dead-letter settings; the bindings show how
messages reach the dead-letter queue. Ask the user which queue they mean if
several match.

### 2. Why messages died (reads and requeues)

```bash
ferrika -f json rabbit dlq inspect DLQ -n 100 --mask-pii
```

Groups the messages by original queue, reason, exchange and routing key,
with a masked sample. Summarize by group for the user:

- `rejected`: the consumer nacked or rejected them (application error).
- `expired`: a message TTL ran out before a consumer took them.
- `maxlen`: the source queue was full.
- `delivery_limit`: a quorum queue gave up after too many redeliveries.

A quorum queue with a delivery limit on RabbitMQ before 4.0 is refused,
because each read counts as a delivery attempt and can dead-letter or drop
messages. `--allow-delivery-count` overrides that; use it only when the user
explicitly accepts the risk.

### 3. Preview the redrive

```bash
ferrika rabbit dlq redrive DLQ -n 50
```

Nothing is moved: it shows where each message would go (the exchange and
routing key of its newest `x-death` entry). Confirm with the user that the
cause is fixed; otherwise the messages are dead-lettered again.

### 4. Approval, then a small batch

```bash
ferrika rabbit dlq redrive DLQ -n 10 --execute
```

It asks for confirmation; let the user answer, or add `--yes` only when
they approved this exact command. Each message is taken without auto-ack,
published with `mandatory` and publisher confirms, and acked only after the
confirm; the run stops at the first failure and returns that message to the
queue. Redriven messages keep their headers and get
`x-ferrika-redriven-from`. Check with the user that the consumers handled
them, then continue with a larger `-n`.

To a retry exchange instead of the origin:

```bash
ferrika rabbit dlq redrive DLQ --to-exchange RETRY_EXCHANGE -k ROUTING_KEY -n 10 --execute
```

### 5. Verify

```bash
ferrika -f json rabbit queues --name DLQ
ferrika -f json audit -n 10
```

The queue's message count should drop by the number moved. The audit log
records the counts and destinations, never bodies.

## Safety rules

- Preview before `--execute`; never `--execute` or `--yes` without the
  user's approval. A redrive cannot be undone (`rabbit undo` covers queue,
  exchange and binding changes, not messages).
- Never purge or delete the dead-letter queue to "clean up"; that loses the
  messages for good.
- Keep `--mask-pii` on and do not paste message bodies.
- An error that starts with `Refused` comes from the company policy or a
  read-only RabbitMQ profile. Stop and tell the user.
- Never print or ask for the RabbitMQ password; credentials in
  `--rabbit-url` end up in shell history.

## With the Ferrika MCP server

`rabbit_queues` and `rabbit_bindings` list without reading messages. With
`--allow-write`, `rabbit_dlq_inspect` and `rabbit_dlq_redrive` follow the
same plan, confirm and audit path (redrive at most 1,000 messages per call).
