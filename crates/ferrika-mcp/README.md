# ferrika-mcp

A [Model Context Protocol](https://modelcontextprotocol.io) server on
stdin/stdout that lets AI assistants (Claude Code, Claude Desktop, IDE agents)
inspect a Kafka cluster. It is what `ferrika mcp` runs; see
[Ferrika](https://github.com/Jangmyun/ferrika).

- Read-only by default: no tool creates, changes or deletes cluster state, and
  peeked or previewed records never join or commit to a consumer group.
- Record text is PII-masked unless the caller turns masking off.
- `transform_preview` shows what a `ferrika transform` pipeline would write;
  `transform_execute` (a write tool) runs it into another topic of the same
  cluster.
- RabbitMQ (preview): `rabbit_queues`, `rabbit_exchanges` and
  `rabbit_bindings` list a RabbitMQ profile's virtual host, without message
  bodies. With writes enabled, `rabbit_peek` and `rabbit_dlq_inspect` read
  messages (masked; reading requeues them, so they are write tools),
  `rabbit_publish` publishes one message and `rabbit_dlq_redrive` moves
  dead-lettered messages back over AMQP (feature `amqp`).
- Write tools are opt-in. Each write is planned, confirmed, then run, and
  passes the same company policy, read-only profile and audit gate as the
  command line.

The entry point is `McpServer`: build it from a resolved cluster context,
optionally with `WriteOptions`, and call `serve_stdio()`.

## Features

- `amqp` (off by default): the `rabbit_dlq_redrive` write tool, which needs
  `ferrika-rabbitmq`'s AMQP client (lapin). The `ferrika` CLI enables it.

## Stability

Pre-1.0: public items can change between minor versions.

## License

Apache-2.0
