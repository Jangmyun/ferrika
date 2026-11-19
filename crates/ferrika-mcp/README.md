# ferrika-mcp

A [Model Context Protocol](https://modelcontextprotocol.io) server on
stdin/stdout that lets AI assistants (Claude Code, Claude Desktop, IDE agents)
inspect a Kafka cluster. It is what `ferrika mcp` runs; see
[Ferrika](https://github.com/Jangmyun/ferrika).

- Read-only by default: no tool creates, changes or deletes cluster state, and
  peeked records never join or commit to a consumer group.
- Record text is PII-masked unless the caller turns masking off.
- Write tools are opt-in. Each write is planned, confirmed, then run, and
  passes the same company policy, read-only profile and audit gate as the
  command line.

The entry point is `McpServer`: build it from a resolved cluster context,
optionally with `WriteOptions`, and call `serve_stdio()`.

## Stability

Pre-1.0: public items can change between minor versions.

## License

Apache-2.0
