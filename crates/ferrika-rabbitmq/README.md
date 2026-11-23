# ferrika-rabbitmq

**Preview.** RabbitMQ support of [Ferrika](https://github.com/Jangmyun/ferrika),
behind the `ferrika rabbit` command. Its API, file formats and behaviour may
change in any release until it leaves preview.

- `client`: a typed client for the RabbitMQ management HTTP API (plugin
  `rabbitmq_management`): queues, exchanges and bindings of a vhost (list,
  read, declare, delete, purge, bind, unbind), peeking at messages (taken
  and requeued: they are marked redelivered) and publishing to an exchange
- `model`: the API's responses and request bodies, and the `x-death`
  history of dead-lettered messages
- `profile`: named connections in `rabbit-profiles.json` next to Ferrika's
  `profiles.json`, passwords in the OS credential store
- `amqp` (feature `amqp`, on by default): moving dead-lettered messages back
  to their original exchange over AMQP 0-9-1 with
  [lapin](https://crates.io/crates/lapin): one message at a time, published
  with publisher confirms and acknowledged only after the confirm
- `connection`: a profile with the user's overrides applied, and from it
  the login, the management client and the AMQP target
- `view`: messages as a front end shows them (PII-masked on request), the
  dead-letter inspection grouped by `x-death` origin, the redrive preview
  and the redrive's audit text

Ferrika's company policy and audit log are applied by the caller (the
`ferrika` command line or another front end) through `ferrika-governance`.

Feature `test-util` (tests only, never in a shipped binary) adds `fake`,
an in-memory management API over HTTP for testing code that uses the
client.

## Stability

Preview, pre-1.0: public items can change in any version.

## License

Apache-2.0
