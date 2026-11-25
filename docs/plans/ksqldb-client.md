# ksqlDB client

Status: preview implementation (`ferrika ksql`, crate `ferrika-ksql`, MCP
read tools). Product decision of 2026-11-25: Kafka tools commonly offer a
client for the user's existing ksqlDB server; Ferrika does too. Ferrika does
**not** embed, run or redistribute ksqlDB. It talks to a server the user
already runs, over the server's public REST API. Kafka behaviour is
unchanged.

The client is written from the public ksqlDB REST API documentation and
from what a ksqlDB 0.29 server answers; no code was taken from Confluent's
repositories or from other Kafka consoles.

## Goal

Browse a ksqlDB server (streams, tables, persistent queries, topics,
`DESCRIBE`), run pull and push queries, and run statements, with the same
governance as every other Ferrika write and export:

- a statement that changes the server passes the company policy and the
  read-only profile check **before anything is sent**, per statement, and is
  recorded in the hash-chained audit log before and after it runs;
- query rows are record data: masked under the masking rules and exported
  under the export rules, exactly like `consume`.

## Crate layout

```
ferrika-cli ──> ferrika-mcp ──> ferrika-ops ──> ferrika-governance ──> ferrika-core
     │               │                                                     ^
     └───────────────┴──> ferrika-ksql ─────────────────────────────────────┘
```

`crates/ferrika-ksql` depends on `ferrika-core` only (URL parsing, the REST
TLS setup, Base64, `ClusterProfile`, PII masking, `PolicyAction`). It has no
Kafka client. Like `ferrika-rabbitmq` it applies no policy and writes no
audit entries: it classifies, the caller gates.

| Module | Contents |
| --- | --- |
| `config` | `KsqlConfig`: URL, basic-auth login, CA file, client certificate and key, time limit; `from_profile(&ClusterProfile)`; `address()` (`host:port` for the policy and audit) |
| `client` | `KsqlClient`: `info`, `streams`, `tables`, `queries`, `topics`, `describe`, `ksql` (raw `/ksql`), `execute(&Statement, &Session)`, `query` (a `QueryStream`), `run_query` (collect with row/time limits and cancel), `close_query`; `Session` (properties and variables), `QueryLimits`, `quote_identifier` |
| `statement` | the lexer, `split`, `Statement::parse`, `StatementKind`, `Access`, `policy_actions`, `summary` |
| `view` | `mask_row`, `QueryResult::masked`, `row_objects`, `cell_text` |
| `model` | `ServerInfo`, `SourceInfo`, `QueryInfo`, `TopicInfo`, `SourceDescription` (with `FieldSchema::render`), `Column`, `QueryHeader`, `QueryResult`, `QueryEnd`, `CommandResult` |
| `http` (private) | an HTTP/1.1 transport whose body can be read line by line while it streams |

No new third-party crate: `tokio`, `rustls`, `tokio-rustls`, `serde`,
`serde_json`, `thiserror`, `tracing` are already in the workspace.

## REST API endpoints

| Use | Request | Answer |
| --- | --- | --- |
| server info | `GET /info` | `{"KsqlServerInfo": {"version", "kafkaClusterId", "ksqlServiceId", "serverStatus"}}` |
| listings, `DESCRIBE`, statements | `POST /ksql` with `{"ksql": "<one statement>", "streamsProperties": {...}, "sessionVariables": {...}}`, `Accept: application/vnd.ksql.v1+json` | an array of entities (`streams`, `tables`, `queries`, `kafka_topics`, `sourceDescription`, `currentStatus` ...); an `INSERT ... VALUES` returns an empty array; errors are HTTP 400 `{"@type": "statement_error", "error_code", "message"}` |
| queries | `POST /query-stream` with `{"sql": "<one SELECT>", "properties": {...}}`, `Accept: application/vnd.ksqlapi.delimited.v1` | chunked: a header line `{"queryId", "columnNames", "columnTypes"}`, then one JSON array per row; errors as above |
| closing a query | `POST /close-query` with `{"queryId"}` | see below |

### Why HTTP/1.1 and `/query-stream`

- The workspace has an HTTP/1.1 client and no HTTP/2 stack. Adding `h2`/
  `hyper` for one endpoint would add a large dependency tree.
- ksqlDB serves `/query-stream` on HTTP/1.1 too: the response is chunked and
  each row arrives as its own line, so push queries stream fine.
- `/query-stream` gives column names and types as arrays; the older `/query`
  endpoint (HTTP/1.1, still present in 0.29) gives the schema as one string
  that would need parsing, and its rows wrapped in `{"row": {"columns": ...}}`.
- ferrika-core's `Endpoint` reads a response until the server closes it,
  which cannot hand out rows of a push query that never ends. `ferrika-ksql`
  therefore has its own small transport (same limits: timeouts, size caps,
  one request per connection, `Connection: close`) whose body is read
  incrementally (chunked decoding, a 16 MiB cap per row line, 64 MiB for a
  whole body).

### Closing queries

A push query runs until its own `LIMIT`, until the client closes it, or
until the connection drops. When the client stops early (row limit, time
limit, Ctrl+C, an error) it calls `/close-query` and then drops the
connection. Observed on ksqlDB 0.29: a `/close-query` sent on another
HTTP/1.1 connection answers HTTP 500 `"On wrong context or worker"` **but
does close the query** (a second close answers 400 `"No query with id"`, and
the stream ends). The client treats both answers as closed. Dropping the
connection alone also ends the query on the server, so a failed close only
logs. The e2e test checks that the query id is gone after a row-limited
push query.

## Statement classification

`statement.rs` has a small lexer, not a ksqlDB parser:

- drops `--` line comments and `/* */` block comments;
- keeps `'string'` literals (`''` escapes) and `` `quoted` `` / `"quoted"`
  identifiers intact, so a `;`, a keyword or `--` inside them does not count;
- splits on `;` outside of them; empty statements are dropped; the last
  statement may omit its `;`; an unterminated literal, identifier or comment
  is an error (nothing is sent);
- classifies each statement by its leading keywords (case-insensitive).

| Statement | Kind | Access | Policy categories |
| --- | --- | --- | --- |
| `SELECT ...` (`EMIT CHANGES` / `EMIT FINAL` = push) | `Select` | read | |
| `PRINT` | `Print` | read | (not run by the CLI: use `consume`) |
| `SHOW ...`, `LIST ...`, `DESCRIBE ...`, `EXPLAIN ...`, `ASSERT ...` | | read | |
| `SET 'p'='v'`, `UNSET 'p'`, `DEFINE v = 'x'`, `UNDEFINE v` | session | session | |
| `CREATE [OR REPLACE] [SOURCE] STREAM/TABLE` | `Create` | write | `ksql.create`; `topics.create` too with `AS SELECT` or a `PARTITIONS` property (the server creates the topic) |
| `CREATE TYPE` | `Create` | write | `ksql.create` |
| `CREATE SOURCE/SINK CONNECTOR`, `DROP CONNECTOR` | | write | `connect.change` |
| `DROP STREAM/TABLE/TYPE` | `Drop` | write | `ksql.drop`; `topics.delete` too with `DELETE TOPIC` |
| `INSERT INTO ... VALUES` / `INSERT INTO ... SELECT` | `Insert` | write | `ksql.insert` and `produce` (`also_requires`) |
| `TERMINATE`, `PAUSE`, `RESUME` | | write | `ksql.terminate` |
| `ALTER STREAM/TABLE` | `Alter` | write | `ksql.alter` |
| anything else (`RUN SCRIPT`, `ALTER SYSTEM`, a malformed `SET`, a future statement) | `Unknown` | write | every category above and `config.change` |

A statement that references `${variable}` (other than the session
statements) is `Unknown`: its text is only known after the server
substitutes it, so it gets the strictest gate.

`Statement::summary()` describes a statement with keywords and identifiers
only (`INSERT INTO ORDERS VALUES`, `DROP TABLE T DELETE TOPIC`, `SET
'auto.offset.reset'`): it is the audit target and never carries literals,
which can be personal data (`INSERT` values) or secrets (`SET` values).

## Policy

### New categories

| Category | Covers |
| --- | --- |
| `ksql.create` | `CREATE STREAM/TABLE/TYPE` (starts persistent queries for `AS SELECT`) |
| `ksql.drop` | `DROP STREAM/TABLE/TYPE` |
| `ksql.insert` | `INSERT INTO`; `also_requires` `produce` |
| `ksql.terminate` | `TERMINATE`, `PAUSE`, `RESUME` |
| `ksql.alter` | `ALTER STREAM/TABLE` |

The names follow `resource.verb` (`topics.delete`, `messages.peek`). As in
the multi-broker plan, a ksqlDB statement whose effect matches an existing
category also needs that one: `INSERT` writes records (`produce`), `DELETE
TOPIC` deletes a topic (`topics.delete`), a CSAS/CTAS creates its sink topic
(`topics.create`), connectors are Kafka Connect changes (`connect.change`).
So `forbid: ["topics.delete"]` also stops `DROP ... DELETE TOPIC`, and
`forbid: ["produce"]` stops `INSERT`.

`SET`/`UNSET`/`DEFINE` are applied by the client to the statements after
them (the REST API is stateless; they are sent as `streamsProperties` and
`sessionVariables`). They change nothing on the server and need no category.

Compatibility: a signed policy naming an unknown `forbid` category is
invalid for older builds, which then run in the strictest mode. Roll out a
policy with `ksql.*` only once every client knows them (CHANGELOG and
`docs/POLICY.md` say so). `PolicyAction` gains five variants (`ALL` has 21).

### The gate for `ksql exec`

`main`'s write gate checks one category per command. A script can mix
categories, so `write_action` returns `None` for every `ksql` subcommand
(exhaustively matched, no wildcard) and `ksql exec` gates itself, like
`undo` and `lint --fix`:

1. Split and classify the whole script. A `SELECT`/`PRINT` in it refuses
   the script (queries go through `ksql query`).
2. For every write statement and every category it needs:
   `EffectivePolicy::check(category, profile, addresses)`. The first
   refusal is recorded as `policy.blocked` (target `ksql exec <summary>`)
   and refuses the script. **Nothing has been sent.**
3. A read-only profile refuses any write (recorded as a refused request).
4. Confirmation (the statements with their categories) unless `--yes`.
5. One audit request entry per write statement (`action` `ksql exec`,
   `target` the summary, outcome `requested`), via `authorize_write`
   (so `audit: required` refuses when the log cannot be written). Still
   nothing sent.
6. The statements run one per `/ksql` request, in order, with the session's
   properties and variables. The first failure stops the script.
7. One result entry per write: `ok: SUCCESS[, query <id>]`, `failed:
   ksqlDB error <code> (HTTP <status>)` (never the server's message, which
   can quote the statement), or `not run (an earlier statement failed)`.

`--dry-run` prints each statement, its access, its categories and the
policy's verdict, records nothing and sends nothing.

### Addresses and profile

The gates match:

- the Kafka profile's name (`read_only` globs, environments), when the
  server comes from the profile;
- the ksqlDB server's `host:port`, plus the profile's bootstrap servers
  (and, when the policy needs them, the brokers the cluster reports): the
  server writes into that cluster, so `read_only_brokers: ["*.prod.corp"]`
  covers `ksql.prod.corp` and a server whose Kafka is in prod.

With `--ksql-url` no profile is used: neither its login, its TLS files nor
its name or read-only mark; the gates match the server's address only (as
`--rabbit-url` without `--rabbit-profile`).

### Export and masking

`ksql query` is classified in `export_gate` as `(Stdout, "ksql query",
<summary>)`, exactly like `consume`: on a terminal the rows are viewed; with
stdout redirected or piped they are an export, refused under `export:
forbidden` (with the same hint) and recorded under `export: audited`
(request entry, then `records: N` when the query ended). Rows are masked
with `--mask-pii` or when the policy enforces masking, as `consume` does:
each value is masked with its column name as the JSON key (a `PASSWORD`
column is hidden whole, an `EMAIL` column's address masked, card numbers or
SSNs inside text masked, structs and maps field by field).
`exec` refuses queries, and `SHOW`/`DESCRIBE`/`EXPLAIN` output is metadata.
`PRINT` (raw topic records) is not run: `ferrika consume` reads topics with
full decoding and masking.

## Profiles

The server belongs to a Kafka profile, like the Schema Registry (a ksqlDB
server always works on one Kafka cluster):

| Field | Option of `profile add` |
| --- | --- |
| `ksql_url` | `--ksql-url URL`, with `--ksql-user U` and `--ksql-password-stdin` / `--ksql-password` putting `U:password@` into the URL |
| `ksql_ca_location` | `--ksql-ca-location PEM` |
| `ksql_certificate_location`, `ksql_key_location` | `--ksql-certificate-location PEM --ksql-key-location PEM` (both required together; the key unencrypted) |

`ksql_url` is a secret field like `schema_registry_url`: the password part
goes to the OS credential store (`<profile>/ksql_url`) and the file keeps
`https://user@host`; with `FERRIKA_SECRET_STORE=file` it stays in the file.
`profile show` masks it; replacing a profile without `--ksql-url` reports
that the ksqlDB URL was dropped.

Public API: `ClusterProfile` has public fields and is not
`#[non_exhaustive]`, so the four new fields break code that builds it with
a struct literal (the CLI and core use `ClusterProfile::new`). Allowed before
1.0 in a minor version; noted in the CHANGELOG.

Command options (`ferrika ksql ...`): `--ksql-url` (`FERRIKA_KSQL_URL`),
`--ksql-user` (`FERRIKA_KSQL_USER`), `--ksql-password`
(`FERRIKA_KSQL_PASSWORD`), `--ksql-password-stdin`. A hand-typed URL with
credentials prints a warning (shell history).

## MCP

Read tools on the server of the profile the MCP server starts with (no URL
argument, so stored credentials go only to that server):

| Tool | Arguments | Gate |
| --- | --- | --- |
| `ksql_list` | `kind`: streams, tables, queries, topics; `filter` | none (metadata), at most 500 entries |
| `ksql_describe` | `name`, `extended` | none |
| `ksql_query` | `sql` (one `SELECT`), `limit` (20, max 100), `timeout_secs` (5, max 30), `from_beginning`, `mask_pii` (default true; forced under enforced masking) | export gate (channel `mcp`; hidden from `tools/list` and refused under `export: forbidden`, recorded under `audited`) |

Write tools are left for a follow-up: the MCP write guard
(`guard.rs`/`write_tools.rs`) plans, confirms and executes one change with
one category against one Kafka cluster's topics; a ksqlDB script needs
per-statement categories, a confirmation that shows statements, and a
`--ksql-statements` style allow-list. `ksql_query` refuses non-`SELECT`
statements.

## Limits

| Limit | Value |
| --- | --- |
| request timeout (connect, TLS, each request) | `--timeout`, default 30 s (MCP 10 s) |
| `ksql query` rows | `--limit` 1..100,000, default 100 |
| `ksql query` time | `--timeout SECS`, none by default (push queries also stop on Ctrl+C) |
| MCP `ksql_query` | 100 rows, 30 s, text cells cut at 2,000 characters |
| one row line / one response body | 16 MiB / 64 MiB |

## Versions

Developed and tested against `confluentinc/ksqldb-server:0.29.0` (the
standalone ksqlDB release matching the Confluent Platform 7.x line), on
Kafka 3.9. The endpoints used exist since ksqlDB 0.15 (`/query-stream`,
`/close-query`); `sessionVariables` since 0.18; pull queries on streams
since 0.23. Older servers are not supported. Confluent Cloud ksqlDB uses
the same API with an API key as basic auth (not tested).

## Tests

- Unit tests: the lexer and classifier (comments, strings with `;` and
  `--`, quoted identifiers, lowercase, multiple statements, `CREATE ... AS
  SELECT`, `DELETE TOPIC`, connectors, variables, unknown statements, broken
  scripts), the HTTP transport against a local fake server (chunked rows,
  size caps, timeouts, unreachable servers, error bodies), the client (pull
  and push queries, row and time limits, cancel, close, `DESCRIBE` quoting),
  masking, profiles (secret field, `Debug` redaction), the CLI gate
  (all-or-nothing refusal, read-only profile, no literals in audit
  entries), the gate classification in `app.rs`, the MCP tools (refusals,
  export gate, masking).
- `tools/e2e/ksql_smoke.py` against a real ksqlDB server (CI job
  `e2e-ksqldb`: Kafka 3.9 and ksqlDB 0.29 on host networking).

Not tested end to end: basic authentication and TLS against a real server
(they need a JAAS file and keystores in the server container); both are
covered by unit tests of the request headers and the TLS setup.

## API surface for other front ends (desktop)

A ksqlDB view in another front end uses:

- `KsqlConfig::from_profile(&profile)` (and `.with_timeout`) and
  `KsqlClient::new`;
- `info`, `streams`, `tables`, `queries`, `topics`, `describe(name,
  extended)` for the browser;
- `statement::split` for the editor, `Statement::access`, `is_query`,
  `policy_actions`, `summary` for the gate and the confirmation dialog;
- per write: `EffectivePolicy::check` for every category, then
  `authorize_write(WriteRequest { source: "gui", action: categories[0],
  label: "ksql exec", target: &summary, ... }, AuditMode::ProbeOnly, ...)`
  and an audit result entry with the outcome; `Session::apply` for session
  statements and `KsqlClient::execute`;
- `run_query(sql, session, limits, cancel, on_row)` with a cancel future
  wired to a Stop button and `on_row` to append rows live (or `query` and
  `QueryStream::next_row` / `close` for full control), then
  `QueryResult::masked()` / `view::mask_row` under masking and the export
  gate for copy/export actions.

## Follow-ups

1. MCP write tool `ksql_exec` through the write guard (plan, confirm,
   per-statement categories, allow-list of statement kinds).
2. Desktop ksqlDB tab (private repository) on the API above.
3. HTTP/2 transport if a server ever drops HTTP/1.1 for `/query-stream`.
4. `--ksql-ca-location` etc. on the command for `--ksql-url` without a
   profile.
5. Basic-auth and TLS e2e against a configured server (JAAS, keystores).
6. Pagination is not needed (ksqlDB has none); very large `SHOW` results
   hit the 64 MiB cap.
