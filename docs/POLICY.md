# Signed company policy

A security team writes one policy, signs it with a private key it keeps, and
installs the signed file plus the public key on developer machines. Every
Ferrika on such a machine enforces the policy with no server: read-only
targets, forbidden actions, mandatory PII masking and auditing, no record
export, and a screen watermark. Changing one
character of the policy is detected, and Ferrika then falls back to its
strictest mode.

The policy file is shared with the commercial Ferrika desktop app, which is
not part of this repository. Rules and behaviour that act only in the desktop
app (the watermark, the unmask toggle, badges, buttons) are marked "desktop
app" below; the `ferrika` CLI reads the same rules and ignores those parts.

**Read [Threat model](#threat-model-and-what-open-source-changes) before you
rely on this.** Ferrika is open source, so anyone can build a binary that
skips the checks. A policy is enforced only on machines that are allowed to
run signed official binaries and nothing else.

Without the profile setting a developer could switch read-only and masking
off. With a policy they are mandatory in the official binary.

## Files and location

| File | What it is |
|---|---|
| `company.policy` | The signed policy (JSON, below) |
| `policy.pub` | The trusted Ed25519 public key (64 hex digits) |
| `policy.keys` | Optional: further trusted keys, for key rotation (below) |
| `policy-signing.pk8` | The private key (PKCS#8). **Stays with the security team, never installed.** |

Ferrika looks in a machine-wide directory that normal users cannot write:

- Windows: `Ferrika\` in the ProgramData known folder
  (`SHGetKnownFolderPath(FOLDERID_ProgramData)`, usually
  `C:\ProgramData\Ferrika\`). The `%ProgramData%` environment variable is
  not used, since any user can change it; if the known folder cannot be
  resolved, `C:\ProgramData\Ferrika\` is used.
- macOS / Linux: `/etc/ferrika/`
- `FERRIKA_POLICY_DIR` overrides the directory in **debug builds and builds
  with the `dev-policy-dir` feature only** (tests, demos:
  `cargo build --release -p ferrika-cli --features dev-policy-dir`). Shipped
  release builds
  ignore it, print a warning on stderr and record `policy.override_ignored`
  in the audit log, so a user cannot point Ferrika at a policy they signed
  themselves.

| Directory contains | Result |
|---|---|
| neither file | No policy: Ferrika behaves exactly as without this feature |
| both, signature valid | The policy is enforced; desktop app badge "Company policy active · signature verified (issuer)" |
| a policy directory that cannot be read (for example permission denied; only "not found" counts as absent) | **Fail closed**, as below: strictest mode and a `policy.invalid` audit entry, never "no policy" |
| a key file (`policy.pub`/`policy.keys`) only, `company.policy` only, bad signature, a key that is not trusted, a malformed line in `policy.keys`, unparsable JSON, unknown rule or category, unparsable `expires_at` | **Fail closed**: strictest mode (everything read-only, every change refused, masking enforced, unmasking forbidden, auditing required, record export forbidden, watermark on), red desktop app badge "Policy signature invalid · strictest mode", and a `policy.invalid` audit entry |

## Format

```json
{
  "format": 1,
  "key_id": "ef93539e524d8949",
  "payload": "{\"issuer\":\"Security team\",\"issued_at\":\"2026-11-02\",\"rules\":{...}}",
  "signature": "<128 hex digits>"
}
```

- `format`: 1, or 2 when the payload uses a format 2 field (`expires_at`,
  `on_expiry`, `read_only_brokers`, `audit_forward`, `audit_anchor`,
  `anchor_required`, `export`, `export_clusters`, `watermark`,
  `watermark_text`, `environments`, `default_environment`, `copy_down`;
  below). Rules left at their defaults are not written
  out, so they do not make a policy format 2.
- `key_id`: first 16 hex digits of SHA-256 of the public key.
- `signature`: Ed25519 over the exact bytes of the `payload` string, so no
  JSON canonicalisation is involved.
- `payload` (what `ferrika policy sign --in` reads; `ferrika policy example`
  prints this one; it stays format 1 so older builds can read it, and
  `read_only_brokers` below makes it format 2):

```json
{
  "issuer": "Security team",
  "issued_at": "2026-11-02",
  "rules": {
    "read_only": ["prod-*"],
    "forbid": ["topics.delete", "topics.purge", "offsets.reset", "acl.change"],
    "masking": "enforced",
    "unmask": "with_reason",
    "audit": "required"
  }
}
```

| Rule | Values |
|---|---|
| `read_only` | Globs (`*`, `?`, case-insensitive) matched against the profile name and each bootstrap address (`prod-*`, `*.prod.corp:*`). `*` = everything. Every change to a match is refused. The profile name is chosen by the user, so a name glob alone can be avoided by renaming a profile; use `read_only_brokers` for clusters that must stay read-only. |
| `read_only_brokers` | v2, optional: globs matched against broker addresses only, never the profile name (see [Read-only brokers](#read-only-brokers-format-2)). |
| `forbid` | Categories refused everywhere: `topics.create`, `topics.delete`, `topics.purge`, `topics.alter` (partitions, replication, leader election, reassignment), `config.change`, `produce` (also generate, mirror, restore, probe, bench produce, DLQ redrive), `offsets.reset`, `offsets.commit` (`consume --group`, which commits the group's offsets), `groups.delete`, `acl.change`, `users.change`, `quota.change`, `schema.change`, `connect.change`, `messages.peek` (RabbitMQ: `rabbit peek`, `rabbit dlq inspect` and the `rabbit dlq redrive` preview, which take messages and requeue them; MCP `rabbit_peek`, `rabbit_dlq_inspect` and the `rabbit_dlq_redrive` plan), `messages.move` (RabbitMQ: `rabbit dlq redrive --execute` and MCP `rabbit_dlq_redrive`, which also need `produce` allowed since they republish); `*` = every change. `rabbit publish` and MCP `rabbit_publish` are `produce`. `messages.peek` and `messages.move` are new in 0.2.0: builds before 0.2.0 do not know them and treat a policy that names them as invalid (strictest mode). |
| `masking` | `optional` (default) or `enforced`: PII stays masked in the desktop app's Message Studio, `consume`, `dlq`, `diff`, `trace`, `compact` and MCP; `dump` is refused, and so are `mirror` without `--mask` and `transform` without `--mask` as its first step. |
| `unmask` | With enforced masking: `allowed`, `with_reason` (the desktop app asks for a reason and records `record.unmask`), `forbidden` (the toggle is disabled). |
| `audit` | `optional` or `required`: a CLI change whose audit entry cannot be written is refused, and so is a desktop app unmask. The desktop app checks that the audit log can be written before it runs a change and refuses it otherwise ("Refused by company policy: the audit log cannot be written (audit: required)"). |
| `audit_anchor` | v2, optional: `none` (default) or `os_log`: every audit line's `seq` and `hash` are also written to the OS log so `audit verify --anchor` can detect a truncated or replaced log (see below). |
| `anchor_required` | v2, optional, default `false`: when `true`, a failed anchor write counts as a failed audit write (under `audit: required` the CLI change is refused). When `false` it only prints a warning. |

| `audit_forward` | Optional. Also send every audit entry to a SIEM; see [Forwarding audit entries](#forwarding-audit-entries-siem). |
| `export` | v2, optional: `allowed` (default), `audited` or `forbidden`: whether record data (keys, payloads, header values) may leave the app (see [Record export and watermark](#record-export-and-watermark-format-2)). |
| `export_clusters` | v2, optional: broker-address globs `export` applies to (matched like `read_only_brokers`, never the profile name). Empty (default) = every cluster. |
| `watermark` | v2, optional: `off` (default) or `on`: the desktop app draws user, host, time and cluster over the whole window (no effect in the CLI). |
| `watermark_text` | v2, optional: 1–40 characters (no control characters) put in front of the watermark, e.g. `"CONFIDENTIAL"`. |
| `mcp_write` | v2, optional: what an AI assistant may change through `ferrika mcp --allow-write`, on top of every other rule: `allowed` (default), `confirm_human` (only after a person confirms in the client through MCP elicitation; the one-time token, which the model can pass on its own, is refused, and clients without elicitation get no write tools) or `forbidden` (no write tools; calls are refused and audited). The CLI and the desktop app are not affected. |
| `screen_capture` | v2, optional: `allowed` (default) or `blocked`: the desktop app's window stays out of screenshots, screen recordings and screen shares (Windows; see [Record export and watermark](#record-export-and-watermark-format-2)). No effect in the CLI. |
| `environments` | v2, optional: environment tiers by name, each with the clusters it covers (`profiles`, `brokers` globs) and its own `read_only` and `forbid` (see [Environments](#environments-format-2)). |
| `default_environment` | v2, optional: the environment of a cluster no environment matches. |
| `copy_down` | v2, optional: `allow` (default), `masked` or `forbid`: what a record copy from a higher tier to a lower one needs. |

Unknown rule names or categories make the policy invalid (fail closed), so a
typo cannot silently weaken it.

### Read-only brokers (format 2)

```json
"rules": { "read_only_brokers": ["*.prod.corp", "10.20.*", "kafka-dr.corp:9094"] }
```

- Matched against every bootstrap address of the profile (or `--bootstrap`),
  never against the profile name, so a production profile renamed to `dev`
  is still read-only.
- A broker list may be separated by commas and/or whitespace
  (`a:9092, b:9092` or `a:9092 b:9092`); every entry is checked.
- Addresses and patterns are normalized before matching: surrounding spaces
  and a `SASL_SSL://`-style scheme are dropped, the host is lowercase
  without a trailing dot (`Kafka1.Prod.Corp.` is `kafka1.prod.corp`), and
  an address without a port is on 9092. IPv6 hosts are compared in brackets
  (`[fd00::5]:9092`, `::1` is `[::1]:9092`). Duplicates do not matter: the
  lists are compared as sets of `host:port`. `read_only` globs (which also
  match the profile name) see each broker both as written and normalized,
  and `environments.*.brokers`, `export_clusters` and the `mirror` /
  `transform` same-cluster check use the same normalization.
- A pattern without a port matches the host on any port (`*.prod.corp`
  covers `kafka1.prod.corp:9093`); a pattern with a port also matches the
  port (`10.20.*:9092`).
- The brokers' advertised addresses from the cluster metadata count too, so
  connecting to a read-only cluster through an IP or another DNS name still
  matches (this applies to `read_only` broker globs too). The desktop app uses the
  metadata it polls, so the first poll must have listed the brokers. The CLI
  (and MCP write tools) fetch the metadata right before a change, but only
  when `read_only_brokers` is set; with no such rule no request is made. If
  the metadata cannot be read, the change is refused rather than waved
  through. Cover a cluster's IP ranges as well as its names if you want the
  rule to hold without a metadata lookup.
- Refusals name the matching address and rule: "Refused by company policy
  …: 'kafka1.prod.corp:9093' is read-only (rule "*.prod.corp"), so
  `produce` is not allowed." The desktop app shows the `read-only (policy)` tag,
  and the badge's rules window lists "Read-only brokers".
- `policy show` prints a `read-only brokers:` line when the rule is set;
  `-f json policy show` has it in `rules`.
- Older builds reject the unknown rule and fall back to the strictest mode:
  update Ferrika before installing a policy that uses it.

### Expiry (format 2)

Two optional payload fields make a policy expire:

```json
{
  "issuer": "Security team",
  "issued_at": "2026-11-02",
  "expires_at": "2027-06-30T23:59:59+09:00",
  "on_expiry": "warn",
  "rules": { ... }
}
```

| Field | Values |
|---|---|
| `expires_at` | RFC 3339 time with an offset (`2027-06-30T23:59:59+09:00`, `2027-06-30T15:00:00Z`). A plain date or anything unparsable makes the policy invalid (fail closed). |
| `on_expiry` | `warn` (default): the rules keep applying, with a warning. `strictest`: the strictest mode, as for an unverifiable policy. |

- An expired policy never turns into "no policy": it keeps its rules or
  becomes stricter.
- From 14 days before `expires_at` the CLI prints a warning on stderr
  ("The company policy by Security team, key … expires in 9 days …"), refusal
  messages carry "(expires in 9 days)", and the desktop app badge turns amber
  ("… · expires in 9 days"). Once expired: amber "Company policy expired ·
  rules still apply" (`warn`) or red "Company policy expired · strictest
  mode" (`strictest`). The desktop app checks the time whenever it consults the
  policy, so a window left open past `expires_at` switches too.
- `policy sign` writes `"format": 2` when the payload uses either field (or
  the rules `read_only_brokers`, `audit_forward`, `audit_anchor`,
  `anchor_required`, `export`, `export_clusters`, `watermark` or
  `watermark_text`) and
  `"format": 1` otherwise, so policies without them stay readable by older
  Ferrika builds. `verify` accepts both; a file marked format 1 that uses
  the new fields is invalid.
- Older builds refuse format 2 (they fall back to the strictest mode).
  **Update Ferrika on the machines before installing a policy with
  `expires_at`.**
- `policy show` and `policy verify` print the expiry; `-f json policy show`
  adds `expires_at`, `on_expiry`, `expiry` (`current`, `expires_soon`,
  `expired`, `expired_strictest`) and `strictest`, and its `rules` are the
  ones in force now.

### Record export and watermark (format 2)

```json
{
  "issuer": "Security team",
  "issued_at": "2026-11-03",
  "rules": {
    "masking": "enforced",
    "export": "forbidden",
    "export_clusters": ["*.prod.corp", "10.20.*"],
    "watermark": "on",
    "watermark_text": "CONFIDENTIAL"
  }
}
```

`export` governs **record data** (keys, payloads, header values) leaving the
app. Looking at records on screen is never refused; the watermark leaves a
trace on photos and screen shares instead.

| `export` | Effect |
|---|---|
| `allowed` (default) | As without the rule; nothing is recorded. |
| `audited` | Every export goes ahead and is recorded first as `record.export` (target `export:<channel> <command> <topic>`, outcome `requested`); when the export finishes, a second `record.export` line with the same target records `records: N` where the command knows the count (`dump`, `consume` to a file or redirected stdout, `dlq export`, `compact snapshot`, `mirror`). Under `audit: required` an export whose entry cannot be written is refused. The CLI says so on stderr. |
| `forbidden` | Every export is refused and recorded as `policy.blocked` ("Refused by company policy …: exporting record data is forbidden (export: forbidden), so `dump` is not allowed."). |

What counts as an export:

| Where | Exports (gated) | Not gated |
|---|---|---|
| CLI, files | `dump`, `consume --save`, `dlq export`, `compact snapshot` | `report`, `monitor export`, `topics analyze` and other metadata (topics, groups, lag, brokers) |
| CLI, stdout | `consume` (also `-f json`), `dlq inspect`, `compact tombstones`, `diff`, `trace inspect`, `crypto mask --topic`: only when stdout is redirected or piped. On a terminal the records are only viewed and are allowed. | `crypto mask` reading stdin (the user's own input) |
| CLI, other cluster | `mirror` and `transform --execute` whose destination brokers are not all brokers of the source (normalized like `read_only_brokers`; the same brokers under another profile name are the same cluster). Under enforced masking `mirror` needs `--mask` anyway. | A copy within one cluster (a `produce`) |
| Desktop app | Message Studio: Export (JSONL), Save payload, Copy record as JSON, Copy payload, and Resend (its text can be copied from the producer form). Under `forbidden` these buttons are disabled with the tooltip "Disabled by company policy (export: forbidden)", and pressing one anyway is refused. | Topics/Consumers Export CSV, Overview Report (metadata) |
| MCP | `peek_messages` and `transform_preview` hand records to an outside AI service: under `forbidden` they are left out of `tools/list` and refused if called; under `audited` each call is recorded. The RabbitMQ write tools `rabbit_peek` and `rabbit_dlq_inspect` are refused under `forbidden` (matched on the broker's management and AMQP addresses) and recorded under `audited`. | The other tools (metadata, `transform_execute`, which copies within the cluster, and the `rabbit_dlq_redrive` preview, which returns destinations only) |

- `crypto audit` is a CI gate run in pipelines, so it is not refused: under
  `forbidden` it keeps its counts (and `--fail-on-leak`) but leaves out the
  findings, whose masked samples are parts of records.
- Masking is a separate rule. Masked records are still record data:
  `export: forbidden` refuses them too, and `masking: enforced` with
  `export: allowed` exports masked values as before.
- `export_clusters` is matched against the cluster's bootstrap addresses and
  the brokers from its metadata, never the profile name; the source cluster
  for `mirror`. A cluster none of the globs matches is treated as `allowed`.
  When `export` is not `allowed` and `export_clusters` is set, the CLI looks
  up the metadata before the export; if that fails, the export is refused.
- **stdout rule.** There is no flag or variable to get around it. Known false
  refusal: older Git Bash (mintty without winpty) presents the console as a
  pipe, so record output is refused there; the message says to use
  PowerShell or Windows Terminal. Not prevented: copying from the terminal's
  scrollback, terminal session recording (`script`, ConPTY recorders) and
  terminal log features.
- File paths are never recorded (a path can name a customer); entries name
  the channel, command and topic (and partition/offset in the desktop app).
- `watermark: on` tiles `[watermark_text ·] user@host · YYYY-MM-DD HH:MM
  zone · cluster` faintly over the whole desktop app window, above dialogs,
  notifications, menus and tooltips; clicks, scrolling and typing pass
  through. The time follows the UTC setting and moves on every 30 seconds;
  the cluster is the connected profile (or the first bootstrap address). It
  is separate from `export`: turning export off does not turn the watermark
  on.
- `screen_capture: blocked` is the stronger, opt-in step: the desktop app
  window is left out of screenshots, screen recordings and screen shares
  (Windows 10 2004 or later: `SetWindowDisplayAffinity(WDA_EXCLUDEFROMCAPTURE)`,
  the window is missing from the capture; older Windows: `WDA_MONITOR`, it is
  black). It also hides the app when someone shares their screen during an
  incident, which is why it is not part of `export: forbidden` or of the
  strictest mode. It does not stop a phone camera; combine it with the
  watermark. macOS and Linux builds ignore it (the badge's rules window says
  "not supported on this system").
- The strictest mode (an unverifiable policy, or `on_expiry: strictest`
  after expiry) forbids export everywhere and turns the watermark on; it
  does not block screen capture (people need to show the red badge to
  whoever fixes the policy).
- `policy show` prints `record export:`, `watermark:` and `screen capture:`
  lines; `-f json policy show` always has `export`, `export_clusters`,
  `watermark` and `screen_capture` in `rules`. The desktop app's rules window lists Export, Export clusters, Watermark and Screen capture.
- **Rolling out.** Older builds reject the unknown rules and fall back to the
  strictest mode, where every change is refused. Update Ferrika on every
  machine before installing a policy that uses them. A typo in a value
  (`"forbiden"`), an empty `export_clusters` entry or a bad `watermark_text`
  makes the policy invalid; `policy sign` refuses them.


### Environments (format 2)

Environment tiers classify clusters (`prod`, `staging`, `dev`) so a policy
can say "production data may not be copied to a lower environment
unmasked" and give production stricter rules than the global ones.

```json
{
  "issuer": "Security team",
  "issued_at": "2026-11-22",
  "rules": {
    "forbid": ["acl.change"],
    "environments": {
      "prod":    { "tier": 3, "brokers": ["*.prod.corp", "10.20.*"],
                   "profiles": ["prod-*"],
                   "forbid": ["topics.delete", "topics.purge", "offsets.reset"] },
      "staging": { "tier": 2, "brokers": ["*.staging.corp"] },
      "dev":     { "tier": 1, "brokers": ["localhost", "127.0.0.1", "*.dev.corp"] }
    },
    "default_environment": "dev",
    "copy_down": "masked"
  }
}
```

- **Names and tiers.** An environment name is 1–32 letters, digits, `-`,
  `_` or `.`. `tier` is a whole number; higher is more sensitive. Two
  environments may share a tier (neither is below the other).
- **Matching.** A cluster is in an environment when one of its broker
  addresses matches one of the environment's `brokers` globs (matched like
  `read_only_brokers`: normalized, a pattern without a port matches any
  port, and the brokers from the cluster metadata count too), or its
  profile name matches one of its `profiles` globs. The profile name is the
  user's to choose, so a name glob alone can be avoided by renaming a
  profile: classify clusters that matter by `brokers`. A cluster that no
  environment matches is in `default_environment`, or in no environment
  when that is not set.
- **More than one match.** A cluster can match several environments. Their
  `read_only` and `forbid` rules all apply; it is shown (and judged as a
  copy source) as the highest of them, and judged as a copy destination as
  the lowest, so the stricter reading always wins.
- **`read_only` and `forbid` per environment.** `"read_only": true` refuses
  every change to the environment's clusters; `forbid` refuses those
  categories there, on top of the global `forbid` (which still applies
  everywhere). So `prod` can forbid `topics.delete` while `dev` allows it.
  Refusals name the environment: "Refused by company policy …:
  `topics.delete` is forbidden in the environment 'prod'." They apply in
  the CLI, the MCP write tools and the desktop app alike (the shared
  `EffectivePolicy::check`). When an environment with such rules is told
  apart by `brokers`, the CLI reads the cluster metadata before a change,
  as for `read_only_brokers`; if that fails, the change is refused.
- **`copy_down`.** A record copy from a cluster whose environment has a
  higher tier to one with a lower tier, or to a cluster in no environment,
  is a copy down. A copy whose source is in no environment is not judged,
  and a copy within one cluster never is.

  | `copy_down` | A copy down is |
  |---|---|
  | `allow` (default) | allowed, as without the rule. |
  | `masked` | allowed only when every record is PII-masked before anything else reads it: `transform --mask …` (mask as the first step) or `mirror --mask` (which cannot be combined with mirror's raw-value filters). Otherwise refused and recorded as `policy.blocked`. |
  | `forbid` | refused and recorded as `policy.blocked`, masked or not. |

  The rule is checked in `ferrika_ops::mirror::authorize_copy`, which
  `mirror`, `transform --execute` and the desktop app's copy all use, before
  the export gate. Both clusters' metadata is read when `copy_down` is set
  and an environment has `brokers` globs; if that fails, the copy is
  refused. Example refusal: "Refused by company policy …: `mirror` would
  copy unmasked records from 'prod' (tier 3) to 'dev' (tier 1), a lower
  environment, and the policy requires them masked (copy_down: masked). Add
  --mask to copy masked records." `dlq redrive` re-produces records to the
  same cluster, so it is never a copy down.
- **Where the environment shows.** `policy show` lists the environments
  (highest tier first), `default env:` and `copy down:`; `-f json policy
  show` always has `copy_down` in `rules`. `profile list` adds an `ENV`
  column while the policy names environments (and `-f json profile list`
  an `environment` field, `null` without one); it judges each profile by
  its name and bootstrap servers only, without reading cluster metadata.
  An allowed change's audit request entry has the outcome `requested
  (environment prod)` instead of `requested`.
- **Why not an audit field.** The environment goes into the outcome text
  rather than a new audit field. A new optional field would keep the hash
  chain valid (older lines keep their hashes, like `actor`), but every
  front end builds audit entries itself, so a new field would change the
  entry type for all of them; the text needs no such change.
- **Strictest mode** ignores the environments: `read_only: *` and
  `forbid: *` already refuse every change and copy.
- **Mistakes fail closed.** An unknown field in an environment
  (`"readonly"`), a missing `tier`, an unknown category in its `forbid`, an
  empty glob, a `default_environment` that names no environment, or
  `copy_down` without `environments` makes the policy invalid; `policy
  sign` refuses them.

### Compatibility

- Policies without these fields verify and behave exactly as before, and
  `sign` keeps writing format 1 for them.
- A policy that uses `environments`, `default_environment` or a
  `copy_down` other than `allow` is format 2.
- **Older builds** (any build without environment support) refuse unknown
  rule fields rather than ignore them, so they treat such a policy as
  invalid and run in the strictest mode: every change and every record export is refused until
  Ferrika is updated. Builds that only read format 1 refuse format 2 files
  the same way. Ignoring an unknown section would silently drop a
  restriction, so failing closed is deliberate. Update Ferrika on every
  machine before installing a policy that uses environments.


## Forwarding audit entries (SIEM)

`rules.audit_forward` sends each audit entry, **after** it has been appended
to the local hash-chained log, to syslog and/or a spool folder:

```json
{
  "issuer": "Security team",
  "issued_at": "2026-11-02",
  "rules": {
    "audit": "required",
    "audit_forward": {
      "syslog": "tcp://siem.corp:601",
      "facility": "auth",
      "app_name": "ferrika",
      "spool_dir": "C:\\ProgramData\\Ferrika\\spool",
      "timeout_ms": 1000,
      "required": false
    }
  }
}
```

| Field | Values |
|---|---|
| `syslog` | `udp://host[:port]` (RFC 5426, default port 514), `tcp://host[:port]` (RFC 6587 octet counting, default 514) or `tls://host[:port]` (RFC 5425, default 6514). IPv6 as `[::1]`. |
| `spool_dir` | A folder a log collector (Splunk Universal Forwarder, Filebeat, NXLog…) watches. Each entry is appended as one JSON line to `ferrika-audit-<host>-<user>.<random>.jsonl` (16 hex digits), files per machine and account; see [Spool folder security](#spool-folder-security). Point the collector at `ferrika-audit-*.jsonl`. |
| `facility` | Syslog facility name, default `auth` (`kern`, `user`, `daemon`, `auth`, `authpriv`, `security`, `local0`…`local7`, …). |
| `app_name` | RFC 5424 APP-NAME, default `ferrika` (1–48 printable ASCII, no spaces). |
| `timeout_ms` | Longest a command or desktop app action waits for one forward, default 1000, allowed 50–10000. |
| `required` | Default `false`. With `true` **and** `audit: required`, a failed forward refuses the CLI change (and a desktop app unmask); the entry is still in the local log. |
| `ca_file` | PEM CA file for `tls://` (the OS trust store otherwise). |

At least one of `syslog` and `spool_dir` is needed; both may be set. Bad
values or unknown fields make the policy invalid (fail closed), and
`policy sign` refuses them.

**Message format.** One RFC 5424 message per entry:

```
<37>1 2026-11-02T09:30:00.123Z DEV-PC-01 ferrika 4242 audit - {"time_ms":…,"action":"topics delete",…,"user":"kim","prev":"…","hash":"…"}
```

PRI = facility × 8 + severity (notice 5 for `ok`, warning 4 for failed or
refused operations), UTC timestamp with milliseconds, host name, APP-NAME,
process id, MSGID `audit`, no structured data (`-`), and as MSG the exact
JSON line from the local log, `prev` and `hash` included (no BOM), so the
SIEM can check the chain and notice when the end of a PC's log was cut off.
Over TCP/TLS each message is framed as `LEN SP MSG` (byte count) on its own
connection; over UDP it is one datagram.

**Behaviour.**

- Forwarding happens after the local append succeeds; if the local append
  fails nothing is forwarded (and the existing `audit: required` rules apply).
- Each target is sent on a worker thread; the caller waits at most
  `timeout_ms` (DNS, connect, TLS handshake and write included), so a dead
  collector can never hang the CLI or the desktop app.
- A failure is counted and logged (`FERRIKA_LOG=ferrika=warn`); the CLI also
  prints `warning: audit entry saved locally but not forwarded to the SIEM (…)`
  on stderr. Nothing about the failure is written into the chain.
- Default: a failed forward does **not** block the action (a network outage
  should not stop operations that are already recorded locally). Set
  `required: true` together with `audit: required` to make it block.
- "Sent" means handed to the network or written to the spool folder: syslog
  has no acknowledgement. There is no local retry queue yet; an entry that
  failed to forward is only in the local log (and in the spool, if both are
  configured).

### Spool folder security

A spool folder is often shared by every account on a machine, so Ferrika
does not trust what it finds there:

- A new spool file gets an unpredictable name (`.<16 random hex digits>.jsonl`)
  and is created with create-new semantics, so an existing file or link is
  never opened by mistake. On Unix it is created `0600`; on Windows it gets
  the folder's inherited ACL (with the `-SpoolDir` folder of
  `Install-FerrikaPolicy.ps1`: the creator may modify it, other users only
  read it; a folder under the user profile is the user's own).
- Ferrika keeps appending to the newest of its own files
  (`ferrika-audit-<host>-<user>.<random>.jsonl`) only when it passes these
  checks: not a symbolic link, junction or other non-regular file (opened
  without following links), and owned by the current account (Windows: the
  account, or the default owner of its new objects, e.g. Administrators for
  an elevated process). On Unix it must also not be writable by group or
  others and have a single hard link. A file that fails is left alone (not
  deleted), a warning names it (`audit forwarding: skipping spool file …`),
  and the entry goes to a new file.
- Ferrika creates a missing spool folder owner-only (`0700`) on Unix. An
  existing folder on Unix must be owned by the current account or root and
  must not be writable by group or others, unless the sticky bit is set
  (`1777` / `1733`, like `/tmp`: no one can rename or delete another
  account's files). Otherwise forwarding to it fails with a clear error
  (`spool folder … may be written by group or others`).
- Files are `0600`, so on Unix the log collector has to run as root or as
  the same account. The files written by Ferrika 0.2.0 and earlier
  (`ferrika-audit-<host>-<user>.jsonl`, no random part) are no longer
  appended to.

**Without a policy (individual use).** `FERRIKA_AUDIT_FORWARD` sets
forwarding for one user: `udp://host:514`, `tcp://host:514`,
`tls://host:6514`, `spool:C:\path\to\folder`, or the JSON of an
`audit_forward` section. A verified policy with `audit_forward` always wins
(the variable is then ignored); a policy without it leaves the variable in
effect. `required` from the variable is ignored: only a signed policy can make
forwarding block actions. An invalid policy's section is never trusted.

**Testing.**

```bash
ferrika audit forward-test                          # configured targets; writes a real, chained audit.forward_test entry
ferrika audit forward-test --to udp://127.0.0.1:514 # try a target before signing it (entry not written locally)
ferrika -f json audit forward-test                  # per-target ok/error/elapsed_ms; exit code 1 if any failed
```

`ferrika policy show` lists the forwarding targets.

**Rolling out.** Ferrika versions without this feature reject the unknown
`audit_forward` rule and fall back to the strictest mode. Upgrade every
machine before signing a policy that contains it. Policies without it verify
and behave exactly as before.

## Commands

```bash
ferrika policy example --out policy.json                   # starting point (or print to stdout)
ferrika policy keygen --out keys/                          # keys/policy-signing.pk8 + keys/policy.pub
ferrika policy sign --key keys/policy-signing.pk8 --in policy.json --out company.policy
ferrika policy verify [company.policy] [--pub policy.pub]  # defaults: the installed files
ferrika policy show                                        # effective policy, source, key id, valid/invalid
ferrika -f json policy show
```

`policy verify` without `--pub` checks against every trusted key of the
policy directory; `policy show` lists them and marks the one that signed
the policy.

`policy show` also warns when the policy directory or a policy file in it
may be written by accounts other than administrators (`-f json`:
`dir_warnings`, a list of messages). Anyone who can add a broken
`policy.keys` there can at least force strictest mode. The check is best
effort: on Unix the directory and files must be owned by root and not
writable by group or others; on Windows the owner and every allow entry
that grants write, create, delete or permission-change rights (inherit-only
entries aside) must be Administrators, SYSTEM or TrustedInstaller. The
install scripts in `packaging/` set these permissions and verify them.

## Deployment

Install the policy with the scripts in [`packaging/`](../packaging/README.md),
not by copying the files by hand:

- Windows (Intune, GPO, SCCM): `packaging/windows/Install-FerrikaPolicy.ps1 -SourceDir <folder>`
  (`-WhatIf` previews), or the WiX fragment `packaging/windows/FerrikaPolicy.wxs` for an MSI.
- Linux / macOS: `sudo packaging/linux/install-ferrika-policy.sh <folder>`.

They copy `company.policy`, `policy.pub` and (if present) `policy.keys`,
and lock the folder: on Windows owner Administrators, inheritance off,
Administrators and SYSTEM full control, Users read only; on Linux
`root:root` `0755`/`0644`. An existing folder gets its owner and ACL reset,
because ProgramData lets any user create `Ferrika\` first and, as its owner,
later delete the policy. The Windows script also registers the `Ferrika`
event log source for the audit anchor (below). Roll out the Ferrika binary
before a policy that uses newer fields.

## Key rotation (`policy.keys`)

Ferrika trusts the key in `policy.pub` plus every key listed in an
optional `policy.keys` next to it, and checks a policy with the trusted key
its `key_id` names. A policy signed by a key in neither file is invalid
(fail closed), and so is the whole policy when `policy.keys` has a line
that is not a key.

```text
# policy.keys: one key per line, then an optional label; # starts a comment
3b6a27bcceb6a42d62a3a8d02a6f0d73653215771de243a63ac048a18b59da29  security team 2026
9f0e1d2c3b4a59687766554433221100ffeeddccbbaa99887766554433221100  security team 2027
```

Rotating from an old key to a new one without a moment in strictest mode:

1. `ferrika policy keygen --out keys-2027/` creates the new pair.
2. Add the new public key (the hex in `keys-2027/policy.pub`) as a line of
   `policy.keys` on every machine. Leave `policy.pub` (the old key) and
   `company.policy` as they are. Machines now trust both keys.
3. Once every machine has the new `policy.keys` **and a Ferrika build that
   reads it** (older builds ignore `policy.keys` and trust only
   `policy.pub`, so a policy signed with the new key puts them in strictest
   mode), sign the policy with the new key and install the new
   `company.policy`.
4. Retire the old key: replace `policy.pub` with the new key and remove its
   line from `policy.keys` (or delete `policy.keys`).

If the old private key leaked, skip the waiting: install the new
`policy.pub`, remove the old key from both files and install a policy
signed with the new key at once. Machines that still have the old files
then run in strictest mode until they get the new ones, which is the safe
direction.

## What is enforced where

- **CLI**: every changing command is checked before it runs (the same list
  that read-only profiles use). Refusals name the policy and are logged as
  `policy.blocked`. `consume --group` commits the group's offsets, so it is
  the `offsets.commit` change: on a read-only target, or when
  `offsets.commit` (or `*`) is forbidden, it is refused before connecting;
  `consume` without `--group` reads without committing and is never refused.
  `consume` masks keys, payloads and headers (`--mask-pii`
  without a policy; `--hex` is ignored while masking). Under enforced masking
  `dlq inspect`/`export`, `diff`, `trace inspect` and `compact
  snapshot`/`tombstones` mask the keys, payload previews, exception texts and
  header values they print or write (JSON included; previews are cut after
  masking). `diff` still compares the real values and `dlq redrive --execute`
  re-produces the real records, which is a change the policy checks like any
  other. `dump` is refused under enforced masking, and so is `mirror` (it
  would copy unmasked records to another cluster) unless it runs with
  `--mask`. `mirror` is also checked
  against its destination cluster (`--dst-profile`/`--dst-bootstrap-servers`)
  as a `produce`, so a read-only destination or `forbid: produce` refuses it.
  Under `export: forbidden`/`audited` the commands that write record data to
  a file, to redirected stdout or to another cluster are refused or recorded
  before they connect (see [Record export and watermark](#record-export-and-watermark-format-2)).
- **Desktop app**: every changing action (create/delete/purge topics, produce, offset
  resets, ACL, config, schema, connect, user and quota changes) is refused by
  the worker, in live and mock mode, and logged as `policy.blocked`. Under
  `audit: required` a change is also refused before it runs when the audit
  log cannot be opened for appending; the outcome entry is still written
  after a change that runs. The title
  bar shows the badge (click it for the rules) and "read-only (policy)" when
  the connected profile matches. Message Studio starts masked and masks
  again on every topic change. Its filter autocomplete never suggests record
  values under enforced masking (not even after an audited unmask) or while
  the panel masks; schema field names and enum symbols are still offered.
  Under `export: forbidden` Message Studio's
  export, save, copy and Resend buttons are disabled, and `watermark: on`
  draws the watermark over the window.
- **`lint --fix` and the desktop app's config check**: each fix is checked as the
  category of the change it makes, not as one `lint` command:
  config changes (topic overrides, broker and cluster-default configs) are
  `config.change`; replication changes and preferred-leader elections are
  `topics.alter`; deleting an orphaned group is `groups.delete`. So
  `forbid: ["config.change"]` blocks most auto fixes while replication fixes
  still run, and a read-only target refuses them all. The CLI leaves refused
  fixes out of the plan (nothing is logged until a fix is tried), then logs
  each attempt as `lint fix <rule>` with its outcome on a second line; under
  `audit: required` an unwritable log stops the run before any change.
  `--dry-run` changes nothing and is not checked, like `topics config
  --dry-run`. The desktop app sends each fix as the ordinary command, so its gate
  applies, and greys out buttons the policy or profile would refuse.
- **Undo** (`ferrika undo`, the desktop app's History tab and Undo toast): undo is a
  change like any other. It is checked under the category of each change it
  makes (undoing a topic creation is `topics.delete`, undoing a config change
  `config.change`); there is no separate `history.undo` category. It is
  refused on read-only targets, and under `audit: required` like other
  changes. It is also refused when the history entry no longer matches the
  audit log's `op_digest` (recorded as `history.tampered`), when the cluster
  in use is not the one the entry names (same cluster id when one was
  recorded, else the same bootstrap servers), and, with a policy installed,
  when the audit log has no entry for the operation. An entry with a legacy
  digest (recorded by 0.1) is undone only after a second confirmation or
  with `--accept-legacy`.
- **MCP**: `mask_pii: false` is ignored under enforced masking. The read
  tools never change the cluster. With `ferrika mcp --allow-write`, every
  write tool call (`produce_messages`, `create_topic`, `alter_topic_config`,
  `reset_consumer_group_offsets`, `transform_execute`, and for RabbitMQ
  `rabbit_peek`, `rabbit_dlq_inspect`, `rabbit_publish`,
  `rabbit_dlq_redrive`) is checked like a CLI command (`produce`,
  `topics.create`, `config.change`, `offsets.reset`, `produce`,
  `messages.peek`, `messages.peek`, `produce`, `messages.move` plus
  `produce`; the RabbitMQ ones against the RabbitMQ profile and the
  management and AMQP `host:port`, as `ferrika rabbit`): `forbid`,
  `read_only`, `read_only_brokers`, strictest mode and a read-only profile
  refuse it, at the plan and again right before it runs, and tools refused
  outright are not offered. Under `audit: required` a write is refused when
  the audit log cannot be appended to. Refusals are `policy.blocked`;
  results are `mcp <tool>` entries with source `mcp` and an `actor` naming
  the MCP client and session (self-reported by the client, so the OS `user`
  stays the identity). There is no policy rule yet that allows CLI and desktop app
  writes but refuses MCP writes; until then use `forbid`/`read_only`, or do
  not start the server with `--allow-write`. Under `export: forbidden`
  `peek_messages` and `transform_preview` are hidden and refused, and
  `rabbit_peek` and `rabbit_dlq_inspect` are refused; under
  enforced masking `transform_preview` puts a mask step first and
  `transform_execute` is refused unless its first step masks (as
  `ferrika transform`). `transform_execute` copies only within the server's
  cluster, so it never passes the export gate. `config_lint` and the
  RabbitMQ listings (`rabbit_queues`, `rabbit_exchanges`,
  `rabbit_bindings`) only read; lint fixes are not offered as a tool.

## Tamper-evident audit log

Each line of `~/.ferrika/audit.log` now records the OS user and two hashes:
`prev` (the previous line's `hash`, or SHA-256 of an older unchained line,
or 64 zeros for the first line) and `hash` (SHA-256 of the line's JSON
without `hash`). Older unchained lines at the start stay readable.

```bash
ferrika audit verify      # ✔ OK, or ✖ the first edited/inserted/removed line (exit code 2)
ferrika audit verify --anchor   # also compare with the OS log anchor (truncated/rewritten → exit code 2)
ferrika audit             # now with a USER column
```

Entries written by the policy: `policy.blocked`, `policy.invalid`,
`policy.override_ignored`, `record.unmask` (user, time, profile, topic, partition/offset, reason),
`record.export` (`export: audited`; source `cli`, `gui` or `mcp`, target
`export:<channel> <command> <topic>`, outcome `requested` before the export and `records: N` after it where known;
no file paths). A refused export is a `policy.blocked` entry with an
`export:` target.

Changes recorded for undo carry two optional fields: `op` (the operation id
in `history.jsonl`) and `op_digest` (the digest of its journal entry, see
[History digests](#history-digests)). In the CLI the request line (outcome `requested`) already
names the `op`, and a second line with the same action and `op` gives the
result and adds `op_digest` once the change has run; a command that changes
several resources (`offset delete-group a b`) names its first operation on
the request line, and each operation has its own result line. The desktop app
writes both fields on its one outcome line. Lines are matched by `op`, so
lines written before this layout (a separate `<command> recorded` result
line) still verify. Lines without an operation leave the fields out, so
older lines and `audit verify` are unaffected. A refused undo of an edited
history entry writes `history.tampered`. The history file itself (it holds
config values) is not forwarded to a SIEM.

### History digests

`op_digest` has two versions; the audit log's value says which one applies,
so editing the journal cannot select the weaker one.

| Version | Written by | Value | Covers |
|---|---|---|---|
| 2 | 0.2.0 and later | `v2:` + SHA-256 (64 hex) | the whole entry: id, source, user, scope (profile, bootstrap servers, cluster id, registry, Connect URL), action, target, reversibility, limits, info, before-state, `undoes`, final after-state, result |
| 1 (legacy) | 0.1.x | SHA-256 (64 hex), no prefix | before- and after-state only |

The version is a prefix of the digest string rather than a separate field:
a 0.1 reader compares the whole string with its own 64-digit digest, so it
can never accept a version 2 entry by mistake (it refuses to undo it as
edited instead), and older audit lines keep their chain hashes.

- A version 1 entry still verifies, but only its states are protected: its
  scope, action, `undoes` link and reversibility could have been edited.
  `ferrika history` (AUDIT column), `history show` and `audit verify` show
  it as `legacy digest (scope not covered)`; `ferrika undo` prints the
  cluster the entry names and the one in use and asks again (`-y` needs
  `--accept-legacy`).
- An `undoes` link (which marks another operation as undone) counts only
  when the undoing entry's version 2 digest matches, or, without a policy,
  when the audit log has no entry for it. Otherwise `undo` warns, ignores
  the link and lets the current state decide.
- `ferrika audit verify` also compares every history entry with its
  `op_digest`: verified, legacy, without an audit entry, or edited (listed,
  exit code 2). `-f json` adds the counts as `history`.
- Before undo runs, the cluster in use must be the recorded one. With a
  recorded cluster id the current cluster's id must be readable and equal
  (bootstrap servers alone are not enough); without one the set of
  bootstrap servers must match; Schema Registry and Kafka Connect changes
  compare those URLs.
- A failed multi-step undo is not rolled back. Its outcome (shown and kept
  in the history entry) lists the steps applied before the failure, which
  stay in place, and the steps not run. Running the undo again plans only
  what still differs from the recorded before-state (it reports drift, so
  it needs `--force`).

The journal (`history.jsonl`) is append-only for each change. Once it
passes 1 MiB it is trimmed to the last 1000 operations of the last 90
days; if it is still above 512 KiB it is renamed to `history.jsonl.1`,
replacing the previous one (whose operations and connector secrets are
dropped). `history` and `undo` read both files.

### Truncation detection: `seq` and the OS log anchor

Every chained line also carries `seq`, its 1-based position among the
chained lines (inside the hashed JSON). Lines written by older versions have
no `seq` but still count, so the first line written after upgrading gets
`seq` = number of existing chained lines + 1 (the appender reads the file
once to count them; afterwards it continues from the last line). Torn
fragments do not count. `audit verify` reports a line whose `seq` is not its
position.

With `"audit_anchor": "os_log"` each append (inside the log's file lock)
also writes `ferrika-audit-anchor seq=<n> hash=<hex> log=<path>`:

- Windows: Application log, source `Ferrika`, event ID 1000
  (`ReportEventW`). `ferrika audit verify --anchor` reads the log backwards
  for the newest anchor of this audit log and compares: log shorter than the
  anchored `seq` or a different hash at that `seq` → "truncated or
  rewritten", exit code 2. A log longer than its anchor is fine (later anchor
  writes may have failed).
- Linux / macOS: syslog (ident `ferrika`, facility auth). Not read back;
  `--anchor` says so and the check is manual (`journalctl -t ferrika`; see
  [packaging/README.md](../packaging/README.md#audit-anchor-on-linux-journald--syslog)
  for persistent journald and forwarding).

Writing events needs no administrator rights, but without a registered
source Event Viewer shows "The description for Event ID 1000 from source
Ferrika cannot be found" (the message is stored and read back anyway).
Register the source once per machine in the installer or deployment script
(`packaging/windows/Install-FerrikaPolicy.ps1` and the WiX fragment do it;
by hand, administrator PowerShell):

```powershell
New-EventLog -LogName Application -Source Ferrika
```

`New-EventLog` creates the registry key
`HKLM\SYSTEM\CurrentControlSet\Services\EventLog\Application\Ferrika`
(with the .NET `EventLogMessages.dll` as `EventMessageFile`); an MSI can
create the same key directly. Remove it with `Remove-EventLog -Source Ferrika`.

The real-OS round trip is an ignored test:
`cargo test os_log_anchor_round_trip -- --ignored`.

A failed anchor write prints a warning and the change goes ahead, unless
`anchor_required: true`. Limits: any process of the same user can write a
forged `Ferrika` event, and an administrator can clear the Application log;
the anchor makes a quiet truncation leave a trace, it does not make one
impossible. Older Ferrika binaries refuse a policy with these fields
(unknown rule → strictest mode), so roll out the binary first.

## Korean PII

Masking and `ferrika crypto audit` also recognise 주민등록번호/외국인등록번호
(`YYMMDD-GXXXXXX` with a real birth date; bare 13 digits must also pass the
pre-2020 checksum or have a 2020+ birth year), 휴대전화 (`01[016789]-…`),
사업자등록번호 (`XXX-XX-XXXXX` with checksum) and 계좌번호 (values under
`account`/`acct`/`account_no`/`계좌` keys, or 11–14 digit hyphenated formats
that are not phone-shaped).

Names have no pattern, so they are recognised by their JSON key
(case-insensitive; `_`, `-`, `.` and spaces ignored, so `firstName` and
`first_name` match): `name`, `full_name`, `first_name`, `last_name`,
`surname`, `customer`, `customer_name`, `holder`, `holder_name`,
`card_holder`, `recipient(s)`, `sender_name`, `payer`, `payee`,
`beneficiary`, `이름`, `성명`, `성함`, `고객`, `고객명`, `예금주`, `수취인`,
`수령인`, `받는사람`, `보내는사람` and a few more (`NAME_KEYS` in
`crates/ferrika-core/src/codec/pii.rs`). Only exact keys count: `topic_name`,
`connector_name`, `file_name`, `schema_name`, `host_name`, `display_name`
and `username` are left alone. String values (and the strings of an array)
are masked partially: `김민준` → `김*준`, `남궁민수` → `남**수`, `김민` → `김*`,
`John Smith` → `J*** S****`. `crypto audit` reports them as `Person Name`.
Names inside free text (non-JSON payloads) are not detected.

## Limits (what this is and is not)

- The trust anchor is the machine-wide directory. A local administrator can
  replace `policy.pub` (or add to `policy.keys`) and `company.policy` with
  their own pair, or delete them all (Ferrika then runs without a policy). The policy protects against
  honest-but-careless users and makes deviation detectable; it is not DRM.
  Pair it with broker-side ACLs for hard guarantees.
- The audit chain detects edits, insertions and deletions **inside** the log.
  Truncating the end, or deleting the whole file, leaves a valid shorter
  chain; ship the log off the machine with `audit_forward` (or anchor the
  last hash) for that.
  `audit_anchor: os_log` plus `audit verify --anchor` detects that on
  Windows (see above), or ship the log off the machine. Two Ferrika
  processes appending at the same instant can fork the chain.
- `export: forbidden` and the watermark do not stop screen photos, terminal
  scrollback, remote-desktop clipboards, memory dumps or a local
  administrator. They stop the obvious paths (files, clipboard, pipes,
  another cluster, AI tools) and make the rest traceable.
- The policy is read once at start-up; restart Ferrika after installing a new one.

## Threat model and what open source changes

Ferrika's source is public under Apache-2.0. That has one consequence for this
feature that you should plan for:

- **Anyone can build a Ferrika without the policy checks.** The signature
  verification, the read-only and masking gates and the audit log are ordinary
  code in this repository. A user who can compile Rust, or who can download a
  modified binary, can run a build that skips them, trusts a different
  `policy.pub`, or writes no audit entries. Debug builds also honour
  `FERRIKA_POLICY_DIR`, and a build with the `dev-policy-dir` feature does too.
  Open source did not create this risk (a closed binary can be patched as
  well) but it makes it cheaper.
- **The cryptography does not depend on secrecy.** Ed25519 verification needs
  only the public key. Knowing the code does not let anyone forge a policy
  signed by your private key; it lets them run code that does not ask for one.
- **So enforcement comes from controlling which binary runs, not from the
  binary alone.** To make the policy binding, allow only the signed official
  Ferrika binary on managed machines and block everything else that a user
  could build or download:
  - Windows: AppLocker or Windows Defender Application Control (WDAC) rules
    that allow the official executable by publisher or file hash, deployed with
    Intune or Group Policy; no rule for user-writable folders (home directory,
    `target\`, Downloads).
  - macOS: an MDM that allows only the notarized official app and the CLI by
    signing identity; Gatekeeper alone is not enough for command-line tools
    that a user compiles on the machine.
  - Linux: install the official package from an internal repository and use
    an allow-list mechanism such as fapolicyd or IMA so that binaries from home
    directories do not run.
  - Do not give users local administrator rights or a compiler toolchain on
    machines where the policy must hold.
- **Release binaries are not code-signed yet** (see [SECURITY.md](../SECURITY.md)).
  Until they are, an allow rule can pin the SHA-256 hash of a release you
  have reviewed instead of a publisher. Check the hash against a build you
  made yourself from a tagged source tree.
- **Official builds must not enable `dev-policy-dir` or `test-util`.** The
  first lets a user point Ferrika at a policy they signed themselves; the
  second builds policies without a signature, and a build without debug
  assertions refuses to compile it.
- **Detection and hard limits still apply.** Forward audit entries off the
  machine (`audit_forward`) and anchor them (`audit_anchor`) so that a
  modified or missing log is visible. Use broker-side ACLs, quotas and network
  rules for guarantees that must not depend on the client at all.

In short: the policy is a control for honest-but-careless users on managed
machines. It is not a defence against a user who can run software of their own
choosing.
