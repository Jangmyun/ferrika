# Contributing to Ferrika

Thanks for helping. Issues and pull requests are welcome.

## License and sign-off

Ferrika is licensed under the [Apache License 2.0](LICENSE). By contributing,
you agree that your contribution is licensed under the same terms
(Apache-2.0, section 5).

Every commit needs a [Developer Certificate of Origin](https://developercertificate.org/)
sign-off. It states that you wrote the change or have the right to submit it.
Add it with `git commit -s`, which appends a line like:

```
Signed-off-by: Your Name <you@example.com>
```

There is no separate contributor license agreement.

## Before opening a pull request

Run these checks and make sure they pass before you open the pull request.
CI runs the same commands on the public crates and also checks the DCO
sign-off of every commit:

```bash
cargo fmt --all --check
cargo clippy --all-targets -- -D warnings
cargo test
cargo test -p ferrika-core -p ferrika-governance -p ferrika-ops -p ferrika-mcp -p ferrika-cli
```

Changes that touch a command's behaviour against a real broker should also
pass the end-to-end smoke test (it needs Docker and Python 3 on Linux, macOS
or WSL; use a throw-away broker, never a real cluster):

```bash
docker run -d --name ferrika-kafka-test -p 9092:9092 apache/kafka:3.9.0
cargo build --release -p ferrika-cli
python3 tools/e2e/smoke.py
```

The workspace has five crates, published together on crates.io with one
version: `ferrika-core` (clients, record formats, data model),
`ferrika-governance` (company policy and audit), `ferrika-ops` (operations),
`ferrika-mcp` (the MCP server) and `ferrika-cli` (the `ferrika` command
line). Before 1.0 their API can change in a minor version; a patch version
must not break it, so say in the pull request when a change does. Read
[AGENTS.md](AGENTS.md) for the layout and for one rule you must keep: the policy
gate in `crates/ferrika-cli/src/cli/app.rs` has no wildcard `match` arms, so a
new command must be classified there.

### Build requirements

- Rust 1.88 or newer (the project is tested on a recent stable toolchain).
- CMake and a C compiler, because `rdkafka` builds the bundled `librdkafka`.
- On Windows, MSVC build tools with `rc.exe` on `PATH`.

The `ssl` feature (used for release builds) also needs Perl. See "Building" in the [README](README.md).

Keep changes focused, and say what the change does and why. For anything
large, open an issue first so we can agree on the approach.

## Security issues

Do not open public issues for vulnerabilities. See [SECURITY.md](SECURITY.md).
