## What and why

<!-- What the change does and the problem it solves. Link the issue if there is one. -->

## Checklist

- [ ] Every commit is signed off (`git commit -s`, see CONTRIBUTING.md)
- [ ] `cargo fmt --all --check`, `cargo clippy --all-targets -- -D warnings` and `cargo test` pass
- [ ] New behaviour has a test; behaviour against a broker is in `tools/e2e/smoke.py` where it fits
- [ ] Says so here if it changes a public API (breaking changes wait for the next minor version)
- [ ] No secrets, record contents or personal paths in code, tests or logs
