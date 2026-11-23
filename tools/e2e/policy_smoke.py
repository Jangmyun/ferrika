#!/usr/bin/env python3
"""End-to-end smoke test of the signed company policy, audit log and undo.

Unit tests check each rule on its own; this script checks that a real
`ferrika` binary enforces a signed policy against real clusters: key
generation and signing, `policy show`/`verify`, forbidden categories,
read-only brokers, environments with `copy_down` across two clusters
(mirror and transform --execute), the export gate, enforced masking,
`mcp_write` over stdio JSON-RPC, a tampered policy (strictest mode),
history/undo with an edited journal entry and an edited audit line.

It needs two throw-away clusters: --broker is the "prod" environment and
--broker2 the "dev" one (any Kafka API broker, e.g. Kafka and Redpanda):

    docker run -d --name ferrika-kafka-test -p 9092:9092 apache/kafka:3.9.0
    docker run -d --name ferrika-redpanda-test -p 29092:29092 \\
        docker.redpanda.com/redpandadata/redpanda:v26.3.1 redpanda start --mode dev-container \\
        --smp 1 --memory 1G --overprovisioned --kafka-addr 0.0.0.0:29092 \\
        --advertise-kafka-addr localhost:29092

Run (needs only the Python 3 standard library) with a debug build, which
reads the policy from FERRIKA_POLICY_DIR (a release build needs the
`dev-policy-dir` feature for that, see docs/POLICY.md):

    cargo build -p ferrika-cli
    python3 tools/e2e/policy_smoke.py target/debug/ferrika --broker localhost:9092 --broker2 localhost:29092

The environments are told apart by the exact broker addresses given, so the
two must differ (host:port) and match what the brokers advertise.

Isolation: the keys, the policy directory and FERRIKA_CONFIG_DIR (HOME and
USERPROFILE too) are temporary, so the user's profiles, audit log and
history are not touched. No policy here sets `audit: required`, which would
move the audit log back to the account's real ~/.ferrika. Every topic is
named <prefix><run id>-... and deleted at the end (with the policy removed
first), even after a failure. Exit code 0 when every check passes, 1 when
any fails.

Use test clusters only: the script creates, fills and deletes topics.
"""
import argparse
import json
import os
import queue
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
parser.add_argument("binary", nargs="?", default=str(ROOT / "target" / "debug" / "ferrika"))
parser.add_argument("--broker", default="localhost:9092", help="the 'prod' cluster (default localhost:9092)")
parser.add_argument("--broker2", default="localhost:29092", help="the 'dev' cluster (default localhost:29092)")
parser.add_argument("--prefix", default="zz-", help="name prefix of everything the run creates (default zz-)")
ARGS = parser.parse_args()

F = ARGS.binary
A, B = ARGS.broker, ARGS.broker2
HEX = uuid.uuid4().hex[:6]
RUN = f"{ARGS.prefix}{HEX}-"  # unique per run
SRC = f"{RUN}src"  # on A (prod), holds records with e-mail addresses
N = 3
EMAILS = [f"user{i}-{HEX}@example.com" for i in range(N)]
results = []

tmp = Path(tempfile.mkdtemp(prefix="ferrika-policy-smoke-"))
HOME = tmp / "home"
HOME.mkdir()
CONFIG = HOME / ".ferrika"
KEYS = tmp / "keys"
POLDIR = tmp / "policy"
POLDIR.mkdir()
NOPOL = tmp / "no-policy"  # empty: no policy (set-up and cleanup)
NOPOL.mkdir()
ENV = dict(os.environ)
ENV.update({"HOME": str(HOME), "USERPROFILE": str(HOME), "FERRIKA_CONFIG_DIR": str(CONFIG),
            "FERRIKA_SECRET_STORE": "file", "FERRIKA_POLICY_DIR": str(NOPOL), "NO_COLOR": "1"})
for key in ("FERRIKA_PROFILE", "FERRIKA_CONFIG_PATH", "FERRIKA_AUDIT_FORWARD"):
    ENV.pop(key, None)


def run(args, broker=A, timeout=60, policy=True, env=None):
    """Runs ferrika with -b `broker` (None: no -b); returns (rc, stdout + stderr)."""
    e = dict(ENV, FERRIKA_POLICY_DIR=str(POLDIR if policy else NOPOL), **(env or {}))
    cmd = [F] + (["-b", broker] if broker else []) + args
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace",
                           timeout=timeout, env=e, stdin=subprocess.DEVNULL)
        return p.returncode, p.stdout + p.stderr
    except subprocess.TimeoutExpired as ex:
        return "timeout", str(ex.stdout or "") + str(ex.stderr or "")


def check(name, ok, detail=""):
    results.append(bool(ok))
    print(f"{'PASS' if ok else 'FAIL'}  {name}" + (f"   [{detail}]" if detail and not ok else ""), flush=True)


def tail(s, n=300):
    return str(s)[-n:].replace("\n", " | ")


def parse_json(text):
    try:
        return json.loads(text[text.index("[") if text.lstrip().startswith("[") else text.index("{"):])
    except ValueError:
        return None


def refused(rc, out, *words):
    return rc == 1 and "Refused" in out and all(w in out for w in words)


def install(name, rules):
    """Signs `rules` with the run's key and installs it as the policy."""
    src = tmp / f"{name}.json"
    src.write_text(json.dumps({"issuer": f"E2E {name}", "issued_at": "2026-11-23", "rules": rules}))
    rc, out = run(["policy", "sign", "--key", str(KEYS / "policy-signing.pk8"), "--in", str(src),
                   "--out", str(POLDIR / "company.policy")], broker=None, policy=False)
    if rc != 0:
        raise RuntimeError(f"policy sign {name} failed: rc={rc} {tail(out)}")


def audit_entries(n=500):
    rc, out = run(["-f", "json", "audit", "-n", str(n)], broker=None)
    data = parse_json(out)
    return data if isinstance(data, list) else []


def blocked(target_part, source="cli"):
    """policy.blocked entries of this run whose target contains `target_part`."""
    return [e for e in audit_entries() if e.get("action") == "policy.blocked"
            and target_part in (e.get("target") or "") and e.get("source") == source]


def topic(name, broker=A):
    t = f"{RUN}{name}"
    rc, out = run(["topics", "create", t], broker=broker, policy=False)
    check(f"create {t} on {broker}", rc == 0, f"rc={rc} {tail(out)}")
    return t


def consume(t, broker=A, n=10, extra=(), policy=True):
    return run(["consume", t, "-b", "-n", str(n), "-t", "5000", *extra], broker=broker, policy=policy)


def exists(t, broker=A):
    rc, _ = run(["topics", "describe", t], broker=broker, policy=False)
    return rc == 0


# --- one `ferrika mcp` process over newline-delimited JSON-RPC -------------
class Mcp:
    def __init__(self, args):
        e = dict(ENV, FERRIKA_POLICY_DIR=str(POLDIR))
        self.proc = subprocess.Popen([F, "-b", A, "mcp", *args], stdin=subprocess.PIPE,
                                     stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=e,
                                     text=True, encoding="utf-8", errors="replace", bufsize=1)
        self.lines = queue.Queue()
        self.stderr = []
        self.next_id = 0
        threading.Thread(target=self._read, args=(self.proc.stdout, self.lines.put), daemon=True).start()
        threading.Thread(target=self._read, args=(self.proc.stderr, self.stderr.append), daemon=True).start()

    @staticmethod
    def _read(stream, sink):
        for line in stream:
            sink(line)

    def send(self, message):
        self.proc.stdin.write(json.dumps(message) + "\n")
        self.proc.stdin.flush()

    def request(self, method, params=None, timeout=60):
        self.next_id += 1
        rid = self.next_id
        self.send({"jsonrpc": "2.0", "id": rid, "method": method, "params": params or {}})
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                line = self.lines.get(timeout=max(0.1, deadline - time.time()))
            except queue.Empty:
                break
            try:
                msg = json.loads(line)
            except ValueError:
                continue
            if msg.get("id") == rid and ("result" in msg or "error" in msg):
                return msg
        raise TimeoutError(f"no answer to {method} in {timeout}s; stderr: {tail(''.join(self.stderr))}")

    def start(self):
        """initialize (a client without elicitation) + initialized; returns the tool names."""
        self.request("initialize", {"protocolVersion": "2025-06-18", "capabilities": {},
                                    "clientInfo": {"name": "ferrika-policy-e2e", "version": "1.0"}})
        self.send({"jsonrpc": "2.0", "method": "notifications/initialized"})
        listed = self.request("tools/list")
        return {t.get("name") for t in listed.get("result", {}).get("tools", [])}

    def call(self, name, arguments, timeout=60):
        """tools/call; returns (is_error, raw text)."""
        msg = self.request("tools/call", {"name": name, "arguments": arguments}, timeout)
        if "error" in msg:
            return True, json.dumps(msg["error"])
        result = msg["result"]
        text = "".join(c.get("text", "") for c in result.get("content", []))
        return bool(result.get("isError")), text

    def close(self):
        try:
            self.proc.stdin.close()
            self.proc.wait(timeout=15)
        except (OSError, subprocess.TimeoutExpired):
            self.proc.kill()


WRITE_TOOLS = {"produce_messages", "create_topic", "transform_execute"}


def scenarios():
    # --- set-up without a policy ---------------------------------------
    rc, out = run(["policy", "keygen", "--out", str(KEYS)], broker=None, policy=False)
    check("policy keygen writes the key pair", rc == 0 and (KEYS / "policy.pub").exists()
          and (KEYS / "policy-signing.pk8").exists(), f"rc={rc} {tail(out)}")
    shutil.copy(KEYS / "policy.pub", POLDIR / "policy.pub")
    topic("src")
    for i, email in enumerate(EMAILS):
        run(["produce", SRC, "-k", f"k{i}", "-m", json.dumps({"id": i, "email": email})], policy=False)
    mirror_dst, transform_dst, dev_t = topic("mirror", B), topic("transform", B), topic("dev", B)

    # --- forbid and read_only_brokers ------------------------------------
    install("gate", {"forbid": ["topics.delete"], "read_only_brokers": [B]})
    rc, out = run(["-f", "json", "policy", "show"], broker=None)
    show = parse_json(out) or {}
    if show.get("status") != "verified" or Path(show.get("dir") or "") != POLDIR:
        check("the binary reads FERRIKA_POLICY_DIR (debug build or dev-policy-dir)", False, tail(out))
        return
    check("policy show: verified, rules as signed",
          show.get("strictest") is False and show["rules"].get("forbid") == ["topics.delete"]
          and show["rules"].get("read_only_brokers") == [B], tail(out))
    rc, out = run(["policy", "verify"], broker=None)
    check("policy verify accepts the installed policy", rc == 0 and "Signature valid" in out, f"rc={rc} {tail(out)}")

    rc, out = run(["topics", "delete", SRC, "-y"])
    check("forbid topics.delete: topics delete is refused", refused(rc, out, "topics.delete") and exists(SRC),
          f"rc={rc} {tail(out)}")
    check("the refused delete is audited as policy.blocked", blocked(SRC), "no policy.blocked entry")
    rc, out = run(["produce", dev_t, "-m", "x"], broker=B)
    check("read_only_brokers: produce to the read-only cluster is refused",
          refused(rc, out, B, "read-only"), f"rc={rc} {tail(out)}")
    rc, out = run(["produce", SRC, "-m", json.dumps({"id": 99})])
    check("read_only_brokers: produce to the other cluster is allowed", rc == 0, f"rc={rc} {tail(out)}")
    undo_t = f"{RUN}undo"
    rc, out = run(["topics", "create", undo_t])
    check("topics create is allowed (not forbidden)", rc == 0, f"rc={rc} {tail(out)}")
    rc, out = run(["undo", "-y"])
    check("undo of a topic creation is a topics.delete and is refused",
          refused(rc, out, "topics.delete") and exists(undo_t), f"rc={rc} {tail(out)}")

    # --- environments and copy_down ---------------------------------------
    envs = {"prod": {"tier": 3, "brokers": [A], "forbid": ["topics.purge"]},
            "dev": {"tier": 1, "brokers": [B]}}
    install("env-masked", {"environments": envs, "copy_down": "masked"})
    rc, out = run(["policy", "show"], broker=None)
    check("policy show lists the environments and copy down",
          "prod (tier 3)" in out and "dev (tier 1)" in out and re.search(r"copy down:\s+masked", out), tail(out))
    rc, out = run(["topics", "purge", SRC, "-y"])
    check("per-environment forbid: purge in 'prod' is refused",
          refused(rc, out, "forbidden in the environment 'prod'"), f"rc={rc} {tail(out)}")
    rc, out = run(["topics", "purge", dev_t, "-y"], broker=B)
    check("per-environment forbid: purge in 'dev' is allowed", rc == 0, f"rc={rc} {tail(out)}")
    copy = ["-b", "-n", str(N)]
    rc, out = run(["mirror", SRC, mirror_dst, "--dst-bootstrap-servers", B, *copy])
    check("copy_down masked: mirror prod -> dev without --mask is refused",
          refused(rc, out, "copy_down: masked", "'prod'", "'dev'"), f"rc={rc} {tail(out)}")
    rc, out = run(["transform", SRC, "--to", transform_dst, "--dst-bootstrap-servers", B, *copy, "--execute"])
    check("copy_down masked: transform --execute prod -> dev without --mask is refused",
          refused(rc, out, "copy_down: masked"), f"rc={rc} {tail(out)}")
    rc, out = run(["mirror", SRC, mirror_dst, "--dst-bootstrap-servers", B, *copy, "--mask"], timeout=90)
    check("copy_down masked: mirror --mask is allowed", rc == 0, f"rc={rc} {tail(out)}")
    rc, out = run(["transform", SRC, "--to", transform_dst, "--dst-bootstrap-servers", B, *copy,
                   "--mask", "--set", 'copied="yes"', "--execute"], timeout=90)
    check("copy_down masked: transform --mask ... --execute is allowed", rc == 0, f"rc={rc} {tail(out)}")
    for t, extra in ((mirror_dst, ""), (transform_dst, '"copied":"yes"')):
        rc, out = consume(t, broker=B, policy=False)
        check(f"{t} on dev holds {N} records with the e-mails masked",
              f"Consumed {N}" in out and "@example.com" in out and not any(e in out for e in EMAILS)
              and extra in out.replace(" ", ""), tail(out))
    up = topic("up")
    rc, out = run(["mirror", mirror_dst, up, "--dst-bootstrap-servers", A, "-b", "-n", "1"], broker=B,
                  timeout=90)
    check("copy_down: a copy up (dev -> prod) is not judged", rc == 0, f"rc={rc} {tail(out)}")
    env_t = f"{RUN}env"
    run(["topics", "create", env_t])
    run(["topics", "create", env_t], broker=B)
    outcomes = {e.get("outcome") for e in audit_entries() if e.get("target") == env_t}
    check("audit request entries name the environment",
          {"requested (environment prod)", "requested (environment dev)"} <= outcomes, f"{outcomes}")

    install("env-forbid", {"environments": envs, "copy_down": "forbid"})
    rc, out = run(["mirror", SRC, mirror_dst, "--dst-bootstrap-servers", B, *copy, "--mask"])
    check("copy_down forbid: mirror --mask prod -> dev is refused too",
          refused(rc, out, "copy_down: forbid"), f"rc={rc} {tail(out)}")
    check("the refused copy is audited as policy.blocked", blocked(f"mirror {SRC}"), "no policy.blocked entry")

    # --- export gate -------------------------------------------------------
    install("export", {"export": "forbidden"})
    rc, out = consume(SRC)
    check("export forbidden: consume to a pipe is refused",
          refused(rc, out, "export: forbidden") and EMAILS[0] not in out, f"rc={rc} {tail(out)}")
    saved = tmp / "saved.jsonl"
    rc, out = consume(SRC, extra=["--save", str(saved)])
    check("export forbidden: consume --save is refused and writes no file",
          refused(rc, out, "export: forbidden") and not saved.exists(), f"rc={rc} {tail(out)}")
    rc, out = run(["dump", SRC, "-b", "-o", str(tmp / "dump.jsonl")])
    check("export forbidden: dump is refused", refused(rc, out, "export: forbidden"), f"rc={rc} {tail(out)}")
    check("refused exports are audited with an export: target",
          any((e.get("target") or "").startswith("export:") for e in blocked(SRC)), "no export: entry")

    # --- enforced masking ----------------------------------------------------
    install("masking", {"masking": "enforced"})
    rc, out = consume(SRC)
    check("masking enforced: consume output is masked without --mask-pii",
          rc == 0 and f"Consumed {N + 1}" in out and not any(e in out for e in EMAILS), f"rc={rc} {tail(out)}")
    rc, out = run(["dump", SRC, "-b", "-o", str(tmp / "dump.jsonl")])
    check("masking enforced: dump is refused", refused(rc, out), f"rc={rc} {tail(out)}")

    # --- mcp_write ------------------------------------------------------------
    for rule, confirm in (("forbidden", "auto"), ("confirm_human", "token")):
        install(f"mcp-{rule}", {"mcp_write": rule})
        mcp = Mcp(["--allow-write", "--topics", f"{RUN}*", "--confirm", confirm])
        try:
            names = mcp.start()
            check(f"mcp_write {rule}: tools/list has the read tools and no write tools",
                  "list_topics" in names and not (WRITE_TOOLS & names), f"tools={sorted(names)}")
            err, text = mcp.call("produce_messages", {"topic": SRC, "records": [{"value": "x"}], "mode": "plan"})
            word = "mcp_write: forbidden" if rule == "forbidden" else "mcp_write: confirm_human"
            check(f"mcp_write {rule}: produce_messages is refused", err and word in text, tail(text))
            err, text = mcp.call("list_topics", {"filter": RUN})
            check(f"mcp_write {rule}: read tools still work", not err and SRC in text, tail(text))
        finally:
            mcp.close()
    mine = [e for e in blocked(f"mcp produce_messages {SRC}", source="mcp")
            if "mcp_write: forbidden" in (e.get("outcome") or "")]
    check("the refused MCP write is audited as policy.blocked with the client as actor",
          mine and (mine[0].get("actor") or "").startswith("mcp:ferrika-policy-e2e"), "no policy.blocked entry")

    # --- a tampered policy: strictest mode -------------------------------
    pol = POLDIR / "company.policy"
    signed = pol.read_text(encoding="utf-8")
    pol.write_text(signed.replace("E2E mcp-confirm_human", "E2E mcp-confirm_humaN"), encoding="utf-8")
    rc, out = run(["policy", "verify"], broker=None)
    check("policy verify rejects a tampered policy", rc != 0 and "Signature valid" not in out, f"rc={rc} {tail(out)}")
    rc, out = run(["-f", "json", "policy", "show"], broker=None)
    show = parse_json(out) or {}
    check("policy show: invalid, strictest mode", show.get("status") != "verified" and show.get("strictest") is True,
          tail(out))
    rc, out = run(["produce", SRC, "-m", "x"])
    check("strictest mode: produce is refused", refused(rc, out), f"rc={rc} {tail(out)}")
    rc, out = consume(SRC)
    check("strictest mode: record export (consume to a pipe) is refused", refused(rc, out, "export"),
          f"rc={rc} {tail(out)}")
    check("the invalid policy is audited as policy.invalid",
          any(e.get("action") == "policy.invalid" for e in audit_entries()), "no policy.invalid entry")

    # --- history, undo and the audit chain -----------------------------
    install("history", {"forbid": ["acl.change"]})
    rc, out = run(["-f", "json", "history", "-n", "5"])
    ops = [e for e in (parse_json(out) or []) if e.get("target") == undo_t and e.get("action") == "topics create"]
    undo_id = ops[0]["id"] if ops else None
    check("history lists the topic creation with a matching digest",
          ops and ops[0].get("digest") == "matches", tail(out))
    rc, out = run(["undo", str(undo_id), "-y"])
    check("undo deletes the created topic", rc == 0 and "Undone" in out and not exists(undo_t), f"rc={rc} {tail(out)}")
    rc, out = run(["audit", "verify"], broker=None)
    check("audit verify passes after the change and its undo", rc == 0, f"rc={rc} {tail(out)}")

    edit_t = f"{RUN}edited"
    run(["topics", "create", edit_t])
    journal = CONFIG / "history.jsonl"
    original = journal.read_text(encoding="utf-8")
    lines, edited_id = [], None
    for line in original.splitlines():
        entry = json.loads(line)
        if entry.get("ev") == "begin" and entry.get("target") == edit_t:
            entry["scope"]["bootstrap"] = "elsewhere.example:9092"
            edited_id = entry["id"]
            line = json.dumps(entry, separators=(",", ":"))
        lines.append(line)
    journal.write_text("\n".join(lines) + "\n", encoding="utf-8")
    rc, out = run(["history", "--all", "-n", "5"])
    row = next((l for l in out.splitlines() if edited_id and edited_id in l), "")
    check("history shows the entry with an edited scope as EDITED", "EDITED" in row, tail(out))
    rc, out = run(["undo", str(edited_id), "-y"])
    check("undo of the edited entry is refused", rc != 0 and "edited" in out and exists(edit_t), f"rc={rc} {tail(out)}")
    check("the refused undo is audited as history.tampered",
          any(e.get("action") == "history.tampered" for e in audit_entries()), "no history.tampered entry")
    rc, out = run(["audit", "verify"], broker=None)
    check("audit verify lists the edited history entry (exit 2)", rc == 2 and str(edited_id) in out,
          f"rc={rc} {tail(out)}")
    journal.write_text(original, encoding="utf-8")
    rc, out = run(["audit", "verify"], broker=None)
    check("audit verify passes again with the journal restored", rc == 0, f"rc={rc} {tail(out)}")

    log = CONFIG / "audit.log"
    text = log.read_text(encoding="utf-8")
    needle = f'"target":"{edit_t}","outcome":"requested"'
    check("the audit log is the run's own file", needle in text, "request line not found")
    log.write_text(text.replace(needle, f'"target":"{edit_t}","outcome":"requested!"', 1), encoding="utf-8")
    rc, out = run(["audit", "verify"], broker=None)
    check("audit verify detects a modified audit line (exit 2)", rc == 2 and "✔ Audit log OK" not in out,
          f"rc={rc} {tail(out)}")


def topic_names(broker):
    rc, out = run(["-f", "json", "topics", "list"], broker=broker, policy=False, timeout=30)
    data = parse_json(out)
    try:
        return [t["name"] for t in data]
    except (KeyError, TypeError):
        return re.findall(re.escape(ARGS.prefix) + r"[\w.\-]+", out)


def cleanup():
    left = []
    deleted = 0
    for broker in (A, B):
        names = [n for n in topic_names(broker) if n.startswith(RUN)]
        for name in names:
            run(["topics", "delete", name, "-y"], broker=broker, policy=False)
        deleted += len(names)
        for _ in range(10):
            rest = [n for n in topic_names(broker) if n.startswith(RUN)]
            if not rest:
                break
            time.sleep(1)
        left += rest
    print(f"\ncleanup: deleted {deleted} topics, topics left: {left}")
    return not left


if not Path(F).exists():
    sys.exit(f"binary not found: {F} (build with `cargo build -p ferrika-cli`)")
if A == B:
    sys.exit("--broker and --broker2 must be two different clusters")
print(f"ferrika policy smoke: binary={F} prod={A} dev={B} run={RUN}\n")
for b in (A, B):
    rc, out = run(["topics", "list"], broker=b, policy=False, timeout=30)
    if rc != 0:
        sys.exit(f"cannot reach the broker at {b}: {tail(out)}")
clean = False
started = time.time()
try:
    scenarios()
finally:
    try:
        clean = cleanup()
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
check("cleanup leaves no topics of this run", clean)
print(f"{sum(results)}/{len(results)} passed in {time.time() - started:.0f}s")
sys.exit(0 if all(results) else 1)
