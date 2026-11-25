#!/usr/bin/env python3
"""End-to-end smoke test of `ferrika rabbit` against a real RabbitMQ.

Checks what unit tests cannot: the management API and AMQP as a live broker
answers them. The script builds its own topology through the management API
(an exchange, a work queue that dead-letters into a DLX and a dead-letter
queue), then runs the CLI for profiles, listings, publish, peek, dead-letter
inspection and redrive, topology changes with history and undo (queue,
exchange, bind/unbind), read-only profiles and the audit log.

Start a throw-away broker first (the management plugin is needed):

    docker run -d --name ferrika-rabbit-test -p 15672:15672 -p 5672:5672 \
        -e RABBITMQ_DEFAULT_USER=zz -e RABBITMQ_DEFAULT_PASS=zz-pw rabbitmq:4-management

Run (needs only the Python 3 standard library):

    cargo build -p ferrika-cli
    python3 tools/e2e/rabbit_smoke.py target/debug/ferrika --url http://localhost:15672 \
        --user zz --password zz-pw [--amqp-url amqp://localhost:5672]

Isolation: every exchange, queue and profile is named <prefix><run id>-...,
and the CLI runs with its own FERRIKA_CONFIG_DIR (HOME and USERPROFILE too),
so the user's profiles and audit log are not touched. Everything the run
created is deleted at the end, even after a failure. With
FERRIKA_SECRET_STORE=file (CI) the profile password is kept in the profile
file, which must then be private (mode 0600 on POSIX); otherwise it must go
to the OS credential store and not be in the file. Exit code 0 when every
check passes, 1 when any fails.

Use a test broker only: the script creates, fills and deletes queues.
"""
import argparse
import base64
import json
import os
import shutil
import stat
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
parser.add_argument("binary", nargs="?", default=str(ROOT / "target" / "release" / "ferrika"))
parser.add_argument("--url", default="http://localhost:15672", help="management API (default http://localhost:15672)")
parser.add_argument("--user", default="guest")
parser.add_argument("--password", default="guest")
parser.add_argument("--vhost", default="/")
parser.add_argument("--amqp-url", help="AMQP listener for redrive (default: the management host on 5672)")
parser.add_argument("--prefix", default="zz-", help="name prefix of everything the run creates (default zz-)")
ARGS = parser.parse_args()

F = ARGS.binary
API = ARGS.url.rstrip("/")
VHOST = urllib.parse.quote(ARGS.vhost, safe="")
RUN = f"{ARGS.prefix}{uuid.uuid4().hex[:6]}-"  # unique per run
EX, DLX = f"{RUN}ex", f"{RUN}dlx"
WORK, DLQ = f"{RUN}work", f"{RUN}dlq"
RK = f"{RUN}rk"
PROFILE, RO_PROFILE = f"{RUN}p", f"{RUN}ro"
# Made and removed by the CLI's topology commands (cleaned up as well).
TQ, TQ2, TX = f"{RUN}topo-q", f"{RUN}topo-q2", f"{RUN}topo-x"
# Message bodies: must never show up in the audit log.
BODY = f"body-{uuid.uuid4().hex[:10]}"
FILE_STORE = os.environ.get("FERRIKA_SECRET_STORE", "").lower() == "file"
results = []
created = {"exchanges": [], "queues": []}

tmp = Path(tempfile.mkdtemp(prefix="ferrika-rabbit-smoke-"))
HOME = tmp / "home"
HOME.mkdir()
CONFIG = HOME / ".ferrika"
ENV = dict(os.environ)
ENV.update({"HOME": str(HOME), "USERPROFILE": str(HOME), "FERRIKA_CONFIG_DIR": str(CONFIG),
            "NO_COLOR": "1"})
for key in ("FERRIKA_RABBIT_PROFILE", "FERRIKA_RABBIT_URL", "FERRIKA_RABBIT_USER",
            "FERRIKA_RABBIT_PASSWORD", "FERRIKA_RABBIT_VHOST", "FERRIKA_RABBIT_AMQP_URL", "FERRIKA_PROFILE"):
    ENV.pop(key, None)


def run(args, stdin=None, timeout=60, profile=PROFILE):
    """Runs `ferrika rabbit ...`; returns (rc, stdout, stderr)."""
    cmd = [F, "rabbit"] + (["--rabbit-profile", profile] if profile else []) + args
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace",
                           timeout=timeout, env=ENV, input=stdin)
        return p.returncode, p.stdout, p.stderr
    except subprocess.TimeoutExpired as ex:
        return "timeout", str(ex.stdout or ""), str(ex.stderr or "")


def ferrika(args, timeout=30):
    p = subprocess.run([F] + args, capture_output=True, text=True, encoding="utf-8", errors="replace",
                       timeout=timeout, env=ENV)
    return p.returncode, p.stdout, p.stderr


def check(name, ok, detail=""):
    results.append(bool(ok))
    print(f"{'PASS' if ok else 'FAIL'}  {name}" + (f"   [{detail}]" if detail and not ok else ""), flush=True)


def rj(args, profile=PROFILE, timeout=60):
    """Runs `ferrika -f json rabbit ...`; returns (rc, parsed JSON, output)."""
    cmd = ["-f", "json", "rabbit"] + (["--rabbit-profile", profile] if profile else []) + args
    rc, out, err = ferrika(cmd, timeout=timeout)
    return rc, parse_json(out), out + err


def tail(s, n=240):
    return str(s)[-n:].replace("\n", " | ")


def parse_json(text):
    try:
        return json.loads(text)
    except ValueError:
        return None


# --- management API (set-up, dead-lettering and verification) -------------
AUTH = "Basic " + base64.b64encode(f"{ARGS.user}:{ARGS.password}".encode()).decode()


def api(method, path, body=None, ok_missing=False):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(f"{API}/api/{path}", data=data, method=method,
                                 headers={"Authorization": AUTH, "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            raw = r.read()
            return json.loads(raw) if raw else None
    except urllib.error.HTTPError as e:
        if ok_missing and e.code == 404:
            return None
        raise RuntimeError(f"{method} /api/{path}: HTTP {e.code} {e.read()[:200]!r}") from e


def q(name):
    return urllib.parse.quote(name, safe="")


def declare_exchange(name, kind):
    api("PUT", f"exchanges/{VHOST}/{q(name)}", {"type": kind, "durable": True, "auto_delete": False})
    created["exchanges"].append(name)


def declare_queue(name, arguments=None):
    api("PUT", f"queues/{VHOST}/{q(name)}", {"durable": True, "auto_delete": False, "arguments": arguments or {}})
    created["queues"].append(name)


def bind(exchange, queue, key):
    api("POST", f"bindings/{VHOST}/e/{q(exchange)}/q/{q(queue)}", {"routing_key": key, "arguments": {}})


def get(queue, count, ackmode):
    """Takes up to `count` messages: ack_requeue_true puts them back."""
    return api("POST", f"queues/{VHOST}/{q(queue)}/get",
               {"count": count, "ackmode": ackmode, "encoding": "auto", "truncate": 50000}) or []


def wait_count(queue, n, timeout=10):
    """Messages in `queue` (read with get + requeue, which is exact, unlike
    the queue counters that refresh every few seconds), polled until it is
    `n` or the timeout passes."""
    deadline = time.time() + timeout
    while True:
        msgs = get(queue, 50, "ack_requeue_true")
        if len(msgs) == n or time.time() > deadline:
            return msgs
        time.sleep(0.5)


def setup():
    declare_exchange(EX, "direct")
    declare_exchange(DLX, "fanout")
    declare_queue(WORK, {"x-dead-letter-exchange": DLX})
    declare_queue(DLQ)
    bind(EX, WORK, RK)
    bind(DLX, DLQ, "")


def scenarios():
    amqp = ["--amqp-url", ARGS.amqp_url] if ARGS.amqp_url else []
    vhost = ["--vhost", ARGS.vhost]

    # --- profiles ---------------------------------------------------------
    rc, out, err = run(["profile", "add", PROFILE, "--rabbit-url", API, "--rabbit-user", ARGS.user,
                        "--rabbit-password-stdin", *vhost, *amqp], stdin=ARGS.password + "\n", profile=None)
    check("rabbit profile add", rc == 0, f"rc={rc} {tail(out + err)}")
    rc, out, err = run(["profile", "add", RO_PROFILE, "--read-only", "--rabbit-url", API, "--rabbit-user",
                        ARGS.user, "--rabbit-password-stdin", *vhost, *amqp], stdin=ARGS.password + "\n", profile=None)
    check("rabbit profile add --read-only", rc == 0, f"rc={rc} {tail(out + err)}")
    rc, out, err = run(["profile", "list"], profile=None)
    check("rabbit profile list names both profiles", rc == 0 and PROFILE in out and RO_PROFILE in out,
          f"rc={rc} {tail(out + err)}")
    rc, out, err = ferrika(["-f", "json", "rabbit", "profile", "list"])
    views = {v.get("name"): v for v in (parse_json(out) or [])}
    mine = views.get(PROFILE, {})
    check("rabbit profile list --json: user, read-only flag, no password",
          mine.get("user") == ARGS.user and views.get(RO_PROFILE, {}).get("read_only") is True
          and mine.get("read_only") is False and ARGS.password not in out, f"rc={rc} {tail(out + err)}")
    path = CONFIG / "rabbit-profiles.json"
    text = path.read_text(encoding="utf-8") if path.exists() else ""
    where = mine.get("password")
    if FILE_STORE:
        check("FERRIKA_SECRET_STORE=file: the password is kept in the profile file", where == "file",
              f"password={where}")
        if os.name == "posix":
            mode = stat.S_IMODE(path.stat().st_mode) if path.exists() else None
            check("rabbit-profiles.json is private (0600)", mode == 0o600, f"mode={oct(mode or 0)}")
        rc, out, err = run(["profile", "add", f"{RUN}hint", "--rabbit-url", API, "--rabbit-user", ARGS.user,
                            "--rabbit-password-stdin"], stdin=ARGS.password + "\n", profile=None)
        check("profile add says where the password is kept",
              rc == 0 and "password is kept in" in err and "rabbit-profiles.json" in err, f"rc={rc} {tail(out + err)}")
        run(["profile", "remove", f"{RUN}hint"], profile=None)
    elif where == "os-credential-store":
        check("password in the OS credential store, not in rabbit-profiles.json",
              path.exists() and ARGS.password not in text, "password found in the file")
    else:
        check("password in the OS credential store (or set FERRIKA_SECRET_STORE=file)", False, f"password={where}")

    # --- listings (JSON output) -------------------------------------------
    rc, out, err = ferrika(["-f", "json", "rabbit", "--rabbit-profile", PROFILE, "queues"])
    queues = {x.get("name"): x for x in (parse_json(out) or [])}
    check("queues --json lists the work and dead-letter queues",
          rc == 0 and WORK in queues and DLQ in queues, f"rc={rc} {tail(out + err)}")
    check("queues --json shows the work queue's dead-letter exchange",
          queues.get(WORK, {}).get("arguments", {}).get("x-dead-letter-exchange") == DLX, tail(queues.get(WORK)))
    rc, out, err = ferrika(["-f", "json", "rabbit", "--rabbit-profile", PROFILE, "exchanges"])
    exchanges = {x.get("name"): x for x in (parse_json(out) or [])}
    check("exchanges --json lists the run's exchanges with their types",
          rc == 0 and exchanges.get(EX, {}).get("type", exchanges.get(EX, {}).get("kind")) == "direct"
          and DLX in exchanges, f"rc={rc} {tail(out + err)}")
    rc, out, err = ferrika(["-f", "json", "rabbit", "--rabbit-profile", PROFILE, "bindings"])
    bindings = [(b.get("source"), b.get("destination"), b.get("routing_key")) for b in (parse_json(out) or [])]
    check("bindings --json lists exchange -> queue with the routing key",
          rc == 0 and (EX, WORK, RK) in bindings and (DLX, DLQ, "") in bindings, f"rc={rc} {tail(out + err)}")

    # --- publish ----------------------------------------------------------
    ok = True
    for i in range(3):
        rc, out, err = run(["publish", EX, "-k", RK, "-m", f"{BODY}-{i}", "-H", f"seq:{i}"])
        ok = ok and rc == 0 and "Published" in out
    check("publish 3 routed messages", ok, f"rc={rc} {tail(out + err)}")
    check("published messages arrive in the work queue", len(wait_count(WORK, 3)) == 3)
    rc, out, err = run(["publish", EX, "-k", f"{RUN}nowhere", "-m", f"{BODY}-lost"])
    check("publish with no matching binding fails and says not routed",
          rc not in (0, "timeout") and "not routed" in err, f"rc={rc} {tail(out + err)}")

    # --- peek -------------------------------------------------------------
    rc, out, err = ferrika(["-f", "json", "rabbit", "--rabbit-profile", PROFILE, "peek", WORK, "-n", "3"])
    shown = parse_json(out) or []
    check("peek --json shows the 3 messages",
          rc == 0 and len(shown) == 3 and all(BODY in (m.get("payload") or "") for m in shown), f"rc={rc} {tail(out + err)}")
    check("peek warns that the messages were requeued as redelivered", "redelivered" in err, tail(err))
    msgs = wait_count(WORK, 3)
    check("peek leaves the messages in the queue, marked redelivered",
          len(msgs) == 3 and all(m.get("redelivered") for m in msgs), f"got {len(msgs)}")

    # --- dead-letter the 3 messages (reject without requeue) --------------
    rejected = get(WORK, 3, "reject_requeue_false")
    dead = wait_count(DLQ, 3)
    check("3 messages rejected into the dead-letter queue", len(rejected) == 3 and len(dead) == 3,
          f"rejected={len(rejected)} dlq={len(dead)}")

    # --- dlq inspect ------------------------------------------------------
    rc, out, err = ferrika(["-f", "json", "rabbit", "--rabbit-profile", PROFILE, "dlq", "inspect", DLQ])
    inspection = parse_json(out) or {}
    groups = inspection.get("groups") or []
    check("dlq inspect groups the 3 messages by origin and reason",
          rc == 0 and len(groups) == 1 and groups[0].get("queue") == WORK and groups[0].get("reason") == "rejected"
          and groups[0].get("messages") == 3 and groups[0].get("exchange") == EX, f"rc={rc} {tail(out + err)}")

    # --- dlq redrive: preview, then execute -------------------------------
    rc, out, err = ferrika(["-f", "json", "rabbit", "--rabbit-profile", PROFILE, "dlq", "redrive", DLQ])
    preview = parse_json(out) or {}
    check("dlq redrive preview plans 3 moves back to the original exchange",
          rc == 0 and preview.get("dry_run") is True and len(preview.get("messages") or []) == 3
          and preview.get("unmovable") == 0, f"rc={rc} {tail(out + err)}")
    check("dlq redrive preview moves nothing", len(wait_count(DLQ, 3)) == 3 and len(get(WORK, 50, "ack_requeue_true")) == 0)
    rc, out, err = ferrika(["-f", "json", "rabbit", "--rabbit-profile", PROFILE, "dlq", "redrive", DLQ,
                            "--execute", "--yes"], timeout=90)
    report = parse_json(out) or {}
    check("dlq redrive --execute --yes moves 3 messages", rc == 0 and report.get("moved") == 3,
          f"rc={rc} {tail(out + err)}")
    back = wait_count(WORK, 3)
    left = wait_count(DLQ, 0)
    check("redriven messages are back in the work queue and the DLQ is empty",
          len(back) == 3 and len(left) == 0, f"work={len(back)} dlq={len(left)}")
    headers = [(m.get("properties") or {}).get("headers") or {} for m in back]
    check("redriven messages keep x-death and get x-ferrika-redriven-from",
          len(headers) == 3 and all(h.get("x-death") and "x-ferrika-redriven-from" in h for h in headers),
          tail(headers))
    check("redriven messages keep their bodies", sorted(m.get("payload") for m in back) == [f"{BODY}-{i}" for i in range(3)],
          tail([m.get("payload") for m in back]))

    # --- read-only profile ------------------------------------------------
    for label, args in (("publish", ["publish", EX, "-k", RK, "-m", f"{BODY}-ro"]),
                        ("peek", ["peek", WORK]),
                        ("dlq inspect", ["dlq", "inspect", DLQ]),
                        ("dlq redrive --execute", ["dlq", "redrive", DLQ, "--execute", "--yes"])):
        rc, out, err = run(args, profile=RO_PROFILE)
        check(f"read-only profile refuses {label}", rc not in (0, "timeout") and "read-only" in err,
              f"rc={rc} {tail(out + err)}")
    rc, out, err = run(["queues"], profile=RO_PROFILE)
    check("read-only profile still lists queues", rc == 0 and WORK in out, f"rc={rc} {tail(out + err)}")
    check("read-only refusals changed nothing", len(wait_count(WORK, 3)) == 3)

    # --- audit log --------------------------------------------------------
    rc, out, err = ferrika(["-f", "json", "audit", "--source", "cli", "-n", "500"])
    entries = parse_json(out) or []
    actions = {e.get("action") for e in entries if RUN in (e.get("target") or "")}
    want = {"rabbit publish", "rabbit peek", "rabbit dlq inspect", "rabbit dlq redrive", "rabbit dlq redrive --execute"}
    check("audit log has the rabbit writes of this run", rc == 0 and want <= actions,
          f"rc={rc} missing={sorted(want - actions)} {tail(out + err)}")
    check("audit log records the read-only refusals",
          any(not e.get("ok") and RUN in (e.get("target") or "") for e in entries), tail(out))
    leaked = [p.name for p in CONFIG.rglob("*") if p.is_file() and BODY in p.read_text(encoding="utf-8", errors="replace")]
    check("no message body in the audit log or any config file", rc == 0 and BODY not in out and not leaked,
          f"files={leaked}")

    topology()

    # --- profile remove ---------------------------------------------------
    for name in (PROFILE, RO_PROFILE):
        rc, out, err = run(["profile", "remove", name], profile=None)
        check(f"rabbit profile remove {name}", rc == 0, f"rc={rc} {tail(out + err)}")


def op_of(action, name):
    """The newest recorded operation `action` on `name` (from `rabbit history`)."""
    rc, ops, _ = rj(["history", "-n", "200"])
    for op in ops or []:
        if op.get("action") == action and name in (op.get("target") or ""):
            return op
    return {}


def queue_info(name):
    return api("GET", f"queues/{VHOST}/{q(name)}", ok_missing=True)


def wait_stats(name, n, timeout=20):
    """Waits until the broker's statistics (refreshed every few seconds)
    report `n` messages in queue `name`; the CLI's counts come from them."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if (queue_info(name) or {}).get("messages") == n:
            return True
        time.sleep(1)
    return False


def has_binding(source, queue, key):
    found = api("GET", f"bindings/{VHOST}/e/{q(source)}/q/{q(queue)}", ok_missing=True) or []
    return any(b.get("routing_key") == key for b in found)


def topology():
    """Queue, exchange and binding changes, their history and undo."""
    created["queues"] += [TQ, TQ2]
    created["exchanges"].append(TX)

    # declare -> history -> undo
    rc, out, text = rj(["queue", "declare", TQ, "--type", "quorum"])
    info = queue_info(TQ) or {}
    check("queue declare --type quorum", rc == 0 and (out or {}).get("declared") is True
          and info.get("type") == "quorum", f"rc={rc} {tail(text)}")
    op = op_of("rabbit queue declare", TQ)
    check("rabbit history lists the declaration as undoable if empty",
          op.get("reversibility") == "conditional" and op.get("result") == "ok"
          and op.get("digest") == "matches", tail(op))
    rc, out, text = rj(["queue", "declare", TQ])
    check("declaring an existing queue is refused", rc not in (0, "timeout") and "already exists" in text,
          f"rc={rc} {tail(text)}")
    rc, out, text = rj(["undo", op.get("id", "none"), "--yes"])
    check("undo of the declaration deletes the queue", rc == 0 and queue_info(TQ) is None, f"rc={rc} {tail(text)}")
    rc, out, text = rj(["history", "show", op.get("id", "none")])
    check("history show: the declaration is undone", rc == 0 and (out or {}).get("undone_by"), f"rc={rc} {tail(text)}")

    # an exchange, a binding and a message; delete the queue, undo restores it empty with the binding
    rc, out, text = rj(["exchange", "declare", TX, "--type", "topic"])
    check("exchange declare --type topic", rc == 0 and (api("GET", f"exchanges/{VHOST}/{q(TX)}") or {}).get("type")
          == "topic", f"rc={rc} {tail(text)}")
    rc1, _, t1 = rj(["queue", "declare", TQ, "--arg", "x-max-length=100"])
    rc2, out, t2 = rj(["bind", TX, TQ, "-k", "topo.*"])
    check("queue declare --arg and bind", rc1 == 0 and rc2 == 0 and has_binding(TX, TQ, "topo.*")
          and (queue_info(TQ) or {}).get("arguments", {}).get("x-max-length") == 100, tail(t1 + t2))
    rc, _, text = run(["publish", TX, "-k", "topo.a", "-m", f"{BODY}-topo"])
    check("publish into the bound queue", rc == 0 and len(wait_count(TQ, 1)) == 1 and wait_stats(TQ, 1),
          f"rc={rc} {tail(text)}")
    rc, out, text = rj(["queue", "delete", TQ, "--yes"])
    check("queue delete --yes drops the queue and its binding",
          rc == 0 and (out or {}).get("messages") == 1 and queue_info(TQ) is None and not has_binding(TX, TQ, "topo.*"),
          f"rc={rc} {tail(text)}")
    op = op_of("rabbit queue delete", TQ)
    check("history: the deletion is partly undoable", op.get("reversibility") == "partial", tail(op))
    rc, out, text = rj(["undo", op.get("id", "none"), "--dry-run"])
    steps = ((out or {}).get("steps") or {}).get("steps") or []
    check("undo --dry-run plans declare then bind, changing nothing",
          rc == 0 and [s.get("step") for s in steps] == ["declare_queue", "bind"] and queue_info(TQ) is None,
          f"rc={rc} {tail(text)}")
    rc, out, text = rj(["undo", op.get("id", "none"), "--yes"])
    info = queue_info(TQ) or {}
    check("undo of the deletion declares the queue again, empty, with its binding and arguments",
          rc == 0 and info.get("arguments", {}).get("x-max-length") == 100 and has_binding(TX, TQ, "topo.*")
          and len(wait_count(TQ, 0)) == 0, f"rc={rc} {tail(text)}")

    # unbind -> undo
    rc, out, text = rj(["unbind", TX, TQ, "-k", "topo.*"])
    gone = not has_binding(TX, TQ, "topo.*")
    op = op_of("rabbit unbind", TQ)
    rc2, _, text2 = rj(["undo", op.get("id", "none"), "--yes"])
    check("unbind, then undo binds again", rc == 0 and gone and rc2 == 0 and has_binding(TX, TQ, "topo.*"),
          f"rc={rc}/{rc2} {tail(text + text2)}")

    # undo of a declaration refuses a queue holding messages; purge cannot be undone
    rj(["queue", "declare", TQ2])
    declared = op_of("rabbit queue declare", TQ2)
    run(["publish", "amq.default", "-k", TQ2, "-m", f"{BODY}-q2"])
    wait_count(TQ2, 1)
    wait_stats(TQ2, 1)
    rc, out, text = rj(["undo", declared.get("id", "none"), "--yes", "--force"])
    check("undo of a declaration refuses a queue with messages, even with --force",
          rc not in (0, "timeout") and "1 message(s)" in text and queue_info(TQ2) is not None, f"rc={rc} {tail(text)}")
    rc, out, text = rj(["queue", "purge", TQ2, "--yes"])
    check("queue purge --yes", rc == 0 and (out or {}).get("purged") == 1 and len(wait_count(TQ2, 0)) == 0,
          f"rc={rc} {tail(text)}")
    purged = op_of("rabbit queue purge", TQ2)
    rc, out, text = rj(["undo", purged.get("id", "none"), "--yes"])
    check("a purge is recorded but not undoable", purged.get("reversibility") == "irreversible"
          and rc not in (0, "timeout"), f"rc={rc} {tail(text)}")
    wait_stats(TQ2, 0)  # undo reads the broker's statistics, which lag
    rc, out, text = rj(["undo", declared.get("id", "none"), "--yes"])
    check("once empty, undo of the declaration deletes the queue", rc == 0 and queue_info(TQ2) is None,
          f"rc={rc} {tail(text)}")

    # delete the exchange (removes the binding), undo restores both
    rc, out, text = rj(["exchange", "delete", TX, "--yes"])
    check("exchange delete --yes removes its binding",
          rc == 0 and (out or {}).get("bindings") == 1 and not has_binding(TX, TQ, "topo.*"), f"rc={rc} {tail(text)}")
    op = op_of("rabbit exchange delete", TX)
    rc, out, text = rj(["undo", op.get("id", "none"), "--yes"])
    check("undo of the exchange deletion restores it and its binding",
          rc == 0 and api("GET", f"exchanges/{VHOST}/{q(TX)}", ok_missing=True) and has_binding(TX, TQ, "topo.*"),
          f"rc={rc} {tail(text)}")

    # Kafka's undo points to rabbit undo; read-only profiles refuse
    rc, out, err = ferrika(["undo", op.get("id", "none"), "--yes"])
    check("ferrika undo of a RabbitMQ operation points to rabbit undo",
          rc not in (0, "timeout") and "ferrika rabbit undo" in err, f"rc={rc} {tail(out + err)}")
    for label, args in (("queue declare", ["queue", "declare", f"{RUN}topo-ro"]),
                        ("queue delete", ["queue", "delete", TQ, "--yes"]),
                        ("bind", ["bind", TX, TQ, "-k", "ro"]),
                        ("undo", ["undo", op.get("id", "none"), "--yes"])):
        rc, out, err = run(args, profile=RO_PROFILE)
        check(f"read-only profile refuses {label}", rc not in (0, "timeout") and "read-only" in err,
              f"rc={rc} {tail(out + err)}")
    check("read-only refusals left the topology alone",
          queue_info(TQ) is not None and queue_info(f"{RUN}topo-ro") is None and not has_binding(TX, TQ, "ro"))

    rc, out, err = ferrika(["-f", "json", "audit", "--source", "cli", "-n", "500"])
    actions = {e.get("action") for e in (parse_json(out) or []) if RUN in (e.get("target") or "")}
    want = {"rabbit queue declare", "rabbit queue delete", "rabbit queue purge", "rabbit exchange declare",
            "rabbit exchange delete", "rabbit bind", "rabbit unbind", "rabbit undo"}
    check("audit log has the topology changes and undos", rc == 0 and want <= actions,
          f"missing={sorted(want - actions)}")


def cleanup():
    errors = []
    for name in created["queues"]:
        try:
            api("DELETE", f"queues/{VHOST}/{q(name)}", ok_missing=True)
        except RuntimeError as e:
            errors.append(str(e))
    for name in created["exchanges"]:
        try:
            api("DELETE", f"exchanges/{VHOST}/{q(name)}", ok_missing=True)
        except RuntimeError as e:
            errors.append(str(e))
    # Profiles left by a failed run (their OS credential store entries too).
    for name in (PROFILE, RO_PROFILE, f"{RUN}hint"):
        run(["profile", "remove", name], profile=None)
    left = [n for n in created["queues"] if api("GET", f"queues/{VHOST}/{q(n)}", ok_missing=True)]
    left += [n for n in created["exchanges"] if api("GET", f"exchanges/{VHOST}/{q(n)}", ok_missing=True)]
    print(f"\ncleanup: deleted {len(created['queues'])} queues and {len(created['exchanges'])} exchanges, left: {left}"
          + (f", errors: {errors}" if errors else ""))
    return not left


if not Path(F).exists():
    sys.exit(f"binary not found: {F} (build with `cargo build -p ferrika-cli`)")
print(f"ferrika rabbit smoke: binary={F} url={API} run={RUN} secret store={'file' if FILE_STORE else 'os'}\n")
try:
    api("GET", "overview")
except (RuntimeError, urllib.error.URLError, OSError) as e:
    sys.exit(f"cannot reach the RabbitMQ management API at {API}: {e}")
clean = False
try:
    setup()
    scenarios()
finally:
    try:
        clean = cleanup()
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
check("cleanup leaves no queues or exchanges of this run", clean)
print(f"{sum(results)}/{len(results)} passed")
sys.exit(0 if all(results) else 1)
