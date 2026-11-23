#!/usr/bin/env python3
"""End-to-end smoke test of a Ferrika binary against a real Kafka broker.

Unit tests cannot catch regressions that only show against a live broker or a
hostile peer: decoder bombs from a Schema Registry, panics on non-ASCII text,
repeated headers across dump/restore, closed pipes, zstd support, exit codes
and accidental topic auto-creation. This script runs the CLI for each case.

Start a throw-away broker first (KRaft, single node, no auth):

    docker run -d --name ferrika-kafka-test -p 9092:9092 apache/kafka:3.9.0

Run (needs only the Python 3 standard library):

    cargo build --release -p ferrika-cli
    python3 tools/e2e/smoke.py                       # target/release/ferrika, localhost:9092
    python3 tools/e2e/smoke.py path/to/ferrika --broker host:9092 --prefix zz-

Isolation: every topic and consumer group is named <prefix><run id>-..., so
several runs and other topics on the broker do not interfere. Everything the
run created is deleted at the end (topics and consumer groups), even after a
failure. Each ferrika call runs with a 4 GB address-space limit, so a decoder
regression cannot exhaust the machine's memory. Exit code 0 when every check
passes, 1 when any fails.

Use a test broker only: the script creates and deletes topics.
"""
import argparse
import http.server
import json
import os
import re

try:
    import resource  # POSIX only: caps the binary's memory below
except ImportError:
    resource = None
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
parser.add_argument("binary", nargs="?", default=str(ROOT / "target" / "release" / "ferrika"))
parser.add_argument("--broker", default="localhost:9092", help="bootstrap servers (default localhost:9092)")
parser.add_argument("--prefix", default="zz-", help="name prefix of everything the run creates (default zz-)")
ARGS = parser.parse_args()

F = ARGS.binary
BROKER = ARGS.broker
RUN = f"{ARGS.prefix}{uuid.uuid4().hex[:6]}-"  # unique per run
results = []


def limit():
    resource.setrlimit(resource.RLIMIT_AS, (4 << 30, 4 << 30))


# Without `resource` (Windows) the binary runs without the memory cap.
LIMIT = limit if resource else None


def run(args, env=None, timeout=30, stdin=None, broker=True):
    e = dict(os.environ)
    # Every command runs against a throwaway home: the test must never write
    # to the user's own ~/.ferrika (audit log, history, profiles) or to the
    # OS credential store.
    e.update(ISOLATED)
    if env:
        e.update(env)
    cmd = [F] + (["-b", BROKER] if broker else []) + args
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace",
                           timeout=timeout, env=e, input=stdin, preexec_fn=LIMIT)
        return p.returncode, p.stdout + p.stderr
    except subprocess.TimeoutExpired as ex:
        out = (ex.stdout or b"") + (ex.stderr or b"")
        if isinstance(out, bytes):
            out = out.decode(errors="replace")
        return "timeout", out


def check(name, ok, detail=""):
    results.append(bool(ok))
    print(f"{'PASS' if ok else 'FAIL'}  {name}" + (f"   [{detail}]" if detail and not ok else ""), flush=True)


def crashed(rc):
    # POSIX signals, and Windows' access violation / stack overflow / abort.
    return rc in (-6, -11, 134, 137, -9, "timeout", 0xC0000005, 0xC00000FD, 0xC0000409)


def tail(s, n=240):
    return s[-n:].replace("\n", " | ")


def topic(name):
    t = f"{RUN}{name}"
    run(["topics", "create", t])
    return t


def consume(t, n=1, extra=(), timeout_ms=8000):
    """Read from the beginning with a throw-away group; returns (rc, output)."""
    return run(["consume", t, "-b", "-n", str(n), "-t", str(timeout_ms), "-g", f"{t}-g", *extra], timeout=40)


# --- fake Schema Registry returning hostile schemas ---------------------
def avro_bomb():
    # R0 = {a: R1, b: R1}, ... 40 levels: 2^40 decode nodes from zero bytes.
    node = {"type": "record", "name": "R39", "fields": []}
    for i in range(38, -1, -1):
        node = {"type": "record", "name": f"R{i}",
                "fields": [{"name": "a", "type": node}, {"name": "b", "type": f"R{i+1}"}]}
    return json.dumps(node)


SCHEMAS = {
    1: {"schema": avro_bomb()},
    2: {"schema": 'syntax = "proto3";\n' + "message M{" * 20000, "schemaType": "PROTOBUF"},
    3: {"a": 1},
}


class Registry(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        last = self.path.split("?")[0].rstrip("/").split("/")[-1]
        sid = int(last) if last.isdigit() else 0
        body = json.dumps(SCHEMAS.get(sid, {"error_code": 40403})).encode()
        self.send_response(200 if sid in SCHEMAS else 404)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *a):
        pass


srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Registry)
threading.Thread(target=srv.serve_forever, daemon=True).start()
REG = f"http://127.0.0.1:{srv.server_address[1]}"
tmp = Path(tempfile.mkdtemp(prefix="ferrika-smoke-"))
_iso_home = tmp / "isolated-home"
_iso_home.mkdir()
ISOLATED = {
    "HOME": str(_iso_home),
    # On Windows USERPROFILE comes before HOME.
    "USERPROFILE": str(_iso_home),
    "FERRIKA_CONFIG_DIR": str(_iso_home / ".ferrika"),
    "FERRIKA_SECRET_STORE": "file",
}


def scenarios():
    # --- hostile decoder input ------------------------------------------
    for sid, label in ((1, "avro fan-out bomb"), (2, "protobuf deep nesting")):
        t = topic(f"dec{sid}")
        run(["generate", t, "-n", "1", "-r", "100", "--schema-id", str(sid)])
        rc, out = run(["consume", t, "-b", "-n", "1", "-t", "6000", "-g", f"{t}-g", "-D", "-S", REG], timeout=40)
        check(f"{label}: no crash", not crashed(rc), f"rc={rc} {tail(out)}")

    rc, out = run(["schema", "get", "3", "-S", REG])
    check("registry 200 without a schema is an error", rc != 0 and not crashed(rc), f"rc={rc} {tail(out)}")

    # --- panics on multibyte text ---------------------------------------
    t = topic("akia")
    run(["produce", t, "-m", '{"memo":"AKIA가나다라마바사아자"}'])
    rc, out = run(["crypto", "audit", t], timeout=40)
    check("crypto audit: AKIA + Hangul does not panic", not crashed(rc), f"rc={rc} {tail(out)}")

    t = topic("orders-dlq")
    run(["produce", t, "-m", "x", "-H", "x-exception-message:a" + "가나다라마바사아자차카타파하" * 3])
    rc, out = run(["dlq", "inspect", t], timeout=40)
    check("dlq inspect: long Hangul message does not panic", not crashed(rc), f"rc={rc} {tail(out)}")

    # --- masking --------------------------------------------------------
    rc, masked = run(["crypto", "mask"], broker=False, timeout=20, stdin=json.dumps({
        "api-key": "AbCdEf123456", "pwd": "p4ss", "email": "홍길동@회사.kr", "title": "Basic configuration"}) + "\n")
    check("mask: hyphen api-key hidden", "AbCdEf123456" not in masked, masked[:200])
    check("mask: pwd hidden", "p4ss" not in masked, masked[:200])
    check("mask: Hangul email hidden", "홍길동@회사.kr" not in masked, masked[:200])
    check("mask: ordinary 'Basic configuration' kept", "Basic configuration" in masked, masked[:200])

    # --- repeated headers: dump -> restore -> diff ----------------------
    t, t2 = topic("hdr"), topic("hdr2")
    run(["produce", t, "-m", "v", "-H", "a:1", "-H", "a:2", "-H", "b:3"])
    run(["produce", t, "-m", "w", "-k", "k1", "-H", "c:4"])
    rc, out = consume(t, extra=["--show-headers"])
    check("consume shows repeated headers in order", "a=1, a=2, b=3" in out, tail(out))
    dump = tmp / "d.jsonl"
    rc, out = run(["dump", t, "-b", "-o", str(dump)], timeout=40)
    lines = dump.read_text().splitlines() if dump.exists() else []
    check("dump writes both records", rc == 0 and len(lines) == 2, f"rc={rc} lines={len(lines)} {tail(out)}")
    rc, out = run(["restore", t2, "-i", str(dump)], timeout=40)
    check("restore succeeds", rc == 0, f"rc={rc} {tail(out)}")
    rc, out = consume(t2, extra=["--show-headers"])
    check("dump/restore keeps repeated headers", "a=1, a=2, b=3" in out, tail(out))
    rc, out = run(["diff", t, t2, "--json"], timeout=40)
    check("diff: dumped and restored topics are identical", rc == 0 and not differing(out), f"rc={rc} {tail(out)}")
    t3 = topic("hdr3")
    run(["produce", t3, "-m", "changed", "-k", "k1"])
    rc, out = run(["diff", t, t3, "--json"], timeout=40)
    check("diff: different topics are reported as different", differing(out), f"rc={rc} {tail(out)}")

    # --- mirror to the same topic ---------------------------------------
    rc, out = run(["mirror", t, t, "-n", "1"], timeout=20)
    check("mirror to the same topic is refused", rc == 1 and "same topic" in out, f"rc={rc} {tail(out)}")

    # --- mirror --group resumes after its committed offset ----------------
    src, dst = topic("msrc"), topic("mdst")
    for i in range(3):
        run(["produce", src, "-m", f"rec{i}"])
    rc, out = run(["mirror", src, dst, "-g", f"{src}-g", "-b", "-n", "3"], timeout=60)
    check("mirror --group copies the records", rc == 0, f"rc={rc} {tail(out)}")
    run(["produce", src, "-m", "rec3"])
    # -b again: the committed offset wins, so only the new record is copied.
    rc, out = run(["mirror", src, dst, "-g", f"{src}-g", "-b", "-n", "1"], timeout=60)
    _, copied = consume(dst, n=10, timeout_ms=5000)
    got = sorted(re.findall(r"\brec\d\b", copied))
    check("mirror --group resumes after the committed offset",
          rc == 0 and got == ["rec0", "rec1", "rec2", "rec3"], f"rc={rc} got={got} {tail(out)}")

    # --- dlq redrive continues where the last run stopped ----------------
    dlq, out_t = topic("dlq"), topic("dlqout")
    for i in range(5):
        run(["produce", dlq, "-m", f"bad{i}"])
    rc, out = run(["dlq", "redrive", dlq, "-t", out_t, "-n", "3", "--execute", "--json"], timeout=60)
    try:
        first = json.loads(out[out.index("{"):])
    except ValueError:
        first = {}
    nxt = first.get("next_from_offset") or []
    check("dlq redrive sends the first 3 and says where to continue",
          rc == 0 and first.get("published") == 3 and nxt, f"rc={rc} {tail(out)}")
    args = [a for o in nxt for a in ("--from-offset", o)]
    rc, out = run(["dlq", "redrive", dlq, "-t", out_t, "-n", "10", *args, "--execute", "--json"], timeout=60)
    _, copied = consume(out_t, n=10, timeout_ms=5000)
    got = sorted(re.findall(r"\bbad\d\b", copied))
    check("dlq redrive --from-offset sends the rest, nothing twice",
          rc == 0 and got == [f"bad{i}" for i in range(5)], f"rc={rc} got={got} {tail(out)}")

    # --- audit filters ----------------------------------------------------
    rc, out = run(["-f", "json", "audit", "--source", "cli", "-n", "200"], broker=False)
    check("audit --source cli lists this run's changes", rc == 0 and RUN in out, f"rc={rc} {tail(out)}")
    rc, out = run(["-f", "json", "audit", "--source", "gui", "-n", "200"], broker=False)
    check("audit --source gui leaves out CLI entries", rc == 0 and RUN not in out, f"rc={rc} {tail(out)}")

    # --- audit result lines of record writes ------------------------------
    t = topic("audit-result")
    run(["produce", t, "-m", "x"])
    # A broker that is not there: the produce fails after --timeout.
    run(["-b", "127.0.0.1:1", "--timeout", "3", "produce", t, "-m", "y"], broker=False, timeout=60)
    rc, out = run(["-f", "json", "audit", "--source", "cli", "-n", "200"], broker=False)
    try:
        mine = [(e["outcome"], e["ok"]) for e in json.loads(out) if e.get("target") == t]
    except ValueError:
        mine = []
    check("audit: produce writes a result line after its request",
          ("finished without error", True) in mine and ("requested", True) in mine, f"rc={rc} {mine}")
    check("audit: a failed produce is recorded as failed, without the error text",
          ("failed (see the command's output)", False) in mine, f"{mine}")

    # --- empty environment variables mean not set --------------------------
    rc, out = run(["topics", "list"], env={"FERRIKA_PROFILE": ""})
    check("FERRIKA_PROFILE= (empty) is ignored", rc == 0 and "does not exist" not in out, f"rc={rc} {tail(out)}")

    # --- shell completion -------------------------------------------------
    rc, out = run(["completions", "bash"], broker=False)
    check("completions bash names the commands and new options",
          rc == 0 and "mirror" in out and "--from-scratch" in out, f"rc={rc} {tail(out)}")

    # --- consume --offset beyond the end --------------------------------
    t = topic("off")
    run(["produce", t, "-m", "a"])
    run(["produce", t, "-m", "b"])
    rc, out = run(["consume", t, "--offset", "99999", "-t", "3000", "-g", f"{t}-g"], timeout=30)
    check("consume --offset past the end warns and reads 0 records",
          rc == 0 and "past the end" in out and "Consumed 0" in out, f"rc={rc} {tail(out)}")

    # --- offset reset --shift-by with the largest value -----------------
    rc, out = run(["offset", "reset", f"{t}-g", "-t", t, "--shift-by", "9223372036854775807"])
    check("offset reset --shift-by i64::MAX clamps to the end (dry run)",
          rc == 0 and not crashed(rc) and re.search(r"0 \.\. 2\s+┆\s+2\b", out) is not None, f"rc={rc} {tail(out)}")
    rc, out = run(["offset", "reset", f"{t}-g", "-t", t, "--shift-by", "-9223372036854775808"])
    check("offset reset --shift-by i64::MIN clamps to the start (dry run)",
          rc == 0 and re.search(r"0 \.\. 2\s+┆\s+0\b", out) is not None, f"rc={rc} {tail(out)}")

    # --- bad filters and dates ------------------------------------------
    rc, out = run(["consume", t, "--header-filter", "nocolon", "-t", "2000", "-g", f"{t}-g2"])
    check("invalid --header-filter is refused", rc == 1 and "KEY:VALUE" in out, f"rc={rc} {tail(out)}")
    rc, out = run(["topics", "histogram", t, "--from", "2026-02-30"])
    check("--from 2026-02-30 (no such day) is refused with a usage error", rc == 2 and "not a time" in out, f"rc={rc} {tail(out)}")

    # --- dlq redrive onto itself ----------------------------------------
    d = topic("x-dlq")
    run(["produce", d, "-m", "poison"])
    rc, out = run(["dlq", "redrive", d, "-t", d, "--execute"])
    rc2, out2 = consume(d)
    check("dlq redrive -t <itself> is refused and writes nothing", rc == 1 and "itself" in out and out2.count("poison") == 1,
          f"rc={rc} {tail(out)} / {tail(out2)}")

    # --- local ACL deletion only removes the named binding --------------
    home = tmp / "home"
    home.mkdir()
    # USERPROFILE too: on Windows it comes before HOME, and the test must not
    # touch the user's own ~/.ferrika.
    env = {"HOME": str(home), "USERPROFILE": str(home), "FERRIKA_CONFIG_DIR": str(home / ".ferrika"),
           "FERRIKA_SECRET_STORE": "file"}
    run(["acl", "create", "--local", "-p", "User:x", "-o", "Read", "-t", "*"], env=env, broker=False)
    run(["acl", "create", "--local", "-p", "User:x", "-o", "Read", "-t", "billing"], env=env, broker=False)
    run(["acl", "delete", "--local", "-p", "User:x", "-t", "billing", "-f"], env=env, broker=False)
    rc, out = run(["acl", "list", "--local", "--json"], env=env, broker=False)
    try:
        # A fresh local file holds seed bindings (host "*", billing-service...):
        # compare only the bindings of the principal this test created.
        mine = [b for b in json.loads(out) if b.get("principal") == "User:x"]
        names = sorted(b.get("resource_name") for b in mine)
        check("acl --local delete keeps only the wildcard binding of User:x", names == ["*"], f"bindings={names}")
    except ValueError:
        check("acl --local list --json is valid JSON", False, tail(out))

    # --- exit codes and input validation --------------------------------
    p = topic("p")
    rc, out = run(["monitor", "lag", f"{RUN}no-such-group"])
    check("monitor lag on a missing group fails (1)", rc == 1, f"rc={rc} {tail(out)}")
    rc, out = run(["history", "-n", "0"], broker=False)
    check("history -n 0 refused (2)", rc == 2, f"rc={rc}")
    rc, out = run(["quota", "set", "--client-id", "zz", "--key", "producer_byte_rate", "--value", "NaN"])
    check("quota NaN refused (2)", rc == 2, f"rc={rc}")
    rc, out = run(["reassign", "plan", "-t", p, "-b", "1,1,1", "--rf", "2"])
    check("reassign duplicate brokers refused", rc != 0 and not crashed(rc), f"rc={rc} {tail(out)}")
    rc, out = run(["reassign", "plan", "-t", p, "-b", "1", "-p", "100000000"])
    check("reassign absurd partition count refused", rc != 0 and not crashed(rc), f"rc={rc} {tail(out)}")

    # --- no auto-creation of topics -------------------------------------
    ghost = f"{RUN}ghost"
    rc, out = run(["topics", "describe", ghost])
    check("describe missing topic fails", rc == 1, f"rc={rc} {tail(out)}")
    run(["consume", ghost, "-b", "-t", "2000", "-g", f"{ghost}-g"], timeout=30)
    run(["topics", "histogram", ghost], timeout=30)
    time.sleep(1)
    check("missing topic is not auto-created by describe/consume/histogram", ghost not in topic_names(), "topic exists")

    # --- zstd -----------------------------------------------------------
    z = f"{RUN}zstd"
    rc, out = run(["bench", "produce", z, "-c", "zstd", "-n", "5000"], timeout=60)
    check("bench produce -c zstd works", rc == 0 and "libzstd" not in out, f"rc={rc} {tail(out)}")

    # --- closed stdout/stderr pipes -------------------------------------
    po, wo = os.pipe()
    pe, we = os.pipe()
    pr = subprocess.Popen([F, "-b", BROKER, "consume", p, "-b", "-n", "3", "-t", "3000", "-g", f"{p}-pipe"],
                          stdout=wo, stderr=we, preexec_fn=LIMIT, env={**os.environ, **ISOLATED})
    for fd in (po, pe, wo, we):
        os.close(fd)
    try:
        rc = pr.wait(timeout=40)
    except subprocess.TimeoutExpired:
        pr.kill()
        rc = "timeout"
    check("closed stdout+stderr pipes: no crash", not crashed(rc), f"rc={rc}")

    # --- monitor export: /metrics without duplicate series --------------
    port = free_port()
    ex = subprocess.Popen([F, "-b", BROKER, "monitor", "export", "-P", str(port), "-i", "2"],
                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, preexec_fn=LIMIT,
                          env={**os.environ, **ISOLATED})
    try:
        body, code = None, None
        for _ in range(40):
            time.sleep(0.5)
            try:
                with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=3) as r:
                    code, body = r.status, r.read().decode(errors="replace")
                if "ferrika" in body or "kafka" in body:
                    break
            except (urllib.error.URLError, OSError):
                pass
        time.sleep(4)  # let a second scrape happen: series must not pile up
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=3) as r:
            body = r.read().decode(errors="replace")
        series = [l.split(" ")[0] if "{" not in l else l.rsplit(" ", 1)[0] for l in body.splitlines() if l and not l.startswith("#")]
        dup = sorted({s for s in series if series.count(s) > 1})
        check("monitor export /metrics has samples and no duplicate series", code == 200 and series and not dup, f"code={code} dup={dup[:3]}")
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics?x=1", timeout=3) as r:
                q = r.status
        except urllib.error.HTTPError as e:
            q = e.code
        check("monitor export /metrics?x=1 answers 200", q == 200, f"status={q}")
    finally:
        ex.send_signal(signal.SIGTERM)
        try:
            ex.wait(timeout=10)
        except subprocess.TimeoutExpired:
            ex.kill()


def differing(out):
    """True when `diff --json` lists any record whose diff_type is not Identical."""
    return any(t != "Identical" for t in re.findall(r'"diff_type":\s*"(\w+)"', out))


def free_port():
    import socket
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def topic_names():
    rc, out = run(["-f", "json", "topics", "list"], timeout=30)
    try:
        return [t["name"] for t in json.loads(out)]
    except (ValueError, KeyError, TypeError):
        return re.findall(re.escape(ARGS.prefix) + r"[\w.\-]+", out)


def cleanup():
    rc, out = run(["monitor", "lag"], timeout=30)
    groups = sorted(set(re.findall(re.escape(RUN) + r"[\w.\-]+", out)))
    if groups:
        run(["offset", "delete-group", "-y", *groups], timeout=60)
    names = [n for n in topic_names() if n.startswith(RUN)]
    for name in names:
        run(["topics", "delete", name, "-y"])
    # Deletion finishes on the brokers after the request returns, so a list
    # taken at once may still show some topics: give them up to 10 s.
    for _ in range(10):
        left = [n for n in topic_names() if n.startswith(RUN)]
        if not left:
            break
        time.sleep(1)
    print(f"\ncleanup: deleted {len(names)} topics and {len(groups)} consumer groups, topics left: {left}")
    return not left


if not Path(F).exists():
    sys.exit(f"binary not found: {F} (build with `cargo build --release -p ferrika-cli`)")
print(f"ferrika smoke: binary={F} broker={BROKER} run={RUN}\n")
rc, out = run(["topics", "list"], timeout=30)
if rc != 0:
    sys.exit(f"cannot reach the broker at {BROKER}: {tail(out)}\nstart one: docker run -d --name ferrika-kafka-test -p 9092:9092 apache/kafka:3.9.0")
try:
    scenarios()
finally:
    clean = cleanup()
    shutil.rmtree(tmp, ignore_errors=True)
    srv.shutdown()
check("cleanup leaves no topics of this run", clean)
print(f"{sum(results)}/{len(results)} passed")
sys.exit(0 if all(results) else 1)
