"""Compare the Ferrika CLI with Kafbat UI, AKHQ, Redpanda Console and Kafdrop on the same
broker and the same data.

`measure` needs Windows (process memory is read through the Win32 API); `report`
runs anywhere. The broker and the web servers run in Docker on WSL2 from
tools/bench/compose.yml. Ferrika is measured in two places:

  ferrika_net   a Linux build in a container on the broker's Docker network,
                the same position as the web servers (see "Linux build" below)
  ferrika_host  the Windows exe, reaching the broker through WSL2's port
                forwarding, as a client on another machine would

    python tools/bench/run.py prepare   # create the topic, write the records and the needle
    python tools/bench/run.py measure   # run the scenarios, write docs/bench/
    python tools/bench/run.py report    # rewrite RESULTS.md from results.json

Set FERRIKA_EXE to the `ferrika` CLI binary (default target/release/ferrika.exe).

Scenarios:
  search   Find one record whose key and value hold a unique string, scanning
           the topic from the beginning. The record is the last one written,
           at the end of its partition, so a tool reads that whole partition
           and, reading partitions side by side, most of the others before it
           finds it. Kafbat UI: messages/v2 with stringFilter (key or value
           contains). AKHQ: data/search with searchByValue. Redpanda Console:
           ListMessages with a JavaScript filter. Kafdrop: its search page.
           Ferrika: `consume -b -G` (regex over the value), stopping at the
           first match.
  parallel Three searches at once, as three people would run them: three
           requests to one server, or three Ferrika processes.
  read     Reading the whole topic without a filter (`ferrika bench consume`)
           from each Ferrika position, to show what the transport costs.
  startup  Web servers: container start until the API answers with the
           cluster. (Not measured for the CLI, which has no server to start.)
  idle     Web servers: container memory with no work (usage minus inactive
           page cache, as `docker stats` shows it).
  topics   Warm topic list: one API call to a connected server vs a whole
           `ferrika topics list` run (process start and connection included).

Browsers that show the web UIs are not measured; their memory comes on top.
Versions of every compared product are recorded in results.json.

Linux build (once): a `rust:1-bookworm` container named ferrika-linux-build,
attached to the bench network, with the source in /src and `time` installed,
and `cargo build --release -p ferrika-cli` run in /src.
"""

import argparse
import base64
import ctypes
import datetime
import json
import os
import platform
import re
import shlex
import struct
import statistics
import subprocess
import sys
import threading
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "docs" / "bench"
EXE = Path(os.environ.get("FERRIKA_EXE", ROOT / "target" / "release" / "ferrika.exe"))
BOOTSTRAP = os.environ.get("FERRIKA_BENCH_BOOTSTRAP", "localhost:39092")
DOCKER = shlex.split(os.environ.get("FERRIKA_BENCH_DOCKER", "wsl -e docker"))
LINUX = "ferrika-linux-build"
LINUX_EXE = "/src/target/release/ferrika"
NET_BOOTSTRAP = "kafka-bench:29092"
CLUSTER = "bench"
TOPIC = "bench-10m"
RECORDS = 10_000_000
NEEDLE = "needle-7f3a91c2"
REPEAT = 3
USERS = 3

WEB = {
    "kafbat": {"name": "Kafbat UI", "url": "http://localhost:38080", "container": "kafbat-ui-bench",
               "topics": f"/api/clusters/{CLUSTER}/topics"},
    "akhq": {"name": "AKHQ", "url": "http://localhost:38081", "container": "akhq-bench",
             "topics": f"/api/{CLUSTER}/topic?show=ALL"},
    "redpanda": {"name": "Redpanda Console", "url": "http://localhost:38082",
                 "container": "redpanda-console-bench", "topics": "/api/topics"},
    "kafdrop": {"name": "Kafdrop", "url": "http://localhost:38083", "container": "kafdrop-bench",
                "topics": "/topic"},
}


# --- Win32 process memory -------------------------------------------------

class PROCESS_MEMORY_COUNTERS_EX(ctypes.Structure):
    _fields_ = [
        ("cb", ctypes.c_uint32),
        ("PageFaultCount", ctypes.c_uint32),
        ("PeakWorkingSetSize", ctypes.c_size_t),
        ("WorkingSetSize", ctypes.c_size_t),
        ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
        ("QuotaPagedPoolUsage", ctypes.c_size_t),
        ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
        ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
        ("PagefileUsage", ctypes.c_size_t),
        ("PeakPagefileUsage", ctypes.c_size_t),
        ("PrivateUsage", ctypes.c_size_t),
    ]




psapi = ctypes.WinDLL("psapi", use_last_error=True) if sys.platform == "win32" else None


def process_memory(proc):
    """(working set, peak working set, private bytes) in bytes, or None."""
    counters = PROCESS_MEMORY_COUNTERS_EX()
    counters.cb = ctypes.sizeof(counters)
    handle = ctypes.c_void_p(int(proc._handle))
    if not psapi.GetProcessMemoryInfo(handle, ctypes.byref(counters), counters.cb):
        return None
    return counters.WorkingSetSize, counters.PeakWorkingSetSize, counters.PrivateUsage


class PeakSampler:
    """Polls a value in the background and keeps the largest one seen."""

    def __init__(self, read, interval):
        self.read, self.interval = read, interval
        self.peak = 0
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _run(self):
        while not self._stop.is_set():
            value = self.read()
            if value:
                self.peak = max(self.peak, value)
            self._stop.wait(self.interval)

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, *exc):
        self._stop.set()
        self._thread.join()


# --- helpers ----------------------------------------------------------------

def mib(n):
    return round(n / 1024 / 1024, 1) if n is not None else None


def median(values):
    values = [v for v in values if v is not None]
    return round(statistics.median(values), 3) if values else None


def docker(*args, check=True):
    return subprocess.run(DOCKER + list(args), capture_output=True, text=True, encoding="utf-8",
                          errors="replace", check=check).stdout


def container_memory(name):
    """Container memory as `docker stats` reports it: usage minus inactive file cache."""
    out = docker("exec", name, "sh", "-c",
                 "cat /sys/fs/cgroup/memory.current; grep '^inactive_file ' /sys/fs/cgroup/memory.stat",
                 check=False).split()
    try:
        return int(out[0]) - int(out[2])
    except (IndexError, ValueError):
        return 0


def http_json(url, timeout=30):
    request = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(request, timeout=timeout) as r:
        return json.loads(r.read())


def ferrika(*args):
    return subprocess.run([str(EXE), "-b", BOOTSTRAP, *args], capture_output=True, text=True,
                          stdin=subprocess.DEVNULL)


def timed_ferrika(*args):
    """Run the Windows Ferrika CLI; return (seconds, peak working set, stdout)."""
    start = time.perf_counter()
    proc = subprocess.Popen([str(EXE), "-b", BOOTSTRAP, *args], stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL, text=True,
                            encoding="utf-8", errors="replace")
    with PeakSampler(lambda: (process_memory(proc) or (0, 0, 0))[1], 0.05) as sampler:
        out, _ = proc.communicate()
    return time.perf_counter() - start, sampler.peak, out


def linux_ferrika(*args):
    """Run the Linux Ferrika CLI in its container, timed there.

    Returns (start epoch, seconds, max RSS bytes, stdout)."""
    quoted = " ".join(shlex.quote(a) for a in args)
    script = (f"S=$(date +%s.%N); /usr/bin/time -f %M -o /tmp/rss.$$ {LINUX_EXE} -b {NET_BOOTSTRAP} "
              f"{quoted} > /tmp/out.$$ 2>&1; E=$(date +%s.%N); echo \"@@ $S $E $(cat /tmp/rss.$$)\"; "
              "cat /tmp/out.$$; rm -f /tmp/out.$$ /tmp/rss.$$")
    out = docker("exec", LINUX, "sh", "-c", script)
    marker = next(line for line in out.splitlines() if line.startswith("@@ "))
    _, start, end, rss_kib = marker.split()
    return float(start), float(end) - float(start), int(rss_kib) * 1024, out


# --- searches ----------------------------------------------------------------

def stream(url, on_event):
    """Read a server-sent event stream; on_event(event, data) returns True to stop."""
    event = None
    with urllib.request.urlopen(url, timeout=3600) as r:
        for raw in r:
            line = raw.decode("utf-8", "replace").strip()
            if line.startswith("event:"):
                event = line[6:].strip()
            elif line.startswith("data:"):
                if on_event(event, json.loads(line[5:])):
                    return
            elif not line:
                event = None


def kafbat_search():
    """(seconds to the match or None, seconds to the end of the search)."""
    url = (f"{WEB['kafbat']['url']}/api/clusters/{CLUSTER}/topics/{TOPIC}/messages/v2"
           f"?mode=EARLIEST&limit=1&stringFilter={NEEDLE}")
    start, found = time.perf_counter(), [None]

    def on_event(_, data):
        if data.get("type") == "MESSAGE" and NEEDLE in json.dumps(data.get("message", {})):
            found[0] = found[0] or time.perf_counter() - start
        return data.get("type") == "DONE"

    stream(url, on_event)
    return found[0], time.perf_counter() - start


def akhq_search():
    url = (f"{WEB['akhq']['url']}/api/{CLUSTER}/topic/{TOPIC}/data/search"
           f"?sort=OLDEST&searchByValue={NEEDLE}")
    start, found = time.perf_counter(), [None]

    def on_event(event, data):
        for record in data.get("records") or []:
            if record.get("key") == NEEDLE:
                found[0] = found[0] or time.perf_counter() - start
        return event == "searchEnd"

    stream(url, on_event)
    return found[0], time.perf_counter() - start


def redpanda_search():
    """ConsoleService/ListMessages over Connect RPC (enveloped JSON frames) with a JS filter."""
    code = base64.b64encode(f'return value.order_id == "{NEEDLE}";'.encode()).decode()
    body = json.dumps({"topic": TOPIC, "startOffset": "-2", "partitionId": -1, "maxResults": 1,
                       "filterInterpreterCode": code}).encode()
    request = urllib.request.Request(
        f"{WEB['redpanda']['url']}/redpanda.api.console.v1alpha1.ConsoleService/ListMessages",
        data=struct.pack(">BI", 0, len(body)) + body,
        headers={"Content-Type": "application/connect+json", "Connect-Protocol-Version": "1"})
    start, found = time.perf_counter(), None
    with urllib.request.urlopen(request, timeout=3600) as r:
        while True:
            head = r.read(5)
            if len(head) < 5:
                break
            flags, length = struct.unpack(">BI", head)
            message = json.loads(r.read(length) or b"{}")
            key = message.get("data", {}).get("key", {}).get("normalizedPayload", "")
            if key and base64.b64decode(key).decode(errors="replace") == NEEDLE and found is None:
                found = time.perf_counter() - start
            if flags & 2 or "done" in message:
                break
    return found, time.perf_counter() - start


def kafdrop_search():
    """The search page; Kafdrop stops after a fixed number of scanned records."""
    url = (f"{WEB['kafdrop']['url']}/topic/{TOPIC}/search-messages?searchText={NEEDLE}&maximumCount=1"
           "&partition=-1&format=DEFAULT&keyFormat=DEFAULT&startTimestamp=1970-01-01%2000:00:00.000")
    start = time.perf_counter()
    with urllib.request.urlopen(url, timeout=3600) as r:
        page = r.read().decode("utf-8", "replace")
    total = time.perf_counter() - start
    scanned = re.search(r"after scanning (\d+) messages|Scanned (\d+) messages", page)
    KAFDROP_SCANNED.append(int(next(g for g in scanned.groups() if g)) if scanned else None)
    return (total if NEEDLE in page and "No messages found" not in page else None), total


KAFDROP_SCANNED = []
WEB["kafbat"]["search"] = kafbat_search
WEB["akhq"]["search"] = akhq_search
WEB["redpanda"]["search"] = redpanda_search
WEB["kafdrop"]["search"] = kafdrop_search
SEARCH_ARGS = ("consume", TOPIC, "-b", "-G", NEEDLE, "-n", "1", "-t", "15000")


# --- prepare ----------------------------------------------------------------

def prepare(count):
    if TOPIC in ferrika("topics", "list").stdout:
        sys.exit(f"{TOPIC} already exists; delete it first to regenerate")
    subprocess.run([str(EXE), "-b", BOOTSTRAP, "topics", "create", TOPIC, "-p", "3", "-r", "1"],
                   check=True, stdin=subprocess.DEVNULL)
    subprocess.run([str(EXE), "-b", BOOTSTRAP, "generate", TOPIC, "-n", str(count),
                    "-r", str(count), "-k", "key-", "--seed", "42"],
                   check=True, stdin=subprocess.DEVNULL)
    write_needle()


def write_needle():
    value = json.dumps({"order_id": NEEDLE, "status": "NEEDLE"})
    subprocess.run([str(EXE), "-b", BOOTSTRAP, "produce", TOPIC, "-k", NEEDLE, "-m", value],
                   check=True, stdin=subprocess.DEVNULL)


# --- measure ----------------------------------------------------------------

def measure_web(key, r):
    tool = WEB[key]
    # startup: restart the container, wait until the topic list answers
    docker("stop", tool["container"])
    start = time.perf_counter()
    docker("start", tool["container"])
    while True:
        try:
            if TOPIC in json.dumps(http_json(tool["url"] + tool["topics"], timeout=5)):
                break
        except Exception:
            pass
        time.sleep(0.1)
    r["startup_s"] = round(time.perf_counter() - start, 2)
    time.sleep(15)
    r["idle_mib"] = mib(statistics.median(container_memory(tool["container"]) for _ in range(5)))

    lists = []
    for _ in range(5):
        start = time.perf_counter()
        http_json(tool["url"] + tool["topics"])
        lists.append(time.perf_counter() - start)
    r["topics_s"] = median(lists)

    runs = []
    for _ in range(REPEAT):
        with PeakSampler(lambda: container_memory(tool["container"]), 0.5) as mem:
            found, total = tool["search"]()
        runs.append({"found_s": found and round(found, 2), "total_s": round(total, 2),
                     "peak_mib": mib(mem.peak)})
    r["search"] = {"runs": runs, "found_s": median(x["found_s"] for x in runs),
                   "found_all": all(x["found_s"] is not None for x in runs),
                   "peak_mib": max(x["peak_mib"] for x in runs)}

    results = [None] * USERS

    def one(i):
        results[i] = tool["search"]()

    with PeakSampler(lambda: container_memory(tool["container"]), 0.5) as mem:
        start = time.perf_counter()
        threads = [threading.Thread(target=one, args=(i,)) for i in range(USERS)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
    r["parallel"] = {"wall_s": round(time.perf_counter() - start, 2),
                     "found_all": all(x[0] is not None for x in results), "peak_mib": mib(mem.peak)}
    if key == "kafdrop":
        r["scanned_before_stopping"] = max((n for n in KAFDROP_SCANNED if n), default=None)


def measure_ferrika_net(r):
    runs = []
    for _ in range(REPEAT):
        _, seconds, rss, out = linux_ferrika(*SEARCH_ARGS)
        runs.append({"found_s": round(seconds, 2), "found": NEEDLE in out, "peak_mib": mib(rss)})
    r["search"] = {"runs": runs, "found_s": median(x["found_s"] for x in runs),
                   "found_all": all(x["found"] for x in runs),
                   "peak_mib": max(x["peak_mib"] for x in runs)}

    results = [None] * USERS

    def one(i):
        results[i] = linux_ferrika(*SEARCH_ARGS)

    threads = [threading.Thread(target=one, args=(i,)) for i in range(USERS)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    r["parallel"] = {"wall_s": round(max(s + d for s, d, _, _ in results) - min(s for s, _, _, _ in results), 2),
                     "found_all": all(NEEDLE in out for *_, out in results),
                     "peak_mib": mib(sum(rss for _, _, rss, _ in results))}
    _, seconds, _, _ = linux_ferrika("bench", "consume", TOPIC, "-n", str(RECORDS + 1), "-t", "120")
    r["read_all_s"] = round(seconds, 2)


def measure_ferrika_host(r):
    r["topics_s"] = median(timed_ferrika("topics", "list")[0] for _ in range(5))

    runs = []
    for _ in range(REPEAT):
        seconds, peak, out = timed_ferrika(*SEARCH_ARGS)
        runs.append({"found_s": round(seconds, 2), "found": NEEDLE in out, "peak_mib": mib(peak)})
    r["search"] = {"runs": runs, "found_s": median(x["found_s"] for x in runs),
                   "found_all": all(x["found"] for x in runs),
                   "peak_mib": max(x["peak_mib"] for x in runs)}

    results = [None] * USERS

    def one(i):
        results[i] = timed_ferrika(*SEARCH_ARGS)

    start = time.perf_counter()
    threads = [threading.Thread(target=one, args=(i,)) for i in range(USERS)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    r["parallel"] = {"wall_s": round(time.perf_counter() - start, 2),
                     "found_all": all(NEEDLE in out for *_, out in results),
                     "peak_mib": mib(sum(peak for _, peak, _ in results))}
    r["read_all_s"] = round(timed_ferrika("bench", "consume", TOPIC, "-n", str(RECORDS + 1), "-t", "120")[0], 2)


def environment():
    class MEMORYSTATUSEX(ctypes.Structure):
        _fields_ = [("dwLength", ctypes.c_uint32), ("dwMemoryLoad", ctypes.c_uint32),
                    ("ullTotalPhys", ctypes.c_ulonglong), ("ullAvailPhys", ctypes.c_ulonglong),
                    ("ullTotalPageFile", ctypes.c_ulonglong), ("ullAvailPageFile", ctypes.c_ulonglong),
                    ("ullTotalVirtual", ctypes.c_ulonglong), ("ullAvailVirtual", ctypes.c_ulonglong),
                    ("ullAvailExtendedVirtual", ctypes.c_ulonglong)]
    status = MEMORYSTATUSEX()
    status.dwLength = ctypes.sizeof(status)
    ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(status))
    cpu = subprocess.run(["powershell", "-NoProfile", "-Command", "(Get-CimInstance Win32_Processor).Name"],
                         capture_output=True, text=True).stdout.strip()
    topic = http_json(f"{WEB['kafbat']['url']}/api/clusters/{CLUSTER}/topics/{TOPIC}")
    akhq = http_json(f"{WEB['akhq']['url']}/api/{CLUSTER}/topic/{TOPIC}")
    return {
        "date": datetime.datetime.now().astimezone().isoformat(timespec="seconds"),
        "os": platform.platform(),
        "cpu": cpu,
        "ram_gib": round(status.ullTotalPhys / 1024 ** 3, 1),
        "wsl_vm_gib": round(int(docker("info", "--format", "{{.MemTotal}}").strip()) / 1024 ** 3, 1),
        "ferrika": ferrika("--version").stdout.strip().removeprefix("ferrika "),
        "ferrika_exe_mib": mib(EXE.stat().st_size),
        "ferrika_binary": "ferrika.exe (CLI)",
        "rustc_linux": docker("exec", LINUX, "rustc", "--version").strip(),
        "kafbat_ui": http_json(WEB["kafbat"]["url"] + "/api/info")["build"]["version"],
        "akhq": "0.28.0" if akhq else None,
        "redpanda_console": "v3.12.0",
        "kafdrop": docker("exec", "kafdrop-bench", "sh", "-c", "ls /kafdrop").strip()
        .removeprefix("kafdrop-").removesuffix(".jar"),
        "kafka": docker("exec", "kafka-bench", "/opt/kafka/bin/kafka-topics.sh", "--version").strip(),
        "topic": TOPIC,
        "partitions": len(topic.get("partitions", [])),
        "records": sum(p["offsetMax"] - p["offsetMin"] for p in topic.get("partitions", [])),
        "topic_bytes": topic.get("segmentSize"),
    }


def write_report(results):
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "results.json").write_text(json.dumps(results, indent=2, ensure_ascii=False) + "\n",
                                      encoding="utf-8")
    env = results["environment"]
    cols = [("kafbat", "Kafbat UI"), ("akhq", "AKHQ"), ("redpanda", "Redpanda Console"), ("kafdrop", "Kafdrop"),
            ("ferrika_net", "Ferrika CLI, same network"),
            ("ferrika_host", "Ferrika CLI, from Windows")]
    t = {k: results.get(k, {}) for k, _ in cols}

    def cell(value, unit, ok=True):
        if value is None:
            return "—" if ok else "not found"
        return f"{value:,} {unit}" + ("" if ok else " (not found)")

    def row(label, fn):
        return f"| {label} | " + " | ".join(fn(t[k]) for k, _ in cols) + " |"

    lines = [
        "# Ferrika CLI vs web-based Kafka UIs",
        "",
        f"Generated by `tools/bench/run.py` on {env['date']}. Raw numbers: [`results.json`](results.json).",
        "",
        "| Scenario | " + " | ".join(name for _, name in cols) + " |",
        "|---|" + "---|" * len(cols),
        row(f"Find 1 record in {env['records']:,}",
            lambda x: cell(x.get("search", {}).get("found_s"), "s", x.get("search", {}).get("found_all", True))),
        row("Memory during that search (peak)", lambda x: cell(x.get("search", {}).get("peak_mib"), "MiB")),
        row(f"{USERS} searches at once",
            lambda x: cell(x.get("parallel", {}).get("wall_s") if x.get("parallel", {}).get("found_all", True)
                           else None, "s", x.get("parallel", {}).get("found_all", True))),
        row(f"Memory with {USERS} searches (peak, all processes)",
            lambda x: cell(x.get("parallel", {}).get("peak_mib"), "MiB")),
        row("Read the whole topic, no filter", lambda x: cell(x.get("read_all_s"), "s")),
        row("Server start until usable (web servers only)", lambda x: cell(x.get("startup_s"), "s")),
        row("Server memory at idle (web servers only)", lambda x: cell(x.get("idle_mib"), "MiB")),
        row("Topic list (warm)", lambda x: cell(x.get("topics_s"), "s")),
        "",
        f"Times are the median of {REPEAT} runs (topic list: 5); memory peaks are the largest run.",
        "",
        "## Setup",
        "",
        f"- Machine: {env['cpu']}, {env['ram_gib']} GiB RAM, {env['os']}; WSL2 VM with {env['wsl_vm_gib']} GiB.",
        f"- Kafka {env['kafka']} (one KRaft broker), Kafbat UI {env['kafbat_ui']}, AKHQ {env['akhq']}, "
        f"Redpanda Console {env.get('redpanda_console')} and Kafdrop {env.get('kafdrop')} in Docker on WSL2 with "
        "default settings, from [`tools/bench/compose.yml`](../../tools/bench/compose.yml).",
        f"- Ferrika {env['ferrika']} in two places:",
        f"  - **same network**: a Linux release build ({env['rustc_linux']}) in a container on the broker's Docker "
        "network, the position the web servers have.",
        f"  - **from Windows**: {env.get('ferrika_binary', 'the Windows exe')} reaching the broker "
        "through WSL2's port forwarding, like a client on another machine.",
        f"- Data: topic `{env['topic']}`, {env['partitions']} partitions, {env['records']:,} JSON records "
        f"({mib(env['topic_bytes'] or 0):,} MiB) from `ferrika generate`. The record searched for is the last one "
        "written, at the end of its partition.",
        "",
        "## How to read it",
        "",
        "- **Compare the web servers with the \"same network\" column.** The \"from Windows\" column adds WSL2's "
        "port forwarding, which slows Ferrika's reads here; it stands for a client on another machine.",
        "- Before 2026-10-13 Ferrika needed about 54 s for this search, on either network. librdkafka's "
        "`fetch.queue.backoff.ms` (default 1000 ms) paused a partition's fetching for a second each time its local "
        "queue filled, which happens over and over when a reader processes records a little slower than they arrive. "
        "Ferrika's consumers now set it to 10 ms; memory stayed the same. AKHQ's Java consumer has no such pause and "
        "takes up to 25,000 records per `poll()`.",
        "- Later that day Ferrika's searches and `bench consume` moved to one consumer per partition (up to 8), each "
        "on its own thread and filtering there, instead of one consumer for the topic. One librdkafka consumer reads "
        "a broker through one connection and one thread; with three the search took 6.6 s → 3.1 s on the same "
        "network and 12.3 s → 5.1 s from Windows. Each consumer's prefetch queue is capped at 8 MiB "
        "(`queued.max.messages.kbytes`), which kept the peak memory below the single consumer's.",
        "- The \"same network\" Ferrika runs in a container inside the WSL2 VM, sharing its CPUs with the broker "
        "and the web servers, as they share them with each other.",
        "- Server memory is the container only (usage minus inactive page cache, as `docker stats` shows it). "
        "Each user's browser tab comes on top. Ferrika memory is the whole process (max RSS on Linux, working set on "
        "Windows).",
        "- Search: Kafbat UI `messages/v2` with `stringFilter` (key or value contains); AKHQ `data/search` with "
        "`searchByValue`; Redpanda Console `ListMessages` with the JavaScript filter "
        "`return value.order_id == \"…\"` (its way to search); Kafdrop's search page, which gives up after a fixed "
        "number of scanned records (see `scanned_before_stopping` in results.json) and so cannot find this record; Ferrika `consume -b -G` (case-insensitive regex over the value), stopping at the first match. "
        "- Start: the web servers from container start until the topic list API answers. The CLI has no server "
        "to start, so it has no entry.",
        "- Topic list: the web servers answer one API call while already connected; the Ferrika time is a whole "
        "CLI run that starts a process and connects.",
        "- Redpanda Console is a Go server; the others are JVM servers. "
        "The JVM servers run with their default heap limit (a quarter of the WSL2 VM's memory). Their memory reflects "
        "how far the heap grows when allowed; a smaller `-Xmx` caps memory but may slow or fail large searches. "
        "That trade-off was not measured.",
        "- Scope: one machine, one 3 GiB topic, one scenario family (finding a single record, plus a plain read), "
        "default settings for every product, and the versions listed above. A web UI and a command line do "
        "different jobs; these numbers compare how long and how much memory it takes to find one record, not "
        "features, and other topologies or settings can give other results.",
        "",
        "Reproduce: `docker compose -f tools/bench/compose.yml up -d`, set up the Linux build container (see the "
        "docstring of `run.py`), then `python tools/bench/run.py prepare` and `python tools/bench/run.py measure`.",
    ]
    (OUT / "RESULTS.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def measure(only):
    path = OUT / "results.json"
    results = json.loads(path.read_text(encoding="utf-8")) if only and path.exists() else {}
    results["environment"] = environment()
    steps = {"kafbat": lambda r: measure_web("kafbat", r), "akhq": lambda r: measure_web("akhq", r),
             "redpanda": lambda r: measure_web("redpanda", r), "kafdrop": lambda r: measure_web("kafdrop", r),
             "ferrika_net": measure_ferrika_net, "ferrika_host": measure_ferrika_host}
    for name, step in steps.items():
        if only and name not in only:
            continue
        print(f"[bench] {name} ...", flush=True)
        results[name] = {}
        step(results[name])
        print(f"[bench] {name}: {json.dumps(results[name], ensure_ascii=False)}", flush=True)
        write_report(results)
    print(f"[bench] wrote {OUT / 'RESULTS.md'}")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("prepare")
    p.add_argument("--records", type=int, default=RECORDS)
    sub.add_parser("needle")
    sub.add_parser("report", help="rewrite RESULTS.md from results.json")
    m = sub.add_parser("measure")
    m.add_argument("--only", nargs="*", choices=["kafbat", "akhq", "redpanda", "kafdrop", "ferrika_net", "ferrika_host"],
                   help="measure these and keep the others from results.json")
    args = parser.parse_args()
    if args.cmd != "report" and not EXE.exists():
        sys.exit(f"{EXE} not found; build with `cargo build --release -p ferrika-cli` or set FERRIKA_EXE")
    if args.cmd == "prepare":
        prepare(args.records)
    elif args.cmd == "needle":
        write_needle()
    elif args.cmd == "report":
        write_report(json.loads((OUT / "results.json").read_text(encoding="utf-8")))
    else:
        measure(args.only)


if __name__ == "__main__":
    main()
