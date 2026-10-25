"""Driver for the scatter admission test. Run it through scatter_admission.sh.

    python3 -I scatter_admission.py load   --out DIR
    python3 -I scatter_admission.py arm    --out DIR --pid PID --shape user|country --distributed true|false
    python3 -I scatter_admission.py report --out DIR

`load` creates bench.events and posts many small NDJSON batches, so the table
spans hundreds of files. `arm` holds CONCURRENCY closed-loop clients on
POST /v1/queries for DURATION_S seconds while a sampler records the beam
process every SAMPLE_MS. `report` turns the arm JSON files into markdown tables.

Standard library only. Every knob is an environment variable with a default.
"""
import http.client
import json
import math
import os
import random
import statistics
import subprocess
import sys
import threading
import time

API = os.environ.get("API", "http://127.0.0.1:14100")
METRICS = os.environ.get("METRICS", "http://127.0.0.1:14103")
KEY = os.environ.get("SMOLQUERY_API_KEY", "bench")
SECRET = os.environ.get("SMOLQUERY_INTERNAL_SECRET", "bench-internal")
CONCURRENCY = int(os.environ.get("CONCURRENCY", "8"))
DURATION_S = float(os.environ.get("DURATION_S", "60"))
SAMPLE_MS = int(os.environ.get("SAMPLE_MS", "250"))
BATCHES = int(os.environ.get("BATCHES", "400"))
ROWS_PER_BATCH = int(os.environ.get("ROWS_PER_BATCH", "7500"))
USERS = int(os.environ.get("USERS", "500000"))
LOAD_WORKERS = int(os.environ.get("LOAD_WORKERS", "1"))
LOAD_PAUSE_MS = int(os.environ.get("LOAD_PAUSE_MS", "0"))
DRAIN_WAIT_S = int(os.environ.get("DRAIN_WAIT_S", "0"))
QUERY_TIMEOUT_MS = int(os.environ.get("QUERY_TIMEOUT_MS", "300000"))

COUNTRIES = ["US", "DE", "PL", "FR", "GB", "BR", "IN", "JP", "CN", "CA", "ES", "IT", "NL", "SE", "NO", "MX", "AU", "KR", "TR", "ZA"]
SHAPES = {
    "user": "SELECT user_id, count(*) AS n, avg(latency) AS avg_latency FROM bench.events GROUP BY user_id ORDER BY n DESC LIMIT 10",
    "country": "SELECT country, count(*) AS n, avg(latency) AS avg_latency FROM bench.events GROUP BY country ORDER BY n DESC LIMIT 10",
}


def host_port():
    host, port = API.split("//")[1].split(":")
    return host, int(port)


_local = threading.local()


def call(method, path, body=None, raw=None, ctype="application/json", timeout=600, base=None, headers=None):
    base = base or API
    host, port = base.split("//")[1].split(":")
    hdrs = {"authorization": "Bearer " + KEY}
    hdrs.update(headers or {})
    data = raw
    if body is not None:
        data = json.dumps(body).encode()
    if data is not None:
        hdrs["content-type"] = ctype
    conns = getattr(_local, "conns", None)
    if conns is None:
        conns = _local.conns = {}
    for attempt in (0, 1):
        conn = conns.get(base)
        if conn is None:
            conn = conns[base] = http.client.HTTPConnection(host, int(port), timeout=timeout)
        try:
            conn.request(method, path, body=data, headers=hdrs)
            resp = conn.getresponse()
            return resp.status, resp.read()
        except (http.client.HTTPException, ConnectionError):
            conn.close()
            conns.pop(base, None)
            if attempt:
                raise


def log(*parts):
    print(time.strftime("%H:%M:%S"), *parts, flush=True)


def query(sql, distributed=None, trace=False):
    body = {"query": sql, "maxResults": 10, "timeoutMs": QUERY_TIMEOUT_MS}
    if distributed is not None:
        body["distributed"] = distributed
        body["burst"] = False
    if trace:
        body["trace"] = True
    status, raw = call("POST", "/v1/queries", body)
    try:
        return status, json.loads(raw)
    except ValueError:
        return status, {"raw": raw[:300].decode("utf-8", "replace")}


def scrape():
    status, raw = call("GET", "/metrics", base=METRICS, headers={"x-smolquery-internal": SECRET})
    if status != 200:
        return {}
    series = {}
    for line in raw.decode().splitlines():
        if line.startswith("#") or not line.strip():
            continue
        name, _, value = line.rpartition(" ")
        try:
            series[name] = float(value)
        except ValueError:
            pass
    return series


def delta(after, before, prefix):
    keys = [k for k in after if k == prefix or k.startswith(prefix + "{")]
    return sum(after[k] - before.get(k, 0.0) for k in keys)


def load_one(index, seed):
    rng = random.Random(seed + index)
    base = 1_790_000_000 + index * 3600
    lines = []
    for i in range(ROWS_PER_BATCH):
        ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(base + i // 2))
        lines.append(
            '{"id": %d, "ts": "%s", "user_id": %d, "country": "%s", "latency": %.3f}'
            % (index * ROWS_PER_BATCH + i, ts, rng.randrange(USERS), COUNTRIES[rng.randrange(len(COUNTRIES))], rng.lognormvariate(3.0, 0.8))
        )
    return ("\n".join(lines) + "\n").encode()


def cmd_load(out):
    os.makedirs(out, exist_ok=True)
    log("create dataset and table")
    print(call("POST", "/v1/datasets", {"id": "bench"}))
    schema = [
        {"name": "id", "type": "INT64", "nullable": False},
        {"name": "ts", "type": "TIMESTAMP"},
        {"name": "user_id", "type": "INT64"},
        {"name": "country", "type": "STRING"},
        {"name": "latency", "type": "FLOAT64"},
    ]
    print(call("POST", "/v1/datasets/bench/tables", {"id": "events", "schema": schema}))
    pending = list(range(BATCHES))
    lock = threading.Lock()
    failures = []
    started = time.time()

    def worker():
        while True:
            with lock:
                if not pending:
                    return
                index = pending.pop()
            body = load_one(index, 42)
            time.sleep(LOAD_PAUSE_MS / 1000.0)
            for attempt in range(20):
                status, raw = call("POST", "/v1/datasets/bench/tables/events/insert", raw=body, ctype="application/x-ndjson")
                if status == 200:
                    break
                time.sleep(1 + attempt)
            else:
                failures.append((index, status, raw[:200]))

    threads = [threading.Thread(target=worker) for _ in range(LOAD_WORKERS)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    log("loaded %d batches x %d rows in %.1fs, failures=%d" % (BATCHES, ROWS_PER_BATCH, time.time() - started, len(failures)))
    if failures:
        log("first failures", failures[:3])
        sys.exit(1)
    deadline = time.time() + DRAIN_WAIT_S
    while True:
        info = cmd_inventory(out)
        if not DRAIN_WAIT_S or info["hot_files"] == 0 or time.time() > deadline:
            break
        time.sleep(5)


def cmd_inventory(out):
    status, job = query("SELECT count(*) AS n, count(DISTINCT user_id) AS users FROM bench.events")
    stats = (job.get("job") or job).get("statistics") or {}
    info = {
        "status": status,
        "rows": (job.get("rows") or [[None]])[0],
        "hot_files": (stats.get("hot") or {}).get("filesTotal"),
        "sealed_files": (stats.get("sealed") or {}).get("filesTotal"),
        "hot_rows": (stats.get("hot") or {}).get("rowsScanned"),
        "sealed_rows": (stats.get("sealed") or {}).get("rowsScanned"),
    }
    log("inventory", info)
    with open(os.path.join(out, "inventory.json"), "w") as handle:
        json.dump({"raw": job, "summary": info}, handle, indent=1, default=str)
    return info


def cpu_seconds(text):
    total = 0.0
    for part in text.strip().split(":"):
        total = total * 60 + float(part)
    return total


def read_proc(pid):
    res = subprocess.run(["ps", "-o", "time=,rss=", "-p", str(pid)], capture_output=True, text=True)
    fields = res.stdout.split()
    if len(fields) != 2:
        return None
    threads = subprocess.run(["ps", "-M", "-p", str(pid)], capture_output=True, text=True).stdout.count("\n") - 1
    return cpu_seconds(fields[0]), int(fields[1]) / 1024.0, threads


def sampler(pid, stop, samples):
    previous = None
    while not stop.is_set():
        now = time.time()
        proc = read_proc(pid)
        if proc is not None:
            cpu, rss_mb, threads = proc
            pct = None
            if previous is not None and now > previous[0]:
                pct = 100.0 * (cpu - previous[1]) / (now - previous[0])
            samples.append({"t": now, "rss_mb": rss_mb, "threads": threads, "cpu_pct": pct})
            previous = (now, cpu)
        stop.wait(max(0.0, SAMPLE_MS / 1000.0 - (time.time() - now)))


def summarize(values):
    values = [v for v in values if v is not None]
    if not values:
        return {}
    return {"peak": max(values), "mean": statistics.fmean(values)}


def percentile(sorted_values, p):
    if not sorted_values:
        return None
    return sorted_values[min(len(sorted_values) - 1, math.ceil(p / 100.0 * len(sorted_values)) - 1)]


def peak_engines(jobs):
    events = []
    for job in jobs:
        if job["status"] != 200:
            continue
        engines = 1 + (job["shards"] or 0)
        events.append((job["start"], engines))
        events.append((job["end"], -engines))
    events.sort()
    level = peak = 0
    for _, step in events:
        level += step
        peak = max(peak, level)
    return peak


def cmd_arm(out, pid, shape, distributed):
    sql = SHAPES[shape]
    label = "%s-%s" % (shape, "distributed" if distributed else "single")
    uptime = subprocess.run(["uptime"], capture_output=True, text=True).stdout.strip()
    log(label, "uptime:", uptime)

    inventory = cmd_inventory(out)
    status, probe = query(sql, distributed=distributed)
    job = probe.get("job") or probe
    probe_scatter = job.get("scatter")
    log(label, "probe status", status, "scatter", probe_scatter)
    if distributed and probe_scatter is None:
        sys.exit("distributed probe did not scatter; the shape is refused by the decomposer or the table has too few files")
    if not distributed and probe_scatter is not None:
        sys.exit("single-engine probe scattered")

    idle_samples, stop = [], threading.Event()
    thread = threading.Thread(target=sampler, args=(pid, stop, idle_samples))
    thread.start()
    time.sleep(5)
    stop.set()
    thread.join()

    before = scrape()
    samples, stop = [], threading.Event()
    thread = threading.Thread(target=sampler, args=(pid, stop, samples))
    thread.start()
    jobs, lock = [], threading.Lock()
    deadline = time.time() + DURATION_S

    def client():
        while time.time() < deadline:
            start = time.time()
            try:
                status, body = query(sql, distributed=distributed)
            except Exception as error:
                status, body = 0, {"raw": repr(error)}
            end = time.time()
            node = body.get("job") or body
            scatter = node.get("scatter") if isinstance(node, dict) else None
            record = {
                "start": start,
                "end": end,
                "status": status,
                "ms": (end - start) * 1000.0,
                "scattered": scatter is not None,
                "shards": (scatter or {}).get("shards"),
            }
            if status not in (200, 429):
                record["body"] = str(body)[:300]
            with lock:
                jobs.append(record)
            if status != 200:
                time.sleep(0.25)

    clients = [threading.Thread(target=client) for _ in range(CONCURRENCY)]
    began = time.time()
    [c.start() for c in clients]
    [c.join() for c in clients]
    elapsed = time.time() - began
    stop.set()
    thread.join()
    time.sleep(1)
    after = scrape()

    ok = sorted(j["ms"] for j in jobs if j["status"] == 200)
    result = {
        "label": label,
        "shape": shape,
        "distributed": distributed,
        "sql": sql,
        "uptime": uptime,
        "inventory": inventory,
        "elapsed_s": elapsed,
        "concurrency": CONCURRENCY,
        "probe_scatter": probe_scatter,
        "requests": len(jobs),
        "ok": len(ok),
        "refused_429": sum(1 for j in jobs if j["status"] == 429),
        "other_status": sorted({j["status"] for j in jobs if j["status"] not in (200, 429)}),
        "scattered_jobs": sum(1 for j in jobs if j["scattered"]),
        "shards_per_scattered_job": statistics.fmean([j["shards"] for j in jobs if j["shards"]]) if any(j["shards"] for j in jobs) else None,
        "latency_ms": {"p50": percentile(ok, 50), "p95": percentile(ok, 95), "max": ok[-1] if ok else None},
        "idle": {k: summarize([s[k] for s in idle_samples]) for k in ("rss_mb", "threads")},
        "rss_mb": summarize([s["rss_mb"] for s in samples]),
        "threads": summarize([s["threads"] for s in samples]),
        "cpu_pct": summarize([s["cpu_pct"] for s in samples]),
        "samples": len(samples),
        "peak_engines_from_jobs": peak_engines(jobs),
        "engines_acquired": delta(after, before, "smolquery_query_engines_total"),
        "scattered_total": delta(after, before, "smolquery_query_scattered_total"),
        "scatter_shards_total": delta(after, before, "smolquery_query_scatter_shards_total"),
        "jobs_total": delta(after, before, "smolquery_query_jobs_total"),
        "rss_peak_metric_mb": after.get("smolquery_memory_rss_peak_bytes", 0) / 1048576.0,
    }
    with open(os.path.join(out, label + ".json"), "w") as handle:
        json.dump({"result": result, "errors": errors_by_status(jobs)}, handle, indent=1)
    with open(os.path.join(out, label + ".samples.json"), "w") as handle:
        json.dump(samples, handle)
    log(label, json.dumps({k: result[k] for k in ("requests", "ok", "refused_429", "scattered_jobs", "latency_ms", "rss_mb", "threads", "cpu_pct", "peak_engines_from_jobs", "engines_acquired")}))


def errors_by_status(jobs):
    seen, out = {}, []
    for j in jobs:
        if "body" in j and seen.get(j["status"], 0) < 3:
            seen[j["status"]] = seen.get(j["status"], 0) + 1
            out.append(j)
    return out


def fmt(value, digits=0):
    return "-" if value is None else ("%.*f" % (digits, value))


def cmd_report(out):
    rows = []
    for shape in ("user", "country"):
        for mode in ("single", "distributed"):
            path = os.path.join(out, "%s-%s.json" % (shape, mode))
            if os.path.exists(path):
                rows.append(json.load(open(path))["result"])
    print("| arm | requests | ok | 429 | scattered | p50 ms | p95 ms | RSS peak MB | RSS mean MB | threads peak | threads mean | CPU% peak | CPU% mean | engines acquired / ok job | peak engines (jobs) | idle RSS MB | idle threads |")
    print("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    for r in rows:
        per_job = r["engines_acquired"] / r["ok"] if r["ok"] else None
        print(
            "| %s | %d | %d | %d | %d | %s | %s | %s | %s | %s | %s | %s | %s | %s | %d | %s | %s |"
            % (
                r["label"], r["requests"], r["ok"], r["refused_429"], r["scattered_jobs"],
                fmt(r["latency_ms"]["p50"]), fmt(r["latency_ms"]["p95"]),
                fmt(r["rss_mb"].get("peak")), fmt(r["rss_mb"].get("mean")),
                fmt(r["threads"].get("peak")), fmt(r["threads"].get("mean")),
                fmt(r["cpu_pct"].get("peak")), fmt(r["cpu_pct"].get("mean")),
                fmt(per_job, 1), r["peak_engines_from_jobs"],
                fmt(r["idle"]["rss_mb"].get("mean")), fmt(r["idle"]["threads"].get("mean")),
            )
        )
    print()
    for r in rows:
        print("%s: uptime before: %s" % (r["label"], r["uptime"]))


def main():
    args = sys.argv[1:]
    opts = dict(zip(args[1::2], args[2::2]))
    out = opts.get("--out", "scatter-run")
    os.makedirs(out, exist_ok=True)
    if args[0] == "load":
        cmd_load(out)
    elif args[0] == "inventory":
        cmd_inventory(out)
    elif args[0] == "arm":
        cmd_arm(out, int(opts["--pid"]), opts["--shape"], opts["--distributed"] == "true")
    elif args[0] == "report":
        cmd_report(out)
    else:
        sys.exit("usage: load | inventory | arm | report")


main()
