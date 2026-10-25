# smolquery_bench

Ingest benchmark: [smolquery](https://github.com/chasers/smolquery) against
ClickHouse, as fairly as possible, following
[abc3/load-rig](https://github.com/abc3/load-rig). Both arms get the **identical
NDJSON body** — 3,062 OpenTelemetry log rows × 63 columns, 6.87 MiB — smolquery
via `POST …/insert` (`application/x-ndjson`), ClickHouse via
`INSERT … FORMAT JSONEachRow`.

## Prerequisites

macOS with k6 and ClickHouse (`brew install k6 clickhouse`), Go, Elixir 1.20 or
later, and a smolquery checkout with compiled deps (`SMOLQUERY_DIR`, default
`~/Dev/supabase/smolquery`).

**Every result in this repo was measured on Erlang/OTP 29.** On 2026-08-17 OTP
27.3.4.6 measured **6.2% faster** at 1 VU, with non-overlapping ranges, and it
did not hit the SIGBUS crash in six runs. **The next real local bench should move
to OTP 27 — but the switch has not been made**, because it needs its own
re-baseline rather than a quiet change mid-investigation. Never mix OTP 27 and
OTP 29 numbers in one table, and say which OTP produced a published figure. See
[results/2026-08-17-otp-27-vs-29.md](results/2026-08-17-otp-27-vs-29.md).

## Quick start

```sh
scripts/gen-bodies.exs               # bodies/eachrow.3062.ndjson, seed 42

scripts/setup-smolquery.exs          # cold data dir, server, dataset, table, clustering
scripts/run-arm.exs smolquery        # preflight, 20s warm-up, 15s pause, 60s measured

scripts/setup-clickhouse.exs         # cold path, server, table, fsync settings
scripts/run-arm.exs clickhouse

scripts/report.exs                   # markdown table from results/raw/
scripts/report-html.exs <db.sqlite3> # charted HTML report for one run
scripts/stop.exs                     # stops both servers

scripts/sweep.exs smolquery          # full VU sweep, VUS_LIST="1 4 8 16 32 64"
scripts/sweep.exs clickhouse         # cold table before each run
```

Against a deployed cluster, use the `remote` arm:

```sh
scripts/setup-remote.exs             # health check, dataset, table, clustering
scripts/run-arm.exs remote           # one run against BASE_URL
scripts/sweep.exs remote             # VU sweep

scripts/watch-pods.exs 30 pods.json  # pod CPU and RSS on its own
```

## Commands

`mise.toml` wraps every workflow. Run `mise tasks` for the list.

```sh
mise run bodies                      # regenerate the deterministic body
mise run remote-setup                # dataset + table on the deployed cluster
mise run remote-sweep                # sweep from this machine
mise run report                      # markdown table (RESULTS=... to pick a dir)
```

The in-region load generator, which removes this machine's uplink from the
measurement:

```sh
mise run bench-up                    # launch, install k6 + Go, push harness, build body
mise run bench-status                # instance, SSM ping, api pod it targets
mise run bench-sweep                 # run the selected benches (BENCHES=..., default ingest)
mise run bench-report                # table for results/raw-loadgen/
mise run report-html -- <db>         # charted HTML report for one run
mise run bench-down                  # terminate
```

### Bench types

`BENCHES` selects what a sweep runs. It takes one type or several, separated by
commas. An unknown or empty value fails before anything starts.

```sh
mise run bench-ingest                # BENCHES=ingest
mise run bench-pruning               # BENCHES=pruning
mise run bench-compaction            # BENCHES=compaction
mise run bench-onebrc                # BENCHES=onebrc, TABLE=onebrc_v1
mise run bench-onebrc-ingest         # BENCHES=onebrc, ingest only, one upload per worker count
mise run bench-attrs                 # BENCHES=ingest,attrs; SHAPE, TABLE and VUS_LIST from the shell
mise run bench-tail                  # BENCHES=tail; the last 100 events per project while ingest runs
mise run bench-all                   # BENCHES=ingest,pruning,compaction
BENCHES=ingest,pruning mise run bench-sweep
```

**A task's own `env` block beats the shell.** `TABLE=onebrc_v5 mise run
bench-onebrc` still runs into `onebrc_v1`, because the task sets `TABLE`
itself. To pick a table or a worker list, bypass the task:
`mise exec -- sh -c 'BASE_URL=… BENCHES=onebrc TABLE=onebrc_v5 elixir scripts/loadgen.exs sweep'`.
`mise exec` loads the `[env]` block without the task env.

| type | measures | writes |
|---|---|---|
| `ingest` | the VU sweep: rows/s, latency, refusals, pod CPU and memory | `<label>.k6.json`, `<label>.pods.json` |
| `tail` | the last 100 events for each of several projects, queried back to back from the box, `distributed` alternating true and false — idle, during a measured ingest point, and after the drain | `<label>.tail.json` per phase, `loadgen-tail<suffix>.tail.json` index, `loadgen-tail-vus<N><suffix>.k6.json` and `.pods.json` for the ingest point |
| `attrs` | project-scoped and unscoped attribute queries — count, a string key filter, a numeric key filter, an error-presence filter, a group-by on the route, a body `LIKE` — over hot ∪ sealed, then again after the hot tier drains; each case carries an `"explain": "analyze"` | `<label>.attrs.json` |
| `pruning` | a `count` and a `scan` query against a live date, an empty date, and no filter; each case also records an `"explain": "analyze"` plan and its engine time | `<label>.prune.json` |
| `compaction` | compaction outcomes and pod restarts over a long window | `<label>.compact.json` |
| `onebrc` | the one billion row challenge, in spirit: generate a `station;temperature` CSV on the box, upload it as NDJSON inserts, then time the 1BRC aggregate ten times | `<label>.onebrc.json`, `<label>.upload.json` |

The types always run in the order ingest, attrs, tail, pruning,
compaction, onebrc, whatever order you type them in. Pruning reads the rows ingest writes, and
compaction needs those rows sealed. `onebrc` stands alone: it has its own
table, its own generator and its own uploader.

Every sweep also samples each pod's metrics into a SQLite database — see
[Pod metrics sampling](#pod-metrics-sampling).

**Read the `scan` cases, not the `count` ones, to judge pruning.** `count(*)`
is answered from Parquet footer statistics without touching row data, so it
costs about the same whether pruning works or not. `sum(PRUNE_COLUMN)` forces a
column read, which is what makes a pruned query visibly cheaper. On 2026-08-16
the same table answered the empty-date `count` 9% faster than the control and
the empty-date `scan` **180x** faster.

`bench-up` and friends need a live SSO session:
`aws sso login --profile sandbox`.

## Pod metrics sampling

Every `bench-sweep` samples the full metric set of every pod, every 10
seconds, for the whole sweep. Each sweep writes its own database:
`results/raw-loadgen/loadgen-<UTC stamp><suffix>.metrics.sqlite3`. Run it on
its own with `mise run metrics -- 300 out.sqlite3`.

Since smolquery 0.12.0 (T-302), **every node serves `GET /metrics`** on
port 4003 (`METRICS_PORT` overrides), gated by the **`x-smolquery-internal`**
header (`SMOLQUERY_INTERNAL_SECRET`), never the tenant api key. Counters
are node-local ETS, so one pod's answer never contains another pod's
counters — the sampler visits every pod.

Pods are not routable from outside the cluster, and the apiserver proxy
cannot add the auth header. Each scrape is therefore a `kubectl exec`
running a bash `/dev/tcp` fetch against the pod's own listener, with the
pod's own secret. The image ships no HTTP client, the fetch boots no VM in
the pod, and the secret stays inside the cluster. Builds before 0.12.0
served `/metrics` from the api role only; on those, scrape with
`bin/smolquery rpc "IO.puts(Smolquery.Telemetry.render())"` instead.

One table, `samples`: `sampled_at`, `phase` (the bench step, e.g.
`ingest-vus16`, `compaction`), `pod`, `metric`, `labels`, `value`. All series
except the two `*_shape_info` gauges are counters, so analysis reads deltas:

```sql
SELECT pod, max(value) - min(value) AS sealed
FROM samples
WHERE metric = 'smolquery_seal_segments_total' AND labels LIKE '%ok%'
GROUP BY pod;
```

Counters reset when a pod restarts. A `max - min` delta is wrong across a
crash — a reset shows as a drop, and the drop itself is evidence: the
sampler caught an OOMKill on 2026-08-18 that the log watcher missed.

The seal and compaction series to read first:
`smolquery_seal_attempts_total{result}`, `smolquery_seal_segments_total{result}`,
`smolquery_seal_stuck_attempts_total`, `smolquery_seal_release_failures_total`,
`smolquery_compactions_total{result}`,
`smolquery_compaction_segments_replaced_total`. Compare them against
`smolquery_buffer_rows_committed_total` to judge whether sealing keeps up
with ingest. A nonzero stuck or release-failure count means sealing is
stalled.

Since `main@18117bb` (T-379) the storage pods also carry the sealed object
store's request series: `smolquery_s3_requests_total{op,class}`,
`smolquery_s3_request_microseconds_total{op}`,
`smolquery_s3_request_bytes_total{op}` and
`smolquery_s3_request_microseconds_bucket{op,le}` (10 ms, 50 ms, 250 ms,
1 s, 5 s). Ops are `put`, `head`, `list`, `delete` — no `get`, since reads
go through DuckDB httpfs. One `put` is one seal attempt
(`storage_service/merge.ex:300`), so put bytes equal
`smolquery_seal_segment_bytes_total`. An empty bucket is omitted from the
render; read a missing `le` line as zero. The HTML report lists these
series in its trailing table and does not chart them yet.

## The HTML run report

Every sweep writes a charted HTML report beside the markdown write-ups:
`results/loadgen-<UTC stamp><suffix>.html`. Build one by hand from any
run's metrics database:

```
mise run report-html -- results/raw-loadgen/<run>.metrics.sqlite3 [out.html]
```

The report is one self-contained file — no network, no build step. Open it
in a browser and hover any chart to read one bucket across the whole run.
It covers, in order: the load points from `*.k6.json` and `*.pods.json`,
row throughput and response classes, the ingest pipeline stage by stage,
the buffer commit phases, seal, compaction, housekeeping, the pruning cases
from `*.prune.json`, and a table of every series. A counter the report does
not chart yet is listed at the end rather than dropped.

Sidecar files join the run **by timestamp, not by name**: a `*.k6.json`
belongs to the report when its `inserted_at` falls inside the sampling
window. Names drift between sweeps; timestamps do not.

Three rules keep a degrading cluster from producing fiction:

- A counter that falls is a pod restart. That interval leaves every ratio
  it touches, and the bucket is flagged. A restart during a missed scrape
  still counts — it is exactly the event worth seeing.
- An interval longer than the gap cut is a scrape the sampler missed. It
  leaves every series, and the bucket renders as a hole rather than a zero.
- A per-operation figure divides the summed time delta by the summed
  operation delta over the same intervals, so a slow pod carries its own
  weight. Buckets under `MIN_OPS` (default 20) operations are dropped: a
  pod that boots and dies inside one bucket otherwise reports a mean commit
  of several seconds.

Rates sum per-pod rates instead of dividing a tier delta by a wall clock,
so a partly scraped bucket reads honestly.

`BUCKET_S` sets the bucket width — 30 s for a run under 10 minutes, 60 s
otherwise. `MIN_OPS` sets the denominator floor. Assets live in
`scripts/report/report.css` and `scripts/report/report.js`; the generator
inlines them, so edit those files rather than the emitted HTML.

## Knobs

- **Load**: `VUS`, `DURATION_S`, `WARMUP_S`, `ROWS`, `SEED`; `MODE=rate RATE=30`
  for an open loop. `DAYS=30` spreads each row's `timestamp` uniformly over
  the last 30 days (default 0: one day, ordered by row), so a date bound
  prunes nothing — the pathological shape. `PARTITIONS=64` sets the
  table's write-partition count after the clustering (`PATCH
  {"partitions": N}`, raise-only, at most 64): 64 independent commit and
  seal streams for one table.
- **Row shape**: `SHAPE` (default `otel`, the 63-column body).
  `SHAPE=clickstack` generates the ClickStack logs layout — 15 scalar
  columns plus `resource_attributes`, `scope_attributes` and
  `log_attributes` as JSON objects, ~2,528 B/row — as
  `bodies/clickstack.<rows>.ndjson`, for the `clickstack_*` tables. One
  body serves both `clickstack_map_v2` and `clickstack_variant_v2`: a map
  stores a number as its text, a variant keeps the type. `log_attributes`
  carries an `ingest.stamp` key holding the same per-request stamp as
  `inserted_at`, so every request's attribute bags are distinct — without
  it the body's 3,062 rows repeat verbatim, Parquet dictionary-encodes the
  whole bag, and a variant filter runs on dictionary entries instead of
  rows (the `_v1` tables, 2026-08-27). `SHAPE=kv`
  generates small rows — `key`, `timestamp`, `value`, `inserted_at`, ~133 B/row
  — as `bodies/kv.<rows>.ndjson`, for table `kv_v1`. Pick `ROWS` so the body
  size stays comparable: 50,000 kv rows ≈ 6.4 MiB against the 6.87 MiB otel
  body.
- **Load spread**: the sweep resolves every ready endpoint of the
  `smolquery-api` Service and gives k6 the whole list. Each VU keeps one pod, so
  VUs spread across the api tier and connections stay alive. `API_POD` pins a
  run to a single pod — that was the accidental behavior of every sweep before
  2026-08-16, and it left extra api pods idle. The load generator cannot use the
  Service ClusterIP: it sits outside the cluster, and kube-proxy only balances
  from inside.
- **Drain gate**: every ingest VU point waits for the hot tier to empty
  before its warm-up. The gate polls a pruned query through the query API and
  reads `statistics.hot.filesTotal`. `DRAIN_WAIT_S` (default 300) bounds the
  wait; `DRAIN_POLL_S` (default 10) sets the poll tick. The wait lands in the
  point's `*.pods.json` as `drain_wait_s`, and the metrics database tags the
  period as its own phase, `drain-vusN`. On a timeout the sweep continues and
  records the leftover file count as `drain_hot_files_left`.
- **Bench selection**: `BENCHES` (default `ingest`). Pruning takes
  `PRUNE_DATES` (comma-separated `YYYY-MM-DD`, default today), `PRUNE_REPEATS`
  (default 3), `PRUNE_SETTLE_S` (default 30), `PRUNE_COLUMN` (default
  `duration_ms`) and `PRUNE_TIMEOUT_MS` (default 180000). Compaction takes
  `COMPACT_WATCH_S` (default 900).
- **attrs**: `ATTR_TYPE` (`map`, `variant` or `flat`; inferred from the
  table name when unset) picks the key-access syntax —
  `log_attributes['k']`, `log_attributes['k']::VARCHAR`, or the flattened
  column `k` with dots as underscores; `ATTRS_PROJECT` (default
  `proj_0617`) is the scoped project — the body repeats per request, so a
  project's rows are copies of its rows in the one body, and `proj_0617`
  is one whose seven rows include a `POST`, a 5xx, an exception and a
  slow-query body; `ATTRS_REPEATS` (default 3);
  `ATTRS_TIERS` (default `union,sealed`) — `sealed` waits for an empty hot
  tier first, `ATTRS_DRAIN_WAIT_S` (default 600) bounds the wait.
- **tail**: `TAIL_VUS` (default 4) query VUs, one project each from
  `TAIL_PROJECTS` (default `proj_0617,proj_0589,proj_0878,proj_0809`,
  projects with seven or more rows per body); `TAIL_DURATION_S` (default
  60) per phase; `TAIL_INGEST_VUS` (default `32`, a list runs one ingest
  phase per value); `TAIL_LIMIT` (100); `TAIL_ORDER` (`inserted_at` — the
  per-request stamp, so the real "last 100"; `timestamp` repeats per body
  and ties); `TAIL_COLUMNS` (default `timestamp, trace_id, span_id, body`);
  `TAIL_WINDOW_S` (default 300, 0 = no bound) — `AND <order> >= now - N s`,
  computed on the box per query; `TAIL_QUERY_SERVICE` (unset = the api
  pods that take the inserts) names another Service whose ready endpoints
  take the queries — the dedicated query pods, once they exist;
  `TAIL_SLEEP_S` (0) think time between queries; `TAIL_DRAIN_WAIT_S` (600).
- **explain**: every bench's `"explain": "analyze"` call has its own
  `EXPLAIN_TIMEOUT_MS` (default 120,000). A lost explain response used to
  cost the whole query timeout.
- **onebrc**: `ONEBRC_ROWS` (default 1,000,000,000) sizes the CSV;
  `ONEBRC_UPLOAD=false` skips the generate and upload steps and only times
  the queries against the rows already in the table;
  `ONEBRC_MAX_ROWS` (default 0 = all) caps the upload for a smoke run;
  `ONEBRC_WORKERS` (default 8) sets concurrent requests — a list
  (`"16 32 64"`) runs one full upload per value, each into a fresh
  `<TABLE>_w<N>` table, and stops after a point that lost rows or hit the
  deadline; `ONEBRC_QUERIES=false` skips the query cases and the sample
  rows, so a point is upload, drain and `count(*)` only;
  `ONEBRC_UPLOAD_DEADLINE_S` (default 0 = none) bounds one upload: past it
  the uploader cuts no more batches and stops retrying, and the summary
  carries `stopped_early`;
  `ONEBRC_ROWS_PER_REQUEST` (default 200,000, ~8.5 MB of NDJSON) sets the
  batch; `ONEBRC_POLL_S` (default 30) is the progress tick;
  `ONEBRC_DRAIN_WAIT_S` (default 1800) bounds the wait for an empty hot tier;
  `ONEBRC_QUERY_REPEATS` (default 10) and `ONEBRC_QUERY_TIMEOUT_MS` (default
  600000) shape the query phase. The CSV stays on the box under
  `/opt/bench/data/` and is reused when its `.done` marker matches
  `ONEBRC_ROWS`.
- **Metrics sampling**: `METRICS_INTERVAL_S` (default 10) sets the tick;
  `METRICS_DB` overrides the database path.
- **Body dates**: `BASE_DATE=YYYY-MM-DD` backdates the `timestamp` column.
  Unset, the body carries the date of the run that generates it. Use it to put
  rows in more than one date partition.
- **smolquery**: `FLUSH_MAX_BYTES`, `FLUSH_INTERVAL_MS`, `WRITE_POOL_SIZE`,
  `ENCODE_CONCURRENCY`, `WRITE_ENGINE_THREADS` (DuckDB threads per pool member;
  unset means they divide across the pool), plus `COMMIT_SIBLINGS` and
  `FLUSH_IDLE_INTERVAL_MS` on builds that carry the adaptive group-commit wait.
  `SEAL_ROW_GROUP_SIZE` sets `ROW_GROUP_SIZE` on every sealed Parquet write.
  Builds from 0.11.0 default it to 1,048,576 rows, up from 16,384. DuckDB holds
  a whole row group in memory, so a large value can exhaust the writer's 2 GB
  `memory_limit` and fail the seal.
- **ClickHouse durability**: `FSYNC=0`. Servers log to `/tmp/sqbench/`.
- **The SIGBUS crash**: the server dies in about a third of local runs, at any VU
  count. No knob avoids it. Check the server is alive after every run — see
  [The SIGBUS crash](#the-sigbus-crash).
- **remote arm**: `BASE_URL` (required — the cluster endpoint, port **8443**;
  port 443 is the web UI), `DATASET` (default `bench`), `TABLE` (default
  `otel_logs_v3`), `CLUSTERING`, `SCHEMA_FILE`, and `API_KEY`. With `API_KEY` unset it reads
  `SMOLQUERY_API_KEY` from `SECRETS_FILE`; `mise.toml` points that at the
  deploy repo's `secrets.env.json`. `TABLE` is authoritative: setup overrides
  the schema file's `id` with it. Pod sampling takes `KUBECONFIG` (required,
  set by `mise.toml`), `KUBE_CONTEXT`, `K8S_NAMESPACE` and `POD_SELECTOR`.
- **Run naming**: `LABEL_SUFFIX` tags a run, and `RESULTS` sends its JSON to
  another directory. Use both to keep an A/B out of the baseline raw files.
  Local arms default to `results/raw`; the remote arm defaults to
  `results/raw-remote`, so one remote run cannot flip the shared report into
  the remote table format.

## The SIGBUS crash

The smolquery server dies with `SIGBUS` / `KERN_PROTECTION_FAILURE` under
ordinary ingest. **There is no known workaround.** Roughly a third of runs at
1 VU die; 64 VU dies too.

**It is an out-of-bounds read, not a stack overflow, and not DuckDB.** All eight
crash reports from 2026-08-17 agree:

- The faulting thread is `erts_sched_N`, a **normal** scheduler.
- The ESR reads `(Data Abort) byte read` — a read.
- `sp` is a healthy thread stack, nowhere near the fault address.
- `far + 1` is the **exact start of a MALLOC region**, in 8 of 8.
- The PC lies in JIT-allocated memory, in **no loaded image** — not `beam.smp`,
  not `adbc_nif.so`, not the DuckDB driver.
- The instruction is `LDUR X7, [X9, #-1]`, then `LSR X7, X7, #8`.

BEAM JIT code reads the 8 bytes starting one byte before a heap buffer.

**The read is always out of bounds.** It only faults when the allocator places
the buffer at the very start of a VM region, leaving the previous page unmapped.
Every other run performs the same read and silently gets adjacent heap. A 33%
crash rate is not a 33% bug rate.

An earlier version of this section told you to set
`ERL_FLAGS="+sssdcpu 512 +sssdio 512"`. **That does nothing** — those flags size
*dirty* scheduler stacks, and every crash is on a normal scheduler. Do not use
it.

Tracked as T-286, which carries the full analysis.

**Know what a crash looks like**, because it does not look like a crash:

- k6 reports a flood of refusals. They are
  `dial tcp 127.0.0.1:4000: connect: connection refused`, not HTTP rejections.
- The tell is `data_sent_mib / requests`. A crashed run averages ~81 KB against a
  6.87 MiB body, because the failed requests never opened a connection.
- **The Elixir log shows nothing** — no error, no crash dump, no supervisor
  report. Diagnose from the macOS reports, and look in the `Retired/`
  subdirectory, where macOS moves them:

```sh
curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:4000/healthz   # 000 when dead
find ~/Library/Logs/DiagnosticReports -name 'beam.smp-*.ips' | sort | tail
```

Do not use `pgrep -f 'mix run --no-halt'` to check liveness from a shell script —
the pattern matches the script's own command line. Use the health endpoint.

The extracted evidence is in
[results/2026-08-17-sigbus-evidence.md](results/2026-08-17-sigbus-evidence.md).
The raw `.ips` reports are gitignored — they carry host paths and hardware
identifiers. Fourteen reports from 2026-08-10 carry the same signature, so the
bug predates every change made on 2026-08-17. Tracked as T-286.

## Fairness rules (equal across arms)

- Identical bodies from one deterministic `gen-bodies.exs` run.
- Cold table each run: data dir erased, server restarted.
- One server at a time; k6 shares the machine, and the watcher reports its CPU.
- Durability parity: `async_insert` off (default) plus `fsync_after_insert = 1`
  and `fsync_part_directory = 1`, to match how smolquery fsyncs before a 200.
  `FSYNC=0` is the weaker page-cache-only arm.
- Same sort key on both: `(project_id, timestamp)`.
- Per run: preflight (fails on `insertErrors`), 20 s warm-up, 15 s pause, 60 s
  measured, 5 s stop.
- On a 429 (`buffer_full`) the VU sleeps out `retry-after`, up to 2 s.
  Percentiles cover accepted requests; refusals count separately.

## How to read the smolquery VU curve

Group commit acks on the first trigger: 48 MiB (`FLUSH_MAX_BYTES`) or the window
timer. `TableBuffer` sets that timer **once**, when the window opens, and reads
`in_flight_inserts` at that instant only. Below `COMMIT_SIBLINGS` (5) the window
gets `FLUSH_IDLE_INTERVAL_MS` (5 ms). At or above it, the window gets
`FLUSH_INTERVAL_MS` (1,000 ms), so in practice only the byte trigger ends it.

That leaves three regimes, and the middle one is a dead band:

| load | what closes the window | cost |
|---|---|---|
| below ~5 VU | the 5 ms idle timer | one short window |
| **5 to ~32 VU** | **48 MiB only** | **wait for ~7 bodies to arrive** |
| above ~32 VU | 48 MiB, reached at once | none |

**The mid range is where smolquery loses to ClickHouse**, at about 0.46 of its
throughput at 8 VU. Cutting `FLUSH_MAX_BYTES` to 8 MiB raises 8 VU by 68% and
lowers 64 VU by 38%, so the default is not a wrong number — it is one constant
across two regimes. See
[results/2026-08-17-mid-range-flush-dead-band.md](results/2026-08-17-mid-range-flush-dead-band.md).

On builds before the adaptive wait, below ~5 VUs a closed loop never reached the
byte trigger, so p50 sat near 1 s — **the configured durability cadence, not a
ceiling**.

## Known asymmetries (denoted, not hidden)

| Dimension | smolquery | ClickHouse |
|---|---|---|
| What a 200 means | Manifest fsynced, rows queryable | Part fsynced, with the settings above |
| Row validation | Deferred to the flush; a failed batch is salvaged row by row | Every JSONEachRow row parsed inline |
| Nullability | Every column nullable | Every column except the two ordering keys, since MergeTree keys cannot be nullable — load-rig declared only 4, which favors ClickHouse |
| Timestamp parsing | ISO 8601 without a zone suffix (`2026-08-01T10:00:00.000000`), the one format both default parsers accept; the `basic` parser rejects a trailing `Z` | Same body, default `date_time_input_format=basic` |
| Platform | BEAM release on macOS | Linux-tuned binary on macOS |
| Tuning applied | `FLUSH_MAX_BYTES=48MiB`, `WRITE_POOL_SIZE=10`, `ENCODE_CONCURRENCY=10` (schedulers online), `MAX_BUFFERED_BYTES=128MiB` | Table-level fsync settings only |
| Date partitioning (`otel_logs_v3`) | None. The catalog has no partition key, so a date query prunes only by segment min-max statistics | `PARTITION BY toDate(inserted_at)`, a real partition key |
| Query statistics | `durationMs` and `totalRows` only — no files-read or bytes-read counter, so pruning is timed, not counted | `system.query_log` exposes `SelectedParts` and `ReadBytes` |

## The remote arm

The `remote` arm drives an already-deployed cluster. This harness does not own
that server, so three rules change.

- **No cold table.** The router exposes no table delete, so every run appends
  to the same table.
- **No process watcher.** `tools/watch` reads `ps` on this machine, so it sees
  k6 only. `scripts/watch-pods.exs` supplies the server side: one
  `kubectl exec` per pod reads `/sys/fs/cgroup/cpu.stat` once a second, because
  the sandbox cluster runs no metrics-server. 100% means one core.
- **The client can be the bottleneck.** A 6.7 MiB body over a home uplink caps
  throughput well below the server. Read `MiB/s` in the report before you read
  `rows/s`.

## The load generator

`scripts/loadgen.exs` runs k6 inside `eu-central-1`, because a laptop measures
its own uplink. One instance in the cluster's VPC carries the **EKS cluster
security group**, which permits traffic from itself, so it posts straight to the
api pod IP over plain HTTP:

```
http://<api-pod-ip>:4000/v1/datasets/bench/tables/otel_logs_v2/insert
```

No Cloudflare, no NLB, no TLS, no internet hop, and no new security-group rule.

Access is SSM Session Manager — no public IP, no SSH key, no inbound rule. Code
goes up as an inline base64 tarball (~10 KB) rather than a `git clone`, so a run
always ships the working tree. Results come back the same way (~4 KB). Both sit
well inside SSM's limits, so this needs no S3 bucket. The api key moves through
SSM Parameter Store, not command text, because SSM keeps command history. Each
sweep starts with a stamped preflight insert — k6 discards response bodies, so
it cannot see per-row rejections — and clears the box's results directory, so
`fetch` stays under the inline output limit.

`k6` and `genbody` run on the box; the body is rebuilt there from seed 42, so it
is byte-identical without shipping 6.87 MiB. Pod sampling stays on this machine,
because it needs the kubeconfig.

Knobs: `LOADGEN_INSTANCE_TYPE` (default `c7i.2xlarge`), `LOADGEN_ROOT_GIB`
(default 64, a gp3 root at 500 MiB/s — the 1BRC CSV alone is 13.8 GB),
`LOADGEN_SUBNET`, `VUS_LIST`, `DURATION_S`, `WARMUP_S`, `PAUSE_S`, `RESULTS`.

### The one billion row challenge

`mise run bench-onebrc` answers two questions about the deployed cluster: how
fast it takes a billion rows, and how fast it answers the
[1BRC](https://github.com/gunnarmorling/1brc) query over them. It follows the
challenge in spirit — 413 stations with the original means, Gaussian
temperatures to one decimal, the same query — not its rules.

1. `tools/gen1brc` writes `measurements.<rows>.txt` on the box:
   `station;temperature` lines, seeded, generated in parallel and written in
   order, so a rerun reads the same bytes. It stays on disk as the source.
2. `tools/upload1brc` streams the CSV as NDJSON `POST …/insert` requests,
   `ONEBRC_WORKERS` at a time, round-robin across the api pods. Every batch
   carries an `insertId`, so a retry after a 429, a 5xx, a timeout or a
   dropped connection cannot double-count rows. It runs under `nohup`,
   because SSM caps one command at an hour; the sweep polls its progress
   JSON and prints rows/s as it goes. The summary lands in
   `<label>.upload.json`.
3. The sweep waits for the hot tier to drain, then runs the 1BRC aggregate
   — `min`, `avg`, `max` per station, ordered by station, **without** a
   `round()` on the mean: a function wrapping an aggregate refuses the
   distributed decomposer (`decomposer.ex`, `unsupported_aggregate_shape`),
   so rounding belongs to presentation, as in the challenge —
   `ONEBRC_QUERY_REPEATS` times with default options, again with
   `"distributed": false`, and `count(*)` as the floor. Every wall time lands
   in `<label>.onebrc.json`, with one `"explain": "analyze"` per case.

`rows_accepted` in the upload summary comes from the server's
`insertedRows`, not from the client's count. `count(*)` after the drain
should match it exactly; a gap is a finding. The summary also carries
`client_cpu_s` and `client_cores`, the uploader's own CPU from `getrusage`:
a point whose `client_cores` nears the box's vCPU count measured the box,
not the cluster.

### Attributes: `MAP(STRING, STRING)` vs `VARIANT` vs flat columns

`mise run bench-attrs` compares the two semi-structured column types
smolquery gained in `main@43f1b30` (T-140, T-392) against the 63 flattened
columns, on the ClickStack logs layout
(<https://clickhouse.com/docs/clickstack/ingesting-data/schemas#logs>)
with a `project` column first in the clustering key. One sweep per arm:

```sh
export VUS_LIST="8 32"
SHAPE=clickstack TABLE=clickstack_map_v2     LABEL_SUFFIX=-attrs-map     mise run bench-attrs
SHAPE=clickstack TABLE=clickstack_variant_v2 LABEL_SUFFIX=-attrs-variant mise run bench-attrs
SHAPE=otel       TABLE=otel_logs_v34         LABEL_SUFFIX=-attrs-flat    mise run bench-attrs
```

Each sweep runs the ingest points, then the `attrs` query cases twice:
over hot ∪ sealed right after the last point, and over sealed alone after
the drain. Read three things per arm: rows/s and MiB/s from `*.k6.json`
(the clickstack body is 7.5% larger per row than the flat one), bytes per
sealed row from `smolquery_seal_segment_bytes_total` over
`smolquery_seal_segment_rows_total` in the metrics database, and the query
cases from `*.attrs.json`. **Neither type has statistics bounds, so a key
filter never prunes** — only the `project` predicate can, and the plan says
whether it did. A `VARIANT` column is JSON text on disk, parsed per scanned
row, so the unscoped scan cases are where it pays.

### The tail query under ingest

`mise run bench-tail` is the shape of the product's day: rows stream in
while several projects ask for their last 100 events. `k6/tail.js` runs
one VU per project in a closed loop against the api pods, each iteration
alternating `options.distributed` true and false:

```sql
SELECT timestamp, trace_id, span_id, body
FROM bench.clickstack_map_v2
WHERE project = 'proj_0617' AND inserted_at >= TIMESTAMP '<now - TAIL_WINDOW_S>'
ORDER BY inserted_at DESC LIMIT 100
```

`TAIL_COLUMNS` picks the projection and `TAIL_WINDOW_S` (default 300) the
time bound, computed on the box per query; `TAIL_WINDOW_S=0` drops the
bound. The first run (2026-08-27) selected six columns including the
attribute map and had no bound — see the write-up for what each change
bought.

Three phases, `TAIL_DURATION_S` each: `tail-idle` with no ingest, one
`tail-ingest-vus<N>` per value in `TAIL_INGEST_VUS` (the insert k6 runs
its warm-up, then the tail k6 is detached on the box while the measured
insert runs), and `tail-sealed` after the hot tier drains. Each phase
writes wall and `durationMs` percentiles per mode, rows returned, shards,
hot files seen, and errors. Read wall p50 and p99 per mode per phase, and
whether every query returned `TAIL_LIMIT` rows.

```sh
SHAPE=clickstack TABLE=clickstack_map_v2 TAIL_INGEST_VUS="8 32" TAIL_DURATION_S=90 LABEL_SUFFIX=-tail mise run bench-tail
```

`mise run bench-onebrc-ingest` answers a third question: **at what
concurrency does the upload stop scaling?** It runs the same upload once per
value in `ONEBRC_WORKERS`, each into its own `<TABLE>_w<N>` table, with no
query phase. Every point still waits for the hot tier to drain and checks
`count(*)` against `rows_accepted`. A 600 s deadline bounds a collapsed
point, and the sweep stops after a point that lost rows or hit the
deadline. Read rows/s, then 429s and other retries, then p99 against p50,
then 5xx, `rows_failed` and pod restarts, in that order.

The instance is not Terraform-managed. It is found by tag
`Name=smolquery-bench-loadgen`, and `mise run bench-down` terminates it.

## The TSBS arm

TSBS runs in two places. `mise run bench-tsbs` runs it against the deployed
cluster from the load generator. `scripts/tsbs.exs` runs it against one node
on the same machine. Both write `results/raw-tsbs/s<scale>-<label>.*`, and
`mise run tsbs-report` reads both.

### On the cluster

The cluster serves the edge on port 8428: the api pods take remote write and
the query pods answer MetricsQL. Every sample goes to `metrics.samples`, the
table vmagent also writes. The edge has no table choice per request.

```sh
BASE_URL=https://<sandbox-host>:8443 TSBS_LABEL=before mise run bench-tsbs
# after the next image rolls out: query the same data again
BASE_URL=https://<sandbox-host>:8443 TSBS_LABEL=after TSBS_PHASES=query mise run bench-tsbs
LABELS="before after" SCALE=1000 mise run tsbs-report
```

- The load runs once. The query phase runs once for each build. Both builds
  then read the same rows, so their answers compare one to one.
- Each scale gets its own window, one after the other, `TSBS_HOURS` (24) long
  and ending at midnight UTC on the day of the first run.
  `results/raw-tsbs/windows.json` keeps the windows, so a later run and a new
  box generate the same queries. Scale 100 and scale 1,000 share host names,
  so their windows must not overlap.
- `tools/tsbsrw` writes round-robin to the api pod IPs. The query runner reads
  round-robin from the query pod IPs.
- The drain waits until the hot tier is back to its file count from before
  the load, plus `TSBS_DRAIN_SLACK` (8). vmagent keeps writing, so the hot
  tier is never empty.
- `tools/tsbsdigest` hashes the first `TSBS_VERIFY` (3) answers of each query
  type on the box. SSM returns about 24 KB for each call, and one scale 1,000
  answer is megabytes.
- Knobs: `TSBS_SCALES` (`100 1000`), `TSBS_PHASES` (`load,query`),
  `TSBS_LABEL` (default `sq-<live sha>`), `TSBS_QUERIES` per type (50),
  `TSBS_QUERY_WORKERS` (`1 4`), `TSBS_QUERY_TYPES`, `TSBS_LOAD_WORKERS` (8),
  `TSBS_LOAD_SAMPLES` (10,000), `TSBS_DRAIN_WAIT_S` (1800), `TSBS_REF`.

### On one node

`scripts/tsbs.exs` runs the Time Series Benchmark Suite (TSBS) `cpu-only`
workload against one smolquery node's VictoriaMetrics edge. It compares two
or more smolquery builds on the same generated data. Run it on the machine
that runs the node, the dev box. That machine needs Go, Elixir and a
smolquery clone at `SQ_REPO` (default `~/smolquery`).

The data goes in through the edge's remote write path. TSBS's own loaders do
not fit. `tsbs_load_victoriametrics` sends Influx line protocol, which the
edge does not take. `tsbs_load_prometheus` names a series by its field alone
(`usage_user`), so no TSBS query matches it, and it panics on a 429.
`tools/tsbsrw` reads the `victoriametrics` data file instead. It names each
series `<measurement>_<field>` as VictoriaMetrics does. It retries a 429 or
503 after its `retry-after`, as vmagent does.

```sh
mise run tsbs-setup                          # TSBS at TSBS_REF, tsbsrw, into ~/tsbs/bin
mise run tsbs-build -- before origin/main    # prod build, worktree ~/tsbs/sq-before
mise run tsbs-build -- after origin/t-586-whole-pushdown
SCALE=100 mise exec -- elixir scripts/tsbs.exs gen
SCALE=100 mise exec -- elixir scripts/tsbs.exs run before
SCALE=100 mise exec -- elixir scripts/tsbs.exs run after
SCALE=100 mise run tsbs-report
```

- `run` starts the build on an empty data directory and creates
  `metrics.samples`. It loads the data file, then waits until the buffer
  holds no unsealed entry (`DRAIN_TIMEOUT_S`, default 1800). It keeps the
  first `VERIFY_QUERIES` answers of each query type, runs every type at each
  of `QUERY_WORKERS` (default `1 4`), then stops the node.
- Only 11 query types have a PromQL form in TSBS: `single-groupby-*`,
  `cpu-max-all-*` and `double-groupby-*`. The others panic in the generator.
- A query type whose answer is not a 200 stops at once, because TSBS panics
  on it. The report shows the refusal, for example a 422 past
  `SMOLQUERY_VICTORIAMETRICS_MAX_SAMPLES`.
- `report` compares the kept answers across builds, to 1e-9.
- Knobs: `SCALE` (100), `HOURS` (24, at least 13 for `double-groupby`),
  `TS_END` (this hour), `QUERIES` per type (100), `LOAD_WORKERS` (8),
  `LOAD_SAMPLES` per request (10,000), `LABELS` (`before after`), and
  `API_PORT`, `HOT_PORT`, `METRICS_PORT`, `VM_PORT`.
- The node runs `MIX_ENV=prod` with the cluster's seal and memory tuning. It
  runs the roles api, ingest, buffer, storage, query and victoriametrics. Any
  `SMOLQUERY_*` or `CATALOG_DATABASE_URL` in the shell passes through.
- Keep ports 4000 to 4003 free. The storage and query roles reach the hot
  tier on 4001 even when `HOT_PORT` moves the listener.

## Scatter and query admission

`scripts/scatter_admission.sh` (driver: `scripts/scatter_admission.py`) tests
whether a distributed (scattered) query gets around `max_concurrent_jobs`. It
boots one local node from `SMOLQUERY_DIR` (compiled with `MIX_ENV=prod`),
loads 400 single-insert files into `bench.events`, then runs 8 closed-loop
clients for 60 s per arm, with `"distributed": false` and `true`, on a
high-cardinality and a low-cardinality GROUP BY. It samples the beam process
every 250 ms and reads `smolquery_query_engines_total` before and after.
The result is in
[results/2026-10-25-scatter-admission-overload.md](results/2026-10-25-scatter-admission-overload.md).

```sh
SMOLQUERY_DIR=~/dev/sq/smolquery mise run scatter-admission -- all   # load + 4 arms + tables
mise run scatter-admission -- arms                                   # arms only, RUN_DIR keeps the data
mise run scatter-admission -- serve                                  # boot over RUN_DIR and wait
```

Give an aggregate an alias and `ORDER BY` the alias. A positional
`ORDER BY 2` is refused by the decomposer and the query silently answers from
one engine, so the "distributed" arm would measure nothing. The driver aborts
when the probe's `job.scatter` is not set on the distributed arm.

## Layout

```
k6/tail.js            the last-100-events query loop, one VU per project, distributed alternating
tools/genbody/        deterministic OTel NDJSON generator: 63 flat columns, kv, or the ClickStack layout
tools/gen1brc/        the 1BRC measurements file generator, 413 stations, seeded
tools/upload1brc/     streams a measurements file as idempotent NDJSON inserts
tools/tsbsrw/         loads a TSBS line-protocol file through Prometheus remote write
tools/tsbsdigest/     one hash per TSBS answer, to compare builds without moving the JSON
tools/watch/          CPU and RSS sampler, ps-based, for the server and k6
k6/insert.js          load script, closed loop (VUS) or open loop (RATE)
schemas/              smolquery table-create JSON, ClickHouse MergeTree DDL
schemas/onebrc_stations.sql  the 413 stations and means, generated from tools/gen1brc
scripts/              setup, run, sweep, stop, report, watch-pods, watch-metrics, loadgen
scripts/scatter_admission.sh  one-node scatter vs admission test, with scatter_admission.py
scripts/tsbs.exs      TSBS cpu-only against the VictoriaMetrics edge, per smolquery build
scripts/report_html.exs  the HTML run report generator
scripts/report/       its CSS and JS, inlined into every report
mise.toml             every workflow as a task — `mise tasks`
results/raw/          k6 and watch JSON per run (gitignored)
results/raw-remote*/  the same, plus *.pods.json, for the remote arm
results/raw-loadgen/  the in-region load generator's runs, plus *.metrics.sqlite3 per sweep
results/raw-tsbs/     the TSBS load and query runs (gitignored)
results/raw-scatter/  the scatter admission arms, JSON and 250 ms samples (gitignored)
results/*.md          dated baseline writeups
results/*.html        one charted report per load test, beside its writeup
```

Every result file carries `inserted_at`, an ISO 8601 UTC timestamp written when
the run finishes — `*.k6.json`, `*.watch.json` and `*.pods.json` alike. Files
that predate the field were backfilled from their modification time, which is
when the run wrote them.

## The `inserted_at` column

Every inserted row also carries `inserted_at`, the send time. `gen-bodies.exs`
writes a fixed-width placeholder, `____INSERTED_AT___________`, and whoever
posts the body substitutes the real time — `k6/insert.js` per request, and the
preflight in `scripts/bench.exs`. One value per request, shared by that
request's rows, because a batch lands at one instant.

The format is `2026-08-15T01:13:25.349000`: zone-less microseconds, matching
the other timestamp columns. **A trailing `Z` fails**, since smolquery's
`basic` parser and ClickHouse's `date_time_input_format=basic` both reject it.
The placeholder is exactly as wide as the timestamp, so body size stays
constant at 6.87 MiB.

### How k6 writes the stamp

`k6/insert.js` finds every placeholder offset once, at init, then writes the
26 bytes in place per request:

```js
for (let k = 0; k < stampOffsets.length; k++) bodyBytes.set(stampBytes, stampOffsets[k]);
```

It posts the same `ArrayBuffer` every time. Each VU runs the init context on its
own, so the buffer it patches is its own.

**Do not go back to a string rewrite.** Until 2026-08-17 the script ran
`rawBody.split(PLACEHOLDER).join(nowIso())`, which allocated a fresh 6.87 MiB
string per request per VU. At 64 VU that cost k6 134–158% CPU and 4.2–6.5 GB of
RSS, against 45–57% and ~1.9 GB unstamped. On a laptop that shares ten cores
with the server, it cut both arms by 37–39%. The byte patch writes ~80 KB
instead of 6.87 MiB and keeps the real per-request time.

**The stamp resolves to a millisecond, not a microsecond.** k6 exposes no
high-resolution clock — `performance` is undefined — so `nowIso()` reads
`Date.now()` and pads three zeros. Concurrent VUs that post inside the same
millisecond therefore share a value. The digits are real; the last three are
always zero.

Baselines from before this column carry no stamp at all. Do not compare them
with stamped runs at high VU counts.

Adding the column changed the schema, and smolquery answers 409 on a
`CREATE TABLE` whose schema differs from what exists — there is no
`add_column` route. Each schema change therefore gets a new table. The older
`bench.otel_logs` keeps its rows and has no `inserted_at`.

## The tables

| table | ClickHouse | smolquery | notes |
|---|---|---|---|
| `otel_logs` | `ORDER BY (project_id, timestamp)` | clustering `[project_id, timestamp]` | no `inserted_at` |
| `otel_logs_v2` | same | same | adds `inserted_at` |
| `otel_logs_v3` | **`PARTITION BY toDate(inserted_at)`**, `ORDER BY (project_id, timestamp)` | clustering `[project_id]` | the default |
| `otel_logs_v4` | same as v3 | clustering `[project_id, timestamp]` | same 63 columns as v3; a fresh table to measure compaction under the 2026-08-17 prod code |
| `otel_logs_v5`–`v7` | same as v3 | clustering `[project_id, inserted_at]` | successive fresh tables for the 2026-08-17 compaction runs — the router has no table delete, so each run that needs an empty table gets a new one |
| `otel_logs_v8` | same as v3 | clustering `[project_id, inserted_at]` | the 2026-08-18 seal and compaction bench, the first with metrics sampling |
| `otel_logs_v9` | same as v3 | clustering `[project_id, inserted_at]` | a fresh table for the 2026-08-19 batch-size runs — `otel_logs_v8` wedged on a claim its replica could not accept, so its hot tier stopped draining |
| `otel_logs_v10` | same as v3 | clustering `[project_id, inserted_at]` | a fresh table for the 2026-08-20 group-commit tuning — `otel_logs_v9`'s base partition ref wedged on a 488-segment claim the merge path cannot complete (T-335) |
| `otel_logs_v11`–`v19` | same as v3 | clustering `[project_id, inserted_at]` | the 2026-08-20 seal-parity runs (T-333). `v11` carries a retrying compaction failure (T-343) — do not reuse it |
| `otel_logs_v20`–`v32` | same as v3 | clustering `[project_id, inserted_at]` | one fresh table per run: the 2026-08-20 partition sweep, then the 2026-08-21 memory sweep |
| `otel_logs_v33` | same as v3 | clustering `[project_id, inserted_at]` | the 2026-08-21 96-VU parity run — the current record, and the current table |
| `kv_v1` | `PARTITION BY toDate(inserted_at)`, `ORDER BY (key, inserted_at)` | clustering `[key, inserted_at]` | 4 columns (`key`, `timestamp`, `value`, `inserted_at`), ~133 B/row — the small-row bench, `SHAPE=kv` |
| `onebrc_v1` | `ORDER BY (station)` | clustering `[station]` | 2 columns (`station` STRING, `temperature` FLOAT64), no `inserted_at`, ~42 B/row as NDJSON — the one billion row challenge, `BENCHES=onebrc`. **Holds 1,246,393,006 rows since 2026-08-25**, after an aborted launch — not a 1B table any more |
| `onebrc_v2` | same | same | the 2026-08-22 16-worker upload |
| `onebrc_v3_w<N>` | same | same | one table per point of the 2026-08-24 worker sweep, `mise run bench-onebrc-ingest`; any `onebrc*` table takes the `onebrc_v1` schema and clustering |
| `onebrc_v4` | same | same | the 2026-08-24 48-worker follow-up, ingest only |
| `onebrc_v5` | same | same | the 2026-08-25 48-worker re-check on `main@18117bb`, with the query phase — exactly 1B rows |
| `onebrc_v6` | same | same | the 2026-08-26 48-worker re-check on `main@1a6a543` (F-1 and F-2 fixes), with the query phase — exactly 1B rows |
| `otel_logs_v34` | same as v3 | clustering `[project_id, timestamp]` | the flat control for the 2026-08-27 attributes bench (T-393): the same 63 columns, fresh |
| `clickstack_map_v1` | `ORDER BY (project, timestamp)` | clustering `[project, timestamp]` | the ClickStack logs layout, snake_case, plus `project` and `inserted_at`: 15 scalar columns and `resource_attributes`, `scope_attributes`, `log_attributes` as `MAP(STRING, STRING)`; `SHAPE=clickstack` |
| `clickstack_variant_v1` | same, attributes as `JSON` | same | the same columns with the three attribute bags as `VARIANT` |
| `clickstack_map_v2`, `clickstack_variant_v2` | same | same | the same schemas, fed the stamped body (`log_attributes['ingest.stamp']` varies per request) — the tables to compare; `_v1` hold the unstamped, dictionary-compressed rows |
| `clickstack_map_p64` | `PARTITION BY (project, toDate(timestamp))` | same, `partitions: 64` | the 2026-08-27 pathological table: 64 write partitions and `DAYS=30` timestamps — the nearest smolquery analog of a per-project, per-day partition key |

`otel_logs_v3` is what `TABLE` defaults to. It exists to answer one question:
does a query for a single date read only that date's files?

**The two arms do not answer it the same way.** ClickHouse has a real partition
key, so it prunes whole partitions. smolquery has no partition key at all —
`lib/smolquery/catalog.ex` offers `retention` and `clustering` and nothing else
— so it prunes by segment min-max statistics, which works only while segments
stay date-contiguous. Never publish a pruning comparison that reads as
like-for-like. Say which mechanism produced each number.

`inserted_at` is non-nullable on the v3 ClickHouse DDL. A nullable partition key
collects NULLs in a partition of their own.

## Reference numbers

TSBS through the VictoriaMetrics edge, 2026-09-24 (`main@f4303d0`, the
pushdown stack): **every `cpu-only` query type answers at scale 100 in 0.7 to
1.8 s; at scale 1,000 `double-groupby-5` times out at 4 workers and
`double-groupby-all` OOM-kills a 3 GiB query pod.** Load: 40k samples/s at 8
writers, 77k at 32. A floor of about 0.7 s per query dominates the light
types; every sealed file holds every metric name, so no `name` predicate skips a file
([results/2026-09-24-tsbs-victoriametrics.md](results/2026-09-24-tsbs-victoriametrics.md)).

The one billion row challenge, 2026-08-22: **1,000,000,000 two-column rows in
113.3 s (8,825,572 rows/s, 352 MiB/s)** at 16 workers with zero refusals and
zero restarts, `count(*)` exact; the 1BRC aggregate over them in **10.7 s
median** distributed across the three api pods (the query role lives there,
not on the storage tier), 27.5 s single-engine, `count(*)` 2.0 s. See
[results/2026-08-22-one-billion-rows.md](results/2026-08-22-one-billion-rows.md).
The worker curve, 2026-08-24: peak **10,008,571 rows/s at 64 workers**, knee
at 32 ([results/2026-08-24-onebrc-ingest-worker-sweep.md](results/2026-08-24-onebrc-ingest-worker-sweep.md)).
Re-checked 2026-08-25 on `main@18117bb` at 48 workers: 9,649,356 rows/s,
10.80 s aggregate. That run also read the first S3 request timings: **82
`put` requests for the whole billion, 178 ms and 15.8 MB each, 5.5% of seal
time** ([results/2026-08-25-onebrc-v5-and-s3-timings.md](results/2026-08-25-onebrc-v5-and-s3-timings.md)).
Re-checked again 2026-08-26 on `main@1a6a543` (the F-1 and F-2 replication
fixes): 9,703,402 rows/s in 103.1 s, 11.30 s aggregate, 78 puts at 188 ms —
the fixes cost nothing on a clean upload
([results/2026-08-26-onebrc-v6-post-f1-f2.md](results/2026-08-26-onebrc-v6-post-f1-f2.md)).

Attribute bags, 2026-08-27 (T-393, `main@43f1b30`): **`MAP(STRING, STRING)`
is the type to recommend.** With per-row unique values in the bag, every
query naming a `VARIANT` column over 7.9M rows dies on the 1 GB engine
limit (DuckDB `Out of Memory`, 128 MiB chunk); the map answers the same key
filter in 2.8 s, flat columns in 1.5 s. Ingest at 32 VUs: variant 148k
rows/s, flat 132k, map 102k (the map re-encodes every row). Clustering by
`project` prunes row groups, never files
([results/2026-08-27-clickstack-map-vs-variant.md](results/2026-08-27-clickstack-map-vs-variant.md)).

The tail query, 2026-08-27: **the last 100 events for one project in
0.61 s idle and 0.68–0.72 s p50 while 32 VUs push 90k rows/s** into the
same map table, on the dedicated query pods with `SMOLQUERY_WARM_ENGINES=4`,
a four-column projection and a 5-minute window; four projects back to back
1.3 s idle and 1.7–1.9 s under ingest (p95 3.9–4.3 s). The morning's
baseline on the api pods with the wide query was 1.59 s / 2.97 s for one
project and 3.2 s / 5.1–5.4 s for four. `distributed` is a no-op
(`ORDER BY … LIMIT` never scatters). What remains under ingest is the
hot-tier fetch, ~300 segment requests per query (T-400); what remains at
four queriers is the per-job path itself, 1.16 s on an empty window.
With T-400's Top-N bound (`main@caaf43c`): p95 under ingest down 14–26%,
p99 down 20–28%, p50 up 0.1–0.5 s, the buffer pods' bill per query down
2.4–4× — the probe's second round opens the whole hot tier at the default
`SMOLQUERY_TOP_N_PROBE_ROWS` of 1,000,000. At 100,000 the probe opens a
quarter of that and one querier's p95 drops to 1.21 s, but the median stays
0.1 s (one querier) to 0.4 s (four) above pre-T-400: the probe's two engine
round trips, not its breadth, are the cost (T-403). A 1 s flush window
(`FLUSH_IDLE_INTERVAL_MS` 300 → 1000) gave the best under-ingest tail yet —
one querier 0.69 / 1.1 / 1.2 s p50 / p95 / p99 — but not from bigger commits
(the 94 MB cap binds at ~10 bodies); the closed loop slowed 24% on a +0.5 s
ack, and the hot tier shrank with it
([results/2026-08-27-tail-under-ingest.md](results/2026-08-27-tail-under-ingest.md)).

The pathological table, 2026-08-27: `partitions: 64` and 30 days of
timestamps on the same shape. **Ingest 111,836 rows/s, and the tail query
under it p50 7.2–7.5 s, p95 21–22 s**, ~650 hot files per query, a 269 s
drain, 128 seals at 20 B/row, and a one-day `timestamp` bound reading every
file. `partitions` is seal fan-out, not layout; out-of-order event time
defeats date pruning
([results/2026-08-27-partitions-64.md](results/2026-08-27-partitions-64.md)).

load-rig, on an M1 Pro with 10 cores and 16 GB: smolquery peaked at **383,157
rows/s** (32 VU, pool=4, enc=4), ClickHouse at **165,814 rows/s** with matching
fsync. Land in that range, and **investigate a large gap before you publish**.

Deployed sandbox cluster (3× m7i.xlarge plus a dedicated m7i.large for the api
pod, `SMOLQUERY_WRITE_PARTITIONS=3`, table `otel_logs_v2`): **22,579 rows/s**
at 16 VUs, flat to 64 VUs with zero refusals. Leave the partition count unset
and it defaults to 1, so one buffer pod owns the whole table and throughput
halves past 16 VUs. The laptop's uplink — a noisy 42–51 MiB/s — caps every
number past 8 VUs, so the cluster's real ceiling is still unknown; it never
exceeded 166% of its 1,370% CPU. Current baseline:
[results/2026-08-15-remote-v2-baseline.md](results/2026-08-15-remote-v2-baseline.md);
how we got there:
[results/2026-08-14-remote-sandbox.md](results/2026-08-14-remote-sandbox.md).

The 2026-08-18 `otel_logs_v8` run measured sealing and compaction under
ingest: 175,511 rows/s at 16 VUs, every row sealed, the hot tier empty ten
minutes after ingest — and one storage pod OOMKilled during the seal drain.
Two pre-existing corrupt segments poison the v6 and v7 compaction loops. See
[results/2026-08-18-v8-seal-compaction.md](results/2026-08-18-v8-seal-compaction.md).
The same evening, after the T-304 partition release, a repeat run measured the
seal skew falling from 12/79/9 to 25/36/39 across the storage pods at the same
throughput — and one more OOM, concurrent with a poison compaction:
[results/2026-08-18-v8-post-t304-rebench.md](results/2026-08-18-v8-post-t304-rebench.md).
On smolquery 0.13.0, with the corrupt v6/v7 segments remediated, the third v8
run came back clean: **200,253 rows/s** at 16 VUs, seal skew 32/32/35, zero
errors, zero restarts, no OOM. A 4-point VU sweep the same night put the peak
at 16 VUs and found the ceiling: the buffer pods pin their RSS at the 4,096 MB
container limit at 32–64 VUs, admission sheds load with 429s, and throughput
falls to 107k rows/s at 64 VUs — with zero OOMs.

**Those are burst numbers.** A 30-minute soak at the same 16 VUs collapsed the
buffer tier after three minutes: every buffer pod OOMKilled and entered a
boot–commit–die loop, and the tier recovered only when the load stopped. The
post-load drain then OOMKilled a storage pod and stalled with 458 hot files
unsealed. The first sustained number arrived on 2026-08-20: **38,610 rows/s
for 600 s** at 1 VU with **20,000 rows per insert**, zero restarts and sealing
at parity — see
[`results/2026-08-20-batch-size-and-heap-garbage.md`](results/2026-08-20-batch-size-and-heap-garbage.md).
It holds at that batch size and not at 3,062 rows, so the batch size is part of
the number. See
[results/2026-08-19-v8-soak-collapse.md](results/2026-08-19-v8-soak-collapse.md). The same run showed that the timed pruning
cases measure a fixed ~1.3–4 s per-query floor (engine start plus hot-tier
manifest fetches), not pruning — the engine plans the empty date to
`EMPTY_RESULT` in 6.4 ms:
[results/2026-08-19-v8-0-13-0-rebench.md](results/2026-08-19-v8-0-13-0-rebench.md).

The 2026-08-20/21 tuning sessions then found the cluster's real ceiling: the
merge engine's memory budget. Raising `SMOLQUERY_STORAGE_MEMORY_LIMIT` from
2048 MiB to 3584 MiB tripled sealed throughput at identical settings. The
current record is **254,163 rows/s at 96 VUs with sealing at parity** —
`seal:commit` 0.990, 251,621 rows/s sealed, zero refusals, ordinary 6.87 MiB
requests, on `otel_logs_v33`. That is 3.7x the 2026-08-20 morning baseline,
in a 3-minute window, not a soak. The winning shape is 3 write partitions ×
8 live claims. The running state and the full write-up index live in
[results/README.md](results/README.md); the write-up is
[results/2026-08-21-memory-is-the-seal-ceiling.md](results/2026-08-21-memory-is-the-seal-ceiling.md).

Driven from an in-region load generator instead, the same cluster reached
**127,111 rows/s**. What scaling that to 1M rows/s would cost is modelled in
[results/2026-08-15-cost-model-1m-rows-per-second.md](results/2026-08-15-cost-model-1m-rows-per-second.md).
A 2026-08-16 re-sweep OOMKilled a buffer pod instead of producing a baseline
(the T-245 admission bug). The same run failed 45% of its compaction attempts
against the 1 GiB pin budget:
[results/2026-08-16-loadgen-t245-oomkill.md](results/2026-08-16-loadgen-t245-oomkill.md).
The fixes for both (PR #166, #167, #169) then deployed. The next sweep reached
113k rows/s and killed three pods:
[results/2026-08-16-post245-sweep.md](results/2026-08-16-post245-sweep.md).
`otel_logs_v3` then landed, and its first sweep found that every run in this
repo's history had sent all its load to one api pod:
[results/2026-08-16-v3-first-sweep.md](results/2026-08-16-v3-first-sweep.md).
**Every number measured before 2026-08-16 15:39 UTC is single-api-pod.**

Latest local numbers, smolquery 0.11.0 on 2026-08-17: **44,016 rows/s at 1 VU**
against ClickHouse's 33,324, and a tie at 64 VU (290,753 against 290,385). Both
arms lost 37–39% at 64 VU against the 2026-08-10 baseline, because the
`inserted_at` stamp roughly tripled k6's own CPU. Read the 64 VU rows as a
measurement of the laptop:
[results/2026-08-17-local-rebench-0-11-0.md](results/2026-08-17-local-rebench-0-11-0.md).

The full local baseline:
[results/2026-08-10-baseline.md](results/2026-08-10-baseline.md). ClickHouse runs
well above its reference here, because fsync costs ~3% on this hardware against
54% on the reference setup. The low-VU floor above is what smolquery
[PR #124](https://github.com/chasers/smolquery/pull/124) removes — see
[results/2026-08-11-pr124-adaptive-group-commit.md](results/2026-08-11-pr124-adaptive-group-commit.md).
Builds from 0.11.0 ship `commit_siblings: 5` by default, so the floor is gone
without a flag.
