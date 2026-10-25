# Scatter and query admission on one node

**Verdict: partly confirmed.** Admission counts client jobs only, and a
scattered job starts four more DuckDB engines behind that one slot. With 8
concurrent scattered jobs the node ran **5.0 engines per job (40 in total)**
against 1.0 (8) for the same load with `"distributed": false`, and it paid for
it in CPU: **throughput fell 44 to 49% and median latency rose 64 to 80%**. The
memory half of the hypothesis did not show up. Peak RSS was the same in both
arms (+3% and +10%), because these scans never came near the 1 GB per-engine
limit. Nothing failed, nothing was refused, and no 429 appeared in any arm.

## Setup

| | |
|---|---|
| smolquery | `e7913f17469cbd3a5d395bd73ab8f346f221f751` (main, `0.28.0`), `MIX_ENV=prod`, built from a clean `deps.get` |
| Runtime | Erlang/OTP 29.1, Elixir 1.20.4 (Homebrew) |
| Machine | Apple Silicon Mac, 12 cores, 24 GiB. **Other work was running**: `uptime` load averages of 6.4 to 9.2 before the arms, see the table below |
| Node | one node, roles `api,ingest,buffer,storage,query`, local segment store, SQLite catalog, no clustering |
| Query settings | all defaults: `max_concurrent_jobs` 8, `job_memory_limit` 1GB, `SMOLQUERY_DISTRIBUTED_LOCAL_WORKERS` 4, `SMOLQUERY_DISTRIBUTED_MIN_FILES` 8, DuckDB threads unset (all 12 cores per engine) |
| Table | `bench.events (id INT64, ts TIMESTAMP, user_id INT64, country STRING, latency FLOAT64)` |
| Data | 400 serial inserts of 7,500 rows: **3,000,000 rows, 498,770 distinct `user_id`, 20 countries**, seed 42 |
| Layout | **400 sealed files, 0 hot files** (read back from `statistics.sealed.filesTotal` and `statistics.hot.filesTotal` before every arm) |

The layout needs two settings. Each insert is sealed on its own
(`SMOLQUERY_SEAL_MAX_FILES=1`, `SMOLQUERY_SEAL_MAX_BYTES=1`,
`SMOLQUERY_SEAL_MAX_AGE_MS=1000`), and `SMOLQUERY_COMPACT_TARGET_BYTES=off`
keeps compaction at the hour level. Every batch lands in its own hour, so
nothing merges and the table stays at 400 files. The server restarts before
every arm so each one starts from the same idle RSS. The hot-tier layout is
covered under "Side findings".

## Method

`scripts/scatter_admission.sh` boots the node and runs `scripts/scatter_admission.py`:

1. `load` creates the table and posts the 400 batches, then waits for the hot tier to drain.
2. For each shape and each value of `distributed`, a fresh server boots and idles 20 s. The driver then:
   - records the file inventory and runs one probe query. It aborts unless `job.scatter` is non-null for `true` and null for `false`.
   - samples the idle process for 5 s.
   - scrapes `/metrics` on :4003, runs **8 closed-loop clients on `POST /v1/queries` for 60 s** (each request carries `"distributed": <arm>` and `"burst": false`), then scrapes again.
3. A sampler reads the beam process every 250 ms (RSS and cumulative CPU time from `ps -o time=,rss=`, OS threads from `ps -M`). CPU% is the delta of cumulative CPU time over wall time, so 1200% is all 12 cores.

Shapes (aliases are required, see "Side findings"):

- `user`: `SELECT user_id, count(*) AS n, avg(latency) AS avg_latency FROM bench.events GROUP BY user_id ORDER BY n DESC LIMIT 10`
- `country`: the same with `country` as the key.

Measured and derived are kept apart:

- **Engines acquired** is measured: the delta of `smolquery_query_engines_total`, which counts one per job engine and one per shard engine ([`telemetry.ex:1168-1171`](https://github.com/smolquery/smolquery/blob/e7913f17469cbd3a5d395bd73ab8f346f221f751/lib/smolquery/telemetry.ex#L1168-L1171)).
- **Engines in flight** is an upper bound derived from the job intervals (1 + `scatter.shards` per scattered job, for the whole job). The metrics endpoint exposes no live engine gauge.

## Results

All four arms: 60 s, 8 clients, 0 refusals (429), no other status, 400 sealed and 0 hot files.

### user, `distributed: false`

| metric | value |
|---|---|
| uptime before | load averages 6.39 9.17 7.21 |
| requests / ok | 351 / 351 |
| jobs per second | 5.75 |
| scattered jobs | 0 of 351 |
| latency p50 / p95 / max (ms) | 1289 / 3383 / 4400 |
| RSS MB idle / peak / mean | 523 / 1626 / 1317 |
| OS threads idle / peak / mean | 156 / 270 / 244 |
| CPU% peak / mean | 1238 / 420 |
| engines acquired (total / per job) | 351 / 1.0 |
| engines in flight, upper bound | 8 |

### user, `distributed: true`

| metric | value |
|---|---|
| uptime before | load averages 7.45 9.36 7.51 |
| requests / ok | 197 / 197 |
| jobs per second | 3.21 |
| scattered jobs | **197 of 197**, 4 shards each, 4 peers (all `nonode@nohost`) |
| latency p50 / p95 / max (ms) | 2117 / 5424 / 7596 |
| RSS MB idle / peak / mean | 561 / 1682 / 1196 |
| OS threads idle / peak / mean | 167 / **501** / 316 |
| CPU% peak / mean | 1301 / 703 |
| engines acquired (total / per job) | 985 / **5.0** |
| engines in flight, upper bound | **40** |

### country, `distributed: false`

| metric | value |
|---|---|
| uptime before | load averages 9.24 9.43 7.71 |
| requests / ok | 410 / 410 |
| jobs per second | 6.69 |
| scattered jobs | 0 of 410 |
| latency p50 / p95 / max (ms) | 1260 / 2619 / 3555 |
| RSS MB idle / peak / mean | 469 / 751 / 655 |
| OS threads idle / peak / mean | 156 / 277 / 244 |
| CPU% peak / mean | 1136 / 395 |
| engines acquired (total / per job) | 410 / 1.0 |
| engines in flight, upper bound | 8 |

### country, `distributed: true`

| metric | value |
|---|---|
| uptime before | load averages 8.31 9.24 7.81 |
| requests / ok | 217 / 217 |
| jobs per second | 3.42 |
| scattered jobs | **217 of 217**, 4 shards each |
| latency p50 / p95 / max (ms) | 2271 / 4687 / 5498 |
| RSS MB idle / peak / mean | 470 / 824 / 720 |
| OS threads idle / peak / mean | 167 / **533** / 303 |
| CPU% peak / mean | 1196 / 576 |
| engines acquired (total / per job) | 1085 / **5.0** |
| engines in flight, upper bound | **40** |

### Side by side

| | user single | user distributed | country single | country distributed |
|---|---|---|---|---|
| engines per job (measured) | 1.0 | 5.0 | 1.0 | 5.0 |
| peak threads above idle | +114 | +334 | +121 | +366 |
| peak RSS above idle (MB) | 1103 | 1121 | 282 | 354 |
| CPU% mean | 420 | 703 | 395 | 576 |
| jobs per second | 5.75 | 3.21 | 6.69 | 3.42 |
| latency p50 (ms) | 1289 | 2117 | 1260 | 2271 |

## What the numbers say

**Admission is bypassed: confirmed.** Each distributed job took one admission
slot and ran 5 engines, exactly the 1 job engine + 4 local workers the code
describes, so 8 admitted jobs meant 40 engines of work. The counter is not an
estimate: 985 engines over 197 jobs and 1,085 over 217. The mechanism, all in
smolquery at `e7913f1`:

- `admit/1` counts the jobs in the registry against `max_concurrent_jobs` (default 8, [`runtime.ex:233`](https://github.com/smolquery/smolquery/blob/e7913f17469cbd3a5d395bd73ab8f346f221f751/lib/smolquery/query_service/runtime.ex#L233)) and runs once per client submission: [`client.ex:152`](https://github.com/smolquery/smolquery/blob/e7913f17469cbd3a5d395bd73ab8f346f221f751/lib/smolquery/query_service/client.ex#L152), [`client.ex:318-325`](https://github.com/smolquery/smolquery/blob/e7913f17469cbd3a5d395bd73ab8f346f221f751/lib/smolquery/query_service/client.ex#L318-L325). `max_concurrent_jobs` appears nowhere else in the query service.
- Without clustering the worker list is `local_workers` copies of this node ([`scatter.ex:396-402`](https://github.com/smolquery/smolquery/blob/e7913f17469cbd3a5d395bd73ab8f346f221f751/lib/smolquery/query_service/scatter.ex#L396-L402), default 4 at [`runtime.ex:258`](https://github.com/smolquery/smolquery/blob/e7913f17469cbd3a5d395bd73ab8f346f221f751/lib/smolquery/query_service/runtime.ex#L258)). The gate is the unit count against `min_files` ([`scatter.ex:334`](https://github.com/smolquery/smolquery/blob/e7913f17469cbd3a5d395bd73ab8f346f221f751/lib/smolquery/query_service/scatter.ex#L334), default 8 at [`runtime.ex:255`](https://github.com/smolquery/smolquery/blob/e7913f17469cbd3a5d395bd73ab8f346f221f751/lib/smolquery/query_service/runtime.ex#L255)). 400 files deal out 100 per shard.
- All shards run at once: `Task.async_stream` with `max_concurrency` equal to the shard count ([`scatter.ex:620-626`](https://github.com/smolquery/smolquery/blob/e7913f17469cbd3a5d395bd73ab8f346f221f751/lib/smolquery/query_service/scatter.ex#L620-L626), [`scatter.ex:892-893`](https://github.com/smolquery/smolquery/blob/e7913f17469cbd3a5d395bd73ab8f346f221f751/lib/smolquery/query_service/scatter.ex#L892-L893)).
- A local shard is a direct call, with no queue ([`worker_transport.ex:140-141`](https://github.com/smolquery/smolquery/blob/e7913f17469cbd3a5d395bd73ab8f346f221f751/lib/smolquery/query_service/worker_transport.ex#L140-L141)). `PartialWorker.run` goes straight to `with_engine` ([`partial_worker.ex:245-248`](https://github.com/smolquery/smolquery/blob/e7913f17469cbd3a5d395bd73ab8f346f221f751/lib/smolquery/query_service/partial_worker.ex#L245-L248)), which acquires an engine ([`partial_worker.ex:356-367`](https://github.com/smolquery/smolquery/blob/e7913f17469cbd3a5d395bd73ab8f346f221f751/lib/smolquery/query_service/partial_worker.ex#L356-L367)) with the job memory limit and the read thread count ([`partial_worker.ex:514-521`](https://github.com/smolquery/smolquery/blob/e7913f17469cbd3a5d395bd73ab8f346f221f751/lib/smolquery/query_service/partial_worker.ex#L514-L521)). A cold engine starts when the warm pool is empty ([`job_engine.ex:97-104`](https://github.com/smolquery/smolquery/blob/e7913f17469cbd3a5d395bd73ab8f346f221f751/lib/smolquery/query_service/job_engine.ex#L97-L104), [`job_engine.ex:233-236`](https://github.com/smolquery/smolquery/blob/e7913f17469cbd3a5d395bd73ab8f346f221f751/lib/smolquery/query_service/job_engine.ex#L233-L236)).

**The node does get loaded: confirmed for threads and CPU.** Peak OS threads
reached 501 and 533 against 270 and 277 (+334 and +366 above idle, three times
the single arm's +114 and +121). The single arm's 8 engines account for +114,
about 14 threads per engine. At that rate the distributed peaks are roughly 24
to 26 engines alive at once, short of the 40 upper bound because shard phases
overlap only partly. This is derived, not counted. Mean CPU rose from 420% to
703% and from 395% to 576%.

**The cost is throughput and latency, not failure.** 12 cores were already the
ceiling for 8 single-engine jobs (peak CPU 1136 to 1238%). Five times the
engines on the same cores did not make the queries faster: jobs per second fell
from 5.75 to 3.21 (-44%) and from 6.69 to 3.42 (-49%), median latency rose from
1289 to 2117 ms and from 1260 to 2271 ms. Nothing was refused, because the
8 clients never exceeded admission, and nothing timed out.

**Memory: not confirmed.** Peak RSS above idle was 1103 vs 1121 MB for `user`
and 282 vs 354 MB for `country`. The single arm's per-job footprint is
(1626 - 523) / 8 = 138 MB, so 8 times that is 1103 MB, and the distributed
arm's 1121 MB is 1.0x of it. The declared budget is far larger. Each worker
engine takes the whole `job_memory_limit` unless
`SMOLQUERY_DISTRIBUTED_WORKER_MEMORY_LIMIT` is set
([`docs/configuration.md:147`](https://github.com/smolquery/smolquery/blob/e7913f17469cbd3a5d395bd73ab8f346f221f751/docs/configuration.md?plain=1#L147), [`docs/configuration.md:162`](https://github.com/smolquery/smolquery/blob/e7913f17469cbd3a5d395bd73ab8f346f221f751/docs/configuration.md?plain=1#L162), [`runtime.ex:180-186`](https://github.com/smolquery/smolquery/blob/e7913f17469cbd3a5d395bd73ab8f346f221f751/lib/smolquery/query_service/runtime.ex#L180-L186)), so 8 jobs declare 8 x (1 + 4) x 1 GB = 40 GB. The test used 1.1 GB because the engines scan 3M narrow rows (17 MB of partials at most) and never reach the limit. A table wide enough for engines to fill their limit would show the memory half. This test cannot say, and it was run without a container budget, so no OOM kill was possible to observe.

## Side findings

- **`ORDER BY 2` does not scatter.** The decomposer refuses an ORDER BY on a position ([`decomposer.ex:1486-1489`](https://github.com/smolquery/smolquery/blob/e7913f17469cbd3a5d395bd73ab8f346f221f751/lib/smolquery/query_service/decomposer.ex#L1486-L1489)), and the job answers from one engine with `"scatter": null` and no error. The brief's literal `... GROUP BY user_id ORDER BY 2 DESC LIMIT 10` returned `scatter: null` with `distributed: true`. This run uses `ORDER BY n`. A bare `SELECT user_id, count(*), avg(latency) ... GROUP BY user_id` (unaliased `count(*)`, no ORDER BY) also returned `scatter: null`. The cause was not traced.
- **Hot-tier layout fails earlier, for a different reason.** An earlier pass kept all 400 batches as hot files (`SEAL_MODE=hot`). Even with `distributed: false`, 8 clients got 19 good answers in 148 requests over 30 s (the rest 503 "buffer node ... could not be reached" or 400 "Could not connect to server" on HTTP HEAD to the hot server). With `distributed: true`, 2 of 10 requests succeeded and the other 8 failed after about 245 s (400 on an HTTP HEAD timeout against the hot server, or 504), so the arm ran 309 s instead of 60. Every engine opens HTTP connections to one loopback hot server per hot file. That is the hot-tier read path, not admission, and it makes a many-hot-file table a poor test. Raw files from that pass are in `results/raw-scatter/hot/` (the first pass used an older driver that opened a new connection per request and flooded the host with 257k failed connects; its `user-single` file was replaced by a rerun with the final driver, the `user-distributed` file is from the older driver).

## What this test cannot show

- **The cross-node case.** With clustering on, the workers are the group's member nodes, and a cancelled or timed out job can leave its shards running on peers. Seeing it needs 2 or more query nodes (for example `CATALOG_DATABASE_URL` and two nodes) and a driver that cancels jobs mid-scan.
- **Memory pressure.** No container limit, 24 GiB of RAM, and a scan small enough that no engine neared 1 GB. Whether 40 engines fit a 4 to 8 GiB container is open. Re-run with a wide table or a lower `SMOLQUERY_JOB_MEMORY_LIMIT` to test it.
- **A 429.** The load held exactly 8 clients, so admission never refused. Nine or more clients would refuse client jobs, and that refusal is the visible half of the gap: it tracks client jobs while the engines behind them run five times as many.
- **Noise.** Another workload shared the Mac. Load averages before the four arms were 6.4 to 9.2, and each arm ran once. The arms ran in the order user single, user distributed, country single, country distributed, so the load drifted rather than favored one mode, but the 44 to 49% throughput gap and the 5.0 engines per job are larger than that noise. Treat the exact latencies as one sample.

## Reproduce

```sh
cd smolquery && MIX_ENV=prod mix deps.get && MIX_ENV=prod mix compile
cd ../random-bench
SMOLQUERY_DIR=~/dev/sq/smolquery \
TOOLCHAIN_PATH=/opt/homebrew/Cellar/erlang/29.1/bin:/opt/homebrew/Cellar/elixir/1.20.4/bin \
SMOLQUERY_EXTENSION_DIRECTORY=<dir with the httpfs, json and ducklake DuckDB extensions> \
  mise run scatter-admission -- all
```

Knobs, all environment variables: `RUN_DIR`, `OUT`, `SEAL_MODE`, `SHAPES`,
`MODES`, `BATCHES`, `ROWS_PER_BATCH`, `USERS`, `CONCURRENCY`, `DURATION_S`,
`SAMPLE_MS`. See the header of `scripts/scatter_admission.sh`. Raw per-arm JSON
and the 250 ms samples go to `results/raw-scatter/<run>/` (gitignored, as the
other `results/raw-*` directories are).
