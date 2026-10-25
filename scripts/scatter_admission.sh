#!/usr/bin/env bash
# Does smolquery's scatter path bypass query admission? One node, local store.
#
#   SMOLQUERY_DIR=~/dev/sq/smolquery scripts/scatter_admission.sh [all|load|arms|serve|report]
#
# all     boot, load bench.events, then boot fresh per arm and run the four arms
# load    boot, load bench.events, stop
# arms    boot fresh per arm over the existing data dir, run the arms, stop
# report  print the markdown tables for a finished run
#
# The server restarts before every arm so each arm starts from the same RSS.
# Inputs (environment, with defaults):
#   SMOLQUERY_DIR     checkout compiled with MIX_ENV=prod       (required)
#   RUN_DIR           data dir, logs and raw output             (mktemp -d)
#   OUT               raw results directory   (results/raw-scatter/<utc stamp>)
#   TOOLCHAIN_PATH    directories prepended to PATH for erl and elixir
#   TIMEOUT_BIN       a timeout(1) binary                       (timeout)
#   SEAL_MODE         sealed (default) seals as batches arrive, waits for the hot
#                     tier to drain (LOAD_PAUSE_MS, DRAIN_WAIT_S) and keeps
#                     compaction from merging the one-file-per-hour segments;
#                     hot keeps every batch a hot file
#   SHAPES            query shapes to run                        ("user country")
#   MODES             values of the per-job distributed option   ("false true")
#   plus the driver's: BATCHES ROWS_PER_BATCH USERS CONCURRENCY DURATION_S SAMPLE_MS
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
repo=$(dirname "$here")
mode=${1:-all}

: "${SMOLQUERY_DIR:?set SMOLQUERY_DIR to the compiled smolquery checkout}"
[ -n "${TOOLCHAIN_PATH:-}" ] && export PATH="$TOOLCHAIN_PATH:$PATH"
TIMEOUT_BIN=${TIMEOUT_BIN:-timeout}
RUN_DIR=${RUN_DIR:-$(mktemp -d)}
OUT=${OUT:-$repo/results/raw-scatter/$(date -u +%Y%m%dT%H%M%SZ)}
SEAL_MODE=${SEAL_MODE:-sealed}
[ "$SEAL_MODE" = sealed ] && export LOAD_PAUSE_MS=${LOAD_PAUSE_MS:-100} DRAIN_WAIT_S=${DRAIN_WAIT_S:-300}
SHAPES=${SHAPES:-"user country"}
MODES=${MODES:-"false true"}
export API_PORT=${API_PORT:-14100} METRICS_PORT=${METRICS_PORT:-14103}
export API="http://127.0.0.1:$API_PORT" METRICS="http://127.0.0.1:$METRICS_PORT"
export SMOLQUERY_API_KEY=${SMOLQUERY_API_KEY:-bench}
export SMOLQUERY_INTERNAL_SECRET=${SMOLQUERY_INTERNAL_SECRET:-bench-internal}
mkdir -p "$RUN_DIR" "$OUT"

server_env() {
  export MIX_ENV=prod
  export SMOLQUERY_DATA_DIR="$RUN_DIR/data"
  [ -d "${SMOLQUERY_EXTENSION_DIRECTORY:-}" ] || unset SMOLQUERY_EXTENSION_DIRECTORY
  export SMOLQUERY_API_PORT=$API_PORT SMOLQUERY_METRICS_PORT=$METRICS_PORT
  export SMOLQUERY_WEB_PORT=${SMOLQUERY_WEB_PORT:-14102} SMOLQUERY_HOT_SERVER_PORT=${SMOLQUERY_HOT_SERVER_PORT:-14101}
  export SMOLQUERY_BUFFER_BASE_URL="http://127.0.0.1:$SMOLQUERY_HOT_SERVER_PORT"
  export SMOLQUERY_ROLES=api,ingest,buffer,storage,query
  export SMOLQUERY_INSERT_MAX_NDJSON_BYTES=32000000
  export SMOLQUERY_SECRET_KEY_BASE=${SMOLQUERY_SECRET_KEY_BASE:-scatterscatterscatterscatterscatterscatterscatterscatterscatter00}
  export SMOLQUERY_WEB_USERNAME=smolquery SMOLQUERY_WEB_PASSWORD=smolquery
  if [ "$SEAL_MODE" = hot ]; then
    export SMOLQUERY_SEAL_MAX_FILES=1000000 SMOLQUERY_SEAL_MAX_BYTES=1000000000000 SMOLQUERY_SEAL_MAX_AGE_MS=86400000
  else
    export SMOLQUERY_SEAL_MAX_FILES=${SEAL_MAX_FILES:-1} SMOLQUERY_SEAL_MAX_BYTES=1 SMOLQUERY_SEAL_MAX_AGE_MS=1000
    export SMOLQUERY_COMPACT_TARGET_BYTES=off
  fi
}

SERVER_LOG=
start_server() {
  server_env
  SERVER_LOG="$RUN_DIR/server-$1.log"
  (cd "$SMOLQUERY_DIR" && exec elixir -S mix run --no-halt --no-compile --no-deps-check) >"$SERVER_LOG" 2>&1 &
  SERVER_SHELL=$!
  for _ in $(seq 1 180); do
    kill -0 "$SERVER_SHELL" 2>/dev/null || { echo "server died, see $SERVER_LOG" >&2; exit 1; }
    code=$(curl -s -o /dev/null -w '%{http_code}' -H "authorization: Bearer $SMOLQUERY_API_KEY" "$API/v1/datasets" || true)
    [ "$code" = 200 ] && break
    sleep 1
  done
  [ "$code" = 200 ] || { echo "server never answered" >&2; exit 1; }
  BEAM_PID=$(lsof -tiTCP:"$API_PORT" -sTCP:LISTEN | head -1)
  echo "server up pid=$BEAM_PID log=$SERVER_LOG"
}

stop_server() {
  [ -n "${BEAM_PID:-}" ] && kill "$BEAM_PID" 2>/dev/null || true
  [ -n "${SERVER_SHELL:-}" ] && kill "$SERVER_SHELL" 2>/dev/null || true
  for _ in $(seq 1 30); do
    lsof -tiTCP:"$API_PORT" -sTCP:LISTEN >/dev/null 2>&1 || break
    sleep 1
  done
  for p in $(lsof -tiTCP:"$API_PORT" -sTCP:LISTEN 2>/dev/null); do kill -9 "$p"; done
  for p in $(pgrep -f "$RUN_DIR/data" 2>/dev/null || true); do kill -9 "$p" 2>/dev/null || true; done
  BEAM_PID=
}
trap 'stop_server' EXIT

driver() { "$TIMEOUT_BIN" "${DRIVER_TIMEOUT:-1800}" python3 -I "$here/scatter_admission.py" "$@" --out "$OUT"; }

run_load() {
  start_server load
  driver load
  sleep 5
  stop_server
}

run_arms() {
  for shape in $SHAPES; do
    for dist in $MODES; do
      start_server "$shape-$dist"
      sleep 20
      driver arm --pid "$BEAM_PID" --shape "$shape" --distributed "$dist"
      stop_server
      echo "orphans: $(pgrep -fl "$RUN_DIR/data" | wc -l | tr -d ' ')"
    done
  done
}

case "$mode" in
  load) run_load ;;
  arms) run_arms ;;
  serve) start_server serve; echo "api $API metrics $METRICS; Ctrl-C to stop"; wait "$SERVER_SHELL" ;;
  report) python3 -I "$here/scatter_admission.py" report --out "$OUT" ;;
  all) run_load; run_arms; python3 -I "$here/scatter_admission.py" report --out "$OUT" ;;
  *) echo "usage: $0 [all|load|arms|serve|report]" >&2; exit 2 ;;
esac
echo "raw output: $OUT   run dir: $RUN_DIR"
