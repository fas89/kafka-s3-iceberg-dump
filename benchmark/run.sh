#!/usr/bin/env bash
# Benchmark one engine end to end:
#   clean stack -> produce N events -> let the engine drain into Iceberg ->
#   read the resulting table metrics, ingest speed and resource use.
#
# Writes benchmark/results/<engine>.json with the nested schema compare.py
# expects: top-level timing/resource fields + `producer` (the producer's own
# throughput summary) + `table_stats` (snapshot-log + file-layout facts).
#
#   benchmark/run.sh <flink|spark|duckdb|connect> [record-count]
#
# Speed is measured, not guessed: the harness polls until all N rows have
# landed and derives startup_s / ingest_s from the append-snapshot timestamps,
# and drain_s as the wall-clock window after the producer finished.
# Efficiency: docker stats is sampled over the engine's own containers.
# DRAIN_SECONDS (env) caps how long to wait.
set -euo pipefail

ENGINE="${1:?usage: run.sh <flink|spark|duckdb|connect> [record-count]}"
COUNT="${2:-50000}"
DRAIN_SECONDS="${DRAIN_SECONDS:-120}"

case "$ENGINE" in
  flink|spark|duckdb|connect) ;;
  *) echo "unknown engine: $ENGINE (expected flink|spark|duckdb|connect)" >&2; exit 1 ;;
esac

cd "$(dirname "$0")/.."
COMPOSE=(docker compose -f docker-compose.yml -f "docker-compose.${ENGINE}.yml")
RESULTS="$(pwd)/benchmark/results"
mkdir -p "$RESULTS"

if ! command -v python3 >/dev/null; then
  echo "ERROR: python3 is required on the host (used to build the result JSON)." >&2
  exit 1
fi

# DuckDB registers its tables under the iceberg-rest facade's backend catalog
# name (`rest_backend`); the JDBC-catalog engines use the default (`demo`).
if [ "$ENGINE" = duckdb ]; then catalog_name=rest_backend; else catalog_name=demo; fi

# Container-name pattern for this engine's own containers (resource sampling).
case "$ENGINE" in
  flink)   stats_pattern='flink-jobmanager|flink-taskmanager' ;;
  spark)   stats_pattern='-spark-' ;;
  duckdb)  stats_pattern='engine-duckdb|iceberg-rest' ;;
  connect) stats_pattern='kafka-connect' ;;
esac

STATS_FILE="$(mktemp)"
SAMPLER_PID=""

cleanup() {
  [ -n "$SAMPLER_PID" ] && kill "$SAMPLER_PID" 2>/dev/null || true
  rm -f "$STATS_FILE"
  "${COMPOSE[@]}" down --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

# Background resource sampler: appends one "<cpu%> <mem-mb>" line every ~5s,
# summed across this engine's containers.
sample_resources() {
  while :; do
    docker stats --no-stream --format '{{.Name}}|{{.CPUPerc}}|{{.MemUsage}}' 2>/dev/null \
      | awk -F'|' -v pat="$stats_pattern" '
          $1 ~ pat {
            c = $2; gsub(/%/, "", c); cpu += c;
            split($3, m, " / "); v = m[1];
            num = v; gsub(/[A-Za-z]/, "", num);
            unit = v; gsub(/[0-9.]/, "", unit);
            if (unit == "GiB" || unit == "GB") num *= 1024;
            else if (unit == "KiB" || unit == "kB") num /= 1024;
            else if (unit == "B") num /= 1048576;
            mem += num;
          }
          END { printf "%.1f %.1f\n", cpu, mem }' >> "$STATS_FILE"
    sleep 5
  done
}

# One JSON metrics line from the tools container, or empty.
read_metrics() {
  "${COMPOSE[@]}" run --rm \
    -e ICEBERG_DB="${ENGINE}_db" -e ICEBERG_TABLE=events -e BENCH_ENGINE="$ENGINE" \
    -e ICEBERG_CATALOG_NAME="$catalog_name" \
    tools 2>/dev/null | grep '^{' | tail -1 || true
}
# Extract a numeric field from a flat JSON line.
jnum() { sed -n "s/.*\"$2\":\([0-9.]*\).*/\1/p" <<<"$1"; }

echo "[$ENGINE] cleaning previous stack"
"${COMPOSE[@]}" down --remove-orphans >/dev/null 2>&1 || true
docker volume rm -f s3-table-dump_minio-data >/dev/null 2>&1 || true

echo "[$ENGINE] starting stack — producing $COUNT events"
RECORD_COUNT="$COUNT" "${COMPOSE[@]}" up -d --build
t0=$(date +%s)
sample_resources & SAMPLER_PID=$!

echo "[$ENGINE] waiting for the producer to finish"
"${COMPOSE[@]}" wait producer >/dev/null 2>&1 || true
produce_done=$(date +%s)

# Grab the producer's own one-line JSON summary (emitted by EventProducer
# on shutdown) so the report can break out producer throughput separately.
producer_json="$( "${COMPOSE[@]}" logs --no-color --no-log-prefix producer 2>/dev/null \
                 | grep '^[[:space:]]*{' | tail -n 1 || true )"

echo "[$ENGINE] draining into Iceberg (polling until rows=$COUNT, max ${DRAIN_SECONDS}s)"
deadline=$(( t0 + DRAIN_SECONDS ))
metrics=""
final_rows=0
while :; do
  metrics="$(read_metrics)"
  rows="$(jnum "$metrics" rows)"
  if [ -n "${rows:-}" ]; then final_rows="${rows%.*}"; fi
  if [ -n "${rows:-}" ] && [ "${rows%.*}" -ge "$COUNT" ] 2>/dev/null; then
    break
  fi
  if [ "$(date +%s)" -ge "$deadline" ]; then
    echo "[$ENGINE] WARNING: drain timed out at ${rows:-0}/$COUNT rows" >&2
    break
  fi
  sleep 5
done

kill "$SAMPLER_PID" 2>/dev/null || true
SAMPLER_PID=""

# Wall-clock drain: time spent after the producer finished waiting for rows
# to land. For single-commit engines this often dominates; for streaming
# engines it can be ~0.
drain_s=$(( $(date +%s) - produce_done ))
[ "$drain_s" -lt 0 ] && drain_s=0

# Efficiency: peak summed memory and average summed CPU% across the run.
peak_mem_mb=$(awk 'NF==2 { if ($2 > m) m = $2 } END { printf "%.0f", m }' "$STATS_FILE")
cpu_pct_avg=$(awk 'NF==2 { c += $1; n++ } END { if (n > 0) printf "%.0f", c / n; else print 0 }' "$STATS_FILE")

if [ -z "$metrics" ]; then
  echo "[$ENGINE] WARNING: no metrics returned" >&2
  metrics='{"engine":"'"$ENGINE"'","exists":false,"rows":0,"data_files":0,"size_bytes":0,"snapshots":0,"write_window_s":0,"first_append_ms":0,"last_append_ms":0,"compactions":0,"compaction_files_rewritten":0}'
fi

# Build the final per-engine result as the nested schema compare.py expects.
# Everything heavy is done in python so we don't fight shell quoting.
python3 - "$ENGINE" "${ENGINE}_db.events" "$COUNT" "$final_rows" "$drain_s" "$t0" \
        "$peak_mem_mb" "$cpu_pct_avg" "$metrics" "$producer_json" \
        > "$RESULTS/${ENGINE}.json" <<'PY'
import json, sys, datetime

(engine, table, count, final_rows, drain_s, t0,
 peak_mem_mb, cpu_pct_avg, metrics, producer_raw) = sys.argv[1:11]
count, final_rows, drain_s, t0 = int(count), int(final_rows), int(drain_s), int(t0)
peak_mem_mb = int(float(peak_mem_mb or 0))
cpu_pct_avg = int(float(cpu_pct_avg or 0))

stats = json.loads(metrics) if metrics.strip().startswith("{") else {}
producer = json.loads(producer_raw) if producer_raw.strip().startswith("{") else {}

# Snapshot-log timing:
#   startup_s = producer start -> first append (engine's reaction time)
#   ingest_s  = producer start -> last  append (end-to-end ingest)
# Falls back to wall-clock drain when the snapshot log is empty.
first_append_ms = int(stats.get("first_append_ms") or 0)
last_append_ms = int(stats.get("last_append_ms") or 0)
startup_s = first_append_ms // 1000 - t0 if first_append_ms else 0
if startup_s < 0:
    startup_s = 0
ingest_s = last_append_ms // 1000 - t0 if last_append_ms else 0
if ingest_s < 1:
    ingest_s = drain_s
ingest_throughput_rps = round(count / ingest_s, 1) if ingest_s > 0 else None

result = {
    "engine": engine,
    "table": table,
    "requested": count,
    "final_rows": final_rows,
    "correct": final_rows == count,
    "producer": producer,
    "consume_drain_s": drain_s,
    "startup_s": startup_s,
    "ingest_s": ingest_s,
    "ingest_throughput_rps": ingest_throughput_rps,
    "peak_mem_mb": peak_mem_mb,
    "cpu_pct_avg": cpu_pct_avg,
    "table_stats": stats,
    "timestamp": datetime.datetime.now().isoformat(timespec="seconds"),
}
print(json.dumps(result, indent=2))
PY

echo "[$ENGINE] result written to benchmark/results/${ENGINE}.json"
cat "$RESULTS/${ENGINE}.json"
echo
echo "[$ENGINE] done"
