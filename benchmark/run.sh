#!/usr/bin/env bash
# Benchmark one engine end to end:
#   clean stack -> produce N events -> let the engine drain into Iceberg ->
#   read the resulting table metrics, ingest speed and resource use.
#
# Writes benchmark/results/<engine>.json (one JSON metrics line).
#
#   benchmark/run.sh <flink|spark|duckdb|connect> [record-count]
#
# Speed is measured, not guessed: the harness polls until all N rows have
# landed and derives startup_s / ingest_s from the append-snapshot timestamps.
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

echo "[$ENGINE] draining into Iceberg (polling until rows=$COUNT, max ${DRAIN_SECONDS}s)"
deadline=$(( t0 + DRAIN_SECONDS ))
metrics=""
while :; do
  metrics="$(read_metrics)"
  rows="$(jnum "$metrics" rows)"
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

# Timing: producer/engine start (t0) -> first / last append-snapshot commit.
first_append_ms="$(jnum "$metrics" first_append_ms)"
last_append_ms="$(jnum "$metrics" last_append_ms)"
startup_s=0
ingest_s=0
if [ -n "${first_append_ms:-}" ] && [ "${first_append_ms%.*}" -gt 0 ] 2>/dev/null; then
  startup_s=$(( ${first_append_ms%.*} / 1000 - t0 ))
  [ "$startup_s" -lt 0 ] && startup_s=0
fi
if [ -n "${last_append_ms:-}" ] && [ "${last_append_ms%.*}" -gt 0 ] 2>/dev/null; then
  ingest_s=$(( ${last_append_ms%.*} / 1000 - t0 ))
  [ "$ingest_s" -lt 1 ] && ingest_s=1
fi
throughput_rps=$(awk "BEGIN{ if ($ingest_s>0) printf \"%.1f\", $COUNT/$ingest_s; else print 0 }")

# Efficiency: peak summed memory and average summed CPU% across the run.
peak_mem_mb=$(awk 'NF==2 { if ($2 > m) m = $2 } END { printf "%.0f", m }' "$STATS_FILE")
cpu_pct_avg=$(awk 'NF==2 { c += $1; n++ } END { if (n > 0) printf "%.0f", c / n; else print 0 }' "$STATS_FILE")

if [ -z "$metrics" ]; then
  echo "[$ENGINE] WARNING: no metrics returned" >&2
  metrics="{\"engine\":\"$ENGINE\",\"exists\":false,\"rows\":0,\"data_files\":0,\"size_bytes\":0,\"snapshots\":0,\"write_window_s\":0,\"first_append_ms\":0,\"last_append_ms\":0,\"compactions\":0,\"compaction_files_rewritten\":0}"
fi

# Splice the orchestrator-measured timing + resource use into the tools JSON.
metrics="${metrics%\}},\"startup_s\":${startup_s},\"ingest_s\":${ingest_s},\"throughput_rps\":${throughput_rps},\"peak_mem_mb\":${peak_mem_mb},\"cpu_pct_avg\":${cpu_pct_avg}}"

echo "$metrics" > "$RESULTS/${ENGINE}.json"
echo "[$ENGINE] $metrics"
echo "[$ENGINE] done"
