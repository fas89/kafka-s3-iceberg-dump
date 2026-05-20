#!/usr/bin/env bash
# Failure-recovery test.
#
# Produce a fixed number of events, start the Flink engine, then kill and
# restart its TaskManager mid-ingest. Flink + Iceberg checkpointing is
# exactly-once, so the engine must recover from its checkpoint and land
# exactly the produced count in flink_db.events — no loss, no duplicates.
#
#   tests/recovery.sh [record-count]
set -uo pipefail

cd "$(dirname "$0")/.."
COUNT="${1:-100000}"
DRAIN_SECONDS="${DRAIN_SECONDS:-180}"
COMPOSE=(docker compose -f docker-compose.yml -f docker-compose.flink.yml)

cleanup() { "${COMPOSE[@]}" down --remove-orphans >/dev/null 2>&1 || true; }
trap cleanup EXIT

echo "[recovery] clean stack"
cleanup
docker volume rm -f s3-table-dump_minio-data >/dev/null 2>&1 || true

echo "[recovery] starting stack — producing $COUNT events"
RECORD_COUNT="$COUNT" "${COMPOSE[@]}" up -d --build

echo "[recovery] letting the engine ingest for 25s"
sleep 25

echo "[recovery] killing the Flink TaskManager mid-ingest"
"${COMPOSE[@]}" kill flink-taskmanager >/dev/null 2>&1 || true
sleep 5
echo "[recovery] restarting the Flink TaskManager"
"${COMPOSE[@]}" up -d flink-taskmanager

echo "[recovery] draining (${DRAIN_SECONDS}s)"
sleep "$DRAIN_SECONDS"

echo "[recovery] reading flink_db.events row count"
metrics="$("${COMPOSE[@]}" run --rm \
  -e ICEBERG_DB=flink_db -e ICEBERG_TABLE=events -e BENCH_ENGINE=flink \
  tools 2>/dev/null | grep '^{' | tail -1 || true)"
echo "[recovery] $metrics"
rows="$(sed -n 's/.*"rows":\([0-9]*\).*/\1/p' <<<"$metrics")"

if [ "${rows:-0}" = "$COUNT" ]; then
  echo "[recovery] PASS: $rows rows == $COUNT produced — exactly-once held across restart"
  exit 0
fi
echo "[recovery] FAIL: landed ${rows:-0} rows, expected $COUNT" >&2
exit 1
