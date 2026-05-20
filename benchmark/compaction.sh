#!/usr/bin/env bash
# Compaction-strategy benchmark — runs Iceberg's rewrite_data_files with each
# strategy (binpack / sort / zorder) on an identical many-small-files table
# (via the self-contained Spark CompactionBenchmark job) and prints a
# side-by-side speed comparison.
#
#   benchmark/compaction.sh [rows-per-strategy]
set -euo pipefail

cd "$(dirname "$0")/.."
ROWS="${1:-200000}"
COMPOSE=(docker compose -f docker-compose.yml)
RESULTS="$(pwd)/benchmark/results"
mkdir -p "$RESULTS"

cleanup() { "${COMPOSE[@]}" down --remove-orphans >/dev/null 2>&1 || true; }
trap cleanup EXIT

echo "[compaction] cleaning previous stack"
cleanup
docker volume rm -f s3-table-dump_minio-data >/dev/null 2>&1 || true

# Each strategy writes its own bench_db table, so the stack is shared across
# the three runs (the Maven builder runs once, on the first).
for strategy in binpack sort zorder; do
  echo "[compaction] running '$strategy' on $ROWS rows"
  metrics="$("${COMPOSE[@]}" run --rm \
    -e MAINTENANCE_REWRITE_STRATEGY="$strategy" -e COMPACTION_ROWS="$ROWS" \
    compaction-bench 2>/dev/null | grep '^{' | tail -1 || true)"
  if [ -z "$metrics" ]; then
    echo "[compaction] WARNING: '$strategy' produced no result" >&2
    metrics="{\"strategy\":\"$strategy\",\"files_before\":0,\"files_after\":0,\"rewritten_bytes\":0,\"compaction_s\":0}"
  fi
  echo "$metrics" > "$RESULTS/compaction-${strategy}.json"
  echo "[compaction] $strategy -> $metrics"
done

field() { sed -n "s/.*\"$2\":\([0-9.]*\).*/\1/p" <<<"$1"; }

echo
echo "============== compaction-strategy comparison =============="
echo "rows compacted per strategy: $ROWS"
echo
printf '%-9s %13s %12s %16s %13s\n' \
  strategy files-before files-after rewritten-bytes compaction-s
printf '%-9s %13s %12s %16s %13s\n' \
  --------- ------------- ------------ ---------------- -------------
for strategy in binpack sort zorder; do
  f="benchmark/results/compaction-${strategy}.json"
  [ -f "$f" ] || { printf '%-9s %13s\n' "$strategy" "(no result)"; continue; }
  line="$(cat "$f")"
  printf '%-9s %13s %12s %16s %13s\n' "$strategy" \
    "$(field "$line" files_before)" \
    "$(field "$line" files_after)" \
    "$(field "$line" rewritten_bytes)" \
    "$(field "$line" compaction_s)"
done
echo
echo "binpack just packs small files; sort rewrites in sort order; zorder"
echo "computes a Z-order interleaving — compaction-s typically rises"
echo "binpack <= sort <= zorder."
