#!/usr/bin/env bash
# Benchmark every engine, each on its own clean stack, then print a
# side-by-side comparison built from the per-engine result JSON files in
# benchmark/results/.
#
#   benchmark/run-all.sh [record-count]
set -euo pipefail

cd "$(dirname "$0")/.."
COUNT="${1:-50000}"
ENGINES=(flink spark duckdb connect)
FAILED=()

echo "=== kafka-s3-iceberg-dump benchmark: all engines, $COUNT events each ==="
echo

for engine in "${ENGINES[@]}"; do
  echo "=========================================================="
  echo "  ENGINE: $engine"
  echo "=========================================================="
  if benchmark/run.sh "$engine" "$COUNT"; then
    echo "[$engine] completed"
  else
    echo "[$engine] FAILED" >&2
    FAILED+=("$engine")
  fi
  echo
done

echo "=========================================================="
echo "  COMPARISON"
echo "=========================================================="
python3 benchmark/compare.py

if [ ${#FAILED[@]} -gt 0 ]; then
  echo "WARNING: the following engines failed: ${FAILED[*]}"
  exit 1
fi
