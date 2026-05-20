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
# Hard wall-clock cap per engine — a single stuck engine cannot sink the
# whole run. 600 s comfortably covers a 200k-event run; bump via env.
PER_ENGINE_TIMEOUT_S="${PER_ENGINE_TIMEOUT_S:-600}"

TIMEOUT_BIN=""
for candidate in timeout gtimeout; do
  if command -v "$candidate" >/dev/null 2>&1; then
    TIMEOUT_BIN="$candidate"
    break
  fi
done

# Pure-bash fallback when GNU/BSD `timeout` is not on PATH (macOS default).
# Sends SIGTERM after the cap, then SIGKILL 5 s later if the child is still
# alive. Returns 124 on timeout to match GNU `timeout`'s convention.
run_with_timeout() {
  local seconds=$1; shift
  "$@" &
  local pid=$!
  (
    sleep "$seconds"
    if kill -0 "$pid" 2>/dev/null; then
      kill -TERM "$pid" 2>/dev/null
      sleep 5
      if kill -0 "$pid" 2>/dev/null; then kill -KILL "$pid" 2>/dev/null; fi
    fi
  ) &
  local timer=$!
  local rc=0
  wait "$pid" 2>/dev/null || rc=$?
  kill "$timer" 2>/dev/null || true
  wait "$timer" 2>/dev/null || true
  # rc>=128 means killed by a signal (SIGTERM = 143, SIGKILL = 137);
  # treat that as a wall-clock timeout.
  if [ "$rc" -ge 128 ]; then rc=124; fi
  return "$rc"
}

echo "=== kafka-s3-iceberg-dump benchmark: all engines, $COUNT events each ==="
echo

for engine in "${ENGINES[@]}"; do
  echo "=========================================================="
  echo "  ENGINE: $engine  (per-engine wall-clock cap: ${PER_ENGINE_TIMEOUT_S}s)"
  echo "=========================================================="
  if [ -n "$TIMEOUT_BIN" ]; then
    runner=("$TIMEOUT_BIN" "--foreground" "${PER_ENGINE_TIMEOUT_S}s" benchmark/run.sh "$engine" "$COUNT")
    run_engine() { "${runner[@]}"; }
  else
    run_engine() { run_with_timeout "$PER_ENGINE_TIMEOUT_S" benchmark/run.sh "$engine" "$COUNT"; }
  fi
  if run_engine; then
    echo "[$engine] completed"
  else
    status=$?
    if [ "$status" -eq 124 ]; then
      echo "[$engine] FAILED — exceeded ${PER_ENGINE_TIMEOUT_S}s wall-clock cap" >&2
    else
      echo "[$engine] FAILED (exit $status)" >&2
    fi
    FAILED+=("$engine")
    # Best-effort teardown of anything the timed-out run may have left behind.
    docker compose -f docker-compose.yml -f "docker-compose.${engine}.yml" \
      down --remove-orphans >/dev/null 2>&1 || true
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
