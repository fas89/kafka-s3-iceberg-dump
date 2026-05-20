#!/usr/bin/env bash
# Benchmark every engine, each on its own clean stack, then print a comparison
# table built from the per-engine metrics in benchmark/results/.
#
#   benchmark/run-all.sh [record-count]
set -euo pipefail

cd "$(dirname "$0")/.."
COUNT="${1:-50000}"
ENGINES=(flink spark duckdb connect)

for engine in "${ENGINES[@]}"; do
  echo "=========================================================="
  benchmark/run.sh "$engine" "$COUNT" || echo "[$engine] run failed" >&2
done

# Pull a field (integer or decimal) out of a flat JSON metrics line.
field() { sed -n "s/.*\"$2\":\([0-9.]*\).*/\1/p" <<<"$1"; }

echo
echo "======================== benchmark comparison ========================"
echo "events produced per engine: $COUNT"
echo
echo "  speed: startup-s (cold start), ingest-s + rows/s (end to end)"
echo "  efficiency: peak-mem-mb + cpu-% across the engine's own containers"
echo
printf '%-9s %8s %9s %9s %10s %12s %7s %11s\n' \
  engine rows startup-s ingest-s rows/s peak-mem-mb cpu-% compactions
printf '%-9s %8s %9s %9s %10s %12s %7s %11s\n' \
  --------- -------- --------- --------- ---------- ------------ ------- -----------
for engine in "${ENGINES[@]}"; do
  f="benchmark/results/${engine}.json"
  if [ ! -f "$f" ]; then
    printf '%-9s %8s\n' "$engine" "(no result)"
    continue
  fi
  line="$(cat "$f")"
  printf '%-9s %8s %9s %9s %10s %12s %7s %11s\n' "$engine" \
    "$(field "$line" rows)" \
    "$(field "$line" startup_s)" \
    "$(field "$line" ingest_s)" \
    "$(field "$line" throughput_rps)" \
    "$(field "$line" peak_mem_mb)" \
    "$(field "$line" cpu_pct_avg)" \
    "$(field "$line" compactions)"
done
echo
echo "startup-s = producer start -> first commit; ingest-s = producer start ->"
echo "all rows committed. peak-mem-mb / cpu-% are summed over the engine's own"
echo "containers. For a binpack/sort/zorder compaction-speed comparison, run"
echo "benchmark/compaction.sh."
