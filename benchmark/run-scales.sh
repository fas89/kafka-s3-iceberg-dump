#!/usr/bin/env bash
# Run the full four-engine benchmark at multiple scales and stash each
# scale's per-engine JSON results into benchmark/results/scale-<N>/ so a
# downstream report can compare them side by side.
#
#   benchmark/run-scales.sh                 # default: 50000 100000 200000
#   benchmark/run-scales.sh 20000 50000     # custom scales
set -euo pipefail

cd "$(dirname "$0")/.."
RESULTS=benchmark/results
mkdir -p "$RESULTS"

SCALES=("$@")
if [ ${#SCALES[@]} -eq 0 ]; then
  SCALES=(50000 100000 200000)
fi

echo "=== multi-scale benchmark: ${SCALES[*]} ==="
for scale in "${SCALES[@]}"; do
  echo
  echo "############################################################"
  echo "# scale: $scale events per engine"
  echo "############################################################"
  # Wipe per-engine results so a partial run can't leak in stale data.
  rm -f "$RESULTS"/{flink,spark,duckdb,connect}.json
  benchmark/run-all.sh "$scale" || echo "[scale=$scale] one or more engines failed" >&2
  dest="$RESULTS/scale-$scale"
  mkdir -p "$dest"
  cp -f "$RESULTS"/{flink,spark,duckdb,connect}.json "$dest/" 2>/dev/null || true
  echo "[scale=$scale] results stashed in $dest/"
done

echo
echo "=== multi-scale benchmark done ==="
ls -1 "$RESULTS"/scale-*/ 2>/dev/null
