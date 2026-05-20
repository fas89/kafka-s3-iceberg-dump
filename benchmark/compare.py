"""Print a side-by-side comparison of the four ingestion engines.

Reads benchmark/results/{flink,spark,duckdb,connect}.json (written by run.sh)
and prints throughput / correctness / Iceberg file-layout metrics in one table.
Compaction-strategy results (`compaction-*.json`) are produced by
benchmark/compaction.sh and are NOT consumed here.

    python3 benchmark/compare.py
"""
import json
import os

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")
ENGINE_ORDER = ["flink", "spark", "duckdb", "connect"]


def load_results() -> dict:
    """Load each engine's result file. Only the per-engine `<engine>.json`
    files are read — `compaction-*.json` and any other JSON in the results
    directory are ignored."""
    out = {}
    for engine in ENGINE_ORDER:
        path = os.path.join(RESULTS_DIR, f"{engine}.json")
        if os.path.exists(path):
            with open(path) as fh:
                out[engine] = json.load(fh)
    return out


def fmt(value, suffix="") -> str:
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:,.1f}{suffix}"
    return f"{value:,}{suffix}"


def row(label: str, values: list[str]) -> str:
    return f"{label:<26}" + "".join(f"{v:>18}" for v in values)


def main() -> None:
    results = load_results()
    engines = [e for e in ENGINE_ORDER if e in results]
    if not engines:
        print("No results yet. Run: ./benchmark/run.sh <engine> [count]")
        return

    print()
    print(row("metric", engines))
    print("-" * (26 + 18 * len(engines)))

    def values(fn):
        return [fn(results[e]) for e in engines]

    def stat(r, key):
        return r.get("table_stats", {}).get(key)

    def ingest_rps(r):
        """Snapshot-based ingest throughput, falling back to the wall-clock drain."""
        rps = r.get("ingest_throughput_rps")
        if rps:
            return rps
        drain = r.get("consume_drain_s")
        return round(r["requested"] / drain, 1) if drain else None

    def avg_file_kb(r):
        size, files = stat(r, "size_bytes"), stat(r, "data_files")
        return round(size / files / 1024, 1) if size and files else None

    print(row("records requested",
              values(lambda r: fmt(r["requested"]))))
    print(row("rows landed",
              values(lambda r: fmt(r["final_rows"]))))
    print(row("row count exact",
              values(lambda r: "yes" if r["correct"] else "NO")))
    print(row("producer throughput rps",
              values(lambda r: fmt(r.get("producer", {}).get("throughput_rps")))))
    print(row("startup time (s)",
              values(lambda r: fmt(r.get("startup_s")))))
    print(row("drain time (s)",
              values(lambda r: fmt(r.get("consume_drain_s")))))
    print(row("ingest time (s)",
              values(lambda r: fmt(r.get("ingest_s")))))
    print(row("ingest throughput rps",
              values(lambda r: fmt(ingest_rps(r)))))
    print(row("peak memory (MB)",
              values(lambda r: fmt(r.get("peak_mem_mb")))))
    print(row("avg cpu (%)",
              values(lambda r: fmt(r.get("cpu_pct_avg")))))
    print(row("write window (s)",
              values(lambda r: fmt(stat(r, "write_window_s")))))
    print(row("iceberg data files",
              values(lambda r: fmt(stat(r, "data_files")))))
    print(row("iceberg snapshots",
              values(lambda r: fmt(stat(r, "snapshots")))))
    print(row("compactions",
              values(lambda r: fmt(stat(r, "compactions")))))
    print(row("files rewritten",
              values(lambda r: fmt(stat(r, "compaction_files_rewritten")))))
    print(row("avg data file (KB)",
              values(lambda r: fmt(avg_file_kb(r)))))
    print()
    print("startup time = producer start -> first append; ingest time = producer")
    print("start -> last append; drain time = producer finish -> last append (wall")
    print("clock). peak memory + avg cpu are summed across the engine's own")
    print("containers during ingest. compactions / files rewritten reflect Iceberg")
    print("maintenance (Flink and Spark in-job; DuckDB and Connect via a standalone")
    print("maintenance container). For a binpack / sort / zorder compaction-speed")
    print("comparison, run benchmark/compaction.sh.")
    print()


if __name__ == "__main__":
    main()
