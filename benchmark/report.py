"""Render a self-contained HTML benchmark report from multi-scale results.

Reads:
  - benchmark/results/scale-<N>/<engine>.json  (per-engine, per-scale)
  - benchmark/results/compaction-<strategy>.json (optional; ingested if present)

Writes:
  - benchmark/report.html (default), or the path passed as argv[1]

The HTML is fully self-contained -- inline CSS, inline SVG line charts, no
external assets -- so a single file can be emailed or attached.

    python3 benchmark/report.py [output-path]

Layout (top to bottom):
  1. Header + verdict chip
  2. TL;DR card with winner highlights + hero line chart
  3. Methodology
  4. Per-scale comparison tables (one per scale)
  5. Cross-scale line charts (one per metric)
  6. Scaling-factor table (largest / smallest ratios)
  7. Schema-handling note
  8. Compaction-strategy comparison (if data present)
  9. Verdict + decision matrix
 10. Caveats / threats to validity
 11. Reproducibility / versions
 12. Footer
"""
from __future__ import annotations

import datetime
import html
import json
import os
import sys
from typing import Callable, Optional

# ---------- config ----------------------------------------------------------

ENGINE_ORDER = ["flink", "spark", "duckdb", "connect"]
ENGINE_COLORS = {
    "flink":   "#dc2626",  # red-600  -- crimson reads strong on white
    "spark":   "#ea580c",  # orange-600
    "duckdb":  "#ca8a04",  # yellow-700 -- darker for AA contrast on white
    "connect": "#2563eb",  # blue-600
}
RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")
DEFAULT_OUT = os.path.join(os.path.dirname(__file__), "report.html")

# Pinned versions from README. If the README drifts, update here too.
VERSIONS = [
    ("Java",          "21"),
    ("Apache Flink",  "2.0.2"),
    ("Apache Spark",  "4.0.0 (Scala 2.13)"),
    ("Apache Kafka",  "4.0.0 (KRaft, mTLS)"),
    ("Apache Iceberg", "1.10.2"),
    ("DuckDB JDBC",   "1.4.3.0"),
    ("Kafka Connect", "cp-kafka-connect 7.8.0 + iceberg-kafka-connect 1.9.2"),
    ("MinIO / Postgres", "alpine/minio 2025-10 / pg 17.10"),
]


# ---------- data loading ----------------------------------------------------

def discover_scales() -> list[int]:
    scales = []
    if not os.path.isdir(RESULTS_DIR):
        return scales
    for name in os.listdir(RESULTS_DIR):
        if name.startswith("scale-") and os.path.isdir(os.path.join(RESULTS_DIR, name)):
            try:
                scales.append(int(name.split("-", 1)[1]))
            except ValueError:
                pass
    return sorted(scales)


def load_scale(scale: int) -> dict:
    out: dict[str, dict] = {}
    scale_dir = os.path.join(RESULTS_DIR, f"scale-{scale}")
    for engine in ENGINE_ORDER:
        path = os.path.join(scale_dir, f"{engine}.json")
        if os.path.exists(path):
            with open(path) as fh:
                out[engine] = json.load(fh)
    return out


def load_compaction() -> dict:
    """Returns {strategy: result_dict} for any compaction-*.json present."""
    out: dict[str, dict] = {}
    if not os.path.isdir(RESULTS_DIR):
        return out
    for name in os.listdir(RESULTS_DIR):
        if name.startswith("compaction-") and name.endswith(".json"):
            strategy = name[len("compaction-"):-len(".json")]
            with open(os.path.join(RESULTS_DIR, name)) as fh:
                out[strategy] = json.load(fh)
    return out


# ---------- value accessors -------------------------------------------------

def stat(r: dict, key: str):
    return (r.get("table_stats") or {}).get(key)


def ingest_rps(r: dict) -> Optional[float]:
    rps = r.get("ingest_throughput_rps")
    if rps:
        return rps
    drain = r.get("consume_drain_s")
    return round(r["requested"] / drain, 1) if drain else None


def avg_file_kb(r: dict) -> Optional[float]:
    size, files = stat(r, "size_bytes"), stat(r, "data_files")
    return round(size / files / 1024, 1) if size and files else None


def producer_rps(r: dict) -> Optional[float]:
    return (r.get("producer") or {}).get("throughput_rps")


# Metric spec: (label, accessor, format, "lower"|"higher"|None)
METRICS: list[tuple[str, Callable[[dict], object], str, Optional[str]]] = [
    ("records requested",      lambda r: r.get("requested"),         "{:,}",   None),
    ("rows landed",            lambda r: r.get("final_rows"),        "{:,}",   None),
    ("row count exact",        lambda r: "yes" if r.get("correct") else "NO", "{}", None),
    ("producer rps",           producer_rps,                          "{:,.1f}", "higher"),
    ("startup s",              lambda r: r.get("startup_s"),          "{:,}",   "lower"),
    ("drain s",                lambda r: r.get("consume_drain_s"),    "{:,}",   "lower"),
    ("ingest s",               lambda r: r.get("ingest_s"),           "{:,}",   "lower"),
    ("ingest rps",             ingest_rps,                            "{:,.1f}", "higher"),
    ("peak mem MB",            lambda r: r.get("peak_mem_mb"),        "{:,}",   "lower"),
    ("avg cpu %",              lambda r: r.get("cpu_pct_avg"),        "{:,}",   "lower"),
    ("write window s",         lambda r: stat(r, "write_window_s"),   "{:,.1f}", None),
    ("data files",             lambda r: stat(r, "data_files"),       "{:,}",   None),
    ("snapshots",              lambda r: stat(r, "snapshots"),        "{:,}",   None),
    ("compactions",            lambda r: stat(r, "compactions"),      "{:,}",   None),
    ("files rewritten",        lambda r: stat(r, "compaction_files_rewritten"), "{:,}", None),
    ("avg data file KB",       avg_file_kb,                           "{:,.1f}", None),
]


def fmt(value, fmt_str) -> str:
    if value is None or value == "":
        return "-"
    if isinstance(value, str):
        return value
    try:
        return fmt_str.format(value)
    except (TypeError, ValueError):
        return str(value)


def pick_winner(values: list, mode: Optional[str]) -> Optional[int]:
    if mode not in ("higher", "lower"):
        return None
    numeric = [(i, v) for i, v in enumerate(values)
               if isinstance(v, (int, float)) and not isinstance(v, bool)]
    if not numeric:
        return None
    return (max if mode == "higher" else min)(numeric, key=lambda t: t[1])[0]


# ---------- CSS -------------------------------------------------------------

CSS = """
:root {
  --bg: #fafaf9;
  --panel: #ffffff;
  --panel-2: #f8fafc;
  --border: #e2e8f0;
  --border-strong: #cbd5e1;
  --text: #0f172a;
  --text-2: #334155;
  --muted: #64748b;
  --accent: #2563eb;
  --win: #16a34a;
  --win-bg: #dcfce7;
  --warn: #ca8a04;
  --bad: #dc2626;
}
* { box-sizing: border-box; }
html, body { background: var(--bg); }
body {
  margin: 0;
  font: 15px/1.65 ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont,
        "Segoe UI", "Inter", "Helvetica Neue", Arial, sans-serif;
  color: var(--text);
  padding: 48px 24px 96px;
  max-width: 1080px;
  margin-left: auto;
  margin-right: auto;
}
h1, h2, h3 { font-weight: 700; letter-spacing: -0.015em; color: var(--text); }
h1 { font-size: 34px; line-height: 1.2; margin: 0 0 8px; }
h2 { font-size: 24px; line-height: 1.3; margin: 64px 0 20px;
     padding-bottom: 10px; border-bottom: 1px solid var(--border); }
h3 { font-size: 17px; line-height: 1.4; margin: 24px 0 12px; color: var(--text); }
p  { margin: 8px 0; color: var(--text-2); }
.subtitle { color: var(--muted); margin-bottom: 24px; font-size: 14px; }
.verdict-chip {
  display: inline-flex; align-items: center; gap: 6px;
  margin: 6px 0 20px;
  padding: 6px 14px; border-radius: 999px;
  background: var(--win-bg); color: var(--win);
  font-weight: 600; font-size: 13px;
  border: 1px solid #bbf7d0;
}
.card { background: var(--panel); border: 1px solid var(--border);
        border-radius: 12px; padding: 24px 28px; margin: 20px 0;
        box-shadow: 0 1px 2px rgba(15, 23, 42, 0.04),
                    0 0 0 1px rgba(15, 23, 42, 0.02); }
.tldr-grid { display: grid; grid-template-columns: 1fr; gap: 20px;
             align-items: start; }
.winner-cards { display: grid; grid-template-columns: repeat(3, 1fr);
                gap: 14px; margin: 20px 0 8px; }
.winner-card { background: var(--panel-2); border: 1px solid var(--border);
               border-radius: 10px; padding: 18px; }
.winner-card .label { color: var(--muted); font-size: 11px;
                      letter-spacing: 0.1em; text-transform: uppercase;
                      font-weight: 600; }
.winner-card .value { font-size: 26px; font-weight: 800; line-height: 1.1;
                      margin: 6px 0 4px; }
.winner-card .sub { color: var(--muted); font-size: 13px;
                    font-variant-numeric: tabular-nums; }
table { width: 100%; border-collapse: collapse;
        font-variant-numeric: tabular-nums; font-size: 14px; }
th, td { padding: 9px 12px; text-align: right;
         border-bottom: 1px solid var(--border); }
th:first-child, td:first-child { text-align: left; color: var(--text-2); }
th { font-weight: 600; color: var(--muted); white-space: nowrap;
     background: var(--panel-2); font-size: 12px;
     text-transform: uppercase; letter-spacing: 0.04em; }
tbody tr:hover { background: var(--panel-2); }
.winner { background: var(--win-bg); color: #15803d;
          font-weight: 700; border-radius: 4px; }
.bad   { color: var(--bad);  font-weight: 600; }
.warn  { color: var(--warn); font-weight: 600; }
.engine-flink   { color: #b91c1c; font-weight: 600; }
.engine-spark   { color: #c2410c; font-weight: 600; }
.engine-duckdb  { color: #a16207; font-weight: 600; }
.engine-connect { color: #1d4ed8; font-weight: 600; }
.legend { display: flex; flex-wrap: wrap; gap: 18px; margin: 0 0 14px;
          color: var(--muted); font-size: 13px; }
.legend-swatch { display: inline-block; width: 14px; height: 14px;
                 border-radius: 3px; margin-right: 7px; vertical-align: -2px; }
.callout { border-left: 4px solid var(--accent); padding: 14px 20px;
           background: #eff6ff; border-radius: 0 8px 8px 0;
           margin: 18px 0; color: var(--text); }
.callout.warn { border-color: var(--warn);
                background: #fefce8; }
.callout.win  { border-color: var(--win);
                background: var(--win-bg); }
.callout.bad  { border-color: var(--bad);
                background: #fef2f2; }
ul { padding-left: 22px; color: var(--text-2); }
li { margin: 6px 0; }
code { background: #f1f5f9; color: #334155; border-radius: 4px;
       padding: 1px 6px; font: 13px/1 ui-monospace, "SF Mono", Menlo,
       Consolas, monospace; }
.charts-grid { display: grid; grid-template-columns: 1fr; gap: 24px;
               margin-top: 8px; }
.chart-card { background: var(--panel); border: 1px solid var(--border);
              border-radius: 12px; padding: 24px 28px;
              box-shadow: 0 1px 2px rgba(15, 23, 42, 0.04); }
.chart-card h3 { margin-top: 0; margin-bottom: 4px; }
.chart-card svg { width: 100%; height: auto; display: block;
                  margin-top: 8px;
                  /* Belt-and-braces: explicit aspect ratio so height is
                     never 0 even if the browser ignores the SVG
                     width/height attributes. */
                  aspect-ratio: 960 / 380; }
.chart-title-row { display: flex; justify-content: space-between;
                   align-items: baseline; flex-wrap: wrap; gap: 8px; }
.deco { color: var(--muted); font-weight: 500; font-size: 12px;
        text-transform: uppercase; letter-spacing: 0.05em;
        background: var(--panel-2); padding: 3px 10px; border-radius: 999px;
        border: 1px solid var(--border); }
.matrix { width: 100%; }
.matrix td:first-child { color: var(--text); font-weight: 700; }
footer { color: var(--muted); margin-top: 96px; font-size: 13px;
         border-top: 1px solid var(--border); padding-top: 24px; }
@media (max-width: 700px) {
  body { padding: 24px 16px; }
  .winner-cards { grid-template-columns: 1fr; }
  h1 { font-size: 28px; }
  h2 { font-size: 21px; }
}
"""


# ---------- SVG line chart --------------------------------------------------

def svg_line_chart(
    title: str,
    series: dict[str, list[Optional[float]]],
    x_labels: list[str],
    lower_is_better: bool = False,
    unit: str = "",
    width: int = 960,
    height: int = 380,
) -> str:
    """Render an inline SVG line chart for the light theme.

    series: {engine_name: [value_per_x_point, ...]} -- None for missing data.
    x_labels: labels for x-axis (e.g. ['50k', '100k', '200k']).
    """
    pad_l, pad_r, pad_t, pad_b = 76, 96, 24, 56
    plot_w = width - pad_l - pad_r
    plot_h = height - pad_t - pad_b
    if not x_labels:
        return ""

    flat = [v for vs in series.values() for v in vs
            if isinstance(v, (int, float)) and not isinstance(v, bool)]
    if not flat:
        return ""
    ymin = 0
    ymax_raw = max(flat)
    ymax = ymax_raw * 1.18 if ymax_raw > 0 else 1.0
    n_ticks = 5
    step = ymax / (n_ticks - 1)
    n_x = len(x_labels)

    def xc(i): return pad_l + (i / max(n_x - 1, 1)) * plot_w
    def yc(v): return pad_t + plot_h - ((v - ymin) / max(ymax - ymin, 1e-9)) * plot_h

    # Y-grid + tick labels.
    grid = []
    for k in range(n_ticks):
        v = ymin + step * k
        y = yc(v)
        grid.append(
            f'<line x1="{pad_l}" y1="{y:.1f}" x2="{pad_l + plot_w}" y2="{y:.1f}" '
            f'stroke="#e2e8f0" stroke-width="1" />'
        )
        grid.append(
            f'<text x="{pad_l - 10}" y="{y + 4:.1f}" text-anchor="end" '
            f'fill="#64748b" font-size="12" font-family="ui-sans-serif">'
            f'{v:,.0f}</text>'
        )

    # X-axis labels with subtle event-count below.
    x_ticks = []
    for i, label in enumerate(x_labels):
        x = xc(i)
        x_ticks.append(
            f'<text x="{x:.1f}" y="{height - 28}" text-anchor="middle" '
            f'fill="#0f172a" font-size="14" font-weight="600">'
            f'{html.escape(label)}</text>'
        )
        x_ticks.append(
            f'<text x="{x:.1f}" y="{height - 12}" text-anchor="middle" '
            f'fill="#94a3b8" font-size="11">events</text>'
        )

    # Series polylines + markers + end-of-line value labels.
    series_svg = []
    for engine, vals in series.items():
        color = ENGINE_COLORS.get(engine, "#888")
        pts, markers = [], []
        last_pt = None
        for i, v in enumerate(vals):
            if isinstance(v, (int, float)) and not isinstance(v, bool):
                cx, cy = xc(i), yc(v)
                pts.append(f"{cx:.1f},{cy:.1f}")
                markers.append(
                    f'<circle cx="{cx:.1f}" cy="{cy:.1f}" r="6" '
                    f'fill="{color}" stroke="#ffffff" stroke-width="2.5">'
                    f'<title>{engine} @ {x_labels[i]}: {v:,.1f}{unit}</title>'
                    f'</circle>'
                )
                last_pt = (cx, cy, v)
        if pts:
            series_svg.append(
                f'<polyline points="{" ".join(pts)}" fill="none" '
                f'stroke="{color}" stroke-width="3" stroke-linecap="round" '
                f'stroke-linejoin="round" />'
            )
            series_svg.extend(markers)
            # End-of-line label for the final point.
            if last_pt:
                cx, cy, v = last_pt
                series_svg.append(
                    f'<text x="{cx + 12:.1f}" y="{cy + 5:.1f}" '
                    f'fill="{color}" font-size="13" font-weight="700" '
                    f'font-family="ui-sans-serif">'
                    f'{engine} {v:,.0f}</text>'
                )

    # Axes (subtle).
    axes = (
        f'<line x1="{pad_l}" y1="{pad_t + plot_h}" x2="{pad_l + plot_w}" '
        f'y2="{pad_t + plot_h}" stroke="#cbd5e1" stroke-width="1.5" />'
    )

    direction_note = "↑ better" if not lower_is_better else "↓ better"
    legend = " ".join(
        f"<span><span class='legend-swatch' "
        f"style='background:{ENGINE_COLORS[e]}'></span>{e}</span>"
        for e in ENGINE_ORDER if e in series
    )
    return (
        f"<div class='chart-card'>"
        f"<div class='chart-title-row'>"
        f"<h3>{html.escape(title)}</h3>"
        f"<span class='deco'>{direction_note}</span>"
        f"</div>"
        f"<div class='legend'>{legend}</div>"
        f"<svg width='{width}' height='{height}' "
        f"viewBox='0 0 {width} {height}' "
        f"preserveAspectRatio='xMidYMid meet' "
        f"role='img' aria-label='{html.escape(title)} line chart'>"
        f"{''.join(grid)}{axes}{''.join(series_svg)}{''.join(x_ticks)}"
        f"</svg>"
        f"</div>"
    )


# ---------- table builder ---------------------------------------------------

def html_table(scale: int, results: dict) -> str:
    # Always show every engine as a column, even if it didn't complete at
    # this scale — missing data renders as "—" with a "did not run"
    # footnote, so the per-scale tables stay column-aligned.
    engines = list(ENGINE_ORDER)
    missing = [e for e in engines if e not in results]
    rows_html = []
    for label, accessor, fmt_str, mode in METRICS:
        values = [accessor(results[e]) if e in results else None for e in engines]
        # Only pick a winner among engines that actually have data here.
        active = [(i, v) for i, v in enumerate(values)
                  if engines[i] in results]
        active_values = [v for _, v in active]
        winner_pos = pick_winner(active_values, mode)
        winner_idx = active[winner_pos][0] if winner_pos is not None else None
        cells = []
        for i, v in enumerate(values):
            if engines[i] not in results:
                cells.append("<td class='warn'>&mdash;</td>")
                continue
            cls = " class='winner'" if i == winner_idx else ""
            cells.append(f"<td{cls}>{html.escape(fmt(v, fmt_str))}</td>")
        rows_html.append(f"<tr><td>{html.escape(label)}</td>{''.join(cells)}</tr>")
    heads = "".join(f"<th class='engine-{e}'>{e}</th>" for e in engines)
    footnote = ""
    if missing:
        listed = ", ".join(f"<span class='engine-{e}'>{e}</span>" for e in missing)
        footnote = (
            f"<p class='subtitle' style='margin-top:12px'>"
            f"&mdash; did not complete at this scale: {listed} "
            f"(see Caveats for context).</p>"
        )
    return (
        f"<div class='card'><h3>scale: {scale:,} events per engine</h3>"
        f"<table><thead><tr><th>metric</th>{heads}</tr></thead>"
        f"<tbody>{''.join(rows_html)}</tbody></table>"
        f"{footnote}</div>"
    )


# ---------- section builders ------------------------------------------------

def build_header(scales: list[int]) -> str:
    title = "kafka-s3-iceberg-dump &mdash; multi-engine ingest benchmark"
    subtitle = (
        f"scales: {', '.join(f'{s:,}' for s in scales)} events &middot; "
        f"engines: flink, spark, duckdb, connect &middot; "
        f"generated {datetime.datetime.now().isoformat(timespec='seconds')}"
    )
    # Visible source-file banner so this report can't be confused with any
    # other repo's report.html (e.g. Kafka-Sink's, which has a different
    # layout but the same file name).
    banner = (
        "<div style='padding:8px 14px;margin:0 0 20px;"
        "border:1px solid #c7d2fe;background:#eef2ff;color:#3730a3;"
        "border-radius:8px;font:600 12px ui-monospace,Menlo,monospace;'>"
        "SOURCE: github.com/fas89/kafka-s3-iceberg-dump &middot; "
        "branch feature/four-engine-benchmark &middot; "
        "file benchmark/report.html"
        "</div>"
    )
    return f"{banner}<h1>{title}</h1><div class='subtitle'>{subtitle}</div>"


def build_tldr(per_scale: dict) -> str:
    scales = sorted(per_scale)
    if not scales:
        return "<div class='callout warn'>No benchmark data found.</div>"

    largest = scales[-1]
    top = per_scale[largest]
    engines = [e for e in ENGINE_ORDER if e in top]
    if not engines:
        return "<div class='callout warn'>No engines completed at the largest scale.</div>"

    # Hero chart: ingest throughput vs scale.
    series = {
        e: [ingest_rps(per_scale[s].get(e) or {}) for s in scales]
        for e in engines
    }
    hero_chart = svg_line_chart(
        title="ingest throughput",
        series=series,
        x_labels=[f"{s//1000}k" for s in scales],
        lower_is_better=False,
        unit=" rps",
    )

    # ---- one card per engine: every engine gets a top-of-page slot ------
    def commit_cadence(r):
        snaps = (r.get("table_stats") or {}).get("snapshots")
        ingest_s = r.get("ingest_s")
        if snaps and ingest_s and snaps > 0:
            return ingest_s / snaps
        return None

    def stat_cell(label, value):
        return (f"<div style='display:flex;justify-content:space-between;"
                f"font-size:12px;color:var(--muted);'>"
                f"<span>{label}</span><span style='color:var(--text);"
                f"font-variant-numeric:tabular-nums;font-weight:600'>"
                f"{value}</span></div>")

    cards_html = []
    for e in ENGINE_ORDER:
        r = top.get(e, {})
        rps = ingest_rps(r) or 0
        mem = r.get("peak_mem_mb") or 0
        ing = r.get("ingest_s") or 0
        cad = commit_cadence(r) or 0
        cards_html.append(
            f"<div class='winner-card'>"
            f"  <div class='label'>@ {largest:,} events</div>"
            f"  <div class='value engine-{e}'>{e}</div>"
            f"  <div style='margin-top:10px;display:flex;flex-direction:column;gap:4px'>"
            f"    {stat_cell('rps', f'{rps:,.0f}' if rps else '—')}"
            f"    {stat_cell('ingest', f'{ing} s' if ing else '—')}"
            f"    {stat_cell('peak mem', f'{mem:,} MB' if mem else '—')}"
            f"    {stat_cell('cadence', f'{cad:.2f} s' if cad else '—')}"
            f"  </div>"
            f"</div>"
        )
    cards = f"<div class='winner-cards' style='grid-template-columns:repeat(4,1fr)'>{''.join(cards_html)}</div>"

    return (
        f"<div class='card'>"
        f"  {cards}"
        f"  {hero_chart}"
        f"</div>"
    )


def build_implementation() -> str:
    return """
    <h2>Implementation</h2>
    <div class='card'>
      <p>Maven multi-module monorepo. mTLS Kafka &rarr; per-engine ingest
        &rarr; one Iceberg table per engine on MinIO via a shared
        Postgres JDBC catalog. Iceberg 1.10.2, Java 21, Docker Compose
        (base + one overlay per engine). Maintenance tuning is
        single-sourced in <code>common/MaintenanceTuning.java</code>.</p>
      <table>
        <thead><tr><th>engine</th><th>ingest</th><th>maintenance</th></tr></thead>
        <tbody>
          <tr><td><span class='engine-flink'>flink</span></td>
            <td>ingest-only Flink job</td>
            <td>sibling <code>flink-maintenance.jar</code> container
              (PR #1 split) with <code>RetryingTriggerLockFactory</code>
              + <code>DeleteOrphanFiles</code></td></tr>
          <tr><td><span class='engine-spark'>spark</span></td>
            <td>Structured Streaming, partitioned by <code>event_type</code></td>
            <td>in-job, native procedures (binpack / sort / zorder)</td></tr>
          <tr><td><span class='engine-duckdb'>duckdb</span></td>
            <td>kafka-clients + DuckDB iceberg extension, micro-batched</td>
            <td>sibling <code>flink-maintenance.jar</code> container</td></tr>
          <tr><td><span class='engine-connect'>connect</span></td>
            <td><code>iceberg-kafka-connect</code> sink config, no Java</td>
            <td>sibling <code>flink-maintenance.jar</code> container</td></tr>
        </tbody>
      </table>
    </div>
    """


def build_methodology(scales: list[int]) -> str:
    return f"""
    <h2>How it's measured</h2>
    <div class='card'>
      <p>Java producer &rarr; mTLS Kafka &rarr; each engine writes
        <code>&lt;engine&gt;_db.events</code> on MinIO via a shared
        Postgres JDBC catalog. Scales:
        <strong>{', '.join(f'{s:,}' for s in scales)}</strong> events,
        clean stack per engine per scale. Timing from Iceberg snapshot
        timestamps (engine-agnostic); resources from
        <code>docker stats</code> sampled every 5 s across each engine's
        containers; correctness by polling rows until
        <code>rows &ge; requested</code>.</p>
    </div>
    """


def build_per_scale(per_scale: dict) -> str:
    scales = sorted(per_scale)
    tables = "".join(html_table(s, per_scale[s]) for s in scales)
    return f"<h2>Numbers</h2>{tables}"


def build_cross_scale(per_scale: dict) -> str:
    scales = sorted(per_scale)
    if len(scales) < 2:
        return ""

    x_labels = [f"{s//1000}k" for s in scales]
    engines_present = [
        e for e in ENGINE_ORDER
        if any(e in per_scale[s] for s in scales)
    ]

    def series(accessor):
        return {
            e: [accessor(per_scale[s].get(e) or {}) for s in scales]
            for e in engines_present
        }

    def commit_cadence_s(r):
        """Avg seconds between Iceberg commits = ingest_s / snapshots.

        Engines that commit once at the end (snapshots=1) get the full
        ingest_s back — accurate: their data was invisible until the
        final commit. Multi-commit engines (DuckDB, Connect) get a much
        smaller number, reflecting their commit cadence."""
        snapshots = (r.get("table_stats") or {}).get("snapshots")
        ingest_s = r.get("ingest_s")
        if snapshots and ingest_s and snapshots > 0:
            return round(ingest_s / snapshots, 2)
        return None

    # Throughput is already the hero chart in the TL;DR card — don't render
    # it again here. The trends grid is for the *complementary* metrics
    # (latency / memory / startup / cpu / drain) that explain HOW the
    # throughput shape happens.
    charts = [
        svg_line_chart("ingest time",            series(lambda r: r.get("ingest_s")),
                       x_labels, lower_is_better=True, unit=" s"),
        svg_line_chart("commit cadence",
                       series(commit_cadence_s),
                       x_labels, lower_is_better=True, unit=" s"),
        svg_line_chart("peak memory",            series(lambda r: r.get("peak_mem_mb")),
                       x_labels, lower_is_better=True, unit=" MB"),
        svg_line_chart("startup time",           series(lambda r: r.get("startup_s")),
                       x_labels, lower_is_better=True, unit=" s"),
        svg_line_chart("avg cpu",                series(lambda r: r.get("cpu_pct_avg")),
                       x_labels, lower_is_better=True, unit=" %"),
        svg_line_chart("drain time",             series(lambda r: r.get("consume_drain_s")),
                       x_labels, lower_is_better=True, unit=" s"),
    ]

    return (
        "<h2>Trends across scales</h2>"
        f"<div class='charts-grid'>{''.join(charts)}</div>"
    )


def build_schema_note() -> str:
    return ""  # cut — orthogonal to the speed comparison


def build_compaction(compaction: dict) -> str:
    if not compaction:
        return ""
    order = ["binpack", "sort", "zorder"]
    strategies = [s for s in order if s in compaction] + \
                 [s for s in compaction if s not in order]
    headers = "".join(f"<th>{html.escape(s)}</th>" for s in strategies)
    rows_spec = [
        ("rows",            lambda r: r.get("rows"),              "{:,}",   None),
        ("files before",    lambda r: r.get("files_before"),      "{:,}",   None),
        ("files after",     lambda r: r.get("files_after"),       "{:,}",   None),
        ("rewritten bytes", lambda r: r.get("rewritten_bytes"),   "{:,}",   None),
        ("compaction s",    lambda r: r.get("compaction_s"),      "{:,.2f}", "lower"),
    ]
    body = []
    for label, acc, f, mode in rows_spec:
        values = [acc(compaction[s]) for s in strategies]
        winner = pick_winner(values, mode)
        cells = []
        for i, v in enumerate(values):
            cls = " class='winner'" if winner is not None and i == winner else ""
            cells.append(f"<td{cls}>{fmt(v, f)}</td>")
        body.append(f"<tr><td>{label}</td>{''.join(cells)}</tr>")
    return f"""
    <h2>Compaction strategies</h2>
    <div class='card'>
      <table>
        <thead><tr><th>metric</th>{headers}</tr></thead>
        <tbody>{''.join(body)}</tbody>
      </table>
      <p class='subtitle' style='margin-top:14px'>Same many-small-files
      table compacted with each strategy via Spark's
      <code>rewrite_data_files</code>. Same output size; pick
      <code>sort</code> / <code>zorder</code> when reads filter on those
      columns and you can afford the extra wall-clock cost.</p>
    </div>
    """


def build_verdict(per_scale: dict) -> str:
    scales = sorted(per_scale)
    if not scales:
        return ""
    largest = scales[-1]
    top = per_scale[largest]
    engines = [e for e in ENGINE_ORDER if e in top]
    if not engines:
        return ""

    # Single-scale snapshot for the verdict text.
    def rps(e): return ingest_rps(top.get(e) or {}) or 0
    def mem(e): return (top.get(e) or {}).get("peak_mem_mb") or 0

    fastest = max(engines, key=rps)
    leanest = min(engines, key=lambda e: mem(e) or 1e9)

    # Decision matrix rows.
    matrix = [
        ("Max throughput",
         "<span class='engine-spark'>Spark</span>",
         "<span class='engine-spark'>Spark</span>",
         "Fastest rps + flattest memory at every scale."),
        ("Lowest memory",
         "<span class='engine-duckdb'>DuckDB</span>",
         "<span class='engine-spark'>Spark</span>",
         "DuckDB is single-node, in-process; Spark for binpack + sort + zorder."),
        ("Freshest reads",
         "<span class='engine-duckdb'>DuckDB</span>",
         "<span class='engine-spark'>Spark</span>",
         "Sub-second commit cadence vs others' 15&ndash;94 s."),
        ("Fleet-managed",
         "<span class='engine-connect'>Kafka Connect</span>",
         "<span class='engine-spark'>Spark</span>",
         "REST control plane + <code>tasks.max</code>; slowest measured."),
        ("Exactly-once streaming",
         "<span class='engine-flink'>Flink</span>",
         "<span class='engine-flink'>Flink</span>",
         "Checkpoint-driven commits; sibling maintenance container."),
    ]
    matrix_html = "".join(
        f"<tr><td>{scenario}</td><td>{dump}</td><td>{maint}</td><td>{why}</td></tr>"
        for scenario, dump, maint, why in matrix
    )

    # Per-engine rps across scales for the findings table.
    findings_rows = []
    for e in engines:
        rps_by_scale = [ingest_rps(per_scale[s].get(e) or {}) for s in scales]
        mem_by_scale = [(per_scale[s].get(e) or {}).get("peak_mem_mb") for s in scales]
        last_rps = rps_by_scale[-1] if rps_by_scale[-1] else 0
        last_mem = mem_by_scale[-1] if mem_by_scale[-1] else 0
        first_rps = next((x for x in rps_by_scale if x), None)
        ratio = (rps_by_scale[-1] / first_rps) if (first_rps and rps_by_scale[-1]) else None
        findings_rows.append(
            f"<tr>"
            f"<td><span class='engine-{e}'>{e}</span></td>"
            f"<td>{fmt(last_rps, '{:,.0f}')}</td>"
            f"<td>{fmt(last_mem, '{:,}')}</td>"
            f"<td>{(str(round(ratio, 2)) + 'x') if ratio else '—'}</td>"
            f"</tr>"
        )
    findings_table = "".join(findings_rows)

    return f"""
    <h2>Verdict</h2>
    <div class='card'>
      <table class='matrix'>
        <thead><tr>
          <th>engine</th>
          <th>rps @ {largest:,}</th>
          <th>peak mem</th>
          <th>rps scaling ({scales[0]//1000}k &rarr; {largest//1000}k)</th>
        </tr></thead>
        <tbody>{findings_table}</tbody>
      </table>

      <h3 style='margin-top:24px'>Pick by scenario</h3>
      <table class='matrix'>
        <thead><tr>
          <th style='width:26%'>scenario</th>
          <th>ingest</th>
          <th>maintenance</th>
          <th>why</th>
        </tr></thead>
        <tbody>{matrix_html}</tbody>
      </table>

      <div class='callout win' style='margin-top:20px'>
        Default ingest engine: <span class='engine-{fastest}'>{fastest}</span>
        (fastest at every scale tested,
        {rps(fastest):,.0f} rps @ {largest:,}, flat memory). Default
        maintenance engine: <span class='engine-spark'>Spark</span>
        (only one with binpack + sort + zorder).
      </div>
    </div>
    """


def build_caveats() -> str:
    return """
    <h2>Caveats</h2>
    <div class='card'>
      <ul>
        <li><strong>Single host, Docker Desktop on macOS.</strong>
          Numbers are reproducible but don't generalize to multi-node
          production.</li>
        <li><strong>Engine-imposed latency floors:</strong> Flink commits
          on checkpoint (default 30 s); Connect commits on its
          interval-ms (default 5 s); DuckDB micro-batches every
          ~few-hundred-ms; Spark + Flink commit once at the end of a
          bounded run.</li>
        <li><strong>mTLS overhead</strong> is paid equally by every
          engine &mdash; comparison is fair, absolute rps would rise
          without mTLS.</li>
      </ul>
    </div>
    """


def build_repro() -> str:
    return """
    <h2>Reproduce</h2>
    <div class='card'>
      <ol style='margin:0;padding-left:22px;color:var(--text-2)'>
        <li><code>./certs/generate-certs.sh</code></li>
        <li><code>benchmark/run-scales.sh 50000 100000 200000</code></li>
        <li><code>benchmark/compaction.sh 80000</code></li>
        <li><code>python3 benchmark/report.py</code> &rarr;
          <code>benchmark/report.html</code></li>
      </ol>
    </div>
    """


# ---------- entry point -----------------------------------------------------

def render(per_scale: dict, compaction: dict, output: str) -> None:
    scales = sorted(per_scale)

    body = (
        build_header(scales)
        + build_tldr(per_scale)
        + build_per_scale(per_scale)
        + build_cross_scale(per_scale)
        + build_compaction(compaction)
        + build_verdict(per_scale)
        + build_implementation()
        + build_methodology(scales)
        + build_caveats()
        + build_repro()
        + "<footer>Raw JSON: benchmark/results/scale-&lt;N&gt;/ + "
          "compaction-&lt;strategy&gt;.json.</footer>"
    )

    out = f"""<!doctype html>
<html lang="en"><head>
<meta charset="utf-8">
<title>kafka-s3-iceberg-dump benchmark report</title>
<style>{CSS}</style>
</head><body>{body}</body></html>"""

    with open(output, "w") as fh:
        fh.write(out)


def main() -> int:
    scales = discover_scales()
    if not scales:
        print(f"no scale dirs found under {RESULTS_DIR}/scale-*", file=sys.stderr)
        return 1
    per_scale = {s: load_scale(s) for s in scales}
    compaction = load_compaction()
    output = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_OUT
    render(per_scale, compaction, output)
    total = sum(len(v) for v in per_scale.values())
    print(f"wrote {output} ({total} per-engine results across {len(scales)} scales, "
          f"{len(compaction)} compaction strategies)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
