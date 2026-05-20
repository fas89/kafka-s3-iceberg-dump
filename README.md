# Kafka S3 Iceberg Dump

A demo that streams JSON events from **Kafka (mutual-TLS)** into **Apache
Iceberg tables on S3 (MinIO)** using **four interchangeable ingestion engines**
— Apache Flink, Apache Spark, DuckDB and Kafka Connect — each writing its own
table and running its own Iceberg table maintenance (data-file compaction +
snapshot expiration).

It is an all-Java **Maven multi-module monorepo**: every engine, the producer,
the maintenance job and the metrics tool build to a self-contained fat jar.
The whole stack runs in Docker Compose; no local JDK or Maven is needed.

## The four engines

| Engine | Module | Writes | Maintenance |
|---|---|---|---|
| Apache Flink | `engine-flink` | `flink_db.events` | in-job `TableMaintenance` |
| Apache Spark | `engine-spark` | `spark_db.events` | in-job native Iceberg procedures |
| DuckDB | `engine-duckdb` | `duckdb_db.events` | standalone `MaintenanceJob` container |
| Kafka Connect | `engines/connect` | `connect_db.events` | standalone `MaintenanceJob` container |

All four read the **same** `events` topic and the **same** Postgres Iceberg
JDBC catalog, and write to the **same** MinIO warehouse — only the namespace
differs, so the engines can be compared side by side.

## Architecture

```
        producer ──JSON over mTLS──▶ Kafka topic "events" (3 partitions)
                                            │
        ┌───────────────┬───────────────────┼───────────────────┬──────────────┐
        ▼               ▼                   ▼                   ▼
   engine-flink     engine-spark        engine-duckdb       Kafka Connect
   flink_db.events  spark_db.events     duckdb_db.events    connect_db.events
        │               │                   │                   │
        └───────────────┴─────────┬─────────┴───────────────────┘
                                  ▼
                  Iceberg tables on MinIO (S3)        Postgres
                  warehouse/<engine>_db/events/   ◀── Iceberg JDBC catalog
                                                      (DuckDB reaches it via
                                                       an iceberg-rest facade)

  maintenance:  Flink in-job · Spark in-job procedures ·
                Connect & DuckDB via standalone flink-maintenance containers
```

* **Catalog:** one Iceberg **JDBC catalog** on Postgres. The three JVM engines
  (Flink, Spark, Connect) use it directly; DuckDB's iceberg extension speaks
  only the REST protocol, so the DuckDB overlay adds an `iceberg-rest` facade
  backed by that same Postgres.
* **Storage:** MinIO via Iceberg `S3FileIO` (path-style, `s3://warehouse`).
* **Tables:** `<engine>_db.events` — `id, event_type, user_id, amount,
  event_time` — partitioned (where the engine supports it) by `event_type`, so
  the stream produces many small files for compaction to coalesce.

## Stack (pinned, mutually compatible — May 2026)

| Component | Version | Notes |
|---|---|---|
| Java | 21 | every module compiles to Java 21 bytecode |
| Apache Flink | 2.0.2 | `iceberg-flink-runtime-2.0` |
| Apache Spark | 4.0.0 | Scala 2.13, `iceberg-spark-runtime-4.0_2.13` |
| Apache Kafka | 4.0.0 | KRaft mode, mTLS `SSL` listener |
| Apache Iceberg | 1.10.2 | JDBC catalog + `S3FileIO` |
| DuckDB JDBC | 1.5.2.1 | `iceberg` + `httpfs` extensions |
| Kafka Connect | cp-kafka-connect 7.8.0 | + `iceberg-kafka-connect` 1.9.2 |
| MinIO / Postgres | alpine/minio 2025-10 / pg 17.10 | S3 storage / Iceberg catalog |
| Maven | 3.9.9 | runs in a container; no local JDK/Maven needed |

## Repository layout

```
common/              pure-Java helpers — env config, JDBC-catalog properties,
                     Kafka mTLS properties, maintenance tuning constants
flink-maintenance/   standalone Iceberg MaintenanceJob (compaction + snapshot
                     expiration) for a single table — Flink
spark-maintenance/   the same, as a standalone Spark job — the selectable
                     alternative for the DuckDB / Connect tables
engine-flink/        Flink ingest — in-job TableMaintenance
engine-spark/        Spark Structured Streaming ingest — in-job procedures
engine-duckdb/       DuckDB micro-batch ingest (kafka-clients + DuckDB JDBC)
producer/            Java mTLS event producer — continuous or bounded
tools/               Iceberg table-metrics reader for the benchmark
engines/connect/     Kafka Connect Iceberg sink — iceberg-sink.json + Dockerfile
certs/               local-CA mTLS material generator
benchmark/           per-engine benchmark scripts (run.sh, run-all.sh)
tests/               mTLS-negative and failure-recovery integration tests
docker-compose.yml             shared infra (MinIO, Postgres, Kafka, builder, producer)
docker-compose.<engine>.yml    per-engine overlay
```

## Prerequisites

* Docker / Docker Compose, and `openssl` on the host (for cert generation).
* **≥ 6 GB of memory available to Docker.** Each engine plus Kafka, MinIO and
  Postgres needs it; the Spark and Flink overlays are the heaviest.

## Quick start

```bash
# 1. Generate the mTLS material (local CA + per-principal PKCS12 keystores).
#    Run once; outputs land in certs/ and are git-ignored.
./certs/generate-certs.sh

# 2. Bring up the shared infra plus one engine.
docker compose -f docker-compose.yml -f docker-compose.flink.yml   up --build
docker compose -f docker-compose.yml -f docker-compose.spark.yml   up --build
docker compose -f docker-compose.yml -f docker-compose.duckdb.yml  up --build
docker compose -f docker-compose.yml -f docker-compose.connect.yml up --build
```

Each run, in order: `builder` compiles every module's fat jar in a Maven
container; MinIO / Postgres / Kafka start; the chosen engine starts and creates
its table; the `producer` streams synthetic JSON events into `events`. The
first run downloads images and Maven dependencies, so allow a few minutes.

## Verify it works

* **Flink UI** — http://localhost:8081 (flink overlay) — the ingest job plus
  the `RewriteDataFiles` / `ExpireSnapshots` maintenance operators.
* **MinIO console** — http://localhost:9001 (`admin` / `password`). Browse
  `warehouse/<engine>_db/events/`: `data/` file count grows as events arrive
  and drops after a compaction run; `metadata/` snapshots are pruned by
  expiration.
* **Table metrics** — read row / data-file / snapshot counts straight from the
  catalog:

  ```bash
  docker compose -f docker-compose.yml -f docker-compose.flink.yml \
    run --rm -e ICEBERG_DB=flink_db tools
  ```

## Maintenance

Every engine maintains its own table with identical tuning
(`common/.../MaintenanceTuning.java`): compact to ~64 MB files, keep the last
3 snapshots, expire snapshots older than 5 minutes.

* **Flink** runs `TableMaintenance` inside the ingest job.
* **Spark** runs `rewrite_data_files` / `expire_snapshots` procedures on a
  timer inside the Spark application.
* **DuckDB** and **Kafka Connect** have no native compaction, so their overlays
  run a standalone `maintenance` container scoped to that one table. It is
  **Flink-based by default**; layering `docker-compose.maintenance-spark.yml`
  swaps in the Spark maintenance job, so the same table can be maintained by
  either engine:

  ```bash
  # Flink maintenance (default)
  docker compose -f docker-compose.yml -f docker-compose.duckdb.yml up --build
  # Spark maintenance
  docker compose -f docker-compose.yml -f docker-compose.duckdb.yml \
                 -f docker-compose.maintenance-spark.yml up --build
  ```

## Benchmark

```bash
benchmark/run.sh flink 50000      # one engine: clean stack, produce N, measure
benchmark/run-all.sh 50000        # all four engines + a comparison table
benchmark/compaction.sh 200000    # binpack vs sort vs zorder compaction speed
```

`run.sh` brings up a clean stack, runs the producer in bounded mode, then
**polls until all N rows have landed** in Iceberg — so it measures real ingest
speed instead of waiting a fixed delay. Each per-engine result is written as a
nested JSON document to `benchmark/results/<engine>.json` and reports:

* **Correctness** — `requested`, `final_rows`, `correct` (exact-match
  verdict).
* **Producer** — its own throughput summary (`producer.throughput_rps`,
  `producer.delivered`, `producer.failed`) emitted by the Java producer on
  shutdown, so a slow producer is visible separately from a slow engine.
* **Speed** — `startup_s` (producer start → first Iceberg commit, the
  engine's reaction time), `consume_drain_s` (producer finish → last commit,
  wall clock), `ingest_s` (end to end, producer start → last commit),
  `ingest_throughput_rps`. Consumption and the S3 write are interleaved in
  every streaming engine, so they are not reported separately.
* **Efficiency** — `peak_mem_mb` and `cpu_pct_avg` summed across the engine's
  own containers, sampled every 5 s with `docker stats`.
* **Table state** — under `table_stats`: `rows`, `data_files`, `size_bytes`,
  `snapshots`, `write_window_s` (first → last append timestamp),
  `compactions`, `compaction_files_rewritten` after the engine's own
  maintenance.

`run-all.sh` repeats this per engine and calls `benchmark/compare.py`, which
prints a **transposed** comparison table (metrics on rows, engines on
columns) with derived stats (e.g. `avg data file (KB)` from `size_bytes /
data_files`). The host needs `python3` (built in on macOS / most Linuxes).

`compaction.sh` is a separate, controlled comparison: it writes an identical
many-small-files table and runs Iceberg's `rewrite_data_files` with each
strategy — `binpack`, `sort`, `zorder` — timing each. Strategy comparison is
Spark-driven (Flink's table-maintenance API only does binpack).

## Tests

```bash
./mvnw test                       # JUnit unit tests (common + engine-flink)
tests/mtls-negative.sh            # a no-certificate client is rejected
tests/recovery.sh                 # kill an engine mid-run; final count intact
```

The unit tests cover the `common` helpers and the Flink `JsonToRowData` mapper.
The integration scripts exercise the running stack.

## Build

```bash
./mvnw -DskipTests package         # builds every module's fat jar
./mvnw test                        # runs the unit tests
```

`pom.xml` is a Maven multi-module reactor; each engine/producer/tool module
emits a shaded `target/app.jar`. No local JDK/Maven is needed for the Compose
path — the `builder` service compiles in a `maven:3.9.9-eclipse-temurin-21`
container.

## Security notes

This is a local demo — hardened to be a realistic reference, not a production
deployment:

* **Kafka is mutual-TLS only.** The broker runs a single `SSL` listener with
  `ssl.client.auth=required`; the producer and all four engines present a
  CA-signed PKCS12 keystore from `certs/generate-certs.sh`. There is no
  PLAINTEXT listener — `tests/mtls-negative.sh` proves a client with no
  certificate is rejected.
* **Published ports are bound to `127.0.0.1`** — MinIO, the Flink UI, the
  Connect REST API, Postgres and the S3 endpoint are reachable only from the
  local host.
* **Credentials are demo defaults** — MinIO (`admin`/`password`), Postgres
  (`iceberg`/`iceberg`) and the keystore password (`changeit`) are fixed and
  weak, acceptable only because the stack is local and mTLS-walled. For real
  use, change them in the compose `environment:` blocks, the engine catalog
  env vars (`common/.../CatalogConfig.java`), `CERT_STOREPASS`, and
  `engines/connect/iceberg-sink.json`.
* Generated cert material (`certs/*.p12`, keys, `creds`) is git-ignored and
  never committed.

## Versioning

Releases are tagged with
[git-semver-release](https://github.com/michalstutzmann/git-semver-release)
using **manual bumps** — pick the SemVer level explicitly per release.

```bash
git-semver-release version          # show current version
git-semver-release minor --push     # tag + push the release
```

`pom.xml` uses a [Maven CI-friendly
version](https://maven.apache.org/guides/mini/guide-maven-ci-friendly.html):
`<version>${revision}</version>`, defaulting to `0.0.0-SNAPSHOT`. The version
is never hardcoded — feed it via `-Drevision`, or via the `APP_VERSION` env var
which the compose `builder` forwards:

```bash
./mvnw -DskipTests -Drevision="$(git-semver-release version)" package
APP_VERSION="$(git-semver-release version)" docker compose \
  -f docker-compose.yml -f docker-compose.flink.yml up --build
```

## Teardown

```bash
docker compose -f docker-compose.yml -f docker-compose.flink.yml down -v
```
