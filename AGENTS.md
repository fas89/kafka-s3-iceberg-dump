# AGENTS.md

Conventions and invariants for anyone — or any automated tooling — working in
this repository.

## What this is

A four-engine Kafka → Iceberg demo: a Maven multi-module monorepo where Apache
Flink, Apache Spark, DuckDB and Kafka Connect each ingest the same mTLS Kafka
topic into their own Iceberg table on MinIO (S3), backed by a shared Postgres
JDBC catalog, and each runs its own Iceberg table maintenance. See `README.md`.

## Build & run

```bash
./certs/generate-certs.sh                       # one-time: local CA + keystores
./mvnw -DskipTests package                      # build every module's fat jar
./mvnw test                                     # JUnit unit tests
docker compose -f docker-compose.yml -f docker-compose.<engine>.yml up --build
```

* `<engine>` is `flink`, `spark`, `duckdb` or `connect`.
* No local JDK/Maven required for the Compose path — the `builder` service
  compiles in a `maven:3.9.9-eclipse-temurin-21` container.
* Every module compiles to Java 21 bytecode (`maven.compiler.release=21`).

## Layout

| Path | Purpose |
|---|---|
| `common/` | pure-Java helpers: `Env`, `CatalogConfig`, `KafkaTls`, `MaintenanceTuning` |
| `flink-maintenance/` | standalone `MaintenanceJob` — compaction + snapshot expiration for one table (Flink) |
| `spark-maintenance/` | the same maintenance as a standalone Spark job (`SparkMaintenanceJob`) |
| `engine-flink/` | Flink ingest (`FlinkIngestJob`, `JsonToRowData`) + in-job maintenance |
| `engine-spark/` | Spark Structured Streaming ingest (`SparkIngestJob`) + in-job procedures |
| `engine-duckdb/` | DuckDB micro-batch ingest (`DuckDbIngestJob`) — kafka-clients + DuckDB JDBC |
| `producer/` | Java mTLS event producer (`EventProducer`) — continuous + bounded |
| `tools/` | `TableMetrics` — reads Iceberg table stats for the benchmark |
| `engines/connect/` | Kafka Connect Iceberg sink — `iceberg-sink.json` + `Dockerfile` (no Java module) |
| `certs/generate-certs.sh` | local CA → per-principal PKCS12 keystores |
| `benchmark/`, `tests/` | benchmark scripts; mTLS-negative + recovery integration tests |
| `docker-compose.yml` + `docker-compose.<engine>.yml` | shared infra + per-engine overlays |

## Architecture invariants — do not break

* **Flink is pinned to the 2.0 line, Spark to 4.0.** Iceberg ships
  `iceberg-flink-runtime-2.0` and `iceberg-spark-runtime-4.0_2.13`; do not bump
  `flink.version` / `spark.version` past a line with a matching Iceberg
  runtime. Versions live in `pom.xml` properties.
* **Catalog = one Iceberg JDBC catalog on Postgres.** Flink, Spark and Connect
  use it directly; DuckDB uses the `iceberg-rest` facade (in the duckdb
  overlay) backed by the same Postgres. All connection settings are
  environment-driven with container defaults — see `common/CatalogConfig.java`;
  do not hardcode hosts.
* **`kafka-clients` is not in the parent `dependencyManagement`.** A global pin
  would override the version the Flink/Spark Kafka connectors are tested
  against. The DuckDB engine and producer declare it directly.
* **`common` stays pure-Java** (its only dependency is test-scoped JUnit) so it
  is safe on any engine's classpath without shading conflicts.
* **Every engine owns its own table and its own maintenance.** Flink:
  `flink_db`, in-job `TableMaintenance`. Spark: `spark_db`, in-job native
  procedures. DuckDB: `duckdb_db`. Connect: `connect_db`. Connect and DuckDB
  get a standalone maintenance container — `flink-maintenance` by default, or
  `spark-maintenance` via the `docker-compose.maintenance-spark.yml` overlay
  (it overrides the `maintenance` service). Maintenance tuning is
  single-sourced in `MaintenanceTuning`.
* **The event schema is `id, event_type, user_id, amount, event_time`** and
  must stay consistent across the producer and every engine. The Flink
  `JsonToRowData` field order must match `FlinkIngestJob.SCHEMA`.
* **Checkpointing must stay enabled** for Flink and Spark — the Iceberg sink
  commits on checkpoint/trigger.
* **Kafka is mTLS-only.** Every Java client builds SSL properties via
  `KafkaTls.clientProperties(<principal>)`; the broker runs
  `ssl.client.auth=required`. Keep new clients on this path.
* `hadoop-client-api/runtime` are bundled deliberately — Iceberg's
  `CatalogLoader` needs `org.apache.hadoop.conf.Configuration` even with
  `S3FileIO`.

## Build & packaging

* `pom.xml` is a reactor; each engine/producer/tool module shades a fat jar to
  `target/app.jar` (`finalName=app`). The compose `builder` copies each to the
  `job` volume under a distinct name (`engine-flink.jar`, `producer.jar`, …).
* The project version is a [Maven CI-friendly
  version](https://maven.apache.org/guides/mini/guide-maven-ci-friendly.html):
  `<version>${revision}</version>`, `revision` defaulting to `0.0.0-SNAPSHOT`.
  **Never hardcode `<version>`** — feed it via `-Drevision` (or `APP_VERSION`,
  which the compose `builder` forwards). The `flatten-maven-plugin`
  (`resolveCiFriendliesOnly`) resolves it and is required on Maven 3 — keep it.
* Releases are tagged with
  [git-semver-release](https://github.com/michalstutzmann/git-semver-release)
  using **manual bumps** — no Conventional Commits auto-bump.

## Gotchas

* Needs **≥ 6 GB memory available to Docker**; the Spark and Flink overlays are
  heaviest. Memory is tuned lean in the compose files — keep it tuned.
* All image tags and `pom.xml` deps/plugins are **pinned** — no `latest` or
  floating tags. MinIO uses the maintained `alpine/minio` rebuild (`user: "0"`,
  no `mc`; `minio-setup` makes the bucket with `mkdir`).
* Generated cert material (`certs/*.p12`, keys, `creds`) and `.env` are
  git-ignored — never commit them.
* Re-running an engine overlay starts another instance; for a clean run use
  `docker compose ... down -v` first.
