package com.example.iceberg.maintenance;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;

import com.example.iceberg.common.CatalogConfig;
import com.example.iceberg.common.Env;
import com.example.iceberg.common.MaintenanceTuning;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.hadoop.conf.Configuration;

import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.maintenance.api.ExpireSnapshots;
import org.apache.iceberg.flink.maintenance.api.JdbcLockFactory;
import org.apache.iceberg.flink.maintenance.api.RewriteDataFiles;
import org.apache.iceberg.flink.maintenance.api.TableMaintenance;
import org.apache.iceberg.flink.maintenance.api.TriggerLockFactory;

/**
 * Standalone Iceberg table-maintenance job — data-file compaction +
 * snapshot expiration for a <b>single</b> table, run in a local Flink
 * mini-cluster ({@code java -jar}).
 *
 * <p>The Flink ingest engine maintains its own table in-job; Spark has native
 * maintenance procedures. Kafka Connect and DuckDB have no native compaction,
 * so each runs its own {@code MaintenanceJob} container scoped to its own
 * table — {@code ICEBERG_DB.ICEBERG_TABLE}. Maintenance touches only the
 * Postgres catalog and S3, never Kafka.
 */
public final class MaintenanceJob {

  private MaintenanceJob() {}

  public static void main(String[] args) throws Exception {
    final String dbName = Env.get("ICEBERG_DB", "connect_db");
    final String tableName = Env.get("ICEBERG_TABLE", "events");

    Map<String, String> catalogProps = CatalogConfig.jdbcCatalogProperties();
    Configuration hadoopConf = new Configuration(false);
    CatalogLoader catalogLoader =
        CatalogLoader.custom(
            CatalogConfig.CATALOG_NAME, catalogProps, hadoopConf,
            "org.apache.iceberg.jdbc.JdbcCatalog");

    TableIdentifier tableId = TableIdentifier.of(dbName, tableName);
    // The engine owns table creation; maintenance only maintains it.
    awaitTable(catalogLoader, tableId);

    TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, tableId);

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.enableCheckpointing(Duration.ofSeconds(30).toMillis());

    // Lock prevents two maintenance runs colliding; shares the catalog Postgres.
    TriggerLockFactory lockFactory =
        new JdbcLockFactory(
            CatalogConfig.jdbcUri(),
            dbName + "." + tableName,
            Map.of(
                "jdbc.user", CatalogConfig.jdbcUser(),
                "jdbc.password", CatalogConfig.jdbcPassword(),
                JdbcLockFactory.INIT_LOCK_TABLES_PROPERTY, "true"));

    TableMaintenance.forTable(env, tableLoader, lockFactory)
        .uidSuffix(MaintenanceTuning.UID_SUFFIX)
        .rateLimit(Duration.ofMinutes(MaintenanceTuning.RATE_LIMIT_MINUTES))
        .lockCheckDelay(Duration.ofSeconds(MaintenanceTuning.LOCK_CHECK_DELAY_SECONDS))
        .add(
            RewriteDataFiles.builder()
                .scheduleOnCommitCount(MaintenanceTuning.REWRITE_ON_COMMIT_COUNT)
                .targetFileSizeBytes(MaintenanceTuning.TARGET_FILE_SIZE_BYTES)
                .partialProgressEnabled(true)
                .partialProgressMaxCommits(MaintenanceTuning.REWRITE_PARTIAL_PROGRESS_MAX_COMMITS))
        .add(
            ExpireSnapshots.builder()
                .scheduleOnCommitCount(MaintenanceTuning.EXPIRE_ON_COMMIT_COUNT)
                .maxSnapshotAge(Duration.ofMinutes(MaintenanceTuning.MAX_SNAPSHOT_AGE_MINUTES))
                .retainLast(MaintenanceTuning.RETAIN_LAST_SNAPSHOTS)
                .deleteBatchSize(MaintenanceTuning.EXPIRE_DELETE_BATCH_SIZE))
        .append();

    env.execute("iceberg-maintenance-" + dbName + "." + tableName);
  }

  /** Block until the target table exists (the engine creates it first). */
  private static void awaitTable(CatalogLoader catalogLoader, TableIdentifier tableId)
      throws InterruptedException {
    Catalog catalog = catalogLoader.loadCatalog();
    try {
      for (int attempt = 1; ; attempt++) {
        try {
          if (catalog.tableExists(tableId)) {
            System.out.println("table " + tableId + " found; starting maintenance");
            return;
          }
        } catch (Exception e) {
          // catalog/namespace not ready yet — keep waiting
        }
        System.out.println("waiting for table " + tableId + " (attempt " + attempt + ")");
        Thread.sleep(5000);
      }
    } finally {
      if (catalog instanceof Closeable c) {
        try {
          c.close();
        } catch (IOException ignored) {
          // best-effort close
        }
      }
    }
  }
}
