package com.example.iceberg.sparkmaintenance;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import com.example.iceberg.common.CatalogConfig;
import com.example.iceberg.common.Env;
import com.example.iceberg.common.MaintenanceTuning;

import org.apache.spark.sql.SparkSession;

/**
 * Standalone Spark Iceberg table-maintenance job — data-file compaction and
 * snapshot expiration for a <b>single</b> table, run via {@code spark-submit}.
 *
 * <p>This is the Spark counterpart of the {@code flink-maintenance}
 * {@code MaintenanceJob}. The Connect and DuckDB engines have no native
 * compaction of their own, so either maintenance job can maintain their
 * tables: the default is the Flink job, and the
 * {@code docker-compose.maintenance-spark.yml} overlay swaps in this one.
 *
 * <p>It runs continuously, one maintenance pass per
 * {@link MaintenanceTuning#MAINTENANCE_INTERVAL_MINUTES}. A pass that runs
 * before the engine has created the table simply fails and is retried.
 */
public final class SparkMaintenanceJob {

  // Catalog name — MUST equal CatalogConfig.CATALOG_NAME so this job resolves
  // the same tables the engines wrote (the JDBC catalog keys tables by name).
  private static final String CATALOG = CatalogConfig.CATALOG_NAME;

  // Spark timestamp-literal format for the expire_snapshots procedure.
  private static final DateTimeFormatter TS_LITERAL =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneOffset.UTC);

  private SparkMaintenanceJob() {}

  public static void main(String[] args) throws Exception {
    final String dbName = Env.get("ICEBERG_DB", "duckdb_db");
    final String tableName = Env.get("ICEBERG_TABLE", "events");
    final String fqTable = dbName + "." + tableName;

    SparkSession spark = buildSession();
    spark.sparkContext().setLogLevel("WARN");

    long intervalMs =
        Duration.ofMinutes(MaintenanceTuning.MAINTENANCE_INTERVAL_MINUTES).toMillis();
    System.out.println(
        "spark-maintenance started for " + CATALOG + "." + fqTable
            + " (one pass every " + MaintenanceTuning.MAINTENANCE_INTERVAL_MINUTES + " min)");

    // Runs continuously, like the flink-maintenance MaintenanceJob.
    while (true) {
      runMaintenance(spark, fqTable);
      Thread.sleep(intervalMs);
    }
  }

  /** Builds a {@link SparkSession} bound to the shared Iceberg JDBC catalog. */
  static SparkSession buildSession() {
    SparkSession.Builder builder =
        SparkSession.builder()
            .appName("spark-iceberg-maintenance")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog." + CATALOG, "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.defaultCatalog", CATALOG);
    // Reuse the shared JDBC-catalog properties under the catalog prefix.
    for (Map.Entry<String, String> e : CatalogConfig.jdbcCatalogProperties().entrySet()) {
      builder.config("spark.sql.catalog." + CATALOG + "." + e.getKey(), e.getValue());
    }
    return builder.getOrCreate();
  }

  /** One compaction + snapshot-expiration pass via Iceberg's Spark procedures. */
  private static void runMaintenance(SparkSession spark, String fqTable) {
    try {
      spark.sql(rewriteDataFilesSql(CATALOG, fqTable));
      String olderThan =
          TS_LITERAL.format(
              Instant.now().minus(
                  Duration.ofMinutes(MaintenanceTuning.MAX_SNAPSHOT_AGE_MINUTES)));
      spark.sql(
          "CALL " + CATALOG + ".system.expire_snapshots("
              + "table => '" + fqTable + "', "
              + "older_than => TIMESTAMP '" + olderThan + "', "
              + "retain_last => " + MaintenanceTuning.RETAIN_LAST_SNAPSHOTS + ")");
      System.out.println("maintenance pass complete for " + fqTable);
    } catch (Exception e) {
      // Best-effort: a failed pass (e.g. the table is not created yet) is
      // logged and retried on the next interval.
      System.err.println("maintenance pass skipped for " + fqTable + ": " + e);
    }
  }

  /**
   * Builds the {@code rewrite_data_files} CALL for the configured strategy
   * ({@link MaintenanceTuning#REWRITE_STRATEGY}: {@code binpack} default,
   * {@code sort} or {@code zorder}). Shared with {@link CompactionBenchmark}.
   */
  static String rewriteDataFilesSql(String catalog, String fqTable) {
    String strategy = MaintenanceTuning.REWRITE_STRATEGY;
    StringBuilder sql =
        new StringBuilder("CALL ")
            .append(catalog)
            .append(".system.rewrite_data_files(table => '")
            .append(fqTable)
            .append("'");
    if ("sort".equalsIgnoreCase(strategy)) {
      sql.append(", strategy => 'sort', sort_order => '")
          .append(MaintenanceTuning.SORT_COLUMNS)
          .append("'");
    } else if ("zorder".equalsIgnoreCase(strategy)) {
      sql.append(", strategy => 'sort', sort_order => 'zorder(")
          .append(MaintenanceTuning.ZORDER_COLUMNS)
          .append(")'");
    }
    // binpack is Iceberg's default — no strategy clause needed.
    return sql.append(", options => map('target-file-size-bytes','")
        .append(MaintenanceTuning.TARGET_FILE_SIZE_BYTES)
        .append("','min-input-files','2'))")
        .toString();
  }
}
