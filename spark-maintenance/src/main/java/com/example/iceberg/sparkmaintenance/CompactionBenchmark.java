package com.example.iceberg.sparkmaintenance;

import java.util.Locale;

import com.example.iceberg.common.CatalogConfig;
import com.example.iceberg.common.Env;
import com.example.iceberg.common.MaintenanceTuning;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Self-contained compaction-strategy benchmark.
 *
 * <p>Generates synthetic event rows, writes them as many small data files into
 * a fresh {@code bench_db} table, then runs Iceberg's {@code rewrite_data_files}
 * with the configured strategy ({@link MaintenanceTuning#REWRITE_STRATEGY}:
 * binpack / sort / zorder), times the call, and prints one JSON metrics line.
 *
 * <p>No Kafka and no streaming engine are involved, so each run is controlled
 * and repeatable. {@code benchmark/compaction.sh} runs it once per strategy.
 *
 * <p>Run via {@code spark-submit --class
 * com.example.iceberg.sparkmaintenance.CompactionBenchmark}.
 */
public final class CompactionBenchmark {

  private static final String CATALOG = CatalogConfig.CATALOG_NAME;

  private CompactionBenchmark() {}

  public static void main(String[] args) {
    String strategy = MaintenanceTuning.REWRITE_STRATEGY;
    String db = Env.get("ICEBERG_DB", "bench_db");
    String table = "compaction_" + strategy;
    long rows = Long.parseLong(Env.get("COMPACTION_ROWS", "200000"));
    int files = Integer.parseInt(Env.get("COMPACTION_FILES", "64"));
    String fqName = CATALOG + "." + db + "." + table;

    SparkSession spark = SparkMaintenanceJob.buildSession();
    spark.sparkContext().setLogLevel("WARN");
    spark.sql("CREATE DATABASE IF NOT EXISTS " + CATALOG + "." + db);

    // Generate synthetic events and write them as `files` small data files.
    spark
        .range(0, rows)
        .selectExpr(
            "cast(id as string) as id",
            "element_at("
                + "array('click','view','purchase','scroll','signup'),"
                + " cast(id % 5 as int) + 1) as event_type",
            "concat('user-', cast(id % 1000 as string)) as user_id",
            "round(rand() * 100, 2) as amount",
            "current_timestamp() as event_time")
        .repartition(files)
        .writeTo(fqName)
        .using("iceberg")
        .createOrReplace();

    long filesBefore = dataFileCount(spark, fqName);

    // Time one rewrite_data_files pass with the configured strategy.
    long startMs = System.currentTimeMillis();
    Row result =
        spark.sql(SparkMaintenanceJob.rewriteDataFilesSql(CATALOG, db + "." + table)).first();
    double compactionSeconds = (System.currentTimeMillis() - startMs) / 1000.0;

    long rewrittenFiles = getLong(result, "rewritten_data_files_count");
    long addedFiles = getLong(result, "added_data_files_count");
    long rewrittenBytes = getLong(result, "rewritten_bytes_count");
    long filesAfter = dataFileCount(spark, fqName);

    System.out.println(
        String.format(
            Locale.ROOT,
            "{\"strategy\":\"%s\",\"rows\":%d,\"files_before\":%d,\"files_after\":%d,"
                + "\"rewritten_files\":%d,\"added_files\":%d,\"rewritten_bytes\":%d,"
                + "\"compaction_s\":%.2f}",
            strategy, rows, filesBefore, filesAfter,
            rewrittenFiles, addedFiles, rewrittenBytes, compactionSeconds));

    spark.stop();
  }

  /** Counts the table's live data files via the Iceberg {@code .files} metadata table. */
  private static long dataFileCount(SparkSession spark, String fqName) {
    return spark.sql("SELECT count(*) FROM " + fqName + ".files").first().getLong(0);
  }

  /**
   * Reads a numeric column from the procedure result as a long, tolerating an
   * absent column and either an int or a long column type.
   */
  private static long getLong(Row row, String column) {
    try {
      int idx = row.fieldIndex(column);
      Object value = row.isNullAt(idx) ? null : row.get(idx);
      return (value instanceof Number) ? ((Number) value).longValue() : 0L;
    } catch (Exception e) {
      return 0L;
    }
  }
}
