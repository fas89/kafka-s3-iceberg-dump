package com.example.iceberg.tools;

import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

import com.example.iceberg.common.CatalogConfig;
import com.example.iceberg.common.Env;

import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.CloseableIterable;

/**
 * Reads benchmark metrics for one engine's Iceberg table — row count, data-file
 * count, total size, snapshot count, the write window and compaction facts —
 * straight from the shared JDBC catalog via the Iceberg Java API, and prints
 * them as a single JSON line.
 *
 * <p>Row and data-file counts come from a scan of the table's live data files,
 * not the snapshot summary, so engines that omit the {@code total-records}
 * summary property (such as DuckDB) are still counted. The write window and
 * compaction counts come from the snapshot log — each snapshot's
 * {@code operation} and {@code timestampMillis}.
 *
 * <p>Usage: {@code java -jar tools.jar --db flink_db --table events} (or via
 * the {@code ICEBERG_DB} / {@code ICEBERG_TABLE} / {@code BENCH_ENGINE} env).
 */
public final class TableMetrics {

  private TableMetrics() {}

  public static void main(String[] args) {
    Map<String, String> opt = parseArgs(args);
    String db = opt.getOrDefault("db", Env.get("ICEBERG_DB", "flink_db"));
    String table = opt.getOrDefault("table", Env.get("ICEBERG_TABLE", "events"));
    String engine = opt.getOrDefault("engine", Env.get("BENCH_ENGINE", db));

    Catalog catalog =
        CatalogUtil.buildIcebergCatalog(
            CatalogConfig.CATALOG_NAME, CatalogConfig.jdbcCatalogProperties(), null);
    try {
      System.out.println(metricsJson(catalog, engine, db, table));
    } finally {
      if (catalog instanceof Closeable closeable) {
        try {
          closeable.close();
        } catch (IOException ignored) {
          // best-effort close
        }
      }
    }
  }

  /** One JSON line of table metrics; {@code exists:false} if the table is absent. */
  private static String metricsJson(Catalog catalog, String engine, String db, String table) {
    long rows = 0;
    long dataFiles = 0;
    long sizeBytes = 0;
    int snapshots = 0;
    long firstAppendMs = Long.MAX_VALUE;
    long lastAppendMs = 0;
    int compactions = 0;
    long compactionFilesRewritten = 0;
    boolean exists = false;
    try {
      Table tbl = catalog.loadTable(TableIdentifier.of(db, table));
      exists = true;
      // Snapshot log: append/overwrite snapshots bound the write window;
      // replace snapshots are compaction (RewriteDataFiles) commits.
      for (Snapshot snap : tbl.snapshots()) {
        snapshots++;
        String op = snap.operation();
        if ("append".equals(op) || "overwrite".equals(op)) {
          firstAppendMs = Math.min(firstAppendMs, snap.timestampMillis());
          lastAppendMs = Math.max(lastAppendMs, snap.timestampMillis());
        } else if ("replace".equals(op)) {
          compactions++;
          compactionFilesRewritten += parseLong(snap.summary().get("deleted-data-files"));
        }
      }
      // Count rows/files from the live data files, not the snapshot summary —
      // some writers omit the summary counters.
      if (tbl.currentSnapshot() != null) {
        try (CloseableIterable<FileScanTask> tasks = tbl.newScan().planFiles()) {
          for (FileScanTask task : tasks) {
            rows += task.file().recordCount();
            sizeBytes += task.file().fileSizeInBytes();
            dataFiles++;
          }
        }
      }
    } catch (NoSuchTableException e) {
      exists = false;
    } catch (Exception e) {
      // Table loaded but the scan failed — report what was counted so far.
      System.err.println("metrics scan failed for " + db + "." + table + ": " + e);
    }
    long firstAppendOut = (firstAppendMs == Long.MAX_VALUE) ? 0L : firstAppendMs;
    double writeWindowS =
        (lastAppendMs > 0 && lastAppendMs >= firstAppendMs)
            ? (lastAppendMs - firstAppendMs) / 1000.0
            : 0.0;
    return String.format(
        Locale.ROOT,
        "{\"engine\":\"%s\",\"db\":\"%s\",\"table\":\"%s\",\"exists\":%b,"
            + "\"rows\":%d,\"data_files\":%d,\"size_bytes\":%d,\"snapshots\":%d,"
            + "\"write_window_s\":%.1f,\"first_append_ms\":%d,\"last_append_ms\":%d,"
            + "\"compactions\":%d,\"compaction_files_rewritten\":%d}",
        engine, db, table, exists, rows, dataFiles, sizeBytes, snapshots,
        writeWindowS, firstAppendOut, lastAppendMs, compactions, compactionFilesRewritten);
  }

  private static long parseLong(String value) {
    try {
      return value == null ? 0L : Long.parseLong(value.trim());
    } catch (NumberFormatException e) {
      return 0L;
    }
  }

  private static Map<String, String> parseArgs(String[] args) {
    Map<String, String> opt = new LinkedHashMap<>();
    for (int i = 0; i < args.length; i++) {
      if (args[i].startsWith("--") && i + 1 < args.length) {
        opt.put(args[i].substring(2), args[++i]);
      }
    }
    return opt;
  }
}
