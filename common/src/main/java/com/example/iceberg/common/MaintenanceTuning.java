package com.example.iceberg.common;

/**
 * Iceberg table-maintenance tuning, single-sourced.
 *
 * <p>Every engine maintains its own table with these settings: Flink runs
 * maintenance in-job, Spark runs it via native Iceberg procedures on an
 * interval, and Connect and DuckDB run the standalone {@code MaintenanceJob}.
 */
public final class MaintenanceTuning {

  private MaintenanceTuning() {}

  /** Operator UID suffix for the Flink maintenance pipeline. */
  public static final String UID_SUFFIX = "iceberg-maintenance";

  /** Minimum gap between maintenance runs. */
  public static final int RATE_LIMIT_MINUTES = 1;
  /** Delay between lock-availability checks. */
  public static final int LOCK_CHECK_DELAY_SECONDS = 10;
  /** Interval between interval-driven maintenance passes (Spark engine). */
  public static final int MAINTENANCE_INTERVAL_MINUTES = 2;

  // --- RewriteDataFiles (compaction) ---
  /** Compact after this many table commits. */
  public static final int REWRITE_ON_COMMIT_COUNT = 3;
  /** Target compacted data-file size (64 MB). */
  public static final long TARGET_FILE_SIZE_BYTES = 64L * 1024 * 1024;
  /** Cap on commits per partial-progress compaction run. */
  public static final int REWRITE_PARTIAL_PROGRESS_MAX_COMMITS = 2;
  /**
   * Compaction strategy for Spark's {@code rewrite_data_files} procedure:
   * {@code binpack} (default), {@code sort} or {@code zorder}. The Flink
   * table-maintenance API only supports binpack; this knob drives the Spark
   * maintenance job and the compaction-strategy benchmark.
   */
  public static final String REWRITE_STRATEGY =
      Env.get("MAINTENANCE_REWRITE_STRATEGY", "binpack");
  /** Sort columns used by the {@code sort} compaction strategy. */
  public static final String SORT_COLUMNS = "event_time";
  /** Columns used by the {@code zorder} compaction strategy. */
  public static final String ZORDER_COLUMNS = "user_id, event_time";

  // --- ExpireSnapshots ---
  /** Expire snapshots after this many table commits. */
  public static final int EXPIRE_ON_COMMIT_COUNT = 5;
  /** Snapshots older than this are eligible for expiry. */
  public static final int MAX_SNAPSHOT_AGE_MINUTES = 5;
  /** Always keep at least this many recent snapshots. */
  public static final int RETAIN_LAST_SNAPSHOTS = 3;
  /** Batch size for deleting expired files. */
  public static final int EXPIRE_DELETE_BATCH_SIZE = 100;

  // --- DeleteOrphanFiles ---
  /**
   * Run orphan-file GC after this many commits. Higher than rewrite/expire
   * because orphan detection is the most expensive maintenance task.
   */
  public static final int ORPHAN_ON_COMMIT_COUNT = 10;
  /**
   * Minimum age before a file is eligible for orphan deletion. Kept well
   * above the ingest commit cadence so in-flight files are never mistaken
   * for orphans.
   */
  public static final int ORPHAN_MIN_AGE_MINUTES = 10;
  /** Batch size for deleting orphan files. */
  public static final int ORPHAN_DELETE_BATCH_SIZE = 100;
  /**
   * Use S3-style prefix listing when scanning for orphan files (correct for
   * S3FileIO / MinIO; falls back to a manifest scan when false).
   */
  public static final boolean ORPHAN_USE_PREFIX_LISTING = true;
}
