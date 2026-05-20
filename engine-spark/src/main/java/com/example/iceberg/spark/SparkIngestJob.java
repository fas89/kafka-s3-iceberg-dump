package com.example.iceberg.spark;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.example.iceberg.common.CatalogConfig;
import com.example.iceberg.common.Env;
import com.example.iceberg.common.KafkaTls;
import com.example.iceberg.common.MaintenanceTuning;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamReader;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * Spark Structured Streaming ingest engine: reads JSON events from Kafka and
 * appends them to the Iceberg table {@code spark_db.events}, running Iceberg
 * table maintenance (data-file compaction + snapshot expiration) <b>in the same
 * Spark application</b> via the catalog's native maintenance procedures.
 *
 * <p>All knobs are environment-driven; see {@link CatalogConfig}.
 */
public final class SparkIngestJob {

  // Catalog name Spark binds the shared Iceberg JDBC catalog to. It MUST equal
  // CatalogConfig.CATALOG_NAME: the JDBC catalog keys every table by catalog
  // name, so Flink, Spark, the maintenance job and the tools module must all
  // use the same one or they cannot see each other's tables.
  private static final String CATALOG = CatalogConfig.CATALOG_NAME;

  // JSON event schema — must match the producer payload and the other engines:
  // id, event_type, user_id, amount, event_time.
  private static final StructType EVENT_SCHEMA =
      new StructType()
          .add("id", DataTypes.StringType)
          .add("event_type", DataTypes.StringType)
          .add("user_id", DataTypes.StringType)
          .add("amount", DataTypes.DoubleType)
          .add("event_time", DataTypes.TimestampType);

  // Spark timestamp-literal format for the expire_snapshots procedure.
  private static final DateTimeFormatter TS_LITERAL =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneOffset.UTC);

  private SparkIngestJob() {}

  public static void main(String[] args) throws Exception {
    final String kafkaBootstrap = Env.get("KAFKA_BOOTSTRAP", "kafka:9092");
    final String kafkaTopic = Env.get("KAFKA_TOPIC", "events");
    final String dbName = Env.get("ICEBERG_DB", "spark_db");
    final String tableName = Env.get("ICEBERG_TABLE", "events");
    final String checkpoint =
        Env.get("SPARK_CHECKPOINT", "/tmp/spark-checkpoint/" + dbName + "_" + tableName);

    SparkSession spark = buildSession();
    spark.sparkContext().setLogLevel("WARN");

    // The engine owns its table.
    spark.sql("CREATE DATABASE IF NOT EXISTS " + CATALOG + "." + dbName);
    spark.sql(
        "CREATE TABLE IF NOT EXISTS " + CATALOG + "." + dbName + "." + tableName + " ("
            + "  id STRING,"
            + "  event_type STRING,"
            + "  user_id STRING,"
            + "  amount DOUBLE,"
            + "  event_time TIMESTAMP"
            + ") USING iceberg PARTITIONED BY (event_type)");

    // ---- Kafka source (mTLS) -> parse JSON -> Iceberg sink ------------------
    Dataset<Row> raw = readKafka(spark, kafkaBootstrap, kafkaTopic);
    Dataset<Row> events =
        raw.select(from_json(col("value").cast("string"), EVENT_SCHEMA).alias("e"))
            .select("e.*");

    StreamingQuery query =
        events
            .writeStream()
            .format("iceberg")
            .outputMode("append")
            .trigger(Trigger.ProcessingTime("5 seconds"))
            .option("checkpointLocation", checkpoint)
            .toTable(CATALOG + "." + dbName + "." + tableName);

    // ---- in-job Iceberg maintenance (compaction + snapshot expiration) ------
    // Spark exposes maintenance as catalog procedures; run them on a timer
    // while the streaming query is live.
    ScheduledExecutorService maintenance = startMaintenance(spark, dbName, tableName);

    // Graceful shutdown: stop maintenance and the streaming query so the last
    // micro-batch commits cleanly before the container exits.
    Runtime.getRuntime()
        .addShutdownHook(new Thread(() -> shutdown(maintenance, query), "spark-shutdown"));

    query.awaitTermination();
  }

  /** Best-effort graceful stop of the maintenance timer and streaming query. */
  private static void shutdown(ScheduledExecutorService maintenance, StreamingQuery query) {
    maintenance.shutdownNow();
    try {
      query.stop();
    } catch (Exception e) {
      // best-effort stop during shutdown
    }
  }

  /** Builds a {@link SparkSession} bound to the shared Iceberg JDBC catalog. */
  private static SparkSession buildSession() {
    SparkSession.Builder builder =
        SparkSession.builder()
            .appName("spark-kafka-to-iceberg")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog." + CATALOG, "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.defaultCatalog", CATALOG);
    // Reuse the shared JDBC-catalog properties; Spark expects each catalog
    // property under the `spark.sql.catalog.<name>.` prefix.
    for (Map.Entry<String, String> e : CatalogConfig.jdbcCatalogProperties().entrySet()) {
      builder.config("spark.sql.catalog." + CATALOG + "." + e.getKey(), e.getValue());
    }
    return builder.getOrCreate();
  }

  /** Kafka streaming source with mutual-TLS client properties. */
  private static Dataset<Row> readKafka(SparkSession spark, String bootstrap, String topic) {
    DataStreamReader reader =
        spark
            .readStream()
            .format("kafka")
            .option("kafka.bootstrap.servers", bootstrap)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest");
    // The Kafka source forwards every `kafka.`-prefixed option to the client.
    for (Map.Entry<String, String> e : KafkaTls.clientProperties("spark").entrySet()) {
      reader = reader.option("kafka." + e.getKey(), e.getValue());
    }
    return reader.load();
  }

  /** Schedules periodic native-procedure maintenance on a daemon thread. */
  private static ScheduledExecutorService startMaintenance(
      SparkSession spark, String dbName, String tableName) {
    final String fqTable = dbName + "." + tableName;
    ScheduledExecutorService exec =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r, "iceberg-maintenance");
              t.setDaemon(true);
              return t;
            });
    long period = MaintenanceTuning.MAINTENANCE_INTERVAL_MINUTES;
    exec.scheduleWithFixedDelay(
        () -> runMaintenance(spark, fqTable), period, period, TimeUnit.MINUTES);
    return exec;
  }

  /** One compaction + snapshot-expiration pass via Iceberg's Spark procedures. */
  private static void runMaintenance(SparkSession spark, String fqTable) {
    try {
      spark.sql(
          "CALL " + CATALOG + ".system.rewrite_data_files("
              + "table => '" + fqTable + "', "
              + "options => map("
              + "'target-file-size-bytes','" + MaintenanceTuning.TARGET_FILE_SIZE_BYTES + "',"
              + "'min-input-files','2'))");
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
      // Maintenance is best-effort; a failed pass must not kill ingest.
      System.err.println("maintenance pass failed for " + fqTable + ": " + e);
    }
  }
}
