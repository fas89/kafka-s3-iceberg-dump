package com.example.iceberg.duckdb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.example.iceberg.common.CatalogConfig;
import com.example.iceberg.common.Env;
import com.example.iceberg.common.KafkaTls;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * DuckDB micro-batch ingest engine: a {@code kafka-clients} consumer pulls JSON
 * events over mutual TLS and DuckDB's {@code iceberg} extension writes each
 * batch into the Iceberg table {@code duckdb_db.events}.
 *
 * <p>DuckDB's iceberg extension speaks only the REST catalog protocol, so this
 * engine attaches to a REST-catalog facade (itself backed by the shared
 * Postgres catalog). Table maintenance runs separately as a standalone
 * {@code flink-maintenance} {@code MaintenanceJob} container.
 *
 * <p>Delivery is at-least-once: a batch is written to Iceberg, then Kafka
 * offsets are committed. A crash between those steps replays the batch.
 */
public final class DuckDbIngestJob {

  private DuckDbIngestJob() {}

  public static void main(String[] args) throws Exception {
    final String bootstrap = Env.get("KAFKA_BOOTSTRAP", "kafka:9092");
    final String topic = Env.get("KAFKA_TOPIC", "events");
    final String dbName = Env.get("ICEBERG_DB", "duckdb_db");
    final String tableName = Env.get("ICEBERG_TABLE", "events");
    final String restUri = Env.get("ICEBERG_REST_URI", "http://iceberg-rest:8181");
    // Each batch becomes one multi-row INSERT — i.e. one Iceberg commit. Kept
    // modest so a flush is a single reasonably sized statement.
    final int batchMaxRecords = Env.getInt("BATCH_MAX_RECORDS", 2000);
    final long batchMaxMs = Env.getInt("BATCH_MAX_MS", 5000);

    Class.forName("org.duckdb.DuckDBDriver");
    Connection con = connectDuckDb(restUri, dbName, tableName);
    String fqTable = "iceberg_catalog." + dbName + "." + tableName;

    KafkaConsumer<String, String> consumer = buildConsumer(bootstrap);
    consumer.subscribe(List.of(topic));

    // Graceful shutdown: the hook flips `running` and then blocks until the
    // poll loop has flushed its last batch and signalled the latch, so the
    // final micro-batch commits cleanly before the JVM halts.
    AtomicBoolean running = new AtomicBoolean(true);
    CountDownLatch stopped = new CountDownLatch(1);
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  running.set(false);
                  try {
                    stopped.await(10, TimeUnit.SECONDS);
                  } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                  }
                },
                "duckdb-shutdown"));

    ObjectMapper mapper = new ObjectMapper();
    List<JsonNode> batch = new ArrayList<>();
    long lastFlush = System.currentTimeMillis();
    long total = 0;
    System.out.println("duckdb-iceberg engine started -> " + dbName + "." + tableName);

    try {
      while (running.get()) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
        for (ConsumerRecord<String, String> rec : records) {
          try {
            batch.add(mapper.readTree(rec.value()));
          } catch (Exception malformed) {
            // skip malformed records
          }
        }
        boolean due = System.currentTimeMillis() - lastFlush >= batchMaxMs;
        if (!batch.isEmpty() && (batch.size() >= batchMaxRecords || due)) {
          total += commitBatch(con, fqTable, consumer, batch);
          batch.clear();
          lastFlush = System.currentTimeMillis();
        }
      }
      // Flush whatever is left after a graceful shutdown signal.
      if (!batch.isEmpty()) {
        total += commitBatch(con, fqTable, consumer, batch);
      }
      System.out.println("duckdb-iceberg engine stopped (total " + total + " rows)");
    } finally {
      closeQuietly(consumer);
      closeQuietly(con);
      stopped.countDown();
    }
  }

  /** Writes the batch to Iceberg, then commits Kafka offsets (at-least-once). */
  private static int commitBatch(
      Connection con,
      String fqTable,
      KafkaConsumer<String, String> consumer,
      List<JsonNode> batch)
      throws SQLException, InterruptedException {
    flushWithRetry(con, fqTable, batch);
    consumer.commitSync(); // commit offsets only after the Iceberg write
    System.out.println("committed " + batch.size() + " rows");
    return batch.size();
  }

  /** Opens DuckDB and attaches the Iceberg REST catalog, retrying on startup. */
  private static Connection connectDuckDb(String restUri, String dbName, String tableName)
      throws InterruptedException {
    final int attempts = 10;
    Exception last = null;
    for (int attempt = 1; attempt <= attempts; attempt++) {
      try {
        Connection con = DriverManager.getConnection("jdbc:duckdb:");
        initDuckDb(con, restUri, dbName, tableName);
        System.out.println("DuckDB attached to Iceberg REST catalog at " + restUri);
        return con;
      } catch (Exception e) {
        last = e; // catalog / S3 not ready yet — keep retrying
        System.out.println(
            "catalog not ready (attempt " + attempt + "/" + attempts + "): " + e.getMessage());
        Thread.sleep(3000);
      }
    }
    throw new IllegalStateException("could not initialise DuckDB/Iceberg", last);
  }

  private static void initDuckDb(
      Connection con, String restUri, String dbName, String tableName) throws SQLException {
    try (Statement st = con.createStatement()) {
      st.execute("INSTALL iceberg");
      st.execute("LOAD iceberg");
      st.execute("INSTALL httpfs");
      st.execute("LOAD httpfs");
      // S3 access for MinIO (path-style, plain HTTP). Env values are quoted as
      // escaped SQL literals so they cannot inject into the statement.
      st.execute(
          "CREATE OR REPLACE SECRET minio_s3 (TYPE s3"
              + ", KEY_ID " + sqlLiteral(CatalogConfig.s3AccessKey())
              + ", SECRET " + sqlLiteral(CatalogConfig.s3SecretKey())
              + ", REGION " + sqlLiteral(CatalogConfig.s3Region())
              + ", ENDPOINT " + sqlLiteral(s3Host())
              + ", URL_STYLE 'path', USE_SSL false)");
      // Attach the Iceberg REST-catalog facade (DuckDB speaks only REST).
      st.execute(
          "ATTACH 'warehouse' AS iceberg_catalog (TYPE iceberg"
              + ", ENDPOINT " + sqlLiteral(restUri)
              + ", AUTHORIZATION_TYPE 'none')");
      st.execute("CREATE SCHEMA IF NOT EXISTS iceberg_catalog." + dbName);
      st.execute(
          "CREATE TABLE IF NOT EXISTS iceberg_catalog." + dbName + "." + tableName + " ("
              + "id VARCHAR, event_type VARCHAR, user_id VARCHAR, "
              + "amount DOUBLE, event_time TIMESTAMPTZ)");
    }
  }

  private static KafkaConsumer<String, String> buildConsumer(String bootstrap) {
    Properties p = new Properties();
    p.put("bootstrap.servers", bootstrap);
    p.put("group.id", "duckdb-iceberg");
    p.put("auto.offset.reset", "earliest");
    p.put("enable.auto.commit", "false");
    p.put("key.deserializer", StringDeserializer.class.getName());
    p.put("value.deserializer", StringDeserializer.class.getName());
    // mTLS to the Kafka broker (ssl.client.auth=required).
    p.putAll(KafkaTls.clientProperties("duckdb"));
    return new KafkaConsumer<>(p);
  }

  /**
   * Writes the whole batch as a <b>single multi-row INSERT</b>, which DuckDB's
   * iceberg extension turns into one Iceberg commit. (A per-row
   * {@code executeBatch} would instead trigger one commit per row, flooding the
   * catalog and tripping a fatal metadata-cleanup error.)
   */
  private static void flush(Connection con, String fqTable, List<JsonNode> batch)
      throws SQLException {
    StringBuilder sql = new StringBuilder("INSERT INTO ").append(fqTable).append(" VALUES ");
    for (int i = 0; i < batch.size(); i++) {
      sql.append(i == 0 ? "(?,?,?,?,?)" : ",(?,?,?,?,?)");
    }
    try (PreparedStatement ps = con.prepareStatement(sql.toString())) {
      int p = 1;
      for (JsonNode node : batch) {
        ps.setString(p++, text(node, "id"));
        ps.setString(p++, text(node, "event_type"));
        ps.setString(p++, text(node, "user_id"));
        if (node.hasNonNull("amount")) {
          ps.setDouble(p++, node.get("amount").asDouble());
        } else {
          ps.setNull(p++, Types.DOUBLE);
        }
        String ts = text(node, "event_time");
        if (ts == null) {
          ps.setNull(p++, Types.TIMESTAMP_WITH_TIMEZONE);
        } else {
          ps.setObject(p++, OffsetDateTime.ofInstant(Instant.parse(ts), ZoneOffset.UTC));
        }
      }
      ps.executeUpdate();
    }
  }

  /** Flushes a batch to Iceberg with exponential backoff on transient failures. */
  private static void flushWithRetry(Connection con, String fqTable, List<JsonNode> batch)
      throws SQLException, InterruptedException {
    final int retries = 5;
    for (int attempt = 1; attempt <= retries; attempt++) {
      try {
        flush(con, fqTable, batch);
        return;
      } catch (SQLException e) {
        if (attempt == retries) {
          throw e;
        }
        long waitMs = (long) Math.pow(2, attempt) * 1000L;
        System.out.println(
            "flush failed (attempt " + attempt + "/" + retries + "): " + e.getMessage()
                + "; retrying in " + waitMs + "ms");
        Thread.sleep(waitMs);
      }
    }
  }

  private static String text(JsonNode node, String field) {
    JsonNode v = node.get(field);
    return (v == null || v.isNull()) ? null : v.asText();
  }

  /** DuckDB's S3 ENDPOINT wants {@code host:port} without the URL scheme. */
  private static String s3Host() {
    return CatalogConfig.s3Endpoint().replaceFirst("^https?://", "");
  }

  /** Quotes a value as a SQL string literal, escaping embedded single quotes. */
  private static String sqlLiteral(String value) {
    return "'" + value.replace("'", "''") + "'";
  }

  private static void closeQuietly(AutoCloseable closeable) {
    try {
      closeable.close();
    } catch (Exception ignored) {
      // best-effort close during shutdown
    }
  }
}
