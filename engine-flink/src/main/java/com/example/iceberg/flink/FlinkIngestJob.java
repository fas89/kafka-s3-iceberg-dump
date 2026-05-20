package com.example.iceberg.flink;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;

import com.example.iceberg.common.CatalogConfig;
import com.example.iceberg.common.Env;
import com.example.iceberg.common.KafkaTls;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.apache.hadoop.conf.Configuration;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.IcebergSink;
import org.apache.iceberg.types.Types;

/**
 * Flink ingest engine: reads JSON events from Kafka and appends them to the
 * Iceberg table {@code flink_db.events}. <strong>Ingest-only</strong> &mdash;
 * Iceberg table maintenance (data-file compaction, snapshot expiration,
 * orphan-file GC) is deployed separately, as a sibling Docker Compose
 * service running {@code flink-maintenance.jar} against the same table.
 *
 * <p>The split (upstream PR #1 pattern, applied here without the K8s
 * Application-Mode migration) gives ingest and maintenance independent
 * lifecycles, failure domains and tuning, and lets a stalled maintenance
 * pass not back-pressure the ingest stream.
 *
 * <p>All knobs are environment-driven; see {@link CatalogConfig}.
 */
public final class FlinkIngestJob {

  // Iceberg table schema. Field order here is the order produced by
  // JsonToRowData and expected by the Iceberg sink.
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.StringType.get()),
          Types.NestedField.optional(2, "event_type", Types.StringType.get()),
          Types.NestedField.optional(3, "user_id", Types.StringType.get()),
          Types.NestedField.optional(4, "amount", Types.DoubleType.get()),
          Types.NestedField.optional(5, "event_time", Types.TimestampType.withZone()));

  // Identity-partition by event_type so the demo produces many small data
  // files that RewriteDataFiles can later compact.
  private static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).identity("event_type").build();

  private FlinkIngestJob() {}

  public static void main(String[] args) throws Exception {
    final String kafkaBootstrap = Env.get("KAFKA_BOOTSTRAP", "kafka:9092");
    final String kafkaTopic = Env.get("KAFKA_TOPIC", "events");
    final String dbName = Env.get("ICEBERG_DB", "flink_db");
    final String tableName = Env.get("ICEBERG_TABLE", "events");

    // ---- Iceberg JDBC catalog over MinIO (S3FileIO) -------------------------
    Map<String, String> catalogProps = CatalogConfig.jdbcCatalogProperties();
    Configuration hadoopConf = new Configuration(false);
    CatalogLoader catalogLoader =
        CatalogLoader.custom(
            CatalogConfig.CATALOG_NAME, catalogProps, hadoopConf,
            "org.apache.iceberg.jdbc.JdbcCatalog");

    TableIdentifier tableId = TableIdentifier.of(dbName, tableName);

    // Create namespace + table up-front (runs in the Flink client JVM).
    Catalog catalog = catalogLoader.loadCatalog();
    try {
      Namespace ns = Namespace.of(dbName);
      if (catalog instanceof SupportsNamespaces nsCatalog
          && !nsCatalog.namespaceExists(ns)) {
        nsCatalog.createNamespace(ns);
      }
      if (!catalog.tableExists(tableId)) {
        catalog.createTable(tableId, SCHEMA, SPEC, Map.of("format-version", "2"));
      }
    } catch (Exception e) {
      throw new IllegalStateException("Failed to ensure Iceberg table exists", e);
    } finally {
      if (catalog instanceof Closeable closeable) {
        try {
          closeable.close();
        } catch (IOException ignored) {
          // best-effort close of the client-side catalog
        }
      }
    }

    TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, tableId);

    // ---- Flink job ----------------------------------------------------------
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // Iceberg commits on checkpoint; checkpointing must be enabled.
    env.enableCheckpointing(Duration.ofSeconds(30).toMillis());

    // mTLS to the Kafka broker (ssl.client.auth=required).
    Properties kafkaProps = new Properties();
    kafkaProps.putAll(KafkaTls.clientProperties("flink"));

    KafkaSource<String> source =
        KafkaSource.<String>builder()
            .setBootstrapServers(kafkaBootstrap)
            .setTopics(kafkaTopic)
            .setGroupId("flink-kafka-to-iceberg")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .setProperties(kafkaProps)
            .build();

    RowType rowType = FlinkSchemaUtil.convert(SCHEMA);

    DataStream<RowData> rows =
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source")
            .map(new JsonToRowData())
            .name("json-to-rowdata")
            .returns(InternalTypeInfo.of(rowType));

    IcebergSink.forRowData(rows)
        .tableLoader(tableLoader)
        .writeParallelism(1)
        .append();

    // Maintenance (RewriteDataFiles + ExpireSnapshots + DeleteOrphanFiles)
    // lives in a sibling container running flink-maintenance.jar against
    // the same flink_db.events table — see docker-compose.flink.yml. This
    // ingest job is intentionally maintenance-free so the two halves can
    // restart independently.
    env.execute("flink-kafka-to-iceberg");
  }
}
