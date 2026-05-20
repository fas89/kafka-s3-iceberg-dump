package com.example.iceberg.common;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Connection settings for the shared Iceberg <b>JDBC catalog</b> (on Postgres)
 * over MinIO/S3 storage.
 *
 * <p>Every JVM engine (Flink, Spark, Connect) reads/writes the same Postgres
 * catalog. All values are environment-driven with container-friendly defaults,
 * so the same jar runs locally and in the Docker Compose stack.
 */
public final class CatalogConfig {

  private CatalogConfig() {}

  /**
   * Catalog name for the JDBC-catalog engines (default {@code demo}). DuckDB
   * reaches the catalog through the iceberg-rest facade, whose backend catalog
   * is named {@code rest_backend}; DuckDB's maintenance job and the metrics
   * tool set {@code ICEBERG_CATALOG_NAME} so they resolve DuckDB's tables.
   */
  public static final String CATALOG_NAME = Env.get("ICEBERG_CATALOG_NAME", "demo");

  public static String jdbcUri() {
    return Env.get("ICEBERG_JDBC_URI", "jdbc:postgresql://postgres:5432/iceberg");
  }

  public static String jdbcUser() {
    return Env.get("ICEBERG_JDBC_USER", "iceberg");
  }

  public static String jdbcPassword() {
    return Env.get("ICEBERG_JDBC_PASSWORD", "iceberg");
  }

  public static String warehouse() {
    return Env.get("ICEBERG_WAREHOUSE", "s3://warehouse");
  }

  public static String s3Endpoint() {
    return Env.get("S3_ENDPOINT", "http://minio:9000");
  }

  public static String s3AccessKey() {
    return Env.get("S3_ACCESS_KEY", "admin");
  }

  public static String s3SecretKey() {
    return Env.get("S3_SECRET_KEY", "password");
  }

  public static String s3Region() {
    return Env.get("S3_REGION", "us-east-1");
  }

  /**
   * Properties for an Iceberg JDBC catalog (`org.apache.iceberg.jdbc.JdbcCatalog`)
   * backed by Postgres, with `S3FileIO` pointed at MinIO (path-style).
   */
  public static Map<String, String> jdbcCatalogProperties() {
    Map<String, String> p = new LinkedHashMap<>();
    p.put("catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog");
    p.put("uri", jdbcUri());
    p.put("jdbc.user", jdbcUser());
    p.put("jdbc.password", jdbcPassword());
    p.put("warehouse", warehouse());
    p.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
    p.put("s3.endpoint", s3Endpoint());
    p.put("s3.path-style-access", "true");
    p.put("s3.access-key-id", s3AccessKey());
    p.put("s3.secret-access-key", s3SecretKey());
    p.put("client.region", s3Region());
    return p;
  }
}
