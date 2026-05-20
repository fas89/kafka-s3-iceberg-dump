package com.example.iceberg.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.junit.jupiter.api.Test;

class CatalogConfigTest {

  @Test
  void jdbcCatalogPropertiesExposeTheJdbcCatalogOnS3() {
    Map<String, String> props = CatalogConfig.jdbcCatalogProperties();

    assertEquals("org.apache.iceberg.jdbc.JdbcCatalog", props.get("catalog-impl"));
    assertEquals("org.apache.iceberg.aws.s3.S3FileIO", props.get("io-impl"));
    assertEquals("s3://warehouse", props.get("warehouse"));
    // MinIO needs path-style S3 addressing.
    assertEquals("true", props.get("s3.path-style-access"));
  }

  @Test
  void jdbcCatalogPropertiesCarryConnectionCredentials() {
    Map<String, String> props = CatalogConfig.jdbcCatalogProperties();

    assertTrue(props.get("uri").startsWith("jdbc:postgresql://"));
    assertNotNull(props.get("jdbc.user"));
    assertNotNull(props.get("jdbc.password"));
  }

  @Test
  void catalogNameIsStable() {
    assertEquals("demo", CatalogConfig.CATALOG_NAME);
  }
}
