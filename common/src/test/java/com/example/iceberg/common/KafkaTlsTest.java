package com.example.iceberg.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.junit.jupiter.api.Test;

class KafkaTlsTest {

  @Test
  void clientPropertiesEnableMutualTls() {
    Map<String, String> props = KafkaTls.clientProperties("spark");

    assertEquals("SSL", props.get("security.protocol"));
    assertEquals("PKCS12", props.get("ssl.keystore.type"));
    assertEquals("PKCS12", props.get("ssl.truststore.type"));
    // Local certs are not issued for real hostnames.
    assertEquals("", props.get("ssl.endpoint.identification.algorithm"));
  }

  @Test
  void clientPropertiesUseThePrincipalsOwnKeystore() {
    Map<String, String> props = KafkaTls.clientProperties("spark");

    assertTrue(props.get("ssl.keystore.location").endsWith("/spark.keystore.p12"));
    assertTrue(props.get("ssl.truststore.location").endsWith("/truststore.p12"));
  }

  @Test
  void eachPrincipalGetsADistinctKeystore() {
    assertNotEquals(
        KafkaTls.clientProperties("flink").get("ssl.keystore.location"),
        KafkaTls.clientProperties("duckdb").get("ssl.keystore.location"));
  }
}
