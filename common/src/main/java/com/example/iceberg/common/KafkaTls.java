package com.example.iceberg.common;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Mutual-TLS client properties for connecting to the Kafka broker.
 *
 * <p>The broker runs an {@code SSL} listener with {@code ssl.client.auth=required},
 * so every engine must present its own CA-signed PKCS12 keystore. Keystores live
 * in {@code CERT_DIR} (default {@code /certs}); the store password is
 * {@code CERT_STOREPASS} (default {@code changeit}).
 */
public final class KafkaTls {

  private KafkaTls() {}

  /**
   * Standard Kafka SSL client properties for the given principal
   * (e.g. {@code "flink"} → uses {@code /certs/flink.keystore.p12}).
   */
  public static Map<String, String> clientProperties(String principal) {
    String dir = Env.get("CERT_DIR", "/certs");
    String pass = Env.get("CERT_STOREPASS", "changeit");

    Map<String, String> p = new LinkedHashMap<>();
    p.put("security.protocol", "SSL");
    p.put("ssl.keystore.type", "PKCS12");
    p.put("ssl.keystore.location", dir + "/" + principal + ".keystore.p12");
    p.put("ssl.keystore.password", pass);
    p.put("ssl.key.password", pass);
    p.put("ssl.truststore.type", "PKCS12");
    p.put("ssl.truststore.location", dir + "/truststore.p12");
    p.put("ssl.truststore.password", pass);
    // Local certs are not issued for real hostnames — skip hostname verification.
    p.put("ssl.endpoint.identification.algorithm", "");
    return p;
  }
}
