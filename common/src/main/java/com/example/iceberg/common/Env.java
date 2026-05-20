package com.example.iceberg.common;

/** Environment-variable lookup with defaults — every engine reads its config
 *  this way so the same jar runs locally and in the Docker Compose stack. */
public final class Env {

  private Env() {}

  /** Value of {@code key}, or {@code defaultValue} if unset or blank. */
  public static String get(String key, String defaultValue) {
    String v = System.getenv(key);
    return (v == null || v.isBlank()) ? defaultValue : v;
  }

  /** Integer-valued environment variable, or {@code defaultValue}. */
  public static int getInt(String key, int defaultValue) {
    String v = System.getenv(key);
    return (v == null || v.isBlank()) ? defaultValue : Integer.parseInt(v.trim());
  }
}
