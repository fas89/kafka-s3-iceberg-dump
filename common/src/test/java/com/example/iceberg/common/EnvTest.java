package com.example.iceberg.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.junit.jupiter.api.Test;

class EnvTest {

  private static final String UNSET = "KTI_DEFINITELY_UNSET_VARIABLE_XYZ";

  @Test
  void returnsDefaultWhenVariableUnset() {
    assertEquals("fallback", Env.get(UNSET, "fallback"));
  }

  @Test
  void returnsIntDefaultWhenVariableUnset() {
    assertEquals(42, Env.getInt(UNSET, 42));
  }

  @Test
  void returnsValueWhenVariableSet() {
    // PATH is set in every environment these tests run in.
    assertNotEquals("unused-default", Env.get("PATH", "unused-default"));
  }
}
