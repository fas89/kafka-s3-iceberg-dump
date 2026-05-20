package com.example.iceberg.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;

import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.Test;

class JsonToRowDataTest {

  private final JsonToRowData mapper = new JsonToRowData();

  @Test
  void mapsEveryFieldOfACompleteEvent() throws Exception {
    RowData row =
        mapper.map(
            "{\"id\":\"evt-1\",\"event_type\":\"click\",\"user_id\":\"user-2\","
                + "\"amount\":42.5,\"event_time\":\"2026-05-18T10:00:00Z\"}");

    assertEquals("evt-1", row.getString(0).toString());
    assertEquals("click", row.getString(1).toString());
    assertEquals("user-2", row.getString(2).toString());
    assertEquals(42.5, row.getDouble(3));
    assertEquals(
        Instant.parse("2026-05-18T10:00:00Z"), row.getTimestamp(4, 6).toInstant());
  }

  @Test
  void missingOptionalFieldsBecomeSqlNull() throws Exception {
    RowData row = mapper.map("{\"id\":\"evt-9\"}");

    assertEquals("evt-9", row.getString(0).toString());
    assertTrue(row.isNullAt(1), "event_type should be SQL null");
    assertTrue(row.isNullAt(2), "user_id should be SQL null");
    assertTrue(row.isNullAt(3), "amount should be SQL null");
    assertTrue(row.isNullAt(4), "event_time should be SQL null");
  }
}
