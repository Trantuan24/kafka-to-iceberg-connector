package com.example.kafka.connect.smt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CustomCDCTransformTest {

    private static final String TOPIC = "qtmt-append";
    private static final String TABLE = "def.abc_append";

    private CustomCDCTransform<SinkRecord> transform;

    @BeforeEach
    void setUp() {
        transform = new CustomCDCTransform<>();
    }

    @Test
    void preservesRawJsonAndBuildsExpectedAppendRecord() {
        configureAppend(TOPIC + ":" + TABLE);
        String payload = "[\n  {\"MaTram\": \"TRAM001\"}\n]";

        SinkRecord output = transform.apply(stringRecord(TOPIC, 2, 17L, payload));
        Struct value = (Struct) output.value();

        List<String> fieldNames = value.schema().fields().stream()
            .map(field -> field.name())
            .collect(Collectors.toList());

        assertEquals(
            List.of("id", "record", "ngay_cap_nhat", "iceberg_table"),
            fieldNames);
        assertEquals("qtmt-append-2-17", value.getString("id"));
        assertEquals(payload, value.getString("record"));
        assertEquals(TABLE, value.getString("iceberg_table"));
        assertNotNull(Instant.parse(value.getString("ngay_cap_nhat")));
    }

    @Test
    void preservesRawXmlVerbatim() {
        configureAppend(TOPIC + ":" + TABLE);
        String payload = "<root>\n  <MaTram>TRAM001</MaTram>\n</root>";

        Struct value = (Struct) transform.apply(stringRecord(TOPIC, 0, 3L, payload)).value();

        assertEquals(payload, value.getString("record"));
        assertEquals("qtmt-append-0-3", value.getString("id"));
    }

    @Test
    void rejectsNonStringValueInAppendMode() {
        configureAppend(TOPIC + ":" + TABLE);
        Map<String, Object> payload = new HashMap<>();
        payload.put("MaTram", "TRAM001");
        SinkRecord input = new SinkRecord(TOPIC, 0, null, null, null, payload, 4L);

        DataException error = assertThrows(DataException.class, () -> transform.apply(input));

        assertTrue(error.getMessage().contains("StringConverter"));
    }

    @Test
    void rejectsInvalidMode() {
        Map<String, Object> config = appendConfig(TOPIC + ":" + TABLE);
        config.put("mode", "apend");

        assertThrows(ConfigException.class, () -> transform.configure(config));
    }

    @Test
    void requiresMappingInAppendMode() {
        Map<String, Object> config = new HashMap<>();
        config.put("mode", "append");

        assertThrows(ConfigException.class, () -> transform.configure(config));
    }

    @Test
    void rejectsMalformedAndDuplicateMappings() {
        assertThrows(
            ConfigException.class,
            () -> transform.configure(appendConfig("qtmt-append:def")));
        assertThrows(
            ConfigException.class,
            () -> transform.configure(
                appendConfig("qtmt-append:def.one,qtmt-append:def.two")));
    }

    @Test
    void rejectsRecordFromUnmappedAppendTopic() {
        configureAppend(TOPIC + ":" + TABLE);

        DataException error = assertThrows(
            DataException.class,
            () -> transform.apply(stringRecord("other-topic", 0, 1L, "{}")));

        assertTrue(error.getMessage().contains("No topic.table.map entry"));
    }

    @Test
    void passesTombstoneThroughForOffsetTracking() {
        configureAppend(TOPIC + ":" + TABLE);
        SinkRecord tombstone = new SinkRecord(TOPIC, 0, null, null, null, null, 8L);

        assertSame(tombstone, transform.apply(tombstone));
    }

    @Test
    void exposesSupportedConfiguration() {
        assertTrue(transform.config().names().contains("mode"));
        assertTrue(transform.config().names().contains("iceberg.namespace"));
        assertTrue(transform.config().names().contains("topic.table.map"));
    }

    private void configureAppend(String mapping) {
        transform.configure(appendConfig(mapping));
    }

    private Map<String, Object> appendConfig(String mapping) {
        Map<String, Object> config = new HashMap<>();
        config.put("mode", "append");
        config.put("topic.table.map", mapping);
        return config;
    }

    private SinkRecord stringRecord(
            String topic, int partition, long offset, String value) {
        return new SinkRecord(
            topic,
            partition,
            null,
            null,
            Schema.OPTIONAL_STRING_SCHEMA,
            value,
            offset);
    }
}