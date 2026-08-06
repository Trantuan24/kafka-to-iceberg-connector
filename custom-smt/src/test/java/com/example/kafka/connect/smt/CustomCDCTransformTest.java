package com.example.kafka.connect.smt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CustomCDCTransformTest {

    private static final String TOPIC = "qtmt-append";
    private static final String TABLE = "def.abc_append_v2";
    private static final Instant FIXED_NOW = Instant.parse("2026-08-05T08:45:23.456789Z");

    private CustomCDCTransform<SinkRecord> transform;

    @BeforeEach
    void setUp() {
        transform = new CustomCDCTransform<>(Clock.fixed(FIXED_NOW, ZoneOffset.UTC));
    }

    @Test
    void buildsNineColumnApiPushRecordAndPreservesRawJson() {
        configureAppend(TOPIC + ":" + TABLE);
        String payload = "[\n  {\"MaTram\": \"TRAM001\"}\n]";

        SinkRecord output = transform.apply(stringRecord(TOPIC, 2, 17L, payload));
        Struct value = (Struct) output.value();

        List<String> fieldNames = value.schema().fields().stream()
            .map(field -> field.name())
            .collect(Collectors.toList());

        assertEquals(
            List.of(
                "loainguon",
                "manguondulieu",
                "sukien",
                "phienban",
                "body",
                "header",
                "data",
                "ingest_date",
                "ingest_time",
                "iceberg_table"),
            fieldNames);
        assertEquals("api_push", value.getString("loainguon"));
        assertEquals("qtmt-append-2-17", value.getString("manguondulieu"));
        assertNull(value.get("sukien"));
        assertEquals(1, value.getInt32("phienban"));
        assertNull(value.get("body"));
        assertNull(value.get("header"));
        assertEquals(payload, value.getString("data"));
        assertEquals("2026-08-05", value.getString("ingest_date"));
        assertEquals("2026-08-05T15:45:23.456", value.getString("ingest_time"));
        assertEquals(TABLE, value.getString("iceberg_table"));
        assertEquals(Schema.Type.INT32, value.schema().field("phienban").schema().type());
    }

    @Test
    void readsBodyAndSanitizesHttpHeadersFromKafkaHeaders() {
        configureAppend(TOPIC + ":" + TABLE);
        SinkRecord input = stringRecord(TOPIC, 0, 3L, "{\"maHoSo\":\"HS001\"}");
        input.headers().addString("api.body", "{\"tuNgay\":\"2026-07-30\"}");
        input.headers().addString(
            "api.headers",
            "{\"Content-Type\":\"application/json\","
                + "\"Authorization\":\"Basic must-not-land\","
                + "\"Cookie\":\"session=must-not-land\","
                + "\"X-Request-ID\":\"req-123\","
                + "\"X-Unapproved\":\"drop-me\"}");

        Struct value = (Struct) transform.apply(input).value();

        assertEquals("{\"tuNgay\":\"2026-07-30\"}", value.getString("body"));
        assertEquals(
            "{\"content-type\":\"application/json\",\"x-request-id\":\"req-123\"}",
            value.getString("header"));
        assertFalse(value.getString("header").toLowerCase().contains("authorization"));
        assertFalse(value.getString("header").toLowerCase().contains("cookie"));
        assertFalse(value.getString("header").contains("must-not-land"));
    }

    @Test
    void supportsUtf8ByteArrayMetadataHeaders() {
        configureAppend(TOPIC + ":" + TABLE);
        SinkRecord input = stringRecord(TOPIC, 0, 4L, "payload");
        input.headers().addBytes(
            "api.body", "{\"param\":\"gia-tri\"}".getBytes(StandardCharsets.UTF_8));
        input.headers().addBytes(
            "api.headers", "{\"Accept\":\"application/json\"}".getBytes(StandardCharsets.UTF_8));

        Struct value = (Struct) transform.apply(input).value();

        assertEquals("{\"param\":\"gia-tri\"}", value.getString("body"));
        assertEquals("{\"accept\":\"application/json\"}", value.getString("header"));
    }

    @Test
    void supportsStructuredHeadersProducedByKafkaSimpleHeaderConverter() {
        configureAppend(TOPIC + ":" + TABLE);
        SinkRecord input = stringRecord(TOPIC, 0, 5L, "payload");
        Map<String, String> body = new LinkedHashMap<>();
        body.put("tuNgay", "2026-07-30");
        body.put("denNgay", "2026-08-07");
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("Content-Type", "application/json");
        headers.put("Authorization", "Basic must-not-land");
        headers.put("X-Request-ID", "req-map-123");
        Schema mapSchema = SchemaBuilder.map(
            Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).optional().build();
        input.headers().add("api.body", body, mapSchema);
        input.headers().add("api.headers", headers, mapSchema);

        Struct value = (Struct) transform.apply(input).value();

        assertEquals(
            "{\"tuNgay\":\"2026-07-30\",\"denNgay\":\"2026-08-07\"}",
            value.getString("body"));
        assertEquals(
            "{\"content-type\":\"application/json\",\"x-request-id\":\"req-map-123\"}",
            value.getString("header"));
        assertFalse(value.getString("header").contains("must-not-land"));
    }

    @Test
    void treatsMissingEmptyOrInvalidApiMetadataAsNullWithoutDroppingData() {
        configureAppend(TOPIC + ":" + TABLE);
        SinkRecord input = stringRecord(TOPIC, 0, 5L, "");
        input.headers().addString("api.body", "   ");
        input.headers().addString("api.headers", "not-json");

        Struct value = (Struct) transform.apply(input).value();

        assertNull(value.get("body"));
        assertNull(value.get("header"));
        assertEquals("", value.getString("data"));
    }

    @Test
    void preservesRawXmlVerbatim() {
        configureAppend(TOPIC + ":" + TABLE);
        String payload = "<root>\n  <MaTram>TRAM001</MaTram>\n</root>";

        Struct value = (Struct) transform.apply(stringRecord(TOPIC, 0, 3L, payload)).value();

        assertEquals(payload, value.getString("data"));
        assertEquals("qtmt-append-0-3", value.getString("manguondulieu"));
    }

    @Test
    void supportsExplicitSourceVersionTimezoneAndHeaderNames() {
        Map<String, Object> config = appendConfig(TOPIC + ":" + TABLE);
        config.put("append.source.type", "custom_api_push");
        config.put("append.schema.version", 2);
        config.put("append.timezone", "UTC");
        config.put("append.body.header", "request.params");
        config.put("append.headers.header", "request.headers");
        transform.configure(config);
        SinkRecord input = stringRecord(TOPIC, 0, 6L, "data");
        input.headers().addString("request.params", "{}");
        input.headers().addString("request.headers", "{\"Traceparent\":\"00-abc\"}");

        Struct value = (Struct) transform.apply(input).value();

        assertEquals("custom_api_push", value.getString("loainguon"));
        assertEquals(2, value.getInt32("phienban"));
        assertEquals("{}", value.getString("body"));
        assertEquals("{\"traceparent\":\"00-abc\"}", value.getString("header"));
        assertEquals("2026-08-05T08:45:23.456", value.getString("ingest_time"));
    }

    @Test
    void rejectsInvalidAppendConfiguration() {
        Map<String, Object> invalidMode = appendConfig(TOPIC + ":" + TABLE);
        invalidMode.put("mode", "apend");
        assertThrows(ConfigException.class, () -> transform.configure(invalidMode));

        Map<String, Object> invalidTimezone = appendConfig(TOPIC + ":" + TABLE);
        invalidTimezone.put("append.timezone", "Mars/Olympus");
        assertThrows(ConfigException.class, () -> transform.configure(invalidTimezone));

        Map<String, Object> invalidVersion = appendConfig(TOPIC + ":" + TABLE);
        invalidVersion.put("append.schema.version", 0);
        assertThrows(ConfigException.class, () -> transform.configure(invalidVersion));

        Map<String, Object> blankSource = appendConfig(TOPIC + ":" + TABLE);
        blankSource.put("append.source.type", "  ");
        assertThrows(ConfigException.class, () -> transform.configure(blankSource));
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
    void requiresValidExplicitMappingInAppendMode() {
        Map<String, Object> config = new HashMap<>();
        config.put("mode", "append");
        assertThrows(ConfigException.class, () -> transform.configure(config));

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
        assertTrue(transform.config().names().containsAll(List.of(
            "mode",
            "iceberg.namespace",
            "topic.table.map",
            "append.source.type",
            "append.schema.version",
            "append.timezone",
            "append.body.header",
            "append.headers.header")));
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