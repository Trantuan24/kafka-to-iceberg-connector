package com.example.kafka.connect.smt;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.DateTimeException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Custom SMT transforming Kafka messages into Iceberg-compatible records.
 * Behaviour is selected by the "mode" config (transforms.&lt;name&gt;.mode):
 *
 * ====================================================================
 * MODE = "cdc" (default)
 * ====================================================================
 * Input (CDC envelope):
 * { "data": [...], "key": "MaTram", "type": "INSERT", "version": 1,
 *   "ngay_cap_nhat": "...", "length": 2 }
 *
 * Output struct (10 fields). After the fork connector strips the
 * route-field (iceberg_table) and cdc-field (_cdc_op), 8 columns land
 * in Iceberg: id, dedup_key, record, version, type, key, ngay_cap_nhat, length.
 * Includes an in-memory version filter to drop stale/out-of-order records.
 * Requires value.converter = JsonConverter (schemas.enable=false).
 *
 * ====================================================================
 * MODE = "append"
 * ====================================================================
 * Raw passthrough: the ENTIRE Kafka value (JSON, XML or an empty string)
 * is stored verbatim in `data`. Optional API metadata is read from Kafka
 * headers `api.body` and `api.headers`.
 *
 * Output data columns: loainguon, manguondulieu, sukien, phienban, body,
 * header, data, ingest_date, ingest_time. The additional iceberg_table field
 * is stripped by the sink connector before writing.
 * Use value.converter = StringConverter so any text format passes through.
 * * Append mode does not perform business-key deduplication. Distinct Kafka
 * records, including records with identical payloads, become distinct rows.
 */
public class CustomCDCTransform<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger log = LoggerFactory.getLogger(CustomCDCTransform.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    // Config keys
    private static final String ICEBERG_NAMESPACE_CONFIG = "iceberg.namespace";
    private static final String ICEBERG_NAMESPACE_DEFAULT = "default";
    private static final String TOPIC_TABLE_MAP_CONFIG = "topic.table.map";
    private static final String MODE_CONFIG = "mode";
    private static final String MODE_CDC = "cdc";
    private static final String MODE_APPEND = "append";
    private static final String APPEND_SOURCE_TYPE_CONFIG = "append.source.type";
    private static final String APPEND_SOURCE_TYPE_DEFAULT = "api_push";
    private static final String APPEND_SCHEMA_VERSION_CONFIG = "append.schema.version";
    private static final int APPEND_SCHEMA_VERSION_DEFAULT = 1;
    private static final String APPEND_TIMEZONE_CONFIG = "append.timezone";
    private static final String APPEND_TIMEZONE_DEFAULT = "Asia/Ho_Chi_Minh";
    private static final String APPEND_BODY_HEADER_CONFIG = "append.body.header";
    private static final String APPEND_BODY_HEADER_DEFAULT = "api.body";
    private static final String APPEND_HEADERS_HEADER_CONFIG = "append.headers.header";
    private static final String APPEND_HEADERS_HEADER_DEFAULT = "api.headers";
    private static final Set<String> SAFE_HTTP_HEADERS = Set.of(
        "content-type",
        "accept",
        "user-agent",
        "x-request-id",
        "x-correlation-id",
        "traceparent");
    private static final DateTimeFormatter INGEST_TIME_FORMATTER =
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(
            MODE_CONFIG,
            ConfigDef.Type.STRING,
            MODE_CDC,
            ConfigDef.ValidString.in(MODE_CDC, MODE_APPEND),
            ConfigDef.Importance.HIGH,
            "Processing mode: cdc or append")
        .define(
            ICEBERG_NAMESPACE_CONFIG,
            ConfigDef.Type.STRING,
            ICEBERG_NAMESPACE_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            "Fallback Iceberg namespace used by CDC routing")
        .define(
            TOPIC_TABLE_MAP_CONFIG,
            ConfigDef.Type.STRING,
            "",
            ConfigDef.Importance.HIGH,
            "Comma-separated topic:namespace.table mappings; required in append mode")
        .define(
            APPEND_SOURCE_TYPE_CONFIG,
            ConfigDef.Type.STRING,
            APPEND_SOURCE_TYPE_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            "Value written to loainguon in append mode")
        .define(
            APPEND_SCHEMA_VERSION_CONFIG,
            ConfigDef.Type.INT,
            APPEND_SCHEMA_VERSION_DEFAULT,
            ConfigDef.Range.atLeast(1),
            ConfigDef.Importance.MEDIUM,
            "Landing contract version written to phienban in append mode")
        .define(
            APPEND_TIMEZONE_CONFIG,
            ConfigDef.Type.STRING,
            APPEND_TIMEZONE_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            "Time zone used to generate append ingest_date and ingest_time")
        .define(
            APPEND_BODY_HEADER_CONFIG,
            ConfigDef.Type.STRING,
            APPEND_BODY_HEADER_DEFAULT,
            ConfigDef.Importance.LOW,
            "Kafka header containing API parameters/body metadata")
        .define(
            APPEND_HEADERS_HEADER_CONFIG,
            ConfigDef.Type.STRING,
            APPEND_HEADERS_HEADER_DEFAULT,
            ConfigDef.Importance.LOW,
            "Kafka header containing an HTTP headers JSON object");

    // Schema for transformed record (CDC mode, 10 fields)
    private Schema transformedSchema;

    // Schema for append mode (9 data fields + routing field)
    private Schema appendSchema;

    // Processing mode: "cdc" (default) or "append"
    private String mode = MODE_CDC;

    private String appendSourceType = APPEND_SOURCE_TYPE_DEFAULT;
    private int appendSchemaVersion = APPEND_SCHEMA_VERSION_DEFAULT;
    private ZoneId appendZone = ZoneId.of(APPEND_TIMEZONE_DEFAULT);
    private String appendBodyHeader = APPEND_BODY_HEADER_DEFAULT;
    private String appendHeadersHeader = APPEND_HEADERS_HEADER_DEFAULT;
    private final Clock clockOverride;

    // Configurable namespace for table routing (fallback)
    private String icebergNamespace = ICEBERG_NAMESPACE_DEFAULT;

    // Custom topic → table mapping (topic → "namespace.table")
    private final Map<String, String> topicTableMap = new HashMap<>();

    /**
     * In-memory version cache: dedup_key -> max version seen.
     * Prevents out-of-order / stale records from overwriting newer data in Iceberg.
     */
    private final ConcurrentHashMap<String, Long> versionCache = new ConcurrentHashMap<>();

    public CustomCDCTransform() {
        this(null);
    }

    CustomCDCTransform(Clock clockOverride) {
        this.clockOverride = clockOverride;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        topicTableMap.clear();
        versionCache.clear();
        icebergNamespace = ICEBERG_NAMESPACE_DEFAULT;
        appendSourceType = readNonBlankConfig(
            configs, APPEND_SOURCE_TYPE_CONFIG, APPEND_SOURCE_TYPE_DEFAULT);
        appendSchemaVersion = readPositiveIntConfig(
            configs, APPEND_SCHEMA_VERSION_CONFIG, APPEND_SCHEMA_VERSION_DEFAULT);
        appendBodyHeader = readNonBlankConfig(
            configs, APPEND_BODY_HEADER_CONFIG, APPEND_BODY_HEADER_DEFAULT);
        appendHeadersHeader = readNonBlankConfig(
            configs, APPEND_HEADERS_HEADER_CONFIG, APPEND_HEADERS_HEADER_DEFAULT);
        String timezone = readNonBlankConfig(
            configs, APPEND_TIMEZONE_CONFIG, APPEND_TIMEZONE_DEFAULT);
        try {
            appendZone = ZoneId.of(timezone);
        } catch (DateTimeException e) {
            throw new ConfigException(
                APPEND_TIMEZONE_CONFIG, timezone, "Invalid time zone: " + e.getMessage());
        }

        // Read processing mode (default "cdc"). Reject typos instead of silently using CDC.
        Object modeObj = configs.get(MODE_CONFIG);
        String configuredMode = modeObj == null ? MODE_CDC : modeObj.toString().trim();
        if (!MODE_CDC.equals(configuredMode) && !MODE_APPEND.equals(configuredMode)) {
            throw new ConfigException(
                MODE_CONFIG, configuredMode, "Expected one of: " + MODE_CDC + ", " + MODE_APPEND);
        }
        mode = configuredMode;

        // Read configurable namespace (default "default")
        Object nsObj = configs.get(ICEBERG_NAMESPACE_CONFIG);
        if (nsObj != null) {
            String configuredNamespace = nsObj.toString().trim();
            if (configuredNamespace.isEmpty()) {
                throw new ConfigException(
                    ICEBERG_NAMESPACE_CONFIG, nsObj, "Namespace must not be blank");
            }
            icebergNamespace = configuredNamespace;
        }

        // Read topic→table mapping: "topic1:ns.table1,topic2:ns.table2"
        Object mapObj = configs.get(TOPIC_TABLE_MAP_CONFIG);
        parseTopicTableMap(mapObj);
        if (MODE_APPEND.equals(mode) && topicTableMap.isEmpty()) {
            throw new ConfigException(
                TOPIC_TABLE_MAP_CONFIG, mapObj,
                "At least one topic-to-table mapping is required in append mode");
        }
        if (!topicTableMap.isEmpty()) {
            log.info("Topic-table mapping loaded: {}", topicTableMap);
        }

        transformedSchema = SchemaBuilder.struct()
            .name("com.example.cdc.TransformedRecord")
            .field("id", Schema.OPTIONAL_STRING_SCHEMA)
            .field("dedup_key", Schema.OPTIONAL_STRING_SCHEMA)
            .field("record", Schema.OPTIONAL_STRING_SCHEMA)
            .field("version", Schema.OPTIONAL_INT64_SCHEMA)
            .field("type", Schema.OPTIONAL_STRING_SCHEMA)
            .field("key", Schema.OPTIONAL_STRING_SCHEMA)
            .field("ngay_cap_nhat", Schema.OPTIONAL_STRING_SCHEMA)
            .field("length", Schema.OPTIONAL_STRING_SCHEMA)
            .field("iceberg_table", Schema.OPTIONAL_STRING_SCHEMA)
            .field("_cdc_op", Schema.OPTIONAL_STRING_SCHEMA)
            .build();

        // Append-mode schema: 9 data columns end up in Iceberg
        // (iceberg_table is the route-field and is stripped by the connector before write).
        appendSchema = SchemaBuilder.struct()
            .name("com.example.cdc.AppendRecord")
            .field("loainguon", Schema.OPTIONAL_STRING_SCHEMA)
            .field("manguondulieu", Schema.OPTIONAL_STRING_SCHEMA)
            .field("sukien", Schema.OPTIONAL_STRING_SCHEMA)
            .field("phienban", Schema.OPTIONAL_INT32_SCHEMA)
            .field("body", Schema.OPTIONAL_STRING_SCHEMA)
            .field("header", Schema.OPTIONAL_STRING_SCHEMA)
            .field("data", Schema.OPTIONAL_STRING_SCHEMA)
            .field("ingest_date", Schema.OPTIONAL_STRING_SCHEMA)
            .field("ingest_time", Schema.OPTIONAL_STRING_SCHEMA)
            .field("iceberg_table", Schema.OPTIONAL_STRING_SCHEMA)
            .build();

        log.info("CustomCDCTransform configured. mode={}, namespace={}", mode, icebergNamespace);
    }

    @Override
    public R apply(R record) {
        // Log input for debugging
        log.debug("CustomCDCTransform input: topic={}, partition={}, offset={}, valueClass={}",
            record.topic(),
            record.kafkaPartition(),
            getOffset(record),
            record.value() == null ? "null" : record.value().getClass().getName());

        // Tombstone records: pass through as-is (do NOT return null)
        if (record.value() == null) {
            log.debug("Skipping tombstone record (topic={}, partition={}, offset={})",
                record.topic(), record.kafkaPartition(), getOffset(record));
            return record;
        }

        // APPEND MODE: preserve the raw value and enrich it with safe API metadata.
        if (MODE_APPEND.equals(mode)) {
            return applyAppend(record);
        }

        try {
            // Extract value as Map (JsonConverter schemas.enable=false → Map<String, Object>)
            Map<String, Object> value;
            if (record.value() instanceof Map) {
                value = (Map<String, Object>) record.value();
                log.debug("Value is Map with keys: {}", value.keySet());
            } else if (record.value() instanceof Struct) {
                value = structToMap((Struct) record.value());
                log.debug("Value is Struct, converted to Map with keys: {}", value.keySet());
            } else {
                String errorMsg = String.format(
                    "Unsupported value type: %s (topic=%s, partition=%s, offset=%s, value=%s)",
                    record.value().getClass().getName(),
                    record.topic(), record.kafkaPartition(), getOffset(record), record.value());
                log.error(errorMsg);
                throw new DataException(errorMsg);
            }

            // Generate deterministic id: topic-partition-offset
            String id = generateId(record);

            // -------------------------------------------------------
            // OUT-OF-ORDER VERSION FILTER
            // Drop stale records based on in-memory version cache.
            // -------------------------------------------------------
            String incomingType    = getStringField(value, "type");
            Object incomingVerObj  = value.get("version");
            String incomingKey     = extractDedupKey(value);  // may be null on bad data

            if (incomingKey != null && incomingVerObj != null) {
                long incomingVersion  = castToLong(incomingVerObj);
                Long cachedVersion    = versionCache.get(incomingKey);

                if (cachedVersion != null && incomingVersion <= cachedVersion) {
                    // Stale record — DROP it
                    log.warn("[VERSION-FILTER] DROPPED stale record: " +
                             "key={}, incoming_version={}, cached_version={}, type={}, offset={}",
                             incomingKey, incomingVersion, cachedVersion, incomingType, getOffset(record));
                    return null;  // Kafka Connect will skip this record
                }

                // New or equal version — update cache
                versionCache.put(incomingKey, incomingVersion);
                log.debug("[VERSION-FILTER] Accepted: key={}, version={}, type={}",
                          incomingKey, incomingVersion, incomingType);

                // After a DELETE, reset cache for this key so a future INSERT with a
                // lower version number (e.g. v1 after the row is deleted) is allowed.
                if ("DELETE".equalsIgnoreCase(incomingType)) {
                    versionCache.remove(incomingKey);
                    log.info("[VERSION-FILTER] Cache cleared for key={} after DELETE", incomingKey);
                }
            }
            // -------------------------------------------------------

            // Transform to Iceberg-compatible format
            Struct transformed = transformValue(id, value, record.topic());

            log.info("CustomCDCTransform output: id={}, dedup_key={}, type={}, version={}",
                transformed.get("id"), transformed.get("dedup_key"), transformed.get("type"),
                transformed.get("version"));

            // Create new record with schema + struct
            return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                transformedSchema,
                transformed,
                record.timestamp()
            );

        } catch (DataException e) {
            // Re-throw DataException as-is (already logged)
            throw e;
        } catch (Exception e) {
            String errorMsg = String.format(
                "CustomCDCTransform FAILED. topic=%s, partition=%s, offset=%s, value=%s",
                record.topic(), record.kafkaPartition(), getOffset(record), record.value());
            log.error(errorMsg, e);
            throw new DataException(errorMsg, e);
        }
    }

    /**
     * APPEND MODE handler. Preserves the raw String value in `data`, enriches it
     * with safe API metadata and routes the resulting nine-column record.
     */
    private R applyAppend(R record) {
        try {
            Object val = record.value();
            if (!(val instanceof String)) {
                throw new DataException(String.format(
                    "Append mode requires a String value from StringConverter, but received %s " +
                        "(topic=%s, partition=%s, offset=%s)",
                    val.getClass().getName(), record.topic(), record.kafkaPartition(), getOffset(record)));
            }

            String data = (String) val;
            String maNguonDuLieu = generateId(record);
            String body = readKafkaHeader(record, appendBodyHeader);
            String httpHeaders = sanitizeHttpHeaders(
                readKafkaHeader(record, appendHeadersHeader), record);
            Clock effectiveClock = clockOverride == null
                ? Clock.system(appendZone)
                : clockOverride.withZone(appendZone);
            ZonedDateTime now = ZonedDateTime.now(effectiveClock);
            String ingestDate = now.toLocalDate().toString();
            String ingestTime = INGEST_TIME_FORMATTER.format(now);
            String icebergTable = resolveTable(record.topic());

            Struct out = new Struct(appendSchema);
            out.put("loainguon", appendSourceType);
            out.put("manguondulieu", maNguonDuLieu);
            out.put("sukien", null);
            out.put("phienban", appendSchemaVersion);
            out.put("body", body);
            out.put("header", httpHeaders);
            out.put("data", data);
            out.put("ingest_date", ingestDate);
            out.put("ingest_time", ingestTime);
            out.put("iceberg_table", icebergTable);

            log.debug(
                "[APPEND] source={}, id={}, table={}, data_len={}, has_body={}, has_headers={}",
                appendSourceType,
                maNguonDuLieu,
                icebergTable,
                data.length(),
                body != null,
                httpHeaders != null);

            return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                appendSchema,
                out,
                record.timestamp()
            );
        } catch (DataException e) {
            throw e;
        } catch (Exception e) {
            String errorMsg = String.format(
                "CustomCDCTransform[append] FAILED. topic=%s, partition=%s, offset=%s",
                record.topic(), record.kafkaPartition(), getOffset(record));
            log.error(errorMsg, e);
            throw new DataException(errorMsg, e);
        }
    }

    private String readKafkaHeader(R record, String headerName) {
        Header kafkaHeader = record.headers().lastWithName(headerName);
        if (kafkaHeader == null || kafkaHeader.value() == null) {
            return null;
        }

        Object value = kafkaHeader.value();
        String text;
        if (value instanceof String) {
            text = (String) value;
        } else if (value instanceof byte[]) {
            text = new String((byte[]) value, StandardCharsets.UTF_8);
        } else if (value instanceof Map || value instanceof List) {
            try {
                text = objectMapper.writeValueAsString(value);
            } catch (Exception e) {
                log.warn(
                    "Ignoring Kafka header {} because its structured value cannot be serialized " +
                        "(topic={}, partition={}, offset={}): {}",
                    headerName,
                    record.topic(),
                    record.kafkaPartition(),
                    getOffset(record),
                    e.getMessage());
                return null;
            }
        } else {
            log.warn(
                "Ignoring Kafka header {} with unsupported value type {} " +
                    "(topic={}, partition={}, offset={})",
                headerName,
                value.getClass().getName(),
                record.topic(),
                record.kafkaPartition(),
                getOffset(record));
            return null;
        }

        return text.trim().isEmpty() ? null : text;
    }

    private String sanitizeHttpHeaders(String rawHeaders, R record) {
        if (rawHeaders == null) {
            return null;
        }

        try {
            JsonNode root = objectMapper.readTree(rawHeaders);
            if (root == null || !root.isObject()) {
                log.warn(
                    "Ignoring {} because it is not a JSON object " +
                        "(topic={}, partition={}, offset={})",
                    appendHeadersHeader,
                    record.topic(),
                    record.kafkaPartition(),
                    getOffset(record));
                return null;
            }

            ObjectNode sanitized = objectMapper.createObjectNode();
            Iterator<Map.Entry<String, JsonNode>> fields = root.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                String normalizedName = field.getKey().trim().toLowerCase(Locale.ROOT);
                if (SAFE_HTTP_HEADERS.contains(normalizedName)) {
                    sanitized.set(normalizedName, field.getValue());
                }
            }

            return sanitized.size() == 0 ? null : objectMapper.writeValueAsString(sanitized);
        } catch (Exception e) {
            log.warn(
                "Ignoring invalid JSON in Kafka header {} " +
                    "(topic={}, partition={}, offset={}): {}",
                appendHeadersHeader,
                record.topic(),
                record.kafkaPartition(),
                getOffset(record),
                e.getMessage());
            return null;
        }
    }

    private String readNonBlankConfig(
            Map<String, ?> configs, String configName, String defaultValue) {
        Object rawValue = configs.get(configName);
        String value = rawValue == null ? defaultValue : rawValue.toString().trim();
        if (value.isEmpty()) {
            throw new ConfigException(configName, rawValue, "Value must not be blank");
        }
        return value;
    }

    private int readPositiveIntConfig(
            Map<String, ?> configs, String configName, int defaultValue) {
        Object rawValue = configs.get(configName);
        if (rawValue == null) {
            return defaultValue;
        }

        final int value;
        try {
            value = rawValue instanceof Number
                ? ((Number) rawValue).intValue()
                : Integer.parseInt(rawValue.toString().trim());
        } catch (NumberFormatException e) {
            throw new ConfigException(configName, rawValue, "Value must be a positive integer");
        }
        if (value < 1) {
            throw new ConfigException(configName, rawValue, "Value must be at least 1");
        }
        return value;
    }
    /**
     * Resolve target Iceberg table for a topic. Append mode requires an explicit
     * mapping; CDC mode retains the namespace/topic fallback for compatibility.
     */
    private String resolveTable(String topic) {
        if (topic != null && topicTableMap.containsKey(topic)) {
            return topicTableMap.get(topic);
        }
        if (MODE_APPEND.equals(mode)) {
            throw new DataException(
                "No topic.table.map entry configured for append topic: " + topic);
        }
        return icebergNamespace + "." + (topic != null ? topic.replace("-", "_") : "unknown");
    }

    private void parseTopicTableMap(Object mapObj) {
        if (mapObj == null || mapObj.toString().trim().isEmpty()) {
            return;
        }

        String rawMap = mapObj.toString();
        for (String rawEntry : rawMap.split(",", -1)) {
            String entry = rawEntry.trim();
            int separator = entry.indexOf(':');
            if (entry.isEmpty() || separator <= 0 || separator != entry.lastIndexOf(':')
                    || separator == entry.length() - 1) {
                throw new ConfigException(
                    TOPIC_TABLE_MAP_CONFIG,
                    rawMap,
                    "Each entry must use topic:namespace.table syntax; invalid entry: '" + entry + "'");
            }

            String topic = entry.substring(0, separator).trim();
            String table = entry.substring(separator + 1).trim();
            if (topic.isEmpty() || !isQualifiedTableName(table)) {
                throw new ConfigException(
                    TOPIC_TABLE_MAP_CONFIG,
                    rawMap,
                    "Each entry must contain a non-blank topic and qualified table; invalid entry: '"
                        + entry + "'");
            }
            if (topicTableMap.putIfAbsent(topic, table) != null) {
                throw new ConfigException(
                    TOPIC_TABLE_MAP_CONFIG, rawMap, "Duplicate mapping for topic: " + topic);
            }
        }
    }

    private boolean isQualifiedTableName(String table) {
        String[] parts = table.split("\\.", -1);
        if (parts.length < 2) {
            return false;
        }
        for (String part : parts) {
            if (part.trim().isEmpty()) {
                return false;
            }
        }
        return true;
    }

    private Struct transformValue(String id, Map<String, Object> value, String topic) throws Exception {        // Extract fields from CDC message
        String businessKey = getStringField(value, "key");
        Object dataObj = value.get("data");
        String type = getStringField(value, "type");
        Object versionObj = value.get("version");
        String ngayCapNhat = getStringField(value, "ngay_cap_nhat");
        Object lengthObj = value.get("length");

        // Validate required fields
        if (dataObj == null) {
            throw new DataException("Missing required field 'data' in CDC message");
        }
        if (type == null) {
            throw new DataException("Missing required field 'type' in CDC message");
        }
        if (versionObj == null) {
            throw new DataException("Missing required field 'version' in CDC message");
        }
        if (businessKey == null) {
            throw new DataException("Missing required field 'key' in CDC message");
        }

        // Extract dedup_key: collect key value from ALL items in data[], sort, join with ||
        // Sort ensures order-independence: [{TRAM002},{TRAM001}] == [{TRAM001},{TRAM002}]
        String dedupKey = null;
        if (dataObj instanceof List) {
            List<?> dataList = (List<?>) dataObj;
            if (dataList.isEmpty()) {
                throw new DataException("Field 'data' is an empty list, cannot extract dedup_key");
            }
            List<String> keyValues = new ArrayList<>();
            for (Object item : dataList) {
                if (item instanceof Map) {
                    Object keyVal = ((Map<?, ?>) item).get(businessKey);
                    if (keyVal != null) {
                        keyValues.add(keyVal.toString());
                    }
                }
            }
            if (!keyValues.isEmpty()) {
                Collections.sort(keyValues);           // sort alphabetically
                dedupKey = String.join("||", keyValues); // join: "TRAM001||TRAM002"
            }
        }
        if (dedupKey == null) {
            throw new DataException("Cannot extract dedup_key from data[] using key field: " + businessKey);
        }

        // Stringify data[] to record (handles both List and other JSON types)
        String recordJson;
        if (dataObj instanceof List) {
            recordJson = objectMapper.writeValueAsString(dataObj);
        } else {
            recordJson = objectMapper.writeValueAsString(dataObj);
        }

        // Cast version to BIGINT (INT64)
        long version = castToLong(versionObj);

        // Keep length as STRING (matching table schema VARCHAR)
        String length = lengthObj != null ? lengthObj.toString() : "0";

        // Map type to _cdc_op
        String cdcOp = "I"; // Default to Insert
        switch (type.toUpperCase()) {
            case "INSERT": cdcOp = "I"; break;
            case "UPDATE": cdcOp = "U"; break;
            case "DELETE": cdcOp = "D"; break;
            default: log.warn("Unknown type: {}, defaulting to 'I'", type);
        }

        // Derive iceberg_table: check map first, fallback to auto-derive
        // Map: topic → "namespace.table" (from config)
        // Fallback: namespace + "." + topic.replace("-", "_")
        String icebergTable;
        if (topicTableMap.containsKey(topic)) {
            icebergTable = topicTableMap.get(topic);
        } else {
            icebergTable = icebergNamespace + "." + (topic != null ? topic.replace("-", "_") : "unknown");
        }

        // Build transformed struct - 10 fields
        Struct transformed = new Struct(transformedSchema);
        transformed.put("id", id);
        transformed.put("dedup_key", dedupKey);
        transformed.put("record", recordJson);
        transformed.put("version", version);
        transformed.put("type", type);
        transformed.put("key", businessKey);
        transformed.put("ngay_cap_nhat", ngayCapNhat);
        transformed.put("length", length);
        transformed.put("iceberg_table", icebergTable);
        transformed.put("_cdc_op", cdcOp);

        return transformed;
    }

    /**
     * Extract composite dedup_key from CDC message without full transform.
     * Collects key values from ALL items in data[], sorts them, joins with "||".
     * Used by the version filter before full processing.
     * Returns null if extraction fails (record will be passed through without filtering).
     *
     * Example: data=[{MaTram:"TRAM002"},{MaTram:"TRAM001"}], key="MaTram"
     *   → sorted: ["TRAM001","TRAM002"]
     *   → result: "TRAM001||TRAM002"
     */
    private String extractDedupKey(Map<String, Object> value) {
        try {
            String businessKey = getStringField(value, "key");
            if (businessKey == null) return null;
            Object dataObj = value.get("data");
            if (!(dataObj instanceof List)) return null;
            List<?> dataList = (List<?>) dataObj;
            if (dataList.isEmpty()) return null;

            List<String> keyValues = new ArrayList<>();
            for (Object item : dataList) {
                if (item instanceof Map) {
                    Object keyVal = ((Map<?, ?>) item).get(businessKey);
                    if (keyVal != null) {
                        keyValues.add(keyVal.toString());
                    }
                }
            }
            if (keyValues.isEmpty()) return null;
            Collections.sort(keyValues);
            return String.join("||", keyValues);
        } catch (Exception e) {
            log.warn("extractDedupKey failed: {}", e.getMessage());
            return null;
        }
    }

    /**
     * Generate deterministic id using topic-partition-offset format.
     * Falls back to UUID if offset is not available (e.g., source records).
     */
    private String generateId(R record) {
        String topic = record.topic() != null ? record.topic() : "unknown";
        Integer partition = record.kafkaPartition();

        // SinkRecord has kafkaOffset() method
        if (record instanceof SinkRecord) {
            long offset = ((SinkRecord) record).kafkaOffset();
            String id = topic + "-" + (partition != null ? partition : 0) + "-" + offset;
            log.debug("Generated id from topic-partition-offset: {}", id);
            return id;
        }

        // Fallback to UUID for non-sink records (unlikely in sink connector)
        String id = UUID.randomUUID().toString();
        log.debug("Generated id from UUID (non-SinkRecord): {}", id);
        return id;
    }

    /**
     * Get offset from record for logging purposes.
     */
    private long getOffset(R record) {
        if (record instanceof SinkRecord) {
            return ((SinkRecord) record).kafkaOffset();
        }
        return -1;
    }

    /**
     * Safely extract a String field from the value map.
     */
    private String getStringField(Map<String, Object> value, String fieldName) {
        Object obj = value.get(fieldName);
        if (obj == null) {
            return null;
        }
        return obj.toString();
    }

    private long castToLong(Object value) {
        if (value instanceof Number) {
            return ((Number) value).longValue();
        } else if (value instanceof String) {
            try {
                return Long.parseLong((String) value);
            } catch (NumberFormatException e) {
                throw new DataException("Cannot parse version as long: '" + value + "'", e);
            }
        }
        throw new DataException("Cannot cast to long: " + value + " (type: " + value.getClass().getName() + ")");
    }

    private Map<String, Object> structToMap(Struct struct) {
        Map<String, Object> map = new HashMap<>();
        for (Field field : struct.schema().fields()) {
            map.put(field.name(), struct.get(field));
        }
        return map;
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        // No resources to close
    }
}
