package com.example.kafka.connect.smt;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Custom SMT to transform CDC messages to Iceberg-compatible format.
 * Phase 1: Append-only mode with 7 fields.
 *
 * Input (custom CDC envelope from Kafka):
 * {
 *   "data": [...],
 *   "key": "MaTram",
 *   "type": "INSERT",
 *   "version": 1,
 *   "ngay_cap_nhat": "2026-04-19T15:00:00Z",
 *   "length": 2
 * }
 *
 * Output (Iceberg-compatible record with 7 fields):
 * {
 *   "id": "tram_quan_trac-0-12",
 *   "record": "[{...}, {...}]",
 *   "version": 1,
 *   "type": "INSERT",
 *   "key": "MaTram",
 *   "ngay_cap_nhat": "2026-04-19T15:00:00Z",
 *   "length": "2"
 * }
 *
 * Notes:
 * - id: topic-partition-offset (deterministic), falls back to UUID if offset unavailable
 * - record: entire data[] serialized as JSON string
 * - version: BIGINT (INT64)
 * - length: STRING (to match table schema VARCHAR)
 * - No dedup_key, ingest_time, _cdc in Phase 1
 */
public class CustomCDCTransform<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger log = LoggerFactory.getLogger(CustomCDCTransform.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    // Config keys
    private static final String ICEBERG_NAMESPACE_CONFIG = "iceberg.namespace";
    private static final String ICEBERG_NAMESPACE_DEFAULT = "default";
    private static final String TOPIC_TABLE_MAP_CONFIG = "topic.table.map";

    // Schema for transformed record
    private Schema transformedSchema;

    // Configurable namespace for table routing (fallback)
    private String icebergNamespace = ICEBERG_NAMESPACE_DEFAULT;

    // Custom topic → table mapping (topic → "namespace.table")
    private final java.util.Map<String, String> topicTableMap = new HashMap<>();

    /**
     * In-memory version cache: dedup_key -> max version seen.
     * Prevents out-of-order / stale records from overwriting newer data in Iceberg.
     */
    private final ConcurrentHashMap<String, Long> versionCache = new ConcurrentHashMap<>();

    @Override
    public void configure(Map<String, ?> configs) {
        // Read configurable namespace (default "default")
        Object nsObj = configs.get(ICEBERG_NAMESPACE_CONFIG);
        if (nsObj != null && !nsObj.toString().isEmpty()) {
            icebergNamespace = nsObj.toString();
        }

        // Read topic→table mapping: "topic1:ns.table1,topic2:ns.table2"
        Object mapObj = configs.get(TOPIC_TABLE_MAP_CONFIG);
        if (mapObj != null && !mapObj.toString().isEmpty()) {
            for (String entry : mapObj.toString().split(",")) {
                String[] parts = entry.trim().split(":");
                if (parts.length == 2) {
                    topicTableMap.put(parts[0].trim(), parts[1].trim());
                }
            }
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

        log.info("CustomCDCTransform configured. 10 fields output. namespace={}", icebergNamespace);
    }

    @Override
    public R apply(R record) {
        // Log input for debugging
        log.info("CustomCDCTransform input: topic={}, partition={}, offset={}, valueClass={}",
            record.topic(),
            record.kafkaPartition(),
            getOffset(record),
            record.value() == null ? "null" : record.value().getClass().getName());

        // Tombstone records: pass through as-is (do NOT return null)
        if (record.value() == null) {
            log.info("Skipping tombstone record (topic={}, partition={}, offset={})",
                record.topic(), record.kafkaPartition(), getOffset(record));
            return record;
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

    private Struct transformValue(String id, Map<String, Object> value, String topic) throws Exception {
        // Extract fields from CDC message
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
        return new ConfigDef();
    }

    @Override
    public void close() {
        // No resources to close
    }
}
