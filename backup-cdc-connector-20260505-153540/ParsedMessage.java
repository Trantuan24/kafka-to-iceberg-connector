package com.example.kafka.connect.iceberg;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Represents a parsed CDC message with all required fields extracted and transformed.
 * 
 * This class handles the internal parsing and transformation of Kafka CDC messages,
 * extracting fields from the message value and constructing the dedup_key for
 * version control and deduplication.
 * 
 * Key transformations:
 * - data[] array → JSON string (record field)
 * - dedup_key = "topic:key" where key is the business key field name (NOT value)
 * - version and length cast to BIGINT (long)
 * - ngay_cap_nhat → ingest_time
 */
public class ParsedMessage {
    private static final Logger log = LoggerFactory.getLogger(ParsedMessage.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    private final String technicalId;      // UUID or topic-partition-offset
    private final String dedupKey;         // "topic:key" where key is field name
    private final String recordJson;       // JSON serialization of data[]
    private final String ingestTime;       // From ngay_cap_nhat
    private final long length;             // Cast to BIGINT
    private final String businessKey;      // Business key field name (e.g., "MaTram")
    private final String operationType;    // INSERT, UPDATE, or DELETE
    private final long version;            // Cast to BIGINT
    
    /**
     * Constructs a ParsedMessage from a Kafka SinkRecord.
     * 
     * @param kafkaRecord The Kafka SinkRecord containing the CDC message
     * @throws IllegalArgumentException if required fields are missing or invalid
     * @throws JsonProcessingException if data array cannot be serialized to JSON
     */
    @SuppressWarnings("unchecked")
    public ParsedMessage(SinkRecord kafkaRecord) throws JsonProcessingException {
        Map<String, Object> value = (Map<String, Object>) kafkaRecord.value();
        
        // Extract required fields
        this.businessKey = (String) value.get("key");
        if (this.businessKey == null) {
            throw new IllegalArgumentException("Missing required field: key");
        }
        
        List<Map<String, Object>> dataArray = (List<Map<String, Object>>) value.get("data");
        if (dataArray == null) {
            throw new IllegalArgumentException("Missing required field: data");
        }
        
        this.operationType = (String) value.get("type");
        if (this.operationType == null) {
            throw new IllegalArgumentException("Missing required field: type");
        }
        
        this.ingestTime = (String) value.get("ngay_cap_nhat");
        if (this.ingestTime == null) {
            throw new IllegalArgumentException("Missing required field: ngay_cap_nhat");
        }
        
        // Cast version to BIGINT
        Object versionObj = value.get("version");
        if (versionObj == null) {
            throw new IllegalArgumentException("Missing required field: version");
        }
        this.version = castToLong(versionObj);
        
        // Cast length to BIGINT
        Object lengthObj = value.get("length");
        if (lengthObj == null) {
            throw new IllegalArgumentException("Missing required field: length");
        }
        this.length = castToLong(lengthObj);
        
        // Transform data[] to JSON string
        this.recordJson = serializeToJson(dataArray);
        
        // Construct dedup_key = "topic:key" where key is the field name (NOT value)
        // Example: "tram_quan_trac:MaTram" (NOT "tram_quan_trac:TQ001")
        this.dedupKey = kafkaRecord.topic() + ":" + this.businessKey;
        
        // Generate technical ID
        this.technicalId = generateTechnicalId(kafkaRecord);
        
        log.debug("Parsed message: dedupKey={}, type={}, version={}", 
            this.dedupKey, this.operationType, this.version);
    }
    
    /**
     * Casts a value to long (BIGINT).
     * Handles both Number types and String representations of numbers.
     * 
     * @param value The value to cast (Number or String)
     * @return The long value
     * @throws IllegalArgumentException if value cannot be cast to long
     */
    private long castToLong(Object value) {
        if (value instanceof Number) {
            return ((Number) value).longValue();
        } else if (value instanceof String) {
            try {
                return Long.parseLong((String) value);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Cannot cast string to long: " + value, e);
            }
        }
        throw new IllegalArgumentException("Cannot cast to long: " + value + " (type: " + value.getClass().getName() + ")");
    }
    
    /**
     * Serializes a data array to JSON string.
     * 
     * @param dataArray The data array to serialize
     * @return JSON string representation
     * @throws JsonProcessingException if serialization fails
     */
    private String serializeToJson(List<Map<String, Object>> dataArray) throws JsonProcessingException {
        return objectMapper.writeValueAsString(dataArray);
    }
    
    /**
     * Generates a technical ID for the message.
     * Uses UUID for simplicity and uniqueness.
     * 
     * Alternative: topic-partition-offset format
     * 
     * @param record The Kafka SinkRecord
     * @return Technical ID string
     */
    private String generateTechnicalId(SinkRecord record) {
        // Option 1: UUID (simple and unique)
        return UUID.randomUUID().toString();
        
        // Option 2: topic-partition-offset (deterministic)
        // return String.format("%s-%d-%d", 
        //     record.topic(), record.kafkaPartition(), record.kafkaOffset());
    }
    
    // Getters
    
    public String getTechnicalId() {
        return technicalId;
    }
    
    public String getDedupKey() {
        return dedupKey;
    }
    
    public String getRecordJson() {
        return recordJson;
    }
    
    public String getIngestTime() {
        return ingestTime;
    }
    
    public long getLength() {
        return length;
    }
    
    public String getBusinessKey() {
        return businessKey;
    }
    
    public String getOperationType() {
        return operationType;
    }
    
    public long getVersion() {
        return version;
    }
    
    @Override
    public String toString() {
        return "ParsedMessage{" +
                "technicalId='" + technicalId + '\'' +
                ", dedupKey='" + dedupKey + '\'' +
                ", businessKey='" + businessKey + '\'' +
                ", operationType='" + operationType + '\'' +
                ", version=" + version +
                ", length=" + length +
                ", ingestTime='" + ingestTime + '\'' +
                '}';
    }
}
