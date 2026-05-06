package com.example.kafka.connect.iceberg;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.jqwik.api.*;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.NotEmpty;
import net.jqwik.api.constraints.Size;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.*;

import static org.assertj.core.api.Assertions.*;

/**
 * Property-based tests for ParsedMessage class.
 * 
 * Tests validate the 20 correctness properties defined in the design document.
 * Each property test runs 100 iterations with randomly generated inputs.
 * 
 * Phase 2 Properties:
 * - Property 1: Message Field Extraction Completeness
 * - Property 2: Data Array Serialization Round-Trip
 * - Property 3: Dedup Key Construction
 * - Property 4: Numeric Type Casting
 * - Property 5: Field Mapping Preservation
 */
@Label("Feature: cdc-version-control-connector")
public class ParsedMessagePropertyTest {
    
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    /**
     * Property 1: Message Field Extraction Completeness
     * 
     * For any valid CDC message containing the required fields (data, key, type, version, 
     * ngay_cap_nhat, length), parsing SHALL successfully extract all fields with correct values.
     * 
     * Validates: Requirements 1.2, 1.3, 1.5
     */
    @Property(tries = 100)
    @Label("Property 1: Message Field Extraction Completeness")
    void testMessageFieldExtractionCompleteness(
            @ForAll("validCDCMessages") Map<String, Object> messageValue,
            @ForAll @NotEmpty String topic
    ) throws JsonProcessingException {
        // Create SinkRecord
        SinkRecord record = createSinkRecord(topic, messageValue);
        
        // Parse message
        ParsedMessage parsed = new ParsedMessage(record);
        
        // Verify all fields extracted correctly
        assertThat(parsed.getBusinessKey()).isEqualTo(messageValue.get("key"));
        assertThat(parsed.getOperationType()).isEqualTo(messageValue.get("type"));
        assertThat(parsed.getIngestTime()).isEqualTo(messageValue.get("ngay_cap_nhat"));
        
        // Verify version and length are cast correctly
        long expectedVersion = castToLong(messageValue.get("version"));
        long expectedLength = castToLong(messageValue.get("length"));
        assertThat(parsed.getVersion()).isEqualTo(expectedVersion);
        assertThat(parsed.getLength()).isEqualTo(expectedLength);
        
        // Verify dedup_key format
        String expectedDedupKey = topic + ":" + messageValue.get("key");
        assertThat(parsed.getDedupKey()).isEqualTo(expectedDedupKey);
        
        // Verify technical ID is generated
        assertThat(parsed.getTechnicalId()).isNotNull().isNotEmpty();
    }
    
    /**
     * Property 2: Data Array Serialization Round-Trip
     * 
     * For any data array from a CDC message, serializing to JSON string and then 
     * deserializing SHALL preserve the original structure and values.
     * 
     * Validates: Requirements 1.4
     */
    @Property(tries = 100)
    @Label("Property 2: Data Array Serialization Round-Trip")
    void testDataArraySerializationRoundTrip(
            @ForAll("dataArrays") List<Map<String, Object>> dataArray
    ) throws JsonProcessingException {
        // Serialize to JSON
        String json = objectMapper.writeValueAsString(dataArray);
        
        // Deserialize back
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> deserialized = objectMapper.readValue(json, List.class);
        
        // Verify structure preserved
        assertThat(deserialized).isEqualTo(dataArray);
    }
    
    /**
     * Property 3: Dedup Key Construction
     * 
     * For any topic name and business key field name, the constructed dedup_key SHALL 
     * equal the concatenation of topic, colon, and business key field name 
     * (format: "topic:key" where key is the field name, NOT the value).
     * 
     * Validates: Requirements 1.6
     */
    @Property(tries = 100)
    @Label("Property 3: Dedup Key Construction")
    void testDedupKeyConstruction(
            @ForAll @NotEmpty String topic,
            @ForAll @NotEmpty String businessKeyFieldName,
            @ForAll("validCDCMessages") Map<String, Object> messageValue
    ) throws JsonProcessingException {
        // Override the key field with our test business key field name
        messageValue.put("key", businessKeyFieldName);
        
        // Create SinkRecord
        SinkRecord record = createSinkRecord(topic, messageValue);
        
        // Parse message
        ParsedMessage parsed = new ParsedMessage(record);
        
        // Verify dedup_key format: "topic:key" where key is field name
        String expectedDedupKey = topic + ":" + businessKeyFieldName;
        assertThat(parsed.getDedupKey()).isEqualTo(expectedDedupKey);
        
        // Verify it's NOT the value from the data array
        assertThat(parsed.getDedupKey()).doesNotContain("TQ001");
        assertThat(parsed.getDedupKey()).doesNotContain("value");
    }
    
    /**
     * Property 4: Numeric Type Casting
     * 
     * For any valid numeric value (either as Number type or numeric String), 
     * casting to BIGINT SHALL produce the correct long integer value.
     * 
     * Validates: Requirements 1.7, 1.8
     */
    @Property(tries = 100)
    @Label("Property 4: Numeric Type Casting")
    void testNumericTypeCasting(
            @ForAll("numericValues") Object numericValue,
            @ForAll("validCDCMessages") Map<String, Object> messageValue,
            @ForAll @NotEmpty String topic
    ) throws JsonProcessingException {
        // Test version casting
        messageValue.put("version", numericValue);
        messageValue.put("length", numericValue);
        
        SinkRecord record = createSinkRecord(topic, messageValue);
        ParsedMessage parsed = new ParsedMessage(record);
        
        // Verify casting produces correct long value
        long expectedValue = castToLong(numericValue);
        assertThat(parsed.getVersion()).isEqualTo(expectedValue);
        assertThat(parsed.getLength()).isEqualTo(expectedValue);
    }
    
    /**
     * Property 5: Field Mapping Preservation
     * 
     * For any CDC message with ngay_cap_nhat field, the parsed message's ingest_time 
     * field SHALL equal the ngay_cap_nhat value.
     * 
     * Validates: Requirements 1.9
     */
    @Property(tries = 100)
    @Label("Property 5: Field Mapping Preservation")
    void testFieldMappingPreservation(
            @ForAll @NotEmpty String ngayCapNhat,
            @ForAll("validCDCMessages") Map<String, Object> messageValue,
            @ForAll @NotEmpty String topic
    ) throws JsonProcessingException {
        // Override ngay_cap_nhat with test value
        messageValue.put("ngay_cap_nhat", ngayCapNhat);
        
        SinkRecord record = createSinkRecord(topic, messageValue);
        ParsedMessage parsed = new ParsedMessage(record);
        
        // Verify ingest_time equals ngay_cap_nhat
        assertThat(parsed.getIngestTime()).isEqualTo(ngayCapNhat);
    }
    
    // ========== Arbitraries (Generators) ==========
    
    /**
     * Generates valid CDC messages with all required fields.
     */
    @Provide
    Arbitrary<Map<String, Object>> validCDCMessages() {
        return Combinators.combine(
                Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(50),  // key
                Arbitraries.of("INSERT", "UPDATE", "DELETE"),                   // type
                Arbitraries.longs().greaterOrEqual(0L),                        // version
                Arbitraries.strings().numeric().ofMinLength(10).ofMaxLength(20), // ngay_cap_nhat
                Arbitraries.integers().between(1, 100),                        // length
                dataArrays()                                                    // data
        ).as((key, type, version, ngay, length, data) -> {
            Map<String, Object> msg = new HashMap<>();
            msg.put("key", key);
            msg.put("type", type);
            msg.put("version", version);
            msg.put("ngay_cap_nhat", ngay);
            msg.put("length", length);
            msg.put("data", data);
            return msg;
        });
    }
    
    /**
     * Generates data arrays (list of maps).
     */
    @Provide
    Arbitrary<List<Map<String, Object>>> dataArrays() {
        return Arbitraries.maps(
                Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(20),
                Arbitraries.oneOf(
                        Arbitraries.strings().alpha().ofMaxLength(50),
                        Arbitraries.integers(),
                        Arbitraries.doubles()
                )
        ).ofMinSize(1).ofMaxSize(10)
         .list().ofMinSize(1).ofMaxSize(5);
    }
    
    /**
     * Generates numeric values (both Number and String types).
     */
    @Provide
    Arbitrary<Object> numericValues() {
        return Arbitraries.oneOf(
                Arbitraries.longs().greaterOrEqual(0L).map(l -> (Object) l),
                Arbitraries.integers().greaterOrEqual(0).map(i -> (Object) i),
                Arbitraries.longs().greaterOrEqual(0L).map(l -> (Object) String.valueOf(l))
        );
    }
    
    // ========== Helper Methods ==========
    
    private SinkRecord createSinkRecord(String topic, Map<String, Object> value) {
        return new SinkRecord(
                topic,
                0,  // partition
                null,  // key schema
                null,  // key
                null,  // value schema
                value,
                0L  // offset
        );
    }
    
    private long castToLong(Object value) {
        if (value instanceof Number) {
            return ((Number) value).longValue();
        } else if (value instanceof String) {
            return Long.parseLong((String) value);
        }
        throw new IllegalArgumentException("Cannot cast to long: " + value);
    }
}
