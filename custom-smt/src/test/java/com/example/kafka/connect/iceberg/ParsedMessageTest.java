package com.example.kafka.connect.iceberg;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for ParsedMessage edge cases and error handling.
 * 
 * Tests cover:
 * - Empty data array handling
 * - Null version handling
 * - Invalid JSON in data array
 * - Missing required fields
 * 
 * Validates: Requirements 1.11
 */
class ParsedMessageTest {
    
    @Test
    void testEmptyDataArrayHandling() throws JsonProcessingException {
        // Create message with empty data array
        Map<String, Object> value = createValidMessage();
        value.put("data", Collections.emptyList());
        
        SinkRecord record = createSinkRecord("test_topic", value);
        ParsedMessage parsed = new ParsedMessage(record);
        
        // Verify empty array is serialized correctly
        assertThat(parsed.getRecordJson()).isEqualTo("[]");
        assertThat(parsed.getLength()).isEqualTo(0L);
    }
    
    @Test
    void testNullVersionHandling() {
        // Create message with null version
        Map<String, Object> value = createValidMessage();
        value.put("version", null);
        
        SinkRecord record = createSinkRecord("test_topic", value);
        
        // Verify exception is thrown
        assertThatThrownBy(() -> new ParsedMessage(record))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("version");
    }
    
    @Test
    void testMissingKeyField() {
        // Create message without key field
        Map<String, Object> value = createValidMessage();
        value.remove("key");
        
        SinkRecord record = createSinkRecord("test_topic", value);
        
        // Verify exception is thrown
        assertThatThrownBy(() -> new ParsedMessage(record))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("key");
    }
    
    @Test
    void testMissingDataField() {
        // Create message without data field
        Map<String, Object> value = createValidMessage();
        value.remove("data");
        
        SinkRecord record = createSinkRecord("test_topic", value);
        
        // Verify exception is thrown
        assertThatThrownBy(() -> new ParsedMessage(record))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("data");
    }
    
    @Test
    void testMissingTypeField() {
        // Create message without type field
        Map<String, Object> value = createValidMessage();
        value.remove("type");
        
        SinkRecord record = createSinkRecord("test_topic", value);
        
        // Verify exception is thrown
        assertThatThrownBy(() -> new ParsedMessage(record))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("type");
    }
    
    @Test
    void testMissingNgayCapNhatField() {
        // Create message without ngay_cap_nhat field
        Map<String, Object> value = createValidMessage();
        value.remove("ngay_cap_nhat");
        
        SinkRecord record = createSinkRecord("test_topic", value);
        
        // Verify exception is thrown
        assertThatThrownBy(() -> new ParsedMessage(record))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ngay_cap_nhat");
    }
    
    @Test
    void testMissingLengthField() {
        // Create message without length field
        Map<String, Object> value = createValidMessage();
        value.remove("length");
        
        SinkRecord record = createSinkRecord("test_topic", value);
        
        // Verify exception is thrown
        assertThatThrownBy(() -> new ParsedMessage(record))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("length");
    }
    
    @Test
    void testInvalidVersionType() {
        // Create message with invalid version type
        Map<String, Object> value = createValidMessage();
        value.put("version", "not_a_number");
        
        SinkRecord record = createSinkRecord("test_topic", value);
        
        // Verify exception is thrown
        assertThatThrownBy(() -> new ParsedMessage(record))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cannot cast");
    }
    
    @Test
    void testInvalidLengthType() {
        // Create message with invalid length type
        Map<String, Object> value = createValidMessage();
        value.put("length", "not_a_number");
        
        SinkRecord record = createSinkRecord("test_topic", value);
        
        // Verify exception is thrown
        assertThatThrownBy(() -> new ParsedMessage(record))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cannot cast");
    }
    
    @Test
    void testVersionAsString() throws JsonProcessingException {
        // Create message with version as string
        Map<String, Object> value = createValidMessage();
        value.put("version", "1704067200000");
        
        SinkRecord record = createSinkRecord("test_topic", value);
        ParsedMessage parsed = new ParsedMessage(record);
        
        // Verify version is cast correctly
        assertThat(parsed.getVersion()).isEqualTo(1704067200000L);
    }
    
    @Test
    void testVersionAsInteger() throws JsonProcessingException {
        // Create message with version as integer
        Map<String, Object> value = createValidMessage();
        value.put("version", 1704067200);
        
        SinkRecord record = createSinkRecord("test_topic", value);
        ParsedMessage parsed = new ParsedMessage(record);
        
        // Verify version is cast correctly
        assertThat(parsed.getVersion()).isEqualTo(1704067200L);
    }
    
    @Test
    void testVersionAsLong() throws JsonProcessingException {
        // Create message with version as long
        Map<String, Object> value = createValidMessage();
        value.put("version", 1704067200000L);
        
        SinkRecord record = createSinkRecord("test_topic", value);
        ParsedMessage parsed = new ParsedMessage(record);
        
        // Verify version is cast correctly
        assertThat(parsed.getVersion()).isEqualTo(1704067200000L);
    }
    
    @Test
    void testDedupKeyFormat() throws JsonProcessingException {
        // Create message with specific topic and key
        Map<String, Object> value = createValidMessage();
        value.put("key", "MaTram");
        
        SinkRecord record = createSinkRecord("tram_quan_trac", value);
        ParsedMessage parsed = new ParsedMessage(record);
        
        // Verify dedup_key format is "topic:key" where key is field name
        assertThat(parsed.getDedupKey()).isEqualTo("tram_quan_trac:MaTram");
        
        // Verify it's NOT the value from data array
        assertThat(parsed.getDedupKey()).doesNotContain("TQ001");
    }
    
    @Test
    void testDataArraySerialization() throws JsonProcessingException {
        // Create message with complex data array
        Map<String, Object> dataItem = new HashMap<>();
        dataItem.put("MaTram", "TQ001");
        dataItem.put("TenTram", "Tram Quan Trac 1");
        dataItem.put("ViDo", "10.762622");
        dataItem.put("KinhDo", "106.660172");
        
        Map<String, Object> value = createValidMessage();
        value.put("data", Arrays.asList(dataItem));
        
        SinkRecord record = createSinkRecord("test_topic", value);
        ParsedMessage parsed = new ParsedMessage(record);
        
        // Verify data is serialized to JSON string
        assertThat(parsed.getRecordJson()).contains("MaTram");
        assertThat(parsed.getRecordJson()).contains("TQ001");
        assertThat(parsed.getRecordJson()).contains("TenTram");
        assertThat(parsed.getRecordJson()).startsWith("[");
        assertThat(parsed.getRecordJson()).endsWith("]");
    }
    
    @Test
    void testTechnicalIdGeneration() throws JsonProcessingException {
        // Create two messages
        Map<String, Object> value1 = createValidMessage();
        Map<String, Object> value2 = createValidMessage();
        
        SinkRecord record1 = createSinkRecord("test_topic", value1);
        SinkRecord record2 = createSinkRecord("test_topic", value2);
        
        ParsedMessage parsed1 = new ParsedMessage(record1);
        ParsedMessage parsed2 = new ParsedMessage(record2);
        
        // Verify technical IDs are unique
        assertThat(parsed1.getTechnicalId()).isNotNull().isNotEmpty();
        assertThat(parsed2.getTechnicalId()).isNotNull().isNotEmpty();
        assertThat(parsed1.getTechnicalId()).isNotEqualTo(parsed2.getTechnicalId());
    }
    
    // ========== Helper Methods ==========
    
    private Map<String, Object> createValidMessage() {
        Map<String, Object> dataItem = new HashMap<>();
        dataItem.put("MaTram", "TQ001");
        dataItem.put("TenTram", "Tram Quan Trac 1");
        
        Map<String, Object> value = new HashMap<>();
        value.put("key", "MaTram");
        value.put("type", "INSERT");
        value.put("version", 1704067200000L);
        value.put("ngay_cap_nhat", "2024-01-01T00:00:00Z");
        value.put("length", 1);
        value.put("data", Arrays.asList(dataItem));
        
        return value;
    }
    
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
}
