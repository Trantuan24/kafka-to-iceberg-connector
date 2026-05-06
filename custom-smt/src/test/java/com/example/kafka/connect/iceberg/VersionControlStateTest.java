package com.example.kafka.connect.iceberg;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for VersionControlState class.
 * 
 * Tests cover:
 * - INSERT conflict scenarios
 * - UPDATE skip scenarios
 * - DELETE on non-existing key scenarios
 * - Logging verification (manual inspection of logs)
 * 
 * Validates: Requirements 11.1, 11.2, 11.5
 */
class VersionControlStateTest {
    
    @Test
    void testInsertOnNonExistingKey() throws JsonProcessingException {
        // Create INSERT message
        ParsedMessage msg = createMessage("INSERT", "key1", 1000L);
        
        // Empty existing versions
        Map<String, Long> existingVersions = new HashMap<>();
        
        VersionControlState state = new VersionControlState(existingVersions);
        VersionControlState.Action action = state.determineAction(msg);
        
        // Verify INSERT action
        assertThat(action).isEqualTo(VersionControlState.Action.INSERT);
    }
    
    @Test
    void testInsertConflict() throws JsonProcessingException {
        // Create INSERT message
        ParsedMessage msg = createMessage("INSERT", "key1", 1000L);
        
        // Existing version for same key
        Map<String, Long> existingVersions = new HashMap<>();
        existingVersions.put(msg.getDedupKey(), 500L);
        
        VersionControlState state = new VersionControlState(existingVersions);
        VersionControlState.Action action = state.determineAction(msg);
        
        // Verify ERROR action (logs WARNING)
        assertThat(action).isEqualTo(VersionControlState.Action.ERROR);
    }
    
    @Test
    void testUpdateWithNewerVersion() throws JsonProcessingException {
        // Create UPDATE message
        ParsedMessage msg = createMessage("UPDATE", "key1", 1000L);
        
        // Existing version is older
        Map<String, Long> existingVersions = new HashMap<>();
        existingVersions.put(msg.getDedupKey(), 500L);
        
        VersionControlState state = new VersionControlState(existingVersions);
        VersionControlState.Action action = state.determineAction(msg);
        
        // Verify UPDATE action
        assertThat(action).isEqualTo(VersionControlState.Action.UPDATE);
    }
    
    @Test
    void testUpdateAsUpsert() throws JsonProcessingException {
        // Create UPDATE message
        ParsedMessage msg = createMessage("UPDATE", "key1", 1000L);
        
        // No existing version
        Map<String, Long> existingVersions = new HashMap<>();
        
        VersionControlState state = new VersionControlState(existingVersions);
        VersionControlState.Action action = state.determineAction(msg);
        
        // Verify INSERT action (upsert behavior, logs INFO)
        assertThat(action).isEqualTo(VersionControlState.Action.INSERT);
    }
    
    @Test
    void testUpdateWithStaleVersion() throws JsonProcessingException {
        // Create UPDATE message
        ParsedMessage msg = createMessage("UPDATE", "key1", 1000L);
        
        // Existing version is newer
        Map<String, Long> existingVersions = new HashMap<>();
        existingVersions.put(msg.getDedupKey(), 2000L);
        
        VersionControlState state = new VersionControlState(existingVersions);
        VersionControlState.Action action = state.determineAction(msg);
        
        // Verify SKIP action (logs INFO)
        assertThat(action).isEqualTo(VersionControlState.Action.SKIP);
    }
    
    @Test
    void testUpdateWithEqualVersion() throws JsonProcessingException {
        // Create UPDATE message
        ParsedMessage msg = createMessage("UPDATE", "key1", 1000L);
        
        // Existing version is equal
        Map<String, Long> existingVersions = new HashMap<>();
        existingVersions.put(msg.getDedupKey(), 1000L);
        
        VersionControlState state = new VersionControlState(existingVersions);
        VersionControlState.Action action = state.determineAction(msg);
        
        // Verify SKIP action (logs INFO)
        assertThat(action).isEqualTo(VersionControlState.Action.SKIP);
    }
    
    @Test
    void testDeleteWithNewerVersion() throws JsonProcessingException {
        // Create DELETE message
        ParsedMessage msg = createMessage("DELETE", "key1", 1000L);
        
        // Existing version is older
        Map<String, Long> existingVersions = new HashMap<>();
        existingVersions.put(msg.getDedupKey(), 500L);
        
        VersionControlState state = new VersionControlState(existingVersions);
        VersionControlState.Action action = state.determineAction(msg);
        
        // Verify DELETE action
        assertThat(action).isEqualTo(VersionControlState.Action.DELETE);
    }
    
    @Test
    void testDeleteWithStaleVersion() throws JsonProcessingException {
        // Create DELETE message
        ParsedMessage msg = createMessage("DELETE", "key1", 1000L);
        
        // Existing version is newer
        Map<String, Long> existingVersions = new HashMap<>();
        existingVersions.put(msg.getDedupKey(), 2000L);
        
        VersionControlState state = new VersionControlState(existingVersions);
        VersionControlState.Action action = state.determineAction(msg);
        
        // Verify SKIP action (logs INFO)
        assertThat(action).isEqualTo(VersionControlState.Action.SKIP);
    }
    
    @Test
    void testDeleteOnNonExistingKey() throws JsonProcessingException {
        // Create DELETE message
        ParsedMessage msg = createMessage("DELETE", "key1", 1000L);
        
        // No existing version
        Map<String, Long> existingVersions = new HashMap<>();
        
        VersionControlState state = new VersionControlState(existingVersions);
        VersionControlState.Action action = state.determineAction(msg);
        
        // Verify SKIP action (logs WARNING)
        assertThat(action).isEqualTo(VersionControlState.Action.SKIP);
    }
    
    @Test
    void testUnknownOperationType() throws JsonProcessingException {
        // Create message with unknown type
        ParsedMessage msg = createMessage("UNKNOWN", "key1", 1000L);
        
        Map<String, Long> existingVersions = new HashMap<>();
        
        VersionControlState state = new VersionControlState(existingVersions);
        VersionControlState.Action action = state.determineAction(msg);
        
        // Verify ERROR action (logs ERROR)
        assertThat(action).isEqualTo(VersionControlState.Action.ERROR);
    }
    
    @Test
    void testGetExistingVersion() throws JsonProcessingException {
        // Create message
        ParsedMessage msg = createMessage("UPDATE", "key1", 1000L);
        
        // Existing version
        Map<String, Long> existingVersions = new HashMap<>();
        existingVersions.put(msg.getDedupKey(), 500L);
        
        VersionControlState state = new VersionControlState(existingVersions);
        
        // Verify getExistingVersion returns correct value
        assertThat(state.getExistingVersion(msg.getDedupKey())).isEqualTo(500L);
    }
    
    @Test
    void testGetExistingVersionNotFound() throws JsonProcessingException {
        // Create message
        ParsedMessage msg = createMessage("UPDATE", "key1", 1000L);
        
        // Empty existing versions
        Map<String, Long> existingVersions = new HashMap<>();
        
        VersionControlState state = new VersionControlState(existingVersions);
        
        // Verify getExistingVersion returns null
        assertThat(state.getExistingVersion(msg.getDedupKey())).isNull();
    }
    
    @Test
    void testGetExistingVersionCount() {
        // Create existing versions
        Map<String, Long> existingVersions = new HashMap<>();
        existingVersions.put("key1", 100L);
        existingVersions.put("key2", 200L);
        existingVersions.put("key3", 300L);
        
        VersionControlState state = new VersionControlState(existingVersions);
        
        // Verify count
        assertThat(state.getExistingVersionCount()).isEqualTo(3);
    }
    
    @Test
    void testIdempotence() throws JsonProcessingException {
        // Create messages
        ParsedMessage msg1 = createMessage("INSERT", "key1", 1000L);
        ParsedMessage msg2 = createMessage("UPDATE", "key2", 2000L);
        ParsedMessage msg3 = createMessage("DELETE", "key3", 3000L);
        
        // Existing versions
        Map<String, Long> existingVersions = new HashMap<>();
        existingVersions.put(msg2.getDedupKey(), 1500L);
        existingVersions.put(msg3.getDedupKey(), 2500L);
        
        // Process first time
        VersionControlState state1 = new VersionControlState(new HashMap<>(existingVersions));
        VersionControlState.Action action1_1 = state1.determineAction(msg1);
        VersionControlState.Action action1_2 = state1.determineAction(msg2);
        VersionControlState.Action action1_3 = state1.determineAction(msg3);
        
        // Process second time with same state
        VersionControlState state2 = new VersionControlState(new HashMap<>(existingVersions));
        VersionControlState.Action action2_1 = state2.determineAction(msg1);
        VersionControlState.Action action2_2 = state2.determineAction(msg2);
        VersionControlState.Action action2_3 = state2.determineAction(msg3);
        
        // Verify results are identical
        assertThat(action1_1).isEqualTo(action2_1);
        assertThat(action1_2).isEqualTo(action2_2);
        assertThat(action1_3).isEqualTo(action2_3);
    }
    
    // ========== Helper Methods ==========
    
    private ParsedMessage createMessage(String type, String key, long version) throws JsonProcessingException {
        Map<String, Object> dataItem = new HashMap<>();
        dataItem.put("field", "value");
        
        Map<String, Object> value = new HashMap<>();
        value.put("key", key);
        value.put("type", type);
        value.put("version", version);
        value.put("ngay_cap_nhat", "2024-01-01T00:00:00Z");
        value.put("length", 1);
        value.put("data", Arrays.asList(dataItem));
        
        SinkRecord record = new SinkRecord(
                "test_topic",
                0,  // partition
                null,  // key schema
                null,  // key
                null,  // value schema
                value,
                0L  // offset
        );
        
        return new ParsedMessage(record);
    }
}
