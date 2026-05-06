package com.example.kafka.connect.iceberg;

import com.fasterxml.jackson.core.JsonProcessingException;
import net.jqwik.api.*;
import net.jqwik.api.constraints.NotEmpty;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.*;

import static org.assertj.core.api.Assertions.*;

/**
 * Property-based tests for VersionControlState class.
 * 
 * Tests validate version control rules for INSERT, UPDATE, and DELETE operations.
 * Each property test runs 100 iterations with randomly generated inputs.
 * 
 * Phase 2 Properties:
 * - Property 11: INSERT on Non-Existing Key
 * - Property 12: INSERT Conflict Detection
 * - Property 14: UPDATE with Newer Version
 * - Property 15: UPDATE as Upsert
 * - Property 16: UPDATE with Stale Version
 * - Property 17: DELETE with Newer Version
 * - Property 18: DELETE with Stale Version
 * - Property 19: DELETE on Non-Existing Key
 * - Property 20: Version Control Idempotence
 */
@Label("Feature: cdc-version-control-connector")
public class VersionControlStatePropertyTest {
    
    /**
     * Property 11: INSERT on Non-Existing Key
     * 
     * For any INSERT message where the dedup_key does not exist in the existing 
     * versions map, the message SHALL be categorized for insertion.
     * 
     * Validates: Requirements 4.1
     */
    @Property(tries = 100)
    @Label("Property 11: INSERT on Non-Existing Key")
    void testInsertOnNonExistingKey(
            @ForAll("insertMessages") ParsedMessage msg,
            @ForAll("existingVersionsWithoutKey") Map<String, Long> existingVersions
    ) {
        // Ensure msg.dedupKey is not in existingVersions
        Assume.that(!existingVersions.containsKey(msg.getDedupKey()));
        
        VersionControlState state = new VersionControlState(existingVersions);
        VersionControlState.Action action = state.determineAction(msg);
        
        assertThat(action).isEqualTo(VersionControlState.Action.INSERT);
    }
    
    /**
     * Property 12: INSERT Conflict Detection
     * 
     * For any INSERT message where the dedup_key already exists in the existing 
     * versions map, the message SHALL be categorized as an error and skipped.
     * 
     * Validates: Requirements 4.2
     */
    @Property(tries = 100)
    @Label("Property 12: INSERT Conflict Detection")
    void testInsertConflictDetection(
            @ForAll("insertMessages") ParsedMessage msg,
            @ForAll("existingVersionsWithKey") Map<String, Long> existingVersions
    ) {
        // Ensure msg.dedupKey exists in existingVersions
        existingVersions.put(msg.getDedupKey(), 1000L);
        
        VersionControlState state = new VersionControlState(existingVersions);
        VersionControlState.Action action = state.determineAction(msg);
        
        assertThat(action).isEqualTo(VersionControlState.Action.ERROR);
    }
    
    /**
     * Property 14: UPDATE with Newer Version
     * 
     * For any UPDATE message where the dedup_key exists in the existing versions map 
     * and the incoming version is greater than the existing version, the message 
     * SHALL be categorized for update.
     * 
     * Validates: Requirements 5.1
     */
    @Property(tries = 100)
    @Label("Property 14: UPDATE with Newer Version")
    void testUpdateWithNewerVersion(
            @ForAll("updateMessages") ParsedMessage msg,
            @ForAll("existingVersionsWithOlderVersion") Map<String, Long> existingVersions
    ) {
        // Ensure msg.version > existingVersions.get(msg.dedupKey)
        long olderVersion = msg.getVersion() - 100;
        existingVersions.put(msg.getDedupKey(), olderVersion);
        
        VersionControlState state = new VersionControlState(existingVersions);
        VersionControlState.Action action = state.determineAction(msg);
        
        assertThat(action).isEqualTo(VersionControlState.Action.UPDATE);
    }
    
    /**
     * Property 15: UPDATE as Upsert
     * 
     * For any UPDATE message where the dedup_key does not exist in the existing 
     * versions map, the message SHALL be categorized for insertion (upsert behavior).
     * 
     * Validates: Requirements 5.2
     */
    @Property(tries = 100)
    @Label("Property 15: UPDATE as Upsert")
    void testUpdateAsUpsert(
            @ForAll("updateMessages") ParsedMessage msg,
            @ForAll("existingVersionsWithoutKey") Map<String, Long> existingVersions
    ) {
        // Ensure msg.dedupKey is not in existingVersions
        Assume.that(!existingVersions.containsKey(msg.getDedupKey()));
        
        VersionControlState state = new VersionControlState(existingVersions);
        VersionControlState.Action action = state.determineAction(msg);
        
        assertThat(action).isEqualTo(VersionControlState.Action.INSERT);
    }
    
    /**
     * Property 16: UPDATE with Stale Version
     * 
     * For any UPDATE message where the dedup_key exists in the existing versions map 
     * and the incoming version is less than or equal to the existing version, the 
     * message SHALL be skipped.
     * 
     * Validates: Requirements 5.3
     */
    @Property(tries = 100)
    @Label("Property 16: UPDATE with Stale Version")
    void testUpdateWithStaleVersion(
            @ForAll("updateMessages") ParsedMessage msg,
            @ForAll("existingVersionsWithNewerOrEqualVersion") Map<String, Long> existingVersions
    ) {
        // Ensure msg.version <= existingVersions.get(msg.dedupKey)
        long newerOrEqualVersion = msg.getVersion() + Arbitraries.integers().between(0, 100).sample();
        existingVersions.put(msg.getDedupKey(), newerOrEqualVersion);
        
        VersionControlState state = new VersionControlState(existingVersions);
        VersionControlState.Action action = state.determineAction(msg);
        
        assertThat(action).isEqualTo(VersionControlState.Action.SKIP);
    }
    
    /**
     * Property 17: DELETE with Newer Version
     * 
     * For any DELETE message where the dedup_key exists in the existing versions map 
     * and the incoming version is greater than the existing version, the message 
     * SHALL be categorized for deletion.
     * 
     * Validates: Requirements 6.1, 6.7
     */
    @Property(tries = 100)
    @Label("Property 17: DELETE with Newer Version")
    void testDeleteWithNewerVersion(
            @ForAll("deleteMessages") ParsedMessage msg,
            @ForAll("existingVersionsWithOlderVersion") Map<String, Long> existingVersions
    ) {
        // Ensure msg.version > existingVersions.get(msg.dedupKey)
        long olderVersion = msg.getVersion() - 100;
        existingVersions.put(msg.getDedupKey(), olderVersion);
        
        VersionControlState state = new VersionControlState(existingVersions);
        VersionControlState.Action action = state.determineAction(msg);
        
        assertThat(action).isEqualTo(VersionControlState.Action.DELETE);
    }
    
    /**
     * Property 18: DELETE with Stale Version
     * 
     * For any DELETE message where the dedup_key exists in the existing versions map 
     * and the incoming version is less than or equal to the existing version, the 
     * message SHALL be skipped.
     * 
     * Validates: Requirements 6.2
     */
    @Property(tries = 100)
    @Label("Property 18: DELETE with Stale Version")
    void testDeleteWithStaleVersion(
            @ForAll("deleteMessages") ParsedMessage msg,
            @ForAll("existingVersionsWithNewerOrEqualVersion") Map<String, Long> existingVersions
    ) {
        // Ensure msg.version <= existingVersions.get(msg.dedupKey)
        long newerOrEqualVersion = msg.getVersion() + Arbitraries.integers().between(0, 100).sample();
        existingVersions.put(msg.getDedupKey(), newerOrEqualVersion);
        
        VersionControlState state = new VersionControlState(existingVersions);
        VersionControlState.Action action = state.determineAction(msg);
        
        assertThat(action).isEqualTo(VersionControlState.Action.SKIP);
    }
    
    /**
     * Property 19: DELETE on Non-Existing Key
     * 
     * For any DELETE message where the dedup_key does not exist in the existing 
     * versions map, the message SHALL be skipped.
     * 
     * Validates: Requirements 6.3
     */
    @Property(tries = 100)
    @Label("Property 19: DELETE on Non-Existing Key")
    void testDeleteOnNonExistingKey(
            @ForAll("deleteMessages") ParsedMessage msg,
            @ForAll("existingVersionsWithoutKey") Map<String, Long> existingVersions
    ) {
        // Ensure msg.dedupKey is not in existingVersions
        Assume.that(!existingVersions.containsKey(msg.getDedupKey()));
        
        VersionControlState state = new VersionControlState(existingVersions);
        VersionControlState.Action action = state.determineAction(msg);
        
        assertThat(action).isEqualTo(VersionControlState.Action.SKIP);
    }
    
    /**
     * Property 20: Version Control Idempotence
     * 
     * For any batch of messages processed twice with the same existing versions state, 
     * the categorization results (toInsert, toUpdate, toDelete, skipped) SHALL be identical.
     * 
     * Validates: Requirements 15.1-15.6
     */
    @Property(tries = 100)
    @Label("Property 20: Version Control Idempotence")
    void testVersionControlIdempotence(
            @ForAll("messageBatches") List<ParsedMessage> messages,
            @ForAll("existingVersionsMap") Map<String, Long> existingVersions
    ) {
        // Process batch first time
        VersionControlState state1 = new VersionControlState(new HashMap<>(existingVersions));
        List<VersionControlState.Action> actions1 = new ArrayList<>();
        for (ParsedMessage msg : messages) {
            actions1.add(state1.determineAction(msg));
        }
        
        // Process batch second time with same state
        VersionControlState state2 = new VersionControlState(new HashMap<>(existingVersions));
        List<VersionControlState.Action> actions2 = new ArrayList<>();
        for (ParsedMessage msg : messages) {
            actions2.add(state2.determineAction(msg));
        }
        
        // Verify results are identical
        assertThat(actions1).isEqualTo(actions2);
    }
    
    // ========== Arbitraries (Generators) ==========
    
    /**
     * Generates INSERT messages.
     */
    @Provide
    Arbitrary<ParsedMessage> insertMessages() {
        return parsedMessages("INSERT");
    }
    
    /**
     * Generates UPDATE messages.
     */
    @Provide
    Arbitrary<ParsedMessage> updateMessages() {
        return parsedMessages("UPDATE");
    }
    
    /**
     * Generates DELETE messages.
     */
    @Provide
    Arbitrary<ParsedMessage> deleteMessages() {
        return parsedMessages("DELETE");
    }
    
    /**
     * Generates ParsedMessage instances with specified operation type.
     */
    private Arbitrary<ParsedMessage> parsedMessages(String operationType) {
        return Combinators.combine(
                Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(50),  // topic
                Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(50),  // key
                Arbitraries.longs().greaterOrEqual(1000L),                     // version
                Arbitraries.strings().numeric().ofMinLength(10).ofMaxLength(20), // ngay_cap_nhat
                Arbitraries.integers().between(1, 100)                         // length
        ).as((topic, key, version, ngay, length) -> {
            try {
                Map<String, Object> value = new HashMap<>();
                value.put("key", key);
                value.put("type", operationType);
                value.put("version", version);
                value.put("ngay_cap_nhat", ngay);
                value.put("length", length);
                value.put("data", Arrays.asList(Collections.singletonMap("field", "value")));
                
                SinkRecord record = new SinkRecord(
                        topic, 0, null, null, null, value, 0L
                );
                
                return new ParsedMessage(record);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        });
    }
    
    /**
     * Generates existing versions map without the message's dedup_key.
     */
    @Provide
    Arbitrary<Map<String, Long>> existingVersionsWithoutKey() {
        return Arbitraries.maps(
                Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(50),
                Arbitraries.longs().greaterOrEqual(1000L)
        ).ofMinSize(0).ofMaxSize(10);
    }
    
    /**
     * Generates existing versions map with the message's dedup_key.
     */
    @Provide
    Arbitrary<Map<String, Long>> existingVersionsWithKey() {
        return Arbitraries.maps(
                Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(50),
                Arbitraries.longs().greaterOrEqual(1000L)
        ).ofMinSize(1).ofMaxSize(10);
    }
    
    /**
     * Generates existing versions map with older version.
     */
    @Provide
    Arbitrary<Map<String, Long>> existingVersionsWithOlderVersion() {
        return Arbitraries.maps(
                Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(50),
                Arbitraries.longs().greaterOrEqual(100L).lessOrEqual(900L)
        ).ofMinSize(0).ofMaxSize(10);
    }
    
    /**
     * Generates existing versions map with newer or equal version.
     */
    @Provide
    Arbitrary<Map<String, Long>> existingVersionsWithNewerOrEqualVersion() {
        return Arbitraries.maps(
                Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(50),
                Arbitraries.longs().greaterOrEqual(2000L)
        ).ofMinSize(0).ofMaxSize(10);
    }
    
    /**
     * Generates existing versions map.
     */
    @Provide
    Arbitrary<Map<String, Long>> existingVersionsMap() {
        return Arbitraries.maps(
                Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(50),
                Arbitraries.longs().greaterOrEqual(1000L)
        ).ofMinSize(0).ofMaxSize(20);
    }
    
    /**
     * Generates batches of messages.
     */
    @Provide
    Arbitrary<List<ParsedMessage>> messageBatches() {
        return Arbitraries.oneOf(
                insertMessages(),
                updateMessages(),
                deleteMessages()
        ).list().ofMinSize(1).ofMaxSize(20);
    }
}
