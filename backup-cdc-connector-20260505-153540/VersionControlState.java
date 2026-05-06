package com.example.kafka.connect.iceberg;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Manages version control state and determines actions for CDC messages.
 * 
 * This class implements the version control logic for INSERT, UPDATE, and DELETE
 * operations based on existing versions in the Iceberg table.
 * 
 * Version control rules:
 * - INSERT: Insert if dedup_key not exists, error if exists
 * - UPDATE: Update if version > existing, insert if not exists (upsert), skip if stale
 * - DELETE: Delete if version > existing, skip if stale or not exists
 */
public class VersionControlState {
    private static final Logger log = LoggerFactory.getLogger(VersionControlState.class);
    
    private final Map<String, Long> existingVersions;  // dedup_key -> version
    
    /**
     * Action enum representing the decision for a CDC message.
     */
    public enum Action {
        INSERT,   // New record, no existing version
        UPDATE,   // Existing record, incoming version > existing version
        DELETE,   // Existing record, incoming version > existing version
        SKIP,     // Incoming version <= existing version
        ERROR     // Conflict (e.g., INSERT on existing key)
    }
    
    /**
     * Constructs a VersionControlState with existing versions from the table.
     * 
     * @param existingVersions Map of dedup_key to version from Iceberg table
     */
    public VersionControlState(Map<String, Long> existingVersions) {
        this.existingVersions = existingVersions;
    }
    
    /**
     * Determines the action to take for a CDC message based on version control rules.
     * 
     * INSERT rules:
     * - If dedup_key not exists → INSERT
     * - If dedup_key exists → ERROR (log WARNING)
     * 
     * UPDATE rules:
     * - If dedup_key not exists → INSERT (upsert behavior)
     * - If version > existing → UPDATE
     * - If version <= existing → SKIP (log INFO)
     * 
     * DELETE rules:
     * - If dedup_key not exists → SKIP (log WARNING)
     * - If version > existing → DELETE
     * - If version <= existing → SKIP (log INFO)
     * 
     * @param msg The parsed CDC message
     * @return The action to take
     */
    public Action determineAction(ParsedMessage msg) {
        String dedupKey = msg.getDedupKey();
        Long existingVersion = existingVersions.get(dedupKey);
        String type = msg.getOperationType();
        long incomingVersion = msg.getVersion();
        
        switch (type) {
            case "INSERT":
                if (existingVersion == null) {
                    log.debug("INSERT action: dedup_key={} does not exist", dedupKey);
                    return Action.INSERT;
                } else {
                    log.warn("INSERT conflict: dedup_key={} already exists with version={}, incoming version={}", 
                        dedupKey, existingVersion, incomingVersion);
                    return Action.ERROR;
                }
                
            case "UPDATE":
                if (existingVersion == null) {
                    log.info("UPDATE on non-existing key, treating as INSERT (upsert): dedup_key={}, version={}", 
                        dedupKey, incomingVersion);
                    return Action.INSERT;
                } else if (incomingVersion > existingVersion) {
                    log.debug("UPDATE action: dedup_key={}, incoming version {} > existing {}", 
                        dedupKey, incomingVersion, existingVersion);
                    return Action.UPDATE;
                } else {
                    log.info("UPDATE skipped (stale): dedup_key={}, incoming version {} <= existing {}", 
                        dedupKey, incomingVersion, existingVersion);
                    return Action.SKIP;
                }
                
            case "DELETE":
                if (existingVersion == null) {
                    log.warn("DELETE on non-existing key: dedup_key={}, version={}", 
                        dedupKey, incomingVersion);
                    return Action.SKIP;
                } else if (incomingVersion > existingVersion) {
                    log.debug("DELETE action: dedup_key={}, incoming version {} > existing {}", 
                        dedupKey, incomingVersion, existingVersion);
                    return Action.DELETE;
                } else {
                    log.info("DELETE skipped (stale): dedup_key={}, incoming version {} <= existing {}", 
                        dedupKey, incomingVersion, existingVersion);
                    return Action.SKIP;
                }
                
            default:
                log.error("Unknown operation type: {} for dedup_key={}", type, dedupKey);
                return Action.ERROR;
        }
    }
    
    /**
     * Gets the existing version for a dedup_key.
     * 
     * @param dedupKey The dedup_key to look up
     * @return The existing version, or null if not found
     */
    public Long getExistingVersion(String dedupKey) {
        return existingVersions.get(dedupKey);
    }
    
    /**
     * Gets the number of existing versions in the state.
     * 
     * @return The count of existing versions
     */
    public int getExistingVersionCount() {
        return existingVersions.size();
    }
}
