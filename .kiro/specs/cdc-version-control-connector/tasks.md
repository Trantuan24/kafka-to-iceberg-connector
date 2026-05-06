# Implementation Plan: CDC Version Control Connector

## Overview

This implementation plan breaks down the CDC Version Control Connector into discrete coding tasks. The connector is a custom Kafka Connect Sink Connector that processes CDC messages with version control, deduplication, and batch operations using Apache Iceberg.

**Key Implementation Approach**:
- Message-level processing with internal JSON parsing (no SMT chain)
- Business key-based deduplication (dedup_key = "topic:key" where key is field name)
- Single Iceberg transaction for atomic INSERT/UPDATE/DELETE operations
- Row-level delete using equality delete (format-version=2)
- Property-based testing for 20 correctness properties

**📚 Implementation References**:
- See `implementation-references.md` for detailed phase-by-phase reference mapping
- Each phase below links to specific external resources (databricks, DeepWiki, Iceberg API)
- Use the reference guide to understand which documentation to read for each task

## Tasks

### Phase 1: Project Setup & Configuration

**📚 References**: See `implementation-references.md` - Phase 1
- Apache Iceberg Kafka Connect Official Docs (catalog config, S3 config)
- getindata/kafka-connect-iceberg-sink (config naming conventions)

- [ ] 1. Set up Gradle project structure and dependencies
  - Create `build.gradle` with Java 11 configuration
  - Add Kafka Connect API dependencies (compileOnly)
  - Add Iceberg dependencies (core, hive-metastore, aws, data, parquet)
  - Add Hadoop dependencies (client, aws) for S3A support
  - Add Jackson for JSON processing
  - Add SLF4J for logging
  - Configure JAR packaging with runtime dependencies
  - _Requirements: 13.1, 13.2, 13.3, 13.4, 13.5_
  - _Reference: Official Docs for standard config prefixes, getindata for naming patterns_

### Phase 2: Core Data Models

**📚 References**: See `implementation-references.md` - Phase 2
- databricks/iceberg-kafka-connect (message handling patterns)
- Apache Iceberg Kafka Connect Official Docs (SMT examples - understand what NOT to do)

- [ ] 2. Implement ParsedMessage class
  - [ ] 2.1 Create ParsedMessage class with all required fields
    - Define fields: technicalId, dedupKey, recordJson, ingestTime, length, businessKey, operationType, version
    - Implement constructor that accepts SinkRecord
    - Extract fields from Kafka message value (Map<String, Object>)
    - Implement castToLong() helper for version and length type casting
    - Implement serializeToJson() for data array to JSON string conversion
    - Construct dedupKey as "topic:key" where key is business key field name (NOT value)
    - Generate technicalId (UUID or topic-partition-offset)
    - _Requirements: 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.9, 7.2, 7.3, 7.4, 7.8_
    - _Reference: databricks RecordConverter.java for SinkRecord to GenericRecord patterns_
  
  - [ ]* 2.2 Write property test for ParsedMessage field extraction
    - **Property 1: Message Field Extraction Completeness**
    - **Validates: Requirements 1.2, 1.3, 1.5**
    - Generate arbitrary valid CDC messages
    - Verify all fields extracted correctly
    - Run 100 iterations
  
  - [ ]* 2.3 Write property test for data array serialization
    - **Property 2: Data Array Serialization Round-Trip**
    - **Validates: Requirements 1.4**
    - Generate arbitrary data arrays
    - Verify JSON serialization and deserialization preserves structure
    - Run 100 iterations
  
  - [ ]* 2.4 Write property test for dedup_key construction
    - **Property 3: Dedup Key Construction**
    - **Validates: Requirements 1.6**
    - Generate arbitrary topic names and business keys
    - Verify dedupKey format is "topic:key" (key is field name, NOT value)
    - Run 100 iterations
  
  - [ ]* 2.5 Write property test for numeric type casting
    - **Property 4: Numeric Type Casting**
    - **Validates: Requirements 1.7, 1.8**
    - Generate arbitrary numeric values (Number and String types)
    - Verify casting to BIGINT produces correct long values
    - Run 100 iterations
  
  - [ ]* 2.6 Write property test for field mapping
    - **Property 5: Field Mapping Preservation**
    - **Validates: Requirements 1.9**
    - Generate arbitrary CDC messages with ngay_cap_nhat
    - Verify ingest_time equals ngay_cap_nhat
    - Run 100 iterations
  
  - [ ]* 2.7 Write unit tests for ParsedMessage edge cases
    - Test empty data array handling
    - Test null version handling (should throw exception)
    - Test invalid JSON in data array
    - Test missing required fields
    - _Requirements: 1.11_

- [ ] 3. Implement VersionControlState class
  - [ ] 3.1 Create VersionControlState class with Action enum
    - Define Action enum: INSERT, UPDATE, DELETE, SKIP, ERROR
    - Define fields: existingVersions (Map<String, Long>), incomingMessages (Map<String, ParsedMessage>)
    - Implement determineAction(ParsedMessage) method with I/U/D rules
    - Implement INSERT logic: insert if not exists, error if exists
    - Implement UPDATE logic: insert if not exists (upsert), update if newer, skip if stale
    - Implement DELETE logic: delete if newer, skip if stale or not exists
    - Add logging for all decision paths (WARN for conflicts, INFO for skips)
    - _Requirements: 4.1, 4.2, 5.1, 5.2, 5.3, 6.1, 6.2, 6.3, 11.1, 11.2, 11.5_
  
  - [ ]* 3.2 Write property tests for INSERT operation rules
    - **Property 11: INSERT on Non-Existing Key**
    - **Validates: Requirements 4.1**
    - **Property 12: INSERT Conflict Detection**
    - **Validates: Requirements 4.2**
    - Generate arbitrary INSERT messages and existing versions
    - Verify correct action determination
    - Run 100 iterations per property
  
  - [ ]* 3.3 Write property tests for UPDATE operation rules
    - **Property 14: UPDATE with Newer Version**
    - **Validates: Requirements 5.1**
    - **Property 15: UPDATE as Upsert**
    - **Validates: Requirements 5.2**
    - **Property 16: UPDATE with Stale Version**
    - **Validates: Requirements 5.3**
    - Generate arbitrary UPDATE messages and existing versions
    - Verify correct action determination
    - Run 100 iterations per property
  
  - [ ]* 3.4 Write property tests for DELETE operation rules
    - **Property 17: DELETE with Newer Version**
    - **Validates: Requirements 6.1, 6.7**
    - **Property 18: DELETE with Stale Version**
    - **Validates: Requirements 6.2**
    - **Property 19: DELETE on Non-Existing Key**
    - **Validates: Requirements 6.3**
    - Generate arbitrary DELETE messages and existing versions
    - Verify correct action determination
    - Run 100 iterations per property
  
  - [ ]* 3.5 Write property test for version control idempotence
    - **Property 20: Version Control Idempotence**
    - **Validates: Requirements 15.1-15.6**
    - Generate arbitrary batches and existing versions
    - Process batch twice with same state
    - Verify categorization results are identical
    - Run 100 iterations
  
  - [ ]* 3.6 Write unit tests for logging verification
    - Test INSERT conflict logs WARNING with dedup_key
    - Test UPDATE skip logs INFO with versions
    - Test DELETE on non-existing key logs WARNING
    - Use log capture to verify log messages
    - _Requirements: 11.1, 11.2, 11.5_

### Phase 3: Connector Framework

- [ ] 4. Implement VersionControlIcebergSinkConnector
  - [ ] 4.1 Create connector class extending SinkConnector
    - Implement start(Map<String, String> props) method
    - Implement stop() method
    - Implement taskClass() returning VersionControlIcebergSinkTask.class
    - Implement taskConfigs(int maxTasks) returning single task config
    - Implement version() returning connector version
    - Implement config() returning ConfigDef
    - _Requirements: 12.1_
  
  - [ ]* 4.2 Write unit tests for connector lifecycle
    - Test start() with valid configuration
    - Test taskConfigs() returns single task
    - Test version() returns correct version string
    - _Requirements: 12.1_

- [ ] 5. Implement VersionControlIcebergSinkConfig
  - [ ] 5.1 Create configuration class extending AbstractConfig
    - Define all configuration constants (TABLE_NAME_CONFIG, CATALOG_TYPE_CONFIG, etc.)
    - Create ConfigDef with all required and optional properties
    - Add validation for required properties
    - Add range validation for MAX_POLL_RECORDS and FLUSH_INTERVAL
    - Implement typed accessor methods for all properties
    - _Requirements: 12.1, 12.2, 12.3, 12.5_
  
  - [ ]* 5.2 Write unit tests for configuration validation
    - Test missing required properties throw ConfigException
    - Test invalid range values throw ConfigException
    - Test valid configuration passes validation
    - Test accessor methods return correct values
    - _Requirements: 12.5_

### Phase 4: Message Processing & Deduplication

**📚 References**: See `implementation-references.md` - Phase 3
- getindata/kafka-connect-iceberg-sink (dedup-column concept)
- databricks/iceberg-kafka-connect (id-columns concept)

- [ ] 6. Implement message parsing and deduplication logic
  - [ ] 6.1 Implement parseAndTransform() method in SinkTask
    - Iterate through SinkRecord collection
    - Parse each record using ParsedMessage constructor
    - Handle parse exceptions (log ERROR and skip message)
    - Return Map<String, ParsedMessage> keyed by dedup_key
    - _Requirements: 1.1, 1.2, 1.10, 1.11_
  
  - [ ] 6.2 Implement deduplicateByDedupKey() method
    - Use HashMap<String, ParsedMessage> for O(n) deduplication
    - For each message, compare version with existing entry
    - Keep message with maximum version per dedup_key
    - Log deduplication count at INFO level
    - Return deduplicated Map<String, ParsedMessage>
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6_
    - _Reference: getindata RecordBuffer.java for dedup patterns, databricks for id-columns concept_
  
  - [ ]* 6.3 Write property tests for deduplication logic
    - **Property 6: Deduplication by Maximum Version**
    - **Validates: Requirements 2.1, 2.6**
    - **Property 7: Unique Message Preservation**
    - **Validates: Requirements 2.5**
    - **Property 8: Deduplication by Business Key**
    - **Validates: Requirements 2.3**
    - Generate arbitrary message batches with duplicates
    - Verify only max version retained per dedup_key
    - Verify unique messages preserved
    - Run 100 iterations per property
  
  - [ ]* 6.4 Write unit tests for parse error handling
    - Test invalid JSON structure logs ERROR and skips
    - Test missing required fields logs ERROR and skips
    - Test invalid data types logs ERROR and skips
    - Verify batch continues after parse errors
    - _Requirements: 1.11, 11.3_

- [ ] 7. Checkpoint - Verify parsing and deduplication
  - Ensure all tests pass for ParsedMessage, VersionControlState, and deduplication logic
  - Ask the user if questions arise

### Phase 5: Iceberg Integration - Catalog & Table Setup

**📚 References**: See `implementation-references.md` - Phase 4
- Apache Iceberg Java API (Catalog, Table interfaces)
- Apache Iceberg Kafka Connect Official Docs (catalog setup)

- [ ] 8. Implement Iceberg catalog and table management
  - [ ] 8.1 Implement createHiveCatalog() method
    - Create Hadoop Configuration object
    - Set hive.metastore.uris from config
    - Set S3A filesystem properties (endpoint, access key, secret key, path-style access)
    - Set fs.s3a.impl to S3AFileSystem
    - Create and return HadoopCatalog instance
    - _Requirements: 8.1, 8.2, 8.3, 8.4_
    - _Reference: Iceberg Java API HiveCatalog, Official Docs for S3A config_
  
  - [ ] 8.2 Implement table loading and schema verification
    - Load table using catalog.loadTable(TableIdentifier)
    - Verify table schema matches expected schema
    - Verify table format-version is 2 (for row-level operations)
    - Log table properties and schema at startup
    - _Requirements: 8.1, 8.6_
    - _Reference: Iceberg Java API Table interface, format-version validation_
  
  - [ ]* 8.3 Write integration tests for catalog operations
    - Use Testcontainers for MinIO and Hive Metastore
    - Test catalog creation with valid configuration
    - Test table loading succeeds
    - Test connection failure handling
    - _Requirements: 8.1, 8.2, 8.3, 8.4_

### Phase 6: Iceberg Integration - Query Existing Versions

**📚 References**: See `implementation-references.md` - Phase 5
- Apache Iceberg Java API (Table Scan API)
- databricks/iceberg-kafka-connect (query patterns)

- [ ] 9. Implement queryExistingVersions() method
  - [ ] 9.1 Implement Iceberg Table Scan API query
    - Check if dedupKeys set is empty (return empty map if so)
    - Implement query chunking (10k keys per query) for safety
    - Build Expressions.in("dedup_key", chunk) filter for each chunk
    - Use IcebergGenerics.read(table).where(filter).select("dedup_key", "version")
    - Iterate through CloseableIterable<Record> results
    - Build HashMap<String, Long> mapping dedup_key to version
    - Log query execution time and result count
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7_
    - _Reference: Iceberg Java API IcebergGenerics.read(), Expressions.in() for filters_
  
  - [ ]* 9.2 Write property tests for query logic
    - **Property 9: Query Filter Completeness**
    - **Validates: Requirements 3.3, 3.4**
    - **Property 10: Version Map Construction**
    - **Validates: Requirements 3.5**
    - Generate arbitrary dedup_key sets
    - Verify filter includes all keys
    - Verify HashMap correctly maps keys to versions
    - Run 100 iterations per property
  
  - [ ]* 9.3 Write integration tests for Iceberg query
    - Use Testcontainers for MinIO and Hive Metastore
    - Pre-populate table with test data
    - Query existing versions for known keys
    - Verify correct versions returned
    - Test query with empty key set
    - Test query with large key set (>10k keys)
    - _Requirements: 3.1, 3.2, 3.6, 3.7_

### Phase 7: Version Control Logic Application

**📚 References**: See `implementation-references.md` - Phase 6 & 7
- DeepWiki - Upsert Mode (equality delete + append pattern)
- databricks/iceberg-kafka-connect (upsert-mode implementation)
- Apache Iceberg Java API (DeleteFiles API)

- [ ] 10. Implement applyVersionControlRules() method
  - [ ] 10.1 Implement I/U/D categorization logic
    - Iterate through deduplicated messages
    - For each message, get existing version from query results
    - Call VersionControlState.determineAction(msg, existingVersion)
    - Categorize into toInsert, toUpdate, toDelete lists based on action
    - Track skipped and error counts
    - Log categorization summary (counts for each action)
    - _Requirements: 4.1, 4.2, 5.1, 5.2, 5.3, 6.1, 6.2, 6.3_
    - _Reference: databricks UpsertWriter.java for upsert patterns, DeepWiki for UPDATE/DELETE rules_
  
  - [ ] 10.2 Implement createIcebergRecord() helper method
    - Create GenericRecord with Iceberg schema
    - Set all fields: id, dedup_key, record, ingest_time, length, key, type, version
    - Ensure version and length are BIGINT type
    - Return GenericRecord
    - _Requirements: 4.4, 4.5, 4.6, 7.1, 7.2, 7.3, 7.4, 7.5, 7.6, 7.7, 7.8, 7.9_
  
  - [ ]* 10.3 Write property test for inserted record completeness
    - **Property 13: Inserted Record Completeness**
    - **Validates: Requirements 4.4, 4.5, 4.6, 7.1-7.9**
    - Generate arbitrary ParsedMessage instances
    - Create Iceberg records
    - Verify all fields present with correct types
    - Run 100 iterations
  
  - [ ]* 10.4 Write unit tests for categorization logic
    - Test INSERT categorization with various scenarios
    - Test UPDATE categorization with various scenarios
    - Test DELETE categorization with various scenarios
    - Verify logging for each scenario
    - _Requirements: 4.1, 4.2, 5.1, 5.2, 5.3, 6.1, 6.2, 6.3_

### Phase 8: Batch Write Operations - Single Transaction

**📚 References**: See `implementation-references.md` - Phase 8
- Apache Iceberg Java API (Transaction API) - **CRITICAL**
- databricks/iceberg-kafka-connect (commit coordination)
- DeepWiki - Upsert Mode (atomic operations, equality delete mechanism)

- [ ] 11. Implement batchWriteToIceberg() method with atomic transaction
  - [ ] 11.1 Implement single Iceberg transaction for all operations
    - Create Transaction using table.newTransaction()
    - Collect all dedup_keys to delete (from toUpdate + toDelete)
    - Execute single row-level delete using deleteFromRowFilter with IN expression
    - Collect all records to append (toInsert + toUpdate)
    - Write Parquet file with all records to append
    - Execute append operation with DataFile
    - Commit entire transaction atomically using txn.commitTransaction()
    - Log transaction summary (insert/update/delete counts)
    - _Requirements: 5.6, 6.6, 8.7, 15.1, 15.2, 15.3, 15.4_
    - _Reference: Iceberg Java API Transaction interface, DeepWiki equality delete + append pattern_
  
  - [ ] 11.2 Implement writeParquetFile() helper method
    - Generate unique filename using UUID
    - Create OutputFile using table.io().newOutputFile()
    - Create DataWriter with Parquet format and GenericRecord schema
    - Write all records to Parquet file
    - Close writer and return DataFile
    - _Requirements: 8.1, 8.7_
  
  - [ ]* 11.3 Write integration tests for batch write operations
    - Use Testcontainers for MinIO and Hive Metastore
    - Test INSERT operation writes records correctly
    - Test UPDATE operation deletes old and appends new
    - Test DELETE operation removes records
    - Test mixed batch (INSERT + UPDATE + DELETE) in single transaction
    - Verify atomicity (all or nothing)
    - Test transaction rollback on failure
    - _Requirements: 4.7, 5.6, 6.6, 8.7, 15.1, 15.2, 15.3, 15.4_

### Phase 9: Main Task Implementation

- [ ] 12. Implement VersionControlIcebergSinkTask
  - [ ] 12.1 Implement task lifecycle methods
    - Implement start(Map<String, String> props) method
    - Parse configuration using VersionControlIcebergSinkConfig
    - Initialize Iceberg catalog and load table
    - Initialize ObjectMapper for JSON processing
    - Implement stop() method to close resources
    - Implement version() method
    - _Requirements: 12.1, 12.2, 12.3_
  
  - [ ] 12.2 Implement put(Collection<SinkRecord> records) method
    - Check if records collection is empty (return if so)
    - Log batch size and start time
    - Call parseAndTransform(records) to parse messages
    - Call deduplicateByDedupKey() to deduplicate
    - Extract dedup_keys and call queryExistingVersions()
    - Call applyVersionControlRules() to categorize messages
    - Call batchWriteToIceberg() to execute operations
    - Log processing duration and summary metrics
    - Handle exceptions (parse errors skip, Iceberg errors fail task)
    - _Requirements: 1.1, 2.1, 3.1, 4.1, 5.1, 6.1, 8.1, 11.3, 14.1, 14.2, 14.3, 14.4_
  
  - [ ]* 12.3 Write unit tests for task lifecycle
    - Test start() initializes catalog and table
    - Test stop() closes resources
    - Test put() with empty collection returns immediately
    - _Requirements: 12.1_

- [ ] 13. Checkpoint - Verify end-to-end workflow
  - Ensure all integration tests pass
  - Verify single transaction atomicity
  - Ask the user if questions arise

### Phase 10: Error Handling & Logging

**📚 References**: See `implementation-references.md` - Phase 9
- databricks/iceberg-kafka-connect (error handling patterns)
- Apache Iceberg Kafka Connect Official Docs (error tolerance config)

- [ ] 14. Implement comprehensive error handling
  - [ ] 14.1 Implement parse error handling
    - Wrap parseMessage() in try-catch for JsonProcessingException
    - Log ERROR with topic, partition, offset, and error message
    - Skip message and continue with batch
    - Track parse error count in metrics
    - _Requirements: 1.11, 11.3_
    - _Reference: databricks error handling in writer classes_
  
  - [ ] 14.2 Implement version conflict handling
    - Log WARNING for INSERT conflicts with dedup_key and existing version
    - Log WARNING for DELETE on non-existing key with dedup_key
    - Log INFO for skipped messages with dedup_key, incoming version, existing version
    - Track conflict and skip counts in metrics
    - _Requirements: 11.1, 11.2, 11.5_
  
  - [ ] 14.3 Implement Iceberg operation error handling
    - Wrap Iceberg operations in try-catch for IOException and CommitFailedException
    - Log ERROR with table name and exception stack trace
    - Throw ConnectException to fail task (trigger Kafka Connect retry)
    - _Requirements: 11.3_
  
  - [ ] 14.4 Implement memory error handling
    - Wrap put() in try-catch for OutOfMemoryError
    - Log ERROR with batch size and guidance to reduce max.poll.records
    - Attempt System.gc() before throwing
    - Throw ConnectException to fail task
    - _Requirements: 14.5_
  
  - [ ] 14.5 Implement ErrorMetrics class
    - Track parseErrors, versionConflicts, staleMessages, icebergErrors
    - Use AtomicLong for thread-safe counters
    - Implement logSummary() to log all metrics
    - Call logSummary() at end of each batch
    - _Requirements: 11.5_
  
  - [ ]* 14.6 Write unit tests for error handling
    - Test parse error logs ERROR and continues
    - Test version conflict logs WARNING
    - Test Iceberg error throws ConnectException
    - Test memory error throws ConnectException
    - Verify error metrics incremented correctly
    - _Requirements: 11.1, 11.2, 11.3_

### Phase 11: Build & Deployment Configuration

- [ ] 15. Configure build and packaging
  - [ ] 15.1 Create complete build.gradle file
    - Configure Java 11 source and target compatibility
    - Add all runtime dependencies with correct versions
    - Configure JAR task to include runtime dependencies
    - Set duplicatesStrategy to EXCLUDE
    - Exclude signature files (META-INF/*.SF, *.DSA, *.RSA)
    - Set manifest attributes (Implementation-Title, Implementation-Version)
    - _Requirements: 13.1, 13.2, 13.3, 13.4, 13.5_
  
  - [ ] 15.2 Create connector configuration file
    - Create configs/sink.tram_quan_trac_cdc.json
    - Set connector.class to VersionControlIcebergSinkConnector
    - Set tasks.max to 1
    - Configure Iceberg catalog properties
    - Configure S3/MinIO properties
    - Configure consumer properties (max.poll.records, offset.flush.interval.ms)
    - Configure converters (JsonConverter with schemas disabled)
    - Configure error handling (errors.tolerance, errors.log.enable)
    - _Requirements: 12.1, 12.2, 12.3, 12.4_
  
  - [ ] 15.3 Create Iceberg table creation SQL script
    - Create create-cdc-table.sql
    - Define schema: id, dedup_key, record, ingest_time, length, key, type, version
    - Set format-version=2 for row-level operations
    - Set write.delete.mode and write.update.mode to merge-on-read
    - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5, 7.6, 7.7, 7.8, 8.6_
  
  - [ ]* 15.4 Write build verification tests
    - Test JAR builds successfully without errors
    - Test JAR contains all required dependencies
    - Test JAR manifest has correct attributes
    - _Requirements: 13.4_

### Phase 12: Integration Testing & Performance Validation

**📚 References**: See `implementation-references.md` - Phase 10-13
- databricks/iceberg-kafka-connect (test structure)
- Apache Iceberg Java API (test utilities)

- [ ]* 16. Write end-to-end integration tests
  - [ ]* 16.1 Write complete workflow integration test
    - Use Testcontainers for Kafka, MinIO, Hive Metastore
    - Send INSERT messages and verify records created
    - Send UPDATE messages and verify records updated (not duplicated)
    - Send DELETE messages and verify records removed
    - Verify final record count matches expected
    - _Requirements: 4.1, 5.1, 6.1, 15.6_
    - _Reference: databricks test directory for Testcontainers setup patterns_
  
  - [ ]* 16.2 Write performance integration test
    - Generate 50,000 test records
    - Measure batch processing time
    - Verify processing completes within 60 seconds
    - Verify deduplication completes within 5 seconds
    - Verify Iceberg query completes within 10 seconds
    - _Requirements: 14.1, 14.2, 14.3_
  
  - [ ]* 16.3 Write data consistency integration test
    - Send batch with version conflicts
    - Verify only latest version retained
    - Send duplicate messages with different technical IDs
    - Verify deduplication by business key (not technical ID)
    - Verify atomic transaction (all or nothing)
    - _Requirements: 15.1, 15.2, 15.3, 15.4, 15.5, 15.6, 15.7_

- [ ] 17. Final checkpoint - Complete testing and validation
  - Ensure all property tests pass (20 properties, 100 iterations each)
  - Ensure all unit tests pass
  - Ensure all integration tests pass
  - Verify overall code coverage meets 85% minimum
  - Ask the user if questions arise

### Phase 13: Documentation

- [ ] 18. Create deployment and operational documentation
  - [ ] 18.1 Create README.md with deployment instructions
    - Document build process (./gradlew clean build)
    - Document Docker deployment steps
    - Document connector registration via REST API
    - Document configuration properties reference
    - Document troubleshooting guide
    - _Requirements: 13.4_
  
  - [ ] 18.2 Create operational runbook
    - Document startup checklist
    - Document monitoring metrics and logging
    - Document common problems and solutions
    - Document performance tuning guidelines
    - _Requirements: 9.1, 9.2, 9.3, 9.4, 9.5_

## Notes

- **Tasks marked with `*` are optional** and can be skipped for faster MVP delivery
- **Property-based tests** validate 20 correctness properties with 100 iterations each
- **Unit tests** cover edge cases, error handling, and logging verification
- **Integration tests** validate Iceberg operations and end-to-end workflows
- **Single transaction atomicity** ensures all INSERT/UPDATE/DELETE operations commit together
- **dedup_key format** is "topic:key" where key is the business key field name (e.g., "tram_quan_trac:MaTram"), NOT the value
- **Row-level delete** uses Iceberg equality delete (format-version=2) for efficient updates and deletes
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation and user feedback opportunities

## 📚 Implementation Reference Guide

**IMPORTANT**: Before starting each phase, read the corresponding section in `implementation-references.md`

The reference guide provides:
- **Phase-by-phase mapping** of external resources to tasks
- **Code patterns to study** from databricks, getindata, and Iceberg API
- **What to adopt** vs **what NOT to copy**
- **Quick reference cheat sheet** for fast lookup
- **Implementation tips** for when you get stuck

**Key References by Phase**:
- **Phase 1**: Official Docs (catalog config), getindata (naming conventions)
- **Phase 2**: databricks (message parsing patterns)
- **Phase 3**: getindata (dedup-column), databricks (id-columns)
- **Phase 4**: Iceberg Java API (Catalog, Table)
- **Phase 5**: Iceberg Java API (Table Scan), databricks (query patterns)
- **Phase 6-7**: DeepWiki (upsert mode), databricks (upsert implementation)
- **Phase 8**: Iceberg Java API (Transaction) - **CRITICAL**, DeepWiki (equality delete)
- **Phase 9**: databricks (error handling)
- **Phase 10-13**: databricks (test structure)

**Before Implementation Checklist**:
1. ✅ Read DeepWiki Upsert Mode (15 min) - understand equality delete
2. ✅ Skim databricks README (10 min) - understand config and features
3. ✅ Skim Iceberg Java API overview (10 min) - understand available APIs
4. ✅ Keep `implementation-references.md` open during coding
