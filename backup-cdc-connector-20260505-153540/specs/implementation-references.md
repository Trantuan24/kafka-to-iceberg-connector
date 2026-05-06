# Implementation References Guide

## Overview

This document maps reference sources to specific implementation phases in the CDC Version Control Connector. Use this guide when implementing each phase to understand which external resources are most relevant.

## Reference Sources Summary

### Priority 1 (Must Read)

1. **databricks/iceberg-kafka-connect**
   - URL: https://github.com/databricks/iceberg-kafka-connect
   - Key features: upsert-mode, cdc-field, id-columns, schema evolution
   - Most relevant for: Phases 2-8 (core connector logic)

2. **DeepWiki - Upsert Mode**
   - URL: https://deepwiki.com/databricks/iceberg-kafka-connect/4.3-upsert-mode
   - Key concepts: equality delete + append pattern, format-version=2 requirements
   - Most relevant for: Phase 6 (UPDATE logic), Phase 7 (DELETE logic)

3. **Apache Iceberg Java API**
   - URL: https://iceberg.apache.org/docs/latest/api/
   - Key APIs: newAppend, newDelete, newTransaction, newScan
   - Most relevant for: Phase 5 (query), Phase 8 (write operations)

### Priority 2 (Should Read)

4. **getindata/kafka-connect-iceberg-sink**
   - URL: https://github.com/getindata/kafka-connect-iceberg-sink
   - Key concepts: dedup-column, op-column, upsert config
   - Most relevant for: Phase 1 (config design), Phase 3 (deduplication)

5. **Apache Iceberg Kafka Connect Official Docs**
   - URL: https://iceberg.apache.org/docs/1.10.0/kafka-connect/
   - Key topics: catalog config, S3/Hadoop config, commit coordination
   - Most relevant for: Phase 1 (config), Phase 4 (Iceberg setup)

6. **Apache Iceberg Table Javadoc**
   - URL: https://iceberg.apache.org/javadoc/1.7.0/org/apache/iceberg/Table.html
   - Key methods: API signatures and parameter details
   - Most relevant for: Phase 8 (implementation details)

---

## Phase-by-Phase Reference Mapping

### Phase 1: Project Setup and Configuration

**Primary References**:
- Apache Iceberg Kafka Connect Official Docs (catalog config, S3 config)
- getindata/kafka-connect-iceberg-sink (config naming conventions)

**What to Learn**:
1. **Catalog Configuration Pattern**
   - From Official Docs: Standard Hive catalog config
   - Properties: `iceberg.catalog.type`, `iceberg.catalog.uri`, `iceberg.catalog.warehouse`
   
2. **S3/MinIO Configuration**
   - From Official Docs: S3A filesystem configuration
   - Properties: `s3.endpoint`, `s3.path-style-access`, `s3.access-key-id`, `s3.secret-access-key`

3. **Config Naming Conventions**
   - From getindata: Inspiration for config property names
   - Example: `upsert.dedup-column` → our `version` field
   - Example: `upsert.op-column` → our `type` field

**Key Takeaways**:
- Use standard Iceberg config prefixes (`iceberg.catalog.*`, `iceberg.catalog.s3.*`)
- Follow Kafka Connect config conventions (ConfigDef, validators)
- Keep config minimal - hardcode message format assumptions

**Code to Study**:
```java
// From databricks/iceberg-kafka-connect
// Study: IcebergSinkConfig.java for config definition patterns
// Study: CatalogUtils.java for catalog initialization

// From getindata/kafka-connect-iceberg-sink
// Study: IcebergSinkConnectorConfig.java for dedup/op column config
```

---

### Phase 2: Message Parsing and Transformation

**Primary References**:
- Apache Iceberg Kafka Connect Official Docs (SMT examples - to understand what NOT to do)
- databricks/iceberg-kafka-connect (message handling patterns)

**What to Learn**:
1. **Why NOT to Use SMT**
   - From Official Docs: SMTs are stateless, cannot query Iceberg, cannot handle version control
   - Our approach: Parse and transform internally in SinkTask

2. **Message Parsing Patterns**
   - From databricks: How to extract CDC fields from Kafka records
   - Study: RecordConverter.java or similar

**Key Takeaways**:
- Parse JSON directly in SinkTask.put()
- Extract: data[], key, type, version, ngay_cap_nhat, length
- Transform data[] to JSON string (record field)
- Construct dedup_key = topic + ":" + key (field name)

**Code to Study**:
```java
// From databricks/iceberg-kafka-connect
// Study: RecordConverter.java - how to convert SinkRecord to Iceberg GenericRecord
// Study: FieldConverter.java - type conversions

// Key pattern to adopt:
Map<String, Object> value = (Map<String, Object>) kafkaRecord.value();
String businessKey = (String) value.get("key");
List<Map<String, Object>> dataArray = (List) value.get("data");
String operationType = (String) value.get("type");
long version = castToLong(value.get("version"));
```

---

### Phase 3: Batch Deduplication

**Primary References**:
- getindata/kafka-connect-iceberg-sink (dedup-column concept)
- databricks/iceberg-kafka-connect (id-columns concept)

**What to Learn**:
1. **Deduplication Key Concept**
   - From getindata: `upsert.dedup-column` = field used for ordering (our `version`)
   - From databricks: `id-columns` = fields used for uniqueness (our `dedup_key`)

2. **Max Version Selection**
   - From getindata: Keep record with max timestamp/version
   - Our implementation: HashMap<dedup_key, message>, keep max(version)

**Key Takeaways**:
- Use HashMap for O(n) deduplication
- Key = dedup_key (topic:key field name)
- Value = ParsedMessage with max version
- Message-level dedup means very few unique keys (1-10 typically)

**Code to Study**:
```java
// From getindata/kafka-connect-iceberg-sink
// Study: RecordBuffer.java or similar - how to buffer and deduplicate records

// Key pattern to adopt:
Map<String, ParsedMessage> latestByDedupKey = new HashMap<>();
for (ParsedMessage msg : messages) {
    String dedupKey = msg.getDedupKey();
    ParsedMessage existing = latestByDedupKey.get(dedupKey);
    if (existing == null || msg.getVersion() > existing.getVersion()) {
        latestByDedupKey.put(dedupKey, msg);
    }
}
```

---

### Phase 4: Iceberg Table Initialization

**Primary References**:
- Apache Iceberg Java API (Catalog, Table interfaces)
- Apache Iceberg Kafka Connect Official Docs (catalog setup)

**What to Learn**:
1. **Catalog Initialization**
   - From Java API: HadoopCatalog vs HiveCatalog
   - From Official Docs: Configuration properties for Hive Metastore

2. **Table Loading**
   - From Java API: catalog.loadTable(TableIdentifier)
   - Schema validation, format-version check

**Key Takeaways**:
- Use HiveCatalog for Hive Metastore integration
- Configure S3A filesystem in Hadoop Configuration
- Load table once in start(), reuse in put()
- Validate format-version=2 at startup

**Code to Study**:
```java
// From Apache Iceberg Java API docs
// Study: Catalog interface, HiveCatalog implementation

// Key pattern to adopt:
Configuration conf = new Configuration();
conf.set("hive.metastore.uris", catalogUri);
conf.set("fs.s3a.endpoint", s3Endpoint);
conf.set("fs.s3a.access.key", s3AccessKey);
conf.set("fs.s3a.secret.key", s3SecretKey);
conf.set("fs.s3a.path.style.access", "true");
conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");

Catalog catalog = new HiveCatalog(conf);
Table table = catalog.loadTable(TableIdentifier.of("default", "tram_quan_trac_cdc"));

// Validate format-version
if (table.properties().get("format-version").equals("2")) {
    // OK
} else {
    throw new ConnectException("Table must be format-version=2");
}
```

---

### Phase 5: Query Existing Versions

**Primary References**:
- Apache Iceberg Java API (Table Scan API)
- databricks/iceberg-kafka-connect (query patterns)

**What to Learn**:
1. **Table Scan API**
   - From Java API: IcebergGenerics.read(table).where(filter).select(...)
   - Expression filters: Expressions.in(), Expressions.equal()

2. **Batch Query Pattern**
   - From databricks: How to query multiple keys efficiently
   - Use IN filter with list of dedup_keys

**Key Takeaways**:
- Use IcebergGenerics.read() for simple scans
- Filter: Expressions.in("dedup_key", dedupKeys.toArray())
- Select only needed columns: dedup_key, version
- Build HashMap<dedup_key, version> from results
- Chunk queries if >10k keys (unlikely with message-level dedup)

**Code to Study**:
```java
// From Apache Iceberg Java API docs
// Study: IcebergGenerics.read() examples
// Study: Expressions class for filter building

// Key pattern to adopt:
Expression filter = Expressions.in("dedup_key", dedupKeys.toArray());

Map<String, Long> existingVersions = new HashMap<>();
try (CloseableIterable<Record> records = IcebergGenerics.read(table)
        .where(filter)
        .select("dedup_key", "version")
        .build()) {
    
    for (Record record : records) {
        String dedupKey = record.getField("dedup_key").toString();
        Long version = (Long) record.getField("version");
        existingVersions.put(dedupKey, version);
    }
}
```

---

### Phase 6: UPDATE Logic (Version Control Rules)

**Primary References**:
- DeepWiki - Upsert Mode (equality delete + append pattern)
- databricks/iceberg-kafka-connect (upsert-mode implementation)

**What to Learn**:
1. **Upsert Pattern**
   - From DeepWiki: UPDATE = equality delete old version + append new version
   - From databricks: `upsert-mode-enabled` config and implementation

2. **Version Comparison Rules**
   - UPDATE on existing key with newer version → UPDATE
   - UPDATE on existing key with stale version → SKIP
   - UPDATE on non-existing key → INSERT (upsert behavior)

**Key Takeaways**:
- UPDATE requires two operations: delete + append
- Use equality delete (format-version=2) for efficiency
- Wrap in transaction for atomicity
- Log version conflicts for monitoring

**Code to Study**:
```java
// From databricks/iceberg-kafka-connect
// Study: UpsertWriter.java or similar - how upsert mode works
// Study: How they handle equality deletes

// From DeepWiki documentation
// Study: Explanation of equality delete mechanism
// Study: Why format-version=2 is required

// Key pattern to adopt:
if (operationType.equals("UPDATE")) {
    if (existingVersion == null) {
        // Upsert: treat as INSERT
        toInsert.add(createIcebergRecord(msg));
    } else if (incomingVersion > existingVersion) {
        // UPDATE: will delete old + append new
        toUpdate.add(createIcebergRecord(msg));
    } else {
        // SKIP: stale version
        log.info("UPDATE skipped: stale version");
        skipped++;
    }
}
```

---

### Phase 7: DELETE Logic (Version Control Rules)

**Primary References**:
- DeepWiki - Upsert Mode (equality delete mechanism)
- Apache Iceberg Java API (DeleteFiles API)

**What to Learn**:
1. **Equality Delete**
   - From DeepWiki: Equality delete creates delete files marking rows for deletion
   - More efficient than rewriting data files
   - Requires format-version=2

2. **DELETE Rules**
   - DELETE on existing key with newer version → DELETE
   - DELETE on existing key with stale version → SKIP
   - DELETE on non-existing key → SKIP (log warning)

**Key Takeaways**:
- Use deleteFromRowFilter() with dedup_key filter
- Equality delete creates delete files, not data file rewrites
- Wrap in transaction with other operations
- Log warnings for DELETE on non-existing keys

**Code to Study**:
```java
// From Apache Iceberg Java API docs
// Study: DeleteFiles interface
// Study: deleteFromRowFilter() method

// From DeepWiki documentation
// Study: How equality delete files work
// Study: Performance characteristics

// Key pattern to adopt:
if (operationType.equals("DELETE")) {
    if (existingVersion == null) {
        // SKIP: non-existing key
        log.warn("DELETE on non-existing key: {}", dedupKey);
        skipped++;
    } else if (incomingVersion > existingVersion) {
        // DELETE: will remove record
        toDelete.add(dedupKey);
    } else {
        // SKIP: stale version
        log.info("DELETE skipped: stale version");
        skipped++;
    }
}
```

---

### Phase 8: Batch Write to Iceberg (Transaction)

**Primary References**:
- Apache Iceberg Java API (Transaction API)
- databricks/iceberg-kafka-connect (commit coordination)
- DeepWiki - Upsert Mode (atomic operations)

**What to Learn**:
1. **Transaction API**
   - From Java API: table.newTransaction() for atomic multi-operation commits
   - txn.newDelete(), txn.newAppend(), txn.commitTransaction()

2. **Atomic Batch Pattern**
   - From databricks: How to coordinate multiple operations
   - Single transaction for all INSERT/UPDATE/DELETE

3. **Equality Delete + Append**
   - From DeepWiki: DELETE old versions + APPEND new versions in one transaction
   - Creates single snapshot with both operations

**Key Takeaways**:
- Use Transaction API for atomic batch operations
- Order: DELETE first (UPDATE + DELETE), then APPEND (INSERT + UPDATE)
- Single commitTransaction() for all operations
- If any operation fails, entire transaction rolls back

**Code to Study**:
```java
// From Apache Iceberg Java API docs
// Study: Transaction interface
// Study: Example of multi-operation transaction

// From databricks/iceberg-kafka-connect
// Study: IcebergWriter.java or similar - how they commit batches
// Study: Transaction coordination logic

// Key pattern to adopt:
Transaction txn = table.newTransaction();

try {
    // Step 1: Equality delete for UPDATE + DELETE
    if (!toUpdate.isEmpty() || !toDelete.isEmpty()) {
        Set<String> dedupKeysToDelete = new HashSet<>();
        toUpdate.stream()
            .map(r -> r.getField("dedup_key").toString())
            .forEach(dedupKeysToDelete::add);
        dedupKeysToDelete.addAll(toDelete);
        
        DeleteFiles delete = txn.newDelete();
        delete.deleteFromRowFilter(
            Expressions.in("dedup_key", dedupKeysToDelete.toArray())
        );
        delete.commit();
    }
    
    // Step 2: Append for INSERT + UPDATE
    List<GenericRecord> allToAppend = new ArrayList<>();
    allToAppend.addAll(toInsert);
    allToAppend.addAll(toUpdate);
    
    if (!allToAppend.isEmpty()) {
        DataFile dataFile = writeParquetFile(allToAppend);
        AppendFiles append = txn.newAppend();
        append.appendFile(dataFile);
        append.commit();
    }
    
    // Step 3: Commit transaction atomically
    txn.commitTransaction();
    
} catch (Exception e) {
    log.error("Transaction failed", e);
    throw new ConnectException("Batch write failed", e);
}
```

---

### Phase 9: Error Handling

**Primary References**:
- databricks/iceberg-kafka-connect (error handling patterns)
- Apache Iceberg Kafka Connect Official Docs (error tolerance config)

**What to Learn**:
1. **Error Categories**
   - Parse errors: Skip message, log error
   - Version conflicts: Skip message, log warning
   - Iceberg errors: Fail task, trigger retry

2. **Error Tolerance Config**
   - From Official Docs: `errors.tolerance`, `errors.log.enable`
   - Recommended: `errors.tolerance=none` for fast failure

**Key Takeaways**:
- Non-fatal errors: Skip message, continue batch
- Fatal errors: Throw ConnectException, fail task
- Log all errors with context (topic, partition, offset)
- Track error metrics for monitoring

**Code to Study**:
```java
// From databricks/iceberg-kafka-connect
// Study: Error handling in writer classes
// Study: How they categorize and handle different error types

// Key pattern to adopt:
try {
    ParsedMessage msg = parseMessage(record);
} catch (JsonProcessingException e) {
    log.error("Parse error: topic={}, partition={}, offset={}", 
        record.topic(), record.kafkaPartition(), record.kafkaOffset(), e);
    metrics.incrementParseErrors();
    continue; // Skip message
}

// For Iceberg errors:
try {
    txn.commitTransaction();
} catch (CommitFailedException e) {
    log.error("Commit failed", e);
    throw new ConnectException("Iceberg commit failed", e); // Fail task
}
```

---

### Phase 10-13: Testing

**Primary References**:
- databricks/iceberg-kafka-connect (test structure)
- Apache Iceberg Java API (test utilities)

**What to Learn**:
1. **Property-Based Testing**
   - Test core logic with generated inputs
   - Validate correctness properties

2. **Integration Testing**
   - Use Testcontainers for MinIO, Hive Metastore
   - End-to-end workflow tests

**Key Takeaways**:
- Property tests for parsing, deduplication, version control
- Unit tests for edge cases and error handling
- Integration tests for Iceberg operations
- Performance tests for batch processing

**Code to Study**:
```java
// From databricks/iceberg-kafka-connect
// Study: Test directory structure
// Study: Integration test setup with Testcontainers
// Study: How they test upsert mode

// Key pattern to adopt:
@Test
@Testcontainers
void testUpsertWorkflow() {
    // Setup: MinIO, Hive Metastore containers
    // Send INSERT messages
    // Verify records in Iceberg
    // Send UPDATE messages
    // Verify only latest version exists
    // Send DELETE messages
    // Verify records removed
}
```

---

## Quick Reference Cheat Sheet

| Phase | Primary Reference | Key Concept |
|-------|------------------|-------------|
| 1. Config | Official Docs, getindata | Catalog config, S3 config, naming conventions |
| 2. Parsing | databricks | Message parsing, field extraction |
| 3. Dedup | getindata, databricks | dedup-column, id-columns, max version |
| 4. Iceberg Setup | Java API, Official Docs | Catalog, Table, format-version=2 |
| 5. Query | Java API, databricks | Table Scan, IN filter, batch query |
| 6. UPDATE | DeepWiki, databricks | Upsert pattern, equality delete + append |
| 7. DELETE | DeepWiki, Java API | Equality delete, deleteFromRowFilter |
| 8. Write | Java API, databricks, DeepWiki | Transaction API, atomic commit |
| 9. Errors | databricks, Official Docs | Error categories, tolerance config |
| 10-13. Testing | databricks, Java API | Property tests, integration tests |

---

## Implementation Tips

### When to Read Each Reference

**Before Starting Implementation**:
1. Read DeepWiki Upsert Mode (15 minutes) - understand equality delete
2. Skim databricks README (10 minutes) - understand config and features
3. Skim Apache Iceberg Java API overview (10 minutes) - understand available APIs

**During Implementation**:
- Keep Apache Iceberg Javadoc open for API signatures
- Reference databricks source code for patterns (don't copy, understand)
- Reference getindata for config naming inspiration

**When Stuck**:
- Check databricks issues/discussions for similar problems
- Check Apache Iceberg documentation for API usage examples
- Check DeepWiki for conceptual understanding

### What NOT to Copy

- Don't copy databricks code directly (different message format, different requirements)
- Don't copy getindata code directly (archived, different assumptions)
- Don't copy official connector code (append-only, no version control)

### What to Adopt

- Config naming conventions (iceberg.catalog.*, iceberg.catalog.s3.*)
- Upsert pattern (equality delete + append)
- Transaction pattern (single atomic commit)
- Error handling categories (parse, version conflict, Iceberg)
- Test structure (property tests, integration tests)

---

## Version Compatibility Notes

**Iceberg Version**: 1.7.0 (used in design)
- Check databricks repo for compatible version
- Check official docs for API changes
- Use Javadoc for exact version being used

**Kafka Connect Version**: 3.5.1 (used in design)
- Check databricks repo for compatible version
- Check official docs for API changes

**Hadoop Version**: 3.3.4 (used in design)
- Required for S3A filesystem
- Check compatibility with Iceberg version

---

## Additional Resources

### Community Resources
- Apache Iceberg Slack: #kafka-connect channel
- Databricks Community Forums
- Stack Overflow: [apache-iceberg] tag

### Documentation Links
- Apache Iceberg Docs: https://iceberg.apache.org/docs/latest/
- Kafka Connect Docs: https://kafka.apache.org/documentation/#connect
- Hadoop S3A Docs: https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html

### Example Projects
- databricks/iceberg-kafka-connect: Full-featured connector with upsert
- getindata/kafka-connect-iceberg-sink: Archived but good for concepts
- rapid7/iceberg-kafka-connect: Alternative implementation

---

**Document Version**: 1.0  
**Last Updated**: 2024-01-15  
**Status**: Ready for Use
