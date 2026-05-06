# Design Document: CDC Version Control Connector

## Overview

The CDC Version Control Connector is a custom Kafka Connect Sink Connector that processes Change Data Capture (CDC) messages from Kafka topics and writes them to Apache Iceberg tables with version control logic. The connector implements message-level processing with deduplication, version checking, and batch operations to ensure data consistency and high throughput.

**Design References**:
- **databricks/iceberg-kafka-connect**: Upsert mode, CDC field handling, id-columns pattern
- **DeepWiki Upsert Mode**: Equality delete + append mechanism for UPDATE operations
- **Apache Iceberg Java API**: Transaction API for atomic batch operations
- **getindata/kafka-connect-iceberg-sink**: Dedup-column and op-column concepts

### Key Design Principles

1. **Message-Level Processing**: Parse and transform CDC messages internally without relying on SMT chains
2. **Business Key-Based Version Control**: Use business keys (dedup_key) for deduplication and version comparison, not technical IDs
3. **Batch Optimization**: Process messages in batches with single Iceberg query per batch for performance
4. **Direct Iceberg API**: Use Iceberg Table API and Table Scan API directly, avoiding JDBC overhead
5. **Atomic Operations**: Leverage Iceberg's transactional commit mechanism for data consistency

### Architecture Goals

- **High Throughput**: Process 10k-50k messages per batch with sub-second deduplication
- **Low Latency**: Complete batch processing including Iceberg operations within 30-60 seconds
- **Data Consistency**: Ensure only the latest version of each record exists in the target table
- **Operational Simplicity**: Self-contained connector with minimal external dependencies

## Architecture

### High-Level Component Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Kafka Connect Framework                      │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │         VersionControlIcebergSinkConnector                    │  │
│  │  - Entry point                                                │  │
│  │  - Configuration management                                   │  │
│  │  - Task lifecycle                                             │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                              │                                       │
│                              ▼                                       │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │         VersionControlIcebergSinkTask                         │  │
│  │                                                               │  │
│  │  ┌─────────────────────────────────────────────────────────┐ │  │
│  │  │  Step 1: Message Consumption                            │ │  │
│  │  │  - Receive batch from Kafka (10k-50k records)           │ │  │
│  │  │  - Parse JSON structure                                 │ │  │
│  │  └─────────────────────────────────────────────────────────┘ │  │
│  │                              │                                │  │
│  │                              ▼                                │  │
│  │  ┌─────────────────────────────────────────────────────────┐ │  │
│  │  │  Step 2: Internal Parsing & Transformation              │ │  │
│  │  │  - Extract: key, data[], type, version, ngay_cap_nhat  │ │  │
│  │  │  - Transform: data[] → JSON string (record field)      │ │  │
│  │  │  - Construct: dedup_key = "topic:key"                  │ │  │
│  │  │  - Cast: version/length to BIGINT                      │ │  │
│  │  └─────────────────────────────────────────────────────────┘ │  │
│  │                              │                                │  │
│  │                              ▼                                │  │
│  │  ┌─────────────────────────────────────────────────────────┐ │  │
│  │  │  Step 3: Batch Deduplication                            │ │  │
│  │  │  - HashMap<dedup_key, message>                          │ │  │
│  │  │  - Keep max(version) per dedup_key                      │ │  │
│  │  │  - O(n) time complexity                                 │ │  │
│  │  └─────────────────────────────────────────────────────────┘ │  │
│  │                              │                                │  │
│  │                              ▼                                │  │
│  │  ┌─────────────────────────────────────────────────────────┐ │  │
│  │  │  Step 4: Batch Query Existing Versions                  │ │  │
│  │  │  - Collect all dedup_keys from batch                    │ │  │
│  │  │  - Single Iceberg Table Scan with IN filter             │ │  │
│  │  │  - Build HashMap<dedup_key, existing_version>           │ │  │
│  │  └─────────────────────────────────────────────────────────┘ │  │
│  │                              │                                │  │
│  │                              ▼                                │  │
│  │  ┌─────────────────────────────────────────────────────────┐ │  │
│  │  │  Step 5: Apply I/U/D Rules                              │ │  │
│  │  │  - INSERT: if dedup_key not exists                      │ │  │
│  │  │  - UPDATE: if version > existing_version (or upsert)    │ │  │
│  │  │  - DELETE: if version > existing_version                │ │  │
│  │  │  - Categorize into: toInsert, toUpdate, toDelete        │ │  │
│  │  └─────────────────────────────────────────────────────────┘ │  │
│  │                              │                                │  │
│  │                              ▼                                │  │
│  │  ┌─────────────────────────────────────────────────────────┐ │  │
│  │  │  Step 6: Batch Write to Iceberg                         │ │  │
│  │  │  - Single Iceberg Transaction                           │ │  │
│  │  │  - DELETE: Row-level delete by dedup_key               │ │  │
│  │  │  - APPEND: All INSERT + UPDATE records                  │ │  │
│  │  │  - Atomic commit for entire batch                       │ │  │
│  │  └─────────────────────────────────────────────────────────┘ │  │
│  └───────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      Iceberg Storage Layer                           │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │  Hive Metastore (thrift://hive-metastore:9083)               │  │
│  │  - Table metadata                                            │  │
│  │  - Schema management                                         │  │
│  │  - Transaction coordination                                  │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                              │                                       │
│                              ▼                                       │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │  S3A FileSystem (MinIO: http://minio:9000)                   │  │
│  │  - Data files (Parquet)                                      │  │
│  │  - Metadata files                                            │  │
│  │  - Manifest files                                            │  │
│  └───────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

### Data Flow Diagram

```
Kafka Topic                    Connector                    Iceberg Table
(tram_quan_trac)              Processing                   (tram_quan_trac_cdc)
     │                             │                              │
     │  Batch (10k-50k msgs)       │                              │
     ├────────────────────────────>│                              │
     │                             │                              │
     │                             │ Parse & Transform            │
     │                             │ (internal, no SMT)           │
     │                             │                              │
     │                             │ Deduplicate                  │
     │                             │ (max version per key)        │
     │                             │                              │
     │                             │ Query Existing Versions      │
     │                             ├─────────────────────────────>│
     │                             │<─────────────────────────────┤
     │                             │  (dedup_key, version) pairs  │
     │                             │                              │
     │                             │ Apply I/U/D Rules            │
     │                             │ (version control logic)      │
     │                             │                              │
     │                             │ Batch Write (Single Txn)     │
     │                             ├─────────────────────────────>│
     │                             │  DELETE (row-level)          │
     │                             │  APPEND (INSERT + UPDATE)    │
     │                             │  Atomic commit               │
     │                             │                              │
     │                             │ Commit Offsets               │
     │<────────────────────────────┤                              │
     │                             │                              │
```

## Components and Interfaces

### 1. VersionControlIcebergSinkConnector

**Purpose**: Entry point for the Kafka Connect framework. Manages connector lifecycle and task distribution.

**Responsibilities**:
- Accept and validate configuration properties
- Create and distribute task configurations
- Provide connector metadata (version, config definition)

**Key Methods**:
```java
public class VersionControlIcebergSinkConnector extends SinkConnector {
    
    // Lifecycle methods
    void start(Map<String, String> props)
    void stop()
    
    // Task management
    Class<? extends Task> taskClass()
    List<Map<String, String>> taskConfigs(int maxTasks)
    
    // Metadata
    String version()
    ConfigDef config()
}
```

**Configuration Handling**:
- Validates required properties at startup
- Passes configuration to each task instance
- Supports single-task deployment (tasks.max = 1)

### 2. VersionControlIcebergSinkTask

**Purpose**: Core processing logic. Handles message consumption, transformation, deduplication, version control, and Iceberg operations.

**Responsibilities**:
- Consume and parse CDC messages from Kafka
- Perform internal transformation (no SMT dependency)
- Deduplicate messages within each batch
- Query Iceberg for existing record versions
- Apply INSERT/UPDATE/DELETE rules with version control
- Execute batch writes to Iceberg using Table API

**Key Methods**:
```java
public class VersionControlIcebergSinkTask extends SinkTask {
    
    // Lifecycle
    void start(Map<String, String> props)
    void stop()
    String version()
    
    // Main processing
    void put(Collection<SinkRecord> records)
    
    // Internal processing steps
    private Map<String, ParsedMessage> parseAndTransform(Collection<SinkRecord> records)
    private Map<String, ParsedMessage> deduplicateByDedupKey(Map<String, ParsedMessage> messages)
    private Map<String, Long> queryExistingVersions(Set<String> dedupKeys)
    private void applyVersionControlRules(
        Map<String, ParsedMessage> messages,
        Map<String, Long> existingVersions,
        List<GenericRecord> toInsert,
        List<GenericRecord> toUpdate,
        List<String> toDelete
    )
    private void batchWriteToIceberg(
        List<GenericRecord> toInsert,
        List<GenericRecord> toUpdate,
        List<String> toDelete
    )
    
    // Iceberg operations
    private DataFile writeParquetFile(List<GenericRecord> records)
    
    // Helper methods
    private Catalog createHiveCatalog()
    private GenericRecord createIcebergRecord(ParsedMessage msg)
    private String constructDedupKey(String topic, String businessKey)
}
```

**Internal Data Structures**:
```java
// Parsed message representation
class ParsedMessage {
    String technicalId;      // UUID or topic-partition-offset
    String dedupKey;         // "topic:businessKey"
    String recordJson;       // JSON string of data[]
    String ingestTime;       // ngay_cap_nhat
    long length;             // cast to BIGINT
    String businessKey;      // key field value (e.g., "MaTram")
    String operationType;    // INSERT, UPDATE, DELETE
    long version;            // cast to BIGINT
}

// Processing result tracking
class ProcessingMetrics {
    int totalReceived;
    int afterDedup;
    int toInsert;
    int toUpdate;
    int toDelete;
    int skipped;
    int errors;
    long processingTimeMs;
}
```

### 3. VersionControlIcebergSinkConfig

**Purpose**: Configuration management and validation.

**Responsibilities**:
- Define configuration schema using Kafka Connect ConfigDef
- Validate configuration values
- Provide typed accessors for configuration properties

**Configuration Properties**:
```java
public class VersionControlIcebergSinkConfig extends AbstractConfig {
    
    // Table configuration
    public static final String TABLE_NAME_CONFIG = "iceberg.table.name";
    
    // Catalog configuration
    public static final String CATALOG_TYPE_CONFIG = "iceberg.catalog.type";
    public static final String CATALOG_URI_CONFIG = "iceberg.catalog.uri";
    public static final String CATALOG_WAREHOUSE_CONFIG = "iceberg.catalog.warehouse";
    
    // S3/MinIO configuration
    public static final String S3_ENDPOINT_CONFIG = "iceberg.catalog.s3.endpoint";
    public static final String S3_PATH_STYLE_ACCESS_CONFIG = "iceberg.catalog.s3.path-style-access";
    public static final String S3_ACCESS_KEY_CONFIG = "iceberg.catalog.s3.access-key-id";
    public static final String S3_SECRET_KEY_CONFIG = "iceberg.catalog.s3.secret-access-key";
    public static final String S3_REGION_CONFIG = "iceberg.catalog.s3.region";
    
    // Processing configuration
    public static final String MAX_POLL_RECORDS_CONFIG = "consumer.max.poll.records";
    public static final String FLUSH_INTERVAL_CONFIG = "offset.flush.interval.ms";
    
    // CDC-specific configuration (inspired by databricks/iceberg-kafka-connect and getindata)
    // Reference: https://github.com/databricks/iceberg-kafka-connect
    // Reference: https://github.com/getindata/kafka-connect-iceberg-sink
    //
    // These configs map to our internal logic:
    // - dedup_key field = id-columns (databricks) = primary key for version control
    // - version field = dedup-column (getindata) = timestamp for ordering
    // - type field = op-column (getindata) = CDC operation type (INSERT/UPDATE/DELETE)
    //
    // Note: We don't expose these as config because our message format is fixed,
    // but the concepts are the same as existing Iceberg CDC connectors.
    
    // Accessors
    public String getTableName()
    public String getCatalogType()
    public String getCatalogUri()
    public String getCatalogWarehouse()
    public String getS3Endpoint()
    public boolean getS3PathStyleAccess()
    public String getS3AccessKey()
    public String getS3SecretKey()
    public String getS3Region()
    public int getMaxPollRecords()
    public long getFlushInterval()
}
```

### 4. Iceberg Integration Layer

**Purpose**: Encapsulate Iceberg API interactions.

**Key Components**:

**Catalog Management**:
```java
// Initialize Hive Catalog
private Catalog createHiveCatalog() {
    Configuration conf = new Configuration();
    
    // Hive Metastore
    conf.set("hive.metastore.uris", config.getCatalogUri());
    
    // S3A FileSystem
    conf.set("fs.s3a.endpoint", config.getS3Endpoint());
    conf.set("fs.s3a.access.key", config.getS3AccessKey());
    conf.set("fs.s3a.secret.key", config.getS3SecretKey());
    conf.set("fs.s3a.path.style.access", String.valueOf(config.getS3PathStyleAccess()));
    conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    
    return new HadoopCatalog(conf, config.getCatalogWarehouse());
}
```

**Table Scan API**:
```java
// Query existing versions
private Map<String, Long> queryExistingVersions(Set<String> dedupKeys) {
    if (dedupKeys.isEmpty()) {
        return Collections.emptyMap();
    }
    
    log.debug("Querying Iceberg for {} unique dedup_keys", dedupKeys.size());
    
    // Chunk queries if too many keys (unlikely with message-level dedup)
    int chunkSize = 10000;
    Map<String, Long> result = new HashMap<>();
    
    List<String> dedupKeyList = new ArrayList<>(dedupKeys);
    for (int i = 0; i < dedupKeyList.size(); i += chunkSize) {
        int end = Math.min(i + chunkSize, dedupKeyList.size());
        List<String> chunk = dedupKeyList.subList(i, end);
        
        Expression filter = Expressions.in("dedup_key", chunk.toArray());
        
        try (CloseableIterable<Record> records = IcebergGenerics.read(table)
                .where(filter)
                .select("dedup_key", "version")
                .build()) {
            
            for (Record record : records) {
                String dedupKey = record.getField("dedup_key").toString();
                Long version = (Long) record.getField("version");
                result.put(dedupKey, version);
            }
        }
    }
    
    return result;
}
```

**Query Performance Note**:

With message-level deduplication (dedup_key = topic:key), the number of unique dedup_keys is typically **very small** (1-10) even for large batches (50k messages), because many messages share the same topic and key field name.

**Example**:
- 50,000 messages from topic "tram_quan_trac" with key="MaTram"
- After deduplication: **1 unique dedup_key** = "tram_quan_trac:MaTram"
- Query: IN filter with only 1 key (very fast, <1 second)

**Why so few unique keys?**
- `dedup_key` = topic + ":" + key (field name)
- Example: "tram_quan_trac:MaTram" (NOT "tram_quan_trac:TQ001")
- All messages from the same topic with the same key field name share the same dedup_key
- Only the version differs, so deduplication keeps the latest version

**Query chunking** (10k keys per query) is a safety measure for edge cases where multiple topics or key field names are processed in the same batch.

**Write Operations**:
```java
// INSERT: Append API
private void appendRecords(List<GenericRecord> records) {
    DataFile dataFile = writeParquetFile(records);
    
    AppendFiles append = table.newAppend();
    append.appendFile(dataFile);
    append.commit();
}

// UPDATE: Equality delete + append (upsert pattern from databricks/iceberg-kafka-connect)
// Reference: https://github.com/databricks/iceberg-kafka-connect
// Reference: https://deepwiki.com/databricks/iceberg-kafka-connect/4.3-upsert-mode
//
// Upsert mode works by performing equality delete before each append to replace
// old records with the same primary key (dedup_key in our case).
//
// Requirements:
// - Table must be Iceberg format-version=2
// - Table must have identity fields (dedup_key serves as primary key)
//
// Implementation uses Iceberg's row-level delete capabilities:
// - Equality delete: More efficient, creates delete files (format-version=2)
// - File rewrite: Fallback for older format versions
private void updateRecords(List<GenericRecord> records) {
    Set<String> dedupKeys = records.stream()
        .map(r -> r.getField("dedup_key").toString())
        .collect(Collectors.toSet());
    
    // Row-level delete by dedup_key using equality delete
    // This creates a delete file that marks rows for deletion without rewriting data files
    DeleteFiles delete = table.newDelete();
    delete.deleteFromRowFilter(Expressions.in("dedup_key", dedupKeys.toArray()));
    delete.commit();
    
    // Append new records
    appendRecords(records);
}

// DELETE: Row-level delete by dedup_key using equality delete
// Reference: Apache Iceberg format-version=2 equality delete
//
// Equality delete creates a delete file containing the values of identity fields
// (dedup_key) to mark rows for deletion. This is more efficient than rewriting
// entire data files.
private void deleteRecords(List<String> dedupKeys) {
    DeleteFiles delete = table.newDelete();
    delete.deleteFromRowFilter(Expressions.in("dedup_key", dedupKeys.toArray()));
    delete.commit();
}

// Batch write with single Iceberg transaction (ATOMIC)
// Reference: Apache Iceberg Java API - Transaction
// https://iceberg.apache.org/docs/latest/api/#transactions
//
// Transaction API allows grouping multiple operations (delete + append) into a single
// atomic commit. If any operation fails, the entire transaction is rolled back.
//
// This ensures:
// - No partial writes (all INSERT/UPDATE/DELETE succeed or all fail)
// - Consistent snapshot view for readers
// - Single metadata update instead of multiple commits
private void batchWriteToIceberg(
    List<GenericRecord> toInsert,
    List<GenericRecord> toUpdate,
    List<String> toDelete
) {
    // Create transaction for atomic batch operations
    // Reference: table.newTransaction() from Iceberg Java API
    Transaction txn = table.newTransaction();
    
    try {
        // Step 1: Collect all dedup_keys to delete (UPDATE + DELETE)
        // UPDATE requires deleting old version before appending new version (upsert pattern)
        // DELETE requires removing the record entirely
        if (!toUpdate.isEmpty() || !toDelete.isEmpty()) {
            Set<String> dedupKeysToDelete = new HashSet<>();
            
            // Add dedup_keys from UPDATE (old versions to be replaced)
            toUpdate.stream()
                .map(r -> r.getField("dedup_key").toString())
                .forEach(dedupKeysToDelete::add);
            
            // Add dedup_keys from DELETE (records to be removed)
            dedupKeysToDelete.addAll(toDelete);
            
            // Single equality delete operation for all dedup_keys
            // This creates a delete file (format-version=2) marking rows for deletion
            DeleteFiles delete = txn.newDelete();
            delete.deleteFromRowFilter(
                Expressions.in("dedup_key", dedupKeysToDelete.toArray())
            );
            delete.commit();
        }
        
        // Step 2: Collect all records to append (INSERT + UPDATE)
        // INSERT: New records
        // UPDATE: New versions after old versions were deleted above
        List<GenericRecord> allToAppend = new ArrayList<>();
        allToAppend.addAll(toInsert);
        allToAppend.addAll(toUpdate);
        
        if (!allToAppend.isEmpty()) {
            DataFile dataFile = writeParquetFile(allToAppend);
            AppendFiles append = txn.newAppend();
            append.appendFile(dataFile);
            append.commit();
        }
        
        // Step 3: Commit entire transaction atomically
        // Reference: txn.commitTransaction() from Iceberg Java API
        // This creates a single snapshot containing both delete and append operations
        txn.commitTransaction();
        
        log.info("Batch transaction committed: {} inserts, {} updates, {} deletes",
            toInsert.size(), toUpdate.size(), toDelete.size());
        
    } catch (Exception e) {
        log.error("Failed to commit batch transaction", e);
        throw new ConnectException("Batch write failed", e);
    }
}

// Write Parquet file
private DataFile writeParquetFile(List<GenericRecord> records) throws IOException {
    String filename = UUID.randomUUID().toString();
    OutputFile outputFile = table.io().newOutputFile(
        table.location() + "/data/" + filename + ".parquet");
    
    DataWriter<GenericRecord> writer = Parquet.writeData(outputFile)
        .schema(schema)
        .createWriterFunc(GenericParquetWriter::buildWriter)
        .overwrite()
        .build();
    
    try {
        for (GenericRecord record : records) {
            writer.write(record);
        }
    } finally {
        writer.close();
    }
    
    return writer.toDataFile();
}
```

**Implementation Strategy**:
- **Equality delete** (format-version=2): Preferred method for row-level deletes
  - Creates delete files containing identity field values (dedup_key)
  - More efficient than rewriting data files
  - Supported by Iceberg format-version=2
  - Reference: DeepWiki Upsert Mode documentation
- **Upsert pattern**: DELETE old version + APPEND new version
  - Inspired by databricks/iceberg-kafka-connect upsert mode
  - Ensures only latest version exists in table
  - Atomic when wrapped in transaction
- **Single transaction**: All INSERT, UPDATE, and DELETE operations committed atomically
  - Uses Iceberg Transaction API (table.newTransaction())
  - Guarantees all-or-nothing semantics
  - Single snapshot for consistent reader view
  - Reference: Apache Iceberg Java API documentation
- **Atomicity guarantee**: If any operation fails, entire batch is rolled back
  - No partial writes
  - No inconsistent state
  - Kafka offsets not committed on failure (automatic retry)

## Data Models

### Input Message Schema (Kafka CDC Message)

```json
{
  "data": [
    {
      "MaTram": "TQ001",
      "TenTram": "Tram Quan Trac 1",
      "ViDo": "10.762622",
      "KinhDo": "106.660172"
    }
  ],
  "key": "MaTram",
  "type": "INSERT",
  "version": 1704067200000,
  "ngay_cap_nhat": "2024-01-01T00:00:00Z",
  "length": 1
}
```

**Field Descriptions**:
- `data` (array): Array of business objects to be processed
- `key` (string): Business key field name (e.g., "MaTram")
- `type` (string): Operation type - "INSERT", "UPDATE", or "DELETE"
- `version` (number/string): Timestamp for version control (cast to BIGINT)
- `ngay_cap_nhat` (string): Ingestion timestamp
- `length` (number/string): Number of items in data array (cast to BIGINT)

### Target Table Schema (Iceberg)

```sql
CREATE TABLE tram_quan_trac_cdc (
  id          STRING,
  dedup_key   STRING,
  record      STRING,
  ingest_time STRING,
  length      BIGINT,
  key         STRING,
  type        STRING,
  version     BIGINT
)
USING iceberg
TBLPROPERTIES (
  'format-version' = '2',
  'write.delete.mode' = 'merge-on-read',
  'write.update.mode' = 'merge-on-read',
  'write.merge.mode' = 'merge-on-read'
)
```

**Field Descriptions**:
- `id` (STRING): Technical identifier (UUID or topic-partition-offset) - NOT used for version control
- `dedup_key` (STRING): Composite business key in format "topic:key" where key is the business key field name (e.g., "tram_quan_trac:MaTram") - used for deduplication and version control
- `record` (STRING): JSON string representation of the entire data array from CDC message
- `ingest_time` (STRING): Mapped from ngay_cap_nhat field
- `length` (BIGINT): Number of items in original data array
- `key` (STRING): Business key field name (e.g., "MaTram")
- `type` (STRING): Operation type (INSERT, UPDATE, DELETE)
- `version` (BIGINT): Version timestamp for ordering and conflict resolution

**Design Rationale**:
- `dedup_key` is the primary key for version control (not `id`)
  - Concept similar to `id-columns` in databricks/iceberg-kafka-connect
  - Concept similar to primary key in getindata/kafka-connect-iceberg-sink
- `dedup_key` format is "topic:key" where key is the field name (e.g., "MaTram"), NOT the value (e.g., "TQ001")
- `version` field serves as dedup-column (getindata concept) for ordering records
- `type` field serves as op-column (getindata concept) for CDC operation type
- `record` stores the entire data array as JSON to preserve all business data
- `version` as BIGINT enables efficient numeric comparison
- **Format-version=2 is REQUIRED** for equality delete support
  - Reference: DeepWiki Upsert Mode documentation
  - Enables row-level deletes without rewriting data files
  - More efficient than format-version=1 which requires file rewrites
- Partitioning removed for simplicity (can be added later if needed with a DATE field)

### Internal Data Model (ParsedMessage)

```java
public class ParsedMessage {
    private String technicalId;      // Generated UUID or topic-partition-offset
    private String dedupKey;         // Constructed as "topic:businessKey"
    private String recordJson;       // JSON serialization of data[]
    private String ingestTime;       // From ngay_cap_nhat
    private long length;             // Cast from string/number to long
    private String businessKey;      // Value of key field (e.g., "MaTram")
    private String operationType;    // INSERT, UPDATE, or DELETE
    private long version;            // Cast from string/number to long
    
    // Constructor
    public ParsedMessage(SinkRecord kafkaRecord) {
        Map<String, Object> value = (Map<String, Object>) kafkaRecord.value();
        
        // Extract fields
        this.businessKey = (String) value.get("key");
        List<Map<String, Object>> dataArray = (List) value.get("data");
        this.operationType = (String) value.get("type");
        this.ingestTime = (String) value.get("ngay_cap_nhat");
        
        // Cast version
        Object versionObj = value.get("version");
        this.version = castToLong(versionObj);
        
        // Cast length
        Object lengthObj = value.get("length");
        this.length = castToLong(lengthObj);
        
        // Transform data[] to JSON string
        this.recordJson = serializeToJson(dataArray);
        
        // Construct dedup_key = topic:key (field name, NOT value)
        // Example: "tram_quan_trac:MaTram" (NOT "tram_quan_trac:TQ001")
        this.dedupKey = kafkaRecord.topic() + ":" + this.businessKey;
        
        // Generate technical ID
        this.technicalId = generateTechnicalId(kafkaRecord);
    }
    
    private long castToLong(Object value) {
        if (value instanceof Number) {
            return ((Number) value).longValue();
        } else if (value instanceof String) {
            return Long.parseLong((String) value);
        }
        throw new IllegalArgumentException("Cannot cast to long: " + value);
    }
    
    private String serializeToJson(List<Map<String, Object>> dataArray) {
        try {
            return objectMapper.writeValueAsString(dataArray);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize data array", e);
        }
    }
    
    private String generateTechnicalId(SinkRecord record) {
        // Option 1: UUID
        return UUID.randomUUID().toString();
        
        // Option 2: topic-partition-offset
        // return String.format("%s-%d-%d", 
        //     record.topic(), record.kafkaPartition(), record.kafkaOffset());
    }
}
```

### Version Control State Model

```java
public class VersionControlState {
    private Map<String, Long> existingVersions;  // dedup_key -> version
    private Map<String, ParsedMessage> incomingMessages;  // dedup_key -> message
    
    public enum Action {
        INSERT,   // New record, no existing version
        UPDATE,   // Existing record, incoming version > existing version
        DELETE,   // Existing record, incoming version > existing version
        SKIP,     // Incoming version <= existing version
        ERROR     // Conflict (e.g., INSERT on existing key)
    }
    
    public Action determineAction(ParsedMessage msg) {
        String dedupKey = msg.getDedupKey();
        Long existingVersion = existingVersions.get(dedupKey);
        String type = msg.getOperationType();
        long incomingVersion = msg.getVersion();
        
        switch (type) {
            case "INSERT":
                if (existingVersion == null) {
                    return Action.INSERT;
                } else {
                    log.warn("INSERT conflict: dedup_key={} exists with version={}", 
                        dedupKey, existingVersion);
                    return Action.ERROR;
                }
                
            case "UPDATE":
                if (existingVersion == null) {
                    log.info("UPDATE on non-existing key, treating as INSERT: dedup_key={}", 
                        dedupKey);
                    return Action.INSERT;
                } else if (incomingVersion > existingVersion) {
                    return Action.UPDATE;
                } else {
                    log.info("UPDATE skipped: dedup_key={}, incoming version {} <= existing {}", 
                        dedupKey, incomingVersion, existingVersion);
                    return Action.SKIP;
                }
                
            case "DELETE":
                if (existingVersion == null) {
                    log.warn("DELETE on non-existing key: dedup_key={}", dedupKey);
                    return Action.SKIP;
                } else if (incomingVersion > existingVersion) {
                    return Action.DELETE;
                } else {
                    log.info("DELETE skipped: dedup_key={}, incoming version {} <= existing {}", 
                        dedupKey, incomingVersion, existingVersion);
                    return Action.SKIP;
                }
                
            default:
                log.error("Unknown operation type: {}", type);
                return Action.ERROR;
        }
    }
}
```


## Correctness Properties

*A property is a characteristic or behavior that should hold true across all valid executions of a system—essentially, a formal statement about what the system should do. Properties serve as the bridge between human-readable specifications and machine-verifiable correctness guarantees.*

### Property 1: Message Field Extraction Completeness

*For any* valid CDC message containing the required fields (data, key, type, version, ngay_cap_nhat, length), parsing SHALL successfully extract all fields with correct values.

**Validates: Requirements 1.2, 1.3, 1.5**

### Property 2: Data Array Serialization Round-Trip

*For any* data array from a CDC message, serializing to JSON string and then deserializing SHALL preserve the original structure and values.

**Validates: Requirements 1.4**

### Property 3: Dedup Key Construction

*For any* topic name and business key field name, the constructed dedup_key SHALL equal the concatenation of topic, colon, and business key field name (format: "topic:key" where key is the field name, NOT the value).

**Example**: For topic "tram_quan_trac" and key field name "MaTram", dedup_key = "tram_quan_trac:MaTram" (NOT "tram_quan_trac:TQ001")

**Validates: Requirements 1.6**

### Property 4: Numeric Type Casting

*For any* valid numeric value (either as Number type or numeric String), casting to BIGINT SHALL produce the correct long integer value.

**Validates: Requirements 1.7, 1.8**

### Property 5: Field Mapping Preservation

*For any* CDC message with ngay_cap_nhat field, the parsed message's ingest_time field SHALL equal the ngay_cap_nhat value.

**Validates: Requirements 1.9**

### Property 6: Deduplication by Maximum Version

*For any* batch of messages where multiple messages share the same dedup_key, deduplication SHALL retain only the message with the maximum version value for that dedup_key.

**Validates: Requirements 2.1, 2.6**

### Property 7: Unique Message Preservation

*For any* batch of messages where all dedup_keys are unique, deduplication SHALL preserve all messages without removing any.

**Validates: Requirements 2.5**

### Property 8: Deduplication by Business Key

*For any* batch of messages with identical dedup_keys but different technical IDs, deduplication SHALL treat them as duplicates and retain only the one with maximum version.

**Validates: Requirements 2.3**

### Property 9: Query Filter Completeness

*For any* batch of deduplicated messages, the Iceberg query filter SHALL include all unique dedup_key values from the batch.

**Validates: Requirements 3.3, 3.4**

### Property 10: Version Map Construction

*For any* set of Iceberg scan results containing dedup_key and version fields, the constructed HashMap SHALL correctly map each dedup_key to its corresponding version value.

**Validates: Requirements 3.5**

### Property 11: INSERT on Non-Existing Key

*For any* INSERT message where the dedup_key does not exist in the existing versions map, the message SHALL be categorized for insertion.

**Validates: Requirements 4.1**

### Property 12: INSERT Conflict Detection

*For any* INSERT message where the dedup_key already exists in the existing versions map, the message SHALL be categorized as an error and skipped.

**Validates: Requirements 4.2**

### Property 13: Inserted Record Completeness

*For any* message categorized for insertion, the created Iceberg record SHALL contain all required fields: id, dedup_key, record, ingest_time, length, key, type, and version with correct data types (version and length as BIGINT).

**Validates: Requirements 4.4, 4.5, 4.6, 7.1-7.9**

### Property 14: UPDATE with Newer Version

*For any* UPDATE message where the dedup_key exists in the existing versions map and the incoming version is greater than the existing version, the message SHALL be categorized for update.

**Validates: Requirements 5.1**

### Property 15: UPDATE as Upsert

*For any* UPDATE message where the dedup_key does not exist in the existing versions map, the message SHALL be categorized for insertion (upsert behavior).

**Validates: Requirements 5.2**

### Property 16: UPDATE with Stale Version

*For any* UPDATE message where the dedup_key exists in the existing versions map and the incoming version is less than or equal to the existing version, the message SHALL be skipped.

**Validates: Requirements 5.3**

### Property 17: DELETE with Newer Version

*For any* DELETE message where the dedup_key exists in the existing versions map and the incoming version is greater than the existing version, the message SHALL be categorized for deletion.

**Validates: Requirements 6.1, 6.7**

### Property 18: DELETE with Stale Version

*For any* DELETE message where the dedup_key exists in the existing versions map and the incoming version is less than or equal to the existing version, the message SHALL be skipped.

**Validates: Requirements 6.2**

### Property 19: DELETE on Non-Existing Key

*For any* DELETE message where the dedup_key does not exist in the existing versions map, the message SHALL be skipped.

**Validates: Requirements 6.3**

### Property 20: Version Control Idempotence

*For any* batch of messages processed twice with the same existing versions state, the categorization results (toInsert, toUpdate, toDelete, skipped) SHALL be identical.

**Validates: Requirements 15.1-15.6**

## Error Handling

### Error Categories

The connector implements a layered error handling strategy with different responses based on error severity and recoverability:

#### 1. Parse Errors (Non-Fatal, Skip Message)

**Scenarios**:
- Invalid JSON structure in message value
- Missing required fields (key, type, version)
- Invalid data types (non-numeric version, non-array data)

**Handling**:
```java
try {
    ParsedMessage msg = parseMessage(record);
} catch (JsonProcessingException e) {
    log.error("Failed to parse message from topic={}, partition={}, offset={}: {}", 
        record.topic(), record.kafkaPartition(), record.kafkaOffset(), e.getMessage());
    metrics.incrementParseErrors();
    continue; // Skip this message, continue with batch
}
```

**Rationale**: Individual message parse errors should not fail the entire batch. Log the error for monitoring and skip the problematic message.

#### 2. Version Conflicts (Non-Fatal, Skip Message)

**Scenarios**:
- INSERT on existing dedup_key
- DELETE on non-existing dedup_key
- UPDATE/DELETE with stale version (version <= existing)

**Handling**:
```java
switch (action) {
    case ERROR:
        log.warn("Version conflict: type={}, dedup_key={}, incoming_version={}, existing_version={}", 
            msg.getOperationType(), msg.getDedupKey(), msg.getVersion(), existingVersion);
        metrics.incrementVersionConflicts();
        skipped++;
        break;
    case SKIP:
        log.info("Skipping stale message: type={}, dedup_key={}, incoming_version={}, existing_version={}", 
            msg.getOperationType(), msg.getDedupKey(), msg.getVersion(), existingVersion);
        metrics.incrementStaleMessages();
        skipped++;
        break;
}
```

**Rationale**: Version conflicts are expected in CDC scenarios due to message reordering or reprocessing. Log at appropriate level (WARN for conflicts, INFO for stale) and skip the message.

#### 3. Iceberg Operation Errors (Fatal, Fail Task)

**Scenarios**:
- Failed to connect to Hive Metastore
- Failed to write Parquet file
- Failed to commit transaction
- S3/MinIO connection errors

**Handling**:
```java
try {
    appendRecords(toInsert);
    updateRecords(toUpdate);
    deleteRecords(toDelete);
} catch (IOException e) {
    log.error("Failed to write to Iceberg table: {}", config.getTableName(), e);
    throw new ConnectException("Iceberg write operation failed", e);
} catch (CommitFailedException e) {
    log.error("Failed to commit Iceberg transaction: {}", config.getTableName(), e);
    throw new ConnectException("Iceberg commit failed", e);
}
```

**Rationale**: Iceberg operation failures indicate infrastructure problems that cannot be resolved by skipping messages. Fail the task to trigger Kafka Connect's retry mechanism and alert operators.

#### 4. Memory Errors (Fatal, Fail Task)

**Scenarios**:
- OutOfMemoryError during batch processing
- Heap exhaustion from large batches

**Handling**:
```java
try {
    put(records);
} catch (OutOfMemoryError e) {
    log.error("Out of memory processing batch of {} records. Consider reducing consumer.max.poll.records", 
        records.size(), e);
    // Force GC attempt
    System.gc();
    throw new ConnectException("Out of memory", e);
}
```

**Rationale**: Memory errors require task restart and potentially configuration adjustment. Log guidance for operators and fail the task.

### Error Metrics and Monitoring

The connector tracks error metrics for observability:

```java
public class ErrorMetrics {
    private AtomicLong parseErrors = new AtomicLong(0);
    private AtomicLong versionConflicts = new AtomicLong(0);
    private AtomicLong staleMessages = new AtomicLong(0);
    private AtomicLong icebergErrors = new AtomicLong(0);
    
    public void logSummary() {
        log.info("Error summary: parse_errors={}, version_conflicts={}, stale_messages={}, iceberg_errors={}", 
            parseErrors.get(), versionConflicts.get(), staleMessages.get(), icebergErrors.get());
    }
}
```

### Retry Strategy

**Kafka Connect Framework Retries**:
- Task failures trigger automatic restart with exponential backoff
- Configure `errors.tolerance=none` to fail fast on errors
- Configure `errors.tolerance=all` with DLQ for lenient error handling

**Application-Level Retries**:
- No retries for parse errors or version conflicts (skip immediately)
- No retries for Iceberg errors (fail task, let framework retry)

**Recommended Configuration**:
```json
{
  "errors.tolerance": "none",
  "errors.log.enable": "true",
  "errors.log.include.messages": "true"
}
```

This configuration ensures:
- Fast failure on infrastructure errors
- Detailed error logging for debugging
- No silent data loss

## Testing Strategy

### Testing Approach

The CDC Version Control Connector requires a dual testing approach combining property-based testing for core logic and integration testing for infrastructure components.

### Property-Based Testing

**Scope**: Core business logic that operates on data structures without external dependencies.

**Test Framework**: Use **fast-check** (JavaScript/TypeScript) or **QuickCheck** (Haskell) or **Hypothesis** (Python) or **junit-quickcheck** (Java) depending on implementation language.

**Configuration**:
- Minimum 100 iterations per property test
- Each test tagged with: `Feature: cdc-version-control-connector, Property {number}: {property_text}`

**Property Test Categories**:

#### 1. Message Parsing Properties

Test that parsing logic correctly handles all valid message structures:

```java
@Property
@Tag("Feature: cdc-version-control-connector, Property 1: Message Field Extraction Completeness")
void testMessageFieldExtraction(@ForAll("validCDCMessages") Map<String, Object> message) {
    ParsedMessage parsed = parser.parse(message);
    
    assertThat(parsed.getBusinessKey()).isEqualTo(message.get("key"));
    assertThat(parsed.getOperationType()).isEqualTo(message.get("type"));
    assertThat(parsed.getVersion()).isEqualTo(castToLong(message.get("version")));
    assertThat(parsed.getIngestTime()).isEqualTo(message.get("ngay_cap_nhat"));
    assertThat(parsed.getLength()).isEqualTo(castToLong(message.get("length")));
}

@Provide
Arbitrary<Map<String, Object>> validCDCMessages() {
    return Combinators.combine(
        Arbitraries.strings().alpha().ofMinLength(1),  // key
        Arbitraries.of("INSERT", "UPDATE", "DELETE"),  // type
        Arbitraries.longs().greaterOrEqual(0),         // version
        Arbitraries.strings().numeric(),               // ngay_cap_nhat
        Arbitraries.integers().between(1, 100),        // length
        Arbitraries.list(Arbitraries.maps(...))        // data array
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
```

#### 2. Deduplication Properties

Test that deduplication correctly selects maximum version:

```java
@Property
@Tag("Feature: cdc-version-control-connector, Property 6: Deduplication by Maximum Version")
void testDeduplicationSelectsMaxVersion(
    @ForAll("messagesWithDuplicateKeys") List<ParsedMessage> messages
) {
    Map<String, ParsedMessage> deduplicated = deduplicator.deduplicate(messages);
    
    // Group original messages by dedup_key
    Map<String, List<ParsedMessage>> grouped = messages.stream()
        .collect(Collectors.groupingBy(ParsedMessage::getDedupKey));
    
    // For each dedup_key, verify we kept the max version
    for (Map.Entry<String, List<ParsedMessage>> entry : grouped.entrySet()) {
        String dedupKey = entry.getKey();
        long maxVersion = entry.getValue().stream()
            .mapToLong(ParsedMessage::getVersion)
            .max()
            .orElseThrow();
        
        ParsedMessage kept = deduplicated.get(dedupKey);
        assertThat(kept.getVersion()).isEqualTo(maxVersion);
    }
}
```

#### 3. Version Control Properties

Test that INSERT/UPDATE/DELETE rules are correctly applied:

```java
@Property
@Tag("Feature: cdc-version-control-connector, Property 11: INSERT on Non-Existing Key")
void testInsertOnNonExistingKey(
    @ForAll("insertMessages") ParsedMessage msg,
    @ForAll("existingVersionsWithoutKey") Map<String, Long> existingVersions
) {
    // Ensure msg.dedupKey is not in existingVersions
    Assume.that(!existingVersions.containsKey(msg.getDedupKey()));
    
    VersionControlState state = new VersionControlState(existingVersions);
    Action action = state.determineAction(msg);
    
    assertThat(action).isEqualTo(Action.INSERT);
}

@Property
@Tag("Feature: cdc-version-control-connector, Property 14: UPDATE with Newer Version")
void testUpdateWithNewerVersion(
    @ForAll("updateMessages") ParsedMessage msg,
    @ForAll("existingVersionsWithOlderVersion") Map<String, Long> existingVersions
) {
    // Ensure msg.version > existingVersions.get(msg.dedupKey)
    Long existingVersion = existingVersions.get(msg.getDedupKey());
    Assume.that(existingVersion != null && msg.getVersion() > existingVersion);
    
    VersionControlState state = new VersionControlState(existingVersions);
    Action action = state.determineAction(msg);
    
    assertThat(action).isEqualTo(Action.UPDATE);
}
```

#### 4. Round-Trip Properties

Test serialization/deserialization preserves data:

```java
@Property
@Tag("Feature: cdc-version-control-connector, Property 2: Data Array Serialization Round-Trip")
void testDataArrayRoundTrip(@ForAll("dataArrays") List<Map<String, Object>> dataArray) {
    String json = serializer.toJson(dataArray);
    List<Map<String, Object>> deserialized = serializer.fromJson(json);
    
    assertThat(deserialized).isEqualTo(dataArray);
}
```

### Unit Testing

**Scope**: Specific examples, edge cases, and error conditions not covered by property tests.

**Test Framework**: JUnit 5 with AssertJ

**Unit Test Categories**:

#### 1. Edge Cases

```java
@Test
void testEmptyDataArray() {
    Map<String, Object> message = createMessage(Collections.emptyList());
    ParsedMessage parsed = parser.parse(message);
    assertThat(parsed.getRecordJson()).isEqualTo("[]");
}

@Test
void testNullVersionHandling() {
    Map<String, Object> message = createMessage();
    message.put("version", null);
    
    assertThatThrownBy(() -> parser.parse(message))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("version");
}
```

#### 2. Logging Verification

```java
@Test
void testInsertConflictLogsWarning() {
    ParsedMessage msg = createInsertMessage("key1", 100L);
    Map<String, Long> existing = Map.of("key1", 50L);
    
    VersionControlState state = new VersionControlState(existing);
    state.determineAction(msg);
    
    assertThat(logCapture.getWarnings())
        .anyMatch(log -> log.contains("INSERT conflict") && log.contains("key1"));
}
```

#### 3. Configuration Validation

```java
@Test
void testMissingRequiredConfig() {
    Map<String, String> props = new HashMap<>();
    // Missing iceberg.table.name
    
    assertThatThrownBy(() -> new VersionControlIcebergSinkConfig(props))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("iceberg.table.name");
}
```

### Integration Testing

**Scope**: Infrastructure components and external system interactions.

**Test Framework**: Testcontainers for Docker-based integration tests

**Integration Test Categories**:

#### 1. Iceberg Operations

```java
@Test
@Testcontainers
void testIcebergAppendOperation() {
    // Start MinIO, Hive Metastore containers
    GenericContainer<?> minio = new GenericContainer<>("minio/minio:latest")...;
    GenericContainer<?> hive = new GenericContainer<>("apache/hive:4.0.0")...;
    
    // Create connector with test config
    VersionControlIcebergSinkTask task = new VersionControlIcebergSinkTask();
    task.start(createTestConfig());
    
    // Send test messages
    List<SinkRecord> records = List.of(createTestRecord());
    task.put(records);
    
    // Verify data in Iceberg
    Table table = loadTable();
    assertThat(countRecords(table)).isEqualTo(1);
}
```

#### 2. End-to-End Workflow

```java
@Test
@Testcontainers
void testCompleteWorkflow() {
    // Setup: Kafka, MinIO, Hive Metastore
    // ...
    
    // Send INSERT messages
    sendToKafka(createInsertMessages(10));
    waitForProcessing();
    assertThat(countRecords()).isEqualTo(10);
    
    // Send UPDATE messages
    sendToKafka(createUpdateMessages(10));
    waitForProcessing();
    assertThat(countRecords()).isEqualTo(10); // Same count, updated
    
    // Send DELETE messages
    sendToKafka(createDeleteMessages(5));
    waitForProcessing();
    assertThat(countRecords()).isEqualTo(5);
}
```

#### 3. Performance Testing

```java
@Test
@Testcontainers
void testBatchProcessingPerformance() {
    int batchSize = 50000;
    List<SinkRecord> records = generateTestRecords(batchSize);
    
    long startTime = System.currentTimeMillis();
    task.put(records);
    long duration = System.currentTimeMillis() - startTime;
    
    // Should complete within 60 seconds
    assertThat(duration).isLessThan(60000);
}
```

### Test Coverage Goals

- **Property Tests**: 100% coverage of core business logic (parsing, deduplication, version control)
- **Unit Tests**: 90% coverage of error handling and edge cases
- **Integration Tests**: 80% coverage of Iceberg operations and end-to-end workflows
- **Overall**: Minimum 85% code coverage

### Continuous Integration

```yaml
# .github/workflows/test.yml
name: Test CDC Connector

on: [push, pull_request]

jobs:
  property-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Run Property Tests
        run: ./gradlew test --tests "*Property*"
        
  unit-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Run Unit Tests
        run: ./gradlew test --tests "*Test*"
        
  integration-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Run Integration Tests
        run: ./gradlew integrationTest
```


## Configuration

### Complete Configuration Example

```json
{
  "name": "sink.tram_quan_trac_cdc",
  "config": {
    "connector.class": "com.example.kafka.connect.iceberg.VersionControlIcebergSinkConnector",
    "tasks.max": "1",
    "topics": "tram_quan_trac",
    
    "iceberg.table.name": "default.tram_quan_trac_cdc",
    "iceberg.catalog.type": "hive",
    "iceberg.catalog.uri": "thrift://hive-metastore:9083",
    "iceberg.catalog.warehouse": "s3a://bucket/warehouse/",
    
    "iceberg.catalog.s3.endpoint": "http://minio:9000",
    "iceberg.catalog.s3.path-style-access": "true",
    "iceberg.catalog.s3.access-key-id": "minioadmin",
    "iceberg.catalog.s3.secret-access-key": "minioadmin",
    "iceberg.catalog.s3.region": "us-east-1",
    
    "consumer.max.poll.records": "50000",
    "offset.flush.interval.ms": "30000",
    
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    
    "errors.tolerance": "none",
    "errors.log.enable": "true",
    "errors.log.include.messages": "true"
  }
}
```

### Configuration Properties Reference

#### Required Properties

| Property | Type | Description | Example |
|----------|------|-------------|---------|
| `connector.class` | String | Fully qualified connector class name | `com.example.kafka.connect.iceberg.VersionControlIcebergSinkConnector` |
| `tasks.max` | Integer | Number of tasks (must be 1 for version control) | `1` |
| `topics` | String | Comma-separated list of source topics | `tram_quan_trac` |
| `iceberg.table.name` | String | Target Iceberg table (namespace.table) | `default.tram_quan_trac_cdc` |
| `iceberg.catalog.type` | String | Catalog type (hive, hadoop, rest) | `hive` |
| `iceberg.catalog.uri` | String | Hive Metastore URI | `thrift://hive-metastore:9083` |
| `iceberg.catalog.warehouse` | String | Warehouse location (S3A path) | `s3a://bucket/warehouse/` |

#### S3/MinIO Properties

| Property | Type | Description | Example |
|----------|------|-------------|---------|
| `iceberg.catalog.s3.endpoint` | String | S3-compatible endpoint URL | `http://minio:9000` |
| `iceberg.catalog.s3.path-style-access` | Boolean | Use path-style access (required for MinIO) | `true` |
| `iceberg.catalog.s3.access-key-id` | String | S3 access key | `minioadmin` |
| `iceberg.catalog.s3.secret-access-key` | String | S3 secret key | `minioadmin` |
| `iceberg.catalog.s3.region` | String | S3 region | `us-east-1` |

#### Performance Tuning Properties

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `consumer.max.poll.records` | Integer | 500 | Maximum records per batch (10000-50000 recommended) |
| `offset.flush.interval.ms` | Long | 60000 | Offset commit interval in milliseconds (30000-60000 recommended) |

#### Converter Properties

| Property | Type | Description |
|----------|------|-------------|
| `value.converter` | String | Value converter class (must be JsonConverter) |
| `value.converter.schemas.enable` | Boolean | Enable schema support (must be false) |
| `key.converter` | String | Key converter class |

#### Error Handling Properties

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `errors.tolerance` | String | none | Error tolerance level (none, all) |
| `errors.log.enable` | Boolean | false | Enable error logging |
| `errors.log.include.messages` | Boolean | false | Include message content in error logs |

### Configuration Validation

The connector validates configuration at startup:

```java
public class VersionControlIcebergSinkConfig extends AbstractConfig {
    
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(
            TABLE_NAME_CONFIG,
            ConfigDef.Type.STRING,
            ConfigDef.NO_DEFAULT_VALUE,
            ConfigDef.Importance.HIGH,
            "Iceberg table name"
        )
        .define(
            CATALOG_TYPE_CONFIG,
            ConfigDef.Type.STRING,
            "hive",
            ConfigDef.Importance.HIGH,
            "Catalog type"
        )
        .define(
            CATALOG_URI_CONFIG,
            ConfigDef.Type.STRING,
            ConfigDef.NO_DEFAULT_VALUE,
            ConfigDef.Importance.HIGH,
            "Hive Metastore URI"
        )
        .define(
            CATALOG_WAREHOUSE_CONFIG,
            ConfigDef.Type.STRING,
            ConfigDef.NO_DEFAULT_VALUE,
            ConfigDef.Importance.HIGH,
            "Warehouse location"
        )
        .define(
            S3_ENDPOINT_CONFIG,
            ConfigDef.Type.STRING,
            ConfigDef.NO_DEFAULT_VALUE,
            ConfigDef.Importance.HIGH,
            "S3 endpoint URL"
        )
        .define(
            S3_PATH_STYLE_ACCESS_CONFIG,
            ConfigDef.Type.BOOLEAN,
            true,
            ConfigDef.Importance.MEDIUM,
            "Use path-style access"
        )
        .define(
            S3_ACCESS_KEY_CONFIG,
            ConfigDef.Type.STRING,
            ConfigDef.NO_DEFAULT_VALUE,
            ConfigDef.Importance.HIGH,
            "S3 access key"
        )
        .define(
            S3_SECRET_KEY_CONFIG,
            ConfigDef.Type.PASSWORD,
            ConfigDef.NO_DEFAULT_VALUE,
            ConfigDef.Importance.HIGH,
            "S3 secret key"
        )
        .define(
            MAX_POLL_RECORDS_CONFIG,
            ConfigDef.Type.INT,
            50000,
            ConfigDef.Range.between(1000, 100000),
            ConfigDef.Importance.MEDIUM,
            "Maximum records per batch"
        )
        .define(
            FLUSH_INTERVAL_CONFIG,
            ConfigDef.Type.LONG,
            30000L,
            ConfigDef.Range.between(10000L, 300000L),
            ConfigDef.Importance.MEDIUM,
            "Offset flush interval in milliseconds"
        );
}
```

### Performance Tuning Guidelines

#### Batch Size Tuning

**Small Batches (1k-10k records)**:
- Lower latency (faster commits)
- More frequent Iceberg queries
- Higher overhead per record
- Recommended for: Low-throughput topics, latency-sensitive applications

**Medium Batches (10k-30k records)**:
- Balanced latency and throughput
- Reasonable query frequency
- Good deduplication efficiency
- Recommended for: Most production workloads

**Large Batches (30k-50k records)**:
- Higher throughput
- Better deduplication efficiency
- Increased memory usage
- Longer processing time per batch
- Recommended for: High-throughput topics, batch-oriented workloads

#### Memory Configuration

```bash
# Kafka Connect worker JVM options
KAFKA_HEAP_OPTS="-Xms2G -Xmx4G"

# For large batches (50k records), increase heap:
KAFKA_HEAP_OPTS="-Xms4G -Xmx8G"
```

**Memory Estimation**:
- Base overhead: ~500MB
- Per message: ~1-2KB (depending on data array size)
- Batch of 50k messages: ~50-100MB
- Iceberg operations: ~200-500MB
- **Recommended heap**: 4-8GB for production

#### Parallelism Configuration

**Single Task (Recommended)**:
```json
{
  "tasks.max": "1"
}
```

**Rationale**: Version control requires global ordering within a topic. Multiple tasks would require distributed coordination, adding complexity without significant performance benefit.

**Scaling Strategy**: Scale horizontally by partitioning topics and running separate connector instances per partition group.

## Dependencies

### Runtime Dependencies

```gradle
dependencies {
    // Kafka Connect API (provided by runtime)
    compileOnly 'org.apache.kafka:connect-api:3.5.1'
    compileOnly 'org.apache.kafka:connect-transforms:3.5.1'
    
    // JSON processing
    implementation 'com.fasterxml.jackson.core:jackson-databind:2.15.2'
    
    // Iceberg core
    implementation 'org.apache.iceberg:iceberg-core:1.7.0'
    implementation 'org.apache.iceberg:iceberg-hive-metastore:1.7.0'
    implementation 'org.apache.iceberg:iceberg-aws:1.7.0'
    implementation 'org.apache.iceberg:iceberg-data:1.7.0'
    implementation 'org.apache.iceberg:iceberg-parquet:1.7.0'
    
    // Hadoop for Hive and S3A
    implementation 'org.apache.hadoop:hadoop-client:3.3.4'
    implementation 'org.apache.hadoop:hadoop-aws:3.3.4'
    
    // Hive Metastore client
    implementation 'org.apache.hive:hive-metastore:4.0.0'
    
    // Logging (provided by runtime)
    compileOnly 'org.slf4j:slf4j-api:1.7.36'
}
```

### Dependency Rationale

| Dependency | Version | Purpose |
|------------|---------|---------|
| Kafka Connect API | 3.5.1 | Core connector framework |
| Jackson Databind | 2.15.2 | JSON parsing and serialization |
| Iceberg Core | 1.7.0 | Table API and data structures |
| Iceberg Hive Metastore | 1.7.0 | Hive catalog integration |
| Iceberg AWS | 1.7.0 | S3/S3A file system support |
| Iceberg Data | 1.7.0 | Generic record support |
| Iceberg Parquet | 1.7.0 | Parquet file writing |
| Hadoop Client | 3.3.4 | HDFS and file system abstractions |
| Hadoop AWS | 3.3.4 | S3A file system implementation |
| Hive Metastore | 4.0.0 | Metastore client library |
| SLF4J | 1.7.36 | Logging facade |

### Build Configuration

```gradle
plugins {
    id 'java'
}

group = 'com.example.kafka.connect'
version = '2.0.0'

repositories {
    mavenCentral()
}

java {
    sourceCompatibility = JavaVersion.VERSION_11
    targetCompatibility = JavaVersion.VERSION_11
}

jar {
    manifest {
        attributes(
            'Implementation-Title': 'CDC Version Control Iceberg Sink Connector',
            'Implementation-Version': version
        )
    }
    
    // Include all runtime dependencies in JAR
    from {
        configurations.runtimeClasspath.collect { 
            it.isDirectory() ? it : zipTree(it) 
        }
    }
    
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    
    // Exclude provided dependencies
    exclude 'META-INF/*.SF'
    exclude 'META-INF/*.DSA'
    exclude 'META-INF/*.RSA'
}
```

## Deployment

### Build Process

```bash
# Build the connector JAR
./gradlew clean build

# Output: build/libs/cdc-version-control-connector-2.0.0.jar
```

### Docker Deployment

**Dockerfile for Kafka Connect with Connector**:

```dockerfile
FROM confluentinc/cp-kafka-connect:7.5.0

# Copy connector JAR
COPY build/libs/cdc-version-control-connector-2.0.0.jar \
     /usr/share/java/kafka-connect-iceberg/

# Set environment variables
ENV CONNECT_PLUGIN_PATH="/usr/share/java,/usr/share/confluent-hub-components"
ENV CONNECT_KEY_CONVERTER="org.apache.kafka.connect.storage.StringConverter"
ENV CONNECT_VALUE_CONVERTER="org.apache.kafka.connect.json.JsonConverter"
ENV CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE="false"

# Increase heap for large batches
ENV KAFKA_HEAP_OPTS="-Xms4G -Xmx8G"
```

### Connector Registration

**Using Kafka Connect REST API**:

```bash
# Register connector
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @configs/sink.tram_quan_trac_cdc.json

# Check status
curl http://localhost:8083/connectors/sink.tram_quan_trac_cdc/status

# Pause connector
curl -X PUT http://localhost:8083/connectors/sink.tram_quan_trac_cdc/pause

# Resume connector
curl -X PUT http://localhost:8083/connectors/sink.tram_quan_trac_cdc/resume

# Delete connector
curl -X DELETE http://localhost:8083/connectors/sink.tram_quan_trac_cdc
```

### Monitoring

**Key Metrics to Monitor**:

```java
// Connector metrics (exposed via JMX)
- kafka.connect:type=sink-task-metrics,connector=sink.tram_quan_trac_cdc
  - sink-record-read-rate
  - sink-record-send-rate
  - offset-commit-completion-rate
  - offset-commit-completion-total

// Custom metrics (log-based)
- Batch size (records per put() call)
- Deduplication ratio (original / deduplicated)
- Processing time per batch
- Insert/Update/Delete counts
- Skip/Error counts
```

**Logging Configuration**:

```properties
# log4j.properties
log4j.logger.com.example.kafka.connect.iceberg=INFO, stdout
log4j.logger.org.apache.iceberg=WARN, stdout
log4j.logger.org.apache.hadoop=WARN, stdout
```

### Operational Procedures

#### Startup Checklist

1. ✅ Verify Hive Metastore is accessible
2. ✅ Verify MinIO/S3 is accessible
3. ✅ Verify target Iceberg table exists with correct schema
4. ✅ Verify Kafka topic exists and has messages
5. ✅ Register connector via REST API
6. ✅ Monitor logs for successful startup
7. ✅ Verify first batch processes successfully

#### Troubleshooting Guide

**Problem**: Connector fails to start with "Table not found"

**Solution**:
```sql
-- Create table manually
CREATE TABLE default.tram_quan_trac_cdc (
  id STRING,
  dedup_key STRING,
  record STRING,
  ingest_time STRING,
  length BIGINT,
  key STRING,
  type STRING,
  version BIGINT
)
USING iceberg
TBLPROPERTIES ('format-version' = '2');
```

**Problem**: OutOfMemoryError during batch processing

**Solution**:
1. Reduce `consumer.max.poll.records` to 10000-20000
2. Increase JVM heap: `KAFKA_HEAP_OPTS="-Xms4G -Xmx8G"`
3. Monitor memory usage and adjust accordingly

**Problem**: High number of version conflicts

**Solution**:
1. Check for duplicate messages in Kafka topic
2. Verify version field is monotonically increasing
3. Review CDC source configuration for proper ordering

**Problem**: Slow batch processing (>60 seconds)

**Solution**:
1. Check Iceberg query performance (should be <10s)
2. Verify S3/MinIO network latency
3. Consider partitioning Iceberg table by ingest_time
4. Reduce batch size if deduplication is expensive

## Appendix

### Pseudocode for Core Processing Logic

```
FUNCTION put(records: Collection<SinkRecord>):
    IF records.isEmpty():
        RETURN
    
    LOG "Processing batch: {records.size()} records"
    startTime = currentTimeMillis()
    
    // Step 1: Parse and transform messages
    parsedMessages = []
    FOR EACH record IN records:
        TRY:
            msg = parseMessage(record)
            parsedMessages.add(msg)
        CATCH ParseException:
            LOG ERROR "Failed to parse message: {record}"
            CONTINUE
    
    // Step 2: Deduplicate by dedup_key, keep max version
    latestByDedupKey = HashMap<String, ParsedMessage>()
    FOR EACH msg IN parsedMessages:
        dedupKey = msg.getDedupKey()
        existing = latestByDedupKey.get(dedupKey)
        
        IF existing IS NULL:
            latestByDedupKey.put(dedupKey, msg)
        ELSE IF msg.getVersion() > existing.getVersion():
            latestByDedupKey.put(dedupKey, msg)
    
    LOG "After deduplication: {latestByDedupKey.size()} messages"
    
    // Step 3: Query Iceberg for existing versions
    dedupKeys = latestByDedupKey.keySet()
    existingVersions = queryIcebergVersions(dedupKeys)
    LOG "Queried {existingVersions.size()} existing records"
    
    // Step 4: Apply I/U/D rules
    toInsert = []
    toUpdate = []
    toDelete = []
    skipped = 0
    errors = 0
    
    FOR EACH msg IN latestByDedupKey.values():
        dedupKey = msg.getDedupKey()
        existingVersion = existingVersions.get(dedupKey)
        action = determineAction(msg, existingVersion)
        
        SWITCH action:
            CASE INSERT:
                toInsert.add(createIcebergRecord(msg))
            CASE UPDATE:
                toUpdate.add(createIcebergRecord(msg))
            CASE DELETE:
                toDelete.add(dedupKey)
            CASE SKIP:
                skipped++
            CASE ERROR:
                errors++
    
    LOG "Actions: {toInsert.size()} inserts, {toUpdate.size()} updates, {toDelete.size()} deletes, {skipped} skipped, {errors} errors"
    
    // Step 6: Execute batch operations in single transaction
    batchWriteToIceberg(toInsert, toUpdate, toDelete)
    
    duration = currentTimeMillis() - startTime
    LOG "Batch processed in {duration}ms"


FUNCTION batchWriteToIceberg(
    toInsert: List<GenericRecord>,
    toUpdate: List<GenericRecord>,
    toDelete: List<String>
):
    // Single Iceberg transaction for atomicity
    txn = table.newTransaction()
    
    TRY:
        // Step 1: Collect all dedup_keys to delete (UPDATE + DELETE)
        IF toUpdate NOT EMPTY OR toDelete NOT EMPTY:
            dedupKeysToDelete = Set<String>()
            
            // Add dedup_keys from UPDATE
            FOR EACH record IN toUpdate:
                dedupKeysToDelete.add(record.getField("dedup_key"))
            
            // Add dedup_keys from DELETE
            dedupKeysToDelete.addAll(toDelete)
            
            // Single row-level delete operation
            delete = txn.newDelete()
            delete.deleteFromRowFilter(Expressions.in("dedup_key", dedupKeysToDelete.toArray()))
            delete.commit()
        
        // Step 2: Collect all records to append (INSERT + UPDATE)
        allToAppend = []
        allToAppend.addAll(toInsert)
        allToAppend.addAll(toUpdate)
        
        IF allToAppend NOT EMPTY:
            dataFile = writeParquetFile(allToAppend)
            append = txn.newAppend()
            append.appendFile(dataFile)
            append.commit()
        
        // Step 3: Commit entire transaction atomically
        txn.commitTransaction()
        
        LOG "Batch transaction committed: {toInsert.size()} inserts, {toUpdate.size()} updates, {toDelete.size()} deletes"
        
    CATCH Exception e:
        LOG ERROR "Failed to commit batch transaction: {e}"
        THROW ConnectException("Batch write failed", e)


FUNCTION determineAction(msg: ParsedMessage, existingVersion: Long?): Action:
    type = msg.getOperationType()
    incomingVersion = msg.getVersion()
    
    SWITCH type:
        CASE "INSERT":
            IF existingVersion IS NULL:
                RETURN Action.INSERT
            ELSE:
                LOG WARN "INSERT conflict: dedup_key={msg.getDedupKey()}"
                RETURN Action.ERROR
        
        CASE "UPDATE":
            IF existingVersion IS NULL:
                LOG INFO "UPDATE on non-existing key, treating as INSERT"
                RETURN Action.INSERT
            ELSE IF incomingVersion > existingVersion:
                RETURN Action.UPDATE
            ELSE:
                LOG INFO "UPDATE skipped: stale version"
                RETURN Action.SKIP
        
        CASE "DELETE":
            IF existingVersion IS NULL:
                LOG WARN "DELETE on non-existing key"
                RETURN Action.SKIP
            ELSE IF incomingVersion > existingVersion:
                RETURN Action.DELETE
            ELSE:
                LOG INFO "DELETE skipped: stale version"
                RETURN Action.SKIP
        
        DEFAULT:
            LOG ERROR "Unknown operation type: {type}"
            RETURN Action.ERROR


FUNCTION queryIcebergVersions(dedupKeys: Set<String>): Map<String, Long>:
    IF dedupKeys.isEmpty():
        RETURN emptyMap()
    
    LOG "Querying Iceberg for {dedupKeys.size()} unique dedup_keys"
    
    // Chunk queries if too many keys (unlikely with message-level dedup)
    chunkSize = 10000
    result = HashMap<String, Long>()
    
    dedupKeyList = dedupKeys.toList()
    FOR i = 0 TO dedupKeyList.size() STEP chunkSize:
        end = min(i + chunkSize, dedupKeyList.size())
        chunk = dedupKeyList.subList(i, end)
        
        // Build filter: dedup_key IN (...)
        filter = Expressions.in("dedup_key", chunk.toArray())
        
        // Scan table
        FOR EACH record IN IcebergGenerics.read(table).where(filter).select("dedup_key", "version"):
            dedupKey = record.getField("dedup_key")
            version = record.getField("version")
            result.put(dedupKey, version)
    
    RETURN result

// Note: With message-level deduplication (dedup_key = topic:key),
// the number of unique dedup_keys is typically very small (1-10)
// even for large batches (50k messages), because many messages
// share the same topic and key field name.
//
// Example:
// - 50,000 messages from topic "tram_quan_trac" with key="MaTram"
// - After deduplication: 1 unique dedup_key = "tram_quan_trac:MaTram"
// - Query: IN filter with only 1 key (very fast)


### Message Flow Example

**Input Messages**:
```json
[
  {
    "data": [{"MaTram": "TQ001", "TenTram": "Tram 1"}],
    "key": "MaTram",
    "type": "INSERT",
    "version": 1000,
    "ngay_cap_nhat": "2024-01-01T00:00:00Z",
    "length": 1
  },
  {
    "data": [{"MaTram": "TQ001", "TenTram": "Tram 1 Updated"}],
    "key": "MaTram",
    "type": "UPDATE",
    "version": 2000,
    "ngay_cap_nhat": "2024-01-01T01:00:00Z",
    "length": 1
  },
  {
    "data": [{"MaTram": "TQ001", "TenTram": "Tram 1 Updated Again"}],
    "key": "MaTram",
    "type": "UPDATE",
    "version": 1500,
    "ngay_cap_nhat": "2024-01-01T00:30:00Z",
    "length": 1
  }
]
```

**After Deduplication** (keep max version = 2000):
```json
[
  {
    "data": [{"MaTram": "TQ001", "TenTram": "Tram 1 Updated"}],
    "key": "MaTram",
    "type": "UPDATE",
    "version": 2000,
    "ngay_cap_nhat": "2024-01-01T01:00:00Z",
    "length": 1
  }
]
```

**Iceberg Query Result**:
```
dedup_key: "tram_quan_trac:MaTram", version: 1000
```

**Note**: dedup_key = "tram_quan_trac:MaTram" (topic:key field name), NOT "tram_quan_trac:TQ001" (topic:value)

**Version Control Decision**:
- Type: UPDATE
- Existing version: 1000
- Incoming version: 2000
- Decision: UPDATE (2000 > 1000)

**Final Iceberg Record**:
```json
{
  "id": "uuid-12345",
  "dedup_key": "tram_quan_trac:MaTram",
  "record": "[{\"MaTram\":\"TQ001\",\"TenTram\":\"Tram 1 Updated\"}]",
  "ingest_time": "2024-01-01T01:00:00Z",
  "length": 1,
  "key": "MaTram",
  "type": "UPDATE",
  "version": 2000
}
```

**Note**: dedup_key = "tram_quan_trac:MaTram" because:
- topic = "tram_quan_trac"
- key field name = "MaTram" (NOT the value "TQ001")
- All messages with the same topic and key field name share the same dedup_key

---

**Document Version**: 1.0  
**Last Updated**: 2024-01-15  
**Authors**: Development Team  
**Status**: Ready for Implementation
