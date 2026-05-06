# Requirements Document: CDC Version Control Connector

## Introduction

The CDC Version Control Connector is a custom Kafka Connect Sink Connector that processes Change Data Capture (CDC) messages from Kafka topics and writes them to Apache Iceberg tables with version control. The connector handles INSERT, UPDATE, and DELETE operations while ensuring data consistency through version checking and deduplication.

This connector processes CDC messages at the message/batch level, storing the entire data array as a JSON string in each record. It uses business keys for version control and deduplication, not technical IDs.

## Glossary

- **CDC_Connector**: The custom Kafka Connect Sink Connector that processes CDC messages with version control
- **Source_Topic**: The Kafka topic containing CDC messages (e.g., `tram_quan_trac`)
- **Target_Table**: The Iceberg table where processed records are stored (`tram_quan_trac_cdc`)
- **Batch**: A collection of Kafka messages processed together in a single `put()` call
- **Technical_ID**: The id field containing technical metadata (UUID or topic-partition-offset) - NOT used for version comparison
- **Business_Key**: The key field containing the business key field name (e.g., "MaTram") - used for version control
- **Dedup_Key**: Composite key in format "topic:key" where key is the business key field name (e.g., "tram_quan_trac:MaTram" where "MaTram" is the field name, NOT the value "TQ001") - used for deduplication and version control
- **Version**: A BIGINT timestamp used to determine record freshness and order
- **Deduplication**: The process of selecting only the latest version when multiple messages share the same Dedup_Key
- **Iceberg_Table_API**: Apache Iceberg's Java API for direct table operations (not Trino JDBC)
- **Iceberg_Table_Scan_API**: Apache Iceberg's API for reading existing records from tables
- **Hive_Metastore**: The catalog service for Iceberg table metadata
- **S3A_FileSystem**: The storage layer using MinIO with S3-compatible API
- **Operation_Type**: The CDC operation type: INSERT, UPDATE, or DELETE
- **Record_Field**: JSON string representation of the entire data array from the CDC message

## Requirements

### Requirement 1: Message Consumption and Parsing

**User Story:** As a data engineer, I want the connector to consume CDC messages from Kafka topics and parse them internally, so that I can process change events in real-time without relying on SMT chains.

#### Acceptance Criteria

1. THE CDC_Connector SHALL consume messages from the Source_Topic
2. WHEN a message is received, THE CDC_Connector SHALL parse the raw JSON message structure internally
3. THE CDC_Connector SHALL extract the following fields from each message: data (array), key (string), type (string), version (number), ngay_cap_nhat (string), and length (number)
4. THE CDC_Connector SHALL convert the entire data array to a JSON string and store it in the Record_Field
5. THE CDC_Connector SHALL extract the Business_Key field name from the key field (e.g., "MaTram")
6. THE CDC_Connector SHALL construct the Dedup_Key by concatenating the topic name, a colon, and the Business_Key field name (e.g., "tram_quan_trac:MaTram" where "MaTram" is the field name, NOT the value like "TQ001")
7. THE CDC_Connector SHALL cast the version field from STRING or Number to BIGINT
8. THE CDC_Connector SHALL cast the length field from STRING to BIGINT
9. THE CDC_Connector SHALL map the ngay_cap_nhat field to ingest_time field
10. THE CDC_Connector SHALL NOT rely on SMT chain for data transformation
11. WHEN the message structure is invalid, THE CDC_Connector SHALL log an ERROR and skip the message

### Requirement 2: Batch Deduplication

**User Story:** As a data engineer, I want the connector to deduplicate messages within each batch using business keys, so that only the latest version of each record is processed.

#### Acceptance Criteria

1. WHEN multiple messages in a Batch share the same Dedup_Key, THE CDC_Connector SHALL select only the message with the maximum version value
2. THE CDC_Connector SHALL perform deduplication with O(n) time complexity using a HashMap data structure with Dedup_Key as the key
3. THE CDC_Connector SHALL use Dedup_Key (format: "topic:key") for deduplication, NOT Technical_ID
4. WHEN deduplication occurs, THE CDC_Connector SHALL log the number of messages deduplicated at INFO level
5. THE CDC_Connector SHALL preserve all messages with unique Dedup_Key values
6. WHEN a Dedup_Key appears multiple times, THE CDC_Connector SHALL discard all messages except the one with the highest version

### Requirement 3: Existing Version Retrieval

**User Story:** As a data engineer, I want the connector to efficiently check existing record versions using Iceberg Table Scan API, so that version control rules can be applied correctly.

#### Acceptance Criteria

1. WHEN processing a Batch, THE CDC_Connector SHALL query the Target_Table exactly once per Batch to retrieve existing versions
2. THE CDC_Connector SHALL use Iceberg_Table_Scan_API to read existing records from the Target_Table
3. THE CDC_Connector SHALL filter records by Dedup_Key field (or Business_Key field if Dedup_Key is not present)
4. THE CDC_Connector SHALL include all unique Dedup_Key values from the current Batch in the filter
5. THE CDC_Connector SHALL build a HashMap mapping Dedup_Key to existing version from the scan results
6. THE CDC_Connector SHALL NOT use Trino JDBC for version retrieval
7. THE CDC_Connector SHALL NOT use SQL-style queries for version retrieval
8. WHEN the Target_Table does not contain a Dedup_Key, THE CDC_Connector SHALL treat the existing version as null

### Requirement 4: INSERT Operation Processing

**User Story:** As a data engineer, I want the connector to handle INSERT operations with version control using business keys, so that duplicate inserts are prevented.

#### Acceptance Criteria

1. WHEN the Operation_Type is INSERT and the Dedup_Key does not exist in the Target_Table, THE CDC_Connector SHALL insert the record
2. WHEN the Operation_Type is INSERT and the Dedup_Key already exists in the Target_Table, THE CDC_Connector SHALL log a WARNING message and skip the record
3. THE CDC_Connector SHALL include the Dedup_Key and existing version in the WARNING log message
4. WHEN inserting a record, THE CDC_Connector SHALL write all fields including id, dedup_key, record, ingest_time, length, key, type, and version to the Target_Table
5. THE CDC_Connector SHALL store the version field as BIGINT data type
6. THE CDC_Connector SHALL store the length field as BIGINT data type
7. THE CDC_Connector SHALL use Iceberg append API for INSERT operations

### Requirement 5: UPDATE Operation Processing

**User Story:** As a data engineer, I want the connector to handle UPDATE operations with version control using business keys, so that only newer updates are applied.

#### Acceptance Criteria

1. WHEN the Operation_Type is UPDATE and the Dedup_Key exists in the Target_Table and the incoming version is greater than the existing version, THE CDC_Connector SHALL update the record
2. WHEN the Operation_Type is UPDATE and the Dedup_Key does not exist in the Target_Table, THE CDC_Connector SHALL insert the record (upsert behavior)
3. WHEN the Operation_Type is UPDATE and the incoming version is less than or equal to the existing version, THE CDC_Connector SHALL skip the record
4. WHEN skipping an UPDATE due to version check, THE CDC_Connector SHALL log at INFO level
5. THE CDC_Connector SHALL include the Dedup_Key, incoming version, and existing version in the log message
6. THE CDC_Connector SHALL use row-level delete by dedup_key (using Iceberg equality delete or file rewrite strategy) to replace the old record, then append the new record

### Requirement 6: DELETE Operation Processing

**User Story:** As a data engineer, I want the connector to handle DELETE operations with version control using business keys, so that only valid deletes are executed.

#### Acceptance Criteria

1. WHEN the Operation_Type is DELETE and the Dedup_Key exists in the Target_Table and the incoming version is greater than the existing version, THE CDC_Connector SHALL delete the record
2. WHEN the Operation_Type is DELETE and the incoming version is less than or equal to the existing version, THE CDC_Connector SHALL skip the record
3. WHEN the Operation_Type is DELETE and the Dedup_Key does not exist in the Target_Table, THE CDC_Connector SHALL log a WARNING message and skip the record
4. THE CDC_Connector SHALL include the Dedup_Key in the WARNING log message
5. WHEN skipping a DELETE due to version check, THE CDC_Connector SHALL log at INFO level
6. THE CDC_Connector SHALL use row-level delete by dedup_key (using Iceberg equality delete or file rewrite strategy) for DELETE operations
7. THE DELETE operation SHALL require a version value, and THE CDC_Connector SHALL only delete when the incoming version is greater than the existing version

### Requirement 7: Target Table Schema

**User Story:** As a data engineer, I want records stored in a consistent schema with proper data types, so that I can query and analyze the data reliably.

#### Acceptance Criteria

1. THE CDC_Connector SHALL write records to the Target_Table with the following schema: id (STRING), dedup_key (STRING), record (STRING), ingest_time (STRING), length (BIGINT), key (STRING), type (STRING), version (BIGINT)
2. THE CDC_Connector SHALL use the Technical_ID in the id field (UUID or topic-partition-offset)
3. THE CDC_Connector SHALL use the Dedup_Key in the dedup_key field (format: "topic:key" where key is the business key field name, e.g., "tram_quan_trac:MaTram")
4. THE CDC_Connector SHALL store the record field as a JSON string representation of the entire data array from the CDC message
5. THE CDC_Connector SHALL store the version field as BIGINT data type (cast from STRING or Number)
6. THE CDC_Connector SHALL store the length field as BIGINT data type (cast from STRING)
7. THE CDC_Connector SHALL store the ingest_time field mapped from the ngay_cap_nhat field in the CDC message
8. THE CDC_Connector SHALL store the key field as the Business_Key field name from the original message (e.g., "MaTram")
9. THE CDC_Connector SHALL NOT explode individual items from the data array into separate rows

### Requirement 8: Iceberg Table Operations

**User Story:** As a data engineer, I want the connector to use Iceberg Table API directly with format-version=2 support, so that operations are reliable and support row-level updates and deletes.

#### Acceptance Criteria

1. THE CDC_Connector SHALL use Iceberg_Table_API for all table operations
2. THE CDC_Connector SHALL use Hive_Metastore as the catalog for table metadata
3. THE CDC_Connector SHALL use S3A_FileSystem with MinIO for data storage
4. THE CDC_Connector SHALL configure the catalog with the following properties: catalog type (hive), metastore URI (thrift://hive-metastore:9083), warehouse location (s3a://bucket/warehouse/), S3 endpoint, path-style access, access credentials, and region
5. THE CDC_Connector SHALL NOT use Trino JDBC for any table operations
6. THE Target_Table SHOULD use Iceberg format-version=2 for row-level delete and update support
7. THE CDC_Connector SHALL use Iceberg equality delete or file rewrite strategy for UPDATE and DELETE operations

### Requirement 9: Batch Processing Configuration

**User Story:** As a data engineer, I want to configure batch processing parameters, so that I can optimize throughput and latency.

#### Acceptance Criteria

1. WHERE batch size configuration is provided, THE CDC_Connector SHALL process up to the configured number of records per batch
2. WHERE flush interval configuration is provided, THE CDC_Connector SHALL commit processed records at the configured interval
3. THE CDC_Connector SHALL support batch sizes between 10000 and 50000 records
4. THE CDC_Connector SHALL support flush intervals between 30000 and 60000 milliseconds
5. WHEN neither batch size nor flush interval is reached, THE CDC_Connector SHALL wait until one condition is met before committing

### Requirement 10: Internal Message Transformation

**User Story:** As a data engineer, I want the connector to transform messages internally without relying on SMT chains, so that the transformation logic is self-contained and maintainable.

#### Acceptance Criteria

1. THE CDC_Connector SHALL parse raw Kafka JSON messages internally
2. THE CDC_Connector SHALL convert the data array to a JSON string and store it in the record field
3. THE CDC_Connector SHALL map the ngay_cap_nhat field to the ingest_time field
4. THE CDC_Connector SHALL cast the version field from STRING or Number to BIGINT
5. THE CDC_Connector SHALL cast the length field from STRING to BIGINT
6. THE CDC_Connector SHALL construct the Dedup_Key by concatenating topic name, colon, and Business_Key value
7. THE CDC_Connector SHALL NOT rely on SMT chain for data transformation
8. THE CDC_Connector SHALL NOT use JsonFieldToString SMT
9. THE CDC_Connector SHALL NOT use ReplaceField transform
10. THE CDC_Connector SHALL use JsonConverter with schemas disabled for value deserialization

### Requirement 11: Error Handling and Logging

**User Story:** As a data engineer, I want the connector to log errors and warnings appropriately, so that I can monitor and troubleshoot issues.

#### Acceptance Criteria

1. WHEN a version conflict occurs (INSERT with existing Dedup_Key or DELETE with non-existing Dedup_Key), THE CDC_Connector SHALL log at WARNING level
2. WHEN a record is skipped due to version check, THE CDC_Connector SHALL log at INFO level
3. WHEN an exception occurs during processing, THE CDC_Connector SHALL log at ERROR level with the exception stack trace
4. THE CDC_Connector SHALL use SLF4J as the logging framework
5. THE CDC_Connector SHALL include the Dedup_Key, Operation_Type, and version information in all log messages related to record processing

### Requirement 12: Connector Configuration

**User Story:** As a data engineer, I want to configure the connector through standard Kafka Connect properties, so that deployment is straightforward.

#### Acceptance Criteria

1. THE CDC_Connector SHALL accept configuration for connector class, tasks.max, topics, and table name
2. THE CDC_Connector SHALL accept configuration for Iceberg catalog properties including catalog type, metastore URI, warehouse location, and S3 settings
3. THE CDC_Connector SHALL accept configuration for consumer properties including max.poll.records and offset.flush.interval.ms
4. THE CDC_Connector SHALL accept configuration for value converter, key converter, and transforms
5. THE CDC_Connector SHALL validate required configuration properties at startup and fail with a descriptive error message if any are missing

### Requirement 13: Build and Deployment

**User Story:** As a developer, I want the connector to build successfully with all dependencies, so that I can deploy it to Kafka Connect.

#### Acceptance Criteria

1. THE CDC_Connector SHALL compile with Java 11
2. THE CDC_Connector SHALL package all runtime dependencies into a single JAR file
3. THE CDC_Connector SHALL include the following dependencies: Kafka Connect API, Iceberg Core, Iceberg Hive Metastore, Iceberg AWS, Iceberg Data, Iceberg Parquet, Hadoop Client, Hadoop AWS, Hive Metastore Client, and SLF4J
4. WHEN the build process completes, THE CDC_Connector SHALL produce a JAR file with no compilation errors
5. THE CDC_Connector SHALL use Gradle as the build tool

### Requirement 14: Performance and Scalability

**User Story:** As a data engineer, I want the connector to process large batches efficiently, so that it can handle high-throughput workloads.

#### Acceptance Criteria

1. WHEN processing a Batch of 50000 records, THE CDC_Connector SHALL complete deduplication in less than 5 seconds
2. WHEN querying existing versions, THE CDC_Connector SHALL execute the query in less than 10 seconds for up to 50000 unique keys
3. THE CDC_Connector SHALL use HashMap data structures for O(1) average-case lookup performance
4. THE CDC_Connector SHALL process records sequentially within a single task (tasks.max = 1)
5. WHEN memory usage exceeds available heap space, THE CDC_Connector SHALL log an ERROR and fail the task to trigger a restart

### Requirement 15: Data Consistency Guarantees

**User Story:** As a data engineer, I want the connector to maintain data consistency using business keys, so that the Target_Table accurately reflects the latest state.

#### Acceptance Criteria

1. WHEN a Batch is committed, THE CDC_Connector SHALL ensure all processed records are written to the Target_Table atomically within a single Iceberg transaction
2. THE CDC_Connector SHALL commit all INSERT, UPDATE, and DELETE operations within a single Iceberg transaction to ensure atomicity
3. WHEN a task fails during processing, THE CDC_Connector SHALL NOT commit partial results
4. THE CDC_Connector SHALL use Iceberg's transactional commit mechanism to ensure atomicity
5. WHEN a version conflict is detected, THE CDC_Connector SHALL NOT modify the existing record
6. THE CDC_Connector SHALL ensure that for any given Dedup_Key, the record with the highest version is always retained in the Target_Table
7. THE CDC_Connector SHALL use Dedup_Key (not Technical_ID) for all version control and deduplication operations

