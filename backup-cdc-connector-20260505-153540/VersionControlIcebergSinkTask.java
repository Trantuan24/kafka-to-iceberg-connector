package com.example.kafka.connect.iceberg;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * CDC Version Control Iceberg Sink Task
 * 
 * Implements message-level processing with version control for CDC messages.
 * 
 * Processing flow:
 * 1. Parse and transform: Extract fields from CDC messages (internal, no SMT)
 * 2. Deduplicate: Select max(version) per dedup_key within batch
 * 3. Query existing versions: Single Iceberg Table Scan per batch
 * 4. Apply I/U/D rules: Categorize messages based on version control logic
 * 5. Batch write: Execute all operations in single Iceberg transaction
 * 
 * Key design points:
 * - Message-level processing (entire data array stored as JSON)
 * - dedup_key format: "topic:key" where key is field name (NOT value)
 * - Single transaction for atomic INSERT/UPDATE/DELETE
 * - Row-level delete using equality delete (format-version=2)
 */
public class VersionControlIcebergSinkTask extends SinkTask {

    private static final Logger log = LoggerFactory.getLogger(VersionControlIcebergSinkTask.class);
    
    private VersionControlIcebergSinkConfig config;
    private ObjectMapper objectMapper;
    private Catalog catalog;
    private Table table;
    private Schema schema;
    
    @Override
    public String version() {
        return "2.0.0";
    }

    @Override
    public void start(Map<String, String> props) {
        this.config = new VersionControlIcebergSinkConfig(props);
        this.objectMapper = new ObjectMapper();
        
        try {
            // Initialize Hive Catalog
            this.catalog = createHiveCatalog();
            
            // Load table
            String tableName = config.getTableName();
            TableIdentifier tableId = parseTableIdentifier(tableName);
            this.table = catalog.loadTable(tableId);
            this.schema = table.schema();
            
            // Verify table format-version is 2 for row-level operations
            String formatVersion = table.properties().get("format-version");
            if (formatVersion != null && !formatVersion.equals("2")) {
                log.warn("Table format-version is {}, row-level deletes require format-version=2", formatVersion);
            }
            
            log.info("VersionControlIcebergSinkTask started successfully");
            log.info("Table: {}, format-version: {}", tableName, formatVersion);
            log.info("Schema: {}", schema);
            
        } catch (Exception e) {
            log.error("Failed to start task", e);
            throw new ConnectException("Failed to start task", e);
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (records.isEmpty()) {
            log.debug("Empty batch, skipping");
            return;
        }
        
        log.info("Processing batch: {} records", records.size());
        long startTime = System.currentTimeMillis();
        
        try {
            // Step 1: Parse and transform messages
            Map<String, ParsedMessage> parsedMessages = parseAndTransform(records);
            log.info("Parsed {} messages (from {} original)", parsedMessages.size(), records.size());
            
            // Step 2: Deduplicate by dedup_key (keep max version)
            Map<String, ParsedMessage> deduplicated = deduplicateByDedupKey(parsedMessages);
            log.info("After deduplication: {} messages", deduplicated.size());
            
            // Step 3: Query existing versions from Iceberg
            Set<String> dedupKeys = deduplicated.keySet();
            Map<String, Long> existingVersions = queryExistingVersions(dedupKeys);
            log.info("Queried {} existing versions from Iceberg", existingVersions.size());
            
            // Step 4: Apply version control rules and categorize
            List<GenericRecord> toInsert = new ArrayList<>();
            List<GenericRecord> toUpdate = new ArrayList<>();
            List<String> toDelete = new ArrayList<>();
            
            applyVersionControlRules(deduplicated, existingVersions, toInsert, toUpdate, toDelete);
            
            log.info("Categorized: {} inserts, {} updates, {} deletes", 
                toInsert.size(), toUpdate.size(), toDelete.size());
            
            // Step 5: Execute batch write in single transaction
            batchWriteToIceberg(toInsert, toUpdate, toDelete);
            
            long duration = System.currentTimeMillis() - startTime;
            log.info("Batch processed successfully in {}ms", duration);
            
        } catch (OutOfMemoryError e) {
            log.error("Out of memory processing batch of {} records. Consider reducing consumer.max.poll.records", 
                records.size(), e);
            System.gc();
            throw new ConnectException("Out of memory", e);
        } catch (Exception e) {
            log.error("Error processing batch", e);
            throw new ConnectException("Failed to process batch", e);
        }
    }
    
    /**
     * Step 1: Parse and transform CDC messages.
     * 
     * Extracts fields from each SinkRecord and creates ParsedMessage objects.
     * Handles parse errors by logging and skipping invalid messages.
     * 
     * @param records Collection of Kafka SinkRecords
     * @return Map of dedup_key to ParsedMessage
     */
    private Map<String, ParsedMessage> parseAndTransform(Collection<SinkRecord> records) {
        Map<String, ParsedMessage> result = new HashMap<>();
        int parseErrors = 0;
        
        for (SinkRecord record : records) {
            try {
                ParsedMessage msg = new ParsedMessage(record);
                result.put(msg.getDedupKey(), msg);
            } catch (JsonProcessingException e) {
                log.error("Failed to parse message from topic={}, partition={}, offset={}: JSON error: {}", 
                    record.topic(), record.kafkaPartition(), record.kafkaOffset(), e.getMessage());
                parseErrors++;
            } catch (IllegalArgumentException e) {
                log.error("Failed to parse message from topic={}, partition={}, offset={}: {}", 
                    record.topic(), record.kafkaPartition(), record.kafkaOffset(), e.getMessage());
                parseErrors++;
            } catch (Exception e) {
                log.error("Unexpected error parsing message from topic={}, partition={}, offset={}", 
                    record.topic(), record.kafkaPartition(), record.kafkaOffset(), e);
                parseErrors++;
            }
        }
        
        if (parseErrors > 0) {
            log.warn("Skipped {} messages due to parse errors", parseErrors);
        }
        
        return result;
    }
    
    /**
     * Step 2: Deduplicate messages by dedup_key.
     * 
     * For messages with the same dedup_key, keeps only the one with maximum version.
     * Uses HashMap for O(n) time complexity.
     * 
     * @param messages Map of dedup_key to ParsedMessage
     * @return Deduplicated map with max version per dedup_key
     */
    private Map<String, ParsedMessage> deduplicateByDedupKey(Map<String, ParsedMessage> messages) {
        Map<String, ParsedMessage> result = new HashMap<>();
        int duplicatesRemoved = 0;
        
        for (ParsedMessage msg : messages.values()) {
            String dedupKey = msg.getDedupKey();
            ParsedMessage existing = result.get(dedupKey);
            
            if (existing == null) {
                result.put(dedupKey, msg);
            } else {
                // Keep message with maximum version
                if (msg.getVersion() > existing.getVersion()) {
                    result.put(dedupKey, msg);
                    duplicatesRemoved++;
                    log.debug("Replaced message: dedupKey={}, old version={}, new version={}", 
                        dedupKey, existing.getVersion(), msg.getVersion());
                } else {
                    duplicatesRemoved++;
                    log.debug("Kept existing message: dedupKey={}, existing version={}, incoming version={}", 
                        dedupKey, existing.getVersion(), msg.getVersion());
                }
            }
        }
        
        if (duplicatesRemoved > 0) {
            log.info("Deduplication removed {} duplicate messages", duplicatesRemoved);
        }
        
        return result;
    }
    
    /**
     * Step 3: Query existing versions from Iceberg table.
     * 
     * Uses Iceberg Table Scan API with IN filter to retrieve existing versions
     * for all dedup_keys in the batch. Implements query chunking for safety
     * (10k keys per query).
     * 
     * @param dedupKeys Set of dedup_keys to query
     * @return Map of dedup_key to existing version
     */
    private Map<String, Long> queryExistingVersions(Set<String> dedupKeys) {
        if (dedupKeys.isEmpty()) {
            return Collections.emptyMap();
        }
        
        log.debug("Querying Iceberg for {} unique dedup_keys", dedupKeys.size());
        long queryStart = System.currentTimeMillis();
        
        try {
            Map<String, Long> result = new HashMap<>();
            
            // Chunk queries for safety (10k keys per query)
            int chunkSize = 10000;
            List<String> dedupKeyList = new ArrayList<>(dedupKeys);
            
            for (int i = 0; i < dedupKeyList.size(); i += chunkSize) {
                int end = Math.min(i + chunkSize, dedupKeyList.size());
                List<String> chunk = dedupKeyList.subList(i, end);
                
                // Build IN filter
                Expression filter = Expressions.in("dedup_key", chunk.toArray());
                
                // Execute scan
                try (CloseableIterable<org.apache.iceberg.data.Record> records = 
                        org.apache.iceberg.data.IcebergGenerics.read(table)
                            .where(filter)
                            .select("dedup_key", "version")
                            .build()) {
                    
                    for (org.apache.iceberg.data.Record record : records) {
                        String dedupKey = record.getField("dedup_key").toString();
                        Long version = (Long) record.getField("version");
                        result.put(dedupKey, version);
                    }
                }
            }
            
            long queryDuration = System.currentTimeMillis() - queryStart;
            log.debug("Query completed in {}ms, found {} existing versions", queryDuration, result.size());
            
            return result;
            
        } catch (IOException e) {
            log.error("Failed to query Iceberg table", e);
            throw new ConnectException("Failed to query existing versions", e);
        }
    }
    
    /**
     * Step 4: Apply version control rules and categorize messages.
     * 
     * Uses VersionControlState to determine action for each message based on
     * existing versions. Categorizes into toInsert, toUpdate, toDelete lists.
     * 
     * @param messages Deduplicated messages
     * @param existingVersions Map of dedup_key to existing version
     * @param toInsert Output list for INSERT operations
     * @param toUpdate Output list for UPDATE operations
     * @param toDelete Output list for DELETE operations (dedup_keys)
     */
    private void applyVersionControlRules(
        Map<String, ParsedMessage> messages,
        Map<String, Long> existingVersions,
        List<GenericRecord> toInsert,
        List<GenericRecord> toUpdate,
        List<String> toDelete
    ) {
        VersionControlState state = new VersionControlState(existingVersions);
        int skipped = 0;
        int errors = 0;
        
        for (ParsedMessage msg : messages.values()) {
            VersionControlState.Action action = state.determineAction(msg);
            
            switch (action) {
                case INSERT:
                    GenericRecord insertRecord = createIcebergRecord(msg);
                    toInsert.add(insertRecord);
                    break;
                    
                case UPDATE:
                    GenericRecord updateRecord = createIcebergRecord(msg);
                    toUpdate.add(updateRecord);
                    break;
                    
                case DELETE:
                    toDelete.add(msg.getDedupKey());
                    break;
                    
                case SKIP:
                    skipped++;
                    break;
                    
                case ERROR:
                    errors++;
                    break;
            }
        }
        
        if (skipped > 0) {
            log.info("Skipped {} stale messages", skipped);
        }
        if (errors > 0) {
            log.warn("Encountered {} version conflicts", errors);
        }
    }
    
    /**
     * Step 5: Execute batch write to Iceberg in single transaction.
     * 
     * Uses Iceberg Transaction API to ensure atomicity:
     * 1. Collect all dedup_keys to delete (UPDATE + DELETE)
     * 2. Execute single row-level delete using equality delete
     * 3. Collect all records to append (INSERT + UPDATE)
     * 4. Write Parquet file and append
     * 5. Commit entire transaction atomically
     * 
     * @param toInsert Records to insert
     * @param toUpdate Records to update (delete old + append new)
     * @param toDelete Dedup_keys to delete
     */
    private void batchWriteToIceberg(
        List<GenericRecord> toInsert,
        List<GenericRecord> toUpdate,
        List<String> toDelete
    ) {
        if (toInsert.isEmpty() && toUpdate.isEmpty() && toDelete.isEmpty()) {
            log.info("No operations to execute, skipping write");
            return;
        }
        
        log.info("Executing batch write: {} inserts, {} updates, {} deletes", 
            toInsert.size(), toUpdate.size(), toDelete.size());
        
        try {
            // Create transaction for atomic batch operations
            Transaction txn = table.newTransaction();
            
            // Step 1: Collect all dedup_keys to delete (UPDATE + DELETE)
            if (!toUpdate.isEmpty() || !toDelete.isEmpty()) {
                Set<String> dedupKeysToDelete = new HashSet<>();
                
                // Add dedup_keys from UPDATE (old versions to be replaced)
                toUpdate.stream()
                    .map(r -> r.getField("dedup_key").toString())
                    .forEach(dedupKeysToDelete::add);
                
                // Add dedup_keys from DELETE (records to be removed)
                dedupKeysToDelete.addAll(toDelete);
                
                // Single equality delete operation for all dedup_keys
                log.debug("Deleting {} records by dedup_key", dedupKeysToDelete.size());
                Expression deleteFilter = Expressions.in("dedup_key", dedupKeysToDelete.toArray());
                DeleteFiles delete = txn.newDelete();
                delete.deleteFromRowFilter(deleteFilter);
                delete.commit();
            }
            
            // Step 2: Collect all records to append (INSERT + UPDATE)
            List<GenericRecord> allToAppend = new ArrayList<>();
            allToAppend.addAll(toInsert);
            allToAppend.addAll(toUpdate);
            
            if (!allToAppend.isEmpty()) {
                log.debug("Appending {} records", allToAppend.size());
                DataFile dataFile = writeParquetFile(allToAppend);
                AppendFiles append = txn.newAppend();
                append.appendFile(dataFile);
                append.commit();
            }
            
            // Step 3: Commit entire transaction atomically
            txn.commitTransaction();
            
            log.info("Batch transaction committed successfully");
            
        } catch (IOException e) {
            log.error("Failed to write to Iceberg table: {}", config.getTableName(), e);
            throw new ConnectException("Iceberg write operation failed", e);
        } catch (CommitFailedException e) {
            log.error("Failed to commit Iceberg transaction: {}", config.getTableName(), e);
            throw new ConnectException("Iceberg commit failed", e);
        } catch (Exception e) {
            log.error("Unexpected error during batch write", e);
            throw new ConnectException("Batch write failed", e);
        }
    }
    
    /**
     * Write Parquet file with records.
     * 
     * @param records List of GenericRecords to write
     * @return DataFile reference
     * @throws IOException if write fails
     */
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
    
    /**
     * Create Iceberg GenericRecord from ParsedMessage.
     * 
     * Maps all fields from ParsedMessage to Iceberg schema:
     * - id: technical ID (UUID)
     * - dedup_key: "topic:key" format
     * - record: JSON string of data array
     * - ingest_time: from ngay_cap_nhat
     * - length: BIGINT
     * - key: business key field name
     * - type: operation type
     * - version: BIGINT
     * 
     * @param msg ParsedMessage to convert
     * @return GenericRecord for Iceberg
     */
    private GenericRecord createIcebergRecord(ParsedMessage msg) {
        GenericRecord record = GenericRecord.create(schema);
        record.setField("id", msg.getTechnicalId());
        record.setField("dedup_key", msg.getDedupKey());
        record.setField("record", msg.getRecordJson());
        record.setField("ingest_time", msg.getIngestTime());
        record.setField("length", msg.getLength());
        record.setField("key", msg.getBusinessKey());
        record.setField("type", msg.getOperationType());
        record.setField("version", msg.getVersion());
        return record;
    }
    
    /**
     * Create Hive Catalog with S3A configuration.
     * 
     * @return Configured Catalog instance
     */
    private Catalog createHiveCatalog() {
        Configuration conf = new Configuration();
        
        // Hive Metastore configuration
        conf.set("hive.metastore.uris", config.getCatalogUri());
        
        // S3A FileSystem configuration
        conf.set("fs.s3a.endpoint", config.getS3Endpoint());
        conf.set("fs.s3a.access.key", config.getS3AccessKey());
        conf.set("fs.s3a.secret.key", config.getS3SecretKey());
        conf.set("fs.s3a.path.style.access", String.valueOf(config.getS3PathStyleAccess()));
        conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        
        if (config.getS3Region() != null) {
            conf.set("fs.s3a.region", config.getS3Region());
        }
        
        log.info("Creating Hive Catalog: metastore={}, warehouse={}, s3endpoint={}", 
            config.getCatalogUri(), config.getCatalogWarehouse(), config.getS3Endpoint());
        
        return new HadoopCatalog(conf, config.getCatalogWarehouse());
    }
    
    /**
     * Parse table identifier from configuration.
     * 
     * Supports formats:
     * - "namespace.table" → TableIdentifier.of("namespace", "table")
     * - "table" → TableIdentifier.of("default", "table")
     * 
     * @param tableName Table name from configuration
     * @return TableIdentifier
     */
    private TableIdentifier parseTableIdentifier(String tableName) {
        String[] parts = tableName.split("\\.");
        if (parts.length == 2) {
            return TableIdentifier.of(parts[0], parts[1]);
        } else if (parts.length == 1) {
            return TableIdentifier.of("default", tableName);
        } else {
            throw new IllegalArgumentException("Invalid table name format: " + tableName + 
                ". Expected 'namespace.table' or 'table'");
        }
    }

    @Override
    public void stop() {
        log.info("VersionControlIcebergSinkTask stopped");
    }
}
