-- Create CDC Version Control Iceberg Table
-- 
-- This table stores CDC messages with version control at the message level.
-- The entire data array from each CDC message is stored as a JSON string.
--
-- Key fields:
-- - id: Technical identifier (UUID or topic-partition-offset)
-- - dedup_key: Composite business key in format "topic:key" where key is field name
--   Example: "tram_quan_trac:MaTram" (NOT "tram_quan_trac:TQ001")
-- - record: JSON string representation of the entire data array
-- - version: BIGINT timestamp for version control and ordering
--
-- Table properties:
-- - format-version=2: Required for row-level delete operations (equality delete)
-- - write.delete.mode=merge-on-read: Efficient delete strategy
-- - write.update.mode=merge-on-read: Efficient update strategy

CREATE TABLE IF NOT EXISTS default.tram_quan_trac_cdc (
  id STRING COMMENT 'Technical identifier (UUID or topic-partition-offset)',
  dedup_key STRING COMMENT 'Composite business key: topic:key (e.g., tram_quan_trac:MaTram)',
  record STRING COMMENT 'JSON string of entire data array from CDC message',
  ingest_time STRING COMMENT 'Ingestion timestamp from ngay_cap_nhat field',
  length BIGINT COMMENT 'Number of items in original data array',
  key STRING COMMENT 'Business key field name (e.g., MaTram)',
  type STRING COMMENT 'CDC operation type: INSERT, UPDATE, or DELETE',
  version BIGINT COMMENT 'Version timestamp for ordering and conflict resolution'
)
USING iceberg
TBLPROPERTIES (
  'format-version' = '2',
  'write.delete.mode' = 'merge-on-read',
  'write.update.mode' = 'merge-on-read',
  'write.merge.mode' = 'merge-on-read'
);

-- Verify table properties
DESCRIBE EXTENDED default.tram_quan_trac_cdc;
