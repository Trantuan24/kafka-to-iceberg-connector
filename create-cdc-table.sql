-- ================================================
-- Iceberg Table: tram_quan_trac_cdc
-- Phase 1: Append-only mode, 7 fields
-- ================================================
--
-- This table stores CDC messages at the message/batch level.
-- The entire data array from each CDC message is stored as a JSON string in 'record'.
--
-- Schema (7 fields):
--   id         VARCHAR   Technical identifier (topic-partition-offset)
--   record     VARCHAR   JSON string of entire data[] array
--   version    BIGINT    Version for ordering (larger = newer)
--   type       VARCHAR   CDC operation: INSERT, UPDATE, DELETE
--   key        VARCHAR   Business key field name (e.g., MaTram)
--   ngay_cap_nhat VARCHAR   Update timestamp from source
--   length     VARCHAR   Number of items in original data array
--
-- NOT included in Phase 1:
--   dedup_key    (not needed for single-table append)
--   ingest_time  (using ngay_cap_nhat instead)
--   _cdc         (not needed for append mode, only for upsert/CDC)

-- Drop existing table if schema doesn't match
-- Uncomment if needed:
-- DROP TABLE IF EXISTS iceberg.default.tram_quan_trac_cdc;

CREATE TABLE IF NOT EXISTS iceberg.default.tram_quan_trac_cdc (
    id VARCHAR COMMENT 'Technical identifier: topic-partition-offset',
    record VARCHAR COMMENT 'JSON string of entire data[] array from CDC message',
    version BIGINT COMMENT 'Version number for ordering (larger = newer)',
    type VARCHAR COMMENT 'CDC operation type: INSERT, UPDATE, or DELETE',
    key VARCHAR COMMENT 'Business key field name (e.g., MaTram)',
    ngay_cap_nhat VARCHAR COMMENT 'Update timestamp from source system',
    length VARCHAR COMMENT 'Number of items in original data array'
) WITH (
    format = 'PARQUET'
);

-- Verify table schema
DESCRIBE iceberg.default.tram_quan_trac_cdc;
