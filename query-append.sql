-- Verify APPEND mode data (3 columns)
SELECT id, substr(record, 1, 60) AS record_head, length(record) AS record_len, ngay_cap_nhat
FROM iceberg.def.abc_append
ORDER BY id;

-- Verify snapshot lineage metadata (Consumer Engine Standard)
SELECT
  snapshot_id,
  committed_at,
  element_at(summary, 'task.engine')            AS task_engine,
  element_at(summary, 'consumer.typeingest')     AS consumer_typeingest,
  element_at(summary, 'consumer.connectorname')  AS consumer_connectorname,
  element_at(summary, 'consumer.ingest.time')    AS consumer_ingest_time
FROM iceberg.def."abc_append$snapshots"
ORDER BY committed_at DESC;
