-- Verify APPEND API-push data (9 table columns)
SELECT
  loainguon,
  manguondulieu,
  sukien,
  phienban,
  body,
  header,
  substr(data, 1, 60) AS data_head,
  length(data) AS data_len,
  ingest_date,
  ingest_time
FROM iceberg.def.abc_append_v2
ORDER BY manguondulieu;

-- Verify snapshot lineage metadata (Consumer Engine Standard)
SELECT
  snapshot_id,
  committed_at,
  element_at(summary, 'task.engine')            AS task_engine,
  element_at(summary, 'consumer.typeingest')     AS consumer_typeingest,
  element_at(summary, 'consumer.connectorname')  AS consumer_connectorname,
  element_at(summary, 'consumer.ingest.time')    AS consumer_ingest_time
FROM iceberg.def."abc_append_v2$snapshots"
ORDER BY committed_at DESC;