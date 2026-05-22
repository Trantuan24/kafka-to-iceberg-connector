-- Verify snapshot metadata theo chuẩn mới (consumer engine)
SELECT
  snapshot_id,
  committed_at,
  element_at(summary, 'task.engine')             AS task_engine,
  element_at(summary, 'consumer.typeingest')      AS consumer_typeingest,
  element_at(summary, 'consumer.connectorname')   AS consumer_connectorname,
  element_at(summary, 'consumer.ingest.time')     AS consumer_ingest_time,
  element_at(summary, 'consumer.vtts.time')       AS consumer_vtts_time,
  element_at(summary, 'kafka.connect.commit-id')  AS kc_commit_id,
  element_at(summary, 'added-records')            AS added_records,
  element_at(summary, 'total-records')            AS total_records
FROM iceberg.def."abc$snapshots"
ORDER BY committed_at DESC
LIMIT 5;
