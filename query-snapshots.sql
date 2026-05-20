-- Query snapshot metadata for def.abc
-- Verify: connector.name + typeingest present
SELECT
  snapshot_id,
  committed_at,
  element_at(summary, 'connector.name') AS connector_name,
  element_at(summary, 'typeingest') AS typeingest,
  element_at(summary, 'kafka.connect.commit-id') AS commit_id,
  element_at(summary, 'added-records') AS added_records,
  element_at(summary, 'total-records') AS total_records
FROM iceberg.def."abc$snapshots"
ORDER BY committed_at DESC
LIMIT 5;
