-- Xem checkpoint (offset da commit) cua Iceberg connector
-- Key: kafka.connect.offsets.control-iceberg.cg-control-sink-qtmt-tramquantrac
SELECT
  snapshot_id,
  committed_at,
  element_at(summary, 'kafka.connect.offsets.control-iceberg.cg-control-sink-qtmt-tramquantrac') AS committed_offsets,
  element_at(summary, 'kafka.connect.commit-id') AS commit_id,
  element_at(summary, 'total-records') AS total_records
FROM iceberg.def."abc$snapshots"
ORDER BY committed_at DESC
LIMIT 5;
