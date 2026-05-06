#!/bin/bash
# ================================================
# Check Phase 1 Results: Append Mode Pipeline
# 7 fields: id, record, version, type, key, ngay_cap_nhat, length
# ================================================

CONNECT_CONTAINER="iceberg-kafka-connect-demo-connect-1"
TRINO_CONTAINER="iceberg-kafka-connect-demo-trino-1"
CONNECTOR_NAME="sink.tram_quan_trac_cdc_official"
CONNECT_URL="http://localhost:8083"

echo "=========================================="
echo "Phase 1 Pipeline Check"
echo "=========================================="

# 1. Connector Status
echo ""
echo "1. Connector Status:"
echo "-------------------"
STATUS=$(curl -s "$CONNECT_URL/connectors/$CONNECTOR_NAME/status" 2>/dev/null)
if [ $? -eq 0 ] && [ ! -z "$STATUS" ]; then
    echo "$STATUS" | python3 -m json.tool 2>/dev/null || echo "$STATUS"
else
    echo "❌ Cannot reach Connect REST API at $CONNECT_URL"
fi

# 2. Recent connector logs (SMT + Iceberg)
echo ""
echo ""
echo "2. Recent SMT/Iceberg Logs (last 30 lines):"
echo "-------------------"
docker logs --tail 100 "$CONNECT_CONTAINER" 2>&1 | grep -i "customcdc\|iceberg\|committed\|error\|exception" | tail -30 || echo "No matching logs"

# 3. Consumer group info
echo ""
echo ""
echo "3. Consumer Group (offset/lag):"
echo "-------------------"
docker exec kafka kafka-consumer-groups --bootstrap-server kafka:9092 --describe --all-groups 2>/dev/null || echo "Could not get consumer group info"

# 4. Table schema
echo ""
echo ""
echo "4. Table Schema:"
echo "-------------------"
docker exec "$TRINO_CONTAINER" trino --execute "DESCRIBE iceberg.default.tram_quan_trac_cdc" 2>/dev/null || echo "Could not describe table"

# 5. Record count
echo ""
echo ""
echo "5. Record Count:"
echo "-------------------"
COUNT=$(docker exec "$TRINO_CONTAINER" trino --execute "SELECT COUNT(*) FROM iceberg.default.tram_quan_trac_cdc" 2>/dev/null)
echo "Total rows: $COUNT"

# 6. Table contents
echo ""
echo ""
echo "6. Table Contents (all rows):"
echo "-------------------"
docker exec "$TRINO_CONTAINER" trino --execute "
SELECT 
    id,
    key,
    type,
    version,
    ngay_cap_nhat,
    length,
    SUBSTR(record, 1, 80) as record_preview
FROM iceberg.default.tram_quan_trac_cdc 
ORDER BY version
" 2>/dev/null || echo "Could not query table"

# 7. Count by type
echo ""
echo ""
echo "7. Records by Type:"
echo "-------------------"
docker exec "$TRINO_CONTAINER" trino --execute "
SELECT 
    type,
    COUNT(*) as count
FROM iceberg.default.tram_quan_trac_cdc 
GROUP BY type
ORDER BY type
" 2>/dev/null || echo "No data or table does not exist"

# 8. Check Parquet files in MinIO
echo ""
echo ""
echo "8. Parquet Files in MinIO:"
echo "-------------------"
docker exec iceberg-kafka-connect-demo-minio-1 find /data/bucket/warehouse -name "*.parquet" -type f 2>/dev/null | head -10 || \
    docker exec minio find /data/bucket/warehouse -name "*.parquet" -type f 2>/dev/null | head -10 || \
    echo "Could not check MinIO files (container might have different name)"

# 9. Summary
echo ""
echo ""
echo "=========================================="
echo "Summary"
echo "=========================================="

# Check connector state
CONN_STATE=$(echo "$STATUS" | python3 -c "
import json, sys
try:
    s = json.load(sys.stdin)
    conn = s.get('connector', {}).get('state', 'UNKNOWN')
    tasks = s.get('tasks', [])
    task = tasks[0].get('state', 'UNKNOWN') if tasks else 'NO_TASKS'
    print(f'Connector={conn}, Task={task}')
except:
    print('UNKNOWN')
" 2>/dev/null || echo "UNKNOWN")

echo "Connector: $CONN_STATE"
echo "Row count: $COUNT"

if echo "$CONN_STATE" | grep -q "RUNNING.*RUNNING"; then
    if [ ! -z "$COUNT" ] && [ "$COUNT" != "\"0\"" ] && [ "$COUNT" != "0" ]; then
        echo ""
        echo "✅ Phase 1 pipeline is WORKING!"
        echo "   - Connector RUNNING"
        echo "   - Data flowing to Iceberg"
    else
        echo ""
        echo "⚠️  Connector RUNNING but no data in table yet."
        echo "   - Send test messages: python3 test-custom-smt.py"
        echo "   - Wait for commit interval (10s)"
        echo "   - Check logs for 'committed to' messages"
    fi
else
    echo ""
    echo "❌ Pipeline NOT healthy"
    echo "   Check: docker logs $CONNECT_CONTAINER 2>&1 | tail -50"
fi

echo ""
echo "=========================================="
echo "Check Complete"
echo "=========================================="
