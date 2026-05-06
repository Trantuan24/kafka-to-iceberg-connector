#!/bin/bash
# ================================================
# Deploy Custom SMT + Official Apache Iceberg Sink Connector
# Phase 2: Upsert mode, 9 fields (id-columns = dedup_key)
# ================================================

set -e

CONNECT_CONTAINER="iceberg-kafka-connect-demo-connect-1"
TRINO_CONTAINER="iceberg-kafka-connect-demo-trino-1"
CONNECTOR_NAME="sink.tram_quan_trac_cdc_v2"
CONNECT_URL="http://localhost:8083"

echo "=========================================="
echo "Deploy Phase 2: Custom SMT + Official Connector"
echo "Upsert mode, 9 fields, format-version=2"
echo "=========================================="

# Step 1: Build SMT JAR
echo ""
echo "Step 1: Building Custom SMT JAR..."
if command -v docker &> /dev/null; then
    docker run --rm -v "$(pwd)/custom-smt":/project -w /project gradle:8.5-jdk11 gradle clean build -x test
    JAR_FILE=$(find custom-smt/build/libs -name "*.jar" -not -name "*test*" | head -1)
    echo "✅ JAR built: $JAR_FILE"
    JAR_SIZE=$(du -h "$JAR_FILE" | cut -f1)
    echo "   JAR size: $JAR_SIZE (should be ~2MB, NOT 400MB+)"
else
    echo "❌ Docker not found. Please install Docker."
    exit 1
fi

# Step 2: Copy JAR to plugins directory (for Docker build)
echo ""
echo "Step 2: Copying JAR to plugins/custom-smt/..."
mkdir -p plugins/custom-smt
cp "$JAR_FILE" plugins/custom-smt/
echo "✅ JAR copied to plugins/custom-smt/"

# Step 3: Also copy to running container (hot deploy)
echo ""
echo "Step 3: Hot-deploying JAR to running container..."
if docker ps --format '{{.Names}}' | grep -q "$CONNECT_CONTAINER"; then
    docker exec "$CONNECT_CONTAINER" mkdir -p /usr/share/java/custom-smt/ || true
    docker cp "$JAR_FILE" "$CONNECT_CONTAINER":/usr/share/java/custom-smt/
    echo "✅ JAR copied to container"
    
    echo "⏳ Restarting Kafka Connect to load new plugin..."
    docker-compose restart connect
    echo "⏳ Waiting 40 seconds for Connect to start..."
    sleep 40
else
    echo "⚠️  Container $CONNECT_CONTAINER not running."
    echo "   Run 'docker-compose up -d --build' first."
    echo "   JAR is in plugins/custom-smt/ and will be included in next build."
    exit 1
fi

# Step 4: Wait for Connect REST API to be ready
echo ""
echo "Step 4: Waiting for Connect REST API..."
for i in $(seq 1 30); do
    if curl -s "$CONNECT_URL/connectors" > /dev/null 2>&1; then
        echo "✅ Connect REST API is ready"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "❌ Connect REST API not responding after 30 attempts"
        exit 1
    fi
    echo "   Attempt $i/30..."
    sleep 2
done

# Step 5: Verify plugin loaded
echo ""
echo "Step 5: Verifying plugins loaded..."
PLUGINS=$(curl -s "$CONNECT_URL/connector-plugins" | python3 -c "
import json, sys
plugins = json.load(sys.stdin)
for p in plugins:
    cls = p.get('class', '')
    if 'Iceberg' in cls or 'CustomCDC' in cls:
        print(f'  ✅ {cls}')
" 2>/dev/null || echo "  ⚠️  Could not parse plugins response")
echo "$PLUGINS"

# Step 6: Drop old table if exists (Phase 2 fresh start)
echo ""
echo "Step 6: Preparing Iceberg table v2..."
if docker ps --format '{{.Names}}' | grep -q "$TRINO_CONTAINER"; then
    echo "   Dropping old table (if exists with wrong schema)..."
    docker exec "$TRINO_CONTAINER" trino --execute \
        "DROP TABLE IF EXISTS iceberg.default.tram_quan_trac_cdc_v2" 2>/dev/null || true
    
    echo "   Creating table with 9-field schema and format-version=2..."
    docker exec "$TRINO_CONTAINER" trino --execute "
CREATE TABLE IF NOT EXISTS iceberg.default.tram_quan_trac_cdc_v2 (
    id VARCHAR,
    dedup_key VARCHAR,
    record VARCHAR,
    version BIGINT,
    type VARCHAR,
    key VARCHAR,
    ngay_cap_nhat VARCHAR,
    length VARCHAR,
    _cdc ROW(op VARCHAR),
    _cdc_op VARCHAR
) WITH (
    format = 'PARQUET',
    format_version = 2
)
" 2>/dev/null || echo "   Table may already exist or auto-create enabled"
    
    echo "   Verifying table schema..."
    docker exec "$TRINO_CONTAINER" trino --execute \
        "DESCRIBE iceberg.default.tram_quan_trac_cdc_v2" 2>/dev/null || echo "   ⚠️  Could not describe table"
    echo "✅ Table ready"
else
    echo "⚠️  Trino container not running. Table will be auto-created by connector."
fi

# Step 7: Delete existing connector if any
echo ""
echo "Step 7: Cleaning up existing connector..."
EXISTING=$(curl -s "$CONNECT_URL/connectors" | grep -o "$CONNECTOR_NAME" || echo "")
if [ ! -z "$EXISTING" ]; then
    echo "   Deleting existing connector: $CONNECTOR_NAME"
    curl -s -X DELETE "$CONNECT_URL/connectors/$CONNECTOR_NAME"
    sleep 3
    echo "   ✅ Deleted"
else
    echo "   No existing connector found"
fi

# Step 8: Deploy connector via REST API
echo ""
echo "Step 8: Deploying connector via REST API..."
RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "$CONNECT_URL/connectors" \
    -H "Content-Type: application/json" \
    -d @configs/sink.tram_quan_trac_cdc_v2.json)

HTTP_CODE=$(echo "$RESPONSE" | tail -1)
BODY=$(echo "$RESPONSE" | head -n -1)

if [ "$HTTP_CODE" = "201" ] || [ "$HTTP_CODE" = "200" ]; then
    echo "✅ Connector created successfully (HTTP $HTTP_CODE)"
else
    echo "❌ Failed to create connector (HTTP $HTTP_CODE)"
    echo "$BODY" | python3 -m json.tool 2>/dev/null || echo "$BODY"
    exit 1
fi

# Step 9: Wait and check connector status
echo ""
echo "Step 9: Checking connector status..."
sleep 8
STATUS=$(curl -s "$CONNECT_URL/connectors/$CONNECTOR_NAME/status")
echo "$STATUS" | python3 -m json.tool 2>/dev/null || echo "$STATUS"

CONNECTOR_STATE=$(echo "$STATUS" | python3 -c "
import json, sys
s = json.load(sys.stdin)
print(s.get('connector', {}).get('state', 'UNKNOWN'))
" 2>/dev/null || echo "UNKNOWN")

TASK_STATE=$(echo "$STATUS" | python3 -c "
import json, sys
s = json.load(sys.stdin)
tasks = s.get('tasks', [])
if tasks:
    print(tasks[0].get('state', 'UNKNOWN'))
else:
    print('NO_TASKS')
" 2>/dev/null || echo "UNKNOWN")

echo ""
if [ "$CONNECTOR_STATE" = "RUNNING" ] && [ "$TASK_STATE" = "RUNNING" ]; then
    echo "✅ Connector RUNNING, Task RUNNING"
else
    echo "⚠️  Connector=$CONNECTOR_STATE, Task=$TASK_STATE"
    echo "   Check logs: docker logs $CONNECT_CONTAINER 2>&1 | tail -50"
fi

echo ""
echo "=========================================="
echo "Deployment Complete!"
echo "=========================================="
echo ""
echo "📋 NEXT STEPS:"
echo "1. Send test message:"
echo "   python3 test-custom-smt-v2.py"
echo ""
echo "2. Monitor connector logs:"
echo "   docker logs -f $CONNECT_CONTAINER 2>&1 | grep -i 'customcdc\|iceberg\|error\|committed'"
echo ""
echo "3. Query table:"
echo "   docker exec $TRINO_CONTAINER trino --execute \\"
echo "     \"SELECT * FROM iceberg.default.tram_quan_trac_cdc_v2\""
