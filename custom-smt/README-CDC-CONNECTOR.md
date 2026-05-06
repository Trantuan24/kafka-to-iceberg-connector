# Version Control Iceberg Sink Connector

Custom Kafka Connect Sink Connector với CDC Version Control logic.

## Features

### 1. Deduplication
- Chọn message có **max(version)** trong batch theo batchKey
- Parse CHỈ envelope ngoài (không parse data[])
- Complexity: O(n)

### 2. Version Control
- **INSERT**: Key chưa tồn tại → insert, Key đã tồn tại → log warning + skip
- **UPDATE**: Version mới → update, Version cũ → skip, Key chưa có → upsert
- **DELETE**: Version mới → delete, Version cũ → skip, Key không tồn tại → skip

### 3. Batch Operations
- Query Iceberg 1 lần/batch (không query từng record)
- Batch INSERT/UPDATE/DELETE
- Efficient SQL operations

### 4. Error Handling
- Parse error → log error + skip
- INSERT conflict → log warning + skip
- Không cần DLQ topic (simple logging)

---

## Build

### Build JAR

```bash
cd custom-smt

# Build với Gradle
docker run --rm -v "$(pwd)":/project -w /project gradle:7.6-jdk11 gradle clean build

# Output: build/libs/custom-smt-1.0.0.jar
```

### Verify JAR

```bash
jar tf build/libs/custom-smt-1.0.0.jar | grep VersionControl
```

Expected output:
```
com/example/kafka/connect/iceberg/VersionControlIcebergSinkConnector.class
com/example/kafka/connect/iceberg/VersionControlIcebergSinkTask.class
com/example/kafka/connect/iceberg/VersionControlIcebergSinkConfig.class
```

---

## Deploy

### 1. Copy JAR to Connect

```bash
# Copy to plugins directory
docker cp custom-smt/build/libs/custom-smt-1.0.0.jar \
  iceberg-kafka-connect-demo-connect-1:/usr/share/java/custom-smt/
```

### 2. Restart Connect

```bash
docker-compose restart connect
```

### 3. Verify Connector Loaded

```bash
# Check logs
docker logs iceberg-kafka-connect-demo-connect-1 | grep VersionControl

# Expected:
# Loading plugin from: /usr/share/java/custom-smt/
# Registered loader: PluginClassLoader{pluginLocation=file:/usr/share/java/custom-smt/}
```

---

## Configuration

### Connector Config (JSON for REST API)

```json
{
  "name": "sink.tram_quan_trac_cdc",
  "config": {
    "connector.class": "com.example.kafka.connect.iceberg.VersionControlIcebergSinkConnector",
    "tasks.max": "1",
    "topics": "tram_quan_trac",
    
    "iceberg.table.name": "iceberg.default.tram_quan_trac",
    "iceberg.jdbc.url": "jdbc:trino://trino:8080/iceberg",
    
    "consumer.max.poll.records": "50000",
    "offset.flush.interval.ms": "30000",
    
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    
    "errors.tolerance": "all",
    "errors.log.enable": "true",
    "errors.log.include.messages": "true"
  }
}
```

### Connector Config (Properties for Standalone)

```properties
name=sink.tram_quan_trac_cdc
connector.class=com.example.kafka.connect.iceberg.VersionControlIcebergSinkConnector
tasks.max=1
topics=tram_quan_trac

# Iceberg config
iceberg.table.name=iceberg.default.tram_quan_trac
iceberg.jdbc.url=jdbc:trino://trino:8080/iceberg

# Buffer config
consumer.max.poll.records=50000
offset.flush.interval.ms=30000

# Converters
value.converter=org.apache.kafka.connect.json.JsonConverter
value.converter.schemas.enable=false
key.converter=org.apache.kafka.connect.storage.StringConverter

# Error handling
errors.tolerance=all
errors.log.enable=true
errors.log.include.messages=true
```

---

## Load Connector

### Option 1: REST API (Distributed Mode)

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @configs/sink.tram_quan_trac_cdc.json
```

### Option 2: Standalone Mode

Update `docker-compose.yml`:

```yaml
connect:
  command: >
    bash -c "
      connect-standalone 
        /etc/kafka/connect-standalone.properties 
        /etc/kafka/sink.y_te_831_v3.properties
        /etc/kafka/sink.tram_quan_trac_cdc.properties
    "
```

---

## Testing

### 1. Send Test Messages

```python
from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Test INSERT
message = {
    "data": [
        {"MaTram": "TRAM001", "TenTram": "Tram 1"},
        {"MaTram": "TRAM002", "TenTram": "Tram 2"}
    ],
    "length": 2,
    "key": "MaTram",
    "type": "INSERT",
    "version": 1,
    "ngay_cap_nhat": "2026-05-05T10:00:00Z"
}

producer.send('tram_quan_trac', value=message)
producer.flush()
```

### 2. Check Logs

```bash
# Check connector logs
docker logs -f iceberg-kafka-connect-demo-connect-1 | grep VersionControl

# Expected output:
# Processing batch: 1 records
# After deduplication: 1 messages (from 1 original)
# Queried 0 existing records from Iceberg
# Actions: 2 inserts, 0 updates, 0 deletes, 0 skipped, 0 errors
# Inserted 2 rows
# Batch processed in 123ms
```

### 3. Query Iceberg

```bash
docker exec iceberg-kafka-connect-demo-trino-1 trino --execute \
  "SELECT * FROM iceberg.default.tram_quan_trac"
```

---

## Monitoring

### Key Metrics

```bash
# Count processed batches
docker logs iceberg-kafka-connect-demo-connect-1 | \
  grep "Batch processed" | wc -l

# Count errors
docker logs iceberg-kafka-connect-demo-connect-1 | \
  grep "ERROR.*VersionControl" | wc -l

# Count INSERT conflicts
docker logs iceberg-kafka-connect-demo-connect-1 | \
  grep "INSERT conflict" | wc -l

# Check last batch stats
docker logs iceberg-kafka-connect-demo-connect-1 | \
  grep "Actions:" | tail -1
```

### Alert Conditions

1. **High error rate**
   ```bash
   errors / total_records > 5%
   ```

2. **INSERT conflicts**
   ```bash
   grep "INSERT conflict" logs | wc -l > 100
   ```

3. **Parse errors**
   ```bash
   grep "Failed to parse JSON" logs | wc -l > 50
   ```

---

## Troubleshooting

### Issue 1: Connector Not Loading

**Symptom:**
```
Connector class not found
```

**Solution:**
```bash
# Check JAR exists
docker exec iceberg-kafka-connect-demo-connect-1 \
  ls -la /usr/share/java/custom-smt/

# Check class in JAR
docker exec iceberg-kafka-connect-demo-connect-1 \
  jar tf /usr/share/java/custom-smt/custom-smt-1.0.0.jar | grep VersionControl

# Restart Connect
docker-compose restart connect
```

### Issue 2: JDBC Connection Failed

**Symptom:**
```
Failed to connect to Iceberg
```

**Solution:**
```bash
# Check Trino is running
docker ps | grep trino

# Test connection
docker exec iceberg-kafka-connect-demo-trino-1 \
  trino --execute "SELECT 1"

# Check JDBC URL in config
# Should be: jdbc:trino://trino:8080/iceberg
```

### Issue 3: INSERT Fails

**Symptom:**
```
Error executing inserts: Table not found
```

**Solution:**
```bash
# Check table exists
docker exec iceberg-kafka-connect-demo-trino-1 trino --execute \
  "SHOW TABLES FROM iceberg.default"

# Check table schema
docker exec iceberg-kafka-connect-demo-trino-1 trino --execute \
  "DESCRIBE iceberg.default.tram_quan_trac"

# Expected columns:
# id, record, version, type, key, ngay_cap_nhat, length
```

### Issue 4: High Memory Usage

**Symptom:**
```
OutOfMemoryError
```

**Solution:**
```properties
# Reduce batch size
consumer.max.poll.records=10000

# Increase flush interval
offset.flush.interval.ms=60000
```

---

## Performance Tuning

### Batch Size

```properties
# Start conservative
consumer.max.poll.records=10000

# Monitor lag and memory
# If lag increases → increase batch size
# If memory issues → decrease batch size
# If write latency high → decrease batch size

# Production tuning
consumer.max.poll.records=50000
```

### Flush Interval

```properties
# POC
offset.flush.interval.ms=30000  # 30 seconds

# Production
offset.flush.interval.ms=60000  # 60 seconds
```

### Connection Pooling

For high throughput, consider adding HikariCP:

```gradle
implementation 'com.zaxxer:HikariCP:5.0.1'
```

---

## Schema Requirements

### Iceberg Table Schema

```sql
CREATE TABLE iceberg.default.tram_quan_trac (
    id VARCHAR,              -- Primary key (e.g., TRAM001)
    record VARCHAR,          -- JSON string of data item
    version BIGINT,          -- Version number for comparison
    type VARCHAR,            -- INSERT/UPDATE/DELETE
    key VARCHAR,             -- Key field name (e.g., "MaTram")
    ngay_cap_nhat VARCHAR,   -- Timestamp
    length VARCHAR           -- Array length
) USING iceberg;
```

### Message Format

```json
{
  "data": [
    {
      "MaTram": "TRAM001",
      "TenTram": "Tram 1",
      "...": "..."
    }
  ],
  "length": 1,
  "key": "MaTram",
  "type": "INSERT",
  "version": 1,
  "ngay_cap_nhat": "2026-05-05T10:00:00Z"
}
```

---

## Next Steps

1. ✅ Build and deploy connector
2. ✅ Test with sample data
3. ⏳ Performance tuning
4. ⏳ Production deployment
5. ⏳ Monitoring setup

---

## References

- [Kafka Connect Documentation](https://kafka.apache.org/documentation/#connect)
- [Trino JDBC Driver](https://trino.io/docs/current/client/jdbc.html)
- [Apache Iceberg](https://iceberg.apache.org/)
