# Custom SMT + Official Apache Iceberg Sink Connector

**Approach**: Custom SMT Transform → Official Apache Iceberg Sink Connector  
**Status**: ✅ Implemented - Ready for Testing  
**Date**: 2026-05-05

---

## 📋 Overview

Thay vì build full custom Kafka Connect Sink Connector, phương án này sử dụng:
1. **Custom SMT** - Transform CDC message format
2. **Official Apache Iceberg Sink Connector** - Write to Iceberg

### Ưu điểm:
- ✅ **Ít code hơn nhiều** (~300 lines vs ~1500 lines)
- ✅ **Tận dụng official connector** (proven, tested, maintained)
- ✅ **Exactly-once semantics** (KIP-447)
- ✅ **Commit coordination** (control topic)
- ✅ **Schema evolution** (auto-create, evolve-schema)
- ✅ **Upsert/CDC support** (id-columns, _cdc metadata)

### Nhược điểm:
- ⚠️ **Version ordering** - Dựa vào Kafka offset order, KHÔNG có max(version) dedup trong batch
- ⚠️ **Assumption** - Messages phải đến theo thứ tự version tăng dần

---

## 🔧 Implementation

### 1. Custom SMT Transform

**File**: `custom-smt/src/main/java/com/example/kafka/connect/smt/CustomCDCTransform.java`

**Chức năng**:
- Transform custom CDC envelope thành Iceberg-compatible format
- Generate `id` (UUID)
- Construct `dedup_key` = "topic:key" (key là field name)
- Stringify `data[]` thành `record` (JSON string)
- Map `type` → `_cdc.op` (INSERT→c, UPDATE→u, DELETE→d)
- Copy các fields khác: ingest_time, length, key, type, version

**Input** (CDC message):
```json
{
  "data": [{"MaTram": "TQ001", "TenTram": "Tram 1"}],
  "key": "MaTram",
  "type": "INSERT",
  "version": 1704067200000,
  "ngay_cap_nhat": "2024-01-01T00:00:00Z",
  "length": 1
}
```

**Output** (Iceberg-compatible):
```json
{
  "id": "uuid-12345",
  "dedup_key": "tram_quan_trac:MaTram",
  "record": "[{\"MaTram\":\"TQ001\",\"TenTram\":\"Tram 1\"}]",
  "ingest_time": "2024-01-01T00:00:00Z",
  "length": 1,
  "key": "MaTram",
  "type": "INSERT",
  "version": 1704067200000,
  "_cdc": {
    "op": "c",
    "ts": 1704067200000
  }
}
```

### 2. Official Connector Config

**File**: `configs/sink.tram_quan_trac_cdc_official.json`

**Key configs**:
- `connector.class`: `org.apache.iceberg.connect.IcebergSinkConnector`
- `iceberg.tables.default-id-columns`: `dedup_key` (primary key for upsert)
- `transforms`: `customCdc` (apply custom SMT)
- `transforms.customCdc.type`: `com.example.kafka.connect.smt.CustomCDCTransform`

---

## 🚀 Deployment

### Step 1: Build Custom SMT

```bash
cd custom-smt
./gradlew clean build
```

**Output**: `custom-smt/build/libs/cdc-version-control-connector-2.0.0.jar`

### Step 2: Deploy JAR to Kafka Connect

```bash
# Copy JAR to Kafka Connect plugins directory
cp custom-smt/build/libs/cdc-version-control-connector-2.0.0.jar \
   /usr/share/java/kafka-connect-iceberg/
```

**Note**: JAR chứa cả Custom SMT và Custom Connector (backup)

### Step 3: Download Official Apache Iceberg Sink Connector

```bash
# Download from Apache Iceberg releases
# https://iceberg.apache.org/releases/

# Or use Confluent Hub
confluent-hub install apache/kafka-connect-iceberg:latest
```

### Step 4: Create Iceberg Table

```sql
CREATE TABLE default.tram_quan_trac_cdc (
  id          STRING,
  dedup_key   STRING,
  record      STRING,
  ingest_time STRING,
  length      BIGINT,
  key         STRING,
  type        STRING,
  version     BIGINT,
  _cdc        STRUCT<op: STRING, ts: BIGINT>
)
USING iceberg
TBLPROPERTIES (
  'format-version' = '2',
  'write.delete.mode' = 'merge-on-read',
  'write.update.mode' = 'merge-on-read',
  'write.merge.mode' = 'merge-on-read'
);
```

**Note**: Schema bao gồm `_cdc` struct cho CDC metadata

### Step 5: Register Connector

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @configs/sink.tram_quan_trac_cdc_official.json
```

### Step 6: Verify Connector Status

```bash
curl http://localhost:8083/connectors/sink.tram_quan_trac_cdc_official/status
```

---

## 🧪 Testing

### Test 1: Basic INSERT

**Send message**:
```json
{
  "data": [{"MaTram": "TQ001", "TenTram": "Tram 1"}],
  "key": "MaTram",
  "type": "INSERT",
  "version": 1000,
  "ngay_cap_nhat": "2024-01-01T00:00:00Z",
  "length": 1
}
```

**Expected in Iceberg**:
```sql
SELECT * FROM default.tram_quan_trac_cdc;
-- Should have 1 record with dedup_key = "tram_quan_trac:MaTram"
```

### Test 2: UPDATE (Upsert)

**Send message**:
```json
{
  "data": [{"MaTram": "TQ001", "TenTram": "Tram 1 Updated"}],
  "key": "MaTram",
  "type": "UPDATE",
  "version": 2000,
  "ngay_cap_nhat": "2024-01-01T01:00:00Z",
  "length": 1
}
```

**Expected in Iceberg**:
```sql
SELECT * FROM default.tram_quan_trac_cdc;
-- Should still have 1 record (upserted)
-- record field should contain "Tram 1 Updated"
-- version should be 2000
```

### Test 3: DELETE

**Send message**:
```json
{
  "data": [{"MaTram": "TQ001"}],
  "key": "MaTram",
  "type": "DELETE",
  "version": 3000,
  "ngay_cap_nhat": "2024-01-01T02:00:00Z",
  "length": 1
}
```

**Expected in Iceberg**:
```sql
SELECT * FROM default.tram_quan_trac_cdc;
-- Should have 0 records (deleted)
```

### Test 4: Version Ordering (CRITICAL)

**Send messages OUT OF ORDER**:
```json
// Message 1: version=1000
{"data": [...], "key": "MaTram", "type": "INSERT", "version": 1000, ...}

// Message 2: version=3000
{"data": [...], "key": "MaTram", "type": "UPDATE", "version": 3000, ...}

// Message 3: version=2000 (OLDER than message 2)
{"data": [...], "key": "MaTram", "type": "UPDATE", "version": 2000, ...}
```

**Expected behavior**:
- Official connector uses **Kafka offset order** (last write wins)
- Message 3 (version=2000) will OVERWRITE message 2 (version=3000)
- **This is WRONG if we need max(version) logic**

**Actual result**: Need to test to confirm

**If this fails** → Need to use Custom Connector (đã implement) hoặc add Kafka Streams pre-processing

---

## ⚠️ Known Limitations

### 1. Version Ordering

**Issue**: Official connector KHÔNG có max(version) dedup logic

**Workarounds**:
- **Option A**: Ensure Kafka messages arrive in version order (upstream guarantee)
- **Option B**: Add Kafka Streams pre-processing to deduplicate by max(version)
- **Option C**: Use Custom Connector (đã implement trong backup)

### 2. CDC Metadata Format

**Issue**: `_cdc.op` values cần match với official connector expectations

**Current mapping**:
- INSERT → "c" (create)
- UPDATE → "u" (update)
- DELETE → "d" (delete)

**Need to verify**: Check official connector source code hoặc test thực tế

### 3. Schema Evolution

**Issue**: Nếu `data[]` structure thay đổi, `record` field (JSON string) không tự động evolve

**Workaround**: `record` là STRING nên không bị ảnh hưởng, nhưng query engine phải parse JSON

---

## 📊 Comparison: Custom SMT vs Custom Connector

| Feature | Custom SMT + Official | Custom Connector |
|---------|----------------------|------------------|
| Lines of code | ~300 | ~1500 |
| Complexity | Low | High |
| Maintenance | Low (official maintained) | High (self-maintained) |
| Exactly-once | ✅ Built-in | ✅ Implemented |
| Commit coordination | ✅ Built-in | ✅ Implemented |
| Schema evolution | ✅ Built-in | ✅ Implemented |
| **Version ordering** | ❌ Kafka order only | ✅ max(version) |
| Upsert/CDC | ✅ Built-in | ✅ Implemented |
| Testing effort | Low | High |
| Risk | Low | Medium |

---

## 🎯 Recommendation

### Use Custom SMT + Official Connector IF:
- ✅ Messages arrive in version order (upstream guarantee)
- ✅ Kafka offset order is acceptable
- ✅ Want minimal code and maintenance
- ✅ Want proven, tested connector

### Use Custom Connector IF:
- ✅ MUST have max(version) dedup logic
- ✅ Messages can arrive out of order
- ✅ Need full control over version logic
- ✅ Already implemented and tested

### Hybrid Approach:
- Kafka Streams: Deduplicate by max(version)
- Custom SMT: Transform format
- Official Connector: Write to Iceberg
- **Pros**: Best of both worlds
- **Cons**: More components to manage

---

## 🔍 Next Steps

1. **Test Custom SMT** ✅ (Implemented)
   - Build JAR
   - Deploy to Kafka Connect
   - Register connector

2. **Test Basic Operations** (TODO)
   - INSERT
   - UPDATE (upsert)
   - DELETE

3. **Test Version Ordering** (CRITICAL)
   - Send messages out of order
   - Verify which version is kept
   - **Decision point**: Keep SMT or switch to Custom Connector

4. **Performance Test** (TODO)
   - 50k messages batch
   - Measure throughput
   - Compare with Custom Connector

5. **Production Deployment** (TODO)
   - Monitor connector metrics
   - Set up alerting
   - Document operations

---

## 📝 Files

### Implemented:
- ✅ `custom-smt/src/main/java/com/example/kafka/connect/smt/CustomCDCTransform.java`
- ✅ `configs/sink.tram_quan_trac_cdc_official.json`
- ✅ `RESEARCH-CUSTOM-SMT-APPROACH.md`
- ✅ `CUSTOM-SMT-README.md`

### Backup (Custom Connector):
- ✅ `backup-cdc-connector-20260505-153540/` (Full custom connector implementation)
- ✅ `backup-cdc-connector-20260505-153540/PROGRESS.md` (Implementation status)

---

## 🆘 Troubleshooting

### Issue: Connector fails to start

**Check**:
1. Official Iceberg Sink Connector JAR is in plugins directory
2. Custom SMT JAR is in plugins directory
3. Kafka Connect worker restarted after adding JARs

### Issue: Transform fails

**Check**:
1. Message format matches expected CDC envelope
2. All required fields present (data, key, type, version, ngay_cap_nhat, length)
3. Connector logs for error messages

### Issue: Upsert not working

**Check**:
1. `iceberg.tables.default-id-columns` is set to `dedup_key`
2. Table has `dedup_key` column
3. Table format-version is 2

### Issue: Version ordering wrong

**Expected**: This is a known limitation
**Solution**: Use Custom Connector or add Kafka Streams pre-processing

---

**Document Version**: 1.0  
**Author**: Kiro AI Assistant  
**Status**: Ready for Testing
