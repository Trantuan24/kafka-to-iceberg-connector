# Quick Start: Test Custom SMT + Official Connector

## 🎯 Mục tiêu

Test phương án **Custom SMT + Official Apache Iceberg Sink Connector** để xem:
1. ✅ SMT có transform message đúng format không?
2. ⚠️ **CRITICAL**: Official connector có xử lý version ordering đúng không?

---

## 🚀 Quick Start (3 bước)

### Bước 1: Deploy Connector

```bash
# Make scripts executable
chmod +x deploy-custom-smt.sh check-results.sh

# Deploy connector
./deploy-custom-smt.sh
```

**Kết quả mong đợi:**
```
✅ JAR file exists
✅ JAR copied to Connect container
✅ CustomCDCTransform plugin loaded
✅ Deployment Complete!
```

---

### Bước 2: Run Test

```bash
# Install Python Kafka client (if not installed)
pip install kafka-python

# Run test
python3 test-custom-smt.py
```

**Test sẽ chạy:**
- ✅ TEST 1: Basic INSERT
- ⚠️ **TEST 2: Version Ordering (CRITICAL)** - Gửi v1000 → v3000 → v2000
- ✅ TEST 3: UPDATE
- ✅ TEST 4: DELETE
- ✅ TEST 5: Batch Mixed Operations

---

### Bước 3: Check Results

```bash
./check-results.sh
```

**Kết quả quan trọng nhất - TEST 2:**

**✅ PASSED** (Version control works):
```
version | TenTram      | Status
--------|--------------|-------
3000    | Version 3000 | v3
```
→ Official connector + SMT hoạt động tốt!

**❌ FAILED** (Kafka ordering used):
```
version | TenTram      | Status
--------|--------------|-------
2000    | Version 2000 | v2
```
→ Official connector dùng Kafka offset order, KHÔNG dùng version field
→ Cần dùng Custom Connector hoặc Kafka Streams

---

## 📊 Kịch bản Test

### TEST 2: Version Ordering (CRITICAL)

**Gửi messages theo thứ tự:**
1. TRAM002 - version 1000 - "Version 1000"
2. TRAM002 - version 3000 - "Version 3000" ← **NEWEST**
3. TRAM002 - version 2000 - "Version 2000" ← **STALE**

**Kỳ vọng:**
- Iceberg table chỉ có version 3000 (latest)
- Version 2000 bị skip (stale)

**Thực tế:**
- Nếu có version 3000 → ✅ Version control works
- Nếu có version 2000 → ❌ Kafka ordering used (last write wins)

---

## 🔍 Monitoring

### 1. Watch Connector Logs

```bash
docker logs -f iceberg-kafka-connect-demo-connect-1 | grep -i "customcdc\|error"
```

**Tìm kiếm:**
- ✅ "CustomCDCTransform" - SMT được load
- ✅ "Transformed record" - SMT đang transform
- ❌ "ERROR" - Có lỗi

### 2. Query Iceberg Table

```bash
docker exec iceberg-kafka-connect-demo-trino-1 trino --execute "
SELECT * FROM iceberg.default.tram_quan_trac_cdc ORDER BY version
"
```

### 3. Check Connector Status

```bash
curl -s http://localhost:8083/connectors/sink.tram_quan_trac_cdc_official/status | python3 -m json.tool
```

---

## 🐛 Troubleshooting

### Issue 1: Connector Failed to Start

**Check:**
```bash
docker logs iceberg-kafka-connect-demo-connect-1 | grep -i error
```

**Common causes:**
- JAR not loaded → Restart Connect
- Config error → Check configs/sink.tram_quan_trac_cdc_official.json
- Table not exists → Run create table SQL

### Issue 2: No Data in Iceberg

**Check:**
1. Kafka topic has messages:
   ```bash
   docker exec iceberg-kafka-connect-demo-kafka-1 kafka-console-consumer \
     --bootstrap-server localhost:9092 \
     --topic tram_quan_trac \
     --from-beginning \
     --max-messages 5
   ```

2. Connector is running:
   ```bash
   curl -s http://localhost:8083/connectors/sink.tram_quan_trac_cdc_official/status
   ```

3. Check connector logs for errors

### Issue 3: SMT Transform Error

**Check logs:**
```bash
docker logs iceberg-kafka-connect-demo-connect-1 | grep -i "CustomCDCTransform\|transform"
```

**Common causes:**
- Message format không đúng
- Missing fields (data, key, type, version)
- Type casting error

---

## 📝 Expected Output

### Successful Deployment

```
✅ JAR file exists
✅ JAR copied to Connect container
✅ CustomCDCTransform plugin loaded
✅ Table ready
✅ Deployment Complete!

Connector Status: RUNNING
```

### Successful Test

```
✅ Connected to Kafka
✅ Sent message to tram_quan_trac (partition=0, offset=123)
...
✅ All test messages sent successfully!
```

### Successful Results Check

```
1. Connector Status: RUNNING
2. Recent Connector Logs: No errors found
3. Iceberg Table Contents:
   - TRAM001: INSERT → UPDATE → DELETE
   - TRAM002: version 3000 (✅ PASSED)
   - TRAM003: INSERT → UPDATE
   - TRAM004: INSERT
4. TEST 2 - Version Ordering: ✅ PASSED
```

---

## 🎯 Decision Point

### Nếu TEST 2 PASSED (version 3000):
→ ✅ **Dùng Custom SMT + Official Connector**
- Ít code hơn (~300 lines vs ~1500 lines)
- Tận dụng official connector features
- Dễ maintain hơn

### Nếu TEST 2 FAILED (version 2000):
→ ❌ **Official connector không hỗ trợ version control**

**Options:**
1. **Giữ Custom Connector** (đã implement) - Full control
2. **Kafka Streams** + Custom SMT + Official - Dedup trước khi sink
3. **Chấp nhận Kafka ordering** - Giả định messages đến đúng thứ tự

---

## 📚 Files Created

- `deploy-custom-smt.sh` - Deploy script
- `test-custom-smt.py` - Test script
- `check-results.sh` - Results checker
- `QUICK-START-CUSTOM-SMT.md` - This guide

---

## 🔗 References

- Custom SMT: `custom-smt/src/main/java/com/example/kafka/connect/smt/CustomCDCTransform.java`
- Config: `configs/sink.tram_quan_trac_cdc_official.json`
- Research: `RESEARCH-CUSTOM-SMT-APPROACH.md`

---

**Ready to test? Run:**
```bash
./deploy-custom-smt.sh && python3 test-custom-smt.py && ./check-results.sh
```
