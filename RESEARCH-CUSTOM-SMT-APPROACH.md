# Research: Custom SMT + Official Apache Iceberg Sink Connector

**Date**: 2026-05-05  
**Status**: ✅ FEASIBLE - Recommended Approach

---

## 🎯 Phương án mới (Tốt hơn nhiều!)

**Thay vì**: Build full custom Kafka Connect Sink Connector  
**Làm**: Custom SMT → Official Apache Iceberg Sink Connector

---

## ✅ Tại sao khả thi?

### Official Apache Iceberg Sink Connector có sẵn:

1. **✅ id-columns** - Xác định row key / primary key
   - Config: `iceberg.tables.default-id-columns`
   - Config per table: `iceberg.table.<table>.id-columns`
   - **→ Dùng cho dedup_key của mình!**

2. **✅ CDC support** - Metadata `_cdc.*`
   - `_cdc.op` - operation type (c/u/d)
   - `_cdc.ts` - timestamp
   - `_cdc.source` - source info
   - **→ Map từ type field của mình!**

3. **✅ SMT transforms có sẵn**:
   - `DebeziumTransform` - Debezium CDC format
   - `DmsTransform` - AWS DMS CDC format
   - `JsonToMapTransform` - JSON parsing
   - `KafkaMetadataTransform` - Kafka metadata
   - **→ Học pattern từ đây!**

4. **✅ Control topic** - Commit coordination
   - Exactly-once delivery semantics
   - Centralized Iceberg commits
   - **→ Không cần tự implement!**

5. **✅ Hive/S3 catalog config** - Có sẵn
   - REST, Hive, Hadoop, Glue, Nessie catalogs
   - S3A filesystem support
   - **→ Dùng luôn!**

6. **✅ Auto table creation & schema evolution**
   - `iceberg.tables.auto-create-enabled`
   - `iceberg.tables.evolve-schema-enabled`
   - **→ Tiện lợi!**

---

## ⚠️ Vấn đề: Message format không tương thích

### Message của mình (custom envelope):
```json
{
  "data": [...],
  "key": "MaTram",
  "type": "INSERT",
  "version": 1704067200000,
  "ngay_cap_nhat": "2024-01-01T00:00:00Z",
  "length": 1
}
```

### Official connector KHÔNG hiểu:
- ❌ `type` = INSERT/UPDATE/DELETE (cần `_cdc.op`)
- ❌ `version` = tiêu chí chọn latest (cần dedup logic)
- ❌ `data[]` = cần stringify vào `record`
- ❌ `dedup_key` = topic:key (cần construct)

---

## 💡 Giải pháp: Custom SMT Transform

### Custom SMT sẽ làm gì?

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

**Output** (Iceberg-compatible record):
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

### Mapping rules:

| Source Field | Target Field | Transformation |
|--------------|--------------|----------------|
| - | `id` | Generate UUID or topic-partition-offset |
| `key` + topic | `dedup_key` | Construct "topic:key" |
| `data[]` | `record` | JSON.stringify(data) |
| `ngay_cap_nhat` | `ingest_time` | Direct copy |
| `length` | `length` | Direct copy |
| `key` | `key` | Direct copy |
| `type` | `type` | Direct copy |
| `version` | `version` | Direct copy |
| `type` | `_cdc.op` | Map: INSERT→c, UPDATE→u, DELETE→d |
| `version` | `_cdc.ts` | Direct copy |

### CDC operation mapping:

```java
// From official connector CDC constants
INSERT → _cdc.op = "c" (create)
UPDATE → _cdc.op = "u" (update)
DELETE → _cdc.op = "d" (delete)
```

**Note**: Cần kiểm tra exact value của `_cdc.op` trong version connector đang dùng (có thể là "c"/"u"/"d" hoặc "insert"/"update"/"delete")

---

## 🔧 Implementation Plan

### Step 1: Create Custom SMT

**File**: `custom-smt/src/main/java/com/example/kafka/connect/smt/CustomCDCTransform.java`

```java
public class CustomCDCTransform<R extends ConnectRecord<R>> implements Transformation<R> {
    
    @Override
    public R apply(R record) {
        // 1. Extract fields from CDC message
        Map<String, Object> value = (Map<String, Object>) record.value();
        
        // 2. Transform to Iceberg-compatible format
        Map<String, Object> transformed = new HashMap<>();
        
        // Generate id
        transformed.put("id", UUID.randomUUID().toString());
        
        // Construct dedup_key = topic:key
        String businessKey = (String) value.get("key");
        transformed.put("dedup_key", record.topic() + ":" + businessKey);
        
        // Stringify data[] to record
        List<Map<String, Object>> dataArray = (List) value.get("data");
        transformed.put("record", objectMapper.writeValueAsString(dataArray));
        
        // Copy other fields
        transformed.put("ingest_time", value.get("ngay_cap_nhat"));
        transformed.put("length", value.get("length"));
        transformed.put("key", businessKey);
        transformed.put("type", value.get("type"));
        transformed.put("version", value.get("version"));
        
        // Add CDC metadata
        Map<String, Object> cdc = new HashMap<>();
        String type = (String) value.get("type");
        cdc.put("op", mapOperationType(type)); // INSERT→c, UPDATE→u, DELETE→d
        cdc.put("ts", value.get("version"));
        transformed.put("_cdc", cdc);
        
        // 3. Create new record with transformed value
        return record.newRecord(
            record.topic(),
            record.kafkaPartition(),
            record.keySchema(),
            record.key(),
            buildSchema(), // Schema with all fields including _cdc
            transformed,
            record.timestamp()
        );
    }
    
    private String mapOperationType(String type) {
        switch (type) {
            case "INSERT": return "c"; // create
            case "UPDATE": return "u"; // update
            case "DELETE": return "d"; // delete
            default: throw new IllegalArgumentException("Unknown type: " + type);
        }
    }
}
```

### Step 2: Configure Official Connector

**File**: `configs/sink.tram_quan_trac_cdc_official.json`

```json
{
  "name": "sink.tram_quan_trac_cdc_official",
  "config": {
    "connector.class": "org.apache.iceberg.connect.IcebergSinkConnector",
    "tasks.max": "1",
    "topics": "tram_quan_trac",
    
    "iceberg.tables": "default.tram_quan_trac_cdc",
    "iceberg.tables.default-id-columns": "dedup_key",
    
    "iceberg.catalog.type": "hive",
    "iceberg.catalog.uri": "thrift://hive-metastore:9083",
    "iceberg.catalog.warehouse": "s3a://bucket/warehouse/",
    "iceberg.catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    "iceberg.catalog.s3.endpoint": "http://minio:9000",
    "iceberg.catalog.s3.path-style-access": "true",
    "iceberg.catalog.s3.access-key-id": "minioadmin",
    "iceberg.catalog.s3.secret-access-key": "minioadmin",
    
    "transforms": "customCdc",
    "transforms.customCdc.type": "com.example.kafka.connect.smt.CustomCDCTransform",
    
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    
    "errors.tolerance": "none",
    "errors.log.enable": "true",
    "errors.log.include.messages": "true"
  }
}
```

---

## ⚠️ Điểm cần kiểm chứng (CRITICAL)

### 1. Version ordering & deduplication

**Vấn đề**: Official connector có handle max(version) trong batch không?

**Scenario cần test**:
```
Message 1: version=1000
Message 2: version=3000
Message 3: version=2000
```

**Kỳ vọng**: Giữ message version=3000, skip message version=2000

**Thực tế**:
- Official connector thường dựa vào **Kafka ordering** (offset order)
- Không có config `dedup-column=version` như getindata
- **id-columns** chỉ xác định primary key, KHÔNG xử lý version ordering

**Kết luận**:
- ❌ Official connector **KHÔNG** hỗ trợ max(version) dedup trong batch
- ✅ Chỉ hỗ trợ upsert theo id-columns (last write wins theo Kafka offset order)

**Giải pháp**:

**Option A**: Chấp nhận Kafka ordering
- Giả định: Messages đến theo thứ tự version tăng dần
- Dùng id-columns + upsert của official connector
- **Pros**: Đơn giản, ít code
- **Cons**: Không handle out-of-order messages

**Option B**: Pre-process với Kafka Streams
- Kafka Streams deduplicate theo max(version) trước
- Gửi kết quả vào topic mới
- Official connector consume từ topic đã dedup
- **Pros**: Đúng logic max(version)
- **Cons**: Thêm component (Kafka Streams)

**Option C**: SMT stateful (KHÔNG KHUYẾN NGHỊ)
- SMT giữ state để track max(version)
- **Pros**: Không cần thêm component
- **Cons**: SMT không nên stateful, vi phạm design pattern

**Option D**: Giữ custom connector (như đã implement)
- Full control version logic
- **Pros**: Đúng 100% requirements
- **Cons**: Nhiều code hơn, phải maintain

### 2. CDC metadata format

**Vấn đề**: `_cdc.op` value chính xác là gì?

**Cần kiểm tra**:
- Đọc source code của `DebeziumTransform` hoặc `DmsTransform`
- Tìm `CdcConstants` class
- Xác định exact values: "c"/"u"/"d" hay "insert"/"update"/"delete"

**Tạm thời dùng**: "c", "u", "d" (theo Debezium convention)

---

## 📊 So sánh 2 phương án

| Tiêu chí | Custom Connector (đã làm) | Custom SMT + Official Connector |
|----------|---------------------------|----------------------------------|
| **Lines of code** | ~1500 lines | ~300 lines (chỉ SMT) |
| **Complexity** | High (Iceberg operations) | Low (chỉ transform) |
| **Maintenance** | Phải maintain Iceberg logic | Chỉ maintain SMT |
| **Exactly-once** | Tự implement | Có sẵn (KIP-447) |
| **Commit coordination** | Tự implement | Có sẵn (control topic) |
| **Schema evolution** | Tự implement | Có sẵn |
| **Version ordering** | ✅ Có (max version) | ❌ Không (Kafka order) |
| **Upsert/CDC** | ✅ Có (equality delete) | ✅ Có (id-columns) |
| **Testing** | Cần test nhiều | Ít test hơn (official đã test) |
| **Risk** | Medium (custom logic) | Low (proven connector) |

---

## 🎯 Khuyến nghị

### Nếu chấp nhận Kafka ordering:
→ **Dùng Custom SMT + Official Connector** (Option A)
- Đơn giản nhất
- Ít code nhất
- Tận dụng official connector
- **Assumption**: Messages đến theo thứ tự version

### Nếu BẮT BUỘC max(version) logic:
→ **Giữ Custom Connector** (đã implement)
- Đúng 100% requirements
- Full control
- Đã implement xong rồi

### Nếu muốn cả hai:
→ **Kafka Streams + Custom SMT + Official Connector** (Option B)
- Kafka Streams: Deduplicate max(version)
- Custom SMT: Transform format
- Official Connector: Write to Iceberg
- **Pros**: Best of both worlds
- **Cons**: Thêm complexity (Kafka Streams)

---

## 🚀 Next Steps

### Test Plan:

1. **Implement Custom SMT** (~2 hours)
   - Create CustomCDCTransform class
   - Add to build.gradle
   - Build JAR

2. **Test với Official Connector** (~1 hour)
   - Deploy official connector với custom SMT
   - Send test messages
   - Verify data in Iceberg

3. **Test version ordering** (~1 hour)
   - Send messages out of order (v1, v3, v2)
   - Check which version is kept
   - Confirm Kafka ordering behavior

4. **Decision point**:
   - If Kafka ordering OK → Use Custom SMT + Official
   - If need max(version) → Keep Custom Connector
   - If need both → Add Kafka Streams

---

## 📝 Conclusion

**Custom SMT + Official Connector** là phương án **khả thi và nên thử trước** vì:
- ✅ Ít code hơn nhiều (~300 vs ~1500 lines)
- ✅ Tận dụng official connector (proven, tested)
- ✅ Có exactly-once, commit coordination, schema evolution
- ✅ Dễ maintain hơn

**Nhưng** cần test kỹ **version ordering** trước khi chốt.

Nếu version ordering không đúng requirements → Giữ Custom Connector (đã implement xong).

---

**Document Version**: 1.0  
**Author**: Kiro AI Assistant  
**Status**: Ready for Implementation
