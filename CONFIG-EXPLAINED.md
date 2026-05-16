# Giải thích Connector Config: sink.tram_quan_trac_cdc_v2.json

File: `configs/sink.tram_quan_trac_cdc_v2.json`

Deploy lên Kafka Connect REST API → pipeline tự chạy.

---

## Config hiện tại

```json
{
  "name": "sink.sla-group",
  "config": {
    "connector.class": "io.tabular.iceberg.connect.IcebergSinkConnector",
    "tasks.max": "1",
    "topics": "qtmt-tramquantrac,qtmt-quantrackhithai",

    "iceberg.tables.dynamic-enabled": "true",
    "iceberg.tables.route-field": "iceberg_table",
    "iceberg.tables.auto-create-enabled": "true",
    "iceberg.tables.evolve-schema-enabled": "true",
    "iceberg.tables.schema-force-optional": "true",

    "iceberg.tables.default-id-columns": "dedup_key",
    "iceberg.tables.cdc-field": "_cdc_op",
    "iceberg.tables.upsert-mode-enabled": "false",

    "iceberg.catalog": "default",
    "iceberg.catalog.type": "hive",
    "iceberg.catalog.uri": "thrift://hive-metastore:9083",
    "iceberg.catalog.warehouse": "s3a://bucket/warehouse/",
    "iceberg.catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    "iceberg.catalog.s3.endpoint": "http://minio:9000",
    "iceberg.catalog.s3.path-style-access": "true",
    "iceberg.catalog.s3.access-key-id": "minioadmin",
    "iceberg.catalog.s3.secret-access-key": "minioadmin",
    "iceberg.catalog.client.region": "us-east-1",

    "iceberg.control.commit.interval-ms": "10000",
    "iceberg.control.commit.timeout-ms": "30000",

    "transforms": "customCdc",
    "transforms.customCdc.type": "com.example.kafka.connect.smt.CustomCDCTransform",
    "transforms.customCdc.iceberg.namespace": "default",
    "transforms.customCdc.topic.table.map": "qtmt-quantrackhithai:def.abc",

    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",

    "consumer.override.auto.offset.reset": "earliest",
    "errors.tolerance": "none",
    "errors.log.enable": "true",
    "errors.log.include.messages": "true"
  }
}
```

---

## Giải thích theo phần

### PHẦN 1: Kafka Connect + Dynamic Routing

| Config | Giá trị | Giải thích |
|--------|---------|------------|
| `connector.class` | `io.tabular.iceberg.connect.IcebergSinkConnector` | Fork connector (có strip + snapshot metadata) |
| `tasks.max` | `"1"` | Số task song song |
| `topics` | `"qtmt-tramquantrac,qtmt-quantrackhithai"` | Danh sách topics đọc |
| `dynamic-enabled` | `"true"` | Bật dynamic routing (1 connector → nhiều tables) |
| `route-field` | `"iceberg_table"` | Field trong record chứa tên table đích |

### PHẦN 2: CDC Logic

| Config | Giá trị | Giải thích |
|--------|---------|------------|
| `default-id-columns` | `"dedup_key"` | Identity column cho equality delete |
| `cdc-field` | `"_cdc_op"` | Field chứa I/U/D (connector đọc rồi strip trước khi ghi) |
| `upsert-mode-enabled` | `"false"` | Chỉ dùng CDC field |

**Lưu ý:** `_cdc_op` và `iceberg_table` được connector **strip** trước khi ghi vào Iceberg → không xuất hiện trong bảng.

### PHẦN 3: Catalog + Storage

| Config | Giải thích |
|--------|------------|
| `catalog.type=hive` | Hive Metastore lưu metadata |
| `catalog.uri` | Địa chỉ Hive Metastore |
| `catalog.warehouse` | S3 path lưu data files |
| `s3.endpoint` | MinIO address |

### PHẦN 4: Commit Control

| Config | Giá trị | Giải thích |
|--------|---------|------------|
| `commit.interval-ms` | `"10000"` | Commit snapshot mỗi 10s |
| `commit.timeout-ms` | `"30000"` | Timeout 30s |

### PHẦN 5: SMT + Topic→Table Mapping

| Config | Giá trị | Giải thích |
|--------|---------|------------|
| `transforms.customCdc.type` | `CustomCDCTransform` | Class SMT |
| `iceberg.namespace` | `"default"` | Namespace mặc định (fallback) |
| `topic.table.map` | `"qtmt-quantrackhithai:def.abc"` | Custom mapping: topic → namespace.table |

**Mapping logic:**
1. Topic có trong `topic.table.map` → dùng giá trị chỉ định
2. Topic không có → auto-derive: `namespace + "." + topic.replace("-", "_")`

### PHẦN 6: Converter + Error

| Config | Giải thích |
|--------|------------|
| `value.converter` | JSON (schemas.enable=false) |
| `auto.offset.reset` | Đọc từ đầu topic nếu chưa có offset |
| `errors.tolerance=none` | Dừng ngay khi lỗi |

---

## Schema Iceberg (8 fields nghiệp vụ)

```
id, dedup_key, record, version, type, key, ngay_cap_nhat, length
```

Không có: `source_type`, `iceberg_table`, `_cdc_op` (đã strip/loại bỏ).

---

## Snapshot Metadata (lineage)

Mỗi snapshot tự động có:
```
pipeline.snapshot-uuid  = UUID unique per table per commit
pipeline.topic          = table name
pipeline.source-type    = "API" (configurable)
kafka.connect.commit-id = UUID commit cycle
```

---

## Deploy

```powershell
$body = Get-Content "configs\sink.tram_quan_trac_cdc_v2.json" -Raw
Invoke-RestMethod -Method Post "http://localhost:8083/connectors" -ContentType "application/json" -Body $body
```
