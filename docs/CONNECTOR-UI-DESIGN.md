# Thiết kế UI + Backend + Service Connector

## 1. UI Form (1 lần submit duy nhất) (Cần BA thiết kế lại cho phù hợp)

Cùng 1 trang, user điền TẤT CẢ rồi submit 1 lần:

```
┌──────────────────────────────────────────────────────┐
│ TẠO API TÍCH HỢP (Trang hiện tại)                    │
├──────────────────────────────────────────────────────┤
│ THÔNG TIN CƠ BẢN                                    │
│ Tên API*:     [ct-cuahangxangdau         ]          │
│ Phương thức*: [POST ▼]                              │
│ Đường dẫn URL*: [/ct-cuahangxangdau      ]          │
│ Cụm Kafka*:  [kafka-cluster ▼]                      │
│ Topic*:       [ct_cuahangxangdau ▼]                 │  ← chọn tại đây
│ ...                                                  │
├──────────────────────────────────────────────────────┤
│ CẤU HÌNH CONNECTOR (Phần cần bổ sung)                │
│ Tên connector*: [sink-ct-cuahangxangdau  ]           │  ← auto-generate hoặc tự đặt 
│                                                      │
│ TABLE ĐÍCH                                           │
│ Catalog*:     [staging       ▼]                      │  ← API: GET /api/catalogs
│ Namespace*:   [congthuong    ▼]                      │  ← API: GET /api/catalogs/{catalog}/namespaces
│ Table*:       [cuahangxangdau▼]                      │  ← API: GET /api/catalogs/{catalog}/namespaces/{ns}/tables
│                                                      │
│ ▸ Cấu hình nâng cao                                  │
│   Tasks max:        [1         ]                     │
│   Commit interval:  [10000     ] ms                  │
└──────────────────────────────────────────────────────┘
                    [  SUBMIT  ]
```

- Topic chọn ở phần trên → tự fill vào phần connector (readonly hoặc auto-generate tên) 
    --> Cần để mapping 1 topic -> 1 table cụ thể
- Catalog chọn → đi kèm block config catalog (user không thấy chi tiết)
- Tất cả submit 1 lần

---

## 2. Luồng xử lý khi Submit

```
User nhấn SUBMIT
    │
    ▼
Backend nhận toàn bộ form data
    │
    ├── 1. Tạo record API tích hợp → sinh rdbEndpointsId (UUID) → lưu DB
    │
    ├── 2. Sinh connectoruuid (UUID v4)
    │
    ├── 3. Merge: base config + catalog config + params + topicName + rdbEndpointsId
    │
    ├── 4. POST JSON connector chuẩn lên Service Connector (:8083)
    │
    └── 5. Lưu metadata connector vào DB (gắn với rdbEndpointsId)
    │
    ▼
Service Connector tạo instance → chạy
    │ consume topic → transform → ghi Iceberg
    │ mỗi commit: inject rdbEndpointsId + TypeIngest vào snapshot
    ▼
Iceberg (data + snapshot metadata)
```

**Thứ tự:**
1. Backend tạo `rdbEndpointsId` trước (lưu DB)
2. Dùng `rdbEndpointsId` đó để truyền vào connector config
3. POST connector lên Kafka Connect
4. Tất cả trong 1 request xử lý

---

## 3. Backend logic

### Input (từ UI submit):
```json
{
  "endpointName": "ct-cuahangxangdau",
  "methodName": "POST",
  "uriPath": "/ct-cuahangxangdau",
  "topicName": "ct_cuahangxangdau",
  "connectorName": "sink-ct-cuahangxangdau",
  "catalog": "staging",
  "namespace": "congthuong",
  "table": "cuahangxangdau",
  "tasksMax": "1",
  "commitIntervalMs": "10000"
}
```

### Logic xử lý:
1. Sinh `rdbEndpointsId` = UUID v4
2. Sinh `connectoruuid` = UUID v4
3. Lưu record API tích hợp vào DB (gồm rdbEndpointsId, topicName, endpoint info...)
4. Lookup `CATALOG_REGISTRY[catalog]` → lấy block config catalog
5. Build JSON connector = BASE_CONFIG + catalog config + params
6. POST lên `http://connector-service:8083/connectors`
7. Lưu connector metadata vào DB

### Code minh họa (pseudocode):
```python
def submit_integration(form_data):
    # 1. Sinh IDs
    rdb_endpoints_id = str(uuid4())
    connector_uuid = str(uuid4())

    # 2. Lưu API tích hợp vào DB
    save_integration(rdb_endpoints_id, form_data)

    # 3. Build connector config
    config = BASE_CONFIG.copy()
    config.update(CATALOG_REGISTRY[form_data["catalog"]])
    config["topics"] = form_data["topicName"]
    config["tasks.max"] = form_data.get("tasksMax", "1")
    config["iceberg.control.commit.interval-ms"] = form_data.get("commitIntervalMs", "10000")
    config["transforms.customCdc.topic.table.map"] = (
        f"{form_data['topicName']}:{form_data['namespace']}.{form_data['table']}"
    )
    config["uuid"] = rdb_endpoints_id
    config["typeingest"] = "API"

    # 4. POST lên Kafka Connect
    connector_name = form_data.get("connectorName", f"sink-{form_data['topicName']}")
    requests.post("http://connector-service:8083/connectors", json={
        "name": connector_name,
        "config": config
    })

    # 5. Lưu metadata connector vào DB
    save_connector_metadata(rdb_endpoints_id, connector_uuid, connector_name, form_data)
```

---

## 4. Config

### BASE_CONFIG (cố định trong backend):
```json
{
  "connector.class": "io.tabular.iceberg.connect.IcebergSinkConnector",
  "iceberg.tables.dynamic-enabled": "true",
  "iceberg.tables.route-field": "iceberg_table",
  "iceberg.tables.auto-create-enabled": "false",
  "iceberg.tables.evolve-schema-enabled": "true",
  "iceberg.tables.schema-force-optional": "true",
  "iceberg.tables.default-id-columns": "dedup_key",
  "iceberg.tables.cdc-field": "_cdc_op",
  "iceberg.tables.upsert-mode-enabled": "false",
  "iceberg.control.commit.timeout-ms": "30000",
  "transforms": "customCdc",
  "transforms.customCdc.type": "com.example.kafka.connect.smt.CustomCDCTransform",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter.schemas.enable": "false",
  "key.converter": "org.apache.kafka.connect.storage.StringConverter",
  "consumer.override.auto.offset.reset": "earliest",
  "errors.tolerance": "none",
  "errors.log.enable": "true",
  "errors.log.include.messages": "true"
}
```

### CATALOG_REGISTRY (lưu trong backend/DB):
```json
{
  "staging": {
    "iceberg.catalog": "staging",
    "iceberg.catalog.type": "hive",
    "iceberg.catalog.uri": "thrift://hive-staging:9083",
    "iceberg.catalog.warehouse": "s3a://staging-bucket/warehouse/",
    "iceberg.catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    "iceberg.catalog.s3.endpoint": "http://minio:9000",
    "iceberg.catalog.s3.path-style-access": "true",
    "iceberg.catalog.s3.access-key-id": "xxx",
    "iceberg.catalog.s3.secret-access-key": "xxx",
    "iceberg.catalog.client.region": "us-east-1"
  }
}
```

---

## 5. JSON Connector chuẩn (kết quả merge)

```json
{
  "name": "sink-ct-cuahangxangdau",
  "config": {
    // ===== BASE CONFIG (cố định) =====
    "connector.class": "io.tabular.iceberg.connect.IcebergSinkConnector",
    "iceberg.tables.dynamic-enabled": "true",
    "iceberg.tables.route-field": "iceberg_table",
    "iceberg.tables.auto-create-enabled": "false",
    "iceberg.tables.evolve-schema-enabled": "true",
    "iceberg.tables.schema-force-optional": "true",
    "iceberg.tables.default-id-columns": "dedup_key",
    "iceberg.tables.cdc-field": "_cdc_op",
    "iceberg.tables.upsert-mode-enabled": "false",
    "iceberg.control.commit.timeout-ms": "30000",
    "transforms": "customCdc",
    "transforms.customCdc.type": "com.example.kafka.connect.smt.CustomCDCTransform",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "consumer.override.auto.offset.reset": "earliest",
    "errors.tolerance": "none",
    "errors.log.enable": "true",
    "errors.log.include.messages": "true",

    // ===== CATALOG CONFIG (từ catalog đã chọn) =====
    "iceberg.catalog": "staging",
    "iceberg.catalog.type": "hive",
    "iceberg.catalog.uri": "thrift://hive-staging:9083",
    "iceberg.catalog.warehouse": "s3a://staging-bucket/warehouse/",
    "iceberg.catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    "iceberg.catalog.s3.endpoint": "http://minio:9000",
    "iceberg.catalog.s3.path-style-access": "true",
    "iceberg.catalog.s3.access-key-id": "xxx",
    "iceberg.catalog.s3.secret-access-key": "xxx",
    "iceberg.catalog.client.region": "us-east-1",

    // ===== PARAMS (từ UI form + backend sinh) =====
    "topics": "ct_cuahangxangdau",
    "tasks.max": "1",
    "iceberg.control.commit.interval-ms": "10000",
    "transforms.customCdc.topic.table.map": "ct_cuahangxangdau:congthuong.cuahangxangdau",

    // ===== LINEAGE (inject vào Iceberg snapshot) =====
    "uuid": "524e7d39-7a86-4ff1-8f26-d7777561e0cc",
    "typeingest": "API"
  }
}
```

---

## 6. Iceberg Snapshot Metadata

Mỗi commit chỉ chứa 2 fields:
```
uuid        = "524e7d39-7a86-4ff1-8f26-d7777561e0cc"  (= rdbEndpointsId)
typeingest  = "API"
```

---

## 7. Metadata lưu DB

```json
{
  "rdbEndpointsId": "524e7d39-7a86-4ff1-8f26-d7777561e0cc",
  "topicName": "ct_cuahangxangdau",
  "kafkaconnector": {
    "connectoruuid": "9b7f6a1c-...",
    "connectorName": "sink-ct-cuahangxangdau",
    "catalog": "staging",
    "namespace": "congthuong",
    "table": "cuahangxangdau",
    "tasks.max": "1",
    "iceberg.control.commit.interval-ms": "10000"
  }
}
```

- `connectoruuid`: backend tự sinh (UUID v4)
- `rdbEndpointsId`: backend tự sinh khi submit

---

## 8. APIs

### Connector Service (manage-dp gọi):

| Method | Endpoint | Mô tả |
|--------|----------|--------|
| POST | `/connector-service` | Tạo connector |
| PUT | `/connector-service` | Cập nhật connector |
| DELETE | `/connector-service` | Xóa connector |

### APIs phụ trợ cho UI dropdown (manage-dp expose):

| Method | Endpoint | Mô tả |
|--------|----------|--------|
| GET | `/api/catalogs` | List catalogs |
| GET | `/api/catalogs/{catalog}/namespaces` | List namespaces |
| GET | `/api/catalogs/{catalog}/namespaces/{ns}/tables` | List tables |
