# Connector Service

Kafka Connect với Custom SMT + Fork Iceberg Connector. Đọc data từ Kafka topics, transform, ghi vào Apache Iceberg.

## Phiên bản

| Version | Docker Image | Thay đổi |
|---------|-------------|---------|
| `1.2` | `duytuan24/connector-service:1.2` | Thêm **mode=append** cho SMT: ghi raw message (JSON/XML) vào 1 cột `record`, đích 3 cột |
| `1.1` | `duytuan24/connector-service:1.1` | Chuẩn hóa Snapshot Summary theo Consumer Engine Standard (task.engine, consumer.*) |
| `1.0` | `duytuan24/connector-service:1.0` | Phiên bản đầu tiên |

---

## Build Image

```bash
# Build với version tag
docker build -t duytuan24/connector-service:1.2 .

# Push lên Docker Hub
docker push duytuan24/connector-service:1.2
```

---

## Env vars cần config khi deploy

### Bắt buộc (thay đổi theo môi trường):

| Env var | Mô tả | Ví dụ |
|---|---|---|
| `CONNECT_BOOTSTRAP_SERVERS` | Kafka broker(s) | `kafka-prod:9092` |
| `AWS_REGION` | Region cho S3/MinIO | `us-east-1` |
| `AWS_ACCESS_KEY_ID` | S3/MinIO access key | `xxx` |
| `AWS_SECRET_ACCESS_KEY` | S3/MinIO secret key | `xxx` |

### Framework (có default, ít khi đổi):

| Env var | Default | Mô tả |
|---|---|---|
| `CONNECT_REST_PORT` | `8083` | REST API port |
| `CONNECT_GROUP_ID` | `connector-service` | Connect cluster ID |
| `CONNECT_CONFIG_STORAGE_TOPIC` | `connector-service-config` | Internal topic (config) |
| `CONNECT_OFFSET_STORAGE_TOPIC` | `connector-service-offsets` | Internal topic (offsets) |
| `CONNECT_STATUS_STORAGE_TOPIC` | `connector-service-status` | Internal topic (status) |
| `CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR` | `3` | Replication factor |
| `CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR` | `3` | Replication factor |
| `CONNECT_STATUS_STORAGE_REPLICATION_FACTOR` | `3` | Replication factor |
| `KAFKA_HEAP_OPTS` | `-Xms512M -Xmx4G` | JVM heap |

---

## REST API (built-in, port 8083)

### Tạo connector:

```bash
curl -X POST http://connector-service:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "sink-ct-cuahangxangdau",
    "config": {
      "connector.class": "io.tabular.iceberg.connect.IcebergSinkConnector",
      "tasks.max": "1",
      "topics": "ct_cuahangxangdau",
      "iceberg.tables.dynamic-enabled": "true",
      "iceberg.tables.route-field": "iceberg_table",
      "iceberg.tables.auto-create-enabled": "false",
      "iceberg.tables.evolve-schema-enabled": "true",
      "iceberg.tables.schema-force-optional": "true",
      "iceberg.tables.default-id-columns": "dedup_key",
      "iceberg.tables.cdc-field": "_cdc_op",
      "iceberg.tables.upsert-mode-enabled": "false",
      "iceberg.catalog": "production",
      "iceberg.catalog.type": "hive",
      "iceberg.catalog.uri": "thrift://hive-metastore:9083",
      "iceberg.catalog.warehouse": "s3a://lakehouse/warehouse/",
      "iceberg.catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
      "iceberg.catalog.s3.endpoint": "http://minio:9000",
      "iceberg.catalog.s3.path-style-access": "true",
      "iceberg.catalog.s3.access-key-id": "xxx",
      "iceberg.catalog.s3.secret-access-key": "xxx",
      "iceberg.catalog.client.region": "us-east-1",
      "iceberg.control.commit.interval-ms": "10000",
      "iceberg.control.commit.timeout-ms": "30000",
      "transforms": "customCdc",
      "transforms.customCdc.type": "com.example.kafka.connect.smt.CustomCDCTransform",
      "transforms.customCdc.topic.table.map": "ct_cuahangxangdau:congthuong.cuahangxangdau",
      "task.engine": "consumer",
      "consumer.typeingest": "API",
      "rdbEndpointsId": "<UUID-của-endpoint-nguồn>",
      "value.converter": "org.apache.kafka.connect.json.JsonConverter",
      "value.converter.schemas.enable": "false",
      "key.converter": "org.apache.kafka.connect.storage.StringConverter",
      "consumer.override.auto.offset.reset": "earliest",
      "errors.tolerance": "none",
      "errors.log.enable": "true",
      "errors.log.include.messages": "true"
    }
  }'
```

### Các API khác:

| Method | Endpoint | Mô tả |
|---|---|---|
| GET | `/connectors` | List tất cả connector |
| GET | `/connectors/{name}/status` | Status (RUNNING/FAILED) |
| GET | `/connectors/{name}/config` | Xem toàn bộ config (bao gồm rdbEndpointsId) |
| PUT | `/connectors/{name}/config` | Update config |
| DELETE | `/connectors/{name}` | Xóa connector |
| POST | `/connectors/{name}/restart` | Restart connector |
| PUT | `/connectors/{name}/pause` | Pause |
| PUT | `/connectors/{name}/resume` | Resume |

---

## Chế độ xử lý — `mode` (CDC vs Append)

SMT `CustomCDCTransform` có 2 chế độ, chọn bằng `transforms.customCdc.mode`. **Cùng 1 image** phục vụ cả 2 — chỉ khác JSON config khi `POST /connectors`.

| | **CDC** (mặc định) | **Append** |
|---|---|---|
| `transforms.customCdc.mode` | `cdc` (hoặc bỏ trống) | `append` |
| Input | CDC envelope `{data, type, version, key,...}` | Raw message bất kỳ (JSON/XML) |
| `value.converter` | `JsonConverter` | `StringConverter` |
| `iceberg.tables.cdc-field` | `_cdc_op` (bắt buộc) | **bỏ** |
| `iceberg.tables.default-id-columns` | `dedup_key` | **bỏ** |
| Hành vi | insert/update/delete + dedup + version filter | chỉ append, mỗi message = 1 row |
| Cột đích Iceberg | 8 cột (id, dedup_key, record, version, type, key, ngay_cap_nhat, length) | **3 cột**: id, record, ngay_cap_nhat |

### Append mode — đích chỉ 3 cột

- `id` = `topic-partition-offset` (tự sinh, unique mỗi message)
- `record` = toàn bộ message nguyên văn (JSON array hoặc XML) dạng string
- `ngay_cap_nhat` = thời gian commit (`Instant.now()`)

> **1 topic = 1 định dạng.** Dùng `StringConverter` nên connector không quan tâm nội dung — JSON hay XML đều lưu nguyên văn vào `record`.

DDL table append:
```sql
CREATE TABLE iceberg.<ns>.<table> (
  id VARCHAR, record VARCHAR, ngay_cap_nhat VARCHAR
) WITH (format = 'PARQUET');
```

### Tạo connector APPEND:

```bash
curl -X POST http://connector-service:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "sink-qtmt-append",
    "config": {
      "connector.class": "io.tabular.iceberg.connect.IcebergSinkConnector",
      "tasks.max": "1",
      "topics": "qtmt-append",
      "iceberg.tables.dynamic-enabled": "true",
      "iceberg.tables.route-field": "iceberg_table",
      "iceberg.tables.auto-create-enabled": "false",
      "iceberg.tables.evolve-schema-enabled": "true",
      "iceberg.tables.schema-force-optional": "true",
      "iceberg.catalog": "production",
      "iceberg.catalog.type": "hive",
      "iceberg.catalog.uri": "thrift://hive-metastore:9083",
      "iceberg.catalog.warehouse": "s3a://lakehouse/warehouse/",
      "iceberg.catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
      "iceberg.catalog.s3.endpoint": "http://minio:9000",
      "iceberg.catalog.s3.path-style-access": "true",
      "iceberg.catalog.s3.access-key-id": "xxx",
      "iceberg.catalog.s3.secret-access-key": "xxx",
      "iceberg.catalog.client.region": "us-east-1",
      "iceberg.control.commit.interval-ms": "10000",
      "iceberg.control.commit.timeout-ms": "30000",
      "transforms": "customCdc",
      "transforms.customCdc.type": "com.example.kafka.connect.smt.CustomCDCTransform",
      "transforms.customCdc.mode": "append",
      "transforms.customCdc.topic.table.map": "qtmt-append:congthuong.tramquantrac_raw",
      "task.engine": "consumer",
      "consumer.typeingest": "API",
      "rdbEndpointsId": "<UUID-của-endpoint-nguồn>",
      "value.converter": "org.apache.kafka.connect.storage.StringConverter",
      "key.converter": "org.apache.kafka.connect.storage.StringConverter",
      "consumer.override.auto.offset.reset": "earliest",
      "errors.tolerance": "none",
      "errors.log.enable": "true",
      "errors.log.include.messages": "true"
    }
  }'
```

> Lưu ý: append mode là **at-least-once** — redeliver trước khi commit có thể append trùng row (không có dedup theo thiết kế).

---

## Snapshot Metadata (Consumer Engine Standard)

Mỗi Iceberg commit tự động ghi vào snapshot summary các trường chuẩn hóa:

| Key | Ví dụ | Mô tả |
|-----|-------|-------|
| `task.engine` | `consumer` | Loại engine xử lý |
| `consumer.typeingest` | `API` | Phân biệt loại nguồn (API / CDC) |
| `consumer.connectorname` | `sink-ct-cuahangxangdau` | Tên connector → dùng làm Foreign Key truy vết config qua API |
| `consumer.ingest.time` | `1779437835253` | Thời gian (Epoch ms) lúc commit vào Iceberg |
| `consumer.vtts.time` | `1779437830601` | Event time của message trễ nhất (watermark) |

> **Truy vết:** `consumer.connectorname` + `GET /connectors/{name}/config` → lấy ra `rdbEndpointsId` và toàn bộ thông tin nguồn.

### Query snapshot qua Trino:

```sql
SELECT
  snapshot_id,
  committed_at,
  element_at(summary, 'task.engine')             AS task_engine,
  element_at(summary, 'consumer.typeingest')      AS consumer_typeingest,
  element_at(summary, 'consumer.connectorname')   AS consumer_connectorname,
  element_at(summary, 'consumer.ingest.time')     AS consumer_ingest_time,
  element_at(summary, 'consumer.vtts.time')       AS consumer_vtts_time
FROM iceberg.<namespace>."<table>$snapshots"
ORDER BY committed_at DESC;
```

---

## Yêu cầu infra bên ngoài

- Kafka cluster (broker accessible từ container)
- S3/MinIO (storage cho Iceberg data files)
- Hive Metastore (Iceberg catalog metadata)
- Iceberg tables phải tạo trước (`auto-create = false`)
