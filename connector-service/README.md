# Connector Service

Kafka Connect với Custom SMT + Fork Iceberg Connector. Đọc data từ Kafka topics, transform, ghi vào Apache Iceberg.

## Build Image

```bash
docker build -t connector-service:1.0 .
```

## Push Registry (thay URL registry)

```bash
docker tag connector-service:1.0 your-registry.com/connector-service:1.0
docker push your-registry.com/connector-service:1.0
```

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
      "typeingest": "API",
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
| PUT | `/connectors/{name}/config` | Update config |
| DELETE | `/connectors/{name}` | Xóa connector |
| POST | `/connectors/{name}/restart` | Restart connector |
| PUT | `/connectors/{name}/pause` | Pause |
| PUT | `/connectors/{name}/resume` | Resume |

## Snapshot Metadata

Mỗi Iceberg commit tự động ghi vào snapshot summary:

```
connector.name = "sink-ct-cuahangxangdau"
typeingest     = "API"
```

## Yêu cầu infra bên ngoài

- Kafka cluster (broker accessible từ container)
- S3/MinIO (storage cho Iceberg data files)
- Hive Metastore (Iceberg catalog metadata)
- Iceberg tables phải tạo trước (auto-create = false)
