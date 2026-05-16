# Iceberg Kafka Connect CDC Pipeline

```
Kafka Topics → Custom SMT → Fork Iceberg Sink Connector → Iceberg
```

Pipeline CDC với dynamic routing, version filter, snapshot metadata lineage.

---

## Yêu cầu

- Docker Desktop (Windows)
- Python 3 + `kafka-python` (`pip install kafka-python`)
- PowerShell

---

## Chạy từ đầu đến cuối

### Bước 1: Build và khởi động Docker

```powershell
# Build image (lần đầu hoặc sau khi thay đổi JAR)
docker compose build

# Start tất cả services
docker compose up -d

# Kiểm tra containers (đợi ~90s cho Connect load plugins)
docker compose ps
```

### Bước 2: Kiểm tra services sẵn sàng

```powershell
# Kafka Connect (phải trả về version)
Invoke-RestMethod "http://localhost:8083/"

# Kafka broker
docker exec kafka kafka-topics --bootstrap-server kafka:9092 --list

# Trino
docker exec iceberg-kafka-connect-demo-trino-1 trino --execute "SELECT 1"
```

### Bước 3: Deploy connector

```powershell
# Tạo namespace custom (nếu dùng topic.table.map trỏ vào namespace khác default)
docker exec iceberg-kafka-connect-demo-trino-1 trino --execute "CREATE SCHEMA IF NOT EXISTS iceberg.def"

# Deploy
$body = Get-Content "configs\sink.tram_quan_trac_cdc_v2.json" -Raw
Invoke-RestMethod -Method Post "http://localhost:8083/connectors" -ContentType "application/json" -Body $body

# Kiểm tra status (connector + task phải RUNNING)
Invoke-RestMethod "http://localhost:8083/connectors/sink.sla-group/status" | ConvertTo-Json -Depth 3
```

### Bước 4: Gửi test messages

```powershell
python test.py
```

Test gửi 17 messages trong 3 batches, đủ INSERT/UPDATE/DELETE/Stale DROP trên cả 2 topics.

### Bước 5: Đợi commit (15s) rồi verify data

```powershell
# Table 1: auto-derive (default.qtmt_tramquantrac)
docker exec iceberg-kafka-connect-demo-trino-1 trino --execute "SELECT dedup_key, type, version FROM iceberg.default.qtmt_tramquantrac ORDER BY dedup_key"

# Table 2: custom map (def.abc)
docker exec iceberg-kafka-connect-demo-trino-1 trino --execute "SELECT dedup_key, type, version FROM iceberg.def.abc ORDER BY dedup_key"

# Kiểm tra schema (phải là 8 fields nghiệp vụ, không có _cdc_op, iceberg_table, source_type)
docker exec iceberg-kafka-connect-demo-trino-1 trino --execute "DESCRIBE iceberg.default.qtmt_tramquantrac"
```

Kết quả mong đợi:
```
default.qtmt_tramquantrac: TRAM001||TRAM002 v2, TRAM003 v1, TRAM005||TRAM006 v2, TRAM007||TRAM008 v1
def.abc: TR001 v2, TR002 v1, TR003 v2, TR004 v1
```

Schema: `id, dedup_key, record, version, type, key, ngay_cap_nhat, length` (8 fields)

### Bước 6: Verify snapshot metadata (lineage)

```powershell
# Tạo file query
@"
SELECT snapshot_id, committed_at,
  summary['pipeline.snapshot-uuid'] AS lineage_uuid,
  summary['pipeline.topic'] AS topic,
  summary['pipeline.source-type'] AS source_type
FROM iceberg.default."qtmt_tramquantrac`$snapshots"
ORDER BY committed_at DESC LIMIT 3;
"@ | Set-Content -Path "query-snapshots.sql"

# Copy vào Trino và chạy
docker cp "query-snapshots.sql" "iceberg-kafka-connect-demo-trino-1:/tmp/q.sql"
docker exec iceberg-kafka-connect-demo-trino-1 trino -f /tmp/q.sql
```

Mong đợi mỗi snapshot có:
```
pipeline.snapshot-uuid = UUID (unique per table per commit)
pipeline.topic         = qtmt_tramquantrac
pipeline.source-type   = API
```

---

## Topic → Table Mapping

Config trong `configs/sink.tram_quan_trac_cdc_v2.json`:

```json
"transforms.customCdc.iceberg.namespace": "default",
"transforms.customCdc.topic.table.map": "qtmt-quantrackhithai:def.abc"
```

| Topic | Mapping | Table đích |
|-------|---------|------------|
| `qtmt-tramquantrac` | Auto-derive (không có trong map) | `default.qtmt_tramquantrac` |
| `qtmt-quantrackhithai` | Custom map | `def.abc` |

Chi tiết: xem `TOPIC-TABLE-MAPPING.md`

---

## Rebuild khi sửa code

### Sửa Custom SMT:

```powershell
# Build JAR trong container
docker exec iceberg-kafka-connect-demo-connect-1 mkdir -p /tmp/custom-smt/src/main/java/com/example/kafka/connect/smt
docker cp "custom-smt\src\main\java\com\example\kafka\connect\smt\CustomCDCTransform.java" "iceberg-kafka-connect-demo-connect-1:/tmp/custom-smt/src/main/java/com/example/kafka/connect/smt/CustomCDCTransform.java"
docker cp "build-smt.sh" "iceberg-kafka-connect-demo-connect-1:/tmp/build-smt.sh"
docker exec iceberg-kafka-connect-demo-connect-1 bash /tmp/build-smt.sh

# Copy JAR ra host + rebuild image + recreate
docker cp "iceberg-kafka-connect-demo-connect-1:/tmp/custom-cdc-transform-new.jar" "plugins\custom-smt\custom-cdc-transform.jar"
docker compose build connect
docker compose up -d connect --force-recreate
```

### Sửa Fork Connector:

```powershell
# Build với Gradle (Git Bash)
& "C:\Program Files\Git\bin\bash.exe" -c "cd '/d/nifi-test/iceberg-kafka-connect-demo/iceberg-kafka-connect-fork' && ./gradlew :iceberg-kafka-connect:jar -x test --no-daemon"

# Copy JAR + rebuild + recreate
Copy-Item -Force "iceberg-kafka-connect-fork\kafka-connect\build\libs\iceberg-kafka-connect.jar" "plugins\iceberg-kafka-connect\lib\iceberg-kafka-connect-custom-pipeline-meta.jar"
docker compose build connect
docker compose up -d connect --force-recreate
```

**QUAN TRỌNG:** Luôn `docker compose build connect` sau khi thay JAR. Không build = mất code khi restart.

---

## Dừng / Reset

```powershell
# Dừng (giữ data)
docker compose stop

# Dừng + xóa volumes (reset hoàn toàn)
docker compose down -v
```

---

## Cấu trúc dự án

```
├── configs/
│   └── sink.tram_quan_trac_cdc_v2.json     # Connector config (topics, mapping, CDC)
├── custom-smt/
│   └── src/main/.../CustomCDCTransform.java # SMT: version filter, routing, transform
├── iceberg-kafka-connect-fork/
│   └── kafka-connect/src/...               # Fork connector source (strip, metadata inject)
├── plugins/
│   ├── custom-smt/custom-cdc-transform.jar # SMT JAR
│   └── iceberg-kafka-connect/lib/          # Fork connector JARs + dependencies
├── trino-catalog/iceberg.properties        # Trino config
├── docker-compose.yml                      # Docker services
├── Dockerfile.connect                      # Kafka Connect image
├── test.py                                 # Test script (17 messages, 3 batches, I/U/D/Stale)
├── sample_message1.json                    # Message format mẫu (tramquantrac)
├── sample_message2.json                    # Message format mẫu (quantrackhithai)
├── CONFIG-EXPLAINED.md                     # Giải thích config chi tiết
├── TOPIC-TABLE-MAPPING.md                  # Cách map topic → table
├── CUSTOM-SMT-LOGIC.md                     # Logic SMT step-by-step
└── iceberg_snapshot_metadata_guide.md      # Hướng dẫn fork + metadata injection
```

---

## Services

| Service | Port | Mô tả |
|---------|------|-------|
| Kafka | 9092, 29092 | Message broker (KRaft mode) |
| Kafka Connect | 8083 | Framework + SMT + Fork Connector |
| MinIO | 9000, 9001 | S3-compatible storage (data files) |
| Hive Metastore | 9083 | Iceberg catalog (metadata) |
| Trino | 8080 | Query engine |
| PostgreSQL | 5432 | Hive Metastore backend |
