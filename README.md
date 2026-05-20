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

### Bước 3: Tạo table và deploy connector

```powershell
# Tạo namespace + table
docker exec iceberg-kafka-connect-demo-trino-1 trino --execute "CREATE SCHEMA IF NOT EXISTS iceberg.def"
docker exec iceberg-kafka-connect-demo-trino-1 trino --execute "CREATE TABLE IF NOT EXISTS iceberg.def.abc (id VARCHAR, dedup_key VARCHAR, record VARCHAR, version BIGINT, type VARCHAR, key VARCHAR, ngay_cap_nhat VARCHAR, length VARCHAR) WITH (format = 'PARQUET')"

# Deploy connector
$body = Get-Content "configs\sink.qtmt_tramquantrac.json" -Raw
Invoke-RestMethod -Method Post "http://localhost:8083/connectors" -ContentType "application/json" -Body $body

# Kiểm tra status (connector + task phải RUNNING)
Invoke-RestMethod "http://localhost:8083/connectors/sink-qtmt-tramquantrac/status" | ConvertTo-Json -Depth 3
```

### Bước 4: Gửi test messages

```powershell
python test.py
```

Test gửi 9 messages trong 3 batches (INSERT/UPDATE/DELETE/Stale DROP) trên topic `qtmt-tramquantrac`.

### Bước 5: Verify data (đợi 15s sau khi test xong)

```powershell
docker exec iceberg-kafka-connect-demo-trino-1 trino --execute "SELECT dedup_key, type, version FROM iceberg.def.abc ORDER BY dedup_key"
```

Kết quả mong đợi (4 rows):
```
TRAM001||TRAM002  UPDATE  2
TRAM003           INSERT  1
TRAM005||TRAM006  UPDATE  2
TRAM007||TRAM008  INSERT  1
```

### Bước 6: Verify snapshot metadata (lineage)

```powershell
docker cp query-snapshots.sql iceberg-kafka-connect-demo-trino-1:/tmp/qs.sql
docker exec iceberg-kafka-connect-demo-trino-1 trino -f /tmp/qs.sql
```

Mong đợi mỗi snapshot có:
```
connector.name = sink-qtmt-tramquantrac
typeingest     = API
```

### Bước 7: Test checkpoint recovery (optional)

```powershell
python test_checkpoint.py
```

Test tự động: kill connector → gửi message khi down → restart → verify không mất data.

---

## Topic → Table Mapping

Config trong `configs/sink.qtmt_tramquantrac.json`:

```json
"transforms.customCdc.topic.table.map": "qtmt-tramquantrac:def.abc"
```

| Topic | Table đích |
|-------|------------|
| `qtmt-tramquantrac` | `def.abc` (custom map) |

---

## Rebuild khi sửa code

> **LƯU Ý:** Khi clone repo mới về, KHÔNG cần build Gradle. JARs đã có sẵn trong `plugins/`. Chỉ cần `docker compose build` → `docker compose up -d` là chạy. Phần dưới đây chỉ dùng khi bạn SỬA source code.

### Sửa Custom SMT:

```powershell
# Build JAR trong container
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
│   └── sink.qtmt_tramquantrac.json          # Connector config (topic → def.abc)
├── custom-smt/
│   └── src/main/.../CustomCDCTransform.java  # SMT: version filter, routing, transform
├── iceberg-kafka-connect-fork/
│   └── kafka-connect/src/...                 # Fork connector (strip fields, inject metadata)
├── plugins/
│   ├── custom-smt/custom-cdc-transform.jar   # SMT JAR (pre-built)
│   └── iceberg-kafka-connect/lib/            # Fork connector JARs + dependencies
├── docs/                                     # Documentation
├── trino-catalog/iceberg.properties          # Trino catalog config
├── docker-compose.yml                        # All services
├── Dockerfile.connect                        # Kafka Connect image
├── Dockerfile.hive                           # Hive Metastore image
├── test.py                                   # Test CDC pipeline (9 messages, I/U/D/Stale)
├── test_checkpoint.py                        # Test crash recovery
├── query-snapshots.sql                       # Query snapshot metadata
├── query-checkpoint.sql                      # Query checkpoint offsets
├── sample_message1.json                      # Message format mẫu
└── split_config.json                         # Config structure reference
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

---

## Snapshot Metadata

Mỗi Iceberg commit ghi vào snapshot summary:

```
connector.name = "sink-qtmt-tramquantrac"   ← connector nào ghi
typeingest     = "API"                       ← loại ingestion
```

Truy vết: snapshot → connector.name → GET /connectors/{name}/config → biết topic, table, routing.

---

## Checkpoint

Iceberg connector track offset trong **snapshot summary** (không dùng `kc-offsets`):

```
kafka.connect.offsets.control-iceberg.cg-control-sink-qtmt-tramquantrac = {"0": N}
```

Khi restart → đọc latest snapshot → biết tiếp tục từ offset nào → không mất data.
