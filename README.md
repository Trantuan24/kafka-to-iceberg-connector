# Iceberg Kafka Connect — API Lineage Pipeline

```
Kafka Topic → Custom SMT → Fork Iceberg Sink Connector → Iceberg (Hive + MinIO S3)
```

Pipeline ingest dữ liệu từ API/Kafka vào **Apache Iceberg**, có:
- **Dynamic routing** (1 connector ghi nhiều table theo nội dung message)
- **Snapshot metadata lineage** (mỗi commit ghi nguồn gốc dữ liệu vào snapshot summary)
- **2 chế độ**: **Append** (mặc định, raw JSON/XML) và **CDC** (version filter + dedup + upsert)

> Tài liệu này ưu tiên **Append mode**. Phần CDC ở mục [CDC mode](#cdc-mode) bên dưới.

---

## Mục lục

- [Kiến trúc](#kiến-trúc)
- [Yêu cầu](#yêu-cầu)
- [Khởi động stack](#khởi-động-stack)
- [Append mode (mặc định)](#append-mode-mặc-định)
- [CDC mode](#cdc-mode)
- [Snapshot metadata (lineage)](#snapshot-metadata-lineage)
- [Rebuild khi sửa code](#rebuild-khi-sửa-code)
- [Cấu trúc dự án](#cấu-trúc-dự-án)
- [Services](#services)

---

## Kiến trúc

| Thành phần | Vai trò |
|---|---|
| **Custom SMT** (`custom-smt/`) | `CustomCDCTransform` — biến đổi message trước khi ghi. 2 mode: `append` / `cdc` |
| **Fork Iceberg Connector** (`iceberg-kafka-connect-fork/`) | Fork của Tabular iceberg-kafka-connect — strip field nội bộ (`iceberg_table`, `_cdc_op`) + **inject metadata lineage** vào snapshot |
| **`connector-service/`** | Đóng gói image base `duytuan24/connector-service` (Kafka Connect + plugins) lên Docker Hub |
| **`plugins/`** | JAR đã build sẵn — `Dockerfile.connect` COPY từ đây vào image khi `docker compose build` |

> **Clone về là chạy được ngay** — JAR đã có sẵn trong `plugins/`, KHÔNG cần build Gradle. Chỉ build lại khi sửa source (xem [Rebuild](#rebuild-khi-sửa-code)).

---

## Yêu cầu

- Docker Desktop (Windows)
- Python 3 + `kafka-python` → `pip install kafka-python`
- PowerShell

---

## Khởi động stack

```powershell
docker compose build          # lần đầu, hoặc sau khi thay JAR
docker compose up -d
docker compose ps             # đợi ~90s để Connect load plugins
```

Kiểm tra sẵn sàng:

```powershell
Invoke-RestMethod "http://localhost:8083/"                                   # Connect (trả version)
docker exec kafka kafka-topics --bootstrap-server kafka:9092 --list          # topics
docker exec iceberg-kafka-connect-demo-trino-1 trino --execute "SELECT 1"    # Trino
```

---

## Append mode (mặc định)

**Mục đích:** ingest message thô (JSON hoặc XML) — mỗi message → **1 dòng**, lưu nguyên văn vào cột `record`. Không dedup, không version (append-only, at-least-once).

- `value.converter = StringConverter` → message giữ nguyên dạng chuỗi
- SMT `mode=append` sinh đúng **3 cột**: `id` (= `topic-partition-offset`), `record` (raw body), `ngay_cap_nhat` (`Instant.now()`)
- 1 topic = 1 định dạng (giống thực tế: 1 API endpoint → 1 topic → 1 format)

### Chạy nhanh (E2E tự động)

Script `run_append_e2e.ps1` làm trọn gói: build JAR → rebuild image → đợi Connect → tạo table → deploy connector → gửi test → verify.

```powershell
.\run_append_e2e.ps1
```

### Chạy thủ công

**1. Tạo table (3 cột):**

```powershell
docker exec iceberg-kafka-connect-demo-trino-1 trino --execute "CREATE SCHEMA IF NOT EXISTS iceberg.def"
docker exec iceberg-kafka-connect-demo-trino-1 trino --execute "CREATE TABLE IF NOT EXISTS iceberg.def.abc_append (id VARCHAR, record VARCHAR, ngay_cap_nhat VARCHAR) WITH (format = 'PARQUET')"
```

**2. Deploy connector:**

```powershell
$body = Get-Content "configs\sink.qtmt_append.json" -Raw
Invoke-RestMethod -Method Post "http://localhost:8083/connectors" -ContentType "application/json" -Body $body
Invoke-RestMethod "http://localhost:8083/connectors/sink-qtmt-append/status" | ConvertTo-Json -Depth 3
```

**3. Gửi message test:**

```powershell
python test_append.py            # 1 message JSON  (sample_new.json)
python test_append.py xml        # 1 message XML   (sample_new.xml, cần topic + table riêng)
python test_append_multi.py      # 9 message / 3 batch -> ép >=3 commit (test snapshot/checkpoint)
```

**4. Verify (đợi ~15s):**

```powershell
docker cp query-append.sql iceberg-kafka-connect-demo-trino-1:/tmp/q.sql
docker exec iceberg-kafka-connect-demo-trino-1 trino -f /tmp/q.sql
```

Mong đợi: số dòng = số message đã gửi, mỗi snapshot có metadata lineage (`consumer.connectorname`, `consumer.ingest.time`...).

### Config append (`configs/sink.qtmt_append.json`)

Điểm khác CDC:

```jsonc
"transforms.customCdc.mode": "append",                          // bật append
"transforms.customCdc.topic.table.map": "qtmt-append:def.abc_append",
"value.converter": "org.apache.kafka.connect.storage.StringConverter",  // raw passthrough
"key.converter":   "org.apache.kafka.connect.storage.StringConverter"
// KHÔNG có iceberg.tables.cdc-field, KHÔNG có iceberg.tables.default-id-columns
```

---

## CDC mode

**Mục đích:** xử lý CDC envelope (data/type/version/key) từ nguồn RDBMS — có lọc record cũ (out-of-order), dedup theo business key, hỗ trợ INSERT/UPDATE/DELETE (upsert). Output **8 cột**.

**1. Tạo table (8 cột):**

```powershell
docker exec iceberg-kafka-connect-demo-trino-1 trino --execute "CREATE TABLE IF NOT EXISTS iceberg.def.abc (id VARCHAR, dedup_key VARCHAR, record VARCHAR, version BIGINT, type VARCHAR, key VARCHAR, ngay_cap_nhat VARCHAR, length VARCHAR) WITH (format = 'PARQUET')"
```

**2. Deploy + test:**

```powershell
$body = Get-Content "configs\sink.qtmt_tramquantrac.json" -Raw
Invoke-RestMethod -Method Post "http://localhost:8083/connectors" -ContentType "application/json" -Body $body
python test.py                   # 9 message / 3 batch (INSERT/UPDATE/DELETE/Stale-DROP)
python test_checkpoint.py        # test crash recovery: kill -> gửi -> restart -> verify không mất data
```

**3. Verify (đợi ~15s):**

```powershell
docker exec iceberg-kafka-connect-demo-trino-1 trino --execute "SELECT dedup_key, type, version FROM iceberg.def.abc ORDER BY dedup_key"
```

| Aspect | Append | CDC |
|---|---|---|
| Converter | StringConverter | JsonConverter (schemas.enable=false) |
| Output | 3 cột | 8 cột |
| Version filter / dedup | ❌ | ✅ |
| Operations | append-only | INSERT/UPDATE/DELETE |
| Idempotent | at-least-once | mạnh (dedup + version) |
| Config | `mode=append` | `mode=cdc` (mặc định nếu bỏ trống) |
| Test | `test_append*.py` | `test.py`, `test_checkpoint.py` |

---

## Snapshot metadata (lineage)

Fork connector tự inject vào **snapshot summary** mỗi lần commit (chuẩn Consumer Engine):

```
task.engine              = "consumer"
consumer.typeingest      = "API"                    ← từ config
consumer.connectorname   = "sink-qtmt-append"       ← connector nào ghi
consumer.ingest.time     = <epoch ms khi commit>
consumer.vtts.time       = <watermark record mới nhất trong batch>
```

Truy vết: snapshot → `consumer.connectorname` → `GET /connectors/{name}/config` → biết topic, table, routing.

Query mẫu: `query-append.sql` (append), `query-snapshots.sql` / `query-checkpoint.sql` (CDC).

> Iceberg connector track offset **trong snapshot summary** (không dùng `kc-offsets`). Restart → đọc latest snapshot → tiếp tục đúng offset → không mất/trùng. Xem [docs/CHECKPOINT-EXPLAINED.md](docs/CHECKPOINT-EXPLAINED.md).

---

## Rebuild khi sửa code

> Chỉ cần khi **sửa source**. Sửa comment/Javadoc không đổi bytecode → bỏ qua.

### Sửa Custom SMT

```powershell
# Build JAR trong container (có sẵn Kafka Connect libs), ghi thẳng vào plugins/custom-smt/
docker run --rm -v "${PWD}:/work" duytuan24/connector-service:1.0 bash /work/build-in-container.sh

# Nạp JAR mới vào image + recreate
docker compose up -d --build connect
```

### Sửa Fork Connector

```powershell
# Build Gradle (Git Bash)
& "C:\Program Files\Git\bin\bash.exe" -c "cd '/d/nifi-test/iceberg-kafka-connect-demo/iceberg-kafka-connect-fork' && ./gradlew :iceberg-kafka-connect:jar -x test --no-daemon"

# Copy JAR (đổi tên theo manifest) + rebuild + recreate
Copy-Item -Force "iceberg-kafka-connect-fork\kafka-connect\build\libs\iceberg-kafka-connect.jar" "plugins\iceberg-kafka-connect\lib\iceberg-kafka-connect-custom-pipeline-meta.jar"
docker compose up -d --build connect
```

**QUAN TRỌNG:** luôn `docker compose ... --build` sau khi thay JAR. Không build = restart mất code.

### Dừng / Reset

```powershell
docker compose stop           # dừng, giữ data
docker compose down -v        # reset hoàn toàn (xóa volumes)
```

---

## Cấu trúc dự án

```
├── configs/
│   ├── sink.qtmt_append.json            # ★ Append connector (qtmt-append → def.abc_append)
│   └── sink.qtmt_tramquantrac.json      #   CDC connector    (qtmt-tramquantrac → def.abc)
│
├── custom-smt/                          # [SOURCE] SMT CustomCDCTransform (build ra JAR)
├── iceberg-kafka-connect-fork/          # [SOURCE] Fork Iceberg connector (build ra JAR)
├── connector-service/                   # [SOURCE] Image base lên Docker Hub
├── plugins/                             # JAR pre-built — Dockerfile.connect COPY từ đây
│   ├── custom-smt/custom-cdc-transform.jar
│   └── iceberg-kafka-connect/lib/
│
├── docker-compose.yml                   # toàn bộ stack
├── Dockerfile.connect                   # image Kafka Connect (FROM duytuan24/connector-service:1.0)
├── Dockerfile.hive                      # image Hive Metastore (+ S3)
├── hive-site.xml
├── trino-catalog/iceberg.properties     # catalog Trino
├── build-in-container.sh                # build SMT JAR trong container
│
├── run_append_e2e.ps1                   # ★ E2E append: build→deploy→test→verify
├── test_append.py / test_append_multi.py# ★ test append (1 msg / nhiều batch)
├── sample_new.json / sample_new.xml     # ★ payload mẫu append
├── query-append.sql                     # ★ verify append + lineage
│
├── test.py / test_checkpoint.py         # test CDC + crash recovery
├── sample_message1.json                 # mẫu CDC envelope
├── query-snapshots.sql / query-checkpoint.sql
│
└── docs/                                # tài liệu thiết kế + hướng dẫn chi tiết
```

---

## Services

| Service | Port | Mô tả |
|---|---|---|
| Kafka | 9092, 29092 | Message broker (KRaft mode) |
| Kafka Connect | 8083 | Framework + Custom SMT + Fork Connector |
| MinIO | 9000, 9001 | S3-compatible storage (data files) |
| Hive Metastore | 9083 | Iceberg catalog (metadata) |
| Trino | 8080 | Query engine |
| PostgreSQL | 5432 | Hive Metastore backend |
