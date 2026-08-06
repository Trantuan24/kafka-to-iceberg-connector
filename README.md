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

## Append mode (API push)

**Muc dich:** moi Kafka message duoc append thanh mot dong landing. Kafka value duoc luu nguyen ven vao `data`; SMT khong parse JSON/XML, khong CDC va khong dedup theo business key.

SMT tao 9 cot:

- `loainguon`: config, mac dinh `api_push`
- `manguondulieu`: `topic-partition-offset`
- `sukien`: `null`
- `phienban`: version contract landing, mac dinh `1`
- `body`: Kafka header `api.body`, khong bat buoc
- `header`: Kafka header `api.headers` sau khi loc allowlist an toan
- `data`: toan bo raw Kafka value
- `ingest_date`, `ingest_time`: cung mot thoi diem xu ly, mac dinh mui gio `Asia/Ho_Chi_Minh`

Chi `content-type`, `accept`, `user-agent`, `x-request-id`, `x-correlation-id` va `traceparent` duoc luu. `Authorization`, cookie, API/access/secret key khong bao gio duoc ghi vao bang.

Chi tiet contract va rollout: `docs/append-api-push-plan.md`.

### Chay nhanh (E2E tu dong)

Script `run_append_e2e.ps1` build JAR, rebuild Connect, tao bang v2, deploy connector, gui test va verify:

```powershell
.\run_append_e2e.ps1
```

### Chay thu cong

**1. Tao bang truoc khi deploy connector:**

```powershell
docker exec iceberg-kafka-connect-demo-trino-1 trino --execute "CREATE SCHEMA IF NOT EXISTS iceberg.def"
docker exec iceberg-kafka-connect-demo-trino-1 trino --execute "CREATE TABLE IF NOT EXISTS iceberg.def.abc_append_v2 (loainguon VARCHAR, manguondulieu VARCHAR, sukien VARCHAR, phienban INTEGER, body VARCHAR, header VARCHAR, data VARCHAR, ingest_date DATE, ingest_time TIMESTAMP(3)) WITH (format = 'PARQUET')"
```

**2. Deploy connector:**

```powershell
$body = Get-Content "configs\sink.qtmt_append.json" -Raw
Invoke-RestMethod -Method Post "http://localhost:8083/connectors" -ContentType "application/json" -Body $body
Invoke-RestMethod "http://localhost:8083/connectors/sink-qtmt-append/status" | ConvertTo-Json -Depth 3
```

**3. Gui message test:**

```powershell
python test_append.py
python test_append_multi.py
```

Producer gui raw value va co the them hai Kafka headers UTF-8: `api.body` va `api.headers`. Request `--data ''` khong co tham so nghiep vu se cho `body = null`; raw Kafka value rong van duoc giu la `data = ''`.

**4. Verify sau chu ky commit:**

```powershell
docker cp query-append.sql iceberg-kafka-connect-demo-trino-1:/tmp/q.sql
docker exec iceberg-kafka-connect-demo-trino-1 trino -f /tmp/q.sql
```

### Config append

```jsonc
"iceberg.tables.auto-create-enabled": "false",
"iceberg.tables.evolve-schema-enabled": "false",
"transforms.customCdc.mode": "append",
"transforms.customCdc.topic.table.map": "qtmt-append:def.abc_append_v2",
"transforms.customCdc.append.source.type": "api_push",
"transforms.customCdc.append.schema.version": "1",
"transforms.customCdc.append.timezone": "Asia/Ho_Chi_Minh",
"transforms.customCdc.append.body.header": "api.body",
"transforms.customCdc.append.headers.header": "api.headers",
"value.converter": "org.apache.kafka.connect.storage.StringConverter",
"header.converter": "org.apache.kafka.connect.storage.StringConverter"
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
│   ├── sink.qtmt_append.json            # ★ Append connector (qtmt-append → def.abc_append_v2)
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
