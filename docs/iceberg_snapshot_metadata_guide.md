# Iceberg Snapshot Metadata Injection — Toàn bộ quá trình

## 1. Bối cảnh & Mục tiêu

### Pipeline hiện tại
```
Producer (JSON) → Kafka → Custom SMT → Tabular Iceberg Sink Connector → Iceberg (MinIO + Hive)
```

### Vấn đề
- Tabular Iceberg Sink v0.6.19 (JAR gốc) **không có cách inject custom metadata** vào Iceberg snapshot.
- Cần gắn 3 fields vào mỗi snapshot để phục vụ **data lineage**:
  - `pipeline.snapshot-uuid` — UUID duy nhất **per table per commit** (để trace snapshot)
  - `pipeline.topic` — Kafka topic/table name (nguồn dữ liệu của snapshot này)
  - `pipeline.source-type` — Loại nguồn (default `"API"`)
- Metadata phải nằm ở **snapshot summary** (không phải trong row data) → không ảnh hưởng schema nghiệp vụ.

---

## 2. Kiến trúc mới (After)

```
Producer (JSON)
    │
    ▼
  Kafka Topics
  (qtmt-tramquantrac, qtmt-quantrackhithai, ...)
    │
    ▼
  Custom SMT (giữ nguyên)
  - Version filter (lọc dedup_key)
  - Xử lý U/I/D (_cdc_op)
  - Inject field iceberg_table (routing)
    │
    ▼
  ┌──────────────────────────────────────────────┐
  │  FORKED Iceberg Sink Connector               │
  │  (databricks/iceberg-kafka-connect fork)     │
  │                                              │
  │  Worker.java  ←── nhận SinkRecord           │
  │      │  route theo iceberg_table field       │
  │      ▼                                       │
  │  RecordWriter per Table                      │
  │      │  write DataFile/DeleteFile            │
  │      ▼                                       │
  │  Coordinator.java  ←── commit cycle          │
  │      │  commitToTable() gọi 1 lần/table      │
  │      │  ┌─────────────────────────────┐      │
  │      │  │ inject vào snapshot summary │      │
  │      │  │ pipeline.snapshot-uuid = UUID│      │
  │      │  │ pipeline.topic = table.name  │      │
  │      │  │ pipeline.source-type = "API" │      │
  │      │  └─────────────────────────────┘      │
  │      ▼                                       │
  │  AppendFiles.commit() / RowDelta.commit()    │
  └──────────────────────────────────────────────┘
    │
    ▼
  Iceberg Table (MinIO + Hive Metastore)
  Snapshot Summary chứa metadata lineage ✅
```

### Điểm quan trọng về `commitToTable()`:

| Thời điểm | Đơn vị | Ý nghĩa |
|---|---|---|
| 1 commit cycle | Toàn bộ connector | `kafka.connect.commit-id` (UUID) — **shared** giữa mọi tables |
| 1 `commitToTable()` | 1 table cụ thể | `pipeline.snapshot-uuid` (UUID) — **unique per table** |

→ Table A và Table B **cùng commit cycle** → `commit-id` giống nhau, nhưng `snapshot-uuid` **khác nhau** ✅

---

## 3. Quá trình Fork & Build

### Bước 1: Clone repo

```bash
# Clone databricks/iceberg-kafka-connect (main branch)
git clone https://github.com/databricks/iceberg-kafka-connect.git iceberg-kafka-connect-fork
```

> Repo gốc Tabular đã được donate sang Apache rồi move về Databricks.
> Package name vẫn giữ: `io.tabular.iceberg.connect`

### Bước 2: Kiểm tra cấu trúc project

```
iceberg-kafka-connect-fork/
├── kafka-connect/            ← module chính (tên project: iceberg-kafka-connect)
├── kafka-connect-events/     ← events module
├── kafka-connect-transforms/ ← transforms module
├── kafka-connect-runtime/    ← runtime/packaging
├── build.gradle
└── settings.gradle           ← project name mapping
```

`settings.gradle` quan trọng:
```gradle
include "iceberg-kafka-connect"
project(":iceberg-kafka-connect").projectDir = file("kafka-connect")
```

### Bước 3: Build JAR

```bash
# Dùng Git Bash trên Windows (gradlew là Unix script)
& "C:\Program Files\Git\bin\bash.exe" -c `
  "cd '/d/nifi-test/iceberg-kafka-connect-demo/iceberg-kafka-connect-fork' && `
   ./gradlew :iceberg-kafka-connect:jar -x test --no-daemon"
```

Output JAR:
```
kafka-connect/build/libs/iceberg-kafka-connect-0.7.0-dev.1.uncommitted+4ac3ebb.jar
```

---

## 4. Các thay đổi Code

### File 1: `IcebergSinkConfig.java`

**Vị trí:**
```
kafka-connect/src/main/java/io/tabular/iceberg/connect/IcebergSinkConfig.java
```

**Thêm constant** (gần các props khác):
```java
private static final String PIPELINE_SOURCE_TYPE_PROP    = "pipeline.source-type";
private static final String PIPELINE_SOURCE_TYPE_DEFAULT = "API";
```

**Đăng ký vào ConfigDef** (trong method `newConfigDef()`):
```java
configDef.define(
    PIPELINE_SOURCE_TYPE_PROP,
    Type.STRING,
    PIPELINE_SOURCE_TYPE_DEFAULT,
    Importance.LOW,
    "Source type label injected into Iceberg snapshot summary for lineage (default: API)");
```

**Thêm method accessor**:
```java
/** Returns the pipeline source-type label for snapshot metadata (default: "API"). */
public String pipelineSourceType() {
    return getString(PIPELINE_SOURCE_TYPE_PROP);
}
```

---

### File 2: `Coordinator.java`

**Vị trí:**
```
kafka-connect/src/main/java/io/tabular/iceberg/connect/channel/Coordinator.java
```

**Thêm import UUID**:
```java
import java.util.UUID;
```

**Thêm 3 constants** (thay thế các constants cũ):
```java
// Pipeline lineage metadata (per-table, per-snapshot)
private static final String PIPELINE_SNAPSHOT_UUID_PROP = "pipeline.snapshot-uuid";
private static final String PIPELINE_TOPIC_PROP         = "pipeline.topic";
private static final String PIPELINE_SOURCE_TYPE_PROP   = "pipeline.source-type";
```

**Trong method `commitToTable()`** — thêm vào đầu khối `else`:
```java
if (dataFiles.isEmpty() && deleteFiles.isEmpty()) {
    LOG.info("Nothing to commit to table {}, skipping", tableIdentifier);
} else {
    // Per-table, per-snapshot lineage metadata
    String snapshotUuid = UUID.randomUUID().toString();        // Unique per table per commit!
    String tableTopic   = tableIdentifier.name();              // "qtmt_tramquantrac" (1:1 với Kafka topic)
    String sourceType   = config.pipelineSourceType();         // "API" (từ config)

    if (deleteFiles.isEmpty()) {
        // Nhánh AppendFiles (chỉ INSERT)
        AppendFiles appendOp = transaction.newAppend();
        appendOp.set(COMMIT_ID_SNAPSHOT_PROP, commitState.currentCommitId().toString());
        appendOp.set(PIPELINE_SNAPSHOT_UUID_PROP, snapshotUuid);   // ← inject
        appendOp.set(PIPELINE_TOPIC_PROP, tableTopic);             // ← inject
        appendOp.set(PIPELINE_SOURCE_TYPE_PROP, sourceType);       // ← inject
        // ...
        appendOp.commit();
    } else {
        // Nhánh RowDelta (CDC: U/I/D có delete files)
        RowDelta deltaOp = table.newRowDelta();
        deltaOp.set(COMMIT_ID_SNAPSHOT_PROP, commitState.currentCommitId().toString());
        deltaOp.set(PIPELINE_SNAPSHOT_UUID_PROP, snapshotUuid);    // ← inject
        deltaOp.set(PIPELINE_TOPIC_PROP, tableTopic);              // ← inject
        deltaOp.set(PIPELINE_SOURCE_TYPE_PROP, sourceType);        // ← inject
        // ...
        deltaOp.commit();
    }
}
```

> ⚠️ Phải inject vào **cả 2 nhánh** (AppendFiles và RowDelta).
> `AppendFiles` = chỉ có data files (INSERT thuần).
> `RowDelta` = có cả delete files (CDC với U/D operations).

---

### File 3: `IcebergWriter.java`

**Vị trí:**
```
kafka-connect/src/main/java/io/tabular/iceberg/connect/data/IcebergWriter.java
```

**Mục đích:** Loại bỏ các field nội bộ (`iceberg_table`, `_cdc_op`) khỏi record trước khi ghi vào Iceberg, giữ cho schema bảng sạch sẽ chỉ với 8 fields nghiệp vụ.

**Thay đổi:**
- Chuyển method `stripInternalFields` từ `private` sang `public static` (và truyền thêm config) để có thể được gọi từ bên ngoài.
- Method này tạo ra một Schema và Struct mới hoàn toàn không chứa 2 cột routing/CDC nói trên.

---

### File 4: `IcebergWriterFactory.java`

**Vị trí:**
```
kafka-connect/src/main/java/io/tabular/iceberg/connect/data/IcebergWriterFactory.java
```

**Mục đích:** Đảm bảo khi connector tính năng tự động tạo bảng (`auto-create table`), nó không vô tình đưa 2 field nội bộ vào schema của bảng mới.

**Thay đổi:**
Trong method `autoCreateTable()`, bổ sung bước gọi hàm strip trước khi suy luận schema:
```java
@VisibleForTesting
Table autoCreateTable(String tableName, SinkRecord sample) {
    try {
        // Gọi hàm strip để loại bỏ iceberg_table và _cdc_op trước!
        SinkRecord strippedSample = IcebergWriter.stripInternalFields(sample, config);
        
        StructType structType;
        if (strippedSample.valueSchema() == null) {
            structType = SchemaUtils.inferIcebergType(strippedSample.value(), config)...
        } else {
            structType = SchemaUtils.toIcebergType(strippedSample.valueSchema(), config)...
        }
        // ... (tạo bảng bằng schema đã được làm sạch)
    }
}
```

---

### File 5: `EventDecoder.java`

**Vị trí:**
```
kafka-connect/src/main/java/io/tabular/iceberg/connect/channel/EventDecoder.java
```

**Mục đích:** Khắc phục lỗi build Gradle không tìm thấy package `io.tabular.iceberg.connect.events.*` do dependency events bị rỗng (xảy ra trong lúc khôi phục lại gradle wrapper của fork).

**Thay đổi:**
- Loại bỏ toàn bộ code *legacy fallback* dành cho bản Iceberg 1.4.x (vì triển khai mới hoàn toàn dùng message format 1.5.x+).
- Chỉ giữ lại duy nhất hàm `AvroUtil.decode(value)`. Việc này giúp quá trình build không còn bị phụ thuộc vào class cũ nữa.

---

## 5. Deploy lên môi trường Docker

### Cấu trúc plugins trong dự án:
```
plugins/
└── iceberg-kafka-connect/
    └── lib/
        ├── iceberg-kafka-connect-0.6.19.jar.bak    ← đã backup JAR cũ
        ├── iceberg-kafka-connect-custom-pipeline-meta.jar  ← JAR fork mới
        ├── iceberg-kafka-connect-events-0.6.19.jar
        ├── iceberg-kafka-connect-events-1.5.2.jar
        └── iceberg-kafka-connect-transforms-0.6.19.jar
```

> Kafka Connect chỉ load file `.jar`, bỏ qua `.bak` → an toàn.

### Dockerfile.connect:
```dockerfile
FROM confluentinc/cp-kafka-connect-base:8.0.3
COPY plugins/iceberg-kafka-connect /usr/share/java/iceberg-kafka-connect
COPY plugins/custom-smt /usr/share/java/custom-smt
ENV CONNECT_PLUGIN_PATH="/usr/share/java,/usr/share/confluent-hub-components"
```

> Plugins được **COPY vào image** (không phải volume mount) → mỗi khi swap JAR phải **rebuild image**.

### Quy trình deploy:
```bash
# 1. Copy JAR mới vào plugins/
Copy-Item -Force `
  "iceberg-kafka-connect-fork\kafka-connect\build\libs\*.jar" `
  "plugins\iceberg-kafka-connect\lib\iceberg-kafka-connect-custom-pipeline-meta.jar"

# 2. Rebuild Docker image
docker compose build connect

# 3. Recreate container (không restart toàn bộ stack)
docker compose up -d --no-deps connect
```

---

## 6. Verify — Query Snapshot Metadata

File `check_snapshot_meta.sql`:
```sql
SELECT
  snapshot_id,
  committed_at,
  operation,
  summary['pipeline.snapshot-uuid'] AS snapshot_uuid,
  summary['pipeline.topic']         AS topic,
  summary['pipeline.source-type']   AS source_type,
  summary['kafka.connect.commit-id'] AS commit_id_shared
FROM iceberg.default."tram_quan_trac_cdc_v2$snapshots"
ORDER BY committed_at DESC
LIMIT 5;
```

**Chạy query:**
```bash
docker cp check_snapshot_meta.sql iceberg-kafka-connect-demo-trino-1:/tmp/check.sql
docker exec iceberg-kafka-connect-demo-trino-1 trino --file /tmp/check.sql
```

**Kết quả mong đợi (2 tables cùng commit cycle):**
```
snapshot_id | topic                  | snapshot_uuid        | source_type | commit_id_shared
------------|------------------------|----------------------|-------------|------------------
111111111   | qtmt_tramquantrac      | 3f8a1b2c-xxxx-...   | API         | abc-def-... (chung)
222222222   | qtmt_quantrackhithai   | 9d4e7a1f-yyyy-...   | API         | abc-def-... (chung)
```

> `snapshot-uuid` khác nhau dù `commit-id` giống → đúng yêu cầu ✅

---

## 7. Cách thêm metadata mới vào snapshot (Recipe)

Khi cần thêm 1 field mới, ví dụ `pipeline.env = "production"`:

### Bước 1: Thêm constant trong `Coordinator.java`
```java
private static final String PIPELINE_ENV_PROP = "pipeline.env";
```

### Bước 2: Inject vào cả 2 nhánh trong `commitToTable()`
```java
// Nhánh AppendFiles:
appendOp.set(PIPELINE_ENV_PROP, config.pipelineEnv());

// Nhánh RowDelta:
deltaOp.set(PIPELINE_ENV_PROP, config.pipelineEnv());
```

### Bước 3 (nếu lấy từ config): Cập nhật `IcebergSinkConfig.java`
```java
// Constant:
private static final String PIPELINE_ENV_PROP = "pipeline.env";

// Đăng ký ConfigDef:
configDef.define(PIPELINE_ENV_PROP, Type.STRING, "production", Importance.LOW, "...");

// Accessor method:
public String pipelineEnv() {
    return getString(PIPELINE_ENV_PROP);
}
```

### Bước 4: Build lại JAR + rebuild image
```bash
./gradlew :iceberg-kafka-connect:jar -x test --no-daemon
docker compose build connect && docker compose up -d --no-deps connect
```

### Bước 5: Thêm vào connector config (nếu muốn override):
```json
{
  "config": {
    "pipeline.source-type": "MySQL-CDC",
    "pipeline.env": "production"
  }
}
```

---

## 8. Snapshot Summary — Tất cả fields hiện có

| Key | Nguồn | Ý nghĩa |
|-----|--------|---------|
| `kafka.connect.commit-id` | Connector (có sẵn) | UUID của 1 commit cycle — shared giữa tất cả tables |
| `kafka.connect.vtts` | Connector (có sẵn) | Valid-to-timestamp (epoch ms) |
| `kafka.connect.offsets.<topic>.<group>` | Connector (có sẵn) | Kafka offsets đã commit |
| `pipeline.snapshot-uuid` | **Fork mới** | UUID unique per table per commit |
| `pipeline.topic` | **Fork mới** | Table short name (= Kafka topic name convention) |
| `pipeline.source-type` | **Fork mới** | Loại nguồn, default `"API"` |

---

## 9. Lineage Query — Từ Row đến Snapshot

```sql
-- Tìm snapshot chứa 1 row cụ thể
SELECT
  f.file_path,
  s.snapshot_id,
  s.committed_at,
  s.summary['pipeline.snapshot-uuid'] AS snapshot_uuid,
  s.summary['pipeline.topic']         AS source_topic,
  s.summary['pipeline.source-type']   AS source_type
FROM iceberg.default.qtmt_tramquantrac FOR VERSION AS OF <snapshot_id>
JOIN iceberg.default."qtmt_tramquantrac$files" f ON true
JOIN iceberg.default."qtmt_tramquantrac$snapshots" s
  ON s.snapshot_id = f.added_snapshot_id
WHERE <dedup_key_condition>;
```

**Flow truy vết:**
```
Row data
  └─► "$path" column → file_path
        └─► $files table → added_snapshot_id
              └─► $snapshots table → snapshot summary
                    └─► pipeline.snapshot-uuid, pipeline.topic, pipeline.source-type ✅
```
