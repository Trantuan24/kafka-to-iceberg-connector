# Phase 2 — CDC/Upsert: Ghi chép hiện trạng

> Cập nhật: 2026-05-05

---

## 1. Mục tiêu Phase 2

Chuyển từ **append-only** (Phase 1) sang **CDC/upsert thật** với Iceberg Kafka Connect:

- `INSERT` → ghi row mới
- `UPDATE` → xóa row cũ (cùng key) + ghi row mới → kết quả: chỉ còn bản mới nhất
- `DELETE` → xóa hoàn toàn row khỏi Iceberg table

---

## 2. Những gì đã làm được trong session này

### 2.1 Xác định root cause connector Apache không có CDC

Bằng cách decompile `IcebergSinkConfig.class` trong JAR đang chạy:

```
# JAR: iceberg-kafka-connect-8f1f483.dirty.jar (Apache official)
# Kết quả: KHÔNG có các config sau:
#   ❌ iceberg.tables.cdc-field
#   ❌ iceberg.tables.upsert-mode-enabled
```

→ **Apache official connector không hỗ trợ CDC/upsert**. Các config này chỉ có trong **Databricks/Tabular fork**.

### 2.2 Download và cài đặt Tabular connector v0.6.19

- Download: `iceberg-kafka-connect-runtime-hive-0.6.19.zip` từ GitHub Releases
- Copy các JAR vào container:
  - `iceberg-kafka-connect-0.6.19.jar`
  - `iceberg-kafka-connect-events-0.6.19.jar`
  - `iceberg-kafka-connect-events-1.5.2.jar` ← quan trọng, có class `org.apache.iceberg.connect.events.Payload`
  - `iceberg-kafka-connect-transforms-0.6.19.jar`
- Xóa Apache JARs cũ ra khỏi plugin dir

### 2.3 Cập nhật connector config

File: `configs/sink.tram_quan_trac_cdc_v2.json`

```json
"connector.class": "io.tabular.iceberg.connect.IcebergSinkConnector",
"iceberg.catalog": "default",
"iceberg.tables.default-id-columns": "dedup_key",
"iceberg.tables.cdc-field": "_cdc_op",
"iceberg.tables.upsert-mode-enabled": "true"
```

### 2.4 Plugin Tabular load thành công

```
GET /connector-plugins → ["io.tabular.iceberg.connect.IcebergSinkConnector"]
Connector state: RUNNING
Task state:      RUNNING
```

### 2.5 Commit metrics xác nhận CDC đang chạy

Trước (Apache connector, append-only):
```
operation=append
addedEqualityDeleteFiles=0
```

Sau (Tabular connector):
```
operation=overwrite          ← KHÔNG còn append!
addedEqualityDeleteFiles=1   ← Equality delete đã chạy!
addedPositionalDeleteFiles=1
```

---

## 3. Kết quả test thực tế

### 3.1 Messages gửi (test-custom-smt-v2.py)

| # | TRAM | version | type |
|---|------|---------|------|
| 1 | TRAM001 | v1 | INSERT |
| 2 | TRAM001 | v2 | UPDATE |
| 3 | TRAM002 | v3 | INSERT → rồi DELETE |
| 4 | TRAM003 | v1, v5, v4 | INSERT + 2 UPDATE out-of-order |

### 3.2 Kết quả query Trino

```
dedup_key | type   | _cdc_op | version
----------|--------|---------|--------
TRAM001   | UPDATE | U       | 2        ← ✅ Upsert OK! v1 đã bị xóa
TRAM002   | INSERT | I       | 3        ← ⚠️ vẫn còn
TRAM002   | DELETE | D       | 3        ← ⚠️ DELETE không xóa được INSERT
TRAM003   | INSERT | I       | 1        ← ⚠️ 3 rows còn đủ
TRAM003   | UPDATE | U       | 4
TRAM003   | UPDATE | U       | 5
```

---

## 4. Vấn đề còn tồn tại

### Vấn đề A: TRAM002 — DELETE không xóa được row INSERT

**Nguyên nhân:** Cả INSERT và DELETE của TRAM002 nằm trong **cùng 1 batch commit**.  
Equality delete chỉ xóa được các rows đã commit ở snapshot TRƯỚC — không xóa được row cùng batch.

**Diễn giải:**
```
Batch 1: INSERT TRAM002 + DELETE TRAM002  ← cùng commit
→ Connector: equality delete TRAM002 (xóa rows cũ) + insert cả 2 rows mới
→ Kết quả: cả INSERT và DELETE đều còn trong table
```

Nếu INSERT và DELETE gửi **cách nhau đủ lâu** (hơn `commit.interval-ms=10s`) thì sẽ đúng.

### Vấn đề B: upsert-mode + cdc-field đang bật đồng thời

Hiện config đang có cả 2:
```json
"iceberg.tables.cdc-field": "_cdc_op",
"iceberg.tables.upsert-mode-enabled": "true"
```

Hai mode này **xung đột nhau về semantic DELETE**:

| Mode | Hành vi khi `_cdc_op=D` |
|------|------------------------|
| `upsert-mode-enabled=true` | Xóa row cũ → insert row DELETE mới (row DELETE vẫn còn trong table) |
| `cdc-field` only | Đọc D → chỉ xóa row, KHÔNG insert row mới |

→ **Nên dùng `cdc-field` only (không bật upsert-mode)** nếu muốn DELETE thật.

### Vấn đề C: TRAM003 out-of-order không được de-dup

v4 và v5 đều còn — connector không tự lọc version cũ, chỉ upsert theo dedup_key.

---

## 5. Hướng xử lý tiếp (chưa làm)

### Option A — Pure CDC mode (recommended)
```json
"iceberg.tables.cdc-field": "_cdc_op",
"iceberg.tables.upsert-mode-enabled": "false"
```
- `I/U` → insert row (có xóa row cũ theo identity column trước)
- `D` → chỉ xóa row, không insert

### Option B — Pure upsert mode (không có DELETE thật)
```json
"iceberg.tables.upsert-mode-enabled": "true"
// không có cdc-field
```
- Mọi record đều replace row cũ (kể cả DELETE chỉ là update)

### Option C — Fix batch timing
Điều chỉnh test để INSERT và DELETE gửi **cách nhau > 10 giây** → 2 batch commit riêng.

---

## 6. Trạng thái files hiện tại

| File | Trạng thái |
|------|-----------|
| `configs/sink.tram_quan_trac_cdc_v2.json` | Tabular class, cdc-field + upsert-mode cả 2 bật |
| `custom-smt/.../CustomCDCTransform.java` | Output `_cdc_op` = I/U/D |
| `plugins/iceberg-kafka-connect/lib/` | Có Tabular JARs 0.6.19 + 1.5.2 |
| Connector trong Kafka Connect | RUNNING |

## 7. Ghi chú kỹ thuật quan trọng

- **Tabular connector v0.6.19** đã migrate sang package `org.apache.iceberg` (thay vì `io.tabular`), nhưng vẫn dùng class name `io.tabular.iceberg.connect.IcebergSinkConnector`
- Cần **cả 2 events JAR**: `iceberg-kafka-connect-events-0.6.19.jar` (io.tabular package) + `iceberg-kafka-connect-events-1.5.2.jar` (org.apache.iceberg package) — thiếu 1 trong 2 sẽ lỗi ClassNotFoundException
- Network trong môi trường không kết nối được Docker Hub — rebuild image phải dùng cache
- Network kết nối được Maven Central và GitHub API từ trong container
