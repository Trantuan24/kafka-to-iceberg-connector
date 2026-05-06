# RUNBOOK: PHASE 1 - KAFKA CONNECT ICEBERG SINK (APPEND MODE)

Tài liệu này đóng gói toàn bộ quy trình vận hành, cấu hình và testing cho **Phase 1** của Data Pipeline: `Kafka → Custom SMT → Official Iceberg Sink Connector → Iceberg Table`.

Trong Phase 1, pipeline hoạt động ở chế độ **Append-Only** (lưu lại mọi sự thay đổi dưới dạng event log, không xử lý upsert/delete vật lý).

---

## 1. TỔNG QUAN KIẾN TRÚC & LUỒNG DỮ LIỆU

### 1.1 Kiến trúc
- **Kafka**: Chứa topic `tram_quan_trac` lưu trữ raw CDC envelope messages.
- **Kafka Connect (Distributed Mode)**: Quản lý các connectors qua REST API (Port 8083).
- **Custom SMT (`CustomCDCTransform`)**: Xử lý logic chuyển đổi JSON schemaless thành Struct 7 fields có Schema chuẩn.
- **Official Iceberg Sink Connector**: Đọc Struct từ SMT và ghi trực tiếp thành các file Parquet.
- **Iceberg + Hive Metastore + MinIO**: Lưu trữ data và metadata của bảng `iceberg.default.tram_quan_trac_cdc`.

### 1.2 Format Input/Output

**Đầu vào (Kafka Message):**
```json
{
  "data": [
    {"MaTram": "TRAM001", "TenTram": "Trạm quan trắc Hà Nội 1"}
  ],
  "length": 1,
  "key": "MaTram",
  "type": "INSERT",
  "version": 1,
  "ngay_cap_nhat": "2026-05-05T10:00:00Z"
}
```

**Đầu ra (Iceberg Table - 7 Fields):**
| Cột | Kiểu Dữ Liệu | Giải Thích |
|-----|-------------|------------|
| `id` | VARCHAR | Generate từ `topic-partition-offset` (VD: `tram_quan_trac-0-12`) đảm bảo unique & có thể tracking |
| `record` | VARCHAR | Toàn bộ mảng `data[]` được stringify thành JSON string |
| `version` | BIGINT | Đã được cast sang `long` để phục vụ ordering sau này |
| `type` | VARCHAR | Giữ nguyên (INSERT/UPDATE/DELETE) |
| `key` | VARCHAR | Giữ nguyên tên trường dùng làm khóa (VD: `MaTram`) |
| `ngay_cap_nhat`| VARCHAR | Giữ nguyên tên field |
| `length` | VARCHAR | Giữ dạng String cho khớp schema |

*(Lưu ý: Không còn các field thừa như `dedup_key`, `ingest_time`, `_cdc`)*

---

## 2. QUY TRÌNH DEPLOYMENT (VẬN HÀNH)

### Bước 1: Khởi tạo/Restart hạ tầng
Hệ thống sử dụng Docker Compose. Đảm bảo chạy ở chế độ distributed:
```bash
docker-compose down
docker-compose up -d --build
```
*Thời gian chờ khởi động đầy đủ khoảng 40 giây.*

### Bước 2: Build SMT và Tạo Connector
Chạy script tự động xử lý mọi thao tác:
```bash
bash deploy-custom-smt.sh
```

**Script này sẽ làm gì?**
1. Gọi Gradle (qua Docker) để build lại `kafka-connect-custom-smt-2.0.0.jar` (~2MB).
2. Tự động drop table cũ (nếu sai schema) và tạo lại `iceberg.default.tram_quan_trac_cdc` chuẩn 7 fields.
3. Call REST API Kafka Connect (`POST http://localhost:8083/connectors`) để submit cấu hình connector.

### Bước 3: Kiểm tra trạng thái
```bash
curl -s http://localhost:8083/connectors/sink.tram_quan_trac_cdc_official/status | json_pp
```
**Chỉ tiêu thành công:** Connector `state = RUNNING`, Task 0 `state = RUNNING`.

---

## 3. TESTING & KIỂM CHỨNG

Chạy test suite đã được tinh chỉnh cho Phase 1:
```bash
python3 test-custom-smt.py
```
Script sẽ gửi 5 test case vào topic `tram_quan_trac`:
1. Single INSERT
2. Batch INSERT (mảng `data` có >1 phần tử)
3. UPDATE (append 1 dòng)
4. DELETE (append 1 dòng)
5. Full schema example

**Đọc kết quả:**
Sau khi đợi Iceberg commit (mặc định 10s theo config `iceberg.control.commit.interval-ms`), chạy script tổng hợp:
```bash
bash check-results.sh
```
Kết quả mong đợi:
- Data được ghi đủ.
- Consumer lag về 0.
- `id` có format đúng.

---

## 4. XỬ LÝ SỰ CỐ (TROUBLESHOOTING)

### Lỗi 1: `committed to 0 table(s)` liên tục
**Nguyên nhân:** Connector consume được message nhưng không biết đẩy vào bảng nào.
**Khắc phục:** 
- Đảm bảo cấu hình không chứa `route-regex` bị sai, hoặc `default-id-columns`.
- Ở Phase 1, chỉ cần cấu hình `iceberg.tables=default.tram_quan_trac_cdc`.

### Lỗi 2: Trino query báo `Table not found`
**Nguyên nhân:** Bảng chưa được tạo hoặc metadata Iceberg bị hỏng.
**Khắc phục:**
Chạy lại SQL tạo bảng qua Trino:
```sql
CREATE TABLE iceberg.default.tram_quan_trac_cdc (
    id VARCHAR, record VARCHAR, version BIGINT,
    type VARCHAR, key VARCHAR, ngay_cap_nhat VARCHAR, length VARCHAR
) WITH (format = 'PARQUET');
```

### Lỗi 3: CustomCDCTransform FAILED
**Nguyên nhân:** Dữ liệu Kafka gửi vào thiếu các field bắt buộc hoặc format sai.
**Khắc phục:** Kiểm tra log container:
```bash
docker logs --tail 100 iceberg-kafka-connect-demo-connect-1 | grep "CustomCDCTransform FAILED"
```
Đảm bảo Kafka message có format là 1 Object (hoặc JSON parsable string) chứa `data`, `type`, `version`.

---

## 5. PHẠM VI CHƯA XỬ LÝ (DÀNH CHO PHASE 2)

Pipeline hiện tại đã **đóng băng (freeze)** chuẩn cho Phase 1. Các vấn đề dưới đây được chuyển giao cho Phase 2:
1. **CDC / Upsert:** `UPDATE` và `DELETE` hiện tại chỉ đang sinh ra dòng mới.
2. **Version Control:** Chưa dùng `max(version)` để giải quyết conflict.
3. **Identity Columns:** Sẽ cần config `iceberg.tables.default-id-columns` và chèn thêm `_cdc` metadata vào SMT.
