# 📋 Chuẩn Hóa Metadata: Iceberg Snapshot Summary (Consumer Engine)

Dựa trên bảng mapping chuẩn hóa của toàn bộ hệ thống (NiFi, SmartFlow, Consumer) và kiến trúc API -> Topic, dưới đây là bộ tiêu chuẩn metadata sẽ được ghi vào thuộc tính `summary` của Iceberg Snapshot mỗi khi Kafka Connect thực hiện commit.

Mục tiêu: Đảm bảo tính nhất quán (Consistency), khả năng truy vết (Traceability) và giám sát (Monitoring) trên toàn bộ Data Lakehouse.

---

## 1. Bộ Tiêu Chuẩn Bắt Buộc (Theo thiết kế Mapping)

Các trường này bám sát 100% thiết kế trong bảng mapping của bạn để đảm bảo đồng nhất với các hệ thống khác.

| Key (Tên trường) | Giá trị ví dụ | Nguồn lấy dữ liệu / Ý nghĩa |
|-----------------|---------------|-----------------------------|
| `task.engine` | `consumer` | **Hardcode:** Cố định là `consumer` để hệ thống biết snapshot này sinh ra từ engine nào (tương tự `nifi` hay `SmartFlow`). |
| `consumer.connectorname` | `sink-qtmt-tramquantrac` | **Dynamic:** Tên của connector đang chạy. Dùng làm Foreign Key truy vết ngược qua Kafka Connect REST API. Không cần lưu Topic vì từ tên Connector có thể tra ra mọi thông tin cấu hình qua API. |
| `consumer.typeingest` | `API` | **Dynamic:** Phân biệt hệ thống gốc đổ data vào (VD: `API`, `CDC`). Nhận diện từ file JSON config. |
| `consumer.ingest.time` | `1779426966229` | **Sinh tự động (System Time):** Thời gian (Epoch ms) lúc connector chuẩn bị commit vào Iceberg. Sinh bằng lệnh Java `System.currentTimeMillis()`. |

---

## 2. Thông Tin Bổ Sung Cần Thiết (Dành riêng cho luồng streaming)

| Key (Tên trường) | Giá trị ví dụ | Tại sao lại thực sự cần thiết? |
|-----------------|---------------|--------------------------------|
| `consumer.vtts.time` | `1779426960000` | **Event Time / Watermark:** (Đổi tên từ `kafka.connect.vtts`). Đây là thời gian *sinh ra sự kiện* của message trễ nhất (Low watermark). <br>👉 *Tác dụng:* Tính **Latency**. `consumer.ingest.time` trừ đi `consumer.vtts.time` sẽ ra độ trễ từ lúc sự kiện sinh ra tới lúc vào Lakehouse. |

---

## 3. Hướng Dẫn Cập Nhật Vào Source Code

Bạn chỉ cần chỉnh sửa code tại 2 file sau trong thư mục `iceberg-kafka-connect-fork`.

### Bước 1: Khai báo property trong `IcebergSinkConfig.java`

```java
// Thêm vào trong hàm newConfigDef() để đọc từ file config JSON
configDef.define("task.engine", Type.STRING, "consumer", Importance.HIGH, "Loại xử lý");
configDef.define("consumer.typeingest", Type.STRING, "API", Importance.HIGH, "Loại Ingestion");
```

### Bước 2: Bơm Metadata lúc Commit trong `Coordinator.java`

Tìm đến method `commitToTable()`, cập nhật logic set summary map. Hãy chú ý giữ nguyên các thuộc tính nội bộ như `kafka.connect.offsets.*` vì framework cần nó để khôi phục lỗi.

```java
// Tạo biến thời gian thực lúc commit (ingest time)
String currentIngestTimeMs = String.valueOf(System.currentTimeMillis());

// ====== CẬP NHẬT NHÁNH AppendFiles (Khi chỉ có INSERT) ======
appendOp.set("task.engine", config.getString("task.engine"));
appendOp.set("consumer.typeingest", config.getString("consumer.typeingest"));
appendOp.set("consumer.connectorname", config.connectorName());
appendOp.set("consumer.ingest.time", currentIngestTimeMs);

if (vtts != null) {
    appendOp.set("consumer.vtts.time", Long.toString(vtts.toInstant().toEpochMilli()));
}

// ====== CẬP NHẬT NHÁNH RowDelta (Khi có UPDATE/DELETE) ======
deltaOp.set("task.engine", config.getString("task.engine"));
deltaOp.set("consumer.typeingest", config.getString("consumer.typeingest"));
deltaOp.set("consumer.connectorname", config.connectorName());
deltaOp.set("consumer.ingest.time", currentIngestTimeMs);

if (vtts != null) {
    deltaOp.set("consumer.vtts.time", Long.toString(vtts.toInstant().toEpochMilli()));
}
```
