# Checkpoint & Offset trong Kafka Connect

## Tổng quan

Kafka Connect (distributed mode) dùng **3 internal topics** trên Kafka broker để lưu trạng thái. Tất cả connector trên cùng Connect cluster chia sẻ chung 3 topic này.

## 3 Internal Topics

| Topic | Config | Chứa gì |
|---|---|---|
| `kc-offsets` | `CONNECT_OFFSET_STORAGE_TOPIC` | Offset đã xử lý xong của từng connector |
| `kc-config` | `CONNECT_CONFIG_STORAGE_TOPIC` | Config JSON của các connector đang chạy |
| `kc-storage` | `CONNECT_STATUS_STORAGE_TOPIC` | Trạng thái (RUNNING/FAILED/PAUSED) |

- **Ai tạo?** Connect framework tự tạo khi start lần đầu
- **Lưu ở đâu?** Trên Kafka broker (topic bình thường, `cleanup.policy=compact`)
- **Gắn với gì?** Gắn với Connect cluster, không phải từng connector

## Cách hoạt động khi Crash & Recovery

```
Đang chạy:
  offset 0→49  → commit Iceberg OK → lưu offset=50 vào kc-offsets
  offset 50→79 → commit Iceberg OK → lưu offset=80 vào kc-offsets
  offset 80→89 → đang xử lý... CRASH!

Restart:
  Đọc kc-offsets → committed offset = 80
  → Tiếp tục từ offset 80 (replay 80→89, không đọc lại từ 0)
```

**At-least-once delivery**: một số message có thể bị xử lý lại (80→89), nhưng không mất data.

## `auto.offset.reset`

```json
"consumer.override.auto.offset.reset": "earliest"
```

Chỉ có tác dụng khi **không tìm thấy offset** (connector mới, hoặc đã xóa kc-offsets):

| Value | Hành vi |
|---|---|
| `earliest` | Đọc từ đầu topic (offset 0) |
| `latest` | Chỉ đọc message mới từ lúc start |
| `none` | Throw exception, connector fail |

Nếu đã có offset trong `kc-offsets` → config này bị bỏ qua, tiếp tục từ offset đã lưu.

## Đọc nội dung 3 topics

```powershell
# Xem offset
docker exec kafka kafka-console-consumer --bootstrap-server kafka:9092 \
  --topic kc-offsets --from-beginning --property print.key=true --max-messages 20

# Xem config
docker exec kafka kafka-console-consumer --bootstrap-server kafka:9092 \
  --topic kc-config --from-beginning --property print.key=true --max-messages 20

# Xem status
docker exec kafka kafka-console-consumer --bootstrap-server kafka:9092 \
  --topic kc-storage --from-beginning --property print.key=true --max-messages 20
```

## Dữ liệu bên trong trông thế nào?

**kc-offsets:**
```
Key:   ["sink-qtmt-tramquantrac",{"kafka_topic":"qtmt-tramquantrac","kafka_partition":0}]
Value: {"kafka_offset":9}
```

**kc-config:**
```
Key:   connector-sink-qtmt-tramquantrac
Value: {"connector.class":"io.tabular.iceberg.connect.IcebergSinkConnector",...}
```

**kc-storage:**
```
Key:   status-connector-sink-qtmt-tramquantrac
Value: {"state":"RUNNING","worker_id":"connect:8083"}
```

## Reset offset (force đọc lại từ đầu)

```powershell
# 1. Xóa connector
Invoke-WebRequest -Uri "http://localhost:8083/connectors/sink-qtmt-tramquantrac" -Method DELETE

# 2. Xóa topic offset (hoặc dùng kafka-delete-records)
docker exec kafka kafka-topics --bootstrap-server kafka:9092 --delete --topic kc-offsets

# 3. Restart Connect (để tạo lại topic)
docker compose restart connect

# 4. Deploy connector lại → auto.offset.reset=earliest → đọc từ đầu
```

## So sánh với Kafka consumer thường

| | Consumer thường | Kafka Connect |
|---|---|---|
| Offset lưu ở | `__consumer_offsets` (Kafka tự quản) | `kc-offsets` (Connect tự quản) |
| Commit khi nào | Auto-commit hoặc manual | Chỉ sau khi ghi destination thành công |
| Ai tạo topic | Kafka broker tự tạo | Connect framework tạo |
| Tên topic | Cố định | Bạn config được |
