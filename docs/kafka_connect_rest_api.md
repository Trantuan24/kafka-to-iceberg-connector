# 📡 Kafka Connect REST API

> **Base URL:** `http://localhost:8083`
> 
> Đây là bộ API **do Kafka Connect Worker tự cung cấp** khi khởi động — không phải do người dùng viết. Mọi thao tác quản lý connector đều thực hiện qua đây.

---

## 1. Worker Info

| Method | Endpoint | Mô tả |
|--------|----------|-------|
| `GET` | `/` | Thông tin Worker: version Kafka Connect, Kafka Cluster ID |

```bash
curl http://localhost:8083/
# {"version":"7.6.0-ce","commit":"...","kafka_cluster_id":"..."}
```

---

## 2. Connector — CRUD

| Method | Endpoint | Mô tả |
|--------|----------|-------|
| `GET` | `/connectors` | Liệt kê tên tất cả connectors đang có |
| `GET` | `/connectors?expand=status` | Liệt kê + trạng thái RUNNING/FAILED của từng connector |
| `POST` | `/connectors` | **Tạo mới** connector (body = JSON config) |
| `GET` | `/connectors/{name}` | Xem thông tin connector (name + config) |
| `GET` | `/connectors/{name}/config` | Xem **chỉ phần config** của connector |
| `PUT` | `/connectors/{name}/config` | **Cập nhật config** (connector tự restart áp dụng ngay) |
| `GET` | `/connectors/{name}/status` | Trạng thái hiện tại: `RUNNING` / `FAILED` / `PAUSED` |
| `POST` | `/connectors/{name}/restart` | **Restart** toàn bộ connector |
| `PUT` | `/connectors/{name}/pause` | Tạm dừng (không xóa connector) |
| `PUT` | `/connectors/{name}/resume` | Chạy lại sau khi pause |
| `DELETE` | `/connectors/{name}` | **Xóa** connector |

### Ví dụ thực tế

```powershell
# [CREATE] Tạo mới connector từ file JSON
$body = Get-Content "configs\sink.qtmt_tramquantrac.json" -Raw
Invoke-RestMethod -Method Post "http://localhost:8083/connectors" `
    -ContentType "application/json" -Body $body

# [READ] Xem config đang chạy
Invoke-RestMethod "http://localhost:8083/connectors/sink-qtmt-tramquantrac/config"

# [READ] Xem trạng thái connector + task
Invoke-RestMethod "http://localhost:8083/connectors/sink-qtmt-tramquantrac/status" `
    | ConvertTo-Json -Depth 3

# [UPDATE] Cập nhật 1 tham số (ví dụ tăng commit interval)
$newConfig = @{
    "connector.class" = "io.tabular.iceberg.connect.IcebergSinkConnector"
    "topics" = "qtmt-tramquantrac"
    "iceberg.control.commit.interval-ms" = "30000"
    # ... các trường khác giữ nguyên
} | ConvertTo-Json
Invoke-RestMethod -Method Put `
    "http://localhost:8083/connectors/sink-qtmt-tramquantrac/config" `
    -ContentType "application/json" -Body $newConfig

# [DELETE] Xóa connector
Invoke-RestMethod -Method Delete `
    "http://localhost:8083/connectors/sink-qtmt-tramquantrac"

# [PAUSE / RESUME]
Invoke-RestMethod -Method Put "http://localhost:8083/connectors/sink-qtmt-tramquantrac/pause"
Invoke-RestMethod -Method Put "http://localhost:8083/connectors/sink-qtmt-tramquantrac/resume"

# [RESTART]
Invoke-RestMethod -Method Post "http://localhost:8083/connectors/sink-qtmt-tramquantrac/restart"
```

---

## 3. Tasks — Quản lý Task

Mỗi connector có thể chạy nhiều task (xem `tasks.max`). Task là đơn vị xử lý thực sự.

| Method | Endpoint | Mô tả |
|--------|----------|-------|
| `GET` | `/connectors/{name}/tasks` | Liệt kê tất cả tasks và config của chúng |
| `GET` | `/connectors/{name}/tasks/{taskId}/status` | Trạng thái của 1 task cụ thể |
| `POST` | `/connectors/{name}/tasks/{taskId}/restart` | Restart 1 task bị FAILED (không restart cả connector) |

```powershell
# Xem tasks
Invoke-RestMethod "http://localhost:8083/connectors/sink-qtmt-tramquantrac/tasks"

# Restart riêng task 0 bị lỗi
Invoke-RestMethod -Method Post `
    "http://localhost:8083/connectors/sink-qtmt-tramquantrac/tasks/0/restart"
```

---

## 4. Plugins — Xem Plugin đã cài

| Method | Endpoint | Mô tả |
|--------|----------|-------|
| `GET` | `/connector-plugins` | Liệt kê toàn bộ connector class đã được load vào Worker |
| `PUT` | `/connector-plugins/{classname}/config/validate` | Validate JSON config trước khi tạo connector |

```powershell
# Xem danh sách plugin đã load (bao gồm custom Fork Connector)
Invoke-RestMethod "http://localhost:8083/connector-plugins"

# Validate config trước khi POST (không tạo connector, chỉ kiểm tra lỗi)
$body = Get-Content "configs\sink.qtmt_tramquantrac.json" -Raw
Invoke-RestMethod -Method Put `
    "http://localhost:8083/connector-plugins/io.tabular.iceberg.connect.IcebergSinkConnector/config/validate" `
    -ContentType "application/json" -Body $body
```

---

## 5. Tóm Tắt Lifecycle Connector qua API

```
POST /connectors          → Tạo (RUNNING)
    │
    ├── PUT  /pause        → Tạm dừng (PAUSED)
    │        │
    │        └── PUT /resume → Chạy lại (RUNNING)
    │
    ├── POST /restart      → Restart (RUNNING)
    │
    ├── PUT  /config       → Cập nhật config → tự restart (RUNNING)
    │
    └── DELETE /connectors → Xóa vĩnh viễn
```

---

## 6. HTTP Response Codes

| Code | Ý nghĩa |
|------|---------|
| `200 OK` | Thao tác thành công (GET, PUT, DELETE) |
| `201 Created` | Tạo connector thành công (POST) |
| `204 No Content` | Pause/Resume thành công |
| `409 Conflict` | Connector tên đó đã tồn tại (POST trùng tên) |
| `404 Not Found` | Connector không tồn tại |
| `422 Unprocessable Entity` | Config sai / thiếu trường bắt buộc |
