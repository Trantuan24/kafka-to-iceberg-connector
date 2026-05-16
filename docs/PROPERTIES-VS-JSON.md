# .properties vs .json — 2 loại config trong Kafka Connect

---

## Tóm tắt

| | `.properties` | `.json` (REST API) |
|--|---------------|-------------------|
| **Config cho cái gì** | Kafka Connect **framework** (worker) | **Connector** cụ thể |
| **Dùng khi nào** | Khởi động Kafka Connect process | Deploy/quản lý connector lúc runtime |
| **Cách dùng** | Truyền vào lúc start worker | POST lên REST API `http://localhost:8083/connectors` |
| **Chế độ** | Standalone mode | Distributed mode (đang dùng) |
| **Thay đổi** | Phải restart worker | Không cần restart, deploy lại là xong |

---

## 1. `.properties` — Config cho Worker (framework)

File: `configs/connect-standalone.properties`

```properties
bootstrap.servers=kafka:9092
key.converter=org.apache.kafka.connect.json.JsonConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
value.converter.schemas.enable=false
offset.storage.file.filename=/tmp/connect.offsets
offset.flush.interval.ms=10000
plugin.path=/usr/share/java,/usr/share/confluent-hub-components
```

**Nó config:**
- Kafka Connect process kết nối Kafka broker ở đâu
- Default converter (JSON/Avro/Protobuf)
- Offset lưu ở đâu (file cho standalone, topic cho distributed)
- Plugin path (tìm connector JARs ở đâu)

**Dùng cho:** Standalone mode — chạy bằng lệnh:
```bash
connect-standalone.sh connect-standalone.properties connector.properties
```

---

## 2. `.json` — Config cho Connector (REST API)

File: `configs/sink.tram_quan_trac_cdc_v2.json`

```json
{
  "name": "sink.tram_quan_trac_cdc_v2",
  "config": {
    "connector.class": "io.tabular.iceberg.connect.IcebergSinkConnector",
    "topics": "tram_quan_trac",
    "transforms": "customCdc",
    ...
  }
}
```

**Nó config:**
- Connector nào chạy (class)
- Đọc topic nào
- Transform (SMT) nào
- Ghi vào đâu (Iceberg table, catalog, storage)
- CDC logic (cdc-field, id-columns)

**Dùng cho:** Distributed mode — deploy bằng REST API:
```powershell
$body = Get-Content "configs\sink.tram_quan_trac_cdc_v2.json" -Raw
Invoke-RestMethod -Method Post "http://localhost:8083/connectors" -ContentType "application/json" -Body $body
```

---

## Pipeline hiện tại dùng gì?

**Distributed mode** — không dùng `.properties` trực tiếp.

Worker config được truyền qua **environment variables** trong `docker-compose.yml`:

```yaml
environment:
  CONNECT_BOOTSTRAP_SERVERS: kafka:9092
  CONNECT_GROUP_ID: kc
  CONNECT_CONFIG_STORAGE_TOPIC: kc-config
  CONNECT_OFFSET_STORAGE_TOPIC: kc-offsets
  CONNECT_KEY_CONVERTER: org.apache.kafka.connect.json.JsonConverter
  CONNECT_VALUE_CONVERTER: org.apache.kafka.connect.json.JsonConverter
  CONNECT_PLUGIN_PATH: /usr/share/java,/usr/share/confluent-hub-components
```

→ Tương đương `.properties` nhưng dạng env vars cho Docker.

Connector config thì deploy bằng `.json` qua REST API.

---

## So sánh trực quan

```
STANDALONE MODE:
  connect-standalone.sh  worker.properties  connector.properties
                         ↑                  ↑
                         Worker config      Connector config
                         (.properties)      (.properties)

DISTRIBUTED MODE (đang dùng):
  docker-compose.yml → env vars = Worker config
  REST API POST .json            = Connector config
```

---

## Khi nào dùng cái nào?

| Tình huống | Dùng |
|-----------|------|
| Dev/test nhanh, 1 connector | Standalone + `.properties` |
| Production, nhiều connectors, scale | Distributed + `.json` REST API |
| Thay đổi connector config | `.json` → DELETE + POST lại (không restart) |
| Thay đổi worker config | Restart container / process |
