# Topic → Table Mapping

## Cách hoạt động

SMT đọc topic name → derive tên Iceberg table → ghi vào field `iceberg_table` → connector route record vào table đó.

---

## 2 cách map:

### 1. Custom map (ưu tiên cao)

Chỉ định rõ topic nào → table nào:

```json
"transforms.customCdc.topic.table.map": "qtmt-quantrackhithai:def.abc"
```

Format: `topic:namespace.table` — nhiều cặp cách nhau bằng dấu `,`

Ví dụ nhiều topics:
```json
"transforms.customCdc.topic.table.map": "qtmt-quantrackhithai:def.abc,qtmt-nuocmat:prod.water_monitoring"
```

### 2. Auto-derive (fallback)

Topic không có trong map → tự derive:
```
namespace + "." + topic.replace("-", "_")
```

Config namespace:
```json
"transforms.customCdc.iceberg.namespace": "default"
```

Ví dụ: topic `qtmt-tramquantrac` → `default.qtmt_tramquantrac`

---

## Ví dụ thực tế (config hiện tại):

```json
"topics": "qtmt-tramquantrac,qtmt-quantrackhithai",
"transforms.customCdc.iceberg.namespace": "default",
"transforms.customCdc.topic.table.map": "qtmt-quantrackhithai:def.abc"
```

Kết quả:
| Topic | Có trong map? | Table đích |
|-------|--------------|------------|
| `qtmt-tramquantrac` | Không | `default.qtmt_tramquantrac` (auto-derive) |
| `qtmt-quantrackhithai` | Có | `def.abc` (custom) |

---

## Lưu ý

- Namespace phải tồn tại trước: `CREATE SCHEMA iceberg.def`
- Table tự tạo nếu `auto-create-enabled=true`
- Không cần khai báo tất cả topics trong map — chỉ khai báo topic muốn custom
