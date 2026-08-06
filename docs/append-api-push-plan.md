# Ke hoach nang cap APPEND sink cho API push

## 1. Pham vi

- Chi thay doi luong `mode=append` cua custom SMT, cau hinh/test lien quan va runtime JAR.
- Khong thay doi hanh vi CDC.
- Bang Iceberg phai duoc tao truoc; SMT chi tao record va route den bang da cau hinh.
- Khong goi API that va khong luu thong tin xac thuc cua API.

## 2. Contract dau vao Kafka

Moi Kafka record tu API push co contract sau:

- Kafka value: toan bo du lieu tho can luu, dung `StringConverter`; JSON, XML va chuoi rong deu duoc giu nguyen.
- Kafka header `api.body` (khong bat buoc): tham so/body nghiep vu cua request API duoi dang JSON string UTF-8. Thieu hoac chuoi rong thi cot `body` la `null`. Connector dung `header.converter=StringConverter` de giu nguyen chuoi ngay/so thay vi tu suy dien kieu.
- Kafka header `api.headers` (khong bat buoc): cac HTTP header duoi dang JSON object UTF-8. SMT chi giu allowlist an toan va loai bo header xac thuc/nhay cam.
- Header Kafka co the mang gia tri `String`, `byte[]` UTF-8, hoac `Map`/`List` do Kafka Connect header converter parse tu JSON.

Voi request mau `POST`, Basic Auth, `Content-Type: application/json`, `--data ''`:

- `body = null` neu khong co tham so nghiep vu.
- `header = {"content-type":"application/json"}` neu ingress day header an toan vao Kafka.
- Tuyet doi khong ghi `Authorization`, access key, secret key, cookie hoac API key.

## 3. Contract dau ra APPEND

| Cot | Kieu Iceberg | Quy tac |
| --- | --- | --- |
| `loainguon` | `STRING` | Cau hinh `append.source.type`, mac dinh `api_push` |
| `manguondulieu` | `STRING` | `topic-partition-offset`, on dinh khi replay |
| `sukien` | `STRING` | `null`; append khong suy dien su kien CDC |
| `phienban` | `INT` | Cau hinh `append.schema.version`, mac dinh `1`; la version cua contract landing, khong phai version du lieu |
| `body` | `STRING` | Kafka header `api.body`; thieu/rong thi `null` |
| `header` | `STRING` | JSON tu Kafka header `api.headers` sau khi loc allowlist; thieu/khong hop le/rong thi `null` |
| `data` | `STRING` | Toan bo Kafka value, giu nguyen ky tu |
| `ingest_date` | `DATE` | Ngay local `yyyy-MM-dd` tai luc SMT xu ly |
| `ingest_time` | `TIMESTAMP(3)` | Gio local `yyyy-MM-dd'T'HH:mm:ss.SSS` tai luc SMT xu ly |

Trino/Iceberg co the hien thi cot nay la `timestamp(6)`; SMT sinh millisecond nen ba chu so microsecond cuoi la `000`.

`ingest_date` va `ingest_time` phai sinh tu cung mot lan doc dong ho. Mui gio cau hinh bang `append.timezone`, mac dinh `Asia/Ho_Chi_Minh`.

Ngoai 9 cot tren, SMT them `iceberg_table` de connector route; connector se loai field nay truoc khi ghi Iceberg.

## 4. Bao mat header

Allowlist co dinh hien tai:

- `content-type`
- `accept`
- `user-agent`
- `x-request-id`
- `x-correlation-id`
- `traceparent`

Ten header duoc chuan hoa thanh chu thuong. Moi header ngoai allowlist, dac biet `authorization`, `cookie`, `set-cookie`, `x-api-key`, `access-key`, `secret-key`, deu khong duoc ghi vao bang. JSON header khong hop le khong lam mat record; SMT log canh bao va gan `header = null`.

## 5. Bang dich va rollout

Tao bang moi de tranh pha vo bang 3 cot cu:

```sql
CREATE TABLE iceberg.def.abc_append_v2 (
    loainguon VARCHAR,
    manguondulieu VARCHAR,
    sukien VARCHAR,
    phienban INTEGER,
    body VARCHAR,
    header VARCHAR,
    data VARCHAR,
    ingest_date DATE,
    ingest_time TIMESTAMP(3)
)
WITH (format = 'PARQUET');
```

Connector map `qtmt-append:def.abc_append_v2`, tat auto-create va schema evolution. Thu tu rollout:

1. Tao bang `def.abc_append_v2` bang DDL tren.
2. Deploy runtime JAR moi va restart Kafka Connect.
3. Cap nhat connector config sang bang v2.
4. Gui record co/khong co metadata header; kiem tra 9 cot va snapshot.
5. Sau khi doi chieu dat, moi quyet dinh giu hay ngung bang 3 cot cu.

## 6. Tieu chi kiem thu

- JSON/XML/chuoi rong duoc giu nguyen trong `data`.
- ID dung `topic-partition-offset` va khong doi khi replay.
- `phienban = 1`, `sukien = null`, `loainguon = api_push` theo cau hinh mac dinh.
- `api.body`/`api.headers` ho tro `String`, `byte[]` va `Map`/`List`; metadata thieu khong lam fail record.
- Header nhay cam khong xuat hien trong output.
- Hai cot ingest cung mot thoi diem, dung mui gio va do chinh xac millisecond.
- Tombstone van pass-through de giu hanh vi tracking offset hien tai.
- CDC tests/build khong bi anh huong.
