# Hướng Dẫn Thêm Metadata Mới Vào Iceberg Snapshot

Tài liệu này hướng dẫn chi tiết các bước cần thực hiện khi bạn muốn bổ sung thêm một trường metadata mới vào **Snapshot Summary** của bảng Iceberg thông qua Kafka Connect.

Việc thêm metadata vào snapshot giúp bạn theo dõi lineage (nguồn gốc dữ liệu) hoặc các thông tin tracking khác mà **không làm thay đổi schema của bảng dữ liệu (business schema)**.

---

## Bước 1: Định nghĩa tham số cấu hình (Tùy chọn)
*Nếu metadata của bạn có giá trị tĩnh hoặc được truyền vào từ cấu hình file JSON (ví dụ: `sink.tram_quan_trac_cdc_v2.json`), bạn cần khai báo nó trong file cấu hình.*

**File:** `kafka-connect/src/main/java/io/tabular/iceberg/connect/IcebergSinkConfig.java`

1. **Thêm hằng số cho property mới:**
   ```java
   private static final String PIPELINE_MY_NEW_META_PROP = "pipeline.my-new-meta";
   private static final String PIPELINE_MY_NEW_META_DEFAULT = "default_value";
   ```

2. **Đăng ký vào `ConfigDef` (bên trong method `newConfigDef()`):**
   ```java
   configDef.define(
       PIPELINE_MY_NEW_META_PROP,
       Type.STRING,
       PIPELINE_MY_NEW_META_DEFAULT,
       Importance.LOW,
       "Mô tả cho metadata mới của bạn"
   );
   ```

3. **Thêm hàm accessor để lấy giá trị:**
   ```java
   public String pipelineMyNewMeta() {
       return getString(PIPELINE_MY_NEW_META_PROP);
   }
   ```

---

## Bước 2: Inject metadata vào tiến trình Commit
*Đây là bước quan trọng nhất. Tiến trình commit dữ liệu vào Iceberg được quản lý bởi `Coordinator`.*

**File:** `kafka-connect/src/main/java/io/tabular/iceberg/connect/channel/Coordinator.java`

1. **Khai báo tên của property sẽ hiển thị trên Iceberg Snapshot:**
   ```java
   private static final String PIPELINE_MY_NEW_META_KEY = "pipeline.my-new-meta";
   ```

2. **Tìm đến method `commitToTable()` và khởi tạo giá trị:**
   Bên trong khối lệnh `else` của method `commitToTable()` (ngay trước khi commit), thêm giá trị của bạn:
   ```java
   // Lấy giá trị từ config (hoặc sinh động như UUID, lấy từ tableIdentifier, v.v...)
   String myNewMetaValue = config.pipelineMyNewMeta(); 
   ```

3. **Gắn metadata vào các thao tác Commit (Inject):**
   Iceberg có 2 kiểu commit khi chạy Kafka Connect CDC: `AppendFiles` (chỉ INSERT) và `RowDelta` (có UPDATE/DELETE). **Bạn phải thêm vào cả hai nhánh này.**

   *Nhánh `AppendFiles` (khi `deleteFiles.isEmpty()`):*
   ```java
   AppendFiles appendOp = transaction.newAppend();
   // ... các metadata cũ
   appendOp.set(PIPELINE_MY_NEW_META_KEY, myNewMetaValue); // <--- THÊM VÀO ĐÂY
   appendOp.commit();
   ```

   *Nhánh `RowDelta` (khi có `deleteFiles`):*
   ```java
   RowDelta deltaOp = table.newRowDelta();
   // ... các metadata cũ
   deltaOp.set(PIPELINE_MY_NEW_META_KEY, myNewMetaValue); // <--- THÊM VÀO ĐÂY
   deltaOp.commit();
   ```

---

## Bước 3: Build lại Kafka Connect JAR

Sau khi sửa code Java, bạn phải build lại file JAR của Fork Connector.

**Mở Terminal / Git Bash:**
```bash
cd iceberg-kafka-connect-fork
./gradlew :iceberg-kafka-connect:jar -x test --no-daemon
```

File JAR mới sẽ được tạo ra tại: `kafka-connect/build/libs/iceberg-kafka-connect.jar`.

---

## Bước 4: Deploy và Rebuild Docker Container

**Copy file JAR mới vào thư mục plugins của Docker:**
```powershell
Copy-Item -Force "iceberg-kafka-connect-fork\kafka-connect\build\libs\iceberg-kafka-connect.jar" "plugins\iceberg-kafka-connect\lib\iceberg-kafka-connect-custom-pipeline-meta.jar"
```

**Rebuild image và Recreate container:**
*Do Kafka Connect Docker Image copy plugins vào bên trong Image, bạn bắt buộc phải build lại image khi có JAR mới.*
```bash
docker compose build connect
docker compose up -d connect --force-recreate
```

---

## Bước 5: Kiểm tra kết quả

Sau khi connector chạy và commit dữ liệu, bạn có thể vào Trino (hoặc Spark) để query bảng `$snapshots` xem metadata mới đã được inject thành công chưa:

```sql
SELECT 
    snapshot_id, 
    operation, 
    summary['pipeline.my-new-meta'] AS my_new_meta
FROM iceberg.default."tên_bảng$snapshots";
```
