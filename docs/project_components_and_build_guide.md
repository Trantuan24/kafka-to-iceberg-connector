# Toàn Cảnh Các Thành Phần Quan Trọng Trong Kiến Trúc Build & Deploy

Tài liệu này không chỉ giải quyết các lỗi từng gặp, mà cung cấp cái nhìn **rộng và đầy đủ nhất** về toàn bộ các file, thư viện, và cấu trúc cần thiết để xây dựng và vận hành thành công kiến trúc CDC Pipeline (Debezium -> Kafka -> Custom SMT -> Iceberg).

---

## 1. Môi Trường Build (Build Environment)

Để compile được bộ source code của Forked Iceberg Connector, môi trường build phải đảm bảo các tệp tin sau:

* **`gradle/wrapper/gradle-wrapper.jar` và `gradlew` (Gradle Wrapper):**
  * **Vai trò:** Giúp hệ thống tự động tải và sử dụng đúng phiên bản Gradle cần thiết cho dự án. Khác với Maven, bạn không cần phải cài sẵn Gradle trên máy.
  * **Hậu quả nếu thiếu:** Môi trường build sẽ báo lỗi không tìm thấy Gradle hoặc gây ra xung đột nếu phiên bản Gradle cài trên máy không tương thích với source code.
* **`gradle/libs.versions.toml`:**
  * **Vai trò:** Khai báo toàn bộ versions cho các thư viện phụ thuộc (dependencies) của tất cả các module. Đây là chuẩn mới của Gradle (Version Catalog).
  * **Hậu quả nếu thiếu:** Gradle không biết tải các thư viện Iceberg, Kafka, Jackson... ở version nào, dẫn đến lỗi build thất bại (như lỗi không tìm thấy package `events` trước đây).
* **`settings.gradle` và `build.gradle` (ở Root và các module con):**
  * **Vai trò:** Định nghĩa tên dự án và khai báo cấu trúc đa mô-đun (multi-module) bao gồm `kafka-connect`, `kafka-connect-events`, `kafka-connect-transforms`. Định nghĩa các task như `:iceberg-kafka-connect:jar`.

---

## 2. Các Module Cốt Lõi (Core Source Modules)

Dự án Fork gồm các mảnh ghép độc lập nhưng phối hợp chặt chẽ:

* **Module `kafka-connect` (Tạo ra `iceberg-kafka-connect.jar`):**
  * **Vai trò:** Module trái tim chứa logic Sink Connector, bao gồm `Coordinator` (quản lý commit/lineage metadata), `IcebergWriter` (tách/strip internal fields), và `IcebergWriterFactory` (auto-create tables).
* **Module `kafka-connect-events`:**
  * **Vai trò:** Định nghĩa các class Java dùng để serialize/deserialize các Event giao tiếp nội bộ qua Control Topic (dưới dạng Avro).
  * **Lưu ý quan trọng:** Class `EventDecoder.java` được tinh giản chỉ dùng cấu trúc sự kiện 1.5.x+ (AvroUtil), loại bỏ các dependencies cũ (1.4.x) để build thành công một file JAR hoàn chỉnh mà không bị phụ thuộc rác.
* **Module `kafka-connect-transforms`:**
  * **Vai trò:** Chứa các SMT (Single Message Transforms) mặc định do repo Databricks/Tabular cung cấp (ví dụ: Extract/Route mặc định). Tuy nhiên, trong kiến trúc của ta, ta thường sử dụng Custom SMT riêng.

---

## 3. Các Thành Phần Runtime (Deployment & Plugins)

Khi mang code lên Docker chạy Kafka Connect, các file JAR sau là linh hồn của hệ thống:

* **`iceberg-kafka-connect-custom-pipeline-meta.jar` (Đổi tên từ `iceberg-kafka-connect.jar`):**
  * **Vai trò:** Chứa toàn bộ logic xử lý luồng, inject lineage uuid, và filter fields. Đây là JAR ta copy đè sau mỗi lần compile.
* **`custom-smt.jar` (Custom Single Message Transform):**
  * **Vai trò:** Bộ SMT tùy chỉnh do ta tự viết. Nó can thiệp vào message ngay khi rời khỏi Kafka và **trước khi** vào Iceberg Connector.
  * **Nhiệm vụ:**
    1. Lọc version cũ (version filter based on dedup_key).
    2. Gom schema cồng kềnh của Debezium CDC thành 10 fields nghiệp vụ.
    3. Trích xuất metadata tạo thành field `iceberg_table` (để Connector route đúng bảng) và `_cdc_op` (để Connector biết U/I/D).
* **Các thư viện SerDes & Kafka base (Có sẵn trong base image):**
  * Gồm Avro Converter, Schema Registry Client, v.v., cần thiết để giải mã dữ liệu Kafka.

---

## 4. Nguyên Tắc Cấu Hình Và Docker

Để mọi thứ khớp với nhau trong môi trường Production:

* **Quản lý file `.jar` trong thư mục `plugins/`:**
  * Kafka Connect sử dụng **PluginClassLoader** quét toàn bộ thư mục `/usr/share/java/`.
  * **Nguyên tắc sống còn:** Nếu bạn để hai phiên bản của cùng một JAR (ví dụ `v1.jar` và `v2.jar`), Connect sẽ ném lỗi ClassLoading. Bắt buộc phải xóa file cũ hoặc đổi đuôi thành `.bak` (vì Connect bỏ qua các file không phải `.jar`).
* **Dockerfile.connect:**
  * Lệnh `COPY plugins/...` là chìa khóa. Nó "đóng băng" code vào image.
  * Việc này an toàn cho môi trường phân tán nhưng đồng nghĩa **bắt buộc phải chạy `docker compose build connect`** để mang file JAR mới từ máy Host vào Image mỗi lần có thay đổi code Java.
* **File JSON cấu hình Sink (ví dụ `sink.tram_quan_trac.json`):**
  * Đây là nơi nối "dây điện":
    * Khai báo dùng connector nào: `"connector.class": "io.tabular.iceberg.connect.IcebergSinkConnector"`
    * Khai báo dùng SMT nào: `"transforms": "CustomCDC"`
    * Mapping routing fields: `"iceberg.tables.route-field": "iceberg_table"`
    * Truyền custom meta: `"pipeline.source-type": "API"`
