"""
Test Option A: Pure CDC mode (cdc-field only, upsert-mode=false)

Muc tieu kiem tra:
  - INSERT  -> ghi 1 row moi
  - UPDATE  -> xoa row cu cung dedup_key + ghi row moi
  - DELETE  -> xoa row, KHONG ghi row moi
  - Moi operation cach nhau > 15s de vao batch commit rieng
"""
import json
import time
from kafka import KafkaProducer

KAFKA_BROKER = 'localhost:29092'
TOPIC = 'tram_quan_trac'
COMMIT_WAIT = 18  # > commit.interval-ms=10s de chac chan tach batch

def send(producer, label, msg):
    print(f"  -> Gui: {label}")
    producer.send(TOPIC, value=msg)
    producer.flush()

def wait_commit(label=""):
    print(f"  [Doi {COMMIT_WAIT}s cho commit batch{' - ' + label if label else ''}...]")
    time.sleep(COMMIT_WAIT)

def main():
    print("=" * 60)
    print("Test Option A: Pure CDC mode")
    print("=" * 60)

    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    # -----------------------------------------------------------
    # TEST 1: INSERT TRAM001 v1
    # -----------------------------------------------------------
    print("\n[BATCH 1] INSERT TRAM001 v1")
    send(producer, "INSERT TRAM001 v1", {
        "data": [{"MaTram": "TRAM001", "TenTram": "Tram 1", "Status": "NEW"}],
        "length": 1, "key": "MaTram", "type": "INSERT",
        "version": 1, "ngay_cap_nhat": "2026-05-06T08:00:00Z"
    })
    wait_commit("sau INSERT TRAM001")

    # -----------------------------------------------------------
    # TEST 2: UPDATE TRAM001 v2  ->  ket qua: chi con 1 row v2
    # -----------------------------------------------------------
    print("\n[BATCH 2] UPDATE TRAM001 v2 (phai xoa v1)")
    send(producer, "UPDATE TRAM001 v2", {
        "data": [{"MaTram": "TRAM001", "TenTram": "Tram 1 - UPDATED", "Status": "ACTIVE"}],
        "length": 1, "key": "MaTram", "type": "UPDATE",
        "version": 2, "ngay_cap_nhat": "2026-05-06T08:05:00Z"
    })
    wait_commit("sau UPDATE TRAM001")

    # -----------------------------------------------------------
    # TEST 3: INSERT TRAM002 v1  ->  sau do DELETE TRAM002
    # Hai batch rieng de tranh same-batch conflict
    # -----------------------------------------------------------
    print("\n[BATCH 3] INSERT TRAM002 v1")
    send(producer, "INSERT TRAM002 v1", {
        "data": [{"MaTram": "TRAM002", "TenTram": "Tram 2", "Status": "NEW"}],
        "length": 1, "key": "MaTram", "type": "INSERT",
        "version": 1, "ngay_cap_nhat": "2026-05-06T08:10:00Z"
    })
    wait_commit("sau INSERT TRAM002")

    print("\n[BATCH 4] DELETE TRAM002 (phai xoa hoan toan)")
    send(producer, "DELETE TRAM002", {
        "data": [{"MaTram": "TRAM002", "TenTram": "Tram 2", "Status": "DELETED"}],
        "length": 1, "key": "MaTram", "type": "DELETE",
        "version": 2, "ngay_cap_nhat": "2026-05-06T08:15:00Z"
    })
    wait_commit("sau DELETE TRAM002")

    # -----------------------------------------------------------
    # TEST 4: TRAM003 INSERT -> UPDATE v2  ->  chi con 1 row
    # -----------------------------------------------------------
    print("\n[BATCH 5] INSERT TRAM003 v1")
    send(producer, "INSERT TRAM003 v1", {
        "data": [{"MaTram": "TRAM003", "TenTram": "Tram 3", "Status": "NEW"}],
        "length": 1, "key": "MaTram", "type": "INSERT",
        "version": 1, "ngay_cap_nhat": "2026-05-06T08:20:00Z"
    })
    wait_commit("sau INSERT TRAM003")

    print("\n[BATCH 6] UPDATE TRAM003 v2 (phai xoa v1)")
    send(producer, "UPDATE TRAM003 v2", {
        "data": [{"MaTram": "TRAM003", "TenTram": "Tram 3 - FINAL", "Status": "ACTIVE"}],
        "length": 1, "key": "MaTram", "type": "UPDATE",
        "version": 2, "ngay_cap_nhat": "2026-05-06T08:25:00Z"
    })
    wait_commit("sau UPDATE TRAM003")

    print("\n" + "=" * 60)
    print("Tat ca messages da gui xong!")
    print("Ket qua mong doi:")
    print("  TRAM001 | UPDATE | U | v2   (INSERT v1 da bi xoa)")
    print("  TRAM002                     (da bi DELETE hoan toan - khong con row nao)")
    print("  TRAM003 | UPDATE | U | v2   (INSERT v1 da bi xoa)")
    print("")
    print("Query kiem tra:")
    print("  docker exec iceberg-kafka-connect-demo-trino-1 trino \\")
    print("    --execute \"SELECT dedup_key, type, _cdc_op, version FROM iceberg.default.tram_quan_trac_cdc_v2 ORDER BY dedup_key\"")
    print("=" * 60)

if __name__ == "__main__":
    main()
