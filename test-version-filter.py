"""
Test out-of-order version filter trong SMT.
Kich ban: gui v5 truoc, v4 sau -> v4 phai bi DROP boi SMT.
"""
import json
import time
from kafka import KafkaProducer

KAFKA_BROKER = 'localhost:29092'
TOPIC = 'tram_quan_trac'

def send(producer, label, msg):
    print(f"  -> Gui: {label} (key={msg['data'][0]['MaTram']}, v={msg['version']}, type={msg['type']})")
    producer.send(TOPIC, value=msg)
    producer.flush()

def main():
    print("=" * 60)
    print("Test: Out-of-order version filter (Version Cache SMT)")
    print("=" * 60)

    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    # Xoa table cu truoc khi test
    print("\n[Prep] Doi 3s de connector san sang...")
    time.sleep(3)

    # -----------------------------------------------------------
    # TRAM010: INSERT v1 -> UPDATE v5 -> UPDATE v3 (stale, phai DROP)
    # -----------------------------------------------------------
    print("\n[TEST 1] TRAM010: INSERT v1")
    send(producer, "INSERT v1", {
        "data": [{"MaTram": "TRAM010", "TenTram": "Tram 10", "Status": "NEW"}],
        "length": 1, "key": "MaTram", "type": "INSERT",
        "version": 1, "ngay_cap_nhat": "2026-05-06T08:00:00Z"
    })
    time.sleep(19)  # cho commit batch 1

    print("\n[TEST 2] TRAM010: UPDATE v5 (version nhat)")
    send(producer, "UPDATE v5", {
        "data": [{"MaTram": "TRAM010", "TenTram": "Tram 10 - v5 LATEST", "Status": "ACTIVE"}],
        "length": 1, "key": "MaTram", "type": "UPDATE",
        "version": 5, "ngay_cap_nhat": "2026-05-06T08:20:00Z"
    })
    time.sleep(19)  # cho commit batch 2

    print("\n[TEST 3] TRAM010: UPDATE v3 (stale - PHAI BI DROP boi SMT)")
    send(producer, "UPDATE v3 [STALE - should DROP]", {
        "data": [{"MaTram": "TRAM010", "TenTram": "Tram 10 - v3 OLD", "Status": "STALE"}],
        "length": 1, "key": "MaTram", "type": "UPDATE",
        "version": 3, "ngay_cap_nhat": "2026-05-06T08:10:00Z"
    })
    time.sleep(19)  # cho commit batch 3

    print("\n[TEST 4] TRAM010: UPDATE v6 (moi nhat - PHAI PASS)")
    send(producer, "UPDATE v6 [should PASS]", {
        "data": [{"MaTram": "TRAM010", "TenTram": "Tram 10 - v6 NEWEST", "Status": "FINAL"}],
        "length": 1, "key": "MaTram", "type": "UPDATE",
        "version": 6, "ngay_cap_nhat": "2026-05-06T08:30:00Z"
    })
    time.sleep(19)  # cho commit batch 4

    print("\n" + "=" * 60)
    print("Tat ca messages da gui!")
    print("")
    print("Ket qua MONG DOI (chi co 1 row cho TRAM010):")
    print("  TRAM010 | UPDATE | U | v6 | Tram 10 - v6 NEWEST")
    print("")
    print("Neu thay v3 hoac nhieu hon 1 row -> version filter CHUA hoat dong")
    print("")
    print("Kiem tra logs SMT:")
    print("  docker logs iceberg-kafka-connect-demo-connect-1 2>&1 | grep VERSION-FILTER")
    print("")
    print("Query Trino:")
    print("  docker exec iceberg-kafka-connect-demo-trino-1 trino \\")
    print("    --execute \"SELECT dedup_key,type,_cdc_op,version FROM iceberg.default.tram_quan_trac_cdc_v2 WHERE dedup_key='TRAM010'\"")
    print("=" * 60)

if __name__ == "__main__":
    main()
