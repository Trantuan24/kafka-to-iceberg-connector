import json
import time
from kafka import KafkaProducer

KAFKA_BROKER = 'localhost:29092'
TOPIC = 'tram_quan_trac'

def create_producer():
    return KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def main():
    print("============================================================")
    print("Phase 2 Test: Upsert CDC Mode")
    print("============================================================")
    
    try:
        producer = create_producer()
    except Exception as e:
        print(f"Error connecting to Kafka: {e}")
        return

    # TEST 1: INSERT
    msg_insert = {
        "data": [{"MaTram": "TRAM001", "TenTram": "Station 1", "Status": "NEW"}],
        "length": 1,
        "key": "MaTram",
        "type": "INSERT",
        "version": 1,
        "ngay_cap_nhat": "2026-05-05T10:00:00Z"
    }
    print("\n[TEST 1] Sending INSERT (TRAM001, v1)")
    producer.send(TOPIC, value=msg_insert)
    producer.flush()
    time.sleep(2)
    
    # TEST 2: UPDATE
    msg_update = {
        "data": [{"MaTram": "TRAM001", "TenTram": "Station 1 - UPDATED", "Status": "ACTIVE"}],
        "length": 1,
        "key": "MaTram",
        "type": "UPDATE",
        "version": 2,
        "ngay_cap_nhat": "2026-05-05T10:05:00Z"
    }
    print("[TEST 2] Sending UPDATE (TRAM001, v2)")
    producer.send(TOPIC, value=msg_update)
    producer.flush()
    time.sleep(2)
    
    # TEST 3: DELETE
    msg_delete = {
        "data": [{"MaTram": "TRAM002", "TenTram": "Station 2", "Status": "DELETED"}],
        "length": 1,
        "key": "MaTram",
        "type": "DELETE",
        "version": 3,
        "ngay_cap_nhat": "2026-05-05T10:10:00Z"
    }
    print("[TEST 3] Sending DELETE (TRAM002, v3)")
    # First insert TRAM002 so it exists
    msg_insert_2 = dict(msg_delete)
    msg_insert_2["type"] = "INSERT"
    producer.send(TOPIC, value=msg_insert_2)
    producer.flush()
    time.sleep(1)
    # Then delete it
    producer.send(TOPIC, value=msg_delete)
    producer.flush()
    time.sleep(2)
    
    # TEST 4: OUT-OF-ORDER VERSION
    msg_update_v5 = {
        "data": [{"MaTram": "TRAM003", "TenTram": "Station 3 - v5", "Status": "LATEST"}],
        "length": 1,
        "key": "MaTram",
        "type": "UPDATE",
        "version": 5,
        "ngay_cap_nhat": "2026-05-05T10:20:00Z"
    }
    msg_update_v4 = {
        "data": [{"MaTram": "TRAM003", "TenTram": "Station 3 - v4", "Status": "STALE"}],
        "length": 1,
        "key": "MaTram",
        "type": "UPDATE",
        "version": 4,
        "ngay_cap_nhat": "2026-05-05T10:15:00Z"
    }
    
    print("[TEST 4] Sending OUT-OF-ORDER (TRAM003: v5 first, then v4)")
    # Insert initially
    msg_insert_3 = dict(msg_update_v5)
    msg_insert_3["type"] = "INSERT"
    msg_insert_3["version"] = 1
    producer.send(TOPIC, value=msg_insert_3)
    producer.flush()
    time.sleep(1)
    
    # Send v5 (Latest)
    producer.send(TOPIC, value=msg_update_v5)
    producer.flush()
    time.sleep(1)
    
    # Send v4 (Stale)
    producer.send(TOPIC, value=msg_update_v4)
    producer.flush()
    
    print("\nAll messages sent!")
    print("Wait ~15s for Iceberg commit, then query Trino:")
    print("docker exec iceberg-kafka-connect-demo-trino-1 trino --execute \"SELECT * FROM iceberg.default.tram_quan_trac_cdc_v2\"")

if __name__ == "__main__":
    main()
