"""
Test APPEND mode across multiple commit cycles.

Sends 3 batches x 3 raw JSON messages. Each record also carries optional API
metadata via Kafka headers so body/header mapping is exercised end to end.
"""
from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers=["localhost:29092"],
    value_serializer=lambda value: value.encode("utf-8"),
)

TOPIC = "qtmt-append"
BATCHES = 3
PER_BATCH = 3
SLEEP_BETWEEN = 13


def make_payload(batch, index):
    number = batch * 10 + index
    return json.dumps(
        [
            {
                "MaTram": f"TRAM{number:03d}",
                "TenTram": f"Tram {number}",
                "LoaiHinhQuanTrac": "KHONGKHI",
                "TenTinh": "Ha Noi",
                "ThongSo": "SO2, NO2",
                "batch": batch,
            }
        ],
        ensure_ascii=False,
    )


def metadata_headers(batch, index):
    body = json.dumps(
        {"batch": batch, "requestIndex": index}, ensure_ascii=False
    ).encode("utf-8")
    headers = json.dumps(
        {
            "Content-Type": "application/json",
            "X-Correlation-ID": f"batch-{batch}-item-{index}",
            "Authorization": "Basic must-not-land",
        }
    ).encode("utf-8")
    return [("api.body", body), ("api.headers", headers)]


total = 0
for batch in range(1, BATCHES + 1):
    print(f"=== BATCH {batch}/{BATCHES} ===")
    for index in range(1, PER_BATCH + 1):
        payload = make_payload(batch, index)
        producer.send(
            TOPIC,
            value=payload,
            headers=metadata_headers(batch, index),
        )
        total += 1
        print(f"  sent msg {index} ({len(payload)} chars)")
    producer.flush()
    if batch < BATCHES:
        print(f"  waiting {SLEEP_BETWEEN}s for a separate commit...\n")
        time.sleep(SLEEP_BETWEEN)

print("waiting 15s for final commit...")
time.sleep(15)
print(f"DONE - sent {total} messages across {BATCHES} batches")
producer.close()