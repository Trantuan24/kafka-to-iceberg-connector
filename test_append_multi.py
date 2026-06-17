"""
Test APPEND mode - NHIEU MESSAGE qua NHIEU CHU KY COMMIT (checkpoint)
=====================================================================
- 1 topic = 1 format (JSON), value.converter=StringConverter
- commit.interval-ms = 10000 (10s) => gui cac batch cach nhau >10s
  de ep connector commit NHIEU LAN => sinh NHIEU SNAPSHOT.

Thiet ke: 3 BATCH x 3 message = 9 message, nghi 13s giua cac batch.
Ky vong: >= 3 snapshot (moi batch it nhat 1 commit) + 9 rows.
"""
from kafka import KafkaProducer
import time
import json

producer = KafkaProducer(
    bootstrap_servers=['localhost:29092'],
    value_serializer=lambda v: v.encode('utf-8')   # raw string (StringConverter)
)

TOPIC = "qtmt-append"
BATCHES = 3
PER_BATCH = 3
SLEEP_BETWEEN = 13   # > commit.interval (10s) => moi batch 1 commit rieng

def make_payload(batch, idx):
    # Moi message la 1 JSON array tho (giong sample_new.json)
    n = batch * 10 + idx
    return json.dumps([
        {"MaTram": f"TRAM{n:03d}", "TenTram": f"Tram {n}", "LoaiHinhQuanTrac": "KHONGKHI",
         "TenTinh": "Ha Noi", "ThongSo": "SO2, NO2", "batch": batch}
    ], ensure_ascii=False)

total = 0
for b in range(1, BATCHES + 1):
    print(f"=== BATCH {b}/{BATCHES} ===")
    for i in range(1, PER_BATCH + 1):
        payload = make_payload(b, i)
        producer.send(TOPIC, value=payload)
        total += 1
        print(f"  sent msg {i} ({len(payload)} chars)")
    producer.flush()
    if b < BATCHES:
        print(f"  >>> nghi {SLEEP_BETWEEN}s de ep commit rieng...\n")
        time.sleep(SLEEP_BETWEEN)

print(f"\n  >>> doi them 15s cho commit cuoi...\n")
time.sleep(15)
print(f"DONE - da gui {total} message qua {BATCHES} batch")
producer.close()
