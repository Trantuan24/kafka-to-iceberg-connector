"""
Test APPEND mode: 1 TOPIC = 1 DINH DANG
========================================
- value.converter = StringConverter => message gui di la RAW STRING
- SMT mode=append: nhet toan bo body vao cot `record`, sinh `id` + `ngay_cap_nhat`
- Dich: chi 3 cot: id, record, ngay_cap_nhat

Moi topic chi mang 1 dinh dang (giong thuc te: 1 API endpoint -> 1 topic -> 1 format):
  - qtmt-append      : JSON   (sample_new.json)
  - qtmt-append-xml  : XML    (sample_new.xml)   [optional, can connector + table rieng]

Chon FORMAT muon test ben duoi.
"""
from kafka import KafkaProducer
import time
import sys

# StringConverter => gui raw string bytes (KHONG json.dumps)
producer = KafkaProducer(
    bootstrap_servers=['localhost:29092'],
    value_serializer=lambda v: v.encode('utf-8')
)

# Chon dinh dang: "json" -> topic qtmt-append ; "xml" -> topic qtmt-append-xml
FORMAT = sys.argv[1] if len(sys.argv) > 1 else "json"

if FORMAT == "xml":
    TOPIC = "qtmt-append-xml"
    with open("sample_new.xml", "r", encoding="utf-8") as f:
        payload = f.read()
else:
    TOPIC = "qtmt-append"
    with open("sample_new.json", "r", encoding="utf-8") as f:
        payload = f.read()

print("=" * 60)
print(f"APPEND TEST | format={FORMAT} | topic={TOPIC}")
print("=" * 60)

producer.send(TOPIC, value=payload)
producer.flush()
print(f"  SENT {FORMAT} payload ({len(payload)} chars)")

print("\n  >>> Doi 15s cho connector commit...\n")
time.sleep(15)

print("DONE - 1 message sent")
print(f"""
VERIFY (3 cot: id, record, ngay_cap_nhat):
  docker exec iceberg-kafka-connect-demo-trino-1 trino --execute "SELECT id, substr(record,1,40) AS record_head, ngay_cap_nhat FROM iceberg.def.abc_append ORDER BY id"
""")
producer.close()
