"""
Test APPEND API-push contract: one topic carries one raw data format.

Kafka value is the exact payload written to `data` by StringConverter.
Optional API metadata is propagated through Kafka headers:
  - api.body: API parameters/body metadata JSON
  - api.headers: HTTP headers JSON; SMT keeps only the safe allowlist
"""
from kafka import KafkaProducer
import json
import sys
import time

producer = KafkaProducer(
    bootstrap_servers=["localhost:29092"],
    value_serializer=lambda value: value.encode("utf-8"),
)

format_name = sys.argv[1] if len(sys.argv) > 1 else "json"
if format_name == "xml":
    topic = "qtmt-append-xml"
    filename = "sample_new.xml"
else:
    topic = "qtmt-append"
    filename = "sample_new.json"

with open(filename, "r", encoding="utf-8") as source_file:
    payload = source_file.read()

api_body = json.dumps(
    {"tuNgay": "2026-07-30", "denNgay": "2026-08-07"},
    ensure_ascii=False,
).encode("utf-8")
api_headers = json.dumps(
    {
        "Content-Type": "application/json",
        "X-Request-ID": "append-manual-test",
        "Authorization": "Basic must-not-land",
    }
).encode("utf-8")

print("=" * 60)
print(f"APPEND TEST | format={format_name} | topic={topic}")
print("=" * 60)
producer.send(
    topic,
    value=payload,
    headers=[("api.body", api_body), ("api.headers", api_headers)],
)
producer.flush()
print(f"SENT {format_name} payload ({len(payload)} chars)")
print("Waiting 15s for connector commit...")
time.sleep(15)

print("DONE - 1 message sent")
print(
    "VERIFY:\n"
    "  docker exec iceberg-kafka-connect-demo-trino-1 trino --execute "
    '"SELECT loainguon, manguondulieu, phienban, body, header, '
    "substr(data,1,40), ingest_date, ingest_time "
    'FROM iceberg.def.abc_append_v2 ORDER BY manguondulieu"'
)
producer.close()