"""
Test Pipeline: qtmt-tramquantrac → CustomCDCTransform → Iceberg (def.abc)
=========================================================================
connector.name = sink-qtmt-tramquantrac
typeingest     = API
topic.table.map = qtmt-tramquantrac:def.abc

BATCH 1: 3 INSERTs (6 tram)
BATCH 2: 2 UPDATEs + 1 DELETE
BATCH 3: 1 INSERT sau DELETE + 1 Stale (DROP) + 1 INSERT moi

KET QUA MONG DOI (def.abc, 4 rows cuoi cung):
  TRAM001||TRAM002  UPDATE v2
  TRAM003           INSERT v1 (tai tao sau DELETE)
  TRAM005||TRAM006  UPDATE v2
  TRAM007||TRAM008  INSERT v1
"""
from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers=['localhost:29092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC = 'qtmt-tramquantrac'

def send(msg, label):
    producer.send(TOPIC, value=msg)
    producer.flush()
    keys = [d.get(msg['key']) for d in msg['data']]
    print(f"  {msg['type']:6} v{msg['version']} keys={keys} | {label}")

# ============================================================
print("=" * 60)
print("BATCH 1: 3 INSERTs")
print("=" * 60)

send({"data": [
    {"MaTram": "TRAM001", "TenTram": "Tram Ha Noi 1", "MoTa": "Khong khi", "DiaChiChiTiet": "Cau Giay, Ha Noi", "MaXa": "XA001", "TenXa": "Cau Giay", "MaTinh": "01", "TenTinh": "Ha Noi", "KinhDo": 105.8, "ViDo": 21.03, "LoaiHinhQuanTrac": "KHONGKHI", "ThongSo": "SO2, NO2", "DonViQuanLyVanHanh": "So TNMT Ha Noi"},
    {"MaTram": "TRAM002", "TenTram": "Tram Hai Phong", "MoTa": "Nuoc mat", "DiaChiChiTiet": "Le Chan, Hai Phong", "MaXa": "XA002", "TenXa": "Le Chan", "MaTinh": "31", "TenTinh": "Hai Phong", "KinhDo": 106.68, "ViDo": 20.85, "LoaiHinhQuanTrac": "NUOCMAT", "ThongSo": "pH, DO", "DonViQuanLyVanHanh": "So TNMT Hai Phong"}
], "length": 2, "key": "MaTram", "type": "INSERT", "version": 1, "ngay_cap_nhat": "16/05/2026-12:00"}, "TRAM001||TRAM002")

send({"data": [
    {"MaTram": "TRAM003", "TenTram": "Tram Da Nang", "MoTa": "Nuoc ngam", "DiaChiChiTiet": "Hai Chau, Da Nang", "MaXa": "XA003", "TenXa": "Hai Chau", "MaTinh": "48", "TenTinh": "Da Nang", "KinhDo": 108.22, "ViDo": 16.07, "LoaiHinhQuanTrac": "NUOCNGAM", "ThongSo": "As, Pb, Fe", "DonViQuanLyVanHanh": "So TNMT Da Nang"}
], "length": 1, "key": "MaTram", "type": "INSERT", "version": 1, "ngay_cap_nhat": "16/05/2026-12:00"}, "TRAM003")

send({"data": [
    {"MaTram": "TRAM005", "TenTram": "Tram QN1", "MoTa": "Nuoc bien", "DiaChiChiTiet": "Ha Long, Quang Ninh", "MaXa": "XA005", "TenXa": "Ha Long", "MaTinh": "22", "TenTinh": "Quang Ninh", "KinhDo": 107.08, "ViDo": 20.95, "LoaiHinhQuanTrac": "NUOCBIEN", "ThongSo": "pH, Cl, DO", "DonViQuanLyVanHanh": "So TNMT Quang Ninh"},
    {"MaTram": "TRAM006", "TenTram": "Tram QN2", "MoTa": "Khong khi", "DiaChiChiTiet": "Cam Pha, Quang Ninh", "MaXa": "XA006", "TenXa": "Cam Pha", "MaTinh": "22", "TenTinh": "Quang Ninh", "KinhDo": 107.3, "ViDo": 21.0, "LoaiHinhQuanTrac": "KHONGKHI", "ThongSo": "PM10, SO2, PM2.5", "DonViQuanLyVanHanh": "So TNMT Quang Ninh"}
], "length": 2, "key": "MaTram", "type": "INSERT", "version": 1, "ngay_cap_nhat": "16/05/2026-12:00"}, "TRAM005||TRAM006")

print(f"\n  >>> BATCH 1 DONE. Doi 15s...\n")
time.sleep(15)

# ============================================================
print("=" * 60)
print("BATCH 2: 2 UPDATEs + 1 DELETE")
print("=" * 60)

send({"data": [
    {"MaTram": "TRAM001", "TenTram": "Tram Ha Noi 1 - CAP NHAT", "MoTa": "Bo sung PM2.5", "DiaChiChiTiet": "Cau Giay, Ha Noi", "MaXa": "XA001", "TenXa": "Cau Giay", "MaTinh": "01", "TenTinh": "Ha Noi", "KinhDo": 105.8, "ViDo": 21.03, "LoaiHinhQuanTrac": "KHONGKHI", "ThongSo": "SO2, NO2, PM2.5", "DonViQuanLyVanHanh": "So TNMT Ha Noi"},
    {"MaTram": "TRAM002", "TenTram": "Tram Hai Phong - CAP NHAT", "MoTa": "Them BOD", "DiaChiChiTiet": "Le Chan, Hai Phong", "MaXa": "XA002", "TenXa": "Le Chan", "MaTinh": "31", "TenTinh": "Hai Phong", "KinhDo": 106.68, "ViDo": 20.85, "LoaiHinhQuanTrac": "NUOCMAT", "ThongSo": "pH, DO, BOD", "DonViQuanLyVanHanh": "So TNMT Hai Phong"}
], "length": 2, "key": "MaTram", "type": "UPDATE", "version": 2, "ngay_cap_nhat": "16/05/2026-12:15"}, "TRAM001||TRAM002 UPDATE v2")

send({"data": [
    {"MaTram": "TRAM003", "TenTram": "Tram Da Nang", "MoTa": "XOA", "DiaChiChiTiet": "Hai Chau, Da Nang", "MaXa": "XA003", "TenXa": "Hai Chau", "MaTinh": "48", "TenTinh": "Da Nang", "KinhDo": 108.22, "ViDo": 16.07, "LoaiHinhQuanTrac": "NUOCNGAM", "ThongSo": "As, Pb, Fe", "DonViQuanLyVanHanh": "So TNMT Da Nang"}
], "length": 1, "key": "MaTram", "type": "DELETE", "version": 2, "ngay_cap_nhat": "16/05/2026-12:15"}, "TRAM003 DELETE v2")

send({"data": [
    {"MaTram": "TRAM005", "TenTram": "Tram QN1 - CAP NHAT", "MoTa": "Them NH4", "DiaChiChiTiet": "Ha Long, Quang Ninh", "MaXa": "XA005", "TenXa": "Ha Long", "MaTinh": "22", "TenTinh": "Quang Ninh", "KinhDo": 107.08, "ViDo": 20.95, "LoaiHinhQuanTrac": "NUOCBIEN", "ThongSo": "pH, Cl, DO, NH4", "DonViQuanLyVanHanh": "So TNMT Quang Ninh"},
    {"MaTram": "TRAM006", "TenTram": "Tram QN2 - CAP NHAT", "MoTa": "Them CO", "DiaChiChiTiet": "Cam Pha, Quang Ninh", "MaXa": "XA006", "TenXa": "Cam Pha", "MaTinh": "22", "TenTinh": "Quang Ninh", "KinhDo": 107.3, "ViDo": 21.0, "LoaiHinhQuanTrac": "KHONGKHI", "ThongSo": "PM10, SO2, PM2.5, CO", "DonViQuanLyVanHanh": "So TNMT Quang Ninh"}
], "length": 2, "key": "MaTram", "type": "UPDATE", "version": 2, "ngay_cap_nhat": "16/05/2026-12:15"}, "TRAM005||TRAM006 UPDATE v2")

print(f"\n  >>> BATCH 2 DONE. Doi 15s...\n")
time.sleep(15)

# ============================================================
print("=" * 60)
print("BATCH 3: INSERT sau DELETE + Stale (DROP) + INSERT moi")
print("=" * 60)

send({"data": [
    {"MaTram": "TRAM003", "TenTram": "Tram Da Nang - TAI TAO", "MoTa": "Tram moi sau xoa", "DiaChiChiTiet": "Hai Chau, Da Nang", "MaXa": "XA003", "TenXa": "Hai Chau", "MaTinh": "48", "TenTinh": "Da Nang", "KinhDo": 108.22, "ViDo": 16.07, "LoaiHinhQuanTrac": "NUOCNGAM", "ThongSo": "As, Pb, Fe, Mn", "DonViQuanLyVanHanh": "So TNMT Da Nang"}
], "length": 1, "key": "MaTram", "type": "INSERT", "version": 1, "ngay_cap_nhat": "16/05/2026-12:30"}, "TRAM003 INSERT v1 (sau DELETE)")

send({"data": [
    {"MaTram": "TRAM001", "TenTram": "STALE", "MoTa": "KHONG GHI", "DiaChiChiTiet": "X", "MaXa": "XA001", "TenXa": "X", "MaTinh": "01", "TenTinh": "Ha Noi", "KinhDo": 105.8, "ViDo": 21.03, "LoaiHinhQuanTrac": "KHONGKHI", "ThongSo": "OLD", "DonViQuanLyVanHanh": "OLD"},
    {"MaTram": "TRAM002", "TenTram": "STALE", "MoTa": "KHONG GHI", "DiaChiChiTiet": "X", "MaXa": "XA002", "TenXa": "X", "MaTinh": "31", "TenTinh": "Hai Phong", "KinhDo": 106.68, "ViDo": 20.85, "LoaiHinhQuanTrac": "NUOCMAT", "ThongSo": "OLD", "DonViQuanLyVanHanh": "OLD"}
], "length": 2, "key": "MaTram", "type": "UPDATE", "version": 1, "ngay_cap_nhat": "16/05/2026-11:00"}, "TRAM001||TRAM002 STALE v1 (SHOULD BE DROPPED)")

send({"data": [
    {"MaTram": "TRAM007", "TenTram": "Tram Ninh Binh", "MoTa": "Dat nong nghiep", "DiaChiChiTiet": "Hoa Lu, Ninh Binh", "MaXa": "XA007", "TenXa": "Hoa Lu", "MaTinh": "37", "TenTinh": "Ninh Binh", "KinhDo": 105.97, "ViDo": 20.25, "LoaiHinhQuanTrac": "DAT", "ThongSo": "Pb, Cd, Hg", "DonViQuanLyVanHanh": "So TNMT Ninh Binh"},
    {"MaTram": "TRAM008", "TenTram": "Tram Bac Ninh", "MoTa": "Nuoc thai KCN", "DiaChiChiTiet": "Tu Son, Bac Ninh", "MaXa": "XA008", "TenXa": "Tu Son", "MaTinh": "27", "TenTinh": "Bac Ninh", "KinhDo": 106.0, "ViDo": 21.12, "LoaiHinhQuanTrac": "NUOCTHAI", "ThongSo": "COD, BOD, TSS, NH4", "DonViQuanLyVanHanh": "So TNMT Bac Ninh"}
], "length": 2, "key": "MaTram", "type": "INSERT", "version": 1, "ngay_cap_nhat": "16/05/2026-12:30"}, "TRAM007||TRAM008 INSERT v1")

print(f"\n  >>> BATCH 3 DONE. Doi 15s...\n")
time.sleep(15)

# ============================================================
print("=" * 60)
print("DONE - 9 messages sent to qtmt-tramquantrac")
print("=" * 60)
print("""
VERIFY:
  docker cp query-snapshots.sql iceberg-kafka-connect-demo-trino-1:/tmp/q.sql
  docker exec iceberg-kafka-connect-demo-trino-1 trino -f /tmp/q.sql

  docker exec iceberg-kafka-connect-demo-trino-1 trino --execute "SELECT dedup_key, type, version FROM iceberg.def.abc ORDER BY dedup_key"

KET QUA MONG DOI (def.abc, 4 rows):
  dedup_key          | type   | version
  TRAM001||TRAM002   | UPDATE | 2
  TRAM003            | INSERT | 1
  TRAM005||TRAM006   | UPDATE | 2
  TRAM007||TRAM008   | INSERT | 1

SNAPSHOT METADATA moi commit:
  connector.name = sink-qtmt-tramquantrac
  typeingest     = API
""")
producer.close()
