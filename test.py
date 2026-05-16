"""
Test Pipeline: 2 Topics, Dynamic Routing, CDC (I/U/D), Version Filter
======================================================================
Topic 1: qtmt-tramquantrac    -> auto-derive -> default.qtmt_tramquantrac
Topic 2: qtmt-quantrackhithai -> custom map  -> def.abc

BATCH 1: 6 INSERTs
BATCH 2: 3 UPDATEs + 2 DELETEs + 1 UPDATE
BATCH 3: 2 INSERTs sau DELETE + 1 Stale (DROP) + 2 INSERT moi

KET QUA MONG DOI:
  default.qtmt_tramquantrac (4 rows):
    TRAM001||TRAM002  UPDATE v2
    TRAM005||TRAM006  UPDATE v2
    TRAM003           INSERT v1 (tai tao sau DELETE)
    TRAM007||TRAM008  INSERT v1
  def.abc (4 rows):
    TR001  UPDATE v2
    TR003  UPDATE v2
    TR002  INSERT v1 (tai tao sau DELETE)
    TR004  INSERT v1
"""
from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers=['localhost:29092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

T1 = 'qtmt-tramquantrac'
T2 = 'qtmt-quantrackhithai'

def send(topic, msg, label):
    producer.send(topic, value=msg)
    producer.flush()
    keys = [d.get(msg['key']) for d in msg['data']]
    t = "T1" if topic == T1 else "T2"
    print(f"  [{t}] {msg['type']:6} v{msg['version']} keys={keys} | {label}")

# ============================================================
print("=" * 60)
print("BATCH 1: 6 INSERTs (lien tuc)")
print("=" * 60)

send(T1, {"data": [
    {"MaTram": "TRAM001", "TenTram": "Tram quan trac Ha Noi 1", "MoTa": "Tram giam sat khong khi", "DiaChiChiTiet": "Cau Giay, Ha Noi", "MaXa": "XA001", "TenXa": "Cau Giay", "MaTinh": "01", "TenTinh": "Ha Noi", "KinhDo": 105.8, "ViDo": 21.03, "LoaiHinhQuanTrac": "KHONGKHI", "ThongSo": "SO2, NO2", "DonViQuanLyVanHanh": "So TNMT Ha Noi"},
    {"MaTram": "TRAM002", "TenTram": "Tram quan trac Hai Phong", "MoTa": "Tram nuoc mat", "DiaChiChiTiet": "Le Chan, Hai Phong", "MaXa": "XA002", "TenXa": "Le Chan", "MaTinh": "31", "TenTinh": "Hai Phong", "KinhDo": 106.68, "ViDo": 20.85, "LoaiHinhQuanTrac": "NUOCMAT", "ThongSo": "pH, DO", "DonViQuanLyVanHanh": "So TNMT Hai Phong"}
], "length": 2, "key": "MaTram", "type": "INSERT", "version": 1, "ngay_cap_nhat": "16/05/2026-12:00"}, "TRAM001||TRAM002")

send(T1, {"data": [
    {"MaTram": "TRAM003", "TenTram": "Tram quan trac Da Nang", "MoTa": "Tram nuoc ngam", "DiaChiChiTiet": "Hai Chau, Da Nang", "MaXa": "XA003", "TenXa": "Hai Chau", "MaTinh": "48", "TenTinh": "Da Nang", "KinhDo": 108.22, "ViDo": 16.07, "LoaiHinhQuanTrac": "NUOCNGAM", "ThongSo": "As, Pb, Fe", "DonViQuanLyVanHanh": "So TNMT Da Nang"}
], "length": 1, "key": "MaTram", "type": "INSERT", "version": 1, "ngay_cap_nhat": "16/05/2026-12:00"}, "TRAM003")

send(T1, {"data": [
    {"MaTram": "TRAM005", "TenTram": "Tram quan trac Quang Ninh 1", "MoTa": "Tram nuoc bien", "DiaChiChiTiet": "Ha Long, Quang Ninh", "MaXa": "XA005", "TenXa": "Ha Long", "MaTinh": "22", "TenTinh": "Quang Ninh", "KinhDo": 107.08, "ViDo": 20.95, "LoaiHinhQuanTrac": "NUOCBIEN", "ThongSo": "pH, Cl, DO", "DonViQuanLyVanHanh": "So TNMT Quang Ninh"},
    {"MaTram": "TRAM006", "TenTram": "Tram quan trac Quang Ninh 2", "MoTa": "Tram khong khi", "DiaChiChiTiet": "Cam Pha, Quang Ninh", "MaXa": "XA006", "TenXa": "Cam Pha", "MaTinh": "22", "TenTinh": "Quang Ninh", "KinhDo": 107.3, "ViDo": 21.0, "LoaiHinhQuanTrac": "KHONGKHI", "ThongSo": "PM10, SO2, PM2.5", "DonViQuanLyVanHanh": "So TNMT Quang Ninh"}
], "length": 2, "key": "MaTram", "type": "INSERT", "version": 1, "ngay_cap_nhat": "16/05/2026-12:00"}, "TRAM005||TRAM006")

send(T2, {"data": [
    {"MaTramQuanTrac": "TR001", "LoaiHinhQuanTrac": "Khi", "ThongSo": "PM2.5", "NamQuanTrac": 2026, "ThangQuanTrac": 5, "NgayQuanTrac": 16, "KetQuaQuanTracTrungBinh01gio": 35.2, "KetQuaQuanTracTrungBinh24gio": 40.1, "KetQuaQuanTracTrungBinhThang": 38.5, "SoGioVuotQCVN": 5, "TyLeVuotQCVN_1h": 12.5, "SoNgayVuotQCVN": 2, "TyLeNgayVuotQCVN": 6.7, "QuyChuanMoiTruong": "QCVN 05:2013/BTNMT", "TrangThaiDo": 1, "created_at": "2026-05-16T12:00:00Z", "updated_at": "2026-05-16T12:00:00Z"}
], "length": 1, "key": "MaTramQuanTrac", "type": "INSERT", "version": 1, "ngay_cap_nhat": "16/05/2026-12:00"}, "TR001")

send(T2, {"data": [
    {"MaTramQuanTrac": "TR002", "LoaiHinhQuanTrac": "Khi", "ThongSo": "SO2", "NamQuanTrac": 2026, "ThangQuanTrac": 5, "NgayQuanTrac": 16, "KetQuaQuanTracTrungBinh01gio": 18.7, "KetQuaQuanTracTrungBinh24gio": 22.3, "KetQuaQuanTracTrungBinhThang": 20.1, "SoGioVuotQCVN": 0, "TyLeVuotQCVN_1h": 0, "SoNgayVuotQCVN": 0, "TyLeNgayVuotQCVN": 0, "QuyChuanMoiTruong": "QCVN 05:2013/BTNMT", "TrangThaiDo": 1, "created_at": "2026-05-16T12:00:00Z", "updated_at": "2026-05-16T12:00:00Z"}
], "length": 1, "key": "MaTramQuanTrac", "type": "INSERT", "version": 1, "ngay_cap_nhat": "16/05/2026-12:00"}, "TR002")

send(T2, {"data": [
    {"MaTramQuanTrac": "TR003", "LoaiHinhQuanTrac": "Khi", "ThongSo": "NO2", "NamQuanTrac": 2026, "ThangQuanTrac": 5, "NgayQuanTrac": 16, "KetQuaQuanTracTrungBinh01gio": 22.8, "KetQuaQuanTracTrungBinh24gio": 20.5, "KetQuaQuanTracTrungBinhThang": 19.0, "SoGioVuotQCVN": 0, "TyLeVuotQCVN_1h": 0, "SoNgayVuotQCVN": 0, "TyLeNgayVuotQCVN": 0, "QuyChuanMoiTruong": "QCVN 05:2013/BTNMT", "TrangThaiDo": 1, "created_at": "2026-05-16T12:00:00Z", "updated_at": "2026-05-16T12:00:00Z"}
], "length": 1, "key": "MaTramQuanTrac", "type": "INSERT", "version": 1, "ngay_cap_nhat": "16/05/2026-12:00"}, "TR003")

print(f"\n  >>> BATCH 1 DONE. Doi 15s...\n")
time.sleep(15)

# ============================================================
print("=" * 60)
print("BATCH 2: 3 UPDATEs + 2 DELETEs + 1 UPDATE")
print("=" * 60)

send(T1, {"data": [
    {"MaTram": "TRAM001", "TenTram": "Tram Ha Noi 1 - CAP NHAT", "MoTa": "Bo sung PM2.5", "DiaChiChiTiet": "Cau Giay, Ha Noi", "MaXa": "XA001", "TenXa": "Cau Giay", "MaTinh": "01", "TenTinh": "Ha Noi", "KinhDo": 105.8, "ViDo": 21.03, "LoaiHinhQuanTrac": "KHONGKHI", "ThongSo": "SO2, NO2, PM2.5", "DonViQuanLyVanHanh": "So TNMT Ha Noi"},
    {"MaTram": "TRAM002", "TenTram": "Tram Hai Phong - CAP NHAT", "MoTa": "Them BOD", "DiaChiChiTiet": "Le Chan, Hai Phong", "MaXa": "XA002", "TenXa": "Le Chan", "MaTinh": "31", "TenTinh": "Hai Phong", "KinhDo": 106.68, "ViDo": 20.85, "LoaiHinhQuanTrac": "NUOCMAT", "ThongSo": "pH, DO, BOD", "DonViQuanLyVanHanh": "So TNMT Hai Phong"}
], "length": 2, "key": "MaTram", "type": "UPDATE", "version": 2, "ngay_cap_nhat": "16/05/2026-12:15"}, "TRAM001||TRAM002 UPDATE")

send(T1, {"data": [
    {"MaTram": "TRAM003", "TenTram": "Tram Da Nang", "MoTa": "XOA", "DiaChiChiTiet": "Hai Chau, Da Nang", "MaXa": "XA003", "TenXa": "Hai Chau", "MaTinh": "48", "TenTinh": "Da Nang", "KinhDo": 108.22, "ViDo": 16.07, "LoaiHinhQuanTrac": "NUOCNGAM", "ThongSo": "As, Pb, Fe", "DonViQuanLyVanHanh": "So TNMT Da Nang"}
], "length": 1, "key": "MaTram", "type": "DELETE", "version": 2, "ngay_cap_nhat": "16/05/2026-12:15"}, "TRAM003 DELETE")

send(T1, {"data": [
    {"MaTram": "TRAM005", "TenTram": "Tram QN1 - CAP NHAT", "MoTa": "Them NH4", "DiaChiChiTiet": "Ha Long, Quang Ninh", "MaXa": "XA005", "TenXa": "Ha Long", "MaTinh": "22", "TenTinh": "Quang Ninh", "KinhDo": 107.08, "ViDo": 20.95, "LoaiHinhQuanTrac": "NUOCBIEN", "ThongSo": "pH, Cl, DO, NH4", "DonViQuanLyVanHanh": "So TNMT Quang Ninh"},
    {"MaTram": "TRAM006", "TenTram": "Tram QN2 - CAP NHAT", "MoTa": "Them CO", "DiaChiChiTiet": "Cam Pha, Quang Ninh", "MaXa": "XA006", "TenXa": "Cam Pha", "MaTinh": "22", "TenTinh": "Quang Ninh", "KinhDo": 107.3, "ViDo": 21.0, "LoaiHinhQuanTrac": "KHONGKHI", "ThongSo": "PM10, SO2, PM2.5, CO", "DonViQuanLyVanHanh": "So TNMT Quang Ninh"}
], "length": 2, "key": "MaTram", "type": "UPDATE", "version": 2, "ngay_cap_nhat": "16/05/2026-12:15"}, "TRAM005||TRAM006 UPDATE")

send(T2, {"data": [
    {"MaTramQuanTrac": "TR001", "LoaiHinhQuanTrac": "Khi", "ThongSo": "PM2.5", "NamQuanTrac": 2026, "ThangQuanTrac": 5, "NgayQuanTrac": 16, "KetQuaQuanTracTrungBinh01gio": 48.5, "KetQuaQuanTracTrungBinh24gio": 45.0, "KetQuaQuanTracTrungBinhThang": 42.0, "SoGioVuotQCVN": 10, "TyLeVuotQCVN_1h": 25.0, "SoNgayVuotQCVN": 4, "TyLeNgayVuotQCVN": 13.3, "QuyChuanMoiTruong": "QCVN 05:2013/BTNMT", "TrangThaiDo": 2, "created_at": "2026-05-16T12:00:00Z", "updated_at": "2026-05-16T12:15:00Z"}
], "length": 1, "key": "MaTramQuanTrac", "type": "UPDATE", "version": 2, "ngay_cap_nhat": "16/05/2026-12:15"}, "TR001 UPDATE")

send(T2, {"data": [
    {"MaTramQuanTrac": "TR002", "LoaiHinhQuanTrac": "Khi", "ThongSo": "SO2", "NamQuanTrac": 2026, "ThangQuanTrac": 5, "NgayQuanTrac": 16, "KetQuaQuanTracTrungBinh01gio": 0, "KetQuaQuanTracTrungBinh24gio": 0, "KetQuaQuanTracTrungBinhThang": 0, "SoGioVuotQCVN": 0, "TyLeVuotQCVN_1h": 0, "SoNgayVuotQCVN": 0, "TyLeNgayVuotQCVN": 0, "QuyChuanMoiTruong": "QCVN 05:2013/BTNMT", "TrangThaiDo": 0, "created_at": "2026-05-16T12:00:00Z", "updated_at": "2026-05-16T12:15:00Z"}
], "length": 1, "key": "MaTramQuanTrac", "type": "DELETE", "version": 2, "ngay_cap_nhat": "16/05/2026-12:15"}, "TR002 DELETE")

send(T2, {"data": [
    {"MaTramQuanTrac": "TR003", "LoaiHinhQuanTrac": "Khi", "ThongSo": "NO2", "NamQuanTrac": 2026, "ThangQuanTrac": 5, "NgayQuanTrac": 16, "KetQuaQuanTracTrungBinh01gio": 38.0, "KetQuaQuanTracTrungBinh24gio": 33.0, "KetQuaQuanTracTrungBinhThang": 30.0, "SoGioVuotQCVN": 3, "TyLeVuotQCVN_1h": 7.5, "SoNgayVuotQCVN": 1, "TyLeNgayVuotQCVN": 3.3, "QuyChuanMoiTruong": "QCVN 05:2013/BTNMT", "TrangThaiDo": 2, "created_at": "2026-05-16T12:00:00Z", "updated_at": "2026-05-16T12:15:00Z"}
], "length": 1, "key": "MaTramQuanTrac", "type": "UPDATE", "version": 2, "ngay_cap_nhat": "16/05/2026-12:15"}, "TR003 UPDATE")

print(f"\n  >>> BATCH 2 DONE. Doi 15s...\n")
time.sleep(15)

# ============================================================
print("=" * 60)
print("BATCH 3: INSERT sau DELETE + Stale DROP + INSERT moi")
print("=" * 60)

send(T1, {"data": [
    {"MaTram": "TRAM003", "TenTram": "Tram Da Nang - TAI TAO", "MoTa": "Tram moi sau xoa", "DiaChiChiTiet": "Hai Chau, Da Nang", "MaXa": "XA003", "TenXa": "Hai Chau", "MaTinh": "48", "TenTinh": "Da Nang", "KinhDo": 108.22, "ViDo": 16.07, "LoaiHinhQuanTrac": "NUOCNGAM", "ThongSo": "As, Pb, Fe, Mn", "DonViQuanLyVanHanh": "So TNMT Da Nang"}
], "length": 1, "key": "MaTram", "type": "INSERT", "version": 1, "ngay_cap_nhat": "16/05/2026-12:30"}, "TRAM003 INSERT (sau DELETE)")

send(T1, {"data": [
    {"MaTram": "TRAM001", "TenTram": "STALE DATA", "MoTa": "KHONG GHI", "DiaChiChiTiet": "X", "MaXa": "XA001", "TenXa": "X", "MaTinh": "01", "TenTinh": "Ha Noi", "KinhDo": 105.8, "ViDo": 21.03, "LoaiHinhQuanTrac": "KHONGKHI", "ThongSo": "OLD", "DonViQuanLyVanHanh": "OLD"},
    {"MaTram": "TRAM002", "TenTram": "STALE DATA", "MoTa": "KHONG GHI", "DiaChiChiTiet": "X", "MaXa": "XA002", "TenXa": "X", "MaTinh": "31", "TenTinh": "Hai Phong", "KinhDo": 106.68, "ViDo": 20.85, "LoaiHinhQuanTrac": "NUOCMAT", "ThongSo": "OLD", "DonViQuanLyVanHanh": "OLD"}
], "length": 2, "key": "MaTram", "type": "UPDATE", "version": 1, "ngay_cap_nhat": "16/05/2026-11:00"}, "TRAM001||TRAM002 STALE v1 (DROP)")

send(T1, {"data": [
    {"MaTram": "TRAM007", "TenTram": "Tram quan trac Ninh Binh", "MoTa": "Tram dat nong nghiep", "DiaChiChiTiet": "Hoa Lu, Ninh Binh", "MaXa": "XA007", "TenXa": "Hoa Lu", "MaTinh": "37", "TenTinh": "Ninh Binh", "KinhDo": 105.97, "ViDo": 20.25, "LoaiHinhQuanTrac": "DAT", "ThongSo": "Pb, Cd, Hg", "DonViQuanLyVanHanh": "So TNMT Ninh Binh"},
    {"MaTram": "TRAM008", "TenTram": "Tram quan trac Bac Ninh", "MoTa": "Tram nuoc thai KCN", "DiaChiChiTiet": "Tu Son, Bac Ninh", "MaXa": "XA008", "TenXa": "Tu Son", "MaTinh": "27", "TenTinh": "Bac Ninh", "KinhDo": 106.0, "ViDo": 21.12, "LoaiHinhQuanTrac": "NUOCTHAI", "ThongSo": "COD, BOD, TSS, NH4", "DonViQuanLyVanHanh": "So TNMT Bac Ninh"}
], "length": 2, "key": "MaTram", "type": "INSERT", "version": 1, "ngay_cap_nhat": "16/05/2026-12:30"}, "TRAM007||TRAM008")

send(T2, {"data": [
    {"MaTramQuanTrac": "TR002", "LoaiHinhQuanTrac": "Khi", "ThongSo": "SO2", "NamQuanTrac": 2026, "ThangQuanTrac": 5, "NgayQuanTrac": 16, "KetQuaQuanTracTrungBinh01gio": 12.0, "KetQuaQuanTracTrungBinh24gio": 10.5, "KetQuaQuanTracTrungBinhThang": 9.8, "SoGioVuotQCVN": 0, "TyLeVuotQCVN_1h": 0, "SoNgayVuotQCVN": 0, "TyLeNgayVuotQCVN": 0, "QuyChuanMoiTruong": "QCVN 05:2013/BTNMT", "TrangThaiDo": 1, "created_at": "2026-05-16T12:30:00Z", "updated_at": "2026-05-16T12:30:00Z"}
], "length": 1, "key": "MaTramQuanTrac", "type": "INSERT", "version": 1, "ngay_cap_nhat": "16/05/2026-12:30"}, "TR002 INSERT (sau DELETE)")

send(T2, {"data": [
    {"MaTramQuanTrac": "TR004", "LoaiHinhQuanTrac": "Khi", "ThongSo": "CO", "NamQuanTrac": 2026, "ThangQuanTrac": 5, "NgayQuanTrac": 16, "KetQuaQuanTracTrungBinh01gio": 3.2, "KetQuaQuanTracTrungBinh24gio": 2.8, "KetQuaQuanTracTrungBinhThang": 2.5, "SoGioVuotQCVN": 0, "TyLeVuotQCVN_1h": 0, "SoNgayVuotQCVN": 0, "TyLeNgayVuotQCVN": 0, "QuyChuanMoiTruong": "QCVN 05:2013/BTNMT", "TrangThaiDo": 1, "created_at": "2026-05-16T12:30:00Z", "updated_at": "2026-05-16T12:30:00Z"},
    {"MaTramQuanTrac": "TR005", "LoaiHinhQuanTrac": "Khi", "ThongSo": "O3", "NamQuanTrac": 2026, "ThangQuanTrac": 5, "NgayQuanTrac": 16, "KetQuaQuanTracTrungBinh01gio": 55.0, "KetQuaQuanTracTrungBinh24gio": 48.0, "KetQuaQuanTracTrungBinhThang": 45.0, "SoGioVuotQCVN": 4, "TyLeVuotQCVN_1h": 10.0, "SoNgayVuotQCVN": 2, "TyLeNgayVuotQCVN": 6.7, "QuyChuanMoiTruong": "QCVN 05:2013/BTNMT", "TrangThaiDo": 2, "created_at": "2026-05-16T12:30:00Z", "updated_at": "2026-05-16T12:30:00Z"}
], "length": 2, "key": "MaTramQuanTrac", "type": "INSERT", "version": 1, "ngay_cap_nhat": "16/05/2026-12:30"}, "TR004||TR005 (2 items)")

print(f"\n  >>> BATCH 3 DONE. Doi 15s...\n")
time.sleep(15)

# ============================================================
print("=" * 60)
print("DONE - 17 messages, 3 batches")
print("=" * 60)
print("""
VERIFY:
  docker exec iceberg-kafka-connect-demo-trino-1 trino --execute "SELECT dedup_key, type, version FROM iceberg.default.qtmt_tramquantrac ORDER BY dedup_key"
  docker exec iceberg-kafka-connect-demo-trino-1 trino --execute "SELECT dedup_key, type, version FROM iceberg.def.abc ORDER BY dedup_key"

MONG DOI:
  default.qtmt_tramquantrac: TRAM001||TRAM002 v2, TRAM003 v1, TRAM005||TRAM006 v2, TRAM007||TRAM008 v1
  def.abc: TR001 v2, TR002 v1, TR003 v2, TR004||TR005 v1
""")
