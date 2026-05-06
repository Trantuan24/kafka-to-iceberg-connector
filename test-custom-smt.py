#!/usr/bin/env python3
"""
Test script for Phase 1: Custom SMT + Official Apache Iceberg Sink Connector
Append-only mode - sends messages and verifies they arrive in Iceberg table.

Usage:
    python3 test-custom-smt.py               # Run all tests
    python3 test-custom-smt.py --test 1       # Run single test
    python3 test-custom-smt.py --count-only   # Just check row count
"""

from kafka import KafkaProducer
import json
import time
import sys
import argparse
import subprocess


# Kafka broker address (EXTERNAL listener for host access)
KAFKA_BOOTSTRAP = 'localhost:29092'
TOPIC = 'tram_quan_trac'


def create_producer():
    """Create Kafka producer connecting to EXTERNAL listener."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )
        print(f" Connected to Kafka at {KAFKA_BOOTSTRAP}")
        return producer
    except Exception as e:
        print(f" Failed to connect to Kafka at {KAFKA_BOOTSTRAP}: {e}")
        print("   Make sure Kafka is running: docker-compose ps kafka")
        sys.exit(1)


def send_message(producer, topic, message, key=None):
    """Send a message to Kafka and wait for acknowledgment."""
    try:
        future = producer.send(topic, value=message, key=key)
        result = future.get(timeout=10)
        print(f"   Sent to {topic} (partition={result.partition}, offset={result.offset})")
        return result
    except Exception as e:
        print(f"   Failed to send message: {e}")
        return None


def test_single_insert(producer):
    """Test 1: Single INSERT with one record in data[]."""
    print("\n" + "="*60)
    print("TEST 1: Single INSERT (1 item in data)")
    print("="*60)
    
    message = {
        "data": [
            {
                "MaTram": "TRAM001",
                "TenTram": "Trm quan trc H Ni 1",
                "MoTa": "Trm gim st khng kh",
                "DiaChiChiTiet": "Cu Giy, H Ni",
                "KinhDo": 105.8,
                "ViDo": 21.03,
                "LoaiHinhQuanTrac": "KHONGKHI"
            }
        ],
        "length": 1,
        "key": "MaTram",
        "type": "INSERT",
        "version": 1,
        "ngay_cap_nhat": "2026-05-05T10:00:00Z"
    }
    
    print(f"  Message: type={message['type']}, key={message['key']}, version={message['version']}, length={message['length']}")
    result = send_message(producer, TOPIC, message, key="TRAM001")
    if result:
        expected_id = f"{TOPIC}-{result.partition}-{result.offset}"
        print(f"  Expected id in Iceberg: {expected_id}")
    return result is not None


def test_batch_insert(producer):
    """Test 2: INSERT with multiple records in data[] (batch)."""
    print("\n" + "="*60)
    print("TEST 2: Batch INSERT (2 items in data)")
    print("="*60)
    
    message = {
        "data": [
            {
                "MaTram": "TRAM002",
                "TenTram": "Trm quan trc Hi Phng",
                "MoTa": "Trm nc mt",
                "DiaChiChiTiet": "L Chn, Hi Phng",
                "KinhDo": 106.68,
                "ViDo": 20.85,
                "LoaiHinhQuanTrac": "NUOCMAT"
            },
            {
                "MaTram": "TRAM003",
                "TenTram": "Trm quan trc  Nng",
                "MoTa": "Trm nc thi",
                "DiaChiChiTiet": "Hi Chu,  Nng",
                "KinhDo": 108.22,
                "ViDo": 16.05,
                "LoaiHinhQuanTrac": "NUOCTHAI"
            }
        ],
        "length": 2,
        "key": "MaTram",
        "type": "INSERT",
        "version": 1,
        "ngay_cap_nhat": "2026-05-05T10:01:00Z"
    }
    
    print(f"  Message: type={message['type']}, key={message['key']}, version={message['version']}, length={message['length']}")
    result = send_message(producer, TOPIC, message, key="BATCH001")
    return result is not None


def test_update_message(producer):
    """Test 3: UPDATE message (still append in Phase 1)."""
    print("\n" + "="*60)
    print("TEST 3: UPDATE message (appended in Phase 1)")
    print("="*60)
    
    message = {
        "data": [
            {
                "MaTram": "TRAM001",
                "TenTram": "Trm quan trc H Ni 1 - Updated",
                "MoTa": "Trm gim st khng kh - cp nht",
                "DiaChiChiTiet": "Cu Giy, H Ni",
                "KinhDo": 105.8,
                "ViDo": 21.03,
                "LoaiHinhQuanTrac": "KHONGKHI"
            }
        ],
        "length": 1,
        "key": "MaTram",
        "type": "UPDATE",
        "version": 2,
        "ngay_cap_nhat": "2026-05-05T11:00:00Z"
    }
    
    print(f"  Message: type={message['type']}, key={message['key']}, version={message['version']}")
    print("  Note: In Phase 1 (append), UPDATE is just another row appended to table.")
    result = send_message(producer, TOPIC, message, key="TRAM001")
    return result is not None


def test_delete_message(producer):
    """Test 4: DELETE message (still append in Phase 1)."""
    print("\n" + "="*60)
    print("TEST 4: DELETE message (appended in Phase 1)")
    print("="*60)
    
    message = {
        "data": [
            {"MaTram": "TRAM001"}
        ],
        "length": 1,
        "key": "MaTram",
        "type": "DELETE",
        "version": 3,
        "ngay_cap_nhat": "2026-05-05T12:00:00Z"
    }
    
    print(f"  Message: type={message['type']}, key={message['key']}, version={message['version']}")
    print("  Note: In Phase 1 (append), DELETE is just another row appended to table.")
    result = send_message(producer, TOPIC, message, key="TRAM001")
    return result is not None


def test_full_example(producer):
    """Test 5: Full example message matching the spec exactly."""
    print("\n" + "="*60)
    print("TEST 5: Full example message (from spec)")
    print("="*60)
    
    message = {
        "data": [
            {
                "MaTram": "TRAM001",
                "TenTram": "Trm quan trc H Ni 1",
                "MoTa": "Trm gim st khng kh",
                "DiaChiChiTiet": "Cu Giy, H Ni",
                "MaXa": "XA001",
                "TenXa": "Cu Giy",
                "MaTinh": "01",
                "TenTinh": "H Ni",
                "KinhDo": 105.8,
                "ViDo": 21.03,
                "LoaiHinhQuanTrac": "KHONGKHI",
                "ThongSo": "SO2, NO2",
                "DonViQuanLyVanHanh": "S TNMT H Ni"
            },
            {
                "MaTram": "TRAM002",
                "TenTram": "Trm quan trc Hi Phng",
                "MoTa": "Trm nc mt",
                "DiaChiChiTiet": "L Chn, Hi Phng",
                "MaXa": "XA002",
                "TenXa": "L Chn",
                "MaTinh": "31",
                "TenTinh": "Hi Phng",
                "KinhDo": 106.68,
                "ViDo": 20.85,
                "LoaiHinhQuanTrac": "NUOCMAT",
                "ThongSo": "pH, DO",
                "DonViQuanLyVanHanh": "S TNMT Hi Phng"
            }
        ],
        "length": 2,
        "key": "MaTram",
        "type": "INSERT",
        "version": 1,
        "ngay_cap_nhat": "2026-04-19T15:00:00Z"
    }
    
    print(f"  Message: type={message['type']}, key={message['key']}, version={message['version']}, length={message['length']}")
    print(f"  Data items: {len(message['data'])}")
    result = send_message(producer, TOPIC, message, key="FULL_EXAMPLE")
    return result is not None


def check_count():
    """Check current row count in Iceberg table via Trino."""
    print("\n" + "="*60)
    print("Checking Iceberg table row count...")
    print("="*60)
    try:
        result = subprocess.run(
            ["docker", "exec", "iceberg-kafka-connect-demo-trino-1",
             "trino", "--execute",
             "SELECT COUNT(*) as cnt FROM iceberg.default.tram_quan_trac_cdc"],
            capture_output=True, text=True, timeout=15
        )
        if result.returncode == 0:
            count = result.stdout.strip().strip('"')
            print(f"   Row count: {count}")
            return int(count) if count.isdigit() else 0
        else:
            print(f"    Query failed: {result.stderr.strip()}")
            return -1
    except Exception as e:
        print(f"    Could not query Trino: {e}")
        return -1


def main():
    parser = argparse.ArgumentParser(description='Test Phase 1 Pipeline')
    parser.add_argument('--test', type=int, help='Run specific test (1-5)')
    parser.add_argument('--count-only', action='store_true', help='Just check row count')
    args = parser.parse_args()

    print("="*60)
    print("Phase 1 Test: Custom SMT + Official Iceberg Connector")
    print("Mode: APPEND ONLY (no upsert/delete)")
    print(f"Kafka: {KAFKA_BOOTSTRAP}")
    print(f"Topic: {TOPIC}")
    print("="*60)

    if args.count_only:
        check_count()
        return

    # Get count before
    count_before = check_count()

    producer = create_producer()
    
    tests = {
        1: ("Single INSERT", test_single_insert),
        2: ("Batch INSERT", test_batch_insert),
        3: ("UPDATE message", test_update_message),
        4: ("DELETE message", test_delete_message),
        5: ("Full example", test_full_example),
    }
    
    results = {}
    
    try:
        if args.test:
            if args.test in tests:
                name, func = tests[args.test]
                results[args.test] = func(producer)
            else:
                print(f" Unknown test: {args.test}. Available: {list(tests.keys())}")
                sys.exit(1)
        else:
            for num, (name, func) in tests.items():
                results[num] = func(producer)
        
        producer.flush()
        
        # Summary
        print("\n" + "="*60)
        print("Test Results")
        print("="*60)
        
        sent_count = sum(1 for v in results.values() if v)
        total = len(results)
        
        for num in sorted(results.keys()):
            name = tests[num][0]
            status = " SENT" if results[num] else " FAILED"
            print(f"  Test {num} ({name}): {status}")
        
        print(f"\n  Sent: {sent_count}/{total} messages")
        
        # Wait for Iceberg commit
        print(f"\n Waiting 15s for Iceberg commit (interval=10s)...")
        time.sleep(15)
        
        # Check count after
        count_after = check_count()
        
        if count_after > 0 and count_before >= 0:
            new_rows = count_after - count_before
            print(f"\n  New rows: {new_rows} (expected: {sent_count})")
            if new_rows == sent_count:
                print("   All messages written to Iceberg!")
            elif new_rows > 0:
                print("    Some messages written. May need more time for commit.")
            else:
                print("   No new rows. Check connector logs:")
                print(f"     docker logs {'-f' if False else '--tail 50'} iceberg-kafka-connect-demo-connect-1")
        
        print("\n NEXT STEPS:")
        print("1. Check detailed results:")
        print("   bash check-results.sh")
        print("")
        print("2. Monitor logs:")
        print("   docker logs -f iceberg-kafka-connect-demo-connect-1 2>&1 | grep -i 'customcdc\\|committed\\|error'")
        print("")
        print("3. Query full data:")
        print("   docker exec iceberg-kafka-connect-demo-trino-1 trino --execute \\")
        print("     \"SELECT * FROM iceberg.default.tram_quan_trac_cdc ORDER BY version\"")
        
    except KeyboardInterrupt:
        print("\n\n  Test interrupted by user")
    finally:
        producer.close()
        print("\n Producer closed")


if __name__ == "__main__":
    main()
