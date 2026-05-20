"""
Test Checkpoint Recovery: Kill & Resume
========================================
Verify:
1. Message BEFORE kill → committed to Iceberg
2. Message DURING down → picked up after restart
3. Message AFTER recovery → processed normally

Yeu cau: connector sink-qtmt-tramquantrac dang RUNNING
Chay: python test_checkpoint.py
"""
import subprocess
import json
import time
import sys

from kafka import KafkaProducer

# ============================================================
# CONFIG
# ============================================================
BOOTSTRAP = 'localhost:29092'
TOPIC = 'qtmt-tramquantrac'
CONNECT_URL = 'http://localhost:8083'
TRINO_CONTAINER = 'iceberg-kafka-connect-demo-trino-1'
COMPOSE_DIR = '.'  # root docker-compose.yml

WAIT_COMMIT = 15       # seconds to wait for Iceberg commit
WAIT_STARTUP = 120     # seconds to wait for Connect startup after restart

# ============================================================
# HELPERS
# ============================================================
producer = KafkaProducer(
    bootstrap_servers=[BOOTSTRAP],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def send_message(dedup_key, label):
    """Send a single INSERT message with MaTram = dedup_key"""
    msg = {
        "data": [{"MaTram": dedup_key, "TenTram": label}],
        "key": "MaTram",
        "type": "INSERT",
        "version": 1,
        "ngay_cap_nhat": "19/05/2026-15:00",
        "length": 1
    }
    producer.send(TOPIC, value=msg)
    producer.flush()
    print(f"    [SENT] {dedup_key} | {label}")

def query_trino(sql):
    """Run SQL via trino CLI, return stdout"""
    result = subprocess.run(
        ['docker', 'exec', TRINO_CONTAINER, 'trino', '--execute', sql],
        capture_output=True, text=True, timeout=30
    )
    return result.stdout.strip()

def check_row_exists(dedup_key):
    """Check if dedup_key exists in iceberg.def.abc"""
    sql = f"SELECT dedup_key FROM iceberg.def.abc WHERE dedup_key = '{dedup_key}'"
    output = query_trino(sql)
    return dedup_key in output

def get_connector_status():
    """Get connector status via REST API"""
    try:
        result = subprocess.run(
            ['powershell', '-Command',
             f'(Invoke-WebRequest -Uri "{CONNECT_URL}/connectors/sink-qtmt-tramquantrac/status" -UseBasicParsing -TimeoutSec 5).Content'],
            capture_output=True, text=True, timeout=15
        )
        if result.returncode == 0 and result.stdout.strip():
            data = json.loads(result.stdout.strip())
            return data.get('connector', {}).get('state', 'UNKNOWN')
    except:
        pass
    return 'UNREACHABLE'

def docker_compose_cmd(cmd):
    """Run docker compose command"""
    result = subprocess.run(
        ['docker', 'compose'] + cmd,
        capture_output=True, text=True, timeout=30
    )
    return result.returncode

def wait_connect_ready(timeout_sec):
    """Wait until Connect REST API is reachable and connector RUNNING"""
    print(f"    Waiting up to {timeout_sec}s for Connect to be ready...")
    start = time.time()
    while time.time() - start < timeout_sec:
        status = get_connector_status()
        if status == 'RUNNING':
            print(f"    Connect ready! Connector state: {status}")
            return True
        time.sleep(5)
    print(f"    TIMEOUT after {timeout_sec}s. Last status: {status}")
    return False

# ============================================================
# TEST
# ============================================================
def main():
    print("=" * 60)
    print("TEST CHECKPOINT RECOVERY")
    print("=" * 60)

    # --- Pre-check ---
    print("\n[PRE-CHECK] Connector status...")
    status = get_connector_status()
    print(f"    Status: {status}")
    if status != 'RUNNING':
        print("    ERROR: Connector not RUNNING. Start it first.")
        sys.exit(1)

    # --- STEP 1: Send message BEFORE kill ---
    print("\n" + "=" * 60)
    print("[STEP 1] Send message BEFORE kill")
    print("=" * 60)
    send_message("CHK_BEFORE_KILL", "Message sent before killing connector")

    print(f"    Waiting {WAIT_COMMIT}s for Iceberg commit...")
    time.sleep(WAIT_COMMIT)

    exists = check_row_exists("CHK_BEFORE_KILL")
    if exists:
        print("    ✅ CHK_BEFORE_KILL committed to Iceberg BEFORE kill")
    else:
        print("    ❌ CHK_BEFORE_KILL NOT found! Commit may need more time.")
        print("    Waiting 15s more...")
        time.sleep(15)
        exists = check_row_exists("CHK_BEFORE_KILL")
        if exists:
            print("    ✅ CHK_BEFORE_KILL committed (after extra wait)")
        else:
            print("    ❌ FAIL: Message not committed. Aborting.")
            sys.exit(1)

    # --- STEP 2: Kill connector ---
    print("\n" + "=" * 60)
    print("[STEP 2] KILL connector (simulate crash)")
    print("=" * 60)
    docker_compose_cmd(['kill', 'connect'])
    print("    Connect container KILLED")
    time.sleep(3)

    status = get_connector_status()
    print(f"    Verify unreachable: {status}")
    assert status == 'UNREACHABLE', f"Expected UNREACHABLE, got {status}"
    print("    ✅ Connector confirmed DOWN")

    # --- STEP 3: Send message DURING down ---
    print("\n" + "=" * 60)
    print("[STEP 3] Send message WHILE connector is DOWN")
    print("=" * 60)
    send_message("CHK_DURING_DOWN", "Message sent while connector is dead")
    print("    Message is in Kafka topic, waiting for connector to come back...")

    # --- STEP 4: Restart connector ---
    print("\n" + "=" * 60)
    print("[STEP 4] RESTART connector")
    print("=" * 60)
    docker_compose_cmd(['up', '-d', 'connect'])
    print("    Container starting...")

    ready = wait_connect_ready(WAIT_STARTUP)
    if not ready:
        print("    ❌ FAIL: Connect did not come back in time.")
        sys.exit(1)

    # --- STEP 5: Verify DURING_DOWN message picked up ---
    print("\n" + "=" * 60)
    print("[STEP 5] Verify message sent DURING DOWN is now in Iceberg")
    print("=" * 60)
    print(f"    Waiting {WAIT_COMMIT}s for commit after restart...")
    time.sleep(WAIT_COMMIT)

    exists = check_row_exists("CHK_DURING_DOWN")
    if exists:
        print("    ✅ CHK_DURING_DOWN picked up after restart!")
    else:
        print("    Waiting 15s more...")
        time.sleep(15)
        exists = check_row_exists("CHK_DURING_DOWN")
        if exists:
            print("    ✅ CHK_DURING_DOWN picked up (after extra wait)")
        else:
            print("    ❌ FAIL: Message sent during down NOT processed!")
            sys.exit(1)

    # --- STEP 6: Send message AFTER recovery ---
    print("\n" + "=" * 60)
    print("[STEP 6] Send message AFTER recovery")
    print("=" * 60)
    send_message("CHK_AFTER_RECOVERY", "Message sent after recovery")

    print(f"    Waiting {WAIT_COMMIT}s for commit...")
    time.sleep(WAIT_COMMIT)

    exists = check_row_exists("CHK_AFTER_RECOVERY")
    if exists:
        print("    ✅ CHK_AFTER_RECOVERY processed normally!")
    else:
        print("    Waiting 15s more...")
        time.sleep(15)
        exists = check_row_exists("CHK_AFTER_RECOVERY")
        if exists:
            print("    ✅ CHK_AFTER_RECOVERY processed (after extra wait)")
        else:
            print("    ❌ FAIL: Message after recovery NOT processed!")
            sys.exit(1)

    # --- SUMMARY ---
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print("    ✅ CHK_BEFORE_KILL  → Committed before crash")
    print("    ✅ CHK_DURING_DOWN  → Picked up after restart (no data loss)")
    print("    ✅ CHK_AFTER_RECOVERY → Normal operation resumed")
    print("\n    CHECKPOINT RECOVERY: ALL PASSED ✅")
    print("=" * 60)

    producer.close()

if __name__ == '__main__':
    main()
