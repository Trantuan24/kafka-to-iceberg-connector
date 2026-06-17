# =============================================================================
# END-TO-END APPEND MODE - chay 1 phat tu code -> build -> deploy -> test -> verify
# =============================================================================
# Dung khi BAN DA SUA LOGIC trong CustomCDCTransform.java va muon test lai tu dau.
# Neu chi sua comment/Javadoc => KHONG can chay (bytecode khong doi).
#
# Cach dung:
#   .\run_append_e2e.ps1
# =============================================================================
$ErrorActionPreference = "Stop"
$IMAGE   = "duytuan24/connector-service:1.0"   # image co san Kafka Connect libs de compile
$CONN    = "sink-qtmt-append"
$TABLE   = "iceberg.def.abc_append"
$TRINO   = "iceberg-kafka-connect-demo-trino-1"

function Step($m) { Write-Host "`n=== $m ===" -ForegroundColor Cyan }

# 1) BUILD JAR (compile trong container co san Kafka Connect libs)
Step "1/6 Build JAR SMT"
docker run --rm -v "${PWD}:/work" $IMAGE bash /work/build-in-container.sh
if ($LASTEXITCODE -ne 0) { throw "Build JAR that bai" }

# 2) REBUILD + RECREATE connect (nap JAR moi tu plugins/)
Step "2/6 Rebuild + recreate connect"
docker compose up -d --build connect
if ($LASTEXITCODE -ne 0) { throw "Rebuild connect that bai" }

# 3) Doi Connect REST san sang
Step "3/6 Doi Connect REST (max 150s)"
$ready = $false
for ($i = 0; $i -lt 30; $i++) {
    Start-Sleep -Seconds 5
    try { Invoke-RestMethod "http://localhost:8083/" -TimeoutSec 4 | Out-Null; $ready = $true; break } catch {}
    Write-Host "  ...cho ($($i*5)s)"
}
if (-not $ready) { throw "Connect khong san sang" }
Write-Host "  Connect READY" -ForegroundColor Green

# 4) Tao table append (3 cot) + (re)deploy connector
Step "4/6 Tao table + deploy connector"
docker exec $TRINO trino --execute "CREATE SCHEMA IF NOT EXISTS iceberg.def" 2>$null | Out-Null
docker exec $TRINO trino --execute "CREATE TABLE IF NOT EXISTS $TABLE (id VARCHAR, record VARCHAR, ngay_cap_nhat VARCHAR) WITH (format = 'PARQUET')" 2>$null | Out-Null

# Xoa connector cu (neu co) roi tao lai de chac chan dung config moi
try { Invoke-RestMethod -Method Delete "http://localhost:8083/connectors/$CONN" -TimeoutSec 5 | Out-Null; Start-Sleep 3 } catch {}
$body = Get-Content "configs\sink.qtmt_append.json" -Raw
Invoke-RestMethod -Method Post "http://localhost:8083/connectors" -ContentType "application/json" -Body $body | Out-Null
Start-Sleep -Seconds 8
$st = Invoke-RestMethod "http://localhost:8083/connectors/$CONN/status"
Write-Host "  Connector=$($st.connector.state) Task=$($st.tasks[0].state)" -ForegroundColor Green
if ($st.tasks[0].state -ne "RUNNING") { Write-Host $st.tasks[0].trace -ForegroundColor Red; throw "Connector khong RUNNING" }

# 5) Gui NHIEU message test qua nhieu chu ky commit (JSON - 1 topic 1 format)
Step "5/6 Gui nhieu message test (JSON, nhieu batch)"
python test_append_multi.py

# 6) Verify: 3 cot + so snapshot (>1) + lineage
Step "6/6 Verify ket qua"
Write-Host "--- SO ROW (ky vong 9) ---"
docker exec $TRINO trino --execute "SELECT count(*) FROM $TABLE" 2>$null
Write-Host "--- SCHEMA (phai dung 3 cot) ---"
docker exec $TRINO trino --execute "DESCRIBE $TABLE" 2>$null
Write-Host "--- SO SNAPSHOT / CHECKPOINT (ky vong > 1) ---"
docker exec $TRINO trino --execute "SELECT count(*) AS so_snapshot FROM iceberg.def.`"abc_append`$snapshots`"" 2>$null
Write-Host "--- CHI TIET TUNG SNAPSHOT (moi lan commit 1 dong) ---"
docker exec $TRINO trino --execute "SELECT committed_at, element_at(summary,'consumer.connectorname') AS connector, element_at(summary,'consumer.ingest.time') AS ingest_time, element_at(summary,'added-records') AS added_rows FROM iceberg.def.`"abc_append`$snapshots`" ORDER BY committed_at" 2>$null

Write-Host "`n=== DONE END-TO-END ===" -ForegroundColor Green
