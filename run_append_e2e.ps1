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
$TABLE   = "iceberg.def.abc_append_v2"
$TRINO   = "iceberg-kafka-connect-demo-trino-1"

function Step($m) { Write-Host "`n=== $m ===" -ForegroundColor Cyan }

# PowerShell 5 turns any native stderr line into an ErrorRecord when
# ErrorActionPreference=Stop. Trino emits a harmless JLine warning on stderr,
# so only the native exit code should decide success/failure.
function Invoke-DockerCommand([string[]]$DockerArgs) {
    $previousPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        $output = & docker @DockerArgs 2>$null
        $exitCode = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $previousPreference
    }
    if ($exitCode -ne 0) {
        throw "Docker command failed (exit=$exitCode): docker $($DockerArgs -join ' ')"
    }
    return $output
}

function Invoke-Trino([string]$Sql, [string]$OutputFormat = "") {
    $arguments = @("exec", $TRINO, "trino")
    if ($OutputFormat) {
        $arguments += @("--output-format", $OutputFormat)
    }
    $arguments += @("--execute", $Sql)
    return Invoke-DockerCommand -DockerArgs $arguments
}

# 1) BUILD JAR (compile trong container co san Kafka Connect libs)
Step "1/6 Build JAR SMT"
docker run --rm -v "${PWD}:/work" $IMAGE bash /work/build-in-container.sh
if ($LASTEXITCODE -ne 0) { throw "Build JAR that bai" }

# 2) BUILD + START full demo stack (nap JAR moi tu plugins/)
Step "2/6 Build + start full demo stack"
docker compose up -d --build
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

# 4) Tao table append API-push (9 cot) + (re)deploy connector
Step "4/6 Tao bang 9 cot + deploy connector"
Invoke-Trino "CREATE SCHEMA IF NOT EXISTS iceberg.def" | Out-Null
Invoke-Trino "CREATE TABLE IF NOT EXISTS $TABLE (loainguon VARCHAR, manguondulieu VARCHAR, sukien VARCHAR, phienban INTEGER, body VARCHAR, header VARCHAR, data VARCHAR, ingest_date DATE, ingest_time TIMESTAMP(3)) WITH (format = 'PARQUET')" | Out-Null
$beforeRows = [long](Invoke-Trino "SELECT count(*) FROM $TABLE" "TSV" | Select-Object -First 1)
Write-Host "  Rows before test=$beforeRows"

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

# 6) Verify: 9 cot + so snapshot (>1) + lineage
Step "6/6 Verify ket qua"
$afterRows = [long](Invoke-Trino "SELECT count(*) FROM $TABLE" "TSV" | Select-Object -First 1)
$addedRows = $afterRows - $beforeRows
Write-Host "--- ROW DELTA (ky vong +9) ---"
Write-Host "  before=$beforeRows after=$afterRows added=$addedRows"
if ($addedRows -ne 9) { throw "Sai row delta: ky vong 9, thuc te $addedRows" }

$leakedHeaders = [long](Invoke-Trino "SELECT count(*) FROM $TABLE WHERE lower(coalesce(header,'')) LIKE '%authorization%' OR lower(coalesce(header,'')) LIKE '%must-not-land%' OR lower(coalesce(header,'')) LIKE '%cookie%'" "TSV" | Select-Object -First 1)
Write-Host "--- SENSITIVE HEADER LEAK (ky vong 0) ---"
Write-Host "  leaked=$leakedHeaders"
if ($leakedHeaders -ne 0) { throw "Phat hien header nhay cam trong bang" }

Write-Host "--- SAMPLE APPEND ROWS ---"
Invoke-Trino "SELECT loainguon, manguondulieu, phienban, body, header, length(data) AS data_len, ingest_date, ingest_time FROM $TABLE ORDER BY ingest_time DESC LIMIT 9"
Write-Host "--- SCHEMA (phai dung 9 cot) ---"
Invoke-Trino "DESCRIBE $TABLE"
Write-Host "--- APPEND ROWS + SNAPSHOT LINEAGE ---"
Invoke-DockerCommand -DockerArgs @("cp", "query-append.sql", "${TRINO}:/tmp/query-append.sql") | Out-Null
Invoke-DockerCommand -DockerArgs @("exec", $TRINO, "trino", "-f", "/tmp/query-append.sql")

Write-Host "`n=== DONE END-TO-END ===" -ForegroundColor Green
