# 03_check_connections.ps1
# Verifies that the Airflow connections and the databases they point to actually work.
# Connections are defined in docker-compose.yaml (AIRFLOW_CONN_*), so nothing is created in the UI.
$ErrorActionPreference = 'Continue'
Set-Location (Resolve-Path (Join-Path $PSScriptRoot '..\..'))
$allOk = $true

function Test-Step([string]$label, [scriptblock]$action) {
    & $action *> $null
    if ($LASTEXITCODE -eq 0) {
        Write-Host "[OK]   $label"
    } else {
        Write-Host "[FAIL] $label"
        $script:allOk = $false
    }
}

Test-Step 'Postgres reachable (steam_postgres)' {
    docker compose exec -T postgres psql -U airflow -d airflow -c 'SELECT 1;'
}

Test-Step 'ODS schema steam exists' {
    docker compose exec -T postgres psql -U airflow -d airflow -tAc "SELECT 1 FROM information_schema.schemata WHERE schema_name='steam';" | Select-String -Pattern '^1$' -Quiet
}

Test-Step 'ClickHouse reachable (clickhouse_http)' {
    docker compose exec -T clickhouse clickhouse-client --user airflow --password airflow --database steam --query 'SELECT 1'
}

Test-Step 'ClickHouse table steam.player_stats exists' {
    docker compose exec -T clickhouse clickhouse-client --user airflow --password airflow --database steam --query "EXISTS TABLE steam.player_stats"
}

Test-Step 'Mongo reachable (steam_mongo)' {
    docker compose exec -T mongo mongo --quiet -u root -p example --authenticationDatabase admin --eval 'db.runCommand({ping:1}).ok'
}

if ($allOk) {
    Write-Host "`nAll connections work. Next: 04_run_catalog.ps1"
    exit 0
}
Write-Host "`nSome checks failed. Run: docker compose logs --tail 50 <service>"
exit 1
