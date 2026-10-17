# 02_start_stack.ps1
# Starts all services and waits until the Airflow API server and databases are healthy.
$ErrorActionPreference = 'Stop'
Set-Location (Resolve-Path (Join-Path $PSScriptRoot '..\..'))

if (-not (Test-Path -LiteralPath '.env')) {
    Write-Host '.env not found. Run 01_setup_env.ps1 first.'
    exit 1
}

Write-Host 'Starting stack (first run downloads images, several GB, be patient)...'
docker compose up -d
if ($LASTEXITCODE -ne 0) { Write-Host 'docker compose up failed. See the messages above.'; exit 1 }

$services = 'postgres', 'redis', 'clickhouse', 'mongo', 'airflow-apiserver', 'airflow-scheduler', 'airflow-worker'
$deadline = (Get-Date).AddMinutes(10)
$pending = [System.Collections.Generic.List[string]]::new($services)

while ($pending.Count -gt 0 -and (Get-Date) -lt $deadline) {
    Start-Sleep -Seconds 10
    foreach ($svc in @($pending)) {
        $state = docker compose ps --format '{{.Service}} {{.Health}} {{.State}}' |
            Where-Object { $_ -match "^$svc " }
        # mongo has no healthcheck, so "running" is enough for it
        if ($state -and ($state -match ' healthy ' -or ($svc -eq 'mongo' -and $state -match 'running'))) {
            Write-Host "[READY] $svc"
            $pending.Remove($svc) | Out-Null
        }
    }
}

if ($pending.Count -gt 0) {
    Write-Host "Not ready after 10 minutes: $($pending -join ', ')"
    Write-Host 'Check logs: docker compose logs --tail 50 <service>'
    exit 1
}

Write-Host "`nStack is up. Airflow UI: http://localhost:8080  (login: airflow / password: airflow)"
Write-Host 'Next: 03_register_connections_check.ps1 (optional) or 04_run_catalog.ps1'
exit 0
