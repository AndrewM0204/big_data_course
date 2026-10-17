# 10_stop_stack.ps1
# Stops the stack. Data is kept (volumes stay). Pass -WipeData to delete ALL data (volumes) -- irreversible.
param([switch]$WipeData)
$ErrorActionPreference = 'Stop'
Set-Location (Resolve-Path (Join-Path $PSScriptRoot '..\..'))

if ($WipeData) {
    $answer = Read-Host 'This deletes ALL databases (Postgres, ClickHouse, Mongo, Airflow history). Type YES to continue'
    if ($answer -ne 'YES') { Write-Host 'Cancelled.'; exit 0 }
    docker compose down -v
} else {
    docker compose down
}
if ($LASTEXITCODE -ne 0) { exit 1 }
Write-Host 'Stack stopped.'
exit 0
