# 06_run_player_stats.ps1
# Runs steam_player_stats_etl once (no Steam API key needed). Requires the catalog to be loaded first.
# Usage: .\06_run_player_stats.ps1 [-Date 2026-10-17] [-BatchSize 200]
param(
    [string]$Date = (Get-Date -Format 'yyyy-MM-dd'),
    [int]$BatchSize = 200
)
$ErrorActionPreference = 'Stop'
Set-Location (Resolve-Path (Join-Path $PSScriptRoot '..\..'))

$conf = "{`"batch_size`": $BatchSize}"
Write-Host "Running steam_player_stats_etl for $Date (batch=$BatchSize)..."
docker compose run --rm airflow-cli airflow dags test steam_player_stats_etl $Date --conf $conf
if ($LASTEXITCODE -ne 0) { Write-Host 'Run failed. See the errors above.'; exit 1 }

Write-Host "`nIf the task was SKIPPED with 'steam.ods_games is empty' -- run 04_run_catalog.ps1 first."
Write-Host 'Next: .\05_show_results.ps1'
exit 0
