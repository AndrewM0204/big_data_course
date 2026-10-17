# 07_run_reviews.ps1
# Runs steam_reviews_collector once. Requires steam.ods_games to be filled (step 04).
# The DAG waits for the catalog DAG on the same logical date, so a date with a finished catalog run is needed.
# Usage: .\07_run_reviews.ps1 [-Date 2026-10-17] [-TopN 100] [-MaxPages 3]
param(
    [string]$Date = (Get-Date -Format 'yyyy-MM-dd'),
    [int]$TopN = 100,
    [int]$MaxPages = 3
)
$ErrorActionPreference = 'Stop'
Set-Location (Resolve-Path (Join-Path $PSScriptRoot '..\..'))

$conf = "{`"top_n`": $TopN, `"max_pages_per_game`": $MaxPages}"
Write-Host "Running steam_reviews_collector for $Date (top=$TopN, pages=$MaxPages)..."
Write-Host 'Expected duration: roughly 10-30 minutes (1.5 s per request, plus retries).'
docker compose run --rm airflow-cli airflow dags test steam_reviews_collector $Date --conf $conf
if ($LASTEXITCODE -ne 0) { Write-Host 'Run failed. See the errors above.'; exit 1 }

Write-Host "`nCheck Mongo: .\05_show_results.ps1"
exit 0
