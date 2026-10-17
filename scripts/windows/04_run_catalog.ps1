# 04_run_catalog.ps1
# Runs steam_games_catalog_etl once for a given date and prints the result.
# Usage: .\04_run_catalog.ps1 [-Date 2026-10-17] [-BatchSize 100] [-MaxGames 10000]
param(
    [string]$Date = (Get-Date -Format 'yyyy-MM-dd'),
    [int]$BatchSize = 100,
    [int]$MaxGames = 10000
)
$ErrorActionPreference = 'Stop'
Set-Location (Resolve-Path (Join-Path $PSScriptRoot '..\..'))

Write-Host "Running steam_games_catalog_etl for $Date (batch=$BatchSize, max=$MaxGames)..."
Write-Host 'This takes a while: the DAG waits about 1.2 s per game because of Steam rate limits.'

$conf = "{`"batch_size`": $BatchSize, `"max_games_total`": $MaxGames}"
docker compose run --rm airflow-cli airflow dags test steam_games_catalog_etl $Date --conf $conf
if ($LASTEXITCODE -ne 0) {
    Write-Host "`nThe run failed. Typical causes:"
    Write-Host '  - STEAM_API_KEY is empty in .env (then run 01_setup_env.ps1 and restart the stack)'
    Write-Host '  - Steam API is temporarily unavailable (retry later)'
    exit 1
}

Write-Host "`nDone. Check the result: .\05_show_results.ps1"
exit 0
