# 08_run_benchmark_smoke.ps1
# Quick check of the benchmark DAG on a small dataset (1000 rows, 1 run). Run this before the full benchmark.
$ErrorActionPreference = 'Stop'
Set-Location (Resolve-Path (Join-Path $PSScriptRoot '..\..'))

$conf = '{"sizes": [1000], "runs": 1, "batch_size": 500}'
Write-Host 'Running steam_storage_benchmark in smoke mode (sizes=[1000], runs=1)...'
$date = Get-Date -Format 'yyyy-MM-dd'
docker compose run --rm airflow-cli airflow dags test steam_storage_benchmark $date --conf $conf
if ($LASTEXITCODE -ne 0) { Write-Host 'Smoke run failed. See the errors above.'; exit 1 }

Write-Host "`nIf this passed, run 09_run_benchmark_full.ps1."
exit 0
