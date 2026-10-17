# 09_run_benchmark_full.ps1
# Full benchmark: sizes 10k / 50k / 100k, 3 runs each. Takes a long time and loads the machine heavily.
# Close other heavy programs before running.
$ErrorActionPreference = 'Stop'
Set-Location (Resolve-Path (Join-Path $PSScriptRoot '..\..'))

$conf = '{"sizes": [10000, 50000, 100000], "runs": 3, "batch_size": 5000}'
Write-Host 'Running full benchmark (sizes 10k/50k/100k, 3 runs). Expect 1 hour or more.'
$date = Get-Date -Format 'yyyy-MM-dd'
docker compose run --rm airflow-cli airflow dags test steam_storage_benchmark $date --conf $conf
if ($LASTEXITCODE -ne 0) { Write-Host 'Benchmark failed. See the errors above.'; exit 1 }

Write-Host "`nResults (medians): data\processed\steam_benchmark\$date\benchmark_summary.csv"
Write-Host "All runs:          data\processed\steam_benchmark\$date\benchmark_runs.csv"
exit 0
