# 05_show_results.ps1
# Prints the state of the ODS layer and the analytics table: row counts, sample rows,
# and the number of reviews in MongoDB.
$ErrorActionPreference = 'Continue'
Set-Location (Resolve-Path (Join-Path $PSScriptRoot '..\..'))

Write-Host '=== PostgreSQL: catalog statistics (steam.v_catalog_stats) ==='
docker compose exec -T postgres psql -U airflow -d airflow -c 'SELECT * FROM steam.v_catalog_stats;'

Write-Host '=== PostgreSQL: failed appids ==='
docker compose exec -T postgres psql -U airflow -d airflow -c 'SELECT COUNT(*) AS failed_total FROM steam.failed_appids;'

Write-Host '=== ClickHouse: player statistics rows ==='
docker compose exec -T clickhouse clickhouse-client --user airflow --password airflow --database steam --query 'SELECT count() AS rows FROM steam.player_stats'

Write-Host '=== MongoDB: reviews in steam.ods_reviews ==='
docker compose exec -T mongo mongo --quiet -u root -p example --authenticationDatabase admin --eval "db.getSiblingDB('steam').ods_reviews.count()"

exit 0
