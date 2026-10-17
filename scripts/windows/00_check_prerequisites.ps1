# 00_check_prerequisites.ps1
# Checks Docker, docker compose, RAM, free disk space and busy ports.
Set-Location (Resolve-Path (Join-Path $PSScriptRoot '..\..'))
$script:failed = 0

function Report-Check([bool]$ok, [string]$label, [string]$hint) {
    if ($ok) {
        Write-Host "[OK]   $label"
    } else {
        Write-Host "[FAIL] $label -- $hint"
        $script:failed++
    }
}

$dockerCmd = Get-Command docker -ErrorAction SilentlyContinue
Report-Check ($null -ne $dockerCmd) 'Docker CLI found' 'Install Docker Desktop: https://www.docker.com/products/docker-desktop/'

if ($dockerCmd) {
    docker info > $null 2>&1
    Report-Check ($LASTEXITCODE -eq 0) 'Docker engine is running' "Start Docker Desktop and wait until it says 'Engine running'"

    docker compose version > $null 2>&1
    Report-Check ($LASTEXITCODE -eq 0) 'docker compose (v2) available' 'Update Docker Desktop (Compose v2 is bundled)'
}

$ramGb = [math]::Round((Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory / 1GB, 1)
if ($ramGb -ge 8) { Write-Host "[OK]   RAM: $ramGb GB" } else { Write-Host "[WARN] RAM: $ramGb GB (8 GB recommended, Docker needs at least 6 GB)" }

$driveName = (Split-Path -Qualifier (Get-Location)).TrimEnd(':')
$freeGb = [math]::Round((Get-PSDrive -Name $driveName).Free / 1GB, 1)
if ($freeGb -ge 20) { Write-Host "[OK]   Free disk space on ${driveName}: $freeGb GB" } else { Write-Host "[WARN] Free disk space on ${driveName}: $freeGb GB (20 GB recommended)" }

foreach ($port in 8080, 8123, 9005, 27017) {
    $busy = Get-NetTCPConnection -State Listen -LocalPort $port -ErrorAction SilentlyContinue
    if ($busy) { Write-Host "[WARN] Port $port is already in use (fine only if it belongs to this stack)" } else { Write-Host "[OK]   Port $port is free" }
}

if ($script:failed -gt 0) {
    Write-Host "`n$($script:failed) required check(s) failed. Fix them and run this script again."
    exit 1
}
Write-Host "`nAll required checks passed. Next: 01_setup_env.ps1"
exit 0
