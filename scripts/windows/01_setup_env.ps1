# 01_setup_env.ps1
# Creates .env from .env_example, generates FERNET_KEY, asks for STEAM_API_KEY,
# and creates the data/ and logs/ folders. Secret values are never printed.
$ErrorActionPreference = 'Stop'
Set-Location (Resolve-Path (Join-Path $PSScriptRoot '..\..'))

if (-not (Test-Path -LiteralPath '.env')) {
    Copy-Item -LiteralPath '.env_example' -Destination '.env'
    Write-Host 'Created .env from .env_example'
}

foreach ($dir in 'data', 'logs') {
    New-Item -ItemType Directory -Force -Path $dir | Out-Null
}

# .NET file APIs use the process directory, not the PowerShell location, so use full paths
$envPath = Join-Path (Get-Location) '.env'
$utf8NoBom = New-Object System.Text.UTF8Encoding $false
$script:lines = New-Object System.Collections.Generic.List[string]
$script:lines.AddRange([string[]][System.IO.File]::ReadAllLines($envPath, $utf8NoBom))

function Get-EnvValue([string]$key) {
    foreach ($line in $script:lines) {
        if ($line -match "^$key=") { return $line.Substring($key.Length + 1).Trim() }
    }
    return ''
}

function Set-EnvValue([string]$key, [string]$value) {
    $updated = New-Object System.Collections.Generic.List[string]
    $found = $false
    foreach ($line in $script:lines) {
        if ($line -match "^$key=") {
            $updated.Add("$key=$value")
            $found = $true
        } else {
            $updated.Add($line)
        }
    }
    if (-not $found) { $updated.Add("$key=$value") }
    $script:lines = $updated
}

# 1. FERNET_KEY: encryption key for Airflow connections. Generated inside the official Airflow image.
if (-not (Get-EnvValue 'FERNET_KEY')) {
    Write-Host 'Generating FERNET_KEY (the first run downloads the Airflow image, this can take a while)...'
    $fernet = docker run --rm apache/airflow:3.3.2 python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
    if ($LASTEXITCODE -ne 0 -or -not $fernet) {
        Write-Host 'Failed to generate FERNET_KEY. Is Docker Desktop running?'
        exit 1
    }
    Set-EnvValue 'FERNET_KEY' (($fernet | Select-Object -Last 1).ToString().Trim())
    Write-Host 'FERNET_KEY written to .env'
} else {
    Write-Host 'FERNET_KEY already set, keeping it'
}

# 2. STEAM_API_KEY: needed only for steam_games_catalog_etl. Input is hidden.
if (-not (Get-EnvValue 'STEAM_API_KEY')) {
    Write-Host 'STEAM_API_KEY is empty. Get one at https://steamcommunity.com/dev/apikey'
    $secure = Read-Host 'Paste STEAM_API_KEY (press Enter to skip, catalog DAG will then fail)' -AsSecureString
    $plain = [System.Runtime.InteropServices.Marshal]::PtrToStringAuto(
        [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure))
    if ($plain) {
        Set-EnvValue 'STEAM_API_KEY' $plain.Trim()
        Write-Host 'STEAM_API_KEY saved to .env (value not shown)'
    } else {
        Write-Host 'Skipped STEAM_API_KEY. steam_games_catalog_etl will not work until it is set.'
    }
} else {
    Write-Host 'STEAM_API_KEY already set (value not shown)'
}

# 3. AIRFLOW_UID stays empty on Windows.
[System.IO.File]::WriteAllLines($envPath, [string[]]$script:lines, $utf8NoBom)
Write-Host "`n.env is ready. Next: 02_start_stack.ps1"
exit 0
