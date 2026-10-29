
$ErrorActionPreference = "Stop"

$project = Split-Path -Parent $PSScriptRoot
$secretDir = Join-Path $env:LOCALAPPDATA "EcommerceBatch"

function Read-Secret($name) {
    $path = Join-Path $secretDir "$name.enc"

    $secure = Get-Content $path |
        ConvertTo-SecureString

    $ptr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure)

    try {
        return [Runtime.InteropServices.Marshal]::PtrToStringBSTR($ptr)
    }
    finally {
        [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($ptr)
    }
}

$env:MYSQL_PASSWORD = Read-Secret "mysql"
$env:MINIO_ROOT_USER = "batchadmin"
$env:MINIO_ROOT_PASSWORD = Read-Secret "minio"
$env:BATCH_BRIDGE_TOKEN = Read-Secret "bridge"

# Allow Docker Desktop to initialize after Windows login
Start-Sleep -Seconds 30

Set-Location $project

$python = (Get-Command python.exe -ErrorAction Stop).Source

& $python ".\scripts\airflow_bridge.py"

if ($LASTEXITCODE -ne 0) {
    throw "Bridge exited with code $LASTEXITCODE"
}
