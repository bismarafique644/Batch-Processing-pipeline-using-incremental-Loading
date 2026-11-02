
$ErrorActionPreference = "Stop"

# ============================================================
# CONFIGURATION
# ============================================================

$project = Split-Path -Parent $PSScriptRoot

$secretDir = Join-Path $env:LOCALAPPDATA "EcommerceBatch"
$logDir = Join-Path $secretDir "logs"

New-Item -ItemType Directory -Force -Path $logDir |
    Out-Null

$wrapperLog = Join-Path $logDir "bridge_wrapper.log"

$pythonLog = Join-Path $logDir "bridge_stdout.log"
$pythonErrorLog = Join-Path $logDir "bridge_stderr.log"

$port = 8765
$restartDelay = 15

function Write-Log($message) {
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $line = "[$timestamp] $message"

    Add-Content -Path $wrapperLog -Value $line
    Write-Host $line
}

# ============================================================
# LOAD ENCRYPTED CREDENTIALS
# ============================================================

function Read-Secret($name) {

    $path = Join-Path $secretDir "$name.enc"

    if (-not (Test-Path -LiteralPath $path)) {
        throw "Missing encrypted secret: $name"
    }

    $encrypted = (
        Get-Content -LiteralPath $path -Raw
    ).Trim()

    if (-not $encrypted) {
        throw "Empty encrypted secret: $name"
    }

    $secure = ConvertTo-SecureString `
        -String $encrypted `
        -ErrorAction Stop

    $ptr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR(
        $secure
    )

    try {
        return [Runtime.InteropServices.Marshal]::PtrToStringBSTR(
            $ptr
        )
    }
    finally {
        [Runtime.InteropServices.Marshal]::ZeroFreeBSTR(
            $ptr
        )
    }
}

try {
    $env:MYSQL_PASSWORD = Read-Secret "mysql"
    $env:MINIO_ROOT_USER = "batchadmin"
    $env:MINIO_ROOT_PASSWORD = Read-Secret "minio"
    $env:BATCH_BRIDGE_TOKEN = Read-Secret "bridge"
    $env:PYTHONUNBUFFERED = "1"

    Write-Log "Credentials loaded."
}
catch {
    Write-Log "Credential loading failed."
    throw
}

Set-Location $project

$python = (Get-Command python.exe -ErrorAction Stop).Source
$bridgeScript = Join-Path $project "scripts\airflow_bridge.py"

Write-Log "Bridge supervisor starting."

# ============================================================
# KEEP BRIDGE ALIVE
# ============================================================

while ($true) {

    try {

        # Check if another process already owns port 8765.
        $listener = Get-NetTCPConnection `
            -LocalPort $port `
            -State Listen `
            -ErrorAction SilentlyContinue

        if ($listener) {
            Write-Log "Port $port occupied. Retrying later."
            Start-Sleep -Seconds $restartDelay
            continue
        }

        # Rotate process logs at each restart so recent
        # startup failures are easier to diagnose.
        foreach ($path in @($pythonLog, $pythonErrorLog)) {
            if (Test-Path $path) {
                $archive = "$path.$(Get-Date -Format 'yyyyMMdd_HHmmss')"
                Move-Item -LiteralPath $path -Destination $archive
            }
        }

        Write-Log "Launching Python Bridge."

        $process = Start-Process `
            -FilePath $python `
            -ArgumentList "`"$bridgeScript`"" `
            -WorkingDirectory $project `
            -RedirectStandardOutput $pythonLog `
            -RedirectStandardError $pythonErrorLog `
            -PassThru `
            -NoNewWindow

        Write-Log "Bridge PID: $($process.Id)"

        # Wait while the Python server is running.
        $process.WaitForExit()

        Write-Log "Bridge exited. Exit code: $($process.ExitCode)"

        if (Test-Path $pythonErrorLog) {
            Get-Content $pythonErrorLog -Tail 10 |
                ForEach-Object {
                    # Do not print raw Python errors to the
                    # wrapper log; inspect restricted files.
                    $null = $_
                }
        }

    }
    catch {
        Write-Log "Bridge supervisor detected a process error."
        Write-Log $_.Exception.GetType().Name
    }

    Write-Log "Restarting Bridge after $restartDelay seconds."
    Start-Sleep -Seconds $restartDelay
}
