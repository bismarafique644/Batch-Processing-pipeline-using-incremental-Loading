$ErrorActionPreference = "Stop"

$mainTask = "Ecommerce Power BI Auto Refresh"

# Find running Power BI watcher Python process
$processes = @(
    Get-CimInstance Win32_Process |
    Where-Object {
        $_.Name -match "^python(w)?\.exe$" -and
        $_.CommandLine -match "auto_refresh_powerbi\.py" -and
        $_.CommandLine -match "\-\-watch"
    }
)

if ($processes.Count -eq 0) {
    Start-ScheduledTask -TaskName $mainTask
}
