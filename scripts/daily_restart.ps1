param(
    [string]$Time = "06:00",
    [string]$WorkingDir = "C:\Users\Administrator\Desktop\workspace\spread_trading",
    [string]$LogFile = "C:\Users\Administrator\Desktop\workspace\spread_trading\scripts\daily_restart.log"
)

function Log {
    param($msg)
    $t = Get-Date -Format o
    "$t $msg" | Out-File -FilePath $LogFile -Append -Encoding utf8
}

if (-not (Test-Path $WorkingDir)){
    Log "WorkingDir not found: $WorkingDir"
    throw "WorkingDir not found"
}

Log "Daily restart script started. Scheduled time: $Time"

while ($true) {
    try {
        $now = Get-Date
        $parts = $Time.Split(':')
        $hour = [int]$parts[0]
        $min = [int]$parts[1]
        $target = Get-Date -Hour $hour -Minute $min -Second 0
        if ($target -le $now) { $target = $target.AddDays(1) }
        $wait = [int](([math]::Round(($target - $now).TotalSeconds)))
        Log "Sleeping $wait seconds until $($target.ToString('s'))"
        Start-Sleep -Seconds $wait

        Log "=== Performing scheduled restart ==="

        try {
            $conn = Get-NetTCPConnection -LocalPort 8766 -ErrorAction SilentlyContinue | Select-Object -First 1
            if ($conn) {
                $oldpid = $conn.OwningProcess
                Log "Stopping existing backend PID=$oldpid"
                Stop-Process -Id $oldpid -Force -ErrorAction SilentlyContinue
                Start-Sleep -Seconds 1
            } else {
                Log "No process bound to port 8766"
            }
        } catch {
            Log "Error while stopping old process: $_"
        }

        try {
            $histDir = Join-Path $WorkingDir 'data'
            Get-ChildItem -Path $histDir -Filter 'history_*.jsonl' -File -ErrorAction SilentlyContinue | ForEach-Object {
                Remove-Item -Path $_.FullName -Force -ErrorAction SilentlyContinue
                Log "Deleted history file: $($_.FullName)"
            }
        } catch {
            Log "Error deleting history files: $_"
        }

        try {
            Log "Starting backend: python -m src.main"
            $p = Start-Process -FilePath python -ArgumentList '-m','src.main' -WorkingDirectory $WorkingDir -PassThru -WindowStyle Hidden
            Log "Started backend PID=$($p.Id)"
        } catch {
            Log "Failed to start backend: $_"
            Start-Sleep -Seconds 10
            continue
        }

        try {
            $monitorScript = Join-Path $WorkingDir 'scripts\monitor_process.ps1'
            Start-Process -FilePath powershell -ArgumentList '-NoProfile','-ExecutionPolicy','Bypass','-File',$monitorScript,'-Pid',$p.Id,'-Interval','5','-LogFile',(Join-Path $WorkingDir 'scripts\monitor.log') -WindowStyle Hidden
            Log "Launched monitor for PID=$($p.Id)"
        } catch {
            Log "Failed to start monitor: $_"
        }

        Log "Scheduled restart completed. Next run will be tomorrow at $Time"
        Start-Sleep -Seconds 5
    } catch {
        Log "Unexpected error in loop: $_"
        Start-Sleep -Seconds 60
    }
}
