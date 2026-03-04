<#
restart_backend.ps1
Usage examples:
  # Normal restart, start backend and monitor
  powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\restart_backend.ps1

  # Restart and clear history files (requires -Force to skip confirmation)
  powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\restart_backend.ps1 -ClearHistory -Force

What it does:
  - Kills processes listening on port 8766 if any
  - Kills any running `src.main` Python processes
  - (optional) Clears selected history files when -ClearHistory is passed
  - Starts a single `python -m src.main` in the repository root
  - Starts `monitor_process.ps1` attached to the new backend PID and writes monitor log
#>

param(
    [switch]$ClearHistory,
    [switch]$Force
)

Set-StrictMode -Version Latest

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Definition
$repoRoot = Resolve-Path (Join-Path $scriptDir '..')
Write-Output "Using repository root: $repoRoot"

function Kill-ByPort([int]$port) {
    Write-Output "Checking listeners on port $port..."
    try {
        $listeners = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction Stop
    } catch {
        $listeners = $null
    }
    if ($listeners) {
        $pids = $listeners | Select-Object -ExpandProperty OwningProcess -Unique
        foreach ($procId in $pids) {
            try {
                Write-Output "Stopping process PID=$procId (owns port $port)"
                Stop-Process -Id $procId -Force -ErrorAction Stop
            } catch {
                Write-Warning ("Failed to stop PID {0}: {1}" -f $procId, $_.ToString())
            }
        }
    } else {
        Write-Output "No listener found on port $port via Get-NetTCPConnection"
        # fallback to netstat parse
        $lines = netstat -ano | Select-String ":$port\s" -SimpleMatch
        foreach ($l in $lines) {
            $parts = ($l -split '\s+') | Where-Object { $_ -ne '' }
            $procId = $parts[-1]
            if ($procId -and ($procId -ne 'PID')) {
                try {
                    Write-Output "Stopping process PID=$procId (from netstat)"
                    Stop-Process -Id $procId -Force -ErrorAction Stop
                } catch {
                    Write-Warning ("Failed to stop PID {0}: {1}" -f $procId, $_.ToString())
                }
            }
        }
    }
}

function Kill-srcMainProcesses() {
    Write-Output "Stopping any running src.main Python processes..."
    $procs = Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -match 'src[\\/]main' -or $_.CommandLine -match 'python -m src.main' }
    if ($procs) {
        foreach ($p in $procs) {
            try {
                Write-Output "Stopping PID=$($p.ProcessId) - $($p.CommandLine)"
                Stop-Process -Id $p.ProcessId -Force -ErrorAction Stop
            } catch {
                Write-Warning ("Failed to stop PID {0}: {1}" -f $($p.ProcessId), $_.ToString())
            }
        }
    } else {
        Write-Output 'No src.main processes found.'
    }
}

# 1) Kill src.main processes first (more reliable than port-based kill)
Kill-srcMainProcesses
Start-Sleep -Seconds 1

# 2) Kill anything still listening on 8766
Kill-ByPort -port 8766

# 3) Wait for port to be fully released (poll up to 15 seconds)
Write-Output "Waiting for port 8766 to be released..."
$maxWait = 15
$waited = 0
while ($waited -lt $maxWait) {
    $busy = Get-NetTCPConnection -LocalPort 8766 -ErrorAction SilentlyContinue
    if (-not $busy) { break }
    Start-Sleep -Seconds 1
    $waited++
    Write-Output "  Port 8766 still in use... ($waited/$maxWait)"
}
if ($waited -ge $maxWait) {
    Write-Warning "Port 8766 still occupied after ${maxWait}s. Forcing kill again..."
    Kill-ByPort -port 8766
    Start-Sleep -Seconds 2
}
Write-Output "Port 8766 is free."

# 4) Optional: clear history files
if ($ClearHistory) {
    if (-not $Force) {
        $confirm = Read-Host "Clear history files under $repoRoot\data and daily_logs? Type YES to confirm"
        if ($confirm -ne 'YES') { Write-Output 'ClearHistory cancelled.'; $ClearHistory = $false }
    }
    if ($ClearHistory) {
        $historyFiles = Get-ChildItem -Path (Join-Path $repoRoot 'data') -Include 'history_*.jsonl','orders.jsonl' -File -Recurse -ErrorAction SilentlyContinue
        foreach ($f in $historyFiles) {
            try { Write-Output "Removing $($f.FullName)"; Remove-Item $f.FullName -Force -ErrorAction Stop } catch { Write-Warning "Failed to remove $($f.FullName): $_" }
        }
        $daily = Join-Path $repoRoot 'data\daily_logs'
        if (Test-Path $daily) {
            $dailyFiles = Get-ChildItem -Path $daily -File -ErrorAction SilentlyContinue
            foreach ($f in $dailyFiles) {
                try { Write-Output "Removing $($f.FullName)"; Remove-Item $f.FullName -Force -ErrorAction Stop } catch { Write-Warning "Failed to remove $($f.FullName): $_" }
            }
        }
    }
}

# 5) Start backend
Write-Output "Starting backend: python -m src.main (cwd=$repoRoot)"
$backend = Start-Process -FilePath python -ArgumentList '-m','src.main' -WorkingDirectory $repoRoot -PassThru -WindowStyle Hidden
$backendPid = $backend.Id
Write-Output "Started backend PID=$backendPid"

# 6) Verify backend is alive and listening (poll up to 20 seconds)
Write-Output "Verifying backend is running..."
$maxCheck = 20
$ok = $false
for ($i = 1; $i -le $maxCheck; $i++) {
    Start-Sleep -Seconds 1
    # Check process is still alive
    $proc = Get-Process -Id $backendPid -ErrorAction SilentlyContinue
    if (-not $proc) {
        Write-Error "Backend process PID=$backendPid died after ${i}s! Check data\app.log for details."
        Get-Content (Join-Path $repoRoot 'data\app.log') -Tail 5
        exit 1
    }
    # Check port is listening
    $listening = Get-NetTCPConnection -LocalPort 8766 -State Listen -ErrorAction SilentlyContinue |
        Where-Object { $_.OwningProcess -eq $backendPid }
    if ($listening) {
        Write-Output "Backend confirmed listening on port 8766 after ${i}s."
        $ok = $true
        break
    }
    Write-Output "  Waiting for port 8766... ($i/$maxCheck)"
}
if (-not $ok) {
    Write-Warning "Backend started but not yet listening on 8766 after ${maxCheck}s. It may still be initializing."
}

# 7) Start monitor attached to new backend
$monitorScript = Join-Path $scriptDir 'monitor_process.ps1'
$monitorLog = Join-Path $scriptDir "monitor_$backendPid.log"
if (Test-Path $monitorScript) {
    Write-Output "Starting monitor for PID=$backendPid -> $monitorLog"
    Start-Process -FilePath powershell -ArgumentList '-NoProfile','-ExecutionPolicy','Bypass','-File',$monitorScript,'-ProcessId',$backendPid,'-Interval','5','-LogFile',$monitorLog -WindowStyle Hidden -PassThru | Out-Null
} else {
    Write-Warning "monitor_process.ps1 not found at $monitorScript. Skipping monitor start."
}

Write-Output '--- Done ---'
Write-Output "backend_pid=$backendPid monitor_log=$monitorLog"
