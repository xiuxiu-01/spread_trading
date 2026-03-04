param(
    [Parameter(Mandatory=$true)]
    [int]$ProcessId,
    [int]$Interval = 5,
    [string]$LogFile = "monitor.log"
)

if (-not $ProcessId) {
    Write-Error "Provide a PID using -Pid <number> or -ProcessId <number>."
    exit 1
}

# previous total CPU seconds
$prevCpu = 0.0
$processors = [int]$env:NUMBER_OF_PROCESSORS

"Monitoring PID=$ProcessId every $Interval seconds. Logging to $LogFile" | Tee-Object -FilePath $LogFile -Append

while ($true) {
    $proc = Get-Process -Id $ProcessId -ErrorAction SilentlyContinue
    if (-not $proc) {
        $msg = "$(Get-Date -Format o) - Process $ProcessId exited or not found."
        Write-Output $msg
        Add-Content -Path $LogFile -Value $msg
        break
    }

    $cpu = [double]$proc.CPU
    $cpuDelta = $cpu - $prevCpu
    $prevCpu = $cpu

    if ($Interval -gt 0) {
        $cpuPct = ($cpuDelta / $Interval) / ($processors) * 100
    } else {
        $cpuPct = 0
    }

    # Guard against spurious/invalid CPU readings (e.g., very large or NaN values)
    if ([double]::IsNaN($cpuPct) -or $cpuPct -lt -1 -or $cpuPct -gt 500) {
        $warn = "$(Get-Date -Format o) - WARN: computed CPU%='$cpuPct' out of range; treating as 0"
        Write-Output $warn
        Add-Content -Path $LogFile -Value $warn
        $cpuPct = 0
    }

    $memMB = [math]::Round($proc.WorkingSet64 / 1MB, 2)
    $threads = $proc.Threads.Count
    $handles = $proc.HandleCount
    $time = Get-Date -Format o

    $line = "$time PID=$ProcessId CPU%=$([math]::Round($cpuPct,2)) MemMB=$memMB Threads=$threads Handles=$handles"
    Write-Output $line
    Add-Content -Path $LogFile -Value $line

    Start-Sleep -Seconds $Interval
}
