# Windows: starts the 3 grid nodes of storage "grid-1" (config\galaxy.yaml) in the background.
#   powershell -ExecutionPolicy Bypass -File scripts\start-grid.ps1 [-Heap 1500m]
#   Stop with scripts\stop-local.ps1 (stops everything with a pid file).
param([string]$Heap = "1500m", [string]$Grid = "grid-1", [string[]]$Nodes = @("node-1", "node-2", "node-3"))
$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
$Config = Join-Path $Root "config\galaxy.yaml"
$Run = Join-Path $Root "galaxy-data\run"
$Jar = Join-Path $Root "galaxy-grid-node\target\galaxy-grid-node.jar"
New-Item -ItemType Directory -Force -Path $Run | Out-Null
$Started = @()
foreach ($n in $Nodes) {
    $p = Start-Process -FilePath "java" -ArgumentList @("-Xmx$Heap", "-XX:+ExitOnOutOfMemoryError", '"-Djava.util.logging.SimpleFormatter.format=%1$tFT%1$tT.%1$tL %4$s %3$s %5$s%6$s%n"', "-jar", $Jar, "--config", $Config, "--grid", $Grid, "--node", $n) `
        -PassThru -WindowStyle Hidden -RedirectStandardOutput (Join-Path $Run "$n.log") -RedirectStandardError (Join-Path $Run "$n.err.log")
    $Started += $p
    Set-Content -Path (Join-Path $Run "$n.pid") -Value $p.Id
    Write-Host "$n started (pid $($p.Id))"
}
# /health answers 200 only when the node is READY (every node up with the same configuration)
foreach ($port in 7201, 7202, 7203) {
    $ready = $false
    for ($i = 0; $i -lt 600 -and -not $ready; $i++) {
        if ($Started | Where-Object { $_.HasExited }) { throw "a grid node exited during startup; see galaxy-data\run\*.err.log" }
        try { Invoke-WebRequest -UseBasicParsing "http://localhost:$port/health" | Out-Null; $ready = $true } catch { Start-Sleep -Milliseconds 500 }
    }
    if (-not $ready) { throw "node on admin port $port did not become READY; see galaxy-data\run\*.err.log" }
}
Write-Host "grid READY. Stats: http://localhost:7201/v1/stats   Metrics: http://localhost:7201/metrics"