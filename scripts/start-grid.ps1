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
foreach ($n in $Nodes) {
    $p = Start-Process -FilePath "java" -ArgumentList @("-Xmx$Heap", "-jar", $Jar, "--config", $Config, "--grid", $Grid, "--node", $n) `
        -PassThru -WindowStyle Hidden -RedirectStandardOutput (Join-Path $Run "$n.log") -RedirectStandardError (Join-Path $Run "$n.err.log")
    Set-Content -Path (Join-Path $Run "$n.pid") -Value $p.Id
    Write-Host "$n started (pid $($p.Id))"
}
foreach ($port in 7201, 7202, 7203) {
    for ($i = 0; $i -lt 40; $i++) {
        try { Invoke-WebRequest -UseBasicParsing "http://localhost:$port/health" | Out-Null; break } catch { Start-Sleep -Milliseconds 500 }
    }
}
Write-Host "grid stats: http://localhost:7201/v1/stats"
