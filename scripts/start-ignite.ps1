# Windows: starts the Ignite server nodes of config\galaxy-ignite.yaml in the background and waits for READY.
#   powershell -ExecutionPolicy Bypass -File scripts\start-ignite.ps1 [-Heap 1g]
#   Stop with scripts\stop-ignite.ps1
param([string]$Heap = "1g", [string[]]$Nodes = @("node-1", "node-2", "node-3"),
      [string]$Config = (Join-Path (Split-Path -Parent $PSScriptRoot) "config\galaxy-ignite.yaml"))
$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
$Run = Join-Path $Root "galaxy-data\run"
$Jar = Join-Path $Root "galaxy-ignite-node\target\galaxy-ignite-node.jar"
if (-not (Test-Path $Jar)) { throw "Missing $Jar - build first (mvn -DskipTests package)" }
New-Item -ItemType Directory -Force -Path $Run | Out-Null
$Started = @()
foreach ($n in $Nodes) {
    # the jar's manifest carries the --add-opens Ignite needs on Java 21
    $p = Start-Process -FilePath "java" -ArgumentList @("-Xmx$Heap", "-XX:+ExitOnOutOfMemoryError", "-DIGNITE_QUIET=true", "-DIGNITE_UPDATE_NOTIFIER=false", '"-Djava.util.logging.SimpleFormatter.format=%1$tFT%1$tT.%1$tL %4$s %3$s %5$s%6$s%n"', "-jar", $Jar, "--config", $Config, "--node", $n) `
        -PassThru -WindowStyle Hidden -RedirectStandardOutput (Join-Path $Run "ignite-$n.log") -RedirectStandardError (Join-Path $Run "ignite-$n.err.log")
    $Started += $p
    Set-Content -Path (Join-Path $Run "ignite-$n.pid") -Value $p.Id
    Write-Host "ignite $n started (pid $($p.Id))"
}
foreach ($n in $Nodes) {
    $log = Join-Path $Run "ignite-$n.log"
    $ready = $false
    for ($i = 0; $i -lt 600 -and -not $ready; $i++) {
        if ($Started | Where-Object { $_.HasExited }) { throw "an ignite node exited during startup; see galaxy-data\run\ignite-*.err.log" }
        if ((Test-Path $log) -and (Select-String -Path $log -SimpleMatch "ignite node $n READY" -Quiet)) { $ready = $true } else { Start-Sleep -Milliseconds 500 }
    }
    if (-not $ready) { throw "ignite $n did not become READY; see galaxy-data\run\ignite-$n.err.log" }
}
Write-Host "ignite cluster READY"
