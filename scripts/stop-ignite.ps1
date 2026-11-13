# Windows: stops the Ignite server nodes started by scripts\start-ignite.ps1 (all together).
#   powershell -ExecutionPolicy Bypass -File scripts\stop-ignite.ps1
param([string[]]$Nodes = @("node-1", "node-2", "node-3"))
$Run = Join-Path (Split-Path -Parent $PSScriptRoot) "galaxy-data\run"
foreach ($n in $Nodes) {
    $pidFile = Join-Path $Run "ignite-$n.pid"
    if (-not (Test-Path $pidFile)) { continue }
    $id = [int](Get-Content $pidFile)
    $proc = Get-Process -Id $id -ErrorAction SilentlyContinue
    if ($proc) { Stop-Process -Id $id -Force; Write-Host "stopped ignite $n ($id)" } else { Write-Host "ignite $n ($id) had already stopped" }
    Remove-Item $pidFile
}
