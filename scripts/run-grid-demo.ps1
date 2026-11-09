# Windows: local demo on the 3-node grid. Starts the grid and dashboard if needed, then runs writer processes and one
# reader that waits until every writer has finished. Follow it at http://localhost:8080/?namespace=grid
#   powershell -ExecutionPolicy Bypass -File scripts\run-grid-demo.ps1 [-Total 100000] [-Writers 2] [-NoWait]
param([int]$Total = 100000, [int]$Writers = 2, [switch]$NoWait)
$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root
$Run = Join-Path $Root "galaxy-data\run"
$Config = "config\galaxy.yaml"
$Jar = "galaxy-examples\target\galaxy-examples.jar"
$Main = "io.galaxycache.examples.sizing.GridDemo"

function Test-Http($url) {
    try { Invoke-WebRequest -UseBasicParsing $url -TimeoutSec 3 | Out-Null; return $true } catch { return $false }
}

if (-not ((Test-Http "http://127.0.0.1:7201/health") -and (Test-Http "http://127.0.0.1:7202/health") -and (Test-Http "http://127.0.0.1:7203/health"))) {
    Write-Host "grid is not up; starting it"
    & powershell -ExecutionPolicy Bypass -File (Join-Path $PSScriptRoot "start-grid.ps1")
    if ($LASTEXITCODE -ne 0) { throw "grid did not start" }
}
if (-not (Test-Http "http://127.0.0.1:8080/")) {
    Write-Host "starting the dashboard"
    $d = Start-Process -FilePath "java" -ArgumentList @("-jar", "galaxy-dashboard\target\galaxy-dashboard.jar", "--config", $Config, "--port", "8080") `
        -PassThru -WindowStyle Hidden -RedirectStandardOutput "$Run\dashboard.log" -RedirectStandardError "$Run\dashboard.err.log"
    Set-Content "$Run\dashboard.pid" $d.Id
}

# run hidden and wait: Java logs to stderr, which PowerShell would otherwise treat as a failure
$reset = Start-Process -FilePath "java" -ArgumentList @("-cp", $Jar, $Main, "--config", $Config, "--reset") -PassThru -Wait -WindowStyle Hidden `
    -RedirectStandardOutput "$Run\demo-reset.log" -RedirectStandardError "$Run\demo-reset.err.log"
if ($reset.ExitCode -ne 0) { throw "could not clear the previous demo data; see galaxy-data\run\demo-reset.err.log" }

function Start-Client($name, $heap, $clientArgs) {
    $p = Start-Process -FilePath "java" -ArgumentList (@("-Xmx$heap", "-Dgalaxy.client.name=$name", "-cp", $Jar, $Main, "--config", $Config) + $clientArgs) `
        -PassThru -WindowStyle Hidden -RedirectStandardOutput "$Run\demo-$name.log" -RedirectStandardError "$Run\demo-$name.err.log"
    $null = $p.Handle # keeps the exit code available
    Write-Host "$name started (pid $($p.Id))"
    return $p
}

$clients = @()
$clients += Start-Client "reader" "3g" @("--read", "--writers", "$Writers", "--total", "$Total")
foreach ($w in 1..$Writers) {
    $clients += Start-Client "writer-$w" "1g" @("--write", "$w", "--writers", "$Writers", "--total", "$Total")
}
Write-Host "Dashboard: http://localhost:8080/?namespace=grid"
Write-Host "Logs: galaxy-data\run\demo-*.log"
if ($NoWait) { return }

$clients | Wait-Process
Write-Host ""
foreach ($w in 1..$Writers) { Get-Content "$Run\demo-writer-$w.log" | Select-String "DONE" | ForEach-Object { $_.Line } }
Get-Content "$Run\demo-reader.log" | Select-String "reader:" | ForEach-Object { $_.Line }
$bad = $clients | Where-Object { $_.ExitCode -ne 0 }
if ($bad) { throw "a client failed; see galaxy-data\run\demo-*.err.log" }
