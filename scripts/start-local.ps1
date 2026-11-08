# Windows: build, start both index pods and the dashboard in the background, then run the demo client.
#   powershell -ExecutionPolicy Bypass -File scripts\start-local.ps1 [-Records 200000] [-SkipBuild]
#   Stop everything with: powershell -File scripts\stop-local.ps1
param([int]$Records = 100000, [switch]$SkipBuild)
$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
$Config = Join-Path $Root "config\galaxy.yaml"
$Run = Join-Path $Root "galaxy-data\run"
New-Item -ItemType Directory -Force -Path $Run | Out-Null
Set-Location $Root

if (-not $SkipBuild) {
    mvn -q -B install
    if ($LASTEXITCODE -ne 0) { throw "Maven build failed" }
}

function Start-Bg($name, $jar, $argsList) {
    $p = Start-Process -FilePath "java" -ArgumentList (@("-jar", $jar) + $argsList) -PassThru -WindowStyle Hidden `
        -RedirectStandardOutput (Join-Path $Run "$name.log") -RedirectStandardError (Join-Path $Run "$name.err.log")
    Set-Content -Path (Join-Path $Run "$name.pid") -Value $p.Id
    Write-Host "$name started (pid $($p.Id))"
}

$IndexJar = Join-Path $Root "galaxy-index-server\target\galaxy-index-server.jar"
Start-Bg "idx-a" $IndexJar @("--config", $Config, "--node", "idx-a")
Start-Bg "idx-b" $IndexJar @("--config", $Config, "--node", "idx-b")
Start-Bg "dashboard" (Join-Path $Root "galaxy-dashboard\target\galaxy-dashboard.jar") @("--config", $Config, "--port", "8080")

foreach ($url in "http://localhost:7070/health", "http://localhost:7071/health") {
    for ($i = 0; $i -lt 30; $i++) {
        try { Invoke-WebRequest -UseBasicParsing $url | Out-Null; break } catch { Start-Sleep -Milliseconds 500 }
    }
}

java -jar (Join-Path $Root "galaxy-examples\target\galaxy-examples.jar") --config $Config --records $Records
Write-Host ""
Write-Host "Dashboard: http://localhost:8080   (index pods on :7070 and :7071 keep running)"
