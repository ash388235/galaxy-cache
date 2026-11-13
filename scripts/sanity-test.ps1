# Windows: basic sanity test for a new setup. Builds, then runs the same ProviderDemo on all three engines and checks
# that each one stores, queries and drops data with identical results.
#   1. direct : file storage + 2 index pods      (config\galaxy.yaml, namespace trading)
#   2. grid   : 3-node galaxy grid               (config\galaxy.yaml, namespace grid)
#   3. ignite : 3-node Apache Ignite cluster     (config\galaxy-ignite.yaml)
#   powershell -ExecutionPolicy Bypass -File scripts\sanity-test.ps1 [-SkipBuild] [-WithTests] [-Only direct|grid|ignite]
# Everything it starts is stopped again. Takes about 3 minutes after the build. Needs about 4 GB of free memory.
param([switch]$SkipBuild, [switch]$WithTests, [string]$Only = "")
$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
$Run = Join-Path $Root "galaxy-data\run"
$Jar = Join-Path $Root "galaxy-examples\target\galaxy-examples.jar"
$Config = Join-Path $Root "config\galaxy.yaml"
$IgniteConfig = Join-Path $Root "config\galaxy-ignite.yaml"
$IgniteOpts = Join-Path $Root "config\ignite-jvm.options"
$Expect = "count(symbol = MSFT) = 507"   # 5,000 trades from a fixed seed: identical on every engine
New-Item -ItemType Directory -Force -Path $Run | Out-Null
Set-Location $Root
$Result = [ordered]@{}

if (-not $SkipBuild) {
    Write-Host "== build"
    if ($WithTests) { mvn -B -q verify } else { mvn -B -q package -DskipTests }
    if ($LASTEXITCODE -ne 0) { throw "Maven build failed" }
}
if (-not (Test-Path $Jar)) { throw "Missing $Jar - build first" }

function Invoke-Demo($name, [string]$jvm, [string[]]$demoArgs) {
    $log = Join-Path $Run "sanity-$name.log"
    $ErrorActionPreference = "Continue"
    $javaArgs = @("-Xmx768m")
    if ($jvm) { $javaArgs += $jvm }
    $javaArgs += @("-cp", $Jar, "io.galaxycache.examples.ProviderDemo", "--records", "5000") + $demoArgs
    & java @javaArgs *> $log
    $rc = $LASTEXITCODE
    $ErrorActionPreference = "Stop"
    Get-Content $log | Where-Object { $_ -match '^(   |==)' } | ForEach-Object { "  " + $_ }
    return ($rc -eq 0) -and (Select-String -Path $log -SimpleMatch $Expect -Quiet) -and (Select-String -Path $log -Pattern '^   dropped' -Quiet)
}

function Wait-Http($url) {
    for ($i = 0; $i -lt 60; $i++) {
        try { Invoke-WebRequest -UseBasicParsing -TimeoutSec 2 $url | Out-Null; return $true } catch { Start-Sleep -Milliseconds 500 }
    }
    return $false
}

if (-not $Only -or $Only -eq "direct") {
    Write-Host "== 1. direct: file storage + index pods"
    $IndexJar = Join-Path $Root "galaxy-index-server\target\galaxy-index-server.jar"
    $pods = @()
    foreach ($n in "idx-a", "idx-b") {
        $pods += Start-Process -FilePath "java" -ArgumentList @("-jar", $IndexJar, "--config", $Config, "--node", $n) -PassThru -WindowStyle Hidden `
            -RedirectStandardOutput (Join-Path $Run "$n.log") -RedirectStandardError (Join-Path $Run "$n.err.log")
    }
    try {
        $ok = (Wait-Http "http://localhost:7070/health") -and (Wait-Http "http://localhost:7071/health")
        if ($ok) { $ok = Invoke-Demo "direct" "" @("--config", $Config, "--namespace", "trading") }
        $Result["direct"] = if ($ok) { "PASS" } else { "FAIL" }
    } finally { $pods | ForEach-Object { Stop-Process -Id $_.Id -Force -ErrorAction SilentlyContinue } }
}

if (-not $Only -or $Only -eq "grid") {
    Write-Host "== 2. grid: 3 galaxy grid nodes"
    try {
        & (Join-Path $PSScriptRoot "start-grid.ps1") -Heap 768m
        $ok = Invoke-Demo "grid" "" @("--config", $Config, "--namespace", "grid")
        $Result["grid"] = if ($ok) { "PASS" } else { "FAIL" }
    } catch { Write-Host $_; $Result["grid"] = "FAIL" }
    finally {
        foreach ($n in "node-1", "node-2", "node-3") {
            $pf = Join-Path $Run "$n.pid"
            if (Test-Path $pf) { Stop-Process -Id ([int](Get-Content $pf)) -Force -ErrorAction SilentlyContinue; Remove-Item $pf }
        }
    }
}

if (-not $Only -or $Only -eq "ignite") {
    Write-Host "== 3. ignite: 3 Apache Ignite nodes"
    try {
        & (Join-Path $PSScriptRoot "start-ignite.ps1") -Heap 768m
        $ok = Invoke-Demo "ignite" "@$IgniteOpts" @("--config", $IgniteConfig, "--namespace", "trading")
        $Result["ignite"] = if ($ok) { "PASS" } else { "FAIL" }
    } catch { Write-Host $_; $Result["ignite"] = "FAIL" }
    finally { & (Join-Path $PSScriptRoot "stop-ignite.ps1") | Out-Null }
}

Write-Host ""
Write-Host "== sanity test results (logs in galaxy-data\run\sanity-*.log)"
$failed = $false
foreach ($k in $Result.Keys) { Write-Host ("  {0,-7} {1}" -f $k, $Result[$k]); if ($Result[$k] -ne "PASS") { $failed = $true } }
if ($failed) { exit 1 }
