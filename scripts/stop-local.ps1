# Windows: stops the index pods and dashboard started by start-local.ps1.
$Run = Join-Path (Split-Path -Parent $PSScriptRoot) "galaxy-data\run"
Get-ChildItem -Path $Run -Filter *.pid -ErrorAction SilentlyContinue | ForEach-Object {
    $procId = Get-Content $_.FullName
    Stop-Process -Id $procId -ErrorAction SilentlyContinue
    Write-Host "stopped $($_.BaseName) ($procId)"
    Remove-Item $_.FullName
}
