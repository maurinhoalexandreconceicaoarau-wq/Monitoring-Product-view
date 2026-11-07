$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
Set-Location $projectRoot

Write-Host 'Docker Compose services:'
docker compose ps
if ($LASTEXITCODE -ne 0) {
    Write-Warning 'Docker status could not be read. Check that Docker Desktop is running.'
}

Write-Host "`nDashboard listener on port 8501:"
$listener = Get-NetTCPConnection -State Listen -LocalPort 8501 -ErrorAction SilentlyContinue
if ($listener) { $listener | Select-Object LocalAddress,LocalPort,OwningProcess }
else { Write-Host 'No listener found.' }

Write-Host "`nLatest local component state (may be historical; confirm heartbeat freshness in dashboard):"
$artifactsPath = Join-Path $projectRoot 'artifacts'
foreach ($name in 'replay.json','spark.json','spark-heartbeat.json','metrics.json') {
    $path = Join-Path $artifactsPath $name
    if (Test-Path -LiteralPath $path) {
        Write-Host "--- $name"
        Get-Content -LiteralPath $path -Raw
    }
}
