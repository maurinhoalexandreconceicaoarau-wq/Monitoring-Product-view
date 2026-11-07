$ErrorActionPreference = 'Stop'
$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
Set-Location $projectRoot
docker compose stop
if ($LASTEXITCODE -ne 0) { throw 'docker compose stop failed; check Docker Desktop status.' }
Write-Host 'All Compose services stopped. Containers are preserved and can be started again from Docker Desktop.'
