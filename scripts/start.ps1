$ErrorActionPreference = 'Stop'
$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
Set-Location $projectRoot

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw 'Docker CLI was not found. Start/install Docker Desktop, then retry.'
}
docker compose up -d --build
if ($LASTEXITCODE -ne 0) { throw 'docker compose up failed; inspect Docker Desktop and the Compose output.' }

Write-Host 'All services are starting in Docker. Open http://127.0.0.1:8501 after the dashboard is healthy.'
