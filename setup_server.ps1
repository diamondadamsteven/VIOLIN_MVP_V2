# setup_server.ps1 — One-time setup on Windows
# Run in an elevated PowerShell in your project root

$ErrorActionPreference = "Stop"

Write-Host "==> Checking Python venv"
if (!(Test-Path ".\venv")) {
  py -m venv venv
}
.\venv\Scripts\Activate.ps1

Write-Host "==> Installing Python deps for listener/processor"
pip install --upgrade pip
pip install fastapi uvicorn pyodbc requests numpy pretty_midi

Write-Host "==> Checking ffmpeg"
$ff = (Get-Command ffmpeg -ErrorAction SilentlyContinue)
if (-not $ff) {
  Write-Host "ffmpeg not found. Installing via winget (recommended)."
  try {
    winget install -e --id Gyan.FFmpeg
  } catch {
    Write-Warning "winget install failed. Please install ffmpeg manually and ensure it's on PATH."
  }
}

Write-Host "==> Building O&F Docker image (violin/oaf:latest)"
# Requires Docker Desktop running
docker build -t violin/oaf:latest -f Dockerfile.oaf .

Write-Host "==> Windows Firewall: enabling inbound TCP 7070 (WS listener)"
# Safe to run multiple times; rule will already exist
$ruleName = "VIOLIN_MVP_WS_7070"
$rule = Get-NetFirewallRule -DisplayName $ruleName -ErrorAction SilentlyContinue
if (-not $rule) {
  New-NetFirewallRule -DisplayName $ruleName -Direction Inbound -Action Allow -Protocol TCP -LocalPort 7070 | Out-Null
} else {
  Write-Host "Firewall rule already exists."
}

Write-Host "==> Setup complete."
Write-Host "Run start_engine.bat to launch the listener and O&F microservice."
