# ConcurrentHub — Quick Start Script
# Run this from the CC folder in PowerShell:
#   .\start.ps1

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $ScriptDir

Write-Host ''
Write-Host '========================================' -ForegroundColor Cyan
Write-Host '  ConcurrentHub - Starting Server' -ForegroundColor Cyan
Write-Host '========================================' -ForegroundColor Cyan
Write-Host ''

# Activate virtual environment
$VenvPython = Join-Path $ScriptDir '.venv\Scripts\python.exe'
if (-not (Test-Path $VenvPython)) {
    Write-Host 'Virtual environment not found. Creating it...' -ForegroundColor Yellow
    python -m venv .venv
    Write-Host 'Installing dependencies...' -ForegroundColor Yellow
    & '.\.venv\Scripts\pip.exe' install -r requirements.txt
}

# Check .env exists
if (-not (Test-Path '.env')) {
    Write-Host 'WARNING: .env file not found. Copying .env.example...' -ForegroundColor Yellow
    Copy-Item '.env.example' '.env'
    Write-Host 'Please edit .env with your PostgreSQL password, then re-run this script.' -ForegroundColor Red
    Read-Host 'Press Enter to exit'
    exit 1
}

Write-Host 'Starting FastAPI server...' -ForegroundColor Green
Write-Host 'Open http://localhost:8000 in your browser.' -ForegroundColor Green
Write-Host 'Press Ctrl+C to stop.' -ForegroundColor Yellow
Write-Host ''

$env:PYTHONPATH = $ScriptDir
& $VenvPython -m uvicorn backend.main:app --host 0.0.0.0 --port 8000 --reload
