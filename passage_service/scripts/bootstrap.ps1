$ErrorActionPreference = "Stop"

Set-Location (Join-Path $PSScriptRoot "..")

Write-Output "[1/4] Checking Python..."
$python = Get-Command python -ErrorAction SilentlyContinue
if (-not $python) {
  Write-Error "Python was not found in PATH. Install Python 3.11+ and try again."
}

Write-Output "[2/4] Creating virtual environment if missing..."
if (-not (Test-Path ".\.venv\Scripts\python.exe")) {
  python -m venv .venv
}

Write-Output "[3/4] Activating virtual environment..."
. .\.venv\Scripts\Activate.ps1

Write-Output "[4/4] Installing project..."
python -m pip install --upgrade pip
python -m pip install -e .

Write-Output ""
Write-Output "Bootstrap complete."
Write-Output "Next:"
Write-Output "1. Copy .env.example values into your environment"
Write-Output "2. Set app/config/llm.yaml enabled: true if you want real LLM tagging"
Write-Output "3. Run: .\.venv\Scripts\Activate.ps1"
Write-Output "4. Run: uvicorn app.main:app --reload"
