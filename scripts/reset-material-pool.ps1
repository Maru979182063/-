param(
  [switch]$Recrawl,
  [int]$TargetArticles = 200,
  [switch]$IncludeExports
)

$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$passageRoot = Join-Path $root "passage_service"
$passagePython = Join-Path $passageRoot ".venv\Scripts\python.exe"

Write-Output "[1/5] Stopping background material jobs and passage_service..."
Get-CimInstance Win32_Process |
  Where-Object {
    $_.CommandLine -like "*passage_service*app.main:app*" -or
    $_.CommandLine -like "*reprocess_material_pool.py*" -or
    $_.CommandLine -like "*expand_material_pool.py*" -or
    $_.CommandLine -like "*crawl_and_export_dify_pack.py*"
  } |
  ForEach-Object {
    try { Stop-Process -Id $_.ProcessId -Force -ErrorAction Stop } catch {}
  }

Write-Output "[2/5] Backing up and clearing passage material store..."
$resetArgs = @(
  (Join-Path $passageRoot "scripts\reset_material_store.py")
)
if ($IncludeExports) {
  $resetArgs += "--include-exports"
}
& $passagePython @resetArgs

Write-Output "[3/5] Restarting passage_service..."
Start-Process -FilePath powershell -ArgumentList '-NoExit','-Command',"`$env:PASSAGE_DISABLE_SCHEDULER='true'; Set-Location '$passageRoot'; & '$passagePython' -m uvicorn app.main:app --host 127.0.0.1 --port 8001 --reload" | Out-Null
Start-Sleep -Seconds 6
Invoke-WebRequest -UseBasicParsing -TimeoutSec 15 http://127.0.0.1:8001/materials/stats | Out-Null

if ($Recrawl) {
  Write-Output "[4/5] Starting incremental crawl + rebuild..."
  & $passagePython (Join-Path $passageRoot "scripts\crawl_and_export_dify_pack.py") --target-articles $TargetArticles
} else {
  Write-Output "[4/5] Recrawl skipped."
}

Write-Output "[5/5] Done."
Write-Output "Local material store has been reset."
