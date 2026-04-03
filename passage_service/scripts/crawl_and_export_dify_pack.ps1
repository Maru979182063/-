param(
  [int]$TargetArticles = 100,
  [string]$OutputDir = "",
  [switch]$IncludeGray
)

$projectRoot = Split-Path -Parent $PSScriptRoot
$pythonExe = Join-Path $projectRoot ".venv\\Scripts\\python.exe"

if (-not (Test-Path $pythonExe)) {
  Write-Error "未找到虚拟环境 Python：$pythonExe"
  exit 1
}

$args = @(
  (Join-Path $PSScriptRoot "crawl_and_export_dify_pack.py"),
  "--target-articles", $TargetArticles
)

if ($OutputDir -ne "") {
  $args += @("--output-dir", $OutputDir)
}

if ($IncludeGray) {
  $args += "--include-gray"
}

& $pythonExe @args
