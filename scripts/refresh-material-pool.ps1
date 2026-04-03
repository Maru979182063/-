param(
    [int]$TargetStableMaterials = 1000,
    [int]$MinPerQuestionType = 100,
    [int]$MaxRounds = 2,
    [switch]$MissingStructureOnly
)

$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$envFile = Join-Path $root ".env.demo"
$passagePython = Join-Path $root "passage_service\.venv\Scripts\python.exe"

function Load-DemoEnvFile {
    param([string]$Path)

    if (-not (Test-Path $Path)) {
        return
    }

    foreach ($rawLine in Get-Content $Path) {
        $line = $rawLine.Trim()
        if (-not $line -or $line.StartsWith("#")) {
            continue
        }
        $parts = $line.Split("=", 2)
        if ($parts.Count -ne 2) {
            continue
        }
        $name = $parts[0].Trim()
        $value = $parts[1].Trim()
        if (($value.StartsWith('"') -and $value.EndsWith('"')) -or ($value.StartsWith("'") -and $value.EndsWith("'"))) {
            $value = $value.Substring(1, $value.Length - 2)
        }
        if ($name) {
            [System.Environment]::SetEnvironmentVariable($name, $value, "Process")
        }
    }
}

Load-DemoEnvFile -Path $envFile

if (-not (Test-Path $passagePython)) {
    throw "passage_service Python runtime not found at $passagePython"
}

$passageDbPath = Join-Path $root "passage_service\passage_service.db"
[System.Environment]::SetEnvironmentVariable("PASSAGE_DATABASE_URL", "sqlite:///$($passageDbPath -replace '\\','/')", "Process")

Write-Host "[1/2] Reprocessing existing materials..." -ForegroundColor Cyan
$reprocessArgs = @(
    (Join-Path $root "passage_service\scripts\reprocess_material_pool.py")
)
if ($MissingStructureOnly) {
    $reprocessArgs += "--missing-structure-only"
}
& $passagePython @reprocessArgs

Write-Host "[2/2] Expanding stable material pool..." -ForegroundColor Cyan
& $passagePython (Join-Path $root "passage_service\scripts\expand_material_pool.py") `
    --target-stable-materials $TargetStableMaterials `
    --min-per-question-type $MinPerQuestionType `
    --max-rounds $MaxRounds

Write-Host ""
Write-Host "Material refresh completed." -ForegroundColor Green
