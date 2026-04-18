param(
    [string]$DestinationRoot = "C:\Users\Maru\Documents\agent_mvp"
)

$ErrorActionPreference = "Stop"

$sourceRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path

if (-not (Test-Path $DestinationRoot)) {
    throw "MVP worktree not found: $DestinationRoot"
}

$excludedPrefixes = @(
    "tmp_coze_learning_machine/"
)

function Test-ExcludedPath {
    param([string]$RelativePath)

    foreach ($prefix in $excludedPrefixes) {
        if ($RelativePath.StartsWith($prefix, [System.StringComparison]::OrdinalIgnoreCase)) {
            return $true
        }
    }
    return $false
}

$statusLines = git -C $sourceRoot status --porcelain=v1
if (-not $statusLines) {
    Write-Host "No local changes to sync." -ForegroundColor Yellow
    exit 0
}

$copiedCount = 0
$removedCount = 0

foreach ($line in $statusLines) {
    if ($line.Length -lt 4) {
        continue
    }

    $xy = $line.Substring(0, 2)
    $relativePath = $line.Substring(3).Trim()

    if ($relativePath.StartsWith('"') -and $relativePath.EndsWith('"')) {
        $relativePath = $relativePath.Substring(1, $relativePath.Length - 2)
    }

    if (Test-ExcludedPath -RelativePath $relativePath) {
        continue
    }

    $sourcePath = Join-Path $sourceRoot $relativePath
    $destinationPath = Join-Path $DestinationRoot $relativePath

    if ($xy.Contains("D")) {
        if (Test-Path $destinationPath) {
            Remove-Item -LiteralPath $destinationPath -Force
            $removedCount += 1
        }
        continue
    }

    if (-not (Test-Path $sourcePath)) {
        continue
    }

    $destinationParent = Split-Path -Parent $destinationPath
    if ($destinationParent -and -not (Test-Path $destinationParent)) {
        New-Item -ItemType Directory -Path $destinationParent -Force | Out-Null
    }

    Copy-Item -LiteralPath $sourcePath -Destination $destinationPath -Force
    $copiedCount += 1
}

Write-Host "Snapshot synced to $DestinationRoot" -ForegroundColor Green
Write-Host "Copied: $copiedCount" -ForegroundColor DarkGray
Write-Host "Removed: $removedCount" -ForegroundColor DarkGray
