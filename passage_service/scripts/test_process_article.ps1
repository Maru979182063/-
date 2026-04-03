param(
  [string]$BaseUrl = "http://127.0.0.1:8000",
  [string]$SamplePath = "",
  [string]$SourceName = "manual_test",
  [string]$SourceUrl = "local://manual-test-article",
  [string]$Domain = "manual_test"
)

$ErrorActionPreference = "Stop"

Set-Location (Join-Path $PSScriptRoot "..")

$llmConfigPath = Join-Path (Join-Path $PWD "app") "config\\llm.yaml"
if (Test-Path $llmConfigPath) {
  $llmConfigText = Get-Content -Raw -Encoding UTF8 $llmConfigPath
  if ($llmConfigText -match "enabled:\s*false") {
    Write-Warning "LLM tagging is still disabled in app/config/llm.yaml. This run will use heuristic fallback."
  }
}

if (-not $SamplePath) {
  $SamplePath = Join-Path $PSScriptRoot "sample_article.txt"
}

if (-not (Test-Path $SamplePath)) {
  throw "Sample file not found: $SamplePath"
}

$resolvedSamplePath = (Resolve-Path $SamplePath).Path
$rawText = [System.IO.File]::ReadAllText($resolvedSamplePath, [System.Text.Encoding]::UTF8)
if ($rawText -isnot [string]) {
  throw "Sample text was not loaded as a plain string."
}

$ingestPayload = @{
  source = $SourceName
  source_url = $SourceUrl
  title = "manual test article"
  raw_text = $rawText
  language = "zh"
  domain = $Domain
} | ConvertTo-Json -Depth 6

Write-Output "[1/3] Ingesting article..."
$ingestResponse = Invoke-RestMethod `
  -Method Post `
  -Uri "$BaseUrl/articles/ingest" `
  -ContentType "application/json; charset=utf-8" `
  -Body $ingestPayload

$articleId = $ingestResponse.article_id
Write-Output "ARTICLE_ID: $articleId"

Write-Output "[2/3] Processing article..."
$processPayload = @{ mode = "full" } | ConvertTo-Json
$processResponse = Invoke-RestMethod `
  -Method Post `
  -Uri "$BaseUrl/articles/$articleId/process" `
  -ContentType "application/json; charset=utf-8" `
  -Body $processPayload

Write-Output "[3/3] Exporting review snapshot..."
$reviewResponse = Invoke-RestMethod `
  -Method Post `
  -Uri "$BaseUrl/articles/$articleId/review-export"

Write-Output ""
Write-Output "PROCESS RESULT"
Write-Output ("SEGMENT_STATUS: " + $processResponse.segment.status)
Write-Output ("TAG_COUNT: " + $processResponse.tag.count)
Write-Output ("REVIEW_DIR: " + $reviewResponse.dir)
Write-Output ("REVIEW_JSON: " + $reviewResponse.json_path)
Write-Output ("REVIEW_TXT: " + $reviewResponse.txt_path)
