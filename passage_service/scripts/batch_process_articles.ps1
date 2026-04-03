param(
  [string]$BaseUrl = "http://127.0.0.1:8000",
  [int]$Limit = 100,
  [switch]$OnlyUntagged
)

$ErrorActionPreference = "Stop"

Set-Location (Join-Path $PSScriptRoot "..")

Write-Output "[1/3] Loading article list..."
$listResponse = Invoke-RestMethod `
  -Method Get `
  -Uri "$BaseUrl/articles?limit=$Limit"

$articles = @($listResponse.items)
if ($OnlyUntagged) {
  $articles = @($articles | Where-Object { $_.status -ne "tagged" })
}

if (-not $articles -or $articles.Count -eq 0) {
  Write-Output "No articles found to process."
  exit 0
}

$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$outputDir = Join-Path $PWD "review_samples\\batch_runs\\$timestamp"
New-Item -ItemType Directory -Force -Path $outputDir | Out-Null

$results = @()
$index = 0

Write-Output "[2/3] Processing articles..."
foreach ($article in $articles) {
  $index += 1
  $articleId = $article.article_id
  Write-Output ("[{0}/{1}] {2}" -f $index, $articles.Count, $articleId)

  try {
    $processPayload = @{ mode = "full" } | ConvertTo-Json
    $processResponse = Invoke-RestMethod `
      -Method Post `
      -Uri "$BaseUrl/articles/$articleId/process" `
      -ContentType "application/json; charset=utf-8" `
      -Body $processPayload

    $results += [pscustomobject]@{
      article_id = $articleId
      source = $article.source
      title = $article.title
      status = "ok"
      candidate_count = $processResponse.segment.candidate_span_count
      created_count = $processResponse.tag.summary.created_count
      rejected_count = $processResponse.tag.summary.rejected_count
      review_json = $processResponse.review_export.json_path
      review_dir = $processResponse.review_export.dir
    }
  }
  catch {
    $errorText = $_.Exception.Message
    $results += [pscustomobject]@{
      article_id = $articleId
      source = $article.source
      title = $article.title
      status = "error"
      candidate_count = $null
      created_count = $null
      rejected_count = $null
      review_json = $null
      review_dir = $null
      error = $errorText
    }
  }
}

$jsonPath = Join-Path $outputDir "batch_summary.json"
$csvPath = Join-Path $outputDir "batch_summary.csv"

$results | ConvertTo-Json -Depth 8 | Set-Content -Path $jsonPath -Encoding UTF8
$results | Export-Csv -Path $csvPath -NoTypeInformation -Encoding UTF8

Write-Output "[3/3] Batch complete."
Write-Output ("SUMMARY_JSON: " + $jsonPath)
Write-Output ("SUMMARY_CSV: " + $csvPath)
Write-Output ""
$results | Format-Table -AutoSize
