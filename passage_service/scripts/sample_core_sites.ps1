function Get-DecodedHtml {
  param(
    [Parameter(Mandatory = $true)]
    [string]$Url
  )

  $response = Invoke-WebRequest -UseBasicParsing -Uri $Url -TimeoutSec 30
  $bytes = $response.RawContentStream.ToArray()
  $encodings = New-Object System.Collections.Generic.List[string]

  if ($response.Headers["Content-Type"] -match "charset=([a-zA-Z0-9._-]+)") {
    $encodings.Add($matches[1])
  }

  $asciiHead = [System.Text.Encoding]::ASCII.GetString($bytes, 0, [Math]::Min($bytes.Length, 4096))
  $metaPatterns = @(
    '<meta[^>]+charset=["'']?([a-zA-Z0-9._-]+)',
    '<meta[^>]+content=["''][^"'']*charset=([a-zA-Z0-9._-]+)'
  )
  foreach ($pattern in $metaPatterns) {
    $metaMatch = [regex]::Match($asciiHead, $pattern, [System.Text.RegularExpressions.RegexOptions]::IgnoreCase)
    if ($metaMatch.Success) {
      $encodings.Add($metaMatch.Groups[1].Value)
    }
  }

  @("utf-8", "gb18030", "gbk", "gb2312") | ForEach-Object { $encodings.Add($_) }
  $seen = @{}
  foreach ($encodingName in $encodings) {
    $normalized = $encodingName.Trim().ToLower()
    if (-not $normalized -or $seen.ContainsKey($normalized)) {
      continue
    }
    $seen[$normalized] = $true
    try {
      $encoding = [System.Text.Encoding]::GetEncoding($normalized)
      return $encoding.GetString($bytes)
    } catch {
      continue
    }
  }

  return [System.Text.Encoding]::UTF8.GetString($bytes)
}

function Normalize-HtmlText {
  param(
    [Parameter(Mandatory = $true)]
    [string]$Text
  )

  $value = [regex]::Replace($Text, "<script[\s\S]*?</script>", " ")
  $value = [regex]::Replace($value, "<style[\s\S]*?</style>", " ")
  $value = [regex]::Replace($value, "<[^>]+>", " ")
  $value = [regex]::Replace($value, "&nbsp;|&#160;", " ")
  $value = [regex]::Replace($value, "&emsp;|&#8195;", " ")
  $value = [regex]::Replace($value, "&ensp;|&#8194;", " ")
  $value = [regex]::Replace($value, "&amp;", "&")
  $value = [regex]::Replace($value, "&lt;", "<")
  $value = [regex]::Replace($value, "&gt;", ">")
  $value = [regex]::Replace($value, "\s+", " ")
  return $value.Trim()
}

$targets = @(
  @{
    name = "people"
    url = "http://opinion.people.com.cn/n1/2026/0313/c461529-40680977.html"
    patterns = @(
      '<div class="rm_txt_con"[\s\S]*?</div>',
      '<div class="box_con"[\s\S]*?</div>'
    )
  },
  @{
    name = "news_cn"
    url = "https://www.news.cn/comments/20260324/882a680f19dc46f6828da2b5ef1f2bb0/c.html"
    patterns = @(
      '<div class="article"[\s\S]*?</div>',
      '<div id="detail"[\s\S]*?</div>'
    )
  },
  @{
    name = "gmw"
    url = "https://politics.gmw.cn/2026-03/25/content_38668920.htm"
    patterns = @(
      '<div class="u-mainText"[\s\S]*?</div>',
      '<div class="m-content"[\s\S]*?</div>'
    )
  },
  @{
    name = "qstheory"
    url = "https://www.qstheory.cn/20260325/db91983087f3455cb84b86b428d301fa/c.html"
    patterns = @(
      '<div class="article-content"[\s\S]*?</div>',
      '<div class="text"[\s\S]*?</div>'
    )
  },
  @{
    name = "gov_cn"
    url = "https://www.gov.cn/yaowen/liebiao/202603/content_7063662.htm"
    patterns = @(
      '<div class="pages_content"[\s\S]*?</div>',
      '<div class="content"[\s\S]*?</div>'
    )
  }
)

$outputDir = Join-Path $PSScriptRoot "..\\review_samples\\core_sites"
New-Item -ItemType Directory -Force -Path $outputDir | Out-Null

foreach ($target in $targets) {
  try {
    $html = Get-DecodedHtml -Url $target.url
    $title = ""
    $titleMatch = [regex]::Match($html, "<title[^>]*>([\s\S]*?)</title>", [System.Text.RegularExpressions.RegexOptions]::IgnoreCase)
    if ($titleMatch.Success) {
      $title = Normalize-HtmlText -Text $titleMatch.Groups[1].Value
    }

    $best = ""
    foreach ($pattern in $target.patterns) {
      $matches = [regex]::Matches($html, $pattern, [System.Text.RegularExpressions.RegexOptions]::IgnoreCase)
      foreach ($match in $matches) {
        $text = Normalize-HtmlText -Text $match.Value
        if ($text.Length -gt $best.Length) {
          $best = $text
        }
      }
    }

    if (-not $best) {
      $best = Normalize-HtmlText -Text $html
    }

    $snippet = if ($best.Length -gt 120) { $best.Substring(0, 120) } else { $best }
    Write-Output "[$($target.name)]"
    Write-Output "URL: $($target.url)"
    Write-Output "TITLE: $title"
    Write-Output "BODY_LEN: $($best.Length)"
    Write-Output "SNIPPET: $snippet"
    Write-Output ""

    $result = [ordered]@{
      site = $target.name
      url = $target.url
      title = $title
      body_length = $best.Length
      snippet = $snippet
      body_text = $best
      fetched_at = (Get-Date).ToString("s")
    }
    $jsonPath = Join-Path $outputDir ($target.name + ".json")
    $txtPath = Join-Path $outputDir ($target.name + ".txt")
    ($result | ConvertTo-Json -Depth 6) | Set-Content -Encoding UTF8 $jsonPath
    @(
      "SITE: $($target.name)"
      "URL: $($target.url)"
      "TITLE: $title"
      "BODY_LEN: $($best.Length)"
      ""
      "BODY:"
      $best
    ) | Set-Content -Encoding UTF8 $txtPath
  } catch {
    Write-Output "[$($target.name)] ERROR: $($_.Exception.Message)"
    Write-Output ""

    $errorPath = Join-Path $outputDir ($target.name + ".error.txt")
    @(
      "SITE: $($target.name)"
      "URL: $($target.url)"
      "ERROR: $($_.Exception.Message)"
      "FETCHED_AT: $((Get-Date).ToString("s"))"
    ) | Set-Content -Encoding UTF8 $errorPath
  }
}
