param(
    [Parameter(Mandatory = $true)]
    [string]$InputDir,
    [Parameter(Mandatory = $false)]
    [string]$OutputRoot = ".\reports\distill_batches",
    [Parameter(Mandatory = $false)]
    [string]$BatchDate = (Get-Date -Format "yyyy-MM-dd")
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
Add-Type -AssemblyName System.IO.Compression.FileSystem

function U {
    param([int[]]$Codes)
    return -join ($Codes | ForEach-Object { [char]$_ })
}

function Normalize-Text {
    param([string]$Text)
    if ($null -eq $Text) { return "" }
    $t = $Text -replace "`r", ""
    $t = $t -replace "\u00A0", " "
    $t = $t -replace "\s+", " "
    return $t.Trim()
}

function Get-DocxParagraphs {
    param([string]$DocxPath)
    $zip = [System.IO.Compression.ZipFile]::OpenRead($DocxPath)
    try {
        $entry = $zip.Entries | Where-Object { $_.FullName -eq "word/document.xml" } | Select-Object -First 1
        if (-not $entry) { throw "word/document.xml missing: $DocxPath" }
        $stream = $entry.Open()
        try {
            $reader = New-Object System.IO.StreamReader($stream)
            $xmlText = $reader.ReadToEnd()
            $reader.Close()
        } finally {
            $stream.Dispose()
        }
    } finally {
        $zip.Dispose()
    }

    $xml = [xml]$xmlText
    $ns = New-Object System.Xml.XmlNamespaceManager($xml.NameTable)
    $ns.AddNamespace("w", "http://schemas.openxmlformats.org/wordprocessingml/2006/main")
    $paragraphs = $xml.SelectNodes("//w:p", $ns)
    $out = New-Object System.Collections.Generic.List[string]
    foreach ($p in $paragraphs) {
        $texts = $p.SelectNodes(".//w:t", $ns) | ForEach-Object { $_.'#text' }
        if ($texts) { $out.Add(($texts -join "")) } else { $out.Add("") }
    }
    return $out
}

function Parse-TaggedSections {
    param([string]$Block)
    $lb = [char]0x3010
    $rb = [char]0x3011
    $pattern = [string]::Format("(?s){0}(?<k>[^{1}]+){1}\s*(?<v>.*?)(?={0}[^{1}]+{1}|$)", [regex]::Escape($lb), [regex]::Escape($rb))
    $dict = @{}
    foreach ($m in [regex]::Matches($Block, $pattern)) {
        $k = Normalize-Text $m.Groups["k"].Value
        $v = Normalize-Text $m.Groups["v"].Value
        if (-not $dict.ContainsKey($k)) { $dict[$k] = $v }
    }
    return $dict
}

function Parse-Options {
    param([string]$Text)
    $out = @{ A = ""; B = ""; C = ""; D = "" }
    foreach ($m in [regex]::Matches($Text, "(?s)(?<k>[A-D])[\.\uFF0E\u3001]\s*(?<v>.*?)(?=(?:[A-D][\.\uFF0E\u3001])|$)")) {
        $out[$m.Groups["k"].Value] = Normalize-Text $m.Groups["v"].Value
    }
    return $out
}

function Split-MaterialAndStem {
    param([string]$PromptText)
    $material = $PromptText
    $stem = ""
    if ($PromptText -match "^(?<m>.*?)(?<s>[^\u3002\uFF01\uFF1F!?]*[\uFF08(]\s*[\uFF09)][^\u3002\uFF01\uFF1F!?]*[\u3002\uFF01\uFF1F!?]?)$") {
        $material = Normalize-Text $matches["m"]
        $stem = Normalize-Text $matches["s"]
    }
    return @{ material = $material; stem = $stem }
}

function Count-Matches {
    param([string]$Text, [string[]]$Patterns)
    $count = 0
    foreach ($p in $Patterns) {
        $count += [regex]::Matches($Text, $p).Count
    }
    return $count
}

function Write-Jsonl {
    param([object[]]$Items, [string]$Path)
    $lines = New-Object System.Collections.Generic.List[string]
    foreach ($item in $Items) { $lines.Add(($item | ConvertTo-Json -Depth 8 -Compress)) }
    Set-Content -LiteralPath $Path -Value $lines -Encoding utf8
}

$tagPaper = U @(0x6240,0x5C5E,0x8BD5,0x5377)
$tagBody = U @(0x9898,0x5E72)
$tagAnswer = U @(0x7B54,0x6848)
$tagAnalysis = U @(0x89E3,0x6790)
$tagSource = U @(0x6587,0x6BB5,0x51FA,0x5904)
$tagAccuracy = U @(0x6B63,0x786E,0x7387)
$tagWrong = U @(0x6613,0x9519,0x9879)
$tagPoints = U @(0x8003,0x70B9)
$qidLabel = U @(0x9898,0x53F7)
$fullColon = U @(0xFF1A)
$familyName = U @(0x4E2D,0x5FC3,0x7406,0x89E3,0x9898)
$subfamilyName = U @(0x7279,0x6B8A,0x9898,0x578B)

$batchName = "center_understanding_special_types_$BatchDate"
$outputDir = Join-Path $OutputRoot $batchName
New-Item -ItemType Directory -Path $outputDir -Force | Out-Null

$nameKeyword = U @(0x4E3B,0x9898,0x8BCD,0x3001,0x5173,0x952E,0x8BCD)
$nameFable = U @(0x5BD3,0x8A00,0x6545,0x4E8B)
$nameClassical = U @(0x6587,0x8A00,0x6587,0x3001,0x6563,0x6587,0x7B49)
$nameOther = U @(0x7279,0x6B8A,0x95EE,0x6CD5,0x002D,0x5176,0x4ED6)

$docxFiles = Get-ChildItem -LiteralPath $InputDir -Filter "*.docx" | Where-Object {
    $base = [IO.Path]::GetFileNameWithoutExtension($_.Name)
    ($base -eq $nameKeyword) -or
    ($base -eq $nameFable) -or
    ($base -eq $nameClassical) -or
    ($base -eq $nameOther)
} | Sort-Object Name
if (-not $docxFiles -or $docxFiles.Count -eq 0) { throw "No target docx found in $InputDir" }

$rows = New-Object System.Collections.Generic.List[object]
foreach ($f in $docxFiles) {
    $patternTag = [IO.Path]::GetFileNameWithoutExtension($f.Name)
    $paras = Get-DocxParagraphs -DocxPath $f.FullName
    $raw = ($paras -join "`n")
    $blocks = [regex]::Split($raw, "(?m)(?=^\d+\.\s*" + [regex]::Escape($qidLabel) + [regex]::Escape($fullColon) + "#)")
    foreach ($blockRaw in $blocks) {
        $block = ($blockRaw -replace "`r", "").Trim()
        if ([string]::IsNullOrWhiteSpace($block)) { continue }
        if ($block -notmatch ([regex]::Escape($qidLabel) + [regex]::Escape($fullColon) + "#\d+")) { continue }

        $qid = ""
        if ($block -match ([regex]::Escape($qidLabel) + [regex]::Escape($fullColon) + "#(?<id>\d+)")) { $qid = $matches["id"] }

        $sections = Parse-TaggedSections -Block $block
        $paper = if ($sections.ContainsKey($tagPaper)) { $sections[$tagPaper] } else { "" }
        $body = if ($sections.ContainsKey($tagBody)) { $sections[$tagBody] } else { "" }
        $answer = if ($sections.ContainsKey($tagAnswer)) { $sections[$tagAnswer] } else { "" }
        $analysis = if ($sections.ContainsKey($tagAnalysis)) { $sections[$tagAnalysis] } else { "" }
        $source = if ($sections.ContainsKey($tagSource)) { $sections[$tagSource] } else { "" }
        $accuracy = if ($sections.ContainsKey($tagAccuracy)) { $sections[$tagAccuracy] } else { "" }
        $wrongOption = if ($sections.ContainsKey($tagWrong)) { $sections[$tagWrong] } else { "" }
        $points = if ($sections.ContainsKey($tagPoints)) { $sections[$tagPoints] } else { "" }

        $bodyCompact = Normalize-Text $body
        $optionStart = [regex]::Match($bodyCompact, "[A-D][\.\uFF0E\u3001]")
        $promptText = $bodyCompact
        $optionText = ""
        if ($optionStart.Success) {
            $promptText = Normalize-Text $bodyCompact.Substring(0, $optionStart.Index)
            $optionText = Normalize-Text $bodyCompact.Substring($optionStart.Index)
        }
        $split = Split-MaterialAndStem -PromptText $promptText
        $material = $split.material
        $stem = $split.stem
        $opts = Parse-Options -Text $optionText

        $charCount = $material.Length
        $sentCount = (($material -split "[\u3002\uFF01\uFF1F!?;\uFF1B]" | Where-Object { $_.Trim().Length -gt 0 } | Measure-Object).Count)

        $leafGuess = "special_other"
        if ($patternTag -eq $nameKeyword) { $leafGuess = "keywords_theme" }
        elseif ($patternTag -eq $nameFable) { $leafGuess = "fable_story" }
        elseif ($patternTag -eq $nameClassical) { $leafGuess = "classical_prose" }

        $keywordCount = Count-Matches -Text $material -Patterns @(
            (U @(0x4E3B,0x9898)),
            (U @(0x5173,0x952E)),
            (U @(0x6838,0x5FC3)),
            (U @(0x4E3B,0x65E8)),
            (U @(0x4E3B,0x610F))
        )
        $fableCount = Count-Matches -Text $material -Patterns @(
            (U @(0x5BD3,0x8A00)),
            (U @(0x6545,0x4E8B)),
            (U @(0x542F,0x793A)),
            (U @(0x9053,0x7406)),
            (U @(0x54F2,0x7406))
        )
        $classicalCount = Count-Matches -Text $material -Patterns @(
            (U @(0x4E4B)),
            (U @(0x5176)),
            (U @(0x8005)),
            (U @(0x7136)),
            (U @(0x7109)),
            (U @(0x4E8E))
        )
        $quoteCount = Count-Matches -Text $material -Patterns @(
            (U @(0x201C)),
            (U @(0x201D)),
            (U @(0x300A)),
            (U @(0x300B))
        )

        $rows.Add([PSCustomObject]@{
            family = $familyName
            subfamily = $subfamilyName
            pattern_tag = $patternTag
            leaf_guess = $leafGuess
            source_doc = $f.Name
            question_id = $qid
            paper = $paper
            material = $material
            stem = $stem
            option_a = $opts["A"]
            option_b = $opts["B"]
            option_c = $opts["C"]
            option_d = $opts["D"]
            answer = $answer
            analysis = $analysis
            source = $source
            accuracy = $accuracy
            wrong_option = $wrongOption
            points = $points
            material_char_count = $charCount
            material_sentence_count = $sentCount
            keyword_marker_count = $keywordCount
            fable_marker_count = $fableCount
            classical_marker_count = $classicalCount
            quote_marker_count = $quoteCount
        })
    }
}

$allRows = $rows.ToArray()
$jsonlPath = Join-Path $outputDir "material_samples_cleaned.jsonl"
$csvPath = Join-Path $outputDir "material_samples_cleaned.csv"
$summaryPath = Join-Path $outputDir "batch_summary.json"
$overviewPath = Join-Path $outputDir "distill_pack_overview.md"

Write-Jsonl -Items $allRows -Path $jsonlPath
$allRows | Export-Csv -LiteralPath $csvPath -NoTypeInformation -Encoding utf8

$total = $allRows.Count
$lengths = @($allRows | ForEach-Object { [int]$_.material_char_count })
$sentences = @($allRows | ForEach-Object { [int]$_.material_sentence_count })
$avgLen = if ($total -gt 0) { [math]::Round((($lengths | Measure-Object -Average).Average), 1) } else { 0 }
$avgSent = if ($total -gt 0) { [math]::Round((($sentences | Measure-Object -Average).Average), 2) } else { 0 }
$p25 = if ($total -gt 0) { ($lengths | Sort-Object)[[math]::Floor(($total - 1) * 0.25)] } else { 0 }
$p75 = if ($total -gt 0) { ($lengths | Sort-Object)[[math]::Floor(($total - 1) * 0.75)] } else { 0 }

$leafStats = $allRows | Group-Object leaf_guess | Sort-Object Name | ForEach-Object {
    $g = $_.Group
    [PSCustomObject]@{
        leaf_guess = $_.Name
        sample_count = $_.Count
        avg_material_len = [math]::Round((($g | Measure-Object material_char_count -Average).Average), 1)
        avg_sentence_count = [math]::Round((($g | Measure-Object material_sentence_count -Average).Average), 2)
        avg_keyword_markers = [math]::Round((($g | Measure-Object keyword_marker_count -Average).Average), 2)
        avg_fable_markers = [math]::Round((($g | Measure-Object fable_marker_count -Average).Average), 2)
        avg_classical_markers = [math]::Round((($g | Measure-Object classical_marker_count -Average).Average), 2)
        avg_quote_markers = [math]::Round((($g | Measure-Object quote_marker_count -Average).Average), 2)
    }
}

$summary = [PSCustomObject]@{
    batch = $batchName
    generated_at = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
    input_dir = $InputDir
    total_samples = $total
    avg_material_len = $avgLen
    material_len_p25 = $p25
    material_len_p75 = $p75
    avg_sentence_count = $avgSent
    leaf_stats = $leafStats
}
$summary | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $summaryPath -Encoding utf8

$overview = @"
# Distill Pack Overview (center_understanding / special_types)

## Batch
- batch: $batchName
- total_samples: $total
- avg_material_len: $avgLen
- length_window(p25,p75): [$p25, $p75]
- avg_sentence_count: $avgSent

## Files
- material_samples_cleaned.jsonl
- material_samples_cleaned.csv
- batch_summary.json
- distill_pack_overview.md

## Scope
- Cleaning and distill packs only.
- No material card outputs in this batch.
"@
Set-Content -LiteralPath $overviewPath -Value $overview -Encoding utf8

$leafGroups = $allRows | Group-Object leaf_guess
foreach ($leaf in $leafGroups) {
    $leafName = $leaf.Name
    $items = @($leaf.Group)
    $leafPath = Join-Path $outputDir ("distill_pack_" + $leafName + ".md")
    $leafCount = $items.Count
    $leafLenAvg = if ($leafCount -gt 0) { [math]::Round((($items | Measure-Object material_char_count -Average).Average), 1) } else { 0 }
    $leafSentAvg = if ($leafCount -gt 0) { [math]::Round((($items | Measure-Object material_sentence_count -Average).Average), 2) } else { 0 }
    $leafKwAvg = if ($leafCount -gt 0) { [math]::Round((($items | Measure-Object keyword_marker_count -Average).Average), 2) } else { 0 }
    $leafFableAvg = if ($leafCount -gt 0) { [math]::Round((($items | Measure-Object fable_marker_count -Average).Average), 2) } else { 0 }
    $leafClsAvg = if ($leafCount -gt 0) { [math]::Round((($items | Measure-Object classical_marker_count -Average).Average), 2) } else { 0 }
    $leafQuoteAvg = if ($leafCount -gt 0) { [math]::Round((($items | Measure-Object quote_marker_count -Average).Average), 2) } else { 0 }
    $docs = ($items | Group-Object source_doc | Sort-Object Name | ForEach-Object { $_.Name }) -join ", "

    $leafPack = @"
# Distill Pack: $leafName

## Sample Stats
- sample_count: $leafCount
- source_docs: $docs
- avg_material_len: $leafLenAvg
- avg_sentence_count: $leafSentAvg
- avg_keyword_markers: $leafKwAvg
- avg_fable_markers: $leafFableAvg
- avg_classical_markers: $leafClsAvg
- avg_quote_markers: $leafQuoteAvg

## Consumable Fields
- material
- stem
- option_a / option_b / option_c / option_d
- answer
- analysis
- material_char_count
- material_sentence_count
- keyword_marker_count / fable_marker_count / classical_marker_count / quote_marker_count
"@
    Set-Content -LiteralPath $leafPath -Value $leafPack -Encoding utf8
}

Write-Output "Done. Output directory: $outputDir"
