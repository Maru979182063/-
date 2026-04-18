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
$subfamilyName = U @(0x5206,0x8FF0,0x53E5,0x7279,0x5F81)

$batchName = "center_understanding_subsentence_features_$BatchDate"
$outputDir = Join-Path $OutputRoot $batchName
New-Item -ItemType Directory -Path $outputDir -Force | Out-Null

$docxFiles = Get-ChildItem -LiteralPath $InputDir -Filter "*.docx" | Where-Object {
    $base = [IO.Path]::GetFileNameWithoutExtension($_.Name)
    ($base -eq (U @(0x4E3E,0x4F8B,0x5B50))) -or
    ($base -eq (U @(0x6570,0x636E,0x8D44,0x6599))) -or
    ($base -eq (U @(0x5F15,0x5165,0x94FA,0x57AB))) -or
    ($base -eq (U @(0x591A,0x89D2,0x5EA6,0x8BBA,0x8FF0))) -or
    ($base -like ((U @(0x5206,0x8FF0,0x53E5,0x7279,0x5F81)) + "*"))
} | Sort-Object Name

if (-not $docxFiles -or $docxFiles.Count -eq 0) {
    throw "No target docx found in $InputDir"
}

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

        $exampleCount = Count-Matches -Text $material -Patterns @(
            (U @(0x4F8B,0x5982)), # 例如
            (U @(0x6BD4,0x5982)), # 比如
            (U @(0x4EE5) + ".*" + (U @(0x4E3A,0x4F8B))), # 以...为例
            (U @(0x4E3E,0x4F8B))  # 举例
        )
        $dataCount = Count-Matches -Text $material -Patterns @(
            "\d+(\.\d+)?%","[0-9]{2,}",
            (U @(0x6570,0x636E)), # 数据
            (U @(0x7EDF,0x8BA1)), # 统计
            (U @(0x663E,0x793A))  # 显示
        )
        $preludeCount = Count-Matches -Text $material -Patterns @(
            (U @(0x8FD1,0x5E74,0x6765)),
            (U @(0x968F,0x7740)),
            (U @(0x5F53,0x524D)),
            (U @(0x9996,0x5148)),
            (U @(0x4E00,0x76F4,0x4EE5,0x6765))
        )
        $multiAngleCount = Count-Matches -Text $material -Patterns @(
            (U @(0x4E00,0x65B9,0x9762)),
            (U @(0x53E6,0x4E00,0x65B9,0x9762)),
            (U @(0x591A,0x89D2,0x5EA6)),
            (U @(0x5176,0x4E00)),
            (U @(0x5176,0x4E8C)),
            (U @(0x540C,0x65F6))
        )

        $leafGuess = "other"
        if ($patternTag -like ((U @(0x4E3E,0x4F8B,0x5B50)) + "*")) { $leafGuess = "example" }
        elseif ($patternTag -like ((U @(0x6570,0x636E,0x8D44,0x6599)) + "*")) { $leafGuess = "data" }
        elseif ($patternTag -like ((U @(0x5F15,0x5165,0x94FA,0x57AB)) + "*")) { $leafGuess = "prelude" }
        elseif ($patternTag -like ((U @(0x591A,0x89D2,0x5EA6,0x8BBA,0x8FF0)) + "*")) { $leafGuess = "multi_angle" }

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
            example_marker_count = $exampleCount
            data_marker_count = $dataCount
            prelude_marker_count = $preludeCount
            multi_angle_marker_count = $multiAngleCount
        })
    }
}

$allRows = $rows.ToArray()
$jsonlPath = Join-Path $outputDir "material_samples_cleaned.jsonl"
$csvPath = Join-Path $outputDir "material_samples_cleaned.csv"
$summaryPath = Join-Path $outputDir "batch_summary.json"
$cardFieldPath = Join-Path $outputDir "material_card_fields_v1.yaml"
$reportPath = Join-Path $outputDir "batch_report_zh.md"

Write-Jsonl -Items $allRows -Path $jsonlPath
$allRows | Export-Csv -LiteralPath $csvPath -NoTypeInformation -Encoding utf8

$total = $allRows.Count
$lengths = @($allRows | ForEach-Object { [int]$_.material_char_count })
$sentences = @($allRows | ForEach-Object { [int]$_.material_sentence_count })
$avgLen = if ($total -gt 0) { [math]::Round((($lengths | Measure-Object -Average).Average), 1) } else { 0 }
$avgSent = if ($total -gt 0) { [math]::Round((($sentences | Measure-Object -Average).Average), 2) } else { 0 }
$p25 = if ($total -gt 0) { ($lengths | Sort-Object)[[math]::Floor(($total - 1) * 0.25)] } else { 0 }
$p75 = if ($total -gt 0) { ($lengths | Sort-Object)[[math]::Floor(($total - 1) * 0.75)] } else { 0 }

$patternStats = $allRows | Group-Object pattern_tag | Sort-Object Name | ForEach-Object {
    $g = $_.Group
    [PSCustomObject]@{
        pattern_tag = $_.Name
        sample_count = $_.Count
        avg_material_len = [math]::Round((($g | Measure-Object material_char_count -Average).Average), 1)
        avg_sentence_count = [math]::Round((($g | Measure-Object material_sentence_count -Average).Average), 2)
        avg_example_markers = [math]::Round((($g | Measure-Object example_marker_count -Average).Average), 2)
        avg_data_markers = [math]::Round((($g | Measure-Object data_marker_count -Average).Average), 2)
        avg_prelude_markers = [math]::Round((($g | Measure-Object prelude_marker_count -Average).Average), 2)
        avg_multi_angle_markers = [math]::Round((($g | Measure-Object multi_angle_marker_count -Average).Average), 2)
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
    pattern_stats = $patternStats
}
$summary | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $summaryPath -Encoding utf8

$leafRows = $allRows | Group-Object leaf_guess | Where-Object { $_.Name -ne "other" }
$cardLines = New-Object System.Collections.Generic.List[string]
$cardLines.Add("schema_version: material_card_fields.v1")
$cardLines.Add("family_id: center_understanding")
$cardLines.Add("subfamily_id: subsentence_features")
$cardLines.Add("batch_id: $batchName")
$cardLines.Add("cards:")
foreach ($group in $leafRows) {
    $name = $group.Name
    $g = $group.Group
    $lenAvg = [math]::Round((($g | Measure-Object material_char_count -Average).Average), 1)
    $sentAvg = [math]::Round((($g | Measure-Object material_sentence_count -Average).Average), 2)
    $exAvg = [math]::Round((($g | Measure-Object example_marker_count -Average).Average), 2)
    $dataAvg = [math]::Round((($g | Measure-Object data_marker_count -Average).Average), 2)
    $preAvg = [math]::Round((($g | Measure-Object prelude_marker_count -Average).Average), 2)
    $mulAvg = [math]::Round((($g | Measure-Object multi_angle_marker_count -Average).Average), 2)
    $cardId = "center_material.subsentence_$name"
    $cardLines.Add("  - card_id: $cardId")
    $cardLines.Add("    sample_count: $($g.Count)")
    $cardLines.Add("    target_length:")
    $cardLines.Add("      p25: $p25")
    $cardLines.Add("      p75: $p75")
    $cardLines.Add("      avg: $lenAvg")
    $cardLines.Add("    target_sentences:")
    $cardLines.Add("      avg: $sentAvg")
    $cardLines.Add("      suggested_min: 4")
    $cardLines.Add("      suggested_max: 9")
    $cardLines.Add("    marker_profile:")
    $cardLines.Add("      example_avg: $exAvg")
    $cardLines.Add("      data_avg: $dataAvg")
    $cardLines.Add("      prelude_avg: $preAvg")
    $cardLines.Add("      multi_angle_avg: $mulAvg")
    $cardLines.Add("    required_signals:")
    $cardLines.Add('      single_center_strength: ">=0.54"')
    $cardLines.Add('      closure_score: ">=0.52"')
    switch ($name) {
        "example" { $cardLines.Add('      example_to_theme_strength: ">=0.50"') }
        "data" { $cardLines.Add('      titleability: ">=0.56"') }
        "prelude" { $cardLines.Add('      summary_strength: ">=0.50"') }
        "multi_angle" { $cardLines.Add('      multi_dimension_cohesion: ">=0.56"') }
        default { $cardLines.Add('      titleability: ">=0.54"') }
    }
    $cardLines.Add("    candidate_contract:")
    $cardLines.Add("      allowed_candidate_types: [whole_passage, multi_paragraph_unit, closed_span]")
    $cardLines.Add("      preferred_candidate_types: [whole_passage, multi_paragraph_unit]")
}
Set-Content -LiteralPath $cardFieldPath -Value $cardLines -Encoding utf8

$report = @"
# 批次处理报告（中心理解题 / 分述句特征）

## 已完成
1. 5个指定题包已完成清洗和结构化。
2. 已产出可用于建卡的字段统计和最小材料卡字段包。

## 样本规模
- total_samples: $total
- avg_material_len: $avgLen
- length_window(p25,p75): [$p25, $p75]
- avg_sentence_count: $avgSent

## 输出文件
- material_samples_cleaned.jsonl
- material_samples_cleaned.csv
- batch_summary.json
- material_card_fields_v1.yaml
"@
Set-Content -LiteralPath $reportPath -Value $report -Encoding utf8

Write-Output "Done. Output directory: $outputDir"
