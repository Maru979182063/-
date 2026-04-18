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

function Resolve-CorrectOptionText {
    param([string]$AnswerRaw, [hashtable]$Options)
    $ans = (Normalize-Text $AnswerRaw).ToUpper()
    if ([string]::IsNullOrWhiteSpace($ans)) { return "" }
    if ($ans -match '^[A-D]$') { return [string]$Options[$ans] }
    if ($ans -match '^[A-D][\.\uFF0E\u3001]?$') {
        $k = $ans.Substring(0, 1)
        return [string]$Options[$k]
    }
    foreach ($k in @('A', 'B', 'C', 'D')) {
        $v = [string]$Options[$k]
        if (-not [string]::IsNullOrWhiteSpace($v) -and $ans -eq (Normalize-Text $v).ToUpper()) { return $v }
    }
    return (Normalize-Text $AnswerRaw)
}

function Rebuild-MaterialByAnswer {
    param([string]$PromptText, [string]$CorrectText)
    $prompt = Normalize-Text $PromptText
    $fill = Normalize-Text $CorrectText
    if ([string]::IsNullOrWhiteSpace($prompt)) { return @{ text = ''; replaced = $false; pattern = 'empty_prompt' } }
    if ([string]::IsNullOrWhiteSpace($fill)) { return @{ text = $prompt; replaced = $false; pattern = 'empty_answer' } }

    $patterns = @(
        '_{2,}',
        ([string]([char]0xFE4D) + '{2,}'),
        ([string]([char]0x2014) + '{2,}'),
        ([string]([char]0xFF0D) + '{2,}'),
        ([string]([char]0xFF08) + '\s*' + [string]([char]0xFF09)),
        '\(\s*\)'
    )
    foreach ($p in $patterns) {
        if ([regex]::IsMatch($prompt, $p)) {
            $rebuilt = [regex]::Replace($prompt, $p, $fill, 1)
            return @{ text = (Normalize-Text $rebuilt); replaced = $true; pattern = $p }
        }
    }
    return @{ text = $prompt; replaced = $false; pattern = 'no_blank_pattern' }
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

$familyName = U @(0x8BED,0x53E5,0x586B,0x7A7A,0x9898)
$subfamilyName = U @(0x6A2A,0x7EBF,0x5728,0x7ED3,0x5C3E)

$nameTailClause = U @(0x6A2A,0x7EBF,0x5728,0x7ED3,0x5C3E,0x002D,0x6A2A,0x7EBF,0x4E3A,0x5C3E,0x53E5,0x4E2D,0x7684,0x5206,0x53E5)
$nameTailCountermeasure = U @(0x6A2A,0x7EBF,0x5728,0x7ED3,0x5C3E,0x002D,0x63D0,0x51FA,0x5BF9,0x7B56,0xFF08,0x539F,0x4E3A,0x0020,0x5BF9,0x7B56,0xFF09)
$nameTailSummary = U @(0x6A2A,0x7EBF,0x5728,0x7ED3,0x5C3E,0x002D,0x603B,0x7ED3,0x524D,0x6587,0xFF08,0x539F,0x4E3A,0x0020,0x7ED3,0x8BBA,0xFF09)

$batchName = "sentence_fill_tail_end_$BatchDate"
$outputDir = Join-Path $OutputRoot $batchName
New-Item -ItemType Directory -Path $outputDir -Force | Out-Null

$docxFiles = Get-ChildItem -LiteralPath $InputDir -Filter "*.docx" | Where-Object {
    $base = [IO.Path]::GetFileNameWithoutExtension($_.Name)
    ($base -eq $nameTailClause) -or ($base -eq $nameTailCountermeasure) -or ($base -eq $nameTailSummary)
} | Sort-Object Name
if (-not $docxFiles -or $docxFiles.Count -eq 0) { throw "No target docx found in $InputDir" }

$rows = New-Object System.Collections.Generic.List[object]
foreach ($f in $docxFiles) {
    $patternTag = [IO.Path]::GetFileNameWithoutExtension($f.Name)
    $leafGuess = 'tail_other'
    if ($patternTag -eq $nameTailClause) { $leafGuess = 'tail_clause' }
    elseif ($patternTag -eq $nameTailCountermeasure) { $leafGuess = 'tail_countermeasure' }
    elseif ($patternTag -eq $nameTailSummary) { $leafGuess = 'tail_summary' }

    $paras = Get-DocxParagraphs -DocxPath $f.FullName
    $raw = ($paras -join "`n")
    $blocks = [regex]::Split($raw, "(?m)(?=^\d+\.\s*" + [regex]::Escape($qidLabel) + [regex]::Escape($fullColon) + "#)")
    foreach ($blockRaw in $blocks) {
        $block = ($blockRaw -replace "`r", "").Trim()
        if ([string]::IsNullOrWhiteSpace($block)) { continue }
        if ($block -notmatch ([regex]::Escape($qidLabel) + [regex]::Escape($fullColon) + "#\d+")) { continue }

        $qid = ''
        if ($block -match ([regex]::Escape($qidLabel) + [regex]::Escape($fullColon) + "#(?<id>\d+)")) { $qid = $matches['id'] }

        $sections = Parse-TaggedSections -Block $block
        $paper = if ($sections.ContainsKey($tagPaper)) { $sections[$tagPaper] } else { '' }
        $body = if ($sections.ContainsKey($tagBody)) { $sections[$tagBody] } else { '' }
        $answer = if ($sections.ContainsKey($tagAnswer)) { $sections[$tagAnswer] } else { '' }
        $analysis = if ($sections.ContainsKey($tagAnalysis)) { $sections[$tagAnalysis] } else { '' }
        $source = if ($sections.ContainsKey($tagSource)) { $sections[$tagSource] } else { '' }
        $accuracy = if ($sections.ContainsKey($tagAccuracy)) { $sections[$tagAccuracy] } else { '' }
        $wrongOption = if ($sections.ContainsKey($tagWrong)) { $sections[$tagWrong] } else { '' }
        $points = if ($sections.ContainsKey($tagPoints)) { $sections[$tagPoints] } else { '' }

        $bodyCompact = Normalize-Text $body
        $optionStart = [regex]::Match($bodyCompact, "[A-D][\.\uFF0E\u3001]")
        $promptText = $bodyCompact
        $optionText = ''
        if ($optionStart.Success) {
            $promptText = Normalize-Text $bodyCompact.Substring(0, $optionStart.Index)
            $optionText = Normalize-Text $bodyCompact.Substring($optionStart.Index)
        }
        $opts = Parse-Options -Text $optionText
        $correctText = Resolve-CorrectOptionText -AnswerRaw $answer -Options $opts
        $rebuilt = Rebuild-MaterialByAnswer -PromptText $promptText -CorrectText $correctText
        $material = [string]$rebuilt.text

        $charCount = $material.Length
        $sentCount = (($material -split "[\u3002\uFF01\uFF1F!?;\uFF1B]" | Where-Object { $_.Trim().Length -gt 0 } | Measure-Object).Count)

        $rows.Add([PSCustomObject]@{
            family = $familyName
            subfamily = $subfamilyName
            leaf_guess = $leafGuess
            pattern_tag = $patternTag
            source_doc = $f.Name
            question_id = $qid
            paper = $paper
            material_rebuilt = $material
            prompt_before_fill = $promptText
            correct_option_text = $correctText
            rebuild_replaced = [bool]$rebuilt.replaced
            rebuild_pattern = [string]$rebuilt.pattern
            option_a = $opts['A']
            option_b = $opts['B']
            option_c = $opts['C']
            option_d = $opts['D']
            answer = $answer
            analysis = $analysis
            source = $source
            accuracy = $accuracy
            wrong_option = $wrongOption
            points = $points
            material_char_count = $charCount
            material_sentence_count = $sentCount
        })
    }
}

$allRows = $rows.ToArray()
$jsonlPath = Join-Path $outputDir 'material_samples_rebuilt.jsonl'
$csvPath = Join-Path $outputDir 'material_samples_rebuilt.csv'
$summaryPath = Join-Path $outputDir 'batch_summary.json'
$overviewPath = Join-Path $outputDir 'distill_pack_overview.md'

Write-Jsonl -Items $allRows -Path $jsonlPath
$allRows | Export-Csv -LiteralPath $csvPath -NoTypeInformation -Encoding utf8

$total = $allRows.Count
$lengths = @($allRows | ForEach-Object { [int]$_.material_char_count })
$sentences = @($allRows | ForEach-Object { [int]$_.material_sentence_count })
$rebuildReplacedCount = @($allRows | Where-Object { $_.rebuild_replaced -eq $true }).Count
$avgLen = if ($total -gt 0) { [math]::Round((($lengths | Measure-Object -Average).Average), 1) } else { 0 }
$avgSent = if ($total -gt 0) { [math]::Round((($sentences | Measure-Object -Average).Average), 2) } else { 0 }
$p25 = if ($total -gt 0) { ($lengths | Sort-Object)[[math]::Floor(($total - 1) * 0.25)] } else { 0 }
$p75 = if ($total -gt 0) { ($lengths | Sort-Object)[[math]::Floor(($total - 1) * 0.75)] } else { 0 }

$leafStats = $allRows | Group-Object leaf_guess | Sort-Object Name | ForEach-Object {
    $g = $_.Group
    [PSCustomObject]@{
        leaf_guess = $_.Name
        sample_count = $_.Count
        replaced_count = @($g | Where-Object { $_.rebuild_replaced -eq $true }).Count
        avg_material_len = [math]::Round((($g | Measure-Object material_char_count -Average).Average), 1)
        avg_sentence_count = [math]::Round((($g | Measure-Object material_sentence_count -Average).Average), 2)
    }
}

$summary = [PSCustomObject]@{
    batch = $batchName
    generated_at = (Get-Date).ToString('yyyy-MM-dd HH:mm:ss')
    input_dir = $InputDir
    taxonomy = @{ mother_family = 'sentence_fill'; subfamily = 'tail_end' }
    total_samples = $total
    rebuilt_by_answer_count = $rebuildReplacedCount
    avg_material_len = $avgLen
    material_len_p25 = $p25
    material_len_p75 = $p75
    avg_sentence_count = $avgSent
    leaf_stats = $leafStats
}
$summary | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $summaryPath -Encoding utf8

$overview = @"
# Distill Pack Overview (sentence_fill / tail_end)

## Method
- Rebuild material from prompt by filling blank with correct option text.
- No fast extraction-only mode.

## Batch
- batch: $batchName
- total_samples: $total
- rebuilt_by_answer_count: $rebuildReplacedCount
- avg_material_len: $avgLen
- length_window(p25,p75): [$p25, $p75]
- avg_sentence_count: $avgSent

## Files
- material_samples_rebuilt.jsonl
- material_samples_rebuilt.csv
- batch_summary.json
- distill_pack_overview.md
"@
Set-Content -LiteralPath $overviewPath -Value $overview -Encoding utf8

$leafGroups = $allRows | Group-Object leaf_guess
foreach ($leaf in $leafGroups) {
    $name = $leaf.Name
    $items = @($leaf.Group)
    $packPath = Join-Path $outputDir ("distill_pack_" + $name + ".md")
    $cnt = $items.Count
    $rep = @($items | Where-Object { $_.rebuild_replaced -eq $true }).Count
    $lenAvg = if ($cnt -gt 0) { [math]::Round((($items | Measure-Object material_char_count -Average).Average), 1) } else { 0 }
    $sentAvg = if ($cnt -gt 0) { [math]::Round((($items | Measure-Object material_sentence_count -Average).Average), 2) } else { 0 }
    $docs = ($items | Group-Object source_doc | Sort-Object Name | ForEach-Object { $_.Name }) -join ', '

    $pack = @"
# Distill Pack: $name

## Sample Stats
- sample_count: $cnt
- replaced_by_answer_count: $rep
- source_docs: $docs
- avg_material_len: $lenAvg
- avg_sentence_count: $sentAvg

## Consumable Fields
- material_rebuilt
- prompt_before_fill
- correct_option_text
- rebuild_replaced / rebuild_pattern
- option_a / option_b / option_c / option_d
- answer
- analysis
"@
    Set-Content -LiteralPath $packPath -Value $pack -Encoding utf8
}

Write-Output "Done. Output directory: $outputDir"
