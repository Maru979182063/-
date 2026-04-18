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

function Resolve-CorrectOrderRaw {
    param([string]$AnswerRaw, [hashtable]$Options)
    $ans = (Normalize-Text $AnswerRaw).ToUpper()
    if ([string]::IsNullOrWhiteSpace($ans)) { return "" }
    if ($ans -match '^[A-D]$') { return [string]$Options[$ans] }
    if ($ans -match '^[A-D][\.\uFF0E\u3001]?$') {
        $k = $ans.Substring(0, 1)
        return [string]$Options[$k]
    }
    return (Normalize-Text $AnswerRaw)
}

function Extract-OrderedUnits {
    param([string]$PromptText)
    $prompt = Normalize-Text $PromptText
    $markers = @([string][char]0x2460,[string][char]0x2461,[string][char]0x2462,[string][char]0x2463,[string][char]0x2464,[string][char]0x2465,[string][char]0x2466,[string][char]0x2467,[string][char]0x2468,[string][char]0x2469)
    $units = @{}
    $ids = New-Object System.Collections.Generic.List[string]
    $currentId = ""
    $buffer = New-Object System.Text.StringBuilder
    foreach ($ch in $prompt.ToCharArray()) {
        $s = [string]$ch
        if ($markers -contains $s) {
            if (-not [string]::IsNullOrWhiteSpace($currentId)) {
                $txt = Normalize-Text ($buffer.ToString())
                if (-not [string]::IsNullOrWhiteSpace($txt) -and -not $units.ContainsKey($currentId)) {
                    $units[$currentId] = $txt
                    $ids.Add($currentId)
                }
            }
            $currentId = $s
            $null = $buffer.Clear()
            continue
        }
        if (-not [string]::IsNullOrWhiteSpace($currentId)) { $null = $buffer.Append($s) }
    }
    if (-not [string]::IsNullOrWhiteSpace($currentId)) {
        $txt = Normalize-Text ($buffer.ToString())
        if (-not [string]::IsNullOrWhiteSpace($txt) -and -not $units.ContainsKey($currentId)) {
            $units[$currentId] = $txt
            $ids.Add($currentId)
        }
    }
    return @{ units = $units; ids = @($ids) }
}

function Parse-OrderSequence {
    param([string]$OrderRaw)
    $raw = Normalize-Text $OrderRaw
    $markers = @([string][char]0x2460,[string][char]0x2461,[string][char]0x2462,[string][char]0x2463,[string][char]0x2464,[string][char]0x2465,[string][char]0x2466,[string][char]0x2467,[string][char]0x2468,[string][char]0x2469)
    $seq = New-Object System.Collections.Generic.List[string]
    foreach ($ch in $raw.ToCharArray()) {
        $s = [string]$ch
        if ($markers -contains $s) { $seq.Add($s) }
    }
    return @($seq)
}

function Rebuild-MaterialByOrder {
    param([hashtable]$Units, [string[]]$Sequence)
    $seqArr = @($Sequence)
    if ($null -eq $Units -or @($Units.Keys).Count -eq 0) { return @{ text = ""; ok = $false; reason = "no_units" } }
    if ($null -eq $Sequence -or $seqArr.Count -eq 0) { return @{ text = ""; ok = $false; reason = "no_sequence" } }
    $parts = New-Object System.Collections.Generic.List[string]
    foreach ($id in $seqArr) {
        if (-not $Units.ContainsKey($id)) { return @{ text = ""; ok = $false; reason = "sequence_id_missing:$id" } }
        $parts.Add((Normalize-Text ([string]$Units[$id])))
    }
    return @{ text = (Normalize-Text (($parts -join " "))); ok = $true; reason = "ok" }
}

function Write-Jsonl {
    param([object[]]$Items, [string]$Path)
    $lines = New-Object System.Collections.Generic.List[string]
    foreach ($item in $Items) { $lines.Add(($item | ConvertTo-Json -Depth 10 -Compress)) }
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

$familyName = U @(0x8BED,0x53E5,0x6392,0x5E8F,0x9898)
$subfamilyName = U @(0x786E,0x5B9A,0x987A,0x5E8F)

$nameTime = U @(0x65E5,0x5E38,0x903B,0x8F91,0x002D,0x65F6,0x95F4,0x8109,0x7EDC)
$nameAction = U @(0x65E5,0x5E38,0x903B,0x8F91,0x002D,0x884C,0x52A8,0x987A,0x5E8F)
$nameViewExplain = U @(0x884C,0x6587,0x903B,0x8F91,0x002D,0x89C2,0x70B9,0x002B,0x89E3,0x91CA,0x8BF4,0x660E)
$nameOther = U @(0x884C,0x6587,0x903B,0x8F91,0x002D,0x5176,0x4ED6)
$nameQA = U @(0x884C,0x6587,0x903B,0x8F91,0x002D,0x63D0,0x95EE,0x002B,0x56DE,0x7B54)
$namePS = U @(0x884C,0x6587,0x903B,0x8F91,0x002D,0x95EE,0x9898,0x002B,0x5BF9,0x7B56)

$batchName = "sentence_order_sequence_$BatchDate"
$outputDir = Join-Path $OutputRoot $batchName
New-Item -ItemType Directory -Path $outputDir -Force | Out-Null

$docxFiles = @(Get-ChildItem -LiteralPath $InputDir -Filter "*.docx" | Where-Object {
    $base = [IO.Path]::GetFileNameWithoutExtension($_.Name)
    ($base -eq $nameTime) -or
    ($base -eq $nameAction) -or
    ($base -eq $nameViewExplain) -or
    ($base -eq $nameOther) -or
    ($base -eq $nameQA) -or
    ($base -eq $namePS)
} | Sort-Object Name)
if (-not $docxFiles -or $docxFiles.Count -eq 0) { throw "No target docx found in $InputDir" }

$rows = New-Object System.Collections.Generic.List[object]
foreach ($f in $docxFiles) {
    $patternTag = [IO.Path]::GetFileNameWithoutExtension($f.Name)
    $leafGuess = "sequence_other"
    if ($patternTag -eq $nameTime) { $leafGuess = "daily_time_timeline" }
    elseif ($patternTag -eq $nameAction) { $leafGuess = "daily_action_order" }
    elseif ($patternTag -eq $nameViewExplain) { $leafGuess = "writing_view_explain" }
    elseif ($patternTag -eq $nameOther) { $leafGuess = "writing_other" }
    elseif ($patternTag -eq $nameQA) { $leafGuess = "writing_qa" }
    elseif ($patternTag -eq $namePS) { $leafGuess = "writing_problem_solution" }

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
        $opts = Parse-Options -Text $optionText

        $orderRaw = Resolve-CorrectOrderRaw -AnswerRaw $answer -Options $opts
        $parsedUnits = Extract-OrderedUnits -PromptText $promptText
        $units = $parsedUnits.units
        $sequence = Parse-OrderSequence -OrderRaw $orderRaw
        $rebuilt = Rebuild-MaterialByOrder -Units $units -Sequence $sequence
        $material = [string]$rebuilt.text

        $unitCount = if ($null -eq $units) { 0 } else { @($units.Keys).Count }
        $seqLen = if ($null -eq $sequence) { 0 } else { @($sequence).Count }
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
            prompt_before_reorder = $promptText
            correct_order_raw = $orderRaw
            order_sequence = $sequence
            units = $units
            unit_count = $unitCount
            sequence_count = $seqLen
            rebuild_ok = [bool]$rebuilt.ok
            rebuild_reason = [string]$rebuilt.reason
            material_reordered = $material
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
        })
    }
}

$allRows = $rows.ToArray()
$jsonlPath = Join-Path $outputDir "material_samples_reordered.jsonl"
$csvPath = Join-Path $outputDir "material_samples_reordered.csv"
$summaryPath = Join-Path $outputDir "batch_summary.json"
$overviewPath = Join-Path $outputDir "distill_pack_overview.md"

Write-Jsonl -Items $allRows -Path $jsonlPath
$allRows | Export-Csv -LiteralPath $csvPath -NoTypeInformation -Encoding utf8

$total = $allRows.Count
$okCount = @($allRows | Where-Object { $_.rebuild_ok -eq $true }).Count
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
        rebuild_ok_count = @($g | Where-Object { $_.rebuild_ok -eq $true }).Count
        avg_unit_count = [math]::Round((($g | Measure-Object unit_count -Average).Average), 2)
        avg_seq_count = [math]::Round((($g | Measure-Object sequence_count -Average).Average), 2)
        avg_material_len = [math]::Round((($g | Measure-Object material_char_count -Average).Average), 1)
        avg_sentence_count = [math]::Round((($g | Measure-Object material_sentence_count -Average).Average), 2)
    }
}

$summary = [PSCustomObject]@{
    batch = $batchName
    generated_at = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
    input_dir = $InputDir
    taxonomy = @{
        mother_family = "sentence_order"
        subfamily = "sequence"
    }
    total_samples = $total
    rebuilt_by_answer_order_count = $okCount
    avg_material_len = $avgLen
    material_len_p25 = $p25
    material_len_p75 = $p75
    avg_sentence_count = $avgSent
    leaf_stats = $leafStats
}
$summary | ConvertTo-Json -Depth 10 | Set-Content -LiteralPath $summaryPath -Encoding utf8

$overview = @"
# Distill Pack Overview (sentence_order / sequence)

## Method
- Build ordered units from prompt text (e.g. ①②③...).
- Resolve correct order from answer (letter -> option text if needed).
- Reconstruct material strictly by correct answer sequence.

## Batch
- batch: $batchName
- total_samples: $total
- rebuilt_by_answer_order_count: $okCount
- avg_material_len: $avgLen
- length_window(p25,p75): [$p25, $p75]
- avg_sentence_count: $avgSent

## Files
- material_samples_reordered.jsonl
- material_samples_reordered.csv
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
    $ok = @($items | Where-Object { $_.rebuild_ok -eq $true }).Count
    $lenAvg = if ($cnt -gt 0) { [math]::Round((($items | Measure-Object material_char_count -Average).Average), 1) } else { 0 }
    $sentAvg = if ($cnt -gt 0) { [math]::Round((($items | Measure-Object material_sentence_count -Average).Average), 2) } else { 0 }
    $unitAvg = if ($cnt -gt 0) { [math]::Round((($items | Measure-Object unit_count -Average).Average), 2) } else { 0 }
    $docs = ($items | Group-Object source_doc | Sort-Object Name | ForEach-Object { $_.Name }) -join ", "

    $pack = @"
# Distill Pack: $name

## Sample Stats
- sample_count: $cnt
- rebuild_ok_count: $ok
- source_docs: $docs
- avg_unit_count: $unitAvg
- avg_material_len: $lenAvg
- avg_sentence_count: $sentAvg

## Consumable Fields
- material_reordered
- units
- order_sequence
- correct_order_raw
- rebuild_ok / rebuild_reason
- option_a / option_b / option_c / option_d
- answer
- analysis
"@
    Set-Content -LiteralPath $packPath -Value $pack -Encoding utf8
}

Write-Output "Done. Output directory: $outputDir"
