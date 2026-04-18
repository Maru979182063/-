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
        if (-not $entry) {
            throw "word/document.xml not found: $DocxPath"
        }
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
        if (-not $dict.ContainsKey($k)) {
            $dict[$k] = $v
        }
    }
    return $dict
}

function Parse-Options {
    param([string]$Text)
    $out = @{ A = ""; B = ""; C = ""; D = "" }
    foreach ($m in [regex]::Matches($Text, "(?s)(?<k>[A-D])[\.\uFF0E\u3001]\s*(?<v>.*?)(?=(?:[A-D][\.\uFF0E\u3001])|$)")) {
        $k = $m.Groups["k"].Value
        $v = Normalize-Text $m.Groups["v"].Value
        $out[$k] = $v
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

function Get-AxisGuess {
    param([string]$Material)
    if ($Material -match ((U @(0x53EF,0x89C1)) + "|" + (U @(0x603B,0x4E4B)) + "|" + (U @(0x56E0,0x6B64)) + "|" + (U @(0x6240,0x4EE5)))) {
        return "final_summary"
    }
    if ($Material -match ((U @(0x4F46,0x662F)) + "|" + (U @(0x7136,0x800C)) + "|" + (U @(0x4E0D,0x8FC7)) + "|" + (U @(0x5374)))) {
        return "transition_after"
    }
    if ($Material -match ((U @(0x5E94,0x5F53)) + "|" + (U @(0x9700,0x8981)) + "|" + (U @(0x5FC5,0x987B)) + "|" + (U @(0x5EFA,0x8BAE)))) {
        return "solution_conclusion"
    }
    return "unknown"
}

function Get-StructureGuess {
    param([string]$Material)
    if ($Material -match (U @(0x4E00,0x65B9,0x9762)) -and $Material -match (U @(0x53E6,0x4E00,0x65B9,0x9762))) {
        return "parallel"
    }
    if ($Material -match ((U @(0x95EE,0x9898)) + "|" + (U @(0x56F0,0x5883)) + "|" + (U @(0x6311,0x6218)) + "|" + (U @(0x4E0D,0x8DB3))) -and
        $Material -match ((U @(0x5E94,0x5F53)) + "|" + (U @(0x9700,0x8981)) + "|" + (U @(0x5FC5,0x987B)) + "|" + (U @(0x5BF9,0x7B56)))) {
        return "problem_solution"
    }
    if ($Material -match ((U @(0x7EFC,0x4E0A)) + "|" + (U @(0x53EF,0x89C1)) + "|" + (U @(0x7531,0x6B64,0x53EF,0x89C1)))) {
        return "sub_total"
    }
    return "unknown"
}

function Get-CueCount {
    param([string]$Material)
    $cues = @()
    $cues += (U @(0x4F46,0x662F))
    $cues += (U @(0x7136,0x800C))
    $cues += (U @(0x4E0D,0x8FC7))
    $cues += (U @(0x5374))
    $cues += (U @(0x53EF,0x89C1))
    $cues += (U @(0x56E0,0x6B64))
    $cues += (U @(0x6240,0x4EE5))
    $cues += (U @(0x603B,0x4E4B))
    $cues += (U @(0x4E00,0x65B9,0x9762))
    $cues += (U @(0x53E6,0x4E00,0x65B9,0x9762))
    $cues += (U @(0x6B64,0x5916))
    $cues += (U @(0x540C,0x65F6))
    $cues += (U @(0x5E76,0x4E14))
    $cues += (U @(0x800C,0x4E14))
    $cues += (U @(0x56E0,0x800C))
    $count = 0
    foreach ($c in $cues) {
        $count += [regex]::Matches($Material, [regex]::Escape($c)).Count
    }
    return $count
}

$tagPaper = U @(0x6240,0x5C5E,0x8BD5,0x5377)
$tagBody = U @(0x9898,0x5E72)
$tagAnswer = U @(0x7B54,0x6848)
$tagAnalysis = U @(0x89E3,0x6790)
$tagSource = U @(0x6587,0x6BB5,0x51FA,0x5904)
$tagAccuracy = U @(0x6B63,0x786E,0x7387)
$tagWrong = U @(0x6613,0x9519,0x9879)
$tagPoints = U @(0x8003,0x70B9)
$familyName = U @(0x4E2D,0x5FC3,0x7406,0x89E3,0x9898)
$subfamilyName = U @(0x5173,0x8054,0x8BCD)
$qidLabel = U @(0x9898,0x53F7)
$fullColon = U @(0xFF1A)

$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$batchName = "center_understanding_relation_words_$BatchDate"
$outputDir = Join-Path $OutputRoot $batchName
New-Item -ItemType Directory -Path $outputDir -Force | Out-Null

$docxFiles = Get-ChildItem -LiteralPath $InputDir -Filter "*.docx" | Sort-Object Name
if (-not $docxFiles -or $docxFiles.Count -eq 0) {
    throw "No .docx found in $InputDir"
}

$rows = New-Object System.Collections.Generic.List[object]
foreach ($f in $docxFiles) {
    $patternTag = [System.IO.Path]::GetFileNameWithoutExtension($f.Name)
    $paras = Get-DocxParagraphs -DocxPath $f.FullName
    $raw = ($paras -join "`n")
    $blocks = [regex]::Split($raw, "(?m)(?=^\d+\.\s*" + [regex]::Escape($qidLabel) + [regex]::Escape($fullColon) + "#)")
    $fileAccepted = 0
    foreach ($blockRaw in $blocks) {
        $block = ($blockRaw -replace "`r", "").Trim()
        if ([string]::IsNullOrWhiteSpace($block)) { continue }
        if ($block -notmatch ([regex]::Escape($qidLabel) + [regex]::Escape($fullColon) + "#\d+")) { continue }
        $fileAccepted++

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
        $sentCount = 0
        if ($material.Length -gt 0) {
            $sentCount = (($material -split "[\u3002\uFF01\uFF1F!?;\uFF1B]" | Where-Object { $_.Trim().Length -gt 0 } | Measure-Object).Count)
        }
        $cueCount = Get-CueCount -Material $material
        $axisGuess = Get-AxisGuess -Material $material
        $structureGuess = Get-StructureGuess -Material $material

        $rows.Add([PSCustomObject]@{
            family = $familyName
            subfamily = $subfamilyName
            pattern_tag = $patternTag
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
            relation_cue_count = $cueCount
            axis_source_guess = $axisGuess
            structure_guess = $structureGuess
        })
    }
}

$allRows = $rows.ToArray()

function Write-Jsonl {
    param([object[]]$Items, [string]$Path)
    $lines = New-Object System.Collections.Generic.List[string]
    foreach ($item in $Items) {
        $lines.Add(($item | ConvertTo-Json -Depth 8 -Compress))
    }
    Set-Content -LiteralPath $Path -Value $lines -Encoding utf8
}

$jsonlPath = Join-Path $outputDir "material_samples.jsonl"
$csvPath = Join-Path $outputDir "material_samples.csv"
$summaryPath = Join-Path $outputDir "batch_summary.json"
$materialCardPath = Join-Path $outputDir "material_card_v1.md"
$itemPromptPath = Join-Path $outputDir "item_prompt_v1.md"
$executionPath = Join-Path $outputDir "execution_note.md"

Write-Jsonl -Items $allRows -Path $jsonlPath
$allRows | Export-Csv -LiteralPath $csvPath -NoTypeInformation -Encoding utf8

$total = $allRows.Count
$lengths = @($allRows | ForEach-Object { [int]$_.material_char_count })
$sentences = @($allRows | ForEach-Object { [int]$_.material_sentence_count })
$cues = @($allRows | ForEach-Object { [int]$_.relation_cue_count })

if ($total -gt 0) {
    $sortedLens = @($lengths | Sort-Object)
    $p25 = $sortedLens[[math]::Floor(($total - 1) * 0.25)]
    $p75 = $sortedLens[[math]::Floor(($total - 1) * 0.75)]
    $avgLen = [math]::Round((($lengths | Measure-Object -Average).Average), 1)
    $avgSent = [math]::Round((($sentences | Measure-Object -Average).Average), 2)
    $avgCue = [math]::Round((($cues | Measure-Object -Average).Average), 2)
} else {
    $p25 = 0; $p75 = 0; $avgLen = 0; $avgSent = 0; $avgCue = 0
}

$axisTop = $allRows | Group-Object axis_source_guess | Sort-Object Count -Descending | Select-Object -First 5 Name,Count
$structureTop = $allRows | Group-Object structure_guess | Sort-Object Count -Descending | Select-Object -First 5 Name,Count
$patternStats = $allRows | Group-Object pattern_tag | Sort-Object Name | ForEach-Object {
    $g = $_.Group
    [PSCustomObject]@{
        pattern_tag = $_.Name
        sample_count = $_.Count
        avg_material_len = [math]::Round((($g | Measure-Object material_char_count -Average).Average), 1)
        avg_sentence_count = [math]::Round((($g | Measure-Object material_sentence_count -Average).Average), 2)
        avg_cue_count = [math]::Round((($g | Measure-Object relation_cue_count -Average).Average), 2)
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
    avg_relation_cue_count = $avgCue
    axis_source_top = $axisTop
    structure_top = $structureTop
    pattern_stats = $patternStats
}
$summary | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $summaryPath -Encoding utf8

$materialCard = @"
# Material Card v1 (center_understanding - relation_words)

## Batch
- batch: $batchName
- source_dir: $InputDir
- total_samples: $total

## Preference
1. Length window: prefer [$p25, $p75] chars (avg $avgLen).
2. Sentence count: average $avgSent; keep full argument flow (suggest 4-9 sentences).
3. Cue density: average $avgCue; prefer explicit discourse links.
4. Axis retention: keep anchor sentence around transition or final-summary cues.
5. Register: explanatory/argumentative passages with stable central claim.

## Processing Rules
1. Minimal clean only: normalize spacing and line breaks, keep argument structure.
2. No premature summarization: do not collapse into short abstract.
3. Preserve chain: keep support and conclusion relation visible.
4. Keep sentence order: do not reorder evidence and conclusion.

## Numeric Features
1. material_char_count: target [$p25, $p75]
2. material_sentence_count: target [4, 9]
3. relation_cue_count: prefer >= 2
4. axis_source_guess: prioritize transition_after / final_summary / solution_conclusion
5. structure_guess: prioritize sub_total / problem_solution / parallel
"@
Set-Content -LiteralPath $materialCardPath -Value $materialCard -Encoding utf8

$itemPrompt = @"
# Item + Prompt v1 (center_understanding - relation_words)

## Item Behavior
1. Stem style: use stable center-understanding asks (main idea / intended point).
2. Correct option: full-passage abstraction; avoid local detail restatement.
3. Distractors: cover at least three error modes:
   - partial scope
   - local-detail-as-main-idea
   - topic shift / over-generalization
4. Analysis style: locate axis sentence first, then explain option-level elimination.

## Prompt Constraints
1. Provide axis_source_hint, structure_hint, and cue spans as explicit inputs.
2. Add hard checks before finalize:
   - correct option covers full axis
   - correct option not copied from one local sentence
   - no unsupported value escalation
3. Require distractor diversity; reject obviously irrelevant distractors.
4. Fix analysis template into three blocks:
   - axis locate
   - correct option mapping
   - distractor elimination

## Scope
1. Done in this batch: extraction, material distillation, item/prompt reverse mapping.
2. Not done in this batch: racing rewrite, full validator retrofit, difficulty-card reclaim.
3. Freeze set: not provided in this package.
"@
Set-Content -LiteralPath $itemPromptPath -Value $itemPrompt -Encoding utf8

$executionNote = @"
# Execution Note

1. Input: $InputDir
2. Output: $outputDir
3. Files:
   - material_samples.jsonl
   - material_samples.csv
   - batch_summary.json
   - material_card_v1.md
   - item_prompt_v1.md
4. Method:
   - parse docx XML directly
   - split by question-id blocks
   - extract body/answer/analysis/tags
   - compute basic material signals
"@
Set-Content -LiteralPath $executionPath -Value $executionNote -Encoding utf8

Write-Output "Done. Output directory: $outputDir"
