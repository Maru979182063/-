param(
  [string]$PackDir = "C:\Users\Maru\Desktop\新建文件夹 (3)",
  [string]$OutRoot = "C:\Users\Maru\Documents\agent\reports\distill_runs"
)

$ErrorActionPreference = "Stop"

function Write-Log {
  param(
    [string]$Message,
    [string]$LogPath
  )
  $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
  "$ts`t$Message" | Out-File -LiteralPath $LogPath -Encoding utf8 -Append
}

function Save-Json {
  param(
    [object]$Data,
    [string]$Path
  )
  $Data | ConvertTo-Json -Depth 20 | Out-File -LiteralPath $Path -Encoding utf8
}

function Parse-Options {
  param([string]$OptionsText)
  $map = @{}
  if ([string]::IsNullOrWhiteSpace($OptionsText)) { return $map }
  $norm = $OptionsText -replace "`r`n", " " -replace "`n", " "
  $matches = [regex]::Matches($norm, "([A-D])[\.、．:：]\s*(.*?)(?=\s*[A-D][\.、．:：]|$)")
  foreach ($m in $matches) {
    $map[$m.Groups[1].Value] = $m.Groups[2].Value.Trim()
  }
  return $map
}

function Reconstruct-Fill {
  param(
    [string]$Text,
    [string]$AnswerText
  )
  if ([string]::IsNullOrEmpty($Text)) { return $Text }
  if ($Text -match "_{3,}") {
    return [regex]::Replace(
      $Text,
      "_{3,}",
      [System.Text.RegularExpressions.MatchEvaluator]{ param($m) $AnswerText },
      1
    )
  }
  $pattern = "[\u00A0\s]{4,}"
  if ([regex]::IsMatch($Text, $pattern)) {
    return [regex]::Replace(
      $Text,
      $pattern,
      [System.Text.RegularExpressions.MatchEvaluator]{ param($m) $AnswerText },
      1
    )
  }
  return $Text
}

function Reconstruct-Order {
  param(
    [string]$Text,
    [string]$Sequence
  )
  $lineMap = @{}
  $lines = $Text -split "`r?`n"
  foreach ($ln in $lines) {
    $t = $ln.Trim()
    if ($t.Length -eq 0) { continue }
    $first = [int][char]$t[0]
    if ($first -ge 9312 -and $first -le 9331) {
      $lineMap[$t.Substring(0, 1)] = $t.Substring(1).Trim()
      continue
    }
    if ($t -match "^(\d{1,2})[\.、．]\s*(.+)$") {
      $lineMap[$matches[1]] = $matches[2].Trim()
    }
  }

  if ([string]::IsNullOrWhiteSpace($Sequence)) { return $Text }

  $parts = @()
  $chars = $Sequence.ToCharArray() | ForEach-Object { [string]$_ }
  foreach ($ch in $chars) {
    if ($lineMap.ContainsKey($ch)) { $parts += $lineMap[$ch] }
  }
  if ($parts.Count -gt 0) { return ($parts -join "`n") }
  return $Text
}

function Get-Sha256Hex {
  param([string]$Text)
  $sha = [System.Security.Cryptography.SHA256]::Create()
  try {
    $bytes = [System.Text.Encoding]::UTF8.GetBytes($Text)
    $hash = $sha.ComputeHash($bytes)
    return [System.BitConverter]::ToString($hash).Replace("-", "").ToLowerInvariant()
  }
  finally {
    $sha.Dispose()
  }
}

$now = Get-Date
$runId = $now.ToString("yyyyMMdd_HHmmss")
$runDir = Join-Path $OutRoot ("truth_material_distill_" + $runId)
$logDir = Join-Path $runDir "logs"
$dataDir = Join-Path $runDir "data"
$trainDir = Join-Path $dataDir "train"
$testDir = Join-Path $dataDir "test"
New-Item -ItemType Directory -Path $logDir -Force | Out-Null
New-Item -ItemType Directory -Path $trainDir -Force | Out-Null
New-Item -ItemType Directory -Path $testDir -Force | Out-Null
$mainLog = Join-Path $logDir "run.log"

Write-Log -Message "start run_id=$runId" -LogPath $mainLog

$expectedFiles = @(
  "center_understanding.json",
  "sentence_order.json",
  "sentence_fill.json"
)
$expectedFamilies = @(
  "center_understanding",
  "sentence_order",
  "sentence_fill"
)

$context = [ordered]@{
  run_id = $runId
  run_time = $now.ToString("yyyy-MM-dd HH:mm:ss")
  pack_dir = $PackDir
  out_dir = $runDir
  cards = $expectedFamilies
}
Save-Json -Data $context -Path (Join-Path $logDir "00_context.json")
Write-Log -Message "context saved" -LogPath $mainLog

$all = @()
$fileStats = @()
foreach ($file in $expectedFiles) {
  $path = Join-Path $PackDir $file
  if (-not (Test-Path -LiteralPath $path)) {
    throw "missing input file: $path"
  }
  $arr = Get-Content -LiteralPath $path -Encoding UTF8 | ConvertFrom-Json
  $fileStats += [pscustomobject]@{
    file = $file
    count = @($arr).Count
  }
  foreach ($it in $arr) {
    $all += $it
  }
}

$validation = [ordered]@{
  files = $fileStats
  total_items = @($all).Count
  families = @($all | Group-Object family | ForEach-Object {
      [ordered]@{ family = $_.Name; count = $_.Count }
    })
}
Save-Json -Data $validation -Path (Join-Path $logDir "01_input_validation.json")
Write-Log -Message "input validation saved total=$($validation.total_items)" -LogPath $mainLog

$processed = @()
foreach ($it in $all) {
  $family = [string]$it.family
  if ($expectedFamilies -notcontains $family) { continue }
  $orig = [string]$it.recreated_material
  $answer = [string]$it.gold_answer
  $options = Parse-Options -OptionsText ([string]$it.options)
  $answerText = ""
  if ($options.ContainsKey($answer)) { $answerText = [string]$options[$answer] }

  $material = $orig
  $mode = "direct_material_as_raw"
  if ($family -eq "sentence_fill") {
    $material = Reconstruct-Fill -Text $orig -AnswerText $answerText
    $mode = "reverse_fill_by_gold_answer"
  }
  elseif ($family -eq "sentence_order") {
    $sequence = ""
    if ($options.ContainsKey($answer)) { $sequence = [string]$options[$answer] }
    $material = Reconstruct-Order -Text $orig -Sequence $sequence
    $mode = "reverse_reorder_by_gold_answer"
  }

  $split = [string]$it.suggested_split
  if ($split -ne "train" -and $split -ne "test") {
    $split = "train"
  }

  $processed += [pscustomobject]@{
    family = $family
    family_cn = [string]$it.family_cn
    split = $split
    source_qid = [string]$it.source_qid
    source_doc = [string]$it.source_doc
    doc_question_no = [string]$it.doc_question_no
    source_exam = [string]$it.source_exam
    question_stem = [string]$it.question_stem
    options = [string]$it.options
    gold_answer = $answer
    gold_analysis = [string]$it.gold_analysis
    pattern_tags = [string]$it.pattern_tags
    correct_rate_pct = [string]$it.correct_rate_pct
    easy_wrong_option = [string]$it.easy_wrong_option
    reconstruction_mode = $mode
    reconstruction_changed = ($material -ne $orig)
    material_text = $material
    material_sha256 = (Get-Sha256Hex -Text $material)
    material_char_len = $material.Length
    material_line_count = (@($material -split "`r?`n")).Count
  }
}

$train = @($processed | Where-Object { $_.split -eq "train" })
$test = @($processed | Where-Object { $_.split -eq "test" })

$splitSummary = [ordered]@{
  total = @($processed).Count
  train = @($train).Count
  test = @($test).Count
  by_family = @(
    $expectedFamilies | ForEach-Object {
      $fam = $_
      [ordered]@{
        family = $fam
        train = @($train | Where-Object { $_.family -eq $fam }).Count
        test = @($test | Where-Object { $_.family -eq $fam }).Count
      }
    }
  )
}
Save-Json -Data $splitSummary -Path (Join-Path $logDir "02_split_summary.json")
Write-Log -Message "split summary saved train=$($splitSummary.train) test=$($splitSummary.test)" -LogPath $mainLog

$qidCrossSplit = @(
  $processed |
    Group-Object source_qid |
    Where-Object { ($_.Group | Select-Object -ExpandProperty split -Unique).Count -gt 1 } |
    ForEach-Object { $_.Name }
)
$materialCrossSplit = @(
  $processed |
    Group-Object material_sha256 |
    Where-Object {
      ($_.Group | Select-Object -ExpandProperty split -Unique).Count -gt 1 -and
      -not [string]::IsNullOrWhiteSpace($_.Name)
    } |
    ForEach-Object { $_.Name }
)
$examCrossSplit = @(
  $processed |
    Group-Object source_exam |
    Where-Object {
      -not [string]::IsNullOrWhiteSpace($_.Name) -and
      ($_.Group | Select-Object -ExpandProperty split -Unique).Count -gt 1
    } |
    ForEach-Object { $_.Name }
)

$leakCheck = [ordered]@{
  qid_cross_split_count = @($qidCrossSplit).Count
  qid_cross_split = $qidCrossSplit
  material_hash_cross_split_count = @($materialCrossSplit).Count
  material_hash_cross_split = $materialCrossSplit
  source_exam_cross_split_count = @($examCrossSplit).Count
  source_exam_cross_split = $examCrossSplit
}
Save-Json -Data $leakCheck -Path (Join-Path $logDir "03_leakage_checks.json")
Write-Log -Message "leak check saved qid=$($leakCheck.qid_cross_split_count) mat=$($leakCheck.material_hash_cross_split_count) exam=$($leakCheck.source_exam_cross_split_count)" -LogPath $mainLog

$trainCsv = Join-Path $trainDir "distill_train.csv"
$trainJsonl = Join-Path $trainDir "distill_train.jsonl"
$testCsv = Join-Path $testDir "distill_test.csv"
$testJsonl = Join-Path $testDir "distill_test.jsonl"

$train | Export-Csv -LiteralPath $trainCsv -NoTypeInformation -Encoding UTF8
$test | Export-Csv -LiteralPath $testCsv -NoTypeInformation -Encoding UTF8

$trainJsonLines = $train | ForEach-Object { $_ | ConvertTo-Json -Compress -Depth 10 }
$trainJsonLines | Out-File -LiteralPath $trainJsonl -Encoding utf8
$testJsonLines = $test | ForEach-Object { $_ | ConvertTo-Json -Compress -Depth 10 }
$testJsonLines | Out-File -LiteralPath $testJsonl -Encoding utf8

foreach ($fam in $expectedFamilies) {
  $famTrain = @($train | Where-Object { $_.family -eq $fam })
  $famTest = @($test | Where-Object { $_.family -eq $fam })
  $famTrainCsv = Join-Path $trainDir ("distill_train_" + $fam + ".csv")
  $famTestCsv = Join-Path $testDir ("distill_test_" + $fam + ".csv")
  $famTrain | Export-Csv -LiteralPath $famTrainCsv -NoTypeInformation -Encoding UTF8
  $famTest | Export-Csv -LiteralPath $famTestCsv -NoTypeInformation -Encoding UTF8
}
Write-Log -Message "exports completed" -LogPath $mainLog

$manifest = [ordered]@{
  run_id = $runId
  purpose = "truth-material-distillation-split-and-rebuild"
  cards = $expectedFamilies
  input_pack = $PackDir
  output_dir = $runDir
  outputs = [ordered]@{
    train_csv = $trainCsv
    train_jsonl = $trainJsonl
    test_csv = $testCsv
    test_jsonl = $testJsonl
    logs = $logDir
  }
  counts = $splitSummary
  leakage = $leakCheck
}
Save-Json -Data $manifest -Path (Join-Path $runDir "distill_manifest.json")
Write-Log -Message "manifest saved" -LogPath $mainLog

$summaryMd = Join-Path $runDir "distill_summary.md"
$md = @()
$md += "# 真题材料蒸馏运行摘要"
$md += ""
$md += "- run_id: $runId"
$md += "- 输入包: $PackDir"
$md += "- 题卡: center_understanding / sentence_order / sentence_fill"
$md += "- 样本总数: $($splitSummary.total)"
$md += "- 训练集: $($splitSummary.train)"
$md += "- 测试集: $($splitSummary.test)"
$md += ""
$md += "## 各题卡拆分"
foreach ($x in $splitSummary.by_family) {
  $familyName = [string]$x.family
  $trainCount = [string]$x.train
  $testCount = [string]$x.test
  $line = "- " + $familyName
  $line += " train=" + $trainCount
  $line += ", test=" + $testCount
  $md += $line
}
$md += ""
$md += "## 泄漏检查"
$md += "- qid 跨集合: $($leakCheck.qid_cross_split_count)"
$md += "- 材料哈希跨集合: $($leakCheck.material_hash_cross_split_count)"
$md += "- source_exam 跨集合: $($leakCheck.source_exam_cross_split_count)"
$md += ""
$md += "## 关键文件"
$md += "- 训练集: $trainCsv"
$md += "- 测试集: $testCsv"
$md += "- 清单: $(Join-Path $runDir 'distill_manifest.json')"
$md += "- 日志目录: $logDir"
$md | Out-File -LiteralPath $summaryMd -Encoding utf8
Write-Log -Message "summary generated" -LogPath $mainLog

Write-Output "RUN_DIR=$runDir"
Write-Output "MANIFEST=$(Join-Path $runDir 'distill_manifest.json')"
Write-Output "SUMMARY=$summaryMd"
