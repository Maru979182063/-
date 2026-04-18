Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$root = "C:\Users\Maru\Documents\agent\reports\distill_batches"

function Read-Jsonl([string]$path) {
  $rows = @()
  if (-not (Test-Path $path)) { return ,$rows }
  Get-Content -Path $path -Encoding UTF8 | ForEach-Object {
    if ([string]::IsNullOrWhiteSpace($_)) { return }
    try { $rows += ($_ | ConvertFrom-Json) } catch {}
  }
  return ,$rows
}

function Q([object]$v) {
  if ($null -eq $v) { return '""' }
  $s = [string]$v
  $s = $s.Replace("\", "\\").Replace('"', '\"')
  return '"' + $s + '"'
}

function Clip([string]$s, [int]$n = 80) {
  if ([string]::IsNullOrWhiteSpace($s)) { return "" }
  $t = ($s -replace "\s+", " ").Trim()
  if ($t.Length -le $n) { return $t }
  return $t.Substring(0, $n) + "..."
}

function Inc([hashtable]$m, [string]$k) {
  if ([string]::IsNullOrWhiteSpace($k)) { return }
  if (-not $m.ContainsKey($k)) { $m[$k] = 0 }
  $m[$k] = [int]$m[$k] + 1
}

function Top([hashtable]$m, [int]$n = 3) {
  return @($m.GetEnumerator() | Sort-Object -Property Value -Descending | Select-Object -First $n)
}

function Pctl([double[]]$arr, [double]$p) {
  if ($arr.Count -eq 0) { return 0 }
  $s = @($arr | Sort-Object)
  $idx = [int][math]::Round(($s.Count - 1) * $p / 100.0, 0)
  return [int]$s[$idx]
}

function Detect-QType([string]$batchName) {
  $x = $batchName.ToLowerInvariant()
  if ($x.Contains("sentence_fill")) { return "sentence_fill" }
  if ($x.Contains("sentence_order")) { return "sentence_order" }
  return "main_idea"
}

function Pick-MaterialFile([string]$folder) {
  foreach ($n in @(
      "material_samples_rebuilt.jsonl",
      "material_samples_reordered.jsonl",
      "material_samples_cleaned.jsonl",
      "material_samples.jsonl"
    )) {
    $p = Join-Path $folder $n
    if (Test-Path $p) { return $p }
  }
  return $null
}

function Node-Id([object]$r) {
  foreach ($k in @("pattern_tag", "subfamily_id", "leaf_guess", "subfamily")) {
    if ($r.PSObject.Properties.Name -contains $k) {
      $v = [string]$r.$k
      if (-not [string]::IsNullOrWhiteSpace($v)) { return $v.Trim() }
    }
  }
  return "unlabeled_node"
}

function Stem-Text([object]$r, [string]$qt) {
  if ($r.PSObject.Properties.Name -contains "stem" -and -not [string]::IsNullOrWhiteSpace([string]$r.stem)) {
    return ([string]$r.stem).Trim()
  }
  if ($qt -eq "sentence_fill") { return "Fill the blank with the most suitable option." }
  if ($qt -eq "sentence_order") { return "Reorder the 6 units and choose the correct sequence." }
  return "What is the central meaning of the passage?"
}

function Pick-Material([object]$r, [hashtable]$truthByQid) {
  $qid = if ($r.PSObject.Properties.Name -contains "question_id") { [string]$r.question_id } else { "" }
  if (-not [string]::IsNullOrWhiteSpace($qid) -and $truthByQid.ContainsKey($qid)) {
    return [string]$truthByQid[$qid]
  }
  foreach ($k in @("material", "material_rebuilt", "material_reordered", "material_text", "prompt_before_fill", "prompt_before_reorder")) {
    if ($r.PSObject.Properties.Name -contains $k) {
      $v = [string]$r.$k
      if (-not [string]::IsNullOrWhiteSpace($v)) { return $v }
    }
  }
  return ""
}

function Sent-Count([string]$text, [object]$fallback) {
  if ($null -ne $fallback -and [int]$fallback -gt 0) { return [int]$fallback }
  if ([string]::IsNullOrWhiteSpace($text)) { return 0 }
  $c = [regex]::Matches($text, "[。！？!?；;]").Count
  if ($c -gt 0) { return $c }
  return 1
}

function Wrong-Type([string]$qt, [string]$w, [string]$c, [string]$corrSeq) {
  if ($qt -eq "sentence_order") {
    if ($w -match "①|②|③|④|⑤|⑥|[1-6]") {
      if ($w -notmatch [regex]::Escape($corrSeq)) { return "sequence_mismatch" }
      return "near_order_noise"
    }
    return "non_sequence_noise"
  }
  if ($qt -eq "sentence_fill") {
    if ($w.Length -gt [Math]::Max(12, [int]($c.Length * 1.6))) { return "over_expansion" }
    return "local_fit_global_miss"
  }
  if ($w.Length -gt [Math]::Max(10, [int]($c.Length * 1.5))) { return "scope_too_wide" }
  if ($w.Length -lt [Math]::Max(4, [int]($c.Length * 0.6))) { return "scope_too_narrow" }
  if ($w -match "\d") { return "detail_as_main" }
  return "functional_drift"
}

function Clue-Tags([string]$qt, [string]$a) {
  $tags = @()
  if ($qt -eq "sentence_fill") {
    $tags += "slot_validation"
    $tags += "context_coherence"
  } elseif ($qt -eq "sentence_order") {
    $tags += "head_tail_check"
    $tags += "binding_chain"
  } else {
    $tags += "central_alignment"
    $tags += "structure_anchor"
  }
  if (-not [string]::IsNullOrWhiteSpace($a) -and $a.Length -gt 120) { $tags += "distractor_elimination" }
  return @($tags | Select-Object -Unique)
}

function Hint([string]$qt, [string]$node) {
  if ($qt -eq "sentence_fill") {
    return @("slot_boundary", "context_dependency", "slot_fill", "keep_semantic_equivalence")
  }
  if ($qt -eq "sentence_order") {
    return @("six_unit_chain", "head_tail_plus_binding", "chain_ordering", "no_random_permutation")
  }
  return @("single_center", "center_plus_task", "center_extraction", "scope_and_function_distractor")
}

function Parse-W2Meta([string]$path) {
  $meta = [ordered]@{ business_family_id = ""; question_type = ""; business_subtype = "" }
  if (-not (Test-Path $path)) { return [pscustomobject]$meta }
  $line = Get-Content -Path $path -Encoding UTF8 -TotalCount 1
  try {
    $j = $line | ConvertFrom-Json
    if ($j.material_card) { $meta.business_family_id = [string]$j.material_card.business_family_id }
    if ($j.question_card_skeleton) {
      $meta.question_type = [string]$j.question_card_skeleton.question_type
      $meta.business_subtype = [string]$j.question_card_skeleton.business_subtype
    }
  } catch {}
  return [pscustomobject]$meta
}

function Parse-AllowSignals([string]$path) {
  $signals = @()
  if (-not (Test-Path $path)) { return ,$signals }
  $inside = $false
  Get-Content -Path $path -Encoding UTF8 | ForEach-Object {
    if ($_ -match "^\s*allowed_signal_keys:\s*$") { $inside = $true; return }
    if ($inside -and $_ -match "^\s*[a-zA-Z_]") { $inside = $false }
    if ($inside -and $_ -match "^\s*-\s*(.+?)\s*$") { $signals += $matches[1].Trim() }
  }
  return ,$signals
}

$summary = @()
$dirs = Get-ChildItem -Path $root -Directory | Sort-Object -Property Name

foreach ($d in $dirs) {
  $folder = $d.FullName
  $materialFile = Pick-MaterialFile -folder $folder
  if ($null -eq $materialFile) {
    $summary += [pscustomobject]@{ batch = $d.Name; status = "skip_no_material_samples"; sample_count = 0 }
    continue
  }

  $clean = Join-Path $folder "cleaned_truth_materials.jsonl"
  $w2 = Join-Path $folder "window2_mapped_output.jsonl"
  $en = Join-Path $folder "window2_mapped_output.enriched.v2.jsonl"
  $allow = Join-Path $folder "signal_layer_allowlist.yaml"

  $meta = Parse-W2Meta -path $w2
  $qt = if ([string]::IsNullOrWhiteSpace($meta.question_type)) { Detect-QType -batchName $d.Name } else { [string]$meta.question_type }
  $bs = [string]$meta.business_subtype
  if ([string]::IsNullOrWhiteSpace($bs) -and $qt -eq "main_idea") { $bs = "center_understanding" }
  $bf = [string]$meta.business_family_id
  if ([string]::IsNullOrWhiteSpace($bf)) { $bf = if ($qt -eq "main_idea") { "center_understanding" } else { $qt } }

  $truth = Read-Jsonl -path $clean
  $truthByQid = @{}
  foreach ($t in $truth) {
    if ($t.PSObject.Properties.Name -contains "question_id") {
      $qid = [string]$t.question_id
      if (-not [string]::IsNullOrWhiteSpace($qid) -and -not $truthByQid.ContainsKey($qid)) {
        $truthByQid[$qid] = [string]$t.material_text
      }
    }
  }

  $rows = Read-Jsonl -path $materialFile
  $derived = @()
  foreach ($r in $rows) {
    $node = Node-Id -r $r
    $mat = Pick-Material -r $r -truthByQid $truthByQid
    $stem = Stem-Text -r $r -qt $qt
    $ans = if ($r.PSObject.Properties.Name -contains "answer") { ([string]$r.answer).Trim().ToUpperInvariant() } else { "A" }
    if ($ans -notmatch "^[ABCD]$") { $ans = "A" }

    $opt = @{ A = ""; B = ""; C = ""; D = "" }
    foreach ($L in @("A", "B", "C", "D")) {
      $f = "option_" + $L.ToLowerInvariant()
      if ($r.PSObject.Properties.Name -contains $f) { $opt[$L] = [string]$r.$f }
    }
    $corr = if ($r.PSObject.Properties.Name -contains "correct_option_text" -and -not [string]::IsNullOrWhiteSpace([string]$r.correct_option_text)) { [string]$r.correct_option_text } else { [string]$opt[$ans] }
    $corrSeq = if ($r.PSObject.Properties.Name -contains "correct_order_raw") { [string]$r.correct_order_raw } else { "" }
    $ana = if ($r.PSObject.Properties.Name -contains "analysis") { [string]$r.analysis } else { "" }
    $fb = if ($r.PSObject.Properties.Name -contains "material_sentence_count") { $r.material_sentence_count } else { $null }
    $sc = Sent-Count -text $mat -fallback $fb

    $wt = @()
    foreach ($L in @("A", "B", "C", "D")) {
      if ($L -eq $ans) { continue }
      $wtxt = [string]$opt[$L]
      if ([string]::IsNullOrWhiteSpace($wtxt)) { continue }
      $wt += (Wrong-Type -qt $qt -w $wtxt -c $corr -corrSeq $corrSeq)
    }

    $qid = if ($r.PSObject.Properties.Name -contains "question_id") { [string]$r.question_id } else { "" }
    $derived += [pscustomobject]@{
      qid = $qid
      node = $node
      mat = $mat
      stem = $stem
      ans = $ans
      ana = $ana
      len = ($mat -replace "\s+", "").Length
      sc = $sc
      wt = @($wt)
      cl = @(Clue-Tags -qt $qt -a $ana)
    }
  }

  $n = $derived.Count
  $lens = @($derived | ForEach-Object { [double]$_.len })
  $scs = @($derived | ForEach-Object { [double]$_.sc })
  $l25 = Pctl -arr $lens -p 25
  $l50 = Pctl -arr $lens -p 50
  $l75 = Pctl -arr $lens -p 75
  $s25 = Pctl -arr $scs -p 25
  $s50 = Pctl -arr $scs -p 50
  $s75 = Pctl -arr $scs -p 75

  $stemM = @{}; $wrongM = @{}; $clM = @{}
  foreach ($x in $derived) {
    Inc -m $stemM -k ([string]$x.stem)
    foreach ($w in $x.wt) { Inc -m $wrongM -k ([string]$w) }
    foreach ($c in $x.cl) { Inc -m $clM -k ([string]$c) }
  }
  $topStem = Top -m $stemM -n 5
  $topWrong = Top -m $wrongM -n 6
  $topCl = Top -m $clM -n 6

  $nodes = @($derived | Group-Object -Property node | Sort-Object -Property Count -Descending)
  $nodeStats = @()
  foreach ($g in $nodes) {
    $its = @($g.Group)
    $nm = [string]$g.Name
    $nmW = @{}; $nmC = @{}; $nmS = @{}
    foreach ($it in $its) {
      Inc -m $nmS -k ([string]$it.stem)
      foreach ($w in $it.wt) { Inc -m $nmW -k ([string]$w) }
      foreach ($c in $it.cl) { Inc -m $nmC -k ([string]$c) }
    }
    $e1 = if ($its.Count -ge 1) { "$($its[0].qid): $(Clip -s $its[0].mat -n 60)" } else { "" }
    $e2 = if ($its.Count -ge 2) { "$($its[1].qid): $(Clip -s $its[1].mat -n 60)" } else { "" }
    $nodeStats += [pscustomobject]@{
      node = $nm
      count = [int]$g.Count
      len = Pctl -arr @($its | ForEach-Object { [double]$_.len }) -p 50
      sc = Pctl -arr @($its | ForEach-Object { [double]$_.sc }) -p 50
      w = ((Top -m $nmW -n 3 | ForEach-Object { "$($_.Name)($($_.Value))" }) -join "; ")
      c = ((Top -m $nmC -n 3 | ForEach-Object { "$($_.Name)($($_.Value))" }) -join "; ")
      stem = ((Top -m $nmS -n 1 | ForEach-Object { $_.Name }) -join "")
      h = Hint -qt $qt -node $nm
      e1 = $e1
      e2 = $e2
    }
  }

  $signals = Parse-AllowSignals -path $allow
  $ctype = if ($qt -eq "sentence_fill") { @("functional_slot_unit", "single_paragraph") } elseif ($qt -eq "sentence_order") { @("ordered_unit_group", "sentence_group") } else { @("closed_span", "single_paragraph") }
  $arch = if ($qt -eq "sentence_fill") { "slot_completion" } elseif ($qt -eq "sentence_order") { "ordering_chain" } else { "central_meaning" }

  $parseTotal = 0; $parseBad = 0
  if (Test-Path $en) {
    Get-Content -Path $en -Encoding UTF8 | ForEach-Object {
      if ([string]::IsNullOrWhiteSpace($_)) { return }
      $parseTotal += 1
      try { $null = ($_ | ConvertFrom-Json) } catch { $parseBad += 1 }
    }
  }

  $materialOut = @()
  $materialOut += "meta:"
  $materialOut += "  batch: $(Q $d.Name)"
  $materialOut += "  draft_version: ""v0.1"""
  $materialOut += "  sample_coverage: $n"
  $materialOut += "  business_family_id: $(Q $bf)"
  $materialOut += "  question_type: $(Q $qt)"
  $materialOut += "  business_subtype: $(Q $bs)"
  $materialOut += "source_assets:"
  $materialOut += "  cleaned_truth_materials: $(Q ([IO.Path]::GetFileName($clean)))"
  $materialOut += "  material_samples: $(Q ([IO.Path]::GetFileName($materialFile)))"
  $materialOut += "  window2_mapped_output: $(Q ([IO.Path]::GetFileName($w2)))"
  $materialOut += "  window2_enriched: $(Q ([IO.Path]::GetFileName($en)))"
  $materialOut += "material_card_draft:"
  $materialOut += "  candidate_contract:"
  $materialOut += "    allowed_candidate_types:"
  foreach ($ct in $ctype) { $materialOut += "      - $(Q $ct)" }
  $materialOut += "    preferred_candidate_types:"
  foreach ($ct in $ctype) { $materialOut += "      - $(Q $ct)" }
  $materialOut += "  required_signals:"
  foreach ($s in $signals) { $materialOut += "    - $(Q $s)" }
  $materialOut += "  selection_core:"
  $materialOut += "    suitable_material_profile:"
  $materialOut += "      preferred_length_chars: { p25: $l25, p50: $l50, p75: $l75 }"
  $materialOut += "      preferred_sentence_count: { p25: $s25, p50: $s50, p75: $s75 }"
  $materialOut += ('      preferred_information_density: ' + (Q 'at least 2 info layers and not pure slogan/fragments'))
  $materialOut += "    minimal_processing_policy:"
  $materialOut += ('      - ' + (Q 'only denoise/deduplicate/light cohesion repairs; keep central chain unchanged'))
  $materialOut += ('      - ' + (Q 'do not add external facts or change argument direction'))
  $materialOut += "    unsuitable_material_patterns:"
  $materialOut += ('      - ' + (Q 'missing core axis: cannot support stable question logic'))
  $materialOut += ('      - ' + (Q 'broken structure: severe stitching and dependency gaps'))
  $materialOut += ('      - ' + (Q 'too thin: cannot support competitive 4-option design'))
  $materialOut += "  default_generation_archetype: $(Q $arch)"
  $materialOut += "  node_overrides:"
  foreach ($ns in $nodeStats) {
    $materialOut += "    - node_id: $(Q $ns.node)"
    $materialOut += "      sample_count: $($ns.count)"
    $materialOut += "      offset_from_main:"
    $materialOut += "        material_structure: $(Q $($ns.h[0]))"
    $materialOut += "        material_signal: $(Q $($ns.h[1]))"
    $materialOut += "        carrying_mode: $(Q $($ns.h[2]))"
    $materialOut += "        special_preference: $(Q $($ns.h[3]))"
    $materialOut += "      evidence_samples:"
    $materialOut += "        - $(Q $ns.e1)"
    $materialOut += "        - $(Q $ns.e2)"
  }
  $materialOut += "  draft_only_notes:"
  $materialOut += ('    - ' + (Q 'distilled per subfamily folder only; no cross-subfamily merge'))
  $materialOut += ('    - ' + (Q 'node differences preserved as labels; no upgrade to new family/subtype'))
  Set-Content -Path (Join-Path $folder "material_card_draft.yaml") -Value ($materialOut -join "`n") -Encoding UTF8

  $questionOut = @()
  $questionOut += "meta:"
  $questionOut += "  batch: $(Q $d.Name)"
  $questionOut += "  draft_version: ""v0.1"""
  $questionOut += "  sample_coverage: $n"
  $questionOut += "question_card_draft:"
  $questionOut += "  question_type: $(Q $qt)"
  $questionOut += "  business_subtype_id: $(Q $bs)"
  $questionOut += "  runtime_binding:"
  $questionOut += "    question_type: $(Q $qt)"
  $questionOut += "    business_subtype: $(Q $bs)"
  $questionOut += "  upstream_contract:"
  $questionOut += "    required_candidate_types:"
  foreach ($ct in $ctype) { $questionOut += "      - $(Q $ct)" }
  $questionOut += "  base_slots:"
  $questionOut += ('    stem_style_hint: ' + (Q (($topStem | Select-Object -First 1 | ForEach-Object { $_.Name }) -join "")))
  $questionOut += ('    wrong_option_modes_hint: ' + (Q (($topWrong | ForEach-Object { $_.Name }) -join " / ")))
  $questionOut += ('    analysis_clue_path_hint: ' + (Q (($topCl | ForEach-Object { $_.Name }) -join " / ")))
  $questionOut += "  validator_contract:"
  $questionOut += "    require_single_best_answer: true"
  $questionOut += "    require_material_grounding: true"
  $questionOut += "  generation_behavior:"
  $questionOut += "    stem_how_to_ask:"
  foreach ($x in $topStem) {
    $questionOut += ('      - ' + (Q ("$($x.Name) (n=$($x.Value))")))
  }
  $questionOut += "    correct_option_why_valid:"
  if ($qt -eq "sentence_fill") {
    $questionOut += ('      - ' + (Q 'correct option must fit slot function and bi-directional context'))
    $questionOut += ('      - ' + (Q 'keep semantic equivalence with removed sentence when applicable'))
  } elseif ($qt -eq "sentence_order") {
    $questionOut += ('      - ' + (Q 'correct order must satisfy head/tail legality plus local bindings'))
    $questionOut += ('      - ' + (Q 'evidence must come from source-internal chain, not external connectors'))
  } else {
    $questionOut += ('      - ' + (Q 'correct option must cover central object plus task/judgment'))
    $questionOut += ('      - ' + (Q 'do not elevate local detail/background into main idea'))
  }
  $questionOut += "    distractor_how_to_fail:"
  foreach ($x in $topWrong) {
    $questionOut += ('      - ' + (Q ("$($x.Name) (n=$($x.Value))")))
  }
  $questionOut += "    analysis_clue_focus:"
  foreach ($x in $topCl) {
    $questionOut += ('      - ' + (Q ("$($x.Name) (n=$($x.Value))")))
  }
  $questionOut += "    material_dependency_points:"
  if ($qt -eq "sentence_fill") {
    $questionOut += ('      - ' + (Q 'blank position and slot role consistency'))
    $questionOut += ('      - ' + (Q 'bi-directional context coherence'))
  } elseif ($qt -eq "sentence_order") {
    $questionOut += ('      - ' + (Q 'head legality and tail closure'))
    $questionOut += ('      - ' + (Q 'local bindings and global progression'))
  } else {
    $questionOut += ('      - ' + (Q 'central object and structural axis identification'))
    $questionOut += ('      - ' + (Q 'hierarchy split between support details and central claim'))
  }
  $questionOut += "  node_overrides:"
  foreach ($ns in $nodeStats) {
    $questionOut += "    - node_id: $(Q $ns.node)"
    $questionOut += "      sample_count: $($ns.count)"
    $questionOut += "      stem_bias: $(Q $ns.stem)"
    $questionOut += "      distractor_bias: $(Q $ns.w)"
    $questionOut += "      analysis_clue_bias: $(Q $ns.c)"
    $questionOut += "      material_dependency_offset: $(Q $($ns.h[1]))"
  }
  $questionOut += "  draft_only_notes:"
  $questionOut += ('    - ' + (Q 'node differences are preserved in card fields, not only in report'))
  $questionOut += ('    - ' + (Q 'no schema/runtime changes introduced'))
  Set-Content -Path (Join-Path $folder "question_card_draft.yaml") -Value ($questionOut -join "`n") -Encoding UTF8

  $promptFamily = if ($qt -eq "main_idea" -and $bs -eq "center_understanding") { "main_idea/center_understanding" } else { $qt }
  $promptOut = @()
  $promptOut += "# prompt_draft"
  $promptOut += ""
  $promptOut += "## 1) Material Consumption Prompt Draft"
  $promptOut += ""
  $promptOut += "Allowed landing: question_generation_prompt_assets.yaml (A-class)"
  $promptOut += ""
  $promptOut += "Input fields: sample_id, family, question_type, business_subtype, pattern_tag, material_text, source_doc, resolved_slots, prompt_extras"
  $promptOut += ""
  $promptOut += "Main layer draft:"
  $promptOut += "[PROMPT_START]"
  $promptOut += "You are consuming material for $promptFamily."
  $promptOut += "Use only given fields and material_text. Do not invent external facts."
  if ($qt -eq "sentence_fill") {
    $promptOut += "First lock blank position + slot role, then verify left/right context constraints."
  } elseif ($qt -eq "sentence_order") {
    $promptOut += "First detect head/tail legality, then local bindings, then unique global chain."
  } else {
    $promptOut += "First identify central object, then article task/judgment, then split axis vs support details."
  }
  $promptOut += "Only minimal readability repair is allowed; keep central evidence chain unchanged."
  $promptOut += "[PROMPT_END]"
  $promptOut += ""
  $promptOut += "Node supplements:"
  foreach ($ns in $nodeStats) { $promptOut += "- [$($ns.node)] $($ns.h[1]); $($ns.h[3])" }
  $promptOut += ""
  $promptOut += "## 2) Question Generation Prompt Draft"
  $promptOut += ""
  $promptOut += "Allowed landing: prompt_templates.yaml (only current family generate template, A-class)"
  $promptOut += ""
  $promptOut += "Input fields: question_type, business_subtype, material_text, stem, options(A-D), answer, analysis, pattern_tag, resolved_slots, prompt_extras"
  $promptOut += ""
  $promptOut += "Main layer draft:"
  $promptOut += "[PROMPT_START]"
  $promptOut += "Generate exactly one 4-option single-choice question."
  $promptOut += "Stem style must stay exam-like and aligned to this subfamily."
  $promptOut += "Correct option must be materially defensible and on the same axis as stem."
  $promptOut += "Distractors must stay same topic/same granularity but fail in direction, not by irrelevance."
  if ($qt -eq "sentence_fill") { $promptOut += "For sentence_fill, correct option must fit slot role and avoid macro overreach." }
  elseif ($qt -eq "sentence_order") { $promptOut += "For sentence_order, options must be legal 6-unit permutations with meaningful near-miss distractors." }
  else { $promptOut += "For center_understanding, correct option must cover central object + article task, not local details." }
  $promptOut += "Output structured fields only."
  $promptOut += "[PROMPT_END]"
  $promptOut += ""
  $promptOut += "Node supplements:"
  foreach ($ns in $nodeStats) { $promptOut += "- [$($ns.node)] distractor focus: $($ns.w)" }
  $promptOut += ""
  $promptOut += "## 3) Analysis Prompt Draft"
  $promptOut += ""
  $promptOut += "Allowed landing: question_generation_prompt_assets.yaml (answer_grounding + analysis sections, A-class)"
  $promptOut += ""
  $promptOut += "Input fields: material_text, stem, options, answer, analysis, pattern_tag"
  $promptOut += ""
  $promptOut += "Main layer draft:"
  $promptOut += "[PROMPT_START]"
  $promptOut += "Write analysis in three moves: clue -> why correct -> why key distractors fail."
  $promptOut += "Ground every claim in material evidence."
  $promptOut += "When excluding distractors, state exact failure type (scope/function/chain/position), not generic mismatch."
  if ($qt -eq "sentence_fill") { $promptOut += "Prioritize slot function and bi-directional context checks." }
  elseif ($qt -eq "sentence_order") { $promptOut += "Prioritize head/tail legality and local binding evidence." }
  else { $promptOut += "Prioritize structural axis clue and central-meaning alignment." }
  $promptOut += "End with a single explicit answer conclusion."
  $promptOut += "[PROMPT_END]"
  $promptOut += ""
  $promptOut += "Node supplements:"
  foreach ($ns in $nodeStats) { $promptOut += "- [$($ns.node)] clue priority: $($ns.c)" }
  Set-Content -Path (Join-Path $folder "prompt_draft.md") -Value ($promptOut -join "`n") -Encoding UTF8

  $reportOut = @()
  $reportOut += "# distill_report"
  $reportOut += ""
  $reportOut += "## 1. Subfamily batch name"
  $reportOut += "- $($d.Name)"
  $reportOut += ""
  $reportOut += "## 2. Covered sample count"
  $reportOut += "- total_samples: $n"
  $reportOut += "- material_samples_file: $([IO.Path]::GetFileName($materialFile))"
  $reportOut += ""
  $reportOut += "## 3. Identified node list"
  foreach ($ns in $nodeStats) { $reportOut += "- $($ns.node)" }
  $reportOut += ""
  $reportOut += "## 4. Node coverage"
  foreach ($ns in $nodeStats) { $reportOut += "- $($ns.node): $($ns.count)" }
  $reportOut += ""
  $reportOut += "## 5. Main-layer common behavior"
  $reportOut += "- question_type/business_subtype: $qt / $bs"
  $reportOut += "- material_length_chars_p25_p50_p75: $l25 / $l50 / $l75"
  $reportOut += "- sentence_count_p25_p50_p75: $s25 / $s50 / $s75"
  $topStemText = ($topStem | ForEach-Object { '"' + (Clip -s $_.Name -n 22) + '"(' + $_.Value + ')' }) -join "; "
  $topWrongText = ($topWrong | ForEach-Object { "$($_.Name)($($_.Value))" }) -join "; "
  $topClueText = ($topCl | ForEach-Object { "$($_.Name)($($_.Value))" }) -join "; "
  $reportOut += "- top_stem_patterns: $topStemText"
  $reportOut += "- top_distractor_fail_modes: $topWrongText"
  $reportOut += "- top_analysis_clues: $topClueText"
  $reportOut += ""
  $reportOut += "## 6. Node-level differences"
  foreach ($ns in $nodeStats) {
    $reportOut += "- **$($ns.node)**: n=$($ns.count), len_p50=$($ns.len), sent_p50=$($ns.sc), distractor_bias=$($ns.w), clue_bias=$($ns.c)."
  }
  $reportOut += ""
  $reportOut += "## 7. Likely unstable points"
  $reportOut += "- small nodes (especially n<12) may be under-fitted by main-layer defaults."
  $reportOut += "- noisy records may blur node boundaries."
  if ($parseBad -gt 0) {
    $reportOut += "- enriched parse failures: $parseBad/$parseTotal in window2_mapped_output.enriched.v2.jsonl; not used as primary evidence."
  }
  $reportOut += ""
  $reportOut += "## 8. Pressure-test focus"
  $reportOut += "- run layered tests: main layer first, then per-node behavior preservation."
  $reportOut += "- stress high-confusion distractors + analysis-clue consistency."
  if ($qt -eq "sentence_fill") { $reportOut += "- boundary tests for opening/middle/ending slot roles." }
  elseif ($qt -eq "sentence_order") { $reportOut += "- adversarial tests on head-tail legality and local bindings." }
  else { $reportOut += "- adversarial tests on detail-as-main and scope drift." }
  Set-Content -Path (Join-Path $folder "distill_report.md") -Value ($reportOut -join "`n") -Encoding UTF8

  $summary += [pscustomobject]@{
    batch = $d.Name
    status = "ok"
    sample_count = $n
    node_count = $nodeStats.Count
    question_type = $qt
    business_subtype = $bs
  }
}

$summary | Format-Table -AutoSize
