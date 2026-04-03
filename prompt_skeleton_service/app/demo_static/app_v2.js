const PRIMARY_OPTIONS = [
  { label: "不指定", value: "" },
  { label: "标题填入题", value: "title_selection" },
  { label: "接语续写题", value: "continuation" },
  { label: "语句排序题", value: "sentence_order" },
  { label: "语句填空题", value: "sentence_fill" },
  { label: "中心理解题", value: "center_understanding" },
];

const SECONDARY_BY_PRIMARY = {
  "": [{ label: "不指定", value: "" }],
  title_selection: [
    { label: "不指定", value: "" },
    { label: "转折关系", value: "turning_relation_focus" },
    { label: "因果关系", value: "cause_effect__conclusion_focus" },
    { label: "必要条件关系", value: "necessary_condition_countermeasure" },
    { label: "主题词概括", value: "theme_word_focus" },
  ],
  continuation: [
    { label: "不指定", value: "" },
    { label: "情节续写", value: "plot_continuation" },
    { label: "人物塑造", value: "character_extension" },
    { label: "主题升华", value: "theme_extension" },
    { label: "伏笔呼应", value: "foreshadow_recall" },
    { label: "环境衔接", value: "setting_alignment" },
    { label: "情绪递进", value: "emotion_progression" },
    { label: "冲突解决", value: "conflict_resolution" },
    { label: "结尾收束", value: "ending_resolution" },
    { label: "价值表达", value: "value_expression" },
  ],
  sentence_order: [
    { label: "不指定", value: "" },
    { label: "首尾锁定", value: "head_tail_lock" },
    { label: "确定性捆绑", value: "deterministic_binding" },
    { label: "行文逻辑", value: "discourse_logic" },
    { label: "时间/行动顺序", value: "timeline_action_sequence" },
  ],
  sentence_fill: [
    { label: "不指定", value: "" },
    { label: "开头概括", value: "opening_summary" },
    { label: "开头引入", value: "opening_topic_intro" },
    { label: "中间承上", value: "middle_carry_previous" },
    { label: "中间启下", value: "middle_lead_next" },
    { label: "承上启下", value: "middle_bridge_both_sides" },
    { label: "结尾总结", value: "ending_summary" },
    { label: "结尾对策", value: "ending_countermeasure" },
  ],
  center_understanding: [{ label: "中心理解主卡", value: "" }],
};

const TEXT_DIRECTION_OPTIONS = [
  { label: "不指定", value: "" },
  { label: "概括归纳", value: "概括归纳" },
  { label: "主旨判断", value: "主旨判断" },
  { label: "标题统摄", value: "标题统摄" },
  { label: "结构推进", value: "结构推进" },
  { label: "因果辨析", value: "因果辨析" },
  { label: "转折判断", value: "转折判断" },
  { label: "对策归纳", value: "对策归纳" },
];

const MATERIAL_STRUCTURE_OPTIONS = [
  { label: "不指定", value: "" },
  { label: "整段统合", value: "整段统合" },
  { label: "局部概括", value: "局部概括" },
  { label: "因果链条", value: "因果链条" },
  { label: "转折推进", value: "转折推进" },
  { label: "问题-对策", value: "问题-对策" },
  { label: "总-分", value: "总-分" },
  { label: "分-总", value: "分-总" },
  { label: "并列展开", value: "并列展开" },
  { label: "时间推进", value: "时间推进" },
  { label: "步骤推进", value: "步骤推进" },
];

const VALUE_LABELS = {
  easy: "简单",
  medium: "中等",
  hard: "困难",
  pending_review: "待复核",
  approved: "已通过",
  auto_failed: "建议复核",
  discarded: "已作废",
  sentence_order: "语句排序题",
  sentence_fill: "语句填空题",
  continuation: "接语续写题",
  main_idea: "主旨中心类",
  center_understanding: "中心理解",
  title_selection: "标题填入",
  structure_summary: "结构概括",
  local_paragraph_summary: "局部段意概括",
  whole_passage_integration: "整段统合",
  hidden_thesis_abstraction: "隐含主旨概括",
  dual_anchor_lock: "双锚点锁定",
  carry_parallel_expand: "承接并列展开",
  viewpoint_reason_action: "观点-原因-行动",
  problem_solution_case_blocks: "问题-对策-案例",
  phrase_order_variant: "短句排序变体",
  opening_summary: "开头概括",
  opening_topic_intro: "开头引入",
  middle_carry_previous: "中间承上",
  middle_lead_next: "中间启下",
  middle_bridge_both_sides: "承上启下",
  ending_summary: "结尾总结",
  ending_countermeasure: "结尾对策",
  middle: "中段结论",
  examp: "例子片段",
  scope: "范围偏移",
  middle_conclusion: "中段结论",
  example_fragment: "例子片段",
  scope_shift: "范围偏移",
  undergeneralization: "概括不足",
  fabrication: "无中生有",
  overgeneralization: "过度泛化",
  function_misread: "功能误读",
  title_too_wide: "标题过宽",
  title_too_narrow: "标题过窄",
  catchy_but_offcore: "吸睛但偏核",
  wrong_opening: "首句错误",
  wrong_closing: "尾句错误",
  local_binding_break: "局部捆绑断裂",
  local_binding: "局部捆绑",
  wrong_direction: "方向错误",
  local_paragraph: "局部段意",
  local_paragraph_meaning: "局部段意概括",
  topic_shift: "主题偏移",
  local_bias: "局部偏移",
  block_swap: "板块调换",
  connector_mislead: "连接词误导",
  parallel_misorder: "并列错序",
  summary_misplace: "总结错位",
  reason_fronting: "原因前置",
  action_fronting: "行动前置",
};

const CONTROL_LABELS = {
  difficulty_target: "难度目标",
  pattern_id: "制作卡",
  "material_policy.preferred_document_genres": "偏好文体",
  distractor_style_1: "错误项 1 干扰方式",
  distractor_style_2: "错误项 2 干扰方式",
  distractor_style_3: "错误项 3 干扰方式",
  distractor_strength_1: "错误项 1 迷惑度",
  distractor_strength_2: "错误项 2 迷惑度",
  distractor_strength_3: "错误项 3 迷惑度",
  distractor_style_bundle: "干扰方式",
  option_confusability: "选项迷惑度",
  difficulty_projection_factor: "整体难度调节系数",
};

const state = {
  batchId: null,
  items: [],
  controlsByItem: {},
};

function $(id) {
  return document.getElementById(id);
}

function escapeHtml(value) {
  return String(value == null ? "" : value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function humanize(value) {
  const key = String(value == null ? "" : value);
  return VALUE_LABELS[key] || key || "未填写";
}

function humanizeControlLabel(label, key) {
  return CONTROL_LABELS[key] || VALUE_LABELS[label] || label || key;
}

function showToast(message, tone = "success") {
  let root = $("actionToastRoot");
  if (!root) {
    root = document.createElement("div");
    root.id = "actionToastRoot";
    root.className = "action-toast-root";
    document.body.appendChild(root);
  }
  const toast = document.createElement("div");
  toast.className = "action-toast action-toast-" + tone;
  toast.textContent = message;
  root.appendChild(toast);
  setTimeout(() => toast.classList.add("is-visible"), 10);
  setTimeout(() => {
    toast.classList.remove("is-visible");
    setTimeout(() => toast.remove(), 240);
  }, 2200);
}

function setButtonBusy(button, busy, text) {
  if (!button) return;
  if (busy) {
    if (!button.dataset.originalText) {
      button.dataset.originalText = button.textContent;
    }
    button.textContent = text || "处理中...";
    button.disabled = true;
  } else {
    button.textContent = button.dataset.originalText || button.textContent;
    button.disabled = false;
  }
}

async function apiFetch(path, options = {}) {
  const response = await fetch(path, {
    method: options.method || "GET",
    headers: {
      Accept: "application/json",
      ...(options.body ? { "Content-Type": "application/json" } : {}),
      ...(options.headers || {}),
    },
    body: options.body,
  });
  const contentType = response.headers.get("content-type") || "";
  const payload = contentType.includes("application/json") ? await response.json() : await response.text();
  if (!response.ok) {
    const message =
      typeof payload === "string"
        ? payload
        : (payload && payload.error && payload.error.message) ||
          payload.detail ||
          "请求失败";
    throw new Error(typeof message === "string" ? message : JSON.stringify(message));
  }
  return payload;
}

function switchScreen(name) {
  ["builderScreen", "loadingScreen", "resultScreen"].forEach((id) => {
    const active = id === name + "Screen";
    const node = $(id);
    if (node) node.classList.toggle("active", active);
  });
}

function populateSelect(selectEl, options) {
  selectEl.innerHTML = "";
  options.forEach((option) => {
    const node = document.createElement("option");
    node.value = option.value;
    node.textContent = option.label;
    selectEl.appendChild(node);
  });
}

function syncCountValue() {
  $("countValue").textContent = $("count").value;
}

function setLoadingState(step, desc) {
  const node = $("loadingNode");
  const nodeDesc = $("loadingNodeDesc");
  if (node) node.textContent = step || "处理中...";
  if (nodeDesc) nodeDesc.textContent = desc || "系统正在完成取材、生成与审核。";
}

function collectSourceQuestionPayload() {
  const passage = $("sourceQuestionPassage").value.trim();
  const stem = $("sourceQuestionStem").value.trim();
  const options = {
    A: $("sourceOptionA").value.trim(),
    B: $("sourceOptionB").value.trim(),
    C: $("sourceOptionC").value.trim(),
    D: $("sourceOptionD").value.trim(),
  };
  const answer = $("sourceQuestionAnswer").value.trim();
  const analysis = $("sourceQuestionAnalysis").value.trim();
  if (!passage && !stem && !options.A && !options.B && !options.C && !options.D && !answer && !analysis) {
    return null;
  }
  return { passage, stem, options, answer: answer || null, analysis: analysis || null };
}

function inferQuestionFocus(sourceQuestion) {
  const stem = String((sourceQuestion && sourceQuestion.stem) || "");
  if (/重新排列|语序正确|排序/.test(stem)) return "sentence_order";
  if (/填入|横线|最恰当/.test(stem)) return "sentence_fill";
  if (/标题/.test(stem)) return "title_selection";
  if (/接在|接语|衔接|接续/.test(stem)) return "continuation";
  return "center_understanding";
}

function renderSecondaryOptions() {
  const primary = $("questionFocus").value || "";
  populateSelect($("specialType"), SECONDARY_BY_PRIMARY[primary] || SECONDARY_BY_PRIMARY[""]);
}

function buildGeneratePayload() {
  const sourceQuestion = collectSourceQuestionPayload();
  const focus = $("questionFocus").value || (sourceQuestion ? inferQuestionFocus(sourceQuestion) : "");
  return {
    question_focus: focus,
    difficulty_level: $("difficultyLevel").value,
    text_direction: $("textDirection").value || null,
    material_structure: $("materialStructure").value || null,
    special_question_types: $("specialType").value ? [$("specialType").value] : [],
    count: Number($("count").value || 1),
    source_question: sourceQuestion,
  };
}

function setParseStatus(message, visible) {
  const node = $("sourceQuestionParseStatus");
  if (!node) return;
  node.hidden = !visible;
  node.textContent = visible ? message : "";
}

async function autoDetectSourceQuestion() {
  const rawText = $("sourceQuestionPassage").value.trim();
  if (!rawText) {
    alert("请先把整道原题粘贴到“原题文段”里。");
    return;
  }
  const button = $("sourceQuestionDetectBtn");
  setButtonBusy(button, true, "识别中...");
  setParseStatus("正在自动拆题并回填...", true);
  try {
    const result = await apiFetch("/api/v1/questions/source-question/parse", {
      method: "POST",
      body: JSON.stringify({ raw_text: rawText }),
    });
    const parsed = result.source_question || {};
    $("sourceQuestionPassage").value = parsed.passage || "";
    $("sourceQuestionStem").value = parsed.stem || "";
    $("sourceOptionA").value = (parsed.options && parsed.options.A) || "";
    $("sourceOptionB").value = (parsed.options && parsed.options.B) || "";
    $("sourceOptionC").value = (parsed.options && parsed.options.C) || "";
    $("sourceOptionD").value = (parsed.options && parsed.options.D) || "";
    $("sourceQuestionAnswer").value = parsed.answer || "";
    $("sourceQuestionAnalysis").value = parsed.analysis || "";
    if (!$("questionFocus").value) {
      $("questionFocus").value = inferQuestionFocus(parsed);
      renderSecondaryOptions();
    }
    setParseStatus("已自动拆题并回填到下方表单。", true);
    showToast("自动拆题完成");
  } catch (error) {
    setParseStatus("自动拆题失败，请手动填写。", true);
    alert("自动拆题失败：" + error.message);
  } finally {
    setButtonBusy(button, false);
  }
}

async function generateQuestions(event) {
  event.preventDefault();
  const payload = buildGeneratePayload();
  if (!payload.question_focus) {
    alert("请先选择题型，或先粘贴完整原题后点击自动拆题。");
    return;
  }
  setLoadingState("正在提交生成任务", "系统将依次完成取材、生成、校验与审核。");
  switchScreen("loading");
  try {
    const response = await apiFetch("/api/v1/questions/generate", {
      method: "POST",
      body: JSON.stringify(payload),
    });
    state.batchId = response.batch_id;
    state.items = response.items || [];
    state.controlsByItem = {};
    renderResults();
    switchScreen("result");
    void Promise.all(state.items.map((item) => loadControlsForItem(item.item_id)));
  } catch (error) {
    switchScreen("builder");
    alert("生成失败：" + error.message);
  }
}

function renderOptionsHtml(options) {
  const entries = Object.entries(options || {});
  if (!entries.length) return '<div class="muted">暂无选项</div>';
  return entries
    .map(([key, value]) => {
      return (
        '<div class="option-item"><span class="option-key">' +
        escapeHtml(key) +
        "</span><span>" +
        escapeHtml(value) +
        "</span></div>"
      );
    })
    .join("");
}

function getJudgeSummary(item) {
  const evaluation = item.evaluation_result || {};
  const summary = String(evaluation.judge_reason || evaluation.summary || "").trim();
  return summary || "系统已完成审核，可结合材料、题干和解析继续查看。";
}

function formatMaterialForDisplay(item) {
  const material = item.material_selection || {};
  const text = String(material.text || item.material_text || "").trim();
  return escapeHtml(text);
}

function buildReplacementLabel(entry) {
  const title = String(entry.article_title || entry.label || "").trim();
  const source = String(entry.source_name || "").trim();
  const genre = String(entry.document_genre || "").trim();
  const parts = [];
  if (title) parts.push(title);
  if (source) parts.push(source);
  if (genre) parts.push(genre);
  return parts.join(" / ") || entry.material_id;
}

function renderControlSection(itemId) {
  const panel = state.controlsByItem[itemId];
  if (!panel || !Array.isArray(panel.controls) || !panel.controls.length) {
    return `
      <section class="result-section">
        <h3>控制调节</h3>
        <div class="analysis-box">正在加载控制项...</div>
      </section>
    `;
  }

  const controlsByKey = {};
  (panel.controls || []).forEach((control) => {
    controlsByKey[control.control_key] = control;
  });

  function renderSelectControl(control, key, label, description, currentValue, extraAttrs = "") {
    if (!control) return "";
    const options = Array.isArray(control.options) ? control.options : [];
    return `
      <label class="control-field">
        <span>${escapeHtml(label)}</span>
        <select class="control-input" data-item-id="${itemId}" data-control-key="${escapeHtml(key)}" ${extraAttrs}>
          <option value="">不指定</option>
          ${options
            .map((option) => {
              const value = option.value == null ? "" : String(option.value);
              const selected = String(currentValue || "") === value;
              return `<option value="${escapeHtml(value)}"${selected ? " selected" : ""}>${escapeHtml(
                humanize(option.label || value)
              )}</option>`;
            })
            .join("")}
        </select>
        <small>${escapeHtml(description)}</small>
      </label>
    `;
  }

  const difficultyControl = controlsByKey["difficulty_target"];
  const distractorStrengthControl =
    controlsByKey["option_confusability"] ||
    controlsByKey["distractor_strength"];
  const distractorModesControl =
    controlsByKey["distractor_modes"] ||
    controlsByKey["distractor_style_bundle"];
  const currentModes = Array.isArray(distractorModesControl?.current_value)
    ? distractorModesControl.current_value
    : [];

  const controlsHtml = [
    renderSelectControl(
      difficultyControl,
      "difficulty_target",
      "整体难度调节系数",
      "控制整题整体难度投射，影响推理深度与区分度。",
      difficultyControl?.current_value
    ),
    renderSelectControl(
      distractorStrengthControl,
      distractorStrengthControl?.control_key || "distractor_strength",
      "选项迷惑度",
      "控制错误项整体贴近正确项的程度，不是只影响某一个组合结果。",
      distractorStrengthControl?.current_value
    ),
    renderSelectControl(
      distractorModesControl,
      "distractor_mode_1",
      "错误项 1 干扰方式",
      "分别指定 3 个错误项的主要偏离方式。",
      currentModes[0] || ""
    ),
    renderSelectControl(
      distractorModesControl,
      "distractor_mode_2",
      "错误项 2 干扰方式",
      "分别指定 3 个错误项的主要偏离方式。",
      currentModes[1] || ""
    ),
    renderSelectControl(
      distractorModesControl,
      "distractor_mode_3",
      "错误项 3 干扰方式",
      "分别指定 3 个错误项的主要偏离方式。",
      currentModes[2] || ""
    ),
  ]
    .filter(Boolean)
    .join("");

  return `
    <section class="result-section">
      <h3>控制调节</h3>
      <div class="builder-grid">${controlsHtml}</div>
      <div class="inline-actions">
        <button type="button" class="secondary-btn" data-action="apply-controls" data-item-id="${itemId}">应用调节</button>
      </div>
    </section>
  `;
}

function renderResults() {
  $("resultBatchInfo").textContent = state.batchId
    ? `当前批次：${state.batchId} · 已生成 ${state.items.length} 题`
    : "等待本次生成结果...";
  const list = $("resultList");
  list.innerHTML = "";
  state.items.forEach((item, index) => {
    list.appendChild(buildQuestionCard(item, index + 1));
  });
}

function buildQuestionCard(item, order) {
  const wrapper = document.createElement("section");
  wrapper.className = "review-card";
  wrapper.dataset.itemId = item.item_id;

  const generated = item.generated_question || {};
  const material = item.material_selection || {};
  const validation = item.validation_result || {};
  const optionsHtml = renderOptionsHtml(generated.options || {});
  const originalMaterial = material.original_text || material.text || "";

  wrapper.innerHTML = `
    <div class="review-card-head">
        <div>
          <h2>题目 ${order}</h2>
          <div class="chip-row">
            <span class="meta-chip">状态：${escapeHtml(humanize(item.current_status))}</span>
            <span class="meta-chip">母族：${escapeHtml(humanize(item.question_type))}</span>
            <span class="meta-chip">题卡：${escapeHtml(humanize(item.business_subtype || item.pattern_id || ""))}</span>
            <span class="meta-chip">难度：${escapeHtml(humanize(item.difficulty_target))}</span>
          </div>
        </div>
    </div>

    <section class="result-section">
      <h3>加工后材料</h3>
      <div class="material-box">${formatMaterialForDisplay(item)}</div>
    </section>

    <section class="result-section">
      <h3>题干与选项</h3>
      <div class="stem-box">${escapeHtml(generated.stem || item.stem_text || "")}</div>
      <div class="option-list">${optionsHtml}</div>
    </section>

    <section class="result-section">
      <h3>答案与解析</h3>
      <div class="answer-box"><strong>答案：</strong>${escapeHtml(generated.answer || "")}</div>
      <div class="analysis-box">${escapeHtml(generated.analysis || "")}</div>
    </section>

    <details class="result-section">
      <summary>系统建议与参考信息</summary>
      <div class="analysis-box"><strong>规则校验：</strong>${validation.passed === false ? "待优化" : "通过"}${
    validation.score != null ? " · 参考评分：" + validation.score : ""
  }</div>
      <div class="analysis-box"><strong>系统建议：</strong>${escapeHtml(getJudgeSummary(item))}</div>
      <div class="analysis-box"><strong>参考题干：</strong>${escapeHtml(
        (((item.request_snapshot || {}).source_question || {}).stem) || ""
      )}</div>
    </details>

    <details class="result-section">
      <summary>查看原文与材料来源</summary>
      <div class="analysis-box"><strong>原始材料：</strong></div>
      <div class="material-box">${escapeHtml(originalMaterial)}</div>
      <div class="analysis-box"><strong>来源：</strong>${escapeHtml((material.source || {}).source_name || "")} · ${escapeHtml(
    (material.source || {}).article_title || ""
  )}</div>
    </details>

    ${renderControlSection(item.item_id)}

    <section class="result-section">
      <h3>备选材料与自贴材料</h3>
      <div class="inline-actions">
        <button type="button" class="secondary-btn" data-action="load-replacements" data-item-id="${item.item_id}">加载备选材料</button>
        <select class="replacement-select" data-item-id="${item.item_id}">
          <option value="">请选择备选材料</option>
        </select>
        <button type="button" class="secondary-btn" data-action="apply-replacement" data-item-id="${item.item_id}">使用备选材料重做</button>
      </div>
      <textarea class="custom-material-input" data-item-id="${item.item_id}" rows="4" placeholder="也可以直接粘贴自定义材料，点击“使用自贴材料重做”。"></textarea>
      <div class="inline-actions">
        <button type="button" class="secondary-btn" data-action="apply-custom-material" data-item-id="${item.item_id}">使用自贴材料重做</button>
      </div>
    </section>

    <section class="result-section">
      <h3>手工编辑区</h3>
      <label class="field"><span>加工后材料</span><textarea class="manual-material" data-item-id="${item.item_id}" rows="5">${escapeHtml(
    material.text || item.material_text || ""
  )}</textarea></label>
      <label class="field"><span>题干</span><textarea class="manual-stem" data-item-id="${item.item_id}" rows="2">${escapeHtml(
    generated.stem || item.stem_text || ""
  )}</textarea></label>
      <div class="builder-grid">
        <label class="field"><span>选项 A</span><textarea class="manual-option" data-item-id="${item.item_id}" data-option="A" rows="2">${escapeHtml(
    ((generated.options || {}).A) || ""
  )}</textarea></label>
        <label class="field"><span>选项 B</span><textarea class="manual-option" data-item-id="${item.item_id}" data-option="B" rows="2">${escapeHtml(
    ((generated.options || {}).B) || ""
  )}</textarea></label>
        <label class="field"><span>选项 C</span><textarea class="manual-option" data-item-id="${item.item_id}" data-option="C" rows="2">${escapeHtml(
    ((generated.options || {}).C) || ""
  )}</textarea></label>
        <label class="field"><span>选项 D</span><textarea class="manual-option" data-item-id="${item.item_id}" data-option="D" rows="2">${escapeHtml(
    ((generated.options || {}).D) || ""
  )}</textarea></label>
      </div>
      <div class="builder-grid">
        <label class="field"><span>答案</span>
          <select class="manual-answer" data-item-id="${item.item_id}">
            <option value="A"${generated.answer === "A" ? " selected" : ""}>A</option>
            <option value="B"${generated.answer === "B" ? " selected" : ""}>B</option>
            <option value="C"${generated.answer === "C" ? " selected" : ""}>C</option>
            <option value="D"${generated.answer === "D" ? " selected" : ""}>D</option>
          </select>
        </label>
        <label class="field"><span>解析</span><textarea class="manual-analysis" data-item-id="${item.item_id}" rows="5">${escapeHtml(
    generated.analysis || ""
  )}</textarea></label>
      </div>
      <div class="inline-actions">
        <button type="button" class="primary-btn" data-action="manual-save" data-item-id="${item.item_id}">保存编辑版本</button>
        <button type="button" class="success-btn" data-action="confirm" data-item-id="${item.item_id}">通过</button>
        <button type="button" class="danger-btn" data-action="discard" data-item-id="${item.item_id}">作废</button>
      </div>
    </section>
  `;
  return wrapper;
}

async function loadControlsForItem(itemId) {
  try {
    const panel = await apiFetch("/api/v1/questions/" + itemId + "/controls");
    state.controlsByItem[itemId] = panel;
    renderResults();
  } catch (_error) {
    // Keep the card usable even if controls fail to load.
  }
}

function getCard(itemId) {
  return document.querySelector('[data-item-id="' + itemId + '"]');
}

function collectManualPatch(itemId) {
  const card = getCard(itemId);
  const options = {};
  card.querySelectorAll('.manual-option[data-item-id="' + itemId + '"]').forEach((node) => {
    options[node.dataset.option] = node.value.trim();
  });
  return {
    material_text: card.querySelector(".manual-material").value.trim(),
    stem: card.querySelector(".manual-stem").value.trim(),
    options,
    answer: card.querySelector(".manual-answer").value.trim(),
    analysis: card.querySelector(".manual-analysis").value.trim(),
  };
}

function collectControlOverrides(itemId) {
  const card = getCard(itemId);
  const overrides = {};
  const groupedDistractorModes = [];
  card.querySelectorAll('.control-input[data-item-id="' + itemId + '"]').forEach((node) => {
    const key = node.dataset.controlKey;
    if (!key) return;
    let value = node.value;
    if (!value) return;
    if (/^distractor_mode_[123]$/.test(key)) {
      groupedDistractorModes.push(value);
      return;
    }
    if (key === "material_policy.preferred_document_genres") {
      value = value
        .split(",")
        .map((part) => part.trim())
        .filter(Boolean);
    }
    overrides[key] = value;
  });
  if (groupedDistractorModes.length) {
    overrides.distractor_modes = groupedDistractorModes;
  }
  return overrides;
}

async function handleResultAction(event) {
  const button = event.target.closest("[data-action]");
  if (!button) return;
  const action = button.dataset.action;
  const itemId = button.dataset.itemId;
  if (!itemId) return;

  try {
    if (action === "load-replacements") {
      setButtonBusy(button, true, "加载中...");
      const result = await apiFetch("/api/v1/questions/" + itemId + "/replacement-materials?limit=8");
      const select = getCard(itemId).querySelector(".replacement-select");
      select.innerHTML = '<option value="">请选择备选材料</option>';
      (result.items || []).forEach((entry) => {
        const option = document.createElement("option");
        option.value = entry.material_id;
        option.dataset.materialText = entry.material_text || "";
        option.textContent = buildReplacementLabel(entry);
        select.appendChild(option);
      });
      showToast("备选材料已加载");
      return;
    }

    if (action === "apply-replacement") {
      const card = getCard(itemId);
      const select = card.querySelector(".replacement-select");
      if (!select.value) {
        alert("请先选择一条备选材料。");
        return;
      }
      setButtonBusy(button, true, "重做中...");
      const result = await apiFetch("/api/v1/questions/" + itemId + "/review-actions", {
        method: "POST",
        body: JSON.stringify({
          action: "text_modify",
          instruction: "使用备选材料重做",
          control_overrides: {
            material_id: select.value,
            material_text: select.selectedOptions[0].dataset.materialText || "",
          },
        }),
      });
      state.items = state.items.map((current) => (current.item_id === itemId ? result.item : current));
      renderResults();
      void loadControlsForItem(itemId);
      showToast("已按备选材料重做");
      return;
    }

    if (action === "apply-custom-material") {
      const card = getCard(itemId);
      const customMaterial = card.querySelector(".custom-material-input").value.trim();
      if (!customMaterial) {
        alert("请先粘贴自定义材料。");
        return;
      }
      setButtonBusy(button, true, "重做中...");
      const result = await apiFetch("/api/v1/questions/" + itemId + "/review-actions", {
        method: "POST",
        body: JSON.stringify({
          action: "text_modify",
          instruction: "使用自贴材料重做",
          control_overrides: { material_text: customMaterial },
        }),
      });
      state.items = state.items.map((current) => (current.item_id === itemId ? result.item : current));
      renderResults();
      void loadControlsForItem(itemId);
      showToast("已按自贴材料重做");
      return;
    }

    if (action === "apply-controls") {
      setButtonBusy(button, true, "应用中...");
      const overrides = collectControlOverrides(itemId);
      const result = await apiFetch("/api/v1/questions/" + itemId + "/review-actions", {
        method: "POST",
        body: JSON.stringify({
          action: "question_modify",
          instruction: "应用控制调节",
          control_overrides: overrides,
        }),
      });
      state.items = state.items.map((current) => (current.item_id === itemId ? result.item : current));
      renderResults();
      void loadControlsForItem(itemId);
      showToast("控制调节已应用");
      return;
    }

    if (action === "manual-save") {
      setButtonBusy(button, true, "保存中...");
      showToast("正在保存编辑版本...", "info");
      const result = await apiFetch("/api/v1/questions/" + itemId + "/review-actions", {
        method: "POST",
        body: JSON.stringify({
          action: "manual_edit",
          instruction: "saved from demo",
          control_overrides: { manual_patch: collectManualPatch(itemId) },
        }),
      });
      state.items = state.items.map((current) => (current.item_id === itemId ? result.item : current));
      renderResults();
      void loadControlsForItem(itemId);
      showToast("编辑版本已保存");
      return;
    }

    if (action === "confirm") {
      setButtonBusy(button, true, "确认中...");
      const result = await apiFetch("/api/v1/questions/" + itemId + "/confirm", {
        method: "POST",
        body: JSON.stringify({ operator: "demo" }),
      });
      state.items = state.items.map((current) => (current.item_id === itemId ? result.item : current));
      renderResults();
      void loadControlsForItem(itemId);
      showToast("题目已通过");
      return;
    }

    if (action === "discard") {
      setButtonBusy(button, true, "作废中...");
      const result = await apiFetch("/api/v1/questions/" + itemId + "/review-actions", {
        method: "POST",
        body: JSON.stringify({ action: "discard", operator: "demo" }),
      });
      state.items = state.items.map((current) => (current.item_id === itemId ? result.item : current));
      renderResults();
      void loadControlsForItem(itemId);
      showToast("题目已作废", "info");
    }
  } catch (error) {
    alert("操作失败：" + error.message);
  } finally {
    setButtonBusy(button, false);
  }
}

async function exportApprovedBatch() {
  if (!state.batchId) {
    alert("当前没有可导出的批次。");
    return;
  }
  const response = await fetch("/api/v1/review/batches/" + state.batchId + "/delivery/export?format=markdown");
  if (!response.ok) {
    throw new Error((await response.text()) || "导出失败");
  }
  const text = await response.text();
  const blob = new Blob([text], { type: "text/markdown;charset=utf-8" });
  const href = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = href;
  link.download = "batch_" + state.batchId + ".md";
  document.body.appendChild(link);
  link.click();
  link.remove();
  URL.revokeObjectURL(href);
}

function initPage() {
  populateSelect($("questionFocus"), PRIMARY_OPTIONS);
  populateSelect($("specialType"), SECONDARY_BY_PRIMARY[""]);
  populateSelect($("textDirection"), TEXT_DIRECTION_OPTIONS);
  populateSelect($("materialStructure"), MATERIAL_STRUCTURE_OPTIONS);

  $("difficultyLevel").innerHTML = `
    <option value="easy">简单</option>
    <option value="medium" selected>中等</option>
    <option value="hard">困难</option>
  `;

  $("count").addEventListener("input", syncCountValue);
  $("questionFocus").addEventListener("change", renderSecondaryOptions);
  $("generateForm").addEventListener("submit", generateQuestions);
  $("sourceQuestionDetectBtn").addEventListener("click", () => {
    autoDetectSourceQuestion().catch((error) => alert("自动拆题失败：" + error.message));
  });
  $("resultList").addEventListener("click", (event) => {
    handleResultAction(event).catch((error) => alert("操作失败：" + error.message));
  });
  $("backToBuilderBtn").addEventListener("click", () => switchScreen("builder"));
  $("cancelLoadingBtn").addEventListener("click", () => switchScreen("builder"));
  $("exportApprovedBtn").addEventListener("click", () => {
    exportApprovedBatch()
      .then(() => showToast("导出成功"))
      .catch((error) => alert("导出失败：" + error.message));
  });
  syncCountValue();
}

document.addEventListener("DOMContentLoaded", initPage);
