const QUESTION_FOCUS_OPTIONS = [
  { value: "", label: "不指定" },
  { value: "main_idea", label: "主旨中心类" },
  { value: "continuation", label: "接语选择题" },
  { value: "sentence_order", label: "语句排序题" },
  { value: "sentence_fill", label: "语句填空题" },
  { value: "center_understanding", label: "中心理解题" },
];

const SPECIAL_TYPE_OPTIONS = {
  "": [{ value: "", label: "不指定" }],
  main_idea: [
    { value: "", label: "不指定" },
    { value: "title_selection", label: "标题填入" },
    { value: "turning_relation_focus", label: "转折关系聚焦" },
    { value: "cause_effect__conclusion_focus", label: "因果结论聚焦" },
    { value: "necessary_condition_countermeasure", label: "必要条件与对策" },
    { value: "theme_word_focus", label: "主题词聚焦" },
    { value: "structure_summary", label: "结构概括" },
    { value: "local_paragraph_summary", label: "局部段意概括" },
  ],
  continuation: [
    { value: "", label: "不指定" },
    { value: "plot_continuation", label: "情节续写" },
    { value: "character_extension", label: "人物延展" },
    { value: "theme_extension", label: "主题延展" },
    { value: "foreshadow_recall", label: "伏笔呼应" },
    { value: "setting_alignment", label: "环境衔接" },
    { value: "emotion_progression", label: "情绪递进" },
    { value: "conflict_resolution", label: "冲突解决" },
    { value: "ending_resolution", label: "结尾收束" },
    { value: "value_expression", label: "价值表达" },
  ],
  sentence_order: [
    { value: "", label: "不指定" },
    { value: "head_tail_lock", label: "首尾锁定" },
    { value: "deterministic_binding", label: "确定性捆绑" },
    { value: "discourse_logic", label: "行文逻辑" },
    { value: "timeline_action_sequence", label: "时间行动顺序" },
    { value: "dual_anchor_lock", label: "双锚点锁定" },
    { value: "carry_parallel_expand", label: "承接并列展开" },
    { value: "viewpoint_reason_action", label: "观点-原因-行动" },
    { value: "problem_solution_case_blocks", label: "问题-对策-案例" },
  ],
  sentence_fill: [
    { value: "", label: "不指定" },
    { value: "opening_summary", label: "开头总起" },
    { value: "opening_topic_intro", label: "开头引入" },
    { value: "middle_carry_previous", label: "中间承上" },
    { value: "middle_lead_next", label: "中间启下" },
    { value: "middle_bridge_both_sides", label: "承上启下" },
    { value: "ending_summary", label: "结尾总结" },
    { value: "ending_countermeasure", label: "结尾对策" },
  ],
  center_understanding: [{ value: "", label: "中心理解默认" }],
};

const TEXT_DIRECTION_OPTIONS = [
  { value: "", label: "不指定" },
  { value: "概括归纳", label: "概括归纳" },
  { value: "主旨判断", label: "主旨判断" },
  { value: "标题统摄", label: "标题统摄" },
  { value: "结构推进", label: "结构推进" },
  { value: "因果辨析", label: "因果辨析" },
  { value: "转折判断", label: "转折判断" },
  { value: "对策归纳", label: "对策归纳" },
];

const MATERIAL_STRUCTURE_OPTIONS = [
  { value: "", label: "不指定" },
  { value: "整段统合", label: "整段统合" },
  { value: "局部概括", label: "局部概括" },
  { value: "因果链条", label: "因果链条" },
  { value: "转折推进", label: "转折推进" },
  { value: "问题-对策", label: "问题-对策" },
  { value: "总分", label: "总分" },
  { value: "分总", label: "分总" },
  { value: "并列展开", label: "并列展开" },
  { value: "时间推进", label: "时间推进" },
  { value: "步骤推进", label: "步骤推进" },
];

const DIFFICULTY_OPTIONS = [
  { value: "easy", label: "简单" },
  { value: "medium", label: "中等" },
  { value: "hard", label: "困难" },
];

const VALUE_LABELS = {
  easy: "简单",
  medium: "中等",
  hard: "困难",
  main_idea: "主旨中心类",
  continuation: "接语选择题",
  sentence_order: "语句排序题",
  sentence_fill: "语句填空题",
  center_understanding: "中心理解题",
  title_selection: "标题填入",
  turning_relation_focus: "转折关系聚焦",
  cause_effect__conclusion_focus: "因果结论聚焦",
  necessary_condition_countermeasure: "必要条件与对策",
  theme_word_focus: "主题词聚焦",
  structure_summary: "结构概括",
  local_paragraph_summary: "局部段意概括",
  plot_continuation: "情节续写",
  character_extension: "人物延展",
  theme_extension: "主题延展",
  foreshadow_recall: "伏笔呼应",
  setting_alignment: "环境衔接",
  emotion_progression: "情绪递进",
  conflict_resolution: "冲突解决",
  ending_resolution: "结尾收束",
  value_expression: "价值表达",
  head_tail_lock: "首尾锁定",
  deterministic_binding: "确定性捆绑",
  discourse_logic: "行文逻辑",
  timeline_action_sequence: "时间行动顺序",
  dual_anchor_lock: "双锚点锁定",
  carry_parallel_expand: "承接并列展开",
  viewpoint_reason_action: "观点-原因-行动",
  problem_solution_case_blocks: "问题-对策-案例",
  opening_summary: "开头总起",
  opening_topic_intro: "开头引入",
  middle_carry_previous: "中间承上",
  middle_lead_next: "中间启下",
  middle_bridge_both_sides: "承上启下",
  ending_summary: "结尾总结",
  ending_countermeasure: "结尾对策",
  approved: "已通过",
  pending_review: "待复核",
  auto_failed: "自动校验未过",
  discarded: "已丢弃",
  generated: "已生成",
  generate: "生成",
  revising: "修订中",
  recommended: "推荐保留",
  hold: "继续复核",
  weak_candidate: "弱候选",
  confirm: "通过",
  discard: "丢弃",
  question_modify: "按参数重做",
  text_modify: "替换材料重做",
  manual_edit: "手工编辑",
  role_ambiguity_penalty: "角色歧义惩罚",
  standalone_penalty: "独立成段风险",
  overlong_penalty: "篇幅过长惩罚",
  example_dominance_penalty: "例子压过主旨",
  ambiguity_score: "歧义度",
  complexity_score: "复杂度",
  reasoning_depth_score: "推理深度",
  constraint_intensity_score: "约束强度",
};

const DECISION_REASON_LABELS = {
  recommended_stable_candidate: "候选稳定，可直接保留",
  recommended_candidate_requires_review: "候选质量高，但仍建议复核",
  hard_but_currently_weak_candidate: "难度不低，但当前质量仍偏弱",
  high_readiness_high_penalty: "可用度较高，但风险惩罚偏高",
  high_risk_but_not_high_difficulty: "风险偏高，但并不是高难候选",
  easy_but_weak_candidate: "不是 hard，只是当前质量偏弱",
  borderline_hold_candidate: "边界候选，建议继续 review",
  overall_weak_candidate: "整体偏弱，不建议包装成推荐态",
  material_scoring_missing: "当前材料缺少评分解释，先按保守态展示",
};

const REPAIR_REASON_LABELS = {
  role_ambiguity_repairable_risk: "主要风险在角色或指代歧义，适合修补",
  high_readiness_high_penalty: "可用度还行，但风险偏高，建议修补",
  hard_but_currently_weak_candidate: "难度不低，但当前版本偏弱，可尝试修补",
};

const QUALITY_NOTE_LABELS = {
  hard_but_currently_weak_candidate: "难度不低，但当前版本偏弱",
  difficulty_and_quality_balanced: "难度与质量大体平衡",
  not_hard_but_currently_weak_candidate: "不是 hard，是当前质量偏弱",
};

const FIELD_LABELS = {
  selection_state: "推荐状态",
  review_like_risk: "是否需复审",
  repair_suggested: "是否建议修复",
  difficulty_band_hint: "难度带提示",
  final_candidate_score: "最终得分",
  readiness_score: "可用性评分",
  total_penalty: "总处罚分",
  quality_note: "质量说明",
  decision_reason: "决策原因",
  repair_reason: "修复原因",
  key_penalties: "关键罚分",
  key_difficulty_dimensions: "关键难度维度",
  validator_errors: "校验错误",
  validator_warnings: "校验警告",
  current_status: "当前状态",
  latest_action: "最新动作",
};

const LOADING_STEPS = [
  { key: "collect", title: "整理构建参数", desc: "只提交后端真实消费字段。" },
  { key: "request", title: "调用生成接口", desc: "等待 /api/v1/questions/generate 返回。" },
  { key: "render", title: "切换到结果区", desc: "渲染结果卡并补充控件与动作区。" },
];

const state = {
  batchId: null,
  items: [],
  controlsByItem: {},
  replacementsByItem: {},
  loadingStep: "collect",
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

function humanize(value, dictionary = VALUE_LABELS) {
  const key = String(value == null ? "" : value).trim();
  if (!key) return "未提供";
  return dictionary[key] || key;
}

function humanizeCode(value, dictionary = {}) {
  const key = String(value == null ? "" : value).trim();
  if (!key) return "未提供";
  if (dictionary[key]) return dictionary[key];
  return key.replace(/_/g, " ").replace(/\s+/g, " ").trim();
}

function fieldLabel(key) {
  return FIELD_LABELS[key] || humanizeCode(key);
}

function safeFloat(value) {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : null;
}

function formatScore(value) {
  const numeric = safeFloat(value);
  return numeric == null ? "-" : numeric.toFixed(4);
}

function truthyBoolean(value) {
  if (typeof value === "boolean") return value;
  const text = String(value == null ? "" : value).trim().toLowerCase();
  return ["1", "true", "yes", "on"].includes(text);
}

function screenId(name) {
  return `${name}Screen`;
}

function switchScreen(name) {
  ["builder", "loading", "result"].forEach((screenName) => {
    const node = $(screenId(screenName));
    if (node) node.classList.toggle("active", screenName === name);
  });
}

function populateSelect(node, options) {
  node.innerHTML = "";
  options.forEach((option) => {
    const element = document.createElement("option");
    element.value = option.value;
    element.textContent = option.label;
    node.appendChild(element);
  });
}

function setBanner(id, message, tone = "error") {
  const node = $(id);
  if (!node) return;
  node.hidden = !message;
  node.textContent = message || "";
  node.classList.remove("status-banner-error", "status-banner-info");
  node.classList.add(tone === "info" ? "status-banner-info" : "status-banner-error");
}

function showToast(message, tone = "success") {
  let root = $("actionToastRoot");
  if (!root) {
    root = document.createElement("div");
    root.id = "actionToastRoot";
    root.className = "action-toast-root";
    document.body.appendChild(root);
  }
  const node = document.createElement("div");
  node.className = `action-toast action-toast-${tone}`;
  node.textContent = message;
  root.appendChild(node);
  setTimeout(() => node.classList.add("is-visible"), 10);
  setTimeout(() => {
    node.classList.remove("is-visible");
    setTimeout(() => node.remove(), 220);
  }, 2200);
}

function setButtonBusy(button, busy, text) {
  if (!button) return;
  if (busy) {
    if (!button.dataset.originalText) button.dataset.originalText = button.textContent;
    button.disabled = true;
    button.textContent = text || "处理中...";
    return;
  }
  button.disabled = false;
  button.textContent = button.dataset.originalText || button.textContent;
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
        : payload?.error?.message || payload?.detail || payload?.error?.detail || "请求失败";
    throw new Error(localizeErrorMessage(typeof message === "string" ? message : JSON.stringify(message)));
  }
  return payload;
}

function localizeErrorMessage(message) {
  const text = String(message == null ? "" : message).trim();
  if (!text) return "请求失败，请稍后重试。";
  if (/Blocked questions cannot be confirmed before revision/i.test(text)) {
    return "当前题目在修订完成前不能确认通过，请先处理修订动作。";
  }
  if (/question_modify cannot cross the material boundary; use text_modify instead/i.test(text)) {
    return "当前题目不能跨材料边界做参数重做，请改用“替换材料重做”。";
  }
  if (/Failed to call configured LLM provider/i.test(text) && /getaddrinfo failed/i.test(text)) {
    return "上游生成服务暂时不可用，当前网络解析失败，请稍后重试。";
  }
  if (/Failed to call configured LLM provider/i.test(text)) {
    return "上游生成服务暂时不可用，请稍后重试。";
  }
  if (/Internal Server Error/i.test(text)) {
    return "服务端暂时异常，请稍后重试。";
  }
  return text;
}

function syncCountValue() {
  $("countValue").textContent = $("count").value;
}

function renderSpecialTypeOptions() {
  const focus = $("questionFocus").value || "";
  populateSelect($("specialType"), SPECIAL_TYPE_OPTIONS[focus] || SPECIAL_TYPE_OPTIONS[""]);
}

function renderLoadingSteps() {
  const root = $("loadingSteps");
  if (!root) return;
  root.innerHTML = "";
  const activeIndex = LOADING_STEPS.findIndex((item) => item.key === state.loadingStep);
  LOADING_STEPS.forEach((step, index) => {
    const card = document.createElement("div");
    card.className = "step-card";
    if (index <= activeIndex) card.classList.add("active");
    if (index < activeIndex) card.classList.add("done");
    card.innerHTML = `
      <div class="step-index">${index + 1}</div>
      <div>
        <div class="step-title">${escapeHtml(step.title)}</div>
        <div class="step-desc">${escapeHtml(step.desc)}</div>
      </div>
    `;
    root.appendChild(card);
  });
}

function setLoadingState(stepKey, title, description) {
  state.loadingStep = stepKey;
  renderLoadingSteps();
  $("loadingNode").textContent = title || "处理中...";
  $("loadingNodeDesc").textContent = description || "正在等待服务端返回。";
}

function collectSourceQuestionPayload() {
  const payload = {
    passage: $("sourceQuestionPassage").value.trim(),
    stem: $("sourceQuestionStem").value.trim(),
    options: {
      A: $("sourceOptionA").value.trim(),
      B: $("sourceOptionB").value.trim(),
      C: $("sourceOptionC").value.trim(),
      D: $("sourceOptionD").value.trim(),
    },
    answer: $("sourceQuestionAnswer").value.trim() || null,
    analysis: $("sourceQuestionAnalysis").value.trim() || null,
  };
  const hasAnyContent = Boolean(
    payload.passage || payload.stem || Object.values(payload.options).some(Boolean) || payload.answer || payload.analysis,
  );
  return hasAnyContent ? payload : null;
}

function inferQuestionFocus(sourceQuestion) {
  const stem = String(sourceQuestion?.stem || "");
  if (/排序|重新排列|语序正确/.test(stem)) return "sentence_order";
  if (/填入|横线|最恰当/.test(stem)) return "sentence_fill";
  if (/标题/.test(stem)) return "main_idea";
  if (/接在|接语|衔接|续写/.test(stem)) return "continuation";
  return "center_understanding";
}

function collectPreferredGenres() {
  const raw = $("preferredGenres").value.trim();
  if (!raw) return [];
  return raw
    .replace(/，/g, ",")
    .split(",")
    .map((part) => part.trim())
    .filter(Boolean);
}

function buildGeneratePayload() {
  const sourceQuestion = collectSourceQuestionPayload();
  const preferredGenres = collectPreferredGenres();
  const questionFocus = $("questionFocus").value || (sourceQuestion ? inferQuestionFocus(sourceQuestion) : "");
  const specialType = $("specialType").value || "";
  const payload = {
    question_focus: questionFocus,
    difficulty_level: $("difficultyLevel").value || "medium",
    text_direction: $("textDirection").value || null,
    material_structure: $("materialStructure").value || null,
    special_question_types: specialType ? [specialType] : [],
    count: Number($("count").value || 1),
    source_question: sourceQuestion,
  };
  if (preferredGenres.length) {
    payload.material_policy = { preferred_document_genres: preferredGenres };
  }
  return payload;
}

async function autoDetectSourceQuestion() {
  const rawText = $("sourceQuestionPassage").value.trim();
  if (!rawText) {
    setBanner("builderError", "请先把整道原题粘贴到“原题全文”里。");
    return;
  }

  setBanner("builderError", "");
  const button = $("sourceQuestionDetectBtn");
  const statusNode = $("sourceQuestionParseStatus");
  setButtonBusy(button, true, "识别中...");
  statusNode.hidden = false;
  statusNode.textContent = "正在自动拆题并回填...";

  try {
    const response = await apiFetch("/api/v1/questions/source-question/parse", {
      method: "POST",
      body: JSON.stringify({ raw_text: rawText }),
    });
    const parsed = response.source_question || {};
    $("sourceQuestionPassage").value = parsed.passage || "";
    $("sourceQuestionStem").value = parsed.stem || "";
    $("sourceOptionA").value = parsed.options?.A || "";
    $("sourceOptionB").value = parsed.options?.B || "";
    $("sourceOptionC").value = parsed.options?.C || "";
    $("sourceOptionD").value = parsed.options?.D || "";
    $("sourceQuestionAnswer").value = parsed.answer || "";
    $("sourceQuestionAnalysis").value = parsed.analysis || "";
    if (!$("questionFocus").value) {
      $("questionFocus").value = inferQuestionFocus(parsed);
      renderSpecialTypeOptions();
    }
    statusNode.textContent = "已完成拆题并回填，你可以直接提交生成。";
    showToast("参考母题已自动拆解");
  } catch (error) {
    statusNode.textContent = "自动拆题失败，请保留原文并手动补充必要字段。";
    setBanner("builderError", `自动拆题失败：${error.message}`);
  } finally {
    setButtonBusy(button, false);
  }
}

async function generateQuestions(event) {
  event.preventDefault();
  setBanner("builderError", "");
  setBanner("loadingError", "");

  const payload = buildGeneratePayload();
  if (!payload.question_focus) {
    setBanner("builderError", "请先选择题型，或先提供参考母题让系统自动识别。");
    return;
  }

    setLoadingState("collect", "整理请求参数", "构建区只提交后端真实消费字段。");
  switchScreen("loading");

  try {
    setLoadingState("request", "调用生成接口", "正在请求 /api/v1/questions/generate ...");
    const response = await apiFetch("/api/v1/questions/generate", {
      method: "POST",
      body: JSON.stringify(payload),
    });

    state.batchId = response.batch_id || null;
    state.items = Array.isArray(response.items) ? response.items : [];
    state.controlsByItem = {};
    state.replacementsByItem = {};

    setLoadingState("render", "切换到结果页", "生成成功，正在渲染结果卡。");
    renderResults();
    switchScreen("result");

    void Promise.all(
      state.items.map(async (item) => {
        if (!item?.item_id) return;
        try {
          await loadControlsForItem(item.item_id);
        } catch (_error) {
          // Controls load failure should not break the result screen.
        }
      }),
    );
  } catch (error) {
    setBanner("loadingError", error.message);
    switchScreen("builder");
    setBanner("builderError", `生成失败：${error.message}`);
  }
}

function renderOptions(options) {
  const entries = Object.entries(options || {}).filter(([, value]) => String(value || "").trim());
  if (!entries.length) {
    return '<div class="empty-state">当前没有可展示的选项。</div>';
  }
  return entries
    .map(
      ([key, value]) => `
        <div class="option-item">
          <span class="option-key">${escapeHtml(key)}</span>
          <span>${escapeHtml(value)}</span>
        </div>
      `,
    )
    .join("");
}

function chipToneForSelection(selectionState) {
  if (selectionState === "recommended") return "recommended";
  if (selectionState === "weak_candidate") return "weak";
  if (selectionState === "hold") return "hold";
  return "neutral";
}

function statusChipClass(status) {
  if (status === "approved") return "status-approved";
  if (status === "discarded") return "status-danger";
  if (status === "auto_failed") return "status-warn";
  return "status-pending";
}

function getMaterialSource(item) {
  return item?.material_source || item?.material_selection?.source || {};
}

function getFeedbackSnapshot(item) {
  if (item && typeof item.feedback_snapshot === "object" && item.feedback_snapshot) {
    return item.feedback_snapshot;
  }

  const materialSource = getMaterialSource(item);
  if (materialSource && typeof materialSource.feedback_snapshot === "object" && materialSource.feedback_snapshot) {
    return materialSource.feedback_snapshot;
  }

  const scoring =
    (materialSource && typeof materialSource.scoring === "object" && materialSource.scoring) ||
    (materialSource && typeof materialSource.selected_task_scoring === "object" && materialSource.selected_task_scoring) ||
    {};
  const decisionMeta =
    (materialSource && typeof materialSource.decision_meta === "object" && materialSource.decision_meta) || {};
  const rankingMeta =
    (materialSource && typeof materialSource.ranking_meta === "object" && materialSource.ranking_meta) || {};

  if (!Object.keys(scoring).length && !Object.keys(decisionMeta).length) {
    return null;
  }

  const scoringSummary =
    decisionMeta && typeof decisionMeta.scoring_summary === "object" ? decisionMeta.scoring_summary : {};
  const difficultyTrace =
    scoring && typeof scoring.difficulty_trace === "object" ? scoring.difficulty_trace : {};
  const bandDecision =
    difficultyTrace && typeof difficultyTrace.band_decision === "object" ? difficultyTrace.band_decision : {};
  const difficultyVector =
    scoring && typeof scoring.difficulty_vector === "object" ? scoring.difficulty_vector : {};
  const riskPenalties =
    scoring && typeof scoring.risk_penalties === "object" ? scoring.risk_penalties : {};

  return {
    selection_state: decisionMeta.selection_state ?? null,
    review_like_risk: truthyBoolean(decisionMeta.review_like_risk),
    repair_suggested: truthyBoolean(decisionMeta.repair_suggested),
    decision_reason: decisionMeta.decision_reason ?? null,
    repair_reason: decisionMeta.repair_reason ?? null,
    quality_difficulty_note: decisionMeta.quality_difficulty_note || bandDecision.quality_difficulty_note || null,
    final_candidate_score: scoring.final_candidate_score ?? scoringSummary.final_candidate_score ?? null,
    readiness_score: scoring.readiness_score ?? scoringSummary.readiness_score ?? null,
    total_penalty: scoringSummary.total_penalty ?? null,
    difficulty_band_hint: scoring.difficulty_band_hint || scoringSummary.difficulty_band_hint || null,
    difficulty_vector: difficultyVector,
    key_penalties:
      (decisionMeta && typeof decisionMeta.key_penalties === "object" && decisionMeta.key_penalties) || riskPenalties,
    key_difficulty_dimensions:
      (decisionMeta &&
        typeof decisionMeta.key_difficulty_dimensions === "object" &&
        decisionMeta.key_difficulty_dimensions) ||
      difficultyVector,
    recommended:
      typeof scoring.recommended === "boolean"
        ? scoring.recommended
        : truthyBoolean(scoringSummary.recommended),
    needs_review:
      typeof scoring.needs_review === "boolean"
        ? scoring.needs_review
        : truthyBoolean(scoringSummary.needs_review),
    ranking_meta: rankingMeta,
  };
}

function getFailedChecks(item) {
  const checks = item?.validation_result?.checks;
  if (!checks || typeof checks !== "object") return [];

  return Object.entries(checks)
    .filter(([, payload]) => payload && typeof payload === "object" && payload.passed === false)
    .slice(0, 4)
    .map(([key, payload]) => ({
      name: payload.reason || key,
      source: payload.source || "",
      actual: payload.actual,
      threshold: payload.threshold,
      allowedRange: payload.allowed_range,
      difficultyBand: payload.difficulty_band,
    }));
}

function getValidationMessages(item, key) {
  const messages = item?.validation_result?.[key];
  if (!Array.isArray(messages)) return [];
  return messages
    .map((entry) => String(entry || "").trim())
    .filter(Boolean)
    .slice(0, 4);
}

function normalizeDisplayPairs(record, dictionary = VALUE_LABELS, limit = 3) {
  if (!record || typeof record !== "object") return [];
  return Object.entries(record)
    .filter(([, value]) => safeFloat(value) != null || String(value || "").trim())
    .slice(0, limit)
    .map(([key, value]) => ({
      key,
      label: humanize(key, dictionary),
      value,
    }));
}

function renderSignalSummary(feedback) {
  if (!feedback) {
    return '<div class="empty-state">当前结果没有可用的解释层字段，先按基础题卡展示。</div>';
  }

  const selectionState = feedback.selection_state || "未提供";
  const tone = chipToneForSelection(feedback.selection_state);
  const reviewLikeRisk = feedback.review_like_risk ? "是" : "否";
  const repairSuggested = feedback.repair_suggested ? "是" : "否";

  return `
    <div class="status-strip">
      <span class="signal-chip signal-chip-${tone}">${escapeHtml(fieldLabel("selection_state"))}：${escapeHtml(
        humanize(selectionState),
      )}</span>
      <span class="signal-chip signal-chip-neutral">${escapeHtml(fieldLabel("review_like_risk"))}：${escapeHtml(
        reviewLikeRisk,
      )}</span>
      <span class="signal-chip signal-chip-neutral">${escapeHtml(fieldLabel("repair_suggested"))}：${escapeHtml(
        repairSuggested,
      )}</span>
    </div>
  `;
}

function renderMetricCards(feedback) {
  if (!feedback) {
    return `
      <div class="signal-grid">
        <div class="mini-card"><strong>${escapeHtml(fieldLabel("selection_state"))}</strong><div>未提供</div></div>
        <div class="mini-card"><strong>${escapeHtml(fieldLabel("difficulty_band_hint"))}</strong><div>未提供</div></div>
        <div class="mini-card"><strong>${escapeHtml(fieldLabel("final_candidate_score"))}</strong><div>-</div></div>
      </div>
    `;
  }

  return `
    <div class="signal-grid">
      <div class="mini-card">
        <strong>${escapeHtml(fieldLabel("selection_state"))}</strong>
        <div>${escapeHtml(humanize(feedback.selection_state || "未提供"))}</div>
      </div>
      <div class="mini-card">
        <strong>${escapeHtml(fieldLabel("difficulty_band_hint"))}</strong>
        <div>${escapeHtml(humanize(feedback.difficulty_band_hint || "未提供"))}</div>
      </div>
      <div class="mini-card">
        <strong>${escapeHtml(fieldLabel("final_candidate_score"))}</strong>
        <div>${escapeHtml(formatScore(feedback.final_candidate_score))}</div>
      </div>
      <div class="mini-card">
        <strong>${escapeHtml(fieldLabel("readiness_score"))}</strong>
        <div>${escapeHtml(formatScore(feedback.readiness_score))}</div>
      </div>
      <div class="mini-card">
        <strong>${escapeHtml(fieldLabel("total_penalty"))}</strong>
        <div>${escapeHtml(formatScore(feedback.total_penalty))}</div>
      </div>
      <div class="mini-card">
        <strong>${escapeHtml(fieldLabel("quality_note"))}</strong>
        <div>${escapeHtml(humanize(feedback.quality_difficulty_note, QUALITY_NOTE_LABELS))}</div>
      </div>
    </div>
  `;
}

function renderExplainList(feedback, item) {
  const penaltyPairs = normalizeDisplayPairs(feedback?.key_penalties, VALUE_LABELS, 3);
  const difficultyPairs = normalizeDisplayPairs(
    feedback?.key_difficulty_dimensions || feedback?.difficulty_vector,
    VALUE_LABELS,
    3,
  );
  const failedChecks = getFailedChecks(item);
  const validationErrors = getValidationMessages(item, "errors");
  const validationWarnings = getValidationMessages(item, "warnings");

  const items = [];
  if (feedback?.decision_reason) {
    items.push(`
      <li>
        <strong>${escapeHtml(fieldLabel("decision_reason"))}</strong><br />
        ${escapeHtml(humanize(feedback.decision_reason, DECISION_REASON_LABELS))}
      </li>
    `);
  }
  if (feedback?.repair_reason) {
    items.push(`
      <li>
        <strong>${escapeHtml(fieldLabel("repair_reason"))}</strong><br />
        ${escapeHtml(humanize(feedback.repair_reason, REPAIR_REASON_LABELS))}
      </li>
    `);
  }
  if (penaltyPairs.length) {
    items.push(`
      <li>
        <strong>${escapeHtml(fieldLabel("key_penalties"))}</strong><br />
        ${penaltyPairs
          .map((entry) => `${escapeHtml(entry.label)} = ${escapeHtml(formatScore(entry.value))}`)
          .join("<br />")}
      </li>
    `);
  }
  if (difficultyPairs.length) {
    items.push(`
      <li>
        <strong>${escapeHtml(fieldLabel("key_difficulty_dimensions"))}</strong><br />
        ${difficultyPairs
          .map((entry) => `${escapeHtml(entry.label)} = ${escapeHtml(formatScore(entry.value))}`)
          .join("<br />")}
      </li>
    `);
  }
  if (failedChecks.length) {
    items.push(`
      <li>
        <strong>规则侧失败项</strong><br />
        ${failedChecks
          .map((entry) => {
            const parts = [humanizeCode(entry.name)];
            if (entry.actual != null) parts.push(`actual=${JSON.stringify(entry.actual)}`);
            if (entry.threshold != null) parts.push(`threshold=${JSON.stringify(entry.threshold)}`);
            if (entry.allowedRange != null) parts.push(`allowed=${JSON.stringify(entry.allowedRange)}`);
            if (entry.difficultyBand != null) parts.push(`band=${entry.difficultyBand}`);
            return escapeHtml(parts.join(" | "));
          })
          .join("<br />")}
      </li>
    `);
  }
  if (validationErrors.length) {
    items.push(`
      <li>
        <strong>${escapeHtml(fieldLabel("validator_errors"))}</strong><br />
        ${validationErrors.map((entry) => escapeHtml(entry)).join("<br />")}
      </li>
    `);
  }
  if (validationWarnings.length) {
    items.push(`
      <li>
        <strong>${escapeHtml(fieldLabel("validator_warnings"))}</strong><br />
        ${validationWarnings.map((entry) => escapeHtml(entry)).join("<br />")}
      </li>
    `);
  }

  if (!items.length) {
    return '<div class="empty-state">当前卡片没有更多可解释字段，先保留基础结果展示。</div>';
  }

  return `<ul class="explain-list">${items.join("")}</ul>`;
}

function renderQuestionModifySection(itemId) {
  const panel = state.controlsByItem[itemId];
  if (!panel || !Array.isArray(panel.controls)) {
    return '<div class="inline-feedback">参数重做控件加载中...</div>';
  }

  if (!panel.controls.length) {
    return '<div class="inline-feedback">当前题目没有开放的参数重做控件。</div>';
  }

  const editableControls = panel.controls.filter((control) => !control.read_only && control.mapped_action === "question_modify");
  if (!editableControls.length) {
    return '<div class="inline-feedback">当前题目没有开放的参数重做控件。</div>';
  }

  const fields = editableControls
    .slice(0, 6)
    .map((control) => {
      const options = Array.isArray(control.options) ? control.options : [];
      const selectedValue = control.current_value == null ? "" : String(control.current_value);
      return `
        <label class="field-compact">
          <span>${escapeHtml(humanize(control.label || control.control_key))}</span>
          <select class="question-modify-input" data-item-id="${itemId}" data-control-key="${escapeHtml(control.control_key)}">
            <option value="">不调整</option>
            ${options
              .map((option) => {
                const value = option.value == null ? "" : String(option.value);
                const selected = value === selectedValue ? " selected" : "";
                return `<option value="${escapeHtml(value)}"${selected}>${escapeHtml(
                  humanize(option.label || value),
                )}</option>`;
              })
              .join("")}
          </select>
          <small class="field-help">${escapeHtml(control.description || "沿用后端返回的控件定义。")}</small>
        </label>
      `;
    })
    .join("");

  return `<div class="builder-stack">${fields}</div>`;
}

function renderReplacementOptions(itemId) {
  const replacements = state.replacementsByItem[itemId];
  if (!replacements || !Array.isArray(replacements.items) || !replacements.items.length) {
    return '<option value="">先点击“加载备选材料”</option>';
  }

  return [
    '<option value="">请选择备选材料</option>',
    ...replacements.items.map((entry) => {
      const label = [entry.article_title, entry.source_name, entry.document_genre]
        .map((part) => String(part || "").trim())
        .filter(Boolean)
        .join(" / ");
      return `<option value="${escapeHtml(entry.material_id)}">${escapeHtml(label || entry.material_id)}</option>`;
    }),
  ].join("");
}

function buildQuestionCard(item, index) {
  const generated = item.generated_question || {};
  const material = item.material_selection || {};
  const materialSource = getMaterialSource(item);
  const feedback = getFeedbackSnapshot(item);
  const currentStatus = item.current_status || "generated";
  const approved = currentStatus === "approved";
  const discarded = currentStatus === "discarded";
  const materialText = item.material_text || material.text || "";
  const originalMaterial = material.original_text || material.text || "";

  const card = document.createElement("section");
  card.className = "question-card";
  card.dataset.itemId = item.item_id;
  card.innerHTML = `
    <div class="question-main">
      <div class="question-head">
        <div>
          <h3>题目 ${index}</h3>
          <div class="question-meta">
            <span class="chip status ${statusChipClass(currentStatus)}">${escapeHtml(humanize(currentStatus))}</span>
            <span class="chip">${escapeHtml(humanize(item.question_type))}</span>
            <span class="chip">${escapeHtml(humanize(item.business_subtype || item.pattern_id || "未提供"))}</span>
            <span class="chip">${escapeHtml(humanize(item.difficulty_target || "medium"))}</span>
          </div>
        </div>
      </div>

      <div class="question-box">
        <div class="passage-label">题目主内容</div>
        <div class="passage-preview">${escapeHtml(materialText || "暂无材料文本")}</div>
        <div class="passage-label" style="margin-top: 18px;">题干</div>
        <div class="question-stem">${escapeHtml(generated.stem || item.stem_text || "暂无题干")}</div>
        <div class="option-list">${renderOptions(generated.options || {})}</div>
        <div class="answer-row"><strong>答案：</strong>${escapeHtml(generated.answer || "未提供")}</div>
        <div class="analysis-row"><strong>解析：</strong>${escapeHtml(generated.analysis || "未提供")}</div>
      </div>

      <div class="system-box" style="margin-top: 16px;">
        <div class="section-title">结构与状态</div>
        ${renderSignalSummary(feedback)}
        <div style="margin-top: 14px;">${renderMetricCards(feedback)}</div>
      </div>

      <div class="material-box" style="margin-top: 16px;">
        <div class="section-title">风险与建议</div>
        ${renderExplainList(feedback, item)}
      </div>

      <details class="result-collapse" style="margin-top: 16px;">
        <summary>查看材料来源与上下文</summary>
        <div class="collapse-body">
          <div class="material-box">
            <div class="material-grid">
              <div class="mini-card">
                <strong>材料来源</strong>
                <div>${escapeHtml(materialSource.source_name || materialSource.site || "-")}</div>
              </div>
              <div class="mini-card">
                <strong>文章标题</strong>
                <div>${escapeHtml(materialSource.article_title || "-")}</div>
              </div>
              <div class="mini-card">
                <strong>文体</strong>
                <div>${escapeHtml(material.document_genre || materialSource.document_genre || "-")}</div>
              </div>
              <div class="mini-card">
                <strong>材料结构</strong>
                <div>${escapeHtml(material.material_structure_label || "未提供")}</div>
              </div>
            </div>
            <div class="inline-feedback">原始材料</div>
            <pre class="compact-pre">${escapeHtml(originalMaterial || "未提供")}</pre>
          </div>
        </div>
      </details>

      <details class="result-collapse" style="margin-top: 16px;">
        <summary>参数重做</summary>
        <div class="collapse-body support-box">
          ${renderQuestionModifySection(item.item_id)}
          <div class="action-row">
            <button type="button" class="secondary-btn" data-action="question-modify" data-item-id="${item.item_id}" ${
              discarded ? "disabled" : ""
            }>按参数重做</button>
          </div>
          <div class="inline-feedback">按钮直接调用后端既有参数重做动作，不在前端定义额外语义。</div>
        </div>
      </details>

      <details class="result-collapse" style="margin-top: 16px;">
        <summary>材料重做</summary>
        <div class="collapse-body support-box">
          <div class="action-row">
            <button type="button" class="secondary-btn" data-action="load-replacements" data-item-id="${item.item_id}" ${
              discarded ? "disabled" : ""
            }>加载备选材料</button>
            <select class="replacement-select" data-item-id="${item.item_id}" ${discarded ? "disabled" : ""}>
              ${renderReplacementOptions(item.item_id)}
            </select>
            <button type="button" class="secondary-btn" data-action="apply-replacement" data-item-id="${item.item_id}" ${
              discarded ? "disabled" : ""
            }>使用备选材料重做</button>
          </div>
          <label class="field-compact">
            <span>自贴材料</span>
            <textarea class="custom-material-input" data-item-id="${item.item_id}" rows="4" ${
              discarded ? "disabled" : ""
            } placeholder="可直接粘贴一段替换材料，再执行 text_modify。"></textarea>
          </label>
          <div class="action-row">
            <button type="button" class="secondary-btn" data-action="apply-custom-material" data-item-id="${item.item_id}" ${
              discarded ? "disabled" : ""
            }>使用自贴材料重做</button>
          </div>
        </div>
      </details>

      <details class="result-collapse" style="margin-top: 16px;">
        <summary>手工编辑</summary>
        <div class="collapse-body support-box">
          <label class="field-compact">
            <span>材料</span>
            <textarea class="manual-material" data-item-id="${item.item_id}" rows="5" ${
              discarded ? "disabled" : ""
            }>${escapeHtml(materialText)}</textarea>
          </label>
          <label class="field-compact">
            <span>题干</span>
            <textarea class="manual-stem" data-item-id="${item.item_id}" rows="2" ${
              discarded ? "disabled" : ""
            }>${escapeHtml(generated.stem || item.stem_text || "")}</textarea>
          </label>
          <div class="builder-grid">
            <label class="field-compact">
              <span>选项 A</span>
              <textarea class="manual-option" data-item-id="${item.item_id}" data-option="A" rows="2" ${
                discarded ? "disabled" : ""
              }>${escapeHtml(generated.options?.A || "")}</textarea>
            </label>
            <label class="field-compact">
              <span>选项 B</span>
              <textarea class="manual-option" data-item-id="${item.item_id}" data-option="B" rows="2" ${
                discarded ? "disabled" : ""
              }>${escapeHtml(generated.options?.B || "")}</textarea>
            </label>
            <label class="field-compact">
              <span>选项 C</span>
              <textarea class="manual-option" data-item-id="${item.item_id}" data-option="C" rows="2" ${
                discarded ? "disabled" : ""
              }>${escapeHtml(generated.options?.C || "")}</textarea>
            </label>
            <label class="field-compact">
              <span>选项 D</span>
              <textarea class="manual-option" data-item-id="${item.item_id}" data-option="D" rows="2" ${
                discarded ? "disabled" : ""
              }>${escapeHtml(generated.options?.D || "")}</textarea>
            </label>
          </div>
          <div class="builder-grid">
            <label class="field-compact">
              <span>答案</span>
              <select class="manual-answer" data-item-id="${item.item_id}" ${discarded ? "disabled" : ""}>
                <option value="A"${generated.answer === "A" ? " selected" : ""}>A</option>
                <option value="B"${generated.answer === "B" ? " selected" : ""}>B</option>
                <option value="C"${generated.answer === "C" ? " selected" : ""}>C</option>
                <option value="D"${generated.answer === "D" ? " selected" : ""}>D</option>
              </select>
            </label>
            <label class="field-compact">
              <span>解析</span>
              <textarea class="manual-analysis" data-item-id="${item.item_id}" rows="4" ${
                discarded ? "disabled" : ""
              }>${escapeHtml(generated.analysis || "")}</textarea>
            </label>
          </div>
          <div class="action-row">
            <button type="button" class="primary-btn" data-action="manual-save" data-item-id="${item.item_id}" ${
              discarded ? "disabled" : ""
            }>保存手工编辑</button>
          </div>
        </div>
      </details>
    </div>

    <div class="question-actions">
      <button type="button" class="success-btn ${approved ? "is-approved" : ""}" data-action="confirm" data-item-id="${item.item_id}" ${
        approved || discarded ? "disabled" : ""
      }>${approved ? "已通过" : "确认通过"}</button>
      <button type="button" class="danger-btn" data-action="discard" data-item-id="${item.item_id}" ${
        discarded ? "disabled" : ""
      }>${discarded ? "已丢弃" : "丢弃题目"}</button>
      <div class="inline-feedback">
        ${escapeHtml(fieldLabel("current_status"))}：${escapeHtml(humanize(currentStatus))}<br />
        ${escapeHtml(fieldLabel("latest_action"))}：${escapeHtml(humanize(item.latest_action || "generate"))}
      </div>
    </div>
  `;
  return card;
}

function renderResultSummary() {
  const root = $("resultSummary");
  if (!root) return;

  if (!state.items.length) {
    root.innerHTML = "";
    return;
  }

  const selectionCounts = { recommended: 0, hold: 0, weak_candidate: 0, unknown: 0 };
  state.items.forEach((item) => {
    const feedback = getFeedbackSnapshot(item);
    const key = feedback?.selection_state || "unknown";
    selectionCounts[key] = (selectionCounts[key] || 0) + 1;
  });

  root.innerHTML = `
    <span class="signal-chip signal-chip-recommended">推荐保留：${selectionCounts.recommended || 0}</span>
    <span class="signal-chip signal-chip-hold">继续复核：${selectionCounts.hold || 0}</span>
    <span class="signal-chip signal-chip-weak">弱候选：${selectionCounts.weak_candidate || 0}</span>
    <span class="signal-chip signal-chip-neutral">结果卡：${state.items.length}</span>
  `;
}

function renderResults() {
  const list = $("resultList");
  if (!list) return;

  if (state.batchId) {
    $("resultBatchInfo").textContent = `批次 ${state.batchId}，共返回 ${state.items.length} 个结果卡。`;
  } else {
    $("resultBatchInfo").textContent = "等待本次生成结果...";
  }

  renderResultSummary();
  list.innerHTML = "";

  if (!state.items.length) {
    list.innerHTML = '<div class="empty-state">本次没有返回结果卡，请回到构建区检查请求参数。</div>';
    return;
  }

  state.items.forEach((item, index) => {
    list.appendChild(buildQuestionCard(item, index + 1));
  });
}

function getCard(itemId) {
  return document.querySelector(`[data-item-id="${CSS.escape(itemId)}"]`);
}

function collectManualPatch(itemId) {
  const card = getCard(itemId);
  const options = {};
  card.querySelectorAll(`.manual-option[data-item-id="${itemId}"]`).forEach((node) => {
    options[node.dataset.option] = node.value.trim();
  });
  return {
    material_text: card.querySelector(".manual-material")?.value.trim() || "",
    stem: card.querySelector(".manual-stem")?.value.trim() || "",
    options,
    answer: card.querySelector(".manual-answer")?.value.trim() || "",
    analysis: card.querySelector(".manual-analysis")?.value.trim() || "",
  };
}

function collectQuestionModifyOverrides(itemId) {
  const card = getCard(itemId);
  const overrides = {};
  card.querySelectorAll(`.question-modify-input[data-item-id="${itemId}"]`).forEach((node) => {
    const key = node.dataset.controlKey;
    const value = String(node.value || "").trim();
    if (!key || !value) return;
    overrides[key] = value;
  });
  return overrides;
}

function upsertItem(nextItem) {
  state.items = state.items.map((current) => (current.item_id === nextItem.item_id ? nextItem : current));
}

async function loadControlsForItem(itemId) {
  const payload = await apiFetch(`/api/v1/questions/${itemId}/controls`);
  state.controlsByItem[itemId] = payload;
  renderResults();
}

async function loadReplacementMaterials(itemId) {
  const payload = await apiFetch(`/api/v1/questions/${itemId}/replacement-materials?limit=8`);
  state.replacementsByItem[itemId] = payload;
  renderResults();
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
      await loadReplacementMaterials(itemId);
      showToast("备选材料已加载");
      return;
    }

    if (action === "apply-replacement") {
      const card = getCard(itemId);
      const select = card?.querySelector(`.replacement-select[data-item-id="${itemId}"]`);
      const materialId = String(select?.value || "").trim();
      if (!materialId) {
        showToast("请先选择一条备选材料", "info");
        return;
      }

      setButtonBusy(button, true, "重做中...");
      const result = await apiFetch(`/api/v1/questions/${itemId}/review-actions`, {
        method: "POST",
        body: JSON.stringify({
          action: "text_modify",
          instruction: "use replacement material from demo",
          control_overrides: { material_id: materialId },
        }),
      });
      upsertItem(result.item);
      renderResults();
      void loadControlsForItem(itemId);
      showToast("已按备选材料重做");
      return;
    }

    if (action === "apply-custom-material") {
      const card = getCard(itemId);
      const materialText = card?.querySelector(`.custom-material-input[data-item-id="${itemId}"]`)?.value.trim() || "";
      if (!materialText) {
        showToast("请先粘贴替换材料", "info");
        return;
      }

      setButtonBusy(button, true, "重做中...");
      const result = await apiFetch(`/api/v1/questions/${itemId}/review-actions`, {
        method: "POST",
        body: JSON.stringify({
          action: "text_modify",
          instruction: "use custom material from demo",
          control_overrides: { material_text: materialText },
        }),
      });
      upsertItem(result.item);
      renderResults();
      void loadControlsForItem(itemId);
      showToast("已按自贴材料重做");
      return;
    }

    if (action === "question-modify") {
      const overrides = collectQuestionModifyOverrides(itemId);
      if (!Object.keys(overrides).length) {
        showToast("当前没有可提交的参数重做项", "info");
        return;
      }

      setButtonBusy(button, true, "提交中...");
      const result = await apiFetch(`/api/v1/questions/${itemId}/review-actions`, {
        method: "POST",
        body: JSON.stringify({
          action: "question_modify",
          instruction: "question modify from demo",
          control_overrides: overrides,
        }),
      });
      upsertItem(result.item);
      renderResults();
      void loadControlsForItem(itemId);
      showToast("参数重做已提交");
      return;
    }

    if (action === "manual-save") {
      setButtonBusy(button, true, "保存中...");
      const result = await apiFetch(`/api/v1/questions/${itemId}/review-actions`, {
        method: "POST",
        body: JSON.stringify({
          action: "manual_edit",
          instruction: "manual edit from demo",
          control_overrides: { manual_patch: collectManualPatch(itemId) },
        }),
      });
      upsertItem(result.item);
      renderResults();
      void loadControlsForItem(itemId);
      showToast("手工编辑已保存");
      return;
    }

    if (action === "confirm") {
      setButtonBusy(button, true, "确认中...");
      const result = await apiFetch(`/api/v1/questions/${itemId}/confirm`, {
        method: "POST",
        body: JSON.stringify({ operator: "demo" }),
      });
      upsertItem(result.item);
      renderResults();
      showToast("题目已通过");
      return;
    }

    if (action === "discard") {
      setButtonBusy(button, true, "丢弃中...");
      const result = await apiFetch(`/api/v1/questions/${itemId}/review-actions`, {
        method: "POST",
        body: JSON.stringify({ action: "discard", operator: "demo" }),
      });
      upsertItem(result.item);
      renderResults();
      showToast("题目已丢弃", "info");
    }
  } catch (error) {
    showToast(error.message || "操作失败", "info");
  } finally {
    setButtonBusy(button, false);
  }
}

async function exportApprovedBatch() {
  if (!state.batchId) {
    throw new Error("当前没有可导出的批次。");
  }
  const response = await fetch(`/api/v1/review/batches/${state.batchId}/delivery/export?format=markdown`);
  if (!response.ok) {
    throw new Error((await response.text()) || "导出失败");
  }

  const text = await response.text();
  const blob = new Blob([text], { type: "text/markdown;charset=utf-8" });
  const href = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = href;
  link.download = `batch_${state.batchId}.md`;
  document.body.appendChild(link);
  link.click();
  link.remove();
  URL.revokeObjectURL(href);
}

function initPage() {
  populateSelect($("questionFocus"), QUESTION_FOCUS_OPTIONS);
  populateSelect($("specialType"), SPECIAL_TYPE_OPTIONS[""]);
  populateSelect($("difficultyLevel"), DIFFICULTY_OPTIONS);
  populateSelect($("textDirection"), TEXT_DIRECTION_OPTIONS);
  populateSelect($("materialStructure"), MATERIAL_STRUCTURE_OPTIONS);

  $("count").addEventListener("input", syncCountValue);
  $("questionFocus").addEventListener("change", renderSpecialTypeOptions);
  $("generateForm").addEventListener("submit", generateQuestions);
  $("sourceQuestionDetectBtn").addEventListener("click", () => {
    autoDetectSourceQuestion().catch((error) => {
      setBanner("builderError", `自动拆题失败：${error.message}`);
    });
  });
  $("resultList").addEventListener("click", (event) => {
    handleResultAction(event).catch((error) => {
      showToast(error.message || "操作失败", "info");
    });
  });
  $("backToBuilderBtn").addEventListener("click", () => switchScreen("builder"));
  $("cancelLoadingBtn").addEventListener("click", () => switchScreen("builder"));
  $("exportApprovedBtn").addEventListener("click", () => {
    exportApprovedBatch()
      .then(() => showToast("导出成功"))
      .catch((error) => showToast(error.message || "导出失败", "info"));
  });

  syncCountValue();
  renderLoadingSteps();
}

document.addEventListener("DOMContentLoaded", initPage);
