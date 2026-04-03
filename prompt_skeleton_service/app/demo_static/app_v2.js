const PRIMARY_OPTIONS = [
  { label: "标题填入题", value: "标题填入题" },
  { label: "接语选择题", value: "接语选择题" },
  { label: "语句排序题", value: "语句排序题" },
  { label: "语句填空题", value: "语句填空题" },
  { label: "中心理解题", value: "中心理解题" },
];

const SECONDARY_BY_PRIMARY = {
  标题填入题: [
    { label: "选择标题", value: "选择标题" },
    { label: "主旨概括", value: "主旨概括" },
    { label: "结构概括", value: "结构概括" },
    { label: "局部段意概括", value: "局部段意概括" },
  ],
  接语选择题: [
    { label: "尾句直接承接", value: "尾句直接承接" },
    { label: "问题后接对策", value: "问题后接对策" },
    { label: "机制展开", value: "机制展开" },
    { label: "主题转分话题", value: "主题转分话题" },
    { label: "总结后开启新支点", value: "总结后开启新支点" },
    { label: "观点后接原因", value: "观点后接原因" },
    { label: "个案到宏观展开", value: "个案到宏观展开" },
    { label: "多分支聚焦", value: "多分支聚焦" },
    { label: "张力解释", value: "张力解释" },
    { label: "方法延展", value: "方法延展" },
  ],
  语句排序题: [
    { label: "双锚点锁定", value: "双锚点锁定" },
    { label: "承接并列展开", value: "承接并列展开" },
    { label: "观点-原因-行动排序", value: "观点-原因-行动排序" },
    { label: "问题-对策-案例排序", value: "问题—对策—案例排序" },
  ],
  语句填空题: [
    { label: "开头总起", value: "开头总起" },
    { label: "衔接过渡", value: "衔接过渡" },
    { label: "中段焦点切换", value: "中段焦点切换" },
    { label: "中段解释说明", value: "中段解释说明" },
    { label: "结尾总结", value: "结尾总结" },
    { label: "结尾升华", value: "结尾升华" },
    { label: "定位插入匹配", value: "定位插入匹配" },
    { label: "综合多点匹配", value: "综合多点匹配" },
  ],
  中心理解题: [{ label: "不指定（按中心理解默认）", value: "" }],
};

const TEXT_DIRECTION_OPTIONS = [
  { label: "不指定", value: "" },
  { label: "政策文", value: "政策文" },
  { label: "法条文", value: "法条文" },
  { label: "科普文", value: "科普文" },
  { label: "评论文", value: "评论文" },
  { label: "新闻报道", value: "新闻报道" },
  { label: "通知公告", value: "通知公告" },
  { label: "经验材料", value: "经验材料" },
];

const MATERIAL_STRUCTURE_OPTIONS = [
  { label: "不指定", value: "" },
  { label: "总分归纳", value: "总分归纳" },
  { label: "分总归纳", value: "分总归纳" },
  { label: "转折归旨", value: "转折归旨" },
  { label: "并列推进", value: "并列推进" },
  { label: "问题-对策", value: "问题-对策" },
  { label: "现象-分析", value: "现象-分析" },
  { label: "案例-结论", value: "案例-结论" },
  { label: "观点-论证", value: "观点-论证" },
  { label: "背景-核心结论", value: "背景-核心结论" },
  { label: "综合说明", value: "综合说明" },
];

const LOADING_STEPS = [
  { title: "参数解码", desc: "解析一级题卡、二级分类和难度请求。" },
  { title: "题卡匹配", desc: "确定题型、pattern 与标准骨架。" },
  { title: "材料获取", desc: "从本地材料池里选择可用文段。" },
  { title: "Prompt 组装", desc: "组合材料文本、制作方式和难度参数。" },
  { title: "题目生成", desc: "调用 LLM 输出标准题目格式。" },
  { title: "自动校验", desc: "执行结构、规则与考试风校验。" },
  { title: "LLM 审核", desc: "进入强制 LLM 审核，再交给用户确认。" },
];

const ACTION_LOADING_COPY = {
  generate: { title: "正在生成题目", desc: "系统正在按既定流程完成材料获取、题目生成、自动校验与 LLM 审核。" },
  fineTune: { title: "正在执行微调", desc: "系统会基于当前题目做小幅修改，并重新经过自动校验与 LLM 审核。" },
  textModify: { title: "正在更换文段", desc: "系统会切换到新的本地文段，再重新生成并审核当前题目。" },
  questionModify: { title: "正在按参数重做", desc: "系统会根据新的难度、迷惑度和 3 个错误项干扰方式重新制作题目。" },
  manualSave: { title: "正在保存编辑版本", desc: "系统会保存你当前手工修改后的材料、题干、选项和解析，并重新校验版本状态。" },
  confirm: { title: "正在确认题目", desc: "系统正在写入当前版本的确认结果。" },
  discard: { title: "正在作废题目", desc: "系统正在记录当前题目的作废结果。" },
};

const VALUE_LABELS = {
  easy: "简单",
  medium: "中等",
  hard: "困难",
  low: "低",
  low_medium: "低中",
  medium_high: "中高",
  high: "高",
  topic_shift: "主题偏移",
  overgeneralization: "过度泛化",
  function_misread: "功能误读",
  title_too_wide: "标题过宽",
  title_too_narrow: "标题过窄",
  catchy_but_offcore: "吸睛但偏核",
  old_topic_return: "回到旧话题",
  side_branch_shift: "支线偏移",
  abstract_overraise: "抽象拔高",
  surface_match: "表层匹配",
  logic_reverse: "逻辑反转",
  keyword_trap: "关键词陷阱",
  empty_summary: "空泛总结",
  background_replay: "背景重放",
  parallel_mechanism: "平行机制",
  judgement_repeat: "判断复述",
  case_detail_stay: "停留个案细节",
  macro_topic_drift: "宏观话题漂移",
  value_slogan: "价值口号",
  one_level_down: "顺下一层展开",
  problem_to_solution: "问题转对策",
  object_to_mechanism: "对象转机制",
  theme_to_subtopic: "主题转分论点",
  summary_to_new_pivot: "总结转新支点",
  judgement_to_reason: "判断转原因",
  case_to_macro: "个案转宏观",
  multi_branch_to_focus: "多分支转聚焦",
  tension_to_explanation: "张力转解释",
  analysis_to_method: "分析转方法",
};

const state = {
  batchId: null,
  items: [],
  histories: new Map(),
  loadingTimer: null,
};

const $ = (id) => document.getElementById(id);

function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function humanizeValueLabel(value, fallbackLabel = "") {
  return fallbackLabel || VALUE_LABELS[String(value)] || String(value ?? "");
}

async function apiFetch(path, options = {}) {
  const response = await fetch(path, {
    ...options,
    headers: {
      Accept: "application/json",
      ...(options.body ? { "Content-Type": "application/json" } : {}),
      ...(options.headers || {}),
    },
  });
  const contentType = response.headers.get("content-type") || "";
  const payload = contentType.includes("application/json") ? await response.json() : await response.text();
  if (!response.ok) {
    const message = typeof payload === "string" ? payload : payload?.error?.message || payload?.detail || "请求失败";
    throw new Error(typeof message === "string" ? message : JSON.stringify(message, null, 2));
  }
  return payload;
}

function switchScreen(name) {
  ["builderScreen", "loadingScreen", "resultScreen"].forEach((id) => {
    $(id).classList.toggle("active", id === `${name}Screen`);
  });
}

function populateSelect(selectEl, options) {
  const previous = selectEl.value;
  selectEl.innerHTML = "";
  options.forEach((item) => {
    const option = document.createElement("option");
    option.value = item.value;
    option.textContent = item.label;
    selectEl.appendChild(option);
  });
  if ([...selectEl.options].some((option) => option.value === previous)) {
    selectEl.value = previous;
  }
}

function syncCountValue() {
  $("countValue").textContent = $("count").value;
}

function getFieldValue(id) {
  return String($(id)?.value || "").trim();
}

function setSourceParseStatus(message, isVisible = true) {
  const statusEl = $("sourceQuestionParseStatus");
  if (!statusEl) {
    return;
  }
  statusEl.textContent = message || "";
  statusEl.hidden = !isVisible || !message;
}

function collectSourceQuestionPayload() {
  const stem = getFieldValue("sourceQuestionStem");
  const passage = getFieldValue("sourceQuestionPassage");
  if (!stem && !passage) {
    return null;
  }
  const options = {};
  ["A", "B", "C", "D"].forEach((letter) => {
    const value = getFieldValue(`sourceOption${letter}`);
    if (value) {
      options[letter] = value;
    }
  });
  return {
    passage: passage || null,
    stem: stem || "",
    options,
    answer: getFieldValue("sourceQuestionAnswer") || null,
    analysis: getFieldValue("sourceQuestionAnalysis") || null,
  };
}

function shouldAutoParseSourceQuestion(sourceQuestion) {
  if (!sourceQuestion) {
    return false;
  }
  const stem = String(sourceQuestion.stem || "").trim();
  const passage = String(sourceQuestion.passage || "").trim();
  const hasOptions = Object.values(sourceQuestion.options || {}).some((value) => String(value || "").trim());
  if (!passage || stem || hasOptions) {
    return false;
  }
  return /(?:^|\n)\s*[A-D][\.\u3001\uff0e]|正确答案|答案[:：]|解析[:：]|重新排列|语序正确|将以上|将以下|填入|横线/.test(passage);
}

function renderSecondaryOptions() {
  const primary = $("questionFocus").value;
  const secondaryOptions = primary
    ? [{ label: "Auto", value: "" }, ...(SECONDARY_BY_PRIMARY[primary] || [])]
    : [{ label: "Select", value: "" }];
  populateSelect($("specialType"), secondaryOptions);
  populateSelect($("materialStructure"), MATERIAL_STRUCTURE_OPTIONS);
}

function buildGeneratePayload() {
  const questionFocus = $("questionFocus").value || "";
  const textDirection = $("textDirection").value || null;
  const specialType = $("specialType").value;
  const sourceQuestion = collectSourceQuestionPayload();
  return {
    question_focus: questionFocus,
    difficulty_level: $("difficultyLevel").value,
    text_direction: textDirection,
    material_structure: $("materialStructure").value || null,
    special_question_types: specialType ? [specialType] : [],
    count: Number.parseInt($("count").value || "1", 10),
    topic: null,
    passage_style: null,
    use_fewshot: true,
    fewshot_mode: "structure_only",
    type_slots: {},
    extra_constraints: {},
    source_question: sourceQuestion,
    material_policy: {
      allow_reuse: false,
      cooldown_days: 30,
      preferred_document_genres: textDirection ? [textDirection] : [],
      excluded_material_ids: [],
      prefer_high_quality_reused: false,
    },
  };
}

function renderLoadingStep(activeIndex) {
  const container = $("loadingSteps");
  container.innerHTML = "";
  LOADING_STEPS.forEach((step, index) => {
    const node = document.createElement("div");
    const stateClass = index < activeIndex ? "done" : index === activeIndex ? "active" : "";
    node.className = `step-item ${stateClass}`.trim();
    node.innerHTML = `
      <div class="step-title">${escapeHtml(step.title)}</div>
      <div class="step-desc">${escapeHtml(step.desc)}</div>
    `;
    container.appendChild(node);
  });
}

function stopLoadingSequence() {
  if (state.loadingTimer) {
    window.clearInterval(state.loadingTimer);
    state.loadingTimer = null;
  }
}

function setLoadingContext(copy) {
  $("loadingNode").textContent = copy?.title || "处理中...";
  $("loadingNodeDesc").textContent = copy?.desc || "系统正在执行当前流程。";
}

function startLoadingSequence() {
  stopLoadingSequence();
  let cursor = 0;
  renderLoadingStep(cursor);
  state.loadingTimer = window.setInterval(() => {
    if (cursor >= LOADING_STEPS.length - 1) {
      stopLoadingSequence();
      return;
    }
    cursor += 1;
    renderLoadingStep(cursor);
  }, 900);
}

async function runWithLoading(task, fallbackScreen = "result", loadingCopy = null) {
  switchScreen("loading");
  setLoadingContext(loadingCopy);
  startLoadingSequence();
  try {
    const result = await task();
    renderLoadingStep(LOADING_STEPS.length - 1);
    stopLoadingSequence();
    return result;
  } catch (error) {
    stopLoadingSequence();
    switchScreen(fallbackScreen);
    throw error;
  }
}

async function generateQuestions(event) {
  event.preventDefault();
  let sourceQuestion = collectSourceQuestionPayload();
  if (!$("questionFocus").value && !sourceQuestion) {
    alert("Please choose a question type or provide a reference question first.");
    return;
  }
  try {
    if (shouldAutoParseSourceQuestion(sourceQuestion)) {
      setSourceParseStatus("正在自动拆题并落位到下方表单，请稍等...");
      await autoDetectSourceQuestion();
      sourceQuestion = collectSourceQuestionPayload();
    }
    const response = await runWithLoading(
      () =>
        apiFetch("/api/v1/questions/generate", {
          method: "POST",
          body: JSON.stringify(buildGeneratePayload()),
        }),
      "builder",
      ACTION_LOADING_COPY.generate,
    );
    state.batchId = response.batch_id;
    state.items = response.items || [];
    await renderResults();
    switchScreen("result");
  } catch (error) {
    alert(`生成失败：${error.message}`);
  }
}

async function autoDetectSourceQuestion() {
  const rawText = getFieldValue("sourceQuestionPassage");
  if (!rawText) {
    alert("请先把整道原题粘贴到“原题文段”里。");
    return;
  }
  const button = $("sourceQuestionDetectBtn");
  button.disabled = true;
  setSourceParseStatus("正在用 4o mini 拆题并回填，请稍等...");
  try {
    const result = await apiFetch("/api/v1/questions/source-question/parse", {
      method: "POST",
      body: JSON.stringify({ raw_text: rawText }),
    });
    const parsed = result.source_question || {};
    $("sourceQuestionPassage").value = parsed.passage || "";
    $("sourceQuestionStem").value = parsed.stem || "";
    $("sourceOptionA").value = parsed.options?.A || "";
    $("sourceOptionB").value = parsed.options?.B || "";
    $("sourceOptionC").value = parsed.options?.C || "";
    $("sourceOptionD").value = parsed.options?.D || "";
    $("sourceQuestionAnswer").value = parsed.answer || "";
    $("sourceQuestionAnalysis").value = parsed.analysis || "";
    setSourceParseStatus("已自动拆题并回填到下方表单，你可以直接继续修改。");
  } finally {
    button.disabled = false;
  }
}

async function executeResultMutation(runner, loadingCopy) {
  await runWithLoading(runner, "result", loadingCopy);
  await renderResults();
  switchScreen("result");
}

async function renderResults() {
  $("resultBatchInfo").textContent = state.batchId
    ? `当前批次：${state.batchId} · 已生成 ${state.items.length} 题`
    : "等待本次生成结果...";
  const container = $("resultList");
  container.innerHTML = "";
  for (let index = 0; index < state.items.length; index += 1) {
    const card = await buildQuestionCard(state.items[index], index + 1);
    container.appendChild(card);
  }
}

function renderOptions(options) {
  const entries = Object.entries(options || {});
  if (!entries.length) {
    return '<div class="muted">暂无选项</div>';
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

function renderReplacementSelect(selectEl, items) {
  selectEl.innerHTML = "";
  if (!items.length) {
    const option = document.createElement("option");
    option.value = "";
    option.textContent = "当前没有可替换文段";
    selectEl.appendChild(option);
    return;
  }
  items.forEach((item) => {
    const option = document.createElement("option");
    option.value = item.material_id;
    option.dataset.materialText = item.material_text || "";
    option.textContent = `${item.article_title || item.label}｜${item.source_name || "-"}｜已用 ${item.usage_count_before} 次`;
    selectEl.appendChild(option);
  });
}

function buildQuestionModifyControls(controlPanel) {
  const controlsByKey = new Map((controlPanel?.controls || []).map((control) => [control.control_key, control]));
  const difficultyControl = controlsByKey.get("difficulty_target");
  const confusionControl = controlsByKey.get("option_confusion") || controlsByKey.get("distractor_strength");
  const distractorControl = controlsByKey.get("distractor_modes");

  const controls = [
    {
      controlKey: "difficulty_raise_factor",
      label: "整体难度提高系数",
      currentValue: "0",
      options: [
        { value: "0", label: "保持当前" },
        { value: "1", label: "提高一档" },
        { value: "2", label: "直接拉高" },
      ],
      description: difficultyControl?.description || "用于整体拉高当前题目的难度档位。",
    },
  ];

  if (confusionControl) {
    controls.push({
      controlKey: confusionControl.control_key,
      label: "选项迷惑度",
      currentValue: confusionControl.current_value ?? confusionControl.default_value ?? "",
      options: confusionControl.options || [],
      description: "控制全部错误项整体向正确答案靠近的程度，不是只影响某一个组合结果。",
    });
    controls.push({
      controlKey: "wrong_option_confusion_profile",
      label: "错误项迷惑系数",
      currentValue: [
        confusionControl.current_value ?? confusionControl.default_value ?? "medium",
        confusionControl.current_value ?? confusionControl.default_value ?? "medium",
        confusionControl.current_value ?? confusionControl.default_value ?? "medium",
      ],
      options: confusionControl.options || [],
      description: "分别控制 3 个错误项各自接近正确答案的程度，系数越高越像正确项但仍应败给原文依据。",
      multiValueCount: 3,
    });
  }

  if (distractorControl) {
    const currentValue = Array.isArray(distractorControl.current_value)
      ? distractorControl.current_value
      : Array.isArray(distractorControl.default_value)
        ? distractorControl.default_value
        : [distractorControl.current_value || distractorControl.default_value || ""];
    controls.push({
      controlKey: distractorControl.control_key,
      label: "干扰项方式",
      currentValue,
      options: distractorControl.options || [],
      description: "分别指定 3 个错误项各自的主要偏离方式。",
      multiValueCount: 3,
    });
  }
  return controls;
}

function renderSimpleControls(container, controls, itemId) {
  container.innerHTML = "";
  controls.forEach((control) => {
    const wrapper = document.createElement("label");
    wrapper.className = "control-field";
    wrapper.innerHTML = `<span>${escapeHtml(control.label)}</span>`;

    if (control.multiValueCount) {
      const group = document.createElement("div");
      group.className = "multi-control-group";
      for (let index = 0; index < control.multiValueCount; index += 1) {
        const subWrap = document.createElement("div");
        subWrap.className = "multi-control-item";
        const subLabel = document.createElement("small");
        subLabel.textContent =
          control.controlKey === "wrong_option_confusion_profile"
            ? `错误项 ${index + 1} 迷惑系数`
            : `错误项 ${index + 1} 干扰方式`;
        const select = document.createElement("select");
        select.dataset.scope = "simple-item";
        select.dataset.itemId = itemId;
        select.dataset.controlKey = control.controlKey;
        select.dataset.controlIndex = String(index);
        control.options.forEach((option) => {
          const element = document.createElement("option");
          element.value = option.value;
          element.textContent = humanizeValueLabel(option.value, option.label);
          const selectedValue = control.currentValue[index] ?? control.currentValue[0] ?? "";
          if (String(option.value) === String(selectedValue)) {
            element.selected = true;
          }
          select.appendChild(element);
        });
        subWrap.appendChild(subLabel);
        subWrap.appendChild(select);
        group.appendChild(subWrap);
      }
      wrapper.appendChild(group);
    } else {
      const select = document.createElement("select");
      select.dataset.scope = "simple-item";
      select.dataset.itemId = itemId;
      select.dataset.controlKey = control.controlKey;
      control.options.forEach((option) => {
        const element = document.createElement("option");
        element.value = option.value;
        element.textContent = humanizeValueLabel(option.value, option.label);
        if (String(option.value) === String(control.currentValue)) {
          element.selected = true;
        }
        select.appendChild(element);
      });
      wrapper.appendChild(select);
    }

    const hint = document.createElement("small");
    hint.textContent = control.description;
    wrapper.appendChild(hint);
    container.appendChild(wrapper);
  });
}

function bumpDifficulty(currentDifficulty, raiseFactor) {
  const ladder = ["easy", "medium", "hard"];
  const start = Math.max(ladder.indexOf(currentDifficulty), 0);
  const step = Number.parseInt(raiseFactor || "0", 10);
  return ladder[Math.min(start + step, ladder.length - 1)];
}

function collectSimpleOverrides(card, itemId) {
  const overrides = {};
  const currentItem = state.items.find((item) => item.item_id === itemId) || {};
  const inputs = [...card.querySelectorAll('[data-scope="simple-item"]')];
  const grouped = new Map();
  inputs.forEach((input) => {
    const key = input.dataset.controlKey;
    if (!grouped.has(key)) {
      grouped.set(key, []);
    }
    grouped.get(key).push(input);
  });

  grouped.forEach((group, key) => {
    const first = group[0];
    if (key === "difficulty_raise_factor") {
      overrides.difficulty_target = bumpDifficulty(currentItem.difficulty_target || "medium", first.value);
      return;
    }
    if (key === "distractor_modes") {
      overrides[key] = group.map((input) => input.value).filter(Boolean).slice(0, 3);
      return;
    }
    if (key === "wrong_option_confusion_profile") {
      overrides.extra_constraints = {
        ...(overrides.extra_constraints || {}),
        wrong_option_confusion_profile: group.map((input) => input.value).filter(Boolean).slice(0, 3),
      };
      return;
    }
    overrides[key] = first.value;
  });

  return overrides;
}

function historyText(history) {
  const lines = [`当前版本：v${history.current_version_no}`, ""];
  (history.versions || []).forEach((version) => {
    lines.push(`v${version.version_no} · ${version.source_action} · ${version.current_status} · ${version.created_at || "-"}`);
    if (version.diff_summary && Object.keys(version.diff_summary).length) {
      lines.push(`变更摘要：${JSON.stringify(version.diff_summary, null, 2)}`);
    }
    lines.push("");
  });
  lines.push("审核动作：");
  (history.review_actions || []).forEach((action) => {
    lines.push(`- ${action.action_type || action.action} · ${action.created_at || "-"}`);
  });
  return lines.join("\n");
}

function diffText(diff) {
  return JSON.stringify(
    {
      changed_fields: diff.changed_fields,
      material_changed: diff.material_changed,
      difficulty_changed: diff.difficulty_changed,
      prompt_changed: diff.prompt_changed,
      stem_changed: diff.stem_changed,
      options_changed: diff.options_changed,
      analysis_changed: diff.analysis_changed,
      old_summary: diff.old_summary,
      new_summary: diff.new_summary,
    },
    null,
    2,
  );
}

async function renderHistory(itemId) {
  const history = await apiFetch(`/api/v1/review/items/${itemId}/history`);
  state.histories.set(itemId, history);
  $(`history-${itemId}`).textContent = historyText(history);
}

async function renderLatestDiff(itemId) {
  const history = state.histories.get(itemId) || (await apiFetch(`/api/v1/review/items/${itemId}/history`));
  const versions = history.versions || [];
  if (!versions.length) {
    $(`diff-${itemId}`).textContent = "暂无可用版本差异。";
    return;
  }
  const toVersion = versions[0].version_no;
  const fromVersion = versions[1]?.version_no || versions[0].version_no;
  const diff = await apiFetch(`/api/v1/review/items/${itemId}/diff?from_version=${fromVersion}&to_version=${toVersion}`);
  $(`diff-${itemId}`).textContent = diffText(diff);
}

async function refreshItem(itemId) {
  const item = await apiFetch(`/api/v1/questions/${itemId}`);
  state.items = state.items.map((entry) => (entry.item_id === itemId ? item : entry));
}

function renderReferenceSignals(item) {
  const analysis = item.request_snapshot?.source_question_analysis;
  const sourceQuestion = item.request_snapshot?.source_question;
  if (!analysis && !sourceQuestion) {
    return "";
  }
  const cards = (analysis?.business_card_ids || [])
    .map((value) => `<span class="chip">${escapeHtml(value)}</span>`)
    .join("");
  return `
    <div class="material-box" style="margin-top: 16px;">
      <div class="section-title">参考题驱动信息</div>
      <div class="material-grid">
        <div class="mini-card">
          <strong>原题题干</strong>
          <div>${escapeHtml(sourceQuestion?.stem || "未提供")}</div>
        </div>
        <div class="mini-card">
          <strong>命中业务卡</strong>
          <div style="display:flex;flex-wrap:wrap;gap:8px;">${cards || '<span class="muted">未命中</span>'}</div>
        </div>
        <div class="mini-card">
          <strong>长度目标</strong>
          <div>${escapeHtml(analysis?.target_length ?? "-")} ± ${escapeHtml(analysis?.length_tolerance ?? "-")}</div>
        </div>
        <div class="mini-card">
          <strong>锚点裁缩</strong>
          <div>${escapeHtml(item.material_selection?.anchor_adaptation_reason || "-")}</div>
        </div>
      </div>
      <div class="mini-card" style="margin-top: 12px;">
        <strong>原题解析</strong>
        <div>${escapeHtml(sourceQuestion?.analysis || "未提供")}</div>
      </div>
    </div>
  `;
}

function renderEditableQuestionFields(item, generated) {
  const currentMaterial = escapeHtml(item.material_text || "");
  const originalMaterial = escapeHtml(item.material_selection?.original_text || item.material_text || "");
  return `
    <div class="material-box" style="margin-top: 16px;">
      <div class="section-title">手工编辑区</div>
      <div style="display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:14px;">
        <label class="reference-field reference-field-full">
          <span>加工后文段</span>
          <textarea data-manual-field="material_text" rows="6">${currentMaterial}</textarea>
        </label>
        <label class="reference-field reference-field-full">
          <span>原本文段</span>
          <textarea rows="6" readonly>${originalMaterial}</textarea>
        </label>
        <label class="reference-field reference-field-full">
          <span>题干</span>
          <textarea data-manual-field="stem" rows="2">${escapeHtml(generated.stem || item.stem_text || "")}</textarea>
        </label>
        <label class="reference-field"><span>选项 A</span><textarea data-manual-field="option_A" rows="2">${escapeHtml(generated.options?.A || "")}</textarea></label>
        <label class="reference-field"><span>选项 B</span><textarea data-manual-field="option_B" rows="2">${escapeHtml(generated.options?.B || "")}</textarea></label>
        <label class="reference-field"><span>选项 C</span><textarea data-manual-field="option_C" rows="2">${escapeHtml(generated.options?.C || "")}</textarea></label>
        <label class="reference-field"><span>选项 D</span><textarea data-manual-field="option_D" rows="2">${escapeHtml(generated.options?.D || "")}</textarea></label>
        <label class="reference-field">
          <span>答案</span>
          <select data-manual-field="answer">
            <option value="A" ${generated.answer === "A" ? "selected" : ""}>A</option>
            <option value="B" ${generated.answer === "B" ? "selected" : ""}>B</option>
            <option value="C" ${generated.answer === "C" ? "selected" : ""}>C</option>
            <option value="D" ${generated.answer === "D" ? "selected" : ""}>D</option>
          </select>
        </label>
        <label class="reference-field reference-field-wide">
          <span>解析</span>
          <textarea data-manual-field="analysis" rows="4">${escapeHtml(generated.analysis || "")}</textarea>
        </label>
      </div>
      <div class="modify-actions">
        <button type="button" class="secondary-btn" data-action="manual-save" data-item-id="${item.item_id}">保存编辑版本</button>
      </div>
    </div>
  `;
}

function collectManualEditPayload(card) {
  const read = (selector) => String(card.querySelector(selector)?.value || "").trim();
  return {
    material_text: read('[data-manual-field="material_text"]'),
    stem: read('[data-manual-field="stem"]'),
    options: {
      A: read('[data-manual-field="option_A"]'),
      B: read('[data-manual-field="option_B"]'),
      C: read('[data-manual-field="option_C"]'),
      D: read('[data-manual-field="option_D"]'),
    },
    answer: read('[data-manual-field="answer"]').toUpperCase(),
    analysis: read('[data-manual-field="analysis"]'),
  };
}

async function buildQuestionCard(itemSummary, displayIndex) {
  const item = await apiFetch(`/api/v1/questions/${itemSummary.item_id}`);
  const history = await apiFetch(`/api/v1/review/items/${item.item_id}/history`);
  const replacementList = await apiFetch(`/api/v1/questions/${item.item_id}/replacement-materials?limit=8`);
  const controlPanel = await apiFetch(`/api/v1/questions/${item.item_id}/controls`);
  state.histories.set(item.item_id, history);

  const generated = item.generated_question || {};
  const isBlocked = item.current_status === "auto_failed" || item.validation_result?.passed === false;
  const statusClass = isBlocked ? "status-danger" : item.current_status === "approved" ? "status-approved" : "status-pending";
  const blockedReason = isBlocked
    ? (item.evaluation_result?.judge_reason || (item.validation_result?.errors || []).slice(0, 3).join("；") || "该题已被系统拦截。")
    : "";
  const card = document.createElement("div");
  card.className = "question-card";
  card.dataset.itemId = item.item_id;
  card.innerHTML = `
    <div class="question-main">
      <div class="question-head">
        <div><h3>题目 ${displayIndex}</h3></div>
        <div class="question-meta">
          <span class="chip status ${statusClass}">${escapeHtml(item.current_status || "-")}</span>
          <span class="chip">${escapeHtml(item.question_type || "-")}</span>
          <span class="chip">${escapeHtml(item.pattern_id || item.selected_pattern || "-")}</span>
          <span class="chip">${escapeHtml(humanizeValueLabel(item.difficulty_target || "-"))}</span>
        </div>
      </div>

      ${isBlocked ? `
        <div class="blocked-banner">
          <div class="blocked-title">该题已被系统拦截，当前不可直接通过</div>
          <div class="blocked-desc">${escapeHtml(blockedReason)}</div>
        </div>
      ` : ""}

      <div class="material-box reading-box" style="margin-top: 16px;">
        <div class="section-title">加工后材料</div>
        <div class="reading-lead">先看加工后材料，确认文段是否可读、能否支撑出题。</div>
        <div class="material-text reading-material">${escapeHtml(item.material_text || "暂无文段文本")}</div>
      </div>

      <div class="question-box reading-box" style="margin-top: 16px;">
        <div class="section-title">题干与选项</div>
        <div class="question-stem">${escapeHtml(item.stem_text || generated.stem || "暂无题干")}</div>
        <div class="option-list">${renderOptions(generated.options || {})}</div>
      </div>

      <div class="system-box reading-box" style="margin-top: 16px;">
        <div class="section-title">答案与解析</div>
        <div class="answer-card">
          <div class="answer-pill">答案：${escapeHtml(generated.answer || "-")}</div>
          <div class="analysis-row">${escapeHtml(generated.analysis || "-")}</div>
        </div>
      </div>

      <details class="result-collapse" style="margin-top: 16px;">
        <summary>查看原文与材料来源</summary>
        <div class="material-box collapse-body">
          <div class="section-title">文段信息</div>
          <div class="material-grid">
            <div class="mini-card">
              <strong>文本来源</strong>
              <div>${escapeHtml(item.material_source?.source_name || item.material_source?.source_id || "-")}</div>
            </div>
            <div class="mini-card">
              <strong>文章来源</strong>
              <div>${escapeHtml(item.material_source?.article_title || "-")}</div>
            </div>
            <div class="mini-card">
              <strong>文本分类</strong>
              <div>${escapeHtml(item.material_selection?.document_genre || "-")}</div>
            </div>
            <div class="mini-card">
              <strong>已使用次数</strong>
              <div>${escapeHtml(item.material_usage_count_before ?? 0)}</div>
            </div>
          </div>
          <div class="mini-card">
            <strong>原本文段</strong>
            <div class="material-text">${escapeHtml(item.material_selection?.original_text || item.material_text || "暂无原文")}</div>
          </div>
        </div>
      </details>

      <details class="result-collapse" style="margin-top: 16px;">
        <summary>查看系统判定与参考题信息</summary>
        <div class="collapse-body">
          <div class="system-box">
            <div class="section-title">系统判定</div>
            <div class="system-grid">
              <div class="mini-card">
                <strong>自动校验</strong>
                <div>passed = ${escapeHtml(item.validation_result?.passed ?? "-")}</div>
                <div>score = ${escapeHtml(item.validation_result?.score ?? "-")}</div>
              </div>
              <div class="mini-card">
                <strong>LLM 审核</strong>
                <div>score = ${escapeHtml(item.evaluation_result?.overall_score ?? "-")}</div>
                <div>${escapeHtml(item.evaluation_result?.judge_reason || "暂无审核说明")}</div>
              </div>
            </div>
          </div>
          ${renderReferenceSignals(item)}
        </div>
      </details>

      <div class="material-box visually-hidden" style="margin-top: 16px;">
        <div class="section-title">文段信息</div>
        <div class="material-grid">
          <div class="mini-card">
            <strong>文本来源</strong>
            <div>${escapeHtml(item.material_source?.source_name || item.material_source?.source_id || "-")}</div>
          </div>
          <div class="mini-card">
            <strong>文章来源</strong>
            <div>${escapeHtml(item.material_source?.article_title || "-")}</div>
          </div>
          <div class="mini-card">
            <strong>文本分类</strong>
            <div>${escapeHtml(item.material_selection?.document_genre || "-")}</div>
          </div>
          <div class="mini-card">
            <strong>已使用次数</strong>
            <div>${escapeHtml(item.material_usage_count_before ?? 0)}</div>
          </div>
        </div>
        <div class="mini-card">
          <strong>原本文段</strong>
          <div class="material-text">${escapeHtml(item.material_selection?.original_text || item.material_text || "暂无原文")}</div>
        </div>
      </div>
      ${renderEditableQuestionFields(item, generated)}

      <div class="modify-panel" id="modify-${item.item_id}">
        <div class="modify-stack">
          <div class="modify-panel-section">
            <div class="section-title">微调</div>
            <textarea id="fineTune-${item.item_id}" placeholder="输入微调要求，例如：把题干再凝练一点，解析更像公考风格。"></textarea>
            <div class="modify-actions">
              <button type="button" class="secondary-btn" data-action="fine-tune" data-item-id="${item.item_id}">提交微调</button>
            </div>
          </div>

          <div class="modify-panel-section">
            <div class="section-title">更换文段</div>
            <select id="replacementMaterial-${item.item_id}"></select>
            <textarea id="customReplacementMaterial-${item.item_id}" rows="5" placeholder="鍙€夛細鐩存帴绮樿创澶囩敤鏉愭枡锛岀郴缁熶細浼樺厛浣跨敤杩欓噷鐨勫唴瀹归噸鍋氥€?"></textarea>
            <div class="modify-actions">
              <button type="button" class="secondary-btn" data-action="text-modify" data-item-id="${item.item_id}">使用新文段重做</button>
            </div>
          </div>

          <div class="modify-panel-section">
            <div class="section-title">题目重修</div>
            <div class="simple-controls" id="simpleControls-${item.item_id}"></div>
            <div class="modify-actions">
              <button type="button" class="secondary-btn" data-action="question-modify" data-item-id="${item.item_id}">按参数重做</button>
            </div>
          </div>
        </div>

        <div class="history-wrap">
          <div class="history-box" id="history-${item.item_id}">点击“查看 History”载入版本链。</div>
          <div class="diff-box" id="diff-${item.item_id}">点击“查看最新 Diff”载入版本变化摘要。</div>
        </div>
        <div class="history-actions">
          <button type="button" class="ghost-btn" data-action="load-history" data-item-id="${item.item_id}">查看 History</button>
          <button type="button" class="ghost-btn" data-action="load-diff" data-item-id="${item.item_id}">查看最新 Diff</button>
        </div>
      </div>
    </div>

    <div class="question-actions">
      <button type="button" class="success-btn ${isBlocked ? "is-disabled" : ""}" data-action="confirm" data-item-id="${item.item_id}" ${isBlocked ? "disabled" : ""}>通过</button>
      <button type="button" class="secondary-btn" data-action="toggle-modify" data-item-id="${item.item_id}">修改</button>
      <button type="button" class="danger-btn" data-action="discard" data-item-id="${item.item_id}">作废</button>
    </div>
  `;

  renderReplacementSelect(card.querySelector(`#replacementMaterial-${CSS.escape(item.item_id)}`), replacementList.items || []);
  renderSimpleControls(card.querySelector(`#simpleControls-${CSS.escape(item.item_id)}`), buildQuestionModifyControls(controlPanel), item.item_id);
  return card;
}

async function handleResultAction(action, itemId, button) {
  const card = button.closest(".question-card");
  if (!card) {
    return;
  }
  if (action === "toggle-modify") {
    const panel = card.querySelector(`#modify-${CSS.escape(itemId)}`);
    if (panel) {
      panel.classList.toggle("active");
    }
    return;
  }
  if (action === "confirm") {
    await executeResultMutation(
      async () => {
        await apiFetch(`/api/v1/questions/${itemId}/confirm`, {
          method: "POST",
          body: JSON.stringify({}),
        });
        await refreshItem(itemId);
      },
      ACTION_LOADING_COPY.confirm,
    );
    return;
  }
  if (action === "discard") {
    await executeResultMutation(
      async () => {
        await apiFetch(`/api/v1/questions/${itemId}/review-actions`, {
          method: "POST",
          body: JSON.stringify({ action: "discard" }),
        });
        await refreshItem(itemId);
      },
      ACTION_LOADING_COPY.discard,
    );
    return;
  }
  if (action === "fine-tune") {
    const textarea = card.querySelector(`#fineTune-${CSS.escape(itemId)}`);
    const instruction = textarea?.value.trim() || "";
    if (!instruction) {
      alert("请先输入微调说明。");
      return;
    }
    await executeResultMutation(
      async () => {
        await apiFetch(`/api/v1/questions/${itemId}/fine-tune`, {
          method: "POST",
          body: JSON.stringify({ instruction }),
        });
        await refreshItem(itemId);
      },
      ACTION_LOADING_COPY.fineTune,
    );
    return;
  }
  if (action === "text-modify") {
    const select = card.querySelector(`#replacementMaterial-${CSS.escape(itemId)}`);
    const customMaterial = String(card.querySelector(`#customReplacementMaterial-${CSS.escape(itemId)}`)?.value || "").trim();
    const materialId = select?.value || "";
    const selectedMaterialText = String(select?.selectedOptions?.[0]?.dataset?.materialText || "").trim();
    if (!materialId && !customMaterial && !selectedMaterialText) {
      alert("当前没有可替换文段。");
      return;
    }
    await executeResultMutation(
      async () => {
        await apiFetch(`/api/v1/questions/${itemId}/review-actions`, {
          method: "POST",
          body: JSON.stringify({
            action: "text_modify",
            control_overrides: customMaterial
              ? { material_text: customMaterial }
              : selectedMaterialText
                ? { material_id: materialId, material_text: selectedMaterialText }
                : { material_id: materialId },
          }),
        });
        await refreshItem(itemId);
      },
      ACTION_LOADING_COPY.textModify,
    );
    return;
  }
  if (action === "question-modify") {
    const controlOverrides = collectSimpleOverrides(card, itemId);
    await executeResultMutation(
      async () => {
        await apiFetch(`/api/v1/questions/${itemId}/review-actions`, {
          method: "POST",
          body: JSON.stringify({ action: "question_modify", control_overrides: controlOverrides }),
        });
        await refreshItem(itemId);
      },
      ACTION_LOADING_COPY.questionModify,
    );
    return;
  }
  if (action === "manual-save") {
    const manualPatch = collectManualEditPayload(card);
    await executeResultMutation(
      async () => {
        await apiFetch(`/api/v1/questions/${itemId}/review-actions`, {
          method: "POST",
          body: JSON.stringify({
            action: "manual_edit",
            instruction: "manual edit from review UI",
            control_overrides: { manual_patch: manualPatch },
          }),
        });
        await refreshItem(itemId);
      },
      ACTION_LOADING_COPY.manualSave,
    );
    return;
  }
  if (action === "load-history") {
    await renderHistory(itemId);
    return;
  }
  if (action === "load-diff") {
    await renderLatestDiff(itemId);
  }
}

async function exportApproved() {
  if (!state.batchId) {
    throw new Error("当前没有可导出的批次。");
  }
  const response = await fetch(`/api/v1/review/batches/${state.batchId}/delivery/export?format=markdown`);
  if (!response.ok) {
    throw new Error((await response.text()) || "导出失败");
  }
  const content = await response.text();
  const blob = new Blob([content], { type: "text/markdown;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = `batch-${state.batchId}-approved.md`;
  anchor.click();
  URL.revokeObjectURL(url);
}

function initPage() {
  populateSelect($("questionFocus"), [{ label: "Select", value: "" }, ...PRIMARY_OPTIONS]);
  populateSelect($("textDirection"), TEXT_DIRECTION_OPTIONS);
  renderSecondaryOptions();
  syncCountValue();
  renderLoadingStep(0);
  setSourceParseStatus("", false);

  $("questionFocus").addEventListener("change", renderSecondaryOptions);
  $("count").addEventListener("input", syncCountValue);
  $("generateForm").addEventListener("submit", generateQuestions);
  $("sourceQuestionDetectBtn").addEventListener("click", () => {
    autoDetectSourceQuestion().catch((error) => alert(`自动拆题失败：${error.message}`));
  });
  $("cancelLoadingBtn").addEventListener("click", () => {
    stopLoadingSequence();
    switchScreen("builder");
  });
  $("backToBuilderBtn").addEventListener("click", () => switchScreen("builder"));
  $("exportApprovedBtn").addEventListener("click", () => {
    exportApproved().catch((error) => alert(error.message));
  });
  $("resultList").addEventListener("click", (event) => {
    const target = event.target.closest("button[data-action]");
    if (!target) {
      return;
    }
    handleResultAction(target.dataset.action, target.dataset.itemId, target).catch((error) => {
      alert(error.message);
    });
  });
}

initPage();
