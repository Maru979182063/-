const PRIMARY_OPTIONS = [
  { label: "语句排序题", value: "语句排序题" },
  { label: "语句填空题", value: "语句填空题" },
  { label: "中心理解题", value: "中心理解题" },
];

const SECONDARY_BY_PRIMARY = {
  "语句排序题": [
    { label: "双锚点锁定", value: "双锚点锁定" },
    { label: "承接并列展开", value: "承接并列展开" },
    { label: "观点-原因-行动排序", value: "观点-原因-行动排序" },
    { label: "问题-对策-案例排序", value: "问题-对策-案例排序" },
  ],
  "语句填空题": [
    { label: "开头总起", value: "开头总起" },
    { label: "衔接过渡", value: "衔接过渡" },
    { label: "中段焦点切换", value: "中段焦点切换" },
    { label: "中段解释说明", value: "中段解释说明" },
    { label: "结尾总结", value: "结尾总结" },
    { label: "结尾升华", value: "结尾升华" },
    { label: "定位插入匹配", value: "定位插入匹配" },
    { label: "综合多点匹配", value: "综合多点匹配" },
  ],
  "中心理解题": [{ label: "中心理解题", value: "中心理解题" }],
};

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

const MATERIAL_STRUCTURE_BY_PRIMARY = {
  "语句排序题": ["总分归纳", "分总归纳", "并列推进", "问题-对策", "现象-分析", "案例-结论", "观点-论证", "综合说明"],
  "语句填空题": ["总分归纳", "分总归纳", "转折归旨", "并列推进", "问题-对策", "现象-分析", "背景-核心结论", "综合说明"],
  "中心理解题": ["总分归纳", "分总归纳", "转折归旨", "并列推进", "观点-论证", "背景-核心结论", "综合说明"],
  manualSave: {
    title: "姝ｅ湪淇濆瓨鎵嬪伐缂栬緫",
    desc: "绯荤粺浼氫繚瀛樹綘鍦ㄩ〉闈笂淇敼鐨勬潗鏂欍€侀骞层€侀€夐」鍜岃В鏋愶紝骞剁敓鎴愭柊鐗堟湰绛夊緟纭銆?,
  },
};

const MATERIAL_STRUCTURE_BY_SECONDARY = {
  "选择标题": ["总分归纳", "分总归纳", "转折归旨", "并列推进", "观点-论证", "背景-核心结论", "综合说明"],
  "主旨概括": ["总分归纳", "分总归纳", "转折归旨", "并列推进", "观点-论证", "背景-核心结论", "综合说明"],
  "结构概括": ["总分归纳", "分总归纳", "并列推进", "问题-对策", "现象-分析", "案例-结论", "观点-论证"],
  "局部段意概括": ["分总归纳", "转折归旨", "现象-分析", "案例-结论", "观点-论证", "综合说明"],
  "中心理解题": ["总分归纳", "分总归纳", "转折归旨", "并列推进", "观点-论证", "背景-核心结论", "综合说明"],
  "尾句直接承接": ["转折归旨", "并列推进", "现象-分析", "观点-论证", "背景-核心结论", "综合说明"],
  "问题后接对策": ["问题-对策", "现象-分析", "案例-结论", "背景-核心结论"],
  "机制展开": ["现象-分析", "观点-论证", "背景-核心结论", "综合说明"],
  "主题转分话题": ["总分归纳", "分总归纳", "并列推进", "背景-核心结论", "综合说明"],
  "总结后开启新支点": ["分总归纳", "转折归旨", "并列推进", "观点-论证", "综合说明"],
  "观点后接原因": ["观点-论证", "现象-分析", "背景-核心结论", "综合说明"],
  "个案到宏观展开": ["案例-结论", "现象-分析", "观点-论证", "背景-核心结论"],
  "多分支聚焦": ["并列推进", "总分归纳", "现象-分析", "综合说明"],
  "张力解释": ["转折归旨", "现象-分析", "观点-论证", "综合说明"],
  "方法延展": ["问题-对策", "观点-论证", "现象-分析", "综合说明"],
  "双锚点锁定": ["并列推进", "总分归纳", "分总归纳", "综合说明"],
  "承接并列展开": ["并列推进", "总分归纳", "综合说明"],
  "观点-原因-行动排序": ["观点-论证", "问题-对策", "现象-分析"],
  "问题-对策-案例排序": ["问题-对策", "案例-结论", "现象-分析"],
  "开头总起": ["总分归纳", "背景-核心结论", "综合说明"],
  "衔接过渡": ["转折归旨", "并列推进", "现象-分析", "综合说明"],
  "中段焦点切换": ["转折归旨", "并列推进", "现象-分析", "观点-论证"],
  "中段解释说明": ["现象-分析", "观点-论证", "背景-核心结论", "综合说明"],
  "结尾总结": ["分总归纳", "总分归纳", "观点-论证", "综合说明"],
  "结尾升华": ["分总归纳", "转折归旨", "观点-论证", "背景-核心结论"],
  "定位插入匹配": ["并列推进", "现象-分析", "观点-论证", "综合说明"],
  "综合多点匹配": ["总分归纳", "并列推进", "现象-分析", "综合说明"],
};

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
  generate: {
    title: "正在生成题目",
    desc: "系统正在完成材料获取、题目生成、自动校验与 LLM 审核。",
  },
  fineTune: {
    title: "正在执行微调",
    desc: "系统会基于当前题目做小幅修改，并重新经过自动校验与 LLM 审核。",
  },
  textModify: {
    title: "正在更换文段",
    desc: "系统会切换到新的本地文段，再重新生成并审核当前题目。",
  },
  questionModify: {
    title: "正在按参数重做",
    desc: "系统会根据新的难度、迷惑度和错误项干扰方式重新制作题目。",
  },
};

const VALUE_LABELS = {
  easy: "简单",
  medium: "中等",
  hard: "困难",
  main_idea: "中心理解题",
  sentence_order: "语句排序题",
  sentence_fill: "语句填空题",
  single_claim_capture: "单点主旨提取",
  conclusion_sentence_refinement: "结论句提炼",
  whole_passage_integration: "全文整合",
  hidden_thesis_abstraction: "隐含主旨抽象",
  tail_anchor_direct_extend: "尾句直接承接",
  problem_solution_hook: "问题后接对策",
  mechanism_unfolding: "机制展开",
  raised_theme_to_subtopic: "主题转分话题",
  summary_with_new_pivot: "总结后开启新支点",
  judgement_to_reason: "观点后接原因",
  case_to_macro_unfold: "个案到宏观展开",
  multi_branch_focus: "多分支聚焦",
  tension_explained: "张力解释",
  method_expansion: "方法延展",
  dual_anchor_lock: "双锚点锁定",
  carry_parallel_expand: "承接并列展开",
  viewpoint_reason_action: "观点-原因-行动排序",
  problem_solution_case_blocks: "问题-对策-案例排序",
  opening_summary: "开头总起",
  bridge_transition: "衔接过渡",
  middle_focus_shift: "中段焦点切换",
  middle_explanation: "中段解释说明",
  ending_summary: "结尾总结",
  ending_elevation: "结尾升华",
  inserted_reference_match: "定位插入匹配",
  comprehensive_multi_match: "综合多点匹配",
  low: "低",
  low_medium: "较低",
  medium_low: "较低",
  medium_high: "较高",
  high: "高",
  old_topic_return: "回到旧话题",
  side_branch_shift: "支线偏移",
  abstract_overraise: "抽象拔高",
  empty_summary: "空泛总结",
  background_replay: "背景复述",
  parallel_mechanism: "平行机制",
  judgement_repeat: "判断重复",
  case_detail_stay: "停留个案",
  macro_topic_drift: "宏观漂移",
  value_slogan: "价值口号",
  function_misread: "功能误读",
  topic_shift: "主题偏移",
  overgeneralization: "过度泛化",
  undergeneralization: "概括不足",
  middle_conclusion: "截取中间结论",
  example_fragment: "截取例子片段",
  scope_shift: "范围偏移",
  fabrication: "无中生有",
  title_too_wide: "标题过宽",
  title_too_narrow: "标题过窄",
  catchy_but_offcore: "吸睛但偏题",
  explicit_single_center: "显性单中心",
  turning: "转折结构",
  progressive: "递进结构",
  contrast: "对照结构",
  multi_paragraph_hidden: "多段隐含主旨",
  local_paragraph: "局部段落",
  whole_passage: "全文整体",
  conclusion_sentence: "结论句",
  tail_sentence: "尾句",
  close_rephrase: "贴近改写",
  integrated: "整合概括",
  abstract_generalization: "抽象概括",
  central_meaning: "中心含义",
  article_task: "文章任务",
  title_label: "标题标签",
  structure_summary: "结构概括",
  local_paragraph_meaning: "局部段意",
};

const STATUS_LABELS = {
  pending_review: "待确认",
  waiting_review: "待确认",
  approved: "已通过",
  discarded: "已作废",
  rejected: "已作废",
  auto_failed: "自动失败",
  revising: "重修中",
  draft: "草稿",
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
  if (value === null || value === undefined || value === "") {
    return fallbackLabel || "-";
  }
  const mapped = VALUE_LABELS[String(value)];
  if (mapped) {
    return mapped;
  }
  if (fallbackLabel) {
    const fallbackMapped = VALUE_LABELS[String(fallbackLabel)];
    if (fallbackMapped) {
      return fallbackMapped;
    }
  }
  return fallbackLabel || String(value);
}

function humanizeStatus(status) {
  return STATUS_LABELS[String(status)] || String(status || "-");
}

function statusClass(status) {
  const normalized = String(status || "").toLowerCase();
  if (normalized === "approved") {
    return "status-approved";
  }
  if (normalized === "discarded" || normalized === "rejected" || normalized === "auto_failed") {
    return "status-danger";
  }
  if (normalized === "revising") {
    return "status-warn";
  }
  return "status-pending";
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
    const message =
      typeof payload === "string"
        ? payload
        : payload?.error?.message || payload?.detail || JSON.stringify(payload, null, 2);
    throw new Error(String(message));
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

function collectSourceQuestionPayload() {
  const stem = getFieldValue("sourceQuestionStem");
  if (!stem) {
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
    passage: getFieldValue("sourceQuestionPassage") || null,
    stem,
    options,
    answer: getFieldValue("sourceQuestionAnswer") || null,
    analysis: getFieldValue("sourceQuestionAnalysis") || null,
  };
}

function renderSecondaryOptions() {
  const primary = $("questionFocus").value;
  populateSelect($("specialType"), SECONDARY_BY_PRIMARY[primary] || []);
  if (!$("specialType").value && $("specialType").options.length) {
    $("specialType").selectedIndex = 0;
  }
  renderMaterialStructureOptions();
}

function renderMaterialStructureOptions() {
  const primary = $("questionFocus").value;
  const secondary = $("specialType").value;
  const allowedValues = MATERIAL_STRUCTURE_BY_SECONDARY[secondary] || MATERIAL_STRUCTURE_BY_PRIMARY[primary] || [];
  const options = MATERIAL_STRUCTURE_OPTIONS.filter((option) => option.value === "" || allowedValues.includes(option.value));
  populateSelect($("materialStructure"), options);
  if (!$("materialStructure").value && $("materialStructure").options.length) {
    $("materialStructure").selectedIndex = 0;
  }
}

function buildGeneratePayload() {
  const specialType = $("specialType").value;
  const sourceQuestion = collectSourceQuestionPayload();
  return {
    question_focus: $("questionFocus").value,
    difficulty_level: $("difficultyLevel").value,
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

function upsertItem(item) {
  const index = state.items.findIndex((entry) => entry.item_id === item.item_id);
  if (index >= 0) {
    state.items[index] = item;
  } else {
    state.items.unshift(item);
  }
}

async function generateQuestions(event) {
  event.preventDefault();
  try {
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

async function executeMutationWithLoading(runner, loadingCopy) {
  const response = await runWithLoading(runner, "result", loadingCopy);
  if (response?.item) {
    upsertItem(response.item);
  }
  await renderResults();
  switchScreen("result");
  return response;
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
  const entries = Object.entries(options || {}).sort(([a], [b]) => a.localeCompare(b));
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
    option.textContent = `${item.article_title || item.label} · ${item.source_name || "-"} · 已用 ${item.usage_count_before} 次`;
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
      submitKey: "difficulty_raise_factor",
      label: "整体难度提高系数",
      currentValue: "0",
      options: [
        { value: "0", label: "保持当前" },
        { value: "1", label: "提高一档" },
        { value: "2", label: "直接拉高" },
      ],
      description: "用于整体抬高当前题目的难度档位。",
    },
  ];

  if (confusionControl) {
    controls.push({
      controlKey: confusionControl.control_key,
      submitKey: confusionControl.control_key,
      label: "选项迷惑度",
      currentValue: confusionControl.current_value ?? confusionControl.default_value ?? "",
      options: confusionControl.options || [],
      description: "针对全部错误项整体靠近正确答案的程度，不是只影响某一项。",
    });
    controls.push({
      controlKey: "wrong_option_confusion_profile",
      submitKey: "wrong_option_confusion_profile",
      label: "错误项迷惑系数",
      currentValue: [
        confusionControl.current_value ?? confusionControl.default_value ?? "medium",
        confusionControl.current_value ?? confusionControl.default_value ?? "medium",
        confusionControl.current_value ?? confusionControl.default_value ?? "medium",
      ],
      options: confusionControl.options || [],
      description: "分别控制 3 个错误项各自接近正确答案的程度。",
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
      submitKey: distractorControl.control_key,
      label: "错误项干扰方式",
      currentValue,
      options: distractorControl.options || [],
      description: "分别指定 3 个错误项的主要偏移方向。",
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
        select.dataset.submitKey = control.submitKey;
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
      select.dataset.submitKey = control.submitKey;
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

function collectSimpleOverrides(card, item) {
  const overrides = {};
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
    const submitKey = first.dataset.submitKey || key;
    if (key === "difficulty_raise_factor") {
      overrides.difficulty_target = bumpDifficulty(item.difficulty_target || "medium", first.value);
      return;
    }
    if (key === "distractor_modes") {
      overrides[submitKey] = group.map((input) => input.value).filter(Boolean).slice(0, 3);
      return;
    }
    if (key === "wrong_option_confusion_profile") {
      overrides.extra_constraints = {
        ...(overrides.extra_constraints || {}),
        wrong_option_confusion_profile: group.map((input) => input.value).filter(Boolean).slice(0, 3),
      };
      return;
    }
    overrides[submitKey] = first.value;
  });

  return overrides;
}

function collectManualEditPayload(card) {
  const getValue = (selector) => String(card.querySelector(selector)?.value || "").trim();
  return {
    material_text: getValue('[data-manual-field="material_text"]'),
    stem: getValue('[data-manual-field="stem"]'),
    options: {
      A: getValue('[data-manual-field="option_A"]'),
      B: getValue('[data-manual-field="option_B"]'),
      C: getValue('[data-manual-field="option_C"]'),
      D: getValue('[data-manual-field="option_D"]'),
    },
    answer: getValue('[data-manual-field="answer"]').toUpperCase(),
    analysis: getValue('[data-manual-field="analysis"]'),
  };
}

function historyText(history) {
  const lines = [`当前版本：v${history.current_version_no}`, ""];
  (history.versions || []).forEach((version) => {
    lines.push(
      `v${version.version_no} · ${version.source_action || "-"} · ${humanizeStatus(version.current_status)} · ${
        version.created_at || "-"
      }`,
    );
    if (version.diff_summary && Object.keys(version.diff_summary).length) {
      lines.push(`变化摘要：${JSON.stringify(version.diff_summary, null, 2)}`);
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

async function fetchCardSupportData(itemId) {
  const [historyResult, replacementResult, controlResult] = await Promise.allSettled([
    apiFetch(`/api/v1/review/items/${itemId}/history`),
    apiFetch(`/api/v1/questions/${itemId}/replacement-materials?limit=8`),
    apiFetch(`/api/v1/questions/${itemId}/controls`),
  ]);
  return {
    history: historyResult.status === "fulfilled" ? historyResult.value : null,
    replacementList: replacementResult.status === "fulfilled" ? replacementResult.value : { items: [] },
    controlPanel: controlResult.status === "fulfilled" ? controlResult.value : { controls: [] },
  };
}

function resolveMaterialSource(item) {
  return item.material_source || item.material_selection?.source || {};
}

function cleanMaterialText(text) {
  const normalized = String(text || "").replace(/\r\n/g, "\n").replace(/\r/g, "\n").trim();
  if (!normalized) {
    return "";
  }
  const parts = normalized
    .split(/\n\s*\n/)
    .map((part) => part.trim())
    .filter(Boolean);
  const deduped = [];
  const signatures = [];
  for (const part of parts) {
    const signature = part.replace(/\s+/g, "");
    if (!signature) {
      continue;
    }
    if (signatures.includes(signature)) {
      continue;
    }
    signatures.push(signature);
    deduped.push(part);
  }
  return deduped.join("\n\n") || normalized;
}

function renderReferenceSignals(item) {
  const analysis = item.request_snapshot?.source_question_analysis;
  const sourceQuestion = item.request_snapshot?.source_question;
  if (!analysis && !sourceQuestion) {
    return "";
  }

  const businessCards = (analysis?.business_card_ids || [])
    .map((value) => `<span class="chip">${escapeHtml(value)}</span>`)
    .join("");
  const queryTerms = (analysis?.query_terms || []).slice(0, 6).join(" / ");
  const lengthTarget = analysis?.target_length ?? "-";
  const lengthTolerance = analysis?.length_tolerance ?? "-";
  const anchorReason = item.material_selection?.anchor_adaptation_reason || "-";

  return `
    <div class="reference-insight-box" style="margin-top: 16px;">
      <div class="section-title">参考题驱动信息</div>
      <div class="material-grid">
        <div class="mini-card">
          <strong>原题题干</strong>
          <div>${escapeHtml(sourceQuestion?.stem || "未提供")}</div>
        </div>
        <div class="mini-card">
          <strong>命中业务卡</strong>
          <div style="display:flex;flex-wrap:wrap;gap:8px;">${businessCards || '<span class="muted">未命中</span>'}</div>
        </div>
        <div class="mini-card">
          <strong>长度目标</strong>
          <div>${escapeHtml(lengthTarget)} ± ${escapeHtml(lengthTolerance)}</div>
        </div>
        <div class="mini-card">
          <strong>锚点裁缩</strong>
          <div>${escapeHtml(anchorReason)}</div>
        </div>
      </div>
      <div class="mini-card">
        <strong>检索关键词</strong>
        <div>${escapeHtml(queryTerms || "未提供")}</div>
      </div>
      <div class="mini-card" style="margin-top: 12px;">
        <strong>原题解析</strong>
        <div>${escapeHtml(sourceQuestion?.analysis || "未提供")}</div>
      </div>
    </div>
  `;
}

function renderEditableQuestionFields(item, generated, materialText) {
  const originalMaterial = cleanMaterialText(item.material_selection?.original_text || materialText);
  const answer = generated.answer || "";
  return `
    <div style="margin-top: 16px; padding: 18px; border: 1px solid var(--line); border-radius: var(--radius-lg); background: #fcfdff;">
      <div class="section-title">手工编辑区</div>
      <div style="display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:14px;">
        <label class="reference-field reference-field-full">
          <span>加工后文段</span>
          <textarea data-manual-field="material_text" rows="6">${escapeHtml(materialText)}</textarea>
        </label>
        <label class="reference-field reference-field-full">
          <span>原本文段</span>
          <textarea rows="6" readonly>${escapeHtml(originalMaterial)}</textarea>
        </label>
        <label class="reference-field reference-field-full">
          <span>题干</span>
          <textarea data-manual-field="stem" rows="2">${escapeHtml(generated.stem || item.stem_text || "")}</textarea>
        </label>
        <label class="reference-field">
          <span>选项 A</span>
          <textarea data-manual-field="option_A" rows="2">${escapeHtml(generated.options?.A || "")}</textarea>
        </label>
        <label class="reference-field">
          <span>选项 B</span>
          <textarea data-manual-field="option_B" rows="2">${escapeHtml(generated.options?.B || "")}</textarea>
        </label>
        <label class="reference-field">
          <span>选项 C</span>
          <textarea data-manual-field="option_C" rows="2">${escapeHtml(generated.options?.C || "")}</textarea>
        </label>
        <label class="reference-field">
          <span>选项 D</span>
          <textarea data-manual-field="option_D" rows="2">${escapeHtml(generated.options?.D || "")}</textarea>
        </label>
        <label class="reference-field">
          <span>答案</span>
          <select data-manual-field="answer">
            <option value="A" ${answer === "A" ? "selected" : ""}>A</option>
            <option value="B" ${answer === "B" ? "selected" : ""}>B</option>
            <option value="C" ${answer === "C" ? "selected" : ""}>C</option>
            <option value="D" ${answer === "D" ? "selected" : ""}>D</option>
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

async function buildQuestionCard(itemSeed, displayIndex) {
  const item = itemSeed;
  const generated = item.generated_question || {};
  const { history, replacementList, controlPanel } = await fetchCardSupportData(item.item_id);
  if (history) {
    state.histories.set(item.item_id, history);
  }

  const source = resolveMaterialSource(item);
  const materialText = cleanMaterialText(item.material_text || item.material_selection?.text || "暂无文段文本");
  const materialGenre = item.material_selection?.document_genre || source.document_genre || "-";
  const status = item.current_status || item.statuses?.review_status || "pending_review";
  const approved = status === "approved";

  const card = document.createElement("div");
  card.className = "question-card";
  card.dataset.itemId = item.item_id;
  card.innerHTML = `
    <div class="question-main">
      <div class="question-head">
        <div><h3>题目 ${displayIndex}</h3></div>
        <div class="question-meta">
          <span class="chip ${statusClass(status)}">${escapeHtml(humanizeStatus(status))}</span>
          <span class="chip">${escapeHtml(humanizeValueLabel(item.question_type || "-"))}</span>
          <span class="chip">${escapeHtml(humanizeValueLabel(item.pattern_id || item.selected_pattern || "-"))}</span>
          <span class="chip">${escapeHtml(humanizeValueLabel(item.difficulty_target || "-"))}</span>
        </div>
      </div>

      <div class="question-box">
        <div class="passage-label">文段正文</div>
        <div class="passage-preview">${escapeHtml(materialText)}</div>
        <div class="passage-label" style="margin-top: 18px;">题目</div>
        <div class="question-stem">${escapeHtml(item.stem_text || generated.stem || "暂无题干")}</div>
        <div class="option-list">${renderOptions(generated.options || {})}</div>
        <div class="answer-row"><strong>答案：</strong>${escapeHtml(generated.answer || "-")}</div>
        <div class="analysis-row"><strong>解析：</strong>${escapeHtml(generated.analysis || "-")}</div>
      </div>

      <div class="material-box" style="margin-top: 16px;">
        <div class="section-title">文段信息</div>
        <div class="material-grid">
          <div class="mini-card">
            <strong>文本来源</strong>
            <div>${escapeHtml(source.source_name || source.source_id || "-")}</div>
          </div>
          <div class="mini-card">
            <strong>文章来源</strong>
            <div>${escapeHtml(source.article_title || "-")}</div>
          </div>
          <div class="mini-card">
            <strong>文本分类</strong>
            <div>${escapeHtml(materialGenre)}</div>
          </div>
          <div class="mini-card">
            <strong>文段结构</strong>
            <div>${escapeHtml(item.material_selection?.material_structure_label || "未标注")}</div>
          </div>
          <div class="mini-card">
            <strong>可独立成段分</strong>
            <div>${escapeHtml(item.material_selection?.standalone_readability ?? "-")}</div>
          </div>
          <div class="mini-card">
            <strong>已使用次数</strong>
            <div>${escapeHtml(item.material_usage_count_before ?? 0)}</div>
          </div>
        </div>
        <div class="material-grid" style="margin-top: 12px;">
          <div class="mini-card" style="grid-column: 1 / -1;">
            <strong>结构说明</strong>
            <div>${escapeHtml(item.material_selection?.material_structure_reason || "暂无结构说明")}</div>
          </div>
        </div>
        <div class="material-text">${escapeHtml(materialText)}</div>
      </div>

      <div class="system-box" style="margin-top: 16px;">
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
      ${renderEditableQuestionFields(item, generated, materialText)}

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
      <button type="button" class="success-btn ${approved ? "is-approved" : ""}" data-action="confirm" data-item-id="${item.item_id}" ${
        approved ? "disabled" : ""
      }>${approved ? "已通过 ✓" : "通过"}</button>
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

  const currentItem = state.items.find((item) => item.item_id === itemId);
  if (!currentItem) {
    throw new Error("未找到当前题目。");
  }

  if (action === "toggle-modify") {
    const panel = card.querySelector(`#modify-${CSS.escape(itemId)}`);
    if (panel) {
      panel.classList.toggle("active");
    }
    return;
  }

  if (action === "confirm") {
    button.disabled = true;
    const originalText = button.textContent;
    button.textContent = "确认中...";
    try {
      const response = await apiFetch(`/api/v1/questions/${itemId}/confirm`, {
        method: "POST",
        body: JSON.stringify({}),
      });
      if (response?.item) {
        upsertItem(response.item);
      }
      await renderResults();
      switchScreen("result");
    } catch (error) {
      button.disabled = false;
      button.textContent = originalText;
      throw error;
    }
    return;
  }

  if (action === "discard") {
    button.disabled = true;
    const originalText = button.textContent;
    button.textContent = "作废中...";
    try {
      const response = await apiFetch(`/api/v1/questions/${itemId}/review-actions`, {
        method: "POST",
        body: JSON.stringify({ action: "discard" }),
      });
      if (response?.item) {
        upsertItem(response.item);
      }
      await renderResults();
      switchScreen("result");
    } catch (error) {
      button.disabled = false;
      button.textContent = originalText;
      throw error;
    }
    return;
  }

  if (action === "fine-tune") {
    const textarea = card.querySelector(`#fineTune-${CSS.escape(itemId)}`);
    const instruction = textarea?.value.trim() || "";
    if (!instruction) {
      alert("请先输入微调说明。");
      return;
    }
    await executeMutationWithLoading(
      () =>
        apiFetch(`/api/v1/questions/${itemId}/fine-tune`, {
          method: "POST",
          body: JSON.stringify({ instruction }),
        }),
      ACTION_LOADING_COPY.fineTune,
    );
    return;
  }

  if (action === "text-modify") {
    const select = card.querySelector(`#replacementMaterial-${CSS.escape(itemId)}`);
    const materialId = select?.value || "";
    if (!materialId) {
      alert("当前没有可替换文段。");
      return;
    }
    await executeMutationWithLoading(
      () =>
        apiFetch(`/api/v1/questions/${itemId}/review-actions`, {
          method: "POST",
          body: JSON.stringify({ action: "text_modify", control_overrides: { material_id: materialId } }),
        }),
      ACTION_LOADING_COPY.textModify,
    );
    return;
  }

  if (action === "question-modify") {
    const controlOverrides = collectSimpleOverrides(card, currentItem);
    await executeMutationWithLoading(
      () =>
        apiFetch(`/api/v1/questions/${itemId}/review-actions`, {
          method: "POST",
          body: JSON.stringify({ action: "question_modify", control_overrides: controlOverrides }),
        }),
      ACTION_LOADING_COPY.questionModify,
    );
    return;
  }

  if (action === "manual-save") {
    const manualPatch = collectManualEditPayload(card);
    await executeMutationWithLoading(
      () =>
        apiFetch(`/api/v1/questions/${itemId}/review-actions`, {
          method: "POST",
          body: JSON.stringify({
            action: "manual_edit",
            instruction: "manual edit from review UI",
            control_overrides: { manual_patch: manualPatch },
          }),
        }),
      ACTION_LOADING_COPY.questionModify,
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
  const delivery = await apiFetch(`/api/v1/review/batches/${state.batchId}/delivery`);
  const approvedCount = Array.isArray(delivery.items)
    ? delivery.items.length
    : state.items.filter((item) => String(item.current_status || "").toLowerCase() === "approved").length;
  if (!approvedCount) {
    throw new Error("当前批次还没有已通过题目，暂时不能导出。");
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
  populateSelect($("questionFocus"), PRIMARY_OPTIONS);
  if (!$("questionFocus").value && $("questionFocus").options.length) {
    $("questionFocus").selectedIndex = 0;
  }
  renderSecondaryOptions();
  syncCountValue();
  renderLoadingStep(0);

  $("questionFocus").addEventListener("change", renderSecondaryOptions);
  $("specialType").addEventListener("change", renderMaterialStructureOptions);
  $("count").addEventListener("input", syncCountValue);
  $("generateForm").addEventListener("submit", generateQuestions);
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

