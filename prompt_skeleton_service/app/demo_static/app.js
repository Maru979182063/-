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
    { label: "问题-对策-案例排序", value: "问题-对策-案例排序" },
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
  中心理解题: [{ label: "中心理解题", value: "中心理解题" }],
};

const LOADING_STEPS = [
  { title: "参数解码", desc: "解析一级题卡、二级分类和难度请求。" },
  { title: "题卡匹配", desc: "确定 question_type、pattern 与统一骨架。" },
  { title: "材料获取", desc: "从本地材料库筛出候选文段。" },
  { title: "Prompt 组装", desc: "组合材料文本、制作方式和难度参数。" },
  { title: "题目生成", desc: "调用 LLM 输出标准题目格式。" },
  { title: "自动校验", desc: "执行结构、格式与题型规则校验。" },
  { title: "LLM 审核", desc: "进入强制 LLM 审核，再交给用户确认。" },
];

const OPTION_CONFUSION_OPTIONS = [
  { value: "medium", label: "中" },
  { value: "medium_high", label: "中高" },
  { value: "high", label: "高" },
];

const DISTRACTOR_MODE_OPTIONS = [
  { value: "topic_shift", label: "主题偏移" },
  { value: "overgeneralization", label: "过度泛化" },
  { value: "function_misread", label: "功能误读" },
  { value: "title_too_wide", label: "标题过宽" },
  { value: "title_too_narrow", label: "标题过窄" },
  { value: "catchy_but_offcore", label: "吸睛但偏核" },
  { value: "old_topic_return", label: "回到旧话题" },
  { value: "side_branch_shift", label: "支线偏移" },
  { value: "abstract_overraise", label: "空泛拔高" },
  { value: "surface_match", label: "表层匹配" },
  { value: "logic_reverse", label: "逻辑反转" },
  { value: "keyword_trap", label: "关键词陷阱" },
];

const DIFFICULTY_BOOST_OPTIONS = [
  { value: "0", label: "保持当前" },
  { value: "1", label: "提高一档" },
  { value: "2", label: "直接拉高" },
];

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

function renderSecondaryOptions() {
  const primary = $("questionFocus").value;
  populateSelect($("specialType"), SECONDARY_BY_PRIMARY[primary] || []);
}

function buildGeneratePayload() {
  const specialType = $("specialType").value;
  return {
    question_focus: $("questionFocus").value,
    difficulty_level: $("difficultyLevel").value,
    special_question_types: specialType ? [specialType] : [],
    count: Number.parseInt($("count").value || "1", 10),
    topic: null,
    passage_style: null,
    use_fewshot: true,
    fewshot_mode: "structure_only",
    type_slots: {},
    extra_constraints: {},
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
  $("loadingNode").textContent = LOADING_STEPS[activeIndex]?.title || "处理中...";
  $("loadingNodeDesc").textContent = LOADING_STEPS[activeIndex]?.desc || "";
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

function stopLoadingSequence() {
  if (state.loadingTimer) {
    window.clearInterval(state.loadingTimer);
    state.loadingTimer = null;
  }
}

async function runWithLoading(task, fallbackScreen = "result") {
  switchScreen("loading");
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
  try {
    const response = await runWithLoading(() =>
      apiFetch("/api/v1/questions/generate", {
        method: "POST",
        body: JSON.stringify(buildGeneratePayload()),
      }),
      "builder",
    );
    state.batchId = response.batch_id;
    state.items = response.items || [];
    await renderResults();
    switchScreen("result");
  } catch (error) {
    alert(`生成失败：${error.message}`);
  }
}

async function executeResultMutation(task) {
  await runWithLoading(task, "result");
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

async function buildQuestionCard(itemSummary, displayIndex) {
  const item = await apiFetch(`/api/v1/questions/${itemSummary.item_id}`);
  const history = await apiFetch(`/api/v1/review/items/${item.item_id}/history`);
  const replacementList = await apiFetch(`/api/v1/questions/${item.item_id}/replacement-materials?limit=8`);
  const controlPanel = await apiFetch(`/api/v1/questions/${item.item_id}/controls`);
  state.histories.set(item.item_id, history);

  const generated = item.generated_question || {};
  const card = document.createElement("div");
  card.className = "question-card";
  card.dataset.itemId = item.item_id;
  card.innerHTML = `
    <div class="question-main">
      <div class="question-head">
        <div><h3>题目 ${displayIndex}</h3></div>
        <div class="question-meta">
          <span class="chip status">${escapeHtml(item.current_status || "-")}</span>
          <span class="chip">${escapeHtml(item.question_type || "-")}</span>
          <span class="chip">${escapeHtml(item.pattern_id || item.selected_pattern || "-")}</span>
          <span class="chip">${escapeHtml(item.difficulty_target || "-")}</span>
        </div>
      </div>

      <div class="question-box">
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
            <div>${escapeHtml(item.material_source?.source_name || item.material_source?.source_id || "-")}</div>
          </div>
          <div class="mini-card">
            <strong>文章标题</strong>
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
        <div class="material-text">${escapeHtml(item.material_text || "暂无文段文本")}</div>
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
      <button type="button" class="success-btn" data-action="confirm" data-item-id="${item.item_id}">通过</button>
      <button type="button" class="secondary-btn" data-action="toggle-modify" data-item-id="${item.item_id}">修改</button>
      <button type="button" class="danger-btn" data-action="discard" data-item-id="${item.item_id}">作废</button>
    </div>
  `;

  renderReplacementSelect(card.querySelector(`#replacementMaterial-${CSS.escape(item.item_id)}`), replacementList.items || []);
  renderSimpleControls(
    card.querySelector(`#simpleControls-${CSS.escape(item.item_id)}`),
    buildQuestionModifyControls(item, controlPanel),
    item.item_id,
  );
  return card;
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
    option.textContent = `${item.article_title || item.label}｜${item.source_name || "-"}｜已用 ${item.usage_count_before} 次`;
    selectEl.appendChild(option);
  });
}

function buildQuestionModifyControls(item, controlPanel) {
  const controlsByKey = new Map((controlPanel?.controls || []).map((control) => [control.control_key, control]));
  const difficultyControl = controlsByKey.get("difficulty_target");
  const confusionControl =
    controlsByKey.get("option_confusion") || controlsByKey.get("distractor_strength") || null;
  const distractorControl = controlsByKey.get("distractor_modes");
  const controls = [
    {
      controlKey: "difficulty_raise_factor",
      label: "整体难度提高系数",
      currentValue: "0",
      options: DIFFICULTY_BOOST_OPTIONS,
      description: difficultyControl?.description || "用于整体拉高当前题目的难度档位。",
    },
  ];
  if (confusionControl) {
    controls.push({
      controlKey: confusionControl.control_key,
      label: "选项迷惑度",
      currentValue: confusionControl.current_value ?? confusionControl.default_value ?? "",
      options: confusionControl.options || OPTION_CONFUSION_OPTIONS,
      description: confusionControl.description || "控制正确项和干扰项的接近程度。",
    });
  }
  if (distractorControl) {
    const currentDistractorValues = Array.isArray(distractorControl.current_value)
      ? distractorControl.current_value
      : Array.isArray(distractorControl.default_value)
        ? distractorControl.default_value
        : [distractorControl.current_value || distractorControl.default_value || ""];
    controls.push({
      controlKey: distractorControl.control_key,
      label: "干扰项方式",
      currentValue: currentDistractorValues,
      options: distractorControl.options || DISTRACTOR_MODE_OPTIONS,
      description: distractorControl.description || "分别指定三个错误项各自的主要偏离方式。",
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
    if (control.multiValueCount && Array.isArray(control.currentValue)) {
      const group = document.createElement("div");
      group.className = "multi-control-group";
      for (let index = 0; index < control.multiValueCount; index += 1) {
        const select = document.createElement("select");
        select.dataset.scope = "simple-item";
        select.dataset.itemId = itemId;
        select.dataset.controlKey = control.controlKey;
        select.dataset.controlIndex = String(index);
        control.options.forEach((option) => {
          const element = document.createElement("option");
          element.value = option.value;
          element.textContent = option.label;
          const selectedValue = control.currentValue[index] ?? control.currentValue[0] ?? "";
          if (String(option.value) === String(selectedValue)) {
            element.selected = true;
          }
          select.appendChild(element);
        });
        group.appendChild(select);
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
        element.textContent = option.label;
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
  const groupedInputs = new Map();
  inputs.forEach((input) => {
    const key = input.dataset.controlKey;
    if (!groupedInputs.has(key)) {
      groupedInputs.set(key, []);
    }
    groupedInputs.get(key).push(input);
  });
  groupedInputs.forEach((group, key) => {
    const input = group[0];
    if (key === "difficulty_raise_factor") {
      overrides.difficulty_target = bumpDifficulty(currentItem.difficulty_target || "medium", input.value);
      return;
    }
    if (key === "distractor_modes") {
      overrides[key] = group
        .map((select) => select.value)
        .filter((value, index, array) => value && array.indexOf(value) === index)
        .slice(0, 3);
      return;
    }
    overrides[key] = input.value;
  });
  return overrides;
}

function collectControls(card, itemId) {
  return collectSimpleOverrides(card, itemId);
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
  await renderResults();
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
    await executeResultMutation(async () => {
      await apiFetch(`/api/v1/questions/${itemId}/confirm`, {
        method: "POST",
        body: JSON.stringify({}),
      });
      await refreshItem(itemId);
    });
    return;
  }
  if (action === "discard") {
    await executeResultMutation(async () => {
      await apiFetch(`/api/v1/questions/${itemId}/review-actions`, {
        method: "POST",
        body: JSON.stringify({ action: "discard" }),
      });
      await refreshItem(itemId);
    });
    return;
  }
  if (action === "fine-tune") {
    const textarea = card.querySelector(`#fineTune-${CSS.escape(itemId)}`);
    const instruction = textarea?.value.trim() || "";
    if (!instruction) {
      alert("请先输入微调说明。");
      return;
    }
    await executeResultMutation(async () => {
      await apiFetch(`/api/v1/questions/${itemId}/fine-tune`, {
        method: "POST",
        body: JSON.stringify({ instruction }),
      });
      await refreshItem(itemId);
    });
    return;
  }
  if (action === "text-modify") {
    const select = card.querySelector(`#replacementMaterial-${CSS.escape(itemId)}`);
    const materialId = select?.value || "";
    if (!materialId) {
      alert("当前没有可替换文段。");
      return;
    }
    await executeResultMutation(async () => {
      await apiFetch(`/api/v1/questions/${itemId}/review-actions`, {
        method: "POST",
        body: JSON.stringify({ action: "text_modify", control_overrides: { material_id: materialId } }),
      });
      await refreshItem(itemId);
    });
    return;
  }
  if (action === "question-modify") {
    const controlOverrides = collectControls(card, itemId);
    await executeResultMutation(async () => {
      await apiFetch(`/api/v1/questions/${itemId}/review-actions`, {
        method: "POST",
        body: JSON.stringify({ action: "question_modify", control_overrides: controlOverrides }),
      });
      await refreshItem(itemId);
    });
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
  const blob = await response.blob();
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = `${state.batchId}.md`;
  anchor.click();
  URL.revokeObjectURL(url);
}

function wireEvents() {
  $("questionFocus").addEventListener("change", renderSecondaryOptions);
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

function initPage() {
  populateSelect($("questionFocus"), PRIMARY_OPTIONS);
  renderSecondaryOptions();
  syncCountValue();
  wireEvents();
}

initPage();
