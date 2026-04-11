const QUESTION_FOCUS_OPTIONS = [
  { label: "中心理解题", value: "center_understanding", specialTypes: [{ label: "中心理解题", value: "center_understanding" }] },
  { label: "标题填入题", value: "title_selection", specialTypes: [{ label: "选择标题", value: "title_selection" }] },
  {
    label: "接语选择题",
    value: "continuation",
    specialTypes: [
      { label: "尾句直接承接", value: "tail_anchor_direct_extend" },
      { label: "问题后接对策", value: "problem_solution_hook" },
      { label: "机制展开", value: "mechanism_unfolding" },
    ],
  },
  {
    label: "语句排序题",
    value: "sentence_order",
    specialTypes: [
      { label: "双锚点锁定", value: "dual_anchor_lock" },
      { label: "承接并列展开", value: "carry_parallel_expand" },
      { label: "问题-对策-案例排序", value: "problem_solution_case_blocks" },
    ],
  },
  {
    label: "语句填空题",
    value: "sentence_fill",
    specialTypes: [
      { label: "开头总起", value: "opening_summary" },
      { label: "衔接过渡", value: "bridge_transition" },
      { label: "结尾总结", value: "ending_summary" },
    ],
  },
];

const SAMPLE_TEXT = `新就业群体已成为城市运行不可忽视的一部分。外卖骑手、网约车司机、快递员等群体，一头连着千家万户的日常生活，一头连着平台经济和城市服务的效率提升。面对他们在休息、就餐、停车、权益保障等方面的现实需求，一些地方开始建设友好场景、优化公共服务、完善协同治理机制。服务好新就业群体，看似是在解决具体的民生问题，实际上也是在提升城市治理温度、优化营商环境，并为城市发展持续赋能。`;

const MOCK_RESULT = {
  warnings: [
    "Forced user-material mode enabled: using the user-supplied passage directly and bypassing passage_service retrieval.",
    "This result is tagged as cautionary for later adaptive analysis.",
  ],
  items: [
    {
      question_type: "main_idea",
      business_subtype: "center_understanding",
      current_status: "pending_review",
      generation_mode: "forced_user_material",
      material_source_type: "user_uploaded",
      forced_generation: true,
      warnings: [
        "样例结果：这是一张前端 mock 卡片，用于展示完成态。",
      ],
      material_selection: {
        source: {
          caution_tag: "user_uploaded_material_unvalidated",
        },
      },
      generated_question: {
        stem: "根据这段材料，下列最能概括文段主旨的一项是：",
        answer: "A",
        options: {
          A: "服务好新就业群体，既是保障民生，也是为城市治理升级和发展赋能。",
          B: "新就业群体迅速壮大，说明平台经济已经取代传统城市服务模式。",
          C: "只要完善骑手和司机的休息场所建设，就能系统解决城市治理难题。",
          D: "城市治理的关键在于加强对平台企业的统一管理，而非关注劳动者需求。",
        },
      },
    },
  ],
};

const SPECIAL_LABELS = {
  center_understanding: "中心理解题",
  title_selection: "选择标题",
  tail_anchor_direct_extend: "尾句直接承接",
  problem_solution_hook: "问题后接对策",
  mechanism_unfolding: "机制展开",
  dual_anchor_lock: "双锚点锁定",
  carry_parallel_expand: "承接并列展开",
  problem_solution_case_blocks: "问题-对策-案例排序",
  opening_summary: "开头总起",
  bridge_transition: "衔接过渡",
  ending_summary: "结尾总结",
};

function $(id) {
  return document.getElementById(id);
}

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
    const message =
      typeof payload === "string"
        ? payload
        : payload?.error?.message || payload?.detail || JSON.stringify(payload, null, 2);
    throw new Error(String(message));
  }
  return payload;
}

function setStatus(message, type = "info") {
  const box = $("formStatus");
  if (!message) {
    box.hidden = true;
    box.textContent = "";
    box.className = "forced-status forced-status-info";
    return;
  }
  box.hidden = false;
  box.textContent = message;
  box.className = `forced-status ${type === "error" ? "forced-status-error" : "forced-status-info"}`;
}

function populateQuestionFocus() {
  const select = $("questionFocus");
  select.innerHTML = QUESTION_FOCUS_OPTIONS.map((item) => `<option value="${item.value}">${item.label}</option>`).join("");
  populateSpecialTypes();
}

function populateSpecialTypes() {
  const focus = $("questionFocus").value;
  const group = QUESTION_FOCUS_OPTIONS.find((item) => item.value === focus) || QUESTION_FOCUS_OPTIONS[0];
  $("specialType").innerHTML = group.specialTypes
    .map((item) => `<option value="${item.value}">${item.label}</option>`)
    .join("");
}

function fillSample() {
  $("materialTitle").value = "服务新就业群体也是为城市发展赋能";
  $("materialTopic").value = "城市治理";
  $("documentGenre").value = "commentary";
  $("sourceLabel").value = "用户上传";
  $("materialText").value = SAMPLE_TEXT;
  $("questionFocus").value = "center_understanding";
  populateSpecialTypes();
}

function renderWarnings(warnings) {
  if (!Array.isArray(warnings) || !warnings.length) {
    return "";
  }
  return `
    <ul class="forced-warning-list">
      ${warnings.map((warning) => `<li>${escapeHtml(warning)}</li>`).join("")}
    </ul>
  `;
}

function renderItemCard(item, batchWarnings) {
  const generated = item.generated_question || {};
  const options = generated.options || {};
  const cautionTag = (((item.material_selection || {}).source || {}).caution_tag) || "";
  const answer = generated.answer || "-";
  const tags = [
    item.generation_mode ? `<span class="forced-badge forced-badge-info">${escapeHtml(item.generation_mode)}</span>` : "",
    item.forced_generation ? `<span class="forced-badge forced-badge-warn">forced_generation</span>` : "",
    cautionTag ? `<span class="forced-badge forced-badge-warn">${escapeHtml(cautionTag)}</span>` : "",
  ].join("");

  return `
    <article class="forced-result-card">
      <div class="forced-result-head">
        <div>
          <h3>${escapeHtml(generated.stem || "已返回结果，但题干为空")}</h3>
        </div>
        <div class="forced-badge-row">${tags}</div>
      </div>
      <div class="forced-meta">
        <div class="forced-meta-box">
          <strong>当前状态</strong>
          <div>${escapeHtml(item.current_status || "-")}</div>
        </div>
        <div class="forced-meta-box">
          <strong>答案</strong>
          <div>${escapeHtml(answer)}</div>
        </div>
        <div class="forced-meta-box">
          <strong>题型 / 业务子类</strong>
          <div>${escapeHtml(item.question_type || "-")} / ${escapeHtml(item.business_subtype || "-")}</div>
        </div>
        <div class="forced-meta-box">
          <strong>材料来源</strong>
          <div>${escapeHtml(item.material_source_type || "-")}</div>
        </div>
      </div>
      <p class="forced-stem">${escapeHtml(generated.stem || "暂无题干")}</p>
      <div class="forced-options">
        ${["A", "B", "C", "D"]
          .map((key) => `<div class="forced-option"><strong>${key}.</strong> ${escapeHtml(options[key] || "")}</div>`)
          .join("")}
      </div>
      ${renderWarnings([...(item.warnings || []), ...(batchWarnings || [])])}
    </article>
  `;
}

function renderResult(payload) {
  const stack = $("resultStack");
  const items = payload.items || [];
  if (!items.length) {
    stack.innerHTML = `
      <div class="forced-empty">
        本次请求没有返回可展示的结果。<br />
        你可以换一段用户材料再试，也可以查看接口返回中的 warning。
      </div>
    `;
    return;
  }
  stack.innerHTML = items.map((item) => renderItemCard(item, payload.warnings || [])).join("");
}

function previewMockResult() {
  renderResult(MOCK_RESULT);
  setStatus("这里展示的是完成态 mock，方便你直接看页面效果和 caution 标记。", "info");
}

async function handleSubmit(event) {
  event.preventDefault();
  setStatus("系统正在按用户自带材料强制制作，这条模式会跳过 passage 检索。", "info");
  $("generateButton").disabled = true;
  $("fillSampleButton").disabled = true;

  const payload = {
    generation_mode: "forced_user_material",
    question_focus: $("questionFocus").value,
    difficulty_level: $("difficultyLevel").value,
    special_question_types: [$("specialType").value].filter(Boolean),
    count: 1,
    user_material: {
      text: $("materialText").value.trim(),
      title: $("materialTitle").value.trim() || null,
      topic: $("materialTopic").value.trim() || null,
      document_genre: $("documentGenre").value || null,
      source_label: $("sourceLabel").value.trim() || null,
    },
  };

  try {
    const result = await apiFetch("/api/v1/questions/generate", {
      method: "POST",
      body: JSON.stringify(payload),
    });
    renderResult(result);
    setStatus("制作完成。这条结果已经带上 forced_user_material 和 caution 标记。", "info");
  } catch (error) {
    setStatus(`制作失败：${error.message}`, "error");
    $("resultStack").innerHTML = `
      <div class="forced-empty">
        接口没有返回可展示结果。<br />
        错误信息：${escapeHtml(error.message)}
      </div>
    `;
  } finally {
    $("generateButton").disabled = false;
    $("fillSampleButton").disabled = false;
  }
}

function boot() {
  populateQuestionFocus();
  $("questionFocus").addEventListener("change", populateSpecialTypes);
  $("fillSampleButton").addEventListener("click", fillSample);
  $("previewResultButton").addEventListener("click", previewMockResult);
  $("forcedMaterialForm").addEventListener("submit", handleSubmit);
}

boot();
