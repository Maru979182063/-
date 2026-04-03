(function () {
  const QUESTION_FOCUS_VALUES = ["", "main_idea", "continuation", "sentence_order", "sentence_fill", "center_understanding"];
  const QUESTION_FOCUS_LABELS = ["不指定", "主旨中心类", "接语选择题", "语句排序题", "语句填空题", "中心理解题"];

  const SPECIAL_TYPE_VALUES = {
    main_idea: ["", "title_selection", "main_idea_general", "structure_summary", "local_paragraph_summary"],
    continuation: [
      "",
      "tail_anchor_direct_extend",
      "problem_solution_hook",
      "mechanism_unfolding",
      "raised_theme_to_subtopic",
      "summary_with_new_pivot",
      "judgement_to_reason",
      "case_to_macro_unfold",
      "multi_branch_focus",
      "tension_explained",
      "method_expansion",
    ],
    sentence_order: ["", "dual_anchor_lock", "carry_parallel_expand", "viewpoint_reason_action", "problem_solution_case_blocks"],
    sentence_fill: [
      "",
      "opening_summary",
      "bridge_transition",
      "middle_focus_shift",
      "middle_explanation",
      "ending_summary",
      "ending_elevation",
      "inserted_reference_match",
      "comprehensive_multi_match",
    ],
    center_understanding: [""],
    "": [""],
  };

  const SPECIAL_TYPE_LABELS = {
    main_idea: ["不指定（自动匹配）", "选择标题", "主旨概括", "结构概括", "局部段意概括"],
    continuation: [
      "不指定（自动匹配）",
      "尾句直接承接",
      "问题后接对策",
      "机制展开",
      "主题转分话题",
      "总结后开启新支点",
      "观点后接原因",
      "个案到宏观展开",
      "多分支聚焦",
      "张力解释",
      "方法延展",
    ],
    sentence_order: ["不指定（自动匹配）", "双锚点锁定", "承接并列展开", "观点-原因-行动排序", "问题-对策-案例排序"],
    sentence_fill: ["不指定（自动匹配）", "开头总起", "衔接过渡", "中段焦点切换", "中段解释说明", "结尾总结", "结尾升华", "定位插入匹配", "综合多点匹配"],
    center_understanding: ["不指定（按中心理解默认）"],
    "": ["请选择"],
  };

  const VALUE_MAP = {
    "": "不指定",
    Auto: "不指定（自动匹配）",
    auto: "不指定（自动匹配）",
    Select: "不指定",
    select: "不指定",
    "请选择": "不指定",
    pending_review: "待复核",
    approved: "已通过",
    auto_failed: "建议复核",
    discarded: "已作废",
    main_idea: "主旨中心类",
    continuation: "接语选择题",
    sentence_order: "语句排序题",
    sentence_fill: "语句填空题",
    center_understanding: "中心理解题",
    title_selection: "选择标题",
    main_idea_general: "主旨概括",
    structure_summary: "结构概括",
    local_paragraph_summary: "局部段意概括",
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
    precomputed_window_trim: "预计算窗口裁缩",
    manual_material_override: "手动指定材料",
    reference_source_fallback: "参考母题兜底",
    text_modify: "替换文段",
    fine_tune: "微调",
    question_modify: "按参数重做",
    manual_edit: "手工编辑保存",
    confirm: "通过",
    discard: "作废",
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
    middle_conclusion: "中段结论",
    example_fragment: "例子片段",
    scope_shift: "范围偏移",
    undergeneralization: "概括不足",
    fabrication: "无中生有",
    exemplification: "举例说明",
  };

  function humanize(value) {
    const text = String(value ?? "").trim();
    return VALUE_MAP[text] || text;
  }

  function softenAuditText(text) {
    return String(text || "")
      .replace(/题目不合格/g, "当前版本建议继续优化")
      .replace(/核心问题是/g, "当前建议重点关注")
      .replace(/无法形成稳定的唯一(?:一)?排序答案/g, "排序依据还可进一步增强")
      .replace(/唯一答案强度不足/g, "唯一答案感还可进一步增强")
      .replace(/材料拼接感明显/g, "材料衔接还可进一步顺滑")
      .replace(/材料本身的可排序性不足/g, "材料的排序线索还可进一步增强")
      .replace(/难以支撑唯一答案/g, "当前版本的区分度还可进一步增强")
      .replace(/缺少足够的确定性线索/g, "确定性线索还可以继续补强")
      .replace(/缺少明确收束功能/g, "尾句收束感还可以继续补强")
      .replace(/多条近似可行路径/g, "还存在若干接近的排序路径")
      .replace(/干扰项过于接近/g, "干扰项区分度还可以继续拉开")
      .replace(/区分度不够/g, "区分度还可以继续优化")
      .replace(/建议重写材料并重新设计句间逻辑/g, "建议优先调整材料与句间衔接后再继续优化");
  }

  function patchQuestionFocusSelect() {
    const select = document.getElementById("questionFocus");
    if (!select) return;
    Array.from(select.options || []).forEach((option, index) => {
      option.value = QUESTION_FOCUS_VALUES[index] ?? option.value;
      option.textContent = QUESTION_FOCUS_LABELS[index] ?? humanize(option.value);
    });
  }

  function patchSpecialTypeSelect() {
    const questionFocus = document.getElementById("questionFocus");
    const specialType = document.getElementById("specialType");
    if (!specialType || !questionFocus) return;
    const focusValue = String(questionFocus.value || "");
    const values = SPECIAL_TYPE_VALUES[focusValue] || SPECIAL_TYPE_VALUES[""];
    const labels = SPECIAL_TYPE_LABELS[focusValue] || SPECIAL_TYPE_LABELS[""];
    Array.from(specialType.options || []).forEach((option, index) => {
      option.value = values[index] ?? option.value;
      option.textContent = labels[index] ?? humanize(option.value);
    });
  }

  function patchGenericSelects(root = document) {
    root.querySelectorAll("select").forEach((select) => {
      if (select.id === "questionFocus" || select.id === "specialType") return;
      Array.from(select.options || []).forEach((option) => {
        const value = String(option.value || "").trim();
        option.textContent = humanize(value || option.textContent);
      });
    });
  }

  function normalizeMaterialText(text) {
    return String(text || "")
      .replace(/\r\n/g, "\n")
      .replace(/\n{2,}/g, "\n")
      .split("\n")
      .map((line) => line.trim())
      .filter(Boolean)
      .join(" ");
  }

  function cardIsSentenceOrder(node) {
    const card = node.closest(".card, .result-card, .question-card, .result-item, .question-item") || node.parentElement;
    const metaText = String(card?.querySelector(".question-meta")?.textContent || "").toLowerCase();
    return metaText.includes("sentence_order") || metaText.includes("语句排序");
  }

  function patchMaterialParagraphs(root = document) {
    root.querySelectorAll(".reading-material, details .material-text").forEach((node) => {
      if (cardIsSentenceOrder(node)) return;
      const original = String(node.textContent || "").trim();
      if (!original) return;
      const normalized = normalizeMaterialText(original);
      if (normalized && normalized !== original) {
        node.textContent = normalized;
      }
    });
  }

  function patchBlockedBanner(root = document) {
    root.querySelectorAll(".blocked-banner").forEach((node) => {
      node.remove();
    });
  }

  function ensurePatchStyles() {
    if (document.getElementById("zhPatchStyles")) return;
    const style = document.createElement("style");
    style.id = "zhPatchStyles";
    style.textContent = `
      .blocked-banner { display: none !important; }
    `;
    document.head.appendChild(style);
  }

  function patchText(root = document) {
    root.querySelectorAll(".question-meta .chip, .material-box .chip, .reference-box .chip").forEach((node) => {
      const text = String(node.textContent || "").trim();
      const mapped = humanize(text);
      if (mapped !== text) node.textContent = mapped;
    });

    root.querySelectorAll(".mini-card div").forEach((node) => {
      const text = String(node.textContent || "").trim();
      if (/^passed\s*=/.test(text)) {
        node.textContent = `审核结果：${/true/i.test(text) ? "通过" : /false/i.test(text) ? "待优化" : "-"}`;
      } else if (/^score\s*=/.test(text)) {
        node.textContent = text.replace(/^score\s*=/, "参考评分：");
      } else {
        const mapped = humanize(text);
        if (mapped !== text) node.textContent = mapped;
      }
    });

    root.querySelectorAll("strong").forEach((node) => {
      const text = String(node.textContent || "").trim();
      if (text === "LLM 审核") node.textContent = "系统建议";
      if (text === "自动校验") node.textContent = "规则校验";
    });

    root.querySelectorAll(".mini-card, .review-box, .judge-box").forEach((box) => {
      box.querySelectorAll("div, p, span").forEach((node) => {
        const text = String(node.textContent || "").trim();
        if (!text) return;
        const softened = softenAuditText(text);
        if (softened !== text) node.textContent = softened;
      });
    });

    root.querySelectorAll("button, .history-box, .diff-box").forEach((node) => {
      const text = String(node.textContent || "");
      if (text.includes("History")) {
        node.textContent = text.replace(/查看 History/g, "查看版本记录").replace(/History/g, "版本记录");
      }
      if (text.includes("Diff")) {
        node.textContent = text.replace(/查看最新 Diff/g, "查看最新差异").replace(/Diff/g, "差异");
      }
    });

    root.querySelectorAll('textarea[id^="customReplacementMaterial-"]').forEach((node) => {
      node.placeholder = "可选：直接粘贴备用材料。系统会优先使用这里的内容重做。";
    });
  }

  function hasSourceQuestionInput() {
    const ids = ["sourceQuestionPassage", "sourceQuestionStem", "sourceOptionA", "sourceOptionB", "sourceOptionC", "sourceOptionD"];
    return ids.some((id) => {
      const el = document.getElementById(id);
      return !!String(el?.value || "").trim();
    });
  }

  function normalizeSubmissionValues() {
    patchQuestionFocusSelect();
    patchSpecialTypeSelect();
  }

  function patchAll(root = document) {
    ensurePatchStyles();
    patchQuestionFocusSelect();
    patchSpecialTypeSelect();
    patchGenericSelects(root);
    patchText(root);
    patchMaterialParagraphs(root);
    patchBlockedBanner(root);
  }

  document.addEventListener("DOMContentLoaded", () => {
    patchAll();

    const form = document.getElementById("generateForm");
    const questionFocus = document.getElementById("questionFocus");
    if (questionFocus) {
      questionFocus.addEventListener("change", () => {
        setTimeout(() => patchAll(), 0);
      });
    }

    if (form) {
      form.addEventListener(
        "submit",
        (event) => {
          normalizeSubmissionValues();
          const focus = document.getElementById("questionFocus");
          if (!String(focus?.value || "").trim() && !hasSourceQuestionInput()) {
            event.preventDefault();
            event.stopImmediatePropagation();
            alert("请先选择题型，或先提供一整道参考题。");
          }
        },
        true,
      );
    }

    let patching = false;
    let patchQueued = false;
    const observer = new MutationObserver(() => {
      if (patching || patchQueued) return;
      patchQueued = true;
      requestAnimationFrame(() => {
        patchQueued = false;
        patching = true;
        observer.disconnect();
        try {
          patchAll();
        } finally {
          observer.observe(document.body, { childList: true, subtree: true });
          patching = false;
        }
      });
    });
    observer.observe(document.body, { childList: true, subtree: true });
  });
})();
