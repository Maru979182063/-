const QUESTION_FOCUS_OPTIONS = [
  { value: "", label: "不指定" },
  { value: "sentence_order", label: "语句排序题" },
  { value: "sentence_fill", label: "语句填空题" },
  { value: "center_understanding", label: "中心理解题" },
];

const SPECIAL_TYPE_TREE = {
  "": [],
  sentence_order: [
    {
      value: "sentence_order_first_sentence",
      label: "首句合法",
      children: [
        { value: "first_background_intro", label: "背景引入首句" },
        { value: "first_raise_viewpoint", label: "观点起笔首句" },
        { value: "first_definition", label: "定义开篇首句" },
      ],
    },
    {
      value: "sentence_order_fixed_bundle",
      label: "固定捆绑",
      children: [
        { value: "rel_turning", label: "转折捆绑" },
        { value: "rel_parallel", label: "并列捆绑" },
        { value: "pronoun", label: "指代捆绑" },
      ],
    },
    {
      value: "sentence_order_sequence",
      label: "顺序推进",
      children: [
        { value: "daily_time_timeline", label: "时间脉络推进" },
        { value: "daily_action_order", label: "行动顺序推进" },
        { value: "writing_view_explain", label: "观点解释推进" },
        { value: "writing_problem_solution", label: "问题对策推进" },
      ],
    },
    {
      value: "sentence_order_tail_sentence",
      label: "尾句收束",
      children: [
        { value: "tail_conclusion", label: "结论收束" },
        { value: "tail_countermeasure", label: "对策收束" },
      ],
    },
  ],
  sentence_fill: [
    {
      value: "sentence_fill_head_start",
      label: "开头空位",
      children: [
        { value: "opening_summary", label: "开头总起" },
        { value: "opening_topic_intro", label: "开头引入" },
      ],
    },
    {
      value: "sentence_fill_middle",
      label: "中段空位",
      children: [
        { value: "middle_carry_previous", label: "中间承上" },
        { value: "middle_lead_next", label: "中间启下" },
        { value: "middle_bridge_both_sides", label: "承上启下" },
      ],
    },
    {
      value: "sentence_fill_tail_end",
      label: "结尾空位",
      children: [
        { value: "ending_summary", label: "结尾总结" },
        { value: "ending_countermeasure", label: "结尾对策" },
      ],
    },
  ],
  center_understanding: [
    {
      value: "center_understanding_relation_words",
      label: "关系词归旨",
      children: [
        { value: "turning_relation_focus", label: "转折关系聚焦" },
        { value: "cause_effect__conclusion_focus", label: "因果结论聚焦" },
        { value: "necessary_condition_countermeasure", label: "必要条件与对策" },
        { value: "parallel_comprehensive_summary", label: "并列综合概括" },
        { value: "theme_word_focus", label: "主题词聚焦" },
      ],
    },
  ],
};

const EMPTY_SUBTYPE_OPTION = [{ value: "", label: "不指定" }];
const EMPTY_SPECIAL_TYPE_OPTION = [{ value: "", label: "不指定" }];
const NULL_SPECIAL_TYPE_OPTION = [{ value: "", label: "null（暂无可选项）" }];

const DIFFICULTY_OPTIONS = [
  { value: "easy", label: "简单" },
  { value: "medium", label: "中等" },
  { value: "hard", label: "困难" },
];

const VALUE_LABELS = {
  easy: "简单",
  medium: "中等",
  hard: "困难",
  approve: "通过",
  main_idea: "主旨中心类",
  continuation: "接语选择题",
  sentence_order: "语句排序题",
  sentence_fill: "语句填空题",
  sentence_order_first_sentence: "首句合法",
  sentence_order_fixed_bundle: "固定捆绑",
  sentence_order_sequence: "顺序推进",
  sentence_order_tail_sentence: "尾句收束",
  sentence_fill_head_start: "开头空位",
  sentence_fill_middle: "中段空位",
  sentence_fill_tail_end: "结尾空位",
  center_understanding_relation_words: "关系词归旨",
  center_understanding: "中心理解题",
  title_selection: "中心理解",
  turning_relation_focus: "转折关系聚焦",
  cause_effect__conclusion_focus: "因果结论聚焦",
  necessary_condition_countermeasure: "必要条件与对策",
  parallel_comprehensive_summary: "并列综合概括",
  theme_word_focus: "主题词聚焦",
  cu_relation_turning: "转折关系孙族卡",
  cu_relation_parallel: "并列关系孙族卡",
  cu_relation_countermeasure: "对策条件孙族卡",
  cu_relation_plain: "常规中心孙族卡",
  cu_relation_variant: "结论变体孙族卡",
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
  first_background_intro: "背景引入首句",
  first_raise_viewpoint: "观点起笔首句",
  first_definition: "定义开篇首句",
  rel_turning: "转折捆绑",
  rel_parallel: "并列捆绑",
  pronoun: "指代捆绑",
  daily_time_timeline: "时间脉络推进",
  daily_action_order: "行动顺序推进",
  writing_view_explain: "观点解释推进",
  writing_problem_solution: "问题对策推进",
  tail_conclusion: "结论收束",
  tail_countermeasure: "对策收束",
  opening_summary: "开头总起",
  opening_topic_intro: "开头引入",
  middle_carry_previous: "中间承上",
  middle_lead_next: "中间启下",
  middle_bridge_both_sides: "承上启下",
  ending_summary: "结尾总结",
  ending_countermeasure: "结尾对策",
  "sentence_fill.opening.summary.summary": "开头总起",
  "sentence_fill.opening.topic_intro.continuation": "开头引入",
  "sentence_fill.middle.carry_previous.explanation": "中间承上",
  "sentence_fill.middle.lead_next.focus_shift": "中间启下",
  "sentence_fill.middle.bridge.continuation": "承上启下",
  "sentence_fill.ending.conclusion.summary": "结尾总结",
  "sentence_fill.ending.countermeasure.action": "结尾对策",
  "sentence_order.background_intro.mixed_layers.summary": "背景引入首句",
  "sentence_order.viewpoint_opening.cause_effect_chain.summary": "观点解释推进",
  "sentence_order.explicit_topic.local_binding.summary": "定义开篇首句",
  "sentence_order.explicit_topic.local_binding.conclusion": "结论收束",
  "sentence_order.upper_context_link.parallel_expansion.summary": "并列捆绑",
  "sentence_order.upper_context_link.local_binding.summary": "指代捆绑",
  "sentence_order.background_intro.cause_effect_chain.summary": "时间脉络推进",
  "sentence_order.problem_opening.problem_solution_blocks.case_support": "问题对策推进",
  "sentence_order.problem_opening.problem_solution_blocks.call_to_action": "对策收束",
  "center_understanding.relation_words.turning": "转折关系聚焦",
  "center_understanding.relation_words.parallel": "并列综合概括",
  "center_understanding.relation_words.countermeasure": "必要条件与对策",
  "center_understanding.relation_words.variant": "因果结论聚焦",
  approved: "已通过",
  pending_review: "待复核",
  waiting_review: "待复核",
  needs_revision: "需修订",
  rejected: "已拒绝",
  draft: "草稿",
  auto_failed: "自动校验未过",
  discarded: "已丢弃",
  generated: "已生成",
  generate: "生成",
  revising: "修订中",
  success: "成功",
  failed: "失败",
  passed: "通过",
  not_started: "未开始",
  recommended: "推荐保留",
  hold: "继续复核",
  weak_candidate: "弱候选",
  confirm: "通过",
  discard: "丢弃",
  question_modify: "按参数重做",
  text_modify: "替换材料重做",
  manual_edit: "手工编辑",
  distractor_patch: "单错误项修复",
  fine_tune: "精修调整",
  minor_edit: "微调修改",
  source_question: "参考母题",
  forced_user_material: "用户自带材料",
  user_material: "用户材料",
  material_pool_v2: "材料池 V2",
  review_pending: "待复核",
  review_confirmed: "复核通过",
  auto_tagged: "自动标注",
  stable: "稳定池",
  gray: "灰态池",
  promoted: "已晋升",
  role_ambiguity_penalty: "角色歧义惩罚",
  standalone_penalty: "独立成段风险",
  overlong_penalty: "篇幅过长惩罚",
  example_dominance_penalty: "例子压过主旨",
  ambiguity_score: "歧义度",
  complexity_score: "复杂度",
  reasoning_depth_score: "推理深度",
  constraint_intensity_score: "约束强度",
};

const GENERATION_TARGETS = {
  sentence_order: {
    businessSubtype: null,
    leaves: {
      first_background_intro: {
        patternId: null,
        typeSlots: {
          opening_anchor_type: "background_intro",
          middle_structure_type: "mixed_layers",
          closing_anchor_type: "summary",
        },
        referenceBusinessCards: ["sentence_order__head_tail_logic__abstract"],
        guardLines: ["优先判断谁能合法起句，局部顺滑不能代替首句资格。"],
      },
      first_raise_viewpoint: {
        patternId: null,
        typeSlots: {
          opening_anchor_type: "viewpoint_opening",
          middle_structure_type: "cause_effect_chain",
          closing_anchor_type: "summary",
        },
        referenceBusinessCards: ["sentence_order__head_tail_logic__abstract"],
        guardLines: ["首句要直接立起观点，后文按观点后的解释或论证展开。"],
      },
      first_definition: {
        patternId: null,
        typeSlots: {
          opening_anchor_type: "explicit_topic",
          middle_structure_type: "local_binding",
          closing_anchor_type: "summary",
        },
        referenceBusinessCards: ["sentence_order__head_tail_logic__abstract"],
        guardLines: ["首句应承担定义或总起功能，不要把细节句误推到开头。"],
      },
      rel_turning: {
        patternId: "dual_anchor_lock",
        typeSlots: {
          opening_anchor_type: "explicit_topic",
          middle_structure_type: "local_binding",
          closing_anchor_type: "conclusion",
        },
        referenceBusinessCards: ["sentence_order__deterministic_binding__abstract"],
        guardLines: ["优先保住转折前后的小链条，不要为了整体顺滑拆开关键捆绑。"],
      },
      rel_parallel: {
        patternId: "carry_parallel_expand",
        typeSlots: {
          opening_anchor_type: "upper_context_link",
          middle_structure_type: "parallel_expansion",
          closing_anchor_type: "summary",
        },
        referenceBusinessCards: ["sentence_order__deterministic_binding__abstract"],
        guardLines: ["并列叶优先看局部捆绑和并列接口，而不是泛泛的通顺。"],
      },
      pronoun: {
        patternId: null,
        typeSlots: {
          opening_anchor_type: "upper_context_link",
          middle_structure_type: "local_binding",
          closing_anchor_type: "summary",
        },
        referenceBusinessCards: ["sentence_order__deterministic_binding__abstract"],
        guardLines: ["指代叶必须顺着代词回扣锁序，不能断开指称对象。"],
      },
      daily_time_timeline: {
        patternId: "timeline_action_sequence",
        typeSlots: {
          opening_anchor_type: "background_intro",
          middle_structure_type: "cause_effect_chain",
          closing_anchor_type: "summary",
        },
        referenceBusinessCards: ["sentence_order__timeline_action_sequence__abstract"],
        guardLines: ["按时间脉络推进，不要把后发生的句子提前。"],
      },
      daily_action_order: {
        patternId: "timeline_action_sequence",
        typeSlots: {
          opening_anchor_type: "background_intro",
          middle_structure_type: "cause_effect_chain",
          closing_anchor_type: "summary",
        },
        referenceBusinessCards: ["sentence_order__timeline_action_sequence__abstract"],
        guardLines: ["按行动先后推进，不要打乱步骤链。"],
      },
      writing_view_explain: {
        patternId: "viewpoint_reason_action",
        typeSlots: {
          opening_anchor_type: "viewpoint_opening",
          middle_structure_type: "cause_effect_chain",
          closing_anchor_type: "summary",
        },
        referenceBusinessCards: ["sentence_order__discourse_logic__abstract"],
        guardLines: ["按观点-解释的推进关系组织，不要把解释句前置为首句。"],
      },
      writing_problem_solution: {
        patternId: "problem_solution_case_blocks",
        typeSlots: {
          opening_anchor_type: "problem_opening",
          middle_structure_type: "problem_solution_blocks",
          closing_anchor_type: "case_support",
        },
        referenceBusinessCards: ["sentence_order__discourse_logic__abstract"],
        guardLines: ["按问题-对策-案例板块推进，不要把问题块和对策块互换。"],
      },
      tail_conclusion: {
        patternId: null,
        typeSlots: {
          opening_anchor_type: "explicit_topic",
          middle_structure_type: "local_binding",
          closing_anchor_type: "conclusion",
        },
        referenceBusinessCards: ["sentence_order__head_tail_logic__abstract"],
        guardLines: ["尾句必须真正完成结论收束，不能停在中段解释。"],
      },
      tail_countermeasure: {
        patternId: null,
        typeSlots: {
          opening_anchor_type: "problem_opening",
          middle_structure_type: "problem_solution_blocks",
          closing_anchor_type: "call_to_action",
        },
        referenceBusinessCards: ["sentence_order__head_tail_logic__abstract"],
        guardLines: ["尾句必须落在对策或行动，不要把未完成的问题句留到最后。"],
      },
    },
  },
  sentence_fill: {
    businessSubtype: null,
    leaves: {
      opening_summary: {
        patternId: "opening_summary",
        typeSlots: {
          blank_position: "opening",
          function_type: "summary",
          logic_relation: "summary",
        },
        referenceBusinessCards: ["sentence_fill__opening_summary__abstract"],
        guardLines: ["开头总起叶要负责领起全文，不要提前写成结论。"],
      },
      opening_topic_intro: {
        patternId: null,
        typeSlots: {
          blank_position: "opening",
          function_type: "topic_intro",
          logic_relation: "continuation",
        },
        referenceBusinessCards: ["sentence_fill__opening_topic_intro__abstract"],
        guardLines: ["开头引入叶要先把题眼带出来，不要抹平成说明句。"],
      },
      middle_carry_previous: {
        patternId: "middle_explanation",
        typeSlots: {
          blank_position: "middle",
          function_type: "carry_previous",
          logic_relation: "explanation",
        },
        referenceBusinessCards: ["sentence_fill__middle_carry_previous__abstract"],
        guardLines: ["中间承上叶要先回扣前文对象和判断。"],
      },
      middle_lead_next: {
        patternId: "middle_focus_shift",
        typeSlots: {
          blank_position: "middle",
          function_type: "lead_next",
          logic_relation: "focus_shift",
        },
        referenceBusinessCards: ["sentence_fill__middle_lead_next__abstract"],
        guardLines: ["中间启下叶要点亮后文马上展开的对象。"],
      },
      middle_bridge_both_sides: {
        patternId: "bridge_transition",
        typeSlots: {
          blank_position: "middle",
          function_type: "bridge",
          logic_relation: "continuation",
        },
        referenceBusinessCards: ["sentence_fill__middle_bridge_both_sides__abstract"],
        guardLines: ["承上启下叶必须同时咬住左右两侧，不能只顺一边。"],
      },
      ending_summary: {
        patternId: "ending_summary",
        typeSlots: {
          blank_position: "ending",
          function_type: "conclusion",
          logic_relation: "summary",
        },
        referenceBusinessCards: ["sentence_fill__ending_summary__abstract"],
        guardLines: ["结尾总结叶要完成收束，不要新增论点。"],
      },
      ending_countermeasure: {
        patternId: null,
        typeSlots: {
          blank_position: "ending",
          function_type: "countermeasure",
          logic_relation: "action",
        },
        referenceBusinessCards: ["sentence_fill__ending_countermeasure__abstract"],
        guardLines: ["结尾对策叶必须给出同尺度对策，不要把危害续写当对策。"],
      },
    },
  },
  center_understanding: {
    businessSubtype: "center_understanding",
    leaves: {
      turning_relation_focus: {
        patternId: null,
        typeSlots: {
          structure_type: "turning",
          main_point_source: "whole_passage",
          abstraction_level: "medium",
          statement_visibility: "medium",
          main_axis_source: "transition_after",
        },
        referenceBusinessCards: ["turning_relation_focus__main_idea"],
        guardLines: ["正确项要抓转折后的中心判断，不要停在转折前铺垫。"],
      },
      cause_effect__conclusion_focus: {
        patternId: null,
        typeSlots: {
          structure_type: "progressive",
          main_point_source: "conclusion_sentence",
          abstraction_level: "medium",
          statement_visibility: "medium",
          main_axis_source: "final_summary",
        },
        referenceBusinessCards: ["cause_effect__conclusion_focus__main_idea"],
        guardLines: ["正确项要落在因果推出的核心结论，不要把前置原因当主旨。"],
      },
      necessary_condition_countermeasure: {
        patternId: null,
        typeSlots: {
          structure_type: "progressive",
          main_point_source: "whole_passage",
          abstraction_level: "medium",
          statement_visibility: "medium",
          main_axis_source: "solution_conclusion",
        },
        referenceBusinessCards: ["necessary_condition_countermeasure__main_idea"],
        guardLines: ["正确项要落在必要条件或对策结论上，不要只复述问题表层。"],
      },
      parallel_comprehensive_summary: {
        patternId: null,
        typeSlots: {
          structure_type: "contrast",
          main_point_source: "whole_passage",
          abstraction_level: "medium",
          statement_visibility: "medium",
          main_axis_source: "global_abstraction",
        },
        referenceBusinessCards: ["parallel_comprehensive_summary__main_idea"],
        guardLines: ["正确项要整合并列信息，不要只抓其中一个分点。"],
      },
      theme_word_focus: {
        patternId: null,
        typeSlots: {
          structure_type: "explicit_single_center",
          main_point_source: "whole_passage",
          abstraction_level: "medium",
          statement_visibility: "high",
          main_axis_source: "global_abstraction",
        },
        referenceBusinessCards: ["theme_word_focus__main_idea"],
        guardLines: ["正确项要围绕稳定主题词归旨，不要把例子或局部判断拔成主旨。"],
      },
    },
  },
};

const BUSINESS_CARD_TAXONOMY = {
  turning_relation_focus__main_idea: {
    child: "center_understanding_relation_words",
    leaf: "turning_relation_focus",
  },
  cause_effect__conclusion_focus__main_idea: {
    child: "center_understanding_relation_words",
    leaf: "cause_effect__conclusion_focus",
  },
  necessary_condition_countermeasure__main_idea: {
    child: "center_understanding_relation_words",
    leaf: "necessary_condition_countermeasure",
  },
  parallel_comprehensive_summary__main_idea: {
    child: "center_understanding_relation_words",
    leaf: "parallel_comprehensive_summary",
  },
  theme_word_focus__main_idea: {
    child: "center_understanding_relation_words",
    leaf: "theme_word_focus",
  },
  sentence_fill__opening_summary__abstract: {
    child: "sentence_fill_head_start",
    leaf: "opening_summary",
  },
  sentence_fill__opening_topic_intro__abstract: {
    child: "sentence_fill_head_start",
    leaf: "opening_topic_intro",
  },
  sentence_fill__middle_carry_previous__abstract: {
    child: "sentence_fill_middle",
    leaf: "middle_carry_previous",
  },
  sentence_fill__middle_lead_next__abstract: {
    child: "sentence_fill_middle",
    leaf: "middle_lead_next",
  },
  sentence_fill__middle_bridge_both_sides__abstract: {
    child: "sentence_fill_middle",
    leaf: "middle_bridge_both_sides",
  },
  sentence_fill__ending_summary__abstract: {
    child: "sentence_fill_tail_end",
    leaf: "ending_summary",
  },
  sentence_fill__ending_countermeasure__abstract: {
    child: "sentence_fill_tail_end",
    leaf: "ending_countermeasure",
  },
  sentence_order__head_tail_logic__abstract: {
    child: "sentence_order_first_sentence",
  },
  sentence_order__deterministic_binding__abstract: {
    child: "sentence_order_fixed_bundle",
  },
  sentence_order__discourse_logic__abstract: {
    child: "sentence_order_sequence",
  },
  sentence_order__timeline_action_sequence__abstract: {
    child: "sentence_order_sequence",
  },
};

const DECISION_REASON_LABELS = {
  recommended_stable_candidate: "候选稳定，可直接保留",
  recommended_candidate_requires_review: "候选质量高，但仍建议复核",
  hard_but_currently_weak_candidate: "难度不低，但当前质量仍偏弱",
  high_readiness_high_penalty: "可用度较高，但风险惩罚偏高",
  high_risk_but_not_high_difficulty: "风险偏高，但并不是高难候选",
  easy_but_weak_candidate: "并非高难，只是当前质量偏弱",
  borderline_hold_candidate: "边界候选，建议继续复核",
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
  not_hard_but_currently_weak_candidate: "并非高难，是当前质量偏弱",
};

const CHECK_NAME_LABELS = {
  main_idea_material_scoring_available: "主旨题材料评分尚未就绪",
  sentence_order_material_scoring_available: "排序题材料评分尚未就绪",
  sentence_fill_material_scoring_available: "填空题材料评分尚未就绪",
  continuation_material_scoring_available: "接语题材料评分尚未就绪",
  center_understanding_material_scoring_available: "中心理解题材料评分尚未就绪",
  min_final_candidate_score: "最终得分下限",
};

const PHRASE_LABELS = {
  "llm judge overall score too low": "模型综合评分过低",
  "llm judge material alignment too low": "材料匹配度评分过低",
  "llm judge answer analysis consistency too low": "答案与解析一致性评分过低",
  "difficulty projection is outside the target profile range.": "难度投射超出目标档位范围。",
  "material scoring payload is missing, so validator could not enforce main idea scoring controls.": "材料评分载荷缺失，暂时无法执行主旨题评分约束。",
  "material scoring payload is missing, so validator could not enforce sentence order scoring controls.": "材料评分载荷缺失，暂时无法执行排序题评分约束。",
  "material scoring payload is missing, so validator could not enforce sentence fill scoring controls.": "材料评分载荷缺失，暂时无法执行填空题评分约束。",
  "material scoring payload is missing, so validator could not enforce continuation scoring controls.": "材料评分载荷缺失，暂时无法执行接语题评分约束。",
  "material scoring payload is missing, so validator could not enforce center understanding scoring controls.": "材料评分载荷缺失，暂时无法执行中心理解题评分约束。",
};

const TOKEN_LABELS = {
  main: "主",
  idea: "旨",
  continuation: "接语",
  sentence: "语句",
  fill: "填空",
  order: "排序",
  center: "中心",
  understanding: "理解",
  title: "标题",
  selection: "选择",
  turning: "转折",
  relation: "关系",
  cause: "因果",
  effect: "结果",
  conclusion: "结论",
  necessary: "必要",
  condition: "条件",
  countermeasure: "对策",
  theme: "主题",
  word: "词",
  local: "局部",
  paragraph: "段意",
  plot: "情节",
  character: "人物",
  extension: "延展",
  foreshadow: "伏笔",
  recall: "呼应",
  setting: "环境",
  alignment: "匹配",
  emotion: "情绪",
  progression: "递进",
  conflict: "冲突",
  resolution: "解决",
  value: "值",
  expression: "表达",
  head: "首",
  tail: "尾",
  lock: "锁定",
  deterministic: "确定性",
  binding: "捆绑",
  discourse: "行文",
  logic: "逻辑",
  timeline: "时间线",
  action: "动作",
  carry: "承接",
  parallel: "并列",
  expand: "展开",
  viewpoint: "观点",
  reason: "原因",
  problem: "问题",
  solution: "对策",
  case: "案例",
  blocks: "块",
  opening: "开头",
  summary: "总结",
  intro: "引入",
  middle: "中间",
  bridge: "衔接",
  both: "两侧",
  sides: "两侧",
  ending: "结尾",
  easy: "简单",
  medium: "中等",
  hard: "困难",
  approved: "已通过",
  pending: "待处理",
  review: "复核",
  auto: "自动",
  failed: "失败",
  discarded: "已丢弃",
  generated: "已生成",
  generate: "生成",
  revising: "修订中",
  recommended: "推荐",
  hold: "保留待审",
  weak: "偏弱",
  candidate: "候选",
  confirm: "确认",
  discard: "丢弃",
  distractor: "错误项",
  patch: "修复",
  question: "题目",
  text: "文本",
  modify: "修改",
  manual: "手工",
  fine: "精细",
  tune: "调整",
  minor: "微调",
  source: "来源",
  forced: "强制",
  user: "用户",
  material: "材料",
  pool: "池",
  tagged: "标注",
  confirmed: "确认",
  stable: "稳定",
  gray: "灰态",
  promoted: "晋升",
  ambiguity: "歧义",
  complexity: "复杂度",
  reasoning: "推理",
  depth: "深度",
  constraint: "约束",
  intensity: "强度",
  final: "最终",
  score: "得分",
  readiness: "可用性",
  total: "总",
  penalty: "处罚分",
  penalties: "处罚分",
  difficulty: "难度",
  band: "档位",
  hint: "提示",
  vector: "向量",
  dimensions: "维度",
  dimension: "维度",
  validator: "校验器",
  validation: "校验",
  errors: "错误",
  warnings: "警告",
  current: "当前",
  latest: "最新",
  status: "状态",
  replacement: "备选",
  custom: "自定义",
  distractor: "干扰项",
  strategy: "策略",
  mild: "偏弱",
  strong: "偏强",
  target: "目标",
  adjustment: "调整",
  scope: "范围",
  only: "仅",
  stem: "题干",
  and: "与",
  full: "整体",
  keep: "保持",
  correct: "正确",
  answer: "答案",
  fixed: "固定",
  overall: "整体",
  too: "过于",
  low: "低",
  missing: "缺失",
  payload: "载荷",
  available: "可用",
  started: "开始",
  not: "未",
  true: "是",
  false: "否",
  name: "名称",
  caution: "提示",
  tag: "标签",
  v2: "V2",
  llm: "模型",
  judge: "判定",
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
  selectedReplacementByItem: {},
  loadingStep: "collect",
  approvedHistory: [],
  approvedHistoryTotal: 0,
  approvedHistoryLoaded: false,
  approvedHistoryLoading: false,
  approvedPreviewItemId: null,
  approvedPreviewItem: null,
  approvedPreviewLoading: false,
  historyByItem: {},
  historyLoadingByItem: {},
  selectedVersionByItem: {},
  overlayMode: null,
  overlayContextItemId: null,
};

const QUESTION_MODIFY_MAX_CHANGES = 2;

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
  if (dictionary[key]) return dictionary[key];
  return localizeMachineValue(key);
}

function humanizeCode(value, dictionary = {}) {
  const key = String(value == null ? "" : value).trim();
  if (!key) return "未提供";
  if (dictionary[key]) return dictionary[key];
  return localizeMachineValue(key);
}

function fieldLabel(key) {
  return FIELD_LABELS[key] || localizeMachineValue(key, { fallback: "未识别字段" });
}

function localizeMachineValue(value, options = {}) {
  const fallback = options.fallback || "未识别项";
  const key = String(value == null ? "" : value).trim();
  if (!key) return "未提供";
  if (VALUE_LABELS[key]) return VALUE_LABELS[key];
  if (CHECK_NAME_LABELS[key]) return CHECK_NAME_LABELS[key];
  if (PHRASE_LABELS[key.toLowerCase()]) return PHRASE_LABELS[key.toLowerCase()];
  if (/[\u4e00-\u9fff]/.test(key)) return key;

  const normalized = key
    .replace(/([a-z])([A-Z])/g, "$1 $2")
    .replace(/[_\-\/]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();

  const exactPhrase = PHRASE_LABELS[normalized.toLowerCase()];
  if (exactPhrase) return exactPhrase;

  const translated = normalized
    .split(" ")
    .map((part) => TOKEN_LABELS[part.toLowerCase()] || part.toUpperCase())
    .join(" ")
    .replace(/\s+/g, " ")
    .trim();

  if (!translated) return fallback;
  if (/^[A-Z0-9 _-]+$/.test(translated)) return fallback;
  if (/^[A-Za-z0-9 _-]+$/.test(translated) && !/V\d+/i.test(translated)) return fallback;
  return translated;
}

function safeFloat(value) {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : null;
}

function formatScore(value) {
  const numeric = safeFloat(value);
  return numeric == null ? "-" : numeric.toFixed(4);
}

function isMissingScoringFeedback(feedback) {
  return Boolean(feedback && feedback.decision_reason === "material_scoring_missing");
}

function cleanDisplayText(value, options = {}) {
  const replaceBlankToken = options.replaceBlankToken !== false;
  let text = String(value == null ? "" : value);
  if (!text.trim()) return "";

  text = text.replace(/\r\n?/g, "\n").replace(/\u00a0/g, " ");
  if (replaceBlankToken) {
    text = text.replace(/\[BLANK\]/g, "____");
  }
  text = text.replace(/[ \t]+\n/g, "\n");
  text = text.replace(/\n{3,}/g, "\n\n");
  text = text.replace(/[ \t]{2,}/g, " ");
  text = text.replace(/\s+([，。！？；：、）】》])/g, "$1");
  text = text.replace(/([（【《“‘])\s+/g, "$1");

  let previous = "";
  while (previous !== text) {
    previous = text;
    text = text.replace(/([\u4e00-\u9fff])\s+([\u4e00-\u9fff])/g, "$1$2");
  }

  return text.trim();
}

function shortText(value, limit = 56) {
  const text = cleanDisplayText(value, { replaceBlankToken: false }).replace(/\n+/g, " ");
  if (!text) return "未提供";
  return text.length <= limit ? text : `${text.slice(0, Math.max(0, limit - 1))}…`;
}

function formatDateTime(value) {
  const text = String(value || "").trim();
  if (!text) return "未提供";
  const date = new Date(text);
  if (Number.isNaN(date.getTime())) return text;
  return new Intl.DateTimeFormat("zh-CN", {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  }).format(date);
}

const CIRCLED_INDEX_MARKERS = ["①", "②", "③", "④", "⑤", "⑥", "⑦", "⑧", "⑨", "⑩"];

function getRuntimeMaterialSnapshot(itemLike) {
  return itemLike?.runtime_snapshot?.material_snapshot || {};
}

function getGeneratedQuestion(itemLike) {
  return itemLike?.generated_question || {};
}

function getQuestionType(itemLike) {
  return String(itemLike?.question_type || itemLike?.item?.question_type || getGeneratedQuestion(itemLike)?.question_type || "").trim();
}

function getPromptExtras(itemLike) {
  const materialSource = getMaterialSource(itemLike);
  const promptExtras = materialSource?.prompt_extras;
  return promptExtras && typeof promptExtras === "object" ? promptExtras : {};
}

function rawMaterialFullText(itemLike) {
  const materialSnapshot = getRuntimeMaterialSnapshot(itemLike);
  return cleanDisplayText(
    itemLike?.material_text ||
      itemLike?.material_selection?.text ||
      materialSnapshot?.source?.text ||
      materialSnapshot?.original_text ||
      materialSnapshot?.preview ||
      "",
    { replaceBlankToken: false },
  );
}

function normalizeTextArray(value) {
  if (!Array.isArray(value)) return [];
  return value.map((entry) => cleanDisplayText(entry, { replaceBlankToken: false })).filter(Boolean);
}

function stripOrderedUnitPrefix(value) {
  return cleanDisplayText(value, { replaceBlankToken: false })
    .replace(/^\s*[①②③④⑤⑥⑦⑧⑨⑩]\s*/, "")
    .replace(/^\s*\(?\d+\)?[.、．]?\s*/, "")
    .trim();
}

function parseSortableUnitsFromText(value) {
  const text = cleanDisplayText(value, { replaceBlankToken: false });
  if (!text) return [];
  return text
    .split(/\n+/)
    .map((line) => stripOrderedUnitPrefix(line))
    .filter(Boolean);
}

function extractSentenceOrderUnits(itemLike) {
  const generated = getGeneratedQuestion(itemLike);
  const promptExtras = getPromptExtras(itemLike);
  const modelOutput = itemLike?.runtime_snapshot?.model_output_snapshot?.parsed_structured_output || {};
  const directUnits = normalizeTextArray(
    generated?.original_sentences ||
      itemLike?.original_sentences ||
      modelOutput?.original_sentences,
  );
  if (directUnits.length) {
    return directUnits;
  }
  const promptUnits = normalizeTextArray(promptExtras?.sortable_units);
  if (promptUnits.length) {
    return promptUnits;
  }
  return parseSortableUnitsFromText(promptExtras?.sortable_material_text || "");
}

function formatSortableUnits(units) {
  return units
    .map((unit, index) => `${CIRCLED_INDEX_MARKERS[index] || `${index + 1}.`} ${stripOrderedUnitPrefix(unit)}`)
    .join("\n");
}

function resolvePrimaryContent(itemLike) {
  const questionType = getQuestionType(itemLike);
  const promptExtras = getPromptExtras(itemLike);

  if (questionType === "sentence_order") {
    const units = extractSentenceOrderUnits(itemLike);
    if (units.length) {
      const text = formatSortableUnits(units);
      return { label: "排序原句", text, preview: shortText(text, 64) };
    }
  }

  if (questionType === "sentence_fill") {
    const fillDisplay = cleanDisplayText(
      promptExtras?.fill_ready_material || promptExtras?.fill_ready_local_material || rawMaterialFullText(itemLike),
    );
    if (fillDisplay) {
      return { label: "挖空材料", text: fillDisplay, preview: shortText(fillDisplay, 64) };
    }
  }

  const fallback = rawMaterialFullText(itemLike);
  return { label: "材料", text: fallback, preview: shortText(fallback, 64) };
}

function materialPreviewText(itemLike) {
  const primaryContent = resolvePrimaryContent(itemLike);
  if (primaryContent.preview && primaryContent.preview !== "未提供") {
    return primaryContent.preview;
  }
  return shortText(itemLike?.material_preview || rawMaterialFullText(itemLike), 64);
}

function materialFullText(itemLike) {
  return resolvePrimaryContent(itemLike).text || "";
}

function primaryContentLabel(itemLike) {
  return resolvePrimaryContent(itemLike).label || "材料";
}

function stemFullText(itemLike) {
  return cleanDisplayText(itemLike?.stem || itemLike?.generated_question?.stem || itemLike?.stem_text || "");
}

function answerText(itemLike) {
  return String(itemLike?.answer || itemLike?.generated_question?.answer || "").trim();
}

function analysisFullText(itemLike) {
  return cleanDisplayText(itemLike?.analysis || itemLike?.generated_question?.analysis || "");
}

function optionsMap(itemLike) {
  const options =
    (itemLike && typeof itemLike.options === "object" && itemLike.options) ||
    (itemLike?.generated_question && typeof itemLike.generated_question.options === "object" && itemLike.generated_question.options) ||
    {};
  return ["A", "B", "C", "D"].reduce((acc, key) => {
    if (key in options) acc[key] = cleanDisplayText(options[key] || "");
    return acc;
  }, {});
}

function resolveCategoryLabel(itemLike) {
  const questionType = humanize(itemLike?.question_type || itemLike?.item?.question_type);
  const subtypeRaw =
    itemLike?.business_subtype ||
    itemLike?.item?.business_subtype ||
    itemLike?.generated_question?.business_subtype ||
    itemLike?.item?.generated_question?.business_subtype ||
    resolveDisplayedSubtype(itemLike);
  const subtype = subtypeRaw ? humanize(subtypeRaw) : "";
  return subtype && subtype !== "未提供" ? `${questionType} / ${subtype}` : questionType;
}

function resolveVersionStatus(version, historyPayload) {
  if (!version) return "pending_review";
  if (Number(version.version_no) === Number(historyPayload?.current_version_no || 0) && historyPayload?.item?.current_status) {
    return historyPayload.item.current_status;
  }
  return version.current_status || "pending_review";
}

function sanitizeFileName(value) {
  return String(value || "题目")
    .replace(/[\\/:*?"<>|]+/g, "_")
    .replace(/\s+/g, "_")
    .slice(0, 48);
}

function triggerDownload(text, fileName, mimeType = "text/markdown;charset=utf-8") {
  const blob = new Blob([text], { type: mimeType });
  const href = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = href;
  link.download = fileName;
  document.body.appendChild(link);
  link.click();
  link.remove();
  URL.revokeObjectURL(href);
}

function extractPatternFromBusinessCard(cardId) {
  const raw = String(cardId || "").trim();
  if (!raw || !raw.includes("__")) return null;
  const parts = raw.split("__").filter(Boolean);
  if (parts.length < 2) return null;
  return parts[1] || null;
}

function resolveDisplayedSubtype(item) {
  const actual = resolveActualTaxonomy(item);
  if (actual.leaf && actual.leaf !== "未提供") return actual.leaf;
  if (actual.child && actual.child !== "未提供") return actual.child;

  const requested = resolveRequestedTaxonomy(item);
  if (requested.leaf && requested.leaf !== "未提供") return requested.leaf;
  if (requested.child && requested.child !== "未提供") return requested.child;

  const materialSource = getMaterialSource(item);
  const promptLabel = String(getPromptExtras(item)?.business_feature_card_label || "").trim();
  if (promptLabel) {
    return promptLabel.replace(/^语句填空-/, "").replace(/^接语选择-/, "").replace(/^语句排序-/, "").trim();
  }

  const businessCardPattern = extractPatternFromBusinessCard(materialSource?.selected_business_card);
  if (businessCardPattern) {
    return businessCardPattern;
  }

  return item.business_subtype || item.pattern_id || item.selected_pattern || "未提供";
}

function resolveRequestedTaxonomy(item) {
  const extraConstraints = item?.request_snapshot?.extra_constraints || item?.extra_constraints || {};
  return {
    child: humanize(extraConstraints.requested_child_family || extraConstraints.requested_child_label || ""),
    leaf: humanize(extraConstraints.requested_leaf_id || extraConstraints.requested_leaf_label || ""),
  };
}

function resolveActualTaxonomy(item) {
  const materialSource = getMaterialSource(item);
  const promptExtras = getPromptExtras(item);
  const overlay =
    item?.distill_runtime_overlay ||
    item?.request_snapshot?.distill_runtime_overlay ||
    materialSource?.distill_runtime_overlay ||
    {};
  const selectedBusinessCard = String(materialSource?.selected_business_card || "").trim();
  const taxonomyFromCard = BUSINESS_CARD_TAXONOMY[selectedBusinessCard] || {};
  const promptLabel = String(promptExtras?.business_feature_card_label || "").trim();
  const normalizedPromptLeaf = promptLabel.replace(/^语句填空-/, "").replace(/^接语选择-/, "").replace(/^语句排序-/, "").trim();
  const childRaw =
    overlay.child_family_id ||
    promptExtras.distill_child_family_id ||
    taxonomyFromCard.child ||
    "";
  const structuredLeafRaw =
    overlay.leaf_key ||
    promptExtras.distill_leaf_key ||
    promptExtras.hard_logic_leaf_key ||
    "";
  const leafRaw =
    (structuredLeafRaw && (VALUE_LABELS[structuredLeafRaw] || /[\u4e00-\u9fff]/.test(structuredLeafRaw))
      ? structuredLeafRaw
      : "") ||
    taxonomyFromCard.leaf ||
    normalizedPromptLeaf ||
    structuredLeafRaw ||
    "";
  return {
    child: humanize(childRaw),
    leaf: humanize(leafRaw),
  };
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
  if (/distractor_patch only accepts a non-answer option as target_option/i.test(text)) {
    return "当前选中的目标是正确项，正确项已锁定，不能作为错误项修复。";
  }
  if (/distractor_patch did not produce a scoped change/i.test(text)) {
    return "这次没有形成有效的错误项变更，请先调整该错误项控件或文本。";
  }
  if (/distractor_patch requires at least one patch input/i.test(text)) {
    return "请至少调整迷惑方式、迷惑强度，或直接修改该错误项文本。";
  }
  if (/question_modify accepts at most two type_slot updates per request/i.test(text)) {
    return "题目级修改一次最多调整 2 项参数。";
  }
  if (/question_modify multi-select controls accept at most two values/i.test(text)) {
    return "当前多选字段最多只能选 2 个选项。";
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
  return localizeDisplayMessage(text);
}

function localizeDisplayMessage(message) {
  const text = String(message == null ? "" : message).trim();
  if (!text) return "未提供";
  const lower = text.toLowerCase();
  if (PHRASE_LABELS[lower]) return PHRASE_LABELS[lower];
  if (/^llm_judge_.+_too_low$/i.test(text)) {
    return localizeMachineValue(text);
  }
  return text
    .split(/\n+/)
    .map((line) => {
      const trimmed = line.trim();
      if (!trimmed) return "";
      const phraseHit = PHRASE_LABELS[trimmed.toLowerCase()];
      if (phraseHit) return phraseHit;
      if (/^[A-Za-z0-9_\-./ ]+$/.test(trimmed)) {
        return localizeMachineValue(trimmed, { fallback: "未识别提示" });
      }
      return trimmed
        .replace(/main_idea/gi, "主旨题")
        .replace(/sentence_order/gi, "排序题")
        .replace(/sentence_fill/gi, "填空题")
        .replace(/center_understanding/gi, "中心理解题")
        .replace(/continuation/gi, "接语题")
        .replace(/difficulty projection/gi, "难度投射")
        .replace(/target profile range/gi, "目标档位范围")
        .replace(/validator/gi, "校验器")
        .replace(/review/gi, "复核")
        .replace(/score/gi, "得分")
        .replace(/material/gi, "材料")
        .replace(/alignment/gi, "匹配度")
        .replace(/consistency/gi, "一致性");
    })
    .join("\n");
}

function localizeCheckDetailLabel(key) {
  return {
    actual: "实际值",
    threshold: "阈值",
    allowed: "允许范围",
    band: "难度档位",
  }[key] || localizeMachineValue(key, { fallback: "说明" });
}

function formatCheckDetailValue(value) {
  if (typeof value === "boolean") return value ? "是" : "否";
  if (typeof value === "string") return localizeDisplayMessage(value);
  const serialized = JSON.stringify(value);
  return String(serialized == null ? value : serialized).replace(/\btrue\b/g, "是").replace(/\bfalse\b/g, "否");
}

function ensureHistoryOverlay() {
  let root = $("historyOverlay");
  if (root) return root;
  root = document.createElement("div");
  root.id = "historyOverlay";
  root.className = "overlay-root";
  root.hidden = true;
  root.innerHTML = `
    <div class="overlay-backdrop" data-overlay-close="true"></div>
    <div class="overlay-shell">
      <div class="overlay-header">
        <div>
          <h2 id="historyOverlayTitle">历史记录</h2>
          <p id="historyOverlaySubtitle">正在加载…</p>
        </div>
        <button id="historyOverlayCloseBtn" type="button" class="ghost-btn overlay-close-btn">关闭</button>
      </div>
      <div id="historyOverlayBody" class="overlay-body"></div>
    </div>
  `;
  document.body.appendChild(root);
  return root;
}

function ensureBuilderHistoryEntry() {
  const heroCard = document.querySelector("#builderScreen .hero-card");
  if (!heroCard || heroCard.querySelector('[data-action="open-approved-history"]')) return;
  const titleNode = heroCard.querySelector("h1");
  const subtitleNode = heroCard.querySelector(".screen-subtitle");
  const formNode = heroCard.querySelector("#generateForm");
  if (!titleNode || !subtitleNode || !formNode) return;
  const topbar = document.createElement("div");
  topbar.className = "screen-topbar";
  topbar.innerHTML = `
    <div class="screen-topbar-copy"></div>
    <div class="screen-topbar-actions">
      <button type="button" class="secondary-btn history-trigger-btn" data-action="open-approved-history">已确认历史</button>
    </div>
  `;
  topbar.querySelector(".screen-topbar-copy").append(titleNode, subtitleNode);
  heroCard.insertBefore(topbar, formNode);
}

function ensureResultHistoryEntry() {
  const actionRoot = document.querySelector("#resultScreen .result-header-actions");
  if (!actionRoot || actionRoot.querySelector('[data-action="open-approved-history"]')) return;
  const button = document.createElement("button");
  button.type = "button";
  button.className = "secondary-btn history-trigger-btn";
  button.dataset.action = "open-approved-history";
  button.textContent = "已确认历史";
  actionRoot.insertBefore(button, actionRoot.firstChild);
}

function openHistoryOverlay() {
  ensureHistoryOverlay().hidden = false;
}

function closeHistoryOverlay() {
  state.overlayMode = null;
  state.overlayContextItemId = null;
  ensureHistoryOverlay().hidden = true;
}

function renderOptionsPreview(options) {
  const normalized = optionsMap({ options });
  const keys = Object.keys(normalized);
  if (!keys.length) {
    return '<div class="history-empty">当前版本未提供选项。</div>';
  }
  return `
    <div class="history-options">
      ${keys
        .map(
          (key) => `
            <div class="history-option">
              <strong>选项 ${escapeHtml(key)}</strong>
              <div>${escapeHtml(normalized[key] || "未提供")}</div>
            </div>
          `,
        )
        .join("")}
    </div>
  `;
}

function buildSingleItemMarkdown(item) {
  const category = resolveCategoryLabel(item);
  const material = materialFullText(item) || "未提供";
  const materialLabel = primaryContentLabel(item);
  const stem = stemFullText(item) || "未提供";
  const answer = answerText(item) || "未提供";
  const analysis = analysisFullText(item) || "未提供";
  const options = optionsMap(item);
  const lines = [
    `# ${category}`,
    "",
    `- Item ID: ${item.item_id || "unknown"}`,
    `- Status: ${humanize(item.current_status || "approved")}`,
    `- Updated At: ${item.updated_at || item.latest_action_at || ""}`,
    "",
    `## ${materialLabel}`,
    material,
    "",
    "## Question",
    stem,
    "",
    ...Object.entries(options).map(([key, value]) => `- ${key}. ${value}`),
    "",
    `Answer: ${answer}`,
    `Analysis: ${analysis}`,
    "",
  ];
  return lines.join("\n");
}

async function downloadApprovedHistoryItem(itemId) {
  const payload = await apiFetch(`/api/v1/questions/${itemId}/download`, {
    method: "POST",
    body: JSON.stringify({
      operator: "demo",
      channel: "approved_history",
      export_format: "markdown",
      metadata: { source: "approved_history_overlay" },
    }),
  });
  const item = payload?.item;
  const fileName = `${sanitizeFileName(resolveCategoryLabel(item))}_${itemId}.md`;
  triggerDownload(buildSingleItemMarkdown(item), fileName);
}

async function loadApprovedHistory() {
  if (state.approvedHistoryLoading) return;
  state.approvedHistoryLoading = true;
  renderHistoryOverlay();
  try {
    const payload = await apiFetch("/api/v1/review/items?status=approved&page=1&page_size=100");
    state.approvedHistory = payload.items || [];
    state.approvedHistoryTotal = Number(payload.count || state.approvedHistory.length || 0);
    state.approvedHistoryLoaded = true;
    if (state.approvedPreviewItemId && !state.approvedHistory.some((item) => item.item_id === state.approvedPreviewItemId)) {
      state.approvedPreviewItemId = "";
      state.approvedPreviewItem = null;
    }
    if (!state.approvedPreviewItemId && state.approvedHistory.length) {
      state.approvedPreviewItemId = state.approvedHistory[0].item_id;
    }
    if (state.approvedPreviewItemId) {
      await loadApprovedPreview(state.approvedPreviewItemId);
      return;
    }
    renderHistoryOverlay();
  } finally {
    state.approvedHistoryLoading = false;
    renderHistoryOverlay();
  }
}

async function loadApprovedPreview(itemId) {
  if (!itemId) return;
  state.approvedPreviewItemId = itemId;
  state.approvedPreviewLoading = true;
  renderHistoryOverlay();
  try {
    state.approvedPreviewItem = await apiFetch(`/api/v1/questions/${itemId}`);
  } finally {
    state.approvedPreviewLoading = false;
    renderHistoryOverlay();
  }
}

async function openApprovedHistory() {
  state.overlayMode = "approved";
  state.overlayContextItemId = null;
  openHistoryOverlay();
  if (!state.approvedHistoryLoaded) {
    await loadApprovedHistory();
    return;
  }
  if (state.approvedPreviewItemId && (!state.approvedPreviewItem || state.approvedPreviewItem.item_id !== state.approvedPreviewItemId)) {
    await loadApprovedPreview(state.approvedPreviewItemId);
    return;
  }
  renderHistoryOverlay();
}

async function loadItemVersionHistory(itemId) {
  if (!itemId) return;
  state.historyLoadingByItem[itemId] = true;
  renderHistoryOverlay();
  try {
    const payload = await apiFetch(`/api/v1/review/items/${itemId}/history`);
    state.historyByItem[itemId] = payload;
    state.selectedVersionByItem[itemId] = state.selectedVersionByItem[itemId] || payload.current_version_no;
  } finally {
    state.historyLoadingByItem[itemId] = false;
    renderHistoryOverlay();
  }
}

async function openItemVersionHistory(itemId) {
  state.overlayMode = "versions";
  state.overlayContextItemId = itemId;
  openHistoryOverlay();
  if (!state.historyByItem[itemId]) {
    await loadItemVersionHistory(itemId);
    return;
  }
  renderHistoryOverlay();
}

function renderApprovedHistoryBody() {
  if (state.approvedHistoryLoading && !state.approvedHistory.length) {
    return '<div class="history-loading">正在加载已确认题目…</div>';
  }
  if (!state.approvedHistory.length) {
    return '<div class="history-empty">还没有已确认题目。后面只要点了“确认通过”，这里就会自动累计下来。</div>';
  }
  const previewItem = state.approvedPreviewItem;
  const selectedId = state.approvedPreviewItemId;
  const previewMarkup =
    state.approvedPreviewLoading && (!previewItem || previewItem.item_id !== selectedId)
      ? '<div class="history-loading">正在加载题目详情…</div>'
      : previewItem
        ? `
          <div class="history-detail">
            <div class="history-detail-card">
              <div class="history-detail-head">
                <div>
                  <h3>${escapeHtml(resolveCategoryLabel(previewItem))}</h3>
                  <p class="history-detail-subtitle">最近更新时间：${escapeHtml(formatDateTime(previewItem.updated_at || previewItem.latest_action_at))}</p>
                </div>
                <div class="history-actions">
                  <button type="button" class="secondary-btn" data-action="open-version-history" data-item-id="${escapeHtml(previewItem.item_id)}">查看修题版本</button>
                  <button type="button" class="primary-btn" data-action="download-approved-item" data-item-id="${escapeHtml(previewItem.item_id)}">下载这道题</button>
                </div>
              </div>
            </div>
            <div class="history-detail-card">
              <div class="passage-label">${escapeHtml(primaryContentLabel(previewItem))}</div>
              <pre class="compact-pre">${escapeHtml(materialFullText(previewItem) || "未提供")}</pre>
            </div>
            <div class="history-detail-card">
              <div class="passage-label">题干</div>
              <div class="question-stem">${escapeHtml(stemFullText(previewItem) || "未提供")}</div>
            </div>
            <div class="history-detail-card">
              <div class="passage-label">选项</div>
              ${renderOptionsPreview(optionsMap(previewItem))}
              <div class="answer-row" style="margin-top: 14px;"><strong>答案：</strong>${escapeHtml(answerText(previewItem) || "未提供")}</div>
              <div class="analysis-row" style="margin-top: 10px;"><strong>解析：</strong>${escapeHtml(analysisFullText(previewItem) || "未提供")}</div>
            </div>
          </div>
        `
        : '<div class="history-empty">左侧选一条已确认题，就能在这里直接预览并下载。</div>';
  return `
    <div class="history-layout">
      <div class="history-column">
        <div class="history-list">
          ${state.approvedHistory
            .map((item) => {
              const activeClass = item.item_id === selectedId ? "is-active" : "";
              return `
                <button
                  type="button"
                  class="history-card ${activeClass}"
                  data-action="select-approved-history"
                  data-item-id="${escapeHtml(item.item_id)}"
                >
                  <div class="history-card-head">
                    <div class="history-card-title">${escapeHtml(resolveCategoryLabel(item))}</div>
                    <span class="chip status status-approved">${escapeHtml(humanize(item.current_status || "approved"))}</span>
                  </div>
                  <div class="history-card-meta">${escapeHtml(formatDateTime(item.updated_at || item.latest_action_at || item.created_at))}</div>
                  <div class="history-card-preview">${escapeHtml(materialPreviewText(item))}</div>
                </button>
              `;
            })
            .join("")}
        </div>
      </div>
      <div class="history-column">${previewMarkup}</div>
    </div>
  `;
}

function renderVersionHistoryBody() {
  const itemId = state.overlayContextItemId;
  const payload = state.historyByItem[itemId];
  if (state.historyLoadingByItem[itemId] && !payload) {
    return '<div class="history-loading">正在加载版本历史…</div>';
  }
  if (!payload) {
    return '<div class="history-empty">当前题目还没有可展示的版本历史。</div>';
  }
  const versions = Array.isArray(payload.versions) ? payload.versions : [];
  const selectedVersionNo = Number(state.selectedVersionByItem[itemId] || payload.current_version_no || versions[0]?.version_no || 0);
  const selectedVersion =
    versions.find((version) => Number(version.version_no) === selectedVersionNo) || payload.current_version || versions[0] || null;
  const selectedVersionStatus = resolveVersionStatus(selectedVersion, payload);
  const actionTrail = (payload.review_actions || [])
    .slice(0, 6)
    .map((action) => `<span class="chip">${escapeHtml(humanize(action.action_type || action.requested_action || "generate"))}</span>`)
    .join("");
  const detailMarkup = selectedVersion
    ? `
      <div class="history-detail">
        <div class="history-detail-card">
          <div class="history-detail-head">
            <div>
              <h3>v${escapeHtml(selectedVersion.version_no)} · ${escapeHtml(humanize(selectedVersion.source_action || "generate"))}</h3>
              <p class="history-detail-subtitle">生成时间：${escapeHtml(formatDateTime(selectedVersion.created_at))}</p>
            </div>
            <div class="history-actions">
              ${payload.item?.current_status === "approved" ? `<button type="button" class="primary-btn" data-action="download-approved-item" data-item-id="${escapeHtml(itemId)}">下载当前题</button>` : ""}
            </div>
          </div>
          <div class="history-card-tags" style="margin-top: 14px;">
            <span class="chip">${escapeHtml(resolveCategoryLabel(payload.item || {}))}</span>
            <span class="chip">${escapeHtml(humanize(selectedVersionStatus))}</span>
            ${selectedVersion.target_difficulty ? `<span class="chip">${escapeHtml(humanize(selectedVersion.target_difficulty))}</span>` : ""}
            ${selectedVersion.answer ? `<span class="chip">答案 ${escapeHtml(selectedVersion.answer)}</span>` : ""}
          </div>
          ${actionTrail ? `<div class="history-card-tags" style="margin-top: 10px;">${actionTrail}</div>` : ""}
        </div>
        <div class="history-detail-card">
          <div class="history-detail-grid">
            <div class="history-mini-card">
              <strong>材料预览</strong>
              <div>${escapeHtml(materialPreviewText(selectedVersion))}</div>
            </div>
            <div class="history-mini-card">
              <strong>变化摘要</strong>
              <div>${escapeHtml((selectedVersion.diff_summary?.changed_fields || []).join(" / ") || "首版生成")}</div>
            </div>
          </div>
        </div>
        <div class="history-detail-card">
          <div class="passage-label">${escapeHtml(primaryContentLabel(selectedVersion))}</div>
          <pre class="compact-pre">${escapeHtml(materialFullText(selectedVersion) || "未提供")}</pre>
        </div>
        <div class="history-detail-card">
          <div class="passage-label">题干</div>
          <div class="question-stem">${escapeHtml(stemFullText(selectedVersion) || selectedVersion.stem_preview || "未提供")}</div>
        </div>
        <div class="history-detail-card">
          <div class="passage-label">选项</div>
          ${renderOptionsPreview(optionsMap(selectedVersion))}
          <div class="answer-row" style="margin-top: 14px;"><strong>答案：</strong>${escapeHtml(answerText(selectedVersion) || "未提供")}</div>
          <div class="analysis-row" style="margin-top: 10px;"><strong>解析：</strong>${escapeHtml(analysisFullText(selectedVersion) || "未提供")}</div>
        </div>
      </div>
    `
    : '<div class="history-empty">当前题目还没有可展示的版本快照。</div>';
  return `
    <div class="history-layout">
      <div class="history-column">
        <div class="history-list">
          ${versions
            .map((version) => {
              const activeClass = Number(version.version_no) === Number(selectedVersionNo) ? "is-active" : "";
              const changedFields = (version.diff_summary?.changed_fields || []).slice(0, 4).join(" / ");
              const versionStatus = resolveVersionStatus(version, payload);
              return `
                <button
                  type="button"
                  class="history-card ${activeClass}"
                  data-action="select-version-history"
                  data-item-id="${escapeHtml(itemId)}"
                  data-version-no="${escapeHtml(version.version_no)}"
                >
                  <div class="history-card-head">
                    <div class="history-card-title">v${escapeHtml(version.version_no)} · ${escapeHtml(humanize(version.source_action || "generate"))}</div>
                    <span class="chip">${escapeHtml(humanize(versionStatus))}</span>
                  </div>
                  <div class="history-card-meta">${escapeHtml(formatDateTime(version.created_at))}</div>
                  <div class="history-card-preview">${escapeHtml(changedFields || version.stem_preview || "首版生成")}</div>
                </button>
              `;
            })
            .join("")}
        </div>
      </div>
      <div class="history-column">${detailMarkup}</div>
    </div>
  `;
}

function renderHistoryOverlay() {
  const root = ensureHistoryOverlay();
  const titleNode = $("historyOverlayTitle");
  const subtitleNode = $("historyOverlaySubtitle");
  const bodyNode = $("historyOverlayBody");
  if (!titleNode || !subtitleNode || !bodyNode) return;
  if (!state.overlayMode) {
    root.hidden = true;
    return;
  }
  root.hidden = false;
  if (state.overlayMode === "approved") {
    titleNode.textContent = "已确认历史";
    subtitleNode.textContent = `这里会累计所有已经确认通过的题目。当前共 ${state.approvedHistoryTotal || state.approvedHistory.length || 0} 道。`;
    bodyNode.innerHTML = renderApprovedHistoryBody();
    return;
  }
  const itemId = state.overlayContextItemId;
  const payload = state.historyByItem[itemId];
  titleNode.textContent = "修题版本历史";
  subtitleNode.textContent = payload
    ? `${resolveCategoryLabel(payload.item || {})} · 当前版本 v${payload.current_version_no || "-"}`
    : "正在整理当前题目的版本链…";
  bodyNode.innerHTML = renderVersionHistoryBody();
}

async function handleHistoryOverlayAction(event) {
  const button = event.target.closest("[data-action], [data-overlay-close]");
  if (!button) return;
  if (button.dataset.overlayClose) {
    closeHistoryOverlay();
    return;
  }
  const action = button.dataset.action;
  if (action === "open-approved-history") {
    await openApprovedHistory();
    return;
  }
  if (action === "select-approved-history") {
    await loadApprovedPreview(button.dataset.itemId);
    return;
  }
  if (action === "download-approved-item") {
    await downloadApprovedHistoryItem(button.dataset.itemId);
    showToast("下载成功");
    return;
  }
  if (action === "open-version-history") {
    await openItemVersionHistory(button.dataset.itemId);
    return;
  }
  if (action === "select-version-history") {
    const itemId = button.dataset.itemId;
    state.selectedVersionByItem[itemId] = Number(button.dataset.versionNo || 0);
    renderHistoryOverlay();
  }
}

function syncCountValue() {
  $("countValue").textContent = $("count").value;
}

function getSubtypeNodes() {
  const focus = $("questionFocus").value || "";
  return SPECIAL_TYPE_TREE[focus] || SPECIAL_TYPE_TREE[""];
}

function renderSubtypeOptions() {
  const select = $("businessSubtype");
  if (!select) return;
  const currentValue = select.value || "";
  const nodes = getSubtypeNodes();
  const options = EMPTY_SUBTYPE_OPTION.concat(nodes.map((node) => ({ value: node.value, label: node.label })));
  populateSelect(select, options);
  const allowed = new Set(options.map((option) => option.value));
  select.value = allowed.has(currentValue) ? currentValue : "";
}

function getSelectedSubtypeNode() {
  const subtype = $("businessSubtype")?.value || "";
  return getSubtypeNodes().find((node) => node.value === subtype) || null;
}

function getSelectedLeafNode() {
  const selectedSubtype = getSelectedSubtypeNode();
  if (!selectedSubtype) return null;
  const specialType = $("specialType")?.value || "";
  return (selectedSubtype.children || []).find((node) => node.value === specialType) || null;
}

function buildTaxonomyRequestPayload(questionFocus) {
  const subtypeNode = getSelectedSubtypeNode();
  const leafNode = getSelectedLeafNode();
  const familyConfig = GENERATION_TARGETS[questionFocus] || {};
  const leafConfig = (familyConfig.leaves || {})[leafNode?.value || ""] || {};
  const typeSlots = { ...(leafConfig.typeSlots || {}) };
  const referenceBusinessCards = Array.isArray(leafConfig.referenceBusinessCards)
    ? leafConfig.referenceBusinessCards.filter(Boolean)
    : [];
  const requestedGuardLines = Array.isArray(leafConfig.guardLines) ? leafConfig.guardLines.filter(Boolean) : [];
  const extraConstraints = {};
  if (leafConfig.patternId) {
    extraConstraints.pattern_id = leafConfig.patternId;
  }
  if (subtypeNode?.value) {
    extraConstraints.requested_child_family = subtypeNode.value;
    extraConstraints.requested_child_label = subtypeNode.label;
  }
  if (leafNode?.value) {
    extraConstraints.requested_leaf_id = leafNode.value;
    extraConstraints.requested_leaf_label = leafNode.label;
  }
  if (requestedGuardLines.length) {
    extraConstraints.requested_guard_lines = requestedGuardLines;
  }
  if (referenceBusinessCards.length) {
    extraConstraints.reference_business_cards = referenceBusinessCards;
  }
  return {
    businessSubtype: familyConfig.businessSubtype ?? null,
    typeSlots,
    extraConstraints,
  };
}

function renderSpecialTypeOptions() {
  const select = $("specialType");
  if (!select) return;
  const currentValue = select.value || "";
  const selectedSubtype = getSelectedSubtypeNode();
  if (!selectedSubtype) {
    populateSelect(select, EMPTY_SPECIAL_TYPE_OPTION);
    select.disabled = false;
    select.value = "";
    return;
  }
  const children = Array.isArray(selectedSubtype.children) ? selectedSubtype.children : [];
  if (!children.length) {
    populateSelect(select, NULL_SPECIAL_TYPE_OPTION);
    select.disabled = true;
    select.value = "";
    return;
  }
  const options = EMPTY_SPECIAL_TYPE_OPTION.concat(children.map((node) => ({ value: node.value, label: node.label })));
  populateSelect(select, options);
  select.disabled = false;
  const allowed = new Set(options.map((option) => option.value));
  select.value = allowed.has(currentValue) ? currentValue : "";
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

function hasStructuredSourceQuestion(payload) {
  if (!payload) return false;
  const hasStem = Boolean(String(payload.stem || "").trim());
  const hasOptions = Object.values(payload.options || {}).some((value) => String(value || "").trim());
  const hasAnswer = Boolean(String(payload.answer || "").trim());
  const hasAnalysis = Boolean(String(payload.analysis || "").trim());
  return hasStem || hasOptions || hasAnswer || hasAnalysis;
}

function inferQuestionFocus(sourceQuestion) {
  const stem = String(sourceQuestion?.stem || "");
  if (/排序|重新排列|语序正确/.test(stem)) return "sentence_order";
  if (/填入|横线|最恰当/.test(stem)) return "sentence_fill";
  if (/标题/.test(stem)) return "center_understanding";
  return "center_understanding";
}

function collectUserMaterialPayload() {
  const text = $("userMaterialText")?.value?.trim() || "";
  if (!text) return null;
  return { text };
}

function buildGeneratePayload(options = {}) {
  const includeUserMaterial = Boolean(options.includeUserMaterial);
  const sourceQuestion = collectSourceQuestionPayload();
  const userMaterial = includeUserMaterial ? collectUserMaterialPayload() : null;
  const questionFocus = $("questionFocus").value || (sourceQuestion ? inferQuestionFocus(sourceQuestion) : "");
  const taxonomyPayload = buildTaxonomyRequestPayload(questionFocus);
  const payload = {
    question_focus: questionFocus,
    business_subtype: taxonomyPayload.businessSubtype,
    difficulty_level: $("difficultyLevel").value || "medium",
    special_question_types: [],
    count: Number($("count").value || 1),
    source_question: sourceQuestion,
    type_slots: taxonomyPayload.typeSlots,
    extra_constraints: taxonomyPayload.extraConstraints,
  };
  if (userMaterial) {
    payload.generation_mode = "forced_user_material";
    payload.user_material = userMaterial;
  }
  return payload;
}

function validateGeneratePayload(payload, options = {}) {
  const requireUserMaterial = Boolean(options.requireUserMaterial);
  const useAlert = Boolean(options.useAlert);
  if (!payload.question_focus) {
    if (useAlert) alert("请先配置题型参数，再提交生成。");
    return false;
  }
  if (!payload.difficulty_level) {
    if (useAlert) alert("请先配置难度参数，再提交生成。");
    return false;
  }
  if (requireUserMaterial && !(payload.user_material && payload.user_material.text)) {
    if (useAlert) alert("请先粘贴用户材料，再提交。");
    return false;
  }
  return true;
}

function applyDetectedTarget(detection) {
  const detectedFocus = String(detection?.question_focus || "").trim();
  const detectedSpecialType = String(detection?.special_question_type || "").trim();
  const detectedSubtype = String(detection?.business_subtype || "").trim();

  if (detectedFocus) {
    $("questionFocus").value = detectedFocus;
  }
  renderSubtypeOptions();
  const subtypeValue = detectedSubtype || "";
  if (subtypeValue) {
    $("businessSubtype").value = subtypeValue;
  }
  renderSpecialTypeOptions();
  if (detectedSpecialType && !$("specialType").disabled) {
    $("specialType").value = detectedSpecialType;
  }
}

function buildDetectionStatusHtml(detection) {
  const focus = humanize(detection?.question_focus);
  const subtype = humanize(detection?.business_subtype);
  const specialType = humanize(detection?.special_question_type);
  const leaf = humanize(detection?.leaf_id_primary);
  const confidence = Number.isFinite(Number(detection?.analysis_confidence))
    ? `${Math.round(Number(detection.analysis_confidence) * 100)}%`
    : "未提供";
  const businessCards = (detection?.business_card_ids || [])
    .slice(0, 3)
    .map((item) => humanize(item))
    .join(" / ");
  const queryTerms = (detection?.query_terms || []).slice(0, 4).join(" / ") || "未提供";

  return [
    `已识别题型：${escapeHtml(focus)}`,
    `业务子类：${escapeHtml(subtype)}`,
    `业务孙类：${escapeHtml(specialType)}`,
    `孙族卡：${escapeHtml(leaf)}`,
    `命中业务卡：${escapeHtml(businessCards || "未提供")}`,
    `检索关键词：${escapeHtml(queryTerms)}`,
    `识别置信度：${escapeHtml(confidence)}`,
  ].join("<br />");
}

async function executeGenerate(payload) {
  setBanner("builderError", "");
  setBanner("loadingError", "");

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

function setUserMaterialPanelOpen(open, options = {}) {
  const panel = $("userMaterialPanel");
  const textarea = $("userMaterialText");
  const toggleBtn = $("userMaterialToggleBtn");
  if (!panel || !textarea || !toggleBtn) return;

  panel.hidden = !open;
  toggleBtn.textContent = open ? "收起材料框" : "自己上传材料";
  if (open) {
    textarea.focus();
  } else if (options.clear) {
    textarea.value = "";
  }
}

async function submitUserMaterialGeneration() {
  const payload = buildGeneratePayload({ includeUserMaterial: true });
  if (!validateGeneratePayload(payload, { requireUserMaterial: true, useAlert: true })) {
    return;
  }
  await executeGenerate(payload);
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
    let parsed = collectSourceQuestionPayload() || {};
    if (!hasStructuredSourceQuestion(parsed)) {
      const response = await apiFetch("/api/v1/questions/source-question/parse", {
        method: "POST",
        body: JSON.stringify({ raw_text: rawText }),
      });
      parsed = response.source_question || {};
      $("sourceQuestionPassage").value = parsed.passage || "";
      $("sourceQuestionStem").value = parsed.stem || "";
      $("sourceOptionA").value = parsed.options?.A || "";
      $("sourceOptionB").value = parsed.options?.B || "";
      $("sourceOptionC").value = parsed.options?.C || "";
      $("sourceOptionD").value = parsed.options?.D || "";
      $("sourceQuestionAnswer").value = parsed.answer || "";
      $("sourceQuestionAnalysis").value = parsed.analysis || "";
    }
    const detection = await apiFetch("/api/v1/questions/source-question/detect", {
      method: "POST",
      body: JSON.stringify({ source_question: parsed }),
    });
    applyDetectedTarget(detection);
    statusNode.innerHTML = buildDetectionStatusHtml(detection);
    showToast("参考母题已拆题并识别");
  } catch (error) {
    statusNode.textContent = "自动拆题失败，请保留原文并手动补充必要字段。";
    setBanner("builderError", `自动拆题失败：${error.message}`);
  } finally {
    setButtonBusy(button, false);
  }
}

async function generateQuestions(event) {
  event.preventDefault();
  const payload = buildGeneratePayload();
  if (!validateGeneratePayload(payload)) {
    setBanner("builderError", "请先选择题型，或先提供参考母题让系统自动识别。");
    return;
  }
  await executeGenerate(payload);
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
          <span>${escapeHtml(cleanDisplayText(value))}</span>
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
  return item?.material_source || item?.material_selection?.source || item?.runtime_snapshot?.material_snapshot?.source || {};
}

function isManualOverrideActive(item) {
  if (!item || typeof item !== "object") return false;
  if (truthyBoolean(item.manual_override_active)) return true;
  return truthyBoolean(item?.generated_question?.metadata?.manual_edit);
}

function getFeedbackSnapshot(item) {
  if (isManualOverrideActive(item)) {
    return null;
  }

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
      ${
        isMissingScoringFeedback(feedback)
          ? '<span class="signal-chip signal-chip-neutral">材料评分：暂未回填</span>'
          : ""
      }
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

  if (isMissingScoringFeedback(feedback)) {
    return `
      <div class="signal-grid">
        <div class="mini-card">
          <strong>${escapeHtml(fieldLabel("selection_state"))}</strong>
          <div>${escapeHtml(humanize(feedback.selection_state || "未提供"))}</div>
        </div>
        <div class="mini-card">
          <strong>${escapeHtml(fieldLabel("difficulty_band_hint"))}</strong>
          <div>待判定</div>
        </div>
        <div class="mini-card">
          <strong>${escapeHtml(fieldLabel("final_candidate_score"))}</strong>
          <div>未回填</div>
        </div>
        <div class="mini-card">
          <strong>${escapeHtml(fieldLabel("readiness_score"))}</strong>
          <div>未回填</div>
        </div>
        <div class="mini-card">
          <strong>${escapeHtml(fieldLabel("total_penalty"))}</strong>
          <div>未回填</div>
        </div>
        <div class="mini-card">
          <strong>${escapeHtml(fieldLabel("quality_note"))}</strong>
          <div>材料评分缺失，当前仅展示题面结果</div>
        </div>
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
  if (isMissingScoringFeedback(feedback)) {
    items.push(`
      <li>
        <strong>评分状态</strong><br />
        当前材料尚未回填题卡 scoring，页面不再把占位值显示成 0 分；这张卡的题面可继续参考，但分值信息暂不能当成真实结论。
      </li>
    `);
  }
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
            const parts = [humanizeCode(entry.name, CHECK_NAME_LABELS)];
            if (entry.actual != null) {
              parts.push(`${localizeCheckDetailLabel("actual")}=${formatCheckDetailValue(entry.actual)}`);
            }
            if (entry.threshold != null) {
              parts.push(`${localizeCheckDetailLabel("threshold")}=${formatCheckDetailValue(entry.threshold)}`);
            }
            if (entry.allowedRange != null) {
              parts.push(`${localizeCheckDetailLabel("allowed")}=${formatCheckDetailValue(entry.allowedRange)}`);
            }
            if (entry.difficultyBand != null) {
              parts.push(`${localizeCheckDetailLabel("band")}=${localizeDisplayMessage(entry.difficultyBand)}`);
            }
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
        ${validationErrors.map((entry) => escapeHtml(localizeDisplayMessage(entry))).join("<br />")}
      </li>
    `);
  }
  if (validationWarnings.length) {
    items.push(`
      <li>
        <strong>${escapeHtml(fieldLabel("validator_warnings"))}</strong><br />
        ${validationWarnings.map((entry) => escapeHtml(localizeDisplayMessage(entry))).join("<br />")}
      </li>
    `);
  }

  if (!items.length) {
    return '<div class="empty-state">当前卡片没有更多可解释字段，先保留基础结果展示。</div>';
  }

  return `<ul class="explain-list">${items.join("")}</ul>`;
}

const DISTRACTOR_STRATEGY_OPTIONS = [
  ["", "不调整"],
  ["partial_scope", "以偏概全"],
  ["detail_trap", "细节设陷"],
  ["concept_swap", "偷换概念"],
  ["stronger_conclusion", "结论过强"],
  ["causal_reversal", "因果倒置"],
];

const DISTRACTOR_INTENSITY_OPTIONS = [
  ["", "不调整"],
  ["mild", "偏弱"],
  ["medium", "中等"],
  ["strong", "偏强"],
];

const QUESTION_MODIFY_TUNING_FIELDS = [
  {
    key: "review_difficulty_target",
    label: "目标难度",
    help: "按审核预期把题目整体往简单、中等或困难调整。",
    options: [
      ["", "不调整"],
      ["easy", "简单"],
      ["medium", "中等"],
      ["hard", "困难"],
    ],
  },
  {
    key: "review_adjustment_scope",
    label: "调整范围",
    help: "只改错误项，或连题干一起调整。",
    options: [
      ["", "不调整"],
      ["distractors_only", "仅错误项"],
      ["stem_and_distractors", "题干和错误项"],
      ["full_question", "全题重塑"],
    ],
  },
  {
    key: "review_keep_correct_answer_fixed",
    label: "固定正确答案",
    help: "优先保持当前正确项不动，只调整其余部分。",
    options: [
      ["", "不指定"],
      ["true", "固定"],
      ["false", "允许调整"],
    ],
  },
];

function renderDistractorPatchOptions(optionPairs, selectedValue = "") {
  return optionPairs
    .map(([value, label]) => {
      const selected = value === selectedValue ? " selected" : "";
      return `<option value="${escapeHtml(value)}"${selected}>${escapeHtml(label)}</option>`;
    })
    .join("");
}

function normalizeControlArrayValue(value) {
  if (Array.isArray(value)) {
    return value.map((entry) => String(entry || "").trim()).filter(Boolean);
  }
  const text = String(value || "").trim();
  return text ? [text] : [];
}

function sortStrings(values) {
  return values.slice().sort((left, right) => left.localeCompare(right, "zh-CN"));
}

function sameStringArrays(left, right) {
  const a = sortStrings(left);
  const b = sortStrings(right);
  if (a.length !== b.length) return false;
  return a.every((value, index) => value === b[index]);
}

function renderQuestionModifySection(item) {
  const itemId = item.item_id;
  const panel = state.controlsByItem[itemId];
  if (!panel || !Array.isArray(panel.controls)) {
    return '<div class="inline-feedback">题目级修改控件加载中...</div>';
  }

  if (!panel.controls.length) {
    return '<div class="inline-feedback">当前题型没有开放的题目级修改字段。</div>';
  }

  const editableControls = panel.controls.filter((control) => !control.read_only && control.mapped_action === "question_modify");
  if (!editableControls.length) {
    return '<div class="inline-feedback">当前题型没有开放的题目级修改字段。</div>';
  }

  const fields = editableControls.map((control) => {
      const options = Array.isArray(control.options) ? control.options : [];
      const controlType = String(control.control_type || "string");
      if (controlType === "array") {
        const selectedValues = normalizeControlArrayValue(control.current_value);
        const initialValue = escapeHtml(JSON.stringify(sortStrings(selectedValues)));
        const maxSelected = Number(control.max_selected || 0) || 2;
        return `
          <div
            class="field-compact question-modify-field"
            data-item-id="${itemId}"
            data-control-key="${escapeHtml(control.control_key)}"
            data-control-type="array"
            data-initial-value="${initialValue}"
            data-max-selected="${maxSelected}"
          >
            <span>${escapeHtml(control.label || control.control_key)}</span>
            <div class="option-list">
              ${options
                .map((option) => {
                  const value = option.value == null ? "" : String(option.value);
                  const checked = selectedValues.includes(value) ? " checked" : "";
                  return `
                    <label style="display:block; margin-bottom:6px;">
                      <input
                        type="checkbox"
                        class="question-modify-array-option"
                        data-item-id="${itemId}"
                        data-control-key="${escapeHtml(control.control_key)}"
                        value="${escapeHtml(value)}"${checked}
                      />
                      <span>${escapeHtml(option.label || value)}</span>
                    </label>
                  `;
                })
                .join("")}
            </div>
            <small class="field-help">${escapeHtml(control.description || "单次最多选 2 个。")}</small>
          </div>
        `;
      }
      const selectedValue = control.current_value == null ? "" : String(control.current_value);
      return `
        <div
          class="field-compact question-modify-field"
          data-item-id="${itemId}"
          data-control-key="${escapeHtml(control.control_key)}"
          data-control-type="${escapeHtml(controlType)}"
          data-initial-value="${escapeHtml(selectedValue)}"
        >
          <span>${escapeHtml(control.label || control.control_key)}</span>
          <select class="question-modify-input" data-item-id="${itemId}" data-control-key="${escapeHtml(control.control_key)}">
            <option value="">不调整</option>
            ${options
              .map((option) => {
                const value = option.value == null ? "" : String(option.value);
                const selected = value === selectedValue ? " selected" : "";
                return `<option value="${escapeHtml(value)}"${selected}>${escapeHtml(option.label || value)}</option>`;
              })
              .join("")}
          </select>
          <small class="field-help">${escapeHtml(control.description || "沿用后端返回的控件定义。")}</small>
        </div>
      `;
    })
    .join("");

  return `
    <div class="inline-feedback">题目级修改：单次最多调整 2 项，直接作用于当前题卡结构。</div>
    <div class="builder-stack">${fields}</div>
  `;
}

function renderQuestionModifyPanel(item) {
  const itemId = item.item_id;
  const panel = state.controlsByItem[itemId];
  let dynamicSection = '<div class="inline-feedback">题目级修改控件加载中...</div>';

  if (panel && Array.isArray(panel.controls)) {
    const editableControls = panel.controls.filter((control) => !control.read_only && control.mapped_action === "question_modify");
    if (editableControls.length) {
      dynamicSection = renderQuestionModifySection(item);
    } else {
      dynamicSection = '<div class="inline-feedback">当前题型没有开放的题目级修改字段。</div>';
    }
  }

  return dynamicSection;
}

function renderDistractorPatchPanel(item) {
  const generated = item?.generated_question || {};
  const answer = String(generated.answer || "").trim().toUpperCase();
  const analysisText = cleanDisplayText(generated.analysis || "");
  const options = generated.options || {};
  const discarded = item.current_status === "discarded";

  return ["A", "B", "C", "D"]
    .map((letter) => {
      const optionText = cleanDisplayText(options[letter] || "");
      if (letter === answer) {
        return `
          <div class="material-box">
            <div class="section-title">选项 ${letter}</div>
            <div class="inline-feedback">正确项，已锁定，不提供迷惑方式或迷惑强度编辑入口。</div>
            <pre class="compact-pre">${escapeHtml(optionText || "未提供")}</pre>
          </div>
        `;
      }

      return `
        <div class="material-box">
          <div class="section-title">错误项 ${letter}</div>
          <div class="inline-feedback">这次只会作用于选项 ${letter} 和解析，其它选项、题干、材料与答案保持锁定。</div>
          <label class="field-compact">
            <span>错误项文案</span>
            <textarea class="distractor-option-text" data-item-id="${item.item_id}" data-target-option="${letter}" rows="2" ${
              discarded ? "disabled" : ""
            }>${escapeHtml(optionText)}</textarea>
          </label>
          <div class="builder-grid">
            <label class="field-compact">
              <span>迷惑方式</span>
              <select class="distractor-strategy-input" data-item-id="${item.item_id}" data-target-option="${letter}" ${
                discarded ? "disabled" : ""
              }>
                ${renderDistractorPatchOptions(DISTRACTOR_STRATEGY_OPTIONS)}
              </select>
            </label>
            <label class="field-compact">
              <span>迷惑强度</span>
              <select class="distractor-intensity-input" data-item-id="${item.item_id}" data-target-option="${letter}" ${
                discarded ? "disabled" : ""
              }>
                ${renderDistractorPatchOptions(DISTRACTOR_INTENSITY_OPTIONS)}
              </select>
            </label>
          </div>
          <label class="field-compact">
            <span>解析联动</span>
            <textarea class="distractor-analysis" data-item-id="${item.item_id}" data-target-option="${letter}" rows="4" ${
              discarded ? "disabled" : ""
            }>${escapeHtml(analysisText)}</textarea>
          </label>
          <div class="action-row">
            <button type="button" class="secondary-btn" data-action="apply-distractor-patch" data-item-id="${item.item_id}" data-target-option="${letter}" ${
              discarded ? "disabled" : ""
            }>应用到错误项 ${letter}</button>
          </div>
        </div>
      `;
    })
    .join("");
}

function renderReplacementOptions(itemId) {
  const replacements = state.replacementsByItem[itemId];
  const selectedMaterialId = state.selectedReplacementByItem[itemId] || "";
  if (!replacements || !Array.isArray(replacements.items) || !replacements.items.length) {
    return '<option value="">先点击“加载备选材料”</option>';
  }

  return [
    `<option value=""${selectedMaterialId ? "" : " selected"}>请选择备选材料</option>`,
    ...replacements.items.map((entry) => {
      const label = [entry.article_title, entry.source_name, entry.document_genre]
        .map((part) => String(part || "").trim())
        .filter(Boolean)
        .join(" / ");
      const materialId = String(entry.material_id || "").trim();
      const selected = materialId && materialId === selectedMaterialId ? " selected" : "";
      return `<option value="${escapeHtml(materialId)}"${selected}>${escapeHtml(label || "备选材料")}</option>`;
    }),
  ].join("");
}

function getReplacementEntry(itemId, materialId) {
  const replacements = state.replacementsByItem[itemId];
  if (!replacements || !Array.isArray(replacements.items) || !materialId) return null;
  return (
    replacements.items.find((entry) => String(entry.material_id || "").trim() === String(materialId || "").trim()) || null
  );
}

function renderReplacementPreview(itemId) {
  const materialId = state.selectedReplacementByItem[itemId] || "";
  const entry = getReplacementEntry(itemId, materialId);
  if (!entry) {
    return `
      <div class="replacement-preview-box empty-state">
        选择一条备选材料后，会在这里预览即将替换进去的材料内容。
      </div>
    `;
  }

  const previewText = String(entry.material_text || entry.text_preview || "").trim();
  return `
    <div class="replacement-preview-box">
      <div class="replacement-preview-meta">
        <div class="mini-card">
          <strong>备选标题</strong>
          <div>${escapeHtml(entry.article_title || "未提供")}</div>
        </div>
        <div class="mini-card">
          <strong>来源</strong>
          <div>${escapeHtml(entry.source_name || "未提供")}</div>
        </div>
        <div class="mini-card">
          <strong>文体</strong>
          <div>${escapeHtml(entry.document_genre || "未提供")}</div>
        </div>
      </div>
      <div class="inline-feedback">备选材料预览</div>
      <pre class="compact-pre">${escapeHtml(previewText || "未提供")}</pre>
    </div>
  `;
}

function buildQuestionCard(item, index) {
  const generated = item.generated_question || {};
  const material = item.material_selection || {};
  const materialSource = getMaterialSource(item);
  const manualOverrideActive = isManualOverrideActive(item);
  const feedback = getFeedbackSnapshot(item);
  const currentStatus = item.current_status || "generated";
  const approved = currentStatus === "approved";
  const discarded = currentStatus === "discarded";
  const primaryContent = resolvePrimaryContent(item);
  const materialText = rawMaterialFullText(item);
  const originalMaterial = cleanDisplayText(material.original_text || material.text || "", { replaceBlankToken: false });
  const stemText = cleanDisplayText(generated.stem || item.stem_text || "");
  const analysisText = cleanDisplayText(generated.analysis || "");
  const displaySubtype = resolveDisplayedSubtype(item);
  const requestedTaxonomy = resolveRequestedTaxonomy(item);
  const actualTaxonomy = resolveActualTaxonomy(item);

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
            <span class="chip">${escapeHtml(humanize(displaySubtype || "未提供"))}</span>
            <span class="chip">${escapeHtml(humanize(item.difficulty_target || "medium"))}</span>
            ${item.forced_generation ? '<span class="chip">用户自带材料</span>' : ""}
            ${item.material_source_type ? `<span class="chip">${escapeHtml(humanize(item.material_source_type))}</span>` : ""}
            ${materialSource.caution_tag ? `<span class="chip status status-warn">${escapeHtml(humanize(materialSource.caution_tag))}</span>` : ""}
            ${manualOverrideActive ? '<span class="chip">人工接管</span>' : ""}
          </div>
        </div>
        <div class="question-head-actions">
          <button type="button" class="secondary-btn" data-action="open-version-history" data-item-id="${item.item_id}">版本历史</button>
        </div>
      </div>

      <div class="question-box">
        ${
          requestedTaxonomy.child || requestedTaxonomy.leaf || actualTaxonomy.child || actualTaxonomy.leaf
            ? `
        <div class="inline-feedback">
          用户选择：${escapeHtml(requestedTaxonomy.child || "未提供")} / ${escapeHtml(requestedTaxonomy.leaf || "未提供")}
          &nbsp;&nbsp;|&nbsp;&nbsp;
          系统落位：${escapeHtml(actualTaxonomy.child || "未提供")} / ${escapeHtml(actualTaxonomy.leaf || "未提供")}
        </div>
          `
              : ""
          }
          <div class="passage-label">${escapeHtml(primaryContent.label || "题目主内容")}</div>
          <div class="passage-preview">${escapeHtml(primaryContent.text || "暂无题目主内容")}</div>
          <div class="passage-label" style="margin-top: 18px;">题干</div>
        <div class="question-stem">${escapeHtml(stemText || "暂无题干")}</div>
        <div class="option-list">${renderOptions(generated.options || {})}</div>
        <div class="answer-row"><strong>答案：</strong>${escapeHtml(generated.answer || "未提供")}</div>
        <div class="analysis-row"><strong>解析：</strong>${escapeHtml(analysisText || "未提供")}</div>
      </div>

      ${
        manualOverrideActive
          ? ""
          : `
      <div class="system-box" style="margin-top: 16px;">
        <div class="section-title">结构与状态</div>
        ${renderSignalSummary(feedback)}
        <div style="margin-top: 14px;">${renderMetricCards(feedback)}</div>
      </div>

      <div class="material-box" style="margin-top: 16px;">
        <div class="section-title">风险与建议</div>
        ${renderExplainList(feedback, item)}
      </div>
      `
      }

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
          ${renderQuestionModifyPanel(item)}
          <div class="action-row">
            <button type="button" class="secondary-btn" data-action="question-modify" data-item-id="${item.item_id}" ${
              discarded ? "disabled" : ""
            }>按参数重做</button>
          </div>
          <div class="inline-feedback">按钮直接调用后端既有参数重做动作，不在前端定义额外语义。</div>
        </div>
      </details>

      <details class="result-collapse" style="margin-top: 16px;">
        <summary>错误项定点修复</summary>
        <div class="collapse-body support-box">
          ${renderDistractorPatchPanel(item)}
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
          <div class="replacement-preview-slot">${renderReplacementPreview(item.item_id)}</div>
          <label class="field-compact">
            <span>自贴材料</span>
            <textarea class="custom-material-input" data-item-id="${item.item_id}" rows="4" ${
              discarded ? "disabled" : ""
            } placeholder="可直接粘贴一段替换材料，再执行材料重做。"></textarea>
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
            }>${escapeHtml(stemText)}</textarea>
          </label>
          <div class="builder-grid">
            <label class="field-compact">
              <span>选项 A</span>
              <textarea class="manual-option" data-item-id="${item.item_id}" data-option="A" rows="2" ${
                discarded ? "disabled" : ""
              }>${escapeHtml(cleanDisplayText(generated.options?.A || ""))}</textarea>
            </label>
            <label class="field-compact">
              <span>选项 B</span>
              <textarea class="manual-option" data-item-id="${item.item_id}" data-option="B" rows="2" ${
                discarded ? "disabled" : ""
              }>${escapeHtml(cleanDisplayText(generated.options?.B || ""))}</textarea>
            </label>
            <label class="field-compact">
              <span>选项 C</span>
              <textarea class="manual-option" data-item-id="${item.item_id}" data-option="C" rows="2" ${
                discarded ? "disabled" : ""
              }>${escapeHtml(cleanDisplayText(generated.options?.C || ""))}</textarea>
            </label>
            <label class="field-compact">
              <span>选项 D</span>
              <textarea class="manual-option" data-item-id="${item.item_id}" data-option="D" rows="2" ${
                discarded ? "disabled" : ""
              }>${escapeHtml(cleanDisplayText(generated.options?.D || ""))}</textarea>
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
              }>${escapeHtml(analysisText)}</textarea>
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
  const typeSlots = {};
  let changedCount = 0;
  card.querySelectorAll(`.question-modify-field[data-item-id="${itemId}"]`).forEach((fieldNode) => {
    const key = fieldNode.dataset.controlKey;
    const controlType = fieldNode.dataset.controlType || "string";
    if (!key) return;
    if (controlType === "array") {
      const initialValue = normalizeControlArrayValue(JSON.parse(fieldNode.dataset.initialValue || "[]"));
      const selectedValues = normalizeControlArrayValue(
        Array.from(fieldNode.querySelectorAll(".question-modify-array-option:checked")).map((node) => node.value),
      );
      if (sameStringArrays(selectedValues, initialValue)) return;
      typeSlots[key] = selectedValues;
      changedCount += 1;
      return;
    }
    const node = fieldNode.querySelector(`.question-modify-input[data-control-key="${key}"]`);
    const value = String(node?.value || "").trim();
    const initialValue = String(fieldNode.dataset.initialValue || "").trim();
    if (!value || value === initialValue) return;
    typeSlots[key] = value;
    changedCount += 1;
  });
  if (changedCount > QUESTION_MODIFY_MAX_CHANGES) {
    throw new Error(`题目级修改一次最多调整 ${QUESTION_MODIFY_MAX_CHANGES} 项参数。`);
  }
  if (Object.keys(typeSlots).length) {
    overrides.type_slots = typeSlots;
  }
  return overrides;
}

function handleQuestionModifySelectionChange(event) {
  const checkboxNode = event.target.closest(".question-modify-array-option");
  if (!checkboxNode) return;
  const fieldNode = checkboxNode.closest(".question-modify-field");
  if (!fieldNode) return;
  const maxSelected = Number(fieldNode.dataset.maxSelected || 0) || 2;
  const selectedNodes = fieldNode.querySelectorAll(".question-modify-array-option:checked");
  if (selectedNodes.length <= maxSelected) return;
  checkboxNode.checked = false;
  showToast(`这个字段最多选 ${maxSelected} 个选项`, "info");
}

function collectDistractorPatchPayload(itemId, targetOption) {
  const card = getCard(itemId);
  const item = state.items.find((entry) => entry.item_id === itemId);
  const generated = item?.generated_question || {};
  const normalizedTargetOption = String(targetOption || "").trim().toUpperCase();
  const answer = String(generated.answer || "").trim().toUpperCase();
  if (!card || !normalizedTargetOption) return null;
  if (normalizedTargetOption === answer) return null;

  const currentOptionText = cleanDisplayText(generated.options?.[normalizedTargetOption] || "");
  const currentAnalysis = cleanDisplayText(generated.analysis || "");
  const optionText =
    card.querySelector(`.distractor-option-text[data-item-id="${itemId}"][data-target-option="${normalizedTargetOption}"]`)
      ?.value.trim() || "";
  const analysis =
    card.querySelector(`.distractor-analysis[data-item-id="${itemId}"][data-target-option="${normalizedTargetOption}"]`)
      ?.value.trim() || "";
  const distractorStrategy =
    card.querySelector(`.distractor-strategy-input[data-item-id="${itemId}"][data-target-option="${normalizedTargetOption}"]`)
      ?.value.trim() || "";
  const distractorIntensity =
    card.querySelector(`.distractor-intensity-input[data-item-id="${itemId}"][data-target-option="${normalizedTargetOption}"]`)
      ?.value.trim() || "";

  const hasManualChange = optionText !== currentOptionText || analysis !== currentAnalysis;
  if (!hasManualChange && !distractorStrategy && !distractorIntensity) {
    return null;
  }

  const payload = {
    action: "distractor_patch",
    target_option: normalizedTargetOption,
    option_text: optionText || currentOptionText,
    analysis: analysis || currentAnalysis,
    operator: "demo",
  };
  if (distractorStrategy) payload.distractor_strategy = distractorStrategy;
  if (distractorIntensity) payload.distractor_intensity = distractorIntensity;
  return payload;
}

function upsertItem(nextItem) {
  state.items = state.items.map((current) => (current.item_id === nextItem.item_id ? nextItem : current));
}

function refreshHistoryCaches(itemId, nextItem = null) {
  delete state.historyByItem[itemId];
  delete state.historyLoadingByItem[itemId];
  delete state.selectedVersionByItem[itemId];
  state.approvedHistoryLoaded = false;
  if (state.approvedPreviewItem?.item_id === itemId) {
    state.approvedPreviewItem = nextItem && nextItem.current_status === "approved" ? nextItem : null;
  }
  if (nextItem && nextItem.current_status === "approved") {
    state.approvedPreviewItemId = itemId;
  }
  if (state.overlayMode === "versions" && state.overlayContextItemId === itemId) {
    void loadItemVersionHistory(itemId);
  }
  if (state.overlayMode === "approved") {
    void loadApprovedHistory();
  }
}

async function loadControlsForItem(itemId) {
  const payload = await apiFetch(`/api/v1/questions/${itemId}/controls`);
  state.controlsByItem[itemId] = payload;
  renderResults();
}

async function loadReplacementMaterials(itemId) {
  const payload = await apiFetch(`/api/v1/questions/${itemId}/replacement-materials?limit=8`);
  state.replacementsByItem[itemId] = payload;
  const currentSelection = state.selectedReplacementByItem[itemId] || "";
  if (currentSelection && !getReplacementEntry(itemId, currentSelection)) {
    state.selectedReplacementByItem[itemId] = "";
  }
  renderResults();
}

function updateReplacementPreviewFromSelection(selectNode) {
  if (!selectNode) return;
  const itemId = selectNode.dataset.itemId;
  if (!itemId) return;
  state.selectedReplacementByItem[itemId] = String(selectNode.value || "").trim();
  const card = getCard(itemId);
  const previewNode = card?.querySelector(".replacement-preview-slot");
  if (previewNode) {
    previewNode.innerHTML = renderReplacementPreview(itemId);
  }
}

async function handleResultAction(event) {
  const button = event.target.closest("[data-action]");
  if (!button) return;

  const action = button.dataset.action;
  const itemId = button.dataset.itemId;
  if (!itemId) return;

  try {
    if (action === "open-version-history") {
      await openItemVersionHistory(itemId);
      return;
    }

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
      refreshHistoryCaches(itemId, result.item);
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
      refreshHistoryCaches(itemId, result.item);
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
      refreshHistoryCaches(itemId, result.item);
      renderResults();
      void loadControlsForItem(itemId);
      showToast("参数重做已提交");
      return;
    }

    if (action === "apply-distractor-patch") {
      const targetOption = String(button.dataset.targetOption || "").trim().toUpperCase();
      const payload = collectDistractorPatchPayload(itemId, targetOption);
      if (!payload) {
        showToast("请先调整该错误项的迷惑方式、迷惑强度或文本后再提交", "info");
        return;
      }

      setButtonBusy(button, true, "应用中...");
      const result = await apiFetch(`/api/v1/questions/${itemId}/review-actions`, {
        method: "POST",
        body: JSON.stringify(payload),
      });
      upsertItem(result.item);
      refreshHistoryCaches(itemId, result.item);
      renderResults();
      void loadControlsForItem(itemId);
      showToast(`错误项 ${targetOption} 已提交定点修复`);
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
      refreshHistoryCaches(itemId, result.item);
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
      refreshHistoryCaches(itemId, result.item);
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
      refreshHistoryCaches(itemId, result.item);
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
  link.download = `已通过题目_批次_${state.batchId}.md`;
  document.body.appendChild(link);
  link.click();
  link.remove();
  URL.revokeObjectURL(href);
}

function initPage() {
  ensureHistoryOverlay();
  ensureBuilderHistoryEntry();
  ensureResultHistoryEntry();
  populateSelect($("questionFocus"), QUESTION_FOCUS_OPTIONS);
  populateSelect($("businessSubtype"), EMPTY_SUBTYPE_OPTION);
  populateSelect($("specialType"), EMPTY_SPECIAL_TYPE_OPTION);
  populateSelect($("difficultyLevel"), DIFFICULTY_OPTIONS);
  renderSubtypeOptions();
  renderSpecialTypeOptions();

  $("count").addEventListener("input", syncCountValue);
  $("questionFocus").addEventListener("change", () => {
    renderSubtypeOptions();
    renderSpecialTypeOptions();
  });
  $("businessSubtype")?.addEventListener("change", () => {
    renderSpecialTypeOptions();
  });
  $("generateForm").addEventListener("submit", generateQuestions);
  $("userMaterialToggleBtn")?.addEventListener("click", () => {
    setUserMaterialPanelOpen(Boolean($("userMaterialPanel")?.hidden));
  });
  $("userMaterialCancelBtn")?.addEventListener("click", () => {
    setUserMaterialPanelOpen(false, { clear: true });
  });
  $("userMaterialSubmitBtn")?.addEventListener("click", () => {
    submitUserMaterialGeneration().catch((error) => {
      setBanner("builderError", `用户材料生成失败：${error.message}`);
    });
  });
  $("sourceQuestionDetectBtn").addEventListener("click", () => {
    autoDetectSourceQuestion().catch((error) => {
      setBanner("builderError", `自动拆题失败：${error.message}`);
    });
  });
  document.addEventListener("click", (event) => {
    const button = event.target.closest('[data-action="open-approved-history"]');
    if (!button) return;
    openApprovedHistory().catch((error) => {
      showToast(error.message || "打开历史失败", "info");
    });
  });
  $("resultList").addEventListener("click", (event) => {
    handleResultAction(event).catch((error) => {
      showToast(error.message || "操作失败", "info");
    });
  });
  $("resultList").addEventListener("change", (event) => {
    handleQuestionModifySelectionChange(event);
    const selectNode = event.target.closest(".replacement-select");
    if (!selectNode) return;
    updateReplacementPreviewFromSelection(selectNode);
  });
  $("backToBuilderBtn").addEventListener("click", () => switchScreen("builder"));
  $("cancelLoadingBtn").addEventListener("click", () => switchScreen("builder"));
  $("exportApprovedBtn").addEventListener("click", () => {
    exportApprovedBatch()
      .then(() => showToast("导出成功"))
      .catch((error) => showToast(error.message || "导出失败", "info"));
  });
  $("historyOverlay").addEventListener("click", (event) => {
    handleHistoryOverlayAction(event).catch((error) => {
      showToast(error.message || "历史操作失败", "info");
    });
  });
  $("historyOverlayCloseBtn")?.addEventListener("click", () => {
    closeHistoryOverlay();
  });

  syncCountValue();
  renderLoadingSteps();
}

document.addEventListener("DOMContentLoaded", initPage);


