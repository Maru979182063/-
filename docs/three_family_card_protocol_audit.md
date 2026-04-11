# 三大母族卡片协议审计表

## 说明

本表仅覆盖当前最影响主链业务的三大母族：

- `center_understanding` / `main_idea`
- `sentence_fill`
- `sentence_order`

这里的“字段”不是指所有运行时杂项字段，而是指当前真实参与以下环节的协议字段：

- 检索
- 材料卡匹配
- 信号层判别
- 题卡约束
- prompt/type 生成约束
- validator / review 规则

---

## 1. 中心理解 / 主旨概括母族

### 1.1 协议总表

| 母族语义 | 协议层 | 关键字段 | 当前值/口径 | 来源 | 是否冲突 | 备注 |
|---|---|---|---|---|---|---|
| `center_understanding` | `question_card` | `card_id` | `question.center_understanding.standard_v1` | `card_specs/normalized/question_cards/center_understanding_standard_question_card.normalized.yaml` | 否 | 题卡身份独立 |
| `center_understanding` | `question_card` | `business_family_id` | `title_selection` | 同上 | 是 | 语义是中心理解，母族却仍挂 title_selection |
| `center_understanding` | `question_card` | `business_subtype_id` | `center_understanding` | 同上 | 否 | subtype 独立 |
| `center_understanding` | `question_card` | `runtime_binding.question_type` | `main_idea` | 同上 | 否 | 运行时挂主旨大类 |
| `center_understanding` | `question_card` | `runtime_binding.business_subtype` | `center_understanding` | 同上 | 否 | 子型独立 |
| `center_understanding` | `question_card` | `structure_schema.argument_structure` | `total_sub / sub_total / parallel / problem_solution / phenomenon_analysis / example_conclusion` | 同上 | 否 | 与 main_idea type 对齐 |
| `center_understanding` | `question_card` | `structure_schema.main_axis_source` | `transition_after / final_summary / global_abstraction / solution_conclusion / example_elevation` | 同上 | 否 | 与 main_idea type 对齐 |
| `center_understanding` | `question_card` | `structure_schema.abstraction_level` | `low / medium / high` | 同上 | 否 | 与 main_idea type 对齐 |
| `center_understanding` | `question_card` | `structure_schema.distractor_types` | `detail_as_main / example_as_conclusion / scope_too_wide / scope_too_narrow / subject_shift / focus_shift` | 同上 | 否 | 与 main_idea type 对齐 |
| `center_understanding` | `question_card` | `legacy_slot_mapping.structure_type` | `argument_structure` | 同上 | 部分 | legacy 名称仍在继续使用 |
| `center_understanding` | `question_card` | `legacy_slot_mapping.main_point_source` | `main_axis_source` | 同上 | 部分 | legacy 名称仍在继续使用 |
| `center_understanding` | `question_card` | `legacy_slot_mapping.distractor_modes` | `distractor_types` | 同上 | 部分 | legacy 名称仍在继续使用 |
| `center_understanding` | `question_card` | `legacy_slot_mapping.business_family_id` | `title_selection` | 同上 | 是 | 再次把协议拉回 title_selection |
| `center_understanding` | `question_card` | `upstream_contract.required_candidate_types` | `closed_span / multi_paragraph_unit / whole_passage` | 同上 | 否 | 检索入口可用 |
| `center_understanding` | `question_card` | `upstream_contract.required_profiles` | `analysis_to_conclusion_strength / core_object / example_to_theme_strength / global_main_claim / multi_dimension_cohesion / object_scope_stability / problem_signal_strength / single_center_strength / summary_strength / turning_focus_strength / value_judgement_strength` | 同上 | 部分 | 全是 title 系 signal |
| `center_understanding` | `question_card` | `preferred_material_cards` | `title_material.turning_focus / problem_essence_judgement / multi_dimension_unification / case_to_theme_elevation / example_then_recovery / plain_main_recovery` | 同上 | 是 | 中心理解没有独立材料母族 |
| `center_understanding` | `question_card` | `base_slots.target_form` | `article_task` | 同上 | 部分 | 与 main_idea 默认值不同 |
| `center_understanding` | `question_card` | `base_slots.distractor_strength` | `high` | 同上 | 否 | 与 type 子型接近 |
| `center_understanding` | `question_card` | `base_slots.statement_visibility` | `medium` | 同上 | 是 | 与 main_idea 子型 `high` 冲突 |
| `center_understanding` | `question_card` | `generation_archetype_source` | `material_card.default_generation_archetype` | 同上 | 否 | 依赖 title material |
| `center_understanding` | `question_card` | `validator_contract.center_understanding` | 独立 validator 规则 | 同上 | 否 | 校验口径独立 |
| `center_understanding` | `question_card` | `answer_grounding.expression_fidelity_mode` | `meaning_preserving` | 同上 | 否 | 当前语义允许主旨保持型改写 |
| `main_idea` | `type yaml` | `type_id` | `main_idea` | `prompt_skeleton_service/configs/types/main_idea.yaml` | 否 | 大类正确 |
| `main_idea` | `type yaml` | `structure_schema.*` | 与 center_understanding question card 基本一致 | 同上 | 否 | 结构 schema 对齐 |
| `main_idea` | `type yaml` | `default_slots.target_form` | `central_meaning` | 同上 | 是 | 与 center_understanding question card `article_task` 不同 |
| `main_idea` | `type yaml` | `default_slots.distractor_strength` | `high` | 同上 | 否 | 与 center_understanding question card 接近 |
| `main_idea` | `type yaml` | `default_slots.statement_visibility` | `medium` | 同上 | 部分 | 默认与 question_card 一致 |
| `main_idea` | `type yaml` | `business_subtypes.center_understanding.target_form` | `article_task` | 同上 | 否 | 子型口径已改独立 |
| `main_idea` | `type yaml` | `business_subtypes.center_understanding.statement_visibility` | `high` | 同上 | 是 | 与 question_card `medium` 冲突 |
| `title_selection` | `material_card` | `business_family_id` | `title_selection` | `card_specs/normalized/material_cards/title_selection_intermediate_material_cards.normalized.yaml` | 否 | 材料母族仍是标题选择 |
| `title_selection` | `material_card` | `card_id` 列表 | `title_material.plain_main_recovery / example_then_recovery / turning_focus / multi_dimension_unification / single_object_exposition / problem_essence_judgement / counterintuitive_reversal / development_timeline / case_to_theme_elevation / value_commentary / benefit_result` | 同上 | 是 | 被 center_understanding 直接消费 |
| `title_selection` | `material_card` | `candidate_contract.allowed_candidate_types` | `whole_passage / multi_paragraph_unit / closed_span` 等按卡分配 | 同上 | 否 | 检索入口是稳定的 |
| `title_selection` | `signal_layer` | `business_family_id` | `title_selection` | `card_specs/normalized/signal_layers/title_selection_signal_layer.normalized.yaml` | 是 | center_understanding 实际复用 title signal |
| `title_selection` | `signal_layer` | `document_genre / article_purpose_frame / discourse_shape / core_object / global_main_claim / single_center_strength / summary_strength / turning_focus_strength / example_to_theme_strength / value_judgement_strength / timeline_strength / multi_dimension_cohesion / problem_signal_strength / analysis_to_conclusion_strength / candidate_type / recommended_generation_archetype / recommended_material_cards / distractor_profile` | title 系完整信号层 | 同上 | 是 | 中心理解没有自有 signal layer |

### 1.2 审计结论

- 这张卡的**题卡语义已经独立**，但**检索协议、材料卡、signal layer 仍属于 `title_selection`**。
- 当前状态适合描述为：`center_understanding` 是“独立 question card + 复用 title 族底座”的半独立卡。
- 真正的协议冲突主要有两个：
  - `business_family_id` 仍是 `title_selection`
  - `statement_visibility` 在 `question_card` 与 `type yaml` 子型之间不一致

建议保留口径：

- canonical 题卡母族语义：`center_understanding`
- canonical 运行大类：`main_idea`
- canonical 目标输出：`article_task`
- canonical 材料底座：短期兼容 `title_selection`，中期拆独立 material/signal

---

## 2. 语句填空母族

### 2.1 协议总表

| 母族语义 | 协议层 | 关键字段 | 当前值/口径 | 来源 | 是否冲突 | 备注 |
|---|---|---|---|---|---|---|
| `sentence_fill` | `question_card` | `card_id` | `question.sentence_fill.standard_v1` | `card_specs/normalized/question_cards/sentence_fill_standard_question_card.normalized.yaml` | 否 | 题卡身份稳定 |
| `sentence_fill` | `question_card` | `business_family_id` | `sentence_fill` | 同上 | 否 | 母族正确 |
| `sentence_fill` | `question_card` | `business_subtype_id` | `sentence_fill_selection` | 同上 | 否 | 子型统一 |
| `sentence_fill` | `question_card` | `structure_schema.position` | `opening / middle / ending / inserted / mixed` | 同上 | 否 | 与 type yaml 对齐 |
| `sentence_fill` | `question_card` | `structure_schema.function_type` | `summary / topic_intro / carry_previous / lead_next / bridge / reference_summary / countermeasure / conclusion` | 同上 | 是 | canonical 词表之一 |
| `sentence_fill` | `question_card` | `structure_schema.semantic_scope` | `local / sentence_level / paragraph_level` | 同上 | 否 | question/type 对齐 |
| `sentence_fill` | `question_card` | `legacy_slot_mapping.function_type` | `opening_summary -> summary`、`middle_explanation -> carry_previous`、`middle_focus_shift -> lead_next`、`ending_summary -> conclusion`、`ending_elevation -> conclusion`、`inserted_reference -> reference_summary`、`comprehensive_match -> bridge` | 同上 | 是 | 说明旧协议仍然存在 |
| `sentence_fill` | `question_card` | `upstream_contract.required_candidate_types` | `closed_span / insertion_context_unit / multi_paragraph_unit / whole_passage` | 同上 | 否 | 检索入口完整 |
| `sentence_fill` | `question_card` | `upstream_contract.required_profiles` | `abstraction_level / bidirectional_validation / blank_position / context_dependency / function_type / logic_relation / reference_dependency` | 同上 | 部分 | 名称本身稳定，但来源层不统一 |
| `sentence_fill` | `question_card` | `preferred_material_cards` | `fill_material.opening_summary / bridge_transition / middle_focus_shift / middle_explanation / ending_summary / ending_elevation / inserted_reference_match / comprehensive_multi_match` | 同上 | 是 | question card canonical 与 material card 名称不一致 |
| `sentence_fill` | `question_card` | `base_slots.blank_position` | `middle` | 同上 | 否 | 默认稳定 |
| `sentence_fill` | `question_card` | `base_slots.function_type` | `bridge` | 同上 | 否 | 默认稳定 |
| `sentence_fill` | `question_card` | `base_slots.logic_relation` | `continuation` | 同上 | 否 | 默认稳定 |
| `sentence_fill` | `question_card` | `material_card_overrides` | `opening_summary -> summary`、`middle_focus_shift -> lead_next`、`middle_explanation -> carry_previous`、`ending_summary -> conclusion`、`ending_elevation -> conclusion`、`inserted_reference_match -> reference_summary`、`comprehensive_multi_match -> bridge` | 同上 | 是 | question card 每次都在帮 material card 翻译 |
| `sentence_fill` | `question_card` | `generation_archetypes` | `opening_summary / bridge_transition / middle_focus_shift / middle_explanation / ending_summary / ending_elevation / inserted_reference_match / comprehensive_multi_match` | 同上 | 是 | archetype 名仍沿旧 material 体系 |
| `sentence_fill` | `type yaml` | `type_id` | `sentence_fill` | `prompt_skeleton_service/configs/types/sentence_fill.yaml` | 否 | 类型正确 |
| `sentence_fill` | `type yaml` | `structure_schema.function_type` | `summary / topic_intro / carry_previous / lead_next / bridge / reference_summary / countermeasure / conclusion` | 同上 | 否 | 与 question_card 对齐 |
| `sentence_fill` | `type yaml` | `legacy_slot_mapping.function_type` | 与 question_card 同步保留旧名映射 | 同上 | 是 | 再次保留多词表 |
| `sentence_fill` | `type yaml` | `slot_schema.logic_relation` | `continuation / transition / explanation / focus_shift / summary / action / elevation / reference_match / multi_constraint` | 同上 | 部分 | 与 material/signal 的命名也非完全同一层级 |
| `sentence_fill` | `type yaml` | `default_slots.function_type` | `bridge` | 同上 | 否 | 默认稳定 |
| `sentence_fill` | `type yaml` | `default_slots.distractor_strength` | `high` | 同上 | 否 | 生成默认更强 |
| `sentence_fill` | `material_card` | `business_family_id` | `sentence_fill` | `card_specs/normalized/material_cards/sentence_fill_intermediate_material_cards.normalized.yaml` | 否 | 材料母族正确 |
| `sentence_fill` | `material_card` | `card_id` 列表 | `fill_material.opening_summary / bridge_transition / middle_focus_shift / middle_explanation / ending_summary / ending_elevation / inserted_reference_match / comprehensive_multi_match` | 同上 | 否 | 材料卡命名稳定，但与 question canonical 不同 |
| `sentence_fill` | `material_card` | `required_signals.function_type` | `opening_summary / bridge / middle_focus_shift / middle_explanation / ending_summary / ending_elevation / inserted_reference / comprehensive_match` | 同上 | 是 | 这是第二套 function_type 词表 |
| `sentence_fill` | `material_card` | `required_signals.logic_relation` | `summary / continuation / focus_shift / explanation / summary / elevation / reference_match / multi_constraint` | 同上 | 部分 | 名称能对上，但含义层级混杂 |
| `sentence_fill` | `material_card` | `candidate_contract.allowed_candidate_types` | `whole_passage / closed_span / multi_paragraph_unit / insertion_context_unit` 按卡分配 | 同上 | 否 | 检索口径可用 |
| `sentence_fill` | `signal_layer` | `business_family_id` | `sentence_fill` | `card_specs/normalized/signal_layers/sentence_fill_signal_layer.normalized.yaml` | 否 | 母族正确 |
| `sentence_fill` | `signal_layer` | `blank_position` | `opening / middle / ending / inserted / mixed` | 同上 | 否 | 与 question/type 对齐 |
| `sentence_fill` | `signal_layer` | `function_type` | `opening_summary / bridge / middle_explanation / middle_focus_shift / ending_summary / ending_elevation / inserted_reference / comprehensive_match` | 同上 | 是 | 这是第三套 function_type 词表 |
| `sentence_fill` | `signal_layer` | `logic_relation` | `continuation / transition / explanation / focus_shift / summary / elevation / reference_match / multi_constraint` | 同上 | 否 | 相对稳定 |
| `sentence_fill` | `signal_layer` | `recommended_candidate_types` | `whole_passage / closed_span / multi_paragraph_unit / insertion_context_unit` | 同上 | 否 | 可作为 canonical 候选类型 |
| `sentence_fill` | `resolver` | `meta.slot_role` | `opening / middle / ending` | `passage_service/app/services/main_card_signal_resolver.py` | 是 | 又引入 slot_role 维度 |
| `sentence_fill` | `resolver` | `meta.slot_function` | `summary / topic_intro / carry_previous / lead_next / bridge_both_sides / ending_summary / countermeasure` | 同上 | 是 | 第四套功能词表 |
| `sentence_fill` | `resolver` | `meta.function_type` | `summarize_following_text / topic_introduction / carry_previous / lead_next / bridge_both_sides / propose_countermeasure / summarize_previous_text` | 同上 | 是 | 第五套功能词表 |
| `sentence_fill` | `pipeline` | `_normalize_fill_function_type()` | `summarize_following_text -> opening_summary`、`topic_introduction -> opening_summary`、`summarize_previous_text -> ending_summary`、`propose_countermeasure -> ending_summary`、`carry_previous -> middle_explanation`、`lead_next -> middle_focus_shift`、`bridge_both_sides -> bridge`、`reference_summary -> inserted_reference` | `passage_service/app/services/material_pipeline_v2.py` | 是 | 运行时又做一次强制归一 |
| `sentence_fill` | `pipeline` | `_sentence_fill_business_function()` | `("opening","summary") -> summarize_following_text` 等 | 同上 | 是 | 业务卡映射使用的是 resolver 那套词表 |
| `sentence_fill` | `pipeline` | `_explicit_fill_function_type()` | `("middle","lead_next") -> bridge`、`("middle","bridge_both_sides") -> bridge` 等 | 同上 | 是 | 这里甚至存在信息压缩 |

### 2.2 审计结论

- 这张卡当前不是“一个协议”，而是**至少五种 function_type 方言并存**：
  - question card canonical
  - type yaml canonical
  - material card names
  - signal layer names
  - resolver / pipeline runtime names
- 它已经不是“命名不优雅”的问题，而是**直接影响检索命中、材料字段匹配、业务卡映射和后续蒸馏清洁度**。
- 你前面说的“生题时发出的材料期望和本地材料字段不一致”，这张卡就是最核心的爆点。

建议保留口径：

- canonical 位置字段：`blank_position`
- canonical 功能字段：`function_type`
- canonical 功能值建议收成 question/type 这套：
  - `summary`
  - `topic_intro`
  - `carry_previous`
  - `lead_next`
  - `bridge`
  - `reference_summary`
  - `countermeasure`
  - `conclusion`
- material/signal/resolver 全部做单向映射到这套 canonical，不再多向互转

---

## 3. 语句排序母族

### 3.1 协议总表

| 母族语义 | 协议层 | 关键字段 | 当前值/口径 | 来源 | 是否冲突 | 备注 |
|---|---|---|---|---|---|---|
| `sentence_order` | `question_card` | `card_id` | `question.sentence_order.standard_v1` | `card_specs/normalized/question_cards/sentence_order_standard_question_card.normalized.yaml` | 否 | 标准题卡存在 |
| `sentence_order` | `question_card` | `business_family_id` | `sentence_order` | 同上 | 否 | 母族正确 |
| `sentence_order` | `question_card` | `formal_runtime_spec.primary_business_card_id` | `sentence_order__six_sentence_role_chain__abstract` | 同上 | 否 | 主业务卡明确 |
| `sentence_order` | `question_card` | `formal_runtime_spec.sentence_count` | `fixed=6` | 同上 | 否 | 主协议明确 |
| `sentence_order` | `question_card` | `reject_non_six_sentence_sequences` | `true` | 同上 | 否 | 主协议明确 |
| `sentence_order` | `question_card` | `structure_schema.sentence_roles` | `thesis / definition / explanation / reasoning / transition / example / conclusion / countermeasure` | 同上 | 否 | 结构 schema 稳定 |
| `sentence_order` | `question_card` | `structure_schema.head_constraints` | `must_not_have backward_reference / mid_transition; preferred thesis / definition` | 同上 | 否 | 稳定 |
| `sentence_order` | `question_card` | `structure_schema.tail_constraints` | `must_have closure; preferred conclusion / countermeasure` | 同上 | 否 | 稳定 |
| `sentence_order` | `question_card` | `legacy_slot_mapping` | `opening_anchor_type / closing_anchor_type / local_binding_strength / middle_structure_type / block_order_complexity` | 同上 | 部分 | legacy anchor 口径仍在使用 |
| `sentence_order` | `question_card` | `preferred_business_cards` | primary/supporting/weak_legacy 三层 | 同上 | 部分 | 业务卡层已经分层，但 legacy 仍在 |
| `sentence_order` | `question_card` | `upstream_contract.required_candidate_types` | `sentence_block_group` | 同上 | 否 | 这是主协议应保留的候选类型 |
| `sentence_order` | `question_card` | `preferred_material_cards` | `order_material.dual_anchor_lock / carry_parallel_expand / viewpoint_reason_action / problem_solution_case_blocks` | 同上 | 否 | 主线材料卡清晰 |
| `sentence_order` | `question_card` | `base_slots.sentence_count` | `6` | 同上 | 否 | 稳定 |
| `sentence_order` | `question_card` | `base_slots.distractor_strength` | `medium` | 同上 | 是 | 与 type yaml `high` 冲突 |
| `sentence_order` | `question_card` | `slot_extensions.allowed_sortable_unit_counts` | `[6]` | 同上 | 否 | 稳定 |
| `sentence_order` | `question_card` | `slot_extensions.block_group_primary_only` | `true` | 同上 | 否 | 稳定 |
| `sentence_order` | `question_card` | `material_card_overrides.order_material.phrase_order_variant` | 仍有 override | 同上 | 是 | 旁支协议仍挂在标准题卡里 |
| `sentence_order` | `question_card` | `generation_archetypes.phrase_order_variant` | 仍存在 | 同上 | 是 | 说明标准卡内部仍混着变体 |
| `sentence_order` | `question_card` | `validator_contract.sentence_count` | `fixed=6` | 同上 | 否 | 校验主协议一致 |
| `sentence_order` | `type yaml` | `type_id` | `sentence_order` | `prompt_skeleton_service/configs/types/sentence_order.yaml` | 否 | 类型正确 |
| `sentence_order` | `type yaml` | `formal_runtime_spec.primary_business_card_id` | `sentence_order__six_sentence_role_chain__abstract` | 同上 | 否 | 与 question_card 一致 |
| `sentence_order` | `type yaml` | `formal_runtime_spec.sentence_count` | `fixed=6` | 同上 | 否 | 与 question_card 一致 |
| `sentence_order` | `type yaml` | `structure_schema.*` | 与 question_card 基本一致 | 同上 | 否 | 结构 schema 对齐 |
| `sentence_order` | `type yaml` | `default_slots.sentence_count` | `6` | 同上 | 否 | 一致 |
| `sentence_order` | `type yaml` | `default_slots.distractor_strength` | `high` | 同上 | 是 | 与 question_card `medium` 冲突 |
| `sentence_order` | `type yaml` | `validator_contract.sentence_count` | `fixed=6` | 同上 | 否 | 主协议一致 |
| `sentence_order` | `type yaml` | `patterns.phrase_order_variant.enabled` | `false` | 同上 | 部分 | type 层禁用了旁支，但其他层没删 |
| `sentence_order` | `material_card` | `business_family_id` | `sentence_order` | `card_specs/normalized/material_cards/sentence_order_intermediate_material_cards.normalized.yaml` | 否 | 母族正确 |
| `sentence_order` | `material_card` | `card_id` 列表 | `order_material.dual_anchor_lock / carry_parallel_expand / viewpoint_reason_action / problem_solution_case_blocks / phrase_order_variant` | 同上 | 部分 | 主线 + 旁支并存 |
| `sentence_order` | `material_card` | `candidate_contract.allowed_candidate_types` | 前四张卡是 `sentence_block_group`，phrase 变体是 `phrase_or_clause_group` | 同上 | 是 | 同一母族下混了两种候选协议 |
| `sentence_order` | `signal_layer` | `business_family_id` | `sentence_order` | `card_specs/normalized/signal_layers/sentence_order_signal_layer.normalized.yaml` | 否 | 母族正确 |
| `sentence_order` | `signal_layer` | `opening_anchor_type / middle_structure_type / closing_anchor_type / block_order_complexity / sequence_integrity / unique_opener_score / binding_pair_count / exchange_risk / function_overlap_score / multi_path_risk / discourse_progression_strength / context_closure_score / temporal_order_strength / action_sequence_irreversibility / phrase_order_salience` | 排序题完整信号层 | 同上 | 否 | 主线信号稳定 |
| `sentence_order` | `signal_layer` | `candidate_type` | `sentence_block_group / phrase_or_clause_group` | 同上 | 是 | signal 层仍认可 phrase 旁支 |
| `sentence_order` | `signal_layer` | `recommended_generation_archetype` | `dual_anchor_lock / carry_parallel_expand / viewpoint_reason_action / problem_solution_case_blocks / phrase_order_variant` | 同上 | 是 | 旁支仍是正式推荐项之一 |

### 3.2 审计结论

- 这张卡的主协议其实已经很清楚：**正式运行口径就是固定 6 句、`sentence_block_group`、角色链排序**。
- 真正的问题不在主协议，而在于**旁支 phrase/clause variant 仍然挂在标准题卡、材料卡、signal layer 里**。
- 因为 type yaml 只是把它 `enabled: false`，并没有把旁支从母族协议里真正切出去，所以这张卡目前仍然是“主协议清晰，但标准卡内部未彻底净化”。

建议保留口径：

- canonical 主协议：
  - `sentence_count = 6`
  - `candidate_type = sentence_block_group`
  - `primary_business_card_id = sentence_order__six_sentence_role_chain__abstract`
- `phrase_order_variant` 要么拆成独立子母族，要么完全降为非标准旁支，不应继续挂在标准题卡中

---

## 4. 三大母族横向结论

### 4.1 当前最严重的协议问题排序

1. `sentence_fill`
   - 多词表并存
   - 多层多向互转
   - 已直接影响检索和业务使用

2. `center_understanding`
   - 题卡语义已独立
   - 但底层仍借 `title_selection`
   - 导致检索与统计口径不干净

3. `sentence_order`
   - 主协议其实很清楚
   - 问题是标准母族里还挂着不兼容旁支

4. 共同问题
   - `type yaml` 与 `question_card` 默认槽位不是总一致
   - legacy 字段仍广泛存在
   - “规范层”和“运行层”之间仍有翻译桥，而不是单协议直达

### 4.2 现阶段最值得冻结的 canonical 字段

如果只先收这三大卡，建议先冻结下面这些字段为唯一协议：

- `question_card.card_id`
- `question_card.business_family_id`
- `question_card.business_subtype_id`
- `runtime_binding.question_type`
- `runtime_binding.business_subtype`
- `upstream_contract.required_candidate_types`
- `upstream_contract.preferred_material_cards`
- `structure_schema.*`
- `base_slots.*`
- `validator_contract.*`
- `material_card.card_id`
- `material_card.candidate_contract.allowed_candidate_types`
- `signal_layer.business_family_id`
- `signal_layer.signal_id`
- `signal_layer.allowed_values`

其中最需要优先统一的专属字段：

- `center_understanding`
  - `business_family_id`
  - `statement_visibility`

- `sentence_fill`
  - `function_type`
  - `logic_relation`
  - `blank_position`

- `sentence_order`
  - `sentence_count`
  - `candidate_type`
  - `recommended_generation_archetype`

### 4.3 一句话判断

你现在的系统不是没有题卡标准，而是：

**已经有题卡骨架，但三大母族仍然存在“语义已分、底层未分”“主协议已定、旁支未清”“规范协议和运行协议并存”的问题。**

如果不先把这三大卡收成单协议，后面的题卡驱动扩展和蒸馏都会持续被兼容噪音污染。
