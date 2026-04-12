# 三大母族协议重评估与冻结清单（2026-04-12）

## 评估范围

- `center_understanding / main_idea`
- `sentence_fill`
- `sentence_order`

对比基线：

- [three_family_card_protocol_audit.md](C:\Users\Maru\Documents\agent\docs\three_family_card_protocol_audit.md)

本次只看协议、字段、运行时桥接和质量风险，不展开产品策略。

---

## 一、对比上次，已经明显改善的地方

### 1. `center_understanding` 已经从“假借 title_selection 母族”收成了“独立母族 + 兼容骨架”

当前：

- `business_family_id: center_understanding`
- 新增 `compatibility_backbone.material_signal_family_id: title_selection`
- 新增 `compatibility_backbone.answer_grounding_asset_family_id: title_selection`

这意味着：

- 题卡主身份已经独立
- 旧 title 系底座被显式降级为“兼容骨架”，不再冒充主协议

来源：

- [center_understanding_standard_question_card.normalized.yaml](C:\Users\Maru\Documents\agent\card_specs\normalized\question_cards\center_understanding_standard_question_card.normalized.yaml#L4)

### 2. `center_understanding` 的 `statement_visibility` 漂移已经收敛

上次问题：

- `question_card.base_slots.statement_visibility = medium`
- `main_idea` 中 `center_understanding` 子型是 `high`

当前：

- `question_card.base_slots.statement_visibility = high`

这说明主卡默认槽位已经更接近运行时子型口径。

来源：

- [center_understanding_standard_question_card.normalized.yaml](C:\Users\Maru\Documents\agent\card_specs\normalized\question_cards\center_understanding_standard_question_card.normalized.yaml#L89)
- [main_idea.yaml](C:\Users\Maru\Documents\agent\prompt_skeleton_service\configs\types\main_idea.yaml#L243)

### 3. `sentence_fill` 的 canonical function vocabulary 基本立住了

这次最大的进步在这里。

当前统一情况：

- `question_card.structure_schema.function_type`
- `type yaml.structure_schema.function_type`
- `signal_layer.function_type.allowed_values`
- `resolver` 的返回 schema

都已经收敛到：

- `summary`
- `topic_intro`
- `carry_previous`
- `lead_next`
- `bridge`
- `reference_summary`
- `countermeasure`
- `conclusion`

来源：

- [sentence_fill_standard_question_card.normalized.yaml](C:\Users\Maru\Documents\agent\card_specs\normalized\question_cards\sentence_fill_standard_question_card.normalized.yaml#L19)
- [sentence_fill.yaml](C:\Users\Maru\Documents\agent\prompt_skeleton_service\configs\types\sentence_fill.yaml#L51)
- [sentence_fill_signal_layer.normalized.yaml](C:\Users\Maru\Documents\agent\card_specs\normalized\signal_layers\sentence_fill_signal_layer.normalized.yaml#L18)
- [main_card_signal_resolver.py](C:\Users\Maru\Documents\agent\passage_service\app\services\main_card_signal_resolver.py#L473)

### 4. `sentence_fill` 已经补上统一 helper，桥接开始中心化

现在 `prompt_skeleton_service` 已经有统一协议辅助文件：

- [sentence_fill_protocol.py](C:\Users\Maru\Documents\agent\prompt_skeleton_service\app\services\sentence_fill_protocol.py#L48)

它会从 `sentence_fill.yaml` 动态读取：

- canonical `blank_position`
- canonical `function_type`
- canonical `logic_relation`
- legacy 映射

这说明桥接逻辑不再散落在多个 prompt 文件里硬编码，质量风险下降很多。

### 5. `sentence_order` 的标准卡已经真正去掉了 phrase 旁支

上次问题：

- 标准题卡里还挂着 `order_material.phrase_order_variant`
- signal layer 也还接受 `phrase_or_clause_group`

当前：

- 标准题卡的 `preferred_material_cards` 只保留 4 张主卡
- 标准题卡 `material_card_overrides` 已不再包含 phrase 变体
- signal layer 的 `candidate_type.allowed_values` 只剩 `sentence_block_group`
- signal layer `recommended_generation_archetype` 只剩 4 个主 archetype
- material cards 也只剩 4 张主卡

来源：

- [sentence_order_standard_question_card.normalized.yaml](C:\Users\Maru\Documents\agent\card_specs\normalized\question_cards\sentence_order_standard_question_card.normalized.yaml#L101)
- [sentence_order_intermediate_material_cards.normalized.yaml](C:\Users\Maru\Documents\agent\card_specs\normalized\material_cards\sentence_order_intermediate_material_cards.normalized.yaml#L9)
- [sentence_order_signal_layer.normalized.yaml](C:\Users\Maru\Documents\agent\card_specs\normalized\signal_layers\sentence_order_signal_layer.normalized.yaml#L143)

### 6. `sentence_order` 的 type yaml 与 question card 默认强度已经对齐

上次问题：

- `question_card.base_slots.distractor_strength = medium`
- `type yaml.default_slots.distractor_strength = high`

当前：

- 两边都已经是 `medium`

来源：

- [sentence_order_standard_question_card.normalized.yaml](C:\Users\Maru\Documents\agent\card_specs\normalized\question_cards\sentence_order_standard_question_card.normalized.yaml#L124)
- [sentence_order.yaml](C:\Users\Maru\Documents\agent\prompt_skeleton_service\configs\types\sentence_order.yaml#L292)

---

## 二、现在仍然存在的关键问题

### 1. `center_understanding` 仍然没有独立 material/signal 母座，只是把依赖显式化了

这次已经从“协议污染”收成了“兼容声明”，这是进步；但本质上：

- `upstream_contract.required_profiles` 还是 title 系 signal
- `preferred_material_cards` 还是 title 系 material
- `compatibility_backbone` 还是强依赖 title_selection

所以当前状态不是“完全独立”，而是：

**独立 question card + 显式兼容 material/signal backbone**

这在现阶段可以接受，但还不适合把它当成完全独立母族去做纯净蒸馏。

来源：

- [center_understanding_standard_question_card.normalized.yaml](C:\Users\Maru\Documents\agent\card_specs\normalized\question_cards\center_understanding_standard_question_card.normalized.yaml#L9)
- [center_understanding_standard_question_card.normalized.yaml](C:\Users\Maru\Documents\agent\card_specs\normalized\question_cards\center_understanding_standard_question_card.normalized.yaml#L53)

### 2. `center_understanding` 仍存在局部槽位回退到旧可见度的情况

虽然 `base_slots.statement_visibility` 已经收成 `high`，但某些 material override 仍写回 `medium`，例如：

- `title_material.turning_focus -> statement_visibility: medium`

这不是协议级灾难，但会让题卡默认口径和某些材料卡局部表现发生偏移。

来源：

- [center_understanding_standard_question_card.normalized.yaml](C:\Users\Maru\Documents\agent\card_specs\normalized\question_cards\center_understanding_standard_question_card.normalized.yaml#L109)

### 3. `sentence_fill` 的主协议基本统一了，但 business-function 方言还没完全退场

虽然 canonical `function_type` 已经统一，但以下旧业务语言仍然存在：

- `summarize_following_text`
- `summarize_previous_text`
- `bridge_both_sides`
- `topic_introduction`
- `propose_countermeasure`

它们现在主要还活在：

- `legacy_slot_mapping`
- 业务 feature slot 示例
- 运行时 expected profile / business function 评分逻辑

这说明你已经完成了“规范层统一”，但**业务卡层和评分层仍然是双语系统**。

来源：

- [sentence_fill_standard_question_card.normalized.yaml](C:\Users\Maru\Documents\agent\card_specs\normalized\question_cards\sentence_fill_standard_question_card.normalized.yaml#L45)
- [sentence_fill.yaml](C:\Users\Maru\Documents\agent\prompt_skeleton_service\configs\types\sentence_fill.yaml#L80)
- [material_pipeline_v2.py](C:\Users\Maru\Documents\agent\passage_service\app\services\material_pipeline_v2.py#L5190)

### 4. `sentence_fill` 的 archetype/material 命名仍是旧名系

目前 canonical `function_type` 已经变成 `summary / topic_intro / carry_previous / ...`，  
但材料卡和 archetype 名还是：

- `opening_summary`
- `bridge_transition`
- `middle_focus_shift`
- `middle_explanation`
- `ending_summary`
- `ending_elevation`
- `inserted_reference_match`
- `comprehensive_multi_match`

这本身不是错误，但意味着：

- 题卡协议语言
- 材料卡/archetype 命名语言

仍然不是一套字面协议。

如果你后面要做高质量蒸馏，这会让“标签名”和“运行名”长期分离。

来源：

- [sentence_fill_standard_question_card.normalized.yaml](C:\Users\Maru\Documents\agent\card_specs\normalized\question_cards\sentence_fill_standard_question_card.normalized.yaml#L75)
- [sentence_fill_intermediate_material_cards.normalized.yaml](C:\Users\Maru\Documents\agent\card_specs\normalized\material_cards\sentence_fill_intermediate_material_cards.normalized.yaml#L9)

### 5. `sentence_order` 标准协议已经很干净，但 passage 运行时仍残留旧 opening/closing rule 方言

标准卡和 type yaml 已经统一成：

- `opening_anchor_type: explicit_topic / upper_context_link / viewpoint_opening / problem_opening / weak_opening / none`
- `closing_anchor_type: conclusion / summary / call_to_action / case_support / none`

但 `passage_service` 的 resolver / scoring 里仍然有旧口径：

- `definition_opening`
- `background_opening`
- `explicit_opening`
- `summary_or_conclusion`

这说明 `sentence_order` 的**配置协议已经收口，但运行时评分协议还没完全跟上**。

来源：

- [main_card_signal_resolver.py](C:\Users\Maru\Documents\agent\passage_service\app\services\main_card_signal_resolver.py#L507)
- [material_pipeline_v2.py](C:\Users\Maru\Documents\agent\passage_service\app\services\material_pipeline_v2.py#L5676)
- [sentence_order_standard_question_card.normalized.yaml](C:\Users\Maru\Documents\agent\card_specs\normalized\question_cards\sentence_order_standard_question_card.normalized.yaml#L113)

### 6. `sentence_order` 运行时 candidate_type 仍有实现层别名

标准协议现在已经冻结成：

- `candidate_type = sentence_block_group`

但 `passage_service` 的实际运行时内部还存在：

- `ordered_unit_group`
- `weak_formal_order_group`

这类实现层候选类型。

如果它们只是内部中间态还好；但如果被外溢到持久化、评测或训练集，就会重新污染协议。

来源：

- [material_pipeline_v2.py](C:\Users\Maru\Documents\agent\passage_service\app\services\material_pipeline_v2.py#L1736)
- [material_pipeline_v2.py](C:\Users\Maru\Documents\agent\passage_service\app\services\material_pipeline_v2.py#L1989)

---

## 三、与上次报告相比，变化总结

### 已修复或明显改善

1. `center_understanding.business_family_id` 已改正  
2. `center_understanding` 不再隐式冒充 `title_selection`，而是显式兼容  
3. `center_understanding.statement_visibility` 主槽位已对齐  
4. `sentence_fill` 的 canonical `function_type` 已跨层统一  
5. `sentence_fill` 已有统一协议 helper  
6. `sentence_order` 标准卡已去除 phrase 旁支  
7. `sentence_order` signal layer 已去除 `phrase_or_clause_group`  
8. `sentence_order` 默认 `distractor_strength` 已对齐

### 仍需注意

1. `center_understanding` 还不是独立 material/signal 母座  
2. `sentence_fill` 仍是“canonical 层 + business-function 层”双语系统  
3. `sentence_fill` 材料卡/archetype 命名还没换到 canonical 口径  
4. `sentence_order` passage 运行时仍残留旧 anchor/rule 枚举  
5. `sentence_order` passage 运行时仍保留内部 candidate_type 别名

---

## 四、冻结清单

下面这份是我建议你现在就冻结的 **canonical 协议**。

原则：

- **对业务外协议冻结**
- **对训练样本口径冻结**
- **对审核页/落库字段冻结**
- 允许内部实现保留桥接字段，但不得外溢为主协议

### 4.1 `center_understanding` 冻结清单

#### 必须冻结为 canonical 的字段

- `question_card.card_id = question.center_understanding.standard_v1`
- `question_card.business_family_id = center_understanding`
- `question_card.business_subtype_id = center_understanding`
- `runtime_binding.question_type = main_idea`
- `runtime_binding.business_subtype = center_understanding`
- `structure_schema.argument_structure`
- `structure_schema.main_axis_source`
- `structure_schema.abstraction_level`
- `structure_schema.distractor_types`
- `base_slots.target_form = article_task`
- `base_slots.statement_visibility = high`
- `base_slots.distractor_strength = high`
- `validator_contract.center_understanding.*`

#### 允许保留但必须降级为兼容层的字段

- `compatibility_backbone.material_signal_family_id = title_selection`
- `compatibility_backbone.answer_grounding_asset_family_id = title_selection`
- `preferred_material_cards = title_material.*`
- `required_profiles = title_selection signal subset`

#### 质量注意项

- `material_card_overrides` 中如继续局部写回 `statement_visibility: medium`，要明确标记为“材料特例”，不能回流覆盖标准题卡默认槽位。

### 4.2 `sentence_fill` 冻结清单

#### 必须冻结为 canonical 的字段

- `question_card.card_id = question.sentence_fill.standard_v1`
- `business_family_id = sentence_fill`
- `structure_schema.blank_position`
- `structure_schema.function_type`
- `slot_schema.logic_relation`
- `base_slots.blank_position`
- `base_slots.function_type`
- `base_slots.logic_relation`
- `signal_layer.function_type.allowed_values`
- `signal_layer.logic_relation.allowed_values`
- `resolver.function_type enum`
- `resolver.logic_relation enum`

#### canonical function_type 唯一值

- `summary`
- `topic_intro`
- `carry_previous`
- `lead_next`
- `bridge`
- `reference_summary`
- `countermeasure`
- `conclusion`

#### canonical blank_position 唯一值

- `opening`
- `middle`
- `ending`
- `inserted`
- `mixed`

#### canonical logic_relation 唯一值

- `continuation`
- `transition`
- `explanation`
- `focus_shift`
- `summary`
- `action`
- `elevation`
- `reference_match`
- `multi_constraint`

#### 允许保留但只能作为 legacy alias 的字段

- `opening_summary`
- `middle_explanation`
- `middle_focus_shift`
- `bridge_both_sides`
- `ending_summary`
- `ending_elevation`
- `inserted_reference`
- `comprehensive_match`
- `summarize_following_text`
- `topic_introduction`
- `summarize_previous_text`
- `propose_countermeasure`

#### 冻结规则

- 所有外部输入先过 [sentence_fill_protocol.py](C:\Users\Maru\Documents\agent\prompt_skeleton_service\app\services\sentence_fill_protocol.py#L48)
- 所有持久化输出、训练导出、审核展示、运行时快照一律写 canonical 值
- legacy 值只允许存在于：
  - `legacy_slot_mapping`
  - 旧业务卡导入兼容层
  - 历史样本回放兼容层

#### 质量注意项

- 材料卡名和 archetype 名短期可不改，但训练导出时必须增加一个 canonical 投影字段，例如：
  - `canonical_function_type`
  - `canonical_blank_position`
  - `canonical_logic_relation`

### 4.3 `sentence_order` 冻结清单

#### 必须冻结为 canonical 的字段

- `question_card.card_id = question.sentence_order.standard_v1`
- `business_family_id = sentence_order`
- `formal_runtime_spec.primary_business_card_id = sentence_order__six_sentence_role_chain__abstract`
- `formal_runtime_spec.sortable_unit_count = 6`
- `formal_runtime_spec.sortable_unit_sentence_span = {min:1,max:2}`
- `formal_runtime_spec.sortable_unit_type = sentence_block`
- `formal_runtime_spec.candidate_type = sentence_block_group`
- `formal_runtime_spec.reject_non_six_unit_sequences = true`
- `structure_schema.sentence_roles`
- `structure_schema.head_constraints`
- `structure_schema.tail_constraints`
- `structure_schema.ordering_logic`
- `signal_layer.candidate_type.allowed_values = [sentence_block_group]`
- `default_slots.distractor_strength = medium`
- `validator_contract.sentence_order.sortable_unit_count = 6`

#### 必须冻结的 archetype 列表

- `dual_anchor_lock`
- `carry_parallel_expand`
- `viewpoint_reason_action`
- `problem_solution_case_blocks`

#### 必须废止为标准协议外的内容

- `phrase_order_variant`
- `phrase_or_clause_group`

#### 允许保留但必须限制在运行时内部的实现字段

- `ordered_unit_group`
- `weak_formal_order_group`
- `definition_opening`
- `background_opening`
- `explicit_opening`
- `summary_or_conclusion`

#### 冻结规则

- 对外协议、训练集、审核页、落库一律只认：
  - `sentence_block_group`
  - `sortable_unit_count = 6`
  - 4 个标准 archetype
- passage 内部如果继续保留 `ordered_unit_group / weak_formal_order_group`，必须：
  - 不出现在外部 API
  - 不出现在训练导出
  - 不出现在 question snapshot 主字段

#### 质量注意项

- 运行时 `opening_rule / closing_rule` 词表必须在下一轮尽快和标准卡 anchor 词表对齐，否则评测解释和卡协议仍可能跑偏。

---

## 五、质量优先级建议

如果你下一步要按“质量也很重要”的原则来收口，我建议按这个顺序：

1. **先冻结，不先大改**
   - 先把 canonical 字段和 alias 边界写死
   - 避免继续边改边长新方言

2. **先修运行时外溢，不先修内部实现**
   - 优先保证外部 API、审核页、落库、训练导出只吐 canonical
   - 内部实现层的桥接可暂留

3. **先修 `sentence_order` 运行时旧 rule 枚举**
   - 因为它最接近“只差最后一层桥接”

4. **再修 `sentence_fill` business-function 双语问题**
   - 这一步最影响后续蒸馏质量

5. **最后再拆 `center_understanding` 独立 material/signal 母座**
   - 这一步价值高，但不必抢在前两步前面

---

## 六、一句话结论

和上次比，这次已经不是“协议分裂严重、不能谈冻结”的状态了。  
现在三大卡里：

- `center_understanding`：已经变成**独立母族 + 兼容骨架**
- `sentence_fill`：已经变成**canonical 协议基本成型，但 business-function 仍双语**
- `sentence_order`：已经变成**标准协议很干净，只剩运行时旧枚举尾巴**

也就是说：

**现在可以正式开始冻结，而且冻结的时机已经比上次成熟很多。**
