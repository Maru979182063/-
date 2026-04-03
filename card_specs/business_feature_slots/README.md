# 业务特征卡插槽规范

这套文件的目标不是替代现有 V2，而是给业务侧教材特征卡加一层可直接接入的插槽规范。

推荐关系：

- `V2` 继续作为底层通用特征层
- `business_feature_slot` 作为上层业务特征卡层
- 一张业务特征卡只强绑定一个 `mother_family_id`
- 后续清洗数据时，直接按这里的结构产出 YAML，再投到对应母族的 `type_slots`

## 文件结构

- `templates/business_feature_card.template.yaml`
  通用模板
- `examples/cause_effect_conclusion_focus.main_idea.yaml`
  按“因果关系，结论是重点”做好的示例卡
- `examples/sentence_fill_position_function.abstract.yaml`
  按“语句填空题”教材卡抽象出的母族级业务卡
- `examples/sentence_order_head_tail_logic.abstract.yaml`
  按“语句排序题”教材卡抽象出的母族级业务卡

## 核心原则

- `feature_signature`
  描述业务真正关心的篇章特征
- `retrieval_profile`
  描述本地检索时的硬过滤、软排序和降级策略
- `slot_projection`
  描述如何把业务特征卡投影到当前项目的母族插槽
- `canonical_projection`
  描述它和底层 V2 / universal profile 之间的对应关系

如果一张业务卡本身带多个稳定分支，可以使用：

- `slot_projection.type_slots`
  该卡整体默认投影
- `slot_projection.slot_strategy_map`
  该卡内部不同分支的子策略投影

## 母族与插槽对齐

`mother_family_id=main_idea`

- `structure_type`
- `main_point_source`
- `abstraction_level`
- `coverage_requirement`
- `target_form`
- `title_style`
- `distractor_modes`
- `distractor_strength`
- `statement_visibility`

`mother_family_id=continuation`

- `anchor_focus`
- `continuation_type`
- `progression_mode`
- `ending_function`
- `anchor_clarity`
- `option_confusion`
- `distractor_modes`

`mother_family_id=sentence_order`

- `opening_anchor_type`
- `opening_signal_strength`
- `middle_structure_type`
- `local_binding_strength`
- `closing_anchor_type`
- `closing_signal_strength`
- `block_order_complexity`
- `distractor_modes`
- `distractor_strength`

`mother_family_id=sentence_fill`

- `blank_position`
- `function_type`
- `logic_relation`
- `context_dependency`
- `bidirectional_validation`
- `reference_dependency`
- `abstraction_level`
- `distractor_modes`
- `distractor_strength`

## 清洗接入建议

1. 每张业务特征卡先定 `mother_family_id`
2. 先填 `feature_signature`
3. 再填 `retrieval_profile`
4. 最后只把该母族允许的字段写进 `slot_projection.type_slots`

如果业务卡内部已经天然分叉，例如“开头/中间/结尾”或“首句/尾句/捆绑/顺序”，
就把这些分支写进 `slot_projection.slot_strategy_map`。

## 防止筛光文段

- `hard_filters` 只保留 2 到 4 个最关键条件
- 其他条件尽量放进 `soft_filters`
- `unknown` 不能等于 `false`
- `fallback_policy` 必须允许逐级放宽

## 推荐落地口径

一句话可以直接内部统一：

`V2 保底，业务特征卡提纯，slot_projection 负责接入当前母族。`
