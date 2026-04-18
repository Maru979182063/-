# 深度二修复链路定义

## 1. 目的

本文定义一条面向 `passage_service` 的三层材料处理链路：

1. 深度一：先识别“对目标主卡 / 孙族卡有明显倾向”的候选文段。
2. 深度二：只对“有倾向但未过正式消费门槛”的候选做精细修复与特征放大。
3. 深度三：使用与正式材料一致的判卡 / 打分链路重新复判；若仍失败，则标记为废弃材料。

这条链路的目标不是制造新材料，而是救回“原本就接近可用、但因粗清洗、切段、版式噪声或边界不清而失败”的 near-miss 材料。

## 2. 基本原则

- 深度二不是自由改写器，而是受约束的 repair layer。
- 深度二只能做“精细清洗、轻度收束、特征显化”，不能改变文段主旨、结构主轴和关键事实。
- 深度二只能救“深度一已识别出目标倾向”的材料，不能把无关材料硬修成目标卡。
- 深度三必须复用正式链路，不能使用单独放水的验收标准。
- 仍未达标的材料应直接废弃，而不是继续反复修补。

## 3. 三层定义

### 3.1 深度一

深度一负责回答：

- 这段材料是否对某个主卡有真实倾向？
- 它更接近哪张孙族卡 / business card？
- 它为什么失败，是“不可救”还是“可修复失败”？

在当前代码链路中，深度一的现有基础主要已经存在于：

- 候选规划与切分：
  [material_pipeline_v2.py](C:/Users/Maru/Documents/agent/passage_service/app/services/material_pipeline_v2.py:99)
  [material_pipeline_v2.py](C:/Users/Maru/Documents/agent/passage_service/app/services/material_pipeline_v2.py:3423)
- 候选正式判卡：
  [material_pipeline_v2.py](C:/Users/Maru/Documents/agent/passage_service/app/services/material_pipeline_v2.py:558)
- 主卡信号重判：
  [main_card_signal_resolver.py](C:/Users/Maru/Documents/agent/passage_service/app/services/main_card_signal_resolver.py:48)

深度一的最小输出建议包括：

- `business_family_id`
- `question_card_id`
- `selected_material_card`
- `selected_business_card`
- `business_card_recommendations`
- `quality_score`
- `llm_generation_readiness`
- `candidate_type`
- `neutral_signal_profile`
- `business_feature_profile`
- `question_ready_context`
- `near_miss_reason`
- `repairable_dirty_states`

其中 `near_miss_reason` 和 `repairable_dirty_states` 是新增运行时字段，用于判断是否进入深度二。

### 3.2 深度二

深度二负责回答：

- 这条 near-miss 材料是否属于“可修复失败”？
- 如果可修复，应做哪一类有限修复？
- 修完后，它是否更接近可消费正式单元？

深度二输入必须至少包含：

- 原始候选文本
- 深度一识别出的目标题卡 / 孙族卡信息
- 深度一识别出的脏状态
- 深度一失败原因
- 原始打分与结构特征

深度二允许做的动作：

- 去页面残留、清版式噪声
- 收紧多余上下文
- 重新切段或重定显示单元边界
- 轻度收束已有结构信号
- 放大原文已经存在的槽位 / 顺序 / 主轴特征

深度二禁止做的动作：

- 改写主旨
- 改变论证方向
- 新增关键事实
- 凭空补出原文没有的强逻辑桥
- 把一种题感改造成另一种题感
- 为了命中孙族卡而重写成另一篇文本

深度二的目标不是“修得越像目标卡越好”，而是：

- 更可读
- 更可消费
- 更保留原有主旨和结构
- 更接近该候选本来潜在的正式承载单元

### 3.3 深度三

深度三必须复用正式链路重新判定，而不是另开一套弱标准。

深度三需要复判：

- 主卡落位是否仍成立
- 孙族卡是否更清晰
- `quality_score` 是否提升
- `llm_generation_readiness` 是否提升
- 是否发生主旨漂移
- 是否发生题型偏移

深度三输出建议分四类：

- `pass_strong`
- `pass_weak`
- `fail_drift`
- `fail_discard`

定义如下：

- `pass_strong`：可读、主旨稳定、核心特征稳定，且明显更接近目标正式单元。
- `pass_weak`：基本修回，但边界仍偏弱，只能作为弱正式单元或边界样本。
- `fail_drift`：可读性变好，但主旨、结构主轴或题感发生明显漂移。
- `fail_discard`：修复后仍不可消费，应直接废弃。

## 4. 准入条件

只有满足以下条件的候选才允许进入深度二：

- 深度一已识别出明确 `business_family_id`
- 已命中 `selected_business_card`，或目标卡位于 `business_card_recommendations` 前列
- 首轮失败属于“可修复失败”，而不是“主旨错误”
- 脏状态属于可逆类型，如：
  - `page_residue`
  - `layout_break`
  - `truncated_context`
  - `over_appended_context`
  - `weak_legality_source`
  - `shape_misaligned_for_task`
  - `structure_signal_impure`
  - `example_overdominant`
  - `main_axis_diluted`

不应进入深度二的情况：

- 候选本身不属于目标主卡
- 需要大幅改写才能命中目标孙族
- 原文结构骨架本来就不成立
- 原文信息不完整到无法保持主旨
- 目标卡命中仅来自表面词汇而非真实题感

## 5. 深度二修复动作约束

深度二建议只保留 1 到 3 个有限 repair 动作，不允许无边界叠加。

推荐动作类型：

- `trim_noise`
- `trim_irrelevant_tail`
- `resegment_display_units`
- `tighten_context_window`
- `recover_local_anchor`
- `amplify_existing_structure_signal`

不建议直接在主流程使用的动作：

- 大规模重写
- 风格统一化改写
- 扩写缺失论证
- 生成型过渡句补写

## 6. 当前代码链路中的建议接入点

### 6.1 推荐的一期接入点

一期建议只接入运行时 near-miss repair，不修改库存主文本，不先改 `segment` 主链。

推荐接入点：

- 运行时候选正式判卡之后：
  [material_pipeline_v2.py](C:/Users/Maru/Documents/agent/passage_service/app/services/material_pipeline_v2.py:558)
- 或 `search()` 内候选逐条打分之后、最终丢弃之前：
  [material_pipeline_v2.py](C:/Users/Maru/Documents/agent/passage_service/app/services/material_pipeline_v2.py:165)

推荐流程：

1. 候选正常走一次深度一判卡。
2. 若通过，直接进入正式候选池。
3. 若失败，但符合 near-miss repair 准入条件，则进入深度二。
4. 深度二产出修复文本后，重新走一次 `build_cached_item_from_material()`。
5. 若深度三通过，则以修复后运行时候选参与排序；否则丢弃。

### 6.2 不建议的一期接入点

不建议一期直接接入：

- [segment_service.py](C:/Users/Maru/Documents/agent/passage_service/app/domain/services/segment_service.py:14)

原因：

- 会把候选切分与 repair 混在一起
- 会污染基础候选库存
- 不利于后续比较“原候选 vs 修复候选”
- 不利于深度二测试阶段做 shadow run

### 6.3 二期可考虑接入点

若一期验证稳定，可再考虑把 repair 成功样本纳入预计算索引：

- [material_v2_index_service.py](C:/Users/Maru/Documents/agent/passage_service/app/domain/services/material_v2_index_service.py:12)

但二期前提是：

- 已证明 repair 不会系统性漂移
- 已有足够的可观测字段
- 已明确 repair 成功样本是否允许落库

## 7. 最小运行时字段建议

为便于审计和深度二测试，建议在运行时追加以下字段：

- `repair_candidate`: `true/false`
- `repair_entry_reason`
- `repair_dirty_states`
- `repair_target_business_card`
- `repair_actions`
- `repair_applied`
- `repair_output_text`
- `repair_preserve_ratio`
- `repair_before_scores`
- `repair_after_scores`
- `repair_outcome`

如果一期不想改 schema，可先作为运行时内存字段挂在 `question_ready_context` 或 `local_profile` 下做 shadow 记录。

## 8. 废弃规则

以下情况应直接标记为废弃材料：

- 修复后仍未命中目标主卡
- 修复后主旨明显漂移
- 修复后孙族卡发生明显转移
- 修复后可读性虽提升，但特征骨架丢失
- 修复后 `quality_score` 提升不明显，且 `llm_generation_readiness` 仍低
- 修复所需动作已超出轻修范围

## 9. 深度二测试定义

深度二测试不以“能否恢复原样”为目标，而以“是否恢复复原感”为目标。

“复原感”至少包括：

- 文段可读
- 主旨保留
- 结构主轴保留
- 目标主卡特征保留
- 目标孙族特征仍成立或更清晰
- 相比 dirty text，更接近 gold material 的题感

建议测试结果按以下标准判定：

- 通过：
  - 可读性恢复
  - 主旨未漂移
  - 特征未丢失
  - 更接近 gold 的正式承载单元
- 失败：
  - 文段变顺但题感偏了
  - 文段可读但主旨改了
  - 文段更像另一张卡
  - 文段仍不可消费

## 10. 当前结论

这条“深度一发现倾向 -> 深度二按脏状态和目标孙族做精修 -> 深度三复判 -> 失败废弃”的链路是可行的。

就当前仓库状态看，一期最合理的落地方式是：

- 不把深度二做成全量主路径
- 只救 near-miss 候选
- 只做轻修与特征显化
- 复用当前正式判卡链路做深度三复判
- 先以 shadow / fallback 形式接入运行时链路，再决定是否进入索引与落库
