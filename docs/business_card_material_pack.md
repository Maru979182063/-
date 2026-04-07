# Business Card Material Pack

## 1. 本轮处理范围

本轮先做了两处收紧，再基于收紧后的真实主链结果重新跑了一轮材料与业务卡匹配：

- 收紧 `whole_passage` 正式候选资格闸门
- 收紧 `title_material.plain_main_recovery` 这张“全文整合单中心卡”的命中条件

重跑范围：

- 文章重跑：17 篇
- 主链结果：全部为 `v2_primary`
- fallback：0
- 新生成正式材料：15 条
- 新材料 `span_type`：全部为 `multi_paragraph_unit`

本轮覆盖的 business card：

- `main_idea / title_selection` runtime 卡：5 张
- `sentence_fill` runtime 卡：7 张
- `sentence_order` runtime 卡：5 张

非 runtime 卡处理方式：

- `sentence_fill__position_function__abstract` 不作为运行卡补材
- 本轮只说明其角色，不强行挂运行材料

## 2. 收紧动作摘要

### whole_passage 收紧

- `whole_passage` 不再与 `closed_span / multi_paragraph_unit` 平权进入正式候选
- 现在只有在整篇文本足够短、足够单中心、闭合足够强、上下文依赖低、并且不存在更收束的块状候选时，才允许保留
- 被拦下的整篇候选会留下最小 trace：`whole_passage_gate` 与 `whole_passage_gate_reason`

### 全文整合单中心卡收紧

- 对 `title_material.plain_main_recovery` 新增了更严格的 runtime gate
- 现在只有短而高度单中心、低分叉、低问题-对策/结果归因干扰的材料，才允许命中这张卡
- 风险—防范、问题—对策、成果—问题—下一步、整篇过宽评论/资讯，不再轻易被压成这张泛化卡

### 为什么先做这两步

因为当前最明显的坏样本已经不是机械切碎，而是：

- 整篇过宽材料被 whole_passage 放进正式候选
- 不同结构的材料又被进一步压成“全文整合单中心卡”

这两处不先收紧，后面的 business card 挂材会持续被泛化假阳性污染。

## 3. 各业务卡代表材料清单

### business_card_id: cause_effect__conclusion_focus__main_idea

- family: `main_idea`（运行时归到 `title_selection`）
- 是否 runtime: 是
- 当前匹配状态：半可用
- 选中的 material_id: `mat_98e4e29c6e0145df803bcd45067fb8f8`
- article_id: `article_3f135c73a09e4c27b2c40a52d237b92f`
- title: `读懂中国经济运行逻辑`
- span_type: `multi_paragraph_unit`
- material_status: `gray`
- release_channel: `gray`
- generated_by: `v2_primary_candidate_builder+llm_candidate_planner`
- primary_family: `概括归纳型`
- primary_label: `全文整合单中心卡`

材料正文摘录：

> 伴随我国经济体量持续扩大，增长率呈现下降趋势，这是客观规律使然。长期看，随着我国经济向发达国家水平迈进，增速将逐渐趋近发达国家水平……我国设定的2026年4.5%至5%增长目标，与潜在增长率基本吻合，是积极进取与求真务实的平衡……

命中原因：

- 结构上是“现象/判断 -> 原因解释 -> 结论落点”的链条
- 中心较明确，围绕“中国经济增长目标与运行逻辑”展开
- 收束度尚可，结论句承担了主要信息
- 可以支撑“中心理解 / 结论聚焦”类出题

风险说明：

- 当前仍是 `gray`
- material card 仍落在 `title_material.value_commentary`，不是更纯粹的因果结论卡
- 说明主链已能挂上，但区分仍不够硬

### business_card_id: necessary_condition_countermeasure__main_idea

- family: `main_idea`（运行时归到 `title_selection`）
- 是否 runtime: 是
- 当前匹配状态：半可用
- 选中的 material_id: `mat_8936cb13a3c24e9381d72b5ad37724c2`
- article_id: `article_c0d4cc4d53f44bc183606c0963771fe2`
- title: `坚持唯物辩证法 践行正确政绩观`
- span_type: `multi_paragraph_unit`
- material_status: `gray`
- release_channel: `gray`
- generated_by: `v2_primary_candidate_builder+llm_candidate_planner`
- primary_family: `概括归纳型`
- primary_label: `全文整合单中心卡`

材料正文摘录：

> 政绩观问题是一个根本性问题……树立和践行正确政绩观，必须回答好“政绩为谁而树、树什么样的政绩、靠什么树政绩”的问题……正确政绩观的树立，离不开唯物辩证法的科学指引……

命中原因：

- 文本里有明确的“必须”“离不开”这类必要条件信号
- 中心稳定，始终围绕“正确政绩观如何成立”展开
- 闭合度较强，出题时可以压到“关键前提/关键条件”理解

风险说明：

- 当前仍是 `gray`
- 结构里带较强理论评论色彩，不是纯粹的对策型材料
- 说明这张卡已经能挂到较像的材料，但仍偏评论化

### business_card_id: parallel_comprehensive_summary__main_idea

- family: `main_idea`（运行时归到 `title_selection`）
- 是否 runtime: 是
- 当前匹配状态：半可用
- 选中的 material_id: `mat_a3552dd398604e53b73b8a32337370ae`
- article_id: `article_c0d4cc4d53f44bc183606c0963771fe2`
- title: `坚持唯物辩证法 践行正确政绩观`
- span_type: `multi_paragraph_unit`
- material_status: `gray`
- release_channel: `gray`
- generated_by: `v2_primary_candidate_builder+llm_candidate_planner`
- primary_family: `概括归纳型`
- primary_label: `全文整合单中心卡`

材料正文摘录：

> 正确处理当下与长远，关键在于循序渐进、接续发力……坚持普遍联系观点，处理好发展和稳定、发展和民生、发展和人心的紧密联系……这启示我们，践行正确政绩观，必须坚持系统观念……

命中原因：

- 结构上存在多组并列关系，再统一落到“系统观念/正确政绩观”
- 单中心仍可成立，不是纯散点罗列
- 适合“并列归纳后中心收束”类主旨题

风险说明：

- 当前仍是 `gray`
- material card 实际仍偏 `problem_essence_judgement`
- 说明并列综合类材料能挂上，但与问题判断类仍有混叠

### business_card_id: theme_word_focus__main_idea

- family: `main_idea`（运行时归到 `title_selection`）
- 是否 runtime: 是
- 当前匹配状态：半可用
- 选中的 material_id: `mat_8936cb13a3c24e9381d72b5ad37724c2`
- article_id: `article_c0d4cc4d53f44bc183606c0963771fe2`
- title: `坚持唯物辩证法 践行正确政绩观`
- span_type: `multi_paragraph_unit`
- material_status: `gray`
- release_channel: `gray`
- generated_by: `v2_primary_candidate_builder+llm_candidate_planner`
- primary_family: `概括归纳型`
- primary_label: `全文整合单中心卡`

材料正文摘录：

> 政绩观问题是一个根本性问题……正确政绩观的树立，离不开唯物辩证法的科学指引……面对改革发展稳定繁重任务，必须以唯物辩证法为指导……

命中原因：

- “政绩观”与“唯物辩证法”这两个核心主题词反复锚定
- 主题词焦点明确，中心没有明显漂移
- 适合作为“主题词聚焦 -> 主旨理解”型近似材料

风险说明：

- 当前不是精确主命中，主选中的 business card 仍是 `necessary_condition_countermeasure__main_idea`
- 只能算近似代表材料，不算已经稳定分出“主题词聚焦卡”

### business_card_id: turning_relation_focus__main_idea

- family: `main_idea`（运行时归到 `title_selection`）
- 是否 runtime: 是
- 当前匹配状态：半可用
- 选中的 material_id: `mat_a745c2d405e04beb857cc2ccbd529280`
- article_id: `article_c4680066f2604f439af6a41c8bd12426`
- title: `从“管理”到“服务”——曹杨新村街道探索党建引领物业治理新路径`
- span_type: `multi_paragraph_unit`
- material_status: `gray`
- release_channel: `gray`
- generated_by: `v2_primary_candidate_builder+llm_candidate_planner`
- primary_family: `概括归纳型`
- primary_label: `全文整合单中心卡`

材料正文摘录：

> “管理”强调秩序与约束，“服务”指向需求与回应。这背后，是对物业与居民关系的重新定位……推动物业从“被动应付”走向“主动作为”……

命中原因：

- 明显存在“管理 -> 服务”的转折关系
- 转折后重心后移，真正中心落在治理理念转变上
- 闭合度好，具有较强出题可用性

风险说明：

- 当前仍是 `gray`
- 虽然结构较像，但正文仍带新闻引入与案例说明

### business_card_id: sentence_fill__opening_summary__abstract

- family: `sentence_fill`
- 是否 runtime: 是
- 当前匹配状态：未命中
- 选中的 material_id: 当前未命中合格材料

命中原因：

- 本轮检索里虽然出现了精确 `selected_business_card = sentence_fill__opening_summary__abstract`
- 但命中的实际材料是普通 `multi_paragraph_unit`，正文仍是完整论述块，不是适合做开头句填空的正式材料单元

风险说明：

- 当前命中仍偏假阳性
- 命中的 material card 多为 `fill_material.middle_focus_shift / opening_summary`
- 但文本并没有收束成真正可 blank 的“开头概括句”结构

### business_card_id: sentence_fill__opening_topic_intro__abstract

- family: `sentence_fill`
- 是否 runtime: 是
- 当前匹配状态：未命中
- 选中的 material_id: 当前未命中合格材料

命中原因：

- 推荐列表里可见近似命中
- 但当前主命中仍落在 `sentence_fill__opening_summary__abstract`
- 说明“开头引题”和“开头概括”仍未在正式材料层分清

风险说明：

- 当前仍是普通 `multi_paragraph_unit`
- 不足以直接支撑稳定填空出题

### business_card_id: sentence_fill__ending_countermeasure__abstract

- family: `sentence_fill`
- 是否 runtime: 是
- 当前匹配状态：未命中
- 选中的 material_id: 当前未命中合格材料

命中原因：

- 推荐列表中能看到该卡
- 但主命中并未落到它，且候选正文不是“尾句对策落点”式正式材料

风险说明：

- 仍是结构擦边，不是合格代表材料
- 当前不能把这类结果当作可交付业务材料

### business_card_id: sentence_fill__ending_summary__abstract

- family: `sentence_fill`
- 是否 runtime: 是
- 当前匹配状态：未命中
- 选中的 material_id: 当前未命中合格材料

命中原因：

- 推荐链里可见，但没有形成精确主命中
- 文本本身也偏完整段落，不像适合尾句总结 blank 的材料单元

风险说明：

- 当前仍是泛化命中，不足以支撑后续出题

### business_card_id: sentence_fill__middle_bridge_both_sides__abstract

- family: `sentence_fill`
- 是否 runtime: 是
- 当前匹配状态：未命中
- 选中的 material_id: 当前未命中合格材料

命中原因：

- 推荐列表里有这张卡
- 但正文没有形成真正“中间承前启后”的局部可 blank 结构

风险说明：

- 当前命中的还是完整论述块
- 不是真正的 bridge 两侧验证材料

### business_card_id: sentence_fill__middle_carry_previous__abstract

- family: `sentence_fill`
- 是否 runtime: 是
- 当前匹配状态：未命中
- 选中的 material_id: 当前未命中合格材料

命中原因：

- 推荐列表中偶有出现
- 但精确主命中没有成立，且正文不具备可稳定 blank 的中段承接结构

风险说明：

- 当前仍是普通概括/评论块，不是合格 fill 材料

### business_card_id: sentence_fill__middle_lead_next__abstract

- family: `sentence_fill`
- 是否 runtime: 是
- 当前匹配状态：未命中
- 选中的 material_id: 当前未命中合格材料

命中原因：

- 推荐链里有擦边
- 但没有形成真正“引出下文”的局部功能单元

风险说明：

- 当前命中主要是结构泛化，不是功能真命中

### business_card_id: sentence_fill__position_function__abstract

- family: `sentence_fill`
- 是否 runtime: 否
- 当前匹配状态：不补运行材料

命中原因：

- 这张卡当前角色是总卡 / 映射卡
- 它用于组织“位置 -> 功能 -> 分支策略”，不是运行型单功能 business card

风险说明：

- 本轮不把它当运行卡挂材料，避免把总卡误写成可消费真值卡

### business_card_id: sentence_order__deterministic_binding__abstract

- family: `sentence_order`
- 是否 runtime: 是
- 当前匹配状态：未命中
- 选中的 material_id: 当前未命中合格材料

命中原因：

- 虽然缓存里出现了精确 `selected_business_card`
- 但对应 material card 是 `legacy.sentence_order.precomputed`
- 文本类型仍是 `multi_paragraph_unit`，不是可排序材料单元

风险说明：

- `quality_score = 0.0`
- 当前属于旧兼容层命中，不应视为真实可交付代表材料

### business_card_id: sentence_order__discourse_logic__abstract

- family: `sentence_order`
- 是否 runtime: 是
- 当前匹配状态：未命中
- 选中的 material_id: 当前未命中合格材料

命中原因：

- 命中来自 `legacy.sentence_order.precomputed`
- 不是 sentence_order 的正式材料单元，而是被旧兼容索引吸进去的多段正文块

风险说明：

- 当前不具备稳定排序出题价值

### business_card_id: sentence_order__head_tail_lock__abstract

- family: `sentence_order`
- 是否 runtime: 是
- 当前匹配状态：未命中
- 选中的 material_id: 当前未命中合格材料

命中原因：

- 表面上是精确 business card 命中
- 但底层仍是 `legacy.sentence_order.precomputed + multi_paragraph_unit`

风险说明：

- 不是可直接消费的排序材料
- 只是旧兼容逻辑留下的假阳性

### business_card_id: sentence_order__head_tail_logic__abstract

- family: `sentence_order`
- 是否 runtime: 是
- 当前匹配状态：未命中
- 选中的 material_id: 当前未命中合格材料

命中原因：

- 只有推荐列表中出现，没有形成精确主命中
- 候选文本也不是拆成稳定排序单元的材料

风险说明：

- 当前不能作为 head-tail logic 的代表材料

### business_card_id: sentence_order__timeline_action_sequence__abstract

- family: `sentence_order`
- 是否 runtime: 是
- 当前匹配状态：未命中
- 选中的 material_id: 当前未命中合格材料

命中原因：

- 虽有精确 business card 命中
- 但仍来自 `legacy.sentence_order.precomputed`
- 正文是完整多段论述，不是可排序动作序列材料

风险说明：

- 当前属于结构错位命中，不应算合格代表材料

## 4. 总体结论

本轮先收紧 `whole_passage` 和“全文整合单中心卡”后，再重跑 17 篇文章，得到 15 条新正式材料，全部为 `v2_primary + multi_paragraph_unit`，没有 `whole_passage` 继续溜进正式材料池，说明“整篇过宽”这一主问题已经被明显压住。

但从 business card 挂材结果看，当前流水线只在 `main_idea / title_selection` 这组业务卡上出现了较清楚的代表材料，而且也仍以 `gray` 为主，只能算半可用。`sentence_fill` 和 `sentence_order` 虽然在 V2 cached 检索层还能看到推荐或旧兼容命中，但大多数仍不是合格的正式材料单元：前者主要是普通 `multi_paragraph_unit` 被泛化挂卡，后者主要还停留在 `legacy.sentence_order.precomputed` 的兼容层。

收口判断：

- 已能较稳定挂到较像材料的卡：`cause_effect__conclusion_focus__main_idea`、`necessary_condition_countermeasure__main_idea`、`parallel_comprehensive_summary__main_idea`、`turning_relation_focus__main_idea`
- 目前只有近似挂材、尚未分清的卡：`theme_word_focus__main_idea`
- 当前仍没有合格正式材料的卡：`sentence_fill` 运行卡全组、`sentence_order` 运行卡全组

当前离“满足业务”还差的主问题已经很明确：

- 正式材料池已经比以前更收束，但主要仍服务于 `main_idea / title_selection`
- `sentence_fill / sentence_order` 还没有形成与其业务功能真正对齐的正式材料单元承载层，当前看到的命中大多仍是泛化擦边或旧兼容假阳性
