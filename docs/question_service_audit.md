# 题目服务审计

## 1. 题目服务定位

当前 `prompt_skeleton_service` 的真实职责，已经明显超过“prompt skeleton 组装器”。

- 它负责题目服务的完整编排链路：输入解码、题型配置加载、slot 解析、pattern 选择、difficulty 投影、prompt package 组装、材料桥接调用、模型生成、结构化解析、题型校验、质量评审、review action、manual_edit、版本持久化、snapshot 留痕、review/export 接口。
- 它不负责原始材料抓取、清洗、分段、材料池治理、材料来源回流规则本身；这些属于 `passage_service`。
- 它与题卡层的关系，不是直接全面消费题卡，而是主要消费 `configs/types/*.yaml`、`configs/prompt_templates.yaml`、`configs/question_runtime.yaml` 这套本地配置；同时又通过 `SourceQuestionAnalyzer`、`MaterialBridgeService` 间接使用 `business_card_ids`、`question_card` 相关语义。
- 它与材料层的关系，是通过 `MaterialBridgeService` 调 `passage_service` 的 V2 材料检索接口，取回候选材料，再把材料嵌入生成链路；它不是材料治理中心，但它已经在本地承担了一部分材料选择和回退策略。
- 它与前端层的关系，不只是“给页面返回 prompt”，而是直接支撑 `/api/v1/questions/*`、`/api/v1/review/*`、`/api/v1/meta/*`、`/demo` 这一整套题目生成与评审工作流。

从当前仓库真实结构看，`prompt_skeleton_service` 更像“题目编排与审核中心”，而不是单纯的 prompt 拼装服务。`README.md` 仍停留在“只组装 prompt、不调用模型”的旧定位，已经不能准确反映当前职责。

## 2. 当前题目服务主流程

当前主流程可以按下面的顺序理解。

### 2.1 输入解码

- 外部入口主要在 [questions.py](/Users/Maru/Documents/agent/prompt_skeleton_service/app/routers/questions.py)。
- `/source-question/parse` 调 `SourceQuestionParserService`，把原始母题文本拆成 `SourceQuestionPayload`。
- `/source-question/detect` 调 `SourceQuestionAnalyzer`，再在 router 内额外推断 `question_focus`、`special_question_type`、`material_structure`、`text_direction`。
- `/generate` 接收 `QuestionGenerateRequest`，其中已经包含 `question_focus`、`difficulty_level`、`special_question_types`、`type_slots`、`material_policy`、`source_question` 等字段。

这一段里：

- 请求 schema 是配置无关的系统层。
- `InputDecoderService` 内部的 `QUESTION_FOCUS_MAPPING`、`SPECIAL_TYPE_MAPPING`、`DIFFICULTY_MAPPING` 是代码驱动。
- `/source-question/detect` 里的题型推断、结构推断、文体推断是 router 级硬编码，不是配置驱动。

### 2.2 类型 / 题型配置加载

- `ConfigRegistry` 负责加载 `configs/types/*.yaml`。
- 当前实际题型配置文件有：
  - `main_idea.yaml`
  - `continuation.yaml`
  - `sentence_order.yaml`
  - `sentence_fill.yaml`
- `RuntimeConfigRegistry` 加载 `configs/question_runtime.yaml`。
- `PromptTemplateRegistry` 加载 `configs/prompt_templates.yaml`，并按 `question_type + business_subtype + action_type` 选择活动模板。

这一段主要是配置驱动，属于当前体系里最清晰的一块。

### 2.3 skeleton 构造

- `PromptOrchestratorService.build_prompt()` 是骨架编排中心。
- 它先取题型配置，再交给 `SlotResolverService.resolve()` 做 slot 补全、pattern 选择、difficulty projection、difficulty fit 计算、skeleton 构造。
- 然后交给 `PromptBuilderService.build()` 根据 `control_logic`、`generation_logic`、resolved slots、few-shot 规则、extra constraints 生成 `prompt_package`。
- 返回的结果已经不是单纯 prompt，而是一个初始 `QuestionItem`。

这一段里：

- `slot_schema`、`default_slots`、`patterns`、`difficulty_target_profiles`、`control_logic`、`generation_logic` 来自 yaml。
- 但 difficulty slot 调整规则本身，仍有一部分硬编码在 `SlotResolverService` 的 `DIFFICULTY_BIAS`、`ASCENDING_DIFFICULTY_SLOTS`、`DESCENDING_DIFFICULTY_SLOTS` 中。
- few-shot 的优先级与挑选逻辑，写在 `PromptBuilderService` 代码里，不完全由配置表达。

### 2.4 slot / pattern / difficulty 处理

当前这一步由 `SlotResolverService` 实际控制。

- 它先合并 `default_slots`、subtype 覆盖、用户传入 slots。
- 再根据难度对部分 slot 自动升降档。
- 再按 `match_rules` 选 pattern，生成 `pattern_selection_reason`。
- 再计算 `difficulty_projection`、`difficulty_target_profile`、`difficulty_fit`。

这一层表面上是“题型配置驱动”，但难度到 slot 的映射关系并没有完全上收到配置层，仍有明显的服务内规则。

### 2.5 prompt_package 生成

- `PromptBuilderService` 生成 `system_prompt`、`user_prompt`、`fewshot_examples`、`merged_prompt`。
- `PromptTemplateRegistry.resolve_default()` 再按 action 取模板，交给 `QuestionGenerationService` 叠加使用。

当前 prompt 规则的来源分成四类：

- 类型配置：`control_logic`、`generation_logic`、pattern、few-shot 片段。
- 模板配置：`prompt_templates.yaml` 中各题型的 `generate`、`minor_edit`、`question_modify`、`text_modify`、`judge_review` 模板。
- 服务硬编码：difficulty 文案、few-shot 选取优先级、review override 渲染、wrong-option confusion 说明。
- action 上下文：当前材料、当前题目、母题分析、feedback notes。

### 2.6 questions/generate

`QuestionGenerationService.generate()` 是当前题目服务的主执行入口。

实际顺序是：

1. 预处理请求，必要时解析 `source_question`
2. 通过 `_build_decode_request()` 把表单输入转成标准请求
3. `orchestrator.decode_input()`
4. 若存在母题，`_effective_difficulty_target()` 会把难度默认上调一档
5. `SourceQuestionAnalyzer.analyze()` 产出 `business_card_ids`、`query_terms`、`target_length`、`structure_constraints` 等
6. 通过 `MaterialBridgeService.select_materials()` 取候选材料
7. 对候选材料做 race 生成，当前并发候选数由 `RACE_CANDIDATE_COUNT = 2` 控制
8. 按材料逐个构造 prompt、调用模型、解析结构化输出
9. 生成 `GeneratedQuestion`
10. 对语序题做 `build_sentence_order_question()` 和 `_enforce_sentence_order_six_unit_output()`
11. 做 validator 和质量评审
12. 生成 item/version/snapshot，并写入 repository
13. 返回 batch 与 item 列表

这一段同时混合了三种驱动方式：

- 配置驱动：题型配置、prompt 模板、runtime 配置
- prompt 驱动：生成模板、judge_review 模板、feedback notes 拼装
- action / service 驱动：难度上调、候选材料 race、重试阈值、语序题专用修补、质量门控

### 2.7 validator

- `QuestionValidatorService` 先做通用校验，再按题型分支做细化校验。
- 当前显式支持的题型分支有：`main_idea`、`continuation`、`sentence_order`、`sentence_fill`。
- `ValidationResult` 会写入 `validation_status`、`passed`、`score`、`errors`、`warnings`、`checks`、`difficulty_review`、`next_review_status`。

当前状态是：

- 通用校验已经存在。
- 语序题的题型校验最完整。
- 其他题型有题型判断，但更多是业务规则检查，不完全是“真值锚点校验”。

### 2.8 review / repair / fine-tune / confirm / manual_edit

- `QuestionReviewService.apply_action()` 是统一 action 分发入口。
- `approve` / `confirm` / `discard` 直接改状态。
- `minor_edit` 调 `QuestionGenerationService.revise_minor_edit()`
- `question_modify` 调 `revise_question_modify()`
- `text_modify` 调 `revise_text_modify()`
- `manual_edit` 调 `apply_manual_edit()`
- `/fine-tune` 路由本质上只是把请求转成 `minor_edit`

这里的关键现实情况是：

- `minor_edit` 并不是只修局部文案，而是把当前题目整包喂给模型，要求返回完整更新后的 `GeneratedQuestion`。
- `question_modify` 是保留材料、按控制项重建题。
- `text_modify` 可以手动贴材料、换材料 id 或重新选材料，实质上常常是“换材料后整题重建”。
- `manual_edit` 允许直接 patch `stem`、`options`、`answer`、`analysis`，还允许直接 patch `material_text`、`material_source`、`source_tail` 等材料字段。

因此，当前 review/action 层已经不只是“审核动作”，而是二次生成和直接改题的混合入口。

### 2.9 repository / snapshot / version / action log

- `QuestionRepository` 用 SQLite 管理：
  - `generation_batches`
  - `question_items`
  - `question_item_versions`
  - `question_review_actions`
- `QuestionSnapshotBuilder` 负责构造：
  - `input_snapshot`
  - `prompt_snapshot`
  - `runtime_snapshot`
  - `material_snapshot`
  - `model_output_snapshot`
  - `validation_snapshot`
- `QuestionReviewService` 在每次 action 后保存：
  - 最新 item
  - 可选 version
  - review action log

这一层已经不是临时状态缓存，而是完整的对象留痕系统。它是当前题目服务最成型的一部分。

## 3. 当前对象与字段分层现状

### 3.1 当前是否已经存在统一 question object

存在。

当前统一对象就是 [item.py](/Users/Maru/Documents/agent/prompt_skeleton_service/app/schemas/item.py) 中的 `QuestionItem`，并在 [question.py](/Users/Maru/Documents/agent/prompt_skeleton_service/app/schemas/question.py) 中由 `QuestionGenerationItem` 扩展成带 batch、material、request snapshot 的完整出题对象。

它已经承载了：

- 协议侧字段
- 生成侧字段
- 校验侧字段
- review 状态字段
- 持久化与历史字段

这说明项目并不是“完全没有统一对象”，而是已经有统一对象，但字段分层还没有完全制度化。

### 3.2 QuestionItem 负责什么

`QuestionItem` 当前主要负责承载一次题目从 skeleton 到生成结果的中间与最终状态。

它包含：

- 标识与状态：`item_id`、`current_version_no`、`current_status`、`latest_action`、`latest_action_at`
- 类型协议：`question_type`、`business_subtype`、`pattern_id`、`selected_pattern`
- 配置求解结果：`resolved_slots`、`skeleton`、`difficulty_target`、`difficulty_target_profile`、`difficulty_projection`、`difficulty_fit`
- 生成控制：`control_logic`、`generation_logic`、`prompt_package`
- 结果：`generated_question`
- 质量与评审：`validation_result`、`evaluation_result`、`statuses`
- 辅助信息：`warnings`、`notes`

它是当前系统的主流转对象。

### 3.3 GeneratedQuestion 负责什么

`GeneratedQuestion` 当前负责承载“可交付题目本体”。

字段包括：

- 题型标识：`question_type`、`business_subtype`、`pattern_id`
- 题面：`stem`
- 语序题专用真值字段：`original_sentences`、`correct_order`
- 展示项：`options`
- 展示答案：`answer`
- 解析：`analysis`
- 扩展：`metadata`

它更像“生成结果对象”，不是完整流程对象。

### 3.4 ValidationResult / statuses / snapshots / versions 分别负责什么

- `ValidationResult`：负责记录校验与评审结论，不是题目本体。它同时混合了结构校验、题型校验、难度评审、下一步 review status 建议。
- `statuses`：负责当前流程态，含 `build_status`、`generation_status`、`validation_status`、`review_status`。
- `snapshots`：负责一次生成/修订时的上下文留痕。`QuestionSnapshotBuilder` 已经把输入、prompt、runtime、材料、模型输出、校验结果拆开存。
- `versions`：负责版本化后的题目结果快照。repository 里单独存 `stem`、`options_json`、`answer`、`analysis`、`prompt_package_json`、`runtime_snapshot_json` 等。

这几层已经形成对象生命周期，但字段边界还不够整齐。

### 3.5 当前哪些字段像 truth

从当前实现看，最接近 truth 的字段有：

- `question_type`
- `business_subtype`
- `pattern_id`
- `resolved_slots`
- `control_logic`
- `generation_logic`
- `material_selection`
- `GeneratedQuestion.original_sentences`
- `GeneratedQuestion.correct_order`

其中最明确、最可被程序强校验的真值锚点，实际上集中在语序题。

### 3.6 当前哪些字段像 render

更像展示层或派生层的字段有：

- `GeneratedQuestion.stem`
- `GeneratedQuestion.options`
- `GeneratedQuestion.answer`
- `QuestionItemSummary.stem_preview`
- `QuestionItemSummary.material_preview`
- `material_text`
- `material_source`
- `prompt_package.merged_prompt`

这些字段更接近“展示或交付形式”，不是协议锚点。

### 3.7 当前哪些字段像 analysis

更像分析、解释、诊断的字段有：

- `GeneratedQuestion.analysis`
- `pattern_selection_reason`
- `difficulty_projection`
- `difficulty_fit`
- `validation_result.checks`
- `validation_result.difficulty_review`
- `evaluation_result`
- `warnings`
- `notes`

这一层当前很丰富，但也导致对象里“题目本体”和“诊断信息”混在一起。

### 3.8 当前哪些字段语义还不清晰

以下字段的职责边界当前并不完全清晰：

- `skeleton`：是协议抽象层，还是 prompt 渲染用中间层，目前两者兼有。
- `control_logic` 与 `generation_logic`：它们来自题型配置，但在后续生成、review、manual patch 中没有被严格当作不可越权的控制层。
- `metadata`：当前没有统一约束，容易成为兜底杂项字段。
- `current_status` 与 `statuses.review_status`：两套状态语义部分重叠。
- `material_source` 与 `material_selection.source`：都有来源语义，边界重复。
- `request_snapshot`：既承载原始输入，也承载 source-question 分析、control override 残留，语义很宽。

## 4. 当前配置驱动程度审计

### 4.1 已经由 yaml / config 驱动的部分

当前真正已经配置化的内容主要有四块。

#### 题型协议层

在 `configs/types/*.yaml` 中，已经外置了：

- `type_id`
- `display_name`
- `task_definition`
- `slot_schema`
- `default_slots`
- `business_subtypes`
- `patterns`
- `default_pattern_id`
- `difficulty_target_profiles`
- `control_logic`
- `generation_logic`
- `fewshot_policy`

这部分是当前题目服务最接近“题型协议层”的地方。

#### prompt 模板层

在 `configs/prompt_templates.yaml` 中，已经外置了各题型的：

- `generate`
- `minor_edit`
- `question_modify`
- `text_modify`
- `judge_review`

模板已支持按 `question_type + business_subtype + action_type + template_version` 选择。

#### 运行时配置层

在 `configs/question_runtime.yaml` 中，已经外置了：

- LLM provider 与 model route
- model 参数
- materials 服务地址
- persistence 配置
- evaluation/judge 开关与路由

#### registry / schema 层

配置文件不是字符串裸读，而是走了 schema 与 registry：

- `QuestionTypeConfig`
- `PromptTemplateRecord`
- `QuestionRuntimeConfig`

这一点说明当前系统不是“无结构配置堆”，而是已经有正式的配置入口。

### 4.2 仍被硬编码在 service / validator / action 中的逻辑

当前硬编码量依然很高，主要集中在以下位置。

#### 输入与请求目标映射

- `InputDecoderService` 里写死了 `QUESTION_FOCUS_MAPPING`、`QUESTION_FOCUS_ALIASES`、`SPECIAL_TYPE_MAPPING`、`DIFFICULTY_MAPPING`
- `/source-question/detect` router 里写死了 stem token 到 `question_focus`、`material_structure`、`text_direction` 的映射

这类逻辑已经不只是系统层格式适配，而是在决定业务目标。

#### slot / difficulty 行为

- `SlotResolverService` 中的 `DIFFICULTY_BIAS`
- `ASCENDING_DIFFICULTY_SLOTS`
- `DESCENDING_DIFFICULTY_SLOTS`

这些规则实际定义了“难度如何作用于 slot”，属于题型协议行为，但现在在 service 中。

#### prompt builder 规则

- few-shot 选取优先级写在代码里
- difficulty 渲染文案写在代码里
- review override、wrong-option confusion 的说明拼装写在代码里

这说明 prompt builder 不只是“模板渲染器”，已经承担了业务解释逻辑。

#### source-question 分析与 business card 归因

- `SourceQuestionAnalyzer` 里有 `_BUSINESS_CARD_MAP`
- `_SENTENCE_ORDER_CARD_IDS`
- `_SENTENCE_FILL_CARD_IDS`
- 多组 marker 集合、阈值、打分、fallback business card

这一块本质上已经在 service 内写了一套“参考题 -> 业务卡/母族/结构约束”的判定器。

#### 材料桥接规则

- `MaterialBridgeService` 实际继承 `MaterialBridgeV2Service`
- `main_idea` 被本地映射到 `title_selection`
- 调 passage V2 检索时 `question_card_id=None`
- `min_card_score` 按 difficulty 写死
- 没有结果时触发 relaxed fallback

这不是 passage_service 的治理细节，而是 prompt_skeleton_service 这边对“如何取材”的控制规则。

#### question generation 规则

`QuestionGenerationService` 内部硬编码了：

- 母题存在时难度默认上调一档
- race candidate 数量
- 对齐重试阈值
- quality repair 重试阈值
- answer grounding rules
- reference hard constraints
- 语序题真值重建
- 语序题固定 6 单元输出修补

这些都不是纯引擎动作，而是业务决策。

#### validator 规则

`QuestionValidatorService` 内部硬编码了：

- 标题题会议/汇报文风 marker
- 标题长度与风格约束
- 语序题 6 单元真值源约束
- 填空题 `bridge_both_sides` 的业务要求

当前 validator 并没有直接消费题卡里的 `validator_contract`。

#### review / control 规则

- `QuestionReviewService` 写死 action 分发与状态跳转
- `MetaService` 额外硬编码 `difficulty_target`、`pattern_id`、`material_policy.preferred_document_genres` 这些控制项
- `/fine-tune` 路由直接等同于 `minor_edit`

这说明 review/control 也不是完全由配置层声明。

### 4.3 哪些 prompt 规则本应来自题卡 / 题型协议，但现在散落在代码里

从当前结构看，至少下面这些规则更像协议层规则，而不是 service 临时逻辑：

- 难度如何投影到 slot
- 某类母题应映射到哪些 `business_card_ids`
- 某题型需要哪些 answer grounding 要求
- 某题型面对母题时应追加哪些 reference hard constraints
- 某题型有哪些题型级 validator contract
- reviewer 可调哪些控制项
- 某题型允许哪些 repair action 影响哪些字段

当前这些规则并没有统一挂在题卡 / 协议层，而是散落在 `input_decoder`、`slot_resolver`、`source_question_analyzer`、`question_generation`、`question_validator`、`meta_service` 中。

### 4.4 当前“配置驱动提示词系统”是否已经滑向“代码驱动提示词系统”

已经部分滑向。

结论不是“配置驱动失效”，而是：

- 类型定义、模板定义、运行时定义仍然是配置驱动
- 但决定 prompt 真实行为的很多关键条件，已经被代码前置、代码补充、代码兜底

所以当前系统更准确的描述是：

它是一个“以配置为表层入口、以 service 代码补足业务规则”的混合型提示词系统，而不再是纯粹的配置驱动提示词系统。

## 5. 当前题目服务与题卡驱动设计的契合度

### 5.1 题卡现在真实决定了什么

如果只看 `prompt_skeleton_service` 当前实际消费到的内容，题卡/配置真实决定的主要是：

- 题型及 subtype 的存在与基本定义
- slot schema 和默认值
- pattern 集合与默认 pattern
- `control_logic`
- `generation_logic`
- few-shot 配置
- prompt 模板集合

也就是说，题卡/配置已经能决定“生成器大体该怎么工作”，但还不能完全决定“服务细节怎么执行”。

### 5.2 母族 / slot 现在真实决定了什么

当前母族 / slot 能比较稳定决定的是：

- 题型入口
- pattern 命中
- skeleton 抽象结构
- 部分 difficulty fit 结果
- prompt_package 中的说明文本

它们已经能影响生成方向，但还没有完全覆盖 review、validator、repair、material bridge 的行为边界。

### 5.3 哪些关键行为已经不是题卡控制，而是 service / action 自己控制

以下关键行为已经明显偏向 service 自控：

- 前端输入到 `question_focus/business_subtype` 的映射
- 参考题到 `business_card_ids` 的打分与 fallback
- 母题触发的默认难度升级
- 材料桥的 family 映射与 relaxed fallback
- review action 的语义边界
- `minor_edit`、`text_modify`、`manual_edit` 的实际权限范围
- validator 的题型业务规则
- reviewer 控制项的补充暴露方式

这意味着“主卡参数决定材料准备、生成、校验、调控”的设计哲学，在当前题目服务中还没有完全落地。

### 5.4 当前新增一张题卡时，是否还能主要靠扩配置完成

结论是：只能部分靠扩配置完成。

如果是现有题型下的轻量 pattern 或少量 slot 变化，当前仍可主要通过：

- `configs/types/*.yaml`
- `configs/prompt_templates.yaml`

来完成。

但如果新增题卡会影响以下任一事项，通常已经需要动服务代码：

- source-question 到 business card 的识别
- 材料桥 family / card 命中策略
- validator 的题型级规则
- action 的权限边界
- answer grounding 或 reference hard constraints
- reviewer 可调 control 列表

所以从真实状态看，当前体系还没有达到“新增一张卡主要靠扩配置”的理想程度。

## 6. 当前题目服务已知问题审计

### 6.1 生成链路问题

- `QuestionGenerationService.generate()` 已经承担了解码、母题分析、材料选择、prompt 拼装、模型调用、重试、校验、评审、持久化等多段职责，链路边界较宽。
- 生成仍然偏“一次性大调用”，模型不只生成题面，还在承担局部修复、结构回补、题型细节对齐等任务。
- race 生成、alignment retry、quality repair retry 都在生成服务内部完成，说明状态管理更多靠 service 内部流程，而不是外部协议显式编排。
- 母题存在时自动上调难度，是 service 直接改生成目标，而不是协议层显式声明。

### 6.2 prompt 层问题

- prompt 相关规则已经散在类型 yaml、prompt templates、prompt builder 代码、question_generation 代码中，边界不单一。
- few-shot 规则虽然有配置入口，但“怎么选、优先级是什么、何时启用”仍由代码决定。
- difficulty 文案不是模板配置，而是 builder 内硬编码。
- answer grounding 与 reference hard constraints 这类高价值 prompt 规则，不在模板和题卡层，而在 `QuestionGenerationService` 中。
- prompt 模板虽然分 action，但 action 的真实语义并不完全由模板控制，而由 service 的上下文拼装和后续 patch 决定。

### 6.3 对象与真值源问题

- `QuestionItem` 已经是统一对象，但跨题型的 truth-source 制度并不完整。
- 语序题已经有 `original_sentences + correct_order` 这类显式真值锚点。
- 其他题型更多仍停留在 `options + answer + analysis` 这一层，缺少同等级别的结构化真值字段。
- `answer` 同时承担“展示答案”和“最终判断锚点”角色，边界偏弱。
- `analysis` 是解释字段，但在修订链路里也会被一起整体回生，容易和真值层漂移。
- `metadata` 是开放字段，容易沦为杂项承载点。

### 6.4 action / review / repair 问题

- `minor_edit` 名义上是轻修，实际要求模型返回完整更新后的 `GeneratedQuestion`，动作权限明显偏宽。
- `question_modify` 是控制项重改，但仍会重新跑题目生成，不是局部字段重算。
- `text_modify` 可以更换材料并整题重建，本质更接近“局部入口下的重生成”。
- `manual_edit` 权限非常大，既能改题面和答案，也能改材料相关字段，已经跨越题目层和材料引用层边界。
- `/fine-tune` 只是 `minor_edit` 的别名，名称与真实语义不完全一致。
- `approve/confirm` 只看当前状态与 validation 结果，不区分不同 action 之后的字段可信度等级。

### 6.5 validator / 校验问题

- validator 已有通用校验框架，不是空白。
- 语序题校验最成型，已经会检查 `original_sentences`、`correct_order`、唯一真值选项等。
- 标题/主旨题已有一些业务型规则，例如会议文风过滤、标题长度风格约束，但这些更像经验规则，不完全是协议化真值校验。
- 填空题对 `bridge_both_sides` 有针对性规则，说明题型校验开始形成，但仍是代码分支式实现。
- 当前 validator 没有直接使用 question card 的 `validator_contract`，导致题卡层与校验层没有真正对齐。
- 跨题型的“truth source vs derived fields”校验制度还没有成型。

### 6.6 配置与服务职责混杂问题

- `InputDecoderService` 已经承载题型映射规则。
- `/source-question/detect` router 已经承载业务推断逻辑。
- `SourceQuestionAnalyzer` 已经承载业务卡打分与 fallback 规则。
- `MaterialBridgeService` 已经承载 family 映射、question card 留空、fallback 取材规则。
- `MetaService` 已经承载控制项补丁。
- `QuestionGenerationService` 已经承载题型级 answer grounding、母题 hard constraint、语序题结构补丁、质量门控。
- `QuestionValidatorService` 已经承载一批本应来自 validator contract 的题型规则。
- `QuestionReviewService` 结构清晰，但 action 对应的真实权限边界并没有在协议层显式声明。
- `question_generation.py` 末尾存在“Clean override implementations...”这类覆盖式注释，说明历史补丁已经开始侵蚀文件结构。

## 7. 当前最危险的结构性风险

- 风险点：母题分析到业务卡映射大量硬编码在 `SourceQuestionAnalyzer`
  - 为什么危险：它直接决定取材方向、约束方向和部分 pattern 方向，但不受题卡层统一控制
  - 影响层：题卡驱动层、材料桥接层、生成层
  - 类型判断：更像源头问题

- 风险点：`minor_edit` 实际整题回生
  - 为什么危险：动作名称暗示轻修，但真实行为会重算题面、选项、答案、解析，容易造成字段漂移
  - 影响层：review 层、对象真值层、版本层
  - 类型判断：更像源头问题

- 风险点：`manual_edit` 可直接修改题目字段和材料字段
  - 为什么危险：它绕过了原有生成与校验边界，使题目本体和材料引用同时可被手工 patch
  - 影响层：review 层、对象层、材料引用层
  - 类型判断：更像源头问题

- 风险点：跨题型真值锚点制度不完整
  - 为什么危险：只有语序题具备较强结构化真值，其他题型更依赖 `answer + analysis` 组合，后续 repair 容易漂
  - 影响层：对象层、validator 层、review 层
  - 类型判断：更像源头问题

- 风险点：配置驱动外壳下混入大量 service 规则
  - 为什么危险：看起来可配置，实际新增题卡或新母族时经常需要改代码，破坏“服务做执行器”的边界
  - 影响层：配置层、service 层、题卡驱动层
  - 类型判断：更像源头问题

- 风险点：`MaterialBridgeService` 本地写死 family 映射与 `question_card_id=None`
  - 为什么危险：题目服务在取材入口就丢失了 question card 级约束，导致题卡驱动到材料层的链路变弱
  - 影响层：材料桥接层、题卡约束层
  - 类型判断：更像源头问题

- 风险点：validator 没有直接消费 `validator_contract`
  - 为什么危险：题卡层和校验层会长期分叉，题卡写了约束，不等于服务真正按它校验
  - 影响层：协议层、validator 层
  - 类型判断：更像源头问题

- 风险点：`QuestionGenerationService` 单文件承担职责过宽
  - 为什么危险：生成、重试、材料修补、题型 patch、review 相关修订都在一个大服务里，局部补丁会持续累积
  - 影响层：编排层、服务层、可维护性
  - 类型判断：更像症状问题，但由前面多项源头问题共同推动

- 风险点：状态字段有双轨语义
  - 为什么危险：`current_status` 与 `statuses.review_status` 并存，后续动作和 UI 可能读不同状态
  - 影响层：对象层、repository 层、前端展示层
  - 类型判断：更像症状问题

## 8. 题目服务“归位建议”

这里只做归位判断，不展开重构方案。

### 8.1 应留在题目服务系统层的

- registry 加载与 schema 校验
- prompt orchestration 的通用调度
- 通用 slot 合并与类型校验
- LLM gateway
- repository / versions / action log / snapshots
- review API、query API、delivery/export API
- 通用 validator 框架
- 通用 action 分发框架

### 8.2 应上升到题型协议 / 题卡 / 配置层的

- 输入标签到题型目标的映射
- source-question 到 business card / family / structure constraint 的判定规则
- 难度到 slot 的投影规则
- reviewer 可见控制项及其 action 权限
- 题型级 answer grounding 规则
- 题型级 reference hard constraints
- 题型级 validator contract
- 题型 truth-source 定义
- 题目服务到材料服务的 `question_card_id` / family 绑定规则

### 8.3 应删除、回收或标记为临时补丁的

- `/source-question/detect` 中 router 级业务推断块
- `main_idea -> title_selection` 这种 service 内 family 直映射
- `question_card_id=None` 的取材调用方式
- `/fine-tune` 只是 `minor_edit` 别名这一层语义重复
- `question_generation.py` 末尾的覆盖式实现注释块
- `MetaService` 中额外注入但未在协议层声明的控制项补丁
- `manual_edit` 对材料字段的直接 patch 能力，至少应被清晰标记为越权临时能力

## 9. 一个简明结论

当前 `prompt_skeleton_service` 最像一个“已经长成完整题目编排中心，但仍保留大量补丁式业务规则的混合服务”。它不是没有统一对象，也不是没有配置驱动；相反，它已经有 `QuestionItem`、version、snapshot、review action 这套比较完整的对象流转体系。最不符合设计哲学的地方，在于很多本应由题卡、题型协议、validator contract、控制协议决定的业务规则，已经散落并固化在 `input_decoder`、`source_question_analyzer`、`material_bridge`、`question_generation`、`question_validator`、`meta_service` 中。后续最应该先修的，不是某个题型 bug，而是“配置层与服务层的边界失真”和“跨题型真值源制度不完整”这两类源头问题。
