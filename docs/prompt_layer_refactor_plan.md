# Prompt 层归位说明

## 1. 文档目的

本文档只服务于当前仓库的长期维护，目标不是设计一套新的 prompt 架构，而是把现有仓库里与 prompt 相关的真实逻辑分布盘点清楚，并给出统一的“回收口径”：

- 哪些逻辑本来就是服务引擎职责
- 哪些逻辑应当留在模板层
- 哪些逻辑属于题型协议层
- 哪些逻辑属于题卡配置层
- 哪些业务规则当前被硬编码在代码里
- 这些规则后续应当优先回收到 `jsonl`、`yaml`、`card` 的哪个载体

本文不讨论未来大重构，也不改变当前文件职责边界；只定义后续整理 prompt 层时的归位标准。

## 2. 当前 prompt 相关逻辑分布

### 2.1 服务引擎层

当前服务引擎层主要位于：

- `prompt_skeleton_service/app/services/config_registry.py`
- `prompt_skeleton_service/app/services/runtime_registry.py`
- `prompt_skeleton_service/app/services/prompt_template_registry.py`
- `prompt_skeleton_service/app/services/prompt_orchestrator.py`
- `prompt_skeleton_service/app/services/prompt_builder.py`
- `prompt_skeleton_service/app/services/slot_resolver.py`
- `prompt_skeleton_service/app/services/question_generation.py`
- `prompt_skeleton_service/app/services/material_bridge_v2.py`
- `prompt_skeleton_service/app/services/question_validator.py`
- `prompt_skeleton_service/app/services/question_review.py`
- `prompt_skeleton_service/app/services/source_question_analyzer.py`
- `passage_service/app/services/card_registry_v2.py`
- `passage_service/app/services/material_pipeline_v2.py`

这些文件里，真正属于“服务引擎”的职责包括：

- 加载配置和题卡
- 解析输入
- 解析题型和 business subtype
- 解析 slot、选择 pattern、拼装 prompt 包
- 调用 passage service 取材
- 向模型发送 prompt
- 接收结果并做格式化、持久化、版本记录
- 按协议执行校验和 review action

这些职责的共同特点是：它们负责“执行协议”，而不负责定义“业务规则本身”。

### 2.2 模板层

当前模板层主要位于：

- `prompt_skeleton_service/configs/prompt_templates.yaml`
- `passage_service/app/prompts/*.md`

其中：

- `prompt_templates.yaml` 承载了出题、微调、换材重做、judge review 等动作模板，是 prompt_skeleton_service 侧的模板注册表
- `passage_service/app/prompts/` 承载了材料侧的 tagging、family prompt、segment refine、integrity gate 等提示模板

模板层应负责：

- 用自然语言表达执行动作
- 把已经确定的协议字段组织成模型可消费的 prompt
- 提供可替换的模板文案

模板层不应负责：

- 定义题型真值规则
- 决定题卡映射关系
- 决定某个 business feature 该投影到哪些 slot
- 决定 validator 的业务判定标准

### 2.3 题型协议层

当前题型协议层主要位于：

- `prompt_skeleton_service/configs/types/*.yaml`
- `card_specs/normalized/question_cards/*.yaml`

其中两部分职责不同：

- `configs/types/*.yaml` 定义母族级协议：`slot_schema`、`default_slots`、`patterns`、`difficulty_target_profiles`、`control_logic`、`generation_logic`、`fewshot_policy`
- `question_cards/*.yaml` 定义单题卡绑定协议：`runtime_binding`、`upstream_contract`、`generation_archetype_source`、`validator_contract`

题型协议层应负责：

- 规定一个母族有哪些公共 slot
- 规定 pattern 是什么、如何命名、如何被选择
- 规定上游取材需要满足什么契约
- 规定下游生成和校验要消费哪些协议字段
- 规定 validator 应校验哪些题型级约束

题型协议层不应负责：

- 具体材料候选的挑选实现
- 模型调用细节
- 页面交互逻辑

### 2.4 题卡配置层

当前题卡配置层主要位于：

- `card_specs/business_feature_slots/**/*.yaml`
- `card_specs/normalized/material_cards/*.yaml`
- `card_specs/normalized/signal_layers/*.yaml`
- `card_specs/normalized/question_cards/*.yaml`

这一层已经在仓库中承担了最接近“题卡驱动”的职责：

- 由 question card 指定当前题卡绑定哪个 signal layer、material cards、runtime binding
- 由 business feature card 把材料侧特征投影到 `type_slots`
- 由 business feature card 提供 `pattern_candidates` 和 `prompt_extras`
- 由 material card 和 signal layer 约束可被召回和进入出题链路的材料形态

题卡配置层应负责的是“业务表达”，而不是“执行实现”。

## 3. 当前各层的职责边界

### 3.1 哪些属于服务引擎层

下列内容继续留在服务引擎层是合理的：

- 配置和题卡的读取、缓存、注册
- slot resolve 的通用流程
- prompt 模板查找和渲染
- passage service 调用、候选过滤、结果封装
- LLM 调用、结果解析、存储、版本化
- review action 分发和审计记录
- 通用格式校验和异常处理

这些是“怎么执行”的问题，不是“题目应该长什么样”的问题。

### 3.2 哪些属于模板层

下列内容应当留在模板层：

- `generate`、`minor_edit`、`question_modify`、`text_modify`、`judge_review` 的提示骨架
- 材料侧 tagging、family prompt、integrity gate、segment refine 的提示骨架
- few-shot 示例引用方式
- 模板中的文案风格、指令分段和输出格式约束

模板层只表达“说法”，不表达“业务事实”。

### 3.3 哪些属于题型协议层

下列内容应当归题型协议层维护：

- 题型共有的 slot 定义
- difficulty 到 slot 偏移的规则
- pattern 列表和 pattern 语义
- control logic / generation logic 的结构化定义
- validator_contract
- generation_archetype_source
- upstream_contract

凡是“同一母族内所有题卡都应遵守”的内容，都不应继续写死在单个 service 里。

### 3.4 哪些属于题卡配置层

下列内容应当归题卡配置层维护：

- 某张题卡对应哪类材料
- 某个 business feature 应投影到哪些 `type_slots`
- 某张题卡允许哪些 `pattern_candidates`
- 某类材料应提供哪些 `prompt_extras`
- 某个 signal layer 和 material cards 的绑定
- 某个题卡对 validator_contract 的具体启用方式

凡是“这张卡和那张卡可以不同”的内容，都不应继续写死在公共 service 里。

## 4. 当前被硬编码的业务规则

### 4.1 `source_question_analyzer.py`

当前硬编码内容包括：

- business card id 映射
- 句序题、填空题、主旨题的 marker 集
- 基于 marker 和结构特征的 business feature 推断
- 若干分值和阈值

这些规则本质上是“参考题特征到 business feature card 的映射规则”，属于业务判读，不属于服务执行引擎。

### 4.2 `material_bridge_v2.py`

当前硬编码内容包括：

- 取材时固定 `question_card_id=None`
- `main_idea` 直接映射到 `title_selection` family
- 按难度返回 `_min_card_score`
- relaxed fallback 时直接清空部分查询约束

这些规则本质上是“题型到材料检索协议的绑定逻辑”，应尽量回收到题卡或运行协议中。

### 4.3 `prompt_skeleton_service/app/routers/questions.py`

当前硬编码内容包括：

- 从 `business_card_ids` 推断 `material_structure`
- 基于字符串特征推断 `text_direction`

这些规则本质上属于参考题解析和业务投影，不应长期停留在路由层。

### 4.4 `slot_resolver.py`

当前硬编码内容包括：

- `DIFFICULTY_BIAS`
- `ASCENDING_DIFFICULTY_SLOTS`
- `DESCENDING_DIFFICULTY_SLOTS`

这里已经接近题型协议层，但当前仍以代码常量形式存在；只要不同母族对难度和 slot 的联动方式不同，这些规则就应从代码常量回收到题型协议配置。

### 4.5 `question_generation.py`

当前硬编码内容包括：

- 参考题出现时的难度上调逻辑
- answer grounding contract 的拼接规则
- 句序题按 business feature 回推 pattern 的规则
- 某些题型的材料精修和重构规则
- 句序题真值重建与展示重排逻辑
- 末尾“覆盖旧实现”的后置 override 段

这部分是当前 prompt 层最混杂的区域：既有引擎动作，也夹带了题型业务规则和补丁式收口。

### 4.6 `question_validator.py`

当前硬编码内容包括：

- 标题题会议文风 marker
- 标题长度与风格阈值
- 填空题 `bridge_both_sides` 的解析要求
- 句序题 `correct_order` 的特殊校验

这些规则本质上属于 validator_contract 或题型协议层，不应继续只存在于 validator 代码里。

### 4.7 `question_review.py` 与 control 相关 service

当前硬编码内容包括：

- review action 集合和动作分发
- 控制面板额外注入的 `difficulty_target`、`pattern_id`、`material_policy.preferred_document_genres`

action 分发本身属于引擎，但哪些动作暴露、暴露哪些控制项，已经触及题型协议和题卡控制范围。

## 5. 哪些逻辑不得继续逃逸到 service / prompt / validator

后续整理 prompt 层时，下列逻辑不应再继续新增到 `service`、`prompt`、`validator` 中：

- business feature 到 `type_slots` 的投影规则
- business feature 到 `pattern_candidates` 的绑定关系
- 某题型的真值锚点定义
- 某题型的 validator 业务判据
- 某题型的 family 映射关系
- 某张题卡专属的 prompt extras
- 某题型的难度到 pattern/slot 的映射规则
- 某类题卡的候选材料最低结构要求

这些内容都属于协议或题卡配置，不属于执行层临时判断。

## 6. 回收口径：后续如何回收到 `jsonl` / `yaml` / `card` 中

### 6.1 回收到 `yaml` 的内容

适合回收到 `yaml` 的，是“稳定的结构化协议”，包括：

- 题型级 slot schema
- difficulty 与 slot/pattern 的映射规则
- control logic / generation logic
- validator_contract
- generation_archetype_source
- runtime binding
- review action 的模板定义
- prompt 模板注册关系

当前最直接的落点是：

- `prompt_skeleton_service/configs/types/*.yaml`
- `prompt_skeleton_service/configs/prompt_templates.yaml`
- `prompt_skeleton_service/configs/question_runtime.yaml`
- `card_specs/normalized/question_cards/*.yaml`

### 6.2 回收到 `card` 的内容

适合回收到 `card` 的，是“按卡变化的业务规则”，包括：

- 某 business feature 对应哪些 `type_slots`
- 某 business feature 允许哪些 `pattern_candidates`
- 某 business feature 提供哪些 `prompt_extras`
- 某 question card 绑定哪些 material cards / signal layer
- 某 question card 需要什么 upstream contract
- 某 question card 启用什么 validator contract

当前最直接的落点是：

- `card_specs/business_feature_slots/**/*.yaml`
- `card_specs/normalized/question_cards/*.yaml`
- `card_specs/normalized/material_cards/*.yaml`
- `card_specs/normalized/signal_layers/*.yaml`

### 6.3 回收到 `jsonl` 的内容

当前仓库中的 `jsonl` 主要出现在导出清单中，还不是 prompt 协议主载体。

后续如果需要把一部分 prompt 相关内容回收到 `jsonl`，适合承载的是“样本型资产”，而不是“协议型资产”，例如：

- few-shot 示例集
- judge 样例集
- repair 样例集
- prompt 对照实验样本
- 题卡命中样例和失败样例

`jsonl` 适合存“条目集合”，不适合存“单一真值协议”。

因此，下列内容不建议以 `jsonl` 为主载体：

- 题型协议
- question card 契约
- validator_contract
- family 映射
- slot projection 规则

## 7. 归位优先级

后续整理 prompt 层时，建议遵守以下优先级，而不是同时到处改：

1. 先把“硬编码业务规则”识别为协议、题卡、模板三类。
2. 先回收同一母族共用的规则到 `configs/types/*.yaml` 或 `question_cards/*.yaml`。
3. 再回收单卡差异规则到 `business_feature_slots/**/*.yaml`。
4. 只有样本类内容，才考虑新增 `jsonl`。
5. 服务层只删除已经被配置接管的分支判断，不新增新的业务判断。

这份优先级不是重构路线图，而是后续每次局部整理 prompt 层时的统一判断口径。

## 8. 当前主结论

当前仓库并不是没有 prompt 分层，而是已经有四层并存：

- 服务引擎层负责执行
- 模板层负责表达
- 题型协议层负责母族级约束
- 题卡配置层负责单卡级差异

当前主问题不是“没有模板”，也不是“没有题卡”，而是仍有一批业务规则滞留在 `service`、`prompt`、`validator` 的代码分支里，导致 prompt 层出现协议外溢。

后续整理的目标不应是重写流程，而应是把这些已经识别出的业务规则，按本文件的口径，逐步回收到 `yaml`、`card`、`jsonl` 各自应承载的位置。
