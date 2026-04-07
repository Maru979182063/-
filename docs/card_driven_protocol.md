# 题卡驱动协议

## 1. 文档目的

本文件用于把当前项目中的“题卡驱动各服务组合”正式写成团队维护协议。

本协议服务于当前仓库的真实结构：

- `card_specs/normalized/question_cards/`
- `card_specs/normalized/material_cards/`
- `card_specs/normalized/signal_layers/`
- `card_specs/business_feature_slots/`
- `prompt_skeleton_service/configs/types/`
- `passage_service/`
- `prompt_skeleton_service/`

本协议不讨论理想化架构，只定义当前项目后续维护时的职责边界和改动许可范围。

## 2. 术语

### 2.1 题卡

本项目中的“题卡”指标准化 question card，当前位于：

- `card_specs/normalized/question_cards/*.yaml`

题卡是单道题生产协议的主入口，负责把“某一母族题目如何消费材料、如何绑定运行时题型、如何约束生成和校验”写成稳定契约。

### 2.2 母族

本项目中的“母族”指统一题型家族与其协议骨架，当前主要体现在：

- `prompt_skeleton_service/configs/types/*.yaml`
- `card_specs/business_feature_slots/README.md` 中的 `mother_family_id`

当前母族包括：

- `main_idea`
- `continuation`
- `sentence_order`
- `sentence_fill`

母族是“这类题整体长什么样、有哪些公共 slot、有哪些 pattern、难度如何投影”的定义层。

### 2.3 题型协议

本项目中的“题型协议”指母族运行协议，当前载体是：

- `prompt_skeleton_service/configs/types/*.yaml`

它不是单张题卡，而是该母族所有题卡共同遵守的运行骨架。

### 2.4 业务特征卡

业务特征卡当前位于：

- `card_specs/business_feature_slots/**/*.yaml`

它负责把材料/业务特征投影为：

- `type_slots`
- `pattern_candidates`
- `prompt_extras`

业务特征卡是材料侧向题目侧投射约束的中间协议层。

## 3. 总原则

本项目的正式协议是：

**题卡决定组合规则，母族决定公共协议，服务只执行协议，不定义题意。**

具体落地为：

- 题卡驱动材料卡、业务卡、运行时绑定和校验契约
- 母族驱动 slot、pattern、难度和通用 prompt skeleton
- 服务只做加载、组合、执行、持久化、调用和返回
- prompt、validator、review action 不得私自生成新的题型规则来源

## 4. 题卡决定什么

题卡负责定义“这张题如何工作”，至少包括以下权限。

### 4.1 运行时绑定

题卡决定：

- 对应的 `question_type`
- 对应的 `business_subtype`

当前载体：

- `runtime_binding`

服务不应再在运行时另起一套题型绑定逻辑替代它。

### 4.2 上游材料契约

题卡决定本题可消费什么材料，至少包括：

- `required_candidate_types`
- `required_profiles`
- `preferred_material_cards`

当前载体：

- `upstream_contract`

服务可以依据契约过滤材料，但不应私自补出另一套题卡外材料标准。

### 4.3 题面生成原型

题卡决定该题从哪类 material card 的 archetype 进入生成，至少包括：

- `generation_archetype_source`
- `material_card_overrides`

服务可以读取和执行，不应在 service 中重新硬写同等级的 archetype 选择规则。

### 4.4 校验补充契约

题卡决定该题附加的业务校验规则，至少包括：

- `validator_contract`

题卡是“这张题需要额外检查什么”的正式来源。

如果 validator 要执行题卡特定规则，应优先消费题卡契约，而不是直接在代码里写死题目口径。

## 5. 母族决定什么

母族负责定义“这一类题的公共协议”。

### 5.1 公共 slot 结构

母族决定：

- 本题型有哪些 `slot_schema`
- 每个 slot 的类型、允许值、默认值、说明

当前载体：

- `prompt_skeleton_service/configs/types/*.yaml`

### 5.2 公共 pattern 集

母族决定：

- 有哪些 pattern
- pattern 的 `match_rules`
- pattern 的 `control_logic`
- pattern 的 `generation_logic`

pattern 是母族级公共生成骨架，不是 service 层临时拼出来的策略集。

### 5.3 难度协议

母族决定：

- `difficulty_target_profiles`
- 各 pattern 的 `difficulty_rules`

服务可以做计算和投影，不应发明题型外的难度语义。

### 5.4 fewshot 和 skeleton

母族决定：

- `skeleton`
- `fewshot_policy`
- `default_fewshot`

服务负责装配，不负责改写母族的骨架定义。

## 6. 题型协议决定什么

本项目里，“题型协议”就是母族运行协议加 question card 契约的组合口径。它共同决定以下事项。

### 6.1 题目对象的公共字段

题型协议决定：

- 必须产出的公共结构
- 哪些 `type_slots` 能进入出题流程
- 哪些 pattern 可被选择

### 6.2 控制逻辑与生成逻辑来源

题型协议决定：

- `control_logic` 从 pattern 来
- `generation_logic` 从 pattern 来
- 页面控制项从题型配置导出

服务只能读取并透传这些定义。

### 6.3 校验口径的分层

题型协议决定：

- 母族级通用校验由题型协议承担
- 题卡级特殊校验由题卡契约承担

validator 应是协议执行器，不应成为协议创造者。

## 7. 服务只负责什么

## 7.1 `passage_service` 只负责

- 文章入库、抓取、分段、tag
- 材料池治理与检索
- 按题卡、材料卡、业务卡组合出 `question_ready_context`
- 把业务特征卡的 `slot_projection` 和 `prompt_extras` 投影给下游

它不负责定义新的题目语义。

### 7.2 `prompt_skeleton_service` 只负责

- 加载母族配置
- 解码请求
- resolve slot
- 选择 pattern
- 组装 prompt skeleton
- 请求 `passage_service` 取材
- 调用模型生成
- 调用 validator 与 judge
- 执行 review action
- 持久化版本和批次

它不负责绕开题卡重新发明题型规则。

### 7.3 前端只负责

- 参数录入
- 结果展示
- 调用接口
- 对 review action 做交互承载

前端不应成为题型规则和业务口径的定义来源。

## 8. 哪些逻辑不得逃逸到 service / prompt / validator

以下逻辑不得在 `service`、`prompt`、`validator` 中新增第二来源。

### 8.1 不得逃逸到 service 的逻辑

- 题卡与母族的正式绑定关系
- question card 的选择规则
- 哪些材料卡可用于某题
- 哪些 business card 对应某题
- 题型核心 archetype 选择规则
- 题卡级业务规则

service 可以执行、过滤、评分、排序，但不应重写协议。

### 8.2 不得逃逸到 prompt 的逻辑

- 某题型的正式业务定义
- 某张题卡的唯一真值来源
- 校验口径本身
- review action 的结构边界

prompt 只应表达已有协议，不应补出“只有 prompt 知道”的业务事实。

### 8.3 不得逃逸到 validator 的逻辑

- 题卡专属业务要求的新增定义
- 题型 pattern 的额外分支规则
- 题卡应消费的材料范围
- 业务特征卡的投影含义

validator 可以检查协议是否被满足，但不应自己定义协议。

### 8.4 不得逃逸到 review action 的逻辑

- 新的题型规则
- 新的题卡语义
- 新的生成路径
- 新的材料消费标准

review action 只负责在既有协议下触发：

- `minor_edit`
- `question_modify`
- `text_modify`
- `manual_edit`
- `confirm`
- `discard`

而不应扩展为新的协议层。

## 9. 新增一张卡时允许改哪些层

这里的“新增一张卡”，指新增一张 question card 或 business feature card，但不改变现有母族基本结构。

### 9.1 允许修改的层

- `card_specs/normalized/question_cards/`
  新增或调整 question card
- `card_specs/normalized/material_cards/`
  若新卡需要新的材料卡承接，可补材料卡
- `card_specs/business_feature_slots/`
  新增或调整业务特征卡及其 `slot_projection`
- `card_specs/normalized/signal_layers/`
  若新卡需要新增信号层字段，可补信号层
- `prompt_skeleton_service/configs/types/*.yaml`
  仅当新卡依赖母族现有协议中尚未暴露的 slot / pattern / fewshot / difficulty 配置时，可在母族配置内补充
- 文档层
  应同步更新相关维护文档

### 9.2 允许的改动性质

- 补配置
- 补卡
- 补 slot 投影
- 补 pattern 映射
- 补说明文档

前提是：

- 不改变服务职责边界
- 不新增并行架构
- 不把配置逻辑重新写回 service

## 10. 新增一张卡时不允许改哪些层

### 10.1 不允许直接改 service 来“容纳新卡”

新增一张卡，不应默认去改：

- `question_generation.py`
- `material_bridge_v2.py`
- `question_validator.py`
- `question_review.py`
- 前端脚本

除非当前服务确实缺少“读取协议”的通用能力，而不是缺少某张卡的特判。

### 10.2 不允许给新卡补专属硬编码

新增一张卡时，不允许：

- 在 service 中增加 `if card_id == ...`
- 在 prompt 中增加只对某张卡生效但配置层无出处的口径
- 在 validator 中增加卡级规则但题卡里没有对应契约
- 在前端增加该卡专属的页面分支

### 10.3 不允许改成“卡一来，代码跟着分叉”

新增卡应当优先验证：

- 现有母族协议是否足够承载
- 现有 question card 契约是否足够表达
- 现有 business feature card 投影是否足够表达

如果足够，则只改卡和配置，不改执行器。

## 11. 例外条件

只有在以下情况，新增一张卡才允许触发 service 层改动：

### 11.1 通用执行能力缺失

例如：

- 现有服务根本不能读取某个协议字段
- 现有 bridge 根本不能传递题卡上下文
- 现有 validator 根本不能消费题卡契约

这类改动应当是“补通用引擎能力”，而不是“给新卡写特判”。

### 11.2 母族协议本身缺字段

如果新卡无法被现有母族 slot / pattern 表达，可以先补母族协议，再补卡。

但仍然不应直接绕过配置去写死到 service。

## 12. 当前项目的维护执行口径

基于当前仓库现状，团队维护时应遵守以下口径：

- 题卡是组合规则来源
- 母族是公共协议来源
- 业务特征卡是材料特征到题目参数的投影来源
- 服务是执行器，不是题意定义器
- validator 是校验器，不是协议发明器
- prompt 是表达器，不是规则总源
- 前端是工作台，不是业务规则层

如果某项规则无法明确回答“它属于题卡、母族、业务特征卡还是系统层”，则不应直接写进 service。

## 13. 一句话协议

本项目后续维护的统一口径是：

**题卡决定题目组合，母族决定公共协议，业务卡决定特征投影，服务只执行，不私自定义题意。**
