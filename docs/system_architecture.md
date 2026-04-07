# 项目系统架构说明

## 1. 项目目标

本项目的目标不是单独做一个“出题接口”或“材料库接口”，而是维护一条可长期演进的题目生产链：

- 以外部文章、段落、句群为材料来源
- 通过题卡、材料卡、业务特征卡完成材料筛选与出题约束投影
- 由题型配置驱动 prompt 组装、题目生成、校验、评审和交付
- 保持页面结构、服务结构、配置结构可追踪，便于团队持续维护

当前仓库已经形成“双服务 + 外置题卡/配置 + 单页工作台”的基础形态，但仍处于快速迭代阶段，部分规则已经回流到 service、validator、review action 和前端补丁中。

## 2. 顶层模块职责

### 2.1 仓库顶层

- `card_specs/`
  题卡与卡规范库。保存标准化后的 `signal_layers`、`material_cards`、`question_cards`，以及业务特征卡 `business_feature_slots`。
- `docs/`
  项目文档目录。当前包含材料池接入说明；本文件用于补充系统级结构说明。
- `passage_service/`
  材料服务。负责文章入库、切片、tag、材料池治理和 V2 检索。
- `prompt_skeleton_service/`
  出题与评审服务。负责题型配置加载、prompt 组装、取材、生成、校验、评审、版本记录和前端工作台。
- `scripts/`
  顶层运维与分析脚本，主要用于题卡回填、覆盖率检查、试跑、报表导出和 demo 启动。
- `reports/`
  运行报告、回归结果、失败记录、试验输出。
- `logs/`
  运行日志目录。
- `tmp_truth_docs/`
  临时真值材料与抽取文件，不属于正式运行模块。

### 2.2 前端、服务、配置、文档、脚本位置

- 前端
  位于 `prompt_skeleton_service/app/demo_static/`。
  当前实际入口为 `index.html + app_v2.js + app_v2_zh_patch.js`，由 `prompt_skeleton_service/app/routers/demo.py` 挂载到 `/demo`。
- 服务
  `passage_service/app/` 和 `prompt_skeleton_service/app/`。
- 配置
  `prompt_skeleton_service/configs/`、`passage_service/app/config/`、`card_specs/`。
- 文档
  `docs/`。
- 脚本
  顶层 `scripts/` 与 `passage_service/scripts/`。

## 3. 双服务关系

### 3.1 `passage_service` 的职责

`passage_service` 是材料侧系统，职责是把“文章”加工成“可出题材料”。

当前负责：

- 文章导入与抓取
- 分段与候选切片生成
- 通用和题族相关 tag
- 材料池治理、反馈、重处理、导出
- V1 材料检索
- V2 材料检索与题卡上下文构造

V2 链路中，它已经不是只返回文本，而是返回带 `question_ready_context` 的候选项，包括：

- `question_card_id`
- `runtime_binding`
- `selected_material_card`
- `selected_business_card`
- `generation_archetype`
- `resolved_slots`
- `prompt_extras`
- `validator_contract`

也就是说，`passage_service` 当前已经承担了“按题卡组合材料、材料卡、业务卡，并向下游投影出题上下文”的职责。

### 3.2 `prompt_skeleton_service` 的职责

`prompt_skeleton_service` 是题目生产与评审侧系统，职责是把“出题请求 + 材料候选”变成“可评审、可回溯、可交付的题目对象”。

当前负责：

- 题型配置加载
- `type_slots` 解析与 pattern 选择
- prompt skeleton 构建
- 调用 `passage_service` 取材
- 调用模型生成题目
- 结构化解析、校验、LLM judge
- review action 执行
- 题目版本、批次、review action 持久化
- review 查询与 delivery/export
- demo 工作台页面挂载

### 3.3 双服务当前关系

当前主关系是：

1. `prompt_skeleton_service` 接收页面或接口请求
2. 本地根据题型配置解析 `question_type / business_subtype / pattern / slots`
3. 调用 `passage_service` 的 `/materials/v2/search`
4. 取回带 `question_ready_context` 的材料候选
5. 使用本地 prompt 模板和运行时策略生成题目
6. 本地校验、评审、保存版本和批次

当前口径上，`passage_service` 负责“材料与卡片侧组合”，`prompt_skeleton_service` 负责“题目与评审侧组合”。

## 4. 分层关系

本项目当前可以拆成五层，再加一个前端工作台层。

### 4.1 前端层

位置：`prompt_skeleton_service/app/demo_static/`

作用：

- 提供单页工作台
- 收集出题参数与参考母题
- 展示结果卡
- 触发控制调节、换材、手工编辑、通过/作废、导出

当前实际页面是一个单页三屏：

- `builderScreen`
- `loadingScreen`
- `resultScreen`

### 4.2 编排层

位置：`prompt_skeleton_service/app/routers/` 与 `prompt_skeleton_service/app/services/` 中的 orchestrator/generation/review 入口

作用：

- 请求解码
- slot resolve
- pattern 选择
- prompt build
- 材料检索调用
- 生成、校验、评审流程编排

代表文件：

- `prompt_orchestrator.py`
- `question_generation.py`
- `question_review.py`
- `material_bridge_v2.py`

### 4.3 服务层

位置：

- `passage_service/app/domain/services/`
- `passage_service/app/services/`
- `prompt_skeleton_service/app/services/`

作用：

- 材料加工与检索
- 出题生成
- 校验
- 评审
- 版本记录
- 查询与导出

### 4.4 配置层

位置：

- `prompt_skeleton_service/configs/types/*.yaml`
- `prompt_skeleton_service/configs/prompt_templates.yaml`
- `prompt_skeleton_service/configs/question_runtime.yaml`
- `passage_service/app/config/*.yaml`
- `card_specs/normalized/*.yaml`
- `card_specs/business_feature_slots/**/*.yaml`

作用：

- 定义题型 slot
- 定义 pattern
- 定义难度映射
- 定义 prompt 模板
- 定义运行时 provider 和 routing
- 定义材料卡、题卡、业务特征卡
- 定义业务卡到 `type_slots / prompt_extras` 的投影

### 4.5 校验层

位置：

- `prompt_skeleton_service/app/services/question_validator.py`
- `prompt_skeleton_service/app/services/evaluation_service.py`
- `card_specs/normalized/question_cards/*.yaml` 中的 `validator_contract`

作用：

- 检查题目结构合法性
- 检查题型业务逻辑
- 检查难度拟合
- 执行 LLM judge

### 4.6 当前层间关系

当前主关系是：

- 前端层发请求给编排层
- 编排层读取配置层
- 编排层调用服务层
- 服务层部分行为受配置层约束
- 生成结果再进入校验层
- 校验结果回流到编排层和前端层

需要注意的是：当前校验层与服务层之间仍有部分规则重复，题卡里的 `validator_contract` 还没有成为校验唯一入口。

## 5. 当前主流转

### 5.1 出题主流转

当前主流转如下：

1. 页面在 `/demo` 填写题型、难度、文体方向、材料结构、数量，以及可选参考母题
2. 前端将请求发到 `POST /api/v1/questions/generate`
3. `prompt_skeleton_service` 先做输入解码、slot resolve、pattern 选择和 prompt skeleton 构建
4. `question_generation.py` 调用 `material_bridge_v2.py`
5. `material_bridge_v2.py` 请求 `passage_service` 的 `/materials/v2/search`
6. `passage_service` 读取 question card、signal layer、material cards、business cards，构造 `question_ready_context`
7. `prompt_skeleton_service` 取回候选材料后调用模型生成结构化题目
8. 生成结果进入 `question_validator.py` 和 `evaluation_service.py`
9. 通过的题目与版本快照写入本地 SQLite
10. 页面展示结果卡，并继续允许 review action

### 5.2 Review 主流转

结果页当前支持四类后续操作：

- `question_modify`
  按控制项重做题目
- `text_modify`
  更换备选材料或手贴材料重做
- `manual_edit`
  直接保存手工编辑版本
- `confirm / discard`
  通过或作废

所有动作最终都回到 `question_review.py -> question_generation.py` 执行，并写入版本表与 review action 表。

## 6. 当前主矛盾

当前项目的主矛盾，不是“双服务是否成立”，也不是“题卡是否存在”，而是：

**题卡驱动架构已经搭出来了，但规则主权还没有完全收回到题卡/配置层。**

具体表现为五点。

### 6.1 题卡已经存在，但没有成为唯一控制入口

当前仓库已经有：

- 题型配置 `configs/types`
- 标准题卡 `card_specs/normalized/question_cards`
- 业务特征卡 `card_specs/business_feature_slots`

但在真实运行链路里，仍有一批关键判断没有完全从这些配置中取值，而是写在 service 中直接决定流程。

### 6.2 `passage_service` 已经按题卡组合，但 `prompt_skeleton_service` 仍有绕开题卡的本地判断

材料侧已经能做：

- question card 绑定
- business card 匹配
- slot projection
- prompt extras 回传

但题目侧仍有多处本地补判，例如：

- `main_idea` 检索时统一映射到 `title_selection`
- 参考母题触发难度上调
- 语序题 archetype 回推
- 参考题到 business card 的本地启发式分析

这说明“题卡驱动各服务组合”的方向已经存在，但还没有收口。

### 6.3 校验层和题卡契约尚未完全对齐

题卡中已经有 `validator_contract`，但 `question_validator.py` 当前仍主要靠代码内置规则执行题型校验。

结果是：

- 题卡中已有校验口径
- validator 中又有一套实际执行口径

这会带来长期维护上的双重来源问题。

### 6.4 前端工作台已经承载过多业务动作

当前 `/demo` 页面不是纯表单和结果展示页，而是同时承担：

- 参数填写
- 参考母题拆题
- 结果展示
- 控制调节
- 换材重做
- 手工改题
- 审核通过/作废
- 批次导出

这让前端已经开始承接部分业务组织责任，页面结构和服务结构的映射变弱。

### 6.5 补丁与临时口径已经进入正式流转

当前正式挂载页面依赖：

- `demo.py` 对静态资源版本号做替换
- `app_v2_zh_patch.js` 对 DOM 做持续补丁

说明当前系统已经有“主流转之外的补丁层”介入正式展示口径。这个补丁层不是独立维护文档的一部分，但已经影响真实运行结果。

## 7. 维护口径

团队后续维护本项目时，应以以下现实口径理解当前系统：

- 当前项目是“双服务 + 外置卡片/配置 + 单页工作台”
- `passage_service` 已经承担材料与题卡组合职责
- `prompt_skeleton_service` 已经承担出题、评审和交付职责
- 题卡和配置层已经存在，但尚未完全收回规则主权
- 当前最需要持续看护的不是“再加多少功能”，而是“让已有分层不继续漂移”

这份文档描述的是**当前仓库真实结构**，不是未来理想结构。后续如果服务边界、前端入口或配置主权发生变化，应直接更新本文件。
