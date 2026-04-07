# 前端结构说明

## 1. 文档目的

本文档用于说明当前仓库里的真实前端结构，帮助团队在不改动现有架构边界的前提下，恢复对页面职责、页面流转和数据来源的掌控。

本文只描述当前已经接入运行的页面结构，不讨论理想形态，也不把未接入文件当成正式页面。

## 2. 当前前端入口

当前前端的实际入口是：

- 路由：`prompt_skeleton_service/app/routers/demo.py`
- 页面壳：`prompt_skeleton_service/app/demo_static/index.html`
- 主脚本：`prompt_skeleton_service/app/demo_static/app_v2.js`
- 页面补丁：`prompt_skeleton_service/app/demo_static/app_v2_zh_patch.js`

当前实际挂载的是一个 `/demo` 单页工作台，不是多路由站点。

`index.html` 中实际存在 3 个 screen：

- `builderScreen`
- `loadingScreen`
- `resultScreen`

虽然目录下还有 `app.js`、`app_v3.js`，但当前 `/demo` 实际加载的是 `app_v2.js`，并由 `demo.py` 额外注入 `app_v2_zh_patch.js`。因此，当前正式页面结构应以这 3 个 screen 为准。

## 3. 当前有哪些页面

### 3.1 Builder 页面

页面节点：

- `builderScreen`

页面职责：

- 选择题目母族和二级类型
- 选择难度、文本方向、材料结构
- 设置生成数量
- 录入参考母题
- 发起“自动拆题并回填”
- 发起正式生成

当前调用接口：

- `POST /api/v1/questions/source-question/parse`
- `POST /api/v1/questions/generate`

这个页面本质上是“出题编排入口页”。

### 3.2 Loading 页面

页面节点：

- `loadingScreen`

页面职责：

- 展示当前流程状态
- 展示节点说明
- 允许用户返回上一页

当前接口关系：

- 不直接发起独立业务接口
- 主要依附于生成流程状态切换

这个页面本质上是“状态展示页”。

### 3.3 Result 页面

页面节点：

- `resultScreen`

页面职责：

- 展示本次生成结果
- 展示每道题的状态、内容、答案、解析、材料信息
- 加载和展示控制面板
- 加载备选材料
- 执行换材重做
- 执行自贴材料重做
- 执行控制项调节
- 执行手工编辑保存
- 执行通过/作废
- 批次导出

当前调用接口：

- `GET /api/v1/questions/{item_id}/controls`
- `GET /api/v1/questions/{item_id}/replacement-materials`
- `POST /api/v1/questions/{item_id}/review-actions`
- `POST /api/v1/questions/{item_id}/confirm`
- `GET /api/v1/review/batches/{batch_id}/delivery/export?format=markdown`

这个页面当前已经不是纯结果展示页，而是“展示 + 审核 + 调参 + 重做 + 导出”的综合工作页。

## 4. 页面之间怎么流转

当前页面流转是单页内 screen 切换，不是浏览器路由跳转。

主流转如下：

1. 用户进入 `/demo`，默认落在 `builderScreen`。
2. 用户在 Builder 页填写参数或参考题后，提交生成。
3. 页面切换到 `loadingScreen`，展示当前流程状态。
4. 生成完成后，页面切换到 `resultScreen`。
5. Result 页会继续按题目逐项加载 controls。
6. 用户可在 Result 页执行重做、调参、人工编辑、通过、作废、导出。
7. 用户点击返回首页后，再切回 `builderScreen`。

当前没有单独的“详情页”“审核页”“导出页”“设置页”。

## 5. 哪些页面只负责展示

从当前职责看，真正接近“只负责展示”的只有：

- `loadingScreen`

它的作用是：

- 展示流程状态
- 展示节点文案
- 提供返回按钮

它不承载题型判断、不承载参数拼装、不承载 review 动作，不应继续增加业务决策逻辑。

## 6. 哪些页面不应该承载业务逻辑

### 6.1 Builder 页面不应承载的逻辑

`builderScreen` 不应长期承载下列逻辑：

- 由前端自行推断题型归属
- 由前端自行推断材料结构
- 由前端自行推断文本方向
- 由前端自行决定某些 business feature 映射
- 由前端拼接题型协议字段

Builder 页应做的是“采集输入并提交后端”，而不是替代协议层做业务判断。

### 6.2 Result 页面不应承载的逻辑

`resultScreen` 不应长期承载下列逻辑：

- 由页面自行决定某控制项是否存在
- 由页面拼装题型专属业务规则
- 由页面根据局部字段猜测 review action 的业务含义
- 由页面在本地重建答案语义或题目协议
- 由页面决定哪些字段是原始真值、哪些字段是派生展示

Result 页可以承载“交互动作”，但不应承载“业务判定”。

### 6.3 Loading 页面不应承载的逻辑

`loadingScreen` 不应承载任何题型业务逻辑或状态修补逻辑，只应展示后端给出的当前阶段信息。

## 7. 哪些数据应来自接口透传，而不是页面内部拼装

### 7.1 题型与二级类型相关数据

下列数据应由接口或配置透传，而不是由页面内部推断或重命名：

- `question_focus`
- `special_question_type`
- 可选题型列表
- 可选二级类型列表
- 题型展示名称
- 二级类型展示名称

页面可以渲染这些值，但不应成为它们的事实来源。

### 7.2 控制面板相关数据

下列数据应来自 `GET /api/v1/questions/{item_id}/controls` 的透传结果：

- 控制项列表
- 控制项 key
- 控制项类型
- 控制项当前值
- 控制项候选值
- 控制项展示名称
- 控制项是否可编辑

页面不应通过本地规则推断“某题型应该出现哪些控制项”。

### 7.3 结果卡片相关数据

下列数据应直接来自生成结果或 review 返回结果：

- 题面
- 选项
- 正确答案
- 解析
- 材料文本
- 审核状态
- 当前版本信息
- review action 执行结果

页面不应再次推导答案，也不应本地生成题面解释。

### 7.4 备选材料相关数据

下列数据应由 `replacement-materials` 接口透传：

- 候选材料列表
- 材料标识
- 材料摘要
- 候选理由
- 命中标签
- 可替换性说明

页面只负责展示和选择，不应本地做候选排序逻辑补算。

### 7.5 导出相关数据

导出按钮只应触发导出接口，不应由页面自行拼装 markdown 正文、人工筛选通过题目或重算导出内容。

## 8. 当前已经出现的页面内拼装现象

当前前端中，已经能看到一些页面侧业务拼装或补丁式处理：

- Builder 页存在对题型的本地推断逻辑
- Result 页会根据本地条件重组 controls 的展示和交互
- `app_v2_zh_patch.js` 会重写部分选项值和展示文案
- `app_v2_zh_patch.js` 会移除 `.blocked-banner`
- `app_v2_zh_patch.js` 用 `MutationObserver` 持续修补 DOM

这些都说明当前前端除了“展示接口结果”之外，已经承担了一部分本不应长期停留在页面层的修补职责。

## 9. 当前前端层的主结论

当前仓库前端不是多页面系统，而是一个 `/demo` 单页工作台，内部包含：

- 录入页
- 加载页
- 结果工作页

其中：

- `loadingScreen` 最接近纯展示页
- `builderScreen` 应以输入采集和提交为主，不应承担协议判断
- `resultScreen` 当前承担的职责最多，也是最容易混入业务逻辑的页面

后续维护前端时，应优先坚持一个原则：

**页面负责展示和交互，接口负责返回结构化事实，页面不应成为业务规则的事实来源。**
