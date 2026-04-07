# 材料层服务审计

## 1. 材料层服务定位

当前仓库中的材料层服务就是 `passage_service`。它不是一个纯抓取脚本集合，也不是一个单纯的材料检索接口，而是一条从“来源进入”到“材料入池、治理、导出、反馈回流”的完整材料生产链。

### 1.1 它当前负责什么

基于当前目录和代码，`passage_service` 真实负责的内容包括：

- 文章来源定义与定时抓取
  - 配置位于 `app/config/sources.yaml`
  - 调度入口位于 `app/jobs/scheduler.py`
  - 抓取执行位于 `app/domain/services/ingest_service.py`
- 手工 ingest 与文章入库
  - API 位于 `app/api/routes/articles.py`
  - 清洗、去重、入库位于 `IngestService`
- 段落/句子切分与候选 span 生成
  - 入口位于 `SegmentService`
  - 具体 split/generate 位于 `app/infra/segment/*`
- 材料标签、母族路由、子类打分、治理、入池
  - 入口位于 `TagService`
  - 规则、LLM、治理、merge、pool 写入共同完成
- 材料池查询、promote、reprocess、feedback
  - API 位于 `app/api/routes/materials.py`、`feedback.py`
- V2 材料检索与预计算
  - API 位于 `app/api/routes/materials_v2.py`
  - 主执行位于 `app/domain/services/material_pipeline_v2_service.py`
  - 核心算法位于 `app/services/material_pipeline_v2.py`
- review export 与 Dify pack 导出
  - `ReviewExportService`
  - `DifyExportService`
- 材料回流与同步
  - feedback 聚合位于 `FeedbackService`
  - 外部知识库同步位于 `SyncService`

### 1.2 它当前不负责什么

当前 `passage_service` 不负责：

- 题目生成
- 题面组装
- 选项、答案、解析生成
- 前端页面逻辑
- 题目 review action 的业务编排

它会为上层提供“可被出题消费的材料”与“带上下文的材料检索结果”，但不直接生成题目。

### 1.3 它与题卡层、题目生成层、前端层的关系

- 与题卡层的关系
  - 当前已经存在一条明确的题卡接入链：`CardRegistryV2` 会读取 `card_specs/normalized/question_cards`、`material_cards`、`signal_layers` 以及 `card_specs/business_feature_slots`
  - `MaterialPipelineV2` 会在检索时消费 `question_card`、`signal_layer`、`material_cards`、`business_cards`
  - 这说明材料层已经开始接受题卡和 business feature card 的驱动
- 与题目生成层的关系
  - 当前主要通过 `/materials/v2/search` 向上游输出带 `question_ready_context` 的候选材料
  - 它是上游题目生成前的材料准备器，不是题目生成器
- 与前端层的关系
  - 当前对前端暴露的是文章处理、材料池、V2 检索、反馈、导出、抓取任务等接口
  - 前端并不直接控制材料服务内部治理规则

当前更准确的定位是：

**`passage_service` 是材料生产与材料治理服务，不是题目服务；但它已经不只是原始材料库，而是一个带有“材料适配判断”的预出题层。**

## 2. 当前材料层主流程

下面按实际代码链路梳理当前主流程。

### 2.1 来源进入

来源定义主要来自：

- `app/config/sources.yaml`
- `app/config/source_scope_catalog.yaml`

其中：

- `sources.yaml` 已直接接入抓取与调度链路
- `source_scope_catalog.yaml` 当前只被加载进 `ConfigBundle`，但未看到进入主执行链路

这意味着当前真正生效的来源入口是 `sources.yaml`，而不是整个 source scope catalog。

### 2.2 抓取 / ingest

主入口有两类：

- 手工 ingest
  - `POST /articles/ingest`
  - 调用 `IngestService.ingest`
- 爬取入口
  - `POST /crawl/run`
  - `POST /crawl/source/{source_id}/run`
  - 调用 `CrawlService` 和 `_SourceCrawler`

关键对象与 service：

- `HttpCrawlerFetcher`
- `ReadabilityLikeExtractor`
- `BasicCleaner`
- `build_content_hash`
- `ArticleORM`

执行方式：

- 抓取列表页和文章页是规则/抓取器驱动
- 文本清洗和去重是规则驱动
- 是否自动进入后续处理由 `sources.yaml` 的 `auto_process_after_ingest` 与 `process_mode` 决定
- 定时触发由 `APScheduler` 和 `scheduler.py` 驱动

实际流程是：

1. 从 `sources.yaml` 读取已启用来源。
2. 取 `entry_urls` 或 `base_url` 做 URL 发现。
3. 用 `allowed_domains`、`article_url_patterns`、`exclude_url_patterns` 做筛选。
4. 抓文章正文，若正文长度低于 `min_body_length` 则跳过。
5. 进入 `IngestService.ingest`。
6. `BasicCleaner` 清洗文本，`content_hash` 去重。
7. 若 URL 已存在则更新文章；若 hash 已存在则直接返回已有文章；否则创建新文章。

### 2.3 清洗 / segment

入口：

- `POST /articles/{article_id}/segment`
- `ProcessService.process_article(..., mode="full")` 内部也会调用

关键对象与 service：

- `SegmentService`
- `DefaultParagraphSplitter`
- `DefaultSentenceSplitter`
- `ParagraphWindowGenerator`
- `SentenceWindowGenerator`
- `StoryFragmentGenerator`
- `LogicalSegmentRefiner`
- `CandidateSpanORM`

执行方式：

- 基础切分和窗口生成是规则驱动
- `LogicalSegmentRefiner` 是“规则优先 + 可选 LLM 复核”

实际流程是：

1. 对 `article.clean_text` 做段落切分。
2. 对段落内容继续做句子切分。
3. 生成 paragraph/sentence 记录。
4. 用多种 generator 生成候选 span。
5. 对短文章做额外 throttle，限制过细切片数量。
6. 调用 `LogicalSegmentRefiner` 对 fragment 进行 keep / merge / drop 决策。
7. 覆盖式写入 `candidate_spans`。

### 2.4 标签 / 路由

入口：

- `POST /articles/{article_id}/tag`
- `ProcessService.process_article(..., mode="full")`

关键对象与 service：

- `TagService`
- `UniversalTagger`
- `DocumentGenreClassifier`
- `FamilyRouter`
- `SummarizationFamilyTagger`
- `TitleFamilyTagger`
- `FillFamilyTagger`
- `OrderingFamilyTagger`
- `ContinuationFamilyTagger`
- `MaterialIntegrityGate`
- `MaterialGovernanceService`
- `MaterialMergeService`
- `PoolWriter`
- `FitMapper`

执行方式：

- `UniversalTagger`：LLM 优先，失败时回退 heuristic
- `DocumentGenreClassifier`：规则驱动
- `FamilyRouter`：规则驱动
- family taggers：LLM 优先，失败时回退 heuristic
- `MaterialIntegrityGate`：规则优先，必要时走 LLM 审核
- `MaterialGovernanceService`：规则驱动
- `MaterialMergeService`：规则驱动

实际流程是：

1. 读取文章对应的 `candidate_spans`。
2. 对每个 span 先做完整性 gate。
3. 对通过 gate 的 span 做 universal profile。
4. 用 genre classifier 判断文种。
5. 用 family router 计算 family_scores、parallel_families、primary_route。
6. 用 family taggers 计算 subtype candidates。
7. 用 governance 选择 `candidate_labels`、`primary_label`、`secondary_candidates`。
8. 计算 fit scores、quality flags、release channel。
9. merge 相似材料，形成 primary + variants。
10. 写入 `material_spans`。
11. 初始化 tagging review。
12. 执行 sync。
13. 更新 candidate span 状态。

### 2.5 材料池入库

主落点：

- `MaterialSpanORM`
- `PoolService.create_material`

材料入池时已经带有大量衍生信息：

- `universal_profile`
- `family_scores`
- `parallel_families`
- `structure_features`
- `family_profiles`
- `subtype_candidates`
- `candidate_labels`
- `primary_label`
- `primary_route`
- `integrity`
- `quality_flags`
- `fit_scores`
- `feature_profile`
- `v2_index_payload`

这说明材料池已经不是“原文仓库”，而是“带治理标签和出题前判断结果的材料仓库”。

### 2.6 检索 / promote / reprocess

V1 材料池接口：

- `POST /materials/search`
- `GET /materials/stats`
- `POST /materials/promote`
- `POST /materials/reprocess`

关键 service：

- `PoolService`
- `ReprocessService`

V2 检索接口：

- `POST /materials/v2/search`
- `POST /materials/v2/precompute`

关键 service：

- `MaterialPipelineV2Service`
- `MaterialV2IndexService`
- `MaterialPipelineV2`
- `CardRegistryV2`

执行方式：

- `promote` 是规则/API 驱动
- `reprocess` 是任务触发重跑
- V2 search 是“卡 + 规则 + 可选 LLM planner”组合执行

实际流程：

1. V1 search 主要按 status、release_channel、family、genre、structure、fit_score 等表面字段过滤。
2. V2 precompute 为 material 生成按 business family 切分的 `v2_index_payload`。
3. V2 search 优先命中缓存；缓存未命中时回到 article 级搜索与在线构建。
4. V2 search 过程中会结合：
   - question card
   - signal layer
   - material cards
   - business cards
   - structure constraints
   - query terms

### 2.7 导出 / review-export / dify-pack

review export：

- 接口：`POST /articles/{article_id}/review-export`
- service：`ReviewExportService`
- 输出目录：`review_samples/processed/<article_id>/`

作用：

- 导出 article + candidate + material 的人工审阅快照

Dify pack：

- 接口：`POST /materials/export/dify-pack`
- service：`DifyExportService`
- 输出目录：`exports/dify_pack_<timestamp>/`

作用：

- 导出 `manifest.jsonl`
- 导出 `materials.csv`
- 导出 `articles.csv`
- 导出文章清洗版与材料 markdown 文档

### 2.8 feedback / jobs / scheduler

feedback：

- 接口：`POST /materials/feedback`
- service：`FeedbackService`

作用：

- 写 feedback record
- 更新 feedback aggregate
- 更新 usage_count / accept_count / reject_count / quality_score
- 在简单阈值下把 gray 提升为 stable，或把 bad case 多的材料打成 deprecated

jobs / scheduler：

- 调度器位于 `app/jobs/scheduler.py`
- 当前实际接上的只有 crawl 定时任务
- `daily_ingest_job.py`、`daily_segment_and_tag_job.py`、`nightly_fit_rescore_job.py`、`weekly_deprecation_job.py` 当前都是 stubbed

结论是：

**材料层的“定时抓取”已接上；“夜间重评分、周度下线、日常分段打标”等治理型作业名义上存在，但当前并未形成真实自动化闭环。**

## 3. 当前材料层的分层结构

### 3.1 API 层

目录：

- `app/api/router.py`
- `app/api/routes/*.py`

负责：

- 暴露文章处理、材料池、V2 检索、反馈、抓取任务接口
- 做 request schema 与 service 调用的最外层编排

当前状态：

- API 层整体比较薄
- 主要问题不是 API 本身，而是其下层 service 已经承载了大量业务分支

### 3.2 Schemas 层

目录：

- `app/schemas/*.py`

负责：

- 定义 `MaterialV2SearchRequest`
- 定义 `SpanRecord`、`UniversalProfile`、`FamilyScores`、`SubtypeRoute`

当前状态：

- schema 层比较清楚
- 但 schema 只承载结果结构，很多业务约束并没有被 schema 化，而是仍留在 service 判断里

### 3.3 Services 层

目录分成两块：

- `app/domain/services/*.py`
- `app/services/*.py`

其中：

- `domain/services` 更像流程编排层
- `app/services` 更像算法与治理层

当前职责分布：

- `domain/services`
  - ingest、segment、tag、process、pool、reprocess、review export、dify export、sync、feedback、v2 service
- `app/services`
  - family router、universal tagger、genre classifier、integrity gate、governance、merge、V2 pipeline、card registry

当前混杂点很明显：

- `TagService` 不只是编排，还承担了整条 tagging/governance 主链的收口
- `MaterialPipelineV2` 体量过大，混合了 candidate 规划、family 特化、card hit、business card hit、presentation、缓存刷新、质量打分
- `MaterialGovernanceService` 不只是治理阈值，还写入了大量 family-specific coverage 规则

### 3.4 Rules 层

目录：

- `app/rules/family_config.py`

负责：

- 从 `family_routing.yaml` 读取 family names 和阈值

当前状态：

- rules 层实际非常薄
- 许多真正的路由、coverage、结构判断规则并不在 `rules/`，而在 `service` 中

### 3.5 Jobs 层

目录：

- `app/jobs/*.py`

负责：

- scheduler
- 预留的日常 ingest / segment / rescore / deprecation 作业

当前状态：

- `scheduler.py` 真正在工作
- 其他 job 文件是占位 stub
- 这意味着材料层存在“作业层目录完整，但治理作业未成型”的现状

### 3.6 Infra / Domain / Core 层

`core`：

- 配置、枚举、日志、异常、时钟

`infra`：

- crawl fetcher/extractor
- ingest cleaner/dedupe
- segment splitters/generators
- db session/orm/repositories
- llm provider
- plugins
- kb adapters

`domain`：

- ORM 对应的 domain model
- repository interface 的落地使用
- 业务流程 service

当前状态：

- `core` 和 `infra` 边界相对清楚
- `domain` 与 `app/services` 的边界不完全清楚
- 特别是 “编排在 domain/services、业务算法在 app/services” 这个分法成立，但 `app/services` 中已经沉积了很多本应继续上升到配置/题卡层的规则

### 3.7 是否存在职责混杂

存在，主要体现在三类：

- family-specific 业务逻辑大量沉入 `app/services`
- 题卡/材料卡之外的材料消费约束，被 `MaterialPipelineV2` 自己做了很多判断
- 治理配置和治理代码并存，部分阈值来自 yaml，部分阈值直接写死在 service

## 4. 当前材料层与题卡驱动设计的契合度

### 4.1 已经契合的部分

当前材料层已经出现了比较明确的题卡驱动能力，主要集中在 V2：

- `CardRegistryV2` 已从 `card_specs` 加载：
  - `question_cards`
  - `material_cards`
  - `signal_layers`
  - `business_feature_slots`
- `MaterialPipelineV2.search` 已明确消费：
  - `question_card_id`
  - `business_card_ids`
  - `business_family_id`
  - `structure_constraints`
- `question_card` 已能向下提供：
  - `runtime_binding`
  - `upstream_contract`
  - `validator_contract`
- `business card` 已能向下提供：
  - `slot_projection.type_slots`
  - `prompt_extras`

这说明材料层并不是完全“自己决定自己”，它已经有一条真实存在的 card-driven V2 链路。

### 4.2 还主要由材料服务自己决定的部分

当前仍然主要由材料服务自己决定的内容包括：

- 候选 span 如何生成与保留
- 哪些 family 更适合某段材料
- family score 的计算公式
- subtype coverage 是否成立
- sentence_order / sentence_fill 的材料结构是否达标
- V2 候选如何按质量、结构、query terms 重新排序
- 什么时候 gray，什么时候 stable

这些决定多数发生在：

- `FamilyRouter`
- `MaterialGovernanceService`
- `MaterialIntegrityGate`
- `MaterialPipelineV2`
- `TagService`

### 4.3 本应由题卡/题型协议提供约束、但当前被写死在材料服务中的内容

当前最明显的下沉内容有：

- `sentence_order` 的固定 6 单元假设
  - `MaterialPipelineV2.SENTENCE_ORDER_FIXED_UNIT_COUNT = 6`
- sentence_order business card 的命中规则
  - 直接按具体 `business_card_id` 分支打分
- sentence_fill business card 的命中规则
  - 直接按具体 `business_card_id` 和 blank/function mapping 分支打分
- 不同 family 的 presentation 生成方式
  - `sentence_order`、`sentence_fill`、`continuation` 都在 V2 pipeline 里专门分支
- family alias 关系
  - `title_selection` 与 `main_idea` 的别名关系在 `CardRegistryV2` 代码里处理
- family routing 的核心评分公式
  - 虽然阈值来自 yaml，但“如何打分”仍是代码硬编码
- subtype coverage 的判定标准
  - 仍在 `MaterialGovernanceService._validate_subtype_coverage` 中手写

### 4.4 已偏离“服务做执行器、题卡做控制器”的地方

最明显的偏离点有两个：

1. `MaterialPipelineV2` 已经兼具“执行器 + 控制器”双重角色  
它不只是执行卡片，而是在很多地方自己决定：
- 哪类候选可用
- 哪类结构更好
- 哪张 business card 更匹配
- 哪个 family 的 presentation 该怎样生成

2. `TagService + FamilyRouter + MaterialGovernanceService` 形成了一套独立的材料业务控制中心  
这套中心当然能工作，但它控制的很多业务语义，目前并没有完全上升到题卡/协议层。

因此，当前材料层的契合度可以概括为：

**V2 检索入口已经明显开始题卡化，但材料治理主链仍然主要是 service 自己在做业务控制。**

## 5. 当前材料层已知问题审计

### 5.1 来源与抓取问题

- 来源接入主入口实际上是 `sources.yaml`，`source_scope_catalog.yaml` 虽然存在，但未看到进入主执行链路。
  - 这意味着“来源治理目录”和“真实抓取配置”是分离的。
- 当前抓取策略明显偏固定来源、静态页面、规则发现。
  - `discover_article_urls`、URL pattern、selector 配置是主干。
  - 对更复杂站点、需要登录站点、教材类手工来源，目前没有进入统一执行链。
- 抓取器主要是 `HttpCrawlerFetcher + ReadabilityLikeExtractor`。
  - 对复杂页面、异形版式、动态站点的鲁棒性有限。
- 文章的真实发布时间没有稳定落库。
  - crawler 抓取时会得到 `published_at` 并记入 audit；
  - 但 `ArticleORM` 没有 `published_at` 字段，`IngestService` 也未保存该字段；
  - 结果是下游 `build_source_info` 只能退回 `created_at`。
- 去重主要依赖 `content_hash` 和 `source_url`。
  - 对跨站转载、轻度改写、同文异 URL 的治理能力有限。

### 5.2 清洗与切分问题

- 文章切分主链仍是机械切分优先。
  - 先 paragraph/sentence split，再 window generate，再 logical refine。
  - 逻辑结构是在切完之后再修，而不是从题型消费约束反推切片。
- `LogicalSegmentRefiner` 虽有 LLM 复核，但使用条件和规则仍较粗。
  - 例如 short/bridge/no_terminal 等启发式，主要是通用 fragment 修补。
- `segmentation.yaml` 提供的参数较少。
  - 很多切片保留/丢弃逻辑仍在代码里，例如 `_throttle_short_article_spans`。
- 短文 throttle 是代码硬编码，不是题卡约束。
  - 这会影响后续哪些材料能进入不同题型消费链。
- 候选 span 的写入是覆盖式 replace。
  - 每次重跑都会删除并重建 candidate spans，版本保留较弱。

### 5.3 标签与路由问题

- family routing、universal tagging、family subtype tagging、governance label 选择是多套系统叠加。
  - 能工作，但边界不够清。
- `knowledge_tree.yaml`、plugin registry、plugin contracts 已存在，但在主 tagging 决策中参与度很低。
  - 当前真正起核心作用的是 `UniversalTagger + FamilyRouter + family_taggers + Governance`。
- `knowledge_tree` 当前更多像版本与插件名配置，而不是主业务控制骨架。
- family 名称体系和 V2 business family 体系并不是一套。
  - 一个来源于 `family_routing.yaml` 的 family names；
  - 一个来源于 V2 question card/business family ids，如 `title_selection`、`sentence_fill`、`sentence_order`、`continuation`。
  - 中间靠 service 代码做映射。
- 文种、结构、family、subtype、business card 命中关系没有统一的单一真值层。
  - 不同服务各自保留一部分判断。

### 5.4 材料池治理问题

- `demote_existing_for_article` 会在重新打标前把该文章现有 primary material 全部降为 `deprecated`。
  - 这是一种强覆盖策略，风险较高。
  - 一次重跑质量不佳时，可能直接把原有稳定材料整体打下去。
- tagging review 只做了 `init_review`，没有看到成型的 review 决策闭环。
  - review 表存在；
  - review export 存在；
  - 但 review 并没有形成真正的材料治理主控面。
- feedback 回路是存在的，但很简化。
  - promotion / deprecation 逻辑只看 usage、accept_rate、bad_case_count；
  - `FeedbackUpdater` 仍是 noop 占位。
- 配置里的 release 阈值与代码里的 feedback 阈值已经出现分离。
  - `release.yaml` 定义了 promotion / deprecation 条件；
  - `FeedbackService` 里仍直接写死 `accept_rate >= 0.8 && usage_count >= 3`、`bad_case_count >= 3`。
- sync 接口存在，但默认是 `noop`，Dify adapter 也是 stubbed。
  - 说明“入池后同步到外部知识库”的链路还未真正闭环。
- reprocess 能重跑，但 payload 里的 `material_ids`、`segmentation_version`、`tag_version`、`fit_version` 基本没有被实质消费。
  - 当前更像 article 级重新 segment + tag。

### 5.5 配置驱动不足问题

- `family_routing.yaml` 只提供 family names 和阈值。
  - 真正的 score 公式仍在 `FamilyRouter` 代码中。
- `material_governance.yaml` 只提供 minimums / merge / wide_labels。
  - 真正的 subtype coverage、bridge 判定、half-turn 判定、isolated example 判定仍在 `MaterialGovernanceService`。
- `segmentation.yaml` 只提供少量参数。
  - 文章短文 throttle、若干 merge/keep 逻辑仍写在 `SegmentService` 和 `LogicalSegmentRefiner` 中。
- V2 已能加载 card，但 business card 匹配逻辑并未完全交给 card。
  - `MaterialPipelineV2` 仍直接按具体 `business_card_id` 写 sentence_order/sentence_fill 的打分分支。
- `sentence_order` 的 6 单元约束是代码常量，不是配置或题卡契约。
- `release.yaml` 已有 promotion/deprecation 条件，但 `FeedbackService` 未消费该配置。
- `source_scope_catalog.yaml` 存在但未接入主链，属于“有配置但不驱动系统”。

## 6. 当前最危险的结构性风险

- 双套控制体系并存：V1 family routing/governance 与 V2 question-card/business-card 并行存在。
  - 为什么危险：同一材料会被两套规则体系解释，长期会产生口径漂移。
  - 影响层：标签层、检索层、治理层。
  - 类型：源头问题。

- `MaterialPipelineV2` 过于集中。
  - 为什么危险：它同时承载 candidate 规划、family 特化、business card 命中、presentation、缓存刷新、质量打分，任何局部改动都容易影响整链。
  - 影响层：V2 检索层、题卡接入层、材料输出层。
  - 类型：源头问题。

- 题卡已接入，但 card 之外仍有大量 family-specific 硬编码。
  - 为什么危险：服务不再只是执行器，而是在继续定义业务意图。
  - 影响层：题卡驱动层、材料检索层。
  - 类型：源头问题。

- 发布时间未稳定落库。
  - 为什么危险：来源可信度、材料时效性、source_tail、导出元数据都会失真。
  - 影响层：来源层、导出层、治理层。
  - 类型：源头问题。

- reprocess 采用“先整体降级再重建”的强覆盖方式。
  - 为什么危险：重跑失败会伤到已有稳定材料，治理结果不具备温和回滚特征。
  - 影响层：材料池治理层。
  - 类型：源头问题。

- 治理闭环名义完整，实际不完整。
  - 为什么危险：review 表、feedback 表、job 文件都在，但关键动作很多仍是初始化、stub、noop，容易形成“看似可控、实际不可控”的错觉。
  - 影响层：review、feedback、jobs、scheduler。
  - 类型：源头问题。

- 配置与代码阈值已经出现漂移。
  - 为什么危险：团队会误以为改 yaml 就能改行为，但真实执行仍可能遵循代码常量。
  - 影响层：release、feedback、治理层。
  - 类型：源头问题。

- `source_scope_catalog.yaml` 与实际抓取链断开。
  - 为什么危险：来源治理和来源执行不是同一套真值源，后续扩源时会反复失控。
  - 影响层：来源层、抓取层。
  - 类型：源头问题。

- plugin / knowledge_tree 骨架与真实 tagging 主链脱节。
  - 为什么危险：形式上像插件化、树化设计，实际上主链靠专门 service 写死；后续维护容易高估扩展性。
  - 影响层：规则层、插件层、治理层。
  - 类型：源头问题。

## 7. 材料层“归位建议”

这一节只做“归位判断”，不展开方案。

### 7.1 应留在材料服务系统层的

- 抓取器、提取器、清洗器、去重器
- paragraph/sentence splitter 与 window generator
- DB ORM、repository、session、audit、job 持久化
- LLM provider 与 prompt 文件读取
- review export、Dify pack 导出、文件写入
- scheduler 与 crawl job 注册
- Sync adapter 边界
- Card registry 的加载与缓存机制

这些属于材料服务的执行基础设施，应继续留在系统层。

### 7.2 应上升到题型协议 / 题卡约束层的

- 各 business family 对候选材料的结构要求
- 各 business card 的命中条件与优先级
- `sentence_order` 的 unit count 要求
- `sentence_fill` 的 blank/function 结构要求
- family alias 与 runtime family 的正式映射关系
- 候选材料类型要求
- 题卡对应的 `prompt_extras`、`slot_projection`
- V2 检索时的结构约束语义
- 哪类材料适合哪张 question card 的上游契约

这些已经不只是“怎么执行”，而是在定义“什么材料才算适配某题卡/某母族”。

### 7.3 应删除或回收的临时补丁式逻辑

- `daily_ingest_job.py`、`daily_segment_and_tag_job.py`、`nightly_fit_rescore_job.py`、`weekly_deprecation_job.py` 这类 stub 作业
- `FeedbackUpdater` 这类 noop 占位
- `DifyKnowledgeBaseAdapter` 当前 stub 返回
- 未接主链的 `source_scope_catalog.yaml` 运行期幻象
- plugin / builder / controller 中仅作占位的 noop 实现，如果它们继续不参与主链
- `MaterialPipelineV2` 里以具体 `business_card_id` 为中心的临时打分分支
- `sentence_order` 固定 6 单元这类直接写死的 family 特化常量

这些逻辑的共同问题不是“写得不好”，而是它们在当前系统里制造了“看起来已经成型、实际上仍是桥接或占位”的错觉。

## 8. 一个简明结论

当前材料层最像一条“已经能跑通、而且能力不少的材料生产线”，不是简单材料库；它已经包含抓取、切分、标签、治理、入池、V2 检索、导出、反馈回流等完整链路。最不符合设计哲学的地方，不是某个接口缺失，而是材料服务内部已经积累了太多本应由题卡/协议层控制的业务意图，尤其集中在 family 路由、subtype coverage 和 V2 business card 命中这几块。后续如果要先修一类问题，最应该先修的不是单个 bug，而是“配置/题卡真值”与“service 内部硬编码业务规则”之间的错位；因为这才是材料层失控感的源头。 
