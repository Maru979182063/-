# V2 材料池接入定义

## 1. 结论

当前生成链已切换为 `passage_service` 的 **V2 材料池链路**。

运行侧口径：
- 生成入口仍使用 `MaterialBridgeService`
- 但该入口已内部代理到 `MaterialBridgeV2Service`
- 题目生成、改题换材料、替换材料列表都以 `/materials/v2/search` 为主

关键文件：
- `prompt_skeleton_service/app/services/material_bridge.py`
- `prompt_skeleton_service/app/services/material_bridge_v2.py`
- `prompt_skeleton_service/app/services/question_generation.py`
- `prompt_skeleton_service/configs/question_runtime.yaml`

## 2. 材料池分层

V2 材料池不是单表模型，而是三层：

1. `articles`
   文章主表，保存原文和来源。
2. `candidate_spans`
   候选切片表，保存段落/句子窗口。
3. `material_spans`
   正式材料池表，保存 V1 正式入池材料。

同时，V2 运行时还会基于文章实时派生候选，并返回一套 **生成索引字段**，这套字段是“生题时真正用来选材”的主索引。

## 3. 数据库存储定义

### 3.1 `articles`

用途：文章原始存储层。

核心字段：
- `id`: 文章主键
- `source`: 来源名
- `source_url`: 原文链接
- `title`: 标题
- `raw_text`: 原始正文
- `clean_text`: 清洗后正文
- `language`: 语言
- `domain`: 业务域
- `status`: 文章状态
- `hash`: 去重哈希
- `created_at`
- `updated_at`

### 3.2 `candidate_spans`

用途：文章切片层，供候选窗口、段落定位、动态成段使用。

核心字段：
- `id`: 候选片段主键
- `article_id`: 关联文章
- `start_paragraph`
- `end_paragraph`
- `start_sentence`
- `end_sentence`
- `span_type`: 切片类型
- `text`: 切片文本
- `generated_by`: 生成方式
- `status`
- `segmentation_version`
- `created_at`
- `updated_at`

### 3.3 `material_spans`

用途：正式材料池层，适合做审核、治理、统计、回收、导出。

核心主键与关联字段：
- `id`
- `article_id`
- `candidate_span_id`
- `normalized_text_hash`
- `material_family_id`
- `is_primary`

文本与长度字段：
- `text`
- `span_type`
- `length_bucket`
- `paragraph_count`
- `sentence_count`

发布治理字段：
- `status`
- `release_channel`
- `gray_ratio`
- `gray_reason`
- `reject_reason`

版本字段：
- `segmentation_version`
- `tag_version`
- `fit_version`
- `prompt_version`

路由与标签字段：
- `primary_family`
- `primary_subtype`
- `secondary_subtypes`
- `primary_label`
- `candidate_labels`
- `knowledge_tags`

画像与索引字段：
- `universal_profile`
- `family_scores`
- `family_profiles`
- `subtype_candidates`
- `secondary_candidates`
- `primary_route`
- `feature_profile`
- `fit_scores`
- `capability_scores`
- `parallel_families`
- `structure_features`
- `integrity`
- `decision_trace`

来源与质量字段：
- `source`
- `source_tail`
- `quality_flags`
- `quality_score`

使用反馈字段：
- `usage_count`
- `accept_count`
- `reject_count`
- `last_used_at`
- `created_at`
- `updated_at`

## 4. V2 生题索引字段

对接同事如果是“为了生题时调用材料池”，优先不要直接依赖 `material_spans` 的旧路由字段，而要优先使用 V2 搜索返回里的这些字段。

V2 搜索入口：
- `POST /materials/v2/search`

请求字段：
- `business_family_id`
- `question_card_id`
- `article_ids`
- `business_card_ids`
- `article_limit`
- `candidate_limit`
- `min_card_score`
- `min_business_card_score`

### 4.1 顶层候选标识

每条 V2 候选的核心标识字段：
- `candidate_id`
- `article_id`
- `article_title`
- `candidate_type`
- `material_card_id`
- `selected_business_card`
- `quality_score`

建议外部系统把 `candidate_id` 视为 **V2 材料主键**。

### 4.2 文本字段

- `text`: 原候选文本
- `consumable_text`: 供生题直接消费的文本

建议生题默认用：
- `consumable_text`

### 4.3 来源字段

`source` 对象建议保留：
- `source_name`
- `source_url`
- `domain`

### 4.4 文章级索引字段

`article_profile` 中建议对外暴露：
- `document_genre`
- `article_purpose_frame`
- `discourse_shape`
- `core_object`
- `global_main_claim`
- `closure_score`
- `context_dependency`
- `paragraph_count`
- `sentence_count`

这些字段适合做：
- 文体过滤
- 文章级粗召回
- 题型/母族预匹配

### 4.5 中性信号字段

`neutral_signal_profile` 中建议重点使用：
- `standalone_readability`
- `semantic_completeness_score`
- `topic_consistency_strength`
- `turning_focus_strength`
- `cause_effect_strength`
- `necessary_condition_strength`
- `countermeasure_signal_strength`
- `parallel_enumeration_strength`
- `summary_strength`
- `context_dependency`
- `document_genre`
- `material_structure_label`

这些字段适合做：
- 候选粗排
- 生题前基础可用性判断

### 4.6 业务卡字段

`business_feature_profile` 是 V2 现阶段最推荐的业务侧索引层。

建议重点使用：
- `feature_type`
- `logic_relations`
- `theme_words`
- `topic_consistency_strength`
- `semantic_completeness_score`
- `readability`
- `material_structure_label`
- `conclusion_focus`
- `conclusion_position`
- `key_sentence_position`
- `explicit_marker_group`
- `explicit_marker_hits`
- `marker_hit_ratio`
- `require_explicit_marker_ready`
- `require_complete_unit_ready`
- `non_key_detail_density`

这层字段适合做：
- 业务卡匹配
- 特征段过滤
- 错项风险识别
- 题目风格对齐

### 4.7 材料卡与业务卡命中字段

材料卡相关：
- `eligible_material_cards`
- `material_card_recommendations`

业务卡相关：
- `eligible_business_cards`
- `business_card_recommendations`

建议保留的最小字段：
- `card_id` / `business_card_id`
- `score`
- `reason`

### 4.8 生题上下文字段

`question_ready_context` 是最接近“可直接出题”的字段层。

建议对外暴露：
- `question_card_id`
- `runtime_binding`
- `selected_material_card`
- `selected_business_card`
- `generation_archetype`
- `resolved_slots`
- `prompt_extras`
- `validator_contract`

如果外部系统只想“拿材料直接出题”，这层已经足够做首版接入。

## 5. 推荐外部接入主键

建议外部系统以这组主键组织数据：

- `article_id`: 文章主键
- `candidate_id`: V2 候选主键
- `selected_material_card`: 材料结构卡主键
- `selected_business_card`: 业务卡主键

推荐唯一键：

`candidate_id + selected_material_card + selected_business_card`

## 6. 推荐最小接入视图

如果同事后期不想直接吃全量 V2 返回，可以先约定一个最小视图：

```json
{
  "candidate_id": "article_xxx:whole_passage:1",
  "article_id": "article_xxx",
  "article_title": "示例标题",
  "source": {
    "source_name": "人民网",
    "source_url": "https://...",
    "domain": "时评政论"
  },
  "text": "原候选文本",
  "consumable_text": "生题直接用的文本",
  "document_genre": "评论议论",
  "candidate_type": "whole_passage",
  "quality_score": 0.91,
  "feature_type": "因果关系",
  "selected_material_card": "title_material.problem_essence_judgement",
  "selected_business_card": "cause_effect__conclusion_focus__main_idea",
  "generation_archetype": "turning_focus",
  "resolved_slots": {
    "structure_type": "progressive",
    "main_point_source": "conclusion_sentence"
  }
}
```

## 7. 对接建议

如果是内部调用，建议优先按下面顺序接：

1. 先接 `/materials/v2/search`
2. 以 `candidate_id` 作为 V2 材料标识
3. 以 `selected_material_card + selected_business_card` 作为生成侧索引
4. 若需长期沉淀与审计，再补接 `articles / candidate_spans / material_spans`

## 8. 当前口径

当前项目口径已经调整为：

- V1 旧材料桥接入口名保留
- 但运行链已由 V2 材料池驱动
- 后续新功能应优先补在 V2 搜索与 V2 业务卡字段上

