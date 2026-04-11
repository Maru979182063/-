# 真题数据整理与标注方案

## 1. 目标

这份文档只解决一个问题：  
真题和线上样本到底要存什么，才能支撑后续蒸馏、校准和回放。

原则是：

- 先存全
- 再洗干净
- 标签允许分层
- 原始数据绝不丢

## 2. 数据分层

建议把数据拆成 5 张逻辑表。

## 2.1 `truth_question_raw`

存原始真题，不做主观改写。

建议字段：

- `truth_id`
- `source_name`
- `source_year`
- `source_region`
- `source_exam`
- `question_type_raw`
- `material_text_raw`
- `stem_raw`
- `options_raw`
- `answer_raw`
- `analysis_raw`
- `ocr_noise_flag`
- `copyright_note`
- `import_batch_id`

## 2.2 `truth_question_normalized`

存清洗后的结构化字段。

建议字段：

- `truth_id`
- `question_type_normalized`
- `business_family_id`
- `question_card_id`
- `business_card_id`
- `material_structure_label`
- `text_direction`
- `document_genre`
- `difficulty_label_gold`
- `material_text_clean`
- `stem_clean`
- `options_clean`
- `answer_clean`
- `analysis_clean`
- `has_reference_question`
- `is_strong_source_required`

## 2.3 `truth_material_labels`

存材料和结构相关标签。

建议字段：

- `truth_id`
- `material_subject`
- `material_theme`
- `main_axis_source`
- `argument_structure`
- `slot_position`
- `slot_function_type`
- `semantic_scope`
- `bidirectional_dependency`
- `structure_notes`

用途：

- 训练材料卡分类器
- 校准 sentence_fill / sentence_order 映射
- 校准 main_idea 结构标签

## 2.4 `truth_option_labels`

存选项层标签，尤其是错误项错法。

建议字段：

- `truth_id`
- `option_id`
- `is_correct`
- `abstraction_level`
- `coverage_scope`
- `distractor_type`
- `distractor_intensity`
- `error_mode_primary`
- `error_mode_secondary`
- `option_rationale`

建议先统一用一组小词表：

- `detail_as_main`
- `scope_too_wide`
- `scope_too_narrow`
- `subject_shift`
- `focus_shift`
- `concept_swap`
- `stronger_conclusion`
- `causal_reversal`
- `example_as_conclusion`
- `countermeasure_overreach`

## 2.5 `truth_review_gold`

存审核视角标签。

建议字段：

- `truth_id`
- `overall_score_gold`
- `material_alignment_gold`
- `difficulty_fit_gold`
- `distractor_quality_gold`
- `analysis_quality_gold`
- `recommended_state_gold`
- `repair_needed_gold`
- `review_notes_gold`

用途：

- 校准你现在的自动评分
- 校准是否进入“继续复核”

## 3. 用户行为样本表

真题之外，再建 3 张“线上行为表”。

## 3.1 `question_download_feedback`

- `item_id`
- `downloaded`
- `download_version_no`
- `download_after_manual_edit`
- `download_after_question_modify`
- `download_after_material_replace`
- `download_timestamp`

## 3.2 `question_edit_diff`

- `item_id`
- `field_changed`
- `before_value`
- `after_value`
- `change_type`
- `editor_role`

用途：

- 看用户到底最常改哪类错误项
- 看哪些题干/解析最常被重写

## 3.3 `question_failure_case`

- `item_id`
- `failure_stage`
- `error_type`
- `question_type`
- `question_card_id`
- `business_card_id`
- `material_id`
- `judge_score`
- `validation_errors`
- `failure_snapshot`

## 4. 标注层级建议

不要要求一开始每题都标到最细。建议三层：

### 层 1：必标

- 题型
- 正确答案
- 题位
- 是否强原文
- 难度
- question card

### 层 2：推荐标

- business card
- material structure label
- distractor type
- main axis source
- abstraction level

### 层 3：后补

- distractor intensity
- risk tag
- review gold score
- analysis quality

## 5. 第一轮样本规模建议

不要一开始全量洗。建议先做试点集：

- `main_idea`：200 题
- `sentence_fill`：200 题
- `sentence_order`：100 题

其中每类再覆盖：

- 简单 / 中等 / 困难
- 强原文 / 非强原文
- 成功样本 / 失败样本 / 用户修改样本

## 6. 数据清洗优先级

先处理最影响蒸馏质量的脏问题：

1. 小标题、编号、括号说明混入正文
2. 选项编号不统一
3. 正确答案字母与选项顺序不一致
4. 解析混入题干
5. OCR 断行和乱码

## 7. 标注组织方式

推荐一个非常务实的分工方式：

1. 机器先预标
2. 人工只改机器高不确定样本
3. 每轮先收 50-100 题做口径统一
4. 口径统一后再批量扩样

## 8. 数据使用方式

这些数据后面会分别进入：

- 材料处理回放
- 映射分类回放
- 候选材料排序回放
- 题目生成 few-shot 检索
- 评分校准
- 审核页修复建议生成

## 9. 第一轮最值钱的数据

如果你时间有限，先抓这几类：

1. 通过业务测试的真题
2. 结构清晰、答案争议小的真题
3. 你系统当前最容易失败的题卡对应真题
4. 用户修改幅度大的线上样本
5. 被下载走的高质量成品题

这五类数据最能迅速拉动系统质量。
