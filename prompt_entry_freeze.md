# 1. 本轮二B允许触碰的 prompt 入口
- 名称：材料消费权威入口（主链）
- 文件位置：`prompt_skeleton_service/configs/question_generation_prompt_assets.yaml`；`prompt_skeleton_service/app/services/question_generation.py`（`_build_generation_prompt_sections`、`_build_material_context_sections`）
- 作用：定义并注入材料消费段（`selected_material`、`original_material_evidence`、`material_readability_contract`、`material_prompt_extras`、`material_answer_anchor`）。
- 为什么属于二B允许入口：主链真实消费，且直接对应“材料消费提示词”目标。

- 名称：生题权威入口（主模板）
- 文件位置：`prompt_skeleton_service/configs/prompt_templates.yaml`（仅 `action_type=generate`）；`prompt_skeleton_service/app/services/question_generation.py`（`_generate_question`）
- 作用：提供生成阶段系统主指令模板，并与 skeleton prompt 合并形成最终 `system_prompt`。
- 为什么属于二B允许入口：这是当前生题 LLM 调用前的第一层模板入口，直接决定题干/选项/答案/解析生成风格。

- 名称：解析权威入口（生成内解析约束）
- 文件位置：`prompt_skeleton_service/configs/question_generation_prompt_assets.yaml`（`answer_grounding/*`、`final_generation_instruction`）；`prompt_skeleton_service/app/services/question_generation.py`（`_build_answer_grounding_rules`）
- 作用：把解析与答案、材料证据链绑定，约束 analysis 的依据与排错路径。
- 为什么属于二B允许入口：解析并非独立服务生成，而是随生题主链同步生成并受该入口直接控制。

- 名称：本轮 family 级模板落点（先锁 center_understanding）
- 文件位置：`prompt_skeleton_service/configs/prompt_templates.yaml`（`main_idea_center_understanding_generate`；回退 `main_idea_generate_default`）
- 作用：给当前二B family 提供专用 generate 模板落点，避免跨 family 同改。
- 为什么属于二B允许入口：满足“先蒸一个 family”并且是当前最小可控落点。

# 2. 本轮二B禁止触碰的 prompt 入口
- 名称：评审与修订动作模板入口
- 文件位置：`prompt_skeleton_service/configs/prompt_templates.yaml`（`action_type=judge_review|minor_edit|question_modify|text_modify`）
- 当前作用：服务评审、修题、改文链路，不是首轮蒸馏落点。
- 为什么本轮不能碰：改动会联动 review/repair 行为，导致二B和旁支链路耦合。

- 名称：非当前 family 的 generate 模板入口
- 文件位置：`prompt_skeleton_service/configs/prompt_templates.yaml`（如 `sentence_fill/*`、`sentence_order/*`、`continuation/*`、`main_idea/title_selection`）
- 当前作用：其他题型/子型在运行时仍会消费。
- 为什么本轮不能碰：当前二B先锁一个 family，跨 family 改动会放大回归面。

- 名称：Skeleton 组装提示入口
- 文件位置：`prompt_skeleton_service/app/services/prompt_builder.py`；`prompt_skeleton_service/configs/types/*.yaml`
- 当前作用：把 type/pattern/control/generation 规则写入 `prompt_package.system_prompt/user_prompt`。
- 为什么本轮不能碰：该层是跨题型底盘，改动不是“二B蒸馏落点”，容易引发全局漂移。

- 名称：代码内硬编码修复提示入口
- 文件位置：`prompt_skeleton_service/app/services/question_generation.py`（`_build_material_refinement_prompts`、`_run_targeted_question_repair`、`apply_analysis_only_repair` 及同类 repair 分支）
- 当前作用：用于材料轻修与 targeted repair 的系统/用户提示。
- 为什么本轮不能碰：属于 repair/编辑链路，改动会混入二B范围外目标。

- 名称：评审器硬编码提示入口
- 文件位置：`prompt_skeleton_service/app/services/evaluation_service.py`
- 当前作用：LLM judge `system_prompt` 与模板回退分支。
- 为什么本轮不能碰：这是评测门控层，不是二B“材料消费/生题/解析”主生成落点。

- 名称：原题解析/分析提示入口
- 文件位置：`prompt_skeleton_service/app/services/source_question_parser.py`；`prompt_skeleton_service/app/services/source_question_analyzer.py`
- 当前作用：解析上传真题、抽取检索约束。
- 为什么本轮不能碰：属于 source-question 旁支，改动会影响检索与参考模板抽取，不是二B主目标。

- 名称：材料服务主卡判官模板入口
- 文件位置：`passage_service/app/config/llm.yaml`（`main_card_dual_judge`、`main_card_signal_resolver`、`main_card_family_landing`）
- 当前作用：材料侧主卡落位、信号重判与 family 落位判定。
- 为什么本轮不能碰：这是材料服务判官链路，二B此轮不做材料服务策略改写。

- 名称：材料服务 LLM prompt 文件入口
- 文件位置：`passage_service/app/prompts/*.md`（`candidate_planner_v2_prompt.md`、`logical_segment_refiner_prompt.md`、`material_integrity_gate_prompt.md`、`universal_tagger_prompt.md`、`*_family_prompt.md`）
- 当前作用：候选规划、分段修正、完整性闸门、family 打标。
- 为什么本轮不能碰：它们影响材料筛选与分类旁路，非二B提示词落点。

# 3. 疑似 legacy / 非主链路 prompt 入口
- 名称：`targeted_material_rewrite_prompt.md`
- 文件位置：`passage_service/app/prompts/targeted_material_rewrite_prompt.md`
- 当前状态判断：文件存在，但在当前审计到的主链代码中无调用引用。
- 是否建议后续清理：建议后续清点后处理（先确认是否有外部任务脚本调用）。

- 名称：`prompt_package.merged_prompt`（调试产物）
- 文件位置：`prompt_skeleton_service/app/services/prompt_builder.py`
- 当前状态判断：构建并返回，但主生成调用使用的是 `system_prompt` 与分段 `user_prompt`，不是 `merged_prompt`。
- 是否建议后续清理：建议后续统一为“仅调试字段”并降低误用风险。

- 名称：Judge 模板代码回退文案
- 文件位置：`prompt_skeleton_service/app/services/evaluation_service.py`（`_resolve_judge_template` 的 fallback 分支）
- 当前状态判断：仅在模板注册器不可用时触发，正常链路优先走 `prompt_templates.yaml`。
- 是否建议后续清理：建议后续明确“仅应急”并加可观测标记。

# 4. 二B使用提示
- 二B只改 A 类入口，禁止跨到 B/C。
- 当前 family 先锁 `main_idea/center_understanding`，不跨 family。
- 生题模板只动 `action_type=generate`。
- 解析约束优先落在 `question_generation_prompt_assets.yaml` 的 `answer_grounding/*`。
- 材料消费约束优先落在 `material_readability_contract` 与 `material_prompt_extras` 对应资产。
- prompt 只表达生成行为，不复写 validator 规则。
- `configs/types/*.yaml` 本轮视为底盘冻结层。
- repair/review/source-question 相关提示全部冻结。
- passage_service 下全部 prompt 入口本轮冻结。
- 发现“看起来可改但主链无调用”的入口，先归 C，不作为二B落点。
