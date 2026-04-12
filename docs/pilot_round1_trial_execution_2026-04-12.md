# 小规模题包试点首轮启动包（2026-04-12）

## 冻结样本清单
- 文件：C:\Users\Maru\Documents\agent\reports\pilot_round1_sample_manifest_2026-04-12.csv
- source_batch：desktop_docx_pilot_seed_2026-04-12_v1
- center_understanding：50
- sentence_fill：50
- sentence_order：50
- 合计：150

## 最小样本表字段
- sample_id
- usiness_family_id
- usiness_subtype_id
- question_card_id
- source_name
- source_batch
- source_qid
- source_exam
- is_candidate_for_pilot
- gate_status
- locked_reason
- is_canonical_clean
- lank_position
- unction_type
- logic_relation
- main_axis_source
- rgument_structure
- candidate_type
- opening_anchor_type
- closing_anchor_type
- ormal_export_eligible
- 
eview_status
- 
otes

## 第一轮执行顺序
1. 先用冻结清单逐条导入试点样本表，只回填基础元信息，不提前补协议字段。
2. 再按三大母族分别跑 canonical gate / strict projection，回填 gate_status、locked_reason、is_canonical_clean 以及各族最小字段。
3. 这一步直接产出 blocked 样本池：gate_status != pass 的样本另存为 
eports/pilot_round1_blocked_pool_2026-04-12.csv。
4. 然后从 gate_status = pass 的样本中筛正式标注候选池，另存为 
eports/pilot_round1_annotation_candidates_2026-04-12.csv。
5. 对正式标注候选池跑第一轮 review / replay 检查，核对标准展示、delivery、export 是否仍保持 canonical。
6. 最后产出回放检查报表：
eports/pilot_round1_replay_check_2026-04-12.md 和同名 json。

## 当前说明
- 这轮先直接使用你给的三份 docx 作为首轮题源，不再扩量。
- 当前所有样本默认处于 pending_gate，表示已冻结入池、尚未做准入校验，不表示已通过准入。
- sentence_order 只有在 strict projection 通过后，才允许把 ormal_export_eligible 回填为 	rue。
