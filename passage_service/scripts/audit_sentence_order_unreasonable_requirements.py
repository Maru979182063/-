import argparse
import json
import os
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

from sqlalchemy import select  # noqa: E402

from app.infra.db.orm.material_span import MaterialSpanORM  # noqa: E402
from app.infra.db.repositories.article_repo_sqlalchemy import SQLAlchemyArticleRepository  # noqa: E402
from app.infra.db.session import get_session  # noqa: E402
from app.services.material_pipeline_v2 import MaterialPipelineV2  # noqa: E402
from scripts.review_business_usable_rate import FAMILY_MAP, _review_one  # noqa: E402


def _safe_float(value: Any) -> float:
    try:
        if value is None:
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _formal_requirement_matrix() -> list[dict[str, Any]]:
    return [
        {
            "requirement": "formal_type_whitelist",
            "layer": "formal_unit_admission",
            "rule": "strict formal hit 仅认 ordered_unit_group",
            "status": "hard_gate",
            "business_need": "medium",
            "notes": "当前 sentence_block_group / sentence_group / paragraph_window 即使业务可用，也不会进入 strict formal。",
        },
        {
            "requirement": "group_size_exact_six",
            "layer": "ordered_unit_group_builder",
            "rule": "ordered_unit_group 最终必须归一到 6 句",
            "status": "hard_gate",
            "business_need": "medium",
            "notes": "builder 允许 6-8 句原料，但最终必须压到 6 句。",
        },
        {
            "requirement": "first_unit_stable",
            "layer": "ordered_unit_group_builder",
            "rule": "第 0 句必须进入 first_candidate_indices，阈值 >= 0.50",
            "status": "hard_gate",
            "business_need": "high",
            "notes": "首句资格是排序题刚需，但当前是一票否决。",
        },
        {
            "requirement": "last_unit_stable",
            "layer": "ordered_unit_group_builder",
            "rule": "末句必须进入 last_candidate_indices，阈值 >= 0.54",
            "status": "hard_gate",
            "business_need": "high",
            "notes": "尾句收束是排序题刚需，但当前阈值偏硬。",
        },
        {
            "requirement": "too_many_first_like_units",
            "layer": "ordered_unit_group_builder",
            "rule": "first_candidate_indices 不能 >= 4",
            "status": "hard_gate",
            "business_need": "medium",
            "notes": "控制首句歧义有业务价值，但不应过早一票否决。",
        },
        {
            "requirement": "too_many_last_like_units",
            "layer": "ordered_unit_group_builder",
            "rule": "last_candidate_indices 不能 >= 4",
            "status": "hard_gate",
            "business_need": "medium",
            "notes": "控制尾句歧义有业务价值，但当前对 summary-like 句组较敏感。",
        },
        {
            "requirement": "pairwise_constraints_exist",
            "layer": "ordered_unit_group_builder",
            "rule": "pairwise_constraints 至少 1 条",
            "status": "hard_gate",
            "business_need": "high",
            "notes": "没有任何顺序约束的句组不该进入排序 formal。",
        },
        {
            "requirement": "links_not_sparse",
            "layer": "ordered_unit_group_builder",
            "rule": "必须有 local_binding 或 precedence",
            "status": "hard_gate",
            "business_need": "high",
            "notes": "局部 binding/确定性顺序链是排序题刚需。",
        },
        {
            "requirement": "candidate_type_gate_for_order_cards",
            "layer": "material_card_gate",
            "rule": "order_material.* 当前只接受 sentence_block_group / ordered_unit_group",
            "status": "hard_gate",
            "business_need": "low",
            "notes": "这不是业务刚需，更像实现层便捷限制。",
        },
        {
            "requirement": "unit_count_exact_six_for_cards",
            "layer": "material_card_gate",
            "rule": "order_material.* 要求 unit_count == 6",
            "status": "hard_gate",
            "business_need": "medium",
            "notes": "业务常见 6 句，但不应把 5-7 句局部可用组直接拦掉。",
        },
        {
            "requirement": "structure_meaning_thresholds",
            "layer": "material_card_gate",
            "rule": "structure_score >= 0.60 且 meaning_score >= 0.60",
            "status": "hard_gate",
            "business_need": "medium",
            "notes": "本来像评分项，但当前实际充当准入门。",
        },
        {
            "requirement": "unique_opener_score_threshold",
            "layer": "material_card_gate",
            "rule": "unique_opener_score >= 0.58",
            "status": "score_but_hard_effect",
            "business_need": "medium",
            "notes": "强调唯一首句，但对现实材料偏理想化。",
        },
        {
            "requirement": "binding_pair_count_threshold",
            "layer": "material_card_gate",
            "rule": "binding_pair_count >= 2",
            "status": "score_but_hard_effect",
            "business_need": "high",
            "notes": "需要一定约束链，但不应要求过整。",
        },
        {
            "requirement": "risk_caps",
            "layer": "material_card_gate",
            "rule": "exchange_risk <= 0.38 且 multi_path_risk <= 0.40 且 function_overlap_score <= 0.46",
            "status": "score_but_hard_effect",
            "business_need": "medium",
            "notes": "风险控制有必要，但当前阈值对真实材料偏紧。",
        },
        {
            "requirement": "progression_closure_thresholds",
            "layer": "material_card_gate",
            "rule": "discourse_progression_strength >= 0.54 且 context_closure_score >= 0.56",
            "status": "score_but_hard_effect",
            "business_need": "medium",
            "notes": "提升完整 formal 感，但不一定应在准入层一票否决。",
        },
    ]


def _requirement_tiers() -> dict[str, list[dict[str, str]]]:
    return {
        "A_business_hard_needs": [
            {
                "requirement": "存在至少一条稳定顺序链",
                "why": "完全无 pairwise/local binding 的句组，不足以支撑排序题。",
            },
            {
                "requirement": "有可辨识的首尾方向",
                "why": "首尾不必完美唯一，但不能完全无起始/收束感。",
            },
            {
                "requirement": "句组规模足以形成排序任务",
                "why": "至少应是可排序的连续句组，而不是零散片段。",
            },
        ],
        "B_important_but_not_veto": [
            {
                "requirement": "first/last 资格强度",
                "why": "重要，但不应要求一上来就达到完美 anchor。",
            },
            {
                "requirement": "binding_pair_count 与 local_binding 强度",
                "why": "业务希望有约束链，但中等强度句组仍可能可出题。",
            },
            {
                "requirement": "progression / closure 完整度",
                "why": "影响题感，但不该先于可排序性本身成为拒绝门。",
            },
            {
                "requirement": "6 句常规规模",
                "why": "业务常见，但 5-7 句局部组也可能有用。",
            },
        ],
        "C_system_overstrict": [
            {
                "requirement": "strict formal 只认 ordered_unit_group",
                "why": "把已成形的 6 句 sentence_block_group / sentence_group 直接挡在 formal path 外。",
            },
            {
                "requirement": "unique_opener_score >= 0.58",
                "why": "更像系统追求唯一首句洁癖，而非业务刚需。",
            },
            {
                "requirement": "first/last 作为 builder 一票否决",
                "why": "应允许进入弱 formal / 过渡 formal，而不是直接消失。",
            },
            {
                "requirement": "risk / progression / closure 多重阈值同时做准入",
                "why": "本来像评分维度，却叠加成了形式门槛。",
            },
            {
                "requirement": "candidate_type 只放 sentence_block_group / ordered_unit_group",
                "why": "paragraph_window / sentence_group 中的业务可用句组因此失去机会。",
            },
        ],
    }


def run() -> dict[str, Any]:
    session = get_session()
    article_repo = SQLAlchemyArticleRepository(session)
    pipeline = MaterialPipelineV2()
    article_cache: dict[str, Any] = {}

    try:
        stmt = select(MaterialSpanORM).where(
            MaterialSpanORM.is_primary.is_(True),
            MaterialSpanORM.status == "promoted",
            MaterialSpanORM.release_channel == "stable",
            MaterialSpanORM.v2_index_version.is_not(None),
        )
        materials = list(session.scalars(stmt))
        sentence_order_materials: list[MaterialSpanORM] = []
        for material in materials:
            fams = set(material.v2_business_family_ids or [])
            payload = material.v2_index_payload or {}
            if "sentence_order" in fams and isinstance(payload.get("sentence_order"), dict):
                sentence_order_materials.append(material)

        records: list[dict[str, Any]] = []
        c_records: list[dict[str, Any]] = []
        source_counter = Counter()
        issue_counter = Counter()
        builder_reason_counter = Counter()
        builder_source_counter: dict[str, Counter] = defaultdict(Counter)
        blocker_counter = Counter()
        opening_rule_counter = Counter()
        closing_rule_counter = Counter()
        binding_rule_counter = Counter()
        logic_mode_counter = Counter()
        source_unit_count_counter = Counter()
        gate_examples: dict[str, list[dict[str, Any]]] = defaultdict(list)

        for material in sorted(sentence_order_materials, key=lambda item: str(item.id)):
            article_id = str(material.article_id)
            if article_id not in article_cache:
                article_cache[article_id] = article_repo.get(article_id)
            article = article_cache.get(article_id)
            if article is None:
                continue

            review = _review_one(
                review_family="sentence_order",
                business_family=FAMILY_MAP["sentence_order"],
                material=material,
                article=article,
                pipeline=pipeline,
            )
            runtime_item = pipeline.build_cached_item_from_material(
                material=material,
                article=article,
                business_family_id="sentence_order",
                enable_fill_formalization_bridge=False,
            )
            signal_profile = dict((runtime_item or {}).get("neutral_signal_profile") or {})
            order_profile = dict(((runtime_item or {}).get("business_feature_profile") or {}).get("sentence_order_profile") or {})
            runtime_type = str((runtime_item or {}).get("candidate_type") or "")
            source_type = str(material.span_type or "")
            text = str((runtime_item or {}).get("text") or material.text or "")
            units = pipeline._sentence_order_units(str(material.text or ""), source_type)
            source_unit_count = len(units)
            source_unit_count_counter[source_unit_count] += 1 if review.a_b_c_d == "C" else 0

            row = {
                "material_id": str(material.id),
                "article_id": article_id,
                "source_candidate_type": source_type,
                "runtime_candidate_type": runtime_type,
                "a_b_c_d": review.a_b_c_d,
                "business_level": review.business_level,
                "formal_unit_hit": review.formal_unit_hit,
                "issues": list(review.issues),
                "final_score": review.final_score,
                "readiness_score": review.readiness_score,
                "text_clip": review.text_clip,
                "source_unit_count": source_unit_count,
                "order_profile": order_profile,
            }
            records.append(row)

            if review.a_b_c_d != "C":
                continue

            source_counter[source_type] += 1
            for issue in review.issues:
                issue_counter[issue] += 1

            opening_rule_counter[str(order_profile.get("opening_rule") or "none")] += 1
            closing_rule_counter[str(order_profile.get("closing_rule") or "none")] += 1
            for rule in order_profile.get("binding_rules") or []:
                binding_rule_counter[str(rule)] += 1
            for mode in order_profile.get("logic_modes") or []:
                logic_mode_counter[str(mode)] += 1

            normalized = None
            if source_unit_count in (6, 7, 8):
                normalized = pipeline._normalize_ordered_units_to_six(units)
            if normalized is None:
                builder_reason = "unit_count_out_of_builder_range" if source_unit_count not in (6, 7, 8) else "normalize_failed"
                normalized_units = []
                pairwise_constraints: list[dict[str, Any]] = []
                first_indices: list[int] = []
                last_indices: list[int] = []
            else:
                normalized_units, unit_forms, local_bindings, normalization_reason = normalized
                worthwhile, builder_reason, pairwise_constraints, first_indices, last_indices = pipeline._ordered_unit_group_worthwhile(normalized_units)
                if worthwhile:
                    builder_reason = "would_pass_builder"
            builder_reason_counter[builder_reason] += 1
            builder_source_counter[source_type][builder_reason] += 1

            if int(order_profile.get("unit_count") or 0) != 6:
                blocker_counter["unit_count_not_six"] += 1
            if _safe_float(order_profile.get("unique_opener_score")) < 0.58:
                blocker_counter["unique_opener_below_card_gate"] += 1
            if _safe_float(order_profile.get("binding_pair_count")) < 2:
                blocker_counter["binding_pair_count_below_card_gate"] += 1
            if _safe_float(order_profile.get("exchange_risk")) > 0.38:
                blocker_counter["exchange_risk_above_card_gate"] += 1
            if _safe_float(order_profile.get("multi_path_risk")) > 0.40:
                blocker_counter["multi_path_risk_above_card_gate"] += 1
            if _safe_float(order_profile.get("function_overlap_score")) > 0.46:
                blocker_counter["function_overlap_above_card_gate"] += 1
            if _safe_float(order_profile.get("discourse_progression_strength")) < 0.54:
                blocker_counter["progression_below_card_gate"] += 1
            if _safe_float(order_profile.get("context_closure_score")) < 0.56:
                blocker_counter["closure_below_card_gate"] += 1
            if _safe_float(order_profile.get("local_binding_strength")) < 0.48:
                blocker_counter["local_binding_below_business_strong"] += 1
            if _safe_float(order_profile.get("sequence_integrity")) < 0.52:
                blocker_counter["sequence_integrity_below_business_strong"] += 1

            current_gate_examples = gate_examples[builder_reason]
            if len(current_gate_examples) < 4:
                current_gate_examples.append(
                    {
                        "material_id": str(material.id),
                        "source_candidate_type": source_type,
                        "source_unit_count": source_unit_count,
                        "builder_reason": builder_reason,
                        "first_candidate_indices": first_indices,
                        "last_candidate_indices": last_indices,
                        "pairwise_constraint_count": len(pairwise_constraints),
                        "opening_rule": order_profile.get("opening_rule"),
                        "closing_rule": order_profile.get("closing_rule"),
                        "binding_rules": order_profile.get("binding_rules") or [],
                        "logic_modes": order_profile.get("logic_modes") or [],
                        "text_clip": review.text_clip,
                    }
                )

            c_records.append(row)

        summary = {
            "total": len(records),
            "c_count": len(c_records),
            "a_count": sum(1 for row in records if row["a_b_c_d"] == "A"),
            "b_count": sum(1 for row in records if row["a_b_c_d"] == "B"),
            "d_count": sum(1 for row in records if row["a_b_c_d"] == "D"),
        }
        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "summary": summary,
            "formal_requirements": _formal_requirement_matrix(),
            "c_audit": {
                "source_type_distribution": dict(source_counter),
                "issue_distribution": dict(issue_counter),
                "builder_replay_reason_distribution": dict(builder_reason_counter),
                "builder_replay_reason_by_source_type": {key: dict(value) for key, value in builder_source_counter.items()},
                "card_gate_blocker_distribution": dict(blocker_counter),
                "unit_count_distribution": dict(source_unit_count_counter),
                "opening_rule_distribution": dict(opening_rule_counter),
                "closing_rule_distribution": dict(closing_rule_counter),
                "binding_rule_distribution": dict(binding_rule_counter),
                "logic_mode_distribution": dict(logic_mode_counter),
                "examples_by_builder_reason": gate_examples,
            },
            "requirement_tiers": _requirement_tiers(),
            "design_advice": {
                "first_requirement_to_relax": {
                    "requirement": "last_unit_stable 从 builder 硬门降为弱 formal 条件",
                    "why": "20 条 C 样本里，builder replay 最大拦截因子是 last_unit_unstable(11)。这说明现实业务可用句组常常收束感足够出题，但达不到 current formal 的末句硬锁。",
                },
                "need_transition_formal_layer": True,
                "transition_formal_direction": "允许 sentence_block_group / sentence_group / paragraph_window 中已经具备 6 句规模、局部约束链和中等首尾资格的句组，先进入 weak_formal_order_group，而不是要求一步到位 ordered_unit_group。",
                "next_cut_priority": "先调 formal admission 与 weak formal 层，再考虑 candidate generator。",
                "why_not_complex_bridge_first": "当前 20 条 C 样本里，18 条本身就是 6 句，5 条重放 builder 后可直接通过，说明主问题不是缺复杂 bridge，而是 formal 入口过窄、builder/card gate 过严。",
            },
        }
    finally:
        session.close()


def _to_markdown(report: dict[str, Any]) -> str:
    lines: list[str] = []
    summary = report.get("summary") or {}
    c_audit = report.get("c_audit") or {}
    advice = report.get("design_advice") or {}

    lines.append("# Sentence Order Unreasonable Requirement Audit")
    lines.append("")
    lines.append("## Summary")
    lines.append(f"- total: {summary.get('total', 0)}")
    lines.append(f"- C count: {summary.get('c_count', 0)}")
    lines.append(f"- A/B/D: {summary.get('a_count', 0)}/{summary.get('b_count', 0)}/{summary.get('d_count', 0)}")
    lines.append("")
    lines.append("## Current Formal Requirements")
    for item in report.get("formal_requirements") or []:
        lines.append(
            f"- {item.get('requirement')}: layer={item.get('layer')}; status={item.get('status')}; business_need={item.get('business_need')}; rule={item.get('rule')}"
        )
    lines.append("")
    lines.append("## C Sample Attribution")
    lines.append(f"- source_type_distribution: {c_audit.get('source_type_distribution', {})}")
    lines.append(f"- issue_distribution: {c_audit.get('issue_distribution', {})}")
    lines.append(f"- builder_replay_reason_distribution: {c_audit.get('builder_replay_reason_distribution', {})}")
    lines.append(f"- card_gate_blocker_distribution: {c_audit.get('card_gate_blocker_distribution', {})}")
    lines.append(f"- unit_count_distribution: {c_audit.get('unit_count_distribution', {})}")
    lines.append(f"- opening_rule_distribution: {c_audit.get('opening_rule_distribution', {})}")
    lines.append(f"- closing_rule_distribution: {c_audit.get('closing_rule_distribution', {})}")
    lines.append("")
    lines.append("## Requirement Tiers")
    tiers = report.get("requirement_tiers") or {}
    for bucket in ("A_business_hard_needs", "B_important_but_not_veto", "C_system_overstrict"):
        lines.append(f"### {bucket}")
        for item in tiers.get(bucket) or []:
            lines.append(f"- {item.get('requirement')}: {item.get('why')}")
    lines.append("")
    lines.append("## Design Advice")
    lines.append(f"- first_requirement_to_relax: {(advice.get('first_requirement_to_relax') or {}).get('requirement')}")
    lines.append(f"- why: {(advice.get('first_requirement_to_relax') or {}).get('why')}")
    lines.append(f"- need_transition_formal_layer: {advice.get('need_transition_formal_layer')}")
    lines.append(f"- transition_formal_direction: {advice.get('transition_formal_direction')}")
    lines.append(f"- next_cut_priority: {advice.get('next_cut_priority')}")
    lines.append(f"- why_not_complex_bridge_first: {advice.get('why_not_complex_bridge_first')}")
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit unreasonable formal requirements for sentence_order.")
    parser.add_argument("--output-json", type=Path, required=True)
    parser.add_argument("--output-md", type=Path, required=True)
    args = parser.parse_args()

    report = run()
    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    args.output_json.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    args.output_md.write_text(_to_markdown(report), encoding="utf-8")
    print(f"Wrote {args.output_json}")
    print(f"Wrote {args.output_md}")


if __name__ == "__main__":
    main()
