from __future__ import annotations

import csv
import os
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
PASSAGE_SERVICE_ROOT = ROOT / "passage_service"
REPORTS_DIR = ROOT / "reports"
MANIFEST_PATH = REPORTS_DIR / "round1_material_scoring_replay_manifest_2026-04-12.csv"
RESULTS_PATH = REPORTS_DIR / "round1_material_scoring_replay_results_2026-04-12.csv"
REPORT_PATH = REPORTS_DIR / "round1_material_scoring_replay_report_2026-04-12.md"

os.chdir(PASSAGE_SERVICE_ROOT)
if str(PASSAGE_SERVICE_ROOT) not in sys.path:
    sys.path.insert(0, str(PASSAGE_SERVICE_ROOT))

from app.core.enums import MaterialStatus, ReleaseChannel  # noqa: E402
from app.domain.services.material_pipeline_v2_service import MaterialPipelineV2Service  # noqa: E402
from app.infra.db.repositories.material_span_repo_sqlalchemy import SQLAlchemyMaterialSpanRepository  # noqa: E402
from app.infra.db.session import get_session  # noqa: E402


FAMILIES = ("sentence_fill", "center_understanding", "sentence_order")
SOURCE_FAMILY_FOR_REPLAY = {
    "sentence_fill": "sentence_fill",
    "center_understanding": "title_selection",
    "sentence_order": "sentence_order",
}
CACHED_LIMITS = {
    "sentence_fill": 220,
    "center_understanding": 180,
    "sentence_order": 220,
}
GROUPS_PER_FAMILY = 8
GROUP_SIZE = 3
FINAL_CANDIDATES_PER_FAMILY = GROUPS_PER_FAMILY * GROUP_SIZE


def _top_score(items: list[dict[str, Any]], key: str) -> float:
    if items and isinstance(items[0], dict):
        return round(float(items[0].get(key) or 0.0), 4)
    return 0.0


def _paragraph_span(item: dict[str, Any]) -> str:
    span = list((item.get("meta") or {}).get("paragraph_range") or [])
    if len(span) == 2:
        return f"{span[0]}-{span[1]}"
    return ""


def _sentence_span(item: dict[str, Any]) -> str:
    span = list((item.get("meta") or {}).get("sentence_range") or [])
    if len(span) == 2:
        return f"{span[0]}-{span[1]}"
    return ""


def _material_structure_label(item: dict[str, Any]) -> str:
    business = item.get("business_feature_profile") or {}
    neutral = item.get("neutral_signal_profile") or {}
    return str(
        business.get("material_structure_label")
        or neutral.get("material_structure_label")
        or ""
    )


def _task_final_score(item: dict[str, Any]) -> float:
    scoring = item.get("selected_task_scoring") or {}
    return round(float(scoring.get("final_candidate_score") or 0.0), 4)


def _family_match_score(item: dict[str, Any]) -> float:
    return round(float((item.get("llm_family_match_hint") or {}).get("score") or 0.0), 4)


def _generation_readiness_score(item: dict[str, Any]) -> float:
    return round(float((item.get("llm_generation_readiness") or {}).get("score") or 0.0), 4)


def _structure_score(item: dict[str, Any]) -> float:
    return round(float((item.get("llm_structure_integrity_judgment") or {}).get("score") or 0.0), 4)


def _asset_anchor(item: dict[str, Any]) -> dict[str, Any]:
    return dict(((item.get("llm_family_match_hint") or {}).get("asset_anchor")) or {})


def _asset_anchor_role(item: dict[str, Any]) -> str:
    return str(_asset_anchor(item).get("anchor_role") or "")


def _compare_bucket(index_in_group: int) -> str:
    if index_in_group == 0:
        return "top"
    if index_in_group == 1:
        return "control_near"
    return "control_far"


def _fill_local_cohesion(item: dict[str, Any]) -> float:
    profile = ((item.get("business_feature_profile") or {}).get("sentence_fill_profile") or {})
    parts = [
        float(profile.get("bidirectional_validation") or 0.0),
        float(profile.get("backward_link_strength") or 0.0),
        float(profile.get("forward_link_strength") or 0.0),
    ]
    return round(sum(parts) / max(1, len(parts)), 4)


def _candidate_type(item: dict[str, Any]) -> str:
    return str(item.get("candidate_type") or "")


def _fill_paragraph_width(item: dict[str, Any]) -> int:
    span = list((item.get("meta") or {}).get("paragraph_range") or [])
    if len(span) != 2:
        return 0
    try:
        return max(1, int(span[1]) - int(span[0]) + 1)
    except (TypeError, ValueError):
        return 0


def _fill_slot_function_clarity(item: dict[str, Any]) -> float:
    profile = ((item.get("business_feature_profile") or {}).get("sentence_fill_profile") or {})
    neutral = item.get("neutral_signal_profile") or {}
    blank_position = str(profile.get("blank_position") or "")
    function_type = str(profile.get("function_type") or "")
    structure_label = str(
        profile.get("material_structure_label")
        or ((item.get("business_feature_profile") or {}).get("material_structure_label"))
        or (neutral.get("material_structure_label"))
        or ""
    )
    closure_score = float(neutral.get("closure_score") or 0.0)
    titleability = float(neutral.get("titleability") or 0.0)
    bonus = 0.0
    if blank_position == "opening" and function_type in {"topic_introduction", "topic_intro", "summary"}:
        if structure_label in {"总分", "背景-核心结论", "观点-论证"}:
            bonus += 0.03
        elif structure_label in {"时间演进", "并列展开"}:
            bonus -= 0.01
    if blank_position == "inserted" and function_type == "reference_summary":
        bonus += 0.02
    bonus += 0.03 * max(0.0, titleability - 0.62)
    bonus += 0.02 * max(0.0, closure_score - 0.60)
    return round(bonus, 4)


def _fill_edge_position_exposure(item: dict[str, Any]) -> float:
    profile = ((item.get("business_feature_profile") or {}).get("sentence_fill_profile") or {})
    blank_position = str(profile.get("blank_position") or "")
    anchor_ids = list((_asset_anchor(item).get("anchor_sample_ids") or []))
    if blank_position not in {"opening", "inserted"} or anchor_ids:
        return 0.0
    width_penalty = min(0.09, max(0, _fill_paragraph_width(item) - 2) * 0.03)
    dependency_penalty = min(0.03, float(profile.get("reference_dependency") or 0.0) * 0.03)
    return round(0.13 + width_penalty + dependency_penalty, 4)


def _fill_rank_fit(item: dict[str, Any]) -> float:
    profile = ((item.get("business_feature_profile") or {}).get("sentence_fill_profile") or {})
    blank_position = str(profile.get("blank_position") or "")
    readiness = _generation_readiness_score(item)
    local_cohesion = _fill_local_cohesion(item)
    closure_score = float((item.get("neutral_signal_profile") or {}).get("closure_score") or 0.0)
    middle_bonus = 0.08 + max(0.0, local_cohesion - 0.70) * 0.10 if blank_position == "middle" else 0.0
    local_closure_bonus = 0.0
    if blank_position == "middle":
        local_closure_bonus = 0.02 + 0.04 * max(0.0, closure_score - 0.55) + 0.03 * max(0.0, local_cohesion - 0.68)
    elif blank_position in {"opening", "inserted"}:
        local_closure_bonus = 0.02 * max(0.0, closure_score - 0.60)
    slot_clarity = _fill_slot_function_clarity(item)
    edge_exposure = _fill_edge_position_exposure(item)
    return round(
        readiness + 0.22 * local_cohesion + middle_bonus + local_closure_bonus + slot_clarity - edge_exposure,
        4,
    )


def _fill_blank_position(item: dict[str, Any]) -> str:
    return str((((item.get("business_feature_profile") or {}).get("sentence_fill_profile") or {}).get("blank_position")) or "")


def _fill_core_sort_key(item: dict[str, Any]) -> tuple[float, ...]:
    return (
        _fill_rank_fit(item),
        _generation_readiness_score(item),
        _fill_local_cohesion(item),
        _family_match_score(item),
        _structure_score(item),
    )


def _fill_legacy_free_sort_key(item: dict[str, Any]) -> tuple[float, ...]:
    return (
        _fill_rank_fit(item),
        _family_match_score(item),
        _generation_readiness_score(item),
        _structure_score(item),
        _task_final_score(item),
    )


def _order_anchor_clarity(item: dict[str, Any]) -> float:
    profile = ((item.get("business_feature_profile") or {}).get("sentence_order_profile") or {})
    neutral = item.get("neutral_signal_profile") or {}
    parts = [
        float(profile.get("opening_signal_strength") or neutral.get("opening_signal_strength") or 0.0),
        float(profile.get("closing_signal_strength") or neutral.get("closing_signal_strength") or 0.0),
        float(profile.get("sequence_integrity") or neutral.get("sequence_integrity") or 0.0),
        1
        - min(
            1.0,
            float(profile.get("multi_path_risk") or neutral.get("multi_path_risk") or 0.0),
        ),
    ]
    return round(sum(parts) / max(1, len(parts)), 4)


def _center_semantic_strength(item: dict[str, Any]) -> float:
    axis = float((item.get("llm_main_axis_source_hint") or {}).get("score") or 0.0)
    argument = float((item.get("llm_argument_structure_hint") or {}).get("score") or 0.0)
    single = float((item.get("llm_single_center_judgment") or {}).get("score") or 0.0)
    return round((axis + argument + single) / 3, 4)


def _center_core_sort_key(item: dict[str, Any]) -> tuple[float, ...]:
    return (
        _center_semantic_strength(item),
        _generation_readiness_score(item),
        _family_match_score(item),
        _structure_score(item),
    )


def _center_legacy_free_sort_key(item: dict[str, Any]) -> tuple[float, ...]:
    return (
        _generation_readiness_score(item),
        _center_semantic_strength(item),
        _family_match_score(item),
        _structure_score(item),
    )


def _order_stability_strength(item: dict[str, Any]) -> float:
    profile = ((item.get("business_feature_profile") or {}).get("sentence_order_profile") or {})
    sequence_integrity = float(profile.get("sequence_integrity") or 0.0)
    multi_path_risk = float(profile.get("multi_path_risk") or 0.0)
    block_bonus = 0.12 if _candidate_type(item) == "sentence_block_group" else 0.0
    return round(block_bonus + 0.55 * sequence_integrity + 0.45 * (1 - min(1.0, multi_path_risk)), 4)


def _order_core_sort_key(item: dict[str, Any]) -> tuple[float, ...]:
    return (
        _candidate_type(item) == "sentence_block_group",
        _order_anchor_clarity(item),
        _order_stability_strength(item),
        _generation_readiness_score(item),
        _family_match_score(item),
    )


def _order_legacy_free_sort_key(item: dict[str, Any]) -> tuple[float, ...]:
    return (
        _order_anchor_clarity(item),
        _order_stability_strength(item),
        _generation_readiness_score(item),
        _family_match_score(item),
    )


def _rank_group_ids(
    family: str,
    group_rows: list[dict[str, Any]],
    *,
    view: str,
) -> list[str]:
    if view == "current":
        return [row["candidate_id"] for row in sorted(group_rows, key=lambda row: int(row["rank_position"]))]
    if family == "sentence_fill":
        key_fn = _fill_core_sort_key if view == "core" else _fill_legacy_free_sort_key
    elif family == "center_understanding":
        key_fn = _center_core_sort_key if view == "core" else _center_legacy_free_sort_key
    else:
        key_fn = _order_core_sort_key if view == "core" else _order_legacy_free_sort_key
    ranked = sorted(group_rows, key=lambda row: (key_fn(row["_item"]), row["candidate_id"]), reverse=True)
    return [row["candidate_id"] for row in ranked]


def _score_under_view(family: str, item: dict[str, Any], *, view: str) -> float:
    if family == "sentence_fill":
        return _fill_rank_fit(item)
    if family == "center_understanding":
        return _center_semantic_strength(item) if view == "core" else _generation_readiness_score(item)
    return _order_anchor_clarity(item) if view == "core" else _order_stability_strength(item)


def _group_top_gap(
    family: str,
    group_rows: list[dict[str, Any]],
    *,
    view: str,
) -> float:
    if family == "sentence_fill":
        score_fn = _fill_rank_fit
    elif family == "center_understanding":
        score_fn = _center_semantic_strength if view == "core" else _generation_readiness_score
    else:
        score_fn = _order_anchor_clarity if view == "core" else _order_stability_strength
    ranked = sorted(group_rows, key=lambda row: (score_fn(row["_item"]), row["candidate_id"]), reverse=True)
    if len(ranked) < 2:
        return 0.0
    return round(score_fn(ranked[0]["_item"]) - score_fn(ranked[1]["_item"]), 4)


def _group_stability_observation(family: str, group_rows: list[dict[str, Any]]) -> dict[str, str]:
    current_ids = _rank_group_ids(family, group_rows, view="current")
    core_ids = _rank_group_ids(family, group_rows, view="core")
    legacy_ids = _rank_group_ids(family, group_rows, view="legacy_free")
    current_top = current_ids[0] if current_ids else ""
    core_top = core_ids[0] if core_ids else ""
    legacy_top = legacy_ids[0] if legacy_ids else ""
    current_top_item = next((row["_item"] for row in group_rows if row["candidate_id"] == current_top), {})
    core_best_item = next((row["_item"] for row in group_rows if row["candidate_id"] == core_top), {})
    legacy_best_item = next((row["_item"] for row in group_rows if row["candidate_id"] == legacy_top), {})
    tolerance = {"sentence_fill": 0.012, "center_understanding": 0.03, "sentence_order": 0.045}[family]
    core_deficit = round(
        _score_under_view(family, core_best_item, view="core") - _score_under_view(family, current_top_item, view="core"),
        4,
    )
    legacy_deficit = round(
        _score_under_view(family, legacy_best_item, view="legacy_free")
        - _score_under_view(family, current_top_item, view="legacy_free"),
        4,
    )
    top1_stable = core_deficit <= tolerance and legacy_deficit <= tolerance
    top2_current = set(current_ids[:2])
    top2_core = set(core_ids[:2])
    top2_legacy = set(legacy_ids[:2])
    top3_stable = top1_stable and top2_current == top2_core == top2_legacy
    current_gap = _group_top_gap(family, group_rows, view="current")
    core_gap = _group_top_gap(family, group_rows, view="core")
    legacy_gap = _group_top_gap(family, group_rows, view="legacy_free")
    if not top1_stable and max(core_deficit, legacy_deficit) > tolerance + 0.02:
        flip_risk = "high"
    elif not top1_stable:
        flip_risk = "medium"
    elif min(current_gap, core_gap, legacy_gap) < 0.015:
        flip_risk = "medium"
    else:
        flip_risk = "low"
    legacy_influence = "none"
    if legacy_deficit > tolerance:
        legacy_influence = "material"
    elif not top3_stable:
        legacy_influence = "weak"
    observation = {
        "current_top1_candidate": current_top,
        "core_top1_candidate": core_top,
        "legacy_free_top1_candidate": legacy_top,
        "top1_stable_across_views": "true" if top1_stable else "false",
        "top3_order_stable_across_views": "true" if top3_stable else "false",
        "flip_risk_level": flip_risk,
        "current_top_gap": f"{current_gap:.4f}",
        "core_top_gap": f"{core_gap:.4f}",
        "legacy_free_top_gap": f"{legacy_gap:.4f}",
        "core_view_deficit": f"{max(0.0, core_deficit):.4f}",
        "legacy_view_deficit": f"{max(0.0, legacy_deficit):.4f}",
        "legacy_tiebreak_influence": legacy_influence,
    }
    if family == "sentence_fill":
        top_blank = _fill_blank_position(current_top_item)
        top_type = _candidate_type(current_top_item)
        top_width = _fill_paragraph_width(current_top_item)
        compact_preferred = top_type in {"closed_span", "functional_slot_unit"} or top_width <= 2
        middle_like_preferred = top_blank == "middle" or (
            _fill_local_cohesion(current_top_item) >= 0.62 and top_blank not in {"opening", "inserted"}
        )
        edge_risk = "high" if top_blank in {"opening", "inserted"} and flip_risk == "high" else ("medium" if top_blank in {"opening", "inserted"} else "low")
        observation.update(
            {
                "compact_fill_fit_preferred": "true" if compact_preferred else "false",
                "middle_like_preferred": "true" if middle_like_preferred else "false",
                "edge_position_risk": edge_risk,
            }
        )
    return observation


def _collect_ranked_items() -> dict[str, list[dict[str, Any]]]:
    session = get_session()
    try:
        service = MaterialPipelineV2Service(session)
        repo = SQLAlchemyMaterialSpanRepository(session)
        ranked_by_family: dict[str, list[dict[str, Any]]] = {}
        for family in FAMILIES:
            source_family = SOURCE_FAMILY_FOR_REPLAY[family]
            question_card = service._resolve_search_question_card(
                business_family_id=source_family,
                question_card_id=None,
            )
            materials = repo.list_v2_cached(
                business_family_id=source_family,
                status=MaterialStatus.PROMOTED.value,
                release_channel=ReleaseChannel.STABLE.value,
                limit=CACHED_LIMITS[family],
            )
            review_status_map = service._load_review_status_map([material.id for material in materials])
            materials, _ = service._apply_review_gate(
                materials=materials,
                review_status_map=review_status_map,
                mode="stable_relaxed",
            )
            candidates: list[dict[str, Any]] = []
            for material in materials:
                cached_payload = dict(material.v2_index_payload or {})
                cached_item = dict(cached_payload.get(source_family) or {})
                if not cached_item:
                    continue
                cached_item["_business_family_id"] = family
                cached_item["_cached_business_family_id"] = family
                question_ready_context = dict(cached_item.get("question_ready_context") or {})
                runtime_binding = dict(question_ready_context.get("runtime_binding") or {})
                runtime_binding["question_type"] = family
                question_ready_context["runtime_binding"] = runtime_binding
                cached_item["question_ready_context"] = question_ready_context
                refreshed = service.pipeline.refresh_cached_item(
                    cached_item=cached_item,
                    query_terms=[],
                    target_length=None,
                    length_tolerance=120,
                    enable_anchor_adaptation=True,
                    preserve_anchor=True,
                )
                refreshed["quality_score"] = float(
                    refreshed.get("quality_score") or getattr(material, "quality_score", 0.0) or 0.0
                )
                refreshed["review_status"] = review_status_map.get(material.id)
                candidates.append(refreshed)
            ranked_by_family[family] = service.pipeline._select_diverse_items(
                candidates,
                FINAL_CANDIDATES_PER_FAMILY,
            )
        return ranked_by_family
    finally:
        session.close()


def _build_manifest_rows(items_by_family: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for family, items in items_by_family.items():
        capped = items[:FINAL_CANDIDATES_PER_FAMILY]
        for family_index in range(GROUPS_PER_FAMILY):
            group = capped[family_index * GROUP_SIZE : (family_index + 1) * GROUP_SIZE]
            if len(group) < 2:
                continue
            replay_group_id = f"{family}.r{family_index + 1:02d}"
            for index_in_group, item in enumerate(group):
                rows.append(
                    {
                        "family": family,
                        "replay_group_id": replay_group_id,
                        "candidate_id": str(item.get("candidate_id") or ""),
                        "article_id": str(item.get("article_id") or ""),
                        "rank_position": family_index * GROUP_SIZE + index_in_group + 1,
                        "llm_selection_score": round(float(item.get("llm_selection_score") or 0.0), 4),
                        "llm_family_match_hint": _family_match_score(item),
                        "llm_generation_readiness": _generation_readiness_score(item),
                        "quality_score": round(float(item.get("quality_score") or 0.0), 4),
                        "selected_task_scoring": _task_final_score(item),
                        "material_card_score": _top_score(list(item.get("eligible_material_cards") or []), "score"),
                        "business_card_score": _top_score(list(item.get("eligible_business_cards") or []), "score"),
                        "material_structure_label": _material_structure_label(item),
                        "paragraph_span": _paragraph_span(item),
                        "sentence_span": _sentence_span(item),
                        "is_top_candidate": "true" if index_in_group == 0 else "false",
                        "compare_bucket": _compare_bucket(index_in_group),
                        "_item": item,
                    }
                )
    return rows


def _judge_sentence_fill_group(group_rows: list[dict[str, Any]]) -> list[dict[str, str]]:
    top = group_rows[0]
    top_item = top["_item"]
    fill_profile = ((top_item.get("business_feature_profile") or {}).get("sentence_fill_profile") or {})
    top_blank_position = str(fill_profile.get("blank_position") or "")
    top_anchor_ids = list((_asset_anchor(top_item).get("anchor_sample_ids") or []))
    strongest_control = max(
        group_rows[1:],
        key=lambda row: (_fill_rank_fit(row["_item"]), _generation_readiness_score(row["_item"])),
    )
    control_item = strongest_control["_item"]
    top_composite = _fill_rank_fit(top_item)
    control_composite = _fill_rank_fit(control_item)
    top_uncovered_edge = top_blank_position in {"opening", "inserted"} and not top_anchor_ids
    required_margin = 0.005 if top_uncovered_edge else -0.01
    rank_justified = top_composite >= control_composite + required_margin
    top_mechanical = bool(
        _generation_readiness_score(top_item) < _generation_readiness_score(control_item)
        and (
            float(top.get("quality_score") or 0.0) > float(strongest_control.get("quality_score") or 0.0)
            or float(top.get("material_card_score") or 0.0)
            > float(strongest_control.get("material_card_score") or 0.0)
            or float(top.get("business_card_score") or 0.0)
            > float(strongest_control.get("business_card_score") or 0.0)
        )
    )
    top_reason = (
        "top candidate keeps clearer local closure, interpretable slot function, and closer Round 1 fill alignment"
        if rank_justified
        else "top candidate is coverage-light on opening or inserted behavior, while a lower candidate shows more stable local closure"
    )
    top_calibration = (
        "none"
        if rank_justified
        else "demote uncovered opening or inserted candidates one tier unless llm_selection_score clearly dominates"
    )

    judgments: list[dict[str, str]] = []
    for row in group_rows:
        item = row["_item"]
        is_top = row["is_top_candidate"] == "true"
        if is_top:
            judgments.append(
                {
                    "replay_judgment": "top_preferred" if rank_justified else "top_should_be_demoted",
                    "is_rank_justified": "true" if rank_justified else "false",
                    "main_reason": top_reason,
                    "mechanical_residue_flag": "true" if top_mechanical else "false",
                    "calibration_hint": top_calibration,
                    "notes": f"blank_position={top_blank_position}; anchor_role={_asset_anchor_role(item)}",
                }
            )
            continue
        should_promote = (not rank_justified) and row["candidate_id"] == strongest_control["candidate_id"]
        judgments.append(
            {
                "replay_judgment": "control_should_be_promoted" if should_promote else "control_weaker",
                "is_rank_justified": "false" if should_promote else "true",
                "main_reason": (
                    "lower candidate shows more stable middle-like fill behavior and should move ahead"
                    if should_promote
                    else "lower candidate is weaker on local closure or slot interpretability"
                ),
                "mechanical_residue_flag": "true" if should_promote else "false",
                "calibration_hint": (
                    "let stable middle-like local cohesion outrank uncovered edge-position candidates"
                    if should_promote
                    else "none"
                ),
                "notes": f"anchor_role={_asset_anchor_role(item)}",
            }
        )
    return judgments


def _judge_center_group(group_rows: list[dict[str, Any]]) -> list[dict[str, str]]:
    top = group_rows[0]
    top_item = top["_item"]
    strongest_control = max(
        group_rows[1:],
        key=lambda row: (_center_semantic_strength(row["_item"]), _generation_readiness_score(row["_item"])),
    )
    control_item = strongest_control["_item"]
    top_boundary = _asset_anchor_role(top_item) == "review_holdout_boundary"
    top_semantic = _center_semantic_strength(top_item)
    control_semantic = _center_semantic_strength(control_item)
    rank_justified = not (top_boundary and control_semantic >= top_semantic - 0.02)
    top_mechanical = bool(
        _generation_readiness_score(top_item) < _generation_readiness_score(control_item)
        and (
            float(top.get("quality_score") or 0.0) > float(strongest_control.get("quality_score") or 0.0)
            or float(top.get("material_card_score") or 0.0)
            > float(strongest_control.get("material_card_score") or 0.0)
        )
    )
    top_reason = (
        "top candidate shows stronger single-center stability and clearer main-axis / argument-structure hints"
        if rank_justified
        else "top candidate still looks boundary-like, while a lower candidate is closer to stable gold-ready center-understanding material"
    )
    top_calibration = (
        "none"
        if rank_justified
        else "demote review-holdout-boundary patterns one tier unless single-center and semantic hints clearly dominate"
    )

    judgments: list[dict[str, str]] = []
    for row in group_rows:
        item = row["_item"]
        axis_hint = str((item.get("llm_main_axis_source_hint") or {}).get("value") or "")
        structure_hint = str((item.get("llm_argument_structure_hint") or {}).get("value") or "")
        is_top = row["is_top_candidate"] == "true"
        if is_top:
            judgments.append(
                {
                    "replay_judgment": "top_preferred" if rank_justified else "top_should_be_demoted",
                    "is_rank_justified": "true" if rank_justified else "false",
                    "main_reason": top_reason,
                    "mechanical_residue_flag": "true" if top_mechanical else "false",
                    "calibration_hint": top_calibration,
                    "notes": f"axis={axis_hint}; structure={structure_hint}; anchor_role={_asset_anchor_role(item)}",
                }
            )
            continue
        should_promote = (not rank_justified) and row["candidate_id"] == strongest_control["candidate_id"]
        judgments.append(
            {
                "replay_judgment": "control_should_be_promoted" if should_promote else "control_weaker",
                "is_rank_justified": "false" if should_promote else "true",
                "main_reason": (
                    "lower candidate is more gold-ready-like on single-center, main-axis, and argument-structure stability"
                    if should_promote
                    else "lower candidate is weaker on single-center or center-understanding semantic hints"
                ),
                "mechanical_residue_flag": "true" if should_promote else "false",
                "calibration_hint": (
                    "raise llm_main_axis_source_hint and llm_argument_structure_hint ahead of generic quality tie-breaks"
                    if should_promote
                    else "none"
                ),
                "notes": f"axis={axis_hint}; structure={structure_hint}; anchor_role={_asset_anchor_role(item)}",
            }
        )
    return judgments


def _judge_sentence_order_group(group_rows: list[dict[str, Any]]) -> list[dict[str, str]]:
    top = group_rows[0]
    top_item = top["_item"]
    strongest_control = max(
        group_rows[1:],
        key=lambda row: (_order_anchor_clarity(row["_item"]), _generation_readiness_score(row["_item"])),
    )
    control_item = strongest_control["_item"]
    top_block = str(top_item.get("candidate_type") or "") == "sentence_block_group"
    control_block = str(control_item.get("candidate_type") or "") == "sentence_block_group"
    top_clarity = _order_anchor_clarity(top_item)
    control_clarity = _order_anchor_clarity(control_item)
    top_readiness = _generation_readiness_score(top_item)
    control_readiness = _generation_readiness_score(control_item)
    top_family_match = _family_match_score(top_item)
    control_family_match = _family_match_score(control_item)
    rank_justified = not (
        (not top_block and control_block)
        or (
            control_block
            and control_clarity >= top_clarity + 0.10
            and control_readiness >= top_readiness - 0.03
            and control_family_match >= top_family_match - 0.03
        )
    )
    top_mechanical = bool(
        _generation_readiness_score(top_item) < _generation_readiness_score(control_item)
        and (
            float(top.get("quality_score") or 0.0) > float(strongest_control.get("quality_score") or 0.0)
            or float(top.get("material_card_score") or 0.0)
            > float(strongest_control.get("material_card_score") or 0.0)
        )
    )
    top_reason = (
        "top candidate shows stronger 6-unit stability, clearer anchor signals, and better sentence-order block quality"
        if rank_justified
        else "top candidate is less stable than a lower candidate on block integrity or anchor clarity"
    )
    top_calibration = (
        "none"
        if rank_justified
        else "raise sentence_block_group and anchor_clarity ahead of residual generic scores"
    )

    judgments: list[dict[str, str]] = []
    for row in group_rows:
        item = row["_item"]
        is_top = row["is_top_candidate"] == "true"
        if is_top:
            judgments.append(
                {
                    "replay_judgment": "top_preferred" if rank_justified else "top_should_be_demoted",
                    "is_rank_justified": "true" if rank_justified else "false",
                    "main_reason": top_reason,
                    "mechanical_residue_flag": "true" if top_mechanical else "false",
                    "calibration_hint": top_calibration,
                    "notes": f"candidate_type={item.get('candidate_type')}; anchor_role={_asset_anchor_role(item)}",
                }
            )
            continue
        should_promote = (not rank_justified) and row["candidate_id"] == strongest_control["candidate_id"]
        judgments.append(
            {
                "replay_judgment": "control_should_be_promoted" if should_promote else "control_weaker",
                "is_rank_justified": "false" if should_promote else "true",
                "main_reason": (
                    "lower candidate is a cleaner sentence_block_group with stronger anchor clarity and should move ahead"
                    if should_promote
                    else "lower candidate is weaker on block stability or anchor clarity"
                ),
                "mechanical_residue_flag": "true" if should_promote else "false",
                "calibration_hint": (
                    "keep sequence_integrity and multi_path_risk ahead of residual card or quality tie-breaks"
                    if should_promote
                    else "none"
                ),
                "notes": f"candidate_type={item.get('candidate_type')}; anchor_role={_asset_anchor_role(item)}",
            }
        )
    return judgments


def _judge_group(family: str, group_rows: list[dict[str, Any]]) -> list[dict[str, str]]:
    if family == "sentence_fill":
        return _judge_sentence_fill_group(group_rows)
    if family == "center_understanding":
        return _judge_center_group(group_rows)
    return _judge_sentence_order_group(group_rows)


def _build_results_rows(manifest_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in manifest_rows:
        grouped[(row["family"], row["replay_group_id"])].append(row)

    results: list[dict[str, Any]] = []
    for (family, replay_group_id), group_rows in grouped.items():
        sorted_group = sorted(group_rows, key=lambda row: int(row["rank_position"]))
        judgments = _judge_group(family, sorted_group)
        observation = _group_stability_observation(family, sorted_group)
        for row, judgment in zip(sorted_group, judgments):
            results.append(
                {
                    "family": family,
                    "replay_group_id": replay_group_id,
                    "candidate_id": row["candidate_id"],
                    "rank_position": row["rank_position"],
                    "is_top_candidate": row["is_top_candidate"],
                    "replay_judgment": judgment["replay_judgment"],
                    "is_rank_justified": judgment["is_rank_justified"],
                    "main_reason": judgment["main_reason"],
                    "mechanical_residue_flag": judgment["mechanical_residue_flag"],
                    "calibration_hint": judgment["calibration_hint"],
                    "current_top1_candidate": observation["current_top1_candidate"],
                    "core_top1_candidate": observation["core_top1_candidate"],
                    "legacy_free_top1_candidate": observation["legacy_free_top1_candidate"],
                    "top1_stable_across_views": observation["top1_stable_across_views"],
                    "top3_order_stable_across_views": observation["top3_order_stable_across_views"],
                    "flip_risk_level": observation["flip_risk_level"],
                    "current_top_gap": observation["current_top_gap"],
                    "core_top_gap": observation["core_top_gap"],
                    "legacy_free_top_gap": observation["legacy_free_top_gap"],
                    "core_view_deficit": observation["core_view_deficit"],
                    "legacy_view_deficit": observation["legacy_view_deficit"],
                    "legacy_tiebreak_influence": observation["legacy_tiebreak_influence"],
                    "compact_fill_fit_preferred": observation.get("compact_fill_fit_preferred", ""),
                    "middle_like_preferred": observation.get("middle_like_preferred", ""),
                    "edge_position_risk": observation.get("edge_position_risk", ""),
                    "notes": judgment["notes"],
                }
            )
    return results


def _write_csv(path: Path, rows: list[dict[str, Any]], *, drop_internal_item: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    writable_rows: list[dict[str, Any]] = []
    for row in rows:
        if drop_internal_item:
            writable_rows.append({key: value for key, value in row.items() if key != "_item"})
        else:
            writable_rows.append(row)
    fieldnames = list(writable_rows[0].keys()) if writable_rows else []
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(writable_rows)


def _family_summary(results_rows: list[dict[str, Any]], family: str) -> dict[str, Any]:
    subset = [row for row in results_rows if row["family"] == family]
    groups = sorted({row["replay_group_id"] for row in subset})
    unjustified = {
        row["replay_group_id"]
        for row in subset
        if row["is_rank_justified"] == "false" and row["is_top_candidate"] == "true"
    }
    residue = [row for row in subset if row["mechanical_residue_flag"] == "true"]
    top_rows = [row for row in subset if row["is_top_candidate"] == "true"]
    top1_stable = [row for row in top_rows if row["top1_stable_across_views"] == "true"]
    top3_stable = [row for row in top_rows if row["top3_order_stable_across_views"] == "true"]
    high_flip = [row for row in top_rows if row["flip_risk_level"] == "high"]
    material_legacy = [row for row in top_rows if row["legacy_tiebreak_influence"] == "material"]
    calibration_hints = [
        row["calibration_hint"]
        for row in subset
        if row["calibration_hint"] and row["calibration_hint"] != "none"
    ]
    return {
        "group_count": len(groups),
        "top_justified_groups": len(groups) - len(unjustified),
        "top_unjustified_groups": len(unjustified),
        "mechanical_residue_count": len(residue),
        "top1_stable_groups": len(top1_stable),
        "top3_order_stable_groups": len(top3_stable),
        "high_flip_risk_groups": len(high_flip),
        "material_legacy_tiebreak_groups": len(material_legacy),
        "top_calibration_hints": Counter(calibration_hints).most_common(3),
    }


def _family_stability_label(summary: dict[str, Any], family: str) -> str:
    if family == "sentence_fill":
        if summary["top_justified_groups"] >= 7 and summary["top1_stable_groups"] >= 7:
            return "conditionally_stable_with_boundary_risk"
        return "needs_more_stability_work"
    if family == "center_understanding":
        if summary["top_justified_groups"] == summary["group_count"] and summary["mechanical_residue_count"] <= 1:
            return "stable"
        return "watch_boundary_drift"
    if summary["top_justified_groups"] == summary["group_count"] and summary["mechanical_residue_count"] == 0:
        return "stable"
    return "watch_order_drift"


def _build_report(results_rows: list[dict[str, Any]]) -> str:
    summaries = {
        family: _family_summary(results_rows, family)
        for family in FAMILIES
    }
    yield_gain = {
        family: summaries[family]["top_justified_groups"] - summaries[family]["mechanical_residue_count"]
        for family in summaries
    }
    best_family = max(yield_gain.items(), key=lambda item: item[1])[0]
    least_stable_family = max(
        summaries.items(),
        key=lambda item: (item[1]["top_unjustified_groups"], item[1]["mechanical_residue_count"]),
    )[0]
    if (
        summaries["sentence_fill"]["top1_stable_groups"] >= 7
        and summaries["sentence_fill"]["top_unjustified_groups"] <= 1
        and summaries["sentence_fill"]["material_legacy_tiebreak_groups"] == 0
    ):
        next_stage = "enter_material_selection_stable_phase"
    elif summaries["sentence_fill"]["top_unjustified_groups"] <= 1:
        next_stage = "need_boundary_case_only_guardrail"
    else:
        next_stage = "need_one_more_sentence_fill_micro_tuning"

    lines = [
        "# Round 1 Material Scoring Replay Report",
        "",
        f"- generated_at: {datetime.now(timezone.utc).isoformat()}",
        f"- replay_scope: sentence_fill / center_understanding / sentence_order, {GROUPS_PER_FAMILY} groups each",
        "- source_mode: cached current v2 material payloads replayed with current ranking keys",
        "- interpretation: post-calibration replay check anchored to current Round 1 assets, not a separate A/B experiment",
        "- stability_views: current ranking / family-core ranking / legacy-tiebreak-light ranking",
        "",
        "## Overall",
        "",
        f"- strongest gain: `{best_family}`",
        f"- least stable family: `{least_stable_family}`",
        f"- next step suggestion: `{next_stage}`",
        "",
    ]
    for family in FAMILIES:
        summary = summaries[family]
        lines.extend(
            [
                f"## {family}",
                "",
                f"- groups: `{summary['group_count']}`",
                f"- top rank justified groups: `{summary['top_justified_groups']}`",
                f"- top rank unjustified groups: `{summary['top_unjustified_groups']}`",
                f"- top1 stable across views: `{summary['top1_stable_groups']}`",
                f"- top3 order stable across views: `{summary['top3_order_stable_groups']}`",
                f"- high flip-risk groups: `{summary['high_flip_risk_groups']}`",
                f"- material legacy-tiebreak groups: `{summary['material_legacy_tiebreak_groups']}`",
                f"- mechanical residue flags: `{summary['mechanical_residue_count']}`",
                f"- stability judgement: `{_family_stability_label(summary, family)}`",
                f"- dominant calibration hints: `{summary['top_calibration_hints']}`",
                "",
            ]
        )
        if family == "sentence_fill":
            lines.append(
                "- replay read: ranking improves when stable middle-like local closure stays ahead of coverage-light edge-position candidates."
            )
        elif family == "center_understanding":
            lines.append(
                "- replay read: main-axis and argument-structure hints are helping, but boundary-like center-understanding candidates still need more calibration."
            )
        else:
            lines.append(
                "- replay read: ranking is strongest when sentence_block_group stability and anchor clarity stay ahead of weaker order-like forms."
            )
        lines.append("")

    fill_boundary_rows = [
        row
        for row in results_rows
        if row["family"] == "sentence_fill" and row["is_top_candidate"] == "true"
    ]
    unstable_fill = [row for row in fill_boundary_rows if row["flip_risk_level"] != "low"]
    lines.extend(["## Stability Read", ""])
    lines.append(
        f"- sentence_fill top1 stability: `{summaries['sentence_fill']['top1_stable_groups']}/{summaries['sentence_fill']['group_count']}` groups stable across current/core/legacy-free views"
    )
    lines.append(
        f"- center_understanding top1 stability: `{summaries['center_understanding']['top1_stable_groups']}/{summaries['center_understanding']['group_count']}`"
    )
    lines.append(
        f"- sentence_order top1 stability: `{summaries['sentence_order']['top1_stable_groups']}/{summaries['sentence_order']['group_count']}`"
    )
    if unstable_fill:
        lines.append(
            "- sentence_fill boundary read: remaining instability is concentrated in low-score near-neighbor groups where compact closed-span and broader multi-paragraph candidates are still close."
        )
    else:
        lines.append("- sentence_fill boundary read: no strong near-neighbor instability remains in this replay slice.")
    lines.append("")

    residue_notes: list[str] = []
    for row in results_rows:
        if row["mechanical_residue_flag"] == "true":
            residue_notes.append(
                f"- `{row['family']}` / `{row['replay_group_id']}` / `{row['candidate_id']}`: {row['main_reason']}"
            )
    lines.extend(["## Mechanical Residue", ""])
    if residue_notes:
        lines.extend(residue_notes[:12])
    else:
        lines.append("- no strong mechanical residue was observed in this replay slice")
    lines.extend(["", "## Boundary / Legacy Observation", ""])
    boundary_rows = [
        row
        for row in results_rows
        if row["family"] == "sentence_fill"
        and row["is_top_candidate"] == "true"
        and (
            row["flip_risk_level"] != "low"
            or row["legacy_tiebreak_influence"] != "none"
            or row["edge_position_risk"] in {"medium", "high"}
        )
    ]
    if boundary_rows:
        for row in boundary_rows[:8]:
            lines.append(
                f"- `{row['replay_group_id']}`: top1=`{row['candidate_id']}`, flip_risk=`{row['flip_risk_level']}`, edge_position_risk=`{row.get('edge_position_risk') or 'n/a'}`, legacy_tiebreak_influence=`{row['legacy_tiebreak_influence']}`, current/core/legacy-free=`{row['current_top1_candidate']}` / `{row['core_top1_candidate']}` / `{row['legacy_free_top1_candidate']}`"
            )
    else:
        lines.append("- no boundary-like fill groups currently show observable flip risk or legacy-tiebreak leakage")
    lines.extend(
        [
            "",
            "## Verdict",
            "",
            f"- current ranking is most improved for `{best_family}`",
            f"- current ranking still needs the most calibration on `{least_stable_family}`",
            f"- stability verdict: `{next_stage}`",
            f"- recommended next action: `{next_stage}`",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> None:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    items_by_family = _collect_ranked_items()
    manifest_rows = _build_manifest_rows(items_by_family)
    results_rows = _build_results_rows(manifest_rows)
    report_text = _build_report(results_rows)

    _write_csv(MANIFEST_PATH, manifest_rows, drop_internal_item=True)
    _write_csv(RESULTS_PATH, results_rows)
    REPORT_PATH.write_text(report_text, encoding="utf-8")

    print(MANIFEST_PATH)
    print(RESULTS_PATH)
    print(REPORT_PATH)


if __name__ == "__main__":
    main()
