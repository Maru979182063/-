from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
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
from app.services.material_pipeline_v2 import ACTION_MEASURE_MARKERS, MaterialPipelineV2  # noqa: E402


FAMILY_MAP = {
    "center_understanding": "title_selection",
    "sentence_order": "sentence_order",
    "sentence_fill": "sentence_fill",
}
FORMAL_TYPES = {
    "center_understanding": {"whole_passage", "closed_span", "multi_paragraph_unit"},
    "sentence_order": {"ordered_unit_group"},
    "sentence_fill": {"functional_slot_unit"},
}
FILL_SOURCE_SCOPE = {
    "sentence_group",
    "multi_paragraph_unit",
    "paragraph_window",
    "sentence_block_group",
}


@dataclass
class ReviewRecord:
    review_family: str
    business_family: str
    material_id: str
    article_id: str
    source_candidate_type: str
    runtime_candidate_type: str
    system_caught: bool
    system_caught_strict: bool
    rebuild_none: bool
    formal_unit_hit: bool
    has_selected_task_scoring: bool
    final_score: float
    readiness_score: float
    structural_level: str
    business_level: str
    business_accept: bool
    root_cause: str
    issues: list[str]
    potential_when_rejected: bool
    a_b_c_d: str
    text_clip: str


def _clip(text: str, limit: int = 120) -> str:
    normalized = " ".join(str(text or "").replace("\xa0", " ").split())
    if len(normalized) <= limit:
        return normalized
    return f"{normalized[:limit]}..."


def _safe_float(value: Any) -> float:
    try:
        if value is None:
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _sentence_count(text: str) -> int:
    parts = [segment for segment in re.split(r"(?<=[。！？!?])", str(text or "")) if segment.strip()]
    return max(1, len(parts))


def _contains_any(text: str, markers: tuple[str, ...]) -> bool:
    return any(marker in text for marker in markers)


def _count_marker(text: str, markers: tuple[str, ...]) -> int:
    return sum(1 for marker in markers if marker in text)


def _enum_density(text: str) -> float:
    stripped = re.sub(r"\s+", "", str(text or ""))
    if not stripped:
        return 0.0
    enum = sum(1 for ch in stripped if ch in {"，", ",", "；", ";", "、"})
    return enum / len(stripped)


def _title_potential(text: str) -> bool:
    if len(text) < 80 or len(text) > 560:
        return False
    summary_or_turn = ("总之", "可见", "由此", "但是", "然而", "不过", "关键")
    return _contains_any(text, summary_or_turn)


def _title_strong_potential(text: str) -> bool:
    if not _title_potential(text):
        return False
    sent_n = _sentence_count(text)
    return 3 <= sent_n <= 8 and _count_marker(text, ("总之", "可见", "由此", "关键")) >= 1


def _order_potential(text: str) -> bool:
    if _sentence_count(text) < 6:
        return False
    markers = ("首先", "其次", "最后", "因此", "同时", "然后", "一方面", "另一方面")
    return _contains_any(text, markers)


def _order_strong_potential(text: str) -> bool:
    sent_n = _sentence_count(text)
    if sent_n < 6 or sent_n > 9:
        return False
    markers = ("首先", "其次", "最后", "因此", "同时", "然后", "一方面", "另一方面")
    return _count_marker(text, markers) >= 2


def _fill_potential(text: str, source_type: str) -> bool:
    if source_type not in FILL_SOURCE_SCOPE:
        return False
    if _sentence_count(text) < 2:
        return False
    markers = ("因此", "同时", "对此", "接下来", "总之", "可见", "应当", "需要", "必须", "建议")
    return _contains_any(text, markers)


def _fill_strong_potential(text: str, source_type: str) -> bool:
    if not _fill_potential(text, source_type):
        return False
    sent_n = _sentence_count(text)
    if sent_n < 3 or sent_n > 7:
        return False
    markers = ("因此", "同时", "对此", "接下来", "总之", "可见", "应当", "需要", "必须", "建议")
    return _count_marker(text, markers) >= 2


def _fill_topic_intro_generic(slot_text: str, right_context: str) -> bool:
    intro_markers = ("当前", "如今", "近年来", "在此背景下", "面对", "随着")
    problem_markers = ("问题", "挑战", "困境", "矛盾", "痛点", "如何", "为何")
    digit_count = len(re.findall(r"\d|[%％]", slot_text))
    digit_density = digit_count / max(1, len(re.sub(r"\s+", "", slot_text)))
    if not right_context.strip():
        return True
    if digit_density >= 0.10 and not _contains_any(slot_text, intro_markers + problem_markers):
        return True
    if len(slot_text) >= 140 and "?" not in slot_text and "？" not in slot_text:
        return True
    if _count_marker(slot_text, intro_markers + problem_markers) == 0 and _enum_density(slot_text) >= 0.08:
        return True
    return False


def _fill_countermeasure_generic(slot_text: str, left_context: str) -> bool:
    modal = ("应当", "应该", "要", "需要", "必须", "建议")
    policy = ("会议", "报告", "规划", "意见", "提出", "强调", "部署", "要求", "指出")
    problem = ("问题", "挑战", "困境", "矛盾", "痛点", "风险", "压力")
    action_hits = _count_marker(slot_text, ACTION_MEASURE_MARKERS)
    modal_hits = _count_marker(slot_text, modal)
    policy_hits = _count_marker(slot_text, policy)
    problem_context = _contains_any(left_context, problem)
    if modal_hits > 0 and action_hits == 0 and not problem_context:
        return True
    if policy_hits >= 2 and action_hits == 0 and not problem_context:
        return True
    return False


def _center_evaluate(
    *,
    runtime_item: dict[str, Any] | None,
    source_text: str,
    formal_hit: bool,
) -> tuple[str, str, bool, list[str], bool]:
    issues: list[str] = []
    if runtime_item is None:
        potential = _title_potential(source_text)
        if _title_strong_potential(source_text):
            return "fail", "usable", True, ["rebuild_none", "formal_path_miss"], potential
        if potential:
            return "fail", "borderline", False, ["rebuild_none", "formal_path_miss"], potential
        return "fail", "unusable", False, ["rebuild_none"], potential

    scoring = dict(runtime_item.get("selected_task_scoring") or {})
    struct = dict(scoring.get("structure_scores") or {})
    risks = dict(scoring.get("risk_penalties") or {})
    final_score = _safe_float(scoring.get("final_candidate_score"))

    single_center = _safe_float(struct.get("single_center_score"))
    closure = _safe_float(struct.get("closure_score"))
    lift = _safe_float(struct.get("theme_lift_score"))
    example_penalty = _safe_float(risks.get("example_dominance_penalty"))
    text = str(runtime_item.get("text") or source_text or "")
    enum_density = _enum_density(text)

    if not formal_hit:
        issues.append("not_formal_unit")
    if single_center < 0.46:
        issues.append("single_center_weak")
    if closure < 0.48:
        issues.append("closure_weak")
    if lift < 0.48:
        issues.append("theme_lift_weak")
    if example_penalty > 0.55:
        issues.append("example_dominant")
    if enum_density >= 0.10:
        issues.append("enumeration_heavy")

    structural = "fail"
    if formal_hit and single_center >= 0.46 and closure >= 0.48 and lift >= 0.48 and example_penalty <= 0.55:
        structural = "pass"
    elif formal_hit and (single_center + closure + lift) / 3 >= 0.38:
        structural = "borderline"

    business = "unusable"
    if structural == "pass" and final_score >= 0.48 and 80 <= len(text) <= 560 and enum_density < 0.12:
        business = "usable"
    elif final_score >= 0.34 and (structural in {"pass", "borderline"} or _title_potential(text)):
        business = "borderline"
    if not formal_hit:
        if _title_strong_potential(text) and final_score >= 0.24:
            business = "usable"
        elif _title_potential(text) and final_score >= 0.16 and business == "unusable":
            business = "borderline"

    return structural, business, business == "usable", issues, _title_potential(text)


def _order_evaluate(
    *,
    runtime_item: dict[str, Any] | None,
    source_text: str,
    formal_hit: bool,
) -> tuple[str, str, bool, list[str], bool]:
    issues: list[str] = []
    if runtime_item is None:
        potential = _order_potential(source_text)
        if _order_strong_potential(source_text):
            return "fail", "usable", True, ["rebuild_none", "formal_path_miss"], potential
        if potential:
            return "fail", "borderline", False, ["rebuild_none", "formal_path_miss"], potential
        return "fail", "unusable", False, ["rebuild_none"], potential

    scoring = dict(runtime_item.get("selected_task_scoring") or {})
    struct = dict(scoring.get("structure_scores") or {})
    final_score = _safe_float(scoring.get("final_candidate_score"))

    first_ok = _safe_float(struct.get("first_eligibility_score"))
    last_ok = _safe_float(struct.get("last_eligibility_score"))
    pairwise = _safe_float(struct.get("pairwise_constraint_score"))
    local_binding = _safe_float(struct.get("local_binding_score"))
    text = str(runtime_item.get("text") or source_text or "")
    sent_n = _sentence_count(text)

    if not formal_hit:
        issues.append("not_formal_unit")
    if first_ok < 0.35:
        issues.append("first_weak")
    if last_ok < 0.35:
        issues.append("last_weak")
    if pairwise < 0.42:
        issues.append("pairwise_weak")
    if local_binding < 0.38:
        issues.append("local_binding_weak")
    if sent_n < 6:
        issues.append("sentence_count_small")

    structural = "fail"
    if formal_hit and first_ok >= 0.50 and last_ok >= 0.50 and pairwise >= 0.50 and local_binding >= 0.50:
        structural = "pass"
    elif pairwise >= 0.42 and local_binding >= 0.38:
        structural = "borderline"

    strong_order = pairwise >= 0.52 and local_binding >= 0.48 and first_ok >= 0.35 and last_ok >= 0.35 and sent_n >= 6
    medium_order = pairwise >= 0.42 and local_binding >= 0.38 and sent_n >= 5
    business = "unusable"
    if strong_order and final_score >= 0.40:
        business = "usable"
    elif medium_order and final_score >= 0.24:
        business = "borderline"
    if not formal_hit:
        if _order_strong_potential(text):
            business = "usable"
        elif _order_potential(text) and business == "unusable":
            business = "borderline"

    return structural, business, business == "usable", issues, _order_potential(text)


def _fill_evaluate(
    *,
    runtime_item: dict[str, Any] | None,
    source_text: str,
    source_type: str,
    formal_hit: bool,
) -> tuple[str, str, bool, list[str], bool]:
    issues: list[str] = []
    if runtime_item is None:
        potential = _fill_potential(source_text, source_type)
        if _fill_strong_potential(source_text, source_type):
            return "fail", "usable", True, ["rebuild_none", "formal_path_miss"], potential
        if potential:
            return "fail", "borderline", False, ["rebuild_none", "formal_path_miss"], potential
        return "fail", "unusable", False, ["rebuild_none"], potential

    text = str(runtime_item.get("text") or source_text or "")
    scoring = dict(runtime_item.get("selected_task_scoring") or {})
    final_score = _safe_float(scoring.get("final_candidate_score"))
    meta = dict(runtime_item.get("meta") or {})
    role = str(meta.get("slot_role") or "")
    function = str(meta.get("slot_function") or "")
    left_context = str(meta.get("left_context_text") or "")
    right_context = str(meta.get("right_context_text") or "")
    blank_ready = bool(meta.get("blank_value_ready"))

    if not formal_hit:
        issues.append("not_formal_unit")
    if not role or not function:
        issues.append("slot_role_or_function_missing")
    if not blank_ready:
        issues.append("blank_value_not_ready")
    if role == "opening" and not right_context.strip():
        issues.append("opening_missing_right_context")
    if role == "ending" and not left_context.strip():
        issues.append("ending_missing_left_context")
    if role == "middle" and function == "carry_previous" and not left_context.strip():
        issues.append("middle_carry_missing_left_context")
    if role == "middle" and function == "lead_next" and not right_context.strip():
        issues.append("middle_lead_missing_right_context")
    if role == "middle" and function == "bridge_both_sides" and (not left_context.strip() or not right_context.strip()):
        issues.append("middle_bridge_missing_two_side_context")
    if role == "opening" and function == "topic_intro" and _fill_topic_intro_generic(text, right_context):
        issues.append("topic_intro_generic")
    if role == "ending" and function == "countermeasure" and _fill_countermeasure_generic(text, left_context):
        issues.append("countermeasure_generic")
    if len(text) < 10:
        issues.append("slot_too_short")
    if len(text) > 180:
        issues.append("slot_too_long")
    if final_score < 0.30:
        issues.append("fill_score_low")
    elif final_score < 0.40:
        issues.append("fill_score_mid")

    structural = "fail"
    if formal_hit and blank_ready and not any(
        item in issues
        for item in (
            "slot_role_or_function_missing",
            "topic_intro_generic",
            "countermeasure_generic",
            "slot_too_short",
        )
    ):
        structural = "pass"
    elif formal_hit and role and function:
        structural = "borderline"

    business = "unusable"
    if structural == "pass" and final_score >= 0.40 and "slot_too_long" not in issues:
        business = "usable"
    elif structural in {"pass", "borderline"} and final_score >= 0.30:
        business = "borderline"

    return structural, business, business == "usable", issues, _fill_potential(text, source_type)


def _root_cause(
    *,
    system_caught_strict: bool,
    business_level: str,
    formal_hit: bool,
    issues: list[str],
    potential_when_rejected: bool,
) -> str:
    if system_caught_strict and business_level != "usable":
        if any(token in issues for token in ("topic_intro_generic", "countermeasure_generic", "slot_too_long", "opening_missing_right_context", "ending_missing_left_context")):
            return "bridge_conflict"
        if any(token in issues for token in ("fill_score_low", "fill_score_mid")):
            return "bridge_conflict"
        if business_level == "borderline":
            return "bridge_conflict"
        return "other"
    if (not system_caught_strict) and business_level == "usable":
        return "unreasonable_requirement"
    if (not system_caught_strict) and business_level == "borderline" and potential_when_rejected:
        return "unreasonable_requirement"
    if not formal_hit and potential_when_rejected:
        return "unreasonable_requirement"
    if "rebuild_none" in issues:
        return "weak_source"
    return "weak_source"


def _abcd(*, system_caught_strict: bool, business_accept: bool) -> str:
    if system_caught_strict and business_accept:
        return "A"
    if system_caught_strict and not business_accept:
        return "B"
    if (not system_caught_strict) and business_accept:
        return "C"
    return "D"


def _review_one(
    *,
    review_family: str,
    business_family: str,
    material: MaterialSpanORM,
    article: Any,
    pipeline: MaterialPipelineV2,
) -> ReviewRecord:
    source_type = str(material.span_type or "")
    source_text = str(material.text or "")
    runtime_item = pipeline.build_cached_item_from_material(
        material=material,
        article=article,
        business_family_id=business_family,
        enable_fill_formalization_bridge=(business_family == "sentence_fill"),
    )
    system_caught = runtime_item is not None
    runtime_candidate_type = str((runtime_item or {}).get("candidate_type") or "")
    formal_hit = runtime_candidate_type in FORMAL_TYPES[review_family]
    system_caught_strict = system_caught and formal_hit
    rebuild_none = not system_caught
    selected = dict((runtime_item or {}).get("selected_task_scoring") or {})
    final_score = _safe_float(selected.get("final_candidate_score"))
    readiness_score = _safe_float(selected.get("readiness_score"))
    has_selected_task_scoring = bool(selected)

    if review_family == "center_understanding":
        structural, business, business_accept, issues, potential = _center_evaluate(
            runtime_item=runtime_item,
            source_text=source_text,
            formal_hit=formal_hit,
        )
    elif review_family == "sentence_order":
        structural, business, business_accept, issues, potential = _order_evaluate(
            runtime_item=runtime_item,
            source_text=source_text,
            formal_hit=formal_hit,
        )
    else:
        structural, business, business_accept, issues, potential = _fill_evaluate(
            runtime_item=runtime_item,
            source_text=source_text,
            source_type=source_type,
            formal_hit=formal_hit,
        )

    cause = _root_cause(
        system_caught_strict=system_caught_strict,
        business_level=business,
        formal_hit=formal_hit,
        issues=issues,
        potential_when_rejected=potential,
    )
    label = _abcd(system_caught_strict=system_caught_strict, business_accept=business_accept)
    text_clip = _clip((runtime_item or {}).get("text") or source_text, limit=150)
    return ReviewRecord(
        review_family=review_family,
        business_family=business_family,
        material_id=str(material.id),
        article_id=str(material.article_id),
        source_candidate_type=source_type,
        runtime_candidate_type=runtime_candidate_type,
        system_caught=system_caught,
        system_caught_strict=system_caught_strict,
        rebuild_none=rebuild_none,
        formal_unit_hit=formal_hit,
        has_selected_task_scoring=has_selected_task_scoring,
        final_score=round(final_score, 4),
        readiness_score=round(readiness_score, 4),
        structural_level=structural,
        business_level=business,
        business_accept=business_accept,
        root_cause=cause,
        issues=issues,
        potential_when_rejected=potential,
        a_b_c_d=label,
        text_clip=text_clip,
    )


def _pick_samples(records: list[ReviewRecord], sample_each: int) -> dict[str, list[dict[str, Any]]]:
    good = [item for item in records if item.system_caught_strict and item.business_level == "usable"]
    if not good:
        good = [item for item in records if item.system_caught and item.business_level == "usable"]
    good.sort(key=lambda item: item.final_score, reverse=True)

    boundary = [
        item
        for item in records
        if item.system_caught_strict and item.business_level in {"borderline", "unusable"}
    ]
    if not boundary:
        boundary = [item for item in records if item.system_caught and item.business_level == "borderline"]
    boundary.sort(key=lambda item: (item.business_level, -item.final_score))
    selected_ids = {item.material_id for item in good[:sample_each]} | {item.material_id for item in boundary[:sample_each]}

    rejected = [
        item
        for item in records
        if (not item.system_caught_strict) and (item.potential_when_rejected or item.business_level in {"usable", "borderline"}) and item.material_id not in selected_ids
    ]
    rejected.sort(key=lambda item: (0 if item.business_level == "usable" else 1, -item.final_score))

    def _to_dict(item: ReviewRecord) -> dict[str, Any]:
        return {
            "material_id": item.material_id,
            "article_id": item.article_id,
            "source_candidate_type": item.source_candidate_type,
            "runtime_candidate_type": item.runtime_candidate_type,
            "system_caught": item.system_caught,
            "system_caught_strict": item.system_caught_strict,
            "formal_unit_hit": item.formal_unit_hit,
            "selected_task_scoring": item.has_selected_task_scoring,
            "final_score": item.final_score,
            "structural_level": item.structural_level,
            "business_level": item.business_level,
            "business_accept": item.business_accept,
            "a_b_c_d": item.a_b_c_d,
            "root_cause": item.root_cause,
            "issues": list(item.issues),
            "potential_when_rejected": item.potential_when_rejected,
            "text_clip": item.text_clip,
        }

    return {
        "system_good": [_to_dict(item) for item in good[:sample_each]],
        "system_boundary": [_to_dict(item) for item in boundary[:sample_each]],
        "rejected_but_potential": [_to_dict(item) for item in rejected[:sample_each]],
    }


def _family_summary(records: list[ReviewRecord]) -> dict[str, Any]:
    total = len(records)
    count = Counter()
    issue_counter = Counter()
    cause_counter = Counter()
    for item in records:
        count["system_caught"] += 1 if item.system_caught else 0
        count["system_caught_strict"] += 1 if item.system_caught_strict else 0
        count["rebuild_none"] += 1 if item.rebuild_none else 0
        count["formal_unit_hit"] += 1 if item.formal_unit_hit else 0
        count["business_usable"] += 1 if item.business_level == "usable" else 0
        count["business_borderline"] += 1 if item.business_level == "borderline" else 0
        count["business_unusable"] += 1 if item.business_level == "unusable" else 0
        count[f"class_{item.a_b_c_d}"] += 1
        cause_counter[item.root_cause] += 1
        for issue in item.issues:
            issue_counter[issue] += 1

    return {
        "total": total,
        "system_caught": int(count["system_caught"]),
        "system_caught_strict": int(count["system_caught_strict"]),
        "rebuild_none": int(count["rebuild_none"]),
        "formal_unit_hit": int(count["formal_unit_hit"]),
        "business_usable": int(count["business_usable"]),
        "business_borderline": int(count["business_borderline"]),
        "business_unusable": int(count["business_unusable"]),
        "a_b_c_d": {
            "A": int(count["class_A"]),
            "B": int(count["class_B"]),
            "C": int(count["class_C"]),
            "D": int(count["class_D"]),
        },
        "root_cause_top": [[key, value] for key, value in cause_counter.most_common(8)],
        "issue_top": [[key, value] for key, value in issue_counter.most_common(12)],
    }


def _build_rubric() -> dict[str, Any]:
    return {
        "layer_1_system_capture": {
            "definition": "看系统是否接住：是否rebuild_none、是否进入formal unit、是否有selected_task_scoring。",
            "fields": ["system_caught", "system_caught_strict", "rebuild_none", "formal_unit_hit", "has_selected_task_scoring"],
        },
        "layer_2_structure_correctness": {
            "definition": "看结构是否真符合主卡formal unit，不只看表面像。",
            "levels": {
                "pass": "结构信号完整，formal unit与主卡目标一致。",
                "borderline": "有部分结构信号，但存在明显弱项或歧义。",
                "fail": "不满足主卡formal unit核心结构。",
            },
        },
        "layer_3_business_usability": {
            "definition": "看业务是否愿意拿去出题。",
            "levels": {
                "usable": "真可用，可直接进入出题链。",
                "borderline": "勉强可用，需要业务二次加工。",
                "unusable": "不建议出题，属于系统自嗨或原料弱。",
            },
        },
        "abcd_definition": {
            "A": "系统接住且业务认可。",
            "B": "系统接住但业务不认可。",
            "C": "系统没接住但业务认可。",
            "D": "系统没接住且业务不认可。",
        },
    }


def run(*, sample_each: int, max_items_per_family: int | None) -> dict[str, Any]:
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
        by_family: dict[str, list[MaterialSpanORM]] = defaultdict(list)
        for material in materials:
            fams = set(material.v2_business_family_ids or [])
            payload = material.v2_index_payload or {}
            for review_family, business_family in FAMILY_MAP.items():
                if business_family in fams and isinstance(payload.get(business_family), dict):
                    by_family[review_family].append(material)

        all_reports: dict[str, Any] = {}
        for review_family, business_family in FAMILY_MAP.items():
            rows = sorted(by_family.get(review_family, []), key=lambda item: str(item.id))
            if max_items_per_family is not None:
                rows = rows[: max(1, int(max_items_per_family))]
            records: list[ReviewRecord] = []
            for material in rows:
                article_id = str(material.article_id)
                if article_id not in article_cache:
                    article_cache[article_id] = article_repo.get(article_id)
                article = article_cache.get(article_id)
                if article is None:
                    continue
                records.append(
                    _review_one(
                        review_family=review_family,
                        business_family=business_family,
                        material=material,
                        article=article,
                        pipeline=pipeline,
                    )
                )

            summary = _family_summary(records)
            samples = _pick_samples(records, sample_each=max(1, int(sample_each)))
            all_reports[review_family] = {
                "summary": summary,
                "samples": samples,
            }

        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "rubric": _build_rubric(),
            "families": all_reports,
        }
    finally:
        session.close()


def _to_markdown(report: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# Business Usability Sampling Review")
    lines.append("")
    lines.append("## Rubric")
    rubric = report.get("rubric") or {}
    lines.append(f"- Layer 1: {(rubric.get('layer_1_system_capture') or {}).get('definition')}")
    lines.append(f"- Layer 2: {(rubric.get('layer_2_structure_correctness') or {}).get('definition')}")
    lines.append(f"- Layer 3: {(rubric.get('layer_3_business_usability') or {}).get('definition')}")
    lines.append("")
    for family, payload in (report.get("families") or {}).items():
        lines.append(f"## {family}")
        summary = payload.get("summary") or {}
        lines.append(f"- total: {summary.get('total', 0)}")
        lines.append(f"- system_caught_strict: {summary.get('system_caught_strict', 0)}")
        lines.append(f"- rebuild_none: {summary.get('rebuild_none', 0)}")
        lines.append(f"- business_usable: {summary.get('business_usable', 0)}")
        lines.append(f"- business_borderline: {summary.get('business_borderline', 0)}")
        lines.append(f"- business_unusable: {summary.get('business_unusable', 0)}")
        lines.append(f"- A/B/C/D: {summary.get('a_b_c_d', {})}")
        lines.append(f"- root_cause_top: {summary.get('root_cause_top', [])[:4]}")
        lines.append("")
        samples = payload.get("samples") or {}
        for bucket, items in samples.items():
            lines.append(f"### {bucket}")
            if not items:
                lines.append("- (none)")
                continue
            for item in items[:4]:
                lines.append(
                    "- "
                    + f"{item.get('material_id')} "
                    + f"src={item.get('source_candidate_type')} "
                    + f"runtime={item.get('runtime_candidate_type') or 'none'} "
                    + f"biz={item.get('business_level')} "
                    + f"class={item.get('a_b_c_d')} "
                    + f"cause={item.get('root_cause')}"
                )
                lines.append(f"  - issues: {item.get('issues')}")
                lines.append(f"  - text: {_clip(str(item.get('text_clip') or ''), 110)}")
        lines.append("")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Business usability oriented sampling review for three primary cards.")
    parser.add_argument("--sample-each", type=int, default=10, help="Sample count per bucket per family.")
    parser.add_argument("--max-items-per-family", type=int, default=None, help="Optional cap per family.")
    parser.add_argument("--output-json", type=Path, default=None, help="Output JSON file path.")
    parser.add_argument("--output-md", type=Path, default=None, help="Output markdown file path.")
    args = parser.parse_args()

    report = run(
        sample_each=max(1, int(args.sample_each)),
        max_items_per_family=args.max_items_per_family,
    )
    text = json.dumps(report, ensure_ascii=False, indent=2)
    if args.output_json:
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        args.output_json.write_text(text, encoding="utf-8")
    else:
        print(text)
    if args.output_md:
        args.output_md.parent.mkdir(parents=True, exist_ok=True)
        args.output_md.write_text(_to_markdown(report), encoding="utf-8")


if __name__ == "__main__":
    main()
