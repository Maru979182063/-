from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import select


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

from app.infra.db.orm.material_span import MaterialSpanORM  # noqa: E402
from app.infra.db.repositories.article_repo_sqlalchemy import SQLAlchemyArticleRepository  # noqa: E402
from app.infra.db.session import get_session  # noqa: E402
from app.services.material_pipeline_v2 import MaterialPipelineV2  # noqa: E402
from scripts.review_business_usable_rate import _abcd, _fill_evaluate  # noqa: E402


FILL_FORMAL_TYPE = "functional_slot_unit"
FILL_FAMILY_ID = "sentence_fill"
SOURCE_SCOPE = {"sentence_group", "multi_paragraph_unit", "paragraph_window", "sentence_block_group"}
MIDDLE_FUNCTIONS = {"carry_previous", "lead_next", "bridge_both_sides"}
SURFACE_ROLE_SET = {("opening", "topic_intro"), ("ending", "countermeasure")}
LARGE_SOURCE_TYPES = {"multi_paragraph_unit", "paragraph_window"}


@dataclass
class FillAuditRecord:
    material_id: str
    article_id: str
    source_candidate_type: str
    runtime_candidate_type: str
    strict_hit: bool
    business_level: str
    class_label: str
    final_score: float
    readiness_score: float
    issues: list[str]
    slot_role: str
    slot_function: str
    slot_bridge_action: str
    blank_value_ready: bool
    blank_value_reason: str
    slot_sentence_text: str
    left_context_text: str
    right_context_text: str
    source_sentence_count: int
    slot_sentence_count: int
    context_sentence_count: int
    primary_conflict_mode: str
    signal_blank_value_weak: bool
    signal_role_surface_fit: bool
    signal_hard_bridge_extraction: bool
    signal_context_dependency_conflict: bool
    source_text_clip: str


def _safe_float(value: Any) -> float:
    try:
        if value is None:
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _clip(text: str, limit: int = 120) -> str:
    normalized = " ".join(str(text or "").replace("\xa0", " ").split())
    if len(normalized) <= limit:
        return normalized
    return f"{normalized[:limit]}..."


def _parse_sentence_count(range_value: Any, fallback: int = 1) -> int:
    if not isinstance(range_value, list) or len(range_value) != 2:
        return max(1, int(fallback))
    try:
        start = int(range_value[0])
        end = int(range_value[1])
    except (TypeError, ValueError):
        return max(1, int(fallback))
    return max(1, end - start + 1)


def _signal_blank_value_weak(*, issues: list[str], final_score: float) -> bool:
    return ("fill_score_low" in issues) or ("fill_score_mid" in issues) or final_score < 0.40


def _signal_role_surface_fit(*, slot_role: str, slot_function: str, issues: list[str], final_score: float) -> bool:
    if "topic_intro_generic" in issues or "countermeasure_generic" in issues:
        return True
    return (slot_role, slot_function) in SURFACE_ROLE_SET and final_score < 0.40


def _signal_hard_bridge_extraction(
    *,
    source_candidate_type: str,
    source_sentence_count: int,
    slot_sentence_count: int,
    final_score: float,
) -> bool:
    return (
        source_candidate_type in LARGE_SOURCE_TYPES
        and source_sentence_count >= 7
        and slot_sentence_count <= 2
        and final_score < 0.40
    )


def _signal_context_dependency_conflict(
    *,
    slot_function: str,
    source_sentence_count: int,
    context_sentence_count: int,
    final_score: float,
) -> bool:
    return (
        slot_function in MIDDLE_FUNCTIONS
        and source_sentence_count >= 8
        and context_sentence_count <= 3
        and final_score < 0.30
    )


def _primary_conflict_mode(
    *,
    slot_role: str,
    slot_function: str,
    source_candidate_type: str,
    source_sentence_count: int,
    slot_sentence_count: int,
    context_sentence_count: int,
    issues: list[str],
    final_score: float,
) -> str:
    if _signal_context_dependency_conflict(
        slot_function=slot_function,
        source_sentence_count=source_sentence_count,
        context_sentence_count=context_sentence_count,
        final_score=final_score,
    ):
        return "context_dependency_conflict"
    if _signal_hard_bridge_extraction(
        source_candidate_type=source_candidate_type,
        source_sentence_count=source_sentence_count,
        slot_sentence_count=slot_sentence_count,
        final_score=final_score,
    ):
        return "hard_bridge_extraction"
    if _signal_role_surface_fit(
        slot_role=slot_role,
        slot_function=slot_function,
        issues=issues,
        final_score=final_score,
    ):
        return "role_surface_fit"
    return "blank_value_weak"


def _material_rows() -> list[MaterialSpanORM]:
    session = get_session()
    try:
        stmt = select(MaterialSpanORM).where(
            MaterialSpanORM.is_primary.is_(True),
            MaterialSpanORM.status == "promoted",
            MaterialSpanORM.release_channel == "stable",
            MaterialSpanORM.v2_index_version.is_not(None),
        )
        return list(session.scalars(stmt))
    finally:
        session.close()


def _collect_records() -> list[FillAuditRecord]:
    session = get_session()
    article_repo = SQLAlchemyArticleRepository(session)
    pipeline = MaterialPipelineV2()
    article_cache: dict[str, Any] = {}
    records: list[FillAuditRecord] = []
    try:
        stmt = select(MaterialSpanORM).where(
            MaterialSpanORM.is_primary.is_(True),
            MaterialSpanORM.status == "promoted",
            MaterialSpanORM.release_channel == "stable",
            MaterialSpanORM.v2_index_version.is_not(None),
        )
        materials = list(session.scalars(stmt))
        for material in materials:
            family_ids = set(material.v2_business_family_ids or [])
            payload = material.v2_index_payload or {}
            if FILL_FAMILY_ID not in family_ids:
                continue
            if not isinstance(payload.get(FILL_FAMILY_ID), dict):
                continue

            article_id = str(material.article_id)
            if article_id not in article_cache:
                article_cache[article_id] = article_repo.get(article_id)
            article = article_cache.get(article_id)
            if article is None:
                continue

            source_text = str(material.text or "")
            source_type = str(material.span_type or "")
            runtime_item = pipeline.build_cached_item_from_material(
                material=material,
                article=article,
                business_family_id=FILL_FAMILY_ID,
                enable_fill_formalization_bridge=True,
            )
            runtime_type = str((runtime_item or {}).get("candidate_type") or "")
            formal_hit = runtime_type == FILL_FORMAL_TYPE
            strict_hit = bool(runtime_item is not None and formal_hit)
            structural, business_level, business_accept, issues, _ = _fill_evaluate(
                runtime_item=runtime_item,
                source_text=source_text,
                source_type=source_type,
                formal_hit=formal_hit,
            )
            _ = structural
            class_label = _abcd(system_caught_strict=strict_hit, business_accept=business_accept)

            meta = dict((runtime_item or {}).get("meta") or {})
            scoring = dict((runtime_item or {}).get("selected_task_scoring") or {})
            final_score = _safe_float(scoring.get("final_candidate_score"))
            readiness_score = _safe_float(scoring.get("readiness_score"))

            slot_role = str(meta.get("slot_role") or "")
            slot_function = str(meta.get("slot_function") or "")
            slot_bridge_action = str(meta.get("slot_bridge_action") or "none")
            blank_value_ready = bool(meta.get("blank_value_ready"))
            blank_value_reason = str(meta.get("blank_value_reason") or "")
            slot_sentence_text = str(meta.get("slot_sentence_text") or (runtime_item or {}).get("text") or "")
            left_context_text = str(meta.get("left_context_text") or "")
            right_context_text = str(meta.get("right_context_text") or "")
            source_candidate_type = str(meta.get("slot_source_candidate_type") or source_type or "")
            source_sentence_count = max(1, int(getattr(material, "sentence_count", 1) or 1))
            slot_sentence_count = _parse_sentence_count(meta.get("slot_sentence_range"), fallback=1)
            context_sentence_count = _parse_sentence_count(meta.get("slot_context_sentence_range"), fallback=1)

            primary_mode = ""
            if class_label == "B":
                primary_mode = _primary_conflict_mode(
                    slot_role=slot_role,
                    slot_function=slot_function,
                    source_candidate_type=source_candidate_type,
                    source_sentence_count=source_sentence_count,
                    slot_sentence_count=slot_sentence_count,
                    context_sentence_count=context_sentence_count,
                    issues=issues,
                    final_score=final_score,
                )

            records.append(
                FillAuditRecord(
                    material_id=str(material.id),
                    article_id=article_id,
                    source_candidate_type=source_candidate_type,
                    runtime_candidate_type=runtime_type,
                    strict_hit=strict_hit,
                    business_level=business_level,
                    class_label=class_label,
                    final_score=round(final_score, 4),
                    readiness_score=round(readiness_score, 4),
                    issues=list(issues),
                    slot_role=slot_role,
                    slot_function=slot_function,
                    slot_bridge_action=slot_bridge_action,
                    blank_value_ready=blank_value_ready,
                    blank_value_reason=blank_value_reason,
                    slot_sentence_text=slot_sentence_text,
                    left_context_text=left_context_text,
                    right_context_text=right_context_text,
                    source_sentence_count=source_sentence_count,
                    slot_sentence_count=slot_sentence_count,
                    context_sentence_count=context_sentence_count,
                    primary_conflict_mode=primary_mode,
                    signal_blank_value_weak=_signal_blank_value_weak(issues=issues, final_score=final_score),
                    signal_role_surface_fit=_signal_role_surface_fit(
                        slot_role=slot_role,
                        slot_function=slot_function,
                        issues=issues,
                        final_score=final_score,
                    ),
                    signal_hard_bridge_extraction=_signal_hard_bridge_extraction(
                        source_candidate_type=source_candidate_type,
                        source_sentence_count=source_sentence_count,
                        slot_sentence_count=slot_sentence_count,
                        final_score=final_score,
                    ),
                    signal_context_dependency_conflict=_signal_context_dependency_conflict(
                        slot_function=slot_function,
                        source_sentence_count=source_sentence_count,
                        context_sentence_count=context_sentence_count,
                        final_score=final_score,
                    ),
                    source_text_clip=_clip(slot_sentence_text or source_text, limit=150),
                )
            )
    finally:
        session.close()
    return records


def _pct(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round(100.0 * numerator / denominator, 1)


def _top_modes(records: list[FillAuditRecord], *, topn: int = 2) -> list[list[Any]]:
    counter = Counter(
        record.primary_conflict_mode
        for record in records
        if record.class_label == "B" and record.primary_conflict_mode
    )
    return [[mode, count] for mode, count in counter.most_common(topn)]


def _sample_rows(records: list[FillAuditRecord], *, limit: int = 4) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for record in records[:limit]:
        rows.append(
            {
                "material_id": record.material_id,
                "article_id": record.article_id,
                "slot_role": record.slot_role,
                "slot_function": record.slot_function,
                "slot_bridge_action": record.slot_bridge_action,
                "source_candidate_type": record.source_candidate_type,
                "source_sentence_count": record.source_sentence_count,
                "slot_sentence_count": record.slot_sentence_count,
                "context_sentence_count": record.context_sentence_count,
                "blank_value_reason": record.blank_value_reason,
                "business_level": record.business_level,
                "class_label": record.class_label,
                "final_score": record.final_score,
                "issues": list(record.issues),
                "text_clip": record.source_text_clip,
            }
        )
    return rows


def _build_report(records: list[FillAuditRecord]) -> dict[str, Any]:
    total = len(records)
    strict_records = [record for record in records if record.strict_hit]
    strict_total = len(strict_records)
    class_counter = Counter(record.class_label for record in records)
    level_counter = Counter(record.business_level for record in records)
    b_records = [record for record in records if record.class_label == "B"]
    b_total = len(b_records)

    b_primary_counter = Counter(
        record.primary_conflict_mode for record in b_records if record.primary_conflict_mode
    )
    b_signal_counter = Counter(
        {
            "blank_value_weak_signal": sum(1 for record in b_records if record.signal_blank_value_weak),
            "role_surface_fit_signal": sum(1 for record in b_records if record.signal_role_surface_fit),
            "hard_bridge_extraction_signal": sum(1 for record in b_records if record.signal_hard_bridge_extraction),
            "context_dependency_conflict_signal": sum(1 for record in b_records if record.signal_context_dependency_conflict),
        }
    )

    b_by_mode_samples: dict[str, list[dict[str, Any]]] = {}
    for mode, _ in b_primary_counter.most_common():
        mode_records = sorted(
            [record for record in b_records if record.primary_conflict_mode == mode],
            key=lambda record: (record.final_score, record.material_id),
        )
        b_by_mode_samples[mode] = _sample_rows(mode_records, limit=4)

    role_groups: dict[tuple[str, str], list[FillAuditRecord]] = defaultdict(list)
    for record in strict_records:
        role_groups[(record.slot_role or "none", record.slot_function or "none")].append(record)
    role_rows: list[dict[str, Any]] = []
    for key, group in role_groups.items():
        usable = sum(1 for record in group if record.business_level == "usable")
        borderline = sum(1 for record in group if record.business_level == "borderline")
        unusable = sum(1 for record in group if record.business_level == "unusable")
        b_count = sum(1 for record in group if record.class_label == "B")
        role_rows.append(
            {
                "slot_role": key[0],
                "slot_function": key[1],
                "strict_hits": len(group),
                "usable": usable,
                "borderline": borderline,
                "unusable": unusable,
                "usable_ratio_pct": _pct(usable, len(group)),
                "b_count": b_count,
                "b_ratio_pct": _pct(b_count, len(group)),
                "top_conflict_modes": _top_modes(group, topn=3),
            }
        )
    role_rows.sort(key=lambda row: (-row["strict_hits"], -row["b_count"], row["slot_role"], row["slot_function"]))

    action_groups: dict[str, list[FillAuditRecord]] = defaultdict(list)
    for record in strict_records:
        action_groups[record.slot_bridge_action or "none"].append(record)
    action_rows: list[dict[str, Any]] = []
    for action, group in action_groups.items():
        usable = sum(1 for record in group if record.business_level == "usable")
        borderline = sum(1 for record in group if record.business_level == "borderline")
        unusable = sum(1 for record in group if record.business_level == "unusable")
        b_count = sum(1 for record in group if record.class_label == "B")
        total_hits = len(group)
        b_ratio = _pct(b_count, total_hits)
        usable_ratio = _pct(usable, total_hits)
        decision = "retain"
        if total_hits >= 20 and b_ratio >= 80.0:
            decision = "rewrite"
        elif b_ratio >= 60.0:
            decision = "tighten"
        action_rows.append(
            {
                "bridge_action": action,
                "strict_hits": total_hits,
                "usable": usable,
                "borderline": borderline,
                "unusable": unusable,
                "usable_ratio_pct": usable_ratio,
                "b_count": b_count,
                "b_ratio_pct": b_ratio,
                "high_fake_formal": bool(total_hits >= 20 and b_ratio >= 80.0),
                "recommendation": decision,
                "top_conflict_modes": _top_modes(group, topn=3),
            }
        )
    action_rows.sort(key=lambda row: (-row["strict_hits"], -row["b_count"], row["bridge_action"]))

    blank_groups: dict[str, list[FillAuditRecord]] = defaultdict(list)
    for record in strict_records:
        blank_groups[record.blank_value_reason or "none"].append(record)
    blank_rows: list[dict[str, Any]] = []
    for reason, group in blank_groups.items():
        usable = sum(1 for record in group if record.business_level == "usable")
        b_count = sum(1 for record in group if record.class_label == "B")
        blank_rows.append(
            {
                "blank_value_reason": reason,
                "strict_hits": len(group),
                "usable": usable,
                "b_count": b_count,
                "usable_ratio_pct": _pct(usable, len(group)),
                "b_ratio_pct": _pct(b_count, len(group)),
            }
        )
    blank_rows.sort(key=lambda row: (-row["strict_hits"], -row["b_count"], row["blank_value_reason"]))

    b_blank_ready = sum(1 for record in b_records if record.blank_value_ready)
    b_blank_gap = sum(
        1 for record in b_records if record.blank_value_ready and record.signal_blank_value_weak
    )

    high_hit_low_value_roles = [
        row
        for row in role_rows
        if row["strict_hits"] >= 20 and row["usable_ratio_pct"] <= 10.0
    ]
    low_hit_high_value_roles = [
        row
        for row in role_rows
        if row["strict_hits"] <= 20 and row["usable_ratio_pct"] >= 35.0 and row["usable"] >= 4
    ]

    summary = {
        "total": total,
        "strict_hits": strict_total,
        "business_usable": int(level_counter["usable"]),
        "business_borderline": int(level_counter["borderline"]),
        "business_unusable": int(level_counter["unusable"]),
        "a_b_c_d": {
            "A": int(class_counter["A"]),
            "B": int(class_counter["B"]),
            "C": int(class_counter["C"]),
            "D": int(class_counter["D"]),
        },
    }

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "scope": {
            "family": "sentence_fill",
            "strict_formal_type": FILL_FORMAL_TYPE,
            "source_scope": sorted(SOURCE_SCOPE),
            "goal": "reduce fake-formal functional_slot_unit and improve business usable rate",
        },
        "summary": summary,
        "b_conflict_portrait": {
            "b_total": b_total,
            "primary_conflict_modes": [
                {
                    "mode": mode,
                    "count": count,
                    "ratio_pct": _pct(count, b_total),
                }
                for mode, count in b_primary_counter.most_common()
            ],
            "signal_incidence": [
                {
                    "signal": signal,
                    "count": count,
                    "ratio_pct": _pct(count, b_total),
                }
                for signal, count in b_signal_counter.most_common()
            ],
            "top_issue_tokens": [
                [token, count]
                for token, count in Counter(
                    issue
                    for record in b_records
                    for issue in record.issues
                ).most_common(12)
            ],
            "mode_samples": b_by_mode_samples,
        },
        "role_function_value_layers": role_rows,
        "bridge_action_value": action_rows,
        "blank_value_business_gap": {
            "b_blank_value_ready": b_blank_ready,
            "b_blank_value_ready_ratio_pct": _pct(b_blank_ready, b_total),
            "b_passed_blank_gate_but_still_weak": b_blank_gap,
            "b_passed_blank_gate_but_still_weak_ratio_pct": _pct(b_blank_gap, b_total),
            "blank_reason_rows": blank_rows[:20],
        },
        "design_recommendation": {
            "high_hit_low_value_roles": high_hit_low_value_roles,
            "low_hit_high_value_roles": low_hit_high_value_roles,
            "priority_bridge_action_to_fix": next(
                (
                    row["bridge_action"]
                    for row in action_rows
                    if row["recommendation"] == "rewrite"
                ),
                action_rows[0]["bridge_action"] if action_rows else "none",
            ),
            "priority_strategy": "intersection_of_bridge_action_blank_value_gate_and_role_priority",
        },
    }


def _to_markdown(report: dict[str, Any]) -> str:
    summary = report.get("summary") or {}
    abcd = summary.get("a_b_c_d") or {}
    lines: list[str] = []
    lines.append("# Sentence Fill Bridge Conflict Audit")
    lines.append("")
    lines.append("## Summary")
    lines.append(f"- total: {summary.get('total', 0)}")
    lines.append(f"- strict_hits(functional_slot_unit): {summary.get('strict_hits', 0)}")
    lines.append(f"- business_usable/borderline/unusable: {summary.get('business_usable', 0)} / {summary.get('business_borderline', 0)} / {summary.get('business_unusable', 0)}")
    lines.append(f"- A/B/C/D: {abcd}")
    lines.append("")

    portrait = report.get("b_conflict_portrait") or {}
    lines.append("## B Conflict Portrait")
    lines.append(f"- B total: {portrait.get('b_total', 0)}")
    lines.append("- primary_conflict_modes:")
    for row in portrait.get("primary_conflict_modes") or []:
        lines.append(f"  - {row.get('mode')}: {row.get('count')} ({row.get('ratio_pct')}%)")
    lines.append("- signal_incidence:")
    for row in portrait.get("signal_incidence") or []:
        lines.append(f"  - {row.get('signal')}: {row.get('count')} ({row.get('ratio_pct')}%)")
    lines.append("- top_issue_tokens:")
    lines.append(f"  - {(portrait.get('top_issue_tokens') or [])[:8]}")
    lines.append("")

    lines.append("## Role/Function Value Layers")
    for row in (report.get("role_function_value_layers") or [])[:12]:
        lines.append(
            "- "
            + f"{row.get('slot_role')}/{row.get('slot_function')} "
            + f"strict={row.get('strict_hits')} usable={row.get('usable')} borderline={row.get('borderline')} unusable={row.get('unusable')} "
            + f"usable_ratio={row.get('usable_ratio_pct')}% B={row.get('b_count')}({row.get('b_ratio_pct')}%) "
            + f"conflicts={row.get('top_conflict_modes')}"
        )
    lines.append("")

    lines.append("## Bridge Action Value")
    for row in (report.get("bridge_action_value") or [])[:8]:
        lines.append(
            "- "
            + f"{row.get('bridge_action')} "
            + f"strict={row.get('strict_hits')} usable={row.get('usable')} B={row.get('b_count')} "
            + f"usable_ratio={row.get('usable_ratio_pct')}% B_ratio={row.get('b_ratio_pct')}% "
            + f"high_fake_formal={row.get('high_fake_formal')} recommendation={row.get('recommendation')} "
            + f"conflicts={row.get('top_conflict_modes')}"
        )
    lines.append("")

    gap = report.get("blank_value_business_gap") or {}
    lines.append("## Blank-Value Gap")
    lines.append(
        "- "
        + f"B blank_ready={gap.get('b_blank_value_ready')} ({gap.get('b_blank_value_ready_ratio_pct')}%), "
        + f"passed_gate_but_still_weak={gap.get('b_passed_blank_gate_but_still_weak')} ({gap.get('b_passed_blank_gate_but_still_weak_ratio_pct')}%)"
    )
    lines.append("- blank_reason_rows(top):")
    for row in (gap.get("blank_reason_rows") or [])[:10]:
        lines.append(
            "  - "
            + f"{row.get('blank_value_reason')} strict={row.get('strict_hits')} usable={row.get('usable')} B={row.get('b_count')} "
            + f"usable_ratio={row.get('usable_ratio_pct')}% B_ratio={row.get('b_ratio_pct')}%"
        )
    lines.append("")

    rec = report.get("design_recommendation") or {}
    lines.append("## Next-Cut Recommendation")
    lines.append(f"- priority_bridge_action_to_fix: {rec.get('priority_bridge_action_to_fix')}")
    lines.append(f"- priority_strategy: {rec.get('priority_strategy')}")
    lines.append(f"- high_hit_low_value_roles: {rec.get('high_hit_low_value_roles')}")
    lines.append(f"- low_hit_high_value_roles: {rec.get('low_hit_high_value_roles')}")
    lines.append("")

    mode_samples = (portrait.get("mode_samples") or {})
    lines.append("## Mode Samples")
    for mode, items in mode_samples.items():
        lines.append(f"### {mode}")
        if not items:
            lines.append("- (none)")
            continue
        for item in items[:3]:
            lines.append(
                "- "
                + f"{item.get('material_id')} role={item.get('slot_role')}/{item.get('slot_function')} "
                + f"action={item.get('slot_bridge_action')} score={item.get('final_score')} "
                + f"src={item.get('source_candidate_type')} issues={item.get('issues')}"
            )
            lines.append(f"  - text: {item.get('text_clip')}")
        lines.append("")
    return "\n".join(lines)


def run() -> dict[str, Any]:
    records = _collect_records()
    return _build_report(records)


def main() -> None:
    parser = argparse.ArgumentParser(description="Sentence-fill bridge conflict audit focused on fake-formal functional_slot_unit.")
    parser.add_argument("--output-json", type=Path, required=False, default=None, help="Output JSON path.")
    parser.add_argument("--output-md", type=Path, required=False, default=None, help="Output Markdown path.")
    args = parser.parse_args()

    report = run()
    report_text = json.dumps(report, ensure_ascii=False, indent=2)
    if args.output_json:
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        args.output_json.write_text(report_text, encoding="utf-8")
    else:
        print(report_text)
    if args.output_md:
        args.output_md.parent.mkdir(parents=True, exist_ok=True)
        args.output_md.write_text(_to_markdown(report), encoding="utf-8")


if __name__ == "__main__":
    main()
