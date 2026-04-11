from __future__ import annotations

import argparse
import json
import os
import re
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
from app.services.material_pipeline_v2 import ACTION_MEASURE_MARKERS, MaterialPipelineV2  # noqa: E402


FAMILY = "sentence_fill"
SOURCE_SCOPE = {
    "sentence_group",
    "multi_paragraph_unit",
    "paragraph_window",
    "sentence_block_group",
}
OPENING_INTRO_MARKERS = ("当前", "如今", "近年来", "在此背景下", "面对", "随着", "放眼", "当下")
OPENING_PROBLEM_MARKERS = ("问题", "挑战", "困境", "矛盾", "痛点", "关键", "核心", "如何", "为何", "亟需", "必须")
POLICY_WORDS = ("会议", "报告", "规划", "意见", "提出", "强调", "部署", "要求", "指出")
SUMMARY_WORDS = ("总之", "可见", "由此", "综上", "这说明", "这表明")
TRANSITION_WORDS = ("因此", "由此", "同时", "此外", "接下来", "进一步", "从而", "这也")


def _clip(text: str, limit: int = 120) -> str:
    normalized = " ".join(str(text or "").replace("\xa0", " ").split())
    if len(normalized) <= limit:
        return normalized
    return f"{normalized[:limit]}..."


def _theme_overlap_ratio(pipeline: MaterialPipelineV2, left: str, right: str) -> float:
    left_words = set(pipeline._theme_words(left, None))
    right_words = set(pipeline._theme_words(right, None))
    if not left_words or not right_words:
        return 0.0
    return len(left_words & right_words) / max(1, min(len(left_words), len(right_words)))


def _contains_digits(text: str) -> bool:
    return bool(re.search(r"\d|[%％]", text))


def _digit_density(text: str) -> float:
    stripped = re.sub(r"\s+", "", str(text or ""))
    if not stripped:
        return 0.0
    digit_count = sum(1 for ch in stripped if ch.isdigit() or ch in {"%", "％"})
    return digit_count / len(stripped)


def _count_hits(text: str, markers: tuple[str, ...]) -> int:
    return sum(1 for marker in markers if marker in text)


def _middle_potential_count(pipeline: MaterialPipelineV2, text: str) -> int:
    sentences = [s.strip() for s in pipeline.sentence_splitter.split(text) if s.strip()]
    if len(sentences) < 3:
        return 0
    count = 0
    for idx in range(1, len(sentences) - 1):
        slot_text = sentences[idx]
        slot_function = pipeline._infer_functional_slot_function(
            slot_role="middle",
            slot_text=slot_text,
            context_text="".join(sentences[max(0, idx - 1) : min(len(sentences), idx + 2)]),
            context_before=sentences[idx - 1],
            context_after=sentences[idx + 1],
        )
        if slot_function in {"carry_previous", "lead_next", "bridge_both_sides"}:
            count += 1
    return count


def _topic_intro_misjudge_mode(
    *,
    pipeline: MaterialPipelineV2,
    slot_text: str,
    right_context_text: str,
) -> str | None:
    intro_hits = _count_hits(slot_text, OPENING_INTRO_MARKERS)
    problem_hits = _count_hits(slot_text, OPENING_PROBLEM_MARKERS)
    data_opening = _contains_digits(slot_text) and intro_hits == 0 and problem_hits == 0
    detail_heavy = len(slot_text) >= 96 and (_digit_density(slot_text) >= 0.06 or slot_text.count("，") >= 4)
    overlap = _theme_overlap_ratio(pipeline, slot_text, right_context_text)
    weak_expand = not right_context_text or overlap < 0.28
    transition_like = any(marker in slot_text for marker in TRANSITION_WORDS)
    if data_opening:
        return "data_opening_statement"
    if detail_heavy:
        return "detail_heavy_opening"
    if weak_expand and intro_hits == 0 and problem_hits == 0 and not transition_like:
        return "background_without_clear_gap"
    return None


def _countermeasure_misjudge_mode(
    *,
    slot_text: str,
    left_context_text: str,
) -> str | None:
    has_modal = any(marker in slot_text for marker in ("应当", "应该", "要", "需要", "必须", "建议"))
    action_hits = _count_hits(slot_text, ACTION_MEASURE_MARKERS)
    policy_hits = _count_hits(slot_text, POLICY_WORDS)
    summary_hits = _count_hits(slot_text, SUMMARY_WORDS)
    problem_context = _count_hits(left_context_text, OPENING_PROBLEM_MARKERS) > 0
    if has_modal and action_hits == 0:
        return "modal_without_action"
    if policy_hits >= 2 and action_hits <= 1 and not problem_context:
        return "policy_statement_miscast"
    if summary_hits > 0 and action_hits == 0:
        return "ending_summary_miscast_as_countermeasure"
    return None


def run(*, max_items: int | None, sample_limit: int) -> dict[str, Any]:
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
        all_rows = list(session.scalars(stmt))
        fill_rows = []
        for row in all_rows:
            payload = row.v2_index_payload or {}
            if FAMILY not in set(row.v2_business_family_ids or []):
                continue
            if not isinstance(payload.get(FAMILY), dict):
                continue
            fill_rows.append(row)
        fill_rows.sort(key=lambda item: str(item.id))
        if max_items is not None:
            fill_rows = fill_rows[: max(1, int(max_items))]

        totals = Counter()
        role_dist = Counter()
        func_dist = Counter()
        misjudge_patterns = Counter()
        misjudge_samples: dict[str, list[dict[str, Any]]] = defaultdict(list)
        middle_diag = Counter()
        middle_samples: dict[str, list[dict[str, Any]]] = defaultdict(list)

        for material in fill_rows:
            totals["total"] += 1
            source_type = str(material.span_type or "")
            in_source_scope = source_type in SOURCE_SCOPE
            if in_source_scope:
                totals["source_scope_total"] += 1

            article_id = str(material.article_id)
            if article_id not in article_cache:
                article_cache[article_id] = article_repo.get(article_id)
            article = article_cache.get(article_id)
            if article is None:
                totals["missing_article"] += 1
                continue

            item = pipeline.build_cached_item_from_material(
                material=material,
                article=article,
                business_family_id=FAMILY,
                enable_fill_formalization_bridge=True,
            )
            if item is None:
                totals["rebuild_none"] += 1
                if in_source_scope:
                    middle_diag["rebuild_none_in_source_scope"] += 1
                continue

            totals["bridged_non_none"] += 1
            candidate_type = str(item.get("candidate_type") or "")
            if candidate_type == "functional_slot_unit":
                totals["functional_slot_unit"] += 1
            meta = dict(item.get("meta") or {})
            role = str(meta.get("slot_role") or "")
            function = str(meta.get("slot_function") or "")
            role_dist[role] += 1
            func_dist[function] += 1

            slot_text = str(meta.get("slot_sentence_text") or item.get("text") or "")
            left_context = str(meta.get("left_context_text") or "")
            right_context = str(meta.get("right_context_text") or "")

            if role == "opening" and function == "topic_intro":
                totals["opening_topic_intro"] += 1
                mode = _topic_intro_misjudge_mode(
                    pipeline=pipeline,
                    slot_text=slot_text,
                    right_context_text=right_context,
                )
                if mode:
                    misjudge_patterns[mode] += 1
                    if len(misjudge_samples[mode]) < sample_limit:
                        misjudge_samples[mode].append(
                            {
                                "material_id": str(material.id),
                                "source_candidate_type": source_type,
                                "slot_text": _clip(slot_text, 180),
                                "right_context_text": _clip(right_context, 140),
                            }
                        )
            if role == "ending" and function == "countermeasure":
                totals["ending_countermeasure"] += 1
                mode = _countermeasure_misjudge_mode(
                    slot_text=slot_text,
                    left_context_text=left_context,
                )
                if mode:
                    misjudge_patterns[mode] += 1
                    if len(misjudge_samples[mode]) < sample_limit:
                        misjudge_samples[mode].append(
                            {
                                "material_id": str(material.id),
                                "source_candidate_type": source_type,
                                "slot_text": _clip(slot_text, 180),
                                "left_context_text": _clip(left_context, 140),
                            }
                        )

            if in_source_scope:
                sentence_count = max(1, int(material.sentence_count or 1))
                if sentence_count < 3:
                    middle_diag["short_span_no_middle_slot"] += 1
                    if len(middle_samples["short_span_no_middle_slot"]) < sample_limit:
                        middle_samples["short_span_no_middle_slot"].append(
                            {
                                "material_id": str(material.id),
                                "source_candidate_type": source_type,
                                "sentence_count": sentence_count,
                                "slot_role": role,
                                "slot_function": function,
                            }
                        )
                else:
                    potential = _middle_potential_count(pipeline, str(material.text or ""))
                    if potential == 0:
                        middle_diag["no_middle_signal_in_text"] += 1
                    else:
                        middle_diag["has_middle_signal_in_text"] += 1
                        if role in {"opening", "ending"}:
                            middle_diag["middle_signal_shadowed_by_edge_role"] += 1
                            if len(middle_samples["middle_signal_shadowed_by_edge_role"]) < sample_limit:
                                middle_samples["middle_signal_shadowed_by_edge_role"].append(
                                    {
                                        "material_id": str(material.id),
                                        "source_candidate_type": source_type,
                                        "selected_slot_role": role,
                                        "selected_slot_function": function,
                                        "middle_signal_count": potential,
                                    }
                                )
                        elif role == "middle":
                            middle_diag["middle_signal_selected_as_middle"] += 1

        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "family": FAMILY,
            "totals": dict(totals),
            "role_distribution": dict(role_dist),
            "function_distribution": dict(func_dist),
            "misjudge_patterns": dict(misjudge_patterns),
            "misjudge_samples": dict(misjudge_samples),
            "middle_diagnostics": dict(middle_diag),
            "middle_samples": dict(middle_samples),
        }
    finally:
        session.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit sentence_fill slot role/function purity and misjudge patterns.")
    parser.add_argument("--max-items", type=int, default=None, help="Optional cap for fill materials.")
    parser.add_argument("--sample-limit", type=int, default=5, help="Max samples per pattern.")
    parser.add_argument("--output", type=Path, default=None, help="Output JSON path.")
    args = parser.parse_args()
    report = run(max_items=args.max_items, sample_limit=max(1, int(args.sample_limit)))
    text = json.dumps(report, ensure_ascii=False, indent=2)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text, encoding="utf-8")
    else:
        print(text)


if __name__ == "__main__":
    main()
