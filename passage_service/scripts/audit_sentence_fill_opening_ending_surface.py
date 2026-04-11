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


FAMILY_ID = "sentence_fill"
TARGET_FUNCTIONS = {
    ("opening", "topic_intro"),
    ("opening", "summary"),
    ("ending", "countermeasure"),
    ("ending", "ending_summary"),
}


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


def _topic_intro_pattern(text: str, right_context: str) -> str:
    digit_density = len(re.findall(r"\d|[%％]", text)) / max(1, len(re.sub(r"\s+", "", text)))
    comma_count = text.count("，") + text.count(",")
    intro_markers = ("当前", "如今", "近年来", "在此背景下", "面对", "随着", "放眼", "当下")
    problem_markers = ("问题", "挑战", "困境", "矛盾", "痛点", "关键", "核心", "如何", "为何", "亟需", "必须")
    question_markers = ("如何", "为何", "怎么办", "关键在于", "这意味着", "这说明")
    summary_markers = ("总之", "可见", "由此", "这意味着", "这说明")
    has_intro = any(marker in text for marker in intro_markers)
    has_problem = any(marker in text for marker in problem_markers)
    has_question = any(marker in text for marker in question_markers)
    has_summary = any(marker in text for marker in summary_markers)
    if has_summary:
        return "should_be_opening_summary"
    if digit_density >= 0.08 or comma_count >= 5:
        return "data_or_list_opening"
    if has_intro and not (has_problem or has_question):
        return "macro_background_opening"
    if not right_context.strip():
        return "missing_forward_expansion"
    return "ordinary_theme_opening"


def _countermeasure_pattern(text: str, left_context: str) -> str:
    action_markers = ("通过", "采取", "推动", "完善", "优化", "健全", "构建", "打造", "提供", "推出", "实施", "建立", "强化", "服务", "机制", "举措", "政策")
    modal_markers = ("应当", "应该", "要", "需要", "必须", "建议")
    policy_markers = ("会议", "报告", "规划", "意见", "提出", "强调", "部署", "要求", "指出")
    summary_markers = ("总之", "可见", "由此", "这启示我们", "这说明")
    value_markers = ("重要", "关键", "意义", "值得", "必须", "亟需")
    action_hits = sum(1 for marker in action_markers if marker in text)
    modal_hits = sum(1 for marker in modal_markers if marker in text)
    policy_hits = sum(1 for marker in policy_markers if marker in text)
    value_hits = sum(1 for marker in value_markers if marker in text)
    has_summary = any(marker in text for marker in summary_markers)
    if has_summary and action_hits == 0:
        return "should_be_ending_summary"
    if policy_hits >= 2 and action_hits <= 1:
        return "policy_statement_tail"
    if modal_hits > 0 and action_hits == 0:
        return "attitude_or_slogan_tail"
    if value_hits >= 2 and action_hits == 0:
        return "value_judgement_tail"
    if not left_context.strip():
        return "missing_backward_support"
    return "weak_countermeasure_surface_fit"


def run() -> dict[str, Any]:
    session = get_session()
    repo = SQLAlchemyArticleRepository(session)
    pipeline = MaterialPipelineV2()
    article_cache: dict[str, Any] = {}
    rows: list[dict[str, Any]] = []
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
            if FAMILY_ID not in family_ids or not isinstance(payload.get(FAMILY_ID), dict):
                continue
            article_id = str(material.article_id)
            if article_id not in article_cache:
                article_cache[article_id] = repo.get(article_id)
            article = article_cache.get(article_id)
            if article is None:
                continue
            runtime_item = pipeline.build_cached_item_from_material(
                material=material,
                article=article,
                business_family_id=FAMILY_ID,
                enable_fill_formalization_bridge=True,
            )
            meta = dict((runtime_item or {}).get("meta") or {})
            role = str(meta.get("slot_role") or "")
            function = str(meta.get("slot_function") or "")
            if (role, function) not in TARGET_FUNCTIONS:
                continue
            formal_hit = bool(runtime_item and runtime_item.get("candidate_type") == "functional_slot_unit")
            if not formal_hit:
                continue
            source_text = str(material.text or "")
            source_type = str(material.span_type or "")
            _, business_level, business_accept, issues, _ = _fill_evaluate(
                runtime_item=runtime_item,
                source_text=source_text,
                source_type=source_type,
                formal_hit=formal_hit,
            )
            label = _abcd(system_caught_strict=True, business_accept=business_accept)
            slot_text = str(meta.get("slot_sentence_text") or (runtime_item or {}).get("text") or "")
            left_context = str(meta.get("left_context_text") or "")
            right_context = str(meta.get("right_context_text") or "")
            pattern = (
                _topic_intro_pattern(slot_text, right_context)
                if (role, function) == ("opening", "topic_intro")
                else _countermeasure_pattern(slot_text, left_context)
                if (role, function) == ("ending", "countermeasure")
                else ""
            )
            rows.append(
                {
                    "material_id": str(material.id),
                    "article_id": article_id,
                    "slot_role": role,
                    "slot_function": function,
                    "business_level": business_level,
                    "class_label": label,
                    "pattern": pattern,
                    "final_score": round(
                        _safe_float(((runtime_item or {}).get("selected_task_scoring") or {}).get("final_candidate_score")),
                        4,
                    ),
                    "issues": list(issues),
                    "text_clip": _clip(slot_text, 140),
                }
            )
    finally:
        session.close()

    grouped: dict[str, Any] = {}
    for role, function in TARGET_FUNCTIONS:
        items = [row for row in rows if row["slot_role"] == role and row["slot_function"] == function]
        patterns = Counter(row["pattern"] for row in items if row["pattern"])
        grouped[f"{role}/{function}"] = {
            "strict_hits": len(items),
            "usable": sum(1 for row in items if row["business_level"] == "usable"),
            "borderline": sum(1 for row in items if row["business_level"] == "borderline"),
            "unusable": sum(1 for row in items if row["business_level"] == "unusable"),
            "b_count": sum(1 for row in items if row["class_label"] == "B"),
            "pattern_rows": [
                {"pattern": pattern, "count": count}
                for pattern, count in patterns.most_common()
            ],
            "samples": {
                pattern: [
                    {
                        "material_id": row["material_id"],
                        "final_score": row["final_score"],
                        "business_level": row["business_level"],
                        "class_label": row["class_label"],
                        "issues": row["issues"],
                        "text_clip": row["text_clip"],
                    }
                    for row in items
                    if row["pattern"] == pattern
                ][:3]
                for pattern, _ in patterns.most_common(4)
            },
        }
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "focus": "sentence_fill opening/topic_intro and ending/countermeasure surface-role audit",
        "groups": grouped,
    }


def _to_markdown(report: dict[str, Any]) -> str:
    lines = ["# Sentence Fill Opening/Ending Surface Audit", ""]
    for key, payload in (report.get("groups") or {}).items():
        lines.append(f"## {key}")
        lines.append(
            f"- strict/usable/borderline/unusable/B: {payload.get('strict_hits', 0)} / {payload.get('usable', 0)} / {payload.get('borderline', 0)} / {payload.get('unusable', 0)} / {payload.get('b_count', 0)}"
        )
        lines.append(f"- pattern_rows: {payload.get('pattern_rows', [])}")
        samples = payload.get("samples") or {}
        for pattern, items in samples.items():
            lines.append(f"### {pattern}")
            for item in items:
                lines.append(
                    f"- {item.get('material_id')} score={item.get('final_score')} biz={item.get('business_level')} class={item.get('class_label')} issues={item.get('issues')}"
                )
                lines.append(f"  - text: {item.get('text_clip')}")
        lines.append("")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Focused audit for sentence_fill opening/ending role-surface conflicts.")
    parser.add_argument("--output-json", type=Path, default=None)
    parser.add_argument("--output-md", type=Path, default=None)
    args = parser.parse_args()
    report = run()
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
