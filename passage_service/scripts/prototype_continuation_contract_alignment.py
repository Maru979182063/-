from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import select


PROJECT_ROOT = Path(__file__).resolve().parent.parent
os.chdir(PROJECT_ROOT)
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

from app.infra.db.orm.material_span import MaterialSpanORM  # noqa: E402
from app.infra.db.repositories.article_repo_sqlalchemy import SQLAlchemyArticleRepository  # noqa: E402
from app.infra.db.session import get_session, init_db  # noqa: E402
from app.services.material_pipeline_v2 import MaterialPipelineV2  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prototype continuation contract alignment against legacy landing.")
    parser.add_argument("--limit", type=int, default=240)
    parser.add_argument("--output-json", type=Path, required=True)
    parser.add_argument("--output-md", type=Path, required=True)
    return parser.parse_args()


def _selected_card(item: dict[str, Any] | None) -> str:
    if not item:
        return ""
    return str(((item.get("question_ready_context") or {}).get("selected_material_card")) or "")


def _candidate_type(item: dict[str, Any] | None) -> str:
    if not item:
        return ""
    return str(item.get("candidate_type") or "")


def _sample_row(
    material: MaterialSpanORM,
    article_title: str,
    baseline_item: dict[str, Any] | None,
    current_item: dict[str, Any] | None,
) -> dict[str, Any]:
    return {
        "article_id": str(material.article_id),
        "material_id": str(material.id),
        "title": article_title,
        "quality_score": float(material.quality_score or 0.0),
        "baseline_selected_material_card": _selected_card(baseline_item),
        "current_selected_material_card": _selected_card(current_item),
        "baseline_candidate_type": _candidate_type(baseline_item),
        "current_candidate_type": _candidate_type(current_item),
        "current_selected_business_card": str(((current_item or {}).get("question_ready_context") or {}).get("selected_business_card") or ""),
    }


def _report(limit: int) -> dict[str, Any]:
    init_db()
    session = get_session()
    article_repo = SQLAlchemyArticleRepository(session)
    try:
        materials = list(
            session.scalars(
                select(MaterialSpanORM)
                .where(
                    MaterialSpanORM.is_primary.is_(True),
                    MaterialSpanORM.v2_index_version.is_not(None),
                )
                .order_by(MaterialSpanORM.updated_at.desc())
                .limit(limit)
            )
        )
        continuation_materials = [
            material
            for material in materials
            if "continuation" in list(material.v2_business_family_ids or [])
        ]

        baseline_pipeline = MaterialPipelineV2()
        baseline_pipeline._continuation_contract_types = lambda candidate: set()  # type: ignore[method-assign]
        current_pipeline = MaterialPipelineV2()

        baseline_legacy = 0
        current_legacy = 0
        baseline_cards = Counter()
        current_cards = Counter()
        lifted_samples: list[dict[str, Any]] = []
        still_legacy_samples: list[dict[str, Any]] = []
        lifted_candidate_types = Counter()

        for material in continuation_materials:
            article = article_repo.get(material.article_id)
            if article is None:
                continue
            article_title = str(getattr(article, "title", "") or "")
            baseline_item = baseline_pipeline.build_cached_item_from_material(
                material=material,
                article=article,
                business_family_id="continuation",
            )
            current_item = current_pipeline.build_cached_item_from_material(
                material=material,
                article=article,
                business_family_id="continuation",
            )
            baseline_card = _selected_card(baseline_item)
            current_card = _selected_card(current_item)
            baseline_cards[baseline_card or "none"] += 1
            current_cards[current_card or "none"] += 1
            if baseline_card.startswith("legacy.continuation"):
                baseline_legacy += 1
            if current_card.startswith("legacy.continuation"):
                current_legacy += 1
            if baseline_card.startswith("legacy.continuation") and current_card and not current_card.startswith("legacy.continuation"):
                lifted_candidate_types[_candidate_type(current_item)] += 1
                if len(lifted_samples) < 20:
                    lifted_samples.append(_sample_row(material, article_title, baseline_item, current_item))
            if current_card.startswith("legacy.continuation") and len(still_legacy_samples) < 20:
                still_legacy_samples.append(_sample_row(material, article_title, baseline_item, current_item))

        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "limit": limit,
            "continuation_material_total": len(continuation_materials),
            "baseline_legacy_total": baseline_legacy,
            "current_legacy_total": current_legacy,
            "lifted_total": max(0, baseline_legacy - current_legacy),
            "baseline_selected_card_top": dict(baseline_cards.most_common(12)),
            "current_selected_card_top": dict(current_cards.most_common(12)),
            "lifted_candidate_type_top": dict(lifted_candidate_types.most_common(8)),
            "lifted_samples": lifted_samples,
            "still_legacy_samples": still_legacy_samples,
        }
    finally:
        session.close()


def _to_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Continuation Contract Alignment Prototype",
        "",
        f"- continuation_material_total: {report.get('continuation_material_total', 0)}",
        f"- baseline_legacy_total: {report.get('baseline_legacy_total', 0)}",
        f"- current_legacy_total: {report.get('current_legacy_total', 0)}",
        f"- lifted_total: {report.get('lifted_total', 0)}",
        "",
        "## Lifted Candidate Types",
    ]
    for key, value in (report.get("lifted_candidate_type_top") or {}).items():
        lines.append(f"- {key}: {value}")
    lines.extend(["", "## Lifted Samples"])
    for row in report.get("lifted_samples", [])[:12]:
        lines.append(
            f"- {row['material_id']} | q={row['quality_score']:.4f} | {row['baseline_candidate_type']} | {row['baseline_selected_material_card']} -> {row['current_selected_material_card']}"
        )
    lines.extend(["", "## Still Legacy Samples"])
    for row in report.get("still_legacy_samples", [])[:12]:
        lines.append(
            f"- {row['material_id']} | q={row['quality_score']:.4f} | {row['current_candidate_type']} | {row['current_selected_material_card']}"
        )
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    report = _report(limit=args.limit)
    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    args.output_json.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    args.output_md.write_text(_to_markdown(report), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
