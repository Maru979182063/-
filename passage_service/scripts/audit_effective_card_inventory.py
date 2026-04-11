from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import select


PROJECT_ROOT = Path(__file__).resolve().parent.parent
os.chdir(PROJECT_ROOT)
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.infra.db.orm.article import ArticleORM  # noqa: E402
from app.infra.db.orm.material_span import MaterialSpanORM  # noqa: E402
from app.infra.db.session import get_session  # noqa: E402
from app.services.card_registry_v2 import CardRegistryV2  # noqa: E402
from app.services.material_pipeline_v2 import MaterialPipelineV2  # noqa: E402


FAMILY_ORDER = ("title_selection", "continuation", "sentence_fill", "sentence_order")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit effective inventory by material/business card ids.")
    parser.add_argument("--target-per-card", type=int, default=20, help="Target effective inventory per card id.")
    parser.add_argument("--output-json", type=Path, required=True)
    parser.add_argument("--output-md", type=Path, required=True)
    return parser.parse_args()


def _all_material_card_ids(registry: CardRegistryV2) -> dict[str, list[str]]:
    result: dict[str, list[str]] = {}
    for family in FAMILY_ORDER:
        cards = [str(card.get("card_id") or "") for card in registry.get_material_cards(family) if str(card.get("card_id") or "")]
        result[family] = cards
    return result


def _all_business_card_ids(registry: CardRegistryV2) -> dict[str, list[str]]:
    grouped: dict[str, list[str]] = defaultdict(list)
    for business_card_id, payload in sorted(registry.payload.get("business_cards", {}).items()):
        meta = payload.get("card_meta") or {}
        mother_family_id = str(meta.get("mother_family_id") or "").strip()
        runtime_family = "title_selection" if mother_family_id == "main_idea" else mother_family_id
        if runtime_family in FAMILY_ORDER:
            grouped[runtime_family].append(str(business_card_id))
    return dict(grouped)


def _family_runtime_item(
    pipeline: MaterialPipelineV2,
    *,
    material: MaterialSpanORM,
    article: ArticleORM,
    family: str,
) -> dict[str, Any] | None:
    return pipeline.build_cached_item_from_material(
        material=material,
        article=article,
        business_family_id=family,
        enable_fill_formalization_bridge=(family == "sentence_fill"),
        enable_sentence_order_weak_formal_bridge=(family == "sentence_order"),
        enable_sentence_order_weak_formal_gate=(family == "sentence_order"),
        enable_sentence_order_weak_formal_closing_gate=(family == "sentence_order"),
    )


def _inventory_report(target_per_card: int) -> dict[str, Any]:
    session = get_session()
    registry = CardRegistryV2()
    pipeline = MaterialPipelineV2()
    article_cache: dict[str, ArticleORM | None] = {}

    material_card_ids_by_family = _all_material_card_ids(registry)
    business_card_ids_by_family = _all_business_card_ids(registry)

    try:
        stable_materials = list(
            session.scalars(
                select(MaterialSpanORM).where(
                    MaterialSpanORM.status == "promoted",
                    MaterialSpanORM.release_channel == "stable",
                    MaterialSpanORM.is_primary.is_(True),
                    MaterialSpanORM.v2_index_version.is_not(None),
                )
            )
        )

        material_counts: Counter[str] = Counter()
        business_counts: Counter[str] = Counter()
        question_counts: Counter[str] = Counter()
        family_effective_counts: Counter[str] = Counter()
        supporting_materials: dict[str, list[str]] = defaultdict(list)
        supporting_business: dict[str, list[str]] = defaultdict(list)

        for material in stable_materials:
            families = [str(fam) for fam in (material.v2_business_family_ids or []) if str(fam) in FAMILY_ORDER]
            if not families:
                continue

            article_id = str(material.article_id)
            if article_id not in article_cache:
                article_cache[article_id] = session.get(ArticleORM, article_id)
            article = article_cache.get(article_id)
            if article is None:
                continue

            for family in families:
                item = _family_runtime_item(pipeline, material=material, article=article, family=family)
                if not item:
                    continue
                qctx = dict(item.get("question_ready_context") or {})
                selected_material_card = str(qctx.get("selected_material_card") or "")
                if not selected_material_card:
                    continue
                selected_business_card = str(qctx.get("selected_business_card") or "")
                selected_question_card = str(qctx.get("question_card_id") or "")

                material_counts[selected_material_card] += 1
                family_effective_counts[family] += 1
                if len(supporting_materials[selected_material_card]) < 20:
                    supporting_materials[selected_material_card].append(str(material.id))
                if selected_question_card:
                    question_counts[selected_question_card] += 1
                if selected_business_card:
                    business_counts[selected_business_card] += 1
                    if len(supporting_business[selected_business_card]) < 20:
                        supporting_business[selected_business_card].append(str(material.id))

        material_gap_rows: list[dict[str, Any]] = []
        business_gap_rows: list[dict[str, Any]] = []
        for family in FAMILY_ORDER:
            for card_id in material_card_ids_by_family.get(family, []):
                current = int(material_counts.get(card_id, 0))
                material_gap_rows.append(
                    {
                        "family": family,
                        "card_type": "material_card",
                        "card_id": card_id,
                        "effective_count": current,
                        "target": target_per_card,
                        "gap": max(0, target_per_card - current),
                        "sample_material_ids": supporting_materials.get(card_id, [])[:5],
                    }
                )
            for card_id in business_card_ids_by_family.get(family, []):
                current = int(business_counts.get(card_id, 0))
                business_gap_rows.append(
                    {
                        "family": family,
                        "card_type": "business_card",
                        "card_id": card_id,
                        "effective_count": current,
                        "target": target_per_card,
                        "gap": max(0, target_per_card - current),
                        "sample_material_ids": supporting_business.get(card_id, [])[:5],
                    }
                )

        material_gap_rows.sort(key=lambda row: (-row["gap"], row["family"], row["card_id"]))
        business_gap_rows.sort(key=lambda row: (-row["gap"], row["family"], row["card_id"]))

        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "target_per_card": target_per_card,
            "stable_primary_material_total": len(stable_materials),
            "family_effective_counts": dict(family_effective_counts),
            "question_card_effective_counts": dict(question_counts),
            "material_card_inventory": material_gap_rows,
            "business_card_inventory": business_gap_rows,
            "material_gap_summary": {
                "total_cards": len(material_gap_rows),
                "cards_below_target": sum(1 for row in material_gap_rows if row["gap"] > 0),
                "worst_gap": max((row["gap"] for row in material_gap_rows), default=0),
            },
            "business_gap_summary": {
                "total_cards": len(business_gap_rows),
                "cards_below_target": sum(1 for row in business_gap_rows if row["gap"] > 0),
                "worst_gap": max((row["gap"] for row in business_gap_rows), default=0),
            },
        }
    finally:
        session.close()


def _to_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Effective Card Inventory Audit",
        "",
        f"- target_per_card: {report.get('target_per_card', 0)}",
        f"- stable_primary_material_total: {report.get('stable_primary_material_total', 0)}",
        f"- family_effective_counts: {report.get('family_effective_counts', {})}",
        f"- question_card_effective_counts: {report.get('question_card_effective_counts', {})}",
        "",
        "## Material Card Gaps",
    ]
    for row in report.get("material_card_inventory", [])[:20]:
        lines.append(f"- {row['card_id']}: effective={row['effective_count']} gap={row['gap']}")
    lines.extend(["", "## Business Card Gaps"])
    for row in report.get("business_card_inventory", [])[:20]:
        lines.append(f"- {row['card_id']}: effective={row['effective_count']} gap={row['gap']}")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    report = _inventory_report(target_per_card=args.target_per_card)
    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    args.output_json.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    args.output_md.write_text(_to_markdown(report), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
