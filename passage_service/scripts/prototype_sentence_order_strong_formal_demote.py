import argparse
import json
import os
import sys
from collections import Counter
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
from scripts.review_business_usable_rate import _abcd, _order_evaluate  # noqa: E402


FORMAL_TYPES = {"ordered_unit_group", "weak_formal_order_group"}


def _evaluate(runtime_item: dict[str, Any] | None, source_text: str) -> dict[str, Any]:
    system_caught = runtime_item is not None
    candidate_type = str((runtime_item or {}).get("candidate_type") or "")
    formal_hit = candidate_type in FORMAL_TYPES
    structural, business, business_accept, issues, potential = _order_evaluate(
        runtime_item=runtime_item,
        source_text=source_text,
        formal_hit=formal_hit,
    )
    system_caught_strict = system_caught and formal_hit
    return {
        "system_caught": system_caught,
        "system_caught_strict": system_caught_strict,
        "formal_hit": formal_hit,
        "candidate_type": candidate_type,
        "structural_level": structural,
        "business_level": business,
        "business_accept": business_accept,
        "issues": issues,
        "potential": potential,
        "label": _abcd(system_caught_strict=system_caught_strict, business_accept=business_accept),
    }


def _summarize(records: list[dict[str, Any]]) -> dict[str, Any]:
    counter = Counter()
    for item in records:
        counter[f"class_{item['label']}"] += 1
        counter["formal_hit"] += 1 if item["formal_hit"] else 0
        counter["business_usable"] += 1 if item["business_level"] == "usable" else 0
        counter["business_borderline"] += 1 if item["business_level"] == "borderline" else 0
        counter["business_unusable"] += 1 if item["business_level"] == "unusable" else 0
        counter["formal_usable"] += 1 if item["formal_hit"] and item["business_level"] == "usable" else 0
    formal_hit = int(counter["formal_hit"])
    formal_usable = int(counter["formal_usable"])
    return {
        "a_b_c_d": {
            "A": int(counter["class_A"]),
            "B": int(counter["class_B"]),
            "C": int(counter["class_C"]),
            "D": int(counter["class_D"]),
        },
        "formal_hit": formal_hit,
        "business_usable": int(counter["business_usable"]),
        "business_borderline": int(counter["business_borderline"]),
        "business_unusable": int(counter["business_unusable"]),
        "usable_ratio_within_formal": round(formal_usable / formal_hit, 4) if formal_hit else 0.0,
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
        rows = []
        for material in materials:
            fams = set(material.v2_business_family_ids or [])
            payload = material.v2_index_payload or {}
            if "sentence_order" in fams and isinstance(payload.get("sentence_order"), dict):
                rows.append(material)

        current_records: list[dict[str, Any]] = []
        prototype_records: list[dict[str, Any]] = []

        current_formal_unusable = Counter()
        prototype_formal_unusable = Counter()
        demoted_examples: list[dict[str, Any]] = []
        prototype_noise_examples: list[dict[str, Any]] = []
        retained_a_examples: list[dict[str, Any]] = []

        for material in sorted(rows, key=lambda item: str(item.id)):
            article_id = str(material.article_id)
            if article_id not in article_cache:
                article_cache[article_id] = article_repo.get(article_id)
            article = article_cache.get(article_id)
            if article is None:
                continue

            source_text = str(material.text or "")
            source_type = str(material.span_type or "")
            material_id = str(material.id)

            current_item = pipeline.build_cached_item_from_material(
                material=material,
                article=article,
                business_family_id="sentence_order",
                enable_sentence_order_weak_formal_bridge=True,
                enable_sentence_order_weak_formal_gate=True,
                enable_sentence_order_weak_formal_closing_gate=True,
                enable_sentence_order_strong_formal_demote=False,
            )
            prototype_item = pipeline.build_cached_item_from_material(
                material=material,
                article=article,
                business_family_id="sentence_order",
                enable_sentence_order_weak_formal_bridge=True,
                enable_sentence_order_weak_formal_gate=True,
                enable_sentence_order_weak_formal_closing_gate=True,
                enable_sentence_order_strong_formal_demote=True,
            )

            current_eval = _evaluate(current_item, source_text)
            prototype_eval = _evaluate(prototype_item, source_text)
            current_records.append({"material_id": material_id, **current_eval})
            prototype_records.append({"material_id": material_id, **prototype_eval})

            if current_eval["formal_hit"] and current_eval["business_level"] != "usable":
                current_formal_unusable[current_eval["candidate_type"]] += 1

            if prototype_eval["formal_hit"] and prototype_eval["business_level"] != "usable":
                prototype_formal_unusable[prototype_eval["candidate_type"]] += 1
                if len(prototype_noise_examples) < 8:
                    prototype_noise_examples.append(
                        {
                            "material_id": material_id,
                            "source_candidate_type": source_type,
                            "prototype_candidate_type": prototype_eval["candidate_type"],
                            "issues_after": prototype_eval["issues"],
                            "text_clip": source_text[:180],
                        }
                    )

            current_type = current_eval["candidate_type"]
            prototype_type = prototype_eval["candidate_type"]
            if current_type == "ordered_unit_group" and prototype_type == "weak_formal_order_group" and len(demoted_examples) < 10:
                prototype_meta = dict((prototype_item or {}).get("meta") or {})
                demoted_examples.append(
                    {
                        "material_id": material_id,
                        "source_candidate_type": source_type,
                        "before_type": current_type,
                        "after_type": prototype_type,
                        "business_level_before": current_eval["business_level"],
                        "business_level_after": prototype_eval["business_level"],
                        "demotion_meta": prototype_meta.get("strong_formal_demotion") or {},
                        "issues_after": prototype_eval["issues"],
                        "text_clip": source_text[:180],
                    }
                )

            if current_eval["label"] == "A" and prototype_eval["label"] == "A" and len(retained_a_examples) < 8:
                retained_a_examples.append(
                    {
                        "material_id": material_id,
                        "candidate_type": prototype_type,
                        "text_clip": source_text[:180],
                    }
                )

        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "summary": {
                "total": len(rows),
                "current_post_gate": _summarize(current_records),
                "strong_formal_demote_prototype": _summarize(prototype_records),
            },
            "current_formal_unusable_by_type": dict(current_formal_unusable),
            "prototype_formal_unusable_by_type": dict(prototype_formal_unusable),
            "demoted_examples": demoted_examples,
            "prototype_noise_examples": prototype_noise_examples,
            "retained_a_examples": retained_a_examples,
        }
    finally:
        session.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Prototype sentence_order strong formal demotion.")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    result = run()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
