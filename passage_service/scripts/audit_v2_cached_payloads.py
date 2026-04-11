from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter
from copy import deepcopy
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
from app.infra.db.repositories.material_span_repo_sqlalchemy import SQLAlchemyMaterialSpanRepository  # noqa: E402
from app.infra.db.session import get_session  # noqa: E402
from app.services.material_pipeline_v2 import MaterialPipelineV2  # noqa: E402


DEFAULT_FAMILIES = ("title_selection", "sentence_fill", "sentence_order")


def _bool_count(items: list[dict[str, Any]], predicate) -> int:
    return sum(1 for item in items if predicate(item))


def _top_counter(items: list[str], limit: int = 8) -> list[list[Any]]:
    return [[key, count] for key, count in Counter(items).most_common(limit)]


def _payload_summary(payload: dict[str, Any] | None) -> dict[str, Any]:
    payload = payload or {}
    qrc = payload.get("question_ready_context") or {}
    selected_task_scoring = payload.get("selected_task_scoring") or {}
    structure_scores = selected_task_scoring.get("structure_scores") or {}
    score_trace = selected_task_scoring.get("score_trace") or {}
    source_fields = score_trace.get("source_fields") or {}
    return {
        "candidate_type": payload.get("candidate_type"),
        "has_task_scoring": "task_scoring" in payload,
        "has_selected_task_scoring": "selected_task_scoring" in payload,
        "eligible_business_cards_len": len(payload.get("eligible_business_cards") or []),
        "business_card_recommendations_len": len(payload.get("business_card_recommendations") or []),
        "selected_business_card": qrc.get("selected_business_card"),
        "selected_material_card": qrc.get("selected_material_card"),
        "question_ready_context_keys": sorted(qrc.keys()),
        "prompt_extras_keys": sorted((qrc.get("prompt_extras") or {}).keys()),
        "selected_task_scoring_keys": sorted(selected_task_scoring.keys()),
        "selected_task_structure_keys": sorted(structure_scores.keys()),
        "selected_task_final_candidate_score": selected_task_scoring.get("final_candidate_score"),
        "selected_task_readiness_score": selected_task_scoring.get("readiness_score"),
        "slot_function": source_fields.get("slot_function"),
        "slot_role": source_fields.get("slot_role"),
        "blank_value_ready": source_fields.get("blank_value_ready"),
        "ordering_reason_trace_keys": sorted((source_fields.get("ordering_reason_trace") or {}).keys()) if isinstance(source_fields.get("ordering_reason_trace"), dict) else [],
    }


def _compare_payloads(old_payload: dict[str, Any] | None, rebuilt_payload: dict[str, Any] | None) -> dict[str, Any]:
    old_summary = _payload_summary(old_payload)
    rebuilt_summary = _payload_summary(rebuilt_payload)
    diff: dict[str, Any] = {
        "task_scoring_added": bool(not old_summary["has_task_scoring"] and rebuilt_summary["has_task_scoring"]),
        "selected_task_scoring_added": bool(not old_summary["has_selected_task_scoring"] and rebuilt_summary["has_selected_task_scoring"]),
        "candidate_type_changed": old_summary["candidate_type"] != rebuilt_summary["candidate_type"],
        "eligible_business_cards_delta": int(rebuilt_summary["eligible_business_cards_len"] or 0) - int(old_summary["eligible_business_cards_len"] or 0),
        "business_card_recommendations_delta": int(rebuilt_summary["business_card_recommendations_len"] or 0) - int(old_summary["business_card_recommendations_len"] or 0),
        "selected_business_card_changed": old_summary["selected_business_card"] != rebuilt_summary["selected_business_card"],
        "selected_material_card_changed": old_summary["selected_material_card"] != rebuilt_summary["selected_material_card"],
        "prompt_extras_keys_added": sorted(set(rebuilt_summary["prompt_extras_keys"]) - set(old_summary["prompt_extras_keys"])),
        "selected_task_structure_keys_added": sorted(set(rebuilt_summary["selected_task_structure_keys"]) - set(old_summary["selected_task_structure_keys"])),
        "question_ready_context_keys_added": sorted(set(rebuilt_summary["question_ready_context_keys"]) - set(old_summary["question_ready_context_keys"])),
    }
    return {
        "old": old_summary,
        "rebuilt": rebuilt_summary,
        "diff": diff,
    }


def _material_base_row(material: MaterialSpanORM) -> dict[str, Any]:
    return {
        "material_id": material.id,
        "article_id": material.article_id,
        "span_type": material.span_type,
        "quality_score": material.quality_score,
        "status": material.status,
        "release_channel": material.release_channel,
        "v2_index_version": material.v2_index_version,
        "v2_business_family_ids": list(material.v2_business_family_ids or []),
        "updated_at": material.updated_at.isoformat() if getattr(material, "updated_at", None) else None,
    }


def _family_items(
    *,
    materials: list[MaterialSpanORM],
    family: str,
) -> list[tuple[MaterialSpanORM, dict[str, Any]]]:
    results: list[tuple[MaterialSpanORM, dict[str, Any]]] = []
    for material in materials:
        payload = material.v2_index_payload or {}
        if (
            family in (material.v2_business_family_ids or [])
            and isinstance(payload, dict)
            and isinstance(payload.get(family), dict)
        ):
            results.append((material, payload[family]))
    return results


def _audit_family(items: list[tuple[MaterialSpanORM, dict[str, Any]]]) -> dict[str, Any]:
    payloads = [payload for _, payload in items]
    return {
        "total": len(items),
        "with_task_scoring": _bool_count(payloads, lambda item: "task_scoring" in item),
        "with_selected_task_scoring": _bool_count(payloads, lambda item: "selected_task_scoring" in item),
        "with_nonempty_eligible_business_cards": _bool_count(payloads, lambda item: bool(item.get("eligible_business_cards"))),
        "with_selected_business_card": _bool_count(payloads, lambda item: bool(((item.get("question_ready_context") or {}).get("selected_business_card")))),
        "with_selected_material_card": _bool_count(payloads, lambda item: bool(((item.get("question_ready_context") or {}).get("selected_material_card")))),
        "candidate_type_top": _top_counter([str(item.get("candidate_type") or "") for item in payloads]),
        "selected_business_card_top": _top_counter([str(((item.get("question_ready_context") or {}).get("selected_business_card")) or "") for item in payloads if str(((item.get("question_ready_context") or {}).get("selected_business_card")) or "").strip()]),
        "selected_material_card_top": _top_counter([str(((item.get("question_ready_context") or {}).get("selected_material_card")) or "") for item in payloads if str(((item.get("question_ready_context") or {}).get("selected_material_card")) or "").strip()]),
        "legacy_material_card_count": _bool_count(payloads, lambda item: str(((item.get("question_ready_context") or {}).get("selected_material_card")) or "").startswith("legacy.")),
    }


def _sample_items(
    items: list[tuple[MaterialSpanORM, dict[str, Any]]],
    *,
    sample_size: int,
) -> list[tuple[MaterialSpanORM, dict[str, Any]]]:
    ranked = sorted(
        items,
        key=lambda entry: (
            0 if "task_scoring" not in entry[1] else 1,
            0 if "selected_task_scoring" not in entry[1] else 1,
            -float(entry[0].quality_score or 0.0),
        ),
    )
    return ranked[:sample_size]


def _write_back_family_payload(
    *,
    material_repo: SQLAlchemyMaterialSpanRepository,
    material: MaterialSpanORM,
    family: str,
    rebuilt_payload: dict[str, Any],
) -> None:
    payload = deepcopy(material.v2_index_payload or {})
    payload[family] = rebuilt_payload
    families = set(material.v2_business_family_ids or [])
    families.add(family)
    material_repo.update_metrics(
        material.id,
        v2_index_version=MaterialPipelineV2.INDEX_VERSION,
        v2_business_family_ids=sorted(families),
        v2_index_payload=payload,
    )


def run_audit(
    *,
    families: list[str],
    sample_size: int,
    writeback: bool,
) -> dict[str, Any]:
    session = get_session()
    article_repo = SQLAlchemyArticleRepository(session)
    material_repo = SQLAlchemyMaterialSpanRepository(session)
    pipeline = MaterialPipelineV2()
    try:
        stmt = select(MaterialSpanORM).where(
            MaterialSpanORM.is_primary.is_(True),
            MaterialSpanORM.v2_index_version.is_not(None),
            MaterialSpanORM.status == "promoted",
            MaterialSpanORM.release_channel == "stable",
        )
        materials = list(session.scalars(stmt))
        report: dict[str, Any] = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "database_url": os.getenv("PASSAGE_DATABASE_URL", "sqlite:///./passage_service.db"),
            "index_version_current_code": pipeline.INDEX_VERSION,
            "totals": {
                "promoted_stable_primary_with_v2_index": len(materials),
                "v2_index_versions_top": _top_counter([str(item.v2_index_version or "") for item in materials], limit=10),
            },
            "families": {},
        }
        for family in families:
            family_rows = _family_items(materials=materials, family=family)
            family_report: dict[str, Any] = {
                "audit": _audit_family(family_rows),
                "samples": [],
            }
            for material, old_payload in _sample_items(family_rows, sample_size=sample_size):
                article = article_repo.get(material.article_id)
                rebuilt_payload = (
                    pipeline.build_cached_item_from_material(
                        material=material,
                        article=article,
                        business_family_id=family,
                    )
                    if article is not None
                    else None
                )
                if writeback and rebuilt_payload is not None:
                    _write_back_family_payload(
                        material_repo=material_repo,
                        material=material,
                        family=family,
                        rebuilt_payload=rebuilt_payload,
                    )
                family_report["samples"].append(
                    {
                        **_material_base_row(material),
                        "comparison": _compare_payloads(old_payload, rebuilt_payload),
                        "rebuilt_is_none": rebuilt_payload is None,
                    }
                )
            report["families"][family] = family_report
        return report
    finally:
        session.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit cached v2_index_payload completeness and dry-run rebuild deltas.")
    parser.add_argument("--families", nargs="+", default=list(DEFAULT_FAMILIES), help="Business families to audit.")
    parser.add_argument("--sample-size", type=int, default=5, help="Dry-run rebuild sample size per family.")
    parser.add_argument("--writeback", action="store_true", help="Write rebuilt payloads back for sampled items. Disabled by default.")
    parser.add_argument("--output", type=Path, default=None, help="Optional JSON output path.")
    args = parser.parse_args()

    result = run_audit(
        families=[str(item) for item in args.families],
        sample_size=max(1, int(args.sample_size)),
        writeback=bool(args.writeback),
    )

    text = json.dumps(result, ensure_ascii=False, indent=2)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text, encoding="utf-8")
    print(text)


if __name__ == "__main__":
    main()
