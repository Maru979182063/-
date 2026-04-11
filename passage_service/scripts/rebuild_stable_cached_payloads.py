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
from app.infra.db.session import get_session, init_db  # noqa: E402
from app.services.material_pipeline_v2 import MaterialPipelineV2  # noqa: E402


DEFAULT_FAMILIES = ("title_selection", "sentence_order")


def _normalized_families(raw: list[str]) -> list[str]:
    families: list[str] = []
    for item in raw:
        for token in str(item).split(","):
            value = token.strip()
            if value:
                families.append(value)
    return list(dict.fromkeys(families))


def _payload_has_complete_scoring(payload: dict[str, Any] | None) -> bool:
    payload = payload or {}
    return isinstance(payload.get("task_scoring"), dict) and isinstance(payload.get("selected_task_scoring"), dict)


def _comparison_summary(old_payload: dict[str, Any] | None, rebuilt_payload: dict[str, Any] | None) -> dict[str, Any]:
    old_payload = old_payload or {}
    rebuilt_payload = rebuilt_payload or {}
    old_qrc = old_payload.get("question_ready_context") or {}
    rebuilt_qrc = rebuilt_payload.get("question_ready_context") or {}
    return {
        "old_has_task_scoring": "task_scoring" in old_payload,
        "rebuilt_has_task_scoring": "task_scoring" in rebuilt_payload,
        "old_has_selected_task_scoring": "selected_task_scoring" in old_payload,
        "rebuilt_has_selected_task_scoring": "selected_task_scoring" in rebuilt_payload,
        "old_eligible_business_cards_len": len(old_payload.get("eligible_business_cards") or []),
        "rebuilt_eligible_business_cards_len": len(rebuilt_payload.get("eligible_business_cards") or []),
        "old_selected_business_card": old_qrc.get("selected_business_card"),
        "rebuilt_selected_business_card": rebuilt_qrc.get("selected_business_card"),
        "old_selected_material_card": old_qrc.get("selected_material_card"),
        "rebuilt_selected_material_card": rebuilt_qrc.get("selected_material_card"),
        "old_selected_task_scoring_keys": sorted((old_payload.get("selected_task_scoring") or {}).keys()),
        "rebuilt_selected_task_scoring_keys": sorted((rebuilt_payload.get("selected_task_scoring") or {}).keys()),
        "rebuilt_structure_score_keys": sorted(((rebuilt_payload.get("selected_task_scoring") or {}).get("structure_scores") or {}).keys()),
    }


def _iter_targets(
    *,
    session,
    families: list[str],
    status: str | None,
    release_channel: str | None,
    primary_only: bool,
) -> list[MaterialSpanORM]:
    stmt = select(MaterialSpanORM).where(MaterialSpanORM.v2_index_version.is_not(None))
    if primary_only:
        stmt = stmt.where(MaterialSpanORM.is_primary.is_(True))
    if status:
        stmt = stmt.where(MaterialSpanORM.status == status)
    if release_channel:
        stmt = stmt.where(MaterialSpanORM.release_channel == release_channel)
    materials = list(session.scalars(stmt))
    family_set = set(families)
    filtered = [
        item
        for item in materials
        if isinstance(item.v2_index_payload, dict) and family_set.intersection(set(item.v2_business_family_ids or []))
    ]
    filtered.sort(key=lambda item: (str(item.v2_index_version or ""), -float(item.quality_score or 0.0), str(item.updated_at or "")))
    return filtered


def run_rebuild(
    *,
    families: list[str],
    status: str | None,
    release_channel: str | None,
    primary_only: bool,
    only_missing_scoring: bool,
    limit: int | None,
    write_back: bool,
) -> dict[str, Any]:
    init_db()
    session = get_session()
    article_repo = SQLAlchemyArticleRepository(session)
    material_repo = SQLAlchemyMaterialSpanRepository(session)
    pipeline = MaterialPipelineV2()
    try:
        materials = _iter_targets(
            session=session,
            families=families,
            status=status,
            release_channel=release_channel,
            primary_only=primary_only,
        )
        report: dict[str, Any] = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "mode": "write_back" if write_back else "dry_run",
            "index_version_current_code": pipeline.INDEX_VERSION,
            "families": families,
            "filters": {
                "status": status,
                "release_channel": release_channel,
                "primary_only": primary_only,
                "only_missing_scoring": only_missing_scoring,
                "limit": limit,
            },
            "scan": {
                "candidate_materials": len(materials),
            },
            "counts": Counter(),
            "by_family": {family: Counter() for family in families},
            "touched_records": [],
        }
        processed = 0
        for material in materials:
            if limit is not None and processed >= limit:
                break
            base_payload = deepcopy(material.v2_index_payload or {})
            article = None
            changed_any = False
            touched_entry = {
                "material_id": material.id,
                "article_id": material.article_id,
                "v2_index_version_before": material.v2_index_version,
                "families": [],
            }
            for family in families:
                old_payload = base_payload.get(family)
                if not isinstance(old_payload, dict):
                    continue
                report["counts"]["family_payload_seen"] += 1
                report["by_family"][family]["family_payload_seen"] += 1
                if only_missing_scoring and _payload_has_complete_scoring(old_payload):
                    report["counts"]["skipped_complete_scoring"] += 1
                    report["by_family"][family]["skipped_complete_scoring"] += 1
                    continue
                if article is None:
                    article = article_repo.get(material.article_id)
                if article is None:
                    report["counts"]["skipped_missing_article"] += 1
                    report["by_family"][family]["skipped_missing_article"] += 1
                    continue
                rebuilt = pipeline.build_cached_item_from_material(
                    material=material,
                    article=article,
                    business_family_id=family,
                )
                if rebuilt is None:
                    report["counts"]["rebuilt_none"] += 1
                    report["by_family"][family]["rebuilt_none"] += 1
                    touched_entry["families"].append(
                        {
                            "family": family,
                            "status": "rebuilt_none",
                            "comparison": _comparison_summary(old_payload, None),
                        }
                    )
                    continue
                report["counts"]["rebuilt_nonnull"] += 1
                report["by_family"][family]["rebuilt_nonnull"] += 1
                comparison = _comparison_summary(old_payload, rebuilt)
                if (
                    json.dumps(old_payload, ensure_ascii=False, sort_keys=True)
                    != json.dumps(rebuilt, ensure_ascii=False, sort_keys=True)
                ):
                    changed_any = True
                    base_payload[family] = rebuilt
                    report["counts"]["payload_changed"] += 1
                    report["by_family"][family]["payload_changed"] += 1
                else:
                    report["counts"]["payload_unchanged"] += 1
                    report["by_family"][family]["payload_unchanged"] += 1
                touched_entry["families"].append(
                    {
                        "family": family,
                        "status": "rebuilt",
                        "comparison": comparison,
                    }
                )
            if not touched_entry["families"]:
                continue
            processed += 1
            if write_back and changed_any:
                merged_families = set(material.v2_business_family_ids or [])
                merged_families.update(base_payload.keys())
                material_repo.update_metrics(
                    material.id,
                    v2_index_version=pipeline.INDEX_VERSION,
                    v2_business_family_ids=sorted(merged_families),
                    v2_index_payload=base_payload,
                )
                report["counts"]["records_written"] += 1
                for fam in [item["family"] for item in touched_entry["families"] if item["status"] == "rebuilt"]:
                    report["by_family"][fam]["records_written"] += 1
                touched_entry["write_back"] = True
                touched_entry["v2_index_version_after"] = pipeline.INDEX_VERSION
            else:
                touched_entry["write_back"] = False
                touched_entry["v2_index_version_after"] = material.v2_index_version
            report["touched_records"].append(touched_entry)
        report["counts"] = dict(report["counts"])
        report["by_family"] = {family: dict(counter) for family, counter in report["by_family"].items()}
        report["scan"]["processed_records"] = processed
        report["scan"]["touched_records"] = len(report["touched_records"])
        return report
    finally:
        session.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Controlled rebuild for stable cached family payloads.")
    parser.add_argument("--families", nargs="+", default=list(DEFAULT_FAMILIES), help="Family ids to rebuild.")
    parser.add_argument("--status", default="promoted", help="Optional status filter.")
    parser.add_argument("--release-channel", default="stable", help="Optional release channel filter.")
    parser.add_argument("--primary-only", action="store_true", default=True, help="Only process primary materials.")
    parser.add_argument("--all-materials", action="store_true", help="Disable primary-only filter.")
    parser.add_argument("--only-missing-scoring", action="store_true", help="Only rebuild payloads missing task_scoring or selected_task_scoring.")
    parser.add_argument("--limit", type=int, default=None, help="Optional processing limit.")
    parser.add_argument("--write-back", action="store_true", help="Persist rebuilt payloads.")
    parser.add_argument("--report-path", type=Path, default=None, help="Optional JSON output path.")
    args = parser.parse_args()

    families = _normalized_families(args.families)
    report = run_rebuild(
        families=families,
        status=args.status or None,
        release_channel=args.release_channel or None,
        primary_only=not args.all_materials,
        only_missing_scoring=bool(args.only_missing_scoring),
        limit=args.limit,
        write_back=bool(args.write_back),
    )
    text = json.dumps(report, ensure_ascii=False, indent=2)
    if args.report_path:
        args.report_path.parent.mkdir(parents=True, exist_ok=True)
        args.report_path.write_text(text, encoding="utf-8")
    print(text)


if __name__ == "__main__":
    main()
