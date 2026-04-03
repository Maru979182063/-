from __future__ import annotations

import copy
import json
import os
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PASSAGE_SERVICE_ROOT = ROOT / "passage_service"
REPORTS_ROOT = ROOT / "reports"

os.chdir(PASSAGE_SERVICE_ROOT)
if str(PASSAGE_SERVICE_ROOT) not in sys.path:
    sys.path.insert(0, str(PASSAGE_SERVICE_ROOT))
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))

import backfill_effective_business_cards as base  # noqa: E402

from app.domain.services.ingest_service import run_crawl_for_source  # noqa: E402
from app.domain.services.material_pipeline_v2_service import MaterialPipelineV2Service  # noqa: E402
from app.infra.db.orm.article import ArticleORM  # noqa: E402
from app.infra.db.orm.material_span import MaterialSpanORM  # noqa: E402
from app.infra.db.repositories.utils import new_id  # noqa: E402
from app.infra.db.session import get_session, init_db  # noqa: E402
from app.infra.ingest.dedupe.content_hash import build_content_hash  # noqa: E402
from app.infra.plugins.loader import load_plugins  # noqa: E402


def latest_article_ids(session, *, source_id: str | None = None, limit: int = 220) -> list[str]:
    query = session.query(ArticleORM.id).order_by(ArticleORM.created_at.desc())
    if source_id:
        query = query.filter(ArticleORM.source == source_id)
    return [row[0] for row in query.limit(limit).all()]


def material_payload_from_search_item(*, family: str, item: dict, selected_business_card: str) -> dict:
    payload = copy.deepcopy(item)
    qrc = dict(payload.get("question_ready_context") or {})
    qrc["selected_business_card"] = selected_business_card
    payload["question_ready_context"] = qrc
    payload.setdefault("source", {})
    payload["source"]["effective_backfill"] = True
    payload["source"]["effective_backfill_version"] = base.VERSION
    text = str(payload.get("text") or "")
    return {
        "id": new_id("mat"),
        "article_id": payload.get("article_id"),
        "candidate_span_id": payload.get("candidate_id"),
        "text": text,
        "normalized_text_hash": build_content_hash(text),
        "material_family_id": f"v2_effective_backfill.{family}",
        "is_primary": True,
        "span_type": payload.get("candidate_type"),
        "length_bucket": base.length_bucket(text),
        "paragraph_count": base.paragraph_count(text),
        "sentence_count": base.sentence_count(text),
        "status": "gray",
        "release_channel": "gray",
        "gray_ratio": 0.0,
        "gray_reason": None,
        "segmentation_version": base.VERSION,
        "tag_version": base.VERSION,
        "fit_version": base.VERSION,
        "prompt_version": base.VERSION,
        "primary_family": f"V2有效补量:{family}",
        "primary_subtype": selected_business_card,
        "secondary_subtypes": [card_id for card_id in (payload.get("business_card_recommendations") or []) if card_id and card_id != selected_business_card],
        "universal_profile": dict(payload.get("neutral_signal_profile") or {}),
        "family_scores": {},
        "capability_scores": {},
        "parallel_families": [],
        "structure_features": dict(payload.get("article_profile") or {}),
        "family_profiles": {},
        "subtype_candidates": [],
        "secondary_candidates": [],
        "candidate_labels": list(dict.fromkeys([selected_business_card, *(payload.get("business_card_recommendations") or [])])),
        "primary_label": payload.get("material_card_id"),
        "decision_trace": {
            "source": "effective_search_backfill",
            "family": family,
            "selected_business_card": selected_business_card,
            "selected_material_card": qrc.get("selected_material_card"),
        },
        "primary_route": {
            "business_family_id": family,
            "selected_business_card": selected_business_card,
        },
        "reject_reason": None,
        "variants": [],
        "source": dict(payload.get("source") or {}),
        "source_tail": str((payload.get("source") or {}).get("source_url") or ""),
        "integrity": {
            "semantic_completeness_score": base.normalize_quality((payload.get("business_feature_profile") or {}).get("semantic_completeness_score")),
            "standalone_readability": base.normalize_quality((payload.get("business_feature_profile") or {}).get("readability")),
        },
        "quality_flags": list(payload.get("quality_flags") or []),
        "knowledge_tags": list(dict.fromkeys(["v2_effective_backfill_search", family, selected_business_card, str(payload.get("material_card_id") or "")])),
        "fit_scores": {
            hit.get("card_id"): hit.get("score")
            for hit in (payload.get("eligible_material_cards") or [])
            if hit.get("card_id")
        },
        "feature_profile": dict(payload.get("business_feature_profile") or {}),
        "quality_score": base.normalize_quality(payload.get("quality_score")),
        "v2_index_version": base.VERSION,
        "v2_business_family_ids": [family],
        "v2_index_payload": {family: payload},
        "usage_count": 0,
        "accept_count": 0,
        "reject_count": 0,
        "last_used_at": None,
    }


def collect_candidates_for_family(session, *, family: str, article_ids: list[str], candidate_limit: int = 800) -> list[dict]:
    if not article_ids:
        return []
    response = MaterialPipelineV2Service(session).search(
        {
            "business_family_id": family,
            "article_ids": article_ids,
            "candidate_limit": candidate_limit,
            "min_card_score": 0.40,
            "min_business_card_score": 0.20,
        }
    )
    return response.get("items") or []


def insert_effective_items(session, *, family: str, items: list[dict], deficits: dict[str, dict[str, int]], existing_pairs: set[tuple[str, str, str]], hashes_by_family_card: dict[tuple[str, str], set[str]]) -> tuple[Counter, list[dict]]:
    inserted = Counter()
    samples: list[dict] = []
    family_deficits = deficits[family]
    for item in items:
        if all(value <= 0 for value in family_deficits.values()):
            break
        text = str(item.get("text") or "").strip()
        if not text:
            continue
        candidate_id = str(item.get("candidate_id") or "")
        if not candidate_id:
            continue
        text_hash = build_content_hash(text)
        candidate_cards = list(dict.fromkeys([
            str(((item.get("question_ready_context") or {}).get("selected_business_card")) or ""),
            *(item.get("business_card_recommendations") or []),
        ]))
        for card_id in candidate_cards:
            if not card_id or family_deficits.get(card_id, 0) <= 0:
                continue
            if (candidate_id, family, card_id) in existing_pairs:
                continue
            if text_hash in hashes_by_family_card.get((family, card_id), set()):
                continue
            ok, _ = base.is_effective_cached_item(
                family=family,
                card_id=card_id,
                cached_item=item,
                exact_selected=False,
            )
            if not ok:
                continue
            material = MaterialSpanORM(**material_payload_from_search_item(family=family, item=item, selected_business_card=card_id))
            session.add(material)
            existing_pairs.add((candidate_id, family, card_id))
            hashes_by_family_card.setdefault((family, card_id), set()).add(text_hash)
            family_deficits[card_id] -= 1
            inserted[card_id] += 1
            if len(samples) < 120:
                samples.append(
                    {
                        "family": family,
                        "card_id": card_id,
                        "material_id": material.id,
                        "article_title": item.get("article_title"),
                        "source_id": (item.get("source") or {}).get("source_id") or (item.get("source") or {}).get("source_name"),
                        "quality_score": base.normalize_quality(item.get("quality_score")),
                    }
                )
            break
    session.commit()
    return inserted, samples


def topup_round(session, deficits, existing_pairs, hashes_by_family_card, *, source_id: str | None = None) -> tuple[dict[str, Counter], list[dict]]:
    inserted_total = {family: Counter() for family in base.TARGETS}
    samples_total: list[dict] = []
    article_ids = latest_article_ids(session, source_id=source_id, limit=260 if source_id else 400)
    for family in base.TARGETS:
        if all(value <= 0 for value in deficits[family].values()):
            continue
        items = collect_candidates_for_family(session, family=family, article_ids=article_ids)
        inserted, samples = insert_effective_items(
            session,
            family=family,
            items=items,
            deficits=deficits,
            existing_pairs=existing_pairs,
            hashes_by_family_card=hashes_by_family_card,
        )
        inserted_total[family].update(inserted)
        samples_total.extend(samples)
    return inserted_total, samples_total


def main() -> int:
    init_db()
    load_plugins()
    session = get_session()
    try:
        baseline_articles = base.count_articles()
        baseline_materials = base.count_materials()
        baseline_counts, existing_pairs, hashes_by_family_card = base.count_effective_existing(session)
        deficits = base.deficits_from(baseline_counts)

        inserted_total = {family: Counter() for family in base.TARGETS}
        samples_total: list[dict] = []
        crawl_results: list[dict] = []

        inserted, samples = topup_round(session, deficits, existing_pairs, hashes_by_family_card, source_id=None)
        for family, counter in inserted.items():
            inserted_total[family].update(counter)
        samples_total.extend(samples)

        for source_id in base.SOURCE_PLAN:
            if all(all(value <= 0 for value in family_deficits.values()) for family_deficits in deficits.values()):
                break
            crawl_result = run_crawl_for_source(session, source_id)
            crawl_results.append(crawl_result)
            inserted, samples = topup_round(session, deficits, existing_pairs, hashes_by_family_card, source_id=source_id)
            for family, counter in inserted.items():
                inserted_total[family].update(counter)
            samples_total.extend(samples)

        final_counts, _, _ = base.count_effective_existing(session)
        final_deficits = base.deficits_from(final_counts)
        all_reached = all(all(value <= 0 for value in family_deficits.values()) for family_deficits in final_deficits.values())
        report = {
            "run_at": datetime.now().isoformat(timespec="seconds"),
            "version": base.VERSION,
            "baseline_articles": baseline_articles,
            "baseline_materials": baseline_materials,
            "baseline_effective_counts": base.compact(baseline_counts),
            "final_articles": base.count_articles(),
            "final_materials": base.count_materials(),
            "final_effective_counts": base.compact(final_counts),
            "final_deficits": final_deficits,
            "inserted_effective_counts": base.compact(inserted_total),
            "crawl_results": crawl_results,
            "all_cards_reached": all_reached,
            "samples": samples_total[:120],
        }

        REPORTS_ROOT.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        json_path = REPORTS_ROOT / f"effective_business_card_search_backfill_{timestamp}.json"
        md_path = REPORTS_ROOT / f"effective_business_card_search_backfill_{timestamp}.md"
        json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        md_path.write_text(base.build_markdown(report), encoding="utf-8")
        print(json.dumps({"json": str(json_path), "markdown": str(md_path), "all_cards_reached": all_reached}, ensure_ascii=False))
        return 0 if all_reached else 1
    finally:
        session.close()


if __name__ == "__main__":
    raise SystemExit(main())
