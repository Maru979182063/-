from __future__ import annotations

import json
import os
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace


ROOT = Path(__file__).resolve().parents[1]
PASSAGE_SERVICE_ROOT = ROOT / "passage_service"
REPORTS_ROOT = ROOT / "reports"
VERSION = "v2.effective.fast_backfill.20260403a"

os.chdir(PASSAGE_SERVICE_ROOT)
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(PASSAGE_SERVICE_ROOT) not in sys.path:
    sys.path.insert(0, str(PASSAGE_SERVICE_ROOT))

from scripts.backfill_effective_business_cards import (  # noqa: E402
    SOURCE_PLAN,
    TARGETS,
    build_markdown,
    compact,
    count_articles,
    count_effective_existing,
    count_materials,
    deficits_from,
    is_effective_cached_item,
    material_payload,
)
from app.domain.services.ingest_service import run_crawl_for_source  # noqa: E402
from app.domain.services.material_pipeline_v2_service import MaterialPipelineV2Service  # noqa: E402
from app.infra.db.orm.article import ArticleORM  # noqa: E402
from app.infra.db.orm.candidate_span import CandidateSpanORM  # noqa: E402
from app.infra.db.orm.material_span import MaterialSpanORM  # noqa: E402
from app.infra.db.repositories.utils import new_id  # noqa: E402
from app.infra.db.session import get_session, init_db  # noqa: E402
from app.infra.ingest.dedupe.content_hash import build_content_hash  # noqa: E402
from app.infra.plugins.loader import load_plugins  # noqa: E402
from app.services.material_pipeline_v2 import MaterialPipelineV2  # noqa: E402


FAMILY_CONFIG = {
    "title_selection": {
        "batch_size": 18,
        "candidate_limit": 120,
        "min_card_score": 0.48,
        "min_business_card_score": 0.34,
        "source_order": ["科普中国", "光明网", "半月谈", "三联生活周刊", "人民网", "新华网", "经济日报", "求是网", "中国政府网", "中国青年报", "果壳"],
    },
    "sentence_fill": {
        "batch_size": 16,
        "candidate_limit": 140,
        "min_card_score": 0.44,
        "min_business_card_score": 0.30,
        "source_order": ["科普中国", "光明网", "半月谈", "人民网", "新华网", "中国青年报", "经济日报", "求是网", "中国政府网", "三联生活周刊", "果壳"],
    },
    "sentence_order": {
        "batch_size": 8,
        "candidate_limit": 160,
        "min_card_score": 0.40,
        "min_business_card_score": 0.28,
        "source_order": ["科普中国", "光明网", "果壳", "三联生活周刊", "半月谈", "人民网", "新华网", "中国青年报", "经济日报", "求是网", "中国政府网"],
    },
}


def _family_done(deficits: dict[str, dict[str, int]], family: str) -> bool:
    return all(value <= 0 for value in deficits[family].values())


def _all_done(deficits: dict[str, dict[str, int]]) -> bool:
    return all(_family_done(deficits, family) for family in deficits)


def _article_sort_key(article: ArticleORM, source_rank: dict[str, int]) -> tuple[int, str]:
    return (source_rank.get(str(article.source or ""), 999), str(article.created_at or ""))


def _ordered_article_ids(session, family: str, *, article_ids: list[str] | None = None) -> list[str]:
    cfg = FAMILY_CONFIG[family]
    source_rank = {name: index for index, name in enumerate(cfg["source_order"])}
    query = session.query(ArticleORM)
    if article_ids:
        query = query.filter(ArticleORM.id.in_(article_ids))
    articles = query.all()
    articles.sort(key=lambda item: _article_sort_key(item, source_rank), reverse=False)
    return [article.id for article in articles]


def _batched(items: list[str], size: int) -> list[list[str]]:
    return [items[index : index + size] for index in range(0, len(items), size)]


def _selected_or_recommended_cards(item: dict) -> list[str]:
    qrc = item.get("question_ready_context") or {}
    return list(
        dict.fromkeys(
            [
                str(qrc.get("selected_business_card") or ""),
                *(item.get("business_card_recommendations") or []),
            ]
        )
    )


def _candidate_key(article_id: str, text: str, span_type: str) -> tuple[str, str, str]:
    return (article_id, build_content_hash(text), span_type)


def _load_candidate_cache(session) -> dict[tuple[str, str, str], str]:
    cache: dict[tuple[str, str, str], str] = {}
    rows = session.query(CandidateSpanORM.id, CandidateSpanORM.article_id, CandidateSpanORM.span_type, CandidateSpanORM.text).all()
    for candidate_id, article_id, span_type, text in rows:
        if not text:
            continue
        cache[_candidate_key(article_id, text, span_type)] = candidate_id
    return cache


def _ensure_candidate_span(session, candidate_cache: dict[tuple[str, str, str], str], item: dict) -> str:
    article_id = str(item.get("article_id") or "")
    text = str(item.get("text") or "").strip()
    span_type = str(item.get("candidate_type") or "material_span")
    key = _candidate_key(article_id, text, span_type)
    existing = candidate_cache.get(key)
    if existing:
        return existing

    meta = item.get("meta") or {}
    paragraph_range = list(meta.get("source_paragraph_range_original") or meta.get("paragraph_range") or [0, 0])
    sentence_range = list(meta.get("source_sentence_range_original") or meta.get("sentence_range") or [None, None])
    start_paragraph = int(paragraph_range[0] or 0) if paragraph_range else 0
    end_paragraph = int(paragraph_range[-1] or start_paragraph) if paragraph_range else start_paragraph
    start_sentence = sentence_range[0] if sentence_range else None
    end_sentence = sentence_range[-1] if sentence_range else None
    candidate_span = CandidateSpanORM(
        id=new_id("cand"),
        article_id=article_id,
        start_paragraph=start_paragraph,
        end_paragraph=end_paragraph,
        start_sentence=int(start_sentence) if start_sentence is not None else None,
        end_sentence=int(end_sentence) if end_sentence is not None else None,
        span_type=span_type,
        text=text,
        generated_by="effective_search_backfill",
        status="active",
        segmentation_version=VERSION,
    )
    session.add(candidate_span)
    session.flush()
    candidate_cache[key] = candidate_span.id
    return candidate_span.id


def _candidate_namespace(candidate_span_id: str, item: dict) -> SimpleNamespace:
    meta = item.get("meta") or {}
    paragraph_range = list(meta.get("source_paragraph_range_original") or meta.get("paragraph_range") or [0, 0])
    sentence_range = list(meta.get("source_sentence_range_original") or meta.get("sentence_range") or [None, None])
    start_paragraph = int(paragraph_range[0] or 0) if paragraph_range else 0
    end_paragraph = int(paragraph_range[-1] or start_paragraph) if paragraph_range else start_paragraph
    start_sentence = sentence_range[0] if sentence_range else None
    end_sentence = sentence_range[-1] if sentence_range else None
    return SimpleNamespace(
        id=candidate_span_id,
        text=str(item.get("text") or ""),
        span_type=str(item.get("candidate_type") or "material_span"),
        start_paragraph=start_paragraph,
        end_paragraph=end_paragraph,
        start_sentence=int(start_sentence) if start_sentence is not None else None,
        end_sentence=int(end_sentence) if end_sentence is not None else None,
    )


def _search_batches(
    session,
    *,
    family: str,
    deficits: dict[str, dict[str, int]],
    existing_pairs: set[tuple[str, str, str]],
    hashes_by_family_card: dict[tuple[str, str], set[str]],
    candidate_cache: dict[tuple[str, str, str], str],
    inserted_counter: dict[str, Counter],
    inserted_samples: list[dict],
    article_ids: list[str],
) -> int:
    cfg = FAMILY_CONFIG[family]
    service = MaterialPipelineV2Service(session)
    inserted = 0

    for batch_ids in _batched(article_ids, cfg["batch_size"]):
        requested_cards = [card_id for card_id, need in deficits[family].items() if need > 0]
        if not requested_cards:
            break
        payload = {
            "business_family_id": family,
            "article_ids": batch_ids,
            "business_card_ids": requested_cards,
            "candidate_limit": cfg["candidate_limit"],
            "min_card_score": cfg["min_card_score"],
            "min_business_card_score": cfg["min_business_card_score"],
            "enable_anchor_adaptation": False,
            "preserve_anchor": True,
        }
        result = service.search(payload)
        for item in result.get("items") or []:
            article = session.get(ArticleORM, item["article_id"])
            if article is None:
                continue
            candidate_cards = _selected_or_recommended_cards(item)
            text_hash = build_content_hash(str(item.get("text") or ""))
            for card_id in candidate_cards:
                if not card_id or deficits[family].get(card_id, 0) <= 0:
                    continue
                if text_hash in hashes_by_family_card.get((family, card_id), set()):
                    continue
                ok, _ = is_effective_cached_item(
                    family=family,
                    card_id=card_id,
                    cached_item=item,
                    exact_selected=False,
                )
                if not ok:
                    continue
                candidate_span_id = _ensure_candidate_span(session, candidate_cache, item)
                if (candidate_span_id, family, card_id) in existing_pairs:
                    continue
                candidate = _candidate_namespace(candidate_span_id, item)
                material = MaterialSpanORM(
                    **material_payload(
                        family=family,
                        article=article,
                        candidate=candidate,
                        cached_item=item,
                        selected_business_card=card_id,
                    )
                )
                session.add(material)
                existing_pairs.add((candidate_span_id, family, card_id))
                hashes_by_family_card.setdefault((family, card_id), set()).add(text_hash)
                deficits[family][card_id] -= 1
                inserted_counter[family][card_id] += 1
                inserted += 1
                if len(inserted_samples) < 200:
                    inserted_samples.append(
                        {
                            "family": family,
                            "card_id": card_id,
                            "material_id": material.id,
                            "article_title": article.title,
                            "source_id": getattr(article, "source", None) or getattr(article, "source_url", None) or "",
                            "text_preview": str(item.get("text") or "")[:160],
                            "quality_score": float(item.get("quality_score") or 0.0),
                        }
                    )
                break
        session.commit()
    return inserted


def _mine_sentence_order_windows(
    session,
    *,
    deficits: dict[str, dict[str, int]],
    existing_pairs: set[tuple[str, str, str]],
    hashes_by_family_card: dict[tuple[str, str], set[str]],
    candidate_cache: dict[tuple[str, str, str], str],
    inserted_counter: dict[str, Counter],
    inserted_samples: list[dict],
    article_ids: list[str],
) -> int:
    family = "sentence_order"
    requested_cards = [card_id for card_id, need in deficits[family].items() if need > 0]
    if not requested_cards:
        return 0
    pipeline = MaterialPipelineV2()
    inserted = 0

    for article_id in article_ids:
        if _family_done(deficits, family):
            break
        article = session.get(ArticleORM, article_id)
        if article is None:
            continue
        article_context = pipeline._build_article_context(article)
        paragraph_sentences = article_context.get("paragraph_sentences") or []
        flattened: list[tuple[int, int, int, str]] = []
        global_index = 0
        for paragraph_index, local_sentences in enumerate(paragraph_sentences):
            for local_index, sentence in enumerate(local_sentences):
                text = str(sentence or "").strip()
                if text:
                    flattened.append((paragraph_index, local_index, global_index, text))
                    global_index += 1
        if len(flattened) < 4:
            continue

        for start in range(0, len(flattened) - 3):
            if _family_done(deficits, family):
                break
            for size in (4, 5, 6):
                block = flattened[start : start + size]
                if len(block) < size:
                    continue
                block_text = "\n".join(item[3] for item in block).strip()
                if len(block_text) < 80:
                    continue
                temp_material = SimpleNamespace(
                    id=f"orderwin::{article.id}::{block[0][2]}::{block[-1][2]}",
                    article_id=article.id,
                    candidate_span_id=None,
                    text=block_text,
                    span_type="sentence_block_group",
                    start_paragraph=block[0][0],
                    end_paragraph=block[-1][0],
                    start_sentence=block[0][2],
                    end_sentence=block[-1][2],
                    paragraph_count=max(1, block[-1][0] - block[0][0] + 1),
                    sentence_count=size,
                    quality_flags=[],
                )
                cached_item = pipeline.build_cached_item_from_material(
                    material=temp_material,
                    article=article,
                    business_family_id=family,
                )
                if not cached_item:
                    continue
                candidate_cards = _selected_or_recommended_cards(cached_item)
                text_hash = build_content_hash(block_text)
                for card_id in candidate_cards:
                    if not card_id or deficits[family].get(card_id, 0) <= 0:
                        continue
                    if text_hash in hashes_by_family_card.get((family, card_id), set()):
                        continue
                    ok, _ = is_effective_cached_item(
                        family=family,
                        card_id=card_id,
                        cached_item=cached_item,
                        exact_selected=False,
                    )
                    if not ok:
                        continue
                    candidate_span_id = _ensure_candidate_span(session, candidate_cache, cached_item)
                    if (candidate_span_id, family, card_id) in existing_pairs:
                        continue
                    candidate = _candidate_namespace(candidate_span_id, cached_item)
                    material = MaterialSpanORM(
                        **material_payload(
                            family=family,
                            article=article,
                            candidate=candidate,
                            cached_item=cached_item,
                            selected_business_card=card_id,
                        )
                    )
                    session.add(material)
                    existing_pairs.add((candidate_span_id, family, card_id))
                    hashes_by_family_card.setdefault((family, card_id), set()).add(text_hash)
                    deficits[family][card_id] -= 1
                    inserted_counter[family][card_id] += 1
                    inserted += 1
                    if len(inserted_samples) < 200:
                        inserted_samples.append(
                            {
                                "family": family,
                                "card_id": card_id,
                                "material_id": material.id,
                                "article_title": article.title,
                                "source_id": getattr(article, "source", None) or getattr(article, "source_url", None) or "",
                                "text_preview": block_text[:160],
                                "quality_score": float(cached_item.get("quality_score") or 0.0),
                            }
                        )
                    break
            if inserted and inserted % 25 == 0:
                session.commit()
        session.commit()
    return inserted


def _mine_sentence_fill_middle_windows(
    session,
    *,
    deficits: dict[str, dict[str, int]],
    existing_pairs: set[tuple[str, str, str]],
    hashes_by_family_card: dict[tuple[str, str], set[str]],
    candidate_cache: dict[tuple[str, str, str], str],
    inserted_counter: dict[str, Counter],
    inserted_samples: list[dict],
    article_ids: list[str],
) -> int:
    family = "sentence_fill"
    middle_cards = [
        "sentence_fill__middle_carry_previous__abstract",
        "sentence_fill__middle_lead_next__abstract",
        "sentence_fill__middle_bridge_both_sides__abstract",
    ]
    requested_cards = [card_id for card_id in middle_cards if deficits[family].get(card_id, 0) > 0]
    if not requested_cards:
        return 0

    pipeline = MaterialPipelineV2()
    inserted = 0

    for article_id in article_ids:
        if all(deficits[family].get(card_id, 0) <= 0 for card_id in middle_cards):
            break
        article = session.get(ArticleORM, article_id)
        if article is None:
            continue
        article_context = pipeline._build_article_context(article)
        paragraph_sentences = article_context.get("paragraph_sentences") or []
        flattened: list[tuple[int, int, int, str]] = []
        global_index = 0
        for paragraph_index, local_sentences in enumerate(paragraph_sentences):
            for local_index, sentence in enumerate(local_sentences):
                text = str(sentence or "").strip()
                if text:
                    flattened.append((paragraph_index, local_index, global_index, text))
                    global_index += 1
        if len(flattened) < 3:
            continue

        for start in range(0, len(flattened) - 2):
            if all(deficits[family].get(card_id, 0) <= 0 for card_id in middle_cards):
                break
            for size in (3, 4, 5):
                block = flattened[start : start + size]
                if len(block) < size:
                    continue
                block_text = "".join(item[3] for item in block).strip()
                if len(block_text) < 80 or len(block_text) > 320:
                    continue
                temp_material = SimpleNamespace(
                    id=f"fillmid::{article.id}::{block[0][2]}::{block[-1][2]}",
                    article_id=article.id,
                    candidate_span_id=None,
                    text=block_text,
                    span_type="sentence_block_group",
                    # Force sentence-fill classification into the middle-position branch.
                    start_paragraph=1,
                    end_paragraph=1,
                    start_sentence=block[0][2],
                    end_sentence=block[-1][2],
                    paragraph_count=1,
                    sentence_count=size,
                    quality_flags=[],
                )
                cached_item = pipeline.build_cached_item_from_material(
                    material=temp_material,
                    article=article,
                    business_family_id=family,
                )
                if not cached_item:
                    continue
                candidate_cards = _selected_or_recommended_cards(cached_item)
                text_hash = build_content_hash(block_text)
                for card_id in candidate_cards:
                    if card_id not in middle_cards or deficits[family].get(card_id, 0) <= 0:
                        continue
                    if text_hash in hashes_by_family_card.get((family, card_id), set()):
                        continue
                    ok, _ = is_effective_cached_item(
                        family=family,
                        card_id=card_id,
                        cached_item=cached_item,
                        exact_selected=False,
                    )
                    if not ok:
                        continue
                    candidate_span_id = _ensure_candidate_span(session, candidate_cache, cached_item)
                    if (candidate_span_id, family, card_id) in existing_pairs:
                        continue
                    candidate = _candidate_namespace(candidate_span_id, cached_item)
                    material = MaterialSpanORM(
                        **material_payload(
                            family=family,
                            article=article,
                            candidate=candidate,
                            cached_item=cached_item,
                            selected_business_card=card_id,
                        )
                    )
                    session.add(material)
                    existing_pairs.add((candidate_span_id, family, card_id))
                    hashes_by_family_card.setdefault((family, card_id), set()).add(text_hash)
                    deficits[family][card_id] -= 1
                    inserted_counter[family][card_id] += 1
                    inserted += 1
                    if len(inserted_samples) < 200:
                        inserted_samples.append(
                            {
                                "family": family,
                                "card_id": card_id,
                                "material_id": material.id,
                                "article_title": article.title,
                                "source_id": getattr(article, "source", None) or getattr(article, "source_url", None) or "",
                                "text_preview": block_text[:160],
                                "quality_score": float(cached_item.get("quality_score") or 0.0),
                            }
                        )
                    break
            if inserted and inserted % 25 == 0:
                session.commit()
        session.commit()
    return inserted


def _run_existing_article_pass(
    session,
    deficits: dict[str, dict[str, int]],
    existing_pairs: set[tuple[str, str, str]],
    hashes_by_family_card: dict[tuple[str, str], set[str]],
    candidate_cache: dict[tuple[str, str, str], str],
    inserted_counter: dict[str, Counter],
    inserted_samples: list[dict],
    *,
    article_ids: list[str] | None = None,
) -> dict[str, int]:
    pass_inserted: dict[str, int] = {}
    for family in TARGETS:
        if _family_done(deficits, family):
            pass_inserted[family] = 0
            continue
        ordered_ids = _ordered_article_ids(session, family, article_ids=article_ids)
        pass_inserted[family] = _search_batches(
            session,
            family=family,
            deficits=deficits,
            existing_pairs=existing_pairs,
            hashes_by_family_card=hashes_by_family_card,
            candidate_cache=candidate_cache,
            inserted_counter=inserted_counter,
            inserted_samples=inserted_samples,
            article_ids=ordered_ids,
        )
        if family == "sentence_fill" and any(
            deficits[family].get(card_id, 0) > 0
            for card_id in (
                "sentence_fill__middle_carry_previous__abstract",
                "sentence_fill__middle_lead_next__abstract",
                "sentence_fill__middle_bridge_both_sides__abstract",
            )
        ):
            pass_inserted[family] += _mine_sentence_fill_middle_windows(
                session,
                deficits=deficits,
                existing_pairs=existing_pairs,
                hashes_by_family_card=hashes_by_family_card,
                candidate_cache=candidate_cache,
                inserted_counter=inserted_counter,
                inserted_samples=inserted_samples,
                article_ids=ordered_ids,
            )
        if family == "sentence_order" and not _family_done(deficits, family):
            pass_inserted[family] += _mine_sentence_order_windows(
                session,
                deficits=deficits,
                existing_pairs=existing_pairs,
                hashes_by_family_card=hashes_by_family_card,
                candidate_cache=candidate_cache,
                inserted_counter=inserted_counter,
                inserted_samples=inserted_samples,
                article_ids=ordered_ids,
            )
    return pass_inserted


def main() -> int:
    init_db()
    load_plugins()
    session = get_session()
    crawl_results: list[dict] = []
    try:
        baseline_articles = count_articles()
        baseline_materials = count_materials()
        baseline_counts, existing_pairs, hashes_by_family_card = count_effective_existing(session)
        deficits = deficits_from(baseline_counts)
        candidate_cache = _load_candidate_cache(session)

        inserted_counter_total: dict[str, Counter] = {family: Counter() for family in TARGETS}
        inserted_samples_total: list[dict] = []
        pass_summaries: list[dict] = []

        pass_summaries.append(
            {
                "stage": "existing_articles",
                "inserted": _run_existing_article_pass(
                    session,
                    deficits,
                    existing_pairs,
                    hashes_by_family_card,
                    candidate_cache,
                    inserted_counter_total,
                    inserted_samples_total,
                ),
            }
        )

        for source_id in SOURCE_PLAN:
            if _all_done(deficits):
                break
            crawl_result = run_crawl_for_source(session, source_id)
            crawl_results.append(crawl_result)
            processed_article_ids = list(crawl_result.get("processed_article_ids") or [])
            recent_article_ids = processed_article_ids[:]
            if not recent_article_ids and int(crawl_result.get("ingested_count") or 0) > 0:
                recent_article_ids = [row[0] for row in session.query(ArticleORM.id).order_by(ArticleORM.created_at.desc()).limit(40).all()]
            if recent_article_ids:
                inserted = _run_existing_article_pass(
                    session,
                    deficits,
                    existing_pairs,
                    hashes_by_family_card,
                    candidate_cache,
                    inserted_counter_total,
                    inserted_samples_total,
                    article_ids=recent_article_ids,
                )
                pass_summaries.append({"stage": f"crawl:{source_id}", "inserted": inserted})

        final_counts, _, _ = count_effective_existing(session)
        final_deficits = deficits_from(final_counts)
        all_reached = _all_done(final_deficits)

        report = {
            "run_at": datetime.now().isoformat(timespec="seconds"),
            "version": VERSION,
            "baseline_articles": baseline_articles,
            "baseline_materials": baseline_materials,
            "baseline_effective_counts": compact(baseline_counts),
            "final_articles": count_articles(),
            "final_materials": count_materials(),
            "final_effective_counts": compact(final_counts),
            "final_deficits": final_deficits,
            "all_cards_reached": all_reached,
            "inserted_effective_counts": compact(inserted_counter_total),
            "crawl_results": crawl_results,
            "pass_summaries": pass_summaries,
            "inserted_samples": inserted_samples_total[:120],
        }

        REPORTS_ROOT.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        json_path = REPORTS_ROOT / f"effective_business_card_fast_backfill_{timestamp}.json"
        md_path = REPORTS_ROOT / f"effective_business_card_fast_backfill_{timestamp}.md"
        json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        md_path.write_text(build_markdown(report), encoding="utf-8")
        print(json.dumps({"json": str(json_path), "markdown": str(md_path), "all_cards_reached": all_reached}, ensure_ascii=False))
        return 0 if all_reached else 1
    finally:
        session.close()


if __name__ == "__main__":
    raise SystemExit(main())
