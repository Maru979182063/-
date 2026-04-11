from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

from sqlalchemy import desc, func, select  # noqa: E402

from app.core.config import get_config_bundle  # noqa: E402
from app.infra.db.orm.article import ArticleORM  # noqa: E402
from app.infra.db.orm.candidate_span import CandidateSpanORM  # noqa: E402
from app.infra.db.orm.material_span import MaterialSpanORM  # noqa: E402
from app.infra.db.session import get_session, init_db  # noqa: E402
from app.rules.family_config import get_family_names, get_family_subtypes  # noqa: E402
from app.schemas.span import SpanRecord, SpanVersionSet  # noqa: E402
from app.services.document_genre_classifier import DocumentGenreClassifier  # noqa: E402
from app.services.family_router import FamilyRouter  # noqa: E402
from app.services.family_taggers.continuation_family_tagger import ContinuationFamilyTagger  # noqa: E402
from app.services.family_taggers.fill_family_tagger import FillFamilyTagger  # noqa: E402
from app.services.family_taggers.ordering_family_tagger import OrderingFamilyTagger  # noqa: E402
from app.services.family_taggers.summarization_family_tagger import SummarizationFamilyTagger  # noqa: E402
from app.services.family_taggers.title_family_tagger import TitleFamilyTagger  # noqa: E402
from app.services.material_governance import MaterialGovernanceService  # noqa: E402
from app.services.universal_tagger import UniversalTagger  # noqa: E402


def _clip(text: str, limit: int = 140) -> str:
    return " ".join(str(text or "").replace("\xa0", " ").split())[:limit]


def _sample_articles(session, article_limit: int) -> list[dict[str, Any]]:
    rows = list(
        session.execute(
            select(
                ArticleORM.id,
                ArticleORM.title,
                func.length(ArticleORM.clean_text).label("clean_len"),
                func.count(func.distinct(CandidateSpanORM.id)).label("cand_count"),
                func.count(func.distinct(MaterialSpanORM.id)).label("mat_count"),
            )
            .join(CandidateSpanORM, CandidateSpanORM.article_id == ArticleORM.id, isouter=True)
            .join(MaterialSpanORM, MaterialSpanORM.article_id == ArticleORM.id, isouter=True)
            .group_by(ArticleORM.id)
            .having(func.count(func.distinct(CandidateSpanORM.id)) > 0)
            .order_by(desc(func.count(func.distinct(CandidateSpanORM.id))), desc(func.length(ArticleORM.clean_text)))
            .limit(max(article_limit * 3, article_limit))
        )
    )
    picked: list[dict[str, Any]] = []
    seen_bands: set[str] = set()
    for row in rows:
        band = "has_material" if int(row.mat_count or 0) > 0 else "no_material"
        if band not in seen_bands or len(picked) < article_limit:
            picked.append(
                {
                    "article_id": row.id,
                    "title": row.title,
                    "candidate_count": int(row.cand_count or 0),
                    "material_count": int(row.mat_count or 0),
                    "clean_text_chars": int(row.clean_len or 0),
                    "material_band": band,
                }
            )
            seen_bands.add(band)
        if len(picked) >= article_limit:
            break
    return picked[:article_limit]


def _family_taggers() -> dict[str, Any]:
    tagger_instances = [
        SummarizationFamilyTagger(),
        TitleFamilyTagger(),
        FillFamilyTagger(),
        OrderingFamilyTagger(),
        ContinuationFamilyTagger(),
    ]
    return {family_name: tagger for family_name, tagger in zip(get_family_names(), tagger_instances, strict=False)}


def _span_record(row: CandidateSpanORM, article: ArticleORM) -> SpanRecord:
    sentence_count = (
        max(1, int((row.end_sentence or 0) - (row.start_sentence or 0) + 1))
        if row.end_sentence is not None and row.start_sentence is not None
        else max(1, row.text.count("。"))
    )
    governance = MaterialGovernanceService()
    return SpanRecord(
        span_id=row.id,
        article_id=row.article_id,
        text=row.text,
        paragraph_count=max(1, int((row.end_paragraph or 0) - (row.start_paragraph or 0) + 1)),
        sentence_count=sentence_count or 1,
        source_domain=article.domain,
        source=governance.build_source_info(article),
        status=row.status,
        version=SpanVersionSet(
            segment_version=row.segmentation_version,
            universal_tag_version="audit",
            route_version="audit",
            family_tag_version="audit",
        ),
    )


def _legacy_family_prompt_chars(*, family_name: str, span: SpanRecord, universal_profile: Any) -> int:
    prompt = "\n".join(
        [
            f"family: {family_name}",
            f"allowed_subtypes: {', '.join(get_family_subtypes(family_name))}",
            f"paragraph_count: {span.paragraph_count}",
            f"sentence_count: {span.sentence_count}",
            f"text: {span.text}",
            f"universal_profile: {universal_profile.model_dump_json(ensure_ascii=False)}",
        ]
    )
    return len(prompt)


def _runtime_context(*, family_name: str, routed: dict[str, Any]) -> dict[str, Any]:
    family_scores = routed["family_scores"].family_scores
    top_candidates = routed.get("top_candidates", [])
    ranked = sorted([(name, family_scores.get(name, 0.0)) for name in top_candidates], key=lambda item: item[1], reverse=True)
    primary_family = routed["family_scores"].primary_family
    primary_score = float(family_scores.get(primary_family or "", 0.0))
    second_score = float(ranked[1][1]) if len(ranked) > 1 else 0.0
    family_score = float(family_scores.get(family_name, 0.0))
    family_rank = next((index for index, (name, _) in enumerate(ranked) if name == family_name), 99)
    return {
        "family": family_name,
        "family_rank": family_rank,
        "family_score": round(family_score, 4),
        "primary_family": primary_family,
        "primary_score": round(primary_score, 4),
        "score_gap_from_primary": round(max(primary_score - family_score, 0.0), 4),
        "primary_second_gap": round(primary_score - second_score, 4) if ranked else 1.0,
        "top_candidates": top_candidates,
    }


def _heuristic_only_score(tagger: Any, span: SpanRecord, universal_profile: Any, runtime_context: dict[str, Any]) -> tuple[list[Any], dict[str, Any]]:
    original_config = tagger.llm_config
    tagger.llm_config = {**original_config, "enabled": False}
    tagger.set_runtime_context(runtime_context)
    try:
        return tagger.score(span, universal_profile)
    finally:
        tagger.clear_runtime_context()
        tagger.llm_config = original_config


def _forced_old_score(tagger: Any, span: SpanRecord, universal_profile: Any, runtime_context: dict[str, Any]) -> tuple[list[Any], dict[str, Any]]:
    original = tagger._should_use_llm
    tagger.set_runtime_context(runtime_context)
    try:
        tagger._should_use_llm = lambda **_: (True, "forced_old_path")  # type: ignore[assignment]
        return tagger.score(span, universal_profile)
    finally:
        tagger._should_use_llm = original  # type: ignore[assignment]
        tagger.clear_runtime_context()


def _new_score(tagger: Any, span: SpanRecord, universal_profile: Any, runtime_context: dict[str, Any]) -> tuple[list[Any], dict[str, Any]]:
    tagger.set_runtime_context(runtime_context)
    try:
        return tagger.score(span, universal_profile)
    finally:
        tagger.clear_runtime_context()


def audit(article_limit: int, span_limit: int, live_pair_limit: int) -> dict[str, Any]:
    init_db()
    config_bundle = get_config_bundle()
    genre_classifier = DocumentGenreClassifier(config_bundle.document_genres)
    universal_tagger = UniversalTagger()
    router = FamilyRouter()
    taggers = _family_taggers()

    with get_session() as session:
        sampled = _sample_articles(session, article_limit)
        article_reports: list[dict[str, Any]] = []
        global_before_calls = 0
        global_after_calls = 0
        global_before_prompt = 0
        global_after_prompt = 0
        global_before_repeated = 0
        global_after_repeated = 0
        global_before_repeated_chars = 0
        global_after_repeated_chars = 0
        live_pair_samples: list[dict[str, Any]] = []

        for article_meta in sampled:
            article = session.get(ArticleORM, article_meta["article_id"])
            if article is None:
                continue
            candidate_rows = list(
                session.execute(
                    select(CandidateSpanORM)
                    .where(CandidateSpanORM.article_id == article.id)
                    .order_by(desc(func.length(CandidateSpanORM.text)), CandidateSpanORM.id)
                    .limit(span_limit)
                )
            )
            spans = [_span_record(row[0], article) for row in candidate_rows]
            profiles = [universal_tagger._heuristic_tag(span) for span in spans]

            before_calls = 0
            after_calls = 0
            before_prompt = 0
            after_prompt = 0
            before_repeated = 0
            after_repeated = 0
            before_repeated_chars = 0
            after_repeated_chars = 0
            article_live_pairs: list[dict[str, Any]] = []

            for span, profile in zip(spans, profiles, strict=False):
                genre_result = genre_classifier.classify(title=article.title, text=span.text, source=article.source)
                profile.document_genre = genre_result["document_genre"]
                profile.document_genre_candidates = genre_result["document_genre_candidates"]
                routed = router.route(span, profile)
                top_families = [family for family in routed.get("top_candidates", []) if family in taggers]
                before_calls += len(top_families)
                before_repeated += max(0, len(top_families) - 1)
                before_repeated_chars += len(span.text) * max(0, len(top_families) - 1)

                llm_used_for_span = 0
                for family_name in top_families:
                    tagger = taggers[family_name]
                    context = _runtime_context(family_name=family_name, routed=routed)
                    heuristic_candidates, _ = _heuristic_only_score(tagger, span, profile, context)
                    tagger.set_runtime_context(context)
                    should_use_llm, gate_reason = tagger._should_use_llm(heuristic_candidates=heuristic_candidates)
                    tagger.clear_runtime_context()
                    before_prompt += _legacy_family_prompt_chars(family_name=family_name, span=span, universal_profile=profile)
                    if should_use_llm:
                        after_calls += 1
                        llm_used_for_span += 1
                        tagger.set_runtime_context(context)
                        after_prompt += len(
                            tagger.build_llm_prompt(
                                span=span,
                                universal_profile=profile,
                                subtype_names=get_family_subtypes(family_name),
                            )
                        )
                        tagger.clear_runtime_context()
                    if should_use_llm or len(article_live_pairs) < live_pair_limit:
                        old_tagger = _family_taggers()[family_name]
                        new_tagger = _family_taggers()[family_name]
                        old_candidates, old_notes = _forced_old_score(old_tagger, span, profile, context)
                        new_candidates, new_notes = _new_score(new_tagger, span, profile, context)
                        article_live_pairs.append(
                            {
                                "span_id": span.span_id,
                                "family": family_name,
                                "gate_reason": gate_reason,
                                "old_top_subtype": old_candidates[0].subtype if old_candidates else None,
                                "new_top_subtype": new_candidates[0].subtype if new_candidates else None,
                                "old_candidate_count": len(old_candidates),
                                "new_candidate_count": len(new_candidates),
                                "old_llm_used": bool(old_notes.get("llm_used")),
                                "new_llm_used": bool(new_notes.get("llm_used")),
                                "text_snippet": _clip(span.text),
                            }
                        )
                after_repeated += max(0, llm_used_for_span - 1)
                compact_text_len = len(taggers[top_families[0]]._compact_text(span.text)) if top_families else 0
                after_repeated_chars += compact_text_len * max(0, llm_used_for_span - 1)

            global_before_calls += before_calls
            global_after_calls += after_calls
            global_before_prompt += before_prompt
            global_after_prompt += after_prompt
            global_before_repeated += before_repeated
            global_after_repeated += after_repeated
            global_before_repeated_chars += before_repeated_chars
            global_after_repeated_chars += after_repeated_chars
            live_pair_samples.extend(article_live_pairs)

            article_reports.append(
                {
                    **article_meta,
                    "sampled_candidate_count": len(spans),
                    "before_family_llm_calls": before_calls,
                    "after_family_llm_calls": after_calls,
                    "before_repeated_family_calls_beyond_first": before_repeated,
                    "after_repeated_family_calls_beyond_first": after_repeated,
                    "before_prompt_chars": before_prompt,
                    "after_prompt_chars": after_prompt,
                    "before_repeated_text_chars": before_repeated_chars,
                    "after_repeated_text_chars": after_repeated_chars,
                    "live_pair_samples": article_live_pairs,
                }
            )

    unchanged_top1 = sum(1 for item in live_pair_samples if item["old_top_subtype"] == item["new_top_subtype"])
    changed_top1 = len(live_pair_samples) - unchanged_top1
    payload_drop_pairs = sum(1 for item in live_pair_samples if item["old_candidate_count"] > 0 and item["new_candidate_count"] == 0)

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "scope": {
            "article_limit": article_limit,
            "span_limit": span_limit,
            "live_pair_limit": live_pair_limit,
        },
        "global_summary": {
            "before_family_llm_calls": global_before_calls,
            "after_family_llm_calls": global_after_calls,
            "before_repeated_family_calls_beyond_first": global_before_repeated,
            "after_repeated_family_calls_beyond_first": global_after_repeated,
            "before_prompt_chars": global_before_prompt,
            "after_prompt_chars": global_after_prompt,
            "before_repeated_text_chars": global_before_repeated_chars,
            "after_repeated_text_chars": global_after_repeated_chars,
            "llm_call_reduction_ratio": round(1 - (global_after_calls / max(global_before_calls, 1)), 4),
            "prompt_char_reduction_ratio": round(1 - (global_after_prompt / max(global_before_prompt, 1)), 4),
            "repeated_call_reduction_ratio": round(1 - (global_after_repeated / max(global_before_repeated, 1)), 4),
            "live_pair_count": len(live_pair_samples),
            "unchanged_top1_pairs": unchanged_top1,
            "changed_top1_pairs": changed_top1,
            "payload_drop_pairs": payload_drop_pairs,
        },
        "articles": article_reports,
        "live_pair_samples": live_pair_samples,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit family_tagger reduction impact.")
    parser.add_argument("--article-limit", type=int, default=3)
    parser.add_argument("--span-limit", type=int, default=12)
    parser.add_argument("--live-pair-limit", type=int, default=6)
    parser.add_argument("--report-path", type=Path, required=True)
    args = parser.parse_args()

    report = audit(article_limit=args.article_limit, span_limit=args.span_limit, live_pair_limit=args.live_pair_limit)
    args.report_path.parent.mkdir(parents=True, exist_ok=True)
    args.report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report["global_summary"], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
