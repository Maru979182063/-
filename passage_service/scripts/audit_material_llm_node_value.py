from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections import Counter
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
from app.infra.segment.paragraph_splitters.default_splitter import DefaultParagraphSplitter  # noqa: E402
from app.infra.segment.sentence_splitters.default_splitter import DefaultSentenceSplitter  # noqa: E402
from app.schemas.span import SpanRecord, SpanVersionSet  # noqa: E402
from app.services.document_genre_classifier import DocumentGenreClassifier  # noqa: E402
from app.services.family_router import FamilyRouter  # noqa: E402
from app.services.family_taggers.continuation_family_tagger import ContinuationFamilyTagger  # noqa: E402
from app.services.family_taggers.fill_family_tagger import FillFamilyTagger  # noqa: E402
from app.services.family_taggers.ordering_family_tagger import OrderingFamilyTagger  # noqa: E402
from app.services.family_taggers.summarization_family_tagger import SummarizationFamilyTagger  # noqa: E402
from app.services.family_taggers.title_family_tagger import TitleFamilyTagger  # noqa: E402
from app.services.material_governance import MaterialGovernanceService  # noqa: E402
from app.services.material_integrity_gate import MaterialIntegrityGate  # noqa: E402
from app.services.material_pipeline_v2 import MaterialPipelineV2  # noqa: E402
from app.services.universal_tagger import UniversalTagger  # noqa: E402


def _clip(text: str, limit: int = 140) -> str:
    return " ".join(str(text or "").replace("\xa0", " ").split())[:limit]


def _timer() -> float:
    return time.perf_counter()


def _elapsed(start: float) -> float:
    return round(time.perf_counter() - start, 4)


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
    seen_material_bands: set[str] = set()
    for row in rows:
        material_band = "has_material" if int(row.mat_count or 0) > 0 else "no_material"
        if material_band not in seen_material_bands or len(picked) < article_limit:
            picked.append(
                {
                    "article_id": row.id,
                    "title": row.title,
                    "clean_text_chars": int(row.clean_len or 0),
                    "candidate_count": int(row.cand_count or 0),
                    "material_count": int(row.mat_count or 0),
                    "material_band": material_band,
                }
            )
            seen_material_bands.add(material_band)
        if len(picked) >= article_limit:
            break
    return picked[:article_limit]


def _span_record(row: CandidateSpanORM, article: ArticleORM) -> SpanRecord:
    sentence_splitter = DefaultSentenceSplitter()
    sentence_count = (
        max(1, int((row.end_sentence or 0) - (row.start_sentence or 0) + 1))
        if row.end_sentence is not None and row.start_sentence is not None
        else max(1, len(sentence_splitter.split(row.text)))
    )
    governance = MaterialGovernanceService()
    return SpanRecord(
        span_id=row.id,
        article_id=row.article_id,
        text=row.text,
        paragraph_count=max(1, int((row.end_paragraph or 0) - (row.start_paragraph or 0) + 1)),
        sentence_count=sentence_count,
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


def _family_taggers() -> dict[str, Any]:
    return {
        SummarizationFamilyTagger().family_name: SummarizationFamilyTagger(),
        TitleFamilyTagger().family_name: TitleFamilyTagger(),
        FillFamilyTagger().family_name: FillFamilyTagger(),
        OrderingFamilyTagger().family_name: OrderingFamilyTagger(),
        ContinuationFamilyTagger().family_name: ContinuationFamilyTagger(),
    }


def _family_prompt_chars(family_name: str, span: SpanRecord, universal_profile: Any) -> int:
    from app.rules.family_config import get_family_subtypes  # noqa: E402

    subtype_names = get_family_subtypes(family_name)
    prompt = "\n".join(
        [
            f"family: {family_name}",
            f"allowed_subtypes: {', '.join(subtype_names)}",
            f"paragraph_count: {span.paragraph_count}",
            f"sentence_count: {span.sentence_count}",
            f"text: {span.text}",
            f"universal_profile: {universal_profile.model_dump_json(ensure_ascii=False)}",
        ]
    )
    return len(prompt)


def _call_family_tagger_heuristic(tagger: Any, span: SpanRecord, universal_profile: Any) -> tuple[list[Any], dict[str, Any]]:
    original = tagger.score_with_llm
    try:
        tagger.score_with_llm = lambda **_: None  # type: ignore[assignment]
        return tagger.score(span, universal_profile)
    finally:
        tagger.score_with_llm = original  # type: ignore[assignment]


def _call_family_tagger_live(tagger: Any, span: SpanRecord, universal_profile: Any) -> tuple[list[Any], dict[str, Any]]:
    return tagger.score(span, universal_profile)


def _candidate_planner_shadow(article: ArticleORM, pipeline: MaterialPipelineV2) -> dict[str, Any]:
    article_context = pipeline._build_article_context(article)
    selected_types = pipeline._expand_candidate_types(pipeline._formal_material_candidate_types())
    t0 = _timer()
    live_candidates = pipeline._derive_candidates_with_llm(article_context=article_context, selected_types=selected_types)
    live_time = _elapsed(t0)
    rule_only = pipeline._derive_functional_slot_rule_candidates(article_context=article_context)
    live_types = Counter([item.get("candidate_type") or "" for item in live_candidates])
    rule_types = Counter([item.get("candidate_type") or "" for item in rule_only])
    non_rule_types = sorted(set(live_types) - set(rule_types))
    return {
        "live_llm_candidate_count": len(live_candidates),
        "live_llm_candidate_types": dict(live_types),
        "rule_only_candidate_count": len(rule_only),
        "rule_only_candidate_types": dict(rule_types),
        "planner_prompt_chars": len(pipeline._build_candidate_planner_prompt(article_context=article_context, selected_types=selected_types)),
        "llm_elapsed_seconds": live_time,
        "unique_value_signal": {
            "adds_non_rule_candidate_types": non_rule_types,
            "appears_high_value": bool(non_rule_types),
        },
    }


def _integrity_gate_shadow(spans: list[SpanRecord], integrity_gate: MaterialIntegrityGate, live_limit: int) -> dict[str, Any]:
    ambiguous: list[dict[str, Any]] = []
    hard_reject = 0
    gray_hold = 0
    allow_direct = 0
    for span in spans:
        signals = integrity_gate._collect_signals(text=span.text, paragraph_count=span.paragraph_count, sentence_count=span.sentence_count)
        hard_fail = integrity_gate._hard_fail_reasons(signals)
        if hard_fail:
            hard_reject += 1
            continue
        if integrity_gate._needs_llm_review(signals):
            ambiguous.append({"span": span, "signals": signals})
            continue
        if integrity_gate._can_rule_allow_directly(signals):
            allow_direct += 1
        else:
            gray_hold += 1
    live_reviews = []
    for item in ambiguous[:live_limit]:
        t0 = _timer()
        result = integrity_gate._llm_review(
            text=item["span"].text,
            paragraph_count=item["span"].paragraph_count,
            sentence_count=item["span"].sentence_count,
            signals=item["signals"],
        )
        live_reviews.append(
            {
                "span_id": item["span"].span_id,
                "prompt_chars": len(
                    "\n".join(
                        [
                            f"paragraph_count: {item['span'].paragraph_count}",
                            f"sentence_count: {item['span'].sentence_count}",
                            f"text: {item['span'].text}",
                            f"signals: {item['signals']}",
                        ]
                    )
                ),
                "elapsed_seconds": _elapsed(t0),
                "result": result,
                "text_snippet": _clip(item["span"].text),
            }
        )
    pass_count = sum(1 for item in live_reviews if item["result"].get("suitable_for_material"))
    return {
        "hard_reject_count": hard_reject,
        "gray_hold_count": gray_hold,
        "rule_allow_count": allow_direct,
        "llm_review_needed_count": len(ambiguous),
        "live_review_sample_count": len(live_reviews),
        "live_review_suitable_count": pass_count,
        "live_review_samples": live_reviews,
    }


def _universal_tagger_shadow(spans: list[SpanRecord], article: ArticleORM, live_limit: int) -> dict[str, Any]:
    tagger = UniversalTagger()
    router = FamilyRouter()
    genre_classifier = DocumentGenreClassifier(get_config_bundle().document_genres)
    sampled = spans[:live_limit]
    if not sampled:
        return {
            "sample_count": 0,
            "exact_top1_match_count": 0,
            "top_candidate_overlap_count": 0,
            "prompt_chars": 0,
            "samples": [],
        }
    heuristic_profiles = [tagger._heuristic_tag(span) for span in sampled]
    t0 = _timer()
    llm_profiles = tagger._tag_many_with_llm(sampled)
    elapsed = _elapsed(t0)
    exact_top1_match = 0
    overlap_count = 0
    sample_rows = []
    for span, heur_profile, llm_profile in zip(sampled, heuristic_profiles, llm_profiles, strict=False):
        genre_result = genre_classifier.classify(title=article.title, text=span.text, source=article.source)
        heur_profile.document_genre = genre_result["document_genre"]
        heur_profile.document_genre_candidates = genre_result["document_genre_candidates"]
        llm_profile.document_genre = genre_result["document_genre"]
        llm_profile.document_genre_candidates = genre_result["document_genre_candidates"]
        heur_route = router.route(span, heur_profile)
        llm_route = router.route(span, llm_profile)
        heur_top = heur_route.get("top_candidates", [])
        llm_top = llm_route.get("top_candidates", [])
        if heur_top[:1] == llm_top[:1]:
            exact_top1_match += 1
        if set(heur_top).intersection(set(llm_top)):
            overlap_count += 1
        sample_rows.append(
            {
                "span_id": span.span_id,
                "heuristic_top_candidates": heur_top,
                "llm_top_candidates": llm_top,
                "heuristic_structure_label": heur_profile.material_structure_label,
                "llm_structure_label": llm_profile.material_structure_label,
                "text_snippet": _clip(span.text),
            }
        )
    return {
        "sample_count": len(sampled),
        "prompt_chars": len(tagger._build_batch_prompt(sampled)),
        "elapsed_seconds": elapsed,
        "exact_top1_match_count": exact_top1_match,
        "top_candidate_overlap_count": overlap_count,
        "samples": sample_rows,
    }


def _family_tagger_shadow(spans: list[SpanRecord], article: ArticleORM, live_limit: int) -> dict[str, Any]:
    tagger = UniversalTagger()
    router = FamilyRouter()
    genre_classifier = DocumentGenreClassifier(get_config_bundle().document_genres)
    taggers = _family_taggers()
    sampled = spans[:live_limit]
    if not sampled:
        return {
            "pair_count": 0,
            "exact_top1_match_count": 0,
            "repeated_text_chars": 0,
            "family_prompt_chars_total": 0,
            "samples": [],
        }
    profiles = tagger._tag_many_with_llm(sampled)
    exact_match = 0
    samples = []
    repeated_text_chars = 0
    prompt_chars_total = 0
    pair_count = 0
    for span, profile in zip(sampled, profiles, strict=False):
        genre_result = genre_classifier.classify(title=article.title, text=span.text, source=article.source)
        profile.document_genre = genre_result["document_genre"]
        profile.document_genre_candidates = genre_result["document_genre_candidates"]
        routed = router.route(span, profile)
        top_families = [family for family in routed.get("top_candidates", []) if family in taggers][:2]
        for family_name in top_families:
            pair_count += 1
            repeated_text_chars += len(span.text)
            prompt_chars_total += _family_prompt_chars(family_name, span, profile)
            live_tagger = taggers[family_name]
            heur_tagger = _family_taggers()[family_name]
            t0 = _timer()
            live_candidates, _ = _call_family_tagger_live(live_tagger, span, profile)
            elapsed = _elapsed(t0)
            heur_candidates, _ = _call_family_tagger_heuristic(heur_tagger, span, profile)
            live_top = live_candidates[0].subtype if live_candidates else None
            heur_top = heur_candidates[0].subtype if heur_candidates else None
            if live_top == heur_top:
                exact_match += 1
            samples.append(
                {
                    "span_id": span.span_id,
                    "family": family_name,
                    "live_top_subtype": live_top,
                    "heuristic_top_subtype": heur_top,
                    "live_candidate_count": len(live_candidates),
                    "heuristic_candidate_count": len(heur_candidates),
                    "prompt_chars": _family_prompt_chars(family_name, span, profile),
                    "elapsed_seconds": elapsed,
                    "text_snippet": _clip(span.text),
                }
            )
    return {
        "pair_count": pair_count,
        "exact_top1_match_count": exact_match,
        "family_prompt_chars_total": prompt_chars_total,
        "repeated_text_chars": repeated_text_chars,
        "samples": samples,
    }


def _article_node_audit(session, article_id: str, live_span_limit: int) -> dict[str, Any]:
    article = session.get(ArticleORM, article_id)
    candidate_rows = list(
        session.scalars(
            select(CandidateSpanORM)
            .where(CandidateSpanORM.article_id == article_id)
            .order_by(CandidateSpanORM.start_paragraph, CandidateSpanORM.start_sentence)
        )
    )
    spans = [_span_record(row, article) for row in candidate_rows]
    paragraph_splitter = DefaultParagraphSplitter()
    sentence_splitter = DefaultSentenceSplitter()
    pipeline = MaterialPipelineV2()
    integrity_gate = MaterialIntegrityGate()
    paragraphs = paragraph_splitter.split(article.clean_text or "")
    sentences = sentence_splitter.split(article.clean_text or "")
    return {
        "article_id": article.id,
        "title": article.title,
        "clean_text_chars": len(article.clean_text or ""),
        "paragraph_count": len(paragraphs),
        "sentence_count": len(sentences),
        "stored_candidate_count": len(candidate_rows),
        "stored_material_count": int(
            session.execute(select(func.count(MaterialSpanORM.id)).where(MaterialSpanORM.article_id == article_id)).scalar_one()
        ),
        "candidate_planner_v2": _candidate_planner_shadow(article, pipeline),
        "material_integrity_gate": _integrity_gate_shadow(spans, integrity_gate, live_limit=live_span_limit),
        "universal_tagger": _universal_tagger_shadow(spans, article, live_limit=live_span_limit),
        "family_tagger_subtype_scoring": _family_tagger_shadow(spans, article, live_limit=live_span_limit),
    }


def run_audit(*, article_limit: int, live_span_limit: int) -> dict[str, Any]:
    init_db()
    session = get_session()
    try:
        articles = _sample_articles(session, article_limit)
        audits = [
            _article_node_audit(session, article["article_id"], live_span_limit=live_span_limit)
            for article in articles
        ]
        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "scope": {
                "article_limit": article_limit,
                "live_span_limit": live_span_limit,
                "sampling": articles,
            },
            "nodes": [
                {
                    "node": "candidate_planner_v2",
                    "file": str(ROOT / "app" / "services" / "material_pipeline_v2.py"),
                    "function": "MaterialPipelineV2._derive_candidates_with_llm",
                    "input_unit": "整篇文章",
                    "output": "formal candidate specs",
                    "role_type": "柔性理解型节点",
                },
                {
                    "node": "logical_segment_refiner",
                    "file": str(ROOT / "app" / "services" / "logical_segment_refiner.py"),
                    "function": "LogicalSegmentRefiner._llm_decision",
                    "input_unit": "单候选 span + 邻接上下文",
                    "output": "merge/drop/keep decision",
                    "role_type": "边界复核型节点",
                },
                {
                    "node": "material_integrity_gate",
                    "file": str(ROOT / "app" / "services" / "material_integrity_gate.py"),
                    "function": "MaterialIntegrityGate._llm_review",
                    "input_unit": "单候选材料",
                    "output": "allow/reject suitability review",
                    "role_type": "边界复核型节点",
                },
                {
                    "node": "universal_tagger",
                    "file": str(ROOT / "app" / "services" / "universal_tagger.py"),
                    "function": "UniversalTagger._tag_many_with_llm",
                    "input_unit": "批量 spans",
                    "output": "universal_profile batch",
                    "role_type": "大批量汇总型节点",
                },
                {
                    "node": "family_tagger_subtype_scoring",
                    "file": str(ROOT / "app" / "services" / "family_taggers" / "base.py"),
                    "function": "BaseFamilyTagger.score_with_llm",
                    "input_unit": "单候选 × 单 family",
                    "output": "subtype candidates",
                    "role_type": "重复解释型节点",
                },
            ],
            "article_audits": audits,
        }
    finally:
        session.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit LLM node value and risk in passage_service material chain.")
    parser.add_argument("--article-limit", type=int, default=3, help="Number of articles to shadow.")
    parser.add_argument("--live-span-limit", type=int, default=4, help="Number of spans per article for live LLM ablation.")
    parser.add_argument("--report-path", type=Path, default=None, help="Optional JSON output path.")
    args = parser.parse_args()
    report = run_audit(article_limit=args.article_limit, live_span_limit=args.live_span_limit)
    text = json.dumps(report, ensure_ascii=False, indent=2)
    if args.report_path:
        args.report_path.parent.mkdir(parents=True, exist_ok=True)
        args.report_path.write_text(text, encoding="utf-8")
    try:
        print(text)
    except UnicodeEncodeError:
        sys.stdout.buffer.write(text.encode("utf-8", errors="replace"))
        sys.stdout.buffer.write(b"\n")


if __name__ == "__main__":
    main()
