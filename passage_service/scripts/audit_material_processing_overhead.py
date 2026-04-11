from __future__ import annotations

import argparse
import json
import os
import statistics
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
from app.infra.segment.window_generators.paragraph_window_generator import ParagraphWindowGenerator  # noqa: E402
from app.infra.segment.window_generators.sentence_window_generator import SentenceWindowGenerator  # noqa: E402
from app.infra.segment.window_generators.story_fragment_generator import StoryFragmentGenerator  # noqa: E402
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
from app.services.material_integrity_gate import MaterialIntegrityGate  # noqa: E402
from app.services.material_pipeline_v2 import MaterialPipelineV2  # noqa: E402
from app.services.universal_tagger import UniversalTagger  # noqa: E402


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _clip_text(text: str, limit: int = 160) -> str:
    normalized = " ".join(str(text or "").replace("\xa0", " ").split())
    return normalized[:limit]


def _timer() -> float:
    return time.perf_counter()


def _duration(start: float) -> float:
    return round(time.perf_counter() - start, 4)


def _family_tagger_registry() -> dict[str, Any]:
    family_names = get_family_names()
    tagger_instances = [
        SummarizationFamilyTagger(),
        TitleFamilyTagger(),
        FillFamilyTagger(),
        OrderingFamilyTagger(),
        ContinuationFamilyTagger(),
    ]
    mapping: dict[str, Any] = {}
    for family_name, tagger in zip(family_names, tagger_instances, strict=False):
        mapping[family_name] = tagger
    return mapping


def _family_tagger_prompt_length(*, family_name: str, span: SpanRecord, universal_profile: Any) -> int:
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


def _collect_llm_call_points() -> list[dict[str, Any]]:
    return [
        {
            "call_point": "candidate_planner_v2",
            "file": str(ROOT / "app" / "services" / "material_pipeline_v2.py"),
            "function": "MaterialPipelineV2._derive_candidates_with_llm",
            "input_unit": "整篇文章",
            "frequency_risk": "每篇文章 1 次",
            "cache": "无显式缓存",
            "skip_condition": "llm disabled 或 provider disabled",
            "necessity": "必要性中等",
            "risk": "单次 prompt 长，后续仍会再做大量候选级处理",
        },
        {
            "call_point": "logical_segment_refiner",
            "file": str(ROOT / "app" / "services" / "logical_segment_refiner.py"),
            "function": "LogicalSegmentRefiner._llm_decision",
            "input_unit": "候选 span + 前后邻接",
            "frequency_risk": "每篇文章最多 review_max_spans 次，当前配置 12",
            "cache": "无显式缓存",
            "skip_condition": "span 太短 / reviewed_count 达上限 / llm disabled",
            "necessity": "必要性偏低到中等",
            "risk": "对 fallback 候选做逐条复核，疑似过细",
        },
        {
            "call_point": "material_integrity_gate",
            "file": str(ROOT / "app" / "services" / "material_integrity_gate.py"),
            "function": "MaterialIntegrityGate._llm_review",
            "input_unit": "单候选材料",
            "frequency_risk": "每个需复核候选 1 次",
            "cache": "无显式缓存",
            "skip_condition": "rule 直接 allow/reject 或 llm disabled",
            "necessity": "必要性中等",
            "risk": "候选数一多会线性放大",
        },
        {
            "call_point": "universal_tagger",
            "file": str(ROOT / "app" / "services" / "universal_tagger.py"),
            "function": "UniversalTagger._tag_many_with_llm",
            "input_unit": "整批 passed spans",
            "frequency_risk": "每篇文章通常 1 次",
            "cache": "无显式缓存",
            "skip_condition": "llm disabled 或 provider disabled",
            "necessity": "必要性中等",
            "risk": "批量 prompt 很长；配置里有 batch_size 但当前代码未消费",
        },
        {
            "call_point": "family_tagger_subtype_scoring",
            "file": str(ROOT / "app" / "services" / "family_taggers" / "base.py"),
            "function": "BaseFamilyTagger.score_with_llm",
            "input_unit": "单候选 span × 单 family",
            "frequency_risk": "每个 passed 候选会按 top_candidates 调 2~3 次",
            "cache": "无显式缓存",
            "skip_condition": "llm disabled 或 provider disabled",
            "necessity": "疑似过高",
            "risk": "同一 span 文本和同一 universal_profile 被重复喂给多个 family tagger，最像过处理主因",
        },
        {
            "call_point": "plugin_llm_feature_tagger",
            "file": str(ROOT / "app" / "infra" / "plugins" / "builtins" / "taggers" / "llm_feature_tagger.py"),
            "function": "LLMFeatureTagger.tag",
            "input_unit": "候选 span",
            "frequency_risk": "插件侧可用，但当前主链未直接消费",
            "cache": "未知",
            "skip_condition": "取决于插件启用",
            "necessity": "不在当前主链",
            "risk": "本轮只做旁注，不计入主路径",
        },
    ]


def _throttle_short_article_spans(
    spans: list[dict[str, Any]],
    paragraphs: list[dict[str, Any]],
    sentences: list[dict[str, Any]],
    clean_text: str,
) -> list[dict[str, Any]]:
    short_article = len(sentences) <= 8 or len(clean_text) <= 1200
    if not short_article:
        return spans

    single_paragraphs = [item for item in spans if item["span_type"] == "single_paragraph"]
    paragraph_windows = [item for item in spans if item["span_type"] == "paragraph_window"]
    sentence_groups = [item for item in spans if item["span_type"] == "sentence_group"]
    story_fragments = [item for item in spans if item["span_type"] == "story_fragment"]

    throttled: list[dict[str, Any]] = []
    throttled.extend(single_paragraphs[: max(1, min(len(paragraphs), 4))])
    throttled.extend(paragraph_windows[:2])
    throttled.extend(sentence_groups[:1])
    if len(sentences) >= 6:
        throttled.extend(story_fragments[:1])
    return throttled


def _build_v1_base_spans(*, config: dict, paragraphs: list[dict[str, Any]], sentences: list[dict[str, Any]], clean_text: str) -> list[dict[str, Any]]:
    generators = [ParagraphWindowGenerator(), SentenceWindowGenerator(), StoryFragmentGenerator()]
    spans: list[dict[str, Any]] = []
    for generator in generators:
        spans.extend(generator.generate(paragraphs, sentences, config))
    spans = _throttle_short_article_spans(
        spans,
        paragraphs,
        sentences,
        clean_text,
    )
    return spans


def _article_profile(article: ArticleORM, candidate_rows: list[CandidateSpanORM]) -> dict[str, Any]:
    config_bundle = get_config_bundle()
    paragraph_splitter = DefaultParagraphSplitter()
    sentence_splitter = DefaultSentenceSplitter()
    pipeline = MaterialPipelineV2()
    integrity_gate = MaterialIntegrityGate()
    universal_tagger = UniversalTagger()
    genre_classifier = DocumentGenreClassifier(config_bundle.document_genres)
    family_router = FamilyRouter()
    family_taggers = _family_tagger_registry()
    governance = MaterialGovernanceService()

    timings: dict[str, float] = {}

    t0 = _timer()
    paragraphs_raw = paragraph_splitter.split(article.clean_text)
    timings["paragraph_split"] = _duration(t0)

    t0 = _timer()
    paragraph_records = [
        {"paragraph_index": idx, "text": text, "char_count": len(text)}
        for idx, text in enumerate(paragraphs_raw)
    ]
    sentence_records: list[dict[str, Any]] = []
    global_sentence_index = 0
    for paragraph in paragraph_records:
        for sentence in sentence_splitter.split(paragraph["text"]):
            sentence_records.append(
                {
                    "paragraph_id": None,
                    "paragraph_index": paragraph["paragraph_index"],
                    "sentence_index": global_sentence_index,
                    "text": sentence,
                }
            )
            global_sentence_index += 1
    paragraph_sentence_ranges: dict[int, list[int]] = {}
    for sentence in sentence_records:
        paragraph_sentence_ranges.setdefault(sentence["paragraph_index"], []).append(sentence["sentence_index"])
    enriched_paragraphs = []
    for paragraph in paragraph_records:
        indexes = paragraph_sentence_ranges.get(paragraph["paragraph_index"], [])
        enriched_paragraphs.append(
            {
                **paragraph,
                "sentence_start": indexes[0] if indexes else None,
                "sentence_end": indexes[-1] if indexes else None,
            }
        )
    timings["sentence_split"] = _duration(t0)

    t0 = _timer()
    candidate_planner_prompt = pipeline._build_candidate_planner_prompt(
        article_context=pipeline._build_article_context(article),
        selected_types=pipeline._expand_candidate_types(pipeline._formal_material_candidate_types()),
    )
    timings["candidate_planner_prompt_build"] = _duration(t0)

    t0 = _timer()
    v1_base_spans = _build_v1_base_spans(
        config=config_bundle.segmentation,
        paragraphs=enriched_paragraphs,
        sentences=sentence_records,
        clean_text=article.clean_text,
    )
    timings["v1_base_span_generation"] = _duration(t0)

    from app.services.logical_segment_refiner import LogicalSegmentRefiner  # noqa: E402
    logical_refiner_obj = LogicalSegmentRefiner()

    t0 = _timer()
    logical_review_candidates = []
    for index, span in enumerate(v1_base_spans):
        decision = logical_refiner_obj._heuristic_decision(span)
        if logical_refiner_obj._should_use_llm(span, decision, len(logical_review_candidates)):
            prev_text = v1_base_spans[index - 1]["text"][:180] if index > 0 else ""
            next_text = v1_base_spans[index + 1]["text"][:180] if index + 1 < len(v1_base_spans) else ""
            prompt = "\n".join(
                [
                    f"current_span_type: {span['span_type']}",
                    f"current_text: {span['text']}",
                    f"prev_text: {prev_text}",
                    f"next_text: {next_text}",
                ]
            )
            logical_review_candidates.append(
                {
                    "span_type": span["span_type"],
                    "text_chars": len(span["text"]),
                    "prompt_chars": len(prompt),
                    "decision": decision["action"],
                }
            )
    timings["logical_refiner_review_scan"] = _duration(t0)

    stored_candidate_count = len(candidate_rows)
    stored_candidates_as_spans: list[SpanRecord] = []
    for row in candidate_rows:
        stored_candidates_as_spans.append(
            SpanRecord(
                span_id=row.id,
                article_id=row.article_id,
                text=row.text,
                paragraph_count=max(1, int((row.end_paragraph or 0) - (row.start_paragraph or 0) + 1)),
                sentence_count=max(1, int(((row.end_sentence or 0) - (row.start_sentence or 0) + 1) if row.end_sentence is not None and row.start_sentence is not None else len(sentence_splitter.split(row.text)))),
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
        )

    t0 = _timer()
    integrity_results = []
    hard_reject = 0
    gray_hold = 0
    allow_direct = 0
    integrity_llm_needed = 0
    for span in stored_candidates_as_spans:
        signals = integrity_gate._collect_signals(
            text=span.text,
            paragraph_count=span.paragraph_count,
            sentence_count=span.sentence_count,
        )
        hard_fail = integrity_gate._hard_fail_reasons(signals)
        needs_llm = False
        admission = "allow"
        if hard_fail:
            hard_reject += 1
            admission = "reject"
        else:
            needs_llm = integrity_gate._needs_llm_review(signals)
            if needs_llm:
                integrity_llm_needed += 1
                admission = "llm_review"
            elif not integrity_gate._can_rule_allow_directly(signals):
                gray_hold += 1
                admission = "gray_hold"
            else:
                allow_direct += 1
                admission = "allow"
        prompt_chars = 0
        if needs_llm:
            prompt = "\n".join(
                [
                    f"paragraph_count: {span.paragraph_count}",
                    f"sentence_count: {span.sentence_count}",
                    f"text: {span.text}",
                    f"signals: {signals}",
                ]
            )
            prompt_chars = len(prompt)
        integrity_results.append(
            {
                "span_id": span.span_id,
                "admission": admission,
                "needs_llm": needs_llm,
                "prompt_chars": prompt_chars,
            }
        )
    timings["integrity_scan"] = _duration(t0)

    passed_spans = [
        span
        for span, info in zip(stored_candidates_as_spans, integrity_results, strict=False)
        if info["admission"] in {"allow", "llm_review", "gray_hold"}
    ]

    t0 = _timer()
    universal_profiles = [universal_tagger._heuristic_tag(span) for span in passed_spans]
    timings["universal_heuristic_tagging"] = _duration(t0)

    universal_prompt_chars = len(universal_tagger._build_batch_prompt(passed_spans)) if passed_spans else 0

    t0 = _timer()
    family_prompt_chars_total = 0
    family_prompt_chars_by_family: Counter[str] = Counter()
    family_call_counter: Counter[str] = Counter()
    top_candidate_widths: list[int] = []
    repeated_family_calls = 0
    for span, profile in zip(passed_spans, universal_profiles, strict=False):
        genre_result = genre_classifier.classify(title=article.title, text=span.text, source=article.source)
        profile.document_genre = genre_result["document_genre"]
        profile.document_genre_candidates = genre_result["document_genre_candidates"]
        routed = family_router.route(span, profile)
        top_candidates = [family for family in routed.get("top_candidates", []) if family in family_taggers]
        top_candidate_widths.append(len(top_candidates))
        if len(top_candidates) > 1:
            repeated_family_calls += len(top_candidates) - 1
        for family_name in top_candidates:
            family_call_counter[family_name] += 1
            prompt_chars = _family_tagger_prompt_length(
                family_name=family_name,
                span=span,
                universal_profile=profile,
            )
            family_prompt_chars_total += prompt_chars
            family_prompt_chars_by_family[family_name] += prompt_chars
    timings["family_routing_and_prompt_estimation"] = _duration(t0)

    return {
        "article_id": article.id,
        "title": article.title,
        "clean_text_chars": len(article.clean_text or ""),
        "paragraph_count": len(paragraphs_raw),
        "sentence_count": len(sentence_records),
        "stored_candidate_count": stored_candidate_count,
        "stage_timings_seconds": timings,
        "candidate_planner": {
            "potential_llm_calls": 1,
            "prompt_chars": len(candidate_planner_prompt),
        },
        "fallback_generation": {
            "v1_base_span_count": len(v1_base_spans),
            "logical_refiner_review_max_spans": logical_refiner_obj.review_max_spans,
            "logical_refiner_potential_llm_calls": len(logical_review_candidates),
            "logical_refiner_samples": logical_review_candidates[:6],
        },
        "integrity_gate": {
            "hard_reject_count": hard_reject,
            "gray_hold_count": gray_hold,
            "rule_allow_count": allow_direct,
            "potential_llm_calls": integrity_llm_needed,
            "prompt_chars_total": sum(item["prompt_chars"] for item in integrity_results),
        },
        "universal_tagger": {
            "passed_span_count": len(passed_spans),
            "potential_llm_calls": 1 if passed_spans else 0,
            "prompt_chars": universal_prompt_chars,
            "batch_size_config_unused": get_config_bundle().llm.get("batch", {}).get("universal_batch_size"),
        },
        "family_taggers": {
            "potential_llm_calls_total": int(sum(family_call_counter.values())),
            "potential_llm_calls_by_family": dict(family_call_counter),
            "average_families_per_passed_candidate": round(statistics.mean(top_candidate_widths), 4) if top_candidate_widths else 0.0,
            "repeated_family_calls_beyond_first": repeated_family_calls,
            "prompt_chars_total": family_prompt_chars_total,
            "prompt_chars_by_family": dict(family_prompt_chars_by_family),
        },
    }


def _global_candidate_stats(session) -> dict[str, Any]:
    rows = list(
        session.execute(
            select(
                ArticleORM.id,
                func.length(ArticleORM.clean_text).label("clean_len"),
                func.count(func.distinct(CandidateSpanORM.id)).label("cand_count"),
                func.count(func.distinct(MaterialSpanORM.id)).label("mat_count"),
            )
            .join(CandidateSpanORM, CandidateSpanORM.article_id == ArticleORM.id, isouter=True)
            .join(MaterialSpanORM, MaterialSpanORM.article_id == ArticleORM.id, isouter=True)
            .group_by(ArticleORM.id)
        )
    )
    candidate_counts = [int(row.cand_count or 0) for row in rows]
    articles_with_candidates = [count for count in candidate_counts if count > 0]
    return {
        "article_total": len(rows),
        "articles_with_candidates": len(articles_with_candidates),
        "candidate_count_stats": {
            "avg": round(statistics.mean(articles_with_candidates), 2) if articles_with_candidates else 0.0,
            "median": round(statistics.median(articles_with_candidates), 2) if articles_with_candidates else 0.0,
            "p90": sorted(articles_with_candidates)[int(len(articles_with_candidates) * 0.9) - 1] if len(articles_with_candidates) >= 10 else (max(articles_with_candidates) if articles_with_candidates else 0),
            "max": max(articles_with_candidates) if articles_with_candidates else 0,
            "gt_100": sum(1 for value in articles_with_candidates if value > 100),
            "gt_300": sum(1 for value in articles_with_candidates if value > 300),
            "gt_500": sum(1 for value in articles_with_candidates if value > 500),
        },
        "top_articles_by_candidate_count": [
            {
                "article_id": row.id,
                "clean_text_chars": int(row.clean_len or 0),
                "candidate_count": int(row.cand_count or 0),
                "material_count": int(row.mat_count or 0),
            }
            for row in sorted(rows, key=lambda item: int(item.cand_count or 0), reverse=True)[:10]
        ],
    }


def run_audit(*, article_limit: int) -> dict[str, Any]:
    init_db()
    session = get_session()
    try:
        sample_rows = list(
            session.execute(
                select(
                    ArticleORM.id,
                    func.length(ArticleORM.clean_text).label("clean_len"),
                    func.count(func.distinct(CandidateSpanORM.id)).label("cand_count"),
                )
                .join(CandidateSpanORM, CandidateSpanORM.article_id == ArticleORM.id)
                .group_by(ArticleORM.id)
                .order_by(desc(func.count(func.distinct(CandidateSpanORM.id))), desc(func.length(ArticleORM.clean_text)))
                .limit(article_limit)
            )
        )
        profiles = []
        for row in sample_rows:
            article = session.get(ArticleORM, row.id)
            candidate_rows = list(
                session.scalars(
                    select(CandidateSpanORM)
                    .where(CandidateSpanORM.article_id == row.id)
                    .order_by(CandidateSpanORM.start_paragraph, CandidateSpanORM.start_sentence)
                )
            )
            profiles.append(_article_profile(article, candidate_rows))
        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "scope": {
                "article_limit": article_limit,
                "sampling_strategy": "按 candidate_count 降序抽样",
            },
            "global_candidate_stats": _global_candidate_stats(session),
            "llm_call_points": _collect_llm_call_points(),
            "sample_profiles": profiles,
        }
    finally:
        session.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit material processing overhead and over-processing risk.")
    parser.add_argument("--article-limit", type=int, default=4, help="Number of articles to sample.")
    parser.add_argument("--report-path", type=Path, default=None, help="Optional JSON report path.")
    args = parser.parse_args()

    report = run_audit(article_limit=args.article_limit)
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
