from __future__ import annotations

import re
from copy import deepcopy
from collections import Counter
from difflib import SequenceMatcher
from typing import Any

from app.core.config import get_config_bundle
from app.infra.segment.paragraph_splitters.default_splitter import DefaultParagraphSplitter
from app.infra.segment.sentence_splitters.default_splitter import DefaultSentenceSplitter
from app.schemas.span import SpanRecord, SpanVersionSet
from app.services.card_registry_v2 import CardRegistryV2
from app.services.document_genre_classifier import DocumentGenreClassifier
from app.services.llm_runtime import get_llm_provider, read_prompt_file
from app.services.universal_tagger import UniversalTagger


STOPWORDS = {"我们", "他们", "你们", "因为", "所以", "如果", "但是", "然而", "一个", "一种", "这个", "这些", "那些", "进行", "通过", "已经", "以及", "需要", "可以", "就是", "不是", "文章", "问题", "发展", "社会", "中国"}
COUNTERINTUITIVE_MARKERS = ("其实", "并非", "恰恰", "反而", "看似", "未必", "不只是", "相反")
TIMELINE_MARKERS = ("起初", "后来", "近年", "如今", "至今", "未来", "当年", "随后")
TURNING_MARKERS = ("但是", "然而", "不过", "却", "更重要的是", "真正关键在于")
SUMMARY_MARKERS = ("总之", "可见", "因此", "由此", "这意味着", "归根结底")
QUESTION_MARKERS = ("为什么", "何以", "如何", "何处", "谁在", "何以至此")
VALUE_MARKERS = ("应该", "必须", "值得", "重要", "关键", "意义", "警惕")
MECHANISM_MARKERS = ("机制", "在于", "导致", "作用于", "驱动", "运转")
PARALLEL_MARKERS = ("一方面", "另一方面", "同时", "并且", "此外", "其一", "其二", "首先", "其次")
CONTEXTUAL_OPENINGS = ("对此", "与此同时", "另一方面", "此外", "这也", "因此", "所以")
CAUSE_MARKERS = ("因为", "由于", "缘于", "起因于")
CONCLUSION_MARKERS = ("所以", "因此", "因而", "故而", "故", "于是", "可见", "看来", "由此")
NECESSARY_CONDITION_MARKERS = ("只有", "才", "唯有", "前提", "基础", "保障")
COUNTERMEASURE_MARKERS = ("应该", "应当", "应", "需要", "要", "必须", "通过", "采取", "措施", "方式", "方法", "渠道", "途径", "才能")
THEME_HINT_MARKERS = ("围绕", "主题", "核心", "关键", "主线")
ORDER_DEFINITION_MARKERS = ("就是", "是指", "指的是")
ORDER_PRONOUN_MARKERS = ("这", "那", "其", "该", "此", "这些", "那些", "他们", "它们")
ORDER_TURNING_BINDING_MARKERS = ("虽然", "但是", "可是", "然而", "不过", "却")
ORDER_PARALLEL_BINDING_MARKERS = ("同时", "同样", "此外", "也", "另一方面")
ORDER_ACTION_MARKERS = ("首先", "其次", "再次", "最后", "第一步", "第二步", "第三步", "先", "再")
ORDER_PROBLEM_MARKERS = ("问题在于", "问题是", "难点在于", "困境在于")
ORDER_SOLUTION_MARKERS = ("因此", "所以", "应当", "应该", "必须", "要")
ORDER_QUESTION_OPENINGS = ("为什么", "如何", "怎么办", "怎样")
ORDER_QUESTION_MARKERS = ORDER_QUESTION_OPENINGS
ORDER_SUMMARY_CLOSING_MARKERS = ("因此", "所以", "可见", "看来", "总之", "由此")
BENEFIT_RESULT_MARKERS = ("有助于", "有利于", "促进", "推动", "提升", "带来", "实现", "增强", "改善", "夯实", "激发", "降低", "减少", "拓宽", "便利", "惠及", "共赢")
BENEFIT_RESULT_NOUNS = ("效率", "活力", "保障", "安全", "质量", "能力", "收益", "成本", "发展", "共赢", "就业", "便利", "基础", "信心", "满意度", "韧性", "动力")
ACTION_MEASURE_MARKERS = ("通过", "采取", "推动", "完善", "优化", "健全", "构建", "打造", "提供", "推出", "实施", "建立", "强化", "服务", "机制", "举措", "政策")


class MaterialPipelineV2:
    INDEX_VERSION = "v2.index.20260402b"
    SENTENCE_ORDER_FIXED_UNIT_COUNT = 6

    def __init__(self) -> None:
        config_bundle = get_config_bundle()
        self.registry = CardRegistryV2()
        self.paragraph_splitter = DefaultParagraphSplitter()
        self.sentence_splitter = DefaultSentenceSplitter()
        self.genre_classifier = DocumentGenreClassifier(config_bundle.document_genres)
        self.universal_tagger = UniversalTagger()
        self.llm_config = config_bundle.llm
        self.provider = get_llm_provider()
        self.candidate_planner_prompt = read_prompt_file("candidate_planner_v2_prompt.md")

    def build_formal_material_candidates(
        self,
        *,
        article: Any,
        candidate_types: list[str] | set[str] | None = None,
    ) -> dict[str, Any]:
        article_context = self._build_article_context(article)
        if not article_context.get("text"):
            return {
                "generation_mode": "v2_primary",
                "candidate_spans": [],
                "fallback_reason": "empty_clean_text",
            }

        selected_types = self._expand_candidate_types(candidate_types or self._formal_material_candidate_types())
        llm_candidates = self._derive_candidates_with_llm(
            article_context=article_context,
            selected_types=selected_types,
        )
        if not llm_candidates:
            return {
                "generation_mode": "v2_primary",
                "candidate_spans": [],
                "fallback_reason": "llm_candidate_builder_unavailable_or_empty",
            }

        candidate_pool = list(llm_candidates)
        if "functional_slot_unit" in selected_types:
            candidate_pool.extend(
                self._derive_functional_slot_rule_candidates(
                    article_context=article_context,
                )
            )

        planned_candidates = self._plan_candidate_pool(
            article_context=article_context,
            candidates=candidate_pool,
            selected_types=selected_types,
        )
        planned_candidates, candidate_plan_trace = self._apply_formal_candidate_gate(
            article_context=article_context,
            candidates=planned_candidates,
        )
        if not planned_candidates:
            return {
                "generation_mode": "v2_primary",
                "candidate_spans": [],
                "candidate_plan_trace": candidate_plan_trace,
                "fallback_reason": "llm_candidates_failed_planner_gate",
            }

        candidate_spans = [
            self._candidate_span_payload_from_candidate(
                article_context=article_context,
                candidate=candidate,
            )
            for candidate in planned_candidates
        ]
        candidate_spans = [item for item in candidate_spans if item is not None]
        return {
            "generation_mode": "v2_primary",
            "candidate_spans": candidate_spans,
            "candidate_plan_trace": candidate_plan_trace,
            "fallback_reason": None if candidate_spans else "llm_candidates_not_materialized",
        }

    def search(
        self,
        *,
        articles: list[Any],
        business_family_id: str,
        question_card_id: str | None = None,
        business_card_ids: list[str] | None = None,
        preferred_business_card_ids: list[str] | None = None,
        query_terms: list[str] | None = None,
        candidate_limit: int = 20,
        min_card_score: float = 0.55,
        min_business_card_score: float = 0.45,
        target_length: int | None = None,
        length_tolerance: int = 120,
        enable_anchor_adaptation: bool = True,
        preserve_anchor: bool = True,
    ) -> dict[str, Any]:
        question_card = self.registry.get_question_card(question_card_id) if question_card_id else self.registry.get_default_question_card(business_family_id)
        runtime_binding = question_card.get("runtime_binding", {})
        signal_layer = self.registry.get_signal_layer(business_family_id)
        material_cards = self.registry.get_material_cards(business_family_id)
        business_cards = self.registry.get_business_cards(
            business_family_id,
            runtime_question_type=runtime_binding.get("question_type"),
            runtime_business_subtype=runtime_binding.get("business_subtype"),
        )
        requested_business_card_ids = set(business_card_ids or [])
        preferred_business_card_set = set(preferred_business_card_ids or [])
        normalized_query_terms = [term.strip() for term in (query_terms or []) if str(term).strip()]
        required_candidate_types = self._expand_candidate_types(
            question_card.get("upstream_contract", {}).get("required_candidate_types", [])
        )
        items: list[dict[str, Any]] = []
        warnings: list[str] = []
        for article in articles:
            article_context = self._build_article_context(article)
            if not article_context["text"]:
                continue
            candidates = self._derive_candidates(article_context=article_context)
            for candidate in candidates:
                if required_candidate_types and candidate["candidate_type"] not in required_candidate_types:
                    continue
                resolved_candidate = self._adapt_candidate_window(
                    article_context=article_context,
                    candidate=candidate,
                    target_length=target_length,
                    length_tolerance=length_tolerance,
                    enable_anchor_adaptation=enable_anchor_adaptation,
                    preserve_anchor=preserve_anchor,
                )
                neutral_signal_profile = resolved_candidate.get("neutral_signal_profile") or self._build_neutral_signal_profile(article_context=article_context, candidate=resolved_candidate)
                signal_profile = self._project_signal_profile(signal_layer=signal_layer, neutral_signal_profile=neutral_signal_profile)
                business_feature_profile = self._build_business_feature_profile(
                    article_context=article_context,
                    candidate=resolved_candidate,
                    neutral_signal_profile=neutral_signal_profile,
                )
                retrieval_match_profile = self._build_retrieval_match_profile(
                    article_context=article_context,
                    candidate=resolved_candidate,
                    query_terms=normalized_query_terms,
                    target_length=target_length,
                    length_tolerance=length_tolerance,
                )
                card_hits = self._score_material_cards(material_cards=material_cards, signal_profile=signal_profile, candidate=resolved_candidate, min_card_score=min_card_score)
                if not card_hits:
                    continue
                business_card_hits = self._score_business_cards(
                    business_cards=business_cards,
                    business_feature_profile=business_feature_profile,
                    neutral_signal_profile=neutral_signal_profile,
                    requested_business_card_ids=requested_business_card_ids,
                    preferred_business_card_ids=preferred_business_card_set,
                    min_business_card_score=min_business_card_score,
                )
                if requested_business_card_ids and not business_card_hits:
                    continue
                if business_family_id == "sentence_fill" and not business_card_hits:
                    continue
                top_hit = card_hits[0]
                top_business_hit = self._select_primary_business_card(business_card_hits, neutral_signal_profile)
                family_affinity = self._family_affinity_topk(neutral_signal_profile)
                local_profile = dict(signal_profile)
                local_profile["family_affinity_topk"] = family_affinity
                local_profile["distractor_profile"] = self._build_distractor_profile(question_card, top_hit, signal_profile)
                local_profile["business_feature_profile"] = business_feature_profile
                local_profile["retrieval_match_profile"] = retrieval_match_profile
                local_profile["business_card_affinity_topk"] = [
                    {
                        "business_card_id": item["business_card_id"],
                        "score": item["score"],
                    }
                    for item in business_card_hits[:3]
                ]
                presentation = self._build_presentation(
                    business_family_id=business_family_id,
                    article_context=article_context,
                    candidate=resolved_candidate,
                    signal_profile=signal_profile,
                )
                consumable_text = self._build_consumable_text(
                    business_family_id=business_family_id,
                    candidate=resolved_candidate,
                    presentation=presentation,
                )
                items.append(
                    {
                        "candidate_id": resolved_candidate["candidate_id"],
                        "article_id": article_context["article_id"],
                        "article_title": article_context["title"],
                        "candidate_type": resolved_candidate["candidate_type"],
                        "material_card_id": top_hit["card_id"],
                        "selected_business_card": top_business_hit["business_card_id"] if top_business_hit else None,
                        "text": resolved_candidate["text"],
                        "original_text": candidate["text"],
                        "meta": resolved_candidate.get("meta", {}),
                        "consumable_text": consumable_text,
                        "presentation": presentation,
                        "source": article_context["source"],
                        "article_profile": article_context["article_profile"],
                        "neutral_signal_profile": neutral_signal_profile,
                        "task_scoring": neutral_signal_profile.get("task_scoring", {}),
                        "selected_task_scoring": (neutral_signal_profile.get("task_scoring", {}) or {}).get(self._task_family_scoring_key(business_family_id) or "", {}),
                        "business_feature_profile": business_feature_profile,
                        "retrieval_match_profile": retrieval_match_profile,
                        "local_profile": local_profile,
                        "family_affinity_topk": family_affinity,
                        "eligible_material_cards": card_hits,
                        "material_card_recommendations": [item["card_id"] for item in card_hits],
                        "eligible_business_cards": business_card_hits,
                        "business_card_recommendations": [item["business_card_id"] for item in business_card_hits],
                        "preferred_question_cards": [question_card["card_id"]],
                        "question_ready_context": {
                            "question_card_id": question_card["card_id"],
                            "runtime_binding": runtime_binding,
                            "selected_material_card": top_hit["card_id"],
                            "selected_business_card": top_business_hit["business_card_id"] if top_business_hit else None,
                            "generation_archetype": top_hit["generation_archetype"],
                            "resolved_slots": self._resolve_slots(question_card, top_hit["card_id"], top_business_hit),
                            "pattern_candidates": list((top_business_hit or {}).get("pattern_candidates") or []),
                            "prompt_extras": self._build_prompt_extras(top_business_hit),
                            "validator_contract": question_card.get("validator_contract", {}),
                        },
                        "quality_flags": candidate.get("quality_flags", []),
                        "quality_score": round(
                            self._score_candidate_quality(
                                business_family_id=business_family_id,
                                signal_profile=signal_profile,
                                top_card_score=top_hit["score"],
                                top_business_score=top_business_hit["score"] if top_business_hit else 0.0,
                                retrieval_match_score=float(retrieval_match_profile.get("match_score") or 0.0),
                                length_fit_score=float(retrieval_match_profile.get("length_fit_score") or 0.0),
                                candidate=resolved_candidate,
                                article_context=article_context,
                            ),
                            4,
                        ),
                    }
                )
        ranked = self._select_diverse_items(items, candidate_limit)
        if not ranked:
            warnings.append("No v2 candidates met the current card score threshold.")
        return {
            "question_card": {"card_id": question_card["card_id"], "business_family_id": question_card["business_family_id"], "business_subtype_id": question_card["business_subtype_id"], "runtime_binding": runtime_binding},
            "available_business_cards": [
                {
                    "business_card_id": (card.get("card_meta") or {}).get("business_card_id"),
                    "display_name": (card.get("card_meta") or {}).get("display_name") or (card.get("card_meta") or {}).get("business_label"),
                    "mother_family_id": (card.get("card_meta") or {}).get("mother_family_id"),
                    "business_subtype": (card.get("card_meta") or {}).get("business_subtype"),
                }
                for card in business_cards
            ],
            "items": ranked,
            "warnings": warnings,
        }

    def build_cached_item_from_material(
        self,
        *,
        material: Any,
        article: Any,
        business_family_id: str,
    ) -> dict[str, Any] | None:
        text = str(getattr(material, "text", "") or "").strip()
        if not text:
            return None
        article_context = self._build_material_context(material=material, article=article)
        question_card = self.registry.get_default_question_card(business_family_id)
        runtime_binding = question_card.get("runtime_binding", {})
        signal_layer = self.registry.get_signal_layer(business_family_id)
        material_cards = self.registry.get_material_cards(business_family_id)
        business_cards = self.registry.get_business_cards(
            business_family_id,
            runtime_question_type=runtime_binding.get("question_type"),
            runtime_business_subtype=runtime_binding.get("business_subtype"),
        )
        candidate = {
            "candidate_id": str(getattr(material, "id", "")),
            "candidate_type": str(getattr(material, "span_type", "") or "material_span"),
            "text": text,
            "meta": {
                "precomputed_from_material": True,
                "candidate_span_id": str(getattr(material, "candidate_span_id", "") or ""),
                "paragraph_range": [0, max(0, int(getattr(material, "paragraph_count", 1) or 1) - 1)],
                "sentence_range": [0, max(0, int(getattr(material, "sentence_count", 1) or 1) - 1)],
                "source_paragraph_range_original": [
                    max(0, int(getattr(material, "start_paragraph", 0) or 0)),
                    max(0, int(getattr(material, "end_paragraph", max(0, int(getattr(material, "paragraph_count", 1) or 1) - 1)) or 0)),
                ],
                "source_sentence_range_original": [
                    max(0, int(getattr(material, "start_sentence", 0) or 0)),
                    max(0, int(getattr(material, "end_sentence", max(0, int(getattr(material, "sentence_count", 1) or 1) - 1)) or 0)),
                ],
                "anchor_adaptation": {
                    "adapted": False,
                    "reason": "precomputed_material_text",
                },
            },
            "quality_flags": list(getattr(material, "quality_flags", []) or []),
        }
        if candidate["candidate_type"] == "functional_slot_unit":
            candidate["meta"].update(
                self._hydrate_functional_slot_meta(
                    article_context=article_context,
                    candidate=candidate,
                )
            )
        neutral_signal_profile = self._build_neutral_signal_profile(article_context=article_context, candidate=candidate)
        signal_profile = self._project_signal_profile(signal_layer=signal_layer, neutral_signal_profile=neutral_signal_profile)
        business_feature_profile = self._build_business_feature_profile(
            article_context=article_context,
            candidate=candidate,
            neutral_signal_profile=neutral_signal_profile,
        )
        retrieval_match_profile = self._build_retrieval_match_profile(
            article_context=article_context,
            candidate=candidate,
            query_terms=[],
            target_length=None,
            length_tolerance=120,
        )
        card_hits = self._score_material_cards(
            material_cards=material_cards,
            signal_profile=signal_profile,
            candidate=candidate,
            min_card_score=0.30,
        )
        if not card_hits:
            card_hits = [
                {
                    "card_id": f"legacy.{business_family_id}.precomputed",
                    "score": 0.35,
                    "generation_archetype": "legacy_material_fallback",
                }
            ]
        business_card_hits = self._score_business_cards(
            business_cards=business_cards,
            business_feature_profile=business_feature_profile,
            neutral_signal_profile=neutral_signal_profile,
            requested_business_card_ids=set(),
            preferred_business_card_ids=set(),
            min_business_card_score=0.25,
        )
        top_hit = card_hits[0]
        top_business_hit = self._select_primary_business_card(business_card_hits, neutral_signal_profile)
        if business_family_id == "sentence_fill" and top_business_hit is None:
            return None
        family_affinity = self._family_affinity_topk(neutral_signal_profile)
        local_profile = dict(signal_profile)
        local_profile["family_affinity_topk"] = family_affinity
        local_profile["distractor_profile"] = self._build_distractor_profile(question_card, top_hit, signal_profile)
        local_profile["business_feature_profile"] = business_feature_profile
        local_profile["retrieval_match_profile"] = retrieval_match_profile
        local_profile["business_card_affinity_topk"] = [
            {
                "business_card_id": item["business_card_id"],
                "score": item["score"],
            }
            for item in business_card_hits[:3]
        ]
        presentation = self._build_presentation(
            business_family_id=business_family_id,
            article_context=article_context,
            candidate=candidate,
            signal_profile=signal_profile,
        )
        consumable_text = self._build_consumable_text(
            business_family_id=business_family_id,
            candidate=candidate,
            presentation=presentation,
        )
        return {
            "candidate_id": candidate["candidate_id"],
            "article_id": article_context["article_id"],
            "article_title": article_context["title"],
            "candidate_type": candidate["candidate_type"],
            "material_card_id": top_hit["card_id"],
            "selected_business_card": top_business_hit["business_card_id"] if top_business_hit else None,
            "text": candidate["text"],
            "original_text": candidate["text"],
            "meta": candidate["meta"],
            "consumable_text": consumable_text,
            "presentation": presentation,
            "source": article_context["source"],
            "article_profile": article_context["article_profile"],
            "neutral_signal_profile": neutral_signal_profile,
            "task_scoring": neutral_signal_profile.get("task_scoring", {}),
            "selected_task_scoring": (neutral_signal_profile.get("task_scoring", {}) or {}).get(self._task_family_scoring_key(business_family_id) or "", {}),
            "business_feature_profile": business_feature_profile,
            "retrieval_match_profile": retrieval_match_profile,
            "local_profile": local_profile,
            "family_affinity_topk": family_affinity,
            "eligible_material_cards": card_hits,
            "material_card_recommendations": [item["card_id"] for item in card_hits],
            "eligible_business_cards": business_card_hits,
            "business_card_recommendations": [item["business_card_id"] for item in business_card_hits],
            "preferred_question_cards": [question_card["card_id"]],
            "question_ready_context": {
                "question_card_id": question_card["card_id"],
                "runtime_binding": runtime_binding,
                "selected_material_card": top_hit["card_id"],
                "selected_business_card": top_business_hit["business_card_id"] if top_business_hit else None,
                "generation_archetype": top_hit["generation_archetype"],
                "resolved_slots": self._resolve_slots(question_card, top_hit["card_id"], top_business_hit),
                "pattern_candidates": list((top_business_hit or {}).get("pattern_candidates") or []),
                "prompt_extras": self._build_prompt_extras(top_business_hit),
                "validator_contract": question_card.get("validator_contract", {}),
            },
            "quality_flags": candidate.get("quality_flags", []),
            "quality_score": round(
                self._score_candidate_quality(
                    business_family_id=business_family_id,
                    signal_profile=signal_profile,
                    top_card_score=top_hit["score"],
                    top_business_score=top_business_hit["score"] if top_business_hit else 0.0,
                    retrieval_match_score=0.0,
                    length_fit_score=0.0,
                    candidate=candidate,
                    article_context=article_context,
                ),
                4,
            ),
            "_cached_business_family_id": business_family_id,
            "_cached_index_version": self.INDEX_VERSION,
        }

    def refresh_cached_item(
        self,
        *,
        cached_item: dict[str, Any],
        query_terms: list[str] | None = None,
        target_length: int | None = None,
        length_tolerance: int = 120,
        enable_anchor_adaptation: bool = True,
        preserve_anchor: bool = True,
    ) -> dict[str, Any]:
        item = deepcopy(cached_item)
        candidate = {
            "candidate_id": str(item.get("candidate_id") or ""),
            "candidate_type": str(item.get("candidate_type") or "material_span"),
            "text": str(item.get("original_text") or item.get("text") or ""),
            "meta": deepcopy(item.get("meta") or {}),
        }
        article_context = self._build_cached_article_context(item)
        adapted = self._adapt_cached_candidate(
            candidate=candidate,
            query_terms=query_terms or [],
            target_length=target_length,
            length_tolerance=length_tolerance,
            enable_anchor_adaptation=enable_anchor_adaptation,
            preserve_anchor=preserve_anchor,
            theme_words=list((item.get("business_feature_profile") or {}).get("theme_words") or []),
        )
        item["text"] = adapted["text"]
        item["meta"] = adapted["meta"]
        item["consumable_text"] = adapted["text"]
        question_ready_context = dict(item.get("question_ready_context") or {})
        selected_business_card = str(question_ready_context.get("selected_business_card") or "")
        selected_runtime_family = str(((question_ready_context.get("runtime_binding") or {}).get("question_type")) or "")
        local_profile = dict(item.get("local_profile") or {})
        if selected_runtime_family == "sentence_order" or selected_business_card.startswith("sentence_order__"):
            order_presentation = self._build_sentence_order_presentation(
                article_context=article_context,
                candidate=adapted,
                signal_profile=local_profile,
            )
            item["presentation"] = order_presentation
            item["consumable_text"] = self._build_consumable_text(
                business_family_id="sentence_order",
                candidate=adapted,
                presentation=order_presentation,
            )
        if selected_business_card.startswith("sentence_fill__"):
            fill_presentation = self._build_sentence_fill_presentation(
                candidate=adapted,
                signal_profile=local_profile,
            )
            item["presentation"] = fill_presentation
            item["consumable_text"] = str(fill_presentation.get("blanked_text") or adapted["text"])
            prompt_extras = dict(question_ready_context.get("prompt_extras") or {})
            prompt_extras.update(fill_presentation)
            question_ready_context["prompt_extras"] = prompt_extras
            item["question_ready_context"] = question_ready_context
        item["retrieval_match_profile"] = self._build_retrieval_match_profile(
            article_context=article_context,
            candidate=adapted,
            query_terms=query_terms or [],
            target_length=target_length,
            length_tolerance=length_tolerance,
        )
        return item

    def _build_article_context(self, article: Any) -> dict[str, Any]:
        text = self._sanitize_article_text((getattr(article, "clean_text", None) or getattr(article, "raw_text", None) or "").strip())
        paragraphs = self.paragraph_splitter.split(text) if text else []
        sentences = self._split_sentences(paragraphs)
        paragraph_sentences: list[list[str]] = []
        paragraph_sentence_offsets: list[int] = []
        sentence_offset = 0
        for paragraph in paragraphs:
            local_sentences = [sentence for sentence in self.sentence_splitter.split(paragraph) if sentence.strip()]
            paragraph_sentences.append(local_sentences)
            paragraph_sentence_offsets.append(sentence_offset)
            sentence_offset += len(local_sentences)
        universal = self.universal_tagger._heuristic_tag(self._build_span(article_id=str(article.id), span_id=f"{article.id}:whole", text=text, paragraph_count=max(1, len(paragraphs)), sentence_count=max(1, len(sentences)), source_domain=getattr(article, "domain", None)))
        genre = self.genre_classifier.classify(title=getattr(article, "title", None), text=text, source=getattr(article, "source", None))
        article_profile = {
            "document_genre": genre["document_genre"],
            "document_genre_candidates": genre["document_genre_candidates"],
            "article_purpose_frame": self._article_purpose_frame(universal, text),
            "discourse_shape": self._discourse_shape(universal, text),
            "core_object": self._core_object(getattr(article, "title", None), text),
            "global_main_claim": self._global_main_claim(sentences),
            "closure_score": self._closure_score(universal, text),
            "context_dependency": round(max(0.0, 1 - universal.independence_score), 4),
            "paragraph_count": len(paragraphs),
            "sentence_count": len(sentences),
        }
        return {
            "article_id": str(article.id),
            "title": getattr(article, "title", None),
            "source": {
                "source_name": getattr(article, "source", None),
                "source_url": getattr(article, "source_url", None),
                "domain": getattr(article, "domain", None),
            },
            "text": text,
            "paragraphs": paragraphs,
            "paragraph_sentences": paragraph_sentences,
            "paragraph_sentence_offsets": paragraph_sentence_offsets,
            "sentences": sentences,
            "article_profile": article_profile,
        }

    def _formal_material_candidate_types(self) -> tuple[str, ...]:
        return ("whole_passage", "closed_span", "multi_paragraph_unit", "functional_slot_unit", "ordered_unit_group")

    def _apply_formal_candidate_gate(
        self,
        *,
        article_context: dict[str, Any],
        candidates: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        focused_types = {"closed_span", "multi_paragraph_unit", "functional_slot_unit"}
        has_focused_alternatives = any(item.get("candidate_type") in focused_types for item in candidates)
        kept: list[dict[str, Any]] = []
        trace: list[dict[str, Any]] = []
        for candidate in candidates:
            if candidate.get("candidate_type") != "whole_passage":
                kept.append(candidate)
                continue
            allowed, reason = self._allow_whole_passage_candidate(
                article_context=article_context,
                candidate=candidate,
                has_focused_alternatives=has_focused_alternatives,
            )
            meta = dict(candidate.get("meta") or {})
            meta["whole_passage_gate"] = "allowed" if allowed else "rejected"
            meta["whole_passage_gate_reason"] = reason
            candidate = {**candidate, "meta": meta}
            if allowed:
                kept.append(candidate)
                continue
            trace.append(
                {
                    "candidate_type": "whole_passage",
                    "candidate_id": candidate.get("candidate_id"),
                    "reason": reason,
                    "paragraph_range": meta.get("paragraph_range"),
                    "planner_score": meta.get("planner_score"),
                }
            )
        return kept, trace

    def _allow_whole_passage_candidate(
        self,
        *,
        article_context: dict[str, Any],
        candidate: dict[str, Any],
        has_focused_alternatives: bool,
    ) -> tuple[bool, str]:
        text = str(candidate.get("text") or "").strip()
        paragraph_count = max(1, text.count("\n\n") + 1)
        char_count = len(text)
        signal_profile = candidate.get("neutral_signal_profile") or {}
        closure = float(signal_profile.get("closure_score") or 0.0)
        single_center = float(signal_profile.get("single_center_strength") or 0.0)
        context_dependency = float(signal_profile.get("context_dependency") or 0.0)
        branch_focus = float(signal_profile.get("branch_focus_strength") or 0.0)
        parallel_strength = float(signal_profile.get("parallel_enumeration_strength") or 0.0)
        summary_strength = float(signal_profile.get("summary_strength") or 0.0)
        titleability = float(signal_profile.get("titleability") or 0.0)
        article_profile = article_context.get("article_profile") or {}
        article_paragraph_count = int(article_profile.get("paragraph_count") or paragraph_count)
        article_char_count = int(article_profile.get("char_count") or char_count)

        if paragraph_count > 4 or char_count > 960 or article_paragraph_count > 4 or article_char_count > 960:
            return False, "whole_passage_too_wide"
        if context_dependency > 0.18:
            return False, "whole_passage_high_context_dependency"
        if closure < 0.72:
            return False, "whole_passage_low_closure"
        if single_center < 0.78:
            return False, "whole_passage_low_focus"
        if branch_focus > 0.40 or parallel_strength > 0.34:
            return False, "whole_passage_multi_branch"

        compact_whole_passage = paragraph_count <= 2 and char_count <= 680
        strong_whole_passage = summary_strength >= 0.68 and titleability >= 0.62
        if has_focused_alternatives and not compact_whole_passage:
            return False, "whole_passage_prefers_focused_units"
        if has_focused_alternatives and not strong_whole_passage:
            return False, "whole_passage_prefers_focused_units"
        return True, "whole_passage_gate_allowed"

    def _candidate_span_payload_from_candidate(
        self,
        *,
        article_context: dict[str, Any],
        candidate: dict[str, Any],
    ) -> dict[str, Any] | None:
        meta = dict(candidate.get("meta") or {})
        paragraph_range = list(meta.get("paragraph_range") or [0, 0])
        if len(paragraph_range) != 2:
            paragraph_range = [0, 0]
        start_paragraph = max(0, int(paragraph_range[0] or 0))
        end_paragraph = max(start_paragraph, int(paragraph_range[1] or start_paragraph))
        sentence_range = meta.get("sentence_range")
        if sentence_range is None:
            sentence_range = self._sentence_range_from_paragraph_range(
                article_context=article_context,
                start_paragraph=start_paragraph,
                end_paragraph=end_paragraph,
            )

        start_sentence = None
        end_sentence = None
        if sentence_range:
            start_sentence = max(0, int(sentence_range[0]))
            end_sentence = max(start_sentence, int(sentence_range[1]))

        planner_source = str(meta.get("planner_source") or "rule_candidate_builder").strip()
        return {
            "start_paragraph": start_paragraph,
            "end_paragraph": end_paragraph,
            "start_sentence": start_sentence,
            "end_sentence": end_sentence,
            "span_type": str(candidate.get("candidate_type") or "closed_span"),
            "text": str(candidate.get("text") or "").strip(),
            "generated_by": self._candidate_generated_by(candidate, planner_source=planner_source),
            "status": "new",
            "segmentation_version": self.INDEX_VERSION,
        }

    def _candidate_generated_by(self, candidate: dict[str, Any], *, planner_source: str) -> str:
        meta = dict(candidate.get("meta") or {})
        parts = ["v2_primary_candidate_builder", planner_source]
        if candidate.get("candidate_type") == "functional_slot_unit":
            slot_role = str(meta.get("slot_role") or "").strip()
            slot_function = str(meta.get("slot_function") or "").strip()
            slot_sentence_range = list(meta.get("slot_sentence_range") or [])
            if slot_role:
                parts.append(f"slot_role={slot_role}")
            if slot_function:
                parts.append(f"slot_function={slot_function}")
            if len(slot_sentence_range) == 2:
                parts.append(f"slot_sentence={slot_sentence_range[0]}-{slot_sentence_range[1]}")
        if candidate.get("candidate_type") == "ordered_unit_group":
            grouped_count = int(meta.get("grouped_unit_count") or 0)
            group_size = int(meta.get("group_size") or 0)
            if group_size:
                parts.append(f"group_size={group_size}")
            if grouped_count:
                parts.append(f"grouped_units={grouped_count}")
        return "+".join(parts)

    def _hydrate_functional_slot_meta(
        self,
        *,
        article_context: dict[str, Any] | None,
        candidate: dict[str, Any],
    ) -> dict[str, Any]:
        if candidate.get("candidate_type") != "functional_slot_unit":
            return {}
        meta = dict(candidate.get("meta") or {})
        if meta.get("slot_role") and meta.get("slot_function"):
            return meta

        paragraph_range = list(
            meta.get("source_paragraph_range_original")
            or meta.get("slot_context_paragraph_range")
            or meta.get("paragraph_range")
            or []
        )
        article_profile = (article_context or {}).get("article_profile") or {}
        article_paragraph_count = int(article_profile.get("paragraph_count") or 0)
        slot_role = str(meta.get("slot_role") or "").strip()
        if not slot_role:
            if paragraph_range and int(paragraph_range[0]) == 0:
                slot_role = "opening"
            elif paragraph_range and article_paragraph_count and int(paragraph_range[-1]) >= article_paragraph_count - 1:
                slot_role = "ending"
            else:
                slot_role = "middle"

        slot_function = str(meta.get("slot_function") or "").strip()
        if not slot_function:
            slot_function = self._infer_functional_slot_function(
                slot_role=slot_role,
                slot_text=str(candidate.get("text") or ""),
                context_text=str(candidate.get("text") or ""),
            )

        hydrated = dict(meta)
        hydrated["unit_type"] = "functional_slot_unit"
        hydrated["slot_role"] = slot_role
        hydrated["slot_function"] = slot_function
        hydrated.setdefault("slot_context_paragraph_range", paragraph_range or None)
        sentence_range = meta.get("sentence_range")
        if sentence_range and not hydrated.get("slot_sentence_range"):
            hydrated["slot_sentence_range"] = list(sentence_range)
        return hydrated

    def _sentence_range_from_paragraph_range(
        self,
        *,
        article_context: dict[str, Any],
        start_paragraph: int,
        end_paragraph: int,
    ) -> list[int] | None:
        paragraph_sentences: list[list[str]] = article_context.get("paragraph_sentences") or []
        paragraph_sentence_offsets: list[int] = article_context.get("paragraph_sentence_offsets") or []
        if not paragraph_sentences or not paragraph_sentence_offsets:
            return None
        if start_paragraph >= len(paragraph_sentences) or end_paragraph >= len(paragraph_sentences):
            return None
        if not paragraph_sentences[start_paragraph] or not paragraph_sentences[end_paragraph]:
            return None
        start_sentence = paragraph_sentence_offsets[start_paragraph]
        end_sentence = paragraph_sentence_offsets[end_paragraph] + len(paragraph_sentences[end_paragraph]) - 1
        return [start_sentence, end_sentence]

    def _build_material_context(self, *, material: Any, article: Any) -> dict[str, Any]:
        text = self._sanitize_article_text((getattr(material, "text", None) or "").strip())
        paragraphs = self.paragraph_splitter.split(text) if text else []
        sentences = self._split_sentences(paragraphs)
        universal = self.universal_tagger._heuristic_tag(
            self._build_span(
                article_id=str(getattr(article, "id", "")),
                span_id=str(getattr(material, "id", "")),
                text=text,
                paragraph_count=max(1, len(paragraphs)),
                sentence_count=max(1, len(sentences)),
                source_domain=getattr(article, "domain", None),
            )
        )
        genre = self.genre_classifier.classify(
            title=getattr(article, "title", None),
            text=text,
            source=getattr(article, "source", None),
        )
        feature_profile = dict(getattr(material, "feature_profile", {}) or {})
        universal_profile = dict(getattr(material, "universal_profile", {}) or {})
        article_profile = {
            "document_genre": feature_profile.get("document_genre") or universal_profile.get("document_genre") or genre["document_genre"],
            "document_genre_candidates": feature_profile.get("document_genre_candidates") or universal_profile.get("document_genre_candidates") or genre["document_genre_candidates"],
            "article_purpose_frame": self._article_purpose_frame(universal, text),
            "discourse_shape": feature_profile.get("material_structure_label") or universal_profile.get("material_structure_label") or self._discourse_shape(universal, text),
            "core_object": self._core_object(getattr(article, "title", None), text),
            "global_main_claim": self._global_main_claim(sentences),
            "closure_score": self._closure_score(universal, text),
            "context_dependency": round(max(0.0, 1 - universal.independence_score), 4),
            "paragraph_count": len(paragraphs),
            "sentence_count": len(sentences),
        }
        source = dict(getattr(material, "source", None) or {})
        source.setdefault("source_name", getattr(article, "source", None))
        source.setdefault("source_url", getattr(article, "source_url", None))
        source.setdefault("domain", getattr(article, "domain", None))
        return {
            "article_id": str(getattr(article, "id", "")),
            "title": getattr(article, "title", None),
            "source": source,
            "text": text,
            "paragraphs": paragraphs,
            "paragraph_sentences": [[sentence for sentence in self.sentence_splitter.split(paragraph) if sentence.strip()] for paragraph in paragraphs],
            "paragraph_sentence_offsets": [],
            "sentences": sentences,
            "article_profile": article_profile,
        }

    def _build_cached_article_context(self, item: dict[str, Any]) -> dict[str, Any]:
        text = str(item.get("original_text") or item.get("text") or "")
        paragraphs = self.paragraph_splitter.split(text) if text else []
        return {
            "article_id": str(item.get("article_id") or ""),
            "title": item.get("article_title"),
            "source": dict(item.get("source") or {}),
            "text": text,
            "paragraphs": paragraphs,
            "paragraph_sentences": [[sentence for sentence in self.sentence_splitter.split(paragraph) if sentence.strip()] for paragraph in paragraphs],
            "paragraph_sentence_offsets": [],
            "sentences": [sentence for sentence in self.sentence_splitter.split(text) if sentence.strip()],
            "article_profile": dict(item.get("article_profile") or {}),
        }

    def _derive_candidates(
        self,
        *,
        article_context: dict[str, Any],
        candidate_types: list[str] | set[str] | None = None,
        required_candidate_types: list[str] | None = None,
        business_family_id: str | None = None,
    ) -> list[dict[str, Any]]:
        selected_types = self._expand_candidate_types(candidate_types or required_candidate_types or self._supported_candidate_types())
        llm_candidates = self._derive_candidates_with_llm(article_context=article_context, selected_types=selected_types)
        heuristic_candidates = self._derive_rule_candidates(article_context=article_context, selected_types=selected_types)
        candidate_pool = llm_candidates + heuristic_candidates
        if not candidate_pool:
            return []
        return self._plan_candidate_pool(article_context=article_context, candidates=candidate_pool, selected_types=selected_types)

    def _expand_candidate_types(self, candidate_types: list[str] | set[str] | tuple[str, ...]) -> set[str]:
        selected = {str(item) for item in candidate_types if str(item)}
        if {"closed_span", "multi_paragraph_unit"} & selected:
            selected.add("functional_slot_unit")
        if "sentence_block_group" in selected:
            selected.add("ordered_unit_group")
        return selected

    def _derive_rule_candidates(
        self,
        *,
        article_context: dict[str, Any],
        selected_types: set[str],
    ) -> list[dict[str, Any]]:
        paragraphs: list[str] = article_context["paragraphs"]
        paragraph_sentences: list[list[str]] = article_context.get("paragraph_sentences") or []
        paragraph_sentence_offsets: list[int] = article_context.get("paragraph_sentence_offsets") or []
        sentences: list[str] = article_context["sentences"]
        article_id = article_context["article_id"]
        article_paragraph_count = len(paragraphs)
        candidates: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()

        def add_candidate(candidate_type: str, text: str, meta: dict[str, Any]) -> None:
            body = text.strip()
            if not body:
                return
            key = (candidate_type, body)
            if key in seen:
                return
            seen.add(key)
            quality_flags = []
            if body.startswith(CONTEXTUAL_OPENINGS):
                quality_flags.append("context_opening")
            if not body.endswith(("。", "！", "？", "!", "?")):
                quality_flags.append("missing_terminal_punctuation")
            candidates.append({"candidate_id": f"{article_id}:{candidate_type}:{len(candidates) + 1}", "candidate_type": candidate_type, "text": body, "meta": meta, "quality_flags": quality_flags})

        if "whole_passage" in selected_types and article_context["text"]:
            if article_paragraph_count <= 8 and len(article_context["text"]) <= 1600:
                add_candidate("whole_passage", article_context["text"], {"paragraph_range": [0, max(len(paragraphs) - 1, 0)]})
        if "closed_span" in selected_types:
            for start in range(len(paragraphs)):
                for window in (1, 2):
                    chunk = paragraphs[start : start + window]
                    if chunk and len("\n\n".join(chunk)) >= 90:
                        add_candidate("closed_span", "\n\n".join(chunk), {"paragraph_range": [start, start + len(chunk) - 1]})
        if "multi_paragraph_unit" in selected_types:
            windows = (2, 3)
            for start in range(len(paragraphs)):
                for window in windows:
                    chunk = paragraphs[start : start + window]
                    joined = "\n\n".join(chunk)
                    if len(chunk) >= 2 and len(joined) >= 140:
                        if len(joined) > 1200:
                            continue
                        if self._has_repeated_enumerative_openings(chunk):
                            continue
                        add_candidate("multi_paragraph_unit", joined, {"paragraph_range": [start, start + len(chunk) - 1]})
        if "functional_slot_unit" in selected_types:
            for candidate in self._derive_functional_slot_rule_candidates(article_context=article_context):
                add_candidate(
                    "functional_slot_unit",
                    candidate["text"],
                    candidate.get("meta") or {},
                )
        if "ordered_unit_group" in selected_types:
            for candidate in self._derive_ordered_unit_group_candidates(article_context=article_context):
                add_candidate(
                    "ordered_unit_group",
                    candidate["text"],
                    candidate.get("meta") or {},
                )
        if "sentence_block_group" in selected_types:
            for paragraph_index, paragraph in enumerate(paragraphs):
                local_sentences = paragraph_sentences[paragraph_index] if paragraph_index < len(paragraph_sentences) else [sentence for sentence in self.sentence_splitter.split(paragraph) if sentence.strip()]
                sentence_offset = paragraph_sentence_offsets[paragraph_index] if paragraph_index < len(paragraph_sentence_offsets) else 0
                if len(local_sentences) < self.SENTENCE_ORDER_FIXED_UNIT_COUNT:
                    continue
                joined = "".join(local_sentences)
                if any(marker in paragraph for marker in "①②③④⑤⑥⑦⑧⑨⑩") and 120 <= len(joined) <= 460 and self._sentence_order_unit_count(joined, "sentence_block_group") == self.SENTENCE_ORDER_FIXED_UNIT_COUNT:
                    add_candidate(
                        "sentence_block_group",
                        joined,
                        {
                            "paragraph_range": [paragraph_index, paragraph_index],
                            "sentence_range": [sentence_offset, sentence_offset + len(local_sentences) - 1],
                            "composition": "single_paragraph_full",
                        },
                    )
                for window in (self.SENTENCE_ORDER_FIXED_UNIT_COUNT,):
                    for start in range(0, max(len(local_sentences) - window + 1, 0)):
                        chunk = local_sentences[start : start + window]
                        body = "".join(chunk)
                        if len(chunk) == self.SENTENCE_ORDER_FIXED_UNIT_COUNT and 120 <= len(body) <= 420 and self._sentence_order_unit_count(body, "sentence_block_group") == self.SENTENCE_ORDER_FIXED_UNIT_COUNT:
                            add_candidate(
                                "sentence_block_group",
                                body,
                                {
                                    "paragraph_range": [paragraph_index, paragraph_index],
                                    "sentence_range": [sentence_offset + start, sentence_offset + start + len(chunk) - 1],
                                    "composition": "single_paragraph_window",
                                },
                            )
            for paragraph_index in range(max(0, len(paragraph_sentences) - 1)):
                left_sentences = paragraph_sentences[paragraph_index]
                right_sentences = paragraph_sentences[paragraph_index + 1]
                if len(left_sentences) < 2 or len(right_sentences) < 2:
                    continue
                left_offset = paragraph_sentence_offsets[paragraph_index]
                right_offset = paragraph_sentence_offsets[paragraph_index + 1]
                for left_count in (2, 3, 4):
                    for right_count in (2, 3, 4):
                        if left_count > len(left_sentences) or right_count > len(right_sentences):
                            continue
                        combined = left_sentences[-left_count:] + right_sentences[:right_count]
                        if len(combined) != self.SENTENCE_ORDER_FIXED_UNIT_COUNT:
                            continue
                        body = "".join(combined)
                        if not (140 <= len(body) <= 520):
                            continue
                        if self._sentence_order_unit_count(body, "sentence_block_group") != self.SENTENCE_ORDER_FIXED_UNIT_COUNT:
                            continue
                        add_candidate(
                            "sentence_block_group",
                            body,
                            {
                                "paragraph_range": [paragraph_index, paragraph_index + 1],
                                "sentence_range": [left_offset + len(left_sentences) - left_count, right_offset + right_count - 1],
                                "composition": "adjacent_paragraph_pair",
                            },
                        )
            for start in range(0, max(len(sentences) - self.SENTENCE_ORDER_FIXED_UNIT_COUNT + 1, 0)):
                chunk = [sentence.strip() for sentence in sentences[start : start + self.SENTENCE_ORDER_FIXED_UNIT_COUNT] if sentence.strip()]
                if len(chunk) != self.SENTENCE_ORDER_FIXED_UNIT_COUNT:
                    continue
                body = "".join(chunk)
                if not (140 <= len(body) <= 520):
                    continue
                if self._sentence_order_unit_count(body, "sentence_block_group") != self.SENTENCE_ORDER_FIXED_UNIT_COUNT:
                    continue
                add_candidate(
                    "sentence_block_group",
                    body,
                    {
                        "sentence_range": [start, start + self.SENTENCE_ORDER_FIXED_UNIT_COUNT - 1],
                        "composition": "global_sentence_window",
                    },
                )
        if "insertion_context_unit" in selected_types:
            for start in range(0, max(len(sentences) - 1, 1)):
                chunk = sentences[start : start + 3]
                if len(chunk) >= 2:
                    add_candidate("insertion_context_unit", "".join(chunk), {"sentence_range": [start, start + len(chunk) - 1]})
        if "phrase_or_clause_group" in selected_types:
            for sentence in sentences:
                clauses = [part.strip() for part in re.split(r"[，、；,;]", sentence) if part.strip()]
                if len(clauses) >= 3:
                    add_candidate("phrase_or_clause_group", "，".join(clauses[:5]), {"clause_count": len(clauses[:5]), "composition": "single_sentence_clause_group"})
        return candidates

    def _derive_ordered_unit_group_candidates(
        self,
        *,
        article_context: dict[str, Any],
    ) -> list[dict[str, Any]]:
        sentences: list[str] = [sentence.strip() for sentence in article_context.get("sentences") or [] if sentence.strip()]
        if len(sentences) < self.SENTENCE_ORDER_FIXED_UNIT_COUNT:
            return []

        candidates: list[dict[str, Any]] = []
        article_id = str(article_context.get("article_id") or "")

        for start in range(0, max(len(sentences) - self.SENTENCE_ORDER_FIXED_UNIT_COUNT + 1, 0)):
            for raw_count in (6, 7, 8):
                raw_units = sentences[start : start + raw_count]
                if len(raw_units) != raw_count:
                    continue
                normalized = self._normalize_ordered_units_to_six(raw_units)
                if normalized is None:
                    continue
                normalized_units, unit_forms, local_bindings, normalization_reason = normalized
                if len(normalized_units) != self.SENTENCE_ORDER_FIXED_UNIT_COUNT:
                    continue
                worthwhile, ordering_reason, pairwise_constraints, first_candidate_indices, last_candidate_indices = self._ordered_unit_group_worthwhile(
                    normalized_units
                )
                if not worthwhile:
                    continue
                body = "\n".join(unit.strip() for unit in normalized_units if unit.strip()).strip()
                if not body:
                    continue
                if len(body) < 120 or len(body) > 520:
                    continue
                sentence_range = [start, start + raw_count - 1]
                paragraph_range = self._paragraph_range_for_sentence_range(article_context=article_context, sentence_range=sentence_range)
                grouped_unit_count = sum(1 for form in unit_forms if form == "grouped_unit")
                meta = {
                    "paragraph_range": paragraph_range or [0, 0],
                    "sentence_range": sentence_range,
                    "planner_source": "rule_ordered_unit_group_builder",
                    "planner_priority": round(
                        0.74
                        + 0.04 * len(pairwise_constraints)
                        + 0.03 * len(local_bindings)
                        + 0.02 * grouped_unit_count,
                        4,
                    ),
                    "planner_reason": f"ordered_unit_group:{ordering_reason}",
                    "unit_type": "ordered_unit_group",
                    "group_size": self.SENTENCE_ORDER_FIXED_UNIT_COUNT,
                    "ordered_units": normalized_units,
                    "unit_forms": unit_forms,
                    "grouped_unit_count": grouped_unit_count,
                    "default_order": list(range(self.SENTENCE_ORDER_FIXED_UNIT_COUNT)),
                    "first_candidate_indices": first_candidate_indices,
                    "last_candidate_indices": last_candidate_indices,
                    "pairwise_constraints": pairwise_constraints,
                    "local_bindings": local_bindings,
                    "ordering_reason_trace": {
                        "normalization_reason": normalization_reason,
                        "ordering_reason": ordering_reason,
                        "raw_unit_count": raw_count,
                    },
                }
                candidates.append(
                    {
                        "candidate_id": f"{article_id}:ordered_unit_group:{len(candidates) + 1}",
                        "candidate_type": "ordered_unit_group",
                        "text": body,
                        "meta": meta,
                        "quality_flags": [],
                    }
                )
        return candidates

    def _normalize_ordered_units_to_six(
        self,
        raw_units: list[str],
    ) -> tuple[list[str], list[str], list[dict[str, Any]], str] | None:
        units = [unit.strip() for unit in raw_units if unit.strip()]
        if len(units) < self.SENTENCE_ORDER_FIXED_UNIT_COUNT or len(units) > 8:
            return None
        if len(units) == self.SENTENCE_ORDER_FIXED_UNIT_COUNT:
            return units, ["single_sentence_unit"] * len(units), [], "raw_six_units"

        merge_need = len(units) - self.SENTENCE_ORDER_FIXED_UNIT_COUNT
        pair_scores: list[tuple[float, int, str]] = []
        for index in range(len(units) - 1):
            score, reason = self._ordered_groupable_pair_score(units[index], units[index + 1])
            if score >= 0.10:
                pair_scores.append((score, index, reason))
        pair_scores.sort(reverse=True)

        selected_pairs: list[tuple[int, str]] = []
        used: set[int] = set()
        for score, index, reason in pair_scores:
            if index in used or index + 1 in used:
                continue
            selected_pairs.append((index, reason))
            used.add(index)
            used.add(index + 1)
            if len(selected_pairs) == merge_need:
                break
        if len(selected_pairs) != merge_need:
            return None

        pair_reason_map = {index: reason for index, reason in selected_pairs}
        normalized_units: list[str] = []
        unit_forms: list[str] = []
        local_bindings: list[dict[str, Any]] = []
        index = 0
        while index < len(units):
            if index in pair_reason_map:
                normalized_units.append(self._merge_ordered_unit_pair(units[index], units[index + 1]))
                unit_forms.append("grouped_unit")
                local_bindings.append(
                    {
                        "before": len(normalized_units) - 1,
                        "after": len(normalized_units) - 1,
                        "reason": pair_reason_map[index],
                        "source_indices": [index, index + 1],
                    }
                )
                index += 2
                continue
            normalized_units.append(units[index])
            unit_forms.append("single_sentence_unit")
            index += 1

        if len(normalized_units) != self.SENTENCE_ORDER_FIXED_UNIT_COUNT:
            return None
        return normalized_units, unit_forms, local_bindings, f"merged_to_six:{merge_need}"

    def _ordered_groupable_pair_score(self, left: str, right: str) -> tuple[float, str]:
        score = 0.0
        reason = "adjacent_short_binding"
        if right.startswith(ORDER_PRONOUN_MARKERS) or right.startswith(CONTEXTUAL_OPENINGS):
            score += 0.30
            reason = "reference_binding_pair"
        if any(marker in right for marker in ORDER_TURNING_BINDING_MARKERS + ORDER_PARALLEL_BINDING_MARKERS):
            score += 0.26
            reason = "transition_binding_pair"
        if any(marker in left for marker in ORDER_PROBLEM_MARKERS) and any(marker in right for marker in ORDER_SOLUTION_MARKERS):
            score += 0.28
            reason = "problem_solution_pair"
        if ("不仅" in left and any(token in right for token in ("而且", "也", "还"))) or ("一方面" in left and "另一方面" in right) or ("既" in left and "又" in right):
            score += 0.30
            reason = "parallel_binding_pair"
        if ("如果" in left and any(token in right for token in ("那么", "就", "则", "才"))) or ("只有" in left and "才" in right):
            score += 0.24
            reason = "condition_result_pair"
        if len(left) <= 34 and len(right) <= 34:
            score += 0.12
        if len(left) + len(right) > 88:
            score -= 0.18
        if any(marker in right for marker in ORDER_SUMMARY_CLOSING_MARKERS):
            score -= 0.08
        return round(max(0.0, min(1.0, score)), 4), reason

    def _merge_ordered_unit_pair(self, left: str, right: str) -> str:
        left_clean = left.strip().rstrip("。！？!?；;")
        right_clean = right.strip()
        if not left_clean:
            return right_clean
        separator = "" if left_clean.endswith(("，", ",", "；", ";", "：", ":")) else "，"
        return f"{left_clean}{separator}{right_clean}".strip()

    def _ordered_unit_group_worthwhile(
        self,
        units: list[str],
    ) -> tuple[bool, str, list[dict[str, Any]], list[int], list[int]]:
        if len(units) != self.SENTENCE_ORDER_FIXED_UNIT_COUNT:
            return False, "group_size_not_six", [], [], []

        first_scores = [self._ordered_first_eligibility(unit, index=index) for index, unit in enumerate(units)]
        last_scores = [self._ordered_last_eligibility(unit, index=index, total=len(units)) for index, unit in enumerate(units)]
        first_candidate_indices = [index for index, score in enumerate(first_scores) if score >= 0.50]
        last_candidate_indices = [index for index, score in enumerate(last_scores) if score >= 0.54]
        pairwise_constraints = self._ordered_pairwise_constraints(units)
        local_bindings = [item for item in pairwise_constraints if item.get("kind") == "local_binding"]

        if 0 not in first_candidate_indices:
            return False, "first_unit_unstable", pairwise_constraints, first_candidate_indices, last_candidate_indices
        if len(last_candidate_indices) == 0 or (len(units) - 1) not in last_candidate_indices:
            return False, "last_unit_unstable", pairwise_constraints, first_candidate_indices, last_candidate_indices
        if len(first_candidate_indices) >= 4:
            return False, "too_many_first_like_units", pairwise_constraints, first_candidate_indices, last_candidate_indices
        if len(last_candidate_indices) >= 4:
            return False, "too_many_last_like_units", pairwise_constraints, first_candidate_indices, last_candidate_indices
        if len(pairwise_constraints) < 1:
            return False, "ordering_constraints_too_weak", pairwise_constraints, first_candidate_indices, last_candidate_indices
        if len(local_bindings) == 0 and sum(1 for item in pairwise_constraints if item.get("kind") == "precedence") < 1:
            return False, "ordering_links_too_sparse", pairwise_constraints, first_candidate_indices, last_candidate_indices
        return True, "ordered_unit_group_ready", pairwise_constraints, first_candidate_indices, last_candidate_indices

    def _ordered_first_eligibility(self, unit: str, *, index: int) -> float:
        text = unit.strip()
        score = 0.42
        if index == 0:
            score += 0.10
        if any(marker in text for marker in ORDER_DEFINITION_MARKERS + ORDER_PROBLEM_MARKERS + ORDER_QUESTION_MARKERS):
            score += 0.18
        if any(marker in text for marker in ("首先", "第一", "起初", "一开始")):
            score += 0.14
        if text.startswith(ORDER_PRONOUN_MARKERS) or text.startswith(CONTEXTUAL_OPENINGS):
            score -= 0.26
        if any(marker in text for marker in ORDER_SUMMARY_CLOSING_MARKERS + COUNTERMEASURE_MARKERS):
            score -= 0.22
        if any(marker in text for marker in ("例如", "比如", "此外", "另外", "同时", "不仅如此")):
            score -= 0.16
        return round(max(0.0, min(1.0, score)), 4)

    def _ordered_last_eligibility(self, unit: str, *, index: int, total: int) -> float:
        text = unit.strip()
        score = 0.32
        if index == total - 1:
            score += 0.12
        if any(marker in text for marker in ORDER_SUMMARY_CLOSING_MARKERS + SUMMARY_MARKERS + CONCLUSION_MARKERS):
            score += 0.28
        if any(marker in text for marker in COUNTERMEASURE_MARKERS):
            score += 0.18
        if any(marker in text for marker in ORDER_DEFINITION_MARKERS + ORDER_PROBLEM_MARKERS):
            score -= 0.14
        if text.startswith(ORDER_PRONOUN_MARKERS) or text.startswith(CONTEXTUAL_OPENINGS):
            score -= 0.10
        return round(max(0.0, min(1.0, score)), 4)

    def _ordered_pairwise_constraints(self, units: list[str]) -> list[dict[str, Any]]:
        constraints: list[dict[str, Any]] = []
        for index in range(len(units) - 1):
            current = units[index]
            nxt = units[index + 1]
            if nxt.startswith(ORDER_PRONOUN_MARKERS) or nxt.startswith(CONTEXTUAL_OPENINGS):
                constraints.append({"kind": "local_binding", "before": index, "after": index + 1, "reason": "reference_dependency"})
            elif any(marker in nxt for marker in ORDER_TURNING_BINDING_MARKERS + ORDER_PARALLEL_BINDING_MARKERS):
                constraints.append({"kind": "local_binding", "before": index, "after": index + 1, "reason": "transition_binding"})
            elif any(marker in current for marker in ORDER_PROBLEM_MARKERS) and any(marker in nxt for marker in ORDER_SOLUTION_MARKERS):
                constraints.append({"kind": "precedence", "before": index, "after": index + 1, "reason": "problem_before_solution"})
            elif ("只有" in current and "才" in nxt) or ("如果" in current and any(token in nxt for token in ("那么", "就", "则", "才"))):
                constraints.append({"kind": "precedence", "before": index, "after": index + 1, "reason": "condition_before_result"})
            elif any(marker in current for marker in ORDER_DEFINITION_MARKERS) and not any(marker in nxt for marker in ORDER_DEFINITION_MARKERS):
                constraints.append({"kind": "precedence", "before": index, "after": index + 1, "reason": "definition_before_expansion"})
            elif any(marker in current for marker in ("因为", "由于", "缘于")) and any(marker in nxt for marker in ORDER_SUMMARY_CLOSING_MARKERS + CONCLUSION_MARKERS):
                constraints.append({"kind": "precedence", "before": index, "after": index + 1, "reason": "cause_before_conclusion"})
        return constraints

    def _derive_functional_slot_rule_candidates(
        self,
        *,
        article_context: dict[str, Any],
    ) -> list[dict[str, Any]]:
        paragraphs: list[str] = article_context.get("paragraphs") or []
        paragraph_sentences: list[list[str]] = article_context.get("paragraph_sentences") or []
        paragraph_sentence_offsets: list[int] = article_context.get("paragraph_sentence_offsets") or []
        if not paragraphs or not paragraph_sentences:
            return []

        candidates: list[dict[str, Any]] = []
        article_id = str(article_context.get("article_id") or "")

        def build_candidate(
            *,
            slot_role: str,
            slot_function: str,
            slot_paragraph_index: int,
            slot_sentence_local_index: int,
            planner_priority: float,
            planner_reason: str,
            slot_trace: dict[str, Any] | None = None,
        ) -> dict[str, Any] | None:
            if slot_paragraph_index < 0 or slot_paragraph_index >= len(paragraphs):
                return None
            slot_sentences = paragraph_sentences[slot_paragraph_index]
            if not slot_sentences:
                return None
            if slot_sentence_local_index < 0 or slot_sentence_local_index >= len(slot_sentences):
                return None
            sentence_window = self._functional_slot_sentence_window(
                slot_role=slot_role,
                slot_function=slot_function,
                local_sentences=slot_sentences,
                slot_sentence_local_index=slot_sentence_local_index,
            )
            if sentence_window is None:
                return None
            local_start, local_end = sentence_window
            slot_text = "".join(slot_sentences[local_start : local_end + 1]).strip()
            if not slot_text:
                return None
            sentence_count = local_end - local_start + 1
            if sentence_count > 2:
                return None
            char_count = len(slot_text)
            if sentence_count == 1 and char_count > 160:
                return None
            if sentence_count == 2 and char_count > 240:
                return None
            slot_sentence_offset = paragraph_sentence_offsets[slot_paragraph_index] if slot_paragraph_index < len(paragraph_sentence_offsets) else 0
            slot_sentence_abs_start = slot_sentence_offset + local_start
            slot_sentence_abs_end = slot_sentence_offset + local_end
            context_paragraph_start = max(0, slot_paragraph_index - 1)
            context_paragraph_end = min(len(paragraphs) - 1, slot_paragraph_index + 1)
            context_before = paragraphs[slot_paragraph_index - 1].strip() if slot_paragraph_index > 0 else ""
            context_after = paragraphs[slot_paragraph_index + 1].strip() if slot_paragraph_index + 1 < len(paragraphs) else ""
            slot_context_text = "\n\n".join(
                part for part in [context_before, slot_text, context_after] if part
            )
            blank_value_ok, blank_value_reason = self._functional_slot_has_blank_value(
                slot_role=slot_role,
                slot_function=slot_function,
                slot_text=slot_text,
                context_before=context_before,
                context_after=context_after,
                slot_context_text=slot_context_text,
            )
            if not blank_value_ok:
                return None
            meta = {
                "paragraph_range": [slot_paragraph_index, slot_paragraph_index],
                "sentence_range": [slot_sentence_abs_start, slot_sentence_abs_end],
                "planner_source": "rule_functional_slot_builder",
                "planner_priority": round(planner_priority, 4),
                "planner_reason": planner_reason,
                "unit_type": "functional_slot_unit",
                "slot_role": slot_role,
                "slot_function": slot_function,
                "slot_sentence_range": [slot_sentence_abs_start, slot_sentence_abs_end],
                "slot_context_paragraph_range": [context_paragraph_start, context_paragraph_end],
                "slot_context_sentence_range": [
                    max(0, slot_sentence_abs_start - 1),
                    slot_sentence_abs_end + (1 if context_after else 0),
                ],
                "blank_value_ready": True,
                "blank_value_reason": blank_value_reason,
            }
            if slot_trace:
                meta.update(
                    {
                        "slot_backward_score": round(float(slot_trace.get("backward_score") or 0.0), 4),
                        "slot_forward_score": round(float(slot_trace.get("forward_score") or 0.0), 4),
                        "slot_bidirectional_score": round(float(slot_trace.get("bidirectional_score") or 0.0), 4),
                        "slot_carry_dependency_score": round(float(slot_trace.get("carry_dependency_score") or 0.0), 4),
                        "slot_implicit_carry_gap_score": round(float(slot_trace.get("implicit_carry_gap_score") or 0.0), 4),
                        "slot_carry_gap_ready": bool(slot_trace.get("carry_gap_ready")),
                        "slot_bridge_dependency_score": round(float(slot_trace.get("bridge_dependency_score") or 0.0), 4),
                        "slot_forward_dependency_score": round(float(slot_trace.get("forward_dependency_score") or 0.0), 4),
                        "slot_topic_continuity_score": round(float(slot_trace.get("topic_continuity_score") or 0.0), 4),
                        "slot_layer_continuity_score": round(float(slot_trace.get("layer_continuity_score") or 0.0), 4),
                        "slot_classification_reason": str(slot_trace.get("classification_reason") or ""),
                    }
                )
            return {
                "candidate_id": f"{article_id}:functional_slot_unit:{len(candidates) + 1}",
                "candidate_type": "functional_slot_unit",
                "text": slot_text,
                "meta": meta,
                "quality_flags": [],
            }

        first_paragraph_sentences = paragraph_sentences[0] if paragraph_sentences else []
        if first_paragraph_sentences:
            opening_slot_text = first_paragraph_sentences[0]
            opening_slot_function = self._infer_functional_slot_function(
                slot_role="opening",
                slot_text=opening_slot_text,
                context_text="\n\n".join(paragraphs[0 : min(len(paragraphs), 2)]),
            )
            candidate = build_candidate(
                slot_role="opening",
                slot_function=opening_slot_function,
                slot_paragraph_index=0,
                slot_sentence_local_index=0,
                planner_priority=0.82,
                planner_reason=f"functional_slot:{opening_slot_function}",
            )
            if candidate is not None:
                candidates.append(candidate)

        if len(paragraphs) >= 2:
            middle_ranked: list[tuple[float, dict[str, Any]]] = []
            for paragraph_index in range(0, len(paragraphs) - 1):
                right_index = paragraph_index + 1
                right_sentences = paragraph_sentences[right_index]
                if not right_sentences:
                    continue
                context_before = paragraphs[paragraph_index].strip()
                context_after = paragraphs[right_index + 1].strip() if right_index + 1 < len(paragraphs) else ""
                context_text = "\n\n".join(
                    part for part in [context_before, paragraphs[right_index].strip(), context_after] if part
                ).strip()
                for local_index in range(min(2, len(right_sentences))):
                    slot_text = right_sentences[local_index]
                    middle_trace = self._classify_middle_functional_slot(
                        slot_text=slot_text,
                        context_before=context_before,
                        context_after=context_after,
                        slot_context_text=context_text,
                    )
                    slot_function = str(middle_trace.get("slot_function") or "")
                    if not slot_function:
                        continue
                    slot_score = self._functional_slot_middle_priority(middle_trace=middle_trace)
                    candidate = build_candidate(
                        slot_role="middle",
                        slot_function=slot_function,
                        slot_paragraph_index=right_index,
                        slot_sentence_local_index=local_index,
                        planner_priority=0.74 + 0.10 * slot_score - 0.03 * local_index,
                        planner_reason=f"functional_slot:{slot_function}",
                        slot_trace=middle_trace,
                    )
                    if candidate is not None:
                        middle_ranked.append((slot_score, candidate))
            middle_ranked.sort(key=lambda item: item[0], reverse=True)
            for _, candidate in middle_ranked[:2]:
                candidates.append(candidate)

        last_index = len(paragraphs) - 1
        last_paragraph_sentences = paragraph_sentences[last_index] if 0 <= last_index < len(paragraph_sentences) else []
        if last_paragraph_sentences:
            ending_slot_text = last_paragraph_sentences[-1]
            ending_slot_function = self._infer_functional_slot_function(
                slot_role="ending",
                slot_text=ending_slot_text,
                context_text="\n\n".join(paragraphs[max(0, last_index - 1) : last_index + 1]),
            )
            candidate = build_candidate(
                slot_role="ending",
                slot_function=ending_slot_function,
                slot_paragraph_index=last_index,
                slot_sentence_local_index=len(last_paragraph_sentences) - 1,
                planner_priority=0.80,
                planner_reason=f"functional_slot:{ending_slot_function}",
            )
            if candidate is not None:
                candidates.append(candidate)

        return candidates

    def _functional_slot_sentence_window(
        self,
        *,
        slot_role: str,
        slot_function: str,
        local_sentences: list[str],
        slot_sentence_local_index: int,
    ) -> tuple[int, int] | None:
        if not local_sentences:
            return None
        start = slot_sentence_local_index
        end = slot_sentence_local_index
        slot_text = local_sentences[slot_sentence_local_index].strip()
        if slot_role == "middle":
            short_slot = len(slot_text) < 42
            if slot_function == "carry_previous" and short_slot and slot_sentence_local_index > 0:
                start = slot_sentence_local_index - 1
            elif slot_function in {"lead_next", "bridge_both_sides"} and short_slot and slot_sentence_local_index + 1 < len(local_sentences):
                end = slot_sentence_local_index + 1
        return (start, end)

    def _functional_slot_has_blank_value(
        self,
        *,
        slot_role: str,
        slot_function: str,
        slot_text: str,
        context_before: str,
        context_after: str,
        slot_context_text: str,
    ) -> tuple[bool, str]:
        summary_signal = self._marker_strength(slot_text, SUMMARY_MARKERS + CONCLUSION_MARKERS)
        countermeasure_signal = self._marker_strength(slot_text, COUNTERMEASURE_MARKERS + ACTION_MEASURE_MARKERS + VALUE_MARKERS)
        transition_markers = ("因此", "同时", "由此", "对此", "进一步", "这也说明", "关键在于", "总之", "可见", "接下来", "此外", "从而", "于是")
        reference_markers = ("这", "其", "该", "这种", "这一", "这一点", "上述", "这些", "此举", "对此", "由此")
        has_transition_marker = any(marker in slot_text for marker in transition_markers)
        has_reference_marker = any(marker in slot_text for marker in reference_markers)
        backward = self._backward_link_strength(slot_context_text)
        forward = self._forward_link_strength(slot_context_text)
        bidirectional = self._bidirectional_validation(slot_context_text)
        if slot_role == "opening":
            opening_markers = ("当前", "如今", "近年来", "面对", "在此背景下", "放眼", "着眼于")
            if slot_function == "summary":
                if summary_signal >= 0.18:
                    return True, "summary_anchor"
                return False, "opening_summary_weak_blank_value"
            if any(marker in slot_text for marker in opening_markers) or self._core_object_anchor_strength(slot_text) >= 0.28:
                return True, "topic_intro_anchor"
            return False, "opening_topic_intro_weak_blank_value"
        if slot_role == "middle":
            middle_trace = self._classify_middle_functional_slot(
                slot_text=slot_text,
                context_before=context_before,
                context_after=context_after,
                slot_context_text=slot_context_text,
            )
            inferred_function = str(middle_trace.get("slot_function") or "")
            if not inferred_function:
                return False, "middle_structural_role_unresolved"
            if inferred_function != slot_function:
                return False, f"middle_role_mismatch:{inferred_function}"
            backward_score = float(middle_trace.get("backward_score") or 0.0)
            forward_score = float(middle_trace.get("forward_score") or 0.0)
            bidirectional_score = float(middle_trace.get("bidirectional_score") or 0.0)
            carry_dependency_score = float(middle_trace.get("carry_dependency_score") or 0.0)
            implicit_carry_gap_score = float(middle_trace.get("implicit_carry_gap_score") or 0.0)
            bridge_dependency_score = float(middle_trace.get("bridge_dependency_score") or 0.0)
            forward_dependency_score = float(middle_trace.get("forward_dependency_score") or 0.0)
            topic_continuity_score = float(middle_trace.get("topic_continuity_score") or 0.0)
            layer_continuity_score = float(middle_trace.get("layer_continuity_score") or 0.0)
            classification_reason = str(middle_trace.get("classification_reason") or "middle_role_supported")
            carry_gap_ready = bool(middle_trace.get("carry_gap_ready"))
            if slot_function == "carry_previous":
                if not carry_gap_ready:
                    return False, "middle_carry_previous_gap_not_ready"
                if classification_reason == "middle_carry_previous_implicit":
                    if (
                        backward_score >= 0.50
                        and implicit_carry_gap_score >= 0.48
                        and topic_continuity_score >= 0.42
                        and layer_continuity_score >= 0.60
                        and forward_dependency_score <= implicit_carry_gap_score
                    ):
                        return True, "middle_carry_previous_implicit:carry_structural_gap"
                elif classification_reason == "middle_carry_previous_weak_explicit":
                    if (
                        backward_score >= 0.56
                        and carry_dependency_score >= 0.48
                        and layer_continuity_score >= 0.78
                        and forward_dependency_score <= 0.36
                    ):
                        return True, "middle_carry_previous_weak_explicit:carry_structural_gap"
                elif (
                    backward_score >= 0.54
                    and carry_dependency_score >= 0.58
                    and forward_dependency_score <= carry_dependency_score
                ):
                    return True, f"{classification_reason}:carry_structural_gap"
                return False, "middle_carry_previous_weak_link"
            if slot_function == "lead_next":
                if forward_score >= 0.54 and bidirectional_score < 0.58 and forward_dependency_score >= 0.52:
                    return True, f"{classification_reason}:lead_next_forward_gap"
                return False, "middle_lead_next_weak_link"
            if slot_function == "bridge_both_sides":
                if (
                    bidirectional_score >= 0.58
                    and min(backward_score, forward_score) >= 0.44
                    and bridge_dependency_score >= 0.62
                ):
                    return True, f"{classification_reason}:bridge_structural_gap"
                return False, "middle_bridge_weak_link"
        if slot_role == "ending":
            if slot_function == "countermeasure":
                if countermeasure_signal >= 0.16 or any(marker in slot_text for marker in ("应当", "应该", "要", "需要", "必须", "建议", "措施", "路径")):
                    return True, "ending_countermeasure_anchor"
                return False, "ending_countermeasure_weak_anchor"
            if summary_signal >= 0.18 or any(marker in slot_text for marker in ("总之", "可见", "由此", "这启示我们", "这说明")):
                return True, "ending_summary_anchor"
            return False, "ending_summary_weak_anchor"
        return False, "slot_role_unresolved"

    def _infer_functional_slot_function(
        self,
        *,
        slot_role: str,
        slot_text: str,
        context_text: str,
        context_before: str = "",
        context_after: str = "",
    ) -> str:
        slot_sentences = [sentence for sentence in self.sentence_splitter.split(slot_text) if sentence.strip()]
        slot_span = self._build_span(
            article_id="functional_slot",
            span_id=f"functional_slot:{slot_role}",
            text=slot_text,
            paragraph_count=1,
            sentence_count=max(1, len(slot_sentences)),
            source_domain=None,
        )
        slot_universal = self.universal_tagger._heuristic_tag(slot_span)
        if slot_role == "opening":
            if slot_universal.summary_strength >= 0.56 or self._marker_strength(slot_text, SUMMARY_MARKERS) >= 0.22:
                return "summary"
            return "topic_intro"
        if slot_role == "ending":
            countermeasure_strength = max(
                self._marker_strength(slot_text, COUNTERMEASURE_MARKERS + ACTION_MEASURE_MARKERS),
                slot_universal.method_signal_strength,
            )
            if countermeasure_strength >= 0.28 or any(marker in slot_text for marker in ("应当", "应该", "要", "需要", "必须", "建议", "路径", "措施")):
                return "countermeasure"
            return "ending_summary"

        middle_trace = self._classify_middle_functional_slot(
            slot_text=slot_text,
            context_before=context_before,
            context_after=context_after,
            slot_context_text=context_text,
        )
        return str(middle_trace.get("slot_function") or "")

    def _carry_previous_gap_ready(
        self,
        *,
        backward_score: float,
        carry_dependency_score: float,
        implicit_carry_gap_score: float,
        forward_dependency_score: float,
        standalone_anchor: float,
    ) -> bool:
        return (
            implicit_carry_gap_score >= 0.40
            and backward_score >= 0.54
            and carry_dependency_score >= 0.48
            and forward_dependency_score <= max(0.38, implicit_carry_gap_score - 0.02)
            and standalone_anchor <= 0.72
        )

    def _classify_middle_functional_slot(
        self,
        *,
        slot_text: str,
        context_before: str,
        context_after: str,
        slot_context_text: str,
    ) -> dict[str, Any]:
        carry_reference_markers = ("这", "这种", "这一", "这一点", "其", "该", "上述", "前述", "由此可见", "这说明")
        weak_carry_markers = ("而这", "这一过程", "这一变化", "由此", "在此基础上", "基于此", "相应地", "由此带来", "这使得", "这使")
        carry_explanation_markers = ("实际上", "具体来看", "从这个意义上说", "进一步说", "换言之", "也就是说", "这意味着")
        forward_markers = ("因此", "于是", "同时", "接下来", "进一步", "此外", "从而", "关键在于", "更重要的是", "这也意味着")
        bridge_markers = ("正因为如此", "与此同时", "这不仅", "不仅如此", "既", "又", "一方面", "另一方面", "同时也", "既要", "也要")
        backward_text = "\n\n".join(part for part in [context_before, slot_text] if part)
        forward_text = "\n\n".join(part for part in [slot_text, context_after] if part)
        backward = self._backward_link_strength(backward_text)
        forward = self._forward_link_strength(forward_text)
        bidirectional = self._bidirectional_validation(slot_context_text)
        has_reference_marker = any(marker in slot_text for marker in carry_reference_markers)
        has_weak_carry_marker = any(marker in slot_text for marker in weak_carry_markers)
        has_explanation_marker = any(marker in slot_text for marker in carry_explanation_markers)
        has_forward_marker = any(marker in slot_text for marker in forward_markers)
        has_bridge_marker = any(marker in slot_text for marker in bridge_markers)
        reference_dependency = self._reference_dependency(slot_text)
        standalone_anchor = self._core_object_anchor_strength(slot_text)
        previous_tail = ""
        previous_sentences = [sentence.strip() for sentence in self.sentence_splitter.split(context_before) if sentence.strip()]
        if previous_sentences:
            previous_tail = previous_sentences[-1]
        previous_theme_words = set(self._theme_words(previous_tail or context_before, None))
        slot_theme_words = set(self._theme_words(slot_text, None))
        theme_overlap_ratio = 0.0
        if previous_theme_words and slot_theme_words:
            theme_overlap_ratio = len(previous_theme_words & slot_theme_words) / max(1, min(len(previous_theme_words), len(slot_theme_words)))
        topic_continuity_score = min(
            1.0,
            0.58 * theme_overlap_ratio
            + (0.18 if previous_tail and slot_theme_words and previous_theme_words & slot_theme_words else 0.0)
            + (0.12 if has_reference_marker or has_weak_carry_marker else 0.0),
        )
        layer_continuity_score = min(
            1.0,
            (0.24 if context_before else 0.0)
            + (0.18 if not has_forward_marker else 0.0)
            + (0.14 if not has_bridge_marker else 0.0)
            + (0.14 if not any(token in slot_text for token in SUMMARY_MARKERS + CONCLUSION_MARKERS) else 0.0)
            + (0.12 if not slot_text.startswith(("因此", "同时", "此外", "接下来", "更重要的是")) else 0.0),
        )
        backward_score = min(
            1.0,
            backward
            + (0.18 if has_reference_marker else 0.0)
            + (0.10 if has_weak_carry_marker else 0.0)
            + (0.14 if has_explanation_marker else 0.0)
            + (0.08 if has_bridge_marker else 0.0)
            + (0.08 if slot_text.startswith(CONTEXTUAL_OPENINGS) else 0.0),
        )
        forward_score = min(
            1.0,
            forward
            + (0.18 if has_forward_marker else 0.0)
            + (0.16 if has_bridge_marker else 0.0)
            + (0.10 if slot_text.startswith(("同时", "因此", "于是", "接下来", "进一步")) else 0.0),
        )
        bidirectional_score = min(
            1.0,
            bidirectional
            + (0.20 if has_bridge_marker else 0.0)
            + (0.12 if has_reference_marker and has_forward_marker else 0.0)
            + (0.08 if min(backward, forward) >= 0.46 else 0.0),
        )
        carry_dependency_score = min(
            1.0,
            0.45 * backward_score
            + 0.20 * reference_dependency
            + (0.15 if has_reference_marker else 0.0)
            + (0.08 if has_weak_carry_marker else 0.0)
            + (0.12 if has_explanation_marker else 0.0)
            + (0.08 if context_before else 0.0)
            - 0.18 * standalone_anchor,
        )
        implicit_carry_gap_score = min(
            1.0,
            0.34 * backward_score
            + 0.24 * topic_continuity_score
            + 0.18 * layer_continuity_score
            + (0.10 if has_weak_carry_marker else 0.0)
            + (0.08 if context_before else 0.0)
            - 0.10 * standalone_anchor
            - (0.10 if has_forward_marker else 0.0)
            - (0.10 if has_bridge_marker else 0.0),
        )
        forward_dependency_score = min(
            1.0,
            0.50 * forward_score
            + (0.14 if has_forward_marker else 0.0)
            + (0.08 if context_after else 0.0)
            + (0.06 if not has_reference_marker else 0.0)
            - (0.08 if has_explanation_marker else 0.0),
        )
        bridge_dependency_score = min(
            1.0,
            0.28 * backward_score
            + 0.28 * forward_score
            + 0.24 * bidirectional_score
            + (0.14 if has_bridge_marker else 0.0)
            + (0.08 if has_reference_marker and has_forward_marker else 0.0)
            + (0.08 if context_before and context_after else 0.0)
            - 0.10 * standalone_anchor,
        )
        carry_gap_ready = self._carry_previous_gap_ready(
            backward_score=backward_score,
            carry_dependency_score=carry_dependency_score,
            implicit_carry_gap_score=implicit_carry_gap_score,
            forward_dependency_score=forward_dependency_score,
            standalone_anchor=standalone_anchor,
        )
        slot_function = ""
        classification_reason = "middle_role_unresolved"
        if (
            has_bridge_marker
            and bidirectional_score >= 0.50
            and backward_score >= 0.40
            and forward_score >= 0.40
            and bridge_dependency_score >= 0.58
        ):
            slot_function = "bridge_both_sides"
            classification_reason = "middle_bridge_marker"
        elif (
            bidirectional_score >= 0.58
            and backward_score >= 0.44
            and forward_score >= 0.44
            and bridge_dependency_score >= 0.60
        ):
            slot_function = "bridge_both_sides"
            classification_reason = "middle_bridge_bidirectional"
        elif (
            carry_gap_ready
            and
            backward_score >= 0.56
            and forward_score <= backward_score - 0.08
            and carry_dependency_score >= 0.56
        ):
            slot_function = "carry_previous"
            classification_reason = "middle_carry_previous_backward"
        elif (
            forward_score >= 0.54
            and backward_score <= forward_score - 0.04
            and forward_dependency_score >= 0.52
        ):
            slot_function = "lead_next"
            classification_reason = "middle_lead_next_forward"
        elif (
            carry_gap_ready
            and
            backward_score >= 0.52
            and forward_score < 0.44
            and carry_dependency_score >= 0.54
        ):
            slot_function = "carry_previous"
            classification_reason = "middle_carry_previous_reference"
        elif (
            carry_gap_ready
            and
            backward_score >= 0.56
            and forward_score <= 0.38
            and carry_dependency_score >= 0.48
            and layer_continuity_score >= 0.78
            and (has_reference_marker or has_weak_carry_marker)
            and not has_bridge_marker
            and forward_dependency_score <= 0.36
        ):
            slot_function = "carry_previous"
            classification_reason = "middle_carry_previous_weak_explicit"
        elif (
            carry_gap_ready
            and
            backward_score >= 0.50
            and forward_score <= backward_score - 0.04
            and topic_continuity_score >= 0.42
            and layer_continuity_score >= 0.60
            and implicit_carry_gap_score >= 0.48
            and standalone_anchor <= 0.72
            and not has_bridge_marker
            and not has_forward_marker
        ):
            slot_function = "carry_previous"
            classification_reason = "middle_carry_previous_implicit"
        elif (
            forward_score >= 0.50
            and backward_score < 0.50
            and forward_dependency_score >= 0.50
        ):
            slot_function = "lead_next"
            classification_reason = "middle_lead_next_transition"
        return {
            "slot_function": slot_function,
            "backward_score": round(backward_score, 4),
            "forward_score": round(forward_score, 4),
            "bidirectional_score": round(bidirectional_score, 4),
            "carry_dependency_score": round(carry_dependency_score, 4),
            "implicit_carry_gap_score": round(implicit_carry_gap_score, 4),
            "carry_gap_ready": carry_gap_ready,
            "bridge_dependency_score": round(bridge_dependency_score, 4),
            "forward_dependency_score": round(forward_dependency_score, 4),
            "topic_continuity_score": round(topic_continuity_score, 4),
            "layer_continuity_score": round(layer_continuity_score, 4),
            "classification_reason": classification_reason,
        }

    def _functional_slot_middle_priority(self, *, middle_trace: dict[str, Any]) -> float:
        backward = float(middle_trace.get("backward_score") or 0.0)
        forward = float(middle_trace.get("forward_score") or 0.0)
        bidirectional = float(middle_trace.get("bidirectional_score") or 0.0)
        return round(max(bidirectional, backward, forward), 4)

    def _derive_candidates_with_llm(
        self,
        *,
        article_context: dict[str, Any],
        selected_types: set[str],
    ) -> list[dict[str, Any]]:
        if not self.llm_config.get("enabled") or not self.provider.is_enabled():
            return []
        schema: dict[str, Any] = {
            "type": "object",
            "properties": {
                "candidates": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "candidate_type": {"type": "string", "enum": list(self._supported_candidate_types())},
                            "paragraph_start": {"type": "integer", "minimum": 0},
                            "paragraph_end": {"type": "integer", "minimum": 0},
                            "sentence_start_in_first_paragraph": {"type": ["integer", "null"], "minimum": 0},
                            "sentence_end_in_last_paragraph": {"type": ["integer", "null"], "minimum": 0},
                            "slot_role": {"type": ["string", "null"], "enum": ["opening", "middle", "ending", None]},
                            "slot_function": {
                                "type": ["string", "null"],
                                "enum": [
                                    "summary",
                                    "topic_intro",
                                    "carry_previous",
                                    "lead_next",
                                    "bridge_both_sides",
                                    "ending_summary",
                                    "countermeasure",
                                    None,
                                ],
                            },
                            "composition": {
                                "type": "string",
                                "enum": [
                                    "whole_passage",
                                    "paragraph_span",
                                    "single_paragraph_window",
                                    "adjacent_paragraph_pair",
                                    "insertion_window",
                                    "functional_slot_window",
                                ],
                            },
                            "priority": {"type": "number"},
                            "reason": {"type": "string"},
                        },
                        "required": ["candidate_type", "paragraph_start", "paragraph_end", "sentence_start_in_first_paragraph", "sentence_end_in_last_paragraph", "composition", "priority", "reason"],
                        "additionalProperties": False,
                    },
                }
            },
            "required": ["candidates"],
            "additionalProperties": False,
        }
        prompt = self._build_candidate_planner_prompt(article_context=article_context, selected_types=selected_types)
        try:
            result = self.provider.generate_json(
                model=self.llm_config.get("models", {}).get("candidate_planner_v2", self.llm_config.get("models", {}).get("family_tagger", "gpt-4.1-mini")),
                instructions=self.candidate_planner_prompt,
                input_payload={
                    "prompt": prompt,
                    "schema_name": "candidate_planner_v2",
                    "schema": schema,
                },
            )
        except Exception:
            return []

        candidates: list[dict[str, Any]] = []
        for index, spec in enumerate(result.get("candidates", []), start=1):
            candidate = self._materialize_candidate_spec(article_context=article_context, spec=spec, rank=index)
            if candidate is not None and candidate["candidate_type"] in selected_types:
                candidates.append(candidate)
        return candidates

    def _plan_candidate_pool(
        self,
        *,
        article_context: dict[str, Any],
        candidates: list[dict[str, Any]],
        selected_types: set[str],
    ) -> list[dict[str, Any]]:
        ranked: list[dict[str, Any]] = []
        for candidate in candidates:
            if candidate["candidate_type"] not in selected_types:
                continue
            neutral_signal_profile = candidate.get("neutral_signal_profile") or self._build_neutral_signal_profile(article_context=article_context, candidate=candidate)
            planner_score = round(self._candidate_plan_score(article_context=article_context, candidate=candidate, neutral_signal_profile=neutral_signal_profile), 4)
            if planner_score < self._planner_score_threshold(candidate["candidate_type"]):
                continue
            meta = dict(candidate.get("meta") or {})
            meta["planner_score"] = planner_score
            candidate = {**candidate, "meta": meta, "neutral_signal_profile": neutral_signal_profile}
            ranked.append(candidate)

        if not ranked:
            return []

        ranked.sort(key=lambda item: (item.get("meta", {}).get("planner_score", 0.0), self._candidate_priority_boost(item)), reverse=True)
        selected: list[dict[str, Any]] = []
        type_counts: Counter[str] = Counter()
        seen: set[tuple[str, str]] = set()
        per_type_limits = self._planner_type_limits()
        for candidate in ranked:
            candidate_type = candidate["candidate_type"]
            if type_counts[candidate_type] >= per_type_limits.get(candidate_type, 4):
                continue
            key = (candidate_type, candidate["text"].strip())
            if key in seen:
                continue
            seen.add(key)
            selected.append(candidate)
            type_counts[candidate_type] += 1

        article_id = article_context["article_id"]
        finalized: list[dict[str, Any]] = []
        for index, candidate in enumerate(selected, start=1):
            finalized.append({**candidate, "candidate_id": f"{article_id}:{candidate['candidate_type']}:{index}"})
        return finalized

    def _build_candidate_planner_prompt(
        self,
        *,
        article_context: dict[str, Any],
        selected_types: set[str],
    ) -> str:
        paragraph_lines: list[str] = []
        paragraph_sentences: list[list[str]] = article_context.get("paragraph_sentences") or []
        for index, paragraph in enumerate(article_context["paragraphs"]):
            local_sentences = paragraph_sentences[index] if index < len(paragraph_sentences) else [sentence for sentence in self.sentence_splitter.split(paragraph) if sentence.strip()]
            snippet = paragraph.strip().replace("\n", " ")
            if len(snippet) > 260:
                snippet = f"{snippet[:257]}..."
            paragraph_lines.append(f"[P{index}] sentences={len(local_sentences)} text={snippet}")
        return "\n".join(
            [
                f"title: {article_context.get('title') or ''}",
                f"selected_candidate_types: {', '.join(sorted(selected_types))}",
                f"article_profile: {article_context['article_profile']}",
                "paragraph_catalog:",
                *paragraph_lines,
            ]
        )

    def _materialize_candidate_spec(
        self,
        *,
        article_context: dict[str, Any],
        spec: dict[str, Any],
        rank: int,
    ) -> dict[str, Any] | None:
        paragraphs: list[str] = article_context["paragraphs"]
        paragraph_sentences: list[list[str]] = article_context.get("paragraph_sentences") or []
        paragraph_sentence_offsets: list[int] = article_context.get("paragraph_sentence_offsets") or []
        candidate_type = str(spec.get("candidate_type") or "").strip()
        if candidate_type not in self._supported_candidate_types():
            return None
        if not paragraphs:
            return None
        paragraph_start = max(0, min(int(spec.get("paragraph_start") or 0), len(paragraphs) - 1))
        paragraph_end = max(paragraph_start, min(int(spec.get("paragraph_end") or paragraph_start), len(paragraphs) - 1))
        composition = str(spec.get("composition") or "paragraph_span")
        meta: dict[str, Any] = {
            "paragraph_range": [paragraph_start, paragraph_end],
            "composition": composition,
            "planner_source": "llm_candidate_planner",
            "planner_priority": round(float(spec.get("priority") or 0.0), 4),
            "planner_reason": str(spec.get("reason") or "").strip(),
        }
        slot_role = str(spec.get("slot_role") or "").strip()
        slot_function = str(spec.get("slot_function") or "").strip()

        if candidate_type == "whole_passage":
            text = article_context["text"]
            meta["paragraph_range"] = [0, max(len(paragraphs) - 1, 0)]
        elif candidate_type in {"closed_span", "multi_paragraph_unit"}:
            text = "\n\n".join(paragraphs[paragraph_start : paragraph_end + 1]).strip()
        elif candidate_type in {"sentence_block_group", "insertion_context_unit", "functional_slot_unit"}:
            start_local = spec.get("sentence_start_in_first_paragraph")
            end_local = spec.get("sentence_end_in_last_paragraph")
            if start_local is None:
                start_local = 0
            if paragraph_end == paragraph_start:
                local_sentences = paragraph_sentences[paragraph_start] if paragraph_start < len(paragraph_sentences) else [sentence for sentence in self.sentence_splitter.split(paragraphs[paragraph_start]) if sentence.strip()]
                if not local_sentences:
                    return None
                start_local = max(0, min(int(start_local), len(local_sentences) - 1))
                if end_local is None:
                    end_local = len(local_sentences) - 1
                end_local = max(start_local, min(int(end_local), len(local_sentences) - 1))
                text = "".join(local_sentences[start_local : end_local + 1]).strip()
                sentence_offset = paragraph_sentence_offsets[paragraph_start] if paragraph_start < len(paragraph_sentence_offsets) else 0
                meta["sentence_range"] = [sentence_offset + start_local, sentence_offset + end_local]
            else:
                if paragraph_end != paragraph_start + 1:
                    return None
                left_sentences = paragraph_sentences[paragraph_start] if paragraph_start < len(paragraph_sentences) else [sentence for sentence in self.sentence_splitter.split(paragraphs[paragraph_start]) if sentence.strip()]
                right_sentences = paragraph_sentences[paragraph_end] if paragraph_end < len(paragraph_sentences) else [sentence for sentence in self.sentence_splitter.split(paragraphs[paragraph_end]) if sentence.strip()]
                if not left_sentences or not right_sentences:
                    return None
                start_local = max(0, min(int(start_local), len(left_sentences) - 1))
                if end_local is None:
                    end_local = len(right_sentences) - 1
                end_local = max(0, min(int(end_local), len(right_sentences) - 1))
                combined = left_sentences[start_local:] + right_sentences[: end_local + 1]
                text = "".join(combined).strip()
                left_offset = paragraph_sentence_offsets[paragraph_start] if paragraph_start < len(paragraph_sentence_offsets) else 0
                right_offset = paragraph_sentence_offsets[paragraph_end] if paragraph_end < len(paragraph_sentence_offsets) else len(left_sentences)
                meta["sentence_range"] = [left_offset + start_local, right_offset + end_local]
        else:
            return None

        if candidate_type == "functional_slot_unit":
            inferred = self._hydrate_functional_slot_meta(
                article_context=article_context,
                candidate={
                    "candidate_type": candidate_type,
                    "text": text,
                    "meta": {
                        **meta,
                        "slot_role": slot_role or None,
                        "slot_function": slot_function or None,
                    },
                },
            )
            meta.update(inferred)

        if not text:
            return None
        quality_flags = []
        if text.startswith(CONTEXTUAL_OPENINGS):
            quality_flags.append("context_opening")
        if not text.endswith(("。", "！", "？", "!", "?")):
            quality_flags.append("missing_terminal_punctuation")
        return {
            "candidate_id": f"{article_context['article_id']}:{candidate_type}:llm{rank}",
            "candidate_type": candidate_type,
            "text": text,
            "meta": meta,
            "quality_flags": quality_flags,
        }

    def _candidate_plan_score(
        self,
        *,
        article_context: dict[str, Any],
        candidate: dict[str, Any],
        neutral_signal_profile: dict[str, Any],
    ) -> float:
        candidate_type = candidate["candidate_type"]
        context_dependency = float(neutral_signal_profile.get("context_dependency") or 0.0)
        closure = float(neutral_signal_profile.get("closure_score") or 0.0)
        center = float(neutral_signal_profile.get("single_center_strength") or 0.0)
        score = 0.34 * (1 - context_dependency) + 0.24 * closure + 0.22 * center
        score += 0.10 * float(neutral_signal_profile.get("summary_strength") or 0.0)

        if candidate_type in {"whole_passage", "closed_span", "multi_paragraph_unit"}:
            score += 0.08 * float(neutral_signal_profile.get("titleability") or 0.0)
            if self._has_repeated_enumerative_openings(candidate["text"].split("\n\n")):
                score -= 0.16
            if self._heading_like_opening(candidate["text"]):
                score -= 0.10
            if self._directive_style_opening(candidate["text"]):
                score -= 0.08
        if candidate_type == "sentence_block_group":
            structure = self._sentence_order_structure_completeness(neutral_signal_profile, candidate)
            meaning = self._sentence_order_meaningfulness(candidate["text"], neutral_signal_profile, candidate_type)
            sequence = float(neutral_signal_profile.get("sequence_integrity") or 0.0)
            unique_opener = float(neutral_signal_profile.get("unique_opener_score") or 0.0)
            binding_pair_count = float(neutral_signal_profile.get("binding_pair_count") or 0.0)
            exchange_risk = float(neutral_signal_profile.get("exchange_risk") or 0.0)
            multi_path_risk = float(neutral_signal_profile.get("multi_path_risk") or 0.0)
            progression = float(neutral_signal_profile.get("discourse_progression_strength") or 0.0)
            score = (
                0.24 * structure
                + 0.22 * meaning
                + 0.12 * sequence
                + 0.10 * (1 - context_dependency)
                + 0.10 * closure
                + 0.10 * unique_opener
                + 0.06 * min(1.0, binding_pair_count / 3)
                + 0.06 * progression
            )
            score -= 0.10 * exchange_risk
            score -= 0.08 * multi_path_risk
            if candidate["text"].startswith(("而", "但", "却", "因此", "所以", "此外", "对此")):
                score -= 0.18
            unit_count = self._sentence_order_unit_count(candidate["text"], candidate_type)
            if unit_count != self.SENTENCE_ORDER_FIXED_UNIT_COUNT:
                score -= 0.40
            else:
                score += 0.08
        if candidate_type == "insertion_context_unit":
            score = 0.30 * float(neutral_signal_profile.get("bidirectional_validation") or 0.0) + 0.24 * float(neutral_signal_profile.get("backward_link_strength") or 0.0) + 0.24 * float(neutral_signal_profile.get("forward_link_strength") or 0.0) + 0.12 * (1 - context_dependency)
        if candidate_type == "functional_slot_unit":
            slot_ready = 1.0 if neutral_signal_profile.get("slot_explicit_ready") else 0.0
            backward = float(neutral_signal_profile.get("backward_link_strength") or 0.0)
            forward = float(neutral_signal_profile.get("forward_link_strength") or 0.0)
            bidirectional = float(neutral_signal_profile.get("bidirectional_validation") or 0.0)
            countermeasure = float(neutral_signal_profile.get("countermeasure_signal_strength") or 0.0)
            summary_strength = float(neutral_signal_profile.get("summary_strength") or 0.0)
            link_fit = max(bidirectional, backward, forward)
            score = (
                0.18 * closure
                + 0.16 * center
                + 0.12 * (1 - context_dependency)
                + 0.18 * slot_ready
                + 0.12 * link_fit
                + 0.12 * summary_strength
                + 0.12 * countermeasure
            )
            if neutral_signal_profile.get("slot_role") in {"opening", "ending"}:
                score += 0.05
            if neutral_signal_profile.get("slot_function") == "bridge_both_sides":
                score += 0.04
        if candidate_type == "ordered_unit_group":
            sequence_integrity = float(neutral_signal_profile.get("sequence_integrity") or 0.0)
            local_binding = float(neutral_signal_profile.get("local_binding_strength") or 0.0)
            opening_strength = float(neutral_signal_profile.get("opening_signal_strength") or 0.0)
            closing_strength = float(neutral_signal_profile.get("closing_signal_strength") or 0.0)
            block_complexity = float(neutral_signal_profile.get("block_order_complexity") or 0.0)
            context_closure = float(neutral_signal_profile.get("context_closure_score") or 0.0)
            exchange_risk = float(neutral_signal_profile.get("exchange_risk") or 0.0)
            score = (
                0.24 * sequence_integrity
                + 0.18 * local_binding
                + 0.14 * opening_strength
                + 0.14 * closing_strength
                + 0.12 * block_complexity
                + 0.10 * context_closure
                + 0.08 * (1 - exchange_risk)
            )
        if candidate_type == "phrase_or_clause_group":
            clause_count = int((candidate.get("meta") or {}).get("clause_count") or 0)
            score = 0.30 * float(neutral_signal_profile.get("phrase_order_salience") or 0.0) + 0.22 * float(neutral_signal_profile.get("local_binding_strength") or 0.0) + 0.18 * float(neutral_signal_profile.get("sequence_integrity") or 0.0)
            score += min(0.16, clause_count * 0.03)
            if clause_count < 4:
                score -= 0.16

        if candidate.get("meta", {}).get("planner_source") == "llm_candidate_planner":
            score += min(0.10, float(candidate["meta"].get("planner_priority") or 0.0) * 0.10)
        score += self._candidate_scope_bonus(article_context=article_context, candidate=candidate)
        return max(0.0, min(1.0, score))

    def _candidate_scope_bonus(self, *, article_context: dict[str, Any], candidate: dict[str, Any]) -> float:
        paragraph_range = (candidate.get("meta") or {}).get("paragraph_range") or []
        article_paragraph_count = int(article_context["article_profile"].get("paragraph_count") or 0)
        if not paragraph_range or not article_paragraph_count:
            return 0.0
        covered = int(paragraph_range[-1]) - int(paragraph_range[0]) + 1
        if candidate["candidate_type"] == "whole_passage":
            return 0.04 if article_paragraph_count <= 6 else -0.08
        if candidate["candidate_type"] == "multi_paragraph_unit":
            return 0.06 if 2 <= covered <= 3 else -0.04
        if candidate["candidate_type"] == "closed_span":
            return 0.05 if covered <= 2 else -0.04
        if candidate["candidate_type"] == "functional_slot_unit":
            return 0.08 if covered <= 2 else -0.06
        if candidate["candidate_type"] == "ordered_unit_group":
            return 0.08 if covered <= 3 else -0.06
        if candidate["candidate_type"] == "sentence_block_group":
            composition = str((candidate.get("meta") or {}).get("composition") or "")
            if composition == "adjacent_paragraph_pair":
                return 0.08
        return 0.0

    def _planner_score_threshold(self, candidate_type: str) -> float:
        thresholds = {
            "whole_passage": 0.48,
            "closed_span": 0.44,
            "multi_paragraph_unit": 0.48,
            "functional_slot_unit": 0.50,
            "ordered_unit_group": 0.56,
            "sentence_block_group": 0.54,
            "insertion_context_unit": 0.46,
            "phrase_or_clause_group": 0.62,
        }
        return thresholds.get(candidate_type, 0.45)

    def _planner_type_limits(self) -> dict[str, int]:
        return {
            "whole_passage": 1,
            "closed_span": 6,
            "multi_paragraph_unit": 6,
            "functional_slot_unit": 4,
            "ordered_unit_group": 4,
            "sentence_block_group": 6,
            "insertion_context_unit": 6,
            "phrase_or_clause_group": 3,
        }

    def _candidate_priority_boost(self, candidate: dict[str, Any]) -> float:
        meta = candidate.get("meta") or {}
        boost = float(meta.get("planner_priority") or 0.0)
        if meta.get("planner_source") == "llm_candidate_planner":
            boost += 0.05
        return boost

    def _supported_candidate_types(self) -> tuple[str, ...]:
        return (
            "whole_passage",
            "closed_span",
            "multi_paragraph_unit",
            "functional_slot_unit",
            "ordered_unit_group",
            "sentence_block_group",
            "insertion_context_unit",
            "phrase_or_clause_group",
        )

    def _build_neutral_signal_profile(self, *, article_context: dict[str, Any], candidate: dict[str, Any]) -> dict[str, Any]:
        text = candidate["text"]
        paragraph_count = max(1, text.count("\n\n") + 1)
        sentence_count = max(1, len(self.sentence_splitter.split(text)))
        universal = self.universal_tagger._heuristic_tag(
            self._build_span(article_id=article_context["article_id"], span_id=candidate["candidate_id"], text=text, paragraph_count=paragraph_count, sentence_count=sentence_count, source_domain=article_context["source"].get("domain"))
        )
        signal_profile = self._derive_signal_values(article_context=article_context, candidate=candidate, text=text, universal=universal)
        signal_profile.update(self._evaluate_main_idea_eligibility(signal_profile=signal_profile, candidate=candidate))
        task_scoring = self._build_task_scoring_profiles(signal_profile=signal_profile, candidate=candidate)
        signal_profile["task_scoring"] = task_scoring
        signal_profile.update(self._flatten_task_scoring(task_scoring))
        meta = dict(candidate.get("meta") or {})
        meta["scoring"] = task_scoring
        candidate["meta"] = meta
        return signal_profile

    def _project_signal_profile(self, *, signal_layer: dict[str, Any], neutral_signal_profile: dict[str, Any]) -> dict[str, Any]:
        allowed = {entry["signal_id"] for entry in signal_layer.get("signals", [])} | {entry["signal_id"] for entry in signal_layer.get("derived_signals", [])}
        return {
            key: value
            for key, value in neutral_signal_profile.items()
            if key in allowed
            or key == "candidate_type"
            or key == "task_scoring"
            or key.startswith("main_idea_")
            or key.startswith("fill_")
            or key.startswith("sentence_order_")
        }

    def _build_signal_profile(self, *, signal_layer: dict[str, Any], article_context: dict[str, Any], candidate: dict[str, Any]) -> dict[str, Any]:
        neutral_signal_profile = self._build_neutral_signal_profile(article_context=article_context, candidate=candidate)
        return self._project_signal_profile(signal_layer=signal_layer, neutral_signal_profile=neutral_signal_profile)

    def _derive_signal_values(self, *, article_context: dict[str, Any], candidate: dict[str, Any], text: str, universal: Any) -> dict[str, Any]:
        sentence_order_signals = self._derive_sentence_order_signals(
            text=text,
            candidate_type=candidate["candidate_type"],
            universal=universal,
        )
        return {
            "candidate_type": candidate["candidate_type"],
            "document_genre": article_context["article_profile"]["document_genre"],
            "article_purpose_frame": article_context["article_profile"]["article_purpose_frame"],
            "discourse_shape": article_context["article_profile"]["discourse_shape"],
            "core_object": self._core_object(article_context["title"], text),
            "global_main_claim": self._global_main_claim(self.sentence_splitter.split(text)),
            "closure_score": self._closure_score(universal, text),
            "context_dependency": round(max(0.0, 1 - universal.independence_score), 4),
            "single_center_strength": round(universal.single_center_strength, 4),
            "summary_strength": round(universal.summary_strength, 4),
            "titleability": round(universal.titleability, 4),
            "value_judgement_strength": round(universal.value_judgement_strength, 4),
            "example_to_theme_strength": round(universal.example_to_theme_strength, 4),
            "problem_signal_strength": round(universal.problem_signal_strength, 4),
            "method_signal_strength": round(universal.method_signal_strength, 4),
            "branch_focus_strength": round(universal.branch_focus_strength, 4),
            "core_object_anchor_strength": self._core_object_anchor_strength(text),
            "object_scope_stability": self._object_scope_stability(text),
            "turning_focus_strength": self._turning_focus_strength(text, universal),
            "counterintuitive_strength": self._marker_strength(text, COUNTERINTUITIVE_MARKERS),
            "timeline_strength": self._marker_strength(text, TIMELINE_MARKERS),
            "multi_dimension_cohesion": self._multi_dimension_cohesion(text, universal),
            "analysis_to_conclusion_strength": self._analysis_to_conclusion_strength(text, universal),
            "title_namingness": self._title_namingness(text, universal),
            "title_rhetoric_form": self._title_rhetoric_form(text, universal),
            "tail_anchor": self._tail_anchor(text),
            "anchor_focus": self._anchor_focus(text, universal),
            "continuation_type": self._continuation_type(text, universal),
            "progression_mode": self._progression_mode(text, universal),
            "ending_function": self._ending_function(text, universal),
            "tail_extension_signal": self._tail_extension_signal(text),
            "continuation_openness": round(max(universal.continuation_openness, self._tail_extension_signal(text)), 4),
            "direction_uniqueness": round(universal.direction_uniqueness, 4),
            "anchor_clarity": self._anchor_clarity(text),
            "mechanism_signal_strength": self._marker_strength(text, MECHANISM_MARKERS),
            "theme_raise_strength": self._theme_raise_strength(text),
            "judgement_signal_strength": round(universal.value_judgement_strength, 4),
            "case_macro_shift_strength": self._case_macro_shift_strength(text),
            "tension_signal_strength": self._tension_signal_strength(text),
            "opening_anchor_type": sentence_order_signals["opening_anchor_type"],
            "opening_signal_strength": sentence_order_signals["opening_signal_strength"],
            "non_opening_penalty": sentence_order_signals["non_opening_penalty"],
            "middle_structure_type": sentence_order_signals["middle_structure_type"],
            "local_binding_strength": sentence_order_signals["local_binding_strength"],
            "connector_signal_strength": sentence_order_signals["connector_signal_strength"],
            "closing_anchor_type": sentence_order_signals["closing_anchor_type"],
            "closing_signal_strength": sentence_order_signals["closing_signal_strength"],
            "block_order_complexity": sentence_order_signals["block_order_complexity"],
            "sequence_integrity": sentence_order_signals["sequence_integrity"],
            "unique_opener_score": sentence_order_signals["unique_opener_score"],
            "binding_pair_count": sentence_order_signals["binding_pair_count"],
            "exchange_risk": sentence_order_signals["exchange_risk"],
            "function_overlap_score": sentence_order_signals["function_overlap_score"],
            "multi_path_risk": sentence_order_signals["multi_path_risk"],
            "discourse_progression_strength": sentence_order_signals["discourse_progression_strength"],
            "context_closure_score": sentence_order_signals["context_closure_score"],
            "temporal_order_strength": sentence_order_signals["temporal_order_strength"],
            "action_sequence_irreversibility": sentence_order_signals["action_sequence_irreversibility"],
            "tail_settlement_strength": sentence_order_signals["closing_signal_strength"],
            "phrase_order_salience": self._phrase_order_salience(candidate["text"], candidate["candidate_type"]),
            "slot_role": self._slot_role(candidate, article_context=article_context),
            "slot_function": self._slot_function(candidate),
            "slot_explicit_ready": self._slot_explicit_ready(candidate, article_context=article_context),
            "blank_position": self._blank_position(candidate),
            "function_type": self._fill_function_type(candidate, text, universal),
            "logic_relation": self._fill_logic_relation(candidate, text, universal),
            "bidirectional_validation": self._bidirectional_validation(text),
            "reference_dependency": self._reference_dependency(text),
            "abstraction_level": self._abstraction_level(text, universal),
            "backward_link_strength": self._backward_link_strength(text),
            "forward_link_strength": self._forward_link_strength(text),
            "object_match_strength": self._core_object_anchor_strength(text),
            "summary_need_strength": self._summary_need_strength(candidate, universal),
            "focus_shift_strength": self._focus_shift_strength(text, universal),
            "explanation_need_strength": round(universal.explanation_strength, 4),
            "elevation_space_strength": self._elevation_space_strength(text, universal),
            "insertion_fit_strength": self._insertion_fit_strength(candidate, text),
            "multi_constraint_density": self._multi_constraint_density(text),
            "logic_relations": self._logic_relations(text, universal),
            "material_structure_label": self._material_structure_label(text, universal),
            "standalone_readability": round(universal.standalone_readability, 4),
            "semantic_completeness_score": self._semantic_completeness_score(universal, text),
            "theme_words": self._theme_words(text, article_context.get("title")),
            "topic_consistency_strength": self._object_scope_stability(text),
            "cause_effect_strength": self._cause_effect_strength(text),
            "necessary_condition_strength": self._necessary_condition_strength(text),
            "countermeasure_signal_strength": self._countermeasure_signal_strength(text),
            "parallel_enumeration_strength": self._parallel_enumeration_strength(text),
            "benefit_result_strength": self._benefit_result_strength(text, universal),
            "benefit_result_count": self._benefit_result_count(text),
            "non_key_detail_density": self._non_key_detail_density(text, universal),
            "key_sentence_position": self._key_sentence_position(text, universal),
        }

    def _evaluate_main_idea_eligibility(
        self,
        *,
        signal_profile: dict[str, Any],
        candidate: dict[str, Any],
    ) -> dict[str, Any]:
        candidate_type = str(candidate.get("candidate_type") or "")
        text = str(candidate.get("text") or "")
        paragraph_count = max(1, text.count("\n\n") + 1)
        sentence_count = max(1, len(self.sentence_splitter.split(text)))

        if candidate_type not in {"whole_passage", "closed_span", "multi_paragraph_unit"}:
            return {
                "main_idea_eligible": False,
                "main_idea_eligibility_reason": "unsupported_unit_type",
                "main_idea_single_center_score": 0.0,
                "main_idea_closure_score": 0.0,
                "main_idea_lift_score": 0.0,
                "main_idea_example_dominance_score": 1.0,
            }

        single_center_strength = float(signal_profile.get("single_center_strength") or 0.0)
        object_scope_stability = float(signal_profile.get("object_scope_stability") or 0.0)
        core_object_anchor_strength = float(signal_profile.get("core_object_anchor_strength") or 0.0)
        branch_focus_strength = float(signal_profile.get("branch_focus_strength") or 0.0)
        multi_dimension_cohesion = float(signal_profile.get("multi_dimension_cohesion") or 0.0)
        titleability = float(signal_profile.get("titleability") or 0.0)
        closure_score = float(signal_profile.get("closure_score") or 0.0)
        summary_strength = float(signal_profile.get("summary_strength") or 0.0)
        analysis_to_conclusion_strength = float(signal_profile.get("analysis_to_conclusion_strength") or 0.0)
        context_dependency = float(signal_profile.get("context_dependency") or 0.0)
        theme_raise_strength = float(signal_profile.get("theme_raise_strength") or 0.0)
        value_judgement_strength = float(signal_profile.get("value_judgement_strength") or 0.0)
        example_to_theme_strength = float(signal_profile.get("example_to_theme_strength") or 0.0)
        non_key_detail_density = float(signal_profile.get("non_key_detail_density") or 0.0)
        case_macro_shift_strength = float(signal_profile.get("case_macro_shift_strength") or 0.0)
        problem_signal_strength = float(signal_profile.get("problem_signal_strength") or 0.0)
        countermeasure_signal_strength = float(signal_profile.get("countermeasure_signal_strength") or 0.0)

        main_idea_single_center_score = min(
            1.0,
            max(
                0.0,
                0.38 * single_center_strength
                + 0.24 * object_scope_stability
                + 0.18 * core_object_anchor_strength
                + 0.10 * titleability
                + 0.10 * multi_dimension_cohesion
                - 0.18 * branch_focus_strength,
            ),
        )
        main_idea_closure_score = min(
            1.0,
            max(
                0.0,
                0.54 * closure_score
                + 0.18 * summary_strength
                + 0.16 * analysis_to_conclusion_strength
                + 0.08 * (1 - context_dependency)
                + 0.04 * value_judgement_strength,
            ),
        )
        main_idea_lift_score = min(
            1.0,
            max(
                0.0,
                0.34 * titleability
                + 0.18 * summary_strength
                + 0.18 * analysis_to_conclusion_strength
                + 0.12 * theme_raise_strength
                + 0.10 * value_judgement_strength
                + 0.08 * core_object_anchor_strength
                + 0.06 * problem_signal_strength
                + 0.04 * countermeasure_signal_strength
                - 0.18 * non_key_detail_density,
            ),
        )
        main_idea_example_dominance_score = min(
            1.0,
            max(
                0.0,
                0.42 * non_key_detail_density
                + 0.18 * max(0.0, 0.58 - example_to_theme_strength)
                + 0.16 * case_macro_shift_strength
                + 0.14 * branch_focus_strength
                + 0.10 * max(0.0, 0.56 - titleability),
            ),
        )

        single_center_ok = (
            main_idea_single_center_score >= 0.46
            and single_center_strength >= 0.48
            and object_scope_stability >= 0.50
        )
        closure_ok = (
            main_idea_closure_score >= 0.50
            and closure_score >= 0.46
        )
        theme_lift_ok = (
            main_idea_lift_score >= 0.50
            and max(titleability, summary_strength, analysis_to_conclusion_strength, theme_raise_strength) >= 0.42
        )
        example_not_dominant = main_idea_example_dominance_score <= 0.52

        if paragraph_count >= 4 and sentence_count >= 8 and closure_score < 0.50 and summary_strength < 0.42:
            closure_ok = False
        if branch_focus_strength >= 0.62 and multi_dimension_cohesion < 0.56:
            single_center_ok = False
        if non_key_detail_density >= 0.62 and example_to_theme_strength < 0.54:
            example_not_dominant = False

        if not single_center_ok:
            reason = "single_center_weak"
        elif not closure_ok:
            reason = "closure_weak"
        elif not theme_lift_ok:
            reason = "theme_not_liftable"
        elif not example_not_dominant:
            reason = "example_dominant"
        else:
            reason = "main_idea_eligible"

        return {
            "main_idea_eligible": reason == "main_idea_eligible",
            "main_idea_eligibility_reason": reason,
            "main_idea_single_center_score": round(main_idea_single_center_score, 4),
            "main_idea_closure_score": round(main_idea_closure_score, 4),
            "main_idea_lift_score": round(main_idea_lift_score, 4),
            "main_idea_example_dominance_score": round(main_idea_example_dominance_score, 4),
        }

    def _build_task_scoring_profiles(
        self,
        *,
        signal_profile: dict[str, Any],
        candidate: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            "main_idea": self._build_main_idea_scoring(signal_profile=signal_profile, candidate=candidate),
            "sentence_fill": self._build_sentence_fill_scoring(signal_profile=signal_profile, candidate=candidate),
            "sentence_order": self._build_sentence_order_scoring(signal_profile=signal_profile, candidate=candidate),
        }

    def _flatten_task_scoring(self, task_scoring: dict[str, Any]) -> dict[str, Any]:
        flattened: dict[str, Any] = {}
        key_map = {
            "main_idea": "main_idea",
            "sentence_fill": "fill",
            "sentence_order": "sentence_order",
        }
        for family, payload in task_scoring.items():
            prefix = key_map.get(family)
            if not prefix:
                continue
            flattened[f"{prefix}_scores"] = dict(payload.get("structure_scores") or {})
            flattened[f"{prefix}_readiness_score"] = float(payload.get("readiness_score") or 0.0)
            flattened[f"{prefix}_final_score"] = float(payload.get("final_candidate_score") or 0.0)
            flattened[f"{prefix}_risk_penalties"] = dict(payload.get("risk_penalties") or {})
            flattened[f"{prefix}_score_trace"] = dict(payload.get("score_trace") or {})
            flattened[f"{prefix}_recommended"] = bool(payload.get("recommended"))
            flattened[f"{prefix}_needs_review"] = bool(payload.get("needs_review"))
        return flattened

    def _build_main_idea_scoring(
        self,
        *,
        signal_profile: dict[str, Any],
        candidate: dict[str, Any],
    ) -> dict[str, Any]:
        single_center_score = float(signal_profile.get("main_idea_single_center_score") or 0.0)
        closure_score = float(signal_profile.get("main_idea_closure_score") or 0.0)
        theme_lift_score = float(signal_profile.get("main_idea_lift_score") or 0.0)
        example_dominance_penalty = float(signal_profile.get("main_idea_example_dominance_score") or 0.0)
        readiness_score = self._round_score(
            0.40 * single_center_score
            + 0.30 * closure_score
            + 0.30 * theme_lift_score
        )
        final_candidate_score = self._round_score(
            readiness_score - 0.35 * example_dominance_penalty
        )
        recommended = bool(
            signal_profile.get("main_idea_eligible")
            and final_candidate_score >= 0.50
        )
        needs_review = bool(
            readiness_score >= 0.60 and example_dominance_penalty >= 0.28
            or (not recommended and readiness_score >= 0.52 and final_candidate_score >= 0.40)
        )
        reason = str(signal_profile.get("main_idea_eligibility_reason") or "main_idea_unscored")
        return {
            "task_family": "main_idea",
            "structure_scores": {
                "single_center_score": self._round_score(single_center_score),
                "closure_score": self._round_score(closure_score),
                "theme_lift_score": self._round_score(theme_lift_score),
            },
            "readiness_score": readiness_score,
            "risk_penalties": {
                "example_dominance_penalty": self._round_score(example_dominance_penalty),
            },
            "final_candidate_score": final_candidate_score,
            "recommended": recommended,
            "needs_review": needs_review,
            "score_trace": {
                "eligibility_reason": reason,
                "candidate_type": str(candidate.get("candidate_type") or ""),
                "recommended_threshold": 0.50,
            },
        }

    def _build_sentence_fill_scoring(
        self,
        *,
        signal_profile: dict[str, Any],
        candidate: dict[str, Any],
    ) -> dict[str, Any]:
        slot_role = str(signal_profile.get("slot_role") or "")
        slot_function = str(signal_profile.get("slot_function") or "")
        candidate_type = str(candidate.get("candidate_type") or "")
        blank_value_ready = bool((candidate.get("meta") or {}).get("blank_value_ready"))
        carry_dependency_score = float((candidate.get("meta") or {}).get("slot_carry_dependency_score") or 0.0)
        bridge_dependency_score = float((candidate.get("meta") or {}).get("slot_bridge_dependency_score") or 0.0)
        forward_dependency_score = float((candidate.get("meta") or {}).get("slot_forward_dependency_score") or 0.0)
        summary_strength = float(signal_profile.get("summary_strength") or 0.0)
        countermeasure_strength = float(signal_profile.get("countermeasure_signal_strength") or 0.0)
        object_match_strength = float(signal_profile.get("object_match_strength") or 0.0)
        standalone_readability = float(signal_profile.get("standalone_readability") or 0.0)
        sentence_count = max(1, len(self.sentence_splitter.split(str(candidate.get("text") or ""))))
        text_length = len(str(candidate.get("text") or ""))

        if slot_function == "carry_previous":
            primary_slot_dependency_score = carry_dependency_score
        elif slot_function == "bridge_both_sides":
            primary_slot_dependency_score = bridge_dependency_score
        elif slot_function == "lead_next":
            primary_slot_dependency_score = forward_dependency_score
        elif slot_function == "topic_intro":
            primary_slot_dependency_score = self._round_score(0.55 * object_match_strength + 0.45 * forward_dependency_score)
        elif slot_function == "summary":
            primary_slot_dependency_score = summary_strength
        elif slot_function == "ending_summary":
            primary_slot_dependency_score = summary_strength
        elif slot_function == "countermeasure":
            primary_slot_dependency_score = countermeasure_strength
        else:
            primary_slot_dependency_score = max(carry_dependency_score, bridge_dependency_score, forward_dependency_score)

        blank_value_score = 0.0
        if blank_value_ready:
            blank_value_score = max(0.58, primary_slot_dependency_score)
            if slot_function in {"summary", "ending_summary"}:
                blank_value_score = max(blank_value_score, summary_strength)
            if slot_function == "countermeasure":
                blank_value_score = max(blank_value_score, countermeasure_strength)
            if slot_function == "topic_intro":
                blank_value_score = max(blank_value_score, 0.50 * object_match_strength + 0.50 * forward_dependency_score)
        blank_value_score = self._round_score(blank_value_score)

        dependency_scores = [carry_dependency_score, bridge_dependency_score, forward_dependency_score]
        ranked_dependencies = sorted(dependency_scores, reverse=True)
        top_dependency = ranked_dependencies[0] if ranked_dependencies else 0.0
        second_dependency = ranked_dependencies[1] if len(ranked_dependencies) > 1 else 0.0
        role_ambiguity_penalty = self._round_score(max(0.0, 0.28 - (top_dependency - second_dependency)) / 0.28 if top_dependency > 0 else 0.40)
        role_confidence_score = self._round_score(max(0.0, min(1.0, top_dependency + (0.12 if signal_profile.get("slot_explicit_ready") else 0.0) - 0.40 * role_ambiguity_penalty)))
        standalone_penalty = self._round_score(max(0.0, standalone_readability - 0.62) / 0.38)
        overlong_penalty = self._round_score(
            max(
                0.0,
                0.55 * max(0, sentence_count - 1)
                + 0.45 * max(0, text_length - 48) / 52,
            )
        )

        readiness_score = self._round_score(
            0.50 * blank_value_score
            + 0.30 * primary_slot_dependency_score
            + 0.20 * role_confidence_score
        )
        final_candidate_score = self._round_score(
            readiness_score
            - 0.25 * standalone_penalty
            - 0.15 * role_ambiguity_penalty
            - 0.10 * overlong_penalty
        )
        recommended = bool(
            candidate_type == "functional_slot_unit"
            and signal_profile.get("slot_explicit_ready")
            and blank_value_ready
            and final_candidate_score >= 0.54
        )
        needs_review = bool(
            readiness_score >= 0.58 and (
                standalone_penalty >= 0.28
                or role_ambiguity_penalty >= 0.26
                or (not recommended and final_candidate_score >= 0.42)
            )
        )
        return {
            "task_family": "sentence_fill",
            "structure_scores": {
                "blank_value_score": blank_value_score,
                "primary_slot_dependency_score": self._round_score(primary_slot_dependency_score),
                "role_confidence_score": role_confidence_score,
            },
            "readiness_score": readiness_score,
            "risk_penalties": {
                "standalone_penalty": standalone_penalty,
                "role_ambiguity_penalty": role_ambiguity_penalty,
                "overlong_penalty": overlong_penalty,
            },
            "final_candidate_score": final_candidate_score,
            "recommended": recommended,
            "needs_review": needs_review,
            "score_trace": {
                "slot_role": slot_role,
                "slot_function": slot_function,
                "blank_value_ready": blank_value_ready,
                "candidate_type": candidate_type,
                "classification_reason": str((candidate.get("meta") or {}).get("slot_classification_reason") or ""),
            },
        }

    def _build_sentence_order_scoring(
        self,
        *,
        signal_profile: dict[str, Any],
        candidate: dict[str, Any],
    ) -> dict[str, Any]:
        meta = candidate.get("meta") or {}
        first_candidate_indices = list(meta.get("first_candidate_indices") or [])
        last_candidate_indices = list(meta.get("last_candidate_indices") or [])
        pairwise_constraints = list(meta.get("pairwise_constraints") or [])
        local_bindings = list(meta.get("local_bindings") or [])
        grouped_unit_count = int(meta.get("grouped_unit_count") or 0)
        first_stability = 1.0 if first_candidate_indices == [0] else 0.78 if 0 in first_candidate_indices and len(first_candidate_indices) <= 2 else 0.56 if 0 in first_candidate_indices else 0.0
        last_index = self.SENTENCE_ORDER_FIXED_UNIT_COUNT - 1
        last_stability = 1.0 if last_candidate_indices == [last_index] else 0.78 if last_index in last_candidate_indices and len(last_candidate_indices) <= 2 else 0.56 if last_index in last_candidate_indices else 0.0
        first_eligibility_score = self._round_score(
            0.55 * first_stability + 0.45 * float(signal_profile.get("unique_opener_score") or 0.0)
        )
        last_eligibility_score = self._round_score(
            0.55 * last_stability + 0.45 * float(signal_profile.get("closing_signal_strength") or 0.0)
        )
        precedence_count = sum(1 for item in pairwise_constraints if item.get("kind") == "precedence")
        local_binding_count = len(local_bindings)
        pairwise_constraint_score = self._round_score(
            min(1.0, 0.22 * precedence_count + 0.12 * local_binding_count + 0.32 * float(signal_profile.get("sequence_integrity") or 0.0))
        )
        local_binding_score = self._round_score(
            min(1.0, 0.28 * local_binding_count + 0.40 * float(signal_profile.get("local_binding_strength") or 0.0))
        )
        first_instability_penalty = self._round_score(1.0 - first_stability)
        last_instability_penalty = self._round_score(1.0 - last_stability)
        weak_constraint_penalty = self._round_score(max(0.0, 0.55 - pairwise_constraint_score) / 0.55)
        over_merge_penalty = self._round_score(min(1.0, grouped_unit_count / 3))
        readiness_score = self._round_score(
            0.25 * first_eligibility_score
            + 0.25 * last_eligibility_score
            + 0.30 * pairwise_constraint_score
            + 0.20 * local_binding_score
        )
        final_candidate_score = self._round_score(
            readiness_score
            - 0.25 * first_instability_penalty
            - 0.30 * last_instability_penalty
            - 0.20 * weak_constraint_penalty
            - 0.10 * over_merge_penalty
        )
        recommended = bool(
            candidate.get("candidate_type") == "ordered_unit_group"
            and int(meta.get("group_size") or 0) == self.SENTENCE_ORDER_FIXED_UNIT_COUNT
            and final_candidate_score >= 0.56
        )
        needs_review = bool(
            readiness_score >= 0.58 and (
                first_instability_penalty >= 0.24
                or last_instability_penalty >= 0.24
                or weak_constraint_penalty >= 0.22
                or (not recommended and final_candidate_score >= 0.46)
            )
        )
        return {
            "task_family": "sentence_order",
            "structure_scores": {
                "first_eligibility_score": first_eligibility_score,
                "last_eligibility_score": last_eligibility_score,
                "pairwise_constraint_score": pairwise_constraint_score,
                "local_binding_score": local_binding_score,
            },
            "readiness_score": readiness_score,
            "risk_penalties": {
                "first_instability_penalty": first_instability_penalty,
                "last_instability_penalty": last_instability_penalty,
                "weak_constraint_penalty": weak_constraint_penalty,
                "over_merge_penalty": over_merge_penalty,
            },
            "final_candidate_score": final_candidate_score,
            "recommended": recommended,
            "needs_review": needs_review,
            "score_trace": {
                "candidate_type": str(candidate.get("candidate_type") or ""),
                "group_size": int(meta.get("group_size") or 0),
                "first_candidate_indices": first_candidate_indices,
                "last_candidate_indices": last_candidate_indices,
                "pairwise_constraint_count": len(pairwise_constraints),
                "local_binding_count": local_binding_count,
            },
        }

    def _round_score(self, value: float) -> float:
        return round(max(0.0, min(1.0, float(value))), 4)

    def _task_family_scoring_key(self, business_family_id: str) -> str | None:
        if business_family_id == "title_selection":
            return "main_idea"
        if business_family_id in {"sentence_fill", "sentence_order"}:
            return business_family_id
        return None

    def _build_business_feature_profile(
        self,
        *,
        article_context: dict[str, Any],
        candidate: dict[str, Any],
        neutral_signal_profile: dict[str, Any],
    ) -> dict[str, Any]:
        text = candidate["text"]
        marker_hits = self._collect_business_marker_hits(text)
        logic_relations = list(neutral_signal_profile.get("logic_relations") or [])
        conclusion_position = self._conclusion_position(text)
        explicit_marker_group = sorted({marker for values in marker_hits.values() for marker in values})
        feature_type = self._primary_business_feature_type(logic_relations, neutral_signal_profile)
        sentence_order_profile = self._build_sentence_order_business_profile(candidate["text"], candidate["candidate_type"], neutral_signal_profile)
        sentence_fill_profile = self._build_sentence_fill_business_profile(neutral_signal_profile)
        return {
            "feature_type": feature_type,
            "logic_relations": logic_relations,
            "theme_words": list(neutral_signal_profile.get("theme_words") or []),
            "topic_consistency_strength": float(neutral_signal_profile.get("topic_consistency_strength") or 0.0),
            "semantic_completeness_score": float(neutral_signal_profile.get("semantic_completeness_score") or 0.0),
            "readability": float(neutral_signal_profile.get("standalone_readability") or 0.0),
            "material_structure_label": neutral_signal_profile.get("material_structure_label"),
            "conclusion_focus": bool(
                neutral_signal_profile.get("turning_focus_strength", 0.0) >= 0.52
                or neutral_signal_profile.get("cause_effect_strength", 0.0) >= 0.52
                or neutral_signal_profile.get("summary_strength", 0.0) >= 0.60
            ),
            "conclusion_position": conclusion_position,
            "key_sentence_position": neutral_signal_profile.get("key_sentence_position") or conclusion_position,
            "explicit_marker_group": explicit_marker_group,
            "explicit_marker_hits": marker_hits,
            "marker_hit_ratio": self._marker_hit_ratio(marker_hits),
            "require_explicit_marker_ready": bool(explicit_marker_group),
            "require_complete_unit_ready": float(neutral_signal_profile.get("semantic_completeness_score") or 0.0) >= 0.58,
            "non_key_detail_density": float(neutral_signal_profile.get("non_key_detail_density") or 0.0),
            "countermeasure_signal_strength": float(neutral_signal_profile.get("countermeasure_signal_strength") or 0.0),
            "parallel_enumeration_strength": float(neutral_signal_profile.get("parallel_enumeration_strength") or 0.0),
            "sentence_order_profile": sentence_order_profile,
            "sentence_fill_profile": sentence_fill_profile,
        }

    def _score_business_cards(
        self,
        *,
        business_cards: list[dict[str, Any]],
        business_feature_profile: dict[str, Any],
        neutral_signal_profile: dict[str, Any],
        requested_business_card_ids: set[str],
        preferred_business_card_ids: set[str],
        min_business_card_score: float,
        ) -> list[dict[str, Any]]:
        hits: list[dict[str, Any]] = []
        logic_relations = set(business_feature_profile.get("logic_relations") or [])
        explicit_markers = set(business_feature_profile.get("explicit_marker_group") or [])
        for card in business_cards:
            meta = card.get("card_meta") or {}
            business_card_id = meta.get("business_card_id")
            feature_name = str(meta.get("display_name") or business_card_id or "")
            if requested_business_card_ids and business_card_id not in requested_business_card_ids:
                continue
            effective_slot_projection = self._resolve_business_slot_projection(card, business_feature_profile)
            mother_family_id = str(meta.get("mother_family_id") or "")
            if mother_family_id == "sentence_order":
                value = self._score_sentence_order_business_card(card, business_feature_profile)
                if business_card_id in preferred_business_card_ids:
                    value += 0.05
                value = round(min(1.0, max(0.0, value)), 4)
                if value < min_business_card_score:
                    continue
                hits.append(
                    {
                        "business_card_id": business_card_id,
                        "display_name": meta.get("display_name"),
                        "score": value,
                        "reason": f"sentence_order_profile={business_feature_profile.get('sentence_order_profile')}",
                        "slot_projection": effective_slot_projection,
                        "pattern_candidates": list(effective_slot_projection.get("pattern_candidates") or []),
                        "feature_signature": card.get("feature_signature") or {},
                    }
                )
                continue
            if mother_family_id == "sentence_fill":
                value = self._score_sentence_fill_business_card(card, business_feature_profile)
                if business_card_id in preferred_business_card_ids:
                    value += 0.05
                value = round(min(1.0, max(0.0, value)), 4)
                if value < min_business_card_score:
                    continue
                hits.append(
                    {
                        "business_card_id": business_card_id,
                        "display_name": meta.get("display_name"),
                        "score": value,
                        "reason": f"sentence_fill_profile={business_feature_profile.get('sentence_fill_profile')}",
                        "slot_projection": effective_slot_projection,
                        "pattern_candidates": list(effective_slot_projection.get("pattern_candidates") or []),
                        "feature_signature": card.get("feature_signature") or {},
                    }
                )
                continue
            retrieval_profile = card.get("retrieval_profile") or {}
            hard_filters = retrieval_profile.get("hard_filters") or {}
            required_relations = set(hard_filters.get("logic_relations") or [])
            relation_match = (
                1.0
                if required_relations.intersection(logic_relations)
                else self._soft_relation_match(
                    card,
                    logic_relations,
                    neutral_signal_profile,
                    business_feature_profile,
                )
            )
            if required_relations and relation_match < 0.52:
                continue
            if hard_filters.get("require_explicit_marker") and not business_feature_profile.get("require_explicit_marker_ready"):
                continue
            if hard_filters.get("require_complete_unit") and not business_feature_profile.get("require_complete_unit_ready"):
                continue

            soft_filters = retrieval_profile.get("soft_filters") or {}
            ranking_weights = retrieval_profile.get("ranking_weights") or {}
            preferred_markers = set(soft_filters.get("preferred_markers") or [])
            marker_match = 1.0 if preferred_markers and preferred_markers.intersection(explicit_markers) else float(business_feature_profile.get("marker_hit_ratio") or 0.0)
            structural_match = self._business_structural_match(
                soft_filters=soft_filters,
                business_feature_profile=business_feature_profile,
                neutral_signal_profile=neutral_signal_profile,
            )
            semantic_completeness = float(business_feature_profile.get("semantic_completeness_score") or 0.0)
            readability = float(business_feature_profile.get("readability") or 0.0)
            value = (
                float(ranking_weights.get("relation_match", 0.35)) * relation_match
                + float(ranking_weights.get("marker_match", 0.20)) * marker_match
                + float(ranking_weights.get("structural_match", 0.20)) * structural_match
                + float(ranking_weights.get("semantic_completeness", 0.15)) * semantic_completeness
                + float(ranking_weights.get("readability", 0.10)) * readability
            )
            marker_hits = business_feature_profile.get("explicit_marker_hits") or {}
            cause_markers = marker_hits.get("cause_markers") or []
            conclusion_markers = marker_hits.get("conclusion_markers") or []
            if self._business_card_matches_relation_family(card, "因果") and conclusion_markers:
                value += 0.10
                if cause_markers:
                    value += 0.08
            if self._business_card_matches_relation_family(card, "主题词"):
                strongest_relation = max(
                    float(neutral_signal_profile.get("turning_focus_strength") or 0.0),
                    float(neutral_signal_profile.get("cause_effect_strength") or 0.0),
                    float(neutral_signal_profile.get("necessary_condition_strength") or 0.0),
                    float(neutral_signal_profile.get("parallel_enumeration_strength") or 0.0),
                )
                if strongest_relation >= 0.58:
                    value -= 0.12
            runtime_match = card.get("_runtime_match") or {}
            if runtime_match.get("subtype_exact_match"):
                value += 0.05
            if business_card_id in preferred_business_card_ids:
                value += 0.05
            value = round(max(0.0, min(1.0, value)), 4)
            if value < min_business_card_score:
                continue
            hits.append(
                {
                    "business_card_id": business_card_id,
                    "display_name": meta.get("display_name"),
                    "score": value,
                    "reason": f"relation={round(relation_match, 4)}; marker={round(marker_match, 4)}; structure={round(structural_match, 4)}",
                    "slot_projection": effective_slot_projection,
                    "pattern_candidates": list(effective_slot_projection.get("pattern_candidates") or []),
                    "feature_signature": card.get("feature_signature") or {},
                }
            )
        return sorted(hits, key=lambda item: item["score"], reverse=True)[:5]

    def _score_sentence_order_business_card(self, card: dict[str, Any], business_feature_profile: dict[str, Any]) -> float:
        profile = business_feature_profile.get("sentence_order_profile") or {}
        unit_count = int(profile.get("unit_count") or 0)
        opening_rule = str(profile.get("opening_rule") or "")
        closing_rule = str(profile.get("closing_rule") or "")
        binding_rules = set(profile.get("binding_rules") or [])
        logic_modes = set(profile.get("logic_modes") or [])
        opening_signal_strength = float(profile.get("opening_signal_strength") or 0.0)
        closing_signal_strength = float(profile.get("closing_signal_strength") or 0.0)
        local_binding_strength = float(profile.get("local_binding_strength") or 0.0)
        sequence_integrity = float(profile.get("sequence_integrity") or 0.0)
        unique_opener_score = float(profile.get("unique_opener_score") or 0.0)
        binding_pair_count = float(profile.get("binding_pair_count") or 0.0)
        exchange_risk = float(profile.get("exchange_risk") or 0.0)
        function_overlap_score = float(profile.get("function_overlap_score") or 0.0)
        multi_path_risk = float(profile.get("multi_path_risk") or 0.0)
        discourse_progression_strength = float(profile.get("discourse_progression_strength") or 0.0)
        context_closure_score = float(profile.get("context_closure_score") or 0.0)
        temporal_order_strength = float(profile.get("temporal_order_strength") or 0.0)
        action_sequence_irreversibility = float(profile.get("action_sequence_irreversibility") or 0.0)
        scoring_mode_result = self._sentence_order_scoring_mode(card)
        card["_runtime_sentence_order_scoring_mode_trace"] = scoring_mode_result
        if not scoring_mode_result.get("allow_continue"):
            return 0.0
        scoring_mode = str(scoring_mode_result.get("value") or "")

        if scoring_mode == "head_tail_logic":
            score = (
                0.18
                + 0.10 * (1.0 if unit_count == self.SENTENCE_ORDER_FIXED_UNIT_COUNT else 0.0)
                + 0.14 * unique_opener_score
                + 0.12 * min(1.0, binding_pair_count / 3)
                + 0.16 * discourse_progression_strength
                + 0.14 * context_closure_score
                + 0.10 * sequence_integrity
                + 0.06 * opening_signal_strength
                + 0.06 * closing_signal_strength
            )
            if opening_rule in {"definition_opening", "explicit_opening"}:
                score += 0.06
            if closing_rule in {"summary_or_conclusion", "countermeasure"}:
                score += 0.06
            if logic_modes.intersection({"discourse_logic", "timeline_sequence", "action_sequence"}):
                score += 0.08
            if binding_pair_count >= 2:
                score += 0.04
            if unique_opener_score >= 0.58 and context_closure_score >= 0.58:
                score += 0.04
            score -= 0.10 * exchange_risk
            score -= 0.08 * function_overlap_score
            score -= 0.07 * multi_path_risk
            return round(min(1.0, max(0.0, score)), 4)
        if scoring_mode == "head_tail_lock":
            score = 0.28 + 0.18 * (1.0 if unit_count == self.SENTENCE_ORDER_FIXED_UNIT_COUNT else 0.0)
            if opening_rule in {"definition_opening", "explicit_opening"}:
                score += 0.22
            if closing_rule in {"summary_or_conclusion", "countermeasure"}:
                score += 0.22
            score += 0.10 * opening_signal_strength + 0.10 * closing_signal_strength
            score += 0.08 * unique_opener_score + 0.06 * context_closure_score
            score -= 0.08 * exchange_risk
            return round(min(1.0, score), 4)
        if scoring_mode == "deterministic_binding":
            score = 0.22 + 0.20 * len(binding_rules) + 0.22 * local_binding_strength
            if "deterministic_binding" in logic_modes:
                score += 0.20
            score += 0.10 * min(1.0, binding_pair_count / 3)
            score -= 0.10 * exchange_risk
            return round(min(1.0, score), 4)
        if scoring_mode == "discourse_logic":
            score = 0.20 + 0.22 * sequence_integrity
            if "discourse_logic" in logic_modes:
                score += 0.34
            if logic_modes.intersection({"viewpoint_explanation", "problem_solution", "question_answer"}):
                score += 0.18
            score += 0.12 * discourse_progression_strength + 0.10 * context_closure_score
            score -= 0.08 * function_overlap_score
            score -= 0.08 * multi_path_risk
            return round(min(1.0, score), 4)
        if scoring_mode == "timeline_action_sequence":
            score = 0.18 + 0.20 * sequence_integrity
            if "timeline_sequence" in logic_modes:
                score += 0.34
            if "action_sequence" in logic_modes:
                score += 0.24
            score += 0.12 * temporal_order_strength + 0.10 * action_sequence_irreversibility
            score -= 0.08 * exchange_risk
            return round(min(1.0, score), 4)
        return 0.0

    def _score_sentence_fill_business_card(self, card: dict[str, Any], business_feature_profile: dict[str, Any]) -> float:
        profile = business_feature_profile.get("sentence_fill_profile") or {}
        if not bool(profile.get("explicit_slot_ready")):
            return 0.0
        blank_position = str(profile.get("blank_position") or "")
        function_type = str(profile.get("function_type") or "")
        backward = float(profile.get("backward_link_strength") or 0.0)
        forward = float(profile.get("forward_link_strength") or 0.0)
        bidirectional = float(profile.get("bidirectional_validation") or 0.0)
        countermeasure = float(profile.get("countermeasure_signal_strength") or 0.0)
        reference_dependency = float(profile.get("reference_dependency") or 0.0)
        expected_profile_result = self._sentence_fill_expected_profile(card)
        card["_runtime_sentence_fill_expected_profile_trace"] = expected_profile_result
        if not expected_profile_result.get("allow_continue"):
            return 0.0
        expected_profile = expected_profile_result.get("value") or {}

        expected_position = str(expected_profile.get("blank_position") or "")
        expected_function = str(expected_profile.get("business_function") or "")
        score = 0.18
        if blank_position == expected_position:
            score += 0.36
        if function_type == expected_function:
            score += 0.36
        if expected_function == "carry_previous":
            score += 0.12 * backward
        elif expected_function == "lead_next":
            score += 0.12 * forward
        elif expected_function == "bridge_both_sides":
            score += 0.16 * bidirectional
            score += 0.08 * min(backward, forward)
        elif expected_function == "propose_countermeasure":
            score += 0.16 * countermeasure
        else:
            score += 0.08 * reference_dependency
        return round(min(1.0, score), 4)

    def _sentence_fill_business_function(self, *, slot_role: str, slot_function: str) -> str:
        mapping = {
            ("opening", "summary"): "summarize_following_text",
            ("opening", "topic_intro"): "topic_introduction",
            ("middle", "carry_previous"): "carry_previous",
            ("middle", "lead_next"): "lead_next",
            ("middle", "bridge_both_sides"): "bridge_both_sides",
            ("ending", "ending_summary"): "summarize_previous_text",
            ("ending", "countermeasure"): "propose_countermeasure",
        }
        return mapping.get((slot_role, slot_function), "bridge_both_sides")

    def _select_primary_business_card(
        self,
        business_card_hits: list[dict[str, Any]],
        neutral_signal_profile: dict[str, Any],
    ) -> dict[str, Any] | None:
        if not business_card_hits:
            return None
        return business_card_hits[0]

    def _resolve_business_slot_projection(
        self,
        card: dict[str, Any],
        business_feature_profile: dict[str, Any],
    ) -> dict[str, Any]:
        slot_projection = deepcopy(card.get("slot_projection") or {})
        effective = {
            "question_type": slot_projection.get("question_type"),
            "business_subtype": slot_projection.get("business_subtype"),
            "pattern_candidates": list(slot_projection.get("pattern_candidates") or []),
            "type_slots": deepcopy(slot_projection.get("type_slots") or {}),
            "prompt_extras": deepcopy(slot_projection.get("prompt_extras") or {}),
        }
        matched_strategy_ids: list[str] = []
        for strategy_id, strategy in (slot_projection.get("slot_strategy_map") or {}).items():
            if not self._business_slot_strategy_matches(strategy.get("when") or {}, business_feature_profile):
                continue
            matched_strategy_ids.append(strategy_id)
            strategy_patterns = list(strategy.get("pattern_candidates") or [])
            if strategy_patterns:
                effective["pattern_candidates"] = strategy_patterns
            effective["type_slots"].update(deepcopy(strategy.get("type_slots") or {}))
            effective["prompt_extras"].update(deepcopy(strategy.get("prompt_extras") or {}))
        if matched_strategy_ids:
            effective["prompt_extras"].setdefault("business_feature_strategy_ids", matched_strategy_ids)
        effective["matched_strategy_ids"] = matched_strategy_ids
        return effective

    def _business_slot_strategy_matches(
        self,
        expected_conditions: dict[str, Any],
        business_feature_profile: dict[str, Any],
    ) -> bool:
        if not expected_conditions:
            return False
        routing_context = self._build_business_feature_routing_context(business_feature_profile)
        for field, expected in expected_conditions.items():
            actual = routing_context.get(field)
            if isinstance(actual, set):
                if expected not in actual:
                    return False
                continue
            if actual != expected:
                return False
        return True

    def _build_business_feature_routing_context(self, business_feature_profile: dict[str, Any]) -> dict[str, Any]:
        sentence_order_profile = business_feature_profile.get("sentence_order_profile") or {}
        sentence_fill_profile = business_feature_profile.get("sentence_fill_profile") or {}
        logic_modes = set(sentence_order_profile.get("logic_modes") or [])
        order_rules: set[str] = set()
        if "viewpoint_explanation" in logic_modes:
            order_rules.add("viewpoint_plus_explanation")
        if "problem_solution" in logic_modes:
            order_rules.add("problem_plus_solution")
        if "question_answer" in logic_modes:
            order_rules.add("question_plus_answer")
        if "timeline_sequence" in logic_modes:
            order_rules.add("timeline_sequence")
        if "action_sequence" in logic_modes:
            order_rules.add("action_sequence")
        opening_positive_cues: set[str] = set()
        if sentence_order_profile.get("opening_rule") == "definition_opening":
            opening_positive_cues.add("definition_sentence")
        return {
            "opening_rule": sentence_order_profile.get("opening_rule"),
            "closing_rule": sentence_order_profile.get("closing_rule"),
            "binding_rule": set(sentence_order_profile.get("binding_rules") or []),
            "order_rule": order_rules,
            "opening_positive_cue": opening_positive_cues,
            "blank_position": sentence_fill_profile.get("blank_position"),
            "business_function": sentence_fill_profile.get("function_type"),
        }

    def _soft_relation_match(
        self,
        card: dict[str, Any],
        logic_relations: set[str],
        neutral_signal_profile: dict[str, Any],
        business_feature_profile: dict[str, Any],
    ) -> float:
        relation_families = self._business_card_relation_families(card)
        marker_hits = business_feature_profile.get("explicit_marker_hits") or {}
        turning_markers = marker_hits.get("turning_markers") or []
        cause_markers = marker_hits.get("cause_markers") or []
        conclusion_markers = marker_hits.get("conclusion_markers") or []
        necessary_markers = marker_hits.get("necessary_condition_markers") or []
        countermeasure_markers = marker_hits.get("countermeasure_markers") or []
        parallel_markers = marker_hits.get("parallel_markers") or []
        conclusion_position = business_feature_profile.get("conclusion_position")
        if "转折" in relation_families and "转折" in logic_relations:
            return 0.92
        if "转折" in relation_families:
            return max(
                float(neutral_signal_profile.get("turning_focus_strength") or 0.0),
                0.82 if turning_markers else 0.0,
            )
        if "因果" in relation_families and "因果" in logic_relations:
            return 0.92
        if "因果" in relation_families:
            explicit_cause = 0.0
            if cause_markers and conclusion_markers:
                explicit_cause = 0.88
            elif conclusion_markers and conclusion_position in {"tail_or_late", "middle", "opening"}:
                explicit_cause = 0.74
            return max(float(neutral_signal_profile.get("cause_effect_strength") or 0.0), explicit_cause)
        if "必要条件" in relation_families and "必要条件" in logic_relations:
            return 0.92
        if "必要条件" in relation_families:
            explicit_necessary = 0.82 if necessary_markers else 0.0
            if countermeasure_markers and conclusion_position in {"tail_or_late", "middle", "opening"}:
                explicit_necessary = max(explicit_necessary, 0.72)
            return max(float(neutral_signal_profile.get("necessary_condition_strength") or 0.0), explicit_necessary)
        if "并列" in relation_families and "并列" in logic_relations:
            return 0.92
        if "并列" in relation_families:
            explicit_parallel = 0.80 if len(parallel_markers) >= 2 else (0.68 if parallel_markers else 0.0)
            return max(float(neutral_signal_profile.get("parallel_enumeration_strength") or 0.0), explicit_parallel)
        if "主题词" in relation_families:
            return float(neutral_signal_profile.get("topic_consistency_strength") or 0.0)
        return 0.0

    def _sentence_order_scoring_mode(self, card: dict[str, Any]) -> dict[str, Any]:
        feature_signature = card.get("feature_signature") or {}
        card_meta = card.get("card_meta") or {}
        slot_projection = card.get("slot_projection") or {}
        prompt_extras = slot_projection.get("prompt_extras") or {}
        mother_family_id = str(card_meta.get("mother_family_id") or "").strip()
        question_type = str(slot_projection.get("question_type") or "").strip()
        business_card_id = str(card_meta.get("business_card_id") or "").strip()
        explicit_mode = str(feature_signature.get("sentence_order_scoring_mode") or "").strip()
        trace_only_signals = {
            "feature_signature.relation_type": feature_signature.get("relation_type"),
            "feature_signature.relation_focus": feature_signature.get("relation_focus"),
            "slot_projection.type_slots": deepcopy(slot_projection.get("type_slots") or {}),
            "slot_projection.pattern_candidates": list(slot_projection.get("pattern_candidates") or []),
            "slot_projection.prompt_extras.business_core_rule": prompt_extras.get("business_core_rule"),
            "slot_projection.slot_strategy_map_keys": list((slot_projection.get("slot_strategy_map") or {}).keys()),
            "legacy_business_card_id": business_card_id or None,
        }
        family_check = {
            "expected_family": "sentence_order",
            "mother_family_id": mother_family_id or None,
            "question_type": question_type or None,
        }
        missing_fields: list[str] = []
        allowed_modes = {
            "head_tail_logic",
            "head_tail_lock",
            "deterministic_binding",
            "discourse_logic",
            "timeline_action_sequence",
        }

        if mother_family_id and question_type and mother_family_id != question_type:
            family_check["status"] = "contract_conflict"
            return {
                "business_card_id": business_card_id or None,
                "family_check": family_check,
                "status": "contract_conflict",
                "value": None,
                "source_field": None,
                "missing_fields": missing_fields,
                "trace_only_signals": trace_only_signals,
                "allow_continue": False,
            }

        if (mother_family_id and mother_family_id != "sentence_order") or (question_type and question_type != "sentence_order"):
            family_check["status"] = "unsupported"
            return {
                "business_card_id": business_card_id or None,
                "family_check": family_check,
                "status": "unsupported",
                "value": None,
                "source_field": None,
                "missing_fields": missing_fields,
                "trace_only_signals": trace_only_signals,
                "allow_continue": False,
            }

        family_check["status"] = "supported"
        if not explicit_mode:
            missing_fields.append("feature_signature.sentence_order_scoring_mode")
            status = "fallback_trace_only" if any(value not in (None, "", [], {}) for value in trace_only_signals.values()) else "contract_missing"
            return {
                "business_card_id": business_card_id or None,
                "family_check": family_check,
                "status": status,
                "value": None,
                "source_field": None,
                "missing_fields": missing_fields,
                "trace_only_signals": trace_only_signals,
                "allow_continue": False,
            }

        if explicit_mode not in allowed_modes:
            return {
                "business_card_id": business_card_id or None,
                "family_check": family_check,
                "status": "unsupported",
                "value": None,
                "source_field": "feature_signature.sentence_order_scoring_mode",
                "missing_fields": missing_fields,
                "trace_only_signals": trace_only_signals,
                "allow_continue": False,
            }

        return {
            "business_card_id": business_card_id or None,
            "family_check": family_check,
            "status": "resolved",
            "value": explicit_mode,
            "source_field": "feature_signature.sentence_order_scoring_mode",
            "missing_fields": missing_fields,
            "trace_only_signals": trace_only_signals,
            "allow_continue": True,
        }

    def _sentence_fill_expected_profile(self, card: dict[str, Any]) -> dict[str, Any]:
        feature_signature = card.get("feature_signature") or {}
        card_meta = card.get("card_meta") or {}
        slot_projection = card.get("slot_projection") or {}
        type_slots = slot_projection.get("type_slots") or {}
        prompt_extras = slot_projection.get("prompt_extras") or {}
        mother_family_id = str(card_meta.get("mother_family_id") or "").strip()
        question_type = str(slot_projection.get("question_type") or "").strip()
        business_card_id = str(card_meta.get("business_card_id") or "").strip()
        blank_position = str(type_slots.get("blank_position") or "").strip()
        explicit_business_function = str(feature_signature.get("business_function") or "").strip()
        trace_only_signals = {
            "slot_projection.type_slots.function_type": type_slots.get("function_type"),
            "slot_projection.type_slots.logic_relation": type_slots.get("logic_relation"),
            "slot_projection.type_slots.bidirectional_validation": type_slots.get("bidirectional_validation"),
            "slot_projection.pattern_candidates": list(slot_projection.get("pattern_candidates") or []),
            "slot_projection.prompt_extras.business_core_rule": prompt_extras.get("business_core_rule"),
            "legacy_business_card_id": business_card_id or None,
        }
        family_check = {
            "expected_family": "sentence_fill",
            "mother_family_id": mother_family_id or None,
            "question_type": question_type or None,
        }
        missing_fields: list[str] = []
        source_fields: dict[str, str] = {}
        allowed_blank_positions = {"opening", "middle", "ending", "inserted"}
        allowed_business_functions = {
            "summarize_following_text",
            "topic_introduction",
            "summarize_previous_text",
            "propose_countermeasure",
            "carry_previous",
            "lead_next",
            "bridge_both_sides",
        }
        allowed_by_position = {
            "opening": {"summarize_following_text", "topic_introduction"},
            "middle": {"carry_previous", "lead_next", "bridge_both_sides"},
            "ending": {"summarize_previous_text", "propose_countermeasure"},
            "inserted": set(),
        }

        if mother_family_id and question_type and mother_family_id != question_type:
            family_check["status"] = "contract_conflict"
            return {
                "business_card_id": business_card_id or None,
                "family_check": family_check,
                "status": "contract_conflict",
                "value": None,
                "resolved_blank_position": None,
                "resolved_business_function": None,
                "source_fields": source_fields,
                "missing_fields": missing_fields,
                "trace_only_signals": trace_only_signals,
                "allow_continue": False,
            }

        if (mother_family_id and mother_family_id != "sentence_fill") or (question_type and question_type != "sentence_fill"):
            family_check["status"] = "unsupported"
            return {
                "business_card_id": business_card_id or None,
                "family_check": family_check,
                "status": "unsupported",
                "value": None,
                "resolved_blank_position": None,
                "resolved_business_function": None,
                "source_fields": source_fields,
                "missing_fields": missing_fields,
                "trace_only_signals": trace_only_signals,
                "allow_continue": False,
            }

        family_check["status"] = "supported"
        if not blank_position:
            missing_fields.append("slot_projection.type_slots.blank_position")
        else:
            source_fields["blank_position"] = "slot_projection.type_slots.blank_position"
        if not explicit_business_function:
            missing_fields.append("feature_signature.business_function")
        else:
            source_fields["business_function"] = "feature_signature.business_function"

        status = "resolved"
        if missing_fields:
            if len(missing_fields) == 2:
                status = "fallback_trace_only" if any(value not in (None, "", [], {}) for value in trace_only_signals.values()) else "contract_missing"
            else:
                status = "partial_resolved"
        elif blank_position not in allowed_blank_positions or explicit_business_function not in allowed_business_functions:
            status = "unsupported"
        elif explicit_business_function not in allowed_by_position.get(blank_position, set()):
            status = "contract_conflict"

        resolved_blank_position = blank_position or None
        resolved_business_function = explicit_business_function or None
        value = None
        allow_continue = False
        if status == "resolved":
            value = {
                "blank_position": resolved_blank_position,
                "business_function": resolved_business_function,
            }
            allow_continue = True

        return {
            "business_card_id": business_card_id or None,
            "family_check": family_check,
            "status": status,
            "value": value,
            "resolved_blank_position": resolved_blank_position,
            "resolved_business_function": resolved_business_function,
            "source_fields": source_fields,
            "missing_fields": missing_fields,
            "trace_only_signals": trace_only_signals,
            "allow_continue": allow_continue,
        }

    def _business_card_relation_families(self, card: dict[str, Any]) -> set[str]:
        feature_signature = card.get("feature_signature") or {}
        explicit_relation_family = feature_signature.get("relation_family")
        if explicit_relation_family:
            if isinstance(explicit_relation_family, (list, tuple, set)):
                return {str(item).strip() for item in explicit_relation_family if str(item).strip()}
            return {str(explicit_relation_family).strip()}
        canonical_projection = card.get("canonical_projection") or {}
        expected_universal = canonical_projection.get("expected_universal_profile") or {}
        expected_business = canonical_projection.get("expected_business_fields") or {}
        relation_text = self._card_text_blob(
            feature_signature.get("relation_type"),
            expected_business.get("feature_type"),
        )
        logic_relations = {str(item) for item in (expected_universal.get("logic_relations") or []) if str(item).strip()}
        families: set[str] = set()
        if any("转折" in item for item in logic_relations) or "转折" in relation_text:
            families.add("转折")
        if any("因果" in item for item in logic_relations) or "因果" in relation_text:
            families.add("因果")
        if any("必要条件" in item for item in logic_relations) or "必要条件" in relation_text:
            families.add("必要条件")
        if any("并列" in item for item in logic_relations) or "并列" in relation_text:
            families.add("并列")
        if "主题词" in relation_text or "主题" in relation_text:
            families.add("主题词")
        return families

    def _business_card_matches_relation_family(self, card: dict[str, Any], family: str) -> bool:
        return family in self._business_card_relation_families(card)

    def _card_text_blob(self, *values: Any) -> str:
        return " ".join(str(value).strip() for value in values if str(value).strip())

    def _business_structural_match(
        self,
        *,
        soft_filters: dict[str, Any],
        business_feature_profile: dict[str, Any],
        neutral_signal_profile: dict[str, Any],
    ) -> float:
        score = 0.0
        preferred_genres = set(soft_filters.get("preferred_document_genres") or [])
        if preferred_genres and neutral_signal_profile.get("document_genre") in preferred_genres:
            score += 0.35
        preferred_structures = set(soft_filters.get("preferred_material_structures") or [])
        if preferred_structures and neutral_signal_profile.get("material_structure_label") in preferred_structures:
            score += 0.35
        preferred_positions = set(soft_filters.get("preferred_conclusion_position") or [])
        if preferred_positions and business_feature_profile.get("conclusion_position") in preferred_positions:
            score += 0.30
        return round(min(1.0, score), 4)

    def _collect_business_marker_hits(self, text: str) -> dict[str, list[str]]:
        marker_groups = {
            "turning_markers": [marker for marker in TURNING_MARKERS if marker in text],
            "cause_markers": [marker for marker in CAUSE_MARKERS if marker in text],
            "conclusion_markers": [marker for marker in CONCLUSION_MARKERS if marker in text],
            "necessary_condition_markers": [marker for marker in NECESSARY_CONDITION_MARKERS if marker in text],
            "countermeasure_markers": [marker for marker in COUNTERMEASURE_MARKERS if marker in text],
            "parallel_markers": [marker for marker in PARALLEL_MARKERS if marker in text],
        }
        return marker_groups

    def _marker_hit_ratio(self, marker_hits: dict[str, list[str]]) -> float:
        matched = sum(len(values) for values in marker_hits.values())
        active_groups = sum(1 for values in marker_hits.values() if values)
        if matched == 0:
            return 0.0
        return round(min(1.0, 0.18 * matched + 0.12 * active_groups), 4)

    def _primary_business_feature_type(self, logic_relations: list[str], neutral_signal_profile: dict[str, Any]) -> str:
        if "转折" in logic_relations:
            return "转折关系"
        if "因果" in logic_relations:
            return "因果关系"
        if "必要条件" in logic_relations:
            return "必要条件关系"
        if "并列" in logic_relations:
            return "并列关系"
        if float(neutral_signal_profile.get("cause_effect_strength") or 0.0) >= 0.60:
            return "因果关系"
        if float(neutral_signal_profile.get("necessary_condition_strength") or 0.0) >= 0.62:
            return "必要条件关系"
        if float(neutral_signal_profile.get("parallel_enumeration_strength") or 0.0) >= 0.60:
            return "并列关系"
        if float(neutral_signal_profile.get("topic_consistency_strength") or 0.0) >= 0.72:
            return "主题词"
        return "未命中特征"

    def _build_sentence_order_business_profile(
        self,
        text: str,
        candidate_type: str,
        neutral_signal_profile: dict[str, Any],
    ) -> dict[str, Any]:
        unit_count = self._sentence_order_unit_count(text, candidate_type)
        sentences = [sentence.strip() for sentence in self.sentence_splitter.split(text) if sentence.strip()]
        first_sentence = sentences[0] if sentences else text.strip()
        last_sentence = sentences[-1] if sentences else text.strip()
        opening_rule = "weak_opening"
        if any(marker in first_sentence for marker in ORDER_DEFINITION_MARKERS):
            opening_rule = "definition_opening"
        elif not first_sentence.startswith(ORDER_PRONOUN_MARKERS) and not any(
            marker in first_sentence for marker in ("例如", "比如", "就像", "但是", "然而", "不过")
        ):
            opening_rule = "explicit_opening"

        closing_rule = "none"
        if any(marker in last_sentence for marker in SUMMARY_MARKERS + CONCLUSION_MARKERS):
            closing_rule = "summary_or_conclusion"
        elif any(marker in last_sentence for marker in COUNTERMEASURE_MARKERS):
            closing_rule = "countermeasure"

        binding_rules: list[str] = []
        if any(marker in text for marker in ORDER_PRONOUN_MARKERS):
            binding_rules.append("pronoun_reference")
        if any(marker in text for marker in ORDER_TURNING_BINDING_MARKERS):
            binding_rules.append("turning_connector")
        if any(marker in text for marker in ORDER_PARALLEL_BINDING_MARKERS):
            binding_rules.append("parallel_connector")

        logic_modes: list[str] = []
        if float(neutral_signal_profile.get("timeline_strength") or 0.0) >= 0.45 or any(marker in text for marker in TIMELINE_MARKERS):
            logic_modes.append("timeline_sequence")
        if any(marker in text for marker in ORDER_ACTION_MARKERS):
            logic_modes.append("action_sequence")
        opening_anchor_type = str(neutral_signal_profile.get("opening_anchor_type") or "")
        middle_structure_type = str(neutral_signal_profile.get("middle_structure_type") or "")
        if opening_anchor_type in {"viewpoint_opening", "problem_opening"} or middle_structure_type in {"cause_effect_chain", "problem_solution_blocks", "mixed_layers"}:
            logic_modes.append("discourse_logic")
        if opening_anchor_type == "viewpoint_opening" or "观点" in text:
            logic_modes.append("viewpoint_explanation")
        if opening_anchor_type == "problem_opening" or any(marker in text for marker in ORDER_PROBLEM_MARKERS):
            logic_modes.append("problem_solution")
        if any(marker in text for marker in ORDER_QUESTION_OPENINGS + QUESTION_MARKERS):
            logic_modes.append("question_answer")
        if binding_rules:
            logic_modes.append("deterministic_binding")

        deduped_logic_modes: list[str] = []
        for mode in logic_modes:
            if mode not in deduped_logic_modes:
                deduped_logic_modes.append(mode)

        return {
            "unit_count": unit_count,
            "opening_rule": opening_rule,
            "closing_rule": closing_rule,
            "binding_rules": binding_rules,
            "logic_modes": deduped_logic_modes,
            "opening_signal_strength": float(neutral_signal_profile.get("opening_signal_strength") or 0.0),
            "closing_signal_strength": float(neutral_signal_profile.get("closing_signal_strength") or 0.0),
            "local_binding_strength": float(neutral_signal_profile.get("local_binding_strength") or 0.0),
            "sequence_integrity": float(neutral_signal_profile.get("sequence_integrity") or 0.0),
            "unique_opener_score": float(neutral_signal_profile.get("unique_opener_score") or 0.0),
            "binding_pair_count": float(neutral_signal_profile.get("binding_pair_count") or 0.0),
            "exchange_risk": float(neutral_signal_profile.get("exchange_risk") or 0.0),
            "function_overlap_score": float(neutral_signal_profile.get("function_overlap_score") or 0.0),
            "multi_path_risk": float(neutral_signal_profile.get("multi_path_risk") or 0.0),
            "discourse_progression_strength": float(neutral_signal_profile.get("discourse_progression_strength") or 0.0),
            "context_closure_score": float(neutral_signal_profile.get("context_closure_score") or 0.0),
            "temporal_order_strength": float(neutral_signal_profile.get("temporal_order_strength") or 0.0),
            "action_sequence_irreversibility": float(neutral_signal_profile.get("action_sequence_irreversibility") or 0.0),
        }

    def _build_sentence_fill_business_profile(self, neutral_signal_profile: dict[str, Any]) -> dict[str, Any]:
        blank_position = str(neutral_signal_profile.get("blank_position") or "middle")
        backward_link_strength = float(neutral_signal_profile.get("backward_link_strength") or 0.0)
        forward_link_strength = float(neutral_signal_profile.get("forward_link_strength") or 0.0)
        bidirectional_validation = float(neutral_signal_profile.get("bidirectional_validation") or 0.0)
        countermeasure_signal_strength = float(neutral_signal_profile.get("countermeasure_signal_strength") or 0.0)
        summary_need_strength = float(neutral_signal_profile.get("summary_need_strength") or 0.0)
        abstraction_level = float(neutral_signal_profile.get("abstraction_level") or 0.0)
        object_match_strength = float(neutral_signal_profile.get("object_match_strength") or 0.0)
        slot_role = str(neutral_signal_profile.get("slot_role") or "")
        slot_function = str(neutral_signal_profile.get("slot_function") or "")
        explicit_slot_ready = bool(neutral_signal_profile.get("slot_explicit_ready"))
        function_type = "bridge_both_sides"
        if explicit_slot_ready:
            function_type = self._sentence_fill_business_function(slot_role=slot_role, slot_function=slot_function)
        elif blank_position == "opening":
            intro_bias = 0.48 * object_match_strength + 0.22 * forward_link_strength + 0.30 * (1 - summary_need_strength)
            function_type = "summarize_following_text"
            if summary_need_strength < 0.74 and abstraction_level < 0.64 and intro_bias >= 0.40:
                function_type = "topic_introduction"
        elif blank_position == "ending":
            function_type = "propose_countermeasure" if countermeasure_signal_strength >= 0.58 else "summarize_previous_text"
        elif backward_link_strength >= 0.60 and backward_link_strength > forward_link_strength + 0.06 and bidirectional_validation < 0.64:
            function_type = "carry_previous"
        elif forward_link_strength >= 0.60 and forward_link_strength > backward_link_strength + 0.06 and bidirectional_validation < 0.64:
            function_type = "lead_next"
        elif bidirectional_validation >= 0.54 or min(backward_link_strength, forward_link_strength) >= 0.54:
            function_type = "bridge_both_sides"
        elif backward_link_strength >= forward_link_strength:
            function_type = "carry_previous"
        else:
            function_type = "lead_next"

        unit_type = "clause" if str(neutral_signal_profile.get("candidate_type") or "") == "phrase_or_clause_group" else "sentence"
        return {
            "blank_position": blank_position,
            "function_type": function_type,
            "slot_role": slot_role,
            "slot_function": slot_function,
            "explicit_slot_ready": explicit_slot_ready,
            "unit_type": unit_type,
            "logic_relation": str(neutral_signal_profile.get("logic_relation") or "continuation"),
            "backward_link_strength": backward_link_strength,
            "forward_link_strength": forward_link_strength,
            "bidirectional_validation": bidirectional_validation,
            "reference_dependency": float(neutral_signal_profile.get("reference_dependency") or 0.0),
            "countermeasure_signal_strength": countermeasure_signal_strength,
        }

    def _score_material_cards(self, *, material_cards: list[dict[str, Any]], signal_profile: dict[str, Any], candidate: dict[str, Any], min_card_score: float) -> list[dict[str, Any]]:
        hits: list[dict[str, Any]] = []
        for card in material_cards:
            contract = card.get("candidate_contract", {})
            allowed_types = contract.get("allowed_candidate_types", [])
            candidate_type = candidate["candidate_type"]
            candidate_contract_types = self._candidate_contract_types(candidate)
            if allowed_types and not (candidate_contract_types & set(allowed_types)):
                continue
            if not self._passes_card_runtime_gate(card_id=card.get("card_id"), signal_profile=signal_profile, candidate=candidate):
                continue
            reasons = []
            scores = []
            for signal_name, requirement in (card.get("required_signals") or {}).items():
                score, reason = self._evaluate_requirement(signal_profile.get(signal_name), requirement)
                scores.append(score)
                reasons.append(f"{signal_name}:{reason}")
            if not scores:
                continue
            value = sum(scores) / len(scores)
            preferred_types = contract.get("preferred_candidate_types", [])
            if preferred_types and candidate_contract_types & set(preferred_types):
                value += 0.08
                reasons.append("preferred_candidate_type")
            if signal_profile.get("context_dependency", 0.0) > 0.72 and candidate_type in {"whole_passage", "closed_span", "multi_paragraph_unit"}:
                value -= 0.10
                reasons.append("context_dependency_penalty")
            benefit_strength = float(signal_profile.get("benefit_result_strength") or 0.0)
            benefit_count = float(signal_profile.get("benefit_result_count") or 0.0)
            if card.get("card_id") == "title_material.benefit_result":
                boost = 0.18 * benefit_strength + 0.14 * benefit_count
                value += boost
                reasons.append(f"benefit_result_boost={round(boost, 4)}")
            elif benefit_strength >= 0.72 and benefit_count >= 0.52:
                if card.get("card_id") == "title_material.value_commentary":
                    penalty = 0.12 if benefit_strength >= float(signal_profile.get("value_judgement_strength") or 0.0) else 0.06
                    value -= penalty
                    reasons.append(f"benefit_result_penalty={round(penalty, 4)}")
                elif card.get("card_id") == "title_material.multi_dimension_unification":
                    value -= 0.10
                    reasons.append("benefit_result_vs_multi_dimension_penalty")
                elif card.get("card_id") == "title_material.turning_focus" and benefit_strength > float(signal_profile.get("turning_focus_strength") or 0.0) + 0.06:
                    value -= 0.10
                    reasons.append("benefit_result_vs_turning_penalty")
                elif card.get("card_id") in {"title_material.example_then_recovery", "title_material.case_to_theme_elevation"} and float(signal_profile.get("example_to_theme_strength") or 0.0) < 0.66:
                    value -= 0.12
                    reasons.append("benefit_result_vs_example_penalty")
            value = round(max(0.0, min(value, 1.0)), 4)
            if value < min_card_score:
                continue
            hits.append({"card_id": card["card_id"], "display_name": card["display_name"], "score": value, "generation_archetype": card.get("default_generation_archetype"), "selection_core": card.get("selection_core"), "reason": "; ".join(reasons)})
        return sorted(hits, key=lambda item: item["score"], reverse=True)[:5]

    def _candidate_contract_types(self, candidate: dict[str, Any]) -> set[str]:
        candidate_type = str(candidate.get("candidate_type") or "")
        contract_types = {candidate_type}
        if candidate_type == "functional_slot_unit":
            contract_types.update({"closed_span", "multi_paragraph_unit"})
        if candidate_type == "ordered_unit_group":
            contract_types.update({"sentence_block_group"})
        return contract_types

    def _resolve_slots(self, question_card: dict[str, Any], material_card_id: str, business_card_hit: dict[str, Any] | None = None) -> dict[str, Any]:
        resolved = dict(question_card.get("base_slots", {}))
        for item in question_card.get("material_card_overrides", []):
            if item.get("material_card") == material_card_id:
                resolved.update(item.get("slot_overrides", {}))
                break
        slot_projection = (business_card_hit or {}).get("slot_projection") or {}
        resolved.update(slot_projection.get("type_slots", {}))
        return resolved

    def _build_prompt_extras(self, business_card_hit: dict[str, Any] | None) -> dict[str, Any]:
        if not business_card_hit:
            return {}
        slot_projection = business_card_hit.get("slot_projection") or {}
        prompt_extras = dict(slot_projection.get("prompt_extras") or {})
        prompt_extras.setdefault("business_feature_card_id", business_card_hit.get("business_card_id"))
        prompt_extras.setdefault("business_feature_card_label", business_card_hit.get("display_name"))
        return prompt_extras

    def _score_candidate_quality(
        self,
        *,
        business_family_id: str,
        signal_profile: dict[str, Any],
        top_card_score: float,
        top_business_score: float,
        retrieval_match_score: float,
        length_fit_score: float,
        candidate: dict[str, Any],
        article_context: dict[str, Any],
    ) -> float:
        closure = float(signal_profile.get("closure_score") or 0.0)
        dependency = float(signal_profile.get("context_dependency") or 0.0)
        titleability = float(signal_profile.get("titleability") or 0.0)
        sequence_integrity = float(signal_profile.get("sequence_integrity") or 0.0)
        continuation = float(signal_profile.get("continuation_openness") or 0.0)
        support = max(titleability, sequence_integrity, continuation, 1 - dependency)
        score = 0.56 * top_card_score + 0.16 * top_business_score + 0.14 * closure + 0.14 * support
        score += 0.06 * retrieval_match_score + 0.14 * length_fit_score

        if business_family_id == "title_selection":
            article_paragraph_count = int(article_context["article_profile"].get("paragraph_count") or 0)
            candidate_paragraph_count = candidate["text"].count("\n\n") + 1
            score += 0.06 * float(signal_profile.get("object_scope_stability") or 0.0)
            score += 0.06 * float(signal_profile.get("title_namingness") or 0.0)
            score += 0.08 * float(signal_profile.get("benefit_result_strength") or 0.0)
            if candidate["candidate_type"] == "whole_passage" and article_paragraph_count > 10:
                score -= 0.22
            elif candidate["candidate_type"] == "multi_paragraph_unit" and article_paragraph_count > 24:
                score -= 0.14
            elif candidate["candidate_type"] == "multi_paragraph_unit" and article_paragraph_count > 12 and candidate_paragraph_count >= 3:
                score -= 0.08
            if self._enumeration_density(candidate["text"]) >= 0.26:
                score -= 0.12
            if self._starts_with_enumerative_opening(candidate["text"]):
                score -= 0.08
            if self._heading_like_opening(candidate["text"]):
                score -= 0.10
            if self._directive_style_opening(candidate["text"]):
                score -= 0.08
        elif business_family_id == "sentence_order":
            unit_count = self._sentence_order_unit_count(candidate["text"], candidate["candidate_type"])
            structure_score = self._sentence_order_structure_completeness(signal_profile, candidate)
            meaning_score = self._sentence_order_meaningfulness(candidate["text"], signal_profile, candidate["candidate_type"])
            score += 0.10 * structure_score
            score += 0.10 * meaning_score
            if candidate["candidate_type"] != "sentence_block_group":
                score -= 0.35
            if unit_count != self.SENTENCE_ORDER_FIXED_UNIT_COUNT:
                score -= 0.40
            else:
                score += 0.10
            if len(candidate["text"]) < 120:
                score -= 0.12
            if meaning_score < 0.60:
                score -= 0.18
            if structure_score < 0.60:
                score -= 0.18
        elif business_family_id == "sentence_fill":
            article_paragraph_count = int(article_context["article_profile"].get("paragraph_count") or 0)
            candidate_paragraph_count = candidate["text"].count("\n\n") + 1
            blank_position = str(signal_profile.get("blank_position") or "")
            if candidate["candidate_type"] == "whole_passage" and (article_paragraph_count > 10 or len(candidate["text"]) > 1400):
                score -= 0.22
            if candidate["candidate_type"] == "whole_passage" and blank_position in {"middle", "inserted"}:
                score -= 0.10
            if candidate["candidate_type"] == "multi_paragraph_unit" and candidate_paragraph_count >= 3 and len(candidate["text"]) > 900:
                score -= 0.08
        elif business_family_id == "continuation":
            article_paragraph_count = int(article_context["article_profile"].get("paragraph_count") or 0)
            candidate_paragraph_count = candidate["text"].count("\n\n") + 1
            paragraph_range = (candidate.get("meta") or {}).get("paragraph_range") or []
            if candidate["candidate_type"] == "whole_passage" and (article_paragraph_count > 8 or len(candidate["text"]) > 1400):
                score -= 0.28
            if candidate["candidate_type"] == "multi_paragraph_unit" and candidate_paragraph_count >= 3 and len(candidate["text"]) > 900:
                score -= 0.12
            if paragraph_range and article_paragraph_count:
                tail_distance = max(0, article_paragraph_count - 1 - int(paragraph_range[-1]))
                if tail_distance == 0:
                    score += 0.06
                elif tail_distance == 1:
                    score += 0.03
                elif tail_distance >= 3:
                    score -= 0.06

        scoring_key = self._task_family_scoring_key(business_family_id)
        task_scoring = (signal_profile.get("task_scoring") or {}).get(scoring_key or "")
        if isinstance(task_scoring, dict):
            family_final_score = float(task_scoring.get("final_candidate_score") or 0.0)
            score = 0.76 * score + 0.24 * family_final_score
            if not bool(task_scoring.get("recommended")):
                score -= 0.04
            if bool(task_scoring.get("needs_review")):
                score -= 0.02

        return max(0.0, min(1.0, score))

    def _adapt_candidate_window(
        self,
        *,
        article_context: dict[str, Any],
        candidate: dict[str, Any],
        target_length: int | None,
        length_tolerance: int,
        enable_anchor_adaptation: bool,
        preserve_anchor: bool,
    ) -> dict[str, Any]:
        if not enable_anchor_adaptation or not target_length:
            return candidate

        text = str(candidate.get("text") or "")
        if not text:
            return candidate
        min_length = max(80, target_length - max(0, length_tolerance))
        max_length = target_length + max(0, length_tolerance)
        if min_length <= len(text) <= max_length:
            return candidate

        sentences: list[str] = article_context.get("sentences") or []
        if not sentences:
            return candidate

        sentence_span = self._candidate_sentence_span(article_context=article_context, candidate=candidate)
        if sentence_span is None:
            return candidate
        start, end = sentence_span
        window_start = start
        window_end = end

        if len(text) < min_length:
            while len("".join(sentences[window_start : window_end + 1])) < min_length and (window_start > 0 or window_end < len(sentences) - 1):
                can_expand_left = window_start > 0
                can_expand_right = window_end < len(sentences) - 1
                if can_expand_left and (not can_expand_right or len(sentences[window_start - 1]) <= len(sentences[window_end + 1])):
                    window_start -= 1
                elif can_expand_right:
                    window_end += 1
                elif can_expand_left:
                    window_start -= 1
            adapted_text = "".join(sentences[window_start : window_end + 1]).strip()
            if not adapted_text:
                return candidate
            return self._with_anchor_adaptation(
                candidate=candidate,
                adapted_text=adapted_text,
                article_context=article_context,
                sentence_range=[window_start, window_end],
                reason="expanded_to_target_length",
            )

        if not preserve_anchor:
            return candidate

        core_anchor = self._pick_core_anchor_index(sentences[start : end + 1]) + start
        window_start = core_anchor
        window_end = core_anchor
        while len("".join(sentences[window_start : window_end + 1])) < min_length and (window_start > 0 or window_end < len(sentences) - 1):
            can_expand_left = window_start > 0
            can_expand_right = window_end < len(sentences) - 1
            left_len = len(sentences[window_start - 1]) if can_expand_left else 10**9
            right_len = len(sentences[window_end + 1]) if can_expand_right else 10**9
            if can_expand_left and (not can_expand_right or left_len <= right_len):
                window_start -= 1
            elif can_expand_right:
                window_end += 1
            elif can_expand_left:
                window_start -= 1
        adapted_text = "".join(sentences[window_start : window_end + 1]).strip()
        if not adapted_text:
            return candidate
        return self._with_anchor_adaptation(
            candidate=candidate,
            adapted_text=adapted_text,
            article_context=article_context,
            sentence_range=[window_start, window_end],
            reason="trimmed_around_anchor",
        )

    def _with_anchor_adaptation(
        self,
        *,
        candidate: dict[str, Any],
        adapted_text: str,
        article_context: dict[str, Any],
        sentence_range: list[int],
        reason: str,
    ) -> dict[str, Any]:
        updated_meta = dict(candidate.get("meta") or {})
        updated_meta["sentence_range"] = sentence_range
        updated_meta["paragraph_range"] = self._paragraph_range_for_sentence_range(article_context=article_context, sentence_range=sentence_range)
        updated_meta["anchor_adaptation"] = {
            "adapted": True,
            "reason": reason,
            "target_length": len(adapted_text),
            "sentence_range": sentence_range,
            "paragraph_range": updated_meta.get("paragraph_range") or [],
        }
        return {
            **candidate,
            "text": adapted_text,
            "meta": updated_meta,
        }

    def _adapt_cached_candidate(
        self,
        *,
        candidate: dict[str, Any],
        query_terms: list[str],
        target_length: int | None,
        length_tolerance: int,
        enable_anchor_adaptation: bool,
        preserve_anchor: bool,
        theme_words: list[str],
    ) -> dict[str, Any]:
        if not enable_anchor_adaptation or not target_length:
            return candidate
        text = str(candidate.get("text") or "")
        if not text:
            return candidate
        min_length = max(80, target_length - max(0, length_tolerance))
        max_length = target_length + max(0, length_tolerance)
        if min_length <= len(text) <= max_length:
            return candidate
        sentences = [sentence for sentence in self.sentence_splitter.split(text) if sentence.strip()]
        if len(sentences) <= 1:
            return candidate

        anchor_terms = [term for term in (query_terms or theme_words or []) if term]
        center_index = len(sentences) // 2
        if anchor_terms:
            scored: list[tuple[int, int, int]] = []
            for idx, sentence in enumerate(sentences):
                hit_count = sum(1 for term in anchor_terms if term in sentence)
                scored.append((hit_count, -abs(idx - center_index), idx))
            scored.sort(reverse=True)
            if scored and scored[0][0] > 0:
                center_index = scored[0][2]

        window_start = center_index
        window_end = center_index
        while len("".join(sentences[window_start : window_end + 1])) < min_length and (window_start > 0 or window_end < len(sentences) - 1):
            can_expand_left = window_start > 0
            can_expand_right = window_end < len(sentences) - 1
            left_len = len(sentences[window_start - 1]) if can_expand_left else 10**9
            right_len = len(sentences[window_end + 1]) if can_expand_right else 10**9
            if can_expand_left and (not can_expand_right or left_len <= right_len):
                window_start -= 1
            elif can_expand_right:
                window_end += 1
            elif can_expand_left:
                window_start -= 1
        adapted_text = "".join(sentences[window_start : window_end + 1]).strip()
        if len(adapted_text) > max_length:
            best_text = adapted_text
            best_start = window_start
            best_end = window_end
            best_gap = abs(len(adapted_text) - target_length)
            for start in range(window_start, window_end + 1):
                for end in range(start, window_end + 1):
                    if preserve_anchor and not (start <= center_index <= end):
                        continue
                    segment = "".join(sentences[start : end + 1]).strip()
                    if len(segment) < min_length:
                        continue
                    gap = abs(len(segment) - target_length)
                    if gap < best_gap:
                        best_text = segment
                        best_gap = gap
                        best_start = start
                        best_end = end
            adapted_text = best_text
            window_start = best_start
            window_end = best_end
        if not adapted_text or adapted_text == text:
            return candidate
        updated_meta = dict(candidate.get("meta") or {})
        updated_meta["anchor_adaptation"] = {
            "adapted": True,
            "reason": "precomputed_window_trim",
            "target_length": target_length,
            "actual_length": len(adapted_text),
            "sentence_range": [window_start, window_end],
            "anchor_sentence_index": center_index,
        }
        return {
            **candidate,
            "text": adapted_text,
            "meta": updated_meta,
        }

    def _candidate_sentence_span(self, *, article_context: dict[str, Any], candidate: dict[str, Any]) -> tuple[int, int] | None:
        meta = candidate.get("meta") or {}
        sentence_range = meta.get("sentence_range") or []
        if len(sentence_range) >= 2:
            return max(0, int(sentence_range[0])), max(0, int(sentence_range[-1]))
        paragraph_range = meta.get("paragraph_range") or []
        paragraph_sentences: list[list[str]] = article_context.get("paragraph_sentences") or []
        paragraph_sentence_offsets: list[int] = article_context.get("paragraph_sentence_offsets") or []
        if len(paragraph_range) >= 2 and paragraph_sentences and paragraph_sentence_offsets:
            start_paragraph = max(0, int(paragraph_range[0]))
            end_paragraph = min(len(paragraph_sentences) - 1, int(paragraph_range[-1]))
            start_offset = paragraph_sentence_offsets[start_paragraph]
            end_offset = paragraph_sentence_offsets[end_paragraph] + max(0, len(paragraph_sentences[end_paragraph]) - 1)
            return start_offset, end_offset
        if candidate.get("candidate_type") == "whole_passage":
            sentences: list[str] = article_context.get("sentences") or []
            if sentences:
                return 0, len(sentences) - 1
        return None

    def _paragraph_range_for_sentence_range(self, *, article_context: dict[str, Any], sentence_range: list[int]) -> list[int]:
        paragraph_sentences: list[list[str]] = article_context.get("paragraph_sentences") or []
        paragraph_sentence_offsets: list[int] = article_context.get("paragraph_sentence_offsets") or []
        if not paragraph_sentences or not paragraph_sentence_offsets or len(sentence_range) < 2:
            return []
        start_sentence = int(sentence_range[0])
        end_sentence = int(sentence_range[-1])
        start_paragraph = 0
        end_paragraph = len(paragraph_sentences) - 1
        for index, offset in enumerate(paragraph_sentence_offsets):
            paragraph_end = offset + max(0, len(paragraph_sentences[index]) - 1)
            if start_sentence >= offset and start_sentence <= paragraph_end:
                start_paragraph = index
                break
        for index, offset in enumerate(paragraph_sentence_offsets):
            paragraph_end = offset + max(0, len(paragraph_sentences[index]) - 1)
            if end_sentence >= offset and end_sentence <= paragraph_end:
                end_paragraph = index
                break
        return [start_paragraph, end_paragraph]

    def _pick_core_anchor_index(self, sentences: list[str]) -> int:
        if not sentences:
            return 0
        marker_groups = [
            TURNING_MARKERS,
            CAUSE_MARKERS,
            CONCLUSION_MARKERS,
            NECESSARY_CONDITION_MARKERS,
            COUNTERMEASURE_MARKERS,
            PARALLEL_MARKERS,
            THEME_HINT_MARKERS,
        ]
        scored: list[tuple[int, float]] = []
        for index, sentence in enumerate(sentences):
            score = 0.0
            for markers in marker_groups:
                score += sum(1 for marker in markers if marker in sentence) * 0.28
            if sentence.rstrip().endswith(("。", "！", "？", "!", "?")):
                score += 0.08
            if len(sentence.strip()) >= 18:
                score += 0.06
            if any(marker in sentence for marker in SUMMARY_MARKERS):
                score += 0.12
            scored.append((index, score))
        return max(scored, key=lambda item: item[1])[0]

    def _build_retrieval_match_profile(
        self,
        *,
        article_context: dict[str, Any],
        candidate: dict[str, Any],
        query_terms: list[str],
        target_length: int | None,
        length_tolerance: int,
    ) -> dict[str, Any]:
        text = str(candidate.get("text") or "")
        hits = [term for term in query_terms if term and term in text]
        article_title = str(article_context.get("title") or "")
        title_hits = [term for term in query_terms if term and term in article_title]
        if query_terms:
            overlap_ratio = len(set(hits + title_hits)) / max(1, len(set(query_terms)))
            match_score = min(1.0, overlap_ratio + 0.10 * len(title_hits))
        else:
            overlap_ratio = 0.0
            match_score = 0.0
        if target_length:
            gap = abs(len(text) - target_length)
            length_fit_score = max(0.0, 1 - gap / max(target_length, max(1, length_tolerance)))
        else:
            length_fit_score = 0.0
        return {
            "query_terms": query_terms[:8],
            "query_hits": (hits + title_hits)[:8],
            "match_score": round(match_score, 4),
            "length_fit_score": round(length_fit_score, 4),
            "target_length": target_length,
            "actual_length": len(text),
        }

    def _build_consumable_text(
        self,
        *,
        business_family_id: str,
        candidate: dict[str, Any],
        presentation: dict[str, Any],
    ) -> str:
        if business_family_id == "sentence_order":
            lead = str(presentation.get("lead_context") or "").strip()
            block = str(presentation.get("sortable_block") or candidate["text"]).strip()
            parts = [part for part in [lead, block] if part]
            return "\n\n".join(parts) if parts else candidate["text"]
        if business_family_id == "sentence_fill":
            blanked = str(presentation.get("blanked_text") or "").strip()
            if blanked:
                return blanked
        if business_family_id == "continuation":
            tail_window = str(presentation.get("tail_window_text") or "").strip()
            if tail_window:
                return tail_window
        return candidate["text"]

    def _build_presentation(
        self,
        *,
        business_family_id: str,
        article_context: dict[str, Any],
        candidate: dict[str, Any],
        signal_profile: dict[str, Any],
    ) -> dict[str, Any]:
        if business_family_id == "sentence_order":
            return self._build_sentence_order_presentation(article_context=article_context, candidate=candidate, signal_profile=signal_profile)
        if business_family_id == "sentence_fill":
            return self._build_sentence_fill_presentation(candidate=candidate, signal_profile=signal_profile)
        if business_family_id == "continuation":
            return self._build_continuation_presentation(candidate=candidate, signal_profile=signal_profile)
        return {}

    def _build_sentence_order_presentation(
        self,
        *,
        article_context: dict[str, Any],
        candidate: dict[str, Any],
        signal_profile: dict[str, Any],
    ) -> dict[str, Any]:
        paragraph_range = (candidate.get("meta") or {}).get("paragraph_range") or []
        paragraphs: list[str] = article_context["paragraphs"]
        lead_context = ""
        follow_context = ""
        source_paragraph = ""
        if paragraph_range:
            start = max(0, int(paragraph_range[0]))
            end = min(len(paragraphs) - 1, int(paragraph_range[-1]))
            if start > 0:
                lead_context = paragraphs[start - 1]
            if end + 1 < len(paragraphs):
                follow_context = paragraphs[end + 1]
            source_paragraph = "\n\n".join(paragraphs[start : end + 1]).strip()
        if not source_paragraph:
            source_paragraph = candidate["text"]
        sortable_units = list((candidate.get("meta") or {}).get("ordered_units") or [])
        if not sortable_units:
            sortable_units = self._sentence_order_units(candidate["text"], candidate["candidate_type"])
        if len(sortable_units) != self.SENTENCE_ORDER_FIXED_UNIT_COUNT and source_paragraph:
            source_units = self._sentence_order_units(source_paragraph, candidate["candidate_type"])
            if len(source_units) >= self.SENTENCE_ORDER_FIXED_UNIT_COUNT:
                sortable_units = source_units[: self.SENTENCE_ORDER_FIXED_UNIT_COUNT]
        sortable_block = self._format_sentence_order_units(sortable_units, fallback_text=candidate["text"])
        return {
            "mode": "sentence_order",
            "lead_context": lead_context,
            "follow_context": follow_context,
            "source_paragraph": source_paragraph,
            "sortable_block": sortable_block,
            "sortable_units": sortable_units,
            "structure_hints": {
                "opening_anchor_type": signal_profile.get("opening_anchor_type"),
                "middle_structure_type": signal_profile.get("middle_structure_type"),
                "closing_anchor_type": signal_profile.get("closing_anchor_type"),
            },
        }

    def _build_sentence_fill_presentation(
        self,
        *,
        candidate: dict[str, Any],
        signal_profile: dict[str, Any],
    ) -> dict[str, Any]:
        blank_position = str(signal_profile.get("blank_position") or "middle")
        function_type = str(signal_profile.get("function_type") or "bridge")
        window_text = self._fill_window_text(candidate["text"], blank_position)
        anchor_payload = self._build_sentence_fill_anchor(window_text, blank_position)
        return {
            "mode": "sentence_fill",
            "blank_position": blank_position,
            "function_type": function_type,
            "context_window": anchor_payload["context_window"],
            "blanked_text": anchor_payload["blanked_text"],
            "answer_anchor_text": anchor_payload["answer_anchor_text"],
            "answer_anchor_kind": anchor_payload["answer_anchor_kind"],
        }

    def _format_sentence_order_units(self, units: list[str], *, fallback_text: str) -> str:
        cleaned_units = [unit.strip() for unit in units if unit and unit.strip()]
        if len(cleaned_units) < 2:
            return fallback_text
        circled = "①②③④⑤⑥⑦⑧⑨⑩"
        lines: list[str] = []
        for index, unit in enumerate(cleaned_units):
            marker = circled[index] if index < len(circled) else f"{index + 1}."
            lines.append(f"{marker} {unit}")
        return "\n".join(lines)

    def _build_continuation_presentation(
        self,
        *,
        candidate: dict[str, Any],
        signal_profile: dict[str, Any],
    ) -> dict[str, Any]:
        cleaned_text = self._strip_front_matter(candidate["text"])
        tail_window_text = self._continuation_window_text(cleaned_text)
        return {
            "mode": "continuation",
            "anchor_focus": signal_profile.get("anchor_focus"),
            "ending_function": signal_profile.get("ending_function"),
            "tail_window_text": tail_window_text or cleaned_text,
        }

    def _fill_window_text(self, text: str, blank_position: str) -> str:
        paragraphs = [paragraph for paragraph in text.split("\n\n") if paragraph.strip()]
        if len(paragraphs) > 1:
            if blank_position == "opening":
                return "\n\n".join(paragraphs[: min(2, len(paragraphs))]).strip()
            if blank_position == "ending":
                return "\n\n".join(paragraphs[max(0, len(paragraphs) - 2) :]).strip()
            middle = len(paragraphs) // 2
            start = max(0, middle - 1)
            end = min(len(paragraphs), start + 3)
            return "\n\n".join(paragraphs[start:end]).strip()

        sentences = [sentence.strip() for sentence in self.sentence_splitter.split(text) if sentence.strip()]
        if len(sentences) <= 4:
            return text.strip()
        if blank_position == "opening":
            return "".join(sentences[:4]).strip()
        if blank_position == "ending":
            return "".join(sentences[-4:]).strip()
        middle = len(sentences) // 2
        start = max(0, middle - 2)
        end = min(len(sentences), start + 4)
        return "".join(sentences[start:end]).strip()

    def _build_sentence_fill_anchor(self, text: str, blank_position: str) -> dict[str, str]:
        stripped = text.strip()
        if not stripped:
            return {
                "context_window": stripped,
                "blanked_text": "[BLANK]",
                "answer_anchor_text": "",
                "answer_anchor_kind": "none",
            }
        sentences = [sentence.strip() for sentence in self.sentence_splitter.split(stripped) if sentence.strip()]
        if len(sentences) <= 1:
            return self._build_clause_level_fill_anchor(stripped, blank_position)
        if blank_position == "opening":
            target_idx = 0
        elif blank_position == "ending":
            target_idx = len(sentences) - 1
        else:
            target_idx = max(1, min(len(sentences) - 2, len(sentences) // 2)) if len(sentences) >= 3 else len(sentences) // 2
        target_sentence = sentences[target_idx]
        if len(target_sentence) >= 44 and any(token in target_sentence for token in ("，", "；", "、")):
            clause_anchor = self._build_clause_level_fill_anchor(stripped, blank_position, preferred_sentence=target_sentence)
            if clause_anchor.get("answer_anchor_text"):
                return clause_anchor
        blanked_sentences = list(sentences)
        blanked_sentences[target_idx] = "[BLANK]"
        return {
            "context_window": "".join(sentences).strip(),
            "blanked_text": "".join(blanked_sentences).strip(),
            "answer_anchor_text": target_sentence.strip("。；;！？!，, "),
            "answer_anchor_kind": "sentence",
        }

    def _build_clause_level_fill_anchor(self, text: str, blank_position: str, preferred_sentence: str | None = None) -> dict[str, str]:
        source = (preferred_sentence or text or "").strip()
        clauses = [part.strip() for part in re.split(r"(?<=[，；;])", source) if part.strip()]
        if len(clauses) <= 1:
            cleaned = source.strip("。；;！？!，, ")
            return {
                "context_window": (text or "").strip(),
                "blanked_text": "[BLANK]" if source == text.strip() else text.replace(source, "[BLANK]", 1),
                "answer_anchor_text": cleaned,
                "answer_anchor_kind": "sentence" if cleaned else "none",
            }
        if blank_position == "opening":
            target_idx = 0
        elif blank_position == "ending":
            target_idx = len(clauses) - 1
        else:
            target_idx = max(1, min(len(clauses) - 2, len(clauses) // 2)) if len(clauses) >= 3 else len(clauses) // 2
        target_clause = clauses[target_idx]
        blanked_clauses = list(clauses)
        blanked_clauses[target_idx] = "[BLANK]"
        blanked_source = "".join(blanked_clauses).strip()
        context_window = (text or "").strip()
        blanked_text = context_window.replace(source, blanked_source, 1) if source and source != context_window else blanked_source
        return {
            "context_window": context_window,
            "blanked_text": blanked_text,
            "answer_anchor_text": target_clause.strip("。；;！？!，, "),
            "answer_anchor_kind": "clause",
        }

    def _insert_blank_marker(self, text: str, blank_position: str) -> str:
        marker = "[BLANK]"
        stripped = text.strip()
        if not stripped:
            return marker
        paragraphs = [paragraph for paragraph in stripped.split("\n\n") if paragraph.strip()]
        if blank_position == "opening":
            return f"{marker}\n\n{stripped}" if len(paragraphs) > 1 else f"{marker}{stripped}"
        if blank_position == "ending":
            return f"{stripped}\n\n{marker}" if len(paragraphs) > 1 else f"{stripped}{marker}"
        if len(paragraphs) > 1:
            insert_at = 1 if len(paragraphs) <= 2 else len(paragraphs) // 2
            pieces = paragraphs[:insert_at] + [marker] + paragraphs[insert_at:]
            return "\n\n".join(pieces)

        sentences = [sentence.strip() for sentence in self.sentence_splitter.split(stripped) if sentence.strip()]
        if len(sentences) <= 1:
            return f"{sentences[0]}{marker}" if sentences else marker
        if blank_position in {"middle", "inserted"}:
            insert_at = max(1, min(len(sentences) - 1, (len(sentences) + 1) // 2))
        else:
            insert_at = max(1, len(sentences) // 2)
        pieces = sentences[:insert_at] + [marker] + sentences[insert_at:]
        return "".join(pieces)

    def _sanitize_article_text(self, text: str) -> str:
        if not text:
            return ""
        lines = [line.strip() for line in text.replace("\r\n", "\n").split("\n")]
        cleaned: list[str] = []
        leading = True
        boilerplate_patterns = [
            re.compile(r"^\u65b0\u534e\u793e.*\u7535$"),
            re.compile(r"^\u5206\u4eab\u8ba9\u66f4\u591a\u4eba\u770b\u5230$"),
            re.compile(r"^\u539f\u6807\u9898[:\uff1a].*$"),
            re.compile(r"^.*\u8bb0\u8005.*\u6444$"),
            re.compile(r"^\u5404\u4f4d\u4ee3\u8868[:\uff1a]$"),
        ]
        for line in lines:
            if not line:
                if cleaned and cleaned[-1]:
                    cleaned.append("")
                continue
            if leading and any(pattern.match(line) for pattern in boilerplate_patterns):
                continue
            leading = False
            cleaned.append(line)
        return "\n".join(cleaned).strip()

    def _passes_card_runtime_gate(self, *, card_id: str | None, signal_profile: dict[str, Any], candidate: dict[str, Any]) -> bool:
        if card_id and card_id.startswith("order_material."):
            unit_count = self._sentence_order_unit_count(candidate["text"], candidate["candidate_type"])
            structure_score = self._sentence_order_structure_completeness(signal_profile, candidate)
            meaning_score = self._sentence_order_meaningfulness(candidate["text"], signal_profile, candidate["candidate_type"])
            unique_opener_score = float(signal_profile.get("unique_opener_score") or 0.0)
            binding_pair_count = float(signal_profile.get("binding_pair_count") or 0.0)
            exchange_risk = float(signal_profile.get("exchange_risk") or 0.0)
            function_overlap_score = float(signal_profile.get("function_overlap_score") or 0.0)
            multi_path_risk = float(signal_profile.get("multi_path_risk") or 0.0)
            discourse_progression_strength = float(signal_profile.get("discourse_progression_strength") or 0.0)
            context_closure_score = float(signal_profile.get("context_closure_score") or 0.0)
            if candidate["candidate_type"] not in {"sentence_block_group", "ordered_unit_group"}:
                return False
            if unit_count != self.SENTENCE_ORDER_FIXED_UNIT_COUNT:
                return False
            if structure_score < 0.60 or meaning_score < 0.60:
                return False
            if unique_opener_score < 0.58:
                return False
            if binding_pair_count < 2:
                return False
            if exchange_risk > 0.38 or multi_path_risk > 0.40:
                return False
            if function_overlap_score > 0.46:
                return False
            if discourse_progression_strength < 0.54 or context_closure_score < 0.56:
                return False
        if card_id and card_id.startswith("title_material."):
            if not bool(signal_profile.get("main_idea_eligible")):
                return False
        if card_id == "order_material.phrase_order_variant":
            return False
        if card_id == "order_material.dual_anchor_lock":
            if float(signal_profile.get("opening_signal_strength") or 0.0) < 0.68:
                return False
            if signal_profile.get("middle_structure_type") not in {"local_binding", "cause_effect_chain"}:
                return False
            if signal_profile.get("closing_anchor_type") not in {"summary", "conclusion"}:
                return False
            if self._looks_like_service_qa(candidate["text"]):
                return False
        if card_id == "order_material.carry_parallel_expand":
            paragraph_range = (candidate.get("meta") or {}).get("paragraph_range") or []
            if signal_profile.get("opening_anchor_type") != "upper_context_link":
                return False
            if signal_profile.get("middle_structure_type") != "parallel_expansion":
                return False
            if signal_profile.get("closing_anchor_type") not in {"summary", "conclusion"}:
                return False
            if not paragraph_range or int(paragraph_range[0]) <= 0:
                return False
            if float(signal_profile.get("function_overlap_score") or 0.0) > 0.34:
                return False
            if float(signal_profile.get("exchange_risk") or 0.0) > 0.28:
                return False
        if card_id == "order_material.viewpoint_reason_action":
            if signal_profile.get("opening_anchor_type") != "viewpoint_opening":
                return False
            if signal_profile.get("middle_structure_type") != "cause_effect_chain":
                return False
            if signal_profile.get("closing_anchor_type") != "call_to_action":
                return False
            if float(signal_profile.get("function_overlap_score") or 0.0) > 0.30:
                return False
            if float(signal_profile.get("multi_path_risk") or 0.0) > 0.30:
                return False
        if card_id == "order_material.problem_solution_case_blocks":
            if signal_profile.get("opening_anchor_type") != "problem_opening":
                return False
            if signal_profile.get("middle_structure_type") != "problem_solution_blocks":
                return False
            if signal_profile.get("closing_anchor_type") not in {"case_support", "summary", "conclusion"}:
                return False
            if float(signal_profile.get("block_order_complexity") or 0.0) < 0.72:
                return False
        if card_id == "title_material.benefit_result":
            if candidate["candidate_type"] not in {"multi_paragraph_unit", "closed_span"}:
                return False
            if float(signal_profile.get("benefit_result_strength") or 0.0) < 0.72:
                return False
            if float(signal_profile.get("benefit_result_count") or 0.0) < 0.50:
                return False
            if float(signal_profile.get("analysis_to_conclusion_strength") or 0.0) < 0.48:
                return False
            if float(signal_profile.get("closure_score") or 0.0) < 0.58:
                return False
            if float(signal_profile.get("turning_focus_strength") or 0.0) > float(signal_profile.get("benefit_result_strength") or 0.0) + 0.08:
                return False
            if float(signal_profile.get("problem_signal_strength") or 0.0) > float(signal_profile.get("benefit_result_strength") or 0.0) + 0.10:
                return False
        if card_id == "title_material.plain_main_recovery":
            if candidate["candidate_type"] != "whole_passage":
                return False
            if len(candidate["text"]) > 760 or candidate["text"].count("\n\n") + 1 > 3:
                return False
            if float(signal_profile.get("single_center_strength") or 0.0) < 0.82:
                return False
            if float(signal_profile.get("closure_score") or 0.0) < 0.78:
                return False
            if float(signal_profile.get("titleability") or 0.0) < 0.76:
                return False
            if float(signal_profile.get("core_object_anchor_strength") or 0.0) < 0.72:
                return False
            if float(signal_profile.get("context_dependency") or 0.0) > 0.18:
                return False
            if float(signal_profile.get("branch_focus_strength") or 0.0) > 0.34:
                return False
            if float(signal_profile.get("parallel_enumeration_strength") or 0.0) > 0.30:
                return False
            if float(signal_profile.get("problem_signal_strength") or 0.0) >= 0.52:
                return False
            if float(signal_profile.get("countermeasure_signal_strength") or 0.0) >= 0.46:
                return False
            if float(signal_profile.get("benefit_result_strength") or 0.0) >= 0.62:
                return False
            if float(signal_profile.get("turning_focus_strength") or 0.0) >= 0.58:
                return False
            if float(signal_profile.get("example_to_theme_strength") or 0.0) >= 0.56:
                return False
            if float(signal_profile.get("multi_dimension_cohesion") or 0.0) >= 0.56:
                return False
        return True

    def _looks_like_service_qa(self, text: str) -> bool:
        sentences = [sentence.strip() for sentence in self.sentence_splitter.split(text) if sentence.strip()]
        if not sentences:
            return False
        head = sentences[0]
        if "\uff1f" in head or "?" in head:
            return True
        return bool(re.search(r"(\u5982\u4f55|\u600e\u4e48|\u600e\u6837|\u4f55\u7533\u8bf7|\u53ef\u901a\u8fc7)", head))

    def _sentence_order_unit_count(self, text: str, candidate_type: str) -> int:
        if candidate_type == "phrase_or_clause_group":
            return len([part.strip() for part in re.split(r"[，、；,;]", text) if part.strip()])
        return len([sentence.strip() for sentence in self.sentence_splitter.split(text) if sentence.strip()])

    def _sentence_order_units(self, text: str, candidate_type: str) -> list[str]:
        if candidate_type == "phrase_or_clause_group":
            return [part.strip() for part in re.split(r"[，、；,;]", text) if part.strip()]
        return [sentence.strip() for sentence in self.sentence_splitter.split(text) if sentence.strip()]

    def _derive_sentence_order_signals(self, *, text: str, candidate_type: str, universal: Any) -> dict[str, Any]:
        opening_anchor_type = self._opening_anchor_type(text)
        opening_signal_strength = self._opening_signal_strength(text)
        non_opening_penalty = self._non_opening_penalty(text)
        middle_structure_type = self._middle_structure_type(text, universal)
        local_binding_strength = self._local_binding_strength(text)
        connector_signal_strength = self._connector_signal_strength(text)
        closing_anchor_type = self._closing_anchor_type(text)
        closing_signal_strength = self._closing_signal_strength(text)
        block_order_complexity = self._block_order_complexity(text, universal, candidate_type)
        sequence_integrity = self._sequence_integrity(text)
        unique_opener_score = self._unique_opener_score(text, candidate_type)
        binding_pair_count = float(self._binding_pair_count(text, candidate_type))
        function_overlap_score = self._function_overlap_score(text, candidate_type)
        temporal_order_strength = self._temporal_order_strength(text, candidate_type)
        action_sequence_irreversibility = self._action_sequence_irreversibility(text, candidate_type)
        discourse_progression_strength = self._discourse_progression_strength(
            opening_signal_strength=opening_signal_strength,
            local_binding_strength=local_binding_strength,
            closing_signal_strength=closing_signal_strength,
            sequence_integrity=sequence_integrity,
            function_overlap_score=function_overlap_score,
            temporal_order_strength=temporal_order_strength,
        )
        context_closure_score = self._context_closure_score(
            discourse_progression_strength=discourse_progression_strength,
            closing_signal_strength=closing_signal_strength,
            sequence_integrity=sequence_integrity,
            context_dependency=round(max(0.0, 1 - universal.independence_score), 4),
        )
        exchange_risk = self._exchange_risk(
            text=text,
            candidate_type=candidate_type,
            unique_opener_score=unique_opener_score,
            binding_pair_count=binding_pair_count,
            function_overlap_score=function_overlap_score,
            context_closure_score=context_closure_score,
            temporal_order_strength=temporal_order_strength,
            action_sequence_irreversibility=action_sequence_irreversibility,
        )
        multi_path_risk = self._multi_path_risk(
            unique_opener_score=unique_opener_score,
            binding_pair_count=binding_pair_count,
            exchange_risk=exchange_risk,
            function_overlap_score=function_overlap_score,
            discourse_progression_strength=discourse_progression_strength,
            context_closure_score=context_closure_score,
        )
        return {
            "opening_anchor_type": opening_anchor_type,
            "opening_signal_strength": opening_signal_strength,
            "non_opening_penalty": non_opening_penalty,
            "middle_structure_type": middle_structure_type,
            "local_binding_strength": local_binding_strength,
            "connector_signal_strength": connector_signal_strength,
            "closing_anchor_type": closing_anchor_type,
            "closing_signal_strength": closing_signal_strength,
            "block_order_complexity": block_order_complexity,
            "sequence_integrity": sequence_integrity,
            "unique_opener_score": unique_opener_score,
            "binding_pair_count": round(binding_pair_count, 4),
            "function_overlap_score": function_overlap_score,
            "temporal_order_strength": temporal_order_strength,
            "action_sequence_irreversibility": action_sequence_irreversibility,
            "discourse_progression_strength": discourse_progression_strength,
            "context_closure_score": context_closure_score,
            "exchange_risk": exchange_risk,
            "multi_path_risk": multi_path_risk,
        }

    def _sentence_order_role(self, unit: str, *, is_last: bool = False) -> str:
        text = (unit or "").strip()
        if not text:
            return "empty"
        if any(marker in text for marker in SUMMARY_MARKERS + CONCLUSION_MARKERS):
            return "summary"
        if any(marker in text for marker in COUNTERMEASURE_MARKERS):
            return "action"
        if any(marker in text for marker in ORDER_PROBLEM_MARKERS):
            return "problem"
        if any(marker in text for marker in ORDER_QUESTION_MARKERS):
            return "question"
        if any(marker in text for marker in ORDER_DEFINITION_MARKERS):
            return "definition"
        if any(marker in text for marker in ("例如", "比如", "譬如", "就像")):
            return "example"
        if text.startswith(ORDER_PRONOUN_MARKERS) or text.startswith(CONTEXTUAL_OPENINGS):
            return "dependent"
        if any(marker in text for marker in ORDER_TURNING_BINDING_MARKERS):
            return "turning"
        if any(marker in text for marker in ORDER_PARALLEL_BINDING_MARKERS):
            return "parallel"
        if any(marker in text for marker in ORDER_ACTION_MARKERS):
            return "timeline"
        if any(marker in text for marker in ("认为", "指出", "表明", "说明", "可见", "关键在于")):
            return "viewpoint"
        if is_last:
            return "tail_statement"
        return "statement"

    def _unit_opener_score(self, unit: str, *, index: int) -> float:
        text = (unit or "").strip()
        if not text:
            return 0.0
        score = 0.28
        if any(marker in text for marker in ORDER_DEFINITION_MARKERS):
            score += 0.32
        if any(marker in text for marker in ORDER_PROBLEM_MARKERS):
            score += 0.22
        if any(marker in text for marker in ("认为", "指出", "表明", "说明", "关键在于")):
            score += 0.18
        if any(marker in text for marker in ORDER_SUMMARY_CLOSING_MARKERS + COUNTERMEASURE_MARKERS):
            score -= 0.18
        if text.startswith(ORDER_PRONOUN_MARKERS) or any(marker in text for marker in ("例如", "比如", "就像")):
            score -= 0.28
        if text.startswith(CONTEXTUAL_OPENINGS):
            score -= 0.22
        if any(marker in text for marker in ORDER_TURNING_BINDING_MARKERS):
            score -= 0.10
        if index == 0 and any(marker in text for marker in ("首先", "第一步", "起初", "一开始")):
            score += 0.16
        if index == 0:
            score += 0.06
        return round(max(0.0, min(1.0, score)), 4)

    def _unique_opener_score(self, text: str, candidate_type: str) -> float:
        units = self._sentence_order_units(text, candidate_type)
        if not units:
            return 0.0
        scores = sorted((self._unit_opener_score(unit, index=index) for index, unit in enumerate(units)), reverse=True)
        best = scores[0]
        second = scores[1] if len(scores) > 1 else 0.0
        gap = max(0.0, best - second)
        return round(max(0.0, min(1.0, 0.68 * best + 0.32 * gap)), 4)

    def _binding_pair_count(self, text: str, candidate_type: str) -> int:
        units = self._sentence_order_units(text, candidate_type)
        count = 0
        for index in range(len(units) - 1):
            current = units[index]
            nxt = units[index + 1]
            if nxt.startswith(ORDER_PRONOUN_MARKERS) or nxt.startswith(CONTEXTUAL_OPENINGS):
                count += 1
                continue
            if any(marker in nxt for marker in ORDER_TURNING_BINDING_MARKERS + ORDER_PARALLEL_BINDING_MARKERS):
                count += 1
                continue
            if any(marker in nxt for marker in ("随后", "然后", "接着", "最后", "之后", "再", "下一步", "这样", "如此")):
                count += 1
                continue
            if any(marker in current for marker in ORDER_QUESTION_MARKERS) and any(marker in nxt for marker in ("因为", "所以", "答案", "关键")):
                count += 1
                continue
            if any(marker in current for marker in ORDER_PROBLEM_MARKERS) and any(marker in nxt for marker in ORDER_SOLUTION_MARKERS):
                count += 1
                continue
            if ("只有" in current and "才" in nxt) or ("如果" in current and any(marker in nxt for marker in ("那么", "就", "才", "还要"))):
                count += 1
                continue
        return min(count, 4)

    def _function_overlap_score(self, text: str, candidate_type: str) -> float:
        units = self._sentence_order_units(text, candidate_type)
        if len(units) <= 1:
            return 1.0
        roles = [self._sentence_order_role(unit, is_last=index == len(units) - 1) for index, unit in enumerate(units)]
        role_counts = Counter(roles)
        duplicate_pairs = sum(max(0, count - 1) for count in role_counts.values())
        directive_density = sum(1 for unit in units if any(marker in unit for marker in COUNTERMEASURE_MARKERS)) / max(len(units), 1)
        overlap = 0.72 * (duplicate_pairs / max(len(units) - 1, 1)) + 0.28 * directive_density
        return round(max(0.0, min(1.0, overlap)), 4)

    def _temporal_order_strength(self, text: str, candidate_type: str) -> float:
        units = self._sentence_order_units(text, candidate_type)
        if not units:
            return 0.0
        marker_hits = sum(sum(unit.count(marker) for marker in TIMELINE_MARKERS + ORDER_ACTION_MARKERS) for unit in units)
        score = 0.18 * min(marker_hits, 4)
        if any("先" in unit and "后" in unit for unit in units):
            score += 0.18
        return round(max(0.0, min(1.0, score)), 4)

    def _action_sequence_irreversibility(self, text: str, candidate_type: str) -> float:
        units = self._sentence_order_units(text, candidate_type)
        if not units:
            return 0.0
        action_hits = sum(1 for unit in units if any(marker in unit for marker in ORDER_ACTION_MARKERS))
        irreversible_markers = sum(1 for unit in units if any(marker in unit for marker in ("首先", "其次", "再次", "最后", "第一步", "第二步", "第三步", "先", "再", "随后")))
        score = 0.14 * min(action_hits, 4) + 0.18 * min(irreversible_markers, 3)
        return round(max(0.0, min(1.0, score)), 4)

    def _discourse_progression_strength(
        self,
        *,
        opening_signal_strength: float,
        local_binding_strength: float,
        closing_signal_strength: float,
        sequence_integrity: float,
        function_overlap_score: float,
        temporal_order_strength: float,
    ) -> float:
        score = (
            0.22 * opening_signal_strength
            + 0.18 * local_binding_strength
            + 0.22 * closing_signal_strength
            + 0.20 * sequence_integrity
            + 0.08 * temporal_order_strength
            + 0.10 * (1 - function_overlap_score)
        )
        return round(max(0.0, min(1.0, score)), 4)

    def _context_closure_score(
        self,
        *,
        discourse_progression_strength: float,
        closing_signal_strength: float,
        sequence_integrity: float,
        context_dependency: float,
    ) -> float:
        score = (
            0.34 * discourse_progression_strength
            + 0.26 * closing_signal_strength
            + 0.22 * sequence_integrity
            + 0.18 * (1 - context_dependency)
        )
        return round(max(0.0, min(1.0, score)), 4)

    def _exchange_risk(
        self,
        *,
        text: str,
        candidate_type: str,
        unique_opener_score: float,
        binding_pair_count: float,
        function_overlap_score: float,
        context_closure_score: float,
        temporal_order_strength: float,
        action_sequence_irreversibility: float,
    ) -> float:
        parallel_density = self._marker_strength(text, ORDER_PARALLEL_BINDING_MARKERS)
        directive_density = sum(1 for unit in self._sentence_order_units(text, candidate_type) if any(marker in unit for marker in COUNTERMEASURE_MARKERS))
        directive_density = min(1.0, directive_density / max(len(self._sentence_order_units(text, candidate_type)), 1))
        score = (
            0.30 * function_overlap_score
            + 0.20 * (1 - min(1.0, binding_pair_count / 3))
            + 0.18 * (1 - unique_opener_score)
            + 0.12 * (1 - context_closure_score)
            + 0.10 * parallel_density
            + 0.06 * directive_density
            + 0.02 * (1 - temporal_order_strength)
            + 0.02 * (1 - action_sequence_irreversibility)
        )
        return round(max(0.0, min(1.0, score)), 4)

    def _multi_path_risk(
        self,
        *,
        unique_opener_score: float,
        binding_pair_count: float,
        exchange_risk: float,
        function_overlap_score: float,
        discourse_progression_strength: float,
        context_closure_score: float,
    ) -> float:
        score = (
            0.34 * exchange_risk
            + 0.22 * function_overlap_score
            + 0.18 * (1 - unique_opener_score)
            + 0.14 * (1 - min(1.0, binding_pair_count / 3))
            + 0.06 * (1 - discourse_progression_strength)
            + 0.06 * (1 - context_closure_score)
        )
        return round(max(0.0, min(1.0, score)), 4)

    def _sentence_order_structure_completeness(self, signal_profile: dict[str, Any], candidate: dict[str, Any]) -> float:
        unit_count = self._sentence_order_unit_count(candidate["text"], candidate["candidate_type"])
        opening_strength = float(signal_profile.get("opening_signal_strength") or 0.0)
        local_binding = float(signal_profile.get("local_binding_strength") or 0.0)
        block_complexity = float(signal_profile.get("block_order_complexity") or 0.0)
        closing_strength = float(signal_profile.get("closing_signal_strength") or 0.0)
        sequence_integrity = float(signal_profile.get("sequence_integrity") or 0.0)
        unique_opener = float(signal_profile.get("unique_opener_score") or 0.0)
        progression = float(signal_profile.get("discourse_progression_strength") or 0.0)
        closure = float(signal_profile.get("context_closure_score") or 0.0)
        exchange_risk = float(signal_profile.get("exchange_risk") or 0.0)
        unit_score = 1.0 if unit_count == self.SENTENCE_ORDER_FIXED_UNIT_COUNT else 0.0
        closing_bonus = closing_strength if signal_profile.get("closing_anchor_type") != "none" else closing_strength * 0.35
        value = (
            0.14 * unit_score
            + 0.14 * opening_strength
            + 0.12 * local_binding
            + 0.12 * block_complexity
            + 0.14 * max(sequence_integrity, closing_bonus)
            + 0.10 * unique_opener
            + 0.12 * progression
            + 0.12 * closure
        )
        value -= 0.10 * exchange_risk
        if self._looks_like_service_qa(candidate["text"]):
            value -= 0.18
        return round(max(0.0, min(1.0, value)), 4)

    def _sentence_order_meaningfulness(self, text: str, signal_profile: dict[str, Any], candidate_type: str) -> float:
        unit_count = self._sentence_order_unit_count(text, candidate_type)
        dependency = float(signal_profile.get("context_dependency") or 0.0)
        opening_strength = float(signal_profile.get("opening_signal_strength") or 0.0)
        local_binding = float(signal_profile.get("local_binding_strength") or 0.0)
        sequence_integrity = float(signal_profile.get("sequence_integrity") or 0.0)
        block_complexity = float(signal_profile.get("block_order_complexity") or 0.0)
        binding_pair_count = float(signal_profile.get("binding_pair_count") or 0.0)
        function_overlap_score = float(signal_profile.get("function_overlap_score") or 0.0)
        multi_path_risk = float(signal_profile.get("multi_path_risk") or 0.0)
        unit_score = 1.0 if unit_count == self.SENTENCE_ORDER_FIXED_UNIT_COUNT else 0.0
        value = (
            0.18 * unit_score
            + 0.14 * (1 - dependency)
            + 0.12 * opening_strength
            + 0.14 * local_binding
            + 0.12 * sequence_integrity
            + 0.08 * block_complexity
            + 0.12 * min(1.0, binding_pair_count / 3)
            + 0.10 * (1 - function_overlap_score)
        )
        value -= 0.10 * multi_path_risk
        if len(text) < 120:
            value -= 0.12
        if text.strip().startswith(CONTEXTUAL_OPENINGS):
            value -= 0.10
        if self._looks_like_service_qa(text):
            value -= 0.18
        return round(max(0.0, min(1.0, value)), 4)

    def _strip_front_matter(self, text: str) -> str:
        paragraphs = [paragraph.strip() for paragraph in text.strip().split("\n\n") if paragraph.strip()]
        if not paragraphs:
            return ""
        patterns = [
            re.compile(r"^\u65b0\u534e\u793e.*\u7535$"),
            re.compile(r"^.*\u8bb0\u8005.*\u6444$"),
            re.compile(r"^\u5404\u4f4d\u4ee3\u8868[:\uff1a]$"),
            re.compile(r"^\u653f\u5e9c\u5de5\u4f5c\u62a5\u544a.*$"),
        ]
        cleaned = list(paragraphs)
        while cleaned and any(pattern.match(cleaned[0]) for pattern in patterns):
            cleaned.pop(0)
        return "\n\n".join(cleaned).strip()

    def _continuation_window_text(self, text: str) -> str:
        stripped = text.strip()
        if not stripped:
            return ""
        paragraphs = [paragraph.strip() for paragraph in stripped.split("\n\n") if paragraph.strip()]
        if len(paragraphs) >= 2:
            joined = "\n\n".join(paragraphs[-2:]).strip()
            if len(joined) <= 900:
                return joined
        sentences = [sentence.strip() for sentence in self.sentence_splitter.split(stripped) if sentence.strip()]
        if len(sentences) <= 5:
            return stripped
        return "".join(sentences[-5:]).strip()

    def _family_affinity_topk(self, signal_profile: dict[str, Any]) -> list[dict[str, Any]]:
        scores = {
            "title_selection": (float(signal_profile.get("titleability") or 0.0) + float(signal_profile.get("single_center_strength") or 0.0) + float(signal_profile.get("core_object_anchor_strength") or 0.0)) / 3,
            "continuation": (float(signal_profile.get("continuation_openness") or 0.0) + float(signal_profile.get("direction_uniqueness") or 0.0) + float(signal_profile.get("tail_extension_signal") or 0.0)) / 3,
            "sentence_order": (float(signal_profile.get("opening_signal_strength") or 0.0) + float(signal_profile.get("closing_signal_strength") or 0.0) + float(signal_profile.get("sequence_integrity") or 0.0)) / 3,
            "sentence_fill": (float(signal_profile.get("bidirectional_validation") or 0.0) + float(signal_profile.get("reference_dependency") or 0.0) + float(signal_profile.get("object_match_strength") or 0.0)) / 3,
        }
        ranked = sorted(scores.items(), key=lambda item: item[1], reverse=True)
        return [{"family": family, "score": round(score, 4)} for family, score in ranked[:3]]

    def _build_span(self, *, article_id: str, span_id: str, text: str, paragraph_count: int, sentence_count: int, source_domain: str | None) -> SpanRecord:
        return SpanRecord(span_id=span_id, article_id=article_id, text=text, paragraph_count=paragraph_count, sentence_count=sentence_count, source_domain=source_domain, version=SpanVersionSet(segment_version="v2.article_first", universal_tag_version="v2.heuristic", route_version="v2.card_pipeline", family_tag_version="v2.card_pipeline"))

    def _split_sentences(self, paragraphs: list[str]) -> list[str]:
        sentences: list[str] = []
        for paragraph in paragraphs:
            sentences.extend(self.sentence_splitter.split(paragraph))
        return [sentence for sentence in sentences if sentence.strip()]

    def _starts_with_enumerative_opening(self, text: str) -> bool:
        stripped = text.strip()
        return bool(re.match(r"^(一是|二是|三是|四是|五是|首先|其次|再次|最后|一年来|过去一年|近年来|近年)", stripped))

    def _has_repeated_enumerative_openings(self, paragraphs: list[str]) -> bool:
        return sum(1 for paragraph in paragraphs if self._starts_with_enumerative_opening(paragraph)) >= 2

    def _heading_like_opening(self, text: str) -> bool:
        stripped = text.strip()
        return bool(re.match(r"^([一二三四五六七八九十]+、|（[一二三四五六七八九十0-9]+）|[0-9]+\.)", stripped))

    def _directive_style_opening(self, text: str) -> bool:
        stripped = text.strip()
        if stripped.startswith("《"):
            return True
        return stripped.startswith(("根据", "按照", "《意见》要求", "《条例》明确", "《办法》提出"))

    def _enumeration_density(self, text: str) -> float:
        hits = len(re.findall(r"\d+(?:\.\d+)?|%|％|[①②③④⑤⑥⑦⑧⑨⑩]|一是|二是|三是|四是|五是|首先|其次|再次|最后", text))
        hits += text.count("；") + text.count(";")
        return round(min(1.0, 0.06 * hits), 4)

    def _marker_strength(self, text: str, markers: tuple[str, ...]) -> float:
        count = sum(text.count(marker) for marker in markers)
        return round(min(1.0, 0.18 * count + (0.30 if count > 0 else 0.0)), 4)

    def _article_purpose_frame(self, universal: Any, text: str) -> str:
        if universal.problem_signal_strength >= 0.65 and universal.method_signal_strength >= 0.55:
            return "问题判断"
        if universal.value_judgement_strength >= 0.70:
            return "评论评议"
        if self._marker_strength(text, COUNTERINTUITIVE_MARKERS) >= 0.48:
            return "认知纠偏"
        if self._marker_strength(text, TIMELINE_MARKERS) >= 0.48:
            return "发展梳理"
        if universal.example_to_theme_strength >= 0.65 or universal.branch_focus_strength >= 0.65:
            return "多维统摄"
        return "介绍说明"

    def _discourse_shape(self, universal: Any, text: str) -> str:
        if self._marker_strength(text, TURNING_MARKERS) >= 0.48:
            return "转折归旨"
        if self._marker_strength(text, TIMELINE_MARKERS) >= 0.48:
            return "时间演进"
        if self._marker_strength(text, PARALLEL_MARKERS) >= 0.48:
            return "并列展开"
        if universal.problem_signal_strength >= 0.62:
            return "问题-分析-结论"
        if universal.example_to_theme_strength >= 0.62:
            return "案例升华"
        return "总分"

    def _core_object(self, title: str | None, text: str) -> str:
        chunks = re.findall(r"[\u4e00-\u9fff]{2,8}", f"{title or ''} {text}")
        counts: Counter[str] = Counter()
        for chunk in chunks:
            if chunk in STOPWORDS:
                continue
            counts[chunk] += 2 if title and chunk in title else 1
        return counts.most_common(1)[0][0] if counts else (title or "未命名对象")

    def _global_main_claim(self, sentences: list[str]) -> str:
        if not sentences:
            return ""
        for sentence in reversed(sentences):
            if any(marker in sentence for marker in SUMMARY_MARKERS + TURNING_MARKERS):
                return sentence
        return sentences[-1] if len(sentences) > 1 else sentences[0]

    def _closure_score(self, universal: Any, text: str) -> float:
        terminal = 1.0 if text.rstrip().endswith(("。", "！", "？", "!", "?")) else 0.4
        value = 0.35 * universal.summary_strength + 0.30 * universal.standalone_readability + 0.20 * universal.independence_score + 0.15 * terminal
        return round(min(1.0, max(0.0, value)), 4)

    def _core_object_anchor_strength(self, text: str) -> float:
        chunks = [chunk for chunk in re.findall(r"[\u4e00-\u9fff]{2,8}", text) if chunk not in STOPWORDS]
        if not chunks:
            return 0.0
        top_count = Counter(chunks).most_common(1)[0][1]
        return round(min(1.0, 0.35 + top_count / max(4, len(chunks))), 4)

    def _object_scope_stability(self, text: str) -> float:
        chunks = [chunk for chunk in re.findall(r"[\u4e00-\u9fff]{2,8}", text) if chunk not in STOPWORDS]
        counts = Counter(chunks).most_common(3)
        if len(counts) <= 1:
            return 0.85
        spread = counts[0][1] - counts[-1][1]
        return round(min(1.0, 0.55 + min(0.30, spread / max(3, len(chunks) or 1))), 4)

    def _theme_words(self, text: str, title: str | None) -> list[str]:
        chunks = [chunk for chunk in re.findall(r"[\u4e00-\u9fff]{2,8}", f"{title or ''} {text}") if chunk not in STOPWORDS]
        return [item[0] for item in Counter(chunks).most_common(5)]

    def _logic_relations(self, text: str, universal: Any) -> list[str]:
        relations: list[str] = []
        if self._turning_focus_strength(text, universal) >= 0.50:
            relations.append("转折")
        if self._cause_effect_strength(text) >= 0.50:
            relations.append("因果")
        if self._necessary_condition_strength(text) >= 0.50:
            relations.append("必要条件")
        if self._parallel_enumeration_strength(text) >= 0.50:
            relations.append("并列")
        return relations

    def _material_structure_label(self, text: str, universal: Any) -> str:
        if self._benefit_result_strength(text, universal) >= 0.66:
            return "行为-结果归旨"
        if self._cause_effect_strength(text) >= 0.52:
            return "现象-分析"
        if self._turning_focus_strength(text, universal) >= 0.52:
            return "背景-核心结论"
        if self._parallel_enumeration_strength(text) >= 0.52:
            return "并列展开"
        if universal.value_judgement_strength >= 0.62:
            return "观点-论证"
        return self._discourse_shape(universal, text)

    def _semantic_completeness_score(self, universal: Any, text: str) -> float:
        terminal = 1.0 if text.rstrip().endswith(("。", "！", "？", "!", "?")) else 0.35
        value = 0.40 * universal.independence_score + 0.35 * universal.standalone_readability + 0.25 * terminal
        return round(min(1.0, max(0.0, value)), 4)

    def _cause_effect_strength(self, text: str) -> float:
        value = 0.50 * self._marker_strength(text, CAUSE_MARKERS) + 0.50 * self._marker_strength(text, CONCLUSION_MARKERS)
        return round(min(1.0, value), 4)

    def _necessary_condition_strength(self, text: str) -> float:
        return self._marker_strength(text, NECESSARY_CONDITION_MARKERS)

    def _countermeasure_signal_strength(self, text: str) -> float:
        return self._marker_strength(text, COUNTERMEASURE_MARKERS)

    def _parallel_enumeration_strength(self, text: str) -> float:
        punctuation_bonus = 0.10 if "；" in text or ";" in text else 0.0
        value = self._marker_strength(text, PARALLEL_MARKERS) + punctuation_bonus
        return round(min(1.0, value), 4)

    def _non_key_detail_density(self, text: str, universal: Any) -> float:
        example_markers = self._marker_strength(text, ("比如", "例如", "例如说", "数据", "案例", "一组"))
        value = 0.55 * example_markers + 0.45 * max(0.0, 1 - universal.summary_strength)
        return round(min(1.0, value), 4)

    def _conclusion_position(self, text: str) -> str:
        sentences = [sentence.strip() for sentence in self.sentence_splitter.split(text) if sentence.strip()]
        if not sentences:
            return "unknown"
        head = sentences[0]
        tail = sentences[-1]
        middle = "".join(sentences[1:-1]) if len(sentences) > 2 else ""
        if any(marker in tail for marker in SUMMARY_MARKERS + CONCLUSION_MARKERS):
            return "tail_or_late"
        if any(marker in head for marker in SUMMARY_MARKERS + CONCLUSION_MARKERS):
            return "opening"
        if middle and any(marker in middle for marker in SUMMARY_MARKERS + CONCLUSION_MARKERS):
            return "middle"
        return "distributed"

    def _key_sentence_position(self, text: str, universal: Any) -> str:
        if universal.summary_strength >= 0.60:
            return self._conclusion_position(text)
        if self._turning_focus_strength(text, universal) >= 0.55:
            return "tail_or_late"
        return "distributed"

    def _turning_focus_strength(self, text: str, universal: Any) -> float:
        return round(min(1.0, 0.55 * self._marker_strength(text, TURNING_MARKERS) + 0.45 * universal.transition_strength), 4)

    def _multi_dimension_cohesion(self, text: str, universal: Any) -> float:
        return round(min(1.0, 0.50 * self._marker_strength(text, PARALLEL_MARKERS) + 0.50 * universal.branch_focus_strength), 4)

    def _analysis_to_conclusion_strength(self, text: str, universal: Any) -> float:
        benefit_support = self._benefit_result_count(text)
        value = (
            0.28 * universal.problem_signal_strength
            + 0.28 * universal.summary_strength
            + 0.24 * self._marker_strength(text, SUMMARY_MARKERS)
            + 0.20 * benefit_support
        )
        return round(min(1.0, value), 4)

    def _benefit_result_count(self, text: str) -> float:
        sentences = [sentence.strip() for sentence in self.sentence_splitter.split(text) if sentence.strip()]
        if not sentences:
            return 0.0
        result_units = 0
        unique_markers: set[str] = set()
        for sentence in sentences:
            marker_hits = [marker for marker in BENEFIT_RESULT_MARKERS if marker in sentence]
            noun_hits = [marker for marker in BENEFIT_RESULT_NOUNS if marker in sentence]
            if marker_hits or (noun_hits and any(token in sentence for token in ("更加", "更", "有效", "进一步", "从而", "进而"))):
                result_units += 1
                unique_markers.update(marker_hits[:2] or noun_hits[:2])
        score = 0.55 * min(1.0, result_units / 3) + 0.45 * min(1.0, len(unique_markers) / 3)
        return round(min(1.0, score), 4)

    def _benefit_result_strength(self, text: str, universal: Any) -> float:
        sentences = [sentence.strip() for sentence in self.sentence_splitter.split(text) if sentence.strip()]
        if not sentences:
            return 0.0
        lead_window = "".join(sentences[: max(1, min(2, len(sentences)))])
        tail_window = "".join(sentences[-2:]) if len(sentences) > 1 else sentences[0]
        action_anchor_strength = min(
            1.0,
            0.60 * self._marker_strength(lead_window, ACTION_MEASURE_MARKERS)
            + 0.40 * self._marker_strength(lead_window, ("通过", "实施", "完善", "优化", "强化", "服务", "机制", "举措")),
        )
        result_count_strength = self._benefit_result_count(text)
        result_chain_strength = self._marker_strength(text, BENEFIT_RESULT_MARKERS)
        closure_support = 0.55 * universal.summary_strength + 0.45 * self._marker_strength(tail_window, SUMMARY_MARKERS + CONCLUSION_MARKERS)
        value_support = 0.55 * universal.value_judgement_strength + 0.45 * self._marker_strength(tail_window, ("意义", "作用", "价值", "关键", "重要"))
        score = (
            0.24 * action_anchor_strength
            + 0.26 * result_count_strength
            + 0.20 * result_chain_strength
            + 0.16 * closure_support
            + 0.14 * value_support
        )
        score -= 0.12 * max(0.0, self._turning_focus_strength(text, universal) - 0.62)
        score -= 0.10 * max(0.0, universal.problem_signal_strength - 0.66)
        score -= 0.08 * max(0.0, self._parallel_enumeration_strength(text) - 0.72)
        return round(max(0.0, min(1.0, score)), 4)

    def _title_namingness(self, text: str, universal: Any) -> float:
        penalty = 0.12 if len(text) > 280 else 0.0
        comma_density = min(1.0, text.count("，") / 6)
        value = 0.45 * universal.titleability + 0.35 * self._core_object_anchor_strength(text) + 0.20 * (1 - comma_density)
        return round(max(0.0, min(1.0, value - penalty)), 4)

    def _title_rhetoric_form(self, text: str, universal: Any) -> str:
        if "？" in text or "?" in text or any(token in text for token in QUESTION_MARKERS):
            return "question"
        if self._marker_strength(text, TURNING_MARKERS) >= 0.48:
            return "contrast"
        if universal.value_judgement_strength >= 0.68 or self._marker_strength(text, COUNTERINTUITIVE_MARKERS) >= 0.48:
            return "judgement"
        if universal.summary_strength >= 0.68 and universal.value_judgement_strength >= 0.58:
            return "abstract"
        if self._core_object_anchor_strength(text) >= 0.72 and universal.titleability >= 0.68:
            return "direct_label"
        return "thematic"

    def _tail_anchor(self, text: str) -> str:
        sentences = self.sentence_splitter.split(text)
        return sentences[-1] if sentences else text

    def _anchor_focus(self, text: str, universal: Any) -> str:
        tail = self._tail_anchor(text)
        if self._marker_strength(tail, ("方法", "路径", "做法", "工具", "机制设计")) >= 0.45:
            return "method_opening"
        if self._tension_signal_strength(tail) >= 0.45:
            return "tension_retained"
        if self._case_macro_shift_strength(tail) >= 0.45:
            return "macro_shift"
        if universal.branch_focus_strength >= 0.68:
            return "branch_focus"
        if universal.problem_signal_strength >= 0.65 and universal.method_signal_strength >= 0.50:
            return "problem_exposed"
        if self._marker_strength(tail, MECHANISM_MARKERS) >= 0.40:
            return "mechanism_named"
        if self._theme_raise_strength(tail) >= 0.48:
            return "theme_raised"
        if universal.value_judgement_strength >= 0.68:
            return "judgement_given"
        if any(marker in tail for marker in SUMMARY_MARKERS):
            return "new_pivot"
        return "tail_anchor"

    def _continuation_type(self, text: str, universal: Any) -> str:
        if universal.problem_signal_strength >= 0.72 and universal.method_signal_strength >= 0.50:
            return "countermeasure"
        if self._marker_strength(text, MECHANISM_MARKERS) >= 0.60:
            return "deepen_mechanism"
        if self._theme_raise_strength(text) >= 0.60:
            return "subtopic_expand"
        if universal.value_judgement_strength >= 0.68:
            return "reason_argument"
        if self._case_macro_shift_strength(text) >= 0.60:
            return "macro_unfold"
        if universal.branch_focus_strength >= 0.68:
            return "focus_branch"
        if self._tension_signal_strength(text) >= 0.60:
            return "resolve_tension"
        if universal.method_signal_strength >= 0.68:
            return "method_expand"
        if self._tail_extension_signal(text) >= 0.70:
            return "explain"
        return "deepen_pivot"

    def _progression_mode(self, text: str, universal: Any) -> str:
        mapping = {
            "explain": "one_level_down",
            "countermeasure": "problem_to_solution",
            "deepen_mechanism": "object_to_mechanism",
            "subtopic_expand": "theme_to_subtopic",
            "deepen_pivot": "summary_to_new_pivot",
            "reason_argument": "judgement_to_reason",
            "macro_unfold": "case_to_macro",
            "focus_branch": "multi_branch_to_focus",
            "resolve_tension": "tension_to_explanation",
            "method_expand": "analysis_to_method",
        }
        return mapping.get(self._continuation_type(text, universal), "one_level_down")

    def _ending_function(self, text: str, universal: Any) -> str:
        tail = self._tail_anchor(text)
        if self._tension_signal_strength(tail) >= 0.48:
            return "tension_hold"
        if any(token in tail for token in SUMMARY_MARKERS) and self._tail_extension_signal(text) >= 0.45:
            return "summary_plus_open"
        if universal.value_judgement_strength >= 0.68:
            return "judgement_trigger"
        if self._marker_strength(tail, TURNING_MARKERS) >= 0.40 or self._marker_strength(tail, ("接下来", "进一步", "随后")) >= 0.40:
            return "transition_trigger"
        return "open_only"

    def _tail_extension_signal(self, text: str) -> float:
        return self._marker_strength(self._tail_anchor(text), ("这也意味着", "这要求", "这提醒我们", "关键在于", "还需要", "进一步", "接下来", "未来"))

    def _anchor_clarity(self, text: str) -> float:
        tail = self._tail_anchor(text)
        value = 0.35 + 0.35 * (1.0 if tail.endswith(("。", "！", "？", "!", "?")) else 0.0) + 0.30 * self._core_object_anchor_strength(tail)
        return round(min(1.0, value), 4)

    def _theme_raise_strength(self, text: str) -> float:
        return self._marker_strength(text, ("更重要的是", "归根到底", "进一步看", "背后是", "这意味着", "放到更大背景下看"))

    def _case_macro_shift_strength(self, text: str) -> float:
        value = 0.45 * self._marker_strength(text, ("比如", "例如", "案例", "有人", "一位", "某个")) + 0.55 * self._marker_strength(text, ("社会", "行业", "整体", "结构性", "更深层", "公共", "群体"))
        return round(min(1.0, value), 4)

    def _tension_signal_strength(self, text: str) -> float:
        return self._marker_strength(text, ("矛盾", "张力", "一方面", "另一方面", "看似", "却", "冲突", "两难"))

    def _opening_anchor_type(self, text: str) -> str:
        sentences = self.sentence_splitter.split(text)
        head = sentences[0] if sentences else text
        if head.startswith(CONTEXTUAL_OPENINGS):
            return "upper_context_link"
        if any(marker in head for marker in VALUE_MARKERS):
            return "viewpoint_opening"
        if any(token in head for token in ("问题", "困境", "为何", "谁在", "怎么办")):
            return "problem_opening"
        if self._core_object_anchor_strength(head) >= 0.60:
            return "explicit_topic"
        if len(head.strip()) <= 14:
            return "weak_opening"
        return "none"

    def _opening_signal_strength(self, text: str) -> float:
        value_by_type = {"explicit_topic": 0.82, "viewpoint_opening": 0.76, "problem_opening": 0.72, "upper_context_link": 0.60, "weak_opening": 0.42, "none": 0.28}
        return value_by_type.get(self._opening_anchor_type(text), 0.28)

    def _non_opening_penalty(self, text: str) -> float:
        sentences = self.sentence_splitter.split(text)
        head = sentences[0] if sentences else text
        penalty = 0.0
        if head.startswith(CONTEXTUAL_OPENINGS):
            penalty += 0.45
        if self._marker_strength(head, TURNING_MARKERS) >= 0.30:
            penalty += 0.25
        if self._reference_dependency(head) >= 0.60:
            penalty += 0.20
        return round(min(1.0, penalty), 4)

    def _middle_structure_type(self, text: str, universal: Any) -> str:
        if self._marker_strength(text, PARALLEL_MARKERS) >= 0.48:
            return "parallel_expansion"
        if universal.problem_signal_strength >= 0.62 and universal.method_signal_strength >= 0.48:
            return "problem_solution_blocks"
        if self._marker_strength(text, ("因为", "所以", "因此", "导致", "由此")) >= 0.42:
            return "cause_effect_chain"
        if self._local_binding_strength(text) >= 0.58:
            return "local_binding"
        return "mixed_layers"

    def _local_binding_strength(self, text: str) -> float:
        sentences = self.sentence_splitter.split(text)
        if len(sentences) <= 1:
            return 0.30
        linked = sum(1 for sentence in sentences[1:] if sentence.startswith(CONTEXTUAL_OPENINGS) or any(sentence.startswith(token) for token in ("因此", "同时", "此外", "不过", "而且", "但")))
        return round(min(1.0, 0.35 + linked / max(1, len(sentences) - 1) * 0.55), 4)

    def _connector_signal_strength(self, text: str) -> float:
        return self._marker_strength(text, ("因此", "所以", "同时", "此外", "不过", "而且", "但是", "由此", "总之"))

    def _closing_anchor_type(self, text: str) -> str:
        tail = self._tail_anchor(text)
        if self._is_call_to_action_tail(tail):
            return "call_to_action"
        if any(token in tail for token in SUMMARY_MARKERS):
            return "summary"
        if any(token in tail for token in ("可见", "由此", "因此", "这说明")):
            return "conclusion"
        if any(token in tail for token in ("例如", "比如", "就像", "这一案例")):
            return "case_support"
        return "none"

    def _closing_signal_strength(self, text: str) -> float:
        value_by_type = {"conclusion": 0.84, "summary": 0.80, "call_to_action": 0.76, "case_support": 0.58, "none": 0.24}
        return value_by_type.get(self._closing_anchor_type(text), 0.24)

    def _is_call_to_action_tail(self, tail: str) -> bool:
        if any(token in tail for token in ("\u5e94\u8be5", "\u9700\u8981", "\u5fc5\u987b", "\u4e0d\u59a8", "\u5e94\u5f53")):
            return True
        return bool(re.search(r"(?:^|[，。；,;])\u8981(?=[\u4e00-\u9fff]{1,8}(?:\u52a0\u5f3a|\u5b8c\u5584|\u505a\u597d|\u907f\u514d|\u63a8\u8fdb|\u575a\u6301|\u91c7\u53d6|\u52a0\u5feb))", tail))

    def _block_order_complexity(self, text: str, universal: Any, candidate_type: str) -> float:
        value = 0.30 + (0.14 if candidate_type == "sentence_block_group" else 0.0) + (0.20 if candidate_type == "phrase_or_clause_group" else 0.0)
        value += 0.18 * self._marker_strength(text, PARALLEL_MARKERS) + 0.18 * universal.branch_focus_strength + 0.12 * universal.problem_signal_strength
        return round(min(1.0, value), 4)

    def _sequence_integrity(self, text: str) -> float:
        value = 0.28 * self._opening_signal_strength(text) + 0.24 * self._closing_signal_strength(text) + 0.24 * self._local_binding_strength(text) + 0.24 * (1 - self._non_opening_penalty(text))
        return round(min(1.0, max(0.0, value)), 4)

    def _phrase_order_salience(self, text: str, candidate_type: str) -> float:
        if candidate_type != "phrase_or_clause_group":
            return 0.10
        clauses = [part.strip() for part in re.split(r"[，、；,;]", text) if part.strip()]
        return round(min(1.0, 0.45 + 0.08 * len(clauses)), 4)

    def _slot_role(self, candidate: dict[str, Any], *, article_context: dict[str, Any] | None = None) -> str:
        meta = dict(candidate.get("meta") or {})
        slot_role = str(meta.get("slot_role") or "").strip()
        if slot_role:
            return slot_role
        if candidate.get("candidate_type") != "functional_slot_unit":
            return ""
        hydrated = self._hydrate_functional_slot_meta(article_context=article_context, candidate=candidate)
        return str(hydrated.get("slot_role") or "")

    def _slot_function(self, candidate: dict[str, Any]) -> str:
        meta = dict(candidate.get("meta") or {})
        slot_function = str(meta.get("slot_function") or "").strip()
        if slot_function:
            return slot_function
        if candidate.get("candidate_type") != "functional_slot_unit":
            return ""
        hydrated = self._hydrate_functional_slot_meta(article_context=None, candidate=candidate)
        return str(hydrated.get("slot_function") or "")

    def _slot_explicit_ready(self, candidate: dict[str, Any], *, article_context: dict[str, Any] | None = None) -> bool:
        slot_role = self._slot_role(candidate, article_context=article_context)
        slot_function = self._slot_function(candidate)
        return bool(candidate.get("candidate_type") == "functional_slot_unit" and slot_role and slot_function)

    def _blank_position(self, candidate: dict[str, Any]) -> str:
        candidate_type = candidate["candidate_type"]
        meta = candidate.get("meta", {})
        slot_role = str(meta.get("slot_role") or "").strip()
        if slot_role:
            return slot_role
        if candidate_type == "insertion_context_unit":
            return "inserted"
        paragraph_range = meta.get("source_paragraph_range_original") or meta.get("paragraph_range") or []
        if paragraph_range and paragraph_range[0] == 0:
            return "opening"
        if paragraph_range and paragraph_range[-1] >= 2:
            return "ending"
        return "middle"

    def _fill_function_type(self, candidate: dict[str, Any], text: str, universal: Any) -> str:
        explicit_function = self._explicit_fill_function_type(candidate)
        if explicit_function:
            return explicit_function
        blank_position = self._blank_position(candidate)
        if candidate["candidate_type"] == "insertion_context_unit":
            return "inserted_reference"
        if blank_position == "opening" and universal.summary_strength >= 0.58:
            return "opening_summary"
        if blank_position == "ending" and self._elevation_space_strength(text, universal) >= 0.62:
            return "ending_elevation"
        if blank_position == "ending":
            return "ending_summary"
        if universal.explanation_strength >= 0.66:
            return "middle_explanation"
        if self._focus_shift_strength(text, universal) >= 0.62:
            return "middle_focus_shift"
        if self._multi_constraint_density(text) >= 0.70:
            return "comprehensive_match"
        return "bridge"

    def _fill_logic_relation(self, candidate: dict[str, Any], text: str, universal: Any) -> str:
        explicit_logic = self._explicit_fill_logic_relation(candidate)
        if explicit_logic:
            return explicit_logic
        function_type = self._fill_function_type(candidate, text, universal)
        if function_type in {"opening_summary", "ending_summary"}:
            return "summary"
        if function_type == "middle_explanation":
            return "explanation"
        if function_type == "middle_focus_shift":
            return "focus_shift"
        if function_type == "ending_elevation":
            return "elevation"
        if function_type == "inserted_reference":
            return "reference_match"
        if function_type == "comprehensive_match":
            return "multi_constraint"
        if self._connector_signal_strength(text) >= 0.45 or self._turning_focus_strength(text, universal) >= 0.45:
            return "continuation_or_transition"
        return "continuation"

    def _explicit_fill_function_type(self, candidate: dict[str, Any]) -> str:
        slot_role = str((candidate.get("meta") or {}).get("slot_role") or "").strip()
        slot_function = str((candidate.get("meta") or {}).get("slot_function") or "").strip()
        mapping = {
            ("opening", "summary"): "opening_summary",
            ("opening", "topic_intro"): "opening_summary",
            ("middle", "carry_previous"): "middle_explanation",
            ("middle", "lead_next"): "bridge",
            ("middle", "bridge_both_sides"): "bridge",
            ("ending", "ending_summary"): "ending_summary",
            ("ending", "countermeasure"): "ending_summary",
        }
        return mapping.get((slot_role, slot_function), "")

    def _explicit_fill_logic_relation(self, candidate: dict[str, Any]) -> str:
        slot_role = str((candidate.get("meta") or {}).get("slot_role") or "").strip()
        slot_function = str((candidate.get("meta") or {}).get("slot_function") or "").strip()
        mapping = {
            ("opening", "summary"): "summary",
            ("opening", "topic_intro"): "transition",
            ("middle", "carry_previous"): "explanation",
            ("middle", "lead_next"): "transition",
            ("middle", "bridge_both_sides"): "continuation",
            ("ending", "ending_summary"): "summary",
            ("ending", "countermeasure"): "summary",
        }
        return mapping.get((slot_role, slot_function), "")

    def _bidirectional_validation(self, text: str) -> float:
        return round((self._backward_link_strength(text) + self._forward_link_strength(text)) / 2, 4)

    def _reference_dependency(self, text: str) -> float:
        count = sum(text.count(marker) for marker in ("这", "其", "该", "这些", "这种", "这一", "上述", "前者", "后者"))
        return round(min(1.0, 0.18 * count + (0.22 if count > 0 else 0.0)), 4)

    def _abstraction_level(self, text: str, universal: Any) -> float:
        value = 0.40 * universal.summary_strength + 0.30 * universal.value_judgement_strength + 0.30 * self._marker_strength(text, SUMMARY_MARKERS)
        return round(min(1.0, value), 4)

    def _backward_link_strength(self, text: str) -> float:
        sentences = self.sentence_splitter.split(text)
        head = sentences[0] if sentences else text
        if head.startswith(CONTEXTUAL_OPENINGS):
            return 0.78
        if self._reference_dependency(head) >= 0.45:
            return 0.62
        return 0.42

    def _forward_link_strength(self, text: str) -> float:
        tail_signal = self._tail_extension_signal(text)
        connector_bonus = 0.15 if any(token in self._tail_anchor(text) for token in ("接下来", "进一步", "还需要", "未来")) else 0.0
        return round(min(1.0, 0.35 + 0.40 * tail_signal + connector_bonus), 4)

    def _summary_need_strength(self, candidate: dict[str, Any], universal: Any) -> float:
        blank_position = self._blank_position(candidate)
        if blank_position == "opening":
            return round(min(1.0, 0.45 + 0.45 * universal.summary_strength), 4)
        if blank_position == "ending":
            return round(min(1.0, 0.50 + 0.35 * universal.summary_strength), 4)
        return round(min(1.0, 0.20 + 0.25 * universal.summary_strength), 4)

    def _focus_shift_strength(self, text: str, universal: Any) -> float:
        return round(min(1.0, 0.60 * self._turning_focus_strength(text, universal) + 0.40 * universal.transition_strength), 4)

    def _elevation_space_strength(self, text: str, universal: Any) -> float:
        value = 0.40 * universal.value_judgement_strength + 0.30 * universal.summary_strength + 0.30 * self._marker_strength(text, SUMMARY_MARKERS)
        return round(min(1.0, value), 4)

    def _insertion_fit_strength(self, candidate: dict[str, Any], text: str) -> float:
        if candidate["candidate_type"] != "insertion_context_unit":
            return 0.18
        value = 0.40 * self._reference_dependency(text) + 0.30 * self._backward_link_strength(text) + 0.30 * self._forward_link_strength(text)
        return round(min(1.0, value), 4)

    def _multi_constraint_density(self, text: str) -> float:
        return round(min(1.0, (self._reference_dependency(text) + self._backward_link_strength(text) + self._forward_link_strength(text)) / 3), 4)

    def _build_distractor_profile(self, question_card: dict[str, Any], top_hit: dict[str, Any], signal_profile: dict[str, Any]) -> dict[str, Any]:
        slots = self._resolve_slots(question_card, top_hit["card_id"])
        return {
            "strongest_gap": question_card.get("slot_extensions", {}).get("strongest_distractor_gap", "medium"),
            "hierarchy": question_card.get("slot_extensions", {}).get("distractor_hierarchy_profile", []),
            "distractor_strength": slots.get("distractor_strength"),
            "title_rhetoric_form": signal_profile.get("title_rhetoric_form"),
        }

    def _evaluate_requirement(self, actual: Any, requirement: Any) -> tuple[float, str]:
        if requirement is None:
            return 1.0, "no_requirement"
        if actual is None:
            return 0.0, "missing"
        if isinstance(requirement, bool):
            matched = bool(actual) is requirement
            return (1.0 if matched else 0.0), f"bool={matched}"
        if isinstance(requirement, (int, float)):
            if isinstance(actual, (int, float)):
                delta = abs(float(actual) - float(requirement))
                return round(max(0.0, 1 - delta), 4), f"target={requirement}"
            matched = actual == requirement
            return (1.0 if matched else 0.0), f"eq={matched}"
        if isinstance(requirement, str):
            req = requirement.strip()
            if req.startswith(">="):
                threshold = float(req[2:])
                actual_value = float(actual)
                return (1.0, req) if actual_value >= threshold else (round(max(0.0, actual_value / threshold), 4), req)
            if req.startswith("<="):
                threshold = float(req[2:])
                actual_value = float(actual)
                return (1.0, req) if actual_value <= threshold else (round(max(0.0, threshold / actual_value), 4), req)
            if req.startswith(">"):
                threshold = float(req[1:])
                actual_value = float(actual)
                return (1.0, req) if actual_value > threshold else (round(max(0.0, actual_value / threshold), 4), req)
            if req.startswith("<"):
                threshold = float(req[1:])
                actual_value = float(actual)
                return (1.0, req) if actual_value < threshold else (round(max(0.0, threshold / actual_value), 4), req)
            if req == "continuation_or_transition":
                matched = actual in {"continuation", "transition", "continuation_or_transition"}
                return (1.0 if matched else 0.0), f"enum={matched}"
            matched = str(actual) == req
            return (1.0 if matched else 0.0), f"enum={matched}"
        if isinstance(requirement, (list, tuple, set)):
            matched = actual in requirement
            return (1.0 if matched else 0.0), f"in={matched}"
        matched = actual == requirement
        return (1.0 if matched else 0.0), f"eq={matched}"

    def _select_diverse_items(self, items: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
        ranked = sorted(items, key=lambda item: item["quality_score"], reverse=True)
        if not ranked or limit <= 0:
            return []

        selected: list[dict[str, Any]] = []
        selected_ids: set[str] = set()
        selected_fingerprints: list[str] = []
        article_counts: Counter[str] = Counter()
        source_counts: Counter[str] = Counter()
        card_counts: Counter[str] = Counter()
        phases = [
            {"max_per_article": 1, "max_per_source": max(1, limit // 3), "max_per_card": max(1, limit // 4)},
            {"max_per_article": 2, "max_per_source": max(2, limit // 2), "max_per_card": max(2, limit // 3)},
            {"max_per_article": 3, "max_per_source": limit, "max_per_card": limit},
        ]

        for phase in phases:
            for item in ranked:
                if len(selected) >= limit:
                    break
                candidate_id = str(item.get("candidate_id") or "")
                if not candidate_id or candidate_id in selected_ids:
                    continue
                if self._is_near_duplicate_item(item, selected_fingerprints):
                    continue
                article_id = str(item.get("article_id") or "")
                source_key = self._source_key(item)
                card_id = str((item.get("question_ready_context") or {}).get("selected_material_card") or "")
                if article_id and article_counts[article_id] >= phase["max_per_article"]:
                    continue
                if source_key and source_counts[source_key] >= phase["max_per_source"]:
                    continue
                if card_id and card_counts[card_id] >= phase["max_per_card"]:
                    continue
                selected.append(item)
                selected_ids.add(candidate_id)
                selected_fingerprints.append(self._fingerprint_text(str(item.get("text") or "")))
                if article_id:
                    article_counts[article_id] += 1
                if source_key:
                    source_counts[source_key] += 1
                if card_id:
                    card_counts[card_id] += 1
            if len(selected) >= limit:
                return selected[:limit]

        for item in ranked:
            if len(selected) >= limit:
                break
            candidate_id = str(item.get("candidate_id") or "")
            if not candidate_id or candidate_id in selected_ids:
                continue
            if self._is_near_duplicate_item(item, selected_fingerprints):
                continue
            selected.append(item)
            selected_ids.add(candidate_id)
            selected_fingerprints.append(self._fingerprint_text(str(item.get("text") or "")))
        return selected[:limit]

    def _source_key(self, item: dict[str, Any]) -> str:
        source = item.get("source") or {}
        return str(source.get("source_name") or source.get("domain") or "")

    def _fingerprint_text(self, text: str) -> str:
        normalized = re.sub(r"[^0-9A-Za-z\u4e00-\u9fff]+", "", text or "")
        return normalized[:500]

    def _is_near_duplicate_item(self, item: dict[str, Any], selected_fingerprints: list[str]) -> bool:
        current = self._fingerprint_text(str(item.get("text") or ""))
        if not current:
            return True
        for existing in selected_fingerprints:
            if current == existing:
                return True
            if len(current) >= 80 and len(existing) >= 80 and SequenceMatcher(None, current, existing).ratio() >= 0.92:
                return True
        return False
