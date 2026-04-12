from __future__ import annotations

from collections import Counter
import hashlib
import re
from types import SimpleNamespace
from urllib.parse import parse_qs, quote_plus, unquote, urlparse

from sqlalchemy import desc, select

from app.core.config import get_config_bundle
from app.core.enums import MaterialStatus, ReleaseChannel, ReviewStatus
from app.domain.services._common import ServiceBase
from app.infra.db.orm.audit import AuditEventORM
from app.infra.db.orm.material_span import MaterialSpanORM
from app.infra.db.orm.review import TaggingReviewORM
from app.infra.crawl.extractors.readability_extractor import ReadabilityLikeExtractor
from app.infra.crawl.fetchers.http_fetcher import HttpCrawlerFetcher
from app.infra.ingest.cleaners.basic_cleaner import BasicCleaner
from app.domain.services.material_v2_index_service import MaterialV2IndexService
from app.services.sentence_fill_protocol import normalize_sentence_fill_function_type
from app.services.material_pipeline_v2 import MaterialPipelineV2


class MaterialPipelineV2Service(ServiceBase):
    SERVABLE_REVIEW_STATUSES = (
        ReviewStatus.AUTO_TAGGED.value,
        ReviewStatus.REVIEW_CONFIRMED.value,
    )
    RELAXED_REVIEW_STATUSES = (
        ReviewStatus.AUTO_TAGGED.value,
        ReviewStatus.REVIEW_CONFIRMED.value,
        ReviewStatus.REVIEW_PENDING.value,
    )
    REJECTED_REVIEW_STATUS = ReviewStatus.REVIEW_REJECTED.value
    EXTERNAL_SEARCH_RESULT_LIMIT = 12
    EXTERNAL_FETCH_LIMIT = 6
    EXTERNAL_ACCEPT_LIMIT = 3

    def __init__(self, session) -> None:
        super().__init__(session)
        self.pipeline = MaterialPipelineV2()
        self.fetcher = HttpCrawlerFetcher(timeout=20.0)
        self.extractor = ReadabilityLikeExtractor()
        self.cleaner = BasicCleaner()

    def search(self, payload: dict) -> dict:
        cached_result = self._search_cached(payload)
        if cached_result is not None:
            return self._apply_external_fallback_if_needed(payload=payload, base_result=cached_result)
        requested_ids = payload.get("article_ids") or []
        article_limit = payload.get("article_limit", 10)
        business_family_id = payload["business_family_id"]
        query_terms = self._lookup_query_terms(payload)
        articles = []
        if requested_ids:
            for article_id in requested_ids:
                article = self.article_repo.get(article_id)
                if article is not None:
                    articles.append(article)
        else:
            if query_terms:
                lookup_limit = max(article_limit * 4, article_limit)
                articles = self.article_repo.search_by_terms(query_terms, limit=lookup_limit)
            if not articles:
                if business_family_id == "sentence_order":
                    article_limit = max(article_limit, 80)
                articles = self.article_repo.list(limit=article_limit)
            else:
                articles = articles[:article_limit]
        result = self.pipeline.search(
            articles=articles,
            business_family_id=business_family_id,
            question_card_id=payload.get("question_card_id"),
            business_card_ids=payload.get("business_card_ids") or [],
            preferred_business_card_ids=payload.get("preferred_business_card_ids") or [],
            query_terms=payload.get("query_terms") or [],
            topic=payload.get("topic"),
            text_direction=payload.get("text_direction"),
            document_genre=payload.get("document_genre"),
            material_structure_label=payload.get("material_structure_label"),
            structure_constraints=payload.get("structure_constraints") or {},
            candidate_limit=payload.get("candidate_limit", 20),
            min_card_score=payload.get("min_card_score", 0.55),
            min_business_card_score=payload.get("min_business_card_score", 0.45),
            target_length=payload.get("target_length"),
            length_tolerance=payload.get("length_tolerance", 120),
            enable_anchor_adaptation=payload.get("enable_anchor_adaptation", True),
            preserve_anchor=payload.get("preserve_anchor", True),
        )
        result["article_count"] = len(articles)
        result["article_ids"] = [article.id for article in articles]
        result = self._annotate_article_fallback_result(
            base_result=result,
            business_family_id=business_family_id,
        )
        return self._apply_external_fallback_if_needed(payload=payload, base_result=result)

    def build_formal_material_candidates(
        self,
        article_id: str,
        *,
        candidate_types: list[str] | set[str] | None = None,
    ) -> dict:
        article = self.article_repo.get(article_id)
        if article is None:
            return {
                "article_id": article_id,
                "generation_mode": "v2_primary",
                "candidate_spans": [],
                "fallback_reason": "article_not_found",
            }
        result = self.pipeline.build_formal_material_candidates(
            article=article,
            candidate_types=candidate_types,
        )
        result["article_id"] = article_id
        return result

    def precompute(self, payload: dict) -> dict:
        return MaterialV2IndexService(self.session).precompute(payload)

    def _search_cached(self, payload: dict) -> dict | None:
        business_family_id = payload["business_family_id"]
        question_card = self._resolve_search_question_card(
            business_family_id=business_family_id,
            question_card_id=payload.get("question_card_id"),
        )
        candidate_limit = payload.get("candidate_limit", 20)
        cache_lookup_limit = max(candidate_limit * 8, 80)
        if business_family_id == "sentence_fill":
            cache_lookup_limit = max(candidate_limit * 24, 480)
        elif business_family_id == "sentence_order":
            cache_lookup_limit = max(candidate_limit * 28, 560)
        requested_business_card_ids = set(payload.get("business_card_ids") or [])
        preferred_business_card_ids = set(payload.get("preferred_business_card_ids") or [])
        structure_constraints = dict(payload.get("structure_constraints") or {})
        enforce_structure_gate = bool(requested_business_card_ids)
        status = payload.get("status")
        release_channel = payload.get("release_channel")
        if status is None and release_channel is None:
            status = MaterialStatus.PROMOTED.value
            release_channel = ReleaseChannel.STABLE.value
        materials = self.material_repo.list_v2_cached(
            business_family_id=business_family_id,
            material_ids=payload.get("material_ids") or None,
            article_ids=payload.get("article_ids") or None,
            status=status,
            release_channel=release_channel,
            limit=cache_lookup_limit,
        )
        review_status_map = self._load_review_status_map([material.id for material in materials])
        review_gate_mode = str(payload.get("review_gate_mode") or "stable_relaxed").strip().lower()
        materials, review_gate = self._apply_review_gate(
            materials=materials,
            review_status_map=review_status_map,
            mode=review_gate_mode,
        )
        if not materials:
            return None
        items = []
        article_ids: list[str] = []
        query_terms = [term for term in (payload.get("query_terms") or []) if term]
        prefiltered: list[tuple[object, dict, tuple[int, float, int, float]]] = []
        prefilter_limit = max(candidate_limit * 10, 120)
        if business_family_id == "sentence_fill":
            prefilter_limit = max(candidate_limit * 6, 72)
        elif business_family_id == "sentence_order":
            prefilter_limit = max(candidate_limit * 8, 96)
        tier_candidates: list[tuple[object, dict, tuple[int, float, int, float]]] = []
        relaxed_card_candidates: list[tuple[object, dict, tuple[int, float, int, float]]] = []
        for material in materials:
            cached_payload = dict(material.v2_index_payload or {})
            cached_item = cached_payload.get(business_family_id)
            if not cached_item:
                continue
            if self._cached_item_requires_rebuild(
                cached_item=cached_item,
                business_family_id=business_family_id,
                question_card=question_card,
            ):
                rebuilt_item = self._rebuild_cached_item(
                    material=material,
                    business_family_id=business_family_id,
                    question_card=question_card,
                )
                if rebuilt_item is None:
                    continue
                cached_item = rebuilt_item
            if not self._cached_item_matches_front_filters(cached_item=cached_item, payload=payload):
                continue
            selected_business_card = str(((cached_item.get("question_ready_context") or {}).get("selected_business_card")) or "")
            haystack = "\n".join(
                [
                    str(cached_item.get("text") or ""),
                    str(cached_item.get("original_text") or ""),
                    str(cached_item.get("article_title") or ""),
                ]
            )
            hit_count = sum(1 for term in query_terms if term in haystack)
            structure_score = self._cached_structure_match_score(
                business_family_id=business_family_id,
                cached_item=cached_item,
                structure_constraints=structure_constraints,
            )
            card_score = 0
            if requested_business_card_ids:
                if selected_business_card in requested_business_card_ids:
                    card_score = 2
                elif requested_business_card_ids.intersection(set(cached_item.get("business_card_recommendations") or [])):
                    card_score = 1
            elif preferred_business_card_ids:
                cached_recommended = set(cached_item.get("business_card_recommendations") or [])
                if selected_business_card:
                    cached_recommended.add(selected_business_card)
                if selected_business_card in preferred_business_card_ids:
                    card_score = 1
                elif preferred_business_card_ids.intersection(cached_recommended):
                    card_score = 0.5
            else:
                card_score = 1
            llm_selection_score = float(
                cached_item.get("llm_selection_score")
                or ((cached_item.get("llm_generation_readiness") or {}).get("score"))
                or 0.0
            )
            quality_score = float(cached_item.get("quality_score") or getattr(material, "quality_score", 0.0) or 0.0)
            sort_key = self.pipeline._cached_prefilter_sort_key(
                cached_item=cached_item,
                business_family_id=business_family_id,
                card_score=card_score,
                structure_score=structure_score,
                hit_count=hit_count,
                quality_score=quality_score,
            )
            entry = (material, cached_item, sort_key)
            relaxed_card_candidates.append(entry)

            tier_candidates.append(entry)

        if not tier_candidates and requested_business_card_ids:
            tier_candidates = relaxed_card_candidates
        if not tier_candidates:
            return None

        strict = [
            entry for entry in tier_candidates
            if (entry[2][3] > 0 or not query_terms)
        ]
        relaxed = [
            entry for entry in tier_candidates
            if True
        ]
        if strict:
            prefiltered = strict
        elif relaxed:
            prefiltered = relaxed
        else:
            prefiltered = tier_candidates

        prefiltered.sort(key=lambda entry: entry[2], reverse=True)
        prefiltered = prefiltered[:prefilter_limit]

        for material, cached_item, _ in prefiltered:
            refreshed = self._refresh_cached_item_for_search(
                material=material,
                cached_item=cached_item,
                payload=payload,
                business_family_id=business_family_id,
                question_card=question_card,
            )
            if refreshed is None:
                continue
            refreshed["usage_count"] = int(getattr(material, "usage_count", 0) or 0)
            refreshed["last_used_at"] = material.last_used_at.isoformat() if getattr(material, "last_used_at", None) else None
            refreshed["review_status"] = review_status_map.get(material.id)
            items.append(refreshed)
            article_ids.append(material.article_id)
        if not items:
            return None
        runtime_binding = question_card.get("runtime_binding", {})
        business_cards = self.pipeline.registry.get_business_cards(
            business_family_id,
            runtime_question_type=runtime_binding.get("question_type"),
            runtime_business_subtype=runtime_binding.get("business_subtype"),
        )
        ranked = self.pipeline._select_diverse_items(items, candidate_limit)
        return {
            "question_card": {
                "card_id": question_card["card_id"],
                "business_family_id": question_card["business_family_id"],
                "business_subtype_id": question_card["business_subtype_id"],
                "runtime_binding": runtime_binding,
            },
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
            "warnings": [],
            "article_count": len({article_id for article_id in article_ids if article_id}),
            "article_ids": list(dict.fromkeys(article_ids)),
            "cache_hit": True,
            "index_version": self.pipeline.INDEX_VERSION,
            "review_gate": review_gate,
        }

    def _resolve_search_question_card(self, *, business_family_id: str, question_card_id: str | None) -> dict:
        return (
            self.pipeline.registry.get_question_card(question_card_id)
            if question_card_id
            else self.pipeline.registry.get_default_question_card(business_family_id)
        )

    @staticmethod
    def _lookup_query_terms(payload: dict) -> list[str]:
        merged: list[str] = []
        seen: set[str] = set()
        for raw in list(payload.get("query_terms") or []) + [payload.get("topic"), payload.get("text_direction")]:
            text = str(raw or "").strip()
            if not text or text in seen:
                continue
            seen.add(text)
            merged.append(text)
        return merged

    @staticmethod
    def _cached_item_matches_front_filters(*, cached_item: dict, payload: dict) -> bool:
        article_profile = dict(cached_item.get("article_profile") or {})
        local_profile = dict(cached_item.get("local_profile") or {})
        text = "\n".join(
            [
                str(cached_item.get("text") or ""),
                str(cached_item.get("original_text") or ""),
                str(cached_item.get("article_title") or ""),
                str(article_profile.get("core_object") or ""),
                str(local_profile.get("core_object") or ""),
            ]
        )
        requested_genre = str(payload.get("document_genre") or "").strip()
        if requested_genre and str(article_profile.get("document_genre") or "").strip() != requested_genre:
            return False
        requested_structure = str(payload.get("material_structure_label") or "").strip()
        candidate_structure = str(local_profile.get("discourse_shape") or article_profile.get("discourse_shape") or "").strip()
        if requested_structure and candidate_structure != requested_structure:
            return False
        requested_topic = str(payload.get("topic") or "").strip()
        if requested_topic and requested_topic not in text:
            return False
        requested_direction = str(payload.get("text_direction") or "").strip()
        if requested_direction and requested_direction not in text:
            return False
        return True

    def _cached_item_requires_rebuild(
        self,
        *,
        cached_item: dict,
        business_family_id: str,
        question_card: dict,
    ) -> bool:
        cached_question_card_id = str(((cached_item.get("question_ready_context") or {}).get("question_card_id")) or "")
        if cached_question_card_id != str(question_card.get("card_id") or ""):
            return True
        if business_family_id in {"title_selection", "sentence_fill", "sentence_order"}:
            scoring = self.pipeline._selected_task_scoring_for_item(
                item=cached_item,
                business_family_id=business_family_id,
            )
            if not scoring:
                return True
        return False

    def _rebuild_cached_item(
        self,
        *,
        material,
        business_family_id: str,
        question_card: dict,
    ) -> dict | None:
        article = self.article_repo.get(material.article_id)
        if article is None:
            return None
        return self.pipeline.build_cached_item_from_material(
            material=material,
            article=article,
            business_family_id=business_family_id,
            question_card_id=str(question_card.get("card_id") or "") or None,
        )

    def _refresh_cached_item_for_search(
        self,
        *,
        material,
        cached_item: dict,
        payload: dict,
        business_family_id: str,
        question_card: dict,
    ) -> dict | None:
        refreshed = self.pipeline.refresh_cached_item(
            cached_item=cached_item,
            query_terms=payload.get("query_terms") or [],
            target_length=payload.get("target_length"),
            length_tolerance=payload.get("length_tolerance", 120),
            enable_anchor_adaptation=payload.get("enable_anchor_adaptation", True),
            preserve_anchor=payload.get("preserve_anchor", True),
        )
        gate_passed, _ = self.pipeline._passes_runtime_material_gate(
            item=refreshed,
            business_family_id=business_family_id,
            question_card=question_card,
            min_card_score=float(payload.get("min_card_score", 0.55) or 0.55),
            min_business_card_score=float(payload.get("min_business_card_score", 0.45) or 0.45),
            require_business_card=False,
        )
        if gate_passed:
            return refreshed
        rebuilt = self._rebuild_cached_item(
            material=material,
            business_family_id=business_family_id,
            question_card=question_card,
        )
        if rebuilt is None:
            return None
        rebuilt = self.pipeline.refresh_cached_item(
            cached_item=rebuilt,
            query_terms=payload.get("query_terms") or [],
            target_length=payload.get("target_length"),
            length_tolerance=payload.get("length_tolerance", 120),
            enable_anchor_adaptation=payload.get("enable_anchor_adaptation", True),
            preserve_anchor=payload.get("preserve_anchor", True),
        )
        gate_passed, _ = self.pipeline._passes_runtime_material_gate(
            item=rebuilt,
            business_family_id=business_family_id,
            question_card=question_card,
            min_card_score=float(payload.get("min_card_score", 0.55) or 0.55),
            min_business_card_score=float(payload.get("min_business_card_score", 0.45) or 0.45),
            require_business_card=False,
        )
        if not gate_passed:
            return None
        return rebuilt

    def _load_review_status_map(self, material_ids: list[str]) -> dict[str, str]:
        if not material_ids:
            return {}
        rows = self.session.execute(
            select(TaggingReviewORM.material_id, TaggingReviewORM.status).where(
                TaggingReviewORM.material_id.in_(material_ids)
            )
        ).all()
        return {material_id: status for material_id, status in rows}

    def _apply_review_gate(
        self,
        *,
        materials: list[object],
        review_status_map: dict[str, str],
        mode: str,
    ) -> tuple[list[object], dict]:
        normalized_mode = mode if mode in {"strict", "stable_relaxed"} else "stable_relaxed"
        observed_status_counts: Counter[str] = Counter()
        included_status_counts: Counter[str] = Counter()
        excluded_status_counts: Counter[str] = Counter()
        included: list[object] = []

        for material in materials:
            review_status = review_status_map.get(material.id)
            status_key = str(review_status or "missing_review")
            observed_status_counts[status_key] += 1

            if normalized_mode == "strict":
                is_allowed = review_status in self.SERVABLE_REVIEW_STATUSES
            else:
                is_allowed = review_status != self.REJECTED_REVIEW_STATUS

            if is_allowed:
                included.append(material)
                included_status_counts[status_key] += 1
            else:
                excluded_status_counts[status_key] += 1

        trace = {
            "mode": normalized_mode,
            "total_candidates": len(materials),
            "included_count": len(included),
            "excluded_count": max(0, len(materials) - len(included)),
            "observed_status_counts": dict(observed_status_counts),
            "included_status_counts": dict(included_status_counts),
            "excluded_status_counts": dict(excluded_status_counts),
        }
        return included, trace

    def observability(self, payload: dict | None = None) -> dict:
        payload = payload or {}
        business_family_id = str(payload.get("business_family_id") or "").strip()
        mode = str(payload.get("review_gate_mode") or "stable_relaxed").strip().lower()
        status = payload.get("status", MaterialStatus.PROMOTED.value)
        release_channel = payload.get("release_channel", ReleaseChannel.STABLE.value)
        limit = int(payload.get("limit") or 10000)
        limit = max(1, min(limit, 50000))

        stmt = select(MaterialSpanORM).where(
            MaterialSpanORM.is_primary.is_(True),
            MaterialSpanORM.v2_index_version.is_not(None),
        )
        if status:
            stmt = stmt.where(MaterialSpanORM.status == status)
        if release_channel:
            stmt = stmt.where(MaterialSpanORM.release_channel == release_channel)
        stmt = stmt.order_by(MaterialSpanORM.updated_at.desc()).limit(limit)
        materials = list(self.session.scalars(stmt))

        if business_family_id:
            materials = [
                material
                for material in materials
                if business_family_id in (material.v2_business_family_ids or [])
                and isinstance(material.v2_index_payload, dict)
                and material.v2_index_payload.get(business_family_id)
            ]

        review_status_map = self._load_review_status_map([material.id for material in materials])
        _, review_gate = self._apply_review_gate(
            materials=materials,
            review_status_map=review_status_map,
            mode=mode,
        )
        usage_counts = [int(getattr(material, "usage_count", 0) or 0) for material in materials]
        used_count = sum(1 for value in usage_counts if value > 0)
        usage_distribution = {
            "used_count_gt_0": used_count,
            "unused_count_eq_0": max(0, len(usage_counts) - used_count),
            "used_ratio_gt_0": round(used_count / max(1, len(usage_counts)), 4),
            "avg_usage_count": round(sum(usage_counts) / max(1, len(usage_counts)), 4),
            "p50_usage_count": self._percentile(usage_counts, 50),
            "p90_usage_count": self._percentile(usage_counts, 90),
        }
        quality_samples = {
            "avg_quality": round(
                sum(float(getattr(material, "quality_score", 0.0) or 0.0) for material in materials) / max(1, len(materials)),
                4,
            ),
            "low_quality_ratio_lt_045": round(
                sum(1 for material in materials if float(getattr(material, "quality_score", 0.0) or 0.0) < 0.45)
                / max(1, len(materials)),
                4,
            ),
        }
        quality_usage = {
            "low_lt_045": {"total": 0, "used_gt_0": 0, "avg_usage_count": 0.0},
            "mid_045_to_065": {"total": 0, "used_gt_0": 0, "avg_usage_count": 0.0},
            "high_gte_065": {"total": 0, "used_gt_0": 0, "avg_usage_count": 0.0},
        }
        for material in materials:
            quality = float(getattr(material, "quality_score", 0.0) or 0.0)
            usage = int(getattr(material, "usage_count", 0) or 0)
            if quality < 0.45:
                bucket_key = "low_lt_045"
            elif quality < 0.65:
                bucket_key = "mid_045_to_065"
            else:
                bucket_key = "high_gte_065"
            bucket = quality_usage[bucket_key]
            bucket["total"] += 1
            if usage > 0:
                bucket["used_gt_0"] += 1
            bucket["avg_usage_count"] += usage

        for bucket in quality_usage.values():
            total = max(1, int(bucket["total"]))
            bucket["used_ratio_gt_0"] = round(int(bucket["used_gt_0"]) / total, 4)
            bucket["avg_usage_count"] = round(float(bucket["avg_usage_count"]) / total, 4)

        family_counts: Counter[str] = Counter()
        for material in materials:
            for family_id in material.v2_business_family_ids or []:
                family_counts[str(family_id)] += 1

        return {
            "filters": {
                "business_family_id": business_family_id or None,
                "status": status,
                "release_channel": release_channel,
                "review_gate_mode": review_gate["mode"],
                "limit": limit,
            },
            "pool_size": len(materials),
            "review_gate": review_gate,
            "quality_samples": quality_samples,
            "usage_distribution": usage_distribution,
            "quality_usage": quality_usage,
            "v2_family_counts": dict(family_counts),
        }

    @staticmethod
    def _percentile(values: list[int], percentile: int) -> int:
        if not values:
            return 0
        rank = max(0, min(100, int(percentile)))
        sorted_values = sorted(values)
        index = int(round((rank / 100) * (len(sorted_values) - 1)))
        return int(sorted_values[index])

    def _annotate_article_fallback_result(self, *, base_result: dict, business_family_id: str) -> dict:
        annotated = dict(base_result)
        warnings = list(annotated.get("warnings") or [])
        warnings.append(
            f"reviewed_material_cache_miss:{business_family_id}:falling_back_to_article_pipeline"
        )
        annotated["warnings"] = warnings
        annotated["cache_hit"] = False
        annotated["result_mode"] = "article_fallback"
        annotated["governance_status"] = "degraded_unreviewed_source"
        return annotated

    def _minimum_structure_score(self, business_family_id: str, structure_constraints: dict) -> float:
        if not structure_constraints:
            return 0.0
        if business_family_id == "sentence_fill":
            return 0.32 if structure_constraints.get("preserve_blank_position") else 0.20
        if business_family_id == "sentence_order":
            return 0.20 if structure_constraints.get("preserve_unit_count") else 0.15
        return 0.0

    def _cached_structure_match_score(
        self,
        *,
        business_family_id: str,
        cached_item: dict,
        structure_constraints: dict,
    ) -> float:
        if not structure_constraints:
            return 0.0
        business_feature_profile = cached_item.get("business_feature_profile") or {}
        if business_family_id == "sentence_fill":
            profile = business_feature_profile.get("sentence_fill_profile") or {}
            score = 0.0
            expected_position = str(structure_constraints.get("blank_position") or "")
            expected_function = str(structure_constraints.get("function_type") or "")
            if expected_position:
                actual_position = str(profile.get("blank_position") or "")
                if actual_position == expected_position:
                    score += 0.62
                elif structure_constraints.get("preserve_blank_position"):
                    score += 0.08
            if expected_function:
                actual_function = self._canonical_sentence_fill_function_type(
                    profile.get("function_type"),
                    blank_position=str(profile.get("blank_position") or ""),
                )
                expected_function = self._canonical_sentence_fill_function_type(
                    expected_function,
                    blank_position=expected_position,
                )
                if actual_function == expected_function:
                    score += 0.30
            return round(min(1.0, score), 4)
        if business_family_id == "sentence_order":
            profile = business_feature_profile.get("sentence_order_profile") or {}
            score = 0.0
            expected_unit_count = int(structure_constraints.get("sortable_unit_count") or 0)
            if expected_unit_count > 0:
                actual_unit_count = int(profile.get("unit_count") or 0)
                if actual_unit_count == expected_unit_count:
                    score += 0.60
                elif abs(actual_unit_count - expected_unit_count) == 1:
                    score += 0.24
                elif structure_constraints.get("preserve_unit_count"):
                    if 4 <= actual_unit_count <= 8:
                        score += 0.10
                    else:
                        score -= 0.04
            expected_logic_modes = set(structure_constraints.get("logic_modes") or [])
            if expected_logic_modes:
                actual_logic_modes = set(profile.get("logic_modes") or [])
                shared = len(expected_logic_modes.intersection(actual_logic_modes))
                if shared:
                    score += min(0.24, shared * 0.08)
            expected_binding_types = set(structure_constraints.get("binding_types") or [])
            if expected_binding_types:
                actual_binding_types = set(profile.get("binding_rules") or [])
                shared = len(expected_binding_types.intersection(actual_binding_types))
                if shared:
                    score += min(0.16, shared * 0.08)
            expected_binding_pair_count = int(structure_constraints.get("expected_binding_pair_count") or 0)
            if expected_binding_pair_count > 0:
                actual_binding_pair_count = float(profile.get("binding_pair_count") or 0.0)
                if actual_binding_pair_count >= expected_binding_pair_count:
                    score += 0.10
                elif actual_binding_pair_count + 1 >= expected_binding_pair_count:
                    score += 0.05
                else:
                    score -= 0.06
            expected_progression = str(structure_constraints.get("discourse_progression_pattern") or "")
            if expected_progression:
                actual_modes = set(profile.get("logic_modes") or [])
                if expected_progression == "timeline_or_action_sequence":
                    if actual_modes.intersection({"timeline_sequence", "action_sequence"}):
                        score += 0.08
                elif expected_progression in actual_modes:
                    score += 0.08
            if structure_constraints.get("temporal_or_action_sequence_presence"):
                temporal_strength = max(
                    float(profile.get("temporal_order_strength") or 0.0),
                    float(profile.get("action_sequence_irreversibility") or 0.0),
                )
                score += min(0.08, temporal_strength * 0.08)
            expected_unique_answer_strength = float(structure_constraints.get("expected_unique_answer_strength") or 0.0)
            if expected_unique_answer_strength > 0:
                actual_strength = (
                    0.30 * float(profile.get("unique_opener_score") or 0.0)
                    + 0.22 * min(1.0, float(profile.get("binding_pair_count") or 0.0) / 3)
                    + 0.22 * float(profile.get("discourse_progression_strength") or 0.0)
                    + 0.18 * float(profile.get("context_closure_score") or 0.0)
                    - 0.12 * float(profile.get("exchange_risk") or 0.0)
                    - 0.08 * float(profile.get("multi_path_risk") or 0.0)
                )
                if actual_strength >= expected_unique_answer_strength:
                    score += 0.12
                elif actual_strength + 0.08 >= expected_unique_answer_strength:
                    score += 0.06
                else:
                    score -= 0.08
            return round(min(1.0, score), 4)
        return 0.0

    @staticmethod
    def _canonical_sentence_fill_function_type(function_type: Any, *, blank_position: str = "") -> str:
        _ = blank_position
        return normalize_sentence_fill_function_type(function_type)

    def _apply_external_fallback_if_needed(self, *, payload: dict, base_result: dict) -> dict:
        should_fallback, reason = self._should_trigger_external_fallback(payload=payload, base_result=base_result)
        if not should_fallback:
            return base_result
        fallback_result = self._search_with_external_fallback(
            payload=payload,
            base_result=base_result,
            trigger_reason=reason,
        )
        if not fallback_result:
            return base_result
        merged = dict(base_result)
        merged_items = self.pipeline._select_diverse_items(
            list(base_result.get("items") or []) + list(fallback_result.get("items") or []),
            payload.get("candidate_limit", 20),
        )
        merged["items"] = merged_items
        merged["warnings"] = list(base_result.get("warnings") or []) + list(fallback_result.get("warnings") or [])
        merged["article_ids"] = list(dict.fromkeys(list(base_result.get("article_ids") or []) + list(fallback_result.get("article_ids") or [])))
        merged["article_count"] = len(merged["article_ids"])
        merged["external_fallback"] = {
            "trigger_reason": trigger_reason,
            "search_queries": fallback_result.get("search_queries") or [],
            "accepted_count": len(fallback_result.get("items") or []),
            "accepted_domains": fallback_result.get("accepted_domains") or [],
            "ingested_article_ids": fallback_result.get("article_ids") or [],
        }
        return merged

    def _should_trigger_external_fallback(self, *, payload: dict, base_result: dict) -> tuple[bool, str]:
        if not bool(payload.get("enable_external_search_fallback", False)):
            return False, ""
        query_terms = [str(term).strip() for term in (payload.get("query_terms") or []) if str(term).strip()]
        if not query_terms:
            return False, ""
        items = list(base_result.get("items") or [])
        if not items:
            return True, "no_local_items"
        candidate_limit = int(payload.get("candidate_limit", 20) or 20)
        top_quality = max(float(item.get("quality_score") or 0.0) for item in items)
        top_task_score = max(float((item.get("selected_task_scoring") or {}).get("final_candidate_score") or 0.0) for item in items)
        if len(items) < min(3, candidate_limit) and max(top_quality, top_task_score) < 0.62:
            return True, "local_candidates_weak"
        return False, ""

    def _search_with_external_fallback(
        self,
        *,
        payload: dict,
        base_result: dict,
        trigger_reason: str,
    ) -> dict | None:
        query_terms = [str(term).strip() for term in (payload.get("query_terms") or []) if str(term).strip()]
        business_family_id = str(payload.get("business_family_id") or "")
        reference_items = self._reference_items_for_external_fallback(payload=payload, base_result=base_result)
        source_configs = self._preferred_external_source_configs()
        search_queries = self._external_search_queries(query_terms=query_terms, source_configs=source_configs)
        search_hits = self._run_external_search_queries(search_queries=search_queries)
        transient_articles, fetch_trace = self._build_transient_external_articles(search_hits=search_hits)
        if not transient_articles:
            self._log_external_search_history(
                business_family_id=business_family_id,
                query_terms=query_terms,
                trigger_reason=trigger_reason,
                search_queries=search_queries,
                fetch_trace=fetch_trace,
                accepted_items=[],
                rejected_items=[],
                ingested_article_ids=[],
            )
            return None

        transient_result = self.pipeline.search(
            articles=transient_articles,
            business_family_id=business_family_id,
            question_card_id=payload.get("question_card_id"),
            business_card_ids=payload.get("business_card_ids") or [],
            preferred_business_card_ids=payload.get("preferred_business_card_ids") or [],
            query_terms=query_terms,
            topic=payload.get("topic"),
            text_direction=payload.get("text_direction"),
            document_genre=payload.get("document_genre"),
            material_structure_label=payload.get("material_structure_label"),
            structure_constraints=payload.get("structure_constraints") or {},
            candidate_limit=max(payload.get("candidate_limit", 20), self.EXTERNAL_ACCEPT_LIMIT * 2),
            min_card_score=payload.get("min_card_score", 0.55),
            min_business_card_score=payload.get("min_business_card_score", 0.45),
            target_length=payload.get("target_length"),
            length_tolerance=payload.get("length_tolerance", 120),
            enable_anchor_adaptation=payload.get("enable_anchor_adaptation", True),
            preserve_anchor=payload.get("preserve_anchor", True),
        )
        ranked_external = self.pipeline.rank_external_fallback_items(
            items=list(transient_result.get("items") or []),
            business_family_id=business_family_id,
            query_terms=query_terms,
            reference_items=reference_items,
            candidate_limit=self.EXTERNAL_ACCEPT_LIMIT,
        )
        accepted_items = list(ranked_external.get("items") or [])
        rejected_items = list(ranked_external.get("rejected_items") or [])
        if not accepted_items:
            self._log_external_search_history(
                business_family_id=business_family_id,
                query_terms=query_terms,
                trigger_reason=trigger_reason,
                search_queries=search_queries,
                fetch_trace=fetch_trace,
                accepted_items=[],
                rejected_items=rejected_items,
                ingested_article_ids=[],
            )
            return None

        accepted_urls = {
            str(((item.get("source") or {}).get("source_url")) or "")
            for item in accepted_items
            if str(((item.get("source") or {}).get("source_url")) or "")
        }
        accepted_article_ids = self._ingest_and_process_external_articles(
            accepted_urls=accepted_urls,
            transient_articles=transient_articles,
        )
        if not accepted_article_ids:
            self._log_external_search_history(
                business_family_id=business_family_id,
                query_terms=query_terms,
                trigger_reason=trigger_reason,
                search_queries=search_queries,
                fetch_trace=fetch_trace,
                accepted_items=[],
                rejected_items=rejected_items,
                ingested_article_ids=[],
            )
            return None

        articles = [self.article_repo.get(article_id) for article_id in accepted_article_ids]
        articles = [article for article in articles if article is not None]
        if not articles:
            return None
        final_result = self.pipeline.search(
            articles=articles,
            business_family_id=business_family_id,
            question_card_id=payload.get("question_card_id"),
            business_card_ids=payload.get("business_card_ids") or [],
            preferred_business_card_ids=payload.get("preferred_business_card_ids") or [],
            query_terms=query_terms,
            topic=payload.get("topic"),
            text_direction=payload.get("text_direction"),
            document_genre=payload.get("document_genre"),
            material_structure_label=payload.get("material_structure_label"),
            structure_constraints=payload.get("structure_constraints") or {},
            candidate_limit=payload.get("candidate_limit", 20),
            min_card_score=payload.get("min_card_score", 0.55),
            min_business_card_score=payload.get("min_business_card_score", 0.45),
            target_length=payload.get("target_length"),
            length_tolerance=payload.get("length_tolerance", 120),
            enable_anchor_adaptation=payload.get("enable_anchor_adaptation", True),
            preserve_anchor=payload.get("preserve_anchor", True),
        )
        rescored = self.pipeline.rank_external_fallback_items(
            items=list(final_result.get("items") or []),
            business_family_id=business_family_id,
            query_terms=query_terms,
            reference_items=reference_items,
            candidate_limit=self.EXTERNAL_ACCEPT_LIMIT,
        )
        final_items = []
        accepted_domains: list[str] = []
        for item in rescored.get("items") or []:
            enriched = dict(item)
            source = dict(enriched.get("source") or {})
            domain = str(source.get("domain") or "")
            if domain:
                accepted_domains.append(domain)
            enriched["external_fallback"] = {
                "trigger_reason": trigger_reason,
                "query_terms": query_terms[:8],
                "match_profile": enriched.get("external_match_profile") or {},
            }
            final_items.append(enriched)
        if not final_items and accepted_items:
            for item in accepted_items[: self.EXTERNAL_ACCEPT_LIMIT]:
                enriched = dict(item)
                source = dict(enriched.get("source") or {})
                domain = str(source.get("domain") or "")
                if domain:
                    accepted_domains.append(domain)
                enriched["external_fallback"] = {
                    "trigger_reason": trigger_reason,
                    "query_terms": query_terms[:8],
                    "materialized": False,
                    "match_profile": enriched.get("external_match_profile") or {},
                }
                final_items.append(enriched)
        self._log_external_search_history(
            business_family_id=business_family_id,
            query_terms=query_terms,
            trigger_reason=trigger_reason,
            search_queries=search_queries,
            fetch_trace=fetch_trace,
            accepted_items=final_items,
            rejected_items=rejected_items,
            ingested_article_ids=accepted_article_ids,
        )
        if not final_items:
            return None
        return {
            "items": final_items,
            "warnings": [f"External search fallback added {len(final_items)} structurally matched candidates."],
            "article_ids": accepted_article_ids,
            "accepted_domains": list(dict.fromkeys(domain for domain in accepted_domains if domain)),
            "search_queries": [entry["query"] for entry in search_queries],
        }

    def _reference_items_for_external_fallback(self, *, payload: dict, base_result: dict) -> list[dict]:
        items = list(base_result.get("items") or [])
        if items:
            return items[:3]
        business_family_id = str(payload.get("business_family_id") or "")
        materials = self.material_repo.list_v2_cached(
            business_family_id=business_family_id,
            status=MaterialStatus.PROMOTED.value,
            release_channel=ReleaseChannel.STABLE.value,
            limit=12,
        )
        query_terms = [str(term).strip() for term in (payload.get("query_terms") or []) if str(term).strip()]
        references: list[dict] = []
        for material in materials:
            cached_item = dict(material.v2_index_payload or {}).get(business_family_id)
            if not cached_item:
                continue
            haystack = "\n".join(
                [
                    str(cached_item.get("text") or ""),
                    str(cached_item.get("original_text") or ""),
                    str(cached_item.get("article_title") or ""),
                ]
            )
            hit_count = sum(1 for term in query_terms if term in haystack)
            cached_item = dict(cached_item)
            cached_item["_reference_hit_count"] = hit_count
            references.append(cached_item)
        references.sort(
            key=lambda item: (
                int(item.get("_reference_hit_count") or 0),
                float(item.get("quality_score") or 0.0),
            ),
            reverse=True,
        )
        return references[:3]

    def _preferred_external_source_configs(self) -> list[dict]:
        sources = []
        for item in get_config_bundle().sources.get("sources", []):
            if not item.get("enabled", True):
                continue
            base_url = str(item.get("base_url") or "").strip()
            search_domain = urlparse(base_url).netloc.lower().lstrip("www.")
            if not search_domain:
                continue
            normalized = dict(item)
            normalized["search_domain"] = search_domain
            sources.append(normalized)
        if not sources:
            return []
        preference_counts: dict[str, int] = {}
        rows = self.session.scalars(
            select(AuditEventORM)
            .where(AuditEventORM.action == "external_search_fallback")
            .order_by(desc(AuditEventORM.created_at))
            .limit(120)
        )
        for event in rows:
            payload = dict(event.payload or {})
            for domain in payload.get("accepted_domains") or []:
                normalized = str(domain or "").strip().lower()
                if normalized:
                    preference_counts[normalized] = preference_counts.get(normalized, 0) + 1
        sources.sort(
            key=lambda item: (
                preference_counts.get(str(item.get("search_domain") or "").strip().lower(), 0),
                1 if item.get("id") in {"people", "xinhuanet", "gmw", "qstheory", "gov"} else 0,
            ),
            reverse=True,
        )
        return sources[:5]

    def _external_search_queries(self, *, query_terms: list[str], source_configs: list[dict]) -> list[dict]:
        query_text = " ".join(query_terms[:6]).strip()
        if not query_text:
            return []
        queries: list[dict] = []
        seen: set[str] = set()
        for source in source_configs:
            domain = str(source.get("search_domain") or "").strip()
            if not domain:
                continue
            query = f"site:{domain} {query_text}"
            if query in seen:
                continue
            seen.add(query)
            queries.append({"query": query, "domain": domain, "source_id": source.get("id"), "site_name": source.get("site_name")})
        queries.append({"query": query_text, "domain": "", "source_id": None, "site_name": "generic"})
        return queries[:6]

    def _run_external_search_queries(self, *, search_queries: list[dict]) -> list[dict]:
        hits: list[dict] = []
        seen_urls: set[str] = set()
        for query_spec in search_queries:
            query = str(query_spec.get("query") or "").strip()
            if not query:
                continue
            try:
                url = f"https://html.duckduckgo.com/html/?q={quote_plus(query)}"
                html = self.fetcher.fetch_text(url)
            except Exception:
                continue
            parsed_hits = self._parse_duckduckgo_results(
                html=html,
                expected_domain=str(query_spec.get("domain") or "").strip(),
            )
            for hit in parsed_hits:
                normalized_url = str(hit.get("url") or "")
                if not normalized_url or normalized_url in seen_urls:
                    continue
                seen_urls.add(normalized_url)
                hit["query"] = query
                hit["expected_domain"] = query_spec.get("domain")
                hit["source_id"] = query_spec.get("source_id")
                hit["site_name"] = query_spec.get("site_name")
                hits.append(hit)
                if len(hits) >= self.EXTERNAL_SEARCH_RESULT_LIMIT:
                    return hits
        return hits

    def _parse_duckduckgo_results(self, *, html: str, expected_domain: str) -> list[dict]:
        matches = re.findall(r'class="result__a"[^>]+href="([^"]+)"', html, flags=re.IGNORECASE)
        results: list[dict] = []
        for raw_href in matches:
            resolved = self._resolve_search_result_url(raw_href)
            if not resolved:
                continue
            domain = urlparse(resolved).netloc.lower().lstrip("www.")
            if expected_domain and expected_domain.lower() not in domain:
                continue
            if not self._external_url_article_like(resolved):
                continue
            results.append({"url": resolved, "domain": domain})
        return results

    def _resolve_search_result_url(self, raw_href: str) -> str | None:
        href = str(raw_href or "").strip()
        if href.startswith("//"):
            href = f"https:{href}"
        if "duckduckgo.com/l/?" in href:
            parsed = urlparse(href)
            uddg = parse_qs(parsed.query).get("uddg", [])
            if uddg:
                href = unquote(uddg[0])
        if not href.startswith(("http://", "https://")):
            return None
        return href

    def _external_url_article_like(self, url: str) -> bool:
        parsed = urlparse(url)
        path = (parsed.path or "").strip().lower()
        if not path or path == "/":
            return False
        if path in {"/big5/", "/big5", "/index", "/index/"}:
            return False
        segments = [segment for segment in path.split("/") if segment]
        if len(segments) < 2 and not path.endswith((".html", ".htm", ".shtml")):
            return False
        article_patterns = (
            r"/\d{4}/\d{2,}/",
            r"/n\d+/",
            r"\.(html|htm|shtml)$",
            r"/content[_-]",
            r"/detail",
            r"/article",
        )
        if any(re.search(pattern, path, flags=re.IGNORECASE) for pattern in article_patterns):
            return True
        return len(segments) >= 3

    def _build_transient_external_articles(self, *, search_hits: list[dict]) -> tuple[list[SimpleNamespace], list[dict]]:
        transient_articles: list[SimpleNamespace] = []
        fetch_trace: list[dict] = []
        seen_hashes: set[str] = set()
        for hit in search_hits[: self.EXTERNAL_FETCH_LIMIT]:
            url = str(hit.get("url") or "")
            domain = str(hit.get("domain") or "")
            existing = self.article_repo.get_by_source_url(url)
            if existing is not None:
                transient_articles.append(
                    SimpleNamespace(
                        id=existing.id,
                        title=getattr(existing, "title", ""),
                        clean_text=getattr(existing, "clean_text", ""),
                        raw_text=getattr(existing, "raw_text", ""),
                        source=getattr(existing, "source", ""),
                        source_url=getattr(existing, "source_url", url),
                        domain=getattr(existing, "domain", domain),
                    )
                )
                fetch_trace.append({"url": url, "domain": domain, "status": "existing"})
                continue
            try:
                html = self.fetcher.fetch_text(url)
                parsed = self.extractor.extract(
                    html,
                    url,
                    {
                        "site_name": hit.get("site_name") or domain,
                        "domain": domain,
                        "language": "zh",
                    },
                )
                raw_text = str(parsed.get("raw_text") or "").strip()
                clean_text = self.cleaner.clean(raw_text)
                if len(clean_text) < 220:
                    fetch_trace.append({"url": url, "domain": domain, "status": "rejected_short"})
                    continue
                content_hash = hashlib.sha1(clean_text.encode("utf-8")).hexdigest()
                if content_hash in seen_hashes:
                    fetch_trace.append({"url": url, "domain": domain, "status": "rejected_duplicate"})
                    continue
                seen_hashes.add(content_hash)
                transient_articles.append(
                    SimpleNamespace(
                        id=f"external:{content_hash[:16]}",
                        title=str(parsed.get("title") or ""),
                        clean_text=clean_text,
                        raw_text=raw_text,
                        source=hit.get("site_name") or domain,
                        source_url=url,
                        domain=domain,
                    )
                )
                fetch_trace.append({"url": url, "domain": domain, "status": "fetched"})
            except Exception as exc:  # noqa: BLE001
                fetch_trace.append({"url": url, "domain": domain, "status": "fetch_failed", "error": str(exc)})
        return transient_articles, fetch_trace

    def _ingest_and_process_external_articles(
        self,
        *,
        accepted_urls: set[str],
        transient_articles: list[SimpleNamespace],
    ) -> list[str]:
        from app.domain.services.ingest_service import IngestService
        from app.domain.services.process_service import ProcessService

        article_ids: list[str] = []
        for article in transient_articles:
            source_url = str(getattr(article, "source_url", "") or "")
            if source_url not in accepted_urls:
                continue
            existing = self.article_repo.get_by_source_url(source_url)
            if existing is not None:
                article_ids.append(existing.id)
                continue
            stored = IngestService(self.session).ingest(
                {
                    "source": getattr(article, "source", "") or getattr(article, "domain", "") or "external_search",
                    "source_url": source_url,
                    "title": getattr(article, "title", ""),
                    "raw_text": getattr(article, "raw_text", ""),
                    "language": "zh",
                    "domain": getattr(article, "domain", ""),
                }
            )
            ProcessService(self.session).process_article(stored.id, mode="full")
            article_ids.append(stored.id)
        return list(dict.fromkeys(article_ids))

    def _log_external_search_history(
        self,
        *,
        business_family_id: str,
        query_terms: list[str],
        trigger_reason: str,
        search_queries: list[dict],
        fetch_trace: list[dict],
        accepted_items: list[dict],
        rejected_items: list[dict],
        ingested_article_ids: list[str],
    ) -> None:
        accepted_domains = list(
            dict.fromkeys(
                str(((item.get("source") or {}).get("domain")) or "")
                for item in accepted_items
                if str(((item.get("source") or {}).get("domain")) or "")
            )
        )
        entity_id = f"external_search:{hashlib.sha1('|'.join(query_terms).encode('utf-8')).hexdigest()[:12]}"
        self.audit_repo.log(
            "material_search",
            entity_id,
            "external_search_fallback",
            {
                "business_family_id": business_family_id,
                "query_terms": query_terms[:8],
                "trigger_reason": trigger_reason,
                "search_queries": [entry["query"] for entry in search_queries],
                "fetch_trace": fetch_trace,
                "accepted_domains": accepted_domains,
                "accepted_candidate_ids": [item.get("candidate_id") for item in accepted_items],
                "accepted_material_cards": [
                    ((item.get("question_ready_context") or {}).get("selected_material_card"))
                    for item in accepted_items
                ],
                "rejected_count": len(rejected_items),
                "ingested_article_ids": ingested_article_ids,
            },
        )
