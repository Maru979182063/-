from __future__ import annotations

import json
import logging
import sqlite3
from typing import Any
from pathlib import Path

import httpx

from app.core.exceptions import DomainError
from app.schemas.question import MaterialPolicy, MaterialSelectionResult
from app.schemas.runtime import MaterialsConfig
from app.services.question_card_binding import QuestionCardBindingService
from app.services.sentence_fill_protocol import normalize_sentence_fill_function_type


logger = logging.getLogger(__name__)


class MaterialBridgeV2Service:
    SEARCH_TIMEOUT_SECONDS = 16
    SEARCH_RETRY_TIMEOUT_SECONDS = 24
    SERVABLE_REVIEW_STATUSES = {"auto_tagged", "review_confirmed"}
    REJECTED_REVIEW_STATUS = "review_rejected"

    def __init__(self, config: MaterialsConfig) -> None:
        self.config = config
        self.question_card_binding = QuestionCardBindingService()

    def select_materials(
        self,
        *,
        question_type: str,
        business_subtype: str | None,
        question_card_id: str | None = None,
        difficulty_target: str,
        topic: str | None,
        text_direction: str | None,
        document_genre: str | None = None,
        material_structure_label: str | None = None,
        material_policy: MaterialPolicy | None = None,
        count: int,
        article_ids: list[str] | None = None,
        article_limit: int = 12,
        business_card_ids: list[str] | None = None,
        preferred_business_card_ids: list[str] | None = None,
        query_terms: list[str] | None = None,
        target_length: int | None = None,
        length_tolerance: int = 120,
        structure_constraints: dict[str, Any] | None = None,
        enable_anchor_adaptation: bool = True,
        exclude_material_ids: set[str] | None = None,
        preference_profile: dict[str, Any] | None = None,
        usage_stats_lookup=None,
    ) -> tuple[list[MaterialSelectionResult], list[str]]:
        binding = self._resolve_question_card_binding(
            question_type=question_type,
            business_subtype=business_subtype,
            question_card_id=question_card_id,
        )
        resolved_question_card_id = binding.get("question_card_id")
        business_family_id = self._resolve_business_family_id(binding)
        warnings: list[str] = []
        binding_warning = self._missing_question_card_warning(
            requested_question_card_id=question_card_id,
            resolved_question_card_id=resolved_question_card_id,
        )
        if binding_warning:
            warnings.append(binding_warning)
        requested_candidate_limit = max(min(self.config.candidate_pool_size, 16), count * 4)
        search_result = self._search_candidates(
            business_family_id=business_family_id,
            question_card_id=resolved_question_card_id,
            article_ids=article_ids or [],
            article_limit=article_limit,
            candidate_limit=requested_candidate_limit,
            min_card_score=self._min_card_score(difficulty_target),
            business_card_ids=business_card_ids or [],
            preferred_business_card_ids=preferred_business_card_ids or [],
            query_terms=query_terms or [],
            topic=topic,
            text_direction=text_direction,
            document_genre=document_genre,
            material_structure_label=material_structure_label,
            target_length=target_length,
            length_tolerance=length_tolerance,
            structure_constraints=structure_constraints or {},
            enable_anchor_adaptation=enable_anchor_adaptation,
        )
        warnings.extend(search_result.get("warnings") or [])
        items = search_result.get("items") or []
        items = self._attach_local_usage_stats(items, usage_stats_lookup)
        ranked = sorted(
            (
                self._score_candidate(
                    item,
                    topic=topic,
                    text_direction=text_direction,
                    document_genre=document_genre,
                    material_structure_label=material_structure_label,
                    material_policy=material_policy,
                    has_explicit_question_card=bool(question_card_id),
                    requested_business_card_ids=business_card_ids or [],
                    structure_constraints=structure_constraints or {},
                    query_terms=query_terms or [],
                    target_length=target_length,
                    preference_profile=preference_profile,
                )
                for item in items
            ),
            key=lambda entry: entry["sort_key"],
            reverse=True,
        )
        selections: list[MaterialSelectionResult] = []
        used_ids: set[str] = set()
        strict_rejections: list[dict[str, Any]] = []
        excluded = set(material_policy.excluded_material_ids) if material_policy else set()
        excluded.update(exclude_material_ids or set())
        for entry in ranked:
            item = entry["item"]
            material_id = str(item.get("candidate_id") or "")
            if not material_id or material_id in used_ids or material_id in excluded:
                continue
            if material_policy and not material_policy.allow_reuse and int(item.get("usage_count") or 0) > 0:
                strict_rejections.append(entry)
                continue
            if material_policy and self._is_in_cooldown(item.get("last_used_at"), material_policy.cooldown_days):
                strict_rejections.append(entry)
                continue
            used_ids.add(material_id)
            selections.append(
                self._to_material_selection(
                    item,
                    entry["reason"],
                    decision_meta=entry.get("decision_meta"),
                    planner_score=entry.get("score"),
                    sort_key=entry.get("sort_key"),
                )
            )
            if len(selections) >= count:
                break

        if len(selections) < count and strict_rejections:
            warnings.append(
                "Fresh v2 materials were insufficient under the current reuse policy; fell back to the least-reused candidates."
            )
            fallback_ranked = sorted(
                strict_rejections,
                key=lambda entry: (
                    int(entry["item"].get("usage_count") or 0),
                    self._cooldown_sort_value(entry["item"].get("last_used_at")),
                    -float(entry.get("sort_key", (0.0,))[0]),
                    -float(entry.get("score") or 0.0),
                ),
            )
            for entry in fallback_ranked:
                item = entry["item"]
                material_id = str(item.get("candidate_id") or "")
                if not material_id or material_id in used_ids or material_id in excluded:
                    continue
                used_ids.add(material_id)
                selections.append(
                    self._to_material_selection(
                        item,
                        f"{entry['reason']}; fallback_due_to_material_policy",
                        decision_meta=entry.get("decision_meta"),
                        planner_score=entry.get("score"),
                        sort_key=entry.get("sort_key"),
                    )
                )
                if len(selections) >= count:
                    break
        if len(selections) < count:
            warnings.append("Not enough v2 materials were returned; generated item count may be lower than requested.")
        return selections, warnings

    def list_material_options(
        self,
        *,
        question_type: str = "main_idea",
        business_subtype: str | None = "title_selection",
        question_card_id: str | None = None,
        document_genre: str | None = None,
        material_structure_label: str | None = None,
        business_card_ids: list[str] | None = None,
        preferred_business_card_ids: list[str] | None = None,
        query_terms: list[str] | None = None,
        target_length: int | None = None,
        length_tolerance: int = 120,
        structure_constraints: dict[str, Any] | None = None,
        enable_anchor_adaptation: bool = True,
        exclude_material_ids: set[str] | None = None,
        limit: int = 8,
        article_ids: list[str] | None = None,
        article_limit: int = 24,
        difficulty_target: str = "medium",
        preference_profile: dict[str, Any] | None = None,
        usage_stats_lookup=None,
    ) -> list[MaterialSelectionResult]:
        binding = self._resolve_question_card_binding(
            question_type=question_type,
            business_subtype=business_subtype,
            question_card_id=question_card_id,
        )
        resolved_question_card_id = binding.get("question_card_id")
        business_family_id = self._resolve_business_family_id(binding)
        self._log_missing_question_card_binding(
            requested_question_card_id=question_card_id,
            resolved_question_card_id=resolved_question_card_id,
            method_name="list_material_options",
        )
        items = self._attach_local_usage_stats(
            (self._search_candidates(
                business_family_id=business_family_id,
                question_card_id=resolved_question_card_id,
                article_ids=article_ids or [],
                article_limit=article_limit,
                candidate_limit=max(min(self.config.candidate_pool_size, 16), limit * 4),
                min_card_score=self._min_card_score(difficulty_target),
                business_card_ids=business_card_ids or [],
                preferred_business_card_ids=preferred_business_card_ids or [],
                query_terms=query_terms or [],
                topic=None,
                text_direction=None,
                document_genre=document_genre,
                material_structure_label=material_structure_label,
                target_length=target_length,
                length_tolerance=length_tolerance,
                structure_constraints=structure_constraints or {},
                enable_anchor_adaptation=enable_anchor_adaptation,
            ).get("items") or []),
            usage_stats_lookup,
        )
        ranked = sorted(
            (
                self._score_candidate(
                    item,
                    topic=None,
                    text_direction=None,
                    document_genre=document_genre,
                    material_structure_label=material_structure_label,
                    material_policy=None,
                    has_explicit_question_card=bool(question_card_id),
                    requested_business_card_ids=business_card_ids or [],
                    structure_constraints=structure_constraints or {},
                    query_terms=query_terms or [],
                    target_length=target_length,
                    preference_profile=preference_profile,
                )
                for item in items
            ),
            key=lambda entry: (
                int(entry["item"].get("usage_count") or 0),
                -float(entry["sort_key"][0]),
                -float(entry["sort_key"][3]),
                -float(entry["score"]),
            ),
        )
        excluded = exclude_material_ids or set()
        selections: list[MaterialSelectionResult] = []
        for entry in ranked:
            item = entry["item"]
            material_id = str(item.get("candidate_id") or "")
            if not material_id or material_id in excluded:
                continue
            selections.append(
                self._to_material_selection(
                    item,
                    "replacement_candidate",
                    decision_meta=entry.get("decision_meta"),
                    planner_score=entry.get("score"),
                    sort_key=entry.get("sort_key"),
                )
            )
            if len(selections) >= limit:
                break
        return selections

    def preview_candidates(
        self,
        *,
        question_type: str,
        business_subtype: str | None,
        question_card_id: str | None = None,
        article_ids: list[str] | None = None,
        article_limit: int = 12,
        candidate_limit: int = 8,
        difficulty_target: str = "medium",
    ) -> dict[str, Any]:
        binding = self._resolve_question_card_binding(
            question_type=question_type,
            business_subtype=business_subtype,
            question_card_id=question_card_id,
        )
        resolved_question_card_id = binding.get("question_card_id")
        business_family_id = self._resolve_business_family_id(binding)
        binding_warning = self._missing_question_card_warning(
            requested_question_card_id=question_card_id,
            resolved_question_card_id=resolved_question_card_id,
        )
        search_result = self._search_candidates(
            business_family_id=business_family_id,
            question_card_id=resolved_question_card_id,
            article_ids=article_ids or [],
            article_limit=article_limit,
            candidate_limit=candidate_limit,
            min_card_score=self._min_card_score(difficulty_target),
            business_card_ids=[],
            preferred_business_card_ids=[],
            query_terms=[],
            topic=None,
            text_direction=None,
            document_genre=None,
            material_structure_label=None,
            target_length=None,
            length_tolerance=120,
            structure_constraints={},
            enable_anchor_adaptation=True,
        )
        items = search_result.get("items") or []
        return {
            "business_family_id": business_family_id,
            "question_card_id": resolved_question_card_id,
            "question_card_binding_warning": binding_warning,
            "items": items,
            "warnings": search_result.get("warnings") or [],
        }

    def _missing_question_card_warning(
        self,
        *,
        requested_question_card_id: str | None,
        resolved_question_card_id: str | None,
    ) -> str | None:
        if requested_question_card_id:
            return None
        if resolved_question_card_id:
            return (
                "question_card_id_missing: upstream did not provide an explicit question card binding; "
                f"bridge derived question_card_id={resolved_question_card_id} from normalized question_card.runtime_binding."
            )
        return None

    def _log_missing_question_card_binding(
        self,
        *,
        requested_question_card_id: str | None,
        resolved_question_card_id: str | None,
        method_name: str,
    ) -> None:
        binding_warning = self._missing_question_card_warning(
            requested_question_card_id=requested_question_card_id,
            resolved_question_card_id=resolved_question_card_id,
        )
        if binding_warning:
            logger.warning("%s %s", method_name, binding_warning)

    def _resolve_question_card_binding(
        self,
        *,
        question_type: str,
        business_subtype: str | None,
        question_card_id: str | None = None,
    ) -> dict[str, Any]:
        return self.question_card_binding.resolve(
            question_card_id=question_card_id,
            question_type=question_type,
            business_subtype=business_subtype,
            require_match=True,
        )

    def _resolve_business_family_id(self, binding: dict[str, Any]) -> str:
        question_card = binding.get("question_card") or {}
        business_family_id = str(question_card.get("business_family_id") or "").strip()
        if business_family_id:
            return business_family_id
        runtime_binding = binding.get("runtime_binding") or {}
        raise DomainError(
            "Resolved question card binding did not provide a business_family_id.",
            status_code=422,
            details={
                "question_card_id": binding.get("question_card_id"),
                "question_type": runtime_binding.get("question_type"),
                "business_subtype": runtime_binding.get("business_subtype"),
            },
        )

    def _min_card_score(self, difficulty_target: str) -> float:
        return {
            "easy": 0.48,
            "medium": 0.55,
            "hard": 0.60,
        }.get(difficulty_target, 0.55)

    def _search_candidates(
        self,
        *,
        business_family_id: str,
        question_card_id: str | None,
        article_ids: list[str],
        article_limit: int,
        candidate_limit: int,
        min_card_score: float,
        business_card_ids: list[str],
        preferred_business_card_ids: list[str],
        query_terms: list[str],
        topic: str | None,
        text_direction: str | None,
        document_genre: str | None,
        material_structure_label: str | None,
        target_length: int | None,
        length_tolerance: int,
        structure_constraints: dict[str, Any],
        enable_anchor_adaptation: bool,
    ) -> dict[str, Any]:
        warnings: list[str] = []
        payload = {
            "business_family_id": business_family_id,
            "question_card_id": question_card_id,
            "article_ids": article_ids,
            "article_limit": article_limit,
            "candidate_limit": candidate_limit,
            "min_card_score": min_card_score,
            "business_card_ids": business_card_ids,
            "preferred_business_card_ids": preferred_business_card_ids,
            "query_terms": query_terms,
            "topic": topic,
            "text_direction": text_direction,
            "document_genre": document_genre,
            "material_structure_label": material_structure_label,
            "target_length": target_length,
            "length_tolerance": length_tolerance,
            "structure_constraints": structure_constraints,
            "enable_anchor_adaptation": enable_anchor_adaptation,
            "review_gate_mode": "stable_relaxed",
        }
        try:
            data = self._post_v2_search(payload)
        except DomainError as exc:
            if not self._is_search_timeout_error(exc):
                raise
            relaxed_payload = self._build_relaxed_search_payload(
                payload,
                query_terms=[],
                business_card_ids=business_card_ids,
                structure_constraints=structure_constraints,
            )
            data = self._post_v2_search(relaxed_payload, timeout=self.SEARCH_RETRY_TIMEOUT_SECONDS)
        warnings.extend(self._extract_search_warnings(data))
        items = data.get("items", [])
        if not items and query_terms:
            relaxed_payload = self._build_relaxed_search_payload(
                payload,
                query_terms=[],
                business_card_ids=business_card_ids,
                structure_constraints=structure_constraints,
            )
            data = self._post_v2_search(relaxed_payload)
            warnings.extend(self._extract_search_warnings(data))
        items = data.get("items", [])
        if not items and structure_constraints and not (query_terms or business_card_ids or preferred_business_card_ids):
            loose_payload = self._build_relaxed_search_payload(
                payload,
                query_terms=[],
                business_card_ids=[],
                structure_constraints={},
                enable_anchor_adaptation=False,
            )
            data = self._post_v2_search(loose_payload, timeout=self.SEARCH_RETRY_TIMEOUT_SECONDS)
            warnings.extend(self._extract_search_warnings(data))
        items = data.get("items", [])
        if not isinstance(items, list):
            raise DomainError(
                "passage_service returned an invalid v2 materials payload.",
                status_code=502,
                details={"payload_keys": sorted(data.keys())},
            )
        return {
            "items": self._filter_reviewable_items(items),
            "warnings": self._dedupe_warnings(warnings),
        }

    def _post_v2_search(self, payload: dict[str, Any], *, timeout: int | None = None) -> dict[str, Any]:
        try:
            with httpx.Client(base_url=self.config.base_url, timeout=timeout or self.SEARCH_TIMEOUT_SECONDS) as client:
                response = client.post(self.config.v2_search_path, json=payload)
                response.raise_for_status()
                data = response.json()
        except httpx.HTTPError as exc:
            if self._disable_local_sqlite_fallback(payload):
                raise DomainError(
                    "Failed to fetch v2 materials from passage_service.",
                    status_code=502,
                    details={
                        "base_url": self.config.base_url,
                        "search_path": self.config.v2_search_path,
                        "reason": str(exc),
                        "fallback_blocked": True,
                    },
                ) from exc
            fallback_items = self._search_candidates_local_sqlite(payload)
            if fallback_items:
                return {
                    "items": fallback_items,
                    "warnings": [f"local_sqlite_fallback:{type(exc).__name__}"],
                    "cache_hit": True,
                }
            raise DomainError(
                "Failed to fetch v2 materials from passage_service.",
                status_code=502,
                details={"base_url": self.config.base_url, "search_path": self.config.v2_search_path, "reason": str(exc)},
            ) from exc
        return data

    @staticmethod
    def _extract_search_warnings(data: dict[str, Any]) -> list[str]:
        raw = data.get("warnings") if isinstance(data, dict) else []
        if not isinstance(raw, list):
            return []
        return [str(item) for item in raw if str(item).strip()]

    @staticmethod
    def _dedupe_warnings(warnings: list[str]) -> list[str]:
        merged: list[str] = []
        seen: set[str] = set()
        for warning in warnings:
            text = str(warning).strip()
            if not text or text in seen:
                continue
            seen.add(text)
            merged.append(text)
        return merged

    @staticmethod
    def _disable_local_sqlite_fallback(payload: dict[str, Any]) -> bool:
        return False

    @staticmethod
    def _build_relaxed_search_payload(
        payload: dict[str, Any],
        *,
        query_terms: list[str],
        business_card_ids: list[str],
        structure_constraints: dict[str, Any],
        enable_anchor_adaptation: bool | None = None,
    ) -> dict[str, Any]:
        relaxed_payload = dict(payload)
        relaxed_payload["query_terms"] = list(query_terms)
        relaxed_payload["business_card_ids"] = list(business_card_ids)
        relaxed_payload["candidate_limit"] = max(8, min(int(payload.get("candidate_limit") or 8), 12))
        relaxed_payload["article_limit"] = min(int(payload.get("article_limit") or 12), 10)
        relaxed_payload["structure_constraints"] = dict(structure_constraints)
        if enable_anchor_adaptation is not None:
            relaxed_payload["enable_anchor_adaptation"] = bool(enable_anchor_adaptation)
        return relaxed_payload

    @staticmethod
    def _is_search_timeout_error(exc: DomainError) -> bool:
        details = exc.details if isinstance(exc.details, dict) else {}
        reason = str(details.get("reason") or "").lower()
        return "timed out" in reason or "timeout" in reason

    def _search_candidates_local_sqlite(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        db_path = self._fallback_db_path()
        if not db_path.exists():
            return []

        business_family_id = str(payload.get("business_family_id") or "")
        requested_business_card_ids = {card_id for card_id in (payload.get("business_card_ids") or []) if card_id}
        preferred_business_card_ids = {card_id for card_id in (payload.get("preferred_business_card_ids") or []) if card_id}
        structure_constraints = dict(payload.get("structure_constraints") or {})
        query_terms = [term for term in (payload.get("query_terms") or []) if term]
        article_ids = [article_id for article_id in (payload.get("article_ids") or []) if article_id]
        limit = max(int(payload.get("candidate_limit") or 20) * 12, 120)
        # Local sqlite fallback should scan broadly enough to actually see
        # non-top-family materials; otherwise sentence_fill/sentence_order can
        # be starved by a quality-sorted top slice dominated by other families.
        limit = max(limit, 1600)

        sql = """
            SELECT material_spans.id,
                   material_spans.article_id,
                   material_spans.quality_score,
                   material_spans.usage_count,
                   material_spans.last_used_at,
                   material_spans.v2_business_family_ids,
                   material_spans.v2_index_payload,
                   tagging_reviews.status
            FROM material_spans
            LEFT JOIN tagging_reviews ON tagging_reviews.material_id = material_spans.id
            WHERE material_spans.is_primary = 1
              AND material_spans.v2_index_version IS NOT NULL
              AND material_spans.status = 'promoted'
              AND material_spans.release_channel = 'stable'
            ORDER BY material_spans.quality_score DESC, material_spans.updated_at DESC
            LIMIT ?
        """

        connection = sqlite3.connect(str(db_path))
        try:
            cursor = connection.cursor()
            cursor.execute(sql, (limit,))
            rows = cursor.fetchall()
        finally:
            connection.close()

        enforce_structure_gate = bool(requested_business_card_ids)
        ranked_matches: list[tuple[dict[str, Any], tuple[float, float, int, float]]] = []
        for row in rows:
            material_id, article_id, _quality_score, usage_count, last_used_at, family_ids_raw, payload_raw, review_status = row
            if article_ids and article_id not in article_ids:
                continue
            if review_status and review_status not in self.SERVABLE_REVIEW_STATUSES:
                continue

            family_ids = self._decode_json_value(family_ids_raw, default=[])
            if business_family_id and business_family_id not in family_ids:
                continue

            payload_map = self._decode_json_value(payload_raw, default={})
            cached_item = dict((payload_map or {}).get(business_family_id) or {})
            if not cached_item:
                continue
            if not self._cached_item_matches_front_filters(cached_item=cached_item, payload=payload):
                continue

            cached_item["usage_count"] = int(usage_count or 0)
            cached_item["last_used_at"] = last_used_at
            cached_item["article_id"] = cached_item.get("article_id") or article_id
            cached_item["candidate_id"] = cached_item.get("candidate_id") or material_id
            cached_item["review_status"] = review_status

            haystack = "\n".join(
                [
                    str(cached_item.get("text") or ""),
                    str(cached_item.get("original_text") or ""),
                    str(cached_item.get("article_title") or ""),
                ]
            )
            selected_business_card = str(((cached_item.get("question_ready_context") or {}).get("selected_business_card")) or "")
            cached_recommended = set(cached_item.get("business_card_recommendations") or [])
            if selected_business_card:
                cached_recommended.add(selected_business_card)

            card_score = 0.0
            if requested_business_card_ids:
                if selected_business_card in requested_business_card_ids:
                    card_score = 2.0
                elif requested_business_card_ids.intersection(cached_recommended):
                    card_score = 1.0
                else:
                    continue
            elif preferred_business_card_ids:
                if selected_business_card in preferred_business_card_ids:
                    card_score = 1.0
                elif preferred_business_card_ids.intersection(cached_recommended):
                    card_score = 0.5
            else:
                card_score = 1.0

            has_query_match = not query_terms or any(term in haystack for term in query_terms)
            if not has_query_match:
                continue

            structure_score = self._local_structure_match_score(
                business_family_id=business_family_id,
                cached_item=cached_item,
                structure_constraints=structure_constraints,
            )
            if enforce_structure_gate and structure_score < self._local_minimum_structure_score(
                business_family_id=business_family_id,
                structure_constraints=structure_constraints,
            ):
                continue

            quality_score = float(cached_item.get("quality_score") or 0.0)
            ranked_matches.append(
                (
                    cached_item,
                    (
                        float(card_score),
                        float(structure_score),
                        int(sum(1 for term in query_terms if term in haystack)),
                        float(quality_score),
                    ),
                )
            )

        ranked_matches.sort(key=lambda entry: entry[1], reverse=True)
        return [item for item, _ in ranked_matches[: int(payload.get("candidate_limit") or 20)]]

    @staticmethod
    def _local_minimum_structure_score(*, business_family_id: str, structure_constraints: dict[str, Any]) -> float:
        if not structure_constraints:
            return 0.0
        if business_family_id == "sentence_fill":
            return 0.32 if structure_constraints.get("preserve_blank_position") else 0.20
        if business_family_id == "sentence_order":
            return 0.20 if structure_constraints.get("preserve_unit_count") else 0.15
        return 0.0

    def _local_structure_match_score(
        self,
        *,
        business_family_id: str,
        cached_item: dict[str, Any],
        structure_constraints: dict[str, Any],
    ) -> float:
        if not structure_constraints:
            return 0.0
        business_feature_profile = cached_item.get("business_feature_profile") or {}
        if business_family_id != "sentence_fill":
            return 0.0
        profile = business_feature_profile.get("sentence_fill_profile") or {}
        expected_position = str(structure_constraints.get("blank_position") or "")
        expected_function = self._canonical_sentence_fill_function_type(
            structure_constraints.get("function_type"),
            blank_position=expected_position,
        )
        actual_position = str(profile.get("blank_position") or "")
        actual_function = self._canonical_sentence_fill_function_type(
            profile.get("function_type"),
            blank_position=actual_position,
        )
        score = 0.0
        if expected_position:
            if actual_position == expected_position:
                score += 0.62
            elif structure_constraints.get("preserve_blank_position"):
                score += 0.08
        if expected_function and actual_function == expected_function:
            score += 0.30
        return round(min(1.0, score), 4)

    @staticmethod
    def _canonical_sentence_fill_function_type(function_type: Any, *, blank_position: str = "") -> str:
        _ = blank_position
        return normalize_sentence_fill_function_type(function_type)

    @staticmethod
    def _cached_item_matches_front_filters(*, cached_item: dict[str, Any], payload: dict[str, Any]) -> bool:
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

    def _filter_reviewable_items(self, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        filtered: list[dict[str, Any]] = []
        for item in items:
            review_status = item.get("review_status")
            if review_status and review_status not in self.SERVABLE_REVIEW_STATUSES:
                continue
            filtered.append(item)
        return filtered

    def _fallback_db_path(self) -> Path:
        root = Path(__file__).resolve().parents[3]
        return root / "passage_service" / "passage_service.db"

    @staticmethod
    def _decode_json_value(raw: Any, *, default: Any) -> Any:
        if raw is None:
            return default
        if isinstance(raw, (dict, list)):
            return raw
        try:
            return json.loads(raw)
        except Exception:
            return default

    @staticmethod
    def _extract_candidate_scoring(item: dict[str, Any]) -> dict[str, Any]:
        scoring = item.get("selected_task_scoring") or (item.get("meta") or {}).get("scoring") or {}
        return dict(scoring) if isinstance(scoring, dict) else {}

    @staticmethod
    def _normalize_preference_profile(preference_profile: dict[str, Any] | None) -> dict[str, float]:
        raw = preference_profile if isinstance(preference_profile, dict) else {}
        normalized: dict[str, float] = {}
        for key in (
            "prefer_higher_reasoning_depth",
            "prefer_lower_ambiguity",
            "prefer_higher_constraint_intensity",
            "penalty_tolerance",
            "repair_tolerance",
        ):
            try:
                value = float(raw.get(key, 0.0) or 0.0)
            except (TypeError, ValueError):
                value = 0.0
            normalized[key] = round(max(-1.0, min(1.0, value)), 4)
        return normalized

    @staticmethod
    def _positive_mapping_snapshot(payload: dict[str, Any], *, limit: int = 3) -> dict[str, float]:
        if not isinstance(payload, dict):
            return {}
        ranked = sorted(
            (
                (str(key), round(float(value or 0.0), 4))
                for key, value in payload.items()
                if float(value or 0.0) > 0
            ),
            key=lambda entry: entry[1],
            reverse=True,
        )
        return {key: value for key, value in ranked[:limit]}

    @staticmethod
    def _has_active_preference(preference_profile: dict[str, float]) -> bool:
        return any(abs(float(value or 0.0)) >= 0.05 for value in preference_profile.values())

    def _build_feedback_snapshot(
        self,
        *,
        scoring: dict[str, Any],
        decision_meta: dict[str, Any],
        preference_profile: dict[str, float],
    ) -> dict[str, Any]:
        if not scoring and decision_meta.get("decision_reason") == "material_scoring_missing":
            return {
                "selection_state": decision_meta.get("selection_state"),
                "review_like_risk": bool(decision_meta.get("review_like_risk")),
                "repair_suggested": bool(decision_meta.get("repair_suggested")),
                "decision_reason": decision_meta.get("decision_reason"),
                "repair_reason": decision_meta.get("repair_reason"),
                "quality_difficulty_note": None,
                "final_candidate_score": None,
                "readiness_score": None,
                "total_penalty": None,
                "difficulty_band_hint": None,
                "difficulty_vector": {},
                "recommended": False,
                "needs_review": False,
                "key_penalties": {},
                "key_difficulty_dimensions": {},
                "preference_profile": dict(preference_profile),
            }
        risk_penalties = scoring.get("risk_penalties") if isinstance(scoring.get("risk_penalties"), dict) else {}
        difficulty_vector = scoring.get("difficulty_vector") if isinstance(scoring.get("difficulty_vector"), dict) else {}
        scoring_summary = decision_meta.get("scoring_summary") if isinstance(decision_meta.get("scoring_summary"), dict) else {}
        return {
            "selection_state": decision_meta.get("selection_state"),
            "review_like_risk": bool(decision_meta.get("review_like_risk")),
            "repair_suggested": bool(decision_meta.get("repair_suggested")),
            "decision_reason": decision_meta.get("decision_reason"),
            "repair_reason": decision_meta.get("repair_reason"),
            "final_candidate_score": round(float(scoring.get("final_candidate_score") or scoring_summary.get("final_candidate_score") or 0.0), 4),
            "readiness_score": round(float(scoring.get("readiness_score") or scoring_summary.get("readiness_score") or 0.0), 4),
            "total_penalty": round(float(scoring_summary.get("total_penalty") or 0.0), 4),
            "difficulty_band_hint": scoring.get("difficulty_band_hint") or scoring_summary.get("difficulty_band_hint"),
            "difficulty_vector": {str(key): round(float(value or 0.0), 4) for key, value in difficulty_vector.items()},
            "recommended": bool(scoring.get("recommended") if "recommended" in scoring else scoring_summary.get("recommended")),
            "needs_review": bool(scoring.get("needs_review") if "needs_review" in scoring else scoring_summary.get("needs_review")),
            "key_penalties": dict(decision_meta.get("key_penalties") or self._positive_mapping_snapshot(risk_penalties, limit=3)),
            "key_difficulty_dimensions": dict(decision_meta.get("key_difficulty_dimensions") or self._positive_mapping_snapshot(difficulty_vector, limit=3)),
            "preference_profile": dict(preference_profile),
        }

    def _build_decision_meta(self, item: dict[str, Any], *, preference_profile: dict[str, Any] | None = None) -> dict[str, Any]:
        normalized_preference = self._normalize_preference_profile(preference_profile)
        scoring = self._extract_candidate_scoring(item)
        if not scoring:
            return {
                "selection_state": "hold",
                "review_like_risk": False,
                "repair_suggested": False,
                "decision_reason": "material_scoring_missing",
                "repair_reason": None,
                "quality_difficulty_note": None,
                "scoring_summary": {
                    "task_family": None,
                    "final_candidate_score": 0.0,
                    "readiness_score": 0.0,
                    "total_penalty": 0.0,
                    "recommended": False,
                    "needs_review": False,
                    "difficulty_band_hint": "",
                },
                "key_penalties": {},
                "key_difficulty_dimensions": {},
                "preference_profile": normalized_preference,
                "preference_note": "neutral_preference_profile",
            }

        risk_penalties = scoring.get("risk_penalties") if isinstance(scoring.get("risk_penalties"), dict) else {}
        difficulty_vector = scoring.get("difficulty_vector") if isinstance(scoring.get("difficulty_vector"), dict) else {}
        total_penalty = round(sum(float(value or 0.0) for value in risk_penalties.values()), 4)
        final_candidate_score = round(float(scoring.get("final_candidate_score") or 0.0), 4)
        readiness_score = round(float(scoring.get("readiness_score") or 0.0), 4)
        recommended = bool(scoring.get("recommended"))
        needs_review = bool(scoring.get("needs_review"))
        difficulty_band = str(scoring.get("difficulty_band_hint") or "").strip().lower()
        difficulty_trace = scoring.get("difficulty_trace") if isinstance(scoring.get("difficulty_trace"), dict) else {}
        band_decision = difficulty_trace.get("band_decision") if isinstance(difficulty_trace.get("band_decision"), dict) else {}
        quality_difficulty_note = str(band_decision.get("quality_difficulty_note") or "").strip() or None
        key_penalties = self._positive_mapping_snapshot(risk_penalties, limit=3)
        key_difficulty_dimensions = self._positive_mapping_snapshot(difficulty_vector, limit=3)
        strongest_difficulty = max(key_difficulty_dimensions.values(), default=0.0)
        penalty_tolerance = normalized_preference.get("penalty_tolerance", 0.0)
        repair_tolerance = normalized_preference.get("repair_tolerance", 0.0)
        elevated_penalty_threshold = 0.4 + 0.08 * penalty_tolerance
        high_penalty_threshold = 0.6 + 0.10 * penalty_tolerance
        weak_penalty_threshold = 0.45 + 0.08 * penalty_tolerance
        salvage_readiness_threshold = 0.45 - 0.04 * repair_tolerance
        high_penalty = total_penalty >= high_penalty_threshold
        elevated_penalty = total_penalty >= elevated_penalty_threshold
        low_readiness = readiness_score < 0.4
        weak_quality = final_candidate_score < 0.3
        structurally_salvageable = bool(
            readiness_score >= salvage_readiness_threshold
            or strongest_difficulty >= 0.62
            or (difficulty_band in {"medium", "hard"} and final_candidate_score >= 0.3)
        )
        clearly_weak = bool(
            (weak_quality and low_readiness)
            or (not recommended and readiness_score < 0.35)
            or (difficulty_band == "easy" and weak_quality and low_readiness)
            or (not structurally_salvageable and total_penalty >= weak_penalty_threshold)
        )

        if recommended and not needs_review and final_candidate_score >= 0.5 and total_penalty < 0.45:
            selection_state = "recommended"
        elif clearly_weak:
            selection_state = "weak_candidate"
        else:
            selection_state = "hold"

        review_like_risk = bool(
            needs_review
            or (readiness_score >= 0.5 and elevated_penalty)
            or (selection_state == "hold" and total_penalty >= max(0.45, high_penalty_threshold - 0.05))
        )

        repair_suggested = False
        repair_reason: str | None = None
        if selection_state == "hold":
            repair_readiness_threshold = 0.45 - 0.05 * repair_tolerance
            repair_penalty_threshold = 0.45 + 0.08 * penalty_tolerance
            top_penalty = next(iter(key_penalties), None)
            if readiness_score >= repair_readiness_threshold and total_penalty >= repair_penalty_threshold:
                repair_suggested = True
                if top_penalty == "role_ambiguity_penalty":
                    repair_reason = "role_ambiguity_repairable_risk"
                else:
                    repair_reason = "high_readiness_high_penalty"
            elif strongest_difficulty >= max(0.58, 0.65 - 0.06 * repair_tolerance) and key_penalties:
                repair_suggested = True
                if top_penalty == "role_ambiguity_penalty":
                    repair_reason = "role_ambiguity_repairable_risk"
                else:
                    repair_reason = f"structurally_strong_but_{top_penalty}"
            elif quality_difficulty_note == "hard_but_currently_weak_candidate":
                repair_suggested = True
                repair_reason = "hard_but_currently_weak_candidate"
        elif selection_state == "weak_candidate":
            if difficulty_band == "hard" and readiness_score >= 0.38 and quality_difficulty_note == "hard_but_currently_weak_candidate":
                repair_suggested = True
                repair_reason = "hard_but_currently_weak_candidate"

        if selection_state == "recommended":
            decision_reason = "recommended_stable_candidate"
        elif recommended and needs_review:
            decision_reason = "recommended_candidate_requires_review"
        elif quality_difficulty_note == "hard_but_currently_weak_candidate":
            decision_reason = "hard_but_currently_weak_candidate"
        elif readiness_score >= 0.5 and elevated_penalty:
            decision_reason = "high_readiness_high_penalty"
        elif selection_state == "weak_candidate" and difficulty_band != "hard" and total_penalty >= 0.45:
            decision_reason = "high_risk_but_not_high_difficulty"
        elif selection_state == "weak_candidate" and difficulty_band == "easy":
            decision_reason = "easy_but_weak_candidate"
        elif selection_state == "hold":
            decision_reason = "borderline_hold_candidate"
        else:
            decision_reason = "overall_weak_candidate"

        return {
            "selection_state": selection_state,
            "review_like_risk": review_like_risk,
            "repair_suggested": repair_suggested,
            "decision_reason": decision_reason,
            "repair_reason": repair_reason,
            "quality_difficulty_note": quality_difficulty_note,
            "scoring_summary": {
                "task_family": scoring.get("task_family"),
                "final_candidate_score": final_candidate_score,
                "readiness_score": readiness_score,
                "total_penalty": total_penalty,
                "recommended": recommended,
                "needs_review": needs_review,
                "difficulty_band_hint": difficulty_band,
            },
            "key_penalties": key_penalties,
            "key_difficulty_dimensions": key_difficulty_dimensions,
            "preference_profile": normalized_preference,
            "preference_note": (
                "preference_profile_applied"
                if self._has_active_preference(normalized_preference)
                else "neutral_preference_profile"
            ),
        }

    @staticmethod
    def _decision_sort_key(*, planner_score: float, decision_meta: dict[str, Any]) -> tuple[float, ...]:
        scoring_summary = decision_meta.get("scoring_summary") if isinstance(decision_meta, dict) else {}
        final_candidate_score = float((scoring_summary or {}).get("final_candidate_score") or 0.0)
        readiness_score = float((scoring_summary or {}).get("readiness_score") or 0.0)
        total_penalty = float((scoring_summary or {}).get("total_penalty") or 0.0)
        recommended = bool((scoring_summary or {}).get("recommended"))
        needs_review = bool((scoring_summary or {}).get("needs_review"))
        difficulty_band = str((scoring_summary or {}).get("difficulty_band_hint") or "")
        selection_state = str((decision_meta or {}).get("selection_state") or "hold")
        repair_suggested = bool((decision_meta or {}).get("repair_suggested"))
        review_like_risk = bool((decision_meta or {}).get("review_like_risk"))
        state_priority = {"recommended": 2.0, "hold": 1.0, "weak_candidate": 0.0}.get(selection_state, 0.5)
        stable_priority = 1.0 if recommended and not needs_review else 0.0
        repair_priority = 1.0 if repair_suggested else 0.0
        penalty_headroom = max(0.0, 1.0 - min(total_penalty, 1.0))
        band_priority = {"hard": 0.2, "medium": 0.1, "easy": 0.0}.get(difficulty_band, 0.0)
        risk_headroom = 0.0 if review_like_risk else 1.0
        preference_profile = decision_meta.get("preference_profile") if isinstance(decision_meta.get("preference_profile"), dict) else {}
        preference_intensity = round(sum(abs(float(value or 0.0)) for value in preference_profile.values()), 4)
        return (
            state_priority,
            stable_priority,
            repair_priority,
            final_candidate_score,
            readiness_score,
            penalty_headroom,
            risk_headroom,
            band_priority,
            preference_intensity,
            round(planner_score, 4),
        )

    def _score_candidate(
        self,
        item: dict[str, Any],
        *,
        topic: str | None,
        text_direction: str | None,
        document_genre: str | None,
        material_structure_label: str | None,
        material_policy: MaterialPolicy | None,
        has_explicit_question_card: bool,
        requested_business_card_ids: list[str],
        structure_constraints: dict[str, Any],
        query_terms: list[str],
        target_length: int | None,
        preference_profile: dict[str, Any] | None,
    ) -> dict[str, Any]:
        score = float(item.get("quality_score") or 0.0)
        reasons = [f"quality_score={score:.2f}"]
        text = str(item.get("text") or "")
        article_profile = item.get("article_profile") or {}
        local_profile = item.get("local_profile") or {}
        retrieval_match_profile = item.get("retrieval_match_profile") or {}
        business_feature_profile = item.get("business_feature_profile") or {}
        sentence_order_profile = business_feature_profile.get("sentence_order_profile") or {}
        candidate_genre = article_profile.get("document_genre")
        candidate_structure = local_profile.get("discourse_shape") or article_profile.get("discourse_shape")
        readability = 1 - float(local_profile.get("context_dependency") or article_profile.get("context_dependency") or 0.0)
        score += readability * 0.20
        reasons.append(f"readability={readability:.2f}")
        usage_count = int(item.get("usage_count") or 0)
        if usage_count > 0:
            reuse_penalty = min(0.36, usage_count * 0.08)
            score -= reuse_penalty
            reasons.append(f"reuse_penalty={reuse_penalty:.2f}")
        if topic and topic in text:
            score += 0.08
            reasons.append("topic_match")
        if text_direction and any(text_direction in str(value) for value in (local_profile.get("core_object"), article_profile.get("core_object"), text)):
            score += 0.12
            reasons.append("text_direction_match")
        if document_genre and candidate_genre == document_genre:
            score += 0.22
            reasons.append("document_genre_match")
        if material_structure_label and candidate_structure == material_structure_label and not has_explicit_question_card:
            score += 0.22
            reasons.append("material_structure_match")
        if material_policy and material_policy.preferred_document_genres and candidate_genre in material_policy.preferred_document_genres:
            score += 0.18
            reasons.append("preferred_document_genre")
        if material_policy and material_policy.prefer_high_quality_reused and usage_count > 0 and float(item.get("quality_score") or 0.0) >= 0.8:
            score += 0.10
            reasons.append("prefer_high_quality_reused")
        if material_policy and material_policy.cooldown_days > 0 and item.get("last_used_at"):
            if self._is_in_cooldown(item.get("last_used_at"), material_policy.cooldown_days):
                score -= 0.28
                reasons.append(f"cooldown_penalty={material_policy.cooldown_days}d")
        selected_business_card = str(((item.get("question_ready_context") or {}).get("selected_business_card")) or "")
        recommended_business_cards = set(item.get("business_card_recommendations") or [])
        if selected_business_card:
            recommended_business_cards.add(selected_business_card)
        requested_card_set = {card_id for card_id in requested_business_card_ids if card_id}
        # Once an explicit question card is already bound upstream, bridge should
        # not re-promote candidates via local business-card preference bonuses.
        if requested_card_set and not has_explicit_question_card:
            if selected_business_card in requested_card_set:
                score += 0.40
                reasons.append("selected_business_card_exact")
            elif requested_card_set.intersection(recommended_business_cards):
                score += 0.20
                reasons.append("business_card_recommendation_match")
        expected_unit_count = int(structure_constraints.get("sortable_unit_count") or 0)
        if expected_unit_count > 0 and structure_constraints.get("preserve_unit_count") and not has_explicit_question_card:
            actual_unit_count = int(sentence_order_profile.get("unit_count") or 0)
            if actual_unit_count != expected_unit_count:
                unit_count_penalty = 0.22
                score -= unit_count_penalty
                reasons.append(
                    f"sentence_order_unit_count_penalty={unit_count_penalty:.2f} expected={expected_unit_count} actual={actual_unit_count}"
                )
        structure_bonus = self._structure_alignment_bonus(
            item=item,
            structure_constraints=structure_constraints,
            has_explicit_question_card=has_explicit_question_card,
        )
        if structure_bonus:
            score += structure_bonus
            reasons.append(f"structure_alignment={structure_bonus:.2f}")
        retrieval_score = float(retrieval_match_profile.get("match_score") or 0.0)
        if query_terms and retrieval_score > 0:
            score += min(0.12, retrieval_score * 0.12)
            reasons.append(f"query_match={retrieval_score:.2f}")
        length_fit = float(retrieval_match_profile.get("length_fit_score") or 0.0)
        if target_length and length_fit > 0:
            score += min(0.22, length_fit * 0.22)
            reasons.append(f"length_fit={length_fit:.2f}")
        scoring = self._extract_candidate_scoring(item)
        difficulty_vector = scoring.get("difficulty_vector") if isinstance(scoring.get("difficulty_vector"), dict) else {}
        risk_penalties = scoring.get("risk_penalties") if isinstance(scoring.get("risk_penalties"), dict) else {}
        normalized_preference = self._normalize_preference_profile(preference_profile)
        if self._has_active_preference(normalized_preference):
            reasoning_depth = float(difficulty_vector.get("reasoning_depth_score") or 0.0)
            ambiguity = float(difficulty_vector.get("ambiguity_score") or 0.0)
            constraint_intensity = float(difficulty_vector.get("constraint_intensity_score") or 0.0)
            total_penalty = min(1.0, sum(float(value or 0.0) for value in risk_penalties.values()))
            preference_adjustment = 0.0
            preference_adjustment += 0.12 * normalized_preference["prefer_higher_reasoning_depth"] * (reasoning_depth - 0.5)
            preference_adjustment += 0.10 * normalized_preference["prefer_higher_constraint_intensity"] * (constraint_intensity - 0.5)
            preference_adjustment += 0.10 * normalized_preference["prefer_lower_ambiguity"] * (0.5 - ambiguity)
            preference_adjustment += 0.08 * normalized_preference["penalty_tolerance"] * (total_penalty - 0.5)
            if preference_adjustment:
                score += preference_adjustment
                reasons.append(f"preference_adjustment={preference_adjustment:.4f}")
        top_card = ((item.get("eligible_material_cards") or [{}])[0] or {}).get("card_id")
        if top_card:
            reasons.append(f"top_card={top_card}")
        decision_meta = self._build_decision_meta(item, preference_profile=normalized_preference)
        reasons.append(f"preference_note={decision_meta.get('preference_note')}")
        scoring_summary = decision_meta.get("scoring_summary") or {}
        reasons.append(
            "selection_state={state}; final_candidate_score={final_score:.4f}; readiness_score={readiness:.4f}; total_penalty={penalty:.4f}".format(
                state=decision_meta.get("selection_state"),
                final_score=float(scoring_summary.get("final_candidate_score") or 0.0),
                readiness=float(scoring_summary.get("readiness_score") or 0.0),
                penalty=float(scoring_summary.get("total_penalty") or 0.0),
            )
        )
        reasons.append(f"decision_reason={decision_meta.get('decision_reason')}")
        return {
            "item": item,
            "score": score,
            "reason": "; ".join(reasons),
            "decision_meta": decision_meta,
            "sort_key": self._decision_sort_key(planner_score=score, decision_meta=decision_meta),
        }

    def _structure_alignment_bonus(
        self,
        *,
        item: dict[str, Any],
        structure_constraints: dict[str, Any],
        has_explicit_question_card: bool,
    ) -> float:
        if not structure_constraints:
            return 0.0
        bonus = 0.0
        business_feature_profile = item.get("business_feature_profile") or {}
        sentence_fill_profile = business_feature_profile.get("sentence_fill_profile") or {}
        sentence_order_profile = business_feature_profile.get("sentence_order_profile") or {}

        expected_blank_position = str(structure_constraints.get("blank_position") or "")
        if expected_blank_position and not has_explicit_question_card:
            actual_blank_position = str(sentence_fill_profile.get("blank_position") or "")
            if actual_blank_position == expected_blank_position:
                bonus += 0.34
            elif structure_constraints.get("preserve_blank_position"):
                bonus -= 0.08

        expected_function_type = str(structure_constraints.get("function_type") or "")
        if expected_function_type and not has_explicit_question_card:
            actual_function_type = str(sentence_fill_profile.get("function_type") or "")
            if actual_function_type == expected_function_type:
                bonus += 0.24

        expected_unit_count = int(structure_constraints.get("sortable_unit_count") or 0)
        if expected_unit_count > 0 and not has_explicit_question_card:
            actual_unit_count = int(sentence_order_profile.get("unit_count") or 0)
            if actual_unit_count == expected_unit_count:
                bonus += 0.36
            elif abs(actual_unit_count - expected_unit_count) == 1:
                bonus += 0.10
            elif structure_constraints.get("preserve_unit_count"):
                bonus -= 0.24

        expected_logic_modes = set(structure_constraints.get("logic_modes") or [])
        if expected_logic_modes and not has_explicit_question_card:
            actual_logic_modes = set(sentence_order_profile.get("logic_modes") or [])
            shared = len(expected_logic_modes.intersection(actual_logic_modes))
            if shared > 0:
                bonus += min(0.20, shared * 0.08)

        expected_binding_types = set(structure_constraints.get("binding_types") or [])
        if expected_binding_types and not has_explicit_question_card:
            actual_binding_types = set(sentence_order_profile.get("binding_rules") or [])
            shared = len(expected_binding_types.intersection(actual_binding_types))
            if shared > 0:
                bonus += min(0.16, shared * 0.08)

        expected_binding_pair_count = int(structure_constraints.get("expected_binding_pair_count") or 0)
        if expected_binding_pair_count > 0 and not has_explicit_question_card:
            actual_binding_pair_count = float(sentence_order_profile.get("binding_pair_count") or 0.0)
            if actual_binding_pair_count >= expected_binding_pair_count:
                bonus += 0.10
            elif actual_binding_pair_count + 1 >= expected_binding_pair_count:
                bonus += 0.04
            else:
                bonus -= 0.08

        expected_progression = str(structure_constraints.get("discourse_progression_pattern") or "")
        if expected_progression and not has_explicit_question_card:
            actual_logic_modes = set(sentence_order_profile.get("logic_modes") or [])
            if expected_progression == "timeline_or_action_sequence":
                if actual_logic_modes.intersection({"timeline_sequence", "action_sequence"}):
                    bonus += 0.10
            elif expected_progression in actual_logic_modes:
                bonus += 0.10

        if structure_constraints.get("temporal_or_action_sequence_presence") and not has_explicit_question_card:
            temporal_strength = max(
                float(sentence_order_profile.get("temporal_order_strength") or 0.0),
                float(sentence_order_profile.get("action_sequence_irreversibility") or 0.0),
            )
            bonus += min(0.08, temporal_strength * 0.08)

        expected_unique_answer_strength = float(structure_constraints.get("expected_unique_answer_strength") or 0.0)
        if expected_unique_answer_strength > 0 and not has_explicit_question_card:
            actual_strength = (
                0.30 * float(sentence_order_profile.get("unique_opener_score") or 0.0)
                + 0.22 * min(1.0, float(sentence_order_profile.get("binding_pair_count") or 0.0) / 3)
                + 0.22 * float(sentence_order_profile.get("discourse_progression_strength") or 0.0)
                + 0.18 * float(sentence_order_profile.get("context_closure_score") or 0.0)
                - 0.12 * float(sentence_order_profile.get("exchange_risk") or 0.0)
                - 0.08 * float(sentence_order_profile.get("multi_path_risk") or 0.0)
            )
            if actual_strength >= expected_unique_answer_strength:
                bonus += 0.12
            elif actual_strength + 0.08 >= expected_unique_answer_strength:
                bonus += 0.04
            else:
                bonus -= 0.12

        return round(bonus, 4)

    def _to_material_selection(
        self,
        item: dict[str, Any],
        selection_reason: str,
        *,
        decision_meta: dict[str, Any] | None = None,
        planner_score: float | None = None,
        sort_key: tuple[float, ...] | None = None,
    ) -> MaterialSelectionResult:
        local_profile = item.get("local_profile") or {}
        article_profile = item.get("article_profile") or {}
        question_ready_context = item.get("question_ready_context") or {}
        question_card_id = question_ready_context.get("question_card_id")
        runtime_binding = question_ready_context.get("runtime_binding")
        resolved_slots = question_ready_context.get("resolved_slots")
        validator_contract = question_ready_context.get("validator_contract")
        selected_material_card = question_ready_context.get("selected_material_card")
        selected_business_card = question_ready_context.get("selected_business_card")
        generation_archetype = question_ready_context.get("generation_archetype")
        prompt_extras = question_ready_context.get("prompt_extras") or {}
        raw_preference_profile = (
            (decision_meta.get("preference_profile") if isinstance(decision_meta, dict) else None)
            or (
            item.get("preference_profile")
            or question_ready_context.get("preference_profile")
            or (item.get("source") or {}).get("preference_profile")
            )
        )
        normalized_preference = self._normalize_preference_profile(raw_preference_profile if isinstance(raw_preference_profile, dict) else None)
        resolved_decision_meta = (
            dict(decision_meta)
            if isinstance(decision_meta, dict)
            else self._build_decision_meta(item, preference_profile=normalized_preference)
        )
        scoring_payload = self._extract_candidate_scoring(item)
        feedback_snapshot = self._build_feedback_snapshot(
            scoring=scoring_payload,
            decision_meta=resolved_decision_meta,
            preference_profile=normalized_preference,
        )
        selection_reason_with_decision = (
            f"{selection_reason}; selection_state={resolved_decision_meta.get('selection_state')}; decision_reason={resolved_decision_meta.get('decision_reason')}"
        )
        consumable_text = str(item.get("consumable_text") or item.get("text") or "")
        if isinstance(selected_business_card, str) and selected_business_card.startswith("sentence_fill__"):
            consumable_text = str(prompt_extras.get("blanked_text") or consumable_text)
        fit_scores = {entry.get("card_id"): float(entry.get("score") or 0.0) for entry in item.get("eligible_material_cards") or [] if entry.get("card_id")}
        knowledge_tags = [
            str(value)
            for value in [
                item.get("candidate_type"),
                selected_material_card,
                selected_business_card,
                generation_archetype,
                local_profile.get("core_object"),
            ]
            if value
        ]
        return MaterialSelectionResult(
            material_id=str(item.get("candidate_id") or ""),
            article_id=str(item.get("article_id") or ""),
            question_card_id=str(question_card_id) if question_card_id else None,
            runtime_binding=dict(runtime_binding) if isinstance(runtime_binding, dict) else None,
            resolved_slots=dict(resolved_slots) if isinstance(resolved_slots, dict) else None,
            validator_contract=dict(validator_contract) if isinstance(validator_contract, dict) else None,
            text=consumable_text,
            original_text=str(item.get("original_text") or item.get("text") or ""),
            source={
                **(item.get("source") or {}),
                "article_title": item.get("article_title"),
                "selected_business_card": selected_business_card,
                "selected_material_card": selected_material_card,
                "prompt_extras": prompt_extras,
                "scoring": scoring_payload,
                "selected_task_scoring": scoring_payload,
                "task_scoring": dict(item.get("task_scoring") or {}),
                "decision_meta": resolved_decision_meta,
                "feedback_snapshot": feedback_snapshot,
                "preference_profile": normalized_preference,
                "ranking_meta": {
                    "planner_score": round(float(planner_score or 0.0), 4),
                    "sort_key": [round(float(value), 4) for value in (sort_key or ())],
                    "selection_reason": selection_reason_with_decision,
                    "preference_note": resolved_decision_meta.get("preference_note"),
                },
            },
            source_tail=((item.get("source") or {}).get("source_url")),
            primary_label=selected_material_card,
            document_genre=article_profile.get("document_genre"),
            material_structure_label=local_profile.get("discourse_shape") or article_profile.get("discourse_shape"),
            material_structure_reason=generation_archetype,
            standalone_readability=round(1 - float(local_profile.get("context_dependency") or article_profile.get("context_dependency") or 0.0), 4),
            quality_score=float(item.get("quality_score") or 0.0),
            fit_scores=fit_scores,
            knowledge_tags=knowledge_tags,
            usage_count_before=int(item.get("usage_count") or 0),
            previously_used=bool(int(item.get("usage_count") or 0) > 0),
            last_used_at=item.get("last_used_at"),
            selection_reason=selection_reason_with_decision,
            anchor_adapted=bool(((item.get("meta") or {}).get("anchor_adaptation") or {}).get("adapted")),
            anchor_adaptation_reason=((item.get("meta") or {}).get("anchor_adaptation") or {}).get("reason"),
            anchor_span=((item.get("meta") or {}).get("anchor_adaptation") or {}),
        )

    def _attach_local_usage_stats(self, items: list[dict[str, Any]], usage_stats_lookup) -> list[dict[str, Any]]:
        if usage_stats_lookup is None:
            return items
        enriched: list[dict[str, Any]] = []
        for item in items:
            material_id = str(item.get("candidate_id") or "")
            usage = usage_stats_lookup(material_id) if material_id else {
                "usage_count_before": 0,
                "previously_used": False,
                "last_used_at": None,
            }
            enriched.append(
                {
                    **item,
                    "usage_count": int(usage.get("usage_count_before") or 0),
                    "last_used_at": usage.get("last_used_at"),
                }
            )
        return enriched

    def _is_in_cooldown(self, last_used_at: str | None, cooldown_days: int) -> bool:
        if not last_used_at or cooldown_days <= 0:
            return False
        try:
            import datetime as _dt

            normalized = last_used_at.replace("Z", "+00:00")
            timestamp = _dt.datetime.fromisoformat(normalized)
            now = _dt.datetime.now(_dt.timezone.utc)
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=_dt.timezone.utc)
            return (now - timestamp) < _dt.timedelta(days=cooldown_days)
        except ValueError:
            return False

    def _cooldown_sort_value(self, last_used_at: str | None) -> float:
        if not last_used_at:
            return float("-inf")
        try:
            import datetime as _dt

            normalized = last_used_at.replace("Z", "+00:00")
            timestamp = _dt.datetime.fromisoformat(normalized)
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=_dt.timezone.utc)
            return timestamp.timestamp()
        except ValueError:
            return float("inf")
