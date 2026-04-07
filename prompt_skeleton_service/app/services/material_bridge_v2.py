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


logger = logging.getLogger(__name__)


class MaterialBridgeV2Service:
    SEARCH_TIMEOUT_SECONDS = 8

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
        items = self._search_candidates(
            business_family_id=business_family_id,
            question_card_id=resolved_question_card_id,
            article_ids=article_ids or [],
            article_limit=article_limit,
            candidate_limit=requested_candidate_limit,
            min_card_score=self._min_card_score(difficulty_target),
            business_card_ids=business_card_ids or [],
            preferred_business_card_ids=preferred_business_card_ids or [],
            query_terms=query_terms or [],
            target_length=target_length,
            length_tolerance=length_tolerance,
            structure_constraints=structure_constraints or {},
            enable_anchor_adaptation=enable_anchor_adaptation,
        )
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
                )
                for item in items
            ),
            key=lambda entry: entry["score"],
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
            selections.append(self._to_material_selection(item, entry["reason"]))
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
                    -float(entry["item"].get("quality_score") or 0.0),
                ),
            )
            for entry in fallback_ranked:
                item = entry["item"]
                material_id = str(item.get("candidate_id") or "")
                if not material_id or material_id in used_ids or material_id in excluded:
                    continue
                used_ids.add(material_id)
                selections.append(self._to_material_selection(item, f"{entry['reason']}; fallback_due_to_material_policy"))
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
            self._search_candidates(
                business_family_id=business_family_id,
                question_card_id=resolved_question_card_id,
                article_ids=article_ids or [],
                article_limit=article_limit,
                candidate_limit=max(min(self.config.candidate_pool_size, 16), limit * 4),
                min_card_score=self._min_card_score(difficulty_target),
                business_card_ids=business_card_ids or [],
                preferred_business_card_ids=preferred_business_card_ids or [],
                query_terms=query_terms or [],
                target_length=target_length,
                length_tolerance=length_tolerance,
                structure_constraints=structure_constraints or {},
                enable_anchor_adaptation=enable_anchor_adaptation,
            ),
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
                )
                for item in items
            ),
            key=lambda entry: (
                int(entry["item"].get("usage_count") or 0),
                -entry["score"],
            ),
        )
        excluded = exclude_material_ids or set()
        selections: list[MaterialSelectionResult] = []
        for entry in ranked:
            item = entry["item"]
            material_id = str(item.get("candidate_id") or "")
            if not material_id or material_id in excluded:
                continue
            selections.append(self._to_material_selection(item, "replacement_candidate"))
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
        items = self._search_candidates(
            business_family_id=business_family_id,
            question_card_id=resolved_question_card_id,
            article_ids=article_ids or [],
            article_limit=article_limit,
            candidate_limit=candidate_limit,
            min_card_score=self._min_card_score(difficulty_target),
            business_card_ids=[],
            preferred_business_card_ids=[],
            query_terms=[],
            target_length=None,
            length_tolerance=120,
            structure_constraints={},
            enable_anchor_adaptation=True,
        )
        return {
            "business_family_id": business_family_id,
            "question_card_id": resolved_question_card_id,
            "question_card_binding_warning": binding_warning,
            "items": items,
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
        target_length: int | None,
        length_tolerance: int,
        structure_constraints: dict[str, Any],
        enable_anchor_adaptation: bool,
    ) -> list[dict[str, Any]]:
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
            "target_length": target_length,
            "length_tolerance": length_tolerance,
            "structure_constraints": structure_constraints,
            "enable_anchor_adaptation": enable_anchor_adaptation,
        }
        data = self._post_v2_search(payload)
        items = data.get("items", [])
        if not items and (query_terms or business_card_ids):
            relaxed_payload = dict(payload)
            relaxed_payload["query_terms"] = []
            relaxed_payload["business_card_ids"] = []
            relaxed_payload["candidate_limit"] = max(8, min(candidate_limit, 12))
            relaxed_payload["article_limit"] = min(article_limit, 10)
            data = self._post_v2_search(relaxed_payload)
        items = data.get("items", [])
        if not isinstance(items, list):
            raise DomainError(
                "passage_service returned an invalid v2 materials payload.",
                status_code=502,
                details={"payload_keys": sorted(data.keys())},
            )
        return items

    def _post_v2_search(self, payload: dict[str, Any]) -> dict[str, Any]:
        try:
            with httpx.Client(base_url=self.config.base_url, timeout=self.SEARCH_TIMEOUT_SECONDS) as client:
                response = client.post(self.config.v2_search_path, json=payload)
                response.raise_for_status()
                data = response.json()
        except httpx.HTTPError as exc:
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

    def _search_candidates_local_sqlite(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        db_path = self._fallback_db_path()
        if not db_path.exists():
            return []

        business_family_id = str(payload.get("business_family_id") or "")
        requested_business_card_ids = {card_id for card_id in (payload.get("business_card_ids") or []) if card_id}
        query_terms = [term for term in (payload.get("query_terms") or []) if term]
        article_ids = [article_id for article_id in (payload.get("article_ids") or []) if article_id]
        limit = max(int(payload.get("candidate_limit") or 20) * 12, 120)

        sql = """
            SELECT id, article_id, quality_score, usage_count, last_used_at, v2_business_family_ids, v2_index_payload
            FROM material_spans
            WHERE is_primary = 1
              AND v2_index_version IS NOT NULL
            ORDER BY quality_score DESC, updated_at DESC
            LIMIT ?
        """

        connection = sqlite3.connect(str(db_path))
        try:
            cursor = connection.cursor()
            cursor.execute(sql, (limit,))
            rows = cursor.fetchall()
        finally:
            connection.close()

        matched: list[dict[str, Any]] = []
        relaxed: list[dict[str, Any]] = []
        for row in rows:
            material_id, article_id, _quality_score, usage_count, last_used_at, family_ids_raw, payload_raw = row
            if article_ids and article_id not in article_ids:
                continue

            family_ids = self._decode_json_value(family_ids_raw, default=[])
            if business_family_id and business_family_id not in family_ids:
                continue

            payload_map = self._decode_json_value(payload_raw, default={})
            cached_item = dict((payload_map or {}).get(business_family_id) or {})
            if not cached_item:
                continue

            cached_item["usage_count"] = int(usage_count or 0)
            cached_item["last_used_at"] = last_used_at
            cached_item["article_id"] = cached_item.get("article_id") or article_id
            cached_item["candidate_id"] = cached_item.get("candidate_id") or material_id

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

            has_card_match = not requested_business_card_ids or bool(requested_business_card_ids.intersection(cached_recommended))
            has_query_match = not query_terms or any(term in haystack for term in query_terms)

            if has_query_match:
                relaxed.append(cached_item)
            if has_card_match and has_query_match:
                matched.append(cached_item)

        if matched:
            return matched[: int(payload.get("candidate_limit") or 20)]
        if relaxed:
            return relaxed[: max(8, min(int(payload.get("candidate_limit") or 20), 12))]
        return []

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
                unit_count_penalty = 0.42
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
        top_card = ((item.get("eligible_material_cards") or [{}])[0] or {}).get("card_id")
        if top_card:
            reasons.append(f"top_card={top_card}")
        return {"item": item, "score": score, "reason": "; ".join(reasons)}

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
                bonus -= 0.18

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
            elif structure_constraints.get("preserve_unit_count"):
                bonus -= 0.80

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

    def _to_material_selection(self, item: dict[str, Any], selection_reason: str) -> MaterialSelectionResult:
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
            selection_reason=selection_reason,
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
