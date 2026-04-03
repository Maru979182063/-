from app.domain.services._common import ServiceBase
from app.domain.services.material_v2_index_service import MaterialV2IndexService
from app.services.material_pipeline_v2 import MaterialPipelineV2


class MaterialPipelineV2Service(ServiceBase):
    def __init__(self, session) -> None:
        super().__init__(session)
        self.pipeline = MaterialPipelineV2()

    def search(self, payload: dict) -> dict:
        cached_result = self._search_cached(payload)
        if cached_result is not None:
            return cached_result
        requested_ids = payload.get("article_ids") or []
        article_limit = payload.get("article_limit", 10)
        business_family_id = payload["business_family_id"]
        query_terms = payload.get("query_terms") or []
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
            query_terms=payload.get("query_terms") or [],
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
        return result

    def precompute(self, payload: dict) -> dict:
        return MaterialV2IndexService(self.session).precompute(payload)

    def _search_cached(self, payload: dict) -> dict | None:
        business_family_id = payload["business_family_id"]
        candidate_limit = payload.get("candidate_limit", 20)
        cache_lookup_limit = max(candidate_limit * 8, 80)
        if business_family_id in {"sentence_order", "sentence_fill"}:
            cache_lookup_limit = max(candidate_limit * 40, 800)
        requested_business_card_ids = set(payload.get("business_card_ids") or [])
        structure_constraints = dict(payload.get("structure_constraints") or {})
        materials = self.material_repo.list_v2_cached(
            business_family_id=business_family_id,
            material_ids=payload.get("material_ids") or None,
            article_ids=payload.get("article_ids") or None,
            status=payload.get("status"),
            release_channel=payload.get("release_channel"),
            limit=cache_lookup_limit,
        )
        if not materials:
            return None
        items = []
        article_ids: list[str] = []
        query_terms = [term for term in (payload.get("query_terms") or []) if term]
        prefiltered: list[tuple[object, dict, tuple[int, float, int, float]]] = []
        prefilter_limit = max(candidate_limit * 10, 120)
        tier_candidates: list[tuple[object, dict, tuple[int, float, int, float]]] = []
        relaxed_card_candidates: list[tuple[object, dict, tuple[int, float, int, float]]] = []
        for material in materials:
            cached_payload = dict(material.v2_index_payload or {})
            cached_item = cached_payload.get(business_family_id)
            if not cached_item:
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
            else:
                card_score = 1
            quality_score = float(cached_item.get("quality_score") or getattr(material, "quality_score", 0.0) or 0.0)
            entry = (material, cached_item, (card_score, structure_score, hit_count, quality_score))
            relaxed_card_candidates.append(entry)

            if requested_business_card_ids:
                cached_recommended = set(cached_item.get("business_card_recommendations") or [])
                if selected_business_card:
                    cached_recommended.add(selected_business_card)
                if not requested_business_card_ids.intersection(cached_recommended):
                    continue
            tier_candidates.append(entry)

        if not tier_candidates and requested_business_card_ids:
            tier_candidates = relaxed_card_candidates
        if not tier_candidates:
            return None

        if requested_business_card_ids and business_family_id in {"sentence_fill", "sentence_order"}:
            exact_card_matches = [entry for entry in tier_candidates if entry[2][0] >= 2]
            if exact_card_matches:
                tier_candidates = exact_card_matches

        strict = [
            entry for entry in tier_candidates
            if (
                (entry[2][0] > 0 or not requested_business_card_ids)
                and (entry[2][2] > 0 or not query_terms)
                and entry[2][1] >= self._minimum_structure_score(business_family_id, structure_constraints)
            )
        ]
        relaxed = [
            entry for entry in tier_candidates
            if (
                (entry[2][0] > 0 or not requested_business_card_ids)
                and entry[2][1] >= self._minimum_structure_score(business_family_id, structure_constraints)
            )
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
            refreshed = self.pipeline.refresh_cached_item(
                cached_item=cached_item,
                query_terms=query_terms,
                target_length=payload.get("target_length"),
                length_tolerance=payload.get("length_tolerance", 120),
                enable_anchor_adaptation=payload.get("enable_anchor_adaptation", True),
                preserve_anchor=payload.get("preserve_anchor", True),
            )
            refreshed["usage_count"] = int(getattr(material, "usage_count", 0) or 0)
            refreshed["last_used_at"] = material.last_used_at.isoformat() if getattr(material, "last_used_at", None) else None
            items.append(refreshed)
            article_ids.append(material.article_id)
        if not items:
            return None
        question_card_id = payload.get("question_card_id")
        question_card = self.pipeline.registry.get_question_card(question_card_id) if question_card_id else self.pipeline.registry.get_default_question_card(business_family_id)
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
        }

    def _minimum_structure_score(self, business_family_id: str, structure_constraints: dict) -> float:
        if not structure_constraints:
            return 0.0
        if business_family_id == "sentence_fill":
            return 0.45 if structure_constraints.get("preserve_blank_position") else 0.20
        if business_family_id == "sentence_order":
            return 0.30 if structure_constraints.get("preserve_unit_count") else 0.15
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
                    return 0.0
            if expected_function:
                actual_function = str(profile.get("function_type") or "")
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
                    return 0.0
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
