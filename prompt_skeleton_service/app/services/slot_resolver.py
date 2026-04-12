from __future__ import annotations

from typing import Any

from app.core.exceptions import DomainError
from app.schemas.api import (
    DifficultyDeviation,
    DifficultyFit,
    DifficultyProjection,
    DifficultyTargetProfile,
    PatternSelectionReason,
)
from app.schemas.config import BusinessSubtypeConfig, PatternConfig, QuestionTypeConfig, SlotFieldConfig


class SlotResolverService:
    TYPE_CASTERS = {
        "string": str,
        "integer": int,
        "number": float,
        "boolean": bool,
        "array": list,
        "object": dict,
    }

    ASCENDING_DIFFICULTY_SLOTS = {
        "option_confusion": ["low", "low_medium", "medium", "medium_high", "high"],
        "distractor_strength": ["low", "medium", "high"],
        "abstraction_level": ["low", "medium", "high"],
        "context_dependency": ["low", "medium", "high"],
        "bidirectional_validation": ["low", "medium", "high"],
        "reference_dependency": ["low", "medium", "high"],
        "block_order_complexity": ["low", "medium", "high"],
        "coverage_requirement": ["close_rephrase", "integrated", "abstract_generalization"],
        "blank_position": ["opening", "middle", "ending", "inserted", "mixed"],
        "function_type": [
            "summary",
            "topic_intro",
            "carry_previous",
            "lead_next",
            "bridge",
            "reference_summary",
            "countermeasure",
            "conclusion",
        ],
        "logic_relation": [
            "continuation",
            "transition",
            "explanation",
            "focus_shift",
            "summary",
            "action",
            "elevation",
            "reference_match",
            "multi_constraint",
        ],
    }

    DESCENDING_DIFFICULTY_SLOTS = {
        "statement_visibility": ["high", "medium", "low"],
        "anchor_clarity": ["high", "medium", "low"],
        "opening_signal_strength": ["high", "medium", "low", "none"],
        "closing_signal_strength": ["high", "medium", "low", "none"],
        "local_binding_strength": ["high", "medium", "low"],
    }

    def resolve(
        self,
        question_type_config: QuestionTypeConfig,
        *,
        difficulty_target: str,
        type_slots: dict[str, Any],
        business_subtype: str | None = None,
        pattern_id: str | None = None,
    ) -> dict[str, Any]:
        warnings: list[str] = []
        subtype_config = self._get_business_subtype(question_type_config, business_subtype)
        resolved_slots = self._resolve_slots(question_type_config, subtype_config, type_slots)
        resolved_slots = self._apply_difficulty_slot_profile(
            question_type_config=question_type_config,
            resolved_slots=resolved_slots,
            incoming_slots=type_slots,
            difficulty_target=difficulty_target,
        )
        pattern, selection_reason = self._select_pattern(
            question_type_config=question_type_config,
            subtype_config=subtype_config,
            resolved_slots=resolved_slots,
            pattern_id=pattern_id,
            warnings=warnings,
        )
        difficulty_projection = self._project_difficulty(
            pattern=pattern,
            resolved_slots=resolved_slots,
            difficulty_target=difficulty_target,
        )
        difficulty_target_profile = self._get_difficulty_target_profile(
            question_type_config=question_type_config,
            difficulty_target=difficulty_target,
        )
        difficulty_fit = self._build_difficulty_fit(
            difficulty_projection=difficulty_projection,
            difficulty_target_profile=difficulty_target_profile,
        )
        skeleton = self._build_skeleton(
            question_type_config=question_type_config,
            pattern=pattern,
            difficulty_target=difficulty_target,
            difficulty_projection=difficulty_projection,
            resolved_slots=resolved_slots,
        )
        return {
            "question_type": question_type_config.type_id,
            "business_subtype": subtype_config.subtype_id if subtype_config else None,
            "selected_pattern": pattern.pattern_id,
            "resolved_slots": resolved_slots,
            "skeleton": skeleton,
            "difficulty_projection": difficulty_projection,
            "difficulty_target_profile": difficulty_target_profile,
            "difficulty_fit": difficulty_fit,
            "control_logic": pattern.control_logic.model_dump(),
            "generation_logic": pattern.generation_logic.model_dump(),
            "pattern_selection_reason": selection_reason,
            "warnings": warnings,
        }

    def _get_business_subtype(
        self,
        question_type_config: QuestionTypeConfig,
        business_subtype: str | None,
    ) -> BusinessSubtypeConfig | None:
        if not business_subtype:
            return None
        for subtype in question_type_config.business_subtypes:
            if subtype.subtype_id == business_subtype:
                return subtype
        raise DomainError(
            "Unknown business_subtype for this question_type.",
            status_code=404,
            details={"question_type": question_type_config.type_id, "business_subtype": business_subtype},
        )

    def _resolve_slots(
        self,
        question_type_config: QuestionTypeConfig,
        subtype_config: BusinessSubtypeConfig | None,
        incoming_slots: dict[str, Any],
    ) -> dict[str, Any]:
        unknown_slots = sorted(set(incoming_slots.keys()) - set(question_type_config.slot_schema.keys()))
        if unknown_slots:
            raise DomainError(
                "type_slots contain unsupported keys.",
                status_code=422,
                details={"unknown_slots": unknown_slots},
            )

        resolved = dict(question_type_config.default_slots)
        if subtype_config:
            resolved.update(subtype_config.default_slot_overrides)

        for slot_name, slot_config in question_type_config.slot_schema.items():
            if slot_name not in resolved and slot_config.default is not None:
                resolved[slot_name] = slot_config.default

        resolved.update(incoming_slots)

        errors: dict[str, str] = {}
        for slot_name, slot_config in question_type_config.slot_schema.items():
            value = resolved.get(slot_name)
            if value is None:
                if slot_config.required:
                    errors[slot_name] = "required slot is missing"
                continue

            try:
                resolved[slot_name] = self._coerce_value(slot_name, value, slot_config)
            except DomainError as exc:
                errors[slot_name] = exc.message
                continue

            if slot_config.allowed and not self._is_allowed_value(resolved[slot_name], slot_config.allowed):
                errors[slot_name] = f"value must be one of {slot_config.allowed}"

        if errors:
            raise DomainError(
                "type_slots validation failed.",
                status_code=422,
                details={"slot_errors": errors},
            )

        return resolved

    def _apply_difficulty_slot_profile(
        self,
        *,
        question_type_config: QuestionTypeConfig,
        resolved_slots: dict[str, Any],
        incoming_slots: dict[str, Any],
        difficulty_target: str,
    ) -> dict[str, Any]:
        return dict(resolved_slots)


    def _coerce_value(self, slot_name: str, value: Any, slot_config: SlotFieldConfig) -> Any:
        expected_type = slot_config.type
        caster = self.TYPE_CASTERS[expected_type]

        if expected_type == "boolean":
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                lowered = value.lower()
                if lowered in {"true", "1", "yes"}:
                    return True
                if lowered in {"false", "0", "no"}:
                    return False
            raise DomainError(f"slot '{slot_name}' must be a boolean")

        if expected_type == "array":
            if not isinstance(value, list):
                raise DomainError(f"slot '{slot_name}' must be an array")
            return value

        if expected_type == "object":
            if not isinstance(value, dict):
                raise DomainError(f"slot '{slot_name}' must be an object")
            return value

        try:
            return caster(value)
        except (TypeError, ValueError) as exc:
            raise DomainError(f"slot '{slot_name}' must be a valid {expected_type}") from exc

    def _is_allowed_value(self, value: Any, allowed: list[Any]) -> bool:
        if isinstance(value, list):
            return all(item in allowed for item in value)
        return value in allowed

    def _select_pattern(
        self,
        *,
        question_type_config: QuestionTypeConfig,
        subtype_config: BusinessSubtypeConfig | None,
        resolved_slots: dict[str, Any],
        pattern_id: str | None,
        warnings: list[str],
    ) -> tuple[PatternConfig, PatternSelectionReason]:
        enabled_patterns = [pattern for pattern in question_type_config.patterns if pattern.enabled]
        default_pattern = self._get_default_pattern(question_type_config, enabled_patterns)

        if pattern_id:
            for pattern in enabled_patterns:
                if pattern.pattern_id == pattern_id:
                    score_info = self._compute_pattern_score(pattern.match_rules, resolved_slots)
                    return pattern, PatternSelectionReason(
                        requested_pattern_id=pattern_id,
                        selected_pattern_id=pattern.pattern_id,
                        selection_mode="direct",
                        matched_fields=score_info["matched_fields"],
                        score=score_info["score"],
                        fallback_used=False,
                        fallback_reason=None,
                    )
            raise DomainError(
                "Requested pattern_id is not available for this question_type.",
                status_code=404,
                details={"pattern_id": pattern_id, "question_type": question_type_config.type_id},
            )

        preferred_patterns = self._get_preferred_patterns(enabled_patterns, subtype_config)
        if preferred_patterns:
            preferred_result = self._pick_best_pattern(
                candidate_patterns=preferred_patterns,
                resolved_slots=resolved_slots,
                preferred_subtype_id=subtype_config.subtype_id if subtype_config else None,
            )
            if self._is_clear_match(preferred_result["top_score"], preferred_result["second_score"]):
                reason = preferred_result["reason"]
                return preferred_result["pattern"], PatternSelectionReason(
                    requested_pattern_id=None,
                    selected_pattern_id=preferred_result["pattern"].pattern_id,
                    selection_mode="auto_match",
                    matched_fields=reason["matched_fields"],
                    score=reason["score"],
                    fallback_used=False,
                    fallback_reason=None,
                )

        overall_result = self._pick_best_pattern(
            candidate_patterns=enabled_patterns,
            resolved_slots=resolved_slots,
            preferred_subtype_id=None,
        )
        if self._is_clear_match(overall_result["top_score"], overall_result["second_score"]):
            reason = overall_result["reason"]
            return overall_result["pattern"], PatternSelectionReason(
                requested_pattern_id=None,
                selected_pattern_id=overall_result["pattern"].pattern_id,
                selection_mode="auto_match",
                matched_fields=reason["matched_fields"],
                score=reason["score"],
                fallback_used=False,
                fallback_reason=None,
            )

        fallback_reason = "No clear best pattern from match_rules."
        if subtype_config and subtype_config.preferred_patterns:
            fallback_reason = (
                f"No clear best pattern inside business_subtype preferred_patterns for '{subtype_config.subtype_id}', "
                "and global match also had no clear winner."
            )
        if default_pattern is None:
            raise DomainError(
                "Pattern resolution is ambiguous without an explicit pattern_id or configured default_pattern_id.",
                status_code=422,
                details={
                    "question_type": question_type_config.type_id,
                    "business_subtype": subtype_config.subtype_id if subtype_config else None,
                    "candidate_scores": {
                        "top_pattern_id": overall_result["pattern"].pattern_id,
                        "top_score": overall_result["top_score"],
                        "second_score": overall_result["second_score"],
                    },
                },
            )
        warnings.append(
            f"Pattern selection used configured default_pattern_id '{default_pattern.pattern_id}' because match_rules produced no unique winner."
        )
        return default_pattern, PatternSelectionReason(
            requested_pattern_id=None,
            selected_pattern_id=default_pattern.pattern_id,
            selection_mode="configured_default",
            matched_fields=overall_result["reason"]["matched_fields"],
            score=overall_result["reason"]["score"],
            fallback_used=True,
            fallback_reason=fallback_reason,
        )

    def _get_preferred_patterns(
        self,
        enabled_patterns: list[PatternConfig],
        subtype_config: BusinessSubtypeConfig | None,
    ) -> list[PatternConfig]:
        if not subtype_config or not subtype_config.preferred_patterns:
            return []
        preferred_ids = set(subtype_config.preferred_patterns)
        return [pattern for pattern in enabled_patterns if pattern.pattern_id in preferred_ids]

    def _pick_best_pattern(
        self,
        *,
        candidate_patterns: list[PatternConfig],
        resolved_slots: dict[str, Any],
        preferred_subtype_id: str | None,
    ) -> dict[str, Any]:
        scored_patterns: list[tuple[dict[str, Any], PatternConfig]] = []
        for pattern in candidate_patterns:
            score_info = self._compute_pattern_score(pattern.match_rules, resolved_slots)
            if preferred_subtype_id:
                score_info["matched_fields"] = [f"business_subtype={preferred_subtype_id}", *score_info["matched_fields"]]
            scored_patterns.append((score_info, pattern))

        scored_patterns.sort(key=lambda item: item[0]["score"], reverse=True)
        top_reason, top_pattern = scored_patterns[0]
        second_score = scored_patterns[1][0]["score"] if len(scored_patterns) > 1 else -1.0
        return {
            "pattern": top_pattern,
            "reason": top_reason,
            "top_score": top_reason["score"],
            "second_score": second_score,
        }

    def _compute_pattern_score(self, match_rules: dict[str, Any], resolved_slots: dict[str, Any]) -> dict[str, Any]:
        if "required_slots" in match_rules or "preferred_slots" in match_rules:
            return self._compute_legacy_score(match_rules, resolved_slots)

        matched_fields: list[str] = []
        score = 0.0
        for slot_name, expected in match_rules.items():
            actual = resolved_slots.get(slot_name)
            if self._match_rule_value(actual, expected):
                score += 1.0
                matched_fields.append(f"{slot_name}={actual}")

        return {"score": score, "matched_fields": matched_fields}

    def _compute_legacy_score(self, match_rules: dict[str, Any], resolved_slots: dict[str, Any]) -> dict[str, Any]:
        required_slots = match_rules.get("required_slots", [])
        preferred_slots = match_rules.get("preferred_slots", {})
        matched_fields: list[str] = []

        for slot_name in required_slots:
            if resolved_slots.get(slot_name) in (None, ""):
                return {"score": -1.0, "matched_fields": []}
            matched_fields.append(f"{slot_name}=present")

        score = 0.0
        for slot_name, expected in preferred_slots.items():
            actual = resolved_slots.get(slot_name)
            if self._match_rule_value(actual, expected):
                score += 1.0
                matched_fields.append(f"{slot_name}={actual}")

        return {"score": score, "matched_fields": matched_fields}

    def _match_rule_value(self, actual: Any, expected: Any) -> bool:
        if isinstance(expected, list):
            if isinstance(actual, list):
                return any(item in expected for item in actual)
            return actual in expected
        return actual == expected

    def _is_clear_match(self, top_score: float, second_score: float) -> bool:
        return top_score > 0 and top_score != second_score

    def _get_default_pattern(
        self,
        question_type_config: QuestionTypeConfig,
        enabled_patterns: list[PatternConfig],
    ) -> PatternConfig | None:
        if question_type_config.default_pattern_id:
            for pattern in enabled_patterns:
                if pattern.pattern_id == question_type_config.default_pattern_id:
                    return pattern
            raise DomainError(
                "Configured default_pattern_id is invalid or disabled.",
                status_code=500,
                details={
                    "question_type": question_type_config.type_id,
                    "default_pattern_id": question_type_config.default_pattern_id,
                },
            )

        return None

    def _project_difficulty(
        self,
        *,
        pattern: PatternConfig,
        resolved_slots: dict[str, Any],
        difficulty_target: str,
    ) -> DifficultyProjection:
        text_lookup = self._build_text_lookup(pattern=pattern, resolved_slots=resolved_slots)
        projection: dict[str, float] = {}

        for metric_name in ("complexity", "ambiguity", "reasoning_depth", "distractor_similarity"):
            metric_rule = getattr(pattern.difficulty_rules, metric_name)
            score = metric_rule.base if metric_rule.base is not None else 0.3

            for slot_name, mapping in metric_rule.by_slot.items():
                slot_value = resolved_slots.get(slot_name)
                if slot_value is not None and str(slot_value) in mapping:
                    score = mapping[str(slot_value)]

            for text_key, mapping in metric_rule.by_text.items():
                text_value = text_lookup.get(text_key)
                if text_value is not None and str(text_value) in mapping:
                    score = mapping[str(text_value)]

            projection[metric_name] = self._clamp(score)

        return DifficultyProjection(**projection)

    def _get_difficulty_target_profile(
        self,
        *,
        question_type_config: QuestionTypeConfig,
        difficulty_target: str,
    ) -> DifficultyTargetProfile:
        profile = question_type_config.difficulty_target_profiles[difficulty_target]
        return DifficultyTargetProfile.model_validate(profile.model_dump())

    def _build_difficulty_fit(
        self,
        *,
        difficulty_projection: DifficultyProjection,
        difficulty_target_profile: DifficultyTargetProfile,
    ) -> DifficultyFit:
        deviations: list[DifficultyDeviation] = []
        for metric_name in ("complexity", "ambiguity", "reasoning_depth", "distractor_similarity"):
            actual = getattr(difficulty_projection, metric_name)
            target_range = getattr(difficulty_target_profile, metric_name)
            if actual < target_range.min or actual > target_range.max:
                deviations.append(
                    DifficultyDeviation(
                        metric=metric_name,
                        target_min=target_range.min,
                        target_max=target_range.max,
                        actual=actual,
                    )
                )

        return DifficultyFit(in_range=not deviations, deviations=deviations)

    def _build_text_lookup(self, *, pattern: PatternConfig, resolved_slots: dict[str, Any]) -> dict[str, Any]:
        lookup = dict(resolved_slots)
        lookup.update(pattern.control_logic.model_dump())
        lookup.update(pattern.generation_logic.model_dump())
        return lookup

    def _build_skeleton(
        self,
        *,
        question_type_config: QuestionTypeConfig,
        pattern: PatternConfig,
        difficulty_target: str,
        difficulty_projection: DifficultyProjection,
        resolved_slots: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            "question_type": question_type_config.type_id,
            "pattern_id": pattern.pattern_id,
            "difficulty_target": difficulty_target,
            "anchor_type": question_type_config.skeleton.get("anchor_type", "generic"),
            "operation_type": question_type_config.skeleton.get("operation_type", "generic"),
            "target_type": question_type_config.skeleton.get("target_type", "generic"),
            "complexity": difficulty_projection.complexity,
            "ambiguity": difficulty_projection.ambiguity,
            "reasoning_depth": difficulty_projection.reasoning_depth,
            "distractor_similarity": difficulty_projection.distractor_similarity,
        }

    def _clamp(self, value: float) -> float:
        return round(max(0.0, min(1.0, value)), 2)
