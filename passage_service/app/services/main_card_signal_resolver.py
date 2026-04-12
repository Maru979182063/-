from __future__ import annotations

import json
from typing import Any

from app.core.config import get_config_bundle
from app.infra.llm.base import BaseLLMProvider
from app.services.llm_runtime import get_llm_provider
from app.services.sentence_fill_protocol import (
    normalize_sentence_fill_blank_position,
    normalize_sentence_fill_function_type,
    normalize_sentence_fill_logic_relation,
)


class _SafeDict(dict):
    def __missing__(self, key: str) -> str:
        return "{" + key + "}"


class MainCardSignalResolver:
    SUPPORTED_FAMILIES = {"center_understanding", "sentence_fill", "sentence_order"}
    FAMILY_ALIASES = {
        "title_selection": "center_understanding",
    }

    def __init__(
        self,
        *,
        provider: BaseLLMProvider | None = None,
        llm_config: dict[str, Any] | None = None,
    ) -> None:
        self.provider = provider or get_llm_provider()
        self.llm_config = llm_config or get_config_bundle().llm
        self.config = dict(self.llm_config.get("main_card_signal_resolver") or {})

    def is_enabled_for_family(self, business_family_id: str) -> bool:
        normalized = self._normalize_family_id(business_family_id)
        if normalized not in self.SUPPORTED_FAMILIES:
            return False
        if not bool(self.config.get("enabled")):
            return False
        if not self.provider.is_enabled():
            return False
        families = dict(self.config.get("families") or {})
        return bool(families.get(normalized))

    def resolve(
        self,
        *,
        business_family_id: str,
        article_context: dict[str, Any],
        candidate: dict[str, Any],
        neutral_signal_profile: dict[str, Any],
        business_feature_profile: dict[str, Any],
    ) -> dict[str, Any] | None:
        normalized_family_id = self._normalize_family_id(business_family_id)
        if not self.is_enabled_for_family(business_family_id):
            return None
        config = dict((self.config.get("families") or {}).get(normalized_family_id) or {})
        judge_specs = self._judge_specs()
        if not judge_specs:
            return None

        prompt_context = self._build_prompt_context(
            business_family_id=normalized_family_id,
            runtime_business_family_id=business_family_id,
            article_context=article_context,
            candidate=candidate,
            neutral_signal_profile=neutral_signal_profile,
            business_feature_profile=business_feature_profile,
            family_config=config,
        )
        instructions = self._render_template(
            str(self.config.get("common_instructions") or "").strip(),
            prompt_context,
        )
        user_prompt = self._render_template(
            str(self.config.get("user_prompt_template") or "").strip(),
            prompt_context,
        )
        schema = self._response_schema(normalized_family_id)
        judge_results: list[dict[str, Any]] = []
        for judge_name, model in judge_specs:
            try:
                result = self.provider.generate_json(
                    model=model,
                    instructions=instructions,
                    input_payload={
                        "prompt": user_prompt,
                        "schema_name": f"{normalized_family_id}_{judge_name}_signal_resolution",
                        "schema": schema,
                    },
                )
                normalized = self._normalize_result(
                    business_family_id=normalized_family_id,
                    judge_name=judge_name,
                    model=model,
                    payload=result,
                )
            except Exception as exc:
                normalized = {
                    "judge_name": judge_name,
                    "model": model,
                    "status": "error",
                    "reason": f"llm_error:{type(exc).__name__}",
                    "neutral_signal_overrides": {},
                    "business_feature_profile_overrides": {},
                }
            judge_results.append(normalized)
        return {
            "enabled": True,
            "mode": str(self.config.get("mode") or "enforce"),
            "business_family_id": normalized_family_id,
            "runtime_business_family_id": business_family_id,
            "judge_results": judge_results,
            "consensus": self._build_consensus(normalized_family_id, judge_results),
        }

    def _judge_specs(self) -> list[tuple[str, str]]:
        models = dict(self.config.get("models") or {})
        judge_model_a = str(models.get("judge_a") or "").strip()
        judge_model_b = str(models.get("judge_b") or "").strip()
        if not judge_model_a:
            return []
        judge_count = self._expected_judge_count()
        if judge_count <= 1:
            return [("judge_a", judge_model_a)]
        return [
            ("judge_a", judge_model_a),
            ("judge_b", judge_model_b or judge_model_a),
        ]

    def _expected_judge_count(self) -> int:
        return max(1, int(self.config.get("judge_count") or 2))

    def consensus_allows_override(self, resolution: dict[str, Any] | None) -> bool:
        consensus = dict((resolution or {}).get("consensus") or {})
        allowed_statuses = {"unanimous"}
        if self._expected_judge_count() <= 1:
            allowed_statuses.add("single")
        return str(consensus.get("status") or "") in allowed_statuses

    def _normalize_family_id(self, business_family_id: str) -> str:
        return str(self.FAMILY_ALIASES.get(business_family_id, business_family_id))

    def _build_prompt_context(
        self,
        *,
        business_family_id: str,
        runtime_business_family_id: str,
        article_context: dict[str, Any],
        candidate: dict[str, Any],
        neutral_signal_profile: dict[str, Any],
        business_feature_profile: dict[str, Any],
        family_config: dict[str, Any],
    ) -> dict[str, str]:
        return {
            "business_family_id": business_family_id,
            "runtime_business_family_id": runtime_business_family_id,
            "family_label": str(family_config.get("label") or business_family_id),
            "goal": str(family_config.get("goal") or ""),
            "signal_goal": str(family_config.get("signal_goal") or ""),
            "article_title": str(article_context.get("title") or ""),
            "article_source_json": self._json_dump(article_context.get("source") or {}),
            "candidate_type": str(candidate.get("candidate_type") or ""),
            "candidate_text": str(candidate.get("text") or ""),
            "candidate_meta_json": self._json_dump(candidate.get("meta") or {}),
            "mechanical_neutral_signal_profile_json": self._json_dump(neutral_signal_profile),
            "mechanical_business_feature_profile_json": self._json_dump(business_feature_profile),
        }

    def _normalize_result(
        self,
        *,
        business_family_id: str,
        judge_name: str,
        model: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        allowed_neutral = set(self._family_neutral_keys(business_family_id))
        neutral = {
            key: payload.get(key)
            for key in allowed_neutral
            if key in payload and payload.get(key) is not None
        }
        business_overrides = self._extract_business_profile_overrides(
            business_family_id=business_family_id,
            payload=payload,
        )
        return {
            "judge_name": judge_name,
            "model": model,
            "status": "ok",
            "reason": str(payload.get("reason") or ""),
            "neutral_signal_overrides": neutral,
            "business_feature_profile_overrides": business_overrides,
        }

    def _extract_business_profile_overrides(
        self,
        *,
        business_family_id: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        if business_family_id == "center_understanding":
            return {
                "feature_type": payload.get("feature_type"),
                "material_structure_label": payload.get("material_structure_label"),
                "conclusion_focus": bool(payload.get("conclusion_focus")),
            }
        if business_family_id == "sentence_fill":
            return {
                "sentence_fill_profile": {
                    **(
                        {"blank_position": normalize_sentence_fill_blank_position(payload.get("blank_position"))}
                        if payload.get("blank_position") is not None
                        else {}
                    ),
                    **(
                        {"function_type": normalize_sentence_fill_function_type(payload.get("function_type"))}
                        if payload.get("function_type") is not None
                        else {}
                    ),
                    **(
                        {"logic_relation": normalize_sentence_fill_logic_relation(payload.get("logic_relation"))}
                        if payload.get("logic_relation") is not None
                        else {}
                    ),
                    **(
                        {"explicit_slot_ready": payload.get("slot_explicit_ready")}
                        if payload.get("slot_explicit_ready") is not None
                        else {}
                    ),
                    **{
                        key: payload.get(key)
                        for key in (
                            "backward_link_strength",
                            "forward_link_strength",
                            "bidirectional_validation",
                            "reference_dependency",
                            "countermeasure_signal_strength",
                        )
                        if key in payload and payload.get(key) is not None
                    },
                }
            }
        if business_family_id == "sentence_order":
            return {
                "sentence_order_profile": {
                    key: payload.get(key)
                    for key in (
                        "unit_count",
                        "opening_rule",
                        "closing_rule",
                        "binding_rules",
                        "logic_modes",
                        "opening_signal_strength",
                        "closing_signal_strength",
                        "local_binding_strength",
                        "sequence_integrity",
                        "unique_opener_score",
                        "binding_pair_count",
                        "exchange_risk",
                        "function_overlap_score",
                        "multi_path_risk",
                        "discourse_progression_strength",
                        "context_closure_score",
                        "temporal_order_strength",
                        "action_sequence_irreversibility",
                    )
                    if key in payload and payload.get(key) is not None
                }
            }
        return {}

    def _build_consensus(
        self,
        business_family_id: str,
        judge_results: list[dict[str, Any]],
    ) -> dict[str, Any]:
        ok_results = [item for item in judge_results if item.get("status") == "ok"]
        if not ok_results:
            return {
                "status": "insufficient_votes",
                "neutral_signal_overrides": {},
                "business_feature_profile_overrides": {},
            }
        if self._expected_judge_count() <= 1 and len(ok_results) == 1:
            return {
                "status": "single",
                "neutral_signal_overrides": dict(ok_results[0].get("neutral_signal_overrides") or {}),
                "business_feature_profile_overrides": dict(ok_results[0].get("business_feature_profile_overrides") or {}),
            }
        if len(ok_results) < self._expected_judge_count():
            return {
                "status": "insufficient_votes",
                "neutral_signal_overrides": {},
                "business_feature_profile_overrides": {},
            }
        if not self._key_fields_align(business_family_id, ok_results):
            return {
                "status": "split_vote",
                "neutral_signal_overrides": {},
                "business_feature_profile_overrides": {},
            }
        merged_neutral = self._merge_value_dicts([item.get("neutral_signal_overrides") or {} for item in ok_results])
        merged_business = self._merge_business_profile_dicts(
            [item.get("business_feature_profile_overrides") or {} for item in ok_results]
        )
        return {
            "status": "unanimous",
            "neutral_signal_overrides": merged_neutral,
            "business_feature_profile_overrides": merged_business,
        }

    def _key_fields_align(self, business_family_id: str, ok_results: list[dict[str, Any]]) -> bool:
        key_fields = list(self._family_key_fields(business_family_id))
        for field in key_fields:
            values = []
            for result in ok_results:
                value = (result.get("neutral_signal_overrides") or {}).get(field)
                if value in (None, "", []):
                    continue
                values.append(json.dumps(value, ensure_ascii=False, sort_keys=True) if isinstance(value, (list, dict)) else str(value))
            if len(set(values)) > 1:
                return False
        return True

    def _merge_value_dicts(self, payloads: list[dict[str, Any]]) -> dict[str, Any]:
        merged: dict[str, Any] = {}
        keys = {key for payload in payloads for key in payload.keys()}
        for key in keys:
            values = [payload.get(key) for payload in payloads if key in payload and payload.get(key) is not None]
            if not values:
                continue
            sample = values[0]
            if isinstance(sample, bool):
                merged[key] = all(bool(value) for value in values)
                continue
            if isinstance(sample, (int, float)) and not isinstance(sample, bool):
                merged[key] = round(sum(float(value) for value in values) / len(values), 4)
                continue
            if isinstance(sample, list):
                normalized = [set(str(item) for item in value) for value in values if isinstance(value, list)]
                if normalized:
                    intersection = set.intersection(*normalized) if len(normalized) > 1 else normalized[0]
                    merged[key] = sorted(intersection)
                continue
            merged[key] = str(sample)
        return merged

    def _merge_business_profile_dicts(self, payloads: list[dict[str, Any]]) -> dict[str, Any]:
        merged: dict[str, Any] = {}
        for payload in payloads:
            for key, value in payload.items():
                if isinstance(value, dict):
                    existing = dict(merged.get(key) or {})
                    existing.update(self._merge_value_dicts([existing, value]))
                    merged[key] = existing
                elif value is not None:
                    merged[key] = value
        return merged

    def _family_key_fields(self, business_family_id: str) -> tuple[str, ...]:
        if business_family_id == "center_understanding":
            return ()
        if business_family_id == "sentence_fill":
            return ("blank_position", "function_type", "logic_relation")
        if business_family_id == "sentence_order":
            return ("opening_rule", "closing_rule")
        return ()

    def _family_neutral_keys(self, business_family_id: str) -> tuple[str, ...]:
        if business_family_id == "center_understanding":
            return (
                "single_center_strength",
                "summary_strength",
                "closure_score",
                "titleability",
                "topic_consistency_strength",
                "analysis_to_conclusion_strength",
                "non_key_detail_density",
                "example_to_theme_strength",
                "branch_focus_strength",
                "turning_focus_strength",
                "value_judgement_strength",
                "core_object_anchor_strength",
                "object_scope_stability",
                "material_structure_label",
            )
        if business_family_id == "sentence_fill":
            return (
                "blank_position",
                "function_type",
                "bidirectional_validation",
                "backward_link_strength",
                "forward_link_strength",
                "summary_need_strength",
                "countermeasure_signal_strength",
                "reference_dependency",
                "slot_explicit_ready",
                "logic_relation",
            )
        if business_family_id == "sentence_order":
            return (
                "opening_anchor_type",
                "opening_signal_strength",
                "local_binding_strength",
                "closing_anchor_type",
                "closing_signal_strength",
                "unique_opener_score",
                "binding_pair_count",
                "exchange_risk",
                "function_overlap_score",
                "multi_path_risk",
                "discourse_progression_strength",
                "context_closure_score",
                "temporal_order_strength",
                "action_sequence_irreversibility",
                "sequence_integrity",
                "opening_rule",
                "closing_rule",
                "binding_rules",
                "logic_modes",
            )
        return ()

    def _response_schema(self, business_family_id: str) -> dict[str, Any]:
        if business_family_id == "center_understanding":
            return {
                "type": "object",
                "properties": {
                    "single_center_strength": {"type": "number"},
                    "summary_strength": {"type": "number"},
                    "closure_score": {"type": "number"},
                    "titleability": {"type": "number"},
                    "topic_consistency_strength": {"type": "number"},
                    "analysis_to_conclusion_strength": {"type": "number"},
                    "non_key_detail_density": {"type": "number"},
                    "example_to_theme_strength": {"type": "number"},
                    "branch_focus_strength": {"type": "number"},
                    "turning_focus_strength": {"type": "number"},
                    "value_judgement_strength": {"type": "number"},
                    "core_object_anchor_strength": {"type": "number"},
                    "object_scope_stability": {"type": "number"},
                    "material_structure_label": {"type": "string"},
                    "feature_type": {"type": "string"},
                    "conclusion_focus": {"type": "boolean"},
                    "reason": {"type": "string"},
                },
                "required": [
                    "single_center_strength",
                    "summary_strength",
                    "closure_score",
                    "titleability",
                    "topic_consistency_strength",
                    "analysis_to_conclusion_strength",
                    "non_key_detail_density",
                    "example_to_theme_strength",
                    "branch_focus_strength",
                    "turning_focus_strength",
                    "value_judgement_strength",
                    "core_object_anchor_strength",
                    "object_scope_stability",
                    "material_structure_label",
                    "feature_type",
                    "conclusion_focus",
                    "reason",
                ],
                "additionalProperties": False,
            }
        if business_family_id == "sentence_fill":
            return {
                "type": "object",
                "properties": {
                    "blank_position": {"type": "string", "enum": ["opening", "middle", "ending", "inserted", "mixed"]},
                    "function_type": {"type": "string", "enum": ["summary", "topic_intro", "carry_previous", "lead_next", "bridge", "reference_summary", "countermeasure", "conclusion"]},
                    "logic_relation": {"type": "string", "enum": ["continuation", "transition", "explanation", "focus_shift", "summary", "action", "elevation", "reference_match", "multi_constraint"]},
                    "bidirectional_validation": {"type": "number"},
                    "backward_link_strength": {"type": "number"},
                    "forward_link_strength": {"type": "number"},
                    "summary_need_strength": {"type": "number"},
                    "countermeasure_signal_strength": {"type": "number"},
                    "reference_dependency": {"type": "number"},
                    "slot_explicit_ready": {"type": "boolean"},
                    "reason": {"type": "string"},
                },
                "required": [
                    "blank_position",
                    "function_type",
                    "logic_relation",
                    "bidirectional_validation",
                    "backward_link_strength",
                    "forward_link_strength",
                    "summary_need_strength",
                    "countermeasure_signal_strength",
                    "reference_dependency",
                    "slot_explicit_ready",
                    "reason",
                ],
                "additionalProperties": False,
            }
        return {
            "type": "object",
            "properties": {
                "opening_anchor_type": {"type": "string", "enum": ["definition_opening", "viewpoint_opening", "problem_opening", "background_opening", "weak_opening"]},
                "opening_rule": {"type": "string", "enum": ["weak_opening", "definition_opening", "explicit_opening"]},
                "opening_signal_strength": {"type": "number"},
                "closing_anchor_type": {"type": "string", "enum": ["summary", "conclusion", "countermeasure", "none"]},
                "closing_rule": {"type": "string", "enum": ["none", "summary_or_conclusion", "countermeasure"]},
                "closing_signal_strength": {"type": "number"},
                "local_binding_strength": {"type": "number"},
                "unique_opener_score": {"type": "number"},
                "binding_pair_count": {"type": "number"},
                "exchange_risk": {"type": "number"},
                "function_overlap_score": {"type": "number"},
                "multi_path_risk": {"type": "number"},
                "discourse_progression_strength": {"type": "number"},
                "context_closure_score": {"type": "number"},
                "temporal_order_strength": {"type": "number"},
                "action_sequence_irreversibility": {"type": "number"},
                "sequence_integrity": {"type": "number"},
                "unit_count": {"type": "integer"},
                "binding_rules": {"type": "array", "items": {"type": "string", "enum": ["pronoun_reference", "turning_connector", "parallel_connector"]}},
                "logic_modes": {"type": "array", "items": {"type": "string", "enum": ["timeline_sequence", "action_sequence", "discourse_logic", "viewpoint_explanation", "problem_solution", "question_answer", "deterministic_binding"]}},
                "reason": {"type": "string"},
            },
            "required": [
                "opening_anchor_type",
                "opening_rule",
                "opening_signal_strength",
                "closing_anchor_type",
                "closing_rule",
                "closing_signal_strength",
                "local_binding_strength",
                "unique_opener_score",
                "binding_pair_count",
                "exchange_risk",
                "function_overlap_score",
                "multi_path_risk",
                "discourse_progression_strength",
                "context_closure_score",
                "temporal_order_strength",
                "action_sequence_irreversibility",
                "sequence_integrity",
                "unit_count",
                "binding_rules",
                "logic_modes",
                "reason",
            ],
            "additionalProperties": False,
        }

    def _render_template(self, template: str, values: dict[str, str]) -> str:
        return template.format_map(_SafeDict(values))

    def _json_dump(self, payload: Any) -> str:
        return json.dumps(payload, ensure_ascii=False, sort_keys=True)
