from __future__ import annotations

import json
from typing import Any

from app.core.config import get_config_bundle
from app.infra.llm.base import BaseLLMProvider
from app.services.llm_runtime import get_llm_provider


class _SafeDict(dict):
    def __missing__(self, key: str) -> str:
        return "{" + key + "}"


class MainCardDualJudge:
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
        self.config = dict(self.llm_config.get("main_card_dual_judge") or {})

    def is_enabled_for_family(self, business_family_id: str) -> bool:
        normalized_family_id = self._normalize_family_id(business_family_id)
        if normalized_family_id not in self.SUPPORTED_FAMILIES:
            return False
        if not bool(self.config.get("enabled")):
            return False
        if not self.provider.is_enabled():
            return False
        families = dict(self.config.get("families") or {})
        return bool(families.get(normalized_family_id))

    def adjudicate(
        self,
        *,
        business_family_id: str,
        item: dict[str, Any],
        question_card: dict[str, Any],
    ) -> dict[str, Any] | None:
        normalized_family_id = self._normalize_family_id(business_family_id)
        if not self.is_enabled_for_family(business_family_id):
            return None

        family_config = dict((self.config.get("families") or {}).get(normalized_family_id) or {})
        judge_specs = self._judge_specs()
        if not judge_specs:
            return None

        prompt_context = self._build_prompt_context(
            business_family_id=normalized_family_id,
            runtime_business_family_id=business_family_id,
            item=item,
            question_card=question_card,
            family_config=family_config,
        )
        instructions = self._render_template(
            str(self.config.get("common_instructions") or "").strip(),
            prompt_context,
        )
        user_prompt = self._render_template(
            str(self.config.get("user_prompt_template") or "").strip(),
            prompt_context,
        )
        schema = self._response_schema()

        judge_results: list[dict[str, Any]] = []
        for judge_name, model in judge_specs:
            try:
                result = self.provider.generate_json(
                    model=model,
                    instructions=instructions,
                    input_payload={
                        "prompt": user_prompt,
                        "schema_name": f"{normalized_family_id}_{judge_name}_adjudication",
                        "schema": schema,
                    },
                )
                normalized = self._normalize_result(
                    judge_name=judge_name,
                    model=model,
                    payload=result,
                )
            except Exception as exc:
                normalized = {
                    "judge_name": judge_name,
                    "model": model,
                    "status": "error",
                    "decision": "reject",
                    "formal_layer": "reject",
                    "selected_material_card": None,
                    "selected_business_card": None,
                    "reason": f"llm_error:{type(exc).__name__}",
                    "evidence_summary": "",
                    "confidence": 0.0,
                }
            judge_results.append(normalized)

        return {
            "enabled": True,
            "mode": str(self.config.get("mode") or "shadow"),
            "consensus_rule": str(self.config.get("consensus_rule") or "unanimous"),
            "business_family_id": normalized_family_id,
            "runtime_business_family_id": business_family_id,
            "question_card_id": question_card.get("card_id"),
            "judge_results": judge_results,
            "consensus": self._build_consensus(judge_results),
        }

    def is_enforce_mode(self) -> bool:
        return bool(self.config.get("enabled")) and str(self.config.get("mode") or "shadow") == "enforce"

    def use_full_card_catalog(self) -> bool:
        return bool(self.config.get("use_full_card_catalog", False))

    def consensus_allows_accept(self, adjudication: dict[str, Any] | None) -> bool:
        if not adjudication:
            return False
        consensus = dict(adjudication.get("consensus") or {})
        allowed_statuses = {"unanimous"}
        if self._expected_judge_count() <= 1:
            allowed_statuses.add("single")
        if str(consensus.get("status") or "") not in allowed_statuses:
            return False
        return str(consensus.get("decision") or "") in {"accept", "borderline"}

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

    def _build_prompt_context(
        self,
        *,
        business_family_id: str,
        runtime_business_family_id: str,
        item: dict[str, Any],
        question_card: dict[str, Any],
        family_config: dict[str, Any],
    ) -> dict[str, str]:
        candidate_meta = dict(item.get("meta") or {})
        question_ready = dict(item.get("question_ready_context") or {})
        context = {
            "business_family_id": business_family_id,
            "runtime_business_family_id": runtime_business_family_id,
            "family_label": str(family_config.get("label") or business_family_id),
            "question_card_id": str(question_card.get("card_id") or ""),
            "family_goal": str(family_config.get("goal") or ""),
            "formal_unit_definition": str(family_config.get("formal_unit_definition") or ""),
            "strong_accept_definition": str(family_config.get("strong_accept_definition") or ""),
            "weak_accept_definition": str(family_config.get("weak_accept_definition") or ""),
            "reject_definition": str(family_config.get("reject_definition") or ""),
            "candidate_type": str(item.get("candidate_type") or ""),
            "candidate_text": str(item.get("text") or ""),
            "article_title": str(item.get("article_title") or ""),
            "selected_material_card": str(item.get("material_card_id") or ""),
            "selected_business_card": str(item.get("selected_business_card") or ""),
            "candidate_meta_json": self._json_dump(candidate_meta),
            "neutral_signal_profile_json": self._json_dump(item.get("neutral_signal_profile") or {}),
            "task_scoring_json": self._json_dump(item.get("selected_task_scoring") or {}),
            "business_feature_profile_json": self._json_dump(item.get("business_feature_profile") or {}),
            "eligible_material_cards_json": self._json_dump((item.get("llm_candidate_material_cards") or item.get("eligible_material_cards") or [])[:8]),
            "eligible_business_cards_json": self._json_dump((item.get("llm_candidate_business_cards") or item.get("eligible_business_cards") or [])[:8]),
            "question_ready_context_json": self._json_dump(question_ready),
            "quality_score": str(item.get("quality_score") or 0.0),
        }
        return context

    def _normalize_family_id(self, business_family_id: str) -> str:
        return str(self.FAMILY_ALIASES.get(business_family_id, business_family_id))

    def _normalize_result(self, *, judge_name: str, model: str, payload: dict[str, Any]) -> dict[str, Any]:
        decision = str(payload.get("decision") or "reject")
        formal_layer = str(payload.get("formal_layer") or "reject")
        return {
            "judge_name": judge_name,
            "model": model,
            "status": "ok",
            "decision": decision,
            "formal_layer": formal_layer,
            "selected_material_card": payload.get("selected_material_card"),
            "selected_business_card": payload.get("selected_business_card"),
            "reason": str(payload.get("reason") or ""),
            "evidence_summary": str(payload.get("evidence_summary") or ""),
            "confidence": float(payload.get("confidence") or 0.0),
        }

    def _build_consensus(self, judge_results: list[dict[str, Any]]) -> dict[str, Any]:
        ok_results = [item for item in judge_results if item.get("status") == "ok"]
        if not ok_results:
            return {
                "status": "error",
                "decision": "reject",
                "formal_layer": "reject",
                "selected_material_card": None,
                "selected_business_card": None,
            }
        if self._expected_judge_count() <= 1 and len(ok_results) == 1:
            only = ok_results[0]
            return {
                "status": "single",
                "decision": only.get("decision", "reject"),
                "formal_layer": only.get("formal_layer", "reject"),
                "selected_material_card": only.get("selected_material_card"),
                "selected_business_card": only.get("selected_business_card"),
            }
        if len(ok_results) < self._expected_judge_count():
            return {
                "status": "insufficient_votes",
                "decision": "reject",
                "formal_layer": "reject",
                "selected_material_card": None,
                "selected_business_card": None,
            }
        decisions = {item.get("decision") for item in ok_results}
        layers = {item.get("formal_layer") for item in ok_results}
        material_cards = {item.get("selected_material_card") for item in ok_results if item.get("selected_material_card")}
        business_cards = {item.get("selected_business_card") for item in ok_results if item.get("selected_business_card")}
        all_ok = len(ok_results) == len(judge_results)
        if all_ok and len(decisions) == 1 and len(layers) == 1 and len(material_cards) <= 1 and len(business_cards) <= 1:
            return {
                "status": "unanimous",
                "decision": next(iter(decisions), "reject"),
                "formal_layer": next(iter(layers), "reject"),
                "selected_material_card": next(iter(material_cards), None),
                "selected_business_card": next(iter(business_cards), None),
            }
        return {
            "status": "split_vote",
            "decision": "conflict",
            "formal_layer": "conflict",
            "selected_material_card": None,
            "selected_business_card": None,
        }

    def _render_template(self, template: str, values: dict[str, str]) -> str:
        return template.format_map(_SafeDict(values))

    def _json_dump(self, payload: Any) -> str:
        return json.dumps(payload, ensure_ascii=False, sort_keys=True)

    def _response_schema(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "decision": {"type": "string", "enum": ["accept", "borderline", "reject"]},
                "formal_layer": {"type": "string", "enum": ["strong", "weak", "reject"]},
                "selected_material_card": {"type": ["string", "null"]},
                "selected_business_card": {"type": ["string", "null"]},
                "reason": {"type": "string"},
                "evidence_summary": {"type": "string"},
                "confidence": {"type": "number"},
            },
            "required": [
                "decision",
                "formal_layer",
                "selected_material_card",
                "selected_business_card",
                "reason",
                "evidence_summary",
                "confidence",
            ],
            "additionalProperties": False,
        }
