from __future__ import annotations

import json
from typing import Any

from app.core.config import get_config_bundle
from app.infra.llm.base import BaseLLMProvider
from app.services.llm_runtime import get_llm_provider


class _SafeDict(dict):
    def __missing__(self, key: str) -> str:
        return "{" + key + "}"


class MainCardFamilyLandingResolver:
    SUPPORTED_MAIN_CARDS = ("center_understanding", "sentence_fill", "sentence_order")
    RUNTIME_FAMILY_MAP = {
        "center_understanding": "title_selection",
        "sentence_fill": "sentence_fill",
        "sentence_order": "sentence_order",
    }

    def __init__(
        self,
        *,
        provider: BaseLLMProvider | None = None,
        llm_config: dict[str, Any] | None = None,
    ) -> None:
        self.provider = provider or get_llm_provider()
        self.llm_config = llm_config or get_config_bundle().llm
        self.config = dict(self.llm_config.get("main_card_family_landing") or {})

    def is_enabled(self) -> bool:
        if not bool(self.config.get("enabled")):
            return False
        return self.provider.is_enabled()

    def resolve(
        self,
        *,
        material: Any,
        article: Any,
        mechanical_v2_families: list[str] | None = None,
    ) -> dict[str, Any] | None:
        if not self.is_enabled():
            return None

        judge_specs = self._judge_specs()
        if not judge_specs:
            return None

        prompt_context = self._build_prompt_context(
            material=material,
            article=article,
            mechanical_v2_families=mechanical_v2_families or [],
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
                        "schema_name": f"main_card_family_landing_{judge_name}",
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
                    "selected_main_cards": [],
                    "reason": f"llm_error:{type(exc).__name__}",
                    "evidence_summary": "",
                    "confidence": 0.0,
                }
            judge_results.append(normalized)

        consensus = self._build_consensus(judge_results)
        return {
            "enabled": True,
            "mode": str(self.config.get("mode") or "enforce"),
            "judge_results": judge_results,
            "consensus": consensus,
            "runtime_families": [
                self.RUNTIME_FAMILY_MAP[family]
                for family in consensus.get("selected_main_cards", [])
                if family in self.RUNTIME_FAMILY_MAP
            ],
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

    def _build_prompt_context(
        self,
        *,
        material: Any,
        article: Any,
        mechanical_v2_families: list[str],
    ) -> dict[str, str]:
        runtime_families = dict(self.config.get("runtime_families") or {})
        allowed_cards = []
        for family_id in self.SUPPORTED_MAIN_CARDS:
            family_config = dict(runtime_families.get(family_id) or {})
            allowed_cards.append(
                {
                    "main_card_id": family_id,
                    "label": family_config.get("label") or family_id,
                    "goal": family_config.get("goal") or "",
                    "formal_unit_definition": family_config.get("formal_unit_definition") or "",
                    "accept_definition": family_config.get("accept_definition") or "",
                    "reject_definition": family_config.get("reject_definition") or "",
                }
            )

        return {
            "article_title": str(getattr(article, "title", "") or ""),
            "article_source": str(getattr(article, "source", "") or ""),
            "material_text": self._compact_text(str(getattr(material, "text", "") or "")),
            "material_id": str(getattr(material, "id", "") or ""),
            "material_status": str(getattr(material, "status", "") or ""),
            "release_channel": str(getattr(material, "release_channel", "") or ""),
            "quality_score": str(getattr(material, "quality_score", 0.0) or 0.0),
            "paragraph_count": str(getattr(material, "paragraph_count", 0) or 0),
            "sentence_count": str(getattr(material, "sentence_count", 0) or 0),
            "primary_family": str(getattr(material, "primary_family", "") or ""),
            "primary_subtype": str(getattr(material, "primary_subtype", "") or ""),
            "parallel_families_json": self._json_dump(getattr(material, "parallel_families", []) or []),
            "family_scores_json": self._json_dump(getattr(material, "family_scores", {}) or {}),
            "universal_profile_json": self._json_dump(getattr(material, "universal_profile", {}) or {}),
            "feature_profile_json": self._json_dump(getattr(material, "feature_profile", {}) or {}),
            "mechanical_v2_families_json": self._json_dump(sorted(mechanical_v2_families)),
            "allowed_main_cards_json": self._json_dump(allowed_cards),
        }

    def _normalize_result(self, *, judge_name: str, model: str, payload: dict[str, Any]) -> dict[str, Any]:
        selected = []
        for family in payload.get("selected_main_cards") or []:
            family_id = str(family or "").strip()
            if family_id in self.SUPPORTED_MAIN_CARDS and family_id not in selected:
                selected.append(family_id)
        return {
            "judge_name": judge_name,
            "model": model,
            "status": "ok",
            "selected_main_cards": selected,
            "reason": str(payload.get("reason") or ""),
            "evidence_summary": str(payload.get("evidence_summary") or ""),
            "confidence": float(payload.get("confidence") or 0.0),
        }

    def _build_consensus(self, judge_results: list[dict[str, Any]]) -> dict[str, Any]:
        ok_results = [item for item in judge_results if item.get("status") == "ok"]
        if not ok_results:
            return {
                "status": "error",
                "selected_main_cards": [],
                "reason": "judge_error",
            }
        if self._expected_judge_count() <= 1 and len(ok_results) == 1:
            return {
                "status": "single",
                "selected_main_cards": sorted(set(ok_results[0].get("selected_main_cards") or [])),
                "reason": "single_judge",
            }
        if len(ok_results) < self._expected_judge_count():
            return {
                "status": "insufficient_votes",
                "selected_main_cards": [],
                "reason": "judge_error",
            }
        judge_sets = [set(item.get("selected_main_cards") or []) for item in ok_results]
        if judge_sets[0] == judge_sets[1]:
            return {
                "status": "unanimous",
                "selected_main_cards": sorted(judge_sets[0]),
                "reason": "exact_match",
            }
        intersected = sorted(judge_sets[0].intersection(judge_sets[1]))
        if intersected:
            return {
                "status": "intersected",
                "selected_main_cards": intersected,
                "reason": "intersection_only",
            }
        return {
            "status": "split_vote",
            "selected_main_cards": [],
            "reason": "no_overlap",
        }

    def _response_schema(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "selected_main_cards": {
                    "type": "array",
                    "items": {
                        "type": "string",
                        "enum": list(self.SUPPORTED_MAIN_CARDS),
                    },
                    "uniqueItems": True,
                },
                "reason": {"type": "string"},
                "evidence_summary": {"type": "string"},
                "confidence": {"type": "number"},
            },
            "required": [
                "selected_main_cards",
                "reason",
                "evidence_summary",
                "confidence",
            ],
            "additionalProperties": False,
        }

    def _render_template(self, template: str, values: dict[str, str]) -> str:
        return template.format_map(_SafeDict(values))

    def _json_dump(self, payload: Any) -> str:
        return json.dumps(payload, ensure_ascii=False, sort_keys=True)

    def _compact_text(self, text: str, *, max_chars: int = 1200) -> str:
        stripped = text.strip()
        if len(stripped) <= max_chars:
            return stripped
        head = stripped[:760]
        tail = stripped[-320:]
        snip_len = max(0, len(stripped) - 1080)
        return f"{head}\n...[snip {snip_len} chars]...\n{tail}"
