from __future__ import annotations

import re
from typing import Any

from app.core.config import get_config_bundle
from app.infra.llm.base import BaseLLMProvider
from app.services.llm_runtime import get_llm_provider, read_prompt_file


class MaterialIntegrityGate:
    def __init__(self) -> None:
        self.config = get_config_bundle().llm
        self.provider: BaseLLMProvider = get_llm_provider()
        self.prompt = read_prompt_file("material_integrity_gate_prompt.md")

    def evaluate(self, *, text: str, paragraph_count: int, sentence_count: int) -> dict[str, Any]:
        signals = self._collect_signals(text=text, paragraph_count=paragraph_count, sentence_count=sentence_count)
        hard_fail_reasons = self._hard_fail_reasons(signals)
        if hard_fail_reasons:
            return {
                "rule_passed": False,
                "needs_llm_review": False,
                "llm_passed": None,
                "is_complete": False,
                "is_truncated": signals["is_truncated_signal"],
                "is_context_dependent": signals["starts_with_context_dependency"],
                "risk_level": "high",
                "reason": ",".join(hard_fail_reasons),
                "signals": signals,
            }

        if not self._needs_llm_review(signals):
            return {
                "rule_passed": True,
                "needs_llm_review": False,
                "llm_passed": None,
                "is_complete": True,
                "is_truncated": False,
                "is_context_dependent": False,
                "risk_level": "low",
                "reason": "passed_rule_gate",
                "signals": signals,
            }

        if not self.config.get("enabled") or not self.provider.is_enabled():
            return {
                "rule_passed": True,
                "needs_llm_review": True,
                "llm_passed": None,
                "is_complete": True,
                "is_truncated": False,
                "is_context_dependent": False,
                "risk_level": "medium",
                "reason": "llm_review_deferred_gray_path",
                "signals": signals,
            }

        llm_result = self._llm_review(text=text, paragraph_count=paragraph_count, sentence_count=sentence_count, signals=signals)
        llm_passed = bool(
            llm_result.get("is_complete")
            and not llm_result.get("is_truncated")
            and not llm_result.get("is_context_dependent")
            and llm_result.get("suitable_for_material", False)
        )
        return {
            "rule_passed": True,
            "needs_llm_review": True,
            "llm_passed": llm_passed,
            "is_complete": bool(llm_result.get("is_complete")),
            "is_truncated": bool(llm_result.get("is_truncated")),
            "is_context_dependent": bool(llm_result.get("is_context_dependent")),
            "risk_level": llm_result.get("risk_level", "medium"),
            "reason": llm_result.get("reason", "llm_review_completed"),
            "signals": signals,
        }

    def _collect_signals(self, *, text: str, paragraph_count: int, sentence_count: int) -> dict[str, Any]:
        stripped = text.strip()
        tail = stripped[-24:]
        head = stripped[:30]
        sequence_tail_tokens = ("\u53ca", "\u548c", "\u4e0e", "\u5e76", "\u5e76\u4e14", "\u4ee5\u53ca", "\u7b49", "\u3001", "\uff0c", "\uff1a", "\u2014")
        context_starters_hard = (
            "\u5bf9\u6b64",
            "\u4e0e\u6b64\u540c\u65f6",
            "\u53e6\u4e00\u65b9\u9762",
            "\u6b64\u5916",
            "\u5176\u4e2d",
            "\u8fd9\u4e00\u70b9",
            "\u8fd9\u4e5f",
            "\u8fd9\u79cd\u60c5\u51b5\u4e0b",
            "\u8fd9\u4e9b\u505a\u6cd5",
            "\u90a3\u5c31\u662f",
        )
        context_starters_soft = ("\u7136\u800c", "\u4f46\u662f", "\u540c\u65f6", "\u56e0\u6b64", "\u6240\u4ee5")
        summary_markers = ("\u603b\u4e4b", "\u53ef\u89c1", "\u56e0\u6b64", "\u7531\u6b64", "\u8fdb\u800c", "\u624d\u80fd")
        terminal_punct = bool(re.search(r"[\u3002\uff01\uff1f!?]$|\u201d[\u3002\uff01\uff1f!?]$|\u3011$", stripped))
        ellipsis_truncation = stripped.endswith("...") or stripped.endswith("\u2026") or stripped.endswith("\u2026\u2026")
        trailing_fragment = stripped.endswith(sequence_tail_tokens) or bool(re.search(r"[\uff08(\u300a\u3010\u201c]$", stripped))
        half_clause_tail = bool(
            re.search(
                r"(\u5982\u679c|\u4f46\u662f|\u56e0\u6b64|\u6240\u4ee5|\u5e76\u4e14|\u800c\u4e14|\u4e3a\u4e86|\u901a\u8fc7)$",
                tail,
            )
        )
        starts_with_context_dependency = stripped.startswith(context_starters_hard)
        starts_with_soft_dependency = stripped.startswith(context_starters_soft)
        has_summary_closure = any(marker in tail for marker in summary_markers)
        long_without_closure = len(stripped) >= 420 and paragraph_count >= 4 and not has_summary_closure
        return {
            "paragraph_count": paragraph_count,
            "sentence_count": sentence_count,
            "char_count": len(stripped),
            "terminal_punct": terminal_punct,
            "ellipsis_truncation": ellipsis_truncation,
            "trailing_fragment": trailing_fragment,
            "half_clause_tail": half_clause_tail,
            "starts_with_context_dependency": starts_with_context_dependency,
            "starts_with_soft_dependency": starts_with_soft_dependency,
            "has_summary_closure": has_summary_closure,
            "long_without_closure": long_without_closure,
            "tail_preview": tail,
            "head_preview": head,
            "is_truncated_signal": (not terminal_punct) or ellipsis_truncation or trailing_fragment or half_clause_tail,
        }

    def _hard_fail_reasons(self, signals: dict[str, Any]) -> list[str]:
        reasons: list[str] = []
        if not signals["terminal_punct"]:
            reasons.append("missing_terminal_punctuation")
        if signals["ellipsis_truncation"]:
            reasons.append("ellipsis_truncation")
        if signals["trailing_fragment"]:
            reasons.append("trailing_fragment")
        if signals["half_clause_tail"]:
            reasons.append("half_clause_tail")
        if signals["starts_with_context_dependency"]:
            reasons.append("starts_with_context_dependency")
        return reasons

    def _needs_llm_review(self, signals: dict[str, Any]) -> bool:
        return bool(
            (signals["starts_with_soft_dependency"] and signals["paragraph_count"] <= 1)
            or signals["long_without_closure"]
            or (signals["char_count"] >= 260 and signals["sentence_count"] >= 5 and not signals["has_summary_closure"])
        )

    def _llm_review(self, *, text: str, paragraph_count: int, sentence_count: int, signals: dict[str, Any]) -> dict[str, Any]:
        schema: dict[str, Any] = {
            "type": "object",
            "properties": {
                "is_complete": {"type": "boolean"},
                "is_truncated": {"type": "boolean"},
                "is_context_dependent": {"type": "boolean"},
                "suitable_for_material": {"type": "boolean"},
                "risk_level": {"type": "string", "enum": ["low", "medium", "high"]},
                "reason": {"type": "string"},
            },
            "required": [
                "is_complete",
                "is_truncated",
                "is_context_dependent",
                "suitable_for_material",
                "risk_level",
                "reason",
            ],
            "additionalProperties": False,
        }
        prompt = "\n".join(
            [
                f"paragraph_count: {paragraph_count}",
                f"sentence_count: {sentence_count}",
                f"text: {text}",
                f"signals: {signals}",
            ]
        )
        return self.provider.generate_json(
            model=self.config.get("models", {}).get("integrity_gate", self.config.get("models", {}).get("universal_tagger", "gpt-4o-mini")),
            instructions=self.prompt,
            input_payload={
                "prompt": prompt,
                "schema_name": "material_integrity_gate",
                "schema": schema,
            },
        )
