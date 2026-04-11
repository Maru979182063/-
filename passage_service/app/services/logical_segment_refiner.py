from __future__ import annotations

from typing import Any

from app.core.config import get_config_bundle
from app.infra.llm.base import BaseLLMProvider
from app.services.llm_runtime import get_llm_provider, read_prompt_file


class LogicalSegmentRefiner:
    def __init__(self) -> None:
        self.segmentation_config = get_config_bundle().segmentation
        self.llm_config = get_config_bundle().llm
        self.provider: BaseLLMProvider = get_llm_provider()
        self.prompt = read_prompt_file("logical_segment_refiner_prompt.md")
        logical_config = self.segmentation_config.get("logical_refiner", {})
        self.enabled = bool(logical_config.get("enabled", True))
        self.min_chars_for_llm = int(logical_config.get("min_chars_for_llm", 70))
        self.review_max_spans = int(logical_config.get("review_max_spans", 12))
        self.merge_max_chars = int(logical_config.get("merge_max_chars", 900))

    def refine(self, spans: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not self.enabled or not spans:
            return spans

        reviewed = []
        for index, span in enumerate(spans):
            decision = self._heuristic_decision(span)
            if self._should_use_llm(span, decision, len(reviewed)):
                llm_decision = self._llm_decision(index=index, span=span, spans=spans)
                if llm_decision is not None:
                    decision = llm_decision
            reviewed.append({"span": span, "decision": decision})

        return self._materialize(reviewed)

    def _heuristic_decision(self, span: dict[str, Any]) -> dict[str, Any]:
        text = span["text"].strip()
        short = len(text) < 90
        no_terminal = not text.endswith(("。", "！", "？", "!", "?"))
        starts_with_dependency = text.startswith(("因此", "同时", "然而", "不过", "对此", "另外"))
        bridge_like = starts_with_dependency and short
        too_fine = span["span_type"] == "sentence_group" and len(text) < 140
        if no_terminal:
            return self._decision("merge_with_next", "missing_terminal_punctuation")
        if bridge_like:
            return self._decision("merge_with_prev", "bridge_like_fragment")
        if too_fine:
            return self._decision("merge_prev_next", "overly_fine_sentence_group")
        if span["span_type"] == "single_paragraph" and short:
            return self._decision("merge_with_next", "short_single_paragraph")
        return self._decision("keep", "heuristic_keep")

    def _should_use_llm(self, span: dict[str, Any], decision: dict[str, Any], reviewed_count: int) -> bool:
        if reviewed_count >= self.review_max_spans:
            return False
        if not self.llm_config.get("enabled") or not self.provider.is_enabled():
            return False
        if len(span["text"].strip()) < self.min_chars_for_llm:
            return False
        return decision["action"] != "keep" or span["span_type"] in {"paragraph_window", "story_fragment"}

    def _llm_decision(self, *, index: int, span: dict[str, Any], spans: list[dict[str, Any]]) -> dict[str, Any] | None:
        schema: dict[str, Any] = {
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ["keep", "drop", "merge_with_prev", "merge_with_next", "merge_prev_next"],
                },
                "suggested_span_type": {
                    "type": "string",
                    "enum": ["single_paragraph", "paragraph_window", "sentence_group", "story_fragment"],
                },
                "is_complete_unit": {"type": "boolean"},
                "is_context_dependent": {"type": "boolean"},
                "reason": {"type": "string"},
            },
            "required": ["action", "suggested_span_type", "is_complete_unit", "is_context_dependent", "reason"],
            "additionalProperties": False,
        }
        prev_text = spans[index - 1]["text"][:180] if index > 0 else ""
        next_text = spans[index + 1]["text"][:180] if index + 1 < len(spans) else ""
        prompt = "\n".join(
            [
                f"current_span_type: {span['span_type']}",
                f"current_text: {span['text']}",
                f"prev_text: {prev_text}",
                f"next_text: {next_text}",
            ]
        )
        try:
            return self.provider.generate_json(
            model=self.llm_config.get("models", {}).get("logical_segment_refiner", "gpt-5.4-nano"),
                instructions=self.prompt,
                input_payload={
                    "prompt": prompt,
                    "schema_name": "logical_segment_refiner",
                    "schema": schema,
                },
            )
        except Exception:
            return None

    def _materialize(self, reviewed: list[dict[str, Any]]) -> list[dict[str, Any]]:
        refined: list[dict[str, Any]] = []
        consumed: set[int] = set()

        for index, item in enumerate(reviewed):
            if index in consumed:
                continue
            span = item["span"]
            decision = item["decision"]
            action = decision.get("action", "keep")
            if action == "drop":
                continue
            if action in {"merge_with_prev", "merge_prev_next"} and refined:
                previous = refined[-1]
                merged = self._merge_spans(previous, span, suggested_span_type=decision.get("suggested_span_type"))
                refined[-1] = merged
                if action == "merge_prev_next" and index + 1 < len(reviewed):
                    next_span = reviewed[index + 1]["span"]
                    if len(merged["text"]) + len(next_span["text"]) <= self.merge_max_chars:
                        refined[-1] = self._merge_spans(refined[-1], next_span, suggested_span_type=decision.get("suggested_span_type"))
                        consumed.add(index + 1)
                continue
            if action == "merge_with_next" and index + 1 < len(reviewed):
                next_span = reviewed[index + 1]["span"]
                if len(span["text"]) + len(next_span["text"]) <= self.merge_max_chars:
                    refined.append(self._merge_spans(span, next_span, suggested_span_type=decision.get("suggested_span_type")))
                    consumed.add(index + 1)
                    continue
            updated = dict(span)
            if decision.get("suggested_span_type"):
                updated["span_type"] = decision["suggested_span_type"]
            updated["generated_by"] = f"{span['generated_by']}+logical_refiner"
            refined.append(updated)

        return self._dedupe(refined)

    def _merge_spans(self, left: dict[str, Any], right: dict[str, Any], *, suggested_span_type: str | None) -> dict[str, Any]:
        merged = {
            **left,
            "end_paragraph": max(left["end_paragraph"], right["end_paragraph"]),
            "start_sentence": min(v for v in [left.get("start_sentence"), right.get("start_sentence")] if v is not None),
            "end_sentence": max(v for v in [left.get("end_sentence"), right.get("end_sentence")] if v is not None),
            "text": f"{left['text']}\n\n{right['text']}".strip(),
            "generated_by": f"{left['generated_by']}+logical_refiner",
        }
        if suggested_span_type:
            merged["span_type"] = suggested_span_type
        else:
            merged["span_type"] = "paragraph_window" if merged["end_paragraph"] > merged["start_paragraph"] else left["span_type"]
        return merged

    def _dedupe(self, spans: list[dict[str, Any]]) -> list[dict[str, Any]]:
        seen: set[tuple[int, int, int | None, int | None, str]] = set()
        result: list[dict[str, Any]] = []
        for span in spans:
            key = (
                span["start_paragraph"],
                span["end_paragraph"],
                span.get("start_sentence"),
                span.get("end_sentence"),
                span["span_type"],
            )
            if key in seen:
                continue
            seen.add(key)
            result.append(span)
        return result

    def _decision(self, action: str, reason: str) -> dict[str, Any]:
        return {
            "action": action,
            "suggested_span_type": "paragraph_window" if "merge" in action else None,
            "is_complete_unit": action == "keep",
            "is_context_dependent": action != "keep",
            "reason": reason,
        }
