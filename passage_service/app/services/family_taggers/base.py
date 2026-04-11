from abc import ABC, abstractmethod
from typing import Any

from app.core.config import get_config_bundle
from app.schemas.span import SpanRecord
from app.schemas.subtype_route import SubtypeCandidate
from app.schemas.universal_profile import UniversalProfile
from app.services.llm_runtime import get_llm_provider, read_prompt_file


class BaseFamilyTagger(ABC):
    family_name: str

    def __init__(self, prompt_file: str) -> None:
        self.provider = get_llm_provider()
        self.prompt = read_prompt_file(prompt_file)
        self.llm_config = get_config_bundle().llm
        self._runtime_context: dict[str, Any] = {}

    @abstractmethod
    def score(self, span: SpanRecord, universal_profile: UniversalProfile) -> tuple[list[SubtypeCandidate], dict]:
        ...

    def set_runtime_context(self, context: dict[str, Any] | None) -> None:
        self._runtime_context = dict(context or {})

    def clear_runtime_context(self) -> None:
        self._runtime_context = {}

    def sort_candidates(self, candidates: list[SubtypeCandidate], *, limit: int = 3) -> list[SubtypeCandidate]:
        return sorted(candidates, key=lambda item: item.score, reverse=True)[:limit]

    def maybe_score_with_llm(
        self,
        *,
        model: str,
        span: SpanRecord,
        universal_profile: UniversalProfile,
        subtype_names: list[str],
        heuristic_candidates: list[SubtypeCandidate],
    ) -> tuple[list[SubtypeCandidate], dict] | None:
        should_use, gate_reason = self._should_use_llm(heuristic_candidates=heuristic_candidates)
        if not should_use:
            return None
        result = self.score_with_llm(
            model=model,
            span=span,
            universal_profile=universal_profile,
            subtype_names=subtype_names,
        )
        if result is None:
            return None
        candidates, notes = result
        notes = {
            **notes,
            "llm_used": True,
            "llm_gate_reason": gate_reason,
            "family_runtime_context": dict(self._runtime_context),
        }
        return self.sort_candidates(candidates), notes

    def score_with_llm(
        self,
        *,
        model: str,
        span: SpanRecord,
        universal_profile: UniversalProfile,
        subtype_names: list[str],
    ) -> tuple[list[SubtypeCandidate], dict] | None:
        if not self.llm_config.get("enabled") or not self.provider.is_enabled():
            return None
        schema: dict[str, Any] = {
            "type": "object",
            "properties": {
                "subtype_candidates": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "family": {"type": "string"},
                            "subtype": {"type": "string"},
                            "score": {"type": "number"},
                        },
                        "required": ["family", "subtype", "score"],
                        "additionalProperties": False,
                    },
                },
                "notes": {"type": "object"},
            },
            "required": ["subtype_candidates", "notes"],
            "additionalProperties": False,
        }
        prompt = self.build_llm_prompt(span=span, universal_profile=universal_profile, subtype_names=subtype_names)
        try:
            result = self.provider.generate_json(
                model=model,
                instructions=self.prompt,
                input_payload={
                    "prompt": prompt,
                    "schema_name": f"{self.family_name}_subtypes",
                    "schema": schema,
                },
            )
        except Exception:
            return None
        subtype_candidates = [SubtypeCandidate.model_validate(item) for item in result.get("subtype_candidates", [])]
        return subtype_candidates, result.get("notes", {})

    def build_llm_prompt(
        self,
        *,
        span: SpanRecord,
        universal_profile: UniversalProfile,
        subtype_names: list[str],
    ) -> str:
        compact_text = self._compact_text(span.text)
        compact_profile = self._compact_universal_profile(universal_profile)
        return "\n".join(
            [
                f"family: {self.family_name}",
                f"allowed_subtypes: {', '.join(subtype_names)}",
                f"paragraph_count: {span.paragraph_count}",
                f"sentence_count: {span.sentence_count}",
                f"candidate_text: {compact_text}",
                f"universal_profile_summary: {compact_profile}",
                f"routing_context: {self._routing_context_summary()}",
            ]
        )

    def _should_use_llm(self, *, heuristic_candidates: list[SubtypeCandidate]) -> tuple[bool, str]:
        if not self.llm_config.get("enabled") or not self.provider.is_enabled():
            return False, "llm_disabled"

        family_score = float(self._runtime_context.get("family_score") or 0.0)
        family_rank = int(self._runtime_context.get("family_rank") or 0)
        primary_score = float(self._runtime_context.get("primary_score") or 0.0)
        score_gap_from_primary = float(self._runtime_context.get("score_gap_from_primary") or max(primary_score - family_score, 0.0))
        second_gap = float(self._runtime_context.get("primary_second_gap") or 1.0)

        if family_score < 0.45:
            return False, "router_score_too_low"
        if family_rank > 0 and score_gap_from_primary > 0.06:
            return False, "non_primary_not_close"

        sorted_candidates = self.sort_candidates(list(heuristic_candidates), limit=8)
        top_score = float(sorted_candidates[0].score) if sorted_candidates else 0.0
        second_score = float(sorted_candidates[1].score) if len(sorted_candidates) > 1 else 0.0
        heuristic_margin = top_score - second_score
        heuristic_confident = bool(
            sorted_candidates
            and (
                len(sorted_candidates) == 1
                or (top_score >= 0.75 and heuristic_margin >= 0.08)
            )
        )

        if heuristic_confident:
            return False, "heuristic_confident"
        if not sorted_candidates:
            return True, "no_heuristic_subtype"
        if family_rank == 0:
            return True, "primary_family_boundary"
        if second_gap <= 0.08 or score_gap_from_primary <= 0.04:
            return True, "close_family_boundary"
        return False, "secondary_family_not_ambiguous"

    def _compact_text(self, text: str, *, max_chars: int = 680) -> str:
        stripped = text.strip()
        if len(stripped) <= max_chars:
            return stripped
        head = stripped[:400]
        tail = stripped[-220:]
        return f"{head}\n...[snip {len(stripped) - 620} chars]...\n{tail}"

    def _compact_universal_profile(self, universal_profile: UniversalProfile) -> str:
        payload = {
            "text_shape": universal_profile.text_shape.model_dump(),
            "material_structure_label": universal_profile.material_structure_label,
            "logic_relations": universal_profile.logic_relations[:4],
            "position_roles": universal_profile.position_roles[:4],
            "scores": {
                "single_center_strength": round(universal_profile.single_center_strength, 4),
                "summary_strength": round(universal_profile.summary_strength, 4),
                "transition_strength": round(universal_profile.transition_strength, 4),
                "explanation_strength": round(universal_profile.explanation_strength, 4),
                "ordering_anchor_strength": round(universal_profile.ordering_anchor_strength, 4),
                "continuation_openness": round(universal_profile.continuation_openness, 4),
                "direction_uniqueness": round(universal_profile.direction_uniqueness, 4),
                "titleability": round(universal_profile.titleability, 4),
                "value_judgement_strength": round(universal_profile.value_judgement_strength, 4),
                "example_to_theme_strength": round(universal_profile.example_to_theme_strength, 4),
                "problem_signal_strength": round(universal_profile.problem_signal_strength, 4),
                "method_signal_strength": round(universal_profile.method_signal_strength, 4),
                "branch_focus_strength": round(universal_profile.branch_focus_strength, 4),
                "independence_score": round(universal_profile.independence_score, 4),
            },
        }
        return str(payload)

    def _routing_context_summary(self) -> dict[str, Any]:
        return {
            "family_rank": self._runtime_context.get("family_rank"),
            "family_score": self._runtime_context.get("family_score"),
            "primary_family": self._runtime_context.get("primary_family"),
            "primary_score": self._runtime_context.get("primary_score"),
            "score_gap_from_primary": self._runtime_context.get("score_gap_from_primary"),
            "primary_second_gap": self._runtime_context.get("primary_second_gap"),
        }
