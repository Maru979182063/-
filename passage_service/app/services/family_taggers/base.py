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

    @abstractmethod
    def score(self, span: SpanRecord, universal_profile: UniversalProfile) -> tuple[list[SubtypeCandidate], dict]:
        ...

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
        prompt = "\n".join(
            [
                f"family: {self.family_name}",
                f"allowed_subtypes: {', '.join(subtype_names)}",
                f"paragraph_count: {span.paragraph_count}",
                f"sentence_count: {span.sentence_count}",
                f"text: {span.text}",
                f"universal_profile: {universal_profile.model_dump_json(ensure_ascii=False)}",
            ]
        )
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
