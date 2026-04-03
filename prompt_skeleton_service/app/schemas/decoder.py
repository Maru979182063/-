from __future__ import annotations

from typing import Any, Literal

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, field_validator

from app.schemas.api import DifficultyTarget, PromptBuildRequest


class MappingTarget(BaseModel):
    question_type: str
    business_subtype: str | None = None
    pattern_id: str | None = None


class BatchMeta(BaseModel):
    requested_count: int
    effective_count: int
    question_type: str
    business_subtype: str | None = None
    pattern_id: str | None = None
    difficulty_target: DifficultyTarget


class DifyFormInput(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    question_focus: str = Field(
        validation_alias=AliasChoices("question_focus", "\u95ee\u9898\u8003\u70b9"),
        description="Business-facing question focus from Dify form.",
    )
    difficulty_level: str = Field(
        validation_alias=AliasChoices("difficulty_level", "\u96be\u5ea6\u7ea7\u522b"),
        description="Business-facing difficulty label from Dify form.",
    )
    text_direction: str | None = Field(
        default=None,
        validation_alias=AliasChoices("text_direction", "\u6587\u672c\u65b9\u5411"),
        description="Not consumed by the prompt engine yet; carried via extra_constraints.",
    )
    special_question_types: list[str] = Field(
        default_factory=list,
        validation_alias=AliasChoices("special_question_types", "\u7279\u6b8a\u9898\u578b"),
        description="UI may send a single string or a checkbox-style list; runtime still enforces single choice.",
    )
    count: int | None = Field(
        default=None,
        validation_alias=AliasChoices("count", "\u6570\u91cf"),
    )

    @field_validator("special_question_types", mode="before")
    @classmethod
    def normalize_special_question_types(cls, value: Any) -> list[str]:
        if value is None:
            return []
        if isinstance(value, str):
            normalized = value.replace("\uFF0C", ",")
            return [item.strip() for item in normalized.split(",") if item.strip()]
        if isinstance(value, (list, tuple, set)):
            return [str(item).strip() for item in value if str(item).strip()]
        raise ValueError("special_question_types must be a string or a list of strings.")


class DecodedPromptBuildEnvelope(BaseModel):
    mapping_source: Literal["question_focus", "special_question_type"]
    selected_special_type: str | None = None
    standard_request: PromptBuildRequest
    batch_meta: BatchMeta
    warnings: list[str] = Field(default_factory=list)
