from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field

from app.schemas.api import (
    DifficultyFit,
    DifficultyProjection,
    DifficultyTarget,
    DifficultyTargetProfile,
    PatternSelectionReason,
    PromptPackage,
)


class ItemStatuses(BaseModel):
    build_status: Literal["success", "failed"]
    generation_status: Literal["not_started", "success", "failed"]
    validation_status: Literal["not_started", "passed", "failed"]
    review_status: Literal["draft", "waiting_review", "approved", "needs_revision", "rejected"]


class GeneratedQuestion(BaseModel):
    question_type: str
    business_subtype: str | None = None
    pattern_id: str | None = None
    stem: str
    original_sentences: list[str] = Field(default_factory=list)
    correct_order: list[int] = Field(default_factory=list)
    options: dict[str, str] = Field(default_factory=lambda: {"A": "", "B": "", "C": "", "D": ""})
    answer: str
    analysis: str
    metadata: dict[str, Any] | None = None


class ValidationResult(BaseModel):
    validation_status: Literal["passed", "failed"]
    passed: bool = True
    score: int = 100
    errors: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    checks: dict[str, Any] = Field(default_factory=dict)
    difficulty_review: dict[str, Any] | None = None
    next_review_status: str | None = None


class QuestionItem(BaseModel):
    item_id: str
    question_type: str
    business_subtype: str | None = None
    pattern_id: str | None = None
    generation_mode: str = "standard"
    material_source_type: str | None = None
    forced_generation: bool = False
    current_version_no: int = 1
    current_status: str = "draft"
    latest_action: str | None = None
    latest_action_at: str | None = None
    manual_override_active: bool = False
    selected_pattern: str
    pattern_selection_reason: PatternSelectionReason | None = None
    resolved_slots: dict[str, Any]
    skeleton: dict[str, Any]
    difficulty_target: DifficultyTarget
    difficulty_target_profile: DifficultyTargetProfile | None = None
    difficulty_projection: DifficultyProjection | None = None
    difficulty_fit: DifficultyFit | None = None
    control_logic: dict[str, Any]
    generation_logic: dict[str, Any]
    prompt_package: PromptPackage
    generated_question: GeneratedQuestion | None = None
    validation_result: ValidationResult | None = None
    evaluation_result: dict[str, Any] | None = None
    statuses: ItemStatuses
    warnings: list[str] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)


class PromptBuildResponse(QuestionItem):
    pass
