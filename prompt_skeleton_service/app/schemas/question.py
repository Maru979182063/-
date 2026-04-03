from __future__ import annotations

from typing import Any, Literal

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, field_validator

from app.schemas.decoder import BatchMeta, DifyFormInput
from app.schemas.item import QuestionItem


class MaterialPolicy(BaseModel):
    allow_reuse: bool = True
    cooldown_days: int = 0
    preferred_document_genres: list[str] = Field(default_factory=list)
    excluded_material_ids: list[str] = Field(default_factory=list)
    prefer_high_quality_reused: bool = False


class SourceQuestionPayload(BaseModel):
    passage: str | None = None
    stem: str
    options: dict[str, str] = Field(default_factory=dict)
    answer: str | None = None
    analysis: str | None = None


class SourceQuestionDetectRequest(BaseModel):
    source_question: SourceQuestionPayload


class SourceQuestionDetectResponse(BaseModel):
    question_focus: str
    special_question_type: str | None = None
    text_direction: str | None = None
    material_structure: str | None = None
    topic: str | None = None
    business_card_ids: list[str] = Field(default_factory=list)
    query_terms: list[str] = Field(default_factory=list)


class SourceQuestionParseRequest(BaseModel):
    raw_text: str


class SourceQuestionParseResponse(BaseModel):
    source_question: SourceQuestionPayload


class QuestionGenerateRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    question_focus: str = Field(validation_alias=AliasChoices("question_focus", "\u95ee\u9898\u8003\u70b9"))
    difficulty_level: str = Field(validation_alias=AliasChoices("difficulty_level", "\u96be\u5ea6\u7ea7\u522b"))
    text_direction: str | None = Field(
        default=None,
        validation_alias=AliasChoices("text_direction", "\u6587\u672c\u65b9\u5411"),
    )
    material_structure: str | None = None
    special_question_types: list[str] = Field(
        default_factory=list,
        validation_alias=AliasChoices("special_question_types", "\u7279\u6b8a\u9898\u578b"),
    )
    count: int | None = Field(default=None, validation_alias=AliasChoices("count", "\u6570\u91cf"))
    topic: str | None = None
    passage_style: str | None = None
    use_fewshot: bool = True
    fewshot_mode: str = "structure_only"
    type_slots: dict[str, Any] = Field(default_factory=dict)
    extra_constraints: dict[str, Any] | None = None
    material_policy: MaterialPolicy | None = None
    source_question: SourceQuestionPayload | None = None

    @field_validator("question_focus", mode="before")
    @classmethod
    def normalize_question_focus(cls, value: Any) -> str:
        text = str(value or "").strip()
        placeholders = {"select", "auto", "不指定", "不指定（自动匹配）", "请选择"}
        if text.lower() in placeholders or text in placeholders:
            return ""
        return text

    @field_validator("special_question_types", mode="before")
    @classmethod
    def normalize_special_question_types(cls, value: Any) -> list[str]:
        if value is None:
            return []
        if isinstance(value, str):
            normalized = value.replace("\uFF0C", ",")
            items = [item.strip() for item in normalized.split(",") if item.strip()]
            return [item for item in items if item.lower() not in {"select", "auto"} and item not in {"不指定", "不指定（自动匹配）", "请选择"}]
        if isinstance(value, (list, tuple, set)):
            items = [str(item).strip() for item in value if str(item).strip()]
            return [item for item in items if item.lower() not in {"select", "auto"} and item not in {"不指定", "不指定（自动匹配）", "请选择"}]
        raise ValueError("special_question_types must be a string or a list of strings.")

    def to_dify_form_input(self) -> DifyFormInput:
        return DifyFormInput(
            question_focus=self.question_focus,
            difficulty_level=self.difficulty_level,
            text_direction=self.text_direction,
            special_question_types=self.special_question_types,
            count=self.count,
        )


class MaterialSelectionResult(BaseModel):
    material_id: str
    article_id: str
    text: str
    original_text: str | None = None
    source: dict[str, Any] = Field(default_factory=dict)
    source_tail: str | None = None
    primary_label: str | None = None
    document_genre: str | None = None
    material_structure_label: str | None = None
    material_structure_reason: str | None = None
    standalone_readability: float = 0.0
    quality_score: float = 0.0
    fit_scores: dict[str, Any] = Field(default_factory=dict)
    knowledge_tags: list[str] = Field(default_factory=list)
    usage_count_before: int = 0
    previously_used: bool = False
    last_used_at: str | None = None
    usage_note: str | None = None
    text_refined: bool = False
    refinement_reason: str | None = None
    anchor_adapted: bool = False
    anchor_adaptation_reason: str | None = None
    anchor_span: dict[str, Any] = Field(default_factory=dict)
    selection_reason: str


class QuestionGenerationItem(QuestionItem):
    batch_id: str
    created_at: str | None = None
    updated_at: str | None = None
    request_snapshot: dict[str, Any] = Field(default_factory=dict)
    material_selection: MaterialSelectionResult | None = None
    stem_text: str | None = None
    material_text: str | None = None
    material_source: dict[str, Any] = Field(default_factory=dict)
    material_usage_count_before: int = 0
    material_previously_used: bool = False
    material_last_used_at: str | None = None
    revision_count: int = 0


class QuestionGenerationBatchResponse(BaseModel):
    batch_id: str
    batch_meta: BatchMeta
    items: list[QuestionGenerationItem] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)


class QuestionReviewActionRequest(BaseModel):
    action: Literal["minor_edit", "question_modify", "text_modify", "manual_edit", "approve", "confirm", "discard"]
    instruction: str | None = None
    control_overrides: dict[str, Any] = Field(default_factory=dict)
    operator: str | None = None


class QuestionFineTuneRequest(BaseModel):
    instruction: str
    operator: str | None = None


class QuestionConfirmRequest(BaseModel):
    operator: str | None = None


class QuestionReviewActionResponse(BaseModel):
    action_id: str
    action: str
    item: QuestionGenerationItem


class QuestionControlValue(BaseModel):
    control_key: str
    label: str
    current_value: Any = None
    default_value: Any = None
    options: list[dict[str, Any]] = Field(default_factory=list)
    affects_difficulty: bool = False
    editable_by: str = "generator_and_reviewer"
    mapped_action: str = "question_modify"
    read_only: bool = False
    description: str | None = None


class QuestionControlPanelResponse(BaseModel):
    item_id: str
    question_type: str
    business_subtype: str | None = None
    pattern_id: str | None = None
    difficulty_target: str
    controls: list[QuestionControlValue] = Field(default_factory=list)


class QuestionItemSummary(BaseModel):
    item_id: str
    batch_id: str
    question_type: str
    business_subtype: str | None = None
    pattern_id: str | None = None
    current_version_no: int = 1
    current_status: str = "draft"
    latest_action: str | None = None
    latest_action_at: str | None = None
    review_status: str
    generation_status: str
    difficulty_target: str
    revision_count: int = 0
    material_id: str | None = None
    document_genre: str | None = None
    stem_preview: str | None = None
    material_preview: str | None = None
    created_at: str | None = None
    updated_at: str


class QuestionItemListResponse(BaseModel):
    count: int
    items: list[QuestionItemSummary] = Field(default_factory=list)


class QuestionReviewActionLog(BaseModel):
    action_id: str
    item_id: str
    action_type: str
    created_at: str
    payload: dict[str, Any] = Field(default_factory=dict)


class QuestionBatchSummary(BaseModel):
    batch_id: str
    requested_count: int
    effective_count: int
    question_type: str
    business_subtype: str | None = None
    difficulty_target: str
    item_count: int
    updated_at: str


class QuestionBatchListResponse(BaseModel):
    count: int
    items: list[QuestionBatchSummary] = Field(default_factory=list)


class QuestionBatchDetailResponse(BaseModel):
    batch_id: str
    requested_count: int
    effective_count: int
    question_type: str
    business_subtype: str | None = None
    difficulty_target: str
    item_count: int
    review_status_counts: dict[str, int] = Field(default_factory=dict)
    generation_status_counts: dict[str, int] = Field(default_factory=dict)
    items: list[QuestionItemSummary] = Field(default_factory=list)
    updated_at: str


class QuestionReviewQueueResponse(BaseModel):
    count: int
    review_status: str
    items: list[QuestionItemSummary] = Field(default_factory=list)


class ReplacementMaterialOption(BaseModel):
    material_id: str
    label: str
    article_title: str | None = None
    source_name: str | None = None
    document_genre: str | None = None
    text_preview: str | None = None
    material_text: str | None = None
    usage_count_before: int = 0


class ReplacementMaterialListResponse(BaseModel):
    item_id: str
    count: int
    items: list[ReplacementMaterialOption] = Field(default_factory=list)
