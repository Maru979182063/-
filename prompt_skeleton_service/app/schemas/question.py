from __future__ import annotations

from typing import Any, Literal

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, field_validator, model_validator

from app.schemas.decoder import BatchMeta, DifyFormInput
from app.schemas.item import QuestionItem


class MaterialPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid")

    allow_reuse: bool = True
    cooldown_days: int = 0
    preferred_document_genres: list[str] = Field(default_factory=list)
    excluded_material_ids: list[str] = Field(default_factory=list)
    prefer_high_quality_reused: bool = False


class SourceQuestionPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

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
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    question_card_id: str | None = None
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
    question_card_id: str | None = None
    runtime_binding: dict[str, Any] | None = None
    resolved_slots: dict[str, Any] | None = None
    validator_contract: dict[str, Any] | None = None
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


def _coerce_float(value: Any) -> float | None:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    return round(numeric, 4)


def _coerce_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "on"}:
        return True
    if text in {"false", "0", "no", "off"}:
        return False
    return None


def _normalize_float_mapping(payload: Any) -> dict[str, float]:
    if not isinstance(payload, dict):
        return {}
    normalized: dict[str, float] = {}
    for key, value in payload.items():
        numeric = _coerce_float(value)
        if numeric is None:
            continue
        normalized[str(key)] = numeric
    return normalized


def _top_positive_values(payload: Any, *, limit: int = 3) -> dict[str, float]:
    ranked = sorted(
        (
            (key, value)
            for key, value in _normalize_float_mapping(payload).items()
            if value > 0
        ),
        key=lambda entry: entry[1],
        reverse=True,
    )
    return {key: value for key, value in ranked[:limit]}


def _material_feedback_snapshot(item: dict[str, Any]) -> dict[str, Any]:
    material_source = item.get("material_source")
    if not isinstance(material_source, dict):
        material_source = {}

    explicit_snapshot = item.get("feedback_snapshot")
    if isinstance(explicit_snapshot, dict) and explicit_snapshot:
        return dict(explicit_snapshot)

    nested_snapshot = material_source.get("feedback_snapshot")
    if isinstance(nested_snapshot, dict) and nested_snapshot:
        return dict(nested_snapshot)

    scoring = material_source.get("scoring") if isinstance(material_source.get("scoring"), dict) else {}
    if not scoring and isinstance(material_source.get("selected_task_scoring"), dict):
        scoring = material_source.get("selected_task_scoring") or {}
    decision_meta = material_source.get("decision_meta") if isinstance(material_source.get("decision_meta"), dict) else {}
    scoring_summary = decision_meta.get("scoring_summary") if isinstance(decision_meta.get("scoring_summary"), dict) else {}
    difficulty_trace = scoring.get("difficulty_trace") if isinstance(scoring.get("difficulty_trace"), dict) else {}
    band_decision = difficulty_trace.get("band_decision") if isinstance(difficulty_trace.get("band_decision"), dict) else {}
    difficulty_vector = scoring.get("difficulty_vector") if isinstance(scoring.get("difficulty_vector"), dict) else {}
    risk_penalties = scoring.get("risk_penalties") if isinstance(scoring.get("risk_penalties"), dict) else {}

    return {
        "selection_state": decision_meta.get("selection_state"),
        "review_like_risk": _coerce_bool(decision_meta.get("review_like_risk")),
        "repair_suggested": _coerce_bool(decision_meta.get("repair_suggested")),
        "decision_reason": decision_meta.get("decision_reason"),
        "repair_reason": decision_meta.get("repair_reason"),
        "quality_difficulty_note": decision_meta.get("quality_difficulty_note") or band_decision.get("quality_difficulty_note"),
        "final_candidate_score": _coerce_float(scoring.get("final_candidate_score") or scoring_summary.get("final_candidate_score")),
        "readiness_score": _coerce_float(scoring.get("readiness_score") or scoring_summary.get("readiness_score")),
        "total_penalty": _coerce_float(scoring_summary.get("total_penalty")),
        "difficulty_band_hint": scoring.get("difficulty_band_hint") or scoring_summary.get("difficulty_band_hint"),
        "difficulty_vector": _normalize_float_mapping(difficulty_vector),
        "key_penalties": _normalize_float_mapping(decision_meta.get("key_penalties") or _top_positive_values(risk_penalties, limit=3)),
        "key_difficulty_dimensions": _normalize_float_mapping(
            decision_meta.get("key_difficulty_dimensions") or _top_positive_values(difficulty_vector, limit=3)
        ),
        "recommended": _coerce_bool(scoring.get("recommended") if "recommended" in scoring else scoring_summary.get("recommended")),
        "needs_review": _coerce_bool(scoring.get("needs_review") if "needs_review" in scoring else scoring_summary.get("needs_review")),
    }


def _normalized_feedback_payload(item: dict[str, Any]) -> dict[str, Any]:
    payload = _material_feedback_snapshot(item)
    quality_note = payload.get("quality_note")
    if quality_note is None:
        quality_note = payload.get("quality_difficulty_note")
    return {
        "selection_state": payload.get("selection_state"),
        "review_like_risk": _coerce_bool(payload.get("review_like_risk")),
        "repair_suggested": _coerce_bool(payload.get("repair_suggested")),
        "decision_reason": payload.get("decision_reason"),
        "repair_reason": payload.get("repair_reason"),
        "quality_note": quality_note,
        "quality_difficulty_note": quality_note,
        "final_candidate_score": _coerce_float(payload.get("final_candidate_score")),
        "readiness_score": _coerce_float(payload.get("readiness_score")),
        "total_penalty": _coerce_float(payload.get("total_penalty")),
        "difficulty_band_hint": payload.get("difficulty_band_hint"),
        "difficulty_vector": _normalize_float_mapping(payload.get("difficulty_vector")),
        "key_penalties": _normalize_float_mapping(payload.get("key_penalties")),
        "key_difficulty_dimensions": _normalize_float_mapping(payload.get("key_difficulty_dimensions")),
        "recommended": _coerce_bool(payload.get("recommended")),
        "needs_review": _coerce_bool(payload.get("needs_review")),
    }


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
    feedback_snapshot: dict[str, Any] = Field(default_factory=dict)
    selection_state: str | None = None
    review_like_risk: bool | None = None
    repair_suggested: bool | None = None
    final_candidate_score: float | None = None
    readiness_score: float | None = None
    total_penalty: float | None = None
    decision_reason: str | None = None
    repair_reason: str | None = None
    quality_note: str | None = None
    difficulty_band_hint: str | None = None
    difficulty_vector: dict[str, float] = Field(default_factory=dict)
    key_penalties: dict[str, float] = Field(default_factory=dict)
    key_difficulty_dimensions: dict[str, float] = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def normalize_feedback_fields(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value

        feedback = _normalized_feedback_payload(value)
        updated = dict(value)
        updated["feedback_snapshot"] = feedback
        updated["selection_state"] = feedback["selection_state"]
        updated["review_like_risk"] = feedback["review_like_risk"]
        updated["repair_suggested"] = feedback["repair_suggested"]
        updated["final_candidate_score"] = feedback["final_candidate_score"]
        updated["readiness_score"] = feedback["readiness_score"]
        updated["total_penalty"] = feedback["total_penalty"]
        updated["decision_reason"] = feedback["decision_reason"]
        updated["repair_reason"] = feedback["repair_reason"]
        updated["quality_note"] = feedback["quality_note"]
        updated["difficulty_band_hint"] = feedback["difficulty_band_hint"]
        updated["difficulty_vector"] = feedback["difficulty_vector"]
        updated["key_penalties"] = feedback["key_penalties"]
        updated["key_difficulty_dimensions"] = feedback["key_difficulty_dimensions"]
        return updated


class QuestionGenerationBatchResponse(BaseModel):
    batch_id: str
    batch_meta: BatchMeta
    items: list[QuestionGenerationItem] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)


class QuestionReviewActionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    action: Literal["minor_edit", "question_modify", "text_modify", "manual_edit", "approve", "confirm", "discard"]
    requested_action: str | None = None
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
