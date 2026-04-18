from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from app.schemas.question import CenterUnderstandingExportView
from app.schemas.question import SentenceFillCanonicalExportView
from app.schemas.question import SentenceOrderCanonicalExportView


class ReviewItemSummary(BaseModel):
    item_id: str
    batch_id: str
    question_type: str
    business_subtype: str | None = None
    target_difficulty: str
    current_status: str
    current_version_no: int
    latest_action: str | None = None
    latest_action_at: str | None = None
    created_at: str | None = None
    updated_at: str
    stem_preview: str | None = None
    material_preview: str | None = None
    sentence_fill_export_view: SentenceFillCanonicalExportView | None = None
    center_understanding_export_view: CenterUnderstandingExportView | None = None
    sentence_order_export_view: SentenceOrderCanonicalExportView | None = None


class ReviewItemListResponse(BaseModel):
    count: int
    page: int
    page_size: int
    items: list[ReviewItemSummary] = Field(default_factory=list)


class ReviewBatchSummary(BaseModel):
    batch_id: str
    batch_status: str
    total_count: int
    pending_count: int
    approved_count: int
    discarded_count: int
    revising_count: int
    created_at: str | None = None
    updated_at: str
    created_by: str | None = None


class ReviewBatchListResponse(BaseModel):
    count: int
    page: int
    page_size: int
    items: list[ReviewBatchSummary] = Field(default_factory=list)


class ReviewBatchDetailResponse(BaseModel):
    batch_id: str
    batch_status: str
    total_count: int
    pending_count: int
    approved_count: int
    discarded_count: int
    revising_count: int
    created_at: str | None = None
    updated_at: str
    items: list[ReviewItemSummary] = Field(default_factory=list)


class ReviewVersionSummary(BaseModel):
    version_id: str
    version_no: int
    parent_version_no: int | None = None
    source_action: str
    current_status: str
    target_difficulty: str | None = None
    material_id: str | None = None
    material_preview: str | None = None
    material_text: str | None = None
    stem_preview: str | None = None
    stem: str | None = None
    options: dict[str, Any] = Field(default_factory=dict)
    answer: str | None = None
    analysis: str | None = None
    validation_result: dict[str, Any] = Field(default_factory=dict)
    evaluation_result: dict[str, Any] = Field(default_factory=dict)
    prompt_template_name: str | None = None
    prompt_template_version: str | None = None
    diff_summary: dict[str, Any] = Field(default_factory=dict)
    runtime_snapshot: dict[str, Any] = Field(default_factory=dict)
    created_at: str
    sentence_fill_export_view: SentenceFillCanonicalExportView | None = None
    center_understanding_export_view: CenterUnderstandingExportView | None = None
    sentence_order_export_view: SentenceOrderCanonicalExportView | None = None


class ReviewItemHistoryResponse(BaseModel):
    item: ReviewItemSummary
    current_version_no: int
    current_version: ReviewVersionSummary | None = None
    versions: list[ReviewVersionSummary] = Field(default_factory=list)
    review_actions: list[dict[str, Any]] = Field(default_factory=list)


class ReviewDiffResponse(BaseModel):
    item_id: str
    from_version: int
    to_version: int
    changed_fields: list[str] = Field(default_factory=list)
    material_changed: bool = False
    difficulty_changed: bool = False
    prompt_changed: bool = False
    stem_changed: bool = False
    options_changed: bool = False
    analysis_changed: bool = False
    old_summary: dict[str, Any] = Field(default_factory=dict)
    new_summary: dict[str, Any] = Field(default_factory=dict)


class DeliveryVersionRecord(BaseModel):
    item_id: str
    version_no: int
    question_type: str
    business_subtype: str | None = None
    difficulty_target: str | None = None
    stem: str | None = None
    options: dict[str, Any] = Field(default_factory=dict)
    answer: str | None = None
    analysis: str | None = None
    material_id: str | None = None
    document_genre: str | None = None
    prompt_template_name: str | None = None
    prompt_template_version: str | None = None
    sentence_fill_export_view: SentenceFillCanonicalExportView | None = None
    sentence_order_export_view: SentenceOrderCanonicalExportView | None = None


class DeliveryBatchResponse(BaseModel):
    batch_id: str
    batch_status: str
    total_count: int
    approved_count: int
    exported_count: int
    items: list[DeliveryVersionRecord] = Field(default_factory=list)
