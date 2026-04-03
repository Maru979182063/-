from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class MaterialSpan(BaseModel):
    id: str
    article_id: str
    candidate_span_id: str
    text: str
    span_type: str
    length_bucket: str
    paragraph_count: int
    sentence_count: int
    status: str
    release_channel: str
    gray_ratio: float = 0.0
    gray_reason: str | None = None
    segmentation_version: str
    tag_version: str
    fit_version: str
    prompt_version: str | None = None
    primary_family: str | None = None
    primary_subtype: str | None = None
    secondary_subtypes: list[str] = Field(default_factory=list)
    universal_profile: dict[str, Any] = Field(default_factory=dict)
    family_scores: dict[str, float] = Field(default_factory=dict)
    family_profiles: dict[str, Any] = Field(default_factory=dict)
    subtype_candidates: list[dict[str, Any]] = Field(default_factory=list)
    secondary_candidates: list[dict[str, Any]] = Field(default_factory=list)
    primary_route: dict[str, Any] = Field(default_factory=dict)
    reject_reason: str | None = None
    quality_flags: list[str] = Field(default_factory=list)
    knowledge_tags: list[str] = Field(default_factory=list)
    fit_scores: dict[str, float] = Field(default_factory=dict)
    feature_profile: dict[str, Any] = Field(default_factory=dict)
    quality_score: float = 0.0
    usage_count: int = 0
    accept_count: int = 0
    reject_count: int = 0
    last_used_at: datetime | None = None
    created_at: datetime
    updated_at: datetime
