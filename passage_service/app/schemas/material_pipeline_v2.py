from __future__ import annotations

from pydantic import BaseModel, Field


class MaterialV2SearchRequest(BaseModel):
    business_family_id: str
    question_card_id: str | None = None
    article_ids: list[str] = Field(default_factory=list)
    business_card_ids: list[str] = Field(default_factory=list)
    preferred_business_card_ids: list[str] = Field(default_factory=list)
    query_terms: list[str] = Field(default_factory=list)
    topic: str | None = None
    text_direction: str | None = None
    document_genre: str | None = None
    material_structure_label: str | None = None
    article_limit: int = Field(default=10, ge=1, le=50)
    candidate_limit: int = Field(default=20, ge=1, le=100)
    min_card_score: float = Field(default=0.55, ge=0.0, le=1.0)
    min_business_card_score: float = Field(default=0.45, ge=0.0, le=1.0)
    target_length: int | None = Field(default=None, ge=80, le=1600)
    length_tolerance: int = Field(default=120, ge=0, le=600)
    structure_constraints: dict = Field(default_factory=dict)
    enable_anchor_adaptation: bool = True
    preserve_anchor: bool = True
    review_gate_mode: str = "stable_relaxed"
