from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class PromptTemplateRecord(BaseModel):
    template_id: str
    template_name: str
    template_version: str
    question_type: str
    business_subtype: str | None = None
    action_type: Literal["generate", "minor_edit", "question_modify", "text_modify", "judge_review"]
    content: str
    is_active: bool = True
    created_at: str | None = None


class PromptTemplateListResponse(BaseModel):
    count: int
    items: list[PromptTemplateRecord] = Field(default_factory=list)


class PromptTemplateGroupResponse(BaseModel):
    template_name: str
    items: list[PromptTemplateRecord] = Field(default_factory=list)
