from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class MetaSubtypeSummary(BaseModel):
    subtype_id: str
    display_name: str
    description: str


class MetaQuestionTypeSummary(BaseModel):
    question_type: str
    display_name: str
    task_definition: str | None = None
    business_subtypes: list[MetaSubtypeSummary] = Field(default_factory=list)


class MetaQuestionTypeListResponse(BaseModel):
    count: int
    items: list[MetaQuestionTypeSummary] = Field(default_factory=list)


class ControlOption(BaseModel):
    value: Any
    label: str


class ControlMetadata(BaseModel):
    control_key: str
    label: str
    control_type: str
    options: list[ControlOption] = Field(default_factory=list)
    default_value: Any = None
    required: bool = False
    affects_difficulty: bool = False
    editable_by: str = "generator_and_reviewer"
    mapped_action: str = "question_modify"
    read_only: bool = False
    description: str = ""


class ControlMetadataResponse(BaseModel):
    question_type: str
    controls: list[ControlMetadata] = Field(default_factory=list)
