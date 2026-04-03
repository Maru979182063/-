from abc import ABC, abstractmethod
from typing import Any

from pydantic import BaseModel, Field


class RunContext(BaseModel):
    segmentation_version: str | None = None
    tag_version: str | None = None
    fit_version: str | None = None
    knowledge_tree_version: str | None = None
    extra: dict[str, Any] = Field(default_factory=dict)


class TaggingResult(BaseModel):
    keep: bool = True
    boundary_adjustment: dict[str, int] = Field(default_factory=dict)
    feature_profile: dict[str, Any] = Field(default_factory=dict)
    reasons: list[str] = Field(default_factory=list)
    hits: list[dict[str, Any]] = Field(default_factory=list)
    review_status: str = "auto_tagged"
    extra: dict[str, Any] = Field(default_factory=dict)


class ControlProfile(BaseModel):
    plugin_name: str
    payload: dict[str, Any] = Field(default_factory=dict)


class PromptBundle(BaseModel):
    plugin_name: str
    payload: dict[str, Any] = Field(default_factory=dict)


class ValidationResult(BaseModel):
    valid: bool
    issues: list[str] = Field(default_factory=list)
    extra: dict[str, Any] = Field(default_factory=dict)


class BaseTagger(ABC):
    name: str
    version: str

    @abstractmethod
    def tag(
        self,
        candidate_span: dict[str, Any],
        article_context: dict[str, Any],
        node_config: dict[str, Any],
        run_context: RunContext,
    ) -> TaggingResult:
        ...


class BaseController(ABC):
    name: str
    version: str

    @abstractmethod
    def build_control_profile(
        self,
        material_span: dict[str, Any],
        request_payload: dict[str, Any],
        run_context: RunContext,
    ) -> ControlProfile:
        ...


class BaseBuilder(ABC):
    name: str
    version: str

    @abstractmethod
    def build_prompt_bundle(
        self,
        material_span: dict[str, Any],
        control_profile: ControlProfile,
        run_context: RunContext,
    ) -> PromptBundle:
        ...

    @abstractmethod
    def validate_output(
        self,
        output_payload: dict[str, Any],
        run_context: RunContext,
    ) -> ValidationResult:
        ...
