from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class SlotFieldConfig(BaseModel):
    type: Literal["string", "integer", "number", "boolean", "array", "object"]
    required: bool = False
    default: Any = None
    allowed: list[Any] | None = None
    description: str = ""


class FewshotPolicyConfig(BaseModel):
    enabled: bool = True
    bind_to_difficulty: bool = False
    max_examples: int = 1
    mode: str = "structure_only"

    @model_validator(mode="after")
    def validate_policy(self) -> "FewshotPolicyConfig":
        if self.bind_to_difficulty:
            raise ValueError("fewshot_policy.bind_to_difficulty must remain false")
        if self.mode != "structure_only":
            raise ValueError("fewshot_policy.mode must be structure_only")
        if self.max_examples != 1:
            raise ValueError("fewshot_policy.max_examples must be 1")
        return self


class DifficultyMetricRule(BaseModel):
    base: float | None = None
    by_slot: dict[str, dict[str, float]] = Field(default_factory=dict)
    by_text: dict[str, dict[str, float]] = Field(default_factory=dict)


class DifficultyRulesConfig(BaseModel):
    complexity: DifficultyMetricRule = Field(default_factory=DifficultyMetricRule)
    ambiguity: DifficultyMetricRule = Field(default_factory=DifficultyMetricRule)
    reasoning_depth: DifficultyMetricRule = Field(default_factory=DifficultyMetricRule)
    distractor_similarity: DifficultyMetricRule = Field(default_factory=DifficultyMetricRule)


class DifficultyRangeConfig(BaseModel):
    min: float
    max: float

    @model_validator(mode="after")
    def validate_range(self) -> "DifficultyRangeConfig":
        if self.min > self.max:
            raise ValueError("difficulty range min cannot be greater than max")
        return self


class DifficultyTargetProfileConfig(BaseModel):
    complexity: DifficultyRangeConfig
    ambiguity: DifficultyRangeConfig
    reasoning_depth: DifficultyRangeConfig
    distractor_similarity: DifficultyRangeConfig


class ControlLogicConfig(BaseModel):
    difficulty_source: Any
    option_confusion: Any
    control_levers: dict[str, Any]
    special_fields: dict[str, Any] = Field(default_factory=dict)


class GenerationLogicConfig(BaseModel):
    generation_core: Any
    processing_type: Any
    correct_logic: Any
    high_freq_traps: list[Any] = Field(default_factory=list)
    distractor_pattern: Any = Field(default_factory=list)
    analysis_steps: Any = Field(default_factory=list)


class FewshotExampleConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    title: str | None = None
    content: str | None = None
    preferred_patterns: list[str] = Field(default_factory=list)
    fit_slots: dict[str, Any] = Field(default_factory=dict)
    input_brief: str | None = None
    question_brief: str | None = None
    options_brief: list[str] = Field(default_factory=list)
    answer: str | None = None
    rationale_brief: str | None = None
    input: str | None = None
    output: str | None = None
    note: str | None = None


class PatternConfig(BaseModel):
    pattern_id: str
    pattern_name: str
    enabled: bool = True
    match_rules: dict[str, Any] = Field(default_factory=dict)
    control_logic: ControlLogicConfig
    generation_logic: GenerationLogicConfig
    difficulty_rules: DifficultyRulesConfig = Field(default_factory=DifficultyRulesConfig)
    fewshot_example: FewshotExampleConfig | None = None
    fewshot_examples: list[FewshotExampleConfig] = Field(default_factory=list)


class BusinessSubtypeConfig(BaseModel):
    subtype_id: str
    display_name: str
    description: str
    preferred_patterns: list[str] = Field(default_factory=list)
    default_slot_overrides: dict[str, Any] = Field(default_factory=dict)
    fewshot_policy: FewshotPolicyConfig | None = None
    fewshot_example: FewshotExampleConfig | None = None
    fewshot_examples: list[FewshotExampleConfig] = Field(default_factory=list)


class QuestionTypeConfig(BaseModel):
    type_id: str
    display_name: str
    task_definition: str | None = None
    enabled: bool = True
    aliases: list[str] = Field(default_factory=list)
    skeleton: dict[str, Any]
    slot_schema: dict[str, SlotFieldConfig]
    default_slots: dict[str, Any] = Field(default_factory=dict)
    fewshot_policy: FewshotPolicyConfig = Field(default_factory=FewshotPolicyConfig)
    default_fewshot: list[FewshotExampleConfig] = Field(default_factory=list)
    patterns: list[PatternConfig]
    default_pattern_id: str | None = None
    difficulty_target_profiles: dict[str, DifficultyTargetProfileConfig] = Field(default_factory=dict)
    business_subtypes: list[BusinessSubtypeConfig] = Field(default_factory=list)

    @field_validator("default_fewshot", mode="before")
    @classmethod
    def normalize_default_fewshot(cls, value: Any) -> list[dict[str, Any]]:
        if value is None:
            return []
        if isinstance(value, list):
            return value
        if isinstance(value, dict):
            return [value]
        raise ValueError("default_fewshot must be a dict or list")

    @field_validator("patterns", mode="before")
    @classmethod
    def normalize_pattern_fewshots(cls, value: Any) -> Any:
        if not isinstance(value, list):
            return value
        normalized = []
        for item in value:
            pattern = dict(item)
            if pattern.get("fewshot_examples") is None:
                pattern["fewshot_examples"] = []
            elif isinstance(pattern["fewshot_examples"], dict):
                pattern["fewshot_examples"] = [pattern["fewshot_examples"]]
            normalized.append(pattern)
        return normalized

    @field_validator("business_subtypes", mode="before")
    @classmethod
    def normalize_business_subtype_fewshots(cls, value: Any) -> Any:
        if not isinstance(value, list):
            return value
        normalized = []
        for item in value:
            subtype = dict(item)
            if subtype.get("fewshot_examples") is None:
                subtype["fewshot_examples"] = []
            elif isinstance(subtype["fewshot_examples"], dict):
                subtype["fewshot_examples"] = [subtype["fewshot_examples"]]
            normalized.append(subtype)
        return normalized

    @model_validator(mode="after")
    def validate_defaults(self) -> "QuestionTypeConfig":
        schema_keys = set(self.slot_schema.keys())
        unknown_defaults = set(self.default_slots.keys()) - schema_keys
        if unknown_defaults:
            unknown_str = ", ".join(sorted(unknown_defaults))
            raise ValueError(f"default_slots contain unknown keys: {unknown_str}")

        enabled_patterns = {pattern.pattern_id for pattern in self.patterns if pattern.enabled}
        if not enabled_patterns:
            raise ValueError("at least one enabled pattern is required")

        if self.default_pattern_id and self.default_pattern_id not in enabled_patterns:
            raise ValueError("default_pattern_id must point to an enabled pattern")

        required_profiles = {"easy", "medium", "hard"}
        if set(self.difficulty_target_profiles.keys()) != required_profiles:
            raise ValueError("difficulty_target_profiles must include easy, medium, and hard")

        subtype_ids: set[str] = set()
        for subtype in self.business_subtypes:
            if subtype.subtype_id in subtype_ids:
                raise ValueError(f"duplicate business subtype: {subtype.subtype_id}")
            subtype_ids.add(subtype.subtype_id)

            unknown_overrides = set(subtype.default_slot_overrides.keys()) - schema_keys
            if unknown_overrides:
                unknown_str = ", ".join(sorted(unknown_overrides))
                raise ValueError(
                    f"business_subtype '{subtype.subtype_id}' has unknown default_slot_overrides: {unknown_str}"
                )

            unknown_patterns = set(subtype.preferred_patterns) - enabled_patterns
            if unknown_patterns:
                unknown_str = ", ".join(sorted(unknown_patterns))
                raise ValueError(
                    f"business_subtype '{subtype.subtype_id}' has unknown preferred_patterns: {unknown_str}"
                )

        return self
