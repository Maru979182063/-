from pydantic import BaseModel, Field


class FeatureProfile(BaseModel):
    structure_hints: list[str] = Field(default_factory=list)
    logic_relations: list[str] = Field(default_factory=list)
    position_roles: list[str] = Field(default_factory=list)
    single_center_strength: float = 0.0
    conclusion_sentence_strength: float = 0.0
    transition_strength: float = 0.0
    summary_strength: float = 0.0
    explanation_strength: float = 0.0
    ordering_anchor_strength: float = 0.0
    story_completeness_strength: float = 0.0
    distractor_space: float = 0.0
    independence_score: float = 0.0
    question_worthiness_score: float = 0.0
