from pydantic import BaseModel, Field


class TextShape(BaseModel):
    length_bucket: str
    paragraph_count: int
    sentence_count: int


class UniversalProfile(BaseModel):
    text_shape: TextShape
    document_genre: str | None = None
    document_genre_candidates: list[str] = Field(default_factory=list)
    material_structure_label: str | None = None
    material_structure_reason: str | None = None
    structure_hints: list[str] = Field(default_factory=list)
    logic_relations: list[str] = Field(default_factory=list)
    position_roles: list[str] = Field(default_factory=list)
    standalone_readability: float = 0.0
    single_center_strength: float = 0.0
    summary_strength: float = 0.0
    transition_strength: float = 0.0
    explanation_strength: float = 0.0
    ordering_anchor_strength: float = 0.0
    continuation_openness: float = 0.0
    direction_uniqueness: float = 0.0
    titleability: float = 0.0
    value_judgement_strength: float = 0.0
    example_to_theme_strength: float = 0.0
    problem_signal_strength: float = 0.0
    method_signal_strength: float = 0.0
    branch_focus_strength: float = 0.0
    independence_score: float = 0.0
