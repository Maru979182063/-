from pydantic import BaseModel, Field


class FamilyScores(BaseModel):
    family_scores: dict[str, float] = Field(default_factory=dict)
    primary_family: str | None = None
    secondary_families: list[str] = Field(default_factory=list)
