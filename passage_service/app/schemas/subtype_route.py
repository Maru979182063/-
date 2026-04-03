from pydantic import BaseModel, Field


class SubtypeCandidate(BaseModel):
    family: str
    subtype: str
    score: float


class PrimaryRoute(BaseModel):
    family: str | None = None
    subtype: str | None = None


class SubtypeRouteResult(BaseModel):
    subtype_candidates: list[SubtypeCandidate] = Field(default_factory=list)
    primary_route: PrimaryRoute = Field(default_factory=PrimaryRoute)
