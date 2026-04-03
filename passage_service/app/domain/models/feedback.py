from datetime import datetime

from pydantic import BaseModel


class FeedbackRecord(BaseModel):
    id: str
    material_id: str
    source_service: str
    feedback_type: str
    feedback_value: str | None = None
    created_at: datetime


class FeedbackAggregate(BaseModel):
    material_id: str
    accept_rate: float = 0.0
    type_match_score: float = 0.0
    difficulty_match_score: float = 0.0
    bad_case_count: int = 0
    last_feedback_at: datetime | None = None
