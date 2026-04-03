from datetime import datetime

from pydantic import BaseModel


class CandidateSpan(BaseModel):
    id: str
    article_id: str
    start_paragraph: int
    end_paragraph: int
    start_sentence: int | None = None
    end_sentence: int | None = None
    span_type: str
    text: str
    generated_by: str
    status: str
    segmentation_version: str
    created_at: datetime
    updated_at: datetime
