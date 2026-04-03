from datetime import datetime

from pydantic import BaseModel


class TaggingReview(BaseModel):
    id: str
    material_id: str
    status: str
    reviewer: str | None = None
    comment: str | None = None
    created_at: datetime
    updated_at: datetime
