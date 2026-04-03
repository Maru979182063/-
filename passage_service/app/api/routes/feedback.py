from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.domain.services.feedback_service import FeedbackService


router = APIRouter()


class FeedbackRequest(BaseModel):
    material_id: str
    source_service: str
    feedback_type: str
    feedback_value: str | None = None


@router.post("/materials/feedback")
def create_feedback(payload: FeedbackRequest, db: Session = Depends(get_db)) -> dict:
    return FeedbackService(db).record_feedback(payload.model_dump())
