from datetime import datetime

from sqlalchemy import DateTime, Float, ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from app.core.clock import utc_now
from app.infra.db.base import Base


class FeedbackRecordORM(Base):
    __tablename__ = "feedback_records"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    material_id: Mapped[str] = mapped_column(ForeignKey("material_spans.id"), index=True)
    source_service: Mapped[str] = mapped_column(String)
    feedback_type: Mapped[str] = mapped_column(String, index=True)
    feedback_value: Mapped[str | None] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)


class FeedbackAggregateORM(Base):
    __tablename__ = "feedback_aggregates"

    material_id: Mapped[str] = mapped_column(ForeignKey("material_spans.id"), primary_key=True)
    accept_rate: Mapped[float] = mapped_column(Float, default=0.0)
    type_match_score: Mapped[float] = mapped_column(Float, default=0.0)
    difficulty_match_score: Mapped[float] = mapped_column(Float, default=0.0)
    bad_case_count: Mapped[int] = mapped_column(Integer, default=0)
    last_feedback_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
