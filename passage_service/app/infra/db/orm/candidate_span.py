from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.core.clock import utc_now
from app.infra.db.base import Base


class CandidateSpanORM(Base):
    __tablename__ = "candidate_spans"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    article_id: Mapped[str] = mapped_column(ForeignKey("articles.id"), index=True)
    start_paragraph: Mapped[int] = mapped_column(Integer)
    end_paragraph: Mapped[int] = mapped_column(Integer)
    start_sentence: Mapped[int | None] = mapped_column(Integer, nullable=True)
    end_sentence: Mapped[int | None] = mapped_column(Integer, nullable=True)
    span_type: Mapped[str] = mapped_column(String)
    text: Mapped[str] = mapped_column(Text)
    generated_by: Mapped[str] = mapped_column(String)
    status: Mapped[str] = mapped_column(String, index=True)
    segmentation_version: Mapped[str] = mapped_column(String)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, onupdate=utc_now)
