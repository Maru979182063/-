from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column

from app.core.clock import utc_now
from app.infra.db.base import Base


class TaggingReviewORM(Base):
    __tablename__ = "tagging_reviews"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    material_id: Mapped[str] = mapped_column(ForeignKey("material_spans.id"), unique=True, index=True)
    status: Mapped[str] = mapped_column(String, index=True)
    reviewer: Mapped[str | None] = mapped_column(String, nullable=True)
    comment: Mapped[str | None] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, onupdate=utc_now)
