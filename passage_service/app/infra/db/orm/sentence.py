from sqlalchemy import ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.infra.db.base import Base


class SentenceORM(Base):
    __tablename__ = "sentences"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    article_id: Mapped[str] = mapped_column(ForeignKey("articles.id"), index=True)
    paragraph_id: Mapped[str | None] = mapped_column(ForeignKey("paragraphs.id"), nullable=True)
    paragraph_index: Mapped[int] = mapped_column(Integer)
    sentence_index: Mapped[int] = mapped_column(Integer)
    text: Mapped[str] = mapped_column(Text)
