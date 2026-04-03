from typing import Protocol

from app.infra.db.orm.paragraph import ParagraphORM


class ParagraphRepository(Protocol):
    def replace_for_article(self, article_id: str, paragraphs: list[dict]) -> list[ParagraphORM]: ...
    def list_by_article(self, article_id: str) -> list[ParagraphORM]: ...
