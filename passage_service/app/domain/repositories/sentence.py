from typing import Protocol

from app.infra.db.orm.sentence import SentenceORM


class SentenceRepository(Protocol):
    def replace_for_article(self, article_id: str, sentences: list[dict]) -> list[SentenceORM]: ...
    def list_by_article(self, article_id: str) -> list[SentenceORM]: ...
