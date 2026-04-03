from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from app.infra.db.orm.sentence import SentenceORM
from app.infra.db.repositories.utils import new_id


class SQLAlchemySentenceRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def replace_for_article(self, article_id: str, sentences: list[dict]) -> list[SentenceORM]:
        self.session.execute(delete(SentenceORM).where(SentenceORM.article_id == article_id))
        records = [SentenceORM(id=new_id("sent"), article_id=article_id, **payload) for payload in sentences]
        self.session.add_all(records)
        self.session.commit()
        return records

    def list_by_article(self, article_id: str) -> list[SentenceORM]:
        return list(self.session.scalars(select(SentenceORM).where(SentenceORM.article_id == article_id).order_by(SentenceORM.paragraph_index, SentenceORM.sentence_index)))
