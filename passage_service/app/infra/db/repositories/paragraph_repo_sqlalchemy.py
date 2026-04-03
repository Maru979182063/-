from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from app.infra.db.orm.paragraph import ParagraphORM
from app.infra.db.repositories.utils import new_id


class SQLAlchemyParagraphRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def replace_for_article(self, article_id: str, paragraphs: list[dict]) -> list[ParagraphORM]:
        self.session.execute(delete(ParagraphORM).where(ParagraphORM.article_id == article_id))
        records = [ParagraphORM(id=new_id("para"), article_id=article_id, **payload) for payload in paragraphs]
        self.session.add_all(records)
        self.session.commit()
        return records

    def list_by_article(self, article_id: str) -> list[ParagraphORM]:
        return list(self.session.scalars(select(ParagraphORM).where(ParagraphORM.article_id == article_id).order_by(ParagraphORM.paragraph_index)))
