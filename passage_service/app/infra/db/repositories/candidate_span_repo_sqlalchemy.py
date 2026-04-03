from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from app.infra.db.orm.candidate_span import CandidateSpanORM
from app.infra.db.repositories.utils import new_id


class SQLAlchemyCandidateSpanRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def replace_for_article(self, article_id: str, spans: list[dict]) -> list[CandidateSpanORM]:
        self.session.execute(delete(CandidateSpanORM).where(CandidateSpanORM.article_id == article_id))
        records = [CandidateSpanORM(id=new_id("cand"), article_id=article_id, **payload) for payload in spans]
        self.session.add_all(records)
        self.session.commit()
        return records

    def list_by_article(self, article_id: str) -> list[CandidateSpanORM]:
        return list(self.session.scalars(select(CandidateSpanORM).where(CandidateSpanORM.article_id == article_id)))

    def list_new(self, article_id: str) -> list[CandidateSpanORM]:
        return list(self.session.scalars(select(CandidateSpanORM).where(CandidateSpanORM.article_id == article_id, CandidateSpanORM.status == "new")))

    def mark_status(self, span_id: str, status: str) -> CandidateSpanORM:
        span = self.session.get(CandidateSpanORM, span_id)
        span.status = status
        self.session.commit()
        self.session.refresh(span)
        return span

    def get(self, span_id: str) -> CandidateSpanORM | None:
        return self.session.get(CandidateSpanORM, span_id)
