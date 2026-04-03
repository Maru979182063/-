from sqlalchemy.orm import Session

from app.core.clock import utc_now
from app.infra.db.orm.feedback import FeedbackAggregateORM, FeedbackRecordORM
from app.infra.db.repositories.utils import new_id


class SQLAlchemyFeedbackRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def create_record(self, **kwargs) -> FeedbackRecordORM:
        record = FeedbackRecordORM(id=new_id("fb"), **kwargs)
        self.session.add(record)
        self.session.commit()
        self.session.refresh(record)
        return record

    def get_aggregate(self, material_id: str) -> FeedbackAggregateORM | None:
        return self.session.get(FeedbackAggregateORM, material_id)

    def upsert_aggregate(self, material_id: str, **kwargs) -> FeedbackAggregateORM:
        aggregate = self.session.get(FeedbackAggregateORM, material_id)
        if aggregate is None:
            aggregate = FeedbackAggregateORM(material_id=material_id, **kwargs)
            self.session.add(aggregate)
        else:
            for key, value in kwargs.items():
                setattr(aggregate, key, value)
        aggregate.last_feedback_at = kwargs.get("last_feedback_at", utc_now())
        self.session.commit()
        self.session.refresh(aggregate)
        return aggregate
