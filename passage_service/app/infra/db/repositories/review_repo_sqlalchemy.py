from sqlalchemy import select
from sqlalchemy.orm import Session

from app.infra.db.orm.review import TaggingReviewORM
from app.infra.db.repositories.utils import new_id


class SQLAlchemyReviewRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def init_review(self, material_id: str, status: str) -> TaggingReviewORM:
        existing = self.session.scalar(select(TaggingReviewORM).where(TaggingReviewORM.material_id == material_id))
        if existing is not None:
            return existing
        review = TaggingReviewORM(id=new_id("rev"), material_id=material_id, status=status)
        self.session.add(review)
        self.session.commit()
        self.session.refresh(review)
        return review

    def update_review(self, material_id: str, status: str, reviewer: str | None, comment: str | None) -> TaggingReviewORM:
        review = self.session.scalar(select(TaggingReviewORM).where(TaggingReviewORM.material_id == material_id))
        if review is None:
            review = TaggingReviewORM(id=new_id("rev"), material_id=material_id, status=status, reviewer=reviewer, comment=comment)
            self.session.add(review)
        else:
            review.status = status
            review.reviewer = reviewer
            review.comment = comment
        self.session.commit()
        self.session.refresh(review)
        return review
