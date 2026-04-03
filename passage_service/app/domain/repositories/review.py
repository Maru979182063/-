from typing import Protocol

from app.infra.db.orm.review import TaggingReviewORM


class ReviewRepository(Protocol):
    def init_review(self, material_id: str, status: str) -> TaggingReviewORM: ...
    def update_review(self, material_id: str, status: str, reviewer: str | None, comment: str | None) -> TaggingReviewORM: ...
