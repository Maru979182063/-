from app.domain.services._common import ServiceBase


class ReviewService(ServiceBase):
    def init_review(self, material_id: str, status: str) -> object:
        review = self.review_repo.init_review(material_id, status)
        self.audit_repo.log("material", material_id, "review_init", {"status": status})
        return review
