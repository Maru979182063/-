from app.domain.services._common import ServiceBase
from app.domain.services.segment_service import SegmentService
from app.domain.services.tag_service import TagService


class ReprocessService(ServiceBase):
    def reprocess(self, payload: dict) -> dict:
        article_ids = payload.get("article_ids", [])
        processed: list[str] = []
        for article_id in article_ids:
            SegmentService(self.session).segment(article_id)
            TagService(self.session).tag_article(article_id)
            processed.append(article_id)
        return {"processed_article_ids": processed, "force_gray": payload.get("force_gray", True)}
