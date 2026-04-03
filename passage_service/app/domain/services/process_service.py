from app.domain.services._common import ServiceBase
from app.domain.services.review_export_service import ReviewExportService
from app.domain.services.segment_service import SegmentService
from app.domain.services.tag_service import TagService


class ProcessService(ServiceBase):
    def process_article(self, article_id: str, mode: str = "full") -> dict:
        if mode == "segment_only":
            segment_result = SegmentService(self.session).segment(article_id)
            return {
                "article_id": article_id,
                "mode": mode,
                "segment": segment_result,
            }

        if mode == "tag_only":
            tag_result = TagService(self.session).tag_article(article_id)
            return {
                "article_id": article_id,
                "mode": mode,
                "tag": tag_result,
            }

        segment_result = SegmentService(self.session).segment(article_id)
        tag_result = TagService(self.session).tag_article(article_id)
        review_export = ReviewExportService(self.session).export_article_review(article_id)
        return {
            "article_id": article_id,
            "mode": "full",
            "segment": segment_result,
            "tag": tag_result,
            "review_export": review_export,
        }
