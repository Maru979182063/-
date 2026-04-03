from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.domain.services.ingest_service import IngestService
from app.domain.services.process_service import ProcessService
from app.domain.services.review_export_service import ReviewExportService
from app.domain.services.segment_service import SegmentService
from app.domain.services.tag_service import TagService


router = APIRouter()


class ArticleIngestRequest(BaseModel):
    source: str
    source_url: str
    title: str | None = None
    raw_text: str
    language: str = "zh"
    domain: str | None = None


class ArticleProcessRequest(BaseModel):
    mode: str = "full"


@router.get("/articles")
def list_articles(limit: int = 100, db: Session = Depends(get_db)) -> dict:
    repo = IngestService(db).article_repo
    articles = repo.list(limit=limit)
    return {
        "count": len(articles),
        "items": [
            {
                "article_id": article.id,
                "source": article.source,
                "source_url": article.source_url,
                "title": article.title,
                "domain": article.domain,
                "status": article.status,
                "created_at": article.created_at.isoformat() if article.created_at else None,
                "updated_at": article.updated_at.isoformat() if article.updated_at else None,
            }
            for article in articles
        ],
    }


@router.post("/articles/ingest")
def ingest_article(payload: ArticleIngestRequest, db: Session = Depends(get_db)) -> dict:
    article = IngestService(db).ingest(payload.model_dump())
    return {"article_id": article.id, "status": article.status}


@router.post("/articles/{article_id}/segment")
def segment_article(article_id: str, db: Session = Depends(get_db)) -> dict:
    return SegmentService(db).segment(article_id)


@router.post("/articles/{article_id}/tag")
def tag_article(article_id: str, db: Session = Depends(get_db)) -> dict:
    return TagService(db).tag_article(article_id)


@router.post("/articles/{article_id}/process")
def process_article(article_id: str, payload: ArticleProcessRequest, db: Session = Depends(get_db)) -> dict:
    return ProcessService(db).process_article(article_id, payload.mode)


@router.post("/articles/{article_id}/review-export")
def export_article_review(article_id: str, db: Session = Depends(get_db)) -> dict:
    return ReviewExportService(db).export_article_review(article_id)
