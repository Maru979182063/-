from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.domain.services.ingest_service import CrawlService


router = APIRouter()


@router.post("/crawl/run")
def run_all_sources(db: Session = Depends(get_db)) -> dict:
    return CrawlService(db).run_all_sources()


@router.post("/crawl/source/{source_id}/run")
def run_single_source(source_id: str, db: Session = Depends(get_db)) -> dict:
    return CrawlService(db).run_source(source_id)


@router.get("/crawl/jobs/{job_id}")
def get_job(job_id: str, db: Session = Depends(get_db)) -> dict:
    return CrawlService(db).get_job(job_id)
