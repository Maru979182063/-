from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.core.exceptions import ConflictError
from app.domain.services.dify_export_service import DifyExportService
from app.domain.services.pool_service import PoolService
from app.domain.services.reprocess_service import ReprocessService


router = APIRouter()


class MaterialSearchRequest(BaseModel):
    primary_family: str | None = None
    subtype: str | None = None
    document_genre: str | None = None
    material_structure_label: str | None = None
    fit_score_threshold: float | None = None
    domain: str | None = None
    length_bucket: str | None = None
    status: str | None = None
    release_channel: str | None = None


class PromoteRequest(BaseModel):
    material_id: str
    status: str = "promoted"
    release_channel: str = "stable"


class ReprocessRequest(BaseModel):
    article_ids: list[str] = Field(default_factory=list)
    material_ids: list[str] = Field(default_factory=list)
    segmentation_version: str | None = None
    tag_version: str | None = None
    fit_version: str | None = None
    force_gray: bool = True


class DifyExportRequest(BaseModel):
    article_ids: list[str] = Field(default_factory=list)
    output_dir: str | None = None
    limit: int | None = None
    include_gray: bool = True


@router.post("/materials/search")
def search_materials(payload: MaterialSearchRequest, db: Session = Depends(get_db)) -> dict:
    return {"items": PoolService(db).search(payload.model_dump(exclude_none=True))}


@router.get("/materials/stats")
def get_material_stats(
    status: str | None = None,
    release_channel: str | None = None,
    db: Session = Depends(get_db),
) -> dict:
    return PoolService(db).get_pool_stats(status=status, release_channel=release_channel)


@router.post("/materials/promote")
def promote_material(payload: PromoteRequest, db: Session = Depends(get_db)) -> dict:
    try:
        item = PoolService(db).promote(payload.material_id, payload.status, payload.release_channel)
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return {"material_id": item.id, "status": item.status, "release_channel": item.release_channel}


@router.post("/materials/reprocess")
def reprocess_materials(payload: ReprocessRequest, db: Session = Depends(get_db)) -> dict:
    return ReprocessService(db).reprocess(payload.model_dump())


@router.post("/materials/export/dify-pack")
def export_dify_pack(payload: DifyExportRequest, db: Session = Depends(get_db)) -> dict:
    return DifyExportService(db).export_materials(
        article_ids=payload.article_ids or None,
        output_dir=payload.output_dir,
        limit=payload.limit,
        include_gray=payload.include_gray,
    )
