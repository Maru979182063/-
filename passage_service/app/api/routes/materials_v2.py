from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.domain.services.material_pipeline_v2_service import MaterialPipelineV2Service
from app.schemas.material_pipeline_v2 import MaterialV2SearchRequest


router = APIRouter()


class MaterialV2PrecomputeRequest(BaseModel):
    material_ids: list[str] = Field(default_factory=list)
    article_ids: list[str] = Field(default_factory=list)
    status: str | None = None
    release_channel: str | None = None
    primary_only: bool = True
    limit: int | None = Field(default=None, ge=1, le=5000)


@router.post("/materials/v2/search")
def search_materials_v2(payload: MaterialV2SearchRequest, db: Session = Depends(get_db)) -> dict:
    return MaterialPipelineV2Service(db).search(payload.model_dump(exclude_none=True))


@router.post("/materials/v2/precompute")
def precompute_materials_v2(payload: MaterialV2PrecomputeRequest, db: Session = Depends(get_db)) -> dict:
    return MaterialPipelineV2Service(db).precompute(payload.model_dump(exclude_none=True))
