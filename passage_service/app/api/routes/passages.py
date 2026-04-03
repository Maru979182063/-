from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.domain.services.pool_service import PoolService


router = APIRouter()


@router.get("/passages")
def list_passages(db: Session = Depends(get_db)) -> dict:
    return {"items": PoolService(db).search({})}


@router.get("/passages/{passage_id}")
def get_passage(passage_id: str, db: Session = Depends(get_db)) -> dict:
    return {"item": PoolService(db).get_material(passage_id)}
