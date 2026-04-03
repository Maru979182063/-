from fastapi import APIRouter, Depends, Query

from app.core.dependencies import get_question_repository
from app.schemas.review import (
    DeliveryBatchResponse,
    ReviewBatchDetailResponse,
    ReviewBatchListResponse,
    ReviewDiffResponse,
    ReviewItemHistoryResponse,
    ReviewItemListResponse,
)
from app.services.question_repository import QuestionRepository
from app.services.review_query_service import ReviewQueryService
from app.services.delivery_service import DeliveryService

router = APIRouter(prefix="/api/v1/review", tags=["review"])


@router.get("/items", response_model=ReviewItemListResponse)
def list_review_items(
    status: str | None = None,
    question_type: str | None = None,
    business_subtype: str | None = None,
    batch_id: str | None = None,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    keyword: str | None = None,
    repository: QuestionRepository = Depends(get_question_repository),
) -> ReviewItemListResponse:
    service = ReviewQueryService(repository)
    return ReviewItemListResponse.model_validate(
        service.list_items(
            status=status,
            question_type=question_type,
            business_subtype=business_subtype,
            batch_id=batch_id,
            page=page,
            page_size=page_size,
            keyword=keyword,
        )
    )


@router.get("/batches", response_model=ReviewBatchListResponse)
def list_review_batches(
    status: str | None = None,
    created_by: str | None = None,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    repository: QuestionRepository = Depends(get_question_repository),
) -> ReviewBatchListResponse:
    service = ReviewQueryService(repository)
    return ReviewBatchListResponse.model_validate(
        service.list_batches(status=status, created_by=created_by, page=page, page_size=page_size)
    )


@router.get("/batches/{batch_id}", response_model=ReviewBatchDetailResponse)
def get_review_batch_detail(
    batch_id: str,
    repository: QuestionRepository = Depends(get_question_repository),
) -> ReviewBatchDetailResponse:
    service = ReviewQueryService(repository)
    return ReviewBatchDetailResponse.model_validate(service.get_batch_detail(batch_id))


@router.get("/items/{item_id}/history", response_model=ReviewItemHistoryResponse)
def get_review_item_history(
    item_id: str,
    repository: QuestionRepository = Depends(get_question_repository),
) -> ReviewItemHistoryResponse:
    service = ReviewQueryService(repository)
    return ReviewItemHistoryResponse.model_validate(service.get_item_history(item_id))


@router.get("/items/{item_id}/diff", response_model=ReviewDiffResponse)
def get_review_item_diff(
    item_id: str,
    from_version: int = Query(..., ge=1),
    to_version: int = Query(..., ge=1),
    repository: QuestionRepository = Depends(get_question_repository),
) -> ReviewDiffResponse:
    service = ReviewQueryService(repository)
    return ReviewDiffResponse.model_validate(service.get_item_diff(item_id, from_version, to_version))


@router.get("/batches/{batch_id}/delivery", response_model=DeliveryBatchResponse)
def get_batch_delivery(
    batch_id: str,
    repository: QuestionRepository = Depends(get_question_repository),
) -> DeliveryBatchResponse:
    return DeliveryBatchResponse.model_validate(DeliveryService(repository).get_batch_delivery(batch_id))


@router.get("/batches/{batch_id}/delivery/export")
def export_batch_delivery(
    batch_id: str,
    format: str = "json",
    repository: QuestionRepository = Depends(get_question_repository),
):
    service = DeliveryService(repository)
    if format == "markdown":
        return service.export_markdown(batch_id)
    return DeliveryBatchResponse.model_validate(service.get_batch_delivery(batch_id))
