from __future__ import annotations

from app.core.exceptions import DomainError
from app.services.delivery_service import build_center_understanding_export_view
from app.services.delivery_service import DeliveryService
from app.services.question_repository import QuestionRepository
from app.services.sentence_order_protocol import project_sentence_order_strict_export_view
from app.services.sentence_fill_protocol import project_sentence_fill_strict_export_view


class ReviewQueryService:
    def __init__(self, repository: QuestionRepository) -> None:
        self.repository = repository

    def list_items(
        self,
        *,
        status: str | None,
        question_type: str | None,
        business_subtype: str | None,
        batch_id: str | None,
        page: int,
        page_size: int,
        keyword: str | None,
    ) -> dict:
        items, total = self.repository.list_review_items(
            status=status,
            question_type=question_type,
            business_subtype=business_subtype,
            batch_id=batch_id,
            keyword=keyword,
            page=page,
            page_size=page_size,
        )
        return {
            "count": total,
            "page": page,
            "page_size": page_size,
            "items": [
                self._attach_sentence_order_review_view(
                    self._attach_center_understanding_review_view(self._attach_sentence_fill_review_view(item))
                )
                for item in items
            ],
        }

    def list_batches(self, *, status: str | None, created_by: str | None, page: int, page_size: int) -> dict:
        items, total = self.repository.list_review_batches(
            status=status,
            created_by=created_by,
            page=page,
            page_size=page_size,
        )
        return {"count": total, "page": page, "page_size": page_size, "items": items}

    def get_batch_detail(self, batch_id: str) -> dict:
        batch = self.repository.get_batch(batch_id)
        if batch is None:
            raise DomainError("Question batch not found.", status_code=404, details={"batch_id": batch_id})
        return {
            "batch_id": batch["batch_id"],
            "batch_status": batch["batch_status"],
            "total_count": batch["total_count"],
            "pending_count": batch["pending_count"],
            "approved_count": batch["approved_count"],
            "discarded_count": batch["discarded_count"],
            "revising_count": batch["revising_count"],
            "created_at": batch.get("created_at"),
            "updated_at": batch["updated_at"],
            "items": [
                self._attach_sentence_order_review_view(
                    self._attach_center_understanding_review_view(
                        self._attach_sentence_fill_review_view(
                            self.repository._item_to_review_summary(self.repository.get_item(item["item_id"]))  # noqa: SLF001
                        )
                    )
                )
                for item in batch.get("items", [])
            ],
        }

    def get_item_history(self, item_id: str) -> dict:
        history = self.repository.get_item_history(item_id)
        if history is None:
            raise DomainError("Question item not found.", status_code=404, details={"item_id": item_id})
        item = self.repository.get_item(item_id)
        sentence_fill_view = self._build_sentence_fill_review_view(item)
        center_view = self._build_center_understanding_review_view(item)
        sentence_order_view = self._build_sentence_order_review_view(item)
        history["item"] = self._attach_sentence_fill_review_view(history["item"])
        history["item"] = self._attach_center_understanding_review_view(history["item"])
        history["item"] = self._attach_sentence_order_review_view(history["item"])
        if history.get("current_version") is not None and sentence_fill_view is not None:
            history["current_version"]["sentence_fill_export_view"] = sentence_fill_view
        if history.get("current_version") is not None and center_view is not None:
            history["current_version"]["center_understanding_export_view"] = center_view
        if history.get("current_version") is not None and sentence_order_view is not None:
            history["current_version"]["sentence_order_export_view"] = sentence_order_view
        return history

    def get_item_diff(self, item_id: str, from_version: int, to_version: int) -> dict:
        diff = self.repository.get_version_pair_diff(item_id, from_version, to_version)
        if diff is None:
            raise DomainError(
                "Requested version pair was not found.",
                status_code=404,
                details={"item_id": item_id, "from_version": from_version, "to_version": to_version},
            )
        return diff

    def get_batch_delivery(self, batch_id: str) -> dict:
        return DeliveryService(self.repository).get_batch_delivery(batch_id)

    def _attach_sentence_fill_review_view(self, item_summary: dict) -> dict:
        payload = dict(item_summary)
        full_item = self.repository.get_item(payload["item_id"])
        view = self._build_sentence_fill_review_view(full_item)
        if view is not None:
            payload["sentence_fill_export_view"] = view
        return payload

    def _build_sentence_fill_review_view(self, item: dict | None) -> dict | None:
        view = project_sentence_fill_strict_export_view(item)
        if view:
            view.pop("field_results", None)
        return view

    def _attach_sentence_order_review_view(self, item_summary: dict) -> dict:
        payload = dict(item_summary)
        full_item = self.repository.get_item(payload["item_id"])
        view = self._build_sentence_order_review_view(full_item)
        if view is not None:
            payload["sentence_order_export_view"] = view
        return payload

    def _build_sentence_order_review_view(self, item: dict | None) -> dict | None:
        view = project_sentence_order_strict_export_view(item)
        if view:
            view.pop("field_results", None)
            view.pop("alias_trace", None)
        return view

    def _attach_center_understanding_review_view(self, item_summary: dict) -> dict:
        payload = dict(item_summary)
        full_item = self.repository.get_item(payload["item_id"])
        view = self._build_center_understanding_review_view(full_item)
        if view is not None:
            payload["center_understanding_export_view"] = view
            payload["business_subtype"] = view.get("business_subtype")
        return payload

    def _build_center_understanding_review_view(self, item: dict | None) -> dict | None:
        return build_center_understanding_export_view(item)
