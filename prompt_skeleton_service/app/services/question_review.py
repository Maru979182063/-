from __future__ import annotations

import logging
from uuid import uuid4

from app.core.exceptions import DomainError
from app.schemas.question import QuestionReviewActionRequest
from app.services.question_generation import QuestionGenerationService
from app.services.question_repository import QuestionRepository


logger = logging.getLogger(__name__)


class QuestionReviewService:
    def __init__(self, repository: QuestionRepository, generation_service: QuestionGenerationService) -> None:
        self.repository = repository
        self.generation_service = generation_service

    def apply_action(self, item_id: str, request: QuestionReviewActionRequest) -> dict:
        item = self.repository.get_item(item_id)
        if item is None:
            raise DomainError(
                "Question item not found.",
                status_code=404,
                details={"item_id": item_id},
            )

        statuses = item.get("statuses", {})
        from_status = statuses.get("review_status", "draft")
        from_version_no = int(item.get("current_version_no", 1))
        action_result = self._run_action(item, request)
        statuses = action_result.get("statuses", {})
        to_status = statuses.get("review_status", from_status)
        to_version_no = int(action_result.get("current_version_no", from_version_no))

        action_id = str(uuid4())
        action_payload = {
            "action_id": action_id,
            "item_id": item_id,
            "action": request.action,
            "patch": {
                "instruction": request.instruction,
                "control_overrides": request.control_overrides,
                "from_status": from_status,
                "to_status": to_status,
                "from_version_no": from_version_no,
                "to_version_no": to_version_no,
                "material_id": (action_result.get("material_selection") or {}).get("material_id"),
                "revision_count": action_result.get("revision_count", item.get("revision_count", 0)),
            },
        }
        version_record = action_result.pop("_version_record", None)
        if version_record is not None:
            self.repository.save_version(version_record)
        self.repository.save_item(action_result)
        self.repository.save_review_action(
            action_id,
            item_id,
            request.action,
            action_payload,
            from_version_no=from_version_no,
            to_version_no=to_version_no,
            result_status=action_result.get("current_status"),
            operator=request.operator or "system",
        )
        logger.info(
            "review_action_applied item_id=%s action=%s from_version=%s to_version=%s status=%s",
            item_id,
            request.action,
            from_version_no,
            to_version_no,
            action_result.get("current_status"),
        )
        return {"action_id": action_id, "action": request.action, "item": action_result}

    def _run_action(self, item: dict, request: QuestionReviewActionRequest) -> dict:
        if request.action in {"approve", "confirm"}:
            validation_result = item.get("validation_result") or {}
            if item.get("current_status") == "auto_failed" or validation_result.get("passed") is False:
                raise DomainError(
                    "Blocked questions cannot be confirmed before revision.",
                    status_code=422,
                    details={
                        "item_id": item.get("item_id"),
                        "current_status": item.get("current_status"),
                        "review_status": item.get("statuses", {}).get("review_status"),
                    },
                )
            item["statuses"]["review_status"] = "approved"
            item["current_status"] = "approved"
            item["latest_action"] = request.action
            item["latest_action_at"] = self.repository._utc_now()
            item["notes"] = item.get("notes", []) + [f"review_action:{request.action}"]
            return item
        if request.action == "discard":
            item["statuses"]["review_status"] = "rejected"
            item["current_status"] = "discarded"
            item["latest_action"] = "discard"
            item["latest_action_at"] = self.repository._utc_now()
            item["notes"] = item.get("notes", []) + ["review_action:discard"]
            return item
        if request.action == "minor_edit":
            item["current_status"] = "revising"
            return self.generation_service.revise_minor_edit(item, request.instruction)
        if request.action == "question_modify":
            item["current_status"] = "revising"
            return self.generation_service.revise_question_modify(item, request.instruction, request.control_overrides)
        if request.action == "text_modify":
            item["current_status"] = "revising"
            return self.generation_service.revise_text_modify(item, request.instruction, request.control_overrides)
        if request.action == "manual_edit":
            item["current_status"] = "revising"
            return self.generation_service.apply_manual_edit(item, request.instruction, request.control_overrides)
        raise DomainError(
            "Unsupported review action.",
            status_code=422,
            details={"action": request.action},
        )
