from __future__ import annotations

import logging
from copy import deepcopy
from typing import Any
from uuid import uuid4

from app.core.exceptions import DomainError
from app.schemas.question import QuestionReviewActionRequest
from app.services.question_generation import QuestionGenerationService
from app.services.question_repository import QuestionRepository


logger = logging.getLogger(__name__)


class QuestionReviewService:
    MATERIAL_INPUT_FIELDS = {"material_id", "material_text", "material_policy"}
    UNIVERSAL_FORBIDDEN_INPUT_FIELDS = {
        "current_status",
        "current_version_no",
        "latest_action",
        "latest_action_at",
        "review_actions",
        "snapshots",
        "statuses",
        "validation_result",
        "versions",
    }
    MINOR_EDIT_ALLOWED_INPUT_FIELDS: set[str] = set()
    QUESTION_MODIFY_ALLOWED_INPUT_FIELDS = {"difficulty_target", "extra_constraints", "pattern_id", "type_slots"}
    TEXT_MODIFY_ALLOWED_INPUT_FIELDS = {"material_id", "material_policy", "material_text"}
    MANUAL_EDIT_ALLOWED_INPUT_FIELDS = {"manual_patch"}
    MANUAL_EDIT_ALLOWED_PATCH_FIELDS = {"analysis", "answer", "material_text", "options", "stem"}
    RENDER_LIKE_FIELDS = {"analysis", "options", "stem"}
    TRUTH_LIKE_FIELDS = {
        "business_subtype",
        "control_logic",
        "correct_order",
        "difficulty_target",
        "generation_logic",
        "original_sentences",
        "pattern_id",
        "question_type",
        "resolved_slots",
    }
    MATERIAL_BOUNDARY_FIELDS = {"material_id", "material_selection", "material_source", "material_text", "source_tail"}

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

        original_item = deepcopy(item)
        requested_action = self._requested_action(request)
        prior_action_context = self._get_latest_action_context(item_id)
        policy = self._build_action_policy(request)
        approval_context = (
            self._build_approval_context(item, prior_action_context)
            if request.action in {"approve", "confirm"}
            else None
        )

        statuses = item.get("statuses", {})
        from_status = statuses.get("review_status", "draft")
        from_version_no = int(item.get("current_version_no", 1))
        preliminary_effective_action = policy["effective_action"]
        action_result = self._run_action(
            item,
            request,
            effective_action=preliminary_effective_action,
            approval_context=approval_context,
        )
        diagnostics = self._audit_action_result(
            before_item=original_item,
            after_item=action_result,
            requested_action=requested_action,
            preliminary_effective_action=preliminary_effective_action,
            semantic_class=policy["semantic_class"],
            trust_level=policy["trust_level"],
            audit_reason=policy.get("upgrade_reason"),
        )
        statuses = action_result.get("statuses", {})
        to_status = statuses.get("review_status", from_status)
        to_version_no = int(action_result.get("current_version_no", from_version_no))

        action_id = str(uuid4())
        action_payload = {
            "action_id": action_id,
            "item_id": item_id,
            "action": requested_action,
            "requested_action": requested_action,
            "preliminary_effective_action": preliminary_effective_action,
            "effective_action": diagnostics["effective_action"],
            "semantic_class": diagnostics["semantic_class"],
            "changed_fields": diagnostics["changed_fields"],
            "truth_touched": diagnostics["truth_touched"],
            "material_boundary_crossed": diagnostics["material_boundary_crossed"],
            "trust_level": diagnostics["trust_level"],
            "audit_reason": diagnostics["audit_reason"],
            "patch": {
                "instruction": request.instruction,
                "control_overrides": request.control_overrides,
                "input_fields": policy["input_fields"],
                "allowed_input_fields": policy["allowed_input_fields"],
                "forbidden_input_fields": policy["forbidden_input_fields"],
                "from_status": from_status,
                "to_status": to_status,
                "from_version_no": from_version_no,
                "to_version_no": to_version_no,
                "material_id": (action_result.get("material_selection") or {}).get("material_id"),
                "revision_count": action_result.get("revision_count", item.get("revision_count", 0)),
                "prior_action_context": prior_action_context,
                "audit_reason": diagnostics["audit_reason"],
            },
        }
        if approval_context is not None:
            action_payload.update(approval_context)
        version_record = action_result.pop("_version_record", None)
        if version_record is not None:
            version_record["source_action"] = diagnostics["effective_action"]
            runtime_snapshot = dict(version_record.get("runtime_snapshot") or {})
            runtime_snapshot["review_action_policy"] = {
                "requested_action": requested_action,
                "preliminary_effective_action": preliminary_effective_action,
                "effective_action": diagnostics["effective_action"],
                "semantic_class": diagnostics["semantic_class"],
                "changed_fields": diagnostics["changed_fields"],
                "truth_touched": diagnostics["truth_touched"],
                "material_boundary_crossed": diagnostics["material_boundary_crossed"],
                "trust_level": diagnostics["trust_level"],
                "prior_action_context": prior_action_context,
                "audit_reason": diagnostics["audit_reason"],
            }
            version_record["runtime_snapshot"] = runtime_snapshot
            self.repository.save_version(version_record)
        self.repository.save_item(action_result)
        self.repository.save_review_action(
            action_id,
            item_id,
            diagnostics["effective_action"],
            action_payload,
            from_version_no=from_version_no,
            to_version_no=to_version_no,
            result_status=action_result.get("current_status"),
            operator=request.operator or "system",
        )
        logger.info(
            "review_action_applied item_id=%s requested_action=%s effective_action=%s from_version=%s to_version=%s status=%s",
            item_id,
            requested_action,
            diagnostics["effective_action"],
            from_version_no,
            to_version_no,
            action_result.get("current_status"),
        )
        return {"action_id": action_id, "action": diagnostics["effective_action"], "item": action_result}

    def _build_action_policy(self, request: QuestionReviewActionRequest) -> dict[str, Any]:
        action = request.action
        control_overrides = request.control_overrides or {}
        input_fields = self._collect_input_fields(control_overrides)

        if action in {"approve", "confirm", "discard"}:
            if control_overrides:
                raise DomainError(
                    "Decision actions do not accept control overrides.",
                    status_code=422,
                    details={"action": action, "input_fields": input_fields},
                )
            return {
                "effective_action": action,
                "semantic_class": "review_decision",
                "trust_level": "high",
                "input_fields": input_fields,
                "allowed_input_fields": [],
                "forbidden_input_fields": [],
                "upgrade_reason": None,
            }

        top_level_fields = set(control_overrides.keys())
        forbidden_present = top_level_fields & self.UNIVERSAL_FORBIDDEN_INPUT_FIELDS
        if forbidden_present:
            raise DomainError(
                "Review action attempted to patch protected workflow fields.",
                status_code=422,
                details={"action": action, "forbidden_input_fields": sorted(forbidden_present)},
            )

        if action == "minor_edit":
            if control_overrides:
                raise DomainError(
                    "minor_edit only accepts instruction-based targeted repair.",
                    status_code=422,
                    details={"action": action, "forbidden_input_fields": input_fields},
                )
            return {
                "effective_action": "minor_edit",
                "semantic_class": "targeted_repair",
                "trust_level": "high",
                "input_fields": input_fields,
                "allowed_input_fields": sorted(self.MINOR_EDIT_ALLOWED_INPUT_FIELDS),
                "forbidden_input_fields": sorted(self.MATERIAL_INPUT_FIELDS | {"manual_patch"}),
                "upgrade_reason": None,
            }

        if action == "question_modify":
            self._ensure_allowed_input_fields(
                action=action,
                provided_fields=top_level_fields,
                allowed_fields=self.QUESTION_MODIFY_ALLOWED_INPUT_FIELDS,
                message="question_modify only accepts control-regenerate fields.",
            )
            return {
                "effective_action": "question_modify",
                "semantic_class": "control_regenerate",
                "trust_level": "medium",
                "input_fields": input_fields,
                "allowed_input_fields": sorted(self.QUESTION_MODIFY_ALLOWED_INPUT_FIELDS),
                "forbidden_input_fields": sorted(top_level_fields - self.QUESTION_MODIFY_ALLOWED_INPUT_FIELDS),
                "upgrade_reason": None,
            }

        if action == "text_modify":
            self._ensure_allowed_input_fields(
                action=action,
                provided_fields=top_level_fields,
                allowed_fields=self.TEXT_MODIFY_ALLOWED_INPUT_FIELDS,
                message="text_modify only accepts material-boundary fields.",
            )
            return {
                "effective_action": "text_modify",
                "semantic_class": "material_regenerate",
                "trust_level": "medium",
                "input_fields": input_fields,
                "allowed_input_fields": sorted(self.TEXT_MODIFY_ALLOWED_INPUT_FIELDS),
                "forbidden_input_fields": sorted(top_level_fields - self.TEXT_MODIFY_ALLOWED_INPUT_FIELDS),
                "upgrade_reason": None,
            }

        if action == "manual_edit":
            extra_fields = top_level_fields - self.MANUAL_EDIT_ALLOWED_INPUT_FIELDS
            if extra_fields:
                raise DomainError(
                    "manual_edit only accepts manual_patch.",
                    status_code=422,
                    details={"action": action, "forbidden_input_fields": sorted(extra_fields)},
                )
            manual_patch = control_overrides.get("manual_patch") or {}
            patch_fields = set(manual_patch.keys())
            forbidden_patch_fields = patch_fields - self.MANUAL_EDIT_ALLOWED_PATCH_FIELDS
            if forbidden_patch_fields:
                raise DomainError(
                    "manual_edit attempted to patch unsupported fields.",
                    status_code=422,
                    details={"action": action, "forbidden_input_fields": sorted(forbidden_patch_fields)},
                )
            material_patch = bool(patch_fields & {"material_text"})
            return {
                "effective_action": "manual_edit",
                "semantic_class": "manual_override_material" if material_patch else "manual_override",
                "trust_level": "low" if material_patch else "medium",
                "input_fields": input_fields,
                "allowed_input_fields": sorted(self.MANUAL_EDIT_ALLOWED_PATCH_FIELDS),
                "forbidden_input_fields": sorted(self.UNIVERSAL_FORBIDDEN_INPUT_FIELDS),
                "upgrade_reason": None,
            }

        raise DomainError(
            "Unsupported review action.",
            status_code=422,
            details={"action": action},
        )

    def _audit_action_result(
        self,
        *,
        before_item: dict,
        after_item: dict,
        requested_action: str,
        preliminary_effective_action: str,
        semantic_class: str,
        trust_level: str,
        audit_reason: str | None,
    ) -> dict[str, Any]:
        before_values = self._extract_tracked_values(before_item)
        after_values = self._extract_tracked_values(after_item)
        changed_fields = sorted(
            field_name for field_name in before_values if before_values.get(field_name) != after_values.get(field_name)
        )
        answer_truth_changed = self._answer_truth_changed(before_item, after_item)
        truth_touched = any(field_name in self.TRUTH_LIKE_FIELDS for field_name in changed_fields) or answer_truth_changed
        material_boundary_crossed = any(field_name in self.MATERIAL_BOUNDARY_FIELDS for field_name in changed_fields)

        final_effective_action = preliminary_effective_action
        final_semantic_class = semantic_class
        final_trust_level = trust_level
        final_audit_reason = audit_reason

        if preliminary_effective_action == "minor_edit":
            if material_boundary_crossed:
                final_effective_action = "text_modify"
                final_semantic_class = "material_regenerate"
                final_trust_level = "low"
                final_audit_reason = "minor_edit_result_crossed_material_boundary"
            elif truth_touched:
                final_effective_action = "question_modify"
                final_semantic_class = "control_regenerate"
                final_trust_level = "medium"
                final_audit_reason = "minor_edit_result_touched_truth_like_fields"
            else:
                final_audit_reason = final_audit_reason or "minor_edit_result_within_render_scope"
        elif preliminary_effective_action == "question_modify":
            if material_boundary_crossed:
                final_effective_action = "text_modify"
                final_semantic_class = "material_regenerate"
                final_trust_level = "medium"
                final_audit_reason = "question_modify_result_became_material_driven"
            elif truth_touched:
                final_audit_reason = "question_modify_result_touched_truth_like_fields"
            else:
                final_audit_reason = "question_modify_result_render_dominant"
        elif preliminary_effective_action == "text_modify":
            final_effective_action = "text_modify"
            final_semantic_class = "material_regenerate"
            if material_boundary_crossed and truth_touched:
                final_trust_level = "low"
                final_audit_reason = "text_modify_result_changed_material_and_truth_like_fields"
            elif material_boundary_crossed:
                final_trust_level = "medium"
                final_audit_reason = "text_modify_result_confirmed_material_boundary_change"
            else:
                final_trust_level = "low"
                final_audit_reason = "text_modify_result_missing_material_boundary_change"
        elif preliminary_effective_action == "manual_edit":
            final_effective_action = "manual_edit"
            if material_boundary_crossed and truth_touched:
                final_semantic_class = "manual_override_high_risk"
                final_trust_level = "low"
                final_audit_reason = "manual_edit_result_touched_material_and_truth_like_fields"
            elif material_boundary_crossed:
                final_semantic_class = "manual_override_material"
                final_trust_level = "low"
                final_audit_reason = "manual_edit_result_crossed_material_boundary"
            elif truth_touched:
                final_semantic_class = "manual_override_high_risk"
                final_trust_level = "low"
                final_audit_reason = "manual_edit_result_touched_truth_like_fields"
            else:
                final_semantic_class = "manual_override"
                final_trust_level = "medium"
                final_audit_reason = "manual_edit_result_within_render_scope"
        else:
            final_audit_reason = final_audit_reason or "result_audit_not_required"

        return {
            "requested_action": requested_action,
            "effective_action": final_effective_action,
            "semantic_class": final_semantic_class,
            "changed_fields": changed_fields,
            "truth_touched": truth_touched,
            "material_boundary_crossed": material_boundary_crossed,
            "trust_level": final_trust_level,
            "audit_reason": final_audit_reason,
        }

    def _extract_tracked_values(self, item: dict) -> dict[str, Any]:
        generated_question = item.get("generated_question") or {}
        material_selection = item.get("material_selection") or {}
        return {
            "question_type": item.get("question_type"),
            "business_subtype": item.get("business_subtype"),
            "pattern_id": item.get("pattern_id"),
            "resolved_slots": item.get("resolved_slots") or {},
            "control_logic": item.get("control_logic") or {},
            "generation_logic": item.get("generation_logic") or {},
            "difficulty_target": item.get("difficulty_target"),
            "stem": generated_question.get("stem"),
            "options": generated_question.get("options") or {},
            "answer": generated_question.get("answer"),
            "analysis": generated_question.get("analysis"),
            "original_sentences": generated_question.get("original_sentences") or [],
            "correct_order": generated_question.get("correct_order") or [],
            "material_text": item.get("material_text"),
            "material_source": item.get("material_source") or {},
            "source_tail": material_selection.get("source_tail"),
            "material_id": material_selection.get("material_id"),
            "material_selection": material_selection,
        }

    def _collect_input_fields(self, control_overrides: dict[str, Any]) -> list[str]:
        fields: list[str] = []
        for key in sorted(control_overrides.keys()):
            fields.append(key)
            if key == "manual_patch" and isinstance(control_overrides[key], dict):
                fields.extend(f"manual_patch.{sub_key}" for sub_key in sorted(control_overrides[key].keys()))
        return fields

    def _ensure_allowed_input_fields(
        self,
        *,
        action: str,
        provided_fields: set[str],
        allowed_fields: set[str],
        message: str,
    ) -> None:
        disallowed_fields = provided_fields - allowed_fields
        if not disallowed_fields:
            return
        raise DomainError(
            message,
            status_code=422,
            details={
                "action": action,
                "allowed_input_fields": sorted(allowed_fields),
                "disallowed_input_fields": sorted(disallowed_fields),
            },
        )

    def _answer_truth_changed(self, before_item: dict, after_item: dict) -> bool:
        before_correct = self._correct_option_text(before_item)
        after_correct = self._correct_option_text(after_item)
        if before_correct is None and after_correct is None:
            return False
        return before_correct != after_correct

    def _correct_option_text(self, item: dict) -> str | None:
        generated_question = item.get("generated_question") or {}
        options = generated_question.get("options") or {}
        answer = str(generated_question.get("answer") or "").strip().upper()
        if answer not in options:
            return None
        return str(options.get(answer) or "").strip()

    def _get_latest_action_context(self, item_id: str) -> dict[str, Any] | None:
        actions = self.repository.list_review_actions(item_id=item_id, limit=1)
        if not actions:
            return None
        latest = actions[0]
        payload = latest.get("payload") or {}
        return {
            "requested_action": payload.get("requested_action") or payload.get("action") or latest.get("action_type"),
            "effective_action": payload.get("effective_action") or latest.get("action_type"),
            "semantic_class": payload.get("semantic_class"),
            "truth_touched": payload.get("truth_touched"),
            "material_boundary_crossed": payload.get("material_boundary_crossed"),
            "trust_level": payload.get("trust_level"),
            "audit_reason": payload.get("audit_reason"),
        }

    def _requested_action(self, request: QuestionReviewActionRequest) -> str:
        return str(request.requested_action or request.action)

    def _build_approval_context(
        self,
        item: dict,
        prior_action_context: dict[str, Any] | None,
    ) -> dict[str, Any]:
        preceding_effective_action = (
            (prior_action_context or {}).get("effective_action")
            or item.get("latest_action")
            or "generate"
        )
        preceding_semantic_class = (
            (prior_action_context or {}).get("semantic_class")
            or ("generated_candidate" if preceding_effective_action == "generate" else "unknown")
        )
        preceding_trust_level = (prior_action_context or {}).get("trust_level") or "unknown"
        preceding_truth_touched = bool((prior_action_context or {}).get("truth_touched"))
        preceding_material_boundary_crossed = bool((prior_action_context or {}).get("material_boundary_crossed"))
        preceding_audit_reason = (prior_action_context or {}).get("audit_reason")
        return {
            "approval_basis": self._resolve_approval_basis(
                preceding_effective_action=preceding_effective_action,
                preceding_semantic_class=preceding_semantic_class,
                preceding_trust_level=preceding_trust_level,
            ),
            "preceding_effective_action": preceding_effective_action,
            "preceding_semantic_class": preceding_semantic_class,
            "preceding_trust_level": preceding_trust_level,
            "preceding_truth_touched": preceding_truth_touched,
            "preceding_material_boundary_crossed": preceding_material_boundary_crossed,
            "preceding_audit_reason": preceding_audit_reason,
        }

    def _resolve_approval_basis(
        self,
        *,
        preceding_effective_action: str,
        preceding_semantic_class: str,
        preceding_trust_level: str,
    ) -> str:
        if preceding_semantic_class == "targeted_repair" and preceding_trust_level == "high":
            return "targeted_repair_high_trust"
        if preceding_semantic_class == "control_regenerate":
            return f"control_regenerate_{preceding_trust_level}"
        if preceding_semantic_class == "material_regenerate":
            return f"material_regenerate_{preceding_trust_level}"
        if preceding_semantic_class.startswith("manual_override"):
            return f"{preceding_semantic_class}_{preceding_trust_level}"
        if preceding_effective_action == "generate":
            return "generated_candidate_validation_passed"
        return f"{preceding_effective_action}_{preceding_trust_level}"

    def _run_action(
        self,
        item: dict,
        request: QuestionReviewActionRequest,
        *,
        effective_action: str,
        approval_context: dict[str, Any] | None = None,
    ) -> dict:
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
            if request.action == "confirm" and (approval_context or {}).get("preceding_trust_level") == "low":
                raise DomainError(
                    "Low-trust reviewed items require explicit approve instead of confirm.",
                    status_code=422,
                    details=approval_context or {},
                )
            item["statuses"]["review_status"] = "approved"
            item["current_status"] = "approved"
            item["latest_action"] = request.action
            item["latest_action_at"] = self.repository._utc_now()
            approval_note = f"approval_basis:{(approval_context or {}).get('approval_basis', 'unknown')}"
            item["notes"] = item.get("notes", []) + [f"review_action:{request.action}", approval_note]
            return item
        if request.action == "discard":
            item["statuses"]["review_status"] = "rejected"
            item["current_status"] = "discarded"
            item["latest_action"] = "discard"
            item["latest_action_at"] = self.repository._utc_now()
            item["notes"] = item.get("notes", []) + ["review_action:discard"]
            return item
        if effective_action == "minor_edit":
            item["current_status"] = "revising"
            return self.generation_service.revise_minor_edit(item, request.instruction)
        if effective_action == "question_modify":
            item["current_status"] = "revising"
            return self.generation_service.revise_question_modify(item, request.instruction, request.control_overrides)
        if effective_action == "text_modify":
            item["current_status"] = "revising"
            return self.generation_service.revise_text_modify(item, request.instruction, request.control_overrides)
        if effective_action == "manual_edit":
            item["current_status"] = "revising"
            return self.generation_service.apply_manual_edit(item, request.instruction, request.control_overrides)
        raise DomainError(
            "Unsupported review action.",
            status_code=422,
            details={"action": effective_action},
        )
