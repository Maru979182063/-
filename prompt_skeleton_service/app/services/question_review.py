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
    OPTION_PATCH_FIELDS = {"A", "B", "C", "D"}
    INTERNAL_EXTRA_CONSTRAINT_FIELDS = {
        "reference_business_cards",
        "reference_query_terms",
        "required_review_overrides",
        "review_instruction",
        "source_question_style_summary",
    }
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

    @staticmethod
    def _top_positive_values(payload: dict[str, Any], *, limit: int = 3) -> dict[str, float]:
        if not isinstance(payload, dict):
            return {}
        ranked = sorted(
            (
                (str(key), round(float(value or 0.0), 4))
                for key, value in payload.items()
                if float(value or 0.0) > 0
            ),
            key=lambda entry: entry[1],
            reverse=True,
        )
        return {key: value for key, value in ranked[:limit]}

    def _build_material_decision_context(self, item: dict[str, Any]) -> dict[str, Any]:
        material_source = item.get("material_source") or {}
        if not isinstance(material_source, dict):
            return {}
        feedback_snapshot = material_source.get("feedback_snapshot") if isinstance(material_source.get("feedback_snapshot"), dict) else {}
        scoring = material_source.get("scoring") if isinstance(material_source.get("scoring"), dict) else {}
        decision_meta = material_source.get("decision_meta") if isinstance(material_source.get("decision_meta"), dict) else {}
        ranking_meta = material_source.get("ranking_meta") if isinstance(material_source.get("ranking_meta"), dict) else {}
        if feedback_snapshot:
            return {
                **dict(feedback_snapshot),
                "task_family": scoring.get("task_family") if isinstance(scoring, dict) else None,
                "ranking_meta": ranking_meta,
            }
        if not scoring and not decision_meta:
            return {}

        risk_penalties = scoring.get("risk_penalties") if isinstance(scoring.get("risk_penalties"), dict) else {}
        difficulty_vector = scoring.get("difficulty_vector") if isinstance(scoring.get("difficulty_vector"), dict) else {}
        difficulty_trace = scoring.get("difficulty_trace") if isinstance(scoring.get("difficulty_trace"), dict) else {}
        band_decision = difficulty_trace.get("band_decision") if isinstance(difficulty_trace.get("band_decision"), dict) else {}
        scoring_summary = decision_meta.get("scoring_summary") if isinstance(decision_meta.get("scoring_summary"), dict) else {}
        return {
            "selection_state": decision_meta.get("selection_state"),
            "review_like_risk": bool(decision_meta.get("review_like_risk")),
            "repair_suggested": bool(decision_meta.get("repair_suggested")),
            "decision_reason": decision_meta.get("decision_reason"),
            "repair_reason": decision_meta.get("repair_reason"),
            "quality_difficulty_note": decision_meta.get("quality_difficulty_note") or band_decision.get("quality_difficulty_note"),
            "task_family": scoring.get("task_family") or scoring_summary.get("task_family"),
            "final_candidate_score": round(float(scoring.get("final_candidate_score") or scoring_summary.get("final_candidate_score") or 0.0), 4),
            "readiness_score": round(float(scoring.get("readiness_score") or scoring_summary.get("readiness_score") or 0.0), 4),
            "total_penalty": round(float(scoring_summary.get("total_penalty") or 0.0), 4),
            "difficulty_band_hint": scoring.get("difficulty_band_hint") or scoring_summary.get("difficulty_band_hint"),
            "recommended": bool(scoring.get("recommended") if "recommended" in scoring else scoring_summary.get("recommended")),
            "needs_review": bool(scoring.get("needs_review") if "needs_review" in scoring else scoring_summary.get("needs_review")),
            "key_penalties": decision_meta.get("key_penalties") or self._top_positive_values(risk_penalties, limit=3),
            "key_difficulty_dimensions": decision_meta.get("key_difficulty_dimensions") or self._top_positive_values(difficulty_vector, limit=3),
            "preference_profile": material_source.get("preference_profile") if isinstance(material_source.get("preference_profile"), dict) else {},
            "ranking_meta": ranking_meta,
        }

    def _build_feedback_snapshot(self, item: dict[str, Any]) -> dict[str, Any]:
        material_source = item.get("material_source") or {}
        if isinstance(material_source, dict):
            payload = material_source.get("feedback_snapshot")
            if isinstance(payload, dict) and payload:
                return dict(payload)
        return self._build_material_decision_context(item)

    def _build_feedback_outcome(
        self,
        *,
        requested_action: str,
        effective_action: str,
        action_result: dict[str, Any],
    ) -> dict[str, Any]:
        result_status = str(action_result.get("current_status") or "")
        review_status = str((action_result.get("statuses") or {}).get("review_status") or "")
        repair_actions = {"minor_edit", "question_modify", "text_modify", "manual_edit"}
        repair_path_taken = effective_action in repair_actions
        accepted_as_is = effective_action in {"approve", "confirm"} and result_status == "approved"
        revised_then_kept = repair_path_taken and result_status in {"pending_review", "approved", "generated"}
        discarded = effective_action == "discard" or result_status == "discarded"
        return {
            "review_action": effective_action or requested_action,
            "requested_action": requested_action,
            "accepted_as_is": accepted_as_is,
            "revised_then_kept": revised_then_kept,
            "discarded": discarded,
            "repair_path_taken": repair_path_taken,
            "result_status": result_status,
            "review_status": review_status,
        }

    def _extract_threshold_failures(self, item: dict[str, Any]) -> list[dict[str, Any]]:
        validation_result = item.get("validation_result") or {}
        checks = validation_result.get("checks") or {}
        if not isinstance(checks, dict):
            return []

        failures: list[dict[str, Any]] = []
        for check_name, payload in checks.items():
            if not isinstance(payload, dict):
                continue
            if payload.get("required") is not True:
                continue
            if payload.get("passed") is not False:
                continue

            has_threshold_like_payload = any(
                key in payload for key in ("threshold", "allowed_range", "actual", "difficulty_band")
            )
            if not has_threshold_like_payload:
                continue

            threshold_name = str(payload.get("reason") or check_name or "").strip()
            if not threshold_name:
                continue

            failure = {
                "check_name": str(check_name),
                "threshold_name": threshold_name,
                "source": str(payload.get("source") or "unknown"),
            }
            if "actual" in payload:
                failure["actual"] = payload.get("actual")
            if "threshold" in payload:
                failure["threshold"] = payload.get("threshold")
            if "allowed_range" in payload:
                failure["allowed_range"] = payload.get("allowed_range")
            if "difficulty_band" in payload:
                failure["difficulty_band"] = payload.get("difficulty_band")
            failures.append(failure)
        return failures

    def _build_feedback_backtest_unit(
        self,
        *,
        item: dict[str, Any],
        original_item: dict[str, Any],
        feedback_snapshot: dict[str, Any],
        feedback_outcome: dict[str, Any],
    ) -> dict[str, Any]:
        material_selection = item.get("material_selection") or {}
        material_source = item.get("material_source") or {}
        threshold_failures = self._extract_threshold_failures(original_item)
        return {
            "item_id": item.get("item_id"),
            "question_type": item.get("question_type"),
            "business_subtype": item.get("business_subtype"),
            "question_card_id": (item.get("request_snapshot") or {}).get("question_card_id") or material_selection.get("question_card_id"),
            "material_id": material_selection.get("material_id"),
            "selection_state": feedback_snapshot.get("selection_state"),
            "review_like_risk": feedback_snapshot.get("review_like_risk"),
            "repair_suggested": feedback_snapshot.get("repair_suggested"),
            "decision_reason": feedback_snapshot.get("decision_reason"),
            "final_candidate_score": feedback_snapshot.get("final_candidate_score"),
            "difficulty_band_hint": feedback_snapshot.get("difficulty_band_hint"),
            "key_penalties": feedback_snapshot.get("key_penalties") or {},
            "key_difficulty_dimensions": feedback_snapshot.get("key_difficulty_dimensions") or {},
            "recommended": feedback_snapshot.get("recommended"),
            "needs_review": feedback_snapshot.get("needs_review"),
            "preference_profile": feedback_snapshot.get("preference_profile") or material_source.get("preference_profile") or {},
            "threshold_failures": threshold_failures,
            "failed_threshold_names": [failure.get("threshold_name") for failure in threshold_failures if failure.get("threshold_name")],
            "review_action": feedback_outcome.get("review_action"),
            "accepted_as_is": feedback_outcome.get("accepted_as_is"),
            "revised_then_kept": feedback_outcome.get("revised_then_kept"),
            "discarded": feedback_outcome.get("discarded"),
            "repair_path_taken": feedback_outcome.get("repair_path_taken"),
        }

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
        self._validate_action_payload_against_item(
            item=item,
            action=request.action,
            control_overrides=request.control_overrides or {},
        )
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
        self._enforce_action_result_within_policy(
            effective_action=preliminary_effective_action,
            diagnostics=diagnostics,
        )
        statuses = action_result.get("statuses", {})
        to_status = statuses.get("review_status", from_status)
        to_version_no = int(action_result.get("current_version_no", from_version_no))
        material_decision_context = self._build_material_decision_context(action_result)
        feedback_snapshot = self._build_feedback_snapshot(action_result)
        feedback_outcome = self._build_feedback_outcome(
            requested_action=requested_action,
            effective_action=diagnostics["effective_action"],
            action_result=action_result,
        )
        feedback_backtest_unit = self._build_feedback_backtest_unit(
            item=action_result,
            original_item=original_item,
            feedback_snapshot=feedback_snapshot,
            feedback_outcome=feedback_outcome,
        )

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
            "material_decision_context": material_decision_context,
            "feedback_snapshot": feedback_snapshot,
            "feedback_outcome": feedback_outcome,
            "feedback_backtest_unit": feedback_backtest_unit,
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
                "material_decision_context": material_decision_context,
                "feedback_snapshot": feedback_snapshot,
                "feedback_outcome": feedback_outcome,
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
                "material_decision_context": material_decision_context,
                "feedback_snapshot": feedback_snapshot,
                "feedback_outcome": feedback_outcome,
                "feedback_backtest_unit": feedback_backtest_unit,
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
            final_audit_reason = (
                "minor_edit_result_crossed_material_boundary"
                if material_boundary_crossed
                else (
                    "minor_edit_result_touched_truth_like_fields"
                    if truth_touched
                    else (final_audit_reason or "minor_edit_result_within_render_scope")
                )
            )
        elif preliminary_effective_action == "question_modify":
            final_audit_reason = (
                "question_modify_result_became_material_driven"
                if material_boundary_crossed
                else ("question_modify_result_touched_truth_like_fields" if truth_touched else "question_modify_result_within_scope")
            )
        elif preliminary_effective_action == "text_modify":
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
            "material_decision_context": self._build_material_decision_context(item),
            "feedback_snapshot": self._build_feedback_snapshot(item),
            "source_tail": material_selection.get("source_tail"),
            "material_id": material_selection.get("material_id"),
            "material_selection": material_selection,
        }

    def _collect_input_fields(self, control_overrides: dict[str, Any]) -> list[str]:
        fields: list[str] = []
        for key in sorted(control_overrides.keys()):
            fields.append(key)
            if key in {"extra_constraints", "type_slots"} and isinstance(control_overrides[key], dict):
                fields.extend(f"{key}.{sub_key}" for sub_key in sorted(control_overrides[key].keys()))
            if key == "manual_patch" and isinstance(control_overrides[key], dict):
                fields.extend(f"manual_patch.{sub_key}" for sub_key in sorted(control_overrides[key].keys()))
                option_patch = control_overrides[key].get("options")
                if isinstance(option_patch, dict):
                    fields.extend(f"manual_patch.options.{sub_key}" for sub_key in sorted(option_patch.keys()))
        return fields

    def _validate_action_payload_against_item(
        self,
        *,
        item: dict[str, Any],
        action: str,
        control_overrides: dict[str, Any],
    ) -> None:
        request_snapshot = item.get("request_snapshot") or {}
        if action == "question_modify":
            allowed_extra_constraint_keys = {
                str(key)
                for key in (request_snapshot.get("extra_constraints") or {}).keys()
                if str(key) not in self.INTERNAL_EXTRA_CONSTRAINT_FIELDS
            }
            allowed_type_slot_keys = {str(key) for key in (request_snapshot.get("type_slots") or {}).keys()}
            self._ensure_nested_override_fields(
                action=action,
                parent_field="extra_constraints",
                payload=control_overrides.get("extra_constraints"),
                allowed_fields=allowed_extra_constraint_keys,
            )
            self._ensure_nested_override_fields(
                action=action,
                parent_field="type_slots",
                payload=control_overrides.get("type_slots"),
                allowed_fields=allowed_type_slot_keys,
            )
            return
        if action == "manual_edit":
            manual_patch = control_overrides.get("manual_patch")
            if manual_patch is None:
                return
            if not isinstance(manual_patch, dict):
                raise DomainError(
                    "manual_edit requires manual_patch to be an object.",
                    status_code=422,
                    details={"action": action, "field": "manual_patch"},
                )
            options_patch = manual_patch.get("options")
            if options_patch is None:
                return
            if not isinstance(options_patch, dict):
                raise DomainError(
                    "manual_edit.options must be an object keyed by option letter.",
                    status_code=422,
                    details={"action": action, "field": "manual_patch.options"},
                )
            unknown_option_keys = sorted(set(options_patch.keys()) - self.OPTION_PATCH_FIELDS)
            if unknown_option_keys:
                raise DomainError(
                    "manual_edit.options only accepts A/B/C/D.",
                    status_code=422,
                    details={
                        "action": action,
                        "field": "manual_patch.options",
                        "disallowed_input_fields": [f"manual_patch.options.{key}" for key in unknown_option_keys],
                    },
                )

    def _ensure_nested_override_fields(
        self,
        *,
        action: str,
        parent_field: str,
        payload: Any,
        allowed_fields: set[str],
    ) -> None:
        if payload is None:
            return
        if not isinstance(payload, dict):
            raise DomainError(
                f"{action} requires {parent_field} to be an object.",
                status_code=422,
                details={"action": action, "field": parent_field},
            )
        disallowed_fields = sorted(set(payload.keys()) - set(allowed_fields))
        if disallowed_fields:
            raise DomainError(
                f"{action} only accepts predeclared nested fields in {parent_field}.",
                status_code=422,
                details={
                    "action": action,
                    "field": parent_field,
                    "allowed_input_fields": sorted(allowed_fields),
                    "disallowed_input_fields": [f"{parent_field}.{field}" for field in disallowed_fields],
                },
            )

    def _enforce_action_result_within_policy(
        self,
        *,
        effective_action: str,
        diagnostics: dict[str, Any],
    ) -> None:
        if effective_action == "minor_edit" and (
            diagnostics["truth_touched"] or diagnostics["material_boundary_crossed"]
        ):
            raise DomainError(
                "minor_edit must stay within render scope and cannot modify truth-like or material-boundary fields.",
                status_code=422,
                details=diagnostics,
            )
        if effective_action == "question_modify" and diagnostics["material_boundary_crossed"]:
            raise DomainError(
                "question_modify cannot cross the material boundary; use text_modify instead.",
                status_code=422,
                details=diagnostics,
            )

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
