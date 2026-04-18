from __future__ import annotations

import os
from copy import deepcopy
from contextlib import ExitStack
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

from fastapi.testclient import TestClient

from app.core.dependencies import get_question_repository
from app.core.exceptions import DomainError
from app.core.settings import get_settings
from app.main import app
from app.schemas.question import QuestionReviewActionRequest
from app.schemas.question import MaterialSelectionResult
from app.services.question_generation import QuestionGenerationService
from app.services.question_repository import QuestionRepository
from app.services.question_review import QuestionReviewService


class ReviewWorkbenchSmokeTest(TestCase):
    def setUp(self) -> None:
        self.tempdir = TemporaryDirectory()
        self.repository = QuestionRepository(Path(self.tempdir.name) / "question_workbench.db")
        app.dependency_overrides[get_question_repository] = lambda: self.repository
        get_settings.cache_clear()
        self.client = TestClient(app)

    def tearDown(self) -> None:
        app.dependency_overrides.clear()
        self.client.close()
        os.environ.pop("PROMPT_SERVICE_API_TOKEN", None)
        os.environ.pop("PROMPT_SERVICE_SECURITY_ENABLED", None)
        os.environ.pop("PROMPT_SERVICE_RATE_LIMIT_PER_MINUTE", None)
        get_settings.cache_clear()
        self.tempdir.cleanup()

    def test_generate_creates_item_and_first_version(self) -> None:
        with self._patched_runtime():
            response = self._generate_one()

        self.assertEqual(response.status_code, 200)
        item = response.json()["items"][0]
        history = self.client.get(f"/api/v1/review/items/{item['item_id']}/history")
        self.assertEqual(history.status_code, 200)
        payload = history.json()
        self.assertEqual(payload["current_version_no"], 1)
        self.assertEqual(len(payload["versions"]), 1)
        self.assertEqual(payload["versions"][0]["source_action"], "generate")

    def test_minor_edit_derives_new_version(self) -> None:
        with self._patched_runtime():
            item_id = self._generate_one().json()["items"][0]["item_id"]
            edit = self.client.post(
                f"/api/v1/questions/{item_id}/review-actions",
                json={"action": "minor_edit", "instruction": "精简题干表述"},
            )

        self.assertEqual(edit.status_code, 200)
        history = self.client.get(f"/api/v1/review/items/{item_id}/history").json()
        self.assertEqual(history["current_version_no"], 2)
        self.assertEqual(len(history["versions"]), 2)
        self.assertEqual(history["versions"][0]["source_action"], "minor_edit")
        self.assertEqual(history["versions"][1]["source_action"], "generate")

    def test_minor_edit_rejects_control_overrides(self) -> None:
        with self._patched_runtime():
            item_id = self._generate_one().json()["items"][0]["item_id"]
            edit = self.client.post(
                f"/api/v1/questions/{item_id}/review-actions",
                json={"action": "minor_edit", "control_overrides": {"material_text": "override"}},
            )

        self.assertEqual(edit.status_code, 422)

    def test_distractor_patch_creates_new_version_with_single_option_scope(self) -> None:
        item = self._save_base_review_item(item_id="dp-1")
        item_id = item["item_id"]
        original_question = deepcopy(item["generated_question"])
        response = self.client.post(
            f"/api/v1/questions/{item_id}/review-actions",
            json={
                "action": "distractor_patch",
                "target_option": "B",
                "option_text": "脱离材料主旨的干扰项",
                "analysis": "A项正确，因为“正确概括主旨”最能概括材料主旨；B项属于脱离材料的无关干扰。",
            },
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["action"], "distractor_patch")
        revised = payload["item"]
        self.assertEqual(revised["generated_question"]["answer"], original_question["answer"])
        self.assertEqual(revised["generated_question"]["options"]["A"], original_question["options"]["A"])
        self.assertEqual(revised["generated_question"]["options"]["C"], original_question["options"]["C"])
        self.assertEqual(revised["generated_question"]["options"]["D"], original_question["options"]["D"])
        self.assertNotEqual(revised["generated_question"]["options"]["B"], original_question["options"]["B"])
        self.assertEqual(revised["current_version_no"], 2)
        self.assertEqual(revised["latest_action"], "distractor_patch")
        self.assertIn("evaluation_result", revised)
        self.assertIn("validation_result", revised)
        latest_action = self.client.get(f"/api/v1/questions/{item_id}/review-actions").json()[0]
        self.assertEqual(latest_action["action_type"], "distractor_patch")
        self.assertEqual(latest_action["payload"]["semantic_class"], "targeted_repair")
        self.assertEqual(latest_action["payload"]["changed_fields"], ["analysis", "options"])
        self.assertFalse(latest_action["payload"]["truth_touched"])
        self.assertFalse(latest_action["payload"]["material_boundary_crossed"])
        history = self.client.get(f"/api/v1/review/items/{item_id}/history").json()
        self.assertEqual(history["current_version_no"], 2)
        self.assertEqual(history["review_actions"][0]["action_type"], "distractor_patch")

    def test_distractor_patch_rejects_answer_option_target(self) -> None:
        item_id = self._save_base_review_item(item_id="dp-2")["item_id"]
        response = self.client.post(
            f"/api/v1/questions/{item_id}/review-actions",
            json={
                "action": "distractor_patch",
                "target_option": "A",
                "option_text": "试图改正确项",
                "analysis": "A项正确，因为A项最能概括材料主旨。",
            },
        )

        self.assertEqual(response.status_code, 422)

    def _legacy_minor_edit_result_truth_change_is_upgraded_by_result_audit(self) -> None:
        with self._patched_runtime():
            item_id = self._generate_one().json()["items"][0]["item_id"]

        item = self.repository.get_item(item_id)

        class StubGenerationService:
            def __init__(self, repository):
                self.repository = repository

            def revise_minor_edit(self, current_item, instruction):
                revised = deepcopy(current_item)
                revised["generated_question"] = {
                    **(current_item.get("generated_question") or {}),
                    "options": {"A": "新A", "B": "新B", "C": "新C", "D": "新D"},
                    "answer": "B",
                    "analysis": "新的解析",
                }
                revised["current_version_no"] = int(current_item.get("current_version_no", 1)) + 1
                revised["current_status"] = "pending_review"
                revised["revision_count"] = int(current_item.get("revision_count", 0)) + 1
                revised["statuses"]["review_status"] = "waiting_review"
                revised["statuses"]["generation_status"] = "success"
                revised["statuses"]["validation_status"] = "passed"
                return revised

        service = QuestionReviewService(self.repository, StubGenerationService(self.repository))
        response = service.apply_action(
            item_id,
            QuestionReviewActionRequest(action="minor_edit", instruction="改正确答案语义"),
        )

        self.assertEqual(response["action"], "question_modify")
        latest_action = self.repository.list_review_actions(item_id=item_id, limit=1)[0]
        self.assertEqual(latest_action["action_type"], "question_modify")
        self.assertTrue(latest_action["payload"]["truth_touched"])
        self.assertEqual(latest_action["payload"]["audit_reason"], "minor_edit_result_touched_truth_like_fields")
        self.assertIn("answer", latest_action["payload"]["changed_fields"])

    def _legacy_minor_edit_result_material_change_is_upgraded_by_result_audit(self) -> None:
        with self._patched_runtime():
            item_id = self._generate_one().json()["items"][0]["item_id"]

        class StubGenerationService:
            def __init__(self, repository):
                self.repository = repository

            def revise_minor_edit(self, current_item, instruction):
                revised = deepcopy(current_item)
                revised["material_text"] = "新的材料文本"
                revised["material_source"] = {"site": "override"}
                revised["material_selection"] = {
                    **(current_item.get("material_selection") or {}),
                    "material_id": "mat-override",
                    "source_tail": "override tail",
                    "text": "新的材料文本",
                    "source": {"site": "override"},
                }
                revised["current_version_no"] = int(current_item.get("current_version_no", 1)) + 1
                revised["current_status"] = "pending_review"
                revised["revision_count"] = int(current_item.get("revision_count", 0)) + 1
                revised["statuses"]["review_status"] = "waiting_review"
                revised["statuses"]["generation_status"] = "success"
                revised["statuses"]["validation_status"] = "passed"
                return revised

        service = QuestionReviewService(self.repository, StubGenerationService(self.repository))
        response = service.apply_action(
            item_id,
            QuestionReviewActionRequest(action="minor_edit", instruction="误把材料换了"),
        )

        self.assertEqual(response["action"], "text_modify")
        latest_action = self.repository.list_review_actions(item_id=item_id, limit=1)[0]
        self.assertEqual(latest_action["action_type"], "text_modify")
        self.assertTrue(latest_action["payload"]["material_boundary_crossed"])
        self.assertEqual(latest_action["payload"]["audit_reason"], "minor_edit_result_crossed_material_boundary")
        self.assertIn("material_id", latest_action["payload"]["changed_fields"])

    def test_minor_edit_result_truth_change_is_rejected(self) -> None:
        with self._patched_runtime():
            item_id = self._generate_one().json()["items"][0]["item_id"]

        class StubGenerationService:
            def __init__(self, repository):
                self.repository = repository

            def revise_minor_edit(self, current_item, instruction):
                revised = deepcopy(current_item)
                revised["generated_question"] = {
                    **(current_item.get("generated_question") or {}),
                    "options": {"A": "新A", "B": "新B", "C": "新C", "D": "新D"},
                    "answer": "B",
                    "analysis": "新的解析",
                }
                revised["current_version_no"] = int(current_item.get("current_version_no", 1)) + 1
                revised["current_status"] = "pending_review"
                revised["revision_count"] = int(current_item.get("revision_count", 0)) + 1
                revised["statuses"]["review_status"] = "waiting_review"
                revised["statuses"]["generation_status"] = "success"
                revised["statuses"]["validation_status"] = "passed"
                return revised

        service = QuestionReviewService(self.repository, StubGenerationService(self.repository))
        with self.assertRaises(DomainError):
            service.apply_action(
                item_id,
                QuestionReviewActionRequest(action="minor_edit", instruction="reject truth drift"),
            )

        self.assertEqual(self.repository.list_review_actions(item_id=item_id, limit=1), [])

    def test_minor_edit_result_material_change_is_rejected(self) -> None:
        with self._patched_runtime():
            item_id = self._generate_one().json()["items"][0]["item_id"]

        class StubGenerationService:
            def __init__(self, repository):
                self.repository = repository

            def revise_minor_edit(self, current_item, instruction):
                revised = deepcopy(current_item)
                revised["material_text"] = "新的材料文本"
                revised["material_source"] = {"site": "override"}
                revised["material_selection"] = {
                    **(current_item.get("material_selection") or {}),
                    "material_id": "mat-override",
                    "source_tail": "override tail",
                    "text": "新的材料文本",
                    "source": {"site": "override"},
                }
                revised["current_version_no"] = int(current_item.get("current_version_no", 1)) + 1
                revised["current_status"] = "pending_review"
                revised["revision_count"] = int(current_item.get("revision_count", 0)) + 1
                revised["statuses"]["review_status"] = "waiting_review"
                revised["statuses"]["generation_status"] = "success"
                revised["statuses"]["validation_status"] = "passed"
                return revised

        service = QuestionReviewService(self.repository, StubGenerationService(self.repository))
        with self.assertRaises(DomainError):
            service.apply_action(
                item_id,
                QuestionReviewActionRequest(action="minor_edit", instruction="reject material drift"),
            )

        self.assertEqual(self.repository.list_review_actions(item_id=item_id, limit=1), [])

    def test_approve_updates_current_status(self) -> None:
        with self._patched_runtime():
            item_id = self._generate_one().json()["items"][0]["item_id"]
            approve = self.client.post(f"/api/v1/questions/{item_id}/review-actions", json={"action": "approve"})

        self.assertEqual(approve.status_code, 200)
        item = self.client.get(f"/api/v1/questions/{item_id}").json()
        self.assertEqual(item["current_status"], "approved")

    def test_confirm_alias_updates_current_status(self) -> None:
        with self._patched_runtime():
            item_id = self._generate_one().json()["items"][0]["item_id"]
            confirm = self.client.post(f"/api/v1/questions/{item_id}/confirm", json={})

        self.assertEqual(confirm.status_code, 200)
        item = self.client.get(f"/api/v1/questions/{item_id}").json()
        self.assertEqual(item["current_status"], "approved")
        self.assertEqual(item["latest_action"], "confirm")

    def test_validation_fail_marks_auto_failed(self) -> None:
        with self._patched_runtime(invalid_output=True):
            response = self._generate_one()

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(len(payload["items"]), 1)
        item = payload["items"][0]
        self.assertEqual(item["current_status"], "auto_failed")
        self.assertIn("blocked_attempt_returned_for_review", item["notes"])
        self.assertTrue(
            any("manual review" in warning for warning in payload.get("warnings", []))
        )

    def test_review_list_filters_by_status(self) -> None:
        with self._patched_runtime():
            item_id = self._generate_one().json()["items"][0]["item_id"]
            self.client.post(f"/api/v1/questions/{item_id}/review-actions", json={"action": "approve"})

        approved = self.client.get("/api/v1/review/items?status=approved")
        pending = self.client.get("/api/v1/review/items?status=pending_review")
        self.assertEqual(approved.status_code, 200)
        self.assertEqual(approved.json()["count"], 1)
        self.assertEqual(pending.json()["count"], 0)

    def test_batch_detail_returns_status_stats(self) -> None:
        with self._patched_runtime():
            response = self.client.post(
                "/api/v1/questions/generate",
                json={"question_focus": "center_understanding", "difficulty_level": "medium", "count": 2},
            )
            batch_id = response.json()["batch_id"]
            item_id = response.json()["items"][0]["item_id"]
            self.client.post(f"/api/v1/questions/{item_id}/review-actions", json={"action": "approve"})

        batch = self.client.get(f"/api/v1/review/batches/{batch_id}")
        self.assertEqual(batch.status_code, 200)
        payload = batch.json()
        self.assertEqual(payload["approved_count"], 1)
        self.assertEqual(payload["pending_count"], 1)
        self.assertEqual(len(payload["items"]), 2)

    def test_history_exposes_versions_and_action_chain(self) -> None:
        with self._patched_runtime():
            item_id = self._generate_one().json()["items"][0]["item_id"]
            self.client.post(
                f"/api/v1/questions/{item_id}/review-actions",
                json={"action": "minor_edit", "instruction": "调整措辞"},
            )
            self.client.post(f"/api/v1/questions/{item_id}/review-actions", json={"action": "approve"})

        history = self.client.get(f"/api/v1/review/items/{item_id}/history")
        self.assertEqual(history.status_code, 200)
        payload = history.json()
        self.assertEqual(payload["current_version_no"], 2)
        self.assertEqual(len(payload["versions"]), 2)
        self.assertEqual(len(payload["review_actions"]), 2)
        self.assertEqual(payload["review_actions"][0]["action_type"], "approve")
        self.assertTrue(payload["versions"][0]["material_text"])
        self.assertTrue(payload["versions"][0]["stem"])
        self.assertIn("A", payload["versions"][0]["options"])
        self.assertTrue(payload["versions"][0]["analysis"])

    def test_manual_edit_keeps_prior_versions_visible_in_history(self) -> None:
        with self._patched_runtime():
            item_id = self._generate_one().json()["items"][0]["item_id"]
            self.client.post(
                f"/api/v1/questions/{item_id}/review-actions",
                json={"action": "question_modify", "control_overrides": {"type_slots": {"abstraction_level": "high"}}},
            )
            self.client.post(
                f"/api/v1/questions/{item_id}/review-actions",
                json={
                    "action": "manual_edit",
                    "instruction": "manual patch",
                    "control_overrides": {
                        "manual_patch": {
                            "stem": "手工编辑后的题干",
                            "options": {"A": "甲", "B": "乙", "C": "丙", "D": "丁"},
                            "answer": "A",
                            "analysis": "手工编辑后的解析",
                        }
                    },
                },
            )

        history = self.client.get(f"/api/v1/review/items/{item_id}/history")
        self.assertEqual(history.status_code, 200)
        payload = history.json()
        self.assertEqual(payload["current_version_no"], 3)
        self.assertEqual([version["source_action"] for version in payload["versions"]], ["manual_edit", "question_modify", "generate"])
        self.assertEqual(payload["current_version"]["stem"], "手工编辑后的题干")
        self.assertEqual(payload["current_version"]["validation_result"], {})
        self.assertEqual(payload["current_version"]["evaluation_result"], {})

    def test_confirm_records_targeted_repair_approval_basis(self) -> None:
        with self._patched_runtime():
            item_id = self._generate_one().json()["items"][0]["item_id"]
            self.client.post(
                f"/api/v1/questions/{item_id}/review-actions",
                json={"action": "minor_edit", "instruction": "调整措辞"},
            )
            self._force_item_approvable(item_id)
            confirm = self.client.post(f"/api/v1/questions/{item_id}/confirm", json={})

        self.assertEqual(confirm.status_code, 200)
        latest_action = self.client.get(f"/api/v1/questions/{item_id}/review-actions").json()[0]
        self.assertEqual(latest_action["action_type"], "confirm")
        self.assertEqual(latest_action["payload"]["approval_basis"], "targeted_repair_high_trust")
        self.assertEqual(latest_action["payload"]["preceding_effective_action"], "minor_edit")
        self.assertEqual(latest_action["payload"]["preceding_semantic_class"], "targeted_repair")
        self.assertEqual(latest_action["payload"]["preceding_trust_level"], "high")
        self.assertFalse(latest_action["payload"]["preceding_truth_touched"])
        self.assertFalse(latest_action["payload"]["preceding_material_boundary_crossed"])

    def test_second_generation_marks_material_as_previously_used(self) -> None:
        with self._patched_runtime():
            first = self._generate_one()
            second = self._generate_one()

        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)
        second_material = second.json()["items"][0]["material_selection"]
        self.assertTrue(second_material["previously_used"])
        self.assertEqual(second_material["usage_count_before"], 1)

    def test_prompt_template_version_is_recorded(self) -> None:
        with self._patched_runtime():
            item_id = self._generate_one().json()["items"][0]["item_id"]

        history = self.client.get(f"/api/v1/review/items/{item_id}/history").json()
        current_version = history["current_version"]
        self.assertTrue(current_version["prompt_template_name"].startswith("main_idea_center_understanding_generate"))
        self.assertTrue(current_version["prompt_template_version"])

    def test_controls_meta_endpoint_returns_controls(self) -> None:
        response = self.client.get("/api/v1/meta/question-types/main_idea/controls")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(
            [control["control_key"] for control in payload["controls"]],
            ["abstraction_level", "statement_visibility", "main_point_source"],
        )
        self.assertEqual(payload["controls"][0]["label"], "抽象层级")

    def test_sentence_order_controls_meta_marks_multi_select_limit(self) -> None:
        response = self.client.get("/api/v1/meta/question-types/sentence_order/controls")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        distractor_modes = next(control for control in payload["controls"] if control["control_key"] == "distractor_modes")
        self.assertEqual(distractor_modes["max_selected"], 2)
        self.assertEqual(distractor_modes["options"][0]["label"], "首句错置")

    def test_item_controls_endpoint_returns_current_values(self) -> None:
        with self._patched_runtime():
            item_id = self._generate_one().json()["items"][0]["item_id"]

        response = self.client.get(f"/api/v1/questions/{item_id}/controls")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["item_id"], item_id)
        self.assertTrue(any(control["control_key"] == "abstraction_level" for control in payload["controls"]))
        self.assertFalse(any(control["control_key"] == "difficulty_target" for control in payload["controls"]))

    def test_diff_endpoint_detects_version_changes(self) -> None:
        with self._patched_runtime():
            item_id = self._generate_one().json()["items"][0]["item_id"]
            self.client.post(
                f"/api/v1/questions/{item_id}/review-actions",
                json={"action": "minor_edit", "instruction": "调整表述"},
            )

        diff = self.client.get(f"/api/v1/review/items/{item_id}/diff?from_version=1&to_version=2")
        self.assertEqual(diff.status_code, 200)
        payload = diff.json()
        self.assertEqual(payload["from_version"], 1)
        self.assertEqual(payload["to_version"], 2)

    def test_judge_result_is_returned_and_stored(self) -> None:
        with self._patched_runtime():
            item_id = self._generate_one().json()["items"][0]["item_id"]

        item = self.client.get(f"/api/v1/questions/{item_id}").json()
        self.assertIn("evaluation_result", item)
        self.assertEqual(item["evaluation_result"]["provider"], "generation_llm")
        self.assertIsInstance(item["evaluation_result"]["raw"], dict)

    def test_fine_tune_alias_derives_new_version(self) -> None:
        with self._patched_runtime():
            item_id = self._generate_one().json()["items"][0]["item_id"]
            response = self.client.post(
                f"/api/v1/questions/{item_id}/fine-tune",
                json={"instruction": "优化题干措辞"},
            )

        self.assertEqual(response.status_code, 200)
        history = self.client.get(f"/api/v1/review/items/{item_id}/history").json()
        self.assertEqual(history["current_version_no"], 2)
        self.assertEqual(history["versions"][0]["source_action"], "minor_edit")
        self.assertEqual(history["review_actions"][0]["action_type"], "minor_edit")
        self.assertEqual(history["review_actions"][0]["payload"]["requested_action"], "fine_tune")
        self.assertEqual(history["review_actions"][0]["payload"]["effective_action"], "minor_edit")

    def test_question_modify_rejects_material_input_outside_allowlist(self) -> None:
        with self._patched_runtime():
            item_id = self._generate_one().json()["items"][0]["item_id"]
            response = self.client.post(
                f"/api/v1/questions/{item_id}/review-actions",
                json={"action": "question_modify", "control_overrides": {"material_text": "manual replacement material"}},
            )

        self.assertEqual(response.status_code, 422)
        payload = response.json()
        self.assertEqual(payload["error"]["details"]["action"], "question_modify")
        self.assertIn("material_text", payload["error"]["details"]["disallowed_input_fields"])

    def test_question_modify_accepts_allowed_control_fields(self) -> None:
        with self._patched_runtime():
            item_id = self._generate_one().json()["items"][0]["item_id"]
            response = self.client.post(
                f"/api/v1/questions/{item_id}/review-actions",
                json={"action": "question_modify", "control_overrides": {"type_slots": {"abstraction_level": "high"}}},
            )

        self.assertEqual(response.status_code, 200)
        latest_action = self.client.get(f"/api/v1/questions/{item_id}/review-actions").json()[0]
        self.assertEqual(latest_action["payload"]["requested_action"], "question_modify")
        self.assertEqual(
            latest_action["payload"]["patch"]["control_overrides"],
            {"type_slots": {"abstraction_level": "high"}},
        )

    def test_review_action_rejects_unknown_top_level_field(self) -> None:
        with self._patched_runtime():
            item_id = self._generate_one().json()["items"][0]["item_id"]
            response = self.client.post(
                f"/api/v1/questions/{item_id}/review-actions",
                json={
                    "action": "question_modify",
                    "control_overrides": {"difficulty_target": "hard"},
                    "unexpected_field": True,
                },
            )

        self.assertEqual(response.status_code, 422)

    def test_question_modify_rejects_unknown_nested_extra_constraints(self) -> None:
        with self._patched_runtime():
            item_id = self._generate_one().json()["items"][0]["item_id"]
            response = self.client.post(
                f"/api/v1/questions/{item_id}/review-actions",
                json={
                    "action": "question_modify",
                    "control_overrides": {"extra_constraints": {"new_semantic_switch": True}},
                },
            )

        self.assertEqual(response.status_code, 422)
        payload = response.json()
        self.assertIn("extra_constraints.new_semantic_switch", payload["error"]["details"]["disallowed_input_fields"])

    def test_question_modify_rejects_unknown_nested_type_slots(self) -> None:
        with self._patched_runtime():
            item_id = self._generate_one().json()["items"][0]["item_id"]
            response = self.client.post(
                f"/api/v1/questions/{item_id}/review-actions",
                json={
                    "action": "question_modify",
                    "control_overrides": {"type_slots": {"new_slot": "value"}},
                },
            )

        self.assertEqual(response.status_code, 422)
        payload = response.json()
        self.assertIn("type_slots.new_slot", payload["error"]["details"]["disallowed_input_fields"])

    def test_manual_edit_rejects_unknown_option_patch_key(self) -> None:
        with self._patched_runtime():
            item_id = self._generate_one().json()["items"][0]["item_id"]
            response = self.client.post(
                f"/api/v1/questions/{item_id}/review-actions",
                json={
                    "action": "manual_edit",
                    "instruction": "manual patch",
                    "control_overrides": {
                        "manual_patch": {
                            "options": {"A": "甲", "B": "乙", "E": "越界"},
                            "answer": "A",
                        }
                    },
                },
            )

        self.assertEqual(response.status_code, 422)
        payload = response.json()
        self.assertIn("manual_patch.options.E", payload["error"]["details"]["disallowed_input_fields"])

    def test_approve_records_material_regenerate_approval_basis(self) -> None:
        with self._patched_runtime():
            item_id = self._generate_one().json()["items"][0]["item_id"]
            self.client.post(
                f"/api/v1/questions/{item_id}/review-actions",
                json={"action": "text_modify", "control_overrides": {"material_text": "manual replacement material"}},
            )
            self._force_item_approvable(item_id)
            approve = self.client.post(f"/api/v1/questions/{item_id}/review-actions", json={"action": "approve"})

        self.assertEqual(approve.status_code, 200)
        latest_action = self.client.get(f"/api/v1/questions/{item_id}/review-actions").json()[0]
        self.assertEqual(latest_action["action_type"], "approve")
        self.assertEqual(latest_action["payload"]["approval_basis"], "material_regenerate_medium")
        self.assertEqual(latest_action["payload"]["preceding_effective_action"], "text_modify")
        self.assertEqual(latest_action["payload"]["preceding_semantic_class"], "material_regenerate")
        self.assertEqual(latest_action["payload"]["preceding_trust_level"], "medium")
        self.assertTrue(latest_action["payload"]["preceding_material_boundary_crossed"])

    def test_text_modify_rejects_non_material_input_outside_allowlist(self) -> None:
        with self._patched_runtime():
            item_id = self._generate_one().json()["items"][0]["item_id"]
            response = self.client.post(
                f"/api/v1/questions/{item_id}/review-actions",
                json={"action": "text_modify", "control_overrides": {"pattern_id": "alt_pattern"}},
            )

        self.assertEqual(response.status_code, 422)
        payload = response.json()
        self.assertEqual(payload["error"]["details"]["action"], "text_modify")
        self.assertIn("pattern_id", payload["error"]["details"]["disallowed_input_fields"])

    def test_text_modify_accepts_allowed_material_fields(self) -> None:
        with self._patched_runtime():
            item_id = self._generate_one().json()["items"][0]["item_id"]
            response = self.client.post(
                f"/api/v1/questions/{item_id}/review-actions",
                json={"action": "text_modify", "control_overrides": {"material_text": "manual replacement material"}},
            )

        self.assertEqual(response.status_code, 200)
        latest_action = self.client.get(f"/api/v1/questions/{item_id}/review-actions").json()[0]
        self.assertEqual(latest_action["action_type"], "text_modify")
        self.assertEqual(latest_action["payload"]["effective_action"], "text_modify")
        self.assertEqual(
            latest_action["payload"]["patch"]["control_overrides"],
            {"material_text": "manual replacement material"},
        )

    def test_manual_edit_material_patch_is_marked_as_material_boundary_crossed(self) -> None:
        with self._patched_runtime():
            item_id = self._generate_one().json()["items"][0]["item_id"]
            response = self.client.post(
                f"/api/v1/questions/{item_id}/review-actions",
                json={
                    "action": "manual_edit",
                    "instruction": "manual patch",
                    "control_overrides": {
                        "manual_patch": {
                            "stem": "手工改题干",
                            "options": {"A": "甲", "B": "乙", "C": "丙", "D": "丁"},
                            "answer": "A",
                            "analysis": "手工改解析",
                            "material_text": "手工改材料",
                        }
                    },
                },
            )

        self.assertEqual(response.status_code, 200)
        latest_action = self.client.get(f"/api/v1/questions/{item_id}/review-actions").json()[0]
        self.assertEqual(latest_action["action_type"], "manual_edit")
        self.assertEqual(latest_action["payload"]["semantic_class"], "manual_override_high_risk")
        self.assertTrue(latest_action["payload"]["material_boundary_crossed"])
        self.assertEqual(latest_action["payload"]["trust_level"], "low")
        self.assertEqual(latest_action["payload"]["audit_reason"], "manual_edit_result_touched_material_and_truth_like_fields")

    def test_confirm_accepts_low_trust_manual_override(self) -> None:
        with self._patched_runtime():
            item_id = self._save_base_review_item(item_id="item-manual-confirm")["item_id"]
            self.client.post(
                f"/api/v1/questions/{item_id}/review-actions",
                json={
                    "action": "manual_edit",
                    "instruction": "manual patch",
                    "control_overrides": {
                        "manual_patch": {
                            "stem": "手工改题干",
                            "options": {"A": "甲", "B": "乙", "C": "丙", "D": "丁"},
                            "answer": "A",
                            "analysis": "手工改解析",
                            "material_text": "手工改材料",
                        }
                    },
                },
            )
            confirm = self.client.post(f"/api/v1/questions/{item_id}/confirm", json={})

        self.assertEqual(confirm.status_code, 200)
        latest_action = self.client.get(f"/api/v1/questions/{item_id}/review-actions").json()[0]
        self.assertEqual(latest_action["action_type"], "confirm")
        self.assertEqual(latest_action["payload"]["approval_basis"], "manual_override_high_risk_low")
        self.assertEqual(latest_action["payload"]["preceding_effective_action"], "manual_edit")
        self.assertEqual(latest_action["payload"]["preceding_semantic_class"], "manual_override_high_risk")
        self.assertEqual(latest_action["payload"]["preceding_trust_level"], "low")
        self.assertTrue(latest_action["payload"]["preceding_truth_touched"])
        self.assertTrue(latest_action["payload"]["preceding_material_boundary_crossed"])


    def test_metrics_summary_aggregates(self) -> None:
        with self._patched_runtime():
            item_id = self._generate_one().json()["items"][0]["item_id"]
            self.client.post(f"/api/v1/questions/{item_id}/review-actions", json={"action": "approve"})

        metrics = self.client.get("/api/v1/metrics/review-summary")
        self.assertEqual(metrics.status_code, 200)
        payload = metrics.json()
        self.assertEqual(payload["total_count"], 1)
        self.assertEqual(payload["approved_count"], 1)

    def test_delivery_view_only_returns_approved_current_versions(self) -> None:
        with self._patched_runtime():
            response = self.client.post(
                "/api/v1/questions/generate",
                json={"question_focus": "center_understanding", "difficulty_level": "medium", "count": 2},
            )
            batch_id = response.json()["batch_id"]
            first_item_id = response.json()["items"][0]["item_id"]
            self.client.post(f"/api/v1/questions/{first_item_id}/review-actions", json={"action": "approve"})

        delivery = self.client.get(f"/api/v1/review/batches/{batch_id}/delivery")
        self.assertEqual(delivery.status_code, 200)
        payload = delivery.json()
        self.assertEqual(payload["approved_count"], 1)
        self.assertEqual(len(payload["items"]), 1)

    def test_review_history_marks_sentence_fill_item_blocked_without_crashing(self) -> None:
        self._save_sentence_fill_item(
            item_id="sf-1",
            resolved_slots={
                "blank_position": "middle",
                "function_type": "legacy_new_name",
                "logic_relation": "continuation",
            },
        )

        history = self.client.get("/api/v1/review/items/sf-1/history")
        self.assertEqual(history.status_code, 200)
        view = history.json()["item"]["sentence_fill_export_view"]
        self.assertEqual(view["status"], "blocked")
        self.assertEqual(view["blocked_reason"], "unknown_sentence_fill_function_type_alias:legacy_new_name")

    def test_review_history_marks_center_understanding_item_blocked_without_crashing(self) -> None:
        self._save_center_understanding_item(
            item_id="center-1",
            item_business_subtype="title_selection",
            request_business_subtype="center_understanding",
        )

        history = self.client.get("/api/v1/review/items/center-1/history")
        self.assertEqual(history.status_code, 200)
        view = history.json()["item"]["center_understanding_export_view"]
        self.assertEqual(view["status"], "blocked")
        self.assertEqual(
            view["blocked_reason"],
            "title_selection_leaked_to_center_understanding_export:item.business_subtype",
        )

    def test_review_history_marks_sentence_order_item_blocked_without_crashing(self) -> None:
        self._save_sentence_order_item(
            item_id="so-1",
            material_resolved_slots={"candidate_type": "ordered_unit_group"},
            runtime_binding={
                "opening_rule": "explicit_opening",
                "closing_rule": "summary_or_conclusion",
            },
        )

        history = self.client.get("/api/v1/review/items/so-1/history")
        self.assertEqual(history.status_code, 200)
        view = history.json()["item"]["sentence_order_export_view"]
        self.assertEqual(view["status"], "blocked")
        self.assertEqual(view["blocked_reason"], "ambiguous_sentence_order_closing_anchor:summary_or_conclusion")
        self.assertIsNone(view["closing_anchor_type"])
        self.assertNotIn("closing_rule", view)

    def test_material_policy_affects_selection(self) -> None:
        with self._patched_runtime(material_policy_sensitive=True):
            response = self.client.post(
                "/api/v1/questions/generate",
                json={
                    "question_focus": "center_understanding",
                    "difficulty_level": "medium",
                    "count": 1,
                    "material_policy": {
                        "allow_reuse": True,
                        "preferred_document_genres": ["科普文"],
                    },
                },
            )

        self.assertEqual(response.status_code, 200)
        item = response.json()["items"][0]
        self.assertEqual(item["material_selection"]["document_genre"], "科普文")

    def test_auth_token_is_enforced_when_enabled(self) -> None:
        os.environ["PROMPT_SERVICE_API_TOKEN"] = "demo-token"
        os.environ["PROMPT_SERVICE_SECURITY_ENABLED"] = "true"
        get_settings.cache_clear()

        unauthorized = self.client.get("/api/v1/meta/question-types")
        authorized = self.client.get(
            "/api/v1/meta/question-types",
            headers={"Authorization": "Bearer demo-token"},
        )

        self.assertEqual(unauthorized.status_code, 401)
        self.assertEqual(authorized.status_code, 200)

    def _generate_one(self):
        return self.client.post(
            "/api/v1/questions/generate",
            json={"question_focus": "center_understanding", "difficulty_level": "medium", "count": 1},
        )

    def _force_item_approvable(self, item_id: str) -> None:
        item = self.repository.get_item(item_id)
        item["current_status"] = "pending_review"
        item["statuses"]["review_status"] = "waiting_review"
        item["statuses"]["validation_status"] = "passed"
        item["validation_result"] = {
            **(item.get("validation_result") or {}),
            "passed": True,
            "validation_status": "passed",
        }
        self.repository.save_item(item)

    def _save_base_review_item(self, *, item_id: str) -> dict:
        item = {
            "item_id": item_id,
            "batch_id": "batch-dp",
            "question_type": "main_idea",
            "business_subtype": "title_selection",
            "pattern_id": "pattern-1",
            "difficulty_target": "medium",
            "resolved_slots": {},
            "skeleton": {},
            "control_logic": {},
            "generation_logic": {},
            "prompt_package": {
                "system_prompt": "sys",
                "user_prompt": "user",
                "fewshot_examples": [],
                "merged_prompt": "sys\nuser",
            },
            "generated_question": {
                "question_type": "main_idea",
                "business_subtype": "title_selection",
                "pattern_id": "pattern-1",
                "stem": "以下哪一项最能概括材料主旨？",
                "options": {"A": "正确概括主旨", "B": "原始干扰项B", "C": "原始干扰项C", "D": "原始干扰项D"},
                "answer": "A",
                "analysis": "A项正确，因为A项最能概括材料主旨。",
            },
            "material_selection": {
                "material_id": "mat-dp-1",
                "article_id": "art-dp-1",
                "text": "材料原文",
                "source": {"site": "demo"},
                "selection_reason": "test seed",
            },
            "material_text": "材料原文",
            "material_source": {"site": "demo"},
            "request_snapshot": {
                "question_type": "main_idea",
                "business_subtype": "title_selection",
                "pattern_id": "pattern-1",
                "difficulty_target": "medium",
                "source_form": {},
                "type_slots": {},
                "extra_constraints": {},
            },
            "statuses": {
                "build_status": "success",
                "review_status": "waiting_review",
                "generation_status": "success",
                "validation_status": "passed",
            },
            "validation_result": {
                "passed": True,
                "validation_status": "passed",
                "checks": {
                    "analysis_answer_consistency": {"passed": True},
                    "analysis_mentions_correct_option_text": {"passed": True},
                },
            },
            "evaluation_result": {"overall_score": 80, "provider": "openai", "raw": {"judge_prompt": "demo"}},
            "current_version_no": 1,
            "current_status": "pending_review",
            "revision_count": 0,
            "notes": [],
            "selected_pattern": "pattern-1",
        }
        self.repository.save_item(item)
        return item

    def _save_sentence_fill_item(self, *, item_id: str, resolved_slots: dict[str, str]) -> None:
        self.repository.save_batch(
            "batch-sf",
            {
                "batch_meta": {
                    "requested_count": 1,
                    "effective_count": 1,
                    "question_type": "sentence_fill",
                    "business_subtype": "sentence_fill_selection",
                    "difficulty_target": "medium",
                }
            },
        )
        item = {
            "item_id": item_id,
            "batch_id": "batch-sf",
            "question_type": "sentence_fill",
            "business_subtype": "sentence_fill_selection",
            "pattern_id": "bridge_transition",
            "difficulty_target": "medium",
            "resolved_slots": dict(resolved_slots),
            "skeleton": {},
            "control_logic": {},
            "generation_logic": {},
            "prompt_package": {},
            "generated_question": {
                "question_type": "sentence_fill",
                "business_subtype": "sentence_fill_selection",
                "pattern_id": "bridge_transition",
                "stem": "填入画横线部分最恰当的一项是（ ）。",
                "options": {"A": "甲", "B": "乙", "C": "丙", "D": "丁"},
                "answer": "A",
                "analysis": "解析",
            },
            "material_selection": {
                "material_id": "mat-sf-1",
                "article_id": "art-sf-1",
                "text": "材料",
                "resolved_slots": dict(resolved_slots),
                "source": {"site": "demo"},
            },
            "request_snapshot": {
                "type_slots": {
                    "blank_position": "opening",
                    "function_type": "summary",
                    "logic_relation": "summary",
                }
            },
            "statuses": {
                "review_status": "waiting_review",
                "generation_status": "success",
                "validation_status": "passed",
            },
            "validation_result": {"passed": True, "validation_status": "passed"},
            "current_version_no": 1,
            "current_status": "pending_review",
            "selected_pattern": "bridge_transition",
        }
        self.repository.save_item(item)

    def _save_center_understanding_item(
        self,
        *,
        item_id: str,
        item_business_subtype: str,
        request_business_subtype: str,
    ) -> None:
        self.repository.save_batch(
            "batch-center",
            {
                "batch_meta": {
                    "requested_count": 1,
                    "effective_count": 1,
                    "question_type": "main_idea",
                    "business_subtype": "center_understanding",
                    "difficulty_target": "medium",
                }
            },
        )
        item = {
            "item_id": item_id,
            "batch_id": "batch-center",
            "question_type": "main_idea",
            "business_subtype": item_business_subtype,
            "pattern_id": "whole_passage_integration",
            "difficulty_target": "medium",
            "resolved_slots": {},
            "skeleton": {},
            "control_logic": {},
            "generation_logic": {},
            "prompt_package": {},
            "generated_question": {
                "question_type": "main_idea",
                "business_subtype": item_business_subtype,
                "pattern_id": "whole_passage_integration",
                "stem": "主旨是？",
                "options": {"A": "甲", "B": "乙", "C": "丙", "D": "丁"},
                "answer": "A",
                "analysis": "解析",
            },
            "material_selection": {
                "material_id": "mat-center-1",
                "article_id": "art-center-1",
                "text": "材料",
                "source": {"site": "demo"},
            },
            "request_snapshot": {
                "business_subtype": request_business_subtype,
                "question_card_id": "question.center_understanding.standard_v1",
            },
            "statuses": {
                "review_status": "waiting_review",
                "generation_status": "success",
                "validation_status": "passed",
            },
            "validation_result": {"passed": True, "validation_status": "passed"},
            "current_version_no": 1,
            "current_status": "pending_review",
            "selected_pattern": "whole_passage_integration",
        }
        self.repository.save_item(item)

    def _save_sentence_order_item(
        self,
        *,
        item_id: str,
        material_resolved_slots: dict[str, str] | None = None,
        runtime_binding: dict[str, str] | None = None,
    ) -> None:
        self.repository.save_batch(
            "batch-so",
            {
                "batch_meta": {
                    "requested_count": 1,
                    "effective_count": 1,
                    "question_type": "sentence_order",
                    "business_subtype": "sentence_order_selection",
                    "difficulty_target": "medium",
                }
            },
        )
        item = {
            "item_id": item_id,
            "batch_id": "batch-so",
            "question_type": "sentence_order",
            "business_subtype": "sentence_order_selection",
            "pattern_id": "dual_anchor_lock",
            "difficulty_target": "medium",
            "resolved_slots": {},
            "skeleton": {},
            "control_logic": {},
            "generation_logic": {},
            "prompt_package": {},
            "generated_question": {
                "question_type": "sentence_order",
                "business_subtype": "sentence_order_selection",
                "pattern_id": "dual_anchor_lock",
                "stem": "将以下6个句子重新排列，语序正确的是：",
                "options": {"A": "1-2-3-4-5-6", "B": "1-3-2-4-5-6", "C": "2-1-3-4-5-6", "D": "1-2-4-3-5-6"},
                "answer": "A",
                "analysis": "先看首句和尾句，再看中间衔接。",
            },
            "material_selection": {
                "material_id": "mat-so-1",
                "article_id": "art-so-1",
                "text": "①先交代背景。②再说明问题。③随后分析原因。④接着提出对策。⑤再补充条件。⑥最后总结判断。",
                "resolved_slots": dict(material_resolved_slots or {}),
                "runtime_binding": dict(runtime_binding or {}),
                "source": {"site": "demo"},
            },
            "request_snapshot": {
                "type_slots": {
                    "candidate_type": "sentence_block_group",
                    "opening_anchor_type": "explicit_topic",
                    "closing_anchor_type": "conclusion",
                }
            },
            "statuses": {
                "review_status": "approved",
                "generation_status": "success",
                "validation_status": "passed",
            },
            "validation_result": {"passed": True, "validation_status": "passed"},
            "current_version_no": 1,
            "current_status": "approved",
            "selected_pattern": "dual_anchor_lock",
        }
        self.repository.save_item(item)

    def _patched_runtime(self, *, invalid_output: bool = False, material_policy_sensitive: bool = False):
        def fake_select_materials(_, **kwargs):
            exclude_ids = kwargs.get("exclude_material_ids") or set()
            material_policy = kwargs.get("material_policy")
            items = []
            if "mat-1" not in exclude_ids:
                items.append(
                    MaterialSelectionResult(
                        material_id="mat-1",
                        article_id="art-1",
                        text="政策材料一。材料测试内容。",
                        source={"site": "demo"},
                        source_tail="demo tail",
                        primary_label="main_idea",
                        document_genre="政策文",
                        quality_score=0.91,
                        fit_scores={"main_idea": 0.88},
                        knowledge_tags=["genre:政策文"],
                        selection_reason="best fit",
                    )
                )
            items.append(
                MaterialSelectionResult(
                    material_id="mat-2",
                    article_id="art-2",
                    text="科普材料二。备选内容。",
                    source={"site": "demo"},
                    source_tail="demo tail 2",
                    primary_label="main_idea",
                    document_genre="科普文",
                    quality_score=0.83,
                    fit_scores={"main_idea": 0.79},
                    knowledge_tags=["genre:科普文"],
                    selection_reason="fallback fit",
                )
            )
            if material_policy_sensitive and material_policy and "科普文" in material_policy.preferred_document_genres:
                items = [items[-1], *items[:-1]]
            return items[: kwargs.get("count", 1)], []

        def fake_generate_json(_, **kwargs):
            if kwargs.get("schema_name") == "judge_result":
                return {
                    "provider": "openai",
                    "question_type_fit": 0.82,
                    "difficulty_fit": 0.74,
                    "material_alignment": 0.79,
                    "distractor_quality": 0.77,
                    "answer_analysis_consistency": 0.81,
                    "overall_score": 78.0,
                    "judge_reason": "LLM judge accepted the generated question as exam-like and structurally valid.",
                    "raw": {"judge_model": "fake-openai"},
                }
            if invalid_output:
                return {
                    "stem": "以下哪一项最能概括文段主旨？",
                    "options": {"A": "A项", "B": "A项", "C": "", "D": "A项"},
                    "answer": "",
                    "analysis": "",
                    "metadata": {"mocked": True},
                }
            return {
                "stem": "以下哪一项最能概括文段主旨？",
                "options": {"A": "A项", "B": "B项", "C": "C项", "D": "D项"},
                "answer": "A",
                "analysis": "正确项为A项，因为它最能对应主旨。",
                "metadata": {"mocked": True},
            }

        stack = ExitStack()
        stack.enter_context(
            patch("app.services.material_bridge.MaterialBridgeService.select_materials", new=fake_select_materials)
        )
        stack.enter_context(
            patch("app.services.llm_gateway.LLMGatewayService.generate_json", new=fake_generate_json)
        )
        return stack
