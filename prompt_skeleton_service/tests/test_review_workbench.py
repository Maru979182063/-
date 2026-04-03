from __future__ import annotations

import os
from contextlib import ExitStack
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

from fastapi.testclient import TestClient

from app.core.dependencies import get_question_repository
from app.core.settings import get_settings
from app.main import app
from app.schemas.question import MaterialSelectionResult
from app.services.question_repository import QuestionRepository


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
        item = response.json()["items"][0]
        self.assertEqual(item["current_status"], "auto_failed")
        self.assertFalse(item["validation_result"]["passed"])

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
                json={"question_focus": "标题填入题", "difficulty_level": "中等", "count": 2},
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
        self.assertEqual(current_version["prompt_template_name"], "main_idea_generate_default")
        self.assertEqual(current_version["prompt_template_version"], "v2")

    def test_controls_meta_endpoint_returns_controls(self) -> None:
        response = self.client.get("/api/v1/meta/question-types/main_idea/controls")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(any(control["control_key"] == "difficulty_target" for control in payload["controls"]))
        self.assertTrue(any(control["control_key"] == "pattern_id" for control in payload["controls"]))

    def test_item_controls_endpoint_returns_current_values(self) -> None:
        with self._patched_runtime():
            item_id = self._generate_one().json()["items"][0]["item_id"]

        response = self.client.get(f"/api/v1/questions/{item_id}/controls")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["item_id"], item_id)
        self.assertTrue(any(control["control_key"] == "difficulty_target" for control in payload["controls"]))

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
        self.assertEqual(item["evaluation_result"]["provider"], "openai")
        self.assertIn("judge_prompt", item["evaluation_result"]["raw"])

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
                json={"question_focus": "标题填入题", "difficulty_level": "中等", "count": 2},
            )
            batch_id = response.json()["batch_id"]
            first_item_id = response.json()["items"][0]["item_id"]
            self.client.post(f"/api/v1/questions/{first_item_id}/review-actions", json={"action": "approve"})

        delivery = self.client.get(f"/api/v1/review/batches/{batch_id}/delivery")
        self.assertEqual(delivery.status_code, 200)
        payload = delivery.json()
        self.assertEqual(payload["approved_count"], 1)
        self.assertEqual(len(payload["items"]), 1)

    def test_material_policy_affects_selection(self) -> None:
        with self._patched_runtime(material_policy_sensitive=True):
            response = self.client.post(
                "/api/v1/questions/generate",
                json={
                    "question_focus": "标题填入题",
                    "difficulty_level": "中等",
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
            json={"question_focus": "标题填入题", "difficulty_level": "中等", "count": 1},
        )

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
