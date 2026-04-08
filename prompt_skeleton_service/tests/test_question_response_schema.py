from __future__ import annotations

from unittest import TestCase

from fastapi.testclient import TestClient

from app.core.dependencies import get_question_repository
from app.main import app
from app.schemas.question import QuestionGenerationItem


def _base_item() -> dict:
    return {
        "item_id": "item-1",
        "batch_id": "batch-1",
        "question_type": "main_idea",
        "business_subtype": "title_selection",
        "pattern_id": "pattern-1",
        "current_version_no": 1,
        "current_status": "pending_review",
        "latest_action": "generate",
        "selected_pattern": "pattern-1",
        "resolved_slots": {},
        "skeleton": {},
        "difficulty_target": "medium",
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
            "pattern_id": "pattern-1",
            "stem": "题干",
            "options": {"A": "甲", "B": "乙", "C": "丙", "D": "丁"},
            "answer": "A",
            "analysis": "解析",
        },
        "validation_result": {"validation_status": "passed", "passed": True, "score": 100},
        "statuses": {
            "build_status": "success",
            "generation_status": "success",
            "validation_status": "passed",
            "review_status": "waiting_review",
        },
    }


class QuestionResponseSchemaTest(TestCase):
    def setUp(self) -> None:
        self.client = TestClient(app)

    def tearDown(self) -> None:
        self.client.close()
        app.dependency_overrides.clear()

    def test_generation_item_flattens_nested_feedback_snapshot(self) -> None:
        item = _base_item()
        item["material_source"] = {
            "feedback_snapshot": {
                "selection_state": "hold",
                "review_like_risk": True,
                "repair_suggested": True,
                "decision_reason": "borderline_hold_candidate",
                "repair_reason": "role_ambiguity_repairable_risk",
                "quality_difficulty_note": "not_hard_but_currently_weak_candidate",
                "final_candidate_score": 0.61234,
                "readiness_score": 0.73456,
                "total_penalty": 0.11222,
                "difficulty_band_hint": "medium",
                "difficulty_vector": {
                    "ambiguity_score": 0.58,
                    "complexity_score": 0.71,
                },
                "key_penalties": {"role_ambiguity_penalty": 0.12},
                "key_difficulty_dimensions": {"complexity_score": 0.71},
            }
        }

        parsed = QuestionGenerationItem.model_validate(item)

        self.assertEqual(parsed.selection_state, "hold")
        self.assertTrue(parsed.review_like_risk)
        self.assertTrue(parsed.repair_suggested)
        self.assertEqual(parsed.decision_reason, "borderline_hold_candidate")
        self.assertEqual(parsed.repair_reason, "role_ambiguity_repairable_risk")
        self.assertEqual(parsed.quality_note, "not_hard_but_currently_weak_candidate")
        self.assertEqual(parsed.final_candidate_score, 0.6123)
        self.assertEqual(parsed.readiness_score, 0.7346)
        self.assertEqual(parsed.total_penalty, 0.1122)
        self.assertEqual(parsed.difficulty_band_hint, "medium")
        self.assertEqual(parsed.difficulty_vector, {"ambiguity_score": 0.58, "complexity_score": 0.71})
        self.assertEqual(parsed.key_penalties, {"role_ambiguity_penalty": 0.12})
        self.assertEqual(parsed.key_difficulty_dimensions, {"complexity_score": 0.71})
        self.assertEqual(parsed.feedback_snapshot["quality_note"], "not_hard_but_currently_weak_candidate")

    def test_generation_item_derives_feedback_from_material_decision_context_and_keeps_missing_values_explicit(self) -> None:
        item = _base_item()
        item["material_source"] = {
            "scoring": {
                "final_candidate_score": 0.0,
                "readiness_score": 0.0,
                "difficulty_vector": {
                    "ambiguity_score": 0.844,
                    "complexity_score": 0.0875,
                },
                "risk_penalties": {
                    "example_dominance_penalty": 1.0,
                    "role_ambiguity_penalty": 0.0,
                },
            },
            "decision_meta": {
                "selection_state": "weak_candidate",
                "review_like_risk": False,
                "repair_suggested": False,
                "decision_reason": "high_risk_but_not_high_difficulty",
                "scoring_summary": {
                    "difficulty_band_hint": "medium",
                    "total_penalty": 1.0,
                },
            },
        }

        parsed = QuestionGenerationItem.model_validate(item)

        self.assertEqual(parsed.selection_state, "weak_candidate")
        self.assertFalse(parsed.review_like_risk)
        self.assertFalse(parsed.repair_suggested)
        self.assertEqual(parsed.decision_reason, "high_risk_but_not_high_difficulty")
        self.assertIsNone(parsed.repair_reason)
        self.assertIsNone(parsed.quality_note)
        self.assertEqual(parsed.difficulty_band_hint, "medium")
        self.assertEqual(parsed.total_penalty, 1.0)
        self.assertEqual(parsed.key_penalties, {"example_dominance_penalty": 1.0})
        self.assertEqual(
            parsed.key_difficulty_dimensions,
            {"ambiguity_score": 0.844, "complexity_score": 0.0875},
        )
        self.assertIsNone(parsed.feedback_snapshot["repair_reason"])
        self.assertIsNone(parsed.feedback_snapshot["quality_note"])

    def test_question_item_route_returns_standardized_feedback_fields(self) -> None:
        item = _base_item()
        item["material_source"] = {
            "feedback_snapshot": {
                "selection_state": "hold",
                "review_like_risk": True,
                "repair_suggested": True,
                "decision_reason": "borderline_hold_candidate",
                "repair_reason": None,
                "quality_difficulty_note": None,
                "final_candidate_score": 0.5123,
                "readiness_score": 0.6888,
                "total_penalty": 0.2,
                "difficulty_band_hint": None,
                "difficulty_vector": {"ambiguity_score": 0.4},
                "key_penalties": {},
                "key_difficulty_dimensions": {},
            }
        }

        class _Repo:
            def get_item(self, item_id: str) -> dict | None:
                return item if item_id == "item-1" else None

        app.dependency_overrides[get_question_repository] = lambda: _Repo()

        response = self.client.get("/api/v1/questions/item-1")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["selection_state"], "hold")
        self.assertTrue(payload["review_like_risk"])
        self.assertTrue(payload["repair_suggested"])
        self.assertEqual(payload["final_candidate_score"], 0.5123)
        self.assertIsNone(payload["difficulty_band_hint"])
        self.assertIn("feedback_snapshot", payload)
        self.assertIn("quality_note", payload["feedback_snapshot"])
