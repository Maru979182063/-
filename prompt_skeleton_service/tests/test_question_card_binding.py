from __future__ import annotations

import sys
import types
from unittest import TestCase
from unittest.mock import patch


def _install_test_stubs() -> None:
    if "fastapi" not in sys.modules:
        fastapi = types.ModuleType("fastapi")
        fastapi.FastAPI = type("FastAPI", (), {})
        fastapi.Request = type("Request", (), {})
        sys.modules["fastapi"] = fastapi
    if "fastapi.responses" not in sys.modules:
        responses = types.ModuleType("fastapi.responses")
        responses.JSONResponse = type("JSONResponse", (), {})
        sys.modules["fastapi.responses"] = responses
    if "yaml" not in sys.modules:
        yaml = types.ModuleType("yaml")
        yaml.safe_load = lambda *args, **kwargs: {}
        sys.modules["yaml"] = yaml


_install_test_stubs()

from app.core.exceptions import DomainError
from app.services import question_card_binding as binding_module
from app.services.question_card_binding import QuestionCardBindingService


class QuestionCardBindingServiceUnitTest(TestCase):
    def setUp(self) -> None:
        self.registry = {
            "cards_by_id": {
                "question.title_selection.standard_v1": {
                    "card_id": "question.title_selection.standard_v1",
                    "business_family_id": "title_selection",
                    "runtime_binding": {
                        "question_type": "main_idea",
                        "business_subtype": "title_selection",
                    },
                },
                "question.sentence_order.standard_v1": {
                    "card_id": "question.sentence_order.standard_v1",
                    "business_family_id": "sentence_order",
                    "runtime_binding": {
                        "question_type": "sentence_order",
                        "business_subtype": None,
                    },
                },
                "question.duplicate.one": {
                    "card_id": "question.duplicate.one",
                    "business_family_id": "dup_one",
                    "runtime_binding": {
                        "question_type": "duplicate_type",
                        "business_subtype": "dup",
                    },
                },
                "question.duplicate.two": {
                    "card_id": "question.duplicate.two",
                    "business_family_id": "dup_two",
                    "runtime_binding": {
                        "question_type": "duplicate_type",
                        "business_subtype": "dup",
                    },
                },
            },
            "cards_by_runtime_binding": {
                ("main_idea", "title_selection"): [
                    {
                        "card_id": "question.title_selection.standard_v1",
                        "business_family_id": "title_selection",
                        "runtime_binding": {
                            "question_type": "main_idea",
                            "business_subtype": "title_selection",
                        },
                    }
                ],
                ("sentence_order", None): [
                    {
                        "card_id": "question.sentence_order.standard_v1",
                        "business_family_id": "sentence_order",
                        "runtime_binding": {
                            "question_type": "sentence_order",
                            "business_subtype": None,
                        },
                    }
                ],
                ("duplicate_type", "dup"): [
                    {
                        "card_id": "question.duplicate.one",
                        "business_family_id": "dup_one",
                        "runtime_binding": {
                            "question_type": "duplicate_type",
                            "business_subtype": "dup",
                        },
                    },
                    {
                        "card_id": "question.duplicate.two",
                        "business_family_id": "dup_two",
                        "runtime_binding": {
                            "question_type": "duplicate_type",
                            "business_subtype": "dup",
                        },
                    },
                ],
            },
        }
        self.registry_patcher = patch.object(binding_module, "load_question_card_registry", return_value=self.registry)
        self.registry_patcher.start()
        self.service = QuestionCardBindingService()

    def tearDown(self) -> None:
        self.registry_patcher.stop()

    def test_explicit_question_card_id_is_authoritative(self) -> None:
        binding = self.service.resolve(
            question_card_id="question.title_selection.standard_v1",
            question_type="main_idea",
            business_subtype="center_understanding",
            require_match=True,
        )

        self.assertEqual(binding["question_card_id"], "question.title_selection.standard_v1")
        self.assertEqual(binding["runtime_binding"]["question_type"], "main_idea")
        self.assertEqual(binding["runtime_binding"]["business_subtype"], "title_selection")
        self.assertIn("overrode_requested_runtime_binding", binding["warning"])

    def test_runtime_binding_lookup_resolves_sentence_order_card(self) -> None:
        binding = self.service.resolve(
            question_type="sentence_order",
            business_subtype=None,
            require_match=True,
        )

        self.assertEqual(binding["question_card_id"], "question.sentence_order.standard_v1")
        self.assertEqual(binding["binding_source"], "runtime_binding_lookup")

    def test_unmatched_runtime_binding_requires_explicit_question_card(self) -> None:
        with self.assertRaises(DomainError):
            self.service.resolve(
                question_type="main_idea",
                business_subtype="center_understanding",
                require_match=True,
            )

    def test_ambiguous_runtime_binding_requires_explicit_question_card(self) -> None:
        with self.assertRaises(DomainError):
            self.service.resolve(
                question_type="duplicate_type",
                business_subtype="dup",
                require_match=True,
            )
