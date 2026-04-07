from __future__ import annotations

import sys
import types
from unittest import TestCase


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


_install_test_stubs()

from app.core.exceptions import DomainError
from app.schemas.config import QuestionTypeConfig
from app.services.slot_resolver import SlotResolverService


def _build_type_config(*, default_pattern_id: str | None) -> QuestionTypeConfig:
    return QuestionTypeConfig.model_validate(
        {
            "type_id": "main_idea",
            "display_name": "Main Idea",
            "task_definition": "Summarize the passage focus.",
            "skeleton": {
                "anchor_type": "summary",
                "operation_type": "select",
                "target_type": "main_idea",
            },
            "slot_schema": {
                "option_confusion": {
                    "type": "string",
                    "required": False,
                    "allowed": ["low", "medium", "high"],
                }
            },
            "default_slots": {},
            "patterns": [
                {
                    "pattern_id": "pattern.alpha",
                    "pattern_name": "Alpha",
                    "enabled": True,
                    "match_rules": {},
                    "control_logic": {
                        "difficulty_source": "card",
                        "option_confusion": "configured",
                        "control_levers": {
                            "passage": "from_config",
                            "correct_option": "from_config",
                            "wrong_options": "from_config",
                        },
                    },
                    "generation_logic": {
                        "generation_core": "core.alpha",
                        "processing_type": "literal",
                        "correct_logic": "logic.alpha",
                        "high_freq_traps": [],
                        "distractor_pattern": "pattern.alpha",
                        "analysis_steps": "steps.alpha",
                    },
                    "difficulty_rules": {
                        "complexity": {"base": 0.3},
                        "ambiguity": {"base": 0.35},
                        "reasoning_depth": {"base": 0.4},
                        "distractor_similarity": {"base": 0.45},
                    },
                },
                {
                    "pattern_id": "pattern.beta",
                    "pattern_name": "Beta",
                    "enabled": True,
                    "match_rules": {},
                    "control_logic": {
                        "difficulty_source": "card",
                        "option_confusion": "configured",
                        "control_levers": {
                            "passage": "from_config",
                            "correct_option": "from_config",
                            "wrong_options": "from_config",
                        },
                    },
                    "generation_logic": {
                        "generation_core": "core.beta",
                        "processing_type": "literal",
                        "correct_logic": "logic.beta",
                        "high_freq_traps": [],
                        "distractor_pattern": "pattern.beta",
                        "analysis_steps": "steps.beta",
                    },
                    "difficulty_rules": {
                        "complexity": {"base": 0.3},
                        "ambiguity": {"base": 0.35},
                        "reasoning_depth": {"base": 0.4},
                        "distractor_similarity": {"base": 0.45},
                    },
                },
            ],
            "default_pattern_id": default_pattern_id,
            "difficulty_target_profiles": {
                "easy": {
                    "complexity": {"min": 0.0, "max": 0.6},
                    "ambiguity": {"min": 0.0, "max": 0.6},
                    "reasoning_depth": {"min": 0.0, "max": 0.6},
                    "distractor_similarity": {"min": 0.0, "max": 0.6},
                },
                "medium": {
                    "complexity": {"min": 0.2, "max": 0.8},
                    "ambiguity": {"min": 0.2, "max": 0.8},
                    "reasoning_depth": {"min": 0.2, "max": 0.8},
                    "distractor_similarity": {"min": 0.2, "max": 0.8},
                },
                "hard": {
                    "complexity": {"min": 0.4, "max": 1.0},
                    "ambiguity": {"min": 0.4, "max": 1.0},
                    "reasoning_depth": {"min": 0.4, "max": 1.0},
                    "distractor_similarity": {"min": 0.4, "max": 1.0},
                },
            },
        }
    )


class SlotResolverUnitTest(TestCase):
    def setUp(self) -> None:
        self.service = SlotResolverService()

    def test_resolve_rejects_ambiguous_pattern_without_explicit_or_config_default(self) -> None:
        config = _build_type_config(default_pattern_id=None)

        with self.assertRaises(DomainError):
            self.service.resolve(
                config,
                difficulty_target="medium",
                type_slots={},
            )

    def test_resolve_uses_configured_default_pattern_without_first_enabled_fallback(self) -> None:
        config = _build_type_config(default_pattern_id="pattern.beta")

        result = self.service.resolve(
            config,
            difficulty_target="medium",
            type_slots={},
        )

        self.assertEqual(result["selected_pattern"], "pattern.beta")
        self.assertEqual(result["pattern_selection_reason"].selection_mode, "configured_default")
        self.assertTrue(result["pattern_selection_reason"].fallback_used)

    def test_resolve_does_not_inject_difficulty_based_slot_values(self) -> None:
        config = _build_type_config(default_pattern_id="pattern.alpha")

        result = self.service.resolve(
            config,
            difficulty_target="hard",
            type_slots={},
        )

        self.assertNotIn("option_confusion", result["resolved_slots"])

    def test_difficulty_projection_does_not_add_target_bias(self) -> None:
        config = _build_type_config(default_pattern_id="pattern.alpha")

        easy = self.service.resolve(
            config,
            difficulty_target="easy",
            type_slots={},
        )
        hard = self.service.resolve(
            config,
            difficulty_target="hard",
            type_slots={},
        )

        self.assertEqual(easy["difficulty_projection"].model_dump(), hard["difficulty_projection"].model_dump())
