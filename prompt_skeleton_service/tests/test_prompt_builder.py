from __future__ import annotations

from unittest import TestCase

from app.schemas.config import PatternConfig, QuestionTypeConfig
from app.services.prompt_builder import PromptBuilderService


def _build_pattern() -> PatternConfig:
    return PatternConfig.model_validate(
        {
            "pattern_id": "pattern.alpha",
            "pattern_name": "Alpha",
            "enabled": True,
            "match_rules": {"slot_x": "value_x"},
            "control_logic": {
                "difficulty_source": "card",
                "option_confusion": "configured",
                "control_levers": {
                    "passage": "passage lever",
                    "correct_option": "correct lever",
                    "wrong_options": "wrong lever",
                },
                "special_fields": {"validator_contract": "contract.alpha"},
            },
            "generation_logic": {
                "generation_core": "core.alpha",
                "processing_type": "literal",
                "correct_logic": "logic.alpha",
                "high_freq_traps": ["trap.alpha"],
                "distractor_pattern": "distractor.alpha",
                "analysis_steps": ["step.alpha"],
            },
            "difficulty_rules": {
                "complexity": {"base": 0.3},
                "ambiguity": {"base": 0.35},
                "reasoning_depth": {"base": 0.4},
                "distractor_similarity": {"base": 0.45},
            },
        }
    )


def _build_type_config(pattern: PatternConfig) -> QuestionTypeConfig:
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
            "slot_schema": {},
            "default_slots": {},
            "patterns": [pattern.model_dump()],
            "default_pattern_id": pattern.pattern_id,
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


class PromptBuilderUnitTest(TestCase):
    def setUp(self) -> None:
        self.service = PromptBuilderService()
        self.pattern = _build_pattern()
        self.type_config = _build_type_config(self.pattern)

    def test_summarize_difficulty_target_only_echoes_explicit_target(self) -> None:
        self.assertEqual(self.service._summarize_difficulty_target("hard"), "hard")

    def test_build_prompt_transcribes_configured_logic_without_service_law_text(self) -> None:
        package = self.service.build(
            question_type_config=self.type_config,
            business_subtype_config=None,
            pattern=self.pattern,
            difficulty_target="hard",
            resolved_slots={"slot_x": "value_x"},
            skeleton={
                "anchor_type": "summary",
                "operation_type": "select",
                "target_type": "main_idea",
            },
            control_logic=self.pattern.control_logic.model_dump(),
            generation_logic=self.pattern.generation_logic.model_dump(),
            topic="topic-x",
            count=1,
            passage_style="policy",
            use_fewshot=False,
            fewshot_mode="structure_only",
            extra_constraints={"validator_contract": "contract.alpha"},
        )

        self.assertIn("Configured control logic:", package["system_prompt"])
        self.assertIn("Configured generation logic:", package["system_prompt"])
        self.assertIn("Configured difficulty target: hard", package["system_prompt"])
        self.assertIn("Extra constraints: validator_contract=contract.alpha", package["user_prompt"])
        self.assertNotIn("Apply this confusion level to all wrong options as a whole", package["system_prompt"])
        self.assertNotIn("These override values are mandatory", package["user_prompt"])
        self.assertNotIn("strongest distractor should still read as reasonable", package["user_prompt"])

    def test_build_prompt_filters_reference_inference_constraints_from_prompt(self) -> None:
        package = self.service.build(
            question_type_config=self.type_config,
            business_subtype_config=None,
            pattern=self.pattern,
            difficulty_target="medium",
            resolved_slots={},
            skeleton={
                "anchor_type": "summary",
                "operation_type": "select",
                "target_type": "main_idea",
            },
            control_logic=self.pattern.control_logic.model_dump(),
            generation_logic=self.pattern.generation_logic.model_dump(),
            topic="topic-x",
            count=1,
            passage_style="policy",
            use_fewshot=False,
            fewshot_mode="structure_only",
            extra_constraints={
                "validator_contract": "contract.alpha",
                "source_question_style_summary": {"tone": "formal"},
                "reference_business_cards": ["card-a"],
                "reference_query_terms": ["term-a"],
            },
        )

        self.assertIn("Extra constraints: validator_contract=contract.alpha", package["user_prompt"])
        self.assertNotIn("source_question_style_summary", package["user_prompt"])
        self.assertNotIn("reference_business_cards", package["user_prompt"])
        self.assertNotIn("reference_query_terms", package["user_prompt"])
