from __future__ import annotations

from pathlib import Path
import sys
from unittest import TestCase


ROOT = Path(__file__).resolve().parents[2]
APP_ROOT = ROOT / "prompt_skeleton_service"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from app.schemas.item import GeneratedQuestion
from app.services.question_validator import QuestionValidatorService


class SentenceOrderSchemaTest(TestCase):
    def test_type_config_fixed_sortable_unit_count_is_six(self) -> None:
        content = (ROOT / "prompt_skeleton_service" / "configs" / "types" / "sentence_order.yaml").read_text(
            encoding="utf-8"
        )
        self.assertIn("sortable_unit_count:\n    mode: fixed\n    value: 6", content)
        self.assertIn("sortable_unit_sentence_span:\n    min: 1\n    max: 2", content)
        self.assertIn("candidate_type: sentence_block_group", content)
        self.assertIn("reject_non_six_unit_sequences: true", content)
        self.assertIn("allowed:\n      - 6", content)
        self.assertIn("distractor_strength: medium", content)
        self.assertNotIn("phrase_order_variant", content)

    def test_normalized_question_card_carries_six_unit_contract(self) -> None:
        content = (
            ROOT / "card_specs" / "normalized" / "question_cards" / "sentence_order_standard_question_card.normalized.yaml"
        ).read_text(encoding="utf-8")
        self.assertIn("primary_business_card_id: sentence_order__six_sentence_role_chain__abstract", content)
        self.assertIn("sortable_unit_count:\n    mode: fixed\n    value: 6", content)
        self.assertIn("sortable_unit_sentence_span:\n    min: 1\n    max: 2", content)
        self.assertIn("candidate_type: sentence_block_group", content)
        self.assertIn("fixed_sortable_unit_count: 6", content)
        self.assertIn("allowed_sortable_unit_counts:\n  - 6", content)
        self.assertIn("require_binding_pairs_intact: true", content)
        self.assertIn("require_complete_ordering_chain: true", content)
        self.assertIn("distractor_strength: medium", content)
        self.assertNotIn("phrase_order_variant", content)

    def test_standard_material_and_signal_specs_are_block_group_only(self) -> None:
        material_content = (
            ROOT / "card_specs" / "normalized" / "material_cards" / "sentence_order_intermediate_material_cards.normalized.yaml"
        ).read_text(encoding="utf-8")
        signal_content = (
            ROOT / "card_specs" / "normalized" / "signal_layers" / "sentence_order_signal_layer.normalized.yaml"
        ).read_text(encoding="utf-8")
        self.assertNotIn("order_material.phrase_order_variant", material_content)
        self.assertNotIn("phrase_or_clause_group", material_content)
        self.assertNotIn("phrase_or_clause_group", signal_content)
        self.assertNotIn("phrase_order_variant", signal_content)
        self.assertNotIn("phrase_order_salience", signal_content)

    def test_primary_role_chain_card_exists(self) -> None:
        content = (
            ROOT / "card_specs" / "business_feature_slots" / "examples" / "sentence_order_six_sentence_role_chain.abstract.yaml"
        ).read_text(encoding="utf-8")
        self.assertIn("business_card_id: sentence_order__six_sentence_role_chain__abstract", content)
        self.assertIn("card_role: primary_card", content)
        self.assertIn("sortable_unit_count: 6", content)
        self.assertIn("binding_violation", content)

    def test_timeline_card_is_weak_legacy(self) -> None:
        content = (
            ROOT / "card_specs" / "business_feature_slots" / "examples" / "sentence_order_timeline_action_sequence.abstract.yaml"
        ).read_text(encoding="utf-8")
        self.assertIn("card_role: weak_legacy", content)
        self.assertIn("schema_status: weak_legacy", content)


class SentenceOrderConstraintEnforcementTest(TestCase):
    def setUp(self) -> None:
        self.validator = QuestionValidatorService()
        self.contract = {
            "sentence_order": {
                "sortable_unit_count": 6,
                "sortable_unit_sentence_span": {"min": 1, "max": 2},
                "require_legal_head": True,
                "require_legal_tail": True,
                "require_binding_pairs_intact": True,
                "require_complete_ordering_chain": True,
            }
        }
        self.base_sentences = [
            "首先要摸清社区旧设施的分布。",
            "只有掌握底数，后续投入才不会失焦。",
            "在此基础上，还要确定改造重点。",
            "例如，应优先处理积水和照明问题。",
            "随后再补齐配套条件，保证方案可执行。",
            "因此，分步实施比一次铺开更稳妥。",
        ]
        self.base_material = " ".join(self.base_sentences)
        self.base_options = {
            "A": "1-2-3-4-5-6",
            "B": "1-3-2-4-5-6",
            "C": "4-1-2-3-5-6",
            "D": "1-2-6-3-4-5",
        }

    def _analysis_for(self, order: list[int]) -> str:
        circled = {1: "①", 2: "②", 3: "③", 4: "④", 5: "⑤", 6: "⑥", 7: "⑦"}
        return "先判断首句和尾句，正确顺序为" + "".join(circled[index] for index in order) + "，故正确答案为A。"

    def _validate(
        self,
        *,
        order: list[int],
        original_sentences: list[str] | None = None,
        options: dict[str, str] | None = None,
        material_text: str | None = None,
        control_logic: dict[str, object] | None = None,
    ):
        original = list(original_sentences or self.base_sentences)
        question = GeneratedQuestion(
            question_type="sentence_order",
            stem="将以下6个部分重新排列，语序正确的一项是：",
            original_sentences=original,
            correct_order=order,
            options=options or self.base_options,
            answer="A",
            analysis=self._analysis_for(order),
        )
        return self.validator.validate(
            question_type="sentence_order",
            generated_question=question,
            material_text=material_text or self.base_material,
            validator_contract=self.contract,
            control_logic=control_logic,
        )

    def test_transition_or_reference_head_fails_with_illegal_head(self) -> None:
        result = self._validate(order=[2, 1, 3, 4, 5, 6])
        self.assertFalse(result.passed)
        self.assertIn("ordering_chain_incomplete", result.errors)

        result = self._validate(order=[4, 1, 2, 3, 5, 6])
        self.assertFalse(result.passed)
        self.assertIn("ordering_chain_incomplete", result.errors)

    def test_example_tail_fails_with_illegal_tail(self) -> None:
        result = self._validate(order=[1, 2, 3, 4, 6, 5])
        self.assertFalse(result.passed)
        self.assertIn("ordering_chain_incomplete", result.errors)

    def test_binding_swap_fails_with_binding_violation(self) -> None:
        result = self._validate(
            order=[1, 3, 2, 4, 5, 6],
            control_logic={"binding_pairs": [(2, 3)]},
        )
        self.assertFalse(result.passed)
        self.assertIn("binding_violation", result.errors)
        self.assertEqual(
            result.checks["sentence_order_binding_enforcement"]["violations"],
            [{"before": 2, "after": 3}],
        )

    def test_sortable_unit_count_mismatch_for_five_and_seven_units(self) -> None:
        result = self._validate(
            order=[1, 2, 3, 4, 5],
            original_sentences=self.base_sentences[:5],
            options={
                "A": "1-2-3-4-5",
                "B": "1-3-2-4-5",
                "C": "5-4-3-2-1",
                "D": "2-1-3-4-5",
            },
            material_text=" ".join(self.base_sentences[:5]),
        )
        self.assertFalse(result.passed)
        self.assertIn("sentence_count_mismatch", result.errors)

        seven_sentences = self.base_sentences + ["最后，还要持续复盘实施效果。"]
        result = self._validate(
            order=[1, 2, 3, 4, 5, 6, 7],
            original_sentences=seven_sentences,
            options={
                "A": "1-2-3-4-5-6-7",
                "B": "1-3-2-4-5-6-7",
                "C": "7-6-5-4-3-2-1",
                "D": "2-1-3-4-5-6-7",
            },
            material_text=" ".join(seven_sentences),
        )
        self.assertFalse(result.passed)
        self.assertIn("sentence_count_mismatch", result.errors)

    def test_six_units_allow_one_or_two_sentences_per_unit(self) -> None:
        unit_material = (
            "① 首先交代讨论背景。\n"
            "② 接着说明现实限制。因此需要重新安排资源。\n"
            "③ 随后提出推进思路。\n"
            "④ 同时补充执行条件，让方案更具可行性。然后归纳阶段重点。\n"
            "⑤ 最后说明影响范围。\n"
            "⑥ 给出总结判断。"
        )
        units = [
            "首先交代讨论背景。",
            "接着说明现实限制。因此需要重新安排资源。",
            "随后提出推进思路。",
            "同时补充执行条件，让方案更具可行性。然后归纳阶段重点。",
            "最后说明影响范围。",
            "给出总结判断。",
        ]
        result = self._validate(
            order=[1, 2, 3, 4, 5, 6],
            original_sentences=units,
            material_text=unit_material,
            options={
                "A": "1-2-3-4-5-6",
                "B": "1-3-2-4-5-6",
                "C": "2-1-3-4-5-6",
                "D": "1-2-4-3-5-6",
            },
        )
        self.assertTrue(result.checks["sentence_order_original_sentences"]["passed"])
        self.assertTrue(result.checks["sentence_order_unit_sentence_span"]["passed"])
        self.assertEqual(result.checks["sentence_order_unit_sentence_span"]["counts"], [1, 2, 1, 2, 1, 1])
        self.assertNotIn("sentence_count_mismatch", result.errors)

    def test_legacy_six_single_sentence_units_still_pass_unit_span_contract(self) -> None:
        result = self._validate(order=[1, 2, 3, 4, 5, 6])
        self.assertTrue(result.checks["sentence_order_original_sentences"]["passed"])
        self.assertTrue(result.checks["sentence_order_unit_sentence_span"]["passed"])
        self.assertEqual(result.checks["sentence_order_unit_sentence_span"]["counts"], [1, 1, 1, 1, 1, 1])

    def test_role_order_conflict_for_conclusion_in_middle(self) -> None:
        result = self._validate(order=[1, 2, 6, 3, 4, 5])
        self.assertFalse(result.passed)
        self.assertIn("role_order_conflict", result.errors)
