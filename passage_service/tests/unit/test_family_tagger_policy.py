import unittest
from unittest.mock import patch

from app.schemas.span import SpanRecord, SpanVersionSet
from app.schemas.subtype_route import SubtypeCandidate
from app.schemas.universal_profile import TextShape, UniversalProfile
from app.services.family_taggers.title_family_tagger import TitleFamilyTagger


class _FakeProvider:
    def is_enabled(self) -> bool:
        return True


def _span() -> SpanRecord:
    return SpanRecord(
        span_id="span-1",
        article_id="article-1",
        text="这是一段围绕单一主题展开的完整材料，能够独立阅读，并在结尾完成收束。",
        paragraph_count=2,
        sentence_count=4,
        source_domain="example.com",
        version=SpanVersionSet(
            segment_version="seg.v1",
            universal_tag_version="u.v1",
            route_version="r.v1",
            family_tag_version="f.v1",
        ),
    )


def _profile(**overrides) -> UniversalProfile:
    base = UniversalProfile(
        text_shape=TextShape(length_bucket="medium", paragraph_count=2, sentence_count=4),
        material_structure_label="综合说明",
        material_structure_reason="测试",
        logic_relations=["总结提升"],
        position_roles=["尾段总结"],
        standalone_readability=0.82,
        single_center_strength=0.8,
        summary_strength=0.72,
        transition_strength=0.25,
        explanation_strength=0.35,
        ordering_anchor_strength=0.1,
        continuation_openness=0.1,
        direction_uniqueness=0.25,
        titleability=0.82,
        value_judgement_strength=0.76,
        example_to_theme_strength=0.2,
        problem_signal_strength=0.2,
        method_signal_strength=0.1,
        branch_focus_strength=0.2,
        independence_score=0.82,
    )
    return base.model_copy(update=overrides)


class FamilyTaggerPolicyTests(unittest.TestCase):
    def test_family_tagger_skips_llm_when_heuristic_is_confident(self) -> None:
        tagger = TitleFamilyTagger()
        tagger.provider = _FakeProvider()
        tagger.llm_config = {"enabled": True, "models": {"family_tagger": "fake-model"}}
        tagger.set_runtime_context(
            {
                "family": tagger.family_name,
                "family_rank": 0,
                "family_score": 0.84,
                "primary_family": tagger.family_name,
                "primary_score": 0.84,
                "score_gap_from_primary": 0.0,
                "primary_second_gap": 0.18,
            }
        )

        with patch.object(
            tagger,
            "score_with_llm",
            return_value=([SubtypeCandidate(family=tagger.family_name, subtype="llm-card", score=0.99)], {"family": tagger.family_name}),
        ) as mocked:
            candidates, notes = tagger.score(_span(), _profile(value_judgement_strength=0.2, example_to_theme_strength=0.1))

        self.assertFalse(mocked.called)
        self.assertTrue(candidates)
        self.assertFalse(notes["llm_used"])
        self.assertEqual(notes["llm_gate_reason"], "heuristic_path")

    def test_family_tagger_uses_llm_for_primary_boundary_case(self) -> None:
        tagger = TitleFamilyTagger()
        tagger.provider = _FakeProvider()
        tagger.llm_config = {"enabled": True, "models": {"family_tagger": "fake-model"}}
        tagger.set_runtime_context(
            {
                "family": tagger.family_name,
                "family_rank": 0,
                "family_score": 0.71,
                "primary_family": tagger.family_name,
                "primary_score": 0.71,
                "score_gap_from_primary": 0.0,
                "primary_second_gap": 0.03,
            }
        )
        weak_profile = _profile(
            titleability=0.35,
            value_judgement_strength=0.25,
            single_center_strength=0.42,
            summary_strength=0.28,
        )
        with patch.object(
            tagger,
            "score_with_llm",
            return_value=([SubtypeCandidate(family=tagger.family_name, subtype="llm-card", score=0.86)], {"family": tagger.family_name}),
        ) as mocked:
            candidates, notes = tagger.score(_span(), weak_profile)

        self.assertTrue(mocked.called)
        self.assertEqual(candidates[0].subtype, "llm-card")
        self.assertTrue(notes["llm_used"])
        self.assertEqual(notes["llm_gate_reason"], "no_heuristic_subtype")

    def test_family_tagger_compacts_long_text_before_llm(self) -> None:
        tagger = TitleFamilyTagger()
        long_text = "甲" * 900

        compact = tagger._compact_text(long_text)

        self.assertLess(len(compact), len(long_text))
        self.assertIn("[snip", compact)


if __name__ == "__main__":
    unittest.main()
