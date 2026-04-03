from app.schemas.span import SpanRecord
from app.schemas.subtype_route import SubtypeCandidate
from app.schemas.universal_profile import UniversalProfile
from app.services.family_taggers.base import BaseFamilyTagger


class TitleFamilyTagger(BaseFamilyTagger):
    family_name = "标题命名型"

    def __init__(self) -> None:
        super().__init__("title_family_prompt.md")

    def score(self, span: SpanRecord, universal_profile: UniversalProfile) -> tuple[list[SubtypeCandidate], dict]:
        llm_result = self.score_with_llm(
            model=self.llm_config.get("models", {}).get("family_tagger", "gpt-4.1-mini"),
            span=span,
            universal_profile=universal_profile,
            subtype_names=["平实主旨回收卡", "例证归纳回收卡", "转折后重心卡", "多维统摄卡", "单对象说明卡", "问题本质判断卡", "反常识反转卡", "发展脉络卡", "由小见大升华卡", "价值评议卡"],
        )
        if llm_result is not None:
            candidates, notes = llm_result
            return sorted(candidates, key=lambda item: item.score, reverse=True)[:3], notes

        candidates: list[SubtypeCandidate] = []
        if universal_profile.titleability >= 0.75:
            candidates.append(SubtypeCandidate(family=self.family_name, subtype="平实主旨回收卡", score=0.80))
        if universal_profile.example_to_theme_strength >= 0.7:
            candidates.append(SubtypeCandidate(family=self.family_name, subtype="例证归纳回收卡", score=0.78))
        if "转折后结论" in universal_profile.position_roles:
            candidates.append(SubtypeCandidate(family=self.family_name, subtype="转折后重心卡", score=0.77))
        if universal_profile.branch_focus_strength >= 0.7:
            candidates.append(SubtypeCandidate(family=self.family_name, subtype="多维统摄卡", score=0.74))
        if universal_profile.problem_signal_strength >= 0.7:
            candidates.append(SubtypeCandidate(family=self.family_name, subtype="问题本质判断卡", score=0.75))
        if universal_profile.value_judgement_strength >= 0.75:
            candidates.append(SubtypeCandidate(family=self.family_name, subtype="价值评议卡", score=0.79))
        return sorted(candidates, key=lambda item: item.score, reverse=True)[:3], {"family": self.family_name}
