from app.schemas.span import SpanRecord
from app.schemas.subtype_route import SubtypeCandidate
from app.schemas.universal_profile import UniversalProfile
from app.services.family_taggers.base import BaseFamilyTagger


class SummarizationFamilyTagger(BaseFamilyTagger):
    family_name = "概括归纳型"

    def __init__(self) -> None:
        super().__init__("summarization_family_prompt.md")

    def score(self, span: SpanRecord, universal_profile: UniversalProfile) -> tuple[list[SubtypeCandidate], dict]:
        llm_result = self.score_with_llm(
            model=self.llm_config.get("models", {}).get("family_tagger", "gpt-4.1-mini"),
            span=span,
            universal_profile=universal_profile,
            subtype_names=["全文整合单中心卡", "结尾回收卡", "开头总领卡", "转折后重心卡", "例证归纳卡", "多维统摄卡", "问题分析收束卡", "态度/价值落点卡"],
        )
        if llm_result is not None:
            candidates, notes = llm_result
            return sorted(candidates, key=lambda item: item.score, reverse=True)[:3], notes

        candidates: list[SubtypeCandidate] = []
        if universal_profile.single_center_strength >= 0.75:
            candidates.append(SubtypeCandidate(family=self.family_name, subtype="全文整合单中心卡", score=0.82))
        if "尾段总结" in universal_profile.position_roles:
            candidates.append(SubtypeCandidate(family=self.family_name, subtype="结尾回收卡", score=0.78))
        if "开头总领" in universal_profile.position_roles:
            candidates.append(SubtypeCandidate(family=self.family_name, subtype="开头总领卡", score=0.74))
        if "转折后结论" in universal_profile.position_roles:
            candidates.append(SubtypeCandidate(family=self.family_name, subtype="转折后重心卡", score=0.79))
        if universal_profile.example_to_theme_strength >= 0.7:
            candidates.append(SubtypeCandidate(family=self.family_name, subtype="例证归纳卡", score=0.77))
        if universal_profile.branch_focus_strength >= 0.7:
            candidates.append(SubtypeCandidate(family=self.family_name, subtype="多维统摄卡", score=0.73))
        if universal_profile.problem_signal_strength >= 0.7:
            candidates.append(SubtypeCandidate(family=self.family_name, subtype="问题分析收束卡", score=0.72))
        if universal_profile.value_judgement_strength >= 0.7:
            candidates.append(SubtypeCandidate(family=self.family_name, subtype="态度/价值落点卡", score=0.76))
        return sorted(candidates, key=lambda item: item.score, reverse=True)[:3], {"family": self.family_name}
