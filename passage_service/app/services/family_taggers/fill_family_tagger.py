from app.schemas.span import SpanRecord
from app.schemas.subtype_route import SubtypeCandidate
from app.schemas.universal_profile import UniversalProfile
from app.services.family_taggers.base import BaseFamilyTagger


class FillFamilyTagger(BaseFamilyTagger):
    family_name = "衔接补位型"

    def __init__(self) -> None:
        super().__init__("fill_family_prompt.md")

    def score(self, span: SpanRecord, universal_profile: UniversalProfile) -> tuple[list[SubtypeCandidate], dict]:
        llm_result = self.score_with_llm(
            model=self.llm_config.get("models", {}).get("family_tagger", "gpt-4.1-mini"),
            span=span,
            universal_profile=universal_profile,
            subtype_names=["段首总领卡", "承上启下桥接卡", "中间承接补环卡", "中间解释说明卡", "定位插入约束卡", "尾句总结收束卡", "尾句升华拔高卡", "综合多约束卡"],
        )
        if llm_result is not None:
            candidates, notes = llm_result
            return sorted(candidates, key=lambda item: item.score, reverse=True)[:3], notes

        candidates: list[SubtypeCandidate] = []
        if "开头总领" in universal_profile.position_roles:
            candidates.append(SubtypeCandidate(family=self.family_name, subtype="段首总领卡", score=0.80))
        if universal_profile.transition_strength >= 0.65:
            candidates.append(SubtypeCandidate(family=self.family_name, subtype="承上启下桥接卡", score=0.78))
        if "中间解释位" in universal_profile.position_roles:
            candidates.append(SubtypeCandidate(family=self.family_name, subtype="中间解释说明卡", score=0.77))
        if universal_profile.explanation_strength >= 0.7:
            candidates.append(SubtypeCandidate(family=self.family_name, subtype="综合多约束卡", score=0.72))
        if "尾段总结" in universal_profile.position_roles:
            candidates.append(SubtypeCandidate(family=self.family_name, subtype="尾句总结收束卡", score=0.79))
        if "尾段升华" in universal_profile.position_roles or universal_profile.value_judgement_strength >= 0.7:
            candidates.append(SubtypeCandidate(family=self.family_name, subtype="尾句升华拔高卡", score=0.74))
        return sorted(candidates, key=lambda item: item.score, reverse=True)[:3], {"family": self.family_name}
