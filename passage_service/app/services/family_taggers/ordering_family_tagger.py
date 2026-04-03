from app.schemas.span import SpanRecord
from app.schemas.subtype_route import SubtypeCandidate
from app.schemas.universal_profile import UniversalProfile
from app.services.family_taggers.base import BaseFamilyTagger


class OrderingFamilyTagger(BaseFamilyTagger):
    family_name = "顺序重建型"

    def __init__(self) -> None:
        super().__init__("ordering_family_prompt.md")

    def score(self, span: SpanRecord, universal_profile: UniversalProfile) -> tuple[list[SubtypeCandidate], dict]:
        llm_result = self.score_with_llm(
            model=self.llm_config.get("models", {}).get("family_tagger", "gpt-4.1-mini"),
            span=span,
            universal_profile=universal_profile,
            subtype_names=["首尾双锚点卡", "承接并列收束卡", "观点推进号召卡", "问题对策案例卡", "固定搭配局部排序卡"],
        )
        if llm_result is not None:
            candidates, notes = llm_result
            return sorted(candidates, key=lambda item: item.score, reverse=True)[:3], notes

        candidates: list[SubtypeCandidate] = []
        if all(role in universal_profile.position_roles for role in ("排序首句候选", "排序尾句候选")):
            candidates.append(SubtypeCandidate(family=self.family_name, subtype="首尾双锚点卡", score=0.84))
        if any(rel in universal_profile.logic_relations for rel in ("承接", "并列/递进")):
            candidates.append(SubtypeCandidate(family=self.family_name, subtype="承接并列收束卡", score=0.78))
        if any(token in span.text for token in ("一是", "二是", "三是")):
            candidates.append(SubtypeCandidate(family=self.family_name, subtype="问题对策案例卡", score=0.76))
        if universal_profile.branch_focus_strength >= 0.7:
            candidates.append(SubtypeCandidate(family=self.family_name, subtype="固定搭配局部排序卡", score=0.72))
        return sorted(candidates, key=lambda item: item.score, reverse=True)[:3], {"family": self.family_name}
