from app.schemas.span import SpanRecord
from app.schemas.subtype_route import SubtypeCandidate
from app.schemas.universal_profile import UniversalProfile
from app.services.family_taggers.base import BaseFamilyTagger


class OrderingFamilyTagger(BaseFamilyTagger):
    family_name = "椤哄簭閲嶅缓鍨?"

    def __init__(self) -> None:
        super().__init__("ordering_family_prompt.md")

    def score(self, span: SpanRecord, universal_profile: UniversalProfile) -> tuple[list[SubtypeCandidate], dict]:
        subtype_names = [
            "棣栧熬鍙岄敋鐐瑰崱",
            "鎵挎帴骞跺垪鏀舵潫鍗?",
            "瑙傜偣鎺ㄨ繘鍙峰彫鍗?",
            "闂瀵圭瓥妗堜緥鍗?",
            "鍥哄畾鎼厤灞€閮ㄦ帓搴忓崱",
        ]
        candidates: list[SubtypeCandidate] = []
        if all(role in universal_profile.position_roles for role in ("鎺掑簭棣栧彞鍊欓€?", "鎺掑簭灏惧彞鍊欓€?")):
            candidates.append(SubtypeCandidate(family=self.family_name, subtype="棣栧熬鍙岄敋鐐瑰崱", score=0.84))
        if any(rel in universal_profile.logic_relations for rel in ("鎵挎帴", "骞跺垪/閫掕繘")):
            candidates.append(SubtypeCandidate(family=self.family_name, subtype="鎵挎帴骞跺垪鏀舵潫鍗?", score=0.78))
        if any(token in span.text for token in ("涓€鏄?", "浜屾槸", "涓夋槸")):
            candidates.append(SubtypeCandidate(family=self.family_name, subtype="闂瀵圭瓥妗堜緥鍗?", score=0.76))
        if universal_profile.branch_focus_strength >= 0.7:
            candidates.append(SubtypeCandidate(family=self.family_name, subtype="鍥哄畾鎼厤灞€閮ㄦ帓搴忓崱", score=0.72))
        heuristic_candidates = self.sort_candidates(candidates)
        llm_result = self.maybe_score_with_llm(
            model=self.llm_config.get("models", {}).get("family_tagger", "gpt-4.1-mini"),
            span=span,
            universal_profile=universal_profile,
            subtype_names=subtype_names,
            heuristic_candidates=heuristic_candidates,
        )
        if llm_result is not None:
            return llm_result
        return heuristic_candidates, {
            "family": self.family_name,
            "llm_used": False,
            "llm_gate_reason": "heuristic_path",
            "family_runtime_context": dict(self._runtime_context),
        }
