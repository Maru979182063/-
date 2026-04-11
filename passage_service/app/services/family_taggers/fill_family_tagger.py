from app.schemas.span import SpanRecord
from app.schemas.subtype_route import SubtypeCandidate
from app.schemas.universal_profile import UniversalProfile
from app.services.family_taggers.base import BaseFamilyTagger


class FillFamilyTagger(BaseFamilyTagger):
    family_name = "琛旀帴琛ヤ綅鍨?"

    def __init__(self) -> None:
        super().__init__("fill_family_prompt.md")

    def score(self, span: SpanRecord, universal_profile: UniversalProfile) -> tuple[list[SubtypeCandidate], dict]:
        subtype_names = [
            "娈甸鎬婚鍗?",
            "鎵夸笂鍚笅妗ユ帴鍗?",
            "涓棿鎵挎帴琛ョ幆鍗?",
            "涓棿瑙ｉ噴璇存槑鍗?",
            "瀹氫綅鎻掑叆绾︽潫鍗?",
            "灏惧彞鎬荤粨鏀舵潫鍗?",
            "灏惧彞鍗囧崕鎷旈珮鍗?",
            "缁煎悎澶氱害鏉熷崱",
        ]
        candidates: list[SubtypeCandidate] = []
        if "寮€澶存€婚" in universal_profile.position_roles:
            candidates.append(SubtypeCandidate(family=self.family_name, subtype="娈甸鎬婚鍗?", score=0.80))
        if universal_profile.transition_strength >= 0.65:
            candidates.append(SubtypeCandidate(family=self.family_name, subtype="鎵夸笂鍚笅妗ยู帴鍗?", score=0.78))
        if "涓棿瑙ｉ噴浣?" in universal_profile.position_roles:
            candidates.append(SubtypeCandidate(family=self.family_name, subtype="涓棿瑙ｉ噴璇存槑鍗?", score=0.77))
        if universal_profile.explanation_strength >= 0.7:
            candidates.append(SubtypeCandidate(family=self.family_name, subtype="缁煎悎澶氱害鏉熷崱", score=0.72))
        if "灏炬鎬荤粨" in universal_profile.position_roles:
            candidates.append(SubtypeCandidate(family=self.family_name, subtype="灏惧彞鎬荤粨鏀舵潫鍗?", score=0.79))
        if "灏炬鍗囧崕" in universal_profile.position_roles or universal_profile.value_judgement_strength >= 0.7:
            candidates.append(SubtypeCandidate(family=self.family_name, subtype="灏惧彞鍗囧崕鎷旈珮鍗?", score=0.74))
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
