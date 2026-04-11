from app.schemas.span import SpanRecord
from app.schemas.subtype_route import SubtypeCandidate
from app.schemas.universal_profile import UniversalProfile
from app.services.family_taggers.base import BaseFamilyTagger


class SummarizationFamilyTagger(BaseFamilyTagger):
    family_name = "姒傛嫭褰掔撼鍨?"

    def __init__(self) -> None:
        super().__init__("summarization_family_prompt.md")

    def score(self, span: SpanRecord, universal_profile: UniversalProfile) -> tuple[list[SubtypeCandidate], dict]:
        subtype_names = [
            "鍏ㄦ枃鏁村悎鍗曚腑蹇冨崱",
            "缁撳熬鍥炴敹鍗?",
            "寮€澶存€婚鍗?",
            "杞姌鍚庨噸蹇冨崱",
            "渚嬭瘉褰掔撼鍗?",
            "澶氱淮缁熸憚鍗?",
            "闂鍒嗘瀽鏀舵潫鍗?",
            "鎬佸害/浠峰€艰惤鐐瑰崱",
        ]
        candidates: list[SubtypeCandidate] = []
        if universal_profile.single_center_strength >= 0.75:
            candidates.append(SubtypeCandidate(family=self.family_name, subtype="鍏ㄦ枃鏁村悎鍗曚腑蹇冨崱", score=0.82))
        if "灏炬鎬荤粨" in universal_profile.position_roles:
            candidates.append(SubtypeCandidate(family=self.family_name, subtype="缁撳熬鍥炴敹鍗?", score=0.78))
        if "寮€澶存€婚" in universal_profile.position_roles:
            candidates.append(SubtypeCandidate(family=self.family_name, subtype="寮€澶存€婚鍗?", score=0.74))
        if "杞姌鍚庣粨璁?" in universal_profile.position_roles:
            candidates.append(SubtypeCandidate(family=self.family_name, subtype="杞姌鍚庨噸蹇冨崱", score=0.79))
        if universal_profile.example_to_theme_strength >= 0.7:
            candidates.append(SubtypeCandidate(family=self.family_name, subtype="渚嬭瘉褰掔撼鍗?", score=0.77))
        if universal_profile.branch_focus_strength >= 0.7:
            candidates.append(SubtypeCandidate(family=self.family_name, subtype="澶氱淮缁熸憚鍗?", score=0.73))
        if universal_profile.problem_signal_strength >= 0.7:
            candidates.append(SubtypeCandidate(family=self.family_name, subtype="闂鍒嗘瀽鏀舵潫鍗?", score=0.72))
        if universal_profile.value_judgement_strength >= 0.7:
            candidates.append(SubtypeCandidate(family=self.family_name, subtype="鎬佸害/浠峰€艰惤鐐瑰崱", score=0.76))
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
