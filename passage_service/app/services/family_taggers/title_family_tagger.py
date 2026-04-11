from app.schemas.span import SpanRecord
from app.schemas.subtype_route import SubtypeCandidate
from app.schemas.universal_profile import UniversalProfile
from app.services.family_taggers.base import BaseFamilyTagger


class TitleFamilyTagger(BaseFamilyTagger):
    family_name = "鏍囬鍛藉悕鍨?"

    def __init__(self) -> None:
        super().__init__("title_family_prompt.md")

    def score(self, span: SpanRecord, universal_profile: UniversalProfile) -> tuple[list[SubtypeCandidate], dict]:
        subtype_names = [
            "骞冲疄涓绘棬鍥炴敹鍗?",
            "渚嬭瘉褰掔撼鍥炴敹鍗?",
            "杞姌鍚庨噸蹇冨崱",
            "澶氱淮缁熸憚鍗?",
            "鍗曞璞¤鏄庡崱",
            "闂鏈川鍒ゆ柇鍗?",
            "鍙嶅父璇嗗弽杞崱",
            "鍙戝睍鑴夌粶鍗?",
            "鐢卞皬瑙佸ぇ鍗囧崕鍗?",
            "浠峰€艰瘎璁崱",
        ]
        candidates: list[SubtypeCandidate] = []
        if universal_profile.titleability >= 0.75:
            candidates.append(SubtypeCandidate(family=self.family_name, subtype="骞冲疄涓绘棬鍥炴敹鍗?", score=0.80))
        if universal_profile.example_to_theme_strength >= 0.7:
            candidates.append(SubtypeCandidate(family=self.family_name, subtype="渚嬭瘉褰掔撼鍥炴敹鍗?", score=0.78))
        if "杞姌鍚庣粨璁?" in universal_profile.position_roles:
            candidates.append(SubtypeCandidate(family=self.family_name, subtype="杞姌鍚庨噸蹇冨崱", score=0.77))
        if universal_profile.branch_focus_strength >= 0.7:
            candidates.append(SubtypeCandidate(family=self.family_name, subtype="澶氱淮缁熸憚鍗?", score=0.74))
        if universal_profile.problem_signal_strength >= 0.7:
            candidates.append(SubtypeCandidate(family=self.family_name, subtype="闂鏈川鍒ゆ柇鍗?", score=0.75))
        if universal_profile.value_judgement_strength >= 0.75:
            candidates.append(SubtypeCandidate(family=self.family_name, subtype="浠峰€艰瘎璁崱", score=0.79))
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
