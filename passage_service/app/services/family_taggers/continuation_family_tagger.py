from app.schemas.span import SpanRecord
from app.schemas.subtype_route import SubtypeCandidate
from app.schemas.universal_profile import UniversalProfile
from app.services.family_taggers.base import BaseFamilyTagger


class ContinuationFamilyTagger(BaseFamilyTagger):
    family_name = "灏炬缁啓鍨?"

    def __init__(self) -> None:
        super().__init__("continuation_family_prompt.md")

    def score(self, span: SpanRecord, universal_profile: UniversalProfile) -> tuple[list[SubtypeCandidate], dict]:
        llm_result = self.score_with_llm(
            model=self.llm_config.get("models", {}).get("family_tagger", "gpt-4.1-mini"),
            span=span,
            universal_profile=universal_profile,
            subtype_names=["鏂拌惤鐐瑰欢灞曞崱", "闂鏆撮湶杞绛栧崱", "鏈哄埗褰卞搷灞曞紑鍗?", "姒傚康鎻愬崌寮曢鍗?", "鎬荤粨鍚庣暀鐧藉崱", "鍒ゆ柇钀界偣琛ヨ璇佸崱", "涓杞ぇ鑳屾櫙鍗?", "骞跺垪鎷╀竴灞曞紑鍗?", "寮犲姏鍐茬獊鎵挎帴鍗?", "鏂规硶璺緞缁嗗寲鍗?"],
        )
        if llm_result is not None:
            candidates, notes = llm_result
            return sorted(candidates, key=lambda item: item.score, reverse=True)[:3], notes

        text = span.text.strip()
        tail = text[-80:]
        has_complete_tail = text.endswith(("\u3002", "\uff01", "\uff1f", "!", "?"))
        has_tail_extension_signal = any(
            token in tail
            for token in (
                "\u8fd9\u4e5f\u610f\u5473\u7740",
                "\u8fd9\u8981\u6c42",
                "\u8fd9\u63d0\u9192\u6211\u4eec",
                "\u5173\u952e\u5728\u4e8e",
                "\u8fd8\u9700\u8981",
                "\u8fdb\u4e00\u6b65",
                "\u624d\u80fd",
            )
        )

        candidates: list[SubtypeCandidate] = []
        if universal_profile.continuation_openness >= 0.72 and universal_profile.direction_uniqueness >= 0.58 and has_complete_tail and has_tail_extension_signal:
            candidates.append(SubtypeCandidate(family=self.family_name, subtype="鏂拌惤鐐瑰欢灞曞崱", score=0.80))
        if universal_profile.problem_signal_strength >= 0.7 and universal_profile.method_signal_strength >= 0.5 and has_complete_tail:
            candidates.append(SubtypeCandidate(family=self.family_name, subtype="闂鏆撮湶杞绛栧崱", score=0.82))
        if universal_profile.method_signal_strength >= 0.7 and has_complete_tail and has_tail_extension_signal:
            candidates.append(SubtypeCandidate(family=self.family_name, subtype="鏂规硶璺緞缁嗗寲鍗?", score=0.79))
        if universal_profile.direction_uniqueness >= 0.7 and has_complete_tail and has_tail_extension_signal:
            candidates.append(SubtypeCandidate(family=self.family_name, subtype="鍒ゆ柇钀界偣琛ヨ璇佸崱", score=0.76))
        if universal_profile.value_judgement_strength >= 0.7 and universal_profile.summary_strength >= 0.45 and has_complete_tail and has_tail_extension_signal:
            candidates.append(SubtypeCandidate(family=self.family_name, subtype="鎬荤粨鍚庣暀鐧藉崱", score=0.74))
        return sorted(candidates, key=lambda item: item.score, reverse=True)[:3], {
            "family": self.family_name,
            "has_complete_tail": has_complete_tail,
            "has_tail_extension_signal": has_tail_extension_signal,
        }
