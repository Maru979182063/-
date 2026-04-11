from app.schemas.span import SpanRecord
from app.schemas.subtype_route import SubtypeCandidate
from app.schemas.universal_profile import UniversalProfile
from app.services.family_taggers.base import BaseFamilyTagger


class ContinuationFamilyTagger(BaseFamilyTagger):
    family_name = "鐏忕偓顔岀紒顓炲晸閸?"

    def __init__(self) -> None:
        super().__init__("continuation_family_prompt.md")

    def score(self, span: SpanRecord, universal_profile: UniversalProfile) -> tuple[list[SubtypeCandidate], dict]:
        subtype_names = [
            "閺傛媽鎯ら悙鐟版鐏炴洖宕?",
            "闂傤噣顣介弳鎾苟鏉烆剙顕粵鏍у幢",
            "閺堝搫鍩楄ぐ鍗炴惙鐏炴洖绱戦崡?",
            "濮掑倸搴烽幓鎰磳瀵洟顣介崡?",
            "閹崵绮ㄩ崥搴ｆ殌閻ц棄宕?",
            "閸掋倖鏌囬拃鐣屽仯鐞涖儴顔戠拠浣稿幢",
            "娑擃亝顢嶆潪顒€銇囬懗灞炬珯閸?",
            "楠炶泛鍨幏鈺€绔寸仦鏇炵磻閸?",
            "瀵姴濮忛崘鑼崐閹垫寧甯撮崡?",
            "閺傝纭剁捄顖氱窞缂佸棗瀵查崡?",
        ]
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
            candidates.append(SubtypeCandidate(family=self.family_name, subtype="閺傛媽鎯ら悙鐟版鐏炴洖宕?", score=0.80))
        if universal_profile.problem_signal_strength >= 0.7 and universal_profile.method_signal_strength >= 0.5 and has_complete_tail:
            candidates.append(SubtypeCandidate(family=self.family_name, subtype="闂傤噣顣介弳鎾苟鏉烆剙顕粵鏍у幢", score=0.82))
        if universal_profile.method_signal_strength >= 0.7 and has_complete_tail and has_tail_extension_signal:
            candidates.append(SubtypeCandidate(family=self.family_name, subtype="閺傝纭剁捄顖氱窞缂佸棗瀵查崡?", score=0.79))
        if universal_profile.direction_uniqueness >= 0.7 and has_complete_tail and has_tail_extension_signal:
            candidates.append(SubtypeCandidate(family=self.family_name, subtype="閸掋倖鏌囬拃鐣屽仯鐞涖儴顔戠拠浣稿幢", score=0.76))
        if universal_profile.value_judgement_strength >= 0.7 and universal_profile.summary_strength >= 0.45 and has_complete_tail and has_tail_extension_signal:
            candidates.append(SubtypeCandidate(family=self.family_name, subtype="閹崵绮ㄩ崥搴ｆ殌閻ц棄宕?", score=0.74))
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
            "has_complete_tail": has_complete_tail,
            "has_tail_extension_signal": has_tail_extension_signal,
        }
