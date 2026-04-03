from app.rules.family_config import get_family_names, get_thresholds
from app.schemas.family_scores import FamilyScores
from app.schemas.span import SpanRecord
from app.schemas.subtype_route import PrimaryRoute
from app.schemas.universal_profile import UniversalProfile


class FamilyRouter:
    def __init__(self) -> None:
        thresholds = get_thresholds()
        self.family_names = get_family_names()
        self.threshold_low = thresholds["threshold_low"]
        self.threshold_high = thresholds["threshold_high"]
        self.margin = thresholds["margin"]

    def route(self, span: SpanRecord, universal_profile: UniversalProfile) -> dict:
        structure_features = self._build_structure_features(span)
        score_values = [
            self._score_summarization(universal_profile),
            self._score_title(universal_profile),
            self._score_fill(universal_profile, structure_features),
            self._score_ordering(universal_profile, structure_features),
            self._score_continuation(universal_profile, structure_features),
        ]
        scores = {family: round(score_values[index], 4) for index, family in enumerate(self.family_names)}
        capability_scores = {family: score for family, score in scores.items()}
        ranked = sorted(scores.items(), key=lambda item: item[1], reverse=True)
        primary = ranked[0][0] if ranked else None
        parallel_families = [
            {
                "family": family,
                "score": score,
                "reason": self._family_reason(family, structure_features, universal_profile),
            }
            for family, score in ranked
            if score >= max(self.threshold_low - 0.08, 0.35)
        ][:4]
        top_candidates = [item["family"] for item in parallel_families[:2]]
        ordering_family = self._ordering_family()
        if structure_features["strong_ordering_signal"] and ordering_family and ordering_family not in top_candidates:
            top_candidates = (top_candidates + [ordering_family])[:3]

        secondary = [item["family"] for item in parallel_families[1:4]]
        return {
            "family_scores": FamilyScores(
                family_scores=scores,
                primary_family=primary if scores.get(primary or "", 0.0) >= self.threshold_low else None,
                secondary_families=secondary,
            ),
            "capability_scores": capability_scores,
            "parallel_families": parallel_families,
            "structure_features": structure_features,
            "top_candidates": top_candidates,
            "decision": self._decision(scores, ranked, structure_features, ordering_family),
            "primary_route": PrimaryRoute(family=primary),
        }

    def _build_structure_features(self, span: SpanRecord) -> dict:
        text = span.text
        sequence_markers = [
            marker
            for marker in (
                "\u9996\u5148",
                "\u5176\u6b21",
                "\u518d\u6b21",
                "\u6700\u540e",
                "\u603b\u4e4b",
                "\u4e00\u662f",
                "\u4e8c\u662f",
                "\u4e09\u662f",
            )
            if marker in text
        ]
        parallel_markers = [
            marker
            for marker in (
                "\u540c\u65f6",
                "\u53e6\u4e00\u65b9\u9762",
                "\u5e76\u4e14",
                "\u6b64\u5916",
                "\u4e0d\u4ec5",
                "\u800c\u4e14",
            )
            if marker in text
        ]
        tail_text = text.strip()[-120:]
        tail_extension_markers = [
            marker
            for marker in (
                "\u8fd9\u4e5f\u610f\u5473\u7740",
                "\u8fd9\u8981\u6c42",
                "\u8fd9\u63d0\u9192\u6211\u4eec",
                "\u5173\u952e\u5728\u4e8e",
                "\u8fd8\u9700\u8981",
                "\u8fdb\u4e00\u6b65",
                "\u4ece\u800c",
                "\u624d\u80fd",
            )
            if marker in tail_text
        ]
        has_summary_closure = any(marker in text for marker in ("\u603b\u4e4b", "\u53ef\u89c1", "\u56e0\u6b64", "\u7531\u6b64"))
        is_enumeration = len(sequence_markers) >= 2 or any(marker in text for marker in ("\u4e00\u662f", "\u4e8c\u662f", "\u4e09\u662f"))
        order_conditions = 0
        if len(sequence_markers) >= 2:
            order_conditions += 1
        if span.paragraph_count >= 3 and is_enumeration:
            order_conditions += 1
        if span.paragraph_count >= 3 and has_summary_closure:
            order_conditions += 1
        is_order_sensitive = order_conditions >= 2
        strong_ordering_signal = len(sequence_markers) >= 3 and span.paragraph_count >= 3 and has_summary_closure
        return {
            "has_sequence_markers": bool(sequence_markers),
            "sequence_markers": sequence_markers,
            "sequence_marker_count": len(sequence_markers),
            "has_parallel_markers": bool(parallel_markers),
            "parallel_markers": parallel_markers,
            "is_enumeration": is_enumeration,
            "is_order_sensitive": is_order_sensitive,
            "has_summary_closure": has_summary_closure,
            "paragraph_count": span.paragraph_count,
            "sentence_count": span.sentence_count,
            "strong_ordering_signal": strong_ordering_signal,
            "ordering_condition_count": order_conditions,
            "tail_extension_markers": tail_extension_markers,
            "has_tail_extension_signal": bool(tail_extension_markers),
        }

    def _score_summarization(self, p: UniversalProfile) -> float:
        paragraph_bonus = 0.08 if p.text_shape.paragraph_count >= 2 else 0.0
        sentence_bonus = 0.06 if p.text_shape.sentence_count >= 5 else 0.0
        score = 0.32 * p.single_center_strength + 0.26 * p.independence_score + 0.24 * p.summary_strength
        score += 0.08 * p.example_to_theme_strength + 0.07 * p.value_judgement_strength
        score += paragraph_bonus + sentence_bonus
        score -= 0.10 * p.ordering_anchor_strength + 0.12 * p.continuation_openness
        return self._clamp(score)

    def _score_title(self, p: UniversalProfile) -> float:
        score = 0.36 * p.titleability + 0.24 * p.single_center_strength + 0.22 * p.independence_score
        score += 0.08 * p.value_judgement_strength + 0.05 * p.example_to_theme_strength
        score += 0.05 if p.text_shape.paragraph_count >= 2 else 0.0
        score -= 0.10 * p.continuation_openness
        return self._clamp(score)

    def _score_fill(self, p: UniversalProfile, structure_features: dict) -> float:
        score = 0.26 * max(p.transition_strength, p.explanation_strength)
        score += 0.18 * (1 - p.independence_score)
        score += 0.16 * (0.9 if p.position_roles else 0.2)
        score += 0.15 if len(p.position_roles) >= 1 else 0.0
        score += 0.15 if len(p.logic_relations) >= 2 else 0.0
        score += 0.08 if structure_features["has_parallel_markers"] else 0.0
        score -= 0.12 * p.summary_strength
        score -= 0.10 * p.single_center_strength
        if p.text_shape.paragraph_count >= 2 and p.text_shape.sentence_count >= 5:
            score -= 0.08
        return self._clamp(score)

    def _score_ordering(self, p: UniversalProfile, structure_features: dict) -> float:
        score = 0.30 * p.ordering_anchor_strength
        score += 0.12 if len(p.logic_relations) >= 2 else 0.0
        score += 0.12 if len(p.position_roles) >= 2 else 0.0
        score += 0.14 * p.branch_focus_strength
        score -= 0.10 * p.summary_strength
        score -= 0.08 * p.independence_score
        boost = 0.0
        if structure_features["ordering_condition_count"] >= 3:
            boost = 0.55
        elif structure_features["ordering_condition_count"] == 2:
            boost = 0.40
        elif structure_features["ordering_condition_count"] == 1:
            boost = 0.25
        score += boost
        if structure_features["strong_ordering_signal"]:
            score = max(score, self.threshold_low + 0.20)
        return self._clamp(score)

    def _score_continuation(self, p: UniversalProfile, structure_features: dict) -> float:
        score = 0.24 * p.continuation_openness + 0.24 * p.direction_uniqueness
        score += 0.12 * p.problem_signal_strength + 0.12 * p.method_signal_strength
        score += 0.10 * p.branch_focus_strength + 0.08 * p.value_judgement_strength
        score += 0.12 if structure_features["has_summary_closure"] else -0.10
        score += 0.15 if structure_features.get("has_tail_extension_signal") else -0.12
        score -= 0.08 if p.text_shape.sentence_count < 4 else 0.0
        score -= 0.10 if p.independence_score < 0.58 else 0.0
        return self._clamp(score)

    def _decision(self, scores: dict[str, float], ranked: list[tuple[str, float]], structure_features: dict, ordering_family: str | None) -> dict:
        if not ranked or all(score < self.threshold_low for score in scores.values()):
            return {"action": "reject", "release_channel": "gray"}
        if structure_features["strong_ordering_signal"] and ordering_family is not None:
            return {"action": "gray_parallel_with_strong_ordering", "release_channel": "gray"}
        if ranked[0][1] > self.threshold_high and (len(ranked) == 1 or ranked[0][1] - ranked[1][1] > self.margin):
            return {"action": "stable_top1", "release_channel": "stable"}
        if len(ranked) >= 2 and ranked[0][1] > 0.65 and ranked[1][1] > 0.58 and ranked[0][1] - ranked[1][1] <= self.margin:
            return {"action": "gray_top2", "release_channel": "gray"}
        return {"action": "gray_top1", "release_channel": "gray"}

    def _ordering_family(self) -> str | None:
        return self.family_names[3] if len(self.family_names) >= 4 else None

    def _family_reason(self, family: str, structure_features: dict, p: UniversalProfile) -> str:
        summarization_family = self.family_names[0] if len(self.family_names) >= 1 else ""
        title_family = self.family_names[1] if len(self.family_names) >= 2 else ""
        fill_family = self.family_names[2] if len(self.family_names) >= 3 else ""
        ordering_family = self.family_names[3] if len(self.family_names) >= 4 else ""
        continuation_family = self.family_names[4] if len(self.family_names) >= 5 else ""

        if family == ordering_family:
            markers = "/".join(structure_features.get("sequence_markers", [])[:3]) or "\u9996\u5148/\u5176\u6b21/\u518d\u6b21"
            if structure_features.get("strong_ordering_signal"):
                return f"\u5b58\u5728\u201c{markers}\u201d\u7b49\u5e8f\u5217\u6807\u8bb0\uff0c\u6bb5\u843d\u987a\u5e8f\u654f\u611f\uff0c\u6253\u4e71\u540e\u903b\u8f91\u4f1a\u53d7\u635f\uff0c\u53ef\u7528\u4e8e\u987a\u5e8f\u91cd\u5efa"
            return "\u6587\u672c\u5448\u73b0\u5206\u70b9\u63a8\u8fdb\u7ed3\u6784\uff0c\u6bb5\u95f4\u6709\u660e\u786e\u5148\u540e\u5173\u7cfb\uff0c\u9002\u5408\u8bed\u5e8f\u91cd\u5efa"

        if family == summarization_family:
            if p.summary_strength >= 0.68 or p.text_shape.paragraph_count >= 2:
                return "\u5168\u6587\u56f4\u7ed5\u5355\u4e00\u4e2d\u5fc3\u5c55\u5f00\uff0c\u591a\u4e2a\u5206\u70b9\u5171\u540c\u652f\u6491\u4e3b\u65e8\uff0c\u9002\u5408\u6982\u62ec\u6574\u5408"
            return "\u6587\u6bb5\u4e3b\u65e8\u96c6\u4e2d\uff0c\u4fe1\u606f\u53ef\u6536\u675f\u4e3a\u7edf\u4e00\u4e2d\u5fc3\uff0c\u9002\u5408\u6982\u62ec\u5f52\u7eb3"

        if family == title_family:
            if p.titleability >= 0.72:
                return "\u6587\u6bb5\u4e2d\u5fc3\u660e\u786e\uff0c\u53ef\u8fdb\u4e00\u6b65\u62bd\u8c61\u4e3a\u6807\u9898\u6216\u547d\u540d\u8868\u8fbe"
            return "\u6750\u6599\u6709\u7a33\u5b9a\u4e3b\u65e8\uff0c\u9002\u5408\u8fdb\u884c\u6807\u9898\u63d0\u70bc"

        if family == fill_family:
            if structure_features.get("has_parallel_markers") or p.transition_strength >= 0.62:
                return "\u5c40\u90e8\u5b58\u5728\u627f\u63a5\u6216\u8865\u4f4d\u9700\u6c42\uff0c\u53ef\u7528\u4e8e\u8854\u63a5\u8865\u4f4d\u7c7b\u9898\u578b"
            return "\u524d\u540e\u8bed\u4e49\u4f9d\u8d56\u8f83\u5f3a\uff0c\u9002\u5408\u8003\u67e5\u8bed\u53e5\u8854\u63a5\u4e0e\u8865\u4f4d"

        if family == continuation_family:
            if structure_features.get("has_tail_extension_signal"):
                return "\u6587\u6bb5\u6574\u4f53\u5b8c\u6574\uff0c\u4f46\u5c3e\u53e5\u4ecd\u7559\u6709\u660e\u786e\u5ef6\u5c55\u843d\u70b9\uff0c\u9002\u5408\u5c3e\u6bb5\u7eed\u5199"
            return "\u6587\u6bb5\u672c\u8eab\u5b8c\u6574\uff0c\u5c3e\u90e8\u5b58\u5728\u53ef\u7ee7\u7eed\u63a8\u5f00\u7684\u65b9\u5411\uff0c\u5177\u5907\u7eed\u5199\u6f5c\u529b"

        return "\u4fdd\u7559\u5e76\u5217\u65cf\u7c7b\u4ee5\u652f\u6301\u540e\u7eed\u51fa\u9898\u7b56\u7565"

    def _clamp(self, score: float) -> float:
        return round(max(0.0, min(score, 1.0)), 4)
