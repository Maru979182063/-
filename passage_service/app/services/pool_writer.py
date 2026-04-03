from app.rules.family_config import get_thresholds


class PoolWriter:
    def build_quality_flags(self, universal_profile: dict, family_scores: dict[str, float], decision: dict) -> list[str]:
        threshold_low = get_thresholds()["threshold_low"]
        flags: list[str] = []
        if all(score < threshold_low for score in family_scores.values()):
            flags.append("low_family_confidence")
        if universal_profile.get("independence_score", 0.0) < 0.45:
            flags.append("low_independence")
        if universal_profile.get("continuation_openness", 0.0) > 0.75 and universal_profile.get("direction_uniqueness", 0.0) < 0.55:
            flags.append("continuation_direction_ambiguous")
        if decision.get("release_channel") == "gray":
            flags.append("gray_review")
        return flags
