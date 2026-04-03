from typing import Any


class FitMapper:
    def __init__(self, mapping_config: dict[str, Any]) -> None:
        self.mapping_config = mapping_config or {}

    def compute(self, feature_profile: dict[str, Any]) -> dict[str, float]:
        mapping = self.mapping_config.get("fit_scores", {})
        result: dict[str, float] = {}
        for fit_name, rules in mapping.items():
            score = 0.0
            for field_name, weight in rules.get("weights", {}).items():
                score += float(feature_profile.get(field_name, 0.0)) * float(weight)
            result[fit_name] = round(min(score, 1.0), 4)
        return result
