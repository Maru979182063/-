from app.core.config import get_config_bundle


def get_family_routing_config() -> dict:
    return get_config_bundle().family_routing


def get_family_names() -> list[str]:
    return [family["name"] for family in get_family_routing_config().get("families", [])]


def get_family_subtypes(family_name: str) -> list[str]:
    for family in get_family_routing_config().get("families", []):
        if family.get("name") == family_name:
            return family.get("subtypes", [])
    return []


def get_thresholds() -> dict:
    thresholds = get_family_routing_config().get("thresholds", {})
    return {
        "threshold_low": thresholds.get("threshold_low", 0.45),
        "threshold_high": thresholds.get("threshold_high", 0.70),
        "margin": thresholds.get("margin", 0.12),
    }
