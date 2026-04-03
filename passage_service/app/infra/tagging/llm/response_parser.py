from app.domain.models.plugin_contracts import TaggingResult


def parse_tagging_response(payload: dict) -> TaggingResult:
    return TaggingResult.model_validate(payload)
