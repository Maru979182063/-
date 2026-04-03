FEATURE_PROFILE_SCHEMA = {
    "type": "object",
    "properties": {
        "keep": {"type": "boolean"},
        "boundary_adjustment": {"type": "object"},
        "feature_profile": {"type": "object"},
        "reasons": {"type": "array"},
    },
    "required": ["keep", "boundary_adjustment", "feature_profile", "reasons"],
}
