class PassageServiceError(Exception):
    """Base application error."""


class NotFoundError(PassageServiceError):
    """Resource was not found."""


class ConflictError(PassageServiceError):
    """Conflict with current state."""
