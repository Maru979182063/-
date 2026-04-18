from __future__ import annotations

from fastapi import APIRouter

from app.services.diagnostics import build_prompt_diagnostics


router = APIRouter(prefix="/api/v1/diagnostics", tags=["diagnostics"])


@router.get("/dependencies")
def prompt_dependency_diagnostics() -> dict:
    return build_prompt_diagnostics()
