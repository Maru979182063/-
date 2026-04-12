from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class PatchScope:
    name: str
    allowed_fields: tuple[str, ...]
    locked_field_groups: tuple[str, ...]
    semantic_class: str
    default_executor: str
    default_source: str
    requires_llm: bool
    allows_rebuild: bool
    default_priority: int


_PATCH_SCOPES: dict[str, PatchScope] = {
    "analysis_only": PatchScope(
        name="analysis_only",
        allowed_fields=("analysis",),
        locked_field_groups=(
            "material",
            "stem",
            "options",
            "answer",
            "structure_truth",
            "control_build",
        ),
        semantic_class="targeted_repair",
        default_executor="apply_analysis_only_repair",
        default_source="auto_repair",
        requires_llm=True,
        allows_rebuild=False,
        default_priority=1,
    ),
    "single_distractor_patch": PatchScope(
        name="single_distractor_patch",
        allowed_fields=("options.target", "analysis"),
        locked_field_groups=(
            "material",
            "stem",
            "answer",
            "other_options",
            "structure_truth",
            "control_build",
        ),
        semantic_class="targeted_repair",
        default_executor="apply_distractor_patch",
        default_source="review_action",
        requires_llm=False,
        allows_rebuild=False,
        default_priority=1,
    ),
    "answer_binding_patch": PatchScope(
        name="answer_binding_patch",
        allowed_fields=("options", "answer", "analysis"),
        locked_field_groups=(
            "material",
            "stem",
            "structure_truth",
            "control_build",
        ),
        semantic_class="targeted_repair",
        default_executor="apply_answer_binding_patch",
        default_source="auto_repair",
        requires_llm=True,
        allows_rebuild=False,
        default_priority=2,
    ),
}


def get_patch_scope(name: str) -> PatchScope | None:
    if not name:
        return None
    return _PATCH_SCOPES.get(str(name))


def resolve_action_scope(action: str | None) -> PatchScope | None:
    if not action:
        return None
    if action == "distractor_patch":
        return _PATCH_SCOPES["single_distractor_patch"]
    return None


def resolve_repair_mode_scope(mode: str | None) -> PatchScope | None:
    if not mode:
        return None
    if mode == "analysis_only_repair":
        return _PATCH_SCOPES["analysis_only"]
    if mode in {
        "main_idea_axis_repair",
        "sentence_order_answer_explanation_repair",
        "answer_binding_patch",
    }:
        return _PATCH_SCOPES["answer_binding_patch"]
    return None


def list_patch_scopes() -> list[dict[str, Any]]:
    return [
        {
            "scope_name": scope.name,
            "allowed_fields": list(scope.allowed_fields),
            "locked_field_groups": list(scope.locked_field_groups),
            "semantic_class": scope.semantic_class,
            "default_executor": scope.default_executor,
            "default_source": scope.default_source,
            "requires_llm": scope.requires_llm,
            "allows_rebuild": scope.allows_rebuild,
            "default_priority": scope.default_priority,
        }
        for scope in _PATCH_SCOPES.values()
    ]
