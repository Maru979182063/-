from __future__ import annotations

from typing import Final

from app.core.exceptions import DomainError
from app.schemas.api import PromptBuildRequest
from app.schemas.decoder import BatchMeta, DifyFormInput, MappingTarget

MIN_COUNT: Final[int] = 1
MAX_COUNT: Final[int] = 5

QUESTION_FOCUS_MAPPING: Final[dict[str, MappingTarget]] = {
    "\u4e3b\u65e8\u4e2d\u5fc3\u7c7b": MappingTarget(question_type="main_idea", business_subtype="center_understanding"),
    "\u6807\u9898\u586b\u5165\u9898": MappingTarget(question_type="main_idea", business_subtype="title_selection"),
    "title_selection": MappingTarget(question_type="main_idea", business_subtype="title_selection"),
    "\u63a5\u8bed\u9009\u62e9\u9898": MappingTarget(question_type="continuation", business_subtype=None),
    "continuation": MappingTarget(question_type="continuation", business_subtype=None),
    "\u8bed\u53e5\u6392\u5e8f\u9898": MappingTarget(question_type="sentence_order", business_subtype=None),
    "sentence_order": MappingTarget(question_type="sentence_order", business_subtype=None),
    "\u8bed\u53e5\u586b\u7a7a\u9898": MappingTarget(question_type="sentence_fill", business_subtype=None),
    "sentence_fill": MappingTarget(question_type="sentence_fill", business_subtype=None),
    "\u4e2d\u5fc3\u7406\u89e3\u9898": MappingTarget(question_type="main_idea", business_subtype="center_understanding"),
    "center_understanding": MappingTarget(question_type="main_idea", business_subtype="center_understanding"),
}

QUESTION_FOCUS_ALIASES: Final[dict[str, str]] = {
    "\u4e3b\u65e8\u4e2d\u5fc3\u7c7b": "\u4e2d\u5fc3\u7406\u89e3\u9898",
    "main_idea": "\u4e2d\u5fc3\u7406\u89e3\u9898",
    "title": "\u6807\u9898\u586b\u5165\u9898",
    "\u6392\u5e8f": "\u8bed\u53e5\u6392\u5e8f\u9898",
    "\u586b\u7a7a": "\u8bed\u53e5\u586b\u7a7a\u9898",
}

# Keep these mappings centralized so business can adjust them without touching router code.
# Pattern-based special types intentionally use pattern_id instead of inventing fake subtypes.
SPECIAL_TYPE_MAPPING: Final[dict[str, MappingTarget]] = {
    "\u4e3b\u65e8\u6982\u62ec": MappingTarget(question_type="main_idea", business_subtype="main_idea_general"),
    "main_idea_general": MappingTarget(question_type="main_idea", business_subtype="main_idea_general"),
    "\u9009\u62e9\u6807\u9898": MappingTarget(question_type="main_idea", business_subtype="title_selection"),
    "title_selection": MappingTarget(question_type="main_idea", business_subtype="title_selection"),
    "\u7ed3\u6784\u6982\u62ec": MappingTarget(question_type="main_idea", business_subtype="structure_summary"),
    "structure_summary": MappingTarget(question_type="main_idea", business_subtype="structure_summary"),
    "\u5c40\u90e8\u6bb5\u610f\u6982\u62ec": MappingTarget(question_type="main_idea", business_subtype="local_paragraph_summary"),
    "local_paragraph_summary": MappingTarget(question_type="main_idea", business_subtype="local_paragraph_summary"),
    "\u4e2d\u5fc3\u7406\u89e3\u9898": MappingTarget(question_type="main_idea", business_subtype="center_understanding"),
    "center_understanding": MappingTarget(question_type="main_idea", business_subtype="center_understanding"),
    "\u5c3e\u53e5\u76f4\u63a5\u627f\u63a5": MappingTarget(
        question_type="continuation",
        business_subtype=None,
        pattern_id="tail_anchor_direct_extend",
    ),
    "tail_anchor_direct_extend": MappingTarget(
        question_type="continuation",
        business_subtype=None,
        pattern_id="tail_anchor_direct_extend",
    ),
    "\u603b\u7ed3\u540e\u5f00\u542f\u65b0\u652f\u70b9": MappingTarget(
        question_type="continuation",
        business_subtype=None,
        pattern_id="summary_with_new_pivot",
    ),
    "summary_with_new_pivot": MappingTarget(
        question_type="continuation",
        business_subtype=None,
        pattern_id="summary_with_new_pivot",
    ),
    "\u95ee\u9898\u540e\u63a5\u5bf9\u7b56": MappingTarget(
        question_type="continuation",
        business_subtype=None,
        pattern_id="problem_solution_hook",
    ),
    "problem_solution_hook": MappingTarget(
        question_type="continuation",
        business_subtype=None,
        pattern_id="problem_solution_hook",
    ),
    "\u673a\u5236\u5c55\u5f00": MappingTarget(
        question_type="continuation",
        business_subtype=None,
        pattern_id="mechanism_unfolding",
    ),
    "mechanism_unfolding": MappingTarget(
        question_type="continuation",
        business_subtype=None,
        pattern_id="mechanism_unfolding",
    ),
    "\u4e3b\u9898\u8f6c\u5206\u8bdd\u9898": MappingTarget(
        question_type="continuation",
        business_subtype=None,
        pattern_id="raised_theme_to_subtopic",
    ),
    "raised_theme_to_subtopic": MappingTarget(
        question_type="continuation",
        business_subtype=None,
        pattern_id="raised_theme_to_subtopic",
    ),
    "\u89c2\u70b9\u540e\u63a5\u539f\u56e0": MappingTarget(
        question_type="continuation",
        business_subtype=None,
        pattern_id="judgement_to_reason",
    ),
    "judgement_to_reason": MappingTarget(
        question_type="continuation",
        business_subtype=None,
        pattern_id="judgement_to_reason",
    ),
    "\u4e2a\u6848\u5230\u5b8f\u89c2\u5c55\u5f00": MappingTarget(
        question_type="continuation",
        business_subtype=None,
        pattern_id="case_to_macro_unfold",
    ),
    "case_to_macro_unfold": MappingTarget(
        question_type="continuation",
        business_subtype=None,
        pattern_id="case_to_macro_unfold",
    ),
    "\u591a\u5206\u652f\u805a\u7126": MappingTarget(
        question_type="continuation",
        business_subtype=None,
        pattern_id="multi_branch_focus",
    ),
    "multi_branch_focus": MappingTarget(
        question_type="continuation",
        business_subtype=None,
        pattern_id="multi_branch_focus",
    ),
    "\u5f20\u529b\u89e3\u91ca": MappingTarget(
        question_type="continuation",
        business_subtype=None,
        pattern_id="tension_explained",
    ),
    "tension_explained": MappingTarget(
        question_type="continuation",
        business_subtype=None,
        pattern_id="tension_explained",
    ),
    "\u65b9\u6cd5\u5ef6\u5c55": MappingTarget(
        question_type="continuation",
        business_subtype=None,
        pattern_id="method_expansion",
    ),
    "method_expansion": MappingTarget(
        question_type="continuation",
        business_subtype=None,
        pattern_id="method_expansion",
    ),
    "\u53cc\u951a\u70b9\u9501\u5b9a": MappingTarget(
        question_type="sentence_order",
        business_subtype=None,
        pattern_id="dual_anchor_lock",
    ),
    "dual_anchor_lock": MappingTarget(
        question_type="sentence_order",
        business_subtype=None,
        pattern_id="dual_anchor_lock",
    ),
    "\u627f\u63a5\u5e76\u5217\u5c55\u5f00": MappingTarget(
        question_type="sentence_order",
        business_subtype=None,
        pattern_id="carry_parallel_expand",
    ),
    "carry_parallel_expand": MappingTarget(
        question_type="sentence_order",
        business_subtype=None,
        pattern_id="carry_parallel_expand",
    ),
    "\u89c2\u70b9-\u539f\u56e0-\u884c\u52a8\u6392\u5e8f": MappingTarget(
        question_type="sentence_order",
        business_subtype=None,
        pattern_id="viewpoint_reason_action",
    ),
    "viewpoint_reason_action": MappingTarget(
        question_type="sentence_order",
        business_subtype=None,
        pattern_id="viewpoint_reason_action",
    ),
    "\u95ee\u9898-\u5bf9\u7b56-\u6848\u4f8b\u6392\u5e8f": MappingTarget(
        question_type="sentence_order",
        business_subtype=None,
        pattern_id="problem_solution_case_blocks",
    ),
    "\u95ee\u9898\u2014\u5bf9\u7b56\u2014\u6848\u4f8b\u6392\u5e8f": MappingTarget(
        question_type="sentence_order",
        business_subtype=None,
        pattern_id="problem_solution_case_blocks",
    ),
    "problem_solution_case_blocks": MappingTarget(
        question_type="sentence_order",
        business_subtype=None,
        pattern_id="problem_solution_case_blocks",
    ),
    "\u5b9a\u4f4d\u63d2\u5165\u5339\u914d": MappingTarget(
        question_type="sentence_fill",
        business_subtype=None,
        pattern_id="inserted_reference_match",
    ),
    "inserted_reference_match": MappingTarget(
        question_type="sentence_fill",
        business_subtype=None,
        pattern_id="inserted_reference_match",
    ),
    "\u5f00\u5934\u603b\u8d77": MappingTarget(
        question_type="sentence_fill",
        business_subtype=None,
        pattern_id="opening_summary",
    ),
    "opening_summary": MappingTarget(
        question_type="sentence_fill",
        business_subtype=None,
        pattern_id="opening_summary",
    ),
    "\u8844\u63a5\u8fc7\u6e21": MappingTarget(
        question_type="sentence_fill",
        business_subtype=None,
        pattern_id="bridge_transition",
    ),
    "bridge_transition": MappingTarget(
        question_type="sentence_fill",
        business_subtype=None,
        pattern_id="bridge_transition",
    ),
    "\u4e2d\u6bb5\u7126\u70b9\u5207\u6362": MappingTarget(
        question_type="sentence_fill",
        business_subtype=None,
        pattern_id="middle_focus_shift",
    ),
    "middle_focus_shift": MappingTarget(
        question_type="sentence_fill",
        business_subtype=None,
        pattern_id="middle_focus_shift",
    ),
    "\u4e2d\u6bb5\u89e3\u91ca\u8bf4\u660e": MappingTarget(
        question_type="sentence_fill",
        business_subtype=None,
        pattern_id="middle_explanation",
    ),
    "middle_explanation": MappingTarget(
        question_type="sentence_fill",
        business_subtype=None,
        pattern_id="middle_explanation",
    ),
    "\u7ed3\u5c3e\u603b\u7ed3": MappingTarget(
        question_type="sentence_fill",
        business_subtype=None,
        pattern_id="ending_summary",
    ),
    "ending_summary": MappingTarget(
        question_type="sentence_fill",
        business_subtype=None,
        pattern_id="ending_summary",
    ),
    "\u7ed3\u5c3e\u5347\u534e": MappingTarget(
        question_type="sentence_fill",
        business_subtype=None,
        pattern_id="ending_elevation",
    ),
    "ending_elevation": MappingTarget(
        question_type="sentence_fill",
        business_subtype=None,
        pattern_id="ending_elevation",
    ),
    "\u7efc\u5408\u591a\u70b9\u5339\u914d": MappingTarget(
        question_type="sentence_fill",
        business_subtype=None,
        pattern_id="comprehensive_multi_match",
    ),
    "comprehensive_multi_match": MappingTarget(
        question_type="sentence_fill",
        business_subtype=None,
        pattern_id="comprehensive_multi_match",
    ),
}

SPECIAL_TYPE_ALIASES: Final[dict[str, str]] = {
    "\u4e0d\u6307\u5b9a": "",
    "\u4e0d\u6307\u5b9a\uff08\u81ea\u52a8\u5339\u914d\uff09": "",
    "\u8bf7\u9009\u62e9": "",
    # Backward compatibility for the current demo UI and older source-question labels.
    "turning_relation_focus": "center_understanding",
    "cause_effect__conclusion_focus": "center_understanding",
    "necessary_condition_countermeasure": "center_understanding",
    "theme_word_focus": "center_understanding",
    "parallel_comprehensive_summary": "main_idea_general",
    "turning_relation_focus__main_idea": "center_understanding",
    "cause_effect__conclusion_focus__main_idea": "center_understanding",
    "necessary_condition_countermeasure__main_idea": "center_understanding",
    "theme_word_focus__main_idea": "center_understanding",
    "parallel_comprehensive_summary__main_idea": "main_idea_general",
    "plot_continuation": "tail_anchor_direct_extend",
    "character_extension": "tail_anchor_direct_extend",
    "theme_extension": "raised_theme_to_subtopic",
    "foreshadow_recall": "summary_with_new_pivot",
    "setting_alignment": "tail_anchor_direct_extend",
    "emotion_progression": "tension_explained",
    "conflict_resolution": "tension_explained",
    "ending_resolution": "problem_solution_hook",
    "value_expression": "judgement_to_reason",
    "head_tail_logic": "dual_anchor_lock",
    "head_tail_lock": "dual_anchor_lock",
    "deterministic_binding": "carry_parallel_expand",
    "discourse_logic": "carry_parallel_expand",
    "timeline_action_sequence": "viewpoint_reason_action",
    "sentence_order__head_tail_logic__abstract": "dual_anchor_lock",
    "sentence_order__head_tail_lock__abstract": "dual_anchor_lock",
    "sentence_order__deterministic_binding__abstract": "carry_parallel_expand",
    "sentence_order__discourse_logic__abstract": "carry_parallel_expand",
    "sentence_order__timeline_action_sequence__abstract": "viewpoint_reason_action",
    "opening_topic_intro": "opening_summary",
    "middle_carry_previous": "middle_explanation",
    "middle_lead_next": "middle_focus_shift",
    "middle_bridge_both_sides": "bridge_transition",
    "ending_countermeasure": "ending_summary",
    "sentence_fill__opening_topic_intro__abstract": "opening_summary",
    "sentence_fill__middle_carry_previous__abstract": "middle_explanation",
    "sentence_fill__middle_lead_next__abstract": "middle_focus_shift",
    "sentence_fill__middle_bridge_both_sides__abstract": "bridge_transition",
    "sentence_fill__ending_countermeasure__abstract": "ending_summary",
}

DIFFICULTY_MAPPING: Final[dict[str, str]] = {
    "\u7b80\u5355": "easy",
    "\u4e2d\u7b49": "medium",
    "\u56f0\u96be": "hard",
    "easy": "easy",
    "medium": "medium",
    "hard": "hard",
}


class InputDecoderService:
    def decode(self, request: DifyFormInput) -> dict:
        selected_special_type = self._select_special_type(request.special_question_types)
        mapping_source = "special_question_type" if selected_special_type else "question_focus"
        mapping_target = self._resolve_mapping(request.question_focus, selected_special_type)
        difficulty_target = self._resolve_difficulty(request.difficulty_level)
        requested_count = request.count or 1
        effective_count, count_warnings = self._normalize_count(requested_count)
        extra_constraints = self._build_extra_constraints(request.text_direction)

        standard_request = PromptBuildRequest(
            question_type=mapping_target.question_type,
            business_subtype=mapping_target.business_subtype,
            pattern_id=mapping_target.pattern_id,
            difficulty_target=difficulty_target,
            topic=None,
            count=effective_count,
            passage_style=None,
            use_fewshot=True,
            fewshot_mode="structure_only",
            type_slots={},
            extra_constraints=extra_constraints,
        )
        batch_meta = BatchMeta(
            requested_count=requested_count,
            effective_count=effective_count,
            question_type=mapping_target.question_type,
            business_subtype=mapping_target.business_subtype,
            pattern_id=mapping_target.pattern_id,
            difficulty_target=difficulty_target,
        )
        return {
            "mapping_source": mapping_source,
            "selected_special_type": selected_special_type,
            "standard_request": standard_request.model_dump(),
            "batch_meta": batch_meta.model_dump(),
            "warnings": count_warnings,
        }

    def _select_special_type(self, special_question_types: list[str]) -> str | None:
        if not special_question_types:
            return None
        if len(special_question_types) > 1:
            raise DomainError(
                "Only one special_question_type can be selected.",
                status_code=422,
                details={
                    "special_question_types": special_question_types,
                    "rule": "special_question_type is logically single-select even if the UI uses checkboxes.",
                },
            )
        selected = special_question_types[0].strip()
        if not selected:
            return None
        if selected.lower() in {"select", "auto"}:
            return None
        aliased = SPECIAL_TYPE_ALIASES.get(selected, selected)
        return aliased or None

    def _resolve_mapping(self, question_focus: str, selected_special_type: str | None) -> MappingTarget:
        if selected_special_type:
            mapping = SPECIAL_TYPE_MAPPING.get(selected_special_type)
            if not mapping:
                raise DomainError(
                    "Selected special_question_type is not mapped yet.",
                    status_code=422,
                    details={"special_question_type": selected_special_type},
                )
            return mapping

        normalized_focus = (question_focus or "").strip()
        if normalized_focus.lower() in {"select", "auto"} or normalized_focus in {"\u4e0d\u6307\u5b9a", "\u4e0d\u6307\u5b9a\uff08\u81ea\u52a8\u5339\u914d\uff09", "\u8bf7\u9009\u62e9"}:
            normalized_focus = ""
        if not normalized_focus:
            raise DomainError(
                "question_focus is required when no valid reference-question inference is available.",
                status_code=422,
                details={"question_focus": question_focus},
            )
        normalized_focus = QUESTION_FOCUS_ALIASES.get(normalized_focus, normalized_focus)
        mapping = QUESTION_FOCUS_MAPPING.get(normalized_focus)
        if not mapping:
            raise DomainError(
                "Selected question_focus is not mapped yet.",
                status_code=422,
                details={"question_focus": question_focus, "normalized_question_focus": normalized_focus},
            )
        return mapping

    def _resolve_difficulty(self, difficulty_level: str) -> str:
        difficulty_target = DIFFICULTY_MAPPING.get(difficulty_level)
        if not difficulty_target:
            raise DomainError(
                "Unsupported difficulty_level.",
                status_code=422,
                details={
                    "difficulty_level": difficulty_level,
                    "supported": sorted(set(DIFFICULTY_MAPPING.keys())),
                },
            )
        return difficulty_target

    def _normalize_count(self, requested_count: int) -> tuple[int, list[str]]:
        warnings: list[str] = []
        effective_count = requested_count
        if requested_count < MIN_COUNT:
            effective_count = MIN_COUNT
            warnings.append("count was below 1 and has been corrected to 1.")
        elif requested_count > MAX_COUNT:
            effective_count = MAX_COUNT
            warnings.append("count was above 5 and has been truncated to 5 for the current demo.")
        return effective_count, warnings

    def _build_extra_constraints(self, text_direction: str | None) -> dict:
        # The prompt engine does not consume text_direction yet; keep it in extra_constraints for future use.
        if not text_direction:
            return {}
        return {"text_direction": text_direction}
