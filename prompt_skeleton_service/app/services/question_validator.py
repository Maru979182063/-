from __future__ import annotations

import re
import unicodedata
from difflib import SequenceMatcher
from typing import Any, Callable

from app.schemas.item import GeneratedQuestion, ValidationResult
from app.services.sentence_fill_protocol import normalize_sentence_fill_function_type


ValidatorFn = Callable[[GeneratedQuestion, dict[str, Any]], tuple[list[str], list[str], dict[str, Any]]]


class QuestionValidatorService:
    EXPLICIT_ANSWER_PATTERNS = (
        re.compile(r"\u6b63\u786e\u7b54\u6848(?:\u4e3a|\u662f)?\s*([A-F])", flags=re.IGNORECASE),
        re.compile(r"\u7b54\u6848(?:\u4e3a|\u662f)?\s*([A-F])", flags=re.IGNORECASE),
        re.compile(r"\u9009\u9879\s*([A-F])\s*\u6b63\u786e", flags=re.IGNORECASE),
        re.compile(r"([A-F])\s*\u9879(?:\u6b63\u786e|\u7b26\u5408\u9898\u610f)", flags=re.IGNORECASE),
        re.compile(r"[\uff08(]\s*([A-F])\s*[)\uff09]\s*(?:\u6b63\u786e|\u7b26\u5408\u9898\u610f|\u5f53\u9009)", flags=re.IGNORECASE),
    )
    HARD_ERROR_CODES = {
        "ordering_chain_incomplete",
        "sentence_count_mismatch",
        "binding_violation",
        "reference_anchor_missing",
    }
    HARD_ERROR_PREFIXES = (
        "stem must not be empty.",
        "analysis must not be empty.",
        "options count must be between 2 and 6.",
        "options must not be empty:",
        "answer must not be empty.",
        "answer must be one of the options.",
        "options text must not all be identical.",
        "analysis explicitly marks multiple options as correct",
        "analysis explicitly marks option ",
        "selected material contains repeated sentence-level fragments",
        "selected material shows obvious stitched or overlapping fragments",
        "sentence_fill correct option must be the original removed sentence.",
        "sentence_fill correct option is not sufficiently grounded in the removed source span.",
        "Structured question output was not produced.",
    )

    def __init__(self) -> None:
        self.registry: dict[str, ValidatorFn] = {
            "main_idea": self._validate_main_idea,
            "continuation": self._validate_continuation,
            "sentence_order": self._validate_sentence_order,
            "sentence_fill": self._validate_sentence_fill,
        }

    @staticmethod
    def _build_contract_gated_check(
        *,
        active: bool,
        passed: bool,
        source: str,
        **details: Any,
    ) -> dict[str, Any]:
        payload = {
            **details,
            "required": active,
            "source": source,
            "status": "active" if active else "skipped_missing_contract",
        }
        payload["passed"] = passed if active else None
        return payload

    @staticmethod
    def _append_unique_error(errors: list[str], code: str) -> None:
        if code and code not in errors:
            errors.append(code)

    @staticmethod
    def _coerce_int(value: Any) -> int | None:
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _coerce_float(value: Any) -> float | None:
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _lookup_contract_value(
        sources: list[tuple[dict[str, Any] | None, str]],
        field_names: tuple[str, ...],
    ) -> tuple[Any, str]:
        for payload, source in sources:
            if not isinstance(payload, dict):
                continue
            for field_name in field_names:
                if payload.get(field_name) not in (None, ""):
                    return payload.get(field_name), source
        return None, ""

    @staticmethod
    def _read_nested_value(payload: dict[str, Any] | None, path: tuple[str, ...]) -> Any:
        current: Any = payload or {}
        for key in path:
            if not isinstance(current, dict):
                return None
            current = current.get(key)
        return current

    @staticmethod
    def _normalize_band_allowed(value: Any) -> list[str]:
        if value is None:
            return []
        if isinstance(value, str):
            bands = [part.strip().lower() for part in value.replace("|", ",").split(",") if part.strip()]
            return [band for band in bands if band in {"easy", "medium", "hard"}]
        if isinstance(value, (list, tuple, set)):
            bands = [str(part).strip().lower() for part in value if str(part).strip()]
            return [band for band in bands if band in {"easy", "medium", "hard"}]
        return []

    def _task_family_for_context(self, context: dict[str, Any]) -> str:
        question_type = str(context.get("question_type") or "").strip()
        business_subtype = str(context.get("business_subtype") or "").strip()
        if question_type in {"main_idea", "sentence_fill", "sentence_order"}:
            return question_type
        if business_subtype == "title_selection":
            return "main_idea"
        return question_type

    def _extract_material_scoring(self, context: dict[str, Any]) -> tuple[dict[str, Any], str]:
        material_source = context.get("material_source") or {}
        if not isinstance(material_source, dict):
            return {}, "missing"
        scoring = material_source.get("scoring")
        if isinstance(scoring, dict) and scoring:
            return dict(scoring), "material_source.scoring"
        selected_task_scoring = material_source.get("selected_task_scoring")
        if isinstance(selected_task_scoring, dict) and selected_task_scoring:
            return dict(selected_task_scoring), "material_source.selected_task_scoring"
        task_scoring = material_source.get("task_scoring")
        task_family = self._task_family_for_context(context)
        if isinstance(task_scoring, dict) and isinstance(task_scoring.get(task_family), dict):
            return dict(task_scoring.get(task_family) or {}), f"material_source.task_scoring.{task_family}"
        return {}, "missing"

    def _material_scoring_compatibility_profile(self, *, task_family: str, business_subtype: str | None = None) -> dict[str, Any]:
        if task_family == "main_idea" and business_subtype == "title_selection":
            return {
                "min_final_candidate_score": 0.35,
                "min_readiness_score": 0.45,
                "max_total_penalty": 0.40,
                "review_if_high_readiness_high_penalty": True,
                "min_reasoning_depth_score": 0.45,
                "max_ambiguity_score": 0.50,
            }
        if task_family == "main_idea" and business_subtype == "center_understanding":
            return {
                "min_final_candidate_score": 0.35,
                "min_readiness_score": 0.45,
                "max_total_penalty": 0.40,
                "review_if_high_readiness_high_penalty": True,
                "min_reasoning_depth_score": 0.50,
                "max_ambiguity_score": 0.50,
            }
        if task_family == "main_idea":
            return {
                "min_final_candidate_score": 0.30,
                "min_readiness_score": 0.40,
                "max_total_penalty": 0.45,
                "review_if_high_readiness_high_penalty": True,
                "min_reasoning_depth_score": 0.40,
                "max_ambiguity_score": 0.58,
            }
        if task_family == "sentence_fill":
            return {
                "min_final_candidate_score": 0.30,
                "min_readiness_score": 0.45,
                "max_total_penalty": 0.72,
                "review_if_high_readiness_high_penalty": True,
                "min_reasoning_depth_score": 0.60,
                "min_constraint_intensity_score": 0.60,
                "max_role_ambiguity_penalty": 0.50,
                "max_standalone_penalty": 0.40,
            }
        if task_family == "sentence_order":
            return {
                "min_final_candidate_score": 0.35,
                "min_readiness_score": 0.40,
                "max_total_penalty": 0.67,
                "review_if_high_readiness_high_penalty": True,
                "min_complexity_score": 0.70,
                "min_constraint_intensity_score": 0.70,
                "max_first_instability_penalty": 0.35,
                "max_last_instability_penalty": 0.35,
                "max_weak_constraint_penalty": 0.20,
            }
        return {}

    def _resolve_scoring_contract_value(
        self,
        *,
        sources: list[tuple[dict[str, Any] | None, str]],
        field_names: tuple[str, ...],
        compatibility: dict[str, Any],
        compatibility_key: str,
    ) -> tuple[Any, str]:
        value, source = self._lookup_contract_value(sources, field_names)
        if value not in (None, ""):
            return value, source
        if compatibility_key in compatibility:
            return compatibility.get(compatibility_key), "compatibility"
        return None, "compatibility_disabled"

    def _apply_scoring_threshold_check(
        self,
        *,
        checks: dict[str, Any],
        errors: list[str],
        warnings: list[str],
        check_name: str,
        actual: float,
        threshold: float | None,
        source: str,
        comparator: str,
        reason: str,
        error_message: str,
        warn_only_on_compatibility: bool = True,
        extra: dict[str, Any] | None = None,
    ) -> None:
        active = threshold is not None
        if comparator == "min":
            passed = actual >= float(threshold or 0.0)
            payload = {
                "actual": round(actual, 4),
                "threshold": round(float(threshold or 0.0), 4) if threshold is not None else None,
                "reason": reason,
            }
        else:
            passed = actual <= float(threshold or 0.0)
            payload = {
                "actual": round(actual, 4),
                "threshold": round(float(threshold or 0.0), 4) if threshold is not None else None,
                "reason": reason,
            }
        if extra:
            payload.update(extra)
        checks[check_name] = self._build_contract_gated_check(
            active=active,
            passed=passed if active else True,
            source=source,
            **payload,
        )
        if not active or passed:
            return
        if warn_only_on_compatibility and source == "compatibility":
            warnings.append(error_message)
        else:
            self._append_unique_error(errors, error_message)

    def _apply_scoring_band_check(
        self,
        *,
        checks: dict[str, Any],
        errors: list[str],
        warnings: list[str],
        check_name: str,
        difficulty_band: str,
        allowed_bands: list[str],
        source: str,
        error_message: str,
    ) -> None:
        active = bool(allowed_bands)
        passed = difficulty_band in allowed_bands if active else True
        checks[check_name] = self._build_contract_gated_check(
            active=active,
            passed=passed if active else True,
            source=source,
            difficulty_band=difficulty_band,
            allowed_range=allowed_bands,
            reason="difficulty_band_allowed",
        )
        if not active or passed:
            return
        if source == "compatibility":
            warnings.append(error_message)
        else:
            self._append_unique_error(errors, error_message)

    def _resolve_sentence_order_units_for_sequence(
        self,
        *,
        original_sentences: list[str],
        order: list[int],
    ) -> list[tuple[int, str]]:
        resolved: list[tuple[int, str]] = []
        for raw_index in order:
            unit_index = self._coerce_int(raw_index)
            if unit_index is None or unit_index < 1 or unit_index > len(original_sentences):
                return []
            resolved.append((unit_index, str(original_sentences[unit_index - 1] or "").strip()))
        return resolved

    def _sentence_order_head_is_illegal(self, text: str) -> bool:
        candidate = (text or "").strip()
        if not candidate:
            return True
        if candidate.startswith(("这", "这种", "这一", "该", "此")):
            return True
        if candidate.startswith(("但是", "然而", "不过")):
            return True
        if candidate.startswith(("同时", "此外", "在此基础上", "另一方面", "与此同时", "而")):
            return True
        if candidate.startswith(("因此", "所以")):
            return True
        return False

    def _sentence_order_tail_is_illegal(self, text: str) -> bool:
        candidate = (text or "").strip()
        if not candidate:
            return True
        if candidate.startswith(("棣栧厛", "鍏舵", "鍐嶆")):
            return True
        if candidate.startswith(("渚嬪", "姣斿")):
            return True
        if any(token in candidate for token in ("渚嬪", "姣斿")):
            return True
        if any(token in candidate for token in ("棣栧厛", "鍏舵", "鍐嶆")):
            return True
        return False

    def _extract_sentence_order_binding_pairs(self, context: dict[str, Any]) -> list[tuple[int, int]]:
        candidates: list[Any] = []
        material_source = context.get("material_source")
        resolved_slots = context.get("resolved_slots")
        control_logic = context.get("control_logic")
        source_question = context.get("source_question")
        source_question_analysis = context.get("source_question_analysis")
        validator_contract = context.get("validator_contract")

        if isinstance(material_source, dict):
            prompt_extras = material_source.get("prompt_extras")
            if isinstance(prompt_extras, dict):
                candidates.extend(
                    [
                        prompt_extras.get("binding_pairs"),
                        prompt_extras.get("sentence_order_binding_pairs"),
                    ]
                )
        if isinstance(resolved_slots, dict):
            candidates.extend(
                [
                    resolved_slots.get("binding_pairs"),
                    (resolved_slots.get("structure_schema") or {}).get("binding_pairs")
                    if isinstance(resolved_slots.get("structure_schema"), dict)
                    else None,
                ]
            )
        if isinstance(control_logic, dict):
            candidates.extend(
                [
                    control_logic.get("binding_pairs"),
                    (control_logic.get("sentence_order") or {}).get("binding_pairs")
                    if isinstance(control_logic.get("sentence_order"), dict)
                    else None,
                ]
            )
        if isinstance(source_question, dict):
            candidates.extend(
                [
                    source_question.get("binding_pairs"),
                    (source_question.get("control_logic") or {}).get("binding_pairs")
                    if isinstance(source_question.get("control_logic"), dict)
                    else None,
                ]
            )
        if isinstance(source_question_analysis, dict):
            structure_constraints = source_question_analysis.get("structure_constraints")
            candidates.extend(
                [
                    source_question_analysis.get("binding_pairs"),
                    structure_constraints.get("binding_pairs") if isinstance(structure_constraints, dict) else None,
                    source_question_analysis.get("control_logic"),
                ]
            )
        if isinstance(validator_contract, dict):
            sentence_order_contract = validator_contract.get("sentence_order")
            structure_contract = validator_contract.get("structure_constraints")
            candidates.extend(
                [
                    validator_contract.get("binding_pairs"),
                    sentence_order_contract.get("binding_pairs") if isinstance(sentence_order_contract, dict) else None,
                    structure_contract.get("binding_pairs") if isinstance(structure_contract, dict) else None,
                ]
            )

        pairs: list[tuple[int, int]] = []
        for candidate in candidates:
            if candidate is None:
                continue
            if isinstance(candidate, dict):
                nested = candidate.get("binding_pairs")
                if nested is not None:
                    candidates.append(nested)
                continue
            if not isinstance(candidate, list):
                continue
            for item in candidate:
                pair = self._coerce_binding_pair(item)
                if pair is not None and pair not in pairs:
                    pairs.append(pair)
        return pairs

    def _coerce_binding_pair(self, value: Any) -> tuple[int, int] | None:
        if isinstance(value, (list, tuple)) and len(value) >= 2:
            left = self._coerce_int(value[0])
            right = self._coerce_int(value[1])
            if left is not None and right is not None:
                return left, right
            return None
        if isinstance(value, str):
            numbers = [self._coerce_int(part) for part in re.findall(r"\d+", value)]
            if len(numbers) >= 2 and numbers[0] is not None and numbers[1] is not None:
                return numbers[0], numbers[1]
            return None
        if isinstance(value, dict):
            aliases = (
                ("before", "after"),
                ("first", "second"),
                ("left", "right"),
                ("source", "target"),
                ("from", "to"),
                ("a", "b"),
            )
            for left_key, right_key in aliases:
                if left_key in value and right_key in value:
                    left = self._coerce_int(value.get(left_key))
                    right = self._coerce_int(value.get(right_key))
                    if left is not None and right is not None:
                        return left, right
        return None

    def _extract_sentence_order_roles(self, context: dict[str, Any]) -> dict[int, str]:
        def _coerce_roles(candidate: Any) -> dict[int, str]:
            roles: dict[int, str] = {}
            if candidate is None:
                return roles
            if isinstance(candidate, dict):
                for raw_key, raw_role in candidate.items():
                    key = self._coerce_int(raw_key)
                    role = self._normalize_sentence_order_role(raw_role)
                    if key is not None and role:
                        roles[key] = role
            elif isinstance(candidate, list):
                for index, raw_role in enumerate(candidate, start=1):
                    role = self._normalize_sentence_order_role(raw_role)
                    if role:
                        roles[index] = role
            return roles

        prioritized_candidates: list[Any] = []
        fallback_candidates: list[Any] = []
        material_source = context.get("material_source")
        resolved_slots = context.get("resolved_slots")
        control_logic = context.get("control_logic")
        source_question = context.get("source_question")
        source_question_analysis = context.get("source_question_analysis")
        validator_contract = context.get("validator_contract")

        if isinstance(material_source, dict):
            prompt_extras = material_source.get("prompt_extras")
            if isinstance(prompt_extras, dict):
                prioritized_candidates.extend(
                    [
                        prompt_extras.get("sentence_roles"),
                        prompt_extras.get("sentence_order_roles"),
                    ]
                )
        if isinstance(validator_contract, dict):
            sentence_order_contract = validator_contract.get("sentence_order")
            structure_contract = validator_contract.get("structure_constraints")
            prioritized_candidates.extend(
                [
                    validator_contract.get("sentence_roles"),
                    sentence_order_contract.get("sentence_roles") if isinstance(sentence_order_contract, dict) else None,
                    structure_contract.get("sentence_roles") if isinstance(structure_contract, dict) else None,
                ]
            )
        if isinstance(resolved_slots, dict):
            prioritized_candidates.extend(
                [
                    resolved_slots.get("sentence_roles"),
                    (resolved_slots.get("structure_schema") or {}).get("sentence_roles")
                    if isinstance(resolved_slots.get("structure_schema"), dict)
                    else None,
                ]
            )
        if isinstance(control_logic, dict):
            prioritized_candidates.extend([control_logic.get("sentence_roles"), control_logic.get("roles")])
        if isinstance(source_question, dict):
            fallback_candidates.extend(
                [
                    source_question.get("sentence_roles"),
                    (source_question.get("control_logic") or {}).get("sentence_roles")
                    if isinstance(source_question.get("control_logic"), dict)
                    else None,
                ]
            )
        if isinstance(source_question_analysis, dict):
            structure_constraints = source_question_analysis.get("structure_constraints")
            fallback_candidates.extend(
                [
                    source_question_analysis.get("sentence_roles"),
                    structure_constraints.get("sentence_roles") if isinstance(structure_constraints, dict) else None,
                ]
            )

        for candidate in prioritized_candidates:
            roles = _coerce_roles(candidate)
            if roles:
                return roles

        for candidate in fallback_candidates:
            roles = _coerce_roles(candidate)
            if roles:
                return roles
        return {}

    def _normalize_sentence_order_role(self, value: Any) -> str:
        role = str(value or "").strip().lower()
        if not role:
            return ""
        if role in {"summary", "conclusion", "closing", "tail_statement", "action", "countermeasure"}:
            return "conclusion"
        if role in {"thesis", "definition", "viewpoint", "opening_anchor"}:
            return "thesis"
        if role in {"transition", "connector", "timeline", "dependent"}:
            return "transition"
        return role

    def _infer_sentence_order_role(self, text: str) -> str:
        candidate = (text or "").strip()
        if not candidate:
            return ""
        if candidate.startswith(("因此", "所以", "总之", "可见", "由此", "无论如何")) or any(
            token in candidate for token in ("因此", "所以", "总之", "可见", "由此", "无论如何")
        ):
            return "conclusion"
        if candidate.startswith(("但是", "然而", "不过", "同时", "此外", "在此基础上", "另一方面", "与此同时", "而")):
            return "transition"
        if candidate.startswith(("总体而言", "总的来看", "归根结底", "关键在于", "本质上")):
            return "thesis"
        if "是" in candidate and any(token in candidate for token in ("必要条件", "关键", "基础")):
            return "thesis"
        return ""

    def _normalize_sentence_fill_function_type(self, value: Any) -> str:
        return normalize_sentence_fill_function_type(value)

    def _extract_sentence_fill_constraints(self, context: dict[str, Any]) -> dict[str, Any]:
        validator_contract = context.get("validator_contract") or {}
        material_source = context.get("material_source") or {}
        material_prompt_extras = (
            material_source.get("prompt_extras")
            if isinstance(material_source, dict) and isinstance(material_source.get("prompt_extras"), dict)
            else {}
        )
        resolved_slots = context.get("resolved_slots") or {}
        control_logic = context.get("control_logic") or {}

        sentence_fill_contract = validator_contract.get("sentence_fill") if isinstance(validator_contract, dict) else {}
        structure_contract = (
            validator_contract.get("structure_constraints") if isinstance(validator_contract, dict) else {}
        )

        def lookup(field: str) -> tuple[Any, str]:
            if field in material_prompt_extras and material_prompt_extras.get(field) not in (None, ""):
                return material_prompt_extras.get(field), "material_source.prompt_extras"
            if isinstance(resolved_slots, dict) and resolved_slots.get(field) not in (None, ""):
                return resolved_slots.get(field), "resolved_slots"
            if isinstance(sentence_fill_contract, dict) and sentence_fill_contract.get(field) not in (None, ""):
                return sentence_fill_contract.get(field), "validator_contract"
            if isinstance(structure_contract, dict) and structure_contract.get(field) not in (None, ""):
                return structure_contract.get(field), "validator_contract"
            if isinstance(validator_contract, dict) and validator_contract.get(field) not in (None, ""):
                return validator_contract.get(field), "validator_contract"
            if isinstance(control_logic, dict) and control_logic.get(field) not in (None, ""):
                return control_logic.get(field), "control_logic"
            return None, "compatibility_disabled"

        position_value, position_source = lookup("blank_position")
        if position_value in (None, ""):
            position_value, position_source = lookup("position")
        function_value, function_source = lookup("function_type")
        reference_value, reference_source = lookup("reference_anchor")
        bidirectional_value, bidirectional_source = lookup("bidirectional_check")
        semantic_scope_value, semantic_scope_source = lookup("semantic_scope")

        contract_position = ""
        contract_function = ""
        if isinstance(sentence_fill_contract, dict):
            contract_position = str(sentence_fill_contract.get("blank_position") or sentence_fill_contract.get("position") or "")
            contract_function = self._normalize_sentence_fill_function_type(sentence_fill_contract.get("function_type"))
        if not contract_position and isinstance(structure_contract, dict):
            contract_position = str(structure_contract.get("blank_position") or structure_contract.get("position") or "")
        if not contract_function and isinstance(structure_contract, dict):
            contract_function = self._normalize_sentence_fill_function_type(structure_contract.get("function_type"))

        return {
            "blank_position": str(position_value or ""),
            "blank_position_source": position_source,
            "function_type": self._normalize_sentence_fill_function_type(function_value),
            "function_type_source": function_source,
            "reference_anchor": str(reference_value or ""),
            "reference_anchor_source": reference_source,
            "bidirectional_check": bidirectional_value if isinstance(bidirectional_value, dict) else {},
            "bidirectional_check_source": bidirectional_source,
            "semantic_scope": str(semantic_scope_value or ""),
            "semantic_scope_source": semantic_scope_source,
            "contract_blank_position": contract_position,
            "contract_function_type": contract_function,
            "runtime_blank_position": str(material_prompt_extras.get("blank_position") or material_prompt_extras.get("position") or ""),
            "runtime_function_type": self._normalize_sentence_fill_function_type(material_prompt_extras.get("function_type")),
        }

    def _extract_sentence_fill_blank_context(self, material_text: str) -> tuple[str, str, str]:
        text = (material_text or "").strip()
        if not text:
            return "", "", ""
        markers = ("____", "___", "[BLANK]", "（  ）", "( )", "（   ）", "（）")
        for marker in markers:
            idx = text.find(marker)
            if idx >= 0:
                return text[:idx].strip(), text[idx + len(marker) :].strip(), marker
        return text, "", ""

    def _sentence_fill_correct_option_text(self, generated_question: GeneratedQuestion) -> str:
        answer = str(generated_question.answer or "").strip().upper()
        options = generated_question.options or {}
        return str(options.get(answer) or "").strip()

    def _normalize_sentence_fill_anchor_text(self, text: str) -> str:
        normalized = unicodedata.normalize("NFKC", str(text or ""))
        normalized = normalized.replace("…", "...").replace("—", "-")
        normalized = re.sub(r"\s+", "", normalized)
        normalized = re.sub(r"[銆傦紒锛燂紱锛氾紝銆?.!?;:]+$", "", normalized)
        return normalized.strip().lower()

    def _sentence_fill_has_reference_anchor(self, text: str) -> bool:
        candidate = (text or "").strip()
        if not candidate:
            return False
        return bool(
            re.search(
                r"((?:\u8fd9(?:\u4e00|\u79cd|\u7c7b)?|\u8be5|\u6b64)(?:\u7406\u8bba|\u73b0\u8c61|\u8fc7\u7a0b|\u95ee\u9898|\u60c5\u51b5|\u8d8b\u52bf|\u7ed3\u679c|\u673a\u5236|\u6a21\u5f0f|\u8def\u5f84|\u53d8\u5316|\u505a\u6cd5|\u89c2\u70b9|\u7ed3\u8bba|\u903b\u8f91|\u9636\u6bb5|\u80cc\u666f)?)",
                candidate,
            )
        )

    def _sentence_fill_reference_anchor_support(self, *, candidate_text: str, previous_text: str) -> dict[str, Any]:
        candidate = (candidate_text or "").strip()
        previous = (previous_text or "").strip()
        match = re.search(
            r"((?:\u8fd9(?:\u4e00|\u79cd|\u7c7b)?|\u8be5|\u6b64)(?P<head>\u7406\u8bba|\u73b0\u8c61|\u8fc7\u7a0b|\u95ee\u9898|\u60c5\u51b5|\u8d8b\u52bf|\u7ed3\u679c|\u673a\u5236|\u6a21\u5f0f|\u8def\u5f84|\u53d8\u5316|\u505a\u6cd5|\u89c2\u70b9|\u7ed3\u8bba|\u903b\u8f91|\u9636\u6bb5|\u80cc\u666f)?)",
            candidate,
        )
        if not match:
            return {"has_anchor": False, "passed": False, "anchor": "", "head": ""}
        head = match.group("head") or ""
        if not previous:
            return {"has_anchor": True, "passed": False, "anchor": match.group(0), "head": head}
        if head:
            return {
                "has_anchor": True,
                "passed": head in previous,
                "anchor": match.group(0),
                "head": head,
            }
        return {"has_anchor": True, "passed": True, "anchor": match.group(0), "head": head}

    def _sentence_fill_has_conclusion_marker(self, text: str) -> bool:
        candidate = (text or "").strip()
        markers = ("??", "??", "??", "??", "??", "??")
        return candidate.startswith(markers) or any(token in candidate for token in markers)

    def _sentence_fill_has_countermeasure_marker(self, text: str) -> bool:
        candidate = (text or "").strip()
        markers = ("??", "??", "??", "??", "?", "??", "??")
        return candidate.startswith(markers) or any(token in candidate for token in markers)

    def _sentence_fill_has_specific_action(self, text: str) -> bool:
        candidate = (text or "").strip()
        markers = ("??", "??", "??", "??", "??", "??", "??", "??", "??", "??", "??")
        return any(token in candidate for token in markers)

    def _sentence_fill_has_backward_signal(self, text: str) -> bool:
        candidate = (text or "").strip()
        markers = ("?", "??", "??", "?", "?", "??", "??", "??")
        return candidate.startswith(markers) or any(token in candidate for token in markers)

    def _sentence_fill_has_forward_signal(self, text: str) -> bool:
        candidate = (text or "").strip()
        markers = ("??", "??", "??", "??", "??", "??", "???", "??")
        return candidate.startswith(markers) or any(token in candidate for token in markers)

    def _sentence_fill_support_ratio(self, *, evidence_text: str, candidate_text: str) -> float:
        return self._compute_support_profile(evidence_text=evidence_text, candidate_text=candidate_text)["supported_token_ratio"]

    def _sentence_fill_directional_validity(
        self,
        *,
        candidate_text: str,
        previous_text: str,
        next_text: str,
    ) -> dict[str, Any]:
        prev_ratio = self._sentence_fill_support_ratio(evidence_text=previous_text, candidate_text=candidate_text)
        next_ratio = self._sentence_fill_support_ratio(evidence_text=next_text, candidate_text=candidate_text)
        previous_valid = bool(previous_text) and (
            self._sentence_fill_has_backward_signal(candidate_text)
            or self._sentence_fill_reference_anchor_support(candidate_text=candidate_text, previous_text=previous_text)["passed"]
            or prev_ratio >= 0.12
        )
        next_valid = bool(next_text) and (
            self._sentence_fill_has_forward_signal(candidate_text)
            or next_ratio >= 0.12
        )
        return {
            "previous_valid": previous_valid,
            "next_valid": next_valid,
            "previous_ratio": prev_ratio,
            "next_ratio": next_ratio,
        }

    def _normalize_main_idea_argument_structure(self, value: Any) -> str:
        raw = str(value or "").strip().lower()
        if not raw:
            return ""
        alias_map = {
            "turning": "sub_total",
            "contrast": "sub_total",
            "sub_total": "sub_total",
            "cause_effect": "phenomenon_analysis",
            "phenomenon_analysis": "phenomenon_analysis",
            "problem_solution": "problem_solution",
            "example_to_conclusion": "example_conclusion",
            "example_conclusion": "example_conclusion",
            "case_to_theme_elevation": "example_conclusion",
            "final_summary": "total_sub",
            "explicit_single_center": "total_sub",
            "total_sub": "total_sub",
            "parallel": "parallel",
        }
        return alias_map.get(raw, raw)

    def _normalize_main_idea_axis_source(self, value: Any) -> str:
        raw = str(value or "").strip().lower()
        if not raw:
            return ""
        alias_map = {
            "whole_passage": "global_abstraction",
            "global_abstraction": "global_abstraction",
            "transition_after": "transition_after",
            "after_transition": "transition_after",
            "conclusion_sentence": "final_summary",
            "tail_summary": "final_summary",
            "final_summary": "final_summary",
            "solution_conclusion": "solution_conclusion",
            "example_elevation": "example_elevation",
            "example_conclusion": "example_elevation",
        }
        return alias_map.get(raw, raw)

    def _normalize_main_idea_abstraction_level(self, value: Any) -> str:
        if isinstance(value, (int, float)):
            if value <= 1:
                return "low"
            if value >= 3:
                return "high"
            return "medium"
        raw = str(value or "").strip().lower()
        alias_map = {
            "low": "low",
            "detail": "low",
            "local": "low",
            "medium": "medium",
            "mid": "medium",
            "high": "high",
            "global": "high",
            "abstract": "high",
        }
        return alias_map.get(raw, raw)

    def _normalize_main_idea_distractor_types(self, value: Any) -> list[str]:
        if value in (None, ""):
            return []
        raw_items: list[str] = []
        if isinstance(value, str):
            raw_items = [item.strip() for item in re.split(r"[,|/]", value) if item.strip()]
        elif isinstance(value, (list, tuple, set)):
            raw_items = [str(item or "").strip() for item in value if str(item or "").strip()]
        alias_map = {
            "detail": "detail_as_main",
            "detail_as_main": "detail_as_main",
            "example_as_main": "example_as_conclusion",
            "example_as_conclusion": "example_as_conclusion",
            "scope_shift": "scope_too_narrow",
            "scope_too_narrow": "scope_too_narrow",
            "scope_too_wide": "scope_too_wide",
            "subject_shift": "subject_shift",
            "focus_shift": "focus_shift",
        }
        normalized: list[str] = []
        for item in raw_items:
            mapped = alias_map.get(item.lower(), item.lower())
            if mapped and mapped not in normalized:
                normalized.append(mapped)
        return normalized

    def _derive_main_idea_structure_mode(
        self,
        *,
        argument_structure: str,
        main_axis_source: str,
        legacy_structure_type: str = "",
        business_card_id: str = "",
    ) -> str:
        legacy = str(legacy_structure_type or "").strip().lower()
        business_card = str(business_card_id or "").strip().lower()
        # Explicit runtime hints must beat residual legacy/card labels for center-understanding validation.
        if main_axis_source == "transition_after" or argument_structure == "sub_total":
            return "turning"
        if argument_structure in {"phenomenon_analysis", "problem_solution"} or main_axis_source == "solution_conclusion":
            return "cause_effect"
        if (
            argument_structure == "example_conclusion"
            or main_axis_source == "example_elevation"
        ):
            return "example_to_conclusion"
        if main_axis_source == "final_summary" or argument_structure == "total_sub":
            return "final_summary"
        if legacy == "turning":
            return "turning"
        if legacy in {"cause_effect", "progressive"}:
            return "cause_effect"
        if legacy in {"example_to_conclusion", "example_conclusion"}:
            return "example_to_conclusion"
        if legacy in {"final_summary", "explicit_single_center"}:
            return "final_summary"
        if "turning_relation_focus" in business_card:
            return "turning"
        if "cause_effect_conclusion_focus" in business_card:
            return "cause_effect"
        if "case_to_theme" in business_card or "example" in business_card:
            return "example_to_conclusion"
        return ""

    def _extract_main_idea_constraints(self, context: dict[str, Any]) -> dict[str, Any]:
        validator_contract = context.get("validator_contract") or {}
        material_source = context.get("material_source") or {}
        resolved_slots = context.get("resolved_slots") or {}
        control_logic = context.get("control_logic") or {}

        prompt_extras = (
            material_source.get("prompt_extras")
            if isinstance(material_source, dict) and isinstance(material_source.get("prompt_extras"), dict)
            else {}
        )
        type_slots = (
            (material_source.get("slot_projection") or {}).get("type_slots")
            if isinstance(material_source, dict) and isinstance(material_source.get("slot_projection"), dict)
            else {}
        )
        resolved_structure = (
            resolved_slots.get("structure_schema")
            if isinstance(resolved_slots, dict) and isinstance(resolved_slots.get("structure_schema"), dict)
            else {}
        )
        control_main_idea = (
            control_logic.get("main_idea")
            if isinstance(control_logic, dict) and isinstance(control_logic.get("main_idea"), dict)
            else {}
        )
        control_center = (
            control_logic.get("center_understanding")
            if isinstance(control_logic, dict) and isinstance(control_logic.get("center_understanding"), dict)
            else {}
        )
        center_contract = (
            validator_contract.get("center_understanding")
            if isinstance(validator_contract, dict) and isinstance(validator_contract.get("center_understanding"), dict)
            else {}
        )
        structure_contract = (
            validator_contract.get("structure_schema")
            if isinstance(validator_contract, dict) and isinstance(validator_contract.get("structure_schema"), dict)
            else {}
        )
        main_idea_contract = (
            validator_contract.get("main_idea")
            if isinstance(validator_contract, dict) and isinstance(validator_contract.get("main_idea"), dict)
            else {}
        )

        runtime_sources = [
            (prompt_extras, "material_source.prompt_extras"),
            (type_slots, "material_source.slot_projection.type_slots"),
            (resolved_slots, "resolved_slots"),
            (resolved_structure, "resolved_slots.structure_schema"),
            (control_center, "control_logic.center_understanding"),
            (control_main_idea, "control_logic.main_idea"),
            (control_logic if isinstance(control_logic, dict) else {}, "control_logic"),
        ]
        contract_sources = [
            (center_contract, "validator_contract.center_understanding"),
            (structure_contract, "validator_contract.structure_schema"),
            (main_idea_contract, "validator_contract.main_idea"),
            (validator_contract if isinstance(validator_contract, dict) else {}, "validator_contract"),
        ]

        def read_field(sources: list[tuple[dict[str, Any], str]], aliases: tuple[str, ...]) -> tuple[Any, str]:
            for payload, source in sources:
                if not isinstance(payload, dict):
                    continue
                for alias in aliases:
                    candidate = payload.get(alias)
                    if candidate not in (None, "", []):
                        return candidate, f"{source}.{alias}"
            return None, "compatibility_disabled"

        runtime_argument_raw, runtime_argument_source = read_field(runtime_sources, ("argument_structure", "structure_type"))
        runtime_axis_raw, runtime_axis_source = read_field(runtime_sources, ("main_axis_source", "main_point_source"))
        runtime_abstraction_raw, runtime_abstraction_source = read_field(runtime_sources, ("abstraction_level",))
        runtime_distractors_raw, runtime_distractor_source = read_field(runtime_sources, ("distractor_types", "distractor_modes"))
        contract_argument_raw, contract_argument_source = read_field(contract_sources, ("argument_structure", "structure_type"))
        contract_axis_raw, contract_axis_source = read_field(contract_sources, ("main_axis_source", "main_point_source"))
        contract_abstraction_raw, contract_abstraction_source = read_field(contract_sources, ("abstraction_level",))
        contract_distractors_raw, contract_distractor_source = read_field(contract_sources, ("distractor_types", "distractor_modes"))
        runtime_structure_type, _ = read_field(runtime_sources, ("structure_type",))
        contract_structure_type, _ = read_field(contract_sources, ("structure_type",))

        business_card_id = str(prompt_extras.get("business_feature_card_id") or "")
        runtime_argument = self._normalize_main_idea_argument_structure(runtime_argument_raw)
        contract_argument = self._normalize_main_idea_argument_structure(contract_argument_raw)
        runtime_axis = self._normalize_main_idea_axis_source(runtime_axis_raw)
        contract_axis = self._normalize_main_idea_axis_source(contract_axis_raw)
        runtime_abstraction = self._normalize_main_idea_abstraction_level(runtime_abstraction_raw)
        contract_abstraction = self._normalize_main_idea_abstraction_level(contract_abstraction_raw)

        runtime_structure_mode = self._derive_main_idea_structure_mode(
            argument_structure=runtime_argument,
            main_axis_source=runtime_axis,
            legacy_structure_type=str(runtime_structure_type or ""),
            business_card_id=business_card_id,
        )
        contract_structure_mode = self._derive_main_idea_structure_mode(
            argument_structure=contract_argument,
            main_axis_source=contract_axis,
            legacy_structure_type=str(contract_structure_type or ""),
            business_card_id=business_card_id,
        )

        return {
            "runtime_argument_structure": runtime_argument,
            "runtime_argument_structure_source": runtime_argument_source,
            "runtime_main_axis_source": runtime_axis,
            "runtime_main_axis_source_source": runtime_axis_source,
            "runtime_abstraction_level": runtime_abstraction,
            "runtime_abstraction_level_source": runtime_abstraction_source,
            "runtime_distractor_types": self._normalize_main_idea_distractor_types(runtime_distractors_raw),
            "runtime_distractor_types_source": runtime_distractor_source,
            "runtime_structure_mode": runtime_structure_mode,
            "contract_argument_structure": contract_argument,
            "contract_argument_structure_source": contract_argument_source,
            "contract_main_axis_source": contract_axis,
            "contract_main_axis_source_source": contract_axis_source,
            "contract_abstraction_level": contract_abstraction,
            "contract_abstraction_level_source": contract_abstraction_source,
            "contract_distractor_types": self._normalize_main_idea_distractor_types(contract_distractors_raw),
            "contract_distractor_types_source": contract_distractor_source,
            "contract_structure_mode": contract_structure_mode,
            "effective_argument_structure": runtime_argument or contract_argument,
            "effective_main_axis_source": runtime_axis or contract_axis,
            "effective_abstraction_level": runtime_abstraction or contract_abstraction,
            "effective_distractor_types": self._normalize_main_idea_distractor_types(runtime_distractors_raw)
            or self._normalize_main_idea_distractor_types(contract_distractors_raw),
            "effective_structure_mode": runtime_structure_mode or contract_structure_mode,
            "business_card_id": business_card_id,
        }

    def _main_idea_extract_marker_clause(self, text: str, markers: tuple[str, ...], *, after: bool) -> str:
        candidate = (text or "").strip()
        if not candidate:
            return ""
        best_index = -1
        best_marker = ""
        for marker in markers:
            idx = candidate.rfind(marker)
            if idx > best_index:
                best_index = idx
                best_marker = marker
        if best_index < 0:
            return candidate
        if after:
            extracted = candidate[best_index + len(best_marker) :].strip(" 锛?锛?")
        else:
            extracted = candidate[:best_index].strip(" 锛?锛?")
        return extracted or candidate

    def _main_idea_has_example_marker(self, text: str) -> bool:
        candidate = (text or "").strip()
        markers = ("??", "??", "??", "????", "??", "???")
        return any(token in candidate for token in markers)

    def _main_idea_has_summary_marker(self, text: str) -> bool:
        candidate = (text or "").strip()
        markers = ("??", "??", "??", "??", "??", "????", "????")
        return any(token in candidate for token in markers)

    def _build_main_idea_structure_profile(self, *, material_text: str, structure_mode: str) -> dict[str, Any]:
        units = self._split_material_units(material_text)
        if not units:
            return {
                "detected": False,
                "mode": structure_mode,
                "units": [],
                "axis_text": "",
                "axis_unit_index": -1,
                "background_text": "",
                "example_text": "",
            }

        turning_markers = ("??", "??", "??", "?", "?", "???")
        conclusion_markers = ("??", "??", "??", "??", "??", "??")
        profile = {
            "detected": False,
            "mode": structure_mode,
            "units": units,
            "axis_text": units[-1],
            "axis_unit_index": len(units) - 1,
            "background_text": " ".join(units[:-1]).strip(),
            "example_text": "",
        }

        if structure_mode == "turning":
            for index in range(len(units) - 1, -1, -1):
                unit = units[index]
                if any(marker in unit for marker in turning_markers):
                    profile["detected"] = True
                    profile["axis_unit_index"] = index
                    profile["axis_text"] = self._main_idea_extract_marker_clause(unit, turning_markers, after=True)
                    prefix = self._main_idea_extract_marker_clause(unit, turning_markers, after=False)
                    background_parts = units[:index]
                    if prefix and prefix != unit:
                        background_parts.append(prefix)
                    profile["background_text"] = " ".join(part for part in background_parts if part).strip()
                    return profile
            return profile

        if structure_mode == "cause_effect":
            for index in range(len(units) - 1, -1, -1):
                unit = units[index]
                if any(marker in unit for marker in conclusion_markers):
                    profile["detected"] = True
                    profile["axis_unit_index"] = index
                    profile["axis_text"] = self._main_idea_extract_marker_clause(unit, conclusion_markers, after=True)
                    profile["background_text"] = " ".join(units[:index]).strip()
                    return profile
            profile["detected"] = len(units) >= 2
            return profile

        if structure_mode == "example_to_conclusion":
            example_units = [unit for unit in units[:-1] if self._main_idea_has_example_marker(unit)]
            for index in range(len(units) - 1, -1, -1):
                unit = units[index]
                if index > 0 and (self._main_idea_has_summary_marker(unit) or any(token in unit for token in ("????", "???????", "?????"))):
                    profile["axis_unit_index"] = index
                    profile["axis_text"] = self._main_idea_extract_marker_clause(unit, conclusion_markers + ("?????",), after=True)
                    break
            profile["background_text"] = " ".join(units[: profile["axis_unit_index"]]).strip()
            profile["example_text"] = " ".join(example_units or units[: max(profile["axis_unit_index"], 1)]).strip()
            profile["detected"] = bool(profile["example_text"] and profile["axis_unit_index"] > 0)
            return profile

        if structure_mode == "final_summary":
            profile["axis_text"] = units[-1]
            profile["axis_unit_index"] = len(units) - 1
            profile["background_text"] = " ".join(units[:-1]).strip()
            profile["detected"] = len(units) >= 2 and self._main_idea_has_summary_marker(units[-1])
            return profile

        return profile

    def _profile_best_support(self, *, units: list[str], candidate_text: str) -> tuple[int, dict[str, Any]]:
        best_index = -1
        best_profile = {"shared_token_count": 0, "candidate_token_count": 0, "supported_token_ratio": 0.0}
        for index, unit in enumerate(units):
            support = self._compute_support_profile(evidence_text=unit, candidate_text=candidate_text)
            if (
                support["shared_token_count"] > best_profile["shared_token_count"]
                or (
                    support["shared_token_count"] == best_profile["shared_token_count"]
                    and support["supported_token_ratio"] > best_profile["supported_token_ratio"]
                )
            ):
                best_index = index
                best_profile = support
        return best_index, best_profile

    def _main_idea_option_profile(
        self,
        *,
        option_text: str,
        material_text: str,
        structure_profile: dict[str, Any],
    ) -> dict[str, Any]:
        units = structure_profile.get("units") or []
        axis_text = str(structure_profile.get("axis_text") or "")
        background_text = str(structure_profile.get("background_text") or "")
        example_text = str(structure_profile.get("example_text") or "")
        axis_support = self._compute_support_profile(evidence_text=axis_text, candidate_text=option_text)
        material_support = self._compute_support_profile(evidence_text=material_text, candidate_text=option_text)
        background_support = self._compute_support_profile(evidence_text=background_text, candidate_text=option_text)
        example_support = self._compute_support_profile(evidence_text=example_text, candidate_text=option_text)
        best_index, best_unit_support = self._profile_best_support(units=units, candidate_text=option_text)
        option_tokens = self._extract_tokens(option_text)
        generic_markers = ("??", "??", "??", "??", "??", "??", "???")
        detail_markers = ("??", "??", "??", "??", "??", "??", "??", "??")
        generic_like = any(token in option_text for token in generic_markers)
        detail_like = any(token in option_text for token in detail_markers) or self._main_idea_has_example_marker(option_text)
        local_dominant = (
            best_index >= 0
            and best_index != structure_profile.get("axis_unit_index")
            and best_unit_support["shared_token_count"] >= max(2, axis_support["shared_token_count"])
            and best_unit_support["supported_token_ratio"] >= axis_support["supported_token_ratio"] + 0.08
        )
        low_abstraction = detail_like or local_dominant
        over_abstract = bool(
            generic_like
            and material_support["shared_token_count"] < 2
            and material_support["supported_token_ratio"] < 0.18
            and len(option_tokens) >= 3
        )
        axis_aligned = bool(
            axis_support["shared_token_count"] >= 2
            or axis_support["supported_token_ratio"] >= 0.18
        )
        return {
            "axis_support": axis_support,
            "material_support": material_support,
            "background_support": background_support,
            "example_support": example_support,
            "best_unit_index": best_index,
            "best_unit_support": best_unit_support,
            "low_abstraction": low_abstraction,
            "over_abstract": over_abstract,
            "axis_aligned": axis_aligned,
            "local_dominant": local_dominant,
            "example_dominant": example_support["shared_token_count"] >= max(2, axis_support["shared_token_count"])
            and example_support["supported_token_ratio"] >= axis_support["supported_token_ratio"],
        }

    def validate(
        self,
        *,
        question_type: str,
        business_subtype: str | None = None,
        generated_question: GeneratedQuestion | None,
        material_text: str | None,
        original_material_text: str | None = None,
        material_source: dict[str, Any] | None = None,
        validator_contract: dict[str, Any] | None = None,
        difficulty_fit: dict[str, Any] | Any | None = None,
        source_question: dict[str, Any] | None = None,
        source_question_analysis: dict[str, Any] | None = None,
        resolved_slots: dict[str, Any] | None = None,
        control_logic: dict[str, Any] | None = None,
    ) -> ValidationResult:
        if generated_question is None:
            return ValidationResult(
                validation_status="failed",
                passed=False,
                score=0,
                errors=["Structured question output was not produced."],
                warnings=[],
                checks={"parse": {"passed": False}},
                next_review_status="needs_revision",
            )

        if hasattr(difficulty_fit, "model_dump"):
            difficulty_fit = difficulty_fit.model_dump()

        context = {
            "question_type": question_type,
            "material_text": material_text or "",
            "original_material_text": original_material_text or "",
            "material_source": material_source or {},
            "validator_contract": validator_contract or {},
            "business_subtype": business_subtype,
            "source_question": source_question or {},
            "source_question_analysis": source_question_analysis or {},
            "resolved_slots": resolved_slots or {},
            "control_logic": control_logic or {},
        }

        errors, warnings, checks = self._validate_common(generated_question, material_text or "", context)
        validator = self.registry.get(question_type)
        if validator is not None:
            type_errors, type_warnings, type_checks = validator(generated_question, context)
            errors.extend(type_errors)
            warnings.extend(type_warnings)
            checks.update(type_checks)

        difficulty_review = self._build_difficulty_review(difficulty_fit or {})
        if difficulty_review and not difficulty_review.get("in_range", True):
            if question_type == "sentence_fill" and self._is_sentence_fill_soft_difficulty_miss(difficulty_review):
                warnings.append("sentence_fill difficulty projection is slightly below the target profile range.")
            elif (
                question_type == "main_idea"
                and str(business_subtype or "").strip() == "center_understanding"
                and self._is_center_understanding_soft_difficulty_miss(difficulty_review)
            ):
                warnings.append("center_understanding difficulty projection is slightly below the target profile range.")
            elif (
                question_type == "sentence_order"
                and self._is_sentence_order_soft_difficulty_miss(
                    difficulty_review=difficulty_review,
                    checks=checks,
                )
            ):
                warnings.append(
                    "sentence_order difficulty projection is slightly outside the target profile range, "
                    "but structural ordering signals remain within the accepted band."
                )
            else:
                warnings.append("difficulty projection is outside the target profile range.")

        errors, warnings = self._apply_minimum_compliance_profile(
            question_type=question_type,
            errors=errors,
            warnings=warnings,
            checks=checks,
        )

        fatal = bool(errors)
        score = max(0, 100 - len(errors) * 25 - len(warnings) * 5)
        return ValidationResult(
            validation_status="failed" if fatal else "passed",
            passed=not fatal,
            score=score,
            errors=errors,
            warnings=warnings,
            checks=checks,
            difficulty_review=difficulty_review or None,
            next_review_status="needs_revision" if fatal else "waiting_review",
        )

    def _apply_minimum_compliance_profile(
        self,
        *,
        question_type: str,
        errors: list[str],
        warnings: list[str],
        checks: dict[str, Any],
    ) -> tuple[list[str], list[str]]:
        hard_errors: list[str] = []
        soft_errors: list[str] = []
        for error in errors:
            if self._is_minimum_compliance_hard_error(
                question_type=question_type,
                error=error,
                checks=checks,
            ):
                hard_errors.append(error)
            else:
                soft_errors.append(error)

        merged_warnings = list(warnings)
        for error in soft_errors:
            advisory = f"soft_validation::{error}"
            if advisory not in merged_warnings:
                merged_warnings.append(advisory)
        return hard_errors, merged_warnings

    def _is_minimum_compliance_hard_error(
        self,
        *,
        question_type: str,
        error: str,
        checks: dict[str, Any],
    ) -> bool:
        normalized = str(error or "").strip()
        if not normalized:
            return False
        if question_type == "sentence_order" and normalized == "ordering_chain_incomplete":
            hard_sentence_order_checks = (
                "sentence_order_material_unit_count",
                "sentence_order_option_unit_counts",
                "sentence_order_correct_order",
                "sentence_order_single_truth_option",
                "sentence_order_answer_binding",
                "sentence_order_analysis_binding",
                "sentence_order_reference_unit_alignment",
                "sentence_order_original_sentences",
            )
            for key in hard_sentence_order_checks:
                check = checks.get(key)
                if isinstance(check, dict) and check.get("passed") is False:
                    return True
            return False
        if normalized in self.HARD_ERROR_CODES:
            return True
        if any(normalized.startswith(prefix) for prefix in self.HARD_ERROR_PREFIXES):
            return True
        if question_type == "sentence_fill" and normalized == "position_function_mismatch":
            anchor_check = checks.get("sentence_fill_anchor_grounding") or {}
            return bool(anchor_check) and anchor_check.get("require_original_answer_sentence") is True and anchor_check.get("passed") is False
        return False

    def _validate_common(
        self,
        generated_question: GeneratedQuestion,
        material_text: str,
        context: dict[str, Any],
    ) -> tuple[list[str], list[str], dict[str, Any]]:
        errors: list[str] = []
        warnings: list[str] = []
        checks: dict[str, Any] = {}

        stem = (generated_question.stem or "").strip()
        analysis = (generated_question.analysis or "").strip()
        answer = (generated_question.answer or "").strip().upper()
        options = {key: (value or "").strip() for key, value in (generated_question.options or {}).items()}

        checks["stem_present"] = {"passed": bool(stem)}
        if not stem:
            errors.append("stem must not be empty.")

        checks["analysis_present"] = {"passed": bool(analysis)}
        if not analysis:
            errors.append("analysis must not be empty.")

        checks["options_count"] = {"passed": 2 <= len(options) <= 6, "count": len(options)}
        if len(options) < 2 or len(options) > 6:
            errors.append("options count must be between 2 and 6.")

        option_keys = list(options.keys())
        standard_keys = [chr(ord("A") + idx) for idx in range(len(options))]
        checks["option_keys_standard"] = {"passed": option_keys == standard_keys, "keys": option_keys}
        if option_keys != standard_keys:
            warnings.append("options keys do not follow standard sequential A/B/C/D style.")

        empty_options = [key for key, value in options.items() if not value]
        checks["options_non_empty"] = {"passed": not empty_options, "empty_options": empty_options}
        if empty_options:
            errors.append(f"options must not be empty: {', '.join(empty_options)}.")

        checks["answer_in_options"] = {"passed": bool(answer and answer in options)}
        if not answer:
            errors.append("answer must not be empty.")
        elif answer not in options:
            errors.append("answer must be one of the options.")

        unique_option_text = {value for value in options.values() if value}
        checks["options_not_all_duplicate"] = {"passed": len(unique_option_text) > 1}
        if options and len(unique_option_text) <= 1:
            errors.append("options text must not all be identical.")

        option_lengths = {key: len(value) for key, value in options.items()}
        if option_lengths:
            min_len = min(option_lengths.values())
            max_len = max(option_lengths.values())
            balanced = not (min_len > 0 and max_len >= min_len * 3 and max_len - min_len >= 12)
            checks["option_length_balance"] = {
                "passed": balanced,
                "min_length": min_len,
                "max_length": max_len,
            }
            if not balanced:
                warnings.append("option lengths are imbalanced and may reveal the answer too easily.")

        lower_analysis = analysis.lower()
        answer_mentioned = answer.lower() in lower_analysis or "姝ｇ‘" in analysis
        checks["analysis_mentions_answer"] = {"passed": answer_mentioned}
        if analysis and not answer_mentioned:
            warnings.append("analysis does not clearly mention the correct answer.")

        explicit_answer_letters = self._extract_explicit_answer_letters(analysis)
        checks["analysis_answer_consistency"] = {
            "passed": True,
            "explicit_letters": explicit_answer_letters,
            "declared_answer": answer,
        }
        if len(explicit_answer_letters) > 1:
            checks["analysis_answer_consistency"]["passed"] = False
            errors.append("analysis explicitly marks multiple options as correct, which conflicts with single-answer requirements.")
        elif explicit_answer_letters:
            explicit_answer = explicit_answer_letters[0]
            if answer and explicit_answer != answer:
                checks["analysis_answer_consistency"]["passed"] = False
                errors.append(f"analysis explicitly marks option {explicit_answer} as correct but answer is {answer}.")

        correct_option_text = options.get(answer, "")
        answer_basis_present = bool(
            correct_option_text
            and any(
                fragment and fragment in analysis
                for fragment in (
                    correct_option_text[: min(8, len(correct_option_text))],
                    correct_option_text[-min(6, len(correct_option_text)) :],
                )
            )
        )
        checks["analysis_mentions_correct_option_text"] = {"passed": answer_basis_present}
        if correct_option_text and not answer_basis_present:
            warnings.append("analysis does not clearly explain why the correct option text fits.")

        meta_tone_markers = [
            "ai??",
            "chatgpt",
            "????",
            "????",
            "????",
            "????",
            "??????",
        ]
        lower_stem = stem.lower()
        meta_tone_found = [marker for marker in meta_tone_markers if marker in lower_stem or marker in lower_analysis]
        checks["exam_style_tone"] = {"passed": not meta_tone_found, "markers": meta_tone_found}
        if meta_tone_found:
            warnings.append("question wording contains meta or AI-style phrasing rather than exam-style phrasing.")

        checks["stem_length"] = {"length": len(stem), "passed": True}
        if stem and len(stem) < 8:
            warnings.append("stem is unusually short.")
        if len(stem) > 220:
            warnings.append("stem is unusually long.")

        checks["analysis_length"] = {"length": len(analysis), "passed": True}
        if analysis and len(analysis) < 8:
            warnings.append("analysis is unusually short.")

        overlap_ratio = self._compute_material_overlap_ratio(
            material_text=material_text,
            question_text=" ".join([stem, analysis, correct_option_text]),
        )
        checks["material_overlap"] = {"ratio": overlap_ratio, "passed": overlap_ratio >= 0.03}
        if material_text and overlap_ratio < 0.03:
            warnings.append("question wording shows weak lexical overlap with the selected material.")

        repeated_units = self._find_repeated_material_units(material_text)
        checks["material_no_repeated_units"] = {"passed": not repeated_units, "repeated_units": repeated_units[:3]}
        if repeated_units:
            errors.append("selected material contains repeated sentence-level fragments and is not readable enough.")

        stitched_pairs = self._find_stitched_material_pairs(material_text)
        checks["material_no_stitched_pairs"] = {"passed": not stitched_pairs, "stitched_pairs": stitched_pairs[:2]}
        if stitched_pairs:
            errors.append("selected material shows obvious stitched or overlapping fragments and is not readable enough.")

        source_question = context.get("source_question") or {}
        evidence_text = str(context.get("original_material_text") or material_text or "")
        question_type = str(context.get("question_type") or "")
        material_source = context.get("material_source") or {}
        prompt_extras = (material_source.get("prompt_extras") or {}) if isinstance(material_source, dict) else {}
        answer_anchor_text = str(prompt_extras.get("answer_anchor_text") or "").strip()
        checks["reference_template_present"] = {"passed": bool(source_question)}
        if source_question:
            correct_support = self._compute_support_profile(
                evidence_text=evidence_text,
                candidate_text=correct_option_text,
            )
            analysis_support = self._compute_support_profile(
                evidence_text=evidence_text,
                candidate_text=analysis,
            )
            answer_grounded = True
            if question_type not in {"sentence_order"}:
                answer_grounded = correct_support["supported_token_ratio"] >= 0.12 or correct_support["shared_token_count"] >= 2
            checks["reference_answer_grounding"] = {
                "passed": answer_grounded,
                **correct_support,
            }
            checks["analysis_material_grounding"] = {
                "passed": analysis_support["supported_token_ratio"] >= 0.08 or analysis_support["shared_token_count"] >= 3,
                **analysis_support,
            }
            if question_type not in {"sentence_order"} and correct_option_text and not checks["reference_answer_grounding"]["passed"]:
                warnings.append("correct option looks weakly grounded in the original material evidence.")
            if analysis and not checks["analysis_material_grounding"]["passed"]:
                warnings.append("analysis looks weakly grounded in the original material evidence.")
        if question_type == "sentence_fill" and answer_anchor_text:
            require_original_answer_sentence = bool(prompt_extras.get("require_original_answer_sentence"))
            anchor_support = self._compute_support_profile(
                evidence_text=answer_anchor_text,
                candidate_text=correct_option_text,
            )
            normalized_anchor = self._normalize_sentence_fill_anchor_text(answer_anchor_text)
            normalized_correct = self._normalize_sentence_fill_anchor_text(correct_option_text)
            exact_anchor_match = bool(normalized_anchor and normalized_correct and normalized_anchor == normalized_correct)
            anchor_grounded = anchor_support["supported_token_ratio"] >= 0.2 or anchor_support["shared_token_count"] >= 2
            passed = exact_anchor_match if require_original_answer_sentence else anchor_grounded
            checks["sentence_fill_anchor_grounding"] = {
                "passed": passed,
                "anchor_text": answer_anchor_text,
                "require_original_answer_sentence": require_original_answer_sentence,
                "exact_anchor_match": exact_anchor_match,
                "normalized_anchor_text": normalized_anchor,
                "normalized_correct_option_text": normalized_correct,
                **anchor_support,
            }
            if correct_option_text and not checks["sentence_fill_anchor_grounding"]["passed"]:
                if require_original_answer_sentence:
                    errors.append("sentence_fill correct option must be the original removed sentence.")
                else:
                    errors.append("sentence_fill correct option is not sufficiently grounded in the removed source span.")
        source_alignment_errors, source_alignment_warnings, source_alignment_checks = self._validate_source_alignment(
            generated_question=generated_question,
            source_question=source_question if isinstance(source_question, dict) else {},
            material_text=material_text,
            validator_contract=context.get("validator_contract") if isinstance(context.get("validator_contract"), dict) else {},
        )
        errors.extend(source_alignment_errors)
        warnings.extend(source_alignment_warnings)
        checks.update(source_alignment_checks)
        return errors, warnings, checks

    def _normalize_alignment_text(self, text: str) -> str:
        normalized = unicodedata.normalize("NFKC", str(text or ""))
        normalized = normalized.replace("\u3000", " ").strip().lower()
        normalized = re.sub(r"\s+", "", normalized)
        return normalized

    def _text_similarity(self, left: str, right: str) -> float:
        a = self._normalize_alignment_text(left)
        b = self._normalize_alignment_text(right)
        if not a and not b:
            return 1.0
        if not a or not b:
            return 0.0
        return round(SequenceMatcher(None, a, b).ratio(), 4)

    def _resolve_source_correct_option_text(self, source_question: dict[str, Any]) -> str:
        answer = str(source_question.get("answer") or "").strip().upper()
        options = source_question.get("options") or {}
        if not isinstance(options, dict):
            return ""
        return str(options.get(answer) or "").strip()

    def _validate_source_alignment(
        self,
        *,
        generated_question: GeneratedQuestion,
        source_question: dict[str, Any],
        material_text: str,
        validator_contract: dict[str, Any] | None = None,
    ) -> tuple[list[str], list[str], dict[str, Any]]:
        errors: list[str] = []
        warnings: list[str] = []
        checks: dict[str, Any] = {}

        source_passage = str(source_question.get("passage") or "").strip()
        source_stem = str(source_question.get("stem") or "").strip()
        source_answer = str(source_question.get("answer") or "").strip().upper()
        source_correct_option = self._resolve_source_correct_option_text(source_question)

        has_source_alignment_reference = bool(source_passage or source_stem or source_answer or source_correct_option)
        checks["source_alignment_reference_present"] = {
            "passed": has_source_alignment_reference,
            "source_question_fields": {
                "passage": bool(source_passage),
                "stem": bool(source_stem),
                "answer": bool(source_answer),
                "correct_option_text": bool(source_correct_option),
            },
        }
        if not has_source_alignment_reference:
            return errors, warnings, checks

        contract = validator_contract or {}
        source_alignment_contract = (
            contract.get("source_alignment") if isinstance(contract.get("source_alignment"), dict) else {}
        )
        enforce_answer_match = bool(source_alignment_contract.get("enforce_answer_match"))
        enforce_stem_similarity = bool(source_alignment_contract.get("enforce_stem_similarity"))
        enforce_correct_option_similarity = bool(source_alignment_contract.get("enforce_correct_option_similarity"))
        hard_fail_on_low_overall = bool(source_alignment_contract.get("hard_fail_on_low_overall"))
        min_stem_similarity = self._coerce_float(source_alignment_contract.get("min_stem_similarity")) or 0.45
        min_correct_option_similarity = (
            self._coerce_float(source_alignment_contract.get("min_correct_option_similarity")) or 0.25
        )
        min_material_similarity = self._coerce_float(source_alignment_contract.get("min_material_similarity")) or 0.20
        min_overall_alignment = self._coerce_float(source_alignment_contract.get("min_overall_alignment")) or 0.45

        generated_answer = str(generated_question.answer or "").strip().upper()
        generated_options = generated_question.options or {}
        if not isinstance(generated_options, dict):
            generated_options = {}
        generated_correct_option = str(generated_options.get(generated_answer) or "").strip()

        answer_match = bool(source_answer and generated_answer and source_answer == generated_answer)
        stem_similarity = self._text_similarity(source_stem, generated_question.stem or "")
        correct_option_similarity = self._text_similarity(source_correct_option, generated_correct_option)
        material_similarity = self._text_similarity(source_passage, material_text or "")
        overall_alignment = round(
            0.20 * (1.0 if answer_match else 0.0)
            + 0.25 * stem_similarity
            + 0.30 * correct_option_similarity
            + 0.25 * material_similarity,
            4,
        )

        checks["source_alignment_metrics"] = {
            "passed": overall_alignment >= min_overall_alignment,
            "answer_match": answer_match,
            "stem_similarity": stem_similarity,
            "correct_option_similarity": correct_option_similarity,
            "material_similarity": material_similarity,
            "overall_alignment": overall_alignment,
            "thresholds": {
                "min_stem_similarity": min_stem_similarity,
                "min_correct_option_similarity": min_correct_option_similarity,
                "min_material_similarity": min_material_similarity,
                "min_overall_alignment": min_overall_alignment,
            },
            "enforcement": {
                "enforce_answer_match": enforce_answer_match,
                "enforce_stem_similarity": enforce_stem_similarity,
                "enforce_correct_option_similarity": enforce_correct_option_similarity,
                "hard_fail_on_low_overall": hard_fail_on_low_overall,
            },
        }

        if not answer_match:
            if enforce_answer_match:
                self._append_unique_error(errors, "source_alignment_answer_mismatch")
            else:
                warnings.append("source alignment: generated answer letter differs from source answer.")
        if stem_similarity < min_stem_similarity:
            if enforce_stem_similarity:
                self._append_unique_error(errors, "source_alignment_stem_similarity_low")
            else:
                warnings.append("source alignment: stem similarity to source question is below the target band.")
        if correct_option_similarity < min_correct_option_similarity:
            if enforce_correct_option_similarity:
                self._append_unique_error(errors, "source_alignment_correct_option_similarity_low")
            else:
                warnings.append("source alignment: correct-option text similarity to source question is low.")
        if material_similarity < min_material_similarity:
            warnings.append("source alignment: material similarity to source passage is lower than expected.")
        if overall_alignment < min_overall_alignment and hard_fail_on_low_overall:
            self._append_unique_error(errors, "source_alignment_overall_low")

        return errors, warnings, checks

    def _validate_main_idea(
        self,
        generated_question: GeneratedQuestion,
        context: dict[str, Any],
    ) -> tuple[list[str], list[str], dict[str, Any]]:
        stem = generated_question.stem.strip()
        prompt_markers = ("????????????", "????????", "????????", "??????????", "????????")
        exam_style = any(marker in stem for marker in prompt_markers)
        business_subtype = context.get("business_subtype")
        material_text = context.get("material_text") or ""
        checks = {
            "main_idea_single_answer": {"passed": bool(generated_question.answer)},
            "main_idea_exam_style_prompt": {"passed": exam_style},
        }
        warnings = [] if exam_style else ["main_idea stem does not look like a standard exam-style summary/title prompt."]
        errors: list[str] = []

        is_title_selection = business_subtype == "title_selection" or "鏍囬" in stem
        if is_title_selection:
            options = generated_question.options or {}
            validator_contract = context.get("validator_contract") or {}
            title_selection_contract = validator_contract.get("title_selection") if isinstance(validator_contract, dict) else None
            material_constraints_contract = validator_contract.get("material_constraints") if isinstance(validator_contract, dict) else None
            correct_text = (options.get(generated_question.answer or "", "") or "").strip()
            avg_len = round(sum(len((value or "").strip()) for value in options.values()) / max(len(options), 1), 2) if options else 0
            long_sentence_like = bool(correct_text and (len(correct_text) >= 24 or "?" in correct_text or correct_text.count("?") >= 3))
            meeting_markers = ["??", "??", "??", "??", "???"]
            marker_hits = [marker for marker in meeting_markers if marker in material_text]
            fragment_heavy = bool(
                options
                and all(
                    len((value or "").strip()) >= 14
                    or any(token in (value or "") for token in ("?", "?", "?", "?"))
                    for value in options.values()
                )
            )
            contract_enforce_material_fit = None
            contract_enforce_title_like = None
            contract_enforce_option_diversity = None
            if isinstance(validator_contract, dict):
                raw_enforce_material_fit = (
                    validator_contract.get("enforce_material_fit")
                    or (title_selection_contract.get("enforce_material_fit") if isinstance(title_selection_contract, dict) else None)
                    or (material_constraints_contract.get("enforce_material_fit") if isinstance(material_constraints_contract, dict) else None)
                )
                if raw_enforce_material_fit is not None:
                    if isinstance(raw_enforce_material_fit, bool):
                        contract_enforce_material_fit = raw_enforce_material_fit
                    elif isinstance(raw_enforce_material_fit, str):
                        normalized_flag = raw_enforce_material_fit.strip().lower()
                        if normalized_flag in {"true", "1", "yes", "on"}:
                            contract_enforce_material_fit = True
                        elif normalized_flag in {"false", "0", "no", "off"}:
                            contract_enforce_material_fit = False
                raw_enforce_title_like = (
                    validator_contract.get("enforce_title_like")
                    or (title_selection_contract.get("enforce_title_like") if isinstance(title_selection_contract, dict) else None)
                    or (
                        validator_contract.get("title_constraints", {}).get("enforce_title_like")
                        if isinstance(validator_contract.get("title_constraints"), dict)
                        else None
                    )
                )
                if raw_enforce_title_like is not None:
                    if isinstance(raw_enforce_title_like, bool):
                        contract_enforce_title_like = raw_enforce_title_like
                    elif isinstance(raw_enforce_title_like, str):
                        normalized_flag = raw_enforce_title_like.strip().lower()
                        if normalized_flag in {"true", "1", "yes", "on"}:
                            contract_enforce_title_like = True
                        elif normalized_flag in {"false", "0", "no", "off"}:
                            contract_enforce_title_like = False
                raw_enforce_option_diversity = (
                    validator_contract.get("enforce_option_diversity")
                    or (title_selection_contract.get("enforce_option_diversity") if isinstance(title_selection_contract, dict) else None)
                    or (
                        validator_contract.get("option_constraints", {}).get("enforce_option_diversity")
                        if isinstance(validator_contract.get("option_constraints"), dict)
                        else None
                    )
                )
                if raw_enforce_option_diversity is not None:
                    if isinstance(raw_enforce_option_diversity, bool):
                        contract_enforce_option_diversity = raw_enforce_option_diversity
                    elif isinstance(raw_enforce_option_diversity, str):
                        normalized_flag = raw_enforce_option_diversity.strip().lower()
                        if normalized_flag in {"true", "1", "yes", "on"}:
                            contract_enforce_option_diversity = True
                        elif normalized_flag in {"false", "0", "no", "off"}:
                            contract_enforce_option_diversity = False
            enforce_material_fit = bool(contract_enforce_material_fit is True)
            material_fit_requirement_source = (
                "validator_contract" if contract_enforce_material_fit is not None else "compatibility_disabled"
            )
            enforce_title_like = bool(contract_enforce_title_like is True)
            title_like_requirement_source = (
                "validator_contract" if contract_enforce_title_like is not None else "compatibility_disabled"
            )
            enforce_option_diversity = bool(contract_enforce_option_diversity is True)
            option_diversity_requirement_source = (
                "validator_contract" if contract_enforce_option_diversity is not None else "compatibility_disabled"
            )
            checks["title_selection_title_like"] = self._build_contract_gated_check(
                active=enforce_title_like,
                passed=not long_sentence_like,
                source=title_like_requirement_source,
                correct_option_length=len(correct_text),
                correct_option_text=correct_text,
            )
            checks["title_selection_material_fit"] = self._build_contract_gated_check(
                active=enforce_material_fit,
                passed=len(marker_hits) < 2,
                source=material_fit_requirement_source,
                marker_hits=marker_hits,
            )
            checks["title_selection_option_diversity"] = self._build_contract_gated_check(
                active=enforce_option_diversity,
                passed=not fragment_heavy,
                source=option_diversity_requirement_source,
                avg_option_length=avg_len,
            )
            if enforce_title_like and long_sentence_like:
                errors.append("title_selection correct option reads like a long summary sentence rather than a title.")
            if enforce_material_fit and len(marker_hits) >= 2:
                errors.append("title_selection material is too close to a meeting-summary or report-style passage and should not be used directly.")
            if enforce_option_diversity and fragment_heavy:
                warnings.append("title_selection options are overly uniform and mostly look like fragment extraction rather than layered title design.")

        validator_contract = context.get("validator_contract") or {}
        title_selection_contract = validator_contract.get("title_selection") if isinstance(validator_contract, dict) else None
        material_constraints_contract = validator_contract.get("material_constraints") if isinstance(validator_contract, dict) else None
        main_idea_contract = (
            validator_contract.get("main_idea")
            if isinstance(validator_contract, dict) and isinstance(validator_contract.get("main_idea"), dict)
            else None
        )
        scoring, scoring_source = self._extract_material_scoring(context)
        task_scoring_available = bool(scoring)
        final_candidate_score = self._coerce_float(scoring.get("final_candidate_score")) or 0.0
        readiness_score = self._coerce_float(scoring.get("readiness_score")) or 0.0
        risk_penalties = scoring.get("risk_penalties") if isinstance(scoring.get("risk_penalties"), dict) else {}
        total_penalty = round(sum(float(value or 0.0) for value in risk_penalties.values()), 4)
        difficulty_vector = scoring.get("difficulty_vector") if isinstance(scoring.get("difficulty_vector"), dict) else {}
        difficulty_band = str(scoring.get("difficulty_band_hint") or "")
        checks["main_idea_material_scoring_available"] = {
            "passed": task_scoring_available,
            "source": scoring_source,
            "task_family": scoring.get("task_family"),
            "recommended": bool(scoring.get("recommended")) if task_scoring_available else None,
            "needs_review": bool(scoring.get("needs_review")) if task_scoring_available else None,
        }
        if not task_scoring_available:
            warnings.append("material scoring payload is missing, so validator could not enforce main_idea scoring controls.")
        else:
            scoring_sources = [
                (title_selection_contract if isinstance(title_selection_contract, dict) else None, "validator_contract.title_selection"),
                (main_idea_contract, "validator_contract.main_idea"),
                (
                    validator_contract.get("center_understanding")
                    if isinstance(validator_contract, dict) and isinstance(validator_contract.get("center_understanding"), dict)
                    else None,
                    "validator_contract.center_understanding",
                ),
                (material_constraints_contract if isinstance(material_constraints_contract, dict) else None, "validator_contract.material_constraints"),
                (validator_contract if isinstance(validator_contract, dict) else None, "validator_contract"),
            ]
            compatibility = self._material_scoring_compatibility_profile(
                task_family="main_idea",
                business_subtype=str(business_subtype or ""),
            )
            min_final_raw, min_final_source = self._resolve_scoring_contract_value(
                sources=scoring_sources,
                field_names=("min_final_candidate_score",),
                compatibility=compatibility,
                compatibility_key="min_final_candidate_score",
            )
            min_readiness_raw, min_readiness_source = self._resolve_scoring_contract_value(
                sources=scoring_sources,
                field_names=("min_readiness_score",),
                compatibility=compatibility,
                compatibility_key="min_readiness_score",
            )
            max_total_penalty_raw, max_total_penalty_source = self._resolve_scoring_contract_value(
                sources=scoring_sources,
                field_names=("max_total_penalty",),
                compatibility=compatibility,
                compatibility_key="max_total_penalty",
            )
            review_signal_raw, review_signal_source = self._resolve_scoring_contract_value(
                sources=scoring_sources,
                field_names=("review_if_high_readiness_high_penalty",),
                compatibility=compatibility,
                compatibility_key="review_if_high_readiness_high_penalty",
            )
            min_reasoning_raw, min_reasoning_source = self._resolve_scoring_contract_value(
                sources=scoring_sources,
                field_names=("min_reasoning_depth_score",),
                compatibility=compatibility,
                compatibility_key="min_reasoning_depth_score",
            )
            max_ambiguity_raw, max_ambiguity_source = self._resolve_scoring_contract_value(
                sources=scoring_sources,
                field_names=("max_ambiguity_score",),
                compatibility=compatibility,
                compatibility_key="max_ambiguity_score",
            )
            difficulty_band_allowed_raw, difficulty_band_allowed_source = self._resolve_scoring_contract_value(
                sources=scoring_sources,
                field_names=("difficulty_band_allowed",),
                compatibility=compatibility,
                compatibility_key="difficulty_band_allowed",
            )
            reasoning_depth_score = self._coerce_float(difficulty_vector.get("reasoning_depth_score")) or 0.0
            ambiguity_score = self._coerce_float(difficulty_vector.get("ambiguity_score")) or 0.0
            self._apply_scoring_threshold_check(
                checks=checks,
                errors=errors,
                warnings=warnings,
                check_name="main_idea_final_candidate_score",
                actual=final_candidate_score,
                threshold=self._coerce_float(min_final_raw),
                source=min_final_source,
                comparator="min",
                reason="min_final_candidate_score",
                error_message="main_idea material final_candidate_score is below the accepted floor.",
                extra={"difficulty_band": difficulty_band, "scoring_source": scoring_source},
            )
            self._apply_scoring_threshold_check(
                checks=checks,
                errors=errors,
                warnings=warnings,
                check_name="main_idea_readiness_score",
                actual=readiness_score,
                threshold=self._coerce_float(min_readiness_raw),
                source=min_readiness_source,
                comparator="min",
                reason="min_readiness_score",
                error_message="main_idea material readiness_score is below the accepted floor.",
                extra={"difficulty_band": difficulty_band, "scoring_source": scoring_source},
            )
            self._apply_scoring_threshold_check(
                checks=checks,
                errors=errors,
                warnings=warnings,
                check_name="main_idea_total_penalty",
                actual=total_penalty,
                threshold=self._coerce_float(max_total_penalty_raw),
                source=max_total_penalty_source,
                comparator="max",
                reason="max_total_penalty",
                error_message="main_idea material total penalty is higher than the allowed range.",
                extra={"difficulty_band": difficulty_band, "risk_penalties": risk_penalties, "scoring_source": scoring_source},
            )
            self._apply_scoring_threshold_check(
                checks=checks,
                errors=errors,
                warnings=warnings,
                check_name="main_idea_reasoning_depth",
                actual=reasoning_depth_score,
                threshold=self._coerce_float(min_reasoning_raw),
                source=min_reasoning_source,
                comparator="min",
                reason="min_reasoning_depth_score",
                error_message="main_idea material reasoning depth is lower than the target requirement.",
                extra={"difficulty_band": difficulty_band, "difficulty_vector": difficulty_vector, "scoring_source": scoring_source},
            )
            self._apply_scoring_threshold_check(
                checks=checks,
                errors=errors,
                warnings=warnings,
                check_name="main_idea_ambiguity",
                actual=ambiguity_score,
                threshold=self._coerce_float(max_ambiguity_raw),
                source=max_ambiguity_source,
                comparator="max",
                reason="max_ambiguity_score",
                error_message="main_idea material ambiguity is higher than the accepted range.",
                extra={"difficulty_band": difficulty_band, "difficulty_vector": difficulty_vector, "scoring_source": scoring_source},
            )
            self._apply_scoring_band_check(
                checks=checks,
                errors=errors,
                warnings=warnings,
                check_name="main_idea_difficulty_band",
                difficulty_band=difficulty_band,
                allowed_bands=self._normalize_band_allowed(difficulty_band_allowed_raw),
                source=difficulty_band_allowed_source,
                error_message="main_idea material difficulty band is outside the allowed range.",
            )
            review_signal_active = bool(
                review_signal_raw is True
                or str(review_signal_raw).strip().lower() in {"true", "1", "yes", "on"}
            )
            review_penalty_threshold = self._coerce_float(max_total_penalty_raw)
            if review_penalty_threshold is None:
                review_penalty_threshold = 0.40
            review_readiness_threshold = self._coerce_float(min_readiness_raw)
            if review_readiness_threshold is None:
                review_readiness_threshold = 0.45
            review_triggered = bool(
                review_signal_active
                and readiness_score >= review_readiness_threshold
                and total_penalty >= review_penalty_threshold
            )
            checks["main_idea_review_like_risk"] = self._build_contract_gated_check(
                active=review_signal_active,
                passed=not review_triggered,
                source=review_signal_source,
                actual={
                    "readiness_score": round(readiness_score, 4),
                    "total_penalty": total_penalty,
                    "difficulty_band": difficulty_band,
                },
                threshold={
                    "readiness_score": round(review_readiness_threshold, 4),
                    "total_penalty": round(review_penalty_threshold, 4),
                },
                reason=(
                    "high_risk_but_not_high_difficulty"
                    if difficulty_band != "hard" and total_penalty >= review_penalty_threshold
                    else "high_readiness_high_penalty"
                ),
            )
            if review_triggered:
                warnings.append("main_idea material is structurally usable, but high readiness is paired with elevated penalty risk.")

        center_constraints = self._extract_main_idea_constraints(context)
        center_contract = (
            context.get("validator_contract", {}).get("center_understanding")
            if isinstance(context.get("validator_contract"), dict)
            and isinstance(context.get("validator_contract", {}).get("center_understanding"), dict)
            else {}
        )
        is_center_understanding = (
            not is_title_selection
            and (
                business_subtype == "center_understanding"
                or bool(center_contract)
                or bool(center_constraints["effective_structure_mode"])
                or bool(center_constraints["effective_main_axis_source"])
            )
        )
        checks["center_understanding_constraint_read"] = {
            "passed": True if is_center_understanding else None,
            "required": is_center_understanding,
            "source": "validator_runtime_alignment" if is_center_understanding else "compatibility_disabled",
            "runtime_argument_structure": center_constraints["runtime_argument_structure"],
            "runtime_main_axis_source": center_constraints["runtime_main_axis_source"],
            "runtime_abstraction_level": center_constraints["runtime_abstraction_level"],
            "runtime_distractor_types": center_constraints["runtime_distractor_types"],
            "contract_argument_structure": center_constraints["contract_argument_structure"],
            "contract_main_axis_source": center_constraints["contract_main_axis_source"],
            "contract_abstraction_level": center_constraints["contract_abstraction_level"],
            "contract_distractor_types": center_constraints["contract_distractor_types"],
        }

        if is_center_understanding:
            if (
                center_constraints["runtime_argument_structure"]
                and center_constraints["contract_argument_structure"]
                and center_constraints["runtime_argument_structure"] != center_constraints["contract_argument_structure"]
            ):
                self._append_unique_error(errors, "argument_structure_mismatch")
            if (
                center_constraints["runtime_main_axis_source"]
                and center_constraints["contract_main_axis_source"]
                and center_constraints["runtime_main_axis_source"] != center_constraints["contract_main_axis_source"]
            ):
                self._append_unique_error(errors, "main_axis_mismatch")
            if (
                center_constraints["runtime_abstraction_level"]
                and center_constraints["contract_abstraction_level"]
                and center_constraints["runtime_abstraction_level"] != center_constraints["contract_abstraction_level"]
            ):
                self._append_unique_error(errors, "abstraction_level_mismatch")

            structure_profile = self._build_main_idea_structure_profile(
                material_text=material_text,
                structure_mode=center_constraints["effective_structure_mode"],
            )
            checks["center_understanding_argument_structure"] = {
                "passed": structure_profile["detected"] if center_constraints["effective_structure_mode"] else None,
                "required": bool(center_constraints["effective_structure_mode"]),
                "source": center_constraints["runtime_argument_structure_source"]
                if center_constraints["runtime_argument_structure"]
                else center_constraints["contract_argument_structure_source"],
                "structure_mode": center_constraints["effective_structure_mode"],
                "axis_unit_index": structure_profile["axis_unit_index"],
                "axis_text": structure_profile["axis_text"],
            }
            if center_constraints["effective_structure_mode"] and not structure_profile["detected"]:
                self._append_unique_error(errors, "argument_structure_mismatch")

            options = generated_question.options or {}
            correct_text = (options.get(generated_question.answer or "", "") or "").strip()
            option_profile = self._main_idea_option_profile(
                option_text=correct_text,
                material_text=material_text,
                structure_profile=structure_profile,
            )
            checks["center_understanding_main_axis_alignment"] = {
                "passed": option_profile["axis_aligned"],
                "required": True,
                "source": center_constraints["runtime_main_axis_source_source"]
                if center_constraints["runtime_main_axis_source"]
                else center_constraints["contract_main_axis_source_source"],
                "axis_support": option_profile["axis_support"],
                "background_support": option_profile["background_support"],
                "example_support": option_profile["example_support"],
                "best_unit_index": option_profile["best_unit_index"],
            }
            if not option_profile["axis_aligned"]:
                self._append_unique_error(errors, "main_axis_mismatch")

            if center_constraints["effective_structure_mode"] == "turning":
                if (
                    option_profile["background_support"]["shared_token_count"] >= max(2, option_profile["axis_support"]["shared_token_count"])
                        and option_profile["background_support"]["supported_token_ratio"] >= option_profile["axis_support"]["supported_token_ratio"]
                ):
                    self._append_unique_error(errors, "main_axis_mismatch")

            if center_constraints["effective_structure_mode"] in {"cause_effect", "final_summary"} and option_profile["local_dominant"]:
                self._append_unique_error(errors, "local_point_as_main_axis")

            if center_constraints["effective_structure_mode"] == "example_to_conclusion" and option_profile["example_dominant"]:
                self._append_unique_error(errors, "example_promoted_to_main_idea")

            expected_abstraction = center_constraints["effective_abstraction_level"] or "medium"
            checks["center_understanding_abstraction_level"] = {
                "passed": not option_profile["low_abstraction"] and not option_profile["over_abstract"],
                "required": True,
                "source": center_constraints["runtime_abstraction_level_source"]
                if center_constraints["runtime_abstraction_level"]
                else center_constraints["contract_abstraction_level_source"],
                "expected_level": expected_abstraction,
                "low_abstraction": option_profile["low_abstraction"],
                "over_abstract": option_profile["over_abstract"],
            }
            if expected_abstraction in {"medium", "high"} and option_profile["low_abstraction"]:
                self._append_unique_error(errors, "abstraction_level_mismatch")
            if option_profile["over_abstract"]:
                self._append_unique_error(errors, "abstraction_level_mismatch")

        return errors, warnings, checks

    def _validate_continuation(
        self,
        generated_question: GeneratedQuestion,
        context: dict[str, Any],
    ) -> tuple[list[str], list[str], dict[str, Any]]:
        errors: list[str] = []
        stem = generated_question.stem.strip()
        continuation_markers = ("?????????", "????????", "?????", "??????", "????")
        exam_style = any(marker in stem for marker in continuation_markers)
        checks = {
            "continuation_material_present": {"passed": bool(context.get("material_text"))},
            "continuation_stem_present": {"passed": bool(stem)},
            "continuation_exam_style_prompt": {"passed": exam_style},
        }
        if not context.get("material_text"):
            errors.append("continuation requires non-empty source material.")
        warnings = [] if exam_style else ["continuation stem does not show a clear follow-up prompt pattern."]
        return errors, warnings, checks

    def _resolve_sentence_order_runtime_material_text(
        self,
        *,
        generated_question: GeneratedQuestion,
        context: dict[str, Any],
    ) -> tuple[str, str]:
        material_source = context.get("material_source") or {}
        prompt_extras = (
            material_source.get("prompt_extras")
            if isinstance(material_source, dict) and isinstance(material_source.get("prompt_extras"), dict)
            else {}
        )
        sortable_units = [
            str(unit or "").strip()
            for unit in (prompt_extras.get("sortable_units") or [])
            if str(unit or "").strip()
        ]
        if len(sortable_units) >= 2:
            circled = "①②③④⑤⑥⑦⑧⑨⑩"
            material_text = "\n".join(
                f"{circled[index] if index < len(circled) else f'{index + 1}.'} {unit}"
                for index, unit in enumerate(sortable_units)
            )
            return material_text, "material_source.prompt_extras.sortable_units"
        sortable_material_text = str(prompt_extras.get("sortable_material_text") or "").strip()
        if sortable_material_text:
            return sortable_material_text, "material_source.prompt_extras.sortable_material_text"

        original_sentences = [str(item or "").strip() for item in (generated_question.original_sentences or []) if str(item or "").strip()]
        correct_order = [self._coerce_int(item) for item in (generated_question.correct_order or [])]
        if original_sentences and len(original_sentences) == len(correct_order) and all(index is not None for index in correct_order):
            ordered_units = self._resolve_sentence_order_units_for_sequence(
                original_sentences=original_sentences,
                order=[int(index) for index in correct_order if index is not None],
            )
            if len(ordered_units) == len(original_sentences):
                circled = "①②③④⑤⑥⑦⑧⑨⑩"
                rebuilt_text = "\n".join(
                    f"{circled[index] if index < len(circled) else f'{index + 1}.'} {text}"
                    for index, (_, text) in enumerate(ordered_units)
                )
                if rebuilt_text.strip():
                    return rebuilt_text, "generated_question.original_sentences"
        return str(context.get("material_text") or ""), "material_text"

    def _validate_sentence_order(
        self,
        generated_question: GeneratedQuestion,
        context: dict[str, Any],
    ) -> tuple[list[str], list[str], dict[str, Any]]:
        stem = generated_question.stem.strip()
        material_text, material_text_source = self._resolve_sentence_order_runtime_material_text(
            generated_question=generated_question,
            context=context,
        )
        validator_contract = context.get("validator_contract") or {}
        source_analysis = context.get("source_question_analysis") or {}
        structure_constraints = source_analysis.get("structure_constraints") or {}

        has_order_signal = all(
            bool(self._extract_order_sequence(value))
            for value in generated_question.options.values()
        ) if generated_question.options else False
        stem_exam_style = bool(
            re.search(r"将以下\d+个句子重新排列[，,]?\s*语序正确的一项是[:：]?", stem)
        )
        material_unit_count = self._count_sortable_units_from_material(material_text)
        option_unit_counts = sorted(self._extract_order_option_unit_counts(generated_question.options))
        option_unit_count = option_unit_counts[-1] if option_unit_counts else 0
        orderability = self._build_sentence_order_uniqueness_profile(material_text)
        sentence_order_contract = validator_contract.get("sentence_order") if isinstance(validator_contract, dict) else None
        structure_contract = validator_contract.get("structure_constraints") if isinstance(validator_contract, dict) else None
        thresholds_contract = validator_contract.get("thresholds") if isinstance(validator_contract, dict) else None
        reasoning_contract = validator_contract.get("reasoning") if isinstance(validator_contract, dict) else None
        contract_sortable_unit_count = None
        contract_unique_opener_min_score = None
        contract_closure_min_score = None
        contract_exchange_risk_max = None
        contract_multi_path_risk_max = None
        contract_function_overlap_max = None
        contract_expected_binding_pair_count = None
        contract_expected_unique_answer_strength = None
        contract_required_reasoning_modes: set[str] = set()
        if isinstance(validator_contract, dict):
            raw_sortable_unit_count = (
                validator_contract.get("sortable_unit_count")
                or (sentence_order_contract.get("sortable_unit_count") if isinstance(sentence_order_contract, dict) else None)
                or (structure_contract.get("sortable_unit_count") if isinstance(structure_contract, dict) else None)
            )
            if raw_sortable_unit_count not in (None, ""):
                contract_sortable_unit_count = int(raw_sortable_unit_count)
            raw_unique_opener_min_score = (
                validator_contract.get("unique_opener_min_score")
                or (sentence_order_contract.get("unique_opener_min_score") if isinstance(sentence_order_contract, dict) else None)
                or (thresholds_contract.get("unique_opener_min_score") if isinstance(thresholds_contract, dict) else None)
            )
            if raw_unique_opener_min_score not in (None, ""):
                contract_unique_opener_min_score = float(raw_unique_opener_min_score)
            raw_closure_min_score = (
                validator_contract.get("closure_min_score")
                or (sentence_order_contract.get("closure_min_score") if isinstance(sentence_order_contract, dict) else None)
                or (thresholds_contract.get("closure_min_score") if isinstance(thresholds_contract, dict) else None)
            )
            if raw_closure_min_score not in (None, ""):
                contract_closure_min_score = float(raw_closure_min_score)
            raw_exchange_risk_max = (
                validator_contract.get("exchange_risk_max")
                or (sentence_order_contract.get("exchange_risk_max") if isinstance(sentence_order_contract, dict) else None)
                or (thresholds_contract.get("exchange_risk_max") if isinstance(thresholds_contract, dict) else None)
            )
            if raw_exchange_risk_max not in (None, ""):
                contract_exchange_risk_max = float(raw_exchange_risk_max)
            raw_multi_path_risk_max = (
                validator_contract.get("multi_path_risk_max")
                or (sentence_order_contract.get("multi_path_risk_max") if isinstance(sentence_order_contract, dict) else None)
                or (thresholds_contract.get("multi_path_risk_max") if isinstance(thresholds_contract, dict) else None)
            )
            if raw_multi_path_risk_max not in (None, ""):
                contract_multi_path_risk_max = float(raw_multi_path_risk_max)
            raw_function_overlap_max = (
                validator_contract.get("function_overlap_max")
                or (sentence_order_contract.get("function_overlap_max") if isinstance(sentence_order_contract, dict) else None)
                or (thresholds_contract.get("function_overlap_max") if isinstance(thresholds_contract, dict) else None)
            )
            if raw_function_overlap_max not in (None, ""):
                contract_function_overlap_max = float(raw_function_overlap_max)
            raw_expected_binding_pair_count = (
                validator_contract.get("expected_binding_pair_count")
                or (sentence_order_contract.get("expected_binding_pair_count") if isinstance(sentence_order_contract, dict) else None)
                or (structure_contract.get("expected_binding_pair_count") if isinstance(structure_contract, dict) else None)
            )
            if raw_expected_binding_pair_count not in (None, ""):
                contract_expected_binding_pair_count = int(raw_expected_binding_pair_count)
            raw_expected_unique_answer_strength = (
                validator_contract.get("expected_unique_answer_strength")
                or (sentence_order_contract.get("expected_unique_answer_strength") if isinstance(sentence_order_contract, dict) else None)
                or (structure_contract.get("expected_unique_answer_strength") if isinstance(structure_contract, dict) else None)
            )
            if raw_expected_unique_answer_strength not in (None, ""):
                contract_expected_unique_answer_strength = float(raw_expected_unique_answer_strength)
            raw_required_reasoning_modes = (
                validator_contract.get("required_reasoning_modes")
                or (sentence_order_contract.get("required_reasoning_modes") if isinstance(sentence_order_contract, dict) else None)
                or (reasoning_contract.get("required_modes") if isinstance(reasoning_contract, dict) else None)
            )
            if isinstance(raw_required_reasoning_modes, str):
                contract_required_reasoning_modes = {raw_required_reasoning_modes}
            elif isinstance(raw_required_reasoning_modes, (list, tuple, set)):
                contract_required_reasoning_modes = {
                    str(mode).strip()
                    for mode in raw_required_reasoning_modes
                    if str(mode).strip()
                }
        expected_sortable_unit_count = contract_sortable_unit_count
        expected_sortable_unit_count_source = (
            "validator_contract"
            if contract_sortable_unit_count is not None
            else "compatibility_disabled"
        )
        unique_opener_min_score = contract_unique_opener_min_score
        unique_opener_min_score_source = (
            "validator_contract" if contract_unique_opener_min_score is not None else "compatibility_disabled"
        )
        closure_min_score = contract_closure_min_score
        closure_min_score_source = (
            "validator_contract" if contract_closure_min_score is not None else "compatibility_disabled"
        )
        exchange_risk_max = contract_exchange_risk_max
        exchange_risk_max_source = (
            "validator_contract" if contract_exchange_risk_max is not None else "compatibility_disabled"
        )
        multi_path_risk_max = contract_multi_path_risk_max
        multi_path_risk_max_source = (
            "validator_contract" if contract_multi_path_risk_max is not None else "compatibility_disabled"
        )
        function_overlap_max = contract_function_overlap_max
        function_overlap_max_source = (
            "validator_contract" if contract_function_overlap_max is not None else "compatibility_disabled"
        )
        expected_binding_pair_count = contract_expected_binding_pair_count
        expected_binding_pair_count_source = (
            "validator_contract"
            if contract_expected_binding_pair_count is not None
            else "compatibility_disabled"
        )
        expected_unique_answer_strength = contract_expected_unique_answer_strength
        expected_unique_answer_strength_source = (
            "validator_contract"
            if contract_expected_unique_answer_strength is not None
            else "compatibility_disabled"
        )
        correct_order = list(generated_question.correct_order or [])
        raw_original_units = [str(item or "").strip() for item in (generated_question.original_sentences or []) if str(item or "").strip()]
        normalized_original_units = self._normalize_sentence_order_units_to_six(raw_original_units) or raw_original_units
        expected_order_size = expected_sortable_unit_count or len(normalized_original_units)
        expected_order_sequence = list(range(1, expected_order_size + 1)) if expected_order_size else []
        option_orders = {
            key: self._extract_order_sequence(value)
            for key, value in (generated_question.options or {}).items()
        }
        answer = str(generated_question.answer or "").strip().upper()
        analysis_orders = self._extract_reference_order_sequences(generated_question.analysis or "")
        ordered_units = self._resolve_sentence_order_units_for_sequence(
            original_sentences=normalized_original_units,
            order=correct_order,
        )
        binding_pairs = self._extract_sentence_order_binding_pairs(context)
        explicit_roles = self._extract_sentence_order_roles(context)
        unit_sentence_counts = [self._sentence_order_unit_sentence_count(unit) for unit in normalized_original_units]

        checks = {
            "sentence_order_signal": {"passed": has_order_signal},
            "sentence_order_exam_style_prompt": {"passed": stem_exam_style},
            "sentence_order_material_unit_count": {
                "passed": material_unit_count >= 4,
                "count": material_unit_count,
                "source": material_text_source,
            },
            "sentence_order_option_unit_counts": {"passed": bool(option_unit_counts), "counts": option_unit_counts},
            "sentence_order_unique_opener": self._build_contract_gated_check(
                active=unique_opener_min_score is not None,
                passed=orderability["unique_opener_score"] >= (unique_opener_min_score or 0.0),
                source=unique_opener_min_score_source,
                score=orderability["unique_opener_score"],
                threshold=unique_opener_min_score,
            ),
            "sentence_order_binding_pairs": self._build_contract_gated_check(
                active=expected_binding_pair_count is not None,
                passed=orderability["binding_pair_count"] >= (expected_binding_pair_count or 0),
                source=expected_binding_pair_count_source,
                count=orderability["binding_pair_count"],
                expected=expected_binding_pair_count,
            ),
            "sentence_order_closure": self._build_contract_gated_check(
                active=closure_min_score is not None,
                passed=orderability["has_closing_role"] and orderability["context_closure_score"] >= (closure_min_score or 0.0),
                source=closure_min_score_source,
                context_closure_score=orderability["context_closure_score"],
                expected=closure_min_score,
            ),
            "sentence_order_exchange_risk": self._build_contract_gated_check(
                active=exchange_risk_max is not None,
                passed=orderability["exchange_risk"] <= (exchange_risk_max if exchange_risk_max is not None else 1.0),
                source=exchange_risk_max_source,
                score=orderability["exchange_risk"],
                expected=exchange_risk_max,
            ),
            "sentence_order_multi_path_risk": self._build_contract_gated_check(
                active=multi_path_risk_max is not None,
                passed=orderability["multi_path_risk"] <= (multi_path_risk_max if multi_path_risk_max is not None else 1.0),
                source=multi_path_risk_max_source,
                score=orderability["multi_path_risk"],
                expected=multi_path_risk_max,
            ),
            "sentence_order_function_overlap": self._build_contract_gated_check(
                active=function_overlap_max is not None,
                passed=orderability["function_overlap_score"] <= (function_overlap_max if function_overlap_max is not None else 1.0),
                source=function_overlap_max_source,
                score=orderability["function_overlap_score"],
                expected=function_overlap_max,
            ),
            "sentence_order_original_sentences": {
                "passed": (
                    len(normalized_original_units) == expected_sortable_unit_count
                    if expected_sortable_unit_count
                    else len(normalized_original_units) >= 2
                ),
                "count": len(normalized_original_units),
                "expected": expected_sortable_unit_count,
                "source": expected_sortable_unit_count_source,
            },
            "sentence_order_unit_sentence_span": {
                "passed": bool(unit_sentence_counts) and all(1 <= count <= 2 for count in unit_sentence_counts),
                "counts": unit_sentence_counts,
                "allowed": [1, 2],
            },
            "sentence_order_correct_order": {
                "passed": bool(correct_order) and (not expected_order_sequence or sorted(correct_order) == expected_order_sequence),
                "value": correct_order,
                "expected": expected_order_sequence,
            },
        }
        errors: list[str] = []
        warnings: list[str] = []

        if not has_order_signal:
            warnings.append("sentence_order options do not show obvious ordering signals.")
        if not stem_exam_style:
            warnings.append("sentence_order stem does not look like a standard ordering prompt.")
        if material_unit_count < 4:
            self._append_unique_error(errors, "ordering_chain_incomplete")
        if option_unit_counts and len(set(option_unit_counts)) > 1:
            self._append_unique_error(errors, "ordering_chain_incomplete")
        if expected_sortable_unit_count:
            if len(normalized_original_units) != expected_sortable_unit_count:
                self._append_unique_error(errors, "sentence_count_mismatch")
        elif len(normalized_original_units) < 2:
            self._append_unique_error(errors, "sentence_count_mismatch")
        if unit_sentence_counts and not all(1 <= count <= 2 for count in unit_sentence_counts):
            warnings.append("sentence_order material units are not evenly segmented into 1-2 sentence spans.")
        if not correct_order or (expected_order_sequence and sorted(correct_order) != expected_order_sequence):
            self._append_unique_error(errors, "ordering_chain_incomplete")
        correct_option_letters = [key for key, sequence in option_orders.items() if sequence == correct_order]
        checks["sentence_order_single_truth_option"] = {"passed": len(correct_option_letters) == 1, "matching_letters": correct_option_letters}
        if len(correct_option_letters) != 1:
            self._append_unique_error(errors, "ordering_chain_incomplete")
        checks["sentence_order_answer_binding"] = {
            "passed": bool(answer and answer in option_orders and option_orders.get(answer) == correct_order),
            "answer": answer,
            "answer_order": option_orders.get(answer),
            "correct_order": correct_order,
        }
        if answer not in option_orders or option_orders.get(answer) != correct_order:
            self._append_unique_error(errors, "ordering_chain_incomplete")
        checks["sentence_order_analysis_binding"] = {
            "passed": bool(analysis_orders and analysis_orders[0] == correct_order),
            "analysis_orders": analysis_orders,
            "correct_order": correct_order,
        }
        if not analysis_orders or analysis_orders[0] != correct_order:
            self._append_unique_error(errors, "ordering_chain_incomplete")
        if len(analysis_orders) > 1 and any(sequence != correct_order for sequence in analysis_orders[1:]):
            self._append_unique_error(errors, "ordering_chain_incomplete")
        if expected_sortable_unit_count:
            aligned = option_unit_count == expected_sortable_unit_count or material_unit_count == expected_sortable_unit_count
            checks["sentence_order_reference_unit_alignment"] = {
                "passed": aligned,
                "reference_unit_count": expected_sortable_unit_count,
                "generated_option_unit_count": option_unit_count,
                "material_unit_count": material_unit_count,
                "source": expected_sortable_unit_count_source,
            }
            if not aligned:
                self._append_unique_error(errors, "sentence_count_mismatch")
        if unique_opener_min_score is not None and orderability["unique_opener_score"] < unique_opener_min_score:
            warnings.append("sentence_order unique opener strength is below the preferred threshold.")
        if expected_binding_pair_count is not None and orderability["binding_pair_count"] < expected_binding_pair_count:
            warnings.append("sentence_order binding pair count is below the preferred threshold.")
        if closure_min_score is not None and (not orderability["has_closing_role"] or orderability["context_closure_score"] < closure_min_score):
            warnings.append("sentence_order closing strength is below the preferred threshold.")
        if exchange_risk_max is not None and orderability["exchange_risk"] > exchange_risk_max:
            warnings.append("sentence_order exchange risk is above the preferred threshold.")
        if multi_path_risk_max is not None and orderability["multi_path_risk"] > multi_path_risk_max:
            warnings.append("sentence_order multi-path risk is above the preferred threshold.")
        if function_overlap_max is not None and orderability["function_overlap_score"] > function_overlap_max:
            warnings.append("sentence_order function overlap is above the preferred threshold.")
        if expected_unique_answer_strength is not None:
            unique_strength_ok = orderability["unique_answer_strength"] + 0.06 >= expected_unique_answer_strength
            checks["sentence_order_unique_answer_strength"] = self._build_contract_gated_check(
                active=True,
                passed=unique_strength_ok,
                source=expected_unique_answer_strength_source,
                score=orderability["unique_answer_strength"],
                expected=expected_unique_answer_strength,
            )
            if not unique_strength_ok:
                warnings.append("sentence_order unique answer strength is below the preferred threshold.")

        scoring, scoring_source = self._extract_material_scoring(context)
        task_scoring_available = bool(scoring)
        final_candidate_score = self._coerce_float(scoring.get("final_candidate_score")) or 0.0
        readiness_score = self._coerce_float(scoring.get("readiness_score")) or 0.0
        risk_penalties = scoring.get("risk_penalties") if isinstance(scoring.get("risk_penalties"), dict) else {}
        total_penalty = round(sum(float(value or 0.0) for value in risk_penalties.values()), 4)
        difficulty_vector = scoring.get("difficulty_vector") if isinstance(scoring.get("difficulty_vector"), dict) else {}
        difficulty_band = str(scoring.get("difficulty_band_hint") or "")
        checks["sentence_order_material_scoring_available"] = {
            "passed": task_scoring_available,
            "source": scoring_source,
            "task_family": scoring.get("task_family"),
            "recommended": bool(scoring.get("recommended")) if task_scoring_available else None,
            "needs_review": bool(scoring.get("needs_review")) if task_scoring_available else None,
        }
        if not task_scoring_available:
            warnings.append("material scoring payload is missing, so validator could not enforce sentence_order scoring controls.")
        else:
            scoring_sources = [
                (sentence_order_contract if isinstance(sentence_order_contract, dict) else None, "validator_contract.sentence_order"),
                (structure_contract if isinstance(structure_contract, dict) else None, "validator_contract.structure_constraints"),
                (thresholds_contract if isinstance(thresholds_contract, dict) else None, "validator_contract.thresholds"),
                (validator_contract if isinstance(validator_contract, dict) else None, "validator_contract"),
            ]
            compatibility = self._material_scoring_compatibility_profile(task_family="sentence_order")
            min_final_raw, min_final_source = self._resolve_scoring_contract_value(
                sources=scoring_sources,
                field_names=("min_final_candidate_score",),
                compatibility=compatibility,
                compatibility_key="min_final_candidate_score",
            )
            min_readiness_raw, min_readiness_source = self._resolve_scoring_contract_value(
                sources=scoring_sources,
                field_names=("min_readiness_score",),
                compatibility=compatibility,
                compatibility_key="min_readiness_score",
            )
            max_total_penalty_raw, max_total_penalty_source = self._resolve_scoring_contract_value(
                sources=scoring_sources,
                field_names=("max_total_penalty",),
                compatibility=compatibility,
                compatibility_key="max_total_penalty",
            )
            review_signal_raw, review_signal_source = self._resolve_scoring_contract_value(
                sources=scoring_sources,
                field_names=("review_if_high_readiness_high_penalty",),
                compatibility=compatibility,
                compatibility_key="review_if_high_readiness_high_penalty",
            )
            min_complexity_raw, min_complexity_source = self._resolve_scoring_contract_value(
                sources=scoring_sources,
                field_names=("min_complexity_score",),
                compatibility=compatibility,
                compatibility_key="min_complexity_score",
            )
            min_constraint_raw, min_constraint_source = self._resolve_scoring_contract_value(
                sources=scoring_sources,
                field_names=("min_constraint_intensity_score",),
                compatibility=compatibility,
                compatibility_key="min_constraint_intensity_score",
            )
            max_first_instability_raw, max_first_instability_source = self._resolve_scoring_contract_value(
                sources=scoring_sources,
                field_names=("max_first_instability_penalty",),
                compatibility=compatibility,
                compatibility_key="max_first_instability_penalty",
            )
            max_last_instability_raw, max_last_instability_source = self._resolve_scoring_contract_value(
                sources=scoring_sources,
                field_names=("max_last_instability_penalty",),
                compatibility=compatibility,
                compatibility_key="max_last_instability_penalty",
            )
            max_weak_constraint_raw, max_weak_constraint_source = self._resolve_scoring_contract_value(
                sources=scoring_sources,
                field_names=("max_weak_constraint_penalty",),
                compatibility=compatibility,
                compatibility_key="max_weak_constraint_penalty",
            )
            difficulty_band_allowed_raw, difficulty_band_allowed_source = self._resolve_scoring_contract_value(
                sources=scoring_sources,
                field_names=("difficulty_band_allowed",),
                compatibility=compatibility,
                compatibility_key="difficulty_band_allowed",
            )
            complexity_score = self._coerce_float(difficulty_vector.get("complexity_score")) or 0.0
            constraint_intensity_score = self._coerce_float(difficulty_vector.get("constraint_intensity_score")) or 0.0
            first_instability_penalty = self._coerce_float(risk_penalties.get("first_instability_penalty")) or 0.0
            last_instability_penalty = self._coerce_float(risk_penalties.get("last_instability_penalty")) or 0.0
            weak_constraint_penalty = self._coerce_float(risk_penalties.get("weak_constraint_penalty")) or 0.0

            self._apply_scoring_threshold_check(
                checks=checks,
                errors=errors,
                warnings=warnings,
                check_name="sentence_order_final_candidate_score",
                actual=final_candidate_score,
                threshold=self._coerce_float(min_final_raw),
                source=min_final_source,
                comparator="min",
                reason="min_final_candidate_score",
                error_message="sentence_order material final_candidate_score is below the accepted floor.",
                extra={"difficulty_band": difficulty_band, "scoring_source": scoring_source},
            )
            self._apply_scoring_threshold_check(
                checks=checks,
                errors=errors,
                warnings=warnings,
                check_name="sentence_order_readiness_score",
                actual=readiness_score,
                threshold=self._coerce_float(min_readiness_raw),
                source=min_readiness_source,
                comparator="min",
                reason="min_readiness_score",
                error_message="sentence_order material readiness_score is below the accepted floor.",
                extra={"difficulty_band": difficulty_band, "scoring_source": scoring_source},
            )
            self._apply_scoring_threshold_check(
                checks=checks,
                errors=errors,
                warnings=warnings,
                check_name="sentence_order_total_penalty",
                actual=total_penalty,
                threshold=self._coerce_float(max_total_penalty_raw),
                source=max_total_penalty_source,
                comparator="max",
                reason="max_total_penalty",
                error_message="sentence_order material total penalty is higher than the allowed range.",
                extra={"difficulty_band": difficulty_band, "risk_penalties": risk_penalties, "scoring_source": scoring_source},
            )
            self._apply_scoring_threshold_check(
                checks=checks,
                errors=errors,
                warnings=warnings,
                check_name="sentence_order_complexity",
                actual=complexity_score,
                threshold=self._coerce_float(min_complexity_raw),
                source=min_complexity_source,
                comparator="min",
                reason="min_complexity_score",
                error_message="sentence_order material complexity is lower than the target requirement.",
                extra={"difficulty_band": difficulty_band, "difficulty_vector": difficulty_vector, "scoring_source": scoring_source},
            )
            self._apply_scoring_threshold_check(
                checks=checks,
                errors=errors,
                warnings=warnings,
                check_name="sentence_order_constraint_intensity",
                actual=constraint_intensity_score,
                threshold=self._coerce_float(min_constraint_raw),
                source=min_constraint_source,
                comparator="min",
                reason="min_constraint_intensity_score",
                error_message="sentence_order material constraint intensity is lower than the target requirement.",
                extra={"difficulty_band": difficulty_band, "difficulty_vector": difficulty_vector, "scoring_source": scoring_source},
            )
            self._apply_scoring_threshold_check(
                checks=checks,
                errors=errors,
                warnings=warnings,
                check_name="sentence_order_first_instability",
                actual=first_instability_penalty,
                threshold=self._coerce_float(max_first_instability_raw),
                source=max_first_instability_source,
                comparator="max",
                reason="max_first_instability_penalty",
                error_message="sentence_order material first-instability penalty is too high.",
                extra={"difficulty_band": difficulty_band, "risk_penalties": risk_penalties, "scoring_source": scoring_source},
            )
            self._apply_scoring_threshold_check(
                checks=checks,
                errors=errors,
                warnings=warnings,
                check_name="sentence_order_last_instability",
                actual=last_instability_penalty,
                threshold=self._coerce_float(max_last_instability_raw),
                source=max_last_instability_source,
                comparator="max",
                reason="max_last_instability_penalty",
                error_message="sentence_order material last-instability penalty is too high.",
                extra={"difficulty_band": difficulty_band, "risk_penalties": risk_penalties, "scoring_source": scoring_source},
            )
            self._apply_scoring_threshold_check(
                checks=checks,
                errors=errors,
                warnings=warnings,
                check_name="sentence_order_weak_constraint",
                actual=weak_constraint_penalty,
                threshold=self._coerce_float(max_weak_constraint_raw),
                source=max_weak_constraint_source,
                comparator="max",
                reason="max_weak_constraint_penalty",
                error_message="sentence_order material weak-constraint penalty is too high.",
                extra={"difficulty_band": difficulty_band, "risk_penalties": risk_penalties, "scoring_source": scoring_source},
            )
            self._apply_scoring_band_check(
                checks=checks,
                errors=errors,
                warnings=warnings,
                check_name="sentence_order_difficulty_band",
                difficulty_band=difficulty_band,
                allowed_bands=self._normalize_band_allowed(difficulty_band_allowed_raw),
                source=difficulty_band_allowed_source,
                error_message="sentence_order material difficulty band is outside the allowed range.",
            )
            review_signal_active = bool(
                review_signal_raw is True
                or str(review_signal_raw).strip().lower() in {"true", "1", "yes", "on"}
            )
            review_penalty_threshold = self._coerce_float(max_total_penalty_raw)
            if review_penalty_threshold is None:
                review_penalty_threshold = 0.55
            review_readiness_threshold = self._coerce_float(min_readiness_raw)
            if review_readiness_threshold is None:
                review_readiness_threshold = 0.50
            review_triggered = bool(
                review_signal_active
                and readiness_score >= review_readiness_threshold
                and total_penalty >= review_penalty_threshold
            )
            checks["sentence_order_review_like_risk"] = self._build_contract_gated_check(
                active=review_signal_active,
                passed=not review_triggered,
                source=review_signal_source,
                actual={
                    "readiness_score": round(readiness_score, 4),
                    "total_penalty": total_penalty,
                    "difficulty_band": difficulty_band,
                },
                threshold={
                    "readiness_score": round(review_readiness_threshold, 4),
                    "total_penalty": round(review_penalty_threshold, 4),
                },
                reason=(
                    "high_risk_but_not_high_difficulty"
                    if difficulty_band != "hard" and total_penalty >= review_penalty_threshold
                    else "high_readiness_high_penalty"
                ),
            )
            if review_triggered:
                warnings.append("sentence_order material is structurally usable, but merge/instability risk is elevated.")

        legal_head_required = bool(
            isinstance(sentence_order_contract, dict) and sentence_order_contract.get("require_legal_head")
        )
        legal_tail_required = bool(
            isinstance(sentence_order_contract, dict) and sentence_order_contract.get("require_legal_tail")
        )
        binding_pairs_required = bool(
            isinstance(sentence_order_contract, dict) and sentence_order_contract.get("require_binding_pairs_intact")
        )
        complete_ordering_required = bool(
            isinstance(sentence_order_contract, dict) and sentence_order_contract.get("require_complete_ordering_chain")
        )

        illegal_head = False
        if legal_head_required and ordered_units:
            illegal_head = self._sentence_order_head_is_illegal(ordered_units[0][1])
            checks["sentence_order_head_enforcement"] = {
                "passed": not illegal_head,
                "unit_index": ordered_units[0][0],
                "text": ordered_units[0][1],
            }
            if illegal_head:
                self._append_unique_error(errors, "illegal_head")

        illegal_tail = False
        if legal_tail_required and ordered_units:
            illegal_tail = self._sentence_order_tail_is_illegal(ordered_units[-1][1])
            checks["sentence_order_tail_enforcement"] = {
                "passed": not illegal_tail,
                "unit_index": ordered_units[-1][0],
                "text": ordered_units[-1][1],
            }
            if illegal_tail:
                self._append_unique_error(errors, "illegal_tail")

        binding_violations: list[dict[str, int]] = []
        if binding_pairs_required and binding_pairs and ordered_units:
            positions = {unit_index: position for position, (unit_index, _) in enumerate(ordered_units)}
            for left, right in binding_pairs:
                if left in positions and right in positions and positions[left] > positions[right]:
                    binding_violations.append({"before": left, "after": right})
            checks["sentence_order_binding_enforcement"] = {
                "passed": not binding_violations,
                "binding_pairs": [{"before": left, "after": right} for left, right in binding_pairs],
                "violations": binding_violations,
            }
            if binding_violations:
                self._append_unique_error(errors, "binding_violation")

        role_violations: list[dict[str, Any]] = []
        if complete_ordering_required and ordered_units:
            ordered_roles: list[dict[str, Any]] = []
            for position, (unit_index, text) in enumerate(ordered_units):
                explicit_role = explicit_roles.get(unit_index, "")
                role = explicit_role or self._infer_sentence_order_role(text)
                ordered_roles.append(
                    {
                        "unit_index": unit_index,
                        "position": position,
                        "role": role,
                        "text": text,
                        "source": "explicit" if explicit_role else ("inferred" if role else "missing"),
                    }
                )
            for entry in ordered_roles[:-1]:
                if entry["role"] == "conclusion":
                    role_violations.append({"type": "conclusion_not_last", "unit_index": entry["unit_index"]})
            if ordered_roles and ordered_roles[-1]["role"] == "thesis":
                role_violations.append({"type": "thesis_not_last_allowed", "unit_index": ordered_roles[-1]["unit_index"]})
            if ordered_roles and ordered_roles[0]["role"] == "transition" and not illegal_head:
                role_violations.append({"type": "transition_not_first_allowed", "unit_index": ordered_roles[0]["unit_index"]})
            checks["sentence_order_role_enforcement"] = {
                "passed": not role_violations,
                "roles": ordered_roles,
                "violations": role_violations,
            }
            if role_violations:
                self._append_unique_error(errors, "role_order_conflict")
        required_reasoning_modes = set(contract_required_reasoning_modes)
        reasoning_modes_source = "validator_contract" if contract_required_reasoning_modes else "compatibility_disabled"
        timeline_reasoning_required = bool(required_reasoning_modes.intersection({"timeline_sequence", "temporal_chain"}))
        binding_reasoning_required = bool(required_reasoning_modes.intersection({"deterministic_binding", "binding_clue", "binding_pairs"}))
        head_tail_reasoning_required = bool(required_reasoning_modes.intersection({"head_tail_roles", "opener_closure_roles"}))
        if timeline_reasoning_required:
            analysis_has_timeline = any(
                token in generated_question.analysis
                for token in ("时间", "先后", "顺序", "时序", "先看", "再看")
            )
            checks["sentence_order_timeline_reasoning"] = {
                "passed": analysis_has_timeline,
                "required": True,
                "source": reasoning_modes_source,
            }
            if not analysis_has_timeline:
                warnings.append("reference question is timeline-oriented, but analysis does not clearly explain the time/order chain.")
        if binding_reasoning_required:
            analysis_has_binding = any(
                token in generated_question.analysis
                for token in ("承接", "衔接", "照应", "呼应", "转折", "捆绑", "关联")
            )
            checks["sentence_order_binding_reasoning"] = {
                "passed": analysis_has_binding,
                "required": True,
                "source": reasoning_modes_source,
            }
            if not analysis_has_binding:
                warnings.append("reference question relies on deterministic binding, but analysis does not clearly explain the binding clues.")
        analysis_has_head_tail = any(
            token in generated_question.analysis
            for token in ("首句", "尾句", "开头", "结尾", "起点", "收束")
        )
        checks["sentence_order_head_tail_reasoning"] = self._build_contract_gated_check(
            active=head_tail_reasoning_required,
            passed=analysis_has_head_tail,
            source=reasoning_modes_source,
        )
        if head_tail_reasoning_required and not analysis_has_head_tail:
            warnings.append("sentence_order analysis does not clearly explain opener/closing roles.")
        return errors, warnings, checks


    def _validate_sentence_fill(
        self,
        generated_question: GeneratedQuestion,
        context: dict[str, Any],
    ) -> tuple[list[str], list[str], dict[str, Any]]:
        stem = generated_question.stem
        material_source = context.get("material_source") or {}
        material_prompt_extras = (
            material_source.get("prompt_extras")
            if isinstance(material_source, dict) and isinstance(material_source.get("prompt_extras"), dict)
            else {}
        )
        fill_ready_material = str(material_prompt_extras.get("fill_ready_material") or "").strip()
        fill_ready_local_material = str(material_prompt_extras.get("fill_ready_local_material") or "").strip()
        answer_anchor_text = str(material_prompt_extras.get("answer_anchor_text") or "").strip()
        require_original_answer_sentence = bool(material_prompt_extras.get("require_original_answer_sentence"))
        display_material_text = fill_ready_material or str(context.get("material_text") or "")
        validation_material_text = fill_ready_local_material or fill_ready_material or str(context.get("material_text") or "")
        constraints = self._extract_sentence_fill_constraints(context)
        correct_text = self._sentence_fill_correct_option_text(generated_question)
        previous_text, next_text, blank_marker = self._extract_sentence_fill_blank_context(validation_material_text)

        has_blank_signal = any(token in display_material_text for token in ("____", "___", "[BLANK]", "?  ?", "( )", "? ?"))
        fill_prompt_markers = ("??", "???????", "?????", "????", "??????")
        fit_prompt_markers = ("???", "???", "???")
        has_fill_prompt = any(token in stem for token in fill_prompt_markers) and any(
            token in stem for token in fit_prompt_markers
        )
        blank_position = self._detect_blank_position(validation_material_text)
        reference_blank_position = constraints["blank_position"]
        reference_blank_position_source = constraints["blank_position_source"]

        checks = {
            "sentence_fill_gap_signal": {"passed": has_blank_signal},
            "sentence_fill_exam_style_prompt": {"passed": has_fill_prompt},
            "sentence_fill_blank_position": {"passed": bool(blank_position), "blank_position": blank_position},
            "sentence_fill_correct_option_text": {"passed": bool(correct_text), "text": correct_text},
            "sentence_fill_runtime_material_form": {
                "passed": bool(fill_ready_material),
                "source": "material_source.prompt_extras" if fill_ready_material else "material_text",
                "validation_source": (
                    "material_source.prompt_extras.fill_ready_local_material"
                    if fill_ready_local_material
                    else ("material_source.prompt_extras.fill_ready_material" if fill_ready_material else "material_text")
                ),
            },
        }
        errors: list[str] = []
        warnings: list[str] = []

        if not has_blank_signal:
            errors.append("sentence_fill material does not show an obvious blank marker.")
        if not has_fill_prompt:
            warnings.append("sentence_fill stem does not look like a standard fill-in-the-blank prompt.")

        normalized_anchor = self._normalize_sentence_fill_anchor_text(answer_anchor_text)
        normalized_correct = self._normalize_sentence_fill_anchor_text(correct_text)
        exact_anchor_match = bool(normalized_anchor and normalized_correct and normalized_anchor == normalized_correct)
        contract_consistent = bool(
            has_blank_signal
            and has_fill_prompt
            and correct_text
            and (not require_original_answer_sentence or exact_anchor_match)
            and blank_marker
        )
        checks["sentence_fill_material_question_consistency"] = {
            "passed": contract_consistent,
            "has_blank_signal": has_blank_signal,
            "has_fill_prompt": has_fill_prompt,
            "has_correct_option_text": bool(correct_text),
            "has_blank_marker_context": bool(blank_marker),
            "require_original_answer_sentence": require_original_answer_sentence,
            "exact_anchor_match": exact_anchor_match if require_original_answer_sentence else None,
        }
        if not contract_consistent:
            self._append_unique_error(errors, "sentence_fill_material_question_consistency_fail")

        validator_contract = context.get("validator_contract") or {}
        sentence_fill_contract = validator_contract.get("sentence_fill") if isinstance(validator_contract, dict) else None
        structure_contract = validator_contract.get("structure_constraints") if isinstance(validator_contract, dict) else None
        scoring, scoring_source = self._extract_material_scoring(context)
        task_scoring_available = bool(scoring)
        final_candidate_score = self._coerce_float(scoring.get("final_candidate_score")) or 0.0
        readiness_score = self._coerce_float(scoring.get("readiness_score")) or 0.0
        risk_penalties = scoring.get("risk_penalties") if isinstance(scoring.get("risk_penalties"), dict) else {}
        total_penalty = round(sum(float(value or 0.0) for value in risk_penalties.values()), 4)
        difficulty_vector = scoring.get("difficulty_vector") if isinstance(scoring.get("difficulty_vector"), dict) else {}
        difficulty_band = str(scoring.get("difficulty_band_hint") or "")
        checks["sentence_fill_material_scoring_available"] = {
            "passed": task_scoring_available,
            "source": scoring_source,
            "task_family": scoring.get("task_family"),
            "recommended": bool(scoring.get("recommended")) if task_scoring_available else None,
            "needs_review": bool(scoring.get("needs_review")) if task_scoring_available else None,
        }
        if not task_scoring_available:
            warnings.append("material scoring payload is missing, so validator could not enforce sentence_fill scoring controls.")
        else:
            scoring_sources = [
                (sentence_fill_contract if isinstance(sentence_fill_contract, dict) else None, "validator_contract.sentence_fill"),
                (structure_contract if isinstance(structure_contract, dict) else None, "validator_contract.structure_constraints"),
                (validator_contract if isinstance(validator_contract, dict) else None, "validator_contract"),
            ]
            compatibility = self._material_scoring_compatibility_profile(task_family="sentence_fill")
            min_final_raw, min_final_source = self._resolve_scoring_contract_value(
                sources=scoring_sources,
                field_names=("min_final_candidate_score",),
                compatibility=compatibility,
                compatibility_key="min_final_candidate_score",
            )
            min_readiness_raw, min_readiness_source = self._resolve_scoring_contract_value(
                sources=scoring_sources,
                field_names=("min_readiness_score",),
                compatibility=compatibility,
                compatibility_key="min_readiness_score",
            )
            max_total_penalty_raw, max_total_penalty_source = self._resolve_scoring_contract_value(
                sources=scoring_sources,
                field_names=("max_total_penalty",),
                compatibility=compatibility,
                compatibility_key="max_total_penalty",
            )
            review_signal_raw, review_signal_source = self._resolve_scoring_contract_value(
                sources=scoring_sources,
                field_names=("review_if_high_readiness_high_penalty",),
                compatibility=compatibility,
                compatibility_key="review_if_high_readiness_high_penalty",
            )
            min_reasoning_raw, min_reasoning_source = self._resolve_scoring_contract_value(
                sources=scoring_sources,
                field_names=("min_reasoning_depth_score",),
                compatibility=compatibility,
                compatibility_key="min_reasoning_depth_score",
            )
            min_constraint_raw, min_constraint_source = self._resolve_scoring_contract_value(
                sources=scoring_sources,
                field_names=("min_constraint_intensity_score",),
                compatibility=compatibility,
                compatibility_key="min_constraint_intensity_score",
            )
            max_role_ambiguity_raw, max_role_ambiguity_source = self._resolve_scoring_contract_value(
                sources=scoring_sources,
                field_names=("max_role_ambiguity_penalty",),
                compatibility=compatibility,
                compatibility_key="max_role_ambiguity_penalty",
            )
            max_standalone_raw, max_standalone_source = self._resolve_scoring_contract_value(
                sources=scoring_sources,
                field_names=("max_standalone_penalty",),
                compatibility=compatibility,
                compatibility_key="max_standalone_penalty",
            )
            difficulty_band_allowed_raw, difficulty_band_allowed_source = self._resolve_scoring_contract_value(
                sources=scoring_sources,
                field_names=("difficulty_band_allowed",),
                compatibility=compatibility,
                compatibility_key="difficulty_band_allowed",
            )
            reasoning_depth_score = self._coerce_float(difficulty_vector.get("reasoning_depth_score")) or 0.0
            constraint_intensity_score = self._coerce_float(difficulty_vector.get("constraint_intensity_score")) or 0.0
            role_ambiguity_penalty = self._coerce_float(risk_penalties.get("role_ambiguity_penalty")) or 0.0
            standalone_penalty = self._coerce_float(risk_penalties.get("standalone_penalty")) or 0.0

            self._apply_scoring_threshold_check(
                checks=checks,
                errors=errors,
                warnings=warnings,
                check_name="sentence_fill_final_candidate_score",
                actual=final_candidate_score,
                threshold=self._coerce_float(min_final_raw),
                source=min_final_source,
                comparator="min",
                reason="min_final_candidate_score",
                error_message="sentence_fill material final_candidate_score is below the accepted floor.",
                extra={"difficulty_band": difficulty_band, "scoring_source": scoring_source},
            )
            self._apply_scoring_threshold_check(
                checks=checks,
                errors=errors,
                warnings=warnings,
                check_name="sentence_fill_readiness_score",
                actual=readiness_score,
                threshold=self._coerce_float(min_readiness_raw),
                source=min_readiness_source,
                comparator="min",
                reason="min_readiness_score",
                error_message="sentence_fill material readiness_score is below the accepted floor.",
                extra={"difficulty_band": difficulty_band, "scoring_source": scoring_source},
            )
            self._apply_scoring_threshold_check(
                checks=checks,
                errors=errors,
                warnings=warnings,
                check_name="sentence_fill_total_penalty",
                actual=total_penalty,
                threshold=self._coerce_float(max_total_penalty_raw),
                source=max_total_penalty_source,
                comparator="max",
                reason="max_total_penalty",
                error_message="sentence_fill material total penalty is higher than the allowed range.",
                extra={"difficulty_band": difficulty_band, "risk_penalties": risk_penalties, "scoring_source": scoring_source},
            )
            self._apply_scoring_threshold_check(
                checks=checks,
                errors=errors,
                warnings=warnings,
                check_name="sentence_fill_reasoning_depth",
                actual=reasoning_depth_score,
                threshold=self._coerce_float(min_reasoning_raw),
                source=min_reasoning_source,
                comparator="min",
                reason="min_reasoning_depth_score",
                error_message="sentence_fill material reasoning depth is lower than the target requirement.",
                extra={"difficulty_band": difficulty_band, "difficulty_vector": difficulty_vector, "scoring_source": scoring_source},
            )
            self._apply_scoring_threshold_check(
                checks=checks,
                errors=errors,
                warnings=warnings,
                check_name="sentence_fill_constraint_intensity",
                actual=constraint_intensity_score,
                threshold=self._coerce_float(min_constraint_raw),
                source=min_constraint_source,
                comparator="min",
                reason="min_constraint_intensity_score",
                error_message="sentence_fill material constraint intensity is lower than the target requirement.",
                extra={"difficulty_band": difficulty_band, "difficulty_vector": difficulty_vector, "scoring_source": scoring_source},
            )
            self._apply_scoring_threshold_check(
                checks=checks,
                errors=errors,
                warnings=warnings,
                check_name="sentence_fill_role_ambiguity",
                actual=role_ambiguity_penalty,
                threshold=self._coerce_float(max_role_ambiguity_raw),
                source=max_role_ambiguity_source,
                comparator="max",
                reason="max_role_ambiguity_penalty",
                error_message="sentence_fill material role ambiguity penalty is too high.",
                extra={"difficulty_band": difficulty_band, "risk_penalties": risk_penalties, "scoring_source": scoring_source},
            )
            self._apply_scoring_threshold_check(
                checks=checks,
                errors=errors,
                warnings=warnings,
                check_name="sentence_fill_standalone_penalty",
                actual=standalone_penalty,
                threshold=self._coerce_float(max_standalone_raw),
                source=max_standalone_source,
                comparator="max",
                reason="max_standalone_penalty",
                error_message="sentence_fill material standalone penalty is too high.",
                extra={"difficulty_band": difficulty_band, "risk_penalties": risk_penalties, "scoring_source": scoring_source},
            )
            self._apply_scoring_band_check(
                checks=checks,
                errors=errors,
                warnings=warnings,
                check_name="sentence_fill_difficulty_band",
                difficulty_band=difficulty_band,
                allowed_bands=self._normalize_band_allowed(difficulty_band_allowed_raw),
                source=difficulty_band_allowed_source,
                error_message="sentence_fill material difficulty band is outside the allowed range.",
            )
            review_signal_active = bool(
                review_signal_raw is True
                or str(review_signal_raw).strip().lower() in {"true", "1", "yes", "on"}
            )
            review_penalty_threshold = self._coerce_float(max_total_penalty_raw)
            if review_penalty_threshold is None:
                review_penalty_threshold = 0.55
            review_readiness_threshold = self._coerce_float(min_readiness_raw)
            if review_readiness_threshold is None:
                review_readiness_threshold = 0.45
            review_triggered = bool(
                review_signal_active
                and readiness_score >= review_readiness_threshold
                and total_penalty >= review_penalty_threshold
            )
            checks["sentence_fill_review_like_risk"] = self._build_contract_gated_check(
                active=review_signal_active,
                passed=not review_triggered,
                source=review_signal_source,
                actual={
                    "readiness_score": round(readiness_score, 4),
                    "total_penalty": total_penalty,
                    "difficulty_band": difficulty_band,
                },
                threshold={
                    "readiness_score": round(review_readiness_threshold, 4),
                    "total_penalty": round(review_penalty_threshold, 4),
                },
                reason=(
                    "high_risk_but_not_high_difficulty"
                    if difficulty_band != "hard" and total_penalty >= review_penalty_threshold
                    else "high_readiness_high_penalty"
                ),
            )
            if review_triggered:
                warnings.append("sentence_fill material is structurally usable, but penalty risk remains elevated.")

        if reference_blank_position:
            aligned = blank_position == reference_blank_position
            checks["sentence_fill_blank_position_alignment"] = {
                "passed": aligned,
                "reference_blank_position": reference_blank_position,
                "generated_blank_position": blank_position,
                "source": reference_blank_position_source,
            }
            if not aligned:
                self._append_unique_error(errors, "position_function_mismatch")

        function_type = constraints["function_type"]
        function_type_source = constraints["function_type_source"]
        reference_anchor_mode = constraints["reference_anchor"]
        bidirectional_contract = constraints["bidirectional_check"]
        checks["sentence_fill_function_type"] = {
            "passed": bool(function_type),
            "function_type": function_type,
            "source": function_type_source,
        }
        checks["sentence_fill_reference_anchor_contract"] = {
            "reference_anchor": reference_anchor_mode,
            "source": constraints["reference_anchor_source"],
        }
        checks["sentence_fill_bidirectional_contract"] = {
            "bidirectional_check": bidirectional_contract,
            "source": constraints["bidirectional_check_source"],
        }

        runtime_function_type = constraints["runtime_function_type"]
        contract_function_type = constraints["contract_function_type"]
        if runtime_function_type and contract_function_type:
            checks["sentence_fill_function_alignment"] = {
                "passed": runtime_function_type == contract_function_type,
                "runtime_function_type": runtime_function_type,
                "contract_function_type": contract_function_type,
            }
            if runtime_function_type != contract_function_type:
                self._append_unique_error(errors, "position_function_mismatch")

        runtime_position = constraints["runtime_blank_position"]
        contract_position = constraints["contract_blank_position"]
        if runtime_position and contract_position:
            checks["sentence_fill_position_alignment"] = {
                "passed": runtime_position == contract_position,
                "runtime_blank_position": runtime_position,
                "contract_blank_position": contract_position,
            }
            if runtime_position != contract_position:
                self._append_unique_error(errors, "position_function_mismatch")

        directional = self._sentence_fill_directional_validity(
            candidate_text=correct_text,
            previous_text=previous_text,
            next_text=next_text,
        )
        checks["sentence_fill_bidirectional_runtime"] = {
            **directional,
            "blank_marker": blank_marker,
        }

        anchor_support = self._sentence_fill_reference_anchor_support(
            candidate_text=correct_text,
            previous_text=previous_text,
        )
        checks["sentence_fill_reference_anchor"] = anchor_support

        anchor_required = function_type == "reference_summary" or reference_anchor_mode == "required"
        if anchor_required and (not anchor_support["has_anchor"] or not anchor_support["passed"]):
            self._append_unique_error(errors, "reference_anchor_missing")

        if function_type == "bridge":
            analysis_has_bridge = any(token in generated_question.analysis for token in ("??", "??", "??", "??"))
            checks["sentence_fill_bridge_reasoning"] = {
                "passed": analysis_has_bridge,
                "function_type": function_type,
                "source": function_type_source,
            }
            if not analysis_has_bridge:
                warnings.append("reference fill question is bridge-oriented, but analysis does not clearly explain both-side linkage.")

        if function_type == "topic_intro":
            if blank_position and blank_position != "opening":
                self._append_unique_error(errors, "position_function_mismatch")
            if (
                self._sentence_fill_has_conclusion_marker(correct_text)
                or self._sentence_fill_has_countermeasure_marker(correct_text)
                or anchor_support["has_anchor"]
            ):
                self._append_unique_error(errors, "position_function_mismatch")
            if next_text and not directional["next_valid"]:
                self._append_unique_error(errors, "position_function_mismatch")

        if function_type == "summary" and blank_position == "opening":
            if (
                self._sentence_fill_has_conclusion_marker(correct_text)
                or self._sentence_fill_has_countermeasure_marker(correct_text)
                or anchor_support["has_anchor"]
            ):
                self._append_unique_error(errors, "position_function_mismatch")
            if next_text and not directional["next_valid"]:
                self._append_unique_error(errors, "function_scope_mismatch")

        if function_type == "carry_previous":
            if blank_position and blank_position != "middle":
                self._append_unique_error(errors, "position_function_mismatch")
            if not directional["previous_valid"]:
                self._append_unique_error(errors, "position_function_mismatch")
            if self._sentence_fill_has_conclusion_marker(correct_text) or self._sentence_fill_has_countermeasure_marker(correct_text):
                self._append_unique_error(errors, "function_scope_mismatch")
            if next_text and not directional["next_valid"]:
                warnings.append("carry_previous option links weakly to following context.")

        if function_type == "lead_next":
            if blank_position and blank_position != "middle":
                self._append_unique_error(errors, "position_function_mismatch")
            if not directional["next_valid"]:
                self._append_unique_error(errors, "position_function_mismatch")
            if anchor_support["has_anchor"] and not self._sentence_fill_has_forward_signal(correct_text):
                self._append_unique_error(errors, "function_scope_mismatch")
            if self._sentence_fill_has_conclusion_marker(correct_text):
                self._append_unique_error(errors, "function_scope_mismatch")
            if previous_text and not directional["previous_valid"]:
                warnings.append("lead_next option links weakly to previous context.")

        if function_type == "bridge":
            if blank_position and blank_position != "middle":
                self._append_unique_error(errors, "position_function_mismatch")
            if not directional["previous_valid"] or not directional["next_valid"]:
                self._append_unique_error(errors, "bidirectional_failure")
            if self._sentence_fill_has_conclusion_marker(correct_text) or self._sentence_fill_has_countermeasure_marker(correct_text):
                self._append_unique_error(errors, "function_scope_mismatch")

        if function_type == "reference_summary":
            if blank_position not in {"middle", "inserted", "mixed"}:
                self._append_unique_error(errors, "position_function_mismatch")
            if not directional["previous_valid"] or not directional["next_valid"]:
                self._append_unique_error(errors, "bidirectional_failure")
            if not anchor_support["has_anchor"] or not anchor_support["passed"]:
                self._append_unique_error(errors, "reference_anchor_missing")

        if function_type in {"conclusion", "summary"} and blank_position == "ending":
            if self._sentence_fill_has_forward_signal(correct_text) or any(token in correct_text for token in ("渚嬪", "姣斿", "棣栧厛", "鍏舵")):
                self._append_unique_error(errors, "position_function_mismatch")
            if not self._sentence_fill_has_conclusion_marker(correct_text) and next_text:
                warnings.append("ending summary lacks a strong closure marker.")

        if function_type == "countermeasure":
            if blank_position and blank_position != "ending":
                self._append_unique_error(errors, "position_function_mismatch")
            if not self._sentence_fill_has_countermeasure_marker(correct_text):
                self._append_unique_error(errors, "position_function_mismatch")
            elif not self._sentence_fill_has_specific_action(correct_text):
                self._append_unique_error(errors, "function_scope_mismatch")

        if isinstance(bidirectional_contract, dict):
            require_previous = bidirectional_contract.get("previous_valid") is True
            require_next = bidirectional_contract.get("next_valid") is True
            if (require_previous and not directional["previous_valid"]) or (require_next and not directional["next_valid"]):
                self._append_unique_error(errors, "bidirectional_failure")
        return errors, warnings, checks

    def _build_difficulty_review(self, difficulty_fit: dict[str, Any]) -> dict[str, Any]:
        if not difficulty_fit:
            return {}
        deviations = difficulty_fit.get("deviations") or []
        return {
            "in_range": difficulty_fit.get("in_range", True),
            "deviation_count": len(deviations),
            "deviations": deviations,
        }

    def _is_sentence_fill_soft_difficulty_miss(self, difficulty_review: dict[str, Any]) -> bool:
        deviations = difficulty_review.get("deviations") or []
        if not deviations:
            return False
        parsed: list[dict[str, float | str]] = []
        for deviation in deviations:
            if isinstance(deviation, dict):
                metric = str(deviation.get("metric") or "").strip()
                actual = self._coerce_float(deviation.get("actual"))
                target_min = self._coerce_float(deviation.get("target_min"))
            else:
                metric = str(getattr(deviation, "metric", "") or "").strip()
                actual = self._coerce_float(getattr(deviation, "actual", None))
                target_min = self._coerce_float(getattr(deviation, "target_min", None))
            if not metric:
                return False
            parsed.append(
                {
                    "metric": metric,
                    "actual": actual if actual is not None else 0.0,
                    "target_min": target_min if target_min is not None else 0.0,
                }
            )

        if any(item["metric"] != "reasoning_depth" for item in parsed):
            return False

        for item in parsed:
            target_min = float(item["target_min"])
            actual = float(item["actual"])
            if actual >= target_min:
                continue
            if target_min - actual > 0.1:
                return False
        return True

    def _is_center_understanding_soft_difficulty_miss(self, difficulty_review: dict[str, Any]) -> bool:
        deviations = difficulty_review.get("deviations") or []
        if not deviations:
            return False
        parsed: list[dict[str, float | str]] = []
        for deviation in deviations:
            if isinstance(deviation, dict):
                metric = str(deviation.get("metric") or "").strip()
                actual = self._coerce_float(deviation.get("actual"))
                target_min = self._coerce_float(deviation.get("target_min"))
                target_max = self._coerce_float(deviation.get("target_max"))
            else:
                metric = str(getattr(deviation, "metric", "") or "").strip()
                actual = self._coerce_float(getattr(deviation, "actual", None))
                target_min = self._coerce_float(getattr(deviation, "target_min", None))
                target_max = self._coerce_float(getattr(deviation, "target_max", None))
            if not metric:
                return False
            parsed.append(
                {
                    "metric": metric,
                    "actual": actual if actual is not None else 0.0,
                    "target_min": target_min if target_min is not None else 0.0,
                    "target_max": target_max if target_max is not None else 0.0,
                }
            )
        allowed_metrics = {"reasoning_depth", "complexity", "distractor_similarity"}
        if any(str(item["metric"]) not in allowed_metrics for item in parsed):
            return False
        for item in parsed:
            metric = str(item["metric"])
            target_min = float(item["target_min"])
            target_max = float(item["target_max"])
            actual = float(item["actual"])
            if metric == "distractor_similarity":
                if actual <= target_max:
                    continue
                if actual - target_max > 0.08:
                    return False
                continue
            if actual >= target_min:
                continue
            if target_min - actual > 0.12:
                return False
        return True

    def _is_sentence_order_soft_difficulty_miss(
        self,
        *,
        difficulty_review: dict[str, Any],
        checks: dict[str, Any],
    ) -> bool:
        deviations = difficulty_review.get("deviations") or []
        if not deviations:
            return False
        parsed: list[dict[str, float | str]] = []
        for deviation in deviations:
            if isinstance(deviation, dict):
                metric = str(deviation.get("metric") or "").strip()
                actual = self._coerce_float(deviation.get("actual"))
                target_min = self._coerce_float(deviation.get("target_min"))
                target_max = self._coerce_float(deviation.get("target_max"))
            else:
                metric = str(getattr(deviation, "metric", "") or "").strip()
                actual = self._coerce_float(getattr(deviation, "actual", None))
                target_min = self._coerce_float(getattr(deviation, "target_min", None))
                target_max = self._coerce_float(getattr(deviation, "target_max", None))
            if not metric:
                return False
            parsed.append(
                {
                    "metric": metric,
                    "actual": actual if actual is not None else 0.0,
                    "target_min": target_min if target_min is not None else 0.0,
                    "target_max": target_max if target_max is not None else 0.0,
                }
            )
        allowed_metrics = {"reasoning_depth", "complexity", "distractor_similarity"}
        if any(str(item["metric"]) not in allowed_metrics for item in parsed):
            return False

        structural_keys = (
            "sentence_order_unique_opener",
            "sentence_order_binding_pairs",
            "sentence_order_closure",
            "sentence_order_exchange_risk",
            "sentence_order_multi_path_risk",
            "sentence_order_function_overlap",
            "sentence_order_unique_answer_strength",
            "sentence_order_single_truth_option",
            "sentence_order_answer_binding",
            "sentence_order_analysis_binding",
            "sentence_order_head_enforcement",
            "sentence_order_tail_enforcement",
            "sentence_order_binding_enforcement",
        )
        for key in structural_keys:
            payload = checks.get(key)
            if not isinstance(payload, dict):
                continue
            status = str(payload.get("status") or "").strip()
            if status and status != "active":
                continue
            if payload.get("passed") is not True:
                return False

        for item in parsed:
            metric = str(item["metric"])
            target_min = float(item["target_min"])
            target_max = float(item["target_max"])
            actual = float(item["actual"])
            if metric == "distractor_similarity":
                if actual <= target_max:
                    continue
                if actual - target_max > 0.06:
                    return False
                continue
            if actual >= target_min:
                continue
            if target_min - actual > 0.08:
                return False
        return True

    def _compute_material_overlap_ratio(self, *, material_text: str, question_text: str) -> float:
        material_tokens = self._extract_tokens(material_text)
        question_tokens = self._extract_tokens(question_text)
        if not material_tokens or not question_tokens:
            return 0.0
        overlap = material_tokens & question_tokens
        return round(len(overlap) / max(len(question_tokens), 1), 4)

    def _split_material_units(self, material_text: str) -> list[str]:
        text = (material_text or "").strip()
        if not text:
            return []
        text = text.replace("\r\n", "\n").replace("\r", "\n")
        segments = re.split(r"(?<=[。！？!?；;])\s*|\n+", text)
        return [segment.strip() for segment in segments if segment and segment.strip()]

    def _normalize_material_signature(self, text: str) -> str:
        return re.sub(r"\s+", "", (text or "").strip())

    def _find_repeated_material_units(self, material_text: str) -> list[str]:
        units = self._split_material_units(material_text)
        seen: list[str] = []
        repeated: list[str] = []
        for unit in units:
            signature = self._normalize_material_signature(unit)
            if len(signature) < 10:
                continue
            if any(signature == existing or signature in existing or existing in signature for existing in seen):
                repeated.append(unit)
                continue
            seen.append(signature)
        return repeated

    def _find_stitched_material_pairs(self, material_text: str) -> list[str]:
        units = self._split_material_units(material_text)
        stitched: list[str] = []
        for previous, current in zip(units, units[1:]):
            prev_sig = self._normalize_material_signature(previous)
            curr_sig = self._normalize_material_signature(current)
            if len(prev_sig) < 12 or len(curr_sig) < 12:
                continue
            if prev_sig in curr_sig or curr_sig in prev_sig:
                stitched.append(f"{previous} || {current}")
        return stitched

    def _compute_support_profile(self, *, evidence_text: str, candidate_text: str) -> dict[str, Any]:
        evidence_tokens = self._extract_tokens(evidence_text)
        candidate_tokens = self._extract_tokens(candidate_text)
        if not evidence_tokens or not candidate_tokens:
            return {
                "shared_token_count": 0,
                "candidate_token_count": len(candidate_tokens),
                "supported_token_ratio": 0.0,
            }
        overlap = evidence_tokens & candidate_tokens
        return {
            "shared_token_count": len(overlap),
            "candidate_token_count": len(candidate_tokens),
            "supported_token_ratio": round(len(overlap) / max(len(candidate_tokens), 1), 4),
        }

    def _extract_explicit_answer_letters(self, analysis: str) -> list[str]:
        if not analysis:
            return []
        found: set[str] = set()
        for pattern in self.EXPLICIT_ANSWER_PATTERNS:
            for match in pattern.findall(analysis):
                found.add(str(match).upper())
        return sorted(found)

    def _extract_tokens(self, text: str) -> set[str]:
        clean = (text or "").strip().lower()
        if not clean:
            return set()
        words = set(re.findall(r"[a-z0-9]{2,}", clean))
        cjk_chars = [char for char in clean if "\u4e00" <= char <= "\u9fff"]
        bigrams = {"".join(cjk_chars[index : index + 2]) for index in range(len(cjk_chars) - 1)}
        stop_bigrams = {"??", "??", "??", "??", "??", "??", "??", "??", "??", "??"}
        return {token for token in words | bigrams if token and token not in stop_bigrams}

    def _count_sortable_units_from_material(self, material_text: str) -> int:
        text = (material_text or "").strip()
        if not text:
            return 0
        sortable_block = text.split("\n\n")[-1].strip()
        units = self._extract_order_material_units(sortable_block)
        normalized_units = self._normalize_sentence_order_units_to_six(units) or units
        return len(normalized_units)

    def _extract_order_option_unit_counts(self, options: dict[str, str]) -> list[int]:
        counts: list[int] = []
        for value in options.values():
            text = (value or "").strip()
            if not text:
                continue
            circled = re.findall(r"[\u2460-\u2469]", text)
            if circled:
                counts.append(len(set(circled)))
                continue
            digits = re.findall(r"\d+", text)
            if digits:
                if len(digits) == 1 and len(digits[0]) > 1:
                    counts.append(len(set(digits[0])))
                else:
                    counts.append(len(set(digits)))
        return counts

    def _extract_order_sequence(self, text: str) -> list[int]:
        raw = str(text or "").strip()
        if not raw:
            return []
        circled_map = {
            "\u2460": 1,
            "\u2461": 2,
            "\u2462": 3,
            "\u2463": 4,
            "\u2464": 5,
            "\u2465": 6,
            "\u2466": 7,
            "\u2467": 8,
            "\u2468": 9,
            "\u2469": 10,
        }
        circled = [circled_map[ch] for ch in raw if ch in circled_map]
        if circled:
            return circled
        digit_groups = re.findall(r"\d+", raw)
        if not digit_groups:
            return []
        if len(digit_groups) == 1 and len(digit_groups[0]) > 1:
            return [int(ch) for ch in digit_groups[0]]
        return [int(value) for value in digit_groups]

    def _extract_reference_order_sequences(self, text: str) -> list[list[int]]:
        raw = str(text or "")
        if not raw:
            return []
        sequences: list[list[int]] = []
        for match in re.findall(r"[\u2460-\u2469]{2,10}", raw):
            sequence = self._extract_order_sequence(match)
            if len(sequence) >= 2:
                sequences.append(sequence)
        return sequences

    def _extract_order_sequences_from_text(self, text: str) -> list[list[int]]:
        raw = str(text or "")
        if not raw:
            return []
        sequences: list[list[int]] = []
        for match in re.findall(r"[\u2460-\u2469]{6,10}", raw):
            sequence = self._extract_order_sequence(match)
            if len(sequence) >= 6:
                sequences.append(sequence[:6])
        return sequences

    def _detect_blank_position(self, material_text: str) -> str:
        text = (material_text or "").strip()
        if not text:
            return ""
        marker_index = -1
        for marker in ("____", "___", "[BLANK]", "?  ?", "( )", "??"):
            idx = text.find(marker)
            if idx >= 0:
                marker_index = idx
                break
        if marker_index < 0:
            return ""
        ratio = marker_index / max(len(text), 1)
        if ratio <= 0.22:
            return "opening"
        if ratio >= 0.70:
            return "ending"
        return "middle"

    def _extract_order_material_units(self, material_text: str) -> list[str]:
        text = (material_text or "").strip()
        if not text:
            return []
        sortable_block = text.split("\n\n")[-1].strip()
        if not sortable_block:
            return []
        enumerated_parts = re.split(r"[\u2460-\u2469]\s*", sortable_block)
        units = [part.strip() for part in enumerated_parts if part and part.strip()]
        if units:
            return units
        return [item.strip() for item in re.split(r"(?<=[。！？!?；;])\s*", sortable_block) if item.strip()]

    def _normalize_sentence_order_units_to_six(self, units: list[str]) -> list[str] | None:
        cleaned = [unit.strip() for unit in units if unit and unit.strip()]
        if len(cleaned) < 6 or len(cleaned) > 12:
            return None
        if len(cleaned) == 6:
            return cleaned
        merge_need = len(cleaned) - 6
        if merge_need > len(cleaned) // 2:
            return None
        pair_scores: list[tuple[float, int]] = []
        for index in range(len(cleaned) - 1):
            left = cleaned[index]
            right = cleaned[index + 1]
            score = 0.0
            if right.startswith(("??", "??", "??", "??", "??", "?", "?", "??", "??", "??")):
                score += 0.30
            if len(left) <= 28 and len(right) <= 28:
                score += 0.12
            if len(left) + len(right) > 96:
                score -= 0.18
            pair_scores.append((score, index))
        pair_scores.sort(reverse=True)
        selected: list[int] = []
        used: set[int] = set()
        for score, index in pair_scores:
            if index in used or index + 1 in used:
                continue
            selected.append(index)
            used.add(index)
            used.add(index + 1)
            if len(selected) == merge_need:
                break
        if len(selected) != merge_need:
            return None
        selected_set = set(selected)
        normalized: list[str] = []
        index = 0
        while index < len(cleaned):
            if index in selected_set:
                normalized.append(self._merge_sentence_order_unit_pair(cleaned[index], cleaned[index + 1]))
                index += 2
                continue
            normalized.append(cleaned[index])
            index += 1
        if len(normalized) != 6:
            return None
        return normalized

    def _merge_sentence_order_unit_pair(self, left: str, right: str) -> str:
        left_clean = left.strip()
        right_clean = right.strip()
        if not left_clean:
            return right_clean
        separator = ""
        if not left_clean.endswith(("?", "?", "?", "!", "?", "?", ";", "?", ",", "?", ":")):
            separator = "?"
        return f"{left_clean}{separator}{right_clean}".strip()

    def _sentence_order_unit_sentence_count(self, unit: str) -> int:
        text = (unit or "").strip()
        if not text:
            return 0
        line_units = [line.strip() for line in text.splitlines() if line.strip()]
        if len(line_units) > 1:
            return len(line_units)
        terminal_count = len(re.findall(r"[。！？!?；;]", text))
        return max(1, terminal_count)

    def _sentence_order_unit_role(self, unit: str, *, is_last: bool = False) -> str:
        text = (unit or "").strip()
        if not text:
            return "empty"
        summary_markers = ("??", "??", "??", "??", "??", "??")
        action_markers = ("??", "??", "??", "??", "?", "??")
        problem_markers = ("??", "??", "??", "??")
        definition_markers = ("??", "??", "?")
        opening_markers = ("???", "????", "????", "??", "??")
        dependent_markers = ("?", "??", "??", "?", "?")
        connector_markers = ("??", "??", "??", "??", "??", "??", "??")
        timeline_markers = ("??", "??", "??", "??", "??", "??")
        viewpoint_markers = ("???", "???", "???", "???", "????")
        if any(token in text for token in summary_markers):
            return "summary"
        if any(token in text for token in action_markers):
            return "action"
        if any(token in text for token in problem_markers):
            return "problem"
        if any(token in text for token in definition_markers):
            return "definition"
        if text.startswith(opening_markers):
            return "opening_anchor"
        if text.startswith(dependent_markers):
            return "dependent"
        if any(token in text for token in connector_markers):
            return "connector"
        if any(token in text for token in timeline_markers):
            return "timeline"
        if any(token in text for token in viewpoint_markers):
            return "viewpoint"
        if is_last:
            return "tail_statement"
        return "statement"

    def _sentence_order_unit_opener_score(self, unit: str, *, index: int) -> float:
        text = (unit or "").strip()
        if not text:
            return 0.0
        score = 0.26
        opening_markers = ("???", "????", "????", "??", "???")
        definition_markers = ("??", "??", "?")
        timeline_markers = ("??", "??", "??", "??", "??")
        problem_markers = ("??", "??", "??", "??")
        viewpoint_markers = ("???", "???", "???", "???", "????")
        summary_markers = ("??", "??", "??", "??", "??", "??")
        action_markers = ("??", "??", "??", "??", "?", "??")
        dependent_markers = ("?", "??", "??", "?", "?")
        example_markers = ("??", "??", "??", "??")
        connector_markers = ("??", "??", "??", "??", "??")
        if text.startswith(opening_markers):
            score += 0.18
        if any(token in text for token in definition_markers):
            score += 0.34
        if any(token in text for token in timeline_markers):
            score += 0.24
        if any(token in text for token in problem_markers):
            score += 0.24
        if any(token in text for token in viewpoint_markers):
            score += 0.16
        if any(token in text for token in summary_markers + action_markers):
            score -= 0.20
        if text.startswith(dependent_markers):
            score -= 0.30
        if any(token in text for token in example_markers):
            score -= 0.22
        if any(token in text for token in connector_markers):
            score -= 0.10
        if index == 0 and any(token in text for token in timeline_markers):
            score += 0.16
        if index == 0:
            score += 0.06
        return round(max(0.0, min(1.0, score)), 4)

    def _build_sentence_order_uniqueness_profile(self, material_text: str) -> dict[str, Any]:
        units = self._extract_order_material_units(material_text)
        units = self._normalize_sentence_order_units_to_six(units) or units
        if not units:
            return {
                "unique_opener_score": 0.0,
                "binding_pair_count": 0,
                "function_overlap_score": 1.0,
                "exchange_risk": 1.0,
                "multi_path_risk": 1.0,
                "context_closure_score": 0.0,
                "has_closing_role": False,
                "unique_answer_strength": 0.0,
            }

        opener_scores = sorted(
            (self._sentence_order_unit_opener_score(unit, index=index) for index, unit in enumerate(units)),
            reverse=True,
        )
        best = opener_scores[0]
        second = opener_scores[1] if len(opener_scores) > 1 else 0.0
        unique_opener_score = round(max(0.0, min(1.0, 0.68 * best + 0.32 * max(0.0, best - second))), 4)

        dependent_markers = ("?", "??", "??", "?", "?")
        connector_markers = ("??", "??", "??", "??", "??", "??", "??")
        bridge_markers = ("??", "??", "??", "??")
        problem_markers = ("??", "??", "??", "??")
        closing_markers = ("??", "??", "??", "??", "??", "??", "??", "??", "??", "??")
        action_markers = ("??", "??", "??", "??", "?", "??")

        binding_pair_count = 0
        for index in range(len(units) - 1):
            current = units[index]
            nxt = units[index + 1]
            if nxt.startswith(dependent_markers):
                binding_pair_count += 1
                continue
            if any(token in nxt for token in connector_markers + bridge_markers):
                binding_pair_count += 1
                continue
            if any(token in current for token in problem_markers) and any(token in nxt for token in closing_markers + action_markers):
                binding_pair_count += 1
                continue
        binding_pair_count = min(binding_pair_count, 4)

        roles = [self._sentence_order_unit_role(unit, is_last=index == len(units) - 1) for index, unit in enumerate(units)]
        role_counts: dict[str, int] = {}
        for role in roles:
            role_counts[role] = role_counts.get(role, 0) + 1
        duplicate_pairs = sum(max(0, count - 1) for count in role_counts.values())
        directive_density = sum(1 for unit in units if any(token in unit for token in action_markers)) / max(len(units), 1)
        function_overlap_score = round(
            max(0.0, min(1.0, 0.72 * (duplicate_pairs / max(len(units) - 1, 1)) + 0.28 * directive_density)),
            4,
        )

        has_closing_role = any(role in {"summary", "action", "tail_statement"} for role in roles[-2:]) or any(
            token in units[-1] for token in closing_markers
        )
        context_closure_score = round(
            max(
                0.0,
                min(
                    1.0,
                    0.34 * (1.0 if has_closing_role else 0.25)
                    + 0.24 * min(1.0, binding_pair_count / 3)
                    + 0.22 * unique_opener_score
                    + 0.20 * (1 - function_overlap_score),
                ),
            ),
            4,
        )
        exchange_risk = round(
            max(
                0.0,
                min(
                    1.0,
                    0.32 * function_overlap_score
                    + 0.22 * (1 - min(1.0, binding_pair_count / 3))
                    + 0.20 * (1 - unique_opener_score)
                    + 0.14 * (1 - context_closure_score)
                    + 0.12 * min(1.0, sum(1 for unit in units if any(token in unit for token in connector_markers)) / max(len(units), 1)),
                ),
            ),
            4,
        )
        multi_path_risk = round(
            max(
                0.0,
                min(
                    1.0,
                    0.34 * exchange_risk
                    + 0.22 * function_overlap_score
                    + 0.18 * (1 - unique_opener_score)
                    + 0.14 * (1 - min(1.0, binding_pair_count / 3))
                    + 0.12 * (1 - context_closure_score),
                ),
            ),
            4,
        )
        unique_answer_strength = round(
            max(
                0.0,
                min(
                    1.0,
                    0.30 * unique_opener_score
                    + 0.22 * min(1.0, binding_pair_count / 3)
                    + 0.24 * context_closure_score
                    + 0.14 * (1 - exchange_risk)
                    + 0.10 * (1 - multi_path_risk),
                ),
            ),
            4,
        )
        return {
            "unique_opener_score": unique_opener_score,
            "binding_pair_count": binding_pair_count,
            "function_overlap_score": function_overlap_score,
            "exchange_risk": exchange_risk,
            "multi_path_risk": multi_path_risk,
            "context_closure_score": context_closure_score,
            "has_closing_role": has_closing_role,
            "unique_answer_strength": unique_answer_strength,
        }
