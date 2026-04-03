from __future__ import annotations

import re
from typing import Any, Callable

from app.schemas.item import GeneratedQuestion, ValidationResult


ValidatorFn = Callable[[GeneratedQuestion, dict[str, Any]], tuple[list[str], list[str], dict[str, Any]]]


class QuestionValidatorService:
    EXPLICIT_ANSWER_PATTERNS = (
        re.compile(r"正确答案(?:为|是)?\s*([A-F])", flags=re.IGNORECASE),
        re.compile(r"答案(?:为|是)?\s*([A-F])", flags=re.IGNORECASE),
        re.compile(r"故正确答案(?:为|是)?\s*([A-F])", flags=re.IGNORECASE),
        re.compile(r"应选\s*([A-F])", flags=re.IGNORECASE),
        re.compile(r"选项\s*([A-F])\s*(?:最为|最|是)(?:正确|恰当|符合题意|为正确答案)", flags=re.IGNORECASE),
        re.compile(r"([A-F])\s*项\s*(?:最为|最|是)(?:正确|恰当|符合题意|为正确答案)", flags=re.IGNORECASE),
        re.compile(r"[（(]\s*([A-F])\s*[）)]\s*(?:正确|最恰当|符合题意)", flags=re.IGNORECASE),
    )

    def __init__(self) -> None:
        self.registry: dict[str, ValidatorFn] = {
            "main_idea": self._validate_main_idea,
            "continuation": self._validate_continuation,
            "sentence_order": self._validate_sentence_order,
            "sentence_fill": self._validate_sentence_fill,
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
        difficulty_fit: dict[str, Any] | Any | None = None,
        source_question: dict[str, Any] | None = None,
        source_question_analysis: dict[str, Any] | None = None,
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
            "business_subtype": business_subtype,
            "source_question": source_question or {},
            "source_question_analysis": source_question_analysis or {},
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
            errors.append("difficulty projection is outside the target profile range.")

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
        answer_mentioned = answer.lower() in lower_analysis or "正确" in analysis
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
            "作为ai",
            "作为一个ai",
            "根据提供的材料生成",
            "以下是",
            "本题答案是",
            "生成一道",
            "请选择",
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
            anchor_support = self._compute_support_profile(
                evidence_text=answer_anchor_text,
                candidate_text=correct_option_text,
            )
            checks["sentence_fill_anchor_grounding"] = {
                "passed": anchor_support["supported_token_ratio"] >= 0.2 or anchor_support["shared_token_count"] >= 2,
                "anchor_text": answer_anchor_text,
                **anchor_support,
            }
            if correct_option_text and not checks["sentence_fill_anchor_grounding"]["passed"]:
                errors.append("sentence_fill correct option is not sufficiently grounded in the removed source span.")
        return errors, warnings, checks

    def _validate_main_idea(
        self,
        generated_question: GeneratedQuestion,
        context: dict[str, Any],
    ) -> tuple[list[str], list[str], dict[str, Any]]:
        stem = generated_question.stem.strip()
        prompt_markers = ("主旨", "标题", "概括", "中心", "最适合", "意在", "强调")
        exam_style = any(marker in stem for marker in prompt_markers)
        business_subtype = context.get("business_subtype")
        material_text = context.get("material_text") or ""
        checks = {
            "main_idea_single_answer": {"passed": bool(generated_question.answer)},
            "main_idea_exam_style_prompt": {"passed": exam_style},
        }
        warnings = [] if exam_style else ["main_idea stem does not look like a standard exam-style summary/title prompt."]
        errors: list[str] = []

        is_title_selection = business_subtype == "title_selection" or "标题" in stem
        if is_title_selection:
            options = generated_question.options or {}
            correct_text = (options.get(generated_question.answer or "", "") or "").strip()
            avg_len = round(sum(len((value or "").strip()) for value in options.values()) / max(len(options), 1), 2) if options else 0
            long_sentence_like = bool(correct_text and (len(correct_text) >= 24 or "，" in correct_text or correct_text.count("的") >= 3))
            meeting_markers = [
                "政府工作报告",
                "会议高度评价",
                "审议并批准",
                "部署今后一段时期工作",
                "过去一年和“十四五”时期",
            ]
            marker_hits = [marker for marker in meeting_markers if marker in material_text]
            fragment_heavy = bool(
                options
                and all(
                    len((value or "").strip()) >= 14
                    or any(token in (value or "") for token in ("和", "及", "与", "的"))
                    for value in options.values()
                )
            )
            checks["title_selection_title_like"] = {
                "passed": not long_sentence_like,
                "correct_option_length": len(correct_text),
                "correct_option_text": correct_text,
            }
            checks["title_selection_material_fit"] = {
                "passed": len(marker_hits) < 2,
                "marker_hits": marker_hits,
            }
            checks["title_selection_option_diversity"] = {
                "passed": not fragment_heavy,
                "avg_option_length": avg_len,
            }
            if long_sentence_like:
                errors.append("title_selection correct option reads like a long summary sentence rather than a title.")
            if len(marker_hits) >= 2:
                errors.append("title_selection material is too close to a meeting-summary or report-style passage and should not be used directly.")
            if fragment_heavy:
                warnings.append("title_selection options are overly uniform and mostly look like fragment extraction rather than layered title design.")

        return errors, warnings, checks

    def _validate_continuation(
        self,
        generated_question: GeneratedQuestion,
        context: dict[str, Any],
    ) -> tuple[list[str], list[str], dict[str, Any]]:
        errors: list[str] = []
        stem = generated_question.stem.strip()
        continuation_markers = ("接在", "之后", "接下来", "最可能", "下文")
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

    def _validate_sentence_order(
        self,
        generated_question: GeneratedQuestion,
        context: dict[str, Any],
    ) -> tuple[list[str], list[str], dict[str, Any]]:
        stem = generated_question.stem.strip()
        material_text = str(context.get("material_text") or "")
        source_analysis = context.get("source_question_analysis") or {}
        structure_constraints = source_analysis.get("structure_constraints") or {}

        has_order_signal = any(
            any(token in value for token in ("①", "②", "③", "④", "⑤", "⑥", "⑦", "⑧", "A.", "B.", "首句", "尾句"))
            for value in generated_question.options.values()
        )
        stem_exam_style = any(token in stem for token in ("排序", "语句", "排列", "顺序"))
        material_unit_count = self._count_sortable_units_from_material(material_text)
        option_unit_counts = sorted(self._extract_order_option_unit_counts(generated_question.options))
        reference_unit_count = int(structure_constraints.get("sortable_unit_count") or 0)
        option_unit_count = option_unit_counts[-1] if option_unit_counts else 0
        orderability = self._build_sentence_order_uniqueness_profile(material_text)
        expected_binding_pair_count = int(structure_constraints.get("expected_binding_pair_count") or 2)
        expected_unique_answer_strength = float(structure_constraints.get("expected_unique_answer_strength") or 0.0)
        correct_order = list(generated_question.correct_order or [])
        original_sentences = list(generated_question.original_sentences or [])
        option_orders = {
            key: self._extract_order_sequence(value)
            for key, value in (generated_question.options or {}).items()
        }
        answer = str(generated_question.answer or "").strip().upper()
        analysis_orders = self._extract_order_sequences_from_text(generated_question.analysis or "")

        checks = {
            "sentence_order_signal": {"passed": has_order_signal},
            "sentence_order_exam_style_prompt": {"passed": stem_exam_style},
            "sentence_order_material_unit_count": {"passed": material_unit_count >= 4, "count": material_unit_count},
            "sentence_order_option_unit_counts": {"passed": bool(option_unit_counts), "counts": option_unit_counts},
            "sentence_order_unique_opener": {"passed": orderability["unique_opener_score"] >= 0.55, "score": orderability["unique_opener_score"]},
            "sentence_order_binding_pairs": {"passed": orderability["binding_pair_count"] >= expected_binding_pair_count, "count": orderability["binding_pair_count"], "expected": expected_binding_pair_count},
            "sentence_order_closure": {"passed": orderability["has_closing_role"], "context_closure_score": orderability["context_closure_score"]},
            "sentence_order_exchange_risk": {"passed": orderability["exchange_risk"] <= 0.38, "score": orderability["exchange_risk"]},
            "sentence_order_multi_path_risk": {"passed": orderability["multi_path_risk"] <= 0.40, "score": orderability["multi_path_risk"]},
            "sentence_order_function_overlap": {"passed": orderability["function_overlap_score"] <= 0.46, "score": orderability["function_overlap_score"]},
            "sentence_order_original_sentences": {"passed": len(original_sentences) == 6, "count": len(original_sentences)},
            "sentence_order_correct_order": {"passed": len(correct_order) == 6 and sorted(correct_order) == [1, 2, 3, 4, 5, 6], "value": correct_order},
        }
        errors: list[str] = []
        warnings: list[str] = []

        if not has_order_signal:
            warnings.append("sentence_order options do not show obvious ordering signals.")
        if not stem_exam_style:
            warnings.append("sentence_order stem does not look like a standard ordering prompt.")
        if material_unit_count < 4:
            errors.append("sentence_order material does not preserve enough sortable units.")
        if option_unit_counts and len(set(option_unit_counts)) > 1:
            errors.append("sentence_order options do not use a consistent sortable-unit set.")
        if len(original_sentences) != 6:
            errors.append("sentence_order original_sentences must contain exactly 6 units.")
        if len(correct_order) != 6 or sorted(correct_order) != [1, 2, 3, 4, 5, 6]:
            errors.append("sentence_order correct_order must be a single 6-unit truth source.")
        correct_option_letters = [key for key, sequence in option_orders.items() if sequence == correct_order]
        checks["sentence_order_single_truth_option"] = {"passed": len(correct_option_letters) == 1, "matching_letters": correct_option_letters}
        if len(correct_option_letters) != 1:
            errors.append("sentence_order options must contain exactly one option derived from correct_order.")
        checks["sentence_order_answer_binding"] = {
            "passed": bool(answer and answer in option_orders and option_orders.get(answer) == correct_order),
            "answer": answer,
            "answer_order": option_orders.get(answer),
            "correct_order": correct_order,
        }
        if answer not in option_orders or option_orders.get(answer) != correct_order:
            errors.append("sentence_order answer does not point to the option derived from correct_order.")
        checks["sentence_order_analysis_binding"] = {
            "passed": bool(analysis_orders and analysis_orders[0] == correct_order),
            "analysis_orders": analysis_orders,
            "correct_order": correct_order,
        }
        if not analysis_orders or analysis_orders[0] != correct_order:
            errors.append("sentence_order analysis must explicitly explain the same correct_order as the answer.")
        if len(analysis_orders) > 1 and any(sequence != correct_order for sequence in analysis_orders[1:]):
            errors.append("sentence_order analysis contains conflicting ordering sequences.")
        if reference_unit_count:
            aligned = option_unit_count == reference_unit_count or material_unit_count == reference_unit_count
            checks["sentence_order_reference_unit_alignment"] = {
                "passed": aligned,
                "reference_unit_count": reference_unit_count,
                "generated_option_unit_count": option_unit_count,
                "material_unit_count": material_unit_count,
            }
            if not aligned:
                errors.append(
                    f"sentence_order should preserve the reference sortable-unit count ({reference_unit_count}), but generated result drifted."
                )
        if orderability["unique_opener_score"] < 0.55:
            errors.append("sentence_order material does not show a strong enough unique opener candidate.")
        if orderability["binding_pair_count"] < expected_binding_pair_count:
            errors.append("sentence_order material lacks enough deterministic binding pairs to support a unique-best sequence.")
        if not orderability["has_closing_role"] or orderability["context_closure_score"] < 0.55:
            errors.append("sentence_order material does not form a clear closing or closure role.")
        if orderability["exchange_risk"] > 0.38:
            errors.append("sentence_order material remains too readable after key-unit exchange and is not uniquely orderable enough.")
        if orderability["multi_path_risk"] > 0.40:
            errors.append("sentence_order material admits too many near-plausible ordering paths.")
        if orderability["function_overlap_score"] > 0.46:
            errors.append("sentence_order material has overly similar unit functions, so the options feel interchangeable.")
        if expected_unique_answer_strength:
            unique_strength_ok = orderability["unique_answer_strength"] + 0.06 >= expected_unique_answer_strength
            checks["sentence_order_unique_answer_strength"] = {
                "passed": unique_strength_ok,
                "score": orderability["unique_answer_strength"],
                "expected": expected_unique_answer_strength,
            }
            if not unique_strength_ok:
                errors.append("sentence_order material does not reach the reference question's unique-answer strength.")
        logic_modes = structure_constraints.get("logic_modes") or []
        if "timeline_sequence" in logic_modes:
            analysis_has_timeline = any(token in generated_question.analysis for token in ("先", "后", "随后", "最后", "时间"))
            checks["sentence_order_timeline_reasoning"] = {"passed": analysis_has_timeline}
            if not analysis_has_timeline:
                warnings.append("reference question is timeline-oriented, but analysis does not clearly explain the time/order chain.")
        if "deterministic_binding" in logic_modes:
            analysis_has_binding = any(token in generated_question.analysis for token in ("指代", "捆绑", "关联", "承接"))
            checks["sentence_order_binding_reasoning"] = {"passed": analysis_has_binding}
            if not analysis_has_binding:
                warnings.append("reference question relies on deterministic binding, but analysis does not clearly explain the binding clues.")
        analysis_has_head_tail = any(token in generated_question.analysis for token in ("首句", "尾句", "收束", "总结"))
        checks["sentence_order_head_tail_reasoning"] = {"passed": analysis_has_head_tail}
        if not analysis_has_head_tail:
            warnings.append("sentence_order analysis does not clearly explain opener/closing roles.")
        return errors, warnings, checks

    def _validate_sentence_fill(
        self,
        generated_question: GeneratedQuestion,
        context: dict[str, Any],
    ) -> tuple[list[str], list[str], dict[str, Any]]:
        stem = generated_question.stem
        material_text = str(context.get("material_text") or "")
        source_analysis = context.get("source_question_analysis") or {}
        structure_constraints = source_analysis.get("structure_constraints") or {}

        has_blank_signal = any(token in material_text for token in ("____", "___", "[BLANK]", "（  ）", "( )"))
        has_fill_prompt = any(token in stem for token in ("填入", "依次填入", "横线", "最恰当"))
        blank_position = self._detect_blank_position(material_text)
        reference_blank_position = str(structure_constraints.get("blank_position") or "")

        checks = {
            "sentence_fill_gap_signal": {"passed": has_blank_signal},
            "sentence_fill_exam_style_prompt": {"passed": has_fill_prompt},
            "sentence_fill_blank_position": {"passed": bool(blank_position), "blank_position": blank_position},
        }
        errors: list[str] = []
        warnings: list[str] = []
        if not has_blank_signal:
            errors.append("sentence_fill material does not show an obvious blank marker.")
        if not has_fill_prompt:
            warnings.append("sentence_fill stem does not look like a standard fill-in-the-blank prompt.")
        if reference_blank_position:
            aligned = blank_position == reference_blank_position
            checks["sentence_fill_blank_position_alignment"] = {
                "passed": aligned,
                "reference_blank_position": reference_blank_position,
                "generated_blank_position": blank_position,
            }
            if not aligned:
                errors.append(
                    f"sentence_fill should preserve the reference blank position ({reference_blank_position}), but generated material drifted."
                )
        function_type = str(structure_constraints.get("function_type") or "")
        if function_type == "bridge_both_sides":
            analysis_has_bridge = any(token in generated_question.analysis for token in ("承上启下", "承前启后", "前文", "后文"))
            checks["sentence_fill_bridge_reasoning"] = {"passed": analysis_has_bridge}
            if not analysis_has_bridge:
                warnings.append("reference fill question is bridge-oriented, but analysis does not clearly explain both-side linkage.")
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
        stop_bigrams = {"正确", "答案", "选项", "题目", "材料", "解析", "文段", "根据", "一项", "的是"}
        return {token for token in words | bigrams if token and token not in stop_bigrams}

    def _count_sortable_units_from_material(self, material_text: str) -> int:
        text = (material_text or "").strip()
        if not text:
            return 0
        sortable_block = text.split("\n\n")[-1].strip()
        enumerated = re.findall(r"[①②③④⑤⑥⑦⑧⑨⑩]", sortable_block)
        if enumerated:
            return len(set(enumerated))
        sentences = [item.strip() for item in re.split(r"(?<=[。！？!?])", sortable_block) if item.strip()]
        return len(sentences)

    def _extract_order_option_unit_counts(self, options: dict[str, str]) -> list[int]:
        counts: list[int] = []
        for value in options.values():
            text = (value or "").strip()
            if not text:
                continue
            circled = re.findall(r"[①②③④⑤⑥⑦⑧⑨⑩]", text)
            if circled:
                counts.append(len(set(circled)))
                continue
            digits = re.findall(r"\d+", text)
            if digits:
                counts.append(len(set(digits)))
        return counts

    def _extract_order_sequence(self, text: str) -> list[int]:
        raw = str(text or "").strip()
        if not raw:
            return []
        circled_map = {
            "①": 1,
            "②": 2,
            "③": 3,
            "④": 4,
            "⑤": 5,
            "⑥": 6,
            "⑦": 7,
            "⑧": 8,
            "⑨": 9,
            "⑩": 10,
        }
        circled = [circled_map[ch] for ch in raw if ch in circled_map]
        if circled:
            return circled
        return [int(value) for value in re.findall(r"\d+", raw)]

    def _extract_order_sequences_from_text(self, text: str) -> list[list[int]]:
        raw = str(text or "")
        if not raw:
            return []
        sequences: list[list[int]] = []
        for match in re.findall(r"[①②③④⑤⑥⑦⑧⑨⑩]{6,10}", raw):
            sequence = self._extract_order_sequence(match)
            if len(sequence) >= 6:
                sequences.append(sequence[:6])
        return sequences

    def _detect_blank_position(self, material_text: str) -> str:
        text = (material_text or "").strip()
        if not text:
            return ""
        marker_index = -1
        for marker in ("____", "___", "[BLANK]", "（  ）", "( )"):
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
        enumerated_parts = re.split(r"[①②③④⑤⑥⑦⑧⑨⑩]\s*", sortable_block)
        units = [part.strip() for part in enumerated_parts if part and part.strip()]
        if units:
            return units
        return [item.strip() for item in re.split(r"(?<=[。！？!?])", sortable_block) if item.strip()]

    def _sentence_order_unit_role(self, unit: str, *, is_last: bool = False) -> str:
        text = (unit or "").strip()
        if not text:
            return "empty"
        if any(token in text for token in ("因此", "所以", "可见", "看来", "总之", "由此")):
            return "summary"
        if any(token in text for token in ("应该", "应当", "需要", "必须", "要", "可通过")):
            return "action"
        if any(token in text for token in ("问题在于", "难点在于", "困境在于", "为何")):
            return "problem"
        if any(token in text for token in ("是什么", "是指", "就是")):
            return "definition"
        if text.startswith(("只有", "首先", "起初", "第一", "一开始")):
            return "opening_anchor"
        if text.startswith(("这", "那", "其", "该", "此", "这些", "他们")):
            return "dependent"
        if any(token in text for token in ("但是", "然而", "不过", "却", "同时", "此外", "也")):
            return "connector"
        if any(token in text for token in ("首先", "其次", "再次", "随后", "最后", "第一", "第二", "第三")):
            return "timeline"
        if any(token in text for token in ("认为", "指出", "表明", "说明", "关键在于")):
            return "viewpoint"
        if is_last:
            return "tail_statement"
        return "statement"

    def _sentence_order_unit_opener_score(self, unit: str, *, index: int) -> float:
        text = (unit or "").strip()
        if not text:
            return 0.0
        score = 0.26
        if any(token in text for token in ("是什么", "是指", "就是")):
            score += 0.34
        if text.startswith(("首先", "起初", "第一", "一开始")) or "首先" in text:
            score += 0.24
        if any(token in text for token in ("问题在于", "难点在于", "困境在于", "为何")):
            score += 0.24
        if text.startswith(("只有", "对于", "面对")):
            score += 0.10
        if any(token in text for token in ("认为", "指出", "表明", "说明", "关键在于")):
            score += 0.16
        if any(token in text for token in ("因此", "所以", "可见", "看来", "总之", "由此", "应该", "应当", "需要", "必须")):
            score -= 0.20
        if text.startswith(("这", "那", "其", "该", "此", "这些", "他们")):
            score -= 0.30
        if any(token in text for token in ("例如", "比如", "就像")):
            score -= 0.22
        if any(token in text for token in ("但是", "然而", "不过", "却", "同时", "此外")):
            score -= 0.10
        if index == 0 and any(token in text for token in ("首先", "第一步", "起初", "一开始")):
            score += 0.16
        if index == 0:
            score += 0.06
        return round(max(0.0, min(1.0, score)), 4)

    def _build_sentence_order_uniqueness_profile(self, material_text: str) -> dict[str, Any]:
        units = self._extract_order_material_units(material_text)
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

        binding_pair_count = 0
        for index in range(len(units) - 1):
            current = units[index]
            nxt = units[index + 1]
            if nxt.startswith(("这", "那", "其", "该", "此", "这些", "他们")):
                binding_pair_count += 1
                continue
            if any(token in nxt for token in ("但是", "然而", "不过", "却", "同时", "此外", "也")):
                binding_pair_count += 1
                continue
            if any(token in nxt for token in ("随后", "然后", "接着", "最后", "之后", "再", "下一步", "这样", "如此")):
                binding_pair_count += 1
                continue
            if any(token in current for token in ("问题在于", "难点在于", "困境在于", "为何")) and any(
                token in nxt for token in ("因此", "所以", "由此", "应该", "应当", "需要", "必须")
            ):
                binding_pair_count += 1
                continue
            if ("只有" in current and "才" in nxt) or ("如果" in current and any(token in nxt for token in ("那么", "就", "才", "还要"))):
                binding_pair_count += 1
                continue
        binding_pair_count = min(binding_pair_count, 4)

        roles = [self._sentence_order_unit_role(unit, is_last=index == len(units) - 1) for index, unit in enumerate(units)]
        role_counts: dict[str, int] = {}
        for role in roles:
            role_counts[role] = role_counts.get(role, 0) + 1
        duplicate_pairs = sum(max(0, count - 1) for count in role_counts.values())
        directive_density = sum(
            1 for unit in units if any(token in unit for token in ("应该", "应当", "需要", "必须", "要"))
        ) / max(len(units), 1)
        function_overlap_score = round(
            max(0.0, min(1.0, 0.72 * (duplicate_pairs / max(len(units) - 1, 1)) + 0.28 * directive_density)),
            4,
        )

        has_closing_role = any(role in {"summary", "action", "tail_statement"} for role in roles[-2:]) or any(
            token in units[-1] for token in ("这样", "由此", "因此", "所以", "才能", "最终")
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
                    + 0.12 * min(1.0, sum(1 for unit in units if any(token in unit for token in ("同时", "此外", "也", "以及"))) / max(len(units), 1)),
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
