from __future__ import annotations

from typing import Any

from app.schemas.config import BusinessSubtypeConfig, FewshotExampleConfig, PatternConfig, QuestionTypeConfig
from app.services.question_generation_prompt_assets import (
    get_round1_family_prompt_guards,
    get_round1_fewshot_examples,
)
from app.services.text_readability import normalize_prompt_text


class PromptBuilderService:
    NON_PROMPTABLE_EXTRA_CONSTRAINT_KEYS = {
        "source_question_style_summary",
        "reference_business_cards",
        "reference_query_terms",
    }

    def build(
        self,
        *,
        question_type_config: QuestionTypeConfig,
        business_subtype_config: BusinessSubtypeConfig | None,
        pattern: PatternConfig,
        difficulty_target: str,
        resolved_slots: dict[str, Any],
        skeleton: dict[str, Any],
        control_logic: dict[str, Any],
        generation_logic: dict[str, Any],
        topic: str | None,
        count: int,
        passage_style: str | None,
        use_fewshot: bool,
        fewshot_mode: str,
        extra_constraints: dict[str, Any] | None,
    ) -> dict[str, Any]:
        fewshot_examples = self._select_fewshot(
            question_type_config=question_type_config,
            business_subtype_config=business_subtype_config,
            pattern=pattern,
            resolved_slots=resolved_slots,
            use_fewshot=use_fewshot,
            fewshot_mode=fewshot_mode,
        )
        fewshot_guard_lines = self._resolve_round1_fewshot_guard_lines(
            question_type_config=question_type_config,
            business_subtype_config=business_subtype_config,
            use_fewshot=use_fewshot,
        )

        slots_summary = self.summarize_slots(resolved_slots)
        control_summary = self.summarize_control_logic(control_logic)
        generation_summary = self.summarize_generation_logic(generation_logic)

        system_prompt = self._build_system_prompt(
            question_type_config=question_type_config,
            business_subtype_config=business_subtype_config,
            skeleton=skeleton,
            control_summary=control_summary,
            generation_summary=generation_summary,
            difficulty_target=difficulty_target,
            fewshot_guard_lines=fewshot_guard_lines,
        )
        user_prompt = self._build_user_prompt(
            question_type=question_type_config.type_id,
            business_subtype=business_subtype_config.subtype_id if business_subtype_config else None,
            pattern=pattern,
            difficulty_target=difficulty_target,
            topic=topic,
            count=count,
            passage_style=passage_style,
            slots_summary=slots_summary,
            control_summary=control_summary,
            generation_summary=generation_summary,
            extra_constraints=extra_constraints,
        )
        system_prompt = normalize_prompt_text(system_prompt)
        user_prompt = normalize_prompt_text(user_prompt)
        merged_prompt = normalize_prompt_text(self._merge_prompt(system_prompt, fewshot_examples, user_prompt))
        fewshot_text_block = normalize_prompt_text(self._render_fewshot_block(fewshot_examples))

        return {
            "system_prompt": system_prompt,
            "user_prompt": user_prompt,
            "fewshot_examples": [example.model_dump(exclude_none=True) for example in fewshot_examples],
            "fewshot_text_block": fewshot_text_block,
            "merged_prompt": merged_prompt,
        }

    def summarize_slots(self, resolved_slots: dict[str, Any]) -> str:
        if not resolved_slots:
            return "No type slots were provided."
        return "; ".join(f"{key}={self._summarize_value(value)}" for key, value in resolved_slots.items())

    def summarize_control_logic(self, control_logic: dict[str, Any]) -> dict[str, str]:
        levers = control_logic.get("control_levers", {})
        return {
            "difficulty_source": self._summarize_value(control_logic.get("difficulty_source")),
            "option_confusion": self._summarize_value(control_logic.get("option_confusion")),
            "passage": self._summarize_value(levers.get("passage")),
            "correct_option": self._summarize_value(levers.get("correct_option")),
            "wrong_options": self._summarize_value(levers.get("wrong_options")),
            "special_fields": self._summarize_kv_dict(control_logic.get("special_fields", {})),
        }

    def summarize_generation_logic(self, generation_logic: dict[str, Any]) -> dict[str, str]:
        return {
            "generation_core": self._summarize_value(generation_logic.get("generation_core")),
            "processing_type": self._summarize_value(generation_logic.get("processing_type")),
            "correct_logic": self._summarize_value(generation_logic.get("correct_logic")),
            "high_freq_traps": self._summarize_value(generation_logic.get("high_freq_traps")),
            "distractor_pattern": self._summarize_value(generation_logic.get("distractor_pattern")),
            "analysis_steps": self._summarize_value(generation_logic.get("analysis_steps")),
        }

    def _select_fewshot(
        self,
        *,
        question_type_config: QuestionTypeConfig,
        business_subtype_config: BusinessSubtypeConfig | None,
        pattern: PatternConfig,
        resolved_slots: dict[str, Any],
        use_fewshot: bool,
        fewshot_mode: str,
    ) -> list[FewshotExampleConfig]:
        if not use_fewshot or fewshot_mode != "structure_only":
            return []

        round1_candidates = get_round1_fewshot_examples(
            question_type=question_type_config.type_id,
            business_subtype=business_subtype_config.subtype_id if business_subtype_config else None,
        )
        selected = self._pick_best_fewshot(
            round1_candidates,
            resolved_slots,
            pattern.pattern_id,
            require_positive_score=True,
        )
        if selected:
            return [selected]

        candidates: list[FewshotExampleConfig] = []
        if business_subtype_config and self._fewshot_enabled(business_subtype_config.fewshot_policy):
            candidates = self._collect_fewshots(
                business_subtype_config.fewshot_example,
                business_subtype_config.fewshot_examples,
            )
            selected = self._pick_best_fewshot(candidates, resolved_slots, pattern.pattern_id)
            if selected:
                return [selected]

        candidates = self._collect_fewshots(pattern.fewshot_example, pattern.fewshot_examples)
        selected = self._pick_best_fewshot(candidates, resolved_slots, pattern.pattern_id)
        if selected:
            return [selected]

        if self._fewshot_enabled(question_type_config.fewshot_policy):
            candidates = question_type_config.default_fewshot
            selected = self._pick_best_fewshot(candidates, resolved_slots, pattern.pattern_id)
            if selected:
                return [selected]

        return []

    def _fewshot_enabled(self, policy) -> bool:
        return policy is None or policy.enabled

    def _collect_fewshots(
        self,
        single_example: FewshotExampleConfig | None,
        examples: list[FewshotExampleConfig],
    ) -> list[FewshotExampleConfig]:
        collected = list(examples)
        if single_example:
            collected.insert(0, single_example)
        return collected

    def _pick_best_fewshot(
        self,
        examples: list[FewshotExampleConfig],
        resolved_slots: dict[str, Any],
        selected_pattern_id: str,
        require_positive_score: bool = False,
    ) -> FewshotExampleConfig | None:
        if not examples:
            return None

        best_example = examples[0]
        best_score = -1
        for example in examples:
            score = 0
            if example.preferred_patterns and selected_pattern_id in example.preferred_patterns:
                score += 2
            for key, value in example.fit_slots.items():
                if resolved_slots.get(key) == value:
                    score += 1
            if score > best_score:
                best_score = score
                best_example = example
        if require_positive_score and best_score <= 0:
            return None
        return best_example

    def _build_system_prompt(
        self,
        *,
        question_type_config: QuestionTypeConfig,
        business_subtype_config: BusinessSubtypeConfig | None,
        skeleton: dict[str, Any],
        control_summary: dict[str, str],
        generation_summary: dict[str, str],
        difficulty_target: str,
        fewshot_guard_lines: list[str] | None = None,
    ) -> str:
        parts = [
            "You are building a prompt skeleton for question generation.",
            f"Task definition: {question_type_config.task_definition or question_type_config.display_name}",
        ]
        if business_subtype_config:
            parts.append(f"Business subtype description: {business_subtype_config.description}")
        parts.extend(
            [
                (
                    f"Configured skeleton anchor={skeleton['anchor_type']}, "
                    f"operation={skeleton['operation_type']}, target={skeleton['target_type']}."
                ),
                f"Configured difficulty target: {self._summarize_difficulty_target(difficulty_target)}",
                (
                    "Configured control logic: "
                    f"difficulty_source={control_summary['difficulty_source']}; "
                    f"option_confusion={control_summary['option_confusion']}; "
                    f"passage={control_summary['passage']}; "
                    f"correct_option={control_summary['correct_option']}; "
                    f"wrong_options={control_summary['wrong_options']}; "
                    f"special_fields={control_summary['special_fields']}."
                ),
                (
                    "Configured generation logic: "
                    f"core={generation_summary['generation_core']}; "
                    f"processing={generation_summary['processing_type']}; "
                    f"correct_logic={generation_summary['correct_logic']}; "
                    f"traps={generation_summary['high_freq_traps']}; "
                    f"distractors={generation_summary['distractor_pattern']}; "
                    f"analysis_steps={generation_summary['analysis_steps']}."
                ),
                "Output contract: return the structured fields required by the caller.",
            ]
        )
        if fewshot_guard_lines:
            parts.append("Round 1 few-shot guardrails:")
            parts.extend(f"- {line}" for line in fewshot_guard_lines)
        return "\n".join(parts)

    def _build_user_prompt(
        self,
        *,
        question_type: str,
        business_subtype: str | None,
        pattern: PatternConfig,
        difficulty_target: str,
        topic: str | None,
        count: int,
        passage_style: str | None,
        slots_summary: str,
        control_summary: dict[str, str],
        generation_summary: dict[str, str],
        extra_constraints: dict[str, Any] | None,
    ) -> str:
        prompt_safe_constraints = self._filter_prompt_safe_extra_constraints(extra_constraints)
        return "\n".join(
            [
                f"Current question_type: {question_type}",
                f"Current business_subtype: {business_subtype or 'not specified'}",
                f"Current selected_pattern: {pattern.pattern_id} ({pattern.pattern_name})",
                f"Configured difficulty target: {self._summarize_difficulty_target(difficulty_target)}",
                f"Topic: {topic or 'not specified'}",
                f"Count: {count}",
                f"Passage style: {passage_style or 'not specified'}",
                f"Extra constraints: {self._summarize_kv_dict(prompt_safe_constraints)}",
                f"Slots summary: {slots_summary}",
                (
                    "Configured control logic summary: "
                    f"difficulty source={control_summary['difficulty_source']}; "
                    f"passage={control_summary['passage']}; "
                    f"correct option={control_summary['correct_option']}; "
                    f"wrong options={control_summary['wrong_options']}; "
                    f"special fields={control_summary['special_fields']}."
                ),
                (
                    "Configured generation logic summary: "
                    f"core={generation_summary['generation_core']}; "
                    f"processing={generation_summary['processing_type']}; "
                    f"correct logic={generation_summary['correct_logic']}; "
                    f"traps={generation_summary['high_freq_traps']}; "
                    f"distractors={generation_summary['distractor_pattern']}; "
                    f"analysis steps={generation_summary['analysis_steps']}."
                ),
            ]
        )

    def _merge_prompt(
        self,
        system_prompt: str,
        fewshot_examples: list[FewshotExampleConfig],
        user_prompt: str,
    ) -> str:
        fewshot_block = "None"
        if fewshot_examples:
            fewshot_block = "\n\n".join(
                self._render_fewshot_example(example, index)
                for index, example in enumerate(fewshot_examples, start=1)
            )

        return "\n\n".join(
            [
                f"[System Prompt]\n{system_prompt}",
                f"[Few-shot Examples]\n{fewshot_block}",
                f"[User Prompt]\n{user_prompt}",
            ]
        )

    def _render_fewshot_block(self, fewshot_examples: list[FewshotExampleConfig]) -> str:
        if not fewshot_examples:
            return "None"
        return "\n\n".join(
            self._render_fewshot_example(example, index)
            for index, example in enumerate(fewshot_examples, start=1)
        )

    def _render_fewshot_example(self, example: FewshotExampleConfig, index: int) -> str:
        if example.input or example.output:
            return (
                f"Few-shot {index}: {example.title or 'example'}\n"
                f"Input:\n{example.input or ''}\n"
                f"Output:\n{example.output or ''}"
            )

        if example.content:
            return f"Few-shot {index}: {example.title or 'example'}\n{example.content}"

        parts = [
            f"Few-shot {index}: {example.title or 'example'}",
            f"Input brief: {example.input_brief or 'not specified'}",
            f"Question brief: {example.question_brief or 'not specified'}",
            f"Options brief: {self._summarize_value(example.options_brief)}",
            f"Answer: {example.answer or 'not specified'}",
            f"Rationale: {example.rationale_brief or 'not specified'}",
        ]
        return "\n".join(parts)

    def _summarize_value(self, value: Any) -> str:
        if value is None:
            return "not specified"
        if isinstance(value, list):
            return "; ".join(str(item) for item in value) if value else "none"
        if isinstance(value, dict):
            return self._summarize_kv_dict(value)
        return str(value)

    def _summarize_kv_dict(self, data: dict[str, Any]) -> str:
        if not data:
            return "none"
        return "; ".join(f"{key}={self._summarize_value(value)}" for key, value in data.items())

    def _filter_prompt_safe_extra_constraints(self, extra_constraints: dict[str, Any] | None) -> dict[str, Any]:
        if not extra_constraints:
            return {}
        return {
            key: value
            for key, value in extra_constraints.items()
            if str(key) not in self.NON_PROMPTABLE_EXTRA_CONSTRAINT_KEYS
        }

    def _summarize_difficulty_target(self, difficulty_target: str) -> str:
        return str(difficulty_target or "not specified")

    def _resolve_round1_fewshot_guard_lines(
        self,
        *,
        question_type_config: QuestionTypeConfig,
        business_subtype_config: BusinessSubtypeConfig | None,
        use_fewshot: bool,
    ) -> list[str]:
        if not use_fewshot:
            return []
        return get_round1_family_prompt_guards(
            question_type=question_type_config.type_id,
            business_subtype=business_subtype_config.subtype_id if business_subtype_config else None,
        )
