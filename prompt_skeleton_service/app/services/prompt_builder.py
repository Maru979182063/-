from __future__ import annotations

from typing import Any

from app.schemas.config import BusinessSubtypeConfig, FewshotExampleConfig, PatternConfig, QuestionTypeConfig


class PromptBuilderService:
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
        merged_prompt = self._merge_prompt(system_prompt, fewshot_examples, user_prompt)

        return {
            "system_prompt": system_prompt,
            "user_prompt": user_prompt,
            "fewshot_examples": [example.model_dump(exclude_none=True) for example in fewshot_examples],
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
                    f"Unified skeleton anchor={skeleton['anchor_type']}, "
                    f"operation={skeleton['operation_type']}, target={skeleton['target_type']}."
                ),
                (
                    "Correct option requirements: "
                    f"{generation_summary['correct_logic']}; core={generation_summary['generation_core']}."
                ),
                (
                    "Wrong option requirements: "
                    f"{control_summary['wrong_options']}; distractor pattern={generation_summary['distractor_pattern']}; "
                    f"confusion level={control_summary['option_confusion']}. "
                    "Apply this confusion level to all wrong options as a whole, so each distractor should stay reasonably close "
                    "to the correct answer instead of leaving only one plausible distractor."
                ),
                f"Difficulty execution rule: {self._summarize_difficulty_target(difficulty_target)}",
                "Output format requirements: return a structured package containing passage, stem, options, answer, and concise rationale.",
            ]
        )
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
        return "\n".join(
            [
                f"Current question_type: {question_type}",
                f"Current business_subtype: {business_subtype or 'not specified'}",
                f"Current selected_pattern: {pattern.pattern_id} ({pattern.pattern_name})",
                f"Target difficulty: {difficulty_target}",
                f"Difficulty rendering instruction: {self._summarize_difficulty_target(difficulty_target)}",
                f"Topic: {topic or 'not specified'}",
                f"Count: {count}",
                f"Passage style: {passage_style or 'not specified'}",
                f"Extra constraints: {self._summarize_kv_dict(extra_constraints or {})}",
                (
                    "Required review overrides: "
                    f"{self._summarize_kv_dict((extra_constraints or {}).get('required_review_overrides') or {})}. "
                    "These override values are mandatory and should be reflected explicitly in the regenerated question."
                ),
                (
                    "Wrong-option confusion profile: "
                    f"{self._summarize_value((extra_constraints or {}).get('wrong_option_confusion_profile'))}. "
                    "The strongest distractor should still read as reasonable and textually relevant, and lose only because "
                    "it is not the best-supported choice in the original passage."
                ),
                f"Slots summary: {slots_summary}",
                (
                    "Control logic summary: "
                    f"difficulty source={control_summary['difficulty_source']}; "
                    f"passage={control_summary['passage']}; "
                    f"correct option={control_summary['correct_option']}; "
                    f"wrong options={control_summary['wrong_options']}; "
                    f"special fields={control_summary['special_fields']}. "
                    "The configured option confusion should affect the full set of wrong options, not just one pairwise combination."
                ),
                (
                    "Generation logic summary: "
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

    def _summarize_difficulty_target(self, difficulty_target: str) -> str:
        if difficulty_target == "easy":
            return (
                "keep the main clue explicit, allow one clearer elimination path, and keep distractors related but not overly compressed "
                "toward the correct answer."
            )
        if difficulty_target == "hard":
            return (
                "raise abstraction and reasoning depth substantially, reduce overt clue visibility, and make all wrong options stay close to the correct answer. "
                "All three distractors should remain plausible at first glance, and the strongest distractor should look fully reasonable and fail only because the original passage does not support it best."
            )
        return (
            "keep the core clue available but not overtly obvious, and ensure multiple distractors remain topic-aligned and noticeably close "
            "to the correct answer, without leaving obvious throwaway options."
        )
