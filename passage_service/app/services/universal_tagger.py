from typing import Any

from pydantic import BaseModel, Field

from app.core.config import get_config_bundle
from app.schemas.span import SpanRecord
from app.schemas.universal_profile import TextShape, UniversalProfile
from app.services.llm_runtime import get_llm_provider, read_prompt_file


class UniversalTaggerBatchResponse(BaseModel):
    items: list[UniversalProfile] = Field(default_factory=list)


UNIVERSAL_PROFILE_JSON_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "items": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "text_shape": {
                        "type": "object",
                        "properties": {
                            "length_bucket": {"type": "string"},
                            "paragraph_count": {"type": "integer"},
                            "sentence_count": {"type": "integer"},
                        },
                        "required": ["length_bucket", "paragraph_count", "sentence_count"],
                        "additionalProperties": False,
                    },
                    "material_structure_label": {"type": "string"},
                    "material_structure_reason": {"type": "string"},
                    "structure_hints": {"type": "array", "items": {"type": "string"}},
                    "logic_relations": {"type": "array", "items": {"type": "string"}},
                    "position_roles": {"type": "array", "items": {"type": "string"}},
                    "standalone_readability": {"type": "number"},
                    "single_center_strength": {"type": "number"},
                    "summary_strength": {"type": "number"},
                    "transition_strength": {"type": "number"},
                    "explanation_strength": {"type": "number"},
                    "ordering_anchor_strength": {"type": "number"},
                    "continuation_openness": {"type": "number"},
                    "direction_uniqueness": {"type": "number"},
                    "titleability": {"type": "number"},
                    "value_judgement_strength": {"type": "number"},
                    "example_to_theme_strength": {"type": "number"},
                    "problem_signal_strength": {"type": "number"},
                    "method_signal_strength": {"type": "number"},
                    "branch_focus_strength": {"type": "number"},
                    "independence_score": {"type": "number"},
                },
                "required": [
                    "text_shape",
                    "material_structure_label",
                    "material_structure_reason",
                    "structure_hints",
                    "logic_relations",
                    "position_roles",
                    "standalone_readability",
                    "single_center_strength",
                    "summary_strength",
                    "transition_strength",
                    "explanation_strength",
                    "ordering_anchor_strength",
                    "continuation_openness",
                    "direction_uniqueness",
                    "titleability",
                    "value_judgement_strength",
                    "example_to_theme_strength",
                    "problem_signal_strength",
                    "method_signal_strength",
                    "branch_focus_strength",
                    "independence_score",
                ],
                "additionalProperties": False,
            },
        }
    },
    "required": ["items"],
    "additionalProperties": False,
}


class UniversalTagger:
    def __init__(self) -> None:
        self.provider = get_llm_provider()
        self.llm_config = get_config_bundle().llm
        self.prompt = read_prompt_file("universal_tagger_prompt.md")

    def tag_many(self, spans: list[SpanRecord]) -> list[UniversalProfile]:
        if not spans:
            return []
        if self.llm_config.get("enabled") and self.provider.is_enabled():
            try:
                return self._tag_many_with_llm(spans)
            except Exception:
                return [self._heuristic_tag(span) for span in spans]
        return [self._heuristic_tag(span) for span in spans]

    def tag(self, span: SpanRecord) -> UniversalProfile:
        return self.tag_many([span])[0]

    def _tag_many_with_llm(self, spans: list[SpanRecord]) -> list[UniversalProfile]:
        prompt = self._build_batch_prompt(spans)
        result = self.provider.generate_json(
            model=self.llm_config.get("models", {}).get("universal_tagger", "gpt-4o-mini"),
            instructions=self.prompt,
            input_payload={
                "prompt": prompt,
                "schema_name": "universal_profile_batch",
                "schema": UNIVERSAL_PROFILE_JSON_SCHEMA,
            },
        )
        parsed = UniversalTaggerBatchResponse.model_validate(result)
        if len(parsed.items) != len(spans):
            raise ValueError("Universal tagger returned mismatched item count.")
        return parsed.items

    def _build_batch_prompt(self, spans: list[SpanRecord]) -> str:
        lines = [
            "Analyze each span and return one universal_profile per span in the same order.",
            "Score each numeric field from 0.0 to 1.0.",
            "Use only the provided labels for structure_hints, logic_relations, and position_roles.",
            "For material_structure_label, choose one concise Chinese label from this set: 总分归纳, 分总归纳, 转折归旨, 并列推进, 问题-对策, 现象-分析, 案例-结论, 观点-论证, 背景-核心结论, 综合说明.",
            "material_structure_reason should be a short Chinese explanation of why the span belongs to that structure.",
            "standalone_readability should reflect whether the span can be directly shown to a human as a self-contained passage.",
            "",
        ]
        for index, span in enumerate(spans, start=1):
            lines.extend(
                [
                    f"[SPAN {index}]",
                    f"paragraph_count: {span.paragraph_count}",
                    f"sentence_count: {span.sentence_count}",
                    "text:",
                    span.text,
                    "",
                ]
            )
        return "\n".join(lines)

    def _heuristic_tag(self, span: SpanRecord) -> UniversalProfile:
        text = span.text
        paragraph_count = max(1, span.paragraph_count)
        sentence_count = max(1, span.sentence_count)
        text_length = len(text)

        length_bucket = "long" if text_length > 500 else "medium" if text_length > 180 else "short"
        structure_hints: list[str] = []
        logic_relations: list[str] = []
        position_roles: list[str] = []

        has_summary_word = any(token in text for token in ("\u603b\u4e4b", "\u53ef\u89c1", "\u7531\u6b64\u53ef\u89c1", "\u56e0\u6b64"))
        has_turn = any(token in text for token in ("\u4f46\u662f", "\u7136\u800c", "\u4e0d\u8fc7"))
        has_example = any(token in text for token in ("\u4f8b\u5982", "\u6bd4\u5982", "\u8b6c\u5982"))
        has_problem = any(token in text for token in ("\u95ee\u9898", "\u56f0\u5883", "\u75c7\u7ed3"))
        has_method = any(token in text for token in ("\u8def\u5f84", "\u505a\u6cd5", "\u63aa\u65bd", "\u529e\u6cd5", "\u5efa\u8bae"))
        has_enumeration = any(token in text for token in ("\u9996\u5148", "\u5176\u6b21", "\u518d\u6b21", "\u6700\u540e"))
        has_value = any(token in text for token in ("\u5e94\u5f53", "\u5fc5\u987b", "\u503c\u5f97", "\u5173\u952e", "\u91cd\u8981"))
        has_open_ending = any(token in text for token in ("\u672a\u6765", "\u63a5\u4e0b\u6765", "\u4ecd\u9700", "\u66f4\u8981"))
        starts_with_context_dependency = text.startswith(("\u5bf9\u6b64", "\u4e0e\u6b64\u540c\u65f6", "\u53e6\u4e00\u65b9\u9762", "\u6b64\u5916", "\u8fd9\u4e5f"))
        has_terminal_punct = text.rstrip().endswith(("\u3002", "\uff01", "\uff1f", "!", "?"))

        if paragraph_count >= 3:
            structure_hints.append("\u591a\u6bb5\u5e76\u8fdb")
        if has_enumeration:
            structure_hints.append("\u627f\u63a5-\u5e76\u5217-\u603b\u7ed3")
            logic_relations.append("\u5e76\u5217/\u9012\u8fdb")
            position_roles.extend(("\u6392\u5e8f\u9996\u53e5\u5019\u9009", "\u6392\u5e8f\u5c3e\u53e5\u5019\u9009"))
        if has_summary_word:
            structure_hints.append("\u5206\u603b")
            logic_relations.append("\u603b\u7ed3\u63d0\u5347")
            position_roles.append("\u5c3e\u6bb5\u603b\u7ed3")
        if has_turn:
            structure_hints.append("\u8f6c\u6298\u6536\u675f")
            logic_relations.append("\u8f6c\u6298")
            position_roles.append("\u8f6c\u6298\u540e\u7ed3\u8bba")
        if has_example:
            structure_hints.append("\u4e3e\u4f8b\u8bba\u8bc1\u540e\u5f52\u7eb3")
        if has_problem:
            logic_relations.append("\u95ee\u9898\u66b4\u9732")
        if has_method:
            logic_relations.append("\u65b9\u6cd5\u5f15\u51fa")
        if any(token in text for token in ("\u56e0\u6b64", "\u6240\u4ee5", "\u56e0\u4e3a", "\u5bfc\u81f4")):
            logic_relations.append("\u56e0\u679c")
        if any(token in text for token in ("\u540c\u65f6", "\u5e76\u4e14", "\u4e00\u65b9\u9762", "\u53e6\u4e00\u65b9\u9762")):
            logic_relations.append("\u627f\u63a5")
        if text.startswith(("\u5f53\u524d", "\u9762\u5bf9", "\u8fd1\u5e74\u6765")):
            position_roles.append("\u5f00\u5934\u603b\u9886")
        if any(token in text for token in ("\u6362\u53e5\u8bdd\u8bf4", "\u8fdb\u4e00\u6b65\u770b", "\u5177\u4f53\u800c\u8a00")):
            position_roles.append("\u4e2d\u95f4\u89e3\u91ca\u4f4d")
        if has_open_ending:
            position_roles.append("\u5c3e\u6bb5\u7eed\u5199\u843d\u70b9")

        has_complete_discourse = paragraph_count >= 2 and sentence_count >= 5
        single_center = 0.35
        single_center += 0.18 if has_summary_word else 0.0
        single_center += 0.16 if has_complete_discourse else 0.0
        single_center += 0.10 if text_length >= 260 else 0.0
        single_center = min(single_center, 0.92)

        summary_strength = 0.30
        summary_strength += 0.35 if has_summary_word else 0.0
        summary_strength += 0.18 if has_complete_discourse else 0.0
        summary_strength += 0.08 if text_length >= 260 else 0.0
        summary_strength = min(summary_strength, 0.92)

        transition_strength = 0.80 if has_turn else 0.25
        explanation_strength = 0.70 if any(token in text for token in ("\u5373", "\u4e5f\u5c31\u662f", "\u5c31\u662f\u8bf4", "\u4f8b\u5982", "\u6bd4\u5982")) else 0.35

        ordering_anchor_strength = 0.72 if has_enumeration else 0.22
        if has_complete_discourse:
            ordering_anchor_strength -= 0.12
        if has_enumeration and sentence_count <= 5:
            ordering_anchor_strength += 0.12
        ordering_anchor_strength = max(0.0, min(ordering_anchor_strength, 0.9))

        continuation_openness = 0.80 if has_open_ending else 0.28
        direction_uniqueness = 0.75 if any(token in text for token in ("\u5fc5\u987b", "\u5173\u952e\u5728\u4e8e", "\u6839\u672c\u5728\u4e8e")) else 0.38
        titleability = min(1.0, 0.38 + single_center * 0.34 + summary_strength * 0.18 + (0.12 if paragraph_count >= 2 else 0.0))
        value_judgement_strength = 0.75 if has_value else 0.30
        example_to_theme_strength = 0.80 if has_example and has_summary_word else 0.22
        problem_signal_strength = 0.80 if has_problem else 0.22
        method_signal_strength = 0.80 if has_method else 0.22
        branch_focus_strength = 0.72 if has_enumeration else 0.22

        material_structure_label, material_structure_reason = self._infer_material_structure_label(
            has_summary_word=has_summary_word,
            has_turn=has_turn,
            has_example=has_example,
            has_problem=has_problem,
            has_method=has_method,
            has_enumeration=has_enumeration,
            has_value=has_value,
            paragraph_count=paragraph_count,
            text=text,
        )

        independence_score = 0.34
        independence_score += 0.22 if paragraph_count >= 2 else 0.0
        independence_score += 0.16 if sentence_count >= 5 else 0.0
        independence_score += 0.14 if text_length >= 220 else 0.0
        independence_score += 0.10 if has_summary_word else 0.0
        independence_score = min(independence_score, 0.95)

        standalone_readability = independence_score
        if starts_with_context_dependency:
            standalone_readability = max(0.0, standalone_readability - 0.22)
        if not has_terminal_punct:
            standalone_readability = max(0.0, standalone_readability - 0.18)
        if "【关键词】" in text or "【事件】" in text or "【点评】" in text:
            standalone_readability = max(0.0, standalone_readability - 0.24)
        standalone_readability = round(min(0.98, standalone_readability), 4)

        return UniversalProfile(
            text_shape=TextShape(
                length_bucket=length_bucket,
                paragraph_count=paragraph_count,
                sentence_count=sentence_count,
            ),
            material_structure_label=material_structure_label,
            material_structure_reason=material_structure_reason,
            structure_hints=sorted(set(structure_hints)),
            logic_relations=sorted(set(logic_relations)),
            position_roles=sorted(set(position_roles)),
            standalone_readability=standalone_readability,
            single_center_strength=round(single_center, 4),
            summary_strength=round(summary_strength, 4),
            transition_strength=round(transition_strength, 4),
            explanation_strength=round(explanation_strength, 4),
            ordering_anchor_strength=round(ordering_anchor_strength, 4),
            continuation_openness=round(continuation_openness, 4),
            direction_uniqueness=round(direction_uniqueness, 4),
            titleability=round(titleability, 4),
            value_judgement_strength=round(value_judgement_strength, 4),
            example_to_theme_strength=round(example_to_theme_strength, 4),
            problem_signal_strength=round(problem_signal_strength, 4),
            method_signal_strength=round(method_signal_strength, 4),
            branch_focus_strength=round(branch_focus_strength, 4),
            independence_score=round(independence_score, 4),
        )

    def _infer_material_structure_label(
        self,
        *,
        has_summary_word: bool,
        has_turn: bool,
        has_example: bool,
        has_problem: bool,
        has_method: bool,
        has_enumeration: bool,
        has_value: bool,
        paragraph_count: int,
        text: str,
    ) -> tuple[str, str]:
        if has_problem and has_method:
            return "问题-对策", "材料先暴露问题，再给出路径或措施。"
        if has_example and has_summary_word:
            return "案例-结论", "通过个案或事例铺垫，最后归出主题结论。"
        if has_turn and has_summary_word:
            return "转折归旨", "前文铺垫后通过转折收束到核心判断。"
        if has_enumeration and paragraph_count >= 2:
            return "并列推进", "材料围绕同一主题分点展开、并列推进。"
        if has_summary_word and paragraph_count >= 2:
            return "分总归纳", "前文展开说明，尾部做集中归纳。"
        if text.startswith(("\u5f53\u524d", "\u8fd1\u5e74\u6765", "\u9762\u5bf9")) and has_value:
            return "背景-核心结论", "先交代背景，再落到中心判断。"
        if has_value:
            return "观点-论证", "先提出态度或判断，再补充支撑。"
        if any(token in text for token in ("\u73b0\u8c61", "\u8868\u73b0", "\u53cd\u6620", "\u6298\u5c04")):
            return "现象-分析", "从现象出发，进一步解释其含义或原因。"
        return "综合说明", "材料信息较综合，以说明和概括为主。"
