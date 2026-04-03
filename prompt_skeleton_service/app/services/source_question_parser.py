from __future__ import annotations

import re
from typing import Any

from app.core.exceptions import DomainError
from app.schemas.question import SourceQuestionPayload
from app.schemas.runtime import OperationRouteConfig, QuestionRuntimeConfig
from app.services.llm_gateway import LLMGatewayService

_GENERIC_STEMS = {
    "这段文字意在说明（ ）。",
    "这段文字意在强调（ ）。",
    "这段文字主要说明（ ）。",
}
_ANSWER_PATTERN = re.compile(r"(?:故?正确答案(?:为|是)?|答案)[:：]?\s*([A-D])")
_ANALYSIS_MARKER_PATTERN = re.compile(r"^(?:解析[:：]?|[A-D]项|(?:故?正确答案(?:为|是)?|答案)[:：]?)")
_STEM_HINTS = (
    "最恰当的一项",
    "最恰当的一句",
    "意在强调",
    "意在说明",
    "符合文意",
    "重新排列",
    "语序正确",
    "填入横线",
    "填入画横线",
    "接在",
    "标题",
)
_OPTION_LINE_PATTERN = re.compile(r"^[A-D][\.\uff0e、:：\s]*")
_SECTION_PREFIX_PATTERN = re.compile(r"^[一二三四五六七八九十\d]+[、\.\uff0e]?\s*")


class SourceQuestionParserService:
    def __init__(self, runtime_config: QuestionRuntimeConfig) -> None:
        self.runtime_config = runtime_config
        self.llm_gateway = LLMGatewayService(runtime_config)

    def parse(self, raw_text: str) -> SourceQuestionPayload:
        normalized = (raw_text or "").strip()
        if not normalized:
            raise DomainError(
                "raw_text is required for source question parsing.",
                status_code=422,
                details={"field": "raw_text"},
            )

        sentence_order = self._try_parse_sentence_order(normalized)
        if sentence_order is not None:
            return SourceQuestionPayload.model_validate(sentence_order)

        heuristic = self._heuristic_parse(normalized)
        if self._heuristic_is_good_enough(heuristic):
            return SourceQuestionPayload.model_validate(
                {
                    "passage": heuristic.get("passage"),
                    "stem": heuristic.get("stem") or "这段文字意在说明（ ）。",
                    "options": heuristic.get("options") or {key: "" for key in ("A", "B", "C", "D")},
                    "answer": self._normalize_answer(heuristic.get("answer")),
                    "analysis": heuristic.get("analysis"),
                }
            )

        schema: dict[str, Any] = {
            "type": "object",
            "properties": {
                "passage": {"type": ["string", "null"]},
                "stem": {"type": "string"},
                "options": {
                    "type": "object",
                    "properties": {
                        "A": {"type": "string"},
                        "B": {"type": "string"},
                        "C": {"type": "string"},
                        "D": {"type": "string"},
                    },
                },
                "answer": {"type": ["string", "null"]},
                "analysis": {"type": ["string", "null"]},
            },
        }
        system_prompt = (
            "你是一个中文公考题拆解助手。"
            "你的任务是把用户粘贴的一整段原题文本，拆解成固定字段。"
            "只做结构化提取，不要改写原文，不要润色，不要补充不存在的信息。"
            "如果文中同时出现材料、题干、A/B/C/D选项、答案、解析，请分别提取。"
            "passage 只保留材料文段本身，不要带题干、选项、答案、解析。"
            "stem 只保留题干句子。"
            "options 必须返回 A/B/C/D 四个键，没有就填空字符串。"
            "answer 只返回单个大写字母 A/B/C/D；如果文中没有明确答案就返回 null。"
            "analysis 只保留解析部分；如果没有解析就返回 null。"
            "如果原文里有“故正确答案为D”“答案：B”这类表达，要提取到 answer。"
            "如果 passage 无法和其他部分区分，尽量保留最像材料正文的连续段落。"
        )
        user_prompt = f"请拆解下面这段原题文本，并严格输出 JSON：\n\n{normalized}"
        try:
            parsed = self.llm_gateway.generate_json(
                route=self._resolve_route(),
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                schema_name="source_question_parse",
                schema=schema,
            )
        except Exception:
            if heuristic.get("passage") and heuristic.get("stem"):
                return SourceQuestionPayload.model_validate(
                    {
                        "passage": heuristic.get("passage"),
                        "stem": heuristic.get("stem"),
                        "options": heuristic.get("options") or {key: "" for key in ("A", "B", "C", "D")},
                        "answer": self._normalize_answer(heuristic.get("answer")),
                        "analysis": heuristic.get("analysis"),
                    }
                )
            raise
        normalized_options = parsed.get("options") or {}
        parsed_stem = self._clean_text(parsed.get("stem"))
        if parsed_stem in _GENERIC_STEMS:
            parsed_stem = None
        payload = SourceQuestionPayload.model_validate(
            {
                "passage": heuristic.get("passage") or self._clean_text(parsed.get("passage")),
                "stem": heuristic.get("stem") or parsed_stem or "这段文字意在说明（ ）。",
                "options": {
                    "A": heuristic["options"].get("A") or self._clean_text(normalized_options.get("A")) or "",
                    "B": heuristic["options"].get("B") or self._clean_text(normalized_options.get("B")) or "",
                    "C": heuristic["options"].get("C") or self._clean_text(normalized_options.get("C")) or "",
                    "D": heuristic["options"].get("D") or self._clean_text(normalized_options.get("D")) or "",
                },
                "answer": self._normalize_answer(heuristic.get("answer")) or self._normalize_answer(parsed.get("answer")),
                "analysis": heuristic.get("analysis") or self._clean_text(parsed.get("analysis")),
            }
        )
        return payload

    def _try_parse_sentence_order(self, raw_text: str) -> dict[str, Any] | None:
        normalized = raw_text.replace("\r\n", "\n").replace("\r", "\n").strip()
        lines = [line.strip() for line in normalized.split("\n") if line.strip()]
        if len(lines) < 7:
            return None

        unit_line_pattern = re.compile(
            r"^(?:[一二三四五六七八九十]+[、\.\uff0e]?)?(?:[\u2460-\u2473]|\d+[\.\u3001\uff0e\)])"
        )
        option_line_pattern = re.compile(r"^([A-D])[\.\uff0e\u3001:：]\s*(.+)$")
        stem_markers = ("重新排列", "语序正确", "排序")
        answer_pattern = re.compile(r"(?:正确答案|答案)\s*[:：]?\s*([A-D])")
        analysis_pattern = re.compile(r"^(?:解析)\s*[:：]?\s*(.*)$")

        stem_index = next((idx for idx, line in enumerate(lines) if any(marker in line for marker in stem_markers)), None)
        if stem_index is None or stem_index < 2:
            return None

        unit_lines = lines[:stem_index]
        if sum(1 for line in unit_lines if unit_line_pattern.match(line)) < 4:
            return None

        passage = "\n".join(unit_lines).strip()
        stem = lines[stem_index].strip()
        options = {key: "" for key in ("A", "B", "C", "D")}
        answer = None
        analysis_parts: list[str] = []
        in_analysis = False

        for line in lines[stem_index + 1 :]:
            option_match = option_line_pattern.match(line)
            if option_match and not in_analysis:
                options[option_match.group(1)] = option_match.group(2).strip()
                continue

            answer_match = answer_pattern.search(line)
            if answer_match:
                answer = answer_match.group(1)

            analysis_match = analysis_pattern.match(line)
            if analysis_match:
                in_analysis = True
                first_line = analysis_match.group(1).strip()
                if first_line:
                    analysis_parts.append(first_line)
                continue

            if in_analysis:
                analysis_parts.append(line)

        if sum(1 for value in options.values() if value) < 4:
            return None

        return {
            "passage": passage,
            "stem": stem,
            "options": options,
            "answer": answer,
            "analysis": "\n".join(analysis_parts).strip() or None,
        }

    def _resolve_route(self) -> OperationRouteConfig:
        route = self.runtime_config.llm.routing.source_question_parse
        if route is not None:
            return route
        return OperationRouteConfig(
            provider=self.runtime_config.llm.active_provider,
            model_key="reference_parse",
        )

    @staticmethod
    def _normalize_answer(value: Any) -> str | None:
        if value is None:
            return None
        answer = str(value).strip().upper()
        return answer if answer in {"A", "B", "C", "D"} else None

    @staticmethod
    def _clean_text(value: Any) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    def _heuristic_parse(self, raw_text: str) -> dict[str, Any]:
        normalized = raw_text.replace("\r\n", "\n").replace("\r", "\n").strip()
        lines = [line.strip() for line in normalized.split("\n") if line.strip()]
        stem_index = self._find_stem_index(lines)
        option_indices = [idx for idx, line in enumerate(lines) if _OPTION_LINE_PATTERN.match(line)]
        answer_match = _ANSWER_PATTERN.search(normalized)

        passage_lines: list[str] = []
        if stem_index is not None:
            passage_lines = lines[:stem_index]
        elif option_indices:
            passage_lines = lines[: option_indices[0]]
        if passage_lines:
            passage_lines[0] = _SECTION_PREFIX_PATTERN.sub("", passage_lines[0]).strip()

        stem = lines[stem_index] if stem_index is not None else None
        options = {key: "" for key in ("A", "B", "C", "D")}
        analysis_marker_index = None
        for idx, line in enumerate(lines):
            if _ANALYSIS_MARKER_PATTERN.match(line):
                analysis_marker_index = idx
                break

        for position, start_idx in enumerate(option_indices):
            line = lines[start_idx]
            letter = line[0]
            if position + 1 < len(option_indices):
                end_idx = option_indices[position + 1]
            elif analysis_marker_index is not None and analysis_marker_index > start_idx:
                end_idx = analysis_marker_index
            else:
                end_idx = len(lines)
            chunk = [_OPTION_LINE_PATTERN.sub("", line, count=1)] + lines[start_idx + 1 : end_idx]
            chunk = [item for item in chunk if item and not _ANALYSIS_MARKER_PATTERN.match(item)]
            options[letter] = "\n".join(chunk).strip()

        analysis_start = None
        if analysis_marker_index is not None:
            analysis_start = analysis_marker_index
        elif option_indices:
            analysis_start = option_indices[-1] + 1
        if answer_match:
            for idx, line in enumerate(lines):
                if answer_match.group(0) in line:
                    analysis_start = idx if analysis_start is None else min(analysis_start, idx)
                    break
        analysis_lines = lines[analysis_start:] if analysis_start is not None and analysis_start < len(lines) else []

        return {
            "passage": "\n".join(passage_lines).strip() or None,
            "stem": stem,
            "options": options,
            "answer": answer_match.group(1) if answer_match else None,
            "analysis": "\n".join(analysis_lines).strip() or None,
        }

    @staticmethod
    def _heuristic_is_good_enough(parsed: dict[str, Any]) -> bool:
        stem = str(parsed.get("stem") or "").strip()
        passage = str(parsed.get("passage") or "").strip()
        options = parsed.get("options") or {}
        option_count = sum(1 for value in options.values() if str(value or "").strip())
        return bool(stem and passage and option_count >= 4)

    @staticmethod
    def _find_stem_index(lines: list[str]) -> int | None:
        for idx, line in enumerate(lines):
            if ("（" in line and "）" in line) or ("(" in line and ")" in line):
                return idx
            if any(token in line for token in _STEM_HINTS):
                return idx
        return None
