from __future__ import annotations

import html
import json
import re
import unicodedata
from copy import deepcopy
from typing import Any


_CONTROL_CHAR_PATTERN = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
_WORD_XML_TAG_PATTERN = re.compile(r"</?w:[^>]+>")
_GENERIC_XML_TAG_PATTERN = re.compile(r"</?[^>\s/]+(?::[^>\s/]+)?[^>]*>")
_MARKDOWN_JSON_FENCE_PATTERN = re.compile(r"```(?:json)?\s*([\s\S]*?)```", re.IGNORECASE)
_NBSP_ESCAPE_PATTERN = re.compile(r"(?:\\xa0|\\u00a0)+", re.IGNORECASE)

_SUSPICIOUS_TOKENS = (
    "浣犳",
    "鏄",
    "鐨",
    "鍙",
    "鍥",
    "闂",
    "璇",
    "锛",
    "銆",
    "鎴",
    "娌",
    "瀛",
    "绗",
    "瑙",
    "鎬",
    "鍑",
    "璁",
)
_COMMON_CHINESE_CHARS = set(
    "的一是在不了有人这中大为上个国我以要他时来用们生到作地于出就分对成会可主发年动"
    "同工也能下过子说产种面而方后多定行学法所民得经十三之进着等部度家电力里如水化高"
    "自二理起小物现实加量都两体制机当使点从业本去把性好应开它合还因由其些然前外天政"
    "四日那社义事平形全表间样与关各重新线内数正心反你明看原又么利比或但质气第向道命"
    "此变条只没结解问意建月公无系军很情者最立代想已通并提直题党程展五果料象员革位入"
)


def normalize_prompt_text(value: Any) -> str:
    return _normalize_text(value, strip_xml=False)


def normalize_readable_text(value: Any) -> str:
    return _normalize_text(value, strip_xml=True)


def normalize_prompt_structure(value: Any) -> Any:
    return _normalize_structure(value, strip_xml=False)


def normalize_readable_structure(value: Any) -> Any:
    return _normalize_structure(value, strip_xml=True)


def normalize_extracted_lines(lines: list[Any] | tuple[Any, ...] | None) -> list[str]:
    normalized_lines: list[str] = []
    for line in lines or []:
        cleaned = normalize_readable_text(line)
        if cleaned:
            normalized_lines.append(cleaned)
    return normalized_lines


def normalize_source_question_payload(source_question: dict[str, Any] | None) -> dict[str, Any]:
    payload = normalize_readable_structure(deepcopy(source_question or {}))
    if not isinstance(payload, dict):
        return {}

    normalized_options: dict[str, str] = {}
    options = payload.get("options") or {}
    if isinstance(options, dict):
        for key, value in options.items():
            normalized_options[str(key)] = normalize_readable_text(value)
    payload["options"] = normalized_options

    for field_name in ("passage", "stem", "answer", "analysis"):
        if field_name in payload:
            payload[field_name] = _normalize_optional_text(payload.get(field_name))
    return payload


def normalize_user_material_payload(user_material: dict[str, Any] | None) -> dict[str, Any]:
    payload = normalize_readable_structure(deepcopy(user_material or {}))
    if not isinstance(payload, dict):
        return {}

    for field_name in ("text", "title", "topic", "source_label", "document_genre"):
        if field_name in payload:
            payload[field_name] = _normalize_optional_text(payload.get(field_name))
    return payload


def normalize_reference_payload(
    source_question: dict[str, Any] | None,
    *,
    passage_limit: int | None = 600,
    omit_analysis: bool = True,
) -> dict[str, Any]:
    payload = normalize_source_question_payload(source_question)
    passage = payload.get("passage")
    if passage_limit and isinstance(passage, str) and len(passage) > passage_limit:
        payload["passage"] = f"{passage[:passage_limit]}...(truncated)"
    if omit_analysis and "analysis" in payload:
        payload["analysis"] = "[omitted_for_structure_only_fewshot]"
    return payload


def extract_json_object(raw_text: str) -> dict[str, Any]:
    text = str(raw_text or "").strip()
    if not text:
        raise ValueError("empty_output")

    decoder = json.JSONDecoder()
    for candidate in _json_candidate_texts(text):
        if not candidate:
            continue
        try:
            parsed, _ = decoder.raw_decode(candidate)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            return normalize_readable_structure(parsed)
    raise ValueError("json_object_not_found")


def detect_readability_issues(value: Any) -> list[str]:
    text = str(value or "")
    if not text.strip():
        return []

    issues: list[str] = []
    if "<w:" in text or _WORD_XML_TAG_PATTERN.search(text):
        issues.append("word_xml_leak")
    if _NBSP_ESCAPE_PATTERN.search(text) or "\u00a0" in text:
        issues.append("escaped_nbsp")
    if _CONTROL_CHAR_PATTERN.search(text):
        issues.append("control_characters")
    if _suspicious_token_count(text) >= 2:
        issues.append("mojibake_text")
    return issues


def _normalize_structure(value: Any, *, strip_xml: bool) -> Any:
    if isinstance(value, str):
        return _normalize_text(value, strip_xml=strip_xml)
    if isinstance(value, list):
        return [_normalize_structure(item, strip_xml=strip_xml) for item in value]
    if isinstance(value, dict):
        return {str(key): _normalize_structure(item, strip_xml=strip_xml) for key, item in value.items()}
    return value


def _normalize_optional_text(value: Any) -> Any:
    return normalize_readable_text(value) if str(value or "").strip() else value


def _normalize_text(value: Any, *, strip_xml: bool) -> str:
    text = str(value or "")
    if not text:
        return ""

    text = html.unescape(text)
    text = text.replace("\ufeff", "")
    text = _NBSP_ESCAPE_PATTERN.sub(" ", text)
    text = text.replace("\u00a0", " ")
    text = text.replace("\r\n", "\n").replace("\r", "\n")

    should_strip_xml = strip_xml or "<w:" in text or "</w:" in text
    if should_strip_xml:
        text = _WORD_XML_TAG_PATTERN.sub(" ", text)
        text = _GENERIC_XML_TAG_PATTERN.sub(" ", text)

    text = _maybe_repair_mojibake(text)
    text = _CONTROL_CHAR_PATTERN.sub("", text)
    text = unicodedata.normalize("NFKC", text)
    text = re.sub(r"[ \t\f\v]+", " ", text)
    text = re.sub(r" *\n *", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _maybe_repair_mojibake(text: str) -> str:
    if not _contains_cjk(text) and _suspicious_token_count(text) == 0:
        return text

    best = text
    best_score = _readability_score(text)
    segmented_candidate = _repair_text_segments(text)
    segmented_score = _readability_score(segmented_candidate)
    if segmented_candidate != best and segmented_score > best_score:
        best = segmented_candidate
        best_score = segmented_score

    for encoding in ("gb18030", "gbk"):
        try:
            candidate = text.encode(encoding, errors="ignore").decode("utf-8", errors="ignore")
        except Exception:
            continue
        candidate = candidate.strip()
        if not candidate or candidate == best:
            continue
        candidate_score = _readability_score(candidate)
        if _should_prefer_candidate(
            original=best,
            candidate=candidate,
            original_score=best_score,
            candidate_score=candidate_score,
        ):
            best = candidate
            best_score = candidate_score
    return best


def _repair_text_segments(text: str) -> str:
    parts = re.split(r"(\s+)", text)
    if len(parts) <= 1:
        return text

    repaired_parts: list[str] = []
    changed = False
    for part in parts:
        if not part or part.isspace():
            repaired_parts.append(part)
            continue
        repaired = _repair_single_token(part)
        repaired_parts.append(repaired)
        if repaired != part:
            changed = True
    return "".join(repaired_parts) if changed else text


def _repair_single_token(text: str) -> str:
    if not _contains_cjk(text) and _suspicious_token_count(text) == 0:
        return text

    best = text
    best_score = _readability_score(text)
    for encoding in ("gb18030", "gbk"):
        try:
            candidate = text.encode(encoding, errors="ignore").decode("utf-8", errors="ignore")
        except Exception:
            continue
        candidate = candidate.strip()
        if not candidate or candidate == best:
            continue
        candidate_score = _readability_score(candidate)
        if _should_prefer_candidate(
            original=best,
            candidate=candidate,
            original_score=best_score,
            candidate_score=candidate_score,
        ):
            best = candidate
            best_score = candidate_score
    return best


def _should_prefer_candidate(
    *,
    original: str,
    candidate: str,
    original_score: int,
    candidate_score: int,
) -> bool:
    suspicious_delta = _suspicious_token_count(original) - _suspicious_token_count(candidate)
    common_delta = _common_chinese_count(candidate) - _common_chinese_count(original)
    score_delta = candidate_score - original_score

    if suspicious_delta >= 2 and score_delta >= 1:
        return True
    if suspicious_delta >= 1 and common_delta >= 2 and score_delta >= 1:
        return True
    if common_delta >= 2 and score_delta >= 4:
        return True
    if score_delta >= 8:
        return True
    return False


def _readability_score(text: str) -> int:
    chinese_chars = _chinese_char_count(text)
    common_chars = _common_chinese_count(text)
    suspicious_hits = _suspicious_token_count(text)
    xml_hits = len(_WORD_XML_TAG_PATTERN.findall(text))
    replacement_hits = text.count("\ufffd")
    return chinese_chars + common_chars * 3 - suspicious_hits * 4 - xml_hits * 8 - replacement_hits * 6


def _contains_cjk(text: str) -> bool:
    return any("\u4e00" <= ch <= "\u9fff" for ch in text)


def _chinese_char_count(text: str) -> int:
    return sum(1 for ch in text if "\u4e00" <= ch <= "\u9fff")


def _common_chinese_count(text: str) -> int:
    return sum(1 for ch in text if ch in _COMMON_CHINESE_CHARS)


def _suspicious_token_count(text: str) -> int:
    return sum(text.count(token) for token in _SUSPICIOUS_TOKENS)


def _json_candidate_texts(text: str) -> list[str]:
    candidates: list[str] = []
    stripped = text.strip()
    if stripped:
        candidates.append(stripped)

    fence_match = _MARKDOWN_JSON_FENCE_PATTERN.search(stripped)
    if fence_match:
        fenced = fence_match.group(1).strip()
        if fenced:
            candidates.append(fenced)

    start = stripped.find("{")
    end = stripped.rfind("}")
    if start >= 0 and end > start:
        candidates.append(stripped[start : end + 1].strip())

    unique: list[str] = []
    for candidate in candidates:
        normalized = candidate.lstrip("\ufeff").strip()
        if normalized and normalized not in unique:
            unique.append(normalized)
    return unique
