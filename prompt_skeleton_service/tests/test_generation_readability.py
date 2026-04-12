from __future__ import annotations

import sys
import types
from unittest import TestCase


def _install_test_stubs() -> None:
    if "fastapi" not in sys.modules:
        fastapi = types.ModuleType("fastapi")
        fastapi.FastAPI = type("FastAPI", (), {})
        fastapi.Request = type("Request", (), {})
        sys.modules["fastapi"] = fastapi
    if "fastapi.responses" not in sys.modules:
        responses = types.ModuleType("fastapi.responses")
        responses.JSONResponse = type("JSONResponse", (), {})
        sys.modules["fastapi.responses"] = responses
    if "yaml" not in sys.modules:
        try:
            import yaml as _yaml  # type: ignore
        except ImportError:
            yaml = types.ModuleType("yaml")
            yaml.safe_load = lambda *args, **kwargs: {}
            sys.modules["yaml"] = yaml
        else:
            sys.modules["yaml"] = _yaml


_install_test_stubs()

from app.services.question_generation import QuestionGenerationService
from app.services.text_readability import (
    extract_json_object,
    normalize_extracted_lines,
    normalize_prompt_text,
    normalize_readable_structure,
    normalize_readable_text,
    normalize_reference_payload,
    normalize_source_question_payload,
    normalize_user_material_payload,
)


class GenerationReadabilityTest(TestCase):
    def test_normalize_readable_text_repairs_mojibake(self) -> None:
        normalized = normalize_readable_text("浣犳槸")

        self.assertEqual(normalized, "你是")

    def test_normalize_readable_text_strips_word_xml(self) -> None:
        normalized = normalize_readable_text('<w:p><w:r><w:t>答案</w:t></w:r></w:p>')

        self.assertEqual(normalized, "答案")

    def test_extract_json_object_handles_markdown_fence(self) -> None:
        raw = '```json\n{"stem":"题干","options":{"A":"甲","B":"乙","C":"丙","D":"丁"},"answer":"A","analysis":"解析","original_sentences":[],"correct_order":[]}\n```'

        parsed = extract_json_object(raw)

        self.assertEqual(parsed["stem"], "题干")
        self.assertEqual(parsed["answer"], "A")

    def test_make_prompt_section_normalizes_lines(self) -> None:
        service = QuestionGenerationService.__new__(QuestionGenerationService)
        service._section_label = lambda key: f"[{key}]"

        section = service._make_prompt_section(
            "reference_question_template",
            ['浣犳槸 <w:t>答案</w:t>'],
        )

        self.assertEqual(section[0], "[reference_question_template]")
        self.assertEqual(section[1], "你是 答案")

    def test_normalize_prompt_text_keeps_json_shell_characters(self) -> None:
        normalized = normalize_prompt_text('{"stem":"浣犳槸","analysis":"<w:t>答案</w:t>"}')

        self.assertIn('{"stem"', normalized)
        self.assertIn("你是", normalized)

    def test_normalize_extracted_lines_drops_empty_and_cleans(self) -> None:
        normalized = normalize_extracted_lines(["<w:t>第一段</w:t>", "", "\u00a0", "浣犳槸"])

        self.assertEqual(normalized, ["第一段", "你是"])

    def test_normalize_source_question_payload_cleans_nested_fields(self) -> None:
        payload = normalize_source_question_payload(
            {
                "passage": "<w:p><w:t>第一段\\u00a0内容</w:t></w:p>\x07",
                "stem": "浣犳槸",
                "options": {"A": "<w:t>选项A</w:t>", "B": "普通项"},
                "analysis": "<w:t>解析</w:t>",
            }
        )

        self.assertEqual(payload["passage"], "第一段 内容")
        self.assertEqual(payload["stem"], "你是")
        self.assertEqual(payload["options"]["A"], "选项A")
        self.assertEqual(payload["analysis"], "解析")

    def test_normalize_user_material_payload_cleans_generation_inputs(self) -> None:
        payload = normalize_user_material_payload(
            {
                "text": "<w:p><w:t>材料正文\\u00a0</w:t></w:p>",
                "title": "<w:t>材料标题</w:t>",
                "source_label": "浣犳槸",
            }
        )

        self.assertEqual(payload["text"], "材料正文")
        self.assertEqual(payload["title"], "材料标题")
        self.assertEqual(payload["source_label"], "你是")

    def test_normalize_reference_payload_truncates_and_omits_analysis(self) -> None:
        payload = normalize_reference_payload(
            {
                "passage": "<w:t>" + ("甲" * 620) + "</w:t>",
                "analysis": "<w:t>解析</w:t>",
            },
            passage_limit=600,
            omit_analysis=True,
        )

        self.assertTrue(payload["passage"].endswith("...(truncated)"))
        self.assertEqual(payload["analysis"], "[omitted_for_structure_only_fewshot]")

    def test_normalize_readable_structure_cleans_nested_payload_fields(self) -> None:
        payload = {
            "passage": "<w:p><w:t>第一段\\u00a0内容</w:t></w:p>\x07",
            "options": {"A": "<w:t>选项A</w:t>", "B": "普通项"},
            "nested": ["<w:t>附注</w:t>", "第二段"],
        }

        normalized = normalize_readable_structure(payload)

        self.assertEqual(normalized["passage"], "第一段 内容")
        self.assertEqual(normalized["options"]["A"], "选项A")
        self.assertEqual(normalized["nested"][0], "附注")
