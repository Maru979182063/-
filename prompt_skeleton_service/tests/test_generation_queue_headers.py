from __future__ import annotations

from unittest import TestCase
from unittest.mock import patch

from fastapi.testclient import TestClient

from app.main import app


class GenerationQueueHeaderTest(TestCase):
    def setUp(self) -> None:
        self.client = TestClient(app)

    def tearDown(self) -> None:
        self.client.close()

    def test_generate_response_exposes_queue_headers(self) -> None:
        with patch(
            "app.routers.questions.QuestionGenerationService.generate",
            return_value={
                "batch_id": "batch-1",
                "batch_meta": {
                    "requested_count": 1,
                    "effective_count": 1,
                    "question_type": "main_idea",
                    "business_subtype": "center_understanding",
                    "pattern_id": "pattern-1",
                    "difficulty_target": "medium",
                },
                "items": [],
                "warnings": [],
                "notes": [],
            },
        ):
            response = self.client.post(
                "/api/v1/questions/generate",
                json={
                    "question_focus": "center_understanding",
                    "difficulty_level": "medium",
                    "count": 1,
                },
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers.get("X-Generation-Queue-Position"), "0")
        self.assertEqual(response.headers.get("X-Generation-Wait-Seconds"), "0.0")
        self.assertEqual(response.headers.get("X-Generation-Max-Concurrent"), "2")
        self.assertEqual(response.headers.get("X-Generation-Max-Waiting"), "12")
