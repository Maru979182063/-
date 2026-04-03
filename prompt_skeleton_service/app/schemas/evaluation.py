from __future__ import annotations

from pydantic import BaseModel, Field


class JudgeRaw(BaseModel):
    judge_prompt: str = ""


class JudgeResult(BaseModel):
    provider: str
    question_type_fit: float = 0.0
    difficulty_fit: float = 0.0
    material_alignment: float = 0.0
    distractor_quality: float = 0.0
    answer_analysis_consistency: float = 0.0
    overall_score: float = 0.0
    judge_reason: str = ""
    raw: JudgeRaw = Field(default_factory=JudgeRaw)


class ReviewMetricsSummaryResponse(BaseModel):
    total_count: int
    approved_count: int
    discarded_count: int
    auto_failed_count: int
    avg_review_rounds: float
    action_success_rate: float
