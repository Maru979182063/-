from __future__ import annotations

from pydantic import BaseModel, Field


class ModelRoutingConfig(BaseModel):
    default_generation: str
    question_generation: str | None = None
    question_repair: str | None = None
    minor_edit: str
    question_modify: str
    text_modify: str
    judge_review: str | None = None
    reference_parse: str | None = None


class ProviderParamsConfig(BaseModel):
    temperature: float = 0.7
    max_output_tokens: int = 4000
    timeout_seconds: int = 90


class ProviderConfig(BaseModel):
    api_key_env: str
    base_url_env: str | None = None
    default_base_url: str = "https://api.openai.com/v1"
    models: ModelRoutingConfig
    params: ProviderParamsConfig = Field(default_factory=ProviderParamsConfig)


class OperationRouteConfig(BaseModel):
    provider: str
    model_key: str


class ReviewRoutingConfig(BaseModel):
    minor_edit: OperationRouteConfig
    question_modify: OperationRouteConfig
    text_modify: OperationRouteConfig


class LLMRoutingConfig(BaseModel):
    generate_question: OperationRouteConfig
    question_generation: OperationRouteConfig | None = None
    question_repair: OperationRouteConfig | None = None
    material_refinement: OperationRouteConfig | None = None
    review_actions: ReviewRoutingConfig
    source_question_parse: OperationRouteConfig | None = None


class LLMConfig(BaseModel):
    active_provider: str
    providers: dict[str, ProviderConfig]
    routing: LLMRoutingConfig


class MaterialsConfig(BaseModel):
    base_url: str
    search_path: str = "/materials/search"
    v2_search_path: str = "/materials/v2/search"
    default_status: str = "promoted"
    default_release_channel: str = "stable"
    candidate_pool_size: int = 24


class PersistenceConfig(BaseModel):
    sqlite_path: str = "data/question_workbench.db"


class JudgeRouteConfig(BaseModel):
    provider: str = "openai"
    enabled: bool = True
    model_key: str = "judge_review"


class EvaluationConfig(BaseModel):
    judge: JudgeRouteConfig = Field(default_factory=JudgeRouteConfig)


class QuestionRuntimeConfig(BaseModel):
    llm: LLMConfig
    materials: MaterialsConfig
    persistence: PersistenceConfig = Field(default_factory=PersistenceConfig)
    evaluation: EvaluationConfig = Field(default_factory=EvaluationConfig)
