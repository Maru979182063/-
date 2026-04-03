from functools import lru_cache

from app.core.settings import get_settings
from app.services.config_registry import ConfigRegistry
from app.services.prompt_template_registry import PromptTemplateRegistry
from app.services.question_repository import QuestionRepository
from app.services.runtime_registry import RuntimeConfigRegistry


@lru_cache(maxsize=1)
def get_registry() -> ConfigRegistry:
    settings = get_settings()
    return ConfigRegistry(settings.config_dir)


@lru_cache(maxsize=1)
def get_runtime_registry() -> RuntimeConfigRegistry:
    settings = get_settings()
    return RuntimeConfigRegistry(settings.runtime_config_path)


@lru_cache(maxsize=1)
def get_prompt_template_registry() -> PromptTemplateRegistry:
    settings = get_settings()
    return PromptTemplateRegistry(settings.prompt_template_config_path)


@lru_cache(maxsize=1)
def get_question_repository() -> QuestionRepository:
    settings = get_settings()
    return QuestionRepository(settings.question_db_path)
