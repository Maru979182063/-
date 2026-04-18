from functools import lru_cache
import os
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="PASSAGE_",
        extra="ignore",
        env_file=str(Path(__file__).resolve().parents[2] / ".env"),
        env_file_encoding="utf-8",
    )

    app_name: str = "passage-service"
    app_version: str = "0.1.0"
    database_url: str = "sqlite:///./passage_service.db"
    config_dir: Path = Path(__file__).resolve().parent.parent / "config"
    openai_api_key: str | None = None
    openai_base_url: str = "https://api.openai.com/v1"
    disable_scheduler: bool = False
    allow_non_primary_database: bool = False
    expected_primary_database_name: str = "passage_service.db"
    db_pool_size: int = 20
    db_max_overflow: int = 40
    db_pool_timeout_seconds: float = 60.0
    db_pool_recycle_seconds: int = 1800

    @property
    def service_root(self) -> Path:
        return Path(__file__).resolve().parents[2]

    @property
    def resolved_database_url(self) -> str:
        raw = (self.database_url or "").strip()
        if not raw.startswith("sqlite:///"):
            return raw
        suffix = raw[len("sqlite:///") :]
        db_path = Path(suffix)
        if not db_path.is_absolute():
            db_path = (self.service_root / db_path).resolve()
        return f"sqlite:///{db_path.as_posix()}"

    @property
    def resolved_database_path(self) -> Path | None:
        raw = (self.resolved_database_url or "").strip()
        if not raw.startswith("sqlite:///"):
            return None
        return Path(raw[len("sqlite:///") :]).resolve()

    @property
    def database_mode(self) -> str:
        db_path = self.resolved_database_path
        if db_path is None:
            return "non_sqlite"
        name = db_path.name.lower()
        expected = self.expected_primary_database_name.lower()
        if name == expected:
            return "primary"
        if ".mvp." in name or name.endswith(".mvp.db"):
            return "mvp"
        if ".dev." in name or name.endswith(".dev.db"):
            return "dev"
        return "custom"

    @property
    def database_is_primary(self) -> bool:
        return self.database_mode == "primary"

    @property
    def database_guard(self) -> dict[str, Any]:
        db_path = self.resolved_database_path
        return {
            "configured_database_url": self.database_url,
            "resolved_database_url": self.resolved_database_url,
            "resolved_database_path": str(db_path) if db_path else None,
            "database_mode": self.database_mode,
            "expected_primary_database_name": self.expected_primary_database_name,
            "allow_non_primary_database": self.allow_non_primary_database,
            "db_pool_size": self.db_pool_size,
            "db_max_overflow": self.db_max_overflow,
            "db_pool_timeout_seconds": self.db_pool_timeout_seconds,
            "db_pool_recycle_seconds": self.db_pool_recycle_seconds,
        }

    @property
    def resolved_openai_api_key(self) -> str | None:
        return (
            self.openai_api_key
            or os.getenv("MATERIAL_LLM_API_KEY")
            or os.getenv("GENERATION_LLM_API_KEY")
        )

    @property
    def resolved_openai_base_url(self) -> str:
        return (
            (self.openai_base_url or "").strip()
            or os.getenv("MATERIAL_LLM_BASE_URL", "").strip()
            or os.getenv("GENERATION_LLM_BASE_URL", "").strip()
            or "https://api.openai.com/v1"
        )


class ConfigBundle(BaseModel):
    app: dict[str, Any]
    knowledge_tree: dict[str, Any]
    document_genres: dict[str, Any]
    plugins: dict[str, Any]
    segmentation: dict[str, Any]
    family_routing: dict[str, Any]
    material_governance: dict[str, Any]
    llm: dict[str, Any]
    fit_mapping: dict[str, Any]
    release: dict[str, Any]
    sync: dict[str, Any]
    sources: dict[str, Any]
    source_scope_catalog: dict[str, Any]


@lru_cache
def get_settings() -> Settings:
    return Settings()


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


@lru_cache
def get_config_bundle() -> ConfigBundle:
    config_dir = get_settings().config_dir
    return ConfigBundle(
        app=_read_yaml(config_dir / "app.yaml"),
        knowledge_tree=_read_yaml(config_dir / "knowledge_tree.yaml"),
        document_genres=_read_yaml(config_dir / "document_genres.yaml"),
        plugins=_read_yaml(config_dir / "plugins.yaml"),
        segmentation=_read_yaml(config_dir / "segmentation.yaml"),
        family_routing=_read_yaml(config_dir / "family_routing.yaml"),
        material_governance=_read_yaml(config_dir / "material_governance.yaml"),
        llm=_read_yaml(config_dir / "llm.yaml"),
        fit_mapping=_read_yaml(config_dir / "fit_mapping.yaml"),
        release=_read_yaml(config_dir / "release.yaml"),
        sync=_read_yaml(config_dir / "sync.yaml"),
        sources=_read_yaml(config_dir / "sources.yaml"),
        source_scope_catalog=_read_yaml(config_dir / "source_scope_catalog.yaml"),
    )
