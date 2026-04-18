from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import APIRouter
from fastapi.responses import JSONResponse
from sqlalchemy import text

from app.core.config import get_config_bundle, get_settings
from app.infra.db.session import engine


router = APIRouter(tags=["diagnostics"])


def build_passage_diagnostics() -> dict[str, Any]:
    settings = get_settings()
    checks = [
        _path_check("config_dir", settings.config_dir),
        _database_check(settings),
        _config_bundle_check(),
    ]
    return {
        "service": "passage_service",
        "status": _overall_status(checks),
        "checks": checks,
        "settings": {
            "config_dir": str(settings.config_dir),
            "database_url": settings.database_url,
            "resolved_database_url": settings.resolved_database_url,
            "resolved_database_path": str(settings.resolved_database_path) if settings.resolved_database_path else None,
            "database_mode": settings.database_mode,
            "allow_non_primary_database": settings.allow_non_primary_database,
            "expected_primary_database_name": settings.expected_primary_database_name,
            "db_pool_size": settings.db_pool_size,
            "db_max_overflow": settings.db_max_overflow,
            "db_pool_timeout_seconds": settings.db_pool_timeout_seconds,
            "db_pool_recycle_seconds": settings.db_pool_recycle_seconds,
            "disable_scheduler": settings.disable_scheduler,
        },
    }


@router.get("/healthz")
def healthcheck() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/readyz")
def readiness() -> JSONResponse:
    payload = build_passage_diagnostics()
    status_code = 200 if payload["status"] == "ready" else 503
    return JSONResponse(status_code=status_code, content=payload)


@router.get("/api/v1/diagnostics/runtime")
def runtime_diagnostics() -> dict[str, Any]:
    return build_passage_diagnostics()


def _path_check(name: str, path: Path) -> dict[str, Any]:
    exists = path.exists()
    return {
        "name": name,
        "status": "ok" if exists else "error",
        "critical": True,
        "details": {"path": str(path), "exists": exists},
    }


def _database_check(settings) -> dict[str, Any]:
    details = {
        "database_url": settings.database_url,
        "resolved_database_url": settings.resolved_database_url,
        "resolved_database_path": str(settings.resolved_database_path) if settings.resolved_database_path else None,
        "database_mode": settings.database_mode,
        "expected_primary_database_name": settings.expected_primary_database_name,
        "allow_non_primary_database": settings.allow_non_primary_database,
        "db_pool_size": settings.db_pool_size,
        "db_max_overflow": settings.db_max_overflow,
        "db_pool_timeout_seconds": settings.db_pool_timeout_seconds,
        "db_pool_recycle_seconds": settings.db_pool_recycle_seconds,
    }
    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
            material_span_count = connection.execute(text("SELECT COUNT(*) FROM material_spans")).scalar_one_or_none()
            primary_count = connection.execute(text("SELECT COUNT(*) FROM material_spans WHERE is_primary = 1")).scalar_one_or_none()
            v2_count = connection.execute(
                text(
                    "SELECT COUNT(*) FROM material_spans "
                    "WHERE is_primary = 1 AND v2_index_payload IS NOT NULL AND v2_index_payload <> '' AND v2_index_payload <> '{}'"
                )
            ).scalar_one_or_none()
        details["material_span_count"] = int(material_span_count or 0)
        details["primary_material_count"] = int(primary_count or 0)
        details["v2_indexed_primary_count"] = int(v2_count or 0)
        db_path = settings.resolved_database_path
        if db_path is not None:
            details["database_exists"] = db_path.exists()
            if db_path.exists():
                details["database_size_bytes"] = db_path.stat().st_size
        status = "ok"
        if not settings.database_is_primary and not settings.allow_non_primary_database:
            status = "error"
            details["reason"] = "non_primary_database_not_allowed"
        elif int(primary_count or 0) <= 0:
            status = "error"
            details["reason"] = "primary_material_pool_empty"
    except Exception as exc:  # noqa: BLE001
        status = "error"
        details["reason"] = str(exc)
    return {
        "name": "database",
        "status": status,
        "critical": True,
        "details": details,
    }


def _config_bundle_check() -> dict[str, Any]:
    details: dict[str, Any] = {}
    try:
        bundle = get_config_bundle()
        details["sources_count"] = len((bundle.sources or {}).get("sources", []))
        details["plugins_configured"] = len((bundle.plugins or {}).get("plugins", []))
        status = "ok"
    except Exception as exc:  # noqa: BLE001
        status = "error"
        details["reason"] = str(exc)
    return {
        "name": "config_bundle",
        "status": status,
        "critical": True,
        "details": details,
    }


def _overall_status(checks: list[dict[str, Any]]) -> str:
    if any(check["critical"] and check["status"] != "ok" for check in checks):
        return "not_ready"
    return "ready"
