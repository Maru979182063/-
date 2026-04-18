from fastapi import FastAPI
from sqlalchemy import text

from app.api.router import api_router
from app.core.config import get_settings
from app.core.observability import install_observability, register_exception_handlers
from app.infra.db.session import engine, init_db
from app.infra.plugins.loader import load_plugins
from app.jobs.scheduler import setup_scheduler, shutdown_scheduler


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(title=settings.app_name, version=settings.app_version)
    install_observability(app)
    register_exception_handlers(app)
    app.include_router(api_router)

    @app.on_event("startup")
    def on_startup() -> None:
        _enforce_runtime_database_guard(settings)
        init_db()
        load_plugins()
        if not settings.disable_scheduler:
            setup_scheduler(run_scheduled_crawl)

    @app.on_event("shutdown")
    def on_shutdown() -> None:
        shutdown_scheduler()

    return app


def run_scheduled_crawl(source_id: str) -> None:
    from app.domain.services.ingest_service import run_crawl_for_source
    from app.infra.db.session import get_session

    session = get_session()
    try:
        run_crawl_for_source(session, source_id)
    finally:
        session.close()


def _enforce_runtime_database_guard(settings) -> None:
    db_path = settings.resolved_database_path
    if db_path is not None and not db_path.exists():
        raise RuntimeError(
            f"Configured passage_service database does not exist: {db_path}. "
            "Refuse to start with a missing database."
        )
    if not settings.database_is_primary and not settings.allow_non_primary_database:
        raise RuntimeError(
            "passage_service is pointing at a non-primary database "
            f"({settings.database_mode}: {db_path}) but PASSAGE_ALLOW_NON_PRIMARY_DATABASE is not enabled."
        )
    with engine.connect() as connection:
        primary_count = connection.execute(text("SELECT COUNT(*) FROM material_spans WHERE is_primary = 1")).scalar_one_or_none()
    if int(primary_count or 0) <= 0:
        raise RuntimeError(
            f"Configured passage_service database has no primary materials: {db_path}. "
            "Refuse to start an empty retrieval service."
        )


app = create_app()
