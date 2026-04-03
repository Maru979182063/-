from fastapi import FastAPI

from app.api.router import api_router
from app.core.config import get_settings
from app.infra.db.session import init_db
from app.infra.plugins.loader import load_plugins
from app.jobs.scheduler import setup_scheduler, shutdown_scheduler


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(title=settings.app_name, version=settings.app_version)
    app.include_router(api_router)

    @app.on_event("startup")
    def on_startup() -> None:
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


app = create_app()
