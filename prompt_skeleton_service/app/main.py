from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from app.core.dependencies import get_prompt_template_registry, get_question_repository, get_registry, get_runtime_registry
from app.core.exceptions import register_exception_handlers
from app.core.security import install_security_middleware
from app.routers.admin import router as admin_router
from app.routers.diagnostics import router as diagnostics_router
from app.routers.demo import router as demo_router
from app.routers.meta import router as meta_router
from app.routers.metrics import router as metrics_router
from app.routers.prompt import router as prompt_router
from app.routers.questions import router as questions_router
from app.routers.review import router as review_router
from app.routers.slots import router as slots_router
from app.routers.types import router as types_router
from app.services.diagnostics import build_prompt_diagnostics

DEMO_STATIC_DIR = Path(__file__).resolve().parent / "demo_static"


def create_app() -> FastAPI:
    app = FastAPI(
        title="Prompt Skeleton Service",
        version="0.1.0",
        description="Config-driven FastAPI prompt skeleton service for Dify.",
    )
    install_security_middleware(app)

    app.include_router(types_router)
    app.include_router(slots_router)
    app.include_router(prompt_router)
    app.include_router(demo_router)
    app.include_router(meta_router)
    app.include_router(questions_router)
    app.include_router(review_router)
    app.include_router(metrics_router)
    app.include_router(diagnostics_router)
    app.include_router(admin_router)
    app.mount("/demo-static", StaticFiles(directory=DEMO_STATIC_DIR), name="demo-static")
    register_exception_handlers(app)

    @app.on_event("startup")
    def startup() -> None:
        get_registry().load()
        get_runtime_registry().load()
        get_prompt_template_registry().load()
        get_question_repository()

    @app.get("/healthz", tags=["health"])
    def healthcheck() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/readyz", tags=["health"])
    def readiness() -> JSONResponse:
        payload = build_prompt_diagnostics()
        status_code = 200 if payload["status"] == "ready" else 503
        return JSONResponse(status_code=status_code, content=payload)

    return app


app = create_app()
