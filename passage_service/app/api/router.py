from fastapi import APIRouter

from app.api.routes import articles, feedback, jobs, materials, materials_v2, passages


api_router = APIRouter()
api_router.include_router(articles.router, tags=["articles"])
api_router.include_router(materials.router, tags=["materials"])
api_router.include_router(materials_v2.router, tags=["materials-v2"])
api_router.include_router(feedback.router, tags=["feedback"])
api_router.include_router(jobs.router, tags=["jobs"])
api_router.include_router(passages.router, tags=["passages"])
