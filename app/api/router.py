from fastapi import APIRouter

from app.api.routes import analytics, events, health, users

api_router = APIRouter()
api_router.include_router(health.router)
api_router.include_router(events.router, prefix="/v1")
api_router.include_router(users.router, prefix="/v1")
api_router.include_router(analytics.router, prefix="/v1")
