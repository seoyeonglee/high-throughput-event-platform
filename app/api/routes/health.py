from fastapi import APIRouter, status
from sqlalchemy import text

from app.core.database import async_session_maker
from app.core.redis import get_redis

router = APIRouter(tags=["health"])


@router.get("/health/live")
async def liveness() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/health/ready", status_code=status.HTTP_200_OK)
async def readiness() -> dict[str, str]:
    async with async_session_maker() as session:
        await session.execute(text("SELECT 1"))
    redis = await get_redis()
    await redis.ping()
    return {"status": "ready", "postgres": "ok", "redis": "ok"}
