import time

from fastapi import HTTPException, Request, status

from app.core.config import settings
from app.core.redis import get_redis


async def enforce_rate_limit(request: Request) -> None:
    redis = await get_redis()
    identity = request.headers.get("x-api-key")
    if not identity:
        identity = request.client.host if request.client else "unknown"

    window = settings.rate_limit_window_seconds
    current_window = int(time.time()) // window
    key = f"rate:{identity}:{current_window}"

    count = await redis.incr(key)
    if count == 1:
        await redis.expire(key, window + 1)
    if count > settings.rate_limit_requests:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Rate limit exceeded",
            headers={"Retry-After": str(window)},
        )
