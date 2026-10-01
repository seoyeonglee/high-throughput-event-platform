from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import make_asgi_app

from app.api.router import api_router
from app.core.config import settings
from app.core.database import close_database, init_database
from app.core.redis import close_redis, get_redis
from app.core.streams import ensure_consumer_group


@asynccontextmanager
async def lifespan(_: FastAPI):
    await init_database()
    redis = await get_redis()
    await ensure_consumer_group(redis)
    yield
    await close_redis()
    await close_database()


app = FastAPI(
    title="High Throughput Event Platform",
    version="1.0.0",
    description=(
        "Event ingestion API backed by Redis Streams and PostgreSQL, with "
        "idempotency, retries, dead-letter handling, rate limiting, and metrics."
    ),
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router)
app.mount("/metrics", make_asgi_app())


@app.get("/", tags=["meta"])
async def root() -> dict[str, str]:
    return {
        "service": settings.app_name,
        "status": "ok",
        "docs": "/docs",
        "metrics": "/metrics",
    }
