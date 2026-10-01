from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_name: str = "high-throughput-event-platform"
    app_env: str = "development"
    log_level: str = "INFO"

    database_url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/events"
    redis_url: str = "redis://localhost:6379/0"

    redis_stream: str = "events"
    redis_consumer_group: str = "event-workers"
    redis_dlq_stream: str = "events-dlq"

    idempotency_ttl_seconds: int = 86_400
    rate_limit_requests: int = 300
    rate_limit_window_seconds: int = 60

    worker_batch_size: int = 100
    worker_block_ms: int = 5_000
    worker_max_retries: int = 3
    worker_claim_idle_ms: int = 60_000


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
