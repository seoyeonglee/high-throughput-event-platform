from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, Field, field_validator


class EventIn(BaseModel):
    event_id: str = Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9_.:-]+$")
    user_id: str = Field(min_length=1, max_length=128)
    event_type: str = Field(min_length=1, max_length=64, pattern=r"^[a-z0-9_.-]+$")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    properties: dict[str, Any] = Field(default_factory=dict)

    @field_validator("timestamp")
    @classmethod
    def timestamp_must_be_timezone_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("timestamp must include a timezone")
        return value.astimezone(UTC)


class BatchIngestRequest(BaseModel):
    events: list[EventIn] = Field(min_length=1, max_length=1000)


class IngestResponse(BaseModel):
    event_id: str
    accepted: bool
    status: str
    request_id: str | None = None


class BatchIngestResponse(BaseModel):
    received: int
    accepted: int
    duplicates: int
