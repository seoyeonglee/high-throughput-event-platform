from typing import Annotated, Any

from fastapi import APIRouter, Depends, Query, Request, status
from pydantic import BaseModel
from sqlalchemy import select

from app.core.database import async_session_maker
from app.core.metrics import BATCH_SIZE, EVENTS_ACCEPTED, EVENTS_DUPLICATE
from app.models.event import Event
from app.schemas.event import BatchIngestRequest, BatchIngestResponse, EventIn, IngestResponse
from app.services.ingestion import EventIngestionService
from app.services.rate_limit import enforce_rate_limit

router = APIRouter(prefix="/events", tags=["events"])


class RecentEventOut(BaseModel):
    event_id: str
    user_id: str
    event_type: str
    occurred_at: str
    received_at: str
    properties: dict[str, Any]


async def ingestion_service() -> EventIngestionService:
    return await EventIngestionService.create()


RateLimitDependency = Annotated[None, Depends(enforce_rate_limit)]
IngestionDependency = Annotated[EventIngestionService, Depends(ingestion_service)]


@router.post("", response_model=IngestResponse, status_code=status.HTTP_202_ACCEPTED)
async def ingest_event(
    payload: EventIn,
    request: Request,
    _: RateLimitDependency,
    service: IngestionDependency,
) -> IngestResponse:
    accepted = await service.enqueue(payload)
    label = payload.event_type
    if accepted:
        EVENTS_ACCEPTED.labels(event_type=label).inc()
    else:
        EVENTS_DUPLICATE.labels(event_type=label).inc()
    return IngestResponse(
        event_id=payload.event_id,
        accepted=accepted,
        status="queued" if accepted else "duplicate",
        request_id=request.headers.get("x-request-id"),
    )


@router.post("/batch", response_model=BatchIngestResponse, status_code=status.HTTP_202_ACCEPTED)
async def ingest_batch(
    payload: BatchIngestRequest,
    _: RateLimitDependency,
    service: IngestionDependency,
) -> BatchIngestResponse:
    BATCH_SIZE.observe(len(payload.events))
    results = await service.enqueue_batch(payload.events)
    accepted = sum(results)
    duplicates = len(results) - accepted
    for event, was_accepted in zip(payload.events, results, strict=True):
        metric = EVENTS_ACCEPTED if was_accepted else EVENTS_DUPLICATE
        metric.labels(event_type=event.event_type).inc()
    return BatchIngestResponse(
        received=len(payload.events),
        accepted=accepted,
        duplicates=duplicates,
    )


@router.get("/recent", response_model=list[RecentEventOut])
async def recent_events(
    limit: Annotated[int, Query(ge=1, le=100)] = 12,
) -> list[RecentEventOut]:
    async with async_session_maker() as session:
        rows = (
            await session.execute(
                select(Event).order_by(Event.occurred_at.desc()).limit(limit)
            )
        ).scalars().all()

    return [
        RecentEventOut(
            event_id=row.event_id,
            user_id=row.user_id,
            event_type=row.event_type,
            occurred_at=row.occurred_at.isoformat(),
            received_at=row.received_at.isoformat() if row.received_at else "",
            properties=row.properties,
        )
        for row in rows
    ]
