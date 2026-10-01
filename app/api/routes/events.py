from fastapi import APIRouter, Depends, Request, status

from app.core.metrics import BATCH_SIZE, EVENTS_ACCEPTED, EVENTS_DUPLICATE
from app.schemas.event import BatchIngestRequest, BatchIngestResponse, EventIn, IngestResponse
from app.services.ingestion import EventIngestionService
from app.services.rate_limit import enforce_rate_limit

router = APIRouter(prefix="/events", tags=["events"])


async def ingestion_service() -> EventIngestionService:
    return await EventIngestionService.create()


@router.post("", response_model=IngestResponse, status_code=status.HTTP_202_ACCEPTED)
async def ingest_event(
    payload: EventIn,
    request: Request,
    _: None = Depends(enforce_rate_limit),
    service: EventIngestionService = Depends(ingestion_service),
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
    _: None = Depends(enforce_rate_limit),
    service: EventIngestionService = Depends(ingestion_service),
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
