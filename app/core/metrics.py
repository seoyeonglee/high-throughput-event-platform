from prometheus_client import Counter, Histogram

EVENTS_ACCEPTED = Counter(
    "events_accepted_total",
    "Events accepted by the ingestion API",
    ["event_type"],
)
EVENTS_DUPLICATE = Counter(
    "events_duplicate_total",
    "Duplicate events rejected by the ingestion API",
    ["event_type"],
)
EVENTS_PROCESSED = Counter(
    "events_processed_total",
    "Events successfully processed by workers",
    ["event_type"],
)
EVENTS_FAILED = Counter(
    "events_failed_total",
    "Event processing failures",
    ["stage"],
)
BATCH_SIZE = Histogram(
    "event_batch_size",
    "Number of events submitted per batch request",
    buckets=(1, 10, 50, 100, 250, 500, 1000),
)
PROCESSING_SECONDS = Histogram(
    "event_processing_seconds",
    "Worker processing latency per event",
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5),
)
