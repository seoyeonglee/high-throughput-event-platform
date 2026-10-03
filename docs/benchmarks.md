# Durable throughput and recovery evidence

This benchmark measures the full **FastAPI → Redis Streams → worker → PostgreSQL**
reference stack. The public Render demo uses an ephemeral in-memory queue and is
**not** the system being measured here.

## What each number means

- **Requested:** unique event IDs the client planned to send.
- **Attempted:** events placed in HTTP requests, including failed requests.
- **Accepted:** the sum of validated HTTP 202 response `accepted` counts, never the
  number requested. Duplicate, rejected, malformed, non-202, and network outcomes
  are counted separately. A lost response is ambiguous even if the DB later commits.
- **DB completed:** unique IDs for this run observed in committed PostgreSQL rows.
  A read-only observer excludes unrelated IDs and refuses a reused run ID already
  present in the database. HTTP acceptance does not prove durable completion.
- **Ingestion events/s:** accepted events divided by the HTTP sending phase.
- **Completed events/s:** DB-completed events divided by the entire timed load,
  including ingestion and database drain. Metadata/preflight are outside this window.
- **Request p95/p99:** per-batch HTTP elapsed time, including failed attempts.
- **DB-observed p95/p99:** per-event time from its request start to first committed-row
  observation. These are polling **upper bounds**, not exact commit latency. They
  include queueing, processing, query duration, and observer scheduling. A nominal
  250 ms poll interval is not a guaranteed maximum measurement error.

A run exits nonzero for any request/accounting failure, duplicate response,
observer failure, or incomplete drain. It does not retry requests and does not
hide partial completion behind a success-only throughput figure.

## Reproduce with Docker Compose

Requirements: Docker with Compose v2. The isolated validation project creates its
own containers, uses no host ports, and requires no `.env` or production credentials.
Use the explicit project name below; do not run `down` against your application project.

```bash
export COMPOSE_FILE=compose.validation.yml
export COMPOSE_PROJECT_NAME=event-validation
mkdir -p artifacts

docker compose up -d --build --wait api worker
docker compose run --build --rm test

# Fresh database/queue for each scenario.
docker compose down -v --remove-orphans
docker compose up -d --wait api worker
docker compose run --build --rm benchmark \
  --events 10000 --concurrency 20 --batch-size 100 --users 10000 \
  --worker-count 1 --drain-timeout 600 --poll-interval 0.25 \
  --environment-label local-compose --output /evidence/10000-worker1.json

docker compose down -v --remove-orphans
docker compose up -d --wait api worker
docker compose run --rm benchmark \
  --events 100000 --concurrency 20 --batch-size 100 --users 10000 \
  --worker-count 1 --drain-timeout 600 --poll-interval 0.25 \
  --environment-label local-compose --output /evidence/100000-worker1.json

# Only compare scaling after the correctness suite and one-worker runs pass.
docker compose down -v --remove-orphans
docker compose up -d --scale worker=2 --wait api worker
docker compose run --rm benchmark \
  --events 100000 --concurrency 20 --batch-size 100 --users 10000 \
  --worker-count 2 --drain-timeout 600 --poll-interval 0.25 \
  --environment-label local-compose --output /evidence/100000-worker2.json

docker compose exec -T redis redis-cli XINFO GROUPS events
docker compose exec -T redis redis-cli XLEN events-dlq
docker compose down -v --remove-orphans
```

`RATE_LIMIT_REQUESTS=1000000` is explicit in the validation configuration so the
benchmark measures the pipeline rather than the default 300-requests/minute quota.
Production/default application settings are unchanged. Batch size is 100, client
concurrency is 20, and event IDs and users are distributed globally across batches.
API reload is disabled. Worker batch size is 100; each worker's SQLAlchemy pool is
10 connections plus 20 overflow. Redis AOF is enabled. Each comparison starts with
an empty DB/queue and has no separate warm-up.

If the API/worker are already running outside Docker:

```bash
# Supply a read-only-capable connection to the same PostgreSQL database.
export DATABASE_URL='postgresql+asyncpg://postgres:postgres@localhost:5432/events'
python scripts/benchmark.py --events 10000 --concurrency 20 --batch-size 100 \
  --worker-count 1 --drain-timeout 600 --output artifacts/10000-worker1.json
```

`--worker-count` is caller-provided metadata, not service discovery. DB URL and API
key are never serialized in reports. Do not point a load generator at a production
service without authorization.

## Reliability evidence

The opt-in integration suite uses actual Redis Streams and PostgreSQL, unique SQL
schemas and Redis keys, and spawned worker processes. It never flushes a shared DB.

| Scenario | Assertion |
|---|---|
| Failed stream append | Newly created dedup marker is removed; retry enqueues exactly once; existing markers are preserved |
| Concurrent duplicate HTTP ingestion | Exactly one stream entry and one accepted response |
| Duplicate queue deliveries | One event row and one aggregate update per ID |
| Out-of-order purchases | Correct event/purchase/amount totals; last-seen never moves backward |
| SIGKILL before commit | Both event and profile writes exist inside the uncommitted transaction; externally invisible; process death rolls back; replacement commits once |
| SIGKILL after commit, before ACK | Pending entry is reclaimed; committed event/aggregate are not counted twice |
| Malformed payload | Direct DLQ routing and ACK, no event row |
| Poison database constraint | Retry counts 0 → 1 → 2 → DLQ 3; healthy event still completes |
| Invalid retry metadata | DLQ instead of stranded pending entry |
| Concurrent/repeated retry or DLQ handling | One atomic destination append and source ACK |
| Failed DLQ append | Original remains pending and can be retried |
| Fresh pending prefix before stale tail | Reclaim cursor advances beyond the first scan page |

The worker now preserves `XAUTOCLAIM`'s cursor and uses a guarded Lua operation to
move pending messages and ACK together. A lost Redis response/repeated failure
handler cannot generate another retry/DLQ copy after the source is no longer
pending. `WORKER_CLAIM_INTERVAL_MS` defaults to 30000; idle threshold defaults to
60000 ms. Integration tests use shorter intervals so correctness checks are bounded.
These are at-least-once queue semantics with effectively-once PostgreSQL effects,
not an exactly-once transport guarantee.

## Limits and interpretation

- These are bounded synthetic product-view workloads on a shared runner, not a
  production capacity claim or an SLA. One run per scenario cannot establish stable
  speedup, confidence intervals, or maximum sustainable throughput.
- The observer itself queries PostgreSQL and consumes CPU/network resources. Its
  overhead is included in the measurement. Host caches and shared-runner contention
  remain uncontrolled even when containers are reset.
- Two workers add database connections and contention; throughput need not double.
  Report the observed result even if it is flat or worse.
- Tests cover worker process death and application-level retry boundaries. They do
  not prove durability across Redis host loss, PostgreSQL host loss, disk corruption,
  multi-region failure, or production failover. Redis AOF `everysec` has its normal
  persistence trade-off.
- The public demo has no durable Redis/PostgreSQL backend; its responsiveness and
  dashboard screenshot are not performance evidence for this reference architecture.

The `real-stack` GitHub Actions workflow retains raw JSON, resolved configuration,
runner/service versions, pytest/JUnit output, queue state, and service logs as an
artifact for 30 days. Permanent raw result snapshots are linked with each recorded run.
