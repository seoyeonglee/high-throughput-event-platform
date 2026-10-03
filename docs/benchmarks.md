# Durable throughput and recovery evidence

This benchmark measures the full **FastAPI → Redis Streams → worker → PostgreSQL**
reference stack. The public Render demo uses an ephemeral in-memory queue and is
**not** the system being measured here.

## Recorded results — 2026-10-03 UTC

Source: [`9f22610`](https://github.com/seoyeonglee/high-throughput-event-platform/commit/9f226100d1125ecd77e811066f1b334de80076aa), clean checkout.
All three runs used actual **PostgreSQL 17.11 + Redis 8.0.2**, installed from official
Debian packages in a cloud Linux workspace, with one API process and fresh service
data per scenario. They were **not Docker runs**: Docker is absent in that workspace.
The separate CI validation uses PostgreSQL 16 + Redis 7 through Compose.

Python 3.12.14; Linux x86-64; 9 logical CPUs exposed; approximately 9.7 GiB memory
reported. Detailed CPU topology and CPU quota are unavailable in the sandbox.
[Exact environment/dependencies](benchmark-results/2026-10-03/environment.txt).
All scenarios: concurrency 20, batch 100, 10,000 users, 250 ms observer interval,
600 s drain deadline, Redis AOF enabled; no separate warm-up.

| Events / workers | Requested / accepted / DB completed | Accepted events/s | Completed events/s | HTTP p95 / p99 (ms) | DB-observed p95 / p99 (s, upper bounds) | Timed load (s) |
|---|---:|---:|---:|---:|---:|---:|
| [10,000 / 1 worker](benchmark-results/2026-10-03/10000-worker1.json) | 10,000 / 10,000 / 10,000 | 7,758.9 | 533.8 | 538.2 / 1268.2 | 16.75 / 18.46 | 18.73 |
| [100,000 / 1 worker](benchmark-results/2026-10-03/100000-worker1.json) | 100,000 / 100,000 / 100,000 | 11,375.3 | 552.1 | 229.2 / 447.7 | 163.75 / 170.42 | 181.12 |
| [100,000 / 2 workers](benchmark-results/2026-10-03/100000-worker2.json) | 100,000 / 100,000 / 100,000 | 12,361.9 | 1,062.9 | 219.0 / 433.6 | 81.69 / 85.16 | 94.08 |

Every run had zero HTTP/network/accounting failures, zero duplicates, zero DLQ
entries, zero pending messages, and zero consumer-group lag at the final check
([queue snapshots](benchmark-results/2026-10-03/)). The two-worker 100K run measured
**1.93×** the one-worker completed throughput in this environment. This is one
comparison, not a general linear-scaling claim. The much higher acceptance rate
also demonstrates why HTTP throughput must not be advertised as completed throughput.

### A failed run is retained, too

On the GitHub Actions 4-vCPU / 15 GiB runner, PostgreSQL 16.15 + Redis 7 processing
was slower: the initial 100K run accepted 100,000 but observed only **76,537** committed
rows before its **600 s drain deadline**. It correctly exited 1 with `drain_timeout`,
with no request failures or DLQ entries. The worker was healthy and still draining
(100 pending; 23,264 not yet delivered at the later queue snapshot). This is not
reported as a successful 100K result or evidence of lost events.

- [Original failure JSON](benchmark-results/2026-10-03/ci-600s-timeout.json)
- [Runner and service environment](benchmark-results/2026-10-03/ci-timeout-environment.txt)
- [Final queue snapshot](benchmark-results/2026-10-03/ci-timeout-queue.txt)
- [Original CI run](https://github.com/seoyeonglee/high-throughput-event-platform/actions/runs/37079706180)

The CI drain bound is now 1200 s to allow measurement on the slower runner; error
checks and exact completion requirements remain unchanged. The latest exact-head
verification is available from [PR #1 checks](https://github.com/seoyeonglee/high-throughput-event-platform/pull/1/checks).

### Correctness checks

The same application code passed **74 tests**, including **16 real-service
integration cases** ([JUnit evidence](benchmark-results/2026-10-03/local-tests.xml)).
Ruff and the TypeScript/Vite production build passed. The default test command
reports 58 passed and 16 skipped unless the real-service tests are explicitly enabled.

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
  --worker-count 1 --drain-timeout 1200 --poll-interval 0.25 \
  --environment-label local-compose --output /evidence/10000-worker1.json

docker compose down -v --remove-orphans
docker compose up -d --wait api worker
docker compose run --rm benchmark \
  --events 100000 --concurrency 20 --batch-size 100 --users 10000 \
  --worker-count 1 --drain-timeout 1200 --poll-interval 0.25 \
  --environment-label local-compose --output /evidence/100000-worker1.json

# Only compare scaling after the correctness suite and one-worker runs pass.
docker compose down -v --remove-orphans
docker compose up -d --scale worker=2 --wait api worker
docker compose run --rm benchmark \
  --events 100000 --concurrency 20 --batch-size 100 --users 10000 \
  --worker-count 2 --drain-timeout 1200 --poll-interval 0.25 \
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
  --worker-count 1 --drain-timeout 1200 --output artifacts/10000-worker1.json
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
