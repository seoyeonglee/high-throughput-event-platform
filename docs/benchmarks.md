# Benchmark Guide

This repository deliberately does **not** publish invented performance numbers. Run the benchmark on your own machine or deployment and commit the measured output.

## Run

```bash
docker compose up --build
python scripts/benchmark.py --events 10000 --concurrency 100 --batch-size 100
python scripts/benchmark.py --events 100000 --concurrency 200 --batch-size 250
```

## Record

| Scenario | Events | Concurrency | Batch | Throughput | p50 | p95 | p99 | Failures |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Local baseline | 10,000 | 100 | 100 | _run locally_ | _run locally_ | _run locally_ | _run locally_ | _run locally_ |
| Local stress | 100,000 | 200 | 250 | _run locally_ | _run locally_ | _run locally_ | _run locally_ | _run locally_ |

## What to observe

- API p95/p99 latency while the stream backlog grows;
- event throughput versus worker throughput;
- Redis stream length and consumer-group pending entries;
- PostgreSQL CPU, connections, and transaction latency;
- DLQ growth and retry rate.

A useful follow-up experiment is to increase worker replicas and demonstrate that processing throughput scales independently from API ingestion capacity.
