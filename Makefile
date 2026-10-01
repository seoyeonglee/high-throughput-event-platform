.PHONY: up down logs test lint seed benchmark

up:
	docker compose up --build

down:
	docker compose down

logs:
	docker compose logs -f api worker

test:
	pytest -q

lint:
	ruff check .

seed:
	python scripts/generate_events.py --count 1000 --batch-size 100

benchmark:
	python scripts/benchmark.py --events 10000 --concurrency 100 --batch-size 100
