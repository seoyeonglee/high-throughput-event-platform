"""Benchmark accounting uses observed outcomes, never requested/elapsed."""

import asyncio
import json
from types import SimpleNamespace

import httpx
import pytest

from scripts import benchmark


def configuration(**overrides):
    values = {
        "events": 7,
        "batch_size": 3,
        "concurrency": 2,
        "run_id": "test-run",
        "poll_interval": 0.001,
        "drain_timeout": 0.02,
        "request_timeout": 0.1,
        "users": 5,
    }
    values.update(overrides)
    return benchmark.BenchmarkConfig(**values)


class Observer:
    def __init__(self):
        self.ids = set()
        self.calls = 0

    async def __call__(self, run_id):
        self.calls += 1
        return set(self.ids)


async def execute(handler, observer, **overrides):
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        return await benchmark.run_benchmark(configuration(**overrides), client, observer)


@pytest.mark.asyncio
async def test_success_counts_actual_batches_unique_database_rows_and_global_users():
    observer = Observer()
    batches = []

    async def handler(request):
        events = json.loads(request.content)["events"]
        batches.append(events)
        observer.ids.update(event["event_id"] for event in events)
        return httpx.Response(
            202, json={"received": len(events), "accepted": len(events), "duplicates": 0}
        )

    report = await execute(handler, observer)

    assert sorted(map(len, batches)) == [1, 3, 3]
    events = [event for batch in batches for event in batch]
    assert len({event["event_id"] for event in events}) == 7
    assert all(len(event["event_id"]) <= 64 for event in events)
    assert [event["user_id"] for event in events] == [f"bench_user_{i % 5}" for i in range(7)]
    assert report["events"]["requested"] == report["events"]["accepted"] == 7
    assert report["events"]["db_completed"] == 7
    assert report["requests"]["attempted"] == report["requests"]["accepted"] == 3
    assert report["completion"]["all_requested_completed"] is True
    assert report["completion"]["timed_out"] is False
    assert report["success"] is True
    assert report["exit_code"] == 0
    assert report["throughput"]["ingestion_events_per_second"] == pytest.approx(
        7 / report["timing"]["ingestion_seconds"]
    )
    assert report["throughput"]["completed_events_per_second"] == pytest.approx(
        7 / report["timing"]["total_seconds"]
    )
    assert set(report["request_latency_ms"]) >= {"p50", "p95", "p99", "count"}
    assert report["db_observed_end_to_end_latency_ms"]["count"] == 7
    assert report["db_observed_end_to_end_latency_ms"]["measurement"] == "polling_upper_bound"
    assert report["db_observed_end_to_end_latency_ms"]["poll_interval_seconds"] == 0.001
    assert report["environment"]["python"]
    assert report["config"]["users"] == 5
    assert observer.calls >= 2


@pytest.mark.asyncio
async def test_mixed_responses_account_every_event_without_inflating_ingestion():
    observer = Observer()
    call = 0

    async def handler(request):
        nonlocal call
        call += 1
        if call == 1:
            return httpx.Response(202, json={"received": 2, "accepted": 1, "duplicates": 1})
        if call == 2:
            return httpx.Response(
                202, json={"received": 2, "accepted": 1, "duplicates": 0, "rejected": 1}
            )
        if call == 3:
            return httpx.Response(429)
        if call == 4:
            raise httpx.ConnectError("connection lost", request=request)
        return httpx.Response(202, json={"unexpected": True})

    report = await execute(handler, observer, events=10, batch_size=2, concurrency=1)

    assert report["events"]["requested"] == report["events"]["attempted"] == 10
    assert {
        key: report["events"][key]
        for key in (
            "accepted",
            "duplicates",
            "rejected",
            "http_error",
            "network_failure",
            "malformed_response",
            "db_completed",
        )
    } == {
        "accepted": 2,
        "duplicates": 1,
        "rejected": 1,
        "http_error": 2,
        "network_failure": 2,
        "malformed_response": 2,
        "db_completed": 0,
    }
    assert report["requests"]["http_status_counts"] == {"202": 3, "429": 1}
    assert report["completion"]["timed_out"] is True
    assert report["throughput"]["ingestion_events_per_second"] == pytest.approx(
        2 / report["timing"]["ingestion_seconds"]
    )
    assert report["throughput"]["completed_events_per_second"] == 0
    assert report["db_observed_end_to_end_latency_ms"]["p50"] is None
    assert report["success"] is False
    assert report["exit_code"] == 1


@pytest.mark.parametrize("status", [200, 201, 204, 302, 400, 500])
@pytest.mark.asyncio
async def test_only_http_202_is_accepted(status):
    report = await execute(lambda request: httpx.Response(status), Observer(), events=1)
    assert report["events"]["accepted"] == 0
    assert report["events"]["http_error"] == 1
    assert report["exit_code"] == 1


@pytest.mark.parametrize(
    "body",
    [
        {},
        [],
        {"received": 1, "accepted": True, "duplicates": 0},
        {"received": 1, "accepted": -1, "duplicates": 2},
        {"received": 1, "accepted": 2, "duplicates": 0},
        {"received": 2, "accepted": 1, "duplicates": 0},
        {"received": 1, "accepted": 0, "duplicates": 0},
        {"received": 1, "accepted": "1", "duplicates": 0},
    ],
)
@pytest.mark.asyncio
async def test_malformed_or_inconsistent_counts_never_count_as_accepted(body):
    report = await execute(lambda request: httpx.Response(202, json=body), Observer(), events=1)
    assert report["events"]["malformed_response"] == 1
    assert report["events"]["accepted"] == 0
    assert report["requests"]["malformed_response"] == 1
    assert report["exit_code"] == 1


@pytest.mark.asyncio
async def test_non_json_response_is_reported():
    report = await execute(
        lambda request: httpx.Response(202, text="invalid"), Observer(), events=1
    )
    assert report["events"]["malformed_response"] == 1


@pytest.mark.asyncio
async def test_database_completion_waits_for_delayed_rows_and_ignores_unrelated_ids():
    submitted = set()
    release = asyncio.Event()

    async def poll(run_id):
        if release.is_set():
            return submitted | {"another_run_1", "bench_test-run_999"}
        return set()

    async def handler(request):
        submitted.update(event["event_id"] for event in json.loads(request.content)["events"])
        asyncio.get_running_loop().call_later(0.01, release.set)
        return httpx.Response(202, json={"received": 1, "accepted": 1, "duplicates": 0})

    report = await execute(handler, poll, events=1, drain_timeout=0.1)
    assert report["events"]["db_completed"] == 1
    assert report["timing"]["drain_seconds"] >= 0.005
    assert report["completion"]["all_requested_completed"] is True


@pytest.mark.asyncio
async def test_database_completion_observed_before_response_does_not_need_another_poll():
    observer = Observer()

    async def handler(request):
        observer.ids.update(event["event_id"] for event in json.loads(request.content)["events"])
        await asyncio.sleep(0.015)
        return httpx.Response(202, json={"received": 1, "accepted": 1, "duplicates": 0})

    report = await execute(
        handler, observer, events=1, poll_interval=0.01, drain_timeout=0.001
    )

    assert report["events"]["accepted"] == report["events"]["db_completed"] == 1
    assert report["completion"]["all_requested_completed"] is True
    assert report["completion"]["timed_out"] is False
    assert report["exit_code"] == 0
    assert report["throughput"]["completed_rate_window"] == (
        "load_start_to_end_of_ingestion_and_database_drain"
    )
    assert "excludes metadata collection and preflight" in report["timing"]["description"]
    assert "both sender completion and database observation" in report["timing"]["description"]


@pytest.mark.asyncio
async def test_existing_run_ids_abort_before_sending_to_avoid_reusing_old_completions():
    observer = Observer()
    observer.ids.add("bench_test-run_0")

    async def handler(request):
        pytest.fail("a reused run must not send traffic")

    report = await execute(handler, observer)
    assert report["events"]["attempted"] == 0
    assert report["completion"]["reason"] == "run_id_collision"
    assert report["exit_code"] == 1


@pytest.mark.asyncio
async def test_database_errors_abort_with_explicit_failure_report():
    async def observer(run_id):
        raise OSError("database unavailable")

    report = await execute(lambda request: pytest.fail("no traffic without DB observer"), observer)
    assert report["events"]["attempted"] == 0
    assert report["completion"]["reason"] == "observer_error"
    assert report["completion"]["observer_errors"] == ["OSError"]
    assert report["exit_code"] == 1


@pytest.mark.asyncio
async def test_sender_task_count_is_bounded_not_one_task_per_request(monkeypatch):
    observer = Observer()
    original = asyncio.create_task
    created = []

    def create_task(coro, *args, **kwargs):
        created.append(coro)
        return original(coro, *args, **kwargs)

    monkeypatch.setattr(asyncio, "create_task", create_task)

    async def handler(request):
        events = json.loads(request.content)["events"]
        observer.ids.update(event["event_id"] for event in events)
        await asyncio.sleep(0)
        return httpx.Response(202, json={"received": 1, "accepted": 1, "duplicates": 0})

    report = await execute(handler, observer, events=1000, batch_size=1, concurrency=3)
    assert len(created) <= 4
    assert report["requests"]["attempted"] == 1000
    assert report["exit_code"] == 0


@pytest.mark.parametrize(
    "arguments",
    [
        ["--events", "0"],
        ["--events", "-1"],
        ["--concurrency", "0"],
        ["--batch-size", "0"],
        ["--batch-size", "1001"],
        ["--drain-timeout", "0"],
        ["--poll-interval", "-1"],
        ["--request-timeout", "nan"],
        ["--users", "0"],
        ["--run-id", "a" * 41],
        ["--run-id", "has spaces"],
        ["--url", "file:///tmp/a"],
    ],
)
def test_invalid_cli_arguments_fail_with_usage(arguments):
    with pytest.raises(SystemExit) as error:
        benchmark.parse_args(arguments)
    assert error.value.code == 2


def test_cli_defaults_stay_compatible_and_database_url_uses_environment(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://user:secret@db:5432/events")
    args = benchmark.parse_args([])
    assert (args.events, args.concurrency, args.batch_size) == (10000, 100, 100)
    assert args.database_url == "postgresql+asyncpg://user:secret@db:5432/events"


def test_percentile_empty_is_unavailable_instead_of_fake_zero():
    assert benchmark.percentile([], 50) is None
    assert benchmark.percentile([1, 2, 3, 4], 50) == 2


@pytest.mark.asyncio
async def test_drain_deadline_bounds_an_observer_query_that_started_during_ingestion():
    submitted = asyncio.Event()
    calls = 0

    async def observer(run_id):
        nonlocal calls
        calls += 1
        if calls == 1:
            return set()
        await submitted.wait()
        await asyncio.sleep(1)
        return set()

    async def handler(request):
        submitted.set()
        return httpx.Response(202, json={"received": 1, "accepted": 1, "duplicates": 0})

    started = asyncio.get_running_loop().time()
    report = await execute(handler, observer, events=1, drain_timeout=0.02, request_timeout=0.3)
    elapsed = asyncio.get_running_loop().time() - started
    assert report["completion"]["timed_out"] is True
    assert report["completion"]["reason"] == "drain_timeout"
    assert elapsed < 0.2


@pytest.mark.asyncio
async def test_preflight_failure_records_its_elapsed_time():
    async def observer(run_id):
        await asyncio.sleep(0.005)
        raise OSError("unavailable")

    report = await execute(lambda request: pytest.fail("no requests"), observer)
    assert report["timing"]["preflight_seconds"] >= 0.005


@pytest.mark.parametrize("server_version", ["17.11", "16.2", "9.6.24", "18beta1"])
@pytest.mark.asyncio
async def test_postgres_observer_uses_only_parameterized_selects_in_readonly_transactions(
    monkeypatch, server_version,
):
    import asyncpg

    transactions = []
    queries = []
    connection_arguments = []

    class Transaction:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

    class Connection:
        def transaction(self, **kwargs):
            transactions.append(kwargs)
            return Transaction()

        async def fetch(self, sql, *args):
            queries.append((sql, args))
            return [{"event_id": "bench_test-run_0"}]

        def get_server_version(self):
            return asyncpg.serverversion.split_server_version_string(server_version)

        def get_settings(self):
            return SimpleNamespace(server_version=server_version)

        async def close(self, **kwargs):
            pass

    async def connect(*args, **kwargs):
        connection_arguments.append((args, kwargs))
        return Connection()

    monkeypatch.setattr(asyncpg, "connect", connect)
    observer = benchmark.PostgresObserver("postgresql+asyncpg://u:p@db/events", timeout=0.1)
    assert await observer("test-run") == {"bench_test-run_0"}
    assert await observer("test-run") == {"bench_test-run_0"}
    await observer.close()
    assert len(connection_arguments) == 1
    assert connection_arguments[0][0] == ("postgresql://u:p@db/events",)
    assert connection_arguments[0][1]["server_settings"]["default_transaction_read_only"] == "on"
    assert transactions == [{"readonly": True}, {"readonly": True}]
    assert all(sql == "SELECT event_id FROM events WHERE event_id LIKE $1" for sql, _ in queries)
    assert queries[0][1] == (r"bench\_test-run\_%",)
    assert observer.server_version == server_version


@pytest.mark.asyncio
async def test_network_ambiguity_can_complete_in_db_but_is_not_a_successful_run():
    observer = Observer()

    async def handler(request):
        observer.ids.update(event["event_id"] for event in json.loads(request.content)["events"])
        raise httpx.ReadTimeout("response lost", request=request)

    report = await execute(handler, observer, events=1)
    assert report["events"]["accepted"] == 0
    assert report["events"]["db_completed"] == 1
    assert report["events"]["network_failure"] == 1
    assert report["completion"]["all_requested_completed"] is True
    assert report["exit_code"] == 1


@pytest.mark.asyncio
async def test_request_timeout_is_caught_and_attempt_latency_is_recorded():
    async def handler(request):
        await asyncio.sleep(1)

    report = await execute(handler, Observer(), events=1, request_timeout=0.005)
    assert report["events"]["network_failure"] == 1
    assert report["request_latency_ms"]["count"] == 1
    assert report["request_latency_ms"]["p50"] >= 5


@pytest.mark.asyncio
async def test_metadata_excludes_credentials_and_preserves_supplied_worker_count():
    report = await execute(
        lambda request: httpx.Response(503),
        Observer(),
        events=1,
        url="https://user:TOPSECRET@example.org",
        database_url="postgresql://u:DBSECRET@db/events",
        api_key="KEYSECRET",
        worker_count=4,
    )
    serialized = json.dumps(report)
    assert not any(secret in serialized for secret in ("TOPSECRET", "DBSECRET", "KEYSECRET"))
    assert "database_url" not in report["config"]
    assert report["config"]["url"] == "https://example.org"
    assert report["config"]["worker_count"] == 4


@pytest.mark.asyncio
async def test_main_writes_identical_sorted_json_to_stdout_and_output_for_setup_failure(
    monkeypatch,
    tmp_path,
    capsys,
):
    class UnavailableObserver:
        def __init__(self, *args):
            pass

        async def __call__(self, run_id):
            raise OSError("database is unavailable")

        async def close(self):
            pass

    monkeypatch.setattr(benchmark, "PostgresObserver", UnavailableObserver)
    for name in (
        "ALL_PROXY",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "all_proxy",
        "http_proxy",
        "https_proxy",
    ):
        monkeypatch.delenv(name, raising=False)
    output = tmp_path / "result.json"
    exit_code = await benchmark.main(["--events", "1", "--output", str(output)])
    stdout = capsys.readouterr().out
    assert stdout == output.read_text()
    report = json.loads(stdout)
    assert stdout == json.dumps(report, sort_keys=True, indent=2, allow_nan=False) + "\n"
    assert exit_code == report["exit_code"] == 1
    assert report["completion"]["reason"] == "observer_error"


@pytest.mark.asyncio
async def test_main_ignores_ambient_proxy_and_creates_report_parent_directory(
    monkeypatch, tmp_path, capsys,
):
    class UnavailableObserver:
        def __init__(self, *args):
            pass

        async def __call__(self, run_id):
            raise OSError("database is unavailable")

        async def close(self):
            pass

    monkeypatch.setattr(benchmark, "PostgresObserver", UnavailableObserver)
    # The optional SOCKS dependency is intentionally absent. A direct benchmark
    # must not initialize a proxy transport from unrelated shell settings.
    monkeypatch.setenv("ALL_PROXY", "socks5h://127.0.0.1:1")
    output = tmp_path / "new-directory" / "report.json"
    assert await benchmark.main(["--events", "1", "--output", str(output)]) == 1
    assert json.loads(capsys.readouterr().out) == json.loads(output.read_text())


@pytest.mark.asyncio
async def test_a_poll_reaching_drain_deadline_is_explicitly_a_timeout():
    calls = 0

    async def observer(run_id):
        nonlocal calls
        calls += 1
        if calls <= 2:
            return set()
        await asyncio.sleep(1)
        return set()

    report = await execute(
        lambda request: httpx.Response(202, json={"received": 1, "accepted": 1, "duplicates": 0}),
        observer, events=1, drain_timeout=0.015,
    )
    assert report["completion"]["timed_out"] is True
    assert report["completion"]["reason"] == "drain_timeout"
