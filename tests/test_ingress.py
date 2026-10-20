import asyncio
import gzip
import threading
import time
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest
from aiohttp import ClientSession, web

import sentinel_bot.ingress as ingress
from sentinel_bot.config import IngressSettings, PushSource, Settings
from sentinel_bot.monitoring import MonitoringProgress
from sentinel_bot.storage import SQLiteStore, StorageError, initialize_state

TOKEN = "a" * 40
OTHER_TOKEN = "b" * 40
AUTH = {"Authorization": f"Bearer {TOKEN}"}
SOURCES = (
    PushSource("edge", frozenset({"attack", "deploy"}), TOKEN),
    PushSource("service", frozenset({"deploy"}), OTHER_TOKEN),
)


def event(**changes: object) -> dict[str, object]:
    return {
        "event_id": str(uuid4()),
        "occurred_at": datetime.now(UTC).isoformat(),
        "kind": "notification",
        "type": changes.pop("event_type", "attack"),
        "severity": "warning",
        "summary": "Synthetic event",
        **changes,
    }


def prepare(tmp_path: Path, **limits: object) -> tuple[Settings, SQLiteStore]:
    settings = Settings(tmp_path, ingress=replace(IngressSettings(), **limits), sources=SOURCES)
    initialize_state(tmp_path, datetime.now(UTC))
    store = SQLiteStore(tmp_path)
    store.configure_telegram(None, datetime.now(UTC))
    store.reconcile_push_sources(settings.sources)
    return settings, store


@asynccontextmanager
async def server(
    settings: Settings, store: SQLiteStore, lock: asyncio.Lock | None = None
) -> AsyncIterator[tuple[str, web.Application]]:
    app = ingress.create_app(settings, store, lock or asyncio.Lock())
    runner = web.AppRunner(app, shutdown_timeout=0.2)
    await runner.setup()
    await web.TCPSite(runner, "127.0.0.1", 0).start()
    try:
        yield f"http://127.0.0.1:{runner.addresses[0][1]}", app
    finally:
        await runner.cleanup()


async def eventually(predicate: Callable[[], bool]) -> None:
    async with asyncio.timeout(3):
        while not predicate():
            await asyncio.sleep(0.005)


def test_acceptance_is_durable_and_duplicate_receipt_survives_restart(tmp_path: Path) -> None:
    settings, store = prepare(tmp_path)
    payload = event(summary="Синтетическое событие")

    async def scenario() -> None:
        async with ClientSession() as client:
            async with server(settings, store) as (url, _):
                async with client.post(url + ingress.PATH, headers=AUTH, json=payload) as response:
                    assert response.status == 202
                    receipt = await response.json()
                    assert receipt["event_id"] == payload["event_id"]
                    assert receipt["outcome"] == "notification"
                    assert set(receipt) == {"event_id", "received_at", "outcome"}
                    assert store.pending_count() == 1
                    assert response.headers["Cache-Control"] == "no-store"
            async with server(settings, SQLiteStore(tmp_path)) as (url, _):
                reversed_payload = dict(reversed(tuple(payload.items())))
                async with client.post(
                    url + ingress.PATH, headers=AUTH, json=reversed_payload
                ) as response:
                    assert response.status == 202
                    assert await response.json() == receipt
        assert store.pending_count() == 1

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("headers", "body"),
    [
        ({"Content-Type": "application/json"}, b"invalid"),
        ({"Content-Type": "text/plain"}, b"x" * 20000),
        ({"Content-Encoding": "gzip"}, gzip.compress(b"x" * 100000)),
        ({"Authorization": "Bearer wrong"}, b""),
        ({"Authorization": f"Basic {TOKEN}"}, b""),
        ({"Authorization": "Bearer " + "x" * 300}, b""),
        ({"Authorization": "Bearer é"}, b""),
    ],
)
def test_authentication_precedes_body_errors(
    tmp_path: Path, headers: dict[str, str], body: bytes
) -> None:
    settings, store = prepare(tmp_path)

    async def scenario() -> None:
        async with server(settings, store) as (url, app), ClientSession() as client:
            async with client.post(url + ingress.PATH, headers=headers, data=body) as response:
                assert response.status == 401
                assert await response.json() == {"error": {"code": "unauthorized"}}
                assert "Bearer" in response.headers["WWW-Authenticate"]
                assert response.headers["Connection"] == "close"
            assert set(app[ingress._STATE].source_buckets) == {"edge", "service"}
        assert store.pending_count() == 0

    asyncio.run(scenario())


def test_duplicate_authorization_headers_do_not_select_a_source(tmp_path: Path) -> None:
    settings, store = prepare(tmp_path)

    async def scenario() -> None:
        async with server(settings, store) as (url, _), ClientSession() as client:
            async with client.post(
                url + ingress.PATH,
                headers=[
                    ("Authorization", f"Bearer {TOKEN}"),
                    ("Authorization", f"Bearer {TOKEN}"),
                ],
                json=event(),
            ) as response:
                assert response.status == 401
        assert store.pending_count() == 0

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("method", "path", "status", "code"),
    [
        ("GET", ingress.PATH, 405, "method_not_allowed"),
        ("HEAD", ingress.PATH, 405, "method_not_allowed"),
        ("POST", "/api/v1/events/", 404, "not_found"),
        ("POST", "/health", 404, "not_found"),
        ("POST", "/telegram/webhook", 404, "not_found"),
    ],
)
def test_unknown_routes_and_methods_have_safe_errors(
    tmp_path: Path, method: str, path: str, status: int, code: str
) -> None:
    settings, store = prepare(tmp_path)

    async def scenario() -> None:
        async with server(settings, store) as (url, _), ClientSession() as client:
            async with client.request(method, url + path) as response:
                assert response.status == status
                if method != "HEAD":
                    assert await response.json() == {"error": {"code": code}}
                if status == 405:
                    assert response.headers["Allow"] == "POST"

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("headers", "body", "status", "code"),
    [
        ({"Content-Type": "text/plain"}, b"{}", 415, "content_type"),
        ({"Content-Type": "application/json; charset=latin-1"}, b"{}", 415, "content_type"),
        (
            {"Content-Type": "application/json", "Content-Encoding": "gzip"},
            b"x",
            415,
            "content_type",
        ),
        ({"Content-Type": "application/json"}, b"x" * 16385, 413, "body_too_large"),
        ({"Content-Type": "application/json"}, b"{}", 400, "schema"),
        ({"Content-Type": "application/json"}, b'{"event_id":1,"event_id":2}', 400, "schema"),
        ({"Content-Type": "application/json"}, b'{"value":NaN}', 400, "schema"),
        ({"Content-Type": "application/json"}, b"\xff", 400, "schema"),
    ],
)
def test_type_size_and_strict_json_errors(
    tmp_path: Path, headers: dict[str, str], body: bytes, status: int, code: str
) -> None:
    settings, store = prepare(tmp_path)

    async def scenario() -> None:
        async with server(settings, store) as (url, _), ClientSession() as client:
            async with client.post(
                url + ingress.PATH, headers=AUTH | headers, data=body
            ) as response:
                assert response.status == status
                assert await response.json() == {"error": {"code": code}}
        assert store.pending_count() == 0

    asyncio.run(scenario())


@pytest.mark.parametrize("field", ["source", "chat_id", "recipients", "url", "targets"])
def test_payload_cannot_choose_identity_targets_or_recipients(tmp_path: Path, field: str) -> None:
    settings, store = prepare(tmp_path)

    async def scenario() -> None:
        async with server(settings, store) as (url, _), ClientSession() as client:
            async with client.post(
                url + ingress.PATH, headers=AUTH, json=event(**{field: "injected"})
            ) as response:
                assert response.status == 400
                assert await response.json() == {"error": {"code": "schema"}}
        assert store.pending_count() == 0

    asyncio.run(scenario())


def test_query_parameters_cannot_select_a_target(tmp_path: Path) -> None:
    settings, store = prepare(tmp_path)

    async def scenario() -> None:
        async with server(settings, store) as (url, _), ClientSession() as client:
            async with client.post(
                url + ingress.PATH + "?url=https://example.test", headers=AUTH, json=event()
            ) as response:
                assert response.status == 400
                assert await response.json() == {"error": {"code": "schema"}}
        assert store.pending_count() == 0

    asyncio.run(scenario())


def test_chunked_request_body_has_the_same_byte_limit(tmp_path: Path) -> None:
    settings, store = prepare(tmp_path)

    async def chunks() -> AsyncIterator[bytes]:
        for _ in range(5):
            yield b"x" * 4096
            await asyncio.sleep(0)

    async def scenario() -> None:
        async with server(settings, store) as (url, _), ClientSession() as client:
            async with client.post(
                url + ingress.PATH,
                headers=AUTH | {"Content-Type": "application/json"},
                data=chunks(),
            ) as response:
                assert response.status == 413
                assert await response.json() == {"error": {"code": "body_too_large"}}
        assert store.pending_count() == 0

    asyncio.run(scenario())


def test_body_deadline_closes_slow_request_and_releases_capacity(tmp_path: Path) -> None:
    settings, store = prepare(tmp_path, body_timeout=0.03, max_concurrency=1)

    async def chunks() -> AsyncIterator[bytes]:
        yield b"{"
        await asyncio.sleep(1)
        yield b"}"

    async def scenario() -> None:
        async with server(settings, store) as (url, _), ClientSession() as client:
            async with client.post(
                url + ingress.PATH,
                headers=AUTH | {"Content-Type": "application/json"},
                data=chunks(),
            ) as response:
                assert response.status == 408
                assert await response.json() == {"error": {"code": "body_timeout"}}
            async with client.post(url + ingress.PATH, headers=AUTH, json=event()) as response:
                assert response.status == 202
        assert store.pending_count() == 1

    asyncio.run(scenario())


def test_malformed_chunk_framing_remains_bounded_without_failing_the_worker(
    tmp_path: Path,
) -> None:
    settings, store = prepare(tmp_path, body_timeout=0.03)

    async def scenario() -> None:
        async with server(settings, store) as (url, app):
            port = int(url.rsplit(":", 1)[1])
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            try:
                writer.write(
                    (
                        f"POST {ingress.PATH} HTTP/1.1\r\nHost: localhost\r\n"
                        f"Authorization: Bearer {TOKEN}\r\nContent-Type: application/json\r\n"
                        "Transfer-Encoding: chunked\r\n\r\n"
                    ).encode()
                )
                await writer.drain()
                await eventually(lambda: app[ingress._STATE].active == 1)
                writer.write(b"private-invalid-chunk\r\n")
                await writer.drain()
                response = await asyncio.wait_for(reader.read(), 1)
                assert response.startswith(b"HTTP/1.1 408")
                assert b'{"error": {"code": "body_timeout"}}' in response
                assert b"private-invalid-chunk" not in response
                assert not app[ingress._STATE].failed.done()
            finally:
                writer.close()
                await writer.wait_closed()
        assert store.pending_count() == 0

    asyncio.run(scenario())


def test_unauthorized_request_does_not_wait_for_its_declared_body(tmp_path: Path) -> None:
    settings, store = prepare(tmp_path)

    async def scenario() -> None:
        async with server(settings, store) as (url, _):
            reader, writer = await asyncio.open_connection("127.0.0.1", int(url.rsplit(":", 1)[1]))
            try:
                writer.write(
                    f"POST {ingress.PATH} HTTP/1.1\r\nHost: localhost\r\n"
                    "Content-Length: 20000\r\nContent-Type: text/plain\r\n\r\n".encode()
                )
                await writer.drain()
                response = await asyncio.wait_for(reader.read(), 0.5)
                assert response.startswith(b"HTTP/1.1 401")
                assert b"unauthorized" in response
            finally:
                writer.close()
                await writer.wait_closed()

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("method", "path", "authorization", "expectation", "status"),
    [
        ("POST", ingress.PATH, "Bearer wrong", "100-continue", 401),
        ("POST", ingress.PATH, "Bearer wrong", "unsupported", 401),
        ("POST", "/unknown", f"Bearer {TOKEN}", "100-continue", 404),
        ("GET", ingress.PATH, f"Bearer {TOKEN}", "100-continue", 405),
        ("POST", ingress.PATH, f"Bearer {TOKEN}", "unsupported", 400),
    ],
)
def test_expect_errors_preserve_authentication_and_route_priority(
    tmp_path: Path, method: str, path: str, authorization: str, expectation: str, status: int
) -> None:
    settings, store = prepare(tmp_path)

    async def scenario() -> None:
        async with server(settings, store) as (url, _):
            reader, writer = await asyncio.open_connection("127.0.0.1", int(url.rsplit(":", 1)[1]))
            try:
                writer.write(
                    (
                        f"{method} {path} HTTP/1.1\r\nHost: localhost\r\n"
                        f"Authorization: {authorization}\r\nExpect: {expectation}\r\n"
                        "Content-Length: 100\r\nContent-Type: application/json\r\n\r\n"
                    ).encode()
                )
                await writer.drain()
                response = await asyncio.wait_for(reader.read(), 0.5)
                assert response.startswith(f"HTTP/1.1 {status}".encode())
                assert b"100 Continue" not in response
                assert b'{"error": {"code":' in response
            finally:
                writer.close()
                await writer.wait_closed()
        assert store.pending_count() == 0

    asyncio.run(scenario())


def test_authorized_continue_consumes_one_admission_and_respects_rate_limit(tmp_path: Path) -> None:
    settings, store = prepare(tmp_path, source_burst=1)

    async def scenario() -> None:
        async with server(settings, store) as (url, _), ClientSession() as client:
            async with client.post(
                url + ingress.PATH, headers=AUTH, json=event(), expect100=True
            ) as response:
                assert response.status == 202
                assert (await response.json())["outcome"] == "notification"
            async with asyncio.timeout(0.5):
                async with client.post(
                    url + ingress.PATH, headers=AUTH, json=event(), expect100=True
                ) as response:
                    assert response.status == 429
                    assert await response.json() == {"error": {"code": "rate_limited"}}
        assert store.pending_count() == 1

    asyncio.run(scenario())


def test_source_rate_limit_is_independent_and_unauthorized_requests_spend_no_budget(
    tmp_path: Path,
) -> None:
    settings, store = prepare(tmp_path, source_burst=1)

    async def scenario() -> None:
        async with server(settings, store) as (url, _), ClientSession() as client:
            async with client.post(url + ingress.PATH, json=event()) as response:
                assert response.status == 401
            async with client.post(url + ingress.PATH, headers=AUTH, json=event()) as response:
                assert response.status == 202
            async with client.post(url + ingress.PATH, headers=AUTH, json=event()) as response:
                assert response.status == 429
                assert 1 <= int(response.headers["Retry-After"]) <= 60
                assert await response.json() == {"error": {"code": "rate_limited"}}
            async with client.post(
                url + ingress.PATH,
                headers={"Authorization": f"Bearer {OTHER_TOKEN}"},
                json=event(event_type="deploy"),
            ) as response:
                assert response.status == 202
        assert store.pending_count() == 2

    asyncio.run(scenario())


def test_global_rate_limit_covers_all_configured_sources(tmp_path: Path) -> None:
    settings, store = prepare(tmp_path, global_burst=1)

    async def scenario() -> None:
        async with server(settings, store) as (url, _), ClientSession() as client:
            async with client.post(url + ingress.PATH, headers=AUTH, json=event()) as response:
                assert response.status == 202
            async with client.post(
                url + ingress.PATH,
                headers={"Authorization": f"Bearer {OTHER_TOKEN}"},
                json=event(event_type="deploy"),
            ) as response:
                assert response.status == 429

    asyncio.run(scenario())


def test_saturation_rejects_without_waiting_for_a_request_or_state_lock(tmp_path: Path) -> None:
    settings, store = prepare(tmp_path, max_concurrency=1, body_timeout=2)

    async def scenario() -> None:
        lock = asyncio.Lock()
        async with server(settings, store, lock) as (url, app), ClientSession() as client:
            await lock.acquire()
            first = asyncio.create_task(client.post(url + ingress.PATH, headers=AUTH, json=event()))
            try:
                await eventually(lambda: app[ingress._STATE].active == 1)
                async with asyncio.timeout(0.5):
                    async with client.post(
                        url + ingress.PATH, headers=AUTH, json=event()
                    ) as response:
                        assert response.status == 503
                        assert response.headers["Retry-After"] == "1"
                        assert await response.json() == {"error": {"code": "capacity"}}
                assert not first.done()
            finally:
                lock.release()
                response = await first
                assert response.status == 202
                await response.read()
                response.release()
        assert store.pending_count() == 1

    asyncio.run(scenario())


def test_concurrent_duplicate_requests_have_one_durable_effect(tmp_path: Path) -> None:
    settings, store = prepare(tmp_path)
    payload = event()

    async def scenario() -> None:
        async with server(settings, store) as (url, _), ClientSession() as client:

            async def send() -> dict[str, object]:
                async with client.post(url + ingress.PATH, headers=AUTH, json=payload) as response:
                    assert response.status == 202
                    return await response.json()

            receipts = await asyncio.gather(*(send() for _ in range(5)))
            assert all(item == receipts[0] for item in receipts)
        assert store.pending_count() == 1

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("changes", "status", "code"),
    [
        ({"event_type": "unconfigured"}, 403, "event_type"),
        ({"occurred_at": (datetime.now(UTC) - timedelta(days=2)).isoformat()}, 422, "event_time"),
        ({"occurred_at": (datetime.now(UTC) + timedelta(days=2)).isoformat()}, 422, "event_time"),
    ],
)
def test_domain_rejections_are_safe_http_errors(
    tmp_path: Path, changes: dict[str, object], status: int, code: str
) -> None:
    settings, store = prepare(tmp_path)

    async def scenario() -> None:
        async with server(settings, store) as (url, _), ClientSession() as client:
            async with client.post(
                url + ingress.PATH, headers=AUTH, json=event(**changes)
            ) as response:
                assert response.status == status
                assert await response.json() == {"error": {"code": code}}
        assert store.pending_count() == 0

    asyncio.run(scenario())


def test_conflict_and_capacity_preserve_prior_acceptance(tmp_path: Path) -> None:
    settings, store = prepare(tmp_path, max_pending=1, max_pending_per_source=1)
    payload = event()

    async def scenario() -> None:
        async with server(settings, store) as (url, _), ClientSession() as client:
            async with client.post(url + ingress.PATH, headers=AUTH, json=payload) as response:
                assert response.status == 202
                receipt = await response.json()
            async with client.post(
                url + ingress.PATH, headers=AUTH, json=payload | {"summary": "Changed content"}
            ) as response:
                assert response.status == 409
                assert await response.json() == {"error": {"code": "conflict"}}
            async with client.post(url + ingress.PATH, headers=AUTH, json=event()) as response:
                assert response.status == 503
                assert response.headers["Retry-After"] == "1"
            async with client.post(url + ingress.PATH, headers=AUTH, json=payload) as response:
                assert response.status == 202
                assert await response.json() == receipt
        assert store.pending_count() == 1

    asyncio.run(scenario())


def test_lifecycle_rejection_does_not_resolve_an_unrelated_type(tmp_path: Path) -> None:
    settings, store = prepare(tmp_path)
    opened = event(kind="firing", incident_key="attack-1")

    async def scenario() -> None:
        async with server(settings, store) as (url, _), ClientSession() as client:
            async with client.post(url + ingress.PATH, headers=AUTH, json=opened) as response:
                assert response.status == 202
                assert (await response.json())["outcome"] == "opened"
            async with client.post(
                url + ingress.PATH,
                headers=AUTH,
                json=event(kind="resolved", incident_key="attack-1", event_type="deploy"),
            ) as response:
                assert response.status == 422
                assert await response.json() == {"error": {"code": "lifecycle"}}
        assert store.pending_count() == 1

    asyncio.run(scenario())


def test_event_ids_and_allowed_types_are_scoped_to_authenticated_source(tmp_path: Path) -> None:
    settings, store = prepare(tmp_path)
    payload = event(event_type="deploy")

    async def scenario() -> None:
        async with server(settings, store) as (url, _), ClientSession() as client:
            for token in (TOKEN, OTHER_TOKEN):
                async with client.post(
                    url + ingress.PATH, headers={"Authorization": f"Bearer {token}"}, json=payload
                ) as response:
                    assert response.status == 202
            async with client.post(
                url + ingress.PATH,
                headers={"Authorization": f"Bearer {OTHER_TOKEN}"},
                json=event(),
            ) as response:
                assert response.status == 403
        assert store.pending_count() == 2

    asyncio.run(scenario())


def test_expected_storage_failure_is_503_and_does_not_hide_programming_errors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings, store = prepare(tmp_path)

    def fail(*args: object) -> None:
        raise StorageError("private storage detail")

    monkeypatch.setattr(store, "accept_event", fail)

    async def scenario() -> None:
        async with server(settings, store) as (url, app), ClientSession() as client:
            async with client.post(url + ingress.PATH, headers=AUTH, json=event()) as response:
                assert response.status == 503
                assert await response.json() == {"error": {"code": "unavailable"}}
                assert not app[ingress._STATE].failed.done()

    asyncio.run(scenario())


def test_unexpected_failure_has_safe_response_and_supervision_signal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings, store = prepare(tmp_path)
    failure = RuntimeError("private programming detail")

    def fail(*args: object) -> None:
        raise failure

    monkeypatch.setattr(store, "accept_event", fail)

    async def scenario() -> None:
        async with server(settings, store) as (url, app), ClientSession() as client:
            async with client.post(url + ingress.PATH, headers=AUTH, json=event()) as response:
                assert response.status == 500
                assert await response.json() == {"error": {"code": "internal"}}
            assert app[ingress._STATE].failed.result() is failure

    asyncio.run(scenario())


def test_client_disconnect_after_commit_keeps_duplicate_receipt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings, store = prepare(tmp_path)
    payload = event()
    committed, release = threading.Event(), threading.Event()
    accept = store.accept_event

    def delayed(*args: object) -> object:
        receipt = accept(*args)
        committed.set()
        assert release.wait(3)
        return receipt

    monkeypatch.setattr(store, "accept_event", delayed)

    async def scenario() -> None:
        async with server(settings, store) as (url, _), ClientSession() as client:
            request = asyncio.create_task(
                client.post(url + ingress.PATH, headers=AUTH, json=payload)
            )
            try:
                await eventually(committed.is_set)
                assert store.pending_count() == 1
                assert not request.done()
                request.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await request
            finally:
                release.set()
            monkeypatch.setattr(store, "accept_event", accept)
            async with client.post(url + ingress.PATH, headers=AUTH, json=payload) as response:
                assert response.status == 202
                assert (await response.json())["event_id"] == payload["event_id"]
        assert store.pending_count() == 1

    asyncio.run(scenario())


def test_runtime_progress_remains_fresh_while_listener_is_idle_and_cleans_up(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings, _ = prepare(tmp_path)
    settings = replace(settings, ingress=replace(settings.ingress, port=0), health_timeout=0.09)
    apps: list[web.Application] = []
    create_app = ingress.create_app

    def capture(*args: object) -> web.Application:
        app = create_app(*args)
        apps.append(app)
        return app

    monkeypatch.setattr(ingress, "create_app", capture)

    async def scenario() -> None:
        stopped, progress = asyncio.Event(), MonitoringProgress()
        task = asyncio.create_task(ingress.run_ingress(settings, stopped, progress, asyncio.Lock()))
        try:
            await eventually(lambda: progress.ingress is not None)
            first = progress.ingress
            await asyncio.sleep(0.1)
            assert progress.ingress is not None and first is not None
            assert progress.ingress > first
            assert time.monotonic() - progress.ingress < settings.health_timeout
            assert not task.done()
        finally:
            stopped.set()
            await asyncio.wait_for(task, 1)
        assert apps and apps[0]._loop is not None

    asyncio.run(scenario())


def test_runtime_propagates_handler_programming_failure_and_cleans_up(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings, _ = prepare(tmp_path)
    settings = replace(settings, ingress=replace(settings.ingress, port=0))
    listeners: list[web.TCPSite] = []
    start = web.TCPSite.start

    async def capture(listener: web.TCPSite) -> None:
        await start(listener)
        listeners.append(listener)

    def fail(*args: object) -> None:
        raise RuntimeError("synthetic handler failure")

    monkeypatch.setattr(web.TCPSite, "start", capture)
    monkeypatch.setattr(SQLiteStore, "accept_event", fail)

    async def scenario() -> None:
        stopped, progress = asyncio.Event(), MonitoringProgress()
        task = asyncio.create_task(ingress.run_ingress(settings, stopped, progress, asyncio.Lock()))
        try:
            await eventually(lambda: bool(listeners))
            listener = listeners[0]._server
            assert listener is not None
            port = listener.sockets[0].getsockname()[1]
            async with ClientSession() as client:
                async with client.post(
                    f"http://127.0.0.1:{port}" + ingress.PATH, headers=AUTH, json=event()
                ) as response:
                    assert response.status == 500
                    assert await response.json() == {"error": {"code": "internal"}}
            with pytest.raises(RuntimeError, match="synthetic handler failure"):
                await task
            assert not listener.is_serving()
        finally:
            stopped.set()
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    asyncio.run(scenario())


def test_repeated_cancellation_finishes_owned_listener_cleanup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings, _ = prepare(tmp_path)
    settings = replace(settings, ingress=replace(settings.ingress, port=0))
    cleanup = web.AppRunner.cleanup

    async def scenario() -> None:
        started, release, finished = asyncio.Event(), asyncio.Event(), asyncio.Event()

        async def delayed(runner: web.AppRunner) -> None:
            started.set()
            await release.wait()
            await cleanup(runner)
            finished.set()

        monkeypatch.setattr(web.AppRunner, "cleanup", delayed)
        progress = MonitoringProgress()
        task = asyncio.create_task(
            ingress.run_ingress(settings, asyncio.Event(), progress, asyncio.Lock())
        )
        try:
            await eventually(lambda: progress.ingress is not None)
            task.cancel()
            await asyncio.wait_for(started.wait(), 1)
            task.cancel()
            await asyncio.sleep(0)
            assert not task.done()
        finally:
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await task
        assert finished.is_set()

    asyncio.run(scenario())


def test_disabled_ingress_cannot_create_a_listener(tmp_path: Path) -> None:
    initialize_state(tmp_path, datetime.now(UTC))

    async def scenario() -> None:
        settings = Settings(tmp_path)
        with pytest.raises(ValueError, match="disabled"):
            ingress.create_app(settings, SQLiteStore(tmp_path), asyncio.Lock())
        with pytest.raises(ValueError, match="disabled"):
            await ingress.run_ingress(
                settings, asyncio.Event(), MonitoringProgress(), asyncio.Lock()
            )

    asyncio.run(scenario())
