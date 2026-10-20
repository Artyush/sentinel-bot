"""Authenticated, bounded HTTP acceptance for configured push sources."""

import asyncio
import hmac
import logging
import math
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime

from aiohttp import HttpVersion11, web
from aiohttp.http_exceptions import HttpProcessingError

from sentinel_bot.config import IngressSettings, PushSource, Settings
from sentinel_bot.events import EventRejected, parse_event
from sentinel_bot.monitoring import MonitoringProgress
from sentinel_bot.storage import SQLiteStore, StorageError
from sentinel_bot.workers import await_completion, run_blocking

PATH = "/api/v1/events"
_PARSER_LOG = logging.getLogger("sentinel_bot.ingress.parser")
_PARSER_LOG.addHandler(logging.NullHandler())
_PARSER_LOG.propagate = False
_PARSER_LOG.setLevel(logging.CRITICAL + 1)
_REJECTIONS = {
    "schema": 400,
    "event_time": 422,
    "lifecycle": 422,
    "conflict": 409,
    "capacity": 503,
    "event_type": 403,
}


@dataclass
class _Bucket:
    rate: float
    burst: int
    tokens: float
    updated_at: float

    def available_in(self, now: float) -> float:
        self.tokens = min(self.burst, self.tokens + max(0.0, now - self.updated_at) * self.rate)
        self.updated_at = now
        return max(0.0, (1 - self.tokens) / self.rate)


@dataclass
class _State:
    limits: IngressSettings
    sources: tuple[PushSource, ...]
    store: SQLiteStore
    lock: asyncio.Lock
    failed: asyncio.Future[Exception]
    global_bucket: _Bucket
    source_buckets: dict[str, _Bucket] = field(default_factory=dict)
    active: int = 0


_STATE = web.AppKey("ingress", _State)


def _error(status: int, code: str, *, retry_after: int | None = None) -> web.Response:
    headers = {"Cache-Control": "no-store"}
    if retry_after is not None:
        headers["Retry-After"] = str(max(1, min(60, retry_after)))
    if status == 401:
        headers["WWW-Authenticate"] = 'Bearer realm="sentinel"'
    if status == 405:
        headers["Allow"] = "POST"
    response = web.json_response({"error": {"code": code}}, status=status, headers=headers)
    response.force_close()
    return response


def _authenticate(request: web.Request, sources: tuple[PushSource, ...]) -> PushSource | None:
    values = request.headers.getall("Authorization", ())
    if len(values) != 1:
        return None
    scheme, separator, token = values[0].partition(" ")
    if not separator or scheme.lower() != "bearer" or not token or len(token) > 256:
        return None
    candidate = token.encode("utf-8", "surrogateescape")
    matched = None
    for source in sources:
        if hmac.compare_digest(candidate, source.token.encode("utf-8")):
            matched = source
    return matched


def _rate_delay(state: _State, source: PushSource) -> int | None:
    now = time.monotonic()
    local = state.source_buckets[source.alias]
    delay = max(state.global_bucket.available_in(now), local.available_in(now))
    if delay > 0:
        return max(1, min(60, math.ceil(delay)))
    state.global_bucket.tokens -= 1
    local.tokens -= 1
    return None


async def _accept(request: web.Request, state: _State, source: PushSource) -> web.Response:
    limits = state.limits
    if request.query_string:
        return _error(400, "schema")
    expectations = request.headers.getall("Expect", ())
    if expectations and (
        len(expectations) != 1
        or expectations[0].lower() != "100-continue"
        or request.version != HttpVersion11
    ):
        return _error(400, "schema")
    content_types = request.headers.getall("Content-Type", ())
    encodings = request.headers.getall("Content-Encoding", ())
    if (
        len(content_types) != 1
        or request.content_type != "application/json"
        or request.charset not in {None, "utf-8", "UTF-8", "utf8"}
        or len(encodings) > 1
        or (encodings and encodings[0].lower() != "identity")
    ):
        return _error(415, "content_type")
    if request.content_length is not None and request.content_length > limits.max_body_bytes:
        return _error(413, "body_too_large")
    body = bytearray()
    try:
        async with asyncio.timeout(limits.body_timeout):
            if expectations:
                await request.writer.write(b"HTTP/1.1 100 Continue\r\n\r\n")
                request.writer.output_size = 0
            while chunk := await request.content.read(limits.max_body_bytes + 1 - len(body)):
                body.extend(chunk)
                if len(body) > limits.max_body_bytes:
                    return _error(413, "body_too_large")
    except TimeoutError:
        return _error(408, "body_timeout")
    except ConnectionError, HttpProcessingError:
        return _error(400, "schema")
    try:
        event = parse_event(
            bytes(body),
            max_body_bytes=limits.max_body_bytes,
            max_summary_chars=limits.max_summary_chars,
        )
        async with state.lock:
            receipt = await run_blocking(
                lambda: state.store.accept_event(source, event, datetime.now(UTC), limits)
            )
    except EventRejected as exc:
        status = _REJECTIONS[exc.code]
        return _error(status, exc.code, retry_after=1 if status == 503 else None)
    except StorageError:
        return _error(503, "unavailable", retry_after=1)
    return web.json_response(receipt.as_dict(), status=202, headers={"Cache-Control": "no-store"})


async def _handle(request: web.Request) -> web.Response:
    state = request.app[_STATE]
    admitted = False
    try:
        if request.path != PATH:
            return _error(404, "not_found")
        if request.method != "POST":
            return _error(405, "method_not_allowed")
        source = _authenticate(request, state.sources)
        if source is None:
            return _error(401, "unauthorized")
        if state.active >= state.limits.max_concurrency:
            return _error(503, "capacity", retry_after=1)
        delay = _rate_delay(state, source)
        if delay is not None:
            return _error(429, "rate_limited", retry_after=delay)
        state.active += 1
        admitted = True
        return await _accept(request, state, source)
    except Exception as exc:
        if not state.failed.done():
            state.failed.set_result(exc)
        return _error(500, "internal")
    finally:
        if admitted:
            state.active -= 1


def create_app(settings: Settings, store: SQLiteStore, state_lock: asyncio.Lock) -> web.Application:
    """Create the private HTTP listener; TLS and connection limits belong to its proxy."""
    limits = settings.ingress
    if limits is None:
        raise ValueError("Incoming events are disabled")
    now = time.monotonic()
    state = _State(
        limits,
        settings.sources,
        store,
        state_lock,
        asyncio.get_running_loop().create_future(),
        _Bucket(limits.global_rate_per_minute / 60, limits.global_burst, limits.global_burst, now),
        {
            source.alias: _Bucket(
                limits.source_rate_per_minute / 60, limits.source_burst, limits.source_burst, now
            )
            for source in settings.sources
        },
    )
    app = web.Application(
        client_max_size=limits.max_body_bytes,
        handler_args={
            "auto_decompress": False,
            "handler_cancellation": True,
            "access_log": None,
            "logger": _PARSER_LOG,
            "max_line_size": 4096,
            "max_field_size": 4096,
            "max_headers": 32,
            "read_bufsize": limits.max_body_bytes,
            "lingering_time": 0,
            "keepalive_timeout": 5,
        },
    )
    app[_STATE] = state
    app.router.add_route("*", "/{path:.*}", _handle, expect_handler=_handle)
    return app


async def _cleanup(runner: web.AppRunner, stop: asyncio.Task[bool]) -> None:
    stop.cancel()
    await asyncio.gather(stop, return_exceptions=True)
    await runner.cleanup()


async def run_ingress(
    settings: Settings,
    stopped: asyncio.Event,
    progress: MonitoringProgress,
    state_lock: asyncio.Lock,
) -> None:
    """Supervise listener failures and keep idle-listener progress current."""
    limits = settings.ingress
    if limits is None:
        raise ValueError("Incoming events are disabled")
    app = create_app(settings, SQLiteStore(settings.state_dir), state_lock)
    runner = web.AppRunner(app, shutdown_timeout=settings.shutdown_timeout)
    stop: asyncio.Task[bool] = asyncio.create_task(stopped.wait())
    try:
        await runner.setup()
        await web.TCPSite(runner, limits.host, limits.port).start()
        waiters: set[asyncio.Future[Exception] | asyncio.Task[bool]] = {stop, app[_STATE].failed}
        while not stopped.is_set():
            progress.ingress = time.monotonic()
            done, _ = await asyncio.wait(
                waiters,
                timeout=min(1, settings.health_timeout / 3),
                return_when=asyncio.FIRST_COMPLETED,
            )
            if app[_STATE].failed in done:
                raise app[_STATE].failed.result()
    finally:
        await await_completion(asyncio.create_task(_cleanup(runner, stop)))
