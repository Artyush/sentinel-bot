import asyncio
import gzip
import json
import socket
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import replace

import pytest
from aiohttp import web
from aiohttp.abc import ResolveResult

from sentinel_bot.config import SiteConfig
from sentinel_bot.http import AddressPolicyError, CheckDeferred, HttpChecker, PublicAddressResolver
from sentinel_bot.models import ErrorKind


@asynccontextmanager
async def http_fixture(
    handler: Callable[[web.Request], Awaitable[web.StreamResponse]],
) -> AsyncIterator[str]:
    app = web.Application()
    app.router.add_route("*", "/{path:.*}", handler)
    runner = web.AppRunner(app, shutdown_timeout=0.1)
    await runner.setup()
    server = web.TCPSite(runner, "127.0.0.1", 0)
    await server.start()
    try:
        address = runner.addresses[0]
        yield f"http://127.0.0.1:{address[1]}"
    finally:
        await runner.cleanup()


def site(url: str, **changes: object) -> SiteConfig:
    return replace(SiteConfig(alias="sample", url=url), **changes)


@pytest.mark.parametrize("status", [200, 204, 299, 304, 399])
def test_accepted_status_without_body(status: int) -> None:
    async def scenario() -> None:
        async def respond(request: web.Request) -> web.Response:
            return web.Response(status=status)

        async with http_fixture(respond) as url:
            async with HttpChecker(fixture_hosts=frozenset({"127.0.0.1"})) as checker:
                result = await checker.check(site(url))
        assert result.success
        assert result.http_status == status
        assert result.error is None
        assert result.latency_ms is not None and result.latency_ms >= 0

    asyncio.run(scenario())


@pytest.mark.parametrize("status", [400, 404, 500, 503])
def test_rejected_status_is_observation(status: int) -> None:
    async def scenario() -> None:
        async def respond(request: web.Request) -> web.Response:
            return web.Response(status=status, text="private diagnostic body")

        async with http_fixture(respond) as url:
            async with HttpChecker(fixture_hosts=frozenset({"127.0.0.1"})) as checker:
                result = await checker.check(site(url, marker="expected"))
        assert not result.success
        assert result.error is ErrorKind.HTTP_STATUS
        assert result.http_status == status
        assert "private" not in str(result)

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("body", "marker", "expected"),
    [
        (b"ready", "ready", None),
        ("Привет, ready".encode(), "Привет", None),
        (b"missing", "ready", ErrorKind.MARKER),
        (b"ready\xff", "ready", ErrorKind.MARKER),
    ],
)
def test_utf8_marker(body: bytes, marker: str, expected: ErrorKind | None) -> None:
    async def scenario() -> None:
        async def respond(request: web.Request) -> web.Response:
            return web.Response(body=body)

        async with http_fixture(respond) as url:
            async with HttpChecker(fixture_hosts=frozenset({"127.0.0.1"})) as checker:
                result = await checker.check(site(url, marker=marker))
        assert result.error is expected
        assert result.success == (expected is None)

    asyncio.run(scenario())


def test_marker_spans_network_chunks() -> None:
    async def scenario() -> None:
        async def respond(request: web.Request) -> web.StreamResponse:
            response = web.StreamResponse()
            await response.prepare(request)
            await response.write(b"rea")
            await asyncio.sleep(0.01)
            await response.write(b"dy")
            await response.write_eof()
            return response

        async with http_fixture(respond) as url:
            async with HttpChecker(fixture_hosts=frozenset({"127.0.0.1"})) as checker:
                result = await checker.check(site(url, marker="ready"))
        assert result.success

    asyncio.run(scenario())


@pytest.mark.parametrize("chunked", [False, True])
def test_body_limit_for_declared_and_chunked_responses(chunked: bool) -> None:
    async def scenario() -> None:
        async def respond(request: web.Request) -> web.StreamResponse:
            if not chunked:
                return web.Response(body=b"a" * 33)
            response = web.StreamResponse()
            await response.prepare(request)
            await response.write(b"a" * 33)
            await response.write_eof()
            return response

        async with http_fixture(respond) as url:
            async with HttpChecker(fixture_hosts=frozenset({"127.0.0.1"})) as checker:
                result = await checker.check(site(url, marker="a", max_body_bytes=32))
        assert result.error is ErrorKind.BODY_LIMIT

    asyncio.run(scenario())


@pytest.mark.parametrize("oversized", [False, True])
def test_compressed_body_is_bounded_after_decompression(oversized: bool) -> None:
    async def scenario() -> None:
        async def respond(request: web.Request) -> web.Response:
            body = b"ready" if not oversized else b"ready" + b"a" * 10_000
            return web.Response(body=gzip.compress(body), headers={"Content-Encoding": "gzip"})

        async with http_fixture(respond) as url:
            async with HttpChecker(fixture_hosts=frozenset({"127.0.0.1"})) as checker:
                result = await checker.check(site(url, marker="ready", max_body_bytes=100))
        assert result.error is (ErrorKind.BODY_LIMIT if oversized else None)

    asyncio.run(scenario())


@pytest.mark.parametrize("encoding", ["gzip", "deflate", "br"])
def test_invalid_or_unsupported_compression_fails_marker(encoding: str) -> None:
    async def scenario() -> None:
        async def respond(request: web.Request) -> web.Response:
            return web.Response(body=b"ready", headers={"Content-Encoding": encoding})

        async with http_fixture(respond) as url:
            async with HttpChecker(fixture_hosts=frozenset({"127.0.0.1"})) as checker:
                result = await checker.check(site(url, marker="ready"))
        assert result.error is ErrorKind.MARKER

    asyncio.run(scenario())


@pytest.mark.parametrize("marker", [None, "ready"])
def test_header_only_checks_do_not_wait_for_body(marker: str | None) -> None:
    async def scenario() -> None:
        async def respond(request: web.Request) -> web.StreamResponse:
            response = web.StreamResponse()
            await response.prepare(request)
            await asyncio.sleep(0.2)
            return response

        async with http_fixture(respond) as url:
            async with HttpChecker(fixture_hosts=frozenset({"127.0.0.1"})) as checker:
                result = await checker.check(site(url, marker=marker, total_timeout=0.05))
        assert result.error is (ErrorKind.TIMEOUT if marker else None)

    asyncio.run(scenario())


def test_redirects_share_one_total_deadline() -> None:
    async def scenario() -> None:
        async def respond(request: web.Request) -> web.Response:
            await asyncio.sleep(0.04)
            if request.path == "/one":
                return web.Response(status=302, headers={"Location": "/two"})
            return web.Response(text="ready")

        async with http_fixture(respond) as url:
            async with HttpChecker(fixture_hosts=frozenset({"127.0.0.1"})) as checker:
                result = await checker.check(site(url + "/one", total_timeout=0.06))
        assert result.error is ErrorKind.TIMEOUT

    asyncio.run(scenario())


@pytest.mark.parametrize("redirects", [3, 4])
def test_redirect_count_bound(redirects: int) -> None:
    async def scenario() -> None:
        requested: list[str] = []

        async def respond(request: web.Request) -> web.Response:
            requested.append(request.path)
            current = int(request.path[1:])
            if current < redirects:
                return web.Response(status=302, headers={"Location": f"/{current + 1}"})
            return web.Response(text="ready")

        async with http_fixture(respond) as url:
            async with HttpChecker(fixture_hosts=frozenset({"127.0.0.1"})) as checker:
                result = await checker.check(site(url + "/0"))
        assert len(requested) == 4
        assert result.error is (ErrorKind.REDIRECT if redirects == 4 else None)

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "location",
    [
        "http://localhost:1/",
        "https://127.0.0.1/",
        "/?token=secret",
        "/#secret",
        "http://user:secret@127.0.0.1/",
    ],
)
def test_redirects_cannot_change_origin_or_add_sensitive_url_parts(location: str) -> None:
    async def scenario() -> None:
        requests = 0

        async def respond(request: web.Request) -> web.Response:
            nonlocal requests
            requests += 1
            return web.Response(status=302, headers={"Location": location})

        async with http_fixture(respond) as url:
            async with HttpChecker(fixture_hosts=frozenset({"127.0.0.1"})) as checker:
                result = await checker.check(site(url))
        assert result.error is ErrorKind.REDIRECT
        assert requests == 1

    asyncio.run(scenario())


def test_redirect_requires_location() -> None:
    async def scenario() -> None:
        async def respond(request: web.Request) -> web.Response:
            return web.Response(status=302)

        async with http_fixture(respond) as url:
            async with HttpChecker(fixture_hosts=frozenset({"127.0.0.1"})) as checker:
                result = await checker.check(site(url))
        assert result.error is ErrorKind.REDIRECT

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "address", ["127.0.0.1", "10.0.0.1", "169.254.169.254", "[::1]", "224.0.0.1"]
)
def test_live_literal_addresses_are_rejected_without_connections(address: str) -> None:
    async def scenario() -> None:
        async with HttpChecker() as checker:
            result = await checker.check(site(f"http://{address}/"))
        assert result.error is ErrorKind.ADDRESS_POLICY

    asyncio.run(scenario())


def test_dns_policy_rejects_mixed_public_and_private_answers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def answers(
        self: object, host: str, port: int, family: socket.AddressFamily
    ) -> list[ResolveResult]:
        return [
            ResolveResult(
                hostname=host, host=address, port=port, family=socket.AF_INET, proto=0, flags=0
            )
            for address in ["93.184.215.14", "10.0.0.1"]
        ]

    monkeypatch.setattr("sentinel_bot.http.AsyncResolver.resolve", answers)

    async def scenario() -> None:
        resolver = PublicAddressResolver()
        try:
            with pytest.raises(AddressPolicyError):
                await resolver.resolve("example.test", 80)
        finally:
            await resolver.close()
        async with HttpChecker() as checker:
            result = await checker.check(site("http://example.test/"))
        assert result.error is ErrorKind.ADDRESS_POLICY

    asyncio.run(scenario())


def test_dns_failure_is_sanitized(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fail(
        self: object, host: str, port: int, family: socket.AddressFamily
    ) -> list[ResolveResult]:
        raise socket.gaierror("private diagnostic")

    monkeypatch.setattr("sentinel_bot.http.AsyncResolver.resolve", fail)

    async def scenario() -> None:
        async with HttpChecker() as checker:
            result = await checker.check(site("http://example.test/"))
        assert result.error is ErrorKind.DNS
        assert "private" not in str(result)

    asyncio.run(scenario())


def test_connection_failure_uses_real_closed_loopback_port() -> None:
    async def scenario() -> None:
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        async with HttpChecker(fixture_hosts=frozenset({"127.0.0.1"})) as checker:
            result = await checker.check(site(f"http://127.0.0.1:{port}/"))
        assert result.error is ErrorKind.CONNECTION

    asyncio.run(scenario())


def test_concurrency_is_bounded_and_slow_site_does_not_block_other_site() -> None:
    async def scenario() -> None:
        active = 0
        maximum = 0

        async def respond(request: web.Request) -> web.Response:
            nonlocal active, maximum
            active += 1
            maximum = max(maximum, active)
            try:
                await asyncio.sleep(0.08 if request.path == "/slow" else 0.01)
                return web.Response(text="ready")
            finally:
                active -= 1

        async with http_fixture(respond) as url:
            async with HttpChecker(
                max_concurrency=2, fixture_hosts=frozenset({"127.0.0.1"})
            ) as checker:
                slow = asyncio.create_task(checker.check(site(url + "/slow")))
                fast = asyncio.create_task(checker.check(site(url + "/fast")))
                fast_result = await fast
                assert fast_result.success
                assert not slow.done()
                others = [asyncio.create_task(checker.check(site(url))) for _ in range(4)]
                assert all(result.success for result in await asyncio.gather(slow, *others))
        assert maximum == 2

    asyncio.run(scenario())


def test_cancellation_propagates_and_checker_closes() -> None:
    async def scenario() -> None:
        entered = asyncio.Event()

        async def respond(request: web.Request) -> web.Response:
            entered.set()
            await asyncio.sleep(1)
            return web.Response()

        async with http_fixture(respond) as url:
            async with HttpChecker(fixture_hosts=frozenset({"127.0.0.1"})) as checker:
                checking = asyncio.create_task(checker.check(site(url)))
                await entered.wait()
                checking.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await checking
            with pytest.raises(RuntimeError, match="not open"):
                await checker.check(site(url))

    asyncio.run(scenario())


def test_unexpected_programming_error_remains_visible(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fail(
        self: object, host: str, port: int, family: socket.AddressFamily
    ) -> list[ResolveResult]:
        raise RuntimeError("unexpected resolver defect")

    monkeypatch.setattr("sentinel_bot.http.AsyncResolver.resolve", fail)

    async def scenario() -> None:
        async with HttpChecker() as checker:
            with pytest.raises(RuntimeError, match="unexpected resolver defect"):
                await checker.check(site("http://example.test/"))

    asyncio.run(scenario())


def test_session_ignores_ambient_proxy_and_cookies(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HTTP_PROXY", "http://127.0.0.1:1")
    monkeypatch.setenv("http_proxy", "http://127.0.0.1:1")
    monkeypatch.setenv("ALL_PROXY", "http://127.0.0.1:1")

    async def scenario() -> None:
        cookies: list[str | None] = []

        async def respond(request: web.Request) -> web.Response:
            cookies.append(request.headers.get("Cookie"))
            assert "Authorization" not in request.headers
            response = web.Response(text="ready")
            response.set_cookie("session", "secret")
            return response

        async with http_fixture(respond) as url:
            async with HttpChecker(fixture_hosts=frozenset({"127.0.0.1"})) as checker:
                assert (await checker.check(site(url))).success
                assert (await checker.check(site(url))).success
        assert cookies == [None, None]

    asyncio.run(scenario())


def test_connection_performs_dns_validation_again_after_redirect_and_each_check(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lookups = 0

    async def answers(
        self: object, host: str, port: int, family: socket.AddressFamily
    ) -> list[ResolveResult]:
        nonlocal lookups
        lookups += 1
        return [
            ResolveResult(
                hostname=host, host="127.0.0.1", port=port, family=socket.AF_INET, proto=0, flags=0
            )
        ]

    monkeypatch.setattr("sentinel_bot.http.AsyncResolver.resolve", answers)

    async def scenario() -> None:
        async def respond(request: web.Request) -> web.Response:
            if request.path == "/start":
                return web.Response(status=302, headers={"Location": "/finish"})
            return web.Response(text="ready")

        async with http_fixture(respond) as url:
            target = url.replace("127.0.0.1", "fixture.test") + "/start"
            async with HttpChecker(fixture_hosts=frozenset({"fixture.test"})) as checker:
                assert (await checker.check(site(target))).success
                assert (await checker.check(site(target))).success
        assert lookups == 4

    asyncio.run(scenario())


def test_changed_dns_answer_is_not_reused(monkeypatch: pytest.MonkeyPatch) -> None:
    lookups = 0

    async def answers(
        self: object, host: str, port: int, family: socket.AddressFamily
    ) -> list[ResolveResult]:
        nonlocal lookups
        lookups += 1
        return [
            ResolveResult(
                hostname=host,
                host="93.184.215.14" if lookups == 1 else "127.0.0.1",
                port=port,
                family=socket.AF_INET,
                proto=0,
                flags=0,
            )
        ]

    monkeypatch.setattr("sentinel_bot.http.AsyncResolver.resolve", answers)

    async def scenario() -> None:
        resolver = PublicAddressResolver()
        try:
            assert (await resolver.resolve("example.test", 80))[0]["host"] == "93.184.215.14"
            with pytest.raises(AddressPolicyError):
                await resolver.resolve("example.test", 80)
        finally:
            await resolver.close()

    asyncio.run(scenario())


def test_dns_wait_is_inside_total_deadline(monkeypatch: pytest.MonkeyPatch) -> None:
    cancelled = False

    async def delayed(
        self: object, host: str, port: int, family: socket.AddressFamily
    ) -> list[ResolveResult]:
        nonlocal cancelled
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            cancelled = True
            raise
        raise AssertionError("DNS deadline must cancel the resolver")

    monkeypatch.setattr("sentinel_bot.http.AsyncResolver.resolve", delayed)

    async def scenario() -> None:
        async with HttpChecker() as checker:
            result = await checker.check(site("http://example.test/", total_timeout=0.02))
        assert result.error is ErrorKind.TIMEOUT
        assert cancelled

    asyncio.run(scenario())


def test_dns_cleanup_releases_resolver_on_cancellation(monkeypatch: pytest.MonkeyPatch) -> None:
    entered = asyncio.Event()
    closed = False

    async def delayed(
        self: object, host: str, port: int, family: socket.AddressFamily
    ) -> list[ResolveResult]:
        entered.set()
        await asyncio.sleep(60)
        raise AssertionError("DNS resolution should be cancelled")

    original_close = PublicAddressResolver.close

    async def close(resolver: PublicAddressResolver) -> None:
        nonlocal closed
        await original_close(resolver)
        closed = True

    monkeypatch.setattr("sentinel_bot.http.AsyncResolver.resolve", delayed)
    monkeypatch.setattr(PublicAddressResolver, "close", close)

    async def scenario() -> None:
        async with HttpChecker() as checker:
            checking = asyncio.create_task(checker.check(site("http://example.test/")))
            await entered.wait()
            checking.cancel()
            with pytest.raises(asyncio.CancelledError):
                await checking
        assert closed

    asyncio.run(scenario())


def test_tls_protocol_failure_is_a_sanitized_observation() -> None:
    async def scenario() -> None:
        async def plain_http(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
            await writer.drain()
            writer.close()
            await writer.wait_closed()

        server = await asyncio.start_server(plain_http, "127.0.0.1", 0)
        async with server:
            port = server.sockets[0].getsockname()[1]
            async with HttpChecker(fixture_hosts=frozenset({"127.0.0.1"})) as checker:
                result = await checker.check(site(f"https://127.0.0.1:{port}/"))
        assert result.error is ErrorKind.TLS
        assert result.http_status is None

    asyncio.run(scenario())


@pytest.mark.parametrize("address", ["2130706433", "127.1", "0x7f000001"])
def test_legacy_ipv4_cannot_bypass_literal_policy(address: str) -> None:
    async def scenario() -> None:
        async with HttpChecker() as checker:
            result = await checker.check(site(f"http://{address}/"))
        assert result.error is ErrorKind.ADDRESS_POLICY

    asyncio.run(scenario())


def test_malformed_external_status_cannot_escape_as_programming_error() -> None:
    async def scenario() -> None:
        async def respond(request: web.Request) -> web.Response:
            return web.Response(status=999)

        async with http_fixture(respond) as url:
            async with HttpChecker(fixture_hosts=frozenset({"127.0.0.1"})) as checker:
                result = await checker.check(site(url))
        assert result.error is ErrorKind.HTTP_STATUS
        assert result.http_status is None

    asyncio.run(scenario())


def test_capacity_timeout_does_not_make_a_website_failure() -> None:
    async def scenario() -> None:
        entered = asyncio.Event()
        released = asyncio.Event()
        requests = 0

        async def respond(request: web.Request) -> web.Response:
            nonlocal requests
            requests += 1
            entered.set()
            await released.wait()
            return web.Response(text="ready")

        async with http_fixture(respond) as url:
            async with HttpChecker(
                max_concurrency=1, fixture_hosts=frozenset({"127.0.0.1"})
            ) as checker:
                checking = asyncio.create_task(checker.check(site(url)))
                await entered.wait()
                with pytest.raises(CheckDeferred):
                    await checker.check(site(url, total_timeout=0.02))
                assert requests == 1
                released.set()
                assert (await checking).success

    asyncio.run(scenario())


def test_capacity_wait_does_not_consume_network_deadline_or_latency() -> None:
    async def scenario() -> None:
        entered = asyncio.Event()

        async def respond(request: web.Request) -> web.Response:
            if request.path == "/occupied":
                entered.set()
                await asyncio.sleep(0.15)
            else:
                await asyncio.sleep(0.1)
            return web.Response(text="ready")

        async with http_fixture(respond) as url:
            async with HttpChecker(
                max_concurrency=1, fixture_hosts=frozenset({"127.0.0.1"})
            ) as checker:
                checking = asyncio.create_task(checker.check(site(url + "/occupied")))
                await entered.wait()
                started = time.monotonic()
                result = await checker.check(site(url + "/next", total_timeout=0.2))
                elapsed_ms = (time.monotonic() - started) * 1000
                assert (await checking).success
        assert result.success
        assert result.latency_ms is not None
        assert elapsed_ms - result.latency_ms >= 100

    asyncio.run(scenario())


def test_cancelled_checks_restore_capacity_for_the_next_request() -> None:
    async def scenario() -> None:
        entered = asyncio.Event()

        async def respond(request: web.Request) -> web.Response:
            if request.path == "/occupied":
                entered.set()
                await asyncio.sleep(60)
            return web.Response(text="ready")

        async with http_fixture(respond) as url:
            async with HttpChecker(
                max_concurrency=1, fixture_hosts=frozenset({"127.0.0.1"})
            ) as checker:
                active = asyncio.create_task(checker.check(site(url + "/occupied")))
                await entered.wait()
                waiting = asyncio.create_task(checker.check(site(url)))
                await asyncio.sleep(0)
                waiting.cancel()
                active.cancel()
                for checking in (waiting, active):
                    with pytest.raises(asyncio.CancelledError):
                        await checking
                assert (await checker.check(site(url, total_timeout=0.1))).success

    asyncio.run(scenario())


def test_post_sends_configured_json_bytes_and_unicode_without_extra_fields() -> None:
    async def scenario() -> None:
        payload = {
            "probe": "availability",
            "message": "Привет 🌍",
            "nested": {"enabled": True, "threshold": 2},
            "values": [None, 1.5],
        }
        encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()
        received: list[bytes] = []

        async def respond(request: web.Request) -> web.Response:
            assert request.method == "POST"
            assert request.path == "/health/probe"
            assert request.headers["Content-Type"] == "application/json"
            assert request.content_length == len(encoded)
            assert "Authorization" not in request.headers
            assert "Cookie" not in request.headers
            received.append(await request.read())
            assert json.loads(received[-1]) == payload
            return web.Response(status=201, text="READY")

        async with http_fixture(respond) as url:
            async with HttpChecker(fixture_hosts=frozenset({"127.0.0.1"})) as checker:
                result = await checker.check(
                    site(
                        url + "/health/probe",
                        method="POST",
                        request_body=encoded,
                        marker="READY",
                        status_min=201,
                        status_max=201,
                    )
                )
        assert result.success
        assert result.http_status == 201
        assert received == [encoded]
        assert payload["message"] not in repr(result)

    asyncio.run(scenario())


@pytest.mark.parametrize("method", ["GET", "POST"])
def test_bodyless_checks_send_no_json_payload(method: str) -> None:
    async def scenario() -> None:
        received: list[bytes] = []

        async def respond(request: web.Request) -> web.Response:
            assert request.method == method
            assert request.headers.get("Content-Type") != "application/json"
            received.append(await request.read())
            return web.Response(status=204)

        async with http_fixture(respond) as url:
            async with HttpChecker(fixture_hosts=frozenset({"127.0.0.1"})) as checker:
                result = await checker.check(site(url, method=method))
        assert result.success
        assert received == [b""]

    asyncio.run(scenario())


def test_post_empty_json_object_remains_an_explicit_json_body() -> None:
    async def scenario() -> None:
        async def respond(request: web.Request) -> web.Response:
            assert request.method == "POST"
            assert request.headers["Content-Type"] == "application/json"
            assert await request.read() == b"{}"
            return web.Response(text="READY")

        async with http_fixture(respond) as url:
            async with HttpChecker(fixture_hosts=frozenset({"127.0.0.1"})) as checker:
                result = await checker.check(site(url, method="POST", request_body=b"{}"))
        assert result.success

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("status", "response_body", "limit", "expected"),
    [
        (200, b"READY", 32, None),
        (503, b"READY", 32, ErrorKind.HTTP_STATUS),
        (200, b"missing", 32, ErrorKind.MARKER),
        (200, b"READY\xff", 32, ErrorKind.MARKER),
        (200, b"READY" + b"a" * 32, 32, ErrorKind.BODY_LIMIT),
    ],
)
def test_post_uses_existing_status_marker_and_response_limits(
    status: int, response_body: bytes, limit: int, expected: ErrorKind | None
) -> None:
    async def scenario() -> None:
        calls = 0
        payload = b'{"probe":"synthetic-private-value"}'

        async def respond(request: web.Request) -> web.Response:
            nonlocal calls
            calls += 1
            assert request.method == "POST"
            assert await request.read() == payload
            return web.Response(status=status, body=response_body)

        async with http_fixture(respond) as url:
            async with HttpChecker(fixture_hosts=frozenset({"127.0.0.1"})) as checker:
                result = await checker.check(
                    site(
                        url,
                        method="POST",
                        request_body=payload,
                        marker="READY",
                        max_body_bytes=limit,
                    )
                )
        assert result.error is expected
        assert result.http_status == status
        assert calls == 1
        assert "synthetic-private-value" not in repr(result)
        assert "READY" not in repr(result)

    asyncio.run(scenario())


@pytest.mark.parametrize("status", [301, 302, 303, 307, 308])
@pytest.mark.parametrize("body", [None, b'{"probe":"availability"}'])
def test_post_redirect_is_never_followed_or_changed_to_get(status: int, body: bytes | None) -> None:
    async def scenario() -> None:
        requests: list[tuple[str, str, bytes]] = []

        async def respond(request: web.Request) -> web.Response:
            requests.append((request.method, request.path, await request.read()))
            return web.Response(status=status, headers={"Location": "/redirected"})

        async with http_fixture(respond) as url:
            async with HttpChecker(fixture_hosts=frozenset({"127.0.0.1"})) as checker:
                result = await checker.check(site(url + "/probe", method="POST", request_body=body))
        assert result.error is ErrorKind.REDIRECT
        assert result.http_status == status
        assert requests == [("POST", "/probe", body or b"")]

    asyncio.run(scenario())


@pytest.mark.parametrize("body", [None, b'{"probe":"availability"}'])
def test_post_disconnect_after_receiving_request_does_not_resend(body: bytes | None) -> None:
    async def scenario() -> None:
        requests: list[tuple[str, bytes]] = []

        async def drop(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            try:
                async with asyncio.timeout(1):
                    headers = await reader.readuntil(b"\r\n\r\n")
                    lines = headers.split(b"\r\n")
                    length = next(
                        (
                            int(line.split(b":", 1)[1])
                            for line in lines
                            if line.lower().startswith(b"content-length:")
                        ),
                        0,
                    )
                    requests.append(
                        (lines[0].split(b" ", 1)[0].decode(), await reader.readexactly(length))
                    )
            finally:
                writer.transport.abort()

        server = await asyncio.start_server(drop, "127.0.0.1", 0)
        async with server:
            port = server.sockets[0].getsockname()[1]
            async with HttpChecker(fixture_hosts=frozenset({"127.0.0.1"})) as checker:
                result = await checker.check(
                    site(f"http://127.0.0.1:{port}/", method="POST", request_body=body)
                )
        assert result.error is ErrorKind.CONNECTION
        assert requests == [("POST", body or b"")]

    asyncio.run(scenario())


@pytest.mark.parametrize("phase", ["headers", "body"])
def test_post_deadline_does_not_resend_a_received_request(phase: str) -> None:
    async def scenario() -> None:
        calls = 0

        async def respond(request: web.Request) -> web.StreamResponse:
            nonlocal calls
            calls += 1
            assert request.method == "POST"
            assert await request.read() == b"{}"
            response = web.StreamResponse()
            if phase == "body":
                await response.prepare(request)
            await asyncio.Event().wait()
            return response

        async with http_fixture(respond) as url:
            async with HttpChecker(fixture_hosts=frozenset({"127.0.0.1"})) as checker:
                result = await checker.check(
                    site(url, method="POST", request_body=b"{}", marker="READY", total_timeout=0.03)
                )
        assert result.error is ErrorKind.TIMEOUT
        assert calls == 1

    asyncio.run(scenario())


def test_post_cancellation_does_not_resend_and_releases_request_capacity() -> None:
    async def scenario() -> None:
        entered = asyncio.Event()
        methods: list[str] = []

        async def respond(request: web.Request) -> web.Response:
            methods.append(request.method)
            if request.method == "POST":
                assert await request.read() == b"{}"
                entered.set()
                await asyncio.Event().wait()
            return web.Response(text="READY")

        async with http_fixture(respond) as url:
            async with HttpChecker(
                max_concurrency=1, fixture_hosts=frozenset({"127.0.0.1"})
            ) as checker:
                checking = asyncio.create_task(
                    checker.check(site(url, method="POST", request_body=b"{}"))
                )
                await entered.wait()
                checking.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await checking
                assert (await checker.check(site(url, total_timeout=0.1))).success
        assert methods == ["POST", "GET"]

    asyncio.run(scenario())


def test_post_is_sent_again_only_when_a_new_check_is_requested() -> None:
    async def scenario() -> None:
        requests: list[bytes] = []

        async def respond(request: web.Request) -> web.Response:
            assert request.method == "POST"
            requests.append(await request.read())
            return web.Response(status=503 if len(requests) == 1 else 200, text="READY")

        async with http_fixture(respond) as url:
            async with HttpChecker(fixture_hosts=frozenset({"127.0.0.1"})) as checker:
                configured = site(url, method="POST", request_body=b"{}", marker="READY")
                first = await checker.check(configured)
                assert first.error is ErrorKind.HTTP_STATUS
                assert requests == [b"{}"]
                assert (await checker.check(configured)).success
        assert requests == [b"{}", b"{}"]

    asyncio.run(scenario())
