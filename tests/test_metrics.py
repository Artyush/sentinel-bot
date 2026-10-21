import asyncio
import gzip
import socket
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import replace

import aiohttp
import pytest
from aiohttp import web

from sentinel_bot.config import ServerConfig
from sentinel_bot.http import AddressPolicyError, CheckDeferred
from sentinel_bot.metrics import (
    MAX_METRICS_BYTES,
    InvalidMetrics,
    MetricsAddressResolver,
    MetricsChecker,
    _validate_address,
    parse_metrics,
)
from sentinel_bot.models import ErrorKind

KEY = "synthetic_exporter_credential_123456789"
BODY = """# TYPE node_scrape_collector_success gauge
node_scrape_collector_success{collector="meminfo"} 1
# TYPE node_memory_MemTotal_bytes gauge
node_memory_MemTotal_bytes 1000
# TYPE node_memory_MemAvailable_bytes gauge
node_memory_MemAvailable_bytes 250
"""


@asynccontextmanager
async def http_fixture(
    handler: Callable[[web.Request], Awaitable[web.StreamResponse]],
) -> AsyncIterator[str]:
    app = web.Application()
    app.router.add_route("*", "/{path:.*}", handler)
    runner = web.AppRunner(app, shutdown_timeout=0.05)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    try:
        yield f"http://127.0.0.1:{runner.addresses[0][1]}"
    finally:
        await runner.cleanup()


def server(url: str, **changes) -> ServerConfig:
    return replace(ServerConfig("sample", url, token=KEY), **changes)


def test_fixed_origin_bearer_get_disables_ambient_proxy_cookies_and_caches(monkeypatch):
    async def scenario():
        calls = []

        async def handler(request):
            calls.append((request.method, request.path, dict(request.headers)))
            response = web.Response(text=BODY)
            response.set_cookie("secret", "not-reused")
            return response

        monkeypatch.setenv("HTTP_PROXY", "http://127.0.0.1:1")
        async with http_fixture(handler) as base:
            async with MetricsChecker(fixture_hosts=frozenset({"127.0.0.1"})) as checker:
                source = server(base + "/metrics")
                first = await checker.check(source)
                second = await checker.check(source)
                assert len(checker._connections) == 1
                connection = next(iter(checker._connections.values()))
                assert not connection.session.trust_env
                assert not connection.session.connector._use_dns_cache
                assert connection.session.connector.force_close
            assert connection.session.closed
            assert checker._connections == {}
        for method, path, headers in calls:
            assert method == "GET" and path == "/metrics"
            assert headers["Authorization"] == f"Bearer {KEY}"
            assert headers["Accept-Encoding"] == "identity"
            assert headers["Accept"] == "text/plain; version=0.0.4"
            assert "Cookie" not in headers
        assert first.result.success and second.result.success
        assert first.snapshot.memory_total == 1000
        assert first.snapshot.memory_available == 250
        assert KEY not in repr(first)

    asyncio.run(scenario())


@pytest.mark.parametrize("status", [201, 204, 304, 400, 401, 403, 404, 429, 500, 503])
def test_non_200_status_is_safe_unavailable_observation(status):
    async def scenario():
        async def handler(request):
            return web.Response(status=status, text="private exporter diagnostic " + KEY)

        async with http_fixture(handler) as base:
            async with MetricsChecker(fixture_hosts=frozenset({"127.0.0.1"})) as checker:
                result = await checker.check(server(base))
        assert result.result.error is ErrorKind.HTTP_STATUS
        assert result.result.http_status == status
        assert result.snapshot is None
        assert KEY not in repr(result)

    asyncio.run(scenario())


@pytest.mark.parametrize("status", [301, 302, 303, 307, 308])
def test_redirect_never_replays_exporter_credentials(status):
    async def scenario():
        calls = []

        async def handler(request):
            calls.append(request.path)
            return web.Response(status=status, headers={"Location": "/stolen"})

        async with http_fixture(handler) as base:
            async with MetricsChecker(fixture_hosts=frozenset({"127.0.0.1"})) as checker:
                result = await checker.check(server(base + "/metrics"))
        assert result.result.error is ErrorKind.REDIRECT
        assert calls == ["/metrics"]

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "body,headers,expected",
    [
        (b"invalid\xff", {}, ErrorKind.METRICS),
        (b"broken{", {}, ErrorKind.METRICS),
        (BODY.encode(), {"Content-Type": "application/json"}, ErrorKind.METRICS),
        (gzip.compress(BODY.encode()), {"Content-Encoding": "gzip"}, ErrorKind.METRICS),
        (BODY.encode(), {"Content-Encoding": "br"}, ErrorKind.METRICS),
        (b"x" * (MAX_METRICS_BYTES + 1), {}, ErrorKind.BODY_LIMIT),
    ],
    ids=["utf8", "exposition", "content_type", "gzip", "brotli", "body_limit"],
)
def test_invalid_untrusted_bodies_are_normalized(body, headers, expected):
    async def scenario():
        async def handler(request):
            return web.Response(body=body, headers={"Content-Type": "text/plain", **headers})

        async with http_fixture(handler) as base:
            async with MetricsChecker(fixture_hosts=frozenset({"127.0.0.1"})) as checker:
                result = await checker.check(server(base))
        assert result.result.error is expected
        assert result.result.http_status == 200
        assert result.snapshot is None

    asyncio.run(scenario())


def test_chunked_body_without_content_length_stays_bounded():
    async def scenario():
        async def handler(request):
            response = web.StreamResponse(headers={"Content-Type": "text/plain"})
            await response.prepare(request)
            try:
                for _ in range(130):
                    await response.write(b"x" * 8192)
            except ConnectionResetError:
                pass
            return response

        async with http_fixture(handler) as base:
            async with MetricsChecker(fixture_hosts=frozenset({"127.0.0.1"})) as checker:
                result = await checker.check(server(base))
        assert result.result.error is ErrorKind.BODY_LIMIT

    asyncio.run(scenario())


def test_total_deadline_includes_slow_body_and_releases_shared_capacity():
    async def scenario():
        async def handler(request):
            if request.path == "/slow":
                response = web.StreamResponse(headers={"Content-Type": "text/plain"})
                await response.prepare(request)
                await asyncio.sleep(0.1)
                return response
            return web.Response(text=BODY)

        async with http_fixture(handler) as base:
            async with MetricsChecker(1, frozenset({"127.0.0.1"})) as checker:
                result = await checker.check(server(base + "/slow", total_timeout=0.02))
                success = await checker.check(server(base + "/metrics"))
        assert result.result.error is ErrorKind.TIMEOUT
        assert success.result.success

    asyncio.run(scenario())


def test_waiting_for_local_capacity_defers_without_failed_observation():
    async def scenario():
        arrived = asyncio.Event()

        async def handler(request):
            arrived.set()
            await asyncio.sleep(0.1)
            return web.Response(text=BODY)

        async with http_fixture(handler) as base:
            async with MetricsChecker(1, frozenset({"127.0.0.1"})) as checker:
                first = asyncio.create_task(checker.check(server(base, total_timeout=0.05)))
                await arrived.wait()
                with pytest.raises(CheckDeferred):
                    await checker.check(server(base, alias="other", total_timeout=0.01))
                assert (await first).result.error is ErrorKind.TIMEOUT

    asyncio.run(scenario())


def test_cancellation_releases_capacity_and_closes_owned_session():
    async def scenario():
        arrived = asyncio.Event()

        async def handler(request):
            arrived.set()
            await asyncio.sleep(0.1)
            return web.Response(text=BODY)

        async with http_fixture(handler) as base:
            async with MetricsChecker(1, frozenset({"127.0.0.1"})) as checker:
                task = asyncio.create_task(checker.check(server(base)))
                await arrived.wait()
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
                assert not checker._semaphore.locked()
                connections = list(checker._connections.values())
            assert all(connection.session.closed for connection in connections)

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "address",
    [
        "127.0.0.1",
        "::1",
        "169.254.169.254",
        "224.0.0.1",
        "0.0.0.0",
        "fe80::1",
        "::ffff:10.0.0.1",
        "invalid",
    ],
)
def test_never_allow_special_addresses_even_with_broad_cidrs(address):
    with pytest.raises(AddressPolicyError):
        _validate_address(address, ("0.0.0.0/0", "::/0"))


@pytest.mark.parametrize(
    "address,cidrs",
    [
        ("10.7.1.2", ("10.7.1.0/24",)),
        ("172.20.1.4", ("172.20.0.0/16",)),
        ("192.168.4.7", ("192.168.4.0/24",)),
        ("fd01::2", ("fd01::/64",)),
        ("8.8.8.8", ()),
    ],
)
def test_exact_per_source_allowlist_accepts_intended_targets(address, cidrs):
    _validate_address(address, cidrs)


@pytest.mark.parametrize("cidrs", [(), ("10.8.0.0/16",)])
def test_unallowlisted_private_addresses_are_rejected(cidrs):
    with pytest.raises(AddressPolicyError):
        _validate_address("10.7.1.2", cidrs)


def test_every_dns_answer_must_satisfy_this_sources_allowlist(monkeypatch):
    async def scenario():
        async def answers(self, host, port, family):
            return [
                {
                    "hostname": host,
                    "host": address,
                    "port": port,
                    "family": socket.AF_INET,
                    "proto": 0,
                    "flags": 0,
                }
                for address in ("10.7.1.2", "10.8.1.2")
            ]

        monkeypatch.setattr("sentinel_bot.metrics.AsyncResolver.resolve", answers)
        resolver = MetricsAddressResolver(("10.7.1.0/24",), frozenset())
        try:
            with pytest.raises(AddressPolicyError):
                await resolver.resolve("exporter.example.test", 443)
        finally:
            await resolver.close()

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "url",
    [
        "http://example.com/metrics",
        "https://127.0.0.1/metrics",
        "https://example.com/metrics?token=private",
        "https://example.com/#fragment",
        "https://user:pass@example.com/metrics",
        "https://2130706433/metrics",
    ],
)
def test_invalid_target_is_rejected_before_any_request(url):
    async def scenario():
        async with MetricsChecker() as checker:
            result = await checker.check(server(url))
            assert not checker._connections
        assert result.result.error is ErrorKind.ADDRESS_POLICY
        assert result.result.http_status is None

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "error,expected",
    [
        (aiohttp.ClientConnectorDNSError(None, OSError("secret")), ErrorKind.DNS),
        (aiohttp.ClientSSLError(None, OSError("secret")), ErrorKind.TLS),
        (aiohttp.ClientConnectionError("secret"), ErrorKind.CONNECTION),
        (aiohttp.ClientPayloadError("secret"), ErrorKind.CONNECTION),
    ],
)
def test_expected_network_failures_are_observations(monkeypatch, error, expected):
    async def scenario():
        async def fail(self, source):
            raise error

        monkeypatch.setattr(MetricsChecker, "_connection", fail)
        async with MetricsChecker() as checker:
            result = await checker.check(server("https://example.com/metrics"))
        assert result.result.error is expected
        assert "secret" not in repr(result)

    asyncio.run(scenario())


def test_programming_error_from_parser_remains_visible(monkeypatch):
    async def scenario():
        def fail(*args):
            raise RuntimeError("programming failure")

        monkeypatch.setattr("sentinel_bot.metrics.parse_metrics", fail)

        async def handler(request):
            return web.Response(text=BODY)

        async with http_fixture(handler) as base:
            async with MetricsChecker(fixture_hosts=frozenset({"127.0.0.1"})) as checker:
                with pytest.raises(RuntimeError, match="programming failure"):
                    await checker.check(server(base))

    asyncio.run(scenario())


def test_checker_requires_lifecycle_and_rejects_duplicate_entry():
    async def scenario():
        checker = MetricsChecker()
        with pytest.raises(RuntimeError, match="not open"):
            await checker.check(server("https://example.com/metrics"))
        async with checker:
            with pytest.raises(RuntimeError, match="already open"):
                await checker.__aenter__()

    asyncio.run(scenario())


@pytest.mark.parametrize("count", [0, 21])
def test_invalid_concurrency_fails_before_network_resources(count):
    with pytest.raises(ValueError):
        MetricsChecker(count)


@pytest.mark.parametrize(
    "case", ["body", "line", "samples", "families", "labels", "label_size", "name"]
)
def test_parser_resource_bounds(case):
    text = {
        "body": "#" * (MAX_METRICS_BYTES + 1),
        "line": "#" * 16385,
        "samples": "# TYPE a gauge\n" + "a 1\n" * 10001,
        "families": "\n".join(f"family_{index} 1" for index in range(513)),
        "labels": "a{" + ",".join(f'label_{index}="x"' for index in range(9)) + "} 1",
        "label_size": 'a{label="' + "x" * 1025 + '"} 1',
        "name": "a" * 129 + " 1",
    }[case]
    with pytest.raises(InvalidMetrics):
        parse_metrics(text)


def test_empty_valid_exposition_is_available_but_all_readings_unknown():
    result = parse_metrics("# Empty exporter scrape\n", (("root", "/"),))
    assert result.cpu_counters is result.memory_total is result.memory_available is None
    assert result.filesystems[0].value is None


def test_shared_web_and_metrics_capacity_is_enforced_across_adapters():
    from sentinel_bot.config import SiteConfig
    from sentinel_bot.http import HttpChecker

    async def scenario():
        arrived = asyncio.Event()

        async def handler(request):
            if request.path == "/slow":
                arrived.set()
                await asyncio.sleep(0.1)
            return web.Response(text=BODY)

        budget = asyncio.Semaphore(1)
        async with http_fixture(handler) as base:
            async with (
                HttpChecker(1, frozenset({"127.0.0.1"}), semaphore=budget) as websites,
                MetricsChecker(1, frozenset({"127.0.0.1"}), semaphore=budget) as metrics,
            ):
                first = asyncio.create_task(
                    websites.check(SiteConfig("website", base + "/slow", total_timeout=0.05))
                )
                await arrived.wait()
                with pytest.raises(CheckDeferred):
                    await metrics.check(server(base, total_timeout=0.01))
                assert (await first).error is ErrorKind.TIMEOUT
                assert (await metrics.check(server(base))).result.success

    asyncio.run(scenario())


def test_repeated_cancellation_waits_for_owned_resolver_cleanup(monkeypatch):
    async def scenario():
        cleaning = asyncio.Event()
        finish = asyncio.Event()
        completed = asyncio.Event()
        real_close = MetricsAddressResolver.close

        async def close(resolver):
            cleaning.set()
            await finish.wait()
            await real_close(resolver)
            completed.set()

        monkeypatch.setattr(MetricsAddressResolver, "close", close)

        async def handler(request):
            return web.Response(text=BODY)

        async with http_fixture(handler) as base:
            checker = MetricsChecker(fixture_hosts=frozenset({"127.0.0.1"}))
            await checker.__aenter__()
            assert (await checker.check(server(base))).result.success
            task = asyncio.create_task(checker.__aexit__(None, None, None))
            await cleaning.wait()
            task.cancel()
            await asyncio.sleep(0)
            task.cancel()
            assert not task.done()
            finish.set()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert completed.is_set()
            assert checker._connections == {}

    asyncio.run(scenario())


def test_partial_session_initialization_closes_connector_and_resolver(monkeypatch):
    async def scenario():
        closed = asyncio.Event()
        real_close = MetricsAddressResolver.close

        async def close(resolver):
            await real_close(resolver)
            closed.set()

        def fail(**kwargs):
            raise RuntimeError("session initialization failed")

        monkeypatch.setattr(MetricsAddressResolver, "close", close)
        monkeypatch.setattr("sentinel_bot.metrics.aiohttp.ClientSession", fail)
        async with MetricsChecker() as checker:
            with pytest.raises(RuntimeError, match="session initialization failed"):
                await checker.check(server("https://example.com/metrics"))
            assert not checker._connections
        assert closed.is_set()

    asyncio.run(scenario())
