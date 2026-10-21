"""Bounded Node Exporter exposition and fixed-origin authenticated HTTP adapter."""

import asyncio
import ipaddress
import math
import re
import socket
import time
from dataclasses import dataclass, field
from types import TracebackType

import aiohttp
from aiohttp.abc import AbstractResolver, ResolveResult
from aiohttp.resolver import AsyncResolver
from prometheus_client.parser import text_string_to_metric_families
from yarl import URL

from sentinel_bot.config import ServerConfig
from sentinel_bot.http import AddressPolicyError, CheckDeferred
from sentinel_bot.metric_models import CpuCounter, FilesystemReading, MetricData, _capacity_used
from sentinel_bot.models import CheckResult, ErrorKind
from sentinel_bot.workers import await_completion, run_blocking

MAX_METRICS_BYTES = 1024 * 1024
MAX_METRICS_SAMPLES = 10000
MAX_METRICS_FAMILIES = 512
MAX_METRICS_LINE_BYTES = 16384
MAX_CPUS = 256
_CPU_MODES = frozenset({"user", "nice", "system", "idle", "iowait", "irq", "softirq", "steal"})
_REQUIRED_NAMES = frozenset(
    {
        "node_cpu_seconds_total",
        "node_boot_time_seconds",
        "node_memory_MemTotal_bytes",
        "node_memory_MemAvailable_bytes",
        "node_filesystem_size_bytes",
        "node_filesystem_avail_bytes",
        "node_filesystem_readonly",
        "node_filesystem_device_error",
        "node_scrape_collector_success",
    }
)


class InvalidMetrics(ValueError):
    """The bounded response does not satisfy the supported exposition contract."""


@dataclass(frozen=True)
class ScrapeResult:
    result: CheckResult
    snapshot: MetricData | None = field(default=None, repr=False)


@dataclass(frozen=True)
class _Sample:
    labels: tuple[tuple[str, str], ...]
    value: float | None


def _one(samples: list[_Sample], labels: tuple[tuple[str, str], ...] = ()) -> float | None:
    return samples[0].value if len(samples) == 1 and samples[0].labels == labels else None


def _collector(samples: list[_Sample], name: str) -> bool:
    matching = [sample for sample in samples if dict(sample.labels).get("collector") == name]
    return _one(matching, (("collector", name),)) == 1


def _cpu(samples: list[_Sample]) -> tuple[CpuCounter, ...] | None:
    counters: dict[tuple[str, str], float] = {}
    for sample in samples:
        labels = dict(sample.labels)
        if labels.get("mode") in {"guest", "guest_nice"}:
            continue
        if set(labels) != {"cpu", "mode"}:
            return None
        cpu = labels["cpu"]
        mode = labels["mode"]
        if (
            not re.fullmatch(r"0|[1-9][0-9]{0,5}", cpu)
            or mode not in _CPU_MODES
            or sample.value is None
            or sample.value < 0
            or (cpu, mode) in counters
        ):
            return None
        counters[cpu, mode] = sample.value
    cpus = {cpu for cpu, _ in counters}
    if not cpus or len(cpus) > MAX_CPUS:
        return None
    if any({mode for core, mode in counters if core == cpu} != _CPU_MODES for cpu in cpus):
        return None
    return tuple(CpuCounter(cpu, mode, value) for (cpu, mode), value in sorted(counters.items()))


def _filesystem(
    samples: dict[str, list[_Sample]], alias: str, mount: str, collector_ok: bool
) -> FilesystemReading:
    if not collector_ok:
        return FilesystemReading(alias, None, "collector")
    selected: list[_Sample] = []
    for name in (
        "node_filesystem_size_bytes",
        "node_filesystem_avail_bytes",
        "node_filesystem_readonly",
        "node_filesystem_device_error",
    ):
        matches = [s for s in samples.get(name, []) if dict(s.labels).get("mountpoint") == mount]
        if len(matches) != 1:
            return FilesystemReading(alias, None, "missing" if not matches else "ambiguous")
        selected.append(matches[0])
    labels = dict(selected[0].labels)
    if (
        set(labels)
        not in (
            {"device", "mountpoint", "fstype"},
            {"device", "mountpoint", "fstype", "device_error"},
        )
        or not labels["device"]
        or not labels["fstype"]
        or labels.get("device_error", "") not in {"", "0"}
        or any(sample.labels != selected[0].labels for sample in selected)
    ):
        return FilesystemReading(alias, None, "ambiguous")
    size, available, readonly, device_error = (sample.value for sample in selected)
    if device_error != 0:
        return FilesystemReading(alias, None, "device")
    if readonly != 0:
        return FilesystemReading(alias, None, "read_only")
    value = _capacity_used(available, size)
    return FilesystemReading(alias, value, "invalid" if value is None else None)


def parse_metrics(text: str, mounts: tuple[tuple[str, str], ...] = ()) -> MetricData:
    """Normalize required Node Exporter families without retaining host labels or text."""
    if len(text.encode("utf-8")) > MAX_METRICS_BYTES:
        raise InvalidMetrics("Metrics body exceeds its bound")
    lines = text.splitlines()
    if len(lines) > MAX_METRICS_SAMPLES + MAX_METRICS_FAMILIES * 2 or any(
        len(line.encode("utf-8")) > MAX_METRICS_LINE_BYTES for line in lines
    ):
        raise InvalidMetrics("Metrics line bounds exceeded")
    collected: dict[str, list[_Sample]] = {}
    sample_count = 0
    try:
        for family_count, family in enumerate(text_string_to_metric_families(text), start=1):
            if family_count > MAX_METRICS_FAMILIES:
                raise InvalidMetrics("Metrics family bound exceeded")
            for sample in family.samples:
                sample_count += 1
                if sample_count > MAX_METRICS_SAMPLES:
                    raise InvalidMetrics("Metrics sample bound exceeded")
                if (
                    len(sample.name) > 128
                    or len(sample.labels) > 8
                    or any(
                        len(key) > 128 or len(value) > 1024 for key, value in sample.labels.items()
                    )
                ):
                    raise InvalidMetrics("Metric label bounds exceeded")
                if sample.name in _REQUIRED_NAMES:
                    expected = "counter" if sample.name == "node_cpu_seconds_total" else "gauge"
                    valid = sample.timestamp is None and family.type in {expected, "unknown"}
                    value = sample.value if valid and math.isfinite(sample.value) else None
                    collected.setdefault(sample.name, []).append(
                        _Sample(tuple(sorted(sample.labels.items())), value)
                    )
    except ValueError, OverflowError:
        raise InvalidMetrics("Invalid Prometheus text exposition") from None
    collectors = collected.get("node_scrape_collector_success", [])
    cpu = (
        _cpu(collected.get("node_cpu_seconds_total", [])) if _collector(collectors, "cpu") else None
    )
    boot = (
        _one(collected.get("node_boot_time_seconds", []))
        if _collector(collectors, "stat")
        else None
    )
    total = available = None
    if _collector(collectors, "meminfo"):
        total = _one(collected.get("node_memory_MemTotal_bytes", []))
        available = _one(collected.get("node_memory_MemAvailable_bytes", []))
    filesystems = tuple(
        _filesystem(collected, alias, mount, _collector(collectors, "filesystem"))
        for alias, mount in mounts
    )
    return MetricData(cpu, boot, total, available, filesystems)


def _validate_address(address: str, allowed_cidrs: tuple[str, ...]) -> None:
    try:
        parsed = ipaddress.ip_address(address)
    except ValueError:
        raise AddressPolicyError from None
    if (
        parsed.is_loopback
        or parsed.is_link_local
        or parsed.is_multicast
        or parsed.is_unspecified
        or parsed.is_reserved
        or (isinstance(parsed, ipaddress.IPv6Address) and parsed.ipv4_mapped is not None)
    ):
        raise AddressPolicyError
    if parsed.is_global:
        return
    if not any(parsed in ipaddress.ip_network(network) for network in allowed_cidrs):
        raise AddressPolicyError


class MetricsAddressResolver(AbstractResolver):
    """A source-specific allowlist applies to every returned DNS answer."""

    def __init__(self, allowed_cidrs: tuple[str, ...], fixture_hosts: frozenset[str]) -> None:
        self._resolver = AsyncResolver(timeout=1.0, tries=2)
        self._allowed_cidrs = allowed_cidrs
        self._fixture_hosts = fixture_hosts

    async def resolve(
        self, host: str, port: int = 0, family: socket.AddressFamily = socket.AF_UNSPEC
    ) -> list[ResolveResult]:
        answers = await self._resolver.resolve(host, port, family)
        if host not in self._fixture_hosts:
            for answer in answers:
                _validate_address(answer["host"], self._allowed_cidrs)
        return answers

    async def close(self) -> None:
        await self._resolver.close()


@dataclass
class _Connection:
    session: aiohttp.ClientSession
    resolver: MetricsAddressResolver

    async def close(self) -> None:
        try:
            await self.session.close()
        finally:
            await self.resolver.close()


class MetricsChecker:
    """Read fixed HTTPS origins with isolated credentials and per-source address rules."""

    def __init__(
        self,
        max_concurrency: int = 5,
        fixture_hosts: frozenset[str] = frozenset(),
        *,
        semaphore: asyncio.Semaphore | None = None,
    ) -> None:
        if not 1 <= max_concurrency <= 20:
            raise ValueError("Metrics concurrency must be between 1 and 20")
        self._semaphore = semaphore if semaphore is not None else asyncio.Semaphore(max_concurrency)
        self._fixture_hosts = fixture_hosts
        self._connections: dict[tuple[str, str, tuple[str, ...]], _Connection] = {}
        self._open = False

    async def __aenter__(self) -> MetricsChecker:
        if self._open:
            raise RuntimeError("Metrics checker is already open")
        self._open = True
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await await_completion(asyncio.create_task(self._close()))

    async def _close(self) -> None:
        self._open = False
        connections, self._connections = self._connections, {}
        async with asyncio.TaskGroup() as group:
            for connection in connections.values():
                group.create_task(connection.close())

    def _target(self, server: ServerConfig) -> URL:
        target = URL(server.url)
        host = target.host
        if (
            host is None
            or target.user is not None
            or target.password is not None
            or target.query_string
            or target.fragment
            or target.scheme not in {"https", "http"}
            or (target.scheme != "https" and host not in self._fixture_hosts)
        ):
            raise AddressPolicyError
        if host not in self._fixture_hosts:
            try:
                ipaddress.ip_address(host)
            except ValueError:
                if re.fullmatch(r"(?:0x[0-9a-f]+|[0-9]+)(?:\.(?:0x[0-9a-f]+|[0-9]+))*", host, re.I):
                    raise AddressPolicyError from None
            else:
                _validate_address(host, server.allowed_cidrs)
        return target

    async def _connection(self, server: ServerConfig) -> _Connection:
        key = server.alias, server.url, server.allowed_cidrs
        if key in self._connections:
            return self._connections[key]
        if len(self._connections) >= 20:
            raise ValueError("Metrics source bound exceeded")
        resolver = MetricsAddressResolver(server.allowed_cidrs, self._fixture_hosts)
        connector = None
        try:
            connector = aiohttp.TCPConnector(
                resolver=resolver, use_dns_cache=False, force_close=True, limit=1, ssl=True
            )
            session = aiohttp.ClientSession(
                connector=connector,
                cookie_jar=aiohttp.DummyCookieJar(),
                trust_env=False,
                auto_decompress=False,
                read_bufsize=8192,
                max_line_size=8192,
                max_field_size=8192,
                max_headers=64,
                headers={
                    "User-Agent": "SentinelBot",
                    "Accept": "text/plain; version=0.0.4",
                    "Accept-Encoding": "identity",
                },
            )
        except BaseException:

            async def close_partial() -> None:
                if connector is not None:
                    await connector.close()
                await resolver.close()

            await await_completion(asyncio.create_task(close_partial()))
            raise
        connection = _Connection(session, resolver)
        self._connections[key] = connection
        return connection

    async def check(self, server: ServerConfig) -> ScrapeResult:
        if not self._open:
            raise RuntimeError("Metrics checker is not open")
        try:
            async with asyncio.timeout(server.total_timeout):
                await self._semaphore.acquire()
        except TimeoutError:
            raise CheckDeferred("Metrics capacity wait exceeded its deadline") from None
        started = time.monotonic()
        status = None
        try:
            try:
                async with asyncio.timeout(server.total_timeout):
                    target = self._target(server)
                    connection = await self._connection(server)
                    timeout = aiohttp.ClientTimeout(
                        total=None,
                        connect=server.connect_timeout,
                        sock_connect=server.connect_timeout,
                    )
                    async with connection.session.get(
                        target,
                        headers={"Authorization": f"Bearer {server.token}"},
                        allow_redirects=False,
                        timeout=timeout,
                    ) as response:
                        status = response.status
                        if status in {301, 302, 303, 307, 308}:
                            return self._result(started, ErrorKind.REDIRECT, status)
                        if status != 200:
                            return self._result(started, ErrorKind.HTTP_STATUS, status)
                        if (
                            response.headers.get("Content-Encoding", "identity").lower().strip()
                            != "identity"
                            or response.content_type != "text/plain"
                        ):
                            return self._result(started, ErrorKind.METRICS, status)
                        if (
                            response.content_length is not None
                            and response.content_length > MAX_METRICS_BYTES
                        ):
                            return self._result(started, ErrorKind.BODY_LIMIT, status)
                        body = bytearray()
                        async for chunk in response.content.iter_chunked(8192):
                            body.extend(chunk)
                            if len(body) > MAX_METRICS_BYTES:
                                return self._result(started, ErrorKind.BODY_LIMIT, status)
                        try:
                            text = body.decode("utf-8")
                        except UnicodeDecodeError:
                            return self._result(started, ErrorKind.METRICS, status)
                        mounts = tuple((mount.alias, mount.mount) for mount in server.mounts)
                        data = await run_blocking(lambda: parse_metrics(text, mounts))
                        return self._result(started, None, status, data)
            except AddressPolicyError, aiohttp.InvalidURL:
                error = ErrorKind.ADDRESS_POLICY
            except TimeoutError:
                error = ErrorKind.TIMEOUT
            except aiohttp.ClientConnectorDNSError:
                error = ErrorKind.DNS
            except aiohttp.ClientSSLError, aiohttp.ServerFingerprintMismatch:
                error = ErrorKind.TLS
            except (
                aiohttp.ClientConnectionError,
                aiohttp.ClientPayloadError,
                aiohttp.ClientResponseError,
            ):
                error = ErrorKind.CONNECTION
            except InvalidMetrics:
                error = ErrorKind.METRICS
            return self._result(started, error, status)
        finally:
            self._semaphore.release()

    @staticmethod
    def _result(
        started: float,
        error: ErrorKind | None,
        status: int | None,
        snapshot: MetricData | None = None,
    ) -> ScrapeResult:
        return ScrapeResult(
            CheckResult(error is None, error, status, (time.monotonic() - started) * 1000), snapshot
        )
