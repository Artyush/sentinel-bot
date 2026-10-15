"""Bounded HTTP observations for operator-configured website targets."""

import asyncio
import ipaddress
import re
import socket
import time
import zlib
from types import TracebackType

import aiohttp
from aiohttp.abc import AbstractResolver, ResolveResult
from aiohttp.resolver import AsyncResolver
from yarl import URL

from sentinel_bot.config import SiteConfig
from sentinel_bot.models import CheckResult, ErrorKind

_REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})
_MAX_REDIRECTS = 3
_CHUNK_SIZE = 8192


class AddressPolicyError(Exception):
    """A target resolved to an address outside the website policy."""


class CheckDeferred(Exception):
    """Local request capacity was unavailable; no website observation was made."""


def _validate_address(address: str) -> None:
    try:
        parsed = ipaddress.ip_address(address)
    except ValueError as error:
        raise AddressPolicyError from error
    if not parsed.is_global or parsed.is_multicast or parsed.is_reserved:
        raise AddressPolicyError


class PublicAddressResolver(AbstractResolver):
    """Validate every DNS answer before the connector uses its numeric address."""

    def __init__(self, fixture_hosts: frozenset[str] = frozenset()) -> None:
        self._resolver = AsyncResolver(timeout=1.0, tries=2)
        self._fixture_hosts = fixture_hosts

    async def resolve(
        self, host: str, port: int = 0, family: socket.AddressFamily = socket.AF_UNSPEC
    ) -> list[ResolveResult]:
        answers = await self._resolver.resolve(host, port, family)
        if host not in self._fixture_hosts:
            for answer in answers:
                _validate_address(answer["host"])
        return answers

    async def close(self) -> None:
        await self._resolver.close()


class HttpChecker:
    """Check websites without ambient credentials or unbounded response reads.

    Fixture hosts are an explicit development boundary. Live application wiring
    supplies no exemptions; only the isolated synthetic demo supplies its host.
    """

    def __init__(
        self, max_concurrency: int = 5, fixture_hosts: frozenset[str] = frozenset()
    ) -> None:
        if max_concurrency < 1:
            raise ValueError("HTTP concurrency must be positive")
        self._max_concurrency = max_concurrency
        self._fixture_hosts = fixture_hosts
        self._semaphore = asyncio.Semaphore(max_concurrency)
        self._session: aiohttp.ClientSession | None = None
        self._resolver: PublicAddressResolver | None = None

    async def __aenter__(self) -> HttpChecker:
        if self._session is not None:
            raise RuntimeError("HTTP checker is already open")
        self._resolver = PublicAddressResolver(self._fixture_hosts)
        connector = aiohttp.TCPConnector(
            resolver=self._resolver,
            family=socket.AF_UNSPEC,
            use_dns_cache=False,
            force_close=True,
            limit=self._max_concurrency,
            limit_per_host=self._max_concurrency,
            ssl=True,
        )
        self._session = aiohttp.ClientSession(
            connector=connector,
            cookie_jar=aiohttp.DummyCookieJar(),
            trust_env=False,
            auto_decompress=False,
            read_bufsize=_CHUNK_SIZE,
            max_line_size=8192,
            max_field_size=8192,
            max_headers=64,
            headers={"User-Agent": "SentinelBot", "Accept-Encoding": "gzip, deflate"},
        )
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None
        if self._resolver is not None:
            await self._resolver.close()
            self._resolver = None

    async def check(self, site: SiteConfig) -> CheckResult:
        """Return an observation, or defer when local capacity remains unavailable."""
        if self._session is None:
            raise RuntimeError("HTTP checker is not open")
        try:
            async with asyncio.timeout(site.total_timeout):
                await self._semaphore.acquire()
        except TimeoutError:
            raise CheckDeferred("HTTP capacity wait exceeded its deadline") from None
        started = time.monotonic()
        try:
            try:
                async with asyncio.timeout(site.total_timeout):
                    return await self._probe(site, started)
            except AddressPolicyError, aiohttp.InvalidURL:
                error = ErrorKind.ADDRESS_POLICY
            except TimeoutError:
                error = ErrorKind.TIMEOUT
            except aiohttp.ClientConnectorDNSError:
                error = ErrorKind.DNS
            except aiohttp.ClientSSLError, aiohttp.ServerFingerprintMismatch:
                error = ErrorKind.TLS
            except aiohttp.ClientConnectionError, aiohttp.ClientPayloadError:
                error = ErrorKind.CONNECTION
            except aiohttp.ClientResponseError:
                error = ErrorKind.CONNECTION
            return self._result(started, False, error, None)
        finally:
            self._semaphore.release()

    async def _probe(self, site: SiteConfig, started: float) -> CheckResult:
        session = self._session
        if session is None:
            raise RuntimeError("HTTP checker is not open")
        target = URL(site.url)
        origin = target.origin()
        timeout = aiohttp.ClientTimeout(
            total=None, connect=site.connect_timeout, sock_connect=site.connect_timeout
        )
        for redirect_count in range(_MAX_REDIRECTS + 1):
            self._validate_target(target)
            async with session.get(target, allow_redirects=False, timeout=timeout) as response:
                status = response.status
                if not 100 <= status <= 599:
                    response.close()
                    return self._result(started, False, ErrorKind.HTTP_STATUS, None)
                try:
                    if status in _REDIRECT_STATUSES:
                        location = response.headers.get("Location")
                        if location is None or redirect_count == _MAX_REDIRECTS:
                            return self._result(started, False, ErrorKind.REDIRECT, status)
                        try:
                            redirected = target.join(URL(location))
                            self._validate_target(redirected)
                            if redirected.origin() != origin:
                                return self._result(started, False, ErrorKind.REDIRECT, status)
                        except ValueError, AddressPolicyError:
                            return self._result(started, False, ErrorKind.REDIRECT, status)
                        target = redirected
                        continue
                    if not site.status_min <= status <= site.status_max:
                        return self._result(started, False, ErrorKind.HTTP_STATUS, status)
                    if site.marker is not None:
                        error = await self._marker_error(response, site)
                        if error is not None:
                            return self._result(started, False, error, status)
                    return self._result(started, True, None, status)
                finally:
                    response.close()
        raise AssertionError("Redirect bound must return a result")

    def _validate_target(self, target: URL) -> None:
        host = target.host
        if (
            target.scheme not in {"http", "https"}
            or host is None
            or target.user is not None
            or target.password is not None
            or target.query_string
            or target.fragment
        ):
            raise AddressPolicyError
        if host in self._fixture_hosts:
            return
        try:
            ipaddress.ip_address(host)
        except ValueError:
            if re.fullmatch(r"(?:0x[0-9a-f]+|[0-9]+)(?:\.(?:0x[0-9a-f]+|[0-9]+))*", host, re.I):
                raise AddressPolicyError from None
            return
        _validate_address(host)

    @staticmethod
    async def _marker_error(response: aiohttp.ClientResponse, site: SiteConfig) -> ErrorKind | None:
        limit = site.max_body_bytes
        if response.content_length is not None and response.content_length > limit:
            return ErrorKind.BODY_LIMIT
        encoding = response.headers.get("Content-Encoding", "identity").lower().strip()
        if encoding not in {"identity", "gzip", "deflate"}:
            return ErrorKind.MARKER
        decoder = None
        if encoding != "identity":
            decoder = zlib.decompressobj(
                16 + zlib.MAX_WBITS if encoding == "gzip" else zlib.MAX_WBITS
            )
        body = bytearray()
        encoded_size = 0
        try:
            async for chunk in response.content.iter_chunked(_CHUNK_SIZE):
                encoded_size += len(chunk)
                if encoded_size > limit:
                    return ErrorKind.BODY_LIMIT
                decoded = (
                    chunk if decoder is None else decoder.decompress(chunk, limit - len(body) + 1)
                )
                body.extend(decoded)
                if len(body) > limit:
                    return ErrorKind.BODY_LIMIT
            if decoder is not None and (not decoder.eof or decoder.unused_data):
                return ErrorKind.MARKER
            text = body.decode("utf-8")
        except UnicodeDecodeError, zlib.error:
            return ErrorKind.MARKER
        return None if site.marker is not None and site.marker in text else ErrorKind.MARKER

    @staticmethod
    def _result(
        started: float, success: bool, error: ErrorKind | None, status: int | None
    ) -> CheckResult:
        return CheckResult(
            success=success,
            error=error,
            http_status=status,
            latency_ms=(time.monotonic() - started) * 1000,
        )
