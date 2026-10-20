"""Bounded email transports; acceptance and durable retries belong to the caller."""

from __future__ import annotations

import asyncio
import ipaddress
import json
import math
import re
from datetime import UTC, datetime
from email.message import EmailMessage
from email.policy import SMTP
from email.utils import parsedate_to_datetime
from types import TracebackType
from typing import Protocol

import aiohttp
from aiohttp.resolver import AsyncResolver
from yarl import URL

from .config import ConfigError, _email_address
from .workers import await_completion

MAX_RESPONSE_BYTES = 16384
MAX_RETRY_AFTER = 7 * 24 * 60 * 60
MAX_BODY_BYTES = 16384


class EmailError(Exception):
    """Only normalized categories escape the boundary, never provider text or addresses."""

    def __init__(
        self,
        category: str,
        retryable: bool,
        *,
        retry_after: float | None = None,
        channel_wide: bool = False,
    ) -> None:
        self.category = category
        self.retryable = retryable
        self.retry_after = retry_after
        self.channel_wide = channel_wide
        super().__init__(f"Email request failed: {category}.")


class EmailTransport(Protocol):
    async def send(self, recipient: str, subject: str, body: str) -> None:
        """Submit once to one recipient; returning means provider acceptance only."""
        ...


def _mailbox(value: str) -> str:
    try:
        return _email_address(value)
    except ConfigError:
        raise ValueError("Email requires a bounded plain ASCII mailbox.") from None


def _timeout(value: float) -> float:
    if isinstance(value, bool) or not math.isfinite(value) or not 0.1 <= value <= 10:
        raise ValueError("Email request timeout must be between 0.1 and 10 seconds.")
    return value


def _message(recipient: str, subject: str, body: str) -> str:
    recipient = _mailbox(recipient)
    try:
        invalid = (
            not subject
            or len(subject) > 200
            or len(subject.encode("utf-8")) > 512
            or any(ord(c) < 32 or ord(c) == 127 for c in subject)
            or not body
            or len(body.encode("utf-8")) > MAX_BODY_BYTES
            or any((ord(c) < 32 and c not in "\n\t") or ord(c) == 127 for c in body)
        )
    except UnicodeEncodeError:
        invalid = True
    if invalid:
        raise ValueError("Email subject or body exceeds supported text limits.")
    return recipient


def _fixture_origin(value: str) -> str:
    try:
        url = URL(value)
        if (
            url.scheme not in {"http", "https"}
            or url.host is None
            or not ipaddress.ip_address(url.host).is_loopback
            or url.user is not None
            or url.password is not None
            or url.path not in {"", "/"}
            or url.query_string
            or url.fragment
            or url.port == 0
        ):
            raise ValueError
        return str(url.origin())
    except ValueError, TypeError:
        raise ValueError("Email fixtures require a numeric loopback origin.") from None


def _retry_after(value: str | None) -> float | None:
    if value is None or len(value) > 128:
        return None
    if re.fullmatch(r"[0-9]{1,128}", value):
        return float(min(int(value), MAX_RETRY_AFTER + 1))
    try:
        parsed = parsedate_to_datetime(value)
        if parsed.tzinfo is None:
            return None
        delay = max(0.0, (parsed - datetime.now(UTC)).total_seconds())
        return min(delay, MAX_RETRY_AFTER + 1)
    except ValueError, TypeError, OverflowError:
        return None


class MailchimpEmailAPI:
    """Submit one recipient through verified fixed-origin Mailchimp Transactional HTTPS."""

    def __init__(
        self,
        api_key: str,
        from_address: str,
        *,
        request_timeout: float = 10.0,
        fixture_base_url: str | None = None,
    ) -> None:
        if re.fullmatch(r"[A-Za-z0-9_-]{16,128}", api_key) is None:
            raise ValueError("Mailchimp requires a bounded API key.")
        self._key = api_key
        self._sender = _mailbox(from_address)
        self._timeout = _timeout(request_timeout)
        self._origin = (
            "https://mandrillapp.com"
            if fixture_base_url is None
            else _fixture_origin(fixture_base_url)
        )
        self._session: aiohttp.ClientSession | None = None
        self._resolver: AsyncResolver | None = None
        self._connector: aiohttp.TCPConnector | None = None

    async def __aenter__(self) -> MailchimpEmailAPI:
        if self._session is not None:
            raise RuntimeError("Email API is already open.")
        try:
            self._resolver = AsyncResolver(timeout=1.0, tries=2)
            self._connector = aiohttp.TCPConnector(
                resolver=self._resolver,
                limit=1,
                limit_per_host=1,
                ssl=True,
                force_close=True,
            )
            self._session = aiohttp.ClientSession(
                connector=self._connector,
                cookie_jar=aiohttp.DummyCookieJar(),
                trust_env=False,
                auto_decompress=False,
                read_bufsize=4096,
                max_line_size=8192,
                max_field_size=8192,
                max_headers=64,
                headers={"User-Agent": "SentinelBot", "Accept-Encoding": "identity"},
            )
        except BaseException:
            await await_completion(asyncio.create_task(self._close()))
            raise
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await await_completion(asyncio.create_task(self._close()))

    async def _close(self) -> None:
        try:
            if self._session is not None:
                await self._session.close()
        finally:
            self._session = None
            try:
                if self._connector is not None and not self._connector.closed:
                    await self._connector.close()
            finally:
                self._connector = None
                try:
                    if self._resolver is not None:
                        await self._resolver.close()
                finally:
                    self._resolver = None

    async def send(self, recipient: str, subject: str, body: str) -> None:
        recipient = _message(recipient, subject, body)
        session = self._session
        if session is None:
            raise RuntimeError("Email API is not open.")
        payload = {
            "key": self._key,
            "message": {
                "from_email": self._sender,
                "to": [{"email": recipient, "type": "to"}],
                "subject": subject,
                "text": body,
            },
            "async": False,
        }
        try:
            async with asyncio.timeout(self._timeout):
                async with session.post(
                    self._origin + "/api/1.4/messages/send",
                    json=payload,
                    allow_redirects=False,
                    timeout=aiohttp.ClientTimeout(total=None, connect=min(self._timeout, 5.0)),
                ) as response:
                    retry = _retry_after(response.headers.get("Retry-After"))
                    if retry is not None and retry > MAX_RETRY_AFTER:
                        raise EmailError("configuration", False, channel_wide=True)
                    if response.status in {401, 403}:
                        raise EmailError("unauthorized", False, channel_wide=True)
                    if response.status in {402, 429}:
                        category = "quota" if response.status == 402 else "rate_limit"
                        if response.status == 402 and retry is None:
                            retry = 3600.0
                        raise EmailError(category, True, retry_after=retry, channel_wide=True)
                    if response.status >= 500:
                        raise EmailError("server_error", True, retry_after=retry)
                    if response.status != 200:
                        raise EmailError("configuration", False, channel_wide=True)
                    result = await self._read(response)
                    self._accepted(result, recipient)
        except aiohttp.ClientSSLError:
            raise EmailError("tls", True, channel_wide=True) from None
        except TimeoutError:
            raise EmailError("timeout", True) from None
        except aiohttp.ClientError:
            raise EmailError("network", True) from None

    @staticmethod
    async def _read(response: aiohttp.ClientResponse) -> object:
        if response.headers.get("Content-Encoding", "identity").lower().strip() != "identity":
            raise EmailError("invalid_response", True)
        if response.content_length is not None and response.content_length > MAX_RESPONSE_BYTES:
            raise EmailError("invalid_response", True)
        body = bytearray()
        async for chunk in response.content.iter_chunked(4096):
            if len(body) + len(chunk) > MAX_RESPONSE_BYTES:
                raise EmailError("invalid_response", True)
            body.extend(chunk)
        try:
            return json.loads(body)
        except ValueError, RecursionError:
            raise EmailError("invalid_response", True) from None

    @staticmethod
    def _accepted(result: object, recipient: str) -> None:
        if isinstance(result, dict) and result.get("status") == "error":
            name = result.get("name")
            if name == "Invalid_Key":
                raise EmailError("unauthorized", False, channel_wide=True)
            if name == "PaymentRequired":
                raise EmailError("quota", True, retry_after=3600.0, channel_wide=True)
            if name == "ValidationError":
                raise EmailError("configuration", False, channel_wide=True)
            raise EmailError("invalid_response", True)
        if not isinstance(result, list) or len(result) != 1 or not isinstance(result[0], dict):
            raise EmailError("invalid_response", True)
        row = result[0]
        returned = row.get("email")
        if not isinstance(returned, str) or returned.lower() != recipient.lower():
            raise EmailError("invalid_response", True)
        status = row.get("status")
        if status == "invalid":
            raise EmailError("invalid_recipient", False)
        if status == "rejected":
            reason = row.get("reject_reason")
            if isinstance(reason, str) and reason in {"unsigned", "invalid-sender"}:
                raise EmailError("configuration", False, channel_wide=True)
            raise EmailError("rejected", False)
        identifier = row.get("_id")
        if (
            not isinstance(status, str)
            or status not in {"sent", "queued", "scheduled"}
            or not isinstance(identifier, str)
            or not 1 <= len(identifier) <= 256
            or any(ord(c) < 32 or ord(c) == 127 for c in identifier)
        ):
            raise EmailError("invalid_response", True)


class LocalSMTPEmailAPI:
    """Offline sink only: synthetic addresses, bounded async SMTP, no credentials or relay."""

    def __init__(
        self,
        from_address: str,
        *,
        host: str = "mailpit",
        port: int = 1025,
        request_timeout: float = 10.0,
    ) -> None:
        self._sender = _mailbox(from_address)
        if self._sender.rsplit("@", 1)[1] != "example.test":
            raise ValueError("Local email requires synthetic example.test addresses.")
        if host not in {"mailpit", "localhost", "127.0.0.1", "::1"}:
            raise ValueError("Local email host must be Mailpit or loopback.")
        if type(port) is not int or not 1 <= port <= 65535:
            raise ValueError("Local email port is outside the supported range.")
        if host == "mailpit" and port != 1025:
            raise ValueError("Local Mailpit email uses port 1025.")
        self._host = host
        self._port = port
        self._timeout = _timeout(request_timeout)
        self._open = False
        self._resolver: AsyncResolver | None = None

    async def __aenter__(self) -> LocalSMTPEmailAPI:
        if self._open:
            raise RuntimeError("Local email API is already open.")
        if self._host in {"mailpit", "localhost"}:
            self._resolver = AsyncResolver(timeout=1.0, tries=2)
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
        try:
            if self._resolver is not None:
                await self._resolver.close()
        finally:
            self._resolver = None
            self._open = False

    @staticmethod
    async def _reply(reader: asyncio.StreamReader) -> int:
        size = 0
        expected = None
        for _ in range(32):
            try:
                line = await reader.readuntil(b"\n")
            except asyncio.IncompleteReadError, asyncio.LimitOverrunError:
                raise EmailError("invalid_response", True) from None
            size += len(line)
            if (
                size > MAX_RESPONSE_BYTES
                or len(line) > 512
                or len(line) < 5
                or not line.endswith(b"\r\n")
                or not line[:3].isdigit()
                or line[3:4] not in {b" ", b"-"}
            ):
                raise EmailError("invalid_response", True)
            code = int(line[:3])
            if not 200 <= code <= 599 or (expected is not None and code != expected):
                raise EmailError("invalid_response", True)
            expected = code
            if line[3:4] == b" ":
                return code
        raise EmailError("invalid_response", True)

    @staticmethod
    def _expect(code: int, expected: int, *, recipient: bool = False) -> None:
        if code == expected:
            return
        if 400 <= code < 500:
            raise EmailError("server_error", True, channel_wide=not recipient)
        if 500 <= code < 600:
            raise EmailError(
                "invalid_recipient" if recipient else "configuration",
                False,
                channel_wide=not recipient,
            )
        raise EmailError("invalid_response", True)

    async def send(self, recipient: str, subject: str, body: str) -> None:
        recipient = _message(recipient, subject, body)
        if recipient.rsplit("@", 1)[1] != "example.test":
            raise ValueError("Local email requires synthetic example.test addresses.")
        if not self._open:
            raise RuntimeError("Local email API is not open.")
        message = EmailMessage(policy=SMTP)
        message["From"] = self._sender
        message["To"] = recipient
        message["Subject"] = subject
        message.set_content(body, charset="utf-8", cte="quoted-printable")
        encoded = message.as_bytes()
        data = b"\r\n".join(
            b"." + line if line.startswith(b".") else line for line in encoded.split(b"\r\n")
        )
        writer: asyncio.StreamWriter | None = None
        try:
            async with asyncio.timeout(self._timeout):
                host = self._host
                if self._resolver is not None:
                    addresses = await self._resolver.resolve(host, self._port)
                    if not addresses:
                        raise OSError("Local email host has no address.")
                    host = addresses[0]["host"]
                reader, writer = await asyncio.open_connection(host, self._port, limit=2048)
                self._expect(await self._reply(reader), 220)
                for command, code, is_recipient in (
                    (b"EHLO sentinel.local", 250, False),
                    (f"MAIL FROM:<{self._sender}>".encode("ascii"), 250, False),
                    (f"RCPT TO:<{recipient}>".encode("ascii"), 250, True),
                    (b"DATA", 354, False),
                ):
                    writer.write(command + b"\r\n")
                    await writer.drain()
                    self._expect(await self._reply(reader), code, recipient=is_recipient)
                writer.write(data + b".\r\n")
                await writer.drain()
                self._expect(await self._reply(reader), 250)
        except TimeoutError:
            raise EmailError("timeout", True) from None
        except OSError:
            raise EmailError("network", True) from None
        finally:
            if writer is not None:
                writer.close()
                await await_completion(asyncio.create_task(self._close_writer(writer)))

    @staticmethod
    async def _close_writer(writer: asyncio.StreamWriter) -> None:
        try:
            async with asyncio.timeout(0.5):
                await writer.wait_closed()
        except TimeoutError, OSError:
            pass
