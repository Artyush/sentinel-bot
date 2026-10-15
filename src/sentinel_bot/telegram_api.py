"""A bounded Telegram Bot API boundary with sanitized failure categories."""

import asyncio
import ipaddress
import json
import math
import re
from dataclasses import dataclass
from enum import StrEnum
from types import TracebackType
from typing import cast

import aiohttp
from aiohttp.resolver import AsyncResolver
from yarl import URL

MAX_RESPONSE_BYTES = 1024 * 1024
MAX_UPDATES = 20
MAX_TEXT_UNITS = 4096
MAX_RETRY_AFTER = 7 * 24 * 60 * 60
_TOKEN = re.compile(r"[1-9][0-9]{0,19}:[A-Za-z0-9_-]{20,128}\Z")
_USERNAME = re.compile(r"[A-Za-z0-9_]{1,32}\Z")


class TelegramErrorKind(StrEnum):
    TRANSIENT = "transient"
    RATE_LIMITED = "rate_limited"
    UNAUTHORIZED = "unauthorized"
    CONFLICT = "conflict"
    PERMANENT = "permanent"
    PROTOCOL = "protocol"
    WEBHOOK = "webhook"


class TelegramError(Exception):
    """A safe error; response descriptions and token-bearing URLs are discarded.

    Retry hints above the supported horizon use MAX_RETRY_AFTER + 1. Callers must
    reject that sentinel rather than retry earlier than Telegram requested.
    """

    def __init__(
        self,
        kind: TelegramErrorKind,
        *,
        retry_after: int | None = None,
        status: int | None = None,
    ) -> None:
        self.kind = kind
        self.retry_after = retry_after
        self.status = status
        super().__init__(f"Telegram request failed: {kind.value}.")

    @property
    def retryable(self) -> bool:
        return self.kind in {
            TelegramErrorKind.TRANSIENT,
            TelegramErrorKind.RATE_LIMITED,
            TelegramErrorKind.PROTOCOL,
        }


@dataclass(frozen=True)
class BotIdentity:
    id: int
    username: str


def _object(value: object) -> dict[str, object]:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise TelegramError(TelegramErrorKind.PROTOCOL)
    return cast(dict[str, object], value)


def _integer(value: object, *, minimum: int = 0) -> int:
    if type(value) is not int or not minimum <= value < 2**52:
        raise TelegramError(TelegramErrorKind.PROTOCOL)
    return value


def _fixture_origin(value: str) -> str:
    try:
        url = URL(value)
        host = url.host
        if (
            url.scheme not in {"http", "https"}
            or host is None
            or not ipaddress.ip_address(host).is_loopback
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
        raise ValueError("Telegram fixtures require a numeric loopback origin.") from None


class TelegramAPI:
    """Use a fixed verified HTTPS API, with a loopback-only test seam.

    Rate limiting, persistent retry and polling ownership belong to orchestration.
    This adapter never changes a webhook or retries an external send itself.
    """

    def __init__(
        self,
        token: str,
        *,
        fixture_base_url: str | None = None,
        request_timeout: float = 10.0,
    ) -> None:
        if not isinstance(token, str) or _TOKEN.fullmatch(token) is None:
            raise ValueError("Telegram token has an invalid format.")
        if (
            isinstance(request_timeout, bool)
            or not math.isfinite(request_timeout)
            or not 0 < request_timeout <= 60
        ):
            raise ValueError("Telegram request timeout must be between zero and 60 seconds.")
        self._token = token
        self._origin = (
            "https://api.telegram.org"
            if fixture_base_url is None
            else _fixture_origin(fixture_base_url)
        )
        self._request_timeout = request_timeout
        self._session: aiohttp.ClientSession | None = None
        self._resolver: AsyncResolver | None = None

    async def __aenter__(self) -> TelegramAPI:
        if self._session is not None:
            raise RuntimeError("Telegram API is already open.")
        self._resolver = AsyncResolver(timeout=1.0, tries=2)
        connector = aiohttp.TCPConnector(
            resolver=self._resolver,
            limit=2,
            limit_per_host=2,
            ssl=True,
        )
        self._session = aiohttp.ClientSession(
            connector=connector,
            cookie_jar=aiohttp.DummyCookieJar(),
            trust_env=False,
            auto_decompress=False,
            read_bufsize=8192,
            max_line_size=8192,
            max_field_size=8192,
            max_headers=64,
            headers={"User-Agent": "SentinelBot", "Accept-Encoding": "identity"},
        )
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        try:
            if self._session is not None:
                await self._session.close()
                self._session = None
        finally:
            if self._resolver is not None:
                await self._resolver.close()
                self._resolver = None

    async def startup(self) -> BotIdentity:
        """Validate credentials and refuse an existing webhook without altering it."""
        me = _object(await self._request("getMe", {}))
        username = me.get("username")
        if (
            me.get("is_bot") is not True
            or not isinstance(username, str)
            or _USERNAME.fullmatch(username) is None
        ):
            raise TelegramError(TelegramErrorKind.PROTOCOL)
        identity = BotIdentity(_integer(me.get("id"), minimum=1), username)
        webhook = _object(await self._request("getWebhookInfo", {}))
        url = webhook.get("url")
        if not isinstance(url, str):
            raise TelegramError(TelegramErrorKind.PROTOCOL)
        if url:
            raise TelegramError(TelegramErrorKind.WEBHOOK)
        return identity

    async def get_updates(
        self, offset: int | None, *, timeout: int = 20
    ) -> tuple[dict[str, object], ...]:
        """Read at most twenty updates; the caller durably advances the offset."""
        if type(timeout) is not int or not 0 <= timeout <= 30:
            raise ValueError("Telegram polling timeout must be between zero and 30 seconds.")
        parameters: dict[str, object] = {
            "limit": MAX_UPDATES,
            "timeout": timeout,
            "allowed_updates": ["message"],
        }
        if offset is not None:
            if type(offset) is not int or not 0 <= offset < 2**52:
                raise ValueError("Telegram polling offset must be a nonnegative integer.")
            parameters["offset"] = offset
        result = await self._request(
            "getUpdates", parameters, timeout=max(self._request_timeout, timeout + 5.0)
        )
        if not isinstance(result, list) or len(result) > MAX_UPDATES:
            raise TelegramError(TelegramErrorKind.PROTOCOL)
        updates = tuple(_object(value) for value in result)
        for update in updates:
            _integer(update.get("update_id"))
        return updates

    async def send_message(self, chat_id: int, text: str, *, topic_id: int | None = None) -> int:
        """Send plain text once and return Telegram's accepted message identifier."""
        if type(chat_id) is not int or not 0 < abs(chat_id) < 2**52:
            raise ValueError("Telegram chat must be a nonzero numeric identifier.")
        if not isinstance(text, str) or not text:
            raise ValueError("Telegram message must be nonempty text.")
        try:
            units = len(text.encode("utf-16-le")) // 2
        except UnicodeEncodeError:
            raise ValueError("Telegram message must contain valid Unicode.") from None
        if units > MAX_TEXT_UNITS:
            raise ValueError("Telegram message exceeds the text limit.")
        parameters: dict[str, object] = {
            "chat_id": chat_id,
            "text": text,
            "link_preview_options": {"is_disabled": True},
        }
        if topic_id is not None:
            if type(topic_id) is not int or not 0 < topic_id < 2**31:
                raise ValueError("Telegram topic must be a positive numeric identifier.")
            parameters["message_thread_id"] = topic_id
        message = _object(await self._request("sendMessage", parameters))
        return _integer(message.get("message_id"), minimum=1)

    async def _request(
        self, method: str, parameters: dict[str, object], *, timeout: float | None = None
    ) -> object:
        session = self._session
        if session is None:
            raise RuntimeError("Telegram API is not open.")
        deadline = self._request_timeout if timeout is None else timeout
        try:
            async with asyncio.timeout(deadline):
                async with session.post(
                    f"{self._origin}/bot{self._token}/{method}",
                    json=parameters,
                    allow_redirects=False,
                    timeout=aiohttp.ClientTimeout(total=None, connect=min(deadline, 5.0)),
                ) as response:
                    if response.status in {401, 404}:
                        raise TelegramError(TelegramErrorKind.UNAUTHORIZED, status=response.status)
                    if response.status == 409:
                        raise TelegramError(TelegramErrorKind.CONFLICT, status=409)
                    if 300 <= response.status < 400:
                        raise TelegramError(TelegramErrorKind.PROTOCOL, status=response.status)
                    if response.status >= 500:
                        raise TelegramError(TelegramErrorKind.TRANSIENT, status=response.status)
                    try:
                        envelope = await self._read_response(response)
                    except TelegramError:
                        if response.status == 429:
                            raise TelegramError(
                                TelegramErrorKind.RATE_LIMITED, status=429
                            ) from None
                        if 400 <= response.status < 500:
                            raise TelegramError(
                                TelegramErrorKind.PERMANENT, status=response.status
                            ) from None
                        raise
                    if envelope.get("ok") is False:
                        self._raise_api_error(envelope, response.status)
                    if response.status == 429:
                        raise TelegramError(TelegramErrorKind.RATE_LIMITED, status=429)
                    if 400 <= response.status < 500:
                        raise TelegramError(TelegramErrorKind.PERMANENT, status=response.status)
                    if response.status != 200 or envelope.get("ok") is not True:
                        raise TelegramError(TelegramErrorKind.PROTOCOL, status=response.status)
                    if "result" not in envelope:
                        raise TelegramError(TelegramErrorKind.PROTOCOL, status=response.status)
                    return envelope["result"]
        except TimeoutError, aiohttp.ClientError:
            raise TelegramError(TelegramErrorKind.TRANSIENT) from None

    @staticmethod
    async def _read_response(response: aiohttp.ClientResponse) -> dict[str, object]:
        if response.headers.get("Content-Encoding", "identity").lower().strip() != "identity":
            raise TelegramError(TelegramErrorKind.PROTOCOL)
        if response.content_length is not None and response.content_length > MAX_RESPONSE_BYTES:
            raise TelegramError(TelegramErrorKind.PROTOCOL)
        body = bytearray()
        async for chunk in response.content.iter_chunked(8192):
            if len(body) + len(chunk) > MAX_RESPONSE_BYTES:
                raise TelegramError(TelegramErrorKind.PROTOCOL)
            body.extend(chunk)
        try:
            return _object(json.loads(body))
        except ValueError, RecursionError:
            raise TelegramError(TelegramErrorKind.PROTOCOL) from None

    @staticmethod
    def _raise_api_error(envelope: dict[str, object], status: int) -> None:
        code = envelope.get("error_code")
        if type(code) is not int:
            raise TelegramError(TelegramErrorKind.PROTOCOL, status=status)
        if code == 429 or status == 429:
            parameters = envelope.get("parameters")
            retry_after: int | None = None
            if isinstance(parameters, dict):
                value = parameters.get("retry_after")
                if type(value) is int and value >= 0:
                    retry_after = min(value, MAX_RETRY_AFTER + 1)
            raise TelegramError(
                TelegramErrorKind.RATE_LIMITED, retry_after=retry_after, status=status
            )
        kind = {
            401: TelegramErrorKind.UNAUTHORIZED,
            404: TelegramErrorKind.UNAUTHORIZED,
            409: TelegramErrorKind.CONFLICT,
        }.get(code)
        if kind is None:
            kind = TelegramErrorKind.TRANSIENT if code >= 500 else TelegramErrorKind.PERMANENT
        raise TelegramError(kind, status=status)
