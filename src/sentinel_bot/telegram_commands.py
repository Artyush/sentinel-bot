"""Authorized cached Telegram commands and bounded plain-text views."""

import math
import re
import unicodedata
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import cast

from sentinel_bot.config import TelegramSettings

MAX_MESSAGE_UNITS = 3500
MAX_COMMAND_CHARACTERS = 256
MAX_SITES = 20
_COMMAND = re.compile(r"/([A-Za-z][A-Za-z0-9_]{0,31})(?:@([A-Za-z0-9_]{1,32}))?\Z")
_ALIAS = re.compile(r"[A-Za-z][A-Za-z0-9_-]{0,31}\Z")
_HELP = (
    "Sentinel Bot\n"
    "/help — show commands\n"
    "/status — cached monitoring and delivery status\n"
    "/sites — configured website status\n"
    "/servers — configured servers\n"
    "/server <alias> — one configured server\n"
    "Commands read cached data and do not start checks."
)


@dataclass(frozen=True)
class Command:
    name: str
    alias: str | None = None


@dataclass(frozen=True)
class IncomingMessage:
    chat_id: int
    user_id: int | None
    topic_id: int | None
    text: str
    is_bot: bool
    sender_chat: bool


def _identifier(value: object, *, signed: bool = False) -> bool:
    return type(value) is int and 0 < (abs(value) if signed else value) < 2**63


def parse_message(update: dict[str, object]) -> IncomingMessage | None:
    """Accept ordinary user messages only; edits and anonymous posts are ignored."""
    raw = _mapping(update.get("message"))
    chat = _mapping(raw.get("chat"))
    user = _mapping(raw.get("from"))
    chat_id, user_id = chat.get("id"), user.get("id")
    topic_id, text = raw.get("message_thread_id"), raw.get("text")
    if (
        chat.get("type") not in ("private", "group", "supergroup")
        or not _identifier(chat_id, signed=True)
        or not _identifier(user_id)
        or user.get("is_bot") is not False
        or "sender_chat" in raw
        or ("message_thread_id" in raw and not _identifier(topic_id))
        or not isinstance(text, str)
        or not text
        or len(text) > 4096
        or len(text.encode("utf-16-le", errors="replace")) > 8192
    ):
        return None
    return IncomingMessage(
        cast(int, chat_id), cast(int, user_id), cast(int | None, topic_id), text, False, False
    )


@dataclass
class _Bucket:
    tokens: float
    updated_at: float

    def replenish(self, now: float, rate: float, capacity: int) -> None:
        self.tokens = min(float(capacity), self.tokens + max(0, now - self.updated_at) * rate)
        self.updated_at = max(self.updated_at, now)


class CommandGate:
    """Authorize before consuming bounded per-user and global command budgets."""

    def __init__(self, settings: TelegramSettings, bot_username: str) -> None:
        if not re.fullmatch(r"[A-Za-z0-9_]{1,32}", bot_username):
            raise ValueError("Invalid bot username")
        if not 1 <= len(settings.allowed_user_ids) <= 20:
            raise ValueError("Command allowlist must contain between 1 and 20 users")
        self.settings = settings
        self.bot_username = bot_username.casefold()
        self._users: dict[int, _Bucket] = {}
        self._global: _Bucket | None = None

    def accept(self, message: IncomingMessage, *, now: float) -> Command | None:
        if not math.isfinite(now) or now < 0:
            raise ValueError("Command time must be finite and nonnegative")
        if (
            type(message.chat_id) is not int
            or message.chat_id != self.settings.chat_id
            or type(message.user_id) is not int
            or message.user_id not in self.settings.allowed_user_ids
            or message.is_bot is not False
            or message.sender_chat is not False
            or (
                self.settings.topic_id is not None
                and (
                    type(message.topic_id) is not int or message.topic_id != self.settings.topic_id
                )
            )
        ):
            return None
        command = self._parse(message.text)
        if command is None:
            return None
        user = self._users.setdefault(message.user_id, _Bucket(2, now))
        if self._global is None:
            self._global = _Bucket(5, now)
        user.replenish(now, 6 / 60, 2)
        self._global.replenish(now, 30 / 60, 5)
        if user.tokens < 1 or self._global.tokens < 1:
            return None
        user.tokens -= 1
        self._global.tokens -= 1
        return command

    def _parse(self, text: str) -> Command | None:
        if not isinstance(text, str) or len(text) > MAX_COMMAND_CHARACTERS:
            return None
        parts = text.split()
        if not parts or not text.startswith("/"):
            return None
        match = _COMMAND.fullmatch(parts[0])
        if match is None:
            return None
        name, username = match.groups()
        if username is not None and username.casefold() != self.bot_username:
            return None
        if name == "server" and len(parts) == 2 and _ALIAS.fullmatch(parts[1]):
            return Command(name, parts[1])
        if name in ("help", "status", "sites", "servers") and len(parts) == 1:
            return Command(name)
        return Command("help")


def bound_text(text: str, limit: int = MAX_MESSAGE_UNITS) -> str:
    """Bound Telegram's UTF-16 units without splitting a supplementary character."""
    if limit < 1:
        raise ValueError("Message limit must be positive")
    cleaned = "".join(
        character
        for character in text
        if character == "\n" or not unicodedata.category(character).startswith("C")
    )
    encoded = cleaned.encode("utf-16-le")
    if len(encoded) <= limit * 2:
        return cleaned
    prefix = encoded[: (limit - 1) * 2].decode("utf-16-le", errors="ignore")
    return prefix + "…"


def _mapping(value: object) -> Mapping[str, object]:
    return cast(Mapping[str, object], value) if isinstance(value, Mapping) else {}


def _count(value: object) -> str:
    return str(value) if type(value) is int and value >= 0 else "unknown"


def _age(value: object, now: datetime) -> str:
    if not isinstance(value, str):
        return "unknown"
    try:
        checked_at = datetime.fromisoformat(value)
        if checked_at.tzinfo is None or checked_at.utcoffset() is None:
            return "unknown"
        seconds = (now - checked_at).total_seconds()
    except ValueError, OverflowError:
        return "unknown"
    if seconds < 0:
        return "clock-skew"
    if seconds < 60:
        return f"{int(seconds)}s"
    if seconds < 3600:
        return f"{int(seconds // 60)}m"
    if seconds < 86400:
        return f"{int(seconds // 3600)}h"
    return f"{int(seconds // 86400)}d"


def _site_line(site: Mapping[str, object], now: datetime) -> str:
    alias = site.get("alias")
    label = bound_text(alias, 32) if isinstance(alias, str) and alias else "unknown"
    state = site.get("state")
    if state not in ("UNKNOWN", "UP", "DOWN"):
        state = "UNKNOWN"
    freshness = "fresh" if site.get("fresh") is True else "stale"
    observation = _mapping(site.get("observation"))
    outcome = "unknown"
    if observation.get("success") is True:
        outcome = "passed"
    elif observation.get("success") is False:
        error = observation.get("error")
        safe_errors = {
            "dns",
            "connection",
            "timeout",
            "tls",
            "http_status",
            "marker",
            "address_policy",
            "redirect",
            "body_limit",
        }
        outcome = f"failed/{error}" if isinstance(error, str) and error in safe_errors else "failed"
    status = observation.get("http_status")
    if type(status) is int and 100 <= status <= 599:
        outcome += f" HTTP{status}"
    latency = observation.get("latency_ms")
    if (
        isinstance(latency, (int, float))
        and not isinstance(latency, bool)
        and 0 <= latency <= 120_000
        and math.isfinite(latency)
    ):
        outcome += f" {latency:.0f}ms"
    return (
        f"{label}: {state}; {freshness}; {outcome}; age={_age(observation.get('checked_at'), now)}"
    )


def render_command(command: Command, snapshot: Mapping[str, object], *, now: datetime) -> str:
    """Render cached status; command arguments never become targets or reply text."""
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("Rendering time must be timezone-aware")
    if command.name == "servers":
        return "No server sources configured."
    if command.name == "server":
        return "No server sources configured. Use /servers to list configured aliases."
    if command.name not in ("status", "sites"):
        return _HELP
    title = "Sentinel status" if command.name == "status" else "Configured websites"
    lines = [
        title,
        f"Snapshot time (UTC): {now.astimezone(UTC).isoformat()}",
        f"Pending notifications: {_count(snapshot.get('pending_notifications'))}",
    ]
    delivery = _mapping(snapshot.get("telegram"))
    if delivery:
        counters = "; ".join(
            f"{key}={_count(delivery.get(key))}"
            for key in ("pending", "retrying", "failed", "blocked")
        )
        lines.append(f"Telegram: {counters}")
        error = delivery.get("last_error")
        if error in (
            "interrupted",
            "destination_changed",
            "opening_failed",
            "retries_exhausted",
            "network",
            "timeout",
            "rate_limit",
            "server_error",
            "forbidden",
            "bad_request",
            "unauthorized",
            "invalid_response",
            "conflict",
            "webhook",
        ):
            lines.append(f"Telegram error: {error}")
    sites = snapshot.get("sites")
    if isinstance(sites, (list, tuple)):
        rows = [_mapping(value) for value in sites[:MAX_SITES]]
    else:
        rows = []
    if rows:
        lines.extend(_site_line(site, now.astimezone(UTC)) for site in rows)
    else:
        lines.append("No website sources configured.")
    return bound_text("\n".join(lines))
