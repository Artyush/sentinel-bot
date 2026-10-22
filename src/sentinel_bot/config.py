"""Validate the configuration supported by the current runtime."""

from __future__ import annotations

import hashlib
import ipaddress
import json
import math
import os
import re
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

from sentinel_bot.demo import (
    DEMO_TELEGRAM_CHAT_ID,
    DEMO_TELEGRAM_TOKEN,
    DEMO_TELEGRAM_USER_ID,
)
from sentinel_bot.metric_models import IncidentRule

MAX_CONFIG_BYTES = 16 * 1024
MAX_REQUEST_BODY_BYTES = 4096


class ConfigError(ValueError):
    """An operator configuration cannot be used safely."""


@dataclass(frozen=True)
class SiteConfig:
    alias: str
    url: str
    marker: str | None = None
    status_min: int = 200
    status_max: int = 399
    poll_interval: float = 30.0
    connect_timeout: float = 5.0
    total_timeout: float = 10.0
    failure_confirmation: int = 3
    recovery_confirmation: int = 2
    reminder_interval: float = 1800.0
    max_body_bytes: int = 65536
    enabled: bool = True
    method: str = "GET"
    request_body: bytes | None = field(default=None, repr=False)

    @property
    def fingerprint(self) -> str:
        """Identify check and transition semantics, excluding scheduling changes."""
        value: tuple[object, ...] = (
            self.url,
            self.marker,
            self.status_min,
            self.status_max,
            self.failure_confirmation,
            self.recovery_confirmation,
            self.max_body_bytes,
            self.connect_timeout,
            self.total_timeout,
        )
        if self.method != "GET" or self.request_body is not None:
            value += (
                self.method,
                None if self.request_body is None else self.request_body.hex(),
            )
        return hashlib.sha256(json.dumps(value, ensure_ascii=True).encode()).hexdigest()

    @property
    def freshness_limit(self) -> float:
        return 2 * self.poll_interval + self.total_timeout


@dataclass(frozen=True)
class FilesystemConfig:
    alias: str
    mount: str = field(repr=False)
    trigger: float | None = 85.0
    recover: float | None = 75.0


@dataclass(frozen=True)
class ServerConfig:
    alias: str
    url: str = field(repr=False)
    token: str = field(default="", repr=False)
    allowed_cidrs: tuple[str, ...] = ()
    poll_interval: float = 30.0
    connect_timeout: float = 5.0
    total_timeout: float = 10.0
    failure_confirmation: int = 3
    recovery_confirmation: int = 2
    reminder_interval: float = 1800.0
    cpu_trigger: float | None = 85.0
    cpu_recover: float | None = 75.0
    memory_trigger: float | None = 85.0
    memory_recover: float | None = 75.0
    mounts: tuple[FilesystemConfig, ...] = ()
    enabled: bool = True

    @property
    def freshness_limit(self) -> float:
        return 2 * self.poll_interval + self.total_timeout

    @property
    def fingerprint(self) -> str:
        return hashlib.sha256(
            json.dumps(
                (
                    self.url,
                    self.allowed_cidrs,
                    self.connect_timeout,
                    self.total_timeout,
                    [(mount.alias, mount.mount) for mount in self.mounts],
                )
            ).encode()
        ).hexdigest()

    @property
    def rules(self) -> tuple[IncidentRule, ...]:
        values: list[tuple[str, float | None, float | None, str | None]] = [
            ("exporter", None, None, None)
        ]
        if self.cpu_trigger is not None:
            values.append(("cpu", self.cpu_trigger, self.cpu_recover, None))
        if self.memory_trigger is not None:
            values.append(("memory", self.memory_trigger, self.memory_recover, None))
        values.extend(
            ("fs_" + m.alias, m.trigger, m.recover, m.mount)
            for m in self.mounts
            if m.trigger is not None
        )
        return tuple(
            IncidentRule(
                alias=f"{self.alias}/{name}",
                fingerprint=hashlib.sha256(
                    json.dumps(
                        (
                            self.url,
                            self.allowed_cidrs,
                            self.connect_timeout,
                            self.total_timeout,
                            name,
                            trigger,
                            recover,
                            mount,
                            self.failure_confirmation,
                            self.recovery_confirmation,
                        )
                    ).encode()
                ).hexdigest(),
                poll_interval=self.poll_interval,
                total_timeout=self.total_timeout,
                failure_confirmation=self.failure_confirmation,
                recovery_confirmation=self.recovery_confirmation,
                reminder_interval=self.reminder_interval,
                trigger=trigger,
                recover=recover,
                enabled=self.enabled,
            )
            for name, trigger, recover, mount in values
        )


@dataclass(frozen=True)
class TelegramSettings:
    """One fixed destination and its allowed command users; credentials stay private."""

    token: str = field(repr=False)
    chat_id: int = field(repr=False)
    allowed_user_ids: frozenset[int] = field(repr=False)
    topic_id: int | None = field(default=None, repr=False)
    provider: str = "telegram"

    @property
    def destination(self) -> str:
        channel = "telegram:demo" if self.provider == "demo" else "telegram"
        return f"{channel}:{self.chat_id}:{self.topic_id or 0}"


@dataclass(frozen=True)
class EmailSettings:
    """Fixed operator routing; persisted destination keys contain no address."""

    provider: str
    api_key: str = field(repr=False)
    from_address: str = field(repr=False)
    recipients: tuple[str, ...] = field(repr=False)
    request_timeout: float = 10.0
    local_host: str = "mailpit"
    local_port: int = 1025

    @property
    def destinations(self) -> tuple[str, ...]:
        boundary = (self.local_host, self.local_port) if self.provider == "local_smtp" else None
        return tuple(
            "email:"
            + hashlib.sha256(
                json.dumps((self.provider, self.from_address, recipient, boundary)).encode()
            ).hexdigest()
            for recipient in self.recipients
        )

    def recipient_for(self, destination: str) -> str:
        """Resolve only a configured opaque route, never an event-selected address."""
        for key, recipient in zip(self.destinations, self.recipients, strict=True):
            if key == destination:
                return recipient
        raise ValueError("Email destination is not configured.")


@dataclass(frozen=True)
class PushSource:
    """A configured sender; its credential authorizes no outbound operation."""

    alias: str
    allowed_types: frozenset[str]
    token: str = field(repr=False)
    mode: str = "push"


@dataclass(frozen=True)
class IngressSettings:
    host: str = "127.0.0.1"
    port: int = 8080
    max_body_bytes: int = 16384
    max_summary_chars: int = 1000
    body_timeout: float = 5.0
    max_concurrency: int = 10
    source_rate_per_minute: int = 60
    source_burst: int = 10
    global_rate_per_minute: int = 300
    global_burst: int = 50
    max_event_age: float = 86400.0
    future_skew: float = 300.0
    retention: float = 172800.0
    max_receipts: int = 10000
    max_receipts_per_source: int = 1000
    max_tombstones: int = 10000
    max_tombstones_per_source: int = 1000
    max_pending: int = 1000
    max_pending_per_source: int = 200
    max_active_incidents: int = 200
    max_active_incidents_per_source: int = 50


@dataclass(frozen=True)
class Settings:
    state_dir: Path
    mode: str = "idle"
    heartbeat_interval: float = 1.0
    health_timeout: float = 5.0
    shutdown_timeout: float = 5.0
    sites: tuple[SiteConfig, ...] = ()
    max_concurrency: int = 5
    telegram: TelegramSettings | None = field(default=None, repr=False)
    ingress: IngressSettings | None = None
    sources: tuple[PushSource, ...] = ()
    email: EmailSettings | None = field(default=None, repr=False)
    servers: tuple[ServerConfig, ...] = ()

    @property
    def polled_sources(self) -> tuple[SiteConfig | ServerConfig, ...]:
        return (*self.sites, *self.servers)

    @property
    def polled_rules(self) -> tuple[SiteConfig | IncidentRule, ...]:
        return (*self.sites, *(rule for server in self.servers for rule in server.rules))


def _duration(value: object, field: str, lower: float, upper: float) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ConfigError(f"runtime.{field} must be a number.")
    try:
        number = float(value)
    except OverflowError as exc:
        raise ConfigError(f"runtime.{field} is outside the supported range.") from exc
    if not math.isfinite(number) or not lower <= number <= upper:
        raise ConfigError(f"runtime.{field} is outside the supported range.")
    return number


def _integer(value: object, field: str, lower: int, upper: int) -> int:
    if type(value) is not int or not lower <= value <= upper:
        raise ConfigError(f"{field} is outside the supported integer range.")
    return value


def _url(value: object, mode: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) > 2048
        or any(ord(c) <= 32 or ord(c) == 127 for c in value)
    ):
        raise ConfigError("Site URL must be a bounded HTTP(S) URL.")
    if "?" in value or "#" in value or "\\" in value:
        raise ConfigError("Site URL cannot contain a query, fragment, or backslash.")
    try:
        parsed = urlsplit(value)
        host = parsed.hostname
        port = parsed.port
        if parsed.scheme not in {"http", "https"} or host is None or parsed.username is not None:
            raise ValueError
        host = host.encode("idna").decode("ascii").lower().rstrip(".")
        if not host or parsed.password is not None:
            raise ValueError
        try:
            address = ipaddress.ip_address(host)
        except ValueError:
            address = None
        if address is not None:
            if not address.is_global or address.is_multicast or address.is_reserved:
                raise ValueError
        elif not re.fullmatch(r"[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?", host):
            raise ValueError
        if host == "localhost" or host.endswith(".localhost"):
            raise ValueError
        if address is None and (
            host.isdigit()
            or all(c in "0123456789." for c in host)
            or re.fullmatch(r"(?:0x[0-9a-f]+|[0-9]+)(?:\.(?:0x[0-9a-f]+|[0-9]+))*", host)
        ):
            raise ValueError
        effective_port = port if port is not None else (443 if parsed.scheme == "https" else 80)
        if mode == "demo":
            if host != "site-fixture" or parsed.scheme != "http" or effective_port != 8080:
                raise ValueError
        elif effective_port not in {80, 443}:
            raise ValueError
        authority = f"[{host}]" if ":" in host else host
        if port is not None:
            authority += f":{port}"
        return urlunsplit((parsed.scheme, authority, parsed.path or "/", "", ""))
    except (ValueError, UnicodeError) as exc:
        raise ConfigError("Site URL violates the supported address/port policy.") from exc


def _site(value: object, mode: str) -> SiteConfig:
    if not isinstance(value, dict):
        raise ConfigError("Each site must be a TOML table.")
    row: dict[str, object] = value
    allowed = {
        "alias",
        "url",
        "marker",
        "status_min",
        "status_max",
        "poll_interval_seconds",
        "connect_timeout_seconds",
        "total_timeout_seconds",
        "failure_confirmation",
        "recovery_confirmation",
        "reminder_interval_seconds",
        "max_body_bytes",
        "enabled",
        "method",
        "json_body",
    }
    if set(row) - allowed:
        raise ConfigError("Unsupported site configuration fields.")
    alias = row.get("alias")
    if not isinstance(alias, str) or not re.fullmatch(r"[A-Za-z][A-Za-z0-9_-]{0,31}", alias):
        raise ConfigError("Site alias must be a unique bounded identifier.")
    url = _url(row.get("url"), mode)
    method = row.get("method", "GET")
    if not isinstance(method, str) or method not in {"GET", "POST"}:
        raise ConfigError("Site method must be GET or POST.")
    request_body = None
    if "json_body" in row:
        if method != "POST" or not isinstance(row["json_body"], dict):
            raise ConfigError("Site json_body requires POST and a TOML table.")
        try:
            request_body = json.dumps(
                row["json_body"],
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            ).encode("utf-8")
        except (TypeError, ValueError, RecursionError) as exc:
            raise ConfigError("Site json_body must contain only supported JSON values.") from exc
        if len(request_body) > MAX_REQUEST_BODY_BYTES:
            raise ConfigError("Site json_body cannot exceed 4096 UTF-8 bytes.")
    maximum = _integer(row.get("max_body_bytes", 65536), "max_body_bytes", 1, 65536)
    marker = row.get("marker")
    if marker is not None and (
        not isinstance(marker, str) or not marker or len(marker.encode()) > min(maximum, 4096)
    ):
        raise ConfigError("Site marker must be bounded nonempty UTF-8 text.")
    minimum_status = _integer(row.get("status_min", 200), "status_min", 100, 599)
    maximum_status = _integer(row.get("status_max", 399), "status_max", minimum_status, 599)
    connect = _duration(row.get("connect_timeout_seconds", 5.0), "connect_timeout_seconds", 0.1, 30)
    total = _duration(row.get("total_timeout_seconds", 10.0), "total_timeout_seconds", 0.2, 120)
    if connect > total:
        raise ConfigError("Connect timeout cannot exceed the total request deadline.")
    enabled = row.get("enabled", True)
    if type(enabled) is not bool:
        raise ConfigError("Site enabled must be a boolean.")
    return SiteConfig(
        alias=alias,
        url=url,
        marker=marker,
        status_min=minimum_status,
        status_max=maximum_status,
        poll_interval=_duration(
            row.get("poll_interval_seconds", 30.0), "poll_interval_seconds", 0.1, 86400
        ),
        connect_timeout=connect,
        total_timeout=total,
        failure_confirmation=_integer(
            row.get("failure_confirmation", 3), "failure_confirmation", 1, 100
        ),
        recovery_confirmation=_integer(
            row.get("recovery_confirmation", 2), "recovery_confirmation", 1, 100
        ),
        reminder_interval=_duration(
            row.get("reminder_interval_seconds", 1800), "reminder_interval_seconds", 1, 604800
        ),
        max_body_bytes=maximum,
        enabled=enabled,
        method=method,
        request_body=request_body,
    )


def _server_thresholds(
    row: dict[str, object], prefix: str = ""
) -> tuple[float | None, float | None]:
    enabled = row.get(prefix + "alerts", True)
    if type(enabled) is not bool:
        raise ConfigError("Server alert flags must be booleans.")
    trigger = _duration(row.get(prefix + "trigger_percent", 85), "trigger_percent", 0, 100)
    recover = _duration(row.get(prefix + "recovery_percent", 75), "recovery_percent", 0, 100)
    if recover >= trigger:
        raise ConfigError("Resource recovery threshold must be lower than its trigger.")
    return (trigger, recover) if enabled else (None, None)


def _server(value: object, mode: str, environ: Mapping[str, str]) -> ServerConfig:
    allowed = {
        "alias",
        "url",
        "token_env",
        "allowed_cidrs",
        "poll_interval_seconds",
        "connect_timeout_seconds",
        "total_timeout_seconds",
        "failure_confirmation",
        "recovery_confirmation",
        "reminder_interval_seconds",
        "enabled",
        "cpu_alerts",
        "cpu_trigger_percent",
        "cpu_recovery_percent",
        "memory_alerts",
        "memory_trigger_percent",
        "memory_recovery_percent",
        "filesystems",
    }
    if not isinstance(value, dict) or set(value) - allowed:
        raise ConfigError("Unsupported server configuration fields.")
    row: dict[str, object] = value
    alias = row.get("alias")
    if not isinstance(alias, str) or not re.fullmatch(r"[A-Za-z][A-Za-z0-9_-]{0,31}", alias):
        raise ConfigError("Server alias must be a bounded identifier.")
    enabled = row.get("enabled", True)
    if type(enabled) is not bool:
        raise ConfigError("Server enabled must be a boolean.")
    cidrs = row.get("allowed_cidrs", [])
    if not isinstance(cidrs, list) or len(cidrs) > 16 or any(not isinstance(x, str) for x in cidrs):
        raise ConfigError("Server allowed_cidrs must be a bounded list of networks.")
    networks = []
    for item in cidrs:
        try:
            if "%" in item:
                raise ValueError
            network = ipaddress.ip_network(item, strict=True)
            for address in (network.network_address, network.broadcast_address):
                if (
                    address.is_loopback
                    or address.is_link_local
                    or address.is_multicast
                    or address.is_unspecified
                    or address.is_reserved
                    or (
                        isinstance(address, ipaddress.IPv6Address)
                        and (address.ipv4_mapped is not None or address.scope_id is not None)
                    )
                ):
                    raise ValueError
        except ValueError as exc:
            raise ConfigError("Server address allowlist contains an unsupported network.") from exc
        networks.append(str(network))
    if len(set(networks)) != len(networks):
        raise ConfigError("Server address networks must be unique.")
    url = row.get("url")
    if (
        not isinstance(url, str)
        or len(url) > 2048
        or not url
        or any(ord(c) <= 32 or ord(c) == 127 for c in url)
        or any(c in url for c in "?#\\")
    ):
        raise ConfigError("Server URL must be a bounded fixed-origin HTTPS URL.")
    try:
        parsed = urlsplit(url)
        host = parsed.hostname
        port = parsed.port
        if host is None or parsed.username is not None or parsed.password is not None:
            raise ValueError
        host = host.encode("idna").decode("ascii").lower().rstrip(".")
        port = port if port is not None else (443 if parsed.scheme == "https" else 80)
        if not 1 <= port <= 65535:
            raise ValueError
        if mode == "demo":
            if parsed.scheme != "http" or host != "metrics-fixture" or port not in {9100, 9101}:
                raise ValueError
        else:
            if parsed.scheme != "https" or host == "localhost" or host.endswith(".localhost"):
                raise ValueError
            try:
                address = ipaddress.ip_address(host)
            except ValueError:
                if not re.fullmatch(r"[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?", host) or re.fullmatch(
                    r"(?:0x[0-9a-f]+|[0-9]+)(?:\.(?:0x[0-9a-f]+|[0-9]+))*", host
                ):
                    raise ValueError from None
            else:
                if (
                    address.is_loopback
                    or address.is_link_local
                    or address.is_multicast
                    or address.is_unspecified
                    or address.is_reserved
                    or (
                        isinstance(address, ipaddress.IPv6Address)
                        and (address.ipv4_mapped is not None or address.scope_id is not None)
                    )
                ):
                    raise ValueError
                if not address.is_global and not any(
                    address in ipaddress.ip_network(n) for n in networks
                ):
                    raise ValueError
        authority = f"[{host}]" if ":" in host else host
        if parsed.port is not None:
            authority += f":{port}"
        url = urlunsplit((parsed.scheme, authority, parsed.path or "/", "", ""))
    except (ValueError, UnicodeError) as exc:
        raise ConfigError("Server URL violates the HTTPS/address policy.") from exc
    token_name = row.get("token_env")
    if token_name is not None and (
        not isinstance(token_name, str) or not re.fullmatch(r"[A-Z][A-Z0-9_]{0,127}", token_name)
    ):
        raise ConfigError("Server token_env must name an environment variable.")
    token = ""
    if mode != "demo":
        if token_name is None:
            raise ConfigError("Server HTTPS polling requires a token environment reference.")
        if enabled:
            assert isinstance(token_name, str)
            token = environ.get(token_name, "")
            if not re.fullmatch(r"[A-Za-z0-9_-]{32,128}", token):
                raise ConfigError("Enabled server requires a valid token environment value.")
    mounts_raw = row.get("filesystems", [])
    if not isinstance(mounts_raw, list) or len(mounts_raw) > 4:
        raise ConfigError("A server supports at most four configured filesystems.")
    mounts = []
    for item in mounts_raw:
        if not isinstance(item, dict) or set(item) - {
            "alias",
            "mount",
            "trigger_percent",
            "recovery_percent",
            "alerts",
        }:
            raise ConfigError("Unsupported filesystem fields.")
        name = item.get("alias")
        mount = item.get("mount")
        if not isinstance(name, str) or not re.fullmatch(r"[A-Za-z][A-Za-z0-9_-]{0,31}", name):
            raise ConfigError("Filesystem alias must be a bounded identifier.")
        if (
            not isinstance(mount, str)
            or not mount.startswith("/")
            or len(mount) > 256
            or any(ord(c) < 32 or ord(c) == 127 for c in mount)
        ):
            raise ConfigError("Filesystem mount must be a bounded absolute path.")
        trigger, recover = _server_thresholds(item)
        mounts.append(FilesystemConfig(name, mount, trigger, recover))
    if len({m.alias for m in mounts}) != len(mounts) or len({m.mount for m in mounts}) != len(
        mounts
    ):
        raise ConfigError("Filesystem aliases and mount paths must be unique per server.")
    connect = _duration(row.get("connect_timeout_seconds", 5), "connect_timeout_seconds", 0.1, 30)
    total = _duration(row.get("total_timeout_seconds", 10), "total_timeout_seconds", 0.2, 120)
    if connect > total:
        raise ConfigError("Connect timeout cannot exceed the total deadline.")
    cpu = _server_thresholds(row, "cpu_")
    memory = _server_thresholds(row, "memory_")
    return ServerConfig(
        alias,
        url,
        token,
        tuple(networks),
        _duration(row.get("poll_interval_seconds", 30), "poll_interval_seconds", 0.1, 86400),
        connect,
        total,
        _integer(row.get("failure_confirmation", 3), "failure_confirmation", 1, 100),
        _integer(row.get("recovery_confirmation", 2), "recovery_confirmation", 1, 100),
        _duration(
            row.get("reminder_interval_seconds", 1800), "reminder_interval_seconds", 1, 604800
        ),
        *cpu,
        *memory,
        tuple(mounts),
        enabled,
    )


def _telegram(value: object, environ: Mapping[str, str], mode: str) -> TelegramSettings | None:
    if not isinstance(value, dict) or set(value) - {"enabled", "provider"}:
        raise ConfigError("Unsupported fields in [telegram].")
    provider = value.get("provider", "telegram")
    if not isinstance(provider, str) or provider not in {"telegram", "demo"}:
        raise ConfigError("Telegram provider must be telegram or demo.")
    if provider == "demo" and mode != "demo":
        raise ConfigError("The synthetic Telegram provider requires demo mode.")
    enabled = value.get("enabled", False)
    if type(enabled) is not bool:
        raise ConfigError("telegram.enabled must be a boolean.")
    if not enabled:
        return None
    if provider == "demo":
        return TelegramSettings(
            DEMO_TELEGRAM_TOKEN,
            DEMO_TELEGRAM_CHAT_ID,
            frozenset({DEMO_TELEGRAM_USER_ID}),
            provider="demo",
        )
    if mode == "demo":
        raise ConfigError("The isolated website demo cannot enable live Telegram.")
    token = environ.get("SENTINEL_TELEGRAM_BOT_TOKEN", "")
    if not re.fullmatch(r"[1-9][0-9]{0,19}:[A-Za-z0-9_-]{20,128}", token):
        raise ConfigError("Telegram requires a valid bot token environment variable.")

    def identifier(name: str, *, negative: bool = False) -> int:
        raw = environ.get(name, "")
        pattern = r"-?[1-9][0-9]{0,15}" if negative else r"[1-9][0-9]{0,15}"
        if not re.fullmatch(pattern, raw):
            raise ConfigError(
                "Telegram destination and user IDs must be numeric environment values."
            )
        number = int(raw)
        if abs(number) >= 2**52:
            raise ConfigError("Telegram identifier is outside the supported range.")
        return number

    chat_id = identifier("SENTINEL_TELEGRAM_CHAT_ID", negative=True)
    raw_users = environ.get("SENTINEL_TELEGRAM_ALLOWED_USER_IDS", "")
    if not raw_users or len(raw_users) > 400:
        raise ConfigError("Telegram requires an explicit allowlist of at most 20 users.")
    parts = raw_users.split(",")
    if len(parts) > 20 or any(
        not re.fullmatch(r"[1-9][0-9]{0,15}", part.strip()) for part in parts
    ):
        raise ConfigError("Telegram requires an explicit numeric user allowlist.")
    users = frozenset(int(part.strip()) for part in parts)
    if len(users) != len(parts) or any(user >= 2**52 for user in users):
        raise ConfigError("Telegram user identifiers must be unique and bounded.")
    topic_id = None
    if environ.get("SENTINEL_TELEGRAM_TOPIC_ID", ""):
        topic_id = identifier("SENTINEL_TELEGRAM_TOPIC_ID")
        if topic_id > 2**31 - 1 or chat_id > 0:
            raise ConfigError("Telegram topics require a group chat and a bounded topic ID.")
    return TelegramSettings(token, chat_id, users, topic_id)


def _email_address(value: str) -> str:
    if (
        len(value) > 254
        or re.fullmatch(
            r"[A-Za-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[A-Za-z0-9!#$%&'*+/=?^_`{|}~-]+)*"
            r"@(?:[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?\.)+"
            r"[A-Za-z]{2,63}",
            value,
        )
        is None
    ):
        raise ConfigError("Email addresses must be bounded plain ASCII mailboxes.")
    local, domain = value.rsplit("@", 1)
    if len(local) > 64:
        raise ConfigError("Email addresses must be bounded plain ASCII mailboxes.")
    return local + "@" + domain.lower()


def _email(value: object, environ: Mapping[str, str], mode: str) -> EmailSettings | None:
    allowed = {"enabled", "provider", "request_timeout_seconds", "local_host", "local_port"}
    if not isinstance(value, dict) or set(value) - allowed:
        raise ConfigError("Unsupported fields in [email].")
    enabled = value.get("enabled", False)
    if type(enabled) is not bool:
        raise ConfigError("email.enabled must be a boolean.")
    if not enabled:
        return None
    provider = value.get("provider", "mailchimp")
    if not isinstance(provider, str) or provider not in {"mailchimp", "local_smtp"}:
        raise ConfigError("Email provider must be mailchimp or local_smtp.")
    if mode == "demo" and provider != "local_smtp":
        raise ConfigError("The isolated demo can enable only synthetic local email.")
    timeout = _duration(
        value.get("request_timeout_seconds", 10.0), "email.request_timeout_seconds", 0.1, 10.0
    )
    host = value.get("local_host", "mailpit")
    port = _integer(value.get("local_port", 1025), "email.local_port", 1025, 1025)
    if not isinstance(host, str) or host not in {"mailpit", "localhost", "127.0.0.1", "::1"}:
        raise ConfigError("Local email host must be Mailpit or loopback.")
    if provider == "mailchimp" and {"local_host", "local_port"} & set(value):
        raise ConfigError("Local SMTP settings require the local_smtp email provider.")
    api_key = ""
    if provider == "mailchimp":
        api_key = environ.get("SENTINEL_EMAIL_API_KEY", "")
        if re.fullmatch(r"[A-Za-z0-9_-]{16,128}", api_key) is None:
            raise ConfigError("Mailchimp email requires a valid API key environment value.")
    sender = _email_address(environ.get("SENTINEL_EMAIL_FROM", ""))
    raw_recipients = environ.get("SENTINEL_EMAIL_RECIPIENTS", "")
    if (
        not raw_recipients
        or len(raw_recipients) > 1279
        or any(ord(character) < 32 or ord(character) == 127 for character in raw_recipients)
    ):
        raise ConfigError("Email requires between one and five configured recipients.")
    parts = raw_recipients.split(",")
    if not 1 <= len(parts) <= 5:
        raise ConfigError("Email requires between one and five configured recipients.")
    recipients = tuple(_email_address(part.strip()) for part in parts)
    if len({recipient.lower() for recipient in recipients}) != len(recipients):
        raise ConfigError("Email recipients must be unique.")
    if provider == "local_smtp" and any(
        address.rsplit("@", 1)[1] != "example.test" for address in (sender, *recipients)
    ):
        raise ConfigError("Local email requires synthetic example.test addresses.")
    return EmailSettings(provider, api_key, sender, recipients, timeout, host, port)


def _ingress(value: object) -> IngressSettings | None:
    if not isinstance(value, dict):
        raise ConfigError("Ingress configuration must be a TOML table.")
    integer_limits = {
        "port": (8080, 1024, 65535),
        "max_body_bytes": (16384, 256, 16384),
        "max_summary_chars": (1000, 1, 1000),
        "max_concurrency": (10, 1, 10),
        "source_rate_per_minute": (60, 1, 600),
        "source_burst": (10, 1, 100),
        "global_rate_per_minute": (300, 1, 3000),
        "global_burst": (50, 1, 500),
        "max_receipts": (10000, 1, 10000),
        "max_receipts_per_source": (1000, 1, 1000),
        "max_tombstones": (10000, 1, 10000),
        "max_tombstones_per_source": (1000, 1, 1000),
        "max_pending": (1000, 1, 1000),
        "max_pending_per_source": (200, 1, 200),
        "max_active_incidents": (200, 1, 200),
        "max_active_incidents_per_source": (50, 1, 50),
    }
    duration_limits = {
        "body_timeout": (5.0, 0.1, 30.0),
        "max_event_age": (86400.0, 1.0, 604800.0),
        "future_skew": (300.0, 0.0, 3600.0),
        "retention": (172800.0, 172800.0, 1209600.0),
    }
    allowed = {"enabled", "host", *integer_limits}
    allowed.update(name + "_seconds" for name in duration_limits)
    if set(value) - allowed:
        raise ConfigError("Unsupported fields in [ingress].")
    enabled = value.get("enabled", False)
    if type(enabled) is not bool:
        raise ConfigError("ingress.enabled must be a boolean.")
    host = value.get("host", "127.0.0.1")
    if not isinstance(host, str) or host not in {"127.0.0.1", "0.0.0.0", "::1", "::"}:
        raise ConfigError("Ingress host must be a loopback or wildcard IP literal.")
    integers = {
        name: _integer(value.get(name, default), "ingress." + name, lower, upper)
        for name, (default, lower, upper) in integer_limits.items()
    }
    durations = {
        name: _duration(value.get(name + "_seconds", default), name + "_seconds", lower, upper)
        for name, (default, lower, upper) in duration_limits.items()
    }
    if durations["retention"] < durations["max_event_age"] + durations["future_skew"]:
        raise ConfigError("Ingress retention must cover the event age and future clock skew.")
    for limit in ("max_receipts", "max_tombstones", "max_pending", "max_active_incidents"):
        if integers[limit + "_per_source"] > integers[limit]:
            raise ConfigError("Per-source ingress capacity cannot exceed global capacity.")
    if (
        integers["source_rate_per_minute"] > integers["global_rate_per_minute"]
        or integers["source_burst"] > integers["global_burst"]
    ):
        raise ConfigError("Per-source ingress rate cannot exceed the global rate.")
    settings = IngressSettings(
        host=host,
        port=integers["port"],
        max_body_bytes=integers["max_body_bytes"],
        max_summary_chars=integers["max_summary_chars"],
        body_timeout=durations["body_timeout"],
        max_concurrency=integers["max_concurrency"],
        source_rate_per_minute=integers["source_rate_per_minute"],
        source_burst=integers["source_burst"],
        global_rate_per_minute=integers["global_rate_per_minute"],
        global_burst=integers["global_burst"],
        max_event_age=durations["max_event_age"],
        future_skew=durations["future_skew"],
        retention=durations["retention"],
        max_receipts=integers["max_receipts"],
        max_receipts_per_source=integers["max_receipts_per_source"],
        max_tombstones=integers["max_tombstones"],
        max_tombstones_per_source=integers["max_tombstones_per_source"],
        max_pending=integers["max_pending"],
        max_pending_per_source=integers["max_pending_per_source"],
        max_active_incidents=integers["max_active_incidents"],
        max_active_incidents_per_source=integers["max_active_incidents_per_source"],
    )
    return settings if enabled else None


def _sources(
    value: object,
    all_sites: tuple[SiteConfig | ServerConfig, ...],
    environ: Mapping[str, str],
    enabled: bool,
) -> tuple[PushSource, ...]:
    if not isinstance(value, list) or len(value) > 20:
        raise ConfigError("Inventory must contain at most 20 source tables.")
    site_aliases = {site.alias for site in all_sites}
    enabled_sites = {site.alias for site in all_sites if site.enabled}
    aliases: set[str] = set()
    token_names: set[str] = set()
    tokens: set[str] = set()
    sources: list[PushSource] = []
    for row in value:
        if not isinstance(row, dict) or set(row) - {
            "alias",
            "mode",
            "allowed_types",
            "token_env",
            "enabled",
        }:
            raise ConfigError("Unsupported source configuration fields.")
        alias = row.get("alias")
        if not isinstance(alias, str) or not re.fullmatch(r"[A-Za-z][A-Za-z0-9_-]{0,31}", alias):
            raise ConfigError("Source alias must be a unique bounded identifier.")
        if alias in aliases:
            raise ConfigError("Source aliases must be unique.")
        aliases.add(alias)
        mode = row.get("mode", "push")
        if not isinstance(mode, str) or mode not in {"push", "hybrid"}:
            raise ConfigError("Source mode must be push or hybrid.")
        if mode == "push" and alias in site_aliases:
            raise ConfigError("A push-only source cannot have an outbound polling target.")
        if mode == "hybrid" and alias not in enabled_sites:
            raise ConfigError("A hybrid source requires an explicitly enabled polling source.")
        types = row.get("allowed_types")
        if (
            not isinstance(types, list)
            or not 1 <= len(types) <= 20
            or any(
                not isinstance(item, str) or not re.fullmatch(r"[a-z][a-z0-9_.-]{0,63}", item)
                for item in types
            )
            or len(set(types)) != len(types)
        ):
            raise ConfigError("Source types must be unique bounded technical identifiers.")
        token_name = row.get("token_env")
        if (
            not isinstance(token_name, str)
            or not re.fullmatch(r"[A-Z][A-Z0-9_]{0,127}", token_name)
            or token_name in token_names
        ):
            raise ConfigError("Each source requires a distinct token environment reference.")
        token_names.add(token_name)
        source_enabled = row.get("enabled", True)
        if type(source_enabled) is not bool:
            raise ConfigError("Source enabled must be a boolean.")
        if not enabled or not source_enabled:
            continue
        token = environ.get(token_name, "")
        if not re.fullmatch(r"[A-Za-z0-9_-]{32,128}", token) or token in tokens:
            raise ConfigError("Enabled sources require distinct valid token environment values.")
        tokens.add(token)
        sources.append(PushSource(alias, frozenset(types), token, mode))
    if len(site_aliases | aliases) > 20:
        raise ConfigError("Inventory must contain at most 20 distinct sources, including disabled.")
    return tuple(sources)


def _read_document(path: Path) -> dict[str, object]:
    try:
        with path.open("rb") as source:
            raw = source.read(MAX_CONFIG_BYTES + 1)
    except OSError as exc:
        raise ConfigError("Configuration file cannot be read.") from exc
    if len(raw) > MAX_CONFIG_BYTES:
        raise ConfigError("Configuration exceeds 16 KiB.")
    try:
        return tomllib.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, tomllib.TOMLDecodeError, RecursionError) as exc:
        raise ConfigError("Configuration must be valid UTF-8 TOML.") from exc


def _site_rows(
    document: dict[str, object], runtime: dict[str, object], config_path: Path
) -> object:
    if "sites_file" not in runtime:
        return document.get("sites", [])
    if "sites" in document:
        raise ConfigError("Inline sites cannot be combined with runtime.sites_file.")
    value = runtime["sites_file"]
    if (
        not isinstance(value, str)
        or not value.strip()
        or len(value) > 2048
        or "\\" in value
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
    ):
        raise ConfigError("runtime.sites_file must be a bounded relative path.")
    relative = Path(value)
    if relative.is_absolute() or not relative.parts or ".." in relative.parts:
        raise ConfigError("runtime.sites_file must stay within its configuration directory.")
    try:
        directory = config_path.parent.resolve()
        inventory = (directory / relative).resolve()
    except (OSError, RuntimeError, ValueError) as exc:
        raise ConfigError("Site inventory path cannot be resolved.") from exc
    if not inventory.is_relative_to(directory):
        raise ConfigError("runtime.sites_file must stay within its configuration directory.")
    external = _read_document(inventory)
    if set(external) != {"sites"}:
        raise ConfigError("Site inventory must contain only [[sites]] tables.")
    return external["sites"]


def load_settings(path: Path, environ: Mapping[str, str] | None = None) -> Settings:
    """Read bounded TOML files without probing sources or creating state."""
    document = _read_document(path)
    if (
        "runtime" not in document
        or set(document)
        - {"runtime", "sites", "telegram", "ingress", "sources", "email", "servers"}
        or not isinstance(document["runtime"], dict)
    ):
        raise ConfigError("Unsupported configuration sections or missing [runtime].")
    runtime: dict[str, object] = document["runtime"]
    allowed = {
        "mode",
        "state_dir",
        "heartbeat_interval_seconds",
        "health_timeout_seconds",
        "shutdown_timeout_seconds",
        "max_concurrency",
        "sites_file",
    }
    if set(runtime) - allowed:
        raise ConfigError("Unsupported fields in [runtime].")
    mode = runtime.get("mode")
    if not isinstance(mode, str) or mode not in {"idle", "monitor", "demo"}:
        raise ConfigError("runtime.mode must explicitly be idle, monitor, or demo.")
    site_rows = _site_rows(document, runtime, path)
    if not isinstance(site_rows, list) or len(site_rows) > 20:
        raise ConfigError("Inventory must contain at most 20 site tables.")
    sites = tuple(_site(row, mode) for row in site_rows)
    if len({site.alias for site in sites}) != len(sites):
        raise ConfigError("Site aliases must be unique.")
    all_sites = sites
    sites = tuple(site for site in all_sites if site.enabled)
    env = os.environ if environ is None else environ
    ingress = _ingress(document.get("ingress", {}))
    server_rows = document.get("servers", [])
    if not isinstance(server_rows, list) or len(server_rows) > 20:
        raise ConfigError("Inventory supports at most 20 server tables.")
    all_servers = tuple(_server(row, mode, env) for row in server_rows)
    if len({server.alias for server in all_servers}) != len(all_servers):
        raise ConfigError("Server aliases must be unique.")
    if {server.alias for server in all_servers} & {site.alias for site in all_sites}:
        raise ConfigError("Website and server aliases must be distinct.")
    servers = tuple(server for server in all_servers if server.enabled)
    if len(sites) + sum(len(server.rules) for server in servers) > 40:
        raise ConfigError("Expanded polling inventory supports at most 40 incident rules.")
    sources = _sources(
        document.get("sources", []), (*all_sites, *all_servers), env, ingress is not None
    )
    if mode == "idle" and (sites or servers or ingress is not None):
        raise ConfigError("Idle mode cannot contain enabled inputs.")
    if mode != "idle" and not sites and not sources and not servers:
        raise ConfigError("Monitoring mode requires at least one enabled source.")
    telegram = _telegram(document.get("telegram", {}), env, mode)
    email = _email(document.get("email", {}), env, mode)
    if ingress is not None:
        if not sources or (telegram is None and email is None):
            raise ConfigError("Ingress requires configured sources and an enabled delivery route.")
        if (
            ingress.max_pending < 2 * ingress.max_active_incidents
            or ingress.max_pending_per_source < 2 * ingress.max_active_incidents_per_source
        ):
            raise ConfigError("Ingress delivery capacity must reserve incident lifecycle slots.")
    state_value = env.get("SENTINEL_STATE_DIR", runtime.get("state_dir", "data"))
    if (
        not isinstance(state_value, str)
        or not state_value.strip()
        or len(state_value) > 2048
        or "\x00" in state_value
    ):
        raise ConfigError("State directory must be a nonempty path.")
    state_dir = Path(state_value)
    try:
        if state_dir.exists() and not state_dir.is_dir():
            raise ConfigError("State path must be a directory.")
    except OSError as exc:
        raise ConfigError("State path cannot be inspected.") from exc

    heartbeat = _duration(
        runtime.get("heartbeat_interval_seconds", 1.0),
        "heartbeat_interval_seconds",
        0.1,
        60.0,
    )
    health_timeout = _duration(
        runtime.get("health_timeout_seconds", 5.0),
        "health_timeout_seconds",
        0.2,
        300.0,
    )
    if health_timeout < 2 * heartbeat:
        raise ConfigError("Health timeout must cover at least two heartbeat intervals.")
    shutdown_timeout = _duration(
        runtime.get("shutdown_timeout_seconds", 5.0),
        "shutdown_timeout_seconds",
        0.1,
        30.0,
    )
    concurrency = _integer(runtime.get("max_concurrency", 5), "max_concurrency", 1, 5)
    return Settings(
        state_dir,
        mode,
        heartbeat,
        health_timeout,
        shutdown_timeout,
        sites,
        concurrency,
        telegram,
        ingress,
        sources,
        email,
        servers,
    )
