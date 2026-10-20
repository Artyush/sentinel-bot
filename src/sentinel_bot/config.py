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
class TelegramSettings:
    """One fixed destination and its allowed command users; credentials stay private."""

    token: str = field(repr=False)
    chat_id: int = field(repr=False)
    allowed_user_ids: frozenset[int] = field(repr=False)
    topic_id: int | None = field(default=None, repr=False)

    @property
    def destination(self) -> str:
        return f"telegram:{self.chat_id}:{self.topic_id or 0}"


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


def _telegram(value: object, environ: Mapping[str, str], mode: str) -> TelegramSettings | None:
    if not isinstance(value, dict) or set(value) - {"enabled"}:
        raise ConfigError("Only the enabled flag is supported in [telegram].")
    enabled = value.get("enabled", False)
    if type(enabled) is not bool:
        raise ConfigError("telegram.enabled must be a boolean.")
    if not enabled:
        return None
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
    all_sites: tuple[SiteConfig, ...],
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
            raise ConfigError("A push-only source cannot have an outbound website check.")
        if mode == "hybrid" and alias not in enabled_sites:
            raise ConfigError("A hybrid source requires an explicitly enabled website check.")
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
        or set(document) - {"runtime", "sites", "telegram", "ingress", "sources"}
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
    sources = _sources(document.get("sources", []), all_sites, env, ingress is not None)
    if mode == "idle" and (sites or ingress is not None):
        raise ConfigError("Idle mode cannot contain enabled inputs.")
    if mode != "idle" and not sites and not sources:
        raise ConfigError("Monitoring mode requires at least one enabled source.")
    telegram = _telegram(document.get("telegram", {}), env, mode)
    if ingress is not None:
        if mode == "demo" or not sources or telegram is None:
            raise ConfigError("Ingress requires configured sources and an enabled Telegram route.")
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
    )
