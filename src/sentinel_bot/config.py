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
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

MAX_CONFIG_BYTES = 16 * 1024


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

    @property
    def fingerprint(self) -> str:
        """Identify check and transition semantics, excluding scheduling changes."""
        value = (
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
        return hashlib.sha256(json.dumps(value, ensure_ascii=True).encode()).hexdigest()

    @property
    def freshness_limit(self) -> float:
        return 2 * self.poll_interval + self.total_timeout


@dataclass(frozen=True)
class Settings:
    state_dir: Path
    mode: str = "idle"
    heartbeat_interval: float = 1.0
    health_timeout: float = 5.0
    shutdown_timeout: float = 5.0
    sites: tuple[SiteConfig, ...] = ()
    max_concurrency: int = 5


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
    }
    if set(row) - allowed:
        raise ConfigError("Unsupported site configuration fields.")
    alias = row.get("alias")
    if not isinstance(alias, str) or not re.fullmatch(r"[A-Za-z][A-Za-z0-9_-]{0,31}", alias):
        raise ConfigError("Site alias must be a unique bounded identifier.")
    url = _url(row.get("url"), mode)
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
    )


def load_settings(path: Path, environ: Mapping[str, str] | None = None) -> Settings:
    """Read a bounded TOML file without probing sources or creating state."""
    try:
        with path.open("rb") as source:
            raw = source.read(MAX_CONFIG_BYTES + 1)
    except OSError as exc:
        raise ConfigError("Configuration file cannot be read.") from exc
    if len(raw) > MAX_CONFIG_BYTES:
        raise ConfigError("Configuration exceeds 16 KiB.")
    try:
        document = tomllib.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, tomllib.TOMLDecodeError, RecursionError) as exc:
        raise ConfigError("Configuration must be valid UTF-8 TOML.") from exc

    if (
        "runtime" not in document
        or set(document) - {"runtime", "sites"}
        or not isinstance(document["runtime"], dict)
    ):
        raise ConfigError("Only [runtime] and [[sites]] configuration sections are supported.")
    runtime: dict[str, object] = document["runtime"]
    allowed = {
        "mode",
        "state_dir",
        "heartbeat_interval_seconds",
        "health_timeout_seconds",
        "shutdown_timeout_seconds",
        "max_concurrency",
    }
    if set(runtime) - allowed:
        raise ConfigError("Unsupported fields in [runtime].")
    mode = runtime.get("mode")
    if not isinstance(mode, str) or mode not in {"idle", "monitor", "demo"}:
        raise ConfigError("runtime.mode must explicitly be idle, monitor, or demo.")
    site_rows = document.get("sites", [])
    if not isinstance(site_rows, list) or len(site_rows) > 20:
        raise ConfigError("Inventory must contain at most 20 site tables.")
    sites = tuple(_site(row, mode) for row in site_rows)
    if len({site.alias for site in sites}) != len(sites):
        raise ConfigError("Site aliases must be unique.")
    sites = tuple(site for site in sites if site.enabled)
    if mode == "idle" and sites:
        raise ConfigError("Idle mode cannot contain enabled sites.")
    if mode != "idle" and not sites:
        raise ConfigError("Monitoring mode requires at least one enabled site.")

    env = os.environ if environ is None else environ
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
        state_dir, mode, heartbeat, health_timeout, shutdown_timeout, sites, concurrency
    )
