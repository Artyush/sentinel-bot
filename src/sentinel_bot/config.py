"""Validate the configuration supported by the current runtime."""

from __future__ import annotations

import math
import os
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

MAX_CONFIG_BYTES = 16 * 1024


class ConfigError(ValueError):
    """An operator configuration cannot be used safely."""


@dataclass(frozen=True)
class Settings:
    state_dir: Path
    mode: str = "idle"
    heartbeat_interval: float = 1.0
    health_timeout: float = 5.0
    shutdown_timeout: float = 5.0


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

    if set(document) != {"runtime"} or not isinstance(document["runtime"], dict):
        raise ConfigError("Only the [runtime] configuration section is supported.")
    runtime: dict[str, object] = document["runtime"]
    allowed = {
        "mode",
        "state_dir",
        "heartbeat_interval_seconds",
        "health_timeout_seconds",
        "shutdown_timeout_seconds",
    }
    if set(runtime) - allowed:
        raise ConfigError("Unsupported fields in [runtime].")
    if runtime.get("mode") != "idle":
        raise ConfigError("runtime.mode must explicitly be 'idle'.")

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
    return Settings(state_dir, "idle", heartbeat, health_timeout, shutdown_timeout)
