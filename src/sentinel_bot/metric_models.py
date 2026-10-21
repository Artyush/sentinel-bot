"""Transport-independent Linux readings, CPU intervals and resource thresholds."""

import math
from dataclasses import dataclass
from datetime import datetime

from sentinel_bot.models import CheckResult, require_aware


@dataclass(frozen=True)
class IncidentRule:
    alias: str
    fingerprint: str
    poll_interval: float = 30.0
    total_timeout: float = 10.0
    failure_confirmation: int = 3
    recovery_confirmation: int = 2
    reminder_interval: float = 1800.0
    trigger: float | None = None
    recover: float | None = None
    enabled: bool = True

    @property
    def freshness_limit(self) -> float:
        return 2 * self.poll_interval + self.total_timeout


@dataclass(frozen=True)
class FilesystemReading:
    alias: str
    value: float | None
    error: str | None = None


@dataclass(frozen=True)
class MetricValues:
    cpu_percent: float | None = None
    memory_percent: float | None = None
    uptime_seconds: float | None = None
    filesystems: tuple[FilesystemReading, ...] = ()


@dataclass(frozen=True)
class ServerSnapshot:
    checked_at: datetime
    result: CheckResult
    values: MetricValues

    def __post_init__(self) -> None:
        require_aware(self.checked_at)


@dataclass(frozen=True)
class CpuCounter:
    cpu: str
    mode: str
    value: float


@dataclass(frozen=True)
class CpuBaseline:
    counters: tuple[CpuCounter, ...]
    boot_time: float
    sampled_monotonic: float


@dataclass(frozen=True)
class MetricData:
    """Only normalized required values survive the exposition adapter."""

    cpu_counters: tuple[CpuCounter, ...] | None
    boot_time: float | None
    memory_total: float | None
    memory_available: float | None
    filesystems: tuple[FilesystemReading, ...]


def classify_metric(value: float | None, trigger: float, recover: float) -> bool | None:
    """Return healthy/failing, or reset pending counts for unknown/hysteresis band."""
    if (
        not math.isfinite(trigger)
        or not math.isfinite(recover)
        or not 0 <= recover < trigger <= 100
    ):
        raise ValueError("Resource thresholds require 0 <= recover < trigger <= 100")
    if value is None or not math.isfinite(value) or not 0 <= value <= 100:
        return None
    if value >= trigger:
        return False
    if value <= recover:
        return True
    return None


def _capacity_used(available: float | None, total: float | None) -> float | None:
    if (
        available is None
        or total is None
        or not math.isfinite(available)
        or not math.isfinite(total)
        or total <= 0
        or not 0 <= available <= total
    ):
        return None
    return 100 * (1 - available / total)


def calculate_metrics(
    data: MetricData | None,
    previous: CpuBaseline | None,
    now_monotonic: float,
    freshness_limit: float,
    *,
    checked_at: datetime,
) -> tuple[MetricValues, CpuBaseline | None]:
    """Warm CPU intervals in this run; never retain a baseline across an invalid scrape."""
    require_aware(checked_at)
    if (
        not math.isfinite(now_monotonic)
        or not math.isfinite(freshness_limit)
        or freshness_limit <= 0
    ):
        raise ValueError("CPU interval clocks must be finite with a positive freshness limit")
    if data is None:
        return MetricValues(), None
    memory = _capacity_used(data.memory_available, data.memory_total)
    uptime = None
    if data.boot_time is not None and 0 < data.boot_time <= checked_at.timestamp():
        uptime = checked_at.timestamp() - data.boot_time
    baseline = None
    cpu = None
    if (
        data.cpu_counters
        and data.boot_time is not None
        and math.isfinite(data.boot_time)
        and data.boot_time > 0
    ):
        baseline = CpuBaseline(data.cpu_counters, data.boot_time, now_monotonic)
        if (
            previous is not None
            and previous.boot_time == baseline.boot_time
            and 0 < now_monotonic - previous.sampled_monotonic <= freshness_limit
            and tuple((c.cpu, c.mode) for c in previous.counters)
            == tuple((c.cpu, c.mode) for c in baseline.counters)
        ):
            deltas = tuple(
                (new.mode, new.value - old.value)
                for old, new in zip(previous.counters, baseline.counters, strict=True)
            )
            total = sum(delta for _, delta in deltas)
            idle = sum(delta for mode, delta in deltas if mode == "idle")
            if (
                all(math.isfinite(delta) and delta >= 0 for _, delta in deltas)
                and math.isfinite(total)
                and total > 0
            ):
                cpu = _capacity_used(idle, total)
    return MetricValues(cpu, memory, uptime, data.filesystems), baseline
