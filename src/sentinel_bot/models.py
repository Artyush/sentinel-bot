"""Transport-independent website observations and incident values."""

import math
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum


class SiteState(StrEnum):
    UNKNOWN = "UNKNOWN"
    UP = "UP"
    DOWN = "DOWN"


class ErrorKind(StrEnum):
    DNS = "dns"
    CONNECTION = "connection"
    TIMEOUT = "timeout"
    TLS = "tls"
    HTTP_STATUS = "http_status"
    MARKER = "marker"
    ADDRESS_POLICY = "address_policy"
    REDIRECT = "redirect"
    BODY_LIMIT = "body_limit"


@dataclass(frozen=True)
class CheckResult:
    success: bool
    error: ErrorKind | None = None
    http_status: int | None = None
    latency_ms: float | None = None

    def __post_init__(self) -> None:
        if self.success != (self.error is None):
            raise ValueError("A failed check requires one normalized error")
        if self.http_status is not None and not 100 <= self.http_status <= 599:
            raise ValueError("HTTP status must be between 100 and 599")
        if self.latency_ms is not None and (
            not math.isfinite(self.latency_ms) or self.latency_ms < 0
        ):
            raise ValueError("Latency must be finite and nonnegative")


@dataclass(frozen=True)
class Observation:
    checked_at: datetime
    result: CheckResult

    def __post_init__(self) -> None:
        require_aware(self.checked_at)


@dataclass(frozen=True)
class PolicyState:
    confirmed: SiteState = SiteState.UNKNOWN
    failures: int = 0
    successes: int = 0
    first_failure_at: datetime | None = None
    incident_id: str | None = None
    incident_started_at: datetime | None = None

    def __post_init__(self) -> None:
        if self.failures < 0 or self.successes < 0 or (self.failures and self.successes):
            raise ValueError("Confirmation counters must describe one pending series")
        if (self.failures > 0) != (self.first_failure_at is not None):
            raise ValueError("A pending failure series requires its start time")
        if self.first_failure_at is not None:
            require_aware(self.first_failure_at)
        if self.incident_started_at is not None:
            require_aware(self.incident_started_at)
        if self.confirmed is SiteState.DOWN:
            if not self.incident_id or self.incident_started_at is None:
                raise ValueError("DOWN requires an active incident identity and start time")
        elif self.incident_id is not None or self.incident_started_at is not None:
            raise ValueError("Only DOWN can retain an active incident")


class EventKind(StrEnum):
    DOWN = "DOWN"
    RECOVERY = "RECOVERY"
    REMINDER = "REMINDER"


@dataclass(frozen=True)
class Event:
    kind: EventKind
    incident_id: str
    occurred_at: datetime
    started_at: datetime

    def __post_init__(self) -> None:
        require_aware(self.occurred_at)
        require_aware(self.started_at)
        if not self.incident_id:
            raise ValueError("An incident event requires its identity")


@dataclass(frozen=True)
class Transition:
    state: PolicyState
    event: Event | None = None


def require_aware(timestamp: datetime) -> None:
    """Reject ambiguous wall-clock timestamps at the domain boundary."""
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise ValueError("Timestamp must be timezone-aware")
