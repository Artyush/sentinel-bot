"""Validated sender reports and pure occurrence-based incident decisions."""

import hashlib
import json
import math
import re
import unicodedata
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID

from sentinel_bot.models import require_aware


class EventRejected(ValueError):
    """A report cannot be accepted; the code contains no sender-controlled text."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


@dataclass(frozen=True)
class IncomingEvent:
    event_id: str
    occurred_at: datetime
    kind: str
    event_type: str
    severity: str
    summary: str
    incident_key: str | None = None

    def as_dict(self) -> dict[str, object]:
        document: dict[str, object] = {
            "event_id": self.event_id,
            "occurred_at": self.occurred_at.astimezone(UTC).isoformat(),
            "kind": self.kind,
            "type": self.event_type,
            "severity": self.severity,
            "summary": self.summary,
        }
        if self.incident_key is not None:
            document["incident_key"] = self.incident_key
        return document

    @property
    def fingerprint(self) -> str:
        """Compare immutable validated content, independent of JSON representation."""
        canonical = json.dumps(
            self.as_dict(), ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        return hashlib.sha256(canonical).hexdigest()


@dataclass(frozen=True)
class IncomingReceipt:
    event_id: str
    received_at: datetime
    outcome: str

    def as_dict(self) -> dict[str, str]:
        return {
            "event_id": self.event_id,
            "received_at": self.received_at.astimezone(UTC).isoformat(),
            "outcome": self.outcome,
        }


def _object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    document: dict[str, object] = {}
    for key, value in pairs:
        if key in document:
            raise EventRejected("schema")
        document[key] = value
    return document


def _nonfinite(_: str) -> object:
    raise EventRejected("schema")


def _timestamp(value: object) -> datetime:
    if not isinstance(value, str) or not re.fullmatch(
        r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|\+00:00)", value
    ):
        raise EventRejected("schema")
    try:
        return datetime.fromisoformat(value).astimezone(UTC)
    except ValueError:
        raise EventRejected("schema") from None


def parse_event(
    raw: bytes, *, max_body_bytes: int = 16384, max_summary_chars: int = 1000
) -> IncomingEvent:
    """Parse the strict public JSON contract without retaining raw input."""
    if len(raw) > max_body_bytes:
        raise EventRejected("schema")
    try:
        document: object = json.loads(
            raw.decode("utf-8"), object_pairs_hook=_object, parse_constant=_nonfinite
        )
    except UnicodeDecodeError, json.JSONDecodeError, RecursionError, ValueError:
        raise EventRejected("schema") from None
    fields = {"event_id", "occurred_at", "kind", "type", "severity", "summary"}
    if (
        not isinstance(document, dict)
        or not fields <= document.keys()
        or document.keys() - fields - {"incident_key"}
    ):
        raise EventRejected("schema")
    identity = document["event_id"]
    if not isinstance(identity, str) or not re.fullmatch(
        r"[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}", identity
    ):
        raise EventRejected("schema")
    identity = str(UUID(identity))
    occurred_at = _timestamp(document["occurred_at"])
    kind = document["kind"]
    event_type = document["type"]
    severity = document["severity"]
    summary = document["summary"]
    if not isinstance(kind, str) or kind not in {"notification", "firing", "resolved"}:
        raise EventRejected("schema")
    if not isinstance(event_type, str) or not re.fullmatch(r"[a-z][a-z0-9_.-]{0,63}", event_type):
        raise EventRejected("schema")
    if not isinstance(severity, str) or severity not in {"info", "warning", "critical"}:
        raise EventRejected("schema")
    if (
        not isinstance(summary, str)
        or not summary.strip()
        or len(summary) > max_summary_chars
        or any(unicodedata.category(char).startswith("C") for char in summary)
        or any(char in "\u2028\u2029" for char in summary)
    ):
        raise EventRejected("schema")
    incident_key = document.get("incident_key")
    if kind == "notification":
        if "incident_key" in document:
            raise EventRejected("schema")
    elif not isinstance(incident_key, str) or not re.fullmatch(
        r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}", incident_key
    ):
        raise EventRejected("schema")
    return IncomingEvent(identity, occurred_at, kind, event_type, severity, summary, incident_key)


def validate_event_time(
    event: IncomingEvent, now: datetime, max_age: float, future_skew: float
) -> None:
    """Validate new reports only; retained duplicate receipts bypass this check."""
    require_aware(now)
    require_aware(event.occurred_at)
    if (
        not math.isfinite(max_age)
        or max_age <= 0
        or not math.isfinite(future_skew)
        or future_skew < 0
    ):
        raise ValueError("Event time limits must be finite and bounded")
    age = (now - event.occurred_at).total_seconds()
    if age > max_age or age < -future_skew:
        raise EventRejected("event_time")


@dataclass(frozen=True)
class OccurrenceState:
    event_type: str
    opened_at: datetime | None = None
    resolved_at: datetime | None = None


@dataclass(frozen=True)
class EventDecision:
    state: OccurrenceState | None
    outcome: str
    notify: bool


def decide_event(event: IncomingEvent, previous: OccurrenceState | None) -> EventDecision:
    """A retained terminal occurrence cannot reopen, regardless of event ordering."""
    if event.kind == "notification":
        return EventDecision(None, "notification", True)
    if previous is not None:
        if previous.event_type != event.event_type:
            raise EventRejected("lifecycle")
        if previous.resolved_at is not None or event.kind == "firing":
            return EventDecision(previous, "noop", False)
        assert previous.opened_at is not None
        if event.occurred_at < previous.opened_at:
            raise EventRejected("lifecycle")
        return EventDecision(
            OccurrenceState(previous.event_type, previous.opened_at, event.occurred_at),
            "resolved",
            True,
        )
    if event.kind == "firing":
        return EventDecision(OccurrenceState(event.event_type, event.occurred_at), "opened", True)
    return EventDecision(
        OccurrenceState(event.event_type, resolved_at=event.occurred_at), "noop", False
    )
