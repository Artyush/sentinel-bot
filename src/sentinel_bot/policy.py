"""Pure confirmation, incident identity, reminder, and freshness decisions."""

import json
import math
from dataclasses import replace
from datetime import UTC, datetime
from uuid import NAMESPACE_URL, uuid5

from sentinel_bot.models import (
    Event,
    EventKind,
    Observation,
    PolicyState,
    SiteState,
    Transition,
    require_aware,
)


def apply_observation(
    alias: str,
    fingerprint: str,
    state: PolicyState,
    observation: Observation,
    *,
    failure_confirmation: int = 3,
    recovery_confirmation: int = 2,
) -> Transition:
    """Advance pending confirmations once per new observation.

    Callers persist the returned state and optional event atomically. A restored
    state is usable for incident continuity but does not establish freshness.
    """
    if failure_confirmation < 1 or recovery_confirmation < 1:
        raise ValueError("Confirmation thresholds must be positive")
    checked_at = observation.checked_at.astimezone(UTC)
    if observation.result.success:
        return _apply_success(state, checked_at, recovery_confirmation)
    if state.confirmed is SiteState.DOWN:
        return Transition(replace(state, successes=0))

    failures = state.failures + 1
    started_at = (state.first_failure_at or checked_at).astimezone(UTC)
    if failures < failure_confirmation:
        return Transition(
            replace(state, failures=failures, successes=0, first_failure_at=started_at)
        )

    identity = json.dumps([alias, fingerprint, started_at.isoformat()], separators=(",", ":"))
    incident_id = str(uuid5(NAMESPACE_URL, identity))
    active = PolicyState(
        confirmed=SiteState.DOWN,
        incident_id=incident_id,
        incident_started_at=started_at,
    )
    return Transition(active, Event(EventKind.DOWN, incident_id, checked_at, started_at))


def _apply_success(state: PolicyState, checked_at: datetime, confirmation: int) -> Transition:
    if state.confirmed is not SiteState.DOWN:
        return Transition(PolicyState(confirmed=SiteState.UP))

    successes = state.successes + 1
    if successes < confirmation:
        return Transition(replace(state, successes=successes))

    assert state.incident_id is not None
    assert state.incident_started_at is not None
    event = Event(
        EventKind.RECOVERY,
        state.incident_id,
        checked_at,
        state.incident_started_at.astimezone(UTC),
    )
    return Transition(PolicyState(confirmed=SiteState.UP), event)


def due_reminder(
    state: PolicyState,
    now: datetime,
    last_delivered_at: datetime | None,
    reminder_interval_seconds: float,
    fresh: bool,
) -> Event | None:
    """Return a reminder only after this destination's accepted opening/reminder.

    The delivery layer must also enforce one pending reminder per destination.
    This decision does not mark anything delivered or mutate its clock.
    """
    if not math.isfinite(reminder_interval_seconds) or reminder_interval_seconds <= 0:
        raise ValueError("Reminder interval must be finite and positive")
    require_aware(now)
    if last_delivered_at is not None:
        require_aware(last_delivered_at)
    if not fresh or state.confirmed is not SiteState.DOWN or last_delivered_at is None:
        return None
    assert state.incident_id is not None
    assert state.incident_started_at is not None
    if last_delivered_at < state.incident_started_at:
        return None
    if (now - last_delivered_at).total_seconds() < reminder_interval_seconds:
        return None
    return Event(
        EventKind.REMINDER,
        state.incident_id,
        now.astimezone(UTC),
        state.incident_started_at.astimezone(UTC),
    )


def is_fresh(last_progress: float | None, now_monotonic: float, freshness_limit: float) -> bool:
    """Restored observations lack monotonic progress and remain stale until checked."""
    if not math.isfinite(freshness_limit) or freshness_limit <= 0:
        raise ValueError("Freshness limit must be finite and positive")
    if last_progress is None:
        return False
    elapsed = now_monotonic - last_progress
    return math.isfinite(elapsed) and 0 <= elapsed <= freshness_limit
