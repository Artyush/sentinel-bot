from dataclasses import replace
from datetime import UTC, datetime, timedelta, timezone

import pytest

from sentinel_bot.models import (
    CheckResult,
    ErrorKind,
    Event,
    EventKind,
    Observation,
    PolicyState,
    SiteState,
)
from sentinel_bot.policy import apply_observation, due_reminder, is_fresh

START = datetime(2026, 1, 1, tzinfo=UTC)


def observe(
    state: PolicyState, success: bool, second: int = 0, **thresholds: int
) -> tuple[PolicyState, Event | None]:
    observation = Observation(
        START + timedelta(seconds=second),
        CheckResult(success, None if success else ErrorKind.CONNECTION),
    )
    transition = apply_observation("demo", "fingerprint", state, observation, **thresholds)
    return transition.state, transition.event


def active_incident() -> PolicyState:
    state = PolicyState()
    for second in range(3):
        state, _ = observe(state, False, second)
    return state


@pytest.mark.parametrize(
    ("sequence", "expected_states", "expected_events"),
    [
        ([True], [SiteState.UP], [None]),
        (
            [False, False, False, False],
            [SiteState.UNKNOWN, SiteState.UNKNOWN, SiteState.DOWN, SiteState.DOWN],
            [None, None, EventKind.DOWN, None],
        ),
        (
            [True, False, False, False],
            [SiteState.UP, SiteState.UP, SiteState.UP, SiteState.DOWN],
            [None, None, None, EventKind.DOWN],
        ),
        (
            [False, True, False, True, False, True],
            [
                SiteState.UNKNOWN,
                SiteState.UP,
                SiteState.UP,
                SiteState.UP,
                SiteState.UP,
                SiteState.UP,
            ],
            [None] * 6,
        ),
        (
            [False, False, False, True, True, True],
            [
                SiteState.UNKNOWN,
                SiteState.UNKNOWN,
                SiteState.DOWN,
                SiteState.DOWN,
                SiteState.UP,
                SiteState.UP,
            ],
            [None, None, EventKind.DOWN, None, EventKind.RECOVERY, None],
        ),
    ],
)
def test_confirmation_sequences(sequence, expected_states, expected_events):
    state = PolicyState()
    for second, (success, expected_state, expected_event) in enumerate(
        zip(sequence, expected_states, expected_events, strict=True)
    ):
        state, event = observe(state, success, second)
        assert state.confirmed is expected_state
        assert (event.kind if event else None) is expected_event


def test_incident_starts_at_first_failure_in_unbroken_series():
    state = PolicyState()
    for second, success in enumerate([False, False, True, False, False, False]):
        state, event = observe(state, success, second)
    assert state.incident_started_at == START + timedelta(seconds=3)
    assert event is not None
    assert event.started_at == START + timedelta(seconds=3)
    assert event.occurred_at == START + timedelta(seconds=5)
    assert state.failures == state.successes == 0
    assert state.first_failure_at is None


def test_failure_resets_pending_recovery_and_keeps_incident_identity():
    active = active_incident()
    state, _ = observe(active, True, 3)
    assert state.successes == 1
    state, event = observe(state, False, 4)
    assert state == active
    assert event is None
    state, event = observe(state, True, 5)
    assert state.confirmed is SiteState.DOWN
    assert event is None
    state, event = observe(state, True, 6)
    assert state == PolicyState(confirmed=SiteState.UP)
    assert event is not None
    assert event.kind is EventKind.RECOVERY
    assert event.incident_id == active.incident_id
    assert event.started_at == active.incident_started_at


@pytest.mark.parametrize(("failure_count", "recovery_count"), [(1, 1), (2, 3), (5, 4)])
def test_custom_confirmation_counts_are_exact(failure_count, recovery_count):
    state = PolicyState()
    for second in range(failure_count):
        state, event = observe(
            state,
            False,
            second,
            failure_confirmation=failure_count,
            recovery_confirmation=recovery_count,
        )
        assert (event is not None) == (second == failure_count - 1)
    incident_id = state.incident_id
    for index in range(recovery_count):
        state, event = observe(
            state,
            True,
            failure_count + index,
            failure_confirmation=failure_count,
            recovery_confirmation=recovery_count,
        )
        assert (event is not None) == (index == recovery_count - 1)
    assert event is not None
    assert event.kind is EventKind.RECOVERY
    assert event.incident_id == incident_id


def test_restored_pending_and_active_state_preserve_incident_continuity():
    state, _ = observe(PolicyState(), False)
    checkpoint = replace(state)
    continued, _ = observe(checkpoint, False, 1)
    restored_down, event = observe(continued, False, 2)
    uninterrupted = active_incident()
    assert restored_down == uninterrupted
    assert event is not None
    assert event.incident_id == uninterrupted.incident_id
    state, event = observe(replace(restored_down), False, 3)
    assert state == restored_down
    assert event is None


def test_incident_identity_varies_with_source_configuration_and_occurrence():
    observation = Observation(START, CheckResult(False, ErrorKind.TIMEOUT))
    identities = {
        apply_observation(
            alias, fingerprint, PolicyState(), observation, failure_confirmation=1
        ).state.incident_id
        for alias, fingerprint in [("one", "a"), ("two", "a"), ("one", "b")]
    }
    later = replace(observation, checked_at=START + timedelta(seconds=1))
    identities.add(
        apply_observation(
            "one", "a", PolicyState(), later, failure_confirmation=1
        ).state.incident_id
    )
    assert len(identities) == 4


def test_incident_identity_and_events_use_normalized_utc_times():
    offset = timezone(timedelta(hours=4))
    local_time = START.astimezone(offset)
    utc = Observation(START, CheckResult(False, ErrorKind.DNS))
    local = replace(utc, checked_at=local_time)
    first = apply_observation("demo", "same", PolicyState(), utc, failure_confirmation=1)
    second = apply_observation("demo", "same", PolicyState(), local, failure_confirmation=1)
    assert first == second
    assert second.event is not None
    assert second.event.started_at.tzinfo is UTC
    assert second.event.occurred_at.tzinfo is UTC


@pytest.mark.parametrize(
    ("state", "delivered_second", "now_second", "fresh", "expected"),
    [
        (PolicyState(), 10, 1810, True, False),
        (PolicyState(confirmed=SiteState.UP), 10, 1810, True, False),
        (active_incident(), None, 1810, True, False),
        (active_incident(), 10, 1810, False, False),
        (active_incident(), 10, 1809, True, False),
        (active_incident(), 10, 1810, True, True),
        (active_incident(), 1000, 1810, True, False),
        (active_incident(), 1000, 2800, True, True),
        (active_incident(), 10, 5, True, False),
        (active_incident(), -1, 1810, True, False),
    ],
)
def test_reminders_require_fresh_active_incident_and_delivery_interval(
    state, delivered_second, now_second, fresh, expected
):
    delivered_at = (
        START + timedelta(seconds=delivered_second) if delivered_second is not None else None
    )
    event = due_reminder(state, START + timedelta(seconds=now_second), delivered_at, 1800, fresh)
    assert (event is not None) is expected
    if event is not None:
        assert event.kind is EventKind.REMINDER
        assert event.incident_id == state.incident_id
        assert event.started_at == state.incident_started_at


@pytest.mark.parametrize(
    ("last_progress", "now", "expected"),
    [
        (None, 100, False),
        (30, 30, True),
        (30, 100, True),
        (30, 100.001, False),
        (30, 29, False),
        (float("nan"), 100, False),
        (30, float("inf"), False),
    ],
)
def test_freshness_uses_monotonic_boundary_and_requires_post_start_progress(
    last_progress, now, expected
):
    assert is_fresh(last_progress, now, 70) is expected


@pytest.mark.parametrize("thresholds", [{"failure_confirmation": 0}, {"recovery_confirmation": 0}])
def test_invalid_confirmation_thresholds_fail_before_advancing_state(thresholds):
    with pytest.raises(ValueError, match="positive"):
        observe(PolicyState(), True, **thresholds)


@pytest.mark.parametrize("value", [0, -1, float("inf"), float("nan")])
def test_invalid_time_limits_cannot_make_data_fresh_or_reminders_due(value):
    with pytest.raises(ValueError, match="finite and positive"):
        is_fresh(0, 0, value)
    with pytest.raises(ValueError, match="finite and positive"):
        due_reminder(active_incident(), START, START, value, True)


@pytest.mark.parametrize(
    "result",
    [
        {"success": False},
        {"success": True, "error": ErrorKind.DNS},
        {"success": True, "http_status": 99},
        {"success": True, "http_status": 600},
        {"success": True, "latency_ms": -1},
        {"success": True, "latency_ms": float("nan")},
        {"success": True, "latency_ms": float("inf")},
    ],
)
def test_check_result_rejects_ambiguous_or_invalid_observations(result):
    with pytest.raises(ValueError):
        CheckResult(**result)


@pytest.mark.parametrize(
    "state",
    [
        {"failures": -1},
        {"successes": -1},
        {"failures": 1, "successes": 1},
        {"failures": 1},
        {"first_failure_at": START},
        {"confirmed": SiteState.DOWN},
        {"incident_id": "closed"},
        {"incident_started_at": START},
    ],
)
def test_policy_state_rejects_inconsistent_incident_or_pending_series(state):
    with pytest.raises(ValueError):
        PolicyState(**state)


def test_naive_times_are_rejected_at_domain_boundary():
    naive = START.replace(tzinfo=None)
    with pytest.raises(ValueError, match="timezone-aware"):
        Observation(naive, CheckResult(True))
    with pytest.raises(ValueError, match="timezone-aware"):
        PolicyState(failures=1, first_failure_at=naive)
    with pytest.raises(ValueError, match="timezone-aware"):
        due_reminder(active_incident(), naive, START, 1800, True)
    with pytest.raises(ValueError, match="timezone-aware"):
        due_reminder(active_incident(), START, naive, 1800, True)


def test_empty_event_identity_is_rejected():
    with pytest.raises(ValueError, match="identity"):
        Event(EventKind.DOWN, "", START, START)
