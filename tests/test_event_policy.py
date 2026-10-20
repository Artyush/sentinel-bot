import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from sentinel_bot.events import (
    EventRejected,
    IncomingReceipt,
    OccurrenceState,
    decide_event,
    parse_event,
    validate_event_time,
)

NOW = datetime(2026, 1, 1, 12, tzinfo=UTC)
IDENTITY = "a7b869ae-2d98-434d-95cc-00961232f401"


def payload(**changes: object) -> dict[str, object]:
    return {
        "event_id": IDENTITY,
        "occurred_at": "2026-01-01T12:00:00Z",
        "kind": "notification",
        "type": "attack.detected",
        "severity": "warning",
        "summary": "Synthetic detector reported a traffic spike.",
        **changes,
    }


def event(**changes: object):
    return parse_event(json.dumps(payload(**changes), ensure_ascii=False).encode())


@pytest.mark.parametrize("kind", ["notification", "firing", "resolved"])
def test_valid_event_contract_and_plain_unicode_roundtrip(kind: str) -> None:
    changes = {"incident_key": "attack:2026-01-01.1"} if kind != "notification" else {}
    report = event(kind=kind, summary="Synthetic alert: нагрузка élevée ⚠", **changes)
    assert report.event_id == IDENTITY
    assert report.occurred_at == NOW
    assert report.kind == kind
    assert report.event_type == "attack.detected"
    assert report.severity == "warning"
    assert parse_event(json.dumps(report.as_dict(), ensure_ascii=False).encode()) == report


def test_canonical_fingerprint_ignores_json_order_spacing_uuid_case_and_utc_spelling() -> None:
    first = event()
    document = dict(
        reversed(
            list(
                payload(
                    event_id=IDENTITY.upper(), occurred_at="2026-01-01T12:00:00.000000+00:00"
                ).items()
            )
        )
    )
    second = parse_event(json.dumps(document, indent=2).encode())
    assert first == second
    assert first.fingerprint == second.fingerprint


@pytest.mark.parametrize(
    "change",
    [
        {"summary": "Different synthetic observation"},
        {"severity": "critical"},
        {"type": "deploy.completed"},
        {"occurred_at": "2026-01-01T12:00:01Z"},
        {"kind": "firing", "incident_key": "new-occurrence"},
    ],
)
def test_changed_canonical_content_has_different_fingerprint(change: dict[str, object]) -> None:
    assert event(**change).fingerprint != event().fingerprint


@pytest.mark.parametrize(
    "raw",
    [
        b"",
        b"{",
        b"[]",
        b"true",
        b"null",
        b"42",
        b'"text"',
        b"\xff",
        b'{"event_id":NaN}',
        b'{"event_id":Infinity}',
        b'{"event_id":-Infinity}',
        b"[" * 1200 + b"0" + b"]" * 1200,
    ],
)
def test_malformed_or_non_object_json_is_sanitized(raw: bytes) -> None:
    with pytest.raises(EventRejected) as raised:
        parse_event(raw)
    assert raised.value.code == "schema"
    assert str(raised.value) == "schema"


@pytest.mark.parametrize(
    "field", ["event_id", "occurred_at", "kind", "type", "severity", "summary"]
)
def test_missing_required_field_rejected(field: str) -> None:
    document = payload()
    del document[field]
    with pytest.raises(EventRejected, match="schema"):
        parse_event(json.dumps(document).encode())


@pytest.mark.parametrize(
    "field", ["recipient", "url", "source", "token", "headers", "metadata", "command"]
)
def test_untrusted_payload_cannot_choose_source_targets_or_configuration(field: str) -> None:
    with pytest.raises(EventRejected, match="schema"):
        event(**{field: "synthetic-private-value-do-not-print"})


@pytest.mark.parametrize(
    "field,value",
    [
        ("event_id", "invalid"),
        ("event_id", IDENTITY.replace("-", "")),
        ("event_id", True),
        ("event_id", "{" + IDENTITY + "}"),
        ("event_id", "urn:uuid:" + IDENTITY),
        ("kind", "opened"),
        ("kind", "DOWN"),
        ("kind", []),
        ("kind", None),
        ("type", "Attack"),
        ("type", "has spaces"),
        ("type", "a" * 65),
        ("type", {}),
        ("severity", "error"),
        ("severity", "Warning"),
        ("severity", True),
        ("summary", ""),
        ("summary", "   "),
        ("summary", True),
        ("summary", "x" * 1001),
        ("summary", "line\nline"),
        ("summary", "line\rline"),
        ("summary", "text\x00"),
        ("summary", "text\ttext"),
        ("summary", "text\x7f"),
        ("summary", "text\u202etext"),
        ("summary", "text\u2028text"),
        ("summary", "text\u2029text"),
        ("summary", "text\ud800"),
    ],
)
def test_invalid_fields_and_control_text_rejected(field: str, value: object) -> None:
    raw = json.dumps(payload(**{field: value})).encode()
    with pytest.raises(EventRejected) as raised:
        parse_event(raw)
    assert raised.value.code == "schema"


@pytest.mark.parametrize(
    "timestamp",
    [
        "2026-01-01",
        "2026-01-01 12:00:00Z",
        "2026-01-01T12:00Z",
        "2026-01-01T12:00:00",
        "2026-01-01T12:00:00-00:00",
        "2026-01-01T13:00:00+01:00",
        "2026-01-01T12:00:00.1234567Z",
        "2026-01-01T12:00:00z",
        "2026-02-30T12:00:00Z",
        "2026-01-01T12:00:60Z",
        "2026-01-01T12:00:00Z\n",
        True,
        1767268800,
    ],
)
def test_timestamp_requires_real_utc_rfc3339_value(timestamp: object) -> None:
    with pytest.raises(EventRejected, match="schema"):
        event(occurred_at=timestamp)


@pytest.mark.parametrize(
    "timestamp",
    ["2026-01-01T12:00:00Z", "2026-01-01T12:00:00+00:00", "2026-01-01T12:00:00.000001Z"],
)
def test_supported_utc_timestamp_spellings(timestamp: str) -> None:
    assert event(occurred_at=timestamp).occurred_at.tzinfo is UTC


@pytest.mark.parametrize("kind", ["firing", "resolved"])
@pytest.mark.parametrize("key", [None, "", "contains space", "a" * 129, "-first", True, []])
def test_lifecycle_requires_bounded_occurrence_key(kind: str, key: object) -> None:
    with pytest.raises(EventRejected, match="schema"):
        event(kind=kind, incident_key=key)


@pytest.mark.parametrize("key", [None, "unexpected"])
def test_one_shot_event_must_omit_occurrence_key(key: object) -> None:
    with pytest.raises(EventRejected, match="schema"):
        event(incident_key=key)


def test_exact_string_limits_and_configured_summary_limit() -> None:
    report = event(kind="firing", incident_key="x" * 128, type="a" * 64, summary="é" * 1000)
    assert len(report.summary) == 1000
    raw = json.dumps(payload(summary="éé"), ensure_ascii=False).encode()
    assert parse_event(raw, max_body_bytes=len(raw), max_summary_chars=2).summary == "éé"
    for bounds in ({"max_body_bytes": len(raw) - 1}, {"max_summary_chars": 1}):
        with pytest.raises(EventRejected, match="schema"):
            parse_event(raw, **bounds)


def test_duplicate_json_keys_are_rejected_even_if_values_match() -> None:
    raw = json.dumps(payload()).encode()
    raw = b'{"kind":"notification",' + raw[1:]
    with pytest.raises(EventRejected, match="schema"):
        parse_event(raw)


@pytest.mark.parametrize(
    "age,accepted",
    [
        (86400, True),
        (86400.000001, False),
        (-300, True),
        (-300.000001, False),
        (0, True),
    ],
)
def test_event_time_boundaries(age: float, accepted: bool) -> None:
    report = replace(event(), occurred_at=NOW - timedelta(seconds=age))
    if accepted:
        validate_event_time(report, NOW, 86400, 300)
    else:
        with pytest.raises(EventRejected, match="event_time"):
            validate_event_time(report, NOW, 86400, 300)


@pytest.mark.parametrize(
    "age,skew", [(0, 0), (-1, 0), (float("nan"), 0), (1, -1), (1, float("inf"))]
)
def test_invalid_time_policy_is_a_programming_error(age: float, skew: float) -> None:
    with pytest.raises(ValueError):
        validate_event_time(event(), NOW, age, skew)


def test_time_validation_rejects_naive_runtime_clock() -> None:
    with pytest.raises(ValueError):
        validate_event_time(event(), NOW.replace(tzinfo=None), 86400, 300)


def test_notification_is_immediately_eligible_without_polling_confirmation() -> None:
    result = decide_event(event(), None)
    assert result.state is None
    assert result.outcome == "notification"
    assert result.notify


def test_firing_then_matching_resolution_produces_ordered_transitions() -> None:
    opening = event(kind="firing", incident_key="attack-1")
    first = decide_event(opening, None)
    assert first.state == OccurrenceState("attack.detected", NOW)
    assert first.outcome == "opened" and first.notify
    resolution = replace(opening, kind="resolved", occurred_at=NOW + timedelta(seconds=10))
    result = decide_event(resolution, first.state)
    assert result.state == OccurrenceState("attack.detected", NOW, resolution.occurred_at)
    assert result.outcome == "resolved" and result.notify


@pytest.mark.parametrize("offset", [-10, 0, 10])
def test_repeated_firing_is_noop_and_does_not_shift_opening_time(offset: int) -> None:
    previous = OccurrenceState("attack.detected", NOW)
    repeated = replace(
        event(kind="firing", incident_key="attack-1"), occurred_at=NOW + timedelta(seconds=offset)
    )
    result = decide_event(repeated, previous)
    assert result.state is previous
    assert result.outcome == "noop" and not result.notify


def test_resolution_before_opening_creates_tombstone_without_fake_recovery() -> None:
    resolution = event(kind="resolved", incident_key="attack-1")
    first = decide_event(resolution, None)
    assert first.state == OccurrenceState("attack.detected", resolved_at=NOW)
    assert first.outcome == "noop" and not first.notify
    for offset in (-100, 0, 100):
        firing = replace(resolution, kind="firing", occurred_at=NOW + timedelta(seconds=offset))
        result = decide_event(firing, first.state)
        assert result.state is first.state
        assert result.outcome == "noop" and not result.notify


@pytest.mark.parametrize("kind", ["firing", "resolved"])
def test_closed_occurrence_never_reopens_or_emits_duplicate_resolution(kind: str) -> None:
    previous = OccurrenceState("attack.detected", NOW - timedelta(seconds=20), NOW)
    report = replace(
        event(kind=kind, incident_key="attack-1"), occurred_at=NOW + timedelta(seconds=10)
    )
    result = decide_event(report, previous)
    assert result.state is previous
    assert result.outcome == "noop" and not result.notify


@pytest.mark.parametrize("closed", [False, True])
def test_occurrence_type_binding_cannot_change(closed: bool) -> None:
    previous = OccurrenceState("other.type", NOW, NOW if closed else None)
    with pytest.raises(EventRejected, match="lifecycle"):
        decide_event(event(kind="firing", incident_key="attack-1"), previous)


def test_active_resolution_must_not_predate_its_opening() -> None:
    previous = OccurrenceState("attack.detected", NOW + timedelta(seconds=1))
    with pytest.raises(EventRejected, match="lifecycle"):
        decide_event(event(kind="resolved", incident_key="attack-1"), previous)
    equal = decide_event(
        event(kind="resolved", incident_key="attack-1"), OccurrenceState("attack.detected", NOW)
    )
    assert equal.notify and equal.outcome == "resolved"


def test_receipt_represents_acceptance_without_claiming_provider_delivery() -> None:
    receipt = IncomingReceipt(IDENTITY, NOW, "notification")
    assert receipt.as_dict() == {
        "event_id": IDENTITY,
        "received_at": "2026-01-01T12:00:00+00:00",
        "outcome": "notification",
    }
