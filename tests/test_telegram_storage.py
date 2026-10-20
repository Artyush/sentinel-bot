import sqlite3
from contextlib import closing
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from sentinel_bot.config import SiteConfig
from sentinel_bot.models import CheckResult, ErrorKind, EventKind, Observation, SiteState
from sentinel_bot.storage import (
    DATABASE_NAME,
    MAX_PENDING_NOTIFICATIONS,
    MAX_PENDING_PUSH_NOTIFICATIONS,
    SQLiteStore,
    StorageError,
    initialize_state,
)

NOW = datetime(2026, 1, 1, tzinfo=UTC)
DESTINATION = "telegram:-100123456:0"
OTHER_DESTINATION = "telegram:-100987654:17"


@pytest.fixture
def site() -> SiteConfig:
    return SiteConfig(
        "site", "https://example.test/", failure_confirmation=1, recovery_confirmation=1
    )


@pytest.fixture
def store(tmp_path: Path, site: SiteConfig) -> SQLiteStore:
    initialize_state(tmp_path, NOW)
    result = SQLiteStore(tmp_path)
    result.reconcile((site,))
    return result


def check(store: SQLiteStore, site: SiteConfig, seconds: int = 0, *, success: bool = False) -> None:
    store.apply_observation(
        site,
        Observation(
            NOW + timedelta(seconds=seconds),
            CheckResult(success, None if success else ErrorKind.TIMEOUT),
        ),
    )


def legacy_database(path: Path) -> None:
    """Create the actual schema-2 contract without importing migration internals."""
    with closing(sqlite3.connect(path / DATABASE_NAME)) as connection, connection:
        connection.executescript(
            """PRAGMA user_version = 2;
            CREATE TABLE runtime_metadata (
                singleton INTEGER PRIMARY KEY, first_started_at TEXT NOT NULL,
                last_started_at TEXT NOT NULL, startup_count INTEGER NOT NULL);
            CREATE TABLE sites (
                alias TEXT PRIMARY KEY, fingerprint TEXT NOT NULL,
                state TEXT NOT NULL, observation TEXT);
            CREATE TABLE notification_outbox (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                notification_id TEXT UNIQUE NOT NULL, source_alias TEXT NOT NULL,
                destination TEXT NOT NULL, incident_id TEXT NOT NULL,
                kind TEXT NOT NULL, event TEXT NOT NULL,
                in_flight INTEGER NOT NULL DEFAULT 0, delivered_at TEXT);
            CREATE TABLE delivery_progress (
                source_alias TEXT NOT NULL, destination TEXT NOT NULL,
                incident_id TEXT NOT NULL, last_delivered_at TEXT NOT NULL,
                PRIMARY KEY (source_alias, destination));
            CREATE TABLE incident_audit (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                source_alias TEXT NOT NULL, event TEXT NOT NULL);
            CREATE INDEX pending_notifications
                ON notification_outbox (source_alias, destination, delivered_at);
            CREATE UNIQUE INDEX one_pending_reminder
                ON notification_outbox (source_alias, destination, incident_id)
                WHERE kind = 'REMINDER' AND delivered_at IS NULL;"""
        )
        connection.execute(
            "INSERT INTO runtime_metadata VALUES (1, ?, ?, 7)",
            (NOW.isoformat(), NOW.isoformat()),
        )


def test_schema_two_upgrade_preserves_pending_accepted_events_and_startups(tmp_path: Path) -> None:
    legacy_database(tmp_path)
    event = (
        '{"kind":"DOWN","incident_id":"incident",'
        '"occurred_at":"2026-01-01T00:00:00+00:00",'
        '"started_at":"2026-01-01T00:00:00+00:00"}'
    )
    with closing(sqlite3.connect(tmp_path / DATABASE_NAME)) as connection, connection:
        for identity, delivered in (("pending", None), ("accepted", NOW.isoformat())):
            connection.execute(
                """INSERT INTO notification_outbox
                (notification_id, source_alias, destination, incident_id, kind, event, delivered_at)
                VALUES (?, 'site', 'operator', 'incident', 'DOWN', ?, ?)""",
                (identity, event, delivered),
            )
    startup = initialize_state(tmp_path, NOW + timedelta(days=1))
    assert startup.schema_version == 4 and startup.startup_count == 8
    assert startup.first_started_at == NOW.isoformat()
    store = SQLiteStore(tmp_path)
    before = store.pending_notifications()[0]
    assert before.id == "pending" and before.destination == "operator" and before.attempts == 0
    store.configure_telegram(DESTINATION, NOW)
    after = store.pending_notifications()[0]
    assert after.id == before.id and after.event == before.event
    assert after.destination == DESTINATION
    with closing(sqlite3.connect(store.database)) as connection:
        assert connection.execute(
            "SELECT destination, delivered_at FROM notification_outbox WHERE notification_id='accepted'"
        ).fetchone() == ("operator", NOW.isoformat())


def test_failed_schema_two_upgrade_rolls_back_columns_and_metadata(tmp_path: Path) -> None:
    legacy_database(tmp_path)
    with closing(sqlite3.connect(tmp_path / DATABASE_NAME)) as connection, connection:
        connection.execute(
            """CREATE TRIGGER reject_startup BEFORE UPDATE ON runtime_metadata
            BEGIN SELECT RAISE(ABORT, 'synthetic failure'); END"""
        )
    with pytest.raises(StorageError, match="Cannot initialize"):
        initialize_state(tmp_path, NOW)
    with closing(sqlite3.connect(tmp_path / DATABASE_NAME)) as connection:
        assert connection.execute("PRAGMA user_version").fetchone() == (2,)
        assert connection.execute("SELECT startup_count FROM runtime_metadata").fetchone() == (7,)
        assert "attempts" not in {
            row[1] for row in connection.execute("PRAGMA table_info(notification_outbox)")
        }


def test_configure_routes_existing_ordered_pair_without_acknowledgement(
    store: SQLiteStore, site: SiteConfig
) -> None:
    check(store, site)
    check(store, site, 1, success=True)
    before = store.pending_notifications()
    store.configure_telegram(DESTINATION, NOW)
    after = store.pending_notifications()
    assert [record.id for record in after] == [record.id for record in before]
    assert [record.event for record in after] == [record.event for record in before]
    assert all(record.destination == DESTINATION and record.attempts == 0 for record in after)
    assert store.delivery_status()["accepted"] == 0


def test_configured_observation_and_fixed_route_commit_together(
    store: SQLiteStore, site: SiteConfig
) -> None:
    store.configure_telegram(DESTINATION, NOW)
    check(store, site)
    opening = store.pending_notifications()[0]
    assert opening.destination == DESTINATION
    check(store, site, 1, success=True)
    assert store.pending_notifications()[1].destination == opening.destination


def test_retry_attempt_deadline_and_delay_survive_reopening(
    store: SQLiteStore, site: SiteConfig
) -> None:
    store.configure_telegram(DESTINATION, NOW)
    check(store, site)
    first = store.reserve_due_delivery(NOW)
    assert first is not None and first.in_flight and first.attempts == 1
    assert store.reserve_due_delivery(NOW) is None
    due = NOW + timedelta(seconds=42)
    store.retry_delivery(first.id, due, "network", delay_seconds=42)
    reopened = SQLiteStore(store.database.parent)
    reopened.configure_telegram(DESTINATION, NOW + timedelta(seconds=5))
    saved = reopened.pending_notifications()[0]
    assert saved.id == first.id and saved.attempts == 1 and not saved.in_flight
    assert saved.next_attempt_at == due and saved.retry_delay_seconds == 42
    assert saved.last_error == "network"
    assert reopened.reserve_due_delivery(due - timedelta(microseconds=1)) is None
    second = reopened.reserve_due_delivery(due)
    assert second is not None and second.id == first.id and second.attempts == 2


def test_monotonic_due_override_handles_backward_wall_clock(
    store: SQLiteStore, site: SiteConfig
) -> None:
    store.configure_telegram(DESTINATION, NOW)
    check(store, site)
    first = store.reserve_due_delivery(NOW)
    assert first is not None
    store.retry_delivery(first.id, NOW + timedelta(seconds=10), "timeout")
    assert store.pending_notifications()[0].retry_delay_seconds == 10
    assert store.reserve_due_delivery(NOW - timedelta(days=1), ready_ids=()) is None
    second = store.reserve_due_delivery(NOW - timedelta(days=1), ready_ids=(first.id,))
    assert second is not None and second.id == first.id


def test_interrupted_send_is_retried_same_identity_destination_with_visible_window(
    store: SQLiteStore, site: SiteConfig
) -> None:
    store.configure_telegram(DESTINATION, NOW)
    check(store, site)
    reserved = store.reserve_due_delivery(NOW)
    assert reserved is not None
    restored = SQLiteStore(store.database.parent)
    assert restored.configure_telegram(DESTINATION, NOW + timedelta(seconds=5)) == 1
    saved = restored.pending_notifications()[0]
    assert saved.id == reserved.id and saved.destination == reserved.destination
    assert saved.attempts == 1 and saved.last_error == "interrupted" and not saved.in_flight
    assert restored.configure_telegram(DESTINATION, NOW + timedelta(seconds=6)) == 0
    again = restored.reserve_due_delivery(NOW + timedelta(seconds=6))
    assert again is not None and again.id == reserved.id and again.attempts == 2


@pytest.mark.parametrize("removed", [False, True])
def test_startup_in_flight_recovery_then_inventory_change_preserves_unknown_send(
    store: SQLiteStore, site: SiteConfig, removed: bool
) -> None:
    store.configure_telegram(DESTINATION, NOW)
    check(store, site)
    reserved = store.reserve_due_delivery(NOW)
    assert reserved is not None
    restored = SQLiteStore(store.database.parent)
    assert restored.configure_telegram(DESTINATION, NOW) == 1
    restored.reconcile(()) if removed else restored.reconcile((replace(site, marker="changed"),))
    pending = restored.pending_notifications()
    assert len(pending) == 1 and pending[0].id == reserved.id
    assert pending[0].destination == DESTINATION and pending[0].attempts == 1
    assert pending[0].last_error == "interrupted"
    retry = restored.reserve_due_delivery(NOW)
    assert retry is not None and retry.id == reserved.id and retry.attempts == 2


def test_flapping_before_telegram_reconnect_cannot_coalesce_unknown_opening_attempt(
    store: SQLiteStore, site: SiteConfig
) -> None:
    store.configure_telegram(DESTINATION, NOW)
    check(store, site)
    old = store.reserve_due_delivery(NOW)
    assert old is not None
    store.configure_telegram(DESTINATION, NOW)
    check(store, site, 1, success=True)
    check(store, site, 2)
    check(store, site, 3, success=True)
    pending = store.pending_notifications()
    assert pending[0].id == old.id and pending[0].attempts == 1
    assert [record.event.kind for record in pending[1:]] == [EventKind.DOWN, EventKind.RECOVERY]
    assert pending[1].event.incident_id != old.event.incident_id
    retry = store.reserve_due_delivery(NOW)
    assert retry is not None and retry.id == old.id


def test_recovery_cannot_drop_a_previously_attempted_reminder(
    store: SQLiteStore, site: SiteConfig
) -> None:
    store.configure_telegram(DESTINATION, NOW)
    check(store, site)
    opening = store.reserve_due_delivery(NOW)
    assert opening is not None
    store.acknowledge(opening.id, NOW)
    assert store.queue_reminder(site, NOW, True, due=True)
    reminder = store.reserve_due_delivery(NOW)
    assert reminder is not None
    store.retry_delivery(reminder.id, NOW, "network", delay_seconds=0)
    check(store, site, 1, success=True)
    pending = store.pending_notifications()
    assert pending[0].id == reminder.id and pending[0].attempts == 1
    assert pending[1].event.kind is EventKind.RECOVERY


def test_disabled_channel_pauses_attempts_and_preserves_bound_route(
    store: SQLiteStore, site: SiteConfig
) -> None:
    store.configure_telegram(DESTINATION, NOW)
    check(store, site)
    before = store.pending_notifications()
    store.configure_telegram(None, NOW)
    assert store.reserve_due_delivery(NOW + timedelta(days=1)) is None
    assert store.pending_notifications() == before
    store.configure_telegram(DESTINATION, NOW)
    reserved = store.reserve_due_delivery(NOW)
    assert reserved is not None and reserved.id == before[0].id


def test_disabled_telegram_does_not_schedule_new_reminders_for_old_accepted_route(
    store: SQLiteStore, site: SiteConfig
) -> None:
    store.configure_telegram(DESTINATION, NOW)
    check(store, site)
    opening = store.reserve_due_delivery(NOW)
    assert opening is not None
    store.acknowledge(opening.id, NOW)
    store.configure_telegram(None, NOW)
    assert not store.queue_reminder(site, NOW + timedelta(days=1), True)
    assert not store.queue_reminder(site, NOW + timedelta(days=1), True, due=True)
    assert store.pending_count() == 0


@pytest.mark.parametrize("attempted", [False, True])
def test_reenabled_route_coalesces_old_unsent_pair_but_preserves_interrupted_attempt(
    store: SQLiteStore, site: SiteConfig, attempted: bool
) -> None:
    store.configure_telegram(DESTINATION, NOW)
    check(store, site)
    old = store.pending_notifications()[0]
    if attempted:
        assert store.reserve_due_delivery(NOW) is not None
    store.configure_telegram(None, NOW)
    check(store, site, 1, success=True)
    check(store, site, 2)
    check(store, site, 3, success=True)
    latest_incident = store.pending_notifications()[-1].event.incident_id
    store.configure_telegram(DESTINATION, NOW)
    pending = store.pending_notifications()
    if attempted:
        assert pending[0].id == old.id and pending[0].attempts == 1
        pending = pending[1:]
    assert [record.event.kind for record in pending] == [EventKind.DOWN, EventKind.RECOVERY]
    assert all(record.event.incident_id == latest_incident for record in pending)
    assert all(record.destination == DESTINATION for record in pending)
    store.configure_telegram(DESTINATION, NOW)
    assert store.pending_count() == (3 if attempted else 2)


def test_route_change_fails_old_opening_and_blocks_future_recovery_without_reroute(
    store: SQLiteStore, site: SiteConfig
) -> None:
    store.configure_telegram(DESTINATION, NOW)
    check(store, site)
    old = store.pending_notifications()[0]
    store.configure_telegram(OTHER_DESTINATION, NOW)
    assert store.reserve_due_delivery(NOW) is None
    assert store.delivery_status()["failed"] == 1
    check(store, site, 1, success=True)
    assert store.delivery_status()["blocked"] == 1
    with closing(sqlite3.connect(store.database)) as connection:
        assert connection.execute(
            "SELECT destination, blocked_by FROM notification_outbox WHERE kind = 'RECOVERY'"
        ).fetchone() == (DESTINATION, old.id)
    check(store, site, 2)
    current = store.pending_notifications()[0]
    assert current.destination == OTHER_DESTINATION and current.event.kind is EventKind.DOWN


def test_destination_change_preserves_accepted_send_and_never_resends_it(
    store: SQLiteStore, site: SiteConfig
) -> None:
    store.configure_telegram(DESTINATION, NOW)
    check(store, site)
    opening = store.reserve_due_delivery(NOW)
    assert opening is not None
    store.acknowledge(opening.id, NOW)
    store.configure_telegram(OTHER_DESTINATION, NOW)
    assert store.delivery_status()["accepted"] == 1
    assert store.reserve_due_delivery(NOW) is None
    with closing(sqlite3.connect(store.database)) as connection:
        assert connection.execute(
            "SELECT destination, delivered_at FROM notification_outbox WHERE notification_id = ?",
            (opening.id,),
        ).fetchone() == (DESTINATION, NOW.isoformat())
    assert not store.queue_reminder(site, NOW + timedelta(days=1), True, due=True)
    check(store, site, 1, success=True)
    assert store.pending_count() == 0
    assert store.delivery_status()["failed"] == 1
    assert store.delivery_status()["blocked"] == 0
    with closing(sqlite3.connect(store.database)) as connection:
        assert connection.execute(
            "SELECT destination, last_error FROM notification_outbox WHERE kind = 'RECOVERY'"
        ).fetchone() == (DESTINATION, "destination_changed")


def test_destination_change_of_existing_pair_has_one_failure_and_one_blocked_recovery(
    store: SQLiteStore, site: SiteConfig
) -> None:
    store.configure_telegram(DESTINATION, NOW)
    check(store, site)
    check(store, site, 1, success=True)
    store.configure_telegram(OTHER_DESTINATION, NOW)
    assert store.delivery_status()["failed"] == 1
    assert store.delivery_status()["blocked"] == 1
    assert store.pending_count() == 0


def test_recovery_cannot_bypass_opening_retry_but_other_source_can(
    store: SQLiteStore, site: SiteConfig
) -> None:
    other = replace(site, alias="other")
    store.reconcile((site, other))
    store.configure_telegram(DESTINATION, NOW)
    check(store, site)
    check(store, site, 1, success=True)
    check(store, other, 2)
    opening = store.reserve_due_delivery(NOW)
    assert opening is not None and opening.source_alias == site.alias
    store.retry_delivery(opening.id, NOW + timedelta(seconds=30), "network")
    independent = store.reserve_due_delivery(NOW)
    assert independent is not None and independent.source_alias == other.alias
    store.acknowledge(independent.id, NOW)
    assert store.reserve_due_delivery(NOW) is None
    opening = store.reserve_due_delivery(NOW + timedelta(seconds=30))
    assert opening is not None
    store.acknowledge(opening.id, NOW + timedelta(seconds=30))
    recovery = store.reserve_due_delivery(NOW + timedelta(seconds=30))
    assert recovery is not None and recovery.event.kind is EventKind.RECOVERY


@pytest.mark.parametrize("recovery_first", [False, True])
def test_permanent_opening_failure_blocks_dependent_recovery(
    store: SQLiteStore, site: SiteConfig, recovery_first: bool
) -> None:
    store.configure_telegram(DESTINATION, NOW)
    check(store, site)
    if recovery_first:
        check(store, site, 1, success=True)
    opening = store.reserve_due_delivery(NOW)
    assert opening is not None
    store.fail_delivery(opening.id, NOW, "forbidden")
    if not recovery_first:
        check(store, site, 1, success=True)
    assert store.delivery_status()["failed"] == 1
    assert store.delivery_status()["blocked"] == 1
    assert store.pending_count() == 0 and store.reserve_due_delivery(NOW) is None
    assert store.read_site(site.alias).state.confirmed is SiteState.UP
    with pytest.raises(StorageError, match="terminal"):
        store.acknowledge(opening.id, NOW)
    store.fail_delivery(opening.id, NOW + timedelta(days=1), "bad_request")
    assert store.delivery_status()["last_error"] in {"forbidden", "opening_failed"}


def test_transient_failure_does_not_enable_reminders(store: SQLiteStore, site: SiteConfig) -> None:
    store.configure_telegram(DESTINATION, NOW)
    check(store, site)
    opening = store.reserve_due_delivery(NOW)
    assert opening is not None
    store.retry_delivery(opening.id, NOW + timedelta(seconds=10), "network")
    assert store.reminder_progress() == ()
    assert not store.queue_reminder(site, NOW + timedelta(days=1), True, due=True)


def test_monotonic_reminder_requires_fresh_accepted_matching_incident(
    store: SQLiteStore, site: SiteConfig
) -> None:
    store.configure_telegram(DESTINATION, NOW)
    check(store, site)
    opening = store.reserve_due_delivery(NOW)
    assert opening is not None
    accepted = NOW - timedelta(hours=1)
    store.acknowledge(opening.id, accepted)
    progress = store.reminder_progress()
    assert len(progress) == 1 and progress[0].destination == DESTINATION
    assert progress[0].last_delivered_at == accepted
    assert not store.queue_reminder(site, accepted, False, due=True)
    assert not store.queue_reminder(site, accepted, True, due=False)
    assert store.queue_reminder(site, accepted, True, due=True)
    assert not store.queue_reminder(site, accepted, True, due=True)
    assert store.pending_notifications()[0].event.kind is EventKind.REMINDER


def test_permanent_reminder_failure_suppresses_repeat_but_keeps_recovery_deliverable(
    store: SQLiteStore, site: SiteConfig
) -> None:
    store.configure_telegram(DESTINATION, NOW)
    check(store, site)
    opening = store.reserve_due_delivery(NOW)
    assert opening is not None
    store.acknowledge(opening.id, NOW)
    assert store.queue_reminder(site, NOW, True, due=True)
    reminder = store.reserve_due_delivery(NOW)
    assert reminder is not None and reminder.event.kind is EventKind.REMINDER
    store.fail_delivery(reminder.id, NOW, "forbidden")
    restored = SQLiteStore(store.database.parent)
    restored.configure_telegram(DESTINATION, NOW)
    assert not restored.queue_reminder(site, NOW + timedelta(days=1), True, due=True)
    check(restored, site, 1, success=True)
    recovery = restored.reserve_due_delivery(NOW)
    assert recovery is not None and recovery.event.kind is EventKind.RECOVERY
    restored.acknowledge(recovery.id, NOW)
    check(restored, site, 2)
    opening = restored.reserve_due_delivery(NOW)
    assert opening is not None and opening.event.kind is EventKind.DOWN
    restored.acknowledge(opening.id, NOW)
    assert restored.queue_reminder(site, NOW, True, due=True)


def test_telegram_status_excludes_unrouted_logical_operator_records(
    store: SQLiteStore, site: SiteConfig
) -> None:
    check(store, site)
    assert store.pending_count() == 1
    assert store.delivery_status()["pending"] == 0
    store.configure_telegram(DESTINATION, NOW)
    assert store.delivery_status()["pending"] == 1


def test_shared_retry_after_pause_survives_restart_and_can_clear(store: SQLiteStore) -> None:
    assert store.get_send_pause() is None
    until = NOW + timedelta(seconds=123)
    store.set_send_pause(until, 123)
    reopened = SQLiteStore(store.database.parent)
    reopened.configure_telegram(DESTINATION, NOW + timedelta(days=10))
    assert reopened.get_send_pause() == (until, 123)
    reopened.set_telegram_error(None)
    assert reopened.delivery_status()["paused"] is True
    assert reopened.delivery_status()["last_error"] == "rate_limit"
    reopened.clear_send_pause()
    assert store.get_send_pause() is None
    assert reopened.delivery_status()["paused"] is False
    assert reopened.delivery_status()["last_error"] is None


def test_shared_pause_does_not_hide_a_permanent_delivery_failure(
    store: SQLiteStore, site: SiteConfig
) -> None:
    store.configure_telegram(DESTINATION, NOW)
    check(store, site)
    opening = store.reserve_due_delivery(NOW)
    assert opening is not None
    store.fail_delivery(opening.id, NOW, "forbidden")
    store.set_send_pause(NOW + timedelta(seconds=123), 123)
    status = store.delivery_status()
    assert status["paused"] is True and status["last_error"] == "forbidden"


def test_channel_error_visible_without_any_delivery_records(store: SQLiteStore) -> None:
    store.set_telegram_error("network")
    assert store.delivery_status()["last_error"] == "network"
    store.set_telegram_error(None)
    assert store.delivery_status()["last_error"] is None
    with pytest.raises(ValueError, match="normalized"):
        store.set_telegram_error("https://api.example.test/token-secret")


def test_cursor_is_durable_monotonic_and_scoped_to_bot_identity(store: SQLiteStore) -> None:
    assert store.get_poll_cursor(123) == 0
    store.set_poll_cursor(123, 42)
    store.set_poll_cursor(123, 10)
    assert SQLiteStore(store.database.parent).get_poll_cursor(123) == 42
    assert store.get_poll_cursor(456) == 0
    store.set_poll_cursor(456, 7)
    assert store.get_poll_cursor(123) == 0 and store.get_poll_cursor(456) == 7
    with closing(sqlite3.connect(store.database)) as connection:
        assert connection.execute("SELECT COUNT(*) FROM telegram_updates").fetchone() == (1,)


def test_cursor_checkpoint_time_survives_restart_and_only_new_offsets_advance_it(
    store: SQLiteStore,
) -> None:
    assert store.get_poll_checkpoint(123) == (0, None)
    store.set_poll_cursor(123, 42, received_at=NOW)
    reopened = SQLiteStore(store.database.parent)
    assert reopened.get_poll_checkpoint(123) == (42, NOW)
    reopened.set_poll_cursor(123, 42, received_at=NOW + timedelta(hours=1))
    reopened.set_poll_cursor(123, 10, received_at=NOW + timedelta(hours=2))
    assert store.get_poll_checkpoint(123) == (42, NOW)
    updated = NOW + timedelta(hours=3)
    reopened.set_poll_cursor(123, 43, received_at=updated)
    assert store.get_poll_checkpoint(123) == (43, updated)


def test_expired_cursor_can_reset_before_randomly_lower_next_update_identity(
    store: SQLiteStore,
) -> None:
    store.set_poll_cursor(123, 1_000_000, received_at=NOW)
    store.reset_poll_cursor(456)
    assert store.get_poll_checkpoint(123) == (1_000_000, NOW)
    store.reset_poll_cursor(123)
    assert store.get_poll_checkpoint(123) == (0, None)
    later = NOW + timedelta(days=8)
    store.set_poll_cursor(123, 20, received_at=later)
    assert store.get_poll_checkpoint(123) == (20, later)


def test_cursor_rejects_ambiguous_checkpoint_time_before_mutation(store: SQLiteStore) -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        store.set_poll_cursor(123, 42, received_at=datetime(2026, 1, 1))
    assert store.get_poll_checkpoint(123) == (0, None)


@pytest.mark.parametrize("value", [True, 0, -1, 2**63])
def test_cursor_rejects_invalid_bot_identity_without_mutation(
    store: SQLiteStore, value: int
) -> None:
    with pytest.raises(ValueError, match="bot ID"):
        store.set_poll_cursor(value, 1)
    assert store.get_poll_cursor(123) == 0


@pytest.mark.parametrize("value", [True, -1, 2**63])
def test_cursor_rejects_invalid_offset(store: SQLiteStore, value: int) -> None:
    with pytest.raises(ValueError, match="offset"):
        store.set_poll_cursor(123, value)


@pytest.mark.parametrize("delay", [True, -1, float("nan"), float("inf"), 604801])
def test_retry_rejects_unbounded_delays_without_releasing_send(
    store: SQLiteStore, site: SiteConfig, delay: float
) -> None:
    store.configure_telegram(DESTINATION, NOW)
    check(store, site)
    opening = store.reserve_due_delivery(NOW)
    assert opening is not None
    with pytest.raises(ValueError, match="Retry delay"):
        store.retry_delivery(opening.id, NOW, "network", delay_seconds=delay)
    assert store.pending_notifications()[0].in_flight


def test_seven_day_retry_hint_and_shared_pause_are_persisted_without_shortening(
    store: SQLiteStore, site: SiteConfig
) -> None:
    store.configure_telegram(DESTINATION, NOW)
    check(store, site)
    opening = store.reserve_due_delivery(NOW)
    assert opening is not None
    until = NOW + timedelta(days=7)
    store.retry_delivery(opening.id, until, "rate_limit", delay_seconds=604800)
    store.set_send_pause(until, 604800)
    assert store.pending_notifications()[0].retry_delay_seconds == 604800
    assert store.get_send_pause() == (until, 604800)


@pytest.mark.parametrize("destination", ["telegram:0:0", "other:123:0", "telegram:123:x", "secret"])
def test_destination_validation_does_not_change_queue(
    store: SQLiteStore, site: SiteConfig, destination: str
) -> None:
    check(store, site)
    before = store.pending_notifications()
    with pytest.raises(ValueError, match="destination"):
        store.configure_telegram(destination, NOW)
    assert store.pending_notifications() == before


def test_failed_history_is_bounded_and_preserves_active_opening_failure(
    store: SQLiteStore, site: SiteConfig
) -> None:
    active = replace(site, alias="active")
    store.reconcile((site, active))
    store.configure_telegram(DESTINATION, NOW)
    check(store, active)
    old = store.reserve_due_delivery(NOW)
    assert old is not None
    store.fail_delivery(old.id, NOW, "forbidden")
    for index in range(110):
        check(store, site, index * 2)
        opening = store.reserve_due_delivery(NOW)
        assert opening is not None
        store.fail_delivery(opening.id, NOW, "bad_request")
        check(store, site, index * 2 + 1, success=True)
    with closing(sqlite3.connect(store.database)) as connection:
        assert connection.execute("SELECT COUNT(*) FROM notification_outbox").fetchone() == (200,)
        assert (
            connection.execute(
                "SELECT 1 FROM notification_outbox WHERE notification_id = ?", (old.id,)
            ).fetchone()
            is None
        )
    check(store, active, 300, success=True)
    assert store.pending_count() == 0
    with closing(sqlite3.connect(store.database)) as connection:
        assert connection.execute(
            "SELECT blocked_by FROM notification_outbox WHERE source_alias = 'active'"
        ).fetchone() == (old.id,)


def test_storage_rejects_retry_without_reserved_send(store: SQLiteStore, site: SiteConfig) -> None:
    store.configure_telegram(DESTINATION, NOW)
    check(store, site)
    opening = store.pending_notifications()[0]
    with pytest.raises(StorageError, match="in-flight"):
        store.retry_delivery(opening.id, NOW, "network")
    with pytest.raises(ValueError, match="normalized"):
        store.fail_delivery(opening.id, NOW, "synthetic provider credential secret")
    assert store.pending_notifications()[0] == opening


def test_acknowledgement_commit_failure_keeps_unknown_provider_result_for_restart(
    store: SQLiteStore, site: SiteConfig
) -> None:
    store.configure_telegram(DESTINATION, NOW)
    check(store, site)
    opening = store.reserve_due_delivery(NOW)
    assert opening is not None
    with closing(sqlite3.connect(store.database)) as connection, connection:
        connection.execute(
            """CREATE TRIGGER reject_acceptance BEFORE UPDATE OF delivered_at
            ON notification_outbox WHEN NEW.delivered_at IS NOT NULL
            BEGIN SELECT RAISE(ABORT, 'synthetic local commit failure'); END"""
        )
    with pytest.raises(StorageError, match="Cannot commit"):
        store.acknowledge(opening.id, NOW)
    assert store.pending_notifications()[0] == opening
    assert store.reminder_progress() == ()
    assert store.configure_telegram(DESTINATION, NOW) == 1
    retry = store.reserve_due_delivery(NOW)
    assert retry is not None and retry.id == opening.id and retry.attempts == 2


def test_terminal_failure_and_recovery_blocking_are_one_transaction(
    store: SQLiteStore, site: SiteConfig
) -> None:
    store.configure_telegram(DESTINATION, NOW)
    check(store, site)
    check(store, site, 1, success=True)
    opening = store.reserve_due_delivery(NOW)
    assert opening is not None
    before = store.pending_notifications()
    with closing(sqlite3.connect(store.database)) as connection, connection:
        connection.execute(
            """CREATE TRIGGER reject_block BEFORE UPDATE OF blocked_by
            ON notification_outbox WHEN NEW.blocked_by IS NOT NULL
            BEGIN SELECT RAISE(ABORT, 'synthetic failure'); END"""
        )
    with pytest.raises(StorageError, match="Cannot commit"):
        store.fail_delivery(opening.id, NOW, "forbidden")
    assert store.pending_notifications() == before
    assert store.delivery_status()["failed"] == store.delivery_status()["blocked"] == 0


def test_legacy_in_flight_identity_is_recovered_without_destination_reroute(
    store: SQLiteStore, site: SiteConfig
) -> None:
    check(store, site)
    legacy = store.pending_notifications()[0]
    store.mark_in_flight(legacy.id)
    assert store.configure_telegram(DESTINATION, NOW) == 1
    recovered = store.pending_notifications()[0]
    assert recovered.id == legacy.id and recovered.destination == "operator"
    assert recovered.last_error == "interrupted"
    assert store.reserve_due_delivery(NOW) is None


def test_reserve_enforces_global_in_flight_bound_across_removed_sources(
    store: SQLiteStore, site: SiteConfig
) -> None:
    old_sites = tuple(replace(site, alias=f"old{index}") for index in range(20))
    store.reconcile(old_sites)
    store.configure_telegram(DESTINATION, NOW)
    reservations = []
    for old in old_sites:
        check(store, old)
        reserved = store.reserve_due_delivery(NOW)
        assert reserved is not None
        reservations.append(reserved)
    store.reconcile((site,))
    check(store, site, 3)
    assert store.reserve_due_delivery(NOW) is None
    store.acknowledge(reservations[0].id, NOW)
    current = store.reserve_due_delivery(NOW)
    assert current is not None and current.source_alias == site.alias


def test_corrupt_shared_pause_is_visible(store: SQLiteStore) -> None:
    with closing(sqlite3.connect(store.database)) as connection, connection:
        connection.execute(
            "UPDATE telegram_route SET pause_until = ?, pause_delay_seconds = -1",
            (NOW.isoformat(),),
        )
    with pytest.raises(StorageError, match="pause is invalid"):
        store.get_send_pause()


def test_missing_route_metadata_rejects_startup_without_incrementing_count(
    store: SQLiteStore,
) -> None:
    with closing(sqlite3.connect(store.database)) as connection, connection:
        connection.execute("DELETE FROM telegram_route")
    with pytest.raises(StorageError, match="route metadata is missing"):
        initialize_state(store.database.parent, NOW)
    with closing(sqlite3.connect(store.database)) as connection:
        assert connection.execute("SELECT startup_count FROM runtime_metadata").fetchone() == (1,)


def test_reservation_capacity_and_unknown_candidates_do_not_mutate_state(
    store: SQLiteStore, site: SiteConfig
) -> None:
    store.configure_telegram(DESTINATION, NOW)
    check(store, site)
    before = store.pending_notifications()
    assert store.reserve_due_delivery(NOW, ready_ids=("unknown",)) is None
    with pytest.raises(ValueError, match="Too many"):
        store.reserve_due_delivery(
            NOW,
            ready_ids=tuple(
                str(index)
                for index in range(MAX_PENDING_NOTIFICATIONS + MAX_PENDING_PUSH_NOTIFICATIONS + 1)
            ),
        )
    assert store.pending_notifications() == before
