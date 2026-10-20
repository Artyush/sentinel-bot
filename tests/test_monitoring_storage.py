import sqlite3
from contextlib import closing
from dataclasses import replace
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import pytest

from sentinel_bot.config import SiteConfig
from sentinel_bot.models import CheckResult, ErrorKind, EventKind, Observation, SiteState
from sentinel_bot.storage import DATABASE_NAME, SQLiteStore, StorageError, initialize_state

NOW = datetime(2026, 1, 1, tzinfo=UTC)


def observation(seconds: int, success: bool = False) -> Observation:
    return Observation(
        NOW + timedelta(seconds=seconds),
        CheckResult(success, None if success else ErrorKind.CONNECTION, latency_ms=1.5),
    )


@pytest.fixture
def site() -> SiteConfig:
    return SiteConfig("site", "https://example.test/")


@pytest.fixture
def store(tmp_path: Path, site: SiteConfig) -> SQLiteStore:
    initialize_state(tmp_path, NOW)
    store = SQLiteStore(tmp_path)
    store.reconcile((site,))
    return store


def confirm_down(store: SQLiteStore, site: SiteConfig, offset: int = 0) -> None:
    for seconds in range(offset, offset + site.failure_confirmation):
        store.apply_observation(site, observation(seconds))


def confirm_recovery(store: SQLiteStore, site: SiteConfig, offset: int = 10) -> None:
    for seconds in range(offset, offset + site.recovery_confirmation):
        store.apply_observation(site, observation(seconds, True))


def test_schema_one_upgrade_preserves_startup_metadata(tmp_path: Path) -> None:
    with closing(sqlite3.connect(tmp_path / DATABASE_NAME)) as connection, connection:
        connection.execute("PRAGMA user_version = 1")
        connection.execute(
            """CREATE TABLE runtime_metadata (
            singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
            first_started_at TEXT NOT NULL, last_started_at TEXT NOT NULL,
            startup_count INTEGER NOT NULL CHECK (startup_count > 0))"""
        )
        connection.execute(
            "INSERT INTO runtime_metadata VALUES (1, ?, ?, 7)",
            (NOW.isoformat(), NOW.isoformat()),
        )
    started = NOW + timedelta(days=1)
    metadata = initialize_state(tmp_path, started)
    assert metadata.schema_version == 4
    assert metadata.first_started_at == NOW.isoformat()
    assert metadata.last_started_at == started.isoformat()
    assert metadata.startup_count == 8
    with closing(sqlite3.connect(tmp_path / DATABASE_NAME)) as connection:
        assert connection.execute("PRAGMA user_version").fetchone() == (4,)
        assert connection.execute("SELECT COUNT(*) FROM sites").fetchone() == (0,)
        assert connection.execute("SELECT COUNT(*) FROM notification_outbox").fetchone() == (0,)


def test_failed_upgrade_rolls_back_tables_version_and_metadata(tmp_path: Path) -> None:
    with closing(sqlite3.connect(tmp_path / DATABASE_NAME)) as connection, connection:
        connection.execute("PRAGMA user_version = 1")
        connection.execute(
            """CREATE TABLE runtime_metadata (
            singleton INTEGER PRIMARY KEY, first_started_at TEXT NOT NULL,
            last_started_at TEXT NOT NULL, startup_count INTEGER NOT NULL)"""
        )
        connection.execute(
            "INSERT INTO runtime_metadata VALUES (1, ?, ?, 7)",
            (NOW.isoformat(), NOW.isoformat()),
        )
        connection.execute(
            """CREATE TRIGGER reject_startup BEFORE UPDATE ON runtime_metadata
            BEGIN SELECT RAISE(ABORT, 'synthetic rejection'); END"""
        )
    with pytest.raises(StorageError, match="Cannot initialize SQLite"):
        initialize_state(tmp_path, NOW + timedelta(days=1))
    with closing(sqlite3.connect(tmp_path / DATABASE_NAME)) as connection:
        assert connection.execute("PRAGMA user_version").fetchone() == (1,)
        assert connection.execute("SELECT startup_count FROM runtime_metadata").fetchone() == (7,)
        assert connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall() == [("runtime_metadata",)]


def test_store_requires_initialized_state(tmp_path: Path) -> None:
    with pytest.raises(StorageError, match="initialized"):
        SQLiteStore(tmp_path)
    assert not (tmp_path / DATABASE_NAME).exists()


def test_store_rejects_future_version_without_mutation(store: SQLiteStore) -> None:
    with closing(sqlite3.connect(store.database)) as connection, connection:
        connection.execute("PRAGMA user_version = 100")
    with pytest.raises(StorageError, match="Unsupported state schema"):
        store.read_sites()
    with closing(sqlite3.connect(store.database)) as connection:
        assert connection.execute("PRAGMA user_version").fetchone() == (100,)
        assert connection.execute("SELECT COUNT(*) FROM sites").fetchone() == (1,)


def test_new_sources_are_unknown_and_first_success_does_not_notify(
    store: SQLiteStore, site: SiteConfig
) -> None:
    initial = store.read_site(site.alias)
    assert initial.state.confirmed is SiteState.UNKNOWN
    assert initial.observation is None
    result = store.apply_observation(site, observation(0, True))
    assert result.state.confirmed is SiteState.UP
    assert result.observation == observation(0, True)
    assert store.pending_notifications() == ()


def test_down_and_recovery_commit_ordered_pair(store: SQLiteStore, site: SiteConfig) -> None:
    confirm_down(store, site)
    down = store.read_site(site.alias)
    assert down.state.confirmed is SiteState.DOWN
    assert down.state.incident_started_at == NOW
    pending = store.pending_notifications()
    assert [record.event.kind for record in pending] == [EventKind.DOWN]
    assert pending[0].source_alias == site.alias
    assert pending[0].destination == "operator"
    confirm_recovery(store, site)
    pending = store.pending_notifications()
    assert [record.event.kind for record in pending] == [EventKind.DOWN, EventKind.RECOVERY]
    assert pending[0].event.incident_id == pending[1].event.incident_id
    assert store.read_site(site.alias).state.confirmed is SiteState.UP
    assert store.pending_count() == 2


def test_restart_preserves_snapshot_and_active_incident_without_duplicate_down(
    tmp_path: Path, store: SQLiteStore, site: SiteConfig
) -> None:
    confirm_down(store, site)
    before = store.read_site(site.alias)
    before_pending = store.pending_notifications()
    initialize_state(tmp_path, NOW + timedelta(days=1))
    reopened = SQLiteStore(tmp_path)
    reopened.reconcile((site,))
    assert reopened.read_site(site.alias) == before
    reopened.apply_observation(site, observation(30))
    assert reopened.read_site(site.alias).state.incident_id == before.state.incident_id
    assert reopened.pending_notifications() == before_pending


def test_restart_discards_pending_confirmation_but_keeps_snapshot(
    tmp_path: Path, store: SQLiteStore, site: SiteConfig
) -> None:
    store.apply_observation(site, observation(0))
    store.apply_observation(site, observation(1))
    assert SQLiteStore(tmp_path).read_site(site.alias).state.failures == 2
    store.reconcile((site,))
    restored = store.read_site(site.alias)
    assert restored.state.failures == 0
    assert restored.state.first_failure_at is None
    assert restored.observation == observation(1)
    store.apply_observation(site, observation(20))
    assert store.read_site(site.alias).state.failures == 1
    assert store.pending_count() == 0


def test_gap_reset_preserves_confirmed_incident(store: SQLiteStore, site: SiteConfig) -> None:
    confirm_down(store, site)
    store.apply_observation(site, observation(10, True))
    before = store.read_site(site.alias)
    assert before.state.successes == 1
    store.reset_pending(site.alias)
    after = store.read_site(site.alias)
    assert after.state.successes == 0
    assert after.state.incident_id == before.state.incident_id
    assert after.observation == before.observation
    store.apply_observation(site, observation(30, True))
    assert store.read_site(site.alias).state.confirmed is SiteState.DOWN
    assert store.pending_count() == 1


def test_cached_observation_cannot_advance_confirmation(
    store: SQLiteStore, site: SiteConfig
) -> None:
    first = store.apply_observation(site, observation(0))
    assert store.apply_observation(site, observation(0)) == first
    assert store.read_site(site.alias).state.failures == 1
    assert store.pending_count() == 0


@pytest.mark.parametrize(
    "changes",
    [
        {"url": "https://example.test/changed"},
        {"marker": "healthy"},
        {"status_min": 201},
        {"failure_confirmation": 4},
        {"recovery_confirmation": 3},
    ],
)
def test_changed_configuration_resets_only_affected_source_without_recovery(
    store: SQLiteStore, site: SiteConfig, changes: dict[str, object]
) -> None:
    other = replace(site, alias="other")
    store.reconcile((site, other))
    confirm_down(store, site)
    confirm_down(store, other)
    other_before = store.read_site(other.alias)
    changed = replace(site, **changes)
    store.reconcile((changed, other))
    assert store.read_site(changed.alias).state.confirmed is SiteState.UNKNOWN
    assert store.read_site(changed.alias).observation is None
    assert store.read_site(changed.alias).fingerprint == changed.fingerprint
    assert store.read_site(other.alias) == other_before
    pending = store.pending_notifications()
    assert len(pending) == 1
    assert pending[0].source_alias == other.alias
    assert pending[0].event.kind is EventKind.DOWN


def test_timing_change_preserves_confirmed_state(store: SQLiteStore, site: SiteConfig) -> None:
    confirm_down(store, site)
    before = store.read_site(site.alias)
    store.reconcile((replace(site, poll_interval=60),))
    assert store.read_site(site.alias) == before
    assert store.pending_count() == 1


@pytest.mark.parametrize("disabled", [False, True])
def test_removed_or_disabled_source_cancels_unsent_notifications(
    store: SQLiteStore, site: SiteConfig, disabled: bool
) -> None:
    confirm_down(store, site)
    store.reconcile((replace(site, enabled=False),) if disabled else ())
    assert store.read_sites() == ()
    assert store.pending_notifications() == ()
    with pytest.raises(StorageError, match="not present"):
        store.read_site(site.alias)


@pytest.mark.parametrize("removed", [False, True])
def test_configuration_invalidation_does_not_delete_in_flight_send(
    store: SQLiteStore, site: SiteConfig, removed: bool
) -> None:
    confirm_down(store, site)
    queued = store.pending_notifications()[0]
    store.mark_in_flight(queued.id)
    changed = replace(site, marker="changed")
    store.reconcile(()) if removed else store.reconcile((changed,))
    remaining = store.pending_notifications()
    assert len(remaining) == 1
    assert remaining[0].id == queued.id
    assert remaining[0].in_flight
    store.acknowledge(queued.id, NOW + timedelta(seconds=10))
    assert store.pending_count() == 0


def test_observation_and_outbox_roll_back_together(store: SQLiteStore, site: SiteConfig) -> None:
    store.apply_observation(site, observation(0))
    store.apply_observation(site, observation(1))
    before = store.read_site(site.alias)
    with closing(sqlite3.connect(store.database)) as connection, connection:
        connection.execute(
            """CREATE TRIGGER reject_notification BEFORE INSERT ON notification_outbox
            BEGIN SELECT RAISE(ABORT, 'synthetic write failure'); END"""
        )
    with pytest.raises(StorageError, match="Cannot commit SQLite"):
        store.apply_observation(site, observation(2))
    assert store.read_site(site.alias) == before
    assert store.pending_count() == 0
    with closing(sqlite3.connect(store.database)) as connection:
        assert connection.execute("SELECT COUNT(*) FROM incident_audit").fetchone() == (0,)


def test_policy_programming_errors_are_visible_and_leave_state_unchanged(
    store: SQLiteStore, site: SiteConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    before = store.read_site(site.alias)

    def broken(*args: object, **kwargs: object) -> None:
        raise RuntimeError("synthetic programming error")

    monkeypatch.setattr("sentinel_bot.storage.decide_observation", broken)
    with pytest.raises(RuntimeError, match="synthetic programming error"):
        store.apply_observation(site, observation(0))
    assert store.read_site(site.alias) == before
    assert store.pending_count() == 0


def test_unreconciled_configuration_cannot_commit_observations(
    store: SQLiteStore, site: SiteConfig
) -> None:
    before = store.read_site(site.alias)
    with pytest.raises(StorageError, match="not reconciled"):
        store.apply_observation(replace(site, marker="changed"), observation(0))
    assert store.read_site(site.alias) == before


def test_flapping_coalesces_to_latest_ordered_pair_and_bounds_audit(
    store: SQLiteStore, site: SiteConfig
) -> None:
    for index in range(110):
        confirm_down(store, site, index * 10)
        confirm_recovery(store, site, index * 10 + 5)
    pending = store.pending_notifications()
    assert [record.event.kind for record in pending] == [EventKind.DOWN, EventKind.RECOVERY]
    assert pending[0].event.incident_id == pending[1].event.incident_id
    assert pending[0].event.started_at == NOW + timedelta(seconds=1090)
    with closing(sqlite3.connect(store.database)) as connection:
        assert connection.execute("SELECT COUNT(*) FROM incident_audit").fetchone() == (200,)


def test_flapping_preserves_in_flight_opening_and_latest_pair(
    store: SQLiteStore, site: SiteConfig
) -> None:
    confirm_down(store, site)
    opening = store.pending_notifications()[0]
    store.mark_in_flight(opening.id)
    for index in range(1, 5):
        confirm_recovery(store, site, index * 10)
        confirm_down(store, site, index * 10 + 5)
    confirm_recovery(store, site, 100)
    pending = store.pending_notifications()
    assert len(pending) == 3
    assert pending[0].id == opening.id
    assert pending[0].in_flight
    assert [record.event.kind for record in pending[1:]] == [EventKind.DOWN, EventKind.RECOVERY]
    with pytest.raises(StorageError, match="earlier notification"):
        store.mark_in_flight(pending[1].id)
    store.acknowledge(opening.id, NOW + timedelta(seconds=110))
    store.mark_in_flight(pending[1].id)


def test_acknowledgement_enforces_down_before_recovery_and_is_idempotent(
    store: SQLiteStore, site: SiteConfig
) -> None:
    confirm_down(store, site)
    confirm_recovery(store, site)
    down, recovery = store.pending_notifications()
    with pytest.raises(StorageError, match="earlier notification"):
        store.acknowledge(recovery.id, NOW + timedelta(seconds=20))
    store.acknowledge(down.id, NOW + timedelta(seconds=20))
    store.acknowledge(down.id, NOW + timedelta(seconds=30))
    assert store.pending_count() == 1
    store.acknowledge(recovery.id, NOW + timedelta(seconds=31))
    assert store.pending_count() == 0
    with closing(sqlite3.connect(store.database)) as connection:
        assert connection.execute(
            "SELECT delivered_at FROM notification_outbox WHERE notification_id = ?", (down.id,)
        ).fetchone() == ((NOW + timedelta(seconds=20)).isoformat(),)


def test_failed_delivery_remains_pending(store: SQLiteStore, site: SiteConfig) -> None:
    confirm_down(store, site)
    queued = store.pending_notifications()[0]
    store.mark_in_flight(queued.id)
    store.release_in_flight(queued.id)
    assert store.pending_notifications() == (queued,)
    assert not store.queue_reminder(site, NOW + timedelta(days=1), True)


def test_reminder_clock_begins_at_delivery_and_recovery_cancels_pending_reminder(
    store: SQLiteStore, site: SiteConfig
) -> None:
    confirm_down(store, site)
    accepted = NOW + timedelta(seconds=100)
    assert not store.queue_reminder(site, accepted + timedelta(hours=2), True)
    opening = store.pending_notifications()[0]
    store.acknowledge(opening.id, accepted)
    due = accepted + timedelta(seconds=site.reminder_interval)
    assert not store.queue_reminder(site, due, False)
    assert not store.queue_reminder(site, due - timedelta(microseconds=1), True)
    assert store.queue_reminder(site, due, True)
    assert not store.queue_reminder(site, due + timedelta(hours=1), True)
    reminder = store.pending_notifications()[0]
    assert reminder.event.kind is EventKind.REMINDER
    store.acknowledge(reminder.id, due + timedelta(seconds=10))
    assert not store.queue_reminder(site, due + timedelta(seconds=site.reminder_interval), True)
    assert store.queue_reminder(site, due + timedelta(seconds=site.reminder_interval + 10), True)
    confirm_recovery(store, site, 5000)
    assert [record.event.kind for record in store.pending_notifications()] == [EventKind.RECOVERY]
    assert not store.queue_reminder(site, NOW + timedelta(days=1), True)


def test_completed_history_is_bounded(store: SQLiteStore, site: SiteConfig) -> None:
    for index in range(110):
        confirm_down(store, site, index * 10)
        store.acknowledge(
            store.pending_notifications()[0].id, NOW + timedelta(seconds=index * 10 + 3)
        )
        confirm_recovery(store, site, index * 10 + 5)
        store.acknowledge(
            store.pending_notifications()[0].id, NOW + timedelta(seconds=index * 10 + 7)
        )
    assert store.pending_count() == 0
    with closing(sqlite3.connect(store.database)) as connection:
        assert connection.execute("SELECT COUNT(*) FROM notification_outbox").fetchone() == (200,)
        assert connection.execute("SELECT COUNT(*) FROM incident_audit").fetchone() == (200,)


def test_reminder_delivery_clock_survives_history_pruning(
    store: SQLiteStore, site: SiteConfig
) -> None:
    busy = replace(site, alias="busy")
    store.reconcile((site, busy))
    confirm_down(store, site)
    store.acknowledge(store.pending_notifications()[0].id, NOW + timedelta(seconds=3))
    for index in range(101):
        confirm_down(store, busy, index * 10)
        store.acknowledge(
            store.pending_notifications()[0].id, NOW + timedelta(seconds=index * 10 + 3)
        )
        confirm_recovery(store, busy, index * 10 + 5)
        store.acknowledge(
            store.pending_notifications()[0].id, NOW + timedelta(seconds=index * 10 + 7)
        )
    assert store.queue_reminder(site, NOW + timedelta(seconds=site.reminder_interval + 3), True)
    assert store.pending_notifications()[0].source_alias == site.alias


def test_persisted_fields_are_normalized_and_utc(store: SQLiteStore, site: SiteConfig) -> None:
    offset = timezone(timedelta(hours=4))
    checked = datetime(2026, 1, 1, 4, 0, tzinfo=offset)
    store.apply_observation(site, Observation(checked, CheckResult(False, ErrorKind.TLS)))
    with closing(sqlite3.connect(store.database)) as connection:
        raw = connection.execute("SELECT observation FROM sites").fetchone()[0]
    assert '"checked_at":"2026-01-01T00:00:00+00:00"' in raw
    assert '"error":"tls"' in raw
    assert '"latency_ms":null' in raw
    assert "body" not in raw and "https://" not in raw and "exception" not in raw
    restored = store.read_site(site.alias).observation
    assert restored is not None
    assert restored.checked_at.tzinfo is UTC
    assert restored.result.latency_ms is None


def test_corrupt_snapshot_is_visible(store: SQLiteStore, site: SiteConfig) -> None:
    with closing(sqlite3.connect(store.database)) as connection, connection:
        connection.execute("UPDATE sites SET state = '{}' WHERE alias = ?", (site.alias,))
    with pytest.raises(StorageError, match="snapshot is invalid"):
        store.read_site(site.alias)


def test_inventory_bound_is_checked_before_mutation(store: SQLiteStore, site: SiteConfig) -> None:
    before = store.read_sites()
    with pytest.raises(ValueError, match="20 unique aliases"):
        store.reconcile(tuple(replace(site, alias=f"site{index}") for index in range(21)))
    with pytest.raises(ValueError, match="20 unique aliases"):
        store.reconcile((site, site))
    assert store.read_sites() == before


def test_malformed_current_schema_is_rejected_before_startup_mutation(tmp_path: Path) -> None:
    initialize_state(tmp_path, NOW)
    with closing(sqlite3.connect(tmp_path / DATABASE_NAME)) as connection, connection:
        connection.execute("DROP TABLE sites")
    with pytest.raises(StorageError, match="Cannot initialize SQLite"):
        initialize_state(tmp_path, NOW + timedelta(days=1))
    with closing(sqlite3.connect(tmp_path / DATABASE_NAME)) as connection:
        assert connection.execute("SELECT startup_count FROM runtime_metadata").fetchone() == (1,)


def test_outbox_capacity_rejection_rolls_back_transition(
    store: SQLiteStore, site: SiteConfig
) -> None:
    store.apply_observation(site, observation(0))
    store.apply_observation(site, observation(1))
    before = store.read_site(site.alias)
    with closing(sqlite3.connect(store.database)) as connection, connection:
        for index in range(80):
            connection.execute(
                """INSERT INTO notification_outbox
                (notification_id, source_alias, destination, incident_id, kind, event)
                VALUES (?, 'old-source', 'operator', ?, 'DOWN', '{}')""",
                (f"fixture-{index}", f"incident-{index}"),
            )
    with pytest.raises(StorageError, match="outbox capacity"):
        store.apply_observation(site, observation(2))
    assert store.read_site(site.alias) == before
    assert store.pending_count() == 80


def test_in_flight_capacity_is_bounded_across_removed_source_generations(
    store: SQLiteStore, site: SiteConfig
) -> None:
    first = tuple(replace(site, alias=f"old{index}") for index in range(20))
    store.reconcile(first)
    for item in first:
        confirm_down(store, item)
        store.mark_in_flight(store.pending_notifications()[-1].id)
    store.reconcile((site,))
    confirm_down(store, site, 10)
    current = store.pending_notifications()[-1]
    with pytest.raises(StorageError, match="In-flight notification capacity"):
        store.mark_in_flight(current.id)
    assert not store.pending_notifications()[-1].in_flight
    first_pending = store.pending_notifications()[0]
    store.mark_in_flight(first_pending.id)
    store.acknowledge(first_pending.id, NOW + timedelta(seconds=30))
    store.mark_in_flight(current.id)


def test_missing_legacy_metadata_rolls_back_schema_upgrade(tmp_path: Path) -> None:
    with closing(sqlite3.connect(tmp_path / DATABASE_NAME)) as connection, connection:
        connection.execute("PRAGMA user_version = 1")
        connection.execute(
            """CREATE TABLE runtime_metadata (
            singleton INTEGER PRIMARY KEY, first_started_at TEXT NOT NULL,
            last_started_at TEXT NOT NULL, startup_count INTEGER NOT NULL)"""
        )
    with pytest.raises(StorageError, match="metadata is missing"):
        initialize_state(tmp_path, NOW)
    with closing(sqlite3.connect(tmp_path / DATABASE_NAME)) as connection:
        assert connection.execute("PRAGMA user_version").fetchone() == (1,)
        assert connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall() == [("runtime_metadata",)]


def test_corrupt_observation_and_notification_are_visible(
    store: SQLiteStore, site: SiteConfig
) -> None:
    confirm_down(store, site)
    with closing(sqlite3.connect(store.database)) as connection, connection:
        connection.execute("UPDATE sites SET observation = '{'")
    with pytest.raises(StorageError, match="snapshot is invalid"):
        store.read_sites()
    with closing(sqlite3.connect(store.database)) as connection, connection:
        connection.execute("UPDATE notification_outbox SET event = '{}'")
    with pytest.raises(StorageError, match="notification is invalid"):
        store.pending_notifications()
