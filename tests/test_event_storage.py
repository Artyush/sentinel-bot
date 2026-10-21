import sqlite3
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest

from sentinel_bot.config import IngressSettings, PushSource, SiteConfig
from sentinel_bot.events import EventRejected, IncomingEvent
from sentinel_bot.models import CheckResult, ErrorKind, EventKind, Observation
from sentinel_bot.storage import (
    HISTORY_LIMIT,
    MAX_PENDING_NOTIFICATIONS,
    MAX_PENDING_PUSH_NOTIFICATIONS,
    SQLiteStore,
    StorageError,
    initialize_state,
)

NOW = datetime(2026, 1, 1, tzinfo=UTC)
SOURCE = PushSource("gateway", frozenset({"security.attack", "deploy.finished"}), "x" * 32)
LIMITS = IngressSettings()
ROUTE = "telegram:-100123:0"


def incoming(kind: str = "notification", **changes: object) -> IncomingEvent:
    values: dict[str, object] = {
        "event_id": str(uuid4()),
        "occurred_at": NOW,
        "kind": kind,
        "event_type": "security.attack",
        "severity": "warning",
        "summary": "Synthetic detector report: нагрузка",
        "incident_key": None if kind == "notification" else "occurrence-1",
    }
    values.update(changes)
    return IncomingEvent(**values)


@pytest.fixture
def store(tmp_path: Path) -> SQLiteStore:
    initialize_state(tmp_path, NOW)
    result = SQLiteStore(tmp_path)
    result.configure_telegram(ROUTE, NOW)
    result.reconcile_push_sources((SOURCE,))
    return result


def count(store: SQLiteStore, table: str) -> int:
    assert table in {"event_receipts", "pushed_incidents", "notification_outbox", "push_sources"}
    with closing(sqlite3.connect(store.database)) as connection:
        return connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


def test_one_shot_is_durable_immediate_and_separate_from_polling(store: SQLiteStore) -> None:
    event = incoming()
    receipt = store.accept_event(SOURCE, event, NOW, LIMITS)
    queued = store.pending_notifications()
    assert receipt.event_id == event.event_id
    assert receipt.outcome == "notification"
    assert len(queued) == 1
    assert queued[0].event.kind is EventKind.NOTIFICATION
    assert queued[0].event.summary == event.summary
    assert queued[0].event.event_type == event.event_type
    assert queued[0].event.severity == event.severity
    assert queued[0].event.received_at == NOW
    assert queued[0].event.occurred_at == NOW
    assert queued[0].destination == ROUTE
    assert store.read_sites() == ()
    assert count(store, "pushed_incidents") == 0
    store.mark_in_flight(queued[0].id)
    store.acknowledge(queued[0].id, NOW + timedelta(seconds=1))
    assert store.pending_count() == 0
    assert store.reminder_progress() == ()


def test_duplicate_survives_restart_and_precedes_age_permission_and_capacity(
    store: SQLiteStore,
) -> None:
    event = incoming()
    receipt = store.accept_event(SOURCE, event, NOW, LIMITS)
    reopened = SQLiteStore(store.database.parent)
    disabled_type = replace(SOURCE, allowed_types=frozenset({"different.type"}))
    saturated = replace(LIMITS, max_receipts=0, max_pending=0)
    assert (
        reopened.accept_event(disabled_type, event, NOW + timedelta(days=3), saturated) == receipt
    )
    assert count(store, "event_receipts") == store.pending_count() == 1


@pytest.mark.parametrize("field", ["summary", "severity", "event_type", "kind", "occurred_at"])
def test_changed_duplicate_is_conflict_without_mutation(store: SQLiteStore, field: str) -> None:
    original = incoming()
    store.accept_event(SOURCE, original, NOW, LIMITS)
    values = {
        "summary": "changed",
        "severity": "critical",
        "event_type": "deploy.finished",
        "kind": "resolved",
        "occurred_at": NOW + timedelta(seconds=1),
    }
    with pytest.raises(EventRejected) as error:
        store.accept_event(SOURCE, replace(original, **{field: values[field]}), NOW, LIMITS)
    assert error.value.code == "conflict"
    assert count(store, "event_receipts") == store.pending_count() == 1


def test_event_identity_is_scoped_to_authenticated_source(store: SQLiteStore) -> None:
    event = incoming()
    second = replace(SOURCE, alias="second", token="y" * 32)
    store.accept_event(SOURCE, event, NOW, LIMITS)
    store.accept_event(second, event, NOW, LIMITS)
    assert count(store, "event_receipts") == store.pending_count() == 2


def test_concurrent_retries_commit_exactly_one_logical_delivery(store: SQLiteStore) -> None:
    event = incoming()
    with ThreadPoolExecutor(max_workers=8) as executor:
        receipts = list(
            executor.map(lambda _: store.accept_event(SOURCE, event, NOW, LIMITS), range(16))
        )
    assert all(receipt == receipts[0] for receipt in receipts)
    assert count(store, "event_receipts") == store.pending_count() == 1


@pytest.mark.parametrize(
    ("changes", "code"),
    [
        ({"event_type": "disallowed"}, "event_type"),
        ({"occurred_at": NOW - timedelta(days=1, microseconds=1)}, "event_time"),
        ({"occurred_at": NOW + timedelta(seconds=300, microseconds=1)}, "event_time"),
    ],
)
def test_rejected_new_event_leaves_no_receipt_or_queue(
    store: SQLiteStore, changes: dict[str, object], code: str
) -> None:
    with pytest.raises(EventRejected) as error:
        store.accept_event(SOURCE, incoming(**changes), NOW, LIMITS)
    assert error.value.code == code
    assert count(store, "event_receipts") == store.pending_count() == 0


def test_lifecycle_open_repeat_resolve_and_terminal_no_reopen(store: SQLiteStore) -> None:
    opening = incoming("firing")
    assert store.accept_event(SOURCE, opening, NOW, LIMITS).outcome == "opened"
    assert store.accept_event(SOURCE, incoming("firing"), NOW, LIMITS).outcome == "noop"
    resolution = incoming("resolved", occurred_at=NOW + timedelta(seconds=1))
    assert store.accept_event(SOURCE, resolution, NOW, LIMITS).outcome == "resolved"
    for kind in ("firing", "resolved"):
        assert (
            store.accept_event(
                SOURCE, incoming(kind, occurred_at=NOW + timedelta(seconds=2)), NOW, LIMITS
            ).outcome
            == "noop"
        )
    queued = store.pending_notifications()
    assert [row.event.kind for row in queued] == [EventKind.FIRING, EventKind.RESOLVED]
    assert queued[0].event.incident_id == queued[1].event.incident_id == "occurrence-1"
    assert queued[1].event.started_at == NOW
    assert store.incoming_status((SOURCE.alias,))[0]["active_incidents"] == 0
    first = store.reserve_due_delivery(NOW)
    assert first is not None and first.id == queued[0].id
    assert store.reserve_due_delivery(NOW) is None
    store.acknowledge(first.id, NOW)
    second = store.reserve_due_delivery(NOW)
    assert second is not None and second.id == queued[1].id


def test_resolution_first_is_terminal_without_fabricated_recovery(store: SQLiteStore) -> None:
    assert store.accept_event(SOURCE, incoming("resolved"), NOW, LIMITS).outcome == "noop"
    reopened = SQLiteStore(store.database.parent)
    assert (
        reopened.accept_event(
            SOURCE, incoming("firing", occurred_at=NOW + timedelta(seconds=2)), NOW, LIMITS
        ).outcome
        == "noop"
    )
    assert store.pending_count() == 0
    assert count(store, "pushed_incidents") == 1


@pytest.mark.parametrize("terminal_first", [False, True])
def test_occurrence_type_is_immutable(store: SQLiteStore, terminal_first: bool) -> None:
    store.accept_event(SOURCE, incoming("resolved" if terminal_first else "firing"), NOW, LIMITS)
    with pytest.raises(EventRejected) as error:
        store.accept_event(SOURCE, incoming("resolved", event_type="deploy.finished"), NOW, LIMITS)
    assert error.value.code == "lifecycle"
    assert count(store, "event_receipts") == 1


def test_resolution_before_opening_is_rejected_atomically(store: SQLiteStore) -> None:
    store.accept_event(SOURCE, incoming("firing"), NOW, LIMITS)
    with pytest.raises(EventRejected) as error:
        store.accept_event(
            SOURCE, incoming("resolved", occurred_at=NOW - timedelta(seconds=1)), NOW, LIMITS
        )
    assert error.value.code == "lifecycle"
    assert count(store, "event_receipts") == store.pending_count() == 1
    assert store.incoming_status((SOURCE.alias,))[0]["active_incidents"] == 1


@pytest.mark.parametrize("budget", ["pending", "receipts", "tombstones", "active_incidents"])
@pytest.mark.parametrize("per_source", [False, True])
def test_global_and_source_capacity_failure_rolls_back(
    store: SQLiteStore, budget: str, per_source: bool
) -> None:
    setting = f"max_{budget}" + ("_per_source" if per_source else "")
    limits = replace(LIMITS, **{setting: 1})
    if budget in {"pending", "receipts", "tombstones"}:
        kind = "notification" if budget != "tombstones" else "resolved"
        first = incoming(kind)
        store.accept_event(SOURCE, first, NOW, limits)
        second = incoming(kind, incident_key=None if kind == "notification" else "occurrence-2")
    else:
        first = incoming("firing")
        store.accept_event(SOURCE, first, NOW, limits)
        second = incoming("firing", incident_key="occurrence-2")
    with pytest.raises(EventRejected) as error:
        store.accept_event(SOURCE, second, NOW, limits)
    assert error.value.code == "capacity"
    assert count(store, "event_receipts") == 1
    assert store.accept_event(SOURCE, first, NOW, limits).event_id == first.event_id


@pytest.mark.parametrize("budget", ["pending", "receipts"])
def test_one_shots_cannot_spend_matching_resolution_reservation(
    store: SQLiteStore, budget: str
) -> None:
    limits = replace(LIMITS, **{f"max_{budget}": 3, f"max_{budget}_per_source": 3})
    store.accept_event(SOURCE, incoming("firing"), NOW, limits)
    store.accept_event(SOURCE, incoming(), NOW, limits)
    with pytest.raises(EventRejected) as error:
        store.accept_event(SOURCE, incoming(), NOW, limits)
    assert error.value.code == "capacity"
    assert store.accept_event(SOURCE, incoming("resolved"), NOW, limits).outcome == "resolved"
    assert count(store, "event_receipts") == store.pending_count() == 3


def test_tombstone_capacity_is_reserved_for_active_resolution(store: SQLiteStore) -> None:
    limits = replace(LIMITS, max_tombstones=1, max_tombstones_per_source=1)
    store.accept_event(SOURCE, incoming("firing"), NOW, limits)
    with pytest.raises(EventRejected) as error:
        store.accept_event(SOURCE, incoming("resolved", incident_key="another"), NOW, limits)
    assert error.value.code == "capacity"
    assert store.accept_event(SOURCE, incoming("resolved"), NOW, limits).outcome == "resolved"


def test_existing_resolution_survives_reduced_configured_limits(store: SQLiteStore) -> None:
    store.accept_event(SOURCE, incoming("firing"), NOW, LIMITS)
    reduced = replace(
        LIMITS,
        max_pending=1,
        max_pending_per_source=1,
        max_receipts=1,
        max_receipts_per_source=1,
        max_tombstones=1,
        max_tombstones_per_source=1,
        max_active_incidents=0,
    )
    assert store.accept_event(SOURCE, incoming("resolved"), NOW, reduced).outcome == "resolved"
    assert store.pending_count() == 2


def test_retention_exact_boundary_is_protected_then_expires(store: SQLiteStore) -> None:
    event = incoming("resolved")
    limits = replace(LIMITS, max_receipts=1, max_tombstones=1)
    store.accept_event(SOURCE, event, NOW, limits)
    boundary = NOW + timedelta(seconds=limits.retention)
    with pytest.raises(EventRejected) as error:
        store.accept_event(SOURCE, incoming(occurred_at=boundary), boundary, limits)
    assert error.value.code == "capacity"
    assert count(store, "event_receipts") == count(store, "pushed_incidents") == 1
    later = boundary + timedelta(microseconds=1)
    store.accept_event(SOURCE, incoming(occurred_at=later), later, limits)
    assert count(store, "event_receipts") == 1
    assert count(store, "pushed_incidents") == 0


def test_opening_failure_blocks_resolution_after_history_is_pruned(store: SQLiteStore) -> None:
    store.accept_event(SOURCE, incoming("firing"), NOW, LIMITS)
    opening = store.pending_notifications()[0]
    store.fail_delivery(opening.id, NOW, "forbidden")
    for _ in range(HISTORY_LIMIT + 1):
        store.accept_event(SOURCE, incoming(), NOW, LIMITS)
        store.fail_delivery(store.pending_notifications()[0].id, NOW, "bad_request")
    with closing(sqlite3.connect(store.database)) as connection:
        assert (
            connection.execute(
                "SELECT 1 FROM notification_outbox WHERE notification_id = ?", (opening.id,)
            ).fetchone()
            is None
        )
    store.accept_event(SOURCE, incoming("resolved"), NOW, LIMITS)
    assert store.pending_count() == 0
    with closing(sqlite3.connect(store.database)) as connection:
        assert connection.execute(
            "SELECT blocked_by,last_error FROM notification_outbox WHERE kind = 'RESOLVED'"
        ).fetchone() == (opening.id, "opening_failed")


def test_destination_change_preserves_occurrence_route_and_blocks_recovery(
    store: SQLiteStore,
) -> None:
    store.accept_event(SOURCE, incoming("firing"), NOW, LIMITS)
    opening = store.pending_notifications()[0]
    store.configure_telegram("telegram:-100999:0", NOW)
    store.accept_event(SOURCE, incoming("resolved"), NOW, LIMITS)
    assert store.pending_count() == 0
    with closing(sqlite3.connect(store.database)) as connection:
        assert connection.execute(
            "SELECT destination,blocked_by FROM notification_outbox WHERE kind = 'RESOLVED'"
        ).fetchone() == (ROUTE, opening.id)


def test_accepted_opening_with_changed_route_fails_resolution_visibly(store: SQLiteStore) -> None:
    store.accept_event(SOURCE, incoming("firing"), NOW, LIMITS)
    opening = store.pending_notifications()[0]
    store.acknowledge(opening.id, NOW)
    store.configure_telegram("telegram:-100999:0", NOW)
    store.accept_event(SOURCE, incoming("resolved"), NOW, LIMITS)
    with closing(sqlite3.connect(store.database)) as connection:
        assert connection.execute(
            "SELECT destination,last_error,failed_at FROM notification_outbox WHERE kind = 'RESOLVED'"
        ).fetchone() == (ROUTE, "destination_changed", NOW.isoformat())


def test_first_telegram_binding_updates_push_opening_and_matching_resolution(
    tmp_path: Path,
) -> None:
    initialize_state(tmp_path, NOW)
    store = SQLiteStore(tmp_path)
    store.accept_event(SOURCE, incoming("firing"), NOW, LIMITS)
    store.configure_telegram(ROUTE, NOW)
    store.accept_event(SOURCE, incoming("resolved"), NOW, LIMITS)
    assert [row.destination for row in store.pending_notifications()] == [ROUTE, ROUTE]
    store.configure_telegram(None, NOW)
    assert store.reserve_due_delivery(NOW) is None
    assert store.pending_count() == 2


def test_hybrid_reconciliation_and_site_coalescence_preserve_one_shots(
    store: SQLiteStore,
) -> None:
    site = SiteConfig(
        SOURCE.alias, "https://example.com", failure_confirmation=1, recovery_confirmation=1
    )
    store.reconcile((site,))
    store.accept_event(SOURCE, incoming(), NOW, LIMITS)
    retained = store.pending_notifications()[0].id
    for index, success in enumerate((False, True, False)):
        store.apply_observation(
            site,
            Observation(
                NOW + timedelta(seconds=index),
                CheckResult(success, None if success else ErrorKind.CONNECTION),
            ),
        )
    store.configure_telegram(ROUTE, NOW)
    assert retained in {row.id for row in store.pending_notifications()}
    store.reconcile(())
    assert [row.id for row in store.pending_notifications()] == [retained]


def test_cached_source_status_preserves_times_after_receipt_retention(
    store: SQLiteStore,
) -> None:
    event = incoming(occurred_at=NOW - timedelta(hours=1))
    store.accept_event(SOURCE, event, NOW, LIMITS)
    later = NOW + timedelta(days=3)
    second = replace(SOURCE, alias="second")
    store.accept_event(second, incoming(occurred_at=later), later, LIMITS)
    assert store.incoming_status((SOURCE.alias, "unknown")) == (
        {
            "alias": SOURCE.alias,
            "last_received_at": NOW.isoformat(),
            "last_event_at": event.occurred_at.isoformat(),
            "active_incidents": 0,
            "pending_notifications": 1,
        },
        {
            "alias": "unknown",
            "last_received_at": None,
            "last_event_at": None,
            "active_incidents": 0,
            "pending_notifications": 0,
        },
    )
    store.reconcile_push_sources((second,))
    assert count(store, "push_sources") == 1
    assert store.pending_count() == 2


def test_migration_from_schema_three_preserves_metadata_websites_and_delivery(
    store: SQLiteStore,
) -> None:
    site = SiteConfig("website", "https://example.com", failure_confirmation=1)
    store.reconcile((site,))
    store.apply_observation(site, Observation(NOW, CheckResult(False, ErrorKind.CONNECTION)))
    record = store.pending_notifications()[0]
    with closing(sqlite3.connect(store.database)) as connection, connection:
        for table in (
            "event_receipts",
            "pushed_incidents",
            "push_sources",
            "email_routes",
            "email_channel",
            "email_outbox",
            "email_site_routes",
            "email_push_routes",
            "email_progress",
            "server_snapshots",
        ):
            connection.execute(f"DROP TABLE {table}")
        connection.execute("PRAGMA user_version = 3")
    metadata = initialize_state(store.database.parent, NOW + timedelta(seconds=1))
    assert metadata.schema_version == 6
    assert metadata.startup_count == 2
    reopened = SQLiteStore(store.database.parent)
    assert reopened.pending_notifications() == (record,)
    assert reopened.read_sites()[0].state.incident_id == record.event.incident_id
    assert reopened.accept_event(SOURCE, incoming(), NOW, LIMITS).outcome == "notification"


def test_failed_receipt_insert_rolls_back_lifecycle_and_delivery(store: SQLiteStore) -> None:
    with closing(sqlite3.connect(store.database)) as connection, connection:
        connection.execute(
            """CREATE TRIGGER reject_receipt BEFORE INSERT ON event_receipts
            BEGIN SELECT RAISE(ABORT, 'synthetic rejection'); END"""
        )
    with pytest.raises(StorageError):
        store.accept_event(SOURCE, incoming("firing"), NOW, LIMITS)
    assert count(store, "event_receipts") == count(store, "pushed_incidents") == 0
    assert store.pending_count() == 0
    assert store.incoming_status((SOURCE.alias,))[0]["last_received_at"] is None


@pytest.mark.parametrize("saturated_pool", ["website", "push"])
def test_website_and_push_capacity_are_independent(store: SQLiteStore, saturated_pool: str) -> None:
    site = SiteConfig(
        "website", "https://example.com", failure_confirmation=1, recovery_confirmation=1
    )
    store.reconcile((site,))
    if saturated_pool == "website":
        store.apply_observation(site, Observation(NOW, CheckResult(False, ErrorKind.CONNECTION)))
        first = store.pending_notifications()[0]
        # Protected historical generations can outlive their configured inventory.
        with closing(sqlite3.connect(store.database)) as connection, connection:
            for index in range(MAX_PENDING_NOTIFICATIONS - 1):
                connection.execute(
                    """INSERT INTO notification_outbox
                    (notification_id, source_alias, destination, incident_id, kind, event, attempts)
                    SELECT ?, ?, destination, incident_id, kind, event, 1
                    FROM notification_outbox WHERE notification_id = ?""",
                    (str(uuid4()), f"retired-{index}", first.id),
                )
        assert store.pending_count() == MAX_PENDING_NOTIFICATIONS
        store.accept_event(SOURCE, incoming(), NOW, LIMITS)
        assert store.pending_count() == MAX_PENDING_NOTIFICATIONS + 1
        assert store.pending_notifications()[-1].event.kind is EventKind.NOTIFICATION
    else:
        for source_index in range(5):
            source = replace(SOURCE, alias=f"sender-{source_index}")
            for _ in range(LIMITS.max_pending_per_source):
                store.accept_event(source, incoming(), NOW, LIMITS)
        assert store.pending_count() == MAX_PENDING_PUSH_NOTIFICATIONS
        with pytest.raises(EventRejected) as error:
            store.accept_event(SOURCE, incoming(), NOW, LIMITS)
        assert error.value.code == "capacity"
        store.apply_observation(site, Observation(NOW, CheckResult(False, ErrorKind.CONNECTION)))
        store.apply_observation(site, Observation(NOW + timedelta(seconds=1), CheckResult(True)))
        website = [row for row in store.pending_notifications() if row.source_alias == site.alias]
        assert [row.event.kind for row in website] == [EventKind.DOWN, EventKind.RECOVERY]
        assert website[0].event.incident_id == website[1].event.incident_id
        assert store.pending_count() == MAX_PENDING_PUSH_NOTIFICATIONS + 2


def test_failed_schema_three_ddl_upgrade_rolls_back_all_new_state(
    store: SQLiteStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    with closing(sqlite3.connect(store.database)) as connection, connection:
        for table in (
            "event_receipts",
            "pushed_incidents",
            "push_sources",
            "email_routes",
            "email_channel",
            "email_outbox",
            "email_site_routes",
            "email_push_routes",
            "email_progress",
            "server_snapshots",
        ):
            connection.execute(f"DROP TABLE {table}")
        connection.execute("PRAGMA user_version = 3")
        original_metadata = connection.execute("SELECT * FROM runtime_metadata").fetchall()
        original_schema = connection.execute(
            "SELECT type,name,sql FROM sqlite_master ORDER BY name"
        ).fetchall()

    class RejectLifecycleDDL(sqlite3.Connection):
        def execute(self, sql: str, parameters: object = (), /) -> sqlite3.Cursor:
            if sql.startswith("CREATE TABLE pushed_incidents"):
                raise sqlite3.OperationalError("synthetic lifecycle DDL failure")
            return super().execute(sql, parameters)

    original_connect = sqlite3.connect

    def rejecting_connect(*args: object, **kwargs: object) -> sqlite3.Connection:
        return original_connect(*args, **kwargs, factory=RejectLifecycleDDL)

    with monkeypatch.context() as patch:
        patch.setattr(sqlite3, "connect", rejecting_connect)
        with pytest.raises(StorageError, match="Cannot initialize SQLite"):
            initialize_state(store.database.parent, NOW + timedelta(seconds=1))

    with closing(sqlite3.connect(store.database)) as connection:
        assert connection.execute("PRAGMA user_version").fetchone() == (3,)
        assert connection.execute("SELECT * FROM runtime_metadata").fetchall() == original_metadata
        assert (
            connection.execute("SELECT type,name,sql FROM sqlite_master ORDER BY name").fetchall()
            == original_schema
        )


@pytest.mark.parametrize("delta", [-1, 1])
def test_terminal_occurrence_stays_closed_after_delivery_history_pruning(
    store: SQLiteStore, delta: int
) -> None:
    store.accept_event(SOURCE, incoming("firing"), NOW, LIMITS)
    store.accept_event(SOURCE, incoming("resolved"), NOW, LIMITS)
    lifecycle_ids = tuple(row.id for row in store.pending_notifications())
    for notification_id in lifecycle_ids:
        store.acknowledge(notification_id, NOW)
    for _ in range(HISTORY_LIMIT + 1):
        store.accept_event(SOURCE, incoming(), NOW, LIMITS)
        store.acknowledge(store.pending_notifications()[0].id, NOW)
    with closing(sqlite3.connect(store.database)) as connection:
        for notification_id in lifecycle_ids:
            assert (
                connection.execute(
                    "SELECT 1 FROM notification_outbox WHERE notification_id = ?",
                    (notification_id,),
                ).fetchone()
                is None
            )
    reopened = SQLiteStore(store.database.parent)
    receipt = reopened.accept_event(
        SOURCE, incoming("firing", occurred_at=NOW + timedelta(seconds=delta)), NOW, LIMITS
    )
    assert receipt.outcome == "noop"
    assert reopened.pending_count() == 0
    assert reopened.incoming_status((SOURCE.alias,))[0]["active_incidents"] == 0
