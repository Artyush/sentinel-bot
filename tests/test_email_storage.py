import sqlite3
from contextlib import closing
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest

from sentinel_bot.config import IngressSettings, PushSource, SiteConfig
from sentinel_bot.events import EventRejected, IncomingEvent
from sentinel_bot.models import CheckResult, ErrorKind, EventKind, Observation, SiteState
from sentinel_bot.storage import (
    HISTORY_LIMIT,
    MAX_IN_FLIGHT_EMAIL,
    MAX_PENDING_NOTIFICATIONS,
    OutboxRecord,
    SQLiteStore,
    StorageError,
    initialize_state,
)

NOW = datetime(2026, 1, 1, tzinfo=UTC)
FIRST = "email:" + "a" * 64
SECOND = "email:" + "b" * 64
THIRD = "email:" + "c" * 64
TELEGRAM = "telegram:-100123:0"
SITE = SiteConfig("website", "https://example.com", failure_confirmation=1, recovery_confirmation=1)
SOURCE = PushSource("gateway", frozenset({"security.attack"}), "x" * 32)
LIMITS = IngressSettings()
EMAIL_TABLES = (
    "email_routes",
    "email_channel",
    "email_outbox",
    "email_site_routes",
    "email_push_routes",
    "email_progress",
)


def incoming(kind: str = "notification", **changes: object) -> IncomingEvent:
    values: dict[str, object] = {
        "event_id": str(uuid4()),
        "occurred_at": NOW,
        "kind": kind,
        "event_type": "security.attack",
        "severity": "warning",
        "summary": "Synthetic detector report",
        "incident_key": None if kind == "notification" else "occurrence-1",
    }
    values.update(changes)
    return IncomingEvent(**values)


@pytest.fixture
def store(tmp_path: Path) -> SQLiteStore:
    initialize_state(tmp_path, NOW)
    result = SQLiteStore(tmp_path)
    result.configure_email((FIRST, SECOND), NOW)
    result.reconcile((SITE,))
    result.reconcile_push_sources((SOURCE,))
    return result


def observe(store: SQLiteStore, success: bool, seconds: int = 0) -> None:
    store.apply_observation(
        SITE,
        Observation(
            NOW + timedelta(seconds=seconds),
            CheckResult(success, None if success else ErrorKind.CONNECTION),
        ),
    )


def count(store: SQLiteStore, table: str) -> int:
    assert table in {*EMAIL_TABLES, "notification_outbox", "event_receipts", "pushed_incidents"}
    with closing(sqlite3.connect(store.database)) as connection:
        return connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


def reserve(store: SQLiteStore, destination: str, seconds: int = 0) -> OutboxRecord:
    candidates = [
        record.id
        for record in store.pending_email_notifications()
        if record.destination == destination
    ]
    record = store.reserve_due_email_delivery(
        NOW + timedelta(seconds=seconds), ready_ids=candidates
    )
    assert record is not None
    assert record.destination == destination
    return record


def downgrade_four(store: SQLiteStore) -> None:
    with closing(sqlite3.connect(store.database)) as connection, connection:
        for table in (*EMAIL_TABLES, "server_snapshots"):
            connection.execute(f"DROP TABLE {table}")
        connection.execute("PRAGMA user_version = 4")


def test_schema_four_upgrade_preserves_existing_channel_events_and_metadata(
    store: SQLiteStore,
) -> None:
    store.configure_telegram(TELEGRAM, NOW)
    observe(store, False)
    store.accept_event(SOURCE, incoming("firing"), NOW, LIMITS)
    before = store.pending_notifications()
    with closing(sqlite3.connect(store.database)) as connection:
        receipts = connection.execute("SELECT * FROM event_receipts").fetchall()
        incidents = connection.execute("SELECT * FROM pushed_incidents").fetchall()
    downgrade_four(store)
    metadata = initialize_state(store.database.parent, NOW + timedelta(days=1))
    assert metadata.schema_version == 6
    assert metadata.startup_count == 2
    assert metadata.first_started_at == NOW.isoformat()
    assert store.pending_notifications() == before
    assert store.read_site(SITE.alias).state.confirmed is SiteState.DOWN
    assert store.pending_email_notifications() == ()
    with closing(sqlite3.connect(store.database)) as connection:
        assert connection.execute("SELECT * FROM event_receipts").fetchall() == receipts
        assert connection.execute("SELECT * FROM pushed_incidents").fetchall() == incidents


def test_schema_five_migration_rolls_back_all_tables_on_startup_failure(store: SQLiteStore) -> None:
    downgrade_four(store)
    with closing(sqlite3.connect(store.database)) as connection, connection:
        connection.execute(
            "CREATE TRIGGER reject_startup BEFORE UPDATE ON runtime_metadata "
            "BEGIN SELECT RAISE(ABORT, 'synthetic rejection'); END"
        )
    with pytest.raises(StorageError, match="Cannot initialize SQLite"):
        initialize_state(store.database.parent, NOW)
    with closing(sqlite3.connect(store.database)) as connection:
        assert connection.execute("PRAGMA user_version").fetchone() == (4,)
        assert connection.execute("SELECT startup_count FROM runtime_metadata").fetchone() == (1,)
        tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master")}
        assert not tables.intersection(EMAIL_TABLES)


@pytest.mark.parametrize("pushed", [False, True])
def test_email_only_creates_no_logical_telegram_deliveries(
    store: SQLiteStore, pushed: bool
) -> None:
    if pushed:
        store.accept_event(SOURCE, incoming("firing"), NOW, LIMITS)
        store.accept_event(
            SOURCE, incoming("resolved", occurred_at=NOW + timedelta(seconds=1)), NOW, LIMITS
        )
    else:
        observe(store, False)
        observe(store, True, 1)
    assert store.pending_notifications() == ()
    assert store.pending_count() == 4
    assert store.email_pending_count() == 4
    assert {record.destination for record in store.pending_email_notifications()} == {FIRST, SECOND}


def test_partial_recipient_acceptance_is_durable_and_does_not_resend(store: SQLiteStore) -> None:
    store.accept_event(SOURCE, incoming(), NOW, LIMITS)
    first = reserve(store, FIRST)
    store.acknowledge_email(first.id, NOW)
    second = reserve(store, SECOND)
    store.retry_email_delivery(second.id, NOW + timedelta(seconds=30), "network", delay_seconds=30)
    reopened = SQLiteStore(store.database.parent)
    reopened.configure_email((FIRST, SECOND), NOW + timedelta(seconds=1))
    assert reopened.email_delivery_status(FIRST)["accepted"] == 1
    assert reopened.email_delivery_status(SECOND)["retrying"] == 1
    assert [record.destination for record in reopened.pending_email_notifications()] == [SECOND]
    assert reopened.reserve_due_email_delivery(NOW + timedelta(seconds=29)) is None
    retry = reopened.reserve_due_email_delivery(NOW + timedelta(seconds=30))
    assert retry is not None and retry.id == second.id and retry.attempts == 2
    reopened.acknowledge_email(retry.id, NOW + timedelta(seconds=31))
    assert reopened.email_delivery_status()["accepted"] == 2
    assert reopened.reserve_due_email_delivery(NOW + timedelta(days=1)) is None


def test_email_schedule_reads_only_ordered_pending_metadata_without_decoding_payloads(
    store: SQLiteStore,
) -> None:
    observe(store, False)
    first = reserve(store, FIRST)
    store.retry_email_delivery(first.id, NOW + timedelta(seconds=30), "network", delay_seconds=30)
    second = reserve(store, SECOND)
    before = store.pending_email_notifications()
    with closing(sqlite3.connect(store.database)) as connection, connection:
        connection.execute("UPDATE email_outbox SET event = 'synthetic invalid event JSON'")
    schedule = store.email_pending_schedule()
    assert [
        (item.id, item.destination, item.in_flight, item.attempts, item.retry_delay_seconds)
        for item in schedule
    ] == [
        (item.id, item.destination, item.in_flight, item.attempts, item.retry_delay_seconds)
        for item in before
    ]
    assert schedule[0].id == first.id
    assert schedule[1].id == second.id
    assert not hasattr(schedule[0], "event")
    with pytest.raises(
        StorageError, match="Stored state is invalid|Stored notification is invalid|JSON"
    ):
        store.pending_email_notifications()
    store.fail_email_delivery(first.id, NOW, "network")
    assert [item.id for item in store.email_pending_schedule()] == [second.id]


def test_email_schedule_rejects_rows_beyond_the_global_pending_bound(store: SQLiteStore) -> None:
    observe(store, False)
    template = store.pending_email_notifications()[0]
    with closing(sqlite3.connect(store.database)) as connection, connection:
        connection.executemany(
            """INSERT INTO email_outbox
            (notification_id,source_alias,destination,incident_id,kind,event)
            SELECT ?,source_alias,destination,incident_id,kind,event
            FROM email_outbox WHERE notification_id = ?""",
            ((str(uuid4()), template.id) for _ in range(5400)),
        )
    with pytest.raises(StorageError, match="exceeds its capacity"):
        store.email_pending_schedule()


@pytest.mark.parametrize("pushed", [False, True])
def test_failure_and_ordering_are_independent_per_recipient(
    store: SQLiteStore, pushed: bool
) -> None:
    if pushed:
        store.accept_event(SOURCE, incoming("firing"), NOW, LIMITS)
        store.accept_event(
            SOURCE, incoming("resolved", occurred_at=NOW + timedelta(seconds=1)), NOW, LIMITS
        )
    else:
        observe(store, False)
        observe(store, True, 1)
    failed = reserve(store, FIRST)
    store.fail_email_delivery(failed.id, NOW, "invalid_recipient")
    assert store.email_delivery_status(FIRST)["failed"] == 1
    assert store.email_delivery_status(FIRST)["blocked"] == 1
    other_opening = reserve(store, SECOND)
    store.acknowledge_email(other_opening.id, NOW)
    other_resolution = reserve(store, SECOND)
    assert other_resolution.event.kind in (EventKind.RECOVERY, EventKind.RESOLVED)
    store.acknowledge_email(other_resolution.id, NOW)
    assert store.email_delivery_status(SECOND)["accepted"] == 2
    assert store.reserve_due_email_delivery(NOW + timedelta(days=1)) is None


def test_retrying_one_recipient_does_not_block_another_recipient_or_telegram(
    store: SQLiteStore,
) -> None:
    store.configure_telegram(TELEGRAM, NOW)
    observe(store, False)
    first = reserve(store, FIRST)
    store.retry_email_delivery(first.id, NOW + timedelta(minutes=1), "network")
    second = store.reserve_due_email_delivery(NOW)
    telegram = store.reserve_due_delivery(NOW)
    assert second is not None and second.destination == SECOND
    assert telegram is not None and telegram.destination == TELEGRAM
    store.acknowledge_email(second.id, NOW)
    store.acknowledge(telegram.id, NOW)
    assert store.delivery_status()["accepted"] == 1
    assert store.email_delivery_status()["accepted"] == 1


@pytest.mark.parametrize("pushed", [False, True])
def test_new_recipient_does_not_receive_an_orphan_resolution(
    store: SQLiteStore, pushed: bool
) -> None:
    store.configure_email((FIRST,), NOW)
    if pushed:
        store.accept_event(SOURCE, incoming("firing"), NOW, LIMITS)
    else:
        observe(store, False)
    opening = reserve(store, FIRST)
    store.acknowledge_email(opening.id, NOW)
    store.configure_email((FIRST, SECOND), NOW)
    if pushed:
        store.accept_event(
            SOURCE, incoming("resolved", occurred_at=NOW + timedelta(seconds=1)), NOW, LIMITS
        )
    else:
        observe(store, True, 1)
    assert [record.destination for record in store.pending_email_notifications()] == [FIRST]
    assert store.email_delivery_status(SECOND)["accepted"] == 0


@pytest.mark.parametrize("pushed", [False, True])
def test_removed_recipient_is_failed_without_rerouting_recovery(
    store: SQLiteStore, pushed: bool
) -> None:
    if pushed:
        store.accept_event(SOURCE, incoming("firing"), NOW, LIMITS)
    else:
        observe(store, False)
    store.configure_email((THIRD,), NOW)
    if pushed:
        store.accept_event(
            SOURCE, incoming("resolved", occurred_at=NOW + timedelta(seconds=1)), NOW, LIMITS
        )
    else:
        observe(store, True, 1)
    assert store.email_delivery_status()["failed"] == 2
    assert store.email_delivery_status()["blocked"] == 2
    assert store.email_delivery_status(THIRD)["pending"] == 0
    assert store.reserve_due_email_delivery(NOW) is None


@pytest.mark.parametrize("pushed", [False, True])
def test_email_disable_pauses_existing_opening_and_matching_resolution(
    store: SQLiteStore, pushed: bool
) -> None:
    if pushed:
        store.accept_event(SOURCE, incoming("firing"), NOW, LIMITS)
    else:
        observe(store, False)
    store.configure_email((), NOW)
    if pushed:
        store.accept_event(
            SOURCE, incoming("resolved", occurred_at=NOW + timedelta(seconds=1)), NOW, LIMITS
        )
    else:
        observe(store, True, 1)
    assert store.email_delivery_status()["paused"] is True
    assert len(store.pending_email_notifications()) == 4
    assert store.reserve_due_email_delivery(NOW) is None
    store.configure_email((FIRST, SECOND), NOW)
    assert store.email_delivery_status()["paused"] is False
    assert store.reserve_due_email_delivery(NOW) is not None


@pytest.mark.parametrize("pushed", [False, True])
def test_old_telegram_incident_does_not_backfill_new_email_route(
    store: SQLiteStore, pushed: bool
) -> None:
    store.configure_email((), NOW)
    store.configure_telegram(TELEGRAM, NOW)
    if pushed:
        store.accept_event(SOURCE, incoming("firing"), NOW, LIMITS)
    else:
        observe(store, False)
    store.configure_email((FIRST, SECOND), NOW)
    if pushed:
        store.accept_event(
            SOURCE, incoming("resolved", occurred_at=NOW + timedelta(seconds=1)), NOW, LIMITS
        )
    else:
        observe(store, True, 1)
    assert store.pending_email_notifications() == ()
    assert len(store.pending_notifications()) == 2


@pytest.mark.parametrize("pushed", [False, True])
def test_email_only_incident_is_not_backfilled_when_telegram_enabled(
    store: SQLiteStore, pushed: bool
) -> None:
    if pushed:
        store.accept_event(SOURCE, incoming("firing"), NOW, LIMITS)
    else:
        observe(store, False)
    store.configure_telegram(TELEGRAM, NOW)
    if pushed:
        store.accept_event(
            SOURCE, incoming("resolved", occurred_at=NOW + timedelta(seconds=1)), NOW, LIMITS
        )
    else:
        observe(store, True, 1)
    assert store.pending_notifications() == ()
    assert len(store.pending_email_notifications()) == 4


def test_disabled_telegram_keeps_its_existing_opening_route_without_backfilling_email(
    store: SQLiteStore,
) -> None:
    store.configure_email((), NOW)
    store.configure_telegram(TELEGRAM, NOW)
    observe(store, False)
    original = store.pending_notifications()[0]
    store.configure_telegram(None, NOW)
    store.configure_email((FIRST, SECOND), NOW)
    observe(store, True, 1)
    queued = store.pending_notifications()
    assert queued[0].id == original.id
    assert [record.event.kind for record in queued] == [EventKind.DOWN, EventKind.RECOVERY]
    assert {record.destination for record in queued} == {TELEGRAM}
    assert store.pending_email_notifications() == ()
    assert store.reserve_due_delivery(NOW) is None


def test_original_unbound_telegram_operator_route_remains_compatible_without_email(
    store: SQLiteStore,
) -> None:
    store.configure_email((), NOW)
    store.configure_telegram(None, NOW)
    observe(store, False)
    observe(store, True, 1)
    queued = store.pending_notifications()
    assert len(queued) == 2
    assert {record.destination for record in queued} == {"operator"}
    store.configure_telegram(TELEGRAM, NOW)
    assert [record.event.kind for record in store.pending_notifications()] == [
        EventKind.DOWN,
        EventKind.RECOVERY,
    ]
    assert {record.destination for record in store.pending_notifications()} == {TELEGRAM}


def test_current_recipient_does_not_spend_removed_opening_route_reservations(
    store: SQLiteStore,
) -> None:
    store.configure_email((FIRST,), NOW)
    store.accept_event(SOURCE, incoming("firing"), NOW, LIMITS)
    record = reserve(store, FIRST)
    store.acknowledge_email(record.id, NOW)
    store.configure_email((SECOND,), NOW)
    limits = replace(LIMITS, max_pending=1, max_pending_per_source=1)
    store.accept_event(SOURCE, incoming(), NOW, limits)
    assert store.email_delivery_status(SECOND)["pending"] == 1
    store.accept_event(
        SOURCE, incoming("resolved", occurred_at=NOW + timedelta(seconds=1)), NOW, limits
    )
    assert store.email_delivery_status(FIRST)["failed"] == 1
    assert store.email_delivery_status(SECOND)["pending"] == 1


def test_each_source_has_an_independent_per_recipient_push_budget(store: SQLiteStore) -> None:
    limits = replace(LIMITS, max_pending=10, max_pending_per_source=2)
    for _ in range(2):
        store.accept_event(SOURCE, incoming(), NOW, limits)
    with pytest.raises(EventRejected) as rejected:
        store.accept_event(SOURCE, incoming(), NOW, limits)
    assert rejected.value.code == "capacity"
    second_source = replace(SOURCE, alias="second-source")
    store.accept_event(second_source, incoming(), NOW, limits)
    assert store.email_delivery_status(FIRST)["pending"] == 3
    assert store.email_delivery_status(SECOND)["pending"] == 3


def test_interrupted_send_retries_same_identity_and_destination(store: SQLiteStore) -> None:
    observe(store, False)
    first = reserve(store, FIRST)
    assert first.attempts == 1 and first.in_flight
    assert store.configure_email((FIRST, SECOND), NOW + timedelta(seconds=5)) == 1
    retry = reserve(store, FIRST, 5)
    assert retry.id == first.id and retry.attempts == 2
    assert retry.last_error == "interrupted"


def test_unknown_prior_attempt_and_in_flight_records_survive_coalescing(store: SQLiteStore) -> None:
    observe(store, False)
    first = reserve(store, FIRST)
    second = reserve(store, SECOND)
    store.retry_email_delivery(first.id, NOW + timedelta(minutes=1), "timeout")
    observe(store, True, 1)
    observe(store, False, 2)
    saved = {record.id: record for record in store.pending_email_notifications()}
    assert saved[first.id].attempts == 1
    assert saved[second.id].in_flight
    assert len(saved) == 4


@pytest.mark.parametrize("pushed", [False, True])
def test_failed_opening_blocks_resolution_even_after_terminal_history_pruning(
    store: SQLiteStore, pushed: bool
) -> None:
    store.configure_email((FIRST,), NOW)
    if pushed:
        store.accept_event(SOURCE, incoming("firing"), NOW, LIMITS)
    else:
        observe(store, False)
    failed = reserve(store, FIRST)
    store.fail_email_delivery(failed.id, NOW, "rejected")
    for _ in range(HISTORY_LIMIT + 1):
        store.accept_event(SOURCE, incoming(), NOW, LIMITS)
        record = reserve(store, FIRST)
        store.acknowledge_email(record.id, NOW)
    with closing(sqlite3.connect(store.database)) as connection:
        assert (
            connection.execute(
                "SELECT 1 FROM email_outbox WHERE notification_id = ?", (failed.id,)
            ).fetchone()
            is None
        )
    if pushed:
        store.accept_event(
            SOURCE, incoming("resolved", occurred_at=NOW + timedelta(seconds=1)), NOW, LIMITS
        )
    else:
        observe(store, True, 1)
    assert store.email_delivery_status()["blocked"] == 1
    assert count(store, "email_outbox") == HISTORY_LIMIT


def test_reminders_use_separate_accepted_recipient_clocks_and_freshness(store: SQLiteStore) -> None:
    observe(store, False)
    first = reserve(store, FIRST)
    store.acknowledge_email(first.id, NOW)
    later = NOW + timedelta(seconds=20)
    second = reserve(store, SECOND, 20)
    store.acknowledge_email(second.id, later)
    progress = {
        record.destination: record.last_delivered_at for record in store.email_reminder_progress()
    }
    assert progress == {FIRST: NOW, SECOND: later}
    due = NOW + timedelta(seconds=SITE.reminder_interval)
    assert not store.queue_email_reminder(SITE, FIRST, due, False)
    assert not store.queue_email_reminder(SITE, SECOND, due, True)
    assert store.queue_email_reminder(SITE, FIRST, due, True)
    assert not store.queue_email_reminder(SITE, FIRST, due, True)
    assert store.queue_email_reminder(SITE, SECOND, due, True, due=True)
    assert not store.queue_email_reminder(SITE, SECOND, due, True, due=False)


def test_failed_reminder_suppresses_only_its_recipient_but_allows_recovery(
    store: SQLiteStore,
) -> None:
    observe(store, False)
    for destination in (FIRST, SECOND):
        record = reserve(store, destination)
        store.acknowledge_email(record.id, NOW)
    due = NOW + timedelta(seconds=SITE.reminder_interval)
    assert store.queue_email_reminder(SITE, FIRST, due, True, due=True)
    reminder = reserve(store, FIRST)
    store.fail_email_delivery(reminder.id, due, "retries_exhausted")
    assert not store.queue_email_reminder(SITE, FIRST, due, True, due=True)
    assert store.queue_email_reminder(SITE, SECOND, due, True, due=True)
    observe(store, True, 1)
    assert {record.event.kind for record in store.pending_email_notifications()} == {
        EventKind.RECOVERY
    }


def test_unaccepted_opening_never_starts_a_reminder_clock(store: SQLiteStore) -> None:
    observe(store, False)
    assert store.email_reminder_progress() == ()
    assert not store.queue_email_reminder(SITE, FIRST, NOW + timedelta(days=1), True, due=True)


def test_full_email_pool_defers_optional_reminder_without_blocking_other_recipient(
    store: SQLiteStore,
) -> None:
    observe(store, False)
    first = reserve(store, FIRST)
    store.acknowledge_email(first.id, NOW)
    second = reserve(store, SECOND)
    store.acknowledge_email(second.id, NOW)
    with closing(sqlite3.connect(store.database)) as connection, connection:
        for _ in range(MAX_PENDING_NOTIFICATIONS):
            connection.execute(
                """INSERT INTO email_outbox
                (notification_id,source_alias,destination,incident_id,kind,event)
                SELECT ?,source_alias,destination,incident_id,kind,event
                FROM email_outbox WHERE notification_id = ?""",
                (str(uuid4()), first.id),
            )
    due = NOW + timedelta(seconds=SITE.reminder_interval)
    assert not store.queue_email_reminder(SITE, FIRST, due, True, due=True)
    assert store.queue_email_reminder(SITE, SECOND, due, True, due=True)
    assert store.read_site(SITE.alias).state.confirmed is SiteState.DOWN
    assert store.email_delivery_status(FIRST)["pending"] == MAX_PENDING_NOTIFICATIONS
    assert store.email_delivery_status(SECOND)["pending"] == 1


@pytest.mark.parametrize("pushed", [False, True])
def test_destination_insert_failure_rolls_back_every_channel_and_state(
    store: SQLiteStore, pushed: bool
) -> None:
    store.configure_telegram(TELEGRAM, NOW)
    with closing(sqlite3.connect(store.database)) as connection, connection:
        connection.execute(
            "CREATE TRIGGER reject_recipient BEFORE INSERT ON email_outbox "
            f"WHEN NEW.destination = '{SECOND}' "
            "BEGIN SELECT RAISE(ABORT, 'synthetic recipient failure'); END"
        )
    with pytest.raises(StorageError):
        if pushed:
            store.accept_event(SOURCE, incoming("firing"), NOW, LIMITS)
        else:
            observe(store, False)
    assert store.pending_count() == 0
    assert count(store, "event_receipts") == 0
    assert count(store, "pushed_incidents") == 0
    assert count(store, "email_site_routes") == 0
    assert count(store, "email_push_routes") == 0
    assert store.read_site(SITE.alias).state.confirmed is SiteState.UNKNOWN


def test_pending_push_capacity_reserves_each_recipient_resolution(store: SQLiteStore) -> None:
    limits = replace(LIMITS, max_pending=4, max_pending_per_source=4)
    store.accept_event(SOURCE, incoming("firing"), NOW, limits)
    for _ in range(2):
        store.accept_event(SOURCE, incoming(), NOW, limits)
    rejected = incoming()
    with pytest.raises(EventRejected) as error:
        store.accept_event(SOURCE, rejected, NOW, limits)
    assert error.value.code == "capacity"
    assert count(store, "event_receipts") == 3
    store.accept_event(
        SOURCE, incoming("resolved", occurred_at=NOW + timedelta(seconds=1)), NOW, limits
    )
    assert store.email_pending_count() == 8
    assert store.incoming_status((SOURCE.alias,))[0]["pending_notifications"] == 8


def test_full_recipient_queue_rejects_fanout_before_accepting_other_recipient(
    store: SQLiteStore,
) -> None:
    limits = replace(LIMITS, max_pending=2, max_pending_per_source=2)
    for _ in range(2):
        store.accept_event(SOURCE, incoming(), NOW, limits)
        record = reserve(store, FIRST)
        store.acknowledge_email(record.id, NOW)
    assert store.email_delivery_status(FIRST)["pending"] == 0
    assert store.email_delivery_status(SECOND)["pending"] == 2
    with pytest.raises(EventRejected):
        store.accept_event(SOURCE, incoming(), NOW, limits)
    assert count(store, "event_receipts") == 2
    assert store.email_delivery_status(FIRST)["pending"] == 0


def test_website_and_push_pending_budgets_are_separate(store: SQLiteStore) -> None:
    store.configure_email((FIRST,), NOW)
    observe(store, False)
    template = store.pending_email_notifications()[0]
    with closing(sqlite3.connect(store.database)) as connection, connection:
        for _ in range(MAX_PENDING_NOTIFICATIONS - 1):
            connection.execute(
                """INSERT INTO email_outbox
                (notification_id,source_alias,destination,incident_id,kind,event)
                SELECT ?,source_alias,destination,incident_id,kind,event
                FROM email_outbox WHERE notification_id = ?""",
                (str(uuid4()), template.id),
            )
    with pytest.raises(StorageError, match="capacity"):
        observe(store, True, 1)
    assert store.read_site(SITE.alias).state.confirmed is SiteState.DOWN
    store.accept_event(SOURCE, incoming(), NOW, LIMITS)
    assert store.email_pending_count() == MAX_PENDING_NOTIFICATIONS + 1


def test_retained_duplicate_is_returned_without_new_email_records(store: SQLiteStore) -> None:
    event = incoming()
    receipt = store.accept_event(SOURCE, event, NOW, LIMITS)
    store.configure_email((THIRD,), NOW)
    assert store.accept_event(SOURCE, event, NOW + timedelta(days=3), LIMITS) == receipt
    assert count(store, "email_outbox") == 2
    assert store.email_delivery_status(THIRD)["pending"] == 0


def test_source_reconciliation_cancels_only_never_attempted_polled_records(
    store: SQLiteStore,
) -> None:
    observe(store, False)
    protected = reserve(store, FIRST)
    store.retry_email_delivery(protected.id, NOW + timedelta(minutes=1), "network")
    store.accept_event(SOURCE, incoming(), NOW, LIMITS)
    changed = replace(SITE, url="https://example.org")
    store.reconcile((changed,))
    pending = store.pending_email_notifications()
    assert protected.id in {record.id for record in pending}
    assert len(pending) == 3
    assert count(store, "email_site_routes") == 0
    assert store.email_reminder_progress() == ()


def test_terminal_occurrence_pruning_also_bounds_email_routing(store: SQLiteStore) -> None:
    event = incoming("firing")
    store.accept_event(SOURCE, event, NOW, LIMITS)
    store.accept_event(
        SOURCE, incoming("resolved", occurred_at=NOW + timedelta(seconds=1)), NOW, LIMITS
    )
    assert count(store, "email_push_routes") == 2
    later = NOW + timedelta(seconds=LIMITS.retention + 2)
    store.accept_event(SOURCE, incoming(occurred_at=later), later, LIMITS)
    assert count(store, "email_push_routes") == 0


def test_email_channel_pause_error_and_destination_status_are_private(store: SQLiteStore) -> None:
    store.set_email_pause(NOW + timedelta(seconds=60), 60)
    store.set_email_error("quota")
    assert store.get_email_pause() == (NOW + timedelta(seconds=60), 60)
    assert store.get_send_pause() is None
    assert store.email_delivery_status()["paused"] is True
    assert store.email_delivery_status(FIRST)["last_error"] == "quota"
    assert FIRST not in str(store.email_delivery_status(FIRST))
    assert "@" not in str(store.email_delivery_status())
    assert SQLiteStore(store.database.parent).get_email_pause() == store.get_email_pause()
    store.clear_email_pause()
    store.set_email_error(None)
    assert store.get_email_pause() is None
    assert store.email_delivery_status()["paused"] is False
    assert store.email_delivery_status()["last_error"] is None


def test_provider_suspension_preserves_backlog_until_explicit_configuration_restart(
    store: SQLiteStore,
) -> None:
    store.accept_event(SOURCE, incoming(), NOW, LIMITS)
    record = reserve(store, FIRST)
    store.retry_email_delivery(record.id, NOW, "unauthorized", delay_seconds=0)
    store.set_email_error("unauthorized")
    store.set_email_suspended(True)
    assert store.email_delivery_status()["paused"] is True
    assert store.reserve_due_email_delivery(NOW + timedelta(days=1)) is None
    assert len(store.pending_email_notifications()) == 2
    reopened = SQLiteStore(store.database.parent)
    assert reopened.email_delivery_status()["paused"] is True
    reopened.set_email_pause(NOW + timedelta(minutes=1), 60)
    reopened.configure_email((FIRST, SECOND), NOW)
    assert reopened.get_email_pause() == (NOW + timedelta(minutes=1), 60)
    reopened.clear_email_pause()
    assert reopened.email_delivery_status()["paused"] is False
    retry = reserve(reopened, FIRST)
    assert retry.id == record.id


def test_missing_email_metadata_is_rejected_at_startup(store: SQLiteStore) -> None:
    with closing(sqlite3.connect(store.database)) as connection, connection:
        connection.execute("DELETE FROM email_channel")
    with pytest.raises(StorageError, match="Email channel metadata is missing"):
        initialize_state(store.database.parent, NOW)


def test_resolution_after_recipient_removal_stays_terminal_even_if_opening_was_accepted(
    store: SQLiteStore,
) -> None:
    store.configure_email((FIRST,), NOW)
    store.accept_event(SOURCE, incoming("firing"), NOW, LIMITS)
    opening = reserve(store, FIRST)
    store.acknowledge_email(opening.id, NOW)
    store.configure_email((SECOND,), NOW)
    store.accept_event(
        SOURCE, incoming("resolved", occurred_at=NOW + timedelta(seconds=1)), NOW, LIMITS
    )
    assert store.email_delivery_status(FIRST)["failed"] == 1
    assert store.email_delivery_status(SECOND)["pending"] == 0
    assert store.reserve_due_email_delivery(NOW) is None


@pytest.mark.parametrize("pushed", [False, True])
def test_disabling_email_does_not_revive_an_earlier_removed_recipient(
    store: SQLiteStore, pushed: bool
) -> None:
    store.configure_email((FIRST,), NOW)
    if pushed:
        store.accept_event(SOURCE, incoming("firing"), NOW, LIMITS)
    else:
        observe(store, False)
    opening = reserve(store, FIRST)
    store.acknowledge_email(opening.id, NOW)
    store.configure_email((SECOND,), NOW)
    store.configure_email((), NOW)
    if pushed:
        store.accept_event(
            SOURCE, incoming("resolved", occurred_at=NOW + timedelta(seconds=1)), NOW, LIMITS
        )
    else:
        observe(store, True, 1)
    assert store.email_pending_schedule() == ()
    assert store.email_delivery_status(FIRST)["failed"] == 1
    assert store.email_delivery_status(FIRST)["last_error"] == "destination_changed"


@pytest.mark.parametrize("error", ["raw transport text", "person@example.com", ""])
def test_unnormalized_email_errors_are_rejected_without_persistence(
    store: SQLiteStore, error: str
) -> None:
    observe(store, False)
    record = reserve(store, FIRST)
    with pytest.raises(ValueError, match="normalized category"):
        store.retry_email_delivery(record.id, NOW, error)
    with pytest.raises(ValueError, match="normalized category"):
        store.fail_email_delivery(record.id, NOW, error)
    with pytest.raises(ValueError, match="normalized category"):
        store.set_email_error(error)
    assert store.email_delivery_status()["last_error"] is None


def test_retry_readiness_uses_explicit_monotonic_candidates_without_changing_identity(
    store: SQLiteStore,
) -> None:
    observe(store, False)
    record = reserve(store, FIRST)
    store.retry_email_delivery(
        record.id, NOW + timedelta(days=1), "rate_limit", delay_seconds=86400
    )
    assert store.reserve_due_email_delivery(NOW, ready_ids=()) is None
    retry = store.reserve_due_email_delivery(NOW, ready_ids=(record.id,))
    assert retry is not None and retry.id == record.id and retry.attempts == 2


def test_too_many_email_delivery_candidates_are_rejected_before_transaction(
    store: SQLiteStore,
) -> None:
    with pytest.raises(ValueError, match="Too many email"):
        store.reserve_due_email_delivery(NOW, ready_ids=("synthetic",) * 5401)


def test_no_pushed_reminder_clock_is_created_by_acceptance(store: SQLiteStore) -> None:
    store.accept_event(SOURCE, incoming("firing"), NOW, LIMITS)
    for destination in (FIRST, SECOND):
        record = reserve(store, destination)
        store.acknowledge_email(record.id, NOW)
    assert store.email_reminder_progress() == ()


def test_email_route_inventory_remains_bounded_after_repeated_configuration_changes(
    store: SQLiteStore,
) -> None:
    for index in range(10):
        destination = "email:" + f"{index:064x}"
        store.configure_email((destination,), NOW)
        store.accept_event(SOURCE, incoming(), NOW, LIMITS)
    assert count(store, "email_routes") == 1
    assert store.email_pending_count() == 1
    assert store.email_delivery_status()["failed"] == 9


@pytest.mark.parametrize(
    "destinations", [(FIRST, FIRST), ("person@example.com",), tuple(str(i) for i in range(6))]
)
def test_invalid_email_routes_are_rejected_before_mutation(
    store: SQLiteStore, destinations: tuple[str, ...]
) -> None:
    with pytest.raises(ValueError, match="opaque destinations"):
        store.configure_email(destinations, NOW)
    assert count(store, "email_routes") == 2


@pytest.mark.parametrize("value", [-1, float("nan"), float("inf"), 7 * 86400 + 1, True])
def test_invalid_email_retry_and_pause_durations_are_rejected(
    store: SQLiteStore, value: float
) -> None:
    observe(store, False)
    record = reserve(store, FIRST)
    with pytest.raises(ValueError, match="seven days"):
        store.retry_email_delivery(record.id, NOW, "network", delay_seconds=value)
    with pytest.raises(ValueError, match="seven days"):
        store.set_email_pause(NOW, value)


def test_in_flight_capacity_is_independent_of_telegram(store: SQLiteStore) -> None:
    for index in range(MAX_IN_FLIGHT_EMAIL + 1):
        source = replace(SOURCE, alias=f"source-{index}")
        store.accept_event(source, incoming(), NOW, LIMITS)
    reserved = [store.reserve_due_email_delivery(NOW) for _ in range(MAX_IN_FLIGHT_EMAIL)]
    assert all(record is not None for record in reserved)
    assert store.reserve_due_email_delivery(NOW) is None


def test_email_acknowledgement_enforces_order_and_terminal_state(store: SQLiteStore) -> None:
    observe(store, False)
    observe(store, True, 1)
    records = [
        record for record in store.pending_email_notifications() if record.destination == FIRST
    ]
    with pytest.raises(StorageError, match="earlier email"):
        store.acknowledge_email(records[1].id, NOW)
    store.fail_email_delivery(records[0].id, NOW, "bad_request")
    with pytest.raises(StorageError, match="terminal email"):
        store.acknowledge_email(records[0].id, NOW)
    with pytest.raises(StorageError, match="terminal email"):
        store.acknowledge_email(records[1].id, NOW)
    with pytest.raises(StorageError, match="in-flight email"):
        store.retry_email_delivery(records[0].id, NOW, "network")
